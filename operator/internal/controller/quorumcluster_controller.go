package controller

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	quorumv1alpha1 "github.com/momoElbn/quorum/operator/api/v1alpha1"
)

const (
	// fieldOwner is our name in server-side apply. The API server records
	// which fields each owner set, so it can tell our fields apart from
	// fields set by others (defaults, other controllers, kubectl).
	fieldOwner = "quorum-operator"

	// statusPollInterval is how often we re-check the Raft state. Leader
	// elections happen inside the pods and don't change any Kubernetes
	// object, so no watch event tells us about them. We have to ask.
	statusPollInterval = 15 * time.Second

	// scalingPollInterval is how often we look again while a scale
	// operation is in progress. Each reconcile does at most one step, so
	// this sets the pace; the catch-up check also relies on two looks a
	// couple of seconds apart.
	scalingPollInterval = 2 * time.Second

	// stuckAfter is how long one step may make no progress before the
	// operator gives up and reports Stuck. It doesn't roll back: undoing a
	// half-done membership change automatically could make things worse,
	// so a human decides.
	stuckAfter = 5 * time.Minute
)

// EventRecorder emits Kubernetes Events. It is the part of
// events.EventRecorder we use, so tests can pass a fake.
type EventRecorder interface {
	Eventf(regarding runtime.Object, related runtime.Object, eventtype, reason, action, note string, args ...any)
}

// QuorumClusterReconciler makes the cluster match each QuorumCluster.
type QuorumClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// ClusterDomain is the cluster's DNS suffix, usually "cluster.local".
	ClusterDomain string

	// FetchStatus asks a pod for its /status. Tests replace it with a fake.
	FetchStatus StatusFetcher

	// Members changes the Raft membership. Tests replace it with a fake.
	Members Membership

	// Recorder emits an Event for every scaling step, so
	// `kubectl get events` shows the timeline. May be nil.
	Recorder EventRecorder

	// Now returns the current time. Tests set it to control "stuck".
	Now func() time.Time
}

// These markers generate the operator's RBAC rules (config/rbac/role.yaml)
// when you run `make manifests`. Keep charts/quorum-operator in sync.
//
// +kubebuilder:rbac:groups=quorum.momoelbn.github.io,resources=quorumclusters,verbs=get;list;watch
// +kubebuilder:rbac:groups=quorum.momoelbn.github.io,resources=quorumclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=quorum.momoelbn.github.io,resources=quorumclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

// Reconcile is called whenever a QuorumCluster or one of the objects it owns
// changes, and every statusPollInterval. It doesn't look at what changed:
// it computes the full desired state and makes the cluster match it. That
// way it gives the right result however often it is called, and it fixes
// any drift, whatever caused it.
func (r *QuorumClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// 1. Read the QuorumCluster.
	var qc quorumv1alpha1.QuorumCluster
	if err := r.Get(ctx, req.NamespacedName, &qc); err != nil {
		// Not found means it was deleted. There is nothing to clean up:
		// the owner references make Kubernetes delete the StatefulSet,
		// Services and PDB.
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !qc.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil // being deleted; garbage collection does the rest
	}

	// 2. Observe: the StatefulSet's current size and readiness, every pod's
	// readiness, and the Raft state the pods themselves report.
	var sts appsv1.StatefulSet
	stsExists := true
	if err := r.Get(ctx, types.NamespacedName{Namespace: qc.Namespace, Name: qc.Name}, &sts); err != nil {
		if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("get statefulset: %w", err)
		}
		stsExists = false
	}
	current := qc.Spec.Replicas
	if !qc.Spec.UnsafeScaling {
		// Safe mode starts with only pod 0, which bootstraps the cluster.
		// The operator then adds the others one by one, like any scale-up.
		current = 1
		if stsExists && sts.Spec.Replicas != nil {
			current = *sts.Spec.Replicas
		}
	}
	nodes := queryNodes(ctx, r.FetchStatus, &qc, max(current, 1), r.ClusterDomain)
	ready, err := r.podReadiness(ctx, &qc)
	if err != nil {
		return ctrl.Result{}, err
	}

	// 3. Decide and carry out at most one scaling step (safe mode only).
	replicas := current
	scaling, scalingCond, err := r.scale(ctx, &qc, current, nodes, ready, &replicas)
	if err != nil {
		return ctrl.Result{}, err
	}

	// 4. Make the owned objects match the desired state. Server-side apply
	// creates an object if it's missing, changes only the fields that
	// differ, and does nothing if everything matches. ForceOwnership:
	// these objects are ours, so if someone else edited a field we set
	// (e.g. `kubectl edit`), our value wins.
	desired := []runtime.ApplyConfiguration{
		headlessService(&qc),
		clientService(&qc),
		podDisruptionBudget(&qc),
		statefulSet(&qc, r.ClusterDomain, replicas),
	}
	for _, obj := range desired {
		if err := r.Apply(ctx, obj, client.FieldOwner(fieldOwner), client.ForceOwnership); err != nil {
			// Returning the error makes controller-runtime retry later,
			// waiting longer after each failure.
			return ctrl.Result{}, fmt.Errorf("apply %T: %w", obj, err)
		}
	}

	// 5. Write the status, but only if it changed. Skipping identical
	// writes keeps reconciles idempotent, and avoids an update event that
	// would only trigger another reconcile.
	newStatus := computeStatus(&qc, sts.Status.ReadyReplicas, nodes)
	newStatus.Replicas = replicas
	newStatus.Scaling = scaling
	meta.SetStatusCondition(&newStatus.Conditions, scalingCond)
	if !equality.Semantic.DeepEqual(qc.Status, newStatus) {
		// A merge patch sends only the status, so it can't overwrite a
		// spec change someone made since we read the object.
		patch := client.MergeFrom(qc.DeepCopy())
		qc.Status = newStatus
		if err := r.Status().Patch(ctx, &qc, patch); err != nil {
			return ctrl.Result{}, fmt.Errorf("update status: %w", err)
		}
		log.Info("status updated", "phase", newStatus.Phase, "leader", newStatus.Leader, "ready", newStatus.ReadyReplicas,
			"replicas", replicas, "step", scaling.Step, "member", scaling.Member)
	}

	// 6. Come back later. While scaling, soon: each reconcile does one
	// step. Otherwise to re-check Raft state (see statusPollInterval).
	if !qc.Spec.UnsafeScaling && scalingCond.Status == metav1.ConditionTrue {
		return ctrl.Result{RequeueAfter: scalingPollInterval}, nil
	}
	return ctrl.Result{RequeueAfter: statusPollInterval}, nil
}

// podReadiness reports which of the cluster's pods are Ready.
func (r *QuorumClusterReconciler) podReadiness(ctx context.Context, qc *quorumv1alpha1.QuorumCluster) (map[string]bool, error) {
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(qc.Namespace), client.MatchingLabels(selectorLabels(qc))); err != nil {
		return nil, fmt.Errorf("list pods: %w", err)
	}
	ready := map[string]bool{}
	for _, p := range pods.Items {
		if p.DeletionTimestamp != nil {
			continue // on its way out: not a pod to count on
		}
		for _, c := range p.Status.Conditions {
			if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
				ready[p.Name] = true
			}
		}
	}
	return ready, nil
}

// scale plans and carries out at most one scaling step. It may change
// *replicas (the StatefulSet size to apply), and returns the new scaling
// status and Scaling condition.
func (r *QuorumClusterReconciler) scale(ctx context.Context, qc *quorumv1alpha1.QuorumCluster, current int32,
	nodes []NodeStatus, ready map[string]bool, replicas *int32) (*quorumv1alpha1.ScalingStatus, metav1.Condition, error) {
	now := metav1.NewTime(r.now())
	cond := metav1.Condition{Type: quorumv1alpha1.ConditionScaling, ObservedGeneration: qc.Generation}

	prev := quorumv1alpha1.ScalingStatus{Step: quorumv1alpha1.StepIdle, Since: now}
	if qc.Status.Scaling != nil {
		prev = *qc.Status.Scaling
	}

	if qc.Spec.UnsafeScaling {
		cond.Status, cond.Reason = metav1.ConditionFalse, "UnsafeScaling"
		cond.Message = "unsafeScaling is on: the StatefulSet follows spec.replicas and Raft membership isn't managed"
		idle := quorumv1alpha1.ScalingStatus{Step: quorumv1alpha1.StepIdle, Since: prev.Since}
		if prev.Step != quorumv1alpha1.StepIdle || prev.Member != "" {
			idle.Since = now
		}
		return &idle, cond, nil
	}

	// Stuck stays stuck until someone changes the spec. Then the step's
	// clock restarts, so it gets a fresh 5 minutes.
	if old := meta.FindStatusCondition(qc.Status.Conditions, quorumv1alpha1.ConditionScaling); old != nil && old.Reason == "Stuck" {
		if old.ObservedGeneration == qc.Generation {
			return &prev, *old, nil
		}
		prev.Since = now
	}

	in := scaleInput{current: current, want: qc.Spec.Replicas, leader: findLeader(nodes), ready: ready, prev: prev, nodes: map[string]NodeStatus{}}
	for _, n := range nodes {
		in.nodes[n.NodeID] = n
	}
	plan := planScale(qc, in)

	next := quorumv1alpha1.ScalingStatus{Step: plan.step, Member: plan.member, Since: prev.Since}
	changed := next.Step != prev.Step || next.Member != prev.Member
	if changed {
		next.Since = now
	}

	// No progress for too long: stop and say so.
	if plan.action == actNone && !changed && next.Step != quorumv1alpha1.StepIdle && now.Sub(prev.Since.Time) > stuckAfter {
		cond.Status, cond.Reason = metav1.ConditionFalse, "Stuck"
		cond.Message = fmt.Sprintf("%s for %s made no progress for %s: %s. Fix the cause or change the spec to retry.",
			next.Step, next.Member, stuckAfter, plan.note)
		r.event(qc, corev1.EventTypeWarning, "Stuck", cond.Message)
		return &next, cond, nil
	}

	if err := r.do(ctx, qc, plan, in.leader, replicas); err != nil {
		r.event(qc, corev1.EventTypeWarning, string(plan.step), fmt.Sprintf("%s failed: %v", plan.note, err))
		return nil, cond, fmt.Errorf("scaling step %s for %s: %w", plan.step, plan.member, err)
	}
	if plan.action != actNone || (changed && !plan.wait) {
		r.event(qc, corev1.EventTypeNormal, string(plan.step), plan.note)
	}

	switch {
	case next.Step != quorumv1alpha1.StepIdle:
		cond.Status, cond.Reason, cond.Message = metav1.ConditionTrue, string(next.Step), plan.note
	case current != qc.Spec.Replicas:
		// Not started yet, e.g. a voter isn't Ready.
		cond.Status, cond.Reason, cond.Message = metav1.ConditionTrue, "Waiting", plan.note
	default:
		cond.Status, cond.Reason, cond.Message = metav1.ConditionFalse, "Idle", plan.note
	}
	return &next, cond, nil
}

// do carries out the plan's action, if any.
func (r *QuorumClusterReconciler) do(ctx context.Context, qc *quorumv1alpha1.QuorumCluster, plan scalePlan, leader *NodeStatus, replicas *int32) error {
	if plan.action == actNone {
		return nil
	}
	if plan.action == actSetReplicas {
		*replicas = plan.replicas // applied with the StatefulSet below
		return nil
	}
	leaderAddr := fmt.Sprintf("%s:%d", podHostByName(qc, leader.NodeID, r.ClusterDomain), httpPort)
	raftAddr := fmt.Sprintf("%s:%d", podHostByName(qc, plan.member, r.ClusterDomain), raftPort)
	switch plan.action {
	case actJoinNonvoter:
		return r.Members.Join(ctx, leaderAddr, plan.member, raftAddr, true)
	case actPromote:
		return r.Members.Join(ctx, leaderAddr, plan.member, raftAddr, false)
	case actTransferLeadership:
		return r.Members.TransferLeadership(ctx, leaderAddr)
	case actRemove:
		return r.Members.Remove(ctx, leaderAddr, plan.member)
	}
	return nil
}

func (r *QuorumClusterReconciler) event(qc *quorumv1alpha1.QuorumCluster, eventType, reason, note string) {
	if r.Recorder != nil {
		r.Recorder.Eventf(qc, nil, eventType, reason, "Scale", "%s", note)
	}
}

func (r *QuorumClusterReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// SetupWithManager registers the controller. For() watches QuorumClusters.
// Owns() watches the objects we create and maps their events back to the
// owning QuorumCluster (through the owner reference). So if someone deletes
// the client Service, the next reconcile recreates it within moments.
func (r *QuorumClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&quorumv1alpha1.QuorumCluster{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Owns(&policyv1.PodDisruptionBudget{}).
		Named("quorumcluster").
		Complete(r)
}
