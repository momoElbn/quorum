package controller

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/equality"
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
)

// QuorumClusterReconciler makes the cluster match each QuorumCluster.
type QuorumClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// ClusterDomain is the cluster's DNS suffix, usually "cluster.local".
	ClusterDomain string

	// FetchStatus asks a pod for its /status. Tests replace it with a fake.
	FetchStatus StatusFetcher
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

	// 2. Make the owned objects match the desired state. Server-side apply
	// creates an object if it's missing, changes only the fields that
	// differ, and does nothing if everything matches. ForceOwnership:
	// these objects are ours, so if someone else edited a field we set
	// (e.g. `kubectl edit`), our value wins.
	desired := []runtime.ApplyConfiguration{
		headlessService(&qc),
		clientService(&qc),
		podDisruptionBudget(&qc),
		statefulSet(&qc, r.ClusterDomain),
	}
	for _, obj := range desired {
		if err := r.Apply(ctx, obj, client.FieldOwner(fieldOwner), client.ForceOwnership); err != nil {
			// Returning the error makes controller-runtime retry later,
			// waiting longer after each failure.
			return ctrl.Result{}, fmt.Errorf("apply %T: %w", obj, err)
		}
	}

	// 3. Observe. Readiness comes from the StatefulSet, Raft state from the
	// pods themselves.
	var sts appsv1.StatefulSet
	if err := r.Get(ctx, types.NamespacedName{Namespace: qc.Namespace, Name: qc.Name}, &sts); err != nil {
		return ctrl.Result{}, fmt.Errorf("get statefulset: %w", err)
	}
	nodes := queryNodes(ctx, r.FetchStatus, &qc, r.ClusterDomain)

	// 4. Write the status, but only if it changed. Skipping identical
	// writes keeps reconciles idempotent, and avoids an update event that
	// would only trigger another reconcile.
	newStatus := computeStatus(&qc, sts.Status.ReadyReplicas, nodes)
	if !equality.Semantic.DeepEqual(qc.Status, newStatus) {
		// A merge patch sends only the status, so it can't overwrite a
		// spec change someone made since we read the object.
		patch := client.MergeFrom(qc.DeepCopy())
		qc.Status = newStatus
		if err := r.Status().Patch(ctx, &qc, patch); err != nil {
			return ctrl.Result{}, fmt.Errorf("update status: %w", err)
		}
		log.Info("status updated", "phase", newStatus.Phase, "leader", newStatus.Leader, "ready", newStatus.ReadyReplicas)
	}

	// 5. Come back later to re-check Raft state (see statusPollInterval).
	return ctrl.Result{RequeueAfter: statusPollInterval}, nil
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
