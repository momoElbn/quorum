package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	quorumv1alpha1 "github.com/momoElbn/quorum/operator/api/v1alpha1"
)

// nsCounter gives each test its own namespace, so tests don't see each
// other's objects. envtest has no namespace controller, so they're never
// deleted, which is fine for a throwaway API server.
var nsCounter atomic.Int32

func newNamespace(t *testing.T) string {
	t.Helper()
	name := fmt.Sprintf("test-%d", nsCounter.Add(1))
	if err := k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}); err != nil {
		t.Fatal(err)
	}
	return name
}

// createCluster creates a 3-replica QuorumCluster named "quorum".
func createCluster(t *testing.T) *quorumv1alpha1.QuorumCluster {
	t.Helper()
	qc := &quorumv1alpha1.QuorumCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "quorum", Namespace: newNamespace(t)},
		Spec: quorumv1alpha1.QuorumClusterSpec{
			Replicas:    3,
			Image:       "quorum-server:dev",
			StorageSize: resource.MustParse("1Gi"),
		},
	}
	if err := k8sClient.Create(ctx, qc); err != nil {
		t.Fatal(err)
	}
	return qc
}

// fakeNodes is a StatusFetcher backed by a map from pod name to status.
// Pods missing from the map behave like pods that don't answer.
type fakeNodes map[string]NodeStatus

func (f fakeNodes) fetch(_ context.Context, url string) (NodeStatus, error) {
	// url looks like http://quorum-0.quorum-headless.<ns>.svc.cluster.local:8080/status
	pod, _, _ := strings.Cut(strings.TrimPrefix(url, "http://"), ".")
	st, ok := f[pod]
	if !ok {
		return st, errors.New("connection refused")
	}
	return st, nil
}

// healthyNodes is what a formed 3-node cluster answers: quorum-0 leads and
// all three are members.
func healthyNodes() fakeNodes {
	members := []struct {
		ID string `json:"id"`
	}{{"quorum-0"}, {"quorum-1"}, {"quorum-2"}}
	f := fakeNodes{}
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("quorum-%d", i)
		state := "follower"
		if i == 0 {
			state = "leader"
		}
		f[id] = NodeStatus{NodeID: id, State: state, Term: 2, LeaderID: "quorum-0", Members: members}
	}
	return f
}

func newReconciler(f fakeNodes) *QuorumClusterReconciler {
	return &QuorumClusterReconciler{
		Client:        k8sClient,
		Scheme:        scheme,
		ClusterDomain: "cluster.local",
		FetchStatus:   f.fetch,
	}
}

func reconcile(t *testing.T, r *QuorumClusterReconciler, qc *quorumv1alpha1.QuorumCluster) ctrl.Result {
	t.Helper()
	res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(qc)})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	return res
}

func get(t *testing.T, ns, name string, obj client.Object) {
	t.Helper()
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, obj); err != nil {
		t.Fatalf("get %T %s: %v", obj, name, err)
	}
}

// checkOwned fails unless obj's controller is qc. The owner reference is
// what makes deleting the QuorumCluster delete obj.
func checkOwned(t *testing.T, obj client.Object, qc *quorumv1alpha1.QuorumCluster) {
	t.Helper()
	ref := metav1.GetControllerOf(obj)
	if ref == nil || ref.UID != qc.UID || ref.Kind != "QuorumCluster" {
		t.Errorf("%T %s: controller owner = %+v, want QuorumCluster %s", obj, obj.GetName(), ref, qc.UID)
	}
}

func envValue(c corev1.Container, name string) string {
	for _, e := range c.Env {
		if e.Name == name {
			return e.Value
		}
	}
	return ""
}

func TestReconcileCreatesOwnedObjects(t *testing.T) {
	qc := createCluster(t)
	res := reconcile(t, newReconciler(healthyNodes()), qc)
	if res.RequeueAfter != statusPollInterval {
		t.Errorf("RequeueAfter = %v, want %v", res.RequeueAfter, statusPollInterval)
	}
	ns := qc.Namespace

	var sts appsv1.StatefulSet
	get(t, ns, "quorum", &sts)
	checkOwned(t, &sts, qc)
	if *sts.Spec.Replicas != 3 || sts.Spec.ServiceName != "quorum-headless" {
		t.Errorf("replicas=%d serviceName=%q", *sts.Spec.Replicas, sts.Spec.ServiceName)
	}
	c := sts.Spec.Template.Spec.Containers[0]
	if c.Image != "quorum-server:dev" {
		t.Errorf("image = %q", c.Image)
	}
	host0 := "quorum-0.quorum-headless." + ns + ".svc.cluster.local"
	wantEnv := map[string]string{
		"NODE_ID":        "$(POD_NAME)",
		"RAFT_ADDR":      "$(POD_NAME).quorum-headless.$(POD_NAMESPACE).svc.cluster.local:7000",
		"BOOTSTRAP_NODE": "quorum-0",
		"JOIN_ADDR":      host0 + ":8080",
	}
	for k, want := range wantEnv {
		if got := envValue(c, k); got != want {
			t.Errorf("env %s = %q, want %q", k, got, want)
		}
	}
	if peers := envValue(c, "PEERS_HTTP"); strings.Count(peers, "=") != 3 || !strings.HasPrefix(peers, "quorum-0="+host0) {
		t.Errorf("PEERS_HTTP = %q", peers)
	}
	vct := sts.Spec.VolumeClaimTemplates
	if len(vct) != 1 || vct[0].Name != "data" || vct[0].Spec.Resources.Requests.Storage().String() != "1Gi" {
		t.Errorf("volumeClaimTemplates = %+v", vct)
	}

	var headless corev1.Service
	get(t, ns, "quorum-headless", &headless)
	checkOwned(t, &headless, qc)
	if headless.Spec.ClusterIP != corev1.ClusterIPNone || !headless.Spec.PublishNotReadyAddresses {
		t.Errorf("headless: clusterIP=%q publishNotReady=%v", headless.Spec.ClusterIP, headless.Spec.PublishNotReadyAddresses)
	}

	var svc corev1.Service
	get(t, ns, "quorum", &svc)
	checkOwned(t, &svc, qc)
	if len(svc.Spec.Ports) != 1 || svc.Spec.Ports[0].Port != 8080 {
		t.Errorf("client service ports = %+v", svc.Spec.Ports)
	}

	var pdb policyv1.PodDisruptionBudget
	get(t, ns, "quorum", &pdb)
	checkOwned(t, &pdb, qc)
	if pdb.Spec.MaxUnavailable == nil || pdb.Spec.MaxUnavailable.IntValue() != 1 {
		t.Errorf("pdb maxUnavailable = %v", pdb.Spec.MaxUnavailable)
	}
}

// Reconciling twice in a row must not change anything the second time: no
// object gets a new resourceVersion (which every real write would cause).
func TestReconcileIsIdempotent(t *testing.T) {
	qc := createCluster(t)
	r := newReconciler(healthyNodes())
	reconcile(t, r, qc)

	objs := map[string]client.Object{
		"statefulset":    &appsv1.StatefulSet{},
		"headless":       &corev1.Service{},
		"client service": &corev1.Service{},
		"pdb":            &policyv1.PodDisruptionBudget{},
		"quorumcluster":  &quorumv1alpha1.QuorumCluster{},
	}
	names := map[string]string{"headless": "quorum-headless"}
	versions := map[string]string{}
	for k, obj := range objs {
		name := names[k]
		if name == "" {
			name = "quorum"
		}
		get(t, qc.Namespace, name, obj)
		versions[k] = obj.GetResourceVersion()
	}

	reconcile(t, r, qc)
	reconcile(t, r, qc)

	for k, obj := range objs {
		get(t, qc.Namespace, obj.GetName(), obj)
		if obj.GetResourceVersion() != versions[k] {
			t.Errorf("%s changed: resourceVersion %s -> %s", k, versions[k], obj.GetResourceVersion())
		}
	}
}

// Changes made behind the operator's back are undone on the next reconcile.
func TestReconcileFixesDrift(t *testing.T) {
	qc := createCluster(t)
	r := newReconciler(healthyNodes())
	reconcile(t, r, qc)

	// Someone edits the image by hand and deletes the client Service.
	var sts appsv1.StatefulSet
	get(t, qc.Namespace, "quorum", &sts)
	sts.Spec.Template.Spec.Containers[0].Image = "hacked:latest"
	if err := k8sClient.Update(ctx, &sts); err != nil {
		t.Fatal(err)
	}
	var svc corev1.Service
	get(t, qc.Namespace, "quorum", &svc)
	if err := k8sClient.Delete(ctx, &svc); err != nil {
		t.Fatal(err)
	}

	reconcile(t, r, qc)

	get(t, qc.Namespace, "quorum", &sts)
	if img := sts.Spec.Template.Spec.Containers[0].Image; img != "quorum-server:dev" {
		t.Errorf("image = %q, want it restored to quorum-server:dev", img)
	}
	get(t, qc.Namespace, "quorum", &svc) // fails the test if not recreated
}

// Changing spec.image rolls out the new image through the StatefulSet.
func TestImageChangeUpdatesStatefulSet(t *testing.T) {
	qc := createCluster(t)
	r := newReconciler(healthyNodes())
	reconcile(t, r, qc)

	get(t, qc.Namespace, "quorum", qc)
	qc.Spec.Image = "quorum-server:v2"
	if err := k8sClient.Update(ctx, qc); err != nil {
		t.Fatal(err)
	}
	reconcile(t, r, qc)

	var sts appsv1.StatefulSet
	get(t, qc.Namespace, "quorum", &sts)
	if img := sts.Spec.Template.Spec.Containers[0].Image; img != "quorum-server:v2" {
		t.Errorf("image = %q, want quorum-server:v2", img)
	}
}

// setReadyReplicas plays the StatefulSet controller, which doesn't run in
// envtest: it reports n pods ready.
func setReadyReplicas(t *testing.T, ns string, n int32) {
	t.Helper()
	var sts appsv1.StatefulSet
	get(t, ns, "quorum", &sts)
	sts.Status.Replicas = 3
	sts.Status.ReadyReplicas = n
	if err := k8sClient.Status().Update(ctx, &sts); err != nil {
		t.Fatal(err)
	}
}

func TestStatusFollowsTheCluster(t *testing.T) {
	qc := createCluster(t)
	nodes := fakeNodes{} // nobody answers yet
	r := newReconciler(nodes)

	check := func(phase quorumv1alpha1.QuorumClusterPhase, leader string, ready int32, available metav1.ConditionStatus) {
		t.Helper()
		reconcile(t, r, qc)
		get(t, qc.Namespace, "quorum", qc)
		st := qc.Status
		if st.Phase != phase || st.Leader != leader || st.ReadyReplicas != ready {
			t.Errorf("status = phase %s leader %q ready %d; want %s %q %d", st.Phase, st.Leader, st.ReadyReplicas, phase, leader, ready)
		}
		if c := meta.FindStatusCondition(st.Conditions, quorumv1alpha1.ConditionAvailable); c == nil || c.Status != available {
			t.Errorf("Available condition = %+v, want %s", c, available)
		}
		if st.ObservedGeneration != qc.Generation {
			t.Errorf("observedGeneration = %d, want %d", st.ObservedGeneration, qc.Generation)
		}
	}

	// Just created: no pod ready, no answers.
	check(quorumv1alpha1.PhasePending, "", 0, metav1.ConditionFalse)

	// All pods up, quorum-0 leads.
	setReadyReplicas(t, qc.Namespace, 3)
	for k, v := range healthyNodes() {
		nodes[k] = v
	}
	check(quorumv1alpha1.PhaseRunning, "quorum-0", 3, metav1.ConditionTrue)
	if got := strings.Join(qc.Status.Members, ","); got != "quorum-0,quorum-1,quorum-2" {
		t.Errorf("members = %q", got)
	}

	// quorum-2 dies: still a leader and a majority, but degraded.
	delete(nodes, "quorum-2")
	setReadyReplicas(t, qc.Namespace, 2)
	check(quorumv1alpha1.PhaseDegraded, "quorum-0", 2, metav1.ConditionTrue)

	// The leader dies too: no majority, no leader.
	delete(nodes, "quorum-0")
	n1 := nodes["quorum-1"]
	n1.State, n1.LeaderID = "candidate", ""
	nodes["quorum-1"] = n1
	setReadyReplicas(t, qc.Namespace, 1)
	check(quorumv1alpha1.PhaseUnavailable, "", 1, metav1.ConditionFalse)
}

func TestReconcileMissingClusterIsNoError(t *testing.T) {
	r := newReconciler(fakeNodes{})
	res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "nowhere", Name: "gone"}})
	if err != nil || res != (ctrl.Result{}) {
		t.Errorf("Reconcile of a deleted cluster = %+v, %v; want empty result, nil", res, err)
	}
}

// The CRD's validation rules run in the API server, so envtest enforces them.
func TestCRDValidation(t *testing.T) {
	ns := newNamespace(t)

	// Defaults: only image is required.
	qc := &quorumv1alpha1.QuorumCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "defaults", Namespace: ns},
		Spec:       quorumv1alpha1.QuorumClusterSpec{Image: "quorum-server:dev"},
	}
	// The Go zero values would be sent as replicas: 0 and storageSize: "0",
	// so send the object without those fields to see the defaults.
	if err := k8sClient.Create(ctx, rawCluster("defaults", ns, map[string]any{"image": "quorum-server:dev"})); err != nil {
		t.Fatal(err)
	}
	get(t, ns, "defaults", qc)
	if qc.Spec.Replicas != 3 || qc.Spec.StorageSize.String() != "1Gi" {
		t.Errorf("defaults: replicas=%d storageSize=%s, want 3 and 1Gi", qc.Spec.Replicas, qc.Spec.StorageSize.String())
	}

	// replicas and storageSize can't change after creation.
	changes := map[string]func(*quorumv1alpha1.QuorumCluster){
		"replicas":    func(c *quorumv1alpha1.QuorumCluster) { c.Spec.Replicas = 5 },
		"storageSize": func(c *quorumv1alpha1.QuorumCluster) { c.Spec.StorageSize = resource.MustParse("2Gi") },
	}
	for field, change := range changes {
		get(t, ns, "defaults", qc)
		change(qc)
		if err := k8sClient.Update(ctx, qc); err == nil || !strings.Contains(err.Error(), field) {
			t.Errorf("changing %s: err = %v, want a validation error", field, err)
		}
	}

	// Names longer than 50 characters are rejected.
	if err := k8sClient.Create(ctx, rawCluster(strings.Repeat("a", 51), ns, map[string]any{"image": "x"})); err == nil {
		t.Error("51-character name: want a validation error")
	}
}

// rawCluster builds a QuorumCluster as plain JSON-like maps, so fields that
// are left out are really absent (a Go struct would send zero values).
func rawCluster(name, ns string, spec map[string]any) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{Object: map[string]any{"spec": spec}}
	obj.SetGroupVersionKind(quorumv1alpha1.GroupVersion.WithKind("QuorumCluster"))
	obj.SetName(name)
	obj.SetNamespace(ns)
	return obj
}
