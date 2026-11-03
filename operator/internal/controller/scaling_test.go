package controller

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	quorumv1alpha1 "github.com/momoElbn/quorum/operator/api/v1alpha1"
)

// fakeRaft plays a quorum-server cluster. It answers /status for the pods
// that run, and applies membership calls to its configuration, the way the
// real leader would. It's both the StatusFetcher and the Membership.
type fakeRaft struct {
	mu      sync.Mutex
	up      map[string]bool   // pod is running and answers /status
	members map[string]bool   // Raft configuration: id -> voter
	leader  string            // current leader's ID
	commit  uint64            // leader's commit index
	applied map[string]uint64 // each node's applied index
	calls   []string          // membership calls and StatefulSet resizes, in order
}

// newFakeRaft is a cluster whose pod 0 has bootstrapped: one voter, leader.
func newFakeRaft() *fakeRaft {
	return &fakeRaft{
		up:      map[string]bool{},
		members: map[string]bool{"quorum-0": true},
		leader:  "quorum-0",
		commit:  1000,
		applied: map[string]uint64{},
	}
}

func (f *fakeRaft) fetch(_ context.Context, url string) (NodeStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	pod, _, _ := strings.Cut(strings.TrimPrefix(url, "http://"), ".")
	if !f.up[pod] {
		return NodeStatus{}, errors.New("connection refused")
	}
	st := NodeStatus{NodeID: pod, State: "follower", Term: 3, LeaderID: f.leader, AppliedIndex: f.applied[pod]}
	if _, ok := f.members[pod]; !ok {
		st.LeaderID = "" // never joined: knows no leader
	}
	if pod == f.leader {
		st.State, st.CommitIndex, st.AppliedIndex = "leader", f.commit, f.commit
	}
	for _, id := range slices.Sorted(maps.Keys(f.members)) {
		st.Members = append(st.Members, NodeMember{ID: id, Voter: f.members[id]})
	}
	return st, nil
}

// checkLeader fails a call that isn't sent to the leader's address.
func (f *fakeRaft) checkLeader(addr string) error {
	if !strings.HasPrefix(addr, f.leader+".") || !strings.HasSuffix(addr, ":8080") {
		return fmt.Errorf("sent to %s, but the leader is %s", addr, f.leader)
	}
	return nil
}

func (f *fakeRaft) Join(_ context.Context, leaderAddr, id, raftAddr string, nonvoter bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkLeader(leaderAddr); err != nil {
		return err
	}
	if !strings.HasPrefix(raftAddr, id+".quorum-headless.") || !strings.HasSuffix(raftAddr, ":7000") {
		return fmt.Errorf("raft address %q for %s", raftAddr, id)
	}
	if nonvoter {
		f.calls = append(f.calls, "join-nonvoter "+id)
		if _, ok := f.members[id]; !ok {
			f.members[id] = false
		}
		return nil
	}
	f.calls = append(f.calls, "promote "+id)
	f.members[id] = true
	return nil
}

func (f *fakeRaft) Remove(_ context.Context, leaderAddr, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkLeader(leaderAddr); err != nil {
		return err
	}
	f.calls = append(f.calls, "remove "+id)
	delete(f.members, id)
	return nil
}

func (f *fakeRaft) TransferLeadership(_ context.Context, leaderAddr string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkLeader(leaderAddr); err != nil {
		return err
	}
	f.calls = append(f.calls, "transfer-leadership from "+f.leader)
	// Raft picks some other voter; take the lowest, so tests are stable.
	for _, id := range slices.Sorted(maps.Keys(f.members)) {
		if id != f.leader && f.members[id] {
			f.leader = id
			return nil
		}
	}
	return errors.New("no other voter")
}

func (f *fakeRaft) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

// recorder collects events as "Reason member-or-note".
type recorder struct {
	mu     sync.Mutex
	events []string
}

func (r *recorder) Eventf(_ runtime.Object, _ runtime.Object, eventtype, reason, action, note string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, reason+": "+fmt.Sprintf(note, args...))
}

func (r *recorder) reasons() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for _, e := range r.events {
		reason, _, _ := strings.Cut(e, ":")
		out = append(out, reason)
	}
	return out
}

// world drives a safe-mode cluster: it reconciles, then plays the parts
// envtest doesn't run. The StatefulSet controller and kubelet create and
// delete pods to match spec.replicas; quorum-server answers /status once a
// pod runs, a nonvoter catches up right away (unless lagging is set), and a
// pod is Ready exactly when /readyz would pass: it's a voter.
type world struct {
	t       *testing.T
	qc      *quorumv1alpha1.QuorumCluster
	raft    *fakeRaft
	events  *recorder
	r       *QuorumClusterReconciler
	now     time.Time
	unready map[string]bool // pods forced not Ready
	lagging map[string]bool // nonvoters that never catch up
	pods    int32           // StatefulSet replicas at the last sync
}

func newWorld(t *testing.T, qc *quorumv1alpha1.QuorumCluster) *world {
	w := &world{t: t, qc: qc, raft: newFakeRaft(), events: &recorder{}, now: time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC),
		unready: map[string]bool{}, lagging: map[string]bool{}}
	w.r = &QuorumClusterReconciler{
		Client:        k8sClient,
		Scheme:        scheme,
		ClusterDomain: "cluster.local",
		FetchStatus:   w.raft.fetch,
		Members:       w.raft,
		Recorder:      w.events,
		Now:           func() time.Time { return w.now },
	}
	return w
}

// step reconciles once, checks it did at most one thing, and then lets the
// world react.
func (w *world) step() {
	w.t.Helper()
	before := w.raft.callCount()
	w.now = w.now.Add(scalingPollInterval)
	reconcile(w.t, w.r, w.qc)
	w.sync()
	if n := w.raft.callCount() - before; n > 1 {
		w.t.Fatalf("one reconcile did %d things: %v", n, w.raft.calls[before:])
	}
}

// sync plays the StatefulSet controller, the kubelet and the pods.
func (w *world) sync() {
	w.t.Helper()
	var sts appsv1.StatefulSet
	get(w.t, w.qc.Namespace, "quorum", &sts)
	replicas := *sts.Spec.Replicas

	f := w.raft
	f.mu.Lock()
	if replicas != w.pods {
		f.calls = append(f.calls, fmt.Sprintf("statefulset %d", replicas))
		w.pods = replicas
	}
	for i := int32(0); i < 10; i++ {
		id := podName(w.qc, i)
		f.up[id] = i < replicas
		if voter, ok := f.members[id]; ok && !voter && !w.lagging[id] {
			f.applied[id] = f.commit // caught up
		}
	}
	ready := map[string]bool{}
	for id, voter := range f.members {
		ready[id] = voter && f.up[id] && !w.unready[id]
	}
	f.mu.Unlock()

	for i := int32(0); i < 10; i++ {
		id := podName(w.qc, i)
		if i < replicas {
			setPod(w.t, w.qc, id, ready[id])
		} else {
			deletePod(w.t, w.qc.Namespace, id)
		}
	}
	get(w.t, w.qc.Namespace, "quorum", w.qc)
}

// runUntilIdle steps until the cluster reaches want voters and is Idle.
func (w *world) runUntilIdle(want int32) {
	w.t.Helper()
	for range 60 {
		w.step()
		if s := w.qc.Status.Scaling; s != nil && s.Step == quorumv1alpha1.StepIdle && w.pods == want && w.voters() == int(want) {
			return
		}
	}
	w.t.Fatalf("not idle at %d after 60 reconciles: status %+v, members %v, events %v", want, w.qc.Status.Scaling, w.raft.members, w.events.events)
}

func (w *world) voters() int {
	w.raft.mu.Lock()
	defer w.raft.mu.Unlock()
	n := 0
	for _, v := range w.raft.members {
		if v {
			n++
		}
	}
	return n
}

func (w *world) setReplicas(n int32) {
	w.t.Helper()
	get(w.t, w.qc.Namespace, "quorum", w.qc)
	w.qc.Spec.Replicas = n
	if err := k8sClient.Update(ctx, w.qc); err != nil {
		w.t.Fatal(err)
	}
}

// reset forgets the calls and events so far.
func (w *world) reset() {
	w.raft.mu.Lock()
	w.raft.calls = nil
	w.raft.mu.Unlock()
	w.events.mu.Lock()
	w.events.events = nil
	w.events.mu.Unlock()
}

// setPod creates or updates a pod with the given readiness. Pods in envtest
// have no node, so the API server deletes them at once.
func setPod(t *testing.T, qc *quorumv1alpha1.QuorumCluster, name string, ready bool) {
	t.Helper()
	pod := &corev1.Pod{}
	err := k8sClient.Get(ctx, types.NamespacedName{Namespace: qc.Namespace, Name: name}, pod)
	if apierrors.IsNotFound(err) {
		pod = &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: qc.Namespace, Labels: selectorLabels(qc)},
			Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "quorum-server", Image: "quorum-server:dev"}}},
		}
		if err := k8sClient.Create(ctx, pod); err != nil {
			t.Fatal(err)
		}
	} else if err != nil {
		t.Fatal(err)
	}
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	if len(pod.Status.Conditions) == 1 && pod.Status.Conditions[0].Status == status {
		return
	}
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: status}}
	if err := k8sClient.Status().Update(ctx, pod); err != nil {
		t.Fatal(err)
	}
}

func deletePod(t *testing.T, ns, name string) {
	t.Helper()
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}}
	if err := k8sClient.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
		t.Fatal(err)
	}
}

// formed returns a safe-mode world that has grown to n voters.
func formed(t *testing.T, n int32) *world {
	t.Helper()
	w := newWorld(t, createClusterWith(t, n, false))
	w.runUntilIdle(n)
	w.reset()
	return w
}

// addSteps lists the events for adding one member.
func addSteps() []string {
	return []string{"AddingPod", "CatchingUp", "Promoting", "Promoting"}
}

// Creating a cluster grows it from pod 0 the same way as any scale-up: the
// operator adds quorum-1 and quorum-2 itself.
func TestCreateAddsMembersOneByOne(t *testing.T) {
	w := newWorld(t, createCluster(t))
	w.runUntilIdle(3)
	want := []string{
		"statefulset 1",
		"statefulset 2", "join-nonvoter quorum-1", "promote quorum-1",
		"statefulset 3", "join-nonvoter quorum-2", "promote quorum-2",
	}
	if !slices.Equal(w.raft.calls, want) {
		t.Errorf("calls = %v\nwant    %v", w.raft.calls, want)
	}
}

func TestScaleUp3To5(t *testing.T) {
	w := formed(t, 3)
	w.setReplicas(5)
	w.runUntilIdle(5)

	want := []string{
		"statefulset 4", "join-nonvoter quorum-3", "promote quorum-3",
		"statefulset 5", "join-nonvoter quorum-4", "promote quorum-4",
	}
	if !slices.Equal(w.raft.calls, want) {
		t.Errorf("calls = %v\nwant    %v", w.raft.calls, want)
	}
	wantEvents := slices.Concat(addSteps(), addSteps(), []string{"Idle"})
	if got := w.events.reasons(); !slices.Equal(got, wantEvents) {
		t.Errorf("events = %v\nwant     %v", w.events.events, wantEvents)
	}
	st := w.qc.Status
	if st.Replicas != 5 {
		t.Errorf("status.replicas = %d, want 5", st.Replicas)
	}
	if c := meta.FindStatusCondition(st.Conditions, quorumv1alpha1.ConditionScaling); c == nil || c.Status != metav1.ConditionFalse || c.Reason != "Idle" {
		t.Errorf("Scaling condition = %+v, want False/Idle", c)
	}
}

// Each member is promoted only after two caught-up checks in a row, and a
// promoted member must be Ready before the next pod is added.
func TestScaleUpWaitsForCatchUpAndReady(t *testing.T) {
	w := formed(t, 3)
	w.lagging["quorum-3"] = true
	w.setReplicas(4)

	w.step() // statefulset 4
	w.step() // join as nonvoter
	for range 3 {
		w.step()
		if s := w.qc.Status.Scaling; s.Step != quorumv1alpha1.StepCatchingUp || s.Member != "quorum-3" {
			t.Fatalf("while lagging: step %s %s, want CatchingUp quorum-3", s.Step, s.Member)
		}
	}
	delete(w.lagging, "quorum-3")
	w.unready["quorum-3"] = true // once promoted, its readiness probe fails
	w.sync()                     // it catches up now
	w.step()                     // first caught-up check
	w.step()                     // second: promote
	if last := w.raft.calls[len(w.raft.calls)-1]; last != "promote quorum-3" {
		t.Fatalf("last call = %q, want promote quorum-3", last)
	}
	// Not Ready yet: the operator must not report Idle.
	for range 3 {
		w.step()
		if s := w.qc.Status.Scaling; s.Step != quorumv1alpha1.StepPromoting {
			t.Fatalf("promoted but not Ready: step %s, want Promoting", s.Step)
		}
	}
	delete(w.unready, "quorum-3")
	w.runUntilIdle(4)
}

func TestNoStepWhileAVoterIsUnready(t *testing.T) {
	w := formed(t, 3)
	w.unready["quorum-1"] = true
	w.sync() // its readiness probe starts failing
	w.setReplicas(5)
	for range 5 {
		w.step()
	}
	if len(w.raft.calls) != 0 {
		t.Fatalf("calls while quorum-1 is unready: %v", w.raft.calls)
	}
	c := meta.FindStatusCondition(w.qc.Status.Conditions, quorumv1alpha1.ConditionScaling)
	if c == nil || c.Status != metav1.ConditionTrue || c.Reason != "Waiting" || !strings.Contains(c.Message, "quorum-1 is not Ready") {
		t.Errorf("Scaling condition = %+v, want True/Waiting naming quorum-1", c)
	}

	delete(w.unready, "quorum-1")
	w.runUntilIdle(5)
}

func TestScaleDown5To3(t *testing.T) {
	w := formed(t, 5)
	w.setReplicas(3)
	w.runUntilIdle(3)

	// Every member leaves Raft before its pod (and volume) goes.
	want := []string{"remove quorum-4", "statefulset 4", "remove quorum-3", "statefulset 3"}
	if !slices.Equal(w.raft.calls, want) {
		t.Errorf("calls = %v\nwant    %v", w.raft.calls, want)
	}
	wantEvents := []string{"Removing", "ShrinkingPods", "Removing", "ShrinkingPods", "Idle"}
	if got := w.events.reasons(); !slices.Equal(got, wantEvents) {
		t.Errorf("events = %v\nwant     %v", w.events.events, wantEvents)
	}
}

func TestScaleDownLeaderIsHighestOrdinal(t *testing.T) {
	w := formed(t, 5)
	w.raft.mu.Lock()
	w.raft.leader = "quorum-4"
	w.raft.mu.Unlock()
	w.setReplicas(3)
	w.runUntilIdle(3)

	want := []string{
		"transfer-leadership from quorum-4", "remove quorum-4", "statefulset 4",
		"remove quorum-3", "statefulset 3",
	}
	if !slices.Equal(w.raft.calls, want) {
		t.Errorf("calls = %v\nwant    %v", w.raft.calls, want)
	}
	if w.raft.leader == "quorum-4" {
		t.Error("quorum-4 is still the leader")
	}
}

func TestStuckStepStops(t *testing.T) {
	w := formed(t, 3)
	w.lagging["quorum-3"] = true
	w.setReplicas(4)
	w.step() // statefulset 4
	w.step() // join as nonvoter, starts CatchingUp
	w.now = w.now.Add(stuckAfter)
	w.step()

	c := meta.FindStatusCondition(w.qc.Status.Conditions, quorumv1alpha1.ConditionScaling)
	if c == nil || c.Status != metav1.ConditionFalse || c.Reason != "Stuck" {
		t.Fatalf("Scaling condition = %+v, want False/Stuck", c)
	}
	// It stays stopped, even once the cause is gone, until the spec changes.
	delete(w.lagging, "quorum-3")
	calls := len(w.raft.calls)
	for range 3 {
		w.step()
	}
	if len(w.raft.calls) != calls {
		t.Errorf("steps after Stuck: %v", w.raft.calls[calls:])
	}
	w.setReplicas(5)
	w.runUntilIdle(5)
}

// Unsafe mode is the naive baseline: the StatefulSet follows spec.replicas
// at once and the operator never touches Raft membership.
func TestUnsafeScalingSkipsMembership(t *testing.T) {
	qc := createClusterWith(t, 3, true)
	raft := &fakeRaft{}
	r := newReconciler(healthyNodes())
	r.Members = raft

	var sts appsv1.StatefulSet
	for _, n := range []int32{3, 5, 3} {
		get(t, qc.Namespace, "quorum", qc)
		qc.Spec.Replicas = n
		if err := k8sClient.Update(ctx, qc); err != nil {
			t.Fatal(err)
		}
		reconcile(t, r, qc)
		get(t, qc.Namespace, "quorum", &sts)
		if *sts.Spec.Replicas != n {
			t.Errorf("statefulset replicas = %d, want %d right away", *sts.Spec.Replicas, n)
		}
	}
	if len(raft.calls) != 0 {
		t.Errorf("membership calls in unsafe mode: %v", raft.calls)
	}
	c := sts.Spec.Template.Spec.Containers[0]
	if got := envValue(c, "JOIN_ADDR"); got != "quorum-0.quorum-headless."+qc.Namespace+".svc.cluster.local:8080" {
		t.Errorf("JOIN_ADDR = %q, want pod 0's address (pods join by themselves)", got)
	}
	if p := sts.Spec.PersistentVolumeClaimRetentionPolicy; p == nil || p.WhenScaled != appsv1.RetainPersistentVolumeClaimRetentionPolicyType {
		t.Errorf("PVC retention = %+v, want whenScaled Retain", p)
	}
	get(t, qc.Namespace, "quorum", qc)
	if c := meta.FindStatusCondition(qc.Status.Conditions, quorumv1alpha1.ConditionScaling); c == nil || c.Reason != "UnsafeScaling" {
		t.Errorf("Scaling condition = %+v, want reason UnsafeScaling", c)
	}
}

// Changing spec.replicas must not touch the pod template: a template change
// restarts every pod, which would wreck the scaling measurements.
func TestScalingLeavesPodTemplateUnchanged(t *testing.T) {
	for _, unsafe := range []bool{false, true} {
		t.Run(fmt.Sprintf("unsafe=%v", unsafe), func(t *testing.T) {
			var w *world
			if unsafe {
				w = newWorld(t, createClusterWith(t, 3, true))
				w.step()
			} else {
				w = formed(t, 3)
			}
			var before, after appsv1.StatefulSet
			get(t, w.qc.Namespace, "quorum", &before)

			w.setReplicas(5)
			w.step()
			get(t, w.qc.Namespace, "quorum", &after)
			if *after.Spec.Replicas == *before.Spec.Replicas {
				t.Fatal("statefulset replicas didn't change; the test proves nothing")
			}
			if !equality.Semantic.DeepEqual(before.Spec.Template, after.Spec.Template) {
				t.Error("pod template changed when spec.replicas changed")
			}
		})
	}
}
