package controller

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	quorumv1alpha1 "github.com/momoElbn/quorum/operator/api/v1alpha1"
)

// These tests cover the pure status logic and the HTTP fetcher. They don't
// use the API server.

func testCluster() *quorumv1alpha1.QuorumCluster {
	return &quorumv1alpha1.QuorumCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "quorum", Namespace: "db", Generation: 4},
		Spec:       quorumv1alpha1.QuorumClusterSpec{Replicas: 3},
	}
}

func nodesOf(f fakeNodes) []NodeStatus {
	var out []NodeStatus
	for _, n := range f {
		out = append(out, n)
	}
	return out
}

func TestComputeStatusPhases(t *testing.T) {
	healthy := nodesOf(healthyNodes())
	twoMembers := nodesOf(healthyNodes())
	for i := range twoMembers {
		twoMembers[i].Members = twoMembers[i].Members[:2] // quorum-2 never joined
	}
	followersOnly := nodesOf(healthyNodes())
	for i := range followersOnly {
		followersOnly[i].State = "follower"
	}

	cases := []struct {
		name   string
		ready  int32
		nodes  []NodeStatus
		phase  quorumv1alpha1.QuorumClusterPhase
		leader string
	}{
		{"nothing up yet", 0, nil, quorumv1alpha1.PhasePending, ""},
		{"pods up, no leader", 3, followersOnly, quorumv1alpha1.PhaseUnavailable, ""},
		{"all healthy", 3, healthy, quorumv1alpha1.PhaseRunning, "quorum-0"},
		{"one pod not ready", 2, healthy, quorumv1alpha1.PhaseDegraded, "quorum-0"},
		{"one pod not a member", 3, twoMembers, quorumv1alpha1.PhaseDegraded, "quorum-0"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			st := computeStatus(testCluster(), c.ready, c.nodes)
			if st.Phase != c.phase || st.Leader != c.leader {
				t.Errorf("phase=%s leader=%q, want %s %q", st.Phase, st.Leader, c.phase, c.leader)
			}
			if st.ObservedGeneration != 4 {
				t.Errorf("observedGeneration = %d, want 4", st.ObservedGeneration)
			}
			degraded := meta.IsStatusConditionTrue(st.Conditions, quorumv1alpha1.ConditionDegraded)
			if degraded != (c.phase != quorumv1alpha1.PhaseRunning) {
				t.Errorf("Degraded condition = %v in phase %s", degraded, c.phase)
			}
		})
	}
}

// After a network split heals, the old leader may still claim to lead for a
// moment. The node with the higher term is the real leader.
func TestComputeStatusPicksLeaderWithHighestTerm(t *testing.T) {
	nodes := nodesOf(healthyNodes())
	for i := range nodes {
		if nodes[i].NodeID == "quorum-1" {
			nodes[i].State = "leader"
			nodes[i].Term = 3
		}
	}
	if st := computeStatus(testCluster(), 3, nodes); st.Leader != "quorum-1" {
		t.Errorf("leader = %q, want quorum-1 (term 3 beats term 2)", st.Leader)
	}
}

// Computing the status again from the same facts must give an identical
// result, including condition timestamps. Otherwise every reconcile would
// write the status.
func TestComputeStatusIsStable(t *testing.T) {
	qc := testCluster()
	nodes := nodesOf(healthyNodes())
	first := computeStatus(qc, 3, nodes)
	qc.Status = first
	second := computeStatus(qc, 3, nodes)
	if !slices.EqualFunc(first.Conditions, second.Conditions, func(a, b metav1.Condition) bool { return a == b }) {
		t.Errorf("conditions changed:\n%+v\n%+v", first.Conditions, second.Conditions)
	}
}

func TestQueryNodesUsesStableDNSNames(t *testing.T) {
	var mu sync.Mutex
	var urls []string
	fetch := func(_ context.Context, url string) (NodeStatus, error) {
		mu.Lock()
		urls = append(urls, url)
		mu.Unlock()
		return NodeStatus{}, nil
	}
	got := queryNodes(context.Background(), fetch, testCluster(), "cluster.local")
	if len(got) != 3 {
		t.Fatalf("got %d answers, want 3", len(got))
	}
	slices.Sort(urls)
	want := "http://quorum-0.quorum-headless.db.svc.cluster.local:8080/status"
	if urls[0] != want {
		t.Errorf("url = %q, want %q", urls[0], want)
	}
}

func TestFetchStatusHTTP(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/status" {
			http.NotFound(w, r)
			return
		}
		// A real answer from quorum-server, shortened.
		w.Write([]byte(`{"node_id":"quorum-0","state":"leader","term":7,"leader_id":"quorum-0",` +
			`"commit_index":12,"members":[{"id":"quorum-0","raft_addr":"a:7000","voter":true}]}`))
	}))
	defer srv.Close()

	st, err := FetchStatusHTTP(context.Background(), srv.URL+"/status")
	if err != nil {
		t.Fatal(err)
	}
	if st.NodeID != "quorum-0" || st.State != "leader" || st.Term != 7 || len(st.Members) != 1 {
		t.Errorf("decoded %+v", st)
	}
	if _, err := FetchStatusHTTP(context.Background(), srv.URL+"/nope"); err == nil {
		t.Error("want an error for a non-200 answer")
	}
}
