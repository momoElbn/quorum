package controller

// This file turns what the pods report on GET /status into the
// QuorumCluster's status (phase, leader, members, conditions).

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	quorumv1alpha1 "github.com/momoElbn/quorum/operator/api/v1alpha1"
)

// NodeStatus is the part of quorum-server's GET /status answer the operator
// uses. It mirrors store.Status in the server. The operator is a separate Go
// module and can't import the server's internal package, so the fields are
// copied here.
type NodeStatus struct {
	NodeID   string `json:"node_id"`
	State    string `json:"state"` // "leader", "follower", "candidate", ...
	Term     uint64 `json:"term"`
	LeaderID string `json:"leader_id"`
	Members  []struct {
		ID string `json:"id"`
	} `json:"members"`
}

// StatusFetcher asks one pod for its status. It is a function type so tests
// can pass a fake instead of making real HTTP calls.
type StatusFetcher func(ctx context.Context, url string) (NodeStatus, error)

// httpClient is shared so connections are reused between reconciles. The
// short timeout keeps one dead pod from slowing down every reconcile.
var httpClient = &http.Client{Timeout: 2 * time.Second}

// FetchStatusHTTP is the real StatusFetcher: GET <url> and decode the JSON.
func FetchStatusHTTP(ctx context.Context, url string) (NodeStatus, error) {
	var st NodeStatus
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return st, err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return st, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return st, fmt.Errorf("GET %s: %s", url, resp.Status)
	}
	err = json.NewDecoder(resp.Body).Decode(&st)
	return st, err
}

// queryNodes asks every pod for its /status, all at once, and returns the
// answers it got. Pods that don't answer (not started, crashed,
// unreachable) are skipped: their absence shows up in the status as "not
// ready" or "not a member".
func queryNodes(ctx context.Context, fetch StatusFetcher, qc *quorumv1alpha1.QuorumCluster, clusterDomain string) []NodeStatus {
	var (
		mu      sync.Mutex
		wg      sync.WaitGroup
		answers []NodeStatus
	)
	for i := int32(0); i < qc.Spec.Replicas; i++ {
		url := fmt.Sprintf("http://%s:%d/status", podHost(qc, i, clusterDomain), httpPort)
		wg.Go(func() {
			st, err := fetch(ctx, url)
			if err != nil {
				return
			}
			mu.Lock()
			answers = append(answers, st)
			mu.Unlock()
		})
	}
	wg.Wait()
	return answers
}

// computeStatus works out the new status from the facts we observed:
// the pods' readiness (from the StatefulSet) and their /status answers.
// It's a pure function, which keeps it easy to test.
//
// Conditions start from the current ones. meta.SetStatusCondition only
// changes lastTransitionTime when a condition's status actually flips, so
// computing the same status twice gives exactly the same result. Without
// that, every reconcile would write a new timestamp.
func computeStatus(qc *quorumv1alpha1.QuorumCluster, readyReplicas int32, nodes []NodeStatus) quorumv1alpha1.QuorumClusterStatus {
	st := quorumv1alpha1.QuorumClusterStatus{
		ReadyReplicas:      readyReplicas,
		ObservedGeneration: qc.Generation,
		Conditions:         slices.Clone(qc.Status.Conditions),
	}

	// Trust only a node that says it is the leader itself; a follower may
	// still name an old leader. If two nodes claim it (an old leader cut off
	// from the others hasn't noticed yet), the higher term is the real one:
	// Raft never has two leaders in the same term.
	var leader *NodeStatus
	for i := range nodes {
		if nodes[i].State == "leader" && (leader == nil || nodes[i].Term > leader.Term) {
			leader = &nodes[i]
		}
	}
	if leader != nil {
		st.Leader = leader.NodeID
		// The leader's membership list is the authoritative one.
		for _, m := range leader.Members {
			st.Members = append(st.Members, m.ID)
		}
		slices.Sort(st.Members) // stable order, so the status doesn't flap
	}

	// Healthy means: every pod is ready, and every pod is a Raft member.
	healthy := leader != nil && readyReplicas == qc.Spec.Replicas
	for i := int32(0); i < qc.Spec.Replicas && healthy; i++ {
		healthy = slices.Contains(st.Members, podName(qc, i))
	}

	switch {
	case leader == nil && readyReplicas == 0:
		st.Phase = quorumv1alpha1.PhasePending
	case leader == nil:
		st.Phase = quorumv1alpha1.PhaseUnavailable
	case healthy:
		st.Phase = quorumv1alpha1.PhaseRunning
	default:
		st.Phase = quorumv1alpha1.PhaseDegraded
	}

	available := metav1.Condition{
		Type:               quorumv1alpha1.ConditionAvailable,
		Status:             metav1.ConditionFalse,
		Reason:             "NoLeader",
		Message:            "no Raft leader: reads and writes fail until one is elected",
		ObservedGeneration: qc.Generation,
	}
	if leader != nil {
		available.Status = metav1.ConditionTrue
		available.Reason = "LeaderElected"
		available.Message = leader.NodeID + " is the leader"
	}
	meta.SetStatusCondition(&st.Conditions, available)

	degraded := metav1.Condition{
		Type:               quorumv1alpha1.ConditionDegraded,
		Status:             metav1.ConditionTrue,
		Reason:             "NotAllHealthy",
		Message:            fmt.Sprintf("%d/%d pods ready, %d/%d Raft members", readyReplicas, qc.Spec.Replicas, len(st.Members), qc.Spec.Replicas),
		ObservedGeneration: qc.Generation,
	}
	if healthy {
		degraded.Status = metav1.ConditionFalse
		degraded.Reason = "AllHealthy"
	}
	meta.SetStatusCondition(&st.Conditions, degraded)

	return st
}
