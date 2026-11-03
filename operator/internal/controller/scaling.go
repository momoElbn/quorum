package controller

// This file decides the next step of a safe scale operation. planScale is a
// pure function: it looks at what was observed (StatefulSet size, Raft
// membership from the leader, pod readiness) and returns at most one
// action. The reconciler carries it out, and the next reconcile observes
// the result and plans again. Doing one step per reconcile keeps every
// change small and visible, and means a crashed operator just picks up
// from what it observes.
//
// Why the steps are in this order (majority arithmetic):
//   - Scale up adds a nonvoter first. A new node has an empty log. If it
//     joined as a voter right away, 3 -> 4 voters would raise the majority
//     from 2 to 3 while the new node can't yet acknowledge anything, so one
//     more failure would stop all writes. As a nonvoter it catches up
//     without counting, and is promoted only once it's current.
//   - Scale down removes the member from Raft before deleting its pod.
//     Deleting the pod first would leave 5 voters with one dead: the
//     majority stays 3, but only 4 can answer, so the cluster tolerates one
//     failure instead of two, and a second deleted pod would leave 3 of 5:
//     no margin at all. Removing first shrinks the majority with the
//     membership, 5 -> 4 voters (majority 3) -> 3 voters (majority 2).

import (
	"fmt"
	"slices"

	quorumv1alpha1 "github.com/momoElbn/quorum/operator/api/v1alpha1"
)

// maxCatchUpLag is how many entries a nonvoter's applied index may trail
// the leader's commit index and still count as caught up. It matches the
// readiness probe's limit, so a promoted node is Ready right away.
const maxCatchUpLag = 100

type scaleAction int

const (
	actNone scaleAction = iota
	actSetReplicas
	actJoinNonvoter
	actPromote
	actTransferLeadership
	actRemove
)

// scaleInput is everything planScale looks at.
type scaleInput struct {
	current int32 // StatefulSet replicas now
	want    int32 // spec.replicas
	leader  *NodeStatus
	nodes   map[string]NodeStatus // pod name -> its /status, for pods that answered
	ready   map[string]bool       // pod name -> pod is Ready
	prev    quorumv1alpha1.ScalingStatus
}

// scalePlan is what planScale decided.
type scalePlan struct {
	action   scaleAction
	member   string // pod the step is about
	replicas int32  // new StatefulSet size, for actSetReplicas
	step     quorumv1alpha1.ScalingStep
	// wait means no step is possible yet; step and member stay as they
	// were. note then says why.
	wait bool
	note string // human-readable; becomes the event and condition message
}

func waitFor(prev quorumv1alpha1.ScalingStatus, format string, args ...any) scalePlan {
	return scalePlan{wait: true, step: prev.Step, member: prev.Member, note: fmt.Sprintf(format, args...)}
}

func planScale(qc *quorumv1alpha1.QuorumCluster, in scaleInput) scalePlan {
	// Never change membership without a leader: only the leader can, and
	// "no leader" usually means an election or a lost majority, which a
	// membership change would only make worse.
	if in.leader == nil {
		return waitFor(in.prev, "no Raft leader known")
	}
	members := map[string]bool{} // id -> voter
	var ids []string
	for _, m := range in.leader.Members {
		members[m.ID] = m.Voter
		ids = append(ids, m.ID)
	}
	slices.Sort(ids)
	// Every voter must be Ready before the next step. A voter that isn't
	// already reduces the margin; changing membership on top of that could
	// cost the majority. This is also how "wait until the promoted pod is
	// Ready" happens: it's a voter now, so nothing moves until it's Ready.
	for _, id := range ids {
		if members[id] && !in.ready[id] {
			return waitFor(in.prev, "voter %s is not Ready", id)
		}
	}

	// Pods 0..keep-1 should end up as voters. Pods at or above want are on
	// their way out, so they're never (re)joined or promoted.
	keep := min(in.current, in.want)

	// 1. A pod that should be a member but isn't: the one just added (or
	// quorum-1, quorum-2 at creation). Join it as a nonvoter once it
	// answers. Note we can't wait for it to be Ready: a pod that isn't a
	// voter never passes /readyz.
	for i := range keep {
		id := podName(qc, i)
		if _, ok := members[id]; ok {
			continue
		}
		if _, up := in.nodes[id]; !up {
			return scalePlan{step: quorumv1alpha1.StepAddingPod, member: id, wait: true,
				note: fmt.Sprintf("waiting for %s to answer /status", id)}
		}
		return scalePlan{action: actJoinNonvoter, step: quorumv1alpha1.StepCatchingUp, member: id,
			note: fmt.Sprintf("joined %s as a nonvoter; it is catching up on the log", id)}
	}

	// 2. A nonvoter: promote it once it has caught up on two checks in a
	// row (2 s apart). One check could catch it at a lucky moment.
	for i := range keep {
		id := podName(qc, i)
		if voter, ok := members[id]; !ok || voter {
			continue
		}
		n, up := in.nodes[id]
		if !up {
			return scalePlan{step: quorumv1alpha1.StepCatchingUp, member: id, wait: true,
				note: fmt.Sprintf("nonvoter %s doesn't answer /status", id)}
		}
		lag := lagBehind(in.leader.CommitIndex, n.AppliedIndex)
		if lag > maxCatchUpLag {
			return scalePlan{step: quorumv1alpha1.StepCatchingUp, member: id, wait: true,
				note: fmt.Sprintf("%s is %d entries behind the leader", id, lag)}
		}
		if in.prev.Step == quorumv1alpha1.StepPromoting && in.prev.Member == id {
			return scalePlan{action: actPromote, step: quorumv1alpha1.StepPromoting, member: id,
				note: fmt.Sprintf("promoted %s to voter; waiting for it to be Ready", id)}
		}
		return scalePlan{step: quorumv1alpha1.StepPromoting, member: id,
			note: fmt.Sprintf("%s caught up (%d entries behind); promoting it if it still is on the next check", id, lag)}
	}

	// Pods 0..keep-1 are all Ready voters now.
	switch {
	case in.want > in.current:
		id := podName(qc, in.current)
		return scalePlan{action: actSetReplicas, replicas: in.current + 1, step: quorumv1alpha1.StepAddingPod, member: id,
			note: fmt.Sprintf("raising the StatefulSet to %d pods for %s", in.current+1, id)}

	case in.want < in.current:
		// Always the highest ordinal: the StatefulSet can only delete its
		// last pod.
		id := podName(qc, in.current-1)
		if _, ok := members[id]; ok {
			if in.leader.NodeID == id {
				// Removing the leader itself would work in Raft, but the
				// cluster would then sit without a leader until an election
				// timeout. Handing over first costs no availability.
				return scalePlan{action: actTransferLeadership, step: quorumv1alpha1.StepTransferringLeadership, member: id,
					note: fmt.Sprintf("%s is the leader; transferring leadership before removing it", id)}
			}
			return scalePlan{action: actRemove, step: quorumv1alpha1.StepRemoving, member: id,
				note: fmt.Sprintf("removing %s from the Raft configuration", id)}
		}
		return scalePlan{action: actSetReplicas, replicas: in.current - 1, step: quorumv1alpha1.StepShrinkingPods, member: id,
			note: fmt.Sprintf("%s is out of Raft; lowering the StatefulSet to %d pods", id, in.current-1)}
	}
	return scalePlan{step: quorumv1alpha1.StepIdle,
		note: fmt.Sprintf("%d Raft voters, all Ready", in.current)}
}

// lagBehind is how far applied trails commit, 0 if it doesn't.
func lagBehind(commit, applied uint64) uint64 {
	if applied >= commit {
		return 0
	}
	return commit - applied
}
