package store

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/hashicorp/raft"
)

const (
	// maxLeaderSilence is how long a follower may go without hearing from
	// the leader and still call itself ready. The leader sends a heartbeat
	// well within Raft's 1s heartbeat timeout, so 3s means several missed
	// heartbeats in a row: the follower is probably cut off, not just slow.
	maxLeaderSilence = 3 * time.Second

	// maxApplyLag is how many committed entries a node may still have to
	// apply to its map. A node far behind (e.g. replaying after a restart)
	// isn't wrong, but it can't help clients until it has caught up.
	maxApplyLag = 100
)

// readiness is the part of a node's Raft state that Ready looks at. Ready
// collects it from Raft and check decides, so the decision can be tested
// with any combination of values, without a real cluster.
type readiness struct {
	state           raft.RaftState
	leaderID        string
	sinceContact    time.Duration // since the last message from the leader; < 0 means never
	leaderHTTPKnown bool          // false: followers couldn't redirect clients to the leader
	voter           bool
	commitIndex     uint64
	appliedIndex    uint64
}

// Ready reports whether this node should receive client traffic, which is
// what the Kubernetes readiness probe (GET /readyz) asks. It returns nil
// when the node is ready and an error naming the first failing check
// otherwise.
//
// Readiness is stricter than liveness (GET /healthz): a node that isn't
// ready is still healthy and must not be restarted. It is only taken out of
// the client Service until it can answer requests or redirect them.
func (s *Store) Ready() error {
	f := s.raft.GetConfiguration()
	if err := f.Error(); err != nil {
		return fmt.Errorf("read raft configuration: %w", err)
	}
	r := readiness{
		state:        s.raft.State(),
		sinceContact: -1,
		commitIndex:  s.raft.CommitIndex(),
		appliedIndex: s.raft.AppliedIndex(),
	}
	r.leaderID, _, r.leaderHTTPKnown = s.LeaderHTTPAddr()
	if last := s.raft.LastContact(); !last.IsZero() {
		r.sinceContact = time.Since(last)
	}
	for _, srv := range f.Configuration().Servers {
		if srv.ID == raft.ServerID(s.cfg.NodeID) && srv.Suffrage == raft.Voter {
			r.voter = true
		}
	}
	return r.check()
}

// check applies the readiness rules in order and returns the first one that
// fails.
func (r readiness) check() error {
	// Candidates are in the middle of an election and know no leader;
	// "shutdown" means Raft has stopped.
	if r.state != raft.Leader && r.state != raft.Follower {
		return fmt.Errorf("raft state is %s", strings.ToLower(r.state.String()))
	}
	if r.leaderID == "" {
		return errors.New("no leader known")
	}
	// A follower remembers the last leader it heard of even after losing
	// contact, so a known leader alone doesn't prove the follower is
	// connected. A node that just restarted hasn't heard from anyone yet.
	if r.state == raft.Follower {
		if r.sinceContact < 0 {
			return errors.New("no contact with the leader yet")
		}
		if r.sinceContact >= maxLeaderSilence {
			return fmt.Errorf("no contact with the leader for %s", r.sinceContact.Round(time.Millisecond))
		}
	}
	if !r.leaderHTTPKnown {
		return fmt.Errorf("no HTTP address for leader %s (not in PEERS_HTTP, and none can be derived from its Raft address), so clients can't be redirected to it", r.leaderID)
	}
	// A node that hasn't joined yet (or was removed, or is a non-voter)
	// isn't part of the majority, so it shouldn't count as ready.
	if !r.voter {
		return errors.New("this node is not a voter in the current configuration")
	}
	if r.commitIndex > r.appliedIndex && r.commitIndex-r.appliedIndex > maxApplyLag {
		return fmt.Errorf("applied index %d is %d entries behind commit index %d",
			r.appliedIndex, r.commitIndex-r.appliedIndex, r.commitIndex)
	}
	return nil
}
