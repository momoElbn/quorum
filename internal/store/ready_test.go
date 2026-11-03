package store

import (
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func TestReadinessCheck(t *testing.T) {
	// A healthy follower; each case changes one thing.
	ok := readiness{
		state:           raft.Follower,
		leaderID:        "node0",
		sinceContact:    100 * time.Millisecond,
		leaderHTTPKnown: true,
		voter:           true,
		commitIndex:     500,
		appliedIndex:    450,
	}
	cases := []struct {
		name    string
		change  func(*readiness)
		wantErr string // "" means ready
	}{
		{"healthy follower", func(*readiness) {}, ""},
		{"healthy leader", func(r *readiness) { r.state = raft.Leader; r.sinceContact = -1 }, ""},
		{"lag exactly at limit", func(r *readiness) { r.appliedIndex = r.commitIndex - maxApplyLag }, ""},
		{"candidate", func(r *readiness) { r.state = raft.Candidate }, "raft state is candidate"},
		{"shut down", func(r *readiness) { r.state = raft.Shutdown }, "raft state is shutdown"},
		{"no leader", func(r *readiness) { r.leaderID = "" }, "no leader known"},
		{"never heard from leader", func(r *readiness) { r.sinceContact = -1 }, "no contact with the leader yet"},
		{"leader silent too long", func(r *readiness) { r.sinceContact = maxLeaderSilence }, "no contact with the leader for 3s"},
		{"no HTTP address for the leader", func(r *readiness) { r.leaderHTTPKnown = false }, "no HTTP address for leader node0"},
		{"not a voter", func(r *readiness) { r.voter = false }, "not a voter"},
		{"too far behind", func(r *readiness) { r.appliedIndex = r.commitIndex - maxApplyLag - 1 }, "101 entries behind"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := ok
			c.change(&r)
			err := r.check()
			switch {
			case c.wantErr == "" && err != nil:
				t.Errorf("check() = %v, want ready", err)
			case c.wantErr != "" && (err == nil || !strings.Contains(err.Error(), c.wantErr)):
				t.Errorf("check() = %v, want an error containing %q", err, c.wantErr)
			}
		})
	}
}

// A node that has never joined isn't ready, whatever else is true.
func TestReadyBeforeJoin(t *testing.T) {
	s := openStore(t, "node1", t.TempDir(), "", false)
	if err := s.Ready(); err == nil || !strings.Contains(err.Error(), "no leader known") {
		t.Fatalf("Ready() = %v, want \"no leader known\"", err)
	}
}

func waitReady(t *testing.T, s *Store) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	var err error
	for time.Now().Before(deadline) {
		if err = s.Ready(); err == nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("%s never became ready: %v", s.cfg.NodeID, err)
}

// A follower that restarts comes back knowing its membership (it is still a
// voter on disk) but not who the leader is. It must stay unready until the
// leader has contacted it, or Kubernetes would route clients to a node that
// can only answer "no leader".
func TestRestartedNodeNotReadyUntilLeaderContact(t *testing.T) {
	if testing.Short() {
		t.Skip("starts a 3-node cluster")
	}
	nodes := newCluster(t, 3)
	leader := waitForLeader(t, nodes)
	for _, n := range nodes {
		waitReady(t, n)
	}

	f := without(nodes, leader)[0]
	id, dir, addr := f.cfg.NodeID, f.cfg.DataDir, f.RaftAddr()
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	// Writes the node misses while it's down. Staying down for a second
	// also lets the leader's retry backoff for this peer grow to a few
	// hundred milliseconds, so the first heartbeat arrives well after our
	// first check below rather than racing it.
	for i := range 10 {
		if err := leader.Set("k", string(rune('a'+i))); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(time.Second)

	// Same address: it is part of the stored membership.
	s := openStore(t, id, dir, addr, false)
	if err := s.Ready(); err == nil {
		t.Fatal("restarted node is ready before hearing from the leader")
	}
	waitReady(t, s)
	if s.State() == "follower" && s.raft.LastContact().IsZero() {
		t.Error("follower became ready without any contact from the leader")
	}
}
