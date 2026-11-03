package store

import (
	"errors"
	"io"
	"testing"
	"time"
)

// testPeers lists every node ID the tests use, so Ready finds the leader in
// it. The store never dials these addresses; only the HTTP layer does.
var testPeers = map[string]string{"node0": "unused", "node1": "unused", "node2": "unused"}

// openStore starts one node on a free localhost port. bind may be empty to
// pick a random port.
func openStore(t *testing.T, id, dir, bind string, bootstrap bool) *Store {
	t.Helper()
	if bind == "" {
		bind = "127.0.0.1:0"
	}
	s := New(Config{NodeID: id, RaftBind: bind, DataDir: dir, Peers: testPeers, LogOutput: io.Discard})
	if err := s.Open(bootstrap); err != nil {
		t.Fatalf("open %s: %v", id, err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// waitForLeader polls until exactly one of the stores is leader. Elections
// take a second or two with Raft's default timeouts.
func waitForLeader(t *testing.T, stores []*Store) *Store {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		var leaders []*Store
		for _, s := range stores {
			if s.State() == "leader" {
				leaders = append(leaders, s)
			}
		}
		if len(leaders) == 1 {
			return leaders[0]
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("no leader elected")
	return nil
}

// newCluster starts n nodes: node0 bootstraps, the rest join it.
func newCluster(t *testing.T, n int) []*Store {
	t.Helper()
	first := openStore(t, "node0", t.TempDir(), "", true)
	waitForLeader(t, []*Store{first})

	stores := []*Store{first}
	for i := 1; i < n; i++ {
		id := "node" + string(rune('0'+i))
		s := openStore(t, id, t.TempDir(), "", false)
		if err := first.Join(id, s.RaftAddr()); err != nil {
			t.Fatalf("join %s: %v", id, err)
		}
		stores = append(stores, s)
	}
	return stores
}

func without(stores []*Store, drop *Store) []*Store {
	var out []*Store
	for _, s := range stores {
		if s != drop {
			out = append(out, s)
		}
	}
	return out
}

// The main test: a write acknowledged by the leader must survive the
// leader's death.
func TestClusterSurvivesLeaderFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("starts a 3-node cluster and waits for elections")
	}
	nodes := newCluster(t, 3)
	leader := waitForLeader(t, nodes)

	st, err := leader.Status()
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Members) != 3 {
		t.Fatalf("members = %v, want 3", st.Members)
	}

	if err := leader.Set("color", "blue"); err != nil {
		t.Fatalf("set: %v", err)
	}

	// Followers refuse reads and writes so the HTTP layer can redirect.
	follower := without(nodes, leader)[0]
	if _, _, err := follower.Get("color"); !errors.Is(err, ErrNotLeader) {
		t.Errorf("follower Get err = %v, want ErrNotLeader", err)
	}
	if err := follower.Set("color", "red"); !errors.Is(err, ErrNotLeader) {
		t.Errorf("follower Set err = %v, want ErrNotLeader", err)
	}

	// Kill the leader. The other two are still a majority of 3, so they
	// elect a new leader. Raft only elects a node whose log has every
	// committed entry, so the new leader must have "color".
	leader.Close()
	rest := without(nodes, leader)
	newLeader := waitForLeader(t, rest)

	v, ok, err := newLeader.Get("color")
	if err != nil || !ok || v != "blue" {
		t.Fatalf("Get on new leader = %q, %v, %v; want \"blue\", true, nil", v, ok, err)
	}

	// The cluster still takes writes with one node down.
	if err := newLeader.Delete("color"); err != nil {
		t.Fatalf("delete on new leader: %v", err)
	}
	if _, ok, _ := newLeader.Get("color"); ok {
		t.Error("color still present after delete")
	}
	if got := newLeader.LeaderChanges(); got == 0 {
		t.Error("LeaderChanges = 0, want at least 1")
	}
}

func TestJoinIsIdempotentAndRemoveWorks(t *testing.T) {
	if testing.Short() {
		t.Skip("starts a 3-node cluster")
	}
	nodes := newCluster(t, 3)
	leader := waitForLeader(t, nodes)

	// Joining again with the same ID and address must change nothing.
	n1 := nodes[1]
	if err := leader.Join("node1", n1.RaftAddr()); err != nil {
		t.Fatalf("second join: %v", err)
	}
	if st, _ := leader.Status(); len(st.Members) != 3 {
		t.Fatalf("members after repeat join = %v, want 3", st.Members)
	}

	// Remove a follower.
	victim := without(nodes, leader)[0]
	if err := leader.Remove(victim.cfg.NodeID); err != nil {
		t.Fatalf("remove: %v", err)
	}
	st, _ := leader.Status()
	if len(st.Members) != 2 {
		t.Fatalf("members after remove = %v, want 2", st.Members)
	}
	for _, m := range st.Members {
		if m.ID == victim.cfg.NodeID {
			t.Fatalf("%s still a member", m.ID)
		}
	}
}

// A restarted node with BOOTSTRAP=true must keep its data, not try to
// create a fresh cluster.
func TestRestartKeepsDataAndSkipsBootstrap(t *testing.T) {
	if testing.Short() {
		t.Skip("waits for elections")
	}
	dir := t.TempDir()
	s := openStore(t, "node0", dir, "", true)
	waitForLeader(t, []*Store{s})
	if err := s.Set("k", "v"); err != nil {
		t.Fatal(err)
	}
	addr := s.RaftAddr()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Same port: the address is part of the stored membership.
	s2 := openStore(t, "node0", dir, addr, true)
	waitForLeader(t, []*Store{s2})
	v, ok, err := s2.Get("k")
	if err != nil || !ok || v != "v" {
		t.Fatalf("Get after restart = %q, %v, %v; want \"v\", true, nil", v, ok, err)
	}
}
