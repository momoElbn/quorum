// Package store is a key-value store replicated across nodes with Raft.
//
// Writes go through the Raft log, so every node applies them in the same
// order. Reads are served only by the leader, after it has checked that it
// still is the leader. See docs/decisions.md for the reasoning.
package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// ErrNotLeader means this node can't handle the request because it isn't
// the leader. The HTTP layer turns it into a redirect to the leader.
var ErrNotLeader = errors.New("not the leader")

const (
	// applyTimeout is how long we wait for Raft to accept a command, a
	// barrier or a membership change before giving up.
	applyTimeout = 10 * time.Second

	// retainSnapshots is how many snapshots stay on disk. Keeping more than
	// one means we still have a good snapshot if the newest file is damaged.
	retainSnapshots = 2

	// maxPool is how many TCP connections we keep open to each peer.
	// Raft can use several at once, for example to send log entries and a
	// snapshot in parallel.
	maxPool = 3
)

// Config describes one node.
type Config struct {
	NodeID string // unique and stable across restarts, e.g. "node1"

	// RaftAddr is the address the other nodes use to reach this node's Raft
	// port, e.g. "node1:7000". It is stored in the cluster membership, so
	// it must stay the same when the node restarts. If it is empty, the
	// address the listener actually got is used (handy in tests that bind
	// to port 0).
	RaftAddr string

	// RaftBind is the address to listen on, e.g. ":7000". Empty means
	// RaftAddr.
	RaftBind string

	DataDir string // where the Raft log, stable store and snapshots live

	LogOutput io.Writer // where Raft writes its logs; nil means os.Stderr
}

// Store is one node of the replicated key-value store.
type Store struct {
	cfg       Config
	fsm       *fsm
	raft      *raft.Raft
	bolt      *raftboltdb.BoltStore
	transport *raft.NetworkTransport

	observer   *raft.Observer
	observerCh chan raft.Observation

	leaderChanges atomic.Uint64

	// barrierTerm is the last term in which a read ran Barrier. See Get.
	barrierTerm atomic.Uint64

	closeOnce sync.Once
	closeErr  error
}

// New returns a Store that isn't running yet. Call Open to start it.
func New(cfg Config) *Store {
	if cfg.LogOutput == nil {
		cfg.LogOutput = os.Stderr
	}
	if cfg.RaftBind == "" {
		cfg.RaftBind = cfg.RaftAddr
	}
	return &Store{cfg: cfg, fsm: newFSM()}
}

// Open sets up storage and networking and starts Raft.
//
// If bootstrap is true and the data dir has no Raft state yet, the node
// creates a brand-new cluster with itself as the only member. If state
// already exists, bootstrap is ignored. BOOTSTRAP=true usually stays set in
// the environment, so this is what happens every time the first node
// restarts. Bootstrapping again would try to create a second cluster on top
// of the existing one.
func (s *Store) Open(bootstrap bool) (err error) {
	if err := os.MkdirAll(s.cfg.DataDir, 0o755); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}

	// If any step below fails, undo the steps that already worked. Without
	// this, the bolt file would stay locked (bbolt allows only one open
	// handle) and the Raft port would stay bound, so a retry would fail.
	defer func() {
		if err == nil {
			return
		}
		if s.raft != nil {
			s.Close() // stops Raft, the transport and bolt
			return
		}
		if s.transport != nil {
			s.transport.Close()
		}
		if s.bolt != nil {
			s.bolt.Close()
		}
	}()

	conf := raft.DefaultConfig()
	conf.LocalID = raft.ServerID(s.cfg.NodeID)
	conf.LogOutput = s.cfg.LogOutput

	// One bbolt file stores both the log and the "stable store". The stable
	// store holds the current term and who we voted for. Raft must keep
	// these on disk: if a node forgot its vote after a restart, it could
	// vote twice in one term and help elect two leaders.
	s.bolt, err = raftboltdb.NewBoltStore(filepath.Join(s.cfg.DataDir, "raft.db"))
	if err != nil {
		return fmt.Errorf("open bolt store: %w", err)
	}

	snaps, err := raft.NewFileSnapshotStore(s.cfg.DataDir, retainSnapshots, s.cfg.LogOutput)
	if err != nil {
		return fmt.Errorf("open snapshot store: %w", err)
	}

	ln, err := net.Listen("tcp", s.cfg.RaftBind)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", s.cfg.RaftBind, err)
	}
	stream := &tcpStream{Listener: ln, advertise: s.cfg.RaftAddr}
	s.transport = raft.NewNetworkTransport(stream, maxPool, applyTimeout, s.cfg.LogOutput)

	// Check before NewRaft: once Raft is running we can't tell any more
	// whether the state was already on disk.
	hasState, err := raft.HasExistingState(s.bolt, s.bolt, snaps)
	if err != nil {
		return fmt.Errorf("check existing state: %w", err)
	}

	// Count leader changes for the quorum_leader_changes_total metric. The
	// observer gets a LeaderObservation every time this node sees the
	// leader change. blocking=false: if we fall behind, Raft drops the
	// observation instead of waiting for us.
	s.observerCh = make(chan raft.Observation, 16)
	s.observer = raft.NewObserver(s.observerCh, false, func(o *raft.Observation) bool {
		_, ok := o.Data.(raft.LeaderObservation)
		return ok
	})
	go s.watchLeader()

	raftNode, err := raft.NewRaft(conf, s.fsm, s.bolt, s.bolt, snaps, s.transport)
	if err != nil {
		close(s.observerCh) // stops watchLeader; Raft never saw the channel
		return fmt.Errorf("start raft: %w", err)
	}
	s.raft = raftNode
	s.raft.RegisterObserver(s.observer)

	if bootstrap && !hasState {
		// The first configuration contains only this node. With one voter,
		// a majority is 1, so it elects itself leader and can accept joins.
		boot := raft.Configuration{Servers: []raft.Server{{
			ID:      conf.LocalID,
			Address: s.transport.LocalAddr(),
		}}}
		if err := s.raft.BootstrapCluster(boot).Error(); err != nil {
			return fmt.Errorf("bootstrap: %w", err)
		}
	}
	return nil
}

// watchLeader counts leader changes until the observer channel is closed.
func (s *Store) watchLeader() {
	for o := range s.observerCh {
		// While an election runs, the leader is briefly unknown (empty).
		// We only count the moments when a leader is actually in place.
		if lo, ok := o.Data.(raft.LeaderObservation); ok && lo.LeaderID != "" {
			s.leaderChanges.Add(1)
		}
	}
}

// Close stops Raft and closes the files. Calling it more than once is safe.
// It does not remove the node from the cluster: a node that shuts down is
// usually about to restart.
func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		// Deregister before shutting down, so Raft can't send to the
		// channel after we close it.
		s.raft.DeregisterObserver(s.observer)
		// Shutdown also closes the transport.
		s.closeErr = s.raft.Shutdown().Error()
		close(s.observerCh)
		if err := s.bolt.Close(); s.closeErr == nil {
			s.closeErr = err
		}
	})
	return s.closeErr
}

// Set stores key=value in the cluster.
func (s *Store) Set(key, value string) error {
	return s.apply(command{Op: "set", Key: key, Value: value})
}

// Delete removes key from the cluster. Deleting a missing key is not an
// error.
func (s *Store) Delete(key string) error {
	return s.apply(command{Op: "delete", Key: key})
}

// apply sends a command through Raft and waits until the leader has applied
// it to its own map. When this returns nil, a majority of nodes have the
// command on disk, so it survives the loss of any single node.
func (s *Store) apply(c command) error {
	data, err := json.Marshal(c)
	if err != nil {
		return err
	}
	// raft.Apply works only on the leader. Elsewhere it fails right away
	// with raft.ErrNotLeader.
	f := s.raft.Apply(data, applyTimeout)
	if err := f.Error(); err != nil {
		// Only ErrNotLeader becomes a redirect. ErrLeadershipLost means the
		// command may or may not have been committed, so we report an
		// error and let the client decide whether to retry.
		if errors.Is(err, raft.ErrNotLeader) {
			return ErrNotLeader
		}
		return err
	}
	// The value our FSM's Apply returned (nil, or an error for a bad
	// command).
	if err, ok := f.Response().(error); ok {
		return err
	}
	return nil
}

// Get returns the value for key. Only the leader answers, and the read is
// linearizable: it sees every write acknowledged before Get was called.
func (s *Store) Get(key string) (string, bool, error) {
	// A leader that has been cut off from the others may not know yet that
	// a new leader exists and is accepting writes. VerifyLeader asks a
	// majority to confirm we are still the leader before we trust our map.
	if err := s.raft.VerifyLeader().Error(); err != nil {
		return "", false, notLeader(err)
	}

	// A node that has just become leader may still hold committed entries
	// from the previous leader that it hasn't applied to its map yet.
	// Barrier waits until everything committed so far has been applied.
	// After that, the map stays current for the rest of the term, because
	// the leader acknowledges a write only after applying it. So the
	// barrier runs only on the first read of each term.
	term := s.raft.CurrentTerm()
	if s.barrierTerm.Load() != term {
		if err := s.raft.Barrier(applyTimeout).Error(); err != nil {
			return "", false, notLeader(err)
		}
		s.barrierTerm.Store(term)
	}

	v, ok := s.fsm.get(key)
	return v, ok, nil
}

// notLeader turns Raft's "lost leadership" errors into ErrNotLeader. For a
// read that is safe, because nothing was written: the client can simply ask
// the new leader.
func notLeader(err error) error {
	if errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLeadershipLost) {
		return ErrNotLeader
	}
	return err
}

// Join adds a node to the cluster as a voter. It is idempotent: joining
// again with the same ID and address does nothing, so a node can retry
// safely after a timeout.
func (s *Store) Join(id, raftAddr string) error {
	f := s.raft.GetConfiguration()
	if err := f.Error(); err != nil {
		return err
	}
	for _, srv := range f.Configuration().Servers {
		sameID := srv.ID == raft.ServerID(id)
		sameAddr := srv.Address == raft.ServerAddress(raftAddr)
		if sameID && sameAddr {
			return nil // already a member
		}
		// The ID or the address is in use by an older entry, for example
		// a node that came back with a new address. Remove the old entry
		// first, or two entries would point at one node and the cluster
		// would count a voter that can never answer.
		if sameID || sameAddr {
			if err := s.raft.RemoveServer(srv.ID, 0, applyTimeout).Error(); err != nil {
				return notLeader(err)
			}
		}
	}
	// prevIndex 0 means "apply this change whatever the current
	// configuration is". Only the leader can change membership.
	err := s.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(raftAddr), 0, applyTimeout).Error()
	return notLeader(err)
}

// Remove takes a node out of the cluster, so it no longer counts towards
// the majority. Only the leader can do this.
func (s *Store) Remove(id string) error {
	return notLeader(s.raft.RemoveServer(raft.ServerID(id), 0, applyTimeout).Error())
}

// Leader returns the ID and Raft address of the current leader, as far as
// this node knows. Both are empty while an election is in progress.
func (s *Store) Leader() (id, raftAddr string) {
	addr, sid := s.raft.LeaderWithID()
	return string(sid), string(addr)
}

// TransferLeadership asks Raft to hand leadership to another voter. A
// graceful shutdown calls it so the cluster has a new leader right away,
// instead of waiting for an election timeout. It does nothing if this node
// isn't the leader.
func (s *Store) TransferLeadership() error {
	if s.raft.State() != raft.Leader {
		return nil
	}
	return s.raft.LeadershipTransfer().Error()
}

// State is this node's Raft role, in lower case: "follower", "candidate",
// "leader" or "shutdown".
func (s *Store) State() string {
	return strings.ToLower(s.raft.State().String())
}

// LeaderChanges is how many times this node has seen a new leader take over.
func (s *Store) LeaderChanges() uint64 {
	return s.leaderChanges.Load()
}

// RaftAddr is the Raft address this node advertises to the others.
func (s *Store) RaftAddr() string {
	return string(s.transport.LocalAddr())
}

// Member is one server in the cluster configuration.
type Member struct {
	ID       string `json:"id"`
	RaftAddr string `json:"raft_addr"`
	Voter    bool   `json:"voter"`
}

// Status is a snapshot of this node's view of the cluster, for GET /status.
type Status struct {
	NodeID     string `json:"node_id"`
	State      string `json:"state"`
	Term       uint64 `json:"term"`
	LeaderID   string `json:"leader_id"`
	LeaderAddr string `json:"leader_addr"`
	// CommitIndex is the newest log entry this node knows is stored on a
	// majority. AppliedIndex is the newest entry applied to this node's map.
	// AppliedIndex trailing CommitIndex by a lot means the node is behind.
	CommitIndex  uint64   `json:"commit_index"`
	AppliedIndex uint64   `json:"applied_index"`
	Members      []Member `json:"members"`
}

// Status reports this node's view of the cluster. Any node can answer,
// and a follower's view may be slightly out of date.
func (s *Store) Status() (Status, error) {
	f := s.raft.GetConfiguration()
	if err := f.Error(); err != nil {
		return Status{}, err
	}
	members := []Member{}
	for _, srv := range f.Configuration().Servers {
		members = append(members, Member{
			ID:       string(srv.ID),
			RaftAddr: string(srv.Address),
			Voter:    srv.Suffrage == raft.Voter,
		})
	}
	leaderID, leaderAddr := s.Leader()
	return Status{
		NodeID:       s.cfg.NodeID,
		State:        s.State(),
		Term:         s.raft.CurrentTerm(),
		LeaderID:     leaderID,
		LeaderAddr:   leaderAddr,
		CommitIndex:  s.raft.CommitIndex(),
		AppliedIndex: s.raft.AppliedIndex(),
		Members:      members,
	}, nil
}

// tcpStream is the network layer under Raft's transport: plain TCP, but it
// advertises a hostname such as "node1:7000" instead of an IP.
//
// Why not raft.NewTCPTransport? It only accepts an IP as advertise address.
// The advertised address is stored in the cluster membership, and in
// Kubernetes a pod gets a new IP every time it restarts, so the membership
// would point at dead IPs. A StatefulSet's DNS name stays the same across
// restarts, and peers resolve it again every time they dial.
type tcpStream struct {
	net.Listener        // provides Accept and Close
	advertise    string // empty means the listener's real address
}

// Dial opens a connection to another node's Raft port.
func (t *tcpStream) Dial(addr raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", string(addr), timeout)
}

// Addr is the address Raft tells other nodes to use for this one.
func (t *tcpStream) Addr() net.Addr {
	if t.advertise == "" {
		return t.Listener.Addr()
	}
	return hostAddr(t.advertise)
}

// hostAddr is a net.Addr that is just a "host:port" string.
type hostAddr string

func (a hostAddr) Network() string { return "tcp" }
func (a hostAddr) String() string  { return string(a) }
