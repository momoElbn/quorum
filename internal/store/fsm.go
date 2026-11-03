package store

import (
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"sync"

	"github.com/hashicorp/raft"
)

// command is one change to the map. Commands are written to the Raft log as
// JSON, and every node decodes and applies them in the same order.
type command struct {
	Op    string `json:"op"` // "set" or "delete"
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

// fsm is the "finite state machine" Raft drives: a plain map that changes
// only when Raft hands it a committed log entry. All nodes start from the
// same state and apply the same entries in the same order, so they all end
// up with the same map.
type fsm struct {
	// Raft calls Apply from its own goroutine while HTTP handlers call get
	// from theirs, so the map needs a lock.
	mu   sync.RWMutex
	data map[string]string
}

func newFSM() *fsm {
	return &fsm{data: make(map[string]string)}
}

// Apply is called by Raft once a log entry is committed (stored by a
// majority). It must be deterministic: if it behaved differently on
// different nodes, their maps would drift apart.
//
// The return value goes back to the caller of raft.Apply on the leader. We
// return an error for a command we can't understand, instead of panicking,
// so one bad entry can't crash every node in the cluster.
func (f *fsm) Apply(l *raft.Log) interface{} {
	var c command
	if err := json.Unmarshal(l.Data, &c); err != nil {
		return fmt.Errorf("decode command: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	switch c.Op {
	case "set":
		f.data[c.Key] = c.Value
	case "delete":
		delete(f.data, c.Key)
	default:
		return fmt.Errorf("unknown op %q", c.Op)
	}
	return nil
}

// get reads one key from this node's local copy of the map.
func (f *fsm) get(key string) (string, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	v, ok := f.data[key]
	return v, ok
}

// Snapshot captures the current map so Raft can later throw away the log
// entries that built it.
//
// Raft does not call Apply while Snapshot runs, so Snapshot should be quick.
// We copy the map here (fast, in memory) and do the slow disk write later in
// Persist. Apply can run during Persist, which is fine because Persist works
// on the copy.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return &fsmSnapshot{data: maps.Clone(f.data)}, nil
}

// Restore replaces the whole map with the contents of a snapshot. Raft calls
// it on startup (to load the latest snapshot) and when a follower is so far
// behind that the leader sends it a snapshot instead of log entries.
func (f *fsm) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	data := make(map[string]string)
	if err := json.NewDecoder(rc).Decode(&data); err != nil {
		return fmt.Errorf("decode snapshot: %w", err)
	}

	// Swap in the new map instead of merging: keys that are not in the
	// snapshot must disappear.
	f.mu.Lock()
	f.data = data
	f.mu.Unlock()
	return nil
}

// fsmSnapshot is a frozen copy of the map, waiting to be written to disk.
type fsmSnapshot struct {
	data map[string]string
}

// Persist writes the snapshot to the sink Raft gives us (a file in the
// snapshot store).
func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if err := json.NewEncoder(sink).Encode(s.data); err != nil {
		// Cancel tells Raft to throw away this half-written snapshot, so a
		// broken file is never used for a restore.
		sink.Cancel()
		return fmt.Errorf("write snapshot: %w", err)
	}
	return sink.Close()
}

// Release is called when Raft is done with the snapshot. The copy is plain
// memory that the garbage collector frees, so there is nothing to do.
func (s *fsmSnapshot) Release() {}
