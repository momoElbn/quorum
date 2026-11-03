package store

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"

	"github.com/hashicorp/raft"
)

// applyCmd feeds one command to the FSM the way Raft would after it is
// committed.
func applyCmd(t *testing.T, f *fsm, c command) interface{} {
	t.Helper()
	data, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	return f.Apply(&raft.Log{Data: data})
}

func TestFSMSetAndDelete(t *testing.T) {
	f := newFSM()

	if res := applyCmd(t, f, command{Op: "set", Key: "a", Value: "1"}); res != nil {
		t.Fatalf("set returned %v", res)
	}
	if v, ok := f.get("a"); !ok || v != "1" {
		t.Fatalf("get(a) = %q, %v; want \"1\", true", v, ok)
	}

	// Setting again overwrites.
	applyCmd(t, f, command{Op: "set", Key: "a", Value: "2"})
	if v, _ := f.get("a"); v != "2" {
		t.Fatalf("get(a) = %q, want \"2\"", v)
	}

	if res := applyCmd(t, f, command{Op: "delete", Key: "a"}); res != nil {
		t.Fatalf("delete returned %v", res)
	}
	if _, ok := f.get("a"); ok {
		t.Fatal("a still present after delete")
	}

	// Deleting a missing key is fine: the same log may be replayed after a
	// restart, so applying must not fail the second time.
	if res := applyCmd(t, f, command{Op: "delete", Key: "a"}); res != nil {
		t.Fatalf("delete of missing key returned %v", res)
	}
}

func TestFSMRejectsBadCommands(t *testing.T) {
	f := newFSM()

	if _, ok := applyCmd(t, f, command{Op: "explode", Key: "a"}).(error); !ok {
		t.Error("unknown op: want an error response")
	}
	if _, ok := f.Apply(&raft.Log{Data: []byte("not json")}).(error); !ok {
		t.Error("bad JSON: want an error response")
	}
	if len(f.data) != 0 {
		t.Errorf("bad commands changed the map: %v", f.data)
	}
}

// memSink is an in-memory raft.SnapshotSink, so the test doesn't need files.
type memSink struct {
	bytes.Buffer
	cancelled bool
}

func (s *memSink) ID() string    { return "test" }
func (s *memSink) Close() error  { return nil }
func (s *memSink) Cancel() error { s.cancelled = true; return nil }

func TestFSMSnapshotAndRestore(t *testing.T) {
	f := newFSM()
	applyCmd(t, f, command{Op: "set", Key: "a", Value: "1"})
	applyCmd(t, f, command{Op: "set", Key: "b", Value: "2"})

	snap, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	// A write after Snapshot must not leak into it: Raft keeps applying
	// entries while Persist runs.
	applyCmd(t, f, command{Op: "set", Key: "c", Value: "3"})

	var sink memSink
	if err := snap.Persist(&sink); err != nil {
		t.Fatal(err)
	}
	snap.Release()

	// Restore into an FSM that already has other data. Restore must
	// replace the map, not merge into it.
	g := newFSM()
	applyCmd(t, g, command{Op: "set", Key: "stale", Value: "x"})
	if err := g.Restore(io.NopCloser(&sink)); err != nil {
		t.Fatal(err)
	}

	want := map[string]string{"a": "1", "b": "2"}
	if len(g.data) != len(want) {
		t.Fatalf("restored map = %v, want %v", g.data, want)
	}
	for k, v := range want {
		if got, _ := g.get(k); got != v {
			t.Errorf("restored %s = %q, want %q", k, got, v)
		}
	}
}

func TestFSMRestoreRejectsGarbage(t *testing.T) {
	f := newFSM()
	applyCmd(t, f, command{Op: "set", Key: "a", Value: "1"})

	if err := f.Restore(io.NopCloser(bytes.NewBufferString("{broken"))); err == nil {
		t.Fatal("want an error for a corrupt snapshot")
	}
	// A failed restore must leave the old data alone.
	if v, _ := f.get("a"); v != "1" {
		t.Errorf("a = %q after failed restore, want \"1\"", v)
	}
}
