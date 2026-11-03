package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeKV stands in for a quorum cluster's leader. It can be told to fail
// writes, to lose some acknowledged writes, or to apply a write but lose
// the answer, so each of the checker's verdicts can be tested.
type fakeKV struct {
	down         atomic.Bool // answer 503 to writes, like a cluster without a leader
	failReads    atomic.Bool // answer 503 to reads
	dropEvery    int         // acknowledge but don't store every Nth write (a bug)
	hideAckEvery int         // store but answer 503 to every Nth write (lost answer)

	mu      sync.Mutex
	data    map[string]string
	writes  int
	dropped []string
}

func newFakeKV() *fakeKV { return &fakeKV{data: map[string]string{}} }

func (f *fakeKV) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	switch r.Method {
	case http.MethodPut:
		if f.down.Load() {
			http.Error(w, "no leader", http.StatusServiceUnavailable)
			return
		}
		body, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		f.writes++
		n := f.writes
		if f.dropEvery > 0 && n%f.dropEvery == 0 {
			f.dropped = append(f.dropped, key)
		} else {
			f.data[key] = string(body)
		}
		f.mu.Unlock()
		if f.hideAckEvery > 0 && n%f.hideAckEvery == 0 {
			http.Error(w, "leadership lost", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case http.MethodGet:
		if f.failReads.Load() {
			http.Error(w, "no leader", http.StatusServiceUnavailable)
			return
		}
		f.mu.Lock()
		v, ok := f.data[key]
		f.mu.Unlock()
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Write([]byte(v))
	}
}

func testConfig(url string, d time.Duration) Config {
	return Config{URL: url, Rate: 100, Duration: d, Timeout: time.Second, RunID: "test"}
}

// The happy path, with an outage in the middle: writes go through a
// "follower" that redirects to the "leader", the leader fails writes for a
// while, and the checker must report one window and no lost writes.
func TestOutageIsMeasuredAndNothingLost(t *testing.T) {
	kv := newFakeKV()
	leader := httptest.NewServer(kv)
	defer leader.Close()
	follower := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, leader.URL+r.URL.Path, http.StatusTemporaryRedirect)
	}))
	defer follower.Close()

	go func() {
		time.Sleep(300 * time.Millisecond)
		kv.down.Store(true)
		time.Sleep(300 * time.Millisecond)
		kv.down.Store(false)
	}()
	rep := Run(context.Background(), testConfig(follower.URL, time.Second), &http.Client{})
	s := rep.Summary

	if !s.Passed || len(s.LostAcked) != 0 {
		t.Fatalf("want a pass with nothing lost, got %+v", s)
	}
	if s.Total < 50 || s.Acked == 0 || s.Acked == s.Total {
		t.Errorf("total=%d acked=%d: want some writes acked and some failed", s.Total, s.Acked)
	}
	if len(s.Windows) != 1 {
		t.Fatalf("windows = %+v, want exactly 1", s.Windows)
	}
	// The outage lasted ~300ms. Allow slack for a slow machine (-race).
	if d := s.Windows[0].DurationSeconds; d < 0.15 || d > 0.6 {
		t.Errorf("window = %.3fs, want about 0.3s", d)
	}
	leaderHost := strings.TrimPrefix(leader.URL, "http://")
	for _, r := range rep.Requests {
		if r.Acked && r.Node != leaderHost {
			t.Fatalf("acked write answered by %q, want the leader %q (redirect not followed?)", r.Node, leaderHost)
		}
	}
}

// A server that acknowledges writes and then forgets them must fail the
// check, and the checker must name exactly the forgotten keys.
func TestDetectsLostAcknowledgedWrites(t *testing.T) {
	kv := newFakeKV()
	kv.dropEvery = 5
	srv := httptest.NewServer(kv)
	defer srv.Close()

	rep := Run(context.Background(), testConfig(srv.URL, 300*time.Millisecond), &http.Client{})
	s := rep.Summary

	if s.Passed {
		t.Fatal("want a failure: the server dropped acknowledged writes")
	}
	lost, dropped := slices.Clone(s.LostAcked), slices.Clone(kv.dropped)
	slices.Sort(lost)
	slices.Sort(dropped)
	if len(dropped) == 0 || !slices.Equal(lost, dropped) {
		t.Errorf("lost = %v, want exactly the dropped keys %v", lost, dropped)
	}
}

// A write whose answer was lost (applied, but the client saw an error) is
// fine: a failed write means "unknown", not "didn't happen".
func TestUnacknowledgedButStoredIsNotALoss(t *testing.T) {
	kv := newFakeKV()
	kv.hideAckEvery = 4
	srv := httptest.NewServer(kv)
	defer srv.Close()

	s := Run(context.Background(), testConfig(srv.URL, 300*time.Millisecond), &http.Client{}).Summary
	if !s.Passed || s.UnackedButStored == 0 {
		t.Errorf("want a pass with some unacked-but-stored writes, got %+v", s)
	}
}

// If reads keep failing, the checker can't prove anything, so it must not
// pass.
func TestUnverifiableReadsFail(t *testing.T) {
	defer func(a int, b time.Duration) { readAttempts, firstReadBackoff = a, b }(readAttempts, firstReadBackoff)
	readAttempts, firstReadBackoff = 2, time.Millisecond

	kv := newFakeKV()
	kv.failReads.Store(true)
	srv := httptest.NewServer(kv)
	defer srv.Close()

	s := Run(context.Background(), testConfig(srv.URL, 100*time.Millisecond), &http.Client{}).Summary
	if s.Passed || len(s.Unverified) != s.Acked || s.Acked == 0 {
		t.Errorf("want a failure with every acked key unverified, got passed=%v unverified=%d acked=%d", s.Passed, len(s.Unverified), s.Acked)
	}
}

func TestUnavailabilityWindows(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	at := func(ms int) time.Time { return t0.Add(time.Duration(ms) * time.Millisecond) }
	req := func(seq, startMS int, acked bool) Request {
		return Request{Seq: seq, Start: at(startMS), End: at(startMS + 30), Acked: acked}
	}
	requests := []Request{
		req(0, 0, true),
		req(1, 100, false), // window 1 opens
		req(2, 200, false),
		req(3, 300, true), // window 1 closes at 300
		req(4, 400, true),
		req(5, 500, false), // window 2 opens, run ends inside it
		req(6, 600, false), // ends at 630
	}
	w := unavailabilityWindows(requests)
	if len(w) != 2 {
		t.Fatalf("got %d windows, want 2: %+v", len(w), w)
	}
	if !w[0].Start.Equal(at(100)) || !w[0].End.Equal(at(300)) || w[0].FailedWrites != 2 || w[0].DurationSeconds != 0.2 {
		t.Errorf("window 1 = %+v", w[0])
	}
	if !w[1].Start.Equal(at(500)) || !w[1].End.Equal(at(630)) || w[1].FailedWrites != 2 {
		t.Errorf("window 2 = %+v", w[1])
	}
	if len(unavailabilityWindows([]Request{req(0, 0, true)})) != 0 {
		t.Error("no failures should give no windows")
	}
}

func TestMedian(t *testing.T) {
	cases := []struct {
		in   []float64
		want float64
	}{
		{nil, 0},
		{[]float64{3, 1, 2}, 2},
		{[]float64{4, 1, 3, 2}, 2.5},
	}
	for _, c := range cases {
		if got := median(c.in); got != c.want {
			t.Errorf("median(%v) = %v, want %v", c.in, got, c.want)
		}
	}
}
