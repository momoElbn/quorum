package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"
)

// Config controls one checker run.
type Config struct {
	URL      string        // base URL of the client Service, e.g. http://quorum:8080
	Rate     int           // writes per second
	Duration time.Duration // how long to keep writing
	Timeout  time.Duration // per request
	RunID    string        // prefix that makes this run's keys unique
}

// Request is one PUT, as the checker saw it.
type Request struct {
	Seq   int       `json:"seq"`
	Key   string    `json:"key"`
	Value string    `json:"value"`
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
	// Acked is true only for a 204 answer: the server confirmed the write is
	// committed (stored by a majority). Anything else is not a promise.
	Acked  bool   `json:"acked"`
	Status int    `json:"status,omitempty"` // HTTP status of the final answer; 0 if none
	Error  string `json:"error,omitempty"`  // network error or response body
	Node   string `json:"node,omitempty"`   // host that gave the final answer (after redirects)
}

// Readback is the result of reading one key back after the load phase.
type Readback struct {
	Key      string `json:"key"`
	Acked    bool   `json:"acked"`
	Outcome  string `json:"outcome"` // "found", "missing", "wrong_value", "unverified"
	Got      string `json:"got,omitempty"`
	Attempts int    `json:"attempts"`
	Error    string `json:"error,omitempty"`
}

// Window is a period during which writes failed: from the start of the
// first failed write to the start of the next acknowledged one.
type Window struct {
	Start           time.Time `json:"start"`
	End             time.Time `json:"end"`
	DurationSeconds float64   `json:"duration_seconds"`
	FailedWrites    int       `json:"failed_writes"`
}

// Summary is the verdict.
type Summary struct {
	Total int `json:"total_writes"`
	Acked int `json:"acknowledged"`
	// LostAcked lists acknowledged keys that were missing or had the wrong
	// value when read back. Must be empty.
	LostAcked []string `json:"lost_acknowledged"`
	// Unverified lists acknowledged keys whose read-back kept failing, so we
	// couldn't tell either way. Also makes the run fail.
	Unverified []string `json:"unverified"`
	// UnackedButStored counts writes that failed for the client but were
	// applied anyway (e.g. the answer was lost in a leader crash). Allowed:
	// a failed write is "unknown", not "didn't happen".
	UnackedButStored    int      `json:"unacked_but_stored"`
	Windows             []Window `json:"unavailability_windows"`
	MedianWindowSeconds float64  `json:"median_window_seconds"`
	MaxWindowSeconds    float64  `json:"max_window_seconds"`
	Passed              bool     `json:"passed"`
}

// Report is everything written to the JSON file.
type Report struct {
	Config    Config     `json:"config"`
	Started   time.Time  `json:"started"`
	Finished  time.Time  `json:"finished"`
	Requests  []Request  `json:"requests"`
	Readbacks []Readback `json:"readbacks"`
	Summary   Summary    `json:"summary"`
}

// Run does the whole check: write, read back, analyze.
func Run(ctx context.Context, cfg Config, client *http.Client) Report {
	rep := Report{Config: cfg, Started: time.Now()}
	rep.Requests = writePhase(ctx, cfg, client)
	// Cancelling ctx (Ctrl-C) only ends the writing early. The read-back must
	// still run, or every key would show up as unverified.
	rep.Readbacks = readPhase(context.WithoutCancel(ctx), cfg, client, rep.Requests)
	rep.Summary = summarize(rep.Requests, rep.Readbacks)
	rep.Finished = time.Now()
	return rep
}

// writePhase sends one PUT every 1/rate seconds until the duration is over.
// Each write runs in its own goroutine, so a slow or hanging request doesn't
// delay the next one. The checker keeps knocking at a steady pace even while
// the cluster is down, which is what lets us measure when it comes back.
func writePhase(ctx context.Context, cfg Config, client *http.Client) []Request {
	ticker := time.NewTicker(time.Second / time.Duration(cfg.Rate))
	defer ticker.Stop()
	stop := time.After(cfg.Duration)

	var (
		mu       sync.Mutex
		wg       sync.WaitGroup
		requests []Request
	)
loop:
	for seq := 0; ; seq++ {
		select {
		case <-stop:
			break loop
		case <-ctx.Done():
			break loop
		case <-ticker.C:
		}
		wg.Go(func() {
			r := write(ctx, cfg, client, seq)
			mu.Lock()
			requests = append(requests, r)
			mu.Unlock()
		})
	}
	wg.Wait()
	slices.SortFunc(requests, func(a, b Request) int { return a.Seq - b.Seq })
	return requests
}

// write sends one PUT with a key and value that no other write uses, so
// a read-back can tell exactly which write it sees.
func write(ctx context.Context, cfg Config, client *http.Client, seq int) Request {
	r := Request{
		Seq:   seq,
		Key:   fmt.Sprintf("%s-%06d", cfg.RunID, seq),
		Value: fmt.Sprintf("%s/%d", cfg.RunID, seq),
		Start: time.Now(),
	}
	ctx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()
	// strings.NewReader lets the client resend the body when a follower
	// answers 307 and it follows the redirect to the leader.
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, cfg.URL+"/kv/"+r.Key, strings.NewReader(r.Value))
	if err != nil {
		r.End, r.Error = time.Now(), err.Error()
		return r
	}
	resp, err := client.Do(req)
	r.End = time.Now()
	if err != nil {
		r.Error = err.Error()
		return r
	}
	defer resp.Body.Close()
	r.Status = resp.StatusCode
	r.Node = resp.Request.URL.Host
	r.Acked = resp.StatusCode == http.StatusNoContent
	if !r.Acked {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 200))
		r.Error = strings.TrimSpace(string(body))
	}
	return r
}

// readPhase reads back every key, acknowledged or not. Reads go to the
// leader and are linearizable, so a "not found" is a real answer, not a
// stale view. Failed reads are retried: the cluster may still be recovering.
func readPhase(ctx context.Context, cfg Config, client *http.Client, requests []Request) []Readback {
	readbacks := make([]Readback, len(requests))
	jobs := make(chan int)
	var wg sync.WaitGroup
	for range 8 { // 8 readers in parallel
		wg.Go(func() {
			for i := range jobs {
				readbacks[i] = readBack(ctx, cfg, client, requests[i])
			}
		})
	}
	for i := range requests {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	return readbacks
}

// Read-back retries: up to readAttempts tries, waiting firstReadBackoff
// after the first failure and doubling up to 2s. Variables so tests can
// shorten them.
var (
	readAttempts     = 10
	firstReadBackoff = 200 * time.Millisecond
)

func readBack(ctx context.Context, cfg Config, client *http.Client, w Request) Readback {
	rb := Readback{Key: w.Key, Acked: w.Acked}
	backoff := firstReadBackoff
	for rb.Attempts = 1; rb.Attempts <= readAttempts; rb.Attempts++ {
		status, body, err := get(ctx, cfg, client, w.Key)
		switch {
		case err == nil && status == http.StatusOK && body == w.Value:
			rb.Outcome, rb.Error = "found", ""
			return rb
		case err == nil && status == http.StatusOK:
			// Keys are unique per write, so any other value is corruption.
			rb.Outcome, rb.Got, rb.Error = "wrong_value", body, ""
			return rb
		case err == nil && status == http.StatusNotFound:
			// A definite answer from the leader: the key doesn't exist.
			rb.Outcome, rb.Error = "missing", ""
			return rb
		case err != nil:
			rb.Error = err.Error()
		default:
			rb.Error = fmt.Sprintf("status %d: %s", status, body)
		}
		select {
		case <-ctx.Done():
			rb.Outcome = "unverified"
			return rb
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 2*time.Second)
	}
	rb.Attempts = readAttempts
	rb.Outcome = "unverified"
	return rb
}

func get(ctx context.Context, cfg Config, client *http.Client, key string) (int, string, error) {
	ctx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, cfg.URL+"/kv/"+key, nil)
	if err != nil {
		return 0, "", err
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil && !errors.Is(err, io.EOF) {
		return 0, "", err
	}
	return resp.StatusCode, string(body), nil
}

// summarize turns the raw records into the verdict.
func summarize(requests []Request, readbacks []Readback) Summary {
	s := Summary{Total: len(requests), LostAcked: []string{}, Unverified: []string{}}
	for i, w := range requests {
		rb := readbacks[i]
		if w.Acked {
			s.Acked++
			switch rb.Outcome {
			case "missing", "wrong_value":
				s.LostAcked = append(s.LostAcked, w.Key)
			case "unverified":
				s.Unverified = append(s.Unverified, w.Key)
			}
		} else if rb.Outcome == "found" {
			s.UnackedButStored++
		}
	}
	s.Windows = unavailabilityWindows(requests)
	var durations []float64
	for _, w := range s.Windows {
		durations = append(durations, w.DurationSeconds)
	}
	s.MedianWindowSeconds = median(durations)
	if len(durations) > 0 {
		s.MaxWindowSeconds = slices.Max(durations)
	}
	s.Passed = len(s.LostAcked) == 0 && len(s.Unverified) == 0
	return s
}

// unavailabilityWindows finds every stretch of failed writes. Writes are
// looked at in the order they were sent (by Seq). A window opens at the
// start of the first failed write and closes at the start of the next
// acknowledged write: the first moment we know the cluster takes writes
// again. If the run ends inside a window, it closes at the end of the last
// failed write.
func unavailabilityWindows(requests []Request) []Window {
	var windows []Window
	var cur *Window
	var lastFailEnd time.Time
	for _, r := range requests {
		switch {
		case !r.Acked && cur == nil:
			cur = &Window{Start: r.Start, FailedWrites: 1}
			lastFailEnd = r.End
		case !r.Acked:
			cur.FailedWrites++
			lastFailEnd = r.End
		case r.Acked && cur != nil:
			cur.End = r.Start
			windows = append(windows, closeWindow(*cur))
			cur = nil
		}
	}
	if cur != nil {
		cur.End = lastFailEnd
		windows = append(windows, closeWindow(*cur))
	}
	return windows
}

func closeWindow(w Window) Window {
	w.DurationSeconds = w.End.Sub(w.Start).Seconds()
	return w
}

// median of xs, or 0 for an empty list.
func median(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	s := slices.Clone(xs)
	slices.Sort(s)
	mid := len(s) / 2
	if len(s)%2 == 1 {
		return s[mid]
	}
	return (s[mid-1] + s[mid]) / 2
}
