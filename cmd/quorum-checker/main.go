// quorum-checker measures availability and checks for lost writes.
//
// It writes unique keys at a fixed rate for a fixed time through the client
// Service, records every request, then reads every key back. It reports how
// many writes were acknowledged, whether any acknowledged write was lost, and
// every window during which writes failed.
//
// Run it inside the cluster (scripts/run-checker.sh does that): followers
// redirect to the leader's in-cluster DNS name, which only resolves there.
//
//	quorum-checker -url http://quorum:8080 -rate 20 -duration 2m -out results.json
//
// Exit code: 0 if no acknowledged write was lost, 1 if one was (or couldn't
// be verified), 2 for usage or file errors.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	var cfg Config
	var out string
	flag.StringVar(&cfg.URL, "url", "http://quorum:8080", "base URL of the quorum client Service")
	flag.IntVar(&cfg.Rate, "rate", 20, "writes per second")
	flag.DurationVar(&cfg.Duration, "duration", time.Minute, "how long to write")
	flag.DurationVar(&cfg.Timeout, "timeout", 2*time.Second, "timeout for each request")
	flag.StringVar(&cfg.RunID, "run-id", "chk-"+time.Now().Format("20060102-150405"), "prefix that makes this run's keys unique")
	flag.StringVar(&out, "out", "results.json", "where to write the full JSON record")
	flag.Parse()
	if cfg.Rate < 1 || cfg.Duration <= 0 || cfg.Timeout <= 0 {
		fmt.Fprintln(os.Stderr, "-rate, -duration and -timeout must be positive")
		os.Exit(2)
	}

	// Ctrl-C or SIGTERM stops writing early; the read-back still runs.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		stop() // a second Ctrl-C kills the process the normal way
	}()

	fmt.Printf("writing to %s: %d/s for %s (keys %s-*)\n", cfg.URL, cfg.Rate, cfg.Duration, cfg.RunID)
	rep := Run(ctx, cfg, &http.Client{})

	if err := writeJSON(out, rep); err != nil {
		fmt.Fprintln(os.Stderr, "write results:", err)
		os.Exit(2)
	}
	printSummary(os.Stdout, rep.Summary)
	fmt.Printf("full record: %s\n", out)
	if !rep.Summary.Passed {
		os.Exit(1)
	}
}

func writeJSON(path string, rep Report) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(rep); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func printSummary(w io.Writer, s Summary) {
	fmt.Fprintf(w, "\ntotal writes:           %d\n", s.Total)
	fmt.Fprintf(w, "acknowledged:           %d\n", s.Acked)
	fmt.Fprintf(w, "failed (not acked):     %d  (of which applied anyway: %d)\n", s.Total-s.Acked, s.UnackedButStored)
	fmt.Fprintf(w, "lost acknowledged:      %d\n", len(s.LostAcked))
	for _, k := range s.LostAcked {
		fmt.Fprintf(w, "  LOST %s\n", k)
	}
	if len(s.Unverified) > 0 {
		fmt.Fprintf(w, "unverified (read-back kept failing): %d\n", len(s.Unverified))
	}
	fmt.Fprintf(w, "unavailability windows: %d\n", len(s.Windows))
	for i, win := range s.Windows {
		fmt.Fprintf(w, "  %2d. %s -> %s  %6.3fs  (%d failed writes)\n", i+1,
			win.Start.Format("15:04:05.000"), win.End.Format("15:04:05.000"), win.DurationSeconds, win.FailedWrites)
	}
	if len(s.Windows) > 0 {
		fmt.Fprintf(w, "  median %.3fs, max %.3fs\n", s.MedianWindowSeconds, s.MaxWindowSeconds)
	}
	verdict := "PASS: every acknowledged write was read back"
	if !s.Passed {
		verdict = "FAIL: acknowledged writes lost or unverifiable"
	}
	fmt.Fprintf(w, "result: %s\n", verdict)
}
