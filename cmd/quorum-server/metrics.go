package main

import (
	"github.com/momoElbn/quorum/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// metrics holds the Prometheus metrics the HTTP handlers update.
type metrics struct {
	writes        *prometheus.CounterVec // label result: ok, redirect or error
	writeDuration prometheus.Histogram
}

// newMetrics registers all metrics on reg. We use our own registry instead
// of Prometheus's global one, so tests can create several servers in one
// process without "duplicate metric" panics.
func newMetrics(reg prometheus.Registerer, st *store.Store) *metrics {
	m := &metrics{
		writes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "quorum_writes_total",
			Help: "PUT and DELETE requests, by result (ok, redirect, error).",
		}, []string{"result"}),
		writeDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "quorum_write_duration_seconds",
			Help: "Time for the leader to commit and apply a write through Raft.",
			// Default buckets (5ms to 10s) fit: a commit needs one network
			// round trip to a majority plus a disk sync.
			Buckets: prometheus.DefBuckets,
		}),
	}
	reg.MustRegister(m.writes, m.writeDuration)

	// Create the label values up front so they show up as 0 before the
	// first write. Otherwise a rate() alert has no series to look at.
	for _, r := range []string{"ok", "redirect", "error"} {
		m.writes.WithLabelValues(r)
	}

	// quorum_raft_state{state="leader"} is 1 on the leader and 0 elsewhere,
	// and so on for each state. One series per state, instead of one number
	// encoding the state, lets you write queries like
	// sum(quorum_raft_state{state="leader"}) == 1. The value is read from
	// Raft at scrape time, so nothing needs to keep it updated.
	for _, state := range []string{"follower", "candidate", "leader", "shutdown"} {
		reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name:        "quorum_raft_state",
			Help:        "1 if this node is in the given Raft state, 0 otherwise.",
			ConstLabels: prometheus.Labels{"state": state},
		}, func() float64 {
			if st.State() == state {
				return 1
			}
			return 0
		}))
	}

	// Frequent leader changes mean elections keep happening: usually a
	// slow disk, an overloaded node or a flaky network.
	reg.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{
		Name: "quorum_leader_changes_total",
		Help: "Times this node has seen a new leader take over.",
	}, func() float64 { return float64(st.LeaderChanges()) }))

	// Standard process and Go runtime metrics (memory, goroutines, CPU).
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return m
}
