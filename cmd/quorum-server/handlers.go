package main

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/momoElbn/quorum/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// maxBody caps request bodies, so a client can't make the server buffer
// unlimited data. Every value also ends up in the Raft log on every node.
const maxBody = 1 << 20 // 1 MiB

type server struct {
	store   *store.Store
	metrics *metrics
}

// newHandler wires the routes. It is separate from main so tests can run it
// with httptest.
func newHandler(st *store.Store, reg *prometheus.Registry) http.Handler {
	s := &server{store: st, metrics: newMetrics(reg, st)}

	mux := http.NewServeMux()
	mux.HandleFunc("PUT /kv/{key}", s.handlePut)
	mux.HandleFunc("GET /kv/{key}", s.handleGet)
	mux.HandleFunc("DELETE /kv/{key}", s.handleDelete)
	mux.HandleFunc("POST /join", s.handleJoin)
	mux.HandleFunc("POST /remove", s.handleRemove)
	mux.HandleFunc("POST /transfer-leadership", s.handleTransferLeadership)
	mux.HandleFunc("GET /status", s.handleStatus)
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /readyz", s.handleReadyz)
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	return mux
}

func (s *server) handlePut(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
	if err != nil {
		http.Error(w, "cannot read body", http.StatusBadRequest)
		return
	}
	start := time.Now()
	err = s.store.Set(r.PathValue("key"), string(body))
	s.finishWrite(w, r, err, start)
}

func (s *server) handleDelete(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	err := s.store.Delete(r.PathValue("key"))
	s.finishWrite(w, r, err, start)
}

// finishWrite sends the response for a PUT or DELETE and records metrics.
func (s *server) finishWrite(w http.ResponseWriter, r *http.Request, err error, start time.Time) {
	switch {
	case errors.Is(err, store.ErrNotLeader):
		// Don't time redirects: the histogram measures Raft commits, and
		// a follower answers this case without touching Raft.
		s.metrics.writes.WithLabelValues("redirect").Inc()
		s.redirectToLeader(w, r)
	case err != nil:
		s.metrics.writes.WithLabelValues("error").Inc()
		s.metrics.writeDuration.Observe(time.Since(start).Seconds())
		// Most Raft errors are temporary (lost leadership mid-write,
		// timeout waiting for a majority), so 503 tells the client to retry.
		log.Printf("write failed: %v", err)
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
	default:
		s.metrics.writes.WithLabelValues("ok").Inc()
		s.metrics.writeDuration.Observe(time.Since(start).Seconds())
		w.WriteHeader(http.StatusNoContent)
	}
}

func (s *server) handleGet(w http.ResponseWriter, r *http.Request) {
	v, ok, err := s.store.Get(r.PathValue("key"))
	switch {
	case errors.Is(err, store.ErrNotLeader):
		s.redirectToLeader(w, r)
	case err != nil:
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
	case !ok:
		http.NotFound(w, r)
	default:
		io.WriteString(w, v)
	}
}

// redirectToLeader sends the client to the same path on the leader.
//
// We use 307, not 302: with 302, many clients turn PUT into GET and drop
// the body. 307 requires the client to repeat the same method and body.
func (s *server) redirectToLeader(w http.ResponseWriter, r *http.Request) {
	leaderID, addr, ok := s.store.LeaderHTTPAddr()
	if leaderID == "" {
		// An election is in progress. It usually finishes within a couple
		// of seconds, so ask the client to retry shortly.
		w.Header().Set("Retry-After", "1")
		http.Error(w, "no leader elected yet", http.StatusServiceUnavailable)
		return
	}
	// Raft only knows the leader's Raft address. The store works out its
	// HTTP address from PEERS_HTTP or from the Raft address's host.
	if !ok {
		http.Error(w, "no HTTP address for leader "+leaderID, http.StatusServiceUnavailable)
		return
	}
	http.Redirect(w, r, "http://"+addr+r.URL.RequestURI(), http.StatusTemporaryRedirect)
}

type joinRequest struct {
	ID       string `json:"id"`
	RaftAddr string `json:"raft_addr"`
	// Nonvoter adds the node without a vote, so it can catch up on the log
	// before it counts towards the majority. Joining again without it
	// promotes the node to voter.
	Nonvoter bool `json:"nonvoter,omitempty"`
}

func (s *server) handleJoin(w http.ResponseWriter, r *http.Request) {
	var req joinRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBody)).Decode(&req); err != nil || req.ID == "" || req.RaftAddr == "" {
		http.Error(w, `want JSON {"id": "...", "raft_addr": "host:port"}`, http.StatusBadRequest)
		return
	}
	// Only the leader can change membership, so followers redirect.
	// The joining node's HTTP client follows the 307 automatically.
	join, what := s.store.Join, "join "+req.ID
	if req.Nonvoter {
		join, what = s.store.JoinNonvoter, "join "+req.ID+" as nonvoter"
	}
	s.finishMembership(w, r, join(req.ID, req.RaftAddr), what)
}

type removeRequest struct {
	ID string `json:"id"`
}

func (s *server) handleRemove(w http.ResponseWriter, r *http.Request) {
	var req removeRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBody)).Decode(&req); err != nil || req.ID == "" {
		http.Error(w, `want JSON {"id": "..."}`, http.StatusBadRequest)
		return
	}
	err := s.store.Remove(req.ID)
	s.finishMembership(w, r, err, "remove "+req.ID)
}

// handleTransferLeadership hands leadership to another voter. The operator
// calls it before removing the leader from the cluster, so that writes don't
// stop for an election timeout. Followers redirect to the leader.
func (s *server) handleTransferLeadership(w http.ResponseWriter, r *http.Request) {
	if s.store.State() != "leader" {
		s.redirectToLeader(w, r)
		return
	}
	s.finishMembership(w, r, s.store.TransferLeadership(), "transfer leadership")
}

func (s *server) finishMembership(w http.ResponseWriter, r *http.Request, err error, what string) {
	switch {
	case errors.Is(err, store.ErrNotLeader):
		s.redirectToLeader(w, r)
	case err != nil:
		log.Printf("%s failed: %v", what, err)
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
	default:
		log.Printf("%s: ok", what)
		w.WriteHeader(http.StatusOK)
	}
}

// handleStatus shows this node's view of the cluster. Any node answers
// without redirecting: when debugging, you want each node's own view.
func (s *server) handleStatus(w http.ResponseWriter, r *http.Request) {
	st, err := s.store.Status()
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(st)
}

// handleHealthz answers 200 as long as the process and Raft are running.
//
// It deliberately does not check for a leader. Kubernetes restarts pods
// that fail their liveness probe. If healthz failed whenever there was no
// leader, a network split would get every pod restarted at once, which only
// makes recovery slower.
func (s *server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	if s.store.State() == "shutdown" {
		http.Error(w, "raft is shut down", http.StatusServiceUnavailable)
		return
	}
	io.WriteString(w, "ok\n")
}

// handleReadyz answers 200 when this node can serve clients, and 503 with
// the reason when it can't (see store.Ready for the rules).
//
// Kubernetes uses it for the readiness probe: a pod that fails it is taken
// out of the client Service but not restarted. So unlike /healthz, it can
// and should fail while there is no leader.
func (s *server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if err := s.store.Ready(); err != nil {
		http.Error(w, "not ready: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	io.WriteString(w, "ok\n")
}
