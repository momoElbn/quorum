// quorum-server is one node of a Raft-replicated key-value store.
//
// It is configured through environment variables (see loadConfig) and
// serves the key-value API, cluster management, status and metrics over
// HTTP. See docs/decisions.md for how reads, writes and joining work.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/momoElbn/quorum/internal/store"
	"github.com/prometheus/client_golang/prometheus"
)

type config struct {
	NodeID    string
	HTTPAddr  string            // where to listen for HTTP, e.g. ":8080"
	RaftAddr  string            // advertised Raft address, e.g. "node1:7000"
	RaftBind  string            // where to listen for Raft, e.g. ":7000"
	DataDir   string            // Raft log, stable store and snapshots
	Bootstrap bool              // create a new cluster if there's no state on disk
	JoinAddrs []string          // HTTP addresses to send POST /join to
	Peers     map[string]string // node ID -> HTTP address, for redirects
}

// loadConfig reads the configuration. getenv is a parameter so tests can
// pass a fake environment.
func loadConfig(getenv func(string) string) (config, error) {
	c := config{
		NodeID:   getenv("NODE_ID"),
		HTTPAddr: getenv("HTTP_ADDR"),
		RaftAddr: getenv("RAFT_ADDR"),
		DataDir:  getenv("DATA_DIR"),
		Peers:    map[string]string{},
	}
	if c.NodeID == "" || c.RaftAddr == "" || c.DataDir == "" {
		return c, errors.New("NODE_ID, RAFT_ADDR and DATA_DIR are required")
	}
	if c.HTTPAddr == "" {
		c.HTTPAddr = ":8080"
	}

	// RAFT_ADDR is the name other nodes dial (node1:7000), and it might not
	// be an address of this machine at all, for example a Kubernetes
	// Service name. So we listen on the same port on every interface.
	_, port, err := net.SplitHostPort(c.RaftAddr)
	if err != nil {
		return c, fmt.Errorf("RAFT_ADDR %q: %w", c.RaftAddr, err)
	}
	c.RaftBind = ":" + port

	if b := getenv("BOOTSTRAP"); b != "" {
		if c.Bootstrap, err = strconv.ParseBool(b); err != nil {
			return c, fmt.Errorf("BOOTSTRAP %q: %w", b, err)
		}
	}

	// JOIN_ADDR may list several nodes (comma-separated), so a new node can
	// still join when one of them is down.
	for _, a := range strings.Split(getenv("JOIN_ADDR"), ",") {
		if a = strings.TrimSpace(a); a != "" {
			c.JoinAddrs = append(c.JoinAddrs, a)
		}
	}

	// PEERS_HTTP looks like "node1=node1:8080,node2=node2:8080".
	for _, p := range strings.Split(getenv("PEERS_HTTP"), ",") {
		if p = strings.TrimSpace(p); p == "" {
			continue
		}
		id, addr, ok := strings.Cut(p, "=")
		if !ok || id == "" || addr == "" {
			return c, fmt.Errorf("PEERS_HTTP entry %q: want id=host:port", p)
		}
		c.Peers[id] = addr
	}
	return c, nil
}

func main() {
	cfg, err := loadConfig(os.Getenv)
	if err != nil {
		log.Fatal(err)
	}

	st := store.New(store.Config{
		NodeID:   cfg.NodeID,
		RaftAddr: cfg.RaftAddr,
		RaftBind: cfg.RaftBind,
		DataDir:  cfg.DataDir,
	})
	if err := st.Open(cfg.Bootstrap); err != nil {
		log.Fatal(err)
	}

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           newHandler(st, cfg.Peers, prometheus.NewRegistry()),
		ReadHeaderTimeout: 10 * time.Second,
	}

	// SIGTERM is what Kubernetes sends before it kills a pod. SIGINT is
	// Ctrl-C when running locally.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	serveErr := make(chan error, 1)
	go func() {
		log.Printf("%s: HTTP on %s, Raft on %s", cfg.NodeID, cfg.HTTPAddr, cfg.RaftAddr)
		serveErr <- srv.ListenAndServe()
	}()

	// Join in the background, so /healthz and /status answer while we wait
	// for the leader. A node that is already in the membership stored on
	// disk (it is restarting) doesn't need to join again.
	if len(cfg.JoinAddrs) > 0 && !isMember(st, cfg.NodeID) {
		go func() {
			err := joinCluster(ctx, http.DefaultClient, cfg.JoinAddrs, cfg.NodeID, st.RaftAddr(), joinBackoff)
			if err != nil && ctx.Err() == nil {
				log.Printf("join failed: %v", err)
			}
		}()
	}

	select {
	case <-ctx.Done():
		log.Print("shutting down")
	case err := <-serveErr:
		log.Printf("http server: %v", err)
	}
	shutdown(srv, st)
}

// shutdown stops the node in an order that keeps the cluster available.
func shutdown(srv *http.Server, st *store.Store) {
	// Kubernetes waits 30s by default between SIGTERM and SIGKILL. 10s for
	// HTTP leaves time for the Raft steps below.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 1. Stop taking new requests and let in-flight ones finish.
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("http shutdown: %v", err)
	}

	// 2. If we are the leader, hand leadership to another node. Without
	//    this, the others only notice we're gone after the heartbeat timeout
	//    (about 1s) plus an election, so writes fail for a few seconds.
	//    During a rolling upgrade that would happen on every leader restart.
	if err := st.TransferLeadership(); err != nil {
		log.Printf("leadership transfer: %v", err)
	}

	// 3. Stop Raft and close the files. We don't leave the cluster: a pod
	//    that gets SIGTERM usually comes back with the same identity.
	if err := st.Close(); err != nil {
		log.Printf("close store: %v", err)
	}
}

// isMember reports whether nodeID is in the cluster membership this node
// already has on disk.
func isMember(st *store.Store, nodeID string) bool {
	status, err := st.Status()
	if err != nil {
		return false
	}
	return slices.ContainsFunc(status.Members, func(m store.Member) bool { return m.ID == nodeID })
}

// joinBackoff is the wait after the first failed round of join attempts.
// It doubles after each failed round, up to maxJoinBackoff.
const (
	joinBackoff    = 500 * time.Millisecond
	maxJoinBackoff = 10 * time.Second
)

// joinCluster asks one of targets to add this node to the cluster, and
// retries until one succeeds or ctx is cancelled.
//
// Retrying matters because the nodes start in any order. The bootstrap
// node may not be up or leader yet, or the join may land on a follower in
// the middle of an election.
func joinCluster(ctx context.Context, client *http.Client, targets []string, id, raftAddr string, backoff time.Duration) error {
	body, err := json.Marshal(joinRequest{ID: id, RaftAddr: raftAddr})
	if err != nil {
		return err
	}
	for {
		for _, target := range targets {
			err := postJoin(ctx, client, target, body)
			if err == nil {
				log.Printf("joined cluster via %s", target)
				return nil
			}
			log.Printf("join via %s: %v (retrying)", target, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, maxJoinBackoff)
	}
}

func postJoin(ctx context.Context, client *http.Client, target string, body []byte) error {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	// bytes.NewReader lets the client resend the body if a follower answers
	// 307. Go follows 307 redirects only when it can rewind the body.
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+target+"/join", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %s", resp.Status)
	}
	return nil
}
