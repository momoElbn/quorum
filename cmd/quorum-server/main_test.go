package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/momoElbn/quorum/internal/store"
	"github.com/prometheus/client_golang/prometheus"
)

func fakeEnv(vars map[string]string) func(string) string {
	return func(k string) string { return vars[k] }
}

func TestLoadConfig(t *testing.T) {
	cfg, err := loadConfig(fakeEnv(map[string]string{
		"NODE_ID":    "node2",
		"HTTP_ADDR":  ":8081",
		"RAFT_ADDR":  "node2:7000",
		"DATA_DIR":   "/data",
		"BOOTSTRAP":  "false",
		"JOIN_ADDR":  "node1:8080, node3:8080",
		"PEERS_HTTP": "node1=node1:8080,node2=node2:8080",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RaftBind != ":7000" {
		t.Errorf("RaftBind = %q, want :7000", cfg.RaftBind)
	}
	if cfg.Bootstrap {
		t.Error("Bootstrap = true, want false")
	}
	if len(cfg.JoinAddrs) != 2 || cfg.JoinAddrs[1] != "node3:8080" {
		t.Errorf("JoinAddrs = %v", cfg.JoinAddrs)
	}
	if cfg.Peers["node1"] != "node1:8080" || len(cfg.Peers) != 2 {
		t.Errorf("Peers = %v", cfg.Peers)
	}
}

func TestLoadConfigBootstrapNode(t *testing.T) {
	for id, want := range map[string]bool{"quorum-0": true, "quorum-1": false} {
		cfg, err := loadConfig(fakeEnv(map[string]string{
			"NODE_ID":        id,
			"RAFT_ADDR":      id + ".quorum-headless:7000",
			"DATA_DIR":       "/data",
			"BOOTSTRAP_NODE": "quorum-0",
		}))
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Bootstrap != want {
			t.Errorf("%s: Bootstrap = %v, want %v", id, cfg.Bootstrap, want)
		}
	}
}

func TestLoadConfigErrors(t *testing.T) {
	cases := map[string]map[string]string{
		"missing NODE_ID":        {"RAFT_ADDR": "n:7000", "DATA_DIR": "/d"},
		"RAFT_ADDR without port": {"NODE_ID": "n", "RAFT_ADDR": "n", "DATA_DIR": "/d"},
		"bad BOOTSTRAP":          {"NODE_ID": "n", "RAFT_ADDR": "n:7000", "DATA_DIR": "/d", "BOOTSTRAP": "maybe"},
		"PEERS_HTTP without =":   {"NODE_ID": "n", "RAFT_ADDR": "n:7000", "DATA_DIR": "/d", "PEERS_HTTP": "node1"},
		"PEERS_HTTP empty addr":  {"NODE_ID": "n", "RAFT_ADDR": "n:7000", "DATA_DIR": "/d", "PEERS_HTTP": "node1="},
		"both bootstrap vars":    {"NODE_ID": "n", "RAFT_ADDR": "n:7000", "DATA_DIR": "/d", "BOOTSTRAP": "true", "BOOTSTRAP_NODE": "n"},
	}
	for name, env := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := loadConfig(fakeEnv(env)); err == nil {
				t.Error("want an error")
			}
		})
	}
}

func TestJoinRetriesUntilSuccess(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Fail twice, as if the leader weren't elected yet.
		if calls.Add(1) <= 2 {
			http.Error(w, "no leader", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	target := strings.TrimPrefix(srv.URL, "http://")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := joinCluster(ctx, srv.Client(), []string{target}, "n", "n:7000", time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if got := calls.Load(); got != 3 {
		t.Errorf("calls = %d, want 3", got)
	}
}

func TestJoinStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// Port 1 on localhost refuses connections, so every attempt fails.
	if err := joinCluster(ctx, http.DefaultClient, []string{"127.0.0.1:1"}, "n", "n:7000", time.Millisecond); err == nil {
		t.Fatal("want an error after cancel")
	}
}

// node is one in-process quorum-server: a store plus its HTTP server.
type node struct {
	id    string
	store *store.Store
	http  *httptest.Server
	addr  string // host:port of the HTTP server
}

// startCluster runs 3 nodes the way they run in production: node0
// bootstraps, and node1 and node2 join it over HTTP with joinCluster.
func startCluster(t *testing.T) []*node {
	t.Helper()
	peers := map[string]string{}
	var nodes []*node
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("node%d", i)
		st := store.New(store.Config{NodeID: id, RaftBind: "127.0.0.1:0", DataDir: t.TempDir(), LogOutput: io.Discard})
		if err := st.Open(i == 0); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { st.Close() })

		// Unstarted, so we know every HTTP address before any handler
		// runs. The handlers read peers, which must be complete by then.
		srv := httptest.NewUnstartedServer(nil)
		n := &node{id: id, store: st, http: srv, addr: srv.Listener.Addr().String()}
		peers[id] = n.addr
		nodes = append(nodes, n)
	}
	for _, n := range nodes {
		n.http.Config.Handler = newHandler(n.store, peers, prometheus.NewRegistry())
		n.http.Start()
		t.Cleanup(n.http.Close)
	}

	waitForLeader(t, nodes[:1])
	for _, n := range nodes[1:] {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		err := joinCluster(ctx, http.DefaultClient, []string{nodes[0].addr}, n.id, n.store.RaftAddr(), 50*time.Millisecond)
		cancel()
		if err != nil {
			t.Fatalf("join %s: %v", n.id, err)
		}
	}
	return nodes
}

func waitForLeader(t *testing.T, nodes []*node) *node {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.store.State() == "leader" {
				return n
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("no leader elected")
	return nil
}

func do(t *testing.T, client *http.Client, method, url, body string) (*http.Response, string) {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, string(b)
}

func TestFollowerRedirectsToLeader(t *testing.T) {
	if testing.Short() {
		t.Skip("starts a 3-node cluster")
	}
	nodes := startCluster(t)
	leader := waitForLeader(t, nodes)
	var follower *node
	for _, n := range nodes {
		if n != leader {
			follower = n
			break
		}
	}

	// A client that doesn't follow redirects, so we can look at the 307.
	noFollow := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	resp, _ := do(t, noFollow, http.MethodPut, "http://"+follower.addr+"/kv/color", "blue")
	if resp.StatusCode != http.StatusTemporaryRedirect {
		t.Fatalf("PUT on follower: status %d, want 307", resp.StatusCode)
	}
	if want := "http://" + leader.addr + "/kv/color"; resp.Header.Get("Location") != want {
		t.Fatalf("Location = %q, want %q", resp.Header.Get("Location"), want)
	}

	// Go's default client follows the 307 and resends the body.
	if resp, body := do(t, http.DefaultClient, http.MethodPut, "http://"+follower.addr+"/kv/color", "blue"); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("PUT via follower: status %d %s", resp.StatusCode, body)
	}
	if resp, body := do(t, http.DefaultClient, http.MethodGet, "http://"+follower.addr+"/kv/color", ""); resp.StatusCode != http.StatusOK || body != "blue" {
		t.Fatalf("GET via follower: %d %q, want 200 \"blue\"", resp.StatusCode, body)
	}
	if resp, _ := do(t, http.DefaultClient, http.MethodDelete, "http://"+follower.addr+"/kv/color", ""); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE via follower: status %d", resp.StatusCode)
	}
	if resp, _ := do(t, http.DefaultClient, http.MethodGet, "http://"+leader.addr+"/kv/color", ""); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET after delete: status %d, want 404", resp.StatusCode)
	}

	// Each node answers /status and /healthz itself.
	if resp, body := do(t, http.DefaultClient, http.MethodGet, "http://"+follower.addr+"/status", ""); resp.StatusCode != http.StatusOK || !strings.Contains(body, `"state":"follower"`) {
		t.Errorf("/status on follower: %d %s", resp.StatusCode, body)
	}
	if resp, _ := do(t, http.DefaultClient, http.MethodGet, "http://"+follower.addr+"/healthz", ""); resp.StatusCode != http.StatusOK {
		t.Errorf("/healthz: status %d", resp.StatusCode)
	}

	// The follower counted the redirects, and the leader the actual writes.
	_, fm := do(t, http.DefaultClient, http.MethodGet, "http://"+follower.addr+"/metrics", "")
	_, lm := do(t, http.DefaultClient, http.MethodGet, "http://"+leader.addr+"/metrics", "")
	for _, want := range []string{`quorum_writes_total{result="redirect"} 3`, `quorum_raft_state{state="follower"} 1`} {
		if !strings.Contains(fm, want) {
			t.Errorf("follower /metrics missing %q", want)
		}
	}
	for _, want := range []string{`quorum_writes_total{result="ok"} 2`, `quorum_raft_state{state="leader"} 1`, "quorum_write_duration_seconds_count 2", "quorum_leader_changes_total"} {
		if !strings.Contains(lm, want) {
			t.Errorf("leader /metrics missing %q", want)
		}
	}
}
