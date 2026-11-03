package controller

// This file talks to quorum-server's membership API: POST /join,
// /remove and /transfer-leadership. In safe scaling mode the operator, not
// the pods, decides who is a Raft member.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Membership changes the Raft configuration through the leader. Every call
// goes to leaderAddr (host:port of the leader's HTTP API); if leadership
// moved in the meantime, the server redirects to the new leader. Tests
// replace it with a fake.
type Membership interface {
	// Join adds id (reachable at raftAddr) as a voter, or as a nonvoter if
	// nonvoter is true. Joining an existing nonvoter as a voter promotes it.
	Join(ctx context.Context, leaderAddr, id, raftAddr string, nonvoter bool) error
	// Remove takes id out of the configuration.
	Remove(ctx context.Context, leaderAddr, id string) error
	// TransferLeadership makes the leader hand over to another voter.
	TransferLeadership(ctx context.Context, leaderAddr string) error
}

// HTTPMembership is the real Membership.
type HTTPMembership struct{}

// membershipClient waits longer than httpClient: a membership change is a
// Raft commit, and a leadership transfer waits for the new leader.
var membershipClient = &http.Client{Timeout: 15 * time.Second}

func (HTTPMembership) Join(ctx context.Context, leaderAddr, id, raftAddr string, nonvoter bool) error {
	return post(ctx, leaderAddr, "/join", map[string]any{"id": id, "raft_addr": raftAddr, "nonvoter": nonvoter})
}

func (HTTPMembership) Remove(ctx context.Context, leaderAddr, id string) error {
	return post(ctx, leaderAddr, "/remove", map[string]any{"id": id})
}

func (HTTPMembership) TransferLeadership(ctx context.Context, leaderAddr string) error {
	return post(ctx, leaderAddr, "/transfer-leadership", nil)
}

// post sends body as JSON and expects 200. bytes.Reader lets Go's client
// resend the body when a follower answers 307, which it only does when it
// can rewind the body.
func post(ctx context.Context, addr, path string, body any) error {
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+path, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := membershipClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("POST %s: %s: %s", path, resp.Status, strings.TrimSpace(string(msg)))
	}
	return nil
}
