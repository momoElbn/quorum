# Quorum
A Kubernetes operator in Go that runs a Raft-replicated key-value store
(hashicorp/raft) and keeps it available through failures, scaling and upgrades.
See docs/decisions.md for design decisions.

## Rules
- Comment the "why" of anything Raft- or Kubernetes-specific in the code.
- Keep code simple: standard library first, no clever abstractions.
- Don't add a dependency without asking me first.
- Every new component gets tests. Run `go test -race ./...` before saying done.
- One task at a time, in small changes. Don't commit; I commit.
- Never put secrets in Git.