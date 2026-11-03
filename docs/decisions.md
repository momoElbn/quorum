# Design decisions

## quorum-server: a 3-node Raft key-value store

quorum-server keeps a `map[string]string` in memory on every node and uses
hashicorp/raft to make all nodes apply the same changes in the same order.
Raft needs a majority (2 of 3) to make progress, so the cluster tolerates one
failed node.

### Write path (PUT / DELETE)

1. A client sends `PUT /kv/{key}` to any node.
2. A node that is not the leader answers `307 Temporary Redirect` with
   `Location` set to the leader's HTTP address. 307, not 302, because 307
   tells the client to repeat the same method and body.
   Followers do not proxy the request. A redirect keeps the server simple, and
   the client can see which node it is talking to.
3. The leader turns the request into a JSON command
   (`{"op":"set","key":"k","value":"v"}`) and calls `raft.Apply`.
4. Raft appends the command to the leader's log, sends it to the followers,
   and marks it *committed* once a majority has stored it on disk.
5. Each node applies committed commands, in log order, to its map (the FSM).
   The leader answers `204` only after its own FSM has applied the command, so
   an acknowledged write survives the loss of any single node.

If the leader loses leadership in the middle of a write, the client gets
`503` and may retry. `set` and `delete` are idempotent, so repeating them is
safe.

### Read path (GET)

Reads are linearizable: a GET sees every write that was acknowledged before
the GET started. To get that guarantee:

- Only the leader serves reads. Followers redirect with 307, like for writes.
  A follower's map may be behind the leader's.
- The leader calls `VerifyLeader` before reading. A leader that has been cut
  off may not know yet that a new leader exists and is accepting writes.
  `VerifyLeader` asks a majority to confirm the leader is still in charge.
- The first read in each new term also calls `Barrier`. A node that has just
  become leader may hold committed entries from the previous leader that it
  has not applied to its map yet. `Barrier` waits until they are applied.
  After that, every write acknowledged in this term is already applied,
  because the leader only acknowledges after applying. So later reads in the
  same term skip the barrier.

The cost is one network round trip to a majority per read. Leader leases would
be faster, but they depend on synchronised clocks, and we don't need that
speed yet.

### Joining

- One node starts with `BOOTSTRAP=true`. If it has no Raft state on disk, it
  creates a one-member cluster containing only itself.
  In Kubernetes, all pods of a StatefulSet share one env, so the chart sets
  `BOOTSTRAP_NODE=quorum-0` instead. Only the node whose `NODE_ID` matches
  bootstraps.
- The other nodes start with `JOIN_ADDR` set. Each one calls `POST /join`
  with its node ID and Raft address, retrying with exponential backoff
  (0.5s, doubling up to 10s) until it succeeds. A follower redirects the join
  to the leader, and the leader calls `AddVoter`.
- `Join` is idempotent. If the server is already a member with the same
  address, nothing happens. If the ID or address is already in use, the old
  entry is removed first. This covers a node that came back with a new
  address.
- A node that already finds itself in the membership on disk does not join
  again.
- Nodes are not removed on shutdown. In Kubernetes, SIGTERM usually means a
  restart (rolling upgrade, rescheduling), and removing the node each time
  would churn the membership. Removal is explicit, with `POST /remove`.
- Followers need the leader's HTTP address to redirect, but Raft only knows
  Raft addresses. `PEERS_HTTP` (`node1=node1:8080,node2=node2:8080,...`) maps
  node IDs to HTTP addresses.

Known risk: if the bootstrap node loses its disk and restarts with
`BOOTSTRAP=true`, it creates a new, separate one-node cluster (split brain).
A later Kubernetes operator should bootstrap only when the whole cluster is
brand new.

### Storage

Everything lives under `DATA_DIR`:

- `raft.db`: a raft-boltdb (bbolt) file holding both the Raft log and the
  "stable store" (current term and last vote). Raft needs these on disk so
  a restarted node never votes twice in the same term or forgets committed
  entries.
- `snapshots/`: the file snapshot store, which keeps the 2 latest snapshots.
  A snapshot is the whole map as JSON. Raft takes one automatically after
  enough new log entries, so the log can be truncated and a node can catch up
  without replaying the full history.

The map itself is not persisted separately. On restart, a node loads the
latest snapshot and replays the log entries after it.

Raft addresses are hostnames (`node1:7000`), not IPs. Pod IPs change when a
pod restarts, but StatefulSet DNS names stay the same. hashicorp/raft's
`NewTCPTransport` only accepts an IP as advertise address, so the store uses
a small TCP stream layer that advertises the hostname.
