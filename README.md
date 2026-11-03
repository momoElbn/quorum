# Quorum

Quorum is a 3-node key-value store replicated with Raft
([hashicorp/raft](https://github.com/hashicorp/raft)), plus a Kubernetes
operator that runs it and keeps it available through failures, scaling and
upgrades. A write is acknowledged only after a majority has stored it, so the
cluster survives the loss of any one node without losing data.

- **quorum-server** (`cmd/quorum-server`): HTTP API (`PUT/GET/DELETE /kv/{key}`). Followers redirect to the leader with `307`; reads are linearizable.
- **quorum-operator** (`operator/`): turns a `QuorumCluster` resource into a StatefulSet, two Services and a PodDisruptionBudget, and reports Raft health in its status.
- **quorum-checker** (`cmd/quorum-checker`): writes unique keys at a fixed rate, reads every acknowledged one back, and measures unavailability.
- **[quorum-gitops](https://github.com/momoElbn/quorum-gitops)**: everything that runs in the cluster, deployed by Argo CD.

Design decisions: [docs/decisions.md](docs/decisions.md).

## Architecture

![Quorum architecture](docs/quorum-architecture-light.drawio.png)

Editable source: [docs/architecture.drawio](docs/architecture.drawio) (open it in [draw.io](https://app.diagrams.net)).

## Run it from scratch

You need Docker, [k3d](https://k3d.io) 5.9+, kubectl and git.

```bash
git clone https://github.com/momoElbn/quorum-gitops
cd quorum-gitops
scripts/bootstrap.sh                      # k3d cluster, Argo CD, root app
kubectl -n quorum get quorumcluster -w    # wait until PHASE is Running
```

Argo CD deploys kube-prometheus-stack, the operator and a 3-node
`QuorumCluster` from Git. The images are public on GHCR, so you don't build anything.

Write and read a key from inside the cluster (followers redirect to in-cluster DNS names):

```bash
kubectl -n quorum run curl --rm -i --restart=Never --image=curlimages/curl:8.11.1 --command -- \
  sh -c 'curl -sL -X PUT --data hello quorum:8080/kv/greeting && curl -sL quorum:8080/kv/greeting'
```

Grafana (dashboard "Quorum"):

```bash
kubectl -n monitoring port-forward svc/kube-prometheus-stack-grafana 3000:80
# http://localhost:3000, user admin, password admin-local-only (local cluster only)
```

Without Kubernetes: `docker compose -f deploy/docker-compose.yml up -d --build`,
then `scripts/status.sh`.

## Experiment E1: leader failure

**Question:** when the leader crashes, how long do writes fail, and are any
acknowledged writes lost?

```bash
RATE=100 DURATION=30m scripts/run-checker.sh          # terminal 1
MODE=kill INTERVAL=60 COUNT=29 scripts/kill-leader.sh  # terminal 2
```

`MODE=kill` sends SIGKILL to the leader's process. Nothing shuts down
cleanly and leadership isn't handed off, so the two survivors have to detect
missing heartbeats and elect a new leader. The script then replaces the dead
pod and waits until all 3 pods are Ready before the next kill, so each round
is a single failure. Only `204` answers count as acknowledged. Every key and
value is unique, and read-backs are linearizable, so a missing or wrong value
means a write was really lost.

### Results

| | Run 1 | Run 2 |
|---|---|---|
| Leader kills | 29 | 29 |
| Writes sent | 179,999 | 180,000 |
| Acknowledged | 172,650 (95.9 %) | 171,777 (95.4 %) |
| **Lost acknowledged writes** | **0** | **0** |
| **Failover time, median** | **2.05 s** | **2.20 s** |
| **Failover time, max** | **3.21 s** | **2.91 s** |
| Disruption per kill, median / max | 2.29 s / 8.25 s | 2.24 s / 9.00 s |
| Total time unavailable | 73.5 s (4.1 %) | 82.2 s (4.6 %) |
| Failed writes that were applied anyway | 61 | 47 |

- **Nothing was lost:** all 344,427 acknowledged writes were read back correctly.
- **Failover time:** for each kill, the longest write outage that starts within 5 s of it. The median of about 2 s
  matches hashicorp/raft's defaults (a follower waits 1–2 s without a heartbeat before it calls an election). The two runs
  differ by 0.15 s.
- **Disruption per kill** also counts errors after the election. On 6 kills (run 1) and 4 kills (run 2), writes failed again for
  up to 6.5 s. The replacement pod reports Ready before it knows who the leader is, so it answers `503 no leader`.
  The next fix is to make readiness wait until the pod knows the leader.
- **Applied anyway:** the write was committed, but the leader died before it could reply. A failed write means
  "outcome unknown", so the checker reports these separately and doesn't count them as lost.

![Grafana dashboard during E1 run 2](docs/e1-grafana.png)

*Run 2 in Grafana: the leader moves to a new pod every minute and writes
stay at about 100/s. "Write errors" stays at 0 because requests to a crashed
server never reach it, so only the checker sees those failures.*

### Test conditions

| | |
|---|---|
| PC | AMD Ryzen 5 7600X (6 cores, 12 threads), 32 GB RAM |
| OS | Windows, WSL2 (kernel 6.6.87.2), Ubuntu 26.04 LTS |
| Docker | Docker Desktop 29.1.3 on WSL2: 12 CPUs, 15.2 GiB RAM |
| Kubernetes | k3d 5.9.0, k3s v1.35.5 (1 server + 2 agent nodes) |
| Cluster | 3 replicas, one pod per node, 1 GiB local-path volume each |
| Load | 100 writes/s for 30 min, 2 s client timeout |
| Fault | leader SIGKILLed every 60 s, 29 times; next kill only once all 3 pods are Ready |
| Version | quorum-server and quorum-operator at commit `be31ee1` |
| Date | 2026-11-03; run 1 04:06–04:38 UTC, run 2 04:40–05:11 UTC |

## Repository layout

| Path | What |
|---|---|
| `cmd/quorum-server`, `internal/store` | Raft key-value server |
| `cmd/quorum-checker` | load and data-loss checker |
| `operator/` | kubebuilder operator (`QuorumCluster` CRD) |
| `charts/` | Helm charts for the server and the operator |
| `deploy/` | Docker Compose setup and an example `QuorumCluster` |
| `scripts/` | `run-checker.sh`, `kill-leader.sh`, `status.sh`, `write-loop.sh` |
| `.github/workflows/ci.yml` | test, build, push to GHCR, deploy via quorum-gitops |
