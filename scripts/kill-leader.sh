#!/usr/bin/env bash
# Every INTERVAL seconds, finds the Raft leader of a quorum cluster and takes
# it down. Run it next to scripts/run-checker.sh to measure failovers and to
# check that no acknowledged write is lost.
#
#   scripts/kill-leader.sh                      # delete the leader pod every 30s
#   INTERVAL=15 COUNT=4 scripts/kill-leader.sh  # 4 times, 15s apart
#   MODE=kill scripts/kill-leader.sh            # crash it instead (see below)
#
# MODE=delete (default) deletes the leader's pod. Kubernetes sends SIGTERM
# first, so the server hands leadership to another node before exiting. This
# is what a rolling update or a node drain does, and it should cost almost
# no availability.
#
# MODE=kill crashes the leader: SIGKILL straight to the quorum-server
# process, with no SIGTERM and no handoff, like a kernel OOM kill or a power
# cut. The others have to notice the missing heartbeats and elect a new
# leader, so expect a second or two without writes. The dead pod is then
# replaced by a fresh one (see crash below). This reaches into the k3d node
# container with `docker exec`, so it only works on k3d.
#
# Both modes wait until every pod is Ready before taking the next leader
# down, so each round tests exactly one failure.
# (Even `kubectl delete --grace-period=0 --force` isn't a crash: the kubelet
# still sends SIGTERM before killing the container.)
#
# Needs: kubectl pointed at the cluster; docker for MODE=kill.
set -euo pipefail

NS=${NS:-quorum}
CLUSTER=${CLUSTER:-quorum} # QuorumCluster name
INTERVAL=${INTERVAL:-30}
COUNT=${COUNT:-0} # 0 = forever
MODE=${MODE:-delete}

case "$MODE" in
delete | kill) ;;
*)
	echo "MODE must be delete or kill" >&2
	exit 2
	;;
esac

# find_leader prints the name of the pod that says it is the leader.
# It asks each pod's GET /status through the API server's pod proxy, which
# works from outside the cluster without port-forwarding. (kubectl exec
# would need a shell, which the distroless image doesn't have.)
find_leader() {
	local pods pod
	pods=$(kubectl -n "$NS" get pods -l "app.kubernetes.io/name=quorum,app.kubernetes.io/instance=$CLUSTER" \
		-o jsonpath='{.items[*].metadata.name}')
	for pod in $pods; do
		if kubectl get --raw "/api/v1/namespaces/$NS/pods/$pod:8080/proxy/status" 2>/dev/null |
			grep -q '"state":"leader"'; then
			echo "$pod"
			return 0
		fi
	done
	return 1
}

# crash sends SIGKILL to the quorum-server process of a pod. A k3d node is a
# Docker container running containerd; crictl (containerd's CLI) finds the
# pod's container and its process ID on that node.
crash() {
	local pod=$1 node cid pid
	node=$(kubectl -n "$NS" get pod "$pod" -o jsonpath='{.spec.nodeName}')
	cid=$(docker exec "$node" crictl ps -q --name quorum-server \
		--label "io.kubernetes.pod.name=$pod" --label "io.kubernetes.pod.namespace=$NS")
	pid=$(docker exec "$node" crictl inspect --output go-template --template '{{.info.pid}}' "$cid")
	docker exec "$node" kill -9 "$pid"
	# Then replace the pod instead of letting the kubelet restart the crashed
	# container in place. The kubelet waits longer after each crash of the
	# same container (CrashLoopBackOff: 10s, 20s, 40s... up to 5 min), so
	# after a few rounds a killed pod would still be down at the next kill:
	# two of three nodes gone, which is a different experiment. A new pod
	# starts without that history. The process is already dead, so this
	# delete can't trigger a graceful handoff.
	kubectl -n "$NS" delete pod "$pod" --wait=false >/dev/null
}

# wait_healthy waits until every pod of the cluster is Ready and prints how
# many seconds that took. Each kill should hit a healthy cluster, so every
# measured outage is one leader dying, not two failures piling up.
wait_healthy() {
	local want ready waited=0
	want=$(kubectl -n "$NS" get statefulset "$CLUSTER" -o jsonpath='{.spec.replicas}')
	while true; do
		ready=$(kubectl -n "$NS" get statefulset "$CLUSTER" -o jsonpath='{.status.readyReplicas}')
		[ "${ready:-0}" = "$want" ] && break
		sleep 1
		waited=$((waited + 1))
	done
	echo "$waited"
}

echo "taking down the leader of $NS/$CLUSTER every ${INTERVAL}s (mode: $MODE, count: ${COUNT/#0/unlimited})"
done_count=0
while [ "$COUNT" -eq 0 ] || [ "$done_count" -lt "$COUNT" ]; do
	sleep "$INTERVAL"
	waited=$(wait_healthy)
	if [ "$waited" -gt 0 ]; then
		echo "$(date -u +%H:%M:%S)  waited ${waited}s for all pods to be ready"
	fi
	ts=$(date -u +%H:%M:%S)
	if ! leader=$(find_leader); then
		# Probably still electing after the last round. Try next time.
		echo "$ts  no leader found, skipping this round"
		continue
	fi
	if [ "$MODE" = kill ]; then
		crash "$leader"
		what="crashed (SIGKILL)"
	else
		kubectl -n "$NS" delete pod "$leader" --wait=false >/dev/null
		what="deleted"
	fi
	done_count=$((done_count + 1))
	echo "$ts  $what leader $leader [$done_count]"
done
