#!/usr/bin/env bash
# Experiment E3: scaling. Runs a fixed timeline against a 3-node cluster and
# prints a UTC timestamp for every event:
#
#   wait 60 s -> scale to 5 -> wait until done -> hold 120 s
#   -> scale to 3 -> wait until done -> hold 60 s
#   -> SIGKILL one follower -> hold 120 s
#
# Run scripts/run-checker.sh next to it to measure availability and lost
# writes over the whole timeline (about 8 minutes plus the scaling time,
# e.g. DURATION=12m).
#
#   scripts/scale-experiment.sh            # safe scaling (the operator's default)
#   UNSAFE=1 scripts/scale-experiment.sh   # naive baseline, spec.unsafeScaling=true
#
# "Done" means: in safe mode, the QuorumCluster lists N Raft members, N pods
# are Ready and the operator reports Idle. In unsafe mode the operator
# doesn't manage membership, so only N Ready pods are checked. Unsafe
# scale-down never removes anyone from Raft: 3 pods remain, but 5 voters,
# so the follower kill at the end leaves 2 of 5, which isn't a majority.
#
# Argo CD would undo the replica patches right away (self-heal), so this
# turns off auto-sync for the root and quorum-cluster apps while it runs,
# and turns it back on when it exits, even on Ctrl-C. Argo CD then sets the
# cluster back to what Git says (3 replicas, safe mode).
#
# Needs: kubectl pointed at the cluster; docker for the follower kill
# (see kill-leader.sh).
set -euo pipefail

NS=${NS:-quorum}
CLUSTER=${CLUSTER:-quorum}
UNSAFE=${UNSAFE:-0}
TIMEOUT=${TIMEOUT:-900} # seconds to wait for one scale operation
ARGO_APPS=${ARGO_APPS:-"root quorum-cluster"} # root first: it would restore the other's spec
SCRIPTS=$(cd "$(dirname "$0")" && pwd)

# Timestamps: whole seconds. Durations: seconds since the epoch.
now() { date -u +%s; }
log() { echo "$(date -u +%H:%M:%S)  $*"; }

qc() { kubectl -n "$NS" get quorumcluster "$CLUSTER" -o jsonpath="$1"; }
sts() { kubectl -n "$NS" get statefulset "$CLUSTER" -o jsonpath="$1"; }
patch_qc() { kubectl -n "$NS" patch quorumcluster "$CLUSTER" --type merge -p "$1" >/dev/null; }

# --- Argo CD: pause auto-sync, restore it on exit -------------------------
declare -A saved_policy
pause_argo() {
	local app
	for app in $ARGO_APPS; do
		saved_policy[$app]=$(kubectl -n argocd get application "$app" -o jsonpath='{.spec.syncPolicy}')
		kubectl -n argocd patch application "$app" --type merge -p '{"spec":{"syncPolicy":{"automated":null}}}' >/dev/null
	done
	log "paused Argo CD auto-sync for: $ARGO_APPS"
}
restore_argo() {
	local app
	for app in $ARGO_APPS; do
		[ -n "${saved_policy[$app]:-}" ] || continue
		kubectl -n argocd patch application "$app" --type merge -p "{\"spec\":{\"syncPolicy\":${saved_policy[$app]}}}" >/dev/null
	done
	log "restored Argo CD auto-sync; it will set the cluster back to what Git says"
}

# --- waiting ---------------------------------------------------------------

# reached N: is the cluster done scaling to N?
reached() {
	local n=$1
	[ "$(sts '{.spec.replicas}')" = "$n" ] || return 1
	[ "$(sts '{.status.replicas}')" = "$n" ] || return 1 # no extra pod left
	[ "$(sts '{.status.readyReplicas}')" = "$n" ] || return 1
	[ "$UNSAFE" = 1 ] && return 0
	[ "$(qc '{.status.members[*]}' | wc -w)" = "$n" ] || return 1
	[ "$(qc '{.status.scaling.step}')" = Idle ]
}

# Each operator step and when it was first seen, for the summary.
step_log=()

# wait_scaled N LABEL: wait until reached N, logging every operator step
# (safe mode) or ready-pod count (unsafe mode) as it changes.
wait_scaled() {
	local n=$1 label=$2 start seen="" cur
	start=$(now)
	while ! reached "$n"; do
		if [ "$UNSAFE" = 1 ]; then
			cur="ready $(sts '{.status.readyReplicas}')/$(sts '{.spec.replicas}') pods"
		else
			cur="$(qc '{.status.scaling.step} {.status.scaling.member}')"
		fi
		if [ "$cur" != "$seen" ]; then
			log "  $cur"
			step_log+=("$(now) $label: $cur")
			seen=$cur
		fi
		if [ $(($(now) - start)) -ge "$TIMEOUT" ]; then
			log "gave up after ${TIMEOUT}s waiting for $n ($label)"
			qc '{.status.conditions}'
			echo
			exit 1
		fi
		sleep 1
	done
	step_log+=("$(now) $label: done")
}

hold() {
	log "hold $1 s"
	sleep "$1"
}

# set_mode: make spec.unsafeScaling match UNSAFE. Changing it changes the
# pod template, so wait for the rolling update to finish.
set_mode() {
	local want=false cur
	[ "$UNSAFE" = 1 ] && want=true
	cur=$(qc '{.spec.unsafeScaling}')
	[ "${cur:-false}" = "$want" ] && return 0
	log "setting spec.unsafeScaling=$want (rolling restart)"
	patch_qc "{\"spec\":{\"unsafeScaling\":$want}}"
	# The operator applies the new template in its next reconcile, which
	# also sets the Scaling condition. Wait for that before asking for the
	# rollout, or rollout status could report on the old template.
	local reason
	while true; do
		reason=$(qc '{.status.conditions[?(@.type=="Scaling")].reason}')
		{ [ "$want" = true ] && [ "$reason" = UnsafeScaling ]; } && break
		{ [ "$want" = false ] && [ -n "$reason" ] && [ "$reason" != UnsafeScaling ]; } && break
		sleep 1
	done
	kubectl -n "$NS" rollout status "statefulset/$CLUSTER" --timeout=10m
	log "all pods Ready after the restart"
}

# --- the experiment --------------------------------------------------------

mode=safe
[ "$UNSAFE" = 1 ] && mode=unsafe
log "E3 scaling experiment on $NS/$CLUSTER, $mode mode"

trap restore_argo EXIT # set first: it restores only what pause_argo saved
pause_argo

set_mode
if [ "$(qc '{.spec.replicas}')" != 3 ] || ! reached 3; then
	log "expected a healthy 3-node cluster to start from"
	exit 1
fi

hold 60

log "scale to 5"
up_start=$(now)
patch_qc '{"spec":{"replicas":5}}'
wait_scaled 5 "3->5"
up_end=$(now)
log "scaled to 5 in $((up_end - up_start)) s"

hold 120

log "scale to 3"
down_start=$(now)
patch_qc '{"spec":{"replicas":3}}'
wait_scaled 3 "5->3"
down_end=$(now)
log "scaled to 3 in $((down_end - down_start)) s"

hold 60

log "SIGKILL one follower"
TARGET=follower MODE=kill INTERVAL=0 COUNT=1 NS=$NS CLUSTER=$CLUSTER "$SCRIPTS/kill-leader.sh" | sed 's/^/  /'

hold 120

# --- summary ---------------------------------------------------------------

echo
log "summary"
leader=$(qc '{.status.leader}')
if [ -n "$leader" ]; then
	echo "Raft members according to the leader ($leader):"
	kubectl get --raw "/api/v1/namespaces/$NS/pods/$leader:8080/proxy/status" |
		grep -o '"id":"[^"]*","raft_addr":"[^"]*","voter":[a-z]*' |
		sed 's/"id":"\([^"]*\)","raft_addr":"[^"]*","voter":\(.*\)/  \1  voter=\2/'
else
	echo "no leader: the QuorumCluster reports none (no majority?)"
fi

echo
echo "scale to 5: $((up_end - up_start)) s    scale to 3: $((down_end - down_start)) s"
echo "time per step (until the next one started):"
for i in "${!step_log[@]}"; do
	read -r t rest <<<"${step_log[$i]}"
	[[ $rest == *": done" ]] && continue
	read -r next _ <<<"${step_log[$((i + 1))]}"
	printf '  %-40s %4d s\n' "$rest" $((next - t))
done
echo
echo "Operator events (the precise timeline): kubectl -n $NS get events --field-selector involvedObject.kind=QuorumCluster --sort-by=.lastTimestamp"
