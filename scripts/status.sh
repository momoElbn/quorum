#!/usr/bin/env bash
# Prints a table with each node's view of the cluster, from GET /status.
#
#   scripts/status.sh
#   NODES="localhost:8081 localhost:8082" scripts/status.sh
#
# Needs only bash, curl and sed (no jq).
set -u

NODES=${NODES:-"localhost:8081 localhost:8082 localhost:8083"}

# field NAME JSON: the value of a top-level "NAME": field. Good enough for
# /status, whose top-level values are plain strings and numbers.
field() {
	printf '%s' "$2" | sed -n "s/.*\"$1\":\"\{0,1\}\([^\",]*\).*/\1/p"
}

row='%-16s %-8s %-10s %-5s %-8s %-7s %-8s %s\n'
printf "$row" ADDRESS NODE STATE TERM LEADER COMMIT APPLIED MEMBERS
for node in $NODES; do
	if ! json=$(curl -sf --max-time 2 "http://$node/status"); then
		printf "$row" "$node" - unreachable - - - - -
		continue
	fi
	leader=$(field leader_id "$json")
	# Each member entry has one "raft_addr", so counting them counts members.
	members=$(printf '%s' "$json" | grep -o '"raft_addr"' | wc -l)
	printf "$row" "$node" \
		"$(field node_id "$json")" \
		"$(field state "$json")" \
		"$(field term "$json")" \
		"${leader:--}" \
		"$(field commit_index "$json")" \
		"$(field applied_index "$json")" \
		"$members"
done
