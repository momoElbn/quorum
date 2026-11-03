#!/usr/bin/env bash
# Writes an increasing number to one key every 100 ms and prints each result.
# Run it while you stop and start nodes, to see how long writes fail during
# a failover.
#
#   scripts/write-loop.sh                # key "counter" via localhost:8081..8083
#   KEY=x NODES="localhost:8082" scripts/write-loop.sh
#
# Follows 307 redirects to the leader. If a node can't be reached, it moves
# on to the next node. The value only goes up after a successful write, so
# the values that get stored have no gaps: after a failure, the same value is
# sent again. Repeating a PUT is safe, because setting a key to the same value
# twice has the same effect as doing it once.
set -u

read -r -a nodes <<<"${NODES:-localhost:8081 localhost:8082 localhost:8083}"
key=${KEY:-counter}
current=0 # index into nodes: the node we send to
value=1

while true; do
	node=${nodes[$current]}
	# Timestamp with milliseconds, using bash's own clock ($EPOCHREALTIME is
	# "seconds.microseconds"). This avoids date +%N, which not every date
	# implementation supports.
	now=$EPOCHREALTIME
	printf -v ts '%(%H:%M:%S)T.%s' "${now%.*}" "${now#*.}"
	ts=${ts:0:12} # keep 3 digits of the fraction: HH:MM:SS.mmm

	# -L follows the 307 and, because it is a 307, curl repeats the PUT
	# with the same body. %{url_effective} is the URL that answered last,
	# which shows which node actually took the write.
	out=$(curl -s -o /dev/null -L --max-time 2 -X PUT --data "$value" \
		-w '%{http_code} %{url_effective}' "http://$node/kv/$key")
	rc=$?

	if [ "$rc" -ne 0 ]; then
		# curl failed: connection refused, timeout, ... The node (or the
		# leader it redirected us to) is down. Try the next node.
		next=$(((current + 1) % ${#nodes[@]}))
		echo "$ts  $key=$value  via $node  ERROR curl exit $rc, switching to ${nodes[$next]}"
		current=$next
	else
		code=${out%% *}
		answered=${out#* }
		answered=${answered#http://}
		answered=${answered%%/*}
		if [ "$code" = 204 ]; then
			echo "$ts  $key=$value  via $node  OK (leader $answered)"
			value=$((value + 1))
		else
			# 503: no leader yet (election running) or the write failed.
			echo "$ts  $key=$value  via $node  FAILED http $code from $answered"
		fi
	fi
	sleep 0.1
done
