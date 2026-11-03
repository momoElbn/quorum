#!/usr/bin/env bash
# Runs quorum-checker inside the cluster, prints its report and copies the
# full JSON record to this machine.
#
#   scripts/run-checker.sh                          # 20 writes/s for 2 minutes
#   RATE=50 DURATION=5m scripts/run-checker.sh
#   # in another terminal, while it runs:
#   INTERVAL=20 scripts/kill-leader.sh
#
# Why inside the cluster: followers redirect writes to the leader's
# in-cluster DNS name (quorum-1.quorum-headless...), which doesn't resolve
# on this machine. And `kubectl port-forward svc/quorum` sticks to a single
# pod and breaks when that pod is killed.
#
# The checker ships in the quorum-server image. By default this uses the
# same image the cluster runs, so it's already on the nodes.
#
# Exit code: the checker's (0 = no acknowledged write lost).
set -euo pipefail

NS=${NS:-quorum}
CLUSTER=${CLUSTER:-quorum}
RATE=${RATE:-20}
DURATION=${DURATION:-2m}
OUT=${OUT:-results-$(date +%Y%m%d-%H%M%S).json}
IMAGE=${IMAGE:-$(kubectl -n "$NS" get statefulset "$CLUSTER" -o jsonpath='{.spec.template.spec.containers[0].image}')}
POD=quorum-checker

# Two containers share an emptyDir. "checker" runs the test and writes
# /out/results.json. "fetch" just waits, so that after the checker exits we
# can still copy the file out (kubectl cp needs tar, and the distroless
# image has none).
kubectl -n "$NS" delete pod "$POD" --ignore-not-found --wait=true >/dev/null
kubectl -n "$NS" apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: $POD
  labels:
    app.kubernetes.io/name: quorum-checker
spec:
  restartPolicy: Never
  securityContext:
    runAsNonRoot: true
    runAsUser: 65532
    runAsGroup: 65532
    fsGroup: 65532
  volumes:
    - name: out
      emptyDir: {}
  containers:
    - name: checker
      image: $IMAGE
      command:
        - /quorum-checker
        - -url=http://$CLUSTER:8080
        - -rate=$RATE
        - -duration=$DURATION
        - -out=/out/results.json
      volumeMounts:
        - name: out
          mountPath: /out
    - name: fetch
      image: busybox:1.37
      command: ["sh", "-c", "sleep 86400"]
      volumeMounts:
        - name: out
          mountPath: /out
EOF

cleanup() { kubectl -n "$NS" delete pod "$POD" --wait=false >/dev/null 2>&1 || true; }
trap cleanup EXIT

kubectl -n "$NS" wait --for=jsonpath='{.status.phase}'=Running pod/"$POD" --timeout=2m >/dev/null
# Follow the checker's output; this returns when the checker exits.
kubectl -n "$NS" logs -f "$POD" -c checker

# The log stream can end a moment before Kubernetes records the exit code.
code=""
for _ in $(seq 1 30); do
	code=$(kubectl -n "$NS" get pod "$POD" \
		-o jsonpath='{.status.containerStatuses[?(@.name=="checker")].state.terminated.exitCode}')
	[ -n "$code" ] && break
	sleep 1
done
kubectl -n "$NS" cp -c fetch "$POD:/out/results.json" "$OUT" >/dev/null
echo "copied the JSON record to $OUT"
exit "${code:-2}"
