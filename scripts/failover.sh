#!/usr/bin/env bash
# failover.sh — measures automatic recovery from node loss. Deploys 3
# replicas (one per node), simulates a node dying (kills its agent AND its
# containers), and times how long until the cluster reschedules the lost
# replica and it becomes healthy again on a surviving node.
#
# Usage: scripts/failover.sh
set -uo pipefail
cd "$(dirname "$0")/.."
export PATH="$PATH:/usr/local/go/bin"

LOGDIR="$(mktemp -d)"
pids=()
cleanup() {
  echo "--- cleanup ---"
  for p in "${pids[@]:-}"; do kill "$p" 2>/dev/null || true; done
  docker rm -f $(docker ps -aq --filter "name=helios-") >/dev/null 2>&1 || true
}
trap cleanup EXIT

healthy_count() { curl -s http://127.0.0.1:8080/v1/instances 2>/dev/null | grep -o '"status":"healthy"' | wc -l | tr -d ' '; }
# healthy replicas on SURVIVING nodes only (a dead node's ghost instances
# linger in actual state; they must not count toward recovery).
healthy_alive() { curl -s http://127.0.0.1:8080/v1/instances 2>/dev/null | tr '{' '\n' | grep '"status":"healthy"' | grep -v '"node_id":"node-a"' | wc -l | tr -d ' '; }

echo "=== build ==="; make build >/dev/null

echo "=== start control plane + 3 nodes + proxy ==="
./bin/controld >"$LOGDIR/controld.log" 2>&1 & pids+=($!)
sleep 0.5
./bin/noded -id node-a -addr 127.0.0.1:9091 -region us-east  >"$LOGDIR/a.log" 2>&1 & A_PID=$!; pids+=($A_PID)
./bin/noded -id node-b -addr 127.0.0.1:9092 -region eu-west  >"$LOGDIR/b.log" 2>&1 & pids+=($!)
./bin/noded -id node-c -addr 127.0.0.1:9093 -region ap-south >"$LOGDIR/c.log" 2>&1 & pids+=($!)
./bin/proxyd >"$LOGDIR/proxyd.log" 2>&1 & pids+=($!)

for _ in $(seq 1 30); do
  nc=$(curl -s http://127.0.0.1:8080/v1/nodes 2>/dev/null | grep -o '"id"' | wc -l | tr -d ' ' || true)
  [ "${nc:-0}" -ge 3 ] && break || true; sleep 0.5
done

echo "=== deploy web (3 replicas, spread across nodes) ==="
./bin/helios deploy --name web --image nginx:alpine --port 80 --replicas 3 >/dev/null
for _ in $(seq 1 60); do [ "$(healthy_count)" -ge 3 ] && break; sleep 0.5; done
echo "healthy replicas before failure: $(healthy_count)"

# Identify node-a's containers, then simulate node-a dying.
victims=$(docker ps --filter "name=helios-" --format '{{.ID}} {{.Ports}}' | awk '{print $1}')
a_containers=$(curl -s http://127.0.0.1:8080/v1/instances \
  | tr '{' '\n' | grep '"node_id":"node-a"' | grep -o '"container_id":"[^"]*"' | cut -d'"' -f4)

echo "=== KILL node-a (agent + its containers) ==="
kill -9 "$A_PID" 2>/dev/null || true
for c in $a_containers; do docker rm -f "$c" >/dev/null 2>&1 || true; done
t0=$(date +%s)

echo "=== wait for reschedule + re-heal to 3 healthy on surviving nodes ==="
recovered=""
for _ in $(seq 1 60); do
  hc=$(healthy_alive)
  if [ "${hc:-0}" -ge 3 ]; then recovered=$(( $(date +%s) - t0 )); break; fi
  sleep 0.5
done

echo ""
echo "=========== RESULT ==========="
if [ -n "$recovered" ]; then
  echo "recovered to 3 healthy replicas in ~${recovered}s after node loss"
else
  echo "did NOT recover within 30s (healthy=$(healthy_count))"
fi
echo "final placement:"
curl -s http://127.0.0.1:8080/v1/instances | tr '{' '\n' | grep -o '"node_id":"[^"]*"' | sort | uniq -c || true
echo "=============================="
echo "logs in: $LOGDIR"
