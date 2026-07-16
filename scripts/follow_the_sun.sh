#!/usr/bin/env bash
# follow_the_sun.sh — proves the M2 predictive control plane end to end.
# Deploys an autoplace app across 3 regions, drives rotating "daylight"
# traffic with trafficgen, and shows the placer moving replicas toward the
# region whose forecast is rising — before the peak lands.
#
# Usage: scripts/follow_the_sun.sh
set -uo pipefail
cd "$(dirname "$0")/.."
export PATH="$PATH:/usr/local/go/bin"

LOGDIR="$(mktemp -d)"
DAY=90          # seconds per simulated global day
DURATION=210    # total run (~2.3 days)
RPS=90

pids=()
cleanup() {
  echo "--- cleanup ---"
  for p in "${pids[@]:-}"; do kill "$p" 2>/dev/null || true; done
  docker rm -f $(docker ps -aq --filter "name=helios-") >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "=== build ==="; make build >/dev/null

echo "=== start control plane + 3 regional nodes + proxy ==="
./bin/controld >"$LOGDIR/controld.log" 2>&1 & pids+=($!)
sleep 0.5
./bin/noded -id node-a -addr 127.0.0.1:9091 -region us-east  >"$LOGDIR/a.log" 2>&1 & pids+=($!)
./bin/noded -id node-b -addr 127.0.0.1:9092 -region eu-west  >"$LOGDIR/b.log" 2>&1 & pids+=($!)
./bin/noded -id node-c -addr 127.0.0.1:9093 -region ap-south >"$LOGDIR/c.log" 2>&1 & pids+=($!)
./bin/proxyd >"$LOGDIR/proxyd.log" 2>&1 & pids+=($!)

for _ in $(seq 1 30); do
  n=$(curl -s http://127.0.0.1:8080/v1/nodes 2>/dev/null | grep -o '"id"' | wc -l | tr -d ' ' || true)
  [ "${n:-0}" -ge 3 ] && break || true; sleep 0.5
done
echo "nodes: ${n:-0} regions us-east / eu-west / ap-south"

echo "=== deploy autoplace app (rps/replica=20, max=6) ==="
./bin/helios deploy --name web --image nginx:alpine --port 80 --replicas 1 \
  --autoplace --rps-per-replica 20 --max-replicas 6 >/dev/null

for _ in $(seq 1 60); do
  code=$(curl -s -o /dev/null -w '%{http_code}' -H 'Host: web.local' http://127.0.0.1:8000/ || true)
  [ "$code" = "200" ] && break; sleep 0.5
done
echo "serving: $code"

echo "=== drive rotating global traffic (${RPS} rps, day=${DAY}s, ${DURATION}s) ==="
./bin/trafficgen --host web.local --rps "$RPS" --day "${DAY}s" --duration "${DURATION}s" \
  >"$LOGDIR/trafficgen.log" 2>&1 & tgpid=$!
pids+=($tgpid)

# Watch placement follow the traffic.
while kill -0 "$tgpid" 2>/dev/null; do
  sleep 20
  echo "--- t=$(date +%T) current placement (region:count) ---"
  curl -s http://127.0.0.1:8080/v1/instances 2>/dev/null \
    | tr '{' '\n' | grep -o '"node_id":"[^"]*"' | sort | uniq -c \
    | sed 's/node-a/us-east/;s/node-b/eu-west/;s/node-c/ap-south/' || true
done

echo ""
echo "========= PLACER DECISION LOG (newest first) ========="
curl -s http://127.0.0.1:8080/v1/decisions 2>/dev/null \
  | tr '{' '\n' | grep '"reason"' \
  | sed -E 's/.*"app":"([^"]*)".*"region":"([^"]*)".*"from":([0-9]+),"to":([0-9]+),"reason":"([^"]*)".*/\1 \2: \3 -> \4  (\5)/' \
  | head -40
echo "======================================================"
echo "logs in: $LOGDIR"
