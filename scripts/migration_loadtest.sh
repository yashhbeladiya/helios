#!/usr/bin/env bash
# migration_loadtest.sh — proves hitless (make-before-break) migration:
# hammer an app through proxyd while migrating it across nodes, and count
# every non-2xx response. A correct system drops zero requests.
#
# Usage: scripts/migration_loadtest.sh
set -euo pipefail
cd "$(dirname "$0")/.."
export PATH="$PATH:/usr/local/go/bin"

LOGDIR="$(mktemp -d)"
CODES="$LOGDIR/codes.txt"
CONCURRENCY=8
LOAD_SECONDS=20
MIGRATE_AT=6   # seconds into the load window

pids=()
cleanup() {
  echo "--- cleanup ---"
  for p in "${pids[@]:-}"; do kill "$p" 2>/dev/null || true; done
  docker rm -f $(docker ps -aq --filter "name=helios-") >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "=== build ==="
make build >/dev/null

echo "=== start control plane + 3 nodes + proxy ==="
./bin/controld >"$LOGDIR/controld.log" 2>&1 &  pids+=($!)
sleep 0.5
./bin/noded -id node-a -addr 127.0.0.1:9091 -region us-east >"$LOGDIR/node-a.log" 2>&1 &  pids+=($!)
./bin/noded -id node-b -addr 127.0.0.1:9092 -region eu-west >"$LOGDIR/node-b.log" 2>&1 &  pids+=($!)
./bin/noded -id node-c -addr 127.0.0.1:9093 -region ap-south >"$LOGDIR/node-c.log" 2>&1 & pids+=($!)
./bin/proxyd >"$LOGDIR/proxyd.log" 2>&1 &      pids+=($!)

echo "=== wait for 3 nodes to register ==="
n=0
for _ in $(seq 1 30); do
  n=$(curl -s http://127.0.0.1:8080/v1/nodes 2>/dev/null | grep -o '"id"' | wc -l | tr -d ' ' || true)
  [ "${n:-0}" -ge 3 ] && break || true
  sleep 0.5
done
echo "nodes registered: $n"

echo "=== deploy web (nginx, 2 replicas) ==="
./bin/helios deploy --name web --image nginx:alpine --port 80 --replicas 2 >/dev/null

echo "=== wait until app serves 200 through proxyd ==="
for _ in $(seq 1 60); do
  code=$(curl -s -o /dev/null -w '%{http_code}' -H 'Host: web.local' http://127.0.0.1:8000/ || true)
  [ "$code" = "200" ] && break; sleep 0.5
done
echo "first good response: $code"
echo "initial placement:"
curl -s http://127.0.0.1:8080/v1/instances | tr ',' '\n' | grep -E '"node_id"|"status"' || true

echo "=== load: ${CONCURRENCY} workers x ${LOAD_SECONDS}s, migrate at ${MIGRATE_AT}s ==="
: > "$CODES"
deadline=$(( $(date +%s) + LOAD_SECONDS ))
worker() {
  while [ "$(date +%s)" -lt "$deadline" ]; do
    curl -s -o /dev/null -w '%{http_code}\n' -H 'Host: web.local' http://127.0.0.1:8000/ >> "$CODES" 2>/dev/null || echo "000" >> "$CODES"
  done
}
load_pids=()
for _ in $(seq 1 $CONCURRENCY); do worker & load_pids+=($!); done

sleep "$MIGRATE_AT"
echo ">>> migrating web -> node-c under load"
./bin/helios migrate --app web --node node-c >/dev/null

for p in "${load_pids[@]}"; do wait "$p"; done

echo "=== final placement ==="
curl -s http://127.0.0.1:8080/v1/instances | tr ',' '\n' | grep -E '"node_id"|"status"' || true

echo ""
echo "=========== RESULT ==========="
total=$(wc -l < "$CODES" | tr -d ' ')
ok=$(grep -c '^200$' "$CODES" || true)
non200=$(( total - ok ))
echo "total requests: $total"
echo "  200 OK:       $ok"
echo "  non-200:      $non200"
echo "status code distribution:"
sort "$CODES" | uniq -c | sort -rn
echo "=============================="
echo "logs in: $LOGDIR"
