#!/usr/bin/env bash
# experiment.sh — the proof (roadmap #5). Runs the SAME rotating global
# workload (6 regions, demand rotating around the globe) against three
# placement strategies and compares cost (average replicas held) against
# client-observed latency and the % of requests served in-region:
#
#   helios         autoplace: the placer follows the sun within a budget
#   static-spread  same budget, fixed spread across regions (can't cover all 6)
#   static-cover   a replica in every region (static, but 3x the budget)
#
# Each arm runs on a fresh cluster for clean isolation. The workload
# (cmd/loadapp) pays a cross-region RTT penalty, so latency reflects whether
# a region's traffic is served by replicas in that region — which only the
# placer can arrange under shifting demand.
#
# Usage: scripts/experiment.sh
set -uo pipefail
cd "$(dirname "$0")/.."
export PATH="$PATH:/usr/local/go/bin"

RPS=120
DAY=80            # seconds per simulated global day
WARMUP=220        # unmeasured lead-in so the seasonal predictor learns the
                  # cycle (needs >=2 periods of history) before we measure
DURATION=120      # measured window (~1.5 days)
CONC=3            # demand concentrates in the currently-sunny region
RPS_PER=60        # capacity model: one replica serves 60 rps

# Six regions, but an affordable budget of only 2 replicas: static
# placement CANNOT cover every region, so it must guess where demand will
# be. This is the regime follow-the-sun is built for.
REGIONS=(us-east eu-west ap-south sa-east af-south au-east)
REGIONS_CSV=$(IFS=,; echo "${REGIONS[*]}")
BUDGET=2          # affordable replicas (placer max / static-spread count)
COVER=${#REGIONS[@]} # replicas needed to statically cover every region

# Scale down quickly enough to see follow-the-sun free capacity within a
# short run (production defaults are far more conservative). Applied to
# controld's placer via env.
export HELIOS_SCALEDOWN_COOLDOWN=15s
export HELIOS_LOW_STREAK=2

echo "=== build binaries + loadapp image ==="
make build >/dev/null
make loadapp-image >/dev/null 2>&1

RESULTS=""

run_arm() {
  local name="$1"; shift
  local deploy_flags="$*"
  local LOGDIR; LOGDIR="$(mktemp -d)"
  local pids=()

  echo ""
  echo "############ ARM: $name ############"
  ./bin/controld >"$LOGDIR/controld.log" 2>&1 & pids+=($!)
  sleep 0.5
  local idx=0
  for region in "${REGIONS[@]}"; do
    ./bin/noded -id "node-$idx" -addr "127.0.0.1:$((9091 + idx))" -region "$region" >"$LOGDIR/node-$idx.log" 2>&1 & pids+=($!)
    idx=$((idx + 1))
  done
  ./bin/proxyd >"$LOGDIR/proxyd.log" 2>&1 & pids+=($!)

  for _ in $(seq 1 40); do
    nc=$(curl -s http://127.0.0.1:8080/v1/nodes 2>/dev/null | grep -o '"id"' | wc -l | tr -d ' ' || true)
    [ "${nc:-0}" -ge "$COVER" ] && break || true; sleep 0.5
  done

  # shellcheck disable=SC2086
  ./bin/helios deploy --name web --image helios-loadapp:latest --port 8080 $deploy_flags >/dev/null
  for _ in $(seq 1 60); do
    code=$(curl -s -o /dev/null -w '%{http_code}' -H 'Host: web.local' http://127.0.0.1:8000/ || true)
    [ "$code" = "200" ] && break; sleep 0.5
  done

  # Warmup: drive the workload (unmeasured) so telemetry accumulates enough
  # history for the seasonal predictor to lock onto the cycle before we
  # measure. Static arms ignore it, so all arms see the same total load.
  if [ "${WARMUP:-0}" -gt 0 ]; then
    ./bin/trafficgen --host web.local --rps "$RPS" --day "${DAY}s" --duration "${WARMUP}s" \
      --concentration "$CONC" --regions "$REGIONS_CSV" >/dev/null 2>&1
  fi

  # Sample held replicas once per second for the cost metric.
  local sumfile="$LOGDIR/replicas.txt"; : > "$sumfile"
  ( while true; do
      curl -s http://127.0.0.1:8080/v1/instances 2>/dev/null | grep -o '"status":"healthy"' | wc -l | tr -d ' ' >> "$sumfile"
      sleep 1
    done ) & local sampler=$!

  ./bin/trafficgen --host web.local --rps "$RPS" --day "${DAY}s" --duration "${DURATION}s" \
    --concentration "$CONC" --regions "$REGIONS_CSV" >"$LOGDIR/trafficgen.log" 2>&1

  kill "$sampler" 2>/dev/null || true

  local line mean p95 local_pct avg
  line=$(grep LATENCY_MS "$LOGDIR/trafficgen.log" | tail -1)
  mean=$(echo "$line" | sed -E 's/.*mean=([0-9.]+).*/\1/')
  p95=$(echo "$line" | sed -E 's/.*p95=([0-9.]+).*/\1/')
  local_pct=$(echo "$line" | sed -E 's/.*local_pct=([0-9.]+).*/\1/')
  avg=$(awk '{s+=$1; n++} END{if(n)printf "%.2f", s/n; else print "0"}' "$sumfile")
  local errs; errs=$(grep -oE 'errs=[0-9]+' "$LOGDIR/trafficgen.log" | tail -1 | cut -d= -f2)

  printf 'ARM %-12s avg_replicas=%-5s mean=%-6s p95=%-6s local%%=%-6s errs=%s\n' \
    "$name" "$avg" "${mean:-?}" "${p95:-?}" "${local_pct:-?}" "${errs:-?}"
  RESULTS="${RESULTS}${name}|${avg}|${mean:-?}|${p95:-?}|${local_pct:-?}\n"

  for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done
  docker rm -f $(docker ps -aq --filter "name=helios-") >/dev/null 2>&1 || true
  sleep 1
}

trap 'pkill -f "bin/controld|bin/noded|bin/proxyd" 2>/dev/null || true; docker rm -f $(docker ps -aq --filter "name=helios-") >/dev/null 2>&1 || true' EXIT

# helios and static-spread share the SAME budget ($BUDGET); static-cover
# pays for a replica in every region (the only static way to be local
# everywhere).
run_arm "helios"        --replicas 1 --autoplace --rps-per-replica $RPS_PER --max-replicas $BUDGET
run_arm "static-spread" --replicas $BUDGET
run_arm "static-cover"  --replicas $COVER

echo ""
echo "===================== RESULTS ====================="
printf '%-13s %-13s %-9s %-9s %-9s\n' "strategy" "avg_replicas" "mean_ms" "p95_ms" "local_%"
printf '%-13s %-13s %-9s %-9s %-9s\n' "--------" "------------" "-------" "------" "-------"
printf "$RESULTS" | while IFS='|' read -r name avg mean p95 lp; do
  [ -z "$name" ] && continue
  printf '%-13s %-13s %-9s %-9s %-9s\n' "$name" "$avg" "$mean" "$p95" "$lp"
done
echo "==================================================="
echo "workload: ${RPS} rps, day=${DAY}s, ${DURATION}s, concentration=${CONC}; loadapp local=25ms cross-region=+75ms"
echo "cost = avg replicas held; lower is cheaper. local_% = requests served in-region; latency mean reflects locality."
