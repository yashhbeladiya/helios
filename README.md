# Helios

**A deployment platform that anticipates its users.** Vercel and Fly ask you
where your app should live — Helios figures it out, continuously. It watches
live traffic geography and shape, forecasts the next window, and autonomously
decides *where* and *how big* every app should be: instances migrate around the
globe following demand ("follow the sun") and capacity pre-warms before spikes
land instead of cold-starting after them.

Built from first principles: no Kubernetes, no nginx — a custom scheduler,
reconciler, node agent, and L7 proxy, so every layer of the system can be
explained and defended.

## Status

**Milestone 1 (this repo): the core platform.**
Deploy container apps across a cluster, route traffic through a from-scratch
reverse proxy, and perform zero-downtime, make-before-break migration of an app
between nodes on command. Node failure triggers automatic rescheduling.

**Milestone 2: the predictive control plane.**
Traffic telemetry from `proxyd` feeds a wavefront predictor, which feeds an
autonomous placer. The reconciler converges on "desired state" from any
writer, so the placer is just another writer of that state.

## Architecture

```
            helios (CLI)
                │  deploy / status / migrate
                ▼
   ┌───────────────────────────┐
   │         controld          │   desired state, scheduler (bin-packing +
   │   API + reconciler loop   │   anti-affinity), migration orchestration
   └─────┬──────────────┬──────┘
         │ assignments   │ routing table
         ▼               ▼
   ┌──────────┐    ┌──────────┐
   │  noded   │    │  proxyd  │   L7 proxy: Host-header routing, round-robin,
   │ (per VM) │    │  (edge)  │   connection draining (hitless migration)
   └────┬─────┘    └──────────┘
        │ docker run / rm
        ▼
   app containers (ephemeral host ports, TCP health-checked)
```

Control flow is a reconciliation loop, not imperative commands: `controld`
recomputes desired placement every 2s; `noded` converges local Docker state to
its assignment; `proxyd` converges its routing table to actual healthy
instances. Everything is crash-tolerant by construction — kill any component
and it re-converges.

**Migration is make-before-break:** new instances start on the target node
first; old instances are marked *draining* (proxy sends no new requests,
in-flight requests complete); only when the target replicas are healthy does
the old copy stop. Zero dropped requests.

## Run it (single machine, three logical nodes)

Requirements: Go ≥ 1.22, Docker.

```bash
make build

# 1. control plane
./bin/controld

# 2. three "nodes" (separate terminals; same Docker daemon, distinct identities)
./bin/noded -id node-a -addr 127.0.0.1:9091 -region us-east
./bin/noded -id node-b -addr 127.0.0.1:9092 -region eu-west
./bin/noded -id node-c -addr 127.0.0.1:9093 -region ap-south

# 3. edge proxy
./bin/proxyd

# 4. deploy something real
./bin/helios deploy --name web --image nginx:alpine --port 80 --replicas 2
curl -H 'Host: web.local' http://localhost:8000/

# 5. the demo: hitless migration under load
#    terminal A: sustained load
while true; do curl -s -o /dev/null -w '%{http_code}\n' -H 'Host: web.local' http://localhost:8000/; done
#    terminal B:
./bin/helios migrate --app web --node node-c
#    watch: zero non-200s while instances move. Then kill a noded process
#    and watch the reconciler reschedule its instances within seconds.
```

`helios status` shows nodes, instances, and placement at any time.

## Tested & verified

Unit tests cover the pure logic where correctness lives — run `make test`
(race detector + coverage):

```
scheduler   95.6%    reconciler  86.3%    telemetry  95.3%
tsdb        96.4%    place       92.2%    predict    75.6%
```

They pin down the subtle invariants: placement stability and anti-affinity,
dead-node rescheduling, the make-before-break migration lifecycle, the
placer's asymmetric hysteresis (immediate scale-up, cooldown-gated
scale-down) and global floor, and the predictor's trend extrapolation.

Three scripts drive the *running* cluster end to end and produce hard
numbers (each is self-contained: build → start cluster → measure → clean up):

- **`scripts/migration_loadtest.sh`** — hammers `web` through proxyd with 8
  concurrent workers while migrating it across nodes. Result: **0 non-200
  responses out of ~10k requests**, repeatably (≈28k requests across 3 runs,
  zero dropped).
- **`scripts/failover.sh`** — kills a node (agent *and* its containers) and
  times recovery. Result: the lost replica is rescheduled and healthy again
  on a surviving node in **~14s** (dominated by the 10s dead-node detection
  window; tunable via `NodeAliveWindow`).
- **`scripts/follow_the_sun.sh`** — runs the predictive control plane against
  rotating global traffic. The placer autonomously moves replicas across
  us-east → eu-west → ap-south chasing forecasted demand, e.g.:
  ```
  web us-east: 0 -> 6  (scale-up: holt forecast 211.1 rps, capacity 20 rps/replica)
  web us-east: 6 -> 0  (scale-down: forecast 0.0 rps low for 4 ticks, cooldown passed)
  web eu-west: 0 -> 4  (scale-up: holt forecast 104.5 rps, capacity 20 rps/replica)
  ```

Two correctness bugs surfaced *by these load tests* and were fixed:

1. **Migration wasn't actually hitless** (~2.6% of requests 502'd): `noded`
   killed a de-assigned container before proxyd had stopped routing to it.
   Fixed with a two-phase teardown — the node reports the instance as
   *draining* (dropped from the routing table) and keeps it alive for a
   `DrainGrace` window so in-flight requests finish before removal.
2. **Dead nodes kept receiving traffic**: a crashed node's last-reported
   instances lingered in state and stayed in the routing table (their
   containers gone → 502). `Routes()` now excludes instances on any node
   past its heartbeat window.

## Design notes & honest limitations (M1)

- **State is in-memory** behind a `Store` interface — restart `controld` and
  nodes re-register/re-report, but app specs are lost. Next: sqlite, then a
  replicated log (this is the deliberate swap point).
- **Single controld** — no control-plane HA yet. If it dies, the data plane
  keeps serving: proxyd holds its last-synced routing table and containers
  keep running; only *changes* (deploys, migrations, failover) pause.
- **Health checks are TCP** — HTTP `/healthz` is a natural upgrade.
- **Apps are prebuilt images** — the source→image build pipeline (git push
  deploys) is a later addition.
- Agents **poll** (2s) rather than watch; fine at this scale, and an honest
  conversation-starter about watch streams vs. polling.

## Telemetry

proxyd is the traffic sensor: every request is recorded by (app, client
region) with a latency histogram, aggregated at the edge, and shipped as
delta reports to controld every 5s. controld keeps them in a tiny built-in
time-series store (10s buckets, 1h retention) — the exact data the
predictor reads. Client region comes from the `X-Client-Region`
header in simulation; production would GeoIP the client address (the
resolver in `internal/proxy` is the one-function seam).

Demo — watch a simulated global day rotate through your telemetry:

```bash
# with the M1 stack running and `web` deployed:
./bin/trafficgen --host web.local --rps 50 --day 120s

# in another terminal, poll the series (per-region rates + p95):
watch -n 2 "curl -s 'localhost:8080/v1/telemetry?app=web&minutes=2'"
```

You'll see demand peak in us-east, hand off to eu-west, then ap-south, and
wrap — one full cycle every two minutes. That rotating wave is exactly what
the predictor forecasts and the placer chases.

## Wavefront predictor

Every 10s the predictor refits per-(app, region) forecasts over the last
5 minutes of telemetry and projects 30s ahead. Two algorithms run side by
side: **EWMA** (smooth, trend-blind baseline) and **Holt** double
exponential smoothing (level + trend) — on the rising edge of a regional
wave Holt forecasts *above* current traffic, which is the lead time the
placer exploits. The engine is stateless-recompute (same crash-tolerant
philosophy as the reconciler) and **self-scoring**: every forecast is later
compared to the actual, maintaining a running MAE per algorithm, so the API
reports not just predictions but how wrong each predictor has been.

```bash
# with trafficgen running:
watch -n 2 "curl -s 'localhost:8080/v1/forecast?app=web'"
```

Watch a region's rising edge: `holt.predicted_rps` climbs ahead of
`current_rps` while `ewma` lags — and compare their `mae` after a few
simulated days. Details that matter: gaps in the series are filled as zero
traffic (a missing bucket is silence, not missing data), and the newest,
partial bucket is excluded from fitting (it would read as a phantom crash).

## Autonomous placer — follow the sun

Deploy an app with `--autoplace` and Helios manages its geography: every
10s the placer turns per-region Holt forecasts into a region plan
(app → region → replica count) and writes it to the store. It never touches
nodes or containers — it is deliberately *just another writer of desired
state*; the reconciler and (now region-aware) scheduler converge on it.

Anti-flapping policy is asymmetric by design: **scale-ups are immediate**
(the forecast lead time is the whole point — hesitating wastes it) while
**scale-downs are reluctant** (the forecast must stay low for 3 consecutive
ticks *and* a 60s cooldown must have passed since that region's last
scale-up). A floor guarantees one replica globally, in the strongest-
forecast region. Every change is logged with its reason at
`GET /v1/decisions`.

The follow-the-sun demo:

```bash
./bin/helios deploy --name web --image nginx:alpine --port 80 \
    --autoplace --rps-per-replica 20 --max-replicas 5
./bin/trafficgen --host web.local --rps 50 --day 180s

# watch instances chase the traffic around the "globe":
watch -n 2 './bin/helios status'
watch -n 2 "curl -s localhost:8080/v1/plan; echo; curl -s localhost:8080/v1/decisions | head -c 2000"
```

Within one simulated day you'll see replicas appear in us-east as its wave
rises, spread to eu-west as the peak hands off, and drain from us-east
after the cooldown — with every decision explained in the log. Manual
`helios migrate` still works and takes priority over the plan while active.

## Roadmap

Built so far: the cluster, scheduler, reconciler, and proxy; hitless
migration; automatic failover; edge traffic telemetry; the wavefront
predictor; and the autonomous placer with its decision log. The scripts in
`scripts/` drive the live cluster end to end and produce the numbers above.

Still ahead: an automated p95/cost comparison against a static multi-region
baseline, a source→image build pipeline (git-push deploys), and a live
world-map view of placement decisions.
