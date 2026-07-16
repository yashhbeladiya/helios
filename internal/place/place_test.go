package place

import (
	"testing"
	"time"

	"helios/internal/api"
	"helios/internal/predict"
	"helios/internal/store"
)

// stubFC is a settable forecast source standing in for the predictor.
type stubFC struct{ f []predict.Forecast }

func (s *stubFC) Forecasts() []predict.Forecast { return s.f }

func fc(region string, holtRPS float64) predict.Forecast {
	return predict.Forecast{
		App: "web", Region: region, CurrentRPS: holtRPS,
		Holt:     predict.AlgoForecast{PredictedRPS: holtRPS},
		EWMA:     predict.AlgoForecast{PredictedRPS: holtRPS},
		Best:     predict.AlgoForecast{PredictedRPS: holtRPS},
		BestAlgo: "holt",
	}
}

func autoApp(rpsPerReplica, maxReplicas int) api.App {
	return api.App{
		Name: "web", Image: "img", Port: 80, Replicas: 1, Host: "web.local",
		CPUMillis: 100, MemMB: 64,
		AutoPlace: true, RPSPerReplica: rpsPerReplica, MaxReplicas: maxReplicas,
	}
}

// keepNodesAlive (re)registers nodes with a heartbeat at `now` so the
// placer sees their regions as live for that tick.
func keepNodesAlive(st *store.Memory, now time.Time, regions ...string) {
	for i, r := range regions {
		st.UpsertNode(api.Node{
			ID: r + "-node", Addr: "127.0.0.1:909" + string(rune('0'+i)),
			Region: r, CPUMillis: 10_000, MemMB: 10_000, LastHeartbeat: now,
		})
	}
}

func planSum(p map[string]int) int {
	total := 0
	for _, n := range p {
		total += n
	}
	return total
}

func TestStep_ScaleUpIsImmediate(t *testing.T) {
	st := store.NewMemory()
	st.UpsertApp(autoApp(50, 10))
	now := time.Now()
	keepNodesAlive(st, now, "us-east")

	e := NewEngine(st, &stubFC{f: []predict.Forecast{fc("us-east", 100)}})
	e.step(now)

	plan := st.RegionPlans()["web"]
	// ceil(100 rps * 1.2 headroom / 50 rps-per-replica) = 3
	if plan["us-east"] != 3 {
		t.Fatalf("scale-up should be immediate to 3 replicas, got %v", plan)
	}
}

func TestStep_GlobalFloorKeepsOneReplica(t *testing.T) {
	st := store.NewMemory()
	st.UpsertApp(autoApp(50, 10))
	now := time.Now()
	keepNodesAlive(st, now, "us-east", "eu-west")

	// No traffic anywhere.
	e := NewEngine(st, &stubFC{f: nil})
	e.step(now)

	plan := st.RegionPlans()["web"]
	if planSum(plan) != 1 {
		t.Fatalf("floor: app must keep exactly one replica somewhere on total silence, got %v", plan)
	}
}

func TestStep_RespectsGlobalReplicaCap(t *testing.T) {
	st := store.NewMemory()
	st.UpsertApp(autoApp(50, 2)) // cap at 2
	now := time.Now()
	keepNodesAlive(st, now, "us-east", "eu-west")

	// Both regions want 3 each (6 total) — must be trimmed to 2.
	e := NewEngine(st, &stubFC{f: []predict.Forecast{fc("us-east", 100), fc("eu-west", 100)}})
	e.step(now)

	plan := st.RegionPlans()["web"]
	if planSum(plan) != 2 {
		t.Fatalf("cap: total replicas must not exceed 2, got %v (sum %d)", plan, planSum(plan))
	}
}

func TestStep_ScaleDownIsReluctant(t *testing.T) {
	st := store.NewMemory()
	st.UpsertApp(autoApp(50, 10))
	fcSrc := &stubFC{}
	e := NewEngine(st, fcSrc)

	t0 := time.Now()

	// Tick 1: high demand -> scale up to 3.
	keepNodesAlive(st, t0, "us-east")
	fcSrc.f = []predict.Forecast{fc("us-east", 100)}
	e.step(t0)
	if st.RegionPlans()["web"]["us-east"] != 3 {
		t.Fatalf("setup: expected scale-up to 3, got %v", st.RegionPlans()["web"])
	}

	// Demand collapses. Scale-down must NOT happen until the low forecast
	// persists for LowStreakTicks AND the cooldown since scale-up passes.
	fcSrc.f = []predict.Forecast{fc("us-east", 0)}

	// Ticks 2 & 3 (past cooldown but streak not yet met): hold at 3.
	for i, dt := range []time.Duration{61 * time.Second, 62 * time.Second} {
		now := t0.Add(dt)
		keepNodesAlive(st, now, "us-east")
		e.step(now)
		if got := st.RegionPlans()["web"]["us-east"]; got != 3 {
			t.Fatalf("tick %d: premature scale-down, plan=%d want 3", i+2, got)
		}
	}

	// Tick 4: streak (3) satisfied and cooldown elapsed -> scale down.
	now := t0.Add(63 * time.Second)
	keepNodesAlive(st, now, "us-east")
	e.step(now)
	if got := st.RegionPlans()["web"]["us-east"]; got != 1 {
		t.Fatalf("tick 4: expected reluctant scale-down to floor (1), got %d", got)
	}
}

func TestStep_IgnoresNonAutoPlaceApps(t *testing.T) {
	st := store.NewMemory()
	app := autoApp(50, 10)
	app.AutoPlace = false
	st.UpsertApp(app)
	now := time.Now()
	keepNodesAlive(st, now, "us-east")

	e := NewEngine(st, &stubFC{f: []predict.Forecast{fc("us-east", 100)}})
	e.step(now)

	if plan := st.RegionPlans()["web"]; len(plan) != 0 {
		t.Fatalf("non-autoplace app must not get a region plan, got %v", plan)
	}
}

func TestStep_DecisionsAreAudited(t *testing.T) {
	st := store.NewMemory()
	st.UpsertApp(autoApp(50, 10))
	now := time.Now()
	keepNodesAlive(st, now, "us-east")

	e := NewEngine(st, &stubFC{f: []predict.Forecast{fc("us-east", 100)}})
	e.step(now)

	ds := e.Decisions()
	if len(ds) == 0 {
		t.Fatal("scale-up should record an audited decision with a reason")
	}
	if ds[0].To != 3 || ds[0].Reason == "" {
		t.Errorf("decision = %+v, want To=3 with a non-empty reason", ds[0])
	}
}

func TestCapTotal_TrimsLowestForecastFirst(t *testing.T) {
	needed := map[string]int{"us": 3, "eu": 3}
	rps := map[string]float64{"us": 100, "eu": 50}
	capTotal(needed, rps, 1)

	if planSum(needed) != 1 {
		t.Fatalf("capTotal should cut to 1, got %v", needed)
	}
	if needed["eu"] > needed["us"] {
		t.Errorf("lower-forecast region eu should be trimmed first: %v", needed)
	}
}

func TestEnsureFloor(t *testing.T) {
	needed := map[string]int{"us": 0, "eu": 0}
	rps := map[string]float64{"us": 5, "eu": 10}
	ensureFloor(needed, rps, map[string]int{})

	if needed["eu"] != 1 || planSum(needed) != 1 {
		t.Errorf("floor should land the single replica in the strongest-forecast region (eu): %v", needed)
	}

	// When capacity already exists, floor is a no-op.
	have := map[string]int{"us": 2}
	ensureFloor(have, map[string]float64{"us": 5}, map[string]int{})
	if have["us"] != 2 {
		t.Errorf("ensureFloor must not alter a plan that already has replicas: %v", have)
	}
}
