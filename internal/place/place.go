// Package place implements the autonomous placer — the piece that makes
// Helios follow the sun. Every tick it turns per-region forecasts into a
// region plan (app -> region -> replica count) and writes it to the
// store, where the reconciler + scheduler converge on it. The placer
// never touches nodes or containers: it is deliberately just another
// writer of desired state.
//
// The core control problem is flapping: forecasts wiggle, and reacting
// to every wiggle would migrate instances constantly. The policy is
// asymmetric by design:
//
//   - Scale-UP is immediate. Latency is what Helios protects; forecast
//     lead time is worthless if we hesitate.
//   - Scale-DOWN is reluctant: it requires the low forecast to persist
//     for LowStreakTicks consecutive ticks AND a cooldown since the last
//     scale-up in that region. Removing capacity is cheap to delay and
//     expensive to regret.
//
// A global floor guarantees at least one replica somewhere (the region
// with the strongest forecast) — the placer may chase the sun, but it
// never turns the app off.
//
// Every change is recorded with its reason in a ring buffer
// (GET /v1/decisions): the audit trail, the demo narration, and the feed
// for the world-map dashboard.
package place

import (
	"fmt"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"helios/internal/predict"
	"helios/internal/scheduler"
	"helios/internal/store"
)

const (
	// Headroom over-provisions relative to the forecast.
	Headroom = 1.2
	// ScaleDownCooldown: minimum time since the last scale-up in a
	// region before that region may scale down.
	ScaleDownCooldown = 60 * time.Second
	// LowStreakTicks: consecutive ticks the forecast must stay below
	// current capacity before scaling down.
	LowStreakTicks = 3
	// DefaultRPSPerReplica if the app didn't specify a capacity model.
	DefaultRPSPerReplica = 50
	// DefaultMaxReplicas if the app didn't specify a global cap.
	DefaultMaxReplicas = 10

	decisionRingSize = 200
)

// Decision is one audited placement change.
type Decision struct {
	Time   time.Time `json:"time"`
	App    string    `json:"app"`
	Region string    `json:"region"`
	From   int       `json:"from"`
	To     int       `json:"to"`
	Reason string    `json:"reason"`
}

// forecastSource lets tests stub the predictor.
type forecastSource interface {
	Forecasts() []predict.Forecast
}

type regionKey struct{ app, region string }

type Engine struct {
	st store.Store
	fc forecastSource

	// Scale-down hysteresis, overridable via env for demos/experiments
	// (HELIOS_SCALEDOWN_COOLDOWN, HELIOS_LOW_STREAK); defaults are the
	// package constants.
	cooldown   time.Duration
	lowStreakN int

	mu          sync.RWMutex
	decisions   []Decision
	lastScaleUp map[regionKey]time.Time
	lowStreak   map[regionKey]int
}

func NewEngine(st store.Store, fc forecastSource) *Engine {
	return &Engine{
		st:          st,
		fc:          fc,
		cooldown:    envDuration("HELIOS_SCALEDOWN_COOLDOWN", ScaleDownCooldown),
		lowStreakN:  envInt("HELIOS_LOW_STREAK", LowStreakTicks),
		lastScaleUp: map[regionKey]time.Time{},
		lowStreak:   map[regionKey]int{},
	}
}

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

// Run recomputes plans every interval. Blocks.
func (e *Engine) Run(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		e.step(time.Now())
	}
}

// Decisions returns the recent decision log, newest first.
func (e *Engine) Decisions() []Decision {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]Decision, len(e.decisions))
	for i, d := range e.decisions {
		out[len(out)-1-i] = d
	}
	return out
}

func (e *Engine) step(now time.Time) {
	// Regions that actually have live nodes: traffic from regions we
	// can't serve locally can't drive placement.
	regions := map[string]bool{}
	for _, n := range e.st.Nodes() {
		if now.Sub(n.LastHeartbeat) <= scheduler.NodeAliveWindow {
			regions[n.Region] = true
		}
	}
	if len(regions) == 0 {
		return
	}

	// Group forecasts by app.
	byApp := map[string]map[string]predict.Forecast{}
	for _, f := range e.fc.Forecasts() {
		if !regions[f.Region] {
			continue
		}
		if byApp[f.App] == nil {
			byApp[f.App] = map[string]predict.Forecast{}
		}
		byApp[f.App][f.Region] = f
	}

	plans := e.st.RegionPlans()

	for _, app := range e.st.Apps() {
		if !app.AutoPlace {
			continue
		}
		rpsPer := app.RPSPerReplica
		if rpsPer <= 0 {
			rpsPer = DefaultRPSPerReplica
		}
		maxReplicas := app.MaxReplicas
		if maxReplicas <= 0 {
			maxReplicas = DefaultMaxReplicas
		}

		cur := plans[app.Name]
		if cur == nil {
			cur = map[string]int{}
		}
		forecasts := byApp[app.Name] // may be nil: no traffic seen yet

		// Raw need per region from the best forecast: the predictor
		// self-scores each algorithm's accuracy (MAE) and exposes the most
		// accurate one as Best. On cyclic demand that's the seasonal model,
		// which pre-warms through turning points instead of lagging them.
		needed := map[string]int{}
		rpsOf := map[string]float64{}
		for region := range regions {
			var rps float64
			if f, ok := forecasts[region]; ok {
				rps = f.Best.PredictedRPS
			}
			rpsOf[region] = rps
			needed[region] = int(math.Ceil(rps * Headroom / float64(rpsPer)))
		}

		capTotal(needed, rpsOf, maxReplicas)
		ensureFloor(needed, rpsOf, cur)

		// Apply asymmetric hysteresis region by region.
		final := map[string]int{}
		changed := false
		for region := range regions {
			k := regionKey{app.Name, region}
			c, n := cur[region], needed[region]
			switch {
			case n > c: // scale up: immediate
				final[region] = n
				e.record(now, app.Name, region, c, n,
					fmt.Sprintf("scale-up: holt forecast %.1f rps, capacity %d rps/replica", rpsOf[region], rpsPer))
				e.mu.Lock()
				e.lastScaleUp[k] = now
				e.lowStreak[k] = 0
				e.mu.Unlock()
				changed = true
			case n < c: // scale down: reluctant
				e.mu.Lock()
				e.lowStreak[k]++
				streak := e.lowStreak[k]
				lastUp := e.lastScaleUp[k]
				e.mu.Unlock()
				if streak >= e.lowStreakN && now.Sub(lastUp) >= e.cooldown {
					final[region] = n
					e.record(now, app.Name, region, c, n,
						fmt.Sprintf("scale-down: forecast %.1f rps low for %d ticks, cooldown passed", rpsOf[region], streak))
					e.mu.Lock()
					e.lowStreak[k] = 0
					e.mu.Unlock()
					changed = true
				} else {
					final[region] = c // hold
				}
			default:
				final[region] = c
				e.mu.Lock()
				e.lowStreak[k] = 0
				e.mu.Unlock()
			}
		}

		if changed || len(plans[app.Name]) == 0 {
			e.st.SetRegionPlan(app.Name, final)
		}
	}
}

// capTotal trims the plan to the global replica cap, removing capacity
// from the lowest-forecast regions first.
func capTotal(needed map[string]int, rpsOf map[string]float64, maxReplicas int) {
	total := 0
	for _, n := range needed {
		total += n
	}
	if total <= maxReplicas {
		return
	}
	order := make([]string, 0, len(needed))
	for r := range needed {
		order = append(order, r)
	}
	sort.Slice(order, func(i, j int) bool {
		if rpsOf[order[i]] != rpsOf[order[j]] {
			return rpsOf[order[i]] < rpsOf[order[j]]
		}
		return order[i] < order[j]
	})
	for total > maxReplicas {
		for _, r := range order {
			if needed[r] > 0 && total > maxReplicas {
				needed[r]--
				total--
			}
		}
	}
}

// ensureFloor guarantees at least one replica globally, in the region
// with the strongest forecast (ties broken by current placement, then
// name for determinism).
func ensureFloor(needed map[string]int, rpsOf map[string]float64, cur map[string]int) {
	for _, n := range needed {
		if n > 0 {
			return
		}
	}
	best, bestScore := "", -1.0
	for r := range needed {
		score := rpsOf[r]
		if cur[r] > 0 {
			score += 0.001 // prefer staying put on total silence
		}
		if score > bestScore || (score == bestScore && r < best) {
			best, bestScore = r, score
		}
	}
	if best != "" {
		needed[best] = 1
	}
}

func (e *Engine) record(t time.Time, app, region string, from, to int, reason string) {
	log.Printf("placer: %s/%s %d -> %d (%s)", app, region, from, to, reason)
	e.mu.Lock()
	defer e.mu.Unlock()
	e.decisions = append(e.decisions, Decision{Time: t, App: app, Region: region, From: from, To: to, Reason: reason})
	if len(e.decisions) > decisionRingSize {
		e.decisions = e.decisions[len(e.decisions)-decisionRingSize:]
	}
}
