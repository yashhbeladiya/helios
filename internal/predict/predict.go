// Package predict implements the wavefront predictor: short-horizon
// per-(app, region) request-rate forecasts computed from the telemetry
// time series.
//
// Two algorithms run side by side:
//
//   - EWMA: exponentially weighted moving average. Smooth but trend-blind
//     (always forecasts "more of the same"). This is the honest baseline.
//   - Holt: double exponential smoothing (level + trend). On the rising
//     edge of a regional traffic wave it forecasts ABOVE the current
//     level — that lead time is what lets the placer (step 4) pre-warm
//     capacity before the spike lands.
//
// The engine is a stateless-recompute loop, same philosophy as the
// reconciler: every tick it re-reads the recent window from the tsdb and
// refits from scratch. It also self-scores: every forecast is remembered
// and, once the target bucket's actual value arrives, folded into a
// running mean absolute error per algorithm. Consumers can therefore see
// not just the prediction but how wrong each predictor has been lately.
package predict

import (
	"math"
	"sync"
	"time"

	"helios/internal/tsdb"
)

const (
	// Window of history to fit on.
	Window = 5 * time.Minute
	// HorizonBuckets is how far ahead forecasts target, in tsdb
	// resolution buckets (3 x 10s = 30s ahead).
	HorizonBuckets = 3

	alphaEWMA = 0.4 // EWMA smoothing
	alphaHolt = 0.5 // Holt level smoothing
	betaHolt  = 0.3 // Holt trend smoothing
	maeDecay  = 0.1 // running-MAE update weight

	// overshootCap bounds Holt's extrapolation to this multiple of the
	// largest value observed in the fit window.
	overshootCap = 1.5
)

// AlgoForecast is one algorithm's view of one series.
type AlgoForecast struct {
	PredictedRPS float64 `json:"predicted_rps"`
	MAE          float64 `json:"mae"` // running mean absolute error (RPS)
}

// Forecast is the full prediction for one (app, region).
type Forecast struct {
	App        string       `json:"app"`
	Region     string       `json:"region"`
	CurrentRPS float64      `json:"current_rps"`
	HorizonSec int          `json:"horizon_sec"`
	EWMA       AlgoForecast `json:"ewma"`
	Holt       AlgoForecast `json:"holt"`
}

type seriesKey struct{ app, region, algo string }

type pending struct {
	slot      int64 // bucket the forecast targets
	predicted float64
}

type Engine struct {
	db *tsdb.DB

	mu        sync.RWMutex
	forecasts []Forecast
	pendings  map[seriesKey][]pending
	mae       map[seriesKey]float64
	maeSeeded map[seriesKey]bool
}

func NewEngine(db *tsdb.DB) *Engine {
	return &Engine{
		db:        db,
		pendings:  map[seriesKey][]pending{},
		mae:       map[seriesKey]float64{},
		maeSeeded: map[seriesKey]bool{},
	}
}

// Run recomputes forecasts every interval. Blocks.
func (e *Engine) Run(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		e.step(time.Now())
	}
}

// Forecasts returns the latest forecast set (all apps).
func (e *Engine) Forecasts() []Forecast {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return append([]Forecast(nil), e.forecasts...)
}

func (e *Engine) step(now time.Time) {
	res := int64(tsdb.Resolution.Seconds())
	// Only completed buckets: the bucket containing `now` is partial and
	// would read as a spurious traffic crash.
	lastComplete := now.Truncate(tsdb.Resolution).Unix()/res - 1

	var out []Forecast
	for _, app := range e.db.Apps() {
		series := e.db.Query(app, now.Add(-Window))
		for region, points := range series {
			values, firstSlot := fill(points, lastComplete, res)
			if len(values) < 2 {
				continue
			}
			current := values[len(values)-1]

			ewmaPred := forecastEWMA(values)
			holtPred := forecastHolt(values, HorizonBuckets)

			targetSlot := lastComplete + HorizonBuckets
			e.settleAndRecord(app, region, "ewma", ewmaPred, targetSlot, values, firstSlot)
			e.settleAndRecord(app, region, "holt", holtPred, targetSlot, values, firstSlot)

			out = append(out, Forecast{
				App:        app,
				Region:     region,
				CurrentRPS: current,
				HorizonSec: HorizonBuckets * int(res),
				EWMA:       AlgoForecast{PredictedRPS: ewmaPred, MAE: e.getMAE(app, region, "ewma")},
				Holt:       AlgoForecast{PredictedRPS: holtPred, MAE: e.getMAE(app, region, "holt")},
			})
		}
	}

	e.mu.Lock()
	e.forecasts = out
	e.mu.Unlock()
}

// settleAndRecord scores any matured past forecasts for this series
// against actuals, then queues the new forecast for future scoring.
func (e *Engine) settleAndRecord(app, region, algo string, predicted float64, targetSlot int64, values []float64, firstSlot int64) {
	k := seriesKey{app, region, algo}
	lastSlot := firstSlot + int64(len(values)) - 1

	e.mu.Lock()
	defer e.mu.Unlock()

	var still []pending
	for _, p := range e.pendings[k] {
		if p.slot > lastSlot {
			still = append(still, p)
			continue
		}
		idx := p.slot - firstSlot
		if idx < 0 {
			continue // fell out of the window before maturing; drop
		}
		err := math.Abs(p.predicted - values[idx])
		if !e.maeSeeded[k] {
			e.mae[k] = err
			e.maeSeeded[k] = true
		} else {
			e.mae[k] = maeDecay*err + (1-maeDecay)*e.mae[k]
		}
	}
	still = append(still, pending{slot: targetSlot, predicted: predicted})
	e.pendings[k] = still
}

func (e *Engine) getMAE(app, region, algo string) float64 {
	return e.mae[seriesKey{app, region, algo}]
}

// fill converts sparse tsdb points into a dense per-bucket RPS slice up
// to lastComplete, inserting zeros for gaps (a missing bucket means zero
// traffic, not missing data). Returns the values and the slot of
// values[0].
func fill(points []tsdb.Point, lastComplete, res int64) ([]float64, int64) {
	if len(points) == 0 {
		return nil, 0
	}
	bySlot := map[int64]float64{}
	firstSlot := int64(math.MaxInt64)
	for _, p := range points {
		slot := p.Time.Unix() / res
		if slot > lastComplete {
			continue // partial bucket
		}
		bySlot[slot] = float64(p.Requests) / float64(res)
		if slot < firstSlot {
			firstSlot = slot
		}
	}
	if firstSlot == math.MaxInt64 || firstSlot > lastComplete {
		return nil, 0
	}
	values := make([]float64, lastComplete-firstSlot+1)
	for slot, rps := range bySlot {
		values[slot-firstSlot] = rps
	}
	return values, firstSlot
}

// forecastEWMA fits an EWMA over the series and forecasts flat.
func forecastEWMA(values []float64) float64 {
	level := values[0]
	for _, x := range values[1:] {
		level = alphaEWMA*x + (1-alphaEWMA)*level
	}
	return clamp(level)
}

// forecastHolt fits Holt's linear trend method and extrapolates
// `steps` buckets ahead. The extrapolation is bounded to overshootCap
// times the largest value in the window: on a steep rising edge the raw
// trend term can project far above anything actually observed, which would
// make the placer massively over-provision. Keeping the forecast above the
// current level (the pre-warm lead) while capping runaway extrapolation is
// the honest middle ground.
func forecastHolt(values []float64, steps int) float64 {
	level := values[0]
	trend := values[1] - values[0]
	maxV := values[0]
	for _, x := range values[1:] {
		prevLevel := level
		level = alphaHolt*x + (1-alphaHolt)*(level+trend)
		trend = betaHolt*(level-prevLevel) + (1-betaHolt)*trend
		if x > maxV {
			maxV = x
		}
	}
	f := level + float64(steps)*trend
	if cap := overshootCap * maxV; maxV > 0 && f > cap {
		f = cap
	}
	return clamp(f)
}

func clamp(v float64) float64 {
	if v < 0 || math.IsNaN(v) {
		return 0
	}
	return v
}
