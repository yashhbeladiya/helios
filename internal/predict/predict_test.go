package predict

import (
	"math"
	"testing"
	"time"

	"helios/internal/api"
	"helios/internal/telemetry"
	"helios/internal/tsdb"
)

func TestForecastEWMA_ConstantSeries(t *testing.T) {
	got := forecastEWMA([]float64{5, 5, 5, 5, 5})
	if math.Abs(got-5) > 1e-9 {
		t.Errorf("EWMA of constant 5 = %v, want 5", got)
	}
}

func TestForecastHolt_ExtrapolatesRisingTrend(t *testing.T) {
	// Steadily rising: a trend-aware forecast must exceed the last value,
	// which is exactly the pre-warm lead time the placer relies on.
	values := []float64{1, 2, 3, 4, 5}
	got := forecastHolt(values, HorizonBuckets)
	if got <= values[len(values)-1] {
		t.Errorf("Holt forecast %v should exceed current %v on a rising trend", got, values[len(values)-1])
	}
}

func TestForecastHolt_ClampsNegativeToZero(t *testing.T) {
	// Falling hard: naive extrapolation would go negative; must clamp.
	got := forecastHolt([]float64{10, 5, 0}, HorizonBuckets)
	if got < 0 {
		t.Errorf("Holt forecast must never be negative, got %v", got)
	}
}

func TestClamp(t *testing.T) {
	if clamp(-1) != 0 {
		t.Error("negative should clamp to 0")
	}
	if clamp(math.NaN()) != 0 {
		t.Error("NaN should clamp to 0")
	}
	if clamp(3.5) != 3.5 {
		t.Error("valid value should pass through")
	}
}

func TestFill_InsertsZerosForGaps(t *testing.T) {
	res := int64(tsdb.Resolution.Seconds())
	base := time.Unix(1_700_000_000, 0) // resolution-aligned
	slot0 := base.Unix() / res
	points := []tsdb.Point{
		{Time: base, Requests: 50},                        // rps 5
		{Time: base.Add(20 * time.Second), Requests: 100}, // rps 10, two slots later
	}
	values, first := fill(points, slot0+2, res)

	if first != slot0 {
		t.Fatalf("first slot = %d, want %d", first, slot0)
	}
	want := []float64{5, 0, 10} // the gap bucket is zero traffic, not missing
	if len(values) != len(want) {
		t.Fatalf("len(values) = %d, want %d: %v", len(values), len(want), values)
	}
	for i := range want {
		if math.Abs(values[i]-want[i]) > 1e-9 {
			t.Errorf("values[%d] = %v, want %v", i, values[i], want[i])
		}
	}
}

// resolveWeb maps routed host to app for tsdb ingest.
func resolveWeb(host string) (string, bool) {
	if host == "web.local" {
		return "web", true
	}
	return "", false
}

func TestEngine_StepEmitsForecastForConstantLoad(t *testing.T) {
	db := tsdb.New()
	res := int64(tsdb.Resolution.Seconds())

	// Build ~10 recent, resolution-aligned buckets of constant 5 RPS so
	// they fall inside both the retention window and the fit window.
	nowReal := time.Now()
	lastEnd := time.Unix((nowReal.Unix()/res)*res, 0)
	const buckets = 10
	for i := 0; i < buckets; i++ {
		end := lastEnd.Add(-time.Duration(i) * tsdb.Resolution)
		db.Ingest(api.TelemetryReport{
			End: end,
			Entries: []api.TelemetryEntry{{
				App: "web.local", Region: "us-east",
				Requests:       5 * res, // 5 rps over the bucket
				LatencyBuckets: make([]int64, telemetry.NumBuckets),
			}},
		}, resolveWeb)
	}

	e := NewEngine(db)
	stepNow := lastEnd.Add(tsdb.Resolution)
	e.step(stepNow)

	fcs := e.Forecasts()
	if len(fcs) != 1 {
		t.Fatalf("want 1 forecast, got %d: %+v", len(fcs), fcs)
	}
	f := fcs[0]
	if f.App != "web" || f.Region != "us-east" {
		t.Errorf("forecast identity = %s/%s, want web/us-east", f.App, f.Region)
	}
	if math.Abs(f.CurrentRPS-5) > 1 {
		t.Errorf("current rps = %v, want ~5", f.CurrentRPS)
	}
	if math.Abs(f.EWMA.PredictedRPS-5) > 1 {
		t.Errorf("EWMA forecast = %v, want ~5 for constant load", f.EWMA.PredictedRPS)
	}
	if math.Abs(f.Holt.PredictedRPS-5) > 1.5 {
		t.Errorf("Holt forecast = %v, want ~5 for constant load", f.Holt.PredictedRPS)
	}
}

// --- seasonal predictor (option 2) ---

// sineSeries builds a periodic RPS series of `n` buckets with the given
// period, oscillating between ~10 and ~90 rps.
func sineSeries(n, period int) []float64 {
	v := make([]float64, n)
	for i := range v {
		x := 50 + 40*math.Sin(2*math.Pi*float64(i)/float64(period))
		if x < 0 {
			x = 0
		}
		v[i] = x
	}
	return v
}

func TestDetectPeriod_FindsCycle(t *testing.T) {
	if p := detectPeriod(sineSeries(60, 10)); p != 10 {
		t.Errorf("detectPeriod = %d, want 10", p)
	}
}

func TestDetectPeriod_FlatSeriesHasNone(t *testing.T) {
	flat := make([]float64, 40)
	for i := range flat {
		flat[i] = 7
	}
	if p := detectPeriod(flat); p != 0 {
		t.Errorf("flat series period = %d, want 0", p)
	}
}

func TestForecastSeasonal_RequiresTwoCycles(t *testing.T) {
	if _, ok := forecastSeasonal(sineSeries(15, 10), 0, 5, 10); ok {
		t.Error("seasonal should refuse with fewer than two full cycles")
	}
	if _, ok := forecastSeasonal(sineSeries(30, 10), 0, 5, 10); !ok {
		t.Error("seasonal should work with two+ full cycles")
	}
}

func TestForecastSeasonal_PredictsSamePhase(t *testing.T) {
	// With clean periodicity, the forecast for `steps` ahead should match
	// the true value at the target phase.
	period, n, steps := 10, 50, 5
	vals := sineSeries(n, period)
	got, ok := forecastSeasonal(vals, 0, steps, period)
	if !ok {
		t.Fatal("expected a seasonal forecast")
	}
	targetSlot := int64(n-1) + int64(steps)
	want := 50 + 40*math.Sin(2*math.Pi*float64(targetSlot)/float64(period))
	if math.Abs(got-want) > 2 {
		t.Errorf("seasonal forecast = %.2f, want ~%.2f (phase %d)", got, want, targetSlot%int64(period))
	}
}

// The headline test: on periodic demand, the seasonal model's self-scored
// MAE beats Holt's, and the engine selects it as Best.
func TestEngine_SeasonalBeatsHoltOnPeriodicDemand(t *testing.T) {
	db := tsdb.New()
	res := int64(tsdb.Resolution.Seconds())
	const period, n = 10, 70

	nowReal := time.Now()
	lastEnd := time.Unix((nowReal.Unix()/res)*res, 0)
	v := func(slot int64) float64 {
		x := 50 + 40*math.Sin(2*math.Pi*float64(slot)/float64(period))
		if x < 0 {
			x = 0
		}
		return x
	}
	for i := 0; i < n; i++ {
		end := lastEnd.Add(-time.Duration(n-1-i) * tsdb.Resolution)
		slot := end.Unix() / res
		db.Ingest(api.TelemetryReport{
			End: end,
			Entries: []api.TelemetryEntry{{
				App: "web.local", Region: "us-east",
				Requests:       int64(v(slot) * float64(res)),
				LatencyBuckets: make([]int64, telemetry.NumBuckets),
			}},
		}, resolveWeb)
	}

	e := NewEngine(db)
	firstEnd := lastEnd.Add(-time.Duration(n-1) * tsdb.Resolution)
	for s := 0; s < n; s++ {
		e.step(firstEnd.Add(time.Duration(s+1) * tsdb.Resolution))
	}

	holtMAE := e.getMAE("web", "us-east", "holt")
	seasonalMAE := e.getMAE("web", "us-east", "seasonal")
	if !e.maeSeeded[seriesKey{"web", "us-east", "seasonal"}] {
		t.Fatal("seasonal model never matured — check window/period")
	}
	if seasonalMAE >= holtMAE {
		t.Errorf("seasonal MAE %.2f should beat Holt MAE %.2f on periodic demand", seasonalMAE, holtMAE)
	}

	fcs := e.Forecasts()
	if len(fcs) != 1 {
		t.Fatalf("want 1 forecast, got %d", len(fcs))
	}
	if fcs[0].BestAlgo != "seasonal" {
		t.Errorf("model selection picked %q, want seasonal (period detected: %ds)", fcs[0].BestAlgo, fcs[0].PeriodSec)
	}
	if fcs[0].PeriodSec != period*int(res) {
		t.Errorf("detected period = %ds, want %ds", fcs[0].PeriodSec, period*int(res))
	}
}
