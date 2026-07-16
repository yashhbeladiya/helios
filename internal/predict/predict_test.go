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
