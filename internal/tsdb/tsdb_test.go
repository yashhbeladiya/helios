package tsdb

import (
	"testing"
	"time"

	"helios/internal/api"
	"helios/internal/telemetry"
)

func entry(host, region string, reqs int64) api.TelemetryEntry {
	return api.TelemetryEntry{
		App:            host,
		Region:         region,
		Requests:       reqs,
		LatencySumMs:   reqs * 10,
		LatencyBuckets: make([]int64, telemetry.NumBuckets),
	}
}

// resolveWeb maps the routed host "web.local" to app "web".
func resolveWeb(host string) (string, bool) {
	if host == "web.local" {
		return "web", true
	}
	return "", false
}

func TestIngestAndQuery(t *testing.T) {
	db := New()
	now := time.Now()
	db.Ingest(api.TelemetryReport{
		End:     now,
		Entries: []api.TelemetryEntry{entry("web.local", "us-east", 100)},
	}, resolveWeb)

	series := db.Query("web", now.Add(-time.Minute))
	pts, ok := series["us-east"]
	if !ok {
		t.Fatalf("expected us-east series, got %v", series)
	}
	if len(pts) != 1 || pts[0].Requests != 100 {
		t.Fatalf("want 1 point with 100 requests, got %+v", pts)
	}
}

func TestIngestResolvesHostToApp(t *testing.T) {
	db := New()
	db.Ingest(api.TelemetryReport{
		End:     time.Now(),
		Entries: []api.TelemetryEntry{entry("web.local", "us-east", 10)},
	}, resolveWeb)

	apps := db.Apps()
	if len(apps) != 1 || apps[0] != "web" {
		t.Fatalf("host should resolve to app name 'web', got %v", apps)
	}
}

func TestIngestMergesSameSlot(t *testing.T) {
	db := New()
	now := time.Now()
	// Two reports whose End lands in the same resolution bucket.
	db.Ingest(api.TelemetryReport{End: now, Entries: []api.TelemetryEntry{entry("web.local", "us-east", 30)}}, resolveWeb)
	db.Ingest(api.TelemetryReport{End: now, Entries: []api.TelemetryEntry{entry("web.local", "us-east", 70)}}, resolveWeb)

	pts := db.Query("web", now.Add(-time.Minute))["us-east"]
	if len(pts) != 1 {
		t.Fatalf("same slot should merge to 1 point, got %d", len(pts))
	}
	if pts[0].Requests != 100 {
		t.Errorf("merged requests = %d, want 100", pts[0].Requests)
	}
}

func TestQueryOrdersOldestFirst(t *testing.T) {
	db := New()
	base := time.Now().Add(-50 * time.Second)
	// Ingest out of chronological order across distinct slots.
	for _, off := range []int{30, 0, 10, 20, 40} {
		db.Ingest(api.TelemetryReport{
			End:     base.Add(time.Duration(off) * time.Second),
			Entries: []api.TelemetryEntry{entry("web.local", "us-east", int64(off))},
		}, resolveWeb)
	}
	pts := db.Query("web", base.Add(-time.Minute))["us-east"]
	for i := 1; i < len(pts); i++ {
		if pts[i].Time.Before(pts[i-1].Time) {
			t.Fatalf("points not ordered oldest-first: %v", pts)
		}
	}
}

func TestRetentionPrunesOldData(t *testing.T) {
	db := New()
	old := time.Now().Add(-2 * Retention) // well beyond retention
	db.Ingest(api.TelemetryReport{
		End:     old,
		Entries: []api.TelemetryEntry{entry("web.local", "us-east", 100)},
	}, resolveWeb)

	if series := db.Query("web", old.Add(-time.Minute)); len(series) != 0 {
		t.Fatalf("data older than retention should be pruned, got %v", series)
	}
}

func TestQuerySinceFiltersHistory(t *testing.T) {
	db := New()
	now := time.Now()
	db.Ingest(api.TelemetryReport{End: now.Add(-40 * time.Second), Entries: []api.TelemetryEntry{entry("web.local", "us-east", 1)}}, resolveWeb)
	db.Ingest(api.TelemetryReport{End: now, Entries: []api.TelemetryEntry{entry("web.local", "us-east", 2)}}, resolveWeb)

	// Only ask for the last 20s: the older point must be excluded.
	pts := db.Query("web", now.Add(-20*time.Second))["us-east"]
	if len(pts) != 1 {
		t.Fatalf("since-filter: want 1 recent point, got %d", len(pts))
	}
	if pts[0].Requests != 2 {
		t.Errorf("wrong point survived filter: %+v", pts[0])
	}
}
