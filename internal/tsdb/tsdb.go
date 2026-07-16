// Package tsdb is a deliberately tiny in-memory time-series store for
// traffic telemetry on the control plane: fixed-resolution buckets per
// (app, region), bounded retention, histogram merging.
//
// This is the data the Milestone 2 predictor reads (step 3) and the
// world-map dashboard charts (step 6). When it needs to survive restarts
// it moves behind the same swap-point philosophy as internal/store.
package tsdb

import (
	"sync"
	"time"

	"helios/internal/api"
	"helios/internal/telemetry"
)

// Resolution is the bucket width. Reports are merged into the bucket
// containing their window end.
const Resolution = 10 * time.Second

// Retention is how much history is kept per series.
const Retention = time.Hour

type seriesKey struct{ App, Region string }

// Point is one resolution-bucket of one (app, region) series.
type Point struct {
	Time         time.Time `json:"time"` // bucket start
	Requests     int64     `json:"requests"`
	LatencySumMs int64     `json:"latency_sum_ms"`
	P95Ms        int64     `json:"p95_ms"`
	buckets      []int64
}

// RegionSeries is what queries return: region -> ordered points.
type RegionSeries map[string][]Point

type DB struct {
	mu     sync.RWMutex
	series map[seriesKey]map[int64]*Point // slot (unix/res) -> point
}

func New() *DB {
	return &DB{series: map[seriesKey]map[int64]*Point{}}
}

// Ingest merges one telemetry report. resolveApp maps a routed hostname
// to an app name (reports are keyed by Host at the proxy).
func (db *DB) Ingest(report api.TelemetryReport, resolveApp func(host string) (string, bool)) {
	slot := report.End.Truncate(Resolution).Unix() / int64(Resolution.Seconds())

	db.mu.Lock()
	defer db.mu.Unlock()
	for _, e := range report.Entries {
		app, ok := resolveApp(e.App)
		if !ok {
			app = e.App // unknown host: keep raw for debuggability
		}
		k := seriesKey{App: app, Region: e.Region}
		slots, ok := db.series[k]
		if !ok {
			slots = map[int64]*Point{}
			db.series[k] = slots
		}
		p, ok := slots[slot]
		if !ok {
			p = &Point{
				Time:    time.Unix(slot*int64(Resolution.Seconds()), 0),
				buckets: make([]int64, telemetry.NumBuckets),
			}
			slots[slot] = p
		}
		p.Requests += e.Requests
		p.LatencySumMs += e.LatencySumMs
		for i, n := range e.LatencyBuckets {
			if i < len(p.buckets) {
				p.buckets[i] += n
			}
		}
		p.P95Ms = telemetry.EstimateP95Ms(p.buckets)
	}
	db.pruneLocked()
}

func (db *DB) pruneLocked() {
	cutoff := time.Now().Add(-Retention).Unix() / int64(Resolution.Seconds())
	for _, slots := range db.series {
		for slot := range slots {
			if slot < cutoff {
				delete(slots, slot)
			}
		}
	}
}

// Query returns per-region points for an app since the given time,
// ordered oldest-first.
func (db *DB) Query(app string, since time.Time) RegionSeries {
	sinceSlot := since.Unix() / int64(Resolution.Seconds())

	db.mu.RLock()
	defer db.mu.RUnlock()
	out := RegionSeries{}
	for k, slots := range db.series {
		if k.App != app {
			continue
		}
		var pts []Point
		for slot, p := range slots {
			if slot >= sinceSlot {
				pts = append(pts, *p)
			}
		}
		sortPoints(pts)
		if len(pts) > 0 {
			out[k.Region] = pts
		}
	}
	return out
}

// Apps lists app names present in the store.
func (db *DB) Apps() []string {
	db.mu.RLock()
	defer db.mu.RUnlock()
	seen := map[string]bool{}
	var out []string
	for k := range db.series {
		if !seen[k.App] {
			seen[k.App] = true
			out = append(out, k.App)
		}
	}
	return out
}

func sortPoints(pts []Point) {
	// insertion sort: series are short (<= Retention/Resolution points)
	for i := 1; i < len(pts); i++ {
		for j := i; j > 0 && pts[j].Time.Before(pts[j-1].Time); j-- {
			pts[j], pts[j-1] = pts[j-1], pts[j]
		}
	}
}
