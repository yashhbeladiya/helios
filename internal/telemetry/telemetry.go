// Package telemetry implements edge-side traffic measurement.
//
// proxyd records every proxied request into a Collector: request count and
// a coarse exponential latency histogram per (app, client region). Every
// report interval the collector is flushed — snapshot and reset — and the
// delta is shipped to controld. Aggregating at the edge and shipping deltas
// keeps telemetry O(apps x regions) per interval, independent of request
// volume.
package telemetry

import (
	"sync"

	"helios/internal/api"
)

// BoundsMs are the latency histogram bucket upper bounds in milliseconds.
// A request of duration d lands in the first bucket with bound >= d; the
// final implicit bucket is +Inf. Percentiles (p95 etc.) are estimated from
// these buckets on the query side.
var BoundsMs = []int64{1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000}

// NumBuckets is len(BoundsMs)+1 (the last is the +Inf overflow bucket).
var NumBuckets = len(BoundsMs) + 1

type key struct{ app, region string }

type cell struct {
	requests     int64
	latencySumMs int64
	buckets      []int64
}

// Collector accumulates per-(app,region) counters between flushes.
type Collector struct {
	mu    sync.Mutex
	cells map[key]*cell
}

func NewCollector() *Collector {
	return &Collector{cells: map[key]*cell{}}
}

// Record adds one served request.
func (c *Collector) Record(app, region string, latencyMs int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	k := key{app, region}
	cl, ok := c.cells[k]
	if !ok {
		cl = &cell{buckets: make([]int64, NumBuckets)}
		c.cells[k] = cl
	}
	cl.requests++
	cl.latencySumMs += latencyMs
	cl.buckets[bucketFor(latencyMs)]++
}

func bucketFor(ms int64) int {
	for i, bound := range BoundsMs {
		if ms <= bound {
			return i
		}
	}
	return len(BoundsMs) // +Inf
}

// Flush snapshots and resets the collector, returning the delta since the
// previous flush.
func (c *Collector) Flush() []api.TelemetryEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]api.TelemetryEntry, 0, len(c.cells))
	for k, cl := range c.cells {
		out = append(out, api.TelemetryEntry{
			App:            k.app,
			Region:         k.region,
			Requests:       cl.requests,
			LatencySumMs:   cl.latencySumMs,
			LatencyBuckets: cl.buckets,
		})
	}
	c.cells = map[key]*cell{}
	return out
}

// EstimateP95Ms estimates the 95th percentile latency from histogram
// buckets, linearly interpolating within the winning bucket.
func EstimateP95Ms(buckets []int64) int64 {
	var total int64
	for _, n := range buckets {
		total += n
	}
	if total == 0 {
		return 0
	}
	target := (total*95 + 99) / 100 // ceil(0.95 * total)
	var cum int64
	for i, n := range buckets {
		prev := cum
		cum += n
		if cum >= target {
			lo, hi := int64(0), BoundsMs[len(BoundsMs)-1]*2
			if i > 0 {
				lo = BoundsMs[i-1]
			}
			if i < len(BoundsMs) {
				hi = BoundsMs[i]
			}
			if n == 0 {
				return hi
			}
			// position of the target within this bucket
			frac := float64(target-prev) / float64(n)
			return lo + int64(frac*float64(hi-lo))
		}
	}
	return BoundsMs[len(BoundsMs)-1]
}
