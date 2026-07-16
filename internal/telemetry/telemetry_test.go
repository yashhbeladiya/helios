package telemetry

import "testing"

func TestCollector_RecordAndFlushDelta(t *testing.T) {
	c := NewCollector()
	c.Record("web", "us-east", 5)
	c.Record("web", "us-east", 15)
	c.Record("web", "eu-west", 3)

	entries := c.Flush()
	if len(entries) != 2 {
		t.Fatalf("want 2 (app,region) cells, got %d", len(entries))
	}
	byRegion := map[string]TelemetrySummary{}
	for _, e := range entries {
		byRegion[e.Region] = TelemetrySummary{e.Requests, e.LatencySumMs}
	}
	if got := byRegion["us-east"]; got.reqs != 2 || got.sum != 20 {
		t.Errorf("us-east = %+v, want reqs=2 sum=20", got)
	}
	if got := byRegion["eu-west"]; got.reqs != 1 || got.sum != 3 {
		t.Errorf("eu-west = %+v, want reqs=1 sum=3", got)
	}
}

type TelemetrySummary struct{ reqs, sum int64 }

func TestCollector_FlushResets(t *testing.T) {
	c := NewCollector()
	c.Record("web", "us-east", 5)
	if len(c.Flush()) != 1 {
		t.Fatal("first flush should return the recorded cell")
	}
	if got := c.Flush(); len(got) != 0 {
		t.Fatalf("second flush should be empty (delta semantics), got %d", len(got))
	}
}

func TestBucketFor(t *testing.T) {
	cases := []struct {
		ms   int64
		want int
	}{
		{0, 0},                 // <= 1
		{1, 0},                 // <= 1
		{2, 1},                 // <= 2
		{3, 2},                 // <= 5
		{50, 5},                // <= 50
		{51, 6},                // <= 100
		{99999, len(BoundsMs)}, // +Inf overflow
	}
	for _, c := range cases {
		if got := bucketFor(c.ms); got != c.want {
			t.Errorf("bucketFor(%d) = %d, want %d", c.ms, got, c.want)
		}
	}
}

func TestEstimateP95_Empty(t *testing.T) {
	if got := EstimateP95Ms(make([]int64, NumBuckets)); got != 0 {
		t.Errorf("empty histogram p95 = %d, want 0", got)
	}
}

func TestEstimateP95_ConcentratedBucket(t *testing.T) {
	// 100 requests all landing in the 25..50ms bucket (bound index 5 => 50).
	buckets := make([]int64, NumBuckets)
	buckets[5] = 100
	got := EstimateP95Ms(buckets)
	if got < BoundsMs[4] || got > BoundsMs[5] { // 25..50
		t.Errorf("p95 = %d, want within [%d,%d]", got, BoundsMs[4], BoundsMs[5])
	}
}

func TestEstimateP95_IgnoresTailBelowThreshold(t *testing.T) {
	// 95 fast (<=1ms) + 5 slow (in the ~1000ms bucket). p95 should sit in
	// the fast bucket, not be dragged up by the 5% tail.
	buckets := make([]int64, NumBuckets)
	buckets[0] = 95
	buckets[9] = 5 // bound 1000
	got := EstimateP95Ms(buckets)
	if got > BoundsMs[0] {
		t.Errorf("p95 = %d, want <= %d (tail must not dominate)", got, BoundsMs[0])
	}
}
