// trafficgen simulates global traffic against a Helios app: total load is
// split across regions whose demand follows evenly-offset sine "daylight"
// curves over an accelerated day. It stamps X-Client-Region on every
// request, which proxyd's telemetry records.
//
// This is both the step-2 demo driver and the load source for the step-5
// experiment (Helios vs. static placement).
//
//	trafficgen --host web.local --rps 50 --day 120s --duration 10m
package main

import (
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// latencies collects client-observed request latencies (successful
// responses only) so the experiment can compare p50/p95/p99 across
// placement strategies.
type latencies struct {
	mu sync.Mutex
	ms []float64
}

func (l *latencies) add(d time.Duration) {
	l.mu.Lock()
	l.ms = append(l.ms, float64(d.Microseconds())/1000.0)
	l.mu.Unlock()
}

func (l *latencies) percentile(p float64) float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.ms) == 0 {
		return 0
	}
	s := append([]float64(nil), l.ms...)
	sort.Float64s(s)
	idx := int(p / 100 * float64(len(s)-1))
	return s[idx]
}

func (l *latencies) mean() float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.ms) == 0 {
		return 0
	}
	var sum float64
	for _, v := range l.ms {
		sum += v
	}
	return sum / float64(len(l.ms))
}

func main() {
	proxyURL := flag.String("proxy", "http://127.0.0.1:8000", "proxyd base URL")
	host := flag.String("host", "web.local", "app hostname (Host header)")
	regionsCSV := flag.String("regions", "us-east,eu-west,ap-south", "comma-separated client regions")
	rps := flag.Int("rps", 50, "total requests per second at peak-sum")
	day := flag.Duration("day", 120*time.Second, "length of one simulated global day")
	duration := flag.Duration("duration", 10*time.Minute, "how long to run")
	concentration := flag.Float64("concentration", 1, "daylight peakiness: higher = demand concentrates in fewer regions at once")
	flag.Parse()

	regions := strings.Split(*regionsCSV, ",")
	client := &http.Client{Timeout: 5 * time.Second}
	start := time.Now()
	deadline := start.Add(*duration)

	var ok2xx, errs, localHits int64
	perRegion := make([]int64, len(regions))
	lat := &latencies{}

	// Fire requests on a fixed tick; weight region choice by each
	// region's current "daylight". Weight_i(t) = max(0, sin(2pi*(t/day + i/N)))
	// so demand peaks rotate around the globe.
	tick := time.NewTicker(time.Second / time.Duration(max(*rps, 1)))
	defer tick.Stop()

	statusEvery := time.NewTicker(2 * time.Second)
	defer statusEvery.Stop()

	fmt.Printf("trafficgen: %d rps across %v, simulated day = %v\n", *rps, regions, *day)
	for now := range tick.C {
		if now.After(deadline) {
			break
		}
		select {
		case <-statusEvery.C:
			printStatus(start, *day, regions, perRegion, &ok2xx, &errs)
		default:
		}

		i := pickRegion(time.Since(start), *day, len(regions), *concentration)
		atomic.AddInt64(&perRegion[i], 1)
		go func(region string) {
			req, err := http.NewRequest(http.MethodGet, *proxyURL+"/", nil)
			if err != nil {
				atomic.AddInt64(&errs, 1)
				return
			}
			req.Host = *host
			req.Header.Set("X-Client-Region", region)
			t0 := time.Now()
			resp, err := client.Do(req)
			if err != nil {
				atomic.AddInt64(&errs, 1)
				return
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode < 300 {
				atomic.AddInt64(&ok2xx, 1)
				lat.add(time.Since(t0))
				// loadapp echoes "region=<its region>"; a match means the
				// request was served locally (no cross-region hop).
				if strings.Contains(string(body), "region="+region) {
					atomic.AddInt64(&localHits, 1)
				}
			} else {
				atomic.AddInt64(&errs, 1)
			}
		}(regions[i])
	}
	// Let in-flight requests finish before reporting final latencies.
	time.Sleep(500 * time.Millisecond)
	printStatus(start, *day, regions, perRegion, &ok2xx, &errs)
	ok := atomic.LoadInt64(&ok2xx)
	localPct := 0.0
	if ok > 0 {
		localPct = 100 * float64(atomic.LoadInt64(&localHits)) / float64(ok)
	}
	fmt.Printf("LATENCY_MS mean=%.1f p50=%.1f p95=%.1f p99=%.1f local_pct=%.1f samples=%d\n",
		lat.mean(), lat.percentile(50), lat.percentile(95), lat.percentile(99), localPct, len(lat.ms))
	fmt.Println("done")
}

// pickRegion samples a region index weighted by rotating daylight curves.
// concentration sharpens the peaks (weight = sin^concentration), so higher
// values put more of the load in the single currently-sunny region.
func pickRegion(elapsed, day time.Duration, n int, concentration float64) int {
	t := elapsed.Seconds() / day.Seconds()
	weights := make([]float64, n)
	var total float64
	for i := range weights {
		phase := 2 * math.Pi * (t + float64(i)/float64(n))
		w := math.Sin(phase)
		if w < 0 {
			w = 0
		}
		w = math.Pow(w, concentration)
		weights[i] = w
		total += w
	}
	if total == 0 {
		return rand.Intn(n)
	}
	r := rand.Float64() * total
	for i, w := range weights {
		if r < w {
			return i
		}
		r -= w
	}
	return n - 1
}

func printStatus(start time.Time, day time.Duration, regions []string, perRegion []int64, ok, errs *int64) {
	t := time.Since(start).Seconds() / day.Seconds()
	simHour := int(math.Mod(t, 1) * 24)
	var parts []string
	for i, r := range regions {
		parts = append(parts, fmt.Sprintf("%s=%d", r, atomic.LoadInt64(&perRegion[i])))
	}
	fmt.Printf("[sim %02d:00] sent: %s | 2xx=%d errs=%d\n",
		simHour, strings.Join(parts, " "), atomic.LoadInt64(ok), atomic.LoadInt64(errs))
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
