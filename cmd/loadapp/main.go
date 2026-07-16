// loadapp is a latency-bearing workload for the Helios experiment harness.
//
// A real app's latency depends on where it runs relative to its users and
// on how much load each replica carries. loadapp models both:
//
//   - Locality: every request carries an X-Client-Region header (stamped by
//     trafficgen). If it doesn't match this replica's own region — injected
//     by noded as HELIOS_REGION — the request pays a cross-region RTT
//     penalty. Serving a region's traffic from replicas in that region is
//     therefore faster, which is the whole premise of follow-the-sun.
//   - Concurrency: each replica has a finite number of service slots
//     (CAPACITY). Requests beyond that queue, so an under-provisioned region
//     also pays in latency.
//
// It is deliberately dependency-free (stdlib only) so it builds into a tiny
// container. Tunables come from the environment so the same image serves
// every region:
//
//	PORT (8080)  BASE_MS (25)  RTT_MS (75)  CAPACITY (16)  HELIOS_REGION
package main

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	port := envStr("PORT", "8080")
	region := os.Getenv("HELIOS_REGION")
	baseMs := envInt("BASE_MS", 25)
	rttMs := envInt("RTT_MS", 75)
	capacity := envInt("CAPACITY", 16)

	// A buffered channel of `capacity` slots models finite concurrency:
	// once full, further requests block here until a slot frees — their
	// wait time is the queueing latency an overloaded replica imposes.
	slots := make(chan struct{}, capacity)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		slots <- struct{}{}
		defer func() { <-slots }()

		d := time.Duration(baseMs) * time.Millisecond
		if cr := r.Header.Get("X-Client-Region"); cr != "" && region != "" && cr != region {
			d += time.Duration(rttMs) * time.Millisecond // cross-region hop
		}
		time.Sleep(d)
		fmt.Fprintf(w, "served by region=%s\n", region)
	})

	fmt.Printf("loadapp region=%q base=%dms rtt=%dms capacity=%d listening on :%s\n",
		region, baseMs, rttMs, capacity, port)
	_ = http.ListenAndServe(":"+port, mux)
}

func envStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
