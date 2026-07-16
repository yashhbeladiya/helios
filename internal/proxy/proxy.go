// Package proxy implements proxyd's core: an L7 reverse proxy that
// syncs its routing table from controld, round-robins across backends,
// and drains connections gracefully — a removed backend finishes its
// in-flight requests but receives no new ones. That draining behavior
// is what makes zero-downtime migration real.
//
// Milestone 2: this is also the traffic sensor. Every proxied request is
// recorded (app host, client region, latency) into a telemetry.Collector,
// flushed as a delta report to controld on an interval.
package proxy

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"helios/internal/api"
	"helios/internal/telemetry"
)

type backend struct {
	addr     string
	region   string
	inflight int64
	proxy    *httputil.ReverseProxy
}

type Proxy struct {
	ControlAddr string
	ID          string

	mu        sync.RWMutex
	routes    map[string][]*backend // host -> backends
	rr        map[string]*uint64    // host -> round-robin counter
	client    *http.Client
	collector *telemetry.Collector
	lastFlush time.Time
}

func New(controlAddr, id string) *Proxy {
	return &Proxy{
		ControlAddr: controlAddr,
		ID:          id,
		routes:      map[string][]*backend{},
		rr:          map[string]*uint64{},
		client:      &http.Client{Timeout: 5 * time.Second},
		collector:   telemetry.NewCollector(),
		lastFlush:   time.Now(),
	}
}

// ReportLoop flushes telemetry deltas to controld on an interval.
// Failures are logged, not fatal: telemetry is best-effort and the
// counters simply accumulate into the next report window.
func (p *Proxy) ReportLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		entries := p.collector.Flush()
		now := time.Now()
		report := api.TelemetryReport{
			ProxyID: p.ID,
			Start:   p.lastFlush,
			End:     now,
			Entries: entries,
		}
		p.lastFlush = now
		if len(entries) == 0 {
			continue
		}
		body, err := json.Marshal(report)
		if err != nil {
			log.Printf("telemetry marshal: %v", err)
			continue
		}
		resp, err := p.client.Post(p.ControlAddr+"/v1/telemetry", "application/json", bytes.NewReader(body))
		if err != nil {
			log.Printf("telemetry report: %v", err)
			continue
		}
		resp.Body.Close()
	}
}

// resolveRegion determines the client's region for telemetry.
//
// Simulation/experiment mode: the traffic generator stamps
// X-Client-Region. Production mode would GeoIP r.RemoteAddr here
// (e.g. a MaxMind lookup) — this function is the seam.
func resolveRegion(r *http.Request) string {
	if region := r.Header.Get("X-Client-Region"); region != "" {
		return region
	}
	return "unknown"
}

// SyncLoop refreshes the routing table from controld.
func (p *Proxy) SyncLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		if err := p.sync(); err != nil {
			log.Printf("route sync: %v", err)
		}
	}
}

func (p *Proxy) sync() error {
	resp, err := p.client.Get(p.ControlAddr + "/v1/routes")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var routes []api.Route
	if err := json.NewDecoder(resp.Body).Decode(&routes); err != nil {
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	next := map[string][]*backend{}
	for _, r := range routes {
		for _, be := range r.Backends {
			// Reuse existing backend structs so in-flight counters
			// survive table refreshes (that's the draining state).
			if b := findBackend(p.routes[r.Host], be.Addr); b != nil {
				b.region = be.Region
				next[r.Host] = append(next[r.Host], b)
				continue
			}
			u, err := url.Parse("http://" + be.Addr)
			if err != nil {
				continue
			}
			next[r.Host] = append(next[r.Host], &backend{
				addr:   be.Addr,
				region: be.Region,
				proxy:  httputil.NewSingleHostReverseProxy(u),
			})
		}
		if _, ok := p.rr[r.Host]; !ok {
			p.rr[r.Host] = new(uint64)
		}
	}
	// Backends absent from `next` are implicitly drained: they get no
	// new requests, and in-flight ones complete on their own goroutines.
	p.routes = next
	return nil
}

func findBackend(list []*backend, addr string) *backend {
	for _, b := range list {
		if b.addr == addr {
			return b
		}
	}
	return nil
}

// ServeHTTP routes by Host header, preferring a backend in the client's
// own region (falling back to all backends when the region has none),
// round-robins within that set, and records telemetry for every request.
// Region preference is what lets the placer's geography translate into
// lower latency — serving a region's traffic locally.
func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	host := stripPort(r.Host)
	region := resolveRegion(r)

	p.mu.RLock()
	all := p.routes[host]
	counter := p.rr[host]
	p.mu.RUnlock()

	if len(all) == 0 {
		http.Error(w, "no backends for "+host, http.StatusBadGateway)
		return
	}

	// Prefer same-region backends; fall back to the full set.
	candidates := all[:0:0]
	for _, b := range all {
		if b.region == region {
			candidates = append(candidates, b)
		}
	}
	if len(candidates) == 0 {
		candidates = all
	}

	i := atomic.AddUint64(counter, 1)
	b := candidates[i%uint64(len(candidates))]

	atomic.AddInt64(&b.inflight, 1)
	start := time.Now()
	b.proxy.ServeHTTP(w, r)
	p.collector.Record(host, region, time.Since(start).Milliseconds())
	atomic.AddInt64(&b.inflight, -1)
}

func stripPort(host string) string {
	for i := len(host) - 1; i >= 0; i-- {
		if host[i] == ':' {
			return host[:i]
		}
		if host[i] == ']' { // IPv6 literal
			break
		}
	}
	return host
}
