// proxyd is the Helios edge: a from-scratch L7 reverse proxy that routes
// by Host header, load-balances, drains connections during migration, and
// measures traffic telemetry (per-app request geography and latency).
package main

import (
	"flag"
	"log"
	"net/http"
	"time"

	"helios/internal/proxy"
)

func main() {
	listen := flag.String("listen", ":8000", "proxy listen address")
	control := flag.String("control", "http://127.0.0.1:8080", "controld base URL")
	id := flag.String("id", "proxy-1", "proxy instance ID (telemetry attribution)")
	syncInterval := flag.Duration("sync-interval", time.Second, "routing table sync interval")
	reportInterval := flag.Duration("report-interval", 5*time.Second, "telemetry report interval")
	flag.Parse()

	p := proxy.New(*control, *id)
	go p.SyncLoop(*syncInterval)
	go p.ReportLoop(*reportInterval)

	log.Printf("proxyd %s listening on %s (control plane %s)", *id, *listen, *control)
	log.Fatal(http.ListenAndServe(*listen, p))
}
