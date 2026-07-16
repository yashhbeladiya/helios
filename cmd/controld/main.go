// controld is the Helios control plane: API server + reconciler loop.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"helios/internal/api"
	"helios/internal/place"
	"helios/internal/predict"
	"helios/internal/reconciler"
	"helios/internal/store"
	"helios/internal/tsdb"
)

func main() {
	listen := flag.String("listen", ":8080", "API listen address")
	interval := flag.Duration("reconcile-interval", 2*time.Second, "reconcile loop interval")
	flag.Parse()

	st := store.NewMemory()
	rec := reconciler.New(st)
	db := tsdb.New()
	forecaster := predict.NewEngine(db)
	placer := place.NewEngine(st, forecaster)
	go rec.Run(*interval)
	go forecaster.Run(tsdb.Resolution)
	go placer.Run(tsdb.Resolution)

	mux := http.NewServeMux()

	// -- node endpoints (used by noded) --
	mux.HandleFunc("POST /v1/nodes/register", func(w http.ResponseWriter, r *http.Request) {
		var n api.Node
		if !decode(w, r, &n) {
			return
		}
		n.LastHeartbeat = time.Now()
		st.UpsertNode(n)
		log.Printf("node registered: %s (%s, region=%s)", n.ID, n.Addr, n.Region)
		w.WriteHeader(http.StatusNoContent)
	})

	mux.HandleFunc("POST /v1/nodes/heartbeat", func(w http.ResponseWriter, r *http.Request) {
		var hb api.Heartbeat
		if !decode(w, r, &hb) {
			return
		}
		st.TouchNode(hb.NodeID, time.Now())
		st.SetActualForNode(hb.NodeID, hb.Instances)
		w.WriteHeader(http.StatusNoContent)
	})

	mux.HandleFunc("GET /v1/nodes/{id}/assignments", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, rec.AssignmentsFor(r.PathValue("id")))
	})

	mux.HandleFunc("GET /v1/nodes", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, st.Nodes())
	})

	// -- app endpoints (used by CLI) --
	mux.HandleFunc("POST /v1/apps", func(w http.ResponseWriter, r *http.Request) {
		var a api.App
		if !decode(w, r, &a) {
			return
		}
		if a.Replicas <= 0 {
			a.Replicas = 1
		}
		if a.CPUMillis <= 0 {
			a.CPUMillis = 100
		}
		if a.MemMB <= 0 {
			a.MemMB = 64
		}
		if a.Host == "" {
			a.Host = a.Name + ".local"
		}
		if a.AutoPlace {
			if a.RPSPerReplica <= 0 {
				a.RPSPerReplica = 50
			}
			if a.MaxReplicas <= 0 {
				a.MaxReplicas = 10
			}
		}
		st.UpsertApp(a)
		log.Printf("app deployed: %s image=%s replicas=%d host=%s autoplace=%v", a.Name, a.Image, a.Replicas, a.Host, a.AutoPlace)
		w.WriteHeader(http.StatusCreated)
	})

	mux.HandleFunc("GET /v1/apps/{name}", func(w http.ResponseWriter, r *http.Request) {
		app, ok := st.GetApp(r.PathValue("name"))
		if !ok {
			http.NotFound(w, r)
			return
		}
		writeJSON(w, app)
	})

	mux.HandleFunc("GET /v1/apps", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, st.Apps())
	})

	mux.HandleFunc("POST /v1/migrate", func(w http.ResponseWriter, r *http.Request) {
		var m api.MigrateRequest
		if !decode(w, r, &m) {
			return
		}
		if _, ok := st.GetApp(m.AppName); !ok {
			http.Error(w, "unknown app "+m.AppName, http.StatusNotFound)
			return
		}
		st.AddMigration(m)
		w.WriteHeader(http.StatusAccepted)
	})

	// -- telemetry (M2 step 2): proxies report, predictor/dashboard query --
	mux.HandleFunc("POST /v1/telemetry", func(w http.ResponseWriter, r *http.Request) {
		var report api.TelemetryReport
		if !decode(w, r, &report) {
			return
		}
		db.Ingest(report, func(host string) (string, bool) {
			for _, a := range st.Apps() {
				if a.Host == host {
					return a.Name, true
				}
			}
			return "", false
		})
		w.WriteHeader(http.StatusNoContent)
	})

	// GET /v1/telemetry?app=web&minutes=10 -> region -> ordered points
	mux.HandleFunc("GET /v1/telemetry", func(w http.ResponseWriter, r *http.Request) {
		app := r.URL.Query().Get("app")
		if app == "" {
			writeJSON(w, db.Apps())
			return
		}
		minutes := 10
		if m := r.URL.Query().Get("minutes"); m != "" {
			if _, err := fmt.Sscanf(m, "%d", &minutes); err != nil || minutes <= 0 {
				http.Error(w, "bad minutes", http.StatusBadRequest)
				return
			}
		}
		writeJSON(w, db.Query(app, time.Now().Add(-time.Duration(minutes)*time.Minute)))
	})

	// GET /v1/forecast[?app=web] -> latest per-(app,region) forecasts with
	// per-algorithm running MAE. This is what the placer (step 4) reads.
	mux.HandleFunc("GET /v1/forecast", func(w http.ResponseWriter, r *http.Request) {
		all := forecaster.Forecasts()
		if app := r.URL.Query().Get("app"); app != "" {
			filtered := all[:0]
			for _, f := range all {
				if f.App == app {
					filtered = append(filtered, f)
				}
			}
			all = filtered
		}
		writeJSON(w, all)
	})

	// GET /v1/plan -> current region plans (app -> region -> replicas)
	mux.HandleFunc("GET /v1/plan", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, st.RegionPlans())
	})

	// GET /v1/decisions -> recent placer decisions with reasons, newest first
	mux.HandleFunc("GET /v1/decisions", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, placer.Decisions())
	})

	// -- state endpoints (used by proxyd and CLI status) --
	mux.HandleFunc("GET /v1/routes", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, rec.Routes())
	})

	mux.HandleFunc("GET /v1/instances", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, st.Actual())
	})

	log.Printf("controld listening on %s", *listen)
	log.Fatal(http.ListenAndServe(*listen, logRequests(mux)))
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("encode response: %v", err)
	}
}

// logRequests logs mutations, skips the chatty polling endpoints.
func logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && !strings.HasSuffix(r.URL.Path, "/heartbeat") {
			log.Printf("%s %s", r.Method, r.URL.Path)
		}
		next.ServeHTTP(w, r)
	})
}
