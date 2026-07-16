// Package api defines the shared domain types exchanged between
// controld, noded, proxyd and the CLI. Keeping them in one place is the
// contract that lets each component evolve independently.
package api

import "time"

// Node is a machine (VM/container) that can run app instances.
type Node struct {
	ID            string    `json:"id"`
	Addr          string    `json:"addr"` // host:port of the noded agent
	Region        string    `json:"region"`
	CPUMillis     int       `json:"cpu_millis"` // total capacity
	MemMB         int       `json:"mem_mb"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
}

// App is a deployable unit. Milestone 1 deploys prebuilt container
// images; the source->image build pipeline replaces Image later.
type App struct {
	Name      string `json:"name"`
	Image     string `json:"image"`
	Port      int    `json:"port"` // port the app listens on inside the container
	Replicas  int    `json:"replicas"`
	CPUMillis int    `json:"cpu_millis"` // per-replica reservation
	MemMB     int    `json:"mem_mb"`
	Host      string `json:"host"` // hostname proxyd routes on, e.g. myapp.local

	// AutoPlace opts the app into the predictive placer: replica counts
	// and regions are decided by forecasts, not by Replicas (which then
	// only seeds the initial deployment until the first plan is written).
	AutoPlace     bool `json:"auto_place"`
	RPSPerReplica int  `json:"rps_per_replica"` // capacity model: one replica serves this many RPS
	MaxReplicas   int  `json:"max_replicas"`    // global cap for the placer
}

// InstanceStatus is the lifecycle of a single running copy of an app.
type InstanceStatus string

const (
	StatusPending  InstanceStatus = "pending"  // assigned, not yet started
	StatusStarting InstanceStatus = "starting" // container launched, not yet healthy
	StatusHealthy  InstanceStatus = "healthy"
	StatusDraining InstanceStatus = "draining" // being removed; proxy stops sending new requests
	StatusDead     InstanceStatus = "dead"
)

// Instance is one desired or actual running copy of an App on a Node.
type Instance struct {
	ID          string         `json:"id"`
	AppName     string         `json:"app_name"`
	NodeID      string         `json:"node_id"`
	ContainerID string         `json:"container_id,omitempty"`
	HostPort    int            `json:"host_port,omitempty"` // port published on the node
	Status      InstanceStatus `json:"status"`
}

// Assignment is what controld tells a node to converge to.
type Assignment struct {
	Instances []Instance `json:"instances"`
}

// Heartbeat is what noded reports back: liveness + actual instance state.
type Heartbeat struct {
	NodeID    string     `json:"node_id"`
	Instances []Instance `json:"instances"` // actual, as observed via docker
}

// Backend is one routable instance: its address plus the region it runs
// in, so proxyd can prefer serving a client from its own region.
type Backend struct {
	Addr   string `json:"addr"` // "nodeAddrHost:hostPort"
	Region string `json:"region"`
}

// Route is one hostname -> backends mapping consumed by proxyd.
type Route struct {
	Host     string    `json:"host"`
	Backends []Backend `json:"backends"`
}

// MigrateRequest asks controld to move an app's instance(s) to a target node
// with make-before-break semantics (start new, drain old, then stop old).
type MigrateRequest struct {
	AppName    string `json:"app_name"`
	TargetNode string `json:"target_node"`
}

// TelemetryEntry is aggregated traffic for one (app, client region) pair
// over one report window. App holds the routed hostname at the proxy;
// controld resolves it to an app name on ingest. LatencyBuckets follows
// telemetry.BoundsMs with a final +Inf overflow bucket.
type TelemetryEntry struct {
	App            string  `json:"app"`
	Region         string  `json:"region"`
	Requests       int64   `json:"requests"`
	LatencySumMs   int64   `json:"latency_sum_ms"`
	LatencyBuckets []int64 `json:"latency_buckets"`
}

// TelemetryReport is one proxy's delta since its previous report.
type TelemetryReport struct {
	ProxyID string           `json:"proxy_id"`
	Start   time.Time        `json:"start"`
	End     time.Time        `json:"end"`
	Entries []TelemetryEntry `json:"entries"`
}
