// Package store holds cluster state behind an interface so the backing
// implementation can evolve: in-memory (M1) -> sqlite -> replicated log (M2+)
// without touching scheduler/reconciler logic.
package store

import (
	"sync"
	"time"

	"helios/internal/api"
)

// Store is the single source of truth for desired and actual state.
type Store interface {
	// Nodes
	UpsertNode(n api.Node)
	Nodes() []api.Node
	TouchNode(id string, t time.Time)

	// Apps (desired state)
	UpsertApp(a api.App)
	Apps() []api.App
	GetApp(name string) (api.App, bool)

	// Desired instances (what should be running)
	SetDesired(instances []api.Instance)
	Desired() []api.Instance

	// Actual instances (what heartbeats report)
	SetActualForNode(nodeID string, instances []api.Instance)
	Actual() []api.Instance

	// Migration intents consumed by the reconciler
	AddMigration(m api.MigrateRequest)
	PopMigrations() []api.MigrateRequest

	// Region plans written by the predictive placer: app -> region ->
	// desired replica count. When a plan exists for an app it overrides
	// App.Replicas; the scheduler places per-region.
	SetRegionPlan(app string, plan map[string]int)
	RegionPlans() map[string]map[string]int
}

// Memory is a mutex-guarded in-memory Store. Deliberately boring:
// correctness first, persistence later.
type Memory struct {
	mu          sync.RWMutex
	nodes       map[string]api.Node
	apps        map[string]api.App
	desired     []api.Instance
	actual      map[string][]api.Instance // nodeID -> instances
	migrations  []api.MigrateRequest
	regionPlans map[string]map[string]int // app -> region -> replicas
}

func NewMemory() *Memory {
	return &Memory{
		nodes:       map[string]api.Node{},
		apps:        map[string]api.App{},
		actual:      map[string][]api.Instance{},
		regionPlans: map[string]map[string]int{},
	}
}

func (m *Memory) SetRegionPlan(app string, plan map[string]int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make(map[string]int, len(plan))
	for r, n := range plan {
		cp[r] = n
	}
	m.regionPlans[app] = cp
}

func (m *Memory) RegionPlans() map[string]map[string]int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[string]map[string]int, len(m.regionPlans))
	for app, plan := range m.regionPlans {
		cp := make(map[string]int, len(plan))
		for r, n := range plan {
			cp[r] = n
		}
		out[app] = cp
	}
	return out
}

func (m *Memory) UpsertNode(n api.Node) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodes[n.ID] = n
}

func (m *Memory) Nodes() []api.Node {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]api.Node, 0, len(m.nodes))
	for _, n := range m.nodes {
		out = append(out, n)
	}
	return out
}

func (m *Memory) TouchNode(id string, t time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if n, ok := m.nodes[id]; ok {
		n.LastHeartbeat = t
		m.nodes[id] = n
	}
}

func (m *Memory) UpsertApp(a api.App) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.apps[a.Name] = a
}

func (m *Memory) Apps() []api.App {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]api.App, 0, len(m.apps))
	for _, a := range m.apps {
		out = append(out, a)
	}
	return out
}

func (m *Memory) GetApp(name string) (api.App, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	a, ok := m.apps[name]
	return a, ok
}

func (m *Memory) SetDesired(instances []api.Instance) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.desired = append([]api.Instance(nil), instances...)
}

func (m *Memory) Desired() []api.Instance {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]api.Instance(nil), m.desired...)
}

func (m *Memory) SetActualForNode(nodeID string, instances []api.Instance) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.actual[nodeID] = append([]api.Instance(nil), instances...)
}

func (m *Memory) Actual() []api.Instance {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []api.Instance
	for _, list := range m.actual {
		out = append(out, list...)
	}
	return out
}

func (m *Memory) AddMigration(req api.MigrateRequest) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.migrations = append(m.migrations, req)
}

func (m *Memory) PopMigrations() []api.MigrateRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := m.migrations
	m.migrations = nil
	return out
}
