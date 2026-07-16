package scheduler

import (
	"testing"
	"time"

	"helios/internal/api"
)

// node builds an alive node (fresh heartbeat) in a region with generous
// default capacity unless overridden.
func node(id, region string, cpu, mem int) api.Node {
	return api.Node{
		ID:            id,
		Addr:          id + ":9090",
		Region:        region,
		CPUMillis:     cpu,
		MemMB:         mem,
		LastHeartbeat: time.Now(),
	}
}

func app(name string, replicas, cpu, mem int) api.App {
	return api.App{Name: name, Image: "img", Port: 80, Replicas: replicas, CPUMillis: cpu, MemMB: mem, Host: name + ".local"}
}

// countByNode returns instanceCount per node for one app.
func countByNode(insts []api.Instance, appName string) map[string]int {
	out := map[string]int{}
	for _, i := range insts {
		if i.AppName == appName {
			out[i.NodeID]++
		}
	}
	return out
}

func TestPlace_SchedulesRequestedReplicas(t *testing.T) {
	s := New()
	nodes := []api.Node{node("a", "us", 1000, 1000), node("b", "us", 1000, 1000), node("c", "us", 1000, 1000)}
	apps := []api.App{app("web", 3, 100, 64)}

	got := s.Place(nodes, apps, nil, nil, nil)

	if len(got) != 3 {
		t.Fatalf("want 3 instances, got %d: %+v", len(got), got)
	}
	for _, inst := range got {
		if inst.Status != api.StatusPending {
			t.Errorf("new instance should be pending, got %s", inst.Status)
		}
	}
}

func TestPlace_AntiAffinitySpreadsReplicas(t *testing.T) {
	s := New()
	nodes := []api.Node{node("a", "us", 1000, 1000), node("b", "us", 1000, 1000), node("c", "us", 1000, 1000)}
	apps := []api.App{app("web", 3, 100, 64)}

	got := s.Place(nodes, apps, nil, nil, nil)

	per := countByNode(got, "web")
	if len(per) != 3 {
		t.Fatalf("anti-affinity: want replicas across 3 nodes, got %v", per)
	}
	for n, c := range per {
		if c != 1 {
			t.Errorf("node %s got %d replicas, want 1 (anti-affinity)", n, c)
		}
	}
}

func TestPlace_BinPackingRespectsCapacity(t *testing.T) {
	s := New()
	// Two nodes with room for exactly 2 replicas each (200 / 100).
	nodes := []api.Node{node("a", "us", 200, 10_000), node("b", "us", 200, 10_000)}
	apps := []api.App{app("web", 10, 100, 64)}

	got := s.Place(nodes, apps, nil, nil, nil)

	if len(got) != 4 {
		t.Fatalf("capacity-bound: want 4 placeable replicas (2 per node), got %d", len(got))
	}
	per := countByNode(got, "web")
	for n, c := range per {
		if c > 2 {
			t.Errorf("node %s over capacity: %d replicas", n, c)
		}
	}
}

func TestPlace_MemoryIsAlsoABindingConstraint(t *testing.T) {
	s := New()
	// Plenty of CPU, but memory only fits 1 replica per node.
	nodes := []api.Node{node("a", "us", 10_000, 64), node("b", "us", 10_000, 64)}
	apps := []api.App{app("web", 10, 100, 64)}

	got := s.Place(nodes, apps, nil, nil, nil)
	if len(got) != 2 {
		t.Fatalf("mem-bound: want 2 replicas, got %d", len(got))
	}
}

func TestPlace_PlacementStabilityKeepsExistingInstances(t *testing.T) {
	s := New()
	nodes := []api.Node{node("a", "us", 1000, 1000), node("b", "us", 1000, 1000)}
	apps := []api.App{app("web", 2, 100, 64)}

	first := s.Place(nodes, apps, nil, nil, nil)
	// Feed the first result back in as existing state.
	second := s.Place(nodes, apps, first, nil, nil)

	firstIDs := map[string]bool{}
	for _, i := range first {
		firstIDs[i.ID] = true
	}
	if len(second) != 2 {
		t.Fatalf("want 2 stable instances, got %d", len(second))
	}
	for _, i := range second {
		if !firstIDs[i.ID] {
			t.Errorf("placement not stable: new instance %s replaced an existing one", i.ID)
		}
	}
}

func TestPlace_DeadNodeReschedules(t *testing.T) {
	s := New()
	live := node("a", "us", 1000, 1000)
	dead := node("b", "us", 1000, 1000)
	dead.LastHeartbeat = time.Now().Add(-30 * time.Second) // stale => dead
	apps := []api.App{app("web", 2, 100, 64)}

	// Existing: one replica on each node.
	existing := []api.Instance{
		{ID: "keep", AppName: "web", NodeID: "a", Status: api.StatusHealthy},
		{ID: "lost", AppName: "web", NodeID: "b", Status: api.StatusHealthy},
	}

	got := s.Place([]api.Node{live, dead}, apps, existing, nil, nil)

	if len(got) != 2 {
		t.Fatalf("want 2 instances after failover, got %d: %+v", len(got), got)
	}
	var keptLost, keptAlive bool
	for _, i := range got {
		if i.NodeID == "b" {
			t.Errorf("instance placed on dead node b: %+v", i)
		}
		if i.ID == "lost" {
			keptLost = true
		}
		if i.ID == "keep" {
			keptAlive = true
		}
	}
	if keptLost {
		t.Error("instance on dead node should not be kept")
	}
	if !keptAlive {
		t.Error("instance on live node should be kept (stability)")
	}
}

func TestPlace_ScaleDownDropsExtraReplicas(t *testing.T) {
	s := New()
	nodes := []api.Node{node("a", "us", 1000, 1000), node("b", "us", 1000, 1000), node("c", "us", 1000, 1000)}
	existing := []api.Instance{
		{ID: "1", AppName: "web", NodeID: "a", Status: api.StatusHealthy},
		{ID: "2", AppName: "web", NodeID: "b", Status: api.StatusHealthy},
		{ID: "3", AppName: "web", NodeID: "c", Status: api.StatusHealthy},
	}
	apps := []api.App{app("web", 1, 100, 64)} // scaled down to 1

	got := s.Place(nodes, apps, existing, nil, nil)
	if len(got) != 1 {
		t.Fatalf("scale-down: want 1 instance, got %d", len(got))
	}
}

func TestPlace_DeletedAppIsRemoved(t *testing.T) {
	s := New()
	nodes := []api.Node{node("a", "us", 1000, 1000)}
	existing := []api.Instance{{ID: "1", AppName: "gone", NodeID: "a", Status: api.StatusHealthy}}

	got := s.Place(nodes, []api.App{}, existing, nil, nil)
	if len(got) != 0 {
		t.Fatalf("deleted app should leave no instances, got %d", len(got))
	}
}

func TestPlace_RegionPlanPlacesPerRegion(t *testing.T) {
	s := New()
	nodes := []api.Node{node("a", "us-east", 1000, 1000), node("b", "eu-west", 1000, 1000)}
	apps := []api.App{app("web", 1, 100, 64)} // Replicas ignored when a plan exists
	plans := map[string]map[string]int{"web": {"us-east": 1, "eu-west": 1}}

	got := s.Place(nodes, apps, nil, nil, plans)

	region := map[string]string{"a": "us-east", "b": "eu-west"}
	byRegion := map[string]int{}
	for _, i := range got {
		byRegion[region[i.NodeID]]++
	}
	if byRegion["us-east"] != 1 || byRegion["eu-west"] != 1 {
		t.Fatalf("region plan not honored: %v", byRegion)
	}
}

func TestPlace_RegionPlanZeroRemovesRegion(t *testing.T) {
	s := New()
	nodes := []api.Node{node("a", "us-east", 1000, 1000), node("b", "eu-west", 1000, 1000)}
	apps := []api.App{app("web", 1, 100, 64)}
	existing := []api.Instance{
		{ID: "east", AppName: "web", NodeID: "a", Status: api.StatusHealthy},
		{ID: "west", AppName: "web", NodeID: "b", Status: api.StatusHealthy},
	}
	// Follow-the-sun: eu-west drained to 0, all capacity in us-east.
	plans := map[string]map[string]int{"web": {"us-east": 1}}

	got := s.Place(nodes, apps, existing, nil, plans)
	for _, i := range got {
		if i.NodeID == "b" {
			t.Errorf("region planned to 0 still has an instance: %+v", i)
		}
	}
	if len(got) != 1 {
		t.Fatalf("want 1 instance (us-east only), got %d", len(got))
	}
}

func TestPlace_PinnedMigrationForcesTargetNode(t *testing.T) {
	s := New()
	nodes := []api.Node{node("a", "us", 1000, 1000), node("b", "us", 1000, 1000), node("c", "us", 1000, 1000)}
	apps := []api.App{app("web", 2, 100, 64)}
	existing := []api.Instance{
		{ID: "1", AppName: "web", NodeID: "a", Status: api.StatusHealthy},
		{ID: "2", AppName: "web", NodeID: "b", Status: api.StatusHealthy},
	}
	pinned := map[string]string{"web": "c"}

	got := s.Place(nodes, apps, existing, pinned, nil)
	if len(got) != 2 {
		t.Fatalf("want 2 instances on pinned node, got %d", len(got))
	}
	for _, i := range got {
		if i.NodeID != "c" {
			t.Errorf("migration should pin to node c, got %s", i.NodeID)
		}
	}
}

func TestPlace_IsDeterministic(t *testing.T) {
	s := New()
	nodes := []api.Node{node("a", "us", 1000, 1000), node("b", "us", 1000, 1000), node("c", "us", 1000, 1000)}
	apps := []api.App{app("web", 2, 100, 64)}

	a := countByNode(s.Place(nodes, apps, nil, nil, nil), "web")
	b := countByNode(s.Place(nodes, apps, nil, nil, nil), "web")
	if len(a) != len(b) {
		t.Fatalf("non-deterministic placement: %v vs %v", a, b)
	}
	for n := range a {
		if a[n] != b[n] {
			t.Fatalf("non-deterministic placement: %v vs %v", a, b)
		}
	}
}
