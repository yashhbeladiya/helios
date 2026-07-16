package reconciler

import (
	"testing"
	"time"

	"helios/internal/api"
	"helios/internal/store"
)

func aliveNode(st *store.Memory, id, addr, region string) {
	st.UpsertNode(api.Node{ID: id, Addr: addr, Region: region, CPUMillis: 1000, MemMB: 1000, LastHeartbeat: time.Now()})
}

func TestStep_ComputesDesiredFromApps(t *testing.T) {
	st := store.NewMemory()
	aliveNode(st, "a", "127.0.0.1:9091", "us")
	aliveNode(st, "b", "127.0.0.1:9092", "us")
	st.UpsertApp(api.App{Name: "web", Image: "img", Port: 80, Replicas: 2, CPUMillis: 100, MemMB: 64, Host: "web.local"})

	r := New(st)
	r.step()

	if got := len(st.Desired()); got != 2 {
		t.Fatalf("want 2 desired instances, got %d", got)
	}
}

// The core behavior worth proving: make-before-break keeps the old copy
// serving (as draining) until the target node is healthy, and the
// migration retires only once the target holds all healthy replicas.
func TestMigration_MakeBeforeBreakLifecycle(t *testing.T) {
	st := store.NewMemory()
	aliveNode(st, "a", "127.0.0.1:9091", "us")
	aliveNode(st, "b", "127.0.0.1:9092", "us")
	st.UpsertApp(api.App{Name: "web", Image: "img", Port: 80, Replicas: 1, CPUMillis: 100, MemMB: 64, Host: "web.local"})

	r := New(st)

	// Current reality: one healthy instance on node a.
	st.SetActualForNode("a", []api.Instance{
		{ID: "old", AppName: "web", NodeID: "a", Status: api.StatusHealthy, HostPort: 30000},
	})

	// Request migration a -> b and let the reconciler absorb it.
	st.AddMigration(api.MigrateRequest{AppName: "web", TargetNode: "b"})
	r.step()

	if _, ok := r.activeMigrations["web"]; !ok {
		t.Fatal("migration should be active after step")
	}

	// While the target is not yet healthy, node a must still serve the old
	// instance, marked draining (this is what makes migration hitless).
	asgA := r.AssignmentsFor("a")
	var draining *api.Instance
	for i := range asgA.Instances {
		if asgA.Instances[i].ID == "old" {
			draining = &asgA.Instances[i]
		}
	}
	if draining == nil {
		t.Fatal("old instance should still be assigned to node a during migration")
	}
	if draining.Status != api.StatusDraining {
		t.Errorf("old instance should be draining, got %s", draining.Status)
	}

	// Node b should have been assigned the new (pending) desired instance.
	asgB := r.AssignmentsFor("b")
	if len(asgB.Instances) == 0 {
		t.Fatal("target node b should receive the new instance")
	}

	// Target becomes healthy; old copy gone.
	st.SetActualForNode("a", nil)
	st.SetActualForNode("b", []api.Instance{
		{ID: "new", AppName: "web", NodeID: "b", Status: api.StatusHealthy, HostPort: 30001},
	})
	r.step()

	if _, ok := r.activeMigrations["web"]; ok {
		t.Error("migration should retire once target holds all healthy replicas")
	}

	// After completion, node a is no longer told to keep a draining copy.
	if got := len(r.AssignmentsFor("a").Instances); got != 0 {
		t.Errorf("node a should hold no instances post-migration, got %d", got)
	}
}

func TestMigration_DoesNotRetireWhileOldStillHealthy(t *testing.T) {
	st := store.NewMemory()
	aliveNode(st, "a", "127.0.0.1:9091", "us")
	aliveNode(st, "b", "127.0.0.1:9092", "us")
	st.UpsertApp(api.App{Name: "web", Image: "img", Port: 80, Replicas: 1, CPUMillis: 100, MemMB: 64, Host: "web.local"})

	r := New(st)
	st.AddMigration(api.MigrateRequest{AppName: "web", TargetNode: "b"})
	r.step()

	// Both old (a) and new (b) healthy simultaneously => not done yet
	// (healthyElsewhere > 0), so the old copy must not be torn down.
	st.SetActualForNode("a", []api.Instance{{ID: "old", AppName: "web", NodeID: "a", Status: api.StatusHealthy, HostPort: 30000}})
	st.SetActualForNode("b", []api.Instance{{ID: "new", AppName: "web", NodeID: "b", Status: api.StatusHealthy, HostPort: 30001}})
	r.step()

	if _, ok := r.activeMigrations["web"]; !ok {
		t.Error("migration must stay active while a healthy copy remains off-target")
	}
}

func TestRoutes_BuildsBackendsFromHealthyActual(t *testing.T) {
	st := store.NewMemory()
	aliveNode(st, "a", "127.0.0.1:9091", "us")
	st.UpsertApp(api.App{Name: "web", Image: "img", Port: 80, Replicas: 1, Host: "web.local"})
	st.SetActualForNode("a", []api.Instance{
		{ID: "1", AppName: "web", NodeID: "a", Status: api.StatusHealthy, HostPort: 32768},
	})

	r := New(st)
	routes := r.Routes()

	if len(routes) != 1 {
		t.Fatalf("want 1 route, got %d", len(routes))
	}
	if routes[0].Host != "web.local" {
		t.Errorf("route host = %q, want web.local", routes[0].Host)
	}
	if len(routes[0].Backends) != 1 || routes[0].Backends[0].Addr != "127.0.0.1:32768" {
		t.Errorf("backends = %v, want [127.0.0.1:32768]", routes[0].Backends)
	}
	if routes[0].Backends[0].Region != "us" {
		t.Errorf("backend region = %q, want us", routes[0].Backends[0].Region)
	}
}

func TestRoutes_DrainingInstancesAreRemovedFromRotation(t *testing.T) {
	// Draining means "take no new requests"; the proxy finishes in-flight
	// requests to it while the node keeps the container alive for its drain
	// grace. So a draining instance must NOT appear in the routing table.
	st := store.NewMemory()
	aliveNode(st, "a", "127.0.0.1:9091", "us")
	aliveNode(st, "b", "127.0.0.1:9092", "us")
	st.UpsertApp(api.App{Name: "web", Image: "img", Port: 80, Replicas: 1, Host: "web.local"})
	st.SetActualForNode("a", []api.Instance{
		{ID: "old", AppName: "web", NodeID: "a", Status: api.StatusDraining, HostPort: 32768},
	})
	st.SetActualForNode("b", []api.Instance{
		{ID: "new", AppName: "web", NodeID: "b", Status: api.StatusHealthy, HostPort: 32769},
	})

	r := New(st)
	routes := r.Routes()
	if len(routes) != 1 || len(routes[0].Backends) != 1 {
		t.Fatalf("only the healthy instance should route, got %v", routes)
	}
	if routes[0].Backends[0].Addr != "127.0.0.1:32769" {
		t.Errorf("draining backend must be excluded; got %v", routes[0].Backends)
	}
}

func TestRoutes_ExcludesInstancesOnDeadNodes(t *testing.T) {
	// A node's actual instances linger in the store after it dies; routing
	// to them would 502 because its containers are gone. Routes must drop
	// backends on nodes past the heartbeat window.
	st := store.NewMemory()
	aliveNode(st, "a", "127.0.0.1:9091", "us")
	// node b registered but heartbeat is stale => dead.
	st.UpsertNode(api.Node{ID: "b", Addr: "127.0.0.1:9092", Region: "us", CPUMillis: 1000, MemMB: 1000, LastHeartbeat: time.Now().Add(-30 * time.Second)})
	st.UpsertApp(api.App{Name: "web", Image: "img", Port: 80, Replicas: 2, Host: "web.local"})
	st.SetActualForNode("a", []api.Instance{{ID: "live", AppName: "web", NodeID: "a", Status: api.StatusHealthy, HostPort: 32768}})
	st.SetActualForNode("b", []api.Instance{{ID: "ghost", AppName: "web", NodeID: "b", Status: api.StatusHealthy, HostPort: 32769}})

	r := New(st)
	routes := r.Routes()
	if len(routes) != 1 || len(routes[0].Backends) != 1 {
		t.Fatalf("dead node's backend must be excluded, got %v", routes)
	}
	if routes[0].Backends[0].Addr != "127.0.0.1:32768" {
		t.Errorf("only live backend should route, got %v", routes[0].Backends)
	}
}

func TestRoutes_ExcludesUnhealthyAndPortless(t *testing.T) {
	st := store.NewMemory()
	aliveNode(st, "a", "127.0.0.1:9091", "us")
	st.UpsertApp(api.App{Name: "web", Image: "img", Port: 80, Replicas: 2, Host: "web.local"})
	st.SetActualForNode("a", []api.Instance{
		{ID: "1", AppName: "web", NodeID: "a", Status: api.StatusStarting, HostPort: 32768}, // not ready
		{ID: "2", AppName: "web", NodeID: "a", Status: api.StatusHealthy, HostPort: 0},      // no port yet
	})

	r := New(st)
	routes := r.Routes()
	if len(routes) != 0 {
		t.Fatalf("no routable backends expected, got %v", routes)
	}
}
