// Package reconciler runs the control loop: observe actual state,
// compute desired state via the scheduler, converge.
//
// This is the architectural heart of Helios and the Milestone 2 seam:
// today desired state comes from user intent (deploys, migrations);
// later the predictive placer becomes just another input that mutates
// desired placement. The loop itself never changes.
package reconciler

import (
	"log"
	"time"

	"helios/internal/api"
	"helios/internal/scheduler"
	"helios/internal/store"
)

type Reconciler struct {
	store store.Store
	sched *scheduler.Scheduler

	// activeMigrations: appName -> target node. Cleared once all of the
	// app's actual healthy instances live on the target (make-before-break
	// complete).
	activeMigrations map[string]string
}

func New(s store.Store) *Reconciler {
	return &Reconciler{
		store:            s,
		sched:            scheduler.New(),
		activeMigrations: map[string]string{},
	}
}

// Run blocks, reconciling every interval.
func (r *Reconciler) Run(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		r.step()
	}
}

func (r *Reconciler) step() {
	// 1. Absorb new migration intents.
	for _, m := range r.store.PopMigrations() {
		log.Printf("reconciler: migration requested: %s -> %s", m.AppName, m.TargetNode)
		r.activeMigrations[m.AppName] = m.TargetNode
	}

	// 2. Retire completed migrations (all healthy replicas on target).
	actual := r.store.Actual()
	for app, target := range r.activeMigrations {
		healthyOnTarget, healthyElsewhere := 0, 0
		for _, inst := range actual {
			if inst.AppName != app || inst.Status != api.StatusHealthy {
				continue
			}
			if inst.NodeID == target {
				healthyOnTarget++
			} else {
				healthyElsewhere++
			}
		}
		if spec, ok := r.store.GetApp(app); ok && healthyOnTarget >= spec.Replicas && healthyElsewhere == 0 {
			log.Printf("reconciler: migration complete: %s now on %s", app, target)
			delete(r.activeMigrations, app)
		}
	}

	// 3. Recompute desired placement (region plans from the placer flow
	// through here — the placer is just another writer of desired state).
	desired := r.sched.Place(r.store.Nodes(), r.store.Apps(), r.store.Desired(), r.activeMigrations, r.store.RegionPlans())
	r.store.SetDesired(desired)
}

// AssignmentsFor returns the desired instances for one node, PLUS any
// old-node instances of apps mid-migration so the old copy keeps serving
// until the new one is healthy (make-before-break). noded converges to
// exactly this set.
func (r *Reconciler) AssignmentsFor(nodeID string) api.Assignment {
	var out []api.Instance
	for _, inst := range r.store.Desired() {
		if inst.NodeID == nodeID {
			out = append(out, inst)
		}
	}
	// Make-before-break: while an app is migrating, keep its existing
	// healthy instances on other nodes alive (as draining) until the
	// target copies are healthy.
	for app, target := range r.activeMigrations {
		if !r.targetHealthy(app, target) {
			for _, inst := range r.store.Actual() {
				if inst.AppName == app && inst.NodeID == nodeID && nodeID != target && inst.Status == api.StatusHealthy {
					inst.Status = api.StatusDraining
					out = append(out, inst)
				}
			}
		}
	}
	return api.Assignment{Instances: out}
}

func (r *Reconciler) targetHealthy(app, target string) bool {
	spec, ok := r.store.GetApp(app)
	if !ok {
		return true
	}
	healthy := 0
	for _, inst := range r.store.Actual() {
		if inst.AppName == app && inst.NodeID == target && inst.Status == api.StatusHealthy {
			healthy++
		}
	}
	return healthy >= spec.Replicas
}

// Routes builds the proxy routing table from actual state: only healthy
// instances are routable. When a node reports an instance as draining
// (de-assigned, about to be removed), it drops out of the table here so
// the proxy sends it no new requests; its in-flight requests finish on the
// proxy side while the node keeps the container alive for its drain grace.
// That ordering — out of routes first, killed later — is what makes
// migration hitless.
func (r *Reconciler) Routes() []api.Route {
	// Only route to instances on nodes that are still heartbeating. A dead
	// node's last-reported instances linger in actual state, but its
	// containers are gone with it — routing to them would 502. Dropping
	// them here is what keeps the data plane serving through a node loss
	// while the reconciler reschedules the lost replicas elsewhere.
	now := time.Now()
	nodeAddr := map[string]string{}
	nodeRegion := map[string]string{}
	for _, n := range r.store.Nodes() {
		if now.Sub(n.LastHeartbeat) > scheduler.NodeAliveWindow {
			continue
		}
		nodeAddr[n.ID] = n.Addr
		nodeRegion[n.ID] = n.Region
	}
	byHost := map[string][]api.Backend{}
	for _, app := range r.store.Apps() {
		for _, inst := range r.store.Actual() {
			if inst.AppName != app.Name || inst.HostPort == 0 {
				continue
			}
			if inst.Status != api.StatusHealthy {
				continue // draining/starting/dead: not routable
			}
			host, ok := nodeAddr[inst.NodeID]
			if !ok {
				continue
			}
			// noded addr is host:agentPort; backend is host:instancePort.
			addr := hostOnly(host) + ":" + itoa(inst.HostPort)
			byHost[app.Host] = append(byHost[app.Host], api.Backend{Addr: addr, Region: nodeRegion[inst.NodeID]})
		}
	}
	var routes []api.Route
	for host, backends := range byHost {
		routes = append(routes, api.Route{Host: host, Backends: backends})
	}
	return routes
}

func hostOnly(addr string) string {
	for i := len(addr) - 1; i >= 0; i-- {
		if addr[i] == ':' {
			return addr[:i]
		}
	}
	return addr
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [12]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(b[pos:])
}
