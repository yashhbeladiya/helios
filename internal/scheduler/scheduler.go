// Package scheduler decides WHERE instances run: best-fit bin-packing on
// CPU/memory with replica anti-affinity, now region-aware.
//
// Each app resolves to placement targets. An app with a region plan
// (written by the predictive placer) gets one target per region; an app
// without one gets a single region-agnostic target of App.Replicas. The
// scheduler is deterministic and idempotent: existing placements are kept
// when they still satisfy a target (placement stability), and only the
// deltas are (re)scheduled.
package scheduler

import (
	"fmt"
	"sort"
	"time"

	"helios/internal/api"
)

// NodeAliveWindow: a node missing heartbeats longer than this is
// considered dead and receives no placements.
const NodeAliveWindow = 10 * time.Second

// anyRegion is the target region for region-agnostic placement.
const anyRegion = ""

type Scheduler struct{}

func New() *Scheduler { return &Scheduler{} }

type targetKey struct {
	app    string
	region string // anyRegion for region-agnostic
}

// Place computes the full desired instance set.
//
// plans maps appName -> region -> replica count (from the placer);
// pinned maps appName -> nodeID for active manual migrations, which
// override plans while in flight.
func (s *Scheduler) Place(nodes []api.Node, apps []api.App, existing []api.Instance,
	pinned map[string]string, plans map[string]map[string]int) []api.Instance {

	now := time.Now()
	alive := map[string]api.Node{}
	for _, n := range nodes {
		if now.Sub(n.LastHeartbeat) <= NodeAliveWindow {
			alive[n.ID] = n
		}
	}

	freeCPU := map[string]int{}
	freeMem := map[string]int{}
	for id, n := range alive {
		freeCPU[id] = n.CPUMillis
		freeMem[id] = n.MemMB
	}

	appByName := map[string]api.App{}
	for _, a := range apps {
		appByName[a.Name] = a
	}

	// Resolve placement targets.
	targets := map[targetKey]int{}
	for _, a := range apps {
		if _, isPinned := pinned[a.Name]; isPinned {
			// Manual migration wins: a single region-agnostic target;
			// pick() forces the pinned node.
			targets[targetKey{a.Name, anyRegion}] = a.Replicas
			continue
		}
		if plan, ok := plans[a.Name]; ok {
			for region, count := range plan {
				if count > 0 {
					targets[targetKey{a.Name, region}] = count
				}
			}
			continue
		}
		targets[targetKey{a.Name, anyRegion}] = a.Replicas
	}

	// Keep stable placements that still satisfy a target.
	var out []api.Instance
	kept := map[targetKey]int{}
	for _, inst := range existing {
		app, ok := appByName[inst.AppName]
		if !ok {
			continue // app deleted
		}
		node, aliveOK := alive[inst.NodeID]
		if !aliveOK {
			continue // node dead: reschedule
		}
		if target, isPinned := pinned[inst.AppName]; isPinned && inst.NodeID != target {
			continue // migration: reschedule onto pinned node
		}
		k := targetKey{inst.AppName, node.Region}
		if _, ok := targets[k]; !ok {
			k = targetKey{inst.AppName, anyRegion}
			if _, ok := targets[k]; !ok {
				continue // no target covers this instance (e.g. region planned to 0)
			}
		}
		if kept[k] >= targets[k] {
			continue // scale-down
		}
		freeCPU[inst.NodeID] -= app.CPUMillis
		freeMem[inst.NodeID] -= app.MemMB
		out = append(out, inst)
		kept[k]++
	}

	// Schedule missing replicas per target, deterministically ordered.
	ordered := make([]targetKey, 0, len(targets))
	for k := range targets {
		ordered = append(ordered, k)
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].app != ordered[j].app {
			return ordered[i].app < ordered[j].app
		}
		return ordered[i].region < ordered[j].region
	})

	for _, k := range ordered {
		app := appByName[k.app]
		for i := kept[k]; i < targets[k]; i++ {
			nodeID, err := s.pick(alive, freeCPU, freeMem, app, k.region, out, pinned[k.app])
			if err != nil {
				continue // no capacity this round; retried next reconcile
			}
			freeCPU[nodeID] -= app.CPUMillis
			freeMem[nodeID] -= app.MemMB
			out = append(out, api.Instance{
				ID:      fmt.Sprintf("%s-%d-%d", app.Name, now.UnixNano(), i),
				AppName: app.Name,
				NodeID:  nodeID,
				Status:  api.StatusPending,
			})
		}
	}
	return out
}

// pick chooses a node: the pinned node if set, otherwise best-fit among
// alive nodes in the target region (or all nodes for anyRegion), with
// anti-affinity preference.
func (s *Scheduler) pick(alive map[string]api.Node, freeCPU, freeMem map[string]int,
	app api.App, region string, placed []api.Instance, pinnedNode string) (string, error) {

	if pinnedNode != "" {
		if _, ok := alive[pinnedNode]; !ok {
			return "", fmt.Errorf("pinned node %s not alive", pinnedNode)
		}
		if freeCPU[pinnedNode] < app.CPUMillis || freeMem[pinnedNode] < app.MemMB {
			return "", fmt.Errorf("pinned node %s lacks capacity", pinnedNode)
		}
		return pinnedNode, nil
	}

	replicasOn := map[string]int{}
	for _, inst := range placed {
		if inst.AppName == app.Name {
			replicasOn[inst.NodeID]++
		}
	}

	type candidate struct {
		id       string
		replicas int
		freeCPU  int
	}
	var cands []candidate
	for id, n := range alive {
		if region != anyRegion && n.Region != region {
			continue
		}
		if freeCPU[id] >= app.CPUMillis && freeMem[id] >= app.MemMB {
			cands = append(cands, candidate{id, replicasOn[id], freeCPU[id]})
		}
	}
	if len(cands) == 0 {
		return "", fmt.Errorf("no node with capacity for %s in region %q", app.Name, region)
	}
	sort.Slice(cands, func(i, j int) bool {
		if cands[i].replicas != cands[j].replicas {
			return cands[i].replicas < cands[j].replicas
		}
		if cands[i].freeCPU != cands[j].freeCPU {
			return cands[i].freeCPU < cands[j].freeCPU
		}
		return cands[i].id < cands[j].id
	})
	return cands[0].id, nil
}
