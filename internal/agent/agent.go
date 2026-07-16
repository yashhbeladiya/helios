// Package agent implements noded's core: poll assignments from controld,
// converge local Docker containers to match, health-check instances, and
// heartbeat actual state back.
//
// It shells out to the docker CLI instead of using the Docker SDK: zero
// dependencies, trivially debuggable, and honest about being a PoC. The
// exec seam (runDocker) is one function, so swapping to the SDK later is
// mechanical.
package agent

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os/exec"
	"strings"
	"sync"
	"time"

	"helios/internal/api"
)

type Agent struct {
	NodeID      string
	ControlAddr string // http://controld:8080
	AgentAddr   string // advertised host:port of this agent
	Region      string
	CPUMillis   int
	MemMB       int

	mu      sync.Mutex
	running map[string]*localInstance // instanceID -> state
	client  *http.Client
}

// DrainGrace is how long a de-assigned container keeps running after it
// has been reported as draining, so the control plane can pull it from the
// proxy's routing table and in-flight requests can complete before the
// container is removed. It must exceed one heartbeat + one proxy sync
// interval; the defaults are 2s and 1s, so 6s leaves comfortable margin.
// This is what makes make-before-break migration actually hitless.
const DrainGrace = 6 * time.Second

type localInstance struct {
	inst        api.Instance
	containerID string
	hostPort    int
	healthy     bool
	draining    bool
	drainStart  time.Time
}

func New(nodeID, controlAddr, agentAddr, region string, cpu, mem int) *Agent {
	return &Agent{
		NodeID:      nodeID,
		ControlAddr: controlAddr,
		AgentAddr:   agentAddr,
		Region:      region,
		CPUMillis:   cpu,
		MemMB:       mem,
		running:     map[string]*localInstance{},
		client:      &http.Client{Timeout: 5 * time.Second},
	}
}

// Run registers with the control plane and loops forever:
// heartbeat -> fetch assignment -> converge -> health check.
func (a *Agent) Run(interval time.Duration) error {
	if err := a.register(); err != nil {
		return fmt.Errorf("register: %w", err)
	}
	log.Printf("noded %s registered with %s", a.NodeID, a.ControlAddr)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		a.healthCheckAll()
		a.heartbeat()
		assignment, err := a.fetchAssignment()
		if err != nil {
			log.Printf("fetch assignment: %v", err)
			continue
		}
		a.converge(assignment)
	}
	return nil
}

func (a *Agent) register() error {
	n := api.Node{
		ID: a.NodeID, Addr: a.AgentAddr, Region: a.Region,
		CPUMillis: a.CPUMillis, MemMB: a.MemMB, LastHeartbeat: time.Now(),
	}
	return a.post("/v1/nodes/register", n, nil)
}

func (a *Agent) heartbeat() {
	a.mu.Lock()
	insts := make([]api.Instance, 0, len(a.running))
	for _, li := range a.running {
		inst := li.inst
		inst.ContainerID = li.containerID
		inst.HostPort = li.hostPort
		switch {
		case li.draining:
			inst.Status = api.StatusDraining
		case li.healthy:
			inst.Status = api.StatusHealthy
		default:
			inst.Status = api.StatusStarting
		}
		insts = append(insts, inst)
	}
	a.mu.Unlock()
	hb := api.Heartbeat{NodeID: a.NodeID, Instances: insts}
	if err := a.post("/v1/nodes/heartbeat", hb, nil); err != nil {
		log.Printf("heartbeat: %v", err)
	}
}

func (a *Agent) fetchAssignment() (api.Assignment, error) {
	var out api.Assignment
	resp, err := a.client.Get(a.ControlAddr + "/v1/nodes/" + a.NodeID + "/assignments")
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()
	return out, json.NewDecoder(resp.Body).Decode(&out)
}

// converge makes local Docker state match the assignment:
// start what's missing, stop what shouldn't exist.
func (a *Agent) converge(assignment api.Assignment) {
	want := map[string]api.Instance{}
	for _, inst := range assignment.Instances {
		want[inst.ID] = inst
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	now := time.Now()

	// Retire instances no longer assigned here, gracefully: first report
	// them as draining (so controld drops them from the proxy routing
	// table and no new requests arrive), then remove the container only
	// after DrainGrace, once in-flight requests have drained. Killing
	// immediately here would strand requests the proxy still routes for up
	// to a sync interval — the source of dropped requests during migration.
	for id, li := range a.running {
		if _, ok := want[id]; ok {
			li.draining = false // re-assigned before drain elapsed
			li.drainStart = time.Time{}
			continue
		}
		if li.drainStart.IsZero() {
			li.draining = true
			li.drainStart = now
			log.Printf("draining instance %s (container %s): out of rotation, %s grace", id, short(li.containerID), DrainGrace)
			continue
		}
		if now.Sub(li.drainStart) < DrainGrace {
			continue // still draining; keep serving in-flight requests
		}
		log.Printf("stopping drained instance %s (container %s)", id, short(li.containerID))
		if err := runDocker("rm", "-f", li.containerID); err != nil {
			log.Printf("docker rm: %v", err)
		}
		delete(a.running, id)
	}

	// Start newly assigned instances.
	for id, inst := range want {
		if _, ok := a.running[id]; ok {
			continue
		}
		if inst.Status == api.StatusDraining {
			continue // draining instances are kept, never (re)started
		}
		li, err := a.startInstance(inst)
		if err != nil {
			log.Printf("start %s: %v", id, err)
			continue
		}
		a.running[id] = li
	}
}

// startInstance launches the app container, publishing its port on an
// ephemeral host port that we then discover via docker port.
func (a *Agent) startInstance(inst api.Instance) (*localInstance, error) {
	app, err := a.getApp(inst.AppName)
	if err != nil {
		return nil, err
	}
	name := "helios-" + inst.ID
	// Inject the node's region so region-aware workloads (e.g. the
	// experiment's loadapp) can tell local from cross-region traffic.
	out, err := runDockerOut("run", "-d", "--name", name,
		"-e", "HELIOS_REGION="+a.Region,
		"-p", fmt.Sprintf("0:%d", app.Port), app.Image)
	if err != nil {
		return nil, fmt.Errorf("docker run: %w", err)
	}
	containerID := strings.TrimSpace(out)

	portOut, err := runDockerOut("port", containerID, fmt.Sprintf("%d/tcp", app.Port))
	if err != nil {
		return nil, fmt.Errorf("docker port: %w", err)
	}
	hostPort, err := parseHostPort(portOut)
	if err != nil {
		return nil, err
	}
	log.Printf("started %s: container %s on host port %d", inst.ID, short(containerID), hostPort)
	return &localInstance{inst: inst, containerID: containerID, hostPort: hostPort}, nil
}

// healthCheckAll marks an instance healthy when its TCP port accepts
// connections. HTTP /healthz checks are a natural upgrade.
func (a *Agent) healthCheckAll() {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, li := range a.running {
		addr := fmt.Sprintf("127.0.0.1:%d", li.hostPort)
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err != nil {
			li.healthy = false
			continue
		}
		conn.Close()
		li.healthy = true
	}
}

func (a *Agent) getApp(name string) (api.App, error) {
	var app api.App
	resp, err := a.client.Get(a.ControlAddr + "/v1/apps/" + name)
	if err != nil {
		return app, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return app, fmt.Errorf("app %s: status %d", name, resp.StatusCode)
	}
	return app, json.NewDecoder(resp.Body).Decode(&app)
}

func (a *Agent) post(path string, in, out any) error {
	body, err := json.Marshal(in)
	if err != nil {
		return err
	}
	resp, err := a.client.Post(a.ControlAddr+path, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("%s: status %d", path, resp.StatusCode)
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

// --- docker exec seam ---

func runDocker(args ...string) error {
	_, err := runDockerOut(args...)
	return err
}

func runDockerOut(args ...string) (string, error) {
	cmd := exec.Command("docker", args...)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("docker %s: %v: %s", strings.Join(args, " "), err, errb.String())
	}
	return out.String(), nil
}

// parseHostPort extracts the port from docker port output like
// "0.0.0.0:32768" (possibly multiple lines for v4/v6).
func parseHostPort(out string) (int, error) {
	line := strings.SplitN(strings.TrimSpace(out), "\n", 2)[0]
	idx := strings.LastIndex(line, ":")
	if idx < 0 {
		return 0, fmt.Errorf("unexpected docker port output: %q", out)
	}
	var p int
	if _, err := fmt.Sscanf(line[idx+1:], "%d", &p); err != nil {
		return 0, fmt.Errorf("parse port from %q: %w", line, err)
	}
	return p, nil
}

func short(id string) string {
	if len(id) > 12 {
		return id[:12]
	}
	return id
}
