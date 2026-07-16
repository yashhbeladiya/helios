// noded is the Helios node agent: runs on every machine that hosts app
// instances, converging local Docker state to control-plane assignments.
package main

import (
	"flag"
	"log"
	"os"
	"time"

	"helios/internal/agent"
)

func main() {
	nodeID := flag.String("id", hostnameDefault(), "unique node ID")
	control := flag.String("control", "http://127.0.0.1:8080", "controld base URL")
	addr := flag.String("addr", "127.0.0.1:9090", "advertised agent host:port (host must be reachable by proxyd)")
	region := flag.String("region", "local", "node region label (used by M2 placement)")
	cpu := flag.Int("cpu", 4000, "node CPU capacity in millicores")
	mem := flag.Int("mem", 4096, "node memory capacity in MB")
	interval := flag.Duration("interval", 2*time.Second, "converge/heartbeat interval")
	flag.Parse()

	a := agent.New(*nodeID, *control, *addr, *region, *cpu, *mem)
	if err := a.Run(*interval); err != nil {
		log.Fatal(err)
	}
}

func hostnameDefault() string {
	h, err := os.Hostname()
	if err != nil {
		return "node-unknown"
	}
	return h
}
