// helios is the operator CLI.
//
//	helios deploy  --name web --image nginx:alpine --port 80 --replicas 2
//	helios status
//	helios migrate --app web --node node-b
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"text/tabwriter"
	"time"

	"helios/internal/api"
)

var control string

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	cmd, args := os.Args[1], os.Args[2:]
	switch cmd {
	case "deploy":
		deploy(args)
	case "status":
		status(args)
	case "migrate":
		migrate(args)
	default:
		usage()
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
  helios deploy  --name NAME --image IMAGE --port PORT [--replicas N] [--host HOST]
  helios status
  helios migrate --app NAME --node NODE_ID

flags common to all commands:
  --control URL   controld base URL (default http://127.0.0.1:8080)`)
	os.Exit(2)
}

func newFlags(name string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ExitOnError)
	fs.StringVar(&control, "control", "http://127.0.0.1:8080", "controld base URL")
	return fs
}

func deploy(args []string) {
	fs := newFlags("deploy")
	name := fs.String("name", "", "app name (required)")
	image := fs.String("image", "", "container image (required)")
	port := fs.Int("port", 0, "container port the app listens on (required)")
	replicas := fs.Int("replicas", 1, "replica count")
	host := fs.String("host", "", "hostname routed by proxyd (default NAME.local)")
	cpu := fs.Int("cpu", 100, "per-replica CPU millicores")
	mem := fs.Int("mem", 64, "per-replica memory MB")
	autoplace := fs.Bool("autoplace", false, "let the predictive placer manage regions and replica counts")
	rpsPer := fs.Int("rps-per-replica", 50, "capacity model: RPS one replica can serve (autoplace)")
	maxReplicas := fs.Int("max-replicas", 10, "global replica cap (autoplace)")
	fs.Parse(args)
	if *name == "" || *image == "" || *port == 0 {
		fs.Usage()
		os.Exit(2)
	}
	app := api.App{Name: *name, Image: *image, Port: *port, Replicas: *replicas,
		Host: *host, CPUMillis: *cpu, MemMB: *mem,
		AutoPlace: *autoplace, RPSPerReplica: *rpsPer, MaxReplicas: *maxReplicas}
	must(post("/v1/apps", app))
	if app.Host == "" {
		app.Host = app.Name + ".local"
	}
	fmt.Printf("deployed %s (%d replica(s))\ntry:  curl -H 'Host: %s' http://localhost:8000/\n",
		app.Name, app.Replicas, app.Host)
}

func status(args []string) {
	newFlags("status").Parse(args)

	var nodes []api.Node
	must(get("/v1/nodes", &nodes))
	fmt.Println("NODES")
	tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "  ID\tADDR\tREGION\tLAST HEARTBEAT")
	for _, n := range nodes {
		fmt.Fprintf(tw, "  %s\t%s\t%s\t%s ago\n", n.ID, n.Addr, n.Region,
			time.Since(n.LastHeartbeat).Round(time.Second))
	}
	tw.Flush()

	var insts []api.Instance
	must(get("/v1/instances", &insts))
	fmt.Println("\nINSTANCES")
	tw = tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "  ID\tAPP\tNODE\tSTATUS\tHOST PORT")
	for _, i := range insts {
		fmt.Fprintf(tw, "  %s\t%s\t%s\t%s\t%d\n", i.ID, i.AppName, i.NodeID, i.Status, i.HostPort)
	}
	tw.Flush()
}

func migrate(args []string) {
	fs := newFlags("migrate")
	app := fs.String("app", "", "app to migrate (required)")
	node := fs.String("node", "", "target node ID (required)")
	fs.Parse(args)
	if *app == "" || *node == "" {
		fs.Usage()
		os.Exit(2)
	}
	must(post("/v1/migrate", api.MigrateRequest{AppName: *app, TargetNode: *node}))
	fmt.Printf("migration of %s to %s requested (make-before-break; watch `helios status`)\n", *app, *node)
}

func post(path string, v any) error {
	body, _ := json.Marshal(v)
	resp, err := http.Post(control+path, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("%s: HTTP %d", path, resp.StatusCode)
	}
	return nil
}

func get(path string, out any) error {
	resp, err := http.Get(control + path)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return json.NewDecoder(resp.Body).Decode(out)
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
