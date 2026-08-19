// Command distkv-gateway fronts a DistKV cluster with an HTTP API and,
// optionally, the console that consumes it: a live view of every node's
// Raft state, key-value operations routed to the leader, and the same
// fault injection the test suite uses, driven from a browser.
//
// It holds no state of its own beyond a client session. Everything it
// reports it learned by asking the cluster a moment ago.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/sujalbistaa/DistKV/internal/gateway"
	"github.com/sujalbistaa/DistKV/internal/transport"
)

func main() {
	opts := gateway.DefaultOptions()
	var (
		listen   = flag.String("listen", ":8080", "address to serve the HTTP API and console on")
		nodes    = flag.String("nodes", "", "comma-separated id=host:port for every node in the cluster (required)")
		web      = flag.String("web", "", "directory of built console assets to serve at /; empty serves the API alone")
		autoHeal = flag.Duration("auto-heal", 5*time.Minute, "restore a cluster left broken and idle for this long; 0 disables")
	)
	flag.BoolVar(&opts.AllowChaos, "allow-chaos", true, "enable the fault-injection endpoints")
	flag.IntVar(&opts.MaxKeyLen, "max-key-len", opts.MaxKeyLen, "longest key a client may store")
	flag.IntVar(&opts.MaxValueLen, "max-value-len", opts.MaxValueLen, "longest value a client may store")
	flag.IntVar(&opts.MaxKeys, "max-keys", opts.MaxKeys, "how many distinct keys may be stored through this gateway")
	flag.IntVar(&opts.WritesPerMinute, "writes-per-minute", opts.WritesPerMinute, "per-visitor write budget")
	flag.IntVar(&opts.FaultsPerMinute, "faults-per-minute", opts.FaultsPerMinute, "per-visitor fault-injection budget")
	flag.BoolVar(&opts.TrustProxyHeaders, "trust-proxy-headers", false,
		"read the client address from X-Forwarded-For; only safe behind a proxy that always sets it")
	flag.Parse()

	opts.WebDir = *web
	if err := run(*listen, *nodes, *autoHeal, opts); err != nil {
		fmt.Fprintf(os.Stderr, "distkv-gateway: %v\n", err)
		os.Exit(1)
	}
}

func run(listen, nodesFlag string, autoHeal time.Duration, opts gateway.Options) error {
	members, err := parseNodes(nodesFlag)
	if err != nil {
		return err
	}

	cluster, err := gateway.NewCluster(members)
	if err != nil {
		return err
	}
	defer cluster.Close()

	chaos := gateway.NewChaos(cluster, autoHeal)
	defer chaos.Close()

	// One client id for the process, held for its whole life: the state
	// machine deduplicates writes on (client id, sequence number), so this
	// is what makes a request the gateway retries after a redirect land at
	// most once no matter how many nodes see it.
	client := gateway.NewClient(cluster, fmt.Sprintf("gateway-%d-%d", os.Getpid(), rand.Int63()))

	server := &http.Server{
		Addr:    listen,
		Handler: gateway.NewServer(cluster, chaos, client, opts).Handler(),
		// Generous, because the console holds a server-sent-events stream
		// open indefinitely; the read side stays bounded.
		ReadHeaderTimeout: 10 * time.Second,
		WriteTimeout:      0,
	}

	serveErr := make(chan error, 1)
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
		}
	}()

	fmt.Fprintf(os.Stderr, "distkv-gateway: listening on %s, fronting %d nodes (chaos=%v)\n",
		listen, len(members), opts.AllowChaos)
	if opts.WebDir != "" {
		fmt.Fprintf(os.Stderr, "distkv-gateway: serving console from %s\n", opts.WebDir)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	select {
	case sig := <-signals:
		fmt.Fprintf(os.Stderr, "distkv-gateway: %s, shutting down\n", sig)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return server.Shutdown(ctx)
	case err := <-serveErr:
		return err
	}
}

// parseNodes turns "node1=host:1,node2=host:2" into the cluster's members,
// in the order given.
func parseNodes(s string) ([]gateway.Member, error) {
	if strings.TrimSpace(s) == "" {
		return nil, fmt.Errorf("-nodes is required, as a comma-separated list of id=host:port")
	}
	var members []gateway.Member
	seen := make(map[transport.NodeID]bool)
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		name, addr, ok := strings.Cut(part, "=")
		name, addr = strings.TrimSpace(name), strings.TrimSpace(addr)
		if !ok || name == "" || addr == "" {
			return nil, fmt.Errorf("malformed node %q, want id=host:port", part)
		}
		if seen[transport.NodeID(name)] {
			return nil, fmt.Errorf("node %s listed twice", name)
		}
		seen[transport.NodeID(name)] = true
		members = append(members, gateway.Member{ID: transport.NodeID(name), Addr: addr})
	}
	if len(members) == 0 {
		return nil, fmt.Errorf("-nodes listed no usable entries")
	}
	return members, nil
}
