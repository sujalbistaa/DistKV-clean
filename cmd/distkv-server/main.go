// Command distkv-server runs one DistKV node: a Raft peer plus the
// key-value state machine it drives, serving both the internal Raft RPCs
// and the client-facing KV API on a single gRPC listener.
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/sujalbistaa/DistKV/internal/shard"
	"github.com/sujalbistaa/DistKV/internal/supervisor"
	"github.com/sujalbistaa/DistKV/internal/transport"
	"github.com/sujalbistaa/DistKV/internal/transport/grpctransport"
	pb "github.com/sujalbistaa/DistKV/proto/distkvpb"
)

func main() {
	var (
		id                = flag.String("id", "", "this node's id (required); must match a name used in -peers")
		listen            = flag.String("listen", ":7070", "address to serve the Raft and KV gRPC services on")
		peersFlag         = flag.String("peers", "", "comma-separated id=host:port for every node in this group, including this one (required)")
		dataDir           = flag.String("data-dir", "", "directory for the write-ahead log and snapshots (required)")
		shardConfig       = flag.String("shard-config", "", "optional path to a shard map JSON file; when set, this node must appear in exactly one shard")
		snapshotThreshold = flag.Int("snapshot-threshold", 10000, "log entries to accumulate before snapshotting; 0 disables")
		electionMin       = flag.Duration("election-timeout-min", 150*time.Millisecond, "lower bound of the randomised election timeout")
		electionMax       = flag.Duration("election-timeout-max", 300*time.Millisecond, "upper bound of the randomised election timeout")
		heartbeat         = flag.Duration("heartbeat-interval", 50*time.Millisecond, "leader heartbeat interval")
		verbose           = flag.Bool("verbose", false, "log Raft state transitions")
	)
	flag.Parse()

	if err := run(*id, *listen, *peersFlag, *dataDir, *shardConfig, *snapshotThreshold,
		*electionMin, *electionMax, *heartbeat, *verbose); err != nil {
		fmt.Fprintf(os.Stderr, "distkv-server: %v\n", err)
		os.Exit(1)
	}
}

func run(id, listen, peersFlag, dataDir, shardConfig string, snapshotThreshold int,
	electionMin, electionMax, heartbeat time.Duration, verbose bool) error {

	if id == "" {
		return fmt.Errorf("-id is required")
	}
	if dataDir == "" {
		return fmt.Errorf("-data-dir is required")
	}

	addrs, err := parsePeers(peersFlag)
	if err != nil {
		return err
	}
	nodeID := transport.NodeID(id)
	if _, ok := addrs[nodeID]; !ok {
		return fmt.Errorf("-peers must include this node (%s); got %v", id, keysOf(addrs))
	}

	// The peer set is either the whole -peers list or, when a shard map is
	// supplied, just the replicas of the shard this node belongs to. Each
	// shard is an independent Raft group.
	group := keysOf(addrs)
	if shardConfig != "" {
		group, err = shardGroupFor(shardConfig, nodeID)
		if err != nil {
			return err
		}
	}

	peers := make([]transport.NodeID, 0, len(group))
	for _, peer := range group {
		if peer == nodeID {
			continue
		}
		if _, ok := addrs[peer]; !ok {
			return fmt.Errorf("shard replica %s has no address in -peers", peer)
		}
		peers = append(peers, peer)
	}

	logger := log.New(os.Stderr, fmt.Sprintf("[%s] ", id), log.LstdFlags|log.Lmicroseconds)
	if !verbose {
		logger = log.New(nopWriter{}, "", 0)
	}

	tr := grpctransport.New(nodeID, addrs)
	defer tr.Close()

	// The supervisor owns the Raft node, the state machine, and the disk
	// underneath them, so the node can be destroyed and rebuilt from its
	// own write-ahead log without the process going anywhere. It is handed
	// the peer addresses so a leader redirect names somewhere the client
	// can dial rather than a node id it can't resolve.
	sup, err := supervisor.New(supervisor.Config{
		ID:                 nodeID,
		Peers:              peers,
		Addrs:              addrs,
		DataDir:            dataDir,
		Transport:          tr,
		ElectionTimeoutMin: electionMin,
		ElectionTimeoutMax: electionMax,
		HeartbeatInterval:  heartbeat,
		SnapshotThreshold:  snapshotThreshold,
		Logger:             logger,
	})
	if err != nil {
		return err
	}
	defer sup.Close()

	lis, err := net.Listen("tcp", listen)
	if err != nil {
		return fmt.Errorf("listening on %s: %w", listen, err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterRaftServer(grpcServer, tr)
	pb.RegisterKVServer(grpcServer, sup)
	pb.RegisterAdminServer(grpcServer, sup)

	serveErr := make(chan error, 1)
	go func() { serveErr <- grpcServer.Serve(lis) }()

	fmt.Fprintf(os.Stderr, "distkv-server: node %s listening on %s, peers %v, data-dir %s\n",
		id, lis.Addr(), peers, dataDir)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	select {
	case sig := <-signals:
		fmt.Fprintf(os.Stderr, "distkv-server: %s, shutting down\n", sig)
		grpcServer.GracefulStop()
		return nil
	case err := <-serveErr:
		return err
	}
}

// parsePeers turns "a=host:1,b=host:2" into a node id to address map.
func parsePeers(s string) (map[transport.NodeID]string, error) {
	if strings.TrimSpace(s) == "" {
		return nil, fmt.Errorf("-peers is required, as a comma-separated list of id=host:port")
	}
	addrs := make(map[transport.NodeID]string)
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		name, addr, ok := strings.Cut(part, "=")
		if !ok || strings.TrimSpace(name) == "" || strings.TrimSpace(addr) == "" {
			return nil, fmt.Errorf("malformed peer %q, want id=host:port", part)
		}
		addrs[transport.NodeID(strings.TrimSpace(name))] = strings.TrimSpace(addr)
	}
	if len(addrs) == 0 {
		return nil, fmt.Errorf("-peers listed no usable entries")
	}
	return addrs, nil
}

// shardGroupFor returns the replica set of the shard that owns this node.
func shardGroupFor(path string, id transport.NodeID) ([]transport.NodeID, error) {
	shardMap, err := shard.LoadMap(path)
	if err != nil {
		return nil, err
	}
	for _, s := range shardMap.Shards {
		for _, replica := range s.Replicas {
			if replica == id {
				return s.Replicas, nil
			}
		}
	}
	return nil, fmt.Errorf("node %s does not appear in any shard in %s", id, path)
}

func keysOf(m map[transport.NodeID]string) []transport.NodeID {
	out := make([]transport.NodeID, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

type nopWriter struct{}

func (nopWriter) Write(p []byte) (int, error) { return len(p), nil }
