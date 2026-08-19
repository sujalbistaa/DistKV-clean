// Command distkv-cli is a client for DistKV: get, put, append, and delete
// against a cluster, following leader redirects automatically.
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/sujalbistaa/DistKV/proto/distkvpb"
)

func main() {
	var (
		endpoints = flag.String("endpoints", "127.0.0.1:7070", "comma-separated host:port list of cluster nodes")
		timeout   = flag.Duration("timeout", 10*time.Second, "overall timeout for the operation")
		clientID  = flag.String("client-id", "", "client session id; a random one is generated if unset")
	)
	flag.Usage = usage
	flag.Parse()

	if err := run(flag.Args(), *endpoints, *clientID, *timeout); err != nil {
		fmt.Fprintf(os.Stderr, "distkv-cli: %v\n", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `usage: distkv-cli [flags] <command> [args]

commands:
  get <key>
  put <key> <value>
  append <key> <value>
  delete <key>

flags:
`)
	flag.PrintDefaults()
}

func run(args []string, endpoints, clientID string, timeout time.Duration) error {
	if len(args) == 0 {
		usage()
		return fmt.Errorf("no command given")
	}

	addrs := splitEndpoints(endpoints)
	if len(addrs) == 0 {
		return fmt.Errorf("-endpoints listed no usable addresses")
	}

	if clientID == "" {
		// A client session id must be unique per session: the server
		// deduplicates retried writes on (client id, sequence number), so
		// reusing another session's id could suppress a real write.
		clientID = fmt.Sprintf("cli-%d-%d", os.Getpid(), rand.Int63())
	}

	cmd := &pb.Command{ClientId: clientID, SeqNo: 1}
	var op string

	switch strings.ToLower(args[0]) {
	case "get":
		if len(args) != 2 {
			return fmt.Errorf("get takes exactly one argument: get <key>")
		}
		op, cmd.Key = "get", args[1]
	case "put":
		if len(args) != 3 {
			return fmt.Errorf("put takes exactly two arguments: put <key> <value>")
		}
		op, cmd.Key, cmd.Value = "put", args[1], args[2]
	case "append":
		if len(args) != 3 {
			return fmt.Errorf("append takes exactly two arguments: append <key> <value>")
		}
		op, cmd.Key, cmd.Value = "append", args[1], args[2]
	case "delete":
		if len(args) != 2 {
			return fmt.Errorf("delete takes exactly one argument: delete <key>")
		}
		op, cmd.Key = "delete", args[1]
	default:
		usage()
		return fmt.Errorf("unknown command %q", args[0])
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	reply, err := execute(ctx, addrs, op, cmd)
	if err != nil {
		return err
	}

	switch op {
	case "get":
		if !reply.Found {
			fmt.Println("(not found)")
			return nil
		}
		fmt.Println(reply.Value)
	default:
		fmt.Println("OK")
	}
	return nil
}

// execute sends cmd to the cluster, sweeping the endpoints and following
// leader hints until one accepts it or ctx expires. Every attempt reuses
// the same sequence number, so a retry can never apply twice.
func execute(ctx context.Context, addrs []string, op string, cmd *pb.Command) (*pb.CommandReply, error) {
	conns := make(map[string]pb.KVClient, len(addrs))
	defer func() {
		// Connections are closed by the process exiting; nothing to do
		// here beyond keeping the map scoped to this call.
		_ = conns
	}()

	dial := func(addr string) (pb.KVClient, error) {
		if c, ok := conns[addr]; ok {
			return c, nil
		}
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		client := pb.NewKVClient(conn)
		conns[addr] = client
		return client, nil
	}

	// A redirect may name a node the caller never listed. When the hint
	// looks like an address, adopt it and try it first from then on —
	// otherwise a client given a single endpoint could never reach a
	// leader that happens to be some other node.
	var lastErr error
	for ctx.Err() == nil {
		for i := 0; i < len(addrs); i++ {
			addr := addrs[i]
			if ctx.Err() != nil {
				break
			}
			client, err := dial(addr)
			if err != nil {
				lastErr = err
				continue
			}

			attemptCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			var reply *pb.CommandReply
			switch op {
			case "get":
				reply, err = client.Get(attemptCtx, cmd)
			case "put":
				reply, err = client.Put(attemptCtx, cmd)
			case "append":
				reply, err = client.Append(attemptCtx, cmd)
			case "delete":
				reply, err = client.Delete(attemptCtx, cmd)
			}
			cancel()

			if err != nil {
				lastErr = err
				continue
			}
			if reply.Success {
				return reply, nil
			}
			lastErr = fmt.Errorf("%s (leader hint: %q)", reply.Error, reply.LeaderHint)

			if hint := reply.LeaderHint; strings.Contains(hint, ":") && !contains(addrs, hint) {
				// A dialable address we haven't tried: put it at the
				// front so the next attempt goes straight there.
				addrs = append([]string{hint}, addrs...)
				i = 0
			}
		}
		select {
		case <-ctx.Done():
		case <-time.After(50 * time.Millisecond):
		}
	}

	if lastErr != nil {
		return nil, fmt.Errorf("no endpoint accepted the request: %w", lastErr)
	}
	return nil, ctx.Err()
}

func contains(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}

func splitEndpoints(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}
