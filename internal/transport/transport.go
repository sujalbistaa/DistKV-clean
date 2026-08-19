// Package transport defines the RPC boundary that all Raft and KV traffic
// crosses. Production code talks over the grpc implementation; tests talk
// over Fake, which can drop, delay, duplicate, and reorder messages and
// simulate disconnects, partitions, and crashes under a seeded RNG, so a
// failing test reproduces exactly.
package transport

import (
	"context"
	"errors"
)

// NodeID identifies a member of a cluster or shard group.
type NodeID string

var (
	// ErrUnreachable is returned when the destination cannot be reached
	// because of a disconnect, partition, or crash.
	ErrUnreachable = errors.New("transport: destination unreachable")
	// ErrDropped is returned when the network simulator (or, in production,
	// the underlying connection) drops the message in flight.
	ErrDropped = errors.New("transport: message dropped")
	// ErrNoHandler is returned when the destination has not registered a
	// handler to receive RPCs.
	ErrNoHandler = errors.New("transport: no handler registered")
	// ErrCrashed is returned when either endpoint is simulated as crashed.
	ErrCrashed = errors.New("transport: node crashed")
)

// Handler processes an incoming RPC and returns a reply or an error. It is
// invoked with the NodeID of the sender.
type Handler func(ctx context.Context, from NodeID, req any) (any, error)

// Transport is how a node sends RPCs to peers and receives RPCs addressed to
// itself. Each Transport value is bound to exactly one local NodeID.
//
// Implementations: Fake (internal/transport, in-memory, for tests) and the
// production grpc implementation.
type Transport interface {
	// Send delivers req to the node `to` and returns its reply. It blocks
	// until a reply arrives, ctx is done, or the transport determines the
	// message cannot be delivered.
	Send(ctx context.Context, to NodeID, req any) (any, error)

	// RegisterHandler installs the function invoked for RPCs addressed to
	// this transport's local node. A second call replaces the handler.
	RegisterHandler(h Handler)

	// LocalID returns the NodeID this transport is bound to.
	LocalID() NodeID

	// Close releases resources held by the transport.
	Close() error
}
