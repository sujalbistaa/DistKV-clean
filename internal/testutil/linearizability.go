package testutil

// Locking: mu guards ops. Recording happens from many client goroutines
// concurrently; Operations/Check are called once, after they've all
// stopped.

import (
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/anishathalye/porcupine"
)

// KVOpKind is the kind of operation recorded in a history.
type KVOpKind int

const (
	KVGet KVOpKind = iota
	KVPut
	KVAppend
)

func (k KVOpKind) String() string {
	switch k {
	case KVGet:
		return "get"
	case KVPut:
		return "put"
	case KVAppend:
		return "append"
	default:
		return "unknown"
	}
}

// KVInput is one operation's arguments, as recorded in a history.
type KVInput struct {
	Op    KVOpKind
	Key   string
	Value string
}

// KVOutput is what an operation returned. Only Get uses Value; for writes
// it is empty and the model ignores it.
type KVOutput struct {
	Value string
}

// KVModel is the sequential specification a linearizable key-value store
// must satisfy. It is partitioned by key: a history is linearizable if and
// only if each key's sub-history is, which keeps the checker's search
// space manageable, and lets the per-partition state be a single value
// rather than a whole map.
var KVModel = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := make(map[string][]porcupine.Operation)
		for _, op := range history {
			key := op.Input.(KVInput).Key
			byKey[key] = append(byKey[key], op)
		}
		partitions := make([][]porcupine.Operation, 0, len(byKey))
		for _, ops := range byKey {
			partitions = append(partitions, ops)
		}
		return partitions
	},
	Init: func() interface{} { return "" },
	Step: func(state, input, output interface{}) (bool, interface{}) {
		st := state.(string)
		in := input.(KVInput)
		switch in.Op {
		case KVGet:
			// A read is only legal if it returned exactly the current value.
			return output.(KVOutput).Value == st, st
		case KVPut:
			return true, in.Value
		case KVAppend:
			return true, st + in.Value
		default:
			return false, st
		}
	},
	Equal: func(a, b interface{}) bool { return a.(string) == b.(string) },
	DescribeOperation: func(input, output interface{}) string {
		in := input.(KVInput)
		switch in.Op {
		case KVGet:
			return fmt.Sprintf("get(%q) -> %q", in.Key, output.(KVOutput).Value)
		case KVPut:
			return fmt.Sprintf("put(%q, %q)", in.Key, in.Value)
		case KVAppend:
			return fmt.Sprintf("append(%q, %q)", in.Key, in.Value)
		default:
			return "unknown"
		}
	},
	DescribeState: func(state interface{}) string {
		return fmt.Sprintf("%q", state.(string))
	},
}

// History records operation invocations and responses for linearizability
// checking. Timestamps are nanoseconds from the history's creation, taken
// from a monotonic clock.
type History struct {
	start time.Time

	mu  sync.Mutex
	ops []porcupine.Operation
}

// NewHistory returns an empty History whose clock starts now.
func NewHistory() *History {
	return &History{start: time.Now()}
}

// Now returns the current timestamp, to be passed back to Complete or
// Pending as the operation's invocation time. Call it immediately before
// invoking the operation.
func (h *History) Now() int64 {
	return int64(time.Since(h.start))
}

// Complete records an operation that returned a definite result.
func (h *History) Complete(clientID int, call int64, in KVInput, out KVOutput) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.ops = append(h.ops, porcupine.Operation{
		ClientId: clientID,
		Input:    in,
		Call:     call,
		Output:   out,
		Return:   int64(time.Since(h.start)),
	})
}

// Pending records an operation whose outcome is unknown — it timed out or
// errored, so it may or may not have taken effect, possibly at any later
// point. It is recorded as never returning, which is exactly what lets the
// checker consider both possibilities. Only writes should be recorded this
// way: a read that never returned tells us nothing and has no effect, so
// it should simply be dropped.
func (h *History) Pending(clientID int, call int64, in KVInput) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.ops = append(h.ops, porcupine.Operation{
		ClientId: clientID,
		Input:    in,
		Call:     call,
		Output:   KVOutput{},
		Return:   math.MaxInt64,
	})
}

// Len returns how many operations have been recorded.
func (h *History) Len() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.ops)
}

// Operations returns a copy of the recorded history.
func (h *History) Operations() []porcupine.Operation {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]porcupine.Operation(nil), h.ops...)
}

// Check verifies the recorded history against KVModel, giving the checker
// at most timeout to decide.
func (h *History) Check(timeout time.Duration) porcupine.CheckResult {
	return porcupine.CheckOperationsTimeout(KVModel, h.Operations(), timeout)
}
