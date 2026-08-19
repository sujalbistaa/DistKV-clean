package kv_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/sujalbistaa/DistKV/internal/kv"
	pb "github.com/sujalbistaa/DistKV/proto/distkvpb"
)

func encode(t *testing.T, cmd *pb.Command) []byte {
	t.Helper()
	b, err := proto.Marshal(cmd)
	require.NoError(t, err)
	return b
}

func TestStoreAppliesEachOperation(t *testing.T) {
	s := kv.NewStore()

	_, reply := s.Apply(encode(t, &pb.Command{ClientId: "c", SeqNo: 1, Op: pb.Op_OP_PUT, Key: "k", Value: "hello"}))
	require.True(t, reply.Success)

	_, reply = s.Apply(encode(t, &pb.Command{ClientId: "c", SeqNo: 2, Op: pb.Op_OP_APPEND, Key: "k", Value: " world"}))
	require.True(t, reply.Success)

	_, reply = s.Apply(encode(t, &pb.Command{ClientId: "c", SeqNo: 3, Op: pb.Op_OP_GET, Key: "k"}))
	require.True(t, reply.Success)
	require.True(t, reply.Found)
	require.Equal(t, "hello world", reply.Value)

	_, reply = s.Apply(encode(t, &pb.Command{ClientId: "c", SeqNo: 4, Op: pb.Op_OP_DELETE, Key: "k"}))
	require.True(t, reply.Success)

	_, reply = s.Apply(encode(t, &pb.Command{ClientId: "c", SeqNo: 5, Op: pb.Op_OP_GET, Key: "k"}))
	require.True(t, reply.Success)
	require.False(t, reply.Found)
	require.Equal(t, "", reply.Value)
}

// TestStoreDeduplicatesRetriedAppend is the core exactly-once property:
// applying the identical (client id, sequence number) twice must not
// append twice.
func TestStoreDeduplicatesRetriedAppend(t *testing.T) {
	s := kv.NewStore()
	cmd := &pb.Command{ClientId: "c", SeqNo: 1, Op: pb.Op_OP_APPEND, Key: "k", Value: "x"}

	_, first := s.Apply(encode(t, cmd))
	require.True(t, first.Success)
	_, second := s.Apply(encode(t, cmd))
	require.True(t, second.Success)

	_, got := s.Apply(encode(t, &pb.Command{ClientId: "reader", SeqNo: 1, Op: pb.Op_OP_GET, Key: "k"}))
	require.Equal(t, "x", got.Value, "a retried append must apply only once")
}

// TestStoreDeduplicationIsPerClient: two different clients using the same
// sequence numbers must not shadow each other.
func TestStoreDeduplicationIsPerClient(t *testing.T) {
	s := kv.NewStore()
	s.Apply(encode(t, &pb.Command{ClientId: "c1", SeqNo: 1, Op: pb.Op_OP_APPEND, Key: "k", Value: "a"}))
	s.Apply(encode(t, &pb.Command{ClientId: "c2", SeqNo: 1, Op: pb.Op_OP_APPEND, Key: "k", Value: "b"}))

	_, got := s.Apply(encode(t, &pb.Command{ClientId: "reader", SeqNo: 1, Op: pb.Op_OP_GET, Key: "k"}))
	require.Equal(t, "ab", got.Value)
}

// TestStoreGetsAreNotDeduplicated: reads carry a sequence number for
// identification but must never be answered from the dedup cache, or a
// client reusing a sequence number would see a stale value.
func TestStoreGetsAreNotDeduplicated(t *testing.T) {
	s := kv.NewStore()
	s.Apply(encode(t, &pb.Command{ClientId: "c", SeqNo: 1, Op: pb.Op_OP_PUT, Key: "k", Value: "v1"}))

	_, first := s.Apply(encode(t, &pb.Command{ClientId: "reader", SeqNo: 1, Op: pb.Op_OP_GET, Key: "k"}))
	require.Equal(t, "v1", first.Value)

	s.Apply(encode(t, &pb.Command{ClientId: "c", SeqNo: 2, Op: pb.Op_OP_PUT, Key: "k", Value: "v2"}))

	_, second := s.Apply(encode(t, &pb.Command{ClientId: "reader", SeqNo: 1, Op: pb.Op_OP_GET, Key: "k"}))
	require.Equal(t, "v2", second.Value, "a repeated read must see the newest value, not a cached reply")
}

// TestStoreSnapshotRestoreRoundTrip: a restored store must be
// indistinguishable from the original, including its dedup state — losing
// that would let an in-flight retry apply a second time after a snapshot
// install.
func TestStoreSnapshotRestoreRoundTrip(t *testing.T) {
	original := kv.NewStore()
	original.Apply(encode(t, &pb.Command{ClientId: "c", SeqNo: 1, Op: pb.Op_OP_PUT, Key: "a", Value: "1"}))
	original.Apply(encode(t, &pb.Command{ClientId: "c", SeqNo: 2, Op: pb.Op_OP_APPEND, Key: "b", Value: "2"}))

	blob, err := original.Snapshot()
	require.NoError(t, err)

	restored := kv.NewStore()
	require.NoError(t, restored.Restore(blob))

	_, a := restored.Apply(encode(t, &pb.Command{ClientId: "reader", SeqNo: 1, Op: pb.Op_OP_GET, Key: "a"}))
	require.Equal(t, "1", a.Value)
	_, b := restored.Apply(encode(t, &pb.Command{ClientId: "reader", SeqNo: 2, Op: pb.Op_OP_GET, Key: "b"}))
	require.Equal(t, "2", b.Value)

	// The dedup record survived: replaying client c's seq 2 must not
	// append a second time.
	restored.Apply(encode(t, &pb.Command{ClientId: "c", SeqNo: 2, Op: pb.Op_OP_APPEND, Key: "b", Value: "2"}))
	_, bAgain := restored.Apply(encode(t, &pb.Command{ClientId: "reader", SeqNo: 3, Op: pb.Op_OP_GET, Key: "b"}))
	require.Equal(t, "2", bAgain.Value, "dedup state must survive snapshot/restore")
}

func TestStoreRejectsUndecodableCommand(t *testing.T) {
	s := kv.NewStore()
	_, reply := s.Apply([]byte{0xFF, 0xFF, 0xFF, 0xFF})
	require.False(t, reply.Success)
	require.NotEmpty(t, reply.Error)
}
