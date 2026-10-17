/*
Copyright 2026 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/wal/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore/inmemorystorage"
	"github.com/gke-labs/in-cluster-storage/pkg/wal"
	"github.com/gke-labs/in-cluster-storage/pkg/wal/buffer"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type testServerHandle struct {
	srv        *buffer.Server
	addr       string
	grpcServer *grpc.Server
	listener   net.Listener
}

func (h *testServerHandle) StopGraceful() {
	h.grpcServer.Stop()
	_ = h.srv.Close()
	_ = h.listener.Close()
}

// StopWithoutClose stops the gRPC listener and server WITHOUT calling srv.Close(),
// simulating an ungraceful crash where no final flush occurs.
func (h *testServerHandle) StopWithoutClose() {
	h.grpcServer.Stop()
	_ = h.listener.Close()
}

func startBufferServer(t *testing.T, backend objectstore.Backend, dataDir string) *testServerHandle {
	return startBufferServerOnAddr(t, backend, dataDir, "127.0.0.1:0")
}

func startBufferServerOnAddr(t *testing.T, backend objectstore.Backend, dataDir string, addr string) *testServerHandle {
	ctx := t.Context()
	srv, err := buffer.NewServer(ctx, buffer.ServerConfig{
		Backend:       backend,
		DataDir:       dataDir,
		FlushInterval: 10 * time.Second,
		BatchMaxDelay: 2 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to start buffer server: %v", err)
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("failed to listen on %s: %v", addr, err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterWalBufferServer(grpcServer, srv)

	go func() {
		_ = grpcServer.Serve(listener)
	}()

	return &testServerHandle{
		srv:        srv,
		addr:       listener.Addr().String(),
		grpcServer: grpcServer,
		listener:   listener,
	}
}

// 1. Append, witness ack, then permanent ack after Flush; local file deleted only after permanent ack.
func TestAppendWitnessAndPermanentAck(t *testing.T) {
	backend := inmemorystorage.New()
	handle := startBufferServer(t, backend, t.TempDir())
	defer handle.StopGraceful()

	clientDir := t.TempDir()
	streamID := uuid.New()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	// Use small maxSegmentSize so segments rotate quickly
	stream, err := Open(ctx, clientDir, streamID, handle.addr, WithMaxSegmentSize(100))
	if err != nil {
		t.Fatalf("failed to open client stream: %v", err)
	}
	defer stream.Close()

	// Append 10 records
	for i := 1; i <= 10; i++ {
		seq, err := stream.Append(ctx, []byte(fmt.Sprintf("data-%d", i)))
		if err != nil {
			t.Fatalf("failed to append record %d: %v", i, err)
		}
		if err := stream.Wait(ctx, seq, Witness, false); err != nil {
			t.Fatalf("failed waiting for witness ack on %d: %v", seq, err)
		}
	}

	local, witness, permanent := stream.Watermarks()
	if local != 10 || witness != 10 {
		t.Fatalf("expected local=10, witness=10, got local=%d, witness=%d, permanent=%d", local, witness, permanent)
	}

	// Verify local files exist before flush
	filesBefore, _ := os.ReadDir(clientDir)
	if len(filesBefore) == 0 {
		t.Fatalf("expected local segment files to exist before permanent flush")
	}

	// Flush to permanent storage
	if err := stream.Flush(ctx); err != nil {
		t.Fatalf("failed to flush to permanent storage: %v", err)
	}

	local, witness, permanent = stream.Watermarks()
	if permanent != 10 {
		t.Fatalf("expected permanent=10 after flush, got %d", permanent)
	}

	// Check that inactive permanent-acked segment files are deleted
	time.Sleep(50 * time.Millisecond)
	filesAfter, _ := os.ReadDir(clientDir)
	if len(filesAfter) > 1 {
		t.Errorf("expected only active segment file to remain after permanent ack, got %d files", len(filesAfter))
	}
}

// 2. Buffer service restarted (ungracefully without flush) with empty data dir:
// Client reconnects, replays un-permanently-acked records, restarted buffer assigns positions starting at last_position + 1,
// all witness-acked records reappear via Tail, and a Tail resumed from a provisional cursor is clamped and reports resumed_from.
func TestBufferServiceRestartWithoutClose(t *testing.T) {
	backend := inmemorystorage.New()
	handle1 := startBufferServer(t, backend, t.TempDir())

	clientDir := t.TempDir()
	streamID := uuid.New()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	stream, err := Open(ctx, clientDir, streamID, handle1.addr)
	if err != nil {
		t.Fatalf("failed to open stream: %v", err)
	}
	defer stream.Close()

	// Append 5 records and flush to permanent storage
	for i := 1; i <= 5; i++ {
		seq, err := stream.Append(ctx, []byte(fmt.Sprintf("record-%d", i)))
		if err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
		if err := stream.Wait(ctx, seq, Witness, false); err != nil {
			t.Fatalf("wait witness %d failed: %v", seq, err)
		}
	}
	if err := stream.Flush(ctx); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	// Append 5 more records (witness only, NOT flushed to permanent storage)
	for i := 6; i <= 10; i++ {
		seq, err := stream.Append(ctx, []byte(fmt.Sprintf("record-%d", i)))
		if err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
		if err := stream.Wait(ctx, seq, Witness, false); err != nil {
			t.Fatalf("wait witness %d failed: %v", seq, err)
		}
	}

	// Crash stop buffer service WITHOUT Close() (no graceful flush!)
	handle1.StopWithoutClose()

	// Restart buffer service with empty data dir
	handle2 := startBufferServer(t, backend, t.TempDir())
	defer handle2.StopGraceful()

	// Check that restarted server position allocation starts at last_position + 1 (5 + 1 = 6)
	if handle2.srv.LastPosition() != 5 {
		t.Fatalf("expected restarted server last_position to be 5, got %d", handle2.srv.LastPosition())
	}

	// Connect client to new server
	stream2, err := Open(ctx, clientDir, streamID, handle2.addr)
	if err != nil {
		t.Fatalf("failed to reopen client stream: %v", err)
	}
	defer stream2.Close()

	// Wait for client to replay un-permanently-acked records (6..10) and receive witness acks
	if err := stream2.Wait(ctx, 10, Witness, false); err != nil {
		t.Fatalf("failed waiting for witness ack after restart: %v", err)
	}

	// 1. Tail from position 1
	conn, err := grpc.NewClient(handle2.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	client := pb.NewWalBufferClient(conn)
	tailCtx, tailCancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer tailCancel()

	tailStream, err := client.Tail(tailCtx, &pb.TailRequest{FromPosition: 1})
	if err != nil {
		t.Fatalf("tail failed: %v", err)
	}

	var tailedRecords []*pb.LogRecord
	positionsSeen := make(map[uint64]bool)

	for i := 1; i <= 10; i++ {
		resp, err := tailStream.Recv()
		if err != nil {
			t.Fatalf("tail recv %d failed: %v", i, err)
		}
		if i == 1 && resp.ResumedFrom != 1 {
			t.Errorf("expected ResumedFrom 1 on first response, got %d", resp.ResumedFrom)
		}
		if positionsSeen[resp.Record.Position] {
			t.Fatalf("duplicate position %d detected in Tail!", resp.Record.Position)
		}
		positionsSeen[resp.Record.Position] = true
		tailedRecords = append(tailedRecords, resp.Record)
	}

	// Check that all 10 stream records are returned
	streamSeqsSeen := make(map[uint64]bool)
	for _, r := range tailedRecords {
		streamSeqsSeen[r.StreamSeq] = true
	}
	for i := uint64(1); i <= 10; i++ {
		if !streamSeqsSeen[i] {
			t.Errorf("expected stream_seq %d in tailed records, but was missing", i)
		}
	}

	// 2. Tail with a provisional cursor (e.g. from_position=11, above last_position+1=6)
	// Must be clamped to 6 and report resumed_from=6
	tailStreamClamped, err := client.Tail(tailCtx, &pb.TailRequest{FromPosition: 11})
	if err != nil {
		t.Fatalf("tail clamped failed: %v", err)
	}
	firstResp, err := tailStreamClamped.Recv()
	if err != nil {
		t.Fatalf("tail clamped recv failed: %v", err)
	}
	if firstResp.ResumedFrom != 6 {
		t.Errorf("expected resumed_from clamped to 6, got %d", firstResp.ResumedFrom)
	}
	if firstResp.Record.Position != 6 {
		t.Errorf("expected first record at position 6, got %d", firstResp.Record.Position)
	}
}

// 3. A consumer tails through a crash, keeps a (stream_id -> max stream_seq) map,
// reconnects with its old cursor, and ends up with exactly one copy of every record.
func TestConsumerTailThroughCrashWithDedup(t *testing.T) {
	backend := inmemorystorage.New()
	handle1 := startBufferServer(t, backend, t.TempDir())

	clientDir := t.TempDir()
	streamID := uuid.New()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	stream, err := Open(ctx, clientDir, streamID, handle1.addr)
	if err != nil {
		t.Fatalf("failed to open stream: %v", err)
	}
	defer stream.Close()

	// Append 5 records and flush to permanent storage
	for i := 1; i <= 5; i++ {
		seq, err := stream.Append(ctx, []byte(fmt.Sprintf("record-%d", i)))
		if err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
		if err := stream.Wait(ctx, seq, Witness, false); err != nil {
			t.Fatalf("wait witness %d failed: %v", seq, err)
		}
	}
	if err := stream.Flush(ctx); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	// Append 5 more records (witness only, unflushed)
	for i := 6; i <= 10; i++ {
		seq, err := stream.Append(ctx, []byte(fmt.Sprintf("record-%d", i)))
		if err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
		if err := stream.Wait(ctx, seq, Witness, false); err != nil {
			t.Fatalf("wait witness %d failed: %v", seq, err)
		}
	}

	// Consumer tails all 10 records from incarnation 1
	conn1, err := grpc.NewClient(handle1.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn1.Close()

	client1 := pb.NewWalBufferClient(conn1)
	tailStream1, err := client1.Tail(ctx, &pb.TailRequest{FromPosition: 1})
	if err != nil {
		t.Fatalf("tail failed: %v", err)
	}

	var consumerReceivedRecords []*pb.LogRecord
	streamMaxSeq := make(map[string]uint64)
	var lastSeenPosition uint64

	for i := 1; i <= 10; i++ {
		resp, err := tailStream1.Recv()
		if err != nil {
			t.Fatalf("recv %d failed: %v", i, err)
		}
		rec := resp.Record
		lastSeenPosition = rec.Position
		sid := uuid.UUID(rec.StreamId).String()
		if rec.StreamSeq > streamMaxSeq[sid] {
			streamMaxSeq[sid] = rec.StreamSeq
			consumerReceivedRecords = append(consumerReceivedRecords, rec)
		}
	}

	// Consumer cursor is now lastSeenPosition + 1 = 11 (pointing into provisional range of incarnation 1)
	consumerCursor := lastSeenPosition + 1

	// Crash stop server 1
	handle1.StopWithoutClose()

	// Start server 2 (incarnation 2)
	handle2 := startBufferServer(t, backend, t.TempDir())
	defer handle2.StopGraceful()

	// Client reconnects to server 2 and replays unflushed records 6..10
	stream2, err := Open(ctx, clientDir, streamID, handle2.addr)
	if err != nil {
		t.Fatalf("failed to reopen client stream: %v", err)
	}
	defer stream2.Close()

	if err := stream2.Wait(ctx, 10, Witness, false); err != nil {
		t.Fatalf("failed waiting for witness ack on restart: %v", err)
	}

	// Consumer reconnects to server 2 using its old cursor (11)
	conn2, err := grpc.NewClient(handle2.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial 2 failed: %v", err)
	}
	defer conn2.Close()

	client2 := pb.NewWalBufferClient(conn2)
	tailStream2, err := client2.Tail(ctx, &pb.TailRequest{FromPosition: consumerCursor})
	if err != nil {
		t.Fatalf("tail 2 failed: %v", err)
	}

	// Server 2 clamps cursor 11 to last_flushed_position + 1 = 6, returning resumed_from = 6
	// and re-delivers records 6..10.
	var redeliveredCount int
	for i := 1; i <= 5; i++ {
		resp, err := tailStream2.Recv()
		if err != nil {
			t.Fatalf("recv on reconnect %d failed: %v", i, err)
		}
		if i == 1 && resp.ResumedFrom != 6 {
			t.Errorf("expected resumed_from clamped to 6, got %d", resp.ResumedFrom)
		}
		rec := resp.Record
		sid := uuid.UUID(rec.StreamId).String()
		if rec.StreamSeq > streamMaxSeq[sid] {
			streamMaxSeq[sid] = rec.StreamSeq
			consumerReceivedRecords = append(consumerReceivedRecords, rec)
		} else {
			redeliveredCount++
		}
	}

	if redeliveredCount != 5 {
		t.Errorf("expected 5 redelivered duplicate records, got %d", redeliveredCount)
	}

	// Verify consumer has exactly 10 unique records in order 1..10
	if len(consumerReceivedRecords) != 10 {
		t.Fatalf("expected exactly 10 deduplicated records, got %d", len(consumerReceivedRecords))
	}
	for i, rec := range consumerReceivedRecords {
		expectedSeq := uint64(i + 1)
		if rec.StreamSeq != expectedSeq {
			t.Errorf("record %d: expected stream_seq %d, got %d", i, expectedSeq, rec.StreamSeq)
		}
	}
}

// 4. Client restarted mid-stream: retained records replayed, duplicates dropped, no gaps in stream_seq.
func TestClientRestartMidStream(t *testing.T) {
	backend := inmemorystorage.New()
	handle := startBufferServer(t, backend, t.TempDir())
	defer handle.StopGraceful()

	clientDir := t.TempDir()
	streamID := uuid.New()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	// Client 1 appends 5 records
	stream1, err := Open(ctx, clientDir, streamID, handle.addr)
	if err != nil {
		t.Fatalf("failed to open stream1: %v", err)
	}
	for i := 1; i <= 5; i++ {
		seq, err := stream1.Append(ctx, []byte(fmt.Sprintf("msg-%d", i)))
		if err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
		_ = stream1.Wait(ctx, seq, Witness, false)
	}
	_ = stream1.Close()

	// Client 2 reopens same directory and continues appending records 6..10
	stream2, err := Open(ctx, clientDir, streamID, handle.addr)
	if err != nil {
		t.Fatalf("failed to reopen stream2: %v", err)
	}
	defer stream2.Close()

	local, _, _ := stream2.Watermarks()
	if local != 5 {
		t.Fatalf("expected recovered local watermark 5, got %d", local)
	}

	for i := 6; i <= 10; i++ {
		seq, err := stream2.Append(ctx, []byte(fmt.Sprintf("msg-%d", i)))
		if err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
		if err := stream2.Wait(ctx, seq, Witness, false); err != nil {
			t.Fatalf("wait witness %d failed: %v", seq, err)
		}
		if seq != uint64(i) {
			t.Errorf("expected stream_seq %d, got %d (gap detected!)", i, seq)
		}
	}
}

// 4. Two clients interleaved: Tail order is strictly monotonic in position.
func TestTwoClientsInterleaved(t *testing.T) {
	backend := inmemorystorage.New()
	handle := startBufferServer(t, backend, t.TempDir())
	defer handle.StopGraceful()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	stream1, err := Open(ctx, t.TempDir(), uuid.New(), handle.addr)
	if err != nil {
		t.Fatalf("open stream1 failed: %v", err)
	}
	defer stream1.Close()

	stream2, err := Open(ctx, t.TempDir(), uuid.New(), handle.addr)
	if err != nil {
		t.Fatalf("open stream2 failed: %v", err)
	}
	defer stream2.Close()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 1; i <= 10; i++ {
			seq, err := stream1.Append(ctx, []byte(fmt.Sprintf("c1-%d", i)))
			if err == nil {
				_ = stream1.Wait(ctx, seq, Witness, false)
			}
			time.Sleep(1 * time.Millisecond)
		}
	}()

	go func() {
		defer wg.Done()
		for i := 1; i <= 10; i++ {
			seq, err := stream2.Append(ctx, []byte(fmt.Sprintf("c2-%d", i)))
			if err == nil {
				_ = stream2.Wait(ctx, seq, Witness, false)
			}
			time.Sleep(1 * time.Millisecond)
		}
	}()

	wg.Wait()

	// Verify Tail returns 20 records in strictly increasing position
	conn, err := grpc.NewClient(handle.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	client := pb.NewWalBufferClient(conn)
	tailCtx, tailCancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer tailCancel()

	tailStream, err := client.Tail(tailCtx, &pb.TailRequest{FromPosition: 1})
	if err != nil {
		t.Fatalf("tail failed: %v", err)
	}

	var lastPos uint64 = 0
	for i := 1; i <= 20; i++ {
		resp, err := tailStream.Recv()
		if err != nil {
			t.Fatalf("tail recv %d failed: %v", i, err)
		}
		if resp.Record.Position <= lastPos {
			t.Fatalf("position not strictly increasing: prev=%d, curr=%d", lastPos, resp.Record.Position)
		}
		lastPos = resp.Record.Position
	}
}

// 5. Service unreachable after connect: Append keeps succeeding until the retained cap, then blocks.
func TestServiceUnreachableBackpressure(t *testing.T) {
	backend := inmemorystorage.New()
	handle := startBufferServer(t, backend, t.TempDir())

	clientDir := t.TempDir()
	streamID := uuid.New()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	// Set small maxRetainedBytes (e.g. 150 bytes, fits ~2 small records)
	stream, err := Open(ctx, clientDir, streamID, handle.addr, WithMaxRetainedBytes(150))
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}
	defer stream.Close()

	// Stop service so it is unreachable for network replication
	handle.StopGraceful()

	// Appends should succeed locally up to cap
	for i := 1; i <= 2; i++ {
		_, err := stream.Append(ctx, []byte("data"))
		if err != nil {
			t.Fatalf("append %d should succeed locally: %v", i, err)
		}
	}

	// Next append should block due to cap
	blockedCtx, blockedCancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer blockedCancel()

	_, err = stream.Append(blockedCtx, []byte("large-payload-that-exceeds-retained-cap-bytes"))
	if err == nil {
		t.Fatalf("expected append to block due to retained cap")
	}
}

// 6. Invalid level in Wait returns error.
func TestInvalidWaitLevel(t *testing.T) {
	clientDir := t.TempDir()
	streamID := uuid.New()
	ctx := t.Context()

	stream, err := Open(ctx, clientDir, streamID, "")
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}
	defer stream.Close()

	seq, err := stream.Append(ctx, []byte("msg"))
	if err != nil {
		t.Fatalf("append failed: %v", err)
	}

	err = stream.Wait(ctx, seq, Level(999), false)
	if err == nil {
		t.Fatalf("expected error on invalid level, got nil")
	}
}

// 7. Wait(Permanent, requestFlush=true) waits for witness first so Flush flushes the newly appended records.
func TestWaitPermanentFlushesAfterWitness(t *testing.T) {
	backend := inmemorystorage.New()
	handle := startBufferServer(t, backend, t.TempDir()) // 10s flush interval
	defer handle.StopGraceful()

	clientDir := t.TempDir()
	streamID := uuid.New()

	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	stream, err := Open(ctx, clientDir, streamID, handle.addr)
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}
	defer stream.Close()

	// Append 5 records without waiting for witness individually
	for i := 1; i <= 5; i++ {
		if _, err := stream.Append(ctx, []byte(fmt.Sprintf("msg-%d", i))); err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
	}

	// Immediately wait for Permanent with requestFlush=true. Must complete well before 10s periodic flush.
	if err := stream.Wait(ctx, 5, Permanent, true); err != nil {
		t.Fatalf("Wait(Permanent) did not complete before periodic flush: %v", err)
	}

	_, _, permanent := stream.Watermarks()
	if permanent != 5 {
		t.Fatalf("expected permanent watermark 5, got %d", permanent)
	}
}

// 8. Stream.Flush() waits for witness first before issuing Flush RPC.
func TestFlushFlushesAfterWitness(t *testing.T) {
	backend := inmemorystorage.New()
	handle := startBufferServer(t, backend, t.TempDir()) // 10s flush interval
	defer handle.StopGraceful()

	clientDir := t.TempDir()
	streamID := uuid.New()

	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	stream, err := Open(ctx, clientDir, streamID, handle.addr)
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}
	defer stream.Close()

	// Append 5 records without waiting for witness individually
	for i := 1; i <= 5; i++ {
		if _, err := stream.Append(ctx, []byte(fmt.Sprintf("msg-%d", i))); err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
	}

	// Immediately call Flush. Must complete well before 10s periodic flush.
	if err := stream.Flush(ctx); err != nil {
		t.Fatalf("Flush() did not complete before periodic flush: %v", err)
	}

	_, _, permanent := stream.Watermarks()
	if permanent != 5 {
		t.Fatalf("expected permanent watermark 5, got %d", permanent)
	}
}

// 9. Two servers in one process: a flush on server A must not produce an ack on server B's stream.
func TestTwoServersFlushIsolation(t *testing.T) {
	backendA := inmemorystorage.New()
	handleA := startBufferServer(t, backendA, t.TempDir())
	defer handleA.StopGraceful()

	backendB := inmemorystorage.New()
	handleB := startBufferServer(t, backendB, t.TempDir())
	defer handleB.StopGraceful()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	streamA, err := Open(ctx, t.TempDir(), uuid.New(), handleA.addr)
	if err != nil {
		t.Fatalf("failed to open stream A: %v", err)
	}
	defer streamA.Close()

	streamB, err := Open(ctx, t.TempDir(), uuid.New(), handleB.addr)
	if err != nil {
		t.Fatalf("failed to open stream B: %v", err)
	}
	defer streamB.Close()

	seqA, err := streamA.Append(ctx, []byte("dataA"))
	if err != nil {
		t.Fatalf("append A failed: %v", err)
	}
	if err := streamA.Wait(ctx, seqA, Witness, false); err != nil {
		t.Fatalf("wait witness A failed: %v", err)
	}

	seqB, err := streamB.Append(ctx, []byte("dataB"))
	if err != nil {
		t.Fatalf("append B failed: %v", err)
	}
	if err := streamB.Wait(ctx, seqB, Witness, false); err != nil {
		t.Fatalf("wait witness B failed: %v", err)
	}

	// Flush server A
	if err := streamA.Flush(ctx); err != nil {
		t.Fatalf("flush A failed: %v", err)
	}

	_, _, permA := streamA.Watermarks()
	if permA != 1 {
		t.Fatalf("expected permanent watermark 1 on stream A, got %d", permA)
	}

	// Give time for any unexpected notification to arrive on stream B
	time.Sleep(100 * time.Millisecond)

	_, _, permB := streamB.Watermarks()
	if permB != 0 {
		t.Fatalf("expected permanent watermark 0 on stream B after server A flush, got %d", permB)
	}
}

// 10. Two streams on one server: a flush containing records from only one advances only that stream's permanent watermark.
func TestTwoStreamsOneServerFlushIsolation(t *testing.T) {
	backend := inmemorystorage.New()
	handle := startBufferServer(t, backend, t.TempDir())
	defer handle.StopGraceful()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	stream1, err := Open(ctx, t.TempDir(), uuid.New(), handle.addr)
	if err != nil {
		t.Fatalf("failed to open stream 1: %v", err)
	}
	defer stream1.Close()

	stream2, err := Open(ctx, t.TempDir(), uuid.New(), handle.addr)
	if err != nil {
		t.Fatalf("failed to open stream 2: %v", err)
	}
	defer stream2.Close()

	// Initial append on both and flush both
	seq1, _ := stream1.Append(ctx, []byte("s1-1"))
	_ = stream1.Wait(ctx, seq1, Witness, false)
	seq2, _ := stream2.Append(ctx, []byte("s2-1"))
	_ = stream2.Wait(ctx, seq2, Witness, false)

	if err := stream1.Flush(ctx); err != nil {
		t.Fatalf("initial flush failed: %v", err)
	}
	_ = stream2.Wait(ctx, seq2, Permanent, false)

	_, _, perm1 := stream1.Watermarks()
	_, _, perm2 := stream2.Watermarks()
	if perm1 != 1 || perm2 != 1 {
		t.Fatalf("expected perm1=1, perm2=1; got perm1=%d, perm2=%d", perm1, perm2)
	}

	// Append record 2 only to stream 1
	seq1_2, err := stream1.Append(ctx, []byte("s1-2"))
	if err != nil {
		t.Fatalf("append s1-2 failed: %v", err)
	}
	if err := stream1.Wait(ctx, seq1_2, Witness, false); err != nil {
		t.Fatalf("wait witness s1-2 failed: %v", err)
	}

	// Flush server (only stream 1 has unflushed records)
	if err := stream1.Flush(ctx); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	_, _, perm1After := stream1.Watermarks()
	if perm1After != 2 {
		t.Fatalf("expected perm1=2 after flush, got %d", perm1After)
	}

	time.Sleep(100 * time.Millisecond)

	_, _, perm2After := stream2.Watermarks()
	if perm2After != 1 {
		t.Fatalf("expected perm2=1 to remain unchanged, got %d", perm2After)
	}
}

// 12. Target-less stream fast failure on Witness or Permanent durability.
func TestTargetlessStreamDurabilityFastFail(t *testing.T) {
	clientDir := t.TempDir()
	streamID := uuid.New()
	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()

	stream, err := Open(ctx, clientDir, streamID, "")
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}
	defer stream.Close()

	if stream.ReplicationLevel() != Local {
		t.Fatalf("expected ReplicationLevel() to be Local for target-less stream, got %v", stream.ReplicationLevel())
	}

	seq, err := stream.Append(ctx, []byte("test-data"))
	if err != nil {
		t.Fatalf("append failed: %v", err)
	}

	// Local durability should succeed immediately
	if err := stream.Wait(ctx, seq, Local, false); err != nil {
		t.Fatalf("Wait(Local) failed: %v", err)
	}

	// Witness durability should fail fast with ErrDurabilityUnavailable
	err = stream.Wait(ctx, seq, Witness, false)
	if !errors.Is(err, ErrDurabilityUnavailable) {
		t.Fatalf("expected ErrDurabilityUnavailable for Wait(Witness), got: %v", err)
	}

	// Permanent durability should fail fast with ErrDurabilityUnavailable
	err = stream.Wait(ctx, seq, Permanent, false)
	if !errors.Is(err, ErrDurabilityUnavailable) {
		t.Fatalf("expected ErrDurabilityUnavailable for Wait(Permanent, false), got: %v", err)
	}

	err = stream.Wait(ctx, seq, Permanent, true)
	if !errors.Is(err, ErrDurabilityUnavailable) {
		t.Fatalf("expected ErrDurabilityUnavailable for Wait(Permanent, true), got: %v", err)
	}

	// Flush should fail fast with ErrDurabilityUnavailable
	err = stream.Flush(ctx)
	if !errors.Is(err, ErrDurabilityUnavailable) {
		t.Fatalf("expected ErrDurabilityUnavailable for Flush(), got: %v", err)
	}
}

// 13. TailStream helper: tails a specific stream with from_stream_seq across segment boundaries,
// local/object-storage boundary, and buffer restart with interleaved streams.
func TestTailStreamHelper(t *testing.T) {
	backend := inmemorystorage.New()
	handle := startBufferServer(t, backend, t.TempDir())

	clientDir1 := t.TempDir()
	clientDir2 := t.TempDir()
	streamID1 := uuid.New()
	streamID2 := uuid.New()

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	stream1, err := Open(ctx, clientDir1, streamID1, handle.addr)
	if err != nil {
		t.Fatalf("open stream1 failed: %v", err)
	}
	defer stream1.Close()

	stream2, err := Open(ctx, clientDir2, streamID2, handle.addr)
	if err != nil {
		t.Fatalf("open stream2 failed: %v", err)
	}
	defer stream2.Close()

	// 1. Interleaved appends seq 1..10 on both streams, then flush (segment 1 in object storage)
	for i := 1; i <= 10; i++ {
		seq1, _ := stream1.Append(ctx, []byte(fmt.Sprintf("s1-msg-%d", i)))
		_ = stream1.Wait(ctx, seq1, Witness, false)
		seq2, _ := stream2.Append(ctx, []byte(fmt.Sprintf("s2-msg-%d", i)))
		_ = stream2.Wait(ctx, seq2, Witness, false)
	}
	if err := stream1.Flush(ctx); err != nil {
		t.Fatalf("flush 1 failed: %v", err)
	}

	// 2. Interleaved appends seq 11..15 on both streams, then flush (segment 2 in object storage)
	for i := 11; i <= 15; i++ {
		seq1, _ := stream1.Append(ctx, []byte(fmt.Sprintf("s1-msg-%d", i)))
		_ = stream1.Wait(ctx, seq1, Witness, false)
		seq2, _ := stream2.Append(ctx, []byte(fmt.Sprintf("s2-msg-%d", i)))
		_ = stream2.Wait(ctx, seq2, Witness, false)
	}
	if err := stream1.Flush(ctx); err != nil {
		t.Fatalf("flush 2 failed: %v", err)
	}

	// 3. Interleaved appends seq 16..20 on both streams (unflushed, in local store)
	for i := 16; i <= 20; i++ {
		seq1, _ := stream1.Append(ctx, []byte(fmt.Sprintf("s1-msg-%d", i)))
		_ = stream1.Wait(ctx, seq1, Witness, false)
		seq2, _ := stream2.Append(ctx, []byte(fmt.Sprintf("s2-msg-%d", i)))
		_ = stream2.Wait(ctx, seq2, Witness, false)
	}

	// 4. Use TailStream helper to tail stream 1 starting after seq 7 across segment 1, segment 2, and local store
	streamIter, err := TailStream(ctx, handle.addr, streamID1, 7)
	if err != nil {
		t.Fatalf("TailStream failed: %v", err)
	}

	var stream1Seqs []uint64
	var stream1Payloads []string

	for seq, payload := range streamIter {
		stream1Seqs = append(stream1Seqs, seq)
		stream1Payloads = append(stream1Payloads, string(payload))
		if seq == 20 {
			break // All 13 expected records (8..20) received
		}
	}

	expectedSeqs := []uint64{8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}
	if len(stream1Seqs) != len(expectedSeqs) {
		t.Fatalf("expected %d records, got %d: %v", len(expectedSeqs), len(stream1Seqs), stream1Seqs)
	}
	for i, seq := range stream1Seqs {
		if seq != expectedSeqs[i] {
			t.Errorf("at index %d: expected seq %d, got %d", i, expectedSeqs[i], seq)
		}
		expectedPayload := fmt.Sprintf("s1-msg-%d", seq)
		if stream1Payloads[i] != expectedPayload {
			t.Errorf("at index %d: expected payload %s, got %s", i, expectedPayload, stream1Payloads[i])
		}
	}

	// 5. Append seq 21..25 on stream 1 (unflushed) and simulate ungraceful crash of buffer server
	for i := 21; i <= 25; i++ {
		seq1, _ := stream1.Append(ctx, []byte(fmt.Sprintf("s1-msg-%d", i)))
		_ = stream1.Wait(ctx, seq1, Witness, false)
	}

	handle.StopWithoutClose()

	// Restart buffer server on new empty dataDir
	handle2 := startBufferServer(t, backend, t.TempDir())
	defer handle2.StopGraceful()

	// Reopen client stream 1 to reconnect and replay unflushed records
	stream1Restarted, err := Open(ctx, clientDir1, streamID1, handle2.addr)
	if err != nil {
		t.Fatalf("reopen stream1 failed: %v", err)
	}
	defer stream1Restarted.Close()

	if err := stream1Restarted.Wait(ctx, 25, Witness, false); err != nil {
		t.Fatalf("failed waiting for witness ack after restart: %v", err)
	}

	// 6. TailStream helper across buffer restart from seq 18
	restartIter, err := TailStream(ctx, handle2.addr, streamID1, 18)
	if err != nil {
		t.Fatalf("TailStream after restart failed: %v", err)
	}

	var restartedSeqs []uint64
	for seq, payload := range restartIter {
		restartedSeqs = append(restartedSeqs, seq)
		expectedPayload := fmt.Sprintf("s1-msg-%d", seq)
		if string(payload) != expectedPayload {
			t.Errorf("expected payload %s, got %s", expectedPayload, string(payload))
		}
		if seq == 25 {
			break
		}
	}

	expectedRestartSeqs := []uint64{19, 20, 21, 22, 23, 24, 25}
	if len(restartedSeqs) != len(expectedRestartSeqs) {
		t.Fatalf("expected %d records after restart, got %d: %v", len(expectedRestartSeqs), len(restartedSeqs), restartedSeqs)
	}
	for i, seq := range restartedSeqs {
		if seq != expectedRestartSeqs[i] {
			t.Errorf("after restart index %d: expected seq %d, got %d", i, expectedRestartSeqs[i], seq)
		}
	}
}

// 14. Node move: when opening a stream on a new node (empty directory) against a buffer holding seq 1..5,
// Append immediately without sleep must assign seq 6 and succeed (issue #185 reproduction test).
func TestNewNodeStreamContinuesFromBufferHeadWithoutSleep(t *testing.T) {
	backend := inmemorystorage.New()
	handle := startBufferServer(t, backend, t.TempDir())
	defer handle.StopGraceful()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	streamID := uuid.New()
	clientDir1 := t.TempDir()

	// 1. First client appends 5 records on node 1
	stream1, err := Open(ctx, clientDir1, streamID, handle.addr)
	if err != nil {
		t.Fatalf("open stream1 failed: %v", err)
	}
	defer stream1.Close()

	for i := 1; i <= 5; i++ {
		seq, err := stream1.Append(ctx, []byte(fmt.Sprintf("msg-%d", i)))
		if err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
		if err := stream1.Wait(ctx, seq, Witness, false); err != nil {
			t.Fatalf("wait %d failed: %v", i, err)
		}
	}

	// 2. Second client starts with an empty directory (simulating controller moving to a new node)
	clientDir2 := t.TempDir()
	stream2, err := Open(ctx, clientDir2, streamID, handle.addr)
	if err != nil {
		t.Fatalf("open stream2 failed: %v", err)
	}
	defer stream2.Close()

	// Append immediately with NO sleep: must get seq 6 and witness ack
	seq, err := stream2.Append(ctx, []byte("new-node-write"))
	if err != nil {
		t.Fatalf("Append on stream2 failed: %v", err)
	}
	if seq != 6 {
		t.Fatalf("expected stream2 first append to get seq 6, got %d", seq)
	}
	if err := stream2.Wait(ctx, seq, Witness, false); err != nil {
		t.Fatalf("wait on stream2 seq %d failed: %v", seq, err)
	}

	local, witness, _ := stream2.Watermarks()
	if local != 6 || witness != 6 {
		t.Fatalf("expected stream2 local=6, witness=6, got local=%d, witness=%d", local, witness)
	}
}

// 15. Open a stream on an empty directory against a buffer whose witness watermark is W,
// append, and check the first new record gets seq W+1 and is stored by the buffer.
func TestNewNodeAppendsContinueFromWitnessWatermark(t *testing.T) {
	backend := inmemorystorage.New()
	handle := startBufferServer(t, backend, t.TempDir())
	defer handle.StopGraceful()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	streamID := uuid.New()
	clientDir1 := t.TempDir()

	// Client 1 writes W=10 records
	const W = 10
	stream1, err := Open(ctx, clientDir1, streamID, handle.addr)
	if err != nil {
		t.Fatalf("open stream1 failed: %v", err)
	}
	for i := 1; i <= W; i++ {
		seq, err := stream1.Append(ctx, []byte(fmt.Sprintf("w-data-%d", i)))
		if err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
		if err := stream1.Wait(ctx, seq, Witness, false); err != nil {
			t.Fatalf("wait %d failed: %v", seq, err)
		}
	}
	_ = stream1.Close()

	// Client 2 on new empty directory
	clientDir2 := t.TempDir()
	stream2, err := Open(ctx, clientDir2, streamID, handle.addr)
	if err != nil {
		t.Fatalf("open stream2 failed: %v", err)
	}
	defer stream2.Close()

	newSeq, err := stream2.Append(ctx, []byte("w-data-11"))
	if err != nil {
		t.Fatalf("stream2 append failed: %v", err)
	}
	if newSeq != W+1 {
		t.Fatalf("expected newSeq to be %d, got %d", W+1, newSeq)
	}
	if err := stream2.Wait(ctx, newSeq, Witness, false); err != nil {
		t.Fatalf("stream2 wait for witness ack on %d failed: %v", newSeq, err)
	}

	// Verify via Tail that record W+1 is stored by the buffer
	iter, err := TailStream(ctx, handle.addr, streamID, W)
	if err != nil {
		t.Fatalf("TailStream failed: %v", err)
	}
	found := false
	for seq, payload := range iter {
		if seq == W+1 {
			if string(payload) != "w-data-11" {
				t.Fatalf("expected payload w-data-11, got: %s", string(payload))
			}
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("record %d was not found in buffer TailStream", W+1)
	}
}

// 16. Stale local segments (local records at seqs the buffer holds with different content)
// are discarded rather than resent.
func TestStaleLocalSegmentsDiscardedRatherThanResent(t *testing.T) {
	backend := inmemorystorage.New()
	handle := startBufferServer(t, backend, t.TempDir())
	defer handle.StopGraceful()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	streamID := uuid.New()
	node1Dir := t.TempDir()

	// Node 1 writes records 1..3 locally without remote target
	localStream1, err := Open(ctx, node1Dir, streamID, "")
	if err != nil {
		t.Fatalf("open localStream1 failed: %v", err)
	}
	for i := 1; i <= 3; i++ {
		_, err := localStream1.Append(ctx, []byte(fmt.Sprintf("stale-node1-rec-%d", i)))
		if err != nil {
			t.Fatalf("localStream1 append %d failed: %v", i, err)
		}
	}
	_ = localStream1.Close()

	// Node 2 writes authoritative records 1..5 to wal-buffer
	node2Dir := t.TempDir()
	stream2, err := Open(ctx, node2Dir, streamID, handle.addr)
	if err != nil {
		t.Fatalf("open stream2 failed: %v", err)
	}
	for i := 1; i <= 5; i++ {
		seq, err := stream2.Append(ctx, []byte(fmt.Sprintf("authoritative-rec-%d", i)))
		if err != nil {
			t.Fatalf("stream2 append %d failed: %v", i, err)
		}
		if err := stream2.Wait(ctx, seq, Witness, false); err != nil {
			t.Fatalf("stream2 wait %d failed: %v", seq, err)
		}
	}
	_ = stream2.Close()

	// Node 1 restarts on node1Dir (which has stale records 1..3) against wal-buffer (which has 1..5)
	stream1Restarted, err := Open(ctx, node1Dir, streamID, handle.addr)
	if err != nil {
		t.Fatalf("open stream1Restarted failed: %v", err)
	}
	defer stream1Restarted.Close()

	// Next append on Node 1 must continue from buffer head 5 -> seq 6
	seq6, err := stream1Restarted.Append(ctx, []byte("node1-new-rec-6"))
	if err != nil {
		t.Fatalf("stream1Restarted append failed: %v", err)
	}
	if seq6 != 6 {
		t.Fatalf("expected seq 6, got %d", seq6)
	}
	if err := stream1Restarted.Wait(ctx, seq6, Witness, false); err != nil {
		t.Fatalf("wait for seq 6 failed: %v", err)
	}

	// Tail from buffer: verify records 1..5 have authoritative content, NOT stale content
	tailIter, err := TailStream(ctx, handle.addr, streamID, 0)
	if err != nil {
		t.Fatalf("TailStream failed: %v", err)
	}

	recordsMap := make(map[uint64]string)
	for seq, payload := range tailIter {
		recordsMap[seq] = string(payload)
		if seq == 6 {
			break
		}
	}

	for i := 1; i <= 5; i++ {
		expected := fmt.Sprintf("authoritative-rec-%d", i)
		if recordsMap[uint64(i)] != expected {
			t.Errorf("seq %d: expected %q, got %q", i, expected, recordsMap[uint64(i)])
		}
	}
	if recordsMap[6] != "node1-new-rec-6" {
		t.Errorf("seq 6: expected %q, got %q", "node1-new-rec-6", recordsMap[6])
	}
}

// 17. Divergent local tail: Node A writes 1..5 locally without witness.
// Node B writes 1..2 to buffer (head = 2).
// Controller moves back to Node A (local records 3..5 are above buffer head, but local 2 != buffer 2).
// Node A must discard divergent local records 3..5, continue sequence numbering from 3,
// and not corrupt the buffer stream.
func TestDivergentLocalTailAboveWitnessWatermarkDiscarded(t *testing.T) {
	backend := inmemorystorage.New()
	handle := startBufferServer(t, backend, t.TempDir())
	defer handle.StopGraceful()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	streamID := uuid.New()
	node1Dir := t.TempDir()

	// 1. Node A writes records 1..5 locally (no remote target)
	localStream1, err := Open(ctx, node1Dir, streamID, "")
	if err != nil {
		t.Fatalf("open localStream1 failed: %v", err)
	}
	for i := 1; i <= 5; i++ {
		_, err := localStream1.Append(ctx, []byte(fmt.Sprintf("nodeA-rec-%d", i)))
		if err != nil {
			t.Fatalf("localStream1 append %d failed: %v", i, err)
		}
	}
	_ = localStream1.Close()

	// 2. Node B writes authoritative records 1..2 to wal-buffer (head = 2)
	node2Dir := t.TempDir()
	stream2, err := Open(ctx, node2Dir, streamID, handle.addr)
	if err != nil {
		t.Fatalf("open stream2 failed: %v", err)
	}
	for i := 1; i <= 2; i++ {
		seq, err := stream2.Append(ctx, []byte(fmt.Sprintf("nodeB-rec-%d", i)))
		if err != nil {
			t.Fatalf("stream2 append %d failed: %v", i, err)
		}
		if err := stream2.Wait(ctx, seq, Witness, false); err != nil {
			t.Fatalf("stream2 wait %d failed: %v", seq, err)
		}
	}
	_ = stream2.Close()

	// 3. Move back to Node A (node1Dir has local 1..5, buffer head is 2 with nodeB content)
	stream1Restarted, err := Open(ctx, node1Dir, streamID, handle.addr)
	if err != nil {
		t.Fatalf("open stream1Restarted failed: %v", err)
	}
	defer stream1Restarted.Close()

	// Next append on Node A must continue from buffer head 2 -> seq 3
	seq3, err := stream1Restarted.Append(ctx, []byte("nodeA-new-rec-3"))
	if err != nil {
		t.Fatalf("stream1Restarted append failed: %v", err)
	}
	if seq3 != 3 {
		t.Fatalf("expected next append to get seq 3, got %d", seq3)
	}
	if err := stream1Restarted.Wait(ctx, seq3, Witness, false); err != nil {
		t.Fatalf("wait for seq 3 failed: %v", err)
	}

	// Tail from buffer: verify records 1..2 are from Node B, record 3 is nodeA-new-rec-3, and old nodeA 3..5 were discarded
	tailIter, err := TailStream(ctx, handle.addr, streamID, 0)
	if err != nil {
		t.Fatalf("TailStream failed: %v", err)
	}

	recordsMap := make(map[uint64]string)
	for seq, payload := range tailIter {
		recordsMap[seq] = string(payload)
		if seq == 3 {
			break
		}
	}

	if recordsMap[1] != "nodeB-rec-1" {
		t.Errorf("seq 1: expected 'nodeB-rec-1', got %q", recordsMap[1])
	}
	if recordsMap[2] != "nodeB-rec-2" {
		t.Errorf("seq 2: expected 'nodeB-rec-2', got %q", recordsMap[2])
	}
	if recordsMap[3] != "nodeA-new-rec-3" {
		t.Errorf("seq 3: expected 'nodeA-new-rec-3', got %q", recordsMap[3])
	}

	// RecoveredRecords on stream1Restarted should have discarded divergent local records 3..5
	for _, rec := range stream1Restarted.RecoveredRecords() {
		if rec.StreamSeq > 2 && string(rec.Payload) != "nodeA-new-rec-3" {
			t.Errorf("unexpected recovered record %d with payload %q", rec.StreamSeq, string(rec.Payload))
		}
	}
	_ = stream1Restarted.Close()

	// 4. Reopen node1Dir once more to verify disk truncation persisted and RecoveredRecords is contiguous without duplicates
	stream1Reopened, err := Open(ctx, node1Dir, streamID, handle.addr)
	if err != nil {
		t.Fatalf("open stream1Reopened failed: %v", err)
	}
	defer stream1Reopened.Close()

	recovered := stream1Reopened.RecoveredRecords()
	// Should contain records 1, 2, 3 (where 3 is nodeA-new-rec-3) and NO divergent 4, 5
	for _, rec := range recovered {
		if rec.StreamSeq > 3 {
			t.Errorf("unexpected record %d found on disk after truncation", rec.StreamSeq)
		}
	}

	seq4, err := stream1Reopened.Append(ctx, []byte("nodeA-rec-4"))
	if err != nil {
		t.Fatalf("stream1Reopened append failed: %v", err)
	}
	if seq4 != 4 {
		t.Fatalf("expected seq 4, got %d", seq4)
	}
	if err := stream1Reopened.Wait(ctx, seq4, Witness, false); err != nil {
		t.Fatalf("wait for seq 4 failed: %v", err)
	}
}

// TestClientStream_LiveReconnectAcrossBufferRestart verifies that an actively open client stream
// survives a buffer crash and restart with persistent buffer dataDir, reconnects via background loop,
// and continues appending and witness-acknowledging records without hanging.
func TestClientStream_LiveReconnectAcrossBufferRestart(t *testing.T) {
	backend := inmemorystorage.New()
	bufDataDir := t.TempDir()

	handle1 := startBufferServer(t, backend, bufDataDir)
	defer func() {
		if handle1 != nil {
			handle1.StopGraceful()
		}
	}()

	clientDir := t.TempDir()
	streamID := uuid.New()

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	stream, err := Open(ctx, clientDir, streamID, handle1.addr)
	if err != nil {
		t.Fatalf("failed to open stream: %v", err)
	}
	defer stream.Close()

	// Append initial 5 records and wait for witness ack
	for i := 1; i <= 5; i++ {
		seq, err := stream.Append(ctx, []byte(fmt.Sprintf("msg-pre-restart-%d", i)))
		if err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
		if err := stream.Wait(ctx, seq, Witness, false); err != nil {
			t.Fatalf("wait witness %d failed: %v", seq, err)
		}
	}

	// Crash stop buffer server 1 WITHOUT close (no graceful shutdown)
	savedAddr := handle1.addr
	handle1.StopWithoutClose()
	handle1 = nil

	// Restart buffer server 2 on the exact same address and persistent dataDir
	handle2 := startBufferServerOnAddr(t, backend, bufDataDir, savedAddr)
	defer handle2.StopGraceful()

	// Append records post-restart through the same client stream instance.
	// The client's background replication loop must reconnect, exchange Hello/HelloAck,
	// and witness-acknowledge new records without hanging.
	for i := 6; i <= 10; i++ {
		seq, err := stream.Append(ctx, []byte(fmt.Sprintf("msg-post-restart-%d", i)))
		if err != nil {
			t.Fatalf("append post-restart %d failed: %v", i, err)
		}
		if err := stream.Wait(ctx, seq, Witness, false); err != nil {
			t.Fatalf("wait witness post-restart %d failed: %v", seq, err)
		}
	}

	_, witness, _ := stream.Watermarks()
	if witness < 10 {
		t.Errorf("expected witness watermark >= 10, got %d", witness)
	}
}

// TestClientStream_TerminalErrorOnStoreTruncateFailure verifies that when handleHelloAck
// fails due to a store error (e.g. TruncateAfter fails), the stream enters a terminal failed state,
// unblocks waiters, causes Append and Wait to return the terminal error immediately, and stops
// the replication loop instead of retrying forever.
func TestClientStream_TerminalErrorOnStoreTruncateFailure(t *testing.T) {
	ctx := t.Context()
	streamID := uuid.New()
	dir := t.TempDir()

	store, _, err := wal.NewClientSegmentStore(dir, "stream", 100)
	if err != nil {
		t.Fatalf("failed to create client segment store: %v", err)
	}

	for i := uint64(1); i <= 10; i++ {
		rec := &wal.ClientRecord{
			StreamID:  streamID,
			StreamSeq: i,
			Payload:   []byte(fmt.Sprintf("local-rec-%d", i)),
		}
		if err := store.Append(rec); err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
	}

	// Make the last segment unremovable by replacing it with a directory containing a child file
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir failed: %v", err)
	}
	var lastSegPath string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".wal") && !strings.Contains(e.Name(), "0000000000000000001") {
			lastSegPath = filepath.Join(dir, e.Name())
			break
		}
	}
	if lastSegPath == "" {
		t.Fatalf("did not find second segment file")
	}
	if err := os.Remove(lastSegPath); err != nil {
		t.Fatalf("remove seg file failed: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(lastSegPath, "child"), 0755); err != nil {
		t.Fatalf("mkdir child failed: %v", err)
	}

	cancelCtx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	impl := &streamImpl{
		streamID:            streamID,
		store:               store,
		localSeq:            5,
		retainedRecords:     []*wal.ClientRecord{{StreamSeq: 2, Payload: []byte("local-rec-2")}},
		initialHelloAckDone: make(chan struct{}),
		cancelCtx:           cancelCtx,
		cancelFunc:          cancelFunc,
		replicationLevel:    Witness,
	}
	impl.cond = sync.NewCond(&impl.mu)

	helloAck := &pb.HelloAck{
		WitnessAckedStreamSeq: 2,
	}

	// Buffer tail returns different payload for seq 2 (divergence path)
	mockClient := &mockBufferClientWithTail{
		records: []*pb.LogRecord{
			{StreamSeq: 2, Payload: []byte("authoritative-buffer-payload-2")},
		},
	}
	impl.grpcClient = mockClient

	err = impl.handleHelloAck(helloAck)
	if err == nil {
		t.Fatalf("expected handleHelloAck to fail, got nil")
	}

	// Verify terminal error is set
	impl.mu.RLock()
	termErr := impl.terminalErr
	impl.mu.RUnlock()
	if termErr == nil {
		t.Fatalf("expected terminalErr to be set, got nil")
	}

	// Append must return terminalErr immediately
	_, appendErr := impl.Append(ctx, []byte("should-fail"))
	if !errors.Is(appendErr, termErr) && !strings.Contains(appendErr.Error(), termErr.Error()) {
		t.Fatalf("expected Append to return terminal error, got: %v", appendErr)
	}

	// Wait must return terminalErr immediately without blocking
	waitErr := impl.Wait(ctx, 3, Witness, false)
	if !errors.Is(waitErr, termErr) && !strings.Contains(waitErr.Error(), termErr.Error()) {
		t.Fatalf("expected Wait to return terminal error, got: %v", waitErr)
	}
}
