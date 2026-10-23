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

package buffer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/wal/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore/inmemorystorage"
	"github.com/gke-labs/in-cluster-storage/pkg/wal"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func startTestServer(t *testing.T, backend objectstore.Backend, dataDir string) (*Server, string, func()) {
	ctx := t.Context()
	srv, err := NewServer(ctx, ServerConfig{
		Backend:       backend,
		DataDir:       dataDir,
		FlushInterval: 10 * time.Second,
		BatchMaxDelay: 2 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterWalBufferServer(grpcServer, srv)

	go func() {
		_ = grpcServer.Serve(listener)
	}()

	cleanup := func() {
		grpcServer.Stop()
		_ = srv.Close()
		_ = listener.Close()
	}

	return srv, listener.Addr().String(), cleanup
}

type testBufferHandle struct {
	srv        *Server
	grpcServer *grpc.Server
	listener   net.Listener
	addr       string
}

func (h *testBufferHandle) StopGraceful() {
	h.grpcServer.Stop()
	_ = h.srv.Close()
	_ = h.listener.Close()
}

func (h *testBufferHandle) StopWithoutFlush() {
	h.grpcServer.Stop()
	_ = h.listener.Close()
	close(h.srv.stopChan)
	if h.srv.localStore != nil {
		_ = h.srv.localStore.Close()
	}
}

func startTestServerHandle(t *testing.T, backend objectstore.Backend, dataDir string) *testBufferHandle {
	ctx := t.Context()
	srv, err := NewServer(ctx, ServerConfig{
		Backend:       backend,
		DataDir:       dataDir,
		FlushInterval: 10 * time.Second,
		BatchMaxDelay: 2 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterWalBufferServer(grpcServer, srv)

	go func() {
		_ = grpcServer.Serve(listener)
	}()

	return &testBufferHandle{
		srv:        srv,
		grpcServer: grpcServer,
		listener:   listener,
		addr:       listener.Addr().String(),
	}
}

func TestNewServerRequiresDataDir(t *testing.T) {
	ctx := t.Context()
	backend := inmemorystorage.New()
	_, err := NewServer(ctx, ServerConfig{Backend: backend})
	if err == nil {
		t.Fatalf("expected error when DataDir is empty")
	}
}

func TestServerStartupReadOnlyUntilFlush(t *testing.T) {
	backend := inmemorystorage.New()
	srv, addr, cleanup := startTestServer(t, backend, t.TempDir())
	defer cleanup()

	if srv.LastPosition() != 0 {
		t.Errorf("expected initial lastPosition 0, got %d", srv.LastPosition())
	}

	// Verify no segments exist in backend on startup
	segs, lastPos, err := ListSegmentsFromBackend(t.Context(), backend)
	if err != nil {
		t.Fatalf("ListSegmentsFromBackend failed: %v", err)
	}
	if len(segs) != 0 || lastPos != 0 {
		t.Fatalf("expected 0 segments on startup, got %d (lastPos=%d)", len(segs), lastPos)
	}

	// Append a record and flush
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	client := pb.NewWalBufferClient(conn)
	stream, err := client.Append(t.Context())
	if err != nil {
		t.Fatalf("append failed: %v", err)
	}

	streamID := uuid.New()
	_ = stream.Send(&pb.AppendRequest{Msg: &pb.AppendRequest_Hello{Hello: &pb.Hello{StreamId: streamID[:]}}})
	_, _ = stream.Recv()

	_ = stream.Send(&pb.AppendRequest{Msg: &pb.AppendRequest_Record{Record: &pb.AppendRecord{StreamSeq: 1, Payload: []byte("test")}}})
	_, _ = stream.Recv()

	_, err = client.Flush(t.Context(), &pb.FlushRequest{})
	if err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	// Verify segment now exists in backend
	segs, lastPos, err = ListSegmentsFromBackend(t.Context(), backend)
	if err != nil {
		t.Fatalf("ListSegmentsFromBackend failed: %v", err)
	}
	if len(segs) != 1 || lastPos != 1 {
		t.Fatalf("expected 1 segment after flush with lastPos 1, got %d (lastPos=%d)", len(segs), lastPos)
	}

	// Verify manifest file does NOT exist
	var buf bytes.Buffer
	if err := backend.GetObject(t.Context(), "", "streams/wal/manifest.json", 0, 0, &buf); err == nil {
		t.Fatalf("expected manifest file to NOT exist, but GetObject succeeded")
	}
	if err := backend.GetObject(t.Context(), "", "wal/manifest.json", 0, 0, &buf); err == nil {
		t.Fatalf("expected manifest file to NOT exist, but GetObject succeeded")
	}
}

func TestTailClampingAndResumedFrom(t *testing.T) {
	backend := inmemorystorage.New()
	srv, addr, cleanup := startTestServer(t, backend, t.TempDir())
	defer cleanup()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	client := pb.NewWalBufferClient(conn)
	stream, err := client.Append(t.Context())
	if err != nil {
		t.Fatalf("append failed: %v", err)
	}

	streamID := uuid.New()
	_ = stream.Send(&pb.AppendRequest{Msg: &pb.AppendRequest_Hello{Hello: &pb.Hello{StreamId: streamID[:]}}})
	_, _ = stream.Recv()

	// Append 5 records and flush (positions 1..5, last_flushed_position = 5)
	for i := uint64(1); i <= 5; i++ {
		_ = stream.Send(&pb.AppendRequest{Msg: &pb.AppendRequest_Record{Record: &pb.AppendRecord{StreamSeq: i, Payload: []byte(fmt.Sprintf("rec-%d", i))}}})
		_, _ = stream.Recv()
	}

	_, err = client.Flush(t.Context(), &pb.FlushRequest{})
	if err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	// Append 3 more records (unflushed, provisional positions 6..8)
	for i := uint64(6); i <= 8; i++ {
		_ = stream.Send(&pb.AppendRequest{Msg: &pb.AppendRequest_Record{Record: &pb.AppendRecord{StreamSeq: i, Payload: []byte(fmt.Sprintf("rec-%d", i))}}})
		_, _ = stream.Recv()
	}

	// 1. Tail from position 1 (<= last_position + 1)
	tailStream1, err := client.Tail(t.Context(), &pb.TailRequest{FromPosition: 1})
	if err != nil {
		t.Fatalf("tail 1 failed: %v", err)
	}
	first1, err := tailStream1.Recv()
	if err != nil {
		t.Fatalf("tail 1 first recv failed: %v", err)
	}
	if first1.ResumedFrom != 1 {
		t.Errorf("expected ResumedFrom 1, got %d", first1.ResumedFrom)
	}
	if first1.Record.Position != 1 {
		t.Errorf("expected position 1, got %d", first1.Record.Position)
	}

	// 2. Tail from position 4 (<= last_position + 1)
	tailStream2, err := client.Tail(t.Context(), &pb.TailRequest{FromPosition: 4})
	if err != nil {
		t.Fatalf("tail 2 failed: %v", err)
	}
	first2, err := tailStream2.Recv()
	if err != nil {
		t.Fatalf("tail 2 first recv failed: %v", err)
	}
	if first2.ResumedFrom != 4 {
		t.Errorf("expected ResumedFrom 4, got %d", first2.ResumedFrom)
	}
	if first2.Record.Position != 4 {
		t.Errorf("expected position 4, got %d", first2.Record.Position)
	}
	second2, err := tailStream2.Recv()
	if err != nil {
		t.Fatalf("tail 2 second recv failed: %v", err)
	}
	if second2.ResumedFrom != 0 {
		t.Errorf("expected ResumedFrom 0 on second message, got %d", second2.ResumedFrom)
	}

	// 3. Tail with provisional cursor from_position=20 (> last_flushed_position + 1 = 6)
	// Should clamp to 6 and return resumed_from = 6
	tailStream3, err := client.Tail(t.Context(), &pb.TailRequest{FromPosition: 20})
	if err != nil {
		t.Fatalf("tail 3 failed: %v", err)
	}
	first3, err := tailStream3.Recv()
	if err != nil {
		t.Fatalf("tail 3 first recv failed: %v", err)
	}
	if first3.ResumedFrom != 6 {
		t.Errorf("expected ResumedFrom clamped to 6, got %d", first3.ResumedFrom)
	}
	if first3.Record.Position != 6 {
		t.Errorf("expected position 6, got %d", first3.Record.Position)
	}
	_ = srv
}

func TestAppendGroupCommitAndFlush(t *testing.T) {
	backend := inmemorystorage.New()
	srv, addr, cleanup := startTestServer(t, backend, t.TempDir())
	defer cleanup()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	client := pb.NewWalBufferClient(conn)
	stream, err := client.Append(t.Context())
	if err != nil {
		t.Fatalf("failed to start stream: %v", err)
	}

	streamID := uuid.New()
	// 1. Handshake
	if err := stream.Send(&pb.AppendRequest{
		Msg: &pb.AppendRequest_Hello{
			Hello: &pb.Hello{
				StreamId: streamID[:],
			},
		},
	}); err != nil {
		t.Fatalf("failed to send hello: %v", err)
	}

	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("failed to recv hello ack: %v", err)
	}
	helloAck := resp.GetHelloAck()
	if helloAck == nil {
		t.Fatalf("unexpected hello ack: %+v", resp)
	}

	// 2. Append records
	for i := uint64(1); i <= 3; i++ {
		if err := stream.Send(&pb.AppendRequest{
			Msg: &pb.AppendRequest_Record{
				Record: &pb.AppendRecord{
					StreamSeq: i,
					Payload:   []byte(fmt.Sprintf("test-data-%d", i)),
				},
			},
		}); err != nil {
			t.Fatalf("failed to send record %d: %v", i, err)
		}

		ackResp, err := stream.Recv()
		if err != nil {
			t.Fatalf("failed to recv ack %d: %v", i, err)
		}
		ack := ackResp.GetAck()
		if ack == nil {
			t.Fatalf("expected Ack message, got: %+v", ackResp)
		}
		if ack.WitnessAckedStreamSeq != i {
			t.Errorf("expected WitnessAckedStreamSeq %d, got %d", i, ack.WitnessAckedStreamSeq)
		}
	}

	// 3. Flush to permanent storage
	flushResp, err := client.Flush(t.Context(), &pb.FlushRequest{})
	if err != nil {
		t.Fatalf("flush failed: %v", err)
	}
	if flushResp.LastPosition != 3 {
		t.Errorf("expected last_position 3, got %d", flushResp.LastPosition)
	}

	// Verify segments in backend
	segs, lastPos, err := ListSegmentsFromBackend(t.Context(), backend)
	if err != nil {
		t.Fatalf("failed to list segments: %v", err)
	}
	if len(segs) != 1 {
		t.Fatalf("expected 1 segment in backend, got %d", len(segs))
	}
	if lastPos != 3 {
		t.Errorf("expected last_position 3, got %d", lastPos)
	}
	_ = srv
}

func TestTailFlushedAndUnflushedMidStreamFlush(t *testing.T) {
	backend := inmemorystorage.New()
	dataDir := t.TempDir()
	srv, addr, cleanup := startTestServer(t, backend, dataDir)
	defer cleanup()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	client := pb.NewWalBufferClient(conn)
	appendStream, err := client.Append(t.Context())
	if err != nil {
		t.Fatalf("failed to open append stream: %v", err)
	}

	streamID := uuid.New()
	_ = appendStream.Send(&pb.AppendRequest{Msg: &pb.AppendRequest_Hello{Hello: &pb.Hello{StreamId: streamID[:]}}})
	_, _ = appendStream.Recv()

	// 1. Append records 1..10
	for i := uint64(1); i <= 10; i++ {
		_ = appendStream.Send(&pb.AppendRequest{
			Msg: &pb.AppendRequest_Record{
				Record: &pb.AppendRecord{
					StreamSeq: i,
					Payload:   []byte(fmt.Sprintf("record-%03d", i)),
				},
			},
		})
		_, err := appendStream.Recv()
		if err != nil {
			t.Fatalf("failed to recv append ack: %v", err)
		}
	}

	// Flush 1..10 to permanent storage
	if _, err := client.Flush(t.Context(), &pb.FlushRequest{}); err != nil {
		t.Fatalf("failed to flush records 1..10: %v", err)
	}

	// 2. Append records 11..20 (committed locally, unflushed)
	for i := uint64(11); i <= 20; i++ {
		_ = appendStream.Send(&pb.AppendRequest{
			Msg: &pb.AppendRequest_Record{
				Record: &pb.AppendRecord{
					StreamSeq: i,
					Payload:   []byte(fmt.Sprintf("record-%03d", i)),
				},
			},
		})
		_, err := appendStream.Recv()
		if err != nil {
			t.Fatalf("failed to recv append ack: %v", err)
		}
	}

	// 3. Start Tail from position 1
	tailCtx, tailCancel := context.WithCancel(t.Context())
	defer tailCancel()

	tailStream, err := client.Tail(tailCtx, &pb.TailRequest{FromPosition: 1})
	if err != nil {
		t.Fatalf("failed to start tail: %v", err)
	}

	receivedPositions := make([]uint64, 0, 30)
	recvErrChan := make(chan error, 1)

	go func() {
		for len(receivedPositions) < 30 {
			resp, err := tailStream.Recv()
			if err != nil {
				recvErrChan <- err
				return
			}
			rec := resp.GetRecord()
			if rec != nil {
				receivedPositions = append(receivedPositions, rec.Position)
			}
		}
		recvErrChan <- nil
	}()

	// Wait until at least some records are received by Tail
	time.Sleep(50 * time.Millisecond)

	// 4. Trigger a flush mid-stream (flushing 11..20 to object storage)
	if _, err := client.Flush(t.Context(), &pb.FlushRequest{}); err != nil {
		t.Fatalf("failed mid-stream flush: %v", err)
	}

	// 5. Append records 21..30
	for i := uint64(21); i <= 30; i++ {
		_ = appendStream.Send(&pb.AppendRequest{
			Msg: &pb.AppendRequest_Record{
				Record: &pb.AppendRecord{
					StreamSeq: i,
					Payload:   []byte(fmt.Sprintf("record-%03d", i)),
				},
			},
		})
		_, err := appendStream.Recv()
		if err != nil {
			t.Fatalf("failed to recv append ack: %v", err)
		}
	}

	select {
	case err := <-recvErrChan:
		if err != nil {
			t.Fatalf("error receiving tail records: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for 30 tail records, got %d: %v", len(receivedPositions), receivedPositions)
	}

	if len(receivedPositions) != 30 {
		t.Fatalf("expected 30 records, got %d", len(receivedPositions))
	}
	for i, pos := range receivedPositions {
		if pos != uint64(i+1) {
			t.Errorf("expected position %d at index %d, got %d", i+1, i, pos)
		}
	}
	_ = srv
}

func TestTailMemoryBounded(t *testing.T) {
	backend := inmemorystorage.New()
	dataDir := t.TempDir()

	ctx := t.Context()
	srv, err := NewServer(ctx, ServerConfig{
		Backend:        backend,
		DataDir:        dataDir,
		FlushInterval:  0,
		FlushBytes:     64 * 1024 * 1024,
		TailCacheBytes: 1024,
		BatchMaxDelay:  1 * time.Millisecond,
		BatchMaxSize:   64 * 1024,
	})
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}
	defer srv.Close()

	streamID := uuid.New()
	st := srv.getOrCreateStream(streamID)

	const recordCount = 2000
	errCh := make(chan error, 1)
	go func() {
		for i := uint64(1); i <= recordCount; i++ {
			ackChan := make(chan ackResult, 1)
			srv.incomingChan <- incomingItem{
				streamID:  streamID,
				streamSeq: i,
				payload:   []byte("x"),
				ackChan:   ackChan,
			}
			res := <-ackChan
			if res.err != nil {
				errCh <- fmt.Errorf("commit failed at %d: %w", i, res.err)
				return
			}
		}
		errCh <- nil
	}()

	if err := <-errCh; err != nil {
		t.Fatalf("%v", err)
	}

	// Flush everything to permanent storage
	if _, err := srv.Flush(ctx, &pb.FlushRequest{}); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	// Verify server holds no in-memory record history
	srv.unflushedMu.Lock()
	unflushedLen := len(srv.unflushedRecords)
	srv.unflushedMu.Unlock()

	if unflushedLen != 0 {
		t.Errorf("expected 0 unflushed records in memory after flush, got %d", unflushedLen)
	}

	if st.witnessSeq != recordCount {
		t.Errorf("expected witness watermark %d, got %d", recordCount, st.witnessSeq)
	}
}

func TestRetentionPreservesUnflushedFiles(t *testing.T) {
	backend := inmemorystorage.New()
	dataDir := t.TempDir()

	ctx := t.Context()
	srv, err := NewServer(ctx, ServerConfig{
		Backend:        backend,
		DataDir:        dataDir,
		FlushInterval:  10 * time.Second,
		FlushBytes:     256, // small segment files so rotation occurs
		TailCacheBytes: 0,   // prune flushed files aggressively
		BatchMaxDelay:  1 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}
	defer srv.Close()

	streamID := uuid.New()
	// Append 10 records
	for i := uint64(1); i <= 10; i++ {
		ackChan := make(chan ackResult, 1)
		srv.incomingChan <- incomingItem{
			streamID:  streamID,
			streamSeq: i,
			payload:   []byte(fmt.Sprintf("payload-long-string-to-cause-rotation-%d", i)),
			ackChan:   ackChan,
		}
		res := <-ackChan
		if res.err != nil {
			t.Fatalf("append %d failed: %v", i, res.err)
		}
	}

	// Flush the first 10 records
	if _, err := srv.Flush(ctx, &pb.FlushRequest{}); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	// Now append 10 MORE records that remain unflushed
	for i := uint64(11); i <= 20; i++ {
		ackChan := make(chan ackResult, 1)
		srv.incomingChan <- incomingItem{
			streamID:  streamID,
			streamSeq: i,
			payload:   []byte(fmt.Sprintf("payload-long-string-to-cause-rotation-%d", i)),
			ackChan:   ackChan,
		}
		res := <-ackChan
		if res.err != nil {
			t.Fatalf("append %d failed: %v", i, res.err)
		}
	}

	// Check disk: files containing records > 10 (unflushed) must still exist
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		t.Fatalf("failed to read dataDir: %v", err)
	}

	var unflushedFileCount int
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".wal") {
			filePath := filepath.Join(dataDir, entry.Name())
			_, meta, err := wal.ScanLogSegmentFile(filePath)
			if err != nil {
				t.Fatalf("failed to scan log segment %s: %v", filePath, err)
			}
			if meta.LastSeq > 10 {
				unflushedFileCount++
			}
		}
	}

	if unflushedFileCount == 0 {
		t.Fatalf("expected unflushed segment files to be preserved on disk")
	}
}

func TestRestartOnPersistentDataDir(t *testing.T) {
	backend := inmemorystorage.New()
	dataDir := t.TempDir()

	ctx := t.Context()

	// 1. Incarnation 1: write and flush 10 records
	srv1, err := NewServer(ctx, ServerConfig{
		Backend:       backend,
		DataDir:       dataDir,
		FlushInterval: 10 * time.Second,
		BatchMaxDelay: 1 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create server 1: %v", err)
	}

	streamID := uuid.New()
	for i := uint64(1); i <= 10; i++ {
		ackChan := make(chan ackResult, 1)
		srv1.incomingChan <- incomingItem{
			streamID:  streamID,
			streamSeq: i,
			payload:   []byte(fmt.Sprintf("incarnation-1-rec-%d", i)),
			ackChan:   ackChan,
		}
		<-ackChan
	}

	if _, err := srv1.Flush(ctx, &pb.FlushRequest{}); err != nil {
		t.Fatalf("flush failed on srv1: %v", err)
	}
	_ = srv1.Close()

	// 2. Incarnation 2: reopen on the same dataDir
	srv2, addr2, cleanup2 := startTestServer(t, backend, dataDir)
	defer cleanup2()

	// Connect gRPC client to server 2
	conn, err := grpc.NewClient(addr2, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	client := pb.NewWalBufferClient(conn)

	// Append 5 new records in incarnation 2
	appendStream, err := client.Append(t.Context())
	if err != nil {
		t.Fatalf("append failed: %v", err)
	}

	_ = appendStream.Send(&pb.AppendRequest{Msg: &pb.AppendRequest_Hello{Hello: &pb.Hello{StreamId: streamID[:]}}})
	_, _ = appendStream.Recv()

	for i := uint64(11); i <= 15; i++ {
		_ = appendStream.Send(&pb.AppendRequest{
			Msg: &pb.AppendRequest_Record{
				Record: &pb.AppendRecord{
					StreamSeq: i,
					Payload:   []byte(fmt.Sprintf("incarnation-2-rec-%d", i)),
				},
			},
		})
		_, _ = appendStream.Recv()
	}

	// 3. Tail from position 1 on Server 2
	tailCtx, tailCancel := context.WithCancel(t.Context())
	defer tailCancel()

	tailStream, err := client.Tail(tailCtx, &pb.TailRequest{FromPosition: 1})
	if err != nil {
		t.Fatalf("tail failed: %v", err)
	}

	var receivedRecs []*pb.LogRecord
	for len(receivedRecs) < 15 {
		resp, err := tailStream.Recv()
		if err != nil {
			t.Fatalf("tail recv error: %v", err)
		}
		if resp.GetRecord() != nil {
			receivedRecs = append(receivedRecs, resp.GetRecord())
		}
	}

	if len(receivedRecs) != 15 {
		t.Fatalf("expected 15 records from tail, got %d", len(receivedRecs))
	}

	// Records 1..10 should have positions 1..10
	for i := 0; i < 10; i++ {
		if receivedRecs[i].Position != uint64(i+1) {
			t.Errorf("expected position %d at index %d, got %d", i+1, i, receivedRecs[i].Position)
		}
		if string(receivedRecs[i].Payload) != fmt.Sprintf("incarnation-1-rec-%d", i+1) {
			t.Errorf("payload mismatch at index %d: %s", i, string(receivedRecs[i].Payload))
		}
	}

	// Records 11..15 should have positions starting at last_flushed_position + 1 (11..15)
	for i := 10; i < 15; i++ {
		expectedPos := uint64(i + 1)
		if receivedRecs[i].Position != expectedPos {
			t.Errorf("expected position %d at index %d, got %d", expectedPos, i, receivedRecs[i].Position)
		}
		if string(receivedRecs[i].Payload) != fmt.Sprintf("incarnation-2-rec-%d", i+1) {
			t.Errorf("payload mismatch at index %d: %s", i, string(receivedRecs[i].Payload))
		}
	}
	_ = srv2
}

func TestRestartWithoutFlush_WitnessWatermarkNoRegression(t *testing.T) {
	backend := inmemorystorage.New()
	dataDir := t.TempDir()

	ctx := t.Context()

	// 1. Incarnation 1: append records, witness-ack them, but do NOT flush
	handle1 := startTestServerHandle(t, backend, dataDir)

	conn1, err := grpc.NewClient(handle1.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial srv1 failed: %v", err)
	}
	defer conn1.Close()

	client1 := pb.NewWalBufferClient(conn1)
	streamID := uuid.New()

	appendStream1, err := client1.Append(ctx)
	if err != nil {
		t.Fatalf("append stream 1 failed: %v", err)
	}

	if err := appendStream1.Send(&pb.AppendRequest{
		Msg: &pb.AppendRequest_Hello{
			Hello: &pb.Hello{StreamId: streamID[:]},
		},
	}); err != nil {
		t.Fatalf("send hello failed: %v", err)
	}
	helloResp1, err := appendStream1.Recv()
	if err != nil {
		t.Fatalf("recv hello ack failed: %v", err)
	}
	if helloResp1.GetHelloAck() == nil {
		t.Fatalf("expected HelloAck, got %v", helloResp1)
	}

	// Append 5 records, verify each is witness-acked
	for i := uint64(1); i <= 5; i++ {
		if err := appendStream1.Send(&pb.AppendRequest{
			Msg: &pb.AppendRequest_Record{
				Record: &pb.AppendRecord{
					StreamSeq: i,
					Payload:   []byte(fmt.Sprintf("unflushed-rec-%d", i)),
				},
			},
		}); err != nil {
			t.Fatalf("send record %d failed: %v", i, err)
		}
		ackResp, err := appendStream1.Recv()
		if err != nil {
			t.Fatalf("recv ack %d failed: %v", i, err)
		}
		ack := ackResp.GetAck()
		if ack == nil || ack.WitnessAckedStreamSeq != i {
			t.Fatalf("expected witness ack %d, got %v", i, ack)
		}
	}

	// Stop handle1 WITHOUT calling Flush or Close (simulate ungraceful restart with data on disk)
	handle1.StopWithoutFlush()

	// 2. Incarnation 2: restart on the same dataDir without prior flush
	handle2 := startTestServerHandle(t, backend, dataDir)
	defer handle2.StopGraceful()

	conn2, err := grpc.NewClient(handle2.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial srv2 failed: %v", err)
	}
	defer conn2.Close()

	client2 := pb.NewWalBufferClient(conn2)
	appendStream2, err := client2.Append(ctx)
	if err != nil {
		t.Fatalf("append stream 2 failed: %v", err)
	}

	if err := appendStream2.Send(&pb.AppendRequest{
		Msg: &pb.AppendRequest_Hello{
			Hello: &pb.Hello{StreamId: streamID[:]},
		},
	}); err != nil {
		t.Fatalf("send hello 2 failed: %v", err)
	}
	helloResp2, err := appendStream2.Recv()
	if err != nil {
		t.Fatalf("recv hello ack 2 failed: %v", err)
	}
	helloAck2 := helloResp2.GetHelloAck()
	if helloAck2 == nil {
		t.Fatalf("expected HelloAck from srv2, got %v", helloResp2)
	}

	// Assert the HelloAck watermark did not regress (must be >= 5)
	if helloAck2.WitnessAckedStreamSeq < 5 {
		t.Fatalf("watermark regressed on restart: expected >= 5, got %d", helloAck2.WitnessAckedStreamSeq)
	}
}

func recvAppendResponseWithTimeout(t *testing.T, stream pb.WalBuffer_AppendClient, timeout time.Duration) (*pb.AppendResponse, error) {
	respCh := make(chan *pb.AppendResponse, 1)
	errCh := make(chan error, 1)
	go func() {
		resp, err := stream.Recv()
		if err != nil {
			errCh <- err
			return
		}
		respCh <- resp
	}()
	select {
	case resp := <-respCh:
		return resp, nil
	case err := <-errCh:
		return nil, err
	case <-time.After(timeout):
		return nil, errors.New("recv timeout")
	}
}

func TestFlushIsolationAcrossServers(t *testing.T) {
	backendA := inmemorystorage.New()
	srvA, addrA, cleanupA := startTestServer(t, backendA, t.TempDir())
	defer cleanupA()

	backendB := inmemorystorage.New()
	srvB, addrB, cleanupB := startTestServer(t, backendB, t.TempDir())
	defer cleanupB()

	connA, err := grpc.NewClient(addrA, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial A failed: %v", err)
	}
	defer connA.Close()

	connB, err := grpc.NewClient(addrB, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial B failed: %v", err)
	}
	defer connB.Close()

	clientA := pb.NewWalBufferClient(connA)
	streamA, err := clientA.Append(t.Context())
	if err != nil {
		t.Fatalf("failed to start stream A: %v", err)
	}

	clientB := pb.NewWalBufferClient(connB)
	streamB, err := clientB.Append(t.Context())
	if err != nil {
		t.Fatalf("failed to start stream B: %v", err)
	}

	streamIDA := uuid.New()
	streamIDB := uuid.New()

	// 1. Handshake A and B
	if err := streamA.Send(&pb.AppendRequest{
		Msg: &pb.AppendRequest_Hello{
			Hello: &pb.Hello{
				StreamId: streamIDA[:],
			},
		},
	}); err != nil {
		t.Fatalf("failed to send hello A: %v", err)
	}
	if _, err := streamA.Recv(); err != nil {
		t.Fatalf("failed to recv hello ack A: %v", err)
	}

	if err := streamB.Send(&pb.AppendRequest{
		Msg: &pb.AppendRequest_Hello{
			Hello: &pb.Hello{
				StreamId: streamIDB[:],
			},
		},
	}); err != nil {
		t.Fatalf("failed to send hello B: %v", err)
	}
	if _, err := streamB.Recv(); err != nil {
		t.Fatalf("failed to recv hello ack B: %v", err)
	}

	// 2. Append records on both
	if err := streamA.Send(&pb.AppendRequest{
		Msg: &pb.AppendRequest_Record{
			Record: &pb.AppendRecord{
				StreamSeq: 1,
				Payload:   []byte("serverA-data"),
			},
		},
	}); err != nil {
		t.Fatalf("failed to send record A: %v", err)
	}
	ackA, err := streamA.Recv()
	if err != nil || ackA.GetAck() == nil || ackA.GetAck().WitnessAckedStreamSeq != 1 {
		t.Fatalf("unexpected ack A: %+v, err: %v", ackA, err)
	}

	if err := streamB.Send(&pb.AppendRequest{
		Msg: &pb.AppendRequest_Record{
			Record: &pb.AppendRecord{
				StreamSeq: 1,
				Payload:   []byte("serverB-data"),
			},
		},
	}); err != nil {
		t.Fatalf("failed to send record B: %v", err)
	}
	ackB, err := streamB.Recv()
	if err != nil || ackB.GetAck() == nil || ackB.GetAck().WitnessAckedStreamSeq != 1 {
		t.Fatalf("unexpected ack B: %+v, err: %v", ackB, err)
	}

	// 3. Flush ONLY server A
	_, err = clientA.Flush(t.Context(), &pb.FlushRequest{})
	if err != nil {
		t.Fatalf("flush server A failed: %v", err)
	}

	// Stream A must receive S3 ack
	flushAckA, err := recvAppendResponseWithTimeout(t, streamA, 1*time.Second)
	if err != nil {
		t.Fatalf("stream A did not receive S3 ack after flush: %v", err)
	}
	if flushAckA.GetAck() == nil || flushAckA.GetAck().S3AckedStreamSeq != 1 {
		t.Fatalf("expected S3AckedStreamSeq 1 on stream A, got %+v", flushAckA)
	}

	// Stream B must NOT receive any message
	respB, err := recvAppendResponseWithTimeout(t, streamB, 100*time.Millisecond)
	if err == nil {
		t.Fatalf("stream B received unexpected message when server A flushed: %+v", respB)
	}
	_ = srvA
	_ = srvB
}

func TestFlushIsolationAcrossStreams(t *testing.T) {
	backend := inmemorystorage.New()
	srv, addr, cleanup := startTestServer(t, backend, t.TempDir())
	defer cleanup()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	client := pb.NewWalBufferClient(conn)

	stream1, err := client.Append(t.Context())
	if err != nil {
		t.Fatalf("failed to start stream 1: %v", err)
	}
	stream2, err := client.Append(t.Context())
	if err != nil {
		t.Fatalf("failed to start stream 2: %v", err)
	}

	streamID1 := uuid.New()
	streamID2 := uuid.New()

	// 1. Handshake both streams
	if err := stream1.Send(&pb.AppendRequest{
		Msg: &pb.AppendRequest_Hello{Hello: &pb.Hello{StreamId: streamID1[:]}},
	}); err != nil {
		t.Fatalf("failed to send hello 1: %v", err)
	}
	if _, err := stream1.Recv(); err != nil {
		t.Fatalf("failed to recv hello ack 1: %v", err)
	}

	if err := stream2.Send(&pb.AppendRequest{
		Msg: &pb.AppendRequest_Hello{Hello: &pb.Hello{StreamId: streamID2[:]}},
	}); err != nil {
		t.Fatalf("failed to send hello 2: %v", err)
	}
	if _, err := stream2.Recv(); err != nil {
		t.Fatalf("failed to recv hello ack 2: %v", err)
	}

	// 2. Append record 1 on both streams
	_ = stream1.Send(&pb.AppendRequest{
		Msg: &pb.AppendRequest_Record{Record: &pb.AppendRecord{StreamSeq: 1, Payload: []byte("s1-1")}},
	})
	ack1, err := stream1.Recv()
	if err != nil || ack1.GetAck() == nil || ack1.GetAck().WitnessAckedStreamSeq != 1 {
		t.Fatalf("unexpected ack 1: %+v, err: %v", ack1, err)
	}

	_ = stream2.Send(&pb.AppendRequest{
		Msg: &pb.AppendRequest_Record{Record: &pb.AppendRecord{StreamSeq: 1, Payload: []byte("s2-1")}},
	})
	ack2, err := stream2.Recv()
	if err != nil || ack2.GetAck() == nil || ack2.GetAck().WitnessAckedStreamSeq != 1 {
		t.Fatalf("unexpected ack 2: %+v, err: %v", ack2, err)
	}

	// 3. Flush server -> both streams get S3 ack for seq 1
	if _, err := client.Flush(t.Context(), &pb.FlushRequest{}); err != nil {
		t.Fatalf("flush failed: %v", err)
	}
	s3Ack1, err := recvAppendResponseWithTimeout(t, stream1, 1*time.Second)
	if err != nil || s3Ack1.GetAck() == nil || s3Ack1.GetAck().S3AckedStreamSeq != 1 {
		t.Fatalf("expected s3 ack 1 on stream 1, got %+v, err: %v", s3Ack1, err)
	}
	s3Ack2, err := recvAppendResponseWithTimeout(t, stream2, 1*time.Second)
	if err != nil || s3Ack2.GetAck() == nil || s3Ack2.GetAck().S3AckedStreamSeq != 1 {
		t.Fatalf("expected s3 ack 1 on stream 2, got %+v, err: %v", s3Ack2, err)
	}

	// 4. Append record 2 ONLY on stream 1
	_ = stream1.Send(&pb.AppendRequest{
		Msg: &pb.AppendRequest_Record{Record: &pb.AppendRecord{StreamSeq: 2, Payload: []byte("s1-2")}},
	})
	ack1_2, err := stream1.Recv()
	if err != nil || ack1_2.GetAck() == nil || ack1_2.GetAck().WitnessAckedStreamSeq != 2 {
		t.Fatalf("unexpected ack 1_2: %+v, err: %v", ack1_2, err)
	}

	// 5. Flush server (containing records ONLY from stream 1)
	if _, err := client.Flush(t.Context(), &pb.FlushRequest{}); err != nil {
		t.Fatalf("flush 2 failed: %v", err)
	}

	// Stream 1 MUST receive S3 ack for seq 2
	flushAck1, err := recvAppendResponseWithTimeout(t, stream1, 1*time.Second)
	if err != nil || flushAck1.GetAck() == nil || flushAck1.GetAck().S3AckedStreamSeq != 2 {
		t.Fatalf("expected S3AckedStreamSeq 2 on stream 1, got %+v, err: %v", flushAck1, err)
	}

	// Stream 2 MUST NOT receive any ack
	resp2, err := recvAppendResponseWithTimeout(t, stream2, 100*time.Millisecond)
	if err == nil {
		t.Fatalf("stream 2 received unexpected message when stream 1 flushed: %+v", resp2)
	}
	_ = srv
}

func TestPerStreamTailFiltering(t *testing.T) {
	backend := inmemorystorage.New()
	srv, addr, cleanup := startTestServer(t, backend, t.TempDir())
	defer cleanup()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	client := pb.NewWalBufferClient(conn)

	streamA, err := client.Append(t.Context())
	if err != nil {
		t.Fatalf("failed to start stream A: %v", err)
	}
	streamB, err := client.Append(t.Context())
	if err != nil {
		t.Fatalf("failed to start stream B: %v", err)
	}
	streamC, err := client.Append(t.Context())
	if err != nil {
		t.Fatalf("failed to start stream C: %v", err)
	}

	streamIDA := uuid.New()
	streamIDB := uuid.New()
	streamIDC := uuid.New()

	_ = streamA.Send(&pb.AppendRequest{Msg: &pb.AppendRequest_Hello{Hello: &pb.Hello{StreamId: streamIDA[:]}}})
	_, _ = streamA.Recv()
	_ = streamB.Send(&pb.AppendRequest{Msg: &pb.AppendRequest_Hello{Hello: &pb.Hello{StreamId: streamIDB[:]}}})
	_, _ = streamB.Recv()
	_ = streamC.Send(&pb.AppendRequest{Msg: &pb.AppendRequest_Hello{Hello: &pb.Hello{StreamId: streamIDC[:]}}})
	_, _ = streamC.Recv()

	// 1. Interleave appends: stream A 1..5, stream B 1..5, stream C 1..5
	for i := uint64(1); i <= 5; i++ {
		_ = streamA.Send(&pb.AppendRequest{Msg: &pb.AppendRequest_Record{Record: &pb.AppendRecord{StreamSeq: i, Payload: []byte(fmt.Sprintf("A-%d", i))}}})
		_, _ = streamA.Recv()
		_ = streamB.Send(&pb.AppendRequest{Msg: &pb.AppendRequest_Record{Record: &pb.AppendRecord{StreamSeq: i, Payload: []byte(fmt.Sprintf("B-%d", i))}}})
		_, _ = streamB.Recv()
		_ = streamC.Send(&pb.AppendRequest{Msg: &pb.AppendRequest_Record{Record: &pb.AppendRecord{StreamSeq: i, Payload: []byte(fmt.Sprintf("C-%d", i))}}})
		_, _ = streamC.Recv()
	}

	// Flush first segment to permanent storage (positions 1..15)
	if _, err := client.Flush(t.Context(), &pb.FlushRequest{}); err != nil {
		t.Fatalf("flush 1 failed: %v", err)
	}

	// 2. Interleave more appends: stream A 6..10, stream B 6..10
	for i := uint64(6); i <= 10; i++ {
		_ = streamA.Send(&pb.AppendRequest{Msg: &pb.AppendRequest_Record{Record: &pb.AppendRecord{StreamSeq: i, Payload: []byte(fmt.Sprintf("A-%d", i))}}})
		_, _ = streamA.Recv()
		_ = streamB.Send(&pb.AppendRequest{Msg: &pb.AppendRequest_Record{Record: &pb.AppendRecord{StreamSeq: i, Payload: []byte(fmt.Sprintf("B-%d", i))}}})
		_, _ = streamB.Recv()
	}

	// Flush second segment to permanent storage (positions 16..25)
	if _, err := client.Flush(t.Context(), &pb.FlushRequest{}); err != nil {
		t.Fatalf("flush 2 failed: %v", err)
	}

	// 3. Interleave unflushed appends: stream A 11..15, stream C 6..10 (in localStore/memory)
	for i := uint64(11); i <= 15; i++ {
		_ = streamA.Send(&pb.AppendRequest{Msg: &pb.AppendRequest_Record{Record: &pb.AppendRecord{StreamSeq: i, Payload: []byte(fmt.Sprintf("A-%d", i))}}})
		_, _ = streamA.Recv()
	}
	for i := uint64(6); i <= 10; i++ {
		_ = streamC.Send(&pb.AppendRequest{Msg: &pb.AppendRequest_Record{Record: &pb.AppendRecord{StreamSeq: i, Payload: []byte(fmt.Sprintf("C-%d", i))}}})
		_, _ = streamC.Recv()
	}

	// 4. Tail Stream A from from_stream_seq = 3 across flushed segments and local store
	tailCtx, tailCancel := context.WithCancel(t.Context())
	defer tailCancel()

	tailStream, err := client.Tail(tailCtx, &pb.TailRequest{
		StreamId:      streamIDA[:],
		FromStreamSeq: 3,
	})
	if err != nil {
		t.Fatalf("tail stream A failed: %v", err)
	}

	var receivedSeqs []uint64
	var receivedPayloads []string
	recvErrChan := make(chan error, 1)

	go func() {
		// Expect seq 4..15 (12 records) + 2 live records (16, 17) = 14 records total
		for len(receivedSeqs) < 14 {
			resp, err := tailStream.Recv()
			if err != nil {
				recvErrChan <- err
				return
			}
			rec := resp.GetRecord()
			if rec != nil {
				receivedSeqs = append(receivedSeqs, rec.StreamSeq)
				receivedPayloads = append(receivedPayloads, string(rec.Payload))
			}
		}
		recvErrChan <- nil
	}()

	// Wait a moment for backlog to be processed
	time.Sleep(50 * time.Millisecond)

	// 5. Append live records: stream A 16, stream B 11, stream A 17
	_ = streamA.Send(&pb.AppendRequest{Msg: &pb.AppendRequest_Record{Record: &pb.AppendRecord{StreamSeq: 16, Payload: []byte("A-16")}}})
	_, _ = streamA.Recv()
	_ = streamB.Send(&pb.AppendRequest{Msg: &pb.AppendRequest_Record{Record: &pb.AppendRecord{StreamSeq: 11, Payload: []byte("B-11")}}})
	_, _ = streamB.Recv()
	_ = streamA.Send(&pb.AppendRequest{Msg: &pb.AppendRequest_Record{Record: &pb.AppendRecord{StreamSeq: 17, Payload: []byte("A-17")}}})
	_, _ = streamA.Recv()

	select {
	case err := <-recvErrChan:
		if err != nil {
			t.Fatalf("error receiving tailed records: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for tail records, received: %v", receivedSeqs)
	}

	expectedSeqs := []uint64{4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}
	if len(receivedSeqs) != len(expectedSeqs) {
		t.Fatalf("expected %d records, got %d: %v", len(expectedSeqs), len(receivedSeqs), receivedSeqs)
	}

	for i, seq := range receivedSeqs {
		if seq != expectedSeqs[i] {
			t.Errorf("at index %d: expected seq %d, got %d", i, expectedSeqs[i], seq)
		}
		expectedPayload := fmt.Sprintf("A-%d", seq)
		if receivedPayloads[i] != expectedPayload {
			t.Errorf("at index %d: expected payload %s, got %s", i, expectedPayload, receivedPayloads[i])
		}
	}
	_ = srv
}

func TestPerStreamTailInvalidStreamID(t *testing.T) {
	backend := inmemorystorage.New()
	srv, addr, cleanup := startTestServer(t, backend, t.TempDir())
	defer cleanup()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	client := pb.NewWalBufferClient(conn)

	tailStream, err := client.Tail(t.Context(), &pb.TailRequest{
		StreamId: []byte("short-id"), // invalid length (!= 16)
	})
	if err != nil {
		t.Fatalf("tail failed: %v", err)
	}

	_, err = tailStream.Recv()
	if err == nil {
		t.Fatalf("expected error for invalid stream_id, got nil")
	}
	_ = srv
}
