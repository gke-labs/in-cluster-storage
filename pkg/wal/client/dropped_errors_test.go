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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/wal/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore/inmemorystorage"
	"github.com/gke-labs/in-cluster-storage/pkg/wal"
	"github.com/google/uuid"
	"google.golang.org/grpc"
)

func TestOpen_HelloTimeoutCleansUp(t *testing.T) {
	ctx := t.Context()
	shortCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()

	streamID := uuid.New()
	dir := t.TempDir()

	// Target pointing to an unreachable dummy address that will timeout waiting for hello
	_, err := Open(shortCtx, dir, streamID, "127.0.0.1:54321")
	if err == nil {
		t.Fatalf("expected error from Open on unreachable target, got nil")
	}
	if !strings.Contains(err.Error(), "timeout waiting for initial HelloAck") && !strings.Contains(err.Error(), "failed to receive initial HelloAck") {
		t.Errorf("expected hello timeout error, got: %v", err)
	}
}

func TestStreamImpl_Close(t *testing.T) {
	ctx := t.Context()
	streamID := uuid.New()
	dir := t.TempDir()

	stream, err := Open(ctx, dir, streamID, "")
	if err != nil {
		t.Fatalf("failed to open local stream: %v", err)
	}

	_, err = stream.Append(ctx, []byte("test-record"))
	if err != nil {
		t.Fatalf("failed to append: %v", err)
	}

	if err := stream.Close(); err != nil {
		t.Fatalf("failed to close stream: %v", err)
	}

	// Double close should succeed without error
	if err := stream.Close(); err != nil {
		t.Fatalf("expected double close to succeed, got: %v", err)
	}
}

func TestTailStream_Validation(t *testing.T) {
	streamID := uuid.New()
	iter, err := TailStream(t.Context(), "", streamID, 0)
	if err == nil {
		t.Fatalf("expected error when neither target nor grpcClient provided, got nil")
	}
	if iter != nil {
		t.Errorf("expected nil iterator, got: %v", iter)
	}
}

func TestOpen_DivergenceTruncateFailureFailsOpen(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("skipping read-only directory test when running as root")
	}

	backend := inmemorystorage.New()
	handle := startBufferServer(t, backend, t.TempDir())
	defer handle.StopGraceful()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	streamID := uuid.New()
	clientDir := t.TempDir()

	// 1. Client writes records 1..5 locally with small segment size so multiple segments exist
	localStream, err := Open(ctx, clientDir, streamID, "", WithMaxSegmentSize(100))
	if err != nil {
		t.Fatalf("open localStream failed: %v", err)
	}
	for i := 1; i <= 5; i++ {
		_, err := localStream.Append(ctx, []byte(fmt.Sprintf("nodeA-rec-%d", i)))
		if err != nil {
			t.Fatalf("localStream append %d failed: %v", i, err)
		}
	}
	if err := localStream.Close(); err != nil {
		t.Fatalf("close localStream failed: %v", err)
	}

	// 2. Another node writes authoritative records 1..2 to wal-buffer (witness watermark = 2)
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
	if err := stream2.Close(); err != nil {
		t.Fatalf("close stream2 failed: %v", err)
	}

	// 3. Make clientDir read-only so TruncateAfter cannot remove obsolete segment files
	if err := os.Chmod(clientDir, 0555); err != nil {
		t.Fatalf("chmod clientDir failed: %v", err)
	}
	defer func() {
		_ = os.Chmod(clientDir, 0755)
	}()

	// 4. Open client stream against wal-buffer. Reconciling divergence requires TruncateAfter(2),
	// which must fail on the read-only directory and fail Open.
	_, err = Open(ctx, clientDir, streamID, handle.addr, WithMaxSegmentSize(100))
	if err == nil {
		t.Fatalf("expected Open to fail when TruncateAfter fails, got nil")
	}
	if !strings.Contains(err.Error(), "failed to truncate local segment store") {
		t.Errorf("expected failed to truncate local segment store in error, got: %v", err)
	}
}

func TestHandleHelloAck_TruncateFailureReturnsError(t *testing.T) {
	ctx := t.Context()
	streamID := uuid.New()
	dir := t.TempDir()

	store, _, err := wal.NewClientSegmentStore(dir, fmt.Sprintf("stream-%s", streamID.String()), 120)
	if err != nil {
		t.Fatalf("failed to create client segment store: %v", err)
	}

	for i := uint64(1); i <= 5; i++ {
		rec := &wal.ClientRecord{
			StreamID:  streamID,
			StreamSeq: i,
			Payload:   []byte(fmt.Sprintf("local-rec-%d", i)),
		}
		if err := store.Append(rec); err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
	}

	// Replace the last segment with a non-empty directory so TruncateAfter fails across both root and non-root
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

	impl := &streamImpl{
		streamID:            streamID,
		store:               store,
		localSeq:            5,
		retainedRecords:     []*wal.ClientRecord{{StreamSeq: 2, Payload: []byte("local-rec-2")}},
		initialHelloAckDone: make(chan struct{}),
		cancelCtx:           ctx,
	}

	// Buffer witness watermark is 2, and buffer record payload differs (triggering divergence)
	helloAck := &pb.HelloAck{
		WitnessAckedStreamSeq: 2,
	}

	// Mock buffer tail returning different payload for seq 2
	mockClient := &mockBufferClientWithTail{
		records: []*pb.LogRecord{
			{StreamSeq: 2, Payload: []byte("authoritative-buffer-payload-2")},
		},
	}
	impl.grpcClient = mockClient

	err = impl.handleHelloAck(helloAck)
	if err == nil {
		t.Fatalf("expected handleHelloAck to fail when TruncateAfter fails, got nil")
	}
	if !strings.Contains(err.Error(), "failed to truncate local segment store") {
		t.Errorf("expected 'failed to truncate local segment store' in error, got: %v", err)
	}
}

type mockBufferClientWithTail struct {
	pb.WalBufferClient
	records []*pb.LogRecord
}

type mockTailClientStream struct {
	grpc.ClientStream
	records []*pb.LogRecord
	idx     int
}

func (m *mockTailClientStream) Recv() (*pb.TailResponse, error) {
	if m.idx >= len(m.records) {
		return nil, context.Canceled
	}
	rec := m.records[m.idx]
	m.idx++
	return &pb.TailResponse{Record: rec}, nil
}

func (m *mockBufferClientWithTail) Tail(ctx context.Context, in *pb.TailRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[pb.TailResponse], error) {
	return &mockTailClientStream{records: m.records}, nil
}
