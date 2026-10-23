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
	"bytes"
	"context"
	"errors"
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
	"k8s.io/klog/v2"
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

func TestOpen_DivergenceTruncateFailureDegraded(t *testing.T) {
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
	// which fails on read-only dir and places the stream into degraded state (without crashing Open).
	s, err := Open(ctx, clientDir, streamID, handle.addr, WithMaxSegmentSize(100))
	if err != nil {
		t.Fatalf("expected Open to succeed in degraded state, got error: %v", err)
	}
	defer s.Close()
	if s.Degraded() == nil {
		t.Fatalf("expected stream to be degraded on truncate failure")
	}
	if !strings.Contains(s.Degraded().Error(), "failed to truncate local segment store") {
		t.Errorf("expected failed to truncate local segment store in degraded error, got: %v", s.Degraded())
	}
}

func TestHandleHelloAck_TruncateFailureDegradedErr(t *testing.T) {
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

	var logBuf bytes.Buffer
	klog.LogToStderr(false)
	klog.SetOutput(&logBuf)
	defer func() {
		klog.LogToStderr(true)
		klog.SetOutput(os.Stderr)
	}()
	beforeErrors := klog.Stats.Error.Lines()

	err = impl.handleHelloAck(helloAck)
	if err == nil {
		t.Fatalf("expected handleHelloAck to fail when TruncateAfter fails, got nil")
	}
	var degErr *DegradedError
	if !errors.As(err, &degErr) {
		t.Fatalf("expected error to be or wrap *DegradedError, got: %v", err)
	}
	if degErr.At.IsZero() {
		t.Errorf("expected DegradedError.At to be populated, got zero time")
	}
	if !strings.Contains(err.Error(), "failed to truncate local segment store") {
		t.Errorf("expected 'failed to truncate local segment store' in error, got: %v", err)
	}

	// Verify that memory mutation was NOT applied (order-before-mutation guarantees memory and disk remain consistent)
	if impl.localSeq != 5 {
		t.Errorf("expected localSeq to remain unchanged at 5 on truncate failure, got: %d", impl.localSeq)
	}
	if len(impl.retainedRecords) != 1 {
		t.Errorf("expected retainedRecords to remain unchanged on truncate failure, got: %d", len(impl.retainedRecords))
	}

	// Verify that the stream enters degraded state so Degraded, Append, Wait, and Flush return the DegradedError immediately
	if impl.Degraded() == nil || !errors.As(impl.Degraded(), &degErr) {
		t.Errorf("expected Degraded to return *DegradedError, got: %v", impl.Degraded())
	}
	if _, appendErr := impl.Append(ctx, []byte("should-fail")); appendErr == nil || !errors.As(appendErr, &degErr) {
		t.Errorf("expected Append to fail with DegradedError, got: %v", appendErr)
	}
	if waitErr := impl.Wait(ctx, 1, Witness, false); waitErr == nil || !errors.As(waitErr, &degErr) {
		t.Errorf("expected Wait to fail with DegradedError, got: %v", waitErr)
	}
	if flushErr := impl.Flush(ctx); flushErr == nil || !errors.As(flushErr, &degErr) {
		t.Errorf("expected Flush to fail with DegradedError, got: %v", flushErr)
	}

	klog.Flush()
	if diff := klog.Stats.Error.Lines() - beforeErrors; diff != 1 {
		t.Fatalf("expected exactly 1 error log after stream degradation, got %d", diff)
	}
	expectedLog := fmt.Sprintf("WAL client stream %s degraded", streamID)
	if !strings.Contains(logBuf.String(), expectedLog) {
		t.Fatalf("expected log output to contain %q, got: %s", expectedLog, logBuf.String())
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
