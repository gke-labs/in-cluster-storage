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
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/wal/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/blob"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore/inmemorystorage"
	"github.com/gke-labs/in-cluster-storage/pkg/wal"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestPeriodicPerStreamSealing tests periodic per-stream sealing by interval and size,
// verifies naming format (zero-padded 20 digits), verifies that idle streams produce no empty files,
// and verifies that next open file starts at to+1.
func TestPeriodicPerStreamSealing(t *testing.T) {
	ctx := t.Context()
	backend := inmemorystorage.New()
	dataDir := t.TempDir()

	srv, err := NewServer(ctx, ServerConfig{
		Backend:            backend,
		DataDir:            dataDir,
		FlushInterval:      10 * time.Second,
		BatchMaxDelay:      1 * time.Millisecond,
		StreamSealInterval: 50 * time.Millisecond,
		StreamSealBytes:    1024,
	})
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}
	defer srv.Close()

	streamIDA := uuid.New()
	streamIDB := uuid.New() // idle stream

	// Append 10 records to Stream A
	for i := uint64(1); i <= 10; i++ {
		ackChan := make(chan ackResult, 1)
		srv.incomingChan <- incomingItem{
			streamID:  streamIDA,
			streamSeq: i,
			payload:   []byte(fmt.Sprintf("rec-A-%d", i)),
			ackChan:   ackChan,
		}
		<-ackChan
	}

	// Trigger manual seal on Stream A
	if err := srv.SealStream(ctx, streamIDA); err != nil {
		t.Fatalf("SealStream failed: %v", err)
	}

	// Check object storage: Stream A must have sealed file
	keysA, err := backend.ListObjects(ctx, "", fmt.Sprintf("streams/%s/", streamIDA.String()))
	if err != nil {
		t.Fatalf("ListObjects failed: %v", err)
	}
	if len(keysA) != 1 {
		t.Fatalf("expected 1 sealed object for stream A, got %d (%v)", len(keysA), keysA)
	}

	expectedKeyA := wal.FormatStreamObjectKey(streamIDA, 1, 10)
	if keysA[0] != expectedKeyA {
		t.Fatalf("unexpected object key: got %s, want %s", keysA[0], expectedKeyA)
	}

	// Check idle Stream B: sealing must be a no-op (no files produced)
	if err := srv.SealStream(ctx, streamIDB); err != nil {
		t.Fatalf("SealStream on idle stream B failed: %v", err)
	}
	keysB, err := backend.ListObjects(ctx, "", fmt.Sprintf("streams/%s/", streamIDB.String()))
	if err != nil {
		t.Fatalf("ListObjects for stream B failed: %v", err)
	}
	if len(keysB) != 0 {
		t.Fatalf("expected 0 sealed objects for idle stream B, got %d (%v)", len(keysB), keysB)
	}

	// Append next batch to Stream A (records 11..15)
	for i := uint64(11); i <= 15; i++ {
		ackChan := make(chan ackResult, 1)
		srv.incomingChan <- incomingItem{
			streamID:  streamIDA,
			streamSeq: i,
			payload:   []byte(fmt.Sprintf("rec-A-%d", i)),
			ackChan:   ackChan,
		}
		<-ackChan
	}

	// Seal again
	if err := srv.SealStream(ctx, streamIDA); err != nil {
		t.Fatalf("SealStream 2 failed: %v", err)
	}

	keysA2, err := backend.ListObjects(ctx, "", fmt.Sprintf("streams/%s/", streamIDA.String()))
	if err != nil {
		t.Fatalf("ListObjects 2 failed: %v", err)
	}
	if len(keysA2) != 2 {
		t.Fatalf("expected 2 sealed objects for stream A, got %d (%v)", len(keysA2), keysA2)
	}
	sort.Strings(keysA2)
	expectedKeyA2 := wal.FormatStreamObjectKey(streamIDA, 11, 15)
	if keysA2[1] != expectedKeyA2 {
		t.Fatalf("unexpected second object key: got %s, want %s", keysA2[1], expectedKeyA2)
	}
}

// TestAggregatedSegmentGCWhenCovered tests that an aggregated segment in object storage
// is preserved until all streams in it are sealed & uploaded, and deleted as soon as they are.
func TestAggregatedSegmentGCWhenCovered(t *testing.T) {
	ctx := t.Context()
	backend := inmemorystorage.New()
	dataDir := t.TempDir()

	srv, err := NewServer(ctx, ServerConfig{
		Backend:       backend,
		DataDir:       dataDir,
		FlushInterval: 10 * time.Second,
		BatchMaxDelay: 1 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}
	defer srv.Close()

	streamIDA := uuid.New()
	streamIDB := uuid.New()

	// Append records for both streams into the same aggregated batch
	for i := uint64(1); i <= 5; i++ {
		ackChanA := make(chan ackResult, 1)
		srv.incomingChan <- incomingItem{
			streamID:  streamIDA,
			streamSeq: i,
			payload:   []byte(fmt.Sprintf("A-%d", i)),
			ackChan:   ackChanA,
		}
		<-ackChanA

		ackChanB := make(chan ackResult, 1)
		srv.incomingChan <- incomingItem{
			streamID:  streamIDB,
			streamSeq: i,
			payload:   []byte(fmt.Sprintf("B-%d", i)),
			ackChan:   ackChanB,
		}
		<-ackChanB
	}

	// Flush aggregated segment to object storage (positions 1..10)
	resp, err := srv.Flush(ctx, &pb.FlushRequest{})
	if err != nil || resp.LastPosition != 10 {
		t.Fatalf("flush failed: resp=%+v, err=%v", resp, err)
	}

	segPath := fmt.Sprintf("%s%012d-%012d.wal", SegmentsPrefix, 1, 10)
	var segBuf bytes.Buffer
	if err := backend.GetObject(ctx, "", segPath, 0, 0, &segBuf); err != nil {
		t.Fatalf("expected aggregated segment %s in object storage, err: %v", segPath, err)
	}

	// 1. Seal Stream A only. Segment MUST still exist because Stream B is not yet sealed.
	if err := srv.SealStream(ctx, streamIDA); err != nil {
		t.Fatalf("SealStream A failed: %v", err)
	}
	segBuf.Reset()
	if err := backend.GetObject(ctx, "", segPath, 0, 0, &segBuf); err != nil {
		t.Fatalf("aggregated segment %s should NOT be deleted before stream B is sealed: %v", segPath, err)
	}

	// 2. Seal Stream B. Both streams are now sealed & uploaded past their sequences in segment 1..10.
	if err := srv.SealStream(ctx, streamIDB); err != nil {
		t.Fatalf("SealStream B failed: %v", err)
	}

	// Aggregated segment MUST now be deleted from object storage.
	segBuf.Reset()
	err = backend.GetObject(ctx, "", segPath, 0, 0, &segBuf)
	if err == nil {
		t.Fatalf("expected aggregated segment %s to be deleted after both streams sealed, but still present", segPath)
	}

	// srv.flushedSegments must no longer include segPath
	srv.mu.RLock()
	for _, s := range srv.flushedSegments {
		if s == segPath {
			t.Fatalf("expected %s removed from srv.flushedSegments", segPath)
		}
	}
	srv.mu.RUnlock()
}

// TestTailHistoryAcrossTiersAndLocalEviction tests replaying a stream whose history
// spans:
// 1. Remote sealed per-stream files in object storage (local file evicted/deleted)
// 2. Local sealed per-stream files
// 3. Local open per-stream file
// 4. In-memory / unflushed tail
func TestTailHistoryAcrossTiersAndLocalEviction(t *testing.T) {
	ctx := t.Context()
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

	streamID := uuid.New()
	appendStream, err := client.Append(ctx)
	if err != nil {
		t.Fatalf("append failed: %v", err)
	}

	_ = appendStream.Send(&pb.AppendRequest{Msg: &pb.AppendRequest_Hello{Hello: &pb.Hello{StreamId: streamID[:]}}})
	_, _ = appendStream.Recv()

	// 1. Tier 1: records 1..10, seal, and evict locally
	for i := uint64(1); i <= 10; i++ {
		_ = appendStream.Send(&pb.AppendRequest{
			Msg: &pb.AppendRequest_Record{Record: &pb.AppendRecord{StreamSeq: i, Payload: []byte(fmt.Sprintf("payload-%d", i))}},
		})
		_, _ = appendStream.Recv()
	}
	if err := srv.SealStream(ctx, streamID); err != nil {
		t.Fatalf("SealStream tier 1 failed: %v", err)
	}

	// Simulate local disk eviction of tier 1 file
	tier1Local := filepath.Join(dataDir, "streams", streamID.String(), wal.FormatStreamFileName(1, 10, true))
	if err := os.Remove(tier1Local); err != nil && !os.IsNotExist(err) {
		t.Fatalf("failed to evict local file: %v", err)
	}

	// 2. Tier 2: records 11..20, seal (keep on local disk)
	for i := uint64(11); i <= 20; i++ {
		_ = appendStream.Send(&pb.AppendRequest{
			Msg: &pb.AppendRequest_Record{Record: &pb.AppendRecord{StreamSeq: i, Payload: []byte(fmt.Sprintf("payload-%d", i))}},
		})
		_, _ = appendStream.Recv()
	}
	if err := srv.SealStream(ctx, streamID); err != nil {
		t.Fatalf("SealStream tier 2 failed: %v", err)
	}

	// 3. Tier 3: records 21..25, in open file
	for i := uint64(21); i <= 25; i++ {
		_ = appendStream.Send(&pb.AppendRequest{
			Msg: &pb.AppendRequest_Record{Record: &pb.AppendRecord{StreamSeq: i, Payload: []byte(fmt.Sprintf("payload-%d", i))}},
		})
		_, _ = appendStream.Recv()
	}

	// 4. Tail Stream starting from seq 0: must return 1..25 seamlessly
	tailCtx, tailCancel := context.WithCancel(ctx)
	defer tailCancel()

	tailStream, err := client.Tail(tailCtx, &pb.TailRequest{
		StreamId:      streamID[:],
		FromStreamSeq: 0,
	})
	if err != nil {
		t.Fatalf("Tail failed: %v", err)
	}

	var receivedSeqs []uint64
	for len(receivedSeqs) < 25 {
		resp, err := tailStream.Recv()
		if err != nil {
			t.Fatalf("recv error: %v", err)
		}
		if rec := resp.GetRecord(); rec != nil {
			receivedSeqs = append(receivedSeqs, rec.StreamSeq)
		}
	}

	for i := 0; i < 25; i++ {
		expected := uint64(i + 1)
		if receivedSeqs[i] != expected {
			t.Fatalf("seq mismatch at index %d: got %d, want %d", i, receivedSeqs[i], expected)
		}
	}

	// 5. Test Tail from mid-stream sequence 15: must return 16..25
	tailCtx2, tailCancel2 := context.WithCancel(ctx)
	defer tailCancel2()

	tailStream2, err := client.Tail(tailCtx2, &pb.TailRequest{
		StreamId:      streamID[:],
		FromStreamSeq: 15,
	})
	if err != nil {
		t.Fatalf("Tail mid-stream failed: %v", err)
	}

	var midSeqs []uint64
	for len(midSeqs) < 10 {
		resp, err := tailStream2.Recv()
		if err != nil {
			t.Fatalf("recv mid error: %v", err)
		}
		if rec := resp.GetRecord(); rec != nil {
			midSeqs = append(midSeqs, rec.StreamSeq)
		}
	}
	for i := 0; i < 10; i++ {
		expected := uint64(16 + i)
		if midSeqs[i] != expected {
			t.Fatalf("mid seq mismatch at index %d: got %d, want %d", i, midSeqs[i], expected)
		}
	}
}

// TestCrashRecoveryAndSplitResumption tests that a torn trailing record in an open file
// is safely truncated back to the last valid boundary on crash recovery, missing records are
// re-derived from the aggregated log, and subsequent operations and idempotent uploads proceed smoothly.
func TestCrashRecoveryAndSplitResumption(t *testing.T) {
	ctx := t.Context()
	backend := inmemorystorage.New()
	dataDir := t.TempDir()

	streamID := uuid.New()

	// 1. Start server, write records 1..5, flush to aggregated WAL
	srv1, err := NewServer(ctx, ServerConfig{
		Backend:       backend,
		DataDir:       dataDir,
		FlushInterval: 10 * time.Second,
		BatchMaxDelay: 1 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create server 1: %v", err)
	}

	for i := uint64(1); i <= 5; i++ {
		ackChan := make(chan ackResult, 1)
		srv1.incomingChan <- incomingItem{
			streamID:  streamID,
			streamSeq: i,
			payload:   []byte(fmt.Sprintf("crash-rec-%d", i)),
			ackChan:   ackChan,
		}
		<-ackChan
	}
	if _, err := srv1.Flush(ctx, &pb.FlushRequest{}); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	_ = srv1.Close()

	// 2. Corrupt the open file by appending a torn partial record at the end
	openPath := filepath.Join(dataDir, "streams", streamID.String(), wal.FormatStreamFileName(1, 0, false))
	corruptBytes := []byte{0x57, 0x41, 0x4c, 0x4c, 0x00, 0x00} // partial WALL header
	f, err := os.OpenFile(openPath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("failed to open file for corruption: %v", err)
	}
	_, _ = f.Write(corruptBytes)
	_ = f.Close()

	// 3. Restart server on the same dataDir: recovery must truncate the torn record and re-derive missing tail
	srv2, err := NewServer(ctx, ServerConfig{
		Backend:       backend,
		DataDir:       dataDir,
		FlushInterval: 10 * time.Second,
		BatchMaxDelay: 1 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to restart server: %v", err)
	}
	defer srv2.Close()

	// Verify .recover backup was created
	dirEntries, _ := os.ReadDir(filepath.Join(dataDir, "streams", streamID.String()))
	foundRecover := false
	for _, e := range dirEntries {
		if strings.Contains(e.Name(), ".recover") {
			foundRecover = true
			break
		}
	}
	if !foundRecover {
		t.Fatalf("expected .recover backup file created for corrupted segment")
	}

	// 4. Append 5 more records (6..10)
	for i := uint64(6); i <= 10; i++ {
		ackChan := make(chan ackResult, 1)
		srv2.incomingChan <- incomingItem{
			streamID:  streamID,
			streamSeq: i,
			payload:   []byte(fmt.Sprintf("crash-rec-%d", i)),
			ackChan:   ackChan,
		}
		<-ackChan
	}

	// 5. Seal stream: must create from00000000000000000001-to00000000000000000010.wal
	if err := srv2.SealStream(ctx, streamID); err != nil {
		t.Fatalf("SealStream failed after recovery: %v", err)
	}

	objKey := wal.FormatStreamObjectKey(streamID, 1, 10)
	var sealedBuf bytes.Buffer
	if err := backend.GetObject(ctx, "", objKey, 0, 0, &sealedBuf); err != nil {
		t.Fatalf("sealed file not found in object storage: %v", err)
	}

	// Decode all records from the sealed object: must contain all 10 records without corruption
	r := bytes.NewReader(sealedBuf.Bytes())
	if _, _, err := wal.ReadSegmentHeader(r, false); err != nil {
		t.Fatalf("ReadSegmentHeader failed: %v", err)
	}
	var decodedSeqs []uint64
	for {
		rec, err := wal.DecodeLogRecord(r)
		if err != nil {
			break
		}
		decodedSeqs = append(decodedSeqs, rec.StreamSeq)
	}
	if len(decodedSeqs) != 10 {
		t.Fatalf("expected 10 valid records, got %d (%v)", len(decodedSeqs), decodedSeqs)
	}
	for i := 0; i < 10; i++ {
		if decodedSeqs[i] != uint64(i+1) {
			t.Fatalf("seq mismatch at %d: got %d, want %d", i, decodedSeqs[i], i+1)
		}
	}

	// 6. Idempotent upload test: sealing or uploading the same file again must succeed
	if err := srv2.SealStream(ctx, streamID); err != nil {
		t.Fatalf("second seal should be idempotent / no-op: %v", err)
	}
}

// TestSnapshotThenTrimRetention tests the retention policy rules:
// - Never delete stream records that no published snapshot covers (to <= snapshotSeq).
// - The default is to keep everything (no deletion without explicit trim).
func TestSnapshotThenTrimRetention(t *testing.T) {
	ctx := t.Context()
	backend := inmemorystorage.New()
	dataDir := t.TempDir()

	srv, err := NewServer(ctx, ServerConfig{
		Backend:       backend,
		DataDir:       dataDir,
		FlushInterval: 10 * time.Second,
		BatchMaxDelay: 1 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}
	defer srv.Close()

	streamID := uuid.New()

	appendAndSealBatch := func(start, end uint64) {
		for i := start; i <= end; i++ {
			ackChan := make(chan ackResult, 1)
			srv.incomingChan <- incomingItem{
				streamID:  streamID,
				streamSeq: i,
				payload:   []byte(fmt.Sprintf("rec-%d", i)),
				ackChan:   ackChan,
			}
			<-ackChan
		}
		if err := srv.SealStream(ctx, streamID); err != nil {
			t.Fatalf("SealStream failed: %v", err)
		}
	}

	// Create 4 sealed objects: 1..10, 11..20, 21..30, 31..40
	appendAndSealBatch(1, 10)
	appendAndSealBatch(11, 20)
	appendAndSealBatch(21, 30)
	appendAndSealBatch(31, 40)

	key1 := wal.FormatStreamObjectKey(streamID, 1, 10)
	key2 := wal.FormatStreamObjectKey(streamID, 11, 20)
	key3 := wal.FormatStreamObjectKey(streamID, 21, 30)
	key4 := wal.FormatStreamObjectKey(streamID, 31, 40)

	// Case 1: Default policy (trimSeq = 0) -> nothing is deleted
	deleted, err := srv.TrimStreamHistory(ctx, streamID, 0, 25)
	if err != nil || len(deleted) != 0 {
		t.Fatalf("expected 0 deleted with trimSeq=0, got %d, err: %v", len(deleted), err)
	}

	// Case 2: No published snapshot (snapshotSeq = 0), even with trimSeq = 30 -> nothing is deleted
	deleted, err = srv.TrimStreamHistory(ctx, streamID, 30, 0)
	if err != nil || len(deleted) != 0 {
		t.Fatalf("expected 0 deleted with snapshotSeq=0, got %d, err: %v", len(deleted), err)
	}

	// Case 3: Published snapshot at sequence 25 (snapshotSeq = 25), retention policy requests trimSeq = 35.
	// Retention rule: "Never delete stream records that no published snapshot covers."
	// trimSeq is clamped to snapshotSeq (25).
	// Only files where to <= 25 (key1: 1..10, key2: 11..20) may be deleted.
	// key3 (21..30) has records > 25, so it MUST NOT be deleted.
	// key4 (31..40) MUST NOT be deleted.
	deleted, err = srv.TrimStreamHistory(ctx, streamID, 35, 25)
	if err != nil {
		t.Fatalf("TrimStreamHistory failed: %v", err)
	}
	if len(deleted) != 2 {
		t.Fatalf("expected 2 files deleted, got %d (%v)", len(deleted), deleted)
	}

	// Verify key1 and key2 are deleted from backend
	var buf bytes.Buffer
	if err := backend.GetObject(ctx, "", key1, 0, 0, &buf); err == nil {
		t.Fatalf("expected key1 to be deleted from backend")
	}
	buf.Reset()
	if err := backend.GetObject(ctx, "", key2, 0, 0, &buf); err == nil {
		t.Fatalf("expected key2 to be deleted from backend")
	}

	// Verify key3 and key4 are preserved in backend
	buf.Reset()
	if err := backend.GetObject(ctx, "", key3, 0, 0, &buf); err != nil {
		t.Fatalf("expected key3 to be preserved in backend, err: %v", err)
	}
	buf.Reset()
	if err := backend.GetObject(ctx, "", key4, 0, 0, &buf); err != nil {
		t.Fatalf("expected key4 to be preserved in backend, err: %v", err)
	}
}

type faultyBackend struct {
	objectstore.Backend
	failStreamPut bool
}

func (f *faultyBackend) PutObject(ctx context.Context, bucket, key string, stream blob.ByteStream) (string, error) {
	if f.failStreamPut && strings.Contains(key, "streams/") && !strings.Contains(key, "wal/segments/") {
		return "", errors.New("injected stream upload failure")
	}
	return f.Backend.PutObject(ctx, bucket, key, stream)
}

// TestSealUploadFailurePreservesFileSequenceAndRetries tests that a failed upload does NOT
// corrupt the stream's sequence numbering or orphan records, and retries successfully.
func TestSealUploadFailurePreservesFileSequenceAndRetries(t *testing.T) {
	ctx := t.Context()
	rawBackend := inmemorystorage.New()
	backend := &faultyBackend{Backend: rawBackend, failStreamPut: true}
	dataDir := t.TempDir()

	srv, err := NewServer(ctx, ServerConfig{
		Backend:       backend,
		DataDir:       dataDir,
		FlushInterval: 10 * time.Second,
		BatchMaxDelay: 1 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}
	defer srv.Close()

	streamID := uuid.New()

	// 1. Append 5 records (1..5)
	for i := uint64(1); i <= 5; i++ {
		ackChan := make(chan ackResult, 1)
		srv.incomingChan <- incomingItem{
			streamID:  streamID,
			streamSeq: i,
			payload:   []byte(fmt.Sprintf("rec-%d", i)),
			ackChan:   ackChan,
		}
		<-ackChan
	}

	// 2. First seal fails upload because failStreamPut is true
	sealErr := srv.SealStream(ctx, streamID)
	if sealErr == nil {
		t.Fatalf("expected seal to return upload error")
	}

	// Verify state after failed upload:
	// - Local sealed file from00000000000000000001-to00000000000000000005.wal must exist
	file1Path := filepath.Join(dataDir, "streams", streamID.String(), wal.FormatStreamFileName(1, 5, true))
	if _, err := os.Stat(file1Path); err != nil {
		t.Fatalf("expected local sealed file 1..5 on disk: %v", err)
	}

	// - Writer openFrom must be advanced to 6 (NOT stuck at 1)
	w := srv.streamStore.getStream(streamID)
	w.mu.Lock()
	if w.openFrom != 6 {
		t.Fatalf("expected openFrom advanced to 6, got %d", w.openFrom)
	}
	if w.uploadedTo != 0 {
		t.Fatalf("expected uploadedTo to remain 0 after failed upload, got %d", w.uploadedTo)
	}
	w.mu.Unlock()

	// 3. Append 2 more records (6..7)
	for i := uint64(6); i <= 7; i++ {
		ackChan := make(chan ackResult, 1)
		srv.incomingChan <- incomingItem{
			streamID:  streamID,
			streamSeq: i,
			payload:   []byte(fmt.Sprintf("rec-%d", i)),
			ackChan:   ackChan,
		}
		<-ackChan
	}

	// 4. Clear failure and seal again
	backend.failStreamPut = false
	if err := srv.SealStream(ctx, streamID); err != nil {
		t.Fatalf("second seal failed: %v", err)
	}

	// Verify both sealed files exist locally and in object storage
	key1 := wal.FormatStreamObjectKey(streamID, 1, 5)
	key2 := wal.FormatStreamObjectKey(streamID, 6, 7)

	var buf1 bytes.Buffer
	if err := backend.GetObject(ctx, "", key1, 0, 0, &buf1); err != nil {
		t.Fatalf("expected key1 (1..5) in object storage after retry: %v", err)
	}
	var buf2 bytes.Buffer
	if err := backend.GetObject(ctx, "", key2, 0, 0, &buf2); err != nil {
		t.Fatalf("expected key2 (6..7) in object storage: %v", err)
	}

	// Decode both objects and verify record counts
	r1 := bytes.NewReader(buf1.Bytes())
	if _, _, err := wal.ReadSegmentHeader(r1, false); err != nil {
		t.Fatalf("ReadSegmentHeader failed on file 1..5: %v", err)
	}
	var recs1 []*wal.LogRecord
	for {
		rec, err := wal.DecodeLogRecord(r1)
		if err != nil {
			break
		}
		recs1 = append(recs1, rec)
	}
	if len(recs1) != 5 {
		t.Fatalf("expected 5 records in file 1..5, got %d", len(recs1))
	}

	r2 := bytes.NewReader(buf2.Bytes())
	if _, _, err := wal.ReadSegmentHeader(r2, false); err != nil {
		t.Fatalf("ReadSegmentHeader failed on file 6..7: %v", err)
	}
	var recs2 []*wal.LogRecord
	for {
		rec, err := wal.DecodeLogRecord(r2)
		if err != nil {
			break
		}
		recs2 = append(recs2, rec)
	}
	if len(recs2) != 2 {
		t.Fatalf("expected 2 records in file 6..7, got %d", len(recs2))
	}

	w.mu.Lock()
	if w.uploadedTo != 7 {
		t.Fatalf("expected uploadedTo = 7, got %d", w.uploadedTo)
	}
	w.mu.Unlock()
}

// TestAggregatedSegmentGCPreservedDuringUploadFailure tests that GC does NOT delete
// an aggregated segment while per-stream uploads are still failing.
func TestAggregatedSegmentGCPreservedDuringUploadFailure(t *testing.T) {
	ctx := t.Context()
	rawBackend := inmemorystorage.New()
	backend := &faultyBackend{Backend: rawBackend, failStreamPut: true}
	dataDir := t.TempDir()

	srv, err := NewServer(ctx, ServerConfig{
		Backend:       backend,
		DataDir:       dataDir,
		FlushInterval: 10 * time.Second,
		BatchMaxDelay: 1 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}
	defer srv.Close()

	streamID := uuid.New()

	for i := uint64(1); i <= 5; i++ {
		ackChan := make(chan ackResult, 1)
		srv.incomingChan <- incomingItem{
			streamID:  streamID,
			streamSeq: i,
			payload:   []byte(fmt.Sprintf("rec-%d", i)),
			ackChan:   ackChan,
		}
		<-ackChan
	}

	// Flush aggregated segment 1..5 to object storage
	if _, err := srv.Flush(ctx, &pb.FlushRequest{}); err != nil {
		t.Fatalf("flush failed: %v", err)
	}
	segPath := fmt.Sprintf("%s%012d-%012d.wal", SegmentsPrefix, 1, 5)

	// Per-stream upload fails
	if err := srv.SealStream(ctx, streamID); err == nil {
		t.Fatalf("expected seal error during upload failure")
	}

	// GC check must NOT delete the aggregated segment because upload failed
	srv.checkAggregatedSegmentGC(ctx)

	var segBuf bytes.Buffer
	if err := backend.GetObject(ctx, "", segPath, 0, 0, &segBuf); err != nil {
		t.Fatalf("aggregated segment %s MUST NOT be deleted while upload is failing: %v", segPath, err)
	}

	// Clear failure, retry seal/upload
	backend.failStreamPut = false
	if err := srv.SealStream(ctx, streamID); err != nil {
		t.Fatalf("retry seal failed: %v", err)
	}

	// Now GC check must delete the aggregated segment
	srv.checkAggregatedSegmentGC(ctx)
	segBuf.Reset()
	if err := backend.GetObject(ctx, "", segPath, 0, 0, &segBuf); err == nil {
		t.Fatalf("expected aggregated segment %s to be deleted once upload succeeded", segPath)
	}
}
