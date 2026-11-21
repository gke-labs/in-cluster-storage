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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
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
		StreamSealInterval: 10 * time.Second,
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
		appendDirectRecord(t, srv, streamIDA, i, []byte(fmt.Sprintf("rec-A-%d", i)))
	}

	// Trigger manual seal on Stream A
	if err := srv.SealStream(ctx, streamIDA); err != nil {
		t.Fatalf("SealStream failed: %v", err)
	}

	// Check object storage: Stream A must have sealed file
	entriesA, err := backend.ListObjects(ctx, "", fmt.Sprintf("streams/%s/", streamIDA.String()))
	if err != nil {
		t.Fatalf("ListObjects failed: %v", err)
	}
	if len(entriesA) != 1 {
		t.Fatalf("expected 1 sealed object for stream A, got %d (%v)", len(entriesA), entriesA)
	}

	expectedKeyA := wal.FormatStreamObjectKey(streamIDA, 1, 10)
	if entriesA[0].Key != expectedKeyA {
		t.Fatalf("unexpected object key: got %s, want %s", entriesA[0].Key, expectedKeyA)
	}

	// Check idle Stream B: sealing must be a no-op (no files produced)
	if err := srv.SealStream(ctx, streamIDB); err != nil {
		t.Fatalf("SealStream on idle stream B failed: %v", err)
	}
	entriesB, err := backend.ListObjects(ctx, "", fmt.Sprintf("streams/%s/", streamIDB.String()))
	if err != nil {
		t.Fatalf("ListObjects for stream B failed: %v", err)
	}
	if len(entriesB) != 0 {
		t.Fatalf("expected 0 sealed objects for idle stream B, got %d (%v)", len(entriesB), entriesB)
	}

	// Append next batch to Stream A (records 11..15)
	for i := uint64(11); i <= 15; i++ {
		appendDirectRecord(t, srv, streamIDA, i, []byte(fmt.Sprintf("rec-A-%d", i)))
	}

	// Seal again
	if err := srv.SealStream(ctx, streamIDA); err != nil {
		t.Fatalf("SealStream 2 failed: %v", err)
	}

	entriesA2, err := backend.ListObjects(ctx, "", fmt.Sprintf("streams/%s/", streamIDA.String()))
	if err != nil {
		t.Fatalf("ListObjects 2 failed: %v", err)
	}
	if len(entriesA2) != 2 {
		t.Fatalf("expected 2 sealed objects for stream A, got %d (%v)", len(entriesA2), entriesA2)
	}
	var keysA2 []string
	for _, entry := range entriesA2 {
		keysA2 = append(keysA2, entry.Key)
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
		appendDirectRecord(t, srv, streamIDA, i, []byte(fmt.Sprintf("A-%d", i)))

		appendDirectRecord(t, srv, streamIDB, i, []byte(fmt.Sprintf("B-%d", i)))
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
		appendDirectRecord(t, srv1, streamID, i, []byte(fmt.Sprintf("crash-rec-%d", i)))
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
		appendDirectRecord(t, srv2, streamID, i, []byte(fmt.Sprintf("crash-rec-%d", i)))
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
			appendDirectRecord(t, srv, streamID, i, []byte(fmt.Sprintf("rec-%d", i)))
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
		appendDirectRecord(t, srv, streamID, i, []byte(fmt.Sprintf("rec-%d", i)))
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
		appendDirectRecord(t, srv, streamID, i, []byte(fmt.Sprintf("rec-%d", i)))
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
		appendDirectRecord(t, srv, streamID, i, []byte(fmt.Sprintf("rec-%d", i)))
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

func TestTailOnEmptyStreamReceivesSubsequentAppends(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	backend := inmemorystorage.New()
	_, addr, cleanup := startTestServer(t, backend, t.TempDir())
	defer cleanup()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	client := pb.NewWalBufferClient(conn)
	streamID := uuid.New()

	// 1. Start Tail on empty stream before any appends occur
	tailStream, err := client.Tail(ctx, &pb.TailRequest{
		StreamId:      streamID[:],
		FromStreamSeq: 0,
	})
	if err != nil {
		t.Fatalf("Tail failed: %v", err)
	}

	// 2. Append first record to this stream
	appendStream, err := client.Append(ctx)
	if err != nil {
		t.Fatalf("Append start failed: %v", err)
	}
	if err := appendStream.Send(&pb.AppendRequest{
		Msg: &pb.AppendRequest_Hello{
			Hello: &pb.Hello{StreamId: streamID[:]},
		},
	}); err != nil {
		t.Fatalf("Hello send failed: %v", err)
	}
	if _, err := appendStream.Recv(); err != nil {
		t.Fatalf("HelloAck recv failed: %v", err)
	}

	if err := appendStream.Send(&pb.AppendRequest{
		Msg: &pb.AppendRequest_Record{
			Record: &pb.AppendRecord{
				StreamSeq: 1,
				Payload:   []byte("payload-1"),
			},
		},
	}); err != nil {
		t.Fatalf("Record send failed: %v", err)
	}
	if _, err := appendStream.Recv(); err != nil {
		t.Fatalf("Ack recv failed: %v", err)
	}

	// 3. Tail stream must receive the record
	resp, err := tailStream.Recv()
	if err != nil {
		t.Fatalf("TailStream Recv failed: %v", err)
	}
	if resp.GetRecord() == nil || resp.GetRecord().GetStreamSeq() != 1 {
		t.Fatalf("unexpected record: %+v", resp.GetRecord())
	}
}

type countingSegmentBackend struct {
	objectstore.Backend
	readSegmentCalls int
}

func (c *countingSegmentBackend) GetObject(ctx context.Context, bucket, key string, offset, count int64, w io.Writer) error {
	if strings.HasPrefix(key, SegmentsPrefix) {
		c.readSegmentCalls++
	}
	return c.Backend.GetObject(ctx, bucket, key, offset, count, w)
}

type retentionBackend struct {
	objectstore.Backend
}

func (r *retentionBackend) DeleteObject(ctx context.Context, bucket, key string) error {
	if strings.HasPrefix(key, SegmentsPrefix) {
		return nil // retain aggregated segments in object storage
	}
	return r.Backend.DeleteObject(ctx, bucket, key)
}

// TestBufferRestart_ReadsOnlyUncoveredSegments tests that a buffer restart with N aggregated segments,
// most covered by sealed per-stream files, reads ONLY the uncovered tail during startup recovery,
// while preserving unsealed streams, and that a fresh data dir reads every segment.
func TestBufferRestart_ReadsOnlyUncoveredSegments(t *testing.T) {
	ctx := t.Context()
	baseBackend := inmemorystorage.New()
	retBackend := &retentionBackend{Backend: baseBackend}
	dataDir1 := t.TempDir()

	streamIDA := uuid.New()
	streamIDB := uuid.New()

	// 1. Start server 1
	srv1, err := NewServer(ctx, ServerConfig{
		Backend:       retBackend,
		DataDir:       dataDir1,
		FlushInterval: 10 * time.Second,
		BatchMaxDelay: 1 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create server 1: %v", err)
	}

	// Append 8 batches of 10 records for Stream A (records 1..80, positions 1..80)
	var currentSeqA uint64
	for batch := 0; batch < 8; batch++ {
		for i := 0; i < 10; i++ {
			currentSeqA++
			appendDirectRecord(t, srv1, streamIDA, currentSeqA, []byte(fmt.Sprintf("rec-a-%d", currentSeqA)))
		}
		if _, err := srv1.Flush(ctx, &pb.FlushRequest{}); err != nil {
			t.Fatalf("Flush batch %d failed: %v", batch, err)
		}
	}

	// Seal stream A: creates sealed file covering records 1..80 (positions 1..80)
	if err := srv1.SealStream(ctx, streamIDA); err != nil {
		t.Fatalf("SealStream failed: %v", err)
	}

	// Append 2 more batches in the tail:
	// Stream A appends records 81..100.
	// Stream B (young unsealed stream) appends records 1..20 and NEVER seals.
	var currentSeqB uint64
	for batch := 8; batch < 10; batch++ {
		for i := 0; i < 10; i++ {
			currentSeqA++
			appendDirectRecord(t, srv1, streamIDA, currentSeqA, []byte(fmt.Sprintf("rec-a-%d", currentSeqA)))

			currentSeqB++
			appendDirectRecord(t, srv1, streamIDB, currentSeqB, []byte(fmt.Sprintf("rec-b-%d", currentSeqB)))
		}
		if _, err := srv1.Flush(ctx, &pb.FlushRequest{}); err != nil {
			t.Fatalf("Flush tail batch %d failed: %v", batch, err)
		}
	}

	if err := srv1.Close(); err != nil {
		t.Fatalf("server 1 close failed: %v", err)
	}

	// 2. Restart on the SAME dataDir: coverage.json exists, skips 8 covered segments, reads only 2 uncovered
	counting1 := &countingSegmentBackend{Backend: retBackend}
	srv2, err := NewServer(ctx, ServerConfig{
		Backend:       counting1,
		DataDir:       dataDir1,
		FlushInterval: 10 * time.Second,
		BatchMaxDelay: 1 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create server 2: %v", err)
	}
	defer srv2.Close()

	if counting1.readSegmentCalls != 2 {
		t.Fatalf("expected exactly 2 ReadSegmentFromBackend calls for uncovered tail on same dataDir, got %d", counting1.readSegmentCalls)
	}
	if srv2.lastFlushedPosition != 120 {
		t.Errorf("expected lastFlushedPosition 120, got %d", srv2.lastFlushedPosition)
	}
	if stA := srv2.getOrCreateStream(ctx, streamIDA); stA.witnessSeq != 100 {
		t.Errorf("expected stream A witnessSeq 100, got %d", stA.witnessSeq)
	}
	if srv2.streams[streamIDB.String()].witnessSeq != 20 {
		t.Errorf("expected stream B witnessSeq 20, got %d", srv2.streams[streamIDB.String()].witnessSeq)
	}

	// Verify tailPerStream receives all 100 records for Stream A on restarted server
	tailCtxA, tailCancelA := context.WithCancel(ctx)
	defer tailCancelA()
	var receivedSeqsA []uint64
	mockStreamA := &mockCollectingTailServer{
		ctx: tailCtxA,
		onSend: func(resp *pb.TailResponse) {
			if rec := resp.GetRecord(); rec != nil {
				receivedSeqsA = append(receivedSeqsA, rec.StreamSeq)
				if len(receivedSeqsA) == 100 {
					tailCancelA()
				}
			}
		},
	}
	_ = srv2.tailPerStream(tailCtxA, streamIDA, &pb.TailRequest{FromStreamSeq: 0}, mockStreamA)
	if len(receivedSeqsA) != 100 {
		t.Fatalf("expected 100 records received via tail for Stream A, got %d", len(receivedSeqsA))
	}
	for i := 0; i < 100; i++ {
		if receivedSeqsA[i] != uint64(i+1) {
			t.Fatalf("stream A seq mismatch at index %d: got %d, want %d", i, receivedSeqsA[i], i+1)
		}
	}

	// Verify tailPerStream receives all 20 records for young unsealed Stream B on restarted server
	tailCtxB, tailCancelB := context.WithCancel(ctx)
	defer tailCancelB()
	var receivedSeqsB []uint64
	mockStreamB := &mockCollectingTailServer{
		ctx: tailCtxB,
		onSend: func(resp *pb.TailResponse) {
			if rec := resp.GetRecord(); rec != nil {
				receivedSeqsB = append(receivedSeqsB, rec.StreamSeq)
				if len(receivedSeqsB) == 20 {
					tailCancelB()
				}
			}
		},
	}
	_ = srv2.tailPerStream(tailCtxB, streamIDB, &pb.TailRequest{FromStreamSeq: 0}, mockStreamB)
	if len(receivedSeqsB) != 20 {
		t.Fatalf("expected 20 records received via tail for Stream B, got %d", len(receivedSeqsB))
	}
	for i := 0; i < 20; i++ {
		if receivedSeqsB[i] != uint64(i+1) {
			t.Fatalf("stream B seq mismatch at index %d: got %d, want %d", i, receivedSeqsB[i], i+1)
		}
	}

	// 3. Restart on a FRESH dataDir (no coverage.json exists): must read EVERY segment (all 10)
	counting2 := &countingSegmentBackend{Backend: retBackend}
	srv3, err := NewServer(ctx, ServerConfig{
		Backend:       counting2,
		DataDir:       t.TempDir(),
		FlushInterval: 10 * time.Second,
		BatchMaxDelay: 1 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create server 3: %v", err)
	}
	defer srv3.Close()

	if counting2.readSegmentCalls != 10 {
		t.Fatalf("expected exactly 10 ReadSegmentFromBackend calls on fresh dataDir, got %d", counting2.readSegmentCalls)
	}
	if srv3.lastFlushedPosition != 120 {
		t.Errorf("expected lastFlushedPosition 120 on fresh dataDir, got %d", srv3.lastFlushedPosition)
	}
	if srv3.streams[streamIDA.String()].witnessSeq != 100 {
		t.Errorf("expected stream A witnessSeq 100 on fresh dataDir, got %d", srv3.streams[streamIDA.String()].witnessSeq)
	}
	if srv3.streams[streamIDB.String()].witnessSeq != 20 {
		t.Errorf("expected stream B witnessSeq 20 on fresh dataDir, got %d", srv3.streams[streamIDB.String()].witnessSeq)
	}
}

// TestBufferRestart_InterleavedUnsealedStreamNotSkipped verifies that when an unsealed stream
// writes records interleaved across segments with another stream that seals, the segments are NOT
// skipped on restart, preserving the unsealed stream's records and witness watermark.
func TestBufferRestart_InterleavedUnsealedStreamNotSkipped(t *testing.T) {
	ctx := t.Context()
	baseBackend := inmemorystorage.New()
	retBackend := &retentionBackend{Backend: baseBackend}
	dataDir := t.TempDir()

	streamIDA := uuid.New()
	streamIDB := uuid.New()

	srv1, err := NewServer(ctx, ServerConfig{
		Backend:       retBackend,
		DataDir:       dataDir,
		FlushInterval: 10 * time.Second,
		BatchMaxDelay: 1 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create server 1: %v", err)
	}

	// Both streams write interleaved records in segments 1..5
	var seqA, seqB uint64
	for batch := 0; batch < 5; batch++ {
		for i := 0; i < 5; i++ {
			seqA++
			appendDirectRecord(t, srv1, streamIDA, seqA, []byte(fmt.Sprintf("rec-a-%d", seqA)))

			seqB++
			appendDirectRecord(t, srv1, streamIDB, seqB, []byte(fmt.Sprintf("rec-b-%d", seqB)))
		}
		if _, err := srv1.Flush(ctx, &pb.FlushRequest{}); err != nil {
			t.Fatalf("Flush batch %d failed: %v", batch, err)
		}
	}

	// Seal stream A only; stream B remains unsealed.
	if err := srv1.SealStream(ctx, streamIDA); err != nil {
		t.Fatalf("SealStream A failed: %v", err)
	}

	if err := srv1.Close(); err != nil {
		t.Fatalf("server 1 close failed: %v", err)
	}

	// Restart on the same dataDir: because Stream B has unsealed records from pos 2,
	// covered position must be 0 and all 5 segments must be read.
	counting := &countingSegmentBackend{Backend: retBackend}
	srv2, err := NewServer(ctx, ServerConfig{
		Backend:       counting,
		DataDir:       dataDir,
		FlushInterval: 10 * time.Second,
		BatchMaxDelay: 1 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create server 2: %v", err)
	}
	defer srv2.Close()

	if counting.readSegmentCalls != 5 {
		t.Fatalf("expected all 5 segments read when unsealed stream has interleaved records, got %d", counting.readSegmentCalls)
	}
	if srv2.streams[streamIDB.String()] == nil {
		t.Fatalf("stream B must be known on server 2")
	}
	if srv2.streams[streamIDB.String()].witnessSeq != 25 {
		t.Errorf("expected stream B witnessSeq 25, got %d", srv2.streams[streamIDB.String()].witnessSeq)
	}
}

type mockCollectingTailServer struct {
	grpc.ServerStream
	ctx    context.Context
	onSend func(*pb.TailResponse)
}

func (m *mockCollectingTailServer) Send(resp *pb.TailResponse) error {
	if m.onSend != nil {
		m.onSend(resp)
	}
	return nil
}

func (m *mockCollectingTailServer) Context() context.Context {
	return m.ctx
}

// TestStartup_10kSegments verifies startup with 10k aggregated segments
// where sealed per-stream files cover all but the tail.
func TestStartup_10kSegments(t *testing.T) {
	ctx := t.Context()
	backend := inmemorystorage.New()

	streamID := uuid.New()
	totalSegments := 10000
	coveredSegments := 9990

	// 1. Seed 10k aggregated segments in backend
	for i := 1; i <= totalSegments; i++ {
		pos := uint64(i)
		rec := &wal.LogRecord{
			Position:  pos,
			StreamID:  streamID,
			StreamSeq: pos,
			Payload:   []byte("meas-rec"),
		}
		var buf bytes.Buffer
		buf.Write(wal.DefaultSegmentHeader().Encode())
		buf.Write(rec.Encode())

		segKey := fmt.Sprintf("%s%012d-%012d.wal", SegmentsPrefix, pos, pos)
		if _, err := backend.PutObject(ctx, "", segKey, blob.NewByteStreamFromBytes(buf.Bytes())); err != nil {
			t.Fatalf("failed to put segment %d: %v", i, err)
		}
	}

	// 2. Seed sealed per-stream file covering 1..9990 and persist coverage.json
	dataDir := t.TempDir()
	cov := StreamCoverage{
		CoveredPos: uint64(coveredSegments),
	}
	covData, err := json.Marshal(cov)
	if err != nil {
		t.Fatalf("failed to marshal coverage: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "coverage.json"), covData, 0644); err != nil {
		t.Fatalf("failed to write coverage.json: %v", err)
	}

	// 3. Narrowed startup: reading only 10 uncovered segments
	startNarrowed := time.Now()
	srvNarrowed, err := NewServer(ctx, ServerConfig{
		Backend:       backend,
		DataDir:       dataDir,
		FlushInterval: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("failed to create narrowed server: %v", err)
	}
	durationNarrowed := time.Since(startNarrowed)
	defer srvNarrowed.Close()

	if srvNarrowed.lastFlushedPosition != uint64(totalSegments) {
		t.Errorf("expected lastFlushedPosition %d, got %d", totalSegments, srvNarrowed.lastFlushedPosition)
	}
	if srvNarrowed.streams[streamID.String()].witnessSeq != uint64(totalSegments) {
		t.Errorf("expected witnessSeq %d, got %d", totalSegments, srvNarrowed.streams[streamID.String()].witnessSeq)
	}

	// 4. Measure full scan startup time when OBJECTFS_BENCH is set
	if os.Getenv("OBJECTFS_BENCH") != "" {
		startFull := time.Now()
		srvFull, err := NewServer(ctx, ServerConfig{
			Backend:       backend,
			DataDir:       t.TempDir(), // fresh dir = no coverage.json
			FlushInterval: 10 * time.Second,
		})
		if err != nil {
			t.Fatalf("failed to create full server: %v", err)
		}
		durationFull := time.Since(startFull)
		defer srvFull.Close()

		t.Logf("Measured 10,000 aggregated segments startup: full_scan=%v vs narrowed_uncovered=%v (speedup: %.1fx)",
			durationFull, durationNarrowed, float64(durationFull)/float64(durationNarrowed))
	}
}

func BenchmarkStartup_10kSegments(b *testing.B) {
	ctx := b.Context()
	backend := inmemorystorage.New()

	streamID := uuid.New()
	totalSegments := 10000
	coveredSegments := 9990

	for i := 1; i <= totalSegments; i++ {
		pos := uint64(i)
		rec := &wal.LogRecord{
			Position:  pos,
			StreamID:  streamID,
			StreamSeq: pos,
			Payload:   []byte("meas-rec"),
		}
		var buf bytes.Buffer
		buf.Write(wal.DefaultSegmentHeader().Encode())
		buf.Write(rec.Encode())

		segKey := fmt.Sprintf("%s%012d-%012d.wal", SegmentsPrefix, pos, pos)
		if _, err := backend.PutObject(ctx, "", segKey, blob.NewByteStreamFromBytes(buf.Bytes())); err != nil {
			b.Fatalf("failed to put segment %d: %v", i, err)
		}
	}

	dataDir := b.TempDir()
	cov := StreamCoverage{
		CoveredPos: uint64(coveredSegments),
	}
	covData, err := json.Marshal(cov)
	if err != nil {
		b.Fatalf("failed to marshal coverage: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "coverage.json"), covData, 0644); err != nil {
		b.Fatalf("failed to write coverage.json: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		srv, err := NewServer(ctx, ServerConfig{
			Backend:       backend,
			DataDir:       dataDir,
			FlushInterval: 10 * time.Second,
		})
		if err != nil {
			b.Fatalf("failed to create server: %v", err)
		}
		_ = srv.Close()
	}
}

// TestCoverage_IndependentOfStreamCount verifies that coverage.json file size
// does not grow with the number of streams seen, and startup does not materialize
// streamWriter instances for idle/covered streams.
func TestCoverage_IndependentOfStreamCount(t *testing.T) {
	ctx := t.Context()
	backend := inmemorystorage.New()
	dataDir := t.TempDir()

	const totalStreams = 20
	streamIDs := make([]uuid.UUID, totalStreams)
	for i := 0; i < totalStreams; i++ {
		streamIDs[i] = uuid.New()
	}

	srv, err := NewServer(ctx, ServerConfig{
		Backend:       backend,
		DataDir:       dataDir,
		FlushInterval: 10 * time.Second,
		BatchMaxDelay: 1 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	// Append records to all N streams and flush them
	for _, sid := range streamIDs {
		appendDirectRecord(t, srv, sid, 1, []byte("test-payload"))
	}

	if _, err := srv.Flush(ctx, &pb.FlushRequest{}); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Seal all N streams
	for _, sid := range streamIDs {
		if err := srv.SealStream(ctx, sid); err != nil {
			t.Fatalf("SealStream failed: %v", err)
		}
	}

	// Append unsealed record to only the first stream (streamIDs[0])
	activeID := streamIDs[0]
	appendDirectRecord(t, srv, activeID, 2, []byte("active-unsealed-record"))

	// Run seal passes: first pass marks idle streams, second pass evicts them
	srv.streamStore.checkSealsAndGC(ctx)
	srv.streamStore.checkSealsAndGC(ctx)

	// Check coverage.json file size on disk: must be small scalar JSON (< 50 bytes), independent of N
	covPath := filepath.Join(dataDir, "coverage.json")
	covData, err := os.ReadFile(covPath)
	if err != nil {
		t.Fatalf("failed to read coverage.json: %v", err)
	}
	if len(covData) > 50 {
		t.Errorf("expected coverage.json size < 50 bytes for scalar coverage, got %d bytes: %s", len(covData), string(covData))
	}

	var rawMap map[string]any
	if err := json.Unmarshal(covData, &rawMap); err != nil {
		t.Fatalf("failed to unmarshal coverage: %v", err)
	}
	if _, hasStreams := rawMap["streams"]; hasStreams {
		t.Errorf("coverage.json must not persist per-stream map")
	}
	if _, hasPositions := rawMap["positions"]; hasPositions {
		t.Errorf("coverage.json must not persist per-stream positions")
	}

	// Verify in-memory writer count is bounded by active streams (only activeID has records/open file)
	writers := srv.streamStore.allWriters()
	if len(writers) != 1 {
		t.Errorf("expected exactly 1 active stream writer in memory, got %d", len(writers))
	}

	if err := srv.Close(); err != nil {
		t.Fatalf("server close failed: %v", err)
	}

	// Verify startup with coverage.json and N streams in backend creates no writers for covered streams
	dataDir2 := t.TempDir()
	if err := os.WriteFile(filepath.Join(dataDir2, "coverage.json"), covData, 0644); err != nil {
		t.Fatalf("failed to write coverage.json to dataDir2: %v", err)
	}
	srvFresh, err := NewServer(ctx, ServerConfig{
		Backend:       backend,
		DataDir:       dataDir2,
		FlushInterval: 10 * time.Second,
		BatchMaxDelay: 1 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to start server on dataDir2: %v", err)
	}
	defer srvFresh.Close()

	freshWriters := srvFresh.streamStore.allWriters()
	if len(freshWriters) != 1 {
		t.Errorf("expected startup to create exactly 1 writer for unsealed stream among %d streams, got %d", totalStreams, len(freshWriters))
	}

	// On restart on dataDir2:
	// Verify that contacting an idle stream discovers its sealed state
	idleID := streamIDs[1]
	st := srvFresh.getOrCreateStream(ctx, idleID)
	if st.witnessSeq != 1 {
		t.Errorf("expected idle stream witnessSeq 1 after discovery, got %d", st.witnessSeq)
	}
}

// TestSegmentGC_UnknownStreamNotDeletedUntilDiscovery verifies that CheckSegmentGC does
// not delete an aggregated segment on the strength of a stream it has not yet rebuilt,
// treating an unknown stream as not covered until its state is discovered.
func TestSegmentGC_UnknownStreamNotDeletedUntilDiscovery(t *testing.T) {
	ctx := t.Context()
	backend := inmemorystorage.New()
	dataDir := t.TempDir()

	streamX := uuid.New()

	// 1. Put an aggregated segment in backend containing records for Stream X
	segPath := fmt.Sprintf("%s%012d-%012d.wal", SegmentsPrefix, 1, 10)
	var segRecords []*wal.LogRecord
	for i := uint64(1); i <= 10; i++ {
		segRecords = append(segRecords, &wal.LogRecord{
			Position:  i,
			StreamID:  streamX,
			StreamSeq: i,
			Payload:   []byte(fmt.Sprintf("record-%d", i)),
		})
	}
	var segBuf bytes.Buffer
	segBuf.Write(wal.DefaultSegmentHeader().Encode())
	for _, r := range segRecords {
		segBuf.Write(r.Encode())
	}
	if _, err := backend.PutObject(ctx, "", segPath, blob.NewByteStreamFromBytes(segBuf.Bytes())); err != nil {
		t.Fatalf("failed to put aggregated segment: %v", err)
	}

	// 2. Put a sealed per-stream file in backend covering Stream X (seq 1..10)
	streamObjKey := wal.FormatStreamObjectKey(streamX, 1, 10)
	var streamBuf bytes.Buffer
	streamBuf.Write(wal.DefaultSegmentHeader().Encode())
	for _, r := range segRecords {
		streamBuf.Write(r.Encode())
	}
	if _, err := backend.PutObject(ctx, "", streamObjKey, blob.NewByteStreamFromBytes(streamBuf.Bytes())); err != nil {
		t.Fatalf("failed to put stream segment: %v", err)
	}

	store := NewStreamStore(dataDir, backend, time.Hour, DefaultStreamSealBytes)
	defer func() { _ = store.Close(ctx) }()

	// Track the segment requirements on the store
	store.TrackSegmentCoverage(segPath, segRecords)

	// Stream X is unknown in store (not yet materialized/discovered)
	if store.getStream(streamX) != nil {
		t.Fatalf("expected stream X to be unknown in store initially")
	}

	// Run CheckSegmentGC: segment must NOT be deleted because stream X is unknown
	deleted, err := store.CheckSegmentGC(ctx)
	if err != nil {
		t.Fatalf("CheckSegmentGC failed: %v", err)
	}
	if len(deleted) != 0 {
		t.Fatalf("expected no segments deleted for unknown stream, got: %v", deleted)
	}
	if err := backend.GetObject(ctx, "", segPath, 0, 0, io.Discard); err != nil {
		t.Fatalf("expected aggregated segment to still exist in backend: %v", err)
	}

	// Now discover Stream X
	upTo := store.EnsureDiscovered(ctx, streamX)
	if upTo != 10 {
		t.Errorf("expected uploadedTo 10 after discovery, got %d", upTo)
	}

	// Run CheckSegmentGC again: now segment should be deleted
	deleted, err = store.CheckSegmentGC(ctx)
	if err != nil {
		t.Fatalf("CheckSegmentGC failed after discovery: %v", err)
	}
	if len(deleted) != 1 || deleted[0] != segPath {
		t.Fatalf("expected %s deleted after discovery, got: %v", segPath, deleted)
	}
	if err := backend.GetObject(ctx, "", segPath, 0, 0, io.Discard); err == nil {
		t.Fatalf("expected aggregated segment to be deleted from backend")
	}
}

// TestTail_SurvivesWriterEvictionAndReseal verifies that an already-open Tail does not stall
// when its writer is evicted and subsequently resealed on new appends.
func TestTail_SurvivesWriterEvictionAndReseal(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

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

	appendRecords := func(from, to uint64) {
		for seq := from; seq <= to; seq++ {
			appendDirectRecord(t, srv, streamID, seq, []byte(fmt.Sprintf("rec-%d", seq)))
		}
		if _, err := srv.Flush(ctx, &pb.FlushRequest{}); err != nil {
			t.Fatalf("flush failed: %v", err)
		}
	}

	// 1. Initial records 1..5, sealed
	appendRecords(1, 5)
	if err := srv.SealStream(ctx, streamID); err != nil {
		t.Fatalf("SealStream failed: %v", err)
	}

	// 2. Open tail on the sealed idle stream
	var mu sync.Mutex
	var received []uint64
	receivedChan := make(chan uint64, 20)
	tailCtx, cancelTail := context.WithCancel(ctx)
	defer cancelTail()

	mockStream := &mockCollectingTailServer{
		ctx: tailCtx,
		onSend: func(resp *pb.TailResponse) {
			if rec := resp.GetRecord(); rec != nil {
				mu.Lock()
				received = append(received, rec.StreamSeq)
				mu.Unlock()
				receivedChan <- rec.StreamSeq
			}
		},
	}

	tailErrCh := make(chan error, 1)
	go func() {
		tailErrCh <- srv.tailPerStream(tailCtx, streamID, &pb.TailRequest{FromStreamSeq: 0}, mockStream)
	}()

	waitForSeqs := func(target uint64) {
		for {
			mu.Lock()
			count := len(received)
			var last uint64
			if count > 0 {
				last = received[count-1]
			}
			mu.Unlock()
			if last >= target {
				return
			}
			select {
			case <-receivedChan:
			case err := <-tailErrCh:
				t.Fatalf("tailPerStream exited unexpectedly: %v", err)
			case <-ctx.Done():
				mu.Lock()
				t.Fatalf("timed out waiting for seq %d, received so far: %v", target, received)
				mu.Unlock()
			}
		}
	}

	// Tail should deliver 1..5
	waitForSeqs(5)

	// 3. Run two seal passes: writer becomes idle and is evicted
	srv.streamStore.checkSealsAndGC(ctx)
	srv.streamStore.checkSealsAndGC(ctx)

	if w := srv.streamStore.getStream(streamID); w != nil {
		t.Fatalf("expected stream writer to be evicted after two idle passes")
	}

	// 4. Append 6..8 and flush: new writer created
	appendRecords(6, 8)
	waitForSeqs(8)

	// 5. Seal stream again
	if err := srv.SealStream(ctx, streamID); err != nil {
		t.Fatalf("SealStream 2 failed: %v", err)
	}

	// 6. Append 9..11 and flush: another new writer created
	appendRecords(9, 11)
	waitForSeqs(11)

	// 7. Verify all 1..11 delivered in order
	mu.Lock()
	defer mu.Unlock()
	if len(received) != 11 {
		t.Fatalf("expected 11 records, got %d: %v", len(received), received)
	}
	for i := 0; i < 11; i++ {
		expected := uint64(i + 1)
		if received[i] != expected {
			t.Errorf("record at index %d = %d, want %d", i, received[i], expected)
		}
	}
}

func TestMeasurement_SealPass1kStreams(t *testing.T) {
	if os.Getenv("OBJECTFS_BENCH") == "" {
		t.Skip("skipping 1k streams seal-pass measurement (set OBJECTFS_BENCH=1)")
	}
	ctx := t.Context()
	backend := inmemorystorage.New()
	dataDir := t.TempDir()

	const totalStreams = 1000
	streamIDs := make([]uuid.UUID, totalStreams)
	for i := 0; i < totalStreams; i++ {
		streamIDs[i] = uuid.New()
	}

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

	// Append and seal all 1000 streams
	for _, sid := range streamIDs {
		appendDirectRecord(t, srv, sid, 1, []byte("payload"))
	}
	if _, err := srv.Flush(ctx, &pb.FlushRequest{}); err != nil {
		t.Fatalf("flush failed: %v", err)
	}
	for _, sid := range streamIDs {
		if err := srv.SealStream(ctx, sid); err != nil {
			t.Fatalf("seal failed: %v", err)
		}
	}

	// 1 active stream keeps appending
	activeID := streamIDs[0]
	appendDirectRecord(t, srv, activeID, 2, []byte("payload-2"))

	// Measure pass 1 (before eviction: all 1,000 writers in memory)
	start1 := time.Now()
	srv.streamStore.checkSealsAndGC(ctx)
	durBefore := time.Since(start1)

	// Measure pass 2 (after eviction: only 1 active writer in memory)
	start2 := time.Now()
	srv.streamStore.checkSealsAndGC(ctx)
	durAfter := time.Since(start2)

	activeWriters := len(srv.streamStore.allWriters())
	t.Logf("Measured 1,000 streams seal-pass: before_eviction (%d writers)=%v vs after_eviction (%d writers)=%v (speedup: %.1fx)",
		totalStreams, durBefore, activeWriters, durAfter, float64(durBefore)/float64(durAfter))
}

// TestServerStreams_EvictsIdleEntries verifies that with N idle streams and one active
// stream, the Server.streams map size is strictly bounded by active streams (independent of N)
// after the idle pass.
func TestServerStreams_EvictsIdleEntries(t *testing.T) {
	ctx := t.Context()
	backend := inmemorystorage.New()
	dataDir := t.TempDir()

	const totalStreams = 20
	streamIDs := make([]uuid.UUID, totalStreams)
	for i := 0; i < totalStreams; i++ {
		streamIDs[i] = uuid.New()
	}

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

	// Append and seal all 20 streams
	for _, sid := range streamIDs {
		appendDirectRecord(t, srv, sid, 1, []byte("test-payload"))
	}

	if _, err := srv.Flush(ctx, &pb.FlushRequest{}); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	for _, sid := range streamIDs {
		if err := srv.SealStream(ctx, sid); err != nil {
			t.Fatalf("SealStream failed: %v", err)
		}
	}

	// 1 active stream keeps an unsealed record
	activeID := streamIDs[0]
	appendDirectRecord(t, srv, activeID, 2, []byte("active-unsealed-record"))

	beforeCount := srv.StreamCount()
	if beforeCount != totalStreams {
		t.Fatalf("expected %d streams in server map before eviction, got %d", totalStreams, beforeCount)
	}

	// First pass marks idle streams (clears sealedSinceLastPass), second pass evicts them
	srv.streamStore.checkSealsAndGC(ctx)
	srv.streamStore.checkSealsAndGC(ctx)

	afterCount := srv.StreamCount()
	if afterCount != 1 {
		t.Errorf("expected server streams map size bounded to 1 active stream after eviction, got %d", afterCount)
	}

	srv.mu.RLock()
	_, activeExists := srv.streams[activeID.String()]
	srv.mu.RUnlock()
	if !activeExists {
		t.Errorf("expected active stream %s to remain in server streams map", activeID)
	}

	t.Logf("Measured Server.streams map size with %d streams (19 idle, 1 active): before_eviction=%d, after_eviction=%d",
		totalStreams, beforeCount, afterCount)
}

// TestServerStreams_BlockedWaiterPinsEntry verifies that a client goroutine blocked
// in Wait on streamState.cond pins its streamState entry against idle eviction.
func TestServerStreams_BlockedWaiterPinsEntry(t *testing.T) {
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
	appendDirectRecord(t, srv, streamID, 1, []byte("data"))
	if _, err := srv.Flush(ctx, &pb.FlushRequest{}); err != nil {
		t.Fatalf("flush failed: %v", err)
	}
	if err := srv.SealStream(ctx, streamID); err != nil {
		t.Fatalf("seal failed: %v", err)
	}

	st := srv.getOrCreateStream(ctx, streamID)

	// Block a goroutine in st.cond.Wait() with st.waiters incremented
	ready := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		st.mu.Lock()
		st.waiters++
		close(ready)
		st.cond.Wait()
		st.waiters--
		st.mu.Unlock()
	}()

	<-ready

	// Run idle passes: stream has no pending records and is uploaded, but is pinned by the waiter
	srv.streamStore.checkSealsAndGC(ctx)
	srv.streamStore.checkSealsAndGC(ctx)

	if count := srv.StreamCount(); count != 1 {
		t.Fatalf("expected stream to remain pinned while waiter is blocked, got stream count %d", count)
	}

	// Wake the waiter and wait for goroutine to exit
	st.mu.Lock()
	st.cond.Broadcast()
	st.mu.Unlock()
	<-done

	// Now that waiter has exited (waiters == 0), the next idle pass must evict it
	srv.streamStore.checkSealsAndGC(ctx)
	if count := srv.StreamCount(); count != 0 {
		t.Fatalf("expected stream to be evicted once waiter unblocked, got stream count %d", count)
	}
}

// TestServerStreams_AttachedHandlerPinsEntry verifies that an attached handler
// (attachments > 0) that is not in Wait (waiters == 0) pins its streamState
// entry against idle eviction across multiple idle passes; after it detaches,
// the next pass evicts.
func TestServerStreams_AttachedHandlerPinsEntry(t *testing.T) {
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
	appendDirectRecord(t, srv, streamID, 1, []byte("data"))
	if _, err := srv.Flush(ctx, &pb.FlushRequest{}); err != nil {
		t.Fatalf("flush failed: %v", err)
	}
	if err := srv.SealStream(ctx, streamID); err != nil {
		t.Fatalf("seal failed: %v", err)
	}

	st := srv.getOrCreateStream(ctx, streamID)

	// Attach handler without any goroutine in Wait (waiters == 0)
	st.mu.Lock()
	st.attachments++
	waiters := st.waiters
	st.mu.Unlock()

	if waiters != 0 {
		t.Fatalf("expected waiters == 0, got %d", waiters)
	}

	// Run two idle passes: stream has no pending records and is uploaded,
	// but must be pinned by the attached handler
	srv.streamStore.checkSealsAndGC(ctx)
	srv.streamStore.checkSealsAndGC(ctx)

	if count := srv.StreamCount(); count != 1 {
		t.Fatalf("expected stream to remain pinned while handler is attached, got stream count %d", count)
	}

	// Detach handler
	st.mu.Lock()
	st.attachments--
	st.mu.Unlock()

	// After detaching (attachments == 0, waiters == 0), the next idle pass must evict it
	srv.streamStore.checkSealsAndGC(ctx)
	if count := srv.StreamCount(); count != 0 {
		t.Fatalf("expected stream to be evicted once handler detached, got stream count %d", count)
	}
}

// TestServerStreams_ConnectedAppendSurvivesIdlePasses verifies that an active Append
// connection pins its streamState so that subsequent appends and flushes after
// multiple idle passes continue to advance s3Seq and deliver acks on the same streamState.
func TestServerStreams_ConnectedAppendSurvivesIdlePasses(t *testing.T) {
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
	streamCtx, cancelStream := context.WithCancel(ctx)
	defer cancelStream()

	appendStream, err := client.Append(streamCtx)
	if err != nil {
		t.Fatalf("append failed: %v", err)
	}

	if err := appendStream.Send(&pb.AppendRequest{
		Msg: &pb.AppendRequest_Hello{Hello: &pb.Hello{StreamId: streamID[:]}},
	}); err != nil {
		t.Fatalf("send Hello failed: %v", err)
	}
	resp, err := appendStream.Recv()
	if err != nil {
		t.Fatalf("recv HelloAck failed: %v", err)
	}
	if resp.GetHelloAck() == nil {
		t.Fatalf("expected HelloAck, got %v", resp)
	}

	// Append seq 1..5
	for seq := uint64(1); seq <= 5; seq++ {
		if err := appendStream.Send(&pb.AppendRequest{
			Msg: &pb.AppendRequest_Record{
				Record: &pb.AppendRecord{StreamSeq: seq, Payload: []byte(fmt.Sprintf("data-%d", seq))},
			},
		}); err != nil {
			t.Fatalf("send record %d failed: %v", seq, err)
		}
		ack, err := appendStream.Recv()
		if err != nil {
			t.Fatalf("recv ack %d failed: %v", seq, err)
		}
		if ack.GetAck() == nil || ack.GetAck().WitnessAckedStreamSeq != seq {
			t.Fatalf("unexpected ack %v for seq %d", ack, seq)
		}
	}

	// Flush and seal
	if _, err := srv.Flush(ctx, &pb.FlushRequest{}); err != nil {
		t.Fatalf("flush failed: %v", err)
	}
	if err := srv.SealStream(ctx, streamID); err != nil {
		t.Fatalf("seal failed: %v", err)
	}

	// Receive the S3 ack for seq 5
	s3Ack, err := appendStream.Recv()
	if err != nil {
		t.Fatalf("recv s3Ack failed: %v", err)
	}
	if s3Ack.GetAck() == nil || s3Ack.GetAck().S3AckedStreamSeq != 5 {
		t.Fatalf("expected s3Ack 5, got %v", s3Ack)
	}

	// Run two idle passes while connection is still open
	srv.streamStore.checkSealsAndGC(ctx)
	srv.streamStore.checkSealsAndGC(ctx)

	// Stream must be pinned because the connection is alive
	if count := srv.StreamCount(); count != 1 {
		t.Fatalf("expected stream to remain pinned while connection is open, got %d", count)
	}

	// Now append seq 6: it must be served on the same streamState, flush, and receive S3 ack 6
	if err := appendStream.Send(&pb.AppendRequest{
		Msg: &pb.AppendRequest_Record{
			Record: &pb.AppendRecord{StreamSeq: 6, Payload: []byte("data-6")},
		},
	}); err != nil {
		t.Fatalf("send record 6 failed: %v", err)
	}
	witnessAck6, err := appendStream.Recv()
	if err != nil {
		t.Fatalf("recv witnessAck 6 failed: %v", err)
	}
	if witnessAck6.GetAck() == nil || witnessAck6.GetAck().WitnessAckedStreamSeq != 6 {
		t.Fatalf("expected witnessAck 6, got %v", witnessAck6)
	}

	if _, err := srv.Flush(ctx, &pb.FlushRequest{}); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	s3Ack6, err := appendStream.Recv()
	if err != nil {
		t.Fatalf("recv s3Ack 6 failed: %v", err)
	}
	if s3Ack6.GetAck() == nil || s3Ack6.GetAck().S3AckedStreamSeq != 6 {
		t.Fatalf("expected s3Ack 6, got %v", s3Ack6)
	}

	// Now seal, flush, and close connection
	if err := srv.SealStream(ctx, streamID); err != nil {
		t.Fatalf("seal failed: %v", err)
	}
	cancelStream()

	// Wait briefly for connection handler to exit and detach
	var detachedCount int
	for i := 0; i < 50; i++ {
		time.Sleep(10 * time.Millisecond)
		srv.streamStore.checkSealsAndGC(ctx)
		detachedCount = srv.StreamCount()
		if detachedCount == 0 {
			break
		}
	}
	if detachedCount != 0 {
		t.Fatalf("expected stream to be evicted after connection closed, got count %d", detachedCount)
	}
}

// TestServerStreams_ReconnectAfterEvictionReportsWatermarks verifies that contacting
// an evicted stream seeds watermarks from discovered sealed state equal to pre-eviction watermarks.
func TestServerStreams_ReconnectAfterEvictionReportsWatermarks(t *testing.T) {
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
	for seq := uint64(1); seq <= 5; seq++ {
		appendDirectRecord(t, srv, streamID, seq, []byte(fmt.Sprintf("record-%d", seq)))
	}

	if _, err := srv.Flush(ctx, &pb.FlushRequest{}); err != nil {
		t.Fatalf("flush failed: %v", err)
	}
	if err := srv.SealStream(ctx, streamID); err != nil {
		t.Fatalf("seal failed: %v", err)
	}

	stBefore := srv.getOrCreateStream(ctx, streamID)
	stBefore.mu.Lock()
	witnessBefore := stBefore.witnessSeq
	s3Before := stBefore.s3Seq
	stBefore.mu.Unlock()

	if witnessBefore != 5 || s3Before != 5 {
		t.Fatalf("expected pre-eviction watermarks (5, 5), got (%d, %d)", witnessBefore, s3Before)
	}

	// Run idle passes to evict stream
	srv.streamStore.checkSealsAndGC(ctx)
	srv.streamStore.checkSealsAndGC(ctx)

	if count := srv.StreamCount(); count != 0 {
		t.Fatalf("expected stream to be evicted from server map, got count %d", count)
	}

	// Reconnect: getOrCreateStream discovers sealed state and rebuilds streamState
	stAfter := srv.getOrCreateStream(ctx, streamID)
	if count := srv.StreamCount(); count != 1 {
		t.Fatalf("expected server streams map to hold 1 entry after reconnect, got %d", count)
	}

	stAfter.mu.Lock()
	witnessAfter := stAfter.witnessSeq
	s3After := stAfter.s3Seq
	stAfter.mu.Unlock()

	if witnessAfter != witnessBefore || s3After != s3Before {
		t.Fatalf("expected reconnected watermarks (%d, %d) equal to pre-eviction (%d, %d)",
			witnessAfter, s3After, witnessBefore, s3Before)
	}
}

// TestServerStreams_AttachSessionDuringEvictionAttachedToMapState verifies that a session
// that attaches while eviction runs is attached to the streamState that is in Server.streams
// afterwards, never an orphaned state evicted from the map.
func TestServerStreams_AttachSessionDuringEvictionAttachedToMapState(t *testing.T) {
	ctx := t.Context()
	backend := inmemorystorage.New()
	dataDir := t.TempDir()

	server, err := NewServer(ctx, ServerConfig{
		Backend:       backend,
		DataDir:       dataDir,
		FlushInterval: 10 * time.Second,
		BatchMaxDelay: 1 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}
	defer func() {
		if err := server.Close(); err != nil {
			t.Errorf("server.Close() error: %v", err)
		}
	}()

	// 1. Synchronous ordering: idle stream in map, attachSession then evictIdleStreams.
	// The attached session must pin the streamState in Server.streams against eviction.
	streamID := uuid.New()
	_ = server.getOrCreateStream(ctx, streamID)

	state, _, _, err := server.attachSession(ctx, streamID, 1)
	if err != nil {
		t.Fatalf("attachSession failed: %v", err)
	}

	server.evictIdleStreams()

	server.mu.RLock()
	mapState := server.streams[streamID.String()]
	server.mu.RUnlock()

	if mapState == nil {
		t.Fatal("expected streamState to remain in Server.streams while session is attached, but was evicted")
	}
	if mapState != state {
		t.Fatalf("expected attached streamState %p to be in map, got %p", state, mapState)
	}

	server.detachSession(state)

	server.evictIdleStreams()

	server.mu.RLock()
	mapStateAfter := server.streams[streamID.String()]
	server.mu.RUnlock()

	if mapStateAfter != nil {
		t.Fatalf("expected streamState to be evicted after session detach, but still in map: %p", mapStateAfter)
	}

	// 2. Concurrent execution: attachSession and evictIdleStreams run concurrently across
	// multiple iterations with pre-existing idle streams. Regardless of scheduling,
	// the session that attaches must always be attached to the state in Server.streams afterwards,
	// and a subsequent attachSession for the same stream must return the same state.
	for i := 0; i < 50; i++ {
		concurrentStreamID := uuid.New()
		_ = server.getOrCreateStream(ctx, concurrentStreamID)

		var (
			attached  *streamState
			attachErr error
			wg        sync.WaitGroup
		)

		wg.Add(2)
		go func() {
			defer wg.Done()
			attached, _, _, attachErr = server.attachSession(ctx, concurrentStreamID, 1)
		}()
		go func() {
			defer wg.Done()
			server.evictIdleStreams()
		}()
		wg.Wait()

		if attachErr != nil {
			t.Fatalf("iteration %d: attachSession failed: %v", i, attachErr)
		}
		if attached == nil {
			t.Fatalf("iteration %d: expected non-nil attached streamState", i)
		}

		server.mu.RLock()
		inMap := server.streams[concurrentStreamID.String()]
		server.mu.RUnlock()

		if inMap != attached {
			t.Fatalf("iteration %d: attached state %p is not the state in Server.streams %p (inMap == nil: %v)",
				i, attached, inMap, inMap == nil)
		}

		secondAttached, _, _, err := server.attachSession(ctx, concurrentStreamID, 1)
		if err != nil {
			t.Fatalf("iteration %d: second attachSession failed: %v", i, err)
		}
		if secondAttached != attached {
			t.Fatalf("iteration %d: expected second attachSession to return same state %p, got %p",
				i, attached, secondAttached)
		}

		server.detachSession(attached)
		server.detachSession(secondAttached)
		server.evictIdleStreams()
	}
}

// TestServerStreams_InFlightRecordPinsStateAgainstEviction verifies that a stream state
// with records accounted as in flight (inFlight > 0) is not dropped by idle eviction
// even when no sessions are attached; once commitItems processes the in-flight record,
// the next evictIdleStreams removes the state and inFlight never goes negative.
func TestServerStreams_InFlightRecordPinsStateAgainstEviction(t *testing.T) {
	ctx := t.Context()
	backend := inmemorystorage.New()
	dataDir := t.TempDir()

	server, err := NewServer(ctx, ServerConfig{
		Backend:       backend,
		DataDir:       dataDir,
		FlushInterval: 10 * time.Second,
		BatchMaxDelay: 1 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}
	defer func() {
		if err := server.Close(); err != nil {
			t.Errorf("server.Close() error: %v", err)
		}
	}()

	streamID := uuid.New()
	_ = server.getOrCreateStream(ctx, streamID)

	state, _, _, err := server.attachSession(ctx, streamID, 1)
	if err != nil {
		t.Fatalf("attachSession failed: %v", err)
	}
	server.detachSession(state)

	state.mu.Lock()
	state.inFlight = 1
	state.mu.Unlock()

	// (1) A stream state with one record accounted as in flight and no attached
	// sessions is kept by evictIdleStreams.
	server.evictIdleStreams()

	server.mu.RLock()
	mapState := server.streams[streamID.String()]
	server.mu.RUnlock()

	if mapState == nil {
		t.Fatal("expected streamState to remain in Server.streams while inFlight > 0, but was evicted")
	}
	if mapState != state {
		t.Fatalf("expected streamState %p in map, got %p", state, mapState)
	}

	// Process the queued in-flight record.
	server.commitItems(ctx, []incomingItem{
		{
			streamID:  streamID,
			streamSeq: 0,
			epoch:     1,
			payload:   []byte("queued-record"),
		},
	})

	// (3) inFlight must never go negative.
	state.mu.Lock()
	inFlight := state.inFlight
	state.mu.Unlock()

	if inFlight != 0 {
		t.Fatalf("expected inFlight 0 after commit, got %d", inFlight)
	}

	// (2) After commitItems has processed that record, the next evictIdleStreams
	// removes the state.
	server.evictIdleStreams()

	server.mu.RLock()
	mapStateAfter := server.streams[streamID.String()]
	server.mu.RUnlock()

	if mapStateAfter != nil {
		t.Fatalf("expected streamState to be evicted after in-flight record processed, but still in map: %p", mapStateAfter)
	}
}
