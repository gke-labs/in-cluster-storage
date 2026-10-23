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

package wal

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestClientRecordEncodeDecode(t *testing.T) {
	streamID := uuid.New()
	rec := &ClientRecord{
		StreamID:  streamID,
		StreamSeq: 42,
		Payload:   []byte("test-client-payload-1234"),
	}

	encoded := rec.Encode()
	if len(encoded) != ClientHeaderSize+len(rec.Payload) {
		t.Fatalf("expected encoded length %d, got %d", ClientHeaderSize+len(rec.Payload), len(encoded))
	}

	decoded, err := DecodeClientRecord(bytes.NewReader(encoded))
	if err != nil {
		t.Fatalf("failed to decode client record: %v", err)
	}

	if decoded.StreamID != rec.StreamID {
		t.Errorf("expected StreamID %s, got %s", rec.StreamID, decoded.StreamID)
	}
	if decoded.StreamSeq != rec.StreamSeq {
		t.Errorf("expected StreamSeq %d, got %d", rec.StreamSeq, decoded.StreamSeq)
	}
	if !bytes.Equal(decoded.Payload, rec.Payload) {
		t.Errorf("expected payload %q, got %q", string(rec.Payload), string(decoded.Payload))
	}
	if decoded.CRC32C != rec.CRC32C {
		t.Errorf("expected CRC32C %d, got %d", rec.CRC32C, decoded.CRC32C)
	}
}

func TestLogRecordEncodeDecode(t *testing.T) {
	streamID := uuid.New()
	rec := &LogRecord{
		Position:  100,
		StreamID:  streamID,
		StreamSeq: 42,
		Payload:   []byte("test-log-payload-1234"),
	}

	encoded := rec.Encode()
	if len(encoded) != LogHeaderSize+len(rec.Payload) {
		t.Fatalf("expected encoded length %d, got %d", LogHeaderSize+len(rec.Payload), len(encoded))
	}

	decoded, err := DecodeLogRecord(bytes.NewReader(encoded))
	if err != nil {
		t.Fatalf("failed to decode log record: %v", err)
	}

	if decoded.Position != rec.Position {
		t.Errorf("expected Position %d, got %d", rec.Position, decoded.Position)
	}
	if decoded.StreamID != rec.StreamID {
		t.Errorf("expected StreamID %s, got %s", rec.StreamID, decoded.StreamID)
	}
	if decoded.StreamSeq != rec.StreamSeq {
		t.Errorf("expected StreamSeq %d, got %d", rec.StreamSeq, decoded.StreamSeq)
	}
	if !bytes.Equal(decoded.Payload, rec.Payload) {
		t.Errorf("expected payload %q, got %q", string(rec.Payload), string(decoded.Payload))
	}
}

func TestRecordChecksumMismatch(t *testing.T) {
	streamID := uuid.New()
	rec := &ClientRecord{
		StreamID:  streamID,
		StreamSeq: 1,
		Payload:   []byte("hello world"),
	}

	encoded := rec.Encode()
	encoded[len(encoded)-1] ^= 0xFF

	_, err := DecodeClientRecord(bytes.NewReader(encoded))
	if err != ErrChecksumMismatch {
		t.Fatalf("expected ErrChecksumMismatch, got %v", err)
	}
}

func TestTornTrailingRecordDropAndRecoverFile(t *testing.T) {
	tmpDir := t.TempDir()
	store, _, err := NewClientSegmentStore(tmpDir, "client", 1024*1024)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	streamID := uuid.New()
	for i := uint64(1); i <= 5; i++ {
		rec := &ClientRecord{
			StreamID:  streamID,
			StreamSeq: i,
			Payload:   []byte("record payload"),
		}
		if err := store.Append(rec); err != nil {
			t.Fatalf("failed to append record %d: %v", i, err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatalf("failed to close store: %v", err)
	}

	// Corrupt file by appending partial header
	files, err := os.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("failed to read dir: %v", err)
	}
	if len(files) == 0 {
		t.Fatalf("expected at least one segment file")
	}

	targetFile := filepath.Join(tmpDir, files[0].Name())
	f, err := os.OpenFile(targetFile, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	_, _ = f.Write([]byte("WALC\x00\x01\x02\x03\x04\x05"))
	_ = f.Close()

	// Reopen store: torn trailing record dropped and .recover backup created
	reopenedStore, records, err := NewClientSegmentStore(tmpDir, "client", 1024*1024)
	if err != nil {
		t.Fatalf("failed to reopen store: %v", err)
	}
	defer reopenedStore.Close()

	if len(records) != 5 {
		t.Fatalf("expected 5 recovered records, got %d", len(records))
	}

	// Verify .recover file was created
	entries, _ := os.ReadDir(tmpDir)
	var foundRecover bool
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".recover") {
			foundRecover = true
			break
		}
	}
	if !foundRecover {
		t.Errorf("expected .recover backup file to be created upon truncation")
	}
}

func TestLogSegmentStoreReadFrom(t *testing.T) {
	tmpDir := t.TempDir()
	// Small maxSegmentSize so files rotate frequently (~100 bytes each)
	store, _, err := NewLogSegmentStore(tmpDir, "log", 120)
	if err != nil {
		t.Fatalf("failed to create log segment store: %v", err)
	}
	defer store.Close()

	streamID := uuid.New()
	var records []*LogRecord
	for i := uint64(1); i <= 10; i++ {
		rec := &LogRecord{
			Position:  i,
			StreamID:  streamID,
			StreamSeq: i,
			Payload:   []byte("payload-data"),
		}
		records = append(records, rec)
	}

	if err := store.AppendBatch(records); err != nil {
		t.Fatalf("failed to append batch: %v", err)
	}

	// 1. Read from position 1 (all records)
	var readFrom1 []uint64
	err = store.ReadFrom(1, func(r *LogRecord) error {
		readFrom1 = append(readFrom1, r.Position)
		return nil
	})
	if err != nil {
		t.Fatalf("ReadFrom(1) failed: %v", err)
	}
	if len(readFrom1) != 10 {
		t.Fatalf("expected 10 records, got %d: %v", len(readFrom1), readFrom1)
	}
	for i, pos := range readFrom1 {
		if pos != uint64(i+1) {
			t.Errorf("expected position %d at index %d, got %d", i+1, i, pos)
		}
	}

	// 2. Read from position 5 (records 5..10)
	var readFrom5 []uint64
	err = store.ReadFrom(5, func(r *LogRecord) error {
		readFrom5 = append(readFrom5, r.Position)
		return nil
	})
	if err != nil {
		t.Fatalf("ReadFrom(5) failed: %v", err)
	}
	if len(readFrom5) != 6 {
		t.Fatalf("expected 6 records, got %d: %v", len(readFrom5), readFrom5)
	}
	for i, pos := range readFrom5 {
		if pos != uint64(i+5) {
			t.Errorf("expected position %d at index %d, got %d", i+5, i, pos)
		}
	}

	// 3. Read from position 10
	var readFrom10 []uint64
	err = store.ReadFrom(10, func(r *LogRecord) error {
		readFrom10 = append(readFrom10, r.Position)
		return nil
	})
	if err != nil {
		t.Fatalf("ReadFrom(10) failed: %v", err)
	}
	if len(readFrom10) != 1 || readFrom10[0] != 10 {
		t.Fatalf("expected [10], got %v", readFrom10)
	}

	// 4. Read from beyond last position
	var readFrom11 []uint64
	err = store.ReadFrom(11, func(r *LogRecord) error {
		readFrom11 = append(readFrom11, r.Position)
		return nil
	})
	if err != nil {
		t.Fatalf("ReadFrom(11) failed: %v", err)
	}
	if len(readFrom11) != 0 {
		t.Fatalf("expected 0 records, got %v", readFrom11)
	}

	// 5. Test tolerant trailing partial record
	store.mu.RLock()
	activePath := store.activeMeta.Path
	store.mu.RUnlock()

	f, err := os.OpenFile(activePath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("failed to open active file: %v", err)
	}
	// Append incomplete record bytes
	_, _ = f.Write([]byte("WALL\x00\x00\x00\x00\x00\x00\x00\x0b"))
	_ = f.Close()

	var readAfterPartial []uint64
	err = store.ReadFrom(1, func(r *LogRecord) error {
		readAfterPartial = append(readAfterPartial, r.Position)
		return nil
	})
	if err != nil {
		t.Fatalf("expected ReadFrom to tolerate trailing partial record, got error: %v", err)
	}
	if len(readAfterPartial) != 10 {
		t.Fatalf("expected 10 records after partial trailing write, got %d", len(readAfterPartial))
	}
}

func TestLogSegmentStoreDeleteSegmentsThrough(t *testing.T) {
	tmpDir := t.TempDir()
	store, _, err := NewLogSegmentStore(tmpDir, "log", 100)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	streamID := uuid.New()
	for i := uint64(1); i <= 8; i++ {
		rec := &LogRecord{
			Position:  i,
			StreamID:  streamID,
			StreamSeq: i,
			Payload:   []byte("test-payload-123"),
		}
		if err := store.AppendBatch([]*LogRecord{rec}); err != nil {
			t.Fatalf("append batch %d failed: %v", i, err)
		}
	}

	store.mu.RLock()
	initialSegCount := len(store.segments)
	store.mu.RUnlock()

	if initialSegCount < 3 {
		t.Fatalf("expected multiple segments rotated, got %d", initialSegCount)
	}

	// 1. Delete flushed through position 2 with maxRetainedBytes = 0
	if err := store.DeleteSegmentsThrough(2, 0); err != nil {
		t.Fatalf("DeleteSegmentsThrough failed: %v", err)
	}

	store.mu.RLock()
	for _, seg := range store.segments {
		if seg.LastSeq <= 2 && seg != store.activeMeta {
			t.Errorf("expected segment with LastSeq <= 2 to be deleted, but still present: %+v", seg)
		}
	}
	store.mu.RUnlock()

	// 2. Try to delete through position 100 with maxRetainedBytes = 0.
	// Active segment must NEVER be deleted even though position 100 > all records.
	if err := store.DeleteSegmentsThrough(100, 0); err != nil {
		t.Fatalf("DeleteSegmentsThrough failed: %v", err)
	}

	store.mu.RLock()
	if len(store.segments) == 0 {
		t.Fatalf("expected active segment to be retained, got 0 segments")
	}
	if store.activeMeta == nil {
		t.Fatalf("expected activeMeta not nil")
	}
	store.mu.RUnlock()
}

func TestParseAndFormatStreamPath(t *testing.T) {
	streamID := uuid.MustParse("12345678-1234-1234-1234-123456789abc")

	// Open file format
	openName := FormatStreamFileName(1, 0, false)
	if openName != "from00000000000000000001.wal" {
		t.Fatalf("unexpected openName: %s", openName)
	}
	from, to, sealed, err := ParseStreamPath(openName)
	if err != nil || from != 1 || to != 0 || sealed {
		t.Fatalf("unexpected ParseStreamPath result: from=%d, to=%d, sealed=%v, err=%v", from, to, sealed, err)
	}

	// Sealed file format
	sealedName := FormatStreamFileName(1, 100, true)
	if sealedName != "from00000000000000000001-to00000000000000000100.wal" {
		t.Fatalf("unexpected sealedName: %s", sealedName)
	}
	from, to, sealed, err = ParseStreamPath(sealedName)
	if err != nil || from != 1 || to != 100 || !sealed {
		t.Fatalf("unexpected ParseStreamPath result: from=%d, to=%d, sealed=%v, err=%v", from, to, sealed, err)
	}

	// Object storage key format
	key := FormatStreamObjectKey(streamID, 1, 100)
	expectedKey := "streams/12345678-1234-1234-1234-123456789abc/from00000000000000000001-to00000000000000000100.wal"
	if key != expectedKey {
		t.Fatalf("unexpected key: got %s, want %s", key, expectedKey)
	}
	from, to, sealed, err = ParseStreamPath(key)
	if err != nil || from != 1 || to != 100 || !sealed {
		t.Fatalf("unexpected ParseStreamPath result on key: from=%d, to=%d, sealed=%v, err=%v", from, to, sealed, err)
	}

	// Prefixed format
	from, to, sealed, err = ParseStreamPath("stream1234-from00000000000000000005-to00000000000000000020.wal")
	if err != nil || from != 5 || to != 20 || !sealed {
		t.Fatalf("unexpected ParseStreamPath result on prefixed: from=%d, to=%d, sealed=%v, err=%v", from, to, sealed, err)
	}

	// Invalid format
	if _, _, _, err := ParseStreamPath("invalid.wal"); err == nil {
		t.Fatalf("expected error on invalid.wal")
	}

	// Reserved streams/wal prefix
	if _, _, _, err := ParseStreamPath("streams/wal/segments/000000000001-000000000010.wal"); err == nil {
		t.Fatalf("expected error on streams/wal segment path")
	}
	if _, _, _, err := ParseStreamPath("streams/wal/from00000000000000000001.wal"); err == nil {
		t.Fatalf("expected error on streams/wal open stream path")
	}
	if _, _, _, err := ParseStreamPath("streams/wal/from00000000000000000001-to00000000000000000100.wal"); err == nil {
		t.Fatalf("expected error on streams/wal sealed stream path")
	}
}
