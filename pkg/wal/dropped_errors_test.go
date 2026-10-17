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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestBackupCorruptedSegment_MissingFile(t *testing.T) {
	// Should log a warning and return cleanly without crashing.
	backupCorruptedSegment(filepath.Join(t.TempDir(), "nonexistent.wal"))
}

func TestNewClientSegmentStore_EmptySegmentRemovalFailure(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("skipping read-only directory test when running as root")
	}

	dir := t.TempDir()
	emptyFile := filepath.Join(dir, "stream-0000000000000000001.wal")
	if err := os.WriteFile(emptyFile, []byte{}, 0644); err != nil {
		t.Fatalf("failed to create empty segment file: %v", err)
	}

	// Make dir read-only to prevent removal
	if err := os.Chmod(dir, 0555); err != nil {
		t.Fatalf("failed to chmod dir: %v", err)
	}
	defer func() {
		_ = os.Chmod(dir, 0755)
	}()

	_, _, err := NewClientSegmentStore(dir, "stream", DefaultMaxSegmentSize)
	if err == nil {
		t.Fatalf("expected error removing empty segment file in read-only dir, got nil")
	}
	if !strings.Contains(err.Error(), "failed to remove empty segment file") {
		t.Errorf("expected failed to remove empty segment file in error, got: %v", err)
	}
}

func TestNewLogSegmentStore_StaleSegmentRemovalFailure(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("skipping read-only directory test when running as root")
	}

	dir := t.TempDir()
	file1 := filepath.Join(dir, "log-0000000000000000001.wal")
	file2 := filepath.Join(dir, "log-0000000000000000002.wal")
	if err := os.WriteFile(file1, []byte{}, 0644); err != nil {
		t.Fatalf("failed to create file1: %v", err)
	}
	if err := os.WriteFile(file2, []byte{}, 0644); err != nil {
		t.Fatalf("failed to create file2: %v", err)
	}

	if err := os.Chmod(dir, 0555); err != nil {
		t.Fatalf("failed to chmod dir: %v", err)
	}
	defer func() {
		_ = os.Chmod(dir, 0755)
	}()

	_, err := NewLogSegmentStore(dir, "log", DefaultMaxSegmentSize)
	if err == nil {
		t.Fatalf("expected error removing stale segment files in read-only dir, got nil")
	}
	// Verify it attempted both files (joined errors)
	if !strings.Contains(err.Error(), file1) || !strings.Contains(err.Error(), file2) {
		t.Errorf("expected both sibling files in error, got: %v", err)
	}
}

func TestClientSegmentStore_TruncateAfterRemovalFailure(t *testing.T) {
	dir := t.TempDir()
	store, _, err := NewClientSegmentStore(dir, "stream", 100) // Small max segment size to trigger rotation
	if err != nil {
		t.Fatalf("failed to create client segment store: %v", err)
	}

	streamID := uuid.New()
	for i := uint64(1); i <= 10; i++ {
		rec := &ClientRecord{
			StreamID:  streamID,
			StreamSeq: i,
			Payload:   []byte(fmt.Sprintf("payload-%d", i)),
		}
		if err := store.Append(rec); err != nil {
			t.Fatalf("failed to append record %d: %v", i, err)
		}
	}

	if err := store.Close(); err != nil {
		t.Fatalf("failed to close store: %v", err)
	}

	// Reopen store to reload segments
	store, _, err = NewClientSegmentStore(dir, "stream", 100)
	if err != nil {
		t.Fatalf("failed to reopen store: %v", err)
	}

	if len(store.segments) < 2 {
		t.Fatalf("expected at least 2 segments, got %d", len(store.segments))
	}

	// Replace segment file with a non-empty directory so os.Remove fails across both root and non-root
	targetSeg := store.segments[len(store.segments)-1]
	if err := os.Remove(targetSeg.Path); err != nil {
		t.Fatalf("failed to remove target seg file: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(targetSeg.Path, "child"), 0755); err != nil {
		t.Fatalf("failed to create child directory: %v", err)
	}

	// Truncate to seq targetSeg.FirstSeq - 1 (requiring targetSeg removal)
	err = store.TruncateAfter(targetSeg.FirstSeq - 1)
	if err == nil {
		t.Fatalf("expected error from TruncateAfter when segment removal fails, got nil")
	}
	if !strings.Contains(err.Error(), "failed to remove segment file") {
		t.Errorf("expected failed to remove segment file in error, got: %v", err)
	}
}

func TestClientSegmentStore_TruncateAfter_MissingPartialSegmentFile(t *testing.T) {
	dir := t.TempDir()
	store, _, err := NewClientSegmentStore(dir, "stream", 100)
	if err != nil {
		t.Fatalf("failed to create client segment store: %v", err)
	}

	streamID := uuid.New()
	for i := uint64(1); i <= 10; i++ {
		rec := &ClientRecord{
			StreamID:  streamID,
			StreamSeq: i,
			Payload:   []byte(fmt.Sprintf("rec-%d", i)),
		}
		if err := store.Append(rec); err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
	}

	// Remove the active segment file on disk before TruncateAfter
	targetSeg := store.segments[len(store.segments)-1]
	if err := os.Remove(targetSeg.Path); err != nil {
		t.Fatalf("failed to remove target seg file: %v", err)
	}

	// Truncate to seq targetSeg.FirstSeq (where targetSeg is considered a partial segment)
	err = store.TruncateAfter(targetSeg.FirstSeq)
	if err != nil {
		t.Fatalf("expected TruncateAfter to succeed when missing partial segment is already gone, got: %v", err)
	}

	// Verify targetSeg is no longer in remaining
	for _, s := range store.segments {
		if s.Path == targetSeg.Path {
			t.Errorf("missing segment %s should not remain in segments list", s.Path)
		}
	}
}

func TestScanSegmentFile_Errors(t *testing.T) {
	dir := t.TempDir()
	nonexistent := filepath.Join(dir, "nonexistent.wal")

	_, _, err := ScanClientSegmentFile(nonexistent)
	if err == nil {
		t.Errorf("expected error scanning nonexistent client segment, got nil")
	}

	_, _, err = ScanLogSegmentFile(nonexistent)
	if err == nil {
		t.Errorf("expected error scanning nonexistent log segment, got nil")
	}
}

func TestLogSegmentStore_ReadFromCallbackError(t *testing.T) {
	dir := t.TempDir()
	store, err := NewLogSegmentStore(dir, "log", DefaultMaxSegmentSize)
	if err != nil {
		t.Fatalf("failed to create log segment store: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	streamID := uuid.New()
	rec := &LogRecord{
		Position:  1,
		StreamID:  streamID,
		StreamSeq: 1,
		Payload:   []byte("test"),
	}
	if err := store.AppendBatch([]*LogRecord{rec}); err != nil {
		t.Fatalf("failed to append: %v", err)
	}

	expectedErr := errors.New("callback failed")
	err = store.ReadFrom(1, func(r *LogRecord) error {
		return expectedErr
	})
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected callback error, got: %v", err)
	}
}
