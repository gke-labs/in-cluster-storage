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
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
)

func TestSegmentHeader_Golden(t *testing.T) {
	goldenPath := filepath.Join("testdata", "segment_header.golden")
	actual := DefaultSegmentHeader().Encode()

	// If golden file does not exist, create it
	if _, err := os.Stat(goldenPath); os.IsNotExist(err) {
		if err := os.WriteFile(goldenPath, actual, 0644); err != nil {
			t.Fatalf("failed to write golden file: %v", err)
		}
	}

	expected, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("failed to read golden file %s: %v", goldenPath, err)
	}

	if !bytes.Equal(actual, expected) {
		t.Fatalf("segment header mismatch with golden file:\ngot:  %x\nwant: %x", actual, expected)
	}

	// Verify exact fields in golden file
	if len(expected) != SegmentHeaderSize {
		t.Fatalf("expected golden size %d, got %d", SegmentHeaderSize, len(expected))
	}
	if string(expected[0:4]) != SegmentMagic {
		t.Errorf("magic = %q, want %q", string(expected[0:4]), SegmentMagic)
	}
	ver := binary.BigEndian.Uint16(expected[4:6])
	if ver != 1 {
		t.Errorf("version = %d, want 1", ver)
	}
	hdrLen := binary.BigEndian.Uint16(expected[6:8])
	if hdrLen != SegmentHeaderSize {
		t.Errorf("header length = %d, want %d", hdrLen, SegmentHeaderSize)
	}
}

func TestSegmentHeader_RejectsNewer(t *testing.T) {
	dir := t.TempDir()

	// Create header with version = 2 (newer than current version 1)
	hdr := DefaultSegmentHeader()
	hdr.Version = 2
	hdrBytes := hdr.Encode()

	// 1. Test ReadSegmentHeader directly
	r := bytes.NewReader(hdrBytes)
	_, _, err := ReadSegmentHeader(r, false)
	if err == nil {
		t.Fatal("expected error for newer segment version, got nil")
	}
	if !errors.Is(err, ErrUnsupportedSegmentVersion) {
		t.Fatalf("expected ErrUnsupportedSegmentVersion, got: %v", err)
	}

	// 2. Test ScanClientSegmentFile rejects newer version
	clientSegPath := filepath.Join(dir, "client-0000000000000000001.wal")
	if err := os.WriteFile(clientSegPath, hdrBytes, 0644); err != nil {
		t.Fatalf("failed to write client segment: %v", err)
	}
	_, _, err = ScanClientSegmentFile(clientSegPath)
	if err == nil {
		t.Fatal("expected error from ScanClientSegmentFile for newer version, got nil")
	}
	if !errors.Is(err, ErrUnsupportedSegmentVersion) {
		t.Fatalf("expected ErrUnsupportedSegmentVersion from ScanClientSegmentFile, got: %v", err)
	}

	// 3. Test ScanLogSegmentFile rejects newer version
	logSegPath := filepath.Join(dir, "log-0000000000000000001.wal")
	if err := os.WriteFile(logSegPath, hdrBytes, 0644); err != nil {
		t.Fatalf("failed to write log segment: %v", err)
	}
	_, _, err = ScanLogSegmentFile(logSegPath)
	if err == nil {
		t.Fatal("expected error from ScanLogSegmentFile for newer version, got nil")
	}
	if !errors.Is(err, ErrUnsupportedSegmentVersion) {
		t.Fatalf("expected ErrUnsupportedSegmentVersion from ScanLogSegmentFile, got: %v", err)
	}
}

func TestSegmentHeader_UnversionedUpgrade(t *testing.T) {
	dir := t.TempDir()
	streamID := uuid.New()

	// 1. Test Client Segment unversioned upgrade
	clientRec := &ClientRecord{
		StreamID:  streamID,
		StreamSeq: 1,
		Payload:   []byte("payload-client-1"),
	}
	clientRec.CRC32C = clientRec.ComputeCRC32C()
	unversionedClientFile := filepath.Join(dir, "unversioned-client.wal")
	if err := os.WriteFile(unversionedClientFile, clientRec.Encode(), 0644); err != nil {
		t.Fatalf("failed to write unversioned client file: %v", err)
	}

	// Without flag: must reject with ErrUnversionedSegment
	_, _, err := ScanClientSegmentFile(unversionedClientFile)
	if err == nil {
		t.Fatal("expected error without --accept-unversioned-segments, got nil")
	}
	if !errors.Is(err, ErrUnversionedSegment) {
		t.Fatalf("expected ErrUnversionedSegment, got: %v", err)
	}

	// With flag: must accept and recover records
	recs, meta, err := ScanClientSegmentFile(unversionedClientFile, WithAcceptUnversioned(true))
	if err != nil {
		t.Fatalf("unexpected error with WithAcceptUnversioned: %v", err)
	}
	if len(recs) != 1 || recs[0].StreamSeq != 1 {
		t.Fatalf("expected 1 record at seq 1, got %d (%+v)", len(recs), recs)
	}
	if meta.RecordCount != 1 {
		t.Errorf("expected meta.RecordCount = 1, got %d", meta.RecordCount)
	}

	// 2. Test Log Segment unversioned upgrade
	logRec := &LogRecord{
		Position:  1,
		StreamID:  streamID,
		StreamSeq: 1,
		Payload:   []byte("payload-log-1"),
	}
	logRec.CRC32C = logRec.ComputeCRC32C()
	unversionedLogFile := filepath.Join(dir, "unversioned-log.wal")
	if err := os.WriteFile(unversionedLogFile, logRec.Encode(), 0644); err != nil {
		t.Fatalf("failed to write unversioned log file: %v", err)
	}

	// Without flag: must reject with ErrUnversionedSegment
	_, _, err = ScanLogSegmentFile(unversionedLogFile)
	if err == nil {
		t.Fatal("expected error without --accept-unversioned-segments, got nil")
	}
	if !errors.Is(err, ErrUnversionedSegment) {
		t.Fatalf("expected ErrUnversionedSegment, got: %v", err)
	}

	// With flag: must accept and recover records
	logRecs, logMeta, err := ScanLogSegmentFile(unversionedLogFile, WithAcceptUnversioned(true))
	if err != nil {
		t.Fatalf("unexpected error with WithAcceptUnversioned: %v", err)
	}
	if len(logRecs) != 1 || logRecs[0].Position != 1 {
		t.Fatalf("expected 1 record at pos 1, got %d (%+v)", len(logRecs), logRecs)
	}
	if logMeta.RecordCount != 1 {
		t.Errorf("expected logMeta.RecordCount = 1, got %d", logMeta.RecordCount)
	}
}
