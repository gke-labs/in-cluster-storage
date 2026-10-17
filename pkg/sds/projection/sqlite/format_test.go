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

package sqlite

import (
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/blob"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore/inmemorystorage"
)

func TestSQLiteSnapshot_Golden(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "golden.sqlite")

	db, err := Open(ctx, dbPath, WithStreamID("test-stream-golden"))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	// Verify _meta schema and row
	row := db.SQLDB().QueryRowContext(ctx, "SELECT format_version, created_by, stream_id, position FROM _meta LIMIT 1")
	var formatVer uint32
	var createdBy string
	var streamID string
	var position uint64
	if err := row.Scan(&formatVer, &createdBy, &streamID, &position); err != nil {
		t.Fatalf("failed to query _meta: %v", err)
	}

	if formatVer != SnapshotFormatVersion {
		t.Errorf("format_version = %d, want %d", formatVer, SnapshotFormatVersion)
	}
	if createdBy != DefaultCreatedBy {
		t.Errorf("created_by = %q, want %q", createdBy, DefaultCreatedBy)
	}
	if streamID != "test-stream-golden" {
		t.Errorf("stream_id = %q, want %q", streamID, "test-stream-golden")
	}
	if position != 0 {
		t.Errorf("position = %d, want 0", position)
	}
}

func TestSQLiteSnapshot_RejectsNewer(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	backend := inmemorystorage.New()

	dbPath := filepath.Join(dir, "newer.sqlite")
	rawDB, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open raw sqlite: %v", err)
	}

	// Create _meta table with format_version = 2 (newer than current 1)
	createMeta := `
CREATE TABLE _meta (
    format_version INTEGER NOT NULL,
    created_by TEXT NOT NULL,
    stream_id TEXT PRIMARY KEY,
    position INTEGER NOT NULL
);
INSERT INTO _meta (format_version, created_by, stream_id, position) VALUES (2, 'in-cluster-storage/2.0', 'stream-newer', 10);
`
	if _, err := rawDB.ExecContext(ctx, createMeta); err != nil {
		rawDB.Close()
		t.Fatalf("failed to create newer _meta: %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("failed to close raw sqlite: %v", err)
	}

	// Upload to backend
	snapKey := SnapshotKey("stream-newer", 10)
	snapBytes, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("failed to read db file: %v", err)
	}
	buf := blob.NewByteStreamFromBytes(snapBytes)
	if _, err := backend.PutObject(ctx, "", snapKey, buf); err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// RestoreSnapshotKey must reject newer version
	restorePath := filepath.Join(dir, "restored.sqlite")
	_, _, err = RestoreSnapshotKey(ctx, backend, "stream-newer", snapKey, restorePath)
	if err == nil {
		t.Fatal("expected error restoring snapshot with newer version, got nil")
	}
	if !errors.Is(err, ErrUnsupportedSnapshotVersion) {
		t.Fatalf("expected ErrUnsupportedSnapshotVersion, got: %v", err)
	}
}

func TestSQLiteSnapshot_UnversionedUpgrade(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	backend := inmemorystorage.New()

	dbPath := filepath.Join(dir, "legacy.sqlite")
	rawDB, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open raw sqlite: %v", err)
	}

	// Create legacy unversioned tables (_stream_position and _stream_types) without _meta
	createLegacy := `
CREATE TABLE _stream_position (
    stream_id TEXT PRIMARY KEY,
    position INTEGER NOT NULL,
    snapshot_time INTEGER NOT NULL
);
CREATE TABLE _stream_types (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    fingerprint BLOB NOT NULL,
    descriptors BLOB,
    key_fields TEXT NOT NULL
);
INSERT INTO _stream_position (stream_id, position, snapshot_time) VALUES ('stream-legacy', 42, 123456789);
`
	if _, err := rawDB.ExecContext(ctx, createLegacy); err != nil {
		rawDB.Close()
		t.Fatalf("failed to create legacy tables: %v", err)
	}
	if err := rawDB.Close(); err != nil {
		t.Fatalf("failed to close raw sqlite: %v", err)
	}

	snapKey := SnapshotKey("stream-legacy", 42)
	snapBytes, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("failed to read db file: %v", err)
	}
	if _, err := backend.PutObject(ctx, "", snapKey, blob.NewByteStreamFromBytes(snapBytes)); err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// 1. Without flag: Restore must reject with ErrUnversionedSnapshot
	restorePath1 := filepath.Join(dir, "restored-reject.sqlite")
	_, _, err = RestoreSnapshotKey(ctx, backend, "stream-legacy", snapKey, restorePath1)
	if err == nil {
		t.Fatal("expected error restoring unversioned snapshot without flag, got nil")
	}
	if !errors.Is(err, ErrUnversionedSnapshot) {
		t.Fatalf("expected ErrUnversionedSnapshot, got: %v", err)
	}

	// 2. With flag: Restore must succeed, log warning, and initialize _meta table
	restorePath2 := filepath.Join(dir, "restored-accept.sqlite")
	db, restoredPos, err := RestoreSnapshotKey(ctx, backend, "stream-legacy", snapKey, restorePath2, WithAcceptUnversioned(true))
	if err != nil {
		t.Fatalf("unexpected error with WithAcceptUnversioned: %v", err)
	}
	defer db.Close()

	if restoredPos != 42 {
		t.Errorf("restoredPos = %d, want 42", restoredPos)
	}
	if db.Position() != 42 {
		t.Errorf("db.Position = %d, want 42", db.Position())
	}
	if db.StreamID() != "stream-legacy" {
		t.Errorf("db.StreamID = %q, want %q", db.StreamID(), "stream-legacy")
	}
}
