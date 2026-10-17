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

package table

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble/v2/bloom"
	"github.com/cockroachdb/pebble/v2/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/v2/sstable"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/blob"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore/inmemorystorage"
)

func TestTableSnapshot_Golden(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	backend := inmemorystorage.New()

	db, err := Open(ctx, filepath.Join(dir, "source.db"), WithStreamID("test-stream-golden"))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	key, _, err := PublishSnapshot(ctx, db, backend, dir)
	if err != nil {
		t.Fatalf("PublishSnapshot failed: %v", err)
	}

	var snapBuf bytes.Buffer
	if err := backend.GetObject(ctx, "", key, 0, 0, &snapBuf); err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}

	sr, err := OpenSnapshotBytes(snapBuf.Bytes())
	if err != nil {
		t.Fatalf("OpenSnapshotBytes failed: %v", err)
	}
	defer sr.Close()

	if sr.StreamID() != "test-stream-golden" {
		t.Errorf("StreamID = %q, want %q", sr.StreamID(), "test-stream-golden")
	}
	if sr.Position() != 0 {
		t.Errorf("Position = %d, want 0", sr.Position())
	}
}

func buildTestSSTable(t *testing.T, filePath string, entries map[string][]byte) {
	f, err := vfs.Default.Create(filePath, vfs.WriteCategoryUnspecified)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{
		TableFormat:  SnapshotTableFormat,
		FilterPolicy: bloom.FilterPolicy(10),
		Compression:  sstable.NoCompression,
	})

	// Pebble requires keys to be inserted in strictly ascending order
	// In our test, keys starting with 0x00 sort alphabetically by byte value
	type kv struct {
		k []byte
		v []byte
	}
	var kvs []kv
	for k, v := range entries {
		kvs = append(kvs, kv{k: []byte(k), v: v})
	}
	for i := 0; i < len(kvs); i++ {
		for j := i + 1; j < len(kvs); j++ {
			if bytes.Compare(kvs[i].k, kvs[j].k) > 0 {
				kvs[i], kvs[j] = kvs[j], kvs[i]
			}
		}
	}

	for _, item := range kvs {
		if err := w.Set(item.k, item.v); err != nil {
			w.Close()
			t.Fatalf("Set failed: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestTableSnapshot_RejectsNewer(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	backend := inmemorystorage.New()

	sstPath := filepath.Join(dir, "newer.sst")
	entries := map[string][]byte{
		string(KeyCreatedBy):     []byte("in-cluster-storage/2.0"),
		string(KeyFormatVersion): EncodeFormatVersion(2), // version 2 > current 1
		string(KeyPosition):      EncodePosition(50),
		string(KeyRegistry):      []byte{},
		string(KeyStreamID):      []byte("stream-newer"),
	}
	buildTestSSTable(t, sstPath, entries)

	// 1. OpenSnapshotFile directly must reject newer version
	_, err := OpenSnapshotFile(sstPath)
	if err == nil {
		t.Fatal("expected error opening newer snapshot, got nil")
	}
	if !errors.Is(err, ErrUnsupportedSnapshotVersion) {
		t.Fatalf("expected ErrUnsupportedSnapshotVersion, got: %v", err)
	}

	// 2. Upload and test RestoreSnapshotKey
	snapKey := SnapshotKey("stream-newer", 50)
	snapBytes, err := os.ReadFile(sstPath)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if _, err := backend.PutObject(ctx, "", snapKey, blob.NewByteStreamFromBytes(snapBytes)); err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	restoreDir := filepath.Join(dir, "restore-newer")
	_, _, err = RestoreSnapshotKey(ctx, backend, "stream-newer", snapKey, restoreDir)
	if err == nil {
		t.Fatal("expected error restoring newer snapshot, got nil")
	}
	if !errors.Is(err, ErrUnsupportedSnapshotVersion) {
		t.Fatalf("expected ErrUnsupportedSnapshotVersion, got: %v", err)
	}
}

func TestTableSnapshot_UnversionedUpgrade(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	backend := inmemorystorage.New()

	sstPath := filepath.Join(dir, "legacy.sst")
	// Legacy unversioned snapshot: contains KeyPosition, KeyStreamID, KeyRegistry, but NO KeyFormatVersion
	entries := map[string][]byte{
		string(KeyPosition): EncodePosition(42),
		string(KeyRegistry): []byte{},
		string(KeyStreamID): []byte("stream-legacy"),
	}
	buildTestSSTable(t, sstPath, entries)

	// 1. Standalone reader without flag: reject
	_, err := OpenSnapshotFile(sstPath)
	if err == nil {
		t.Fatal("expected error opening unversioned snapshot without flag, got nil")
	}
	if !errors.Is(err, ErrUnversionedSnapshot) {
		t.Fatalf("expected ErrUnversionedSnapshot, got: %v", err)
	}

	// 2. Standalone reader with flag: accept
	sr, err := OpenSnapshotFile(sstPath, WithReaderAcceptUnversioned(true))
	if err != nil {
		t.Fatalf("unexpected error with WithReaderAcceptUnversioned: %v", err)
	}
	defer sr.Close()
	if sr.Position() != 42 {
		t.Errorf("Position = %d, want 42", sr.Position())
	}

	// 3. Ingest / restore without flag: reject
	snapKey := SnapshotKey("stream-legacy", 42)
	snapBytes, err := os.ReadFile(sstPath)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if _, err := backend.PutObject(ctx, "", snapKey, blob.NewByteStreamFromBytes(snapBytes)); err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	restoreDir1 := filepath.Join(dir, "restore-reject")
	_, _, err = RestoreSnapshotKey(ctx, backend, "stream-legacy", snapKey, restoreDir1)
	if err == nil {
		t.Fatal("expected error restoring unversioned snapshot without flag, got nil")
	}
	if !errors.Is(err, ErrUnversionedSnapshot) {
		t.Fatalf("expected ErrUnversionedSnapshot, got: %v", err)
	}

	// 4. Ingest / restore with flag: accept
	restoreDir2 := filepath.Join(dir, "restore-accept")
	db, restoredPos, err := RestoreSnapshotKey(ctx, backend, "stream-legacy", snapKey, restoreDir2, WithAcceptUnversioned(true))
	if err != nil {
		t.Fatalf("unexpected error restoring with WithAcceptUnversioned: %v", err)
	}
	defer db.Close()

	if restoredPos != 42 {
		t.Errorf("restoredPos = %d, want 42", restoredPos)
	}
	if db.Position() != 42 {
		t.Errorf("db.Position = %d, want 42", db.Position())
	}
}
