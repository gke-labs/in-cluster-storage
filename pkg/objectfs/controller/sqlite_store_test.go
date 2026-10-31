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

package controller

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore/inmemorystorage"
	"github.com/gke-labs/in-cluster-storage/pkg/wal/buffer/buffertest"
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
	"github.com/google/uuid"
)

var (
	testStoreStreamsMu sync.Mutex
	testStoreBuffers   = make(map[string]*buffertest.BufferTest)
)

func testOpenStream(t testing.TB, walDir string, streamID uuid.UUID, backend ObjectStorageBackend) walclient.Stream {
	t.Helper()
	sid := streamID.String()
	testStoreStreamsMu.Lock()
	bt, ok := testStoreBuffers[sid]
	if !ok {
		bt, _ = buffertest.Start(t.Context(), t, buffertest.WithBackend(backend))
		testStoreBuffers[sid] = bt
		t.Cleanup(func() {
			testStoreStreamsMu.Lock()
			delete(testStoreBuffers, sid)
			testStoreStreamsMu.Unlock()
		})
	}
	testStoreStreamsMu.Unlock()

	s, err := walclient.Open(t.Context(), walDir, streamID, bt.Target,
		walclient.WithGRPCClient(bt.Client),
		walclient.WithDialOptions(bt.DialOpts...),
		walclient.WithoutFsync(),
	)
	if err != nil {
		t.Fatalf("testOpenStream failed: %v", err)
	}
	return s
}

func TestSQLiteMetadataStoreOperations(t *testing.T) {
	ctx := t.Context()
	localDir := t.TempDir()
	backend := inmemorystorage.New()
	broadcaster := NewEventBroadcaster()
	volID := "vol-sqlite-ops"

	vol := MustNewVolume(t, volID, backend, broadcaster,
		WithMetadataIndex("sqlite"),
		WithLocalStorageDir(localDir),
	)
	defer vol.Close()

	if err := vol.LoadFromBackend(ctx); err != nil {
		t.Fatalf("LoadFromBackend failed: %v", err)
	}

	// 1. Root attr
	rootAttr, err := vol.GetAttr(ctx, 1)
	if err != nil {
		t.Fatalf("GetAttr(1) failed: %v", err)
	}
	if !rootAttr.GetInode().GetIsDir() {
		t.Errorf("expected root to be dir")
	}

	// 2. Mkdir
	dirAttr, err := vol.Mkdir(ctx, 1, "mydir", 0755, 1000, 1000)
	if err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}
	if dirAttr.GetInode().GetIno() == 0 || dirAttr.GetName() != "mydir" || !dirAttr.GetInode().GetIsDir() {
		t.Fatalf("unexpected dirAttr: %+v", dirAttr)
	}

	// 3. CreateFile (inline)
	data1 := []byte("hello sqlite world")
	fileAttr, err := vol.CreateFile(ctx, dirAttr.GetInode().GetIno(), "hello.txt", 0644, data1, 1000, 1000, 0)
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}
	if fileAttr.GetInode().GetSize() != int64(len(data1)) {
		t.Errorf("file size mismatch: got %d, want %d", fileAttr.GetInode().GetSize(), len(data1))
	}

	// 4. Lookup
	lookupAttr, err := vol.Lookup(ctx, dirAttr.GetInode().GetIno(), "hello.txt")
	if err != nil {
		t.Fatalf("Lookup failed: %v", err)
	}
	if lookupAttr.GetInode().GetIno() != fileAttr.GetInode().GetIno() {
		t.Errorf("lookup inode mismatch: %d vs %d", lookupAttr.GetInode().GetIno(), fileAttr.GetInode().GetIno())
	}

	// 5. ReadFile
	readData, totalSize, _, err := vol.ReadFile(ctx, fileAttr.GetInode().GetIno(), 0, 100)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if totalSize != int64(len(data1)) || !bytes.Equal(readData, data1) {
		t.Errorf("ReadFile mismatch: got %q (total=%d)", string(readData), totalSize)
	}

	// 6. Large chunked write
	largeData := make([]byte, 128*1024)
	for i := range largeData {
		largeData[i] = byte(i % 251)
	}
	chunkAttr, err := vol.CreateFile(ctx, dirAttr.GetInode().GetIno(), "large.bin", 0644, nil, 1000, 1000, 0)
	if err != nil {
		t.Fatalf("CreateFile large.bin failed: %v", err)
	}
	nWritten, newSize, _, err := vol.WriteFile(ctx, chunkAttr.GetInode().GetIno(), 0, largeData, pb.WriteMode_WRITE_MODE_UNSPECIFIED)
	if err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}
	if nWritten != int64(len(largeData)) || newSize != int64(len(largeData)) {
		t.Errorf("WriteFile result mismatch: nWritten=%d, newSize=%d", nWritten, newSize)
	}

	if err := vol.Fsync(ctx, chunkAttr.GetInode().GetIno()); err != nil {
		t.Fatalf("Fsync large.bin failed: %v", err)
	}

	readChunk, _, _, err := vol.ReadFile(ctx, chunkAttr.GetInode().GetIno(), 0, int64(len(largeData)))
	if err != nil {
		t.Fatalf("ReadFile large.bin failed: %v", err)
	}
	if !bytes.Equal(readChunk, largeData) {
		t.Fatalf("readChunk content mismatch")
	}

	// 7. ReadDir
	entries, err := vol.ReadDir(ctx, dirAttr.GetInode().GetIno())
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries in mydir, got %d", len(entries))
	}

	// 8. Rename
	renamedAttr, err := vol.Rename(ctx, dirAttr.GetInode().GetIno(), "hello.txt", dirAttr.GetInode().GetIno(), "greeting.txt")
	if err != nil {
		t.Fatalf("Rename failed: %v", err)
	}
	if renamedAttr.GetName() != "greeting.txt" {
		t.Errorf("renamed name mismatch: %s", renamedAttr.GetName())
	}

	// 9. Unlink
	if _, err := vol.Unlink(ctx, dirAttr.GetInode().GetIno(), "greeting.txt"); err != nil {
		t.Fatalf("Unlink failed: %v", err)
	}
	if _, err := vol.Lookup(ctx, dirAttr.GetInode().GetIno(), "greeting.txt"); err == nil {
		t.Errorf("expected ENOENT after unlink")
	}

	// 10. Truncate
	if _, err := vol.TruncateFile(ctx, chunkAttr.GetInode().GetIno(), 10); err != nil {
		t.Fatalf("TruncateFile failed: %v", err)
	}
	tAttr, _ := vol.GetAttr(ctx, chunkAttr.GetInode().GetIno())
	if tAttr.GetInode().GetSize() != 10 {
		t.Errorf("expected truncated size 10, got %d", tAttr.GetInode().GetSize())
	}

	// 11. Clean up large.bin and Rmdir
	if _, err := vol.Unlink(ctx, dirAttr.GetInode().GetIno(), "large.bin"); err != nil {
		t.Fatalf("Unlink large.bin failed: %v", err)
	}
	if _, err := vol.Rmdir(ctx, 1, "mydir"); err != nil {
		t.Fatalf("Rmdir failed: %v", err)
	}
}

func TestSQLiteMetadataStoreRecoveryFromLocalDB(t *testing.T) {
	ctx := t.Context()
	localDir := t.TempDir()
	backend := inmemorystorage.New()
	broadcaster := NewEventBroadcaster()
	volID := "vol-local-rec"

	// 1. First run: create files
	vol1 := MustNewVolume(t, volID, backend, broadcaster,
		WithMetadataIndex("sqlite"),
		WithLocalStorageDir(localDir),
	)
	if err := vol1.LoadFromBackend(ctx); err != nil {
		t.Fatalf("LoadFromBackend failed: %v", err)
	}

	dirAttr, err := vol1.Mkdir(ctx, 1, "data", 0755, 0, 0)
	if err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}

	content := []byte("persisted in sqlite")
	fileAttr, err := vol1.CreateFile(ctx, dirAttr.GetInode().GetIno(), "test.txt", 0644, content, 0, 0, 0)
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}

	// Close volume
	if err := vol1.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// 2. Second run: reopen with same localDir
	vol2 := MustNewVolume(t, volID, backend, broadcaster,
		WithMetadataIndex("sqlite"),
		WithLocalStorageDir(localDir),
	)
	defer vol2.Close()

	if err := vol2.LoadFromBackend(ctx); err != nil {
		t.Fatalf("LoadFromBackend run 2 failed: %v", err)
	}

	lookupAttr, err := vol2.Lookup(ctx, dirAttr.GetInode().GetIno(), "test.txt")
	if err != nil {
		t.Fatalf("Lookup after local restart failed: %v", err)
	}
	if lookupAttr.GetInode().GetIno() != fileAttr.GetInode().GetIno() {
		t.Errorf("inode mismatch: %d vs %d", lookupAttr.GetInode().GetIno(), fileAttr.GetInode().GetIno())
	}

	data, _, _, err := vol2.ReadFile(ctx, lookupAttr.GetInode().GetIno(), 0, 100)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if !bytes.Equal(data, content) {
		t.Errorf("content mismatch: got %q, want %q", string(data), string(content))
	}
}

func TestSQLiteMetadataStoreRecoveryFromPublishedSnapshot(t *testing.T) {
	ctx := t.Context()
	localDir1 := t.TempDir()
	localDir2 := t.TempDir()
	backend := inmemorystorage.New()
	broadcaster := NewEventBroadcaster()
	volID := "vol-snap-rec"
	streamID := uuid.New()

	// 1. First run: create files and publish snapshot
	vol1 := MustNewVolume(t, volID, backend, broadcaster,
		WithMetadataIndex("sqlite"),
		WithLocalStorageDir(localDir1),
		WithStreamID(streamID),
	)
	if err := vol1.LoadFromBackend(ctx); err != nil {
		t.Fatalf("LoadFromBackend failed: %v", err)
	}

	dirAttr, err := vol1.Mkdir(ctx, 1, "snapdir", 0755, 0, 0)
	if err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}
	content := []byte("snapshot restored content")
	fileAttr, err := vol1.CreateFile(ctx, dirAttr.GetInode().GetIno(), "file.txt", 0644, content, 0, 0, 0)
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}

	// Flush to backend publishes SQLite snapshot and EROFS snapshot
	if err := vol1.FlushToBackend(ctx); err != nil {
		t.Fatalf("FlushToBackend failed: %v", err)
	}
	_ = vol1.Close()

	// 2. Second run: restore into a clean new local directory from published snapshot
	vol2 := MustNewVolume(t, volID, backend, broadcaster,
		WithMetadataIndex("sqlite"),
		WithLocalStorageDir(localDir2),
		WithStreamID(streamID),
	)
	defer vol2.Close()

	if err := vol2.LoadFromBackend(ctx); err != nil {
		t.Fatalf("LoadFromBackend from snapshot failed: %v", err)
	}

	lookupAttr, err := vol2.Lookup(ctx, dirAttr.GetInode().GetIno(), "file.txt")
	if err != nil {
		t.Fatalf("Lookup after snapshot restore failed: %v", err)
	}
	if lookupAttr.GetInode().GetIno() != fileAttr.GetInode().GetIno() {
		t.Errorf("inode mismatch: %d vs %d", lookupAttr.GetInode().GetIno(), fileAttr.GetInode().GetIno())
	}

	data, _, _, err := vol2.ReadFile(ctx, lookupAttr.GetInode().GetIno(), 0, 100)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if !bytes.Equal(data, content) {
		t.Errorf("content mismatch: got %q, want %q", string(data), string(content))
	}
}

func TestSQLiteMetadataStoreCrashRecoveryWithStream(t *testing.T) {
	ctx := t.Context()
	walDir := t.TempDir()
	localDir := t.TempDir()
	backend := inmemorystorage.New()
	broadcaster := NewEventBroadcaster()
	volID := "vol-crash-rec"
	streamID := StreamIDForVolume(volID)

	stream1 := testOpenStream(t, walDir, streamID, backend)

	vol1 := MustNewVolume(t, volID, backend, broadcaster,
		WithMetadataIndex("sqlite"),
		WithLocalStorageDir(localDir),
		WithStream(stream1),
	)
	if err := vol1.LoadFromBackend(ctx); err != nil {
		t.Fatalf("LoadFromBackend failed: %v", err)
	}

	// Perform 10 operations
	for i := 0; i < 10; i++ {
		name := fmt.Sprintf("dir_%d", i)
		_, err := vol1.Mkdir(ctx, 1, name, 0755, 0, 0)
		if err != nil {
			t.Fatalf("Mkdir %s failed: %v", name, err)
		}
	}

	// Simulate crash: close stream and delete SQLite DB to force stream replay from 0
	_ = stream1.Close()
	_ = vol1.Close()
	_ = os.Remove(filepath.Join(localDir, "metadata.sqlite"))

	// Reopen with new stream instance
	stream2 := testOpenStream(t, walDir, streamID, backend)

	vol2 := MustNewVolume(t, volID, backend, broadcaster,
		WithMetadataIndex("sqlite"),
		WithLocalStorageDir(localDir),
		WithStream(stream2),
	)
	defer vol2.Close()

	if err := vol2.LoadFromBackend(ctx); err != nil {
		t.Fatalf("LoadFromBackend replay failed: %v", err)
	}

	// Verify all 10 directories are present
	entries, err := vol2.ReadDir(ctx, 1)
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}
	if len(entries) != 10 {
		t.Fatalf("expected 10 replayed entries, got %d", len(entries))
	}
}

func TestSQLiteReadCacheHitRateAndNegativeCaching(t *testing.T) {
	ctx := t.Context()
	localDir := t.TempDir()
	backend := inmemorystorage.New()
	volID := "vol-sqlite-cache-test"

	vol := MustNewVolume(t, volID, backend, NewEventBroadcaster(),
		WithMetadataIndex("sqlite"),
		WithLocalStorageDir(localDir),
	)
	defer vol.Close()

	if err := vol.LoadFromBackend(ctx); err != nil {
		t.Fatalf("LoadFromBackend failed: %v", err)
	}

	vol.MetadataCacheResetStats()

	// 1. Point lookup of non-existent file -> miss + negative cache entry created
	_, err := vol.Lookup(ctx, 1, "does_not_exist.txt")
	if err == nil {
		t.Fatalf("expected error looking up non-existent file")
	}
	stats := vol.MetadataCacheStats()
	if stats.Misses != 1 || stats.Hits != 0 {
		t.Fatalf("expected 1 miss and 0 hits on first lookup, got misses=%d hits=%d", stats.Misses, stats.Hits)
	}
	if stats.Entries == 0 {
		t.Fatalf("expected negative cache entry stored, got entries=%d", stats.Entries)
	}

	// 2. Second lookup of the same non-existent file -> negative cache hit (no SQLite query!)
	_, err = vol.Lookup(ctx, 1, "does_not_exist.txt")
	if err == nil {
		t.Fatalf("expected error looking up non-existent file")
	}
	stats = vol.MetadataCacheStats()
	if stats.Hits != 1 || stats.Misses != 1 {
		t.Fatalf("expected 1 hit and 1 miss after second lookup, got hits=%d misses=%d", stats.Hits, stats.Misses)
	}
	if stats.HitRate != 0.5 {
		t.Fatalf("expected hitRate 0.5, got %f", stats.HitRate)
	}

	// 3. Create the file -> replaces negative cache entry with positive entry in same transaction
	attr, err := vol.CreateFile(ctx, 1, "does_not_exist.txt", 0644, []byte("now I exist"), 0, 0, 0)
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}
	if err := vol.FlushOverlay(ctx); err != nil {
		t.Fatalf("FlushOverlay failed: %v", err)
	}

	// 4. Lookup now succeeds directly from cache (hit!)
	lookupAttr, err := vol.Lookup(ctx, 1, "does_not_exist.txt")
	if err != nil {
		t.Fatalf("Lookup failed: %v", err)
	}
	if lookupAttr.GetInode().GetIno() != attr.GetInode().GetIno() || lookupAttr.GetInode().GetSize() != int64(len("now I exist")) {
		t.Fatalf("unexpected lookup attr: %+v", lookupAttr)
	}
	stats = vol.MetadataCacheStats()
	// Lookup checks DirEntry (hit) and toEntryAttr checks Inode (hit)
	if stats.Hits < 2 {
		t.Fatalf("expected hits >= 2, got %d", stats.Hits)
	}

	// 5. Stat the inode -> cache hit!
	vol.MetadataCacheResetStats()
	statAttr, err := vol.GetAttr(ctx, attr.GetInode().GetIno())
	if err != nil {
		t.Fatalf("GetAttr failed: %v", err)
	}
	if statAttr.GetInode().GetIno() != attr.GetInode().GetIno() {
		t.Fatalf("mismatched stat inode: %d vs %d", statAttr.GetInode().GetIno(), attr.GetInode().GetIno())
	}
	stats = vol.MetadataCacheStats()
	if stats.Hits != 1 || stats.Misses != 0 {
		t.Fatalf("expected 1 hit 0 misses on GetAttr, got hits=%d misses=%d", stats.Hits, stats.Misses)
	}

	// 6. Delete file -> updates cache entry to negative
	_, err = vol.Unlink(ctx, 1, "does_not_exist.txt")
	if err != nil {
		t.Fatalf("Unlink failed: %v", err)
	}
	if err := vol.FlushOverlay(ctx); err != nil {
		t.Fatalf("FlushOverlay failed: %v", err)
	}

	// 7. Lookup after deletion -> immediate negative cache hit (no SQLite query!)
	vol.MetadataCacheResetStats()
	_, err = vol.Lookup(ctx, 1, "does_not_exist.txt")
	if err == nil {
		t.Fatalf("expected error looking up unlinked file")
	}
	stats = vol.MetadataCacheStats()
	if stats.Hits != 1 || stats.Misses != 0 {
		t.Fatalf("expected 1 hit (negative cache hit) and 0 misses, got hits=%d misses=%d", stats.Hits, stats.Misses)
	}
}

func TestSQLiteReadCacheCoherence(t *testing.T) {
	ctx := t.Context()
	localDir := t.TempDir()
	backend := inmemorystorage.New()
	volID := "vol-sqlite-coherence"

	vol := MustNewVolume(t, volID, backend, NewEventBroadcaster(),
		WithMetadataIndex("sqlite"),
		WithLocalStorageDir(localDir),
	)
	defer vol.Close()

	if err := vol.LoadFromBackend(ctx); err != nil {
		t.Fatalf("LoadFromBackend failed: %v", err)
	}

	// Create file
	fAttr, err := vol.CreateFile(ctx, 1, "initial.txt", 0644, []byte("hello"), 0, 0, 0)
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}

	// Rename file
	_, err = vol.Rename(ctx, 1, "initial.txt", 1, "renamed.txt")
	if err != nil {
		t.Fatalf("Rename failed: %v", err)
	}
	if err := vol.FlushOverlay(ctx); err != nil {
		t.Fatalf("FlushOverlay failed: %v", err)
	}

	// Old name should be negative cache hit
	vol.MetadataCacheResetStats()
	_, err = vol.Lookup(ctx, 1, "initial.txt")
	if err == nil {
		t.Fatalf("expected error for old name after rename")
	}
	stats := vol.MetadataCacheStats()
	if stats.Hits != 1 || stats.Misses != 0 {
		t.Fatalf("expected 1 negative hit for old name, got hits=%d misses=%d", stats.Hits, stats.Misses)
	}

	// New name should be positive cache hit
	vol.MetadataCacheResetStats()
	rAttr, err := vol.Lookup(ctx, 1, "renamed.txt")
	if err != nil {
		t.Fatalf("Lookup for renamed file failed: %v", err)
	}
	if rAttr.GetInode().GetIno() != fAttr.GetInode().GetIno() {
		t.Fatalf("mismatched inode: %d vs %d", rAttr.GetInode().GetIno(), fAttr.GetInode().GetIno())
	}

	// Truncate file updates cached inode size
	_, err = vol.TruncateFile(ctx, fAttr.GetInode().GetIno(), 100)
	if err != nil {
		t.Fatalf("TruncateFile failed: %v", err)
	}
	if err := vol.FlushOverlay(ctx); err != nil {
		t.Fatalf("FlushOverlay failed: %v", err)
	}
	tAttr, err := vol.GetAttr(ctx, fAttr.GetInode().GetIno())
	if err != nil {
		t.Fatalf("GetAttr failed: %v", err)
	}
	if tAttr.GetInode().GetSize() != 100 {
		t.Fatalf("expected size 100 after truncate, got %d", tAttr.GetInode().GetSize())
	}

	// WriteFile updates cached inode size and mtime
	_, newSize, _, err := vol.WriteFile(ctx, fAttr.GetInode().GetIno(), 100, []byte(" appended"), 0)
	if err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}
	if newSize != 109 {
		t.Fatalf("expected size 109 after write, got %d", newSize)
	}
	if err := vol.Fsync(ctx, fAttr.GetInode().GetIno()); err != nil {
		t.Fatalf("Fsync failed: %v", err)
	}
	wAttr, err := vol.GetAttr(ctx, fAttr.GetInode().GetIno())
	if err != nil {
		t.Fatalf("GetAttr failed: %v", err)
	}
	if wAttr.GetInode().GetSize() != 109 {
		t.Fatalf("expected size 109 in cached inode after write, got %d", wAttr.GetInode().GetSize())
	}
}

func TestSQLiteReadCacheMemoryLimits(t *testing.T) {
	ctx := t.Context()
	localDir := t.TempDir()
	backend := inmemorystorage.New()
	volID := "vol-sqlite-limits"

	// Bounded to 50 entries and 10 KB
	vol := MustNewVolume(t, volID, backend, NewEventBroadcaster(),
		WithMetadataIndex("sqlite"),
		WithLocalStorageDir(localDir),
		WithMetadataCacheLimits(50, 10*1024),
	)
	defer vol.Close()

	if err := vol.LoadFromBackend(ctx); err != nil {
		t.Fatalf("LoadFromBackend failed: %v", err)
	}

	var inodes []uint64
	for i := 0; i < 200; i++ {
		attr, err := vol.CreateFile(ctx, 1, fmt.Sprintf("file_%d.txt", i), 0644, []byte("content"), 0, 0, 0)
		if err != nil {
			t.Fatalf("CreateFile failed: %v", err)
		}
		inodes = append(inodes, attr.GetInode().GetIno())
	}

	if err := vol.FlushOverlay(ctx); err != nil {
		t.Fatalf("FlushOverlay failed: %v", err)
	}

	stats := vol.MetadataCacheStats()
	if stats.Entries > 50 {
		t.Fatalf("expected <= 50 cache entries, got %d", stats.Entries)
	}
	if stats.Bytes > 10*1024 {
		t.Fatalf("expected <= %d bytes, got %d bytes", stats.MaxBytes, stats.Bytes)
	}

	// Lookup an early evicted file -> cache miss, reloads cleanly from SQLite
	vol.MetadataCacheResetStats()
	attr, err := vol.Lookup(ctx, 1, "file_0.txt")
	if err != nil {
		t.Fatalf("Lookup for evicted file_0.txt failed: %v", err)
	}
	if attr.GetInode().GetIno() != inodes[0] {
		t.Fatalf("mismatched inode for reloaded file_0.txt: %d vs %d", attr.GetInode().GetIno(), inodes[0])
	}
	stats = vol.MetadataCacheStats()
	if stats.Misses == 0 {
		t.Fatalf("expected at least 1 cache miss for evicted file_0.txt")
	}
}

func TestSQLiteReadCacheConcurrentStress(t *testing.T) {
	ctx := t.Context()
	localDir := t.TempDir()
	backend := inmemorystorage.New()
	volID := "vol-sqlite-concurrent-stress"

	vol := MustNewVolume(t, volID, backend, NewEventBroadcaster(),
		WithMetadataIndex("sqlite"),
		WithLocalStorageDir(localDir),
	)
	defer vol.Close()

	if err := vol.LoadFromBackend(ctx); err != nil {
		t.Fatalf("LoadFromBackend failed: %v", err)
	}

	// Pre-create initial files and directories
	var initInodes []uint64
	for i := 0; i < 30; i++ {
		attr, err := vol.CreateFile(ctx, 1, fmt.Sprintf("init_%d.txt", i), 0644, []byte("init"), 0, 0, 0)
		if err != nil {
			t.Fatalf("CreateFile failed: %v", err)
		}
		initInodes = append(initInodes, attr.GetInode().GetIno())
	}

	done := make(chan struct{})
	var wg sync.WaitGroup

	// Reader goroutines doing concurrent GetAttr and Lookup
	for r := 0; r < 8; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
					idx := readerID % len(initInodes)
					_, _ = vol.GetAttr(ctx, initInodes[idx])
					_, _ = vol.Lookup(ctx, 1, fmt.Sprintf("init_%d.txt", idx))
					// Also lookup non-existent files to stress negative caching concurrently
					_, _ = vol.Lookup(ctx, 1, fmt.Sprintf("missing_%d.txt", readerID))
				}
			}
		}(r)
	}

	// Writer goroutines creating, renaming, and unlinking
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				name := fmt.Sprintf("w_%d_%d.txt", writerID, i)
				attr, err := vol.CreateFile(ctx, 1, name, 0644, []byte("data"), 0, 0, 0)
				if err == nil {
					renamed := fmt.Sprintf("w_%d_%d_renamed.txt", writerID, i)
					_, _ = vol.Rename(ctx, 1, name, 1, renamed)
					_, _ = vol.GetAttr(ctx, attr.GetInode().GetIno())
					_, _ = vol.Unlink(ctx, 1, renamed)
				}
			}
		}(w)
	}

	// Let readers and writers run
	time.Sleep(300 * time.Millisecond)
	close(done)
	wg.Wait()

	stats := vol.MetadataCacheStats()
	t.Logf("Concurrent stress test completed: Hits=%d Misses=%d HitRate=%.2f%% Entries=%d Bytes=%d KB",
		stats.Hits, stats.Misses, stats.HitRate*100, stats.Entries, stats.Bytes/1024)
}

func TestSQLiteAsyncApplierOverlayCommitAndReadsDuringLag(t *testing.T) {
	ctx := t.Context()
	localDir := t.TempDir()
	backend := inmemorystorage.New()
	volID := "vol-sqlite-overlay-lag"

	var faultActive atomic.Bool
	faultActive.Store(true)

	vol := MustNewVolume(t, volID, backend, NewEventBroadcaster(),
		WithMetadataIndex("sqlite"),
		WithLocalStorageDir(localDir),
		WithApplierFaultHook(func() error {
			if faultActive.Load() {
				return errors.New("simulated SQLite apply fault")
			}
			return nil
		}),
	)
	defer vol.Close()

	if err := vol.LoadFromBackend(ctx); err != nil {
		t.Fatalf("LoadFromBackend failed: %v", err)
	}

	// 1. Create directory and files while SQLite applier is stalled by fault hook
	dirAttr, err := vol.Mkdir(ctx, 1, "testdir", 0755, 1000, 1000)
	if err != nil {
		t.Fatalf("Mkdir failed despite log append commit: %v", err)
	}

	fAttr1, err := vol.CreateFile(ctx, dirAttr.GetInode().GetIno(), "file1.txt", 0644, []byte("contents-of-file-1"), 1000, 1000, 0)
	if err != nil {
		t.Fatalf("CreateFile 1 failed: %v", err)
	}

	_, err = vol.CreateFile(ctx, dirAttr.GetInode().GetIno(), "file2.txt", 0644, []byte("contents-of-file-2"), 1000, 1000, 0)
	if err != nil {
		t.Fatalf("CreateFile 2 failed: %v", err)
	}

	// Rename file2 to file2_renamed
	_, err = vol.Rename(ctx, dirAttr.GetInode().GetIno(), "file2.txt", dirAttr.GetInode().GetIno(), "file2_renamed.txt")
	if err != nil {
		t.Fatalf("Rename failed: %v", err)
	}

	// Create file3, then unlink it
	_, err = vol.CreateFile(ctx, dirAttr.GetInode().GetIno(), "file3.txt", 0644, []byte("temp"), 1000, 1000, 0)
	if err != nil {
		t.Fatalf("CreateFile 3 failed: %v", err)
	}
	if _, err := vol.Unlink(ctx, dirAttr.GetInode().GetIno(), "file3.txt"); err != nil {
		t.Fatalf("Unlink failed: %v", err)
	}

	// Write more data to file1
	_, _, _, err = vol.WriteFile(ctx, fAttr1.GetInode().GetIno(), 0, []byte("updated-contents-1"), 0)
	if err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// 2. Verify lag exists and overlay is active
	lag := vol.ApplyLag()
	unappliedBytes := vol.UnappliedBytes()
	if lag == 0 {
		t.Fatalf("expected ApplyLag > 0 while fault hook is active, got %d", lag)
	}
	if unappliedBytes == 0 {
		t.Fatalf("expected UnappliedBytes > 0, got %d", unappliedBytes)
	}

	// 3. Reads during lag: MUST return latest state directly from overlay
	// Lookup file1
	lAttr1, err := vol.Lookup(ctx, dirAttr.GetInode().GetIno(), "file1.txt")
	if err != nil {
		t.Fatalf("Lookup file1 failed during lag: %v", err)
	}
	if lAttr1.GetInode().GetIno() != fAttr1.GetInode().GetIno() || lAttr1.GetInode().GetSize() != int64(len("updated-contents-1")) {
		t.Fatalf("unexpected attr for file1: %+v", lAttr1)
	}

	// Lookup renamed file2
	lAttr2, err := vol.Lookup(ctx, dirAttr.GetInode().GetIno(), "file2_renamed.txt")
	if err != nil {
		t.Fatalf("Lookup file2_renamed failed during lag: %v", err)
	}
	if lAttr2.GetName() != "file2_renamed.txt" {
		t.Fatalf("unexpected name: %s", lAttr2.GetName())
	}

	// Lookup old name of file2 -> ENOENT
	_, err = vol.Lookup(ctx, dirAttr.GetInode().GetIno(), "file2.txt")
	if err == nil {
		t.Fatalf("expected ENOENT for old name of file2")
	}

	// Lookup deleted file3 -> ENOENT
	_, err = vol.Lookup(ctx, dirAttr.GetInode().GetIno(), "file3.txt")
	if err == nil {
		t.Fatalf("expected ENOENT for unlinked file3")
	}

	// ReadDir on testdir during lag -> should see file1.txt and file2_renamed.txt (2 entries)
	entries, err := vol.ReadDir(ctx, dirAttr.GetInode().GetIno())
	if err != nil {
		t.Fatalf("ReadDir failed during lag: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries in testdir during lag, got %d", len(entries))
	}
	names := []string{entries[0].GetName(), entries[1].GetName()}
	if names[0] != "file1.txt" || names[1] != "file2_renamed.txt" {
		t.Fatalf("unexpected entries in ReadDir: %v", names)
	}

	// 4. Clear the fault -> applier retries with backoff and converges to 0 lag
	faultActive.Store(false)
	if err := vol.FlushOverlay(ctx); err != nil {
		t.Fatalf("FlushOverlay failed after clearing fault: %v", err)
	}

	if finalLag := vol.ApplyLag(); finalLag != 0 {
		t.Fatalf("expected ApplyLag == 0 after flush, got %d", finalLag)
	}
	if finalUnapplied := vol.UnappliedBytes(); finalUnapplied != 0 {
		t.Fatalf("expected UnappliedBytes == 0 after flush, got %d", finalUnapplied)
	}

	// Verify reads still return correct state once applied to SQLite
	entriesAfter, err := vol.ReadDir(ctx, dirAttr.GetInode().GetIno())
	if err != nil {
		t.Fatalf("ReadDir after flush failed: %v", err)
	}
	if len(entriesAfter) != 2 {
		t.Fatalf("expected 2 entries after flush, got %d", len(entriesAfter))
	}
}

func TestSQLiteApplierCoalescing(t *testing.T) {
	ctx := t.Context()
	localDir := t.TempDir()
	backend := inmemorystorage.New()
	volID := "vol-sqlite-coalesce"

	vol := MustNewVolume(t, volID, backend, NewEventBroadcaster(),
		WithMetadataIndex("sqlite"),
		WithLocalStorageDir(localDir),
		WithApplierBatchSize(100),
	)
	defer vol.Close()

	if err := vol.LoadFromBackend(ctx); err != nil {
		t.Fatalf("LoadFromBackend failed: %v", err)
	}

	// Create a single file
	attr, err := vol.CreateFile(ctx, 1, "hot_file.bin", 0644, []byte("initial"), 0, 0, 0)
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}

	// Rapidly perform 30 write and truncate operations on the same inode
	for i := 0; i < 30; i++ {
		payload := fmt.Sprintf("iteration-%d", i)
		_, _, _, err = vol.WriteFile(ctx, attr.GetInode().GetIno(), 0, []byte(payload), 0)
		if err != nil {
			t.Fatalf("WriteFile iter %d failed: %v", i, err)
		}
	}

	if err := vol.Fsync(ctx, attr.GetInode().GetIno()); err != nil {
		t.Fatalf("Fsync failed: %v", err)
	}

	// Flush overlay to SQLite
	if err := vol.FlushOverlay(ctx); err != nil {
		t.Fatalf("FlushOverlay failed: %v", err)
	}

	// Verify final state
	finalAttr, err := vol.GetAttr(ctx, attr.GetInode().GetIno())
	if err != nil {
		t.Fatalf("GetAttr failed: %v", err)
	}
	expectedLen := int64(len("iteration-29"))
	if finalAttr.GetInode().GetSize() != expectedLen {
		t.Fatalf("expected size %d, got %d", expectedLen, finalAttr.GetInode().GetSize())
	}
}

func TestSQLiteApplierFaultInjectionAndDegradedState(t *testing.T) {
	ctx := t.Context()
	localDir := t.TempDir()
	backend := inmemorystorage.New()
	volID := "vol-sqlite-fault-degraded"

	var faultActive atomic.Bool
	faultActive.Store(true)

	vol := MustNewVolume(t, volID, backend, NewEventBroadcaster(),
		WithMetadataIndex("sqlite"),
		WithLocalStorageDir(localDir),
		WithApplierFaultHook(func() error {
			if faultActive.Load() {
				return errors.New("simulated SQLite disk I/O error")
			}
			return nil
		}),
	)
	defer vol.Close()

	if err := vol.LoadFromBackend(ctx); err != nil {
		t.Fatalf("LoadFromBackend failed: %v", err)
	}

	// Perform operations that succeed on log append
	for i := 0; i < 5; i++ {
		name := fmt.Sprintf("degraded_test_%d.txt", i)
		_, err := vol.CreateFile(ctx, 1, name, 0644, []byte("sample"), 0, 0, 0)
		if err != nil {
			t.Fatalf("CreateFile %d failed: %v", i, err)
		}
	}

	// Wait briefly for applier to retry and trigger degraded threshold (>= 3 consecutive failures)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if vol.IsDegraded() && vol.ApplyFailures() >= 3 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if !vol.IsDegraded() {
		t.Fatalf("expected store to be marked DEGRADED after consecutive failures, failures=%d", vol.ApplyFailures())
	}

	// Clear fault hook
	faultActive.Store(false)

	// Flush overlay -> should recover from degraded and complete
	if err := vol.FlushOverlay(ctx); err != nil {
		t.Fatalf("FlushOverlay failed after clearing fault: %v", err)
	}

	if vol.IsDegraded() {
		t.Fatalf("expected store to no longer be marked DEGRADED after successful apply")
	}
}

func TestSQLiteBackpressureUnderLag(t *testing.T) {
	ctx := t.Context()
	localDir := t.TempDir()
	backend := inmemorystorage.New()
	volID := "vol-sqlite-backpressure"

	var faultActive atomic.Bool
	faultActive.Store(true)

	// Configure a tight bound on unapplied overlay bytes (e.g. 1024 bytes)
	vol := MustNewVolume(t, volID, backend, NewEventBroadcaster(),
		WithMetadataIndex("sqlite"),
		WithLocalStorageDir(localDir),
		WithMaxUnappliedBytes(1024),
		WithApplierFaultHook(func() error {
			if faultActive.Load() {
				return errors.New("simulated SQLite backpressure fault")
			}
			return nil
		}),
	)
	defer vol.Close()

	if err := vol.LoadFromBackend(ctx); err != nil {
		t.Fatalf("LoadFromBackend failed: %v", err)
	}

	// Write single file that exceeds overlay capacity
	_, err := vol.CreateFile(ctx, 1, "bp_file.bin", 0644, make([]byte, 1500), 0, 0, 0)
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}

	if vol.UnappliedBytes() < 1024 {
		t.Fatalf("expected UnappliedBytes >= 1024, got %d", vol.UnappliedBytes())
	}

	// The next operation with a short timeout should be blocked by backpressure and time out
	timeoutCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	_, err = vol.CreateFile(timeoutCtx, 1, "blocked.txt", 0644, []byte("data"), 0, 0, 0)
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded due to backpressure block, got %v", err)
	}

	// Clear fault -> applier drains overlay, unapplied bytes decrease, writers proceed
	faultActive.Store(false)
	if err := vol.FlushOverlay(ctx); err != nil {
		t.Fatalf("FlushOverlay failed: %v", err)
	}

	if ub := vol.UnappliedBytes(); ub != 0 {
		t.Fatalf("expected UnappliedBytes == 0 after flush, got %d", ub)
	}

	// Now writing succeeds immediately without blocking
	_, err = vol.CreateFile(ctx, 1, "unblocked.txt", 0644, []byte("success"), 0, 0, 0)
	if err != nil {
		t.Fatalf("CreateFile after backpressure release failed: %v", err)
	}
}

func TestSQLiteCrashRecoveryDuringWriteBurst(t *testing.T) {
	ctx := t.Context()
	localDir := t.TempDir()
	walDir := t.TempDir()
	backend := inmemorystorage.New()
	volID := "vol-sqlite-crash-burst"

	streamID := StreamIDForVolume(volID)
	stream := testOpenStream(t, walDir, streamID, backend)

	var stallApplier atomic.Bool
	stallApplier.Store(true)

	// Start volume with stalled SQLite applier so writes accumulate in stream and overlay
	vol := MustNewVolume(t, volID, backend, NewEventBroadcaster(),
		WithMetadataIndex("sqlite"),
		WithLocalStorageDir(localDir),
		WithStream(stream),
		WithApplierFaultHook(func() error {
			if stallApplier.Load() {
				return errors.New("stalled SQLite applier to simulate lag before crash")
			}
			return nil
		}),
	)
	if err := vol.LoadFromBackend(ctx); err != nil {
		t.Fatalf("LoadFromBackend failed: %v", err)
	}

	// Perform burst of 30 acknowledged file creates and writes
	expectedFiles := make(map[string]int64)
	for i := 0; i < 30; i++ {
		name := fmt.Sprintf("burst_file_%d.txt", i)
		content := fmt.Sprintf("burst-content-%d", i)
		attr, cErr := vol.CreateFile(ctx, 1, name, 0644, []byte(content), 0, 0, 0)
		if cErr != nil {
			t.Fatalf("CreateFile %d failed: %v", i, cErr)
		}
		expectedFiles[name] = attr.GetInode().GetSize()
	}

	// Confirm lag is positive (SQLite is behind stream log)
	if vol.ApplyLag() == 0 {
		t.Fatalf("expected ApplyLag > 0 before crash simulation")
	}

	// Abruptly close volume and stream without flushing overlay (simulating kill -9 / crash)
	_ = vol.Close()

	// 2. Restart from backend: open new stream and volume with the same storage dirs
	newStream := testOpenStream(t, walDir, streamID, backend)

	vol2 := MustNewVolume(t, volID, backend, NewEventBroadcaster(),
		WithMetadataIndex("sqlite"),
		WithLocalStorageDir(localDir),
		WithStream(newStream),
	)
	defer vol2.Close()

	if err := vol2.LoadFromBackend(ctx); err != nil {
		t.Fatalf("LoadFromBackend on recovered volume failed: %v", err)
	}

	// 3. Verify ALL acknowledged state is completely restored
	entries, err := vol2.ReadDir(ctx, 1)
	if err != nil {
		t.Fatalf("ReadDir on recovered volume failed: %v", err)
	}
	if len(entries) != len(expectedFiles) {
		t.Fatalf("expected %d entries after crash recovery, got %d", len(expectedFiles), len(entries))
	}

	for _, entry := range entries {
		expectedSize, ok := expectedFiles[entry.GetName()]
		if !ok {
			t.Fatalf("unexpected entry %q in recovered directory", entry.GetName())
		}
		if entry.GetInode().GetSize() != expectedSize {
			t.Fatalf("entry %q size %d != expected %d", entry.GetName(), entry.GetInode().GetSize(), expectedSize)
		}
	}
}

func TestSQLiteSnapshotFlushesOverlay(t *testing.T) {
	ctx := t.Context()
	localDir := t.TempDir()
	backend := inmemorystorage.New()
	volID := "vol-sqlite-snap-flush"

	vol := MustNewVolume(t, volID, backend, NewEventBroadcaster(),
		WithMetadataIndex("sqlite"),
		WithLocalStorageDir(localDir),
	)
	defer vol.Close()

	if err := vol.LoadFromBackend(ctx); err != nil {
		t.Fatalf("LoadFromBackend failed: %v", err)
	}

	// Create files
	for i := 0; i < 5; i++ {
		_, err := vol.CreateFile(ctx, 1, fmt.Sprintf("snap_f_%d.txt", i), 0644, []byte("data"), 0, 0, 0)
		if err != nil {
			t.Fatalf("CreateFile failed: %v", err)
		}
	}

	// Trigger snapshot flush to backend
	if err := vol.FlushToBackend(ctx); err != nil {
		t.Fatalf("FlushToBackend failed: %v", err)
	}

	// Verify overlay is completely clean
	if unapplied := vol.UnappliedBytes(); unapplied != 0 {
		t.Fatalf("expected UnappliedBytes == 0 after snapshot flush, got %d", unapplied)
	}
	if lag := vol.ApplyLag(); lag != 0 {
		t.Fatalf("expected ApplyLag == 0 after snapshot flush, got %d", lag)
	}

	// Restore from published snapshot on fresh volume to confirm validity
	freshDir := t.TempDir()
	vol2 := MustNewVolume(t, volID, backend, NewEventBroadcaster(),
		WithMetadataIndex("sqlite"),
		WithLocalStorageDir(freshDir),
	)
	defer vol2.Close()

	if err := vol2.LoadFromBackend(ctx); err != nil {
		t.Fatalf("LoadFromBackend from published snapshot failed: %v", err)
	}

	entries, err := vol2.ReadDir(ctx, 1)
	if err != nil {
		t.Fatalf("ReadDir on fresh volume failed: %v", err)
	}
	if len(entries) != 5 {
		t.Fatalf("expected 5 files in restored snapshot, got %d", len(entries))
	}
}

func TestSQLiteTransactionBoundaryBatchingAndMidLogRecovery(t *testing.T) {
	ctx := t.Context()
	localDir := t.TempDir()
	walDir := t.TempDir()
	backend := inmemorystorage.New()
	volID := "vol-sqlite-tx-boundary"

	streamID := StreamIDForVolume(volID)
	stream := testOpenStream(t, walDir, streamID, backend)

	// 1. Start volume with batchSize = 1 (smaller than a rename's change count)
	vol := MustNewVolume(t, volID, backend, NewEventBroadcaster(),
		WithMetadataIndex("sqlite"),
		WithLocalStorageDir(localDir),
		WithStream(stream),
		WithApplierBatchSize(1),
	)
	if err := vol.LoadFromBackend(ctx); err != nil {
		t.Fatalf("LoadFromBackend failed: %v", err)
	}

	// Create initial file and flush to SQLite
	fAttr, err := vol.CreateFile(ctx, 1, "before_rename.txt", 0644, []byte("tx-boundary-test"), 0, 0, 0)
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}
	if err := vol.FlushOverlay(ctx); err != nil {
		t.Fatalf("FlushOverlay failed: %v", err)
	}

	posBeforeRename := vol.MetadataAppliedPosition()

	// 2. Perform a multi-change Rename (delete old DirEntry, insert new DirEntry, update Inode)
	// while applier is stalled by fault hook
	var faultActive atomic.Bool
	faultActive.Store(true)
	vol.SetApplierFaultHook(func() error {
		if faultActive.Load() {
			return errors.New("simulated stall during rename")
		}
		return nil
	})

	_, err = vol.Rename(ctx, 1, "before_rename.txt", 1, "after_rename.txt")
	if err != nil {
		t.Fatalf("Rename failed: %v", err)
	}

	// SQLite position must NOT have advanced partially
	if pos := vol.MetadataAppliedPosition(); pos != posBeforeRename {
		t.Fatalf("SQLiteAppliedPosition advanced unexpectedly before batch apply: %d vs %d", pos, posBeforeRename)
	}

	// Abruptly simulate crash without draining overlay
	_ = vol.Close()

	// 3. Restart from the EXISTING SQLite database file (without deleting it) + WAL stream replay
	newStream := testOpenStream(t, walDir, streamID, backend)

	vol2 := MustNewVolume(t, volID, backend, NewEventBroadcaster(),
		WithMetadataIndex("sqlite"),
		WithLocalStorageDir(localDir),
		WithStream(newStream),
		WithApplierBatchSize(1),
	)
	defer vol2.Close()

	if err := vol2.LoadFromBackend(ctx); err != nil {
		t.Fatalf("LoadFromBackend on recovered volume failed: %v", err)
	}

	// 4. Verify rename transaction is fully consistent in recovered SQLite view
	_, err = vol2.Lookup(ctx, 1, "before_rename.txt")
	if err == nil {
		t.Fatalf("expected ENOENT for old name after recovery from existing SQLite file")
	}

	renamedAttr, err := vol2.Lookup(ctx, 1, "after_rename.txt")
	if err != nil {
		t.Fatalf("Lookup after_rename failed after recovery: %v", err)
	}
	if renamedAttr.GetInode().GetIno() != fAttr.GetInode().GetIno() {
		t.Fatalf("mismatched inode: %d vs %d", renamedAttr.GetInode().GetIno(), fAttr.GetInode().GetIno())
	}

	entries, err := vol2.ReadDir(ctx, 1)
	if err != nil {
		t.Fatalf("ReadDir failed after recovery: %v", err)
	}
	if len(entries) != 1 || entries[0].GetName() != "after_rename.txt" {
		t.Fatalf("unexpected entries after recovery: %+v", entries)
	}
}

func TestSQLiteSnapshotFlushNoStarvationUnderContinuousWrites(t *testing.T) {
	ctx := t.Context()
	localDir := t.TempDir()
	backend := inmemorystorage.New()
	volID := "vol-sqlite-continuous-writes"

	vol := MustNewVolume(t, volID, backend, NewEventBroadcaster(),
		WithMetadataIndex("sqlite"),
		WithLocalStorageDir(localDir),
	)
	defer vol.Close()

	if err := vol.LoadFromBackend(ctx); err != nil {
		t.Fatalf("LoadFromBackend failed: %v", err)
	}

	stopWrites := make(chan struct{})
	var writeCount atomic.Int64
	var writeErr atomic.Value

	go func() {
		idx := 0
		for {
			select {
			case <-stopWrites:
				return
			default:
				_, err := vol.CreateFile(ctx, 1, fmt.Sprintf("continuous_%d.txt", idx), 0644, []byte("data"), 0, 0, 0)
				if err != nil {
					writeErr.Store(err)
					return
				}
				writeCount.Add(1)
				idx++
				time.Sleep(1 * time.Millisecond)
			}
		}
	}()

	// Wait until several writes have completed
	for writeCount.Load() < 10 {
		if errVal := writeErr.Load(); errVal != nil {
			t.Fatalf("Writer goroutine failed early: %v", errVal)
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Trigger snapshot flush to backend; must complete promptly without starving
	flushCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	flushDone := make(chan error, 1)
	go func() {
		flushDone <- vol.FlushToBackend(flushCtx)
	}()

	select {
	case err := <-flushDone:
		if err != nil {
			t.Fatalf("FlushToBackend failed during continuous writes: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("FlushToBackend stalled/starved under continuous writes")
	}

	close(stopWrites)
	if errVal := writeErr.Load(); errVal != nil {
		t.Fatalf("Writer goroutine failed: %v", errVal)
	}
}

func TestMemoryMetadataIndexOperationsAndSnapshots(t *testing.T) {
	ctx := t.Context()
	backend := inmemorystorage.New()
	walDir := t.TempDir()
	streamID := uuid.New()
	volID := "vol-memory-index-test"

	stream := testOpenStream(t, walDir, streamID, backend)

	vol := MustNewVolume(t, volID, backend, NewEventBroadcaster(),
		WithMetadataIndex("memory"),
		WithStream(stream),
		WithDurability(walclient.Local),
	)
	defer vol.Close()

	if err := vol.LoadFromBackend(ctx); err != nil {
		t.Fatalf("LoadFromBackend failed: %v", err)
	}

	// 1. Create directory and files
	dirAttr, err := vol.Mkdir(ctx, 1, "testdir", 0755, 0, 0)
	if err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}

	f1Attr, err := vol.CreateFile(ctx, dirAttr.GetInode().GetIno(), "f1.txt", 0644, []byte("memory-index-content"), 0, 0, 0)
	if err != nil {
		t.Fatalf("CreateFile f1 failed: %v", err)
	}

	// 2. Read back
	data, _, _, err := vol.ReadFile(ctx, f1Attr.GetInode().GetIno(), 0, 100)
	if err != nil || string(data) != "memory-index-content" {
		t.Fatalf("ReadFile failed: got %q, err %v", string(data), err)
	}

	// 3. Snapshot creation (publishes memory snapshot pointer)
	snapName, err := vol.CreateSnapshot(ctx, "")
	if err != nil {
		t.Fatalf("CreateSnapshot failed: %v", err)
	}
	if snapName == "" {
		t.Fatalf("empty snapshot name")
	}

	// 4. Add more data after snapshot
	f2Attr, err := vol.CreateFile(ctx, dirAttr.GetInode().GetIno(), "f2.txt", 0644, []byte("f2-content-after-snap"), 0, 0, 0)
	if err != nil {
		t.Fatalf("CreateFile f2 failed: %v", err)
	}

	// 5. Simulate restart with memory index and stream replay
	stream2 := testOpenStream(t, walDir, streamID, backend)

	vol2 := MustNewVolume(t, volID, backend, NewEventBroadcaster(),
		WithMetadataIndex("memory"),
		WithStream(stream2),
	)
	defer vol2.Close()

	if err := vol2.LoadFromBackend(ctx); err != nil {
		t.Fatalf("vol2.LoadFromBackend failed: %v", err)
	}

	// Verify both f1 and f2 exist
	entries, err := vol2.ReadDir(ctx, dirAttr.GetInode().GetIno())
	if err != nil || len(entries) != 2 {
		t.Fatalf("expected 2 entries in dir, got %d (err: %v)", len(entries), err)
	}

	f2Data, _, _, err := vol2.ReadFile(ctx, f2Attr.GetInode().GetIno(), 0, 100)
	if err != nil || string(f2Data) != "f2-content-after-snap" {
		t.Fatalf("ReadFile f2 failed: got %q, err %v", string(f2Data), err)
	}
}

func TestTruncateDownAndReadUntouchedChunks(t *testing.T) {
	for _, indexMode := range []string{"sqlite", "memory"} {
		t.Run(indexMode, func(t *testing.T) {
			ctx := t.Context()
			backend := inmemorystorage.New()
			walDir := t.TempDir()
			streamID := uuid.New()
			volID := fmt.Sprintf("vol-trunc-down-%s", indexMode)

			stream := testOpenStream(t, walDir, streamID, backend)

			vol := MustNewVolume(t, volID, backend, NewEventBroadcaster(),
				WithMetadataIndex(indexMode),
				WithStream(stream),
				WithDurability(walclient.Local),
				WithLocalStorageDir(t.TempDir()),
			)
			defer vol.Close()

			if err := vol.LoadFromBackend(ctx); err != nil {
				t.Fatalf("LoadFromBackend failed: %v", err)
			}

			// 1. Create large chunked file with 20 chunks (64 KiB each = 1,310,720 bytes)
			chunkSize := 64 * 1024
			numChunks := 20
			fullData := make([]byte, numChunks*chunkSize)
			for c := 0; c < numChunks; c++ {
				fillByte := byte((c + 1) % 250)
				for j := 0; j < chunkSize; j++ {
					fullData[c*chunkSize+j] = fillByte
				}
			}

			attr, err := vol.CreateFile(ctx, 1, "large.bin", 0644, fullData, 0, 0, 0)
			if err != nil {
				t.Fatalf("CreateFile failed: %v", err)
			}

			// Verify chunk 10 before truncate
			c10Data, _, _, err := vol.ReadFile(ctx, attr.GetInode().GetIno(), int64(10*chunkSize), int64(chunkSize))
			if err != nil || len(c10Data) != chunkSize || c10Data[0] != byte(11) {
				t.Fatalf("Initial read of chunk 10 failed: len=%d, err=%v", len(c10Data), err)
			}

			// 2. First truncate down: from 20 chunks to 15 chunks (983,040 bytes)
			t1Size := int64(15 * chunkSize)
			_, err = vol.TruncateFile(ctx, attr.GetInode().GetIno(), t1Size)
			if err != nil {
				t.Fatalf("First TruncateFile failed: %v", err)
			}

			// 3. Second truncate down: from 15 chunks to 11 chunks + 1000 bytes (721,920 bytes)
			t2Size := int64(11*chunkSize + 1000)
			_, err = vol.TruncateFile(ctx, attr.GetInode().GetIno(), t2Size)
			if err != nil {
				t.Fatalf("Second TruncateFile failed: %v", err)
			}

			// 4. Read untouched chunk 5 (offset = 5 * 64KB) -> must be byte 6
			c5Data, _, _, err := vol.ReadFile(ctx, attr.GetInode().GetIno(), int64(5*chunkSize), int64(chunkSize))
			if err != nil || len(c5Data) != chunkSize {
				t.Fatalf("ReadFile chunk 5 failed: len=%d err=%v", len(c5Data), err)
			}
			for i, b := range c5Data {
				if b != byte(6) {
					t.Fatalf("Chunk 5 byte %d mismatch: got %d, want %d", i, b, byte(6))
				}
			}

			// 5. Read untouched chunk 10 (offset = 10 * 64KB) -> must be byte 11
			c10After, _, _, err := vol.ReadFile(ctx, attr.GetInode().GetIno(), int64(10*chunkSize), int64(chunkSize))
			if err != nil || len(c10After) != chunkSize {
				t.Fatalf("ReadFile chunk 10 failed: len=%d err=%v", len(c10After), err)
			}
			for i, b := range c10After {
				if b != byte(11) {
					t.Fatalf("Chunk 10 byte %d mismatch: got %d, want %d", i, b, byte(11))
				}
			}

			// 6. Read last partial chunk 11 (offset = 11 * 64KB, length = 1000 bytes)
			c11Data, _, _, err := vol.ReadFile(ctx, attr.GetInode().GetIno(), int64(11*chunkSize), 1000)
			if err != nil || len(c11Data) != 1000 {
				t.Fatalf("ReadFile chunk 11 failed: len=%d err=%v", len(c11Data), err)
			}
			for i, b := range c11Data {
				if b != byte(12) {
					t.Fatalf("Chunk 11 byte %d mismatch: got %d, want %d", i, b, byte(12))
				}
			}

			// 7. Read beyond EOF (chunk 13) -> must return 0 bytes
			pastEOF, _, _, err := vol.ReadFile(ctx, attr.GetInode().GetIno(), int64(13*chunkSize), int64(chunkSize))
			if err != nil || len(pastEOF) != 0 {
				t.Fatalf("ReadFile past EOF failed: len=%d err=%v", len(pastEOF), err)
			}

			// 8. Flush overlay to underlying index and verify consistency again
			if err := vol.FlushOverlay(ctx); err != nil {
				t.Fatalf("FlushOverlay failed: %v", err)
			}

			c10Flushed, _, _, err := vol.ReadFile(ctx, attr.GetInode().GetIno(), int64(10*chunkSize), int64(chunkSize))
			if err != nil || len(c10Flushed) != chunkSize || c10Flushed[0] != byte(11) {
				t.Fatalf("Post-flush ReadFile chunk 10 failed: len=%d err=%v", len(c10Flushed), err)
			}
		})
	}
}

func TestSQLiteIndexRebuildOnDatabaseCorruption(t *testing.T) {
	ctx := t.Context()
	localDir := t.TempDir()
	backend := inmemorystorage.New()
	volID := "vol-sqlite-corrupt-rebuild"

	vol := MustNewVolume(t, volID, backend, NewEventBroadcaster(),
		WithMetadataIndex("sqlite"),
		WithLocalStorageDir(localDir),
	)
	defer vol.Close()

	if err := vol.LoadFromBackend(ctx); err != nil {
		t.Fatalf("LoadFromBackend failed: %v", err)
	}

	// 1. Create initial files
	for i := 1; i <= 10; i++ {
		name := fmt.Sprintf("initial_%d.txt", i)
		data := []byte(fmt.Sprintf("initial data content %d", i))
		_, err := vol.CreateFile(ctx, 1, name, 0644, data, 0, 0, 0)
		if err != nil {
			t.Fatalf("CreateFile %d failed: %v", i, err)
		}
	}

	// Flush to make sure all records are applied into SQLite DB
	if err := vol.FlushOverlay(ctx); err != nil {
		t.Fatalf("FlushOverlay failed: %v", err)
	}

	// 2. Corrupt the SQLite database file on disk with random garbage
	dbPath := filepath.Join(localDir, "metadata.sqlite")
	garbage := bytes.Repeat([]byte{0xDE, 0xAD, 0xBE, 0xEF}, 1024)
	if err := os.WriteFile(dbPath, garbage, 0644); err != nil {
		t.Fatalf("failed to corrupt sqlite file: %v", err)
	}
	_ = os.Remove(dbPath + "-wal")
	_ = os.Remove(dbPath + "-shm")

	// 3. Write new files while the database is corrupt. Writes succeed on WAL append and overlay!
	for i := 1; i <= 5; i++ {
		name := fmt.Sprintf("after_corrupt_%d.txt", i)
		data := []byte(fmt.Sprintf("new data content after corrupt %d", i))
		_, err := vol.CreateFile(ctx, 1, name, 0644, data, 0, 0, 0)
		if err != nil {
			t.Fatalf("CreateFile after corrupt %d failed: %v", i, err)
		}
	}

	// Reads for both initial and new files must succeed without client-visible errors
	attrInit, err := vol.Lookup(ctx, 1, "initial_1.txt")
	if err != nil || attrInit == nil {
		t.Fatalf("Lookup initial_1.txt failed: %v", err)
	}
	attrNew, err := vol.Lookup(ctx, 1, "after_corrupt_1.txt")
	if err != nil || attrNew == nil {
		t.Fatalf("Lookup after_corrupt_1.txt failed: %v", err)
	}

	// 4. Wait for applier to detect unrecoverable corruption, discard, and rebuild the database
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if !vol.IsDegraded() && vol.ApplyLag() == 0 {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}

	if vol.IsDegraded() {
		t.Fatalf("expected volume to recover from degraded state after auto-rebuild")
	}

	// Flush overlay to new rebuilt index
	if err := vol.FlushOverlay(ctx); err != nil {
		t.Fatalf("FlushOverlay after rebuild failed: %v", err)
	}

	// 5. Verify all 15 files are present and readable
	entries, err := vol.ReadDir(ctx, 1)
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}
	if len(entries) != 15 {
		t.Fatalf("expected 15 directory entries after rebuild, got %d", len(entries))
	}

	for i := 1; i <= 10; i++ {
		name := fmt.Sprintf("initial_%d.txt", i)
		attr, err := vol.Lookup(ctx, 1, name)
		if err != nil || attr == nil {
			t.Fatalf("verification Lookup %s failed: %v", name, err)
		}
	}
	for i := 1; i <= 5; i++ {
		name := fmt.Sprintf("after_corrupt_%d.txt", i)
		attr, err := vol.Lookup(ctx, 1, name)
		if err != nil || attr == nil {
			t.Fatalf("verification Lookup %s failed: %v", name, err)
		}
	}

	// 6. Verify stats match full recount after rebuild
	st := vol.Stats()
	recounted, err := vol.countStatsFromIndex(ctx, vol.metadataView.Index())
	if err != nil {
		t.Fatalf("countStatsFromIndex failed after rebuild: %v", err)
	}
	if st.Stats.GetInodesDir() != recounted.GetInodesDir() {
		t.Fatalf("Stats directories mismatch: got %d, recount %d", st.Stats.GetInodesDir(), recounted.GetInodesDir())
	}
	if st.Stats.GetInodesFile() != recounted.GetInodesFile() {
		t.Fatalf("Stats regular files mismatch: got %d, recount %d", st.Stats.GetInodesFile(), recounted.GetInodesFile())
	}
	if st.Stats.GetLogicalBytes() != recounted.GetLogicalBytes() {
		t.Fatalf("Stats logical bytes mismatch: got %d, recount %d", st.Stats.GetLogicalBytes(), recounted.GetLogicalBytes())
	}
	if st.Stats.GetMaxIno() != recounted.GetMaxIno() {
		t.Fatalf("Stats max ino mismatch: got %d, recount %d", st.Stats.GetMaxIno(), recounted.GetMaxIno())
	}
}

func TestSQLiteIndexRebuildOnDatabaseDeletion(t *testing.T) {
	ctx := t.Context()
	localDir := t.TempDir()
	backend := inmemorystorage.New()
	volID := "vol-sqlite-delete-rebuild"

	vol := MustNewVolume(t, volID, backend, NewEventBroadcaster(),
		WithMetadataIndex("sqlite"),
		WithLocalStorageDir(localDir),
	)
	defer vol.Close()

	if err := vol.LoadFromBackend(ctx); err != nil {
		t.Fatalf("LoadFromBackend failed: %v", err)
	}

	// 1. Create initial files
	for i := 1; i <= 10; i++ {
		name := fmt.Sprintf("file_%d.txt", i)
		data := []byte(fmt.Sprintf("sample data %d", i))
		_, err := vol.CreateFile(ctx, 1, name, 0644, data, 0, 0, 0)
		if err != nil {
			t.Fatalf("CreateFile %d failed: %v", i, err)
		}
	}
	if err := vol.FlushOverlay(ctx); err != nil {
		t.Fatalf("FlushOverlay failed: %v", err)
	}

	// 2. Delete the SQLite database files
	dbPath := filepath.Join(localDir, "metadata.sqlite")
	_ = os.Remove(dbPath)
	_ = os.Remove(dbPath + "-wal")
	_ = os.Remove(dbPath + "-shm")

	// 3. Write additional files
	for i := 11; i <= 15; i++ {
		name := fmt.Sprintf("file_%d.txt", i)
		data := []byte(fmt.Sprintf("sample data %d", i))
		_, err := vol.CreateFile(ctx, 1, name, 0644, data, 0, 0, 0)
		if err != nil {
			t.Fatalf("CreateFile %d failed: %v", i, err)
		}
	}

	// 4. Wait for convergence
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if !vol.IsDegraded() && vol.ApplyLag() == 0 {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}

	if vol.IsDegraded() {
		t.Fatalf("expected volume to recover from deleted DB state after auto-rebuild")
	}

	if err := vol.FlushOverlay(ctx); err != nil {
		t.Fatalf("FlushOverlay failed: %v", err)
	}

	// 5. Verify all entries exist
	entries, err := vol.ReadDir(ctx, 1)
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}
	if len(entries) != 15 {
		t.Fatalf("expected 15 directory entries, got %d", len(entries))
	}
}

// TestRebuildIndexPreservesAllFilesWithoutLoss verifies that an index rebuild
// replays all records without data loss (issue #185).
func TestRebuildIndexPreservesAllFilesWithoutLoss(t *testing.T) {
	ctx := t.Context()
	localDir := t.TempDir()
	backend := inmemorystorage.New()
	volID := "vol-rebuild-preserves-all-files"

	vol := MustNewVolume(t, volID, backend, NewEventBroadcaster(),
		WithMetadataIndex("sqlite"),
		WithLocalStorageDir(localDir),
	)
	defer vol.Close()

	if err := vol.LoadFromBackend(ctx); err != nil {
		t.Fatalf("LoadFromBackend failed: %v", err)
	}

	// Write 30 files
	const N = 30
	for i := 1; i <= N; i++ {
		name := fmt.Sprintf("file_%d.txt", i)
		data := []byte(fmt.Sprintf("file content %d", i))
		_, err := vol.CreateFile(ctx, 1, name, 0644, data, 0, 0, 0)
		if err != nil {
			t.Fatalf("CreateFile %d failed: %v", i, err)
		}
	}
	if err := vol.FlushOverlay(ctx); err != nil {
		t.Fatalf("FlushOverlay failed: %v", err)
	}

	// Trigger manual rebuild
	if err := vol.metadataView.Rebuild(ctx); err != nil {
		t.Fatalf("Rebuild failed: %v", err)
	}

	// Verify all 30 entries exist and match content
	entries, err := vol.ReadDir(ctx, 1)
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}
	if len(entries) != N {
		t.Fatalf("expected %d entries after rebuild, got %d", N, len(entries))
	}

	for i := 1; i <= N; i++ {
		name := fmt.Sprintf("file_%d.txt", i)
		attr, err := vol.Lookup(ctx, 1, name)
		if err != nil {
			t.Fatalf("Lookup %s failed after rebuild: %v", name, err)
		}
		data, _, _, err := vol.ReadFile(ctx, attr.GetInode().GetIno(), 0, 100)
		if err != nil {
			t.Fatalf("ReadFile %s failed after rebuild: %v", name, err)
		}
		expected := fmt.Sprintf("file content %d", i)
		if string(data) != expected {
			t.Fatalf("ReadFile %s mismatch: got %q, want %q", name, string(data), expected)
		}
	}
}
