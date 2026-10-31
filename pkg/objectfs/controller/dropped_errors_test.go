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
	"iter"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/blob"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore/inmemorystorage"
	"github.com/gke-labs/in-cluster-storage/pkg/sds"
	"github.com/gke-labs/in-cluster-storage/pkg/wal/buffer/buffertest"
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
	"google.golang.org/protobuf/proto"
)

type faultyRootApplyIndex struct {
	sds.LocalIndex
	failApply atomic.Bool
}

func (f *faultyRootApplyIndex) ApplyBatch(ctx context.Context, changes []sds.Change) error {
	if f.failApply.Load() {
		return errors.New("injected apply batch failure")
	}
	return f.LocalIndex.ApplyBatch(ctx, changes)
}

type faultyRootApplyFactory struct {
	sds.IndexFactory
	idx *faultyRootApplyIndex
}

func (f *faultyRootApplyFactory) NewEmpty(ctx context.Context, streamID string, dir string) (sds.LocalIndex, error) {
	inner, err := f.IndexFactory.NewEmpty(ctx, streamID, dir)
	if err != nil {
		return nil, err
	}
	f.idx.LocalIndex = inner
	return f.idx, nil
}

func (f *faultyRootApplyFactory) OpenLocal(ctx context.Context, streamID string, dir string) (sds.LocalIndex, bool, error) {
	inner, found, err := f.IndexFactory.OpenLocal(ctx, streamID, dir)
	if err != nil || !found {
		return nil, found, err
	}
	f.idx.LocalIndex = inner
	return f.idx, true, nil
}

func openTestStream(t testing.TB, volID string) walclient.Stream {
	t.Helper()
	bt, _ := buffertest.Start(t.Context(), t)
	s, err := walclient.Open(t.Context(), t.TempDir(), StreamIDForVolume(volID), bt.Target,
		walclient.WithGRPCClient(bt.Client),
		walclient.WithDialOptions(bt.DialOpts...),
		walclient.WithoutFsync(),
	)
	if err != nil {
		t.Fatalf("failed to open test stream: %v", err)
	}
	return s
}

func TestRegression_RootInodeInitFailure(t *testing.T) {
	baseFactory, err := sds.GetIndexFactory("memory")
	if err != nil {
		t.Fatalf("GetIndexFactory failed: %v", err)
	}

	faultyIdx := &faultyRootApplyIndex{}
	faultyIdx.failApply.Store(true)
	factory := &faultyRootApplyFactory{IndexFactory: baseFactory, idx: faultyIdx}

	// 1. NewVolume should fail when root inode initialization fails.
	vol, err := NewVolume("test-vol-root-fail", nil, NewEventBroadcaster(),
		WithStream(openTestStream(t, "test-vol-root-fail")),
		WithIndexFactory(factory),
	)
	if err == nil {
		if vol != nil {
			_ = vol.Close()
		}
		t.Fatal("expected NewVolume to fail when root inode creation fails, got nil")
	}

	// 2. loadFromBackendMetadataLocked should also fail when root inode initialization fails.
	faultyIdx.failApply.Store(false)
	volSuccess, err := NewVolume("test-vol-root-reload", nil, NewEventBroadcaster(),
		WithStream(openTestStream(t, "test-vol-root-reload")),
		WithIndexFactory(factory),
	)
	if err != nil {
		t.Fatalf("expected NewVolume to succeed when failApply is false, got: %v", err)
	}
	defer volSuccess.Close()

	// Clear metadataView to simulate empty uninitialized reload and activate failure
	volSuccess.mu.Lock()
	volSuccess.metadataView = nil
	faultyIdx.failApply.Store(true)
	err = volSuccess.loadFromBackendMetadataLocked(t.Context())
	volSuccess.mu.Unlock()
	if err == nil {
		t.Fatal("expected loadFromBackendMetadataLocked to fail when root inode creation fails, got nil")
	}
}

type faultyChunkScanIndex struct {
	sds.LocalIndex
	failChunkScan atomic.Bool
}

func (f *faultyChunkScanIndex) Scan(ctx context.Context, typeName string, prefix []byte) iter.Seq2[proto.Message, error] {
	if f.failChunkScan.Load() && typeName == "objectfs.v1alpha1.FileChunk" {
		return func(yield func(proto.Message, error) bool) {
			yield(nil, errors.New("injected FileChunk scan failure"))
		}
	}
	return f.LocalIndex.Scan(ctx, typeName, prefix)
}

type faultyChunkScanFactory struct {
	sds.IndexFactory
	idx *faultyChunkScanIndex
}

func (f *faultyChunkScanFactory) NewEmpty(ctx context.Context, streamID string, dir string) (sds.LocalIndex, error) {
	inner, err := f.IndexFactory.NewEmpty(ctx, streamID, dir)
	if err != nil {
		return nil, err
	}
	f.idx.LocalIndex = inner
	return f.idx, nil
}

func (f *faultyChunkScanFactory) OpenLocal(ctx context.Context, streamID string, dir string) (sds.LocalIndex, bool, error) {
	inner, found, err := f.IndexFactory.OpenLocal(ctx, streamID, dir)
	if err != nil || !found {
		return nil, found, err
	}
	f.idx.LocalIndex = inner
	return f.idx, true, nil
}

func TestRegression_ChunkListScanFailure(t *testing.T) {
	ctx := t.Context()
	backend := inmemorystorage.New()

	baseFactory, err := sds.GetIndexFactory("memory")
	if err != nil {
		t.Fatalf("GetIndexFactory failed: %v", err)
	}

	faultyIdx := &faultyChunkScanIndex{}
	factory := &faultyChunkScanFactory{IndexFactory: baseFactory, idx: faultyIdx}

	vol := MustNewVolume(t, "test-vol-chunk-fail", backend, NewEventBroadcaster(),
		WithIndexFactory(factory),
		WithChunkSize(1024),
		WithMetadataCacheDisabled(true),
	)
	defer vol.Close()

	if err := vol.LoadFromBackend(ctx); err != nil {
		t.Fatalf("LoadFromBackend failed: %v", err)
	}

	// Create file with content and flush overlay so chunks are in the index
	data := []byte("hello chunk failure test data")
	attr, err := vol.CreateFile(ctx, 1, "test.txt", 0644, data, 1000, 1000, 0)
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}
	ino := attr.GetInode().GetIno()

	if err := vol.FlushOverlay(ctx); err != nil {
		t.Fatalf("FlushOverlay failed: %v", err)
	}

	// Now inject failure on FileChunk scan
	faultyIdx.failChunkScan.Store(true)

	// Attempt to read file; it must fail rather than returning empty/proceeding
	_, _, _, err = vol.ReadFile(ctx, ino, 0, int64(len(data)))
	if err == nil {
		t.Fatal("expected ReadFile to fail when FileChunk scan fails, got nil")
	}
}

type faultyListBackend struct {
	objectstore.Backend
	failList atomic.Bool
}

func (b *faultyListBackend) ListObjects(ctx context.Context, bucket, prefix string) ([]string, error) {
	if b.failList.Load() {
		return nil, errors.New("injected backend list failure")
	}
	return b.Backend.ListObjects(ctx, bucket, prefix)
}

func TestRegression_ListVolumesBackendFailure(t *testing.T) {
	ctx := t.Context()
	backend := &faultyListBackend{Backend: inmemorystorage.New()}
	server := NewTestServer(backend)

	// ListVolumes succeeds when backend does not fail
	resp, err := server.ListVolumes(ctx, &pb.ListVolumesRequest{})
	if err != nil {
		t.Fatalf("ListVolumes failed unexpectedly: %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
	}

	// ListVolumes must surface the error when backend.ListObjects fails
	backend.failList.Store(true)
	_, err = server.ListVolumes(ctx, &pb.ListVolumesRequest{})
	if err == nil {
		t.Fatal("expected ListVolumes to fail when backend.ListObjects fails, got nil")
	}
}

type faultyInodeGetIndex struct {
	sds.LocalIndex
	failInodeGet atomic.Bool
}

func (f *faultyInodeGetIndex) Get(ctx context.Context, typeName string, key sds.Key) (proto.Message, bool, error) {
	if f.failInodeGet.Load() && typeName == "objectfs.v1alpha1.Inode" {
		rootKey, _ := pkInode.Extract(&pb.Inode{Ino: proto.Uint64(1)})
		if !bytes.Equal(key.Bytes(), rootKey.Bytes()) {
			return nil, false, errors.New("injected Inode get failure")
		}
	}
	return f.LocalIndex.Get(ctx, typeName, key)
}

type faultyInodeGetFactory struct {
	sds.IndexFactory
	idx *faultyInodeGetIndex
}

func (f *faultyInodeGetFactory) NewEmpty(ctx context.Context, streamID string, dir string) (sds.LocalIndex, error) {
	inner, err := f.IndexFactory.NewEmpty(ctx, streamID, dir)
	if err != nil {
		return nil, err
	}
	f.idx.LocalIndex = inner
	return f.idx, nil
}

func (f *faultyInodeGetFactory) OpenLocal(ctx context.Context, streamID string, dir string) (sds.LocalIndex, bool, error) {
	inner, found, err := f.IndexFactory.OpenLocal(ctx, streamID, dir)
	if err != nil || !found {
		return nil, found, err
	}
	f.idx.LocalIndex = inner
	return f.idx, true, nil
}

func TestRegression_ReadDirEntryAttrFailure(t *testing.T) {
	ctx := t.Context()
	backend := inmemorystorage.New()

	baseFactory, err := sds.GetIndexFactory("memory")
	if err != nil {
		t.Fatalf("GetIndexFactory failed: %v", err)
	}

	faultyIdx := &faultyInodeGetIndex{}
	factory := &faultyInodeGetFactory{IndexFactory: baseFactory, idx: faultyIdx}

	vol := MustNewVolume(t, "test-vol-readdir-fail", backend, NewEventBroadcaster(),
		WithIndexFactory(factory),
		WithMetadataCacheDisabled(true),
	)
	defer vol.Close()

	if err := vol.LoadFromBackend(ctx); err != nil {
		t.Fatalf("LoadFromBackend failed: %v", err)
	}

	_, err = vol.CreateFile(ctx, 1, "child.txt", 0644, []byte("content"), 1000, 1000, 0)
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}

	if err := vol.FlushOverlay(ctx); err != nil {
		t.Fatalf("FlushOverlay failed: %v", err)
	}
	vol.metadataView.ClearCache()

	// ReadDir succeeds before failure injection
	entries, err := vol.ReadDir(ctx, 1)
	if err != nil {
		t.Fatalf("ReadDir failed unexpectedly: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}

	// Inject error on Inode Get
	faultyIdx.failInodeGet.Store(true)

	// ReadDir must return the error rather than silently omitting entries
	_, err = vol.ReadDir(ctx, 1)
	if err == nil {
		t.Fatal("expected ReadDir to fail when toEntryAttrLocked fails, got nil")
	}
	expectedPrefix := "readdir 1: entry \"child.txt\""
	if !strings.Contains(err.Error(), expectedPrefix) {
		t.Fatalf("expected error containing %q, got: %v", expectedPrefix, err)
	}
}

type faultyPutBackend struct {
	objectstore.Backend
	failPut atomic.Bool
}

func (b *faultyPutBackend) PutObject(ctx context.Context, volumeID, key string, stream blob.ByteStream) (string, error) {
	if b.failPut.Load() {
		return "", errors.New("injected backend put failure")
	}
	return b.Backend.PutObject(ctx, volumeID, key, stream)
}

func TestRegression_TruncateFilePutBlobsFailure(t *testing.T) {
	ctx := t.Context()
	backend := &faultyPutBackend{Backend: inmemorystorage.New()}

	vol := MustNewVolume(t, "test-vol-truncate-fail", backend, NewEventBroadcaster(),
		WithChunkSize(1024),
	)
	defer vol.Close()

	if err := vol.LoadFromBackend(ctx); err != nil {
		t.Fatalf("LoadFromBackend failed: %v", err)
	}

	// Create file spanning multiple chunks (> maxInlineLen of 4096)
	data := bytes.Repeat([]byte("a"), 10000)
	attr, err := vol.CreateFile(ctx, 1, "test.txt", 0644, data, 1000, 1000, 0)
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}
	ino := attr.GetInode().GetIno()

	if err := vol.Fsync(ctx, ino); err != nil {
		t.Fatalf("Fsync failed: %v", err)
	}

	// Now fail backend PutObject during TruncateFile (which uploads the truncated chunk boundary)
	backend.failPut.Store(true)

	_, err = vol.TruncateFile(ctx, ino, 5500)
	if err == nil {
		t.Fatal("expected TruncateFile to fail when PutBlobs fails, got nil")
	}
}

func TestRegression_CorruptLocalIndexRecovery(t *testing.T) {
	ctx := t.Context()
	backend := inmemorystorage.New()

	baseDir := t.TempDir()
	localDir := filepath.Join(baseDir, "idx")
	if err := os.MkdirAll(localDir, 0755); err != nil {
		t.Fatalf("failed to create local dir: %v", err)
	}
	dbPath := filepath.Join(localDir, "metadata.sqlite")
	// Write garbage into the local index SQLite file so OpenLocal fails
	if err := os.WriteFile(dbPath, []byte("garbage not a valid sqlite database header"), 0644); err != nil {
		t.Fatalf("failed to write corrupt db: %v", err)
	}

	vol, err := NewVolume("test-vol-corrupt-idx", backend, NewEventBroadcaster(),
		WithLocalStorageDir(localDir),
		WithStream(openTestStream(t, "test-vol-corrupt-idx")),
	)
	if err != nil {
		t.Fatalf("expected volume to start successfully despite corrupt local index, got: %v", err)
	}
	defer vol.Close()

	// Verify volume came up and is functional
	entries, err := vol.ReadDir(ctx, 1)
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected empty directory, got %d entries", len(entries))
	}

	// Verify the corrupt directory was renamed aside to a .corrupt-* path
	matches, err := filepath.Glob(filepath.Join(baseDir, "idx.corrupt-*"))
	if err != nil || len(matches) == 0 {
		t.Fatalf("expected corrupt directory to be renamed to idx.corrupt-*, found matches: %v", matches)
	}
}

func TestRegression_ShutdownFlushFailureSurfacedInClose(t *testing.T) {
	ctx := t.Context()
	backend := &faultyPutBackend{Backend: inmemorystorage.New()}
	server := NewTestServer(backend, WithServerSyncTimeout(1*time.Second))

	volumeID := "test-vol-shutdown-flush"
	vol, err := server.GetVolume(volumeID)
	if err != nil {
		t.Fatalf("GetVolume failed: %v", err)
	}

	// Create a file to ensure there is state that needs flushing
	_, err = vol.CreateFile(ctx, 1, "test.txt", 0644, []byte("data"), 1000, 1000, 0)
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}

	// Start periodic flush
	server.StartPeriodicFlush(ctx, 1*time.Hour)

	// Inject backend failure
	backend.failPut.Store(true)

	// Close must return error so controller exits non-zero on failed final flush
	if err := server.Close(); err == nil {
		t.Fatal("expected server.Close() to fail when final flush fails, got nil")
	}
}
