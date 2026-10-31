// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controller

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/blob"
	"github.com/gke-labs/in-cluster-storage/pkg/wal/buffer/buffertest"
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type recordingBackend struct {
	ObjectStorageBackend
	mu       sync.Mutex
	packKeys []string
	failPut  bool
}

func newRecordingBackend(base ObjectStorageBackend) *recordingBackend {
	return &recordingBackend{
		ObjectStorageBackend: base,
	}
}

func (b *recordingBackend) PutObject(ctx context.Context, volumeID, key string, stream blob.ByteStream) (string, error) {
	b.mu.Lock()
	if b.failPut {
		b.mu.Unlock()
		return "", fmt.Errorf("injected PutObject failure")
	}
	if strings.HasSuffix(key, ".pack") || strings.Contains(key, "blobs/") {
		b.packKeys = append(b.packKeys, key)
	}
	b.mu.Unlock()
	return b.ObjectStorageBackend.PutObject(ctx, volumeID, key, stream)
}

func (b *recordingBackend) getPackCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.packKeys)
}

// TestStageAndSync_SinglePackMultiInode verifies that multiple writes across several inodes
// stay staged in memory without uploading any packs until Fsync, and that one consolidated
// packfile is uploaded containing all chunks across all inodes.
func TestStageAndSync_SinglePackMultiInode(t *testing.T) {
	ctx := t.Context()
	rawBackend := NewMemoryBackend()
	backend := newRecordingBackend(rawBackend)
	server := NewTestServer(backend)
	volumeID := "test-single-pack-vol"
	defer func() { _ = server.Close() }()

	const numFiles = 5
	const chunksPerFile = 4
	const chunkSize = 64 * 1024

	var fileInos []uint64
	fileData := make(map[uint64][]byte)

	for i := 0; i < numFiles; i++ {
		name := fmt.Sprintf("/file_%d.bin", i)
		createResp, err := testCreateFile(ctx, server, volumeID, name, 0644, nil, 0, 0)
		if err != nil || createResp.GetError() != 0 {
			t.Fatalf("CreateFile %s failed: %v", name, err)
		}
		ino := createResp.GetAttr().GetInode().GetIno()
		fileInos = append(fileInos, ino)

		data := make([]byte, chunksPerFile*chunkSize)
		for j := range data {
			data[j] = byte((i*chunksPerFile + j) % 251)
		}
		fileData[ino] = data

		wResp, err := server.WriteFile(ctx, &pb.WriteFileRequest{
			VolumeId:  volumeID,
			Inode:     ino,
			Offset:    0,
			Data:      data,
			WriteMode: pb.WriteMode_LAZY_WRITE,
		})
		if err != nil || wResp.GetError() != 0 {
			t.Fatalf("WriteFile %s failed: %v", name, err)
		}
	}

	// Before Fsync: exactly 0 packs must be uploaded
	if cnt := backend.getPackCount(); cnt != 0 {
		t.Fatalf("Expected 0 packs before Fsync, got %d", cnt)
	}

	// Before Fsync: ReadFile on server sees pre-write content (0 bytes)
	for _, ino := range fileInos {
		rResp, err := server.ReadFile(ctx, &pb.ReadFileRequest{
			VolumeId: volumeID,
			Inode:    ino,
			Offset:   0,
			Size:     1024,
		})
		if err != nil {
			t.Fatalf("ReadFile before Fsync failed: %v", err)
		}
		if len(rResp.GetData()) != 0 {
			t.Fatalf("Expected 0 bytes before Fsync, got %d", len(rResp.GetData()))
		}
	}

	// Trigger single Fsync on root inode
	fsyncResp, err := server.Fsync(ctx, &pb.FsyncRequest{
		VolumeId: volumeID,
		Inode:    1,
		Blocking: true,
	})
	if err != nil || !fsyncResp.GetSuccess() {
		t.Fatalf("Fsync failed: %v", err)
	}

	// After Fsync: exactly 1 packfile must be created for all chunks across all files
	if cnt := backend.getPackCount(); cnt != 1 {
		t.Fatalf("Expected exactly 1 pack created on Fsync, got %d", cnt)
	}

	// After Fsync: ReadFile on server returns the written data for all files
	for _, ino := range fileInos {
		expected := fileData[ino]
		rResp, err := server.ReadFile(ctx, &pb.ReadFileRequest{
			VolumeId: volumeID,
			Inode:    ino,
			Offset:   0,
			Size:     int64(len(expected)),
		})
		if err != nil {
			t.Fatalf("ReadFile after Fsync failed: %v", err)
		}
		if !bytes.Equal(rResp.GetData(), expected) {
			t.Fatalf("Data mismatch for inode %d after Fsync", ino)
		}
	}
}

// TestStageAndSync_CrossNodeVisibility verifies that a second client sees pre-write content
// until Fsync, and immediately sees the new content after Fsync.
func TestStageAndSync_CrossNodeVisibility(t *testing.T) {
	ctx := t.Context()
	server := NewTestServer(NewMemoryBackend())
	volumeID := "test-cross-node-vol"
	defer func() { _ = server.Close() }()

	initialContent := []byte("hello-initial-content")
	createResp, err := testCreateFile(ctx, server, volumeID, "/shared.txt", 0644, initialContent, 0, 0)
	if err != nil || createResp.GetError() != 0 {
		t.Fatalf("CreateFile failed: %v", err)
	}
	ino := createResp.GetAttr().GetInode().GetIno()

	// Client A writes new data (stages on controller)
	updatedContent := []byte("updated-by-client-a-content-here")
	wResp, err := server.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId:  volumeID,
		Inode:     ino,
		Offset:    0,
		Data:      updatedContent,
		WriteMode: pb.WriteMode_LAZY_WRITE,
	})
	if err != nil || wResp.GetError() != 0 {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Client B reads from controller before Fsync: must see initial content and old size
	rResp1, err := server.ReadFile(ctx, &pb.ReadFileRequest{
		VolumeId: volumeID,
		Inode:    ino,
		Offset:   0,
		Size:     1024,
	})
	if err != nil {
		t.Fatalf("ReadFile before Fsync failed: %v", err)
	}
	if !bytes.Equal(rResp1.GetData(), initialContent) {
		t.Fatalf("Client B before Fsync got %q, want %q", string(rResp1.GetData()), string(initialContent))
	}

	// Client A calls Fsync
	fResp, err := server.Fsync(ctx, &pb.FsyncRequest{
		VolumeId: volumeID,
		Inode:    ino,
		Blocking: true,
	})
	if err != nil || !fResp.GetSuccess() {
		t.Fatalf("Fsync failed: %v", err)
	}

	// Client B reads from controller after Fsync: must see updated content and new size
	rResp2, err := server.ReadFile(ctx, &pb.ReadFileRequest{
		VolumeId: volumeID,
		Inode:    ino,
		Offset:   0,
		Size:     1024,
	})
	if err != nil {
		t.Fatalf("ReadFile after Fsync failed: %v", err)
	}
	if !bytes.Equal(rResp2.GetData(), updatedContent) {
		t.Fatalf("Client B after Fsync got %q, want %q", string(rResp2.GetData()), string(updatedContent))
	}
}

// TestStageAndSync_ControllerCrashBeforeSyncRePush verifies that un-synced writes
// are dropped on crash, Fsync with old epoch fails with ESTALE, and client re-push
// succeeds in making data durable.
func TestStageAndSync_ControllerCrashBeforeSyncRePush(t *testing.T) {
	ctx := t.Context()
	bufDir := t.TempDir()
	_, target, cleanup := startTestWalBufferServer(t, bufDir)
	defer cleanup()

	backend := NewMemoryBackend()
	volumeID := "test-crash-repush-vol"

	// Initial session
	clientDir1 := t.TempDir()
	server1 := NewTestServer(backend, WithServerWAL(clientDir1, target, walclient.Witness))
	initialData := []byte("persisted-initial-data")
	cResp, err := testCreateFile(ctx, server1, volumeID, "/file.txt", 0644, initialData, 0, 0)
	if err != nil || cResp.GetError() != 0 {
		t.Fatalf("CreateFile failed: %v", err)
	}
	ino := cResp.GetAttr().GetInode().GetIno()

	// Write staged data
	stagedData := []byte("new-staged-data-not-synced")
	wResp1, err := server1.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId:  volumeID,
		Inode:     ino,
		Offset:    0,
		Data:      stagedData,
		WriteMode: pb.WriteMode_LAZY_WRITE,
	})
	if err != nil || wResp1.GetError() != 0 {
		t.Fatalf("WriteFile failed: %v", err)
	}
	oldEpoch := wResp1.GetStagingEpoch()

	// Simulate crash of server1 (close without fsync)
	_ = server1.Close()

	// Server 2 starts up (new controller instance with same backend & wal-buffer)
	clientDir2 := t.TempDir()
	server2 := NewTestServer(backend, WithServerWAL(clientDir2, target, walclient.Witness))
	defer func() { _ = server2.Close() }()

	// Client calls Fsync using oldEpoch: must fail with ESTALE
	fsyncResp1, err := server2.Fsync(ctx, &pb.FsyncRequest{
		VolumeId:     volumeID,
		Inode:        ino,
		Blocking:     true,
		StagingEpoch: oldEpoch,
	})
	if err != nil {
		t.Fatalf("Fsync RPC error: %v", err)
	}
	if fsyncResp1.GetError() != int32(syscall.ESTALE) {
		t.Fatalf("Expected ESTALE error (%d), got %d", syscall.ESTALE, fsyncResp1.GetError())
	}

	// Client re-pushes still-dirty data
	wResp2, err := server2.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId:  volumeID,
		Inode:     ino,
		Offset:    0,
		Data:      stagedData,
		WriteMode: pb.WriteMode_LAZY_WRITE,
	})
	if err != nil || wResp2.GetError() != 0 {
		t.Fatalf("WriteFile re-push failed: %v", err)
	}
	newEpoch := wResp2.GetStagingEpoch()

	// Client calls Fsync with new epoch: must succeed
	fsyncResp2, err := server2.Fsync(ctx, &pb.FsyncRequest{
		VolumeId:     volumeID,
		Inode:        ino,
		Blocking:     true,
		StagingEpoch: newEpoch,
	})
	if err != nil || !fsyncResp2.GetSuccess() {
		t.Fatalf("Fsync retry failed: %v, resp=%v", err, fsyncResp2)
	}

	// File reads back the re-pushed content
	readResp, err := server2.ReadFile(ctx, &pb.ReadFileRequest{
		VolumeId: volumeID,
		Inode:    ino,
		Offset:   0,
		Size:     1024,
	})
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if !bytes.Equal(readResp.GetData(), stagedData) {
		t.Fatalf("Read data mismatch: got %q, want %q", string(readResp.GetData()), string(stagedData))
	}
}

// TestStageAndSync_TruncateComposition verifies write-then-truncate-down and
// truncate-then-write composition with staging.
func TestStageAndSync_TruncateComposition(t *testing.T) {
	ctx := t.Context()
	server := NewTestServer(NewMemoryBackend())
	volumeID := "test-trunc-comp-vol"
	defer func() { _ = server.Close() }()

	const cs = 64 * 1024

	// 1. Write-then-truncate-down
	cResp1, err := testCreateFile(ctx, server, volumeID, "/w_then_t.bin", 0644, nil, 0, 0)
	if err != nil || cResp1.GetError() != 0 {
		t.Fatalf("CreateFile 1 failed: %v", err)
	}
	ino1 := cResp1.GetAttr().GetInode().GetIno()

	// Write 192 KiB (3 chunks)
	data192k := bytes.Repeat([]byte("A"), 3*cs)
	wResp1, err := server.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId: volumeID,
		Inode:    ino1,
		Offset:   0,
		Data:     data192k,
	})
	if err != nil || wResp1.GetError() != 0 {
		t.Fatalf("WriteFile 1 failed: %v", err)
	}

	// Truncate down to 50 KiB (inside chunk 0, chunks 1 & 2 dropped)
	tResp1, err := server.TruncateFile(ctx, &pb.TruncateFileRequest{
		VolumeId: volumeID,
		Inode:    ino1,
		Size:     50 * 1024,
	})
	if err != nil || tResp1.GetError() != 0 {
		t.Fatalf("TruncateFile 1 failed: %v", err)
	}

	// Sync
	fResp1, err := server.Fsync(ctx, &pb.FsyncRequest{
		VolumeId: volumeID,
		Inode:    ino1,
		Blocking: true,
	})
	if err != nil || !fResp1.GetSuccess() {
		t.Fatalf("Fsync 1 failed: %v", err)
	}

	// Read back: exactly 50 KiB of "A"
	rResp1, err := server.ReadFile(ctx, &pb.ReadFileRequest{
		VolumeId: volumeID,
		Inode:    ino1,
		Offset:   0,
		Size:     100 * 1024,
	})
	if err != nil {
		t.Fatalf("ReadFile 1 failed: %v", err)
	}
	if len(rResp1.GetData()) != 50*1024 {
		t.Fatalf("Expected 50 KiB, got %d", len(rResp1.GetData()))
	}
	if !bytes.Equal(rResp1.GetData(), data192k[:50*1024]) {
		t.Fatalf("Content mismatch for write-then-truncate-down")
	}

	// 2. Truncate-then-write
	cResp2, err := testCreateFile(ctx, server, volumeID, "/t_then_w.bin", 0644, bytes.Repeat([]byte("Z"), 128*1024), 0, 0)
	if err != nil || cResp2.GetError() != 0 {
		t.Fatalf("CreateFile 2 failed: %v", err)
	}
	ino2 := cResp2.GetAttr().GetInode().GetIno()

	// Truncate to 0
	tResp2, err := server.TruncateFile(ctx, &pb.TruncateFileRequest{
		VolumeId: volumeID,
		Inode:    ino2,
		Size:     0,
	})
	if err != nil || tResp2.GetError() != 0 {
		t.Fatalf("TruncateFile 2 failed: %v", err)
	}

	// Write 64 KiB of "B"
	data64k := bytes.Repeat([]byte("B"), cs)
	wResp2, err := server.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId: volumeID,
		Inode:    ino2,
		Offset:   0,
		Data:     data64k,
	})
	if err != nil || wResp2.GetError() != 0 {
		t.Fatalf("WriteFile 2 failed: %v", err)
	}

	// Sync
	fResp2, err := server.Fsync(ctx, &pb.FsyncRequest{
		VolumeId: volumeID,
		Inode:    ino2,
		Blocking: true,
	})
	if err != nil || !fResp2.GetSuccess() {
		t.Fatalf("Fsync 2 failed: %v", err)
	}

	// Read back: exactly 64 KiB of "B"
	rResp2, err := server.ReadFile(ctx, &pb.ReadFileRequest{
		VolumeId: volumeID,
		Inode:    ino2,
		Offset:   0,
		Size:     100 * 1024,
	})
	if err != nil {
		t.Fatalf("ReadFile 2 failed: %v", err)
	}
	if len(rResp2.GetData()) != cs {
		t.Fatalf("Expected 64 KiB, got %d", len(rResp2.GetData()))
	}
	if !bytes.Equal(rResp2.GetData(), data64k) {
		t.Fatalf("Content mismatch for truncate-then-write")
	}
}

// TestStageAndSync_CoalesceWindowBurst verifies that a burst of N non-blocking syncs
// within the coalesce window coalesces into exactly 1 packfile upload.
func TestStageAndSync_CoalesceWindowBurst(t *testing.T) {
	ctx := t.Context()
	rawBackend := NewMemoryBackend()
	backend := newRecordingBackend(rawBackend)
	server := NewTestServer(backend, WithServerSyncCoalesceWindow(200*time.Millisecond))
	volumeID := "test-coalesce-burst-vol"
	defer func() { _ = server.Close() }()

	const numFiles = 10
	var wg sync.WaitGroup

	for i := 0; i < numFiles; i++ {
		name := fmt.Sprintf("/burst_%d.bin", i)
		cResp, err := testCreateFile(ctx, server, volumeID, name, 0644, nil, 0, 0)
		if err != nil || cResp.GetError() != 0 {
			t.Fatalf("CreateFile %s failed: %v", name, err)
		}
		ino := cResp.GetAttr().GetInode().GetIno()

		data := bytes.Repeat([]byte{byte(i + 1)}, 64*1024)
		wResp, err := server.WriteFile(ctx, &pb.WriteFileRequest{
			VolumeId: volumeID,
			Inode:    ino,
			Offset:   0,
			Data:     data,
		})
		if err != nil || wResp.GetError() != 0 {
			t.Fatalf("WriteFile %s failed: %v", name, err)
		}

		// Schedule non-blocking syncs concurrently
		wg.Add(1)
		go func(fileIno uint64) {
			defer wg.Done()
			syncResp, sErr := server.Fsync(ctx, &pb.FsyncRequest{
				VolumeId: volumeID,
				Inode:    fileIno,
				Blocking: false,
			})
			if sErr != nil || !syncResp.GetSuccess() {
				t.Errorf("Non-blocking Fsync failed: %v, resp=%v", sErr, syncResp)
			}
		}(ino)
	}

	wg.Wait()

	// Exactly 1 packfile must have been uploaded for the entire burst
	if cnt := backend.getPackCount(); cnt != 1 {
		t.Fatalf("Expected exactly 1 packfile for burst within coalesce window, got %d", cnt)
	}
}

// TestStageAndSync_BlockingFsyncPreemptsCoalesceWindow verifies that a blocking Fsync
// arriving during an active coalesce window immediately flushes all pending writes.
func TestStageAndSync_BlockingFsyncPreemptsCoalesceWindow(t *testing.T) {
	ctx := t.Context()
	rawBackend := NewMemoryBackend()
	backend := newRecordingBackend(rawBackend)
	// Long coalesce window (5 seconds)
	server := NewTestServer(backend, WithServerSyncCoalesceWindow(5*time.Second))
	volumeID := "test-preempt-window-vol"
	defer func() { _ = server.Close() }()

	// File 1 writes and issues non-blocking sync
	cResp1, _ := testCreateFile(ctx, server, volumeID, "/file1.bin", 0644, nil, 0, 0)
	ino1 := cResp1.GetAttr().GetInode().GetIno()
	_, _ = server.WriteFile(ctx, &pb.WriteFileRequest{VolumeId: volumeID, Inode: ino1, Offset: 0, Data: bytes.Repeat([]byte("1"), 64*1024)})

	nonBlockingDone := make(chan struct{})
	go func() {
		defer close(nonBlockingDone)
		_, _ = server.Fsync(ctx, &pb.FsyncRequest{VolumeId: volumeID, Inode: ino1, Blocking: false})
	}()

	time.Sleep(50 * time.Millisecond)
	if backend.getPackCount() != 0 {
		t.Fatalf("Packs uploaded too early before window flush")
	}

	// File 2 writes and issues blocking sync
	cResp2, _ := testCreateFile(ctx, server, volumeID, "/file2.bin", 0644, nil, 0, 0)
	ino2 := cResp2.GetAttr().GetInode().GetIno()
	_, _ = server.WriteFile(ctx, &pb.WriteFileRequest{VolumeId: volumeID, Inode: ino2, Offset: 0, Data: bytes.Repeat([]byte("2"), 64*1024)})

	start := time.Now()
	bResp, err := server.Fsync(ctx, &pb.FsyncRequest{VolumeId: volumeID, Inode: ino2, Blocking: true})
	elapsed := time.Since(start)

	if err != nil || !bResp.GetSuccess() {
		t.Fatalf("Blocking Fsync failed: %v", err)
	}
	// Should have flushed well before the 5s coalesce window
	if elapsed >= 3*time.Second {
		t.Fatalf("Blocking Fsync waited for coalesce window instead of preempting: took %v", elapsed)
	}

	// Non-blocking sync should also have unblocked
	select {
	case <-nonBlockingDone:
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("Non-blocking waiter was not notified by preempting sync")
	}

	// Exactly 1 packfile created containing writes from both files
	if cnt := backend.getPackCount(); cnt != 1 {
		t.Fatalf("Expected exactly 1 packfile containing both writes, got %d", cnt)
	}
}

// TestStageAndSync_CloseTriggerStartsCoalescedSyncImmediately verifies that a
// CLOSE-triggered non-blocking Fsync with staged chunks commits without waiting
// for the coalesce window, while a PERIODIC non-blocking Fsync still waits.
func TestStageAndSync_CloseTriggerStartsCoalescedSyncImmediately(t *testing.T) {
	ctx := t.Context()
	rawBackend := NewMemoryBackend()
	backend := newRecordingBackend(rawBackend)
	// 10s coalesce window so any difference is unambiguous
	server := NewTestServer(backend, WithServerSyncCoalesceWindow(10*time.Second))
	defer func() { _ = server.Close() }()

	// 1. Periodic non-blocking Fsync: must wait for the coalesce window
	volPeriodic := "vol-periodic-wait"
	cResp1, _ := testCreateFile(ctx, server, volPeriodic, "/file1.bin", 0644, nil, 0, 0)
	ino1 := cResp1.GetAttr().GetInode().GetIno()
	_, _ = server.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId: volPeriodic,
		Inode:    ino1,
		Offset:   0,
		Data:     bytes.Repeat([]byte("1"), 64*1024),
	})

	periodicDone := make(chan struct{})
	go func() {
		defer close(periodicDone)
		_, _ = server.Fsync(ctx, &pb.FsyncRequest{
			VolumeId: volPeriodic,
			Inode:    ino1,
			Blocking: false,
			Trigger:  pb.FsyncRequest_SYNC_TRIGGER_PERIODIC,
		})
	}()

	time.Sleep(100 * time.Millisecond)
	select {
	case <-periodicDone:
		t.Fatalf("Periodic non-blocking Fsync finished prematurely before coalesce window")
	default:
	}
	if backend.getPackCount() != 0 {
		t.Fatalf("Periodic non-blocking Fsync uploaded packs before window fired")
	}

	// 2. Close-triggered non-blocking Fsync: commits immediately without waiting for the window
	volClose := "vol-close-immediate"
	cResp2, _ := testCreateFile(ctx, server, volClose, "/file2.bin", 0644, nil, 0, 0)
	ino2 := cResp2.GetAttr().GetInode().GetIno()
	_, _ = server.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId: volClose,
		Inode:    ino2,
		Offset:   0,
		Data:     bytes.Repeat([]byte("2"), 64*1024),
	})

	start := time.Now()
	resp2, err := server.Fsync(ctx, &pb.FsyncRequest{
		VolumeId: volClose,
		Inode:    ino2,
		Blocking: false,
		Trigger:  pb.FsyncRequest_SYNC_TRIGGER_CLOSE,
	})
	elapsed := time.Since(start)

	if err != nil || !resp2.GetSuccess() {
		t.Fatalf("Close-triggered Fsync failed: %v, resp=%v", err, resp2)
	}
	if elapsed >= 3*time.Second {
		t.Fatalf("Close-triggered Fsync waited for coalesce window instead of committing immediately: took %v", elapsed)
	}
	if backend.getPackCount() != 1 {
		t.Fatalf("Expected 1 packfile for close-triggered sync, got %d", backend.getPackCount())
	}
}

// TestStageAndSync_CloseTriggerPreemptsActiveWindow verifies that multiple CLOSE-triggered
// non-blocking Fsyncs arriving during the short close coalesce floor window share a single
// packfile and unblock promptly.
func TestStageAndSync_CloseTriggerPreemptsActiveWindow(t *testing.T) {
	ctx := t.Context()
	rawBackend := NewMemoryBackend()
	backend := newRecordingBackend(rawBackend)
	// 10s coalesce window so any difference is unambiguous
	server := NewTestServer(backend, WithServerSyncCoalesceWindow(10*time.Second))
	volumeID := "test-close-preempt-window-vol"
	defer func() { _ = server.Close() }()

	// File 1 writes and issues non-blocking CLOSE sync (starts floor window)
	cResp1, _ := testCreateFile(ctx, server, volumeID, "/file1.bin", 0644, nil, 0, 0)
	ino1 := cResp1.GetAttr().GetInode().GetIno()
	_, _ = server.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId: volumeID,
		Inode:    ino1,
		Offset:   0,
		Data:     bytes.Repeat([]byte("1"), 64*1024),
	})

	closeDone1 := make(chan struct{})
	go func() {
		defer close(closeDone1)
		_, _ = server.Fsync(ctx, &pb.FsyncRequest{
			VolumeId: volumeID,
			Inode:    ino1,
			Blocking: false,
			Trigger:  pb.FsyncRequest_SYNC_TRIGGER_CLOSE,
		})
	}()

	time.Sleep(20 * time.Millisecond)
	if backend.getPackCount() != 0 {
		t.Fatalf("Packs uploaded too early before window flush")
	}

	// File 2 writes and issues non-blocking CLOSE-triggered sync within floor window
	cResp2, _ := testCreateFile(ctx, server, volumeID, "/file2.bin", 0644, nil, 0, 0)
	ino2 := cResp2.GetAttr().GetInode().GetIno()
	_, _ = server.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId: volumeID,
		Inode:    ino2,
		Offset:   0,
		Data:     bytes.Repeat([]byte("2"), 64*1024),
	})

	start := time.Now()
	closeResp, err := server.Fsync(ctx, &pb.FsyncRequest{
		VolumeId: volumeID,
		Inode:    ino2,
		Blocking: false,
		Trigger:  pb.FsyncRequest_SYNC_TRIGGER_CLOSE,
	})
	elapsed := time.Since(start)

	if err != nil || !closeResp.GetSuccess() {
		t.Fatalf("Close-triggered non-blocking Fsync failed: %v", err)
	}
	if elapsed >= 3*time.Second {
		t.Fatalf("Close-triggered Fsync waited for coalesce window instead of preempting: took %v", elapsed)
	}

	// File 1's close waiter should also have unblocked promptly
	select {
	case <-closeDone1:
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("Close-triggered waiter 1 was not notified by coalesced close sync")
	}

	// Exactly 1 packfile created containing writes from both files
	if cnt := backend.getPackCount(); cnt != 1 {
		t.Fatalf("Expected exactly 1 packfile containing both writes, got %d", cnt)
	}
}

// TestStageAndSync_TenConcurrentCloseRequestsCoalesceIntoOnePack verifies that ten
// CLOSE-triggered non-blocking Fsync requests in flight concurrently coalesce into a single packfile.
func TestStageAndSync_TenConcurrentCloseRequestsCoalesceIntoOnePack(t *testing.T) {
	ctx := t.Context()
	rawBackend := NewMemoryBackend()
	backend := newRecordingBackend(rawBackend)
	server := NewTestServer(backend, WithServerSyncCoalesceWindow(10*time.Second))
	volumeID := "test-ten-concurrent-close-vol"
	defer func() { _ = server.Close() }()

	const numFiles = 10
	inodes := make([]uint64, numFiles)
	for i := 0; i < numFiles; i++ {
		fname := fmt.Sprintf("/c%d.bin", i)
		cResp, err := testCreateFile(ctx, server, volumeID, fname, 0644, nil, 0, 0)
		if err != nil {
			t.Fatalf("CreateFile %d failed: %v", i, err)
		}
		ino := cResp.GetAttr().GetInode().GetIno()
		inodes[i] = ino
		_, err = server.WriteFile(ctx, &pb.WriteFileRequest{
			VolumeId: volumeID,
			Inode:    ino,
			Offset:   0,
			Data:     bytes.Repeat([]byte{byte(i + 1)}, 64*1024),
		})
		if err != nil {
			t.Fatalf("WriteFile %d failed: %v", i, err)
		}
	}

	startBarrier := make(chan struct{})
	g, gctx := errgroup.WithContext(ctx)
	for _, ino := range inodes {
		ino := ino
		g.Go(func() error {
			<-startBarrier
			resp, err := server.Fsync(gctx, &pb.FsyncRequest{
				VolumeId: volumeID,
				Inode:    ino,
				Blocking: false,
				Trigger:  pb.FsyncRequest_SYNC_TRIGGER_CLOSE,
			})
			if err != nil {
				return err
			}
			if !resp.GetSuccess() {
				return fmt.Errorf("Fsync for inode %d returned success=false", ino)
			}
			return nil
		})
	}

	close(startBarrier)
	if err := g.Wait(); err != nil {
		t.Fatalf("Concurrent close-triggered Fsyncs failed: %v", err)
	}

	if cnt := backend.getPackCount(); cnt != 1 {
		t.Fatalf("Expected exactly 1 packfile for ten concurrent close requests, got %d", cnt)
	}
}

// TestStageAndSync_CloseTriggerSyncsOnlyClosedInodeLeavesStagedWindow verifies that a
// CLOSE-triggered sync with inode A staged alongside a large inode B commits A,
// leaves B staged (with B's waiter not completed), and that the following window
// sync commits B and completes B's waiter.
func TestStageAndSync_CloseTriggerSyncsOnlyClosedInodeLeavesStagedWindow(t *testing.T) {
	ctx := t.Context()
	rawBackend := NewMemoryBackend()
	backend := newRecordingBackend(rawBackend)
	// 500ms coalesce window so the periodic window sync fires without a long delay
	server := NewTestServer(backend, WithServerSyncCoalesceWindow(500*time.Millisecond))
	volumeID := "test-close-per-inode-vol"
	defer func() { _ = server.Close() }()

	// 1. Inode B stages a large write (256 KiB) and issues non-blocking periodic Fsync
	cRespB, _ := testCreateFile(ctx, server, volumeID, "/fileB.bin", 0644, nil, 0, 0)
	inoB := cRespB.GetAttr().GetInode().GetIno()
	dataB := bytes.Repeat([]byte("B"), 256*1024)
	_, _ = server.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId: volumeID,
		Inode:    inoB,
		Offset:   0,
		Data:     dataB,
	})

	waiterBDone := make(chan struct{})
	go func() {
		defer close(waiterBDone)
		_, _ = server.Fsync(ctx, &pb.FsyncRequest{
			VolumeId: volumeID,
			Inode:    inoB,
			Blocking: false,
			Trigger:  pb.FsyncRequest_SYNC_TRIGGER_PERIODIC,
		})
	}()

	time.Sleep(30 * time.Millisecond)
	if backend.getPackCount() != 0 {
		t.Fatalf("Packs uploaded before any sync fired")
	}

	// 2. Inode A stages a small write (64 KiB) and issues CLOSE-triggered Fsync
	cRespA, _ := testCreateFile(ctx, server, volumeID, "/fileA.bin", 0644, nil, 0, 0)
	inoA := cRespA.GetAttr().GetInode().GetIno()
	dataA := bytes.Repeat([]byte("A"), 64*1024)
	_, _ = server.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId: volumeID,
		Inode:    inoA,
		Offset:   0,
		Data:     dataA,
	})

	start := time.Now()
	respA, err := server.Fsync(ctx, &pb.FsyncRequest{
		VolumeId: volumeID,
		Inode:    inoA,
		Blocking: false,
		Trigger:  pb.FsyncRequest_SYNC_TRIGGER_CLOSE,
	})
	elapsed := time.Since(start)

	if err != nil || !respA.GetSuccess() {
		t.Fatalf("Close-triggered Fsync for A failed: %v, resp=%v", err, respA)
	}
	if elapsed >= 400*time.Millisecond {
		t.Fatalf("Close-triggered Fsync took too long: %v", elapsed)
	}

	// Inode A must be committed in exactly 1 packfile
	if cnt := backend.getPackCount(); cnt != 1 {
		t.Fatalf("Expected exactly 1 packfile after A's close sync, got %d", cnt)
	}

	// Inode A is durable on backend; Inode B is NOT yet durable on backend
	rRespA, _ := server.ReadFile(ctx, &pb.ReadFileRequest{
		VolumeId: volumeID,
		Inode:    inoA,
		Offset:   0,
		Size:     int64(len(dataA)),
	})
	if !bytes.Equal(rRespA.GetData(), dataA) {
		t.Fatalf("ReadFile A on server did not match written data")
	}

	// Inode B's periodic waiter must NOT have completed yet
	select {
	case <-waiterBDone:
		t.Fatalf("Waiter for inode B completed prematurely on inode A's close sync")
	default:
	}

	// 3. Now wait for the coalesce window to fire for Inode B
	select {
	case <-waiterBDone:
	case <-time.After(2 * time.Second):
		t.Fatalf("Waiter for inode B did not complete on following coalesce window")
	}

	// Now exactly 2 packfiles must exist: one for A and one for B
	if cnt := backend.getPackCount(); cnt != 2 {
		t.Fatalf("Expected exactly 2 packfiles after window sync (1 for A, 1 for B), got %d", cnt)
	}

	// Inode B is now durable on backend
	rRespB, _ := server.ReadFile(ctx, &pb.ReadFileRequest{
		VolumeId: volumeID,
		Inode:    inoB,
		Offset:   0,
		Size:     int64(len(dataB)),
	})
	if !bytes.Equal(rRespB.GetData(), dataB) {
		t.Fatalf("ReadFile B on server did not match written data after window sync")
	}
}

// TestStageAndSync_NonBlockingSyncFailureSurfacesOnBlockingFsync verifies that failed
// non-blocking syncs are remembered and returned on the next blocking Fsync.
func TestStageAndSync_NonBlockingSyncFailureSurfacesOnBlockingFsync(t *testing.T) {
	ctx := t.Context()
	rawBackend := NewMemoryBackend()
	backend := newRecordingBackend(rawBackend)
	server := NewTestServer(backend, WithServerSyncCoalesceWindow(50*time.Millisecond))
	volumeID := "test-error-surface-vol"
	defer func() { _ = server.Close() }()

	cResp, _ := testCreateFile(ctx, server, volumeID, "/err_file.bin", 0644, nil, 0, 0)
	ino := cResp.GetAttr().GetInode().GetIno()

	_, _ = server.WriteFile(ctx, &pb.WriteFileRequest{VolumeId: volumeID, Inode: ino, Offset: 0, Data: bytes.Repeat([]byte("E"), 64*1024)})

	// Induce backend failure
	backend.mu.Lock()
	backend.failPut = true
	backend.mu.Unlock()

	// Non-blocking sync fails
	nbResp, _ := server.Fsync(ctx, &pb.FsyncRequest{VolumeId: volumeID, Inode: ino, Blocking: false})
	if nbResp.GetError() == 0 {
		t.Fatalf("Expected non-blocking Fsync to report error on failure")
	}

	// Next blocking Fsync must surface the failure
	bResp, _ := server.Fsync(ctx, &pb.FsyncRequest{VolumeId: volumeID, Inode: ino, Blocking: true})
	if bResp.GetError() == 0 {
		t.Fatalf("Expected blocking Fsync to surface previous failure")
	}

	// Clear fault and retry blocking Fsync: must succeed
	backend.mu.Lock()
	backend.failPut = false
	backend.mu.Unlock()

	retryResp, err := server.Fsync(ctx, &pb.FsyncRequest{VolumeId: volumeID, Inode: ino, Blocking: true})
	if err != nil || !retryResp.GetSuccess() {
		t.Fatalf("Retry Fsync failed: %v, resp=%v", err, retryResp)
	}
}

// TestStageAndSync_MaxStagingBytesEnforced verifies that the controller's
// in-memory staging cap rejects writes when exceeded.
func TestStageAndSync_MaxStagingBytesEnforced(t *testing.T) {
	ctx := t.Context()
	server := NewTestServer(NewMemoryBackend(), WithServerMaxStagingBytes(128*1024))
	volumeID := "test-staging-cap-vol"
	defer func() { _ = server.Close() }()

	cResp, _ := testCreateFile(ctx, server, volumeID, "/cap.bin", 0644, nil, 0, 0)
	ino := cResp.GetAttr().GetInode().GetIno()

	// Write 128 KiB: fills staging buffer
	w1, err := server.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId: volumeID,
		Inode:    ino,
		Offset:   0,
		Data:     make([]byte, 128*1024),
	})
	if err != nil || w1.GetError() != 0 {
		t.Fatalf("First write failed: %v", err)
	}

	// Next write of 1 byte: must be rejected with ENOSPC
	w2, err := server.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId: volumeID,
		Inode:    ino,
		Offset:   128 * 1024,
		Data:     []byte("X"),
	})
	if err != nil {
		t.Fatalf("Expected gRPC success with response error, got %v", err)
	}
	if w2.GetError() != int32(syscall.ENOSPC) {
		t.Fatalf("Expected ENOSPC (%d), got error %d", syscall.ENOSPC, w2.GetError())
	}

	// Fsync flushes staging
	fResp, err := server.Fsync(ctx, &pb.FsyncRequest{VolumeId: volumeID, Inode: ino, Blocking: true})
	if err != nil || !fResp.GetSuccess() {
		t.Fatalf("Fsync failed: %v", err)
	}

	// Subsequent write succeeds
	w3, err := server.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId: volumeID,
		Inode:    ino,
		Offset:   128 * 1024,
		Data:     []byte("X"),
	})
	if err != nil || w3.GetError() != 0 {
		t.Fatalf("Write after Fsync failed: %v, resp=%v", err, w3)
	}
}

// TestStageAndSync_EmptyCoalesceTimerFireDoesNotDeadlock reproduces the pjdfstest churn scenario:
// create, stage write, arm coalesce timer on release, unlink drops staged data, timer fires on empty stagedChunks.
// Ensures executeSyncLocked leaves stagingMu unlocked so subsequent operations never deadlock.
func TestStageAndSync_EmptyCoalesceTimerFireDoesNotDeadlock(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	s := NewTestServer(NewMemoryBackend(), WithServerSyncCoalesceWindow(50*time.Millisecond))
	vol := "test-empty-coalesce-vol"
	defer func() { _ = s.Close() }()

	cResp, err := testCreateFile(ctx, s, vol, "/tmpfile", 0644, nil, 0, 0)
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}
	ino := cResp.GetAttr().GetInode().GetIno()

	_, err = s.WriteFile(ctx, &pb.WriteFileRequest{VolumeId: vol, Inode: ino, Offset: 0, Data: []byte("x")})
	if err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	go func() {
		_, _ = s.Fsync(ctx, &pb.FsyncRequest{VolumeId: vol, Inode: 1, Blocking: false}) // arms the timer
	}()

	time.Sleep(10 * time.Millisecond)
	_, err = testUnlink(ctx, s, vol, "/tmpfile") // drops staged data
	if err != nil {
		t.Fatalf("Unlink failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond) // timer fires with empty stagedChunks

	// Subsequent operations must succeed promptly without blocking on stagingMu
	cResp2, err := testCreateFile(ctx, s, vol, "/after", 0644, nil, 0, 0)
	if err != nil {
		t.Fatalf("CreateFile /after failed: %v", err)
	}
	_, err = s.WriteFile(ctx, &pb.WriteFileRequest{VolumeId: vol, Inode: cResp2.GetAttr().GetInode().GetIno(), Offset: 0, Data: []byte("hello")})
	if err != nil {
		t.Fatalf("WriteFile /after failed: %v", err)
	}
}

type gatedBackend struct {
	ObjectStorageBackend
	mu       sync.Mutex
	gate     chan struct{}
	putCalls int
}

func (b *gatedBackend) PutObject(ctx context.Context, volumeID, key string, stream blob.ByteStream) (string, error) {
	b.mu.Lock()
	b.putCalls++
	callNum := b.putCalls
	b.mu.Unlock()

	if callNum == 1 {
		<-b.gate
	}
	return b.ObjectStorageBackend.PutObject(ctx, volumeID, key, stream)
}

// TestStageAndSync_NonBlockingSyncWaiterNotAcknowledgedByEarlierSync verifies that
// a non-blocking sync waiter registered while a previous sync is in flight is NOT
// acknowledged until its own data has been snapshotted and synced.
func TestStageAndSync_NonBlockingSyncWaiterNotAcknowledgedByEarlierSync(t *testing.T) {
	ctx := t.Context()
	rawBackend := NewMemoryBackend()
	gateBackend := &gatedBackend{
		ObjectStorageBackend: rawBackend,
		gate:                 make(chan struct{}),
	}
	s := NewTestServer(gateBackend, WithServerSyncCoalesceWindow(50*time.Millisecond))
	vol := "test-waiter-seq-vol"
	defer func() { _ = s.Close() }()

	// 1. Stage file 1
	c1, _ := testCreateFile(ctx, s, vol, "/file1.bin", 0644, nil, 0, 0)
	ino1 := c1.GetAttr().GetInode().GetIno()
	_, _ = s.WriteFile(ctx, &pb.WriteFileRequest{VolumeId: vol, Inode: ino1, Offset: 0, Data: bytes.Repeat([]byte("1"), 64*1024)})

	// Start sync 1 in background; its upload blocks on gateBackend
	sync1Done := make(chan struct{})
	go func() {
		defer close(sync1Done)
		_, _ = s.Fsync(ctx, &pb.FsyncRequest{VolumeId: vol, Inode: ino1, Blocking: true})
	}()

	// Wait for sync 1 to start upload and block
	time.Sleep(20 * time.Millisecond)

	// 2. Stage file 2 while sync 1 is in progress
	c2, _ := testCreateFile(ctx, s, vol, "/file2.bin", 0644, nil, 0, 0)
	ino2 := c2.GetAttr().GetInode().GetIno()
	_, _ = s.WriteFile(ctx, &pb.WriteFileRequest{VolumeId: vol, Inode: ino2, Offset: 0, Data: bytes.Repeat([]byte("2"), 64*1024)})

	// Issue non-blocking sync for file 2
	sync2Done := make(chan struct{})
	go func() {
		defer close(sync2Done)
		_, _ = s.Fsync(ctx, &pb.FsyncRequest{VolumeId: vol, Inode: ino2, Blocking: false})
	}()

	time.Sleep(50 * time.Millisecond)

	// Release gate so sync 1 can finish upload
	close(gateBackend.gate)

	// sync 1 finishes
	select {
	case <-sync1Done:
	case <-time.After(2 * time.Second):
		t.Fatalf("sync 1 did not finish")
	}

	// At this point, sync 1 has completed. File 2 was NOT in sync 1's snapshot.
	// When sync 2 unblocks, file 2 must be durable and readable on controller.
	select {
	case <-sync2Done:
		rResp, err := s.ReadFile(ctx, &pb.ReadFileRequest{VolumeId: vol, Inode: ino2, Offset: 0, Size: 64 * 1024})
		if err != nil || len(rResp.GetData()) != 64*1024 {
			t.Fatalf("sync 2 returned before file 2 data was committed! err=%v, len=%d", err, len(rResp.GetData()))
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("sync 2 timed out waiting for follow-up sync")
	}
}

// TestFsyncDurability_WithWAL_DurableAcrossRestart verifies that when a WAL stream is configured,
// a blocking Fsync persists metadata to the WAL, surviving controller restart even without a periodic flush.
func TestFsyncDurability_WithWAL_DurableAcrossRestart(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	bufDir := t.TempDir()
	_, target, cleanup := startTestWalBufferServer(t, bufDir)
	defer cleanup()

	clientDir1 := t.TempDir()
	volumeID := "test-durability-with-wal"
	server1 := NewTestServer(backend, WithServerWAL(clientDir1, target, walclient.Witness))

	// 1. Create /f and snapshot initial state
	createResp, err := testCreateFile(ctx, server1, volumeID, "/f", 0644, nil, 0, 0)
	if err != nil || createResp.GetError() != 0 {
		t.Fatalf("CreateFile /f failed: %v", err)
	}
	fIno := createResp.GetAttr().GetInode().GetIno()

	if err := server1.FlushAll(ctx); err != nil {
		t.Fatalf("FlushAll failed: %v", err)
	}

	// 2. Write "hello" and blocking Fsync
	wResp, err := server1.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId:  volumeID,
		Inode:     fIno,
		Offset:    0,
		Data:      []byte("hello"),
		WriteMode: pb.WriteMode_LAZY_WRITE,
	})
	if err != nil || wResp.GetError() != 0 {
		t.Fatalf("WriteFile failed: %v", err)
	}

	syncResp, err := server1.Fsync(ctx, &pb.FsyncRequest{
		VolumeId: volumeID,
		Inode:    fIno,
		Blocking: true,
	})
	if err != nil || syncResp.GetError() != 0 {
		t.Fatalf("Fsync(blocking) failed: %v, errno=%d", err, syncResp.GetError())
	}

	// 3. Crash restart: close server1 without FlushAll
	_ = server1.Close()

	// 4. Recover on server2 from backend + WAL
	clientDir2 := t.TempDir()
	server2 := NewTestServer(backend, WithServerWAL(clientDir2, target, walclient.Witness))
	defer func() { _ = server2.Close() }()

	readResp2, err := testReadFile(ctx, server2, volumeID, "/f", 0, 1024)
	if err != nil || readResp2.GetError() != 0 {
		t.Fatalf("ReadFile on server2 failed: %v", err)
	}
	attrResp2, err := testGetAttr(ctx, server2, volumeID, "/f")
	if err != nil || attrResp2.GetError() != 0 {
		t.Fatalf("GetAttr on server2 failed: %v", err)
	}
	if string(readResp2.GetData()) != "hello" || attrResp2.GetAttr().GetInode().GetSize() != 5 {
		t.Fatalf("Expected size 5 and 'hello' on recovered server with WAL, got size %d and content %q",
			attrResp2.GetAttr().GetInode().GetSize(), string(readResp2.GetData()))
	}
}

type blockingPutBackend struct {
	ObjectStorageBackend
	blockCh chan struct{}
}

func (b *blockingPutBackend) PutObject(ctx context.Context, volumeID, key string, stream blob.ByteStream) (string, error) {
	select {
	case <-b.blockCh:
		return b.ObjectStorageBackend.PutObject(ctx, volumeID, key, stream)
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func TestOpenBarrier_StagedInodeBlocksAndReturnsFreshAttrs(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewTestServer(backend)
	defer func() { _ = server.Close() }()

	volumeID := "test-barrier-staged-vol"
	vol, err := server.getOrCreateVolume(volumeID)
	if err != nil {
		t.Fatalf("getOrCreateVolume failed: %v", err)
	}
	vol.SetConsistencyMode(pb.ConsistencyMode_CLOSE_TO_OPEN)

	// Create a file
	createResp, err := testCreateFile(ctx, server, volumeID, "/staged.bin", 0644, nil, 0, 0)
	if err != nil || createResp.GetError() != 0 {
		t.Fatalf("CreateFile failed: %v", err)
	}
	ino := createResp.GetAttr().GetInode().GetIno()

	// Write 8 KiB lazily (staged in memory, not fsynced)
	payload := bytes.Repeat([]byte("A"), 8192)
	wResp, err := server.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId:  volumeID,
		Inode:     ino,
		Offset:    0,
		Data:      payload,
		WriteMode: pb.WriteMode_LAZY_WRITE,
	})
	if err != nil || wResp.GetError() != 0 {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Verify staged chunks exist before Open
	vol.stagingMu.Lock()
	stagedCount := len(vol.stagedChunks[ino])
	vol.stagingMu.Unlock()
	if stagedCount == 0 {
		t.Fatalf("Expected staged chunks for inode %d, found 0", ino)
	}

	// Open the file. Under CLOSE_TO_OPEN, Open barrier must sync staged chunks and return fresh attributes.
	openResp, err := server.Open(ctx, &pb.OpenRequest{
		VolumeId: volumeID,
		Inode:    ino,
	})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	if openResp.GetError() != 0 {
		t.Fatalf("Open returned error %d", openResp.GetError())
	}
	if openResp.GetAttr() == nil || openResp.GetAttr().GetInode() == nil {
		t.Fatalf("Open response missing Attr")
	}
	if openResp.GetAttr().GetInode().GetSize() != 8192 {
		t.Fatalf("Open response Attr size = %d, want 8192", openResp.GetAttr().GetInode().GetSize())
	}
	if openResp.GetStreamPosition() == 0 {
		t.Fatalf("Open response stream_position is 0, want > 0")
	}

	// Verify staged chunks were flushed
	vol.stagingMu.Lock()
	remainingStaged := len(vol.stagedChunks[ino])
	vol.stagingMu.Unlock()
	if remainingStaged != 0 {
		t.Fatalf("Expected 0 staged chunks after barrier sync, got %d", remainingStaged)
	}
}

func TestOpenBarrier_CleanInodeDoesNotBlock(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewTestServer(backend)
	defer func() { _ = server.Close() }()

	volumeID := "test-barrier-clean-vol"
	vol, err := server.getOrCreateVolume(volumeID)
	if err != nil {
		t.Fatalf("getOrCreateVolume failed: %v", err)
	}
	vol.SetConsistencyMode(pb.ConsistencyMode_CLOSE_TO_OPEN)

	// Create clean file
	cleanResp, err := testCreateFile(ctx, server, volumeID, "/clean.bin", 0644, nil, 0, 0)
	if err != nil || cleanResp.GetError() != 0 {
		t.Fatalf("CreateFile clean failed: %v", err)
	}
	cleanIno := cleanResp.GetAttr().GetInode().GetIno()

	// Create another file with staged writes
	stagedResp, err := testCreateFile(ctx, server, volumeID, "/staged.bin", 0644, nil, 0, 0)
	if err != nil || stagedResp.GetError() != 0 {
		t.Fatalf("CreateFile staged failed: %v", err)
	}
	stagedIno := stagedResp.GetAttr().GetInode().GetIno()

	wResp, err := server.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId:  volumeID,
		Inode:     stagedIno,
		Offset:    0,
		Data:      []byte("dirty-data"),
		WriteMode: pb.WriteMode_LAZY_WRITE,
	})
	if err != nil || wResp.GetError() != 0 {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Open the clean file: must not block on staged chunks of other inodes
	start := time.Now()
	openResp, err := server.Open(ctx, &pb.OpenRequest{
		VolumeId: volumeID,
		Inode:    cleanIno,
	})
	elapsed := time.Since(start)
	if err != nil || openResp.GetError() != 0 {
		t.Fatalf("Open clean failed: %v, err=%d", err, openResp.GetError())
	}
	if elapsed > 200*time.Millisecond {
		t.Errorf("Open clean took %v, expected near-instant execution", elapsed)
	}

	// Verify staged chunks on the other inode are still staged (not flushed by opening clean file)
	vol.stagingMu.Lock()
	stagedCount := len(vol.stagedChunks[stagedIno])
	vol.stagingMu.Unlock()
	if stagedCount == 0 {
		t.Fatalf("Expected staged chunks for inode %d to remain staged, but were cleared", stagedIno)
	}
}

func TestOpenBarrier_TimeoutReturnsEIO(t *testing.T) {
	ctx := t.Context()
	blockingBackend := &blockingPutBackend{
		ObjectStorageBackend: NewMemoryBackend(),
		blockCh:              make(chan struct{}),
	}
	server := NewTestServer(blockingBackend, WithServerOpenBarrierTimeout(50*time.Millisecond))
	defer func() {
		close(blockingBackend.blockCh)
		_ = server.Close()
	}()

	volumeID := "test-barrier-timeout-vol"
	vol, err := server.getOrCreateVolume(volumeID)
	if err != nil {
		t.Fatalf("getOrCreateVolume failed: %v", err)
	}
	vol.SetConsistencyMode(pb.ConsistencyMode_CLOSE_TO_OPEN)

	createResp, err := testCreateFile(ctx, server, volumeID, "/file.bin", 0644, nil, 0, 0)
	if err != nil || createResp.GetError() != 0 {
		t.Fatalf("CreateFile failed: %v", err)
	}
	ino := createResp.GetAttr().GetInode().GetIno()

	// Write multi-chunk data so it requires PutObject to blobStore (> maxInlineLen)
	payload := make([]byte, 64*1024)
	wResp, err := server.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId:  volumeID,
		Inode:     ino,
		Offset:    0,
		Data:      payload,
		WriteMode: pb.WriteMode_LAZY_WRITE,
	})
	if err != nil || wResp.GetError() != 0 {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Open should trigger barrier sync, block on PutObject, and time out with EIO
	openResp, err := server.Open(ctx, &pb.OpenRequest{
		VolumeId: volumeID,
		Inode:    ino,
	})
	if err != nil {
		t.Fatalf("Open RPC unexpected grpc error: %v", err)
	}
	if openResp.GetError() != int32(syscall.EIO) {
		t.Fatalf("Open expected error %d (EIO), got %d", syscall.EIO, openResp.GetError())
	}
}

type fakeCloseStream struct {
	walclient.Stream
}

func (f *fakeCloseStream) Close() error { return nil }

func TestOpenBarrier_DegradedReturnsEIOImmediately(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewTestServer(backend)
	defer func() { _ = server.Close() }()

	volumeID := "test-barrier-degraded-vol"
	vol, err := server.getOrCreateVolume(volumeID)
	if err != nil {
		t.Fatalf("getOrCreateVolume failed: %v", err)
	}
	vol.SetConsistencyMode(pb.ConsistencyMode_CLOSE_TO_OPEN)

	createResp, err := testCreateFile(ctx, server, volumeID, "/degraded.bin", 0644, nil, 0, 0)
	if err != nil || createResp.GetError() != 0 {
		t.Fatalf("CreateFile failed: %v", err)
	}
	ino := createResp.GetAttr().GetInode().GetIno()

	// Inject degraded stream
	mockStream := &faultInjectingStream{
		Stream: &fakeCloseStream{},
	}
	mockStream.degrade(fmt.Errorf("injected degradation"))
	vol.mu.Lock()
	vol.stream = mockStream
	vol.mu.Unlock()

	// Open on degraded volume must fail immediately with EIO
	start := time.Now()
	openResp, err := server.Open(ctx, &pb.OpenRequest{
		VolumeId: volumeID,
		Inode:    ino,
	})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Open RPC unexpected grpc error: %v", err)
	}
	if openResp.GetError() != int32(syscall.EIO) {
		t.Fatalf("Open on degraded volume expected EIO (%d), got %d", syscall.EIO, openResp.GetError())
	}
	if elapsed > 100*time.Millisecond {
		t.Errorf("Open on degraded volume took %v, expected immediate return", elapsed)
	}
}

type mockWatchStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (m *mockWatchStream) Context() context.Context {
	return m.ctx
}

func (m *mockWatchStream) Send(*pb.WatchVolumeResponse) error {
	return nil
}

func TestWatchVolume_ConflictingConsistencyModeRejected(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewTestServer(backend)
	defer func() { _ = server.Close() }()

	volumeID := "test-watch-conflict-vol"
	vol, err := server.getOrCreateVolume(volumeID)
	if err != nil {
		t.Fatalf("getOrCreateVolume failed: %v", err)
	}
	if err := vol.PersistConsistencyMode(ctx, pb.ConsistencyMode_CLOSE_TO_OPEN); err != nil {
		t.Fatalf("PersistConsistencyMode failed: %v", err)
	}

	stream := &mockWatchStream{ctx: ctx}

	// 1. Client requesting STANDARD on a CLOSE_TO_OPEN volume must be rejected with an error
	err = server.WatchVolume(&pb.WatchVolumeRequest{
		VolumeId:        volumeID,
		NodeId:          "node-1",
		ConsistencyMode: pb.ConsistencyMode_STANDARD,
	}, stream)
	if err == nil {
		t.Fatalf("Expected WatchVolume to reject conflicting STANDARD consistency mode, got nil")
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Expected InvalidArgument code for consistency mode mismatch, got: %v", err)
	}

	// Volume mode must remain CLOSE_TO_OPEN
	if vol.ConsistencyMode() != pb.ConsistencyMode_CLOSE_TO_OPEN {
		t.Fatalf("Volume consistency mode changed after rejected watcher: got %v, want CLOSE_TO_OPEN", vol.ConsistencyMode())
	}

	// 2. Client requesting UNSPECIFIED (e.g. objectfsctl) must not conflict
	watchCtx, cancelWatch := context.WithCancel(ctx)
	unspecStream := &mockWatchStream{ctx: watchCtx}
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancelWatch()
	}()
	err = server.WatchVolume(&pb.WatchVolumeRequest{
		VolumeId:        volumeID,
		NodeId:          "node-2",
		ConsistencyMode: pb.ConsistencyMode_CONSISTENCY_MODE_UNSPECIFIED,
	}, unspecStream)
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Expected UNSPECIFIED watcher to succeed without conflict, got: %v", err)
	}

	// Volume mode must still remain CLOSE_TO_OPEN
	if vol.ConsistencyMode() != pb.ConsistencyMode_CLOSE_TO_OPEN {
		t.Fatalf("Volume consistency mode changed after UNSPECIFIED watcher: got %v, want CLOSE_TO_OPEN", vol.ConsistencyMode())
	}
}

func TestOpenBarrier_SurvivesControllerRestartOverSameMetadataDir(t *testing.T) {
	ctx := t.Context()
	dataDir := t.TempDir()
	metaDir := filepath.Join(dataDir, "metadata")
	backend := NewMemoryBackend()
	volumeID := "test-barrier-restart-vol"
	bt, _ := buffertest.Start(ctx, t, buffertest.WithBackend(backend))
	walDir := t.TempDir()

	// 1. Controller 1 starts with metadataDir and creates close-to-open volume
	server1 := NewTestServer(backend,
		WithServerMetadataDir(metaDir),
		WithServerMetadataIndex("sqlite"),
		WithServerWAL(walDir, bt.Target, walclient.Witness),
		WithServerWALDialOptions(bt.DialOpts...),
	)
	csi1 := NewCSIController(server1)
	_, err := csi1.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name: volumeID,
		Parameters: map[string]string{
			"consistency": "close-to-open",
		},
		VolumeCapabilities: []*csi.VolumeCapability{
			{
				AccessType: &csi.VolumeCapability_Mount{
					Mount: &csi.VolumeCapability_MountVolume{},
				},
				AccessMode: &csi.VolumeCapability_AccessMode{
					Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateVolume failed on server1: %v", err)
	}

	// Create a file and stage writes without fsync
	createResp, err := testCreateFile(ctx, server1, volumeID, "/file.txt", 0644, nil, 0, 0)
	if err != nil || createResp.GetError() != 0 {
		t.Fatalf("CreateFile failed on server1: %v", err)
	}
	ino := createResp.GetAttr().GetInode().GetIno()

	// Stage 8 KiB lazily
	payload := bytes.Repeat([]byte("B"), 8192)
	wResp, err := server1.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId:  volumeID,
		Inode:     ino,
		Offset:    0,
		Data:      payload,
		WriteMode: pb.WriteMode_LAZY_WRITE,
	})
	if err != nil || wResp.GetError() != 0 {
		t.Fatalf("WriteFile failed on server1: %v", err)
	}

	// Flush volume state and close server 1
	vol1, err := server1.getOrCreateVolume(volumeID)
	if err != nil {
		t.Fatalf("getOrCreateVolume failed on server1: %v", err)
	}
	if err := vol1.FlushOverlay(ctx); err != nil {
		t.Fatalf("FlushOverlay failed on server1: %v", err)
	}
	if err := server1.Close(); err != nil {
		t.Fatalf("server1.Close failed: %v", err)
	}

	// 2. Controller 2 starts with same metadataDir over the same backend
	server2 := NewTestServer(backend,
		WithServerMetadataDir(metaDir),
		WithServerMetadataIndex("sqlite"),
		WithServerWAL(walDir, bt.Target, walclient.Witness),
		WithServerWALDialOptions(bt.DialOpts...),
	)
	defer func() { _ = server2.Close() }()

	vol2, err := server2.getOrCreateVolume(volumeID)
	if err != nil {
		t.Fatalf("getOrCreateVolume failed on server2: %v", err)
	}
	if vol2.ConsistencyMode() != pb.ConsistencyMode_CLOSE_TO_OPEN {
		t.Fatalf("Volume consistency mode did not survive restart: got %v, want CLOSE_TO_OPEN", vol2.ConsistencyMode())
	}

	// Now stage a write on server 2 without fsync, and open the file.
	// Because close-to-open was persisted, Open must barrier and flush the staged chunk.
	payload2 := bytes.Repeat([]byte("C"), 16384)
	wResp2, err := server2.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId:  volumeID,
		Inode:     ino,
		Offset:    0,
		Data:      payload2,
		WriteMode: pb.WriteMode_LAZY_WRITE,
	})
	if err != nil || wResp2.GetError() != 0 {
		t.Fatalf("WriteFile failed on server2: %v", err)
	}

	// Open the file on server 2 without any client watcher re-registering the mode
	openResp, err := server2.Open(ctx, &pb.OpenRequest{
		VolumeId: volumeID,
		Inode:    ino,
	})
	if err != nil || openResp.GetError() != 0 {
		t.Fatalf("Open failed on server2: %v, err=%d", err, openResp.GetError())
	}
	if openResp.GetAttr() == nil || openResp.GetAttr().GetInode() == nil {
		t.Fatalf("Open response on server2 missing Attr")
	}
	if openResp.GetAttr().GetInode().GetSize() != 16384 {
		t.Fatalf("Open response on server2 size = %d, want 16384 (barrier did not flush staged write)", openResp.GetAttr().GetInode().GetSize())
	}
}
