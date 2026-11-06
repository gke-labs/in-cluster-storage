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
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"iter"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/blob"
	"github.com/gke-labs/in-cluster-storage/pkg/wal"
	"github.com/gke-labs/in-cluster-storage/pkg/wal/buffer/buffertest"
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type recordingBackend struct {
	ObjectStorageBackend
	mu           sync.Mutex
	packKeys     []string
	packGetCalls int
	failPut      bool
	gateEntered  chan struct{}
	gateRelease  chan struct{}
}

func newRecordingBackend(base ObjectStorageBackend) *recordingBackend {
	return &recordingBackend{
		ObjectStorageBackend: base,
	}
}

func (b *recordingBackend) GetObject(ctx context.Context, volumeID, key string, offset, length int64, writer io.Writer) error {
	b.mu.Lock()
	if strings.HasSuffix(key, ".pack") {
		b.packGetCalls++
	}
	b.mu.Unlock()
	return b.ObjectStorageBackend.GetObject(ctx, volumeID, key, offset, length, writer)
}

// setGate configures channels to pause pack uploads at the storage boundary.
// gateEntered must be a buffered channel of capacity at least one so the
// arrival signal cannot be lost if the test goroutine has not yet reached its
// receive.
func (b *recordingBackend) setGate(gateEntered, gateRelease chan struct{}) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.gateEntered = gateEntered
	b.gateRelease = gateRelease
}

func (b *recordingBackend) PutObject(ctx context.Context, volumeID, key string, stream blob.ByteStream) (string, error) {
	b.mu.Lock()
	if b.failPut {
		b.mu.Unlock()
		return "", fmt.Errorf("injected PutObject failure")
	}
	isPack := strings.HasSuffix(key, ".pack") || strings.Contains(key, "blobs/")
	if isPack {
		b.packKeys = append(b.packKeys, key)
	}
	gateEntered := b.gateEntered
	gateRelease := b.gateRelease
	b.mu.Unlock()

	if isPack && gateEntered != nil && gateRelease != nil {
		select {
		case gateEntered <- struct{}{}:
		default:
		}
		<-gateRelease
	}
	return b.ObjectStorageBackend.PutObject(ctx, volumeID, key, stream)
}

func (b *recordingBackend) getPackCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.packKeys)
}

func (b *recordingBackend) getChunkCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	totalChunks := 0
	for _, key := range b.packKeys {
		if strings.HasSuffix(key, ".pack") {
			baseName := filepath.Base(key)
			trimmed := strings.TrimSuffix(baseName, ".pack")
			lastDash := strings.LastIndex(trimmed, "-")
			if lastDash != -1 {
				count, err := strconv.Atoi(trimmed[lastDash+1:])
				if err == nil {
					totalChunks += count
				}
			}
		} else {
			totalChunks++
		}
	}
	return totalChunks
}

func (b *recordingBackend) getPackGetCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.packGetCalls
}

func (b *recordingBackend) getPackKeys() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.packKeys...)
}

// TestStageAndSync_SinglePackMultiInode verifies that multiple writes across several inodes
// stay staged in memory without uploading any packs until Fsync, and that one consolidated
// packfile is uploaded containing all chunks across all inodes.
func TestStageAndSync_SinglePackMultiInode(t *testing.T) {
	ctx := t.Context()
	rawBackend := NewMemoryBackend()
	backend := newRecordingBackend(rawBackend)
	server := NewTestServer(t, backend)
	volumeID := "test-single-pack-vol"
	defer func() { shutdownTestServer(t, server) }()

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
			WriteMode: pb.WriteMode_LAZY,
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
	server := NewTestServer(t, NewMemoryBackend())
	volumeID := "test-cross-node-vol"
	defer func() { shutdownTestServer(t, server) }()

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
		WriteMode: pb.WriteMode_LAZY,
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
	server1 := NewTestServer(t, backend, WithServerWAL(clientDir1, target, walclient.Witness))
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
		WriteMode: pb.WriteMode_LAZY,
	})
	if err != nil || wResp1.GetError() != 0 {
		t.Fatalf("WriteFile failed: %v", err)
	}
	oldEpoch := wResp1.GetStagingEpoch()

	// Simulate crash of server1 (close without fsync)
	shutdownTestServer(t, server1)

	// Server 2 starts up (new controller instance with same backend & wal-buffer)
	clientDir2 := t.TempDir()
	server2 := NewTestServer(t, backend, WithServerWAL(clientDir2, target, walclient.Witness))

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
		WriteMode: pb.WriteMode_LAZY,
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
	server := NewTestServer(t, NewMemoryBackend())
	volumeID := "test-trunc-comp-vol"
	defer func() { shutdownTestServer(t, server) }()

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
	server := NewTestServer(t, backend, WithServerSyncCoalesceWindow(200*time.Millisecond))
	volumeID := "test-coalesce-burst-vol"
	defer func() { shutdownTestServer(t, server) }()

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

	// All chunks for the burst must have been uploaded; since seal-and-upload,
	// window expiration seals the open pack, so writes arriving across the window
	// fire boundary produce at least one pack rather than strictly one pack.
	if chunkCount := backend.getChunkCount(); chunkCount != numFiles {
		t.Fatalf("Expected %d uploaded chunks for burst within coalesce window, got %d", numFiles, chunkCount)
	}
	if packCount := backend.getPackCount(); packCount < 1 {
		t.Fatalf("Expected at least 1 packfile for burst within coalesce window, got %d", packCount)
	}
}

// TestStageAndSync_BlockingFsyncPreemptsCoalesceWindow verifies that a blocking Fsync
// arriving during an active coalesce window immediately flushes all pending writes.
func TestStageAndSync_BlockingFsyncPreemptsCoalesceWindow(t *testing.T) {
	ctx := t.Context()
	rawBackend := NewMemoryBackend()
	backend := newRecordingBackend(rawBackend)
	// Long coalesce window (5 seconds)
	server := NewTestServer(t, backend, WithServerSyncCoalesceWindow(5*time.Second))
	volumeID := "test-preempt-window-vol"
	defer func() { shutdownTestServer(t, server) }()

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
	server := NewTestServer(t, backend, WithServerSyncCoalesceWindow(10*time.Second))
	defer func() { shutdownTestServer(t, server) }()

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
	server := NewTestServer(t, backend, WithServerSyncCoalesceWindow(10*time.Second))
	volumeID := "test-close-preempt-window-vol"
	defer func() { shutdownTestServer(t, server) }()

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

	// All chunks from both files must have been uploaded; since seal-and-upload,
	// if the close floor window expires before the second write arrives, two packs
	// are uploaded rather than strictly one pack.
	if chunkCount := backend.getChunkCount(); chunkCount != 2 {
		t.Fatalf("Expected 2 uploaded chunks containing both writes, got %d", chunkCount)
	}
	if packCount := backend.getPackCount(); packCount < 1 {
		t.Fatalf("Expected at least 1 packfile containing both writes, got %d", packCount)
	}
}

// TestStageAndSync_TenConcurrentCloseRequestsCoalesceIntoOnePack verifies that ten
// CLOSE-triggered non-blocking Fsync requests in flight concurrently coalesce into a single packfile.
func TestStageAndSync_TenConcurrentCloseRequestsCoalesceIntoOnePack(t *testing.T) {
	ctx := t.Context()
	rawBackend := NewMemoryBackend()
	backend := newRecordingBackend(rawBackend)
	server := NewTestServer(t, backend, WithServerSyncCoalesceWindow(10*time.Second))
	volumeID := "test-ten-concurrent-close-vol"
	defer func() { shutdownTestServer(t, server) }()

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
	server := NewTestServer(t, backend, WithServerSyncCoalesceWindow(500*time.Millisecond))
	volumeID := "test-close-per-inode-vol"
	defer func() { shutdownTestServer(t, server) }()

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

	// In the v2 pack staging design, writes share the open pack; when Inode A closes,
	// the sealed pack containing the staged writes is uploaded and Inode A's metadata committed.
	// When Inode B's coalesce window fires, Inode B's metadata is committed referencing the pack.
	if cnt := backend.getPackCount(); cnt < 1 {
		t.Fatalf("Expected at least 1 packfile after window sync, got %d", cnt)
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
	server := NewTestServer(t, backend, WithServerSyncCoalesceWindow(50*time.Millisecond))
	volumeID := "test-error-surface-vol"
	defer func() { shutdownTestServer(t, server) }()

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
	server := NewTestServer(t, NewMemoryBackend(), WithServerMaxStagingBytes(128*1024))
	volumeID := "test-staging-cap-vol"
	defer func() { shutdownTestServer(t, server) }()

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

	s := NewTestServer(t, NewMemoryBackend(), WithServerSyncCoalesceWindow(50*time.Millisecond))
	vol := "test-empty-coalesce-vol"

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
	s := NewTestServer(t, gateBackend, WithServerSyncCoalesceWindow(50*time.Millisecond))
	vol := "test-waiter-seq-vol"

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
	server1 := NewTestServer(t, backend, WithServerWAL(clientDir1, target, walclient.Witness))

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
		WriteMode: pb.WriteMode_LAZY,
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
	shutdownTestServer(t, server1)

	// 4. Recover on server2 from backend + WAL
	clientDir2 := t.TempDir()
	server2 := NewTestServer(t, backend, WithServerWAL(clientDir2, target, walclient.Witness))

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
	server := NewTestServer(t, backend)
	defer func() { shutdownTestServer(t, server) }()

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
		WriteMode: pb.WriteMode_LAZY,
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
	server := NewTestServer(t, backend)
	defer func() { shutdownTestServer(t, server) }()

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
		WriteMode: pb.WriteMode_LAZY,
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
	server := NewTestServer(t, blockingBackend, WithServerOpenBarrierTimeout(50*time.Millisecond))
	defer func() {
		close(blockingBackend.blockCh)
		shutdownTestServer(t, server)
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
		WriteMode: pb.WriteMode_LAZY,
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

func TestOpenBarrier_WaitsOnInFlightSync(t *testing.T) {
	ctx := t.Context()
	blockingBackend := &blockingPutBackend{
		ObjectStorageBackend: NewMemoryBackend(),
		blockCh:              make(chan struct{}),
	}
	server := NewTestServer(t, blockingBackend, WithServerOpenBarrierTimeout(5*time.Second))
	defer func() {
		select {
		case <-blockingBackend.blockCh:
		default:
			close(blockingBackend.blockCh)
		}
		shutdownTestServer(t, server)
	}()

	volumeID := "test-barrier-wait-inflight-vol"
	vol, err := server.getOrCreateVolume(volumeID)
	if err != nil {
		t.Fatalf("getOrCreateVolume failed: %v", err)
	}
	vol.SetConsistencyMode(pb.ConsistencyMode_CLOSE_TO_OPEN)

	createResp, err := testCreateFile(ctx, server, volumeID, "/inflight.bin", 0644, nil, 0, 0)
	if err != nil || createResp.GetError() != 0 {
		t.Fatalf("CreateFile failed: %v", err)
	}
	inodeID := createResp.GetAttr().GetInode().GetIno()

	payload := make([]byte, 64*1024)
	wResp, err := server.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId:  volumeID,
		Inode:     inodeID,
		Offset:    0,
		Data:      payload,
		WriteMode: pb.WriteMode_LAZY,
	})
	if err != nil || wResp.GetError() != 0 {
		t.Fatalf("WriteFile failed: %v", err)
	}

	type fsyncResult struct {
		resp *pb.FsyncResponse
		err  error
	}
	fsyncDone := make(chan fsyncResult, 1)
	go func() {
		resp, fsyncErr := server.Fsync(ctx, &pb.FsyncRequest{
			VolumeId: volumeID,
			Inode:    inodeID,
			Blocking: false,
			Trigger:  pb.FsyncRequest_SYNC_TRIGGER_CLOSE,
		})
		fsyncDone <- fsyncResult{resp: resp, err: fsyncErr}
	}()

	deadline := time.Now().Add(2 * time.Second)
	for {
		vol.stagingMu.Lock()
		inProgress := vol.syncInProgress
		vol.stagingMu.Unlock()
		if inProgress {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for close sync to enter in-flight state")
		}
		time.Sleep(10 * time.Millisecond)
	}

	type openResult struct {
		resp *pb.OpenResponse
		err  error
	}
	openDone := make(chan openResult, 1)
	go func() {
		resp, openErr := server.Open(ctx, &pb.OpenRequest{
			VolumeId: volumeID,
			Inode:    inodeID,
		})
		openDone <- openResult{resp: resp, err: openErr}
	}()

	select {
	case res := <-openDone:
		t.Fatalf("Open returned prematurely while sync was in-flight: resp=%+v, err=%v", res.resp, res.err)
	case <-time.After(100 * time.Millisecond):
	}

	close(blockingBackend.blockCh)

	select {
	case res := <-fsyncDone:
		if res.err != nil || res.resp.GetError() != 0 {
			t.Fatalf("Fsync failed: %v, resp=%+v", res.err, res.resp)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Fsync failed to complete after backend unblocked")
	}

	select {
	case res := <-openDone:
		if res.err != nil || res.resp.GetError() != 0 {
			t.Fatalf("Open failed: %v, resp=%+v", res.err, res.resp)
		}
		if res.resp.GetAttr() == nil || res.resp.GetAttr().GetInode().GetSize() != int64(len(payload)) {
			t.Fatalf("Open returned unexpected size: %+v", res.resp.GetAttr())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Open failed to unblock after in-flight sync completed")
	}
}

type fakeCloseStream struct {
	walclient.Stream
}

func (f *fakeCloseStream) Close() error { return nil }

func TestOpenBarrier_DegradedReturnsEIOImmediately(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewTestServer(t, backend)
	defer func() { shutdownTestServer(t, server) }()

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
	server := NewTestServer(t, backend)
	defer func() { shutdownTestServer(t, server) }()

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
	server1 := NewTestServer(t, backend,
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
		WriteMode: pb.WriteMode_LAZY,
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
	shutdownTestServer(t, server1)

	// 2. Controller 2 starts with same metadataDir over the same backend
	server2 := NewTestServer(t, backend,
		WithServerMetadataDir(metaDir),
		WithServerMetadataIndex("sqlite"),
		WithServerWAL(walDir, bt.Target, walclient.Witness),
		WithServerWALDialOptions(bt.DialOpts...),
	)

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
		WriteMode: pb.WriteMode_LAZY,
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

func createScratchRemnant(t *testing.T, scratchDirectory string) string {
	t.Helper()
	if err := os.MkdirAll(scratchDirectory, 0755); err != nil {
		t.Fatalf("failed creating scratch directory: %v", err)
	}
	packPath := filepath.Join(scratchDirectory, "scratch.pack")
	if err := os.WriteFile(packPath, []byte("stale-scratch-pack-bytes"), 0644); err != nil {
		t.Fatalf("failed writing scratch pack remnant: %v", err)
	}
	return packPath
}

type scratchStagingHelper struct {
	t                 *testing.T
	ctx               context.Context
	volumeID          string
	inodeID           uint64
	metadataDirectory string
	scratchDirectory  string
	backend           *recordingBackend
	server            *Server
	volume            *Volume
}

func newScratchStagingHelper(t *testing.T, volumeID string) *scratchStagingHelper {
	t.Helper()
	ctx := t.Context()
	metadataDirectory := t.TempDir()
	rawBackend := NewMemoryBackend()
	backend := newRecordingBackend(rawBackend)
	server := NewTestServer(t, backend, WithServerMetadataDir(metadataDirectory))
	volume, err := server.getOrCreateVolume(volumeID)
	if err != nil {
		t.Fatalf("getOrCreateVolume failed: %v", err)
	}

	createResponse, err := testCreateFile(ctx, server, volumeID, "/testfile.bin", 0644, nil, 0, 0)
	if err != nil || createResponse.GetError() != 0 {
		t.Fatalf("CreateFile failed: %v", err)
	}
	inodeID := createResponse.GetAttr().GetInode().GetIno()

	scratchDirectory := filepath.Join(metadataDirectory, volumeID, "staging")
	return &scratchStagingHelper{
		t:                 t,
		ctx:               ctx,
		volumeID:          volumeID,
		inodeID:           inodeID,
		metadataDirectory: metadataDirectory,
		scratchDirectory:  scratchDirectory,
		backend:           backend,
		server:            server,
		volume:            volume,
	}
}

func (h *scratchStagingHelper) write(offset int64, data []byte) int64 {
	h.t.Helper()
	writeResponse, err := h.server.WriteFile(h.ctx, &pb.WriteFileRequest{
		VolumeId:  h.volumeID,
		Inode:     h.inodeID,
		Offset:    offset,
		Data:      data,
		WriteMode: pb.WriteMode_LAZY,
	})
	if err != nil || writeResponse.GetError() != 0 {
		h.t.Fatalf("WriteFile failed: %v, err=%d", err, writeResponse.GetError())
	}
	return writeResponse.GetStagingEpoch()
}

func (h *scratchStagingHelper) activeScratchPackPath() string {
	h.volume.stagingMu.Lock()
	defer h.volume.stagingMu.Unlock()
	return h.volume.scratchPackPath(h.volume.activePackID)
}

func (h *scratchStagingHelper) fsync() {
	h.t.Helper()
	fsyncResponse, err := h.server.Fsync(h.ctx, &pb.FsyncRequest{
		VolumeId: h.volumeID,
		Inode:    h.inodeID,
		Blocking: true,
	})
	if err != nil || !fsyncResponse.GetSuccess() {
		h.t.Fatalf("Fsync failed: %v, err=%d", err, fsyncResponse.GetError())
	}
}

func (h *scratchStagingHelper) assertReadBack(expectedPayload []byte) {
	h.t.Helper()
	readResponse, err := h.server.ReadFile(h.ctx, &pb.ReadFileRequest{
		VolumeId: h.volumeID,
		Inode:    h.inodeID,
		Offset:   0,
		Size:     int64(len(expectedPayload)),
	})
	if err != nil || readResponse.GetError() != 0 {
		h.t.Fatalf("ReadFile after sync failed: %v, err=%d", err, readResponse.GetError())
	}
	if !bytes.Equal(readResponse.GetData(), expectedPayload) {
		h.t.Fatalf("ReadFile content mismatch after sync from scratch pack")
	}
}

func TestStageAndSync_ScratchOpenPack_PushAndReadBack(t *testing.T) {
	helper := newScratchStagingHelper(t, "test-scratch-pack-vol")

	payload := bytes.Repeat([]byte("scratch-open-pack-data-"), 5000)
	helper.write(0, payload)

	helper.volume.stagingMu.Lock()
	stagedChunksCount := len(helper.volume.stagedChunks[helper.inodeID])
	scratchPackPath := helper.volume.scratchPackPath(helper.volume.activePackID)
	helper.volume.stagingMu.Unlock()
	if stagedChunksCount == 0 {
		t.Fatalf("expected staged chunk references for inode %d, found 0", helper.inodeID)
	}

	if _, err := os.Stat(scratchPackPath); err != nil {
		t.Fatalf("expected scratch open pack at %s, stat err: %v", scratchPackPath, err)
	}
	if backendPackCount := helper.backend.getPackCount(); backendPackCount != 0 {
		t.Fatalf("expected 0 backend packs before sync, got %d", backendPackCount)
	}

	helper.fsync()

	if backendPackCount := helper.backend.getPackCount(); backendPackCount == 0 {
		t.Fatalf("expected backend packs uploaded after sync, got 0")
	}
	// In the local pack retention design (Issue #455), the uploaded pack remains on disk
	// for reading without round-tripping through object storage until staging cap eviction.
	if _, err := os.Stat(scratchPackPath); err != nil {
		t.Fatalf("expected uploaded pack %s to remain on disk for reads after sync, stat err: %v", scratchPackPath, err)
	}

	secondPayload := []byte("second-write-payload")
	helper.write(int64(len(payload)), secondPayload)
	secondPackPath := helper.activeScratchPackPath()
	if _, err := os.Stat(secondPackPath); err != nil {
		t.Fatalf("expected fresh scratch pack created on second write, stat err: %v", err)
	}

	helper.fsync()
	helper.assertReadBack(append(payload, secondPayload...))
}

func TestStageAndSync_ScratchOpenPack_RestartClearsScratchAndBumpsEpoch(t *testing.T) {
	ctx := t.Context()
	helper := newScratchStagingHelper(t, "test-scratch-restart-vol")

	writePayload := bytes.Repeat([]byte("staged-not-synced-"), 1000)
	oldEpoch := helper.write(0, writePayload)

	shutdownTestServer(t, helper.server)
	createScratchRemnant(t, helper.scratchDirectory)

	server2 := NewTestServer(t, helper.backend, WithServerMetadataDir(helper.metadataDirectory))
	volume2, err := server2.getOrCreateVolume(helper.volumeID)
	if err != nil {
		t.Fatalf("getOrCreateVolume failed on server2: %v", err)
	}

	remainingPackFiles, err := filepath.Glob(filepath.Join(helper.scratchDirectory, "*.pack"))
	if err != nil {
		t.Fatalf("failed globbing scratch pack files after restart: %v", err)
	}
	if len(remainingPackFiles) != 0 {
		t.Fatalf("expected scratch directory to be cleared on volume open, found %v", remainingPackFiles)
	}

	volume2.stagingMu.Lock()
	newEpoch := volume2.stagingEpoch
	volume2.stagingMu.Unlock()
	if newEpoch == oldEpoch {
		t.Fatalf("expected staging epoch to be bumped on restart, got %d for both", oldEpoch)
	}

	fsyncResponse, err := server2.Fsync(ctx, &pb.FsyncRequest{
		VolumeId:     helper.volumeID,
		Inode:        helper.inodeID,
		Blocking:     true,
		StagingEpoch: oldEpoch,
	})
	if err != nil {
		t.Fatalf("Fsync RPC failed: %v", err)
	}
	if fsyncResponse.GetError() != int32(syscall.ESTALE) {
		t.Fatalf("expected ESTALE (%d) from restarted controller, got %d", syscall.ESTALE, fsyncResponse.GetError())
	}
}

// TestStagedPackRetainedUntilAllReferencingInodesCommit verifies that when an open pack
// contains staged chunks for multiple inodes (A and B) and Inode A is close-synced
// (triggering upload of the sealed pack), the packfile remains on disk and referenced
// in the controller until Inode B commits its staged chunks.
func TestStagedPackRetainedUntilAllReferencingInodesCommit(t *testing.T) {
	ctx := t.Context()
	rawBackend := NewMemoryBackend()
	server := NewTestServer(t, rawBackend, WithServerMaxStagingBytes(64*1024*1024))
	volumeID := "test-pack-retention-vol"
	defer func() {
		shutdownTestServer(t, server)
	}()

	volume, err := server.getOrCreateVolume(volumeID)
	if err != nil {
		t.Fatalf("getOrCreateVolume failed: %v", err)
	}

	inspectPack := func(packID uint64) (bool, int, string) {
		volume.stagingMu.Lock()
		defer volume.stagingMu.Unlock()
		info, tracked := volume.packs[packID]
		var liveReferences int
		if tracked && info != nil {
			liveReferences = info.liveReferences
		}
		return tracked, liveReferences, volume.scratchPackPath(packID)
	}

	createResponseA, err := testCreateFile(ctx, server, volumeID, "/fileA.bin", 0644, nil, 0, 0)
	if err != nil || createResponseA.GetError() != 0 {
		t.Fatalf("CreateFile A failed: %v", err)
	}
	inoA := createResponseA.GetAttr().GetInode().GetIno()

	createResponseB, err := testCreateFile(ctx, server, volumeID, "/fileB.bin", 0644, nil, 0, 0)
	if err != nil || createResponseB.GetError() != 0 {
		t.Fatalf("CreateFile B failed: %v", err)
	}
	inoB := createResponseB.GetAttr().GetInode().GetIno()

	// Stage writes for Inode B (below inline threshold) and Inode A in the same open pack
	dataB := []byte("hello from inode B inline data")
	if _, _, _, _, err := volume.WriteFileWithEpoch(ctx, inoB, 0, dataB, pb.WriteMode_LAZY); err != nil {
		t.Fatalf("WriteFile B failed: %v", err)
	}

	dataA := bytes.Repeat([]byte("A"), 64*1024)
	if _, _, _, _, err := volume.WriteFileWithEpoch(ctx, inoA, 0, dataA, pb.WriteMode_LAZY); err != nil {
		t.Fatalf("WriteFile A failed: %v", err)
	}

	volume.stagingMu.Lock()
	packID := volume.activePackID
	volume.stagingMu.Unlock()

	tracked, _, packPath := inspectPack(packID)
	if !tracked {
		t.Fatalf("expected active pack %d to be tracked", packID)
	}
	if _, err := os.Stat(packPath); err != nil {
		t.Fatalf("expected pack file to exist at %s: %v", packPath, err)
	}

	// Close-sync Inode A only (seals the active pack and uploads it to object storage)
	volume.stagingMu.Lock()
	if _, err := volume.syncInodesLocked(ctx, map[uint64]struct{}{inoA: {}}, pb.FsyncRequest_SYNC_TRIGGER_CLOSE); err != nil {
		t.Fatalf("syncInodesLocked A failed: %v", err)
	}

	// The packfile has been uploaded, but because Inode B still references it,
	// the pack MUST remain in volume.packs and on disk.
	tracked, liveReferences, _ := inspectPack(packID)
	if !tracked {
		t.Fatalf("expected pack %d to remain in volume.packs while Inode B still references it", packID)
	}
	if liveReferences != 1 {
		t.Fatalf("expected 1 live ref for pack %d, got %d", packID, liveReferences)
	}
	if _, err := os.Stat(packPath); err != nil {
		t.Fatalf("expected pack file %s to remain on disk while Inode B references it: %v", packPath, err)
	}

	// Partially overwrite one of B's chunks: tests readStagedChunkBytesLocked on the uploaded pack
	partialData := []byte("UPDATED")
	if _, _, _, _, err := volume.WriteFileWithEpoch(ctx, inoB, 6, partialData, pb.WriteMode_LAZY); err != nil {
		t.Fatalf("partial overwrite on Inode B failed: %v", err)
	}

	// Run a sync for B with B below the inline threshold: tests readStagedChunkBytesLocked in inline path of executeSyncLocked
	responseB, err := server.Fsync(ctx, &pb.FsyncRequest{
		VolumeId: volumeID,
		Inode:    inoB,
		Blocking: true,
	})
	if err != nil || !responseB.GetSuccess() {
		t.Fatalf("Fsync B failed: %v", err)
	}

	// Verify Inode B reads back accurately
	readResponse, err := server.ReadFile(ctx, &pb.ReadFileRequest{
		VolumeId: volumeID,
		Inode:    inoB,
		Offset:   0,
		Size:     int64(len(dataB)),
	})
	if err != nil || readResponse.GetError() != 0 {
		t.Fatalf("ReadFile B failed: %v", err)
	}
	expectedB := append([]byte("hello "), append(partialData, []byte("ode B inline data")...)...)
	if !bytes.Equal(readResponse.GetData(), expectedB) {
		t.Fatalf("read data mismatch on Inode B: got %q, want %q", string(readResponse.GetData()), string(expectedB))
	}

	// In the local pack retention design (Issue #455), the packfile remains on disk
	// and in volume.packs after all referencing inodes commit so immediate post-sync reads
	// are served locally without round-tripping through object storage.
	if _, err := os.Stat(packPath); err != nil {
		t.Fatalf("expected pack file %s to remain on disk for reads after commit: %v", packPath, err)
	}
}

// TestPackObjectKeysDistinctAcrossRestart verifies that two one-chunk packs of identical size
// created across a staging directory reset receive distinct object keys derived from the sha256
// hash of their sealed contents rather than colliding on filename and size.
func TestPackObjectKeysDistinctAcrossRestart(t *testing.T) {
	ctx := t.Context()
	rawBackend := NewMemoryBackend()
	backend := newRecordingBackend(rawBackend)
	server := NewTestServer(t, backend, WithServerMaxStagingBytes(64*1024*1024))
	volumeID := "test-distinct-keys-vol"
	defer func() {
		shutdownTestServer(t, server)
	}()

	volume, err := server.getOrCreateVolume(volumeID)
	if err != nil {
		t.Fatalf("getOrCreateVolume failed: %v", err)
	}

	createResponse1, err := testCreateFile(ctx, server, volumeID, "/file1.bin", 0644, nil, 0, 0)
	if err != nil || createResponse1.GetError() != 0 {
		t.Fatalf("CreateFile 1 failed: %v", err)
	}
	ino1 := createResponse1.GetAttr().GetInode().GetIno()

	// Write 64 KiB of 'A' and sync
	payload1 := bytes.Repeat([]byte("A"), 64*1024)
	if _, _, _, _, err := volume.WriteFileWithEpoch(ctx, ino1, 0, payload1, pb.WriteMode_LAZY); err != nil {
		t.Fatalf("WriteFile 1 failed: %v", err)
	}
	fsyncResponse1, err := server.Fsync(ctx, &pb.FsyncRequest{VolumeId: volumeID, Inode: ino1, Blocking: true})
	if err != nil || !fsyncResponse1.GetSuccess() {
		t.Fatalf("Fsync 1 failed: %v", err)
	}

	keysAfterFirst := backend.getPackKeys()
	if len(keysAfterFirst) != 1 {
		t.Fatalf("expected 1 pack key after first sync, got %d", len(keysAfterFirst))
	}
	firstKey := keysAfterFirst[0]

	// Reset staging directory to simulate restart with nextPackID reset to 0
	volume.stagingMu.Lock()
	if err := volume.removeStagingDirLocked(); err != nil {
		volume.stagingMu.Unlock()
		t.Fatalf("removeStagingDirLocked failed: %v", err)
	}
	volume.nextPackID = 0
	volume.stagingMu.Unlock()

	createResponse2, err := testCreateFile(ctx, server, volumeID, "/file2.bin", 0644, nil, 0, 0)
	if err != nil || createResponse2.GetError() != 0 {
		t.Fatalf("CreateFile 2 failed: %v", err)
	}
	ino2 := createResponse2.GetAttr().GetInode().GetIno()

	// Write 64 KiB of 'B' (identical size, different content) and sync
	payload2 := bytes.Repeat([]byte("B"), 64*1024)
	if _, _, _, _, err := volume.WriteFileWithEpoch(ctx, ino2, 0, payload2, pb.WriteMode_LAZY); err != nil {
		t.Fatalf("WriteFile 2 failed: %v", err)
	}
	fsyncResponse2, err := server.Fsync(ctx, &pb.FsyncRequest{VolumeId: volumeID, Inode: ino2, Blocking: true})
	if err != nil || !fsyncResponse2.GetSuccess() {
		t.Fatalf("Fsync 2 failed: %v", err)
	}

	keysAfterSecond := backend.getPackKeys()
	if len(keysAfterSecond) != 2 {
		t.Fatalf("expected 2 pack keys after second sync, got %d", len(keysAfterSecond))
	}
	secondKey := keysAfterSecond[1]

	if firstKey == secondKey {
		t.Fatalf("expected distinct pack keys for different pack contents across restart, got identical key: %s", firstKey)
	}
}

// TestLocalPackRead_NoBackendRoundTripOnCommittedRead verifies that reads of chunks
// committed from a pack created by the controller are served from the local pack on disk
// without fetching from object storage, and that eviction upon exceeding staging cap
// cleanly falls back to object storage on miss.
func TestLocalPackRead_NoBackendRoundTripOnCommittedRead(t *testing.T) {
	ctx := t.Context()
	backend := newRecordingBackend(NewMemoryBackend())
	server := NewTestServer(t, backend, WithServerMaxStagingBytes(64*1024*1024))
	volumeID := "test-local-pack-read-vol"

	createResp, err := testCreateFile(ctx, server, volumeID, "/chunk.bin", 0644, nil, 0, 0)
	if err != nil || createResp.GetError() != 0 {
		t.Fatalf("CreateFile failed: %v", err)
	}
	ino := createResp.GetAttr().GetInode().GetIno()

	payload := bytes.Repeat([]byte("X"), 64*1024)
	if resp, err := server.WriteFile(ctx, &pb.WriteFileRequest{VolumeId: volumeID, Inode: ino, Data: payload}); err != nil || resp.GetError() != 0 {
		t.Fatalf("WriteFile failed: %v", err)
	}
	if resp, err := server.Fsync(ctx, &pb.FsyncRequest{VolumeId: volumeID, Inode: ino, Blocking: true}); err != nil || !resp.GetSuccess() {
		t.Fatalf("Fsync failed: %v", err)
	}

	// 1. Controller ReadFile RPC (used by daemon and FUSE fallback)
	readResp, err := server.ReadFile(ctx, &pb.ReadFileRequest{VolumeId: volumeID, Inode: ino, Size: int64(len(payload))})
	if err != nil || readResp.GetError() != 0 || !bytes.Equal(readResp.GetData(), payload) {
		t.Fatalf("ReadFile failed: %v", err)
	}

	// 2. Controller GetBlob RPC (used by FUSE disk cache fill)
	hasher := sha256.New()
	hasher.Write(payload)
	stream := &testGetBlobServer{ctx: ctx}
	if err := server.GetBlob(&pb.GetBlobRequest{Sha256: hex.EncodeToString(hasher.Sum(nil))}, stream); err != nil || !bytes.Equal(bytes.Join(stream.chunks, nil), payload) {
		t.Fatalf("GetBlob failed: %v", err)
	}
	if count := backend.getPackGetCount(); count != 0 {
		t.Fatalf("expected 0 backend pack GetObject calls for local pack, got %d", count)
	}

	// 3. Evict pack by reducing staging capacity and verify fallback to backend GetObject
	vol, err := server.getOrCreateVolume(volumeID)
	if err != nil {
		t.Fatalf("getOrCreateVolume failed: %v", err)
	}
	vol.evictAllUploadedPacksForTest()

	postResp, err := server.ReadFile(ctx, &pb.ReadFileRequest{VolumeId: volumeID, Inode: ino, Size: int64(len(payload))})
	if err != nil || postResp.GetError() != 0 || !bytes.Equal(postResp.GetData(), payload) || backend.getPackGetCount() == 0 {
		t.Fatalf("post-eviction ReadFile failed or did not fetch from backend: %v", err)
	}
}

type durabilityCountingStream struct {
	appendCount atomic.Int64
	waitCount   atomic.Int64
	localSeq    atomic.Uint64
}

func (s *durabilityCountingStream) Append(_ context.Context, _ []byte) (uint64, error) {
	s.appendCount.Add(1)
	return s.localSeq.Add(1), nil
}

func (s *durabilityCountingStream) Wait(_ context.Context, _ uint64, _ walclient.Level, _ bool) error {
	s.waitCount.Add(1)
	return nil
}

func (s *durabilityCountingStream) Flush(_ context.Context) error {
	return nil
}

func (s *durabilityCountingStream) Watermarks() (local, witness, permanent uint64) {
	currentSeq := s.localSeq.Load()
	return currentSeq, currentSeq, currentSeq
}

func (s *durabilityCountingStream) RecoveredRecords() []*wal.ClientRecord {
	return nil
}

func (s *durabilityCountingStream) ReplicationLevel() walclient.Level {
	return walclient.Permanent
}

func (s *durabilityCountingStream) Tail(_ context.Context, _ uint64) (iter.Seq2[uint64, []byte], error) {
	return func(yield func(uint64, []byte) bool) {}, nil
}

func (s *durabilityCountingStream) Degraded() *walclient.DegradedError {
	return nil
}

func (s *durabilityCountingStream) Close() error {
	return nil
}

// TestSync_SingleDurabilityWaitPerCommit asserts that during a multi-chunk sync,
// all metadata records are committed with exactly one durability wait at commit time,
// never waiting once per record.
func TestSync_SingleDurabilityWaitPerCommit(t *testing.T) {
	tests := []struct {
		name       string
		durability walclient.Level
	}{
		{name: "Local", durability: walclient.Local},
		{name: "Witness", durability: walclient.Witness},
		{name: "Permanent", durability: walclient.Permanent},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := t.Context()
			countingStream := &durabilityCountingStream{}

			backend := NewMemoryBackend()
			server := NewTestServer(t, backend,
				WithServerStreamFactory(func(volumeID string) (walclient.Stream, error) {
					return countingStream, nil
				}),
				WithServerDurability(testCase.durability),
			)

			volumeID := "test-single-wait-" + strings.ToLower(testCase.name)
			createResponse, err := testCreateFile(ctx, server, volumeID, "/multi_chunk.bin", 0644, nil, 0, 0)
			if err != nil || createResponse.GetError() != 0 {
				t.Fatalf("testCreateFile failed: %v (code %d)", err, createResponse.GetError())
			}
			inodeID := createResponse.GetAttr().GetInode().GetIno()

			// Reset wait and append counters after initial file creation
			countingStream.waitCount.Store(0)
			countingStream.appendCount.Store(0)

			// Write 16 chunks with LAZY so chunks are staged in memory
			chunkSize := 64 * 1024
			numChunks := 16
			payload := bytes.Repeat([]byte("X"), numChunks*chunkSize)

			for chunkIndex := 0; chunkIndex < numChunks; chunkIndex++ {
				offset := int64(chunkIndex * chunkSize)
				chunkData := payload[offset : offset+int64(chunkSize)]
				writeResponse, err := testWriteFile(ctx, server, volumeID, "/multi_chunk.bin", offset, chunkData, pb.WriteMode_LAZY)
				if err != nil || writeResponse.GetError() != 0 {
					t.Fatalf("testWriteFile chunk %d failed: %v (code %d)", chunkIndex, err, writeResponse.GetError())
				}
			}

			// During lazy staging, no WAL appends or durability waits have occurred
			if appends := countingStream.appendCount.Load(); appends != 0 {
				t.Fatalf("expected 0 appends during lazy staging, got %d", appends)
			}
			if waits := countingStream.waitCount.Load(); waits != 0 {
				t.Fatalf("expected 0 durability waits during lazy staging, got %d", waits)
			}

			// Fsync triggers executeSyncLocked, committing all 16 FileChunk rows + Inode update + TxCommit
			fsyncResponse, err := server.Fsync(ctx, &pb.FsyncRequest{
				VolumeId: volumeID,
				Inode:    inodeID,
				Blocking: true,
			})
			if err != nil || !fsyncResponse.GetSuccess() {
				t.Fatalf("server.Fsync failed: %v", err)
			}

			appends := countingStream.appendCount.Load()
			waits := countingStream.waitCount.Load()

			// Expect at least numChunks (16) records appended: 16 FileChunk records + 1 Inode update + 1 TxCommit = 18 records
			if appends < int64(numChunks) {
				t.Errorf("expected at least %d appends, got %d", numChunks, appends)
			}

			// Critical invariant: exactly ONE durability wait per sync, not one per record!
			if waits != 1 {
				t.Errorf("expected exactly 1 durability wait per sync, got %d (for %d appended records)", waits, appends)
			}
		})
	}
}

// TestStagingSlotSeals_RotationAndMetrics verifies that appending more than 1024 unique chunks
// triggers a slot-cap seal rotation and increments StagingSlotSeals in VolumeStats.
func TestStagingSlotSeals_RotationAndMetrics(t *testing.T) {
	ctx := t.Context()
	server := NewTestServer(t, NewMemoryBackend(), WithServerMaxStagingBytes(256*1024*1024))
	volumeID := "test-slot-seals-vol"

	vol, err := server.getOrCreateVolume(volumeID)
	if err != nil {
		t.Fatalf("getOrCreateVolume failed: %v", err)
	}

	cResp, err := testCreateFile(ctx, server, volumeID, "/slot_test.bin", 0644, nil, 0, 0)
	if err != nil || cResp.GetError() != 0 {
		t.Fatalf("CreateFile failed: %v", err)
	}
	ino := cResp.GetAttr().GetInode().GetIno()

	var firstChunkData, lastChunkData []byte

	// Append 1025 unique 16-byte chunks (each at a separate 64 KiB chunk boundary)
	for i := 0; i < 1025; i++ {
		data := make([]byte, 16)
		binary.BigEndian.PutUint64(data[0:8], uint64(i))
		binary.BigEndian.PutUint64(data[8:16], uint64(time.Now().UnixNano()))
		if i == 0 {
			firstChunkData = append([]byte(nil), data...)
		}
		if i == 1024 {
			lastChunkData = append([]byte(nil), data...)
		}
		_, _, _, _, err := vol.WriteFileWithEpoch(ctx, ino, int64(i*64*1024), data, pb.WriteMode_LAZY)
		if err != nil {
			t.Fatalf("WriteFile chunk %d failed: %v", i, err)
		}
		if i == int(blob.DefaultPackSlots)-1 {
			// At exactly DefaultPackSlots entries, pack 1 must be sealed eagerly and cleared.
			vol.stagingMu.Lock()
			activePackNil := vol.activePack == nil
			slotSealsAfter1024 := vol.slotSeals
			vol.stagingMu.Unlock()
			if !activePackNil {
				t.Fatalf("expected activePack to be nil immediately after filling slot cap (eager seal)")
			}
			if slotSealsAfter1024 != 1 {
				t.Fatalf("expected slotSeals=1 immediately after filling slot cap, got %d", slotSealsAfter1024)
			}
		}
	}

	stats := vol.Stats()
	if stats.StagingSlotSeals < 1 {
		t.Fatalf("expected at least 1 slot-cap seal, got %d", stats.StagingSlotSeals)
	}

	vol.stagingMu.Lock()
	packCount := len(vol.packs)
	activeID := vol.activePackID
	vol.stagingMu.Unlock()
	if packCount < 2 {
		t.Fatalf("expected at least 2 packs in staging, got %d", packCount)
	}
	if activeID < 2 {
		t.Fatalf("expected active pack ID >= 2 after rotation, got %d", activeID)
	}

	// Sync to ensure all sealed packs upload cleanly
	if err := vol.Fsync(ctx, ino); err != nil {
		t.Fatalf("Fsync failed: %v", err)
	}

	// Verify both first (from initial pack) and last chunk (from rotated pack) are readable after sync
	read0, err := testReadFile(ctx, server, volumeID, "/slot_test.bin", 0, 16)
	if err != nil || read0.GetError() != 0 {
		t.Fatalf("ReadFile chunk 0 failed: %v, resp=%v", err, read0)
	}
	if !bytes.Equal(read0.GetData(), firstChunkData) {
		t.Fatalf("chunk 0 data mismatch")
	}

	read1024, err := testReadFile(ctx, server, volumeID, "/slot_test.bin", 1024*64*1024, 16)
	if err != nil || read1024.GetError() != 0 {
		t.Fatalf("ReadFile chunk 1024 failed: %v, resp=%v", err, read1024)
	}
	if !bytes.Equal(read1024.GetData(), lastChunkData) {
		t.Fatalf("chunk 1024 data mismatch")
	}
}

func TestCloseToOpen_MultiChunkBarrierRepro(t *testing.T) {
	ctx := t.Context()
	rawBackend := NewMemoryBackend()
	backend := newRecordingBackend(rawBackend)
	server := NewTestServer(t, backend, WithServerMaxStagingBytes(64*1024*1024))
	volumeID := "test-c2o-repro-vol"

	volume, err := server.getOrCreateVolume(volumeID)
	if err != nil {
		t.Fatalf("getOrCreateVolume failed: %v", err)
	}
	volume.SetConsistencyMode(pb.ConsistencyMode_CLOSE_TO_OPEN)

	createResponse, err := testCreateFile(ctx, server, volumeID, "/multi_16m.bin", 0644, nil, 0, 0)
	if err != nil || createResponse.GetError() != 0 {
		t.Fatalf("CreateFile failed: %v", err)
	}
	inodeID := createResponse.GetAttr().GetInode().GetIno()

	// Initial open stream position before multi-chunk write
	initialOpenResponse, err := server.Open(ctx, &pb.OpenRequest{
		VolumeId: volumeID,
		Inode:    inodeID,
		Flags:    0,
	})
	if err != nil {
		t.Fatalf("Initial Open failed: %v", err)
	}
	initialPosition := initialOpenResponse.GetStreamPosition()

	// Write 16 MiB (multi-chunk)
	data := bytes.Repeat([]byte("X"), 16*1024*1024)
	for chunkIndex := 0; chunkIndex < 16; chunkIndex++ {
		if _, _, _, _, writeError := volume.WriteFileWithEpoch(ctx, inodeID, int64(chunkIndex*1024*1024), data[chunkIndex*1024*1024:(chunkIndex+1)*1024*1024], pb.WriteMode_FSYNC); writeError != nil {
			t.Fatalf("WriteFile chunk %d failed: %v", chunkIndex, writeError)
		}
	}

	// Gate the close sync inside the pack upload to deterministically test interleaving
	// with a concurrent open-barrier sync.
	gateEntered := make(chan struct{}, 1)
	gateRelease := make(chan struct{})
	backend.setGate(gateEntered, gateRelease)
	var gateReleaseOnce sync.Once
	t.Cleanup(func() {
		gateReleaseOnce.Do(func() {
			close(gateRelease)
		})
	})

	// Start close sync (blocking Fsync or close trigger)
	go func() {
		if _, fsyncError := server.Fsync(ctx, &pb.FsyncRequest{
			VolumeId: volumeID,
			Inode:    inodeID,
			Blocking: true,
			Trigger:  pb.FsyncRequest_SYNC_TRIGGER_CLOSE,
		}); fsyncError != nil {
			t.Errorf("Fsync failed: %v", fsyncError)
		}
	}()

	// Wait until close sync enters the upload phase and is held at the gate
	select {
	case <-gateEntered:
	case <-time.After(30 * time.Second):
		t.Fatalf("timed out waiting for close sync to enter upload gate")
	}

	// Run open barrier on reader while close sync is in-flight and held inside upload
	type openResult struct {
		response *pb.OpenResponse
		err      error
	}
	openChannel := make(chan openResult, 1)
	go func() {
		response, err := server.Open(ctx, &pb.OpenRequest{
			VolumeId: volumeID,
			Inode:    inodeID,
			Flags:    0,
		})
		openChannel <- openResult{response: response, err: err}
	}()

	// Verify that open barrier does not return while close sync is held
	select {
	case result := <-openChannel:
		t.Fatalf("openBarrier did not wait while close sync was in flight: %+v, err=%v", result.response, result.err)
	case <-time.After(100 * time.Millisecond):
		// Expected: open barrier is blocked waiting for in-flight sync to complete
	}

	// Release the close sync upload gate
	gateReleaseOnce.Do(func() {
		close(gateRelease)
	})

	// Open barrier must now unblock and return successfully
	var result openResult
	select {
	case result = <-openChannel:
	case <-time.After(30 * time.Second):
		t.Fatalf("openBarrier failed to complete after releasing close sync")
	}

	if result.err != nil {
		t.Fatalf("Open failed: %v", result.err)
	}
	openResponse := result.response
	t.Logf("OpenResponse: err=%d, size=%d, pos=%d", openResponse.GetError(), openResponse.GetAttr().GetInode().GetSize(), openResponse.GetStreamPosition())
	if openResponse.GetError() != 0 {
		t.Fatalf("Open returned error: %d", openResponse.GetError())
	}
	if openResponse.GetAttr().GetInode().GetSize() != 16*1024*1024 {
		t.Fatalf("Expected size %d, got %d", 16*1024*1024, openResponse.GetAttr().GetInode().GetSize())
	}
	if openResponse.GetStreamPosition() <= initialPosition {
		t.Fatalf("Expected stream position to advance from %d, got %d", initialPosition, openResponse.GetStreamPosition())
	}

	// Verify chunk rows committed and readable
	readResponse, err := server.ReadFile(ctx, &pb.ReadFileRequest{
		VolumeId: volumeID,
		Inode:    inodeID,
		Offset:   0,
		Size:     1024,
	})
	if err != nil || readResponse.GetError() != 0 {
		t.Fatalf("ReadFile failed: %v, code=%d", err, readResponse.GetError())
	}
	if !bytes.Equal(readResponse.GetData(), data[:1024]) {
		t.Fatalf("ReadFile content mismatch on committed multi-chunk file")
	}
}

func TestVolume_Stop_TimesOutAndIsRetryable(t *testing.T) {
	ctx := t.Context()
	memoryBackend := NewMemoryBackend()
	backend := newRecordingBackend(memoryBackend)
	server := NewTestServer(t, backend, WithServerSyncTimeout(50*time.Millisecond))
	volumeID := "test-stop-timeout-vol"

	volume, err := server.getOrCreateVolume(volumeID)
	if err != nil {
		t.Fatalf("getOrCreateVolume failed: %v", err)
	}

	createResponse, err := testCreateFile(ctx, server, volumeID, "/test.bin", 0644, nil, 0, 0)
	if err != nil || createResponse.GetError() != 0 {
		t.Fatalf("CreateFile failed: %v", err)
	}
	inodeID := createResponse.GetAttr().GetInode().GetIno()

	data := []byte("staged write data")
	if _, _, _, _, writeError := volume.WriteFileWithEpoch(ctx, inodeID, 0, data, pb.WriteMode_FSYNC); writeError != nil {
		t.Fatalf("WriteFile failed: %v", writeError)
	}

	gateEntered := make(chan struct{}, 1)
	gateRelease := make(chan struct{})
	backend.setGate(gateEntered, gateRelease)
	var gateReleaseOnce sync.Once
	t.Cleanup(func() {
		gateReleaseOnce.Do(func() {
			close(gateRelease)
		})
	})

	syncDone := make(chan error, 1)
	go func() {
		syncDone <- volume.Fsync(ctx, inodeID)
	}()

	select {
	case <-gateEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for pack upload to reach storage gate")
	}

	stopCtx, stopCancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer stopCancel()

	stopDone := make(chan error, 1)
	go func() {
		stopDone <- volume.Stop(stopCtx)
	}()

	var stopError error
	select {
	case stopError = <-stopDone:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for volume.Stop")
	}

	if !errors.Is(stopError, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got: %v", stopError)
	}

	if _, _, _, _, writeError := volume.WriteFileWithEpoch(ctx, inodeID, 0, []byte("post-stop data"), pb.WriteMode_FSYNC); !errors.Is(writeError, ErrVolumeStopping) {
		t.Fatalf("expected ErrVolumeStopping on post-stop write, got: %v", writeError)
	}

	gateReleaseOnce.Do(func() {
		close(gateRelease)
	})

	select {
	case syncErr := <-syncDone:
		if syncErr != nil {
			t.Fatalf("in-flight sync failed after release: %v", syncErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("sync goroutine left parked after backend release")
	}

	secondStopCtx, secondStopCancel := context.WithTimeout(ctx, 5*time.Second)
	defer secondStopCancel()
	if secondStopError := volume.Stop(secondStopCtx); secondStopError != nil {
		t.Fatalf("expected second Stop to return nil, got: %v", secondStopError)
	}

	if closeError := volume.Close(); closeError != nil {
		t.Fatalf("expected Close to return nil, got: %v", closeError)
	}
}

func TestVolume_Close_WithoutStopCancelsParkedSync(t *testing.T) {
	ctx := t.Context()
	memoryBackend := NewMemoryBackend()
	backend := newRecordingBackend(memoryBackend)
	server := NewTestServer(t, backend, WithServerSyncTimeout(50*time.Millisecond))
	volumeID := "test-close-without-stop-vol"

	volume, err := server.getOrCreateVolume(volumeID)
	if err != nil {
		t.Fatalf("getOrCreateVolume failed: %v", err)
	}

	createResponse, err := testCreateFile(ctx, server, volumeID, "/test.bin", 0644, nil, 0, 0)
	if err != nil || createResponse.GetError() != 0 {
		t.Fatalf("CreateFile failed: %v", err)
	}
	inodeID := createResponse.GetAttr().GetInode().GetIno()

	data := []byte("staged write data")
	if _, _, _, _, writeError := volume.WriteFileWithEpoch(ctx, inodeID, 0, data, pb.WriteMode_FSYNC); writeError != nil {
		t.Fatalf("WriteFile failed: %v", writeError)
	}

	gateEntered := make(chan struct{}, 1)
	gateRelease := make(chan struct{})
	backend.setGate(gateEntered, gateRelease)
	var gateReleaseOnce sync.Once
	t.Cleanup(func() {
		gateReleaseOnce.Do(func() {
			close(gateRelease)
		})
	})

	syncDone := make(chan error, 1)
	go func() {
		syncDone <- volume.Fsync(ctx, inodeID)
	}()

	select {
	case <-gateEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for pack upload to reach storage gate")
	}

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- volume.Close()
	}()

	var closeError error
	select {
	case closeError = <-closeDone:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for volume.Close")
	}
	if closeError != nil {
		t.Fatalf("expected Close to return nil, got: %v", closeError)
	}

	gateReleaseOnce.Do(func() {
		close(gateRelease)
	})

	select {
	case syncErr := <-syncDone:
		if syncErr == nil {
			t.Fatal("expected parked sync to fail after Close, got nil")
		}
		if !errors.Is(syncErr, context.Canceled) && !strings.Contains(syncErr.Error(), "closed") {
			t.Fatalf("expected cancellation or closed error, got: %v", syncErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("sync goroutine left parked after Close")
	}

	volume.stagingMu.Lock()
	armed := volume.coalesceTimer != nil
	volume.stagingMu.Unlock()
	if armed {
		t.Fatal("coalesceTimer was re-armed on closed volume")
	}

	if _, statErr := os.Stat(volume.stagingDir); !os.IsNotExist(statErr) {
		t.Fatalf("staging directory still exists or reappeared after close: %v", statErr)
	}
}

func TestVolume_Stop_FlushesStagedWrites(t *testing.T) {
	ctx := t.Context()
	memoryBackend := NewMemoryBackend()
	backend := newRecordingBackend(memoryBackend)
	server := NewTestServer(t, backend)
	volumeID := "test-stop-flushes-vol"

	volume, err := server.getOrCreateVolume(volumeID)
	if err != nil {
		t.Fatalf("getOrCreateVolume failed: %v", err)
	}

	createResponse, err := testCreateFile(ctx, server, volumeID, "/test.bin", 0644, nil, 0, 0)
	if err != nil || createResponse.GetError() != 0 {
		t.Fatalf("CreateFile failed: %v", err)
	}
	inodeID := createResponse.GetAttr().GetInode().GetIno()

	data := []byte("staged write data that is flushed by Stop")
	if _, _, _, _, writeError := volume.WriteFileWithEpoch(ctx, inodeID, 0, data, pb.WriteMode_FSYNC); writeError != nil {
		t.Fatalf("WriteFile failed: %v", writeError)
	}

	if count := backend.getPackCount(); count != 0 {
		t.Fatalf("expected 0 uploaded packs before Stop, got %d", count)
	}

	stopCtx, stopCancel := context.WithTimeout(ctx, 5*time.Second)
	defer stopCancel()
	if stopError := volume.Stop(stopCtx); stopError != nil {
		t.Fatalf("Stop failed: %v", stopError)
	}

	if count := backend.getPackCount(); count == 0 {
		t.Fatal("expected at least 1 uploaded pack after Stop, got 0")
	}

	readResponse, err := server.ReadFile(ctx, &pb.ReadFileRequest{
		VolumeId: volumeID,
		Inode:    inodeID,
		Offset:   0,
		Size:     int64(len(data)),
	})
	if err != nil || readResponse.GetError() != 0 {
		t.Fatalf("ReadFile failed after Stop: %v, code=%d", err, readResponse.GetError())
	}
	if !bytes.Equal(readResponse.GetData(), data) {
		t.Fatalf("ReadFile content mismatch: got %q, want %q", string(readResponse.GetData()), string(data))
	}
}

func TestVolume_Stop_FailsWhenDrainFails(t *testing.T) {
	ctx := t.Context()
	memoryBackend := NewMemoryBackend()
	backend := newRecordingBackend(memoryBackend)
	server := NewTestServer(t, backend)
	volumeID := "test-stop-drain-fail-vol"

	volume, err := server.getOrCreateVolume(volumeID)
	if err != nil {
		t.Fatalf("getOrCreateVolume failed: %v", err)
	}

	createResponse, err := testCreateFile(ctx, server, volumeID, "/test.bin", 0644, nil, 0, 0)
	if err != nil || createResponse.GetError() != 0 {
		t.Fatalf("CreateFile failed: %v", err)
	}
	inodeID := createResponse.GetAttr().GetInode().GetIno()

	data := []byte("staged write data that will fail to drain")
	if _, _, _, _, writeError := volume.WriteFileWithEpoch(ctx, inodeID, 0, data, pb.WriteMode_FSYNC); writeError != nil {
		t.Fatalf("WriteFile failed: %v", writeError)
	}

	backend.mu.Lock()
	backend.failPut = true
	backend.mu.Unlock()

	stopCtx, stopCancel := context.WithTimeout(ctx, 5*time.Second)
	defer stopCancel()
	stopError := volume.Stop(stopCtx)

	backend.mu.Lock()
	backend.failPut = false
	backend.mu.Unlock()

	if stopError == nil {
		t.Fatal("expected Stop to fail when drain fails, got nil")
	}
	if !strings.Contains(stopError.Error(), "failed to drain") {
		t.Fatalf("expected error containing 'failed to drain', got: %v", stopError)
	}
}

func TestVolume_Stop_DoneContextReturnsErrorWithoutSync(t *testing.T) {
	ctx := t.Context()
	memoryBackend := NewMemoryBackend()
	backend := newRecordingBackend(memoryBackend)
	server := NewTestServer(t, backend)
	volumeID := "test-stop-done-ctx-vol"

	volume, err := server.getOrCreateVolume(volumeID)
	if err != nil {
		t.Fatalf("getOrCreateVolume failed: %v", err)
	}

	createResponse, err := testCreateFile(ctx, server, volumeID, "/test.bin", 0644, nil, 0, 0)
	if err != nil || createResponse.GetError() != 0 {
		t.Fatalf("CreateFile failed: %v", err)
	}
	inodeID := createResponse.GetAttr().GetInode().GetIno()

	data := []byte("staged write data")
	if _, _, _, _, writeError := volume.WriteFileWithEpoch(ctx, inodeID, 0, data, pb.WriteMode_FSYNC); writeError != nil {
		t.Fatalf("WriteFile failed: %v", writeError)
	}

	doneCtx, doneCancel := context.WithCancel(ctx)
	doneCancel()

	stopErr := volume.Stop(doneCtx)
	if !errors.Is(stopErr, context.Canceled) {
		t.Fatalf("expected Stop with done context to return error wrapping context.Canceled, got: %v", stopErr)
	}

	volume.stagingMu.Lock()
	stagedCount := len(volume.stagedChunks)
	syncInProgress := volume.syncInProgress
	stopping := volume.stopping
	volume.stagingMu.Unlock()

	if syncInProgress {
		t.Fatal("expected no sync in progress after Stop with done context")
	}
	if stopping {
		t.Fatal("expected volume not marked stopping after Stop with done context")
	}
	if stagedCount == 0 {
		t.Fatal("expected staged chunks to remain staged after Stop with done context")
	}
	if backend.getPackCount() != 0 {
		t.Fatalf("expected 0 packs uploaded to backend, got %d", backend.getPackCount())
	}
}

// TestVolume_LockOrder_CreateFileAndFsyncStress is a stress test verifying that
// Volume.mu and Volume.stagingMu maintain strict lock ordering and never deadlock.
// Goroutine A loops CreateFile (acquiring v.mu), while goroutine B loops blocking
// Fsync on an inode with nothing staged (taking stagingMu, then Position() which
// takes v.mu.RLock()). Main calls Stop after a few hundred iterations and then Close.
// The stress body runs in a background goroutine bounded by a 30s select on the test
// goroutine, failing loudly via t.Fatal if a deadlock hangs execution.
func TestVolume_LockOrder_CreateFileAndFsyncStress(t *testing.T) {
	ctx := t.Context()
	memoryBackend := NewMemoryBackend()
	server := NewTestServer(t, memoryBackend)
	volumeID := "test-lock-order-stress-vol"

	volume, err := server.getOrCreateVolume(volumeID)
	if err != nil {
		t.Fatalf("getOrCreateVolume failed: %v", err)
	}

	createResp, err := testCreateFile(ctx, server, volumeID, "/base.txt", 0644, nil, 0, 0)
	if err != nil || createResp.GetError() != 0 {
		t.Fatalf("CreateFile failed: %v", err)
	}
	baseInode := createResp.GetAttr().GetInode().GetIno()

	var errMu sync.Mutex
	var firstErr error
	recordErr := func(e error) {
		errMu.Lock()
		defer errMu.Unlock()
		if firstErr == nil {
			firstErr = e
		}
	}

	done := make(chan struct{})
	go func() {
		defer close(done)

		var wg sync.WaitGroup
		stopSignal := make(chan struct{})
		var iterations atomic.Int64

		// Goroutine A: loops CreateFile
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stopSignal:
					return
				default:
				}
				name := fmt.Sprintf("/stress_%d.txt", i)
				_, createErr := testCreateFile(ctx, server, volumeID, name, 0644, nil, 0, 0)
				if createErr != nil && status.Code(createErr) != codes.Unavailable && !errors.Is(createErr, ErrVolumeStopping) {
					recordErr(fmt.Errorf("CreateFile unexpected error: %w", createErr))
				}
				iterations.Add(1)
			}
		}()

		// Goroutine B: loops blocking Fsync on an inode with nothing staged (the Position() path)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stopSignal:
					return
				default:
				}
				if fsyncErr := volume.Fsync(ctx, baseInode); fsyncErr != nil && !errors.Is(fsyncErr, ErrVolumeStopping) {
					recordErr(fmt.Errorf("Fsync unexpected error: %w", fsyncErr))
				}
			}
		}()

		// Wait until goroutine A passes a few hundred iterations
		for iterations.Load() < 300 {
			time.Sleep(time.Millisecond)
		}

		stopCtx, stopCancel := context.WithTimeout(ctx, 5*time.Second)
		defer stopCancel()
		if stopErr := volume.Stop(stopCtx); stopErr != nil {
			recordErr(fmt.Errorf("volume.Stop failed: %w", stopErr))
		}

		close(stopSignal)
		wg.Wait()

		if closeErr := volume.Close(); closeErr != nil {
			recordErr(fmt.Errorf("volume.Close failed: %w", closeErr))
		}
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("hang guard: deadlock in CreateFile and Fsync lock ordering")
	}

	if firstErr != nil {
		t.Errorf("stress test error: %v", firstErr)
	}
}
