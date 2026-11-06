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
	"context"
	"encoding/binary"
	"strings"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/blob"
)

func waitForPackState(t *testing.T, vol *Volume, packID uint64, want packState) {
	t.Helper()
	vol.stagingMu.Lock()
	defer vol.stagingMu.Unlock()
	deadlineTimer := time.AfterFunc(5*time.Second, func() {
		vol.stagingMu.Lock()
		vol.syncCond.Broadcast()
		vol.stagingMu.Unlock()
	})
	defer deadlineTimer.Stop()

	deadline := time.Now().Add(5 * time.Second)
	for {
		info, ok := vol.packs[packID]
		if ok && info.state == want {
			break
		}
		if time.Now().After(deadline) {
			var got packState
			if ok {
				got = info.state
			}
			t.Fatalf("timed out waiting for pack %d to reach state %v, current state %v", packID, want, got)
		}
		vol.syncCond.Wait()
	}
}

func fillAndSealPack(t *testing.T, vol *Volume, ctx context.Context, inodeID uint64) {
	t.Helper()
	for i := 0; i < int(blob.DefaultPackSlots); i++ {
		data := make([]byte, 16)
		binary.BigEndian.PutUint64(data[0:8], uint64(i))
		binary.BigEndian.PutUint64(data[8:16], uint64(i+1))
		offset := int64(i) * 64 * 1024
		if _, _, _, _, err := vol.WriteFileWithEpoch(ctx, inodeID, offset, data, pb.WriteMode_LAZY); err != nil {
			t.Fatalf("WriteFileWithEpoch chunk %d failed: %v", i, err)
		}
	}
}

func TestSync_WaitsForInFlightUpload(t *testing.T) {
	ctx := t.Context()
	gateEntered := make(chan struct{}, 1)
	gateRelease := make(chan struct{})
	backend := newRecordingBackend(NewMemoryBackend())
	backend.setGate(gateEntered, gateRelease)
	server := NewTestServer(t, backend)
	volumeID := "test-sync-wait-vol"
	vol, err := server.getOrCreateVolume(volumeID)
	if err != nil {
		t.Fatalf("getOrCreateVolume failed: %v", err)
	}

	createResponse, err := testCreateFile(ctx, server, volumeID, "/test.bin", 0644, nil, 0, 0)
	if err != nil || createResponse.GetError() != 0 {
		t.Fatalf("CreateFile failed: %v", err)
	}
	inodeID := createResponse.GetAttr().GetInode().GetIno()

	fillAndSealPack(t, vol, ctx, inodeID)

	select {
	case <-gateEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for background upload to enter gate")
	}

	vol.stagingMu.Lock()
	state := vol.packs[1].state
	vol.stagingMu.Unlock()
	if state != packUploading {
		t.Fatalf("expected pack 1 state packUploading, got %v", state)
	}

	syncDone := make(chan error, 1)
	go func() {
		syncDone <- vol.Fsync(ctx, inodeID)
	}()

	select {
	case syncErr := <-syncDone:
		t.Fatalf("Fsync returned prematurely while upload was in-flight: %v", syncErr)
	case <-time.After(50 * time.Millisecond):
	}

	close(gateRelease)

	select {
	case syncErr := <-syncDone:
		if syncErr != nil {
			t.Fatalf("Fsync failed after release: %v", syncErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Fsync to finish after release")
	}

	waitForPackState(t, vol, 1, packUploaded)
}

func TestSync_FailedBackgroundUploadRetriedBySync(t *testing.T) {
	ctx := t.Context()

	t.Run("retry succeeds", func(t *testing.T) {
		backend := newRecordingBackend(NewMemoryBackend())
		server := NewTestServer(t, backend)
		volumeID := "test-fail-retry-success-vol"
		vol, err := server.getOrCreateVolume(volumeID)
		if err != nil {
			t.Fatalf("getOrCreateVolume failed: %v", err)
		}

		createResponse, err := testCreateFile(ctx, server, volumeID, "/retry.bin", 0644, nil, 0, 0)
		if err != nil || createResponse.GetError() != 0 {
			t.Fatalf("CreateFile failed: %v", err)
		}
		inodeID := createResponse.GetAttr().GetInode().GetIno()

		for i := 0; i < int(blob.DefaultPackSlots)-1; i++ {
			data := make([]byte, 16)
			binary.BigEndian.PutUint64(data[0:8], uint64(i))
			binary.BigEndian.PutUint64(data[8:16], uint64(i+1))
			if _, _, _, _, err := vol.WriteFileWithEpoch(ctx, inodeID, int64(i*64*1024), data, pb.WriteMode_LAZY); err != nil {
				t.Fatalf("WriteFileWithEpoch chunk %d failed: %v", i, err)
			}
		}

		backend.mu.Lock()
		backend.failPut = true
		backend.mu.Unlock()

		lastChunk := make([]byte, 16)
		binary.BigEndian.PutUint64(lastChunk[0:8], uint64(1023))
		binary.BigEndian.PutUint64(lastChunk[8:16], uint64(1024))
		if _, _, _, _, err := vol.WriteFileWithEpoch(ctx, inodeID, int64(1023*64*1024), lastChunk, pb.WriteMode_LAZY); err != nil {
			t.Fatalf("WriteFileWithEpoch final chunk failed: %v", err)
		}

		waitForPackState(t, vol, 1, packFailed)

		vol.stagingMu.Lock()
		packError := vol.packs[1].uploadError
		vol.stagingMu.Unlock()
		if packError == nil {
			t.Fatal("expected pack uploadError to be recorded, got nil")
		}

		backend.mu.Lock()
		backend.failPut = false
		backend.mu.Unlock()

		if err := vol.Fsync(ctx, inodeID); err != nil {
			t.Fatalf("Fsync failed to retry and succeed: %v", err)
		}

		waitForPackState(t, vol, 1, packUploaded)

		vol.stagingMu.Lock()
		clearedError := vol.packs[1].uploadError
		vol.stagingMu.Unlock()
		if clearedError != nil {
			t.Fatalf("expected pack uploadError to be cleared, got %v", clearedError)
		}
	})

	t.Run("retry fails and surfaces", func(t *testing.T) {
		backend := newRecordingBackend(NewMemoryBackend())
		server := NewTestServer(t, backend)
		volumeID := "test-fail-retry-surfaces-vol"
		vol, err := server.getOrCreateVolume(volumeID)
		if err != nil {
			t.Fatalf("getOrCreateVolume failed: %v", err)
		}

		createResponse, err := testCreateFile(ctx, server, volumeID, "/retry_fail.bin", 0644, nil, 0, 0)
		if err != nil || createResponse.GetError() != 0 {
			t.Fatalf("CreateFile failed: %v", err)
		}
		inodeID := createResponse.GetAttr().GetInode().GetIno()

		for i := 0; i < int(blob.DefaultPackSlots)-1; i++ {
			data := make([]byte, 16)
			binary.BigEndian.PutUint64(data[0:8], uint64(i))
			binary.BigEndian.PutUint64(data[8:16], uint64(i+1))
			if _, _, _, _, err := vol.WriteFileWithEpoch(ctx, inodeID, int64(i*64*1024), data, pb.WriteMode_LAZY); err != nil {
				t.Fatalf("WriteFileWithEpoch chunk %d failed: %v", i, err)
			}
		}

		backend.mu.Lock()
		backend.failPut = true
		backend.mu.Unlock()

		lastChunk := make([]byte, 16)
		binary.BigEndian.PutUint64(lastChunk[0:8], uint64(1023))
		binary.BigEndian.PutUint64(lastChunk[8:16], uint64(1024))
		if _, _, _, _, err := vol.WriteFileWithEpoch(ctx, inodeID, int64(1023*64*1024), lastChunk, pb.WriteMode_LAZY); err != nil {
			t.Fatalf("WriteFileWithEpoch final chunk failed: %v", err)
		}

		waitForPackState(t, vol, 1, packFailed)

		err = vol.Fsync(ctx, inodeID)
		if err == nil {
			t.Fatal("expected Fsync to fail when retry fails, got nil")
		}
		if !strings.Contains(err.Error(), "injected PutObject failure") {
			t.Fatalf("expected error containing injected PutObject failure, got %v", err)
		}

		backend.mu.Lock()
		backend.failPut = false
		backend.mu.Unlock()
	})
}

func TestEviction_SkipsUploadingPacks(t *testing.T) {
	ctx := t.Context()
	gateEntered := make(chan struct{}, 1)
	gateRelease := make(chan struct{})
	backend := newRecordingBackend(NewMemoryBackend())
	backend.setGate(gateEntered, gateRelease)
	server := NewTestServer(t, backend)
	volumeID := "test-evict-skip-vol"
	vol, err := server.getOrCreateVolume(volumeID)
	if err != nil {
		t.Fatalf("getOrCreateVolume failed: %v", err)
	}

	createResponse, err := testCreateFile(ctx, server, volumeID, "/evict.bin", 0644, nil, 0, 0)
	if err != nil || createResponse.GetError() != 0 {
		t.Fatalf("CreateFile failed: %v", err)
	}
	inodeID := createResponse.GetAttr().GetInode().GetIno()

	fillAndSealPack(t, vol, ctx, inodeID)

	select {
	case <-gateEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for background upload to enter gate")
	}

	vol.stagingMu.Lock()
	if vol.packs[1].state != packUploading {
		vol.stagingMu.Unlock()
		t.Fatalf("expected pack 1 state packUploading, got %v", vol.packs[1].state)
	}
	vol.packs[1].liveReferences = 0
	vol.evictUploadedPacksLocked(100 * 1024 * 1024)
	_, stillExists := vol.packs[1]
	vol.stagingMu.Unlock()

	if !stillExists {
		t.Fatal("expected pack 1 in packUploading to not be evicted")
	}

	close(gateRelease)
	waitForPackState(t, vol, 1, packUploaded)

	vol.stagingMu.Lock()
	vol.evictUploadedPacksLocked(100 * 1024 * 1024)
	_, existsAfterUpload := vol.packs[1]
	vol.stagingMu.Unlock()

	if existsAfterUpload {
		t.Fatal("expected pack 1 in packUploaded with 0 references to be evicted")
	}
}

func TestSync_DoesNotPayForPacksSealedEarlier(t *testing.T) {
	ctx := t.Context()
	backend := newRecordingBackend(NewMemoryBackend())
	server := NewTestServer(t, backend)
	volumeID := "test-sync-early-sealed-vol"
	vol, err := server.getOrCreateVolume(volumeID)
	if err != nil {
		t.Fatalf("getOrCreateVolume failed: %v", err)
	}

	createResponse, err := testCreateFile(ctx, server, volumeID, "/early.bin", 0644, nil, 0, 0)
	if err != nil || createResponse.GetError() != 0 {
		t.Fatalf("CreateFile failed: %v", err)
	}
	inodeID := createResponse.GetAttr().GetInode().GetIno()

	fillAndSealPack(t, vol, ctx, inodeID)

	waitForPackState(t, vol, 1, packUploaded)

	putsBeforeFsync := backend.getPackCount()
	if putsBeforeFsync != 1 {
		t.Fatalf("expected 1 pack uploaded before Fsync, got %d", putsBeforeFsync)
	}

	syncStartTime := time.Now()
	if err := vol.Fsync(ctx, inodeID); err != nil {
		t.Fatalf("Fsync failed: %v", err)
	}
	syncDuration := time.Since(syncStartTime)
	t.Logf("Fsync completed in %v", syncDuration)

	putsAfterFsync := backend.getPackCount()
	if putsAfterFsync != putsBeforeFsync {
		t.Fatalf("expected no blob Put to occur during Fsync (got %d puts, want %d)", putsAfterFsync, putsBeforeFsync)
	}
}
