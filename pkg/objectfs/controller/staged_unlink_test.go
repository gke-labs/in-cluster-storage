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
	"errors"
	"syscall"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
)

func newStagedDropVolume(t *testing.T, backend ObjectStorageBackend) (*Server, *Volume) {
	t.Helper()
	server := NewTestServer(t, backend)
	volume, err := server.getOrCreateVolume("test-vol")
	if err != nil {
		t.Fatalf("getOrCreateVolume failed: %v", err)
	}
	return server, volume
}

func createFileHandle(t *testing.T, server *Server, path string) (uint64, uint64) {
	t.Helper()
	response, err := testCreateFile(t.Context(), server, "test-vol", path, 0644, nil, 0, 0)
	if err != nil || response.GetError() != 0 {
		t.Fatalf("CreateFile(%s) failed: err=%v code=%d", path, err, response.GetError())
	}
	return response.GetAttr().GetInode().GetIno(), response.GetFh()
}

func createClosedFile(t *testing.T, server *Server, volume *Volume, path string) uint64 {
	t.Helper()
	inodeID, fileHandle := createFileHandle(t, server, path)
	if err := volume.Release(t.Context(), inodeID, fileHandle); err != nil {
		t.Fatalf("Release(%s) failed: %v", path, err)
	}
	return inodeID
}

func stageWrite(t *testing.T, volume *Volume, inodeID uint64, payload []byte) {
	t.Helper()
	if _, _, _, err := volume.WriteFile(t.Context(), inodeID, 0, payload, pb.WriteMode_FSYNC); err != nil {
		t.Fatalf("WriteFile(inode=%d) failed: %v", inodeID, err)
	}
}

func TestStagedChunks_UnlinkWhileOpenPreservesData(t *testing.T) {
	ctx := t.Context()
	server, volume := newStagedDropVolume(t, NewMemoryBackend())
	payload := []byte("hello-open")
	inodeID, fileHandle := createFileHandle(t, server, "/open.bin")
	stageWrite(t, volume, inodeID, payload)

	if _, err := volume.Unlink(ctx, &pb.UnlinkRequest{ParentInode: 1, Name: "open.bin"}); err != nil {
		t.Fatalf("Unlink failed: %v", err)
	}
	if err := volume.Fsync(ctx, inodeID); err != nil {
		t.Fatalf("Fsync failed: %v", err)
	}
	data, _, _, err := volume.ReadFile(ctx, inodeID, 0, int64(len(payload)))
	if err != nil || !bytes.Equal(data, payload) {
		t.Fatalf("ReadFile = (%q, %v), want %q", string(data), err, string(payload))
	}
	if err := volume.Release(ctx, inodeID, fileHandle); err != nil {
		t.Fatalf("Release failed: %v", err)
	}
	if staged := volume.StagedBytes(); staged != 0 {
		t.Fatalf("StagedBytes = %d, want 0", staged)
	}
}

func TestStagedChunks_UnlinkWithHardLinkPreservesData(t *testing.T) {
	ctx := t.Context()
	server, volume := newStagedDropVolume(t, NewMemoryBackend())
	payload := []byte("hello-link")
	inodeID := createClosedFile(t, server, volume, "/link1.bin")
	if _, err := volume.Link(ctx, &pb.LinkRequest{OldInode: inodeID, NewParentInode: 1, NewName: "link2.bin"}); err != nil {
		t.Fatalf("Link failed: %v", err)
	}
	stageWrite(t, volume, inodeID, payload)

	if _, err := volume.Unlink(ctx, &pb.UnlinkRequest{ParentInode: 1, Name: "link1.bin"}); err != nil {
		t.Fatalf("Unlink failed: %v", err)
	}
	if err := volume.Fsync(ctx, inodeID); err != nil {
		t.Fatalf("Fsync failed: %v", err)
	}
	readResponse, err := testReadFile(ctx, server, "test-vol", "/link2.bin", 0, int64(len(payload)))
	if err != nil || readResponse.GetError() != 0 || !bytes.Equal(readResponse.GetData(), payload) {
		t.Fatalf("ReadFile(/link2.bin) = (%q, code=%d, err=%v), want %q", string(readResponse.GetData()), readResponse.GetError(), err, string(payload))
	}
}

func TestStagedChunks_UnlinkLastLinkNoOpenHandleDropsData(t *testing.T) {
	ctx := t.Context()
	server, volume := newStagedDropVolume(t, NewMemoryBackend())
	inodeID := createClosedFile(t, server, volume, "/last.bin")
	stageWrite(t, volume, inodeID, []byte("last-data"))
	if volume.StagedBytes() == 0 {
		t.Fatal("expected non-zero staged bytes before unlink")
	}

	if _, err := volume.Unlink(ctx, &pb.UnlinkRequest{ParentInode: 1, Name: "last.bin"}); err != nil {
		t.Fatalf("Unlink failed: %v", err)
	}
	if staged := volume.StagedBytes(); staged != 0 {
		t.Fatalf("StagedBytes = %d, want 0", staged)
	}
	if err := volume.Fsync(ctx, 1); err != nil {
		t.Fatalf("whole-volume Fsync failed: %v", err)
	}
}

func TestStagedChunks_ReleaseDeletedOrphanDropsData(t *testing.T) {
	ctx := t.Context()
	server, volume := newStagedDropVolume(t, NewMemoryBackend())
	inodeID, fileHandle := createFileHandle(t, server, "/orphan.bin")
	stageWrite(t, volume, inodeID, []byte("orphan-data"))

	if _, err := volume.Unlink(ctx, &pb.UnlinkRequest{ParentInode: 1, Name: "orphan.bin"}); err != nil {
		t.Fatalf("Unlink failed: %v", err)
	}
	if volume.StagedBytes() == 0 {
		t.Fatal("expected staged bytes preserved on open unlinked orphan")
	}
	if err := volume.Release(ctx, inodeID, fileHandle); err != nil {
		t.Fatalf("Release failed: %v", err)
	}
	if staged := volume.StagedBytes(); staged != 0 {
		t.Fatalf("StagedBytes = %d, want 0", staged)
	}
	if err := volume.Fsync(ctx, 1); err != nil {
		t.Fatalf("whole-volume Fsync failed: %v", err)
	}
}

func TestStagedChunks_RenameOverStagedTargetDropsData(t *testing.T) {
	ctx := t.Context()
	server, volume := newStagedDropVolume(t, NewMemoryBackend())
	targetInodeID := createClosedFile(t, server, volume, "/target.bin")
	stageWrite(t, volume, targetInodeID, []byte("old-target"))
	if volume.StagedBytes() == 0 {
		t.Fatal("expected non-zero staged bytes on target before rename")
	}

	createClosedFile(t, server, volume, "/src.bin")
	if _, err := volume.Rename(ctx, &pb.RenameRequest{OldParentInode: 1, OldName: "src.bin", NewParentInode: 1, NewName: "target.bin"}); err != nil {
		t.Fatalf("Rename failed: %v", err)
	}
	if staged := volume.StagedBytes(); staged != 0 {
		t.Fatalf("StagedBytes = %d, want 0", staged)
	}
	if err := volume.Fsync(ctx, 1); err != nil {
		t.Fatalf("whole-volume Fsync failed: %v", err)
	}
}

func TestStagedChunks_SyncSkipsInodeDeletedDuringUpload(t *testing.T) {
	ctx := t.Context()
	backend := newRecordingBackend(NewMemoryBackend())
	server, volume := newStagedDropVolume(t, backend)

	victimInodeID := createClosedFile(t, server, volume, "/victim.bin")
	survivorInodeID := createClosedFile(t, server, volume, "/survivor.bin")
	stageWrite(t, volume, victimInodeID, []byte("victim"))
	survivorPayload := []byte("survivor")
	stageWrite(t, volume, survivorInodeID, survivorPayload)

	gateEntered := make(chan struct{}, 1)
	gateRelease := make(chan struct{})
	backend.setGate(gateEntered, gateRelease)

	syncDone := make(chan error, 1)
	go func() {
		syncDone <- volume.Fsync(ctx, survivorInodeID)
	}()

	select {
	case <-gateEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for pack upload gate")
	}

	if _, err := volume.Unlink(ctx, &pb.UnlinkRequest{ParentInode: 1, Name: "victim.bin"}); err != nil {
		t.Fatalf("Unlink victim failed: %v", err)
	}
	close(gateRelease)

	if err := waitSyncDone(t, syncDone); err != nil {
		t.Fatalf("Fsync failed when another staged inode was deleted during upload: %v", err)
	}
	if staged := volume.StagedBytes(); staged != 0 {
		t.Fatalf("StagedBytes = %d, want 0", staged)
	}
	data, _, _, err := volume.ReadFile(ctx, survivorInodeID, 0, int64(len(survivorPayload)))
	if err != nil || !bytes.Equal(data, survivorPayload) {
		t.Fatalf("ReadFile survivor = (%q, %v), want %q", string(data), err, string(survivorPayload))
	}
}

func TestRelease_RepeatedUnknownHandleIsNoOp(t *testing.T) {
	ctx := t.Context()
	server, volume := newStagedDropVolume(t, NewMemoryBackend())
	inodeID, firstHandle := createFileHandle(t, server, "/repeated-release.bin")
	secondHandle, _, _, err := volume.Open(ctx, inodeID, 0)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	initialPayload := bytes.Repeat([]byte("a"), 8192)
	stageWrite(t, volume, inodeID, initialPayload)
	stagedBefore := volume.StagedBytes()
	if stagedBefore == 0 {
		t.Fatal("expected non-zero staged bytes before unlink")
	}

	if _, err := volume.Unlink(ctx, &pb.UnlinkRequest{ParentInode: 1, Name: "repeated-release.bin"}); err != nil {
		t.Fatalf("Unlink failed: %v", err)
	}
	if err := volume.Release(ctx, inodeID, firstHandle); err != nil {
		t.Fatalf("first Release(handle 1) failed: %v", err)
	}
	if err := volume.Release(ctx, inodeID, firstHandle); err != nil {
		t.Fatalf("repeated Release(handle 1) failed: %v", err)
	}

	if staged := volume.StagedBytes(); staged != stagedBefore {
		t.Fatalf("StagedBytes after repeated Release = %d, want %d", staged, stagedBefore)
	}
	appendedPayload := []byte("more-data")
	if _, _, _, err := volume.WriteFile(ctx, inodeID, int64(len(initialPayload)), appendedPayload, pb.WriteMode_FSYNC); err != nil {
		t.Fatalf("WriteFile after repeated Release failed: %v", err)
	}
	if err := volume.Fsync(ctx, inodeID); err != nil {
		t.Fatalf("Fsync after repeated Release failed: %v", err)
	}
	expected := append(append([]byte(nil), initialPayload...), appendedPayload...)
	data, _, _, err := volume.ReadFile(ctx, inodeID, 0, int64(len(expected)))
	if err != nil || !bytes.Equal(data, expected) {
		t.Fatalf("ReadFile after repeated Release = (%d bytes, %v), want %d bytes", len(data), err, len(expected))
	}

	if err := volume.Release(ctx, inodeID, secondHandle); err != nil {
		t.Fatalf("Release(handle 2) failed: %v", err)
	}
	if _, err := volume.GetAttr(ctx, inodeID); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("GetAttr after releasing last handle = %v, want ENOENT", err)
	}
}

func TestRelease_PreRestartHandleDoesNotCloseReusedHandle(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()

	firstServer, _ := newStagedDropVolume(t, backend)
	oldInode, oldHandle := createFileHandle(t, firstServer, "/before-restart.bin")
	if err := firstServer.FlushAll(ctx); err != nil {
		t.Fatalf("FlushAll failed: %v", err)
	}
	if err := firstServer.Close(); err != nil {
		t.Fatalf("firstServer.Close failed: %v", err)
	}

	secondServer, volume := newStagedDropVolume(t, backend)
	newInode, newHandle := createFileHandle(t, secondServer, "/after-restart.bin")
	if newHandle != oldHandle || newInode == oldInode {
		t.Fatalf("want reused handle %d on distinct inode, got handle=%d inode=%d (old inode=%d)", oldHandle, newHandle, newInode, oldInode)
	}

	payload := []byte("still-open")
	stageWrite(t, volume, newInode, payload)
	stagedBefore := volume.StagedBytes()
	if stagedBefore == 0 {
		t.Fatal("expected non-zero staged bytes before unlink")
	}
	if _, err := volume.Unlink(ctx, &pb.UnlinkRequest{ParentInode: 1, Name: "after-restart.bin"}); err != nil {
		t.Fatalf("Unlink failed: %v", err)
	}

	if err := volume.Release(ctx, oldInode, oldHandle); err != nil {
		t.Fatalf("stale Release failed: %v", err)
	}
	if staged := volume.StagedBytes(); staged != stagedBefore {
		t.Fatalf("StagedBytes after stale Release = %d, want %d", staged, stagedBefore)
	}
	if err := volume.Fsync(ctx, newInode); err != nil {
		t.Fatalf("Fsync after stale Release failed: %v", err)
	}
	data, _, _, err := volume.ReadFile(ctx, newInode, 0, int64(len(payload)))
	if err != nil || !bytes.Equal(data, payload) {
		t.Fatalf("ReadFile after stale Release = (%q, %v), want %q", string(data), err, string(payload))
	}

	if err := volume.Release(ctx, newInode, newHandle); err != nil {
		t.Fatalf("Release(newHandle) failed: %v", err)
	}
	if _, err := volume.GetAttr(ctx, newInode); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("GetAttr after releasing new handle = %v, want ENOENT", err)
	}
}
