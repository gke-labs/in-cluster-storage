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
	"errors"
	"syscall"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/wal/buffer/buffertest"
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
)

func TestDirEntryMatchesRequest(t *testing.T) {
	keyA, keyB := []byte("key-a"), []byte("key-b")
	entry := &pb.DirEntry{Mode: syscall.S_IFREG | 0644, RequestId: keyA}
	noKey := &pb.DirEntry{Mode: syscall.S_IFREG | 0644}
	if !dirEntryMatchesRequest(entry, keyA) {
		t.Error("matching entry and key should match")
	}
	if dirEntryMatchesRequest(entry, keyB) || dirEntryMatchesRequest(entry, nil) ||
		dirEntryMatchesRequest(noKey, nil) || dirEntryMatchesRequest(noKey, keyA) ||
		dirEntryMatchesRequest(nil, keyA) {
		t.Error("mismatched key, empty key, or nil entry must not match")
	}
}

func TestRenameIdempotency_AcrossSnapshotReload(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	bufferServer, cleanup := buffertest.Start(ctx, t, buffertest.WithBackend(backend))
	t.Cleanup(cleanup)

	const volumeID = "test-rename-idempotency"
	keyA, keyB := []byte("rename-key-a"), []byte("rename-key-b")
	newController := func() *Server {
		return NewTestServer(t, backend, WithServerWAL(t.TempDir(), bufferServer.Target, walclient.Witness),
			WithServerWALClient(bufferServer.Client), WithServerWALDialOptions(bufferServer.DialOpts...), WithServerMetadataDir(t.TempDir()))
	}

	server1 := newController()
	var parentInodes [2]uint64
	for i, name := range []string{"source", "destination"} {
		dir, err := server1.Mkdir(ctx, &pb.MkdirRequest{VolumeId: volumeID, ParentInode: 1, Name: name, Mode: 0755})
		if err != nil || dir.GetError() != 0 {
			t.Fatalf("Mkdir %s: err=%v errno=%d", name, err, dir.GetError())
		}
		parentInodes[i] = dir.GetAttr().GetInode().GetIno()
	}
	created, err := server1.CreateFile(ctx, &pb.CreateFileRequest{VolumeId: volumeID, ParentInode: parentInodes[0], Name: "file.txt", Mode: 0644})
	if err != nil || created.GetError() != 0 {
		t.Fatalf("CreateFile: err=%v errno=%d", err, created.GetError())
	}

	request := &pb.RenameRequest{VolumeId: volumeID, OldParentInode: parentInodes[0], OldName: "file.txt", NewParentInode: parentInodes[1], NewName: "moved.txt", RequestId: keyA}
	first, err := server1.Rename(ctx, request)
	if err != nil || first.GetError() != 0 || first.GetAttr().GetInode().GetIno() != created.GetAttr().GetInode().GetIno() {
		t.Fatalf("initial Rename: err=%v errno=%d got=%d want=%d", err, first.GetError(), first.GetAttr().GetInode().GetIno(), created.GetAttr().GetInode().GetIno())
	}
	parentMtimes := func(volume *Volume) [2]time.Time {
		t.Helper()
		var mtimes [2]time.Time
		for i, ino := range parentInodes {
			attr, err := volume.GetAttr(ctx, ino)
			if err != nil {
				t.Fatalf("GetAttr parent %d: %v", ino, err)
			}
			mtimes[i] = attr.GetInode().GetMtime().AsTime()
		}
		return mtimes
	}
	volume1 := mustGetVolume(t, server1, volumeID)
	mtimesBefore, positionBefore := parentMtimes(volume1), volume1.Position()

	assertRepeatUnchanged := func(label string, server *Server, wantPosition uint64) *Volume {
		t.Helper()
		repeat, err := server.Rename(ctx, request)
		if err != nil || repeat.GetError() != 0 || repeat.GetAttr().GetInode().GetIno() != first.GetAttr().GetInode().GetIno() || repeat.GetAttr().GetName() != "moved.txt" {
			t.Fatalf("%s repeat Rename: err=%v errno=%d got=(%d,%q) want=(%d,%q)", label, err, repeat.GetError(), repeat.GetAttr().GetInode().GetIno(), repeat.GetAttr().GetName(), first.GetAttr().GetInode().GetIno(), "moved.txt")
		}
		volume := mustGetVolume(t, server, volumeID)
		if volume.Position() != wantPosition {
			t.Fatalf("%s repeat Rename wrote stream record: position=%d want=%d", label, volume.Position(), wantPosition)
		}
		if mtimesAfter := parentMtimes(volume); !mtimesAfter[0].Equal(mtimesBefore[0]) || !mtimesAfter[1].Equal(mtimesBefore[1]) {
			t.Fatalf("%s repeat Rename mutated parent mtimes: before=%v after=%v", label, mtimesBefore, mtimesAfter)
		}
		return volume
	}

	assertRepeatUnchanged("same controller", server1, positionBefore)
	if _, err := server1.CreateSnapshot(ctx, &pb.CreateSnapshotRequest{VolumeId: volumeID}); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	positionAfterSnapshot := volume1.Position()
	if err := server1.Close(); err != nil {
		t.Fatalf("server1.Close: %v", err)
	}

	server2 := newController()
	if volume2 := assertRepeatUnchanged("after snapshot reload", server2, positionAfterSnapshot); volume2.replaySnapshotLocation == "" {
		t.Fatal("expected second controller to load from snapshot")
	}
	for _, requestID := range [][]byte{keyB, nil} {
		if response, err := server2.Rename(ctx, &pb.RenameRequest{VolumeId: volumeID, OldParentInode: parentInodes[0], OldName: "file.txt", NewParentInode: parentInodes[1], NewName: "moved.txt", RequestId: requestID}); err != nil || response.GetError() != int32(syscall.ENOENT) {
			t.Fatalf("Rename non-matching key: err=%v errno=%d, want ENOENT", err, response.GetError())
		}
	}
}

func TestRenameIdempotency_RepeatWaitsForDurability(t *testing.T) {
	ctx := t.Context()
	stream := &recordingStream{}
	server, volume := newMetadataDurabilityTestServer(t, stream, pb.MetadataDurability_OPERATION)

	created, err := server.CreateFile(ctx, &pb.CreateFileRequest{VolumeId: volume.volumeID, ParentInode: 1, Name: "before.txt", Mode: 0644})
	if err != nil || created.GetError() != 0 {
		t.Fatalf("CreateFile: err=%v errno=%d", err, created.GetError())
	}

	request := &pb.RenameRequest{VolumeId: volume.volumeID, OldParentInode: 1, OldName: "before.txt", NewParentInode: 1, NewName: "after.txt", RequestId: []byte("rename-durability-key-1")}
	stream.setWaitError(errors.New("wait interrupted"))
	if response, err := server.Rename(ctx, request); err == nil && response.GetError() == 0 {
		t.Fatal("expected initial Rename to fail when wait fails")
	}
	if response, err := server.Rename(ctx, request); err == nil && response.GetError() == 0 {
		t.Fatal("expected repeat Rename to fail while durability wait still fails")
	}
	appends, commitSequence := stream.appendCount.Load(), volume.Position()
	stream.setWaitError(nil)
	stream.waitCount.Store(0)
	stream.lastWaitSequence.Store(0)
	repeat, err := server.Rename(ctx, request)
	if err != nil || repeat.GetError() != 0 || repeat.GetAttr().GetInode().GetIno() != created.GetAttr().GetInode().GetIno() {
		t.Fatalf("repeat Rename: err=%v errno=%d got=%d want=%d", err, repeat.GetError(), repeat.GetAttr().GetInode().GetIno(), created.GetAttr().GetInode().GetIno())
	}
	if got := stream.waitCount.Load(); stream.appendCount.Load() != appends || got != 1 || stream.lastWaitSequence.Load() < commitSequence {
		t.Fatalf("repeat Rename durability wait: appends=%d(want %d) count=%d(want 1) sequence=%d(want >=%d)",
			stream.appendCount.Load(), appends, got, stream.lastWaitSequence.Load(), commitSequence)
	}
}
