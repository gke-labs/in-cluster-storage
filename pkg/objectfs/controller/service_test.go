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
	"encoding/json"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	walpb "github.com/gke-labs/in-cluster-storage/pkg/api/wal/v1alpha1"
	walbuffer "github.com/gke-labs/in-cluster-storage/pkg/wal/buffer"
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestControllerServiceOperations(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewServer(backend)
	volumeID := "test-vol-1"

	// 1. Root attribute
	rootAttr, err := server.GetAttr(ctx, &pb.GetAttrRequest{
		VolumeId: volumeID,
		Path:     "/",
	})
	if err != nil {
		t.Fatalf("Failed to get root attr: %v", err)
	}
	if !rootAttr.Attr.IsDir {
		t.Fatalf("Expected root to be directory")
	}

	// 2. Mkdir
	mkdirResp, err := server.Mkdir(ctx, &pb.MkdirRequest{
		VolumeId: volumeID,
		Path:     "/subdir",
		Mode:     0755,
	})
	if err != nil {
		t.Fatalf("Failed to mkdir /subdir: %v", err)
	}
	if !mkdirResp.Attr.IsDir || mkdirResp.Attr.Name != "subdir" {
		t.Fatalf("Unexpected mkdir attr: %v", mkdirResp.Attr)
	}

	// 3. Create file
	createResp, err := server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:       volumeID,
		Path:           "/subdir/hello.txt",
		Mode:           0644,
		InitialContent: []byte("initial content"),
	})
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	if createResp.Attr.Size != int64(len("initial content")) {
		t.Fatalf("Unexpected file size: %d", createResp.Attr.Size)
	}

	// 4. Lookup
	lookupResp, err := server.Lookup(ctx, &pb.LookupRequest{
		VolumeId:   volumeID,
		ParentPath: "/subdir",
		Name:       "hello.txt",
	})
	if err != nil {
		t.Fatalf("Failed to lookup: %v", err)
	}
	if lookupResp.Attr.Path != "/subdir/hello.txt" {
		t.Fatalf("Unexpected path in lookup: %s", lookupResp.Attr.Path)
	}

	// 5. Read file
	readResp, err := server.ReadFile(ctx, &pb.ReadFileRequest{
		VolumeId: volumeID,
		Path:     "/subdir/hello.txt",
		Offset:   0,
		Size:     1024,
	})
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}
	if string(readResp.Data) != "initial content" {
		t.Fatalf("Unexpected data: %s", string(readResp.Data))
	}

	// 6. Write file
	writeResp, err := server.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId:  volumeID,
		Path:      "/subdir/hello.txt",
		Offset:    int64(len("initial ")),
		Data:      []byte("objectfs!"),
		WriteMode: pb.WriteMode_WRITE_THROUGH_FSYNC,
	})
	if err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}
	if writeResp.NewSize != int64(len("initial objectfs!")) {
		t.Fatalf("Unexpected new size: %d", writeResp.NewSize)
	}

	// Read back modified
	readResp2, err := server.ReadFile(ctx, &pb.ReadFileRequest{
		VolumeId: volumeID,
		Path:     "/subdir/hello.txt",
		Offset:   0,
		Size:     1024,
	})
	if err != nil {
		t.Fatalf("Failed to read modified file: %v", err)
	}
	if string(readResp2.Data) != "initial objectfs!" {
		t.Fatalf("Unexpected content after write: %s", string(readResp2.Data))
	}

	// 7. ReadDir
	readdirResp, err := server.ReadDir(ctx, &pb.ReadDirRequest{
		VolumeId: volumeID,
		Path:     "/subdir",
	})
	if err != nil {
		t.Fatalf("Failed to readdir: %v", err)
	}
	if len(readdirResp.Entries) != 1 || readdirResp.Entries[0].Name != "hello.txt" {
		t.Fatalf("Unexpected readdir entries: %v", readdirResp.Entries)
	}

	// 8. Rename
	renameResp, err := server.Rename(ctx, &pb.RenameRequest{
		VolumeId: volumeID,
		OldPath:  "/subdir/hello.txt",
		NewPath:  "/subdir/renamed.txt",
	})
	if err != nil {
		t.Fatalf("Failed to rename: %v", err)
	}
	if renameResp.Attr.Name != "renamed.txt" {
		t.Fatalf("Unexpected rename attr: %v", renameResp.Attr)
	}

	// Verify old path not found
	_, err = server.GetAttr(ctx, &pb.GetAttrRequest{
		VolumeId: volumeID,
		Path:     "/subdir/hello.txt",
	})
	if err == nil {
		t.Fatalf("Expected old path to not exist after rename")
	}

	// 9. Truncate
	truncResp, err := server.TruncateFile(ctx, &pb.TruncateFileRequest{
		VolumeId: volumeID,
		Path:     "/subdir/renamed.txt",
		Size:     7,
	})
	if err != nil {
		t.Fatalf("Failed to truncate: %v", err)
	}
	if truncResp.Attr.Size != 7 {
		t.Fatalf("Expected size 7 after truncate, got %d", truncResp.Attr.Size)
	}

	// 10. Unlink & Rmdir
	_, err = server.Unlink(ctx, &pb.UnlinkRequest{
		VolumeId: volumeID,
		Path:     "/subdir/renamed.txt",
	})
	if err != nil {
		t.Fatalf("Failed to unlink: %v", err)
	}

	_, err = server.Rmdir(ctx, &pb.RmdirRequest{
		VolumeId: volumeID,
		Path:     "/subdir",
	})
	if err != nil {
		t.Fatalf("Failed to rmdir: %v", err)
	}

	// Verify subdir gone
	_, err = server.GetAttr(ctx, &pb.GetAttrRequest{
		VolumeId: volumeID,
		Path:     "/subdir",
	})
	if err == nil {
		t.Fatalf("Expected subdir to not exist after rmdir")
	}
}

func TestBackendPeriodicAndIncrementalFlush(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewServer(backend)
	volumeID := "test-flush-vol"

	// Create files
	_, err := server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:       volumeID,
		Path:           "/file1.txt",
		Mode:           0644,
		InitialContent: []byte("file 1 initial data"),
	})
	if err != nil {
		t.Fatalf("Failed to create file1: %v", err)
	}

	_, err = server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:       volumeID,
		Path:           "/file2.txt",
		Mode:           0644,
		InitialContent: []byte("file 2 initial data"),
	})
	if err != nil {
		t.Fatalf("Failed to create file2: %v", err)
	}

	// Before flush, backend should not have the raw objects
	var dummyBuf bytes.Buffer
	if err := backend.GetObject(ctx, volumeID, "file1.txt", 0, 0, &dummyBuf); err == nil {
		t.Fatalf("Expected backend to not have file1 before flush")
	}

	// Flush to backend
	if err := server.FlushAll(ctx); err != nil {
		t.Fatalf("FlushAll failed: %v", err)
	}

	// Verify raw objects and metadata file exist in backend
	var f1Buf bytes.Buffer
	err = backend.GetObject(ctx, volumeID, "file1.txt", 0, 100, &f1Buf)
	f1Data := f1Buf.Bytes()
	if err != nil || string(f1Data) != "file 1 initial data" {
		t.Fatalf("Expected file1 in backend with initial data, got: %q, err: %v", string(f1Data), err)
	}

	var metaBuf bytes.Buffer
	err = backend.GetObject(ctx, volumeID, MetadataFileName, 0, 0, &metaBuf)
	metaBytes := metaBuf.Bytes()
	if err != nil || len(metaBytes) == 0 {
		t.Fatalf("Expected metadata file in backend, got err: %v", err)
	}

	var meta VolumeMetadata
	if err := json.Unmarshal(metaBytes, &meta); err != nil {
		t.Fatalf("Failed to unmarshal metadata: %v", err)
	}
	if len(meta.Entries) != 3 { // root /, /file1.txt, /file2.txt
		t.Fatalf("Expected 3 entries in metadata, got %d", len(meta.Entries))
	}
	if meta.Entries["/file1.txt"].Size != int64(len("file 1 initial data")) {
		t.Fatalf("Unexpected file1 metadata size: %d", meta.Entries["/file1.txt"].Size)
	}

	// Incremental write: modify only file2
	_, err = server.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId: volumeID,
		Path:     "/file2.txt",
		Offset:   0,
		Data:     []byte("file 2 updated content!"),
	})
	if err != nil {
		t.Fatalf("Failed to update file2: %v", err)
	}

	// Unlink file1
	_, err = server.Unlink(ctx, &pb.UnlinkRequest{
		VolumeId: volumeID,
		Path:     "/file1.txt",
	})
	if err != nil {
		t.Fatalf("Failed to unlink file1: %v", err)
	}

	// Flush again
	if err := server.FlushAll(ctx); err != nil {
		t.Fatalf("Second FlushAll failed: %v", err)
	}

	// Verify file2 updated and file1 deleted in backend
	var f2Buf bytes.Buffer
	err = backend.GetObject(ctx, volumeID, "file2.txt", 0, 100, &f2Buf)
	f2Data := f2Buf.Bytes()
	if err != nil || string(f2Data) != "file 2 updated content!" {
		t.Fatalf("Expected updated file2 in backend, got: %q, err: %v", string(f2Data), err)
	}

	var f1DeletedBuf bytes.Buffer
	if err := backend.GetObject(ctx, volumeID, "file1.txt", 0, 100, &f1DeletedBuf); err == nil {
		t.Fatalf("Expected file1 to be deleted from backend after unlink & flush")
	}

	// Test Recovery / LoadFromBackend
	// Create a new server pointing to the same backend
	newServer := NewServer(backend)
	readResp, err := newServer.ReadFile(ctx, &pb.ReadFileRequest{
		VolumeId: volumeID,
		Path:     "/file2.txt",
		Offset:   0,
		Size:     100,
	})
	if err != nil {
		t.Fatalf("Failed to read file2 from recovered server: %v", err)
	}
	if string(readResp.GetData()) != "file 2 updated content!" {
		t.Fatalf("Expected recovered server to read 'file 2 updated content!', got: %q", string(readResp.GetData()))
	}
}

func TestPeriodicFlusherLifecycle(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewServer(backend)
	volumeID := "test-periodic-vol"

	server.StartPeriodicFlush(ctx, 10*time.Millisecond)

	_, err := server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:       volumeID,
		Path:           "/auto-flushed.txt",
		Mode:           0644,
		InitialContent: []byte("auto flushed data"),
	})
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	// Wait for periodic flusher to run
	time.Sleep(50 * time.Millisecond)

	server.StopPeriodicFlush()

	// Verify backend received the file
	var autoBuf bytes.Buffer
	err = backend.GetObject(ctx, volumeID, "auto-flushed.txt", 0, 100, &autoBuf)
	data := autoBuf.Bytes()
	if err != nil || string(data) != "auto flushed data" {
		t.Fatalf("Expected periodic flusher to sync auto-flushed.txt to backend, got %q (err=%v)", string(data), err)
	}
}

func TestControllerPushNotifications(t *testing.T) {
	ctx := t.Context()
	server := NewServer(nil)
	volumeID := "test-watch-vol"

	ch := server.broadcaster.Subscribe(volumeID)
	defer server.broadcaster.Unsubscribe(volumeID, ch)

	// Trigger create
	_, err := server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:       volumeID,
		Path:           "/event-test.txt",
		Mode:           0644,
		InitialContent: []byte("event data"),
	})
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	select {
	case ev := <-ch:
		if ev.EventType != pb.WatchEventType_EVENT_CREATED || ev.Path != "/event-test.txt" {
			t.Fatalf("Unexpected event received: %v", ev)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("Timed out waiting for push notification")
	}

	// Trigger modify
	_, err = server.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId: volumeID,
		Path:     "/event-test.txt",
		Offset:   0,
		Data:     []byte("more data"),
	})
	if err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}

	select {
	case ev := <-ch:
		if ev.EventType != pb.WatchEventType_EVENT_MODIFIED || ev.Path != "/event-test.txt" {
			t.Fatalf("Unexpected modify event: %v", ev)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("Timed out waiting for modify event")
	}
}

func TestEventBroadcasterSlowSubscriber(t *testing.T) {
	eb := NewEventBroadcaster()
	volumeID := "test-slow-sub"

	ch := eb.Subscribe(volumeID)
	defer eb.Unsubscribe(volumeID, ch)

	// Fill the buffer (128 items)
	for i := 0; i < 128; i++ {
		eb.Broadcast(volumeID, &pb.WatchVolumeResponse{
			EventType: pb.WatchEventType_EVENT_MODIFIED,
			Path:      "/file.txt",
		})
	}

	// Next broadcast should detect full channel and unsubscribe/close it asynchronously
	eb.Broadcast(volumeID, &pb.WatchVolumeResponse{
		EventType: pb.WatchEventType_EVENT_MODIFIED,
		Path:      "/overflow.txt",
	})

	// Drain items from ch until closed
	closed := false
	timeout := time.After(2 * time.Second)
	for !closed {
		select {
		case _, ok := <-ch:
			if !ok {
				closed = true
			}
		case <-timeout:
			t.Fatalf("Timed out waiting for full subscriber channel to be closed")
		}
	}
}

func TestErofsSnapshotCreationAndRecovery(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewServer(backend)
	volumeID := "test-erofs-snap-vol"

	// Create a nested directory hierarchy and files
	_, err := server.Mkdir(ctx, &pb.MkdirRequest{
		VolumeId: volumeID,
		Path:     "/data",
	})
	if err != nil {
		t.Fatalf("Mkdir /data failed: %v", err)
	}

	_, err = server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:       volumeID,
		Path:           "/data/file1.txt",
		Mode:           0644,
		InitialContent: []byte("file 1 content for snapshot"),
	})
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}

	_, err = server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:       volumeID,
		Path:           "/root-file.txt",
		Mode:           0644,
		InitialContent: []byte("root file content"),
	})
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}

	// Create snapshot
	snapResp, err := server.CreateSnapshot(ctx, &pb.CreateSnapshotRequest{
		VolumeId: volumeID,
	})
	if err != nil {
		t.Fatalf("CreateSnapshot failed: %v", err)
	}
	snapName := snapResp.GetSnapshotName()
	if snapName == "" || !strings.HasSuffix(snapName, ".erofs") {
		t.Fatalf("Expected .erofs snapshot name, got: %s", snapName)
	}

	// Verify backend object layout:
	// 1. volumes/<volumeID>/meta/<snapName> exists
	snapKey := "volumes/" + volumeID + "/meta/" + snapName
	var snapBuf bytes.Buffer
	err = backend.GetObject(ctx, "", snapKey, 0, 0, &snapBuf)
	snapBytes := snapBuf.Bytes()
	if err != nil || len(snapBytes) == 0 {
		t.Fatalf("Expected EROFS snapshot in backend at %s: %v", snapKey, err)
	}

	// 2. Blobs exist in blobs/ (packfiles or standalone)
	blobObjects, err := backend.ListObjects(ctx, "", "blobs/")
	if err != nil || len(blobObjects) == 0 {
		t.Fatalf("Expected blobs in backend under blobs/, got: %v (err=%v)", blobObjects, err)
	}

	// 3. List snapshots returns the snapshot
	listResp, err := server.ListSnapshots(ctx, &pb.ListSnapshotsRequest{
		VolumeId: volumeID,
	})
	if err != nil {
		t.Fatalf("ListSnapshots failed: %v", err)
	}
	if len(listResp.GetSnapshots()) != 1 || listResp.GetSnapshots()[0].GetName() != snapName {
		t.Fatalf("Expected snapshots [%s], got %v", snapName, listResp.GetSnapshots())
	}

	// 4. Test Recovery on a new Server instance using only the backend
	newServer := NewServer(backend)

	// Verify directory structure on recovered server
	dirResp, err := newServer.ReadDir(ctx, &pb.ReadDirRequest{
		VolumeId: volumeID,
		Path:     "/data",
	})
	if err != nil {
		t.Fatalf("Recovered server ReadDir /data failed: %v", err)
	}
	if len(dirResp.Entries) != 1 || dirResp.Entries[0].Name != "file1.txt" {
		t.Fatalf("Unexpected entries in recovered /data: %v", dirResp.Entries)
	}

	// Read content from recovered server (verifying lazy blob download)
	readResp, err := newServer.ReadFile(ctx, &pb.ReadFileRequest{
		VolumeId: volumeID,
		Path:     "/data/file1.txt",
		Offset:   0,
		Size:     1024,
	})
	if err != nil {
		t.Fatalf("Recovered server ReadFile failed: %v", err)
	}
	if string(readResp.Data) != "file 1 content for snapshot" {
		t.Fatalf("Recovered data mismatch: got %q", string(readResp.Data))
	}

	readRootResp, err := newServer.ReadFile(ctx, &pb.ReadFileRequest{
		VolumeId: volumeID,
		Path:     "/root-file.txt",
		Offset:   0,
		Size:     1024,
	})
	if err != nil {
		t.Fatalf("Recovered server ReadFile root-file failed: %v", err)
	}
	if string(readRootResp.Data) != "root file content" {
		t.Fatalf("Recovered root data mismatch: got %q", string(readRootResp.Data))
	}
}

func TestErofsSnapshotRollback(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewServer(backend)
	volumeID := "test-rollback-vol"

	// State 1: create v1
	_, err := server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:       volumeID,
		Path:           "/doc.txt",
		Mode:           0644,
		InitialContent: []byte("version 1 data"),
	})
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}

	snapResp1, err := server.CreateSnapshot(ctx, &pb.CreateSnapshotRequest{
		VolumeId: volumeID,
	})
	if err != nil {
		t.Fatalf("CreateSnapshot 1 failed: %v", err)
	}
	snap1 := snapResp1.GetSnapshotName()

	time.Sleep(10 * time.Millisecond)

	// State 2: modify doc.txt and add doc2.txt
	_, err = server.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId: volumeID,
		Path:     "/doc.txt",
		Offset:   0,
		Data:     []byte("version 2 data overwritten"),
	})
	if err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	_, err = server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:       volumeID,
		Path:           "/doc2.txt",
		Mode:           0644,
		InitialContent: []byte("version 2 second document"),
	})
	if err != nil {
		t.Fatalf("CreateFile doc2 failed: %v", err)
	}

	snapResp2, err := server.CreateSnapshot(ctx, &pb.CreateSnapshotRequest{
		VolumeId: volumeID,
	})
	if err != nil {
		t.Fatalf("CreateSnapshot 2 failed: %v", err)
	}
	snap2 := snapResp2.GetSnapshotName()

	// Verify 2 snapshots listed
	listResp, err := server.ListSnapshots(ctx, &pb.ListSnapshotsRequest{
		VolumeId: volumeID,
	})
	if err != nil {
		t.Fatalf("ListSnapshots failed: %v", err)
	}
	if len(listResp.GetSnapshots()) < 2 {
		t.Fatalf("Expected at least 2 snapshots, got %d", len(listResp.GetSnapshots()))
	}

	// Roll back to snap1
	if err := server.RestoreSnapshot(ctx, volumeID, snap1); err != nil {
		t.Fatalf("RestoreSnapshot to %s failed: %v", snap1, err)
	}

	// Read /doc.txt -> should be version 1 data
	readResp, err := server.ReadFile(ctx, &pb.ReadFileRequest{
		VolumeId: volumeID,
		Path:     "/doc.txt",
		Offset:   0,
		Size:     1024,
	})
	if err != nil {
		t.Fatalf("ReadFile after rollback failed: %v", err)
	}
	if string(readResp.Data) != "version 1 data" {
		t.Fatalf("Expected 'version 1 data' after rollback, got %q", string(readResp.Data))
	}

	// /doc2.txt should not exist
	_, err = server.GetAttr(ctx, &pb.GetAttrRequest{
		VolumeId: volumeID,
		Path:     "/doc2.txt",
	})
	if err == nil {
		t.Fatalf("Expected /doc2.txt to not exist after rollback to snap1")
	}

	// Now roll forward to snap2
	if err := server.RestoreSnapshot(ctx, volumeID, snap2); err != nil {
		t.Fatalf("RestoreSnapshot to %s failed: %v", snap2, err)
	}

	readResp2, err := server.ReadFile(ctx, &pb.ReadFileRequest{
		VolumeId: volumeID,
		Path:     "/doc.txt",
		Offset:   0,
		Size:     1024,
	})
	if err != nil {
		t.Fatalf("ReadFile after restore to snap2 failed: %v", err)
	}
	if string(readResp2.Data) != "version 2 data overwritten" {
		t.Fatalf("Expected 'version 2 data overwritten' after restore to snap2, got %q", string(readResp2.Data))
	}
}

func TestCSIControllerOperations(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewServer(backend)
	csiController := NewCSIController(server)

	// 1. GetPluginInfo
	pluginInfo, err := csiController.GetPluginInfo(ctx, &csi.GetPluginInfoRequest{})
	if err != nil {
		t.Fatalf("GetPluginInfo failed: %v", err)
	}
	if pluginInfo.GetName() != "objectfs.labs.gke.io" {
		t.Fatalf("Expected plugin name objectfs.labs.gke.io, got %s", pluginInfo.GetName())
	}

	// 2. GetPluginCapabilities
	pluginCaps, err := csiController.GetPluginCapabilities(ctx, &csi.GetPluginCapabilitiesRequest{})
	if err != nil {
		t.Fatalf("GetPluginCapabilities failed: %v", err)
	}
	if len(pluginCaps.GetCapabilities()) == 0 {
		t.Fatalf("Expected plugin capabilities, got none")
	}

	// 3. Probe
	if _, err := csiController.Probe(ctx, &csi.ProbeRequest{}); err != nil {
		t.Fatalf("Probe failed: %v", err)
	}

	// 4. ControllerGetCapabilities
	ctrlCaps, err := csiController.ControllerGetCapabilities(ctx, &csi.ControllerGetCapabilitiesRequest{})
	if err != nil {
		t.Fatalf("ControllerGetCapabilities failed: %v", err)
	}
	hasCreateDelete := false
	for _, cap := range ctrlCaps.GetCapabilities() {
		if cap.GetRpc().GetType() == csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME {
			hasCreateDelete = true
		}
	}
	if !hasCreateDelete {
		t.Fatalf("Expected CREATE_DELETE_VOLUME capability")
	}

	// 5. CreateVolume
	createVolResp, err := csiController.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name: "test-pvc-volume-1",
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 5 * 1024 * 1024 * 1024,
		},
		VolumeCapabilities: []*csi.VolumeCapability{
			{
				AccessMode: &csi.VolumeCapability_AccessMode{
					Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
				},
			},
		},
		Parameters: map[string]string{
			"writeMode": "lazy",
		},
	})
	if err != nil {
		t.Fatalf("CreateVolume failed: %v", err)
	}
	if createVolResp.GetVolume().GetVolumeId() != "test-pvc-volume-1" {
		t.Fatalf("Unexpected volume ID: %s", createVolResp.GetVolume().GetVolumeId())
	}
	if createVolResp.GetVolume().GetCapacityBytes() != 5*1024*1024*1024 {
		t.Fatalf("Unexpected capacity bytes: %d", createVolResp.GetVolume().GetCapacityBytes())
	}
	if createVolResp.GetVolume().GetVolumeContext()["writeMode"] != "lazy" {
		t.Fatalf("Unexpected volume context writeMode: %s", createVolResp.GetVolume().GetVolumeContext()["writeMode"])
	}

	// 6. ValidateVolumeCapabilities
	valResp, err := csiController.ValidateVolumeCapabilities(ctx, &csi.ValidateVolumeCapabilitiesRequest{
		VolumeId: "test-pvc-volume-1",
		VolumeCapabilities: []*csi.VolumeCapability{
			{
				AccessMode: &csi.VolumeCapability_AccessMode{
					Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("ValidateVolumeCapabilities failed: %v", err)
	}
	if valResp.GetConfirmed() == nil {
		t.Fatalf("Expected confirmed capabilities")
	}

	// 7. DeleteVolume
	if _, err := csiController.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "test-pvc-volume-1"}); err != nil {
		t.Fatalf("DeleteVolume failed: %v", err)
	}
}

type testGetBlobServer struct {
	pb.ObjectFSController_GetBlobServer
	ctx    context.Context
	chunks [][]byte
}

func (t *testGetBlobServer) Context() context.Context {
	return t.ctx
}

func (t *testGetBlobServer) Send(resp *pb.GetBlobResponse) error {
	t.chunks = append(t.chunks, resp.GetData())
	return nil
}

func TestControllerListBlobsAndGetBlob(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewServer(backend)
	volumeID := "test-blob-vol"

	content1 := []byte("blob data content number 1")
	content2 := []byte("blob data content number 2 - slightly longer test blob")

	_, err := server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:       volumeID,
		Path:           "/file1.txt",
		Mode:           0644,
		InitialContent: content1,
	})
	if err != nil {
		t.Fatalf("CreateFile file1 failed: %v", err)
	}

	_, err = server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:       volumeID,
		Path:           "/file2.txt",
		Mode:           0644,
		InitialContent: content2,
	})
	if err != nil {
		t.Fatalf("CreateFile file2 failed: %v", err)
	}

	// Flush volume to write blobs to blob store
	if err := server.FlushAll(ctx); err != nil {
		t.Fatalf("FlushAll failed: %v", err)
	}

	// 1. ListBlobs full
	listResp, err := server.ListBlobs(ctx, &pb.ListBlobsRequest{})
	if err != nil {
		t.Fatalf("ListBlobs failed: %v", err)
	}
	if len(listResp.GetSha256()) != 2 {
		t.Fatalf("Expected 2 blobs, got %d: %v", len(listResp.GetSha256()), listResp.GetSha256())
	}
	if !listResp.GetEndOfData() {
		t.Fatalf("Expected EndOfData to be true for full list")
	}

	// 2. ListBlobs pagination
	p1, err := server.ListBlobs(ctx, &pb.ListBlobsRequest{Limit: 1})
	if err != nil {
		t.Fatalf("ListBlobs page 1 failed: %v", err)
	}
	if len(p1.GetSha256()) != 1 || p1.GetEndOfData() {
		t.Fatalf("Expected 1 sha and EndOfData=false for page 1, got %v (EndOfData=%v)", p1.GetSha256(), p1.GetEndOfData())
	}

	p2, err := server.ListBlobs(ctx, &pb.ListBlobsRequest{FromSha: p1.GetSha256()[0], Limit: 1})
	if err != nil {
		t.Fatalf("ListBlobs page 2 failed: %v", err)
	}
	if len(p2.GetSha256()) != 1 || !p2.GetEndOfData() {
		t.Fatalf("Expected 1 sha and EndOfData=true for page 2, got %v (EndOfData=%v)", p2.GetSha256(), p2.GetEndOfData())
	}

	// 3. ListBlobs prefix
	prefix := listResp.GetSha256()[0][:6]
	prefResp, err := server.ListBlobs(ctx, &pb.ListBlobsRequest{ShaPrefix: prefix})
	if err != nil {
		t.Fatalf("ListBlobs prefix failed: %v", err)
	}
	for _, sha := range prefResp.GetSha256() {
		if !strings.HasPrefix(sha, prefix) {
			t.Fatalf("Expected sha %s to start with %s", sha, prefix)
		}
	}

	// 4. GetBlob for both blobs
	for _, sha := range listResp.GetSha256() {
		stream := &testGetBlobServer{ctx: ctx}
		if err := server.GetBlob(&pb.GetBlobRequest{Sha256: sha}, stream); err != nil {
			t.Fatalf("GetBlob failed for sha %s: %v", sha, err)
		}
		var fullData []byte
		for _, chunk := range stream.chunks {
			fullData = append(fullData, chunk...)
		}
		if string(fullData) != string(content1) && string(fullData) != string(content2) {
			t.Fatalf("Unexpected blob content: %q", string(fullData))
		}
	}

	// 5. GetBlob with offset and limit
	sha0 := listResp.GetSha256()[0]
	streamPartial := &testGetBlobServer{ctx: ctx}
	if err := server.GetBlob(&pb.GetBlobRequest{
		Sha256: sha0,
		Offset: 5,
		Limit:  4,
	}, streamPartial); err != nil {
		t.Fatalf("GetBlob with offset/limit failed: %v", err)
	}
	var partialData []byte
	for _, chunk := range streamPartial.chunks {
		partialData = append(partialData, chunk...)
	}
	if len(partialData) != 4 {
		t.Fatalf("Expected 4 bytes, got %d (%q)", len(partialData), string(partialData))
	}

	// 6. GetBlob for non-existent blob
	streamNotFound := &testGetBlobServer{ctx: ctx}
	if err := server.GetBlob(&pb.GetBlobRequest{Sha256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}, streamNotFound); err == nil {
		t.Fatalf("Expected error for non-existent blob, got nil")
	}
}

func TestListVolumes(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewServer(backend)

	// 1. Empty server returns empty list
	resp, err := server.ListVolumes(ctx, &pb.ListVolumesRequest{})
	if err != nil {
		t.Fatalf("ListVolumes failed: %v", err)
	}
	if len(resp.GetVolumes()) != 0 || !resp.GetEndOfData() {
		t.Fatalf("Expected 0 volumes, got %v", resp.GetVolumes())
	}

	// 2. Create files in 3 volumes
	volNames := []string{"vol-c", "vol-a", "vol-b"}
	for _, v := range volNames {
		_, err := server.CreateFile(ctx, &pb.CreateFileRequest{
			VolumeId:       v,
			Path:           "/hello.txt",
			InitialContent: []byte("content for " + v),
		})
		if err != nil {
			t.Fatalf("CreateFile for %s failed: %v", v, err)
		}
	}

	// 3. List all volumes (should be sorted alphabetically: vol-a, vol-b, vol-c)
	resp, err = server.ListVolumes(ctx, &pb.ListVolumesRequest{})
	if err != nil {
		t.Fatalf("ListVolumes failed: %v", err)
	}
	if len(resp.GetVolumes()) != 3 {
		t.Fatalf("Expected 3 volumes, got %d", len(resp.GetVolumes()))
	}
	expected := []string{"vol-a", "vol-b", "vol-c"}
	for i, exp := range expected {
		if resp.GetVolumes()[i].GetVolumeId() != exp {
			t.Fatalf("Index %d: expected %s, got %s", i, exp, resp.GetVolumes()[i].GetVolumeId())
		}
	}

	// 4. List with pagination (limit 2)
	resp, err = server.ListVolumes(ctx, &pb.ListVolumesRequest{Limit: 2})
	if err != nil {
		t.Fatalf("ListVolumes with limit failed: %v", err)
	}
	if len(resp.GetVolumes()) != 2 || resp.GetEndOfData() {
		t.Fatalf("Expected 2 volumes and EndOfData=false, got %d volumes, EndOfData=%v", len(resp.GetVolumes()), resp.GetEndOfData())
	}

	// 5. List with from_volume_id
	resp, err = server.ListVolumes(ctx, &pb.ListVolumesRequest{FromVolumeId: "vol-a"})
	if err != nil {
		t.Fatalf("ListVolumes with FromVolumeId failed: %v", err)
	}
	if len(resp.GetVolumes()) != 2 || resp.GetVolumes()[0].GetVolumeId() != "vol-b" || resp.GetVolumes()[1].GetVolumeId() != "vol-c" {
		t.Fatalf("Unexpected volumes after vol-a: %v", resp.GetVolumes())
	}

	// 6. Flush vol-a and check backend discovery with a fresh Server instance
	if err := server.FlushAll(ctx); err != nil {
		t.Fatalf("FlushAll failed: %v", err)
	}
	recoveredServer := NewServer(backend)
	recResp, err := recoveredServer.ListVolumes(ctx, &pb.ListVolumesRequest{})
	if err != nil {
		t.Fatalf("recovered server ListVolumes failed: %v", err)
	}
	if len(recResp.GetVolumes()) != 3 {
		t.Fatalf("Expected 3 volumes discovered in backend, got %d: %v", len(recResp.GetVolumes()), recResp.GetVolumes())
	}
}

func TestSnapshotsServicePagination(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewServer(backend)

	// 1. Validation errors
	if _, err := server.CreateSnapshot(ctx, &pb.CreateSnapshotRequest{}); err == nil {
		t.Fatalf("Expected error for empty volume_id on CreateSnapshot")
	}
	if _, err := server.ListSnapshots(ctx, &pb.ListSnapshotsRequest{}); err == nil {
		t.Fatalf("Expected error for empty volume_id on ListSnapshots")
	}

	volumeID := "test-snap-pag"

	// 2. Create snapshot 1
	_, err := server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:       volumeID,
		Path:           "/file1.txt",
		InitialContent: []byte("snap 1 content"),
	})
	if err != nil {
		t.Fatalf("CreateFile 1 failed: %v", err)
	}

	snapResp1, err := server.CreateSnapshot(ctx, &pb.CreateSnapshotRequest{VolumeId: volumeID})
	if err != nil {
		t.Fatalf("CreateSnapshot 1 failed: %v", err)
	}
	snap1 := snapResp1.GetSnapshotName()
	if snapResp1.GetSnapshot().GetCreatedAt() == nil {
		t.Fatalf("Expected non-nil CreatedAt in SnapshotInfo")
	}

	time.Sleep(15 * time.Millisecond)

	// Create snapshot 2
	_, err = server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:       volumeID,
		Path:           "/file2.txt",
		InitialContent: []byte("snap 2 content"),
	})
	if err != nil {
		t.Fatalf("CreateFile 2 failed: %v", err)
	}

	snapResp2, err := server.CreateSnapshot(ctx, &pb.CreateSnapshotRequest{VolumeId: volumeID})
	if err != nil {
		t.Fatalf("CreateSnapshot 2 failed: %v", err)
	}
	snap2 := snapResp2.GetSnapshotName()

	time.Sleep(15 * time.Millisecond)

	// Create snapshot 3
	_, err = server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:       volumeID,
		Path:           "/file3.txt",
		InitialContent: []byte("snap 3 content"),
	})
	if err != nil {
		t.Fatalf("CreateFile 3 failed: %v", err)
	}

	snapResp3, err := server.CreateSnapshot(ctx, &pb.CreateSnapshotRequest{VolumeId: volumeID})
	if err != nil {
		t.Fatalf("CreateSnapshot 3 failed: %v", err)
	}
	snap3 := snapResp3.GetSnapshotName()

	// 3. List all 3
	listResp, err := server.ListSnapshots(ctx, &pb.ListSnapshotsRequest{VolumeId: volumeID})
	if err != nil {
		t.Fatalf("ListSnapshots failed: %v", err)
	}
	if len(listResp.GetSnapshots()) != 3 || !listResp.GetEndOfData() {
		t.Fatalf("Expected 3 snapshots, got %d (endOfData=%v)", len(listResp.GetSnapshots()), listResp.GetEndOfData())
	}
	if listResp.GetSnapshots()[0].GetName() != snap1 ||
		listResp.GetSnapshots()[1].GetName() != snap2 ||
		listResp.GetSnapshots()[2].GetName() != snap3 {
		t.Fatalf("Snapshots not in chronological order: %v", listResp.GetSnapshots())
	}

	// 4. List with Limit: 2
	limResp, err := server.ListSnapshots(ctx, &pb.ListSnapshotsRequest{VolumeId: volumeID, Limit: 2})
	if err != nil {
		t.Fatalf("ListSnapshots with limit failed: %v", err)
	}
	if len(limResp.GetSnapshots()) != 2 || limResp.GetEndOfData() {
		t.Fatalf("Expected 2 snapshots with EndOfData=false, got %d (endOfData=%v)", len(limResp.GetSnapshots()), limResp.GetEndOfData())
	}

	// 5. List with FromSnapshot
	cursorResp, err := server.ListSnapshots(ctx, &pb.ListSnapshotsRequest{
		VolumeId:     volumeID,
		FromSnapshot: snap1,
	})
	if err != nil {
		t.Fatalf("ListSnapshots with FromSnapshot failed: %v", err)
	}
	if len(cursorResp.GetSnapshots()) != 2 || cursorResp.GetSnapshots()[0].GetName() != snap2 || cursorResp.GetSnapshots()[1].GetName() != snap3 {
		t.Fatalf("Unexpected snapshots after snap1: %v", cursorResp.GetSnapshots())
	}

	// 6. List with FromTime (using snap2 timestamp)
	t2 := snapResp2.GetSnapshot().GetCreatedAt()
	fromTimeResp, err := server.ListSnapshots(ctx, &pb.ListSnapshotsRequest{
		VolumeId: volumeID,
		FromTime: t2,
	})
	if err != nil {
		t.Fatalf("ListSnapshots with FromTime failed: %v", err)
	}
	if len(fromTimeResp.GetSnapshots()) < 2 {
		t.Fatalf("Expected at least 2 snapshots from snap2 onwards, got %d", len(fromTimeResp.GetSnapshots()))
	}

	// 7. List with ToTime (using snap2 timestamp)
	toTimeResp, err := server.ListSnapshots(ctx, &pb.ListSnapshotsRequest{
		VolumeId: volumeID,
		ToTime:   t2,
	})
	if err != nil {
		t.Fatalf("ListSnapshots with ToTime failed: %v", err)
	}
	if len(toTimeResp.GetSnapshots()) < 2 {
		t.Fatalf("Expected at least 2 snapshots up to snap2, got %d", len(toTimeResp.GetSnapshots()))
	}
}

func startTestWalBufferServer(t *testing.T, dir string) (*walbuffer.Server, string, func()) {
	ctx := t.Context()
	srv, err := walbuffer.NewServer(ctx, walbuffer.ServerConfig{
		Backend:       NewMemoryBackend(),
		DataDir:       dir,
		FlushInterval: 10 * time.Second,
		BatchMaxDelay: 2 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create walbuffer server: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen for walbuffer: %v", err)
	}

	grpcServer := grpc.NewServer()
	walpb.RegisterWalBufferServer(grpcServer, srv)

	go func() {
		_ = grpcServer.Serve(listener)
	}()

	cleanup := func() {
		grpcServer.Stop()
		_ = srv.Close()
		_ = listener.Close()
	}

	return srv, listener.Addr().String(), cleanup
}

func TestStreamsChangeLogLogging(t *testing.T) {
	ctx := t.Context()
	walDir := t.TempDir()
	backend := NewMemoryBackend()
	server := NewServer(backend, WithServerWAL(walDir, "", walclient.Local))
	defer func() { _ = server.Close() }()

	volumeID := "wal-vol-test"

	// 1. Mkdir should append a mutation record
	mkdirResp, err := server.Mkdir(ctx, &pb.MkdirRequest{
		VolumeId: volumeID,
		Path:     "/testdir",
		Mode:     0755,
	})
	if err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}
	if mkdirResp.Attr.Name != "testdir" {
		t.Fatalf("Unexpected mkdir name: %s", mkdirResp.Attr.Name)
	}

	vol := server.GetVolume(volumeID)
	if vol == nil || vol.Stream() == nil {
		t.Fatalf("Expected volume to have an active WAL stream")
	}

	localSeq, _, _ := vol.Stream().Watermarks()
	if localSeq < 1 {
		t.Fatalf("Expected localSeq >= 1 after Mkdir, got %d", localSeq)
	}

	// 2. CreateFile should log to stream
	createResp, err := server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:       volumeID,
		Path:           "/testdir/data.txt",
		Mode:           0644,
		InitialContent: []byte("hello streams"),
	})
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}
	if createResp.Attr.Size != int64(len("hello streams")) {
		t.Fatalf("Unexpected size: %d", createResp.Attr.Size)
	}

	localSeq2, _, _ := vol.Stream().Watermarks()
	if localSeq2 <= localSeq {
		t.Fatalf("Expected localSeq to advance after CreateFile: %d -> %d", localSeq, localSeq2)
	}

	// 3. WriteFile should log to stream
	writeResp, err := server.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId: volumeID,
		Path:     "/testdir/data.txt",
		Offset:   5,
		Data:     []byte(" world!"),
	})
	if err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}
	if writeResp.BytesWritten != int64(len(" world!")) {
		t.Fatalf("Unexpected bytes written: %d", writeResp.BytesWritten)
	}

	// 4. TruncateFile should log to stream
	truncResp, err := server.TruncateFile(ctx, &pb.TruncateFileRequest{
		VolumeId: volumeID,
		Path:     "/testdir/data.txt",
		Size:     5,
	})
	if err != nil {
		t.Fatalf("TruncateFile failed: %v", err)
	}
	if truncResp.Attr.Size != 5 {
		t.Fatalf("Unexpected truncated size: %d", truncResp.Attr.Size)
	}

	// 5. Rename should log to stream
	renameResp, err := server.Rename(ctx, &pb.RenameRequest{
		VolumeId: volumeID,
		OldPath:  "/testdir/data.txt",
		NewPath:  "/testdir/renamed.txt",
	})
	if err != nil {
		t.Fatalf("Rename failed: %v", err)
	}
	if renameResp.Attr.Name != "renamed.txt" {
		t.Fatalf("Unexpected rename name: %s", renameResp.Attr.Name)
	}

	// 6. Unlink should log to stream
	_, err = server.Unlink(ctx, &pb.UnlinkRequest{
		VolumeId: volumeID,
		Path:     "/testdir/renamed.txt",
	})
	if err != nil {
		t.Fatalf("Unlink failed: %v", err)
	}

	// 7. Rmdir should log to stream
	_, err = server.Rmdir(ctx, &pb.RmdirRequest{
		VolumeId: volumeID,
		Path:     "/testdir",
	})
	if err != nil {
		t.Fatalf("Rmdir failed: %v", err)
	}

	// Verify sequential records were logged
	finalSeq, _, _ := vol.Stream().Watermarks()
	if finalSeq < 7 {
		t.Fatalf("Expected at least 7 mutation records logged, got %d", finalSeq)
	}
}

func TestStreamsCrashRecoveryReplay(t *testing.T) {
	ctx := t.Context()
	walDir := t.TempDir()
	backend := NewMemoryBackend()
	volumeID := "recovery-vol"

	// Step 1: Initialize server 1 and perform initial changes
	server1 := NewServer(backend, WithServerWAL(walDir, "", walclient.Local))
	_, err := server1.Mkdir(ctx, &pb.MkdirRequest{
		VolumeId: volumeID,
		Path:     "/base",
		Mode:     0755,
	})
	if err != nil {
		t.Fatalf("server1 Mkdir failed: %v", err)
	}
	_, err = server1.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:       volumeID,
		Path:           "/base/initial.txt",
		Mode:           0644,
		InitialContent: []byte("initial snapshot content"),
	})
	if err != nil {
		t.Fatalf("server1 CreateFile failed: %v", err)
	}

	// Step 2: Flush EROFS snapshot to backend
	vol1 := server1.GetVolume(volumeID)
	snapName, err := vol1.CreateSnapshot(ctx)
	if err != nil {
		t.Fatalf("CreateSnapshot failed: %v", err)
	}
	if snapName == "" {
		t.Fatalf("Empty snapshot name returned")
	}

	// Step 3: Perform mutations AFTER snapshot (these are in the WAL change-log, not in snapshot)
	_, err = server1.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:       volumeID,
		Path:           "/base/post_snapshot.txt",
		Mode:           0644,
		InitialContent: []byte("post snapshot content"),
	})
	if err != nil {
		t.Fatalf("server1 CreateFile post snapshot failed: %v", err)
	}

	_, err = server1.Rename(ctx, &pb.RenameRequest{
		VolumeId: volumeID,
		OldPath:  "/base/initial.txt",
		NewPath:  "/base/renamed_initial.txt",
	})
	if err != nil {
		t.Fatalf("server1 Rename failed: %v", err)
	}

	// Simulate crash: close server1 without flushing snapshot to backend
	_ = server1.Close()

	// Step 4: Start new server instance with same backend and walDir
	server2 := NewServer(backend, WithServerWAL(walDir, "", walclient.Local))
	defer func() { _ = server2.Close() }()

	vol2 := server2.GetVolume(volumeID)
	if vol2 == nil {
		t.Fatalf("Failed to get recovered volume")
	}

	// Verify that state reflects both the snapshot AND replayed WAL mutations:
	// - /base should exist
	baseAttr, err := server2.GetAttr(ctx, &pb.GetAttrRequest{VolumeId: volumeID, Path: "/base"})
	if err != nil || !baseAttr.Attr.IsDir {
		t.Fatalf("Recovered base directory missing or not dir: %v", err)
	}

	// - /base/initial.txt should have been renamed to /base/renamed_initial.txt
	_, err = server2.GetAttr(ctx, &pb.GetAttrRequest{VolumeId: volumeID, Path: "/base/initial.txt"})
	if err == nil {
		t.Fatalf("Expected /base/initial.txt to not exist after rename replay")
	}

	renamedAttr, err := server2.GetAttr(ctx, &pb.GetAttrRequest{VolumeId: volumeID, Path: "/base/renamed_initial.txt"})
	if err != nil {
		t.Fatalf("Expected /base/renamed_initial.txt to exist: %v", err)
	}
	if renamedAttr.Attr.Size != int64(len("initial snapshot content")) {
		t.Fatalf("Unexpected size on renamed file: %d", renamedAttr.Attr.Size)
	}

	// - /base/post_snapshot.txt should exist and have correct size and content
	readResp, err := server2.ReadFile(ctx, &pb.ReadFileRequest{
		VolumeId: volumeID,
		Path:     "/base/post_snapshot.txt",
		Offset:   0,
		Size:     1024,
	})
	if err != nil {
		t.Fatalf("ReadFile on replayed post-snapshot file failed: %v", err)
	}
	if string(readResp.Data) != "post snapshot content" {
		t.Fatalf("Unexpected data in replayed file: %q", string(readResp.Data))
	}
}

func TestStreamsDurabilityModes(t *testing.T) {
	bufDir := t.TempDir()
	_, target, cleanup := startTestWalBufferServer(t, bufDir)
	defer cleanup()

	clientDir := t.TempDir()
	backend := NewMemoryBackend()
	volumeID := "durability-vol"

	// Create server with Witness durability
	server := NewServer(backend, WithServerWAL(clientDir, target, walclient.Witness))
	defer func() { _ = server.Close() }()

	ctx := t.Context()

	// 1. Mkdir with default durability (Witness)
	_, err := server.Mkdir(ctx, &pb.MkdirRequest{
		VolumeId: volumeID,
		Path:     "/witness_dir",
		Mode:     0755,
	})
	if err != nil {
		t.Fatalf("Mkdir with Witness durability failed: %v", err)
	}

	vol := server.GetVolume(volumeID)
	local, witness, _ := vol.Stream().Watermarks()
	if local < 1 || witness < 1 {
		t.Fatalf("Expected local >= 1 and witness >= 1, got local=%d, witness=%d", local, witness)
	}

	// 2. WriteFile with WRITE_THROUGH_FSYNC (Permanent)
	_, err = server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:       volumeID,
		Path:           "/witness_dir/file.bin",
		Mode:           0644,
		InitialContent: []byte("data"),
	})
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}

	_, err = server.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId:  volumeID,
		Path:      "/witness_dir/file.bin",
		Offset:    4,
		Data:      []byte("more"),
		WriteMode: pb.WriteMode_WRITE_THROUGH_FSYNC,
	})
	if err != nil {
		t.Fatalf("WriteFile with WRITE_THROUGH_FSYNC failed: %v", err)
	}

	_, _, permanent := vol.Stream().Watermarks()
	if permanent < 1 {
		t.Fatalf("Expected permanent watermark >= 1 after WRITE_THROUGH_FSYNC, got %d", permanent)
	}

	// 3. Fsync RPC flushes stream
	fsyncResp, err := server.Fsync(ctx, &pb.FsyncRequest{
		VolumeId: volumeID,
		Path:     "/witness_dir/file.bin",
	})
	if err != nil || !fsyncResp.GetSuccess() {
		t.Fatalf("Fsync failed: %v", err)
	}
}

func TestApplyRecordDirect(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	vol := NewVolume("apply-test", backend, NewEventBroadcaster())

	// Apply Mkdir
	err := vol.ApplyRecordLocked(&MutationRecord{
		Type:     MutationMkdir,
		VolumeId: "apply-test",
		Path:     "/a/b/c",
		Mode:     0755,
		Inode:    10,
	})
	if err != nil {
		t.Fatalf("Apply Mkdir failed: %v", err)
	}

	attr, err := vol.GetAttr(ctx, "/a/b/c")
	if err != nil || !attr.IsDir {
		t.Fatalf("Expected directory /a/b/c: %v", err)
	}

	// Apply CreateFile
	err = vol.ApplyRecordLocked(&MutationRecord{
		Type:     MutationCreateFile,
		VolumeId: "apply-test",
		Path:     "/a/b/c/foo.txt",
		Mode:     0644,
		Size:     4,
		Inode:    11,
		Data:     []byte("test"),
	})
	if err != nil {
		t.Fatalf("Apply CreateFile failed: %v", err)
	}

	data, total, _, err := vol.ReadFile(ctx, "/a/b/c/foo.txt", 0, 100)
	if err != nil || total != 4 || string(data) != "test" {
		t.Fatalf("Unexpected file content: %s (err: %v)", string(data), err)
	}

	// Apply TruncateFile
	err = vol.ApplyRecordLocked(&MutationRecord{
		Type:     MutationTruncateFile,
		VolumeId: "apply-test",
		Path:     "/a/b/c/foo.txt",
		Size:     2,
	})
	if err != nil {
		t.Fatalf("Apply TruncateFile failed: %v", err)
	}
	data, total, _, err = vol.ReadFile(ctx, "/a/b/c/foo.txt", 0, 100)
	if err != nil || total != 2 || string(data) != "te" {
		t.Fatalf("Unexpected truncated content: %s (err: %v)", string(data), err)
	}

	// Apply Rename
	err = vol.ApplyRecordLocked(&MutationRecord{
		Type:     MutationRename,
		VolumeId: "apply-test",
		OldPath:  "/a/b/c/foo.txt",
		Path:     "/a/b/c/bar.txt",
	})
	if err != nil {
		t.Fatalf("Apply Rename failed: %v", err)
	}
	_, err = vol.GetAttr(ctx, "/a/b/c/foo.txt")
	if err == nil {
		t.Fatalf("Expected /a/b/c/foo.txt to be removed after rename")
	}
	barAttr, err := vol.GetAttr(ctx, "/a/b/c/bar.txt")
	if err != nil || barAttr.Name != "bar.txt" {
		t.Fatalf("Expected /a/b/c/bar.txt to exist: %v", err)
	}

	// Apply Unlink
	err = vol.ApplyRecordLocked(&MutationRecord{
		Type:     MutationUnlink,
		VolumeId: "apply-test",
		Path:     "/a/b/c/bar.txt",
	})
	if err != nil {
		t.Fatalf("Apply Unlink failed: %v", err)
	}
	_, err = vol.GetAttr(ctx, "/a/b/c/bar.txt")
	if err == nil {
		t.Fatalf("Expected /a/b/c/bar.txt to be unlinked")
	}

	// Apply Rmdir
	err = vol.ApplyRecordLocked(&MutationRecord{
		Type:     MutationRmdir,
		VolumeId: "apply-test",
		Path:     "/a/b/c",
	})
	if err != nil {
		t.Fatalf("Apply Rmdir failed: %v", err)
	}
	_, err = vol.GetAttr(ctx, "/a/b/c")
	if err == nil {
		t.Fatalf("Expected /a/b/c to be deleted")
	}
}

func TestTargetlessWALDurabilityFastFail(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	walDir := t.TempDir()
	backend := NewMemoryBackend()
	server := NewServer(backend, WithServerWAL(walDir, "", walclient.Local))
	defer func() { _ = server.Close() }()

	volumeID := "test-vol-targetless-wal"

	// Create file should succeed under default Local durability
	_, err := server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:       volumeID,
		Path:           "/test.txt",
		Mode:           0644,
		InitialContent: []byte("initial"),
	})
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}

	// WriteFile with WRITE_THROUGH_FSYNC (Permanent) should fail promptly with FailedPrecondition
	_, err = server.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId:  volumeID,
		Path:      "/test.txt",
		Offset:    0,
		Data:      []byte("data-permanent"),
		WriteMode: pb.WriteMode_WRITE_THROUGH_FSYNC,
	})
	if err == nil {
		t.Fatalf("expected WriteFile(WRITE_THROUGH_FSYNC) to fail on target-less WAL, got nil")
	}
	if st, ok := status.FromError(err); !ok || st.Code() != codes.FailedPrecondition {
		t.Fatalf("expected codes.FailedPrecondition for WRITE_THROUGH_FSYNC, got %v (status: %v)", err, st.Code())
	} else if !strings.Contains(st.Message(), "durability level permanent requested but WAL has no remote target") {
		t.Fatalf("unexpected error message: %q", st.Message())
	}

	// WriteFile with EAGER_REPLICATION (Witness) should fail promptly with FailedPrecondition
	_, err = server.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId:  volumeID,
		Path:      "/test.txt",
		Offset:    0,
		Data:      []byte("data-witness"),
		WriteMode: pb.WriteMode_EAGER_REPLICATION,
	})
	if err == nil {
		t.Fatalf("expected WriteFile(EAGER_REPLICATION) to fail on target-less WAL, got nil")
	}
	if st, ok := status.FromError(err); !ok || st.Code() != codes.FailedPrecondition {
		t.Fatalf("expected codes.FailedPrecondition for EAGER_REPLICATION, got %v (status: %v)", err, st.Code())
	} else if !strings.Contains(st.Message(), "durability level witness requested but WAL has no remote target") {
		t.Fatalf("unexpected error message: %q", st.Message())
	}

	// WriteFile with LAZY_WRITE (Local) should succeed
	writeResp, err := server.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId:  volumeID,
		Path:      "/test.txt",
		Offset:    0,
		Data:      []byte("data-local"),
		WriteMode: pb.WriteMode_LAZY_WRITE,
	})
	if err != nil {
		t.Fatalf("WriteFile(LAZY_WRITE) failed: %v", err)
	}
	if writeResp.BytesWritten != int64(len("data-local")) {
		t.Fatalf("expected %d bytes written, got %d", len("data-local"), writeResp.BytesWritten)
	}

	// Fsync should fail fast with FailedPrecondition on target-less WAL because it flushes to permanent storage
	_, err = server.Fsync(ctx, &pb.FsyncRequest{
		VolumeId: volumeID,
		Path:     "/test.txt",
	})
	if err == nil {
		t.Fatalf("expected Fsync to fail on target-less WAL with unflushed records, got nil")
	}
	if st, ok := status.FromError(err); !ok || st.Code() != codes.FailedPrecondition {
		t.Fatalf("expected codes.FailedPrecondition for Fsync, got %v (status: %v)", err, st.Code())
	}
}
