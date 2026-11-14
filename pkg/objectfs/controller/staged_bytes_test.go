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
	"testing"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
)

func TestControllerStagedBytesAccounting(t *testing.T) {
	ctx := t.Context()
	backend := newRecordingBackend(NewMemoryBackend())
	server := NewTestServer(t, backend, WithServerMaxStagingBytes(64*1024*1024))
	defer func() {
		if closeError := server.Close(); closeError != nil {
			t.Errorf("server.Close failed: %v", closeError)
		}
	}()

	if staged := server.Stats().LocalStagingBytes; staged != 0 {
		t.Fatalf("expected initial localStagingBytes = 0, got %d", staged)
	}

	volume1ID := "test-accounting-volume-1"
	createResponse1, err := testCreateFile(ctx, server, volume1ID, "/file1.bin", 0644, nil, 0, 0)
	if err != nil || createResponse1.GetError() != 0 {
		t.Fatalf("CreateFile file 1 failed: %v", err)
	}
	inode1 := createResponse1.GetAttr().GetInode().GetIno()

	// Volume 1 stages 50 KiB.
	data50K := make([]byte, 50*1024)
	writeResponse1, err := server.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId:  volume1ID,
		Inode:     inode1,
		Offset:    0,
		Data:      data50K,
		WriteMode: pb.WriteMode_LAZY,
	})
	if err != nil || writeResponse1.GetError() != 0 {
		t.Fatalf("WriteFile volume 1 failed: %v", err)
	}
	if staged := server.Stats().LocalStagingBytes; staged != 50*1024 {
		t.Fatalf("expected server localStagingBytes = %d, got %d", 50*1024, staged)
	}

	volume2ID := "test-accounting-volume-2"
	createResponse2, err := testCreateFile(ctx, server, volume2ID, "/file2.bin", 0644, nil, 0, 0)
	if err != nil || createResponse2.GetError() != 0 {
		t.Fatalf("CreateFile file 2 failed: %v", err)
	}
	inode2 := createResponse2.GetAttr().GetInode().GetIno()

	// Volume 2 stages 30 KiB (total 80 KiB across volumes).
	data30K := make([]byte, 30*1024)
	writeResponse2, err := server.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId:  volume2ID,
		Inode:     inode2,
		Offset:    0,
		Data:      data30K,
		WriteMode: pb.WriteMode_LAZY,
	})
	if err != nil || writeResponse2.GetError() != 0 {
		t.Fatalf("WriteFile volume 2 failed: %v", err)
	}
	if staged := server.Stats().LocalStagingBytes; staged != 80*1024 {
		t.Fatalf("expected server localStagingBytes = %d, got %d", 80*1024, staged)
	}

	// Volume 1 fsyncs to commit its staged data. Total drops to volume 2's staged bytes.
	fsyncResponse, err := server.Fsync(ctx, &pb.FsyncRequest{VolumeId: volume1ID, Inode: inode1, Blocking: true})
	if err != nil || !fsyncResponse.GetSuccess() {
		t.Fatalf("Fsync volume 1 failed: %v", err)
	}
	if staged := server.Stats().LocalStagingBytes; staged != 30*1024 {
		t.Fatalf("expected server localStagingBytes = %d after volume 1 sync, got %d", 30*1024, staged)
	}

	// Create snapshot on volume 1 so we can restore it later.
	snapshotResponse, err := server.CreateSnapshot(ctx, &pb.CreateSnapshotRequest{
		VolumeId: volume1ID,
		Name:     "snap-1",
	})
	if err != nil || snapshotResponse.GetSnapshot() == nil {
		t.Fatalf("CreateSnapshot volume 1 failed: %v", err)
	}

	// Volume 1 stages 20 KiB into a new file. Total becomes 30 KiB + 20 KiB = 50 KiB.
	createResponse3, err := testCreateFile(ctx, server, volume1ID, "/file3.bin", 0644, nil, 0, 0)
	if err != nil || createResponse3.GetError() != 0 {
		t.Fatalf("CreateFile file 3 failed: %v", err)
	}
	inode3 := createResponse3.GetAttr().GetInode().GetIno()

	data20K := make([]byte, 20*1024)
	writeResponse3, err := server.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId:  volume1ID,
		Inode:     inode3,
		Offset:    0,
		Data:      data20K,
		WriteMode: pb.WriteMode_LAZY,
	})
	if err != nil || writeResponse3.GetError() != 0 {
		t.Fatalf("WriteFile volume 1 second file failed: %v", err)
	}
	if staged := server.Stats().LocalStagingBytes; staged != 50*1024 {
		t.Fatalf("expected server localStagingBytes = %d, got %d", 50*1024, staged)
	}

	// Restore volume 1 from snapshot. Unsynced staged writes on volume 1 are discarded.
	// Total returns to volume 2's 30 KiB.
	restoreResponse, err := server.RestoreSnapshot(ctx, &pb.RestoreSnapshotRequest{
		VolumeId:     volume1ID,
		SnapshotName: "snap-1",
	})
	if err != nil || restoreResponse.GetSnapshot() == nil {
		t.Fatalf("RestoreSnapshot volume 1 failed: %v", err)
	}
	if staged := server.Stats().LocalStagingBytes; staged != 30*1024 {
		t.Fatalf("expected server localStagingBytes = %d after restore, got %d", 30*1024, staged)
	}

	// Close volume 2 while it still has 30 KiB unsynced staged writes.
	volume2, err := server.getOrCreateVolume(volume2ID)
	if err != nil {
		t.Fatalf("getOrCreateVolume volume 2 failed: %v", err)
	}
	if err := volume2.Close(); err != nil {
		t.Fatalf("volume 2 Close failed: %v", err)
	}

	// Staged bytes must return to zero.
	if staged := server.Stats().LocalStagingBytes; staged != 0 {
		t.Fatalf("expected server localStagingBytes = 0 after volume 2 Close, got %d", staged)
	}
}
