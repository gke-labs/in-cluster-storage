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
	"syscall"
	"testing"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCreateVolumeFromSnapshot(t *testing.T) {
	ctx := t.Context()
	walDirectory := t.TempDir()
	backend := NewMemoryBackend()
	server1 := NewTestServer(t, backend, WithServerWAL(walDirectory, "", walclient.Local))

	volumeA := "volume-a"
	volumeB := "volume-b"
	volumeC := "volume-c"

	if _, err := testCreateFile(ctx, server1, volumeA, "/common.txt", 0644, []byte("shared data"), 0, 0); err != nil {
		t.Fatalf("testCreateFile on volume A failed: %v", err)
	}

	snapshotResponse, err := server1.CreateSnapshot(ctx, &pb.CreateSnapshotRequest{VolumeId: volumeA})
	if err != nil {
		t.Fatalf("CreateSnapshot on volume A failed: %v", err)
	}
	snapshotName := snapshotResponse.GetSnapshotName()

	if _, err := testCreateFile(ctx, server1, volumeC, "/existing.txt", 0644, []byte("volume c existing data"), 0, 0); err != nil {
		t.Fatalf("testCreateFile on volume C failed: %v", err)
	}

	err = server1.CreateVolumeFromSnapshot(ctx, volumeC, volumeA, snapshotName)
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("expected AlreadyExists when creating loaded volume C, got: %v", err)
	}

	if err := server1.CreateVolumeFromSnapshot(ctx, volumeB, volumeA, snapshotName); err != nil {
		t.Fatalf("CreateVolumeFromSnapshot failed: %v", err)
	}

	readCommonFromB, err := testReadFile(ctx, server1, volumeB, "/common.txt", 0, 1024)
	if err != nil {
		t.Fatalf("ReadFile on volume B failed: %v", err)
	}
	if string(readCommonFromB.Data) != "shared data" {
		t.Fatalf("volume B read got %q, want %q", string(readCommonFromB.Data), "shared data")
	}

	volumeBInstance, err := server1.GetVolume(volumeB)
	if err != nil {
		t.Fatalf("GetVolume on volume B failed: %v", err)
	}
	pointers, err := volumeBInstance.ListSnapshots(ctx)
	if err != nil {
		t.Fatalf("ListSnapshots on volume B failed: %v", err)
	}
	if len(pointers) != 1 || pointers[0].GetClonedFrom() != volumeA+"/"+snapshotName || pointers[0].GetRestoredFrom() != "" {
		t.Fatalf("expected cloned_from %q and empty restored_from, got %+v", volumeA+"/"+snapshotName, pointers)
	}

	if _, err := testCreateFile(ctx, server1, volumeB, "/b_only.txt", 0644, []byte("b private data"), 0, 0); err != nil {
		t.Fatalf("testCreateFile on volume B failed: %v", err)
	}
	if _, err := testCreateFile(ctx, server1, volumeA, "/a_only.txt", 0644, []byte("a private data"), 0, 0); err != nil {
		t.Fatalf("testCreateFile on volume A failed: %v", err)
	}

	readBOnlyFromB, err := testReadFile(ctx, server1, volumeB, "/b_only.txt", 0, 1024)
	if err != nil {
		t.Fatalf("ReadFile /b_only.txt on volume B failed: %v", err)
	}
	if string(readBOnlyFromB.Data) != "b private data" {
		t.Fatalf("volume B got %q, want %q", string(readBOnlyFromB.Data), "b private data")
	}

	attributeAInB, err := testGetAttr(ctx, server1, volumeB, "/a_only.txt")
	if err != nil {
		t.Fatalf("GetAttr /a_only.txt on volume B failed: %v", err)
	}
	if attributeAInB.GetError() != int32(syscall.ENOENT) {
		t.Fatalf("volume B unexpectedly saw /a_only.txt; error = %d", attributeAInB.GetError())
	}

	readAOnlyFromA, err := testReadFile(ctx, server1, volumeA, "/a_only.txt", 0, 1024)
	if err != nil {
		t.Fatalf("ReadFile /a_only.txt on volume A failed: %v", err)
	}
	if string(readAOnlyFromA.Data) != "a private data" {
		t.Fatalf("volume A got %q, want %q", string(readAOnlyFromA.Data), "a private data")
	}

	attributeBInA, err := testGetAttr(ctx, server1, volumeA, "/b_only.txt")
	if err != nil {
		t.Fatalf("GetAttr /b_only.txt on volume A failed: %v", err)
	}
	if attributeBInA.GetError() != int32(syscall.ENOENT) {
		t.Fatalf("volume A unexpectedly saw /b_only.txt; error = %d", attributeBInA.GetError())
	}

	if err := server1.FlushAll(ctx); err != nil {
		t.Fatalf("FlushAll server1 failed: %v", err)
	}
	shutdownTestServer(t, server1)

	server2 := NewTestServer(t, backend, WithServerWAL(walDirectory, "", walclient.Local))

	err = server2.CreateVolumeFromSnapshot(ctx, volumeC, volumeA, snapshotName)
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("expected AlreadyExists when creating durable volume C, got: %v", err)
	}

	readCommonAfterRestart, err := testReadFile(ctx, server2, volumeB, "/common.txt", 0, 1024)
	if err != nil {
		t.Fatalf("ReadFile /common.txt on recovered volume B failed: %v", err)
	}
	if string(readCommonAfterRestart.Data) != "shared data" {
		t.Fatalf("recovered volume B read /common.txt got %q, want %q", string(readCommonAfterRestart.Data), "shared data")
	}

	readBOnlyAfterRestart, err := testReadFile(ctx, server2, volumeB, "/b_only.txt", 0, 1024)
	if err != nil {
		t.Fatalf("ReadFile /b_only.txt on recovered volume B failed: %v", err)
	}
	if string(readBOnlyAfterRestart.Data) != "b private data" {
		t.Fatalf("recovered volume B read /b_only.txt got %q, want %q", string(readBOnlyAfterRestart.Data), "b private data")
	}

	attributeAInBAfterRestart, err := testGetAttr(ctx, server2, volumeB, "/a_only.txt")
	if err != nil {
		t.Fatalf("GetAttr /a_only.txt on recovered volume B failed: %v", err)
	}
	if attributeAInBAfterRestart.GetError() != int32(syscall.ENOENT) {
		t.Fatalf("recovered volume B unexpectedly saw /a_only.txt; error = %d", attributeAInBAfterRestart.GetError())
	}
}
