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
	"encoding/binary"
	"syscall"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
)

func TestEvictionAcrossVolumes_CacheCap(t *testing.T) {
	ctx := t.Context()
	backend := newRecordingBackend(NewMemoryBackend())
	server := NewTestServer(t, backend, WithServerMaxStagingBytes(64*1024*1024))
	defer func() {
		if closeError := server.Close(); closeError != nil {
			t.Errorf("server.Close failed: %v", closeError)
		}
	}()

	volume1ID := "test-cache-evict-volume-1"
	volume1, err := server.getOrCreateVolume(volume1ID)
	if err != nil {
		t.Fatalf("getOrCreateVolume volume 1 failed: %v", err)
	}

	createResponse1, err := testCreateFile(ctx, server, volume1ID, "/file1.bin", 0644, nil, 0, 0)
	if err != nil || createResponse1.GetError() != 0 {
		t.Fatalf("CreateFile file 1 failed: %v", err)
	}
	inode1 := createResponse1.GetAttr().GetInode().GetIno()

	fillAndSealPack(t, volume1, ctx, inode1)
	if err := volume1.Fsync(ctx, inode1); err != nil {
		t.Fatalf("Fsync volume 1 failed: %v", err)
	}
	waitForPackState(t, volume1, 1, packUploaded)

	volume1.stagingMu.Lock()
	pack1Info, pack1Exists := volume1.packs[1]
	if !pack1Exists || pack1Info.pack == nil {
		volume1.stagingMu.Unlock()
		t.Fatal("expected pack 1 to exist on volume 1")
	}
	pack1Size := pack1Info.pack.ByteSize()
	// Ensure pack 1 has an earlier lastRead timestamp than volume 2's upcoming pack.
	pack1Info.lastRead = time.Now().Add(-10 * time.Second)
	volume1.stagingMu.Unlock()

	// Set cache cap to pack1Size + a small margin so a second pack cannot fit simultaneously.
	server.diskLimitsMu.Lock()
	server.maxLocalPackCacheBytes = pack1Size + 1024
	server.diskLimitsMu.Unlock()

	serverStatsBefore := server.Stats()
	if serverStatsBefore.LocalPackCacheBytes < pack1Size {
		t.Fatalf("expected server localPackCacheBytes >= %d, got %d", pack1Size, serverStatsBefore.LocalPackCacheBytes)
	}

	volume2ID := "test-cache-evict-volume-2"
	volume2, err := server.getOrCreateVolume(volume2ID)
	if err != nil {
		t.Fatalf("getOrCreateVolume volume 2 failed: %v", err)
	}

	createResponse2, err := testCreateFile(ctx, server, volume2ID, "/file2.bin", 0644, nil, 0, 0)
	if err != nil || createResponse2.GetError() != 0 {
		t.Fatalf("CreateFile file 2 failed: %v", err)
	}
	inode2 := createResponse2.GetAttr().GetInode().GetIno()

	fillAndSealPack(t, volume2, ctx, inode2)
	if err := volume2.Fsync(ctx, inode2); err != nil {
		t.Fatalf("Fsync volume 2 failed: %v", err)
	}
	waitForPackState(t, volume2, 1, packUploaded)

	// Volume 1's Pack 1 should have been evicted across volumes to satisfy the cache cap.
	volume1.stagingMu.Lock()
	_, pack1StillPresent := volume1.packs[1]
	volume1.stagingMu.Unlock()
	if pack1StillPresent {
		t.Fatal("expected pack 1 on volume 1 to be evicted under controller cache cap")
	}

	// Volume 2's pack should remain retained in cache.
	volume2.stagingMu.Lock()
	_, pack2Present := volume2.packs[1]
	volume2.stagingMu.Unlock()
	if !pack2Present {
		t.Fatal("expected pack 1 on volume 2 to be retained in cache")
	}

	// Reading volume 1's file should succeed by falling back to the store.
	readData, totalSize, _, readErr := volume1.ReadFile(ctx, inode1, 0, 16)
	if readErr != nil {
		t.Fatalf("ReadFile on evicted volume 1 failed: %v", readErr)
	}
	if totalSize == 0 || len(readData) != 16 {
		t.Fatalf("unexpected read data length %d, totalSize %d", len(readData), totalSize)
	}
	expectedFirstChunk := make([]byte, 16)
	binary.BigEndian.PutUint64(expectedFirstChunk[0:8], 0)
	binary.BigEndian.PutUint64(expectedFirstChunk[8:16], 1)
	if !bytes.Equal(readData, expectedFirstChunk) {
		t.Fatalf("read data mismatch: got %v, want %v", readData, expectedFirstChunk)
	}

	// Check observability surface.
	volumeStats := volume1.Stats()
	if volumeStats.MaxLocalPackCacheBytes != pack1Size+1024 {
		t.Fatalf("expected volumeStats.MaxLocalPackCacheBytes = %d, got %d", pack1Size+1024, volumeStats.MaxLocalPackCacheBytes)
	}
	if volumeStats.LocalPackCacheBytes > volumeStats.MaxLocalPackCacheBytes {
		t.Fatalf("expected volumeStats.LocalPackCacheBytes <= %d, got %d", volumeStats.MaxLocalPackCacheBytes, volumeStats.LocalPackCacheBytes)
	}
}

func TestFailFast_StagingCap(t *testing.T) {
	ctx := t.Context()
	backend := newRecordingBackend(NewMemoryBackend())
	// Set per-volume staging cap high (64 MiB) and controller staging cap low (100 KiB).
	server := NewTestServer(
		t,
		backend,
		WithServerMaxStagingBytes(64*1024*1024),
		WithServerMaxLocalStagingBytes(100*1024),
	)
	defer func() {
		if closeError := server.Close(); closeError != nil {
			t.Errorf("server.Close failed: %v", closeError)
		}
	}()

	volume1ID := "test-staging-cap-volume-1"
	createResponse1, err := testCreateFile(ctx, server, volume1ID, "/staged1.bin", 0644, nil, 0, 0)
	if err != nil || createResponse1.GetError() != 0 {
		t.Fatalf("CreateFile file 1 failed: %v", err)
	}
	inode1 := createResponse1.GetAttr().GetInode().GetIno()

	// Volume 1 stages 70 KiB (under both per-volume 64 MiB and controller 100 KiB).
	data70K := make([]byte, 70*1024)
	writeResponse1, err := server.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId:  volume1ID,
		Inode:     inode1,
		Offset:    0,
		Data:      data70K,
		WriteMode: pb.WriteMode_LAZY,
	})
	if err != nil {
		t.Fatalf("WriteFile volume 1 failed: %v", err)
	}
	if writeResponse1.GetError() != 0 {
		t.Fatalf("WriteFile volume 1 returned unexpected error: %d", writeResponse1.GetError())
	}

	serverStats := server.Stats()
	if serverStats.LocalStagingBytes != 70*1024 {
		t.Fatalf("expected server localStagingBytes = %d, got %d", 70*1024, serverStats.LocalStagingBytes)
	}

	volume2ID := "test-staging-cap-volume-2"
	createResponse2, err := testCreateFile(ctx, server, volume2ID, "/staged2.bin", 0644, nil, 0, 0)
	if err != nil || createResponse2.GetError() != 0 {
		t.Fatalf("CreateFile file 2 failed: %v", err)
	}
	inode2 := createResponse2.GetAttr().GetInode().GetIno()

	// Volume 2 attempts to write 50 KiB.
	// 50 KiB is well below per-volume cap of 64 MiB, but 70 KiB + 50 KiB = 120 KiB > 100 KiB controller cap.
	// It must fail fast with ENOSPC without stalling.
	data50K := make([]byte, 50*1024)
	writeResponse2, err := server.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId:  volume2ID,
		Inode:     inode2,
		Offset:    0,
		Data:      data50K,
		WriteMode: pb.WriteMode_LAZY,
	})
	if err != nil {
		t.Fatalf("WriteFile volume 2 RPC failed: %v", err)
	}
	if writeResponse2.GetError() != int32(syscall.ENOSPC) {
		t.Fatalf("expected WriteFile to fail fast with ENOSPC (%d), got error: %d", syscall.ENOSPC, writeResponse2.GetError())
	}

	// Volume 1 fsyncs to commit staged data.
	fsyncResponse, err := server.Fsync(ctx, &pb.FsyncRequest{
		VolumeId: volume1ID,
		Inode:    inode1,
		Blocking: true,
	})
	if err != nil || !fsyncResponse.GetSuccess() {
		t.Fatalf("Fsync volume 1 failed: %v", err)
	}

	// Volume 1's staged bytes drop. Volume 2 can now write successfully.
	writeResponse2Retry, err := server.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId:  volume2ID,
		Inode:     inode2,
		Offset:    0,
		Data:      data50K,
		WriteMode: pb.WriteMode_LAZY,
	})
	if err != nil {
		t.Fatalf("WriteFile volume 2 retry failed: %v", err)
	}
	if writeResponse2Retry.GetError() != 0 {
		t.Fatalf("WriteFile volume 2 retry returned error: %d", writeResponse2Retry.GetError())
	}

	// Verify observability metrics.
	serverStatsAfter := server.Stats()
	if serverStatsAfter.LocalStagingBytes != 50*1024 {
		t.Fatalf("expected server localStagingBytes = %d, got %d", 50*1024, serverStatsAfter.LocalStagingBytes)
	}
	if serverStatsAfter.MaxLocalStagingBytes != 100*1024 {
		t.Fatalf("expected server maxLocalStagingBytes = %d, got %d", 100*1024, serverStatsAfter.MaxLocalStagingBytes)
	}
}
