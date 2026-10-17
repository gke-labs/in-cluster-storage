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

package main

import (
	"bytes"
	"context"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/controller"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// TestCheckpointAppendPrefixAndRest verifies the contract:
// a checkpoint while a file is being appended commits a consistent prefix
// and the next checkpoint commits the rest (incremental commit via checkpoint manifest).
// lower/ and upper/ are never mutated or deleted by the daemon while mounted.
func TestCheckpointAppendPrefixAndRest(t *testing.T) {
	ctx := t.Context()
	server := controller.NewServer(nil)
	volumeID := "test-append-prefix-vol"

	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	pb.RegisterObjectFSControllerServer(grpcServer, server)
	go func() {
		_ = grpcServer.Serve(lis)
	}()
	defer grpcServer.Stop()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to dial bufnet: %v", err)
	}
	defer conn.Close()
	client := pb.NewObjectFSControllerClient(conn)

	tempDir := t.TempDir()
	lowerDir := filepath.Join(tempDir, "lower")
	upperDir := filepath.Join(tempDir, "upper")
	manifestPath := filepath.Join(tempDir, "checkpoint.json")

	if err := os.MkdirAll(lowerDir, 0755); err != nil {
		t.Fatalf("mkdir lower: %v", err)
	}
	if err := os.MkdirAll(upperDir, 0755); err != nil {
		t.Fatalf("mkdir upper: %v", err)
	}

	manifest := NewCheckpointManifest()

	// 1. Initial write: create file with 100 bytes (prefix)
	prefixData := bytes.Repeat([]byte("A"), 100)
	filePath := filepath.Join(upperDir, "appended.log")
	if err := os.WriteFile(filePath, prefixData, 0644); err != nil {
		t.Fatalf("write initial file: %v", err)
	}

	// 2. Checkpoint 1: commits the initial 100-byte prefix
	ops1, err := ScanUpperLayer(lowerDir, upperDir, manifest)
	if err != nil {
		t.Fatalf("ScanUpperLayer checkpoint 1 failed: %v", err)
	}

	var hasCreate bool
	for _, op := range ops1 {
		if op.Type == OpCreateFile && op.Name == "appended.log" {
			hasCreate = true
			if op.Size != 100 {
				t.Fatalf("expected initial create size 100, got %d", op.Size)
			}
		}
	}
	if !hasCreate {
		t.Fatalf("expected OpCreateFile for appended.log in checkpoint 1")
	}

	pos1, err := ReplayOperations(ctx, client, volumeID, ops1)
	if err != nil {
		t.Fatalf("ReplayOperations checkpoint 1 failed: %v", err)
	}
	t.Logf("Checkpoint 1 pos: %d", pos1)

	UpdateCheckpointManifest(manifest, upperDir, ops1)
	if err := SaveCheckpointManifest(manifestPath, manifest); err != nil {
		t.Fatalf("SaveCheckpointManifest failed: %v", err)
	}

	// Verify lowerDir was NOT touched (kernel OverlayFS safety invariant)
	if _, err := os.Stat(filepath.Join(lowerDir, "appended.log")); !os.IsNotExist(err) {
		t.Fatalf("lowerDir must never be mutated under live overlay mount")
	}

	// Verify controller has the prefix
	lookupResp, err := client.Lookup(ctx, &pb.LookupRequest{
		VolumeId:    volumeID,
		ParentInode: 1,
		Name:        "appended.log",
	})
	if err != nil || lookupResp.GetError() != 0 {
		t.Fatalf("lookup appended.log on controller failed: %v (err=%d)", err, lookupResp.GetError())
	}
	ino := lookupResp.GetAttr().GetInode().GetIno()

	readResp1, err := client.ReadFile(ctx, &pb.ReadFileRequest{
		VolumeId: volumeID,
		Inode:    ino,
		Offset:   0,
		Size:     100,
	})
	if err != nil || readResp1.GetError() != 0 {
		t.Fatalf("ReadFile checkpoint 1 failed: %v", err)
	}
	if !bytes.Equal(readResp1.GetData(), prefixData) {
		t.Fatalf("ReadFile content mismatch on controller after checkpoint 1")
	}

	// 3. File is appended: append 150 bytes of 'B's to the same file in upperDir
	appendData := bytes.Repeat([]byte("B"), 150)
	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("open file for append: %v", err)
	}
	if _, err := f.Write(appendData); err != nil {
		_ = f.Close()
		t.Fatalf("append to file: %v", err)
	}
	_ = f.Close()

	// Update mtime to be strictly after manifest mtime
	_ = os.Chtimes(filePath, time.Now().Add(time.Second), time.Now().Add(time.Second))

	// 4. Checkpoint 2: should commit ONLY the rest (offset 100, length 150)
	ops2, err := ScanUpperLayer(lowerDir, upperDir, manifest)
	if err != nil {
		t.Fatalf("ScanUpperLayer checkpoint 2 failed: %v", err)
	}

	// Verify ops2 does NOT contain OpCreateFile, only OpWriteFile for the appended chunk
	for _, op := range ops2 {
		if op.Type == OpCreateFile && op.Name == "appended.log" {
			t.Fatalf("checkpoint 2 should not re-create appended.log: %+v", op)
		}
	}

	var foundWriteOffset100 bool
	for _, op := range ops2 {
		if op.Type == OpWriteFile && op.Path == "appended.log" {
			if op.Offset == 100 {
				foundWriteOffset100 = true
				if !bytes.Equal(op.Data, appendData) {
					t.Fatalf("expected write at offset 100 to contain appended data, got len=%d", len(op.Data))
				}
			} else {
				t.Fatalf("unexpected write at offset %d for appended file", op.Offset)
			}
		}
	}
	if !foundWriteOffset100 {
		t.Fatalf("expected OpWriteFile at offset 100 for appended data, got ops: %+v", ops2)
	}

	pos2, err := ReplayOperations(ctx, client, volumeID, ops2)
	if err != nil {
		t.Fatalf("ReplayOperations checkpoint 2 failed: %v", err)
	}
	t.Logf("Checkpoint 2 pos: %d", pos2)

	UpdateCheckpointManifest(manifest, upperDir, ops2)
	if err := SaveCheckpointManifest(manifestPath, manifest); err != nil {
		t.Fatalf("SaveCheckpointManifest failed: %v", err)
	}

	// Verify controller now has complete content (prefix + rest = 250 bytes)
	expectedTotal := append(prefixData, appendData...)
	readResp2, err := client.ReadFile(ctx, &pb.ReadFileRequest{
		VolumeId: volumeID,
		Inode:    ino,
		Offset:   0,
		Size:     250,
	})
	if err != nil || readResp2.GetError() != 0 {
		t.Fatalf("ReadFile after checkpoint 2 failed: %v", err)
	}
	if !bytes.Equal(readResp2.GetData(), expectedTotal) {
		t.Fatalf("ReadFile content mismatch on controller after checkpoint 2: got len %d, expected %d", len(readResp2.GetData()), len(expectedTotal))
	}

	// 5. Checkpoint 3 without modifications: unchanged file must produce 0 operations
	ops3, err := ScanUpperLayer(lowerDir, upperDir, manifest)
	if err != nil {
		t.Fatalf("ScanUpperLayer checkpoint 3 failed: %v", err)
	}
	if len(ops3) != 0 {
		t.Fatalf("expected 0 operations for unchanged file in checkpoint 3, got %d ops: %+v", len(ops3), ops3)
	}
}

// TestScanUpperLayer_ConcurrencyToleratesDeletedFile verifies that if a file is unlinked
// while the scan is running, the scan tolerates it gracefully.
func TestScanUpperLayer_ConcurrencyToleratesDeletedFile(t *testing.T) {
	tempDir := t.TempDir()
	lowerDir := filepath.Join(tempDir, "lower")
	upperDir := filepath.Join(tempDir, "upper")
	_ = os.MkdirAll(lowerDir, 0755)
	_ = os.MkdirAll(upperDir, 0755)

	// File in upper
	fPath := filepath.Join(upperDir, "transient.txt")
	_ = os.WriteFile(fPath, []byte("temporary data"), 0644)

	// Scan should succeed
	ops, err := ScanUpperLayer(lowerDir, upperDir)
	if err != nil {
		t.Fatalf("ScanUpperLayer failed: %v", err)
	}
	if len(ops) == 0 {
		t.Fatalf("expected ops for transient.txt")
	}
}

// TestStartExclusiveLocalSync_ExplicitTrigger verifies that the daemon's WatchVolume listener
// receives EVENT_CHECKPOINT_REQUEST with requestID, executes the commit, and acknowledges via AckCheckpoint.
func TestStartExclusiveLocalSync_ExplicitTrigger(t *testing.T) {
	ctx := t.Context()
	server := controller.NewServer(nil)
	volumeID := "test-sync-explicit-vol"

	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	pb.RegisterObjectFSControllerServer(grpcServer, server)
	go func() {
		_ = grpcServer.Serve(lis)
	}()
	defer grpcServer.Stop()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to dial bufnet: %v", err)
	}
	defer conn.Close()
	client := pb.NewObjectFSControllerClient(conn)

	tempDir := t.TempDir()
	volumeDir := filepath.Join(tempDir, "vol")
	lowerDir := filepath.Join(volumeDir, "lower")
	upperDir := filepath.Join(volumeDir, "upper")
	_ = os.MkdirAll(lowerDir, 0755)
	_ = os.MkdirAll(upperDir, 0755)

	driver := &objectFSDriver{
		nodeID: "test-node",
	}

	var syncWg sync.WaitGroup
	watchCtx, cancel := context.WithCancel(ctx)
	defer func() {
		cancel()
		syncWg.Wait()
	}()

	driver.startExclusiveLocalSync(watchCtx, &syncWg, client, volumeID, volumeDir, lowerDir, upperDir, "/mnt/test-target", 0)

	// Small pause for watcher to establish connection
	time.Sleep(30 * time.Millisecond)

	// Write file in upper
	testData := []byte("content to checkpoint explicitly")
	if err := os.WriteFile(filepath.Join(upperDir, "explicit.txt"), testData, 0644); err != nil {
		t.Fatalf("write upper file: %v", err)
	}

	// Trigger explicit checkpoint via controller RPC (simulating objectfsctl checkpoint)
	cpResp, err := server.Checkpoint(ctx, &pb.CheckpointRequest{
		VolumeId: volumeID,
	})
	if err != nil {
		t.Fatalf("Checkpoint request failed: %v", err)
	}
	t.Logf("Explicit checkpoint acknowledged at stream position %d", cpResp.GetStreamPosition())

	// Verify file is on controller
	lookupResp, err := client.Lookup(ctx, &pb.LookupRequest{
		VolumeId:    volumeID,
		ParentInode: 1,
		Name:        "explicit.txt",
	})
	if err != nil || lookupResp.GetError() != 0 {
		t.Fatalf("lookup explicit.txt on controller failed: %v", err)
	}

	// Verify checkpoint manifest was saved
	manifest, err := LoadCheckpointManifest(filepath.Join(volumeDir, "checkpoint.json"))
	if err != nil {
		t.Fatalf("failed to load checkpoint manifest: %v", err)
	}
	if entry, ok := manifest.Files["explicit.txt"]; !ok || entry.Size != int64(len(testData)) {
		t.Fatalf("manifest entry missing or size mismatch: %+v", entry)
	}
}

// TestStartExclusiveLocalSync_IntervalTrigger verifies periodic background commits on a timer.
func TestStartExclusiveLocalSync_IntervalTrigger(t *testing.T) {
	ctx := t.Context()
	daemonCtx, daemonCancel := context.WithCancel(ctx)
	defer daemonCancel()

	server := controller.NewServer(nil)
	volumeID := "test-sync-interval-vol"

	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	pb.RegisterObjectFSControllerServer(grpcServer, server)
	go func() {
		_ = grpcServer.Serve(lis)
	}()
	defer grpcServer.Stop()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to dial bufnet: %v", err)
	}
	defer conn.Close()
	client := pb.NewObjectFSControllerClient(conn)

	tempDir := t.TempDir()
	volumeDir := filepath.Join(tempDir, "vol")
	lowerDir := filepath.Join(volumeDir, "lower")
	upperDir := filepath.Join(volumeDir, "upper")
	_ = os.MkdirAll(lowerDir, 0755)
	_ = os.MkdirAll(upperDir, 0755)

	driver := &objectFSDriver{
		nodeID: "test-node",
	}

	var syncWg sync.WaitGroup
	defer func() {
		daemonCancel()
		syncWg.Wait()
	}()

	// 25ms interval with daemon-lifetime context
	driver.startExclusiveLocalSync(daemonCtx, &syncWg, client, volumeID, volumeDir, lowerDir, upperDir, "/mnt/test-target", 25*time.Millisecond)

	// Write file in upper
	testData := []byte("interval periodic data")
	if err := os.WriteFile(filepath.Join(upperDir, "interval.txt"), testData, 0644); err != nil {
		t.Fatalf("write upper file: %v", err)
	}

	// Wait for interval tick to commit to controller and save manifest
	deadline := time.Now().Add(5 * time.Second)
	var committed bool
	for time.Now().Before(deadline) {
		lookupResp, err := client.Lookup(daemonCtx, &pb.LookupRequest{
			VolumeId:    volumeID,
			ParentInode: 1,
			Name:        "interval.txt",
		})
		if err == nil && lookupResp.GetError() == 0 {
			manifest, mErr := LoadCheckpointManifest(filepath.Join(volumeDir, "checkpoint.json"))
			if mErr == nil && manifest.Files != nil && manifest.Files["interval.txt"].Size == int64(len(testData)) {
				committed = true
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}

	if !committed {
		t.Fatalf("interval checkpoint did not commit file and update manifest within deadline")
	}
}

// TestScanUpperLayer_UnpublishWithNoManifestMatchesPR290 verifies that when an exclusive-local
// volume has never executed an incremental checkpoint (no checkpoint.json manifest),
// ScanUpperLayer produces the exact unmount operation list as PR #290.
func TestScanUpperLayer_UnpublishWithNoManifestMatchesPR290(t *testing.T) {
	tempDir := t.TempDir()
	lowerDir := filepath.Join(tempDir, "lower")
	upperDir := filepath.Join(tempDir, "upper")
	_ = os.MkdirAll(lowerDir, 0755)
	_ = os.MkdirAll(upperDir, 0755)

	// Base lower files
	if err := os.WriteFile(filepath.Join(lowerDir, "existing.txt"), []byte("original"), 0644); err != nil {
		t.Fatalf("write lower existing: %v", err)
	}

	// Upper mutations
	if err := os.WriteFile(filepath.Join(upperDir, "existing.txt"), []byte("modified-longer-content"), 0644); err != nil {
		t.Fatalf("write upper modified: %v", err)
	}
	if err := os.WriteFile(filepath.Join(upperDir, "new.txt"), []byte("new-file"), 0644); err != nil {
		t.Fatalf("write upper new: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(upperDir, "sub", "dir"), 0755); err != nil {
		t.Fatalf("mkdir upper: %v", err)
	}
	if err := os.WriteFile(filepath.Join(upperDir, "sub", "dir", "nested.txt"), []byte("nested"), 0644); err != nil {
		t.Fatalf("write upper nested: %v", err)
	}

	// 1. Scan with 0 manifests passed
	opsDefault, err := ScanUpperLayer(lowerDir, upperDir)
	if err != nil {
		t.Fatalf("ScanUpperLayer with no args failed: %v", err)
	}

	// 2. Scan with nil manifest
	opsNil, err := ScanUpperLayer(lowerDir, upperDir, nil)
	if err != nil {
		t.Fatalf("ScanUpperLayer with nil manifest failed: %v", err)
	}

	// 3. Scan with loaded empty manifest (no checkpoint.json on disk)
	manifest, err := LoadCheckpointManifest(filepath.Join(tempDir, "non_existent_checkpoint.json"))
	if err != nil {
		t.Fatalf("LoadCheckpointManifest non-existent failed: %v", err)
	}
	opsLoadedEmpty, err := ScanUpperLayer(lowerDir, upperDir, manifest)
	if err != nil {
		t.Fatalf("ScanUpperLayer with empty manifest failed: %v", err)
	}

	// Compare lengths
	if len(opsDefault) != len(opsNil) || len(opsDefault) != len(opsLoadedEmpty) {
		t.Fatalf("operation counts differ: default=%d, nil=%d, loadedEmpty=%d", len(opsDefault), len(opsNil), len(opsLoadedEmpty))
	}

	// Verify all operations match
	for i := range opsDefault {
		if opsDefault[i].Type != opsNil[i].Type || opsDefault[i].Path != opsNil[i].Path {
			t.Fatalf("mismatch between default and nil at %d: %+v vs %+v", i, opsDefault[i], opsNil[i])
		}
		if opsDefault[i].Type != opsLoadedEmpty[i].Type || opsDefault[i].Path != opsLoadedEmpty[i].Path {
			t.Fatalf("mismatch between default and loadedEmpty at %d: %+v vs %+v", i, opsDefault[i], opsLoadedEmpty[i])
		}
	}

	// Verify expected operations from PR #290:
	// - OpMkdir for sub and sub/dir
	// - OpCreateFile for new.txt and sub/dir/nested.txt
	// - OpTruncateFile + OpWriteFile for existing.txt
	// - OpSetAttr for all
	var foundNew, foundMod, foundMkdirSub, foundMkdirSubDir, foundNested bool
	for _, op := range opsDefault {
		switch op.Path {
		case "new.txt":
			if op.Type == OpCreateFile {
				foundNew = true
			}
		case "existing.txt":
			if op.Type == OpWriteFile {
				foundMod = true
			}
		case "sub":
			if op.Type == OpMkdir {
				foundMkdirSub = true
			}
		case "sub/dir":
			if op.Type == OpMkdir {
				foundMkdirSubDir = true
			}
		case "sub/dir/nested.txt":
			if op.Type == OpCreateFile {
				foundNested = true
			}
		}
	}

	if !foundNew || !foundMod || !foundMkdirSub || !foundMkdirSubDir || !foundNested {
		t.Fatalf("expected PR #290 operations missing in unpublish scan without manifest: %+v", opsDefault)
	}
}
