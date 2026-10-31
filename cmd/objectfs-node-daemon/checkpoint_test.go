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
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/controller/controllertest"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

// TestCheckpointAppendPrefixAndRest verifies the contract:
// a checkpoint while a file is being appended commits a consistent prefix
// and the next checkpoint commits the rest (incremental commit via checkpoint manifest).
// lower/ and upper/ are never mutated or deleted by the daemon while mounted.
func TestCheckpointAppendPrefixAndRest(t *testing.T) {
	ctx := t.Context()
	server := controllertest.NewTestServer(nil)
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

	if err := UpdateCheckpointManifest(manifest, upperDir, ops1); err != nil {
		t.Fatalf("UpdateCheckpointManifest checkpoint 1 failed: %v", err)
	}
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

	if err := UpdateCheckpointManifest(manifest, upperDir, ops2); err != nil {
		t.Fatalf("UpdateCheckpointManifest checkpoint 2 failed: %v", err)
	}
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
	server := controllertest.NewTestServer(nil)
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

	driver.startExclusiveLocalSync(watchCtx, &syncWg, nil, nil, client, volumeID, volumeDir, lowerDir, upperDir, "/mnt/test-target", 0)

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

	server := controllertest.NewTestServer(nil)
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
	driver.startExclusiveLocalSync(daemonCtx, &syncWg, nil, nil, client, volumeID, volumeDir, lowerDir, upperDir, "/mnt/test-target", 25*time.Millisecond)

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

func TestScanUpperLayer_XAttrs(t *testing.T) {
	tempDir := t.TempDir()
	lowerDir := filepath.Join(tempDir, "lower")
	upperDir := filepath.Join(tempDir, "upper")
	_ = os.MkdirAll(lowerDir, 0755)
	_ = os.MkdirAll(upperDir, 0755)

	// 1. Lower file with xattr that will be removed in upper
	lowerFile := filepath.Join(lowerDir, "existing.txt")
	if err := os.WriteFile(lowerFile, []byte("lower-content"), 0644); err != nil {
		t.Fatalf("write lowerFile: %v", err)
	}
	if err := unix.Lsetxattr(lowerFile, "user.removed_attr", []byte("remove-me"), 0); err != nil {
		t.Fatalf("set lower xattr: %v", err)
	}

	// 2. Upper file with xattr set, lower xattr removed, and overlay-internal xattr set
	upperFile := filepath.Join(upperDir, "existing.txt")
	if err := os.WriteFile(upperFile, []byte("upper-content"), 0644); err != nil {
		t.Fatalf("write upperFile: %v", err)
	}
	if err := unix.Lsetxattr(upperFile, "user.new_attr", []byte("new-val"), 0); err != nil {
		t.Fatalf("set upper xattr: %v", err)
	}
	if err := unix.Lsetxattr(upperFile, "user.overlay.test", []byte("internal-overlay"), 0); err != nil {
		t.Fatalf("set overlay internal xattr: %v", err)
	}

	// 3. Upper new file with xattrs
	newUpperFile := filepath.Join(upperDir, "new.txt")
	if err := os.WriteFile(newUpperFile, []byte("new-file"), 0644); err != nil {
		t.Fatalf("write newUpperFile: %v", err)
	}
	if err := unix.Lsetxattr(newUpperFile, "user.custom", []byte("custom-val"), 0); err != nil {
		t.Fatalf("set new upper xattr: %v", err)
	}

	ops, err := ScanUpperLayer(lowerDir, upperDir)
	if err != nil {
		t.Fatalf("ScanUpperLayer failed: %v", err)
	}

	var foundSetExisting, foundRemoveExisting, foundSetNew bool
	for _, op := range ops {
		// Verify no overlay-internal xattr is emitted
		if strings.HasPrefix(op.Name, "trusted.overlay.") || strings.HasPrefix(op.Name, "user.overlay.") {
			t.Fatalf("unexpected overlay-internal xattr op emitted: %+v", op)
		}

		if op.Path == "existing.txt" && op.Type == OpSetXAttr && op.Name == "user.new_attr" && string(op.Value) == "new-val" {
			foundSetExisting = true
		}
		if op.Path == "existing.txt" && op.Type == OpRemoveXAttr && op.Name == "user.removed_attr" {
			foundRemoveExisting = true
		}
		if op.Path == "new.txt" && op.Type == OpSetXAttr && op.Name == "user.custom" && string(op.Value) == "custom-val" {
			foundSetNew = true
		}
	}

	if !foundSetExisting {
		t.Errorf("missing OpSetXAttr for existing.txt")
	}
	if !foundRemoveExisting {
		t.Errorf("missing OpRemoveXAttr for existing.txt")
	}
	if !foundSetNew {
		t.Errorf("missing OpSetXAttr for new.txt")
	}
}

func TestScanUpperLayer_FailingGetFailsScan(t *testing.T) {
	tempDir := t.TempDir()
	lowerDir := filepath.Join(tempDir, "lower")
	upperDir := filepath.Join(tempDir, "upper")
	_ = os.MkdirAll(lowerDir, 0755)
	_ = os.MkdirAll(upperDir, 0755)

	upperFile := filepath.Join(upperDir, "file.txt")
	if err := os.WriteFile(upperFile, []byte("content"), 0644); err != nil {
		t.Fatalf("write upperFile: %v", err)
	}
	if err := unix.Lsetxattr(upperFile, "user.test", []byte("val"), 0); err != nil {
		t.Fatalf("setxattr failed: %v", err)
	}

	origGet := unixLgetxattr
	t.Cleanup(func() { unixLgetxattr = origGet })
	unixLgetxattr = func(path string, attr string, dest []byte) (sz int, err error) {
		return 0, syscall.EIO
	}

	_, err := ScanUpperLayer(lowerDir, upperDir)
	if err == nil {
		t.Fatalf("expected ScanUpperLayer to fail when getxattr fails, got nil")
	}
	if !strings.Contains(err.Error(), "getxattr") {
		t.Errorf("expected error to mention getxattr, got: %v", err)
	}
}

func TestScanUpperLayer_ManifestIncrementalXAttrs(t *testing.T) {
	tempDir := t.TempDir()
	lowerDir := filepath.Join(tempDir, "lower")
	upperDir := filepath.Join(tempDir, "upper")
	_ = os.MkdirAll(lowerDir, 0755)
	_ = os.MkdirAll(upperDir, 0755)

	manifest := NewCheckpointManifest()

	// 1. Lower file with an xattr: user.removable
	lowerFile := filepath.Join(lowerDir, "file.txt")
	if err := os.WriteFile(lowerFile, []byte("content"), 0644); err != nil {
		t.Fatalf("write lowerFile: %v", err)
	}
	if err := unix.Lsetxattr(lowerFile, "user.removable", []byte("val1"), 0); err != nil {
		t.Fatalf("set lower xattr: %v", err)
	}

	// 2. In upper: file has user.added, and user.removable is absent (removed)
	upperFile := filepath.Join(upperDir, "file.txt")
	if err := os.WriteFile(upperFile, []byte("content"), 0644); err != nil {
		t.Fatalf("write upperFile: %v", err)
	}
	if err := unix.Lsetxattr(upperFile, "user.added", []byte("val2"), 0); err != nil {
		t.Fatalf("set upper xattr: %v", err)
	}

	// Scan 1 (first checkpoint after mount):
	// user.added should be emitted as OpSetXAttr
	// user.removable should be emitted as OpRemoveXAttr
	ops1, err := ScanUpperLayer(lowerDir, upperDir, manifest)
	if err != nil {
		t.Fatalf("ScanUpperLayer checkpoint 1 failed: %v", err)
	}

	var foundSetAdded, foundRemoveRemovable bool
	for _, op := range ops1 {
		if op.Type == OpSetXAttr && op.Name == "user.added" && string(op.Value) == "val2" {
			foundSetAdded = true
		}
		if op.Type == OpRemoveXAttr && op.Name == "user.removable" {
			foundRemoveRemovable = true
		}
	}
	if !foundSetAdded {
		t.Errorf("checkpoint 1: expected OpSetXAttr for user.added, got: %+v", ops1)
	}
	if !foundRemoveRemovable {
		t.Errorf("checkpoint 1: expected OpRemoveXAttr for user.removable, got: %+v", ops1)
	}

	// Update manifest with checkpoint 1 ops
	if err := UpdateCheckpointManifest(manifest, upperDir, ops1); err != nil {
		t.Fatalf("checkpoint 1: UpdateCheckpointManifest failed: %v", err)
	}

	// Scan 2 (consecutive scan with shared manifest without modifications):
	// Must emit nothing (removal emitted once and not again; unchanged set not re-emitted)
	ops2, err := ScanUpperLayer(lowerDir, upperDir, manifest)
	if err != nil {
		t.Fatalf("ScanUpperLayer checkpoint 2 failed: %v", err)
	}
	if len(ops2) != 0 {
		t.Fatalf("checkpoint 2: expected 0 operations, got %d ops: %+v", len(ops2), ops2)
	}

	// Value change in upper: modify user.added value to val3
	if err := unix.Lsetxattr(upperFile, "user.added", []byte("val3"), 0); err != nil {
		t.Fatalf("set upper xattr modified: %v", err)
	}

	// Scan 3:
	// A value change that is emitted as a set
	ops3, err := ScanUpperLayer(lowerDir, upperDir, manifest)
	if err != nil {
		t.Fatalf("ScanUpperLayer checkpoint 3 failed: %v", err)
	}

	var foundModifiedSet bool
	for _, op := range ops3 {
		if op.Type == OpSetXAttr && op.Name == "user.added" && string(op.Value) == "val3" {
			foundModifiedSet = true
		}
		if op.Type == OpRemoveXAttr {
			t.Errorf("checkpoint 3: unexpected OpRemoveXAttr: %+v", op)
		}
	}
	if !foundModifiedSet {
		t.Fatalf("checkpoint 3: expected OpSetXAttr with val3 for user.added, got: %+v", ops3)
	}

	// Update manifest and verify subsequent scan is clean again
	if err := UpdateCheckpointManifest(manifest, upperDir, ops3); err != nil {
		t.Fatalf("checkpoint 3: UpdateCheckpointManifest failed: %v", err)
	}
	ops4, err := ScanUpperLayer(lowerDir, upperDir, manifest)
	if err != nil {
		t.Fatalf("ScanUpperLayer checkpoint 4 failed: %v", err)
	}
	if len(ops4) != 0 {
		t.Fatalf("checkpoint 4: expected 0 operations, got %d ops: %+v", len(ops4), ops4)
	}

	// 4. Test sequence for unchanged-xattr copy-up, checkpoint, remove, checkpoint:
	// Setup lower file with user.copyup_attr
	copyUpLower := filepath.Join(lowerDir, "copyup.txt")
	if err := os.WriteFile(copyUpLower, []byte("orig-content"), 0644); err != nil {
		t.Fatalf("write copyUpLower: %v", err)
	}
	if err := unix.Lsetxattr(copyUpLower, "user.copyup_attr", []byte("orig-val"), 0); err != nil {
		t.Fatalf("set copyUpLower xattr: %v", err)
	}

	// In upper: file is copied up and modified in content, but xattr user.copyup_attr is unchanged
	copyUpUpper := filepath.Join(upperDir, "copyup.txt")
	if err := os.WriteFile(copyUpUpper, []byte("modified-content"), 0644); err != nil {
		t.Fatalf("write copyUpUpper: %v", err)
	}
	if err := unix.Lsetxattr(copyUpUpper, "user.copyup_attr", []byte("orig-val"), 0); err != nil {
		t.Fatalf("set copyUpUpper xattr: %v", err)
	}

	// Checkpoint 5:
	// copyup.txt has content ops and SetAttr, but NO xattr ops (unchanged relative to lower)
	ops5, err := ScanUpperLayer(lowerDir, upperDir, manifest)
	if err != nil {
		t.Fatalf("ScanUpperLayer checkpoint 5 failed: %v", err)
	}
	for _, op := range ops5 {
		if op.Path == "copyup.txt" && (op.Type == OpSetXAttr || op.Type == OpRemoveXAttr) {
			t.Fatalf("checkpoint 5: unexpected xattr op for unchanged copyup.txt: %+v", op)
		}
	}

	// Update manifest with checkpoint 5
	if err := UpdateCheckpointManifest(manifest, upperDir, ops5); err != nil {
		t.Fatalf("checkpoint 5: UpdateCheckpointManifest failed: %v", err)
	}

	// Remove user.copyup_attr in upper
	if err := unix.Lremovexattr(copyUpUpper, "user.copyup_attr"); err != nil {
		t.Fatalf("remove user.copyup_attr: %v", err)
	}

	// Checkpoint 6:
	// Must emit exactly one OpRemoveXAttr for copyup.txt
	ops6, err := ScanUpperLayer(lowerDir, upperDir, manifest)
	if err != nil {
		t.Fatalf("ScanUpperLayer checkpoint 6 failed: %v", err)
	}

	var removeCount int
	for _, op := range ops6 {
		if op.Path == "copyup.txt" && op.Type == OpRemoveXAttr && op.Name == "user.copyup_attr" {
			removeCount++
		} else if op.Type == OpSetXAttr || op.Type == OpRemoveXAttr {
			t.Fatalf("checkpoint 6: unexpected xattr op: %+v", op)
		}
	}
	if removeCount != 1 {
		t.Fatalf("checkpoint 6: expected exactly one OpRemoveXAttr for user.copyup_attr, got %d (ops: %+v)", removeCount, ops6)
	}

	// Update manifest and verify subsequent scan is clean
	if err := UpdateCheckpointManifest(manifest, upperDir, ops6); err != nil {
		t.Fatalf("checkpoint 6: UpdateCheckpointManifest failed: %v", err)
	}
	ops7, err := ScanUpperLayer(lowerDir, upperDir, manifest)
	if err != nil {
		t.Fatalf("ScanUpperLayer checkpoint 7 failed: %v", err)
	}
	if len(ops7) != 0 {
		t.Fatalf("checkpoint 7: expected 0 operations, got %d ops: %+v", len(ops7), ops7)
	}
}

type fakeRemoveXAttrClient struct {
	pb.ObjectFSControllerClient
	errCode int32
}

func (f *fakeRemoveXAttrClient) Lookup(ctx context.Context, in *pb.LookupRequest, opts ...grpc.CallOption) (*pb.LookupResponse, error) {
	return &pb.LookupResponse{
		Attr: &pb.EntryAttr{
			Inode: &pb.Inode{
				Ino: proto.Uint64(10),
			},
		},
	}, nil
}

func (f *fakeRemoveXAttrClient) RemoveXAttr(ctx context.Context, in *pb.RemoveXAttrRequest, opts ...grpc.CallOption) (*pb.RemoveXAttrResponse, error) {
	return &pb.RemoveXAttrResponse{
		Error: f.errCode,
	}, nil
}

func (f *fakeRemoveXAttrClient) Fsync(ctx context.Context, in *pb.FsyncRequest, opts ...grpc.CallOption) (*pb.FsyncResponse, error) {
	return &pb.FsyncResponse{
		StreamPosition: 42,
	}, nil
}

func TestReplayOperations_RemoveXAttrENODATA(t *testing.T) {
	ctx := t.Context()

	// 1. When RemoveXAttr returns ENODATA (already removed), ReplayOperations succeeds
	fakeOK := &fakeRemoveXAttrClient{
		errCode: int32(syscall.ENODATA),
	}
	ops := []FileOp{
		{
			Type: OpRemoveXAttr,
			Path: "file.txt",
			Name: "user.missing",
		},
	}
	pos, err := ReplayOperations(ctx, fakeOK, "test-vol", ops)
	if err != nil {
		t.Fatalf("expected ReplayOperations to treat ENODATA on RemoveXAttr as already removed, got error: %v", err)
	}
	if pos != 42 {
		t.Fatalf("expected stream position 42, got %d", pos)
	}

	// 2. When RemoveXAttr returns another error (e.g. EPERM), ReplayOperations fails loudly
	fakeFail := &fakeRemoveXAttrClient{
		errCode: int32(syscall.EPERM),
	}
	_, err = ReplayOperations(ctx, fakeFail, "test-vol", ops)
	if err == nil {
		t.Fatalf("expected ReplayOperations to fail on non-ENODATA error, got nil")
	}
}
