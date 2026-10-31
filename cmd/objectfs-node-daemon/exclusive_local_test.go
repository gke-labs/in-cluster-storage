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
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/controller"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/controller/controllertest"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// setWhiteout creates a whiteout marker at path (character device or xattr fallback).
func setWhiteout(t *testing.T, path string) {
	t.Helper()
	// Try creating a whiteout character device (rdev 0)
	err := unix.Mknod(path, unix.S_IFCHR|0600, 0)
	if err == nil {
		return
	}
	// Fall back to creating regular file and tagging with user.overlay.whiteout xattr
	if err := os.WriteFile(path, nil, 0644); err != nil {
		t.Fatalf("failed to create fallback whiteout file %s: %v", path, err)
	}
	if err := unix.Setxattr(path, "user.overlay.whiteout", []byte("y"), 0); err != nil {
		t.Fatalf("failed to set user.overlay.whiteout xattr on %s: %v", path, err)
	}
}

// setOpaqueDir sets the user.overlay.opaque xattr on dir.
func setOpaqueDir(t *testing.T, dir string) {
	t.Helper()
	if err := unix.Setxattr(dir, "user.overlay.opaque", []byte("y"), 0); err != nil {
		t.Fatalf("failed to set user.overlay.opaque xattr on %s: %v", dir, err)
	}
}

// TestScanUpperLayer_OperationList verifies that ScanUpperLayer detects:
// 1. new file
// 2. modified file
// 3. deleted file as whiteout
// 4. opaque directory
// 5. nested create under new dir
func TestScanUpperLayer_OperationList(t *testing.T) {
	baseDir := t.TempDir()
	lowerDir := filepath.Join(baseDir, "lower")
	upperDir := filepath.Join(baseDir, "upper")

	if err := os.MkdirAll(lowerDir, 0755); err != nil {
		t.Fatalf("mkdir lower: %v", err)
	}
	if err := os.MkdirAll(upperDir, 0755); err != nil {
		t.Fatalf("mkdir upper: %v", err)
	}

	// Setup lowerDir base state
	// 1. File to be modified
	if err := os.WriteFile(filepath.Join(lowerDir, "file_to_modify.txt"), []byte("original content"), 0644); err != nil {
		t.Fatalf("write file_to_modify: %v", err)
	}
	// 2. File to be deleted via whiteout
	if err := os.WriteFile(filepath.Join(lowerDir, "file_to_delete.txt"), []byte("delete me"), 0644); err != nil {
		t.Fatalf("write file_to_delete: %v", err)
	}
	// 3. Directory to be replaced with opaque directory
	opaqueLowerDir := filepath.Join(lowerDir, "opaque_dir")
	if err := os.MkdirAll(opaqueLowerDir, 0755); err != nil {
		t.Fatalf("mkdir opaque_dir in lower: %v", err)
	}
	if err := os.WriteFile(filepath.Join(opaqueLowerDir, "stale_child.txt"), []byte("stale"), 0644); err != nil {
		t.Fatalf("write stale_child: %v", err)
	}

	// Setup upperDir mutated state
	// 1. New file
	if err := os.WriteFile(filepath.Join(upperDir, "new_file.txt"), []byte("hello from new file"), 0644); err != nil {
		t.Fatalf("write new_file: %v", err)
	}
	// 2. Modified file
	if err := os.WriteFile(filepath.Join(upperDir, "file_to_modify.txt"), []byte("modified content with extra bytes!"), 0644); err != nil {
		t.Fatalf("write modified file: %v", err)
	}
	// 3. Deleted file as whiteout
	setWhiteout(t, filepath.Join(upperDir, "file_to_delete.txt"))
	// 4. Opaque directory
	opaqueUpperDir := filepath.Join(upperDir, "opaque_dir")
	if err := os.MkdirAll(opaqueUpperDir, 0755); err != nil {
		t.Fatalf("mkdir opaque_dir in upper: %v", err)
	}
	setOpaqueDir(t, opaqueUpperDir)
	if err := os.WriteFile(filepath.Join(opaqueUpperDir, "new_child.txt"), []byte("fresh"), 0644); err != nil {
		t.Fatalf("write new_child in opaque_dir: %v", err)
	}
	// 5. Nested create under new dir
	nestedDir := filepath.Join(upperDir, "new_parent", "new_child")
	if err := os.MkdirAll(nestedDir, 0755); err != nil {
		t.Fatalf("mkdir nested dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(nestedDir, "nested_file.txt"), []byte("nested content"), 0644); err != nil {
		t.Fatalf("write nested_file: %v", err)
	}

	// Scan
	ops, err := ScanUpperLayer(lowerDir, upperDir)
	if err != nil {
		t.Fatalf("ScanUpperLayer failed: %v", err)
	}

	// Check operations
	var foundNewFile, foundModFile, foundWhiteout, foundOpaqueChildDelete, foundNestedParent, foundNestedChild, foundNestedFile bool

	for _, op := range ops {
		switch op.Path {
		case "new_file.txt":
			if op.Type == OpCreateFile && string(op.Data) == "hello from new file" {
				foundNewFile = true
			}
		case "file_to_modify.txt":
			if op.Type == OpWriteFile || op.Type == OpTruncateFile {
				foundModFile = true
			}
		case "file_to_delete.txt":
			if op.Type == OpUnlink {
				foundWhiteout = true
			}
		case "opaque_dir/stale_child.txt":
			if op.Type == OpUnlink {
				foundOpaqueChildDelete = true
			}
		case "new_parent":
			if op.Type == OpMkdir {
				foundNestedParent = true
			}
		case "new_parent/new_child":
			if op.Type == OpMkdir {
				foundNestedChild = true
			}
		case "new_parent/new_child/nested_file.txt":
			if op.Type == OpCreateFile && string(op.Data) == "nested content" {
				foundNestedFile = true
			}
		}
	}

	if !foundNewFile {
		t.Errorf("Expected OpCreateFile for new_file.txt in ops, but not found")
	}
	if !foundModFile {
		t.Errorf("Expected OpWriteFile/OpTruncateFile for file_to_modify.txt in ops, but not found")
	}
	if !foundWhiteout {
		t.Errorf("Expected OpUnlink for whiteout file_to_delete.txt in ops, but not found")
	}
	if !foundOpaqueChildDelete {
		t.Errorf("Expected OpUnlink for stale_child.txt under opaque directory, but not found")
	}
	if !foundNestedParent {
		t.Errorf("Expected OpMkdir for new_parent, but not found")
	}
	if !foundNestedChild {
		t.Errorf("Expected OpMkdir for new_parent/new_child, but not found")
	}
	if !foundNestedFile {
		t.Errorf("Expected OpCreateFile for nested_file.txt, but not found")
	}

	// Verify ordering: deletions before creations, parent dirs before child dirs/files
	seenParent := false
	seenChildDir := false
	seenNestedFile := false
	for _, op := range ops {
		if op.Path == "new_parent" && op.Type == OpMkdir {
			seenParent = true
		}
		if op.Path == "new_parent/new_child" && op.Type == OpMkdir {
			if !seenParent {
				t.Fatalf("Ordering violation: child dir new_parent/new_child created before parent new_parent")
			}
			seenChildDir = true
		}
		if op.Path == "new_parent/new_child/nested_file.txt" && op.Type == OpCreateFile {
			if !seenChildDir {
				t.Fatalf("Ordering violation: file created before parent directory was created")
			}
			seenNestedFile = true
		}
	}
	if !seenNestedFile {
		t.Fatalf("Expected nested_file to be created in order")
	}
}

// TestReplayOperations verifies replaying scanned operations against a real ObjectFS controller server.
func TestReplayOperations(t *testing.T) {
	ctx := t.Context()
	server := controllertest.NewTestServer(nil)
	volumeID := "test-replay-vol"

	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	pb.RegisterObjectFSControllerServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(lis) }()
	defer grpcServer.Stop()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient failed: %v", err)
	}
	defer conn.Close()
	client := pb.NewObjectFSControllerClient(conn)

	// Pre-populate volume with an existing file and an existing directory
	_, err = client.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:       volumeID,
		ParentInode:    1,
		Name:           "existing.txt",
		Mode:           0644,
		InitialContent: []byte("original data"),
	})
	if err != nil {
		t.Fatalf("CreateFile existing.txt failed: %v", err)
	}
	_, err = client.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:       volumeID,
		ParentInode:    1,
		Name:           "to_remove.txt",
		Mode:           0644,
		InitialContent: []byte("delete"),
	})
	if err != nil {
		t.Fatalf("CreateFile to_remove.txt failed: %v", err)
	}

	baseDir := t.TempDir()
	lowerDir := filepath.Join(baseDir, "lower")
	upperDir := filepath.Join(baseDir, "upper")
	_ = os.MkdirAll(lowerDir, 0755)
	_ = os.MkdirAll(upperDir, 0755)

	// In lower:
	_ = os.WriteFile(filepath.Join(lowerDir, "existing.txt"), []byte("original data"), 0644)
	_ = os.WriteFile(filepath.Join(lowerDir, "to_remove.txt"), []byte("delete"), 0644)

	// In upper:
	// 1. Modify existing.txt
	_ = os.WriteFile(filepath.Join(upperDir, "existing.txt"), []byte("updated data"), 0644)
	// 2. Whiteout to_remove.txt
	setWhiteout(t, filepath.Join(upperDir, "to_remove.txt"))
	// 3. New dir and new file
	_ = os.MkdirAll(filepath.Join(upperDir, "sub"), 0755)
	_ = os.WriteFile(filepath.Join(upperDir, "sub", "hello.txt"), []byte("world"), 0644)

	// Scan
	ops, err := ScanUpperLayer(lowerDir, upperDir)
	if err != nil {
		t.Fatalf("ScanUpperLayer failed: %v", err)
	}

	// Replay
	if _, err := ReplayOperations(ctx, client, volumeID, ops); err != nil {
		t.Fatalf("ReplayOperations failed: %v", err)
	}

	// Verify controller state
	// 1. existing.txt should have "updated data"
	lookupExisting, err := client.Lookup(ctx, &pb.LookupRequest{VolumeId: volumeID, ParentInode: 1, Name: "existing.txt"})
	if err != nil || lookupExisting.GetError() != 0 {
		t.Fatalf("Lookup existing.txt failed: %v, resp: %v", err, lookupExisting)
	}
	readResp, err := client.ReadFile(ctx, &pb.ReadFileRequest{
		VolumeId: volumeID,
		Inode:    lookupExisting.GetAttr().GetInode().GetIno(),
		Offset:   0,
		Size:     100,
	})
	if err != nil || readResp.GetError() != 0 {
		t.Fatalf("ReadFile existing.txt failed: %v", err)
	}
	if string(readResp.GetData()) != "updated data" {
		t.Errorf("Expected updated data, got: %q", string(readResp.GetData()))
	}

	// 2. to_remove.txt should be gone (ENOENT)
	lookupDeleted, err := client.Lookup(ctx, &pb.LookupRequest{VolumeId: volumeID, ParentInode: 1, Name: "to_remove.txt"})
	if err != nil {
		t.Fatalf("Lookup to_remove.txt RPC error: %v", err)
	}
	if lookupDeleted.GetError() != int32(syscall.ENOENT) {
		t.Errorf("Expected ENOENT (%d) for deleted file, got: %d", syscall.ENOENT, lookupDeleted.GetError())
	}

	// 3. sub/hello.txt should exist with "world"
	lookupSub, err := client.Lookup(ctx, &pb.LookupRequest{VolumeId: volumeID, ParentInode: 1, Name: "sub"})
	if err != nil || lookupSub.GetError() != 0 {
		t.Fatalf("Lookup sub failed: %v", err)
	}
	lookupHello, err := client.Lookup(ctx, &pb.LookupRequest{
		VolumeId:    volumeID,
		ParentInode: lookupSub.GetAttr().GetInode().GetIno(),
		Name:        "hello.txt",
	})
	if err != nil || lookupHello.GetError() != 0 {
		t.Fatalf("Lookup sub/hello.txt failed: %v", err)
	}
	readHello, err := client.ReadFile(ctx, &pb.ReadFileRequest{
		VolumeId: volumeID,
		Inode:    lookupHello.GetAttr().GetInode().GetIno(),
		Offset:   0,
		Size:     10,
	})
	if err != nil || readHello.GetError() != 0 {
		t.Fatalf("ReadFile hello.txt failed: %v", err)
	}
	if string(readHello.GetData()) != "world" {
		t.Errorf("Expected 'world', got: %q", string(readHello.GetData()))
	}
}

// TestNodePublish_AccessModeValidation verifies that exclusive-local mode requires SINGLE_NODE_SINGLE_WRITER (ReadWriteOncePod).
func TestNodePublish_AccessModeValidation(t *testing.T) {
	ctx := t.Context()
	driver, err := newDriver("test-node", "localhost:50051", t.TempDir(), 1024*1024, 10*1024*1024, 30*time.Second, 10*time.Minute, true, 30*time.Second, "", true)
	if err != nil {
		t.Fatalf("newDriver failed: %v", err)
	}
	defer func() { _ = driver.Close() }()

	tests := []struct {
		name       string
		mode       string
		accessMode csi.VolumeCapability_AccessMode_Mode
		wantErr    bool
		wantCode   codes.Code
	}{
		{
			name:       "exclusive-local with SINGLE_NODE_WRITER (rejected)",
			mode:       "exclusive-local",
			accessMode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			wantErr:    true,
			wantCode:   codes.InvalidArgument,
		},
		{
			name:       "exclusive-local with MULTI_NODE_MULTI_WRITER (rejected)",
			mode:       "exclusive-local",
			accessMode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			wantErr:    true,
			wantCode:   codes.InvalidArgument,
		},
		{
			name:       "exclusive-local without capability (rejected)",
			mode:       "exclusive-local",
			accessMode: csi.VolumeCapability_AccessMode_UNKNOWN,
			wantErr:    true,
			wantCode:   codes.InvalidArgument,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &csi.NodePublishVolumeRequest{
				VolumeId:   "test-vol",
				TargetPath: filepath.Join(t.TempDir(), "target"),
				VolumeCapability: &csi.VolumeCapability{
					AccessMode: &csi.VolumeCapability_AccessMode{
						Mode: tt.accessMode,
					},
				},
				VolumeContext: map[string]string{
					"volumeID": "test-vol",
					"mode":     tt.mode,
				},
			}
			_, err := driver.NodePublishVolume(ctx, req)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("Expected error, got nil")
				}
				st, ok := status.FromError(err)
				if !ok || st.Code() != tt.wantCode {
					t.Errorf("Expected grpc error code %v, got %v (err: %v)", tt.wantCode, st.Code(), err)
				}
			}
		})
	}
}

// TestNodeGetCapabilities_IncludesSingleNodeMultiWriter verifies advertising capability for ReadWriteOncePod.
func TestNodeGetCapabilities_IncludesSingleNodeMultiWriter(t *testing.T) {
	ctx := t.Context()
	driver, err := newDriver("test-node", "localhost:50051", t.TempDir(), 1024*1024, 10*1024*1024, 30*time.Second, 10*time.Minute, true, 30*time.Second, "", true)
	if err != nil {
		t.Fatalf("newDriver failed: %v", err)
	}
	defer func() { _ = driver.Close() }()

	resp, err := driver.NodeGetCapabilities(ctx, &csi.NodeGetCapabilitiesRequest{})
	if err != nil {
		t.Fatalf("NodeGetCapabilities failed: %v", err)
	}

	found := false
	for _, cap := range resp.GetCapabilities() {
		if cap.GetRpc().GetType() == csi.NodeServiceCapability_RPC_SINGLE_NODE_MULTI_WRITER {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Expected capability SINGLE_NODE_MULTI_WRITER in NodeGetCapabilities")
	}
}

// TestCheckpointFailure_NeverDiscardsUpperLayer verifies that if checkpoint replay fails,
// upper/ is preserved on disk so kubelet can retry.
func TestCheckpointFailure_NeverDiscardsUpperLayer(t *testing.T) {
	ctx := t.Context()
	cacheDir := t.TempDir()
	volumeID := "vol-fail-test"
	volumeDir := filepath.Join(cacheDir, volumeID)
	upperDir := filepath.Join(volumeDir, "upper")
	lowerDir := filepath.Join(volumeDir, "lower")
	_ = os.MkdirAll(upperDir, 0755)
	_ = os.MkdirAll(lowerDir, 0755)

	dirtyFile := filepath.Join(upperDir, "uncommitted.txt")
	if err := os.WriteFile(dirtyFile, []byte("important uncommitted work"), 0644); err != nil {
		t.Fatalf("write uncommitted file: %v", err)
	}

	driver, err := newDriver("test-node", "localhost:50051", cacheDir, 1024*1024, 10*1024*1024, 30*time.Second, 10*time.Minute, true, 30*time.Second, "", true)
	if err != nil {
		t.Fatalf("newDriver failed: %v", err)
	}
	defer func() { _ = driver.Close() }()

	targetPath := filepath.Join(t.TempDir(), "target")
	_ = os.MkdirAll(targetPath, 0755)

	driver.mu.Lock()
	driver.mounts[targetPath] = &activeMount{
		volumeID:  volumeID,
		mode:      "exclusive-local",
		volumeDir: volumeDir,
	}
	driver.mu.Unlock()

	// Calling NodeUnpublishVolume when controller is not running: Replay will fail to connect/replay.
	_, unpublishErr := driver.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{
		VolumeId:   volumeID,
		TargetPath: targetPath,
	})
	if unpublishErr == nil {
		t.Fatalf("Expected unpublish to fail due to unreachable controller")
	}

	// CRITICAL CONTRACT: uncommitted upper layer must NOT be deleted
	if _, err := os.Stat(dirtyFile); err != nil {
		t.Fatalf("CONTRACT VIOLATION: uncommitted file %s was deleted after failed checkpoint: %v", dirtyFile, err)
	}
	content, _ := os.ReadFile(dirtyFile)
	if string(content) != "important uncommitted work" {
		t.Fatalf("Expected content preserved, got %q", string(content))
	}
}

// TestMaterialiseVolume verifies eager materialisation downloads directories and files.
func TestMaterialiseVolume(t *testing.T) {
	ctx := t.Context()
	server := controllertest.NewTestServer(nil)
	volumeID := "test-mat-vol"

	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	pb.RegisterObjectFSControllerServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(lis) }()
	defer grpcServer.Stop()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient failed: %v", err)
	}
	defer conn.Close()
	client := pb.NewObjectFSControllerClient(conn)

	// Create /dir/file.txt and /root_file.txt in volume
	mkdirResp, err := client.Mkdir(ctx, &pb.MkdirRequest{
		VolumeId:    volumeID,
		ParentInode: 1,
		Name:        "dir",
		Mode:        0755,
	})
	if err != nil || mkdirResp.GetError() != 0 {
		t.Fatalf("Mkdir dir failed: %v, resp: %v", err, mkdirResp)
	}
	dirIno := mkdirResp.GetAttr().GetInode().GetIno()

	_, err = client.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:       volumeID,
		ParentInode:    dirIno,
		Name:           "file.txt",
		Mode:           0644,
		InitialContent: []byte("materialised content inside dir"),
	})
	if err != nil {
		t.Fatalf("CreateFile inside dir failed: %v", err)
	}

	_, err = client.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:       volumeID,
		ParentInode:    1,
		Name:           "root_file.txt",
		Mode:           0644,
		InitialContent: []byte("root file data"),
	})
	if err != nil {
		t.Fatalf("CreateFile root_file failed: %v", err)
	}

	driver, err := newDriver("test-node", "passthrough://bufnet", t.TempDir(), 1024*1024, 10*1024*1024, 30*time.Second, 10*time.Minute, true, 30*time.Second, "", true)
	if err != nil {
		t.Fatalf("newDriver failed: %v", err)
	}
	defer func() { _ = driver.Close() }()

	lowerPath := filepath.Join(t.TempDir(), "lower")
	_ = os.MkdirAll(lowerPath, 0755)

	if err := driver.materialiseVolume(ctx, client, volumeID, lowerPath); err != nil {
		t.Fatalf("materialiseVolume failed: %v", err)
	}

	// Verify files in lowerPath
	data1, err := os.ReadFile(filepath.Join(lowerPath, "root_file.txt"))
	if err != nil || string(data1) != "root file data" {
		t.Errorf("root_file.txt: err=%v, content=%q", err, string(data1))
	}

	data2, err := os.ReadFile(filepath.Join(lowerPath, "dir", "file.txt"))
	if err != nil || string(data2) != "materialised content inside dir" {
		t.Errorf("dir/file.txt: err=%v, content=%q", err, string(data2))
	}
}

// TestScanUpperLayer_HardLinksBreakIntoSeparateFiles verifies that hard links in upper
// are scanned as independent files (breaking the hard link into separate volume files on checkpoint).
func TestScanUpperLayer_HardLinksBreakIntoSeparateFiles(t *testing.T) {
	baseDir := t.TempDir()
	lowerDir := filepath.Join(baseDir, "lower")
	upperDir := filepath.Join(baseDir, "upper")
	_ = os.MkdirAll(lowerDir, 0755)
	_ = os.MkdirAll(upperDir, 0755)

	f1 := filepath.Join(upperDir, "original.txt")
	f2 := filepath.Join(upperDir, "link.txt")
	if err := os.WriteFile(f1, []byte("shared content"), 0644); err != nil {
		t.Fatalf("write original: %v", err)
	}
	if err := os.Link(f1, f2); err != nil {
		t.Fatalf("link failed: %v", err)
	}

	ops, err := ScanUpperLayer(lowerDir, upperDir)
	if err != nil {
		t.Fatalf("ScanUpperLayer failed: %v", err)
	}

	var foundF1, foundF2 bool
	for _, op := range ops {
		if op.Path == "original.txt" && op.Type == OpCreateFile {
			foundF1 = true
		}
		if op.Path == "link.txt" && op.Type == OpCreateFile {
			foundF2 = true
		}
	}
	if !foundF1 || !foundF2 {
		t.Errorf("Expected both hard-linked files to appear as independent OpCreateFile, got f1=%v, f2=%v", foundF1, foundF2)
	}
}

// TestScanUpperLayer_SpecialFilesSkipped verifies that special files (like FIFOs) are skipped with a warning.
func TestScanUpperLayer_SpecialFilesSkipped(t *testing.T) {
	baseDir := t.TempDir()
	lowerDir := filepath.Join(baseDir, "lower")
	upperDir := filepath.Join(baseDir, "upper")
	_ = os.MkdirAll(lowerDir, 0755)
	_ = os.MkdirAll(upperDir, 0755)

	fifoPath := filepath.Join(upperDir, "my_fifo")
	if err := unix.Mkfifo(fifoPath, 0666); err != nil {
		t.Skipf("mkfifo not supported in environment: %v", err)
	}

	ops, err := ScanUpperLayer(lowerDir, upperDir)
	if err != nil {
		t.Fatalf("ScanUpperLayer failed: %v", err)
	}

	for _, op := range ops {
		if op.Path == "my_fifo" {
			t.Fatalf("Expected FIFO to be skipped, but found operation: %v", op)
		}
	}
}

// TestPublishExclusiveLocal_RecoversLeftoverUpper verifies that publishing recovers and replays
// any leftover uncommitted upper/ from a previous session before mounting fresh.
func TestPublishExclusiveLocal_RecoversLeftoverUpper(t *testing.T) {
	ctx := t.Context()
	server := controllertest.NewTestServer(nil)
	volumeID := "recover-leftover-vol"

	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	pb.RegisterObjectFSControllerServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(lis) }()
	defer grpcServer.Stop()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()
	client := pb.NewObjectFSControllerClient(conn)

	cacheDir := t.TempDir()
	volumeDir := filepath.Join(cacheDir, volumeID)
	upperDir := filepath.Join(volumeDir, "upper")
	lowerDir := filepath.Join(volumeDir, "lower")
	_ = os.MkdirAll(upperDir, 0755)
	_ = os.MkdirAll(lowerDir, 0755)

	// Simulate leftover uncommitted file from a prior failed checkpoint / pod death
	leftoverFile := filepath.Join(upperDir, "leftover_work.txt")
	if err := os.WriteFile(leftoverFile, []byte("salvaged work from prior pod"), 0644); err != nil {
		t.Fatalf("write leftover file: %v", err)
	}

	driver, err := newDriver("test-node", "passthrough://bufnet", cacheDir, 1024*1024, 10*1024*1024, 30*time.Second, 10*time.Minute, true, 30*time.Second, "", true)
	if err != nil {
		t.Fatalf("newDriver failed: %v", err)
	}
	defer func() { _ = driver.Close() }()
	driver.client = client
	driver.clientConn = conn

	targetPath := filepath.Join(t.TempDir(), "target")
	req := &csi.NodePublishVolumeRequest{
		VolumeId:   volumeID,
		TargetPath: targetPath,
		VolumeCapability: &csi.VolumeCapability{
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER,
			},
		},
		VolumeContext: map[string]string{
			"volumeID": volumeID,
			"mode":     "exclusive-local",
		},
	}

	// publish should detect leftover upper, replay it into the controller volume, and succeed
	_, err = driver.NodePublishVolume(ctx, req)
	if err != nil && !strings.Contains(err.Error(), "operation not permitted") && !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("NodePublishVolume failed during leftover upper recovery: %v", err)
	}

	// Verify controller received the salvaged file
	client, err = driver.getClient()
	if err != nil {
		t.Fatalf("getClient: %v", err)
	}
	lookupResp, err := client.Lookup(ctx, &pb.LookupRequest{
		VolumeId:    volumeID,
		ParentInode: 1,
		Name:        "leftover_work.txt",
	})
	if err != nil || lookupResp.GetError() != 0 {
		t.Fatalf("Lookup leftover_work.txt in controller failed: %v, resp: %v", err, lookupResp)
	}
	readResp, err := client.ReadFile(ctx, &pb.ReadFileRequest{
		VolumeId: volumeID,
		Inode:    lookupResp.GetAttr().GetInode().GetIno(),
		Offset:   0,
		Size:     100,
	})
	if err != nil || readResp.GetError() != 0 {
		t.Fatalf("ReadFile leftover_work.txt failed: %v", err)
	}
	if string(readResp.GetData()) != "salvaged work from prior pod" {
		t.Errorf("Expected salvaged content, got: %q", string(readResp.GetData()))
	}
}

type failingReplayServer struct {
	*controller.Server
}

func (s *failingReplayServer) CreateFile(ctx context.Context, req *pb.CreateFileRequest) (*pb.CreateFileResponse, error) {
	return nil, fmt.Errorf("injected replay failure")
}

// TestPublishExclusiveLocal_FailsIfLeftoverUpperReplayFails verifies that if replay of
// leftover upper fails on publish, publish fails naming the directory and leaves upper intact.
func TestPublishExclusiveLocal_FailsIfLeftoverUpperReplayFails(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	cacheDir := t.TempDir()
	volumeID := "recover-fail-vol"
	volumeDir := filepath.Join(cacheDir, volumeID)
	upperDir := filepath.Join(volumeDir, "upper")
	lowerDir := filepath.Join(volumeDir, "lower")
	_ = os.MkdirAll(upperDir, 0755)
	_ = os.MkdirAll(lowerDir, 0755)

	leftoverFile := filepath.Join(upperDir, "critical_unsaved.txt")
	if err := os.WriteFile(leftoverFile, []byte("must not be lost"), 0644); err != nil {
		t.Fatalf("write leftover file: %v", err)
	}

	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	fakeServer := &failingReplayServer{Server: controllertest.NewTestServer(nil)}
	pb.RegisterObjectFSControllerServer(grpcServer, fakeServer)
	go func() { _ = grpcServer.Serve(lis) }()
	defer grpcServer.Stop()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	client := pb.NewObjectFSControllerClient(conn)

	driver := &objectFSDriver{
		nodeID:   "test-node",
		cacheDir: cacheDir,
		client:   client,
		mounts:   make(map[string]*activeMount),
	}
	defer func() { _ = driver.Close() }()

	targetPath := filepath.Join(t.TempDir(), "target")
	req := &csi.NodePublishVolumeRequest{
		VolumeId:   volumeID,
		TargetPath: targetPath,
		VolumeCapability: &csi.VolumeCapability{
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER,
			},
		},
		VolumeContext: map[string]string{
			"volumeID": volumeID,
			"mode":     "exclusive-local",
		},
	}

	_, err = driver.NodePublishVolume(ctx, req)
	if err == nil {
		t.Fatalf("Expected publish to fail due to replay failure during recovery")
	}
	if !strings.Contains(err.Error(), upperDir) {
		t.Errorf("Expected error to mention upper directory %s, got: %v", upperDir, err)
	}

	// Contract: critical unsaved file must NOT be deleted
	if _, statErr := os.Stat(leftoverFile); statErr != nil {
		t.Fatalf("Leftover file was discarded after failed recovery: %v", statErr)
	}
}

// TestPublishExclusiveLocal_FailsIfCleanupAfterRecoveryFails verifies that if wiping volumeDir
// after leftover upper recovery fails, publish fails naming volumeDir.
func TestPublishExclusiveLocal_FailsIfCleanupAfterRecoveryFails(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("skipping read-only directory test when running as root")
	}

	ctx := t.Context()
	server := controllertest.NewTestServer(nil)
	volumeID := "recover-cleanup-fail-vol"

	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	pb.RegisterObjectFSControllerServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(lis) }()
	defer grpcServer.Stop()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()
	client := pb.NewObjectFSControllerClient(conn)

	cacheDir := t.TempDir()
	volumeDir := filepath.Join(cacheDir, volumeID)
	upperDir := filepath.Join(volumeDir, "upper")
	lowerDir := filepath.Join(volumeDir, "lower")
	_ = os.MkdirAll(upperDir, 0755)
	_ = os.MkdirAll(lowerDir, 0755)

	leftoverFile := filepath.Join(upperDir, "leftover_work.txt")
	if err := os.WriteFile(leftoverFile, []byte("salvaged work before wipe failure"), 0644); err != nil {
		t.Fatalf("write leftover file: %v", err)
	}

	driver, err := newDriver("test-node", "passthrough://bufnet", cacheDir, 1024*1024, 10*1024*1024, 30*time.Second, 10*time.Minute, true, 30*time.Second, "", true)
	if err != nil {
		t.Fatalf("newDriver failed: %v", err)
	}
	defer func() { _ = driver.Close() }()
	driver.client = client
	driver.clientConn = conn

	// Make the directory unremovable by removing write permissions from cacheDir and upperDir
	t.Cleanup(func() {
		_ = os.Chmod(cacheDir, 0755)
		_ = os.Chmod(volumeDir, 0755)
		_ = os.Chmod(upperDir, 0755)
	})
	if err := os.Chmod(upperDir, 0555); err != nil {
		t.Fatalf("chmod upperDir: %v", err)
	}
	if err := os.Chmod(cacheDir, 0555); err != nil {
		t.Fatalf("chmod cacheDir: %v", err)
	}

	targetPath := filepath.Join(t.TempDir(), "target")
	req := &csi.NodePublishVolumeRequest{
		VolumeId:   volumeID,
		TargetPath: targetPath,
		VolumeCapability: &csi.VolumeCapability{
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER,
			},
		},
		VolumeContext: map[string]string{
			"volumeID": volumeID,
			"mode":     "exclusive-local",
		},
	}

	_, err = driver.NodePublishVolume(ctx, req)
	if err == nil {
		t.Fatalf("Expected NodePublishVolume to fail when cleaning up volumeDir after recovery fails")
	}
	if !strings.Contains(err.Error(), volumeDir) {
		t.Errorf("Expected error to mention volume directory %s, got: %v", volumeDir, err)
	}
	if !strings.Contains(err.Error(), "failed to cleanup volume directory") {
		t.Errorf("Expected error to mention failed to cleanup volume directory, got: %v", err)
	}

	// Verify that the leftover work was still recovered and replayed before the wipe failed
	lookupResp, err := client.Lookup(ctx, &pb.LookupRequest{
		VolumeId:    volumeID,
		ParentInode: 1,
		Name:        "leftover_work.txt",
	})
	if err != nil || lookupResp.GetError() != 0 {
		t.Fatalf("Lookup leftover_work.txt in controller failed: %v, resp: %v", err, lookupResp)
	}
	readResp, err := client.ReadFile(ctx, &pb.ReadFileRequest{
		VolumeId: volumeID,
		Inode:    lookupResp.GetAttr().GetInode().GetIno(),
		Offset:   0,
		Size:     100,
	})
	if err != nil || readResp.GetError() != 0 {
		t.Fatalf("ReadFile leftover_work.txt failed: %v", err)
	}
	if string(readResp.GetData()) != "salvaged work before wipe failure" {
		t.Errorf("Expected salvaged content, got: %q", string(readResp.GetData()))
	}
}

// TestMaterialiseVolume_Resumable verifies that materialiseVolume skips downloading files that
// are already present with matching size and mtime from a previous attempt.
func TestMaterialiseVolume_Resumable(t *testing.T) {
	ctx := t.Context()
	server := controllertest.NewTestServer(nil)
	volumeID := "test-resume-vol"

	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	pb.RegisterObjectFSControllerServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(lis) }()
	defer grpcServer.Stop()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()
	client := pb.NewObjectFSControllerClient(conn)

	// Create file in volume
	createResp, err := client.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:       volumeID,
		ParentInode:    1,
		Name:           "resumed_file.txt",
		Mode:           0644,
		InitialContent: []byte("resumed content"),
	})
	if err != nil || createResp.GetError() != 0 {
		t.Fatalf("CreateFile: %v, resp: %v", err, createResp)
	}
	fileMtime := createResp.GetAttr().GetInode().GetMtime().AsTime()

	driver, err := newDriver("test-node", "passthrough://bufnet", t.TempDir(), 1024*1024, 10*1024*1024, 30*time.Second, 10*time.Minute, true, 30*time.Second, "", true)
	if err != nil {
		t.Fatalf("newDriver: %v", err)
	}
	defer func() { _ = driver.Close() }()

	lowerPath := filepath.Join(t.TempDir(), "lower")
	_ = os.MkdirAll(lowerPath, 0755)

	// Pre-create the file in lowerPath with matching size and mtime
	targetFile := filepath.Join(lowerPath, "resumed_file.txt")
	if err := os.WriteFile(targetFile, []byte("resumed content"), 0644); err != nil {
		t.Fatalf("write target: %v", err)
	}
	if err := os.Chtimes(targetFile, fileMtime, fileMtime); err != nil {
		t.Fatalf("chtimes: %v", err)
	}
	initialStat, err := os.Stat(targetFile)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}

	// Materialise
	if err := driver.materialiseVolume(ctx, client, volumeID, lowerPath); err != nil {
		t.Fatalf("materialiseVolume failed: %v", err)
	}

	afterStat, err := os.Stat(targetFile)
	if err != nil {
		t.Fatalf("stat after: %v", err)
	}
	// Verify mtime remained exactly the same
	if !afterStat.ModTime().Equal(initialStat.ModTime()) {
		t.Errorf("File was re-written rather than resumed: before=%v, after=%v", initialStat.ModTime(), afterStat.ModTime())
	}
}

func TestPublishExclusiveLocal_HoldConflictReturnsAborted(t *testing.T) {
	ctx := t.Context()
	server := controllertest.NewTestServer(nil)
	volumeID := "test-conflict-publish-vol"

	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	pb.RegisterObjectFSControllerServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(lis) }()
	defer grpcServer.Stop()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	client := pb.NewObjectFSControllerClient(conn)

	// Simulate node-1 holding the exclusive lease on the controller
	_, err = server.AcquireExclusive(ctx, &pb.AcquireExclusiveRequest{
		VolumeId: volumeID,
		NodeId:   "node-1",
		MountId:  "/var/lib/kubelet/target-1",
	})
	if err != nil {
		t.Fatalf("AcquireExclusive for node-1 failed: %v", err)
	}

	// Driver on node-2 tries to publish the volume
	cacheDir := t.TempDir()
	driver := &objectFSDriver{
		nodeID:   "node-2",
		cacheDir: cacheDir,
		client:   client,
		mounts:   make(map[string]*activeMount),
	}
	defer func() { _ = driver.Close() }()

	targetPath := filepath.Join(t.TempDir(), "target")
	req := &csi.NodePublishVolumeRequest{
		VolumeId:   volumeID,
		TargetPath: targetPath,
		VolumeCapability: &csi.VolumeCapability{
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER,
			},
		},
		VolumeContext: map[string]string{
			"volumeID": volumeID,
			"mode":     "exclusive-local",
		},
	}

	_, err = driver.NodePublishVolume(ctx, req)
	if err == nil {
		t.Fatalf("Expected NodePublishVolume to fail when volume is held by node-1")
	}

	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Aborted {
		t.Fatalf("Expected codes.Aborted for kubelet retry backoff, got: %v (st=%v)", err, st)
	}
	if !strings.Contains(st.Message(), "node-1") {
		t.Errorf("Expected error to mention holder node-1, got: %s", st.Message())
	}
}

func TestUnpublishExclusiveLocal_ReleasesHoldAfterFinalCheckpoint(t *testing.T) {
	ctx := t.Context()
	server := controllertest.NewTestServer(nil)
	volumeID := "test-unpublish-release-vol"

	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	pb.RegisterObjectFSControllerServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(lis) }()
	defer grpcServer.Stop()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	client := pb.NewObjectFSControllerClient(conn)

	targetPath := filepath.Join(t.TempDir(), "target")
	mountID := targetPath
	cacheDir := t.TempDir()
	volumeDir := filepath.Join(cacheDir, volumeID)
	lowerDir := filepath.Join(volumeDir, "lower")
	upperDir := filepath.Join(volumeDir, "upper")
	_ = os.MkdirAll(lowerDir, 0755)
	_ = os.MkdirAll(upperDir, 0755)

	// Acquire hold initially on controller
	_, err = server.AcquireExclusive(ctx, &pb.AcquireExclusiveRequest{
		VolumeId: volumeID,
		NodeId:   "node-1",
		MountId:  mountID,
	})
	if err != nil {
		t.Fatalf("AcquireExclusive failed: %v", err)
	}

	// Verify hold is active
	holdResp, err := server.GetExclusiveHold(ctx, &pb.GetExclusiveHoldRequest{VolumeId: volumeID})
	if err != nil || holdResp.GetHold() == nil || holdResp.GetHold().GetNodeId() != "node-1" {
		t.Fatalf("Expected active hold on node-1, got: %v", holdResp.GetHold())
	}

	driver := &objectFSDriver{
		nodeID:   "node-1",
		cacheDir: cacheDir,
		client:   client,
		mounts:   make(map[string]*activeMount),
	}
	defer func() { _ = driver.Close() }()

	mountInfo := &activeMount{
		volumeID:  volumeID,
		mode:      "exclusive-local",
		volumeDir: volumeDir,
		mountID:   mountID,
	}

	// Unpublish volume
	unpubReq := &csi.NodeUnpublishVolumeRequest{
		VolumeId:   volumeID,
		TargetPath: targetPath,
	}
	_, err = driver.unpublishExclusiveLocal(ctx, unpubReq, volumeID, volumeDir, mountInfo)
	if err != nil {
		t.Fatalf("unpublishExclusiveLocal failed: %v", err)
	}

	// Verify hold was released after unpublish
	holdRespAfter, err := server.GetExclusiveHold(ctx, &pb.GetExclusiveHoldRequest{VolumeId: volumeID})
	if err != nil {
		t.Fatalf("GetExclusiveHold failed: %v", err)
	}
	if holdRespAfter.GetHold() != nil {
		t.Errorf("Expected hold to be released after unpublish, but hold remains: %v", holdRespAfter.GetHold())
	}
}

func TestUnpublishExclusiveLocal_ReplayFailurePreservesHoldUntilRetrySucceeds(t *testing.T) {
	ctx := t.Context()
	server := controllertest.NewTestServer(nil)
	volumeID := "test-unpublish-retry-hold-vol"

	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	fakeServer := &failingReplayServer{Server: server}
	pb.RegisterObjectFSControllerServer(grpcServer, fakeServer)
	go func() { _ = grpcServer.Serve(lis) }()
	defer grpcServer.Stop()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	client := pb.NewObjectFSControllerClient(conn)

	targetPath := filepath.Join(t.TempDir(), "target")
	mountID := targetPath
	cacheDir := t.TempDir()
	volumeDir := filepath.Join(cacheDir, volumeID)
	lowerDir := filepath.Join(volumeDir, "lower")
	upperDir := filepath.Join(volumeDir, "upper")
	_ = os.MkdirAll(lowerDir, 0755)
	_ = os.MkdirAll(upperDir, 0755)

	// Write uncommitted file in upper
	if err := os.WriteFile(filepath.Join(upperDir, "uncommitted.txt"), []byte("important writes"), 0644); err != nil {
		t.Fatalf("write upper: %v", err)
	}

	// 1. Acquire hold initially on controller
	_, err = server.AcquireExclusive(ctx, &pb.AcquireExclusiveRequest{
		VolumeId: volumeID,
		NodeId:   "node-1",
		MountId:  mountID,
	})
	if err != nil {
		t.Fatalf("AcquireExclusive failed: %v", err)
	}

	driver := &objectFSDriver{
		nodeID:   "node-1",
		cacheDir: cacheDir,
		client:   client,
		mounts:   make(map[string]*activeMount),
	}
	defer func() { _ = driver.Close() }()

	var syncWg sync.WaitGroup
	syncCtx, syncCancel := context.WithCancel(t.Context())
	defer syncCancel()
	var renewWg sync.WaitGroup
	renewCtx, renewCancel := context.WithCancel(t.Context())
	defer renewCancel()

	// Start background renew ticker
	driver.startExclusiveLocalSync(syncCtx, &syncWg, renewCtx, &renewWg, client, volumeID, volumeDir, lowerDir, upperDir, mountID, 0)

	mountInfo := &activeMount{
		volumeID:    volumeID,
		mode:        "exclusive-local",
		volumeDir:   volumeDir,
		mountID:     mountID,
		cancelFunc:  syncCancel,
		syncWg:      &syncWg,
		renewCancel: renewCancel,
		renewWg:     &renewWg,
	}

	// 2. First unpublish attempt: fails because failingReplayServer rejects CreateFile in replay
	unpubReq := &csi.NodeUnpublishVolumeRequest{
		VolumeId:   volumeID,
		TargetPath: targetPath,
	}
	_, err = driver.unpublishExclusiveLocal(ctx, unpubReq, volumeID, volumeDir, mountInfo)
	if err == nil {
		t.Fatalf("Expected unpublish to fail during replay")
	}

	// 3. Verify hold is STILL ALIVE on controller (must NOT be released when uncommitted writes remain!)
	holdResp, err := server.GetExclusiveHold(ctx, &pb.GetExclusiveHoldRequest{VolumeId: volumeID})
	if err != nil {
		t.Fatalf("GetExclusiveHold failed: %v", err)
	}
	if holdResp.GetHold() == nil || holdResp.GetHold().GetNodeId() != "node-1" {
		t.Fatalf("Expected hold to remain active on node-1 after replay failure, got: %v", holdResp.GetHold())
	}

	// Verify renew loop is still alive and running (renewCancel was not called)
	if renewCtx.Err() != nil {
		t.Fatalf("renewCtx was canceled prematurely on failed unpublish: %v", renewCtx.Err())
	}

	// 4. Second unpublish attempt (retry) with healthy server
	healthyLis := bufconn.Listen(1024 * 1024)
	healthyGrpcServer := grpc.NewServer()
	pb.RegisterObjectFSControllerServer(healthyGrpcServer, server)
	go func() { _ = healthyGrpcServer.Serve(healthyLis) }()
	defer healthyGrpcServer.Stop()

	healthyConn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return healthyLis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to dial healthy server: %v", err)
	}
	defer func() { _ = healthyConn.Close() }()
	driver.client = pb.NewObjectFSControllerClient(healthyConn)

	// Retry unpublish
	_, err = driver.unpublishExclusiveLocal(ctx, unpubReq, volumeID, volumeDir, mountInfo)
	if err != nil {
		t.Fatalf("Retry unpublish failed: %v", err)
	}

	// 5. Verify hold is now released and renew loop was stopped
	holdRespAfter, err := server.GetExclusiveHold(ctx, &pb.GetExclusiveHoldRequest{VolumeId: volumeID})
	if err != nil {
		t.Fatalf("GetExclusiveHold failed: %v", err)
	}
	if holdRespAfter.GetHold() != nil {
		t.Errorf("Expected hold to be released after successful unpublish retry, got: %v", holdRespAfter.GetHold())
	}
	if renewCtx.Err() == nil {
		t.Errorf("Expected renewCtx to be canceled after successful unpublish")
	}
}

func TestMaterialiseVolume_XAttrs(t *testing.T) {
	ctx := t.Context()
	server := controllertest.NewTestServer(nil)
	defer func() { _ = server.Close() }()
	volumeID := "test-mat-xattr-vol"

	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	pb.RegisterObjectFSControllerServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(lis) }()
	defer grpcServer.Stop()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient failed: %v", err)
	}
	defer conn.Close()
	client := pb.NewObjectFSControllerClient(conn)

	// Create /file.txt and /dir
	fileResp, err := client.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:       volumeID,
		ParentInode:    1,
		Name:           "file.txt",
		Mode:           0644,
		InitialContent: []byte("content"),
	})
	if err != nil || fileResp.GetError() != 0 {
		t.Fatalf("CreateFile failed: %v, resp: %v", err, fileResp)
	}
	fileIno := fileResp.GetAttr().GetInode().GetIno()

	dirResp, err := client.Mkdir(ctx, &pb.MkdirRequest{
		VolumeId:    volumeID,
		ParentInode: 1,
		Name:        "dir",
		Mode:        0755,
	})
	if err != nil || dirResp.GetError() != 0 {
		t.Fatalf("Mkdir failed: %v, resp: %v", err, dirResp)
	}
	dirIno := dirResp.GetAttr().GetInode().GetIno()

	// Set extended attributes on file and dir
	_, err = client.SetXAttr(ctx, &pb.SetXAttrRequest{
		VolumeId: volumeID,
		Inode:    fileIno,
		Name:     "user.file_meta",
		Value:    []byte("file_val_123"),
		Flags:    pb.SetXAttrRequest_FLAGS_NONE,
	})
	if err != nil {
		t.Fatalf("SetXAttr on file failed: %v", err)
	}

	_, err = client.SetXAttr(ctx, &pb.SetXAttrRequest{
		VolumeId: volumeID,
		Inode:    dirIno,
		Name:     "user.dir_meta",
		Value:    []byte("dir_val_456"),
		Flags:    pb.SetXAttrRequest_FLAGS_NONE,
	})
	if err != nil {
		t.Fatalf("SetXAttr on dir failed: %v", err)
	}

	driver, err := newDriver("test-node", "passthrough://bufnet", t.TempDir(), 1024*1024, 10*1024*1024, 30*time.Second, 10*time.Minute, true, 30*time.Second, "", true)
	if err != nil {
		t.Fatalf("newDriver failed: %v", err)
	}
	defer func() { _ = driver.Close() }()

	lowerPath := filepath.Join(t.TempDir(), "lower")
	_ = os.MkdirAll(lowerPath, 0755)

	if err := driver.materialiseVolume(ctx, client, volumeID, lowerPath); err != nil {
		t.Fatalf("materialiseVolume failed: %v", err)
	}

	// Verify xattrs in lowerPath
	fileBuf := make([]byte, 64)
	sz, err := unix.Lgetxattr(filepath.Join(lowerPath, "file.txt"), "user.file_meta", fileBuf)
	if err != nil {
		t.Fatalf("getxattr on materialised file.txt failed: %v", err)
	}
	if string(fileBuf[:sz]) != "file_val_123" {
		t.Errorf("file.txt xattr mismatch: got %q, expected %q", string(fileBuf[:sz]), "file_val_123")
	}

	dirBuf := make([]byte, 64)
	sz, err = unix.Lgetxattr(filepath.Join(lowerPath, "dir"), "user.dir_meta", dirBuf)
	if err != nil {
		t.Fatalf("getxattr on materialised dir failed: %v", err)
	}
	if string(dirBuf[:sz]) != "dir_val_456" {
		t.Errorf("dir xattr mismatch: got %q, expected %q", string(dirBuf[:sz]), "dir_val_456")
	}
}

func TestMaterialiseVolume_FailingSetXAttrReturnsError(t *testing.T) {
	ctx := t.Context()
	server := controllertest.NewTestServer(nil)
	defer func() { _ = server.Close() }()
	volumeID := "test-mat-fail-xattr-vol"

	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	pb.RegisterObjectFSControllerServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(lis) }()
	defer grpcServer.Stop()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient failed: %v", err)
	}
	defer conn.Close()
	client := pb.NewObjectFSControllerClient(conn)

	fileResp, err := client.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:       volumeID,
		ParentInode:    1,
		Name:           "file.txt",
		Mode:           0644,
		InitialContent: []byte("content"),
	})
	if err != nil || fileResp.GetError() != 0 {
		t.Fatalf("CreateFile failed: %v, resp: %v", err, fileResp)
	}
	fileIno := fileResp.GetAttr().GetInode().GetIno()

	_, err = client.SetXAttr(ctx, &pb.SetXAttrRequest{
		VolumeId: volumeID,
		Inode:    fileIno,
		Name:     "user.test_attr",
		Value:    []byte("val"),
		Flags:    pb.SetXAttrRequest_FLAGS_NONE,
	})
	if err != nil {
		t.Fatalf("SetXAttr failed: %v", err)
	}

	driver, err := newDriver("test-node", "passthrough://bufnet", t.TempDir(), 1024*1024, 10*1024*1024, 30*time.Second, 10*time.Minute, true, 30*time.Second, "", true)
	if err != nil {
		t.Fatalf("newDriver failed: %v", err)
	}
	defer func() { _ = driver.Close() }()

	lowerPath := filepath.Join(t.TempDir(), "lower")
	_ = os.MkdirAll(lowerPath, 0755)

	origSet := unixLsetxattr
	t.Cleanup(func() { unixLsetxattr = origSet })
	unixLsetxattr = func(path string, attr string, data []byte, flags int) error {
		return syscall.EPERM
	}

	err = driver.materialiseVolume(ctx, client, volumeID, lowerPath)
	if err == nil {
		t.Fatalf("expected materialiseVolume to fail when setxattr fails, got nil")
	}
	if !strings.Contains(err.Error(), "setxattr") || !strings.Contains(err.Error(), "user.test_attr") || !strings.Contains(err.Error(), "file.txt") {
		t.Errorf("expected error naming path and attribute, got: %v", err)
	}
}
