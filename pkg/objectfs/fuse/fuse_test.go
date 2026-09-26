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

package fuse

import (
	"context"
	"net"
	"syscall"
	"testing"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/controller"
	"github.com/hanwen/go-fuse/v2/fuse"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func createTestClient(t *testing.T) (pb.ObjectFSControllerClient, func()) {
	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	server := controller.NewServer(nil)
	pb.RegisterObjectFSControllerServer(grpcServer, server)

	go func() {
		_ = grpcServer.Serve(lis)
	}()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}

	client := pb.NewObjectFSControllerClient(conn)
	cleanup := func() {
		_ = conn.Close()
		grpcServer.Stop()
		_ = lis.Close()
	}
	return client, cleanup
}

func TestRawFileSystemOperations(t *testing.T) {
	client, cleanup := createTestClient(t)
	defer cleanup()

	cache := NewNodeCache(1024 * 1024)
	rawFS := NewObjectFS(client, "vol-1", pb.WriteMode_WRITE_THROUGH_FSYNC, cache)

	// 1. Getattr on root
	var attrOut fuse.AttrOut
	if status := rawFS.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}}, &attrOut); status != fuse.OK {
		t.Fatalf("Getattr on root failed: %v", status)
	}
	if attrOut.Attr.Mode&syscall.S_IFDIR == 0 {
		t.Fatalf("Expected root to have S_IFDIR mode, got: %o", attrOut.Attr.Mode)
	}

	// 2. Mkdir "docs"
	var docsEntryOut fuse.EntryOut
	if status := rawFS.Mkdir(nil, &fuse.MkdirIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Mode: 0755}, "docs", &docsEntryOut); status != fuse.OK {
		t.Fatalf("Mkdir docs failed: %v", status)
	}
	if docsEntryOut.NodeId == 0 {
		t.Fatalf("Expected valid Inode id in EntryOut")
	}

	// 3. Create file "docs/readme.txt"
	var fileCreateOut fuse.CreateOut
	if status := rawFS.Create(nil, &fuse.CreateIn{InHeader: fuse.InHeader{NodeId: docsEntryOut.NodeId}, Mode: 0644}, "readme.txt", &fileCreateOut); status != fuse.OK {
		t.Fatalf("Create file failed: %v", status)
	}
	fileID := fileCreateOut.EntryOut.NodeId
	if fileID == 0 {
		t.Fatalf("Expected valid file Inode id")
	}

	// 4. Write data to file
	testData := []byte("Hello ObjectFS Raw FUSE!")
	written, status := rawFS.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: fileID}}, testData)
	if status != fuse.OK {
		t.Fatalf("Write failed: %v", status)
	}
	if int(written) != len(testData) {
		t.Fatalf("Expected %d bytes written, got %d", len(testData), written)
	}

	// 5. Read data back
	readBuf := make([]byte, 64)
	readRes, status := rawFS.Read(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: fileID}, Size: 64}, readBuf)
	if status != fuse.OK {
		t.Fatalf("Read failed: %v", status)
	}
	resBytes, readStatus := readRes.Bytes(readBuf)
	if readStatus != fuse.OK {
		t.Fatalf("Read result status not OK: %v", readStatus)
	}
	if string(resBytes) != string(testData) {
		t.Fatalf("Read data mismatch: got %q, want %q", string(resBytes), string(testData))
	}

	// 6. ReadDir on "docs"
	dirEntries := fuse.NewDirEntryList(make([]byte, 4096), 0)
	if status := rawFS.ReadDir(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: docsEntryOut.NodeId}}, dirEntries); status != fuse.OK {
		t.Fatalf("ReadDir failed: %v", status)
	}

	// 7. Flush / Fsync
	if status := rawFS.Flush(nil, &fuse.FlushIn{InHeader: fuse.InHeader{NodeId: fileID}}); status != fuse.OK {
		t.Fatalf("Flush failed: %v", status)
	}
	if status := rawFS.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: fileID}}); status != fuse.OK {
		t.Fatalf("Fsync failed: %v", status)
	}

	// 8. Lookup "readme.txt" in "docs"
	var lookupOut fuse.EntryOut
	if status := rawFS.Lookup(nil, &fuse.InHeader{NodeId: docsEntryOut.NodeId}, "readme.txt", &lookupOut); status != fuse.OK {
		t.Fatalf("Lookup failed: %v", status)
	}
	if lookupOut.NodeId != fileID {
		t.Fatalf("Lookup returned node %d, want %d", lookupOut.NodeId, fileID)
	}

	// 8a. Rmdir "docs" while non-empty should fail with ENOTEMPTY
	if st := rawFS.Rmdir(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "docs"); st != fuse.Status(syscall.ENOTEMPTY) {
		t.Fatalf("Rmdir on non-empty dir: expected ENOTEMPTY, got %v", st)
	}

	// 8b. Unlink "docs" (a directory) should fail with EISDIR
	if st := rawFS.Unlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "docs"); st != fuse.Status(syscall.EISDIR) {
		t.Fatalf("Unlink on directory: expected EISDIR, got %v", st)
	}

	// 8c. Rmdir "readme.txt" (a file) should fail with ENOTDIR
	if st := rawFS.Rmdir(nil, &fuse.InHeader{NodeId: docsEntryOut.NodeId}, "readme.txt"); st != fuse.Status(syscall.ENOTDIR) {
		t.Fatalf("Rmdir on file: expected ENOTDIR, got %v", st)
	}

	// 9. Unlink "readme.txt"
	if status := rawFS.Unlink(nil, &fuse.InHeader{NodeId: docsEntryOut.NodeId}, "readme.txt"); status != fuse.OK {
		t.Fatalf("Unlink failed: %v", status)
	}

	// 10. Rmdir "docs"
	if status := rawFS.Rmdir(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "docs"); status != fuse.OK {
		t.Fatalf("Rmdir failed: %v", status)
	}
}

func TestFUSERmdirAndUnlinkErrors(t *testing.T) {
	client, cleanup := createTestClient(t)
	defer cleanup()

	cache := NewNodeCache(1024 * 1024)
	rawFS := NewObjectFS(client, "vol-errors", pb.WriteMode_WRITE_THROUGH_FSYNC, cache)

	// Create directory /dir and child /dir/file
	var dirOut fuse.EntryOut
	if st := rawFS.Mkdir(nil, &fuse.MkdirIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Mode: 0755}, "dir", &dirOut); st != fuse.OK {
		t.Fatalf("Mkdir failed: %v", st)
	}

	var fileOut fuse.CreateOut
	if st := rawFS.Create(nil, &fuse.CreateIn{InHeader: fuse.InHeader{NodeId: dirOut.NodeId}, Mode: 0644}, "file", &fileOut); st != fuse.OK {
		t.Fatalf("Create failed: %v", st)
	}

	// 1. Rmdir non-empty directory returns ENOTEMPTY
	if st := rawFS.Rmdir(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "dir"); st != fuse.Status(syscall.ENOTEMPTY) {
		t.Fatalf("Expected ENOTEMPTY for non-empty dir, got %v", st)
	}

	// 2. Unlink directory returns EISDIR
	if st := rawFS.Unlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "dir"); st != fuse.Status(syscall.EISDIR) {
		t.Fatalf("Expected EISDIR for unlinking directory, got %v", st)
	}

	// 3. Rmdir regular file returns ENOTDIR
	if st := rawFS.Rmdir(nil, &fuse.InHeader{NodeId: dirOut.NodeId}, "file"); st != fuse.Status(syscall.ENOTDIR) {
		t.Fatalf("Expected ENOTDIR for rmdir on regular file, got %v", st)
	}

	// 4. Rmdir nonexistent returns ENOENT
	if st := rawFS.Rmdir(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "nonexistent"); st != fuse.ENOENT {
		t.Fatalf("Expected ENOENT for nonexistent dir, got %v", st)
	}

	// 5. Unlink child file
	if st := rawFS.Unlink(nil, &fuse.InHeader{NodeId: dirOut.NodeId}, "file"); st != fuse.OK {
		t.Fatalf("Unlink file failed: %v", st)
	}

	// 6. Rmdir now-empty directory succeeds
	if st := rawFS.Rmdir(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "dir"); st != fuse.OK {
		t.Fatalf("Rmdir on empty dir failed: %v", st)
	}
}

func TestGrpcErrorToStatus(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected fuse.Status
	}{
		{"nil error", nil, fuse.OK},
		{"not found", status.Error(codes.NotFound, "not found"), fuse.ENOENT},
		{"already exists", status.Error(codes.AlreadyExists, "already exists"), fuse.Status(syscall.EEXIST)},
		{"invalid argument", status.Error(codes.InvalidArgument, "invalid argument"), fuse.EINVAL},
		{"permission denied", status.Error(codes.PermissionDenied, "permission denied"), fuse.EACCES},
		{"unauthenticated", status.Error(codes.Unauthenticated, "unauthenticated"), fuse.EACCES},
		{"unimplemented", status.Error(codes.Unimplemented, "unimplemented"), fuse.ENOSYS},
		{"deadline exceeded", status.Error(codes.DeadlineExceeded, "deadline"), fuse.Status(syscall.ETIMEDOUT)},
		{"canceled", status.Error(codes.Canceled, "canceled"), fuse.Status(syscall.EINTR)},
		{"resource exhausted", status.Error(codes.ResourceExhausted, "out of space"), fuse.Status(syscall.ENOSPC)},
		{"aborted", status.Error(codes.Aborted, "aborted"), fuse.Status(syscall.EBUSY)},
		{"failed precondition not empty", status.Error(codes.FailedPrecondition, "rmdir failed: directory /foo not empty: directory not empty"), fuse.Status(syscall.ENOTEMPTY)},
		{"failed precondition is dir", status.Error(codes.FailedPrecondition, "unlink failed: cannot unlink directory /foo: is a directory"), fuse.Status(syscall.EISDIR)},
		{"failed precondition not dir", status.Error(codes.FailedPrecondition, "rmdir failed: cannot rmdir non-directory /foo: not a directory"), fuse.Status(syscall.ENOTDIR)},
		{"failed precondition busy", status.Error(codes.FailedPrecondition, "cannot rmdir root: device or resource busy"), fuse.Status(syscall.EBUSY)},
		{"failed precondition generic", status.Error(codes.FailedPrecondition, "other precondition failed"), fuse.EINVAL},
		{"internal generic", status.Error(codes.Internal, "internal disk corruption"), fuse.Status(syscall.EIO)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := grpcErrorToStatus(tc.err)
			if got != tc.expected {
				t.Errorf("grpcErrorToStatus(%v) = %v, want %v", tc.err, got, tc.expected)
			}
		})
	}
}

func TestLocalWriteBufferingAndSync(t *testing.T) {
	client, cleanup := createTestClient(t)
	defer cleanup()

	cache := NewNodeCache(1024 * 1024)
	rawFS := NewObjectFS(client, "vol-buffering", pb.WriteMode_WRITE_THROUGH_FSYNC, cache)

	// Create file
	var createOut fuse.CreateOut
	if status := rawFS.Create(nil, &fuse.CreateIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Mode: 0644}, "buffered.txt", &createOut); status != fuse.OK {
		t.Fatalf("Create failed: %v", status)
	}
	fileID := createOut.EntryOut.NodeId

	// Write locally
	localData := []byte("buffered content not yet on service")
	written, status := rawFS.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: fileID}}, localData)
	if status != fuse.OK || int(written) != len(localData) {
		t.Fatalf("Write failed: status=%v, written=%d", status, written)
	}

	// Verify local cache has it and marks it dirty
	entry, isDirty := cache.GetDirty("/buffered.txt")
	if !isDirty || string(entry.Data) != string(localData) {
		t.Fatalf("Expected dirty cache entry with local data")
	}

	// Direct controller ReadFile before sync should NOT have the written data yet (it has 0 bytes initial)
	ctx := t.Context()
	ctrlResp, err := client.ReadFile(ctx, &pb.ReadFileRequest{
		VolumeId: "vol-buffering",
		Path:     "/buffered.txt",
		Offset:   0,
		Size:     1024,
	})
	if err != nil {
		t.Fatalf("Controller read failed: %v", err)
	}
	if len(ctrlResp.GetData()) != 0 {
		t.Fatalf("Expected controller to have 0 bytes before sync, got: %q", string(ctrlResp.GetData()))
	}

	// Now call Flush / Fsync on the FUSE layer
	if status := rawFS.Flush(nil, &fuse.FlushIn{InHeader: fuse.InHeader{NodeId: fileID}}); status != fuse.OK {
		t.Fatalf("Flush failed: %v", status)
	}

	// Controller should now have the synced data
	ctrlResp2, err := client.ReadFile(ctx, &pb.ReadFileRequest{
		VolumeId: "vol-buffering",
		Path:     "/buffered.txt",
		Offset:   0,
		Size:     1024,
	})
	if err != nil {
		t.Fatalf("Controller read after flush failed: %v", err)
	}
	if string(ctrlResp2.GetData()) != string(localData) {
		t.Fatalf("Expected controller to have %q, got %q", string(localData), string(ctrlResp2.GetData()))
	}

	// Cache entry should no longer be dirty
	if _, isDirty := cache.GetDirty("/buffered.txt"); isDirty {
		t.Fatalf("Expected cache entry to be marked clean after flush")
	}
}
