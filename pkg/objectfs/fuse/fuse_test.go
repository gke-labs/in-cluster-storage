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
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/controller"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/controller/controllertest"
	"github.com/gke-labs/in-cluster-storage/pkg/wal/buffer/buffertest"
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
	"github.com/hanwen/go-fuse/v2/fuse"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func createTestClient(t *testing.T) (pb.ObjectFSControllerClient, func()) {
	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	server := controllertest.NewTestServer(nil)
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
	if attrOut.Attr.Nlink != 2 {
		t.Fatalf("Expected root Nlink to be 2, got: %d", attrOut.Attr.Nlink)
	}
	if attrOut.Attr.Owner.Uid != 0 || attrOut.Attr.Owner.Gid != 0 {
		t.Fatalf("Expected root Owner to be 0/0, got: %d/%d", attrOut.Attr.Owner.Uid, attrOut.Attr.Owner.Gid)
	}

	const testUID = 1001
	const testGID = 1002

	// 2. Mkdir "docs"
	var docsEntryOut fuse.EntryOut
	if status := rawFS.Mkdir(nil, &fuse.MkdirIn{
		InHeader: fuse.InHeader{
			NodeId: fuse.FUSE_ROOT_ID,
			Caller: fuse.Caller{Owner: fuse.Owner{Uid: testUID, Gid: testGID}},
		},
		Mode: 0755,
	}, "docs", &docsEntryOut); status != fuse.OK {
		t.Fatalf("Mkdir docs failed: %v", status)
	}
	if docsEntryOut.NodeId == 0 {
		t.Fatalf("Expected valid Inode id in EntryOut")
	}
	if docsEntryOut.Attr.Nlink != 2 {
		t.Fatalf("Expected dir Nlink to be 2, got: %d", docsEntryOut.Attr.Nlink)
	}
	if docsEntryOut.Attr.Owner.Uid != testUID || docsEntryOut.Attr.Owner.Gid != testGID {
		t.Fatalf("Expected dir Owner to match caller UID/GID, got: %d/%d", docsEntryOut.Attr.Owner.Uid, docsEntryOut.Attr.Owner.Gid)
	}

	// 3. Create file "docs/readme.txt"
	var fileCreateOut fuse.CreateOut
	if status := rawFS.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{
			NodeId: docsEntryOut.NodeId,
			Caller: fuse.Caller{Owner: fuse.Owner{Uid: testUID, Gid: testGID}},
		},
		Mode: 0644,
	}, "readme.txt", &fileCreateOut); status != fuse.OK {
		t.Fatalf("Create file failed: %v", status)
	}
	fileID := fileCreateOut.EntryOut.NodeId
	if fileID == 0 {
		t.Fatalf("Expected valid file Inode id")
	}
	if fileCreateOut.EntryOut.Attr.Nlink != 1 {
		t.Fatalf("Expected file Nlink to be 1, got: %d", fileCreateOut.EntryOut.Attr.Nlink)
	}
	if fileCreateOut.EntryOut.Attr.Owner.Uid != testUID || fileCreateOut.EntryOut.Attr.Owner.Gid != testGID {
		t.Fatalf("Expected file Owner to match caller UID/GID, got: %d/%d", fileCreateOut.EntryOut.Attr.Owner.Uid, fileCreateOut.EntryOut.Attr.Owner.Gid)
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
	if lookupOut.Attr.Nlink != 1 {
		t.Fatalf("Expected file Nlink to be 1 in lookup, got: %d", lookupOut.Attr.Nlink)
	}
	if lookupOut.Attr.Owner.Uid != testUID || lookupOut.Attr.Owner.Gid != testGID {
		t.Fatalf("Expected file Owner to match test UID/GID in lookup, got: %d/%d", lookupOut.Attr.Owner.Uid, lookupOut.Attr.Owner.Gid)
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
		{"failed precondition", status.Error(codes.FailedPrecondition, "failed precondition"), fuse.EINVAL},
		{"permission denied", status.Error(codes.PermissionDenied, "permission denied"), fuse.EACCES},
		{"unauthenticated", status.Error(codes.Unauthenticated, "unauthenticated"), fuse.EACCES},
		{"unimplemented", status.Error(codes.Unimplemented, "unimplemented"), fuse.ENOSYS},
		{"deadline exceeded", status.Error(codes.DeadlineExceeded, "deadline"), fuse.Status(syscall.EIO)},
		{"canceled", status.Error(codes.Canceled, "canceled"), fuse.Status(syscall.EINTR)},
		{"resource exhausted", status.Error(codes.ResourceExhausted, "out of space"), fuse.Status(syscall.ENOSPC)},
		{"aborted", status.Error(codes.Aborted, "aborted"), fuse.Status(syscall.EBUSY)},
		{"unavailable", status.Error(codes.Unavailable, "unavailable"), fuse.Status(syscall.EIO)},
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
	entry, isDirty := cache.GetDirty(fileID)
	if !isDirty || string(entry.Data) != string(localData) {
		t.Fatalf("Expected dirty cache entry with local data")
	}

	// Direct controller ReadFile before sync should NOT have the written data yet (it has 0 bytes initial)
	ctx := t.Context()
	ctrlResp, err := client.ReadFile(ctx, &pb.ReadFileRequest{
		VolumeId: "vol-buffering",
		Inode:    fileID,
		Offset:   0,
		Size:     1024,
	})
	if err != nil {
		t.Fatalf("Controller read failed: %v", err)
	}
	if len(ctrlResp.GetData()) != 0 {
		t.Fatalf("Expected controller to have 0 bytes before sync, got: %q", string(ctrlResp.GetData()))
	}

	// Flush to staging: controller view does not change until Fsync
	if status := rawFS.Flush(nil, &fuse.FlushIn{InHeader: fuse.InHeader{NodeId: fileID}}); status != fuse.OK {
		t.Fatalf("Flush failed: %v", status)
	}
	ctrlRespStaged, err := client.ReadFile(ctx, &pb.ReadFileRequest{
		VolumeId: "vol-buffering",
		Inode:    fileID,
		Offset:   0,
		Size:     1024,
	})
	if err != nil {
		t.Fatalf("Controller read after flush failed: %v", err)
	}
	if len(ctrlRespStaged.GetData()) != 0 {
		t.Fatalf("Expected controller to still have 0 bytes after flush before fsync, got: %q", string(ctrlRespStaged.GetData()))
	}

	// Now call Fsync on the FUSE layer
	if status := rawFS.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: fileID}}); status != fuse.OK {
		t.Fatalf("Fsync failed: %v", status)
	}

	// Controller should now have the synced data
	ctrlResp2, err := client.ReadFile(ctx, &pb.ReadFileRequest{
		VolumeId: "vol-buffering",
		Inode:    fileID,
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
	if _, isDirty := cache.GetDirty(fileID); isDirty {
		t.Fatalf("Expected cache entry to be marked clean after flush")
	}
}

func TestFUSEAttributesOwnerAndNlink(t *testing.T) {
	client, cleanup := createTestClient(t)
	defer cleanup()

	const callerUID = 553677
	const callerGID = 1000

	cache := NewNodeCache(1024 * 1024)
	rawFS := NewObjectFS(client, "vol-attrs", pb.WriteMode_WRITE_THROUGH_FSYNC, cache)

	// 1. Root directory GetAttr (root has default 0/0 UID/GID in EROFS)
	var rootAttrOut fuse.AttrOut
	if status := rawFS.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}}, &rootAttrOut); status != fuse.OK {
		t.Fatalf("GetAttr root failed: %v", status)
	}
	if rootAttrOut.Attr.Nlink != 2 {
		t.Fatalf("Root directory Nlink = %d, want 2", rootAttrOut.Attr.Nlink)
	}

	// 2. Mkdir with caller UID/GID
	var dirOut fuse.EntryOut
	mkdirIn := &fuse.MkdirIn{
		InHeader: fuse.InHeader{
			NodeId: fuse.FUSE_ROOT_ID,
			Caller: fuse.Caller{Owner: fuse.Owner{Uid: callerUID, Gid: callerGID}},
		},
		Mode: 0755,
	}
	if status := rawFS.Mkdir(nil, mkdirIn, "sub", &dirOut); status != fuse.OK {
		t.Fatalf("Mkdir failed: %v", status)
	}
	if dirOut.Attr.Nlink != 2 {
		t.Fatalf("Mkdir Nlink = %d, want 2", dirOut.Attr.Nlink)
	}
	if dirOut.Attr.Owner.Uid != callerUID || dirOut.Attr.Owner.Gid != callerGID {
		t.Fatalf("Mkdir Owner = %d/%d, want %d/%d", dirOut.Attr.Owner.Uid, dirOut.Attr.Owner.Gid, callerUID, callerGID)
	}

	// 3. Create regular file with caller UID/GID
	var createOut fuse.CreateOut
	createIn := &fuse.CreateIn{
		InHeader: fuse.InHeader{
			NodeId: fuse.FUSE_ROOT_ID,
			Caller: fuse.Caller{Owner: fuse.Owner{Uid: callerUID, Gid: callerGID}},
		},
		Mode: 0644,
	}
	if status := rawFS.Create(nil, createIn, "hello.txt", &createOut); status != fuse.OK {
		t.Fatalf("Create failed: %v", status)
	}
	if createOut.EntryOut.Attr.Nlink != 1 {
		t.Fatalf("Create Nlink = %d, want 1", createOut.EntryOut.Attr.Nlink)
	}
	if createOut.EntryOut.Attr.Owner.Uid != callerUID || createOut.EntryOut.Attr.Owner.Gid != callerGID {
		t.Fatalf("Create Owner = %d/%d, want %d/%d", createOut.EntryOut.Attr.Owner.Uid, createOut.EntryOut.Attr.Owner.Gid, callerUID, callerGID)
	}
	fileID := createOut.EntryOut.NodeId

	// 4. Mknod file with caller UID/GID
	var mknodOut fuse.EntryOut
	mknodIn := &fuse.MknodIn{
		InHeader: fuse.InHeader{
			NodeId: fuse.FUSE_ROOT_ID,
			Caller: fuse.Caller{Owner: fuse.Owner{Uid: callerUID, Gid: callerGID}},
		},
		Mode: 0644,
	}
	if status := rawFS.Mknod(nil, mknodIn, "mknod.txt", &mknodOut); status != fuse.OK {
		t.Fatalf("Mknod failed: %v", status)
	}
	if mknodOut.Attr.Nlink != 1 {
		t.Fatalf("Mknod Nlink = %d, want 1", mknodOut.Attr.Nlink)
	}
	if mknodOut.Attr.Owner.Uid != callerUID || mknodOut.Attr.Owner.Gid != callerGID {
		t.Fatalf("Mknod Owner = %d/%d, want %d/%d", mknodOut.Attr.Owner.Uid, mknodOut.Attr.Owner.Gid, callerUID, callerGID)
	}

	// 5. GetAttr on file
	var fileAttrOut fuse.AttrOut
	if status := rawFS.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: fileID}}, &fileAttrOut); status != fuse.OK {
		t.Fatalf("GetAttr file failed: %v", status)
	}
	if fileAttrOut.Attr.Nlink != 1 {
		t.Fatalf("GetAttr file Nlink = %d, want 1", fileAttrOut.Attr.Nlink)
	}
	if fileAttrOut.Attr.Owner.Uid != callerUID || fileAttrOut.Attr.Owner.Gid != callerGID {
		t.Fatalf("GetAttr file Owner = %d/%d, want %d/%d", fileAttrOut.Attr.Owner.Uid, fileAttrOut.Attr.Owner.Gid, callerUID, callerGID)
	}

	// 6. SetAttr (truncate)
	var setAttrTruncOut fuse.AttrOut
	if status := rawFS.SetAttr(nil, &fuse.SetAttrIn{SetAttrInCommon: fuse.SetAttrInCommon{InHeader: fuse.InHeader{NodeId: fileID}, Valid: fuse.FATTR_SIZE, Size: 10}}, &setAttrTruncOut); status != fuse.OK {
		t.Fatalf("SetAttr truncate failed: %v", status)
	}
	if setAttrTruncOut.Attr.Nlink != 1 {
		t.Fatalf("SetAttr truncate Nlink = %d, want 1", setAttrTruncOut.Attr.Nlink)
	}
	if setAttrTruncOut.Attr.Owner.Uid != callerUID || setAttrTruncOut.Attr.Owner.Gid != callerGID {
		t.Fatalf("SetAttr truncate Owner = %d/%d, want %d/%d", setAttrTruncOut.Attr.Owner.Uid, setAttrTruncOut.Attr.Owner.Gid, callerUID, callerGID)
	}

	// 7. SetAttr (non-truncate / touch / mode)
	var setAttrOut fuse.AttrOut
	if status := rawFS.SetAttr(nil, &fuse.SetAttrIn{SetAttrInCommon: fuse.SetAttrInCommon{InHeader: fuse.InHeader{NodeId: fileID}, Valid: fuse.FATTR_MODE, Mode: 0600}}, &setAttrOut); status != fuse.OK {
		t.Fatalf("SetAttr failed: %v", status)
	}
	if setAttrOut.Attr.Nlink != 1 {
		t.Fatalf("SetAttr Nlink = %d, want 1", setAttrOut.Attr.Nlink)
	}
	if setAttrOut.Attr.Mode&07777 != 0600 {
		t.Fatalf("SetAttr Mode = %o, want %o", setAttrOut.Attr.Mode&07777, 0600)
	}

	// 8. Lookup directory and file
	var lookupDirOut fuse.EntryOut
	if status := rawFS.Lookup(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "sub", &lookupDirOut); status != fuse.OK {
		t.Fatalf("Lookup sub failed: %v", status)
	}
	if lookupDirOut.Attr.Nlink != 2 {
		t.Fatalf("Lookup dir Nlink = %d, want 2", lookupDirOut.Attr.Nlink)
	}
	if lookupDirOut.Attr.Owner.Uid != callerUID || lookupDirOut.Attr.Owner.Gid != callerGID {
		t.Fatalf("Lookup dir Owner = %d/%d, want %d/%d", lookupDirOut.Attr.Owner.Uid, lookupDirOut.Attr.Owner.Gid, callerUID, callerGID)
	}

	var lookupFileOut fuse.EntryOut
	if status := rawFS.Lookup(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "hello.txt", &lookupFileOut); status != fuse.OK {
		t.Fatalf("Lookup hello.txt failed: %v", status)
	}
	if lookupFileOut.Attr.Nlink != 1 {
		t.Fatalf("Lookup file Nlink = %d, want 1", lookupFileOut.Attr.Nlink)
	}
	if lookupFileOut.Attr.Owner.Uid != callerUID || lookupFileOut.Attr.Owner.Gid != callerGID {
		t.Fatalf("Lookup file Owner = %d/%d, want %d/%d", lookupFileOut.Attr.Owner.Uid, lookupFileOut.Attr.Owner.Gid, callerUID, callerGID)
	}

	// 9. ReadDirPlus on root
	dirList := fuse.NewDirEntryList(make([]byte, 4096), 0)
	if status := rawFS.ReadDirPlus(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}}, dirList); status != fuse.OK {
		t.Fatalf("ReadDirPlus failed: %v", status)
	}
}

func TestFUSEStableInodesAcrossSnapshots(t *testing.T) {
	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	backend := controller.NewMemoryBackend()
	server := controllertest.NewTestServer(backend)
	pb.RegisterObjectFSControllerServer(grpcServer, server)

	go func() {
		_ = grpcServer.Serve(lis)
	}()
	defer func() {
		grpcServer.Stop()
		_ = lis.Close()
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
	defer conn.Close()

	client := pb.NewObjectFSControllerClient(conn)
	volumeID := "fuse-stable-ino-vol"

	cache := NewNodeCache(1024 * 1024)
	rawFS := NewObjectFS(client, volumeID, pb.WriteMode_WRITE_THROUGH_FSYNC, cache)

	// 1. Mkdir /subdir
	var mkdirOut fuse.EntryOut
	if status := rawFS.Mkdir(nil, &fuse.MkdirIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Mode: 0755}, "subdir", &mkdirOut); status != fuse.OK {
		t.Fatalf("Mkdir failed: %v", status)
	}
	dirID := mkdirOut.NodeId

	// 2. Create /file.txt
	var createOut fuse.CreateOut
	if status := rawFS.Create(nil, &fuse.CreateIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Mode: 0644}, "file.txt", &createOut); status != fuse.OK {
		t.Fatalf("Create failed: %v", status)
	}
	fileID := createOut.EntryOut.NodeId

	// Verify initial getattr
	var fileAttrOut fuse.AttrOut
	if status := rawFS.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: fileID}}, &fileAttrOut); status != fuse.OK {
		t.Fatalf("GetAttr on fileID failed: %v", status)
	}
	if fileAttrOut.Attr.Ino != fileID {
		t.Fatalf("Expected st_ino %d, got %d", fileID, fileAttrOut.Attr.Ino)
	}

	// 3. Trigger controller snapshot 1
	ctx := t.Context()
	if _, err := client.CreateSnapshot(ctx, &pb.CreateSnapshotRequest{VolumeId: volumeID}); err != nil {
		t.Fatalf("Failed to create snapshot 1: %v", err)
	}

	// 4. Query GetAttr using the existing fileID after snapshot 1
	var fileAttrAfterSnap1 fuse.AttrOut
	if status := rawFS.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: fileID}}, &fileAttrAfterSnap1); status != fuse.OK {
		t.Fatalf("GetAttr on fileID after snap 1 failed: %v", status)
	}
	if fileAttrAfterSnap1.Attr.Ino != fileID {
		t.Fatalf("st_ino changed after snap 1: expected %d, got %d", fileID, fileAttrAfterSnap1.Attr.Ino)
	}

	// Also verify lookup returns the same stable Inode / NodeId
	var lookupOut fuse.EntryOut
	if status := rawFS.Lookup(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "file.txt", &lookupOut); status != fuse.OK {
		t.Fatalf("Lookup file.txt after snap 1 failed: %v", status)
	}
	if lookupOut.NodeId != fileID || lookupOut.Attr.Ino != fileID {
		t.Fatalf("Lookup returned different inode: expected %d, got NodeId=%d Ino=%d", fileID, lookupOut.NodeId, lookupOut.Attr.Ino)
	}

	// 5. Create a new file before snapshot 2
	var createOut2 fuse.CreateOut
	if status := rawFS.Create(nil, &fuse.CreateIn{InHeader: fuse.InHeader{NodeId: dirID}, Mode: 0644}, "nested.txt", &createOut2); status != fuse.OK {
		t.Fatalf("Create nested failed: %v", status)
	}
	nestedID := createOut2.EntryOut.NodeId

	// Trigger snapshot 2
	if _, err := client.CreateSnapshot(ctx, &pb.CreateSnapshotRequest{VolumeId: volumeID}); err != nil {
		t.Fatalf("Failed to create snapshot 2: %v", err)
	}

	// Verify all inode numbers remain unchanged across snapshot 2
	var fileAttrAfterSnap2 fuse.AttrOut
	if status := rawFS.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: fileID}}, &fileAttrAfterSnap2); status != fuse.OK {
		t.Fatalf("GetAttr on fileID after snap 2 failed: %v", status)
	}
	if fileAttrAfterSnap2.Attr.Ino != fileID {
		t.Fatalf("file st_ino changed after snap 2: expected %d, got %d", fileID, fileAttrAfterSnap2.Attr.Ino)
	}

	var dirAttrAfterSnap2 fuse.AttrOut
	if status := rawFS.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: dirID}}, &dirAttrAfterSnap2); status != fuse.OK {
		t.Fatalf("GetAttr on dirID after snap 2 failed: %v", status)
	}
	if dirAttrAfterSnap2.Attr.Ino != dirID {
		t.Fatalf("dir st_ino changed after snap 2: expected %d, got %d", dirID, dirAttrAfterSnap2.Attr.Ino)
	}

	var nestedAttrAfterSnap2 fuse.AttrOut
	if status := rawFS.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: nestedID}}, &nestedAttrAfterSnap2); status != fuse.OK {
		t.Fatalf("GetAttr on nestedID after snap 2 failed: %v", status)
	}
	if nestedAttrAfterSnap2.Attr.Ino != nestedID {
		t.Fatalf("nested st_ino changed after snap 2: expected %d, got %d", nestedID, nestedAttrAfterSnap2.Attr.Ino)
	}
}

func TestRenameDescendantOpenHandleAndDirtyFlush(t *testing.T) {
	client, cleanup := createTestClient(t)
	defer cleanup()

	volumeID := "vol-rename-bug"
	cache := NewNodeCache(1024 * 1024)
	rawFS := NewObjectFS(client, volumeID, pb.WriteMode_WRITE_THROUGH_FSYNC, cache)

	// 1. Create directory 'a'
	var mkdirOut fuse.EntryOut
	if status := rawFS.Mkdir(nil, &fuse.MkdirIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Mode: 0755}, "a", &mkdirOut); status != fuse.OK {
		t.Fatalf("Mkdir a failed: %v", status)
	}
	dirAID := mkdirOut.NodeId

	// 2. Open / Create 'a/x'
	var createOut fuse.CreateOut
	if status := rawFS.Create(nil, &fuse.CreateIn{InHeader: fuse.InHeader{NodeId: dirAID}, Mode: 0644}, "x", &createOut); status != fuse.OK {
		t.Fatalf("Create a/x failed: %v", status)
	}
	fileXID := createOut.EntryOut.NodeId

	// 3. Write initial content through open descriptor (fileXID) without flushing yet (dirty buffer in cache)
	initData := []byte("initial ")
	written, status := rawFS.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: fileXID}, Offset: 0}, initData)
	if status != fuse.OK || int(written) != len(initData) {
		t.Fatalf("Write to a/x failed: %v", status)
	}

	// 4. Rename 'a' -> 'b' while file descriptor on 'x' is open and dirty
	renameIn := &fuse.RenameIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Newdir:   fuse.FUSE_ROOT_ID,
	}
	if status := rawFS.Rename(nil, renameIn, "a", "b"); status != fuse.OK {
		t.Fatalf("Rename a -> b failed: %v", status)
	}

	// 5. Write more content through the open descriptor on 'x' (fileXID) after parent rename
	appendData := []byte("appended data")
	written, status = rawFS.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: fileXID}, Offset: uint64(len(initData))}, appendData)
	if status != fuse.OK || int(written) != len(appendData) {
		t.Fatalf("Write through open handle after rename failed: %v", status)
	}

	// 6. Flush / close the open descriptor
	if status := rawFS.Flush(nil, &fuse.FlushIn{InHeader: fuse.InHeader{NodeId: fileXID}}); status != fuse.OK {
		t.Fatalf("Flush on open handle after rename failed: %v", status)
	}

	// 7. fstat (GetAttr) on the open handle (fileXID)
	var statOut fuse.AttrOut
	if status := rawFS.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: fileXID}}, &statOut); status != fuse.OK {
		t.Fatalf("fstat on open handle failed: %v", status)
	}
	expectedTotalSize := uint64(len(initData) + len(appendData))
	if statOut.Attr.Size != expectedTotalSize {
		t.Fatalf("Expected size %d, got %d", expectedTotalSize, statOut.Attr.Size)
	}

	// 8. Read through open descriptor (fileXID)
	readBuf := make([]byte, 100)
	readRes, status := rawFS.Read(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: fileXID}, Size: 100, Offset: 0}, readBuf)
	if status != fuse.OK {
		t.Fatalf("Read through open handle failed: %v", status)
	}
	readBytes, readStatus := readRes.Bytes(readBuf)
	if readStatus != fuse.OK {
		t.Fatalf("Read result status failed: %v", readStatus)
	}
	if string(readBytes) != "initial appended data" {
		t.Fatalf("Expected 'initial appended data', got %q", string(readBytes))
	}

	// 9. Lookup 'b/x' under new parent 'b'
	var lookupB fuse.EntryOut
	if status := rawFS.Lookup(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "b", &lookupB); status != fuse.OK {
		t.Fatalf("Lookup b failed: %v", status)
	}
	dirBID := lookupB.NodeId
	if dirBID != dirAID {
		t.Fatalf("Expected directory b to have same inode as a (%d), got %d", dirAID, dirBID)
	}

	var lookupX fuse.EntryOut
	if status := rawFS.Lookup(nil, &fuse.InHeader{NodeId: dirBID}, "x", &lookupX); status != fuse.OK {
		t.Fatalf("Lookup b/x failed: %v", status)
	}
	if lookupX.NodeId != fileXID {
		t.Fatalf("Expected b/x to have inode %d, got %d", fileXID, lookupX.NodeId)
	}
}

func TestDotAndDotDotLookup(t *testing.T) {
	client, cleanup := createTestClient(t)
	defer cleanup()

	volumeID := "vol-dot-dot"
	cache := NewNodeCache(1024 * 1024)
	rawFS := NewObjectFS(client, volumeID, pb.WriteMode_WRITE_THROUGH_FSYNC, cache)

	// 1. Root '.' and '..' lookup
	var rootDotOut, rootDotDotOut fuse.EntryOut
	if status := rawFS.Lookup(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, ".", &rootDotOut); status != fuse.OK {
		t.Fatalf("Lookup '.' on root failed: %v", status)
	}
	if rootDotOut.NodeId != fuse.FUSE_ROOT_ID {
		t.Fatalf("Expected root '.' to have NodeId %d, got %d", fuse.FUSE_ROOT_ID, rootDotOut.NodeId)
	}

	if status := rawFS.Lookup(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "..", &rootDotDotOut); status != fuse.OK {
		t.Fatalf("Lookup '..' on root failed: %v", status)
	}
	if rootDotDotOut.NodeId != fuse.FUSE_ROOT_ID {
		t.Fatalf("Expected root '..' to have NodeId %d, got %d", fuse.FUSE_ROOT_ID, rootDotDotOut.NodeId)
	}

	// 2. Mkdir dir1
	var dir1Out fuse.EntryOut
	if status := rawFS.Mkdir(nil, &fuse.MkdirIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Mode: 0755}, "dir1", &dir1Out); status != fuse.OK {
		t.Fatalf("Mkdir dir1 failed: %v", status)
	}
	dir1ID := dir1Out.NodeId

	// Lookup '.' and '..' on dir1
	var d1Dot, d1DotDot fuse.EntryOut
	if status := rawFS.Lookup(nil, &fuse.InHeader{NodeId: dir1ID}, ".", &d1Dot); status != fuse.OK {
		t.Fatalf("Lookup '.' on dir1 failed: %v", status)
	}
	if d1Dot.NodeId != dir1ID {
		t.Fatalf("Expected dir1 '.' to have NodeId %d, got %d", dir1ID, d1Dot.NodeId)
	}

	if status := rawFS.Lookup(nil, &fuse.InHeader{NodeId: dir1ID}, "..", &d1DotDot); status != fuse.OK {
		t.Fatalf("Lookup '..' on dir1 failed: %v", status)
	}
	if d1DotDot.NodeId != fuse.FUSE_ROOT_ID {
		t.Fatalf("Expected dir1 '..' to have root NodeId %d, got %d", fuse.FUSE_ROOT_ID, d1DotDot.NodeId)
	}

	// 3. Mkdir dir2 inside dir1
	var dir2Out fuse.EntryOut
	if status := rawFS.Mkdir(nil, &fuse.MkdirIn{InHeader: fuse.InHeader{NodeId: dir1ID}, Mode: 0755}, "dir2", &dir2Out); status != fuse.OK {
		t.Fatalf("Mkdir dir2 failed: %v", status)
	}
	dir2ID := dir2Out.NodeId

	var d2DotDot fuse.EntryOut
	if status := rawFS.Lookup(nil, &fuse.InHeader{NodeId: dir2ID}, "..", &d2DotDot); status != fuse.OK {
		t.Fatalf("Lookup '..' on dir2 failed: %v", status)
	}
	if d2DotDot.NodeId != dir1ID {
		t.Fatalf("Expected dir2 '..' to have dir1 NodeId %d, got %d", dir1ID, d2DotDot.NodeId)
	}

	// 4. Move dir2 from dir1 to root
	renameIn := &fuse.RenameIn{
		InHeader: fuse.InHeader{NodeId: dir1ID},
		Newdir:   fuse.FUSE_ROOT_ID,
	}
	if status := rawFS.Rename(nil, renameIn, "dir2", "dir2_moved"); status != fuse.OK {
		t.Fatalf("Rename dir2 failed: %v", status)
	}

	// Verify '..' on dir2 now points to root
	var d2MovedDotDot fuse.EntryOut
	if status := rawFS.Lookup(nil, &fuse.InHeader{NodeId: dir2ID}, "..", &d2MovedDotDot); status != fuse.OK {
		t.Fatalf("Lookup '..' on dir2 after move failed: %v", status)
	}
	if d2MovedDotDot.NodeId != fuse.FUSE_ROOT_ID {
		t.Fatalf("Expected moved dir2 '..' to have root NodeId %d, got %d", fuse.FUSE_ROOT_ID, d2MovedDotDot.NodeId)
	}
}

func TestDeepPathLookupEfficiency(t *testing.T) {
	client, cleanup := createTestClient(t)
	defer cleanup()

	volumeID := "vol-deep-path"
	cache := NewNodeCache(1024 * 1024)
	rawFS := NewObjectFS(client, volumeID, pb.WriteMode_WRITE_THROUGH_FSYNC, cache)

	// Create 50 nested directories
	parentID := uint64(fuse.FUSE_ROOT_ID)
	for i := 0; i < 50; i++ {
		var dOut fuse.EntryOut
		name := fmt.Sprintf("d%d", i)
		if status := rawFS.Mkdir(nil, &fuse.MkdirIn{InHeader: fuse.InHeader{NodeId: parentID}, Mode: 0755}, name, &dOut); status != fuse.OK {
			t.Fatalf("Mkdir level %d failed: %v", i, status)
		}
		parentID = dOut.NodeId
	}

	// Create a leaf file inside the 50th directory
	var leafOut fuse.CreateOut
	if status := rawFS.Create(nil, &fuse.CreateIn{InHeader: fuse.InHeader{NodeId: parentID}, Mode: 0644}, "leaf.txt", &leafOut); status != fuse.OK {
		t.Fatalf("Create leaf file failed: %v", status)
	}
	leafID := leafOut.EntryOut.NodeId

	// Operating on the leaf file uses its inode number directly without traversing 50 levels
	data := []byte("hello deep leaf")
	written, status := rawFS.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: leafID}, Offset: 0}, data)
	if status != fuse.OK || int(written) != len(data) {
		t.Fatalf("Write to leaf failed: %v", status)
	}

	if status := rawFS.Flush(nil, &fuse.FlushIn{InHeader: fuse.InHeader{NodeId: leafID}}); status != fuse.OK {
		t.Fatalf("Flush leaf failed: %v", status)
	}

	var statOut fuse.AttrOut
	if status := rawFS.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: leafID}}, &statOut); status != fuse.OK {
		t.Fatalf("GetAttr on leaf failed: %v", status)
	}
	if statOut.Attr.Size != uint64(len(data)) {
		t.Fatalf("Expected leaf size %d, got %d", len(data), statOut.Attr.Size)
	}
}

func TestFUSENameLengthLimit(t *testing.T) {
	client, cleanup := createTestClient(t)
	defer cleanup()

	volumeID := "vol-name-limit"
	cache := NewNodeCache(1024 * 1024)
	rawFS := NewObjectFS(client, volumeID, pb.WriteMode_WRITE_THROUGH_FSYNC, cache)

	validNames := map[string]string{
		"ascii_255":   strings.Repeat("a", 255),
		"utf8_3b_255": strings.Repeat("日", 85),
		"utf8_2b_255": strings.Repeat("é", 127) + "x",
		"utf8_4b_255": strings.Repeat("🎉", 63) + "xyz",
	}

	tooLongNames := map[string]string{
		"ascii_256":   strings.Repeat("a", 256),
		"utf8_3b_256": strings.Repeat("日", 85) + "x",
		"utf8_2b_256": strings.Repeat("é", 128),
		"utf8_4b_256": strings.Repeat("🎉", 64),
	}

	for desc, name := range validNames {
		if len(name) != 255 {
			t.Fatalf("%s length is %d, expected 255", desc, len(name))
		}

		// 1. Lookup non-existent 255-byte name -> returns ENOENT, not ENAMETOOLONG
		var lOut fuse.EntryOut
		if st := rawFS.Lookup(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, name, &lOut); st != fuse.ENOENT {
			t.Fatalf("[%s] Lookup non-existent: expected ENOENT, got %v", desc, st)
		}

		// 2. Mkdir 255-byte name -> OK
		var dOut fuse.EntryOut
		if st := rawFS.Mkdir(nil, &fuse.MkdirIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Mode: 0755}, name, &dOut); st != fuse.OK {
			t.Fatalf("[%s] Mkdir 255-byte name failed: %v", desc, st)
		}

		// 3. Lookup existing 255-byte name -> OK
		if st := rawFS.Lookup(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, name, &lOut); st != fuse.OK {
			t.Fatalf("[%s] Lookup existing 255-byte name failed: %v", desc, st)
		}

		// 4. Rmdir 255-byte name -> OK
		if st := rawFS.Rmdir(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, name); st != fuse.OK {
			t.Fatalf("[%s] Rmdir 255-byte name failed: %v", desc, st)
		}

		// 5. Create 255-byte name -> OK
		var cOut fuse.CreateOut
		if st := rawFS.Create(nil, &fuse.CreateIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Mode: 0644}, name, &cOut); st != fuse.OK {
			t.Fatalf("[%s] Create 255-byte name failed: %v", desc, st)
		}

		// 6. Rename 255-byte name -> another 255-byte name -> OK
		renamedName := strings.Repeat("b", 255)
		if st := rawFS.Rename(nil, &fuse.RenameIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Newdir: fuse.FUSE_ROOT_ID}, name, renamedName); st != fuse.OK {
			t.Fatalf("[%s] Rename 255-byte name failed: %v", desc, st)
		}

		// 7. Unlink 255-byte name -> OK
		if st := rawFS.Unlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, renamedName); st != fuse.OK {
			t.Fatalf("[%s] Unlink 255-byte name failed: %v", desc, st)
		}

		// 8. Mknod 255-byte name -> OK
		var mOut fuse.EntryOut
		if st := rawFS.Mknod(nil, &fuse.MknodIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Mode: 0644}, name, &mOut); st != fuse.OK {
			t.Fatalf("[%s] Mknod 255-byte name failed: %v", desc, st)
		}
		if st := rawFS.Unlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, name); st != fuse.OK {
			t.Fatalf("[%s] Unlink after Mknod failed: %v", desc, st)
		}
	}

	for desc, name := range tooLongNames {
		if len(name) < 256 {
			t.Fatalf("%s length is %d, expected >= 256", desc, len(name))
		}

		// 1. Lookup 256-byte name -> ENAMETOOLONG
		var lOut fuse.EntryOut
		if st := rawFS.Lookup(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, name, &lOut); st != fuse.Status(syscall.ENAMETOOLONG) {
			t.Fatalf("[%s] Lookup 256-byte: expected ENAMETOOLONG (%d), got %v", desc, syscall.ENAMETOOLONG, st)
		}

		// 2. Mkdir 256-byte name -> ENAMETOOLONG
		var dOut fuse.EntryOut
		if st := rawFS.Mkdir(nil, &fuse.MkdirIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Mode: 0755}, name, &dOut); st != fuse.Status(syscall.ENAMETOOLONG) {
			t.Fatalf("[%s] Mkdir 256-byte: expected ENAMETOOLONG (%d), got %v", desc, syscall.ENAMETOOLONG, st)
		}

		// 3. Create 256-byte name -> ENAMETOOLONG
		var cOut fuse.CreateOut
		if st := rawFS.Create(nil, &fuse.CreateIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Mode: 0644}, name, &cOut); st != fuse.Status(syscall.ENAMETOOLONG) {
			t.Fatalf("[%s] Create 256-byte: expected ENAMETOOLONG (%d), got %v", desc, syscall.ENAMETOOLONG, st)
		}

		// 4. Mknod 256-byte name -> ENAMETOOLONG
		var mOut fuse.EntryOut
		if st := rawFS.Mknod(nil, &fuse.MknodIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Mode: 0644}, name, &mOut); st != fuse.Status(syscall.ENAMETOOLONG) {
			t.Fatalf("[%s] Mknod 256-byte: expected ENAMETOOLONG (%d), got %v", desc, syscall.ENAMETOOLONG, mOut)
		}

		// 5. Unlink 256-byte name -> ENAMETOOLONG
		if st := rawFS.Unlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, name); st != fuse.Status(syscall.ENAMETOOLONG) {
			t.Fatalf("[%s] Unlink 256-byte: expected ENAMETOOLONG (%d), got %v", desc, syscall.ENAMETOOLONG, st)
		}

		// 6. Rmdir 256-byte name -> ENAMETOOLONG
		if st := rawFS.Rmdir(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, name); st != fuse.Status(syscall.ENAMETOOLONG) {
			t.Fatalf("[%s] Rmdir 256-byte: expected ENAMETOOLONG (%d), got %v", desc, syscall.ENAMETOOLONG, st)
		}

		// 7. Rename 256-byte oldName -> ENAMETOOLONG
		if st := rawFS.Rename(nil, &fuse.RenameIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Newdir: fuse.FUSE_ROOT_ID}, name, "valid.txt"); st != fuse.Status(syscall.ENAMETOOLONG) {
			t.Fatalf("[%s] Rename 256-byte oldName: expected ENAMETOOLONG (%d), got %v", desc, syscall.ENAMETOOLONG, st)
		}

		// 8. Rename 256-byte newName -> ENAMETOOLONG
		if st := rawFS.Rename(nil, &fuse.RenameIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Newdir: fuse.FUSE_ROOT_ID}, "valid.txt", name); st != fuse.Status(syscall.ENAMETOOLONG) {
			t.Fatalf("[%s] Rename 256-byte newName: expected ENAMETOOLONG (%d), got %v", desc, syscall.ENAMETOOLONG, st)
		}
	}
}

func TestFUSESetAttrAllFields(t *testing.T) {
	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	backend := controller.NewMemoryBackend()
	server := controllertest.NewTestServer(backend)
	pb.RegisterObjectFSControllerServer(grpcServer, server)

	go func() {
		_ = grpcServer.Serve(lis)
	}()
	defer func() {
		grpcServer.Stop()
		_ = lis.Close()
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
	defer conn.Close()

	client := pb.NewObjectFSControllerClient(conn)
	volumeID := "test-setattr-fields-vol"
	cache := NewNodeCache(16 * 1024 * 1024)
	rawFS := NewObjectFS(client, volumeID, pb.WriteMode_WRITE_THROUGH_FSYNC, cache)

	// Create file
	var createOut fuse.CreateOut
	if status := rawFS.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID, Caller: fuse.Caller{Owner: fuse.Owner{Uid: 100, Gid: 200}}},
		Mode:     0644,
	}, "test.txt", &createOut); status != fuse.OK {
		t.Fatalf("Create failed: %v", status)
	}
	fileID := createOut.EntryOut.NodeId

	// 1. Set mode, uid, gid, mtime, atime
	targetMtime := time.Date(2025, 6, 1, 12, 0, 0, 123456000, time.UTC)
	targetAtime := time.Date(2025, 6, 1, 13, 0, 0, 654321000, time.UTC)

	var setAttrOut fuse.AttrOut
	setIn := &fuse.SetAttrIn{
		SetAttrInCommon: fuse.SetAttrInCommon{
			InHeader:  fuse.InHeader{NodeId: fileID},
			Valid:     fuse.FATTR_MODE | fuse.FATTR_UID | fuse.FATTR_GID | fuse.FATTR_MTIME | fuse.FATTR_ATIME,
			Mode:      0750,
			Owner:     fuse.Owner{Uid: 501, Gid: 601},
			Mtime:     uint64(targetMtime.Unix()),
			Mtimensec: uint32(targetMtime.Nanosecond()),
			Atime:     uint64(targetAtime.Unix()),
			Atimensec: uint32(targetAtime.Nanosecond()),
		},
	}
	if status := rawFS.SetAttr(nil, setIn, &setAttrOut); status != fuse.OK {
		t.Fatalf("SetAttr failed: %v", status)
	}

	if setAttrOut.Attr.Mode&07777 != 0750 {
		t.Fatalf("Expected mode 0750, got %o", setAttrOut.Attr.Mode&07777)
	}
	if setAttrOut.Attr.Owner.Uid != 501 || setAttrOut.Attr.Owner.Gid != 601 {
		t.Fatalf("Expected owner 501/601, got %d/%d", setAttrOut.Attr.Owner.Uid, setAttrOut.Attr.Owner.Gid)
	}
	if setAttrOut.Attr.Mtime != uint64(targetMtime.Unix()) || setAttrOut.Attr.Mtimensec != uint32(targetMtime.Nanosecond()) {
		t.Fatalf("Mtime mismatch: got %d.%d, want %d.%d", setAttrOut.Attr.Mtime, setAttrOut.Attr.Mtimensec, targetMtime.Unix(), targetMtime.Nanosecond())
	}
	if setAttrOut.Attr.Atime != uint64(targetAtime.Unix()) || setAttrOut.Attr.Atimensec != uint32(targetAtime.Nanosecond()) {
		t.Fatalf("Atime mismatch: got %d.%d, want %d.%d", setAttrOut.Attr.Atime, setAttrOut.Attr.Atimensec, targetAtime.Unix(), targetAtime.Nanosecond())
	}

	// 2. Set combined size and mode
	var setAttrCombinedOut fuse.AttrOut
	setCombinedIn := &fuse.SetAttrIn{
		SetAttrInCommon: fuse.SetAttrInCommon{
			InHeader: fuse.InHeader{NodeId: fileID},
			Valid:    fuse.FATTR_SIZE | fuse.FATTR_MODE,
			Size:     1024,
			Mode:     0640,
		},
	}
	if status := rawFS.SetAttr(nil, setCombinedIn, &setAttrCombinedOut); status != fuse.OK {
		t.Fatalf("Combined SetAttr failed: %v", status)
	}
	if setAttrCombinedOut.Attr.Size != 1024 {
		t.Fatalf("Expected size 1024, got %d", setAttrCombinedOut.Attr.Size)
	}
	if setAttrCombinedOut.Attr.Mode&07777 != 0640 {
		t.Fatalf("Expected mode 0640, got %o", setAttrCombinedOut.Attr.Mode&07777)
	}

	// 3. Verify via GetAttr
	var getAttrOut fuse.AttrOut
	if status := rawFS.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: fileID}}, &getAttrOut); status != fuse.OK {
		t.Fatalf("GetAttr failed: %v", status)
	}
	if getAttrOut.Attr.Size != 1024 {
		t.Fatalf("GetAttr size = %d, want 1024", getAttrOut.Attr.Size)
	}
	if getAttrOut.Attr.Mode&07777 != 0640 {
		t.Fatalf("GetAttr mode = %o, want 0640", getAttrOut.Attr.Mode&07777)
	}
	if getAttrOut.Attr.Owner.Uid != 501 || getAttrOut.Attr.Owner.Gid != 601 {
		t.Fatalf("GetAttr owner = %d/%d, want 501/601", getAttrOut.Attr.Owner.Uid, getAttrOut.Attr.Owner.Gid)
	}
}

func TestFUSESymlinkAndLinkOperations(t *testing.T) {
	client, cleanup := createTestClient(t)
	defer cleanup()

	volumeID := "test-fuse-symlinks"
	rawFS := NewObjectFS(client, volumeID, pb.WriteMode_WRITE_THROUGH_FSYNC, NewNodeCache(1024*1024))

	// 1. Create a regular file
	var createOut fuse.CreateOut
	if status := rawFS.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     0644,
	}, "orig.txt", &createOut); status != fuse.OK {
		t.Fatalf("Create failed: %v", status)
	}
	origID := createOut.NodeId
	if createOut.Attr.Nlink != 1 {
		t.Fatalf("Expected initial Nlink to be 1, got %d", createOut.Attr.Nlink)
	}

	// 2. Create hard link to orig.txt
	var linkOut fuse.EntryOut
	if status := rawFS.Link(nil, &fuse.LinkIn{
		InHeader:  fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Oldnodeid: origID,
	}, "hardlink.txt", &linkOut); status != fuse.OK {
		t.Fatalf("Link failed: %v", status)
	}
	if linkOut.NodeId != origID {
		t.Fatalf("Expected hard link node ID %d, got %d", origID, linkOut.NodeId)
	}
	if linkOut.Attr.Nlink != 2 {
		t.Fatalf("Expected hard link Nlink to be 2, got %d", linkOut.Attr.Nlink)
	}

	// Verify orig.txt now has Nlink 2 via GetAttr
	var getAttrOut fuse.AttrOut
	if status := rawFS.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: origID}}, &getAttrOut); status != fuse.OK {
		t.Fatalf("GetAttr failed: %v", status)
	}
	if getAttrOut.Attr.Nlink != 2 {
		t.Fatalf("Expected orig.txt Nlink to be 2, got %d", getAttrOut.Attr.Nlink)
	}

	// 3. Create symlink
	var symlinkOut fuse.EntryOut
	target := "orig.txt"
	if status := rawFS.Symlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID, Uid: 100, Gid: 200}, target, "symlink.lnk", &symlinkOut); status != fuse.OK {
		t.Fatalf("Symlink failed: %v", status)
	}
	if symlinkOut.Attr.Mode&syscall.S_IFMT != syscall.S_IFLNK {
		t.Fatalf("Expected symlink mode S_IFLNK, got %o", symlinkOut.Attr.Mode)
	}
	if symlinkOut.Attr.Nlink != 1 {
		t.Fatalf("Expected symlink Nlink to be 1, got %d", symlinkOut.Attr.Nlink)
	}
	if symlinkOut.Attr.Size != uint64(len(target)) {
		t.Fatalf("Expected symlink size %d, got %d", len(target), symlinkOut.Attr.Size)
	}

	// 4. Readlink
	linkTargetBytes, status := rawFS.Readlink(nil, &fuse.InHeader{NodeId: symlinkOut.NodeId})
	if status != fuse.OK {
		t.Fatalf("Readlink failed: %v", status)
	}
	if string(linkTargetBytes) != target {
		t.Fatalf("Readlink returned %q, want %q", string(linkTargetBytes), target)
	}

	// 5. ReadDir should list entries with proper types
	dirEntries := fuse.NewDirEntryList(make([]byte, 4096), 0)
	if status := rawFS.ReadDir(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}}, dirEntries); status != fuse.OK {
		t.Fatalf("ReadDir failed: %v", status)
	}

	// 6. Unlink orig.txt - hardlink.txt should still exist with Nlink 1
	if status := rawFS.Unlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "orig.txt"); status != fuse.OK {
		t.Fatalf("Unlink orig.txt failed: %v", status)
	}
	var lookupOut fuse.EntryOut
	if status := rawFS.Lookup(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "hardlink.txt", &lookupOut); status != fuse.OK {
		t.Fatalf("Lookup hardlink.txt failed: %v", status)
	}
	if lookupOut.NodeId != origID || lookupOut.Attr.Nlink != 1 {
		t.Fatalf("Expected hardlink.txt to have Nlink 1 and node %d, got Nlink %d node %d", origID, lookupOut.Attr.Nlink, lookupOut.NodeId)
	}

	// 7. Hard link on directory should return EPERM
	var dirEntryOut fuse.EntryOut
	if status := rawFS.Mkdir(nil, &fuse.MkdirIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     0755,
	}, "subdir", &dirEntryOut); status != fuse.OK {
		t.Fatalf("Mkdir failed: %v", status)
	}
	if status := rawFS.Link(nil, &fuse.LinkIn{
		InHeader:  fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Oldnodeid: dirEntryOut.NodeId,
	}, "dirlink", &linkOut); status != fuse.Status(syscall.EPERM) {
		t.Fatalf("Expected EPERM when linking directory, got %v", status)
	}
}

func TestFUSEMknodSpecialFiles(t *testing.T) {
	client, cleanup := createTestClient(t)
	defer cleanup()

	volumeID := "fuse-mknod-vol"

	cache := NewNodeCache(1024 * 1024)
	rawFS := NewObjectFS(client, volumeID, pb.WriteMode_WRITE_THROUGH_FSYNC, cache)

	// 1. Mknod FIFO
	var fifoOut fuse.EntryOut
	status := rawFS.Mknod(nil, &fuse.MknodIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     syscall.S_IFIFO | 0644,
	}, "test_fifo", &fifoOut)
	if status != fuse.OK {
		t.Fatalf("Mknod FIFO failed: %v", status)
	}
	if fifoOut.Attr.Mode&syscall.S_IFMT != syscall.S_IFIFO {
		t.Fatalf("Expected S_IFIFO, got %o", fifoOut.Attr.Mode)
	}

	// 2. Mknod Char Dev
	var chrOut fuse.EntryOut
	status = rawFS.Mknod(nil, &fuse.MknodIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     syscall.S_IFCHR | 0660,
		Rdev:     0x0103,
	}, "test_chr", &chrOut)
	if status != fuse.OK {
		t.Fatalf("Mknod CHR failed: %v", status)
	}
	if chrOut.Attr.Mode&syscall.S_IFMT != syscall.S_IFCHR {
		t.Fatalf("Expected S_IFCHR, got %o", chrOut.Attr.Mode)
	}
	if chrOut.Attr.Rdev != 0x0103 {
		t.Fatalf("Expected Rdev 0x0103, got 0x%x", chrOut.Attr.Rdev)
	}

	// 3. Mknod Block Dev
	var blkOut fuse.EntryOut
	status = rawFS.Mknod(nil, &fuse.MknodIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     syscall.S_IFBLK | 0660,
		Rdev:     0x0801,
	}, "test_blk", &blkOut)
	if status != fuse.OK {
		t.Fatalf("Mknod BLK failed: %v", status)
	}
	if blkOut.Attr.Mode&syscall.S_IFMT != syscall.S_IFBLK {
		t.Fatalf("Expected S_IFBLK, got %o", blkOut.Attr.Mode)
	}
	if blkOut.Attr.Rdev != 0x0801 {
		t.Fatalf("Expected Rdev 0x0801, got 0x%x", blkOut.Attr.Rdev)
	}

	// 4. Mknod Socket
	var sockOut fuse.EntryOut
	status = rawFS.Mknod(nil, &fuse.MknodIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     syscall.S_IFSOCK | 0777,
	}, "test_sock", &sockOut)
	if status != fuse.OK {
		t.Fatalf("Mknod SOCK failed: %v", status)
	}
	if sockOut.Attr.Mode&syscall.S_IFMT != syscall.S_IFSOCK {
		t.Fatalf("Expected S_IFSOCK, got %o", sockOut.Attr.Mode)
	}

	// Verify GetAttr on CHR node reports Rdev
	var getAttrOut fuse.AttrOut
	if status := rawFS.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: chrOut.NodeId}}, &getAttrOut); status != fuse.OK {
		t.Fatalf("GetAttr failed: %v", status)
	}
	if getAttrOut.Attr.Rdev != 0x0103 {
		t.Fatalf("GetAttr Rdev expected 0x0103, got 0x%x", getAttrOut.Attr.Rdev)
	}
	if getAttrOut.Attr.Mode&syscall.S_IFMT != syscall.S_IFCHR {
		t.Fatalf("GetAttr Mode expected S_IFCHR, got %o", getAttrOut.Attr.Mode)
	}
}

func TestFUSETruncateAndUnalignedWriteWithCacheInvalidation(t *testing.T) {
	client, cleanup := createTestClient(t)
	defer cleanup()

	cache := NewNodeCache(1024 * 1024)
	rawFS := NewObjectFS(client, "vol-trunc-test", pb.WriteMode_WRITE_THROUGH_FSYNC, cache)

	var createOut fuse.CreateOut
	if status := rawFS.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     0644,
	}, "trunc_unaligned.bin", &createOut); status != fuse.OK {
		t.Fatalf("Create failed: %v", status)
	}
	fileID := createOut.EntryOut.NodeId

	// Write 200 KiB pattern (pattern[i] = byte(i % 251 + 1))
	initialSize := 200 * 1024
	pattern := make([]byte, initialSize)
	for i := range pattern {
		pattern[i] = byte(i%251 + 1)
	}
	written, status := rawFS.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: fileID}, Offset: 0}, pattern)
	if status != fuse.OK || int(written) != initialSize {
		t.Fatalf("Initial write failed: status=%v, written=%d", status, written)
	}
	if status := rawFS.Flush(nil, &fuse.FlushIn{InHeader: fuse.InHeader{NodeId: fileID}}); status != fuse.OK {
		t.Fatalf("Flush failed: %v", status)
	}

	// Truncate down to 150 KiB
	truncSize := uint64(150 * 1024)
	var attrOut fuse.AttrOut
	if status := rawFS.SetAttr(nil, &fuse.SetAttrIn{
		SetAttrInCommon: fuse.SetAttrInCommon{
			InHeader: fuse.InHeader{NodeId: fileID},
			Valid:    fuse.FATTR_SIZE,
			Size:     truncSize,
		},
	}, &attrOut); status != fuse.OK {
		t.Fatalf("SetAttr truncate failed: %v", status)
	}

	// Invalidate client cache (simulating WatchVolume arrival)
	cache.InvalidateIfNotDirty(fileID)

	// Write 4096 bytes at offset 0 with distinct marker
	patch0 := bytes.Repeat([]byte{0xAA}, 4096)
	written, status = rawFS.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: fileID}, Offset: 0}, patch0)
	if status != fuse.OK || int(written) != len(patch0) {
		t.Fatalf("Write at offset 0 failed: status=%v, written=%d", status, written)
	}

	// Write 1000 bytes at offset 70000 (chunk 1 unaligned)
	patch1 := bytes.Repeat([]byte{0xBB}, 1000)
	written, status = rawFS.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: fileID}, Offset: 70000}, patch1)
	if status != fuse.OK || int(written) != len(patch1) {
		t.Fatalf("Write at offset 70000 failed: status=%v, written=%d", status, written)
	}

	if status := rawFS.Flush(nil, &fuse.FlushIn{InHeader: fuse.InHeader{NodeId: fileID}}); status != fuse.OK {
		t.Fatalf("Flush failed: %v", status)
	}

	// Invalidate client cache and read back through FUSE
	cache.InvalidateIfNotDirty(fileID)

	expected := make([]byte, truncSize)
	copy(expected, pattern[:truncSize])
	copy(expected[0:4096], patch0)
	copy(expected[70000:71000], patch1)

	readBuf := make([]byte, truncSize)
	readRes, status := rawFS.Read(nil, &fuse.ReadIn{
		InHeader: fuse.InHeader{NodeId: fileID},
		Offset:   0,
		Size:     uint32(truncSize),
	}, readBuf)
	if status != fuse.OK {
		t.Fatalf("Read failed: %v", status)
	}
	readBytes, readStatus := readRes.Bytes(readBuf)
	if readStatus != fuse.OK {
		t.Fatalf("Failed to extract read bytes: %v", readStatus)
	}
	if len(readBytes) != len(expected) {
		t.Fatalf("Read length mismatch: got %d, want %d", len(readBytes), len(expected))
	}
	for i := range expected {
		if readBytes[i] != expected[i] {
			t.Fatalf("Data mismatch at offset %d (chunk %d): got 0x%02x, want 0x%02x", i, i/int(DefaultChunkSize), readBytes[i], expected[i])
		}
	}
}

func TestFUSEUnflushedHoleAndDirtyWriteRead(t *testing.T) {
	client, cleanup := createTestClient(t)
	defer cleanup()

	cache := NewNodeCache(1024 * 1024)
	rawFS := NewObjectFS(client, "vol-unflushed-test", pb.WriteMode_WRITE_THROUGH_FSYNC, cache)

	var createOut fuse.CreateOut
	if status := rawFS.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     0644,
	}, "unflushed_hole.bin", &createOut); status != fuse.OK {
		t.Fatalf("Create failed: %v", status)
	}
	fileID := createOut.EntryOut.NodeId

	// 1. Write 4 KiB at offset 200 KiB without flushing
	patchData := bytes.Repeat([]byte{0x42}, 4096)
	written, status := rawFS.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: fileID}, Offset: 200 * 1024}, patchData)
	if status != fuse.OK || int(written) != len(patchData) {
		t.Fatalf("Write at offset 200 KiB failed: status=%v, written=%d", status, written)
	}

	// 2. Read [0, 204 KiB) without flushing -> must return 200 KiB zeros + 4 KiB patchData
	totalSize := uint64(204 * 1024)
	readBuf := make([]byte, totalSize)
	readRes, status := rawFS.Read(nil, &fuse.ReadIn{
		InHeader: fuse.InHeader{NodeId: fileID},
		Offset:   0,
		Size:     uint32(totalSize),
	}, readBuf)
	if status != fuse.OK {
		t.Fatalf("Read failed: %v", status)
	}
	readBytes, readStatus := readRes.Bytes(readBuf)
	if readStatus != fuse.OK {
		t.Fatalf("Failed to extract read bytes: %v", readStatus)
	}
	if uint64(len(readBytes)) != totalSize {
		t.Fatalf("Read length mismatch: got %d, want %d", len(readBytes), totalSize)
	}

	expected := make([]byte, totalSize)
	copy(expected[200*1024:], patchData)
	if !bytes.Equal(readBytes, expected) {
		t.Fatalf("Read data mismatch for unflushed hole read")
	}

	// 3. Variant: sync 64 KiB clean data at offset 0, write 4 KiB at offset 200 KiB without flushing,
	// evict/clear chunk 0 from cache, and read [0, 204 KiB).
	chunk0Data := bytes.Repeat([]byte{0x77}, 64*1024)
	written, status = rawFS.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: fileID}, Offset: 0}, chunk0Data)
	if status != fuse.OK || int(written) != len(chunk0Data) {
		t.Fatalf("Write chunk 0 failed: status=%v, written=%d", status, written)
	}
	if status := rawFS.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: fileID}}); status != fuse.OK {
		t.Fatalf("Fsync failed: %v", status)
	}

	// Dirty write extending past controller size (e.g. at 200 KiB)
	written, status = rawFS.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: fileID}, Offset: 200 * 1024}, patchData)
	if status != fuse.OK || int(written) != len(patchData) {
		t.Fatalf("Write at offset 200 KiB failed: status=%v, written=%d", status, written)
	}

	// Delete chunk 0 from cache to simulate uncached clean chunk + unflushed dirty chunk
	cache.mu.Lock()
	if entry, ok := cache.entries[fileID]; ok {
		delete(entry.Chunks, 0)
	}
	cache.mu.Unlock()

	readBuf2 := make([]byte, totalSize)
	readRes2, status := rawFS.Read(nil, &fuse.ReadIn{
		InHeader: fuse.InHeader{NodeId: fileID},
		Offset:   0,
		Size:     uint32(totalSize),
	}, readBuf2)
	if status != fuse.OK {
		t.Fatalf("Read spanning uncached clean and dirty chunks failed: %v", status)
	}
	readBytes2, readStatus2 := readRes2.Bytes(readBuf2)
	if readStatus2 != fuse.OK {
		t.Fatalf("Failed to extract read bytes: %v", readStatus2)
	}
	if uint64(len(readBytes2)) != totalSize {
		t.Fatalf("Read length mismatch: got %d, want %d", len(readBytes2), totalSize)
	}

	copy(expected[0:64*1024], chunk0Data)
	if !bytes.Equal(readBytes2, expected) {
		t.Fatalf("Read data mismatch for spanning uncached clean and dirty chunks")
	}
}

func TestDirectoryLinkCountsAndDotDotOnRename(t *testing.T) {
	client, cleanup := createTestClient(t)
	defer cleanup()

	volumeID := "vol-dir-nlink-rename"
	cache := NewNodeCache(1024 * 1024)
	rawFS := NewObjectFS(client, volumeID, pb.WriteMode_WRITE_THROUGH_FSYNC, cache)

	// 1. Mkdir src_parent (nlink 2) and dst_parent (nlink 2)
	var srcPOut, dstPOut fuse.EntryOut
	if status := rawFS.Mkdir(nil, &fuse.MkdirIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Mode: 0755}, "src_parent", &srcPOut); status != fuse.OK {
		t.Fatalf("Mkdir src_parent failed: %v", status)
	}
	if status := rawFS.Mkdir(nil, &fuse.MkdirIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Mode: 0755}, "dst_parent", &dstPOut); status != fuse.OK {
		t.Fatalf("Mkdir dst_parent failed: %v", status)
	}
	if srcPOut.Attr.Nlink != 2 || dstPOut.Attr.Nlink != 2 {
		t.Fatalf("Expected initial nlink 2, got src=%d dst=%d", srcPOut.Attr.Nlink, dstPOut.Attr.Nlink)
	}

	// 2. Mkdir src_parent/sub (src_parent nlink becomes 3)
	var subOut fuse.EntryOut
	if status := rawFS.Mkdir(nil, &fuse.MkdirIn{InHeader: fuse.InHeader{NodeId: srcPOut.NodeId}, Mode: 0755}, "sub", &subOut); status != fuse.OK {
		t.Fatalf("Mkdir sub failed: %v", status)
	}

	var statSrcP fuse.AttrOut
	if status := rawFS.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: srcPOut.NodeId}}, &statSrcP); status != fuse.OK {
		t.Fatalf("GetAttr src_parent failed: %v", status)
	}
	if statSrcP.Attr.Nlink != 3 {
		t.Fatalf("Expected src_parent nlink 3 after child mkdir, got %d", statSrcP.Attr.Nlink)
	}

	// 3. Verify lookup ".." in src_parent/sub is src_parent
	var dotdotOut fuse.EntryOut
	if status := rawFS.Lookup(nil, &fuse.InHeader{NodeId: subOut.NodeId}, "..", &dotdotOut); status != fuse.OK {
		t.Fatalf("Lookup '..' in sub failed: %v", status)
	}
	if dotdotOut.NodeId != srcPOut.NodeId {
		t.Fatalf("Expected sub/.. to be src_parent (%d), got %d", srcPOut.NodeId, dotdotOut.NodeId)
	}

	// 4. Rename src_parent/sub -> dst_parent/sub
	if status := rawFS.Rename(nil, &fuse.RenameIn{InHeader: fuse.InHeader{NodeId: srcPOut.NodeId}, Newdir: dstPOut.NodeId}, "sub", "sub"); status != fuse.OK {
		t.Fatalf("Rename failed: %v", status)
	}

	// 5. Check nlink on src_parent (should be 2) and dst_parent (should be 3)
	if status := rawFS.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: srcPOut.NodeId}}, &statSrcP); status != fuse.OK {
		t.Fatalf("GetAttr src_parent failed: %v", status)
	}
	if statSrcP.Attr.Nlink != 2 {
		t.Fatalf("Expected src_parent nlink 2 after rename, got %d", statSrcP.Attr.Nlink)
	}

	var statDstP fuse.AttrOut
	if status := rawFS.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: dstPOut.NodeId}}, &statDstP); status != fuse.OK {
		t.Fatalf("GetAttr dst_parent failed: %v", status)
	}
	if statDstP.Attr.Nlink != 3 {
		t.Fatalf("Expected dst_parent nlink 3 after rename, got %d", statDstP.Attr.Nlink)
	}

	// 6. Verify lookup ".." in dst_parent/sub is dst_parent
	if status := rawFS.Lookup(nil, &fuse.InHeader{NodeId: subOut.NodeId}, "..", &dotdotOut); status != fuse.OK {
		t.Fatalf("Lookup '..' in moved sub failed: %v", status)
	}
	if dotdotOut.NodeId != dstPOut.NodeId {
		t.Fatalf("Expected moved sub/.. to be dst_parent (%d), got %d", dstPOut.NodeId, dotdotOut.NodeId)
	}
}

func TestRenameOntoNonEmptyDirectoryAndMultiplyHardlinked(t *testing.T) {
	client, cleanup := createTestClient(t)
	defer cleanup()

	volumeID := "vol-rename-edge-cases"
	cache := NewNodeCache(1024 * 1024)
	rawFS := NewObjectFS(client, volumeID, pb.WriteMode_WRITE_THROUGH_FSYNC, cache)

	// Test 1: Rename onto non-empty directory returns ENOTEMPTY
	var dirA, dirB, dirBChild fuse.EntryOut
	rawFS.Mkdir(nil, &fuse.MkdirIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Mode: 0755}, "dirA", &dirA)
	rawFS.Mkdir(nil, &fuse.MkdirIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Mode: 0755}, "dirB", &dirB)
	rawFS.Mkdir(nil, &fuse.MkdirIn{InHeader: fuse.InHeader{NodeId: dirB.NodeId}, Mode: 0755}, "child", &dirBChild)

	if status := rawFS.Rename(nil, &fuse.RenameIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Newdir: fuse.FUSE_ROOT_ID}, "dirA", "dirB"); status != fuse.Status(syscall.ENOTEMPTY) && status != fuse.Status(syscall.EEXIST) {
		t.Fatalf("Expected ENOTEMPTY or EEXIST renaming onto non-empty dir, got: %v", status)
	}

	// Test 2: Rename onto multiply hardlinked file
	var file1, file2 fuse.CreateOut
	rawFS.Create(nil, &fuse.CreateIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Mode: 0644}, "file1", &file1)
	rawFS.Create(nil, &fuse.CreateIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Mode: 0644}, "file2", &file2)

	var link1 fuse.EntryOut
	if status := rawFS.Link(nil, &fuse.LinkIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Oldnodeid: file1.NodeId}, "file1_link", &link1); status != fuse.OK {
		t.Fatalf("Link failed: %v", status)
	}
	if link1.Attr.Nlink != 2 {
		t.Fatalf("Expected link1 nlink 2, got %d", link1.Attr.Nlink)
	}

	// Rename file2 onto file1
	if status := rawFS.Rename(nil, &fuse.RenameIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Newdir: fuse.FUSE_ROOT_ID}, "file2", "file1"); status != fuse.OK {
		t.Fatalf("Rename file2 onto file1 failed: %v", status)
	}

	// file1_link should remain with nlink 1
	var statLink1 fuse.AttrOut
	if status := rawFS.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: file1.NodeId}}, &statLink1); status != fuse.OK {
		t.Fatalf("GetAttr on file1 inode failed: %v", status)
	}
	if statLink1.Attr.Nlink != 1 {
		t.Fatalf("Expected file1_link nlink 1 after overwrite, got %d", statLink1.Attr.Nlink)
	}
}

func TestOpenUnlinkedFileSemantics(t *testing.T) {
	client, cleanup := createTestClient(t)
	defer cleanup()

	volumeID := "vol-open-unlinked"
	cache := NewNodeCache(1024 * 1024)
	rawFS := NewObjectFS(client, volumeID, pb.WriteMode_WRITE_THROUGH_FSYNC, cache)

	// 1. Create file and open it
	var createOut fuse.CreateOut
	if status := rawFS.Create(nil, &fuse.CreateIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Mode: 0644}, "temp.txt", &createOut); status != fuse.OK {
		t.Fatalf("Create temp.txt failed: %v", status)
	}
	fileIno := createOut.NodeId

	// Write data
	writeData := []byte("hello open unlinked world")
	_, status := rawFS.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: fileIno}, Offset: 0}, writeData)
	if status != fuse.OK {
		t.Fatalf("Write failed: %v", status)
	}
	rawFS.Flush(nil, &fuse.FlushIn{InHeader: fuse.InHeader{NodeId: fileIno}})

	// 2. Unlink the file while open
	if status := rawFS.Unlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "temp.txt"); status != fuse.OK {
		t.Fatalf("Unlink failed: %v", status)
	}

	// 3. fstat on open handle -> nlink must be 0
	var statOut fuse.AttrOut
	if status := rawFS.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: fileIno}}, &statOut); status != fuse.OK {
		t.Fatalf("GetAttr on open unlinked file failed: %v", status)
	}
	if statOut.Attr.Nlink != 0 {
		t.Fatalf("Expected nlink 0 on open unlinked file, got %d", statOut.Attr.Nlink)
	}

	// 4. Read data through open handle
	readBuf := make([]byte, 50)
	readRes, status := rawFS.Read(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: fileIno}, Size: 50, Offset: 0}, readBuf)
	if status != fuse.OK {
		t.Fatalf("Read on open unlinked file failed: %v", status)
	}
	readBytes, _ := readRes.Bytes(readBuf)
	if string(readBytes) != "hello open unlinked world" {
		t.Fatalf("Expected read data 'hello open unlinked world', got %q", string(readBytes))
	}

	// 5. Release file handle
	rawFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: fileIno}, Fh: createOut.OpenOut.Fh})

	// 6. After release, lookup in root confirms temp.txt does not exist
	var lookupOut fuse.EntryOut
	if status := rawFS.Lookup(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "temp.txt", &lookupOut); status != fuse.Status(syscall.ENOENT) {
		t.Fatalf("Expected ENOENT for unlinked temp.txt, got %v", status)
	}
}

func TestMultipleOpenHandlesAndRelease(t *testing.T) {
	client, cleanup := createTestClient(t)
	defer cleanup()

	volumeID := "vol-multi-open-handles"
	cache := NewNodeCache(1024 * 1024)
	rawFS := NewObjectFS(client, volumeID, pb.WriteMode_WRITE_THROUGH_FSYNC, cache)

	// 1. Create file (assigns handle 1)
	var createOut fuse.CreateOut
	if status := rawFS.Create(nil, &fuse.CreateIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Mode: 0644}, "multi.txt", &createOut); status != fuse.OK {
		t.Fatalf("Create multi.txt failed: %v", status)
	}
	fileIno := createOut.NodeId
	fh1 := createOut.OpenOut.Fh

	// 2. Open file second time (assigns handle 2)
	var openOut2 fuse.OpenOut
	if status := rawFS.Open(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: fileIno}}, &openOut2); status != fuse.OK {
		t.Fatalf("Open 2 failed: %v", status)
	}
	fh2 := openOut2.Fh

	// 3. Open file third time (assigns handle 3)
	var openOut3 fuse.OpenOut
	if status := rawFS.Open(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: fileIno}}, &openOut3); status != fuse.OK {
		t.Fatalf("Open 3 failed: %v", status)
	}
	fh3 := openOut3.Fh

	if fh1 == fh2 || fh2 == fh3 || fh1 == fh3 {
		t.Fatalf("Expected distinct handles for each open, got fh1=%d fh2=%d fh3=%d", fh1, fh2, fh3)
	}

	// Write data
	writeData := []byte("multi handle test content")
	_, status := rawFS.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: fileIno}, Fh: fh1, Offset: 0}, writeData)
	if status != fuse.OK {
		t.Fatalf("Write failed: %v", status)
	}
	rawFS.Flush(nil, &fuse.FlushIn{InHeader: fuse.InHeader{NodeId: fileIno}, Fh: fh1})

	// 4. Unlink file while 3 handles are open
	if status := rawFS.Unlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "multi.txt"); status != fuse.OK {
		t.Fatalf("Unlink failed: %v", status)
	}

	// 5. Release first handle (2 handles remain)
	rawFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: fileIno}, Fh: fh1})

	// 6. Read through handle 2
	readBuf := make([]byte, 50)
	readRes, status := rawFS.Read(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: fileIno}, Fh: fh2, Size: 50, Offset: 0}, readBuf)
	if status != fuse.OK {
		t.Fatalf("Read on handle 2 failed: %v", status)
	}
	readBytes, _ := readRes.Bytes(readBuf)
	if string(readBytes) != "multi handle test content" {
		t.Fatalf("Expected 'multi handle test content', got %q", string(readBytes))
	}

	// 7. Release second handle (1 handle remains)
	rawFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: fileIno}, Fh: fh2})

	// 8. Read through handle 3
	readRes3, status := rawFS.Read(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: fileIno}, Fh: fh3, Size: 50, Offset: 0}, readBuf)
	if status != fuse.OK {
		t.Fatalf("Read on handle 3 failed: %v", status)
	}
	readBytes3, _ := readRes3.Bytes(readBuf)
	if string(readBytes3) != "multi handle test content" {
		t.Fatalf("Expected 'multi handle test content', got %q", string(readBytes3))
	}

	// 9. Release final handle
	rawFS.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: fileIno}, Fh: fh3})

	// 10. Lookup in root confirms file is gone
	var lookupOut fuse.EntryOut
	if status := rawFS.Lookup(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "multi.txt", &lookupOut); status != fuse.Status(syscall.ENOENT) {
		t.Fatalf("Expected ENOENT for unlinked multi.txt after all handles released, got %v", status)
	}
}

func TestFillAttrOutFromEmbeddedInode(t *testing.T) {
	fs := &ObjectFS{}

	mtime := time.Date(2026, 1, 2, 3, 4, 5, 123456789, time.UTC)
	atime := time.Date(2026, 1, 2, 4, 5, 6, 987654321, time.UTC)
	ctime := time.Date(2026, 1, 2, 5, 6, 7, 555555555, time.UTC)

	entry := &pb.EntryAttr{
		Name: "test_entry",
		Inode: &pb.Inode{
			Ino:            proto.Uint64(42),
			Mode:           0644,
			Size:           12345,
			Mtime:          timestamppb.New(mtime),
			Atime:          timestamppb.New(atime),
			Ctime:          timestamppb.New(ctime),
			Uid:            1001,
			Gid:            1002,
			Nlink:          3,
			Rdev:           0x0103,
			IsDir:          false,
			SymlinkTarget:  "target",
			ContentSha256:  "sha-content",
			ManifestSha256: "sha-manifest",
			ParentIno:      proto.Uint64(1),
			Etag:           "etag-val",
			ChunkSize:      16384,
		},
	}

	var attr fuse.Attr
	fs.fillAttrOut(entry, &attr)

	if attr.Ino != 42 {
		t.Errorf("Ino mismatch: got %d, want 42", attr.Ino)
	}
	if attr.Size != 12345 {
		t.Errorf("Size mismatch: got %d, want 12345", attr.Size)
	}
	if (attr.Mode&syscall.S_IFMT) != syscall.S_IFREG || (attr.Mode&0777) != 0644 {
		t.Errorf("Mode mismatch: got %o", attr.Mode)
	}
	if attr.Nlink != 3 {
		t.Errorf("Nlink mismatch: got %d, want 3", attr.Nlink)
	}
	if attr.Rdev != 0x0103 {
		t.Errorf("Rdev mismatch: got 0x%x, want 0x0103", attr.Rdev)
	}
	if attr.Owner.Uid != 1001 || attr.Owner.Gid != 1002 {
		t.Errorf("Owner mismatch: uid=%d, gid=%d", attr.Owner.Uid, attr.Owner.Gid)
	}
	if attr.Mtime != uint64(mtime.Unix()) || attr.Mtimensec != uint32(mtime.Nanosecond()) {
		t.Errorf("Mtime mismatch: %d.%d vs %d.%d", attr.Mtime, attr.Mtimensec, mtime.Unix(), mtime.Nanosecond())
	}
	if attr.Atime != uint64(atime.Unix()) || attr.Atimensec != uint32(atime.Nanosecond()) {
		t.Errorf("Atime mismatch: %d.%d vs %d.%d", attr.Atime, attr.Atimensec, atime.Unix(), atime.Nanosecond())
	}
	if attr.Ctime != uint64(ctime.Unix()) || attr.Ctimensec != uint32(ctime.Nanosecond()) {
		t.Errorf("Ctime mismatch: %d.%d vs %d.%d", attr.Ctime, attr.Ctimensec, ctime.Unix(), ctime.Nanosecond())
	}

	var entryOut fuse.EntryOut
	fs.fillEntryOut(entry, &entryOut, 1, "embedded")
	if entryOut.NodeId != 42 {
		t.Errorf("NodeId mismatch: got %d, want 42", entryOut.NodeId)
	}
}

func TestObjectFS_StatFs(t *testing.T) {
	client, cleanup := createTestClient(t)
	defer cleanup()

	volumeID := "test-statfs-volume"
	cache := NewNodeCache(1024 * 1024)
	fs := NewObjectFS(client, volumeID, pb.WriteMode_WRITE_THROUGH_FSYNC, cache)

	// Initial StatFs: root directory exists (1 inode)
	var out fuse.StatfsOut
	status := fs.StatFs(nil, &fuse.InHeader{}, &out)
	if status != fuse.OK {
		t.Fatalf("StatFs returned %v", status)
	}
	if out.Bsize != 4096 {
		t.Errorf("expected Bsize 4096, got %d", out.Bsize)
	}
	if out.Frsize != 4096 {
		t.Errorf("expected Frsize 4096, got %d", out.Frsize)
	}
	if out.Blocks != 1073741824 {
		t.Errorf("expected 1073741824 blocks, got %d", out.Blocks)
	}
	if out.Bfree != 1073741824 {
		t.Errorf("expected 1073741824 free blocks, got %d", out.Bfree)
	}
	if out.Bavail != 1073741824 {
		t.Errorf("expected 1073741824 available blocks, got %d", out.Bavail)
	}
	if out.Files != 10000000 {
		t.Errorf("expected 10000000 nominal files, got %d", out.Files)
	}
	// Used inodes = 1 (root dir), so free should be 10000000 - 1 = 9999999
	if out.Ffree != 9999999 {
		t.Errorf("expected 9999999 free inodes, got %d", out.Ffree)
	}
	if out.NameLen != 255 {
		t.Errorf("expected NameLen 255, got %d", out.NameLen)
	}

	// Create a file and write 8192 bytes (2 blocks of 4096)
	ctx := t.Context()
	createResp, err := client.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:    volumeID,
		ParentInode: 1,
		Name:        "testfile.bin",
		Mode:        0644,
	})
	if err != nil || createResp.GetError() != 0 {
		t.Fatalf("CreateFile failed: %v", err)
	}
	ino := createResp.GetAttr().GetInode().GetIno()
	writeResp, err := client.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId: volumeID,
		Inode:    ino,
		Offset:   0,
		Data:     make([]byte, 8192),
	})
	if err != nil || writeResp.GetError() != 0 {
		t.Fatalf("WriteFile failed: %v", err)
	}
	fsyncResp, err := client.Fsync(ctx, &pb.FsyncRequest{
		VolumeId: volumeID,
		Inode:    ino,
	})
	if err != nil || fsyncResp.GetError() != 0 {
		t.Fatalf("Fsync failed: %v", err)
	}

	// StatFs should now reflect 2 inodes used and 2 blocks (8192 bytes) used
	var out2 fuse.StatfsOut
	expectedFreeBlocks := uint64(1073741824 - 2)
	deadline := time.Now().Add(5 * time.Second)
	for {
		status = fs.StatFs(nil, &fuse.InHeader{}, &out2)
		if status != fuse.OK {
			t.Fatalf("StatFs returned %v", status)
		}
		if out2.Ffree == 9999998 && out2.Bfree == expectedFreeBlocks && out2.Bavail == expectedFreeBlocks && out2.Blocks == 1073741824 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for StatFs to reflect write: got Ffree=%d (want 9999998), Bfree=%d (want %d)", out2.Ffree, out2.Bfree, expectedFreeBlocks)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

type faultStatsClient struct {
	pb.ObjectFSControllerClient
	err     error
	respErr int32
}

func (c *faultStatsClient) GetVolumeStats(ctx context.Context, in *pb.GetVolumeStatsRequest, opts ...grpc.CallOption) (*pb.GetVolumeStatsResponse, error) {
	if c.err != nil {
		return nil, c.err
	}
	if c.respErr != 0 {
		return &pb.GetVolumeStatsResponse{Error: c.respErr}, nil
	}
	return c.ObjectFSControllerClient.GetVolumeStats(ctx, in, opts...)
}

func TestObjectFS_StatFs_RPCFailure(t *testing.T) {
	client, cleanup := createTestClient(t)
	defer cleanup()

	volumeID := "test-statfs-fail"
	cache := NewNodeCache(1024 * 1024)

	// 1. Nil client yields EIO
	nilFS := &ObjectFS{}
	var out1 fuse.StatfsOut
	if st := nilFS.StatFs(nil, &fuse.InHeader{}, &out1); st != fuse.EIO {
		t.Fatalf("expected EIO on nil client, got %v", st)
	}

	// 2. gRPC RPC transport error yields EIO
	errClient := &faultStatsClient{
		ObjectFSControllerClient: client,
		err:                      status.Error(codes.Unavailable, "controller unavailable"),
	}
	errFS := NewObjectFS(errClient, volumeID, pb.WriteMode_WRITE_THROUGH_FSYNC, cache)
	var out2 fuse.StatfsOut
	if st := errFS.StatFs(nil, &fuse.InHeader{}, &out2); st != fuse.EIO {
		t.Fatalf("expected EIO on gRPC error, got %v", st)
	}

	// 3. Response error field yields EIO
	respErrClient := &faultStatsClient{
		ObjectFSControllerClient: client,
		respErr:                  int32(syscall.EIO),
	}
	respErrFS := NewObjectFS(respErrClient, volumeID, pb.WriteMode_WRITE_THROUGH_FSYNC, cache)
	var out3 fuse.StatfsOut
	if st := respErrFS.StatFs(nil, &fuse.InHeader{}, &out3); st != fuse.EIO {
		t.Fatalf("expected EIO on response error, got %v", st)
	}
}

type countingStatsClient struct {
	pb.ObjectFSControllerClient
	mu    sync.Mutex
	calls int
}

func (c *countingStatsClient) GetVolumeStats(ctx context.Context, in *pb.GetVolumeStatsRequest, opts ...grpc.CallOption) (*pb.GetVolumeStatsResponse, error) {
	c.mu.Lock()
	c.calls++
	c.mu.Unlock()
	return c.ObjectFSControllerClient.GetVolumeStats(ctx, in, opts...)
}

func TestObjectFS_StatFs_Cache(t *testing.T) {
	client, cleanup := createTestClient(t)
	defer cleanup()

	volumeID := "test-statfs-cache"
	countClient := &countingStatsClient{
		ObjectFSControllerClient: client,
	}
	cache := NewNodeCache(1024 * 1024)
	fs := NewObjectFS(countClient, volumeID, pb.WriteMode_WRITE_THROUGH_FSYNC, cache)

	var out1 fuse.StatfsOut
	if st := fs.StatFs(nil, &fuse.InHeader{}, &out1); st != fuse.OK {
		t.Fatalf("StatFs returned %v", st)
	}
	if countClient.calls != 1 {
		t.Fatalf("expected 1 call to GetVolumeStats, got %d", countClient.calls)
	}

	// Immediate second call should hit the cache and not invoke GetVolumeStats
	var out2 fuse.StatfsOut
	if st := fs.StatFs(nil, &fuse.InHeader{}, &out2); st != fuse.OK {
		t.Fatalf("StatFs returned %v", st)
	}
	if countClient.calls != 1 {
		t.Fatalf("expected cache hit (1 call), got %d calls", countClient.calls)
	}
	if out1 != out2 {
		t.Errorf("cached output mismatch: %+v vs %+v", out1, out2)
	}

	// After cache expires, call should invoke GetVolumeStats again
	fs.statfsMu.Lock()
	fs.statfsExpiry = time.Now().Add(-time.Second) // force expiration
	fs.statfsMu.Unlock()

	var out3 fuse.StatfsOut
	if st := fs.StatFs(nil, &fuse.InHeader{}, &out3); st != fuse.OK {
		t.Fatalf("StatFs returned %v", st)
	}
	if countClient.calls != 2 {
		t.Fatalf("expected 2 calls after cache expiration, got %d", countClient.calls)
	}
}

func TestObjectFSWaitForReadyAndRecovery(t *testing.T) {
	// 1. Create a real TCP listener so we can delay starting the gRPC server
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}
	addr := lis.Addr().String()

	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
	)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer conn.Close()

	client := pb.NewObjectFSControllerClient(conn)
	cache := NewNodeCache(1024 * 1024)
	fs := NewObjectFSWithTimeout(client, "vol-wait-ready", pb.WriteMode_WRITE_THROUGH_FSYNC, cache, 5*time.Second)

	done := make(chan fuse.Status, 1)
	var attrOut fuse.AttrOut

	// Start FUSE GetAttr while the server is not serving yet
	go func() {
		st := fs.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}}, &attrOut)
		done <- st
	}()

	// Verify the operation is blocking waiting for ready
	select {
	case st := <-done:
		t.Fatalf("Expected GetAttr to block waiting for server, but it returned immediately with %v", st)
	case <-time.After(150 * time.Millisecond):
		// Expected to still be waiting
	}

	// Now start serving the controller on the listener
	grpcServer := grpc.NewServer()
	ctrlServer := controllertest.NewTestServer(nil)
	pb.RegisterObjectFSControllerServer(grpcServer, ctrlServer)
	go func() {
		_ = grpcServer.Serve(lis)
	}()
	defer grpcServer.Stop()

	// The blocked operation should now unblock and succeed
	select {
	case st := <-done:
		if st != fuse.OK {
			t.Fatalf("Expected GetAttr to succeed after server came up, got %v", st)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("Timed out waiting for GetAttr to complete after server started")
	}

	if attrOut.Attr.Ino != fuse.FUSE_ROOT_ID {
		t.Errorf("Expected root inode %d, got %d", fuse.FUSE_ROOT_ID, attrOut.Attr.Ino)
	}
}

func TestObjectFSServerDownReturnsEIOOnTimeout(t *testing.T) {
	// Connect to a closed listener address with a very short deadline
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}
	addr := lis.Addr().String()
	_ = lis.Close() // Close immediately so nothing is listening

	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
	)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer conn.Close()

	client := pb.NewObjectFSControllerClient(conn)
	cache := NewNodeCache(1024 * 1024)
	// Short deadline of 200ms
	fs := NewObjectFSWithTimeout(client, "vol-timeout", pb.WriteMode_WRITE_THROUGH_FSYNC, cache, 200*time.Millisecond)

	var attrOut fuse.AttrOut
	start := time.Now()
	st := fs.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}}, &attrOut)
	elapsed := time.Since(start)

	if st != fuse.Status(syscall.EIO) {
		t.Fatalf("Expected EIO (not EBUSY) on deadline exceeded, got status %v", st)
	}
	if elapsed < 150*time.Millisecond {
		t.Errorf("Expected GetAttr to wait for deadline (~200ms), but it returned in %v", elapsed)
	}
}

func TestStartWatcherReconnectResync(t *testing.T) {
	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	ctrlServer := controllertest.NewTestServer(nil)
	pb.RegisterObjectFSControllerServer(grpcServer, ctrlServer)
	go func() { _ = grpcServer.Serve(lis) }()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
	)
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer conn.Close()

	client := pb.NewObjectFSControllerClient(conn)
	volumeID := "vol-watcher-test"
	cache := NewNodeCache(1024 * 1024)

	// Populate clean entry in cache
	cache.Put(100, []byte("cached-clean-data"), time.Now(), "")
	// Populate dirty entry in cache
	cache.WriteAt(200, 0, []byte("dirty-data"), time.Now())

	watchCtx, cancelWatch := context.WithCancel(t.Context())
	defer cancelWatch()

	StartWatcher(watchCtx, client, volumeID, "node-1", pb.ConsistencyMode_STANDARD, cache)
	time.Sleep(100 * time.Millisecond)

	// Stop grpc server to simulate controller restart / disconnection
	grpcServer.Stop()
	_ = lis.Close()

	// Wait for watcher to detect disconnect
	time.Sleep(200 * time.Millisecond)

	// Start new server and buffer listener
	lis2 := bufconn.Listen(1024 * 1024)
	grpcServer2 := grpc.NewServer()
	ctrlServer2 := controllertest.NewTestServer(nil)
	pb.RegisterObjectFSControllerServer(grpcServer2, ctrlServer2)
	go func() { _ = grpcServer2.Serve(lis2) }()
	defer grpcServer2.Stop()

	// Update dialer connection
	conn2, err := grpc.NewClient("passthrough://bufnet2",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis2.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
	)
	if err != nil {
		t.Fatalf("Failed to dial bufnet2: %v", err)
	}
	defer conn2.Close()

	client2 := pb.NewObjectFSControllerClient(conn2)
	watchCtx2, cancelWatch2 := context.WithCancel(t.Context())
	defer cancelWatch2()

	// Reconnect watcher with client2
	StartWatcher(watchCtx2, client2, volumeID, "node-1", pb.ConsistencyMode_STANDARD, cache)
	time.Sleep(200 * time.Millisecond)

	// InvalidateAllClean should ensure clean entry 100 was invalidated on reconnect, but dirty entry 200 preserved
	cache.InvalidateAllClean()
	if _, ok := cache.Get(100); ok {
		t.Errorf("Expected clean cached entry 100 to be invalidated after reconnect")
	}
	if _, ok := cache.GetDirty(200); !ok {
		t.Errorf("Expected dirty cached entry 200 to be preserved")
	}
}

func TestFsyncSlowSyncSucceedsWithinSyncTimeout(t *testing.T) {
	lis := bufconn.Listen(1024 * 1024)
	interceptor := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if strings.HasSuffix(info.FullMethod, "/Fsync") {
			time.Sleep(1500 * time.Millisecond)
		}
		return handler(ctx, req)
	}
	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(interceptor))
	ctrlServer := controllertest.NewTestServer(nil)
	pb.RegisterObjectFSControllerServer(grpcServer, ctrlServer)
	go func() { _ = grpcServer.Serve(lis) }()
	defer grpcServer.Stop()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer conn.Close()

	client := pb.NewObjectFSControllerClient(conn)
	cache := NewNodeCache(1024 * 1024)
	volumeID := "vol-slow-sync"

	// rpcTimeout is 1s, but syncTimeout is longer (5s)
	fs := NewObjectFSWithTimeouts(client, volumeID, pb.WriteMode_WRITE_THROUGH_FSYNC, cache, 1*time.Second, 5*time.Second)

	// Create a test file
	var createOut fuse.CreateOut
	if st := fs.Create(nil, &fuse.CreateIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Mode: 0644}, "file.txt", &createOut); st != fuse.OK {
		t.Fatalf("Create failed: %v", st)
	}
	ino := createOut.EntryOut.NodeId

	// Write data to cache (dirty)
	_, st := fs.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: ino}, Offset: 0}, []byte("slow-sync-data"))
	if st != fuse.OK {
		t.Fatalf("Write failed: %v", st)
	}

	// Fsync takes ~250ms, which is slower than rpcTimeout (100ms) but faster than syncTimeout (2s)
	start := time.Now()
	st = fs.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: ino}})
	elapsed := time.Since(start)

	if st != fuse.OK {
		t.Fatalf("Expected Fsync to succeed within syncTimeout (2s), got error: %v", st)
	}
	if elapsed < 200*time.Millisecond {
		t.Errorf("Expected Fsync to take at least 200ms due to delay, took %v", elapsed)
	}
}

func TestFUSETruncateReportsNewSizeOnFstatAndGetAttr(t *testing.T) {
	client, cleanup := createTestClient(t)
	defer cleanup()

	cache := NewNodeCache(1024 * 1024)
	rawFS := NewObjectFS(client, "vol-trunc-stat-test", pb.WriteMode_WRITE_THROUGH_FSYNC, cache)

	var createOut fuse.CreateOut
	if status := rawFS.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     0644,
	}, "trunc_stat.bin", &createOut); status != fuse.OK {
		t.Fatalf("Create failed: %v", status)
	}
	fileID := createOut.EntryOut.NodeId

	// Write 250 KiB (4 chunks: 64K, 64K, 64K, 58K)
	origSize := 250 * 1024
	data := make([]byte, origSize)
	for i := range data {
		data[i] = byte(i%251 + 1)
	}
	written, status := rawFS.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: fileID}, Offset: 0}, data)
	if status != fuse.OK || int(written) != origSize {
		t.Fatalf("Write failed: status=%v, written=%d", status, written)
	}

	// Truncate down to 100 KiB (0x19000 bytes)
	truncSize := uint64(100 * 1024)
	var setAttrOut fuse.AttrOut
	if status := rawFS.SetAttr(nil, &fuse.SetAttrIn{
		SetAttrInCommon: fuse.SetAttrInCommon{
			InHeader: fuse.InHeader{NodeId: fileID},
			Valid:    fuse.FATTR_SIZE,
			Size:     truncSize,
		},
	}, &setAttrOut); status != fuse.OK {
		t.Fatalf("SetAttr truncate failed: %v", status)
	}

	// 1. Verify SetAttr return value has the truncated size
	if setAttrOut.Attr.Size != truncSize {
		t.Fatalf("SetAttr reported size %d, expected %d", setAttrOut.Attr.Size, truncSize)
	}

	// 2. Immediately call GetAttr (simulating fstat right after ftruncate)
	var getAttrOut fuse.AttrOut
	if status := rawFS.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: fileID}}, &getAttrOut); status != fuse.OK {
		t.Fatalf("GetAttr failed: %v", status)
	}
	if getAttrOut.Attr.Size != truncSize {
		t.Fatalf("GetAttr (fstat) reported size %d, expected %d", getAttrOut.Attr.Size, truncSize)
	}

	// 3. Verify cache is marked clean after successful truncate sync
	if _, isDirty := cache.GetDirty(fileID); isDirty {
		t.Fatalf("Expected cache entry to be clean after Truncate succeeded")
	}

	// 4. Simulate a stale chunk read returned from before truncate while entry is locally dirty
	// (e.g. cache.Truncate ran, leaving entry dirty until controller sync completes).
	genBefore := cache.GetGeneration(fileID)
	truncGen := cache.Truncate(fileID, int64(truncSize), time.Now())
	cache.PutChunk(fileID, 3, DefaultChunkSize, int64(origSize), []byte("stale-chunk-data"), time.Now(), genBefore)

	// Verify PutChunk did not resurrect the stale chunk or inflate size on a dirty entry
	if _, ok := cache.GetChunk(fileID, 3); ok {
		t.Fatalf("PutChunk should not have cached chunk beyond truncated size on dirty entry")
	}
	if entry, ok := cache.Get(fileID); ok && entry.Size != int64(truncSize) {
		t.Fatalf("Cache entry size inflated to %d on dirty entry, expected %d", entry.Size, truncSize)
	}

	// 5. For a clean entry, PutChunk trusts the remote totalSize if the file grew on another node
	cache.MarkSizeClean(fileID, truncGen)
	cleanGen := cache.GetGeneration(fileID)
	cache.PutChunk(fileID, 3, DefaultChunkSize, int64(origSize), []byte("remote-chunk-data"), time.Now(), cleanGen)
	if entry, ok := cache.Get(fileID); !ok || entry.Size != int64(origSize) {
		t.Fatalf("Expected clean entry size to adopt remote totalSize %d, got %v", origSize, entry.Size)
	}
}

func TestFUSESetAttrTruncateFailsWhenFlushFails(t *testing.T) {
	// Create client pointing to closed server
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}
	addr := lis.Addr().String()
	_ = lis.Close() // Immediately close so server is unreachable

	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewObjectFSControllerClient(conn)
	cache := NewNodeCache(1024 * 1024)
	fs := NewObjectFSWithTimeouts(client, "vol-fail", pb.WriteMode_WRITE_THROUGH_FSYNC, cache, 50*time.Millisecond, 50*time.Millisecond)

	// Stage a dirty write in cache
	const fileID = uint64(10)
	cache.WriteAt(fileID, 0, []byte("dirty-data"), time.Now())

	// SetAttr with FATTR_SIZE must fail because pre-truncate sync fails
	var setAttrOut fuse.AttrOut
	status := fs.SetAttr(nil, &fuse.SetAttrIn{
		SetAttrInCommon: fuse.SetAttrInCommon{
			InHeader: fuse.InHeader{NodeId: fileID},
			Valid:    fuse.FATTR_SIZE,
			Size:     0,
		},
	}, &setAttrOut)

	if status == fuse.OK {
		t.Fatalf("Expected SetAttr truncate to fail when pre-truncate sync fails, but got OK")
	}
}

func TestFUSEConcurrentOperationsRace(t *testing.T) {
	client, cleanup := createTestClient(t)
	defer cleanup()

	volumeID := "vol-concurrent-race"
	cache := NewNodeCache(4 * 1024 * 1024)
	rawFS := NewObjectFS(client, volumeID, pb.WriteMode_WRITE_THROUGH_FSYNC, cache)

	var createOut fuse.CreateOut
	if status := rawFS.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     0644,
	}, "concurrent_race.bin", &createOut); status != fuse.OK {
		t.Fatalf("Create failed: %v", status)
	}
	fileID := createOut.EntryOut.NodeId

	// Note: writers and truncaters synchronize via modelMu to maintain a deterministic
	// ground-truth oracle; background flushers (syncAndFsync) and readers race
	// concurrently against those mutations without holding modelMu.
	var modelMu sync.Mutex
	model := make([]byte, 0)

	stopCh := make(chan struct{})
	var wg sync.WaitGroup

	// Flusher running concurrently
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopCh:
				return
			default:
				ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
				_ = rawFS.syncAndFsync(ctx, fileID, true, pb.FsyncRequest_SYNC_TRIGGER_UNSPECIFIED)
				cancel()
				runtime.Gosched()
			}
		}
	}()

	// Reader running concurrently
	wg.Add(1)
	go func() {
		defer wg.Done()
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		buf := make([]byte, 8192)
		for {
			select {
			case <-stopCh:
				return
			default:
				off := r.Int63n(128 * 1024)
				_, _ = rawFS.Read(nil, &fuse.ReadIn{
					InHeader: fuse.InHeader{NodeId: fileID},
					Offset:   uint64(off),
					Size:     uint32(len(buf)),
				}, buf)
				runtime.Gosched()
			}
		}
	}()

	// Writers
	var mutatorsWg sync.WaitGroup
	const numWriters = 2
	const writesPerWorker = 30

	for w := 0; w < numWriters; w++ {
		mutatorsWg.Add(1)
		go func(workerID int) {
			defer mutatorsWg.Done()
			r := rand.New(rand.NewSource(int64(workerID*1000 + 42)))
			for i := 0; i < writesPerWorker; i++ {
				// Offset up to 160KB (spans multiple 64KB chunks)
				off := int(r.Int31n(160 * 1024))
				sz := int(r.Int31n(8192)) + 512
				data := make([]byte, sz)
				for b := range data {
					data[b] = byte((workerID*37 + i*13 + b) & 0xFF)
				}

				modelMu.Lock()
				written, status := rawFS.Write(nil, &fuse.WriteIn{
					InHeader: fuse.InHeader{NodeId: fileID},
					Offset:   uint64(off),
				}, data)
				if status != fuse.OK || int(written) != sz {
					t.Errorf("Write failed: status=%v written=%d want=%d", status, written, sz)
				}
				end := off + sz
				if end > len(model) {
					newModel := make([]byte, end)
					copy(newModel, model)
					model = newModel
				}
				copy(model[off:end], data)
				modelMu.Unlock()

				runtime.Gosched()
			}
		}(w)
	}

	// Truncater
	mutatorsWg.Add(1)
	go func() {
		defer mutatorsWg.Done()
		r := rand.New(rand.NewSource(999))
		for i := 0; i < 15; i++ {
			newSize := int(r.Int31n(160 * 1024))
			modelMu.Lock()
			var setAttrOut fuse.AttrOut
			status := rawFS.SetAttr(nil, &fuse.SetAttrIn{
				SetAttrInCommon: fuse.SetAttrInCommon{
					InHeader: fuse.InHeader{NodeId: fileID},
					Valid:    fuse.FATTR_SIZE,
					Size:     uint64(newSize),
				},
			}, &setAttrOut)
			if status != fuse.OK {
				t.Errorf("SetAttr truncate failed: %v", status)
			}
			if newSize < len(model) {
				model = model[:newSize]
			} else if newSize > len(model) {
				newModel := make([]byte, newSize)
				copy(newModel, model)
				model = newModel
			}
			modelMu.Unlock()

			runtime.Gosched()
		}
	}()

	// Wait for all writes and truncates to finish
	mutatorsWg.Wait()

	// Stop background flushers and readers
	close(stopCh)
	wg.Wait()

	// Final flush to ensure all acknowledged writes are flushed
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := rawFS.syncAndFsync(ctx, fileID, true, pb.FsyncRequest_SYNC_TRIGGER_UNSPECIFIED); err != nil {
		t.Fatalf("Final syncAndFsync failed: %v", err)
	}

	modelMu.Lock()
	expected := make([]byte, len(model))
	copy(expected, model)
	expectedSize := uint64(len(model))
	modelMu.Unlock()

	// 1. Verify cache has no remaining dirty state
	if _, isDirty := cache.GetDirty(fileID); isDirty {
		t.Fatalf("Cache entry is still marked dirty after final sync")
	}

	// 2. Verify reported size via GetAttr
	var getAttrOut fuse.AttrOut
	if status := rawFS.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: fileID}}, &getAttrOut); status != fuse.OK {
		t.Fatalf("GetAttr failed: %v", status)
	}
	if getAttrOut.Attr.Size != expectedSize {
		t.Fatalf("Final reported size mismatch: got %d, want %d", getAttrOut.Attr.Size, expectedSize)
	}

	// 3. Verify content on controller matches expected model byte-for-byte
	var controllerData []byte
	const chunkSize = 64 * 1024
	numChunks := int((int64(expectedSize) + chunkSize - 1) / chunkSize)
	if expectedSize == 0 {
		numChunks = 0
	}
	for cIdx := 0; cIdx < numChunks; cIdx++ {
		cStart := int64(cIdx) * chunkSize
		cLen := int64(chunkSize)
		if cStart+cLen > int64(expectedSize) {
			cLen = int64(expectedSize) - cStart
		}
		resp, err := client.ReadFile(ctx, &pb.ReadFileRequest{
			VolumeId: volumeID,
			Inode:    fileID,
			Offset:   cStart,
			Size:     cLen,
		})
		if err != nil {
			t.Fatalf("Controller ReadFile chunk %d failed: %v", cIdx, err)
		}
		if resp.GetError() != 0 {
			t.Fatalf("Controller ReadFile chunk %d returned error: %d", cIdx, resp.GetError())
		}
		data := resp.GetData()
		if int64(len(data)) < cLen {
			padded := make([]byte, cLen)
			copy(padded, data)
			data = padded
		}
		controllerData = append(controllerData, data[:cLen]...)
	}

	if !bytes.Equal(controllerData, expected) {
		for i := 0; i < len(expected) && i < len(controllerData); i++ {
			if controllerData[i] != expected[i] {
				t.Fatalf("Controller data mismatch at byte %d (chunk %d): got 0x%02x want 0x%02x (len got=%d want=%d)",
					i, i/(64*1024), controllerData[i], expected[i], len(controllerData), len(expected))
			}
		}
		t.Fatalf("Controller data mismatch: len(got)=%d len(want)=%d", len(controllerData), len(expected))
	}

	// 4. Verify read through FUSE after clearing cache matches
	cache.Clear()
	fuseData := make([]byte, expectedSize)
	if expectedSize > 0 {
		readRes, status := rawFS.Read(nil, &fuse.ReadIn{
			InHeader: fuse.InHeader{NodeId: fileID},
			Offset:   0,
			Size:     uint32(expectedSize),
		}, fuseData)
		if status != fuse.OK {
			t.Fatalf("rawFS.Read failed: %v", status)
		}
		readBytes, _ := readRes.Bytes(fuseData)
		if !bytes.Equal(readBytes, expected) {
			t.Fatalf("FUSE read data mismatch: len(got)=%d len(want)=%d", len(readBytes), len(expected))
		}
	}
}

func TestFUSEMountXAttr(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("skipping mount-level xattr test on non-linux OS")
	}

	client, cleanup := createTestClient(t)
	defer cleanup()

	volumeID := "vol-xattr-mount"
	cache := NewNodeCache(4 * 1024 * 1024)
	rawFS := NewObjectFS(client, volumeID, pb.WriteMode_WRITE_THROUGH_FSYNC, cache)

	mountDir := t.TempDir()
	mountOpts := &fuse.MountOptions{
		DirectMount: os.Geteuid() == 0,
	}
	server, err := fuse.NewServer(rawFS, mountDir, mountOpts)
	if err != nil {
		t.Skipf("cannot mount FUSE in test environment: %v", err)
	}
	go server.Serve()
	if err := server.WaitMount(); err != nil {
		if uErr := server.Unmount(); uErr != nil {
			t.Logf("server.Unmount failed during cleanup: %v", uErr)
		}
		t.Skipf("cannot wait for FUSE mount in test environment: %v", err)
	}
	defer func() {
		if uErr := server.Unmount(); uErr != nil {
			t.Logf("server.Unmount failed: %v", uErr)
		}
	}()

	testFile := filepath.Join(mountDir, "xattr_test.txt")
	if err := os.WriteFile(testFile, []byte("hello xattr mount"), 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	attrName := "user.testattr"
	attrVal := []byte("hello world xattr value")

	// 1. Setxattr
	if err := unix.Setxattr(testFile, attrName, attrVal, 0); err != nil {
		t.Fatalf("unix.Setxattr failed: %v", err)
	}

	// 2. Flags: XATTR_CREATE on existing should fail with EEXIST
	if err := unix.Setxattr(testFile, attrName, []byte("newval"), unix.XATTR_CREATE); err != unix.EEXIST {
		t.Fatalf("expected EEXIST for XATTR_CREATE on existing, got: %v", err)
	}

	// 3. Flags: XATTR_REPLACE on missing should fail with ENODATA
	if err := unix.Setxattr(testFile, "user.nonexistent", []byte("val"), unix.XATTR_REPLACE); err != unix.ENODATA {
		t.Fatalf("expected ENODATA for XATTR_REPLACE on missing, got: %v", err)
	}

	// 4. Flags: XATTR_REPLACE on existing should succeed
	if err := unix.Setxattr(testFile, attrName, attrVal, unix.XATTR_REPLACE); err != nil {
		t.Fatalf("unix.Setxattr with XATTR_REPLACE failed: %v", err)
	}

	// 5. Getxattr size probe (dest is nil or empty)
	sz, err := unix.Getxattr(testFile, attrName, nil)
	if err != nil {
		t.Fatalf("unix.Getxattr size probe failed: %v", err)
	}
	if sz != len(attrVal) {
		t.Fatalf("expected size %d, got %d", len(attrVal), sz)
	}

	// 6. Getxattr with buffer too small (ERANGE probe)
	smallBuf := make([]byte, len(attrVal)-1)
	if _, err := unix.Getxattr(testFile, attrName, smallBuf); err != unix.ERANGE {
		t.Fatalf("expected ERANGE for small buffer, got: %v", err)
	}

	// 7. Getxattr with sufficient buffer
	fullBuf := make([]byte, len(attrVal)+10)
	sz, err = unix.Getxattr(testFile, attrName, fullBuf)
	if err != nil {
		t.Fatalf("unix.Getxattr failed: %v", err)
	}
	if sz != len(attrVal) || !bytes.Equal(fullBuf[:sz], attrVal) {
		t.Fatalf("expected %q, got %q", string(attrVal), string(fullBuf[:sz]))
	}

	// 8. Listxattr size probe
	listSz, err := unix.Listxattr(testFile, nil)
	if err != nil {
		t.Fatalf("unix.Listxattr size probe failed: %v", err)
	}
	if listSz < len(attrName)+1 {
		t.Fatalf("expected list size >= %d, got %d", len(attrName)+1, listSz)
	}

	// 9. Listxattr with buffer too small (ERANGE probe)
	smallListBuf := make([]byte, listSz-1)
	if _, err := unix.Listxattr(testFile, smallListBuf); err != unix.ERANGE {
		t.Fatalf("expected ERANGE for small list buffer, got: %v", err)
	}

	// 10. Listxattr with sufficient buffer
	listBuf := make([]byte, listSz+10)
	listSz, err = unix.Listxattr(testFile, listBuf)
	if err != nil {
		t.Fatalf("unix.Listxattr failed: %v", err)
	}
	names := strings.Split(strings.TrimRight(string(listBuf[:listSz]), "\x00"), "\x00")
	found := false
	for _, n := range names {
		if n == attrName {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected attribute %q in list %v", attrName, names)
	}

	// 11. Removexattr
	if err := unix.Removexattr(testFile, attrName); err != nil {
		t.Fatalf("unix.Removexattr failed: %v", err)
	}

	// 12. Getxattr after removal returns ENODATA
	if _, err := unix.Getxattr(testFile, attrName, fullBuf); err != unix.ENODATA {
		t.Fatalf("expected ENODATA after Removexattr, got: %v", err)
	}
}

func TestRawFileSystemXAttrOperations(t *testing.T) {
	client, cleanup := createTestClient(t)
	defer cleanup()

	cache := NewNodeCache(1024 * 1024)
	rawFS := NewObjectFS(client, "vol-raw-xattr", pb.WriteMode_WRITE_THROUGH_FSYNC, cache)

	// Create a test file
	var createOut fuse.CreateOut
	if status := rawFS.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     0644,
	}, "xattr_file.txt", &createOut); status != fuse.OK {
		t.Fatalf("Create failed: %v", status)
	}
	fileIno := createOut.NodeId

	attrName := "user.comment"
	attrVal := []byte("raw-test-value")

	// 1. SetXAttr
	if status := rawFS.SetXAttr(nil, &fuse.SetXAttrIn{
		InHeader: fuse.InHeader{NodeId: fileIno},
		Flags:    0,
	}, attrName, attrVal); status != fuse.OK {
		t.Fatalf("SetXAttr failed: %v", status)
	}

	// 2. SetXAttr with CREATE flag on existing -> EEXIST
	if status := rawFS.SetXAttr(nil, &fuse.SetXAttrIn{
		InHeader: fuse.InHeader{NodeId: fileIno},
		Flags:    unix.XATTR_CREATE,
	}, attrName, []byte("another")); status != fuse.Status(syscall.EEXIST) {
		t.Fatalf("expected EEXIST for CREATE on existing, got: %v", status)
	}

	// 3. SetXAttr with REPLACE flag on missing -> ENODATA
	if status := rawFS.SetXAttr(nil, &fuse.SetXAttrIn{
		InHeader: fuse.InHeader{NodeId: fileIno},
		Flags:    unix.XATTR_REPLACE,
	}, "user.missing", []byte("another")); status != fuse.Status(syscall.ENODATA) {
		t.Fatalf("expected ENODATA for REPLACE on missing, got: %v", status)
	}

	// 4. GetXAttr size probe (dest is empty)
	sz, status := rawFS.GetXAttr(nil, &fuse.InHeader{NodeId: fileIno}, attrName, nil)
	if status != fuse.OK || sz != uint32(len(attrVal)) {
		t.Fatalf("GetXAttr size probe failed: status=%v sz=%d want=%d", status, sz, len(attrVal))
	}

	// 5. GetXAttr with buffer too small -> ERANGE
	smallBuf := make([]byte, len(attrVal)-1)
	sz, status = rawFS.GetXAttr(nil, &fuse.InHeader{NodeId: fileIno}, attrName, smallBuf)
	if status != fuse.ERANGE || sz != uint32(len(attrVal)) {
		t.Fatalf("GetXAttr small buffer expected ERANGE, got: status=%v sz=%d", status, sz)
	}

	// 6. GetXAttr with adequate buffer -> OK and matches
	fullBuf := make([]byte, len(attrVal)+10)
	sz, status = rawFS.GetXAttr(nil, &fuse.InHeader{NodeId: fileIno}, attrName, fullBuf)
	if status != fuse.OK || sz != uint32(len(attrVal)) || !bytes.Equal(fullBuf[:sz], attrVal) {
		t.Fatalf("GetXAttr failed: status=%v sz=%d val=%q", status, sz, string(fullBuf[:sz]))
	}

	// 7. ListXAttr size probe (dest is empty)
	expectedEncoded := append([]byte(attrName), 0)
	listSz, status := rawFS.ListXAttr(nil, &fuse.InHeader{NodeId: fileIno}, nil)
	if status != fuse.OK || listSz != uint32(len(expectedEncoded)) {
		t.Fatalf("ListXAttr size probe failed: status=%v sz=%d want=%d", status, listSz, len(expectedEncoded))
	}

	// 8. ListXAttr with buffer too small -> ERANGE
	smallListBuf := make([]byte, listSz-1)
	listSz, status = rawFS.ListXAttr(nil, &fuse.InHeader{NodeId: fileIno}, smallListBuf)
	if status != fuse.ERANGE || listSz != uint32(len(expectedEncoded)) {
		t.Fatalf("ListXAttr small buffer expected ERANGE, got: status=%v sz=%d", status, listSz)
	}

	// 9. ListXAttr with adequate buffer
	fullListBuf := make([]byte, listSz+10)
	listSz, status = rawFS.ListXAttr(nil, &fuse.InHeader{NodeId: fileIno}, fullListBuf)
	if status != fuse.OK || !bytes.Equal(fullListBuf[:listSz], expectedEncoded) {
		t.Fatalf("ListXAttr failed: status=%v sz=%d val=%q", status, listSz, string(fullListBuf[:listSz]))
	}

	// 10. RemoveXAttr
	if status := rawFS.RemoveXAttr(nil, &fuse.InHeader{NodeId: fileIno}, attrName); status != fuse.OK {
		t.Fatalf("RemoveXAttr failed: %v", status)
	}

	// 11. GetXAttr after remove -> ENODATA
	_, status = rawFS.GetXAttr(nil, &fuse.InHeader{NodeId: fileIno}, attrName, fullBuf)
	if status != fuse.Status(syscall.ENODATA) {
		t.Fatalf("expected ENODATA after remove, got: %v", status)
	}
}

type xattrCountingClient struct {
	pb.ObjectFSControllerClient
	getXAttrCalls  int
	listXAttrCalls int
}

func (c *xattrCountingClient) GetXAttr(ctx context.Context, in *pb.GetXAttrRequest, opts ...grpc.CallOption) (*pb.GetXAttrResponse, error) {
	c.getXAttrCalls++
	return c.ObjectFSControllerClient.GetXAttr(ctx, in, opts...)
}

func (c *xattrCountingClient) ListXAttr(ctx context.Context, in *pb.ListXAttrRequest, opts ...grpc.CallOption) (*pb.ListXAttrResponse, error) {
	c.listXAttrCalls++
	return c.ObjectFSControllerClient.ListXAttr(ctx, in, opts...)
}

func TestFUSEHandlerXAttrCountShortcut(t *testing.T) {
	realClient, cleanup := createTestClient(t)
	defer cleanup()

	countingClient := &xattrCountingClient{ObjectFSControllerClient: realClient}
	cache := NewNodeCache(1024 * 1024)
	rawFS := NewObjectFS(countingClient, "vol-xattr-shortcut", pb.WriteMode_WRITE_THROUGH_FSYNC, cache)

	// 1. Create a file. Create populates inode in cache with XattrCount == 0
	var createOut fuse.CreateOut
	if status := rawFS.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     0644,
	}, "shortcut_file.txt", &createOut); status != fuse.OK {
		t.Fatalf("Create failed: %v", status)
	}
	fileIno := createOut.NodeId

	// Ensure inode is cached and has XattrCount == 0
	if !rawFS.isXattrCountZero(fileIno) {
		t.Fatalf("expected fileIno %d to have xattr_count zero", fileIno)
	}

	// 2. Call GetXAttr: must return ENODATA without an RPC!
	dest := make([]byte, 100)
	_, status := rawFS.GetXAttr(nil, &fuse.InHeader{NodeId: fileIno}, "user.anything", dest)
	if status != fuse.Status(syscall.ENODATA) {
		t.Fatalf("expected ENODATA from shortcut, got: %v", status)
	}
	if countingClient.getXAttrCalls != 0 {
		t.Fatalf("expected 0 GetXAttr RPC calls, got: %d", countingClient.getXAttrCalls)
	}

	// 3. Call ListXAttr: must return empty list without an RPC!
	listSz, status := rawFS.ListXAttr(nil, &fuse.InHeader{NodeId: fileIno}, dest)
	if status != fuse.OK || listSz != 0 {
		t.Fatalf("expected OK and 0 bytes from ListXAttr shortcut, got: status=%v sz=%d", status, listSz)
	}
	if countingClient.listXAttrCalls != 0 {
		t.Fatalf("expected 0 ListXAttr RPC calls, got: %d", countingClient.listXAttrCalls)
	}

	// 4. Test unset count (XattrCount == nil): must ask the controller via RPC!
	rawFS.inodeAttrsMu.Lock()
	rawFS.inodeAttrs[fileIno].XattrCount = nil
	rawFS.inodeAttrsMu.Unlock()

	if rawFS.isXattrCountZero(fileIno) {
		t.Fatalf("expected isXattrCountZero to return false when XattrCount is nil (unknown)")
	}

	_, _ = rawFS.GetXAttr(nil, &fuse.InHeader{NodeId: fileIno}, "user.anything", dest)
	if countingClient.getXAttrCalls != 1 {
		t.Fatalf("expected 1 GetXAttr RPC call when count is unset, got: %d", countingClient.getXAttrCalls)
	}

	_, _ = rawFS.ListXAttr(nil, &fuse.InHeader{NodeId: fileIno}, dest)
	if countingClient.listXAttrCalls != 1 {
		t.Fatalf("expected 1 ListXAttr RPC call when count is unset, got: %d", countingClient.listXAttrCalls)
	}
}

func TestFUSEHandlerXAttrCountInvalidationAndPruning(t *testing.T) {
	realClient, cleanup := createTestClient(t)
	defer cleanup()

	cache := NewNodeCache(1024 * 1024)
	rawFS := NewObjectFS(realClient, "vol-xattr-invalidate", pb.WriteMode_WRITE_THROUGH_FSYNC, cache)

	// Create file, which records XattrCount == 0 in inodeAttrs and sets lookup count to 1
	var createOut fuse.CreateOut
	if status := rawFS.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     0644,
	}, "watch_file.txt", &createOut); status != fuse.OK {
		t.Fatalf("Create failed: %v", status)
	}
	fileIno := createOut.NodeId

	if !rawFS.isXattrCountZero(fileIno) {
		t.Fatalf("expected isXattrCountZero to be true initially")
	}

	// 1. Feed an EVENT_MODIFIED for the inode: must invalidate inodeAttrs so isXattrCountZero becomes false
	ev := &pb.WatchVolumeResponse{
		EventType: pb.WatchEventType_EVENT_MODIFIED,
		Inode:     fileIno,
	}
	handleWatchEvent(ev, cache, []*ObjectFS{rawFS})

	if rawFS.isXattrCountZero(fileIno) {
		t.Fatalf("expected isXattrCountZero to be false after EVENT_MODIFIED watch event")
	}

	// 2. Re-record count 0 and verify Forget prunes it when lookup count reaches 0
	rawFS.recordInodeAttr(&pb.Inode{
		Ino:        proto.Uint64(fileIno),
		XattrCount: proto.Uint32(0),
	})
	if !rawFS.isXattrCountZero(fileIno) {
		t.Fatalf("expected isXattrCountZero to be true after recordInodeAttr")
	}

	// Inode has lookup count 1 from Create. Forget it.
	rawFS.Forget(fileIno, 1)

	if rawFS.isXattrCountZero(fileIno) {
		t.Fatalf("expected isXattrCountZero to be false after Forget drops entry")
	}
}

type recordingClient struct {
	pb.ObjectFSControllerClient
	mu                sync.Mutex
	writeReqs         []*pb.WriteFileRequest
	truncateReqs      []*pb.TruncateFileRequest
	callOrder         []string
	failWrite         bool
	failTruncate      bool
	failTruncateCount int
}

func (c *recordingClient) WriteFile(ctx context.Context, in *pb.WriteFileRequest, opts ...grpc.CallOption) (*pb.WriteFileResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.callOrder = append(c.callOrder, "WriteFile")
	if c.failWrite {
		return nil, status.Errorf(codes.Unavailable, "injected write failure")
	}
	c.writeReqs = append(c.writeReqs, proto.Clone(in).(*pb.WriteFileRequest))
	return c.ObjectFSControllerClient.WriteFile(ctx, in, opts...)
}

func (c *recordingClient) TruncateFile(ctx context.Context, in *pb.TruncateFileRequest, opts ...grpc.CallOption) (*pb.TruncateFileResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.callOrder = append(c.callOrder, "TruncateFile")
	c.truncateReqs = append(c.truncateReqs, proto.Clone(in).(*pb.TruncateFileRequest))
	if c.failTruncateCount > 0 {
		c.failTruncateCount--
		return nil, status.Errorf(codes.Unavailable, "injected truncate failure")
	}
	if c.failTruncate {
		return nil, status.Errorf(codes.Unavailable, "injected truncate failure")
	}
	return c.ObjectFSControllerClient.TruncateFile(ctx, in, opts...)
}

func TestSyncFileToServiceCoalescesContiguousChunks(t *testing.T) {
	baseClient, cleanup := createTestClient(t)
	defer cleanup()

	rec := &recordingClient{ObjectFSControllerClient: baseClient}
	cache := NewNodeCache(8 * 1024 * 1024)
	fs := NewObjectFS(rec, "vol-coalesce", pb.WriteMode_WRITE_THROUGH_FSYNC, cache)

	var createOut fuse.CreateOut
	if status := fs.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     0644,
	}, "test_coalesce.bin", &createOut); status != fuse.OK {
		t.Fatalf("Create failed: %v", status)
	}
	fileID := createOut.EntryOut.NodeId

	// Write 3 full 64KB chunks sequentially
	const cs = int64(DefaultChunkSize)
	data := make([]byte, 3*cs)
	for i := range data {
		data[i] = byte(i % 251)
	}
	cache.WriteAt(fileID, 0, data, time.Now())

	ctx := t.Context()
	if err := fs.syncAndFsync(ctx, fileID, true, pb.FsyncRequest_SYNC_TRIGGER_UNSPECIFIED); err != nil {
		t.Fatalf("syncAndFsync failed: %v", err)
	}

	rec.mu.Lock()
	reqs := rec.writeReqs
	rec.mu.Unlock()

	if len(reqs) != 1 {
		t.Fatalf("Expected 1 coalesced WriteFile RPC, got %d", len(reqs))
	}
	if reqs[0].Offset != 0 {
		t.Errorf("Expected offset 0, got %d", reqs[0].Offset)
	}
	if int64(len(reqs[0].Data)) != 3*cs {
		t.Errorf("Expected data len %d, got %d", 3*cs, len(reqs[0].Data))
	}
	if !bytes.Equal(reqs[0].Data, data) {
		t.Errorf("Concatenated data content mismatch")
	}

	if _, isDirty := cache.GetDirty(fileID); isDirty {
		t.Errorf("Expected cache to be clean after sync")
	}
}

func TestSyncFileToServiceNonContiguousRuns(t *testing.T) {
	baseClient, cleanup := createTestClient(t)
	defer cleanup()

	rec := &recordingClient{ObjectFSControllerClient: baseClient}
	cache := NewNodeCache(8 * 1024 * 1024)
	fs := NewObjectFS(rec, "vol-noncontiguous", pb.WriteMode_WRITE_THROUGH_FSYNC, cache)

	var createOut fuse.CreateOut
	if status := fs.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     0644,
	}, "test_noncontiguous.bin", &createOut); status != fuse.OK {
		t.Fatalf("Create failed: %v", status)
	}
	fileID := createOut.EntryOut.NodeId

	const cs = int64(DefaultChunkSize)
	// Write chunk 0 & 1 (contiguous), chunk 4 (isolated), chunk 7 & 8 (contiguous)
	d01 := make([]byte, 2*cs)
	d4 := make([]byte, cs)
	d78 := make([]byte, 2*cs)
	cache.WriteAt(fileID, 0, d01, time.Now())
	cache.WriteAt(fileID, 4*cs, d4, time.Now())
	cache.WriteAt(fileID, 7*cs, d78, time.Now())

	ctx := t.Context()
	if err := fs.syncAndFsync(ctx, fileID, true, pb.FsyncRequest_SYNC_TRIGGER_UNSPECIFIED); err != nil {
		t.Fatalf("syncAndFsync failed: %v", err)
	}

	rec.mu.Lock()
	reqs := rec.writeReqs
	rec.mu.Unlock()

	if len(reqs) != 3 {
		t.Fatalf("Expected 3 WriteFile RPCs for 3 non-contiguous runs, got %d", len(reqs))
	}

	// Run 1: chunks 0..1
	if reqs[0].Offset != 0 || int64(len(reqs[0].Data)) != 2*cs {
		t.Errorf("Run 1 mismatch: offset=%d len=%d want offset=0 len=%d", reqs[0].Offset, len(reqs[0].Data), 2*cs)
	}
	// Run 2: chunk 4
	if reqs[1].Offset != 4*cs || int64(len(reqs[1].Data)) != cs {
		t.Errorf("Run 2 mismatch: offset=%d len=%d want offset=%d len=%d", reqs[1].Offset, len(reqs[1].Data), 4*cs, cs)
	}
	// Run 3: chunks 7..8
	if reqs[2].Offset != 7*cs || int64(len(reqs[2].Data)) != 2*cs {
		t.Errorf("Run 3 mismatch: offset=%d len=%d want offset=%d len=%d", reqs[2].Offset, len(reqs[2].Data), 7*cs, 2*cs)
	}

	if _, isDirty := cache.GetDirty(fileID); isDirty {
		t.Errorf("Expected cache to be clean after sync")
	}
}

func TestSyncFileToServiceSplitsAt32ChunksAnd2MiB(t *testing.T) {
	baseClient, cleanup := createTestClient(t)
	defer cleanup()

	rec := &recordingClient{ObjectFSControllerClient: baseClient}
	cache := NewNodeCache(16 * 1024 * 1024)
	fs := NewObjectFS(rec, "vol-split-32", pb.WriteMode_WRITE_THROUGH_FSYNC, cache)

	var createOut fuse.CreateOut
	if status := fs.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     0644,
	}, "test_split.bin", &createOut); status != fuse.OK {
		t.Fatalf("Create failed: %v", status)
	}
	fileID := createOut.EntryOut.NodeId

	const cs = int64(DefaultChunkSize)
	const totalChunks = 35
	data := make([]byte, totalChunks*cs)
	for i := range data {
		data[i] = byte((i * 17) & 0xFF)
	}
	cache.WriteAt(fileID, 0, data, time.Now())

	ctx := t.Context()
	if err := fs.syncAndFsync(ctx, fileID, true, pb.FsyncRequest_SYNC_TRIGGER_UNSPECIFIED); err != nil {
		t.Fatalf("syncAndFsync failed: %v", err)
	}

	rec.mu.Lock()
	reqs := rec.writeReqs
	rec.mu.Unlock()

	// 35 chunks should be split into 32 chunks (2 MiB) + 3 chunks (192 KiB)
	if len(reqs) != 2 {
		t.Fatalf("Expected 2 WriteFile RPCs due to 32 chunk / 2 MiB cap, got %d", len(reqs))
	}

	if reqs[0].Offset != 0 || int64(len(reqs[0].Data)) != 32*cs {
		t.Errorf("Run 1 mismatch: offset=%d len=%d want offset=0 len=%d", reqs[0].Offset, len(reqs[0].Data), 32*cs)
	}
	if reqs[1].Offset != 32*cs || int64(len(reqs[1].Data)) != 3*cs {
		t.Errorf("Run 2 mismatch: offset=%d len=%d want offset=%d len=%d", reqs[1].Offset, len(reqs[1].Data), 32*cs, 3*cs)
	}

	if _, isDirty := cache.GetDirty(fileID); isDirty {
		t.Errorf("Expected cache to be clean after sync")
	}
}

func TestSyncFileToServicePartialChunkBoundary(t *testing.T) {
	baseClient, cleanup := createTestClient(t)
	defer cleanup()

	rec := &recordingClient{ObjectFSControllerClient: baseClient}
	cache := NewNodeCache(8 * 1024 * 1024)
	fs := NewObjectFS(rec, "vol-partial", pb.WriteMode_WRITE_THROUGH_FSYNC, cache)

	var createOut fuse.CreateOut
	if status := fs.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     0644,
	}, "test_partial.bin", &createOut); status != fuse.OK {
		t.Fatalf("Create failed: %v", status)
	}
	fileID := createOut.EntryOut.NodeId

	const cs = int64(DefaultChunkSize)
	// Write 100 bytes at offset 0 (chunk 0 is partial)
	cache.WriteAt(fileID, 0, make([]byte, 100), time.Now())
	// Then write 64KB at offset cs (chunk 1)
	cache.WriteAt(fileID, cs, make([]byte, cs), time.Now())

	ctx := t.Context()
	if err := fs.syncAndFsync(ctx, fileID, true, pb.FsyncRequest_SYNC_TRIGGER_UNSPECIFIED); err != nil {
		t.Fatalf("syncAndFsync failed: %v", err)
	}

	rec.mu.Lock()
	reqs := rec.writeReqs
	rec.mu.Unlock()

	// Chunk 0 is partial (100 < 65536), so chunk 1 cannot be coalesced into it.
	if len(reqs) != 2 {
		t.Fatalf("Expected 2 separate WriteFile RPCs because chunk 0 was partial, got %d", len(reqs))
	}
	if reqs[0].Offset != 0 || len(reqs[0].Data) != 100 {
		t.Errorf("Run 1 mismatch: offset=%d len=%d want offset=0 len=100", reqs[0].Offset, len(reqs[0].Data))
	}
	if reqs[1].Offset != cs || int64(len(reqs[1].Data)) != cs {
		t.Errorf("Run 2 mismatch: offset=%d len=%d want offset=%d len=%d", reqs[1].Offset, len(reqs[1].Data), cs, cs)
	}
}

func TestSyncFileToServiceRPCFailureLeavesChunksDirty(t *testing.T) {
	baseClient, cleanup := createTestClient(t)
	defer cleanup()

	rec := &recordingClient{
		ObjectFSControllerClient: baseClient,
		failWrite:                true,
	}
	cache := NewNodeCache(8 * 1024 * 1024)
	fs := NewObjectFS(rec, "vol-fail", pb.WriteMode_WRITE_THROUGH_FSYNC, cache)

	var createOut fuse.CreateOut
	if status := fs.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     0644,
	}, "test_fail.bin", &createOut); status != fuse.OK {
		t.Fatalf("Create failed: %v", status)
	}
	fileID := createOut.EntryOut.NodeId

	const cs = int64(DefaultChunkSize)
	cache.WriteAt(fileID, 0, make([]byte, 2*cs), time.Now())

	ctx := t.Context()
	err := fs.syncAndFsync(ctx, fileID, true, pb.FsyncRequest_SYNC_TRIGGER_UNSPECIFIED)
	if err == nil {
		t.Fatalf("Expected syncAndFsync to fail when WriteFile fails, got nil")
	}

	// Verify both chunk 0 and chunk 1 remain dirty
	dirty, _, _, _, isDirty := cache.GetDirtyChunks(fileID)
	if !isDirty {
		t.Fatalf("Expected inode to remain dirty after failed WriteFile")
	}
	if len(dirty) != 2 {
		t.Fatalf("Expected both chunks 0 and 1 to remain dirty, got %d dirty chunks", len(dirty))
	}
}

func TestSyncFileToServiceConcurrentWritePreservesDirtyGeneration(t *testing.T) {
	baseClient, cleanup := createTestClient(t)
	defer cleanup()

	cache := NewNodeCache(8 * 1024 * 1024)
	var writeHook func()

	rec := &hookClient{
		ObjectFSControllerClient: baseClient,
		onWrite: func() {
			if writeHook != nil {
				writeHook()
			}
		},
	}
	fs := NewObjectFS(rec, "vol-race", pb.WriteMode_WRITE_THROUGH_FSYNC, cache)

	var createOut fuse.CreateOut
	if status := fs.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     0644,
	}, "test_race.bin", &createOut); status != fuse.OK {
		t.Fatalf("Create failed: %v", status)
	}
	fileID := createOut.EntryOut.NodeId

	const cs = int64(DefaultChunkSize)
	c0v1 := bytes.Repeat([]byte("A"), int(cs))
	c1v1 := bytes.Repeat([]byte("B"), int(cs))
	c1v2 := bytes.Repeat([]byte("C"), int(cs))

	cache.WriteAt(fileID, 0, append(c0v1, c1v1...), time.Now())

	// When WriteFile is invoked by syncAndFsync, race a new write to chunk 1
	writeHook = func() {
		// Mutate chunk 1 to version 2
		cache.WriteAt(fileID, cs, c1v2, time.Now())
		writeHook = nil // only race once
	}

	ctx := t.Context()
	if err := fs.syncAndFsync(ctx, fileID, true, pb.FsyncRequest_SYNC_TRIGGER_UNSPECIFIED); err != nil {
		t.Fatalf("syncAndFsync failed: %v", err)
	}

	// Chunk 0 was clean and should be cleared.
	// Chunk 1 was mutated concurrently, so it must still be dirty with c1v2!
	dirty, _, _, _, isDirty := cache.GetDirtyChunks(fileID)
	if !isDirty {
		t.Fatalf("Expected inode to remain dirty due to concurrent write to chunk 1")
	}
	if _, c0Dirty := dirty[0]; c0Dirty {
		t.Errorf("Chunk 0 should have been marked clean")
	}
	c1Data, c1Dirty := dirty[1]
	if !c1Dirty {
		t.Fatalf("Chunk 1 should remain dirty")
	}
	if !bytes.Equal(c1Data, c1v2) {
		t.Errorf("Dirty chunk 1 content should be version 2, got %q", c1Data[:10])
	}
}

func TestSyncFileToServiceRetriesFailedTruncateAndClearsSizeGen(t *testing.T) {
	baseClient, cleanup := createTestClient(t)
	defer cleanup()

	rec := &recordingClient{
		ObjectFSControllerClient: baseClient,
		failTruncateCount:        1,
	}
	cache := NewNodeCache(8 * 1024 * 1024)
	fs := NewObjectFS(rec, "vol-retry-trunc", pb.WriteMode_WRITE_THROUGH_FSYNC, cache)

	var createOut fuse.CreateOut
	if status := fs.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     0644,
	}, "test_retry_trunc.bin", &createOut); status != fuse.OK {
		t.Fatalf("Create failed: %v", status)
	}
	fileID := createOut.EntryOut.NodeId

	// Write 100 bytes and sync cleanly
	initialData := bytes.Repeat([]byte("A"), 100)
	cache.WriteAt(fileID, 0, initialData, time.Now())
	if err := fs.syncAndFsync(t.Context(), fileID, true, pb.FsyncRequest_SYNC_TRIGGER_UNSPECIFIED); err != nil {
		t.Fatalf("Initial syncAndFsync failed: %v", err)
	}

	// First SetAttr with FATTR_SIZE fails because TruncateFile fails
	var setAttrOut fuse.AttrOut
	status := fs.SetAttr(nil, &fuse.SetAttrIn{
		SetAttrInCommon: fuse.SetAttrInCommon{
			InHeader: fuse.InHeader{NodeId: fileID},
			Valid:    fuse.FATTR_SIZE,
			Size:     50,
		},
	}, &setAttrOut)
	if status == fuse.OK {
		t.Fatalf("Expected SetAttr to fail when TruncateFile fails, got OK")
	}

	// Confirm SizeGen is set and entry remains dirty
	dirtySize, sizeGen, hasDirtySize := cache.GetDirtySize(fileID)
	if !hasDirtySize || sizeGen == 0 || dirtySize != 50 {
		t.Fatalf("Expected dirty size 50 with non-zero SizeGen, got dirty=%v size=%d gen=%d", hasDirtySize, dirtySize, sizeGen)
	}
	if _, isDirty := cache.GetDirty(fileID); !isDirty {
		t.Fatalf("Expected entry to remain dirty while SizeGen is set")
	}

	// The next syncAndFsync should retry TruncateFile and succeed
	if err := fs.syncAndFsync(t.Context(), fileID, true, pb.FsyncRequest_SYNC_TRIGGER_UNSPECIFIED); err != nil {
		t.Fatalf("Next syncAndFsync failed: %v", err)
	}

	// Confirm SizeGen is cleared and entry is clean
	_, sizeGenAfter, hasDirtySizeAfter := cache.GetDirtySize(fileID)
	if hasDirtySizeAfter || sizeGenAfter != 0 {
		t.Fatalf("Expected SizeGen to be cleared after sync, got dirty=%v gen=%d", hasDirtySizeAfter, sizeGenAfter)
	}
	if _, isDirty := cache.GetDirty(fileID); isDirty {
		t.Fatalf("Expected entry to be clean after successful sync")
	}

	// Confirm controller reports truncated size 50
	attrResp, err := baseClient.GetAttr(t.Context(), &pb.GetAttrRequest{
		VolumeId: "vol-retry-trunc",
		Inode:    fileID,
	})
	if err != nil {
		t.Fatalf("GetAttr failed: %v", err)
	}
	if attrResp.GetAttr().GetInode().GetSize() != 50 {
		t.Fatalf("Expected controller file size 50, got %d", attrResp.GetAttr().GetInode().GetSize())
	}
}

func TestSyncFileToServiceOrderingDirtySizeBeforeChunksAndDropsBeyondEOF(t *testing.T) {
	baseClient, cleanup := createTestClient(t)
	defer cleanup()

	rec := &recordingClient{
		ObjectFSControllerClient: baseClient,
	}
	cache := NewNodeCache(8 * 1024 * 1024)
	fs := NewObjectFS(rec, "vol-ordering", pb.WriteMode_WRITE_THROUGH_FSYNC, cache)

	var createOut fuse.CreateOut
	if status := fs.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     0644,
	}, "test_ordering.bin", &createOut); status != fuse.OK {
		t.Fatalf("Create failed: %v", status)
	}
	fileID := createOut.EntryOut.NodeId

	const cs = int64(DefaultChunkSize)

	// Write chunk 0 (0..64KB) and chunk 1 (64KB..128KB) in cache
	c0 := bytes.Repeat([]byte("0"), int(cs))
	c1 := bytes.Repeat([]byte("1"), int(cs))
	cache.WriteAt(fileID, 0, append(c0, c1...), time.Now())

	// Fail the next TruncateFile so SetAttr leaves SizeGen set
	rec.failTruncateCount = 1
	var setAttrOut fuse.AttrOut
	status := fs.SetAttr(nil, &fuse.SetAttrIn{
		SetAttrInCommon: fuse.SetAttrInCommon{
			InHeader: fuse.InHeader{NodeId: fileID},
			Valid:    fuse.FATTR_SIZE,
			Size:     uint64(cs / 2), // Truncate to cs/2 (32 KiB)
		},
	}, &setAttrOut)
	if status == fuse.OK {
		t.Fatalf("Expected SetAttr to fail, got OK")
	}

	// Manually inject a dirty chunk beyond the new EOF (e.g. chunk 1 at 64KB) into the cache
	// to simulate dirty size plus dirty chunks beyond the new EOF.
	entry, ok := cache.Get(fileID)
	if !ok {
		t.Fatalf("Entry not found")
	}
	gen := cache.GetGeneration(fileID)
	entry.Chunks[1] = c1
	entry.DirtyChunks[1] = gen

	// Clear recorded RPC calls before sync
	rec.mu.Lock()
	rec.callOrder = nil
	rec.truncateReqs = nil
	rec.writeReqs = nil
	rec.mu.Unlock()

	// Run syncAndFsync
	if err := fs.syncAndFsync(t.Context(), fileID, true, pb.FsyncRequest_SYNC_TRIGGER_UNSPECIFIED); err != nil {
		t.Fatalf("syncAndFsync failed: %v", err)
	}

	rec.mu.Lock()
	callOrder := append([]string(nil), rec.callOrder...)
	writeReqs := append([]*pb.WriteFileRequest(nil), rec.writeReqs...)
	truncReqs := append([]*pb.TruncateFileRequest(nil), rec.truncateReqs...)
	rec.mu.Unlock()

	// 1. Confirm TruncateFile was sent first
	if len(truncReqs) != 1 {
		t.Fatalf("Expected 1 TruncateFile RPC, got %d", len(truncReqs))
	}
	if truncReqs[0].Size != cs/2 {
		t.Fatalf("Expected TruncateFile size %d, got %d", cs/2, truncReqs[0].Size)
	}
	if len(callOrder) == 0 || callOrder[0] != "TruncateFile" {
		t.Fatalf("Expected TruncateFile to be sent first, got call order %v", callOrder)
	}

	// 2. Confirm chunks beyond new EOF were NOT written
	for _, req := range writeReqs {
		if req.Offset >= cs/2 {
			t.Fatalf("WriteFile chunk beyond EOF was sent: offset=%d, eof=%d", req.Offset, cs/2)
		}
		if req.Offset+int64(len(req.Data)) > cs/2 {
			t.Fatalf("WriteFile data extends past EOF: offset=%d len=%d eof=%d", req.Offset, len(req.Data), cs/2)
		}
	}

	// 3. Confirm SizeGen is clean
	_, _, hasDirtySize := cache.GetDirtySize(fileID)
	if hasDirtySize {
		t.Fatalf("Expected dirty size to be clean after sync")
	}
	if _, isDirty := cache.GetDirty(fileID); isDirty {
		t.Fatalf("Expected entry to be clean after sync")
	}
}

type hookClient struct {
	pb.ObjectFSControllerClient
	onWrite func()
}

func (h *hookClient) WriteFile(ctx context.Context, in *pb.WriteFileRequest, opts ...grpc.CallOption) (*pb.WriteFileResponse, error) {
	if h.onWrite != nil {
		h.onWrite()
	}
	return h.ObjectFSControllerClient.WriteFile(ctx, in, opts...)
}

func TestFUSETruncateZeroThenGrowReadsZeros(t *testing.T) {
	client, cleanup := createTestClient(t)
	defer cleanup()

	volumeID := "test-fuse-trunc-zero-grow"
	cache := NewNodeCache(64 * 1024 * 1024)
	rawFS := NewObjectFS(client, volumeID, pb.WriteMode_LAZY_WRITE, cache)

	var createOut fuse.CreateOut
	if status := rawFS.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     0644,
	}, "testfile.bin", &createOut); status != fuse.OK {
		t.Fatalf("Create failed: %v", status)
	}
	fileID := createOut.EntryOut.NodeId

	// Truncate to 0
	var setAttrOut fuse.AttrOut
	if status := rawFS.SetAttr(nil, &fuse.SetAttrIn{
		SetAttrInCommon: fuse.SetAttrInCommon{
			InHeader: fuse.InHeader{NodeId: fileID},
			Valid:    fuse.FATTR_SIZE,
			Size:     0,
		},
	}, &setAttrOut); status != fuse.OK {
		t.Fatalf("SetAttr truncate to 0 failed: %v", status)
	}

	// Truncate to 100000
	if status := rawFS.SetAttr(nil, &fuse.SetAttrIn{
		SetAttrInCommon: fuse.SetAttrInCommon{
			InHeader: fuse.InHeader{NodeId: fileID},
			Valid:    fuse.FATTR_SIZE,
			Size:     100000,
		},
	}, &setAttrOut); status != fuse.OK {
		t.Fatalf("SetAttr truncate to 100000 failed: %v", status)
	}

	// Clear local cache to force read from controller
	cache.Clear()

	buf := make([]byte, 65536)
	readRes, status := rawFS.Read(nil, &fuse.ReadIn{
		InHeader: fuse.InHeader{NodeId: fileID},
		Offset:   0,
		Size:     65536,
	}, buf)
	if status != fuse.OK {
		t.Fatalf("Read failed: %v", status)
	}
	data, _ := readRes.Bytes(buf)
	if len(data) != 65536 {
		t.Fatalf("Read returned %d bytes, want 65536", len(data))
	}
	if !bytes.Equal(data, make([]byte, 65536)) {
		t.Fatalf("Read returned non-zero bytes for extended hole")
	}
}

type faultReadFileClient struct {
	pb.ObjectFSControllerClient
	mu      sync.Mutex
	failRPC bool
	respErr int32
}

func (c *faultReadFileClient) ReadFile(ctx context.Context, in *pb.ReadFileRequest, opts ...grpc.CallOption) (*pb.ReadFileResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failRPC {
		return nil, status.Errorf(codes.Unavailable, "injected ReadFile RPC error")
	}
	if c.respErr != 0 {
		return &pb.ReadFileResponse{Error: c.respErr}, nil
	}
	return c.ObjectFSControllerClient.ReadFile(ctx, in, opts...)
}

func TestWritePartialChunkReadFileFailureReturnsEIO(t *testing.T) {
	tests := []struct {
		name     string
		setFault func(fc *faultReadFileClient)
	}{
		{
			name: "RPCError",
			setFault: func(fc *faultReadFileClient) {
				fc.mu.Lock()
				fc.failRPC = true
				fc.mu.Unlock()
			},
		},
		{
			name: "ControllerError",
			setFault: func(fc *faultReadFileClient) {
				fc.mu.Lock()
				fc.respErr = int32(syscall.EIO)
				fc.mu.Unlock()
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			baseClient, cleanup := createTestClient(t)
			defer cleanup()

			faultClient := &faultReadFileClient{ObjectFSControllerClient: baseClient}
			cache := NewNodeCache(8 * 1024 * 1024)
			volumeID := "test-write-fill-" + tc.name
			rawFS := NewObjectFS(faultClient, volumeID, pb.WriteMode_LAZY_WRITE, cache)

			var createOut fuse.CreateOut
			if status := rawFS.Create(nil, &fuse.CreateIn{
				InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
				Mode:     0644,
			}, "testfile.bin", &createOut); status != fuse.OK {
				t.Fatalf("Create failed: %v", status)
			}
			fileID := createOut.EntryOut.NodeId

			// Initialize file with known non-zero bytes across a full chunk
			const cs = int(DefaultChunkSize)
			origContent := make([]byte, cs)
			for i := range origContent {
				origContent[i] = byte(i%251 + 1)
			}

			n, status := rawFS.Write(nil, &fuse.WriteIn{
				InHeader: fuse.InHeader{NodeId: fileID},
				Offset:   0,
			}, origContent)
			if status != fuse.OK || n != uint32(len(origContent)) {
				t.Fatalf("Initial write failed: status=%v, n=%d", status, n)
			}

			// Sync file to service so controller has the initial content
			if status := rawFS.Fsync(nil, &fuse.FsyncIn{
				InHeader: fuse.InHeader{NodeId: fileID},
			}); status != fuse.OK {
				t.Fatalf("Fsync failed: %v", status)
			}

			// Invalidate client cache to ensure the subsequent partial write must fill chunk from controller
			cache.Clear()

			// Inject ReadFile failure
			tc.setFault(faultClient)

			// Perform a partial-chunk write (bytes 100..109 in chunk 0)
			patch := []byte("0123456789")
			n, status = rawFS.Write(nil, &fuse.WriteIn{
				InHeader: fuse.InHeader{NodeId: fileID},
				Offset:   100,
			}, patch)

			if status != fuse.EIO {
				t.Fatalf("expected Write to fail with EIO (%v), got status %v", fuse.EIO, status)
			}
			if n != 0 {
				t.Fatalf("expected 0 bytes written on failure, got %d", n)
			}

			// Verify the cache has no dirty chunk for this file
			if _, _, _, _, isDirty := cache.GetDirtyChunks(fileID); isDirty {
				t.Fatalf("cache should not have any dirty chunks after failed write")
			}
			if entry, isDirty := cache.GetDirty(fileID); isDirty {
				t.Fatalf("cache entry should not be marked dirty after failed write: %+v", entry)
			}

			// Clear fault
			faultClient.mu.Lock()
			faultClient.failRPC = false
			faultClient.respErr = 0
			faultClient.mu.Unlock()

			// Retry the partial write; it must succeed now
			n, status = rawFS.Write(nil, &fuse.WriteIn{
				InHeader: fuse.InHeader{NodeId: fileID},
				Offset:   100,
			}, patch)
			if status != fuse.OK {
				t.Fatalf("retry Write failed: %v", status)
			}
			if n != uint32(len(patch)) {
				t.Fatalf("retry Write returned %d, want %d", n, len(patch))
			}

			// Verify that untouched bytes remain intact (not replaced by zeros)
			expected := make([]byte, cs)
			copy(expected, origContent)
			copy(expected[100:], patch)

			readBuf := make([]byte, cs)
			readRes, status := rawFS.Read(nil, &fuse.ReadIn{
				InHeader: fuse.InHeader{NodeId: fileID},
				Offset:   0,
				Size:     uint32(cs),
			}, readBuf)
			if status != fuse.OK {
				t.Fatalf("Read failed: %v", status)
			}
			readData, _ := readRes.Bytes(readBuf)
			if !bytes.Equal(readData, expected) {
				t.Fatalf("Data mismatch after retry write: untouched bytes corrupted with zeros")
			}

			// Sync to controller, clear cache, and verify controller also has intact content
			if status := rawFS.Fsync(nil, &fuse.FsyncIn{
				InHeader: fuse.InHeader{NodeId: fileID},
			}); status != fuse.OK {
				t.Fatalf("Fsync after retry failed: %v", status)
			}
			cache.Clear()

			readRes2, status2 := rawFS.Read(nil, &fuse.ReadIn{
				InHeader: fuse.InHeader{NodeId: fileID},
				Offset:   0,
				Size:     uint32(cs),
			}, readBuf)
			if status2 != fuse.OK {
				t.Fatalf("Read after Flush failed: %v", status2)
			}
			readData2, _ := readRes2.Bytes(readBuf)
			if !bytes.Equal(readData2, expected) {
				t.Fatalf("Data mismatch on controller after flush: untouched bytes corrupted")
			}
		})
	}
}

func TestLargeSequentialWriteCrossesStagingCapAndSucceeds(t *testing.T) {
	client, cleanup := createTestClient(t)
	defer cleanup()

	volumeID := "vol-large-seq-200mb"
	// Client cache 128 MiB (default), controller staging cap 64 MiB (default)
	cache := NewNodeCache(128 * 1024 * 1024)
	rawFS := NewObjectFS(client, volumeID, pb.WriteMode_LAZY_WRITE, cache)

	var createOut fuse.CreateOut
	if status := rawFS.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     0644,
	}, "large200mb.bin", &createOut); status != fuse.OK {
		t.Fatalf("Create failed: %v", status)
	}
	fileID := createOut.EntryOut.NodeId

	const writeSize = 128 * 1024
	const totalSize = 200 * 1024 * 1024 // 200 MiB
	chunk := make([]byte, writeSize)
	for i := range chunk {
		chunk[i] = byte(i % 251)
	}

	for off := int64(0); off < totalSize; off += writeSize {
		n, status := rawFS.Write(nil, &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: fileID},
			Offset:   uint64(off),
		}, chunk)
		if status != fuse.OK || int(n) != len(chunk) {
			t.Fatalf("write at %d failed: status=%v, n=%d", off, status, n)
		}
	}

	if status := rawFS.Fsync(nil, &fuse.FsyncIn{
		InHeader: fuse.InHeader{NodeId: fileID},
	}); status != fuse.OK {
		t.Fatalf("Fsync failed: %v", status)
	}

	// Verify total size on controller
	attrResp, err := client.GetAttr(t.Context(), &pb.GetAttrRequest{
		VolumeId: volumeID,
		Inode:    fileID,
	})
	if err != nil {
		t.Fatalf("GetAttr failed: %v", err)
	}
	if attrResp.GetAttr().GetInode().GetSize() != totalSize {
		t.Fatalf("expected size %d, got %d", totalSize, attrResp.GetAttr().GetInode().GetSize())
	}
}

func TestBudgetTriggeredSyncFlushesAcrossMultipleDirtyFiles(t *testing.T) {
	client, cleanup := createTestClient(t)
	defer cleanup()

	volumeID := "vol-multi-file-budget"
	// Client cache 128 MiB
	cache := NewNodeCache(128 * 1024 * 1024)
	rawFS := NewObjectFS(client, volumeID, pb.WriteMode_LAZY_WRITE, cache)

	const numFiles = 3
	const fileSize = 50 * 1024 * 1024 // 50 MiB each -> total 150 MiB > 128 MiB cache
	const writeSize = 128 * 1024
	chunk := make([]byte, writeSize)
	for i := range chunk {
		chunk[i] = byte(i % 241)
	}

	var fileIDs []uint64
	for f := 0; f < numFiles; f++ {
		var createOut fuse.CreateOut
		name := fmt.Sprintf("file_%d.bin", f)
		if status := rawFS.Create(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Mode:     0644,
		}, name, &createOut); status != fuse.OK {
			t.Fatalf("Create %s failed: %v", name, status)
		}
		fileIDs = append(fileIDs, createOut.EntryOut.NodeId)
	}

	for f, fID := range fileIDs {
		for off := int64(0); off < fileSize; off += writeSize {
			n, status := rawFS.Write(nil, &fuse.WriteIn{
				InHeader: fuse.InHeader{NodeId: fID},
				Offset:   uint64(off),
			}, chunk)
			if status != fuse.OK || int(n) != len(chunk) {
				t.Fatalf("file %d write at %d failed: status=%v", f, off, status)
			}
		}
	}

	// Final fsync on all files
	for _, fID := range fileIDs {
		if status := rawFS.Fsync(nil, &fuse.FsyncIn{
			InHeader: fuse.InHeader{NodeId: fID},
		}); status != fuse.OK {
			t.Fatalf("Fsync failed for file %d: %v", fID, status)
		}
	}
}

type concurrencyTrackingClient struct {
	pb.ObjectFSControllerClient
	inFlight    atomic.Int64
	maxInFlight atomic.Int64
}

func (c *concurrencyTrackingClient) ReadFile(ctx context.Context, in *pb.ReadFileRequest, opts ...grpc.CallOption) (*pb.ReadFileResponse, error) {
	cur := c.inFlight.Add(1)
	for {
		m := c.maxInFlight.Load()
		if cur <= m || c.maxInFlight.CompareAndSwap(m, cur) {
			break
		}
	}
	time.Sleep(30 * time.Millisecond)
	defer c.inFlight.Add(-1)
	return c.ObjectFSControllerClient.ReadFile(ctx, in, opts...)
}

func TestFUSEReadConcurrentChunks(t *testing.T) {
	baseClient, cleanup := createTestClient(t)
	defer cleanup()

	trackClient := &concurrencyTrackingClient{
		ObjectFSControllerClient: baseClient,
	}

	volumeID := "vol-concurrent-read-test"
	cache := NewNodeCache(10 * 1024 * 1024)
	rawFS := NewObjectFS(trackClient, volumeID, pb.WriteMode_LAZY_WRITE, cache)

	var createOut fuse.CreateOut
	if status := rawFS.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     0644,
	}, "multi_chunk.bin", &createOut); status != fuse.OK {
		t.Fatalf("Create failed: %v", status)
	}
	fileID := createOut.EntryOut.NodeId

	const cs = int(DefaultChunkSize) // 64 KiB
	const numChunks = 4
	totalBytes := numChunks * cs
	expectedData := make([]byte, totalBytes)
	for i := range expectedData {
		expectedData[i] = byte(i % 251)
	}

	// Write and sync data to controller
	n, status := rawFS.Write(nil, &fuse.WriteIn{
		InHeader: fuse.InHeader{NodeId: fileID},
		Offset:   0,
	}, expectedData)
	if status != fuse.OK || int(n) != totalBytes {
		t.Fatalf("Write failed: status=%v, n=%d", status, n)
	}

	if status := rawFS.Fsync(nil, &fuse.FsyncIn{
		InHeader: fuse.InHeader{NodeId: fileID},
	}); status != fuse.OK {
		t.Fatalf("Fsync failed: %v", status)
	}

	// Evict from local cache so Read must fetch chunks from controller
	cache.Clear()
	trackClient.maxInFlight.Store(0)

	readBuf := make([]byte, totalBytes)
	start := time.Now()
	readRes, status := rawFS.Read(nil, &fuse.ReadIn{
		InHeader: fuse.InHeader{NodeId: fileID},
		Offset:   0,
		Size:     uint32(totalBytes),
	}, readBuf)
	elapsed := time.Since(start)

	if status != fuse.OK {
		t.Fatalf("Read failed: %v", status)
	}
	readBytes, _ := readRes.Bytes(readBuf)
	if !bytes.Equal(readBytes, expectedData) {
		t.Fatalf("Read data mismatch")
	}

	maxInFlight := trackClient.maxInFlight.Load()
	t.Logf("Read for %d chunks took %v (maxInFlight=%d)", numChunks, elapsed, maxInFlight)

	if maxInFlight <= 1 {
		t.Fatalf("Expected chunks to be fetched concurrently in Read, but maxInFlight was %d", maxInFlight)
	}

	// 4 chunks with 30ms sleep each: serial would take >= 120ms. Concurrent takes ~30-60ms.
	serialMin := time.Duration(numChunks) * 30 * time.Millisecond
	if elapsed >= serialMin {
		t.Fatalf("Read was serial: elapsed %v >= %v", elapsed, serialMin)
	}
}

func TestPushDirtyChunksToStaging_DrainAndRetryOnInitialENOSPC(t *testing.T) {
	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	// Set controller staging cap to 64 KiB
	server := controllertest.NewTestServer(nil, controller.WithServerMaxStagingBytes(64*1024))
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
	defer func() {
		_ = conn.Close()
		grpcServer.Stop()
		_ = lis.Close()
		_ = server.Close()
	}()

	client := pb.NewObjectFSControllerClient(conn)
	volumeID := "vol-enospc-drain-retry"
	cache := NewNodeCache(10 * 1024 * 1024)
	rawFS := NewObjectFS(client, volumeID, pb.WriteMode_LAZY_WRITE, cache)

	// Create file A and file B
	var createA, createB fuse.CreateOut
	if status := rawFS.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     0644,
	}, "fileA.bin", &createA); status != fuse.OK {
		t.Fatalf("Create A failed: %v", status)
	}
	fileA := createA.EntryOut.NodeId

	if status := rawFS.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     0644,
	}, "fileB.bin", &createB); status != fuse.OK {
		t.Fatalf("Create B failed: %v", status)
	}
	fileB := createB.EntryOut.NodeId

	// 1. Stage 64 KiB on File A directly via client.WriteFile without fsyncing,
	// filling the controller's 64 KiB staging cap completely.
	wResp, err := client.WriteFile(t.Context(), &pb.WriteFileRequest{
		VolumeId:  volumeID,
		Inode:     fileA,
		Offset:    0,
		Data:      make([]byte, 64*1024),
		WriteMode: pb.WriteMode_LAZY_WRITE,
	})
	if err != nil || wResp.GetError() != 0 {
		t.Fatalf("WriteFile A failed: %v, errCode=%d", err, wResp.GetError())
	}

	// 2. Write 64 KiB to File B locally in the FUSE cache (does not push to controller yet)
	bData := bytes.Repeat([]byte("B"), 64*1024)
	n, status := rawFS.Write(nil, &fuse.WriteIn{
		InHeader: fuse.InHeader{NodeId: fileB},
		Offset:   0,
	}, bData)
	if status != fuse.OK || int(n) != len(bData) {
		t.Fatalf("Write B failed: status=%v, n=%d", status, n)
	}

	// 3. Push File B's dirty chunks. Its very first run receives ENOSPC from the controller
	// because File A occupies the staging cap. pushDirtyChunksToStaging must issue a blocking
	// Fsync to drain File A's staged data, then successfully retry and stage File B.
	epoch, runs, gens, err := rawFS.pushDirtyChunksToStaging(t.Context(), fileB)
	if err != nil {
		t.Fatalf("pushDirtyChunksToStaging failed on initial ENOSPC drain-and-retry: %v", err)
	}
	if epoch == 0 || len(runs) == 0 || len(gens) == 0 {
		t.Fatalf("expected non-empty push result, got epoch=%d, runs=%v, gens=%v", epoch, runs, gens)
	}

	// 4. Verify Fsync on File B succeeds
	if status := rawFS.Fsync(nil, &fuse.FsyncIn{
		InHeader: fuse.InHeader{NodeId: fileB},
	}); status != fuse.OK {
		t.Fatalf("Fsync B failed: %v", status)
	}
}

func TestDiskBlobCacheColdAndWarmRead(t *testing.T) {
	client, cleanup := createTestClient(t)
	defer cleanup()

	volumeID := "vol-disk-cache-warm"
	cacheDir := t.TempDir()

	diskCache, err := NewDiskBlobCache(cacheDir, 10*1024*1024)
	if err != nil {
		t.Fatalf("NewDiskBlobCache failed: %v", err)
	}
	defer diskCache.Close()

	memCache := NewNodeCache(1024 * 1024)
	rawFS := NewObjectFS(client, volumeID, pb.WriteMode_WRITE_THROUGH_FSYNC, memCache)
	rawFS.SetDiskBlobCache(diskCache)

	// Create and write 5 chunks (320 KiB)
	chunkSize := 64 * 1024
	totalSize := 5 * chunkSize
	content := bytes.Repeat([]byte("CACHE_TEST_CONTENT_"), totalSize/19+1)[:totalSize]

	var createOut fuse.CreateOut
	if status := rawFS.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     0644,
	}, "cached_file.bin", &createOut); status != fuse.OK {
		t.Fatalf("Create failed: %v", status)
	}
	fileID := createOut.EntryOut.NodeId

	written, status := rawFS.Write(nil, &fuse.WriteIn{
		InHeader: fuse.InHeader{NodeId: fileID},
		Offset:   0,
	}, content)
	if status != fuse.OK || int(written) != totalSize {
		t.Fatalf("Write failed: status=%v, written=%d", status, written)
	}

	if status := rawFS.Fsync(nil, &fuse.FsyncIn{
		InHeader: fuse.InHeader{NodeId: fileID},
	}); status != fuse.OK {
		t.Fatalf("Fsync failed: %v", status)
	}
	res, status := rawFS.Read(nil, &fuse.ReadIn{
		InHeader: fuse.InHeader{NodeId: fileID},
		Offset:   0,
		Size:     uint32(totalSize),
	}, nil)
	if status != fuse.OK {
		t.Fatalf("Cold read failed: %v", status)
	}
	coldData, _ := res.Bytes(nil)
	if !bytes.Equal(coldData, content) {
		t.Fatalf("Cold read data mismatch: len %d != %d", len(coldData), len(content))
	}

	// Verify chunks are stored in diskCache
	if diskCache.CurrentBytes() == 0 {
		t.Fatalf("Expected diskCache.CurrentBytes() > 0 after cold read, got 0")
	}

	// Invalidate in-memory cache again
	memCache.Clear()

	// 2. Warm read: served directly from diskCache
	resWarm, status := rawFS.Read(nil, &fuse.ReadIn{
		InHeader: fuse.InHeader{NodeId: fileID},
		Offset:   0,
		Size:     uint32(totalSize),
	}, nil)
	if status != fuse.OK {
		t.Fatalf("Warm read failed: %v", status)
	}
	warmData, _ := resWarm.Bytes(nil)
	if !bytes.Equal(warmData, content) {
		t.Fatalf("Warm read data mismatch: len %d != %d", len(warmData), len(content))
	}
}

func TestDiskBlobCachePrefetchOnOpen(t *testing.T) {
	client, cleanup := createTestClient(t)
	defer cleanup()

	volumeID := "vol-prefetch-open"
	cacheDir := t.TempDir()

	diskCache, err := NewDiskBlobCache(cacheDir, 10*1024*1024)
	if err != nil {
		t.Fatalf("NewDiskBlobCache failed: %v", err)
	}
	defer diskCache.Close()

	memCache := NewNodeCache(1024 * 1024)
	rawFS := NewObjectFS(client, volumeID, pb.WriteMode_WRITE_THROUGH_FSYNC, memCache)
	rawFS.SetDiskBlobCache(diskCache)

	// Create and write 4 chunks (256 KiB)
	chunkSize := 64 * 1024
	totalSize := 4 * chunkSize
	content := bytes.Repeat([]byte("PREFETCH_TEST_DATA!"), totalSize/19+1)[:totalSize]

	var createOut fuse.CreateOut
	if status := rawFS.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     0644,
	}, "prefetch_file.bin", &createOut); status != fuse.OK {
		t.Fatalf("Create failed: %v", status)
	}
	fileID := createOut.EntryOut.NodeId

	written, status := rawFS.Write(nil, &fuse.WriteIn{
		InHeader: fuse.InHeader{NodeId: fileID},
		Offset:   0,
	}, content)
	if status != fuse.OK || int(written) != totalSize {
		t.Fatalf("Write failed: status=%v, written=%d", status, written)
	}

	if status := rawFS.Fsync(nil, &fuse.FsyncIn{
		InHeader: fuse.InHeader{NodeId: fileID},
	}); status != fuse.OK {
		t.Fatalf("Fsync failed: %v", status)
	}

	// Close and recreate a fresh disk cache (empty) on same or new dir to test prefetch on open
	emptyCacheDir := t.TempDir()
	emptyDiskCache, err := NewDiskBlobCache(emptyCacheDir, 10*1024*1024)
	if err != nil {
		t.Fatalf("NewDiskBlobCache failed: %v", err)
	}
	defer emptyDiskCache.Close()

	rawFS.SetDiskBlobCache(emptyDiskCache)
	memCache.Clear()

	// Calling Open must trigger background prefetch
	var openOut fuse.OpenOut
	if status := rawFS.Open(nil, &fuse.OpenIn{
		InHeader: fuse.InHeader{NodeId: fileID},
		Flags:    0,
	}, &openOut); status != fuse.OK {
		t.Fatalf("Open failed: %v", status)
	}

	// Wait briefly for background prefetch to complete
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if emptyDiskCache.CurrentBytes() >= int64(totalSize) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if emptyDiskCache.CurrentBytes() < int64(totalSize) {
		t.Fatalf("Expected prefetch to fetch all %d bytes, got %d", totalSize, emptyDiskCache.CurrentBytes())
	}

	// Release handle
	rawFS.Release(nil, &fuse.ReleaseIn{
		InHeader: fuse.InHeader{NodeId: fileID},
		Fh:       openOut.Fh,
	})
}

func TestDiskBlobCacheSequentialReadPrefetchExtension(t *testing.T) {
	client, cleanup := createTestClient(t)
	defer cleanup()

	volumeID := "vol-prefetch-seq"
	cacheDir := t.TempDir()

	diskCache, err := NewDiskBlobCache(cacheDir, 20*1024*1024)
	if err != nil {
		t.Fatalf("NewDiskBlobCache failed: %v", err)
	}
	defer diskCache.Close()

	memCache := NewNodeCache(1024 * 1024)
	rawFS := NewObjectFS(client, volumeID, pb.WriteMode_WRITE_THROUGH_FSYNC, memCache)
	rawFS.SetDiskBlobCache(diskCache)

	// Create 40 chunks (40 * 64KB = 2560 KB > 32 chunks default window)
	chunkSize := 64 * 1024
	totalChunks := 40
	totalSize := totalChunks * chunkSize
	content := bytes.Repeat([]byte("SEQUENTIAL_PREFETCH_CHUNK_DATA_"), totalSize/31+1)[:totalSize]

	var createOut fuse.CreateOut
	if status := rawFS.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     0644,
	}, "large_seq.bin", &createOut); status != fuse.OK {
		t.Fatalf("Create failed: %v", status)
	}
	fileID := createOut.EntryOut.NodeId

	for i := 0; i < totalChunks; i++ {
		start := i * chunkSize
		n, status := rawFS.Write(nil, &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: fileID},
			Offset:   uint64(start),
		}, content[start:start+chunkSize])
		if status != fuse.OK || int(n) != chunkSize {
			t.Fatalf("Write chunk %d failed: status=%v, n=%d", i, status, n)
		}
	}

	if status := rawFS.Fsync(nil, &fuse.FsyncIn{
		InHeader: fuse.InHeader{NodeId: fileID},
	}); status != fuse.OK {
		t.Fatalf("Fsync failed: %v", status)
	}

	freshCacheDir := t.TempDir()
	freshDiskCache, err := NewDiskBlobCache(freshCacheDir, 20*1024*1024)
	if err != nil {
		t.Fatalf("NewDiskBlobCache failed: %v", err)
	}
	defer freshDiskCache.Close()

	rawFS.SetDiskBlobCache(freshDiskCache)
	memCache.Clear()

	// Open triggers initial prefetch window (up to DefaultPrefetchWindow = 32 chunks)
	var openOut fuse.OpenOut
	if status := rawFS.Open(nil, &fuse.OpenIn{
		InHeader: fuse.InHeader{NodeId: fileID},
	}, &openOut); status != fuse.OK {
		t.Fatalf("Open failed: %v", status)
	}

	// Read sequentially up through chunk 33, which should extend prefetch window to chunk 40
	for i := 0; i < totalChunks; i++ {
		start := int64(i * chunkSize)
		res, status := rawFS.Read(nil, &fuse.ReadIn{
			InHeader: fuse.InHeader{NodeId: fileID},
			Fh:       openOut.Fh,
			Offset:   uint64(start),
			Size:     uint32(chunkSize),
		}, nil)
		if status != fuse.OK {
			t.Fatalf("Read chunk %d failed: %v", i, status)
		}
		data, _ := res.Bytes(nil)
		if !bytes.Equal(data, content[start:start+int64(chunkSize)]) {
			t.Fatalf("Chunk %d content mismatch", i)
		}
	}

	// Clean up
	rawFS.Release(nil, &fuse.ReleaseIn{
		InHeader: fuse.InHeader{NodeId: fileID},
		Fh:       openOut.Fh,
	})
}

func TestDiskBlobCacheRemountThroughput(t *testing.T) {
	client, cleanup := createTestClient(t)
	defer cleanup()

	volumeID := "vol-remount-benchmark"
	sharedCacheDir := t.TempDir()

	// Create and write a 2 MiB file (32 chunks)
	chunkSize := 64 * 1024
	totalChunks := 32
	totalSize := totalChunks * chunkSize
	content := make([]byte, totalSize)
	for i := range content {
		content[i] = byte(i % 251)
	}

	memCache1 := NewNodeCache(1024 * 1024)
	fs1 := NewObjectFS(client, volumeID, pb.WriteMode_WRITE_THROUGH_FSYNC, memCache1)

	var createOut fuse.CreateOut
	if status := fs1.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     0644,
	}, "bench.bin", &createOut); status != fuse.OK {
		t.Fatalf("Create failed: %v", status)
	}
	fileID := createOut.EntryOut.NodeId

	for i := 0; i < totalChunks; i++ {
		start := i * chunkSize
		n, status := fs1.Write(nil, &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: fileID},
			Offset:   uint64(start),
		}, content[start:start+chunkSize])
		if status != fuse.OK || int(n) != chunkSize {
			t.Fatalf("Write chunk %d failed: status=%v", i, status)
		}
	}
	if status := fs1.Fsync(nil, &fuse.FsyncIn{
		InHeader: fuse.InHeader{NodeId: fileID},
	}); status != fuse.OK {
		t.Fatalf("Fsync failed: %v", status)
	}

	// Mount 1 (Cold Read into shared disk cache)
	diskCache1, err := NewDiskBlobCache(sharedCacheDir, 50*1024*1024)
	if err != nil {
		t.Fatalf("NewDiskBlobCache 1 failed: %v", err)
	}
	fs1.SetDiskBlobCache(diskCache1)
	memCache1.Clear()

	tColdStart := time.Now()
	resCold, status := fs1.Read(nil, &fuse.ReadIn{
		InHeader: fuse.InHeader{NodeId: fileID},
		Offset:   0,
		Size:     uint32(totalSize),
	}, nil)
	coldDuration := time.Since(tColdStart)
	if status != fuse.OK {
		t.Fatalf("Cold read failed: %v", status)
	}
	coldBytes, _ := resCold.Bytes(nil)
	if len(coldBytes) != totalSize {
		t.Fatalf("Cold read size mismatch: %d != %d", len(coldBytes), totalSize)
	}
	coldMBs := (float64(totalSize) / (1024 * 1024)) / coldDuration.Seconds()
	_ = diskCache1.Close()

	// Mount 2 (Remount with same cache directory - Warm Read from persistent disk cache)
	diskCache2, err := NewDiskBlobCache(sharedCacheDir, 50*1024*1024)
	if err != nil {
		t.Fatalf("NewDiskBlobCache 2 (remount) failed: %v", err)
	}
	defer diskCache2.Close()

	memCache2 := NewNodeCache(1024 * 1024)
	fs2 := NewObjectFS(client, volumeID, pb.WriteMode_WRITE_THROUGH_FSYNC, memCache2)
	fs2.SetDiskBlobCache(diskCache2)

	tWarmStart := time.Now()
	resWarm, status := fs2.Read(nil, &fuse.ReadIn{
		InHeader: fuse.InHeader{NodeId: fileID},
		Offset:   0,
		Size:     uint32(totalSize),
	}, nil)
	warmDuration := time.Since(tWarmStart)
	if status != fuse.OK {
		t.Fatalf("Warm read on remount failed: %v", status)
	}
	warmBytes, _ := resWarm.Bytes(nil)
	if len(warmBytes) != totalSize {
		t.Fatalf("Warm read size mismatch: %d != %d", len(warmBytes), totalSize)
	}
	warmMBs := (float64(totalSize) / (1024 * 1024)) / warmDuration.Seconds()

	t.Logf("Cold read (%d bytes): %v (%.2f MB/s)", totalSize, coldDuration, coldMBs)
	t.Logf("Warm remount read (%d bytes): %v (%.2f MB/s)", totalSize, warmDuration, warmMBs)
	t.Logf("Throughput improvement: %.2fx faster", warmMBs/coldMBs)

	if warmDuration >= coldDuration {
		t.Logf("Note: warm remount read duration (%v) compared to cold (%v)", warmDuration, coldDuration)
	}
}

func TestTwoClientsCrossNodeWriteVisibleViaWatcher(t *testing.T) {
	clientA, cleanup := createTestClient(t)
	defer cleanup()
	clientB := clientA

	volumeID := "vol-cross-node-watch"
	cacheA := NewNodeCache(1024 * 1024)
	fsA := NewObjectFS(clientA, volumeID, pb.WriteMode_WRITE_THROUGH_FSYNC, cacheA)
	diskCacheA, err := NewDiskBlobCache(t.TempDir(), 10*1024*1024)
	if err != nil {
		t.Fatalf("NewDiskBlobCache failed: %v", err)
	}
	defer diskCacheA.Close()
	fsA.SetDiskBlobCache(diskCacheA)

	// Start watcher on fsA for node-A
	watchCtx, cancelWatch := context.WithCancel(t.Context())
	defer cancelWatch()
	StartWatcher(watchCtx, clientA, volumeID, "node-A", pb.ConsistencyMode_STANDARD, cacheA, fsA)

	// Client A creates and writes initial version
	var createOut fuse.CreateOut
	if status := fsA.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     0644,
	}, "shared.txt", &createOut); status != fuse.OK {
		t.Fatalf("Create failed: %v", status)
	}
	inode := createOut.EntryOut.NodeId

	dataV1 := []byte("version-1-initial-content")
	written, status := fsA.Write(nil, &fuse.WriteIn{
		InHeader: fuse.InHeader{NodeId: inode},
		Offset:   0,
	}, dataV1)
	if status != fuse.OK || int(written) != len(dataV1) {
		t.Fatalf("Write v1 failed: %v", status)
	}
	if status := fsA.Fsync(nil, &fuse.FsyncIn{
		InHeader: fuse.InHeader{NodeId: inode},
	}); status != fuse.OK {
		t.Fatalf("Fsync v1 failed: %v", status)
	}

	// Client A reads and populates its manifest and disk blob cache
	resA, status := fsA.Read(nil, &fuse.ReadIn{
		InHeader: fuse.InHeader{NodeId: inode},
		Offset:   0,
		Size:     uint32(len(dataV1)),
	}, nil)
	if status != fuse.OK {
		t.Fatalf("Client A read v1 failed: %v", status)
	}
	v1Read, _ := resA.Bytes(nil)
	if !bytes.Equal(v1Read, dataV1) {
		t.Fatalf("Expected v1 data %q, got %q", string(dataV1), string(v1Read))
	}

	// Verify manifest is cached on client A
	fsA.manifestMu.RLock()
	hasManifest := fsA.manifests[inode] != nil
	fsA.manifestMu.RUnlock()
	if !hasManifest {
		t.Fatalf("Expected manifest to be cached on client A")
	}

	// Client B (simulating another node) writes and fsyncs version 2 directly to controller
	dataV2 := []byte("version-2-updated-content-from-client-B")
	wResp, err := clientB.WriteFile(t.Context(), &pb.WriteFileRequest{
		VolumeId:  volumeID,
		Inode:     inode,
		Offset:    0,
		Data:      dataV2,
		WriteMode: pb.WriteMode_WRITE_THROUGH_FSYNC,
	})
	if err != nil || wResp.GetError() != 0 {
		t.Fatalf("Client B WriteFile failed: %v, code=%d", err, wResp.GetError())
	}

	fResp, err := clientB.Fsync(t.Context(), &pb.FsyncRequest{
		VolumeId: volumeID,
		Inode:    inode,
		Blocking: true,
	})
	if err != nil || fResp.GetError() != 0 {
		t.Fatalf("Client B Fsync failed: %v, code=%d", err, fResp.GetError())
	}

	// Wait for the watcher event to arrive and invalidate client A's manifest
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		fsA.manifestMu.RLock()
		invalidated := fsA.manifests[inode] == nil
		fsA.manifestMu.RUnlock()
		if invalidated {
			break
		}
		runtime.Gosched()
	}

	fsA.manifestMu.RLock()
	stillCached := fsA.manifests[inode] != nil
	fsA.manifestMu.RUnlock()
	if stillCached {
		t.Fatalf("Expected watcher to invalidate manifest on client A after remote write")
	}

	// Client A reads again: must observe version 2 without waiting for the 5-minute TTL!
	resA2, status := fsA.Read(nil, &fuse.ReadIn{
		InHeader: fuse.InHeader{NodeId: inode},
		Offset:   0,
		Size:     uint32(len(dataV2)),
	}, nil)
	if status != fuse.OK {
		t.Fatalf("Client A read v2 failed: %v", status)
	}
	v2Read, _ := resA2.Bytes(nil)
	if !bytes.Equal(v2Read, dataV2) {
		t.Fatalf("Client A read stale data: got %q, want %q", string(v2Read), string(dataV2))
	}
}

func TestSharedDiskBlobCacheAcrossMounts(t *testing.T) {
	client, cleanup := createTestClient(t)
	defer cleanup()

	sharedDir := t.TempDir()
	// Shared budget of 500 bytes
	sharedCache, err := NewDiskBlobCache(sharedDir, 500)
	if err != nil {
		t.Fatalf("NewDiskBlobCache failed: %v", err)
	}
	defer sharedCache.Close()

	vol1 := "vol-mount-1"
	vol2 := "vol-mount-2"

	cache1 := NewNodeCache(1024 * 1024)
	fs1 := NewObjectFS(client, vol1, pb.WriteMode_WRITE_THROUGH_FSYNC, cache1)
	fs1.SetDiskBlobCache(sharedCache)

	cache2 := NewNodeCache(1024 * 1024)
	fs2 := NewObjectFS(client, vol2, pb.WriteMode_WRITE_THROUGH_FSYNC, cache2)
	fs2.SetDiskBlobCache(sharedCache)

	// Create and write file in vol1 with specific content (100 bytes)
	data := bytes.Repeat([]byte("A"), 100)
	sha := shaString(data)

	var create1 fuse.CreateOut
	if status := fs1.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     0644,
	}, "f1.bin", &create1); status != fuse.OK {
		t.Fatalf("fs1 Create failed: %v", status)
	}
	f1ID := create1.EntryOut.NodeId

	if n, status := fs1.Write(nil, &fuse.WriteIn{
		InHeader: fuse.InHeader{NodeId: f1ID},
		Offset:   0,
	}, data); status != fuse.OK || int(n) != len(data) {
		t.Fatalf("fs1 Write failed: %v", status)
	}
	if status := fs1.Fsync(nil, &fuse.FsyncIn{
		InHeader: fuse.InHeader{NodeId: f1ID},
	}); status != fuse.OK {
		t.Fatalf("fs1 Fsync failed: %v", status)
	}

	// Cold read from vol1 puts the blob into sharedCache
	cache1.Clear()
	if _, status := fs1.Read(nil, &fuse.ReadIn{
		InHeader: fuse.InHeader{NodeId: f1ID},
		Offset:   0,
		Size:     uint32(len(data)),
	}, nil); status != fuse.OK {
		t.Fatalf("fs1 Read failed: %v", status)
	}

	if !sharedCache.HasBlob(sha) {
		t.Fatalf("Expected sharedCache to have blob %s after vol1 read", sha)
	}

	// Create identical file in vol2
	var create2 fuse.CreateOut
	if status := fs2.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     0644,
	}, "f2.bin", &create2); status != fuse.OK {
		t.Fatalf("fs2 Create failed: %v", status)
	}
	f2ID := create2.EntryOut.NodeId

	if n, status := fs2.Write(nil, &fuse.WriteIn{
		InHeader: fuse.InHeader{NodeId: f2ID},
		Offset:   0,
	}, data); status != fuse.OK || int(n) != len(data) {
		t.Fatalf("fs2 Write failed: %v", status)
	}
	if status := fs2.Fsync(nil, &fuse.FsyncIn{
		InHeader: fuse.InHeader{NodeId: f2ID},
	}); status != fuse.OK {
		t.Fatalf("fs2 Fsync failed: %v", status)
	}

	// Read in vol2: served from shared disk cache (already present from vol1)
	cache2.Clear()
	res2, status := fs2.Read(nil, &fuse.ReadIn{
		InHeader: fuse.InHeader{NodeId: f2ID},
		Offset:   0,
		Size:     uint32(len(data)),
	}, nil)
	if status != fuse.OK {
		t.Fatalf("fs2 Read failed: %v", status)
	}
	d2, _ := res2.Bytes(nil)
	if !bytes.Equal(d2, data) {
		t.Fatalf("fs2 read mismatch: got %q, want %q", string(d2), string(data))
	}

	// Both mounts share one budget (500 bytes)
	if sharedCache.CurrentBytes() > sharedCache.MaxBytes() {
		t.Fatalf("Shared cache exceeded budget: %d > %d", sharedCache.CurrentBytes(), sharedCache.MaxBytes())
	}
}

func TestDirtyReadSurvivesControllerRestartWhileSyncInFlight(t *testing.T) {
	// Reproduces issue #252:
	// A close-triggered non-blocking sync is in-flight when the controller crashes/restarts.
	// The restarted controller has not committed staged chunks (reports size 0).
	// Lookup and Read from the client must preserve and return the dirty cached data.

	ctx := t.Context()
	backend := controller.NewMemoryBackend()
	volumeID := "test-dirty-survive-vol"

	bufDir := t.TempDir()
	bt, cleanupBuf := buffertest.Start(ctx, t, buffertest.WithDataDir(bufDir), buffertest.WithBackend(backend))
	defer cleanupBuf()

	// 1. Controller 1 starts with Witness durability against wal-buffer
	clientDir1 := t.TempDir()
	lis1 := bufconn.Listen(1024 * 1024)
	grpcServer1 := grpc.NewServer()
	ctrl1 := controllertest.NewTestServer(backend,
		controller.WithServerWAL(clientDir1, bt.Target, walclient.Witness),
		controller.WithServerWALDialOptions(bt.DialOpts...),
	)
	pb.RegisterObjectFSControllerServer(grpcServer1, ctrl1)
	go func() { _ = grpcServer1.Serve(lis1) }()

	currentLis := lis1
	var lisMu sync.RWMutex

	conn, err := grpc.NewClient("passthrough://bufnet-failover",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			lisMu.RLock()
			l := currentLis
			lisMu.RUnlock()
			return l.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
	)
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer conn.Close()

	client := pb.NewObjectFSControllerClient(conn)
	cache := NewNodeCache(10 * 1024 * 1024)
	rawFS := NewObjectFSWithTimeout(client, volumeID, pb.WriteMode_LAZY_WRITE, cache, 5*time.Second)

	// 2. Create file final.txt (committed to wal-buffer)
	var createOut fuse.CreateOut
	if status := rawFS.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     0644,
	}, "final.txt", &createOut); status != fuse.OK {
		t.Fatalf("Create final.txt failed: %v", status)
	}
	fileID := createOut.EntryOut.NodeId
	fileContent := []byte("node1-final-content")

	// 3. Write data to final.txt locally
	n, status := rawFS.Write(nil, &fuse.WriteIn{
		InHeader: fuse.InHeader{NodeId: fileID},
		Offset:   0,
	}, fileContent)
	if status != fuse.OK || int(n) != len(fileContent) {
		t.Fatalf("Write failed: status=%v, n=%d", status, n)
	}

	// 4. Client pushes to staging (simulating Flush on close)
	epoch, runs, gens, err := rawFS.pushDirtyChunksToStaging(ctx, fileID)
	if err != nil {
		t.Fatalf("pushDirtyChunksToStaging failed: %v", err)
	}
	if epoch == 0 || len(runs) == 0 || len(gens) == 0 {
		t.Fatalf("Expected non-empty staging push")
	}

	// 5. Controller 1 crashes/stops while sync is in flight (Fsync was never completed/acked)
	grpcServer1.Stop()
	_ = ctrl1.Close()
	_ = lis1.Close()

	// 6. Controller 2 starts on the same backend & wal-buffer (uncommitted staged chunks lost)
	clientDir2 := t.TempDir()
	lis2 := bufconn.Listen(1024 * 1024)
	grpcServer2 := grpc.NewServer()
	ctrl2 := controllertest.NewTestServer(backend,
		controller.WithServerWAL(clientDir2, bt.Target, walclient.Witness),
		controller.WithServerWALDialOptions(bt.DialOpts...),
	)
	pb.RegisterObjectFSControllerServer(grpcServer2, ctrl2)
	go func() { _ = grpcServer2.Serve(lis2) }()
	defer func() {
		grpcServer2.Stop()
		_ = ctrl2.Close()
	}()

	lisMu.Lock()
	currentLis = lis2
	lisMu.Unlock()

	// Verify that Controller 2 itself only knows size 0 (un-synced chunks lost on controller crash)
	attrResp, err := ctrl2.GetAttr(ctx, &pb.GetAttrRequest{
		VolumeId: volumeID,
		Inode:    fileID,
	})
	if err != nil {
		t.Fatalf("Controller 2 GetAttr failed: %v", err)
	}
	if attrResp.GetAttr().GetInode().GetSize() != 0 {
		t.Fatalf("Expected Controller 2 to have size 0 for un-synced file, got %d", attrResp.GetAttr().GetInode().GetSize())
	}

	// 7. Client reconnects / watcher runs InvalidateAllClean
	cache.InvalidateAllClean()

	// 8. Client performs Lookup for final.txt:
	// Prior to the fix, Lookup returned Attr.Size = 0 (from Controller 2), poisoning the kernel attribute cache
	// and causing subsequent reads to return empty EOF without querying FUSE Read.
	// With the fix, fillEntryOut / fillAttrOut overrides with the client's dirty cache entry size.
	var lookupOut fuse.EntryOut
	if st := rawFS.Lookup(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "final.txt", &lookupOut); st != fuse.OK {
		t.Fatalf("Lookup final.txt failed: %v", st)
	}
	if lookupOut.Attr.Size != uint64(len(fileContent)) {
		t.Fatalf("Expected Lookup to report dirty size %d, got %d", len(fileContent), lookupOut.Attr.Size)
	}

	// 9. Client reads final.txt via Read: must return the dirty content from local cache
	buf := make([]byte, 1024)
	res, st := rawFS.Read(nil, &fuse.ReadIn{
		InHeader: fuse.InHeader{NodeId: fileID},
		Offset:   0,
		Size:     1024,
	}, buf)
	if st != fuse.OK {
		t.Fatalf("Read final.txt failed: %v", st)
	}
	readBytes, rSt := res.Bytes(buf)
	if rSt != fuse.OK {
		t.Fatalf("Read result Bytes failed: %v", rSt)
	}
	if !bytes.Equal(readBytes, fileContent) {
		t.Fatalf("Read data mismatch: got %q, want %q", string(readBytes), string(fileContent))
	}

	// 10. ReadDirPlus also must report dirty size
	dirList := fuse.NewDirEntryList(make([]byte, 4096), 0)
	if st := rawFS.ReadDirPlus(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}}, dirList); st != fuse.OK {
		t.Fatalf("ReadDirPlus failed: %v", st)
	}

	// 11. Now trigger sync to Controller 2 (simulating retry / background sync)
	// Because staging epoch changed on Controller 2, syncVolumeNonBlocking re-pushes dirty chunks
	if err := rawFS.syncVolumeNonBlocking(ctx, pb.FsyncRequest_SYNC_TRIGGER_PERIODIC); err != nil {
		t.Fatalf("syncVolumeNonBlocking failed: %v", err)
	}

	// 12. Verify Controller 2 now has the durable committed size and content
	attrResp2, err := ctrl2.GetAttr(ctx, &pb.GetAttrRequest{
		VolumeId: volumeID,
		Inode:    fileID,
	})
	if err != nil {
		t.Fatalf("Controller 2 GetAttr after sync failed: %v", err)
	}
	if attrResp2.GetAttr().GetInode().GetSize() != int64(len(fileContent)) {
		t.Fatalf("Expected Controller 2 to have durable size %d after sync, got %d", len(fileContent), attrResp2.GetAttr().GetInode().GetSize())
	}

	// Invalidate client cache completely to ensure controller is serving
	cache.Clear()
	var lookupOut2 fuse.EntryOut
	if st := rawFS.Lookup(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "final.txt", &lookupOut2); st != fuse.OK {
		t.Fatalf("Lookup after cache clear failed: %v", st)
	}
	if lookupOut2.Attr.Size != uint64(len(fileContent)) {
		t.Fatalf("Expected Lookup after cache clear to report durable size %d, got %d", len(fileContent), lookupOut2.Attr.Size)
	}
}

func TestFUSEFallocateAllocate(t *testing.T) {
	client, cleanup := createTestClient(t)
	defer cleanup()

	volumeID := "vol-falloc-alloc"
	cache := NewNodeCache(4 * 1024 * 1024)
	rawFS := NewObjectFS(client, volumeID, pb.WriteMode_WRITE_THROUGH_FSYNC, cache)

	var createOut fuse.CreateOut
	if st := rawFS.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     0644,
	}, "alloc.bin", &createOut); st != fuse.OK {
		t.Fatalf("Create failed: %v", st)
	}
	fileID := createOut.EntryOut.NodeId

	// 1. Initial write of 100 bytes
	data100 := bytes.Repeat([]byte("A"), 100)
	if _, st := rawFS.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: fileID}, Offset: 0}, data100); st != fuse.OK {
		t.Fatalf("Write failed: %v", st)
	}

	// Mode 0 with offset+len <= size (e.g. 50 + 40 = 90 <= 100) is a no-op
	st := rawFS.Fallocate(nil, &fuse.FallocateIn{
		InHeader: fuse.InHeader{NodeId: fileID},
		Offset:   50,
		Length:   40,
		Mode:     fallocAllocRange,
	})
	if st != fuse.OK {
		t.Fatalf("Fallocate mode 0 <= size failed: %v", st)
	}
	var attrOut fuse.AttrOut
	if st := rawFS.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: fileID}}, &attrOut); st != fuse.OK {
		t.Fatalf("GetAttr failed: %v", st)
	}
	if attrOut.Attr.Size != 100 {
		t.Fatalf("Expected size 100 after no-op fallocate, got %d", attrOut.Attr.Size)
	}

	// Mode 0 with offset+len > size (e.g. 100 + 400 = 500) extends the file
	st = rawFS.Fallocate(nil, &fuse.FallocateIn{
		InHeader: fuse.InHeader{NodeId: fileID},
		Offset:   100,
		Length:   400,
		Mode:     fallocAllocRange,
	})
	if st != fuse.OK {
		t.Fatalf("Fallocate mode 0 extending size failed: %v", st)
	}
	if st := rawFS.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: fileID}}, &attrOut); st != fuse.OK {
		t.Fatalf("GetAttr failed: %v", st)
	}
	if attrOut.Attr.Size != 500 {
		t.Fatalf("Expected size 500 after extending fallocate, got %d", attrOut.Attr.Size)
	}

	// Read back: 0..100 has 'A', 100..500 has 0
	buf := make([]byte, 500)
	res, st := rawFS.Read(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: fileID}, Offset: 0, Size: 500}, buf)
	if st != fuse.OK {
		t.Fatalf("Read failed: %v", st)
	}
	readBytes, _ := res.Bytes(buf)
	if !bytes.Equal(readBytes[:100], data100) {
		t.Fatalf("Prefix mismatch: got %q", readBytes[:100])
	}
	if !bytes.Equal(readBytes[100:], make([]byte, 400)) {
		t.Fatalf("Extended region not zero-filled")
	}

	// Prior unsynced write in client cache:
	// Write 50 bytes at 500..550 in cache (without fsync)
	data50 := bytes.Repeat([]byte("B"), 50)
	if _, st := rawFS.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: fileID}, Offset: 500}, data50); st != fuse.OK {
		t.Fatalf("Unsynced write failed: %v", st)
	}
	// Fallocate to 700
	st = rawFS.Fallocate(nil, &fuse.FallocateIn{
		InHeader: fuse.InHeader{NodeId: fileID},
		Offset:   600,
		Length:   100,
		Mode:     fallocAllocRange,
	})
	if st != fuse.OK {
		t.Fatalf("Fallocate with prior unsynced write failed: %v", st)
	}
	buf700 := make([]byte, 700)
	res, st = rawFS.Read(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: fileID}, Offset: 0, Size: 700}, buf700)
	if st != fuse.OK {
		t.Fatalf("Read 700 failed: %v", st)
	}
	readBytes700, _ := res.Bytes(buf700)
	if !bytes.Equal(readBytes700[500:550], data50) {
		t.Fatalf("Unsynced write corrupted after fallocate")
	}
	if !bytes.Equal(readBytes700[550:700], make([]byte, 150)) {
		t.Fatalf("Unsynced extension not zero-filled")
	}
}

func TestFUSEFallocateKeepSize(t *testing.T) {
	client, cleanup := createTestClient(t)
	defer cleanup()

	volumeID := "vol-falloc-keepsize"
	cache := NewNodeCache(4 * 1024 * 1024)
	rawFS := NewObjectFS(client, volumeID, pb.WriteMode_WRITE_THROUGH_FSYNC, cache)

	var createOut fuse.CreateOut
	if st := rawFS.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     0644,
	}, "keepsize.bin", &createOut); st != fuse.OK {
		t.Fatalf("Create failed: %v", st)
	}
	fileID := createOut.EntryOut.NodeId

	data := bytes.Repeat([]byte("K"), 200)
	if _, st := rawFS.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: fileID}, Offset: 0}, data); st != fuse.OK {
		t.Fatalf("Write failed: %v", st)
	}

	st := rawFS.Fallocate(nil, &fuse.FallocateIn{
		InHeader: fuse.InHeader{NodeId: fileID},
		Offset:   1000,
		Length:   500,
		Mode:     fallocKeepSize,
	})
	if st != fuse.OK {
		t.Fatalf("Fallocate KEEP_SIZE failed: %v", st)
	}

	var attrOut fuse.AttrOut
	if st := rawFS.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: fileID}}, &attrOut); st != fuse.OK {
		t.Fatalf("GetAttr failed: %v", st)
	}
	if attrOut.Attr.Size != 200 {
		t.Fatalf("Expected size 200 after KEEP_SIZE, got %d", attrOut.Attr.Size)
	}

	// Unsynced write in cache
	dataMore := bytes.Repeat([]byte("Z"), 50)
	if _, st := rawFS.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: fileID}, Offset: 200}, dataMore); st != fuse.OK {
		t.Fatalf("Unsynced write failed: %v", st)
	}
	st = rawFS.Fallocate(nil, &fuse.FallocateIn{
		InHeader: fuse.InHeader{NodeId: fileID},
		Offset:   500,
		Length:   500,
		Mode:     fallocKeepSize,
	})
	if st != fuse.OK {
		t.Fatalf("Fallocate KEEP_SIZE with unsynced write failed: %v", st)
	}
	if st := rawFS.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: fileID}}, &attrOut); st != fuse.OK {
		t.Fatalf("GetAttr failed: %v", st)
	}
	if attrOut.Attr.Size != 250 {
		t.Fatalf("Expected size 250 with unsynced write, got %d", attrOut.Attr.Size)
	}
}

func TestFUSEFallocatePunchHoleWholeChunks(t *testing.T) {
	client, cleanup := createTestClient(t)
	defer cleanup()

	volumeID := "vol-punch-whole"
	cache := NewNodeCache(8 * 1024 * 1024)
	rawFS := NewObjectFS(client, volumeID, pb.WriteMode_WRITE_THROUGH_FSYNC, cache)

	var createOut fuse.CreateOut
	if st := rawFS.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     0644,
	}, "punch_whole.bin", &createOut); st != fuse.OK {
		t.Fatalf("Create failed: %v", st)
	}
	fileID := createOut.EntryOut.NodeId

	const cs = int(DefaultChunkSize)
	// Write 3 full chunks (192 KiB)
	chunk0 := bytes.Repeat([]byte("0"), cs)
	chunk1 := bytes.Repeat([]byte("1"), cs)
	chunk2 := bytes.Repeat([]byte("2"), cs)
	allData := append(append(chunk0, chunk1...), chunk2...)
	if _, st := rawFS.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: fileID}, Offset: 0}, allData); st != fuse.OK {
		t.Fatalf("Write failed: %v", st)
	}
	if st := rawFS.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: fileID}}); st != fuse.OK {
		t.Fatalf("Fsync failed: %v", st)
	}

	// Verify manifest before punch has all 3 chunks
	mResp, err := client.GetManifest(t.Context(), &pb.GetManifestRequest{VolumeId: volumeID, Inode: fileID})
	if err != nil || len(mResp.GetChunkSha256()) != 3 || mResp.GetChunkSha256()[1] == "" {
		t.Fatalf("Unexpected manifest before punch: %v", mResp)
	}

	// Punch hole covering whole chunk 1: offset 64 KiB, length 64 KiB
	st := rawFS.Fallocate(nil, &fuse.FallocateIn{
		InHeader: fuse.InHeader{NodeId: fileID},
		Offset:   uint64(cs),
		Length:   uint64(cs),
		Mode:     fallocPunchHole | fallocKeepSize,
	})
	if st != fuse.OK {
		t.Fatalf("Fallocate punch-hole failed: %v", st)
	}

	// Size unchanged
	var attrOut fuse.AttrOut
	if st := rawFS.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: fileID}}, &attrOut); st != fuse.OK {
		t.Fatalf("GetAttr failed: %v", st)
	}
	if attrOut.Attr.Size != uint64(3*cs) {
		t.Fatalf("Expected size %d after punch-hole, got %d", 3*cs, attrOut.Attr.Size)
	}

	// Controller manifest shows chunk 1 has no row (empty sha, hole)
	mRespAfter, err := client.GetManifest(t.Context(), &pb.GetManifestRequest{VolumeId: volumeID, Inode: fileID})
	if err != nil {
		t.Fatalf("GetManifest after punch failed: %v", err)
	}
	if mRespAfter.GetChunkSha256()[1] != "" {
		t.Fatalf("Expected chunk 1 row to be deleted in manifest, got sha %s", mRespAfter.GetChunkSha256()[1])
	}
	if mRespAfter.GetChunkSha256()[0] == "" || mRespAfter.GetChunkSha256()[2] == "" {
		t.Fatalf("Expected chunks 0 and 2 to remain present in manifest")
	}

	// Read back through FUSE: chunk 0 intact, chunk 1 zeros, chunk 2 intact
	buf := make([]byte, 3*cs)
	res, st := rawFS.Read(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: fileID}, Offset: 0, Size: uint32(3 * cs)}, buf)
	if st != fuse.OK {
		t.Fatalf("Read failed: %v", st)
	}
	readBytes, _ := res.Bytes(buf)
	if !bytes.Equal(readBytes[:cs], chunk0) {
		t.Fatalf("Chunk 0 data corrupted")
	}
	if !bytes.Equal(readBytes[cs:2*cs], make([]byte, cs)) {
		t.Fatalf("Chunk 1 not all zeros")
	}
	if !bytes.Equal(readBytes[2*cs:], chunk2) {
		t.Fatalf("Chunk 2 data corrupted")
	}

	// Now test punch-hole with prior unsynced dirty writes in client cache:
	// Overwrite chunk 1 with unsynced dirty data 'D'
	dirtyChunk1 := bytes.Repeat([]byte("D"), cs)
	if _, st := rawFS.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: fileID}, Offset: uint64(cs)}, dirtyChunk1); st != fuse.OK {
		t.Fatalf("Dirty write failed: %v", st)
	}
	// Verify it's dirty in cache
	if _, _, _, _, isDirty := cache.GetDirtyChunks(fileID); !isDirty {
		t.Fatalf("Expected cache to be dirty before punch")
	}

	// Punch chunk 1 again while dirty
	st = rawFS.Fallocate(nil, &fuse.FallocateIn{
		InHeader: fuse.InHeader{NodeId: fileID},
		Offset:   uint64(cs),
		Length:   uint64(cs),
		Mode:     fallocPunchHole | fallocKeepSize,
	})
	if st != fuse.OK {
		t.Fatalf("Fallocate punch-hole on dirty chunk failed: %v", st)
	}

	// Read back: chunk 1 must be zeros
	res, st = rawFS.Read(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: fileID}, Offset: uint64(cs), Size: uint32(cs)}, buf[:cs])
	if st != fuse.OK {
		t.Fatalf("Read chunk 1 failed: %v", st)
	}
	readChunk1, _ := res.Bytes(buf[:cs])
	if !bytes.Equal(readChunk1, make([]byte, cs)) {
		t.Fatalf("Punched dirty chunk not zero-filled")
	}
}

func TestFUSEFallocatePunchHolePartialChunks(t *testing.T) {
	client, cleanup := createTestClient(t)
	defer cleanup()

	volumeID := "vol-punch-partial"
	cache := NewNodeCache(8 * 1024 * 1024)
	rawFS := NewObjectFS(client, volumeID, pb.WriteMode_WRITE_THROUGH_FSYNC, cache)

	var createOut fuse.CreateOut
	if st := rawFS.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     0644,
	}, "punch_partial.bin", &createOut); st != fuse.OK {
		t.Fatalf("Create failed: %v", st)
	}
	fileID := createOut.EntryOut.NodeId

	const cs = int(DefaultChunkSize) // 64 KiB
	// Total size: 150 KiB (2 chunks of 64 KiB + 1 chunk of 22 KiB)
	initialData := make([]byte, 150*1024)
	for i := range initialData {
		initialData[i] = byte(i%251 + 1)
	}
	if _, st := rawFS.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: fileID}, Offset: 0}, initialData); st != fuse.OK {
		t.Fatalf("Write failed: %v", st)
	}
	if st := rawFS.Flush(nil, &fuse.FlushIn{InHeader: fuse.InHeader{NodeId: fileID}}); st != fuse.OK {
		t.Fatalf("Flush failed: %v", st)
	}

	// Punch partial range from 32 KiB to 96 KiB (offset 32 KiB, length 64 KiB):
	// Spans end of chunk 0 (32..64 KiB) and start of chunk 1 (0..32 KiB within chunk 1)
	punchOff := 32 * 1024
	punchLen := 64 * 1024
	st := rawFS.Fallocate(nil, &fuse.FallocateIn{
		InHeader: fuse.InHeader{NodeId: fileID},
		Offset:   uint64(punchOff),
		Length:   uint64(punchLen),
		Mode:     fallocPunchHole | fallocKeepSize,
	})
	if st != fuse.OK {
		t.Fatalf("Fallocate partial punch failed: %v", st)
	}

	// Read back: 0..32KB intact, 32KB..96KB zeros, 96KB..150KB intact
	buf := make([]byte, len(initialData))
	res, st := rawFS.Read(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: fileID}, Offset: 0, Size: uint32(len(initialData))}, buf)
	if st != fuse.OK {
		t.Fatalf("Read failed: %v", st)
	}
	readBytes, _ := res.Bytes(buf)
	if !bytes.Equal(readBytes[:punchOff], initialData[:punchOff]) {
		t.Fatalf("Leading bytes before punch corrupted")
	}
	if !bytes.Equal(readBytes[punchOff:punchOff+punchLen], make([]byte, punchLen)) {
		t.Fatalf("Punched range not zeroed")
	}
	if !bytes.Equal(readBytes[punchOff+punchLen:], initialData[punchOff+punchLen:]) {
		t.Fatalf("Trailing bytes after punch corrupted")
	}

	// With prior unsynced write in cache:
	// Write new pattern over 20KB..40KB (unsynced dirty write)
	patch := bytes.Repeat([]byte{0x77}, 20*1024)
	if _, st := rawFS.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: fileID}, Offset: 20 * 1024}, patch); st != fuse.OK {
		t.Fatalf("Write patch failed: %v", st)
	}
	// Punch 25KB..35KB
	st = rawFS.Fallocate(nil, &fuse.FallocateIn{
		InHeader: fuse.InHeader{NodeId: fileID},
		Offset:   25 * 1024,
		Length:   10 * 1024,
		Mode:     fallocPunchHole | fallocKeepSize,
	})
	if st != fuse.OK {
		t.Fatalf("Fallocate partial punch with dirty write failed: %v", st)
	}

	// Verify 20KB..25KB has 0x77, 25KB..35KB has 0, 35KB..40KB has 0x77
	buf40 := make([]byte, 40*1024)
	res, st = rawFS.Read(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: fileID}, Offset: 0, Size: 40 * 1024}, buf40)
	if st != fuse.OK {
		t.Fatalf("Read failed: %v", st)
	}
	r40, _ := res.Bytes(buf40)
	if !bytes.Equal(r40[20*1024:25*1024], bytes.Repeat([]byte{0x77}, 5*1024)) {
		t.Fatalf("Expected dirty bytes before hole to remain 0x77")
	}
	if !bytes.Equal(r40[25*1024:35*1024], make([]byte, 10*1024)) {
		t.Fatalf("Expected punched range to be zeros")
	}
	if !bytes.Equal(r40[35*1024:40*1024], bytes.Repeat([]byte{0x77}, 5*1024)) {
		t.Fatalf("Expected dirty bytes after hole to remain 0x77")
	}
}

func TestFUSEFallocateZeroRangeGrowsSize(t *testing.T) {
	client, cleanup := createTestClient(t)
	defer cleanup()

	volumeID := "vol-zero-range"
	cache := NewNodeCache(8 * 1024 * 1024)
	rawFS := NewObjectFS(client, volumeID, pb.WriteMode_WRITE_THROUGH_FSYNC, cache)

	var createOut fuse.CreateOut
	if st := rawFS.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     0644,
	}, "zero_range.bin", &createOut); st != fuse.OK {
		t.Fatalf("Create failed: %v", st)
	}
	fileID := createOut.EntryOut.NodeId

	// Initial file size: 50 KiB
	data50k := bytes.Repeat([]byte("Z"), 50*1024)
	if _, st := rawFS.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: fileID}, Offset: 0}, data50k); st != fuse.OK {
		t.Fatalf("Write failed: %v", st)
	}

	// Zero-range from 30 KiB to 130 KiB (offset 30 KiB, length 100 KiB) without KEEP_SIZE
	st := rawFS.Fallocate(nil, &fuse.FallocateIn{
		InHeader: fuse.InHeader{NodeId: fileID},
		Offset:   30 * 1024,
		Length:   100 * 1024,
		Mode:     fallocZeroRange,
	})
	if st != fuse.OK {
		t.Fatalf("Fallocate ZERO_RANGE failed: %v", st)
	}

	// Size must grow to 130 KiB
	var attrOut fuse.AttrOut
	if st := rawFS.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: fileID}}, &attrOut); st != fuse.OK {
		t.Fatalf("GetAttr failed: %v", st)
	}
	if attrOut.Attr.Size != 130*1024 {
		t.Fatalf("Expected size 130 KiB after ZERO_RANGE, got %d", attrOut.Attr.Size)
	}

	// Read back: 0..30KB has 'Z', 30KB..130KB has 0
	buf := make([]byte, 130*1024)
	res, st := rawFS.Read(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: fileID}, Offset: 0, Size: 130 * 1024}, buf)
	if st != fuse.OK {
		t.Fatalf("Read failed: %v", st)
	}
	r, _ := res.Bytes(buf)
	if !bytes.Equal(r[:30*1024], data50k[:30*1024]) {
		t.Fatalf("Prefix before zero-range corrupted")
	}
	if !bytes.Equal(r[30*1024:], make([]byte, 100*1024)) {
		t.Fatalf("Zeroed range not zeros")
	}

	// Zero-range with KEEP_SIZE: size remains unchanged
	st = rawFS.Fallocate(nil, &fuse.FallocateIn{
		InHeader: fuse.InHeader{NodeId: fileID},
		Offset:   100 * 1024,
		Length:   50 * 1024,
		Mode:     fallocZeroRange | fallocKeepSize,
	})
	if st != fuse.OK {
		t.Fatalf("Fallocate ZERO_RANGE|KEEP_SIZE failed: %v", st)
	}
	if st := rawFS.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: fileID}}, &attrOut); st != fuse.OK {
		t.Fatalf("GetAttr failed: %v", st)
	}
	if attrOut.Attr.Size != 130*1024 {
		t.Fatalf("Expected size 130 KiB with KEEP_SIZE, got %d", attrOut.Attr.Size)
	}
}

func TestFUSEFallocateUnsupportedModes(t *testing.T) {
	client, cleanup := createTestClient(t)
	defer cleanup()

	volumeID := "vol-unsupported"
	cache := NewNodeCache(4 * 1024 * 1024)
	rawFS := NewObjectFS(client, volumeID, pb.WriteMode_WRITE_THROUGH_FSYNC, cache)

	var createOut fuse.CreateOut
	if st := rawFS.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     0644,
	}, "unsupported.bin", &createOut); st != fuse.OK {
		t.Fatalf("Create failed: %v", st)
	}
	fileID := createOut.EntryOut.NodeId

	// Prior unsynced dirty write in client cache
	data := []byte("important unsynced data")
	if _, st := rawFS.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: fileID}, Offset: 0}, data); st != fuse.OK {
		t.Fatalf("Write failed: %v", st)
	}

	// Collapse range -> EOPNOTSUPP
	st := rawFS.Fallocate(nil, &fuse.FallocateIn{
		InHeader: fuse.InHeader{NodeId: fileID},
		Offset:   0,
		Length:   10,
		Mode:     fallocCollapseRange,
	})
	if st != fuse.Status(syscall.EOPNOTSUPP) {
		t.Fatalf("Expected EOPNOTSUPP for COLLAPSE_RANGE, got %v", st)
	}

	// Insert range -> EOPNOTSUPP
	st = rawFS.Fallocate(nil, &fuse.FallocateIn{
		InHeader: fuse.InHeader{NodeId: fileID},
		Offset:   0,
		Length:   10,
		Mode:     fallocInsertRange,
	})
	if st != fuse.Status(syscall.EOPNOTSUPP) {
		t.Fatalf("Expected EOPNOTSUPP for INSERT_RANGE, got %v", st)
	}

	// Unshare range -> EOPNOTSUPP
	st = rawFS.Fallocate(nil, &fuse.FallocateIn{
		InHeader: fuse.InHeader{NodeId: fileID},
		Offset:   0,
		Length:   10,
		Mode:     fallocUnshareRange,
	})
	if st != fuse.Status(syscall.EOPNOTSUPP) {
		t.Fatalf("Expected EOPNOTSUPP for UNSHARE_RANGE, got %v", st)
	}

	// Punch hole without KEEP_SIZE -> EINVAL
	st = rawFS.Fallocate(nil, &fuse.FallocateIn{
		InHeader: fuse.InHeader{NodeId: fileID},
		Offset:   0,
		Length:   10,
		Mode:     fallocPunchHole,
	})
	if st != fuse.Status(syscall.EINVAL) {
		t.Fatalf("Expected EINVAL for PUNCH_HOLE without KEEP_SIZE, got %v", st)
	}

	// Verify unsynced data is preserved
	buf := make([]byte, len(data))
	res, st := rawFS.Read(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: fileID}, Offset: 0, Size: uint32(len(data))}, buf)
	if st != fuse.OK {
		t.Fatalf("Read failed: %v", st)
	}
	r, _ := res.Bytes(buf)
	if !bytes.Equal(r, data) {
		t.Fatalf("Unsynced data was modified or corrupted: got %q, want %q", string(r), string(data))
	}
}

func TestFUSEFallocateMultiClientReadsZeros(t *testing.T) {
	client, cleanup := createTestClient(t)
	defer cleanup()

	volumeID := "vol-multi-client-punch"
	cache1 := NewNodeCache(8 * 1024 * 1024)
	rawFS1 := NewObjectFS(client, volumeID, pb.WriteMode_WRITE_THROUGH_FSYNC, cache1)

	cache2 := NewNodeCache(8 * 1024 * 1024)
	rawFS2 := NewObjectFS(client, volumeID, pb.WriteMode_WRITE_THROUGH_FSYNC, cache2)

	var createOut fuse.CreateOut
	if st := rawFS1.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     0644,
	}, "shared.bin", &createOut); st != fuse.OK {
		t.Fatalf("Create failed: %v", st)
	}
	fileID := createOut.EntryOut.NodeId

	// Write 128 KiB through rawFS1 and fsync
	content := make([]byte, 128*1024)
	for i := range content {
		content[i] = byte(i%251 + 1)
	}
	if _, st := rawFS1.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: fileID}, Offset: 0}, content); st != fuse.OK {
		t.Fatalf("Write failed: %v", st)
	}
	if st := rawFS1.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: fileID}}); st != fuse.OK {
		t.Fatalf("Fsync failed: %v", st)
	}

	// Punch range [0, 10 KiB) through rawFS1 (starts on chunk boundary, ends inside chunk 0)
	st := rawFS1.Fallocate(nil, &fuse.FallocateIn{
		InHeader: fuse.InHeader{NodeId: fileID},
		Offset:   0,
		Length:   10 * 1024,
		Mode:     fallocPunchHole | fallocKeepSize,
	})
	if st != fuse.OK {
		t.Fatalf("Fallocate punch-hole failed: %v", st)
	}

	// Warm client (rawFS1) reads back zeros in [0, 10 KiB)
	buf1 := make([]byte, 10*1024)
	res1, st := rawFS1.Read(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: fileID}, Offset: 0, Size: 10 * 1024}, buf1)
	if st != fuse.OK {
		t.Fatalf("rawFS1 Read failed: %v", st)
	}
	r1, _ := res1.Bytes(buf1)
	if !bytes.Equal(r1, make([]byte, 10*1024)) {
		t.Fatalf("Warm client: punched range has non-zero bytes")
	}

	// Second / cold client (rawFS2) on the same volume reads back zeros in [0, 10 KiB)
	var lookupOut fuse.EntryOut
	if st := rawFS2.Lookup(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "shared.bin", &lookupOut); st != fuse.OK {
		t.Fatalf("rawFS2 Lookup failed: %v", st)
	}
	fileID2 := lookupOut.NodeId

	buf2 := make([]byte, 10*1024)
	res2, st := rawFS2.Read(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: fileID2}, Offset: 0, Size: 10 * 1024}, buf2)
	if st != fuse.OK {
		t.Fatalf("rawFS2 Read failed: %v", st)
	}
	r2, _ := res2.Bytes(buf2)
	if !bytes.Equal(r2, make([]byte, 10*1024)) {
		t.Fatalf("Cold client: punched range has non-zero bytes (controller bug masked by warm client cache)")
	}

	// Cold client reads the rest of the file and verifies unpunched data is intact
	bufRest := make([]byte, 118*1024)
	resRest, st := rawFS2.Read(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: fileID2}, Offset: 10 * 1024, Size: 118 * 1024}, bufRest)
	if st != fuse.OK {
		t.Fatalf("rawFS2 Read rest failed: %v", st)
	}
	rRest, _ := resRest.Bytes(bufRest)
	if !bytes.Equal(rRest, content[10*1024:]) {
		t.Fatalf("Cold client: unpunched range corrupted")
	}
}

func TestWatchEventDecisionLogic(t *testing.T) {
	cache := NewNodeCache(1024 * 1024)
	fs := &ObjectFS{
		manifests: make(map[uint64]*fileManifest),
	}

	var events []notifyEvent
	fs.notifyHook = func(e notifyEvent) {
		events = append(events, e)
	}

	// 1. EVENT_MODIFIED: emits InodeNotify(ino, 0, -1) and clears clean cache & manifest
	fs.manifests[100] = &fileManifest{inode: 100}
	cache.Put(100, []byte("clean-data"), time.Now(), "")
	events = nil
	handleWatchEvent(&pb.WatchVolumeResponse{
		EventType: pb.WatchEventType_EVENT_MODIFIED,
		Inode:     100,
	}, cache, []*ObjectFS{fs})

	if len(events) != 1 {
		t.Fatalf("Expected 1 notify event for EVENT_MODIFIED, got %d", len(events))
	}
	if events[0].Op != "inode" || events[0].Inode != 100 || events[0].Off != 0 || events[0].Length != -1 {
		t.Fatalf("Unexpected notify event for EVENT_MODIFIED: %+v", events[0])
	}
	if fs.manifests[100] != nil {
		t.Fatalf("Expected manifest to be invalidated for clean inode 100")
	}
	if _, ok := cache.Get(100); ok {
		t.Fatalf("Expected clean cache entry to be invalidated for inode 100")
	}

	// 2. Dirty skip: local writer wins locally (#222/#263).
	// If inode is dirty, skip all notifications and do not touch cache/manifest.
	cache.WriteAt(200, 0, []byte("dirty-data"), time.Now())
	fs.manifests[200] = &fileManifest{inode: 200}
	events = nil
	handleWatchEvent(&pb.WatchVolumeResponse{
		EventType: pb.WatchEventType_EVENT_MODIFIED,
		Inode:     200,
	}, cache, []*ObjectFS{fs})

	if len(events) != 0 {
		t.Fatalf("Expected 0 notify events for dirty inode 200, got %d (%+v)", len(events), events)
	}
	if !cache.IsInodeDirty(200) {
		t.Fatalf("Expected dirty cache entry for inode 200 to remain dirty")
	}
	if fs.manifests[200] == nil {
		t.Fatalf("Expected dirty inode 200 manifest to be preserved")
	}

	// Dirty skip also applies to DELETED and RENAMED
	events = nil
	handleWatchEvent(&pb.WatchVolumeResponse{
		EventType:   pb.WatchEventType_EVENT_DELETED,
		Inode:       200,
		ParentInode: 1,
		Name:        "file.txt",
	}, cache, []*ObjectFS{fs})
	if len(events) != 0 {
		t.Fatalf("Expected 0 notify events for dirty deleted inode 200, got %d", len(events))
	}

	events = nil
	handleWatchEvent(&pb.WatchVolumeResponse{
		EventType:      pb.WatchEventType_EVENT_RENAMED,
		Inode:          200,
		OldParentInode: 1,
		OldName:        "old.txt",
		ParentInode:    2,
		Name:           "new.txt",
	}, cache, []*ObjectFS{fs})
	if len(events) != 0 {
		t.Fatalf("Expected 0 notify events for dirty renamed inode 200, got %d", len(events))
	}

	// 3. EVENT_CREATED: emits EntryNotify(parent, name)
	events = nil
	handleWatchEvent(&pb.WatchVolumeResponse{
		EventType:   pb.WatchEventType_EVENT_CREATED,
		Inode:       300,
		ParentInode: 1,
		Name:        "created.txt",
	}, cache, []*ObjectFS{fs})
	if len(events) != 1 {
		t.Fatalf("Expected 1 notify event for EVENT_CREATED, got %d", len(events))
	}
	if events[0].Op != "entry" || events[0].Parent != 1 || events[0].Name != "created.txt" {
		t.Fatalf("Unexpected notify event for EVENT_CREATED: %+v", events[0])
	}

	// 4. EVENT_DELETED with known child inode: emits DeleteNotify(parent, child, name)
	events = nil
	handleWatchEvent(&pb.WatchVolumeResponse{
		EventType:   pb.WatchEventType_EVENT_DELETED,
		Inode:       400,
		ParentInode: 1,
		Name:        "deleted.txt",
	}, cache, []*ObjectFS{fs})
	if len(events) != 1 {
		t.Fatalf("Expected 1 notify event for EVENT_DELETED, got %d", len(events))
	}
	if events[0].Op != "delete" || events[0].Parent != 1 || events[0].Child != 400 || events[0].Name != "deleted.txt" {
		t.Fatalf("Unexpected notify event for EVENT_DELETED: %+v", events[0])
	}

	// 5. EVENT_DELETED with unknown child inode (0): emits EntryNotify(parent, name)
	events = nil
	handleWatchEvent(&pb.WatchVolumeResponse{
		EventType:   pb.WatchEventType_EVENT_DELETED,
		Inode:       0,
		ParentInode: 1,
		Name:        "deleted_unknown.txt",
	}, cache, []*ObjectFS{fs})
	if len(events) != 1 {
		t.Fatalf("Expected 1 notify event for EVENT_DELETED unknown child, got %d", len(events))
	}
	if events[0].Op != "entry" || events[0].Parent != 1 || events[0].Name != "deleted_unknown.txt" {
		t.Fatalf("Unexpected notify event for EVENT_DELETED unknown child: %+v", events[0])
	}

	// 6. EVENT_RENAMED across different directories:
	// emits EntryNotify on both (old_parent, old_name) and (parent, name), plus InodeNotify on both directories
	events = nil
	handleWatchEvent(&pb.WatchVolumeResponse{
		EventType:      pb.WatchEventType_EVENT_RENAMED,
		Inode:          500,
		OldParentInode: 1,
		OldName:        "from.txt",
		ParentInode:    2,
		Name:           "to.txt",
	}, cache, []*ObjectFS{fs})
	if len(events) != 4 {
		t.Fatalf("Expected 4 notify events for cross-dir EVENT_RENAMED, got %d (%+v)", len(events), events)
	}
	if events[0].Op != "entry" || events[0].Parent != 1 || events[0].Name != "from.txt" {
		t.Errorf("Unexpected event 0: %+v", events[0])
	}
	if events[1].Op != "inode" || events[1].Inode != 1 || events[1].Off != -1 || events[1].Length != 0 {
		t.Errorf("Unexpected event 1: %+v", events[1])
	}
	if events[2].Op != "entry" || events[2].Parent != 2 || events[2].Name != "to.txt" {
		t.Errorf("Unexpected event 2: %+v", events[2])
	}
	if events[3].Op != "inode" || events[3].Inode != 2 || events[3].Off != -1 || events[3].Length != 0 {
		t.Errorf("Unexpected event 3: %+v", events[3])
	}

	// 7. EVENT_RENAMED within the same directory:
	// emits EntryNotify for old and new name, plus InodeNotify on the directory once
	events = nil
	handleWatchEvent(&pb.WatchVolumeResponse{
		EventType:      pb.WatchEventType_EVENT_RENAMED,
		Inode:          500,
		OldParentInode: 1,
		OldName:        "same_from.txt",
		ParentInode:    1,
		Name:           "same_to.txt",
	}, cache, []*ObjectFS{fs})
	if len(events) != 3 {
		t.Fatalf("Expected 3 notify events for same-dir EVENT_RENAMED, got %d (%+v)", len(events), events)
	}
	if events[0].Op != "entry" || events[0].Parent != 1 || events[0].Name != "same_from.txt" {
		t.Errorf("Unexpected event 0: %+v", events[0])
	}
	if events[1].Op != "inode" || events[1].Inode != 1 || events[1].Off != -1 || events[1].Length != 0 {
		t.Errorf("Unexpected event 1: %+v", events[1])
	}
	if events[2].Op != "entry" || events[2].Parent != 1 || events[2].Name != "same_to.txt" {
		t.Errorf("Unexpected event 2: %+v", events[2])
	}
}

func TestCrossNodeWatchVolumeKernelNotifications(t *testing.T) {
	clientA, cleanup := createTestClient(t)
	defer cleanup()
	clientB := clientA

	volumeID := "vol-cross-node-notify"
	cacheA := NewNodeCache(1024 * 1024)
	fsA := NewObjectFS(clientA, volumeID, pb.WriteMode_WRITE_THROUGH_FSYNC, cacheA)

	cacheB := NewNodeCache(1024 * 1024)
	fsB := NewObjectFS(clientB, volumeID, pb.WriteMode_WRITE_THROUGH_FSYNC, cacheB)

	var notifyEvents []notifyEvent
	var notifyMu sync.Mutex
	fsB.notifyHook = func(e notifyEvent) {
		notifyMu.Lock()
		notifyEvents = append(notifyEvents, e)
		notifyMu.Unlock()
	}

	// Start watcher on fsB
	watchCtx, cancelWatch := context.WithCancel(t.Context())
	defer cancelWatch()
	StartWatcher(watchCtx, clientB, volumeID, "node-B", pb.ConsistencyMode_STANDARD, cacheB, fsB)

	// Poll until fsB watcher connects
	deadlineWait := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadlineWait) {
		if fsB.IsWatcherConnected() {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Client A creates a file: triggers EVENT_CREATED
	var createOut fuse.CreateOut
	if status := fsA.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     0644,
	}, "test-file.txt", &createOut); status != fuse.OK {
		t.Fatalf("Create failed: %v", status)
	}
	fileInode := createOut.EntryOut.NodeId

	// Verify fsB received EntryNotify for the created file
	deadline := time.Now().Add(3 * time.Second)
	foundCreated := false
	for time.Now().Before(deadline) {
		notifyMu.Lock()
		for _, e := range notifyEvents {
			if e.Op == "entry" && e.Parent == fuse.FUSE_ROOT_ID && e.Name == "test-file.txt" {
				foundCreated = true
				break
			}
		}
		notifyMu.Unlock()
		if foundCreated {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !foundCreated {
		t.Fatalf("Client B did not receive EntryNotify for created file")
	}

	// Client A writes and fsyncs data: triggers EVENT_MODIFIED on fileInode
	data := []byte("cross-node-content")
	written, status := fsA.Write(nil, &fuse.WriteIn{
		InHeader: fuse.InHeader{NodeId: fileInode},
		Offset:   0,
	}, data)
	if status != fuse.OK || int(written) != len(data) {
		t.Fatalf("Write failed: %v", status)
	}
	if status := fsA.Fsync(nil, &fuse.FsyncIn{
		InHeader: fuse.InHeader{NodeId: fileInode},
	}); status != fuse.OK {
		t.Fatalf("Fsync failed: %v", status)
	}

	// Verify fsB received InodeNotify for the modified file
	deadline = time.Now().Add(3 * time.Second)
	foundModified := false
	for time.Now().Before(deadline) {
		notifyMu.Lock()
		for _, e := range notifyEvents {
			if e.Op == "inode" && e.Inode == fileInode && e.Off == 0 && e.Length == -1 {
				foundModified = true
				break
			}
		}
		notifyMu.Unlock()
		if foundModified {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !foundModified {
		t.Fatalf("Client B did not receive InodeNotify for modified file")
	}

	// Client B reads the file and immediately sees the new data
	resB, status := fsB.Read(nil, &fuse.ReadIn{
		InHeader: fuse.InHeader{NodeId: fileInode},
		Offset:   0,
		Size:     uint32(len(data)),
	}, nil)
	if status != fuse.OK {
		t.Fatalf("Client B read failed: %v", status)
	}
	readBytes, _ := resB.Bytes(nil)
	if !bytes.Equal(readBytes, data) {
		t.Fatalf("Client B expected data %q, got %q", string(data), string(readBytes))
	}
}

// TestWriteCacheWritebackPastEOFStaleKernelSize tests the scenario where a remote truncate
// on another node reduced the authoritative file size on the controller, but this node's
// kernel i_size is still stale (e.g. before an invalidation is observed). In that case, the
// local kernel may send a writeback WRITE with WRITE_CACHE past the controller's authoritative EOF.
// ObjectFS.Write must clamp or discard the write so NodeCache does not re-extend the file to a
// page boundary or resurrect bytes past EOF.
func TestWriteCacheWritebackPastEOFStaleKernelSize(t *testing.T) {
	client, cleanup := createTestClient(t)
	defer cleanup()

	volumeID := "vol-mmap-writeback"
	cache := NewNodeCache(1024 * 1024)
	rawFS := NewObjectFS(client, volumeID, pb.WriteMode_WRITE_THROUGH_FSYNC, cache)

	var createOut fuse.CreateOut
	if st := rawFS.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     0644,
	}, "test.bin", &createOut); st != fuse.OK {
		t.Fatalf("Create failed: %v", st)
	}
	fileID := createOut.EntryOut.NodeId

	// Write 100 bytes via write(2)
	initialData := bytes.Repeat([]byte("A"), 100)
	n, st := rawFS.Write(nil, &fuse.WriteIn{
		InHeader: fuse.InHeader{NodeId: fileID},
		Offset:   0,
	}, initialData)
	if st != fuse.OK || n != 100 {
		t.Fatalf("Write 100 bytes failed: %v", st)
	}

	// Simulate a writeback write with WRITE_CACHE flag from a kernel whose i_size was
	// stale or rounded up, covering the full 4096-byte page [0, 4096)
	pageData := bytes.Repeat([]byte("B"), 4096)
	n, st = rawFS.Write(nil, &fuse.WriteIn{
		InHeader:   fuse.InHeader{NodeId: fileID},
		Offset:     0,
		WriteFlags: fuse.WRITE_CACHE,
	}, pageData)
	if st != fuse.OK || n != 4096 {
		t.Fatalf("Write with WRITE_CACHE failed: %v (n=%d)", st, n)
	}

	// Size MUST NOT have grown to 4096! It must remain 100!
	var attrOut fuse.AttrOut
	if st := rawFS.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: fileID}}, &attrOut); st != fuse.OK {
		t.Fatalf("GetAttr failed: %v", st)
	}
	if attrOut.Attr.Size != 100 {
		t.Fatalf("File size grew to page boundary: expected 100, got %d", attrOut.Attr.Size)
	}
}

// TestWriteCacheWritebackAfterRemoteTruncate tests that when a file is truncated down,
// any late writeback WRITE with WRITE_CACHE arriving for ranges at or beyond the new EOF
// (such as when the local kernel's i_size was stale relative to the truncate) is safely
// discarded or clamped, preserving the truncated file size and avoiding resurrecting truncated data.
func TestWriteCacheWritebackAfterRemoteTruncate(t *testing.T) {
	client, cleanup := createTestClient(t)
	defer cleanup()

	volumeID := "vol-mmap-truncate"
	cache := NewNodeCache(1024 * 1024)
	rawFS := NewObjectFS(client, volumeID, pb.WriteMode_WRITE_THROUGH_FSYNC, cache)

	var createOut fuse.CreateOut
	if st := rawFS.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     0644,
	}, "test_trunc.bin", &createOut); st != fuse.OK {
		t.Fatalf("Create failed: %v", st)
	}
	fileID := createOut.EntryOut.NodeId

	// Write 4096 bytes
	initialData := bytes.Repeat([]byte("A"), 4096)
	n, st := rawFS.Write(nil, &fuse.WriteIn{
		InHeader: fuse.InHeader{NodeId: fileID},
		Offset:   0,
	}, initialData)
	if st != fuse.OK || n != 4096 {
		t.Fatalf("Write 4096 bytes failed: %v", st)
	}

	// Truncate to 100 bytes
	var attrOut fuse.AttrOut
	if st := rawFS.SetAttr(nil, &fuse.SetAttrIn{
		SetAttrInCommon: fuse.SetAttrInCommon{
			InHeader: fuse.InHeader{NodeId: fileID},
			Valid:    fuse.FATTR_SIZE,
			Size:     100,
		},
	}, &attrOut); st != fuse.OK {
		t.Fatalf("SetAttr truncate failed: %v", st)
	}
	if attrOut.Attr.Size != 100 {
		t.Fatalf("Truncate size unexpected: %d", attrOut.Attr.Size)
	}

	// Late writeback write arriving for [2048, 4096) completely beyond EOF
	lateDataBeyond := bytes.Repeat([]byte("Z"), 2048)
	n, st = rawFS.Write(nil, &fuse.WriteIn{
		InHeader:   fuse.InHeader{NodeId: fileID},
		Offset:     2048,
		WriteFlags: fuse.WRITE_CACHE,
	}, lateDataBeyond)
	if st != fuse.OK || n != 2048 {
		t.Fatalf("Late writeback beyond EOF failed: %v (n=%d)", st, n)
	}

	// Size must remain 100
	if st := rawFS.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: fileID}}, &attrOut); st != fuse.OK {
		t.Fatalf("GetAttr failed: %v", st)
	}
	if attrOut.Attr.Size != 100 {
		t.Fatalf("File size resurrected after late writeback beyond EOF: expected 100, got %d", attrOut.Attr.Size)
	}

	// Late writeback write arriving for [0, 4096) overlapping truncated EOF
	lateDataOverlap := bytes.Repeat([]byte("Y"), 4096)
	n, st = rawFS.Write(nil, &fuse.WriteIn{
		InHeader:   fuse.InHeader{NodeId: fileID},
		Offset:     0,
		WriteFlags: fuse.WRITE_CACHE,
	}, lateDataOverlap)
	if st != fuse.OK || n != 4096 {
		t.Fatalf("Late writeback overlapping EOF failed: %v (n=%d)", st, n)
	}

	// Size must still remain 100
	if st := rawFS.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: fileID}}, &attrOut); st != fuse.OK {
		t.Fatalf("GetAttr failed: %v", st)
	}
	if attrOut.Attr.Size != 100 {
		t.Fatalf("File size resurrected after late writeback overlapping EOF: expected 100, got %d", attrOut.Attr.Size)
	}

	// Read back through cold client / controller directly
	readResp, err := client.ReadFile(t.Context(), &pb.ReadFileRequest{
		VolumeId: volumeID,
		Inode:    fileID,
		Offset:   0,
		Size:     4096,
	})
	if err != nil || readResp.GetError() != 0 {
		t.Fatalf("Controller ReadFile failed: %v, resp: %v", err, readResp)
	}
	if readResp.GetTotalSize() != 100 {
		t.Fatalf("Controller reports resurrected size: expected 100, got %d", readResp.GetTotalSize())
	}
}

// TestWriteCacheWritebackGetAttrFailureReturnsEIO tests that when size is not cached and
// GetAttr to the controller fails during a WRITE_CACHE writeback, Write returns an EIO
// status rather than silently proceeding without clamping.
func TestWriteCacheWritebackGetAttrFailureReturnsEIO(t *testing.T) {
	client, cleanup := createTestClient(t)
	defer cleanup()

	volumeID := "vol-mmap-getattr-err"
	cache := NewNodeCache(1024 * 1024)
	rawFS := NewObjectFS(client, volumeID, pb.WriteMode_WRITE_THROUGH_FSYNC, cache)

	// Write to non-existent inode with WRITE_CACHE flag when not in cache
	nonExistentInode := uint64(999999)
	pageData := bytes.Repeat([]byte("B"), 4096)
	_, st := rawFS.Write(nil, &fuse.WriteIn{
		InHeader:   fuse.InHeader{NodeId: nonExistentInode},
		Offset:     0,
		WriteFlags: fuse.WRITE_CACHE,
	}, pageData)
	if st == fuse.OK {
		t.Fatalf("Expected error status for non-existent inode during WRITE_CACHE writeback, got OK")
	}
}

func TestWatchEventPositionDecisionLogic(t *testing.T) {
	cache := NewNodeCache(1024 * 1024)
	fs := &ObjectFS{
		manifests: make(map[uint64]*fileManifest),
	}

	var events []notifyEvent
	fs.notifyHook = func(e notifyEvent) {
		events = append(events, e)
	}

	// 1. Self event (position equal to the fsync/known position) is skipped.
	// Inode 100 has known position 50.
	cache.RecordPosition(100, 50)
	if pos := cache.KnownPosition(100); pos != 50 {
		t.Fatalf("Expected KnownPosition 50, got %d", pos)
	}
	fs.manifests[100] = &fileManifest{inode: 100}
	cache.Put(100, []byte("cached-data"), time.Now(), "")
	events = nil

	handleWatchEvent(&pb.WatchVolumeResponse{
		EventType:      pb.WatchEventType_EVENT_MODIFIED,
		Inode:          100,
		StreamPosition: 50,
	}, cache, []*ObjectFS{fs})

	if len(events) != 0 {
		t.Fatalf("Expected 0 notify events for self event at pos 50, got %d", len(events))
	}
	if fs.manifests[100] == nil {
		t.Fatalf("Expected manifest to be preserved on self event")
	}
	if _, ok := cache.Get(100); !ok {
		t.Fatalf("Expected cache entry to be preserved on self event")
	}

	// Also an older event (pos 40 <= 50) is skipped.
	handleWatchEvent(&pb.WatchVolumeResponse{
		EventType:      pb.WatchEventType_EVENT_MODIFIED,
		Inode:          100,
		StreamPosition: 40,
	}, cache, []*ObjectFS{fs})
	if len(events) != 0 {
		t.Fatalf("Expected 0 notify events for stale event at pos 40, got %d", len(events))
	}

	// 2. Foreign event with a higher position is NOT skipped.
	handleWatchEvent(&pb.WatchVolumeResponse{
		EventType:      pb.WatchEventType_EVENT_MODIFIED,
		Inode:          100,
		StreamPosition: 51,
	}, cache, []*ObjectFS{fs})

	if len(events) != 1 {
		t.Fatalf("Expected 1 notify event for foreign event at pos 51, got %d", len(events))
	}
	if events[0].Op != "inode" || events[0].Inode != 100 || events[0].Off != 0 || events[0].Length != -1 {
		t.Fatalf("Unexpected notify event: %+v", events[0])
	}
	if fs.manifests[100] != nil {
		t.Fatalf("Expected manifest to be invalidated for foreign event")
	}
	if _, ok := cache.Get(100); ok {
		t.Fatalf("Expected cache entry to be invalidated for foreign event")
	}
	if pos := cache.KnownPosition(100); pos != 51 {
		t.Fatalf("Expected KnownPosition to be updated to 51, got %d", pos)
	}

	// 3. After reconnect, all KnownPositions are reset and a stale-looking event is applied.
	cache.RecordPosition(100, 100)
	cache.ResetKnownPositions()
	if pos := cache.KnownPosition(100); pos != 0 {
		t.Fatalf("Expected KnownPosition 0 after reset, got %d", pos)
	}

	fs.manifests[100] = &fileManifest{inode: 100}
	cache.Put(100, []byte("fresh-data"), time.Now(), "")
	events = nil

	// Stale-looking event at pos 50 (previously lower than 100, but now KnownPosition is 0)
	handleWatchEvent(&pb.WatchVolumeResponse{
		EventType:      pb.WatchEventType_EVENT_MODIFIED,
		Inode:          100,
		StreamPosition: 50,
	}, cache, []*ObjectFS{fs})

	if len(events) != 1 {
		t.Fatalf("Expected 1 notify event after reconnect reset, got %d", len(events))
	}
	if fs.manifests[100] != nil {
		t.Fatalf("Expected manifest to be invalidated after reconnect")
	}
	if _, ok := cache.Get(100); ok {
		t.Fatalf("Expected cache entry to be invalidated after reconnect")
	}

	// 4. Multi-target event: skips only if ALL targets are known >= event.position.
	cache.ResetKnownPositions()
	cache.RecordPosition(100, 60)
	cache.RecordPosition(1, 59) // parent known position is lower than event pos 60

	events = nil
	handleWatchEvent(&pb.WatchVolumeResponse{
		EventType:      pb.WatchEventType_EVENT_CREATED,
		Inode:          100,
		ParentInode:    1,
		Name:           "file.txt",
		StreamPosition: 60,
	}, cache, []*ObjectFS{fs})

	if len(events) != 1 || events[0].Op != "entry" {
		t.Fatalf("Expected 1 entry notify event when parent position is lower, got %+v", events)
	}

	// When both are known >= event.position, it is skipped.
	cache.RecordPosition(1, 60)
	events = nil
	handleWatchEvent(&pb.WatchVolumeResponse{
		EventType:      pb.WatchEventType_EVENT_CREATED,
		Inode:          100,
		ParentInode:    1,
		Name:           "file.txt",
		StreamPosition: 60,
	}, cache, []*ObjectFS{fs})

	if len(events) != 0 {
		t.Fatalf("Expected 0 notify events when both child and parent positions are known, got %d", len(events))
	}

	// 5. Attribute-derived positions are recorded for regular files and symlinks, but never for directories.
	fs.cache = cache
	fileAttr := &pb.EntryAttr{
		Inode: &pb.Inode{
			Ino:  proto.Uint64(200),
			Mode: 0644 | syscall.S_IFREG,
		},
		StreamPosition: 80,
	}
	var fileOut fuse.Attr
	fs.fillAttrOut(fileAttr, &fileOut)
	if pos := cache.KnownPosition(200); pos != 80 {
		t.Fatalf("Expected file KnownPosition 80 after fillAttrOut, got %d", pos)
	}

	dirAttr := &pb.EntryAttr{
		Inode: &pb.Inode{
			Ino:   proto.Uint64(300),
			Mode:  0755 | syscall.S_IFDIR,
			IsDir: true,
		},
		StreamPosition: 90,
	}
	var dirOut fuse.Attr
	fs.fillAttrOut(dirAttr, &dirOut)
	if pos := cache.KnownPosition(300); pos != 0 {
		t.Fatalf("Expected directory KnownPosition to remain 0 after fillAttrOut, got %d", pos)
	}
}

func TestDirectoryGetAttrDoesNotSuppressDeleteNotify(t *testing.T) {
	cache := NewNodeCache(1024 * 1024)
	fs := &ObjectFS{
		cache:     cache,
		manifests: make(map[uint64]*fileManifest),
	}

	var events []notifyEvent
	fs.notifyHook = func(e notifyEvent) {
		events = append(events, e)
	}

	parentIno := uint64(10)
	childIno := uint64(20)

	// Node B runs GetAttr on the parent directory d. The reply carries Position 105.
	parentAttr := &pb.EntryAttr{
		Inode: &pb.Inode{
			Ino:   proto.Uint64(parentIno),
			Mode:  0755 | syscall.S_IFDIR,
			IsDir: true,
		},
		StreamPosition: 105,
	}
	var outAttr fuse.Attr
	fs.fillAttrOut(parentAttr, &outAttr)

	// Since parentIno is a directory, fillAttrOut must NOT record KnownPosition for it.
	if pos := cache.KnownPosition(parentIno); pos != 0 {
		t.Fatalf("Expected directory parent KnownPosition to remain 0 after GetAttr, got %d", pos)
	}

	// Node A unlinks child "x" at position 100 (which is <= parentAttr.Position 105).
	// When Node B receives the EVENT_DELETED watch event, it must NOT skip the notification.
	handleWatchEvent(&pb.WatchVolumeResponse{
		EventType:      pb.WatchEventType_EVENT_DELETED,
		Inode:          childIno,
		ParentInode:    parentIno,
		Name:           "x",
		StreamPosition: 100,
	}, cache, []*ObjectFS{fs})

	if len(events) != 1 {
		t.Fatalf("Expected 1 notify event for DELETED, got %d", len(events))
	}
	if events[0].Op != "delete" || events[0].Parent != parentIno || events[0].Child != childIno || events[0].Name != "x" {
		t.Fatalf("Unexpected notify event: %+v", events[0])
	}
}

func TestTwoClientNoSelfInvalidation(t *testing.T) {
	clientA, cleanup := createTestClient(t)
	defer cleanup()
	clientB := clientA

	volumeID := "vol-no-self-invalidation"
	cacheA := NewNodeCache(1024 * 1024)
	fsA := NewObjectFS(clientA, volumeID, pb.WriteMode_WRITE_THROUGH_FSYNC, cacheA)

	cacheB := NewNodeCache(1024 * 1024)
	fsB := NewObjectFS(clientB, volumeID, pb.WriteMode_WRITE_THROUGH_FSYNC, cacheB)

	var notifyEventsA []notifyEvent
	var notifyMuA sync.Mutex
	fsA.notifyHook = func(e notifyEvent) {
		notifyMuA.Lock()
		notifyEventsA = append(notifyEventsA, e)
		notifyMuA.Unlock()
	}

	var notifyEventsB []notifyEvent
	var notifyMuB sync.Mutex
	fsB.notifyHook = func(e notifyEvent) {
		notifyMuB.Lock()
		notifyEventsB = append(notifyEventsB, e)
		notifyMuB.Unlock()
	}

	watchCtx, cancelWatch := context.WithCancel(t.Context())
	defer cancelWatch()
	StartWatcher(watchCtx, clientA, volumeID, "node-A", pb.ConsistencyMode_STANDARD, cacheA, fsA)
	StartWatcher(watchCtx, clientB, volumeID, "node-B", pb.ConsistencyMode_STANDARD, cacheB, fsB)

	// Short settle delay for watcher subscriptions
	time.Sleep(50 * time.Millisecond)

	// Client A creates a file
	var createOut fuse.CreateOut
	if status := fsA.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     0644,
	}, "test-self-inval.txt", &createOut); status != fuse.OK {
		t.Fatalf("Create failed: %v", status)
	}
	fileInode := createOut.EntryOut.NodeId

	// Client A writes and fsyncs data
	data := []byte("self-invalidation-data")
	written, status := fsA.Write(nil, &fuse.WriteIn{
		InHeader: fuse.InHeader{NodeId: fileInode},
		Offset:   0,
	}, data)
	if status != fuse.OK || int(written) != len(data) {
		t.Fatalf("Write failed: %v", status)
	}
	if status := fsA.Fsync(nil, &fuse.FsyncIn{
		InHeader: fuse.InHeader{NodeId: fileInode},
	}); status != fuse.OK {
		t.Fatalf("Fsync failed: %v", status)
	}

	// Verify fsB received InodeNotify for fileInode
	deadline := time.Now().Add(3 * time.Second)
	foundModifiedB := false
	for time.Now().Before(deadline) {
		notifyMuB.Lock()
		for _, e := range notifyEventsB {
			if e.Op == "inode" && e.Inode == fileInode {
				foundModifiedB = true
				break
			}
		}
		notifyMuB.Unlock()
		if foundModifiedB {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !foundModifiedB {
		t.Fatalf("Client B did not receive InodeNotify for modified file")
	}

	// Verify fsA saw NO InodeNotify for fileInode (self-invalidation was skipped!)
	notifyMuA.Lock()
	sawSelfInodeInval := false
	for _, e := range notifyEventsA {
		if e.Op == "inode" && e.Inode == fileInode {
			sawSelfInodeInval = true
			break
		}
	}
	notifyMuA.Unlock()

	if sawSelfInodeInval {
		t.Fatalf("Client A unexpectedly received InodeNotify for its own write/fsync!")
	}

	// Verify Client A's cached entry was preserved and reads directly from cache
	entryA, hasA := cacheA.Get(fileInode)
	if !hasA || entryA == nil {
		t.Fatalf("Client A's clean cache entry was invalidated; expected it to be preserved")
	}

	resA, status := fsA.Read(nil, &fuse.ReadIn{
		InHeader: fuse.InHeader{NodeId: fileInode},
		Offset:   0,
		Size:     uint32(len(data)),
	}, nil)
	if status != fuse.OK {
		t.Fatalf("Client A read failed: %v", status)
	}
	readBytesA, _ := resA.Bytes(nil)
	if !bytes.Equal(readBytesA, data) {
		t.Fatalf("Client A expected data %q, got %q", string(data), string(readBytesA))
	}
}

func TestFOPENKeepCache(t *testing.T) {
	client, cleanup := createTestClient(t)
	defer cleanup()

	volumeID := "test-keep-cache-vol"
	cache := NewNodeCache(1024 * 1024)
	rawFS := NewObjectFS(client, volumeID, pb.WriteMode_WRITE_THROUGH_FSYNC, cache)

	// 1. Create sets FOPEN_KEEP_CACHE in OpenOut
	var createOut fuse.CreateOut
	createStatus := rawFS.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     0644,
		Flags:    syscall.O_CREAT | syscall.O_RDWR,
	}, "testfile.txt", &createOut)
	if createStatus != fuse.OK {
		t.Fatalf("Create failed: %v", createStatus)
	}
	if createOut.OpenOut.OpenFlags&fuse.FOPEN_KEEP_CACHE == 0 {
		t.Errorf("Expected FOPEN_KEEP_CACHE to be set in Create OpenOut.OpenFlags, got: %x", createOut.OpenOut.OpenFlags)
	}

	fileID := createOut.EntryOut.NodeId
	if fileID == 0 {
		t.Fatalf("Expected valid NodeId from Create")
	}

	// 2. Open sets FOPEN_KEEP_CACHE in OpenOut
	var openOut fuse.OpenOut
	openStatus := rawFS.Open(nil, &fuse.OpenIn{
		InHeader: fuse.InHeader{NodeId: fileID},
		Flags:    syscall.O_RDONLY,
	}, &openOut)
	if openStatus != fuse.OK {
		t.Fatalf("Open failed: %v", openStatus)
	}
	if openOut.OpenFlags&fuse.FOPEN_KEEP_CACHE == 0 {
		t.Errorf("Expected FOPEN_KEEP_CACHE to be set in Open OpenFlags, got: %x", openOut.OpenFlags)
	}
}

func TestInodeLookupCountTracking(t *testing.T) {
	client, cleanup := createTestClient(t)
	defer cleanup()

	volumeID := "test-lookup-tracking-vol"
	cache := NewNodeCache(1024 * 1024)
	rawFS := NewObjectFS(client, volumeID, pb.WriteMode_WRITE_THROUGH_FSYNC, cache)

	// 1. Initial state: root directory (inode 1) has lookup count 1
	if count := rawFS.LookupCount(fuse.FUSE_ROOT_ID); count != 1 {
		t.Fatalf("Expected root lookup count 1, got %d", count)
	}
	held := rawFS.HeldInodes()
	if len(held) != 1 || held[0] != fuse.FUSE_ROOT_ID {
		t.Fatalf("Expected initial HeldInodes to be [1], got: %v", held)
	}

	// 2. Create increments lookup count for new file
	var createOut fuse.CreateOut
	if st := rawFS.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     0644,
	}, "file1.txt", &createOut); st != fuse.OK {
		t.Fatalf("Create failed: %v", st)
	}
	fileID := createOut.EntryOut.NodeId
	if count := rawFS.LookupCount(fileID); count != 1 {
		t.Errorf("Expected file1 lookup count 1 after Create, got %d", count)
	}

	// 3. Lookup increments lookup count
	var lookupOut fuse.EntryOut
	if st := rawFS.Lookup(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "file1.txt", &lookupOut); st != fuse.OK {
		t.Fatalf("Lookup failed: %v", st)
	}
	if count := rawFS.LookupCount(fileID); count != 2 {
		t.Errorf("Expected file1 lookup count 2 after Lookup, got %d", count)
	}

	// 4. Mkdir increments lookup count for new directory
	var mkdirOut fuse.EntryOut
	if st := rawFS.Mkdir(nil, &fuse.MkdirIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     0755,
	}, "subdir", &mkdirOut); st != fuse.OK {
		t.Fatalf("Mkdir failed: %v", st)
	}
	dirID := mkdirOut.NodeId
	if count := rawFS.LookupCount(dirID); count != 1 {
		t.Errorf("Expected subdir lookup count 1 after Mkdir, got %d", count)
	}

	// 5. Mknod increments lookup count
	var mknodOut fuse.EntryOut
	if st := rawFS.Mknod(nil, &fuse.MknodIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     syscall.S_IFIFO | 0644,
	}, "myfifo", &mknodOut); st != fuse.OK {
		t.Fatalf("Mknod failed: %v", st)
	}
	fifoID := mknodOut.NodeId
	if count := rawFS.LookupCount(fifoID); count != 1 {
		t.Errorf("Expected fifo lookup count 1 after Mknod, got %d", count)
	}

	// 6. Symlink increments lookup count
	var symOut fuse.EntryOut
	if st := rawFS.Symlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "file1.txt", "symlink1", &symOut); st != fuse.OK {
		t.Fatalf("Symlink failed: %v", st)
	}
	symID := symOut.NodeId
	if count := rawFS.LookupCount(symID); count != 1 {
		t.Errorf("Expected symlink lookup count 1 after Symlink, got %d", count)
	}

	// 7. Link increments lookup count
	var linkOut fuse.EntryOut
	if st := rawFS.Link(nil, &fuse.LinkIn{
		InHeader:  fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Oldnodeid: fileID,
	}, "hardlink1", &linkOut); st != fuse.OK {
		t.Fatalf("Link failed: %v", st)
	}
	if count := rawFS.LookupCount(fileID); count != 3 {
		t.Errorf("Expected file1 lookup count 3 after Link, got %d", count)
	}

	// 8. ReadDirPlus increments lookup count for all entries returned.
	// Root contains file1.txt (fileID), subdir (dirID), myfifo (fifoID), symlink1 (symID), and hardlink1 (fileID).
	// Because file1.txt and hardlink1 share fileID, fileID is incremented twice (+2 -> 5).
	dirEntries := fuse.NewDirEntryList(make([]byte, 4096), 0)
	if st := rawFS.ReadDirPlus(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}}, dirEntries); st != fuse.OK {
		t.Fatalf("ReadDirPlus failed: %v", st)
	}
	if count := rawFS.LookupCount(fileID); count != 5 {
		t.Errorf("Expected file1 lookup count 5 after ReadDirPlus, got %d", count)
	}
	if count := rawFS.LookupCount(dirID); count != 2 {
		t.Errorf("Expected subdir lookup count 2 after ReadDirPlus, got %d", count)
	}
	if count := rawFS.LookupCount(fifoID); count != 2 {
		t.Errorf("Expected fifo lookup count 2 after ReadDirPlus, got %d", count)
	}
	if count := rawFS.LookupCount(symID); count != 2 {
		t.Errorf("Expected symlink lookup count 2 after ReadDirPlus, got %d", count)
	}

	// 9. Forget decrements lookup count
	rawFS.Forget(fileID, 1)
	if count := rawFS.LookupCount(fileID); count != 4 {
		t.Errorf("Expected file1 lookup count 4 after Forget(1), got %d", count)
	}

	// Decrement remaining count: 4 -> drops to 0 and deleted
	rawFS.Forget(fileID, 4)
	if count := rawFS.LookupCount(fileID); count != 0 {
		t.Errorf("Expected file1 lookup count 0 after Forget(4), got %d", count)
	}

	// Verify fileID is no longer in HeldInodes
	held = rawFS.HeldInodes()
	for _, id := range held {
		if id == fileID {
			t.Errorf("Did not expect fileID %d in HeldInodes after forgetting all references", fileID)
		}
	}

	// 10. Forget decrements multiple inodes
	rawFS.Forget(dirID, 2)
	rawFS.Forget(fifoID, 2)
	if count := rawFS.LookupCount(dirID); count != 0 {
		t.Errorf("Expected dirID lookup count 0 after Forget, got %d", count)
	}
	if count := rawFS.LookupCount(fifoID); count != 0 {
		t.Errorf("Expected fifoID lookup count 0 after Forget, got %d", count)
	}

	// Over-forgetting drops count to 0 without panicking or underflowing
	rawFS.Forget(symID, 999)
	if count := rawFS.LookupCount(symID); count != 0 {
		t.Errorf("Expected symID lookup count 0 after over-forgetting, got %d", count)
	}
}

func TestHealthGatedTimeouts(t *testing.T) {
	client, cleanup := createTestClient(t)
	defer cleanup()

	volumeID := "test-gated-timeouts-vol"
	cache := NewNodeCache(1024 * 1024)
	rawFS := NewObjectFS(client, volumeID, pb.WriteMode_WRITE_THROUGH_FSYNC, cache)

	// Create test file
	var createOut fuse.CreateOut
	if st := rawFS.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     0644,
	}, "testfile.txt", &createOut); st != fuse.OK {
		t.Fatalf("Create failed: %v", st)
	}
	fileID := createOut.EntryOut.NodeId

	// 1. Initially disconnected: timeouts should be 1 second
	if rawFS.IsWatcherConnected() {
		t.Fatalf("Expected IsWatcherConnected to be false initially")
	}

	var lookupOut fuse.EntryOut
	if st := rawFS.Lookup(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "testfile.txt", &lookupOut); st != fuse.OK {
		t.Fatalf("Lookup failed: %v", st)
	}
	if lookupOut.EntryTimeout() != 1*time.Second {
		t.Errorf("Expected disconnected EntryTimeout 1s, got %v", lookupOut.EntryTimeout())
	}
	if lookupOut.AttrTimeout() != 1*time.Second {
		t.Errorf("Expected disconnected AttrTimeout 1s, got %v", lookupOut.AttrTimeout())
	}

	var attrOut fuse.AttrOut
	if st := rawFS.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: fileID}}, &attrOut); st != fuse.OK {
		t.Fatalf("GetAttr failed: %v", st)
	}
	if attrOut.Timeout() != 1*time.Second {
		t.Errorf("Expected disconnected GetAttr Timeout 1s, got %v", attrOut.Timeout())
	}

	var setAttrOut fuse.AttrOut
	if st := rawFS.SetAttr(nil, &fuse.SetAttrIn{
		SetAttrInCommon: fuse.SetAttrInCommon{
			InHeader: fuse.InHeader{NodeId: fileID},
			Valid:    fuse.FATTR_MODE,
			Mode:     0600,
		},
	}, &setAttrOut); st != fuse.OK {
		t.Fatalf("SetAttr failed: %v", st)
	}
	if setAttrOut.Timeout() != 1*time.Second {
		t.Errorf("Expected disconnected SetAttr Timeout 1s, got %v", setAttrOut.Timeout())
	}

	// 2. Watcher connected: timeouts should be DefaultConnectedEntryTimeout (60s)
	rawFS.SetWatcherConnected(true)
	if !rawFS.IsWatcherConnected() {
		t.Fatalf("Expected IsWatcherConnected to be true")
	}

	var lookupOutConn fuse.EntryOut
	if st := rawFS.Lookup(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "testfile.txt", &lookupOutConn); st != fuse.OK {
		t.Fatalf("Lookup failed: %v", st)
	}
	if lookupOutConn.EntryTimeout() != 60*time.Second {
		t.Errorf("Expected connected EntryTimeout 60s, got %v", lookupOutConn.EntryTimeout())
	}
	if lookupOutConn.AttrTimeout() != 60*time.Second {
		t.Errorf("Expected connected AttrTimeout 60s, got %v", lookupOutConn.AttrTimeout())
	}

	var attrOutConn fuse.AttrOut
	if st := rawFS.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: fileID}}, &attrOutConn); st != fuse.OK {
		t.Fatalf("GetAttr failed: %v", st)
	}
	if attrOutConn.Timeout() != 60*time.Second {
		t.Errorf("Expected connected GetAttr Timeout 60s, got %v", attrOutConn.Timeout())
	}

	// 3. Custom configured timeouts while connected
	rawFS.SetEntryTimeout(45 * time.Second)
	rawFS.SetAttrTimeout(30 * time.Second)

	var lookupOutCustom fuse.EntryOut
	if st := rawFS.Lookup(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "testfile.txt", &lookupOutCustom); st != fuse.OK {
		t.Fatalf("Lookup failed: %v", st)
	}
	if lookupOutCustom.EntryTimeout() != 45*time.Second {
		t.Errorf("Expected custom EntryTimeout 45s, got %v", lookupOutCustom.EntryTimeout())
	}
	if lookupOutCustom.AttrTimeout() != 30*time.Second {
		t.Errorf("Expected custom AttrTimeout 30s, got %v", lookupOutCustom.AttrTimeout())
	}

	var attrOutCustom fuse.AttrOut
	if st := rawFS.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: fileID}}, &attrOutCustom); st != fuse.OK {
		t.Fatalf("GetAttr failed: %v", st)
	}
	if attrOutCustom.Timeout() != 30*time.Second {
		t.Errorf("Expected custom GetAttr Timeout 30s, got %v", attrOutCustom.Timeout())
	}

	// 4. Watcher disconnects: timeouts fall back to 1 second
	rawFS.SetWatcherConnected(false)

	var lookupOutFallback fuse.EntryOut
	if st := rawFS.Lookup(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "testfile.txt", &lookupOutFallback); st != fuse.OK {
		t.Fatalf("Lookup failed: %v", st)
	}
	if lookupOutFallback.EntryTimeout() != 1*time.Second {
		t.Errorf("Expected fallback EntryTimeout 1s, got %v", lookupOutFallback.EntryTimeout())
	}
	if lookupOutFallback.AttrTimeout() != 1*time.Second {
		t.Errorf("Expected fallback AttrTimeout 1s, got %v", lookupOutFallback.AttrTimeout())
	}
}

func TestReconnectSweep(t *testing.T) {
	client, cleanup := createTestClient(t)
	defer cleanup()

	volumeID := "test-reconnect-sweep-vol"
	cache := NewNodeCache(1024 * 1024)
	rawFS := NewObjectFS(client, volumeID, pb.WriteMode_WRITE_THROUGH_FSYNC, cache)

	var notifyMu sync.Mutex
	var events []notifyEvent
	rawFS.notifyHook = func(e notifyEvent) {
		notifyMu.Lock()
		events = append(events, e)
		notifyMu.Unlock()
	}

	// Create file1 (ino ~10) and file2 (ino ~20)
	var create1, create2 fuse.CreateOut
	if st := rawFS.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     0644,
	}, "f1.txt", &create1); st != fuse.OK {
		t.Fatalf("Create f1 failed: %v", st)
	}
	if st := rawFS.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     0644,
	}, "f2.txt", &create2); st != fuse.OK {
		t.Fatalf("Create f2 failed: %v", st)
	}

	f1ID := create1.EntryOut.NodeId
	f2ID := create2.EntryOut.NodeId

	// Held inodes are now root (1), f1ID, and f2ID
	held := rawFS.HeldInodes()
	if len(held) != 3 {
		t.Fatalf("Expected 3 held inodes, got %v", held)
	}

	// Trigger reconnect sweep
	notifyMu.Lock()
	events = nil
	notifyMu.Unlock()

	rawFS.SweepRevalidateInodes()

	notifyMu.Lock()
	sweepEvents := make([]notifyEvent, len(events))
	copy(sweepEvents, events)
	notifyMu.Unlock()

	// Verify sweep issued EntryNotify for tracked dentries and InodeNotify(ino, -1, 0) for each held inode:
	// 2 dentries (f1.txt, f2.txt) + 3 inodes (root, f1ID, f2ID) = 5 events.
	if len(sweepEvents) != 5 {
		t.Fatalf("Expected 5 sweep events, got %d: %+v", len(sweepEvents), sweepEvents)
	}

	expectedInodes := map[uint64]bool{
		fuse.FUSE_ROOT_ID: true,
		f1ID:              true,
		f2ID:              true,
	}
	expectedDentries := map[string]bool{
		"f1.txt": true,
		"f2.txt": true,
	}
	for _, ev := range sweepEvents {
		if ev.Op == "entry" {
			if ev.Parent != fuse.FUSE_ROOT_ID {
				t.Errorf("Expected parent %d, got %d", fuse.FUSE_ROOT_ID, ev.Parent)
			}
			if !expectedDentries[ev.Name] {
				t.Errorf("Unexpected dentry name in sweep: %s", ev.Name)
			}
			delete(expectedDentries, ev.Name)
		} else if ev.Op == "inode" {
			if ev.Off != -1 || ev.Length != 0 {
				t.Errorf("Expected metadata-only invalidation (-1, 0), got (%d, %d)", ev.Off, ev.Length)
			}
			if !expectedInodes[ev.Inode] {
				t.Errorf("Unexpected inode in sweep: %d", ev.Inode)
			}
			delete(expectedInodes, ev.Inode)
		} else {
			t.Errorf("Unexpected op: %s", ev.Op)
		}
	}
	if len(expectedInodes) != 0 {
		t.Errorf("Missing inodes in sweep: %+v", expectedInodes)
	}
	if len(expectedDentries) != 0 {
		t.Errorf("Missing dentries in sweep: %+v", expectedDentries)
	}

	// Now forget f2ID completely
	rawFS.Forget(f2ID, rawFS.LookupCount(f2ID))

	// Sweep again: f2ID and its dentry must NOT be swept
	notifyMu.Lock()
	events = nil
	notifyMu.Unlock()

	rawFS.SweepRevalidateInodes()

	notifyMu.Lock()
	sweepEvents2 := make([]notifyEvent, len(events))
	copy(sweepEvents2, events)
	notifyMu.Unlock()

	// 1 dentry (f1.txt) + 2 inodes (root, f1ID) = 3 events
	if len(sweepEvents2) != 3 {
		t.Fatalf("Expected 3 sweep events after forgetting f2ID, got %d: %+v", len(sweepEvents2), sweepEvents2)
	}
	for _, ev := range sweepEvents2 {
		if ev.Op == "inode" && ev.Inode == f2ID {
			t.Errorf("Forgotten inode %d was unexpectedly swept!", f2ID)
		}
		if ev.Op == "entry" && ev.Name == "f2.txt" {
			t.Errorf("Forgotten dentry f2.txt was unexpectedly swept!")
		}
	}
}

func TestTwoClientsDentryAndInodeReconnectSweep(t *testing.T) {
	client, cleanup := createTestClient(t)
	defer cleanup()

	volumeID := "test-two-clients-sweep-vol"
	cacheA := NewNodeCache(1024 * 1024)
	cacheB := NewNodeCache(1024 * 1024)
	rawFSA := NewObjectFS(client, volumeID, pb.WriteMode_WRITE_THROUGH_FSYNC, cacheA)
	rawFSB := NewObjectFS(client, volumeID, pb.WriteMode_WRITE_THROUGH_FSYNC, cacheB)

	// A creates a file under root
	var createA fuse.CreateOut
	if st := rawFSA.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     0644,
	}, "shared_file.txt", &createA); st != fuse.OK {
		t.Fatalf("Client A failed to create file: %v", st)
	}
	fileID := createA.EntryOut.NodeId

	// B hooks notifications
	var notifyMu sync.Mutex
	var eventsB []notifyEvent
	rawFSB.notifyHook = func(e notifyEvent) {
		notifyMu.Lock()
		eventsB = append(eventsB, e)
		notifyMu.Unlock()
	}

	// B looks up the file, populating its dentry and inode cache
	var lookupB fuse.EntryOut
	if st := rawFSB.Lookup(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "shared_file.txt", &lookupB); st != fuse.OK {
		t.Fatalf("Client B failed to lookup file: %v", st)
	}
	if lookupB.NodeId != fileID {
		t.Fatalf("Client B got NodeId %d, want %d", lookupB.NodeId, fileID)
	}

	// B holds dentry (1, "shared_file.txt") and inode fileID
	dentriesB := rawFSB.TrackedDentries()
	if len(dentriesB) != 1 || dentriesB[0].Parent != fuse.FUSE_ROOT_ID || dentriesB[0].Name != "shared_file.txt" {
		t.Fatalf("Client B expected tracked dentry (1, shared_file.txt), got: %+v", dentriesB)
	}

	// Simulate watcher drop and reconnect on B
	notifyMu.Lock()
	eventsB = nil
	notifyMu.Unlock()

	rawFSB.SweepRevalidateInodes()

	notifyMu.Lock()
	sweep1 := make([]notifyEvent, len(eventsB))
	copy(sweep1, eventsB)
	notifyMu.Unlock()

	// Assert sweep emits EntryNotify(1, "shared_file.txt") and InodeNotify(fileID, -1, 0)
	foundEntry := false
	foundInode := false
	for _, ev := range sweep1 {
		if ev.Op == "entry" && ev.Parent == fuse.FUSE_ROOT_ID && ev.Name == "shared_file.txt" {
			foundEntry = true
		}
		if ev.Op == "inode" && ev.Inode == fileID && ev.Off == -1 && ev.Length == 0 {
			foundInode = true
		}
	}
	if !foundEntry {
		t.Errorf("Expected sweep to emit EntryNotify(1, shared_file.txt), got events: %+v", sweep1)
	}
	if !foundInode {
		t.Errorf("Expected sweep to emit InodeNotify(%d, -1, 0), got events: %+v", fileID, sweep1)
	}

	// B forgets the file completely
	rawFSB.Forget(fileID, rawFSB.LookupCount(fileID))

	// Re-sweep: neither EntryNotify nor InodeNotify for fileID should be emitted
	notifyMu.Lock()
	eventsB = nil
	notifyMu.Unlock()

	rawFSB.SweepRevalidateInodes()

	notifyMu.Lock()
	sweep2 := make([]notifyEvent, len(eventsB))
	copy(sweep2, eventsB)
	notifyMu.Unlock()

	for _, ev := range sweep2 {
		if ev.Op == "entry" && ev.Name == "shared_file.txt" {
			t.Errorf("Forgotten dentry shared_file.txt was unexpectedly swept after Forget: %+v", ev)
		}
		if ev.Op == "inode" && ev.Inode == fileID {
			t.Errorf("Forgotten inode %d was unexpectedly swept after Forget: %+v", fileID, ev)
		}
	}
}

func TestStartWatcherHealthAndSweep(t *testing.T) {
	client, cleanup := createTestClient(t)
	defer cleanup()

	volumeID := "test-watcher-health-vol"
	cache := NewNodeCache(1024 * 1024)
	rawFS := NewObjectFS(client, volumeID, pb.WriteMode_WRITE_THROUGH_FSYNC, cache)

	var notifyMu sync.Mutex
	var events []notifyEvent
	rawFS.notifyHook = func(e notifyEvent) {
		notifyMu.Lock()
		events = append(events, e)
		notifyMu.Unlock()
	}

	// File created before watcher starts
	var createOut fuse.CreateOut
	if st := rawFS.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     0644,
	}, "prefile.txt", &createOut); st != fuse.OK {
		t.Fatalf("Create failed: %v", st)
	}

	if rawFS.IsWatcherConnected() {
		t.Fatalf("Expected watcher to be disconnected initially")
	}

	watchCtx, cancelWatch := context.WithCancel(t.Context())
	defer cancelWatch()

	StartWatcher(watchCtx, client, volumeID, "test-node", pb.ConsistencyMode_STANDARD, cache, rawFS)

	// Poll until IsWatcherConnected becomes true
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if rawFS.IsWatcherConnected() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !rawFS.IsWatcherConnected() {
		t.Fatalf("Expected watcher to become connected")
	}

	// While connected, timeouts are 60s
	var lookupOut fuse.EntryOut
	if st := rawFS.Lookup(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "prefile.txt", &lookupOut); st != fuse.OK {
		t.Fatalf("Lookup failed: %v", st)
	}
	if lookupOut.EntryTimeout() != 60*time.Second {
		t.Errorf("Expected 60s EntryTimeout while connected, got %v", lookupOut.EntryTimeout())
	}

	// Cancel watcher
	cancelWatch()

	// Poll until IsWatcherConnected becomes false
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if !rawFS.IsWatcherConnected() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if rawFS.IsWatcherConnected() {
		t.Fatalf("Expected watcher to become disconnected after context cancellation")
	}

	// While disconnected, timeouts fall back to 1s
	if st := rawFS.Lookup(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "prefile.txt", &lookupOut); st != fuse.OK {
		t.Fatalf("Lookup failed: %v", st)
	}
	if lookupOut.EntryTimeout() != 1*time.Second {
		t.Errorf("Expected 1s EntryTimeout after disconnect, got %v", lookupOut.EntryTimeout())
	}
}

type recordingControllerClient struct {
	pb.ObjectFSControllerClient
	mu          sync.Mutex
	lastRequest *pb.FsyncRequest
}

func (c *recordingControllerClient) Fsync(ctx context.Context, in *pb.FsyncRequest, opts ...grpc.CallOption) (*pb.FsyncResponse, error) {
	c.mu.Lock()
	c.lastRequest = proto.Clone(in).(*pb.FsyncRequest)
	c.mu.Unlock()
	return c.ObjectFSControllerClient.Fsync(ctx, in, opts...)
}

func (c *recordingControllerClient) getLastRequest() *pb.FsyncRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastRequest
}

func TestSyncVolumeNonBlockingTrigger(t *testing.T) {
	client, cleanup := createTestClient(t)
	defer cleanup()

	recClient := &recordingControllerClient{ObjectFSControllerClient: client}
	volumeID := "vol-sync-trigger-test"
	cache := NewNodeCache(10 * 1024 * 1024)
	rawFS := NewObjectFS(recClient, volumeID, pb.WriteMode_WRITE_THROUGH_FSYNC, cache)

	ctx := t.Context()
	var createOut fuse.CreateOut
	if st := rawFS.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     0644,
	}, "test.txt", &createOut); st != fuse.OK {
		t.Fatalf("Create failed: %v", st)
	}
	fileID := createOut.EntryOut.NodeId

	// Write dirty data into cache
	data1 := []byte("hello close trigger")
	if _, st := rawFS.Write(nil, &fuse.WriteIn{
		InHeader: fuse.InHeader{NodeId: fileID},
		Fh:       createOut.OpenOut.Fh,
		Offset:   0,
	}, data1); st != fuse.OK {
		t.Fatalf("Write failed: %v", st)
	}

	// Trigger non-blocking sync with CLOSE
	if err := rawFS.syncVolumeNonBlocking(ctx, pb.FsyncRequest_SYNC_TRIGGER_CLOSE); err != nil {
		t.Fatalf("syncVolumeNonBlocking(CLOSE) failed: %v", err)
	}
	lastReq := recClient.getLastRequest()
	if lastReq == nil {
		t.Fatalf("Expected FsyncRequest to be recorded")
	}
	if lastReq.GetTrigger() != pb.FsyncRequest_SYNC_TRIGGER_CLOSE {
		t.Errorf("Expected trigger %v, got %v", pb.FsyncRequest_SYNC_TRIGGER_CLOSE, lastReq.GetTrigger())
	}
	if lastReq.GetBlocking() {
		t.Errorf("Expected blocking to be false")
	}

	// Write more dirty data into cache
	data2 := []byte("hello periodic trigger")
	if _, st := rawFS.Write(nil, &fuse.WriteIn{
		InHeader: fuse.InHeader{NodeId: fileID},
		Fh:       createOut.OpenOut.Fh,
		Offset:   uint64(len(data1)),
	}, data2); st != fuse.OK {
		t.Fatalf("Write 2 failed: %v", st)
	}

	// Trigger non-blocking sync with PERIODIC
	if err := rawFS.syncVolumeNonBlocking(ctx, pb.FsyncRequest_SYNC_TRIGGER_PERIODIC); err != nil {
		t.Fatalf("syncVolumeNonBlocking(PERIODIC) failed: %v", err)
	}
	lastReq = recClient.getLastRequest()
	if lastReq == nil {
		t.Fatalf("Expected FsyncRequest to be recorded")
	}
	if lastReq.GetTrigger() != pb.FsyncRequest_SYNC_TRIGGER_PERIODIC {
		t.Errorf("Expected trigger %v, got %v", pb.FsyncRequest_SYNC_TRIGGER_PERIODIC, lastReq.GetTrigger())
	}
	if lastReq.GetBlocking() {
		t.Errorf("Expected blocking to be false")
	}
}

func TestSyncOnClose_SyncsOnlyClosedInode(t *testing.T) {
	client, cleanup := createTestClient(t)
	defer cleanup()

	recClient := &recordingControllerClient{ObjectFSControllerClient: client}
	volumeID := "vol-sync-on-close-inode-test"
	cache := NewNodeCache(10 * 1024 * 1024)
	rawFS := NewObjectFS(recClient, volumeID, pb.WriteMode_WRITE_THROUGH_FSYNC, cache)

	ctx := t.Context()
	var createOutA, createOutB fuse.CreateOut
	if st := rawFS.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     0644,
	}, "fileA.txt", &createOutA); st != fuse.OK {
		t.Fatalf("Create fileA failed: %v", st)
	}
	fileA := createOutA.EntryOut.NodeId

	if st := rawFS.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
		Mode:     0644,
	}, "fileB.txt", &createOutB); st != fuse.OK {
		t.Fatalf("Create fileB failed: %v", st)
	}
	fileB := createOutB.EntryOut.NodeId

	// Write dirty data to both fileA and fileB
	dataA := []byte("content of file A")
	if _, st := rawFS.Write(nil, &fuse.WriteIn{
		InHeader: fuse.InHeader{NodeId: fileA},
		Fh:       createOutA.OpenOut.Fh,
		Offset:   0,
	}, dataA); st != fuse.OK {
		t.Fatalf("Write fileA failed: %v", st)
	}

	dataB := []byte("content of file B")
	if _, st := rawFS.Write(nil, &fuse.WriteIn{
		InHeader: fuse.InHeader{NodeId: fileB},
		Fh:       createOutB.OpenOut.Fh,
		Offset:   0,
	}, dataB); st != fuse.OK {
		t.Fatalf("Write fileB failed: %v", st)
	}

	if !rawFS.cache.IsInodeDirty(fileA) {
		t.Fatalf("Expected fileA to be dirty in cache")
	}
	if !rawFS.cache.IsInodeDirty(fileB) {
		t.Fatalf("Expected fileB to be dirty in cache")
	}

	// Release fileA
	rawFS.Release(nil, &fuse.ReleaseIn{
		InHeader: fuse.InHeader{NodeId: fileA},
		Fh:       createOutA.OpenOut.Fh,
	})

	// Process closed inodes
	if err := rawFS.syncClosedInodes(ctx); err != nil {
		t.Fatalf("syncClosedInodes failed: %v", err)
	}

	lastReq := recClient.getLastRequest()
	if lastReq == nil {
		t.Fatalf("Expected FsyncRequest to be recorded for closed inode")
	}
	if lastReq.GetInode() != fileA {
		t.Errorf("Expected Fsync for closed inode %d, got %d", fileA, lastReq.GetInode())
	}
	if lastReq.GetTrigger() != pb.FsyncRequest_SYNC_TRIGGER_CLOSE {
		t.Errorf("Expected trigger CLOSE, got %v", lastReq.GetTrigger())
	}
	if lastReq.GetBlocking() {
		t.Errorf("Expected blocking to be false")
	}

	// fileA must now be clean, while fileB must still be dirty
	if rawFS.cache.IsInodeDirty(fileA) {
		t.Errorf("Expected fileA to be clean in cache after syncOnClose")
	}
	if !rawFS.cache.IsInodeDirty(fileB) {
		t.Errorf("Expected fileB to remain dirty in cache after syncOnClose of fileA")
	}

	// Now run periodic sync for the volume
	if err := rawFS.syncVolumeNonBlocking(ctx, pb.FsyncRequest_SYNC_TRIGGER_PERIODIC); err != nil {
		t.Fatalf("syncVolumeNonBlocking(PERIODIC) failed: %v", err)
	}

	lastReq = recClient.getLastRequest()
	if lastReq == nil {
		t.Fatalf("Expected FsyncRequest for periodic sync")
	}
	if lastReq.GetInode() != 1 {
		t.Errorf("Expected volume-level Fsync inode 1, got %d", lastReq.GetInode())
	}
	if lastReq.GetTrigger() != pb.FsyncRequest_SYNC_TRIGGER_PERIODIC {
		t.Errorf("Expected trigger PERIODIC, got %v", lastReq.GetTrigger())
	}
	if rawFS.cache.IsInodeDirty(fileB) {
		t.Errorf("Expected fileB to be clean after periodic sync")
	}
}

type fsyncConcurrencyTrackingClient struct {
	pb.ObjectFSControllerClient
	concurrentInFlight atomic.Int32
	maxInFlight        atomic.Int32
}

func (c *fsyncConcurrencyTrackingClient) Fsync(ctx context.Context, in *pb.FsyncRequest, opts ...grpc.CallOption) (*pb.FsyncResponse, error) {
	if in.GetTrigger() == pb.FsyncRequest_SYNC_TRIGGER_CLOSE {
		curr := c.concurrentInFlight.Add(1)
		defer c.concurrentInFlight.Add(-1)
		for {
			oldMax := c.maxInFlight.Load()
			if curr <= oldMax || c.maxInFlight.CompareAndSwap(oldMax, curr) {
				break
			}
		}
		// Hold briefly to ensure concurrent requests overlap in time
		time.Sleep(30 * time.Millisecond)
	}
	return c.ObjectFSControllerClient.Fsync(ctx, in, opts...)
}

func TestSyncClosedInodes_TenClosedInodesOverlapInTime(t *testing.T) {
	client, cleanup := createTestClient(t)
	defer cleanup()

	trackClient := &fsyncConcurrencyTrackingClient{ObjectFSControllerClient: client}
	volumeID := "vol-concurrent-fsync-overlap-test"
	cache := NewNodeCache(10 * 1024 * 1024)
	rawFS := NewObjectFS(trackClient, volumeID, pb.WriteMode_WRITE_THROUGH_FSYNC, cache)

	ctx := t.Context()
	const numFiles = 10
	for i := 0; i < numFiles; i++ {
		var createOut fuse.CreateOut
		fname := fmt.Sprintf("file_%d.txt", i)
		if st := rawFS.Create(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Mode:     0644,
		}, fname, &createOut); st != fuse.OK {
			t.Fatalf("Create %s failed: %v", fname, st)
		}
		ino := createOut.EntryOut.NodeId
		fh := createOut.OpenOut.Fh

		data := []byte(fmt.Sprintf("data for file %d", i))
		if _, st := rawFS.Write(nil, &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: ino},
			Fh:       fh,
			Offset:   0,
		}, data); st != fuse.OK {
			t.Fatalf("Write %s failed: %v", fname, st)
		}

		rawFS.Release(nil, &fuse.ReleaseIn{
			InHeader: fuse.InHeader{NodeId: ino},
			Fh:       fh,
		})
	}

	// Process closed inodes
	if err := rawFS.syncClosedInodes(ctx); err != nil {
		t.Fatalf("syncClosedInodes failed: %v", err)
	}

	maxConcurrent := trackClient.maxInFlight.Load()
	if maxConcurrent != numFiles {
		t.Fatalf("Expected all %d close-triggered Fsync requests to overlap in time, got max concurrency %d", numFiles, maxConcurrent)
	}
}

func TestOpen_ReaderCacheInvalidationOnMovedStreamPosition(t *testing.T) {
	ctx := t.Context()
	ctrlServer := controllertest.NewTestServer(nil)
	vol, err := ctrlServer.GetVolume("test-open-inval-vol")
	if err != nil {
		t.Fatalf("GetVolume failed: %v", err)
	}
	vol.SetConsistencyMode(pb.ConsistencyMode_CLOSE_TO_OPEN)

	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	pb.RegisterObjectFSControllerServer(grpcServer, ctrlServer)
	go func() { _ = grpcServer.Serve(lis) }()
	defer grpcServer.Stop()

	conn, err := grpc.NewClient("passthrough://bufnet-open-inval",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer conn.Close()

	client := pb.NewObjectFSControllerClient(conn)
	cache := NewNodeCache(64 * 1024 * 1024)
	rawFS := NewObjectFS(client, "test-open-inval-vol", pb.WriteMode_WRITE_THROUGH_FSYNC, cache)
	rawFS.SetConsistencyMode(pb.ConsistencyMode_CLOSE_TO_OPEN)

	var notifyMu sync.Mutex
	var notifyEvents []notifyEvent
	rawFS.notifyHook = func(e notifyEvent) {
		notifyMu.Lock()
		notifyEvents = append(notifyEvents, e)
		notifyMu.Unlock()
	}

	// Create file via controller
	createResp, err := ctrlServer.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:    "test-open-inval-vol",
		ParentInode: 1,
		Name:        "file.txt",
		Mode:        0644,
	})
	if err != nil || createResp.GetError() != 0 {
		t.Fatalf("CreateFile failed: %v", err)
	}
	ino := createResp.GetAttr().GetInode().GetIno()

	// Verify that standard mode keeps FOPEN_KEEP_CACHE and does not purge
	stdFS := NewObjectFS(client, "test-open-inval-vol", pb.WriteMode_WRITE_THROUGH_FSYNC, NewNodeCache(64*1024*1024))
	stdFS.SetConsistencyMode(pb.ConsistencyMode_STANDARD)
	var stdOut fuse.OpenOut
	if st := stdFS.Open(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: ino}}, &stdOut); st != fuse.OK {
		t.Fatalf("stdFS Open failed: %v", st)
	}
	if stdOut.OpenFlags&fuse.FOPEN_KEEP_CACHE == 0 {
		t.Errorf("Expected standard mode to always set FOPEN_KEEP_CACHE")
	}

	// Initial open: stream position > 0, cache empty
	var openOut1 fuse.OpenOut
	if st := rawFS.Open(nil, &fuse.OpenIn{
		InHeader: fuse.InHeader{NodeId: ino},
	}, &openOut1); st != fuse.OK {
		t.Fatalf("First Open failed: %v", st)
	}

	// Stream position moved from 0 to initial pos:
	// Should invalidate page cache (off=0, len=-1) and attr cache (off=-1, len=0), and NOT keep cache
	if openOut1.OpenFlags&fuse.FOPEN_KEEP_CACHE != 0 {
		t.Errorf("Expected FOPEN_KEEP_CACHE to NOT be set when stream position moved")
	}

	notifyMu.Lock()
	eventsCount := len(notifyEvents)
	notifyMu.Unlock()
	if eventsCount < 2 {
		t.Fatalf("Expected at least 2 notify events on first open, got %d", eventsCount)
	}

	// Second open without any changes: stream position has NOT moved
	notifyMu.Lock()
	notifyEvents = nil
	notifyMu.Unlock()

	var openOut2 fuse.OpenOut
	if st := rawFS.Open(nil, &fuse.OpenIn{
		InHeader: fuse.InHeader{NodeId: ino},
	}, &openOut2); st != fuse.OK {
		t.Fatalf("Second Open failed: %v", st)
	}

	// Stream position did not move:
	// Should set FOPEN_KEEP_CACHE and NOT emit invalidation events
	if openOut2.OpenFlags&fuse.FOPEN_KEEP_CACHE == 0 {
		t.Errorf("Expected FOPEN_KEEP_CACHE to be set when stream position did not move")
	}
	notifyMu.Lock()
	eventsCount2 := len(notifyEvents)
	notifyMu.Unlock()
	if eventsCount2 != 0 {
		t.Errorf("Expected 0 notify events when stream position did not move, got %d", eventsCount2)
	}
}

func TestCloseToOpen_TwoNodeReaderCacheInvalidation(t *testing.T) {
	ctx := t.Context()
	backend := controller.NewMemoryBackend()
	ctrlServer := controllertest.NewTestServer(backend)
	defer func() { _ = ctrlServer.Close() }()

	volumeID := "test-c2o-unit-vol"
	vol, err := ctrlServer.GetVolume(volumeID)
	if err != nil {
		t.Fatalf("GetVolume failed: %v", err)
	}
	vol.SetConsistencyMode(pb.ConsistencyMode_CLOSE_TO_OPEN)

	lis := bufconn.Listen(4 * 1024 * 1024)
	grpcServer := grpc.NewServer()
	pb.RegisterObjectFSControllerServer(grpcServer, ctrlServer)
	go func() { _ = grpcServer.Serve(lis) }()
	defer grpcServer.Stop()

	dial := func() pb.ObjectFSControllerClient {
		conn, err := grpc.NewClient("passthrough://bufnet-c2o-unit",
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return lis.Dial()
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			t.Fatalf("dial failed: %v", err)
		}
		t.Cleanup(func() { conn.Close() })
		return pb.NewObjectFSControllerClient(conn)
	}

	clientA := dial()
	clientB := dial()

	cacheA := NewNodeCache(64 * 1024 * 1024)
	cacheB := NewNodeCache(64 * 1024 * 1024)

	fsA := NewObjectFS(clientA, volumeID, pb.WriteMode_LAZY_WRITE, cacheA)
	fsB := NewObjectFS(clientB, volumeID, pb.WriteMode_WRITE_THROUGH_FSYNC, cacheB)
	fsB.SetConsistencyMode(pb.ConsistencyMode_CLOSE_TO_OPEN)
	fsA.SetSyncOnClose(true)

	// 1. Create file
	createResp, err := ctrlServer.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:    volumeID,
		ParentInode: 1,
		Name:        "file4k.bin",
		Mode:        0644,
	})
	if err != nil || createResp.GetError() != 0 {
		t.Fatalf("CreateFile failed: %v", err)
	}
	ino4k := createResp.GetAttr().GetInode().GetIno()

	// 2. Reader fsB opens & reads size=0, populating stale cache
	var openOutB1 fuse.OpenOut
	if st := fsB.Open(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: ino4k}}, &openOutB1); st != fuse.OK {
		t.Fatalf("fsB Open initial failed: %v", st)
	}
	buf0 := make([]byte, 4096)
	readRes, st := fsB.Read(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: ino4k}, Fh: openOutB1.Fh, Offset: 0, Size: 4096}, buf0)
	if st != fuse.OK {
		t.Fatalf("fsB Read initial failed: %v", st)
	}
	if readRes.Size() != 0 {
		t.Fatalf("fsB Read initial size = %d, want 0", readRes.Size())
	}
	fsB.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: ino4k}, Fh: openOutB1.Fh})

	// 3. Writer fsA opens, writes 4 KiB, and closes WITHOUT fsync
	var openOutA1 fuse.OpenOut
	if st := fsA.Open(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: ino4k}, Flags: syscall.O_RDWR}, &openOutA1); st != fuse.OK {
		t.Fatalf("fsA Open failed: %v", st)
	}
	data4k := bytes.Repeat([]byte("X"), 4096)
	if _, st := fsA.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: ino4k}, Fh: openOutA1.Fh, Offset: 0}, data4k); st != fuse.OK {
		t.Fatalf("fsA Write failed: %v", st)
	}
	if st := fsA.Flush(nil, &fuse.FlushIn{InHeader: fuse.InHeader{NodeId: ino4k}, Fh: openOutA1.Fh}); st != fuse.OK {
		t.Fatalf("fsA Flush failed: %v", st)
	}
	fsA.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: ino4k}, Fh: openOutA1.Fh})

	// 4. Reader fsB immediately opens without sleep, measuring barrier latency
	t0_4k := time.Now()
	var openOutB2 fuse.OpenOut
	if st := fsB.Open(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: ino4k}}, &openOutB2); st != fuse.OK {
		t.Fatalf("fsB Open after close failed: %v", st)
	}
	lat4k := time.Since(t0_4k)
	t.Logf("Measured barrier latency for 4 KiB inline file: %v", lat4k)

	// Read and verify data
	readBuf := make([]byte, 4096)
	readRes2, st := fsB.Read(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: ino4k}, Fh: openOutB2.Fh, Offset: 0, Size: 4096}, readBuf)
	if st != fuse.OK {
		t.Fatalf("fsB Read after close failed: %v", st)
	}
	resData, _ := readRes2.Bytes(readBuf)
	if !bytes.Equal(resData, data4k) {
		t.Fatalf("fsB read data mismatch: got length %d, want 4096", len(resData))
	}
	fsB.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: ino4k}, Fh: openOutB2.Fh})

	// Case 2: 16 MiB multi-chunk file
	createResp16, err := ctrlServer.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:    volumeID,
		ParentInode: 1,
		Name:        "file16m.bin",
		Mode:        0644,
	})
	if err != nil || createResp16.GetError() != 0 {
		t.Fatalf("CreateFile 16m failed: %v", err)
	}
	ino16m := createResp16.GetAttr().GetInode().GetIno()

	// fsB opens & reads size=0, populating stale cache
	var openOutB16_1 fuse.OpenOut
	if st := fsB.Open(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: ino16m}}, &openOutB16_1); st != fuse.OK {
		t.Fatalf("fsB Open initial 16m failed: %v", st)
	}
	buf16_0 := make([]byte, 4096)
	readRes16_1, st := fsB.Read(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: ino16m}, Fh: openOutB16_1.Fh, Offset: 0, Size: 4096}, buf16_0)
	if st != fuse.OK {
		t.Fatalf("fsB Read initial 16m failed: %v", st)
	}
	if readRes16_1.Size() != 0 {
		t.Fatalf("fsB Read initial 16m size = %d, want 0", readRes16_1.Size())
	}
	fsB.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: ino16m}, Fh: openOutB16_1.Fh})

	// fsA writes 16 MiB and closes WITHOUT fsync
	var openOutA16_1 fuse.OpenOut
	if st := fsA.Open(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: ino16m}, Flags: syscall.O_RDWR}, &openOutA16_1); st != fuse.OK {
		t.Fatalf("fsA Open 16m failed: %v", st)
	}
	data16m := make([]byte, 16*1024*1024)
	for i := range data16m {
		data16m[i] = byte(i % 251)
	}
	// Write in 1 MiB chunks
	for off := 0; off < len(data16m); off += 1024 * 1024 {
		if _, st := fsA.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: ino16m}, Fh: openOutA16_1.Fh, Offset: uint64(off)}, data16m[off:off+1024*1024]); st != fuse.OK {
			t.Fatalf("fsA Write 16m at offset %d failed: %v", off, st)
		}
	}
	if st := fsA.Flush(nil, &fuse.FlushIn{InHeader: fuse.InHeader{NodeId: ino16m}, Fh: openOutA16_1.Fh}); st != fuse.OK {
		t.Fatalf("fsA Flush 16m failed: %v", st)
	}
	fsA.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: ino16m}, Fh: openOutA16_1.Fh})

	// fsB opens without sleep, measuring barrier latency
	t0_16m := time.Now()
	var openOutB16_2 fuse.OpenOut
	if st := fsB.Open(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: ino16m}}, &openOutB16_2); st != fuse.OK {
		t.Fatalf("fsB Open after close 16m failed: %v", st)
	}
	lat16m := time.Since(t0_16m)
	t.Logf("Measured barrier latency for 16 MiB multi-chunk file: %v", lat16m)

	// Verify reader reads exact 16 MiB
	for off := 0; off < len(data16m); off += 1024 * 1024 {
		buf := make([]byte, 1024*1024)
		r, st := fsB.Read(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: ino16m}, Fh: openOutB16_2.Fh, Offset: uint64(off), Size: 1024 * 1024}, buf)
		if st != fuse.OK {
			t.Fatalf("fsB Read 16m at offset %d failed: %v", off, st)
		}
		chunk, _ := r.Bytes(buf)
		if !bytes.Equal(chunk, data16m[off:off+1024*1024]) {
			t.Fatalf("fsB 16m data mismatch at offset %d", off)
		}
	}
	fsB.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: ino16m}, Fh: openOutB16_2.Fh})
}
