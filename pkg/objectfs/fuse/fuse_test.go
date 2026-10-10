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
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/controller"
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

	// Now call Flush / Fsync on the FUSE layer
	if status := rawFS.Flush(nil, &fuse.FlushIn{InHeader: fuse.InHeader{NodeId: fileID}}); status != fuse.OK {
		t.Fatalf("Flush failed: %v", status)
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
	server := controller.NewServer(backend)
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
	server := controller.NewServer(backend)
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

	// 3. Variant: flush 64 KiB clean data at offset 0, write 4 KiB at offset 200 KiB without flushing,
	// evict/clear chunk 0 from cache, and read [0, 204 KiB).
	chunk0Data := bytes.Repeat([]byte{0x77}, 64*1024)
	written, status = rawFS.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: fileID}, Offset: 0}, chunk0Data)
	if status != fuse.OK || int(written) != len(chunk0Data) {
		t.Fatalf("Write chunk 0 failed: status=%v, written=%d", status, written)
	}
	if status := rawFS.Flush(nil, &fuse.FlushIn{InHeader: fuse.InHeader{NodeId: fileID}}); status != fuse.OK {
		t.Fatalf("Flush failed: %v", status)
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
	fs.fillEntryOut(entry, &entryOut)
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
	if out.Files != 10000000 {
		t.Errorf("expected 10000000 nominal files, got %d", out.Files)
	}
	// Used inodes = 1 (root dir), so free should be 10000000 - 1 = 9999999
	if out.Ffree != 9999999 {
		t.Errorf("expected 9999999 free inodes, got %d", out.Ffree)
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
	deadline := time.Now().Add(2 * time.Second)
	for {
		status = fs.StatFs(nil, &fuse.InHeader{}, &out2)
		if status != fuse.OK {
			t.Fatalf("StatFs returned %v", status)
		}
		if out2.Ffree == 9999998 && out2.Bfree == expectedFreeBlocks {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for StatFs to reflect write: got Ffree=%d (want 9999998), Bfree=%d (want %d)", out2.Ffree, out2.Bfree, expectedFreeBlocks)
		}
		time.Sleep(10 * time.Millisecond)
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
	ctrlServer := controller.NewServer(nil)
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
	ctrlServer := controller.NewServer(nil)
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

	StartWatcher(watchCtx, client, volumeID, "node-1", cache)
	time.Sleep(100 * time.Millisecond)

	// Stop grpc server to simulate controller restart / disconnection
	grpcServer.Stop()
	_ = lis.Close()

	// Wait for watcher to detect disconnect
	time.Sleep(200 * time.Millisecond)

	// Start new server and buffer listener
	lis2 := bufconn.Listen(1024 * 1024)
	grpcServer2 := grpc.NewServer()
	ctrlServer2 := controller.NewServer(nil)
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
	StartWatcher(watchCtx2, client2, volumeID, "node-1", cache)
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
	ctrlServer := controller.NewServer(nil)
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

	// SetAttr with FATTR_SIZE must fail because syncFileToService fails
	var setAttrOut fuse.AttrOut
	status := fs.SetAttr(nil, &fuse.SetAttrIn{
		SetAttrInCommon: fuse.SetAttrInCommon{
			InHeader: fuse.InHeader{NodeId: fileID},
			Valid:    fuse.FATTR_SIZE,
			Size:     0,
		},
	}, &setAttrOut)

	if status == fuse.OK {
		t.Fatalf("Expected SetAttr truncate to fail when syncFileToService fails, but got OK")
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
	// ground-truth oracle; background flushers (syncFileToService) and readers race
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
				_ = rawFS.syncFileToService(ctx, fileID)
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
	if err := rawFS.syncFileToService(ctx, fileID); err != nil {
		t.Fatalf("Final syncFileToService failed: %v", err)
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

type recordingClient struct {
	pb.ObjectFSControllerClient
	mu        sync.Mutex
	writeReqs []*pb.WriteFileRequest
	failWrite bool
}

func (c *recordingClient) WriteFile(ctx context.Context, in *pb.WriteFileRequest, opts ...grpc.CallOption) (*pb.WriteFileResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failWrite {
		return nil, status.Errorf(codes.Unavailable, "injected write failure")
	}
	c.writeReqs = append(c.writeReqs, proto.Clone(in).(*pb.WriteFileRequest))
	return c.ObjectFSControllerClient.WriteFile(ctx, in, opts...)
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
	if err := fs.syncFileToService(ctx, fileID); err != nil {
		t.Fatalf("syncFileToService failed: %v", err)
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
	if err := fs.syncFileToService(ctx, fileID); err != nil {
		t.Fatalf("syncFileToService failed: %v", err)
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
	if err := fs.syncFileToService(ctx, fileID); err != nil {
		t.Fatalf("syncFileToService failed: %v", err)
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
	if err := fs.syncFileToService(ctx, fileID); err != nil {
		t.Fatalf("syncFileToService failed: %v", err)
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
	err := fs.syncFileToService(ctx, fileID)
	if err == nil {
		t.Fatalf("Expected syncFileToService to fail when WriteFile fails, got nil")
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

	// When WriteFile is invoked by syncFileToService, race a new write to chunk 1
	writeHook = func() {
		// Mutate chunk 1 to version 2
		cache.WriteAt(fileID, cs, c1v2, time.Now())
		writeHook = nil // only race once
	}

	ctx := t.Context()
	if err := fs.syncFileToService(ctx, fileID); err != nil {
		t.Fatalf("syncFileToService failed: %v", err)
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
