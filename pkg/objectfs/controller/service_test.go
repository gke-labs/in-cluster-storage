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
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"iter"
	"net"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	sdsv1 "github.com/gke-labs/in-cluster-storage/pkg/api/sds/v1"
	walpb "github.com/gke-labs/in-cluster-storage/pkg/api/wal/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/blob"
	"github.com/gke-labs/in-cluster-storage/pkg/sds"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/record"
	"github.com/gke-labs/in-cluster-storage/pkg/wal"
	walbuffer "github.com/gke-labs/in-cluster-storage/pkg/wal/buffer"
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/klog/v2"
)

func mustGetVolume(t *testing.T, server *Server, volumeID string) *Volume {
	t.Helper()
	vol, err := server.GetVolume(volumeID)
	if err != nil {
		t.Fatalf("GetVolume %q failed: %v", volumeID, err)
	}
	return vol
}

func resolvePath(ctx context.Context, server *Server, volumeID, p string) (uint64, error) {
	vol, err := server.getOrCreateVolume(volumeID)
	if err != nil {
		return 0, err
	}
	return vol.ResolvePath(ctx, p)
}

func testGetAttr(ctx context.Context, server *Server, volumeID, p string) (*pb.GetAttrResponse, error) {
	ino, err := resolvePath(ctx, server, volumeID, p)
	if err != nil {
		return &pb.GetAttrResponse{Error: volErrToSyscall(err)}, nil
	}
	return server.GetAttr(ctx, &pb.GetAttrRequest{VolumeId: volumeID, Inode: ino})
}

func testReadFile(ctx context.Context, server *Server, volumeID, p string, offset, size int64) (*pb.ReadFileResponse, error) {
	ino, err := resolvePath(ctx, server, volumeID, p)
	if err != nil {
		return &pb.ReadFileResponse{Error: volErrToSyscall(err)}, nil
	}
	return server.ReadFile(ctx, &pb.ReadFileRequest{VolumeId: volumeID, Inode: ino, Offset: offset, Size: size})
}

func testWriteFile(ctx context.Context, server *Server, volumeID, p string, offset int64, data []byte, mode pb.WriteMode) (*pb.WriteFileResponse, error) {
	ino, err := resolvePath(ctx, server, volumeID, p)
	if err != nil {
		return &pb.WriteFileResponse{Error: volErrToSyscall(err)}, nil
	}
	return server.WriteFile(ctx, &pb.WriteFileRequest{VolumeId: volumeID, Inode: ino, Offset: offset, Data: data, WriteMode: mode})
}

func testTruncateFile(ctx context.Context, server *Server, volumeID, p string, size int64) (*pb.TruncateFileResponse, error) {
	ino, err := resolvePath(ctx, server, volumeID, p)
	if err != nil {
		return &pb.TruncateFileResponse{Error: volErrToSyscall(err)}, nil
	}
	return server.TruncateFile(ctx, &pb.TruncateFileRequest{VolumeId: volumeID, Inode: ino, Size: size})
}

func testReadDir(ctx context.Context, server *Server, volumeID, p string) (*pb.ReadDirResponse, error) {
	ino, err := resolvePath(ctx, server, volumeID, p)
	if err != nil {
		return &pb.ReadDirResponse{Error: volErrToSyscall(err)}, nil
	}
	return server.ReadDir(ctx, &pb.ReadDirRequest{VolumeId: volumeID, Inode: ino})
}

func testMkdir(ctx context.Context, server *Server, volumeID, p string, mode uint32, uid, gid uint32) (*pb.MkdirResponse, error) {
	p = cleanPath(p)
	parentPath := path.Dir(p)
	name := path.Base(p)
	parentIno, err := resolvePath(ctx, server, volumeID, parentPath)
	if err != nil {
		return &pb.MkdirResponse{Error: volErrToSyscall(err)}, nil
	}
	return server.Mkdir(ctx, &pb.MkdirRequest{VolumeId: volumeID, ParentInode: parentIno, Name: name, Mode: mode, Uid: uid, Gid: gid})
}

func testCreateFile(ctx context.Context, server *Server, volumeID, p string, mode uint32, initialContent []byte, uid, gid uint32) (*pb.CreateFileResponse, error) {
	p = cleanPath(p)
	parentPath := path.Dir(p)
	name := path.Base(p)
	parentIno, err := resolvePath(ctx, server, volumeID, parentPath)
	if err != nil {
		return &pb.CreateFileResponse{Error: volErrToSyscall(err)}, nil
	}
	return server.CreateFile(ctx, &pb.CreateFileRequest{VolumeId: volumeID, ParentInode: parentIno, Name: name, Mode: mode, InitialContent: initialContent, Uid: uid, Gid: gid})
}

func testUnlink(ctx context.Context, server *Server, volumeID, p string) (*pb.UnlinkResponse, error) {
	p = cleanPath(p)
	parentPath := path.Dir(p)
	name := path.Base(p)
	parentIno, err := resolvePath(ctx, server, volumeID, parentPath)
	if err != nil {
		return &pb.UnlinkResponse{Success: false, Error: volErrToSyscall(err)}, nil
	}
	return server.Unlink(ctx, &pb.UnlinkRequest{VolumeId: volumeID, ParentInode: parentIno, Name: name})
}

func testRmdir(ctx context.Context, server *Server, volumeID, p string) (*pb.RmdirResponse, error) {
	p = cleanPath(p)
	parentPath := path.Dir(p)
	name := path.Base(p)
	parentIno, err := resolvePath(ctx, server, volumeID, parentPath)
	if err != nil {
		return &pb.RmdirResponse{Success: false, Error: volErrToSyscall(err)}, nil
	}
	return server.Rmdir(ctx, &pb.RmdirRequest{VolumeId: volumeID, ParentInode: parentIno, Name: name})
}

func testRename(ctx context.Context, server *Server, volumeID, oldP, newP string) (*pb.RenameResponse, error) {
	oldP = cleanPath(oldP)
	newP = cleanPath(newP)
	oldParentIno, err := resolvePath(ctx, server, volumeID, path.Dir(oldP))
	if err != nil {
		return &pb.RenameResponse{Error: volErrToSyscall(err)}, nil
	}
	newParentIno, err := resolvePath(ctx, server, volumeID, path.Dir(newP))
	if err != nil {
		return &pb.RenameResponse{Error: volErrToSyscall(err)}, nil
	}
	return server.Rename(ctx, &pb.RenameRequest{
		VolumeId:       volumeID,
		OldParentInode: oldParentIno,
		OldName:        path.Base(oldP),
		NewParentInode: newParentIno,
		NewName:        path.Base(newP),
	})
}

func testSetAttr(ctx context.Context, server *Server, volumeID string, p string, mode, uid, gid *uint32, atime *time.Time, atimeNow bool, mtime *time.Time, mtimeNow bool, ctime *time.Time, ctimeNow bool) (*pb.SetAttrResponse, error) {
	ino, err := resolvePath(ctx, server, volumeID, p)
	if err != nil {
		return &pb.SetAttrResponse{Error: volErrToSyscall(err)}, nil
	}
	req := &pb.SetAttrRequest{
		VolumeId: volumeID,
		Inode:    ino,
		Mode:     mode,
		Uid:      uid,
		Gid:      gid,
		AtimeNow: atimeNow,
		MtimeNow: mtimeNow,
		CtimeNow: ctimeNow,
	}
	if atime != nil {
		req.Atime = timestamppb.New(*atime)
	}
	if mtime != nil {
		req.Mtime = timestamppb.New(*mtime)
	}
	if ctime != nil {
		req.Ctime = timestamppb.New(*ctime)
	}
	return server.SetAttr(ctx, req)
}

func volGetAttr(ctx context.Context, vol *Volume, p string) (*pb.EntryAttr, error) {
	ino, err := vol.ResolvePath(ctx, p)
	if err != nil {
		return nil, err
	}
	return vol.GetAttr(ctx, ino)
}

func volReadFile(ctx context.Context, vol *Volume, p string, offset, length int64) ([]byte, int64, string, error) {
	ino, err := vol.ResolvePath(ctx, p)
	if err != nil {
		return nil, 0, "", err
	}
	return vol.ReadFile(ctx, ino, offset, length)
}

func volWriteFile(ctx context.Context, vol *Volume, p string, offset int64, data []byte, mode pb.WriteMode) (int64, int64, time.Time, error) {
	ino, err := vol.ResolvePath(ctx, p)
	if err != nil {
		return 0, 0, time.Time{}, err
	}
	return vol.WriteFile(ctx, ino, offset, data, mode)
}

func volTruncateFile(ctx context.Context, vol *Volume, p string, size int64) (*pb.EntryAttr, error) {
	ino, err := vol.ResolvePath(ctx, p)
	if err != nil {
		return nil, err
	}
	return vol.TruncateFile(ctx, ino, size)
}

func volCreateFile(ctx context.Context, vol *Volume, p string, mode uint32, initialContent []byte, uid, gid uint32) (*pb.EntryAttr, error) {
	p = cleanPath(p)
	parentIno, err := vol.ResolvePath(ctx, path.Dir(p))
	if err != nil {
		return nil, err
	}
	return vol.CreateFile(ctx, parentIno, path.Base(p), mode, initialContent, uid, gid, 0)
}

// MustNewVolume creates a new Volume or fails the test if volume initialization fails.
// It is intended for tests and benchmarks.
func MustNewVolume(t testing.TB, volumeID string, backend ObjectStorageBackend, broadcaster *EventBroadcaster, opts ...VolumeOption) *Volume {
	t.Helper()
	v, err := NewVolume(volumeID, backend, broadcaster, opts...)
	if err != nil {
		t.Fatalf("failed to create volume %s: %v", volumeID, err)
	}
	return v
}

func TestControllerServiceOperations(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewServer(backend)
	volumeID := "test-vol-1"

	// 1. Root attribute
	rootAttr, err := server.GetAttr(ctx, &pb.GetAttrRequest{
		VolumeId: volumeID,
		Inode:    1,
	})
	if err != nil {
		t.Fatalf("Failed to get root attr: %v", err)
	}
	if !rootAttr.GetAttr().GetInode().GetIsDir() {
		t.Fatalf("Expected root to be directory")
	}

	// 2. Mkdir
	mkdirResp, err := server.Mkdir(ctx, &pb.MkdirRequest{
		VolumeId:    volumeID,
		ParentInode: 1,
		Name:        "subdir",
		Mode:        0755,
		Uid:         1001,
		Gid:         1002,
	})
	if err != nil {
		t.Fatalf("Failed to mkdir /subdir: %v", err)
	}
	if !mkdirResp.GetAttr().GetInode().GetIsDir() || mkdirResp.GetAttr().GetName() != "subdir" {
		t.Fatalf("Unexpected mkdir attr: %v", mkdirResp.GetAttr())
	}
	if mkdirResp.GetAttr().GetInode().GetUid() != 1001 || mkdirResp.GetAttr().GetInode().GetGid() != 1002 {
		t.Fatalf("Unexpected mkdir owner: uid=%d, gid=%d", mkdirResp.GetAttr().GetInode().GetUid(), mkdirResp.GetAttr().GetInode().GetGid())
	}
	subdirIno := mkdirResp.GetAttr().GetInode().GetIno()

	// 3. Create file
	createResp, err := server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:       volumeID,
		ParentInode:    subdirIno,
		Name:           "hello.txt",
		Mode:           0644,
		InitialContent: []byte("initial content"),
		Uid:            5001,
		Gid:            5002,
	})
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	if createResp.GetAttr().GetInode().GetSize() != int64(len("initial content")) {
		t.Fatalf("Unexpected file size: %d", createResp.GetAttr().GetInode().GetSize())
	}
	if createResp.GetAttr().GetInode().GetUid() != 5001 || createResp.GetAttr().GetInode().GetGid() != 5002 {
		t.Fatalf("Unexpected file owner: uid=%d, gid=%d", createResp.GetAttr().GetInode().GetUid(), createResp.GetAttr().GetInode().GetGid())
	}
	fileIno := createResp.GetAttr().GetInode().GetIno()

	// 4. Lookup
	lookupResp, err := server.Lookup(ctx, &pb.LookupRequest{
		VolumeId:    volumeID,
		ParentInode: subdirIno,
		Name:        "hello.txt",
	})
	if err != nil {
		t.Fatalf("Failed to lookup: %v", err)
	}
	if lookupResp.GetAttr().GetName() != "hello.txt" || lookupResp.GetAttr().GetInode().GetIno() != fileIno {
		t.Fatalf("Unexpected attr in lookup: %v", lookupResp.GetAttr())
	}
	if lookupResp.GetAttr().GetInode().GetUid() != 5001 || lookupResp.GetAttr().GetInode().GetGid() != 5002 {
		t.Fatalf("Unexpected owner in lookup: uid=%d, gid=%d", lookupResp.GetAttr().GetInode().GetUid(), lookupResp.GetAttr().GetInode().GetGid())
	}

	// 5. Read file
	readResp, err := server.ReadFile(ctx, &pb.ReadFileRequest{
		VolumeId: volumeID,
		Inode:    fileIno,
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
		Inode:     fileIno,
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

	// Fsync to commit staged writes
	if fsyncResp, err := server.Fsync(ctx, &pb.FsyncRequest{VolumeId: volumeID, Inode: fileIno, Blocking: true}); err != nil || !fsyncResp.GetSuccess() {
		t.Fatalf("Fsync failed: err=%v, resp=%v", err, fsyncResp)
	}

	// Read back modified
	readResp2, err := server.ReadFile(ctx, &pb.ReadFileRequest{
		VolumeId: volumeID,
		Inode:    fileIno,
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
		Inode:    subdirIno,
	})
	if err != nil {
		t.Fatalf("Failed to readdir: %v", err)
	}
	if len(readdirResp.Entries) != 1 || readdirResp.Entries[0].Name != "hello.txt" {
		t.Fatalf("Unexpected readdir entries: %v", readdirResp.Entries)
	}

	// 8. Rename
	renameResp, err := server.Rename(ctx, &pb.RenameRequest{
		VolumeId:       volumeID,
		OldParentInode: subdirIno,
		OldName:        "hello.txt",
		NewParentInode: subdirIno,
		NewName:        "renamed.txt",
	})
	if err != nil {
		t.Fatalf("Failed to rename: %v", err)
	}
	if renameResp.GetAttr().GetName() != "renamed.txt" {
		t.Fatalf("Unexpected rename attr: %v", renameResp.GetAttr())
	}

	// Verify old name not found in lookup
	oldResp, err := server.Lookup(ctx, &pb.LookupRequest{
		VolumeId:    volumeID,
		ParentInode: subdirIno,
		Name:        "hello.txt",
	})
	if err != nil {
		t.Fatalf("Lookup failed: %v", err)
	}
	if oldResp.GetError() != int32(syscall.ENOENT) {
		t.Fatalf("Expected old name to not exist after rename, got error %d", oldResp.GetError())
	}

	// 9. Truncate
	truncResp, err := server.TruncateFile(ctx, &pb.TruncateFileRequest{
		VolumeId: volumeID,
		Inode:    fileIno,
		Size:     7,
	})
	if err != nil {
		t.Fatalf("Failed to truncate: %v", err)
	}
	if truncResp.GetAttr().GetInode().GetSize() != 7 {
		t.Fatalf("Expected size 7 after truncate, got %d", truncResp.GetAttr().GetInode().GetSize())
	}

	// 10. Unlink & Rmdir
	_, err = server.Unlink(ctx, &pb.UnlinkRequest{
		VolumeId:    volumeID,
		ParentInode: subdirIno,
		Name:        "renamed.txt",
	})
	if err != nil {
		t.Fatalf("Failed to unlink: %v", err)
	}

	_, err = server.Rmdir(ctx, &pb.RmdirRequest{
		VolumeId:    volumeID,
		ParentInode: 1,
		Name:        "subdir",
	})
	if err != nil {
		t.Fatalf("Failed to rmdir: %v", err)
	}

	// Verify subdir gone
	goneResp, err := server.Lookup(ctx, &pb.LookupRequest{
		VolumeId:    volumeID,
		ParentInode: 1,
		Name:        "subdir",
	})
	if err != nil {
		t.Fatalf("Lookup failed: %v", err)
	}
	if goneResp.GetError() != int32(syscall.ENOENT) {
		t.Fatalf("Expected subdir to not exist after rmdir, got error %d", goneResp.GetError())
	}
}

func TestServerRmdirAndUnlinkErrorCodes(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewServer(backend)
	volumeID := "test-error-codes-vol"

	// Create /parent/child.txt
	mkdirResp, err := testMkdir(ctx, server, volumeID, "/parent", 0755, 0, 0)
	if err != nil || mkdirResp.GetError() != 0 {
		t.Fatalf("Mkdir failed: err=%v, resp=%v", err, mkdirResp)
	}

	createResp, err := testCreateFile(ctx, server, volumeID, "/parent/child.txt", 0644, nil, 0, 0)
	if err != nil || createResp.GetError() != 0 {
		t.Fatalf("CreateFile failed: err=%v, resp=%v", err, createResp)
	}

	// 1. Rmdir non-empty directory -> returns non-error gRPC response with error = ENOTEMPTY
	rmdirResp, err := testRmdir(ctx, server, volumeID, "/parent")
	if err != nil {
		t.Fatalf("Expected non-error gRPC response, got err: %v", err)
	}
	if rmdirResp.GetError() != int32(syscall.ENOTEMPTY) {
		t.Fatalf("Expected error code ENOTEMPTY (%d), got %d", syscall.ENOTEMPTY, rmdirResp.GetError())
	}
	if rmdirResp.GetSuccess() {
		t.Fatalf("Expected success to be false for non-empty rmdir")
	}

	// 2. Unlink directory -> returns non-error gRPC response with error = EISDIR
	unlinkResp, err := testUnlink(ctx, server, volumeID, "/parent")
	if err != nil {
		t.Fatalf("Expected non-error gRPC response, got err: %v", err)
	}
	if unlinkResp.GetError() != int32(syscall.EISDIR) {
		t.Fatalf("Expected error code EISDIR (%d), got %d", syscall.EISDIR, unlinkResp.GetError())
	}
	if unlinkResp.GetSuccess() {
		t.Fatalf("Expected success to be false for unlinking directory")
	}

	// 3. Rmdir regular file -> returns non-error gRPC response with error = ENOTDIR
	rmdirFileResp, err := testRmdir(ctx, server, volumeID, "/parent/child.txt")
	if err != nil {
		t.Fatalf("Expected non-error gRPC response, got err: %v", err)
	}
	if rmdirFileResp.GetError() != int32(syscall.ENOTDIR) {
		t.Fatalf("Expected error code ENOTDIR (%d), got %d", syscall.ENOTDIR, rmdirFileResp.GetError())
	}

	// 4. Rmdir nonexistent -> returns non-error gRPC response with error = ENOENT
	rmdirNoneResp, err := testRmdir(ctx, server, volumeID, "/nonexistent")
	if err != nil {
		t.Fatalf("Expected non-error gRPC response, got err: %v", err)
	}
	if rmdirNoneResp.GetError() != int32(syscall.ENOENT) {
		t.Fatalf("Expected error code ENOENT (%d), got %d", syscall.ENOENT, rmdirNoneResp.GetError())
	}

	// 5. Mkdir already exists -> returns non-error gRPC response with error = EEXIST
	mkdirExistResp, err := testMkdir(ctx, server, volumeID, "/parent", 0755, 0, 0)
	if err != nil {
		t.Fatalf("Expected non-error gRPC response, got err: %v", err)
	}
	if mkdirExistResp.GetError() != int32(syscall.EEXIST) {
		t.Fatalf("Expected error code EEXIST (%d), got %d", syscall.EEXIST, mkdirExistResp.GetError())
	}
}

func TestBackendPeriodicAndIncrementalFlush(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewServer(backend)
	volumeID := "test-flush-vol"

	// Create files
	_, err := testCreateFile(ctx, server, volumeID, "/file1.txt", 0644, []byte("file 1 initial data"), 0, 0)
	if err != nil {
		t.Fatalf("Failed to create file1: %v", err)
	}

	_, err = testCreateFile(ctx, server, volumeID, "/file2.txt", 0644, []byte("file 2 initial data"), 0, 0)
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

	// Verify snapshots exist in backend
	vol := mustGetVolume(t, server, volumeID)
	prefix := fmt.Sprintf("streams/%s/snapshots/", vol.StreamID().String())
	snaps, err := backend.ListObjects(ctx, "", prefix)
	if err != nil || len(snaps) == 0 {
		t.Fatalf("Expected snapshot in backend, got: %v (err=%v)", snaps, err)
	}

	// Verify metadata JSON file is NOT uploaded
	var metaBuf bytes.Buffer
	if err := backend.GetObject(ctx, volumeID, ".objectfs-metadata.json", 0, 0, &metaBuf); err == nil {
		t.Fatalf("Expected no .objectfs-metadata.json in backend, but found one")
	}

	// Incremental write: modify only file2
	_, err = testWriteFile(ctx, server, volumeID, "/file2.txt", 0, []byte("file 2 updated content!"), pb.WriteMode_WRITE_MODE_UNSPECIFIED)
	if err != nil {
		t.Fatalf("Failed to update file2: %v", err)
	}

	// Unlink file1
	_, err = testUnlink(ctx, server, volumeID, "/file1.txt")
	if err != nil {
		t.Fatalf("Failed to unlink file1: %v", err)
	}

	// Flush again
	if err := server.FlushAll(ctx); err != nil {
		t.Fatalf("Second FlushAll failed: %v", err)
	}

	// Test Recovery / LoadFromBackend
	// Create a new server pointing to the same backend
	newServer := NewServer(backend)
	readResp, err := testReadFile(ctx, newServer, volumeID, "/file2.txt", 0, 100)
	if err != nil {
		t.Fatalf("Failed to read file2 from recovered server: %v", err)
	}
	if string(readResp.GetData()) != "file 2 updated content!" {
		t.Fatalf("Expected recovered server to read 'file 2 updated content!', got: %q", string(readResp.GetData()))
	}

	// Verify unlinked file1 is not found on recovered server
	resp, err := testReadFile(ctx, newServer, volumeID, "/file1.txt", 0, 100)
	if err != nil || resp.GetError() == 0 {
		t.Fatalf("Expected file1 to be absent on recovered server, got resp=%v, err=%v", resp, err)
	}
}

func TestPeriodicFlusherLifecycle(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewServer(backend)
	volumeID := "test-periodic-vol"

	server.StartPeriodicFlush(ctx, 10*time.Millisecond)

	_, err := testCreateFile(ctx, server, volumeID, "/auto-flushed.txt", 0644, []byte("auto flushed data"), 0, 0)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	// Wait for periodic flusher to run
	time.Sleep(50 * time.Millisecond)

	server.StopPeriodicFlush()

	// Verify recovered server reads the auto-flushed file
	newServer := NewServer(backend)
	readResp, err := testReadFile(ctx, newServer, volumeID, "/auto-flushed.txt", 0, 100)
	if err != nil {
		t.Fatalf("Failed to read auto-flushed file from recovered server: %v", err)
	}
	if string(readResp.GetData()) != "auto flushed data" {
		t.Fatalf("Expected 'auto flushed data', got %q", string(readResp.GetData()))
	}
}

func TestConcurrentFlusherAndMutationsRace(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewServer(backend)
	volumeID := "test-flusher-race-vol"

	server.StartPeriodicFlush(ctx, 5*time.Millisecond)
	defer server.StopPeriodicFlush()

	for i := 0; i < 50; i++ {
		fileName := fmt.Sprintf("/file_%d.txt", i)
		_, err := testCreateFile(ctx, server, volumeID, fileName, 0644, []byte("data"), 0, 0)
		if err != nil {
			t.Fatalf("CreateFile %d failed: %v", i, err)
		}
	}
}

func TestControllerPushNotifications(t *testing.T) {
	ctx := t.Context()
	server := NewServer(nil)
	volumeID := "test-watch-vol"

	ch := server.broadcaster.Subscribe(volumeID)
	defer server.broadcaster.Unsubscribe(volumeID, ch)

	// Trigger create
	_, err := testCreateFile(ctx, server, volumeID, "/event-test.txt", 0644, []byte("event data"), 0, 0)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	select {
	case ev := <-ch:
		if ev.EventType != pb.WatchEventType_EVENT_CREATED || ev.GetInode() == 0 {
			t.Fatalf("Unexpected event received: %v", ev)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("Timed out waiting for push notification")
	}

	// Trigger modify and sync
	_, err = testWriteFile(ctx, server, volumeID, "/event-test.txt", 0, []byte("more data"), pb.WriteMode_WRITE_MODE_UNSPECIFIED)
	if err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}
	ino, _ := resolvePath(ctx, server, volumeID, "/event-test.txt")
	if _, err := server.Fsync(ctx, &pb.FsyncRequest{VolumeId: volumeID, Inode: ino, Blocking: true}); err != nil {
		t.Fatalf("Failed to fsync file: %v", err)
	}

	select {
	case ev := <-ch:
		if ev.EventType != pb.WatchEventType_EVENT_MODIFIED || ev.GetInode() == 0 {
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
			Inode:     uint64(i + 1),
			Name:      "file.txt",
		})
	}

	// Next broadcast should detect full channel and unsubscribe/close it asynchronously
	eb.Broadcast(volumeID, &pb.WatchVolumeResponse{
		EventType: pb.WatchEventType_EVENT_MODIFIED,
		Inode:     999,
		Name:      "overflow.txt",
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

func TestConcurrentAttributeMutationAndReadRace(t *testing.T) {
	ctx := t.Context()
	server := NewServer(nil, WithServerMetadataApplierBatchSize(5))
	volumeID := "test-race-vol"

	// Create test files
	const numFiles = 6
	var inos []uint64
	var names []string
	for i := 0; i < numFiles; i++ {
		name := fmt.Sprintf("race_file_%d.txt", i)
		createResp, err := server.CreateFile(ctx, &pb.CreateFileRequest{
			VolumeId:       volumeID,
			ParentInode:    1,
			Name:           name,
			Mode:           0644,
			InitialContent: []byte("initial-content"),
			Uid:            1000,
			Gid:            1000,
		})
		if err != nil || createResp.GetError() != 0 {
			t.Fatalf("CreateFile %s failed: %v", name, err)
		}
		inos = append(inos, createResp.GetAttr().GetInode().GetIno())
		names = append(names, name)
	}

	subCh1 := server.broadcaster.Subscribe(volumeID)
	defer server.broadcaster.Unsubscribe(volumeID, subCh1)
	subCh2 := server.broadcaster.Subscribe(volumeID)
	defer server.broadcaster.Unsubscribe(volumeID, subCh2)

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// 1. Subscriber goroutines reading and marshaling WatchVolume events
	for _, ch := range []chan *pb.WatchVolumeResponse{subCh1, subCh2} {
		wg.Add(1)
		go func(subCh chan *pb.WatchVolumeResponse) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				case ev, ok := <-subCh:
					if !ok {
						return
					}
					// Marshal event to simulate gRPC network serialization
					_, _ = proto.Marshal(ev)
					if ev.GetAttr() != nil && ev.GetAttr().GetInode() != nil {
						_ = ev.GetAttr().GetInode().GetSize()
						_ = ev.GetAttr().GetInode().GetMode()
						_ = ev.GetAttr().GetInode().GetMtime()
						_ = ev.GetAttr().GetInode().GetUid()
						_ = ev.GetAttr().GetInode().GetGid()
					}
				}
			}
		}(ch)
	}

	// 2. Reader goroutines doing parallel GetAttr, Lookup, and ReadDir
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					ino := inos[readerID%numFiles]
					name := names[readerID%numFiles]

					getResp, err := server.GetAttr(ctx, &pb.GetAttrRequest{
						VolumeId: volumeID,
						Inode:    ino,
					})
					if err == nil {
						_, _ = proto.Marshal(getResp)
						if getResp.GetAttr() != nil && getResp.GetAttr().GetInode() != nil {
							_ = getResp.GetAttr().GetInode().GetSize()
							_ = getResp.GetAttr().GetInode().GetMode()
							_ = getResp.GetAttr().GetInode().GetMtime()
						}
					}

					lookupResp, err := server.Lookup(ctx, &pb.LookupRequest{
						VolumeId:    volumeID,
						ParentInode: 1,
						Name:        name,
					})
					if err == nil {
						_, _ = proto.Marshal(lookupResp)
						if lookupResp.GetAttr() != nil && lookupResp.GetAttr().GetInode() != nil {
							_ = lookupResp.GetAttr().GetInode().GetSize()
							_ = lookupResp.GetAttr().GetInode().GetMode()
						}
					}

					readDirResp, err := server.ReadDir(ctx, &pb.ReadDirRequest{
						VolumeId: volumeID,
						Inode:    1,
					})
					if err == nil {
						_, _ = proto.Marshal(readDirResp)
						for _, ent := range readDirResp.GetEntries() {
							if ent.GetInode() != nil {
								_ = ent.GetInode().GetSize()
								_ = ent.GetInode().GetMode()
								_ = ent.GetInode().GetMtime()
							}
						}
					}
				}
			}
		}(r)
	}

	// 3. Writer goroutines doing concurrent SetAttr, WriteFile, and Rename on the same inodes
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			var counter uint32
			for {
				select {
				case <-stop:
					return
				default:
					counter++
					ino := inos[writerID%numFiles]
					mode := uint32(0600 + (counter % 64))
					uid := uint32(1000 + counter)
					gid := uint32(2000 + counter)
					mtime := time.Now()

					setResp, err := server.SetAttr(ctx, &pb.SetAttrRequest{
						VolumeId: volumeID,
						Inode:    ino,
						Mode:     &mode,
						Uid:      &uid,
						Gid:      &gid,
						Mtime:    timestamppb.New(mtime),
					})
					if err == nil {
						_, _ = proto.Marshal(setResp)
					}

					data := []byte(fmt.Sprintf("write-payload-%d-%d", writerID, counter))
					writeResp, err := server.WriteFile(ctx, &pb.WriteFileRequest{
						VolumeId:  volumeID,
						Inode:     ino,
						Offset:    0,
						Data:      data,
						WriteMode: pb.WriteMode_WRITE_MODE_UNSPECIFIED,
					})
					if err == nil {
						_, _ = proto.Marshal(writeResp)
					}

					// Concurrent Rename between alternate names
					origName := fmt.Sprintf("race_file_%d.txt", writerID)
					altName := fmt.Sprintf("race_file_%d_alt.txt", writerID)
					var fromName, toName string
					if counter%2 == 1 {
						fromName, toName = origName, altName
					} else {
						fromName, toName = altName, origName
					}
					renResp, err := server.Rename(ctx, &pb.RenameRequest{
						VolumeId:       volumeID,
						OldParentInode: 1,
						OldName:        fromName,
						NewParentInode: 1,
						NewName:        toName,
					})
					if err == nil {
						_, _ = proto.Marshal(renResp)
					}
				}
			}
		}(w)
	}

	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()
}

func TestOverlayEntryKeepsRowAsOfChange(t *testing.T) {
	ctx := t.Context()
	var faultErr atomic.Pointer[error]
	vol := MustNewVolume(t, "test-cow-overlay-vol", nil, NewEventBroadcaster(),
		WithVolumeMutationCheck(),
		WithApplierBatchSize(100),
		WithApplierFaultHook(func() error {
			if ep := faultErr.Load(); ep != nil {
				return *ep
			}
			return nil
		}),
	)
	defer vol.Close()

	// Stall the SQLite background applier so changes accumulate and remain in the overlay
	stalled := errors.New("stalled applier to hold overlay entries")
	faultErr.Store(&stalled)

	// Create initial file
	initialContent := []byte("initial-file-content")
	attr1, err := vol.CreateFile(ctx, 1, "testfile.txt", 0644, initialContent, 1000, 1000, 0)
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}
	ino := attr1.GetInode().GetIno()

	// Capture the row as created
	row1 := attr1.GetInode()
	if row1.GetSize() != int64(len(initialContent)) {
		t.Fatalf("expected size %d, got %d", len(initialContent), row1.GetSize())
	}
	if row1.GetMode()&0777 != 0644 {
		t.Fatalf("expected mode 0644, got %o", row1.GetMode()&0777)
	}

	// 1. Mutate attributes via SetAttr
	newMode := uint32(0755)
	newUid := uint32(2000)
	attr2, err := vol.SetAttr(ctx, ino, &newMode, &newUid, nil, nil, false, nil, false, nil, false)
	if err != nil {
		t.Fatalf("SetAttr failed: %v", err)
	}
	row2 := attr2.GetInode()

	// row1 MUST retain its previous values (copy-on-write)
	if row1.GetMode()&0777 != 0644 || row1.GetUid() != 1000 {
		t.Fatalf("row1 was mutated in place after SetAttr! mode=%o uid=%d", row1.GetMode()&0777, row1.GetUid())
	}
	if row2.GetMode()&0777 != 0755 || row2.GetUid() != 2000 {
		t.Fatalf("row2 unexpected values: mode=%o uid=%d", row2.GetMode()&0777, row2.GetUid())
	}

	// 2. Mutate file size via WriteFile and sync
	appendData := []byte(" extended content")
	_, newSize, _, err := vol.WriteFile(ctx, ino, int64(len(initialContent)), appendData, pb.WriteMode_WRITE_MODE_UNSPECIFIED)
	if err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}
	if err := vol.Fsync(ctx, ino); err != nil {
		t.Fatalf("Fsync failed: %v", err)
	}

	attr3, err := vol.GetAttr(ctx, ino)
	if err != nil {
		t.Fatalf("GetAttr failed: %v", err)
	}
	row3 := attr3.GetInode()

	if row1.GetSize() != int64(len(initialContent)) {
		t.Fatalf("row1 was mutated in place after WriteFile! size=%d", row1.GetSize())
	}
	if row2.GetSize() != int64(len(initialContent)) {
		t.Fatalf("row2 was mutated in place after WriteFile! size=%d", row2.GetSize())
	}
	if row3.GetSize() != newSize {
		t.Fatalf("row3 unexpected size: got %d, want %d", row3.GetSize(), newSize)
	}

	// 3. Mutate file size via TruncateFile
	attr4, err := vol.TruncateFile(ctx, ino, 5)
	if err != nil {
		t.Fatalf("TruncateFile failed: %v", err)
	}
	row4 := attr4.GetInode()

	if row1.GetSize() != int64(len(initialContent)) || row2.GetSize() != int64(len(initialContent)) || row3.GetSize() != newSize {
		t.Fatalf("previous rows mutated after TruncateFile! row1.size=%d row2.size=%d row3.size=%d",
			row1.GetSize(), row2.GetSize(), row3.GetSize())
	}
	if row4.GetSize() != 5 {
		t.Fatalf("row4 unexpected size: got %d, want 5", row4.GetSize())
	}

	// Clear fault and verify flush
	faultErr.Store(nil)
}

func TestMutationCheckCatchesInPlaceInodeModification(t *testing.T) {
	ctx := t.Context()
	vol := MustNewVolume(t, "test-mutation-check-vol", nil, NewEventBroadcaster(),
		WithVolumeMutationCheck(),
	)
	defer vol.Close()

	attr, err := vol.CreateFile(ctx, 1, "testfile.txt", 0644, []byte("hello"), 1000, 1000, 0)
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}
	ino := attr.GetInode().GetIno()

	// Ensure all queued changes are fully flushed to index
	if err := vol.metadataView.Flush(ctx); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	origMode := attr.GetInode().GetMode()

	// Illegally mutate the returned Inode row in place without mutate()
	attr.GetInode().Mode = 0777

	didPanic := false
	func() {
		defer func() {
			if r := recover(); r != nil {
				didPanic = true
			}
			// Restore original mode so any subsequent reads or Close() succeed
			attr.GetInode().Mode = origMode
		}()
		_, _ = vol.GetAttr(ctx, ino)
	}()

	if !didPanic {
		t.Fatalf("expected GetAttr to panic when Inode was mutated in place")
	}
}

func TestSnapshotCreationAndRecovery(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewServer(backend)
	volumeID := "test-snap-vol"

	// Create a nested directory hierarchy and files
	_, err := testMkdir(ctx, server, volumeID, "/data", 0755, 0, 0)
	if err != nil {
		t.Fatalf("Mkdir /data failed: %v", err)
	}

	_, err = testCreateFile(ctx, server, volumeID, "/data/file1.txt", 0644, []byte("file 1 content for snapshot"), 0, 0)
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}

	_, err = testCreateFile(ctx, server, volumeID, "/root-file.txt", 0644, []byte("root file content"), 0, 0)
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}

	largeData := bytes.Repeat([]byte("large content blob test "), 4096) // 96KB > chunkSize
	_, err = testCreateFile(ctx, server, volumeID, "/data/large.bin", 0644, largeData, 0, 0)
	if err != nil {
		t.Fatalf("CreateFile large.bin failed: %v", err)
	}

	// Create snapshot
	snapResp, err := server.CreateSnapshot(ctx, &pb.CreateSnapshotRequest{
		VolumeId: volumeID,
	})
	if err != nil {
		t.Fatalf("CreateSnapshot failed: %v", err)
	}
	snapName := snapResp.GetSnapshotName()
	if snapName == "" {
		t.Fatalf("Expected non-empty snapshot name, got empty")
	}

	// Verify backend object layout:
	// 1. Snapshot exists under streams/ in backend
	snapObjects, err := backend.ListObjects(ctx, "", "streams/")
	if err != nil || len(snapObjects) == 0 {
		t.Fatalf("Expected index snapshot in backend under streams/: %v", err)
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
	dirResp, err := testReadDir(ctx, newServer, volumeID, "/data")
	if err != nil {
		t.Fatalf("Recovered server ReadDir /data failed: %v", err)
	}
	if len(dirResp.Entries) != 2 {
		t.Fatalf("Unexpected entries in recovered /data: %v", dirResp.Entries)
	}

	// Read content from recovered server (verifying lazy blob download)
	readResp, err := testReadFile(ctx, newServer, volumeID, "/data/file1.txt", 0, 1024)
	if err != nil {
		t.Fatalf("Recovered server ReadFile failed: %v", err)
	}
	if string(readResp.Data) != "file 1 content for snapshot" {
		t.Fatalf("Recovered data mismatch: got %q", string(readResp.Data))
	}

	readLargeResp, err := testReadFile(ctx, newServer, volumeID, "/data/large.bin", 0, int64(len(largeData)))
	if err != nil {
		t.Fatalf("Recovered server ReadFile large.bin failed: %v", err)
	}
	if !bytes.Equal(readLargeResp.Data, largeData) {
		t.Fatalf("Recovered large data mismatch")
	}

	readRootResp, err := testReadFile(ctx, newServer, volumeID, "/root-file.txt", 0, 1024)
	if err != nil {
		t.Fatalf("Recovered server ReadFile root-file failed: %v", err)
	}
	if string(readRootResp.Data) != "root file content" {
		t.Fatalf("Recovered root data mismatch: got %q", string(readRootResp.Data))
	}
}

func TestSnapshotRollback(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewServer(backend)
	volumeID := "test-rollback-vol"

	// State 1: create v1
	_, err := testCreateFile(ctx, server, volumeID, "/doc.txt", 0644, []byte("version 1 data"), 0, 0)
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
	_, err = testWriteFile(ctx, server, volumeID, "/doc.txt", 0, []byte("version 2 data overwritten"), pb.WriteMode_WRITE_MODE_UNSPECIFIED)
	if err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	_, err = testCreateFile(ctx, server, volumeID, "/doc2.txt", 0644, []byte("version 2 second document"), 0, 0)
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
	readResp, err := testReadFile(ctx, server, volumeID, "/doc.txt", 0, 1024)
	if err != nil {
		t.Fatalf("ReadFile after rollback failed: %v", err)
	}
	if string(readResp.Data) != "version 1 data" {
		t.Fatalf("Expected 'version 1 data' after rollback, got %q", string(readResp.Data))
	}

	// /doc2.txt should not exist
	doc2Resp, err := testGetAttr(ctx, server, volumeID, "/doc2.txt")
	if err != nil {
		t.Fatalf("GetAttr failed: %v", err)
	}
	if doc2Resp.GetError() != int32(syscall.ENOENT) {
		t.Fatalf("Expected /doc2.txt to not exist after rollback to snap1, got error %d", doc2Resp.GetError())
	}

	// Now roll forward to snap2
	if err := server.RestoreSnapshot(ctx, volumeID, snap2); err != nil {
		t.Fatalf("RestoreSnapshot to %s failed: %v", snap2, err)
	}

	readResp2, err := testReadFile(ctx, server, volumeID, "/doc.txt", 0, 1024)
	if err != nil {
		t.Fatalf("ReadFile after restore to snap2 failed: %v", err)
	}
	if string(readResp2.Data) != "version 2 data overwritten" {
		t.Fatalf("Expected 'version 2 data overwritten' after restore to snap2, got %q", string(readResp2.Data))
	}
}

func TestSnapshotRestoreDurableAcrossRestart(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server1 := NewServer(backend)
	volumeID := "test-restore-durable-restart"

	// create /doc.txt "version 1 data"; CreateSnapshot s1
	_, err := testCreateFile(ctx, server1, volumeID, "/doc.txt", 0644, []byte("version 1 data"), 0, 0)
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}
	snapResp1, err := server1.CreateSnapshot(ctx, &pb.CreateSnapshotRequest{VolumeId: volumeID})
	if err != nil {
		t.Fatalf("CreateSnapshot s1 failed: %v", err)
	}
	s1 := snapResp1.GetSnapshotName()

	// write /doc.txt "version 2 data overwritten"; CreateSnapshot s2
	_, err = testWriteFile(ctx, server1, volumeID, "/doc.txt", 0, []byte("version 2 data overwritten"), pb.WriteMode_WRITE_MODE_UNSPECIFIED)
	if err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}
	_, err = server1.CreateSnapshot(ctx, &pb.CreateSnapshotRequest{VolumeId: volumeID})
	if err != nil {
		t.Fatalf("CreateSnapshot s2 failed: %v", err)
	}

	// RestoreSnapshot s1
	if err := server1.RestoreSnapshot(ctx, volumeID, s1); err != nil {
		t.Fatalf("RestoreSnapshot s1 failed: %v", err)
	}

	// in-process read: "version 1 data"
	readResp1, err := testReadFile(ctx, server1, volumeID, "/doc.txt", 0, 1024)
	if err != nil {
		t.Fatalf("in-process ReadFile failed: %v", err)
	}
	if string(readResp1.Data) != "version 1 data" {
		t.Fatalf("in-process read got %q, want %q", string(readResp1.Data), "version 1 data")
	}

	// NewServer(backend) read: "version 1 data" (not "version 2 data overwritten")
	server2 := NewServer(backend)
	readResp2, err := testReadFile(ctx, server2, volumeID, "/doc.txt", 0, 1024)
	if err != nil {
		t.Fatalf("recovered server ReadFile failed: %v", err)
	}
	if string(readResp2.Data) != "version 1 data" {
		t.Fatalf("recovered server read got %q, want %q", string(readResp2.Data), "version 1 data")
	}
}

func TestSnapshotRestoreWriteThenRestart(t *testing.T) {
	ctx := t.Context()
	walDir := t.TempDir()
	backend := NewMemoryBackend()
	volumeID := "test-restore-write-restart"

	server1 := NewServer(backend, WithServerWAL(walDir, "", walclient.Local))

	// Create v1
	_, err := testCreateFile(ctx, server1, volumeID, "/doc.txt", 0644, []byte("version 1 data"), 0, 0)
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}
	s1Resp, err := server1.CreateSnapshot(ctx, &pb.CreateSnapshotRequest{VolumeId: volumeID})
	if err != nil {
		t.Fatalf("CreateSnapshot s1 failed: %v", err)
	}
	s1 := s1Resp.GetSnapshotName()

	// Write pre-restore overwrite & create second doc
	_, err = testWriteFile(ctx, server1, volumeID, "/doc.txt", 0, []byte("version 2 data overwritten"), pb.WriteMode_WRITE_MODE_UNSPECIFIED)
	if err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}
	_, err = testCreateFile(ctx, server1, volumeID, "/pre_restore.txt", 0644, []byte("pre-restore only"), 0, 0)
	if err != nil {
		t.Fatalf("CreateFile pre_restore failed: %v", err)
	}
	_, err = server1.CreateSnapshot(ctx, &pb.CreateSnapshotRequest{VolumeId: volumeID})
	if err != nil {
		t.Fatalf("CreateSnapshot s2 failed: %v", err)
	}

	// RestoreSnapshot s1
	if err := server1.RestoreSnapshot(ctx, volumeID, s1); err != nil {
		t.Fatalf("RestoreSnapshot s1 failed: %v", err)
	}

	// Post-restore write: modify doc.txt and create post_restore.txt
	_, err = testWriteFile(ctx, server1, volumeID, "/doc.txt", 0, []byte("version 3 data post-restore"), pb.WriteMode_WRITE_MODE_UNSPECIFIED)
	if err != nil {
		t.Fatalf("WriteFile post-restore failed: %v", err)
	}
	_, err = testCreateFile(ctx, server1, volumeID, "/post_restore.txt", 0644, []byte("post-restore created"), 0, 0)
	if err != nil {
		t.Fatalf("CreateFile post_restore failed: %v", err)
	}

	if err := server1.FlushAll(ctx); err != nil {
		t.Fatalf("FlushAll failed: %v", err)
	}
	_ = server1.Close()

	// Restart controller from backend + WAL
	server2 := NewServer(backend, WithServerWAL(walDir, "", walclient.Local))
	defer func() { _ = server2.Close() }()

	// Read /doc.txt -> should be post-restore write ("version 3 data post-restore")
	readDoc, err := testReadFile(ctx, server2, volumeID, "/doc.txt", 0, 1024)
	if err != nil {
		t.Fatalf("ReadFile /doc.txt on recovered server failed: %v", err)
	}
	if string(readDoc.Data) != "version 3 data post-restore" {
		t.Fatalf("Expected 'version 3 data post-restore', got %q", string(readDoc.Data))
	}

	// /post_restore.txt MUST be present
	readPost, err := testReadFile(ctx, server2, volumeID, "/post_restore.txt", 0, 1024)
	if err != nil {
		t.Fatalf("ReadFile /post_restore.txt failed: %v", err)
	}
	if string(readPost.Data) != "post-restore created" {
		t.Fatalf("Expected 'post-restore created', got %q", string(readPost.Data))
	}

	// /pre_restore.txt MUST NOT be present (pre-restore write was rolled back)
	preAttr, err := testGetAttr(ctx, server2, volumeID, "/pre_restore.txt")
	if err != nil {
		t.Fatalf("GetAttr /pre_restore.txt failed: %v", err)
	}
	if preAttr.GetError() != int32(syscall.ENOENT) {
		t.Fatalf("Expected /pre_restore.txt to not exist after restart, got error %d", preAttr.GetError())
	}
}

func TestSnapshotRestoreRebuildFromLog(t *testing.T) {
	ctx := t.Context()
	walDir := t.TempDir()
	backend := NewMemoryBackend()
	volumeID := "test-restore-rebuild-log"

	server1 := NewServer(backend, WithServerWAL(walDir, "", walclient.Local))

	// 1. Create file & snapshot s1
	_, err := testCreateFile(ctx, server1, volumeID, "/file1.txt", 0644, []byte("file 1 initial"), 0, 0)
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}
	s1Resp, err := server1.CreateSnapshot(ctx, &pb.CreateSnapshotRequest{VolumeId: volumeID})
	if err != nil {
		t.Fatalf("CreateSnapshot s1 failed: %v", err)
	}
	s1 := s1Resp.GetSnapshotName()

	// 2. Pre-restore write & snapshot s2
	_, err = testWriteFile(ctx, server1, volumeID, "/file1.txt", 0, []byte("file 1 overwritten in s2"), pb.WriteMode_WRITE_MODE_UNSPECIFIED)
	if err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}
	_, err = testCreateFile(ctx, server1, volumeID, "/temp.txt", 0644, []byte("temporary"), 0, 0)
	if err != nil {
		t.Fatalf("CreateFile temp failed: %v", err)
	}
	_, err = server1.CreateSnapshot(ctx, &pb.CreateSnapshotRequest{VolumeId: volumeID})
	if err != nil {
		t.Fatalf("CreateSnapshot s2 failed: %v", err)
	}

	// 3. Restore to s1
	if err := server1.RestoreSnapshot(ctx, volumeID, s1); err != nil {
		t.Fatalf("RestoreSnapshot s1 failed: %v", err)
	}

	// 4. Post-restore writes
	_, err = testWriteFile(ctx, server1, volumeID, "/file1.txt", 0, []byte("file 1 post-restore write"), pb.WriteMode_WRITE_MODE_UNSPECIFIED)
	if err != nil {
		t.Fatalf("WriteFile post-restore failed: %v", err)
	}
	_, err = testCreateFile(ctx, server1, volumeID, "/final.txt", 0644, []byte("final file"), 0, 0)
	if err != nil {
		t.Fatalf("CreateFile final failed: %v", err)
	}

	if err := server1.FlushAll(ctx); err != nil {
		t.Fatalf("FlushAll failed: %v", err)
	}
	vol1 := mustGetVolume(t, server1, volumeID)
	if err := vol1.FlushOverlay(ctx); err != nil {
		t.Fatalf("FlushOverlay failed: %v", err)
	}

	// 5. Trigger Rebuild on volume index directly from log
	// We force a rebuild of the index (which tails the stream records, encounters the restore record,
	// replaces index with snapshot s1, and replays post-restore changes).
	rebuiltIdx, rebuildPos, err := vol1.rebuildIndex(ctx)
	if err != nil {
		t.Fatalf("rebuildIndex failed: %v", err)
	}
	defer func() { _ = rebuiltIdx.Close() }()

	if rebuildPos != vol1.Position() {
		t.Fatalf("Rebuilt index position %d does not match volume position %d", rebuildPos, vol1.Position())
	}

	// 6. Verify rebuilt index content matches recovery state
	entryKeyFinal, err := pkDirEntry.Extract(&pb.DirEntry{ParentIno: proto.Uint64(1), Name: proto.String("final.txt")})
	if err != nil {
		t.Fatalf("Extract final.txt key failed: %v", err)
	}
	msgFinal, okFinal, err := rebuiltIdx.Get(ctx, "objectfs.v1alpha1.DirEntry", entryKeyFinal)
	if err != nil || !okFinal || msgFinal == nil {
		t.Fatalf("final.txt not found in rebuilt index: ok=%v err=%v", okFinal, err)
	}

	entryKeyTemp, err := pkDirEntry.Extract(&pb.DirEntry{ParentIno: proto.Uint64(1), Name: proto.String("temp.txt")})
	if err != nil {
		t.Fatalf("Extract temp.txt key failed: %v", err)
	}
	_, okTemp, err := rebuiltIdx.Get(ctx, "objectfs.v1alpha1.DirEntry", entryKeyTemp)
	if err != nil {
		t.Fatalf("Get temp.txt failed: %v", err)
	}
	if okTemp {
		t.Fatalf("temp.txt unexpectedly found in rebuilt index; restore record was not honoured!")
	}

	_ = server1.Close()

	// Compare with fresh recovered server
	server2 := NewServer(backend, WithServerWAL(walDir, "", walclient.Local))
	defer func() { _ = server2.Close() }()

	read1, err := testReadFile(ctx, server2, volumeID, "/file1.txt", 0, 1024)
	if err != nil {
		t.Fatalf("ReadFile /file1.txt failed: %v", err)
	}
	if string(read1.Data) != "file 1 post-restore write" {
		t.Fatalf("Recovered file1 content mismatch: got %q", string(read1.Data))
	}
	readFinal, err := testReadFile(ctx, server2, volumeID, "/final.txt", 0, 1024)
	if err != nil {
		t.Fatalf("ReadFile /final.txt failed: %v", err)
	}
	if string(readFinal.Data) != "final file" {
		t.Fatalf("Recovered final content mismatch: got %q", string(readFinal.Data))
	}
}

func TestSnapshotRestoreInodeMonotonicity(t *testing.T) {
	ctx := t.Context()
	walDir := t.TempDir()
	backend := NewMemoryBackend()
	volumeID := "test-restore-inode-monotonic"

	server1 := NewServer(backend, WithServerWAL(walDir, "", walclient.Local))

	// 1. Create file 1 and take snapshot s1
	f1Attr, err := testCreateFile(ctx, server1, volumeID, "/file1.txt", 0644, []byte("f1"), 0, 0)
	if err != nil {
		t.Fatalf("CreateFile 1 failed: %v", err)
	}
	ino1 := f1Attr.GetAttr().GetInode().GetIno()

	s1Resp, err := server1.CreateSnapshot(ctx, &pb.CreateSnapshotRequest{VolumeId: volumeID})
	if err != nil {
		t.Fatalf("CreateSnapshot s1 failed: %v", err)
	}
	s1 := s1Resp.GetSnapshotName()

	// 2. Create file 2 and file 3 (allocates higher inode numbers)
	f2Attr, err := testCreateFile(ctx, server1, volumeID, "/file2.txt", 0644, []byte("f2"), 0, 0)
	if err != nil {
		t.Fatalf("CreateFile 2 failed: %v", err)
	}
	ino2 := f2Attr.GetAttr().GetInode().GetIno()

	f3Attr, err := testCreateFile(ctx, server1, volumeID, "/file3.txt", 0644, []byte("f3"), 0, 0)
	if err != nil {
		t.Fatalf("CreateFile 3 failed: %v", err)
	}
	ino3 := f3Attr.GetAttr().GetInode().GetIno()

	if ino2 <= ino1 || ino3 <= ino2 {
		t.Fatalf("Expected strictly increasing inode numbers: ino1=%d, ino2=%d, ino3=%d", ino1, ino2, ino3)
	}

	// 3. Restore to snapshot s1 (file2 and file3 are rolled back)
	if err := server1.RestoreSnapshot(ctx, volumeID, s1); err != nil {
		t.Fatalf("RestoreSnapshot s1 failed: %v", err)
	}

	// 4. Create a new file post-restore in the same running process
	fPostAttr, err := testCreateFile(ctx, server1, volumeID, "/file_post.txt", 0644, []byte("fpost"), 0, 0)
	if err != nil {
		t.Fatalf("CreateFile post failed: %v", err)
	}
	inoPost := fPostAttr.GetAttr().GetInode().GetIno()

	// Inode numbers must NOT collide with ino2 or ino3!
	if inoPost <= ino3 {
		t.Fatalf("Inode collision in running process: inoPost=%d <= ino3=%d (ino1=%d, ino2=%d)", inoPost, ino3, ino1, ino2)
	}

	_ = server1.Close()

	// 5. Restart controller and create another file after recovery
	server2 := NewServer(backend, WithServerWAL(walDir, "", walclient.Local))
	defer func() { _ = server2.Close() }()

	fRestartAttr, err := testCreateFile(ctx, server2, volumeID, "/file_restart.txt", 0644, []byte("frestart"), 0, 0)
	if err != nil {
		t.Fatalf("CreateFile restart failed: %v", err)
	}
	inoRestart := fRestartAttr.GetAttr().GetInode().GetIno()

	if inoRestart <= inoPost {
		t.Fatalf("Inode collision after restart: inoRestart=%d <= inoPost=%d", inoRestart, inoPost)
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

	content1 := bytes.Repeat([]byte("blob data content number 1 "), 300)
	content2 := bytes.Repeat([]byte("blob data content number 2 - slightly longer test blob "), 200)

	_, err := testCreateFile(ctx, server, volumeID, "/file1.txt", 0644, content1, 0, 0)
	if err != nil {
		t.Fatalf("CreateFile file1 failed: %v", err)
	}

	_, err = testCreateFile(ctx, server, volumeID, "/file2.txt", 0644, content2, 0, 0)
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
		_, err := testCreateFile(ctx, server, v, "/hello.txt", 0644, []byte("content for "+v), 0, 0)
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
	_, err := testCreateFile(ctx, server, volumeID, "/file1.txt", 0644, []byte("snap 1 content"), 0, 0)
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
	_, err = testCreateFile(ctx, server, volumeID, "/file2.txt", 0644, []byte("snap 2 content"), 0, 0)
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
	_, err = testCreateFile(ctx, server, volumeID, "/file3.txt", 0644, []byte("snap 3 content"), 0, 0)
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
	return startTestWalBufferServerWithBackend(t, dir, NewMemoryBackend())
}

func startTestWalBufferServerWithBackend(t *testing.T, dir string, backend ObjectStorageBackend) (*walbuffer.Server, string, func()) {
	ctx := t.Context()
	srv, err := walbuffer.NewServer(ctx, walbuffer.ServerConfig{
		Backend:       backend,
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
	mkdirResp, err := testMkdir(ctx, server, volumeID, "/testdir", 0755, 0, 0)
	if err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}
	if mkdirResp.GetAttr().GetName() != "testdir" {
		t.Fatalf("Unexpected mkdir name: %s", mkdirResp.GetAttr().GetName())
	}

	vol := mustGetVolume(t, server, volumeID)
	if vol == nil || vol.Stream() == nil {
		t.Fatalf("Expected volume to have an active WAL stream")
	}

	localSeq, _, _ := vol.Stream().Watermarks()
	if localSeq < 1 {
		t.Fatalf("Expected localSeq >= 1 after Mkdir, got %d", localSeq)
	}

	// 2. CreateFile should log to stream
	createResp, err := testCreateFile(ctx, server, volumeID, "/testdir/data.txt", 0644, []byte("hello streams"), 0, 0)
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}
	if createResp.GetAttr().GetInode().GetSize() != int64(len("hello streams")) {
		t.Fatalf("Unexpected size: %d", createResp.GetAttr().GetInode().GetSize())
	}

	localSeq2, _, _ := vol.Stream().Watermarks()
	if localSeq2 <= localSeq {
		t.Fatalf("Expected localSeq to advance after CreateFile: %d -> %d", localSeq, localSeq2)
	}

	// 3. WriteFile should log to stream
	writeResp, err := testWriteFile(ctx, server, volumeID, "/testdir/data.txt", 5, []byte(" world!"), pb.WriteMode_WRITE_MODE_UNSPECIFIED)
	if err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}
	if writeResp.BytesWritten != int64(len(" world!")) {
		t.Fatalf("Unexpected bytes written: %d", writeResp.BytesWritten)
	}

	// 4. TruncateFile should log to stream
	truncResp, err := testTruncateFile(ctx, server, volumeID, "/testdir/data.txt", 5)
	if err != nil {
		t.Fatalf("TruncateFile failed: %v", err)
	}
	if truncResp.GetAttr().GetInode().GetSize() != 5 {
		t.Fatalf("Unexpected truncated size: %d", truncResp.GetAttr().GetInode().GetSize())
	}

	// 5. Rename should log to stream
	renameResp, err := testRename(ctx, server, volumeID, "/testdir/data.txt", "/testdir/renamed.txt")
	if err != nil {
		t.Fatalf("Rename failed: %v", err)
	}
	if renameResp.GetAttr().GetName() != "renamed.txt" {
		t.Fatalf("Unexpected rename name: %s", renameResp.GetAttr().GetName())
	}

	// 6. Unlink should log to stream
	_, err = testUnlink(ctx, server, volumeID, "/testdir/renamed.txt")
	if err != nil {
		t.Fatalf("Unlink failed: %v", err)
	}

	// 7. Rmdir should log to stream
	_, err = testRmdir(ctx, server, volumeID, "/testdir")
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
	_, err := testMkdir(ctx, server1, volumeID, "/base", 0755, 0, 0)
	if err != nil {
		t.Fatalf("server1 Mkdir failed: %v", err)
	}
	_, err = testCreateFile(ctx, server1, volumeID, "/base/initial.txt", 0644, []byte("initial snapshot content"), 0, 0)
	if err != nil {
		t.Fatalf("server1 CreateFile failed: %v", err)
	}

	// Step 2: Flush snapshot to backend
	vol1 := mustGetVolume(t, server1, volumeID)
	snapName, err := vol1.CreateSnapshot(ctx, "")
	if err != nil {
		t.Fatalf("CreateSnapshot failed: %v", err)
	}
	if snapName == "" {
		t.Fatalf("Empty snapshot name returned")
	}

	// Step 3: Perform mutations AFTER snapshot (these are in the WAL change-log, not in snapshot)
	_, err = testCreateFile(ctx, server1, volumeID, "/base/post_snapshot.txt", 0644, []byte("post snapshot content"), 0, 0)
	if err != nil {
		t.Fatalf("server1 CreateFile post snapshot failed: %v", err)
	}

	_, err = testRename(ctx, server1, volumeID, "/base/initial.txt", "/base/renamed_initial.txt")
	if err != nil {
		t.Fatalf("server1 Rename failed: %v", err)
	}

	// Simulate crash: close server1 without flushing snapshot to backend
	_ = server1.Close()

	// Step 4: Start new server instance with same backend and walDir
	server2 := NewServer(backend, WithServerWAL(walDir, "", walclient.Local))
	defer func() { _ = server2.Close() }()

	vol2 := mustGetVolume(t, server2, volumeID)
	if vol2 == nil {
		t.Fatalf("Failed to get recovered volume")
	}

	// Verify that state reflects both the snapshot AND replayed WAL mutations:
	// - /base should exist
	baseAttr, err := testGetAttr(ctx, server2, volumeID, "/base")
	if err != nil || !baseAttr.GetAttr().GetInode().GetIsDir() {
		t.Fatalf("Recovered base directory missing or not dir: %v", err)
	}

	// - /base/initial.txt should have been renamed to /base/renamed_initial.txt
	initResp, err := testGetAttr(ctx, server2, volumeID, "/base/initial.txt")
	if err != nil {
		t.Fatalf("GetAttr failed: %v", err)
	}
	if initResp.GetError() != int32(syscall.ENOENT) {
		t.Fatalf("Expected /base/initial.txt to not exist after rename replay, got error %d", initResp.GetError())
	}

	renamedAttr, err := testGetAttr(ctx, server2, volumeID, "/base/renamed_initial.txt")
	if err != nil {
		t.Fatalf("Expected /base/renamed_initial.txt to exist: %v", err)
	}
	if renamedAttr.GetAttr().GetInode().GetSize() != int64(len("initial snapshot content")) {
		t.Fatalf("Unexpected size on renamed file: %d", renamedAttr.GetAttr().GetInode().GetSize())
	}

	// - /base/post_snapshot.txt should exist and have correct size and content
	readResp, err := testReadFile(ctx, server2, volumeID, "/base/post_snapshot.txt", 0, 1024)
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
	_, err := testMkdir(ctx, server, volumeID, "/witness_dir", 0755, 0, 0)
	if err != nil {
		t.Fatalf("Mkdir with Witness durability failed: %v", err)
	}

	vol := mustGetVolume(t, server, volumeID)
	local, witness, _ := vol.Stream().Watermarks()
	if local < 1 || witness < 1 {
		t.Fatalf("Expected local >= 1 and witness >= 1, got local=%d, witness=%d", local, witness)
	}

	// 2. WriteFile with WRITE_THROUGH_FSYNC (Permanent)
	_, err = testCreateFile(ctx, server, volumeID, "/witness_dir/file.bin", 0644, []byte("data"), 0, 0)
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}

	_, err = testWriteFile(ctx, server, volumeID, "/witness_dir/file.bin", 4, []byte("more"), pb.WriteMode_WRITE_THROUGH_FSYNC)
	if err != nil {
		t.Fatalf("WriteFile with WRITE_THROUGH_FSYNC failed: %v", err)
	}

	// Staged write is not yet committed to WAL
	_, _, permanentBefore := vol.Stream().Watermarks()
	if permanentBefore != 0 {
		t.Fatalf("Expected permanent watermark 0 before Fsync, got %d", permanentBefore)
	}

	// 3. Fsync RPC commits metadata and flushes stream to permanent storage
	ino, _ := resolvePath(ctx, server, volumeID, "/witness_dir/file.bin")
	fsyncResp, err := server.Fsync(ctx, &pb.FsyncRequest{
		VolumeId: volumeID,
		Inode:    ino,
		Blocking: true,
	})
	if err != nil || !fsyncResp.GetSuccess() {
		t.Fatalf("Fsync failed: %v", err)
	}

	_, _, permanent := vol.Stream().Watermarks()
	if permanent < 1 {
		t.Fatalf("Expected permanent watermark >= 1 after Fsync, got %d", permanent)
	}
}

func TestApplyRecordDirect(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	vol := MustNewVolume(t, "apply-test", backend, NewEventBroadcaster())

	// Direct Mkdir
	dirAttr, err := vol.Mkdir(ctx, 1, "c", 0755, 0, 0)
	if err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}

	attr, err := vol.GetAttr(ctx, dirAttr.GetInode().GetIno())
	if err != nil || !attr.GetInode().GetIsDir() {
		t.Fatalf("Expected directory inode %d: %v", dirAttr.GetInode().GetIno(), err)
	}

	// Direct CreateFile
	fileAttr, err := vol.CreateFile(ctx, dirAttr.GetInode().GetIno(), "foo.txt", 0644, []byte("test"), 0, 0, 0)
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}

	data, total, _, err := vol.ReadFile(ctx, fileAttr.GetInode().GetIno(), 0, 100)
	if err != nil || total != 4 || string(data) != "test" {
		t.Fatalf("Unexpected file content: %s (err: %v)", string(data), err)
	}

	// Direct TruncateFile
	_, err = vol.TruncateFile(ctx, fileAttr.GetInode().GetIno(), 2)
	if err != nil {
		t.Fatalf("TruncateFile failed: %v", err)
	}
	data, total, _, err = vol.ReadFile(ctx, fileAttr.GetInode().GetIno(), 0, 100)
	if err != nil || total != 2 || string(data) != "te" {
		t.Fatalf("Unexpected truncated content: %s (err: %v)", string(data), err)
	}

	// Direct Rename
	_, err = vol.Rename(ctx, dirAttr.GetInode().GetIno(), "foo.txt", dirAttr.GetInode().GetIno(), "bar.txt")
	if err != nil {
		t.Fatalf("Rename failed: %v", err)
	}
	_, err = vol.Lookup(ctx, dirAttr.GetInode().GetIno(), "foo.txt")
	if err == nil {
		t.Fatalf("Expected foo.txt to be removed after rename")
	}
	barAttr, err := vol.Lookup(ctx, dirAttr.GetInode().GetIno(), "bar.txt")
	if err != nil || barAttr.GetName() != "bar.txt" {
		t.Fatalf("Expected bar.txt to exist: %v", err)
	}

	// Direct Unlink
	_, err = vol.Unlink(ctx, dirAttr.GetInode().GetIno(), "bar.txt")
	if err != nil {
		t.Fatalf("Unlink failed: %v", err)
	}
	_, err = vol.Lookup(ctx, dirAttr.GetInode().GetIno(), "bar.txt")
	if err == nil {
		t.Fatalf("Expected bar.txt to be unlinked")
	}

	// Direct Rmdir
	_, err = vol.Rmdir(ctx, 1, "c")
	if err != nil {
		t.Fatalf("Rmdir failed: %v", err)
	}
	_, err = vol.Lookup(ctx, 1, "c")
	if err == nil {
		t.Fatalf("Expected c to be deleted")
	}

	// Direct Symlink
	symAttr, err := vol.Symlink(ctx, 1, "symlink.txt", "target.txt", 0, 0)
	if err != nil {
		t.Fatalf("Symlink failed: %v", err)
	}
	lookupSym, err := vol.Lookup(ctx, 1, "symlink.txt")
	if err != nil || lookupSym.GetInode().GetSymlinkTarget() != "target.txt" || lookupSym.GetInode().GetNlink() != 1 {
		t.Fatalf("Unexpected symlink attr: %v, err: %v", lookupSym, err)
	}

	// Direct Link
	_, err = vol.Link(ctx, symAttr.GetInode().GetIno(), 1, "symlink_link.txt")
	if err != nil {
		t.Fatalf("Link failed: %v", err)
	}
	lookupLink, err := vol.Lookup(ctx, 1, "symlink_link.txt")
	if err != nil || lookupLink.GetInode().GetIno() != symAttr.GetInode().GetIno() || lookupLink.GetInode().GetNlink() != 2 {
		t.Fatalf("Unexpected link attr: %v, err: %v", lookupLink, err)
	}
}

func TestSymlinkAndHardlinkOperations(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	vol := MustNewVolume(t, "symlink-test", backend, NewEventBroadcaster())

	// 1. Create a regular file
	fileAttr, err := vol.CreateFile(ctx, 1, "orig.txt", 0644, []byte("contents"), 100, 200, 0)
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}
	if fileAttr.GetInode().GetNlink() != 1 {
		t.Fatalf("Expected Nlink 1, got %d", fileAttr.GetInode().GetNlink())
	}

	// 2. Create a hard link
	linkAttr, err := vol.Link(ctx, fileAttr.GetInode().GetIno(), 1, "link1.txt")
	if err != nil {
		t.Fatalf("Link failed: %v", err)
	}
	if linkAttr.GetInode().GetIno() != fileAttr.GetInode().GetIno() {
		t.Fatalf("Expected same inode %d, got %d", fileAttr.GetInode().GetIno(), linkAttr.GetInode().GetIno())
	}
	if linkAttr.GetInode().GetNlink() != 2 {
		t.Fatalf("Expected Nlink 2, got %d", linkAttr.GetInode().GetNlink())
	}

	// Verify orig.txt has Nlink 2
	origAttr, err := vol.Lookup(ctx, 1, "orig.txt")
	if err != nil || origAttr.GetInode().GetNlink() != 2 {
		t.Fatalf("Expected orig.txt Nlink 2, got %d, err %v", origAttr.GetInode().GetNlink(), err)
	}

	// Hard link to a directory should fail with EPERM
	dirAttr, err := vol.Mkdir(ctx, 1, "subdir", 0755, 0, 0)
	if err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}
	_, err = vol.Link(ctx, dirAttr.GetInode().GetIno(), 1, "dirlink")
	if !errors.Is(err, syscall.EPERM) {
		t.Fatalf("Expected EPERM for linking directory, got: %v", err)
	}

	// Hard link to an existing name should fail with EEXIST
	_, err = vol.Link(ctx, fileAttr.GetInode().GetIno(), 1, "link1.txt")
	if !errors.Is(err, syscall.EEXIST) {
		t.Fatalf("Expected EEXIST for existing name, got: %v", err)
	}

	// 3. Create a symlink
	symAttr, err := vol.Symlink(ctx, 1, "sym.lnk", "orig.txt", 100, 200)
	if err != nil {
		t.Fatalf("Symlink failed: %v", err)
	}
	if (symAttr.GetInode().GetMode() & syscall.S_IFMT) != syscall.S_IFLNK {
		t.Fatalf("Expected S_IFLNK mode, got %o", symAttr.GetInode().GetMode())
	}
	if symAttr.GetInode().GetSymlinkTarget() != "orig.txt" {
		t.Fatalf("Expected target 'orig.txt', got %q", symAttr.GetInode().GetSymlinkTarget())
	}
	if symAttr.GetInode().GetNlink() != 1 {
		t.Fatalf("Expected symlink Nlink 1, got %d", symAttr.GetInode().GetNlink())
	}

	// 4. Readlink
	target, err := vol.Readlink(ctx, symAttr.GetInode().GetIno())
	if err != nil || target != "orig.txt" {
		t.Fatalf("Readlink failed: target=%q, err=%v", target, err)
	}

	// Readlink on regular file should return EINVAL
	_, err = vol.Readlink(ctx, fileAttr.GetInode().GetIno())
	if !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("Expected EINVAL for Readlink on regular file, got: %v", err)
	}

	// 5. Unlink orig.txt -> link1.txt still exists with Nlink 1
	if _, err := vol.Unlink(ctx, 1, "orig.txt"); err != nil {
		t.Fatalf("Unlink orig.txt failed: %v", err)
	}
	_, err = vol.Lookup(ctx, 1, "orig.txt")
	if !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("Expected ENOENT after unlink, got: %v", err)
	}
	remainingAttr, err := vol.Lookup(ctx, 1, "link1.txt")
	if err != nil || remainingAttr.GetInode().GetNlink() != 1 {
		t.Fatalf("Expected link1.txt Nlink 1, got %d, err %v", remainingAttr.GetInode().GetNlink(), err)
	}

	// Read content through link1.txt inode
	data, total, _, err := vol.ReadFile(ctx, fileAttr.GetInode().GetIno(), 0, 100)
	if err != nil || total != 8 || string(data) != "contents" {
		t.Fatalf("Expected 'contents', got %q (total=%d, err=%v)", string(data), total, err)
	}

	// 6. Unlink link1.txt -> file is deleted (no open handles)
	if _, err := vol.Unlink(ctx, 1, "link1.txt"); err != nil {
		t.Fatalf("Unlink link1.txt failed: %v", err)
	}
	_, err = vol.GetAttr(ctx, fileAttr.GetInode().GetIno())
	if !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("Expected ENOENT for unlinked inode with no open handles, got: %v", err)
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
	createResp, err := testCreateFile(ctx, server, volumeID, "/test.txt", 0644, []byte("initial"), 0, 0)
	if err != nil || createResp.GetError() != 0 {
		t.Fatalf("CreateFile failed: err=%v, resp.Error=%d", err, createResp.GetError())
	}

	// Direct Volume.WriteFile with WRITE_THROUGH_FSYNC (Permanent) should fail promptly with typed error
	vol := mustGetVolume(t, server, volumeID)
	_, _, _, err = volWriteFile(ctx, vol, "/test.txt", 0, []byte("data-permanent"), pb.WriteMode_WRITE_THROUGH_FSYNC)
	if err == nil {
		t.Fatalf("expected vol.WriteFile(WRITE_THROUGH_FSYNC) to fail on target-less WAL, got nil")
	}
	if !strings.Contains(err.Error(), "durability level permanent requested but WAL has no remote target") {
		t.Fatalf("unexpected error message: %v", err)
	}

	// Server.WriteFile with WRITE_THROUGH_FSYNC (Permanent) should return error in response
	writeResp, err := testWriteFile(ctx, server, volumeID, "/test.txt", 0, []byte("data-permanent"), pb.WriteMode_WRITE_THROUGH_FSYNC)
	if err != nil {
		t.Fatalf("unexpected gRPC error: %v", err)
	}
	if writeResp.GetError() == 0 {
		t.Fatalf("expected WriteFile(WRITE_THROUGH_FSYNC) to return non-zero error on target-less WAL")
	}

	// Direct Volume.WriteFile with EAGER_REPLICATION (Witness) should fail promptly with typed error
	_, _, _, err = volWriteFile(ctx, vol, "/test.txt", 0, []byte("data-witness"), pb.WriteMode_EAGER_REPLICATION)
	if err == nil {
		t.Fatalf("expected vol.WriteFile(EAGER_REPLICATION) to fail on target-less WAL, got nil")
	}
	if !strings.Contains(err.Error(), "durability level witness requested but WAL has no remote target") {
		t.Fatalf("unexpected error message: %v", err)
	}

	// Server.WriteFile with EAGER_REPLICATION (Witness) should return error in response
	writeResp, err = testWriteFile(ctx, server, volumeID, "/test.txt", 0, []byte("data-witness"), pb.WriteMode_EAGER_REPLICATION)
	if err != nil {
		t.Fatalf("unexpected gRPC error: %v", err)
	}
	if writeResp.GetError() == 0 {
		t.Fatalf("expected WriteFile(EAGER_REPLICATION) to return non-zero error on target-less WAL")
	}

	// WriteFile with LAZY_WRITE (Local) should succeed
	writeResp, err = testWriteFile(ctx, server, volumeID, "/test.txt", 0, []byte("data-local"), pb.WriteMode_LAZY_WRITE)
	if err != nil || writeResp.GetError() != 0 {
		t.Fatalf("WriteFile(LAZY_WRITE) failed: err=%v, resp.Error=%d", err, writeResp.GetError())
	}
	if writeResp.BytesWritten != int64(len("data-local")) {
		t.Fatalf("expected %d bytes written, got %d", len("data-local"), writeResp.BytesWritten)
	}

	// Direct Volume.Fsync should fail fast on target-less WAL because it flushes to permanent storage
	testIno, _ := vol.ResolvePath(ctx, "/test.txt")
	err = vol.Fsync(ctx, testIno)
	if err == nil {
		t.Fatalf("expected vol.Fsync to fail on target-less WAL with unflushed records, got nil")
	}
	if !strings.Contains(err.Error(), "durability level permanent requested but WAL has no remote target") {
		t.Fatalf("unexpected error message: %v", err)
	}

	// Server.Fsync should return error in response
	fsyncResp, err := server.Fsync(ctx, &pb.FsyncRequest{
		VolumeId: volumeID,
		Inode:    testIno,
	})
	if err != nil {
		t.Fatalf("unexpected gRPC error: %v", err)
	}
	if fsyncResp.GetError() == 0 || fsyncResp.GetSuccess() {
		t.Fatalf("expected Fsync to return non-zero error on target-less WAL with unflushed records")
	}
}

type fakeBlockingStream struct {
	appendCalled chan struct{}
	waitCalled   chan struct{}
	waitGate     chan struct{}
	localSeq     atomic.Uint64
}

func (s *fakeBlockingStream) Append(ctx context.Context, payload []byte) (uint64, error) {
	seq := s.localSeq.Add(1)
	select {
	case s.appendCalled <- struct{}{}:
	default:
	}
	return seq, nil
}

func (s *fakeBlockingStream) Wait(ctx context.Context, seq uint64, level walclient.Level, requestFlush bool) error {
	select {
	case s.waitCalled <- struct{}{}:
	default:
	}
	select {
	case <-s.waitGate:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *fakeBlockingStream) Flush(ctx context.Context) error {
	return nil
}

func (s *fakeBlockingStream) Watermarks() (local, witness, permanent uint64) {
	return s.localSeq.Load(), 0, 0
}

func (s *fakeBlockingStream) RecoveredRecords() []*wal.ClientRecord {
	return nil
}

func (s *fakeBlockingStream) ReplicationLevel() walclient.Level {
	return walclient.Permanent
}

func (s *fakeBlockingStream) Tail(ctx context.Context, fromSeq uint64) (iter.Seq2[uint64, []byte], error) {
	return func(yield func(uint64, []byte) bool) {}, nil
}

func (s *fakeBlockingStream) Degraded() *walclient.DegradedError {
	return nil
}

func (s *fakeBlockingStream) Close() error {
	return nil
}

func TestStreamsDurabilityConcurrency(t *testing.T) {
	ctx := t.Context()
	fakeStream := &fakeBlockingStream{
		appendCalled: make(chan struct{}, 10),
		waitCalled:   make(chan struct{}, 10),
		waitGate:     make(chan struct{}),
	}

	backend := NewMemoryBackend()
	server := NewServer(backend,
		WithServerStreamFactory(func(volumeID string) (walclient.Stream, error) {
			return fakeStream, nil
		}),
		WithServerDurability(walclient.Witness),
	)
	defer func() { _ = server.Close() }()

	volumeID := "test-concurrency-vol"

	// 1. Concurrently start CreateFile which blocks on fakeStream.Wait
	type createResult struct {
		resp *pb.CreateFileResponse
		err  error
	}
	createCh := make(chan createResult, 1)

	go func() {
		resp, err := testCreateFile(ctx, server, volumeID, "/blocking_file.txt", 0644, []byte("initial-data"), 0, 0)
		createCh <- createResult{resp: resp, err: err}
	}()

	// Wait until CreateFile enters fakeStream.Wait (outside v.mu)
	select {
	case <-fakeStream.waitCalled:
	case <-time.After(5 * time.Second):
		t.Fatalf("Timed out waiting for CreateFile to enter stream.Wait")
	}

	// While CreateFile is still blocked in Wait, ensure GetAttr, Lookup, and ReadFile complete promptly
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)

		// Root GetAttr
		rootAttr, err := testGetAttr(ctx, server, volumeID, "/")
		if err != nil || !rootAttr.GetAttr().GetInode().GetIsDir() {
			t.Errorf("GetAttr root failed while write is waiting for durability: %v", err)
			return
		}

		// Lookup the new file (in-memory state is already updated)
		lookupResp, err := server.Lookup(ctx, &pb.LookupRequest{
			VolumeId:    volumeID,
			ParentInode: 1,
			Name:        "blocking_file.txt",
		})
		if err != nil || lookupResp.GetAttr().GetName() != "blocking_file.txt" {
			t.Errorf("Lookup new file failed while write is waiting for durability: %v", err)
			return
		}

		// GetAttr on the new file
		fileAttr, err := testGetAttr(ctx, server, volumeID, "/blocking_file.txt")
		if err != nil || fileAttr.GetAttr().GetInode().GetSize() != int64(len("initial-data")) {
			t.Errorf("GetAttr new file failed while write is waiting for durability: %v", err)
			return
		}

		// ReadFile on the new file
		readResp, err := testReadFile(ctx, server, volumeID, "/blocking_file.txt", 0, 1024)
		if err != nil || string(readResp.Data) != "initial-data" {
			t.Errorf("ReadFile failed while write is waiting for durability: %v", err)
			return
		}
	}()

	select {
	case <-readDone:
	case <-time.After(2 * time.Second):
		t.Fatalf("Concurrent reads stalled while writer waited for WAL durability")
	}

	// Unblock the stream and ensure CreateFile completes successfully
	close(fakeStream.waitGate)
	select {
	case res := <-createCh:
		if res.err != nil {
			t.Fatalf("CreateFile failed: %v", res.err)
		}
		if res.resp.GetAttr().GetName() != "blocking_file.txt" {
			t.Fatalf("Unexpected CreateFile attr: %v", res.resp.GetAttr())
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Timed out waiting for CreateFile to return after unblocking stream")
	}

	// 2. Test wait failure semantics: context cancellation during Wait returns error to caller,
	// but in-memory mutation remains visible.
	fakeStream2 := &fakeBlockingStream{
		appendCalled: make(chan struct{}, 10),
		waitCalled:   make(chan struct{}, 10),
		waitGate:     make(chan struct{}),
	}
	server2 := NewServer(backend,
		WithServerStreamFactory(func(volumeID string) (walclient.Stream, error) {
			return fakeStream2, nil
		}),
		WithServerDurability(walclient.Witness),
	)
	defer func() { _ = server2.Close() }()

	vol2 := "test-wait-fail-vol"
	writeCtx, cancelWrite := context.WithCancel(ctx)

	type createFailResult struct {
		resp *pb.CreateFileResponse
		err  error
	}
	writeCh := make(chan createFailResult, 1)
	go func() {
		resp, err := testCreateFile(writeCtx, server2, vol2, "/fail_durability.txt", 0644, []byte("persisted-in-mem"), 0, 0)
		writeCh <- createFailResult{resp: resp, err: err}
	}()

	select {
	case <-fakeStream2.waitCalled:
	case <-time.After(5 * time.Second):
		t.Fatalf("Timed out waiting for CreateFile on server2 to enter stream.Wait")
	}

	// Cancel context during wait
	cancelWrite()

	select {
	case res := <-writeCh:
		if res.err == nil && (res.resp == nil || res.resp.GetError() == 0) {
			t.Fatalf("Expected CreateFile to fail on canceled context, got success")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Timed out waiting for CreateFile to fail on canceled context")
	}

	// Verify in-memory state is still visible (not rolled back)
	attr, err := testGetAttr(ctx, server2, vol2, "/fail_durability.txt")
	if err != nil || attr.GetError() != 0 || attr.GetAttr().GetInode().GetIno() == 0 {
		t.Fatalf("Expected in-memory state to remain intact after durability wait failure: %v (attr: %v)", err, attr)
	}
}

func TestStreamsDurabilityConcurrencyAllMutations(t *testing.T) {
	ctx := t.Context()
	fakeStream := &fakeBlockingStream{
		appendCalled: make(chan struct{}, 10),
		waitCalled:   make(chan struct{}, 10),
		waitGate:     make(chan struct{}),
	}

	backend := NewMemoryBackend()
	server := NewServer(backend,
		WithServerStreamFactory(func(volumeID string) (walclient.Stream, error) {
			return fakeStream, nil
		}),
		WithServerDurability(walclient.Witness),
	)
	defer func() { _ = server.Close() }()

	volumeID := "test-all-mutations-vol"

	// Helper to run a mutation while checking that concurrent reads succeed
	runMutationTest := func(name string, mutate func(), checkReads func()) {
		// New waitGate for this mutation
		fakeStream.waitGate = make(chan struct{})

		done := make(chan struct{})
		go func() {
			defer close(done)
			mutate()
		}()

		select {
		case <-fakeStream.waitCalled:
		case <-time.After(5 * time.Second):
			t.Fatalf("[%s] Timed out waiting for mutation to enter stream.Wait", name)
		}

		readDone := make(chan struct{})
		go func() {
			defer close(readDone)
			checkReads()
		}()

		select {
		case <-readDone:
		case <-time.After(2 * time.Second):
			t.Fatalf("[%s] Concurrent reads stalled while waiting for durability", name)
		}

		close(fakeStream.waitGate)
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("[%s] Mutation did not complete after unblocking wait", name)
		}
	}

	// 1. CreateFile
	runMutationTest("CreateFile", func() {
		_, err := testCreateFile(ctx, server, volumeID, "/test_file.txt", 0644, []byte("initial"), 0, 0)
		if err != nil {
			t.Errorf("CreateFile failed: %v", err)
		}
	}, func() {
		attr, err := testGetAttr(ctx, server, volumeID, "/test_file.txt")
		if err != nil || attr.GetAttr().GetInode().GetSize() != 7 {
			t.Errorf("GetAttr during CreateFile failed: %v", err)
		}
	})

	// 2. WriteFile + Fsync
	_, wErr := testWriteFile(ctx, server, volumeID, "/test_file.txt", 7, []byte("-appended"), pb.WriteMode_WRITE_THROUGH_FSYNC)
	if wErr != nil {
		t.Errorf("WriteFile failed: %v", wErr)
	}
	fIno, _ := resolvePath(ctx, server, volumeID, "/test_file.txt")
	runMutationTest("Fsync", func() {
		_, err := server.Fsync(ctx, &pb.FsyncRequest{VolumeId: volumeID, Inode: fIno, Blocking: true})
		if err != nil {
			t.Errorf("Fsync failed: %v", err)
		}
	}, func() {
		resp, err := testReadFile(ctx, server, volumeID, "/test_file.txt", 0, 1024)
		if err != nil {
			t.Errorf("ReadFile during Fsync durability wait failed: %v, data=%q", err, string(resp.Data))
		}
	})

	// 3. Mkdir
	runMutationTest("Mkdir", func() {
		_, err := testMkdir(ctx, server, volumeID, "/newdir", 0755, 0, 0)
		if err != nil {
			t.Errorf("Mkdir failed: %v", err)
		}
	}, func() {
		dirAttr, err := testGetAttr(ctx, server, volumeID, "/newdir")
		if err != nil || !dirAttr.GetAttr().GetInode().GetIsDir() {
			t.Errorf("GetAttr during Mkdir durability wait failed: %v", err)
		}
	})

	// 4. Rename
	runMutationTest("Rename", func() {
		_, err := testRename(ctx, server, volumeID, "/test_file.txt", "/renamed_file.txt")
		if err != nil {
			t.Errorf("Rename failed: %v", err)
		}
	}, func() {
		renamedAttr, err := testGetAttr(ctx, server, volumeID, "/renamed_file.txt")
		if err != nil || renamedAttr.GetError() != 0 || renamedAttr.GetAttr() == nil || renamedAttr.GetAttr().GetInode().GetIno() == 0 {
			t.Errorf("GetAttr during Rename durability wait failed: %v", err)
		}
	})

	// 5. TruncateFile
	runMutationTest("TruncateFile", func() {
		_, err := testTruncateFile(ctx, server, volumeID, "/renamed_file.txt", 7)
		if err != nil {
			t.Errorf("TruncateFile failed: %v", err)
		}
	}, func() {
		truncRead, err := testReadFile(ctx, server, volumeID, "/renamed_file.txt", 0, 1024)
		if err != nil || string(truncRead.Data) != "initial" {
			t.Errorf("ReadFile during TruncateFile durability wait failed: %v, data=%q", err, string(truncRead.Data))
		}
	})

	// 6. Unlink
	runMutationTest("Unlink", func() {
		_, err := testUnlink(ctx, server, volumeID, "/renamed_file.txt")
		if err != nil {
			t.Errorf("Unlink failed: %v", err)
		}
	}, func() {
		resp, err := testGetAttr(ctx, server, volumeID, "/renamed_file.txt")
		if err == nil && resp.GetError() == 0 {
			t.Errorf("Expected file to be unlinked in memory during Unlink durability wait")
		}
	})

	// 7. Rmdir
	runMutationTest("Rmdir", func() {
		_, err := testRmdir(ctx, server, volumeID, "/newdir")
		if err != nil {
			t.Errorf("Rmdir failed: %v", err)
		}
	}, func() {
		resp, err := testGetAttr(ctx, server, volumeID, "/newdir")
		if err == nil && resp.GetError() == 0 {
			t.Errorf("Expected directory to be removed in memory during Rmdir durability wait")
		}
	})
}

func TestVolumeFixedBoundaryChunking(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	chunkSize := uint32(16 * 1024) // 16 KiB chunk size for testing
	vol := MustNewVolume(t, "chunk-test-vol", backend, NewEventBroadcaster(), WithChunkSize(chunkSize))

	// 1. Small file <= 1 chunk (e.g. 100 bytes)
	smallData := []byte("small file content unchunked")
	smallAttr, err := volCreateFile(ctx, vol, "/small.txt", 0644, smallData, 0, 0)
	if err != nil {
		t.Fatalf("CreateFile /small.txt failed: %v", err)
	}
	smallSha := fmt.Sprintf("%x", sha256.Sum256(smallData))
	if smallAttr.GetInode().GetManifestSha256() != "" {
		t.Fatalf("expected empty ManifestSha256 for small file, got %s", smallAttr.GetInode().GetManifestSha256())
	}
	if smallAttr.GetInode().GetContentSha256() != smallSha {
		t.Fatalf("expected ContentSha256 %s, got %s", smallSha, smallAttr.GetInode().GetContentSha256())
	}

	// 2. Large file > 1 chunk (e.g. 40 KiB = 2.5 chunks)
	largeData := make([]byte, 40*1024)
	for i := range largeData {
		largeData[i] = byte(i % 251)
	}
	largeSha := fmt.Sprintf("%x", sha256.Sum256(largeData))

	largeAttr, err := volCreateFile(ctx, vol, "/large.bin", 0644, largeData, 0, 0)
	if err != nil {
		t.Fatalf("CreateFile /large.bin failed: %v", err)
	}
	if largeAttr.GetInode().GetSize() != int64(len(largeData)) {
		t.Fatalf("expected Size %d for chunked file, got %d", len(largeData), largeAttr.GetInode().GetSize())
	}
	if largeAttr.GetInode().GetContentSha256() != largeSha {
		t.Fatalf("expected ContentSha256 %s, got %s", largeSha, largeAttr.GetInode().GetContentSha256())
	}

	// 3. Read partial ranges spanning chunk boundaries
	// Read 20 KiB starting at offset 10 KiB (spans chunk 0 and chunk 1)
	partData, total, _, err := volReadFile(ctx, vol, "/large.bin", 10*1024, 20*1024)
	if err != nil {
		t.Fatalf("ReadFile partial spanning chunks failed: %v", err)
	}
	if total != int64(len(largeData)) {
		t.Fatalf("expected total %d, got %d", len(largeData), total)
	}
	if !bytes.Equal(partData, largeData[10*1024:30*1024]) {
		t.Fatalf("partial data mismatch spanning chunks")
	}

	// 4. Random write touching chunk 1 (offset 20 KiB, length 4 KiB)
	patch := []byte("random patch in chunk 1")
	copy(largeData[20*1024:], patch)
	_, newSize, _, err := volWriteFile(ctx, vol, "/large.bin", 20*1024, patch, pb.WriteMode_LAZY_WRITE)
	if err != nil {
		t.Fatalf("WriteFile random write failed: %v", err)
	}
	if newSize != int64(len(largeData)) {
		t.Fatalf("expected size %d, got %d", len(largeData), newSize)
	}
	lIno, _ := vol.ResolvePath(ctx, "/large.bin")
	if err := vol.Fsync(ctx, lIno); err != nil {
		t.Fatalf("Fsync failed: %v", err)
	}

	updatedAttr, err := volGetAttr(ctx, vol, "/large.bin")
	if err != nil {
		t.Fatalf("GetAttr after random write failed: %v", err)
	}
	// ContentSha256 should be cleared (marked unknown) after random write
	if updatedAttr.GetInode().GetContentSha256() != "" {
		t.Fatalf("expected ContentSha256 to be unknown (\"\") after random write, got %s", updatedAttr.GetInode().GetContentSha256())
	}

	// Read back modified range
	readBack, _, _, err := volReadFile(ctx, vol, "/large.bin", 20*1024, int64(len(patch)))
	if err != nil || !bytes.Equal(readBack, patch) {
		t.Fatalf("read back patch mismatch: %q vs %q", string(readBack), string(patch))
	}

	// 5. Test flush to backend succeeds
	if err := vol.FlushToBackend(ctx); err != nil {
		t.Fatalf("FlushToBackend failed: %v", err)
	}

	// 6. Test migration: writing to an unchunked file that grows > chunkSize turns into chunked
	growData := make([]byte, 20*1024)
	for i := range growData {
		growData[i] = 'G'
	}
	_, _, _, err = volWriteFile(ctx, vol, "/small.txt", 100, growData, pb.WriteMode_LAZY_WRITE)
	if err != nil {
		t.Fatalf("WriteFile growing small file failed: %v", err)
	}
	sIno, _ := vol.ResolvePath(ctx, "/small.txt")
	if err := vol.Fsync(ctx, sIno); err != nil {
		t.Fatalf("Fsync growing small file failed: %v", err)
	}

	growAttr, err := volGetAttr(ctx, vol, "/small.txt")
	if err != nil {
		t.Fatalf("GetAttr for grown file failed: %v", err)
	}
	if growAttr.GetInode().GetManifestSha256() == "" {
		t.Fatalf("expected grown file to have ManifestSha256 populated")
	}

	// 7. Test truncation across chunks
	truncAttr, err := volTruncateFile(ctx, vol, "/large.bin", 18*1024)
	if err != nil {
		t.Fatalf("TruncateFile failed: %v", err)
	}
	if truncAttr.GetInode().GetSize() != 18*1024 {
		t.Fatalf("expected size %d after truncation, got %d", 18*1024, truncAttr.GetInode().GetSize())
	}
	truncData, total, _, err := volReadFile(ctx, vol, "/large.bin", 0, 20*1024)
	if err != nil || total != 18*1024 || len(truncData) != 18*1024 {
		t.Fatalf("ReadFile after truncation failed: total=%d, len=%d, err=%v", total, len(truncData), err)
	}
}

func TestStableInodeNumbersAcrossSnapshots(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewServer(backend)
	volumeID := "stable-ino-vol"

	// 1. Create a directory /dir1 and file /file1.txt and /dir1/file2.txt
	_, err := testMkdir(ctx, server, volumeID, "/dir1", 0755, 0, 0)
	if err != nil {
		t.Fatalf("Failed to mkdir /dir1: %v", err)
	}

	create1, err := testCreateFile(ctx, server, volumeID, "/file1.txt", 0644, []byte("content of file 1"), 0, 0)
	if err != nil {
		t.Fatalf("Failed to create /file1.txt: %v", err)
	}

	create2, err := testCreateFile(ctx, server, volumeID, "/dir1/file2.txt", 0644, []byte("content of file 2"), 0, 0)
	if err != nil {
		t.Fatalf("Failed to create /dir1/file2.txt: %v", err)
	}

	dir1Attr, err := testGetAttr(ctx, server, volumeID, "/dir1")
	if err != nil {
		t.Fatalf("Failed to get /dir1 attr: %v", err)
	}

	file1InoInitial := create1.GetAttr().GetInode().GetIno()
	dir1InoInitial := dir1Attr.GetAttr().GetInode().GetIno()
	file2InoInitial := create2.GetAttr().GetInode().GetIno()

	if file1InoInitial == 0 || dir1InoInitial == 0 || file2InoInitial == 0 {
		t.Fatalf("Expected non-zero inode IDs, got file1=%d, dir1=%d, file2=%d", file1InoInitial, dir1InoInitial, file2InoInitial)
	}

	// 2. Take first snapshot
	_, err = server.CreateSnapshot(ctx, &pb.CreateSnapshotRequest{VolumeId: volumeID})
	if err != nil {
		t.Fatalf("Failed to create first snapshot: %v", err)
	}

	// Verify attributes after snapshot 1
	file1AttrAfterSnap1, err := testGetAttr(ctx, server, volumeID, "/file1.txt")
	if err != nil {
		t.Fatalf("Failed to get /file1.txt after snap 1: %v", err)
	}
	if file1AttrAfterSnap1.GetAttr().GetInode().GetIno() != file1InoInitial {
		t.Fatalf("Inode changed after snap 1: expected %d, got %d", file1InoInitial, file1AttrAfterSnap1.GetAttr().GetInode().GetIno())
	}

	dir1AttrAfterSnap1, err := testGetAttr(ctx, server, volumeID, "/dir1")
	if err != nil {
		t.Fatalf("Failed to get /dir1 after snap 1: %v", err)
	}
	if dir1AttrAfterSnap1.GetAttr().GetInode().GetIno() != dir1InoInitial {
		t.Fatalf("Dir inode changed after snap 1: expected %d, got %d", dir1InoInitial, dir1AttrAfterSnap1.GetAttr().GetInode().GetIno())
	}

	file2AttrAfterSnap1, err := testGetAttr(ctx, server, volumeID, "/dir1/file2.txt")
	if err != nil {
		t.Fatalf("Failed to get /dir1/file2.txt after snap 1: %v", err)
	}
	if file2AttrAfterSnap1.GetAttr().GetInode().GetIno() != file2InoInitial {
		t.Fatalf("File2 inode changed after snap 1: expected %d, got %d", file2InoInitial, file2AttrAfterSnap1.GetAttr().GetInode().GetIno())
	}

	// 3. Create a new file before taking second snapshot
	create3, err := testCreateFile(ctx, server, volumeID, "/file3.txt", 0644, []byte("content of file 3"), 0, 0)
	if err != nil {
		t.Fatalf("Failed to create /file3.txt: %v", err)
	}
	file3InoInitial := create3.GetAttr().GetInode().GetIno()

	// 4. Take second snapshot
	_, err = server.CreateSnapshot(ctx, &pb.CreateSnapshotRequest{VolumeId: volumeID})
	if err != nil {
		t.Fatalf("Failed to create second snapshot: %v", err)
	}

	// Verify all attributes after snapshot 2: previous files MUST have identical Inode IDs
	file1AttrAfterSnap2, err := testGetAttr(ctx, server, volumeID, "/file1.txt")
	if err != nil {
		t.Fatalf("Failed to get /file1.txt after snap 2: %v", err)
	}
	if file1AttrAfterSnap2.GetAttr().GetInode().GetIno() != file1InoInitial {
		t.Fatalf("Inode changed after snap 2: expected %d, got %d", file1InoInitial, file1AttrAfterSnap2.GetAttr().GetInode().GetIno())
	}

	dir1AttrAfterSnap2, err := testGetAttr(ctx, server, volumeID, "/dir1")
	if err != nil {
		t.Fatalf("Failed to get /dir1 after snap 2: %v", err)
	}
	if dir1AttrAfterSnap2.GetAttr().GetInode().GetIno() != dir1InoInitial {
		t.Fatalf("Dir inode changed after snap 2: expected %d, got %d", dir1InoInitial, dir1AttrAfterSnap2.GetAttr().GetInode().GetIno())
	}

	file2AttrAfterSnap2, err := testGetAttr(ctx, server, volumeID, "/dir1/file2.txt")
	if err != nil {
		t.Fatalf("Failed to get /dir1/file2.txt after snap 2: %v", err)
	}
	if file2AttrAfterSnap2.GetAttr().GetInode().GetIno() != file2InoInitial {
		t.Fatalf("File2 inode changed after snap 2: expected %d, got %d", file2InoInitial, file2AttrAfterSnap2.GetAttr().GetInode().GetIno())
	}

	file3AttrAfterSnap2, err := testGetAttr(ctx, server, volumeID, "/file3.txt")
	if err != nil {
		t.Fatalf("Failed to get /file3.txt after snap 2: %v", err)
	}
	if file3AttrAfterSnap2.GetAttr().GetInode().GetIno() != file3InoInitial {
		t.Fatalf("File3 inode changed after snap 2: expected %d, got %d", file3InoInitial, file3AttrAfterSnap2.GetAttr().GetInode().GetIno())
	}
}

func TestSDSStepReplayScratch(t *testing.T) {
	ctx := t.Context()
	walDir := t.TempDir()
	backend := NewMemoryBackend()
	volumeID := "sds-replay-vol"

	server := NewServer(backend, WithServerWAL(walDir, "", walclient.Local))

	// 1. Create hierarchy of directories and files
	_, err := testMkdir(ctx, server, volumeID, "/docs", 0755, 0, 0)
	if err != nil {
		t.Fatalf("Mkdir /docs failed: %v", err)
	}
	_, err = testMkdir(ctx, server, volumeID, "/docs/sub", 0755, 0, 0)
	if err != nil {
		t.Fatalf("Mkdir /docs/sub failed: %v", err)
	}
	_, err = testCreateFile(ctx, server, volumeID, "/hello.txt", 0644, []byte("initial hello"), 0, 0)
	if err != nil {
		t.Fatalf("CreateFile /hello.txt failed: %v", err)
	}
	_, err = testCreateFile(ctx, server, volumeID, "/docs/doc1.txt", 0644, []byte("doc1 content"), 0, 0)
	if err != nil {
		t.Fatalf("CreateFile /docs/doc1.txt failed: %v", err)
	}
	_, err = testCreateFile(ctx, server, volumeID, "/docs/sub/doc2.txt", 0644, []byte("doc2 content"), 0, 0)
	if err != nil {
		t.Fatalf("CreateFile /docs/sub/doc2.txt failed: %v", err)
	}

	// 2. Write, Truncate, Rename, Unlink
	_, err = testWriteFile(ctx, server, volumeID, "/hello.txt", 8, []byte("world!"), pb.WriteMode_WRITE_MODE_UNSPECIFIED)
	if err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}
	_, err = testTruncateFile(ctx, server, volumeID, "/docs/doc1.txt", 4)
	if err != nil {
		t.Fatalf("TruncateFile failed: %v", err)
	}
	_, err = testRename(ctx, server, volumeID, "/docs/sub/doc2.txt", "/docs/doc2_renamed.txt")
	if err != nil {
		t.Fatalf("Rename failed: %v", err)
	}
	_, err = testUnlink(ctx, server, volumeID, "/docs/doc1.txt")
	if err != nil {
		t.Fatalf("Unlink failed: %v", err)
	}
	_, err = testRmdir(ctx, server, volumeID, "/docs/sub")
	if err != nil {
		t.Fatalf("Rmdir failed: %v", err)
	}

	liveVol := mustGetVolume(t, server, volumeID)
	if liveVol == nil || liveVol.Stream() == nil {
		t.Fatalf("Expected live volume with active stream")
	}

	// Read all states from live volume
	liveHelloAttr, err := volGetAttr(ctx, liveVol, "/hello.txt")
	if err != nil {
		t.Fatalf("live GetAttr /hello.txt failed: %v", err)
	}
	liveHelloData, _, _, err := volReadFile(ctx, liveVol, "/hello.txt", 0, 100)
	if err != nil {
		t.Fatalf("live ReadFile /hello.txt failed: %v", err)
	}

	liveRenamedAttr, err := volGetAttr(ctx, liveVol, "/docs/doc2_renamed.txt")
	if err != nil {
		t.Fatalf("live GetAttr /docs/doc2_renamed.txt failed: %v", err)
	}
	liveRenamedData, _, _, err := volReadFile(ctx, liveVol, "/docs/doc2_renamed.txt", 0, 100)
	if err != nil {
		t.Fatalf("live ReadFile /docs/doc2_renamed.txt failed: %v", err)
	}

	// 3. Create fresh volume and replay SDS stream from scratch
	streamID := StreamIDForVolume(volumeID)
	replayedVol := MustNewVolume(t, volumeID, backend, NewEventBroadcaster())
	legStream, err := walclient.Open(ctx, walDir, streamID, "")
	if err != nil {
		t.Fatalf("Failed to open WAL stream for replay: %v", err)
	}
	defer func() { _ = legStream.Close() }()

	recovered := legStream.RecoveredRecords()
	sr := sds.NewStreamReader("", streamID, sds.WithRecoveredRecords(recovered))
	changes, err := sr.FeedRecovered(0)
	if err != nil {
		t.Fatalf("FeedRecovered failed: %v", err)
	}

	for _, c := range changes {
		if err := replayedVol.ApplySDSChangeLocked(ctx, c); err != nil {
			t.Fatalf("ApplySDSChangeLocked failed: %v", err)
		}
	}

	// 4. Verify replayed volume matches live volume
	repHelloAttr, err := volGetAttr(ctx, replayedVol, "/hello.txt")
	if err != nil {
		t.Fatalf("replayed GetAttr /hello.txt failed: %v", err)
	}
	if repHelloAttr.GetInode().GetIno() != liveHelloAttr.GetInode().GetIno() || repHelloAttr.GetInode().GetSize() != liveHelloAttr.GetInode().GetSize() {
		t.Fatalf("Replayed /hello.txt attr mismatch: %+v vs %+v", repHelloAttr, liveHelloAttr)
	}
	repHelloData, _, _, err := volReadFile(ctx, replayedVol, "/hello.txt", 0, 100)
	if err != nil || string(repHelloData) != string(liveHelloData) {
		t.Fatalf("Replayed /hello.txt data mismatch: %q vs %q", string(repHelloData), string(liveHelloData))
	}

	repRenamedAttr, err := volGetAttr(ctx, replayedVol, "/docs/doc2_renamed.txt")
	if err != nil {
		t.Fatalf("replayed GetAttr /docs/doc2_renamed.txt failed: %v", err)
	}
	if repRenamedAttr.GetInode().GetIno() != liveRenamedAttr.GetInode().GetIno() || repRenamedAttr.GetInode().GetSize() != liveRenamedAttr.GetInode().GetSize() {
		t.Fatalf("Replayed doc2_renamed attr mismatch: %+v vs %+v", repRenamedAttr, liveRenamedAttr)
	}
	repRenamedData, _, _, err := volReadFile(ctx, replayedVol, "/docs/doc2_renamed.txt", 0, 100)
	if err != nil || string(repRenamedData) != string(liveRenamedData) {
		t.Fatalf("Replayed doc2_renamed data mismatch: %q vs %q", string(repRenamedData), string(liveRenamedData))
	}

	// Verify unlinked file and rmdir'd dir do not exist
	_, err = volGetAttr(ctx, replayedVol, "/docs/doc1.txt")
	if err == nil {
		t.Fatalf("Expected /docs/doc1.txt to not exist in replayed volume")
	}
	_, err = volGetAttr(ctx, replayedVol, "/docs/sub")
	if err == nil {
		t.Fatalf("Expected /docs/sub to not exist in replayed volume")
	}

	_ = server.Close()
}

func TestSDSCatOnObjectFSStream(t *testing.T) {
	ctx := t.Context()
	walDir := t.TempDir()
	backend := NewMemoryBackend()
	volumeID := "sds-cat-vol"

	server := NewServer(backend, WithServerWAL(walDir, "", walclient.Local))

	// Perform changes that register Inode, DirEntry, Content
	_, err := testMkdir(ctx, server, volumeID, "/cats", 0755, 0, 0)
	if err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}
	_, err = testCreateFile(ctx, server, volumeID, "/cats/fluffy.txt", 0644, []byte("meow meow"), 0, 0)
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}

	vol := mustGetVolume(t, server, volumeID)
	if vol == nil || vol.Stream() == nil {
		t.Fatalf("Expected active volume stream")
	}

	// Find the segment file
	files, err := filepath.Glob(filepath.Join(walDir, fmt.Sprintf("stream-%s-*.wal", vol.StreamID())))
	if err != nil || len(files) == 0 {
		t.Fatalf("No WAL segment file found in %s: %v", walDir, err)
	}

	// Read segment file with Decoder
	clientRecs, _, err := wal.ScanClientSegmentFile(files[0])
	if err != nil {
		t.Fatalf("Failed to scan segment file: %v", err)
	}

	reg := record.NewRegistry()
	dec := record.NewDecoder(record.WithDecoderRegistry(reg))

	var seenTypeDefs []string
	var seenOps int
	var seenCommits int

	for _, clientRec := range clientRecs {
		item, err := dec.Decode(clientRec.Payload)
		if err != nil {
			t.Fatalf("Decode error: %v", err)
		}
		if item.TypeID == record.TypeIDTypeDefinition {
			if def, ok := item.Message.(*sdsv1.TypeDefinition); ok {
				seenTypeDefs = append(seenTypeDefs, def.GetName())
			}
		}
		if item.TypeID == record.TypeIDOpRecord {
			seenOps++
		}
		if item.TypeID == record.TypeIDTxCommit {
			seenCommits++
		}
	}

	// Verify that TypeDefinitions for Inode, DirEntry, FileChunk were announced in-band
	hasInodeDef := slices.Contains(seenTypeDefs, "objectfs.v1alpha1.Inode")
	hasDirDef := slices.Contains(seenTypeDefs, "objectfs.v1alpha1.DirEntry")
	hasChunkDef := slices.Contains(seenTypeDefs, "objectfs.v1alpha1.FileChunk")

	if !hasInodeDef || !hasDirDef || !hasChunkDef {
		t.Fatalf("Expected Inode, DirEntry, and FileChunk TypeDefinitions announced in stream, got %v", seenTypeDefs)
	}

	if seenOps < 3 {
		t.Fatalf("Expected at least 3 OpRecords in stream, got %d", seenOps)
	}
	if seenCommits < 2 {
		t.Fatalf("Expected at least 2 TxCommits in stream, got %d", seenCommits)
	}

	_ = server.Close()
}

func TestTruncateUpwardAndSparseRead(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewServer(backend)
	volumeID := "test-vol-truncate"

	// Create a file with small initial content
	_, err := testCreateFile(ctx, server, volumeID, "/sparse.bin", 0644, []byte("hello world"), 0, 0)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	// Truncate up to 256KB across multiple chunks
	targetSize := int64(256 * 1024)
	truncResp, err := testTruncateFile(ctx, server, volumeID, "/sparse.bin", targetSize)
	if err != nil {
		t.Fatalf("Failed to truncate upward: %v", err)
	}
	if truncResp.GetError() != 0 {
		t.Fatalf("TruncateFile returned error: %d", truncResp.GetError())
	}

	// Read back whole file and verify size and contents
	readResp, err := testReadFile(ctx, server, volumeID, "/sparse.bin", 0, 0)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}
	if readResp.GetTotalSize() != targetSize {
		t.Fatalf("Expected total size %d, got %d", targetSize, readResp.GetTotalSize())
	}
	if int64(len(readResp.GetData())) != targetSize {
		t.Fatalf("Expected data length %d, got %d", targetSize, len(readResp.GetData()))
	}
	if string(readResp.GetData()[:11]) != "hello world" {
		t.Fatalf("Expected prefix 'hello world', got %q", string(readResp.GetData()[:11]))
	}
	// Verify that the extended portion is all zeroes
	for i, b := range readResp.GetData()[11:] {
		if b != 0 {
			t.Fatalf("Expected zero byte at offset %d, got %d", 11+i, b)
		}
	}
}

func TestLargeFileBlobFirstAndSparseWrite(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewServer(backend)
	volumeID := "large-vol"
	defer server.Close()

	// 1. Create a 1 GiB sparse file with writes at offset 0, 512 MiB, and 1 GiB - 64 KiB
	targetSize := int64(1 * 1024 * 1024 * 1024) // 1 GiB
	createResp, err := testCreateFile(ctx, server, volumeID, "/large-1gb.bin", 0644, nil, 0, 0)
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}
	if createResp.GetError() != 0 {
		t.Fatalf("CreateFile returned error: %d", createResp.GetError())
	}

	chunk0 := bytes.Repeat([]byte("A"), 64*1024)
	chunkMid := bytes.Repeat([]byte("M"), 64*1024)
	chunkEnd := bytes.Repeat([]byte("Z"), 64*1024)

	midOffset := int64(512 * 1024 * 1024)
	endOffset := targetSize - int64(64*1024)

	// Write at chunk 0
	w0, err := testWriteFile(ctx, server, volumeID, "/large-1gb.bin", 0, chunk0, pb.WriteMode_WRITE_MODE_UNSPECIFIED)
	if err != nil || w0.GetError() != 0 {
		t.Fatalf("WriteFile chunk 0 failed: %v (err=%d)", err, w0.GetError())
	}

	// Write at 512 MiB
	wMid, err := testWriteFile(ctx, server, volumeID, "/large-1gb.bin", midOffset, chunkMid, pb.WriteMode_WRITE_MODE_UNSPECIFIED)
	if err != nil || wMid.GetError() != 0 {
		t.Fatalf("WriteFile mid chunk failed: %v (err=%d)", err, wMid.GetError())
	}

	// Write at 1 GiB - 64 KiB
	wEnd, err := testWriteFile(ctx, server, volumeID, "/large-1gb.bin", endOffset, chunkEnd, pb.WriteMode_WRITE_MODE_UNSPECIFIED)
	if err != nil || wEnd.GetError() != 0 {
		t.Fatalf("WriteFile end chunk failed: %v (err=%d)", err, wEnd.GetError())
	}

	// Fsync to ensure all uploads and SDS transactions commit
	ino, _ := resolvePath(ctx, server, volumeID, "/large-1gb.bin")
	_, err = server.Fsync(ctx, &pb.FsyncRequest{
		VolumeId: volumeID,
		Inode:    ino,
	})
	if err != nil {
		t.Fatalf("Fsync failed: %v", err)
	}

	// Read back chunk 0
	resp0, err := testReadFile(ctx, server, volumeID, "/large-1gb.bin", 0, 64*1024)
	if err != nil || !bytes.Equal(resp0.GetData(), chunk0) {
		t.Fatalf("ReadFile chunk 0 mismatch (err=%v)", err)
	}
	if resp0.GetTotalSize() != targetSize {
		t.Fatalf("Expected total size %d, got %d", targetSize, resp0.GetTotalSize())
	}

	// Read back sparse hole between chunk 0 and mid chunk (e.g. at 256 MiB)
	respHole, err := testReadFile(ctx, server, volumeID, "/large-1gb.bin", 256*1024*1024, 1024)
	if err != nil {
		t.Fatalf("ReadFile hole failed: %v", err)
	}
	for i, b := range respHole.GetData() {
		if b != 0 {
			t.Fatalf("Expected zero byte in sparse hole at %d, got %d", i, b)
		}
	}

	// Read back mid chunk
	respMid, err := testReadFile(ctx, server, volumeID, "/large-1gb.bin", midOffset, 64*1024)
	if err != nil || !bytes.Equal(respMid.GetData(), chunkMid) {
		t.Fatalf("ReadFile mid chunk mismatch (err=%v)", err)
	}

	// Read back end chunk
	respEnd, err := testReadFile(ctx, server, volumeID, "/large-1gb.bin", endOffset, 64*1024)
	if err != nil || !bytes.Equal(respEnd.GetData(), chunkEnd) {
		t.Fatalf("ReadFile end chunk mismatch (err=%v)", err)
	}
}

func TestWriteThenTruncateAndUnlinkOrdering(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewServer(backend)
	volumeID := "order-vol"
	defer server.Close()

	// 1. Write then immediately Truncate
	createResp, err := testCreateFile(ctx, server, volumeID, "/truncate-test.bin", 0644, nil, 0, 0)
	if err != nil || createResp.GetError() != 0 {
		t.Fatalf("CreateFile failed: %v", err)
	}

	data128k := bytes.Repeat([]byte("T"), 128*1024)
	wResp, err := testWriteFile(ctx, server, volumeID, "/truncate-test.bin", 0, data128k, pb.WriteMode_WRITE_MODE_UNSPECIFIED)
	if err != nil || wResp.GetError() != 0 {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Immediately truncate to 32 KiB without fsync
	truncResp, err := testTruncateFile(ctx, server, volumeID, "/truncate-test.bin", 32*1024)
	if err != nil || truncResp.GetError() != 0 {
		t.Fatalf("TruncateFile failed: %v", err)
	}

	// Fsync to commit staged chunk 0
	tIno, _ := resolvePath(ctx, server, volumeID, "/truncate-test.bin")
	if _, err := server.Fsync(ctx, &pb.FsyncRequest{VolumeId: volumeID, Inode: tIno, Blocking: true}); err != nil {
		t.Fatalf("Fsync failed: %v", err)
	}

	// Read back and verify size is 32 KiB and content matches
	readResp, err := testReadFile(ctx, server, volumeID, "/truncate-test.bin", 0, 128*1024)
	if err != nil {
		t.Fatalf("ReadFile after truncate failed: %v", err)
	}
	if readResp.GetTotalSize() != 32*1024 {
		t.Fatalf("Expected total size 32768, got %d", readResp.GetTotalSize())
	}
	if !bytes.Equal(readResp.GetData(), data128k[:32*1024]) {
		t.Fatalf("Data mismatch after truncate")
	}

	// 2. Write then immediately Unlink
	createResp2, err := testCreateFile(ctx, server, volumeID, "/unlink-test.bin", 0644, nil, 0, 0)
	if err != nil || createResp2.GetError() != 0 {
		t.Fatalf("CreateFile failed: %v", err)
	}

	wResp2, err := testWriteFile(ctx, server, volumeID, "/unlink-test.bin", 0, data128k, pb.WriteMode_WRITE_MODE_UNSPECIFIED)
	if err != nil || wResp2.GetError() != 0 {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Immediately unlink without waiting for upload
	unResp, err := testUnlink(ctx, server, volumeID, "/unlink-test.bin")
	if err != nil || unResp.GetError() != 0 {
		t.Fatalf("Unlink failed: %v", err)
	}

	// Read should return error / not found
	readResp2, err := testReadFile(ctx, server, volumeID, "/unlink-test.bin", 0, 1024)
	if err == nil && readResp2.GetError() == 0 {
		t.Fatalf("Expected ReadFile on unlinked file to fail, got data len %d", len(readResp2.GetData()))
	}
}

func TestTinyFileInlineLogging(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewServer(backend)
	volumeID := "tiny-vol"
	defer server.Close()

	tinyData := []byte("tiny inline content < 4KB")
	createResp, err := testCreateFile(ctx, server, volumeID, "/tiny.txt", 0644, tinyData, 0, 0)
	if err != nil || createResp.GetError() != 0 {
		t.Fatalf("CreateFile tiny failed: %v", err)
	}

	// Fsync
	ino, _ := resolvePath(ctx, server, volumeID, "/tiny.txt")
	_, err = server.Fsync(ctx, &pb.FsyncRequest{
		VolumeId: volumeID,
		Inode:    ino,
	})
	if err != nil {
		t.Fatalf("Fsync tiny failed: %v", err)
	}

	// Read back
	readResp, err := testReadFile(ctx, server, volumeID, "/tiny.txt", 0, 100)
	if err != nil || !bytes.Equal(readResp.GetData(), tinyData) {
		t.Fatalf("ReadFile tiny mismatch: %q vs %q (err=%v)", string(readResp.GetData()), string(tinyData), err)
	}
}

func TestHugeSparseTruncateAndUnlink(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewServer(backend)
	volumeID := "huge-vol"
	defer server.Close()

	// Create file
	createResp, err := testCreateFile(ctx, server, volumeID, "/huge.bin", 0644, []byte("initial chunk data"), 0, 0)
	if err != nil || createResp.GetError() != 0 {
		t.Fatalf("CreateFile failed: %v (err=%d)", err, createResp.GetError())
	}

	// Grow file to 999,999,999,999,999 bytes (~1 PB, as in pjdfstest 12.t)
	hugeSize := int64(999999999999999)
	var mBefore runtime.MemStats
	runtime.ReadMemStats(&mBefore)

	truncResp, err := testTruncateFile(ctx, server, volumeID, "/huge.bin", hugeSize)
	if err != nil || truncResp.GetError() != 0 {
		t.Fatalf("TruncateFile to hugeSize failed: %v (err=%d)", err, truncResp.GetError())
	}

	var mAfter runtime.MemStats
	runtime.ReadMemStats(&mAfter)

	// Heap growth must remain tiny (under 10 MB), never allocating billions of chunk entries
	if mAfter.HeapAlloc > mBefore.HeapAlloc && (mAfter.HeapAlloc-mBefore.HeapAlloc) > 10*1024*1024 {
		t.Fatalf("Excessive heap growth on sparse truncate: allocated %d bytes", mAfter.HeapAlloc-mBefore.HeapAlloc)
	}

	// Stat to confirm size
	attrResp, err := testGetAttr(ctx, server, volumeID, "/huge.bin")
	if err != nil || attrResp.GetError() != 0 {
		t.Fatalf("GetAttr failed: %v", err)
	}
	if attrResp.GetAttr().GetInode().GetSize() != hugeSize {
		t.Fatalf("Expected size %d, got %d", hugeSize, attrResp.GetAttr().GetInode().GetSize())
	}

	// Read a few bytes from the end (sparse hole reads as zeros)
	readResp, err := testReadFile(ctx, server, volumeID, "/huge.bin", hugeSize-1024, 1024)
	if err != nil || readResp.GetError() != 0 {
		t.Fatalf("ReadFile near EOF failed: %v (err=%d)", err, readResp.GetError())
	}
	if len(readResp.GetData()) != 1024 {
		t.Fatalf("Expected 1024 bytes, got %d", len(readResp.GetData()))
	}
	for i, b := range readResp.GetData() {
		if b != 0 {
			t.Fatalf("Expected zero byte at %d, got %d", i, b)
		}
	}

	// Unlink should be instantaneous and bounded without iterating billions of chunk indices
	unResp, err := testUnlink(ctx, server, volumeID, "/huge.bin")
	if err != nil || unResp.GetError() != 0 {
		t.Fatalf("Unlink failed: %v (err=%d)", err, unResp.GetError())
	}
}

func TestSetAttrAndMetadataTimestamps(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewServer(backend)
	volumeID := "test-setattr-ts-vol"

	// Create a test file
	t0 := time.Now().Add(-1 * time.Hour)
	createResp, err := testCreateFile(ctx, server, volumeID, "/file.txt", 0644, []byte("hello world"), 100, 200)
	if err != nil || createResp.GetError() != 0 {
		t.Fatalf("CreateFile failed: %v", err)
	}
	attr := createResp.GetAttr()
	if attr.GetInode().GetUid() != 100 || attr.GetInode().GetGid() != 200 {
		t.Fatalf("Unexpected initial uid/gid: %d/%d", attr.GetInode().GetUid(), attr.GetInode().GetGid())
	}
	if attr.GetInode().GetAtime() == nil || attr.GetInode().GetCtime() == nil {
		t.Fatalf("Expected atime and ctime on created file")
	}

	// 1. Test chmod via SetAttr (preserving file type bits and setting setuid/sticky)
	newMode := uint32(0755 | syscall.S_ISUID | syscall.S_ISVTX)
	setResp, err := testSetAttr(ctx, server, volumeID, "/file.txt", &newMode, nil, nil, nil, false, nil, false, nil, false)
	if err != nil || setResp.GetError() != 0 {
		t.Fatalf("SetAttr chmod failed: %v", err)
	}
	if setResp.GetAttr().GetInode().GetMode()&07777 != (newMode & 07777) {
		t.Fatalf("Expected mode %o, got %o", newMode&07777, setResp.GetAttr().GetInode().GetMode()&07777)
	}
	if (setResp.GetAttr().GetInode().GetMode() & syscall.S_IFREG) == 0 {
		t.Fatalf("Expected S_IFREG bit retained")
	}

	// 2. Test chown via SetAttr
	newUid := uint32(500)
	newGid := uint32(600)
	setResp, err = testSetAttr(ctx, server, volumeID, "/file.txt", nil, &newUid, &newGid, nil, false, nil, false, nil, false)
	if err != nil || setResp.GetError() != 0 {
		t.Fatalf("SetAttr chown failed: %v", err)
	}
	if setResp.GetAttr().GetInode().GetUid() != 500 || setResp.GetAttr().GetInode().GetGid() != 600 {
		t.Fatalf("Expected uid/gid 500/600, got %d/%d", setResp.GetAttr().GetInode().GetUid(), setResp.GetAttr().GetInode().GetGid())
	}

	// 3. Test explicit timestamps via SetAttr
	explicitAtime := t0.Add(10 * time.Minute)
	explicitMtime := t0.Add(20 * time.Minute)
	explicitCtime := t0.Add(30 * time.Minute)
	setResp, err = testSetAttr(ctx, server, volumeID, "/file.txt", nil, nil, nil, &explicitAtime, false, &explicitMtime, false, &explicitCtime, false)
	if err != nil || setResp.GetError() != 0 {
		t.Fatalf("SetAttr timestamps failed: %v", err)
	}
	if setResp.GetAttr().GetInode().GetAtime().AsTime().Unix() != explicitAtime.Unix() {
		t.Fatalf("Expected atime %v, got %v", explicitAtime, setResp.GetAttr().GetInode().GetAtime().AsTime())
	}
	if setResp.GetAttr().GetInode().GetMtime().AsTime().Unix() != explicitMtime.Unix() {
		t.Fatalf("Expected mtime %v, got %v", explicitMtime, setResp.GetAttr().GetInode().GetMtime().AsTime())
	}
	if setResp.GetAttr().GetInode().GetCtime().AsTime().Unix() != explicitCtime.Unix() {
		t.Fatalf("Expected ctime %v, got %v", explicitCtime, setResp.GetAttr().GetInode().GetCtime().AsTime())
	}

	// 4. Test timestamp NOW via SetAttr
	beforeNow := time.Now().Add(-1 * time.Second)
	setResp, err = testSetAttr(ctx, server, volumeID, "/file.txt", nil, nil, nil, nil, true, nil, true, nil, false)
	if err != nil || setResp.GetError() != 0 {
		t.Fatalf("SetAttr now failed: %v", err)
	}
	if setResp.GetAttr().GetInode().GetAtime().AsTime().Before(beforeNow) {
		t.Fatalf("Expected atime updated to now")
	}
	if setResp.GetAttr().GetInode().GetMtime().AsTime().Before(beforeNow) {
		t.Fatalf("Expected mtime updated to now")
	}
	if setResp.GetAttr().GetInode().GetCtime().AsTime().Before(beforeNow) {
		t.Fatalf("Expected ctime updated to now")
	}
}

func TestSetgidInheritance(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewServer(backend)
	volumeID := "test-setgid-vol"

	// Create parent directory with setgid bit (02770) and GID 3000
	parentMode := uint32(0770 | syscall.S_ISGID)
	parentGid := uint32(3000)
	parentUid := uint32(1000)
	mkdirResp, err := testMkdir(ctx, server, volumeID, "/setgid_dir", parentMode, parentUid, parentGid)
	if err != nil || mkdirResp.GetError() != 0 {
		t.Fatalf("Mkdir parent setgid_dir failed: %v", err)
	}
	if mkdirResp.GetAttr().GetInode().GetGid() != 3000 {
		t.Fatalf("Expected parent GID 3000, got %d", mkdirResp.GetAttr().GetInode().GetGid())
	}
	if mkdirResp.GetAttr().GetInode().GetMode()&02000 == 0 {
		t.Fatalf("Expected setgid bit on parent directory")
	}

	// 1. Create subdirectory inside setgid dir with caller GID 4000
	// Subdirectory must inherit parent GID 3000 AND setgid bit 02000
	subDirResp, err := testMkdir(ctx, server, volumeID, "/setgid_dir/subdir", 0755, 1000, 4000)
	if err != nil || subDirResp.GetError() != 0 {
		t.Fatalf("Mkdir subdir failed: %v", err)
	}
	if subDirResp.GetAttr().GetInode().GetGid() != 3000 {
		t.Fatalf("Expected subdir to inherit GID 3000, got %d", subDirResp.GetAttr().GetInode().GetGid())
	}
	if subDirResp.GetAttr().GetInode().GetMode()&02000 == 0 {
		t.Fatalf("Expected subdir to inherit setgid bit 02000, got %o", subDirResp.GetAttr().GetInode().GetMode())
	}

	// 2. Create regular file inside setgid dir with caller GID 4000
	// Regular file must inherit parent GID 3000, but NOT setgid bit
	fileResp, err := testCreateFile(ctx, server, volumeID, "/setgid_dir/child.txt", 0644, []byte("data"), 1000, 4000)
	if err != nil || fileResp.GetError() != 0 {
		t.Fatalf("CreateFile child.txt failed: %v", err)
	}
	if fileResp.GetAttr().GetInode().GetGid() != 3000 {
		t.Fatalf("Expected file to inherit GID 3000, got %d", fileResp.GetAttr().GetInode().GetGid())
	}
	if fileResp.GetAttr().GetInode().GetMode()&02000 != 0 {
		t.Fatalf("Expected regular file NOT to inherit setgid bit, got %o", fileResp.GetAttr().GetInode().GetMode())
	}
}

func TestTarExtractionPreservation(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewServer(backend)
	volumeID := "test-tar-extract-vol"

	// Create an in-memory tar archive with various modes, owners, timestamps
	var tarBuf bytes.Buffer
	tw := tar.NewWriter(&tarBuf)

	t1 := time.Date(2025, 5, 1, 10, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 5, 2, 11, 0, 0, 0, time.UTC)
	t3 := time.Date(2025, 5, 3, 12, 0, 0, 0, time.UTC)

	files := []struct {
		header  *tar.Header
		content []byte
	}{
		{
			header: &tar.Header{
				Typeflag: tar.TypeDir,
				Name:     "mydir",
				Mode:     0750,
				Uid:      101,
				Gid:      201,
				ModTime:  t1,
			},
		},
		{
			header: &tar.Header{
				Typeflag: tar.TypeReg,
				Name:     "mydir/app.bin",
				Mode:     0755,
				Uid:      102,
				Gid:      202,
				Size:     11,
				ModTime:  t2,
			},
			content: []byte("binary-data"),
		},
		{
			header: &tar.Header{
				Typeflag: tar.TypeReg,
				Name:     "config.json",
				Mode:     0600,
				Uid:      103,
				Gid:      203,
				Size:     13,
				ModTime:  t3,
			},
			content: []byte(`{"key":"val"}`),
		},
	}

	for _, f := range files {
		if err := tw.WriteHeader(f.header); err != nil {
			t.Fatalf("WriteHeader failed: %v", err)
		}
		if len(f.content) > 0 {
			if _, err := tw.Write(f.content); err != nil {
				t.Fatalf("Write content failed: %v", err)
			}
		}
	}
	tw.Close()

	// Simulate tar extraction into ObjectFS
	tr := tar.NewReader(&tarBuf)
	var dirHeaders []*tar.Header
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("tar.Next failed: %v", err)
		}

		p := "/" + strings.TrimPrefix(hdr.Name, "/")
		if hdr.Typeflag == tar.TypeDir {
			resp, err := testMkdir(ctx, server, volumeID, p, uint32(hdr.Mode), uint32(hdr.Uid), uint32(hdr.Gid))
			if err != nil || resp.GetError() != 0 {
				t.Fatalf("testMkdir for %s failed: %v (err=%d)", p, err, resp.GetError())
			}
			dirHeaders = append(dirHeaders, hdr)
		} else {
			data, err := io.ReadAll(tr)
			if err != nil {
				t.Fatalf("ReadAll for %s failed: %v", p, err)
			}
			resp, err := testCreateFile(ctx, server, volumeID, p, uint32(hdr.Mode), data, uint32(hdr.Uid), uint32(hdr.Gid))
			if err != nil || resp.GetError() != 0 {
				t.Fatalf("testCreateFile for %s failed: %v (err=%d)", p, err, resp.GetError())
			}
			modeVal := uint32(hdr.Mode)
			uidVal := uint32(hdr.Uid)
			gidVal := uint32(hdr.Gid)
			mtimeVal := hdr.ModTime
			setResp, err := testSetAttr(ctx, server, volumeID, p, &modeVal, &uidVal, &gidVal, &mtimeVal, false, &mtimeVal, false, nil, false)
			if err != nil || setResp.GetError() != 0 {
				t.Fatalf("SetAttr for %s failed: %v (err=%d)", p, err, setResp.GetError())
			}
		}
	}

	// Like tar -x, apply directory metadata/timestamps in a deferred pass after children are extracted
	for _, hdr := range dirHeaders {
		p := "/" + strings.TrimPrefix(hdr.Name, "/")
		modeVal := uint32(hdr.Mode)
		uidVal := uint32(hdr.Uid)
		gidVal := uint32(hdr.Gid)
		mtimeVal := hdr.ModTime
		setResp, err := testSetAttr(ctx, server, volumeID, p, &modeVal, &uidVal, &gidVal, &mtimeVal, false, &mtimeVal, false, nil, false)
		if err != nil || setResp.GetError() != 0 {
			t.Fatalf("SetAttr for directory %s failed: %v (err=%d)", p, err, setResp.GetError())
		}
	}

	// Verify all extracted files match their original metadata and content
	for _, f := range files {
		p := "/" + strings.TrimPrefix(f.header.Name, "/")
		attrResp, err := testGetAttr(ctx, server, volumeID, p)
		if err != nil || attrResp.GetError() != 0 {
			t.Fatalf("GetAttr for %s failed: %v", p, err)
		}
		attr := attrResp.GetAttr()
		if attr.GetInode().GetMode()&07777 != uint32(f.header.Mode&07777) {
			t.Fatalf("File %s mode mismatch: got %o, want %o", p, attr.GetInode().GetMode()&07777, f.header.Mode&07777)
		}
		if attr.GetInode().GetUid() != uint32(f.header.Uid) {
			t.Fatalf("File %s uid mismatch: got %d, want %d", p, attr.GetInode().GetUid(), f.header.Uid)
		}
		if attr.GetInode().GetGid() != uint32(f.header.Gid) {
			t.Fatalf("File %s gid mismatch: got %d, want %d", p, attr.GetInode().GetGid(), f.header.Gid)
		}
		if attr.GetInode().GetMtime().AsTime().Unix() != f.header.ModTime.Unix() {
			t.Fatalf("File %s mtime mismatch: got %v, want %v", p, attr.GetInode().GetMtime().AsTime(), f.header.ModTime)
		}

		if len(f.content) > 0 {
			readResp, err := testReadFile(ctx, server, volumeID, p, 0, int64(len(f.content)))
			if err != nil || readResp.GetError() != 0 {
				t.Fatalf("ReadFile for %s failed: %v", p, err)
			}
			if !bytes.Equal(readResp.GetData(), f.content) {
				t.Fatalf("File %s content mismatch", p)
			}
		}
	}
}

func TestSnapshotRestartRecoveryFromSnapshotPlusStream(t *testing.T) {
	ctx := t.Context()
	walDir := t.TempDir()
	backend := NewMemoryBackend()
	volumeID := "test-sds-restart-recovery"

	// 1. Initial server instance
	server1 := NewServer(backend, WithServerWAL(walDir, "", walclient.Local))

	_, err := testMkdir(ctx, server1, volumeID, "/base", 0755, 0, 0)
	if err != nil {
		t.Fatalf("Mkdir /base failed: %v", err)
	}
	_, err = testCreateFile(ctx, server1, volumeID, "/base/snapfile.txt", 0644, []byte("content from snapshot"), 0, 0)
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}

	// 2. Take Snapshot 1
	snapResp, err := server1.CreateSnapshot(ctx, &pb.CreateSnapshotRequest{VolumeId: volumeID})
	if err != nil {
		t.Fatalf("CreateSnapshot failed: %v", err)
	}
	snapName := snapResp.GetSnapshotName()

	// 3. Mutate after Snapshot 1 (logged in SDS stream)
	_, err = testCreateFile(ctx, server1, volumeID, "/base/after_snap.txt", 0644, []byte("content appended after snapshot"), 0, 0)
	if err != nil {
		t.Fatalf("CreateFile after snapshot failed: %v", err)
	}

	_, err = testWriteFile(ctx, server1, volumeID, "/base/snapfile.txt", 0, []byte("modified snapshot file content!"), pb.WriteMode_WRITE_MODE_UNSPECIFIED)
	if err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}
	snapIno, _ := resolvePath(ctx, server1, volumeID, "/base/snapfile.txt")
	if _, err := server1.Fsync(ctx, &pb.FsyncRequest{VolumeId: volumeID, Inode: snapIno, Blocking: true}); err != nil {
		t.Fatalf("Fsync failed: %v", err)
	}

	_, err = testMkdir(ctx, server1, volumeID, "/newdir", 0755, 0, 0)
	if err != nil {
		t.Fatalf("Mkdir /newdir failed: %v", err)
	}

	// Close initial server
	_ = server1.Close()

	// 4. Start second server instance simulating crash restart recovery
	server2 := NewServer(backend, WithServerWAL(walDir, "", walclient.Local))

	// Verify volume recovered from snapshot + stream suffix
	vol2 := mustGetVolume(t, server2, volumeID)
	if vol2 == nil {
		t.Fatalf("Expected volume to be loaded on restart")
	}

	// Verify modified snapshot file
	read1, err := testReadFile(ctx, server2, volumeID, "/base/snapfile.txt", 0, 1024)
	if err != nil {
		t.Fatalf("ReadFile /base/snapfile.txt failed: %v", err)
	}
	if string(read1.Data) != "modified snapshot file content!" {
		t.Fatalf("Modified snapshot file mismatch: got %q", string(read1.Data))
	}

	// Verify file created after snapshot
	read2, err := testReadFile(ctx, server2, volumeID, "/base/after_snap.txt", 0, 1024)
	if err != nil {
		t.Fatalf("ReadFile /base/after_snap.txt failed: %v", err)
	}
	if string(read2.Data) != "content appended after snapshot" {
		t.Fatalf("After snap file mismatch: got %q", string(read2.Data))
	}

	// Verify new directory
	dirResp, err := testReadDir(ctx, server2, volumeID, "/")
	if err != nil {
		t.Fatalf("ReadDir / failed: %v", err)
	}
	var entryNames []string
	for _, e := range dirResp.Entries {
		entryNames = append(entryNames, e.Name)
	}
	if !slices.Contains(entryNames, "base") || !slices.Contains(entryNames, "newdir") {
		t.Fatalf("Expected / to contain 'base' and 'newdir', got %v", entryNames)
	}

	// Verify ListSnapshots still discovers Snapshot 1
	listResp, err := server2.ListSnapshots(ctx, &pb.ListSnapshotsRequest{VolumeId: volumeID})
	if err != nil {
		t.Fatalf("ListSnapshots on recovered server failed: %v", err)
	}
	if len(listResp.GetSnapshots()) != 1 || listResp.GetSnapshots()[0].GetName() != snapName {
		t.Fatalf("Expected snapshot %s, got %v", snapName, listResp.GetSnapshots())
	}
	if listResp.GetSnapshots()[0].GetPosition() == 0 {
		t.Fatalf("Expected non-zero position on listed snapshot")
	}

	_ = server2.Close()
}

func TestSnapshotSafePositionAndS3Watermark(t *testing.T) {
	ctx := t.Context()
	bufDir := t.TempDir()
	_, target, cleanup := startTestWalBufferServer(t, bufDir)
	defer cleanup()

	clientDir := t.TempDir()
	backend := NewMemoryBackend()
	volumeID := "test-sds-safe-watermark"

	server := NewServer(backend, WithServerWAL(clientDir, target, walclient.Permanent))
	defer func() { _ = server.Close() }()

	// 1. Create file with Permanent durability
	_, err := testCreateFile(ctx, server, volumeID, "/safe_test.txt", 0644, []byte("safe watermark test content"), 0, 0)
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}

	vol := mustGetVolume(t, server, volumeID)
	if vol == nil || vol.Stream() == nil {
		t.Fatalf("Expected active volume and stream")
	}

	// 2. Create snapshot
	snapResp, err := server.CreateSnapshot(ctx, &pb.CreateSnapshotRequest{VolumeId: volumeID})
	if err != nil {
		t.Fatalf("CreateSnapshot failed: %v", err)
	}
	snapName := snapResp.GetSnapshotName()

	// 3. Verify snapshot position does not exceed s3Seq
	_, _, s3Seq := vol.Stream().Watermarks()
	snapPos := snapResp.GetSnapshot().GetPosition()
	if snapPos == 0 {
		posStr := strings.TrimSuffix(strings.TrimSuffix(snapName, ".sqlite"), ".snap")
		snapPos, err = strconv.ParseUint(posStr, 10, 64)
		if err != nil {
			t.Fatalf("Failed to parse position from snapshot name %s: %v", snapName, err)
		}
	}

	if snapPos > s3Seq {
		t.Fatalf("Snapshot position %d exceeds permanent s3Seq watermark %d", snapPos, s3Seq)
	}
	if snapPos == 0 {
		t.Fatalf("Expected non-zero snapshot position")
	}
}

func TestNameLengthLimit(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewServer(backend)
	volumeID := "vol-name-limit"

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

		// 1. Lookup non-existent 255-byte name -> should return ENOENT, NOT ENAMETOOLONG
		lookupResp, err := server.Lookup(ctx, &pb.LookupRequest{
			VolumeId:    volumeID,
			ParentInode: 1,
			Name:        name,
		})
		if err != nil {
			t.Fatalf("[%s] Lookup error: %v", desc, err)
		}
		if lookupResp.GetError() != int32(syscall.ENOENT) {
			t.Fatalf("[%s] Expected ENOENT for Lookup, got error code %d", desc, lookupResp.GetError())
		}

		// 2. Mkdir 255-byte name -> succeeds
		mkdirResp, err := server.Mkdir(ctx, &pb.MkdirRequest{
			VolumeId:    volumeID,
			ParentInode: 1,
			Name:        name,
			Mode:        0755,
		})
		if err != nil || mkdirResp.GetError() != 0 {
			t.Fatalf("[%s] Mkdir failed: %v (err=%d)", desc, err, mkdirResp.GetError())
		}

		// 3. Lookup existing 255-byte name -> succeeds
		lookupResp, err = server.Lookup(ctx, &pb.LookupRequest{
			VolumeId:    volumeID,
			ParentInode: 1,
			Name:        name,
		})
		if err != nil || lookupResp.GetError() != 0 {
			t.Fatalf("[%s] Lookup existing dir failed: %v (err=%d)", desc, err, lookupResp.GetError())
		}

		// 4. Rmdir 255-byte name -> succeeds
		rmdirResp, err := server.Rmdir(ctx, &pb.RmdirRequest{
			VolumeId:    volumeID,
			ParentInode: 1,
			Name:        name,
		})
		if err != nil || rmdirResp.GetError() != 0 {
			t.Fatalf("[%s] Rmdir failed: %v (err=%d)", desc, err, rmdirResp.GetError())
		}

		// 5. CreateFile 255-byte name -> succeeds
		createResp, err := server.CreateFile(ctx, &pb.CreateFileRequest{
			VolumeId:    volumeID,
			ParentInode: 1,
			Name:        name,
			Mode:        0644,
		})
		if err != nil || createResp.GetError() != 0 {
			t.Fatalf("[%s] CreateFile failed: %v (err=%d)", desc, err, createResp.GetError())
		}

		// 6. Rename 255-byte name -> another 255-byte name succeeds
		renamedName := strings.Repeat("b", 255)
		renameResp, err := server.Rename(ctx, &pb.RenameRequest{
			VolumeId:       volumeID,
			OldParentInode: 1,
			OldName:        name,
			NewParentInode: 1,
			NewName:        renamedName,
		})
		if err != nil || renameResp.GetError() != 0 {
			t.Fatalf("[%s] Rename to 255-byte name failed: %v (err=%d)", desc, err, renameResp.GetError())
		}

		// 7. Unlink renamed 255-byte file -> succeeds
		unlinkResp, err := server.Unlink(ctx, &pb.UnlinkRequest{
			VolumeId:    volumeID,
			ParentInode: 1,
			Name:        renamedName,
		})
		if err != nil || unlinkResp.GetError() != 0 {
			t.Fatalf("[%s] Unlink failed: %v (err=%d)", desc, err, unlinkResp.GetError())
		}

		// 8. ResolvePath with 255-byte component -> does not fail with ENAMETOOLONG
		_, err = resolvePath(ctx, server, volumeID, "/"+name)
		if err != nil && !errors.Is(err, syscall.ENOENT) {
			t.Fatalf("[%s] ResolvePath returned unexpected error: %v", desc, err)
		}
	}

	for desc, name := range tooLongNames {
		if len(name) < 256 {
			t.Fatalf("%s length is %d, expected >= 256", desc, len(name))
		}

		// 1. Lookup 256-byte name -> ENAMETOOLONG
		lookupResp, err := server.Lookup(ctx, &pb.LookupRequest{
			VolumeId:    volumeID,
			ParentInode: 1,
			Name:        name,
		})
		if err != nil {
			t.Fatalf("[%s] Lookup returned unexpected RPC error: %v", desc, err)
		}
		if lookupResp.GetError() != int32(syscall.ENAMETOOLONG) {
			t.Fatalf("[%s] Expected ENAMETOOLONG (%d) for Lookup, got %d", desc, syscall.ENAMETOOLONG, lookupResp.GetError())
		}

		// 2. Mkdir 256-byte name -> ENAMETOOLONG
		mkdirResp, err := server.Mkdir(ctx, &pb.MkdirRequest{
			VolumeId:    volumeID,
			ParentInode: 1,
			Name:        name,
			Mode:        0755,
		})
		if err != nil {
			t.Fatalf("[%s] Mkdir returned unexpected RPC error: %v", desc, err)
		}
		if mkdirResp.GetError() != int32(syscall.ENAMETOOLONG) {
			t.Fatalf("[%s] Expected ENAMETOOLONG (%d) for Mkdir, got %d", desc, syscall.ENAMETOOLONG, mkdirResp.GetError())
		}

		// 3. CreateFile 256-byte name -> ENAMETOOLONG
		createResp, err := server.CreateFile(ctx, &pb.CreateFileRequest{
			VolumeId:    volumeID,
			ParentInode: 1,
			Name:        name,
			Mode:        0644,
		})
		if err != nil {
			t.Fatalf("[%s] CreateFile returned unexpected RPC error: %v", desc, err)
		}
		if createResp.GetError() != int32(syscall.ENAMETOOLONG) {
			t.Fatalf("[%s] Expected ENAMETOOLONG (%d) for CreateFile, got %d", desc, syscall.ENAMETOOLONG, createResp.GetError())
		}

		// 4. Unlink 256-byte name -> ENAMETOOLONG
		unlinkResp, err := server.Unlink(ctx, &pb.UnlinkRequest{
			VolumeId:    volumeID,
			ParentInode: 1,
			Name:        name,
		})
		if err != nil {
			t.Fatalf("[%s] Unlink returned unexpected RPC error: %v", desc, err)
		}
		if unlinkResp.GetError() != int32(syscall.ENAMETOOLONG) {
			t.Fatalf("[%s] Expected ENAMETOOLONG (%d) for Unlink, got %d", desc, syscall.ENAMETOOLONG, unlinkResp.GetError())
		}

		// 5. Rmdir 256-byte name -> ENAMETOOLONG
		rmdirResp, err := server.Rmdir(ctx, &pb.RmdirRequest{
			VolumeId:    volumeID,
			ParentInode: 1,
			Name:        name,
		})
		if err != nil {
			t.Fatalf("[%s] Rmdir returned unexpected RPC error: %v", desc, err)
		}
		if rmdirResp.GetError() != int32(syscall.ENAMETOOLONG) {
			t.Fatalf("[%s] Expected ENAMETOOLONG (%d) for Rmdir, got %d", desc, syscall.ENAMETOOLONG, rmdirResp.GetError())
		}

		// 6. Rename with 256-byte oldName -> ENAMETOOLONG
		renameResp, err := server.Rename(ctx, &pb.RenameRequest{
			VolumeId:       volumeID,
			OldParentInode: 1,
			OldName:        name,
			NewParentInode: 1,
			NewName:        "valid.txt",
		})
		if err != nil {
			t.Fatalf("[%s] Rename returned unexpected RPC error: %v", desc, err)
		}
		if renameResp.GetError() != int32(syscall.ENAMETOOLONG) {
			t.Fatalf("[%s] Expected ENAMETOOLONG (%d) for Rename oldName, got %d", desc, syscall.ENAMETOOLONG, renameResp.GetError())
		}

		// 7. Rename with 256-byte newName -> ENAMETOOLONG
		renameResp2, err := server.Rename(ctx, &pb.RenameRequest{
			VolumeId:       volumeID,
			OldParentInode: 1,
			OldName:        "valid.txt",
			NewParentInode: 1,
			NewName:        name,
		})
		if err != nil {
			t.Fatalf("[%s] Rename returned unexpected RPC error: %v", desc, err)
		}
		if renameResp2.GetError() != int32(syscall.ENAMETOOLONG) {
			t.Fatalf("[%s] Expected ENAMETOOLONG (%d) for Rename newName, got %d", desc, syscall.ENAMETOOLONG, renameResp2.GetError())
		}

		// 8. ResolvePath with 256-byte component -> ENAMETOOLONG
		_, err = resolvePath(ctx, server, volumeID, "/"+name)
		if !errors.Is(err, syscall.ENAMETOOLONG) {
			t.Fatalf("[%s] Expected ENAMETOOLONG for ResolvePath, got %v", desc, err)
		}
	}
}

func TestControllerMknodSpecialFiles(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewServer(backend)
	volumeID := "test-mknod-vol"

	// Create a FIFO
	fifoResp, err := server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:    volumeID,
		ParentInode: 1,
		Name:        "my_fifo",
		Mode:        syscall.S_IFIFO | 0644,
		Uid:         1000,
		Gid:         1000,
	})
	if err != nil || fifoResp.GetError() != 0 {
		t.Fatalf("CreateFile FIFO failed: err=%v, code=%d", err, fifoResp.GetError())
	}
	if (fifoResp.GetAttr().GetInode().GetMode() & syscall.S_IFMT) != syscall.S_IFIFO {
		t.Fatalf("Expected S_IFIFO in attr mode, got %o", fifoResp.GetAttr().GetInode().GetMode())
	}
	if fifoResp.GetAttr().GetInode().GetSize() != 0 {
		t.Fatalf("Expected FIFO size 0, got %d", fifoResp.GetAttr().GetInode().GetSize())
	}

	// Create a Char device node
	chrResp, err := server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:    volumeID,
		ParentInode: 1,
		Name:        "my_chr",
		Mode:        syscall.S_IFCHR | 0660,
		Rdev:        0x0103, // null device (major 1, minor 3)
		Uid:         0,
		Gid:         0,
	})
	if err != nil || chrResp.GetError() != 0 {
		t.Fatalf("CreateFile CHR failed: err=%v, code=%d", err, chrResp.GetError())
	}
	if (chrResp.GetAttr().GetInode().GetMode() & syscall.S_IFMT) != syscall.S_IFCHR {
		t.Fatalf("Expected S_IFCHR in attr mode, got %o", chrResp.GetAttr().GetInode().GetMode())
	}
	if chrResp.GetAttr().GetInode().GetRdev() != 0x0103 {
		t.Fatalf("Expected Rdev 0x0103, got 0x%x", chrResp.GetAttr().GetInode().GetRdev())
	}

	// Create a Block device node
	blkResp, err := server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:    volumeID,
		ParentInode: 1,
		Name:        "my_blk",
		Mode:        syscall.S_IFBLK | 0660,
		Rdev:        0x0801, // sda1
		Uid:         0,
		Gid:         0,
	})
	if err != nil || blkResp.GetError() != 0 {
		t.Fatalf("CreateFile BLK failed: err=%v, code=%d", err, blkResp.GetError())
	}
	if (blkResp.GetAttr().GetInode().GetMode() & syscall.S_IFMT) != syscall.S_IFBLK {
		t.Fatalf("Expected S_IFBLK in attr mode, got %o", blkResp.GetAttr().GetInode().GetMode())
	}
	if blkResp.GetAttr().GetInode().GetRdev() != 0x0801 {
		t.Fatalf("Expected Rdev 0x0801, got 0x%x", blkResp.GetAttr().GetInode().GetRdev())
	}

	// Create a Socket
	sockResp, err := server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:    volumeID,
		ParentInode: 1,
		Name:        "my_sock",
		Mode:        syscall.S_IFSOCK | 0777,
		Uid:         1000,
		Gid:         1000,
	})
	if err != nil || sockResp.GetError() != 0 {
		t.Fatalf("CreateFile SOCK failed: err=%v, code=%d", err, sockResp.GetError())
	}
	if (sockResp.GetAttr().GetInode().GetMode() & syscall.S_IFMT) != syscall.S_IFSOCK {
		t.Fatalf("Expected S_IFSOCK in attr mode, got %o", sockResp.GetAttr().GetInode().GetMode())
	}

	// Attempt to recreate existing FIFO -> EEXIST
	dupResp, err := server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:    volumeID,
		ParentInode: 1,
		Name:        "my_fifo",
		Mode:        syscall.S_IFIFO | 0644,
	})
	if err != nil || dupResp.GetError() != int32(syscall.EEXIST) {
		t.Fatalf("Expected EEXIST recreating FIFO, got err=%v, code=%d", err, dupResp.GetError())
	}

	// ReadDir verification
	readDirResp, err := server.ReadDir(ctx, &pb.ReadDirRequest{
		VolumeId: volumeID,
		Inode:    1,
	})
	if err != nil || readDirResp.GetError() != 0 {
		t.Fatalf("ReadDir failed: err=%v, code=%d", err, readDirResp.GetError())
	}
	found := make(map[string]*pb.EntryAttr)
	for _, entry := range readDirResp.GetEntries() {
		found[entry.GetName()] = entry
	}
	if entry, ok := found["my_fifo"]; !ok || (entry.GetInode().GetMode()&syscall.S_IFMT) != syscall.S_IFIFO {
		t.Fatalf("ReadDir did not return valid FIFO entry: %v", entry)
	}
	if entry, ok := found["my_chr"]; !ok || (entry.GetInode().GetMode()&syscall.S_IFMT) != syscall.S_IFCHR || entry.GetInode().GetRdev() != 0x0103 {
		t.Fatalf("ReadDir did not return valid CHR entry: %v", entry)
	}
	if entry, ok := found["my_blk"]; !ok || (entry.GetInode().GetMode()&syscall.S_IFMT) != syscall.S_IFBLK || entry.GetInode().GetRdev() != 0x0801 {
		t.Fatalf("ReadDir did not return valid BLK entry: %v", entry)
	}
	if entry, ok := found["my_sock"]; !ok || (entry.GetInode().GetMode()&syscall.S_IFMT) != syscall.S_IFSOCK {
		t.Fatalf("ReadDir did not return valid SOCK entry: %v", entry)
	}

	// GetAttr verification
	chrAttr, err := server.GetAttr(ctx, &pb.GetAttrRequest{VolumeId: volumeID, Inode: chrResp.GetAttr().GetInode().GetIno()})
	if err != nil || chrAttr.GetError() != 0 {
		t.Fatalf("GetAttr CHR failed: err=%v, code=%d", err, chrAttr.GetError())
	}
	if chrAttr.GetAttr().GetInode().GetRdev() != 0x0103 {
		t.Fatalf("GetAttr CHR Rdev expected 0x0103, got 0x%x", chrAttr.GetAttr().GetInode().GetRdev())
	}
}

func TestServerMetadataDirPersistenceAcrossRestart(t *testing.T) {
	ctx := t.Context()
	dataDir := t.TempDir()
	walDir := filepath.Join(dataDir, "wal")
	metadataDir := filepath.Join(dataDir, "metadata")
	backend := NewMemoryBackend()
	volumeID := "test-server-persistence-vol"

	// 1. First server instance writes files
	server1 := NewServer(backend,
		WithServerWAL(walDir, "", walclient.Local),
		WithServerMetadataDir(metadataDir),
	)

	createResp, err := testCreateFile(ctx, server1, volumeID, "/persist.txt", 0644, []byte("hello persistent disk"), 0, 0)
	if err != nil || createResp.GetError() != 0 {
		t.Fatalf("CreateFile failed on server1: err=%v, code=%d", err, createResp.GetError())
	}
	fileIno := createResp.GetAttr().GetInode().GetIno()

	mkdirResp, err := testMkdir(ctx, server1, volumeID, "/sub", 0755, 0, 0)
	if err != nil || mkdirResp.GetError() != 0 {
		t.Fatalf("Mkdir failed on server1: err=%v, code=%d", err, mkdirResp.GetError())
	}

	// Verify SQLite database exists on disk under metadataDir/volumeID
	sqlitePath := filepath.Join(metadataDir, volumeID, "metadata.sqlite")
	if _, err := os.Stat(sqlitePath); err != nil {
		t.Fatalf("Expected SQLite DB at %s, got error: %v", sqlitePath, err)
	}

	// Close first server
	if err := server1.Close(); err != nil {
		t.Fatalf("server1.Close failed: %v", err)
	}

	// 2. Second server instance starts with same WAL and metadata dirs
	server2 := NewServer(backend,
		WithServerWAL(walDir, "", walclient.Local),
		WithServerMetadataDir(metadataDir),
	)
	defer server2.Close()

	// Read back file
	readResp, err := server2.ReadFile(ctx, &pb.ReadFileRequest{
		VolumeId: volumeID,
		Inode:    fileIno,
		Offset:   0,
		Size:     100,
	})
	if err != nil || readResp.GetError() != 0 {
		t.Fatalf("ReadFile failed on server2: err=%v, code=%d", err, readResp.GetError())
	}
	if string(readResp.GetData()) != "hello persistent disk" {
		t.Fatalf("Expected content 'hello persistent disk', got %q", string(readResp.GetData()))
	}

	// Lookup directory
	lookupResp, err := server2.Lookup(ctx, &pb.LookupRequest{
		VolumeId:    volumeID,
		ParentInode: 1,
		Name:        "sub",
	})
	if err != nil || lookupResp.GetError() != 0 {
		t.Fatalf("Lookup 'sub' failed on server2: err=%v, code=%d", err, lookupResp.GetError())
	}
	if !lookupResp.GetAttr().GetInode().GetIsDir() {
		t.Fatalf("Expected 'sub' to be a directory on server2")
	}
}

func TestVolumeIDValidation(t *testing.T) {
	validIDs := []string{
		"vol-123",
		"pvc-a0b1c2d3-e4f5-6789-abcd-ef0123456789",
		"my_volume.name-123",
		"vol",
		"12345",
	}
	for _, id := range validIDs {
		if !isValidVolumeID(id) {
			t.Errorf("Expected valid volumeID %q to pass validation", id)
		}
	}

	invalidIDs := []string{
		"",
		".",
		"..",
		"../escape",
		"../../etc/passwd",
		"vol/sub",
		"vol$bad",
		"vol name",
		"vol\x00bad",
		strings.Repeat("a", 256),
	}
	for _, id := range invalidIDs {
		if isValidVolumeID(id) {
			t.Errorf("Expected invalid volumeID %q to fail validation", id)
		}
	}

	ctx := t.Context()
	server := NewServer(NewMemoryBackend())
	for _, id := range invalidIDs {
		_, err := server.GetAttr(ctx, &pb.GetAttrRequest{VolumeId: id, Inode: 1})
		if err == nil {
			t.Errorf("Expected GetAttr with invalid volumeID %q to fail", id)
		}
	}
}

// TestAuthoritativeRecoveryOnNewNodeWithoutSnapshot tests that when a controller moves to a new node
// with an empty local directory, it recovers all writes acknowledged at witness from the wal-buffer,
// even when no snapshots have been published (issue #185).
func TestAuthoritativeRecoveryOnNewNodeWithoutSnapshot(t *testing.T) {
	ctx := t.Context()
	bufDir := t.TempDir()
	backend := NewMemoryBackend()
	_, target, cleanup := startTestWalBufferServerWithBackend(t, bufDir, backend)
	defer cleanup()

	volumeID := "test-new-node-recovery"
	const N = 10

	// 1. Controller 1 starts on Node 1 (walDir1, metaDir1) with Witness durability
	walDir1 := t.TempDir()
	metaDir1 := t.TempDir()
	server1 := NewServer(backend,
		WithServerWAL(walDir1, target, walclient.Witness),
		WithServerMetadataDir(metaDir1),
		WithServerMetadataIndex("sqlite"),
	)

	// Write N files on Server 1 without publishing any snapshot
	for i := 1; i <= N; i++ {
		_, err := testCreateFile(ctx, server1, volumeID, fmt.Sprintf("/file_%d.txt", i), 0644, []byte(fmt.Sprintf("content_%d", i)), 0, 0)
		if err != nil {
			t.Fatalf("Server 1 failed to create file_%d.txt: %v", i, err)
		}
	}

	// Create a subdirectory with a file as well
	_, err := testMkdir(ctx, server1, volumeID, "/sub", 0755, 0, 0)
	if err != nil {
		t.Fatalf("Server 1 Mkdir failed: %v", err)
	}
	_, err = testCreateFile(ctx, server1, volumeID, "/sub/nested.txt", 0644, []byte("nested_content"), 0, 0)
	if err != nil {
		t.Fatalf("Server 1 failed to create /sub/nested.txt: %v", err)
	}

	// Close Server 1 (simulating node crash / controller moving away)
	_ = server1.Close()

	// 2. Controller 2 starts on Node 2 with EMPTY walDir2 and EMPTY metaDir2 against the same wal-buffer & backend
	walDir2 := t.TempDir()
	metaDir2 := t.TempDir()
	server2 := NewServer(backend,
		WithServerWAL(walDir2, target, walclient.Witness),
		WithServerMetadataDir(metaDir2),
		WithServerMetadataIndex("sqlite"),
	)
	defer func() { _ = server2.Close() }()

	// Verify all N files are present and readable on Server 2
	for i := 1; i <= N; i++ {
		resp, err := testReadFile(ctx, server2, volumeID, fmt.Sprintf("/file_%d.txt", i), 0, 1024)
		if err != nil {
			t.Fatalf("Server 2 failed to read file_%d.txt after node move: %v", i, err)
		}
		expected := fmt.Sprintf("content_%d", i)
		if string(resp.GetData()) != expected {
			t.Fatalf("file_%d.txt content mismatch: got %q, want %q", i, string(resp.GetData()), expected)
		}
	}

	// Verify nested file
	nestedResp, err := testReadFile(ctx, server2, volumeID, "/sub/nested.txt", 0, 1024)
	if err != nil {
		t.Fatalf("Server 2 failed to read /sub/nested.txt: %v", err)
	}
	if string(nestedResp.GetData()) != "nested_content" {
		t.Fatalf("nested.txt content mismatch: got %q, want nested_content", string(nestedResp.GetData()))
	}

	// 3. Write new files on Server 2
	_, err = testCreateFile(ctx, server2, volumeID, "/after_restart.txt", 0644, []byte("after_restart_data"), 0, 0)
	if err != nil {
		t.Fatalf("Server 2 failed to create /after_restart.txt: %v", err)
	}

	// 4. Controller 3 starts on Node 3 (empty dirs again) and verifies all writes survive
	walDir3 := t.TempDir()
	metaDir3 := t.TempDir()
	server3 := NewServer(backend,
		WithServerWAL(walDir3, target, walclient.Witness),
		WithServerMetadataDir(metaDir3),
		WithServerMetadataIndex("sqlite"),
	)
	defer func() { _ = server3.Close() }()

	afterResp, err := testReadFile(ctx, server3, volumeID, "/after_restart.txt", 0, 1024)
	if err != nil {
		t.Fatalf("Server 3 failed to read /after_restart.txt: %v", err)
	}
	if string(afterResp.GetData()) != "after_restart_data" {
		t.Fatalf("after_restart.txt content mismatch: got %q", string(afterResp.GetData()))
	}

	resp1, err := testReadFile(ctx, server3, volumeID, "/file_1.txt", 0, 1024)
	if err != nil {
		t.Fatalf("Server 3 failed to read /file_1.txt: %v", err)
	}
	if string(resp1.GetData()) != "content_1" {
		t.Fatalf("file_1.txt content mismatch: got %q", string(resp1.GetData()))
	}
}

// TestAuthoritativeRecoveryIgnoresStaleNodeSegments tests that when a controller moves back
// to a node with stale local segments, the authoritative records from the buffer are applied
// and the stale local segments are ignored (issue #185).
func TestAuthoritativeRecoveryIgnoresStaleNodeSegments(t *testing.T) {
	ctx := t.Context()
	bufDir := t.TempDir()
	backend := NewMemoryBackend()
	_, target, cleanup := startTestWalBufferServerWithBackend(t, bufDir, backend)
	defer cleanup()

	volumeID := "test-stale-node-recovery"

	// 1. Controller runs on Node 1 with local-only writes that diverge
	node1WalDir := t.TempDir()
	node1MetaDir := t.TempDir()
	streamID := StreamIDForVolume(volumeID)

	localStream1, err := walclient.Open(ctx, node1WalDir, streamID, "")
	if err != nil {
		t.Fatalf("Open localStream1 failed: %v", err)
	}
	volStale := MustNewVolume(t, volumeID, backend, NewEventBroadcaster(),
		WithStream(localStream1),
		WithLocalStorageDir(filepath.Join(node1MetaDir, volumeID)),
	)
	_ = volStale.LoadFromBackend(ctx)
	_, _ = volStale.CreateFile(ctx, 1, "stale_local_file.txt", 0644, []byte("stale-local-data"), 0, 0, 0)
	_ = volStale.Close()

	// 2. Authoritative controller on Node 2 writes authoritative files to wal-buffer
	node2WalDir := t.TempDir()
	node2MetaDir := t.TempDir()
	server2 := NewServer(backend,
		WithServerWAL(node2WalDir, target, walclient.Witness),
		WithServerMetadataDir(node2MetaDir),
		WithServerMetadataIndex("sqlite"),
	)
	_, err = testCreateFile(ctx, server2, volumeID, "/authoritative_file.txt", 0644, []byte("authoritative-data"), 0, 0)
	if err != nil {
		t.Fatalf("Server 2 CreateFile failed: %v", err)
	}
	_ = server2.Close()

	// 3. Controller moves back to Node 1 with stale local WAL directory and stale SQLite DB
	server1Restarted := NewServer(backend,
		WithServerWAL(node1WalDir, target, walclient.Witness),
		WithServerMetadataDir(node1MetaDir),
		WithServerMetadataIndex("sqlite"),
	)
	defer func() { _ = server1Restarted.Close() }()

	// Authoritative file from wal-buffer must be present
	authResp, err := testReadFile(ctx, server1Restarted, volumeID, "/authoritative_file.txt", 0, 1024)
	if err != nil {
		t.Fatalf("Failed to read /authoritative_file.txt on restarted server: %v", err)
	}
	if string(authResp.GetData()) != "authoritative-data" {
		t.Fatalf("authoritative_file.txt mismatch: got %q", string(authResp.GetData()))
	}

	// Stale local file must NOT be present (should return ENOENT)
	staleResp, err := testReadFile(ctx, server1Restarted, volumeID, "/stale_local_file.txt", 0, 1024)
	if err == nil && staleResp.GetError() == 0 {
		t.Fatalf("expected /stale_local_file.txt to not exist on authoritative volume, but got data: %q", string(staleResp.GetData()))
	}
}

type chunkFaultBackend struct {
	ObjectStorageBackend
	mu        sync.Mutex
	failPacks map[string]bool
	failAll   bool
	packFiles []string
}

func newChunkFaultBackend(base ObjectStorageBackend) *chunkFaultBackend {
	return &chunkFaultBackend{
		ObjectStorageBackend: base,
		failPacks:            make(map[string]bool),
	}
}

func (b *chunkFaultBackend) PutObject(ctx context.Context, volumeID, key string, stream blob.ByteStream) (string, error) {
	b.mu.Lock()
	if strings.HasSuffix(key, ".pack") || strings.Contains(key, "blobs/") {
		b.packFiles = append(b.packFiles, key)
	}
	b.mu.Unlock()
	return b.ObjectStorageBackend.PutObject(ctx, volumeID, key, stream)
}

func (b *chunkFaultBackend) setFailPack(key string, fail bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failPacks[key] = fail
}

func (b *chunkFaultBackend) setFailAll(fail bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failAll = fail
}

func (b *chunkFaultBackend) GetObject(ctx context.Context, volumeID, key string, offset, length int64, w io.Writer) error {
	b.mu.Lock()
	failAll := b.failAll
	failPack := b.failPacks[key]
	b.mu.Unlock()

	if failAll || failPack {
		return fmt.Errorf("simulated object get failure for key: %s", key)
	}
	return b.ObjectStorageBackend.GetObject(ctx, volumeID, key, offset, length, w)
}

func TestChunkFetchFailurePropagated(t *testing.T) {
	ctx := t.Context()
	walDir := t.TempDir()
	volumeID := "test-chunk-fetch-failure"
	streamID := StreamIDForVolume(volumeID)

	stream, err := walclient.Open(ctx, walDir, streamID, "")
	if err != nil {
		t.Fatalf("walclient.Open failed: %v", err)
	}

	rawBackend := NewMemoryBackend()
	backend := newChunkFaultBackend(rawBackend)
	chunkSize := uint32(16 * 1024)

	vol := MustNewVolume(t, volumeID, backend, NewEventBroadcaster(),
		WithStream(stream),
		WithChunkSize(chunkSize),
		WithDurability(walclient.Local),
	)
	defer vol.Close()

	if err := vol.LoadFromBackend(ctx); err != nil {
		t.Fatalf("LoadFromBackend failed: %v", err)
	}

	// 1. Create a 32 KiB chunked file across 2 separate flushes so chunk 0 and chunk 1 are in separate packs.
	chunk0Data := bytes.Repeat([]byte("A"), int(chunkSize))
	chunk1Data := bytes.Repeat([]byte("B"), int(chunkSize))
	fullData := append(append([]byte(nil), chunk0Data...), chunk1Data...)

	attr, err := volCreateFile(ctx, vol, "/file.bin", 0644, chunk0Data, 0, 0)
	if err != nil {
		t.Fatalf("volCreateFile failed: %v", err)
	}
	ino := attr.GetInode().GetIno()

	// Flush chunk 0
	if err := vol.FlushToBackend(ctx); err != nil {
		t.Fatalf("FlushToBackend chunk 0 failed: %v", err)
	}

	// Append chunk 1 and flush
	_, _, _, err = vol.WriteFile(ctx, ino, int64(chunkSize), chunk1Data, pb.WriteMode_LAZY_WRITE)
	if err != nil {
		t.Fatalf("WriteFile chunk 1 failed: %v", err)
	}
	if err := vol.FlushToBackend(ctx); err != nil {
		t.Fatalf("FlushToBackend chunk 1 failed: %v", err)
	}

	if len(backend.packFiles) < 2 {
		t.Fatalf("expected at least 2 packfiles, got %d", len(backend.packFiles))
	}
	chunk1Pack := backend.packFiles[len(backend.packFiles)-1]

	// Verify reading both chunks succeeds before inducing failure.
	read0, _, _, err := vol.ReadFile(ctx, ino, 0, int64(chunkSize))
	if err != nil || !bytes.Equal(read0, chunk0Data) {
		t.Fatalf("initial read chunk 0 failed: %v", err)
	}
	read1, _, _, err := vol.ReadFile(ctx, ino, int64(chunkSize), int64(chunkSize))
	if err != nil || !bytes.Equal(read1, chunk1Data) {
		t.Fatalf("initial read chunk 1 failed: %v", err)
	}

	// 2. Fail chunk 1 fetch only.
	backend.setFailPack(chunk1Pack, true)

	// Read chunk 0 must still succeed:
	read0After, _, _, err := vol.ReadFile(ctx, ino, 0, int64(chunkSize))
	if err != nil || !bytes.Equal(read0After, chunk0Data) {
		t.Fatalf("read chunk 0 should succeed when only chunk 1 fails, got err: %v", err)
	}

	// Read range covering chunk 1 must fail (NOT zeros):
	_, _, _, err = vol.ReadFile(ctx, ino, int64(chunkSize), int64(chunkSize))
	if err == nil {
		t.Fatalf("expected ReadFile on failed chunk 1 to return an error, but got nil")
	}

	// Read range spanning chunk 0 and chunk 1 must fail:
	_, _, _, err = vol.ReadFile(ctx, ino, 0, int64(2*chunkSize))
	if err == nil {
		t.Fatalf("expected ReadFile spanning failed chunk 1 to return an error, but got nil")
	}

	// 3. Partial-chunk WriteFile when chunk fetch fails:
	// Must return an error and NOT commit any changes (no data corruption).
	origSeq, _, _ := stream.Watermarks()
	origAttr, err := vol.GetAttr(ctx, ino)
	if err != nil {
		t.Fatalf("GetAttr failed: %v", err)
	}

	patch := []byte("corrupt-overwrite")
	_, _, _, err = vol.WriteFile(ctx, ino, int64(chunkSize)+100, patch, pb.WriteMode_LAZY_WRITE)
	if err == nil {
		t.Fatalf("expected WriteFile to fail when chunk fetch fails, got nil")
	}

	// Assert WAL stream watermark is unchanged (no commit logged)
	currSeq, _, _ := stream.Watermarks()
	if currSeq != origSeq {
		t.Fatalf("expected stream watermark to be unchanged (%d), got %d", origSeq, currSeq)
	}

	// Assert inode row is unchanged
	afterAttr, err := vol.GetAttr(ctx, ino)
	if err != nil {
		t.Fatalf("GetAttr failed: %v", err)
	}
	if afterAttr.GetInode().GetSize() != origAttr.GetInode().GetSize() {
		t.Fatalf("inode size changed: got %d, want %d", afterAttr.GetInode().GetSize(), origAttr.GetInode().GetSize())
	}
	if afterAttr.GetInode().GetMtime().AsTime() != origAttr.GetInode().GetMtime().AsTime() {
		t.Fatalf("inode mtime changed after failed write")
	}

	// 4. Partial-chunk TruncateFile when chunk fetch fails:
	// Must return an error and NOT commit any changes.
	_, err = vol.TruncateFile(ctx, ino, int64(chunkSize)+500)
	if err == nil {
		t.Fatalf("expected TruncateFile to fail when chunk fetch fails, got nil")
	}
	currSeq, _, _ = stream.Watermarks()
	if currSeq != origSeq {
		t.Fatalf("expected stream watermark to be unchanged after failed truncate (%d), got %d", origSeq, currSeq)
	}
	afterTruncAttr, err := vol.GetAttr(ctx, ino)
	if err != nil {
		t.Fatalf("GetAttr failed: %v", err)
	}
	if afterTruncAttr.GetInode().GetSize() != origAttr.GetInode().GetSize() {
		t.Fatalf("inode size changed after failed truncate: got %d, want %d", afterTruncAttr.GetInode().GetSize(), origAttr.GetInode().GetSize())
	}

	// 5. Restore backend and verify the file content is still completely intact.
	backend.setFailPack(chunk1Pack, false)

	fullRead, total, _, err := vol.ReadFile(ctx, ino, 0, int64(2*chunkSize))
	if err != nil {
		t.Fatalf("ReadFile after restoring backend failed: %v", err)
	}
	if total != int64(2*chunkSize) {
		t.Fatalf("expected total %d, got %d", 2*chunkSize, total)
	}
	if !bytes.Equal(fullRead, fullData) {
		t.Fatalf("data corrupted: read data does not match original data")
	}

	// 6. Legitimate hole test:
	// Truncate file up to 48 KiB (chunk 2 is a hole, no chunk row in index).
	// Even if backend fails all requests, reading the hole range must succeed and return zeros without error.
	_, err = vol.TruncateFile(ctx, ino, int64(3*chunkSize))
	if err != nil {
		t.Fatalf("extending TruncateFile failed: %v", err)
	}

	backend.setFailAll(true)
	holeData, holeTotal, _, err := vol.ReadFile(ctx, ino, int64(2*chunkSize), int64(chunkSize))
	if err != nil {
		t.Fatalf("ReadFile on legitimate hole failed when backend down: %v", err)
	}
	if holeTotal != int64(3*chunkSize) {
		t.Fatalf("expected total %d, got %d", 3*chunkSize, holeTotal)
	}
	if !bytes.Equal(holeData, make([]byte, chunkSize)) {
		t.Fatalf("expected zeros for hole chunk, got non-zero bytes")
	}
	backend.setFailAll(false)

	// 7. Legacy whole-file fallback:
	// A file with Sha256 set on the Inode row but no chunk rows.
	legacyData := []byte("legacy-whole-file-content")
	legacySha := fmt.Sprintf("%x", sha256.Sum256(legacyData))
	vol.mu.Lock()
	err = vol.blobStore.PutBlobs(ctx, map[string]blob.ByteStream{
		legacySha: blob.NewByteStreamFromBytes(legacyData),
	})
	vol.mu.Unlock()
	if err != nil {
		t.Fatalf("PutBlobs for legacy blob failed: %v", err)
	}

	vol.mu.Lock()
	legacyTx := vol.beginTxLocked("legacy-insert")
	legacyIno := vol.allocInode(legacyTx)
	_, err = legacyTx.Insert(ctx, &pb.Inode{
		Ino:           proto.Uint64(legacyIno),
		Size:          int64(len(legacyData)),
		Sha256:        legacySha,
		ContentSha256: legacySha,
		Nlink:         1,
	})
	if err != nil {
		vol.mu.Unlock()
		t.Fatalf("Insert legacy Inode failed: %v", err)
	}
	commitSeq, err := legacyTx.Commit(ctx)
	if err != nil {
		vol.mu.Unlock()
		t.Fatalf("Commit legacy Inode failed: %v", err)
	}
	vol.applyTxChangesLocked(ctx, legacyTx)
	waitFn := vol.makeWaitFn(commitSeq, nil)
	vol.mu.Unlock()
	if waitFn != nil {
		_ = waitFn(ctx)
	}

	// When backend fails, ReadFile must return an error rather than zeros:
	backend.setFailAll(true)
	_, _, _, err = vol.ReadFile(ctx, legacyIno, 0, int64(len(legacyData)))
	if err == nil {
		t.Fatalf("expected ReadFile on legacy blob to return error when backend fails, got nil")
	}
	backend.setFailAll(false)

	// When backend succeeds, ReadFile returns the legacy content:
	legacyRead, _, _, err := vol.ReadFile(ctx, legacyIno, 0, int64(len(legacyData)))
	if err != nil || !bytes.Equal(legacyRead, legacyData) {
		t.Fatalf("expected ReadFile on legacy blob to return data, got err=%v, data=%q", err, string(legacyRead))
	}
}

func TestServerEIOOnError(t *testing.T) {
	ctx := t.Context()
	rawBackend := NewMemoryBackend()
	backend := newChunkFaultBackend(rawBackend)
	server := NewServer(backend)
	volumeID := "test-server-eio-vol"

	// Create chunked file via server across 2 flushes
	chunkSize := 64 * 1024
	chunk0Data := bytes.Repeat([]byte("X"), chunkSize)
	chunk1Data := bytes.Repeat([]byte("Y"), chunkSize)

	createResp, err := testCreateFile(ctx, server, volumeID, "/fail.bin", 0644, chunk0Data, 0, 0)
	if err != nil || createResp.GetError() != 0 {
		t.Fatalf("testCreateFile failed: %v (err code %d)", err, createResp.GetError())
	}
	ino := createResp.GetAttr().GetInode().GetIno()

	// Flush volume for chunk 0
	vol, err := server.getOrCreateVolume(volumeID)
	if err != nil {
		t.Fatalf("getOrCreateVolume failed: %v", err)
	}
	if err := vol.FlushToBackend(ctx); err != nil {
		t.Fatalf("FlushToBackend failed: %v", err)
	}

	// Write chunk 1 and flush
	write1Resp, err := server.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId:  volumeID,
		Inode:     ino,
		Offset:    int64(chunkSize),
		Data:      chunk1Data,
		WriteMode: pb.WriteMode_LAZY_WRITE,
	})
	if err != nil || write1Resp.GetError() != 0 {
		t.Fatalf("server WriteFile chunk 1 failed: %v (err %d)", err, write1Resp.GetError())
	}
	if err := vol.FlushToBackend(ctx); err != nil {
		t.Fatalf("FlushToBackend chunk 1 failed: %v", err)
	}

	if len(backend.packFiles) < 2 {
		t.Fatalf("expected at least 2 packfiles, got %d", len(backend.packFiles))
	}
	chunk1Pack := backend.packFiles[len(backend.packFiles)-1]

	// Fail chunk 1 fetch
	backend.setFailPack(chunk1Pack, true)

	// ReadFile via server should return EIO
	readResp, err := server.ReadFile(ctx, &pb.ReadFileRequest{
		VolumeId: volumeID,
		Inode:    ino,
		Offset:   int64(chunkSize),
		Size:     int64(chunkSize),
	})
	if err != nil {
		t.Fatalf("Server ReadFile RPC error: %v", err)
	}
	if readResp.GetError() != int32(syscall.EIO) {
		t.Fatalf("expected ReadFile to return EIO (%d), got error %d", syscall.EIO, readResp.GetError())
	}

	// WriteFile via server on chunk 1 should return EIO
	writeResp, err := server.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId:  volumeID,
		Inode:     ino,
		Offset:    int64(chunkSize) + 50,
		Data:      []byte("mod"),
		WriteMode: pb.WriteMode_LAZY_WRITE,
	})
	if err != nil {
		t.Fatalf("Server WriteFile RPC error: %v", err)
	}
	if writeResp.GetError() != int32(syscall.EIO) {
		t.Fatalf("expected WriteFile to return EIO (%d), got error %d", syscall.EIO, writeResp.GetError())
	}

	// TruncateFile via server touching chunk 1 should return EIO
	truncResp, err := server.TruncateFile(ctx, &pb.TruncateFileRequest{
		VolumeId: volumeID,
		Inode:    ino,
		Size:     int64(chunkSize) + 500,
	})
	if err != nil {
		t.Fatalf("Server TruncateFile RPC error: %v", err)
	}
	if truncResp.GetError() != int32(syscall.EIO) {
		t.Fatalf("expected TruncateFile to return EIO (%d), got error %d", syscall.EIO, truncResp.GetError())
	}
}

func TestTruncateZeroThenGrowReadsZeros(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewServer(backend)
	volumeID := "test-trunc-zero-grow"

	createResp, err := testCreateFile(ctx, server, volumeID, "/f", 0644, nil, 0, 0)
	if err != nil || createResp.GetError() != 0 {
		t.Fatalf("testCreateFile failed: %v (code %d)", err, createResp.GetError())
	}
	ino := createResp.GetAttr().GetInode().GetIno()

	trunc0Resp, err := testTruncateFile(ctx, server, volumeID, "/f", 0)
	if err != nil || trunc0Resp.GetError() != 0 {
		t.Fatalf("truncate 0 failed: %v (code %d)", err, trunc0Resp.GetError())
	}

	truncGrowResp, err := testTruncateFile(ctx, server, volumeID, "/f", 100000)
	if err != nil || truncGrowResp.GetError() != 0 {
		t.Fatalf("truncate 100000 failed: %v (code %d)", err, truncGrowResp.GetError())
	}

	readResp, err := server.ReadFile(ctx, &pb.ReadFileRequest{
		VolumeId: volumeID,
		Inode:    ino,
		Offset:   0,
		Size:     65536,
	})
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if readResp.GetError() != 0 {
		t.Fatalf("ReadFile returned error %d, want 0", readResp.GetError())
	}
	if len(readResp.GetData()) != 65536 {
		t.Fatalf("expected 65536 bytes, got %d", len(readResp.GetData()))
	}
	if !bytes.Equal(readResp.GetData(), make([]byte, 65536)) {
		t.Fatalf("expected 65536 zero bytes, got non-zero bytes")
	}
}

func TestInlineToChunkedTransitionUploadsChunk0OnRestart(t *testing.T) {
	ctx := t.Context()
	walDir := t.TempDir()
	backend := NewMemoryBackend()
	volumeID := "test-inline-chunk-restart"

	server1 := NewServer(backend, WithServerWAL(walDir, "", walclient.Local))
	inlineData := []byte("hello-inline-world")
	createResp, err := testCreateFile(ctx, server1, volumeID, "/file.txt", 0644, inlineData, 0, 0)
	if err != nil || createResp.GetError() != 0 {
		t.Fatalf("CreateFile failed: %v, err=%d", err, createResp.GetError())
	}
	ino := createResp.GetAttr().GetInode().GetIno()

	// Write at chunk 2 (offset 128 KiB), making the file chunked while leaving chunk 0 untouched.
	chunk2Offset := int64(2 * 64 * 1024)
	chunk2Data := []byte("chunk-2-data")
	writeResp, err := server1.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId:  volumeID,
		Inode:     ino,
		Offset:    chunk2Offset,
		Data:      chunk2Data,
		WriteMode: pb.WriteMode_LAZY_WRITE,
	})
	if err != nil || writeResp.GetError() != 0 {
		var code int32
		if writeResp != nil {
			code = writeResp.GetError()
		}
		t.Fatalf("WriteFile at chunk 2 failed: %v, err=%d", err, code)
	}

	vol1, err := server1.getOrCreateVolume(volumeID)
	if err != nil {
		t.Fatalf("getOrCreateVolume failed: %v", err)
	}
	if err := vol1.FlushToBackend(ctx); err != nil {
		t.Fatalf("FlushToBackend failed: %v", err)
	}
	_ = server1.Close()

	// Restart server from the same backend & WAL
	server2 := NewServer(backend, WithServerWAL(walDir, "", walclient.Local))
	defer func() { _ = server2.Close() }()

	readResp, err := server2.ReadFile(ctx, &pb.ReadFileRequest{
		VolumeId: volumeID,
		Inode:    ino,
		Offset:   0,
		Size:     int64(len(inlineData)),
	})
	if err != nil {
		t.Fatalf("ReadFile chunk 0 after restart failed: %v", err)
	}
	if readResp.GetError() != 0 {
		t.Fatalf("ReadFile chunk 0 after restart returned errno %d", readResp.GetError())
	}
	if !bytes.Equal(readResp.GetData(), inlineData) {
		t.Fatalf("ReadFile chunk 0 content mismatch: got %q, want %q", string(readResp.GetData()), string(inlineData))
	}
}

func TestGetManifest(t *testing.T) {
	ctx := t.Context()
	server := NewServer(nil)
	volumeID := "test-manifest-vol"

	// 1. Create small inline file
	contentSmall := []byte("small file inline data")
	cfResp, err := testCreateFile(ctx, server, volumeID, "/small.txt", 0644, contentSmall, 0, 0)
	if err != nil || cfResp.GetError() != 0 {
		t.Fatalf("testCreateFile small.txt failed: err=%v, resp=%v", err, cfResp)
	}
	inoSmall := cfResp.GetAttr().GetInode().GetIno()

	mSmall, err := server.GetManifest(ctx, &pb.GetManifestRequest{
		VolumeId: volumeID,
		Inode:    inoSmall,
	})
	if err != nil || mSmall.GetError() != 0 {
		t.Fatalf("GetManifest small.txt failed: err=%v, resp=%v", err, mSmall)
	}
	if mSmall.GetTotalSize() != int64(len(contentSmall)) {
		t.Errorf("GetManifest small.txt total_size = %d, want %d", mSmall.GetTotalSize(), len(contentSmall))
	}
	if !bytes.Equal(mSmall.GetInlineData(), contentSmall) {
		t.Errorf("GetManifest small.txt inline_data = %q, want %q", mSmall.GetInlineData(), contentSmall)
	}

	// 2. Create multi-chunk file and sync it
	chunkSize := 64 * 1024
	totalSize := 3 * chunkSize
	contentMulti := bytes.Repeat([]byte("M"), totalSize)
	cfMulti, err := testCreateFile(ctx, server, volumeID, "/multi.bin", 0644, nil, 0, 0)
	if err != nil || cfMulti.GetError() != 0 {
		t.Fatalf("testCreateFile multi.bin failed: err=%v, resp=%v", err, cfMulti)
	}
	inoMulti := cfMulti.GetAttr().GetInode().GetIno()

	for i := 0; i < 3; i++ {
		wResp, err := testWriteFile(ctx, server, volumeID, "/multi.bin", int64(i*chunkSize), contentMulti[i*chunkSize:(i+1)*chunkSize], pb.WriteMode_WRITE_THROUGH_FSYNC)
		if err != nil || wResp.GetError() != 0 {
			t.Fatalf("testWriteFile chunk %d failed: %v", i, err)
		}
	}
	// Fsync to pack and commit chunks into SDS
	fsyncResp, err := server.Fsync(ctx, &pb.FsyncRequest{
		VolumeId: volumeID,
		Inode:    inoMulti,
		Blocking: true,
	})
	if err != nil || fsyncResp.GetError() != 0 {
		t.Fatalf("Fsync multi.bin failed: %v", err)
	}

	mMulti, err := server.GetManifest(ctx, &pb.GetManifestRequest{
		VolumeId: volumeID,
		Inode:    inoMulti,
	})
	if err != nil || mMulti.GetError() != 0 {
		t.Fatalf("GetManifest multi.bin failed: err=%v, resp=%v", err, mMulti)
	}
	if mMulti.GetTotalSize() != int64(totalSize) {
		t.Errorf("GetManifest multi.bin total_size = %d, want %d", mMulti.GetTotalSize(), totalSize)
	}
	if len(mMulti.GetChunkSha256()) != 3 {
		t.Fatalf("GetManifest multi.bin chunk count = %d, want 3", len(mMulti.GetChunkSha256()))
	}
	for i, sha := range mMulti.GetChunkSha256() {
		if sha == "" {
			t.Errorf("chunk %d has empty sha", i)
		}
	}
}

func TestPunchHoleWholeChunks(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewServer(backend, WithServerMutationCheck())
	volumeID := "test-punch-whole-ctrl"

	chunkSize := 64 * 1024
	totalSize := 3 * chunkSize
	content := make([]byte, totalSize)
	for i := range content {
		content[i] = byte(i%251 + 1)
	}

	cf, err := testCreateFile(ctx, server, volumeID, "/hole.bin", 0644, nil, 0, 0)
	if err != nil || cf.GetError() != 0 {
		t.Fatalf("testCreateFile failed: %v", err)
	}
	ino := cf.GetAttr().GetInode().GetIno()

	for i := 0; i < 3; i++ {
		wResp, err := testWriteFile(ctx, server, volumeID, "/hole.bin", int64(i*chunkSize), content[i*chunkSize:(i+1)*chunkSize], pb.WriteMode_WRITE_THROUGH_FSYNC)
		if err != nil || wResp.GetError() != 0 {
			t.Fatalf("testWriteFile %d failed: %v", i, err)
		}
	}
	fsyncResp, err := server.Fsync(ctx, &pb.FsyncRequest{VolumeId: volumeID, Inode: ino, Blocking: true})
	if err != nil || fsyncResp.GetError() != 0 {
		t.Fatalf("Fsync failed: %v", err)
	}

	// Punch chunk 1
	pResp, err := server.PunchHole(ctx, &pb.PunchHoleRequest{
		VolumeId: volumeID,
		Inode:    ino,
		Offset:   int64(chunkSize),
		Length:   int64(chunkSize),
		KeepSize: true,
	})
	if err != nil || pResp.GetError() != 0 {
		t.Fatalf("PunchHole failed: err=%v, code=%d", err, pResp.GetError())
	}
	if pResp.GetAttr().GetInode().GetSize() != int64(totalSize) {
		t.Fatalf("Expected size %d, got %d", totalSize, pResp.GetAttr().GetInode().GetSize())
	}

	// Verify manifest
	mResp, err := server.GetManifest(ctx, &pb.GetManifestRequest{VolumeId: volumeID, Inode: ino})
	if err != nil {
		t.Fatalf("GetManifest failed: %v", err)
	}
	if len(mResp.GetChunkSha256()) != 3 {
		t.Fatalf("Expected 3 chunks in manifest, got %d", len(mResp.GetChunkSha256()))
	}
	if mResp.GetChunkSha256()[1] != "" {
		t.Fatalf("Expected chunk 1 sha to be empty (hole), got %s", mResp.GetChunkSha256()[1])
	}
	if mResp.GetChunkSha256()[0] == "" || mResp.GetChunkSha256()[2] == "" {
		t.Fatalf("Expected chunk 0 and 2 shas to be present")
	}

	// Read whole file back
	rResp, err := server.ReadFile(ctx, &pb.ReadFileRequest{
		VolumeId: volumeID,
		Inode:    ino,
		Offset:   0,
		Size:     int64(totalSize),
	})
	if err != nil || rResp.GetError() != 0 {
		t.Fatalf("ReadFile failed: %v", err)
	}
	rData := rResp.GetData()
	if !bytes.Equal(rData[:chunkSize], content[:chunkSize]) {
		t.Fatalf("Chunk 0 data corrupted")
	}
	if !bytes.Equal(rData[chunkSize:2*chunkSize], make([]byte, chunkSize)) {
		t.Fatalf("Chunk 1 not all zeros")
	}
	if !bytes.Equal(rData[2*chunkSize:], content[2*chunkSize:]) {
		t.Fatalf("Chunk 2 data corrupted")
	}
}

func TestPunchHolePartialEdgesAndGrowSize(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewServer(backend, WithServerMutationCheck())
	volumeID := "test-punch-edges-ctrl"

	content := make([]byte, 150*1024)
	for i := range content {
		content[i] = byte(i%251 + 1)
	}

	cf, err := testCreateFile(ctx, server, volumeID, "/edges.bin", 0644, nil, 0, 0)
	if err != nil || cf.GetError() != 0 {
		t.Fatalf("testCreateFile failed: %v", err)
	}
	ino := cf.GetAttr().GetInode().GetIno()

	// Write and fsync
	wResp, err := testWriteFile(ctx, server, volumeID, "/edges.bin", 0, content, pb.WriteMode_WRITE_THROUGH_FSYNC)
	if err != nil || wResp.GetError() != 0 {
		t.Fatalf("WriteFile failed: %v", err)
	}
	fsyncResp, err := server.Fsync(ctx, &pb.FsyncRequest{VolumeId: volumeID, Inode: ino, Blocking: true})
	if err != nil || fsyncResp.GetError() != 0 {
		t.Fatalf("Fsync failed: %v", err)
	}

	// Partial punch spanning 32 KiB to 96 KiB
	pResp, err := server.PunchHole(ctx, &pb.PunchHoleRequest{
		VolumeId: volumeID,
		Inode:    ino,
		Offset:   32 * 1024,
		Length:   64 * 1024,
		KeepSize: true,
	})
	if err != nil || pResp.GetError() != 0 {
		t.Fatalf("PunchHole failed: %v", err)
	}

	rResp, err := server.ReadFile(ctx, &pb.ReadFileRequest{VolumeId: volumeID, Inode: ino, Offset: 0, Size: int64(len(content))})
	if err != nil || rResp.GetError() != 0 {
		t.Fatalf("ReadFile failed: %v", err)
	}
	rData := rResp.GetData()
	if !bytes.Equal(rData[:32*1024], content[:32*1024]) {
		t.Fatalf("Leading bytes corrupted")
	}
	if !bytes.Equal(rData[32*1024:96*1024], make([]byte, 64*1024)) {
		t.Fatalf("Punched range not zeros")
	}
	if !bytes.Equal(rData[96*1024:], content[96*1024:]) {
		t.Fatalf("Trailing bytes corrupted")
	}

	// Zero-range extending size to 200 KiB (offset 100 KiB, length 100 KiB, KeepSize: false)
	pzResp, err := server.PunchHole(ctx, &pb.PunchHoleRequest{
		VolumeId: volumeID,
		Inode:    ino,
		Offset:   100 * 1024,
		Length:   100 * 1024,
		KeepSize: false,
	})
	if err != nil || pzResp.GetError() != 0 {
		t.Fatalf("PunchHole extend failed: %v", err)
	}
	if pzResp.GetAttr().GetInode().GetSize() != 200*1024 {
		t.Fatalf("Expected size 200 KiB, got %d", pzResp.GetAttr().GetInode().GetSize())
	}

	rResp2, err := server.ReadFile(ctx, &pb.ReadFileRequest{VolumeId: volumeID, Inode: ino, Offset: 0, Size: 200 * 1024})
	if err != nil || rResp2.GetError() != 0 {
		t.Fatalf("ReadFile 200KB failed: %v", err)
	}
	rData2 := rResp2.GetData()
	if !bytes.Equal(rData2[100*1024:], make([]byte, 100*1024)) {
		t.Fatalf("Extended region not all zeros")
	}
}

func TestPunchHoleStagedChunks(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewServer(backend, WithServerMutationCheck())
	volumeID := "test-punch-staged"

	chunkSize := 64 * 1024
	content := make([]byte, 2*chunkSize)
	for i := range content {
		content[i] = byte(i%251 + 1)
	}

	cf, err := testCreateFile(ctx, server, volumeID, "/staged.bin", 0644, nil, 0, 0)
	if err != nil || cf.GetError() != 0 {
		t.Fatalf("testCreateFile failed: %v", err)
	}
	ino := cf.GetAttr().GetInode().GetIno()

	// Write using LAZY_WRITE (staged in server memory)
	wResp, err := testWriteFile(ctx, server, volumeID, "/staged.bin", 0, content, pb.WriteMode_LAZY_WRITE)
	if err != nil || wResp.GetError() != 0 {
		t.Fatalf("WriteFile LAZY_WRITE failed: %v", err)
	}

	// Punch chunk 0 in staged chunks
	pResp, err := server.PunchHole(ctx, &pb.PunchHoleRequest{
		VolumeId: volumeID,
		Inode:    ino,
		Offset:   0,
		Length:   int64(chunkSize),
		KeepSize: true,
	})
	if err != nil || pResp.GetError() != 0 {
		t.Fatalf("PunchHole staged failed: %v", err)
	}

	// Fsync commits the staged chunks
	fsyncResp, err := server.Fsync(ctx, &pb.FsyncRequest{VolumeId: volumeID, Inode: ino, Blocking: true})
	if err != nil || fsyncResp.GetError() != 0 {
		t.Fatalf("Fsync after punch failed: %v", err)
	}

	// Read back: chunk 0 must be zeros, chunk 1 must have content
	rResp, err := server.ReadFile(ctx, &pb.ReadFileRequest{VolumeId: volumeID, Inode: ino, Offset: 0, Size: int64(2 * chunkSize)})
	if err != nil || rResp.GetError() != 0 {
		t.Fatalf("ReadFile failed: %v", err)
	}
	rData := rResp.GetData()
	if !bytes.Equal(rData[:chunkSize], make([]byte, chunkSize)) {
		t.Fatalf("Staged chunk 0 was not punched (expected zeros)")
	}
	if !bytes.Equal(rData[chunkSize:], content[chunkSize:]) {
		t.Fatalf("Staged chunk 1 corrupted")
	}
}

func TestPunchHoleChunkBoundaryCases(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewServer(backend, WithServerMutationCheck())
	volumeID := "test-punch-boundary-ctrl"

	// Case 1: file size 128 KiB, punch [0, 10 KiB) (keep_size)
	// Chunk 0 starts at 0, punch ends at 10 KiB within chunk 0.
	content128 := make([]byte, 128*1024)
	for i := range content128 {
		content128[i] = byte(i%251 + 1)
	}
	cf1, err := testCreateFile(ctx, server, volumeID, "/c1.bin", 0644, nil, 0, 0)
	if err != nil || cf1.GetError() != 0 {
		t.Fatalf("testCreateFile /c1.bin failed: %v", err)
	}
	ino1 := cf1.GetAttr().GetInode().GetIno()
	if _, err := testWriteFile(ctx, server, volumeID, "/c1.bin", 0, content128, pb.WriteMode_WRITE_THROUGH_FSYNC); err != nil {
		t.Fatalf("Write /c1.bin failed: %v", err)
	}
	if _, err := server.Fsync(ctx, &pb.FsyncRequest{VolumeId: volumeID, Inode: ino1, Blocking: true}); err != nil {
		t.Fatalf("Fsync /c1.bin failed: %v", err)
	}
	p1, err := server.PunchHole(ctx, &pb.PunchHoleRequest{
		VolumeId: volumeID,
		Inode:    ino1,
		Offset:   0,
		Length:   10 * 1024,
		KeepSize: true,
	})
	if err != nil || p1.GetError() != 0 {
		t.Fatalf("PunchHole /c1.bin failed: %v", err)
	}
	r1, err := server.ReadFile(ctx, &pb.ReadFileRequest{VolumeId: volumeID, Inode: ino1, Offset: 0, Size: 128 * 1024})
	if err != nil || r1.GetError() != 0 {
		t.Fatalf("ReadFile /c1.bin failed: %v", err)
	}
	if !bytes.Equal(r1.GetData()[:10*1024], make([]byte, 10*1024)) {
		t.Fatalf("Case 1: [0, 10 KiB) was not zeroed")
	}
	if !bytes.Equal(r1.GetData()[10*1024:], content128[10*1024:]) {
		t.Fatalf("Case 1: unpunched range corrupted")
	}

	// Case 2: file size 100 KiB, punch [64 KiB, 100 KiB) (keep_size, chunk 1 start to EOF)
	content100 := make([]byte, 100*1024)
	for i := range content100 {
		content100[i] = byte(i%251 + 1)
	}
	cf2, err := testCreateFile(ctx, server, volumeID, "/c2.bin", 0644, nil, 0, 0)
	if err != nil || cf2.GetError() != 0 {
		t.Fatalf("testCreateFile /c2.bin failed: %v", err)
	}
	ino2 := cf2.GetAttr().GetInode().GetIno()
	if _, err := testWriteFile(ctx, server, volumeID, "/c2.bin", 0, content100, pb.WriteMode_WRITE_THROUGH_FSYNC); err != nil {
		t.Fatalf("Write /c2.bin failed: %v", err)
	}
	if _, err := server.Fsync(ctx, &pb.FsyncRequest{VolumeId: volumeID, Inode: ino2, Blocking: true}); err != nil {
		t.Fatalf("Fsync /c2.bin failed: %v", err)
	}
	p2, err := server.PunchHole(ctx, &pb.PunchHoleRequest{
		VolumeId: volumeID,
		Inode:    ino2,
		Offset:   64 * 1024,
		Length:   36 * 1024,
		KeepSize: true,
	})
	if err != nil || p2.GetError() != 0 {
		t.Fatalf("PunchHole /c2.bin failed: %v", err)
	}
	r2, err := server.ReadFile(ctx, &pb.ReadFileRequest{VolumeId: volumeID, Inode: ino2, Offset: 0, Size: 100 * 1024})
	if err != nil || r2.GetError() != 0 {
		t.Fatalf("ReadFile /c2.bin failed: %v", err)
	}
	if !bytes.Equal(r2.GetData()[:64*1024], content100[:64*1024]) {
		t.Fatalf("Case 2: chunk 0 corrupted")
	}
	if !bytes.Equal(r2.GetData()[64*1024:], make([]byte, 36*1024)) {
		t.Fatalf("Case 2: [64 KiB, 100 KiB) was not zeroed")
	}

	// Case 3: file size 200 KiB, punch [64 KiB, 65 KiB) (keep_size)
	content200 := make([]byte, 200*1024)
	for i := range content200 {
		content200[i] = byte(i%251 + 1)
	}
	cf3, err := testCreateFile(ctx, server, volumeID, "/c3.bin", 0644, nil, 0, 0)
	if err != nil || cf3.GetError() != 0 {
		t.Fatalf("testCreateFile /c3.bin failed: %v", err)
	}
	ino3 := cf3.GetAttr().GetInode().GetIno()
	if _, err := testWriteFile(ctx, server, volumeID, "/c3.bin", 0, content200, pb.WriteMode_WRITE_THROUGH_FSYNC); err != nil {
		t.Fatalf("Write /c3.bin failed: %v", err)
	}
	if _, err := server.Fsync(ctx, &pb.FsyncRequest{VolumeId: volumeID, Inode: ino3, Blocking: true}); err != nil {
		t.Fatalf("Fsync /c3.bin failed: %v", err)
	}
	p3, err := server.PunchHole(ctx, &pb.PunchHoleRequest{
		VolumeId: volumeID,
		Inode:    ino3,
		Offset:   64 * 1024,
		Length:   1000,
		KeepSize: true,
	})
	if err != nil || p3.GetError() != 0 {
		t.Fatalf("PunchHole /c3.bin failed: %v", err)
	}
	r3, err := server.ReadFile(ctx, &pb.ReadFileRequest{VolumeId: volumeID, Inode: ino3, Offset: 0, Size: 200 * 1024})
	if err != nil || r3.GetError() != 0 {
		t.Fatalf("ReadFile /c3.bin failed: %v", err)
	}
	if !bytes.Equal(r3.GetData()[:64*1024], content200[:64*1024]) {
		t.Fatalf("Case 3: prefix before punch corrupted")
	}
	if !bytes.Equal(r3.GetData()[64*1024:64*1024+1000], make([]byte, 1000)) {
		t.Fatalf("Case 3: [64 KiB, 65 KiB) was not zeroed")
	}
	if !bytes.Equal(r3.GetData()[64*1024+1000:], content200[64*1024+1000:]) {
		t.Fatalf("Case 3: suffix after punch corrupted")
	}
}

func TestPunchHoleStagedWriteKeepsSize(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewServer(backend, WithServerMutationCheck())
	volumeID := "test-punch-staged-size"

	chunkSize := 64 * 1024
	content := bytes.Repeat([]byte("S"), chunkSize)

	cf, err := testCreateFile(ctx, server, volumeID, "/staged_size.bin", 0644, nil, 0, 0)
	if err != nil || cf.GetError() != 0 {
		t.Fatalf("testCreateFile failed: %v", err)
	}
	ino := cf.GetAttr().GetInode().GetIno()

	// WriteFile 64 KiB into empty file (no fsync, staged in server memory)
	wResp, err := testWriteFile(ctx, server, volumeID, "/staged_size.bin", 0, content, pb.WriteMode_LAZY_WRITE)
	if err != nil || wResp.GetError() != 0 {
		t.Fatalf("WriteFile LAZY_WRITE failed: %v", err)
	}

	// PunchHole(0, 64 KiB, keep_size)
	pResp, err := server.PunchHole(ctx, &pb.PunchHoleRequest{
		VolumeId: volumeID,
		Inode:    ino,
		Offset:   0,
		Length:   int64(chunkSize),
		KeepSize: true,
	})
	if err != nil || pResp.GetError() != 0 {
		t.Fatalf("PunchHole failed: %v, resp=%v", err, pResp)
	}
	if pResp.GetAttr().GetInode().GetSize() != int64(chunkSize) {
		t.Fatalf("Expected PunchHole to report size %d, got %d", chunkSize, pResp.GetAttr().GetInode().GetSize())
	}

	// Fsync and GetAttr
	if _, err := server.Fsync(ctx, &pb.FsyncRequest{VolumeId: volumeID, Inode: ino, Blocking: true}); err != nil {
		t.Fatalf("Fsync failed: %v", err)
	}
	gResp, err := server.GetAttr(ctx, &pb.GetAttrRequest{VolumeId: volumeID, Inode: ino})
	if err != nil || gResp.GetError() != 0 {
		t.Fatalf("GetAttr failed: %v", err)
	}
	if gResp.GetAttr().GetInode().GetSize() != int64(chunkSize) {
		t.Fatalf("Expected GetAttr to report size %d, got %d", chunkSize, gResp.GetAttr().GetInode().GetSize())
	}

	// Read back: 64 KiB of zeros
	rResp, err := server.ReadFile(ctx, &pb.ReadFileRequest{VolumeId: volumeID, Inode: ino, Offset: 0, Size: int64(chunkSize)})
	if err != nil || rResp.GetError() != 0 {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if !bytes.Equal(rResp.GetData(), make([]byte, chunkSize)) {
		t.Fatalf("Expected 64 KiB of zeros")
	}
}

func TestControllerCheckpointRPC(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewServer(backend)
	volumeID := "test-checkpoint-rpc-vol"

	// 1. Empty volume_id returns error
	if _, err := server.Checkpoint(ctx, &pb.CheckpointRequest{}); err == nil {
		t.Fatalf("Expected error for empty volume_id")
	}

	// 2. Subscribe a watcher to receive events
	watcherCh := server.broadcaster.Subscribe(volumeID)
	defer server.broadcaster.Unsubscribe(volumeID, watcherCh)

	// 3. Initiate checkpoint request in a goroutine
	reqDone := make(chan *pb.CheckpointResponse, 1)
	reqErr := make(chan error, 1)
	go func() {
		resp, err := server.Checkpoint(ctx, &pb.CheckpointRequest{
			VolumeId: volumeID,
		})
		if err != nil {
			reqErr <- err
		} else {
			reqDone <- resp
		}
	}()

	// 4. Watcher should receive EVENT_CHECKPOINT_REQUEST with requestID
	var requestID string
	select {
	case ev := <-watcherCh:
		if ev.GetEventType() != pb.WatchEventType_EVENT_CHECKPOINT_REQUEST {
			t.Fatalf("Expected EVENT_CHECKPOINT_REQUEST, got %v", ev.GetEventType())
		}
		requestID = ev.GetCheckpointRequestId()
		if requestID == "" {
			t.Fatalf("Expected non-empty CheckpointRequestId in event")
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("Timed out waiting for WatchVolume EVENT_CHECKPOINT_REQUEST")
	}

	// 5. Node daemon acknowledges via AckCheckpoint
	_, err := server.AckCheckpoint(ctx, &pb.AckCheckpointRequest{
		VolumeId:       volumeID,
		RequestId:      requestID,
		StreamPosition: 12345,
	})
	if err != nil {
		t.Fatalf("AckCheckpoint failed: %v", err)
	}

	// 6. Waiting caller should unblock with stream position 12345
	select {
	case resp := <-reqDone:
		if resp.GetStreamPosition() != 12345 {
			t.Fatalf("Expected stream position 12345, got %d", resp.GetStreamPosition())
		}
	case err := <-reqErr:
		t.Fatalf("Checkpoint request failed: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatalf("Timed out waiting for Checkpoint caller to unblock")
	}
}

type faultInjectingStream struct {
	walclient.Stream
	streamID uuid.UUID
	degraded atomic.Pointer[walclient.DegradedError]
}

func (f *faultInjectingStream) degrade(cause error) {
	deg := &walclient.DegradedError{
		Cause: cause,
		At:    time.Now(),
	}
	if f.degraded.CompareAndSwap(nil, deg) {
		klog.Errorf("WAL client stream %s degraded: %v", f.streamID, deg)
	}
}

func (f *faultInjectingStream) Append(ctx context.Context, payload []byte) (uint64, error) {
	if deg := f.degraded.Load(); deg != nil {
		return 0, deg
	}
	return f.Stream.Append(ctx, payload)
}

func (f *faultInjectingStream) Wait(ctx context.Context, seq uint64, level walclient.Level, requestFlush bool) error {
	if deg := f.degraded.Load(); deg != nil {
		return deg
	}
	return f.Stream.Wait(ctx, seq, level, requestFlush)
}

func (f *faultInjectingStream) Flush(ctx context.Context) error {
	if deg := f.degraded.Load(); deg != nil {
		return deg
	}
	return f.Stream.Flush(ctx)
}

func (f *faultInjectingStream) Degraded() *walclient.DegradedError {
	if deg := f.degraded.Load(); deg != nil {
		return deg
	}
	return f.Stream.Degraded()
}

func TestControllerWALStreamDegraded(t *testing.T) {
	ctx := t.Context()
	storage := NewMemoryBackend()
	dir := t.TempDir()

	vol1ID := "test-degraded-vol1"
	vol2ID := "test-healthy-vol2"

	// Create real stream for vol2
	walDir2 := filepath.Join(dir, "wal2")
	streamID2 := StreamIDForVolume(vol2ID)
	stream2, err := walclient.Open(ctx, walDir2, streamID2, "")
	if err != nil {
		t.Fatalf("Open stream2 failed: %v", err)
	}
	defer stream2.Close()

	// Create real stream for vol1, wrapped with a fault-injecting proxy
	walDir1 := filepath.Join(dir, "wal1")
	streamID1 := StreamIDForVolume(vol1ID)
	realStream1, err := walclient.Open(ctx, walDir1, streamID1, "")
	if err != nil {
		t.Fatalf("Open realStream1 failed: %v", err)
	}
	defer realStream1.Close()

	faultStream1 := &faultInjectingStream{
		Stream:   realStream1,
		streamID: streamID1,
	}

	server := NewServer(storage,
		WithServerStreamFactory(func(volumeID string) (walclient.Stream, error) {
			if volumeID == vol1ID {
				return faultStream1, nil
			}
			if volumeID == vol2ID {
				return stream2, nil
			}
			return nil, fmt.Errorf("unexpected volume %s", volumeID)
		}),
	)
	defer server.Close()

	// Populate initial file and directory objects on vol1 while healthy so mutating RPCs reach the stream
	initFile, err := server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:       vol1ID,
		ParentInode:    1,
		Name:           "file1.txt",
		Mode:           0644,
		InitialContent: []byte("test-content"),
	})
	if err != nil || initFile.GetError() != 0 {
		t.Fatalf("setup CreateFile file1.txt: err=%v, resp=%v", err, initFile)
	}
	fileIno := initFile.GetAttr().GetInode().GetIno()

	initUnlink, err := server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:    vol1ID,
		ParentInode: 1,
		Name:        "file_for_unlink.txt",
		Mode:        0644,
	})
	if err != nil || initUnlink.GetError() != 0 {
		t.Fatalf("setup CreateFile file_for_unlink.txt: err=%v, resp=%v", err, initUnlink)
	}

	initRename, err := server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:    vol1ID,
		ParentInode: 1,
		Name:        "file_for_rename.txt",
		Mode:        0644,
	})
	if err != nil || initRename.GetError() != 0 {
		t.Fatalf("setup CreateFile file_for_rename.txt: err=%v, resp=%v", err, initRename)
	}

	initDir, err := server.Mkdir(ctx, &pb.MkdirRequest{
		VolumeId:    vol1ID,
		ParentInode: 1,
		Name:        "dir_for_rmdir",
		Mode:        0755,
	})
	if err != nil || initDir.GetError() != 0 {
		t.Fatalf("setup Mkdir dir_for_rmdir: err=%v, resp=%v", err, initDir)
	}

	// Initialize vol2 as well
	init2, err := server.Lookup(ctx, &pb.LookupRequest{VolumeId: vol2ID, ParentInode: 1, Name: "nonexistent"})
	if err != nil || init2.GetError() != int32(syscall.ENOENT) {
		t.Fatalf("setup Lookup vol2: err=%v, resp=%v", err, init2)
	}

	// Capture klog output to verify exactly one error log line in total across all operations
	var logBuf bytes.Buffer
	klog.LogToStderr(false)
	klog.SetOutput(&logBuf)
	defer func() {
		klog.LogToStderr(true)
		klog.SetOutput(os.Stderr)
	}()

	beforeErrorLines := klog.Stats.Error.Lines()

	// Degrade the stream on vol1
	faultStream1.degrade(errors.New("read-only file system"))

	// Drive every mutating RPC through the server in a table
	mutatingOps := []struct {
		name        string
		call        func() int32
		expectedErr int32
	}{
		{
			name: "CreateFile",
			call: func() int32 {
				resp, rErr := server.CreateFile(ctx, &pb.CreateFileRequest{
					VolumeId:    vol1ID,
					ParentInode: 1,
					Name:        "newfile.txt",
					Mode:        0644,
				})
				if rErr != nil {
					t.Fatalf("CreateFile gRPC error: %v", rErr)
				}
				return resp.GetError()
			},
			expectedErr: int32(syscall.EIO),
		},
		{
			// Buffered write is accepted into staging memory (POSIX contract); durability fails at subsequent Fsync.
			name: "WriteFile",
			call: func() int32 {
				resp, rErr := server.WriteFile(ctx, &pb.WriteFileRequest{
					VolumeId: vol1ID,
					Inode:    fileIno,
					Offset:   0,
					Data:     []byte("mutating-write-data"),
				})
				if rErr != nil {
					t.Fatalf("WriteFile gRPC error: %v", rErr)
				}
				return resp.GetError()
			},
			expectedErr: 0,
		},
		{
			// Fsync following WriteFile flushes staged data to the degraded stream and returns EIO.
			name: "Fsync",
			call: func() int32 {
				resp, rErr := server.Fsync(ctx, &pb.FsyncRequest{
					VolumeId: vol1ID,
					Inode:    fileIno,
					Blocking: true,
				})
				if rErr != nil {
					t.Fatalf("Fsync gRPC error: %v", rErr)
				}
				return resp.GetError()
			},
			expectedErr: int32(syscall.EIO),
		},
		{
			name: "Mkdir",
			call: func() int32 {
				resp, rErr := server.Mkdir(ctx, &pb.MkdirRequest{
					VolumeId:    vol1ID,
					ParentInode: 1,
					Name:        "newdir",
					Mode:        0755,
				})
				if rErr != nil {
					t.Fatalf("Mkdir gRPC error: %v", rErr)
				}
				return resp.GetError()
			},
			expectedErr: int32(syscall.EIO),
		},
		{
			name: "SetAttr",
			call: func() int32 {
				mode := uint32(0600)
				resp, rErr := server.SetAttr(ctx, &pb.SetAttrRequest{
					VolumeId: vol1ID,
					Inode:    fileIno,
					Mode:     &mode,
				})
				if rErr != nil {
					t.Fatalf("SetAttr gRPC error: %v", rErr)
				}
				return resp.GetError()
			},
			expectedErr: int32(syscall.EIO),
		},
		{
			name: "TruncateFile",
			call: func() int32 {
				resp, rErr := server.TruncateFile(ctx, &pb.TruncateFileRequest{
					VolumeId: vol1ID,
					Inode:    fileIno,
					Size:     0,
				})
				if rErr != nil {
					t.Fatalf("TruncateFile gRPC error: %v", rErr)
				}
				return resp.GetError()
			},
			expectedErr: int32(syscall.EIO),
		},
		{
			name: "PunchHole",
			call: func() int32 {
				resp, rErr := server.PunchHole(ctx, &pb.PunchHoleRequest{
					VolumeId: vol1ID,
					Inode:    fileIno,
					Offset:   0,
					Length:   4,
				})
				if rErr != nil {
					t.Fatalf("PunchHole gRPC error: %v", rErr)
				}
				return resp.GetError()
			},
			expectedErr: int32(syscall.EIO),
		},
		{
			name: "Symlink",
			call: func() int32 {
				resp, rErr := server.Symlink(ctx, &pb.SymlinkRequest{
					VolumeId:    vol1ID,
					ParentInode: 1,
					Name:        "newsymlink",
					Target:      "target",
				})
				if rErr != nil {
					t.Fatalf("Symlink gRPC error: %v", rErr)
				}
				return resp.GetError()
			},
			expectedErr: int32(syscall.EIO),
		},
		{
			name: "Link",
			call: func() int32 {
				resp, rErr := server.Link(ctx, &pb.LinkRequest{
					VolumeId:       vol1ID,
					OldInode:       fileIno,
					NewParentInode: 1,
					NewName:        "newlink",
				})
				if rErr != nil {
					t.Fatalf("Link gRPC error: %v", rErr)
				}
				return resp.GetError()
			},
			expectedErr: int32(syscall.EIO),
		},
		{
			name: "Rename",
			call: func() int32 {
				resp, rErr := server.Rename(ctx, &pb.RenameRequest{
					VolumeId:       vol1ID,
					OldParentInode: 1,
					OldName:        "file_for_rename.txt",
					NewParentInode: 1,
					NewName:        "file_renamed.txt",
				})
				if rErr != nil {
					t.Fatalf("Rename gRPC error: %v", rErr)
				}
				return resp.GetError()
			},
			expectedErr: int32(syscall.EIO),
		},
		{
			name: "Unlink",
			call: func() int32 {
				resp, rErr := server.Unlink(ctx, &pb.UnlinkRequest{
					VolumeId:    vol1ID,
					ParentInode: 1,
					Name:        "file_for_unlink.txt",
				})
				if rErr != nil {
					t.Fatalf("Unlink gRPC error: %v", rErr)
				}
				return resp.GetError()
			},
			expectedErr: int32(syscall.EIO),
		},
		{
			name: "Rmdir",
			call: func() int32 {
				resp, rErr := server.Rmdir(ctx, &pb.RmdirRequest{
					VolumeId:    vol1ID,
					ParentInode: 1,
					Name:        "dir_for_rmdir",
				})
				if rErr != nil {
					t.Fatalf("Rmdir gRPC error: %v", rErr)
				}
				return resp.GetError()
			},
			expectedErr: int32(syscall.EIO),
		},
	}

	for _, tc := range mutatingOps {
		errCode := tc.call()
		if errCode != tc.expectedErr {
			t.Errorf("%s: expected error %d, got %d", tc.name, tc.expectedErr, errCode)
		}
	}

	klog.Flush()
	expectedLog := fmt.Sprintf("WAL client stream %s degraded", streamID1)
	if !strings.Contains(logBuf.String(), expectedLog) {
		t.Fatalf("expected log %q to appear in log output:\n%s", expectedLog, logBuf.String())
	}
	if diff := klog.Stats.Error.Lines() - beforeErrorLines; diff != 1 {
		t.Fatalf("expected exactly 1 error log line in total, got %d", diff)
	}

	// Verify GetVolumeStats reports structured Degraded status on vol1
	stats1, err := server.GetVolumeStats(ctx, &pb.GetVolumeStatsRequest{VolumeId: vol1ID})
	if err != nil {
		t.Fatalf("GetVolumeStats vol1 failed: %v", err)
	}
	if stats1.GetDegraded() == nil {
		t.Fatalf("expected non-nil Degraded status on vol1")
	}
	if stats1.GetDegraded().GetCode() != pb.Degraded_WAL_STREAM_DEGRADED {
		t.Fatalf("expected Degraded.Code to be WAL_STREAM_DEGRADED, got %v", stats1.GetDegraded().GetCode())
	}
	if !strings.Contains(stats1.GetDegraded().GetMessage(), "read-only file system") {
		t.Fatalf("expected Degraded.Message containing 'read-only file system' on vol1, got: %q", stats1.GetDegraded().GetMessage())
	}
	if stats1.GetDegraded().GetSince() == nil || stats1.GetDegraded().GetSince().AsTime().IsZero() {
		t.Fatalf("expected non-zero Degraded.Since on vol1")
	}

	// Verify ListVolumes reports structured Degraded status on vol1 and nil on vol2
	listResp, err := server.ListVolumes(ctx, &pb.ListVolumesRequest{})
	if err != nil {
		t.Fatalf("ListVolumes failed: %v", err)
	}
	var foundVol1, foundVol2 bool
	for _, vi := range listResp.GetVolumes() {
		if vi.GetVolumeId() == vol1ID {
			foundVol1 = true
			if vi.GetDegraded() == nil {
				t.Fatalf("expected non-nil Degraded on vol1 in ListVolumes")
			}
			if vi.GetDegraded().GetCode() != stats1.GetDegraded().GetCode() {
				t.Fatalf("expected ListVolumes vol1 degraded code %v, got %v", stats1.GetDegraded().GetCode(), vi.GetDegraded().GetCode())
			}
			if vi.GetDegraded().GetMessage() != stats1.GetDegraded().GetMessage() {
				t.Fatalf("expected ListVolumes vol1 degraded message %q, got %q", stats1.GetDegraded().GetMessage(), vi.GetDegraded().GetMessage())
			}
			if vi.GetDegraded().GetSince() == nil || vi.GetDegraded().GetSince().AsTime().IsZero() {
				t.Fatalf("expected non-zero Degraded.Since in ListVolumes on vol1")
			}
		}
		if vi.GetVolumeId() == vol2ID {
			foundVol2 = true
			if vi.GetDegraded() != nil {
				t.Fatalf("expected ListVolumes vol2 degraded to be nil, got %v", vi.GetDegraded())
			}
		}
	}
	if !foundVol1 || !foundVol2 {
		t.Fatalf("expected both volumes in ListVolumes: foundVol1=%v, foundVol2=%v", foundVol1, foundVol2)
	}

	// Mutating operation on vol2 on the same server should keep working
	resp2, err := server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:       vol2ID,
		ParentInode:    1,
		Name:           "file2.txt",
		Mode:           0644,
		InitialContent: []byte("healthy-data"),
	})
	if err != nil {
		t.Fatalf("CreateFile vol2 failed: %v", err)
	}
	if resp2.GetError() != 0 {
		t.Fatalf("expected success (error=0) on vol2, got %d", resp2.GetError())
	}

	// Verify GetVolumeStats on vol2 is not degraded
	stats2, err := server.GetVolumeStats(ctx, &pb.GetVolumeStatsRequest{VolumeId: vol2ID})
	if err != nil {
		t.Fatalf("GetVolumeStats vol2 failed: %v", err)
	}
	if stats2.GetDegraded() != nil {
		t.Fatalf("expected nil Degraded on vol2, got: %v", stats2.GetDegraded())
	}
}

func TestSharedWALBufferConnectionAcrossVolumes(t *testing.T) {
	bufDir := t.TempDir()
	_, target, cleanup := startTestWalBufferServer(t, bufDir)
	defer cleanup()

	var dialCount atomic.Int32
	testDialer := func(ctx context.Context, addr string) (net.Conn, error) {
		dialCount.Add(1)
		var d net.Dialer
		return d.DialContext(ctx, "tcp", addr)
	}

	clientDir := t.TempDir()
	backend := NewMemoryBackend()
	server := NewServer(backend,
		WithServerWAL(clientDir, target, walclient.Witness),
		WithServerWALDialOptions(grpc.WithContextDialer(testDialer)),
	)
	defer func() { _ = server.Close() }()

	ctx := t.Context()

	// Open two volumes on the controller and perform mutations.
	_, err := testMkdir(ctx, server, "vol-1", "/dir1", 0755, 0, 0)
	if err != nil {
		t.Fatalf("vol-1 mkdir failed: %v", err)
	}
	_, err = testMkdir(ctx, server, "vol-2", "/dir2", 0755, 0, 0)
	if err != nil {
		t.Fatalf("vol-2 mkdir failed: %v", err)
	}

	vol1 := mustGetVolume(t, server, "vol-1")
	vol2 := mustGetVolume(t, server, "vol-2")
	if vol1.Stream() == nil || vol2.Stream() == nil {
		t.Fatalf("expected both volumes to have active WAL streams")
	}

	// Verify watermarks to ensure records reached the buffer over the connection.
	local1, witness1, _ := vol1.Stream().Watermarks()
	if local1 < 1 || witness1 < 1 {
		t.Fatalf("vol-1 expected local and witness watermarks >= 1, got local=%d witness=%d", local1, witness1)
	}
	local2, witness2, _ := vol2.Stream().Watermarks()
	if local2 < 1 || witness2 < 1 {
		t.Fatalf("vol-2 expected local and witness watermarks >= 1, got local=%d witness=%d", local2, witness2)
	}

	// Assert only a single connection was dialed across both volumes.
	if got := dialCount.Load(); got != 1 {
		t.Fatalf("expected exactly 1 dialed connection across volumes, got %d", got)
	}

	if err := server.Close(); err != nil {
		t.Fatalf("server.Close failed: %v", err)
	}
}

func TestExclusiveHold_AcquireRenewRelease(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewServer(backend, WithExclusiveLeaseTTL(10*time.Second))
	defer func() { _ = server.Close() }()

	volumeID := "test-hold-vol"

	// 1. Initial GetExclusiveHold should return nil
	getResp, err := server.GetExclusiveHold(ctx, &pb.GetExclusiveHoldRequest{VolumeId: volumeID})
	if err != nil {
		t.Fatalf("GetExclusiveHold failed: %v", err)
	}
	if getResp.GetHold() != nil {
		t.Fatalf("Expected nil hold initially, got: %v", getResp.GetHold())
	}

	// 2. Node 1 acquires exclusive hold
	acqResp, err := server.AcquireExclusive(ctx, &pb.AcquireExclusiveRequest{
		VolumeId: volumeID,
		NodeId:   "node-1",
		MountId:  "/mnt/target1",
	})
	if err != nil {
		t.Fatalf("AcquireExclusive failed: %v", err)
	}
	if acqResp.GetHold().GetNodeId() != "node-1" || acqResp.GetHold().GetMountId() != "/mnt/target1" {
		t.Fatalf("Unexpected hold in response: %v", acqResp.GetHold())
	}
	initialExpiry := acqResp.GetHold().GetLeaseExpiry().AsTime()

	// 3. Re-acquire by same node/mount (idempotent)
	reacqResp, err := server.AcquireExclusive(ctx, &pb.AcquireExclusiveRequest{
		VolumeId: volumeID,
		NodeId:   "node-1",
		MountId:  "/mnt/target1",
	})
	if err != nil {
		t.Fatalf("Idempotent AcquireExclusive failed: %v", err)
	}
	if !reacqResp.GetHold().GetLeaseExpiry().AsTime().After(initialExpiry) && !reacqResp.GetHold().GetLeaseExpiry().AsTime().Equal(initialExpiry) {
		t.Fatalf("Expected lease to be extended or equal, was %v vs %v", reacqResp.GetHold().GetLeaseExpiry().AsTime(), initialExpiry)
	}

	// 4. Renew hold by node-1
	renewResp, err := server.RenewExclusive(ctx, &pb.RenewExclusiveRequest{
		VolumeId: volumeID,
		NodeId:   "node-1",
		MountId:  "/mnt/target1",
	})
	if err != nil {
		t.Fatalf("RenewExclusive failed: %v", err)
	}
	if renewResp.GetHold().GetNodeId() != "node-1" {
		t.Fatalf("Unexpected renew hold: %v", renewResp.GetHold())
	}

	// 5. GetExclusiveHold verifies hold is active
	getResp, err = server.GetExclusiveHold(ctx, &pb.GetExclusiveHoldRequest{VolumeId: volumeID})
	if err != nil {
		t.Fatalf("GetExclusiveHold failed: %v", err)
	}
	if getResp.GetHold() == nil || getResp.GetHold().GetNodeId() != "node-1" {
		t.Fatalf("Expected active hold for node-1, got: %v", getResp.GetHold())
	}

	// 6. Release hold
	_, err = server.ReleaseExclusive(ctx, &pb.ReleaseExclusiveRequest{
		VolumeId: volumeID,
		NodeId:   "node-1",
		MountId:  "/mnt/target1",
	})
	if err != nil {
		t.Fatalf("ReleaseExclusive failed: %v", err)
	}

	// 7. GetExclusiveHold verifies hold is gone
	getResp, err = server.GetExclusiveHold(ctx, &pb.GetExclusiveHoldRequest{VolumeId: volumeID})
	if err != nil {
		t.Fatalf("GetExclusiveHold failed: %v", err)
	}
	if getResp.GetHold() != nil {
		t.Fatalf("Expected nil hold after release, got: %v", getResp.GetHold())
	}
}

func TestExclusiveHold_ConflictRejection(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewServer(backend, WithExclusiveLeaseTTL(10*time.Second))
	defer func() { _ = server.Close() }()

	volumeID := "test-conflict-vol"

	// Node 1 acquires hold
	_, err := server.AcquireExclusive(ctx, &pb.AcquireExclusiveRequest{
		VolumeId: volumeID,
		NodeId:   "node-1",
		MountId:  "/mnt/target1",
	})
	if err != nil {
		t.Fatalf("AcquireExclusive node-1 failed: %v", err)
	}

	// Node 2 attempts to acquire -> should be rejected with codes.Aborted
	_, err = server.AcquireExclusive(ctx, &pb.AcquireExclusiveRequest{
		VolumeId: volumeID,
		NodeId:   "node-2",
		MountId:  "/mnt/target2",
	})
	if err == nil {
		t.Fatalf("Expected AcquireExclusive by node-2 to fail while node-1 holds lease")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Aborted {
		t.Fatalf("Expected codes.Aborted, got: %v (st=%v)", err, st)
	}
	if !strings.Contains(st.Message(), "node-1") || !strings.Contains(st.Message(), "/mnt/target1") {
		t.Fatalf("Expected error message to mention holder node-1 and mount /mnt/target1, got: %s", st.Message())
	}

	// Node 2 attempts to renew -> should be rejected with codes.FailedPrecondition
	_, err = server.RenewExclusive(ctx, &pb.RenewExclusiveRequest{
		VolumeId: volumeID,
		NodeId:   "node-2",
		MountId:  "/mnt/target2",
	})
	if err == nil {
		t.Fatalf("Expected RenewExclusive by node-2 to fail")
	}
	st, ok = status.FromError(err)
	if !ok || st.Code() != codes.FailedPrecondition {
		t.Fatalf("Expected codes.FailedPrecondition, got: %v (st=%v)", err, st)
	}

	// Node 1 releases hold
	_, err = server.ReleaseExclusive(ctx, &pb.ReleaseExclusiveRequest{
		VolumeId: volumeID,
		NodeId:   "node-1",
		MountId:  "/mnt/target1",
	})
	if err != nil {
		t.Fatalf("ReleaseExclusive failed: %v", err)
	}

	// Node 2 can now acquire hold
	acqResp2, err := server.AcquireExclusive(ctx, &pb.AcquireExclusiveRequest{
		VolumeId: volumeID,
		NodeId:   "node-2",
		MountId:  "/mnt/target2",
	})
	if err != nil {
		t.Fatalf("AcquireExclusive by node-2 failed after node-1 released: %v", err)
	}
	if acqResp2.GetHold().GetNodeId() != "node-2" {
		t.Fatalf("Expected hold to belong to node-2, got: %v", acqResp2.GetHold())
	}
}

func TestExclusiveHold_LeaseExpiry(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	// Configure short lease TTL (50ms) to test expiry
	server := NewServer(backend, WithExclusiveLeaseTTL(50*time.Millisecond))
	defer func() { _ = server.Close() }()

	volumeID := "test-expiry-vol"

	// Node 1 acquires hold
	_, err := server.AcquireExclusive(ctx, &pb.AcquireExclusiveRequest{
		VolumeId: volumeID,
		NodeId:   "node-1",
		MountId:  "/mnt/target1",
	})
	if err != nil {
		t.Fatalf("AcquireExclusive failed: %v", err)
	}

	// Node 2 rejected immediately
	_, err = server.AcquireExclusive(ctx, &pb.AcquireExclusiveRequest{
		VolumeId: volumeID,
		NodeId:   "node-2",
		MountId:  "/mnt/target2",
	})
	if err == nil {
		t.Fatalf("Expected immediate acquire by node-2 to fail")
	}

	// Wait for lease to expire
	time.Sleep(80 * time.Millisecond)

	// Node 2 can now acquire hold because Node 1's lease expired
	acqResp2, err := server.AcquireExclusive(ctx, &pb.AcquireExclusiveRequest{
		VolumeId: volumeID,
		NodeId:   "node-2",
		MountId:  "/mnt/target2",
	})
	if err != nil {
		t.Fatalf("AcquireExclusive by node-2 failed after lease expiry: %v", err)
	}
	if acqResp2.GetHold().GetNodeId() != "node-2" {
		t.Fatalf("Expected node-2 to be holder, got: %v", acqResp2.GetHold())
	}
}

func TestExclusiveHold_ControllerRestart_RenewReestablishes(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	// Server 1
	server1 := NewServer(backend, WithExclusiveLeaseTTL(10*time.Second))
	defer func() { _ = server1.Close() }()

	volumeID := "test-restart-vol"

	_, err := server1.AcquireExclusive(ctx, &pb.AcquireExclusiveRequest{
		VolumeId: volumeID,
		NodeId:   "node-1",
		MountId:  "/mnt/target1",
	})
	if err != nil {
		t.Fatalf("AcquireExclusive server1 failed: %v", err)
	}

	// Simulate controller restart: new server instance with same backend, but empty memory lease table
	server2 := NewServer(backend, WithExclusiveLeaseTTL(10*time.Second))
	defer func() { _ = server2.Close() }()

	// Periodic renew from node-1 arrives at restarted controller: must re-establish hold
	renewResp, err := server2.RenewExclusive(ctx, &pb.RenewExclusiveRequest{
		VolumeId: volumeID,
		NodeId:   "node-1",
		MountId:  "/mnt/target1",
	})
	if err != nil {
		t.Fatalf("RenewExclusive on restarted server failed: %v", err)
	}
	if renewResp.GetHold().GetNodeId() != "node-1" {
		t.Fatalf("Expected re-established hold for node-1, got: %v", renewResp.GetHold())
	}

	// Node 2 must now be rejected by restarted controller because Node 1 re-established hold
	_, err = server2.AcquireExclusive(ctx, &pb.AcquireExclusiveRequest{
		VolumeId: volumeID,
		NodeId:   "node-2",
		MountId:  "/mnt/target2",
	})
	if err == nil {
		t.Fatalf("Expected node-2 acquire to be rejected after renew re-established hold")
	}
}

func TestControllerRepairVolume(t *testing.T) {
	ctx := t.Context()
	_, walTarget, stopWal := startTestWalBufferServer(t, t.TempDir())
	defer stopWal()

	storage := NewMemoryBackend()
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")

	vol1ID := "repair-vol-1"
	vol2ID := "healthy-vol-2"
	streamID1 := StreamIDForVolume(vol1ID)
	streamID2 := StreamIDForVolume(vol2ID)

	server := NewServer(storage,
		WithServerWAL(walDir, walTarget, walclient.Witness),
	)
	defer server.Close()

	// 1. Initialize data on both volumes using shared walDir
	init1, err := server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:       vol1ID,
		ParentInode:    1,
		Name:           "file1.txt",
		InitialContent: []byte("content1"),
	})
	if err != nil || init1.GetError() != 0 {
		t.Fatalf("CreateFile vol1 failed: %v, resp: %v", err, init1)
	}

	init2, err := server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:       vol2ID,
		ParentInode:    1,
		Name:           "file2.txt",
		InitialContent: []byte("content2"),
	})
	if err != nil || init2.GetError() != 0 {
		t.Fatalf("CreateFile vol2 failed: %v, resp: %v", err, init2)
	}

	// Read entries in shared walDir before repair
	walEntriesBefore, err := os.ReadDir(walDir)
	if err != nil {
		t.Fatalf("ReadDir walDir failed: %v", err)
	}
	var vol2FilesBefore []string
	var vol1FilesBefore []string
	vol2Prefix := fmt.Sprintf("stream-%s-", streamID2.String())
	vol1Prefix := fmt.Sprintf("stream-%s-", streamID1.String())
	for _, e := range walEntriesBefore {
		if strings.HasPrefix(e.Name(), vol2Prefix) {
			vol2FilesBefore = append(vol2FilesBefore, e.Name())
		}
		if strings.HasPrefix(e.Name(), vol1Prefix) {
			vol1FilesBefore = append(vol1FilesBefore, e.Name())
		}
	}
	if len(vol2FilesBefore) == 0 {
		t.Fatalf("expected segment files for vol2 in walDir before repair")
	}
	if len(vol1FilesBefore) == 0 {
		t.Fatalf("expected segment files for vol1 in walDir before repair")
	}

	// 2. Degrade vol1 by wrapping its active stream with faultInjectingStream
	vol1 := mustGetVolume(t, server, vol1ID)
	faultStream1 := &faultInjectingStream{
		Stream:   vol1.Stream(),
		streamID: streamID1,
	}
	if err := vol1.ResetStream(faultStream1); err != nil {
		t.Fatalf("ResetStream failed: %v", err)
	}
	faultStream1.degrade(fmt.Errorf("injected disk failure on vol1"))

	vol1Stats, err := server.GetVolumeStats(ctx, &pb.GetVolumeStatsRequest{VolumeId: vol1ID})
	if err != nil {
		t.Fatalf("GetVolumeStats vol1 failed: %v", err)
	}
	if vol1Stats.GetDegraded() == nil {
		t.Fatalf("expected vol1 to be degraded")
	}

	vol2Stats, err := server.GetVolumeStats(ctx, &pb.GetVolumeStatsRequest{VolumeId: vol2ID})
	if err != nil {
		t.Fatalf("GetVolumeStats vol2 failed: %v", err)
	}
	if vol2Stats.GetDegraded() != nil {
		t.Fatalf("expected vol2 to be healthy")
	}

	// 3. RepairVolume on healthy vol2 without --force must be refused
	_, err = server.RepairVolume(ctx, &pb.RepairVolumeRequest{VolumeId: vol2ID, Force: false})
	if err == nil {
		t.Fatalf("expected RepairVolume on healthy volume without --force to fail")
	}
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition, got: %v", err)
	}

	// Also verify RepairVolume on nonexistent volume returns NotFound
	_, err = server.RepairVolume(ctx, &pb.RepairVolumeRequest{VolumeId: "nonexistent-vol", Force: false})
	if err == nil {
		t.Fatalf("expected RepairVolume on nonexistent volume to fail")
	}
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound, got: %v", err)
	}

	// 4. RepairVolume on degraded vol1
	repairResp, err := server.RepairVolume(ctx, &pb.RepairVolumeRequest{VolumeId: vol1ID, Force: false})
	if err != nil {
		t.Fatalf("RepairVolume vol1 failed: %v", err)
	}

	oldPath := repairResp.GetOldPath()
	if !strings.HasSuffix(oldPath, ".degraded") {
		t.Fatalf("expected oldPath to have suffix '.degraded', got: %s", oldPath)
	}
	if !strings.Contains(oldPath, fmt.Sprintf("stream-%s", streamID1.String())) {
		t.Fatalf("expected oldPath to contain stream-%s, got: %s", streamID1.String(), oldPath)
	}
	if _, err := os.Stat(oldPath); err != nil {
		t.Fatalf("expected moved segment directory %s to exist: %v", oldPath, err)
	}

	// Verify vol1's previous segment files are now inside oldPath
	oldEntries, err := os.ReadDir(oldPath)
	if err != nil {
		t.Fatalf("ReadDir oldPath failed: %v", err)
	}
	if len(oldEntries) == 0 {
		t.Fatalf("expected moved segment files in %s, got 0", oldPath)
	}
	for _, e := range oldEntries {
		if !strings.HasPrefix(e.Name(), vol1Prefix) {
			t.Fatalf("unexpected file %s in degraded directory %s", e.Name(), oldPath)
		}
	}

	// 5. Verify other volume (vol2)'s files are STILL IN PLACE BY NAME
	for _, name := range vol2FilesBefore {
		p := filepath.Join(walDir, name)
		if fi, err := os.Stat(p); err != nil || fi.IsDir() {
			t.Fatalf("expected vol2 segment file %s to still be in place in %s: %v", name, walDir, err)
		}
	}
	vol2DegradedMatches, _ := filepath.Glob(filepath.Join(walDir, fmt.Sprintf("stream-%s.*.degraded", streamID2.String())))
	if len(vol2DegradedMatches) > 0 {
		t.Fatalf("walDir files for vol2 were moved aside during vol1 repair: %v", vol2DegradedMatches)
	}

	// 6. Verify vol2 continues operating normally
	read2, err := testReadFile(ctx, server, vol2ID, "/file2.txt", 0, 100)
	if err != nil {
		t.Fatalf("ReadFile on vol2 failed after vol1 repair: %v", err)
	}
	if string(read2.GetData()) != "content2" {
		t.Fatalf("unexpected data on vol2: %s", string(read2.GetData()))
	}

	// 7. Verify vol1 accepts an fsynced write afterwards
	createResp, err := server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:       vol1ID,
		ParentInode:    1,
		Name:           "after_repair.txt",
		InitialContent: []byte("after repair content"),
	})
	if err != nil || createResp.GetError() != 0 {
		t.Fatalf("CreateFile on vol1 after repair failed: %v, resp: %v", err, createResp)
	}
	fsyncResp, err := server.Fsync(ctx, &pb.FsyncRequest{
		VolumeId: vol1ID,
		Inode:    createResp.GetAttr().GetInode().GetIno(),
		Blocking: true,
	})
	if err != nil || fsyncResp.GetError() != 0 {
		t.Fatalf("Fsync on vol1 after repair failed: %v, resp: %v", err, fsyncResp)
	}

	// And vol1 is healthy
	vol1StatsAfter, err := server.GetVolumeStats(ctx, &pb.GetVolumeStatsRequest{VolumeId: vol1ID})
	if err != nil {
		t.Fatalf("GetVolumeStats vol1 after repair failed: %v", err)
	}
	if vol1StatsAfter.GetDegraded() != nil {
		t.Fatalf("expected vol1 to be healthy after repair, got: %v", vol1StatsAfter.GetDegraded())
	}
}

type faultInjectingTruncateStore struct {
	walclient.SegmentStore
	fail bool
}

func (f *faultInjectingTruncateStore) TruncateAfter(seq uint64) error {
	if f.fail {
		return fmt.Errorf("injected TruncateAfter failure on store")
	}
	return f.SegmentStore.TruncateAfter(seq)
}

func TestControllerRestartDivergenceRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	_, walTarget, stopWal := startTestWalBufferServer(t, t.TempDir())
	defer stopWal()

	storage := NewMemoryBackend()
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	volumeID := "restart-vol"
	streamID := StreamIDForVolume(volumeID)

	// 1. Another server node writes authoritative records to wal-buffer for streamID
	storageB := NewMemoryBackend()
	nodeBDir := t.TempDir()
	serverB := NewServer(storageB, WithServerWAL(nodeBDir, walTarget, walclient.Permanent))
	initB, err := serverB.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:       volumeID,
		ParentInode:    1,
		Name:           "fileB.txt",
		InitialContent: []byte("authoritative content from node B"),
	})
	if err != nil || initB.GetError() != 0 {
		t.Fatalf("CreateFile on serverB failed: %v, resp: %v", err, initB)
	}
	fsyncB, err := serverB.Fsync(ctx, &pb.FsyncRequest{
		VolumeId: volumeID,
		Inode:    initB.GetAttr().GetInode().GetIno(),
		Blocking: true,
	})
	if err != nil || fsyncB.GetError() != 0 {
		t.Fatalf("Fsync on serverB failed: %v, resp: %v", err, fsyncB)
	}
	if err := serverB.Close(); err != nil {
		t.Fatalf("Close serverB failed: %v", err)
	}

	// 2. Server 1 writes local records in local mode (not replicated to buffer), producing divergent records ahead of buffer
	server1 := NewServer(storage, WithServerWAL(walDir, "", walclient.Local))
	for i := 1; i <= 3; i++ {
		initResp, err := server1.CreateFile(ctx, &pb.CreateFileRequest{
			VolumeId:       volumeID,
			ParentInode:    1,
			Name:           fmt.Sprintf("divergent_local_%d.txt", i),
			InitialContent: []byte(fmt.Sprintf("divergent local content %d", i)),
		})
		if err != nil || initResp.GetError() != 0 {
			t.Fatalf("CreateFile on server1 %d failed: %v, resp: %v", i, err, initResp)
		}
	}
	if err := server1.Close(); err != nil {
		t.Fatalf("Close server1 failed: %v", err)
	}

	// 3. Controller restart with fault STILL PRESENT:
	// Inject fault so TruncateAfter fails
	realStore, recovered, err := wal.NewClientSegmentStore(walDir, fmt.Sprintf("stream-%s", streamID.String()), 100)
	if err != nil {
		t.Fatalf("NewClientSegmentStore failed: %v", err)
	}
	faultStore := &faultInjectingTruncateStore{
		SegmentStore: realStore,
		fail:         true,
	}
	faultStream := func(volID string) (walclient.Stream, error) {
		return walclient.Open(ctx, walDir, streamID, walTarget,
			walclient.WithStore(faultStore, recovered),
			walclient.WithDegradedRetryBackoff(10*time.Millisecond, 50*time.Millisecond),
		)
	}

	// Server 2 starts up cleanly - not crashlooping!
	server2 := NewServer(storage, WithServerStreamFactory(faultStream), WithServerDurability(walclient.Permanent))

	// Volume comes back degraded, but server is alive and volume is readable
	statsResp2, err := server2.GetVolumeStats(ctx, &pb.GetVolumeStatsRequest{VolumeId: volumeID})
	if err != nil {
		t.Fatalf("GetVolumeStats on server2 failed: %v", err)
	}
	if statsResp2.GetDegraded() == nil {
		t.Fatalf("expected volume to come back degraded on server2 with fault present")
	}

	// Lookup succeeds (volume remains readable)
	lookupResp, err := server2.Lookup(ctx, &pb.LookupRequest{
		VolumeId:    volumeID,
		ParentInode: 1,
		Name:        "divergent_local_1.txt",
	})
	if err != nil || lookupResp.GetError() != 0 {
		t.Fatalf("Lookup on degraded volume failed: %v, resp: %v", err, lookupResp)
	}

	// Mutating RPC (CreateFile) fails with EIO on degraded volume
	createResp2, err := server2.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:       volumeID,
		ParentInode:    1,
		Name:           "fail_create.txt",
		InitialContent: []byte("fail"),
	})
	if err != nil || createResp2.GetError() != int32(syscall.EIO) {
		t.Fatalf("expected CreateFile to return EIO on degraded volume, got err=%v, resp=%v", err, createResp2)
	}

	// WriteFile stages into memory, but Fsync fails with EIO
	ino := lookupResp.GetAttr().GetInode().GetIno()
	writeResp2, err := server2.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId: volumeID,
		Inode:    ino,
		Offset:   0,
		Data:     []byte("staged data"),
	})
	if err != nil || writeResp2.GetError() != 0 {
		t.Fatalf("expected WriteFile staging to succeed, got err=%v, resp=%v", err, writeResp2)
	}
	fsyncResp2, err := server2.Fsync(ctx, &pb.FsyncRequest{
		VolumeId: volumeID,
		Inode:    ino,
		Blocking: true,
	})
	if err != nil || fsyncResp2.GetError() != int32(syscall.EIO) {
		t.Fatalf("expected Fsync to fail with EIO on degraded volume, got err=%v, resp=%v", err, fsyncResp2)
	}

	if err := server2.Close(); err != nil {
		t.Fatalf("Close server2 failed: %v", err)
	}

	// 4. Controller restart with fault CLEARED:
	// Real store truncate succeeds, controller recovers the volume by itself
	server3 := NewServer(storage, WithServerWAL(walDir, walTarget, walclient.Permanent))
	defer server3.Close()

	statsResp3, err := server3.GetVolumeStats(ctx, &pb.GetVolumeStatsRequest{VolumeId: volumeID})
	if err != nil {
		t.Fatalf("GetVolumeStats on server3 failed: %v", err)
	}
	if statsResp3.GetDegraded() != nil {
		t.Fatalf("expected volume to be healthy on server3 with fault cleared, got degraded: %v", statsResp3.GetDegraded())
	}

	// Volume accepts an fsynced write
	createResp3, err := server3.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:       volumeID,
		ParentInode:    1,
		Name:           "recovered_write.txt",
		InitialContent: []byte("recovered data"),
	})
	if err != nil || createResp3.GetError() != 0 {
		t.Fatalf("CreateFile on server3 failed: %v, resp: %v", err, createResp3)
	}
	fsyncResp3, err := server3.Fsync(ctx, &pb.FsyncRequest{
		VolumeId: volumeID,
		Inode:    createResp3.GetAttr().GetInode().GetIno(),
		Blocking: true,
	})
	if err != nil || fsyncResp3.GetError() != 0 {
		t.Fatalf("Fsync on server3 failed: %v, resp: %v", err, fsyncResp3)
	}
}
