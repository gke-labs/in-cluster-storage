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
	"fmt"
	"sort"
	"sync"
	"syscall"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/hanwen/go-fuse/v2/fuse"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/klog/v2"
)

// MaxNameLength is the maximum allowed byte length for a path component name
// (matching the POSIX NAME_MAX limit of 255 bytes advertised by statfs).
const MaxNameLength = 255

// DefaultRPCTimeout is the default deadline applied to ObjectFS controller RPCs (metadata, reads).
const DefaultRPCTimeout = 30 * time.Second

// DefaultSyncTimeout is the default deadline applied to sync operations (Fsync, Flush, Write).
const DefaultSyncTimeout = 10 * time.Minute

type ObjectFS struct {
	fuse.RawFileSystem

	client        pb.ObjectFSControllerClient
	volumeID      string
	writeMode     pb.WriteMode
	cache         *NodeCache
	server        *fuse.Server
	rpcTimeout    time.Duration
	syncTimeout   time.Duration
	errMu         sync.Mutex
	lastLoggedErr time.Time
}

var _ fuse.RawFileSystem = (*ObjectFS)(nil)

func NewObjectFS(client pb.ObjectFSControllerClient, volumeID string, writeMode pb.WriteMode, cache *NodeCache) *ObjectFS {
	return NewObjectFSWithTimeouts(client, volumeID, writeMode, cache, DefaultRPCTimeout, DefaultSyncTimeout)
}

func NewObjectFSWithTimeout(client pb.ObjectFSControllerClient, volumeID string, writeMode pb.WriteMode, cache *NodeCache, rpcTimeout time.Duration) *ObjectFS {
	return NewObjectFSWithTimeouts(client, volumeID, writeMode, cache, rpcTimeout, DefaultSyncTimeout)
}

func NewObjectFSWithTimeouts(client pb.ObjectFSControllerClient, volumeID string, writeMode pb.WriteMode, cache *NodeCache, rpcTimeout, syncTimeout time.Duration) *ObjectFS {
	if cache == nil {
		cache = NewNodeCache(128 * 1024 * 1024)
	}
	if rpcTimeout <= 0 {
		rpcTimeout = DefaultRPCTimeout
	}
	if syncTimeout <= 0 {
		syncTimeout = DefaultSyncTimeout
	}
	fs := &ObjectFS{
		RawFileSystem: fuse.NewDefaultRawFileSystem(),
		client:        client,
		volumeID:      volumeID,
		writeMode:     writeMode,
		cache:         cache,
		rpcTimeout:    rpcTimeout,
		syncTimeout:   syncTimeout,
	}
	return fs
}

func (fs *ObjectFS) SetRPCTimeout(d time.Duration) {
	if d <= 0 {
		d = DefaultRPCTimeout
	}
	fs.rpcTimeout = d
}

func (fs *ObjectFS) SetSyncTimeout(d time.Duration) {
	if d <= 0 {
		d = DefaultSyncTimeout
	}
	fs.syncTimeout = d
}

func (fs *ObjectFS) String() string {
	return fmt.Sprintf("ObjectFS(%s)", fs.volumeID)
}

func (fs *ObjectFS) Init(server *fuse.Server) {
	fs.server = server
}

func (fs *ObjectFS) makeContext(cancel <-chan struct{}) (context.Context, context.CancelFunc) {
	return fs.makeContextWithTimeout(cancel, fs.rpcTimeout, DefaultRPCTimeout)
}

func (fs *ObjectFS) makeSyncContext(cancel <-chan struct{}) (context.Context, context.CancelFunc) {
	return fs.makeContextWithTimeout(cancel, fs.syncTimeout, DefaultSyncTimeout)
}

func (fs *ObjectFS) makeContextWithTimeout(cancel <-chan struct{}, timeout, defaultTimeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	ctx, cancelFunc := context.WithTimeout(context.Background(), timeout)
	if cancel != nil {
		go func() {
			select {
			case <-cancel:
				cancelFunc()
			case <-ctx.Done():
			}
		}()
	}
	return ctx, cancelFunc
}

func (fs *ObjectFS) logRPCError(op string, err error) {
	if err == nil {
		return
	}
	st, ok := status.FromError(err)
	if !ok {
		return
	}
	if st.Code() == codes.Unavailable || st.Code() == codes.DeadlineExceeded {
		fs.errMu.Lock()
		now := time.Now()
		if now.Sub(fs.lastLoggedErr) >= 1*time.Second {
			fs.lastLoggedErr = now
			fs.errMu.Unlock()
			klog.Warningf("ObjectFS(%s): controller unreachable in %s: %v", fs.volumeID, op, err)
			return
		}
		fs.errMu.Unlock()
	}
}

func grpcErrorToStatus(err error) fuse.Status {
	if err == nil {
		return fuse.OK
	}
	st, ok := status.FromError(err)
	if !ok {
		return fuse.Status(syscall.EIO)
	}
	switch st.Code() {
	case codes.NotFound:
		return fuse.ENOENT
	case codes.AlreadyExists:
		return fuse.Status(syscall.EEXIST)
	case codes.InvalidArgument, codes.FailedPrecondition:
		return fuse.EINVAL
	case codes.PermissionDenied, codes.Unauthenticated:
		return fuse.EACCES
	case codes.Unimplemented:
		return fuse.ENOSYS
	case codes.DeadlineExceeded, codes.Unavailable:
		return fuse.Status(syscall.EIO)
	case codes.Canceled:
		return fuse.Status(syscall.EINTR)
	case codes.ResourceExhausted:
		return fuse.Status(syscall.ENOSPC)
	case codes.Aborted:
		return fuse.Status(syscall.EBUSY)
	default:
		return fuse.Status(syscall.EIO)
	}
}

func (fs *ObjectFS) grpcErrorToStatus(op string, err error) fuse.Status {
	fs.logRPCError(op, err)
	return grpcErrorToStatus(err)
}

func (fs *ObjectFS) fillAttrOut(attr *pb.EntryAttr, out *fuse.Attr) {
	if attr == nil || attr.GetInode() == nil {
		return
	}
	inoRow := attr.GetInode()
	out.Ino = inoRow.GetIno()
	out.Size = uint64(inoRow.GetSize())
	out.Mode = inoRow.GetMode()
	if inoRow.GetIsDir() {
		out.Mode |= syscall.S_IFDIR
		if inoRow.GetNlink() > 0 {
			out.Nlink = inoRow.GetNlink()
		} else {
			out.Nlink = 2
		}
	} else {
		if (out.Mode & syscall.S_IFMT) == 0 {
			out.Mode |= syscall.S_IFREG
		}
		out.Nlink = inoRow.GetNlink()
	}
	out.Rdev = inoRow.GetRdev()
	if inoRow.GetMtime() != nil {
		t := inoRow.GetMtime().AsTime()
		out.Mtime = uint64(t.Unix())
		out.Mtimensec = uint32(t.Nanosecond())
		out.Atime = out.Mtime
		out.Atimensec = out.Mtimensec
		out.Ctime = out.Mtime
		out.Ctimensec = out.Mtimensec
	}
	if inoRow.GetAtime() != nil {
		t := inoRow.GetAtime().AsTime()
		out.Atime = uint64(t.Unix())
		out.Atimensec = uint32(t.Nanosecond())
	}
	if inoRow.GetCtime() != nil {
		t := inoRow.GetCtime().AsTime()
		out.Ctime = uint64(t.Unix())
		out.Ctimensec = uint32(t.Nanosecond())
	}
	out.Owner = fuse.Owner{
		Uid: inoRow.GetUid(),
		Gid: inoRow.GetGid(),
	}
}

func (fs *ObjectFS) fillEntryOut(attr *pb.EntryAttr, out *fuse.EntryOut) {
	fs.fillAttrOut(attr, &out.Attr)
	if attr != nil && attr.GetInode() != nil {
		out.NodeId = attr.GetInode().GetIno()
	}
	out.Generation = 1
	out.SetEntryTimeout(1 * time.Second)
	out.SetAttrTimeout(1 * time.Second)
}

func (fs *ObjectFS) Lookup(cancel <-chan struct{}, header *fuse.InHeader, name string, out *fuse.EntryOut) fuse.Status {
	if len(name) > MaxNameLength {
		return fuse.Status(syscall.ENAMETOOLONG)
	}
	ctx, cancelFunc := fs.makeContext(cancel)
	defer cancelFunc()

	resp, err := fs.client.Lookup(ctx, &pb.LookupRequest{
		VolumeId:    fs.volumeID,
		ParentInode: header.NodeId,
		Name:        name,
	})
	if err != nil {
		return fs.grpcErrorToStatus("Lookup", err)
	}
	if resp.GetError() != 0 {
		return fuse.Status(resp.GetError())
	}
	attr := resp.GetAttr()
	if attr == nil {
		return fuse.ENOENT
	}
	fs.fillEntryOut(attr, out)
	return fuse.OK
}

func (fs *ObjectFS) GetAttr(cancel <-chan struct{}, input *fuse.GetAttrIn, out *fuse.AttrOut) fuse.Status {
	ctx, cancelFunc := fs.makeContext(cancel)
	defer cancelFunc()

	resp, err := fs.client.GetAttr(ctx, &pb.GetAttrRequest{
		VolumeId: fs.volumeID,
		Inode:    input.NodeId,
	})
	if err != nil {
		return fs.grpcErrorToStatus("GetAttr", err)
	}
	if resp.GetError() != 0 {
		return fuse.Status(resp.GetError())
	}
	attr := resp.GetAttr()
	if attr == nil {
		return fuse.ENOENT
	}
	fs.fillAttrOut(attr, &out.Attr)

	if entry, isDirty := fs.cache.GetDirty(input.NodeId); isDirty {
		out.Attr.Size = uint64(entry.Size)
		out.Attr.Mtime = uint64(entry.ModTime.Unix())
		out.Attr.Mtimensec = uint32(entry.ModTime.Nanosecond())
	}
	out.SetTimeout(1 * time.Second)
	return fuse.OK
}

func (fs *ObjectFS) SetAttr(cancel <-chan struct{}, input *fuse.SetAttrIn, out *fuse.AttrOut) fuse.Status {
	ctx, cancelFunc := fs.makeContext(cancel)
	defer cancelFunc()

	var finalAttr *pb.EntryAttr

	if input.Valid&fuse.FATTR_SIZE != 0 {
		syncCtx, syncCancel := fs.makeSyncContext(cancel)
		if err := fs.syncFileToService(syncCtx, input.NodeId); err != nil {
			syncCancel()
			return fs.grpcErrorToStatus("Truncate:Sync", err)
		}
		syncCancel()
		truncGen := fs.cache.Truncate(input.NodeId, int64(input.Size), time.Now())
		resp, err := fs.client.TruncateFile(ctx, &pb.TruncateFileRequest{
			VolumeId: fs.volumeID,
			Inode:    input.NodeId,
			Size:     int64(input.Size),
		})
		if err != nil {
			return fs.grpcErrorToStatus("TruncateFile", err)
		}
		if resp.GetError() != 0 {
			return fuse.Status(resp.GetError())
		}
		fs.cache.MarkSizeClean(input.NodeId, truncGen)
		finalAttr = resp.GetAttr()
	}

	hasOtherAttrs := input.Valid&(fuse.FATTR_MODE|fuse.FATTR_UID|fuse.FATTR_GID|fuse.FATTR_ATIME|fuse.FATTR_ATIME_NOW|fuse.FATTR_MTIME|fuse.FATTR_MTIME_NOW|fuse.FATTR_CTIME) != 0

	if hasOtherAttrs {
		req := &pb.SetAttrRequest{
			VolumeId: fs.volumeID,
			Inode:    input.NodeId,
		}
		if input.Valid&fuse.FATTR_MODE != 0 {
			req.Mode = proto.Uint32(input.Mode)
		}
		if input.Valid&fuse.FATTR_UID != 0 {
			req.Uid = proto.Uint32(input.Uid)
		}
		if input.Valid&fuse.FATTR_GID != 0 {
			req.Gid = proto.Uint32(input.Gid)
		}
		if input.Valid&fuse.FATTR_ATIME_NOW != 0 {
			req.AtimeNow = true
		} else if input.Valid&fuse.FATTR_ATIME != 0 {
			req.Atime = timestamppb.New(time.Unix(int64(input.Atime), int64(input.Atimensec)))
		}
		if input.Valid&fuse.FATTR_MTIME_NOW != 0 {
			req.MtimeNow = true
		} else if input.Valid&fuse.FATTR_MTIME != 0 {
			req.Mtime = timestamppb.New(time.Unix(int64(input.Mtime), int64(input.Mtimensec)))
		}
		if input.Valid&fuse.FATTR_CTIME != 0 {
			req.Ctime = timestamppb.New(time.Unix(int64(input.Ctime), int64(input.Ctimensec)))
		}

		resp, err := fs.client.SetAttr(ctx, req)
		if err != nil {
			return fs.grpcErrorToStatus("SetAttr", err)
		}
		if resp.GetError() != 0 {
			return fuse.Status(resp.GetError())
		}
		finalAttr = resp.GetAttr()
	}

	if finalAttr == nil {
		resp, err := fs.client.GetAttr(ctx, &pb.GetAttrRequest{
			VolumeId: fs.volumeID,
			Inode:    input.NodeId,
		})
		if err != nil {
			return fs.grpcErrorToStatus("GetAttr", err)
		}
		if resp.GetError() != 0 {
			return fuse.Status(resp.GetError())
		}
		if resp.GetAttr() == nil {
			return fuse.ENOENT
		}
		finalAttr = resp.GetAttr()
	}

	fs.fillAttrOut(finalAttr, &out.Attr)
	if input.Valid&fuse.FATTR_SIZE != 0 {
		out.Attr.Size = input.Size
	} else if entry, isDirty := fs.cache.GetDirty(input.NodeId); isDirty {
		out.Attr.Size = uint64(entry.Size)
		out.Attr.Mtime = uint64(entry.ModTime.Unix())
		out.Attr.Mtimensec = uint32(entry.ModTime.Nanosecond())
	}
	out.SetTimeout(1 * time.Second)
	return fuse.OK
}

func (fs *ObjectFS) Mkdir(cancel <-chan struct{}, input *fuse.MkdirIn, name string, out *fuse.EntryOut) fuse.Status {
	if len(name) > MaxNameLength {
		return fuse.Status(syscall.ENAMETOOLONG)
	}
	ctx, cancelFunc := fs.makeContext(cancel)
	defer cancelFunc()

	resp, err := fs.client.Mkdir(ctx, &pb.MkdirRequest{
		VolumeId:    fs.volumeID,
		ParentInode: input.NodeId,
		Name:        name,
		Mode:        input.Mode,
		Uid:         input.Uid,
		Gid:         input.Gid,
	})
	if err != nil {
		return fs.grpcErrorToStatus("Mkdir", err)
	}
	if resp.GetError() != 0 {
		return fuse.Status(resp.GetError())
	}
	attr := resp.GetAttr()
	if attr == nil {
		return fuse.EIO
	}
	fs.fillEntryOut(attr, out)
	return fuse.OK
}

func (fs *ObjectFS) Create(cancel <-chan struct{}, input *fuse.CreateIn, name string, out *fuse.CreateOut) fuse.Status {
	if len(name) > MaxNameLength {
		return fuse.Status(syscall.ENAMETOOLONG)
	}
	ctx, cancelFunc := fs.makeContext(cancel)
	defer cancelFunc()

	resp, err := fs.client.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:    fs.volumeID,
		ParentInode: input.NodeId,
		Name:        name,
		Mode:        input.Mode,
		Uid:         input.Uid,
		Gid:         input.Gid,
	})
	if err != nil {
		return fs.grpcErrorToStatus("CreateFile", err)
	}
	if resp.GetError() != 0 {
		return fuse.Status(resp.GetError())
	}
	attr := resp.GetAttr()
	if attr == nil {
		return fuse.EIO
	}

	fs.cache.Put(attr.GetInode().GetIno(), []byte{}, time.Now(), "")
	fs.fillEntryOut(attr, &out.EntryOut)
	out.OpenOut.Fh = resp.GetFh()
	return fuse.OK
}

func (fs *ObjectFS) Mknod(cancel <-chan struct{}, input *fuse.MknodIn, name string, out *fuse.EntryOut) fuse.Status {
	if len(name) > MaxNameLength {
		return fuse.Status(syscall.ENAMETOOLONG)
	}
	ctx, cancelFunc := fs.makeContext(cancel)
	defer cancelFunc()

	resp, err := fs.client.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:    fs.volumeID,
		ParentInode: input.NodeId,
		Name:        name,
		Mode:        input.Mode,
		Rdev:        input.Rdev,
		Uid:         input.Uid,
		Gid:         input.Gid,
	})
	if err != nil {
		return fs.grpcErrorToStatus("CreateFile", err)
	}
	if resp.GetError() != 0 {
		return fuse.Status(resp.GetError())
	}
	attr := resp.GetAttr()
	if attr == nil {
		return fuse.EIO
	}
	fs.fillEntryOut(attr, out)
	return fuse.OK
}

func (fs *ObjectFS) Unlink(cancel <-chan struct{}, header *fuse.InHeader, name string) fuse.Status {
	if len(name) > MaxNameLength {
		return fuse.Status(syscall.ENAMETOOLONG)
	}
	ctx, cancelFunc := fs.makeContext(cancel)
	defer cancelFunc()

	resp, err := fs.client.Unlink(ctx, &pb.UnlinkRequest{
		VolumeId:    fs.volumeID,
		ParentInode: header.NodeId,
		Name:        name,
	})
	if err != nil {
		return fs.grpcErrorToStatus("Unlink", err)
	}
	if resp.GetError() != 0 {
		return fuse.Status(resp.GetError())
	}
	return fuse.OK
}

func (fs *ObjectFS) Rmdir(cancel <-chan struct{}, header *fuse.InHeader, name string) fuse.Status {
	if len(name) > MaxNameLength {
		return fuse.Status(syscall.ENAMETOOLONG)
	}
	ctx, cancelFunc := fs.makeContext(cancel)
	defer cancelFunc()

	resp, err := fs.client.Rmdir(ctx, &pb.RmdirRequest{
		VolumeId:    fs.volumeID,
		ParentInode: header.NodeId,
		Name:        name,
	})
	if err != nil {
		return fs.grpcErrorToStatus("Rmdir", err)
	}
	if resp.GetError() != 0 {
		return fuse.Status(resp.GetError())
	}
	return fuse.OK
}

func (fs *ObjectFS) Rename(cancel <-chan struct{}, input *fuse.RenameIn, oldName string, newName string) fuse.Status {
	if len(oldName) > MaxNameLength || len(newName) > MaxNameLength {
		return fuse.Status(syscall.ENAMETOOLONG)
	}
	ctx, cancelFunc := fs.makeContext(cancel)
	defer cancelFunc()

	resp, err := fs.client.Rename(ctx, &pb.RenameRequest{
		VolumeId:       fs.volumeID,
		OldParentInode: input.NodeId,
		OldName:        oldName,
		NewParentInode: input.Newdir,
		NewName:        newName,
	})
	if err != nil {
		return fs.grpcErrorToStatus("Rename", err)
	}
	if resp.GetError() != 0 {
		return fuse.Status(resp.GetError())
	}
	return fuse.OK
}

func (fs *ObjectFS) Open(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	ctx, cancelFunc := fs.makeContext(cancel)
	defer cancelFunc()

	resp, err := fs.client.Open(ctx, &pb.OpenRequest{
		VolumeId: fs.volumeID,
		Inode:    input.NodeId,
		Flags:    input.Flags,
	})
	if err != nil {
		return fs.grpcErrorToStatus("Open", err)
	}
	if resp.GetError() != 0 {
		return fuse.Status(resp.GetError())
	}
	out.Fh = resp.GetFh()
	return fuse.OK
}

func (fs *ObjectFS) OpenDir(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	out.Fh = input.NodeId
	return fuse.OK
}

func (fs *ObjectFS) ReadDir(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	ctx, cancelFunc := fs.makeContext(cancel)
	defer cancelFunc()

	resp, err := fs.client.ReadDir(ctx, &pb.ReadDirRequest{
		VolumeId: fs.volumeID,
		Inode:    input.NodeId,
	})
	if err != nil {
		return fs.grpcErrorToStatus("ReadDir", err)
	}
	if resp.GetError() != 0 {
		return fuse.Status(resp.GetError())
	}

	entries := resp.GetEntries()
	for i := int(input.Offset); i < len(entries); i++ {
		entry := entries[i]
		inoRow := entry.GetInode()
		mode := inoRow.GetMode()
		if inoRow.GetIsDir() {
			mode = (mode & ^uint32(syscall.S_IFMT)) | syscall.S_IFDIR
		} else if (mode & syscall.S_IFMT) == 0 {
			mode |= syscall.S_IFREG
		}
		if !out.AddDirEntry(fuse.DirEntry{
			Mode: mode,
			Name: entry.GetName(),
			Ino:  inoRow.GetIno(),
			Off:  uint64(i + 1),
		}) {
			break
		}
	}
	return fuse.OK
}

func (fs *ObjectFS) ReadDirPlus(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	ctx, cancelFunc := fs.makeContext(cancel)
	defer cancelFunc()

	resp, err := fs.client.ReadDir(ctx, &pb.ReadDirRequest{
		VolumeId: fs.volumeID,
		Inode:    input.NodeId,
	})
	if err != nil {
		return fs.grpcErrorToStatus("ReadDir", err)
	}
	if resp.GetError() != 0 {
		return fuse.Status(resp.GetError())
	}

	entries := resp.GetEntries()
	for i := int(input.Offset); i < len(entries); i++ {
		entry := entries[i]
		inoRow := entry.GetInode()
		mode := inoRow.GetMode()
		if inoRow.GetIsDir() {
			mode = (mode & ^uint32(syscall.S_IFMT)) | syscall.S_IFDIR
		} else if (mode & syscall.S_IFMT) == 0 {
			mode |= syscall.S_IFREG
		}
		entryOut := out.AddDirLookupEntry(fuse.DirEntry{
			Mode: mode,
			Name: entry.GetName(),
			Ino:  inoRow.GetIno(),
			Off:  uint64(i + 1),
		})
		if entryOut == nil {
			break
		}
		fs.fillEntryOut(entry, entryOut)
	}
	return fuse.OK
}

func (fs *ObjectFS) Symlink(cancel <-chan struct{}, header *fuse.InHeader, pointedTo string, linkName string, out *fuse.EntryOut) fuse.Status {
	if len(linkName) > MaxNameLength {
		return fuse.Status(syscall.ENAMETOOLONG)
	}
	ctx, cancelFunc := fs.makeContext(cancel)
	defer cancelFunc()

	resp, err := fs.client.Symlink(ctx, &pb.SymlinkRequest{
		VolumeId:    fs.volumeID,
		ParentInode: header.NodeId,
		Name:        linkName,
		Target:      pointedTo,
		Uid:         header.Uid,
		Gid:         header.Gid,
	})
	if err != nil {
		return fs.grpcErrorToStatus("Symlink", err)
	}
	if resp.GetError() != 0 {
		return fuse.Status(resp.GetError())
	}
	attr := resp.GetAttr()
	if attr == nil {
		return fuse.EIO
	}
	fs.fillEntryOut(attr, out)
	return fuse.OK
}

func (fs *ObjectFS) Readlink(cancel <-chan struct{}, header *fuse.InHeader) ([]byte, fuse.Status) {
	ctx, cancelFunc := fs.makeContext(cancel)
	defer cancelFunc()

	resp, err := fs.client.Readlink(ctx, &pb.ReadlinkRequest{
		VolumeId: fs.volumeID,
		Inode:    header.NodeId,
	})
	if err != nil {
		return nil, fs.grpcErrorToStatus("Readlink", err)
	}
	if resp.GetError() != 0 {
		return nil, fuse.Status(resp.GetError())
	}
	return []byte(resp.GetTarget()), fuse.OK
}

func (fs *ObjectFS) Link(cancel <-chan struct{}, input *fuse.LinkIn, filename string, out *fuse.EntryOut) fuse.Status {
	if len(filename) > MaxNameLength {
		return fuse.Status(syscall.ENAMETOOLONG)
	}
	ctx, cancelFunc := fs.makeContext(cancel)
	defer cancelFunc()

	resp, err := fs.client.Link(ctx, &pb.LinkRequest{
		VolumeId:       fs.volumeID,
		OldInode:       input.Oldnodeid,
		NewParentInode: input.NodeId,
		NewName:        filename,
	})
	if err != nil {
		return fs.grpcErrorToStatus("Link", err)
	}
	if resp.GetError() != 0 {
		return fuse.Status(resp.GetError())
	}
	attr := resp.GetAttr()
	if attr == nil {
		return fuse.EIO
	}
	fs.fillEntryOut(attr, out)
	return fuse.OK
}

func (fs *ObjectFS) Read(cancel <-chan struct{}, input *fuse.ReadIn, buf []byte) (fuse.ReadResult, fuse.Status) {
	offset := int64(input.Offset)
	size := int64(input.Size)

	if size <= 0 {
		return fuse.ReadResultData([]byte{}), fuse.OK
	}

	if cachedBytes, ok := fs.cache.GetRange(input.NodeId, offset, size); ok {
		return fuse.ReadResultData(cachedBytes), fuse.OK
	}

	ctx, cancelFunc := fs.makeContext(cancel)
	defer cancelFunc()

	cs := int64(DefaultChunkSize)
	startChunk := int(offset / cs)
	endChunk := int((offset + size - 1) / cs)

	clientSize, hasClientEntry := fs.cache.GetSize(input.NodeId)
	var totalSize int64
	if hasClientEntry {
		totalSize = clientSize
	}

	var result []byte
	for i := startChunk; i <= endChunk; i++ {
		chunkStart := int64(i) * cs
		rStart := offset - chunkStart
		if rStart < 0 {
			rStart = 0
		}
		rEnd := offset + size - chunkStart
		if rEnd > cs {
			rEnd = cs
		}

		// 1. Check if chunk is in cache
		if cachedChunk, ok := fs.cache.GetChunk(input.NodeId, i); ok {
			chunkLen := int64(len(cachedChunk))
			if hasClientEntry && totalSize > 0 {
				expectedLen := cs
				if int64(i+1)*cs > totalSize {
					expectedLen = totalSize - chunkStart
				}
				if expectedLen < 0 {
					expectedLen = 0
				}
				if chunkLen < expectedLen {
					padded := make([]byte, expectedLen)
					copy(padded, cachedChunk)
					cachedChunk = padded
					chunkLen = expectedLen
				}
			}
			if rStart < chunkLen {
				end := rEnd
				if end > chunkLen {
					end = chunkLen
				}
				result = append(result, cachedChunk[rStart:end]...)
			}
			continue
		}

		// 2. Not in cache: fetch chunk from controller
		readGen := fs.cache.GetGeneration(input.NodeId)
		resp, err := fs.client.ReadFile(ctx, &pb.ReadFileRequest{
			VolumeId: fs.volumeID,
			Inode:    input.NodeId,
			Offset:   chunkStart,
			Size:     cs,
		})
		if err != nil {
			return nil, fs.grpcErrorToStatus("ReadFile", err)
		}
		if resp.GetError() != 0 {
			return nil, fuse.Status(resp.GetError())
		}

		if resp.GetTotalSize() > totalSize {
			totalSize = resp.GetTotalSize()
		}

		fetchedData := resp.GetData()
		// Cache full clean chunk if complete
		if (int64(len(fetchedData)) == cs || resp.GetEof()) && len(fetchedData) > 0 {
			fs.cache.PutChunk(input.NodeId, i, uint32(cs), resp.GetTotalSize(), fetchedData, time.Now(), readGen)
		}

		// If chunk has fewer bytes than expected by totalSize (e.g. hole or unflushed write), zero pad
		expectedChunkLen := cs
		if int64(i+1)*cs > totalSize {
			expectedChunkLen = totalSize - chunkStart
		}
		if expectedChunkLen < 0 {
			expectedChunkLen = 0
		}
		if int64(len(fetchedData)) < expectedChunkLen {
			padded := make([]byte, expectedChunkLen)
			copy(padded, fetchedData)
			fetchedData = padded
		}

		fetchedLen := int64(len(fetchedData))
		if rStart < fetchedLen {
			end := rEnd
			if end > fetchedLen {
				end = fetchedLen
			}
			result = append(result, fetchedData[rStart:end]...)
		}
	}

	return fuse.ReadResultData(result), fuse.OK
}

func (fs *ObjectFS) Write(cancel <-chan struct{}, input *fuse.WriteIn, data []byte) (uint32, fuse.Status) {
	// If any chunk touched by write is not fully overwritten and not in cache, load existing chunk content into cache first
	cs := int64(DefaultChunkSize)
	startChunk := int64(input.Offset) / cs
	endChunk := (int64(input.Offset) + int64(len(data)) - 1) / cs

	for i := startChunk; i <= endChunk; i++ {
		chunkStart := i * cs
		chunkEnd := chunkStart + cs
		wStart := int64(input.Offset)
		if wStart < chunkStart {
			wStart = chunkStart
		}
		wEnd := int64(input.Offset) + int64(len(data))
		if wEnd > chunkEnd {
			wEnd = chunkEnd
		}

		if wStart > chunkStart || wEnd < chunkEnd {
			clientSize, hasSize := fs.cache.GetSize(input.NodeId)
			if hasSize && chunkStart >= clientSize {
				continue
			}
			if _, ok := fs.cache.GetChunk(input.NodeId, int(i)); !ok {
				ctx, cancelFunc := fs.makeSyncContext(cancel)
				readGen := fs.cache.GetGeneration(input.NodeId)
				resp, err := fs.client.ReadFile(ctx, &pb.ReadFileRequest{
					VolumeId: fs.volumeID,
					Inode:    input.NodeId,
					Offset:   chunkStart,
					Size:     cs,
				})
				cancelFunc()
				if err != nil {
					fs.logRPCError("Write:ReadFile", err)
				} else if resp.GetError() == 0 && len(resp.GetData()) > 0 {
					fs.cache.PutChunk(input.NodeId, int(i), DefaultChunkSize, resp.GetTotalSize(), resp.GetData(), time.Now(), readGen)
				}
			}
		}
	}

	// Buffer write locally and mark dirty per chunk
	fs.cache.WriteAt(input.NodeId, int64(input.Offset), data, time.Now())
	return uint32(len(data)), fuse.OK
}

// MaxSyncRunChunks is the maximum number of contiguous chunks coalesced into a single WriteFile RPC.
const MaxSyncRunChunks = 32

// MaxSyncRunBytes is the maximum total payload size for a coalesced WriteFile RPC,
// capped at 2 MiB to remain comfortably within the default 4 MiB gRPC receive limit.
const MaxSyncRunBytes = 2 * 1024 * 1024

func (fs *ObjectFS) syncFileToService(ctx context.Context, inode uint64) error {
	dirtyChunks, chunkGens, chunkSize, _, isDirty := fs.cache.GetDirtyChunks(inode)
	if !isDirty || len(dirtyChunks) == 0 {
		return nil
	}
	if chunkSize == 0 {
		chunkSize = DefaultChunkSize
	}

	indices := make([]int, 0, len(dirtyChunks))
	for idx := range dirtyChunks {
		indices = append(indices, idx)
	}
	sort.Ints(indices)

	// Coalesce contiguous chunk indices into runs capped at MaxSyncRunChunks (32 chunks)
	// and MaxSyncRunBytes (2 MiB).
	var runs [][]int
	var currentRun []int
	currentRunBytes := 0

	for _, idx := range indices {
		chunkData := dirtyChunks[idx]
		if len(currentRun) > 0 {
			prevIdx := currentRun[len(currentRun)-1]
			prevData := dirtyChunks[prevIdx]
			// A chunk can continue currentRun only if:
			// 1. It is contiguous with the previous chunk index.
			// 2. The previous chunk has full chunkSize length (otherwise concatenating would misalign byte offsets).
			// 3. Adding this chunk does not exceed MaxSyncRunChunks.
			// 4. Adding this chunk does not exceed MaxSyncRunBytes.
			if idx != prevIdx+1 || len(prevData) != int(chunkSize) || len(currentRun) >= MaxSyncRunChunks || currentRunBytes+len(chunkData) > MaxSyncRunBytes {
				runs = append(runs, currentRun)
				currentRun = nil
				currentRunBytes = 0
			}
		}
		currentRun = append(currentRun, idx)
		currentRunBytes += len(chunkData)
	}
	if len(currentRun) > 0 {
		runs = append(runs, currentRun)
	}

	for _, run := range runs {
		startIdx := run[0]
		chunkOffset := int64(startIdx) * int64(chunkSize)

		var data []byte
		if len(run) == 1 {
			data = dirtyChunks[startIdx]
		} else {
			runLen := 0
			for _, idx := range run {
				runLen += len(dirtyChunks[idx])
			}
			data = make([]byte, 0, runLen)
			for _, idx := range run {
				data = append(data, dirtyChunks[idx]...)
			}
		}

		resp, err := fs.client.WriteFile(ctx, &pb.WriteFileRequest{
			VolumeId:  fs.volumeID,
			Inode:     inode,
			Offset:    chunkOffset,
			Data:      data,
			WriteMode: fs.writeMode,
		})
		if err != nil {
			fs.logRPCError("WriteFile", err)
			return err
		}
		if resp.GetError() != 0 {
			return syscall.Errno(resp.GetError())
		}
		for _, idx := range run {
			fs.cache.MarkChunkClean(inode, idx, chunkGens[idx])
		}
	}

	return nil
}

func (fs *ObjectFS) Flush(cancel <-chan struct{}, input *fuse.FlushIn) fuse.Status {
	ctx, cancelFunc := fs.makeSyncContext(cancel)
	defer cancelFunc()

	if err := fs.syncFileToService(ctx, input.NodeId); err != nil {
		return fs.grpcErrorToStatus("Flush", err)
	}
	return fuse.OK
}

func (fs *ObjectFS) Fsync(cancel <-chan struct{}, input *fuse.FsyncIn) fuse.Status {
	ctx, cancelFunc := fs.makeSyncContext(cancel)
	defer cancelFunc()

	if err := fs.syncFileToService(ctx, input.NodeId); err != nil {
		return fs.grpcErrorToStatus("Fsync", err)
	}

	resp, err := fs.client.Fsync(ctx, &pb.FsyncRequest{
		VolumeId: fs.volumeID,
		Inode:    input.NodeId,
	})
	if err != nil {
		return fs.grpcErrorToStatus("Fsync", err)
	}
	if resp.GetError() != 0 {
		return fuse.Status(resp.GetError())
	}
	return fuse.OK
}

func (fs *ObjectFS) Release(cancel <-chan struct{}, input *fuse.ReleaseIn) {
	ctx, cancelFunc := fs.makeContext(cancel)
	defer cancelFunc()

	resp, err := fs.client.Release(ctx, &pb.ReleaseRequest{
		VolumeId: fs.volumeID,
		Inode:    input.NodeId,
		Fh:       input.Fh,
	})
	if err != nil {
		fs.logRPCError("Release", err)
		klog.Warningf("Release RPC failed for volume %s, inode %d, fh %d: %v", fs.volumeID, input.NodeId, input.Fh, err)
		return
	}
	if resp.GetError() != 0 {
		klog.Warningf("Release returned error %d for volume %s, inode %d, fh %d", resp.GetError(), fs.volumeID, input.NodeId, input.Fh)
	}
}

func (fs *ObjectFS) StatFs(cancel <-chan struct{}, input *fuse.InHeader, out *fuse.StatfsOut) fuse.Status {
	ctx, cancelFunc := fs.makeContext(cancel)
	defer cancelFunc()

	blockSize := uint64(4096)
	nominalTotalBytes := uint64(4 * 1024 * 1024 * 1024 * 1024)
	nominalTotalInodes := uint64(10000000)

	totalBlocks := nominalTotalBytes / blockSize
	usedBytes := uint64(0)
	usedInodes := uint64(0)

	if fs.client != nil {
		resp, err := fs.client.GetVolumeStats(ctx, &pb.GetVolumeStatsRequest{VolumeId: fs.volumeID})
		if err != nil {
			fs.logRPCError("GetVolumeStats", err)
		} else if resp.GetError() == 0 {
			if resp.GetTotalBytes() > 0 {
				nominalTotalBytes = uint64(resp.GetTotalBytes())
				totalBlocks = nominalTotalBytes / blockSize
			}
			if resp.GetUsedBytes() > 0 {
				usedBytes = uint64(resp.GetUsedBytes())
			}
			if resp.GetTotalInodes() > 0 {
				nominalTotalInodes = uint64(resp.GetTotalInodes())
			}
			if resp.GetUsedInodes() > 0 {
				usedInodes = uint64(resp.GetUsedInodes())
			}
		}
	}

	usedBlocks := (usedBytes + blockSize - 1) / blockSize
	freeBlocks := uint64(0)
	if totalBlocks > usedBlocks {
		freeBlocks = totalBlocks - usedBlocks
	}
	freeInodes := uint64(0)
	if nominalTotalInodes > usedInodes {
		freeInodes = nominalTotalInodes - usedInodes
	}

	out.Blocks = totalBlocks
	out.Bfree = freeBlocks
	out.Bavail = freeBlocks
	out.Bsize = uint32(blockSize)
	out.Frsize = uint32(blockSize)
	out.Files = nominalTotalInodes
	out.Ffree = freeInodes
	out.NameLen = MaxNameLength
	return fuse.OK
}

func (fs *ObjectFS) Access(cancel <-chan struct{}, input *fuse.AccessIn) fuse.Status {
	return fuse.OK
}

func StartWatcher(ctx context.Context, client pb.ObjectFSControllerClient, volumeID, nodeID string, cache *NodeCache) {
	go func() {
		backoff := 100 * time.Millisecond
		maxBackoff := 5 * time.Second
		hasDisconnected := false

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			stream, err := client.WatchVolume(ctx, &pb.WatchVolumeRequest{
				VolumeId: volumeID,
				NodeId:   nodeID,
			})
			if err != nil {
				hasDisconnected = true
				select {
				case <-ctx.Done():
					return
				case <-time.After(backoff):
				}
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
				continue
			}

			if hasDisconnected {
				// Invalidate all clean entries that might have changed on controller while disconnected
				cache.InvalidateAllClean()
			}
			backoff = 100 * time.Millisecond

			for {
				ev, err := stream.Recv()
				if err != nil {
					hasDisconnected = true
					break
				}
				if ev.GetInode() != 0 {
					cache.InvalidateIfNotDirty(ev.GetInode())
				}
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
		}
	}()
}
