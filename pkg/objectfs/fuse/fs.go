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
	"path"
	"strings"
	"sync"
	"syscall"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/hanwen/go-fuse/v2/fuse"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ObjectFS struct {
	fuse.RawFileSystem

	client    pb.ObjectFSControllerClient
	volumeID  string
	writeMode pb.WriteMode
	cache     *NodeCache

	mu          sync.RWMutex
	inodeToPath map[uint64]string
	pathToInode map[string]uint64
	server      *fuse.Server
}

var _ fuse.RawFileSystem = (*ObjectFS)(nil)

func NewObjectFS(client pb.ObjectFSControllerClient, volumeID string, writeMode pb.WriteMode, cache *NodeCache) *ObjectFS {
	if cache == nil {
		cache = NewNodeCache(128 * 1024 * 1024)
	}
	fs := &ObjectFS{
		RawFileSystem: fuse.NewDefaultRawFileSystem(),
		client:        client,
		volumeID:      volumeID,
		writeMode:     writeMode,
		cache:         cache,
		inodeToPath:   make(map[uint64]string),
		pathToInode:   make(map[string]uint64),
	}
	fs.inodeToPath[fuse.FUSE_ROOT_ID] = "/"
	fs.pathToInode["/"] = fuse.FUSE_ROOT_ID
	return fs
}

func (fs *ObjectFS) String() string {
	return fmt.Sprintf("ObjectFS(%s)", fs.volumeID)
}

func (fs *ObjectFS) Init(server *fuse.Server) {
	fs.server = server
}

func (fs *ObjectFS) getPath(inode uint64) (string, bool) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	p, ok := fs.inodeToPath[inode]
	return p, ok
}

func (fs *ObjectFS) setInode(inode uint64, p string) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	clean := path.Clean("/" + p)
	fs.inodeToPath[inode] = clean
	fs.pathToInode[clean] = inode
}

func (fs *ObjectFS) removePath(p string) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	clean := path.Clean("/" + p)
	if inode, ok := fs.pathToInode[clean]; ok {
		delete(fs.inodeToPath, inode)
		delete(fs.pathToInode, clean)
	}
}

func (fs *ObjectFS) renamePath(oldPath, newPath string) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	oldClean := path.Clean("/" + oldPath)
	newClean := path.Clean("/" + newPath)
	if inode, ok := fs.pathToInode[oldClean]; ok {
		delete(fs.pathToInode, oldClean)
		fs.inodeToPath[inode] = newClean
		fs.pathToInode[newClean] = inode
	}
}

func makeContext(cancel <-chan struct{}) (context.Context, context.CancelFunc) {
	ctx, cancelFunc := context.WithCancel(context.Background())
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

func grpcErrorToStatus(err error) fuse.Status {
	if err == nil {
		return fuse.OK
	}
	st, ok := status.FromError(err)
	if !ok {
		return fuse.Status(syscall.EIO)
	}
	msg := strings.ToLower(st.Message())
	switch st.Code() {
	case codes.NotFound:
		return fuse.ENOENT
	case codes.AlreadyExists:
		return fuse.Status(syscall.EEXIST)
	case codes.InvalidArgument:
		if strings.Contains(msg, "is a directory") {
			return fuse.Status(syscall.EISDIR)
		}
		if strings.Contains(msg, "not a directory") {
			return fuse.Status(syscall.ENOTDIR)
		}
		return fuse.EINVAL
	case codes.PermissionDenied, codes.Unauthenticated:
		return fuse.EACCES
	case codes.Unimplemented:
		return fuse.ENOSYS
	case codes.DeadlineExceeded:
		return fuse.Status(syscall.ETIMEDOUT)
	case codes.Canceled:
		return fuse.Status(syscall.EINTR)
	case codes.ResourceExhausted:
		return fuse.Status(syscall.ENOSPC)
	case codes.Aborted:
		return fuse.Status(syscall.EBUSY)
	case codes.FailedPrecondition:
		if strings.Contains(msg, "not empty") {
			return fuse.Status(syscall.ENOTEMPTY)
		}
		if strings.Contains(msg, "is a directory") {
			return fuse.Status(syscall.EISDIR)
		}
		if strings.Contains(msg, "not a directory") {
			return fuse.Status(syscall.ENOTDIR)
		}
		if strings.Contains(msg, "busy") {
			return fuse.Status(syscall.EBUSY)
		}
		if strings.Contains(msg, "already exists") || strings.Contains(msg, "file exists") {
			return fuse.Status(syscall.EEXIST)
		}
		if strings.Contains(msg, "not found") || strings.Contains(msg, "no such file") {
			return fuse.ENOENT
		}
		return fuse.EINVAL
	default:
		if strings.Contains(msg, "not empty") {
			return fuse.Status(syscall.ENOTEMPTY)
		}
		if strings.Contains(msg, "is a directory") {
			return fuse.Status(syscall.EISDIR)
		}
		if strings.Contains(msg, "not a directory") {
			return fuse.Status(syscall.ENOTDIR)
		}
		if strings.Contains(msg, "busy") {
			return fuse.Status(syscall.EBUSY)
		}
		return fuse.Status(syscall.EIO)
	}
}

func fillAttr(attr *pb.EntryAttr, out *fuse.Attr) {
	out.Ino = attr.GetInode()
	out.Size = uint64(attr.GetSize())
	out.Mode = attr.GetMode()
	if attr.GetIsDir() {
		out.Mode |= syscall.S_IFDIR
	} else {
		out.Mode |= syscall.S_IFREG
	}
	if attr.GetModTime() != nil {
		t := attr.GetModTime().AsTime()
		out.Mtime = uint64(t.Unix())
		out.Mtimensec = uint32(t.Nanosecond())
		out.Ctime = out.Mtime
		out.Ctimensec = out.Mtimensec
		out.Atime = out.Mtime
		out.Atimensec = out.Mtimensec
	}
}

func fillEntryOut(attr *pb.EntryAttr, out *fuse.EntryOut) {
	fillAttr(attr, &out.Attr)
	out.NodeId = attr.GetInode()
	out.Generation = 1
	out.SetEntryTimeout(1 * time.Second)
	out.SetAttrTimeout(1 * time.Second)
}

func (fs *ObjectFS) Lookup(cancel <-chan struct{}, header *fuse.InHeader, name string, out *fuse.EntryOut) fuse.Status {
	ctx, cancelFunc := makeContext(cancel)
	defer cancelFunc()

	parentPath, ok := fs.getPath(header.NodeId)
	if !ok {
		return fuse.ENOENT
	}

	resp, err := fs.client.Lookup(ctx, &pb.LookupRequest{
		VolumeId:   fs.volumeID,
		ParentPath: parentPath,
		Name:       name,
	})
	if err != nil {
		return grpcErrorToStatus(err)
	}

	attr := resp.GetAttr()
	childPath := path.Join(parentPath, name)
	fs.setInode(attr.GetInode(), childPath)

	fillEntryOut(attr, out)
	return fuse.OK
}

func (fs *ObjectFS) GetAttr(cancel <-chan struct{}, input *fuse.GetAttrIn, out *fuse.AttrOut) fuse.Status {
	ctx, cancelFunc := makeContext(cancel)
	defer cancelFunc()

	p, ok := fs.getPath(input.NodeId)
	if !ok {
		return fuse.ENOENT
	}

	resp, err := fs.client.GetAttr(ctx, &pb.GetAttrRequest{
		VolumeId: fs.volumeID,
		Path:     p,
	})
	if err != nil {
		return grpcErrorToStatus(err)
	}

	fillAttr(resp.GetAttr(), &out.Attr)
	if entry, isDirty := fs.cache.GetDirty(p); isDirty {
		out.Attr.Size = uint64(entry.Size)
		out.Attr.Mtime = uint64(entry.ModTime.Unix())
		out.Attr.Mtimensec = uint32(entry.ModTime.Nanosecond())
	}
	out.SetTimeout(1 * time.Second)
	return fuse.OK
}

func (fs *ObjectFS) SetAttr(cancel <-chan struct{}, input *fuse.SetAttrIn, out *fuse.AttrOut) fuse.Status {
	ctx, cancelFunc := makeContext(cancel)
	defer cancelFunc()

	p, ok := fs.getPath(input.NodeId)
	if !ok {
		return fuse.ENOENT
	}

	if input.Valid&fuse.FATTR_SIZE != 0 {
		fs.cache.Truncate(p, int64(input.Size), time.Now())
		resp, err := fs.client.TruncateFile(ctx, &pb.TruncateFileRequest{
			VolumeId: fs.volumeID,
			Path:     p,
			Size:     int64(input.Size),
		})
		if err != nil {
			return grpcErrorToStatus(err)
		}
		fillAttr(resp.GetAttr(), &out.Attr)
		out.Attr.Size = input.Size
		out.SetTimeout(1 * time.Second)
		return fuse.OK
	}

	resp, err := fs.client.GetAttr(ctx, &pb.GetAttrRequest{
		VolumeId: fs.volumeID,
		Path:     p,
	})
	if err != nil {
		return grpcErrorToStatus(err)
	}
	fillAttr(resp.GetAttr(), &out.Attr)
	if entry, isDirty := fs.cache.GetDirty(p); isDirty {
		out.Attr.Size = uint64(entry.Size)
		out.Attr.Mtime = uint64(entry.ModTime.Unix())
		out.Attr.Mtimensec = uint32(entry.ModTime.Nanosecond())
	}
	out.SetTimeout(1 * time.Second)
	return fuse.OK
}

func (fs *ObjectFS) Mkdir(cancel <-chan struct{}, input *fuse.MkdirIn, name string, out *fuse.EntryOut) fuse.Status {
	ctx, cancelFunc := makeContext(cancel)
	defer cancelFunc()

	parentPath, ok := fs.getPath(input.NodeId)
	if !ok {
		return fuse.ENOENT
	}

	childPath := path.Join(parentPath, name)
	resp, err := fs.client.Mkdir(ctx, &pb.MkdirRequest{
		VolumeId: fs.volumeID,
		Path:     childPath,
		Mode:     input.Mode,
	})
	if err != nil {
		return grpcErrorToStatus(err)
	}

	attr := resp.GetAttr()
	fs.setInode(attr.GetInode(), childPath)
	fillEntryOut(attr, out)
	return fuse.OK
}

func (fs *ObjectFS) Create(cancel <-chan struct{}, input *fuse.CreateIn, name string, out *fuse.CreateOut) fuse.Status {
	ctx, cancelFunc := makeContext(cancel)
	defer cancelFunc()

	parentPath, ok := fs.getPath(input.NodeId)
	if !ok {
		return fuse.ENOENT
	}

	childPath := path.Join(parentPath, name)
	resp, err := fs.client.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId: fs.volumeID,
		Path:     childPath,
		Mode:     input.Mode,
	})
	if err != nil {
		return grpcErrorToStatus(err)
	}

	attr := resp.GetAttr()
	fs.setInode(attr.GetInode(), childPath)
	fs.cache.Put(childPath, []byte{}, time.Now(), "")
	fillEntryOut(attr, &out.EntryOut)
	out.OpenOut.Fh = attr.GetInode()
	return fuse.OK
}

func (fs *ObjectFS) Mknod(cancel <-chan struct{}, input *fuse.MknodIn, name string, out *fuse.EntryOut) fuse.Status {
	ctx, cancelFunc := makeContext(cancel)
	defer cancelFunc()

	parentPath, ok := fs.getPath(input.NodeId)
	if !ok {
		return fuse.ENOENT
	}

	childPath := path.Join(parentPath, name)
	resp, err := fs.client.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId: fs.volumeID,
		Path:     childPath,
		Mode:     input.Mode,
	})
	if err != nil {
		return grpcErrorToStatus(err)
	}

	attr := resp.GetAttr()
	fs.setInode(attr.GetInode(), childPath)
	fs.cache.Put(childPath, []byte{}, time.Now(), "")
	fillEntryOut(attr, out)
	return fuse.OK
}

func (fs *ObjectFS) Unlink(cancel <-chan struct{}, header *fuse.InHeader, name string) fuse.Status {
	ctx, cancelFunc := makeContext(cancel)
	defer cancelFunc()

	parentPath, ok := fs.getPath(header.NodeId)
	if !ok {
		return fuse.ENOENT
	}

	childPath := path.Join(parentPath, name)
	_, err := fs.client.Unlink(ctx, &pb.UnlinkRequest{
		VolumeId: fs.volumeID,
		Path:     childPath,
	})
	if err != nil {
		return grpcErrorToStatus(err)
	}

	fs.cache.Invalidate(childPath)
	fs.removePath(childPath)
	return fuse.OK
}

func (fs *ObjectFS) Rmdir(cancel <-chan struct{}, header *fuse.InHeader, name string) fuse.Status {
	ctx, cancelFunc := makeContext(cancel)
	defer cancelFunc()

	parentPath, ok := fs.getPath(header.NodeId)
	if !ok {
		return fuse.ENOENT
	}

	childPath := path.Join(parentPath, name)
	_, err := fs.client.Rmdir(ctx, &pb.RmdirRequest{
		VolumeId: fs.volumeID,
		Path:     childPath,
	})
	if err != nil {
		return grpcErrorToStatus(err)
	}

	fs.removePath(childPath)
	return fuse.OK
}

func (fs *ObjectFS) Rename(cancel <-chan struct{}, input *fuse.RenameIn, oldName string, newName string) fuse.Status {
	ctx, cancelFunc := makeContext(cancel)
	defer cancelFunc()

	oldParent, ok1 := fs.getPath(input.NodeId)
	newParent, ok2 := fs.getPath(input.Newdir)
	if !ok1 || !ok2 {
		return fuse.ENOENT
	}

	oldPath := path.Join(oldParent, oldName)
	newPath := path.Join(newParent, newName)

	if err := fs.syncFileToService(ctx, oldPath); err != nil {
		return grpcErrorToStatus(err)
	}

	_, err := fs.client.Rename(ctx, &pb.RenameRequest{
		VolumeId: fs.volumeID,
		OldPath:  oldPath,
		NewPath:  newPath,
	})
	if err != nil {
		return grpcErrorToStatus(err)
	}

	fs.cache.Invalidate(oldPath)
	fs.cache.Invalidate(newPath)
	fs.renamePath(oldPath, newPath)
	return fuse.OK
}

func (fs *ObjectFS) Open(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	out.Fh = input.NodeId
	return fuse.OK
}

func (fs *ObjectFS) OpenDir(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	out.Fh = input.NodeId
	return fuse.OK
}

func (fs *ObjectFS) ReadDir(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	ctx, cancelFunc := makeContext(cancel)
	defer cancelFunc()

	dirPath, ok := fs.getPath(input.NodeId)
	if !ok {
		return fuse.ENOENT
	}

	resp, err := fs.client.ReadDir(ctx, &pb.ReadDirRequest{
		VolumeId: fs.volumeID,
		Path:     dirPath,
	})
	if err != nil {
		return grpcErrorToStatus(err)
	}

	entries := resp.GetEntries()
	for i := int(input.Offset); i < len(entries); i++ {
		e := entries[i]
		mode := uint32(syscall.S_IFREG)
		if e.GetIsDir() {
			mode = uint32(syscall.S_IFDIR)
		}
		fs.setInode(e.GetInode(), path.Join(dirPath, e.GetName()))
		ok := out.AddDirEntry(fuse.DirEntry{
			Mode: mode,
			Name: e.GetName(),
			Ino:  e.GetInode(),
			Off:  uint64(i + 1),
		})
		if !ok {
			break
		}
	}
	return fuse.OK
}

func (fs *ObjectFS) ReadDirPlus(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	ctx, cancelFunc := makeContext(cancel)
	defer cancelFunc()

	dirPath, ok := fs.getPath(input.NodeId)
	if !ok {
		return fuse.ENOENT
	}

	resp, err := fs.client.ReadDir(ctx, &pb.ReadDirRequest{
		VolumeId: fs.volumeID,
		Path:     dirPath,
	})
	if err != nil {
		return grpcErrorToStatus(err)
	}

	entries := resp.GetEntries()
	for i := int(input.Offset); i < len(entries); i++ {
		e := entries[i]
		mode := uint32(syscall.S_IFREG)
		if e.GetIsDir() {
			mode = uint32(syscall.S_IFDIR)
		}
		fs.setInode(e.GetInode(), path.Join(dirPath, e.GetName()))
		entryOut := out.AddDirLookupEntry(fuse.DirEntry{
			Mode: mode,
			Name: e.GetName(),
			Ino:  e.GetInode(),
			Off:  uint64(i + 1),
		})
		if entryOut == nil {
			break
		}
		fillEntryOut(e, entryOut)
	}
	return fuse.OK
}

func (fs *ObjectFS) Read(cancel <-chan struct{}, input *fuse.ReadIn, buf []byte) (fuse.ReadResult, fuse.Status) {
	p, ok := fs.getPath(input.NodeId)
	if !ok {
		return nil, fuse.ENOENT
	}

	// Check local cache first (dirty or clean)
	if cached, ok := fs.cache.Get(p); ok {
		if input.Offset >= uint64(cached.Size) {
			return fuse.ReadResultData([]byte{}), fuse.OK
		}
		end := input.Offset + uint64(input.Size)
		if end > uint64(cached.Size) {
			end = uint64(cached.Size)
		}
		return fuse.ReadResultData(cached.Data[input.Offset:end]), fuse.OK
	}

	ctx, cancelFunc := makeContext(cancel)
	defer cancelFunc()

	resp, err := fs.client.ReadFile(ctx, &pb.ReadFileRequest{
		VolumeId: fs.volumeID,
		Path:     p,
		Offset:   int64(input.Offset),
		Size:     int64(input.Size),
	})
	if err != nil {
		return nil, grpcErrorToStatus(err)
	}

	if input.Offset == 0 && resp.GetEof() && len(resp.GetData()) > 0 {
		fs.cache.Put(p, resp.GetData(), time.Now(), "")
	}

	return fuse.ReadResultData(resp.GetData()), fuse.OK
}

func (fs *ObjectFS) Write(cancel <-chan struct{}, input *fuse.WriteIn, data []byte) (uint32, fuse.Status) {
	p, ok := fs.getPath(input.NodeId)
	if !ok {
		return 0, fuse.ENOENT
	}

	// If not in cache and offset > 0, load existing content into cache first
	if _, exists := fs.cache.Get(p); !exists && input.Offset > 0 {
		ctx, cancelFunc := makeContext(cancel)
		resp, err := fs.client.ReadFile(ctx, &pb.ReadFileRequest{
			VolumeId: fs.volumeID,
			Path:     p,
			Offset:   0,
			Size:     int64(input.Offset),
		})
		cancelFunc()
		if err == nil && len(resp.GetData()) > 0 {
			fs.cache.Put(p, resp.GetData(), time.Now(), "")
		}
	}

	// Buffer write locally and mark dirty
	fs.cache.WriteAt(p, int64(input.Offset), data, time.Now())
	return uint32(len(data)), fuse.OK
}

func (fs *ObjectFS) syncFileToService(ctx context.Context, p string) error {
	entry, isDirty := fs.cache.GetDirty(p)
	if !isDirty {
		return nil
	}

	_, err := fs.client.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId:  fs.volumeID,
		Path:      p,
		Offset:    0,
		Data:      entry.Data,
		WriteMode: fs.writeMode,
	})
	if err != nil {
		return err
	}

	fs.cache.MarkClean(p)
	return nil
}

func (fs *ObjectFS) Flush(cancel <-chan struct{}, input *fuse.FlushIn) fuse.Status {
	ctx, cancelFunc := makeContext(cancel)
	defer cancelFunc()

	p, ok := fs.getPath(input.NodeId)
	if !ok {
		return fuse.ENOENT
	}

	if err := fs.syncFileToService(ctx, p); err != nil {
		return grpcErrorToStatus(err)
	}

	return fuse.OK
}

func (fs *ObjectFS) Fsync(cancel <-chan struct{}, input *fuse.FsyncIn) fuse.Status {
	ctx, cancelFunc := makeContext(cancel)
	defer cancelFunc()

	p, ok := fs.getPath(input.NodeId)
	if !ok {
		return fuse.ENOENT
	}

	if err := fs.syncFileToService(ctx, p); err != nil {
		return grpcErrorToStatus(err)
	}

	_, err := fs.client.Fsync(ctx, &pb.FsyncRequest{
		VolumeId: fs.volumeID,
		Path:     p,
	})
	if err != nil {
		return grpcErrorToStatus(err)
	}
	return fuse.OK
}

func (fs *ObjectFS) Release(cancel <-chan struct{}, input *fuse.ReleaseIn) {
	ctx, cancelFunc := makeContext(cancel)
	defer cancelFunc()

	p, ok := fs.getPath(input.NodeId)
	if !ok {
		return
	}

	_ = fs.syncFileToService(ctx, p)
}

func (fs *ObjectFS) StatFs(cancel <-chan struct{}, input *fuse.InHeader, out *fuse.StatfsOut) fuse.Status {
	out.Blocks = 1024 * 1024 * 1024
	out.Bfree = 1024 * 1024 * 1024
	out.Bavail = 1024 * 1024 * 1024
	out.Bsize = 4096
	out.Frsize = 4096
	out.Files = 1000000
	out.Ffree = 1000000
	out.NameLen = 255
	return fuse.OK
}

func (fs *ObjectFS) Access(cancel <-chan struct{}, input *fuse.AccessIn) fuse.Status {
	return fuse.OK
}

// StartWatcher starts a background loop listening to push notifications and invalidating cache.
func StartWatcher(ctx context.Context, client pb.ObjectFSControllerClient, volumeID, nodeID string, cache *NodeCache) {
	go func() {
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
				time.Sleep(1 * time.Second)
				continue
			}

			for {
				ev, err := stream.Recv()
				if err != nil {
					break
				}
				if ev.GetPath() != "" {
					cache.InvalidateIfNotDirty(ev.GetPath())
				}
				if ev.GetOldPath() != "" {
					cache.InvalidateIfNotDirty(ev.GetOldPath())
				}
			}
			time.Sleep(500 * time.Millisecond)
		}
	}()
}
