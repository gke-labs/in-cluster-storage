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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"golang.org/x/sync/errgroup"
	"k8s.io/klog/v2"
)

type dirMeta struct {
	path       string
	inode      uint64
	mode       uint32
	mtime      time.Time
	uid        uint32
	gid        uint32
	xattrCount *uint32
}

type symlinkMeta struct {
	path       string
	target     string
	inode      uint64
	xattrCount *uint32
}

type fileMeta struct {
	path       string
	relPath    string
	inode      uint64
	size       int64
	mode       uint32
	mtime      time.Time
	uid        uint32
	gid        uint32
	chunkSize  uint32
	xattrCount *uint32
}

func applyXAttrs(ctx context.Context, client pb.ObjectFSControllerClient, volumeID string, ino uint64, path string, xattrCount *uint32) error {
	if xattrCount != nil && *xattrCount == 0 {
		return nil
	}
	resp, err := client.ListXAttr(ctx, &pb.ListXAttrRequest{
		VolumeId: volumeID,
		Inode:    ino,
	})
	if err != nil {
		return fmt.Errorf("ListXAttr %s (ino %d): %w", path, ino, err)
	}
	if resp.GetError() != 0 {
		return fmt.Errorf("ListXAttr %s (ino %d) returned error: %d", path, ino, resp.GetError())
	}
	for _, x := range resp.GetXattrs() {
		name := x.GetName()
		val := x.GetValue()
		if err := unixLsetxattr(path, name, val, 0); err != nil {
			return fmt.Errorf("setxattr %s %s: %w", path, name, err)
		}
	}
	return nil
}

func (d *objectFSDriver) materialiseVolume(ctx context.Context, client pb.ObjectFSControllerClient, volumeID, lowerPath string) error {
	type dirQueueItem struct {
		ino     uint64
		relPath string
	}

	var dirs []dirMeta
	var symlinks []symlinkMeta
	var files []fileMeta
	var totalBytes int64

	// Stat root inode
	rootAttrResp, err := client.GetAttr(ctx, &pb.GetAttrRequest{
		VolumeId: volumeID,
		Inode:    1,
	})
	if err == nil && rootAttrResp.GetError() == 0 && rootAttrResp.GetAttr() != nil && rootAttrResp.GetAttr().GetInode() != nil {
		rootIno := rootAttrResp.GetAttr().GetInode()
		dirs = append(dirs, dirMeta{
			path:       lowerPath,
			inode:      1,
			mode:       rootIno.GetMode(),
			mtime:      rootIno.GetMtime().AsTime(),
			uid:        rootIno.GetUid(),
			gid:        rootIno.GetGid(),
			xattrCount: rootIno.XattrCount,
		})
	}

	queue := []dirQueueItem{{ino: 1, relPath: ""}}

	for len(queue) > 0 {
		curr := queue[0]
		queue = queue[1:]

		resp, err := client.ReadDir(ctx, &pb.ReadDirRequest{
			VolumeId: volumeID,
			Inode:    curr.ino,
		})
		if err != nil {
			return fmt.Errorf("ReadDir %d (%s) failed: %w", curr.ino, curr.relPath, err)
		}
		if resp.GetError() != 0 {
			return fmt.Errorf("ReadDir %d (%s) returned error: %d", curr.ino, curr.relPath, resp.GetError())
		}

		for _, entry := range resp.GetEntries() {
			name := entry.GetName()
			childRel := filepath.Join(curr.relPath, name)
			childFull := filepath.Join(lowerPath, childRel)
			ino := entry.GetInode()
			if ino == nil {
				continue
			}

			mode := ino.GetMode()
			fileType := mode & uint32(syscall.S_IFMT)

			if ino.GetIsDir() {
				if err := os.MkdirAll(childFull, 0755); err != nil {
					return fmt.Errorf("mkdir %s: %w", childFull, err)
				}
				dirs = append(dirs, dirMeta{
					path:       childFull,
					inode:      ino.GetIno(),
					mode:       ino.GetMode(),
					mtime:      ino.GetMtime().AsTime(),
					uid:        ino.GetUid(),
					gid:        ino.GetGid(),
					xattrCount: ino.XattrCount,
				})
				queue = append(queue, dirQueueItem{ino: ino.GetIno(), relPath: childRel})
			} else if fileType == uint32(syscall.S_IFLNK) || ino.GetSymlinkTarget() != "" {
				target := ino.GetSymlinkTarget()
				if target == "" {
					rlResp, err := client.Readlink(ctx, &pb.ReadlinkRequest{
						VolumeId: volumeID,
						Inode:    ino.GetIno(),
					})
					if err != nil {
						return fmt.Errorf("Readlink %d: %w", ino.GetIno(), err)
					}
					if rlResp.GetError() != 0 {
						return fmt.Errorf("Readlink %d returned error: %d", ino.GetIno(), rlResp.GetError())
					}
					target = rlResp.GetTarget()
				}
				symlinks = append(symlinks, symlinkMeta{
					path:       childFull,
					target:     target,
					inode:      ino.GetIno(),
					xattrCount: ino.XattrCount,
				})
			} else if fileType == uint32(syscall.S_IFREG) || fileType == 0 {
				// Regular file (hard links are materialised as independent copies)
				cs := ino.GetChunkSize()
				if cs == 0 {
					cs = 64 * 1024
				}
				files = append(files, fileMeta{
					path:       childFull,
					relPath:    childRel,
					inode:      ino.GetIno(),
					size:       ino.GetSize(),
					mode:       ino.GetMode(),
					mtime:      ino.GetMtime().AsTime(),
					uid:        ino.GetUid(),
					gid:        ino.GetGid(),
					chunkSize:  cs,
					xattrCount: ino.XattrCount,
				})
				totalBytes += ino.GetSize()
			} else {
				// Special files (fifos, device nodes, sockets) are skipped with a warning
				klog.Warningf("exclusive-local: skipping unsupported special file %s (mode %o)", childRel, mode)
			}
		}
	}

	// Create symlinks
	for _, sl := range symlinks {
		if err := os.Remove(sl.path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove existing symlink %s: %w", sl.path, err)
		}
		if err := os.MkdirAll(filepath.Dir(sl.path), 0755); err != nil {
			return err
		}
		if err := os.Symlink(sl.target, sl.path); err != nil {
			return fmt.Errorf("symlink %s -> %s: %w", sl.path, sl.target, err)
		}
		if err := applyXAttrs(ctx, client, volumeID, sl.inode, sl.path, sl.xattrCount); err != nil {
			return err
		}
	}

	// Parallel file downloads with bounded concurrency and progress reporting
	totalFiles := int64(len(files))
	var filesDone atomic.Int64
	var bytesDone atomic.Int64

	progressCtx, cancelProgress := context.WithCancel(ctx)
	defer cancelProgress()

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-progressCtx.Done():
				return
			case <-ticker.C:
				fDone := filesDone.Load()
				bDone := bytesDone.Load()
				klog.Infof("Materialising volume %s into %s: %d/%d files (%d/%d bytes) completed",
					volumeID, lowerPath, fDone, totalFiles, bDone, totalBytes)
			}
		}
	}()

	eg, gCtx := errgroup.WithContext(ctx)
	eg.SetLimit(16)

	for _, f := range files {
		f := f
		eg.Go(func() error {
			if err := d.downloadFile(gCtx, client, volumeID, f); err != nil {
				return fmt.Errorf("download file %s (ino %d): %w", f.relPath, f.inode, err)
			}
			filesDone.Add(1)
			bytesDone.Add(f.size)
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return fmt.Errorf("parallel download failed: %w", err)
	}
	cancelProgress()

	klog.Infof("Completed materialisation of volume %s into %s: %d files (%d bytes)",
		volumeID, lowerPath, filesDone.Load(), bytesDone.Load())

	// Apply directory modes, ownership, mtimes, and xattrs bottom-up
	for i := len(dirs) - 1; i >= 0; i-- {
		dm := dirs[i]
		if err := applyXAttrs(ctx, client, volumeID, dm.inode, dm.path, dm.xattrCount); err != nil {
			return err
		}
		if dm.mode != 0 {
			if err := os.Chmod(dm.path, os.FileMode(dm.mode&07777)); err != nil {
				klog.Warningf("chmod %s: %v", dm.path, err)
			}
		}
		if dm.uid != 0 || dm.gid != 0 {
			if err := os.Chown(dm.path, int(dm.uid), int(dm.gid)); err != nil {
				klog.V(4).Infof("chown %s: %v", dm.path, err)
			}
		}
		if !dm.mtime.IsZero() {
			if err := os.Chtimes(dm.path, dm.mtime, dm.mtime); err != nil {
				klog.Warningf("chtimes %s: %v", dm.path, err)
			}
		}
	}

	return nil
}

func closeAndRemove(f *os.File, path string) {
	if f != nil {
		if err := f.Close(); err != nil {
			klog.V(4).Infof("close temp file %s: %v", path, err)
		}
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		klog.V(4).Infof("remove temp file %s: %v", path, err)
	}
}

// downloadFile downloads a file in a resumable, atomic manner:
// 1. Skips downloading if the destination file already exists with matching size and mtime.
// 2. Writes chunks to a temporary file, sets mode/ownership/mtime, and atomically renames on completion.
func (d *objectFSDriver) downloadFile(ctx context.Context, client pb.ObjectFSControllerClient, volumeID string, f fileMeta) (err error) {
	st, statErr := os.Stat(f.path)
	if statErr == nil {
		if st.Size() == f.size && st.ModTime().Equal(f.mtime) {
			// File already fully materialised from a previous attempt; resume without re-downloading
			return nil
		}
	} else if !os.IsNotExist(statErr) {
		return fmt.Errorf("stat %s: %w", f.path, statErr)
	}

	if err := os.MkdirAll(filepath.Dir(f.path), 0755); err != nil {
		return err
	}

	if f.size == 0 {
		mode := os.FileMode(f.mode & 07777)
		if mode == 0 {
			mode = 0644
		}
		tmpPath := fmt.Sprintf("%s.tmp.%d", f.path, time.Now().UnixNano())
		if err := os.WriteFile(tmpPath, nil, mode); err != nil {
			return fmt.Errorf("write empty temp file %s: %w", tmpPath, err)
		}
		if f.uid != 0 || f.gid != 0 {
			if chownErr := os.Chown(tmpPath, int(f.uid), int(f.gid)); chownErr != nil {
				klog.V(4).Infof("chown %s: %v", tmpPath, chownErr)
			}
		}
		if !f.mtime.IsZero() {
			if chtimesErr := os.Chtimes(tmpPath, f.mtime, f.mtime); chtimesErr != nil {
				klog.Warningf("chtimes %s: %v", tmpPath, chtimesErr)
			}
		}
		if err := applyXAttrs(ctx, client, volumeID, f.inode, tmpPath, f.xattrCount); err != nil {
			if remErr := os.Remove(tmpPath); remErr != nil && !os.IsNotExist(remErr) {
				klog.V(4).Infof("remove temp file %s: %v", tmpPath, remErr)
			}
			return err
		}
		if renameErr := os.Rename(tmpPath, f.path); renameErr != nil {
			return fmt.Errorf("rename empty temp file %s to %s: %w", tmpPath, f.path, renameErr)
		}
		return nil
	}

	tmpPath := fmt.Sprintf("%s.tmp.%d", f.path, time.Now().UnixNano())
	out, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, os.FileMode(f.mode&07777))
	if err != nil {
		return fmt.Errorf("create temp file %s: %w", tmpPath, err)
	}

	var manifest *pb.GetManifestResponse
	if d.diskCache != nil {
		mResp, mErr := client.GetManifest(ctx, &pb.GetManifestRequest{
			VolumeId: volumeID,
			Inode:    f.inode,
		})
		if mErr == nil && mResp.GetError() == 0 {
			manifest = mResp
		}
	}

	cs := int64(f.chunkSize)
	if cs <= 0 {
		cs = 64 * 1024
	}

	for offset := int64(0); offset < f.size; offset += cs {
		chunkIdx := int(offset / cs)
		readLen := cs
		if offset+readLen > f.size {
			readLen = f.size - offset
		}

		var chunkData []byte

		// 1. Inline data from manifest if chunk 0
		if manifest != nil && chunkIdx == 0 && len(manifest.GetInlineData()) > 0 {
			chunkData = manifest.GetInlineData()
			if int64(len(chunkData)) > readLen {
				chunkData = chunkData[:readLen]
			}
		}

		// 2. On-disk blob cache by SHA
		if len(chunkData) == 0 && d.diskCache != nil && manifest != nil && chunkIdx < len(manifest.GetChunkSha256()) {
			sha := manifest.GetChunkSha256()[chunkIdx]
			if sha != "" {
				if d.diskCache.HasBlob(sha) {
					if data, readErr := d.diskCache.ReadBlob(ctx, sha); readErr == nil {
						chunkData = data
					} else {
						klog.V(4).Infof("disk cache ReadBlob %s: %v", sha, readErr)
					}
				}
				if len(chunkData) == 0 {
					if data, fetchErr := d.diskCache.FetchBlob(ctx, client, sha); fetchErr == nil {
						chunkData = data
					} else {
						klog.V(4).Infof("disk cache FetchBlob %s: %v", sha, fetchErr)
					}
				}
			}
		}

		// 3. Fallback to controller ReadFile RPC
		if len(chunkData) == 0 {
			readResp, readErr := client.ReadFile(ctx, &pb.ReadFileRequest{
				VolumeId: volumeID,
				Inode:    f.inode,
				Offset:   offset,
				Size:     readLen,
			})
			if readErr != nil {
				closeAndRemove(out, tmpPath)
				return fmt.Errorf("ReadFile %s (offset %d): %w", f.path, offset, readErr)
			}
			if readResp.GetError() != 0 {
				closeAndRemove(out, tmpPath)
				return fmt.Errorf("ReadFile %s (offset %d) returned error: %d", f.path, offset, readResp.GetError())
			}
			chunkData = readResp.GetData()
			if d.diskCache != nil && len(chunkData) > 0 {
				sum := sha256.Sum256(chunkData)
				sha := hex.EncodeToString(sum[:])
				if putErr := d.diskCache.PutBlob(ctx, sha, chunkData); putErr != nil {
					klog.V(4).Infof("disk cache PutBlob %s: %v", sha, putErr)
				}
			}
		}

		if int64(len(chunkData)) > readLen {
			chunkData = chunkData[:readLen]
		}
		if _, writeErr := out.Write(chunkData); writeErr != nil {
			closeAndRemove(out, tmpPath)
			return fmt.Errorf("write %s: %w", tmpPath, writeErr)
		}
	}

	if f.mode != 0 {
		if chmodErr := os.Chmod(tmpPath, os.FileMode(f.mode&07777)); chmodErr != nil {
			klog.Warningf("chmod %s: %v", tmpPath, chmodErr)
		}
	}
	if f.uid != 0 || f.gid != 0 {
		if chownErr := os.Chown(tmpPath, int(f.uid), int(f.gid)); chownErr != nil {
			klog.V(4).Infof("chown %s: %v", tmpPath, chownErr)
		}
	}
	if !f.mtime.IsZero() {
		if chtimesErr := os.Chtimes(tmpPath, f.mtime, f.mtime); chtimesErr != nil {
			klog.Warningf("chtimes %s: %v", tmpPath, chtimesErr)
		}
	}

	if closeErr := out.Close(); closeErr != nil {
		if remErr := os.Remove(tmpPath); remErr != nil && !os.IsNotExist(remErr) {
			klog.V(4).Infof("remove temp file %s: %v", tmpPath, remErr)
		}
		return fmt.Errorf("close temp file %s: %w", tmpPath, closeErr)
	}

	if err := applyXAttrs(ctx, client, volumeID, f.inode, tmpPath, f.xattrCount); err != nil {
		if remErr := os.Remove(tmpPath); remErr != nil && !os.IsNotExist(remErr) {
			klog.V(4).Infof("remove temp file %s: %v", tmpPath, remErr)
		}
		return err
	}

	// Atomically rename into final path
	if renameErr := os.Rename(tmpPath, f.path); renameErr != nil {
		if remErr := os.Remove(tmpPath); remErr != nil && !os.IsNotExist(remErr) {
			klog.V(4).Infof("remove temp file %s: %v", tmpPath, remErr)
		}
		return fmt.Errorf("rename temp file %s to %s: %w", tmpPath, f.path, renameErr)
	}
	return nil
}
