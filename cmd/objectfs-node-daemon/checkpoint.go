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
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/errutil"
	"github.com/gke-labs/in-cluster-storage/pkg/scoped"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type OpType string

const (
	OpMkdir        OpType = "mkdir"
	OpCreateFile   OpType = "create_file"
	OpWriteFile    OpType = "write_file"
	OpTruncateFile OpType = "truncate_file"
	OpSymlink      OpType = "symlink"
	OpUnlink       OpType = "unlink"
	OpRmdir        OpType = "rmdir"
	OpSetAttr      OpType = "set_attr"
	OpSetXAttr     OpType = "set_xattr"
	OpRemoveXAttr  OpType = "remove_xattr"
)

type FileOp struct {
	Type   OpType
	Path   string // relative path from volume root (e.g. "dir/file.txt")
	Parent string // relative path of parent dir (e.g. "dir" or "")
	Name   string // base name (e.g. "file.txt") or xattr name
	Mode   uint32
	Size   int64
	Mtime  time.Time
	Uid    uint32
	Gid    uint32
	Target string // for symlink
	Data   []byte // for create or write
	Offset int64  // for write
	Value  []byte // for set_xattr
}

var (
	unixLlistxattr = unix.Llistxattr
	unixLgetxattr  = unix.Lgetxattr
	unixLsetxattr  = unix.Lsetxattr
)

func isWhiteout(path string, info os.FileInfo) bool {
	sys, ok := info.Sys().(*syscall.Stat_t)
	if ok && (info.Mode()&os.ModeCharDevice != 0) && (sys.Rdev == 0) {
		return true
	}
	// Also check xattr for user-namespace overlayfs or unprivileged test environments
	buf := make([]byte, 16)
	if sz, err := unix.Getxattr(path, "user.overlay.whiteout", buf); err == nil && sz >= 0 {
		return true
	}
	if sz, err := unix.Getxattr(path, "trusted.overlay.whiteout", buf); err == nil && sz >= 0 {
		return true
	}
	return false
}

func isOpaqueDir(path string) bool {
	buf := make([]byte, 16)
	sz, err := unix.Getxattr(path, "trusted.overlay.opaque", buf)
	if err == nil && sz > 0 && string(buf[:sz]) == "y" {
		return true
	}
	sz, err = unix.Getxattr(path, "user.overlay.opaque", buf)
	if err == nil && sz > 0 && string(buf[:sz]) == "y" {
		return true
	}
	return false
}

func splitRelPath(relPath string) (string, string) {
	relPath = filepath.Clean(relPath)
	if relPath == "." || relPath == "" || relPath == "/" {
		return "", ""
	}
	parent := filepath.Dir(relPath)
	if parent == "." {
		parent = ""
	}
	return parent, filepath.Base(relPath)
}

func pathDepth(p string) int {
	clean := filepath.Clean(p)
	if clean == "." || clean == "" {
		return 0
	}
	return strings.Count(clean, string(filepath.Separator)) + 1
}

func isOverlayInternalXattr(name string) bool {
	return strings.HasPrefix(name, "trusted.overlay.") || strings.HasPrefix(name, "user.overlay.")
}

func listXAttrNames(path string) ([]string, error) {
	sz, err := unixLlistxattr(path, nil)
	if err != nil {
		return nil, err
	}
	if sz <= 0 {
		return nil, nil
	}
	buf := make([]byte, sz)
	for {
		sz, err = unixLlistxattr(path, buf)
		if err == unix.ERANGE {
			buf = make([]byte, len(buf)*2)
			continue
		}
		if err != nil {
			return nil, err
		}
		break
	}
	var names []string
	start := 0
	for i := 0; i < sz; i++ {
		if buf[i] == 0 {
			if i > start {
				names = append(names, string(buf[start:i]))
			}
			start = i + 1
		}
	}
	return names, nil
}

func getXAttr(path, name string) ([]byte, error) {
	sz, err := unixLgetxattr(path, name, nil)
	if err != nil {
		return nil, err
	}
	if sz <= 0 {
		return []byte{}, nil
	}
	buf := make([]byte, sz)
	for {
		sz, err = unixLgetxattr(path, name, buf)
		if err == unix.ERANGE {
			buf = make([]byte, len(buf)*2)
			continue
		}
		if err != nil {
			return nil, err
		}
		break
	}
	return buf[:sz], nil
}

func scanXAttrs(lowerDir, upperDir, relPath string, existsInLower bool, prevEntry *ManifestEntry) ([]FileOp, error) {
	upperPath := filepath.Join(upperDir, relPath)
	upperNames, err := listXAttrNames(upperPath)
	if err != nil {
		return nil, fmt.Errorf("listxattr %s: %w", upperPath, err)
	}

	upperMap := make(map[string][]byte)
	upperShaMap := make(map[string]string)
	for _, name := range upperNames {
		if isOverlayInternalXattr(name) {
			continue
		}
		val, err := getXAttr(upperPath, name)
		if err != nil {
			return nil, fmt.Errorf("getxattr %s %s: %w", upperPath, name, err)
		}
		upperMap[name] = val
		h := sha256.Sum256(val)
		upperShaMap[name] = hex.EncodeToString(h[:])
	}

	baselineShaMap := make(map[string]string)
	if prevEntry != nil {
		for k, v := range prevEntry.Xattrs {
			baselineShaMap[k] = v
		}
	} else if existsInLower {
		lowerPath := filepath.Join(lowerDir, relPath)
		lowerNames, err := listXAttrNames(lowerPath)
		if err != nil {
			return nil, fmt.Errorf("listxattr %s: %w", lowerPath, err)
		}
		for _, name := range lowerNames {
			// Drop OverlayFS-internal names so local overlay bookkeeping on the lower layer is never emitted as a removal.
			if isOverlayInternalXattr(name) {
				continue
			}
			val, err := getXAttr(lowerPath, name)
			if err != nil {
				return nil, fmt.Errorf("getxattr %s %s: %w", lowerPath, name, err)
			}
			h := sha256.Sum256(val)
			baselineShaMap[name] = hex.EncodeToString(h[:])
		}
	}

	var setOps []FileOp
	var setNames []string
	for name, upperSha := range upperShaMap {
		baseSha, ok := baselineShaMap[name]
		if !ok || baseSha != upperSha {
			setNames = append(setNames, name)
		}
	}
	sort.Strings(setNames)
	for _, name := range setNames {
		setOps = append(setOps, FileOp{
			Type:  OpSetXAttr,
			Path:  relPath,
			Name:  name,
			Value: upperMap[name],
		})
	}

	var removeOps []FileOp
	var removeNames []string
	for name := range baselineShaMap {
		if _, ok := upperMap[name]; !ok {
			removeNames = append(removeNames, name)
		}
	}
	sort.Strings(removeNames)
	for _, name := range removeNames {
		removeOps = append(removeOps, FileOp{
			Type: OpRemoveXAttr,
			Path: relPath,
			Name: name,
		})
	}

	return append(setOps, removeOps...), nil
}

// ScanUpperLayer compares lowerDir and upperDir (optionally diffing against an existing CheckpointManifest)
// and produces an ordered list of FileOps.
// Renames appear as unlink+create; hard links are scanned as independent files.
func ScanUpperLayer(ctx context.Context, lowerDir, upperDir string, manifests ...*CheckpointManifest) ([]FileOp, error) {
	if _, err := os.Stat(upperDir); os.IsNotExist(err) {
		return nil, nil
	}

	var manifest *CheckpointManifest
	if len(manifests) > 0 && manifests[0] != nil {
		manifest = manifests[0]
	}

	deletedInLower := make(map[string]bool)

	// Phase 1: Scan whiteouts and opaque directories for deletions
	deletions, err := scanWhiteoutsAndOpaqueDirs(lowerDir, upperDir, deletedInLower, manifest)
	if err != nil {
		return nil, err
	}

	// Phase 2: Scan creations, modifications, symlinks, and directory creations
	creations, writes, attrs, err := scanModificationsAndCreations(ctx, lowerDir, upperDir, deletedInLower, manifest)
	if err != nil {
		return nil, err
	}

	// Sort deletions: deepest paths first
	sort.Slice(deletions, func(i, j int) bool {
		return pathDepth(deletions[i].Path) > pathDepth(deletions[j].Path)
	})

	// Sort creations: shallowest paths first (so parents are created before children)
	sort.Slice(creations, func(i, j int) bool {
		return pathDepth(creations[i].Path) < pathDepth(creations[j].Path)
	})

	// Sort attrs: deepest directories last (bottom-up), preserving SetAttr before SetXAttr/RemoveXAttr per path
	sort.SliceStable(attrs, func(i, j int) bool {
		return pathDepth(attrs[i].Path) > pathDepth(attrs[j].Path)
	})

	var result []FileOp
	result = append(result, deletions...)
	result = append(result, creations...)
	result = append(result, writes...)
	result = append(result, attrs...)

	return result, nil
}

func scanWhiteoutsAndOpaqueDirs(lowerDir, upperDir string, deletedInLower map[string]bool, manifest *CheckpointManifest) ([]FileOp, error) {
	var deletions []FileOp

	err := filepath.Walk(upperDir, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relPath, relErr := filepath.Rel(upperDir, path)
		if relErr != nil {
			return relErr
		}
		if relPath == "." {
			return nil
		}

		if isWhiteout(path, info) {
			if manifest != nil && manifest.Whiteouts[relPath] {
				// Already committed in a previous checkpoint; skip to avoid redundant unlinks
				deletedInLower[relPath] = true
				return nil
			}
			lowerPath := filepath.Join(lowerDir, relPath)
			if lInfo, lErr := os.Lstat(lowerPath); lErr == nil {
				if lInfo.IsDir() {
					// Recursively delete lower directory contents
					var subDirs []string
					wErr := filepath.Walk(lowerPath, func(subPath string, subInfo os.FileInfo, err error) error {
						if err != nil {
							return err
						}
						subRel, err := filepath.Rel(lowerDir, subPath)
						if err != nil {
							return err
						}
						if subRel == "." {
							return nil
						}
						if subInfo.IsDir() {
							subDirs = append(subDirs, subRel)
						} else {
							if !deletedInLower[subRel] {
								deletedInLower[subRel] = true
								p, n := splitRelPath(subRel)
								deletions = append(deletions, FileOp{
									Type:   OpUnlink,
									Path:   subRel,
									Parent: p,
									Name:   n,
								})
							}
						}
						return nil
					})
					if wErr != nil {
						return fmt.Errorf("walk lower %s: %w", lowerPath, wErr)
					}
					// Sort subdirs deepest first
					sort.Slice(subDirs, func(i, j int) bool {
						return pathDepth(subDirs[i]) > pathDepth(subDirs[j])
					})
					for _, sd := range subDirs {
						if !deletedInLower[sd] {
							deletedInLower[sd] = true
							p, n := splitRelPath(sd)
							deletions = append(deletions, FileOp{
								Type:   OpRmdir,
								Path:   sd,
								Parent: p,
								Name:   n,
							})
						}
					}
					if !deletedInLower[relPath] {
						deletedInLower[relPath] = true
						p, n := splitRelPath(relPath)
						deletions = append(deletions, FileOp{
							Type:   OpRmdir,
							Path:   relPath,
							Parent: p,
							Name:   n,
						})
					}
				} else {
					if !deletedInLower[relPath] {
						deletedInLower[relPath] = true
						p, n := splitRelPath(relPath)
						deletions = append(deletions, FileOp{
							Type:   OpUnlink,
							Path:   relPath,
							Parent: p,
							Name:   n,
						})
					}
				}
			} else {
				if !deletedInLower[relPath] {
					deletedInLower[relPath] = true
					p, n := splitRelPath(relPath)
					deletions = append(deletions, FileOp{
						Type:   OpUnlink,
						Path:   relPath,
						Parent: p,
						Name:   n,
					})
				}
			}
			return nil
		}

		if info.IsDir() && isOpaqueDir(path) {
			if manifest != nil && manifest.OpaqueDirs[relPath] {
				return nil
			}
			// Opaque directory: prune any entries in lowerDir under relPath not in upperDir
			lowerPath := filepath.Join(lowerDir, relPath)
			if lInfo, lErr := os.Stat(lowerPath); lErr == nil && lInfo.IsDir() {
				var opaqueDeletedDirs []string
				wErr := filepath.Walk(lowerPath, func(subPath string, subInfo os.FileInfo, err error) error {
					if err != nil {
						return err
					}
					subRel, err := filepath.Rel(lowerDir, subPath)
					if err != nil {
						return err
					}
					if subRel == "." || subRel == relPath {
						return nil
					}
					// Check if present in upperDir
					if _, uErr := os.Lstat(filepath.Join(upperDir, subRel)); os.IsNotExist(uErr) {
						if subInfo.IsDir() {
							opaqueDeletedDirs = append(opaqueDeletedDirs, subRel)
						} else {
							if !deletedInLower[subRel] {
								deletedInLower[subRel] = true
								p, n := splitRelPath(subRel)
								deletions = append(deletions, FileOp{
									Type:   OpUnlink,
									Path:   subRel,
									Parent: p,
									Name:   n,
								})
							}
						}
					}
					return nil
				})
				if wErr != nil {
					return fmt.Errorf("walk lower opaque %s: %w", lowerPath, wErr)
				}
				sort.Slice(opaqueDeletedDirs, func(i, j int) bool {
					return pathDepth(opaqueDeletedDirs[i]) > pathDepth(opaqueDeletedDirs[j])
				})
				for _, sd := range opaqueDeletedDirs {
					if !deletedInLower[sd] {
						deletedInLower[sd] = true
						p, n := splitRelPath(sd)
						deletions = append(deletions, FileOp{
							Type:   OpRmdir,
							Path:   sd,
							Parent: p,
							Name:   n,
						})
					}
				}
			}
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk upper for whiteouts: %w", err)
	}

	return deletions, nil
}

func scanModificationsAndCreations(ctx context.Context, lowerDir, upperDir string, deletedInLower map[string]bool, manifest *CheckpointManifest) (creations, writes, attrs []FileOp, err error) {
	err = filepath.Walk(upperDir, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relPath, relErr := filepath.Rel(upperDir, path)
		if relErr != nil {
			return relErr
		}
		if relPath == "." {
			return nil
		}

		if isWhiteout(path, info) {
			return nil
		}

		mode := uint32(info.Mode() & 07777)
		fileType := info.Mode() & os.ModeType

		var uid, gid uint32
		if sys, ok := info.Sys().(*syscall.Stat_t); ok {
			uid = sys.Uid
			gid = sys.Gid
		}

		parent, name := splitRelPath(relPath)
		lowerPath := filepath.Join(lowerDir, relPath)
		lStat, lErr := os.Lstat(lowerPath)
		existsInLower := (lErr == nil) && !deletedInLower[relPath]

		if info.IsDir() {
			var inManifest bool
			var prevEntry *ManifestEntry
			if manifest != nil && manifest.Files != nil {
				if e, ok := manifest.Files[relPath]; ok {
					prevEntry = &e
					inManifest = true
				}
			}
			if !existsInLower && !inManifest {
				creations = append(creations, FileOp{
					Type:   OpMkdir,
					Path:   relPath,
					Parent: parent,
					Name:   name,
					Mode:   mode,
					Uid:    uid,
					Gid:    gid,
				})
			}
			attrs = append(attrs, FileOp{
				Type:   OpSetAttr,
				Path:   relPath,
				Parent: parent,
				Name:   name,
				Mode:   mode,
				Mtime:  info.ModTime(),
				Uid:    uid,
				Gid:    gid,
			})
			xattrOps, xErr := scanXAttrs(lowerDir, upperDir, relPath, existsInLower, prevEntry)
			if xErr != nil {
				return xErr
			}
			attrs = append(attrs, xattrOps...)
			return nil
		}

		if (info.Mode() & os.ModeSymlink) != 0 {
			target, rErr := os.Readlink(path)
			if rErr != nil {
				return fmt.Errorf("readlink %s: %w", path, rErr)
			}
			var inManifest bool
			var prevEntry *ManifestEntry
			if manifest != nil && manifest.Files != nil {
				if e, ok := manifest.Files[relPath]; ok {
					prevEntry = &e
					inManifest = true
				}
			}
			if !inManifest {
				if existsInLower {
					if lStat.IsDir() {
						creations = append(creations, FileOp{Type: OpRmdir, Path: relPath, Parent: parent, Name: name})
					} else {
						creations = append(creations, FileOp{Type: OpUnlink, Path: relPath, Parent: parent, Name: name})
					}
				}
				creations = append(creations, FileOp{
					Type:   OpSymlink,
					Path:   relPath,
					Parent: parent,
					Name:   name,
					Target: target,
					Uid:    uid,
					Gid:    gid,
				})
			}
			xattrOps, xErr := scanXAttrs(lowerDir, upperDir, relPath, existsInLower, prevEntry)
			if xErr != nil {
				return xErr
			}
			attrs = append(attrs, xattrOps...)
			return nil
		}

		if fileType != 0 {
			// Special files (fifos, sockets, character/block devices) are skipped with a warning
			scoped.Log(ctx).Info("exclusive-local: skipping unsupported special file", "path", relPath, "mode", info.Mode())
			return nil
		}

		// Regular file
		fileSize := info.Size()
		mtimeNano := info.ModTime().UnixNano()

		var inManifest bool
		var prevEntry ManifestEntry
		if manifest != nil && manifest.Files != nil {
			prevEntry, inManifest = manifest.Files[relPath]
		}

		// Design Comment (Concurrency Tolerance & Point-in-Time Prefix):
		// During exclusive-local periodic interval or explicit checkpoints, workloads keep running
		// and mutating files in the overlayfs upper directory. The scan must tolerate files changing
		// underneath it without aborting the checkpoint:
		// - If a file is unlinked concurrently before Open/Read, the scan tolerates ENOENT and skips it;
		//   subsequent checkpoints or unmount will process the whiteout/deletion.
		// - If a file is truncated or appended concurrently while reading chunks, the scan commits
		//   the prefix that it successfully read; the next checkpoint picks up the rest.
		// - A checkpoint represents a point-in-time prefix per file, not a cross-file snapshot.
		// - For appended files where a prefix was already committed in a previous checkpoint (recorded
		//   in the checkpoint manifest), the scan seeks directly to the previous size and only reads/emits
		//   writes for the newly appended bytes, avoiding rewriting already-committed prefix chunks.
		// - lower/ and upper/ layers are NEVER modified or deleted by the daemon while mounted, complying
		//   with kernel OverlayFS invariants.

		if inManifest {
			if lStat != nil && lStat.IsDir() {
				creations = append(creations, FileOp{Type: OpRmdir, Path: relPath, Parent: parent, Name: name})
			}

			xattrOps, xErr := scanXAttrs(lowerDir, upperDir, relPath, existsInLower, &prevEntry)
			if xErr != nil {
				return xErr
			}

			if fileSize == prevEntry.Size && mtimeNano == prevEntry.Mtime {
				// Unchanged content since previous checkpoint
				if len(xattrOps) > 0 {
					attrs = append(attrs, FileOp{
						Type:   OpSetAttr,
						Path:   relPath,
						Parent: parent,
						Name:   name,
						Mode:   mode,
						Mtime:  info.ModTime(),
						Uid:    uid,
						Gid:    gid,
					})
					attrs = append(attrs, xattrOps...)
				}
				return nil
			}

			f, oErr := os.Open(path)
			if oErr != nil {
				if os.IsNotExist(oErr) {
					return nil
				}
				return oErr
			}

			startOffset := int64(0)
			if fileSize > prevEntry.Size {
				// File was appended: start reading from previous committed size
				startOffset = prevEntry.Size
				if _, sErr := f.Seek(startOffset, io.SeekStart); sErr != nil {
					errutil.CloseAndLog(f, "upper file")
					return fmt.Errorf("seek appended file %s to %d: %w", path, startOffset, sErr)
				}
			} else if fileSize != prevEntry.Size {
				writes = append(writes, FileOp{
					Type: OpTruncateFile,
					Path: relPath,
					Size: fileSize,
				})
			}

			offset := startOffset
			for {
				chunkBuf := make([]byte, 64*1024)
				readN, readErr := io.ReadFull(f, chunkBuf)
				if readN > 0 {
					writes = append(writes, FileOp{
						Type:   OpWriteFile,
						Path:   relPath,
						Offset: offset,
						Data:   chunkBuf[:readN],
					})
					offset += int64(readN)
				}
				if readErr == io.EOF || readErr == io.ErrUnexpectedEOF {
					break
				}
				if readErr != nil {
					errutil.CloseAndLog(f, "upper file")
					return fmt.Errorf("read chunk of modified file %s at %d: %w", path, offset, readErr)
				}
			}
			errutil.CloseAndLog(f, "upper file")

			attrs = append(attrs, FileOp{
				Type:   OpSetAttr,
				Path:   relPath,
				Parent: parent,
				Name:   name,
				Mode:   mode,
				Mtime:  info.ModTime(),
				Uid:    uid,
				Gid:    gid,
			})
			attrs = append(attrs, xattrOps...)
			return nil
		} else if !existsInLower || (lStat != nil && lStat.IsDir()) {
			if existsInLower && lStat.IsDir() {
				creations = append(creations, FileOp{Type: OpRmdir, Path: relPath, Parent: parent, Name: name})
			}

			// Read first 64 KiB chunk for initial_content
			f, oErr := os.Open(path)
			if oErr != nil {
				if os.IsNotExist(oErr) {
					return nil
				}
				return oErr
			}

			initialChunkSize := int64(64 * 1024)
			initBuf := make([]byte, initialChunkSize)
			n, rErr := io.ReadFull(f, initBuf)
			if rErr != nil && rErr != io.EOF && rErr != io.ErrUnexpectedEOF {
				errutil.CloseAndLog(f, "upper file")
				return fmt.Errorf("read initial content of %s: %w", path, rErr)
			}
			initBuf = initBuf[:n]

			creations = append(creations, FileOp{
				Type:   OpCreateFile,
				Path:   relPath,
				Parent: parent,
				Name:   name,
				Mode:   mode,
				Size:   int64(n),
				Uid:    uid,
				Gid:    gid,
				Data:   initBuf,
			})

			// Remaining chunks
			offset := int64(n)
			for {
				chunkBuf := make([]byte, 64*1024)
				readN, readErr := io.ReadFull(f, chunkBuf)
				if readN > 0 {
					writes = append(writes, FileOp{
						Type:   OpWriteFile,
						Path:   relPath,
						Offset: offset,
						Data:   chunkBuf[:readN],
					})
					offset += int64(readN)
				}
				if readErr == io.EOF || readErr == io.ErrUnexpectedEOF {
					break
				}
				if readErr != nil {
					errutil.CloseAndLog(f, "upper file")
					return fmt.Errorf("read chunk of %s at %d: %w", path, offset, readErr)
				}
			}
			errutil.CloseAndLog(f, "upper file")

			attrs = append(attrs, FileOp{
				Type:   OpSetAttr,
				Path:   relPath,
				Parent: parent,
				Name:   name,
				Mode:   mode,
				Mtime:  info.ModTime(),
				Uid:    uid,
				Gid:    gid,
			})
			xattrOps, xErr := scanXAttrs(lowerDir, upperDir, relPath, existsInLower, nil)
			if xErr != nil {
				return xErr
			}
			attrs = append(attrs, xattrOps...)
			return nil
		} else {
			// Modified file from lower layer before any checkpoint
			xattrOps, xErr := scanXAttrs(lowerDir, upperDir, relPath, existsInLower, nil)
			if xErr != nil {
				return xErr
			}

			if lStat.Size() == fileSize && lStat.ModTime().Equal(info.ModTime()) {
				// Unchanged content relative to lower layer
				if len(xattrOps) > 0 {
					attrs = append(attrs, FileOp{
						Type:   OpSetAttr,
						Path:   relPath,
						Parent: parent,
						Name:   name,
						Mode:   mode,
						Mtime:  info.ModTime(),
						Uid:    uid,
						Gid:    gid,
					})
					attrs = append(attrs, xattrOps...)
				}
				return nil
			}

			f, oErr := os.Open(path)
			if oErr != nil {
				if os.IsNotExist(oErr) {
					return nil
				}
				return oErr
			}

			if lStat != nil && lStat.Size() != fileSize {
				writes = append(writes, FileOp{
					Type: OpTruncateFile,
					Path: relPath,
					Size: fileSize,
				})
			}

			offset := int64(0)
			for {
				chunkBuf := make([]byte, 64*1024)
				readN, readErr := io.ReadFull(f, chunkBuf)
				if readN > 0 {
					writes = append(writes, FileOp{
						Type:   OpWriteFile,
						Path:   relPath,
						Offset: offset,
						Data:   chunkBuf[:readN],
					})
					offset += int64(readN)
				}
				if readErr == io.EOF || readErr == io.ErrUnexpectedEOF {
					break
				}
				if readErr != nil {
					errutil.CloseAndLog(f, "upper file")
					return fmt.Errorf("read chunk of modified file %s at %d: %w", path, offset, readErr)
				}
			}
			errutil.CloseAndLog(f, "upper file")

			attrs = append(attrs, FileOp{
				Type:   OpSetAttr,
				Path:   relPath,
				Parent: parent,
				Name:   name,
				Mode:   mode,
				Mtime:  info.ModTime(),
				Uid:    uid,
				Gid:    gid,
			})
			attrs = append(attrs, xattrOps...)
			return nil
		}
	})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("walk upper for modifications: %w", err)
	}

	return creations, writes, attrs, nil
}

// -----------------------------------------------------------------------------
// Checkpoint Replay
// -----------------------------------------------------------------------------

func resolveInode(ctx context.Context, client pb.ObjectFSControllerClient, volumeID string, pathInodes map[string]uint64, relPath string) (uint64, error) {
	relPath = filepath.Clean(relPath)
	if relPath == "." || relPath == "" || relPath == "/" {
		return 1, nil
	}
	if ino, ok := pathInodes[relPath]; ok {
		return ino, nil
	}
	parts := strings.Split(relPath, string(filepath.Separator))
	currIno := uint64(1)
	currPath := ""
	for _, part := range parts {
		if part == "" || part == "." {
			continue
		}
		if currPath == "" {
			currPath = part
		} else {
			currPath = currPath + "/" + part
		}
		if ino, ok := pathInodes[currPath]; ok {
			currIno = ino
			continue
		}
		resp, err := client.Lookup(ctx, &pb.LookupRequest{
			VolumeId:    volumeID,
			ParentInode: currIno,
			Name:        part,
		})
		if err != nil {
			return 0, fmt.Errorf("lookup %q under ino %d: %w", part, currIno, err)
		}
		if resp.GetError() != 0 {
			return 0, fmt.Errorf("lookup %q under ino %d returned error: %d", part, currIno, resp.GetError())
		}
		ino := resp.GetAttr().GetInode().GetIno()
		pathInodes[currPath] = ino
		currIno = ino
	}
	return currIno, nil
}

// ReplayOperations executes the scanned operations sequentially as RPCs against the controller,
// and issues blocking fsyncs on all touched inodes. It returns the highest stream position
// committed by the fsyncs.
func ReplayOperations(ctx context.Context, client pb.ObjectFSControllerClient, volumeID string, ops []FileOp) (uint64, error) {
	log := scoped.Log(ctx).WithValues("volume", volumeID)
	pathInodes := make(map[string]uint64)
	pathInodes[""] = 1
	pathInodes["."] = 1

	touchedInodes := make(map[uint64]bool)

	for _, op := range ops {
		switch op.Type {
		case OpUnlink:
			parentIno, err := resolveInode(ctx, client, volumeID, pathInodes, op.Parent)
			if err != nil {
				log.V(4).Info("resolveInode parent for unlink (skipping)", "path", op.Path, "err", err)
				continue
			}
			resp, err := client.Unlink(ctx, &pb.UnlinkRequest{
				VolumeId:    volumeID,
				ParentInode: parentIno,
				Name:        op.Name,
			})
			if err != nil {
				return 0, fmt.Errorf("unlink %s (parent %d, name %s): %w", op.Path, parentIno, op.Name, err)
			}
			// Treat ENOENT as success: if a previous cleanup removed lower/ but not upper/,
			// whiteouts without a lower counterpart replay as unlinks of names that may not exist.
			if resp.GetError() != 0 && resp.GetError() != int32(syscall.ENOENT) {
				return 0, fmt.Errorf("unlink %s returned error: %d", op.Path, resp.GetError())
			}
			delete(pathInodes, op.Path)
			touchedInodes[parentIno] = true

		case OpRmdir:
			parentIno, err := resolveInode(ctx, client, volumeID, pathInodes, op.Parent)
			if err != nil {
				log.V(4).Info("resolveInode parent for rmdir (skipping)", "path", op.Path, "err", err)
				continue
			}
			resp, err := client.Rmdir(ctx, &pb.RmdirRequest{
				VolumeId:    volumeID,
				ParentInode: parentIno,
				Name:        op.Name,
			})
			if err != nil {
				return 0, fmt.Errorf("rmdir %s (parent %d, name %s): %w", op.Path, parentIno, op.Name, err)
			}
			// Treat ENOENT as success if the directory does not exist on controller.
			if resp.GetError() != 0 && resp.GetError() != int32(syscall.ENOENT) {
				return 0, fmt.Errorf("rmdir %s returned error: %d", op.Path, resp.GetError())
			}
			delete(pathInodes, op.Path)
			touchedInodes[parentIno] = true

		case OpMkdir:
			parentIno, err := resolveInode(ctx, client, volumeID, pathInodes, op.Parent)
			if err != nil {
				return 0, fmt.Errorf("resolve parent for mkdir %s: %w", op.Path, err)
			}
			mode := op.Mode
			if mode == 0 {
				mode = 0755
			}
			resp, err := client.Mkdir(ctx, &pb.MkdirRequest{
				VolumeId:    volumeID,
				ParentInode: parentIno,
				Name:        op.Name,
				Mode:        mode,
				Uid:         op.Uid,
				Gid:         op.Gid,
			})
			if err != nil {
				return 0, fmt.Errorf("mkdir %s: %w", op.Path, err)
			}
			if resp.GetError() != 0 {
				return 0, fmt.Errorf("mkdir %s returned error: %d", op.Path, resp.GetError())
			}
			newIno := resp.GetAttr().GetInode().GetIno()
			pathInodes[op.Path] = newIno
			touchedInodes[newIno] = true
			touchedInodes[parentIno] = true

		case OpCreateFile:
			parentIno, err := resolveInode(ctx, client, volumeID, pathInodes, op.Parent)
			if err != nil {
				return 0, fmt.Errorf("resolve parent for create %s: %w", op.Path, err)
			}
			mode := op.Mode
			if mode == 0 {
				mode = 0644
			}
			resp, err := client.CreateFile(ctx, &pb.CreateFileRequest{
				VolumeId:       volumeID,
				ParentInode:    parentIno,
				Name:           op.Name,
				Mode:           mode,
				Uid:            op.Uid,
				Gid:            op.Gid,
				InitialContent: op.Data,
			})
			if err != nil {
				return 0, fmt.Errorf("create file %s: %w", op.Path, err)
			}
			if resp.GetError() != 0 {
				return 0, fmt.Errorf("create file %s returned error: %d", op.Path, resp.GetError())
			}
			newIno := resp.GetAttr().GetInode().GetIno()
			pathInodes[op.Path] = newIno
			touchedInodes[newIno] = true
			touchedInodes[parentIno] = true

		case OpSymlink:
			parentIno, err := resolveInode(ctx, client, volumeID, pathInodes, op.Parent)
			if err != nil {
				return 0, fmt.Errorf("resolve parent for symlink %s: %w", op.Path, err)
			}
			resp, err := client.Symlink(ctx, &pb.SymlinkRequest{
				VolumeId:    volumeID,
				ParentInode: parentIno,
				Name:        op.Name,
				Target:      op.Target,
				Uid:         op.Uid,
				Gid:         op.Gid,
			})
			if err != nil {
				return 0, fmt.Errorf("symlink %s: %w", op.Path, err)
			}
			if resp.GetError() != 0 {
				return 0, fmt.Errorf("symlink %s returned error: %d", op.Path, resp.GetError())
			}
			newIno := resp.GetAttr().GetInode().GetIno()
			pathInodes[op.Path] = newIno
			touchedInodes[newIno] = true
			touchedInodes[parentIno] = true

		case OpTruncateFile:
			ino, err := resolveInode(ctx, client, volumeID, pathInodes, op.Path)
			if err != nil {
				return 0, fmt.Errorf("resolve inode for truncate %s: %w", op.Path, err)
			}
			resp, err := client.TruncateFile(ctx, &pb.TruncateFileRequest{
				VolumeId: volumeID,
				Inode:    ino,
				Size:     op.Size,
			})
			if err != nil {
				return 0, fmt.Errorf("truncate file %s: %w", op.Path, err)
			}
			if resp.GetError() != 0 {
				return 0, fmt.Errorf("truncate file %s returned error: %d", op.Path, resp.GetError())
			}
			touchedInodes[ino] = true

		case OpWriteFile:
			ino, err := resolveInode(ctx, client, volumeID, pathInodes, op.Path)
			if err != nil {
				return 0, fmt.Errorf("resolve inode for write %s: %w", op.Path, err)
			}
			resp, err := client.WriteFile(ctx, &pb.WriteFileRequest{
				VolumeId:  volumeID,
				Inode:     ino,
				Offset:    op.Offset,
				Data:      op.Data,
				WriteMode: pb.WriteMode_FSYNC,
			})
			if err != nil {
				return 0, fmt.Errorf("write file %s (offset %d): %w", op.Path, op.Offset, err)
			}
			if resp.GetError() != 0 {
				return 0, fmt.Errorf("write file %s (offset %d) returned error: %d", op.Path, op.Offset, resp.GetError())
			}
			touchedInodes[ino] = true

		case OpSetAttr:
			ino, err := resolveInode(ctx, client, volumeID, pathInodes, op.Path)
			if err != nil {
				log.V(4).Info("resolve inode for setAttr", "path", op.Path, "err", err)
				continue
			}
			req := &pb.SetAttrRequest{
				VolumeId: volumeID,
				Inode:    ino,
			}
			if op.Mode != 0 {
				req.Mode = proto.Uint32(op.Mode)
			}
			if op.Uid != 0 {
				req.Uid = proto.Uint32(op.Uid)
			}
			if op.Gid != 0 {
				req.Gid = proto.Uint32(op.Gid)
			}
			if !op.Mtime.IsZero() {
				req.Mtime = timestamppb.New(op.Mtime)
			}
			resp, err := client.SetAttr(ctx, req)
			if err != nil {
				return 0, fmt.Errorf("setAttr %s: %w", op.Path, err)
			}
			if resp.GetError() != 0 {
				return 0, fmt.Errorf("setAttr %s returned error: %d", op.Path, resp.GetError())
			}
			touchedInodes[ino] = true

		case OpSetXAttr:
			ino, err := resolveInode(ctx, client, volumeID, pathInodes, op.Path)
			if err != nil {
				return 0, fmt.Errorf("resolve inode for setXAttr %s: %w", op.Path, err)
			}
			resp, err := client.SetXAttr(ctx, &pb.SetXAttrRequest{
				VolumeId: volumeID,
				Inode:    ino,
				Name:     op.Name,
				Value:    op.Value,
				Flags:    pb.SetXAttrRequest_FLAGS_NONE,
			})
			if err != nil {
				return 0, fmt.Errorf("setXAttr %s (%s): %w", op.Path, op.Name, err)
			}
			if resp.GetError() != 0 {
				return 0, fmt.Errorf("setXAttr %s (%s) returned error: %d", op.Path, op.Name, resp.GetError())
			}
			touchedInodes[ino] = true

		case OpRemoveXAttr:
			ino, err := resolveInode(ctx, client, volumeID, pathInodes, op.Path)
			if err != nil {
				return 0, fmt.Errorf("resolve inode for removeXAttr %s: %w", op.Path, err)
			}
			resp, err := client.RemoveXAttr(ctx, &pb.RemoveXAttrRequest{
				VolumeId: volumeID,
				Inode:    ino,
				Name:     op.Name,
			})
			if err != nil {
				return 0, fmt.Errorf("removeXAttr %s (%s): %w", op.Path, op.Name, err)
			}
			if resp.GetError() != 0 {
				if resp.GetError() == int32(syscall.ENODATA) {
					log.V(2).Info("removeXAttr attribute already removed on inode", "path", op.Path, "name", op.Name, "inode", ino)
				} else {
					return 0, fmt.Errorf("removeXAttr %s (%s) returned error: %d", op.Path, op.Name, resp.GetError())
				}
			}
			touchedInodes[ino] = true
		}
	}

	// Always fsync root inode 1
	touchedInodes[1] = true

	// Issue blocking fsync per touched inode
	var lastStreamPos uint64
	for ino := range touchedInodes {
		fsyncResp, err := client.Fsync(ctx, &pb.FsyncRequest{
			VolumeId: volumeID,
			Inode:    ino,
			Blocking: true,
		})
		if err != nil {
			return 0, fmt.Errorf("fsync ino %d: %w", ino, err)
		}
		if fsyncResp.GetError() != 0 {
			return 0, fmt.Errorf("fsync ino %d returned error: %d", ino, fsyncResp.GetError())
		}
		if fsyncResp.GetStreamPosition() > lastStreamPos {
			lastStreamPos = fsyncResp.GetStreamPosition()
		}
	}

	return lastStreamPos, nil
}

// CheckpointManifest records state committed to the controller during previous checkpoints,
// allowing subsequent scans to be purely incremental without modifying lower/ or upper/.
type CheckpointManifest struct {
	Files      map[string]ManifestEntry `json:"files,omitempty"`
	Whiteouts  map[string]bool          `json:"whiteouts,omitempty"`
	OpaqueDirs map[string]bool          `json:"opaque_dirs,omitempty"`
}

type ManifestEntry struct {
	Size   int64             `json:"size"`
	Mtime  int64             `json:"mtime_unix_nano"`
	Xattrs map[string]string `json:"xattrs,omitempty"` // attr name -> sha256 hex of value
}

// NewCheckpointManifest creates an empty CheckpointManifest.
func NewCheckpointManifest() *CheckpointManifest {
	return &CheckpointManifest{
		Files:      make(map[string]ManifestEntry),
		Whiteouts:  make(map[string]bool),
		OpaqueDirs: make(map[string]bool),
	}
}

// LoadCheckpointManifest loads the checkpoint manifest from the given path.
func LoadCheckpointManifest(path string) (*CheckpointManifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return NewCheckpointManifest(), nil
		}
		return nil, fmt.Errorf("read checkpoint manifest %s: %w", path, err)
	}
	var m CheckpointManifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("unmarshal checkpoint manifest %s: %w", path, err)
	}
	if m.Files == nil {
		m.Files = make(map[string]ManifestEntry)
	}
	if m.Whiteouts == nil {
		m.Whiteouts = make(map[string]bool)
	}
	if m.OpaqueDirs == nil {
		m.OpaqueDirs = make(map[string]bool)
	}
	return &m, nil
}

// SaveCheckpointManifest writes the manifest atomically to path.tmp then renames it to path.
func SaveCheckpointManifest(path string, m *CheckpointManifest) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal checkpoint manifest: %w", err)
	}
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("write tmp checkpoint manifest %s: %w", tmpPath, err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("rename checkpoint manifest %s to %s: %w", tmpPath, path, err)
	}
	return nil
}

func getUpperXattrs(upperPath string) (map[string]string, error) {
	names, err := listXAttrNames(upperPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("listxattr %s: %w", upperPath, err)
	}
	if len(names) == 0 {
		return nil, nil
	}
	xattrs := make(map[string]string)
	for _, name := range names {
		if isOverlayInternalXattr(name) {
			continue
		}
		val, err := getXAttr(upperPath, name)
		if err != nil {
			return nil, fmt.Errorf("getxattr %s %s: %w", upperPath, name, err)
		}
		h := sha256.Sum256(val)
		xattrs[name] = hex.EncodeToString(h[:])
	}
	if len(xattrs) == 0 {
		return nil, nil
	}
	return xattrs, nil
}

// UpdateCheckpointManifest records the newly committed operations into the in-memory manifest.
func UpdateCheckpointManifest(m *CheckpointManifest, upperDir string, ops []FileOp) error {
	if m == nil {
		return nil
	}
	for _, op := range ops {
		switch op.Type {
		case OpUnlink, OpRmdir:
			m.Whiteouts[op.Path] = true
			delete(m.Files, op.Path)

		case OpCreateFile, OpWriteFile, OpTruncateFile, OpSetAttr:
			upperPath := filepath.Join(upperDir, op.Path)
			entry := m.Files[op.Path]
			if uInfo, err := os.Lstat(upperPath); err == nil && !isWhiteout(upperPath, uInfo) && !uInfo.IsDir() {
				entry.Size = uInfo.Size()
				entry.Mtime = uInfo.ModTime().UnixNano()
			} else {
				if op.Type == OpCreateFile {
					entry.Size = op.Size
				} else if op.Type == OpWriteFile {
					end := op.Offset + int64(len(op.Data))
					if end > entry.Size {
						entry.Size = end
					}
				} else if op.Type == OpTruncateFile {
					entry.Size = op.Size
				}
				if !op.Mtime.IsZero() {
					entry.Mtime = op.Mtime.UnixNano()
				}
			}
			xa, err := getUpperXattrs(upperPath)
			if err != nil {
				return err
			}
			entry.Xattrs = xa
			m.Files[op.Path] = entry

		case OpMkdir, OpSymlink:
			upperPath := filepath.Join(upperDir, op.Path)
			entry := m.Files[op.Path]
			entry.Size = 0
			xa, err := getUpperXattrs(upperPath)
			if err != nil {
				return err
			}
			entry.Xattrs = xa
			m.Files[op.Path] = entry

		case OpSetXAttr, OpRemoveXAttr:
			upperPath := filepath.Join(upperDir, op.Path)
			entry := m.Files[op.Path]
			xa, err := getUpperXattrs(upperPath)
			if err != nil {
				return err
			}
			entry.Xattrs = xa
			m.Files[op.Path] = entry
		}
	}
	return nil
}
