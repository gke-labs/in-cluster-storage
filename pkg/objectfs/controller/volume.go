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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/erofs"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/blob"
	"github.com/gke-labs/in-cluster-storage/pkg/wal"
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const MetadataFileName = ".objectfs-metadata.json"

type VolumeMetadata struct {
	VolumeID    string                  `json:"volume_id"`
	Version     int                     `json:"version"`
	LastFlushed time.Time               `json:"last_flushed"`
	NextInode   uint64                  `json:"next_inode"`
	Entries     map[string]FileMetadata `json:"entries"`
}

type FileMetadata struct {
	Inode   uint64    `json:"inode"`
	Path    string    `json:"path"`
	Name    string    `json:"name"`
	IsDir   bool      `json:"is_dir"`
	Mode    uint32    `json:"mode"`
	Size    int64     `json:"size"`
	ModTime time.Time `json:"mod_time"`
	Sha256  string    `json:"sha256,omitempty"`
	ETag    string    `json:"etag,omitempty"`
}

// metadataResolver provides unified read methods for resolving inodes and directories
// across active in-memory state/caches, local eviction storage, and immutable base EROFS snapshots.
type metadataResolver struct {
	localStore     *LocalStorage
	snapshotReader *erofs.Reader
	snapshotRaw    io.ReaderAt
	inodeCache     *LRUCache[uint64, *CachedInode]
	dirCache       *LRUCache[uint64, *CachedDir]

	// dirtyInodes maps inode IDs to their latest eviction offset in LocalStorage for inodes
	// that have changed since the last EROFS snapshot. Additional unflushed modifications
	// may also reside in inodeCache.
	dirtyInodes map[uint64]LocalOffset

	// dirtyDirs maps directory inode IDs to their latest delta eviction offset in LocalStorage
	// for directories that have changed since the last EROFS snapshot. Additional unflushed
	// delta modifications may also reside in dirCache.
	dirtyDirs map[uint64]LocalOffset
}

type Volume struct {
	metadataResolver

	mu           sync.RWMutex
	volumeID     string
	rootInodeID  uint64
	nextInode    uint64
	backend      ObjectStorageBackend
	blobStore    *blob.Store
	broadcaster  *EventBroadcaster
	maxInlineLen int64

	stream     walclient.Stream
	durability walclient.Level
	streamID   uuid.UUID

	localStorageDir string

	snapshotCutoff LocalOffset
	snapshotMu     sync.Mutex

	// Snapshot trigger limits
	maxBufferFiles   int
	maxDirtyRecords  int
	maxLocalFileSize int64

	lastFlushedMetadata    *VolumeMetadata
	deletedPathsSinceFlush []string
}

// VolumeOption configures a Volume instance.
type VolumeOption func(*Volume)

// WithStream sets the WAL stream for metadata change-logging.
func WithStream(stream walclient.Stream) VolumeOption {
	return func(v *Volume) {
		v.stream = stream
	}
}

// WithDurability sets the default durability level for metadata changes.
func WithDurability(level walclient.Level) VolumeOption {
	return func(v *Volume) {
		v.durability = level
	}
}

// WithStreamID sets an explicit Stream UUID for this volume.
func WithStreamID(id uuid.UUID) VolumeOption {
	return func(v *Volume) {
		v.streamID = id
	}
}

// WithMaxRAMEntries sets the maximum number of inodes and directories kept in RAM.
func WithMaxRAMEntries(maxInodes, maxDirs int) VolumeOption {
	return func(v *Volume) {
		if maxInodes > 0 && v.inodeCache != nil {
			v.inodeCache.capacity = maxInodes
		}
		if maxDirs > 0 && v.dirCache != nil {
			v.dirCache.capacity = maxDirs
		}
	}
}

// WithLocalStorageDir sets the local directory for evicted metadata storage.
func WithLocalStorageDir(dir string) VolumeOption {
	return func(v *Volume) {
		v.localStorageDir = dir
	}
}

// WithMaxBufferFiles sets the maximum number of buffer files before auto-triggering a snapshot.
func WithMaxBufferFiles(count int) VolumeOption {
	return func(v *Volume) {
		v.maxBufferFiles = count
	}
}

// WithSnapshotThreshold sets limits before auto-triggering a snapshot.
func WithSnapshotThreshold(maxDirtyRecords int, maxFileSize int64) VolumeOption {
	return func(v *Volume) {
		v.maxDirtyRecords = maxDirtyRecords
		v.maxLocalFileSize = maxFileSize
	}
}

// StreamIDForVolume generates a deterministic UUID for a given volume ID.
func StreamIDForVolume(volumeID string) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("objectfs:"+volumeID))
}

func NewVolume(volumeID string, backend ObjectStorageBackend, broadcaster *EventBroadcaster, opts ...VolumeOption) *Volume {
	var blobStore *blob.Store
	if backend != nil {
		blobStore = blob.NewStore(backend, 0)
	}

	v := &Volume{
		metadataResolver: metadataResolver{
			dirtyInodes: make(map[uint64]LocalOffset),
			dirtyDirs:   make(map[uint64]LocalOffset),
		},
		volumeID:         volumeID,
		rootInodeID:      1,
		nextInode:        2,
		backend:          backend,
		blobStore:        blobStore,
		broadcaster:      broadcaster,
		maxInlineLen:     4 * 1024 * 1024, // 4MB default inline threshold
		durability:       walclient.Local,
		streamID:         StreamIDForVolume(volumeID),
		maxBufferFiles:   4,
		maxDirtyRecords:  100000,
		maxLocalFileSize: 250 * 1024 * 1024,
	}

	v.inodeCache = NewLRUCache[uint64, *CachedInode](10000, v.onEvictInode)
	v.dirCache = NewLRUCache[uint64, *CachedDir](2000, v.onEvictDir)

	for _, opt := range opts {
		opt(v)
	}

	if v.localStorageDir == "" {
		v.localStorageDir = path.Join(os.TempDir(), fmt.Sprintf("objectfs-local-%s-%d", volumeID, time.Now().UnixNano()))
	}
	ls, err := NewLocalStorage(v.localStorageDir, WithMaxLocalFileSize(v.maxLocalFileSize))
	if err == nil {
		v.localStore = ls
	}

	// Always initialize an initial base EROFS snapshot with empty root directory
	rootNode := erofs.NewMemoryNode("", true, 0755, nil, nil, erofs.WithMtime(uint64(time.Now().Unix())))
	var initialErofsBuf bufferWriterAt
	if err := erofs.WriteImage(&initialErofsBuf, rootNode); err == nil {
		v.snapshotRaw = bytes.NewReader(initialErofsBuf.buf)
		if r, err := erofs.NewReader(v.snapshotRaw); err == nil {
			v.snapshotReader = r
			v.rootInodeID = r.GetRootNID()
			v.nextInode = v.rootInodeID + 100
		}
	}

	return v
}

// persistInode uploads the inode's blob payload if dirty and writes the inode metadata record to local eviction storage.
func (v *Volume) persistInode(ctx context.Context, node *CachedInode) error {
	if !node.IsDirty {
		return nil
	}

	if node.Data != nil && node.Sha256 != "" && v.blobStore != nil {
		if err := node.Data.Rewind(); err != nil {
			return fmt.Errorf("failed to rewind node data: %w", err)
		}
		if err := v.blobStore.PutBlobs(ctx, map[string]blob.ByteStream{node.Sha256: node.Data}); err != nil {
			return fmt.Errorf("failed to persist inode %d blob to blobStore: %w", node.ID, err)
		}
		_ = node.Data.Close()
		node.Data = nil
	}

	if v.localStore != nil {
		rec := &InodeRecord{
			InodeID: node.ID,
			Mode:    node.Mode,
			Size:    node.Size,
			ModTime: node.ModTime,
			IsDir:   node.IsDir,
			Sha256:  node.Sha256,
			ETag:    node.ETag,
		}
		payload, err := EncodeInodeRecord(rec)
		if err != nil {
			return fmt.Errorf("failed to encode inode record %d: %w", node.ID, err)
		}
		off, err := v.localStore.WriteRecord(RecordTypeInode, payload)
		if err != nil {
			return fmt.Errorf("failed to write inode record %d to local storage: %w", node.ID, err)
		}
		v.dirtyInodes[node.ID] = off
	}

	node.IsDirty = false
	return nil
}

func (v *Volume) onEvictInode(inodeID uint64, node *CachedInode) {
	_ = v.persistInode(context.Background(), node)
}

func (v *Volume) onEvictDir(dirID uint64, dir *CachedDir) {
	if !dir.IsDirty || v.localStore == nil {
		return
	}
	if len(dir.Added) == 0 && len(dir.Deleted) == 0 && dir.PrevOffset != NoOffset {
		return
	}
	deletedList := make([]string, 0, len(dir.Deleted))
	for del := range dir.Deleted {
		deletedList = append(deletedList, del)
	}
	addedList := make([]DirEntry, 0, len(dir.Added))
	for name := range dir.Added {
		if entry, ok := dir.Entries[name]; ok {
			addedList = append(addedList, entry)
		}
	}
	rec := &DirDeltaRecord{
		InodeID:    dir.ID,
		PrevOffset: dir.PrevOffset,
		Deleted:    deletedList,
		Added:      addedList,
	}
	payload, err := EncodeDirDeltaRecord(rec)
	if err != nil {
		return
	}
	off, err := v.localStore.WriteRecord(RecordTypeDirDelta, payload)
	if err == nil {
		v.dirtyDirs[dir.ID] = off
		dir.PrevOffset = off
		dir.Added = make(map[string]bool)
		dir.Deleted = make(map[string]bool)
		dir.IsDirty = false
	}
}

// VolumeID returns the volume identifier.
func (v *Volume) VolumeID() string {
	return v.volumeID
}

// Stream returns the configured WAL stream, or nil.
func (v *Volume) Stream() walclient.Stream {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.stream
}

// StreamID returns the stream UUID associated with this volume.
func (v *Volume) StreamID() uuid.UUID {
	return v.streamID
}

// Durability returns the default durability level configured on this volume.
func (v *Volume) Durability() walclient.Level {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.durability
}

// Close closes the volume, local storage, and any underlying WAL streams.
func (v *Volume) Close() error {
	v.mu.Lock()
	defer v.mu.Unlock()

	var firstErr error
	if v.localStore != nil {
		if err := v.localStore.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		_ = os.RemoveAll(v.localStorageDir)
	}
	if v.stream != nil {
		if err := v.stream.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// logMutationLocked appends a mutation record to the WAL stream under v.mu.
// It returns a wait function that blocks until the requested durability level is reached.
// The wait function MUST be invoked outside v.mu so that WAL durability waits do not
// block concurrent volume operations.
//
// If the durability wait fails (e.g. context cancelled, stream closed), the mutation
// has already been applied to in-memory state and appended locally, so no rollback is
// attempted; the error is returned to inform the caller that durability was not achieved.
func (v *Volume) logMutationLocked(ctx context.Context, record *MutationRecord, reqLevel *walclient.Level) (func(context.Context) error, error) {
	if v.stream == nil {
		return nil, nil
	}
	payload, err := EncodeMutationRecord(record)
	if err != nil {
		return nil, fmt.Errorf("failed to encode mutation record: %w", err)
	}
	seq, err := v.stream.Append(ctx, payload)
	if err != nil {
		return nil, fmt.Errorf("failed to append mutation to WAL stream: %w", err)
	}
	record.StreamSeq = seq

	durability := v.durability
	if reqLevel != nil {
		durability = *reqLevel
	}

	waitFn := func(waitCtx context.Context) error {
		switch durability {
		case walclient.Permanent:
			if err := v.stream.Wait(waitCtx, seq, walclient.Permanent, true); err != nil {
				return fmt.Errorf("failed waiting for permanent durability: %w", err)
			}
		case walclient.Witness:
			if err := v.stream.Wait(waitCtx, seq, walclient.Witness, false); err != nil {
				return fmt.Errorf("failed waiting for witness durability: %w", err)
			}
		case walclient.Local:
			// Append already fsynced locally
		}
		return nil
	}
	return waitFn, nil
}

func (v *Volume) allocInode() uint64 {
	return atomic.AddUint64(&v.nextInode, 1) - 1
}

func cleanPath(p string) string {
	cleaned := path.Clean("/" + p)
	if !strings.HasPrefix(cleaned, "/") {
		cleaned = "/" + cleaned
	}
	return cleaned
}

func (r *metadataResolver) resolveInode(ctx context.Context, inodeID uint64, populateCache bool) (*CachedInode, error) {
	if r.inodeCache != nil {
		if populateCache {
			if node, ok := r.inodeCache.Get(inodeID); ok {
				return node, nil
			}
		} else {
			if node, ok := r.inodeCache.Peek(inodeID); ok {
				return node, nil
			}
		}
	}

	// 1. Check if evicted to local storage
	if off, isDirty := r.dirtyInodes[inodeID]; isDirty && off != NoOffset && r.localStore != nil {
		_, payload, err := r.localStore.ReadRecord(off)
		if err == nil {
			rec, err := DecodeInodeRecord(payload)
			if err == nil {
				node := &CachedInode{
					ID:      rec.InodeID,
					Mode:    rec.Mode,
					Size:    rec.Size,
					ModTime: rec.ModTime,
					IsDir:   rec.IsDir,
					Sha256:  rec.Sha256,
					ETag:    rec.ETag,
					IsDirty: populateCache,
				}
				if populateCache && r.inodeCache != nil {
					r.inodeCache.Put(inodeID, node)
				}
				return node, nil
			}
		}
	}

	// 2. Fetch from base snapshot
	if r.snapshotReader != nil && r.snapshotRaw != nil {
		erofsInode, err := erofs.ReadInode(r.snapshotRaw, r.snapshotReader.Superblock(), inodeID)
		if err == nil {
			var shaStr string
			xattrs, xErr := r.snapshotReader.GetXattrs(inodeID)
			if xErr == nil && !xattrs.IsEmpty() {
				if xattrs.UserDigest != "" {
					shaStr = xattrs.UserDigest
				} else if xattrs.UserSHA256 != "" {
					shaStr = xattrs.UserSHA256
				}
			}

			isDir := (erofsInode.Mode & erofs.S_IFMT) == erofs.S_IFDIR
			mode := uint32(erofsInode.Mode)
			if isDir {
				mode |= syscall.S_IFDIR
			} else {
				mode |= syscall.S_IFREG
			}

			mtime := time.Unix(int64(erofsInode.Mtime), int64(erofsInode.MtimeNsec))
			if erofsInode.Mtime == 0 {
				mtime = time.Now()
			}

			node := &CachedInode{
				ID:      inodeID,
				Mode:    mode,
				Size:    int64(erofsInode.Size),
				ModTime: mtime,
				IsDir:   isDir,
				Sha256:  shaStr,
				IsDirty: false,
			}
			if populateCache && r.inodeCache != nil {
				r.inodeCache.Put(inodeID, node)
			}
			return node, nil
		}
	}

	return nil, fmt.Errorf("inode %d not found: %w", inodeID, syscall.ENOENT)
}

func (r *metadataResolver) resolveDir(ctx context.Context, inodeID uint64, populateCache bool) (*CachedDir, error) {
	if r.dirCache != nil {
		if populateCache {
			if dir, ok := r.dirCache.Get(inodeID); ok {
				return dir, nil
			}
		} else {
			if cached, ok := r.dirCache.Peek(inodeID); ok {
				entriesCopy := make(map[string]DirEntry, len(cached.Entries))
				for k, v := range cached.Entries {
					entriesCopy[k] = v
				}
				return &CachedDir{
					ID:         cached.ID,
					Entries:    entriesCopy,
					Added:      make(map[string]bool),
					Deleted:    make(map[string]bool),
					PrevOffset: cached.PrevOffset,
					IsDirty:    false,
				}, nil
			}
		}
	}

	entries := make(map[string]DirEntry)

	// 1. Check if dirty delta exists in local storage
	if off, isDirty := r.dirtyDirs[inodeID]; isDirty && off != NoOffset && r.localStore != nil {
		var deltas []*DirDeltaRecord
		currOff := off
		visited := make(map[LocalOffset]bool)
		for currOff != NoOffset && !visited[currOff] {
			visited[currOff] = true
			_, payload, err := r.localStore.ReadRecord(currOff)
			if err != nil {
				break
			}
			rec, err := DecodeDirDeltaRecord(payload)
			if err != nil {
				break
			}
			deltas = append(deltas, rec)
			currOff = rec.PrevOffset
		}

		// Read base from base snapshot if present
		if r.snapshotReader != nil {
			dirents, err := r.snapshotReader.ListDirectory(inodeID)
			if err == nil {
				for _, de := range dirents {
					if de.Name == "." || de.Name == ".." {
						continue
					}
					isDir := de.FileType == erofs.FTDir
					mode := uint32(0644 | syscall.S_IFREG)
					if isDir {
						mode = uint32(0755 | syscall.S_IFDIR)
					}
					entries[de.Name] = DirEntry{
						Name:    de.Name,
						InodeID: de.NID,
						IsDir:   isDir,
						Mode:    mode,
					}
				}
			}
		}

		// Replay deltas in chronological order (oldest to newest)
		for i := len(deltas) - 1; i >= 0; i-- {
			d := deltas[i]
			for _, del := range d.Deleted {
				delete(entries, del)
			}
			for _, add := range d.Added {
				entries[add.Name] = add
			}
		}

		dir := &CachedDir{
			ID:         inodeID,
			Entries:    entries,
			Added:      make(map[string]bool),
			Deleted:    make(map[string]bool),
			PrevOffset: off,
			IsDirty:    false,
		}
		if populateCache && r.dirCache != nil {
			r.dirCache.Put(inodeID, dir)
		}
		return dir, nil
	}

	// 2. Fetch clean directory from snapshot
	if r.snapshotReader != nil {
		dirents, err := r.snapshotReader.ListDirectory(inodeID)
		if err == nil {
			for _, de := range dirents {
				if de.Name == "." || de.Name == ".." {
					continue
				}
				isDir := de.FileType == erofs.FTDir
				mode := uint32(0644 | syscall.S_IFREG)
				if isDir {
					mode = uint32(0755 | syscall.S_IFDIR)
				}
				entries[de.Name] = DirEntry{
					Name:    de.Name,
					InodeID: de.NID,
					IsDir:   isDir,
					Mode:    mode,
				}
			}
			dir := &CachedDir{
				ID:         inodeID,
				Entries:    entries,
				Added:      make(map[string]bool),
				Deleted:    make(map[string]bool),
				PrevOffset: NoOffset,
				IsDirty:    false,
			}
			if populateCache && r.dirCache != nil {
				r.dirCache.Put(inodeID, dir)
			}
			return dir, nil
		}
	}

	return nil, fmt.Errorf("directory inode %d not found: %w", inodeID, syscall.ENOENT)
}

func (v *Volume) getOrLoadInodeLocked(ctx context.Context, inodeID uint64) (*CachedInode, error) {
	return v.resolveInode(ctx, inodeID, true)
}

func (v *Volume) getOrLoadDirLocked(ctx context.Context, inodeID uint64) (*CachedDir, error) {
	return v.resolveDir(ctx, inodeID, true)
}

func (v *Volume) resolvePathLocked(ctx context.Context, p string) (uint64, uint64, string, error) {
	p = cleanPath(p)
	if p == "/" {
		return v.rootInodeID, 0, "", nil
	}
	parts := strings.Split(strings.Trim(p, "/"), "/")
	currInodeID := v.rootInodeID
	var parentInodeID uint64
	var baseName string
	for i, part := range parts {
		dir, err := v.getOrLoadDirLocked(ctx, currInodeID)
		if err != nil {
			return 0, 0, "", fmt.Errorf("directory for inode %d not found: %w", currInodeID, syscall.ENOENT)
		}
		entry, exists := dir.Entries[part]
		if !exists {
			return 0, 0, "", fmt.Errorf("path component %s not found: %w", part, syscall.ENOENT)
		}
		if i == len(parts)-1 {
			return entry.InodeID, currInodeID, part, nil
		}
		if !entry.IsDir {
			return 0, 0, "", fmt.Errorf("path component %s is not a directory: %w", part, syscall.ENOTDIR)
		}
		parentInodeID = currInodeID
		baseName = part
		currInodeID = entry.InodeID
	}
	return currInodeID, parentInodeID, baseName, nil
}

func (v *Volume) findOrCreateDirParentsLocked(ctx context.Context, p string) (uint64, error) {
	p = cleanPath(p)
	if p == "/" {
		return v.rootInodeID, nil
	}
	parts := strings.Split(strings.Trim(p, "/"), "/")
	currInodeID := v.rootInodeID
	for _, part := range parts {
		dir, err := v.getOrLoadDirLocked(ctx, currInodeID)
		if err != nil {
			return 0, err
		}
		entry, exists := dir.Entries[part]
		if !exists {
			childInodeID := v.allocInode()
			now := time.Now()
			childInode := &CachedInode{
				ID:      childInodeID,
				Mode:    0755 | syscall.S_IFDIR,
				ModTime: now,
				IsDir:   true,
				IsDirty: true,
			}

			newEntry := DirEntry{
				Name:    part,
				InodeID: childInodeID,
				IsDir:   true,
				Mode:    0755 | syscall.S_IFDIR,
			}
			dir.Entries[part] = newEntry
			dir.Added[part] = true
			delete(dir.Deleted, part)
			dir.IsDirty = true

			v.inodeCache.Put(childInodeID, childInode)

			childDir := &CachedDir{
				ID:         childInodeID,
				Entries:    make(map[string]DirEntry),
				Added:      make(map[string]bool),
				Deleted:    make(map[string]bool),
				PrevOffset: NoOffset,
				IsDirty:    true,
			}
			v.dirCache.Put(childInodeID, childDir)
			currInodeID = childInodeID
		} else {
			if !entry.IsDir {
				return 0, fmt.Errorf("path component %s is not a directory: %w", part, syscall.ENOTDIR)
			}
			currInodeID = entry.InodeID
		}
	}
	return currInodeID, nil
}

func (v *Volume) toEntryAttrLocked(ctx context.Context, inodeID uint64, fullPath, name string) (*pb.EntryAttr, error) {
	node, err := v.getOrLoadInodeLocked(ctx, inodeID)
	if err != nil {
		return nil, err
	}
	return &pb.EntryAttr{
		Inode:       node.ID,
		Path:        fullPath,
		Name:        name,
		IsDir:       node.IsDir,
		Size:        node.Size,
		Mode:        node.Mode,
		ModTime:     timestamppb.New(node.ModTime),
		Sha256:      node.Sha256,
		RedirectUrl: node.RedirectURL,
	}, nil
}

func (v *Volume) GetAttr(ctx context.Context, p string) (*pb.EntryAttr, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	p = cleanPath(p)
	inodeID, _, baseName, err := v.resolvePathLocked(ctx, p)
	if err != nil {
		return nil, err
	}
	if p == "/" {
		baseName = "/"
	}

	return v.toEntryAttrLocked(ctx, inodeID, p, baseName)
}

func (v *Volume) Lookup(ctx context.Context, parentPath, name string) (*pb.EntryAttr, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	parentPath = cleanPath(parentPath)
	parentInodeID, _, _, err := v.resolvePathLocked(ctx, parentPath)
	if err != nil {
		return nil, err
	}

	parentDir, err := v.getOrLoadDirLocked(ctx, parentInodeID)
	if err != nil {
		return nil, err
	}

	entry, ok := parentDir.Entries[name]
	if !ok {
		return nil, fmt.Errorf("child %s not found in %s: %w", name, parentPath, syscall.ENOENT)
	}

	fullPath := path.Join(parentPath, name)
	return v.toEntryAttrLocked(ctx, entry.InodeID, fullPath, name)
}

func (v *Volume) ReadDir(ctx context.Context, p string) ([]*pb.EntryAttr, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	p = cleanPath(p)
	dirInodeID, _, _, err := v.resolvePathLocked(ctx, p)
	if err != nil {
		return nil, err
	}

	dirNode, err := v.getOrLoadInodeLocked(ctx, dirInodeID)
	if err != nil {
		return nil, err
	}
	if !dirNode.IsDir {
		return nil, fmt.Errorf("path %s is not a directory: %w", p, syscall.ENOTDIR)
	}

	dir, err := v.getOrLoadDirLocked(ctx, dirInodeID)
	if err != nil {
		return nil, err
	}

	var entries []*pb.EntryAttr
	for name, entry := range dir.Entries {
		childPath := path.Join(p, name)
		attr, err := v.toEntryAttrLocked(ctx, entry.InodeID, childPath, name)
		if err == nil {
			entries = append(entries, attr)
		}
	}
	return entries, nil
}

func (v *Volume) Mkdir(ctx context.Context, p string, mode uint32) (*pb.EntryAttr, error) {
	attr, waitFn, err := func() (*pb.EntryAttr, func(context.Context) error, error) {
		v.mu.Lock()
		defer v.mu.Unlock()

		p = cleanPath(p)
		if p == "/" {
			return nil, nil, fmt.Errorf("cannot recreate root directory: %w", syscall.EEXIST)
		}

		parentPath := path.Dir(p)
		baseName := path.Base(p)

		parentInodeID, _, _, err := v.resolvePathLocked(ctx, parentPath)
		if err != nil {
			return nil, nil, fmt.Errorf("parent directory not found: %w", err)
		}

		parentInode, err := v.getOrLoadInodeLocked(ctx, parentInodeID)
		if err != nil {
			return nil, nil, err
		}
		if !parentInode.IsDir {
			return nil, nil, fmt.Errorf("parent %s is not a directory: %w", parentPath, syscall.ENOTDIR)
		}

		parentDir, err := v.getOrLoadDirLocked(ctx, parentInodeID)
		if err != nil {
			return nil, nil, err
		}

		if _, exists := parentDir.Entries[baseName]; exists {
			return nil, nil, fmt.Errorf("directory %s already exists: %w", p, syscall.EEXIST)
		}

		if mode == 0 {
			mode = 0755
		}
		mode |= syscall.S_IFDIR

		now := time.Now()
		childInodeID := v.allocInode()

		newEntry := DirEntry{
			Name:    baseName,
			InodeID: childInodeID,
			IsDir:   true,
			Mode:    mode,
		}
		parentDir.Entries[baseName] = newEntry
		parentDir.Added[baseName] = true
		delete(parentDir.Deleted, baseName)
		parentDir.IsDirty = true
		parentInode.ModTime = now
		parentInode.IsDirty = true

		childInode := &CachedInode{
			ID:      childInodeID,
			Mode:    mode,
			ModTime: now,
			IsDir:   true,
			IsDirty: true,
		}
		v.inodeCache.Put(childInodeID, childInode)

		childDir := &CachedDir{
			ID:         childInodeID,
			Entries:    make(map[string]DirEntry),
			Added:      make(map[string]bool),
			Deleted:    make(map[string]bool),
			PrevOffset: NoOffset,
			IsDirty:    true,
		}
		v.dirCache.Put(childInodeID, childDir)

		rec := &MutationRecord{
			Type:     MutationMkdir,
			VolumeId: v.volumeID,
			Path:     p,
			Mode:     mode,
			ModTime:  timestamppb.New(now),
			Inode:    childInodeID,
		}
		waitFn, err := v.logMutationLocked(ctx, rec, nil)
		if err != nil {
			delete(parentDir.Entries, baseName)
			delete(parentDir.Added, baseName)
			return nil, nil, err
		}

		attr := &pb.EntryAttr{
			Inode:   childInodeID,
			Path:    p,
			Name:    baseName,
			IsDir:   true,
			Size:    0,
			Mode:    mode,
			ModTime: timestamppb.New(now),
		}
		v.broadcaster.Broadcast(v.volumeID, &pb.WatchVolumeResponse{
			EventType: pb.WatchEventType_EVENT_CREATED,
			Path:      p,
			Attr:      attr,
		})

		v.checkAutoSnapshotTriggerLocked(ctx)
		return attr, waitFn, nil
	}()
	if err != nil {
		return nil, err
	}
	if waitFn != nil {
		if err := waitFn(ctx); err != nil {
			return nil, err
		}
	}
	return attr, nil
}

func (v *Volume) CreateFile(ctx context.Context, p string, mode uint32, initialContent []byte) (*pb.EntryAttr, error) {
	attr, waitFn, err := func() (*pb.EntryAttr, func(context.Context) error, error) {
		v.mu.Lock()
		defer v.mu.Unlock()

		p = cleanPath(p)
		if p == "/" {
			return nil, nil, fmt.Errorf("cannot create file at root: %w", syscall.EISDIR)
		}

		parentPath := path.Dir(p)
		baseName := path.Base(p)

		parentInodeID, _, _, err := v.resolvePathLocked(ctx, parentPath)
		if err != nil {
			return nil, nil, fmt.Errorf("parent directory not found: %w", err)
		}

		parentInode, err := v.getOrLoadInodeLocked(ctx, parentInodeID)
		if err != nil {
			return nil, nil, err
		}
		if !parentInode.IsDir {
			return nil, nil, fmt.Errorf("parent %s is not a directory: %w", parentPath, syscall.ENOTDIR)
		}

		parentDir, err := v.getOrLoadDirLocked(ctx, parentInodeID)
		if err != nil {
			return nil, nil, err
		}

		if mode == 0 {
			mode = 0644
		}
		mode |= syscall.S_IFREG

		now := time.Now()
		var hashStr string
		if len(initialContent) > 0 {
			h := sha256.Sum256(initialContent)
			hashStr = fmt.Sprintf("%x", h)
		}

		dataCopy := make([]byte, len(initialContent))
		copy(dataCopy, initialContent)

		var stream blob.ByteStream
		if len(dataCopy) > 0 {
			stream = blob.NewByteStreamFromBytes(dataCopy)
		}

		entry, exists := parentDir.Entries[baseName]
		if exists {
			childInode, err := v.getOrLoadInodeLocked(ctx, entry.InodeID)
			if err != nil {
				return nil, nil, err
			}
			if childInode.IsDir {
				return nil, nil, fmt.Errorf("cannot overwrite directory with file: %w", syscall.EISDIR)
			}
			if childInode.Data != nil {
				_ = childInode.Data.Close()
			}
			childInode.Mode = mode
			childInode.Size = int64(len(dataCopy))
			childInode.Data = stream
			childInode.ModTime = now
			childInode.Sha256 = hashStr
			childInode.IsDirty = true

			rec := &MutationRecord{
				Type:     MutationCreateFile,
				VolumeId: v.volumeID,
				Path:     p,
				Mode:     mode,
				Size:     int64(len(dataCopy)),
				ModTime:  timestamppb.New(now),
				Sha256:   hashStr,
				Inode:    childInode.ID,
				Data:     dataCopy,
			}
			waitFn, err := v.logMutationLocked(ctx, rec, nil)
			if err != nil {
				return nil, nil, err
			}

			attr := &pb.EntryAttr{
				Inode:   childInode.ID,
				Path:    p,
				Name:    baseName,
				IsDir:   false,
				Size:    childInode.Size,
				Mode:    childInode.Mode,
				ModTime: timestamppb.New(now),
				Sha256:  hashStr,
			}
			v.broadcaster.Broadcast(v.volumeID, &pb.WatchVolumeResponse{
				EventType: pb.WatchEventType_EVENT_MODIFIED,
				Path:      p,
				Attr:      attr,
			})
			v.checkAutoSnapshotTriggerLocked(ctx)
			return attr, waitFn, nil
		}

		childInodeID := v.allocInode()
		newEntry := DirEntry{
			Name:    baseName,
			InodeID: childInodeID,
			IsDir:   false,
			Mode:    mode,
		}
		parentDir.Entries[baseName] = newEntry
		parentDir.Added[baseName] = true
		delete(parentDir.Deleted, baseName)
		parentDir.IsDirty = true
		parentInode.ModTime = now
		parentInode.IsDirty = true

		childInode := &CachedInode{
			ID:      childInodeID,
			Mode:    mode,
			Size:    int64(len(dataCopy)),
			ModTime: now,
			Data:    stream,
			Sha256:  hashStr,
			IsDir:   false,
			IsDirty: true,
		}
		v.inodeCache.Put(childInodeID, childInode)

		rec := &MutationRecord{
			Type:     MutationCreateFile,
			VolumeId: v.volumeID,
			Path:     p,
			Mode:     mode,
			Size:     int64(len(dataCopy)),
			ModTime:  timestamppb.New(now),
			Sha256:   hashStr,
			Inode:    childInodeID,
			Data:     dataCopy,
		}
		waitFn, err := v.logMutationLocked(ctx, rec, nil)
		if err != nil {
			delete(parentDir.Entries, baseName)
			delete(parentDir.Added, baseName)
			return nil, nil, err
		}

		attr := &pb.EntryAttr{
			Inode:   childInodeID,
			Path:    p,
			Name:    baseName,
			IsDir:   false,
			Size:    int64(len(dataCopy)),
			Mode:    mode,
			ModTime: timestamppb.New(now),
			Sha256:  hashStr,
		}
		v.broadcaster.Broadcast(v.volumeID, &pb.WatchVolumeResponse{
			EventType: pb.WatchEventType_EVENT_CREATED,
			Path:      p,
			Attr:      attr,
		})

		v.checkAutoSnapshotTriggerLocked(ctx)
		return attr, waitFn, nil
	}()
	if err != nil {
		return nil, err
	}
	if waitFn != nil {
		if err := waitFn(ctx); err != nil {
			return nil, err
		}
	}
	return attr, nil
}

func (v *Volume) ReadFile(ctx context.Context, p string, offset, length int64) ([]byte, int64, string, error) {
	v.mu.RLock()
	p = cleanPath(p)
	inodeID, _, _, err := v.resolvePathLocked(ctx, p)
	if err != nil {
		v.mu.RUnlock()
		return nil, 0, "", err
	}

	node, err := v.getOrLoadInodeLocked(ctx, inodeID)
	v.mu.RUnlock()
	if err != nil {
		return nil, 0, "", err
	}

	if node.IsDir {
		return nil, 0, "", fmt.Errorf("cannot read directory as file: %w", syscall.EISDIR)
	}

	// Lazy load data from blob store / backend if not currently in memory
	if node.Data == nil && node.Size > 0 {
		if node.Sha256 != "" && v.blobStore != nil {
			stream, err := v.blobStore.GetBlob(ctx, node.Sha256)
			if err == nil {
				node.Data = stream
			}
		}
		if node.Data == nil && v.backend != nil {
			var legacyBuf bytes.Buffer
			err := v.backend.GetObject(ctx, v.volumeID, strings.TrimPrefix(p, "/"), 0, node.Size, &legacyBuf)
			if err == nil {
				node.Data = blob.NewByteStreamFromBytes(legacyBuf.Bytes())
			}
		}
	}

	total := node.Size
	if node.RedirectURL != "" && length > v.maxInlineLen {
		return nil, total, node.RedirectURL, nil
	}

	if offset >= total || node.Data == nil {
		return []byte{}, total, "", nil
	}

	end := offset + length
	if length <= 0 || end > total {
		end = total
	}
	readLen := end - offset

	if _, err := node.Data.Seek(offset, io.SeekStart); err != nil {
		return nil, 0, "", fmt.Errorf("failed to seek node data: %w", err)
	}

	res := make([]byte, readLen)
	n, err := io.ReadFull(node.Data, res)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return nil, 0, "", fmt.Errorf("failed to read node data: %w", err)
	}
	return res[:n], total, "", nil
}

func (v *Volume) WriteFile(ctx context.Context, p string, offset int64, data []byte, writeMode pb.WriteMode) (int64, int64, time.Time, error) {
	nWritten, newSize, modTime, waitFn, err := func() (int64, int64, time.Time, func(context.Context) error, error) {
		v.mu.Lock()
		defer v.mu.Unlock()

		p = cleanPath(p)
		inodeID, _, baseName, err := v.resolvePathLocked(ctx, p)
		if err != nil {
			return 0, 0, time.Time{}, nil, err
		}

		node, err := v.getOrLoadInodeLocked(ctx, inodeID)
		if err != nil {
			return 0, 0, time.Time{}, nil, err
		}

		if node.IsDir {
			return 0, 0, time.Time{}, nil, fmt.Errorf("cannot write to directory: %w", syscall.EISDIR)
		}

		var currentData []byte
		if node.Data != nil {
			_ = node.Data.Rewind()
			currentData, _ = io.ReadAll(node.Data)
			_ = node.Data.Close()
		}

		neededLen := offset + int64(len(data))
		if neededLen > int64(len(currentData)) {
			newBuf := make([]byte, neededLen)
			copy(newBuf, currentData)
			currentData = newBuf
		}
		copy(currentData[offset:], data)
		node.Size = int64(len(currentData))
		now := time.Now()
		node.ModTime = now
		node.IsDirty = true

		h := sha256.Sum256(currentData)
		node.Sha256 = fmt.Sprintf("%x", h)

		if node.Size <= blob.MemoryThreshold {
			node.Data = blob.NewByteStreamFromBytes(currentData)
		} else {
			tf, err := os.CreateTemp("", "objectfs-node-*")
			if err == nil {
				_, _ = tf.Write(currentData)
				_, _ = tf.Seek(0, io.SeekStart)
				node.Data = blob.NewByteStreamFromFile(tf, int64(len(currentData)), true)
			} else {
				node.Data = blob.NewByteStreamFromBytes(currentData)
			}
		}

		var reqLevel *walclient.Level
		switch writeMode {
		case pb.WriteMode_WRITE_THROUGH_FSYNC:
			l := walclient.Permanent
			reqLevel = &l
		case pb.WriteMode_EAGER_REPLICATION:
			l := walclient.Witness
			reqLevel = &l
		case pb.WriteMode_LAZY_WRITE:
			l := walclient.Local
			reqLevel = &l
		}

		rec := &MutationRecord{
			Type:     MutationWriteFile,
			VolumeId: v.volumeID,
			Path:     p,
			Offset:   offset,
			Size:     node.Size,
			ModTime:  timestamppb.New(now),
			Sha256:   node.Sha256,
			Data:     data,
		}
		waitFn, err := v.logMutationLocked(ctx, rec, reqLevel)
		if err != nil {
			return 0, 0, time.Time{}, nil, err
		}

		attr := &pb.EntryAttr{
			Inode:   node.ID,
			Path:    p,
			Name:    baseName,
			IsDir:   false,
			Size:    node.Size,
			Mode:    node.Mode,
			ModTime: timestamppb.New(now),
			Sha256:  node.Sha256,
		}
		v.broadcaster.Broadcast(v.volumeID, &pb.WatchVolumeResponse{
			EventType: pb.WatchEventType_EVENT_MODIFIED,
			Path:      p,
			Attr:      attr,
		})

		v.checkAutoSnapshotTriggerLocked(ctx)
		return int64(len(data)), node.Size, now, waitFn, nil
	}()
	if err != nil {
		return 0, 0, time.Time{}, err
	}
	if waitFn != nil {
		if err := waitFn(ctx); err != nil {
			return 0, 0, time.Time{}, err
		}
	}
	return nWritten, newSize, modTime, nil
}

func (v *Volume) TruncateFile(ctx context.Context, p string, size int64) (*pb.EntryAttr, error) {
	attr, waitFn, err := func() (*pb.EntryAttr, func(context.Context) error, error) {
		v.mu.Lock()
		defer v.mu.Unlock()

		p = cleanPath(p)
		inodeID, _, baseName, err := v.resolvePathLocked(ctx, p)
		if err != nil {
			return nil, nil, err
		}

		node, err := v.getOrLoadInodeLocked(ctx, inodeID)
		if err != nil {
			return nil, nil, err
		}

		if node.IsDir {
			return nil, nil, fmt.Errorf("cannot truncate directory: %w", syscall.EISDIR)
		}

		if size < 0 {
			return nil, nil, fmt.Errorf("invalid size %d: %w", size, syscall.EINVAL)
		}

		var currentData []byte
		if node.Data != nil {
			_ = node.Data.Rewind()
			currentData, _ = io.ReadAll(node.Data)
			_ = node.Data.Close()
		}

		if size < int64(len(currentData)) {
			currentData = currentData[:size]
		} else if size > int64(len(currentData)) {
			newBuf := make([]byte, size)
			copy(newBuf, currentData)
			currentData = newBuf
		}
		node.Size = size
		now := time.Now()
		node.ModTime = now
		node.IsDirty = true
		h := sha256.Sum256(currentData)
		node.Sha256 = fmt.Sprintf("%x", h)

		if node.Size <= blob.MemoryThreshold {
			node.Data = blob.NewByteStreamFromBytes(currentData)
		} else {
			tf, err := os.CreateTemp("", "objectfs-node-*")
			if err == nil {
				_, _ = tf.Write(currentData)
				_, _ = tf.Seek(0, io.SeekStart)
				node.Data = blob.NewByteStreamFromFile(tf, int64(len(currentData)), true)
			} else {
				node.Data = blob.NewByteStreamFromBytes(currentData)
			}
		}

		rec := &MutationRecord{
			Type:     MutationTruncateFile,
			VolumeId: v.volumeID,
			Path:     p,
			Size:     size,
			ModTime:  timestamppb.New(now),
			Sha256:   node.Sha256,
		}
		waitFn, err := v.logMutationLocked(ctx, rec, nil)
		if err != nil {
			return nil, nil, err
		}

		attr := &pb.EntryAttr{
			Inode:   node.ID,
			Path:    p,
			Name:    baseName,
			IsDir:   false,
			Size:    node.Size,
			Mode:    node.Mode,
			ModTime: timestamppb.New(now),
			Sha256:  node.Sha256,
		}
		v.broadcaster.Broadcast(v.volumeID, &pb.WatchVolumeResponse{
			EventType: pb.WatchEventType_EVENT_MODIFIED,
			Path:      p,
			Attr:      attr,
		})
		v.checkAutoSnapshotTriggerLocked(ctx)
		return attr, waitFn, nil
	}()
	if err != nil {
		return nil, err
	}
	if waitFn != nil {
		if err := waitFn(ctx); err != nil {
			return nil, err
		}
	}
	return attr, nil
}

func (v *Volume) Unlink(ctx context.Context, p string) error {
	waitFn, err := func() (func(context.Context) error, error) {
		v.mu.Lock()
		defer v.mu.Unlock()

		p = cleanPath(p)
		if p == "/" {
			return nil, fmt.Errorf("cannot unlink root: %w", syscall.EBUSY)
		}

		parentPath := path.Dir(p)
		baseName := path.Base(p)

		parentInodeID, _, _, err := v.resolvePathLocked(ctx, parentPath)
		if err != nil {
			return nil, err
		}

		parentDir, err := v.getOrLoadDirLocked(ctx, parentInodeID)
		if err != nil {
			return nil, err
		}

		entry, ok := parentDir.Entries[baseName]
		if !ok {
			return nil, fmt.Errorf("file %s not found: %w", p, syscall.ENOENT)
		}

		childInode, err := v.getOrLoadInodeLocked(ctx, entry.InodeID)
		if err != nil {
			return nil, err
		}
		if childInode.IsDir {
			return nil, fmt.Errorf("cannot unlink directory %s: %w", p, syscall.EISDIR)
		}

		if childInode.Data != nil {
			_ = childInode.Data.Close()
		}

		delete(parentDir.Entries, baseName)
		delete(parentDir.Added, baseName)
		parentDir.Deleted[baseName] = true
		parentDir.IsDirty = true

		parentInode, _ := v.getOrLoadInodeLocked(ctx, parentInodeID)
		if parentInode != nil {
			parentInode.ModTime = time.Now()
			parentInode.IsDirty = true
		}

		v.deletedPathsSinceFlush = append(v.deletedPathsSinceFlush, p)

		rec := &MutationRecord{
			Type:     MutationUnlink,
			VolumeId: v.volumeID,
			Path:     p,
		}
		waitFn, err := v.logMutationLocked(ctx, rec, nil)
		if err != nil {
			return nil, err
		}

		v.broadcaster.Broadcast(v.volumeID, &pb.WatchVolumeResponse{
			EventType: pb.WatchEventType_EVENT_DELETED,
			Path:      p,
		})

		v.checkAutoSnapshotTriggerLocked(ctx)
		return waitFn, nil
	}()
	if err != nil {
		return err
	}
	if waitFn != nil {
		if err := waitFn(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (v *Volume) Rmdir(ctx context.Context, p string) error {
	waitFn, err := func() (func(context.Context) error, error) {
		v.mu.Lock()
		defer v.mu.Unlock()

		p = cleanPath(p)
		if p == "/" {
			return nil, fmt.Errorf("cannot rmdir root: %w", syscall.EBUSY)
		}

		parentPath := path.Dir(p)
		baseName := path.Base(p)

		parentInodeID, _, _, err := v.resolvePathLocked(ctx, parentPath)
		if err != nil {
			return nil, err
		}

		parentDir, err := v.getOrLoadDirLocked(ctx, parentInodeID)
		if err != nil {
			return nil, err
		}

		entry, ok := parentDir.Entries[baseName]
		if !ok {
			return nil, fmt.Errorf("directory %s not found: %w", p, syscall.ENOENT)
		}

		childInode, err := v.getOrLoadInodeLocked(ctx, entry.InodeID)
		if err != nil {
			return nil, err
		}
		if !childInode.IsDir {
			return nil, fmt.Errorf("cannot rmdir non-directory %s: %w", p, syscall.ENOTDIR)
		}

		childDir, err := v.getOrLoadDirLocked(ctx, entry.InodeID)
		if err != nil {
			return nil, err
		}
		if len(childDir.Entries) > 0 {
			return nil, fmt.Errorf("directory %s not empty: %w", p, syscall.ENOTEMPTY)
		}

		delete(parentDir.Entries, baseName)
		delete(parentDir.Added, baseName)
		parentDir.Deleted[baseName] = true
		parentDir.IsDirty = true

		parentInode, _ := v.getOrLoadInodeLocked(ctx, parentInodeID)
		if parentInode != nil {
			parentInode.ModTime = time.Now()
			parentInode.IsDirty = true
		}

		rec := &MutationRecord{
			Type:     MutationRmdir,
			VolumeId: v.volumeID,
			Path:     p,
		}
		waitFn, err := v.logMutationLocked(ctx, rec, nil)
		if err != nil {
			return nil, err
		}

		v.broadcaster.Broadcast(v.volumeID, &pb.WatchVolumeResponse{
			EventType: pb.WatchEventType_EVENT_DELETED,
			Path:      p,
		})

		v.checkAutoSnapshotTriggerLocked(ctx)
		return waitFn, nil
	}()
	if err != nil {
		return err
	}
	if waitFn != nil {
		if err := waitFn(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (v *Volume) Rename(ctx context.Context, oldPath, newPath string) (*pb.EntryAttr, error) {
	attr, waitFn, err := func() (*pb.EntryAttr, func(context.Context) error, error) {
		v.mu.Lock()
		defer v.mu.Unlock()

		oldPath = cleanPath(oldPath)
		newPath = cleanPath(newPath)

		if oldPath == "/" || newPath == "/" {
			return nil, nil, fmt.Errorf("cannot rename root: %w", syscall.EBUSY)
		}

		oldParentPath := path.Dir(oldPath)
		oldBaseName := path.Base(oldPath)
		newParentPath := path.Dir(newPath)
		newBaseName := path.Base(newPath)

		oldParentInodeID, _, _, err := v.resolvePathLocked(ctx, oldParentPath)
		if err != nil {
			return nil, nil, fmt.Errorf("old parent not found: %w", err)
		}

		oldParentDir, err := v.getOrLoadDirLocked(ctx, oldParentInodeID)
		if err != nil {
			return nil, nil, fmt.Errorf("old parent directory not loaded: %w", err)
		}

		entry, ok := oldParentDir.Entries[oldBaseName]
		if !ok {
			return nil, nil, fmt.Errorf("source %s not found: %w", oldPath, syscall.ENOENT)
		}

		newParentInodeID, _, _, err := v.resolvePathLocked(ctx, newParentPath)
		if err != nil {
			return nil, nil, fmt.Errorf("new parent not found: %w", err)
		}

		newParentInode, err := v.getOrLoadInodeLocked(ctx, newParentInodeID)
		if err != nil {
			return nil, nil, err
		}
		if !newParentInode.IsDir {
			return nil, nil, fmt.Errorf("target parent %s is not a directory: %w", newParentPath, syscall.ENOTDIR)
		}

		newParentDir, err := v.getOrLoadDirLocked(ctx, newParentInodeID)
		if err != nil {
			return nil, nil, err
		}

		// Move entry
		delete(oldParentDir.Entries, oldBaseName)
		delete(oldParentDir.Added, oldBaseName)
		oldParentDir.Deleted[oldBaseName] = true
		oldParentDir.IsDirty = true

		entry.Name = newBaseName
		newParentDir.Entries[newBaseName] = entry
		newParentDir.Added[newBaseName] = true
		delete(newParentDir.Deleted, newBaseName)
		newParentDir.IsDirty = true

		now := time.Now()
		childInode, err := v.getOrLoadInodeLocked(ctx, entry.InodeID)
		if err == nil {
			childInode.ModTime = now
			childInode.IsDirty = true
		}

		oldParentInode, _ := v.getOrLoadInodeLocked(ctx, oldParentInodeID)
		if oldParentInode != nil {
			oldParentInode.ModTime = now
			oldParentInode.IsDirty = true
		}
		newParentInode.ModTime = now
		newParentInode.IsDirty = true

		v.deletedPathsSinceFlush = append(v.deletedPathsSinceFlush, oldPath)

		rec := &MutationRecord{
			Type:     MutationRename,
			VolumeId: v.volumeID,
			Path:     newPath,
			OldPath:  oldPath,
			ModTime:  timestamppb.New(now),
		}
		waitFn, err := v.logMutationLocked(ctx, rec, nil)
		if err != nil {
			return nil, nil, err
		}

		attr := &pb.EntryAttr{
			Inode:   entry.InodeID,
			Path:    newPath,
			Name:    newBaseName,
			IsDir:   entry.IsDir,
			Size:    childInode.Size,
			Mode:    childInode.Mode,
			ModTime: timestamppb.New(now),
			Sha256:  childInode.Sha256,
		}
		v.broadcaster.Broadcast(v.volumeID, &pb.WatchVolumeResponse{
			EventType: pb.WatchEventType_EVENT_RENAMED,
			Path:      newPath,
			OldPath:   oldPath,
			Attr:      attr,
		})

		v.checkAutoSnapshotTriggerLocked(ctx)
		return attr, waitFn, nil
	}()
	if err != nil {
		return nil, err
	}
	if waitFn != nil {
		if err := waitFn(ctx); err != nil {
			return nil, err
		}
	}
	return attr, nil
}

func (v *Volume) Fsync(ctx context.Context, p string) error {
	v.mu.RLock()
	_, _, _, err := v.resolvePathLocked(ctx, p)
	v.mu.RUnlock()
	if err != nil {
		return err
	}
	if v.stream != nil {
		if err := v.stream.Flush(ctx); err != nil {
			return fmt.Errorf("failed to flush WAL stream: %w", err)
		}
	}
	return nil
}

type bufferWriterAt struct {
	buf []byte
}

func (b *bufferWriterAt) WriteAt(p []byte, off int64) (int, error) {
	end := off + int64(len(p))
	if end > int64(len(b.buf)) {
		newBuf := make([]byte, end)
		copy(newBuf, b.buf)
		b.buf = newBuf
	}
	copy(b.buf[off:end], p)
	return len(p), nil
}

type snapshotResolver struct {
	metadataResolver
	vol *Volume
}

func (r *snapshotResolver) getOrLoadInode(ctx context.Context, inodeID uint64) (*CachedInode, error) {
	return r.resolveInode(ctx, inodeID, false)
}

func (r *snapshotResolver) getOrLoadDir(ctx context.Context, inodeID uint64) (map[string]DirEntry, error) {
	dir, err := r.resolveDir(ctx, inodeID, false)
	if err != nil {
		return nil, err
	}
	return dir.Entries, nil
}

func (r *snapshotResolver) buildErofsTree(ctx context.Context, dirInodeID uint64, dirName, currentPath string, dirtyBlobs map[string]blob.ByteStream, currentEntries map[string]FileMetadata) (erofs.Node, error) {
	entries, err := r.getOrLoadDir(ctx, dirInodeID)
	if err != nil {
		return nil, err
	}
	dirInode, err := r.getOrLoadInode(ctx, dirInodeID)
	if err != nil {
		return nil, err
	}

	currentEntries[currentPath] = FileMetadata{
		Inode:   dirInode.ID,
		Path:    currentPath,
		Name:    dirName,
		IsDir:   true,
		Mode:    dirInode.Mode,
		Size:    dirInode.Size,
		ModTime: dirInode.ModTime,
	}

	var childNames []string
	for name := range entries {
		childNames = append(childNames, name)
	}
	sort.Strings(childNames)

	var children []erofs.Node
	for _, name := range childNames {
		entry := entries[name]
		childPath := path.Join(currentPath, name)
		if entry.IsDir {
			childDirNode, err := r.buildErofsTree(ctx, entry.InodeID, name, childPath, dirtyBlobs, currentEntries)
			if err != nil {
				return nil, err
			}
			children = append(children, childDirNode)
		} else {
			childInode, err := r.getOrLoadInode(ctx, entry.InodeID)
			if err != nil {
				return nil, err
			}

			meta := FileMetadata{
				Inode:   childInode.ID,
				Path:    childPath,
				Name:    name,
				IsDir:   false,
				Mode:    childInode.Mode,
				Size:    childInode.Size,
				ModTime: childInode.ModTime,
				Sha256:  childInode.Sha256,
				ETag:    childInode.ETag,
			}

			needsUpload := childInode.ETag == ""
			if r.vol.lastFlushedMetadata != nil {
				if lastEntry, exists := r.vol.lastFlushedMetadata.Entries[childPath]; !exists || lastEntry.Sha256 != childInode.Sha256 {
					needsUpload = true
				}
			}
			if needsUpload && childInode.Sha256 != "" && r.vol.blobStore != nil && r.vol.backend != nil {
				key := strings.TrimPrefix(childPath, "/")
				blobReader, bErr := r.vol.blobStore.GetBlob(ctx, childInode.Sha256)
				if bErr == nil && blobReader != nil {
					etag, err := r.vol.backend.PutObject(ctx, r.vol.volumeID, key, blobReader)
					if err == nil {
						childInode.ETag = etag
						meta.ETag = etag
					}
					_ = blobReader.Close()
				}
			}

			currentEntries[childPath] = meta

			var xattrs erofs.Xattrs
			if childInode.Sha256 != "" {
				xattrs.UserDigest = childInode.Sha256
				xattrs.UserSHA256 = childInode.Sha256
			}

			leafNode := erofs.NewMemoryNode(
				name,
				false,
				uint16(childInode.Mode),
				nil,
				nil,
				erofs.WithMetadataOnly(true),
				erofs.WithSize(uint64(childInode.Size)),
				erofs.WithMtime(uint64(childInode.ModTime.Unix())),
				erofs.WithXattrs(xattrs),
			)
			children = append(children, leafNode)
		}
	}

	dirNodeName := dirName
	if dirNodeName == "/" {
		dirNodeName = ""
	}
	return erofs.NewMemoryNode(
		dirNodeName,
		true,
		uint16(dirInode.Mode),
		nil,
		children,
		erofs.WithMtime(uint64(dirInode.ModTime.Unix())),
	), nil
}

func (v *Volume) flushToBackendLocked(ctx context.Context) error {
	if v.backend == nil {
		return nil
	}

	// 1. Phase 1: Persist all in-memory dirty cache entries to local storage / blob store.
	v.inodeCache.ForEach(func(id uint64, node *CachedInode) {
		if node.IsDirty {
			_ = v.persistInode(ctx, node)
		}
	})
	v.dirCache.ForEach(func(id uint64, dir *CachedDir) {
		if dir.IsDirty {
			v.onEvictDir(id, dir)
		}
	})

	// 2. Phase 2: Capture point-in-time metadata pointers.
	var cutoffOffset LocalOffset
	if v.localStore != nil {
		cutoffOffset = v.localStore.CurrentOffset()
	}

	snapDirtyInodes := make(map[uint64]LocalOffset, len(v.dirtyInodes))
	for k, off := range v.dirtyInodes {
		snapDirtyInodes[k] = off
	}

	snapDirtyDirs := make(map[uint64]LocalOffset, len(v.dirtyDirs))
	for k, off := range v.dirtyDirs {
		snapDirtyDirs[k] = off
	}

	baseReader := v.snapshotReader
	baseRaw := v.snapshotRaw
	rootID := v.rootInodeID
	delPaths := make([]string, len(v.deletedPathsSinceFlush))
	copy(delPaths, v.deletedPathsSinceFlush)
	v.deletedPathsSinceFlush = nil

	// Release v.mu during snapshot compilation and cloud storage upload to avoid stopping the world
	v.mu.Unlock()

	var erofsBuf bufferWriterAt
	var currentEntries map[string]FileMetadata

	snapErr := func() error {
		for _, delPath := range delPaths {
			key := strings.TrimPrefix(delPath, "/")
			_ = v.backend.DeleteObject(ctx, v.volumeID, key)
		}

		currentEntries = make(map[string]FileMetadata)
		dirtyBlobs := make(map[string]blob.ByteStream)

		resolver := &snapshotResolver{
			vol: v,
			metadataResolver: metadataResolver{
				localStore:     v.localStore,
				snapshotReader: baseReader,
				snapshotRaw:    baseRaw,
				dirtyInodes:    snapDirtyInodes,
				dirtyDirs:      snapDirtyDirs,
			},
		}

		erofsTree, err := resolver.buildErofsTree(ctx, rootID, "/", "/", dirtyBlobs, currentEntries)
		if err != nil {
			return fmt.Errorf("failed to build hierarchy for snapshot: %w", err)
		}

		if len(dirtyBlobs) > 0 && v.blobStore != nil {
			if err := v.blobStore.PutBlobs(ctx, dirtyBlobs); err != nil {
				return fmt.Errorf("failed to persist blobs: %w", err)
			}
		}

		if err := erofs.WriteImage(&erofsBuf, erofsTree); err != nil {
			return fmt.Errorf("failed to compile EROFS snapshot: %w", err)
		}

		readerAt := bytes.NewReader(erofsBuf.buf)
		if err := erofs.Fsck(readerAt); err != nil {
			return fmt.Errorf("Fsck failed on generated EROFS snapshot: %w", err)
		}

		timestamp := time.Now().UTC().Format("20060102T150405.000000Z")
		snapshotName := fmt.Sprintf("%s.erofs", timestamp)
		snapshotKey := path.Join("volumes", v.volumeID, "meta", snapshotName)
		erofsStream := blob.NewByteStreamFromBytes(erofsBuf.buf)
		if _, err := v.backend.PutObject(ctx, "", snapshotKey, erofsStream); err != nil {
			_ = erofsStream.Close()
			return fmt.Errorf("failed to save EROFS snapshot %s: %w", snapshotKey, err)
		}
		_ = erofsStream.Close()

		latestKey := path.Join("volumes", v.volumeID, "meta", "latest")
		latestStream := blob.NewByteStreamFromBytes([]byte(snapshotName))
		if _, err := v.backend.PutObject(ctx, "", latestKey, latestStream); err != nil {
			_ = latestStream.Close()
			return fmt.Errorf("failed to update latest snapshot: %w", err)
		}
		_ = latestStream.Close()

		return nil
	}()

	// Re-acquire v.mu.Lock() for Phase 4
	v.mu.Lock()

	if snapErr != nil {
		return snapErr
	}

	// 4. Phase 4: Update base snapshot reader, prune committed dirty maps, and trim circular buffer
	v.snapshotRaw = bytes.NewReader(erofsBuf.buf)
	if r, err := erofs.NewReader(v.snapshotRaw); err == nil {
		v.snapshotReader = r
		v.rootInodeID = r.GetRootNID()
	}
	v.snapshotCutoff = cutoffOffset

	for inodeID, snapOff := range snapDirtyInodes {
		if currOff, ok := v.dirtyInodes[inodeID]; ok && currOff == snapOff {
			delete(v.dirtyInodes, inodeID)
		}
	}

	for dirID, snapOff := range snapDirtyDirs {
		if currOff, ok := v.dirtyDirs[dirID]; ok && currOff == snapOff {
			delete(v.dirtyDirs, dirID)
		}
	}

	// Reset in-memory clean caches so all directory entries and inode IDs seamlessly rebind to the new EROFS snapshot's NIDs
	v.inodeCache.Clear()
	v.dirCache.Clear()

	if v.localStore != nil && cutoffOffset != NoOffset {
		_ = v.localStore.TrimBefore(cutoffOffset)
	}

	newMeta := VolumeMetadata{
		VolumeID:    v.volumeID,
		Version:     1,
		LastFlushed: time.Now(),
		NextInode:   v.nextInode,
		Entries:     currentEntries,
	}

	metaBytes, err := json.MarshalIndent(newMeta, "", "  ")
	if err == nil {
		metaStream := blob.NewByteStreamFromBytes(metaBytes)
		_, _ = v.backend.PutObject(ctx, v.volumeID, MetadataFileName, metaStream)
		_ = metaStream.Close()
	}
	v.lastFlushedMetadata = &newMeta

	return nil
}

func (v *Volume) checkAutoSnapshotTriggerLocked(ctx context.Context) {
	if v.backend == nil {
		return
	}
	shouldSnapshot := false
	if v.maxDirtyRecords > 0 && (len(v.dirtyInodes)+len(v.dirtyDirs)) >= v.maxDirtyRecords {
		shouldSnapshot = true
	}
	if v.localStore != nil {
		if v.maxLocalFileSize > 0 && v.localStore.ActiveFileSize() >= v.maxLocalFileSize {
			shouldSnapshot = true
		}
		if v.maxBufferFiles > 0 && v.localStore.FileCount() >= v.maxBufferFiles {
			shouldSnapshot = true
		}
	}
	if shouldSnapshot {
		if !v.snapshotMu.TryLock() {
			return
		}
		defer v.snapshotMu.Unlock()
		_ = v.flushToBackendLocked(ctx)
	}
}

func (v *Volume) FlushToBackend(ctx context.Context) error {
	v.snapshotMu.Lock()
	defer v.snapshotMu.Unlock()

	v.mu.Lock()
	defer v.mu.Unlock()
	return v.flushToBackendLocked(ctx)
}

func (v *Volume) findLatestSnapshotNameLocked(ctx context.Context) (string, error) {
	latestKey := path.Join("volumes", v.volumeID, "meta", "latest")
	var latestBuf bytes.Buffer
	err := v.backend.GetObject(ctx, "", latestKey, 0, 0, &latestBuf)
	if err == nil && latestBuf.Len() > 0 {
		return strings.TrimSpace(latestBuf.String()), nil
	}

	metaPrefix := path.Join("volumes", v.volumeID, "meta") + "/"
	objects, err := v.backend.ListObjects(ctx, "", metaPrefix)
	if err != nil {
		return "", err
	}

	var snapshots []string
	for _, obj := range objects {
		if strings.HasSuffix(obj, ".erofs") {
			base := path.Base(obj)
			snapshots = append(snapshots, base)
		}
	}
	if len(snapshots) == 0 {
		return "", fmt.Errorf("no snapshots found for volume %s", v.volumeID)
	}
	sort.Strings(snapshots)
	return snapshots[len(snapshots)-1], nil
}

// ApplyRecordLocked applies a single mutation record to the filesystem tables.
func (v *Volume) ApplyRecordLocked(record *MutationRecord) error {
	ctx := context.Background()
	if record.Inode >= v.nextInode {
		v.nextInode = record.Inode + 1
	}

	switch record.Type {
	case MutationMkdir:
		p := cleanPath(record.Path)
		if p == "/" {
			return nil
		}
		parentPath := path.Dir(p)
		baseName := path.Base(p)

		parentInodeID, err := v.findOrCreateDirParentsLocked(ctx, parentPath)
		if err != nil {
			return err
		}

		parentDir, err := v.getOrLoadDirLocked(ctx, parentInodeID)
		if err != nil {
			return err
		}

		mode := record.Mode
		if mode == 0 {
			mode = 0755
		}
		mode |= syscall.S_IFDIR

		inodeID := record.Inode
		if inodeID == 0 {
			inodeID = v.allocInode()
		}

		var modTime time.Time
		if record.ModTime != nil {
			modTime = record.ModTime.AsTime()
		}
		if modTime.IsZero() {
			modTime = time.Now()
		}

		childInode := &CachedInode{
			ID:      inodeID,
			Mode:    mode,
			ModTime: modTime,
			IsDir:   true,
			IsDirty: false,
		}
		v.inodeCache.Put(inodeID, childInode)

		childDir := &CachedDir{
			ID:         inodeID,
			Entries:    make(map[string]DirEntry),
			Added:      make(map[string]bool),
			Deleted:    make(map[string]bool),
			PrevOffset: NoOffset,
			IsDirty:    false,
		}
		v.dirCache.Put(inodeID, childDir)

		newEntry := DirEntry{
			Name:    baseName,
			InodeID: inodeID,
			IsDir:   true,
			Mode:    mode,
		}
		parentDir.Entries[baseName] = newEntry
		return nil

	case MutationCreateFile:
		p := cleanPath(record.Path)
		if p == "/" {
			return fmt.Errorf("cannot create file at root: %w", syscall.EISDIR)
		}
		parentPath := path.Dir(p)
		baseName := path.Base(p)

		parentInodeID, err := v.findOrCreateDirParentsLocked(ctx, parentPath)
		if err != nil {
			return err
		}

		parentDir, err := v.getOrLoadDirLocked(ctx, parentInodeID)
		if err != nil {
			return err
		}

		mode := record.Mode
		if mode == 0 {
			mode = 0644
		}
		mode |= syscall.S_IFREG

		inodeID := record.Inode
		if inodeID == 0 {
			inodeID = v.allocInode()
		}

		var modTime time.Time
		if record.ModTime != nil {
			modTime = record.ModTime.AsTime()
		}
		if modTime.IsZero() {
			modTime = time.Now()
		}

		var stream blob.ByteStream
		if len(record.Data) > 0 {
			stream = blob.NewByteStreamFromBytes(record.Data)
		}

		childInode := &CachedInode{
			ID:      inodeID,
			Mode:    mode,
			Size:    record.Size,
			ModTime: modTime,
			Data:    stream,
			Sha256:  record.Sha256,
			IsDir:   false,
			IsDirty: false,
		}
		v.inodeCache.Put(inodeID, childInode)

		newEntry := DirEntry{
			Name:    baseName,
			InodeID: inodeID,
			IsDir:   false,
			Mode:    mode,
		}
		parentDir.Entries[baseName] = newEntry
		return nil

	case MutationWriteFile:
		p := cleanPath(record.Path)
		inodeID, _, _, err := v.resolvePathLocked(ctx, p)
		if err != nil {
			return err
		}
		node, err := v.getOrLoadInodeLocked(ctx, inodeID)
		if err != nil {
			return err
		}

		if len(record.Data) > 0 {
			var currentData []byte
			if node.Data != nil {
				_ = node.Data.Rewind()
				currentData, _ = io.ReadAll(node.Data)
				_ = node.Data.Close()
			}
			neededLen := record.Offset + int64(len(record.Data))
			if neededLen > int64(len(currentData)) {
				newBuf := make([]byte, neededLen)
				copy(newBuf, currentData)
				currentData = newBuf
			}
			copy(currentData[record.Offset:], record.Data)
			if node.Size < int64(len(currentData)) {
				node.Size = int64(len(currentData))
			}
			node.Data = blob.NewByteStreamFromBytes(currentData)
		}
		if record.Size > 0 {
			node.Size = record.Size
		}
		if record.Sha256 != "" {
			node.Sha256 = record.Sha256
		}
		if record.ModTime != nil {
			node.ModTime = record.ModTime.AsTime()
		}
		return nil

	case MutationTruncateFile:
		p := cleanPath(record.Path)
		inodeID, _, _, err := v.resolvePathLocked(ctx, p)
		if err != nil {
			return err
		}
		node, err := v.getOrLoadInodeLocked(ctx, inodeID)
		if err != nil {
			return err
		}
		node.Size = record.Size
		if record.Sha256 != "" {
			node.Sha256 = record.Sha256
		}
		if record.ModTime != nil {
			node.ModTime = record.ModTime.AsTime()
		}
		if node.Data != nil {
			_ = node.Data.Rewind()
			currentData, _ := io.ReadAll(node.Data)
			_ = node.Data.Close()
			if record.Size < int64(len(currentData)) {
				currentData = currentData[:record.Size]
			} else if record.Size > int64(len(currentData)) {
				newBuf := make([]byte, record.Size)
				copy(newBuf, currentData)
				currentData = newBuf
			}
			node.Data = blob.NewByteStreamFromBytes(currentData)
		}
		return nil

	case MutationUnlink:
		p := cleanPath(record.Path)
		parentPath := path.Dir(p)
		baseName := path.Base(p)

		parentInodeID, _, _, err := v.resolvePathLocked(ctx, parentPath)
		if err != nil {
			return nil
		}
		parentDir, err := v.getOrLoadDirLocked(ctx, parentInodeID)
		if err != nil {
			return nil
		}
		delete(parentDir.Entries, baseName)
		return nil

	case MutationRmdir:
		p := cleanPath(record.Path)
		parentPath := path.Dir(p)
		baseName := path.Base(p)

		parentInodeID, _, _, err := v.resolvePathLocked(ctx, parentPath)
		if err != nil {
			return nil
		}
		parentDir, err := v.getOrLoadDirLocked(ctx, parentInodeID)
		if err != nil {
			return nil
		}
		delete(parentDir.Entries, baseName)
		return nil

	case MutationRename:
		oldP := cleanPath(record.OldPath)
		newP := cleanPath(record.Path)
		oldParentPath := path.Dir(oldP)
		oldBase := path.Base(oldP)
		newParentPath := path.Dir(newP)
		newBase := path.Base(newP)

		oldParentInodeID, _, _, err := v.resolvePathLocked(ctx, oldParentPath)
		if err != nil {
			return err
		}
		oldParentDir, err := v.getOrLoadDirLocked(ctx, oldParentInodeID)
		if err != nil {
			return err
		}

		entry, ok := oldParentDir.Entries[oldBase]
		if !ok {
			return fmt.Errorf("rename source not found: %s", oldP)
		}

		newParentInodeID, err := v.findOrCreateDirParentsLocked(ctx, newParentPath)
		if err != nil {
			return err
		}
		newParentDir, err := v.getOrLoadDirLocked(ctx, newParentInodeID)
		if err != nil {
			return err
		}

		delete(oldParentDir.Entries, oldBase)
		entry.Name = newBase
		newParentDir.Entries[newBase] = entry

		if record.ModTime != nil {
			childInode, _ := v.getOrLoadInodeLocked(ctx, entry.InodeID)
			if childInode != nil {
				childInode.ModTime = record.ModTime.AsTime()
			}
		}
		return nil

	default:
		return fmt.Errorf("unknown mutation type: %s", record.Type)
	}
}

func (v *Volume) replayRecordsLocked(records []*MutationRecord) error {
	for _, rec := range records {
		if err := v.ApplyRecordLocked(rec); err != nil {
			return fmt.Errorf("failed to apply record %v: %w", rec, err)
		}
	}
	return nil
}

func (v *Volume) replayClientRecordsLocked(records []*wal.ClientRecord) error {
	for _, cr := range records {
		mut, err := DecodeMutationRecord(cr.Payload)
		if err != nil {
			return fmt.Errorf("failed to decode mutation record at seq %d: %w", cr.StreamSeq, err)
		}
		mut.StreamSeq = cr.StreamSeq
		if err := v.ApplyRecordLocked(mut); err != nil {
			return fmt.Errorf("failed to apply mutation record at seq %d: %w", cr.StreamSeq, err)
		}
	}
	return nil
}

// ReplayRecords applies an ordered list of mutation records to the volume.
func (v *Volume) ReplayRecords(records []*MutationRecord) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.replayRecordsLocked(records)
}

// ReplayClientRecords decodes and applies an ordered list of WAL client records to the volume.
func (v *Volume) ReplayClientRecords(records []*wal.ClientRecord) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.replayClientRecordsLocked(records)
}

func (v *Volume) LoadFromBackend(ctx context.Context) error {
	v.mu.Lock()
	defer v.mu.Unlock()

	if v.backend != nil {
		latestSnapshotName, err := v.findLatestSnapshotNameLocked(ctx)
		if err == nil && latestSnapshotName != "" {
			snapshotKey := path.Join("volumes", v.volumeID, "meta", latestSnapshotName)
			var imgBuf bytes.Buffer
			err := v.backend.GetObject(ctx, "", snapshotKey, 0, 0, &imgBuf)
			if err == nil && imgBuf.Len() > 0 {
				snapBytes := imgBuf.Bytes()
				readerAt := bytes.NewReader(snapBytes)
				reader, err := erofs.NewReader(readerAt)
				if err == nil {
					v.snapshotRaw = readerAt
					v.snapshotReader = reader
					v.rootInodeID = reader.GetRootNID()
					maxNID := (uint64(len(snapBytes)))/32 + 1000
					if v.nextInode < maxNID {
						v.nextInode = maxNID
					}
					v.inodeCache.Clear()
					v.dirCache.Clear()
					v.dirtyInodes = make(map[uint64]LocalOffset)
					v.dirtyDirs = make(map[uint64]LocalOffset)
				}
			}
		}
	}

	if v.stream != nil {
		recovered := v.stream.RecoveredRecords()
		if len(recovered) > 0 {
			if err := v.replayClientRecordsLocked(recovered); err != nil {
				return fmt.Errorf("failed to replay recovered WAL records: %w", err)
			}
		}
	}

	return nil
}

// CreateSnapshot creates and returns a new EROFS snapshot of the current volume state.
func (v *Volume) CreateSnapshot(ctx context.Context) (string, error) {
	if err := v.FlushToBackend(ctx); err != nil {
		return "", err
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.findLatestSnapshotNameLocked(ctx)
}

// ListSnapshots returns all EROFS snapshot filenames for this volume sorted chronologically.
func (v *Volume) ListSnapshots(ctx context.Context) ([]string, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	metaPrefix := path.Join("volumes", v.volumeID, "meta") + "/"
	objects, err := v.backend.ListObjects(ctx, "", metaPrefix)
	if err != nil {
		return nil, err
	}

	var snapshots []string
	for _, obj := range objects {
		if strings.HasSuffix(obj, ".erofs") {
			base := path.Base(obj)
			snapshots = append(snapshots, base)
		}
	}
	sort.Strings(snapshots)
	return snapshots, nil
}

// RestoreSnapshot restores the volume filesystem state to a specific EROFS snapshot.
func (v *Volume) RestoreSnapshot(ctx context.Context, snapshotName string) error {
	v.snapshotMu.Lock()
	defer v.snapshotMu.Unlock()

	v.mu.Lock()
	defer v.mu.Unlock()

	if v.backend == nil {
		return fmt.Errorf("no backend configured")
	}

	snapshotKey := path.Join("volumes", v.volumeID, "meta", snapshotName)
	var imgBuf bytes.Buffer
	if err := v.backend.GetObject(ctx, "", snapshotKey, 0, 0, &imgBuf); err != nil {
		return fmt.Errorf("failed to fetch snapshot %s: %w", snapshotKey, err)
	}

	snapBytes := imgBuf.Bytes()
	readerAt := bytes.NewReader(snapBytes)
	reader, err := erofs.NewReader(readerAt)
	if err != nil {
		return fmt.Errorf("failed to parse snapshot %s: %w", snapshotName, err)
	}

	v.snapshotRaw = readerAt
	v.snapshotReader = reader
	v.rootInodeID = reader.GetRootNID()
	maxNID := (uint64(len(snapBytes)))/32 + 1000
	if v.nextInode < maxNID {
		v.nextInode = maxNID
	}

	v.inodeCache.Clear()
	v.dirCache.Clear()
	v.dirtyInodes = make(map[uint64]LocalOffset)
	v.dirtyDirs = make(map[uint64]LocalOffset)
	v.snapshotCutoff = NoOffset
	if v.localStore != nil {
		_ = v.localStore.DeleteAllAndReset()
	}

	return nil
}
