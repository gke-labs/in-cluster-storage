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
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	sdsv1 "github.com/gke-labs/in-cluster-storage/pkg/api/sds/v1"
	"github.com/gke-labs/in-cluster-storage/pkg/erofs"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/blob"
	"github.com/gke-labs/in-cluster-storage/pkg/sds"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/memtable"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/projection/sqlite"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/record"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/view"
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/klog/v2"
)

const (
	defaultApplierBatchSize = 100

	// MaxNameLength is the maximum allowed byte length for a path component name
	// (matching the POSIX NAME_MAX limit of 255 bytes advertised by statfs).
	MaxNameLength = 255
)

type InodeUpload struct {
	wg      sync.WaitGroup
	waiting bool
	err     error
}

type Volume struct {
	mu           sync.RWMutex
	volumeID     string
	rootInodeID  uint64
	nextInode    uint64
	backend      ObjectStorageBackend
	blobStore    *blob.Store
	broadcaster  *EventBroadcaster
	maxInlineLen int64
	chunkSize    uint32

	pendingUploadsMu sync.Mutex
	pendingUploadsWg sync.WaitGroup
	inodeUploads     map[uint64]*InodeUpload

	stream     walclient.Stream
	durability walclient.Level
	streamID   uuid.UUID

	metadataStream   *sds.Writer
	lastCommitSeq    uint64
	snapshotPointers []*sdsv1.SnapshotPointer
	liveStats        *pb.VolumeStats
	debugStatsCheck  *bool

	localStorageDir string
	indexFactory    sds.IndexFactory
	metadataView    *view.View
	viewOpts        []view.Option
	memAppender     *memoryAppender

	closed   bool
	closedCh chan struct{}

	snapshotMu   sync.Mutex
	batchCheckWg sync.WaitGroup

	dirParents map[uint64]uint64

	nextFh     atomic.Uint64
	handlesMu  sync.Mutex
	handles    map[uint64]uint64
	openInodes map[uint64]int
}

// VolumeOption configures a Volume instance.
type VolumeOption func(*Volume)

// WithMetadataIndex sets the metadata local index type (e.g. "sqlite" or "memory").
func WithMetadataIndex(indexType string) VolumeOption {
	return func(v *Volume) {
		f, err := sds.GetIndexFactory(indexType)
		if err != nil {
			panic(err)
		}
		v.indexFactory = f
	}
}

// WithIndexFactory sets the local index factory for the volume.
func WithIndexFactory(f sds.IndexFactory) VolumeOption {
	return func(v *Volume) {
		v.indexFactory = f
	}
}

// WithViewOptions configures options for the underlying sds/view layer.
func WithViewOptions(opts ...view.Option) VolumeOption {
	return func(v *Volume) {
		v.viewOpts = append(v.viewOpts, opts...)
	}
}

// WithStream sets the WAL stream for metadata change-logging.
func WithStream(stream walclient.Stream) VolumeOption {
	return func(v *Volume) {
		v.stream = stream
		if err := v.initMetadataStreamLocked(); err != nil {
			panic(fmt.Sprintf("failed to init metadata stream: %v", err))
		}
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

// WithChunkSize sets the fixed chunk size for large files on this volume.
func WithChunkSize(chunkSize uint32) VolumeOption {
	return func(v *Volume) {
		if chunkSize > 0 {
			v.chunkSize = chunkSize
		}
	}
}

// WithMaxRAMEntries sets the maximum number of entries in the metadata read cache.
func WithMaxRAMEntries(maxInodes, maxDirs int) VolumeOption {
	return func(v *Volume) {
		entries := maxInodes + maxDirs
		if entries <= 0 {
			entries = 65536
		}
		v.viewOpts = append(v.viewOpts, view.WithCacheLimits(entries, 0))
	}
}

// WithMetadataCacheLimits sets the maximum entry count and byte capacity for the metadata read cache.
func WithMetadataCacheLimits(maxEntries int, maxBytes int64) VolumeOption {
	return func(v *Volume) {
		v.viewOpts = append(v.viewOpts, view.WithCacheLimits(maxEntries, maxBytes))
	}
}

// WithMetadataCacheDisabled enables or disables the metadata read cache.
func WithMetadataCacheDisabled(disabled bool) VolumeOption {
	return func(v *Volume) {
		v.viewOpts = append(v.viewOpts, view.WithCacheDisabled(disabled))
	}
}

// WithMaxUnappliedBytes sets the maximum byte bound for unapplied overlay rows before applying backpressure.
func WithMaxUnappliedBytes(maxBytes int64) VolumeOption {
	return func(v *Volume) {
		v.viewOpts = append(v.viewOpts, view.WithOverlayMaxBytes(maxBytes))
	}
}

// WithApplierBatchSize sets the maximum batch size for the background applier.
func WithApplierBatchSize(batchSize int) VolumeOption {
	return func(v *Volume) {
		v.viewOpts = append(v.viewOpts, view.WithBatchSize(batchSize))
	}
}

// WithApplierFaultHook configures a fault injection callback for the background applier (testing only).
func WithApplierFaultHook(hook func() error) VolumeOption {
	return func(v *Volume) {
		v.viewOpts = append(v.viewOpts, view.WithFaultHook(hook))
	}
}

// WithVolumeMutationCheck enables debug mutation checking on the metadata view.
func WithVolumeMutationCheck() VolumeOption {
	return func(v *Volume) {
		v.viewOpts = append(v.viewOpts, view.WithMutationCheck())
		if v.debugStatsCheck == nil {
			t := true
			v.debugStatsCheck = &t
		}
	}
}

// WithoutVolumeMutationCheck disables debug mutation checking on the metadata view (e.g. for benchmarks).
func WithoutVolumeMutationCheck() VolumeOption {
	return func(v *Volume) {
		v.viewOpts = append(v.viewOpts, view.WithoutMutationCheck())
	}
}

// WithVolumeStatsCheck enables debug stats assertions comparing live to durable stats.
func WithVolumeStatsCheck() VolumeOption {
	return func(v *Volume) {
		t := true
		v.debugStatsCheck = &t
	}
}

// WithoutVolumeStatsCheck disables debug stats assertions.
func WithoutVolumeStatsCheck() VolumeOption {
	return func(v *Volume) {
		f := false
		v.debugStatsCheck = &f
	}
}

// WithVolumeViewOptions appends custom view options to the volume's metadata view.
func WithVolumeViewOptions(opts ...view.Option) VolumeOption {
	return func(v *Volume) {
		v.viewOpts = append(v.viewOpts, opts...)
	}
}

// WithLocalStorageDir sets the local directory for metadata storage.
func WithLocalStorageDir(dir string) VolumeOption {
	return func(v *Volume) {
		v.localStorageDir = dir
	}
}

// WithMaxBufferFiles is deprecated and kept for backwards compatibility.
func WithMaxBufferFiles(count int) VolumeOption {
	return func(v *Volume) {}
}

// WithSnapshotThreshold is deprecated and kept for backwards compatibility.
func WithSnapshotThreshold(maxDirtyRecords int, maxFileSize int64) VolumeOption {
	return func(v *Volume) {}
}

// StreamIDForVolume generates a deterministic UUID for an SDS structured stream for the volume.
func StreamIDForVolume(volumeID string) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("objectfs-sds:"+volumeID))
}

func (v *Volume) initMetadataViewLocked(ctx context.Context, customIndex sds.LocalIndex) error {
	var index sds.LocalIndex = customIndex
	if index == nil {
		if v.indexFactory == nil {
			f, err := sds.GetIndexFactory("sqlite")
			if err != nil {
				return err
			}
			v.indexFactory = f
		}
		if v.localStorageDir == "" {
			v.localStorageDir = path.Join(os.TempDir(), fmt.Sprintf("objectfs-local-%s-%d", v.volumeID, time.Now().UnixNano()))
		}
		var err error
		if localIdx, found, _ := v.indexFactory.OpenLocal(ctx, v.streamID.String(), v.localStorageDir); found && localIdx != nil {
			index = localIdx
		} else {
			index, err = v.indexFactory.NewEmpty(ctx, v.streamID.String(), v.localStorageDir)
			if err != nil {
				return fmt.Errorf("failed to create local index: %w", err)
			}
		}
	}

	if v.metadataView != nil {
		_ = v.metadataView.Close()
	}

	viewOpts := append([]view.Option{}, v.viewOpts...)
	initialStats := &pb.VolumeStats{Name: VolumeStatsRowName}
	viewOpts = append(viewOpts, view.WithStats(initialStats, UpdateVolumeStats))
	viewOpts = append(viewOpts, view.WithRebuildFunc(v.rebuildIndex))
	viewOpts = append(viewOpts, view.WithBatchAppliedHook(v.checkStatsOnBatchApplied))
	if v.metadataStream != nil {
		if _, err := v.metadataStream.Registry().RegisterMessage(initialStats, 1); err != nil {
			return fmt.Errorf("failed to register VolumeStats message: %w", err)
		}
		viewOpts = append(viewOpts, view.WithRegistry(v.metadataStream.Registry()))
	}

	v.metadataView = view.New(index, viewOpts...)

	if v.metadataStream != nil {
		if err := v.metadataView.SyncRegistry(ctx, v.metadataStream.Registry()); err != nil {
			return fmt.Errorf("failed to sync registry with metadata view: %w", err)
		}
	}
	if err := v.metadataView.LoadStats(ctx); err != nil {
		return fmt.Errorf("failed to load metadata view stats: %w", err)
	}
	v.initLiveStatsLocked()

	return nil
}

func NewVolume(volumeID string, backend ObjectStorageBackend, broadcaster *EventBroadcaster, opts ...VolumeOption) *Volume {
	var blobStore *blob.Store
	if backend != nil {
		blobStore = blob.NewStore(backend, 0)
	}

	f, _ := sds.GetIndexFactory("sqlite")

	v := &Volume{
		volumeID:     volumeID,
		rootInodeID:  1,
		nextInode:    erofs.DefaultInodeStride,
		backend:      backend,
		blobStore:    blobStore,
		broadcaster:  broadcaster,
		maxInlineLen: 4096,      // 4KB default inline threshold for tiny files
		chunkSize:    64 * 1024, // 64KB default chunk size
		durability:   walclient.Local,
		indexFactory: f,
		streamID:     StreamIDForVolume(volumeID),
		closedCh:     make(chan struct{}),
		dirParents:   make(map[uint64]uint64),
		handles:      make(map[uint64]uint64),
		openInodes:   make(map[uint64]int),
		inodeUploads: make(map[uint64]*InodeUpload),
	}

	for _, opt := range opts {
		opt(v)
	}

	if err := v.initMetadataStreamLocked(); err != nil {
		panic(fmt.Sprintf("failed to init metadata stream: %v", err))
	}

	if err := v.initMetadataViewLocked(context.Background(), nil); err != nil {
		panic(fmt.Sprintf("failed to init metadata view: %v", err))
	}

	rootKey, _ := pkInode.Extract(&pb.Inode{Ino: proto.Uint64(1)})
	msg, ok, _ := v.metadataView.Get(context.Background(), "objectfs.v1alpha1.Inode", rootKey)
	if !ok || msg == nil {
		rootInodeMsg := &pb.Inode{
			Ino:   proto.Uint64(1),
			Mode:  0755 | syscall.S_IFDIR,
			Mtime: timestamppb.Now(),
			IsDir: true,
			Nlink: 2,
		}
		keyBytes, valBytes, _ := sds.SplitKeyAndNonKey(rootInodeMsg, []int32{1})
		initChanges := []sds.Change{
			{
				Seq:      0,
				TypeID:   16,
				TypeName: "objectfs.v1alpha1.Inode",
				Op:       sds.OpCreate,
				Key:      sds.NewKeyFromBytes(keyBytes),
				RawKey:   keyBytes,
				RawVal:   valBytes,
				Row:      rootInodeMsg,
			},
		}
		_ = v.metadataView.ApplyChangesSync(context.Background(), initChanges)
	}

	if err := v.cleanOrphanInodesLocked(context.Background()); err != nil {
		klog.Warningf("Failed to clean orphan inodes during volume init: %v", err)
	}

	v.rootInodeID = 1
	v.nextInode = erofs.DefaultInodeStride
	v.dirParents[1] = 1

	v.initLiveStatsLocked()
	v.assertStatsMatchLocked("after NewVolume")

	return v
}

func (v *Volume) registerPendingUpload(ino uint64) func(error) {
	v.pendingUploadsMu.Lock()
	defer v.pendingUploadsMu.Unlock()

	if v.inodeUploads == nil {
		v.inodeUploads = make(map[uint64]*InodeUpload)
	}

	upload, exists := v.inodeUploads[ino]
	if !exists || upload.waiting {
		upload = &InodeUpload{}
		v.inodeUploads[ino] = upload
	}

	v.pendingUploadsWg.Add(1)
	upload.wg.Add(1)

	return func(err error) {
		v.pendingUploadsMu.Lock()
		defer v.pendingUploadsMu.Unlock()

		if err != nil {
			upload.err = err
		}
		upload.wg.Done()
		v.pendingUploadsWg.Done()
	}
}

func (v *Volume) waitForInodeUploads(ctx context.Context, ino uint64) error {
	v.pendingUploadsMu.Lock()
	upload := v.inodeUploads[ino]
	if upload != nil {
		upload.waiting = true
	}
	v.pendingUploadsMu.Unlock()

	if upload != nil {
		done := make(chan struct{})
		go func() {
			upload.wg.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	v.pendingUploadsMu.Lock()
	var err error
	if upload != nil {
		err = upload.err
		if v.inodeUploads[ino] == upload {
			delete(v.inodeUploads, ino)
		}
	}
	v.pendingUploadsMu.Unlock()

	return err
}

func (v *Volume) waitForVolumeUploads(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		v.pendingUploadsWg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}

	v.pendingUploadsMu.Lock()
	var firstErr error
	for ino, upload := range v.inodeUploads {
		if upload != nil && upload.err != nil && firstErr == nil {
			firstErr = upload.err
		}
		delete(v.inodeUploads, ino)
	}
	v.pendingUploadsMu.Unlock()

	return firstErr
}

type memoryAppender struct {
	mu       sync.Mutex
	count    uint64
	startSeq uint64
	payloads [][]byte
}

func (m *memoryAppender) Append(_ context.Context, payload []byte) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.count++
	if m.startSeq == 0 {
		m.startSeq = 1
	}
	m.payloads = append(m.payloads, payload)
	return m.count, nil
}

func (m *memoryAppender) Payloads() [][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([][]byte, len(m.payloads))
	copy(cp, m.payloads)
	return cp
}

func (m *memoryAppender) StartSeq() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.startSeq == 0 {
		return 1
	}
	return m.startSeq
}

func (v *Volume) initMetadataStreamLocked() error {
	var appender record.Appender
	if v.stream != nil {
		appender = sds.NewWALAppender(v.stream, walclient.Local)
	} else {
		if v.memAppender == nil {
			v.memAppender = &memoryAppender{}
		}
		appender = v.memAppender
	}
	w := sds.NewWriter(appender)
	if _, err := w.RegisterType(&pb.Inode{}, 1); err != nil {
		return fmt.Errorf("failed to register Inode type: %w", err)
	}
	if _, err := w.RegisterType(&pb.DirEntry{}, 1, 2); err != nil {
		return fmt.Errorf("failed to register DirEntry type: %w", err)
	}
	if _, err := w.RegisterType(&pb.FileChunk{}, 1, 2); err != nil {
		return fmt.Errorf("failed to register FileChunk type: %w", err)
	}
	if _, err := w.RegisterType(&pb.Content{}, 1); err != nil {
		return fmt.Errorf("failed to register Content type: %w", err)
	}
	v.metadataStream = w
	return nil
}

func (v *Volume) ensureInodeChunksLoadedLocked(ctx context.Context, tx *sds.Tx, node *CachedInode) error {
	v.checkReadAllowedLocked(tx, "objectfs.v1alpha1.FileChunk")
	ino := node.Row.GetIno()
	chunkPrefix, pErr := sds.EncodeKeyPrefix(&pb.FileChunk{Ino: proto.Uint64(ino)}, 1)
	if pErr != nil {
		panic(fmt.Sprintf("objectfs: failed to encode chunk prefix for inode %d: %v", ino, pErr))
	}
	alreadyRecorded := tx != nil && tx.HasReadPrefix("objectfs.v1alpha1.FileChunk", chunkPrefix)
	v.recordReadPrefixLocked(tx, "objectfs.v1alpha1.FileChunk", chunkPrefix)

	// Invariant: directories and 0-byte regular files never have chunk rows or inline data
	// in the database. Recording the prefix as read establishes that any FileChunk key for
	// this inode is known absent without requiring a physical index scan.
	if node.Row.GetIsDir() || node.Row.GetSize() == 0 {
		return nil
	}
	if node.Row.GetChunkSize() > 0 && len(node.Chunks) > 0 {
		if tx != nil && !alreadyRecorded {
			for idx, sha := range node.Chunks {
				chunk := &pb.FileChunk{
					Ino:    proto.Uint64(ino),
					Index:  proto.Uint32(idx),
					Sha256: sha,
				}
				k, err := pkFileChunk.Extract(chunk)
				if err != nil {
					panic(fmt.Sprintf("objectfs: failed to extract chunk key for inode %d index %d: %v", ino, idx, err))
				}
				v.recordReadLocked(tx, "objectfs.v1alpha1.FileChunk", k, chunk)
			}
			if len(node.InlineData) > 0 {
				chunk := &pb.FileChunk{
					Ino:        proto.Uint64(ino),
					Index:      proto.Uint32(0),
					InlineData: append([]byte(nil), node.InlineData...),
				}
				k, err := pkFileChunk.Extract(chunk)
				if err != nil {
					panic(fmt.Sprintf("objectfs: failed to extract inline chunk key for inode %d: %v", ino, err))
				}
				v.recordReadLocked(tx, "objectfs.v1alpha1.FileChunk", k, chunk)
			}
		}
		return nil
	}
	if v.metadataView != nil {
		prefixBytes, pErr := sds.EncodeKeyPrefix(&pb.FileChunk{Ino: proto.Uint64(node.Row.GetIno())}, 1)
		if pErr == nil {
			// Rows returned by scanSQLiteRowsLocked are shared/immutable; only read fields here.
			chunkMsgs, sErr := v.scanSQLiteRowsLocked(ctx, tx, "objectfs.v1alpha1.FileChunk", prefixBytes)
			if sErr == nil {
				for _, cMsg := range chunkMsgs {
					chunk := cMsg.(*pb.FileChunk)
					if chunk.GetIndex() == 0 && len(chunk.GetInlineData()) > 0 {
						node.InlineData = append([]byte(nil), chunk.GetInlineData()...)
					} else if chunk.GetSha256() != "" {
						if node.Chunks == nil {
							node.Chunks = make(map[uint32]string)
						}
						node.Chunks[chunk.GetIndex()] = chunk.GetSha256()
					}
				}
			}
		}
		return nil
	}
	return nil
}

func (v *Volume) readChunkLocked(ctx context.Context, tx *sds.Tx, node *CachedInode, chunkIdx int) ([]byte, error) {
	if chunkIdx == 0 && len(node.InlineData) > 0 {
		res := make([]byte, len(node.InlineData))
		copy(res, node.InlineData)
		return res, nil
	}

	if node.StagedChunks != nil {
		if data, ok := node.StagedChunks[chunkIdx]; ok {
			res := make([]byte, len(data))
			copy(res, data)
			return res, nil
		}
	}

	if node.DirtyChunks != nil {
		if data, ok := node.DirtyChunks[chunkIdx]; ok {
			res := make([]byte, len(data))
			copy(res, data)
			return res, nil
		}
	}

	if err := v.ensureInodeChunksLoadedLocked(ctx, tx, node); err != nil {
		return nil, fmt.Errorf("read chunk: inode %d chunks load: %w", node.Row.GetIno(), err)
	}

	if chunkSha, ok := node.Chunks[uint32(chunkIdx)]; ok && chunkSha != "" {
		if v.blobStore == nil {
			return nil, fmt.Errorf("read chunk: inode %d chunk %d: blob store not configured", node.Row.GetIno(), chunkIdx)
		}
		stream, err := v.blobStore.GetBlob(ctx, chunkSha)
		if err != nil {
			v.pendingUploadsMu.Lock()
			upload := v.inodeUploads[node.Row.GetIno()]
			v.pendingUploadsMu.Unlock()
			if upload != nil {
				upload.wg.Wait()
				stream, err = v.blobStore.GetBlob(ctx, chunkSha)
			}
		}
		if err != nil {
			return nil, fmt.Errorf("read chunk: inode %d chunk %d sha %s: %w", node.Row.GetIno(), chunkIdx, chunkSha, err)
		}
		defer stream.Close()
		data, err := io.ReadAll(stream)
		if err != nil {
			return nil, fmt.Errorf("read chunk: inode %d chunk %d sha %s read: %w", node.Row.GetIno(), chunkIdx, chunkSha, err)
		}
		return data, nil
	}

	if chunkIdx == 0 && (node.Row.GetSha256() != "" || node.Row.GetContentSha256() != "") && len(node.Chunks) == 0 {
		sha := node.Row.GetSha256()
		if sha == "" {
			sha = node.Row.GetContentSha256()
		}
		if v.blobStore == nil {
			return nil, fmt.Errorf("read chunk: inode %d chunk 0: blob store not configured", node.Row.GetIno())
		}
		stream, err := v.blobStore.GetBlob(ctx, sha)
		if err != nil {
			v.pendingUploadsMu.Lock()
			upload := v.inodeUploads[node.Row.GetIno()]
			v.pendingUploadsMu.Unlock()
			if upload != nil {
				upload.wg.Wait()
				stream, err = v.blobStore.GetBlob(ctx, sha)
			}
		}
		if err != nil {
			return nil, fmt.Errorf("read chunk: inode %d chunk 0 sha %s: %w", node.Row.GetIno(), sha, err)
		}
		defer stream.Close()
		data, err := io.ReadAll(stream)
		if err != nil {
			return nil, fmt.Errorf("read chunk: inode %d chunk 0 sha %s read: %w", node.Row.GetIno(), sha, err)
		}
		return data, nil
	}

	if node.Data != nil && node.Row.GetChunkSize() > 0 {
		off := int64(chunkIdx) * int64(node.Row.GetChunkSize())
		if off < node.Row.GetSize() {
			readLen := int64(node.Row.GetChunkSize())
			if off+readLen > node.Row.GetSize() {
				readLen = node.Row.GetSize() - off
			}
			buf := make([]byte, readLen)
			if err := node.Data.Rewind(); err != nil {
				return nil, fmt.Errorf("read chunk: inode %d chunk %d rewind: %w", node.Row.GetIno(), chunkIdx, err)
			}
			if _, err := node.Data.Seek(off, io.SeekStart); err != nil {
				return nil, fmt.Errorf("read chunk: inode %d chunk %d seek: %w", node.Row.GetIno(), chunkIdx, err)
			}
			n, err := io.ReadFull(node.Data, buf)
			if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
				return nil, fmt.Errorf("read chunk: inode %d chunk %d read: %w", node.Row.GetIno(), chunkIdx, err)
			}
			return buf[:n], nil
		}
	}

	return nil, nil
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
	v.closed = true
	if v.closedCh != nil {
		select {
		case <-v.closedCh:
		default:
			close(v.closedCh)
		}
	}
	mView := v.metadataView
	v.mu.Unlock()

	var firstErr error
	if mView != nil {
		if err := mView.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	v.batchCheckWg.Wait()

	v.mu.Lock()
	defer v.mu.Unlock()

	v.assertStatsMatchLocked("at Close")

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
func (v *Volume) makeWaitFn(commitSeq uint64, reqLevel *walclient.Level) func(context.Context) error {
	if commitSeq > v.lastCommitSeq {
		v.lastCommitSeq = commitSeq
	}
	if v.stream == nil || commitSeq == 0 {
		return nil
	}
	durability := v.durability
	if reqLevel != nil {
		durability = *reqLevel
	}

	return func(waitCtx context.Context) error {
		switch durability {
		case walclient.Permanent:
			return v.stream.Wait(waitCtx, commitSeq, walclient.Permanent, true)
		case walclient.Witness:
			return v.stream.Wait(waitCtx, commitSeq, walclient.Witness, false)
		case walclient.Local:
			// Append already fsynced locally
			return nil
		}
		return nil
	}
}

// Position returns the latest committed stream sequence number for this volume.
func (v *Volume) Position() uint64 {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.lastCommitSeq
}

// SafeSnapshotPosition returns the highest stream sequence number that is safe to snapshot
// (no pending transaction and never beyond the Permanent / s3Seq watermark).
func (v *Volume) SafeSnapshotPosition() uint64 {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.safeSnapshotPositionLocked()
}

func (v *Volume) safeSnapshotPositionLocked() uint64 {
	snapPos := v.lastCommitSeq
	if v.stream != nil {
		_, _, s3Seq := v.stream.Watermarks()
		if s3Seq > 0 && snapPos > s3Seq {
			snapPos = s3Seq
		} else if s3Seq > 0 && snapPos == 0 {
			snapPos = s3Seq
		}
	}
	return snapPos
}

func (v *Volume) allocInode(tx *sds.Tx) uint64 {
	ino := atomic.AddUint64(&v.nextInode, erofs.DefaultInodeStride) - erofs.DefaultInodeStride
	if tx != nil {
		// Soundness: the allocator assigns strictly monotonic inode numbers and never reuses numbers
		// (initialized from max(max_ino, replayed) + stride). Therefore, the new inode row and all its
		// potential FileChunk keys are guaranteed to be absent without needing view/index lookups.
		key, err := pkInode.Extract(&pb.Inode{Ino: proto.Uint64(ino)})
		if err != nil {
			panic(fmt.Sprintf("objectfs: failed to extract inode key for %d: %v", ino, err))
		}
		v.recordReadLocked(tx, "objectfs.v1alpha1.Inode", key, nil)
		chunkPrefix, pErr := sds.EncodeKeyPrefix(&pb.FileChunk{Ino: proto.Uint64(ino)}, 1)
		if pErr != nil {
			panic(fmt.Sprintf("objectfs: failed to encode chunk prefix for inode %d: %v", ino, pErr))
		}
		v.recordReadPrefixLocked(tx, "objectfs.v1alpha1.FileChunk", chunkPrefix)
	}
	return ino
}

func cleanPath(p string) string {
	cleaned := path.Clean("/" + p)
	if !strings.HasPrefix(cleaned, "/") {
		cleaned = "/" + cleaned
	}
	return cleaned
}

var (
	pkInode     = sds.NewPrimaryKey(1)
	pkDirEntry  = sds.NewPrimaryKey(1, 2)
	pkFileChunk = sds.NewPrimaryKey(1, 2)
)

// VolumeStats contains aggregated operational and cache metrics for a Volume.
type VolumeStats struct {
	Lag            uint64
	UnappliedBytes int64
	Failures       uint64
	IsDegraded     bool
	Cache          view.LRUCacheStats

	Stats *pb.VolumeStats
}

// Stats returns a snapshot of volume operational and cache metrics.
func (v *Volume) Stats() VolumeStats {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if v.metadataView == nil {
		return VolumeStats{}
	}
	var st *pb.VolumeStats
	if v.liveStats != nil {
		st = proto.Clone(v.liveStats).(*pb.VolumeStats)
	} else if m := v.metadataView.Stats(); m != nil {
		if vs, ok := m.(*pb.VolumeStats); ok && vs != nil {
			st = proto.Clone(vs).(*pb.VolumeStats)
		}
	}
	if st == nil {
		st = &pb.VolumeStats{Name: VolumeStatsRowName}
	}
	return VolumeStats{
		Lag:            v.metadataView.Lag(v.lastCommitSeq),
		UnappliedBytes: v.metadataView.UnappliedBytes(),
		Failures:       v.metadataView.ApplierFailures(),
		IsDegraded:     v.metadataView.IsDegraded(),
		Cache:          v.metadataView.CacheStats(),
		Stats:          st,
	}
}

// MetadataCacheStats returns cache hit/miss and memory usage statistics.
func (v *Volume) MetadataCacheStats() view.LRUCacheStats {
	return v.Stats().Cache
}

// MetadataCacheResetStats resets the cache hits and misses counters.
func (v *Volume) MetadataCacheResetStats() {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.metadataView != nil {
		v.metadataView.CacheResetStats()
	}
}

// SetMetadataCacheLimits updates the entry count and byte capacity of the metadata read cache.
func (v *Volume) SetMetadataCacheLimits(maxEntries int, maxBytes int64) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.metadataView != nil {
		v.metadataView.SetCacheLimits(maxEntries, maxBytes)
	}
}

// ApplyLag returns the difference between the stream sequence of the last committed transaction
// and the position applied to the metadata index.
func (v *Volume) ApplyLag() uint64 {
	return v.Stats().Lag
}

// ApplyFailures returns the total number of batch apply failures encountered by the background applier.
func (v *Volume) ApplyFailures() uint64 {
	return v.Stats().Failures
}

// IsDegraded returns whether the metadata index is currently in a degraded state due to persistent errors.
func (v *Volume) IsDegraded() bool {
	return v.Stats().IsDegraded
}

// UnappliedBytes returns the current memory footprint in bytes of unapplied overlay rows.
func (v *Volume) UnappliedBytes() int64 {
	return v.Stats().UnappliedBytes
}

// SetApplierFaultHook configures a fault injection callback for the background applier (testing only).
func (v *Volume) SetApplierFaultHook(hook func() error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.metadataView != nil {
		v.metadataView.SetFaultHook(hook)
	}
}

// MetadataAppliedPosition returns the highest stream sequence position applied to the metadata index.
func (v *Volume) MetadataAppliedPosition() uint64 {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if v.metadataView != nil {
		return v.metadataView.AppliedPosition()
	}
	return 0
}

// View returns the underlying view layer for the volume.
func (v *Volume) View() *view.View {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.metadataView
}

// Index returns the underlying local index for the volume.
func (v *Volume) Index() sds.LocalIndex {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if v.metadataView == nil {
		return nil
	}
	return v.metadataView.Index()
}

// FlushOverlay blocks until all currently committed stream transactions have been applied to the metadata index.
func (v *Volume) FlushOverlay(ctx context.Context) error {
	v.mu.Lock()
	mView := v.metadataView
	targetSeq := v.lastCommitSeq
	v.mu.Unlock()

	if mView == nil {
		return nil
	}
	if err := mView.FlushTo(ctx, targetSeq); err != nil {
		return err
	}
	v.batchCheckWg.Wait()
	func() {
		v.mu.Lock()
		defer v.mu.Unlock()
		v.assertStatsMatchLocked("after FlushOverlay")
	}()
	return nil
}

func (v *Volume) flushOverlayLocked(ctx context.Context) error {
	if v.metadataView == nil {
		return nil
	}
	if err := v.metadataView.FlushTo(ctx, v.lastCommitSeq); err != nil {
		return err
	}
	v.assertStatsMatchLocked("after flushOverlayLocked")
	return nil
}

func (v *Volume) checkBackpressureLocked(ctx context.Context) error {
	if v.metadataView == nil {
		return nil
	}
	return v.metadataView.WaitBackpressure(ctx)
}

func (v *Volume) recordOverlayTxChangesLocked(tx *sds.Tx) {
	if tx == nil || v.metadataView == nil {
		return
	}
	changes := tx.Changes()
	if len(changes) == 0 {
		return
	}
	v.metadataView.ApplyChanges(changes)
}

func (v *Volume) beginTxLocked(opName string) *sds.Tx {
	if v.metadataStream == nil {
		return nil
	}
	tx := v.metadataStream.Begin()
	tx.SetOpName(opName)
	return tx
}

func (v *Volume) checkReadAllowedLocked(tx *sds.Tx, typeName string) {
	if tx == nil {
		return
	}
	if tx.HasWrites() {
		op := tx.OpName()
		if op == "" {
			op = "unknown"
		}
		panic(fmt.Sprintf("objectfs: operation %q read %q after transaction buffered write %q", op, typeName, tx.FirstWrite()))
	}
}

func (v *Volume) recordReadLocked(tx *sds.Tx, typeName string, key sds.Key, msg proto.Message) {
	if tx != nil {
		tx.RecordRead(typeName, key, msg)
	}
}

func (v *Volume) recordReadPrefixLocked(tx *sds.Tx, typeName string, prefix []byte) {
	if tx != nil {
		tx.RecordReadPrefix(typeName, prefix)
	}
}

// scanLimitSQLiteRowsLocked queries the view for proto rows matching prefixBytes up to limit (0 for unlimited).
// It enforces the reads-before-writes assertion, records all returned rows into the read set, and records
// the prefix as completely read only if the scan was not truncated by the limit.
func (v *Volume) scanLimitSQLiteRowsLocked(ctx context.Context, tx *sds.Tx, typeName string, prefixBytes []byte, limit int) ([]proto.Message, error) {
	v.checkReadAllowedLocked(tx, typeName)
	if v.metadataView == nil {
		return nil, nil
	}
	allMsgs, err := v.metadataView.ScanSlice(ctx, typeName, prefixBytes)
	if err != nil {
		return nil, err
	}
	truncated := false
	msgs := allMsgs
	if limit > 0 && len(msgs) > limit {
		msgs = msgs[:limit]
		truncated = true
	}
	if len(prefixBytes) > 0 && !truncated {
		v.recordReadPrefixLocked(tx, typeName, prefixBytes)
	}
	if tx != nil {
		for _, msg := range msgs {
			var pk *sds.PrimaryKey
			switch typeName {
			case "objectfs.v1alpha1.Inode":
				pk = pkInode
			case "objectfs.v1alpha1.DirEntry":
				pk = pkDirEntry
			case "objectfs.v1alpha1.FileChunk":
				pk = pkFileChunk
			}
			if pk != nil {
				k, err := pk.Extract(msg)
				if err != nil {
					panic(fmt.Sprintf("objectfs: failed to extract primary key for %s: %v", typeName, err))
				}
				v.recordReadLocked(tx, typeName, k, msg)
			}
		}
	}
	return msgs, nil
}

// scanSQLiteRowsLocked queries the view for all proto rows matching prefixBytes.
// The returned messages are shared and immutable; callers must not modify them in place.
func (v *Volume) scanSQLiteRowsLocked(ctx context.Context, tx *sds.Tx, typeName string, prefixBytes []byte) ([]proto.Message, error) {
	return v.scanLimitSQLiteRowsLocked(ctx, tx, typeName, prefixBytes, 0)
}

// getSQLiteRowLocked retrieves a single proto row from the view.
// The returned message is shared and immutable; callers must not modify it in place.
func (v *Volume) getSQLiteRowLocked(ctx context.Context, tx *sds.Tx, typeName string, key sds.Key) (proto.Message, bool, error) {
	v.checkReadAllowedLocked(tx, typeName)
	if v.metadataView == nil {
		v.recordReadLocked(tx, typeName, key, nil)
		return nil, false, nil
	}
	msg, ok, err := v.metadataView.Get(ctx, typeName, key)
	if err != nil {
		return nil, false, err
	}
	if ok {
		v.recordReadLocked(tx, typeName, key, msg)
	} else {
		v.recordReadLocked(tx, typeName, key, nil)
	}
	return msg, ok, nil
}

func (v *Volume) normalizeInodeID(id uint64) uint64 {
	if id == 0 {
		if v.rootInodeID != 0 {
			return v.rootInodeID
		}
		return 1
	}
	return id
}

func (v *Volume) cleanOrphanInodesLocked(ctx context.Context) error {
	tx := v.beginTxLocked("cleanOrphanInodes")

	prefix, err := sds.EncodeKeyPrefix(&pb.Inode{}, 0)
	if err != nil {
		return fmt.Errorf("failed to encode inode key prefix: %w", err)
	}
	msgs, err := v.scanSQLiteRowsLocked(ctx, tx, "objectfs.v1alpha1.Inode", prefix)
	if err != nil {
		return fmt.Errorf("failed to scan orphan inodes: %w", err)
	}
	var orphanInos []uint64
	for _, msg := range msgs {
		node := msg.(*pb.Inode)
		if !node.GetIsDir() && node.GetNlink() == 0 {
			orphanInos = append(orphanInos, node.GetIno())
		}
	}
	if len(orphanInos) == 0 {
		return nil
	}

	// Reads before writes: scan all orphan chunk rows before buffering any deletions into tx.
	orphanChunks := make(map[uint64][]proto.Message, len(orphanInos))
	for _, ino := range orphanInos {
		chunkPrefix, err := sds.EncodeKeyPrefix(&pb.FileChunk{Ino: proto.Uint64(ino)}, 1)
		if err != nil {
			return fmt.Errorf("failed to encode chunk key prefix for inode %d: %w", ino, err)
		}
		chunkMsgs, err := v.scanSQLiteRowsLocked(ctx, tx, "objectfs.v1alpha1.FileChunk", chunkPrefix)
		if err != nil {
			return fmt.Errorf("failed to scan chunk rows for orphan inode %d: %w", ino, err)
		}
		orphanChunks[ino] = chunkMsgs
	}

	for _, ino := range orphanInos {
		if _, err := tx.Delete(ctx, &pb.Inode{Ino: proto.Uint64(ino)}); err != nil {
			return fmt.Errorf("failed to log orphan inode %d deletion: %w", ino, err)
		}
		for _, cMsg := range orphanChunks[ino] {
			c := cMsg.(*pb.FileChunk)
			if _, err := tx.Delete(ctx, &pb.FileChunk{Ino: proto.Uint64(ino), Index: proto.Uint32(c.GetIndex())}); err != nil {
				return fmt.Errorf("failed to log chunk deletion for orphan inode %d: %w", ino, err)
			}
		}
	}
	commitSeq, err := tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("failed to commit orphan deletion transaction: %w", err)
	}
	v.applyTxChangesLocked(ctx, tx)
	waitFn := v.makeWaitFn(commitSeq, nil)
	if waitFn != nil {
		if err := waitFn(ctx); err != nil {
			return fmt.Errorf("failed to wait for orphan deletion sync: %w", err)
		}
	}
	return nil
}

func (v *Volume) getOrLoadInodeLocked(ctx context.Context, tx *sds.Tx, inodeID uint64) (*CachedInode, error) {
	inodeID = v.normalizeInodeID(inodeID)

	key, err := pkInode.Extract(&pb.Inode{Ino: proto.Uint64(inodeID)})
	if err != nil {
		return nil, err
	}
	// Loaded Inode message is shared/immutable; CachedInode.mutate must be used for any writes.
	msg, ok, err := v.getSQLiteRowLocked(ctx, tx, "objectfs.v1alpha1.Inode", key)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("inode %d: %w", inodeID, syscall.ENOENT)
	}
	inode := msg.(*pb.Inode)
	if inode.GetIsDir() && inode.GetParentIno() != 0 {
		if v.dirParents == nil {
			v.dirParents = make(map[uint64]uint64)
		}
		v.dirParents[inode.GetIno()] = inode.GetParentIno()
	}
	node := &CachedInode{
		Row: inode,
	}

	_ = v.ensureInodeChunksLoadedLocked(ctx, tx, node)

	return node, nil
}

func (v *Volume) getDirEntrySQLiteLocked(ctx context.Context, tx *sds.Tx, parentInodeID uint64, name string) (*pb.DirEntry, bool, error) {
	if v.metadataView == nil {
		return nil, false, nil
	}
	key, err := pkDirEntry.Extract(&pb.DirEntry{
		ParentIno: proto.Uint64(parentInodeID),
		Name:      proto.String(name),
	})
	if err != nil {
		return nil, false, err
	}
	// Loaded DirEntry message is shared/immutable.
	msg, ok, err := v.getSQLiteRowLocked(ctx, tx, "objectfs.v1alpha1.DirEntry", key)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	return msg.(*pb.DirEntry), true, nil
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
		if len(part) > MaxNameLength {
			return 0, 0, "", fmt.Errorf("path component %q exceeds maximum length: %w", part, syscall.ENAMETOOLONG)
		}
		de, ok, err := v.getDirEntrySQLiteLocked(ctx, nil, currInodeID, part)
		if err != nil {
			return 0, 0, "", fmt.Errorf("directory for inode %d not found: %w", currInodeID, err)
		}
		if !ok {
			return 0, 0, "", fmt.Errorf("path component %s not found: %w", part, syscall.ENOENT)
		}
		entryInodeID := de.GetIno()
		isDir := de.GetIsDir()

		if i == len(parts)-1 {
			return entryInodeID, currInodeID, part, nil
		}
		if !isDir {
			return 0, 0, "", fmt.Errorf("path component %s is not a directory: %w", part, syscall.ENOTDIR)
		}
		parentInodeID = currInodeID
		baseName = part
		currInodeID = entryInodeID
	}
	return currInodeID, parentInodeID, baseName, nil
}

func toEntryAttr(row *pb.Inode, name, redirectURL string, rootInodeID uint64) *pb.EntryAttr {
	if row == nil {
		return nil
	}
	ino := row.GetIno()
	if name == "" && (ino == rootInodeID || ino == 1) {
		name = "/"
	}
	if rootInodeID != 0 && ino == rootInodeID && rootInodeID != 1 {
		rowCopy := proto.Clone(row).(*pb.Inode)
		rowCopy.Ino = proto.Uint64(1)
		row = rowCopy
	}
	return &pb.EntryAttr{
		Inode:       row,
		Name:        name,
		RedirectUrl: redirectURL,
	}
}

func (v *Volume) toEntryAttrLocked(ctx context.Context, tx *sds.Tx, inodeID uint64, name string) (*pb.EntryAttr, error) {
	node, err := v.getOrLoadInodeLocked(ctx, tx, inodeID)
	if err != nil {
		return nil, err
	}
	return toEntryAttr(node.Row, name, node.RedirectURL, v.rootInodeID), nil
}

func (v *Volume) GetAttr(ctx context.Context, inodeID uint64) (*pb.EntryAttr, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	inodeID = v.normalizeInodeID(inodeID)
	return v.toEntryAttrLocked(ctx, nil, inodeID, "")
}

func (v *Volume) SetAttr(ctx context.Context, inodeID uint64, mode *uint32, uid *uint32, gid *uint32, atime *time.Time, atimeNow bool, mtime *time.Time, mtimeNow bool, ctime *time.Time, ctimeNow bool) (*pb.EntryAttr, error) {
	attr, waitFn, err := func() (*pb.EntryAttr, func(context.Context) error, error) {
		v.mu.Lock()
		defer v.mu.Unlock()

		tx := v.beginTxLocked("SetAttr")

		if err := v.checkBackpressureLocked(ctx); err != nil {
			return nil, nil, err
		}

		inodeID = v.normalizeInodeID(inodeID)

		node, err := v.getOrLoadInodeLocked(ctx, tx, inodeID)
		if err != nil {
			return nil, nil, err
		}

		now := time.Now()
		modified := (mode != nil) || (uid != nil) || (gid != nil) || atimeNow || (atime != nil) || mtimeNow || (mtime != nil) || ctimeNow || (ctime != nil)

		if !modified {
			attr, err := v.toEntryAttrLocked(ctx, tx, inodeID, "")
			return attr, nil, err
		}

		node.mutate(func(row *pb.Inode) {
			if mode != nil {
				// keep setuid, setgid and sticky (07777), and the file-type bits untouched by chmod
				row.Mode = (row.GetMode() & ^uint32(07777)) | (*mode & 07777)
			}
			if uid != nil {
				row.Uid = *uid
			}
			if gid != nil {
				row.Gid = *gid
			}
			if atimeNow {
				row.Atime = timestamppb.New(now)
			} else if atime != nil {
				row.Atime = timestamppb.New(*atime)
			}
			if mtimeNow {
				row.Mtime = timestamppb.New(now)
			} else if mtime != nil {
				row.Mtime = timestamppb.New(*mtime)
			}

			if ctimeNow {
				row.Ctime = timestamppb.New(now)
			} else if ctime != nil {
				row.Ctime = timestamppb.New(*ctime)
			} else {
				row.Ctime = timestamppb.New(now)
			}
		})

		if _, err := tx.Update(ctx, node.Row); err != nil {
			return nil, nil, fmt.Errorf("failed to log inode update: %w", err)
		}

		commitSeq, err := tx.Commit(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to commit setattr transaction: %w", err)
		}
		v.applyTxChangesLocked(ctx, tx)

		waitFn := v.makeWaitFn(commitSeq, nil)

		attr := toEntryAttr(node.Row, "", node.RedirectURL, v.rootInodeID)
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
	if v.broadcaster != nil {
		v.broadcaster.Broadcast(v.volumeID, &pb.WatchVolumeResponse{
			EventType: pb.WatchEventType_EVENT_MODIFIED,
			Attr:      attr,
			Inode:     attr.GetInode().GetIno(),
		})
	}
	return attr, nil
}

func (v *Volume) Lookup(ctx context.Context, parentInodeID uint64, name string) (*pb.EntryAttr, error) {
	if len(name) > MaxNameLength {
		return nil, fmt.Errorf("name %q exceeds maximum length: %w", name, syscall.ENAMETOOLONG)
	}

	v.mu.RLock()
	defer v.mu.RUnlock()

	parentInodeID = v.normalizeInodeID(parentInodeID)

	if name == "." {
		return v.toEntryAttrLocked(ctx, nil, parentInodeID, ".")
	}
	if name == ".." {
		if parentInodeID == v.rootInodeID || parentInodeID == 1 {
			return v.toEntryAttrLocked(ctx, nil, v.rootInodeID, "..")
		}
		parentNode, err := v.getOrLoadInodeLocked(ctx, nil, parentInodeID)
		if err != nil {
			return nil, err
		}
		if !parentNode.Row.GetIsDir() {
			return nil, syscall.ENOTDIR
		}
		pIno := parentNode.Row.GetParentIno()
		if pIno == 0 {
			pIno = v.dirParents[parentInodeID]
		}
		if pIno == 0 {
			pIno = v.rootInodeID
		}
		return v.toEntryAttrLocked(ctx, nil, pIno, "..")
	}

	de, ok, err := v.getDirEntrySQLiteLocked(ctx, nil, parentInodeID, name)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("child %s not found in inode %d: %w", name, parentInodeID, syscall.ENOENT)
	}
	return v.toEntryAttrLocked(ctx, nil, de.GetIno(), name)
}

func (v *Volume) ReadDir(ctx context.Context, dirInodeID uint64) ([]*pb.EntryAttr, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	dirInodeID = v.normalizeInodeID(dirInodeID)

	dirNode, err := v.getOrLoadInodeLocked(ctx, nil, dirInodeID)
	if err != nil {
		return nil, err
	}
	if !dirNode.Row.GetIsDir() {
		return nil, fmt.Errorf("inode %d is not a directory: %w", dirInodeID, syscall.ENOTDIR)
	}

	prefixBytes, pErr := sds.EncodeKeyPrefix(&pb.DirEntry{ParentIno: proto.Uint64(dirInodeID)}, 1)
	if pErr != nil {
		return nil, pErr
	}
	entryMsgs, err := v.scanSQLiteRowsLocked(ctx, nil, "objectfs.v1alpha1.DirEntry", prefixBytes)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entryMsgs))
	entriesMap := make(map[string]*pb.DirEntry, len(entryMsgs))
	for _, msg := range entryMsgs {
		de := msg.(*pb.DirEntry)
		name := de.GetName()
		names = append(names, name)
		entriesMap[name] = de
	}
	sort.Strings(names)

	var entries []*pb.EntryAttr
	for _, name := range names {
		de := entriesMap[name]
		attr, err := v.toEntryAttrLocked(ctx, nil, de.GetIno(), name)
		if err == nil {
			entries = append(entries, attr)
		}
	}
	return entries, nil
}

// applyTxChangesLocked updates live volume stats and records overlay changes for a committed transaction.
//
// The write-path consistency contract:
//  1. Every operation runs under v.mu, so the view it reads is the stream head: the lock is the snapshot.
//     Because all mutations acquire v.mu, no concurrent commits can interleave. Reading from the
//     metadata view under v.mu observes the exact stream head state without needing a separate MVCC snapshot.
//  2. An operation reads before it writes; writes are buffered in the Tx; reads never depend on the
//     operation's own buffered writes. Because all needed pre-state is read prior to buffering changes in Tx,
//     operations do not require read-your-writes semantics from the view during transaction construction.
//  3. At commit, before-images come from the operation's read set; the after-image is the buffered row;
//     stats deltas come from those pairs with the applier's arithmetic. Because operations read before they
//     write, the pre-state of every written key was already recorded in the transaction's read set.
//     Commit performs zero index/SQLite reads, making applyTxChangesLocked infallible. Invariant violations
//     panic in all modes.
func (v *Volume) applyTxChangesLocked(ctx context.Context, tx *sds.Tx) {
	if tx == nil {
		return
	}
	if v.liveStats != nil {
		var reg *record.Registry
		if v.metadataStream != nil {
			reg = v.metadataStream.Registry()
		}
		beforeLookup := func(typeName string, key sds.Key) (proto.Message, bool, error) {
			msg, ok := tx.LookupRead(typeName, key)
			if !ok {
				panic(fmt.Sprintf("objectfs: key written but never read: type=%s key=%s", typeName, key.String()))
			}
			if msg == nil {
				return nil, false, nil
			}
			return msg, true, nil
		}
		if err := view.UpdateStatsFromChanges(tx.Changes(), v.liveStats, UpdateVolumeStats, reg, beforeLookup); err != nil {
			panic(fmt.Sprintf("objectfs: volume stats update failed [%s]: %v", v.volumeID, err))
		}
	}
	v.recordOverlayTxChangesLocked(tx) // always: the tx is committed
}

func (v *Volume) Mkdir(ctx context.Context, parentInodeID uint64, name string, mode uint32, uid, gid uint32) (*pb.EntryAttr, error) {
	if len(name) > MaxNameLength {
		return nil, fmt.Errorf("directory name %q exceeds maximum length: %w", name, syscall.ENAMETOOLONG)
	}

	attr, waitFn, err := func() (*pb.EntryAttr, func(context.Context) error, error) {
		v.mu.Lock()
		defer v.mu.Unlock()

		tx := v.beginTxLocked("Mkdir")

		if err := v.checkBackpressureLocked(ctx); err != nil {
			return nil, nil, err
		}

		parentInodeID = v.normalizeInodeID(parentInodeID)
		if name == "" || name == "." || name == ".." {
			return nil, nil, fmt.Errorf("invalid directory name %q: %w", name, syscall.EINVAL)
		}

		parentInode, err := v.getOrLoadInodeLocked(ctx, tx, parentInodeID)
		if err != nil {
			return nil, nil, err
		}
		if !parentInode.Row.GetIsDir() {
			return nil, nil, fmt.Errorf("parent inode %d is not a directory: %w", parentInodeID, syscall.ENOTDIR)
		}

		_, exists, err := v.getDirEntrySQLiteLocked(ctx, tx, parentInodeID, name)
		if err != nil {
			return nil, nil, err
		}
		if exists {
			return nil, nil, fmt.Errorf("directory %s already exists under inode %d: %w", name, parentInodeID, syscall.EEXIST)
		}

		if mode == 0 {
			mode = 0755
		}
		mode |= syscall.S_IFDIR

		if (parentInode.Row.GetMode() & 02000) != 0 {
			gid = parentInode.Row.GetGid()
			mode |= 02000
		}

		now := time.Now()
		childInodeID := v.allocInode(tx)

		parentInode.mutate(func(row *pb.Inode) {
			if row.GetNlink() < 2 {
				row.Nlink = 2
			}
			row.Nlink++
			row.Mtime = timestamppb.New(now)
			row.Ctime = timestamppb.New(now)
		})

		if v.dirParents == nil {
			v.dirParents = make(map[uint64]uint64)
		}
		v.dirParents[childInodeID] = parentInodeID

		childInodeMsg := &pb.Inode{
			Ino:       proto.Uint64(childInodeID),
			Mode:      mode,
			Size:      0,
			Mtime:     timestamppb.New(now),
			Atime:     timestamppb.New(now),
			Ctime:     timestamppb.New(now),
			Uid:       uid,
			Gid:       gid,
			IsDir:     true,
			Nlink:     2,
			ParentIno: proto.Uint64(parentInodeID),
		}
		if _, err := tx.Insert(ctx, childInodeMsg); err != nil {
			delete(v.dirParents, childInodeID)
			return nil, nil, fmt.Errorf("failed to log child inode creation: %w", err)
		}

		dirEntryMsg := &pb.DirEntry{
			ParentIno: proto.Uint64(parentInodeID),
			Name:      proto.String(name),
			Ino:       childInodeID,
			IsDir:     true,
			Mode:      mode,
		}
		if _, err := tx.Insert(ctx, dirEntryMsg); err != nil {
			delete(v.dirParents, childInodeID)
			return nil, nil, fmt.Errorf("failed to log directory entry: %w", err)
		}

		if _, err := tx.Update(ctx, parentInode.Row); err != nil {
			delete(v.dirParents, childInodeID)
			return nil, nil, fmt.Errorf("failed to log parent inode update: %w", err)
		}

		commitSeq, err := tx.Commit(ctx)
		if err != nil {
			delete(v.dirParents, childInodeID)
			return nil, nil, fmt.Errorf("failed to commit mkdir transaction: %w", err)
		}
		v.applyTxChangesLocked(ctx, tx)

		waitFn := v.makeWaitFn(commitSeq, nil)

		attr := toEntryAttr(childInodeMsg, name, "", v.rootInodeID)
		v.broadcaster.Broadcast(v.volumeID, &pb.WatchVolumeResponse{
			EventType:   pb.WatchEventType_EVENT_CREATED,
			Attr:        attr,
			Inode:       childInodeID,
			ParentInode: parentInodeID,
			Name:        name,
		})

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

func (v *Volume) CreateFile(ctx context.Context, parentInodeID uint64, name string, mode uint32, initialContent []byte, uid, gid uint32, rdev ...uint32) (*pb.EntryAttr, error) {
	if len(name) > MaxNameLength {
		return nil, fmt.Errorf("file name %q exceeds maximum length: %w", name, syscall.ENAMETOOLONG)
	}

	attr, waitFn, err := func() (*pb.EntryAttr, func(context.Context) error, error) {
		v.mu.Lock()
		defer v.mu.Unlock()

		tx := v.beginTxLocked("CreateFile")

		if err := v.checkBackpressureLocked(ctx); err != nil {
			return nil, nil, err
		}

		var dev uint32
		if len(rdev) > 0 {
			dev = rdev[0]
		}

		parentInodeID = v.normalizeInodeID(parentInodeID)
		if name == "" || name == "." || name == ".." {
			return nil, nil, fmt.Errorf("invalid file name %q: %w", name, syscall.EINVAL)
		}

		parentInode, err := v.getOrLoadInodeLocked(ctx, tx, parentInodeID)
		if err != nil {
			return nil, nil, err
		}
		if !parentInode.Row.GetIsDir() {
			return nil, nil, fmt.Errorf("parent inode %d is not a directory: %w", parentInodeID, syscall.ENOTDIR)
		}

		var existingChildIno uint64
		var exists bool
		de, ok, err := v.getDirEntrySQLiteLocked(ctx, tx, parentInodeID, name)
		if err != nil {
			return nil, nil, err
		}
		if ok {
			exists = true
			existingChildIno = de.GetIno()
		}

		fileType := mode & syscall.S_IFMT
		if fileType == 0 {
			if (mode & 0777) == 0 {
				mode |= 0644
			}
			mode |= syscall.S_IFREG
			fileType = syscall.S_IFREG
		}

		switch fileType {
		case syscall.S_IFREG, syscall.S_IFIFO, syscall.S_IFSOCK, syscall.S_IFCHR, syscall.S_IFBLK:
			// valid file types
		default:
			return nil, nil, fmt.Errorf("unsupported file type %o: %w", fileType, syscall.EINVAL)
		}

		if (parentInode.Row.GetMode() & 02000) != 0 {
			gid = parentInode.Row.GetGid()
		}

		now := time.Now()
		var hashStr string
		var dataCopy []byte
		if fileType == syscall.S_IFREG {
			if len(initialContent) > 0 {
				h := sha256.Sum256(initialContent)
				hashStr = fmt.Sprintf("%x", h)
			}
			dataCopy = make([]byte, len(initialContent))
			copy(dataCopy, initialContent)
		}

		if exists {
			childInode, err := v.getOrLoadInodeLocked(ctx, tx, existingChildIno)
			if err != nil {
				return nil, nil, err
			}
			if childInode.Row.GetIsDir() {
				return nil, nil, fmt.Errorf("cannot overwrite directory with file: %w", syscall.EISDIR)
			}
			if fileType != syscall.S_IFREG || (childInode.Row.GetMode()&syscall.S_IFMT) != syscall.S_IFREG {
				return nil, nil, fmt.Errorf("file %q already exists: %w", name, syscall.EEXIST)
			}
			if childInode.Data != nil {
				_ = childInode.Data.Close()
				childInode.Data = nil
			}

			effectiveChunkSize := childInode.Row.GetChunkSize()
			if effectiveChunkSize == 0 {
				effectiveChunkSize = v.chunkSize
				if effectiveChunkSize == 0 {
					effectiveChunkSize = 64 * 1024
				}
			}

			childInode.mutate(func(row *pb.Inode) {
				row.Mode = mode
				row.Size = int64(len(dataCopy))
				row.Mtime = timestamppb.New(now)
				row.Ctime = timestamppb.New(now)
				row.Uid = uid
				row.Gid = gid
				row.ChunkSize = effectiveChunkSize
				row.ContentSha256 = hashStr
				if len(dataCopy) <= int(v.maxInlineLen) {
					row.ManifestSha256 = ""
				}
			})

			parentInode.mutate(func(row *pb.Inode) {
				row.Mtime = timestamppb.New(now)
				row.Ctime = timestamppb.New(now)
			})

			prefixBytes, pErr := sds.EncodeKeyPrefix(&pb.FileChunk{Ino: proto.Uint64(childInode.Row.GetIno())}, 1)
			if pErr != nil {
				return nil, nil, fmt.Errorf("failed to encode chunk prefix for inode %d: %w", childInode.Row.GetIno(), pErr)
			}
			chunkMsgs, err := v.scanSQLiteRowsLocked(ctx, tx, "objectfs.v1alpha1.FileChunk", prefixBytes)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to scan chunk rows for inode %d: %w", childInode.Row.GetIno(), err)
			}

			if len(dataCopy) <= int(v.maxInlineLen) {
				childInode.InlineData = append([]byte(nil), dataCopy...)
				childInode.Chunks = nil
				childInode.StagedChunks = nil
				childInode.DirtyChunks = nil

				for _, msg := range chunkMsgs {
					c := msg.(*pb.FileChunk)
					if _, err := tx.Delete(ctx, &pb.FileChunk{Ino: proto.Uint64(childInode.Row.GetIno()), Index: proto.Uint32(c.GetIndex())}); err != nil {
						return nil, nil, fmt.Errorf("failed to delete old FileChunk: %w", err)
					}
				}
				if len(dataCopy) > 0 {
					if _, err := tx.Insert(ctx, &pb.FileChunk{Ino: proto.Uint64(childInode.Row.GetIno()), Index: proto.Uint32(0), InlineData: append([]byte(nil), dataCopy...)}); err != nil {
						return nil, nil, fmt.Errorf("failed to log inline FileChunk: %w", err)
					}
				}
			} else {
				childInode.InlineData = nil
				childInode.Chunks = make(map[uint32]string)
				childInode.StagedChunks = make(map[int][]byte)
				childInode.DirtyChunks = nil

				blobsMap := make(map[string]blob.ByteStream)
				cs := int(effectiveChunkSize)
				numChunks := (len(dataCopy) + cs - 1) / cs
				for i := 0; i < numChunks; i++ {
					start := i * cs
					end := (i + 1) * cs
					if end > len(dataCopy) {
						end = len(dataCopy)
					}
					cBytes := make([]byte, end-start)
					copy(cBytes, dataCopy[start:end])
					cSha := fmt.Sprintf("%x", sha256.Sum256(cBytes))
					childInode.Chunks[uint32(i)] = cSha
					childInode.StagedChunks[i] = cBytes
					blobsMap[cSha] = blob.NewByteStreamFromBytes(cBytes)
				}

				if v.blobStore != nil && len(blobsMap) > 0 {
					if err := v.blobStore.PutBlobs(ctx, blobsMap); err != nil {
						return nil, nil, fmt.Errorf("failed to upload blobs: %w", err)
					}
				}

				for _, msg := range chunkMsgs {
					c := msg.(*pb.FileChunk)
					if _, stillPresent := childInode.Chunks[c.GetIndex()]; !stillPresent {
						if _, err := tx.Delete(ctx, &pb.FileChunk{Ino: proto.Uint64(childInode.Row.GetIno()), Index: proto.Uint32(c.GetIndex())}); err != nil {
							return nil, nil, fmt.Errorf("failed to delete old FileChunk: %w", err)
						}
					}
				}
				for i, sha := range childInode.Chunks {
					if _, err := tx.Insert(ctx, &pb.FileChunk{Ino: proto.Uint64(childInode.Row.GetIno()), Index: proto.Uint32(i), Sha256: sha}); err != nil {
						return nil, nil, fmt.Errorf("failed to log FileChunk: %w", err)
					}
				}
			}

			if _, err := tx.Update(ctx, childInode.Row); err != nil {
				return nil, nil, fmt.Errorf("failed to log child inode update: %w", err)
			}
			if _, err := tx.Update(ctx, parentInode.Row); err != nil {
				return nil, nil, fmt.Errorf("failed to log parent inode update: %w", err)
			}

			commitSeq, err := tx.Commit(ctx)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to commit overwrite file transaction: %w", err)
			}
			v.applyTxChangesLocked(ctx, tx)

			waitFn := v.makeWaitFn(commitSeq, nil)

			attr := toEntryAttr(childInode.Row, name, childInode.RedirectURL, v.rootInodeID)
			v.broadcaster.Broadcast(v.volumeID, &pb.WatchVolumeResponse{
				EventType:   pb.WatchEventType_EVENT_MODIFIED,
				Attr:        attr,
				Inode:       childInode.Row.GetIno(),
				ParentInode: parentInodeID,
				Name:        name,
			})
			return attr, waitFn, nil
		}

		childInodeID := v.allocInode(tx)

		parentInode.mutate(func(row *pb.Inode) {
			row.Mtime = timestamppb.New(now)
			row.Ctime = timestamppb.New(now)
		})

		effectiveChunkSize := v.chunkSize
		if effectiveChunkSize == 0 {
			effectiveChunkSize = 64 * 1024
		}

		childInode := &CachedInode{
			Row: &pb.Inode{
				Ino:           proto.Uint64(childInodeID),
				Mode:          mode,
				Size:          int64(len(dataCopy)),
				Mtime:         timestamppb.New(now),
				Atime:         timestamppb.New(now),
				Ctime:         timestamppb.New(now),
				Uid:           uid,
				Gid:           gid,
				IsDir:         false,
				Nlink:         1,
				ChunkSize:     effectiveChunkSize,
				Rdev:          dev,
				ContentSha256: hashStr,
			},
		}

		if fileType == syscall.S_IFREG {
			if len(dataCopy) <= int(v.maxInlineLen) {
				childInode.InlineData = append([]byte(nil), dataCopy...)

				if len(dataCopy) > 0 {
					if _, err := tx.Insert(ctx, &pb.FileChunk{Ino: proto.Uint64(childInodeID), Index: proto.Uint32(0), InlineData: append([]byte(nil), dataCopy...)}); err != nil {
						return nil, nil, fmt.Errorf("failed to log inline FileChunk: %w", err)
					}
				}
			} else {
				childInode.Chunks = make(map[uint32]string)
				childInode.StagedChunks = make(map[int][]byte)

				blobsMap := make(map[string]blob.ByteStream)
				cs := int(effectiveChunkSize)
				numChunks := (len(dataCopy) + cs - 1) / cs
				for i := 0; i < numChunks; i++ {
					start := i * cs
					end := (i + 1) * cs
					if end > len(dataCopy) {
						end = len(dataCopy)
					}
					cBytes := make([]byte, end-start)
					copy(cBytes, dataCopy[start:end])
					cSha := fmt.Sprintf("%x", sha256.Sum256(cBytes))
					childInode.Chunks[uint32(i)] = cSha
					childInode.StagedChunks[i] = cBytes
					blobsMap[cSha] = blob.NewByteStreamFromBytes(cBytes)
				}

				if v.blobStore != nil && len(blobsMap) > 0 {
					if err := v.blobStore.PutBlobs(ctx, blobsMap); err != nil {
						return nil, nil, fmt.Errorf("failed to upload blobs: %w", err)
					}
				}

				for i, sha := range childInode.Chunks {
					if _, err := tx.Insert(ctx, &pb.FileChunk{Ino: proto.Uint64(childInodeID), Index: proto.Uint32(i), Sha256: sha}); err != nil {
						return nil, nil, fmt.Errorf("failed to log FileChunk: %w", err)
					}
				}
			}
		}

		if _, err := tx.Insert(ctx, childInode.Row); err != nil {
			return nil, nil, fmt.Errorf("failed to log child inode creation: %w", err)
		}

		dirEntryMsg := &pb.DirEntry{
			ParentIno: proto.Uint64(parentInodeID),
			Name:      proto.String(name),
			Ino:       childInodeID,
			IsDir:     false,
			Mode:      mode,
		}
		if _, err := tx.Insert(ctx, dirEntryMsg); err != nil {
			return nil, nil, fmt.Errorf("failed to log directory entry: %w", err)
		}

		if _, err := tx.Update(ctx, parentInode.Row); err != nil {
			return nil, nil, fmt.Errorf("failed to log parent inode update: %w", err)
		}
		commitSeq, err := tx.Commit(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to commit create file transaction: %w", err)
		}
		v.applyTxChangesLocked(ctx, tx)

		waitFn := v.makeWaitFn(commitSeq, nil)

		attr := toEntryAttr(childInode.Row, name, "", v.rootInodeID)
		v.broadcaster.Broadcast(v.volumeID, &pb.WatchVolumeResponse{
			EventType:   pb.WatchEventType_EVENT_CREATED,
			Attr:        attr,
			Inode:       childInodeID,
			ParentInode: parentInodeID,
			Name:        name,
		})

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

func (v *Volume) Symlink(ctx context.Context, parentInodeID uint64, name string, target string, uid, gid uint32) (*pb.EntryAttr, error) {
	if len(name) > MaxNameLength {
		return nil, fmt.Errorf("symlink name %q exceeds maximum length: %w", name, syscall.ENAMETOOLONG)
	}

	attr, waitFn, err := func() (*pb.EntryAttr, func(context.Context) error, error) {
		v.mu.Lock()
		defer v.mu.Unlock()

		tx := v.beginTxLocked("Symlink")

		if err := v.checkBackpressureLocked(ctx); err != nil {
			return nil, nil, err
		}

		parentInodeID = v.normalizeInodeID(parentInodeID)
		if name == "" || name == "." || name == ".." {
			return nil, nil, fmt.Errorf("invalid symlink name %q: %w", name, syscall.EINVAL)
		}

		parentInode, err := v.getOrLoadInodeLocked(ctx, tx, parentInodeID)
		if err != nil {
			return nil, nil, err
		}
		if !parentInode.Row.GetIsDir() {
			return nil, nil, fmt.Errorf("parent inode %d is not a directory: %w", parentInodeID, syscall.ENOTDIR)
		}

		_, ok, err := v.getDirEntrySQLiteLocked(ctx, tx, parentInodeID, name)
		if err != nil {
			return nil, nil, err
		}
		if ok {
			return nil, nil, fmt.Errorf("entry %q already exists: %w", name, syscall.EEXIST)
		}

		mode := uint32(0777 | syscall.S_IFLNK)
		if (parentInode.Row.GetMode() & 02000) != 0 {
			gid = parentInode.Row.GetGid()
		}

		now := time.Now()
		childInodeID := v.allocInode(tx)
		parentInode.mutate(func(row *pb.Inode) {
			row.Mtime = timestamppb.New(now)
			row.Ctime = timestamppb.New(now)
		})

		childInode := &CachedInode{
			Row: &pb.Inode{
				Ino:           proto.Uint64(childInodeID),
				Mode:          mode,
				Size:          int64(len(target)),
				Mtime:         timestamppb.New(now),
				Atime:         timestamppb.New(now),
				Ctime:         timestamppb.New(now),
				Uid:           uid,
				Gid:           gid,
				IsDir:         false,
				Nlink:         1,
				SymlinkTarget: target,
			},
		}

		if _, err := tx.Insert(ctx, childInode.Row); err != nil {
			return nil, nil, fmt.Errorf("failed to log symlink inode creation: %w", err)
		}

		dirEntryMsg := &pb.DirEntry{
			ParentIno: proto.Uint64(parentInodeID),
			Name:      proto.String(name),
			Ino:       childInodeID,
			IsDir:     false,
			Mode:      mode,
		}
		if _, err := tx.Insert(ctx, dirEntryMsg); err != nil {
			return nil, nil, fmt.Errorf("failed to log directory entry: %w", err)
		}

		if _, err := tx.Update(ctx, parentInode.Row); err != nil {
			return nil, nil, fmt.Errorf("failed to log parent inode update: %w", err)
		}

		commitSeq, err := tx.Commit(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to commit symlink transaction: %w", err)
		}
		v.applyTxChangesLocked(ctx, tx)

		waitFn := v.makeWaitFn(commitSeq, nil)

		attr := toEntryAttr(childInode.Row, name, "", v.rootInodeID)
		v.broadcaster.Broadcast(v.volumeID, &pb.WatchVolumeResponse{
			EventType:   pb.WatchEventType_EVENT_CREATED,
			Attr:        attr,
			Inode:       childInodeID,
			ParentInode: parentInodeID,
			Name:        name,
		})
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

func (v *Volume) Readlink(ctx context.Context, inodeID uint64) (string, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	inodeID = v.normalizeInodeID(inodeID)
	node, err := v.getOrLoadInodeLocked(ctx, nil, inodeID)
	if err != nil {
		return "", err
	}
	if (node.Row.GetMode()&syscall.S_IFMT) != syscall.S_IFLNK && node.Row.GetSymlinkTarget() == "" {
		return "", syscall.EINVAL
	}
	return node.Row.GetSymlinkTarget(), nil
}

func (v *Volume) Link(ctx context.Context, oldInodeID uint64, newParentInodeID uint64, newName string) (*pb.EntryAttr, error) {
	if len(newName) > MaxNameLength {
		return nil, fmt.Errorf("link name %q exceeds maximum length: %w", newName, syscall.ENAMETOOLONG)
	}

	attr, waitFn, err := func() (*pb.EntryAttr, func(context.Context) error, error) {
		v.mu.Lock()
		defer v.mu.Unlock()

		tx := v.beginTxLocked("Link")

		if err := v.checkBackpressureLocked(ctx); err != nil {
			return nil, nil, err
		}

		oldInodeID = v.normalizeInodeID(oldInodeID)
		newParentInodeID = v.normalizeInodeID(newParentInodeID)

		if newName == "" || newName == "." || newName == ".." {
			return nil, nil, fmt.Errorf("invalid link name %q: %w", newName, syscall.EINVAL)
		}

		oldNode, err := v.getOrLoadInodeLocked(ctx, tx, oldInodeID)
		if err != nil {
			return nil, nil, err
		}
		if oldNode.Row.GetIsDir() || (oldNode.Row.GetMode()&syscall.S_IFMT) == syscall.S_IFDIR {
			return nil, nil, syscall.EPERM // POSIX: directories cannot be hard-linked
		}

		newParentInode, err := v.getOrLoadInodeLocked(ctx, tx, newParentInodeID)
		if err != nil {
			return nil, nil, err
		}
		if !newParentInode.Row.GetIsDir() {
			return nil, nil, fmt.Errorf("parent inode %d is not a directory: %w", newParentInodeID, syscall.ENOTDIR)
		}

		_, ok, err := v.getDirEntrySQLiteLocked(ctx, tx, newParentInodeID, newName)
		if err != nil {
			return nil, nil, err
		}
		if ok {
			return nil, nil, fmt.Errorf("entry %q already exists: %w", newName, syscall.EEXIST)
		}

		now := time.Now()
		oldNode.mutate(func(row *pb.Inode) {
			if row.GetNlink() == 0 {
				row.Nlink = 1
			}
			row.Nlink++
			row.Ctime = timestamppb.New(now)
		})

		newParentInode.mutate(func(row *pb.Inode) {
			row.Mtime = timestamppb.New(now)
			row.Ctime = timestamppb.New(now)
		})

		if _, err := tx.Update(ctx, oldNode.Row); err != nil {
			return nil, nil, fmt.Errorf("failed to log linked inode update: %w", err)
		}

		dirEntryMsg := &pb.DirEntry{
			ParentIno: proto.Uint64(newParentInodeID),
			Name:      proto.String(newName),
			Ino:       oldNode.Row.GetIno(),
			IsDir:     false,
			Mode:      oldNode.Row.GetMode(),
		}
		if _, err := tx.Insert(ctx, dirEntryMsg); err != nil {
			return nil, nil, fmt.Errorf("failed to log directory entry: %w", err)
		}

		if _, err := tx.Update(ctx, newParentInode.Row); err != nil {
			return nil, nil, fmt.Errorf("failed to log parent inode update: %w", err)
		}

		commitSeq, err := tx.Commit(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to commit link transaction: %w", err)
		}
		v.applyTxChangesLocked(ctx, tx)

		waitFn := v.makeWaitFn(commitSeq, nil)

		attr := toEntryAttr(oldNode.Row, newName, "", v.rootInodeID)
		v.broadcaster.Broadcast(v.volumeID, &pb.WatchVolumeResponse{
			EventType:   pb.WatchEventType_EVENT_CREATED,
			Attr:        attr,
			Inode:       oldNode.Row.GetIno(),
			ParentInode: newParentInodeID,
			Name:        newName,
		})
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

func (v *Volume) ReadFile(ctx context.Context, inodeID uint64, offset, length int64) ([]byte, int64, string, error) {
	if inodeID == 0 {
		v.mu.RLock()
		inodeID = v.rootInodeID
		v.mu.RUnlock()
	}
	_ = v.waitForInodeUploads(ctx, inodeID)

	v.mu.RLock()
	node, err := v.getOrLoadInodeLocked(ctx, nil, inodeID)
	v.mu.RUnlock()
	if err != nil {
		return nil, 0, "", err
	}

	if node.Row.GetIsDir() {
		return nil, 0, "", fmt.Errorf("cannot read directory as file: %w", syscall.EISDIR)
	}

	total := node.Row.GetSize()
	if node.RedirectURL != "" && length > v.maxInlineLen {
		return nil, total, node.RedirectURL, nil
	}

	if offset >= total || total == 0 {
		return []byte{}, total, "", nil
	}

	end := offset + length
	if length <= 0 || end > total {
		end = total
	}
	readLen := end - offset

	v.mu.Lock()
	defer v.mu.Unlock()

	if len(node.InlineData) > 0 {
		res := make([]byte, readLen)
		if offset < int64(len(node.InlineData)) {
			copyEnd := end
			if copyEnd > int64(len(node.InlineData)) {
				copyEnd = int64(len(node.InlineData))
			}
			copy(res, node.InlineData[offset:copyEnd])
		}
		return res, total, "", nil
	}

	if node.Row.GetManifestSha256() != "" || node.Row.GetChunkSize() > 0 || len(node.Chunks) > 0 || len(node.StagedChunks) > 0 {
		if err := v.ensureInodeChunksLoadedLocked(ctx, nil, node); err != nil {
			return nil, 0, "", fmt.Errorf("read inode %d chunks load: %w", node.Row.GetIno(), err)
		}
		cs := int64(node.Row.GetChunkSize())
		if cs == 0 {
			cs = int64(v.chunkSize)
			if cs == 0 {
				cs = 64 * 1024
			}
		}
		startChunk := int(offset / cs)
		endChunk := int((end - 1) / cs)

		var res bytes.Buffer
		for i := startChunk; i <= endChunk; i++ {
			chunkData, err := v.readChunkLocked(ctx, nil, node, i)
			if err != nil {
				return nil, 0, "", err
			}
			chunkLen := cs
			if int64(i+1)*cs > total {
				chunkLen = total - int64(i)*cs
			}
			chunkStart := int64(i) * cs
			rStart := offset - chunkStart
			if rStart < 0 {
				rStart = 0
			}
			rEnd := end - chunkStart
			if rEnd > chunkLen {
				rEnd = chunkLen
			}
			if rEnd > rStart {
				for b := rStart; b < rEnd; b++ {
					if b < int64(len(chunkData)) {
						res.WriteByte(chunkData[b])
					} else {
						res.WriteByte(0)
					}
				}
			}
		}
		return res.Bytes(), total, "", nil
	}

	// Lazy load data from blob store if not currently in memory
	if node.Data == nil && node.Row.GetSize() > 0 {
		if node.Row.GetSha256() != "" && v.blobStore != nil {
			stream, err := v.blobStore.GetBlob(ctx, node.Row.GetSha256())
			if err != nil {
				return nil, 0, "", fmt.Errorf("read inode %d blob %s: %w", node.Row.GetIno(), node.Row.GetSha256(), err)
			}
			node.Data = stream
		}
	}

	if node.Data == nil {
		return make([]byte, readLen), total, "", nil
	}

	if _, err := node.Data.Seek(offset, io.SeekStart); err != nil {
		return nil, 0, "", fmt.Errorf("seek inode %d data at %d: %w", node.Row.GetIno(), offset, err)
	}
	res := make([]byte, readLen)
	n, err := io.ReadFull(node.Data, res)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return nil, 0, "", fmt.Errorf("failed to read node data: %w", err)
	}
	return res[:n], total, "", nil
}

func (v *Volume) WriteFile(ctx context.Context, inodeID uint64, offset int64, data []byte, writeMode pb.WriteMode) (int64, int64, time.Time, error) {
	v.mu.RLock()
	inodeID = v.normalizeInodeID(inodeID)
	v.mu.RUnlock()

	if err := v.waitForInodeUploads(ctx, inodeID); err != nil {
		return 0, 0, time.Time{}, err
	}

	nWritten, newSize, modTime, waitFn, err := func() (int64, int64, time.Time, func(context.Context) error, error) {
		v.mu.Lock()
		defer v.mu.Unlock()

		tx := v.beginTxLocked("WriteFile")

		if err := v.checkBackpressureLocked(ctx); err != nil {
			return 0, 0, time.Time{}, nil, err
		}

		if inodeID == 0 {
			inodeID = v.rootInodeID
		}

		node, err := v.getOrLoadInodeLocked(ctx, tx, inodeID)
		if err != nil {
			return 0, 0, time.Time{}, nil, err
		}

		if node.Row.GetIsDir() {
			return 0, 0, time.Time{}, nil, fmt.Errorf("cannot write to directory: %w", syscall.EISDIR)
		}

		effectiveChunkSize := v.chunkSize
		if effectiveChunkSize == 0 {
			effectiveChunkSize = 64 * 1024
		}
		if node.Row.GetChunkSize() > 0 {
			effectiveChunkSize = node.Row.GetChunkSize()
		}

		neededLen := offset + int64(len(data))
		calculatedSize := neededLen
		if calculatedSize < node.Row.GetSize() {
			calculatedSize = node.Row.GetSize()
		}

		now := time.Now()

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

		// Tiny file check
		if calculatedSize <= v.maxInlineLen && len(node.Chunks) == 0 {
			var newInline []byte
			if node.InlineData == nil {
				if offset == 0 {
					newInline = make([]byte, len(data))
					copy(newInline, data)
				} else {
					chunk0, err := v.readChunkLocked(ctx, tx, node, 0)
					if err != nil {
						return 0, 0, time.Time{}, nil, err
					}
					newInline = make([]byte, neededLen)
					copy(newInline, chunk0)
					copy(newInline[offset:], data)
				}
			} else {
				newInline = make([]byte, neededLen)
				copy(newInline, node.InlineData)
				copy(newInline[offset:], data)
			}
			node.IsDirty = true
			node.InlineData = newInline

			node.mutate(func(row *pb.Inode) {
				row.ChunkSize = effectiveChunkSize
				row.Mtime = timestamppb.New(now)
				row.Ctime = timestamppb.New(now)
				row.Size = int64(len(node.InlineData))
				if offset == 0 && int64(len(data)) == row.GetSize() {
					h := sha256.Sum256(data)
					row.ContentSha256 = fmt.Sprintf("%x", h)
				} else {
					row.ContentSha256 = ""
				}
			})

			if _, err := tx.Insert(ctx, &pb.FileChunk{
				Ino:        proto.Uint64(node.Row.GetIno()),
				Index:      proto.Uint32(0),
				InlineData: append([]byte(nil), node.InlineData...),
			}); err != nil {
				return 0, 0, time.Time{}, nil, fmt.Errorf("failed to log inline FileChunk: %w", err)
			}
			if _, err := tx.Update(ctx, node.Row); err != nil {
				return 0, 0, time.Time{}, nil, fmt.Errorf("failed to log node inode update: %w", err)
			}

			commitSeq, err := tx.Commit(ctx)
			if err != nil {
				return 0, 0, time.Time{}, nil, fmt.Errorf("failed to commit tiny write transaction: %w", err)
			}
			v.applyTxChangesLocked(ctx, tx)
			waitFn := v.makeWaitFn(commitSeq, reqLevel)

			attr := toEntryAttr(node.Row, "", node.RedirectURL, v.rootInodeID)
			v.broadcaster.Broadcast(v.volumeID, &pb.WatchVolumeResponse{
				EventType: pb.WatchEventType_EVENT_MODIFIED,
				Attr:      attr,
				Inode:     node.Row.GetIno(),
			})
			return int64(len(data)), node.Row.GetSize(), now, waitFn, nil
		}

		// Chunked file write
		if node.Chunks == nil {
			node.Chunks = make(map[uint32]string)
		}
		if node.StagedChunks == nil {
			node.StagedChunks = make(map[int][]byte)
		}
		if err := v.ensureInodeChunksLoadedLocked(ctx, tx, node); err != nil {
			return 0, 0, time.Time{}, nil, fmt.Errorf("ensure inode chunks loaded: %w", err)
		}

		var inlineBytes []byte
		var inlineSha string
		if len(node.InlineData) > 0 {
			inlineBytes = node.InlineData
			inlineSha = fmt.Sprintf("%x", sha256.Sum256(inlineBytes))
			node.Chunks[0] = inlineSha
			node.StagedChunks[0] = inlineBytes
			node.InlineData = nil
		}

		cs := int64(effectiveChunkSize)
		startChunk := int(offset / cs)
		endChunk := int((offset + int64(len(data)) - 1) / cs)

		touchedIndices := make([]int, 0, endChunk-startChunk+2)
		chunkBlobs := make(map[int][]byte)
		chunkShas := make(map[int]string)

		if inlineBytes != nil && startChunk > 0 {
			touchedIndices = append(touchedIndices, 0)
			chunkBlobs[0] = inlineBytes
			chunkShas[0] = inlineSha
		}

		for i := startChunk; i <= endChunk; i++ {
			chunkStart := int64(i) * cs
			chunkEnd := chunkStart + cs
			wStart := offset - chunkStart
			if wStart < 0 {
				wStart = 0
			}
			wEnd := offset + int64(len(data)) - chunkStart
			if wEnd > cs {
				wEnd = cs
			}
			dataStart := chunkStart - offset
			if dataStart < 0 {
				dataStart = 0
			}
			dataEnd := chunkEnd - offset
			if dataEnd > int64(len(data)) {
				dataEnd = int64(len(data))
			}

			chunkData, err := v.readChunkLocked(ctx, tx, node, i)
			if err != nil {
				return 0, 0, time.Time{}, nil, err
			}
			expectedChunkLen := cs
			if int64(i+1)*cs > calculatedSize {
				expectedChunkLen = calculatedSize - int64(i)*cs
			}
			if int64(len(chunkData)) < expectedChunkLen {
				newBuf := make([]byte, expectedChunkLen)
				copy(newBuf, chunkData)
				chunkData = newBuf
			}
			copy(chunkData[wStart:wEnd], data[dataStart:dataEnd])
			chunkSha := fmt.Sprintf("%x", sha256.Sum256(chunkData))

			touchedIndices = append(touchedIndices, i)
			cCopy := make([]byte, len(chunkData))
			copy(cCopy, chunkData)
			chunkBlobs[i] = cCopy
			chunkShas[i] = chunkSha
		}

		for _, i := range touchedIndices {
			node.Chunks[uint32(i)] = chunkShas[i]
			node.StagedChunks[i] = chunkBlobs[i]
		}
		node.IsDirty = true

		numChunks := int((calculatedSize + cs - 1) / cs)
		manifestChunks := make([][32]byte, numChunks)
		for i := 0; i < numChunks; i++ {
			if cSha, ok := node.Chunks[uint32(i)]; ok && cSha != "" {
				raw, _ := hex.DecodeString(cSha)
				if len(raw) == 32 {
					copy(manifestChunks[i][:], raw)
				}
			}
		}
		var manifestHex string
		manifest := &blob.Manifest{
			ChunkSize:   uint32(cs),
			TotalLength: uint64(calculatedSize),
			Chunks:      manifestChunks,
		}
		manifestBlob, mErr := blob.EncodeManifest(manifest)
		if mErr == nil {
			manifestHex = manifestBlob.SHA256Hex()
		}

		node.mutate(func(row *pb.Inode) {
			row.ChunkSize = uint32(effectiveChunkSize)
			row.Mtime = timestamppb.New(now)
			row.Ctime = timestamppb.New(now)
			row.Size = calculatedSize
			row.ContentSha256 = ""
			if manifestHex != "" {
				row.ManifestSha256 = manifestHex
			}
		})

		for _, idx := range touchedIndices {
			cSize := int64(node.Row.GetChunkSize())
			if cSize == 0 {
				cSize = int64(effectiveChunkSize)
			}
			if int64(idx)*cSize < node.Row.GetSize() {
				if _, err := tx.Insert(ctx, &pb.FileChunk{
					Ino:    proto.Uint64(node.Row.GetIno()),
					Index:  proto.Uint32(uint32(idx)),
					Sha256: chunkShas[idx],
				}); err != nil {
					return 0, 0, time.Time{}, nil, fmt.Errorf("failed to log FileChunk: %w", err)
				}
			}
		}

		if _, err := tx.Update(ctx, node.Row); err != nil {
			return 0, 0, time.Time{}, nil, fmt.Errorf("failed to log inode update: %w", err)
		}

		commitSeq, err := tx.Commit(ctx)
		if err != nil {
			return 0, 0, time.Time{}, nil, fmt.Errorf("failed to commit chunked write transaction: %w", err)
		}
		v.applyTxChangesLocked(ctx, tx)

		waitFn := v.makeWaitFn(commitSeq, reqLevel)

		done := v.registerPendingUpload(node.Row.GetIno())
		go func() {
			var uploadErr error
			defer func() { done(uploadErr) }()

			if v.blobStore != nil && len(chunkBlobs) > 0 {
				blobsMap := make(map[string]blob.ByteStream, len(chunkBlobs))
				for idx, cBytes := range chunkBlobs {
					blobsMap[chunkShas[idx]] = blob.NewByteStreamFromBytes(cBytes)
				}
				if err := v.blobStore.PutBlobs(context.Background(), blobsMap); err != nil {
					uploadErr = err
					return
				}
			}
		}()

		attr := toEntryAttr(node.Row, "", node.RedirectURL, v.rootInodeID)
		v.broadcaster.Broadcast(v.volumeID, &pb.WatchVolumeResponse{
			EventType: pb.WatchEventType_EVENT_MODIFIED,
			Attr:      attr,
			Inode:     node.Row.GetIno(),
		})

		return int64(len(data)), node.Row.GetSize(), now, waitFn, nil
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

func (v *Volume) TruncateFile(ctx context.Context, inodeID uint64, size int64) (*pb.EntryAttr, error) {
	v.mu.RLock()
	inodeID = v.normalizeInodeID(inodeID)
	v.mu.RUnlock()

	if err := v.waitForInodeUploads(ctx, inodeID); err != nil {
		return nil, err
	}

	attr, waitFn, err := func() (*pb.EntryAttr, func(context.Context) error, error) {
		v.mu.Lock()
		defer v.mu.Unlock()

		tx := v.beginTxLocked("TruncateFile")

		if err := v.checkBackpressureLocked(ctx); err != nil {
			return nil, nil, err
		}

		inodeID = v.normalizeInodeID(inodeID)

		node, err := v.getOrLoadInodeLocked(ctx, tx, inodeID)
		if err != nil {
			return nil, nil, err
		}

		if node.Row.GetIsDir() {
			return nil, nil, fmt.Errorf("cannot truncate directory: %w", syscall.EISDIR)
		}

		if size < 0 {
			return nil, nil, fmt.Errorf("invalid size %d: %w", size, syscall.EINVAL)
		}

		now := time.Now()
		if err := v.ensureInodeChunksLoadedLocked(ctx, tx, node); err != nil {
			return nil, nil, fmt.Errorf("truncate file: ensure inode chunks loaded: %w", err)
		}
		oldChunks := node.Chunks

		if size == 0 {
			node.IsDirty = true
			node.mutate(func(row *pb.Inode) {
				row.Mtime = timestamppb.New(now)
				row.Ctime = timestamppb.New(now)
				row.Size = 0
				row.ManifestSha256 = ""
				row.ContentSha256 = fmt.Sprintf("%x", sha256.Sum256([]byte{}))
				row.Sha256 = row.ContentSha256
			})
			node.Chunks = nil
			node.StagedChunks = nil
			node.DirtyChunks = nil
			node.InlineData = nil
			if node.Data != nil {
				_ = node.Data.Close()
				node.Data = nil
			}

			for i := range oldChunks {
				if _, err := tx.Delete(ctx, &pb.FileChunk{Ino: proto.Uint64(node.Row.GetIno()), Index: proto.Uint32(i)}); err != nil {
					return nil, nil, fmt.Errorf("failed to delete FileChunk row: %w", err)
				}
			}
			if len(oldChunks) == 0 {
				if _, err := tx.Delete(ctx, &pb.FileChunk{Ino: proto.Uint64(node.Row.GetIno()), Index: proto.Uint32(0)}); err != nil {
					// delete chunk 0 inline if existed
				}
			}
		} else if size <= v.maxInlineLen && len(oldChunks) <= 1 {
			chunk0, err := v.readChunkLocked(ctx, tx, node, 0)
			if err != nil {
				return nil, nil, err
			}
			node.IsDirty = true
			newBuf := make([]byte, size)
			copy(newBuf, chunk0)
			node.InlineData = newBuf
			node.Chunks = nil
			node.StagedChunks = nil
			node.DirtyChunks = nil

			node.mutate(func(row *pb.Inode) {
				row.Mtime = timestamppb.New(now)
				row.Ctime = timestamppb.New(now)
				row.Size = size
				row.ContentSha256 = ""
			})

			for i := range oldChunks {
				if i != 0 {
					if _, err := tx.Delete(ctx, &pb.FileChunk{Ino: proto.Uint64(node.Row.GetIno()), Index: proto.Uint32(i)}); err != nil {
						return nil, nil, fmt.Errorf("failed to delete FileChunk row: %w", err)
					}
				}
			}
			if _, err := tx.Insert(ctx, &pb.FileChunk{Ino: proto.Uint64(node.Row.GetIno()), Index: proto.Uint32(0), InlineData: append([]byte(nil), node.InlineData...)}); err != nil {
				return nil, nil, fmt.Errorf("failed to update inline FileChunk: %w", err)
			}
		} else {
			cs := int64(node.Row.GetChunkSize())
			if cs == 0 {
				cs = int64(v.chunkSize)
				if cs == 0 {
					cs = 64 * 1024
				}
			}

			if node.Chunks == nil {
				node.Chunks = make(map[uint32]string)
			}

			newNumChunks := int((size + cs - 1) / cs)

			// Reads before writes: read the boundary chunk if needed before buffering any deletions into tx.
			var lastChunkData []byte
			var hasLastChunkData bool
			if newNumChunks > 0 && size < node.Row.GetSize() {
				lastIdx := newNumChunks - 1
				if chunkData, ok := node.StagedChunks[lastIdx]; ok {
					lastChunkData = chunkData
					hasLastChunkData = true
				} else if _, exists := node.Chunks[uint32(lastIdx)]; exists {
					chunkData, err := v.readChunkLocked(ctx, tx, node, lastIdx)
					if err != nil {
						return nil, nil, err
					}
					lastChunkData = chunkData
					hasLastChunkData = true
				}
			}

			// Remove staged and logged chunks beyond newNumChunks
			for k := range node.StagedChunks {
				if k >= newNumChunks {
					delete(node.StagedChunks, k)
				}
			}
			for i := range node.Chunks {
				if int(i) >= newNumChunks {
					delete(node.Chunks, i)
					if _, err := tx.Delete(ctx, &pb.FileChunk{Ino: proto.Uint64(node.Row.GetIno()), Index: proto.Uint32(i)}); err != nil {
						return nil, nil, fmt.Errorf("failed to delete FileChunk row: %w", err)
					}
				}
			}

			if hasLastChunkData {
				lastIdx := newNumChunks - 1
				lastChunkLen := size - int64(lastIdx)*cs
				if int64(len(lastChunkData)) > lastChunkLen {
					truncatedData := lastChunkData[:lastChunkLen]
					chunkSha := fmt.Sprintf("%x", sha256.Sum256(truncatedData))
					node.Chunks[uint32(lastIdx)] = chunkSha
					if node.StagedChunks == nil {
						node.StagedChunks = make(map[int][]byte)
					}
					node.StagedChunks[lastIdx] = truncatedData
					if v.blobStore != nil {
						_ = v.blobStore.PutBlobs(ctx, map[string]blob.ByteStream{
							chunkSha: blob.NewByteStreamFromBytes(truncatedData),
						})
					}
					if _, err := tx.Insert(ctx, &pb.FileChunk{
						Ino:    proto.Uint64(node.Row.GetIno()),
						Index:  proto.Uint32(uint32(lastIdx)),
						Sha256: chunkSha,
					}); err != nil {
						return nil, nil, fmt.Errorf("failed to log truncated chunk: %w", err)
					}
				}
			}

			node.mutate(func(row *pb.Inode) {
				row.Mtime = timestamppb.New(now)
				row.Ctime = timestamppb.New(now)
				row.ChunkSize = uint32(cs)
				row.Size = size
				row.ContentSha256 = ""
			})
		}

		if _, err := tx.Update(ctx, node.Row); err != nil {
			return nil, nil, fmt.Errorf("failed to log node inode update: %w", err)
		}

		commitSeq, err := tx.Commit(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to commit truncate file transaction: %w", err)
		}
		v.applyTxChangesLocked(ctx, tx)

		waitFn := v.makeWaitFn(commitSeq, nil)

		attr := toEntryAttr(node.Row, "", node.RedirectURL, v.rootInodeID)
		v.broadcaster.Broadcast(v.volumeID, &pb.WatchVolumeResponse{
			EventType: pb.WatchEventType_EVENT_MODIFIED,
			Attr:      attr,
			Inode:     node.Row.GetIno(),
		})
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

func (v *Volume) Unlink(ctx context.Context, parentInodeID uint64, name string) error {
	if len(name) > MaxNameLength {
		return fmt.Errorf("file name %q exceeds maximum length: %w", name, syscall.ENAMETOOLONG)
	}

	v.mu.Lock()
	parentInodeID = v.normalizeInodeID(parentInodeID)

	var childInodeID uint64
	de, ok, err := v.getDirEntrySQLiteLocked(ctx, nil, parentInodeID, name)
	if err != nil {
		v.mu.Unlock()
		return err
	}
	if !ok {
		v.mu.Unlock()
		return fmt.Errorf("file %s not found under inode %d: %w", name, parentInodeID, syscall.ENOENT)
	}
	childInode, err := v.getOrLoadInodeLocked(ctx, nil, de.GetIno())
	if err != nil {
		v.mu.Unlock()
		return err
	}
	if childInode.Row.GetIsDir() {
		v.mu.Unlock()
		return fmt.Errorf("cannot unlink directory %s: %w", name, syscall.EISDIR)
	}
	childInodeID = childInode.Row.GetIno()
	v.mu.Unlock()

	if err := v.waitForInodeUploads(ctx, childInodeID); err != nil {
		return err
	}

	waitFn, err := func() (func(context.Context) error, error) {
		v.mu.Lock()
		defer v.mu.Unlock()

		tx := v.beginTxLocked("Unlink")

		if err := v.checkBackpressureLocked(ctx); err != nil {
			return nil, err
		}

		_, ok, err := v.getDirEntrySQLiteLocked(ctx, tx, parentInodeID, name)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("file %s not found under inode %d: %w", name, parentInodeID, syscall.ENOENT)
		}

		childInode, err := v.getOrLoadInodeLocked(ctx, tx, childInodeID)
		if err != nil {
			return nil, err
		}

		if childInode.Data != nil {
			if err := childInode.Data.Close(); err != nil {
				klog.Warningf("Failed to close child inode %d data during unlink: %v", childInodeID, err)
			}
			childInode.Data = nil
		}

		now := time.Now()
		childInode.mutate(func(row *pb.Inode) {
			if row.GetNlink() > 1 {
				row.Nlink--
			} else {
				row.Nlink = 0
			}
			row.Ctime = timestamppb.New(now)
		})

		parentInode, _ := v.getOrLoadInodeLocked(ctx, tx, parentInodeID)
		if parentInode != nil {
			parentInode.mutate(func(row *pb.Inode) {
				row.Mtime = timestamppb.New(now)
				row.Ctime = timestamppb.New(now)
			})
		}

		// Reads before writes: scan chunk rows before buffering any deletions into tx.
		var chunkMsgs []proto.Message
		if childInode.Row.GetNlink() == 0 && !v.hasOpenHandlesLocked(childInodeID) {
			chunkPrefix, err := sds.EncodeKeyPrefix(&pb.FileChunk{Ino: proto.Uint64(childInodeID)}, 1)
			if err != nil {
				return nil, fmt.Errorf("failed to encode chunk key prefix for inode %d: %w", childInodeID, err)
			}
			chunkMsgs, err = v.scanSQLiteRowsLocked(ctx, tx, "objectfs.v1alpha1.FileChunk", chunkPrefix)
			if err != nil {
				return nil, fmt.Errorf("failed to scan chunk rows for inode %d: %w", childInodeID, err)
			}
		}

		var commitSeq uint64
		if _, err := tx.Delete(ctx, &pb.DirEntry{ParentIno: proto.Uint64(parentInodeID), Name: proto.String(name)}); err != nil {
			return nil, fmt.Errorf("failed to log dir entry deletion: %w", err)
		}

		if childInode.Row.GetNlink() > 0 || v.hasOpenHandlesLocked(childInodeID) {
			if _, err := tx.Update(ctx, childInode.Row); err != nil {
				return nil, fmt.Errorf("failed to log child inode update: %w", err)
			}
		} else {
			if _, err := tx.Delete(ctx, &pb.Inode{Ino: proto.Uint64(childInodeID)}); err != nil {
				return nil, fmt.Errorf("failed to log child inode deletion: %w", err)
			}
			for _, cMsg := range chunkMsgs {
				c := cMsg.(*pb.FileChunk)
				if _, err := tx.Delete(ctx, &pb.FileChunk{Ino: proto.Uint64(childInodeID), Index: proto.Uint32(c.GetIndex())}); err != nil {
					return nil, fmt.Errorf("failed to log chunk deletion for inode %d: %w", childInodeID, err)
				}
			}
		}

		if parentInode != nil {
			if _, err := tx.Update(ctx, parentInode.Row); err != nil {
				return nil, fmt.Errorf("failed to log parent inode update: %w", err)
			}
		}
		commitSeq, err = tx.Commit(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to commit transaction: %w", err)
		}
		v.applyTxChangesLocked(ctx, tx)

		waitFn := v.makeWaitFn(commitSeq, nil)

		v.broadcaster.Broadcast(v.volumeID, &pb.WatchVolumeResponse{
			EventType:   pb.WatchEventType_EVENT_DELETED,
			Inode:       childInode.Row.GetIno(),
			ParentInode: parentInodeID,
			Name:        name,
		})

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

func (v *Volume) Rmdir(ctx context.Context, parentInodeID uint64, name string) error {
	if len(name) > MaxNameLength {
		return fmt.Errorf("directory name %q exceeds maximum length: %w", name, syscall.ENAMETOOLONG)
	}

	waitFn, err := func() (func(context.Context) error, error) {
		v.mu.Lock()
		defer v.mu.Unlock()

		tx := v.beginTxLocked("Rmdir")

		if err := v.checkBackpressureLocked(ctx); err != nil {
			return nil, err
		}

		parentInodeID = v.normalizeInodeID(parentInodeID)

		var childInodeID uint64
		de, ok, err := v.getDirEntrySQLiteLocked(ctx, tx, parentInodeID, name)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("directory %s not found under inode %d: %w", name, parentInodeID, syscall.ENOENT)
		}
		childInodeID = de.GetIno()
		childInode, err := v.getOrLoadInodeLocked(ctx, tx, childInodeID)
		if err != nil {
			return nil, err
		}
		if !childInode.Row.GetIsDir() {
			return nil, fmt.Errorf("cannot rmdir non-directory %s: %w", name, syscall.ENOTDIR)
		}

		// Emptiness check: scan child directory with limit 1
		prefixBytes, pErr := sds.EncodeKeyPrefix(&pb.DirEntry{ParentIno: proto.Uint64(childInodeID)}, 1)
		if pErr != nil {
			return nil, pErr
		}
		subEntries, err := v.scanLimitSQLiteRowsLocked(ctx, tx, "objectfs.v1alpha1.DirEntry", prefixBytes, 1)
		if err != nil {
			return nil, err
		}
		if len(subEntries) > 0 {
			return nil, fmt.Errorf("directory %s not empty: %w", name, syscall.ENOTEMPTY)
		}

		delete(v.dirParents, childInodeID)

		now := time.Now()
		parentInode, _ := v.getOrLoadInodeLocked(ctx, tx, parentInodeID)
		if parentInode != nil {
			parentInode.mutate(func(row *pb.Inode) {
				if row.GetNlink() > 2 {
					row.Nlink--
				}
				row.Mtime = timestamppb.New(now)
				row.Ctime = timestamppb.New(now)
			})
		}

		var commitSeq uint64
		if _, err = tx.Delete(ctx, &pb.DirEntry{ParentIno: proto.Uint64(parentInodeID), Name: proto.String(name)}); err != nil {
			return nil, fmt.Errorf("failed to log dir entry deletion: %w", err)
		}
		if _, err = tx.Delete(ctx, &pb.Inode{Ino: proto.Uint64(childInodeID)}); err != nil {
			return nil, fmt.Errorf("failed to log inode deletion: %w", err)
		}
		if parentInode != nil {
			if _, err = tx.Update(ctx, parentInode.Row); err != nil {
				return nil, fmt.Errorf("failed to log parent inode update: %w", err)
			}
		}
		commitSeq, err = tx.Commit(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to commit transaction: %w", err)
		}
		v.applyTxChangesLocked(ctx, tx)

		waitFn := v.makeWaitFn(commitSeq, nil)

		v.broadcaster.Broadcast(v.volumeID, &pb.WatchVolumeResponse{
			EventType:   pb.WatchEventType_EVENT_DELETED,
			Inode:       childInodeID,
			ParentInode: parentInodeID,
			Name:        name,
		})

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

func (v *Volume) Rename(ctx context.Context, oldParentInodeID uint64, oldName string, newParentInodeID uint64, newName string) (*pb.EntryAttr, error) {
	if len(oldName) > MaxNameLength || len(newName) > MaxNameLength {
		return nil, fmt.Errorf("name exceeds maximum length: %w", syscall.ENAMETOOLONG)
	}

	attr, waitFn, err := func() (*pb.EntryAttr, func(context.Context) error, error) {
		v.mu.Lock()
		defer v.mu.Unlock()

		tx := v.beginTxLocked("Rename")

		if err := v.checkBackpressureLocked(ctx); err != nil {
			return nil, nil, err
		}

		oldParentInodeID = v.normalizeInodeID(oldParentInodeID)
		newParentInodeID = v.normalizeInodeID(newParentInodeID)

		if oldName == "" || oldName == "." || oldName == ".." || newName == "" || newName == "." || newName == ".." {
			return nil, nil, fmt.Errorf("invalid name for rename: %w", syscall.EINVAL)
		}

		var targetExists bool
		var targetInodeID uint64

		oldDe, ok, err := v.getDirEntrySQLiteLocked(ctx, tx, oldParentInodeID, oldName)
		if err != nil {
			return nil, nil, err
		}
		if !ok {
			return nil, nil, fmt.Errorf("source %s not found in parent %d: %w", oldName, oldParentInodeID, syscall.ENOENT)
		}

		newParentInode, err := v.getOrLoadInodeLocked(ctx, tx, newParentInodeID)
		if err != nil {
			return nil, nil, err
		}
		if !newParentInode.Row.GetIsDir() {
			return nil, nil, fmt.Errorf("target parent %d is not a directory: %w", newParentInodeID, syscall.ENOTDIR)
		}

		targetDe, ok, err := v.getDirEntrySQLiteLocked(ctx, tx, newParentInodeID, newName)
		if err != nil {
			return nil, nil, err
		}
		if ok {
			targetExists = true
			targetInodeID = targetDe.GetIno()
		}

		childInode, err := v.getOrLoadInodeLocked(ctx, tx, oldDe.GetIno())
		if err != nil {
			return nil, nil, err
		}

		oldParentInode, _ := v.getOrLoadInodeLocked(ctx, tx, oldParentInodeID)

		var targetInode *CachedInode
		var targetChunkMsgs []proto.Message
		if targetExists {
			targetInode, err = v.getOrLoadInodeLocked(ctx, tx, targetInodeID)
			if err != nil {
				return nil, nil, err
			}
			if !targetInode.Row.GetIsDir() && targetInode.Row.GetNlink() <= 1 && !v.hasOpenHandlesLocked(targetInodeID) {
				chunkPrefix, pErr := sds.EncodeKeyPrefix(&pb.FileChunk{Ino: proto.Uint64(targetInodeID)}, 1)
				if pErr != nil {
					return nil, nil, pErr
				}
				var sErr error
				targetChunkMsgs, sErr = v.scanSQLiteRowsLocked(ctx, tx, "objectfs.v1alpha1.FileChunk", chunkPrefix)
				if sErr != nil {
					return nil, nil, sErr
				}
			}
		}

		if childInode.Row.GetIsDir() {
			if targetExists {
				if !targetInode.Row.GetIsDir() {
					return nil, nil, syscall.ENOTDIR
				}
				// Emptiness check: scan target directory with limit 1
				prefixBytes, pErr := sds.EncodeKeyPrefix(&pb.DirEntry{ParentIno: proto.Uint64(targetInodeID)}, 1)
				if pErr != nil {
					return nil, nil, pErr
				}
				subEntries, sErr := v.scanLimitSQLiteRowsLocked(ctx, tx, "objectfs.v1alpha1.DirEntry", prefixBytes, 1)
				if sErr != nil {
					return nil, nil, sErr
				}
				if len(subEntries) > 0 {
					return nil, nil, syscall.ENOTEMPTY
				}
			}

			if newParentInodeID != oldParentInodeID {
				currCheck := newParentInodeID
				for currCheck != 0 && currCheck != v.rootInodeID {
					if currCheck == childInode.Row.GetIno() {
						return nil, nil, fmt.Errorf("cannot move directory into its own subdirectory: %w", syscall.EINVAL)
					}
					pIno, ok := v.dirParents[currCheck]
					if !ok || pIno == currCheck {
						break
					}
					currCheck = pIno
				}
				if v.dirParents == nil {
					v.dirParents = make(map[uint64]uint64)
				}
				v.dirParents[childInode.Row.GetIno()] = newParentInodeID

				if oldParentInode != nil {
					oldParentInode.mutate(func(row *pb.Inode) {
						if row.GetNlink() > 2 {
							row.Nlink--
						}
					})
				}
				if !targetExists && newParentInode != nil {
					newParentInode.mutate(func(row *pb.Inode) {
						if row.GetNlink() < 2 {
							row.Nlink = 2
						}
						row.Nlink++
					})
				}
			} else if targetExists {
				// Replaced empty subdirectory within same parent: parent nlink decreases by 1
				if newParentInode != nil {
					newParentInode.mutate(func(row *pb.Inode) {
						if row.GetNlink() > 2 {
							row.Nlink--
						}
					})
				}
			}
			childInode.mutate(func(row *pb.Inode) {
				row.ParentIno = proto.Uint64(newParentInodeID)
			})
		} else {
			if targetExists {
				if targetInode.Row.GetIsDir() {
					return nil, nil, syscall.EISDIR
				}
			}
		}

		now := time.Now()
		if targetExists && targetInode != nil {
			if targetInode.Data != nil {
				if err := targetInode.Data.Close(); err != nil {
					klog.Warningf("Failed to close target inode %d data during rename: %v", targetInodeID, err)
				}
				targetInode.Data = nil
			}
			if targetInode.Row.GetIsDir() {
				delete(v.dirParents, targetInodeID)
			} else {
				targetInode.mutate(func(row *pb.Inode) {
					if row.GetNlink() > 1 {
						row.Nlink--
					} else {
						row.Nlink = 0
					}
					row.Ctime = timestamppb.New(now)
				})
			}
		}

		childInode.mutate(func(row *pb.Inode) {
			row.Ctime = timestamppb.New(now)
		})

		if oldParentInode != nil {
			oldParentInode.mutate(func(row *pb.Inode) {
				row.Mtime = timestamppb.New(now)
				row.Ctime = timestamppb.New(now)
			})
		}
		if newParentInode != nil {
			newParentInode.mutate(func(row *pb.Inode) {
				row.Mtime = timestamppb.New(now)
				row.Ctime = timestamppb.New(now)
			})
		}

		var commitSeq uint64
		if _, err := tx.Delete(ctx, &pb.DirEntry{ParentIno: proto.Uint64(oldParentInodeID), Name: proto.String(oldName)}); err != nil {
			return nil, nil, fmt.Errorf("failed to log old dir entry deletion: %w", err)
		}
		if targetExists {
			if _, err := tx.Delete(ctx, &pb.DirEntry{ParentIno: proto.Uint64(newParentInodeID), Name: proto.String(newName)}); err != nil {
				return nil, nil, fmt.Errorf("failed to log target dir entry deletion: %w", err)
			}
			if targetInode != nil {
				if targetInode.Row.GetIsDir() {
					if _, err := tx.Delete(ctx, &pb.Inode{Ino: proto.Uint64(targetInode.Row.GetIno())}); err != nil {
						return nil, nil, fmt.Errorf("failed to log target inode deletion: %w", err)
					}
				} else if targetInode.Row.GetNlink() == 0 {
					if v.hasOpenHandlesLocked(targetInodeID) {
						if _, err := tx.Update(ctx, targetInode.Row); err != nil {
							return nil, nil, fmt.Errorf("failed to log target orphan inode update: %w", err)
						}
					} else {
						if _, err := tx.Delete(ctx, &pb.Inode{Ino: proto.Uint64(targetInode.Row.GetIno())}); err != nil {
							return nil, nil, fmt.Errorf("failed to log target inode deletion: %w", err)
						}
						for _, cMsg := range targetChunkMsgs {
							c := cMsg.(*pb.FileChunk)
							if _, err := tx.Delete(ctx, &pb.FileChunk{Ino: proto.Uint64(targetInodeID), Index: proto.Uint32(c.GetIndex())}); err != nil {
								return nil, nil, fmt.Errorf("failed to log chunk deletion for target inode %d: %w", targetInodeID, err)
							}
						}
					}
				} else {
					if _, err := tx.Update(ctx, targetInode.Row); err != nil {
						return nil, nil, fmt.Errorf("failed to log target inode update: %w", err)
					}
				}
			}
		}
		if _, err := tx.Insert(ctx, &pb.DirEntry{
			ParentIno: proto.Uint64(newParentInodeID),
			Name:      proto.String(newName),
			Ino:       childInode.Row.GetIno(),
			IsDir:     childInode.Row.GetIsDir(),
			Mode:      childInode.Row.GetMode(),
		}); err != nil {
			return nil, nil, fmt.Errorf("failed to log new dir entry insertion: %w", err)
		}
		if childInode != nil {
			if _, err := tx.Update(ctx, childInode.Row); err != nil {
				return nil, nil, fmt.Errorf("failed to log child inode update: %w", err)
			}
		}
		if oldParentInode != nil {
			if _, err := tx.Update(ctx, oldParentInode.Row); err != nil {
				return nil, nil, fmt.Errorf("failed to log old parent inode update: %w", err)
			}
		}
		if newParentInode != nil && newParentInodeID != oldParentInodeID {
			if _, err := tx.Update(ctx, newParentInode.Row); err != nil {
				return nil, nil, fmt.Errorf("failed to log new parent inode update: %w", err)
			}
		}
		commitSeq, err = tx.Commit(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to commit rename transaction: %w", err)
		}
		v.applyTxChangesLocked(ctx, tx)

		waitFn := v.makeWaitFn(commitSeq, nil)

		attr := toEntryAttr(childInode.Row, newName, childInode.RedirectURL, v.rootInodeID)
		v.broadcaster.Broadcast(v.volumeID, &pb.WatchVolumeResponse{
			EventType:      pb.WatchEventType_EVENT_RENAMED,
			Attr:           attr,
			Inode:          childInode.Row.GetIno(),
			OldParentInode: oldParentInodeID,
			OldName:        oldName,
			ParentInode:    newParentInodeID,
			Name:           newName,
		})

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

func (v *Volume) Fsync(ctx context.Context, inodeID uint64) error {
	v.mu.RLock()
	if inodeID == 0 {
		inodeID = v.rootInodeID
	}
	_, err := v.getOrLoadInodeLocked(ctx, nil, inodeID)
	isRoot := (inodeID == v.rootInodeID)
	v.mu.RUnlock()
	if err != nil {
		return err
	}

	if isRoot {
		if err := v.waitForVolumeUploads(ctx); err != nil {
			return err
		}
	} else {
		if err := v.waitForInodeUploads(ctx, inodeID); err != nil {
			return err
		}
	}

	if v.stream != nil {
		if err := v.stream.Flush(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (v *Volume) AllocFh(inodeID uint64) uint64 {
	inodeID = v.normalizeInodeID(inodeID)
	fh := v.nextFh.Add(1)
	v.handlesMu.Lock()
	if v.handles == nil {
		v.handles = make(map[uint64]uint64)
	}
	if v.openInodes == nil {
		v.openInodes = make(map[uint64]int)
	}
	v.handles[fh] = inodeID
	v.openInodes[inodeID]++
	v.handlesMu.Unlock()
	return fh
}

func (v *Volume) hasOpenHandlesLocked(inodeID uint64) bool {
	v.handlesMu.Lock()
	defer v.handlesMu.Unlock()
	return v.openInodes != nil && v.openInodes[inodeID] > 0
}

func (v *Volume) Open(ctx context.Context, inodeID uint64, flags uint32) (uint64, error) {
	v.mu.RLock()
	inodeID = v.normalizeInodeID(inodeID)
	node, err := v.getOrLoadInodeLocked(ctx, nil, inodeID)
	v.mu.RUnlock()
	if err != nil {
		return 0, err
	}
	if node == nil {
		return 0, syscall.ENOENT
	}
	fh := v.AllocFh(inodeID)
	return fh, nil
}

func (v *Volume) Release(ctx context.Context, inodeID uint64, fh uint64) error {
	v.mu.Lock()
	defer v.mu.Unlock()

	tx := v.beginTxLocked("Release")

	inodeID = v.normalizeInodeID(inodeID)

	v.handlesMu.Lock()
	if fh != 0 {
		if ino, ok := v.handles[fh]; ok {
			inodeID = ino
			delete(v.handles, fh)
		}
	}
	if v.openInodes != nil {
		if v.openInodes[inodeID] > 1 {
			v.openInodes[inodeID]--
			v.handlesMu.Unlock()
			return nil
		}
		delete(v.openInodes, inodeID)
	}
	v.handlesMu.Unlock()

	node, err := v.getOrLoadInodeLocked(ctx, tx, inodeID)
	if err != nil || node == nil {
		return nil
	}

	if !node.Row.GetIsDir() && node.Row.GetNlink() == 0 {
		chunkPrefix, err := sds.EncodeKeyPrefix(&pb.FileChunk{Ino: proto.Uint64(inodeID)}, 1)
		if err != nil {
			return fmt.Errorf("failed to encode chunk key prefix for inode %d: %w", inodeID, err)
		}
		chunkMsgs, err := v.scanSQLiteRowsLocked(ctx, tx, "objectfs.v1alpha1.FileChunk", chunkPrefix)
		if err != nil {
			return fmt.Errorf("failed to scan chunk rows for inode %d: %w", inodeID, err)
		}

		if _, err := tx.Delete(ctx, &pb.Inode{Ino: proto.Uint64(inodeID)}); err != nil {
			return fmt.Errorf("failed to log orphan inode deletion: %w", err)
		}
		for _, cMsg := range chunkMsgs {
			c := cMsg.(*pb.FileChunk)
			if _, err := tx.Delete(ctx, &pb.FileChunk{Ino: proto.Uint64(inodeID), Index: proto.Uint32(c.GetIndex())}); err != nil {
				return fmt.Errorf("failed to log chunk deletion for inode %d: %w", inodeID, err)
			}
		}
		commitSeq, err := tx.Commit(ctx)
		if err != nil {
			return fmt.Errorf("failed to commit orphan deletion transaction: %w", err)
		}
		v.applyTxChangesLocked(ctx, tx)
		waitFn := v.makeWaitFn(commitSeq, nil)
		if waitFn != nil {
			if err := waitFn(ctx); err != nil {
				return fmt.Errorf("failed to wait for orphan deletion sync: %w", err)
			}
		}
	}
	return nil
}

func (v *Volume) ResolvePath(ctx context.Context, p string) (uint64, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	inodeID, _, _, err := v.resolvePathLocked(ctx, p)
	return inodeID, err
}

func (v *Volume) flushToBackendLocked(ctx context.Context) error {
	if v.backend == nil {
		return nil
	}

	if v.stream != nil && v.stream.ReplicationLevel() == walclient.Permanent {
		if err := v.stream.Flush(ctx); err != nil {
			return fmt.Errorf("failed to flush WAL stream: %w", err)
		}
	}

	if snap, ok := v.metadataView.Index().(sds.Snapshotter); ok && v.backend != nil {
		if err := v.flushOverlayLocked(ctx); err != nil {
			return err
		}
		snapKey, actualPos, err := snap.PublishSnapshot(ctx, v.backend)
		if err != nil {
			return fmt.Errorf("failed to publish index snapshot: %w", err)
		}
		if v.metadataStream != nil && snapKey != "" {
			format := ""
			if v.indexFactory != nil {
				format = v.indexFactory.Format()
			}
			ptr := &sdsv1.SnapshotPointer{
				Position:  actualPos,
				Format:    format,
				Location:  snapKey,
				Name:      path.Base(snapKey),
				CreatedAt: timestamppb.Now(),
			}
			if _, err := v.metadataStream.AppendSnapshotPointer(ctx, ptr); err != nil {
				return fmt.Errorf("failed to append snapshot pointer: %w", err)
			}
			v.snapshotPointers = append(v.snapshotPointers, ptr)
		}

		volKey := fmt.Sprintf("volumes/%s/stream.id", v.volumeID)
		streamObj := blob.NewByteStreamFromBytes([]byte(v.streamID.String()))
		if _, err := v.backend.PutObject(ctx, "", volKey, streamObj); err != nil {
			_ = streamObj.Close()
			return fmt.Errorf("failed to save volume marker: %w", err)
		}
		_ = streamObj.Close()
	}

	return nil
}

func (v *Volume) FlushToBackend(ctx context.Context) error {
	_ = v.waitForVolumeUploads(ctx)

	v.snapshotMu.Lock()
	defer v.snapshotMu.Unlock()

	v.mu.Lock()
	defer v.mu.Unlock()
	return v.flushToBackendLocked(ctx)
}

// ApplySDSChangeLocked applies a single committed SDS change to the metadata index.
func (v *Volume) ApplySDSChangeLocked(ctx context.Context, change sds.Change) error {
	if v.metadataView != nil {
		if err := v.metadataView.ApplyChangesSync(ctx, []sds.Change{change}); err != nil {
			return err
		}
		if change.Seq > v.lastCommitSeq {
			v.lastCommitSeq = change.Seq
		}
		if change.TypeName == "objectfs.v1alpha1.Inode" || change.TypeID == 16 {
			if inode, ok := change.Row.(*pb.Inode); ok {
				ino := inode.GetIno()
				if ino >= v.nextInode {
					v.nextInode = ((ino / erofs.DefaultInodeStride) + 1) * erofs.DefaultInodeStride
				}
			}
		}
	}
	return nil
}

// ReplaySDSChanges applies an ordered list of committed SDS changes to the volume.
func (v *Volume) ReplaySDSChanges(changes []sds.Change) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	ctx := context.Background()
	for _, c := range changes {
		if err := v.ApplySDSChangeLocked(ctx, c); err != nil {
			return err
		}
	}
	return nil
}

func (v *Volume) initNextInodeFromStatsLocked(ctx context.Context, highestReplayedIno uint64) error {
	var maxIno uint64
	if v.liveStats != nil {
		maxIno = v.liveStats.GetMaxIno()
	} else if v.metadataView != nil {
		if m := v.metadataView.Stats(); m != nil {
			if vs, ok := m.(*pb.VolumeStats); ok && vs != nil {
				maxIno = vs.GetMaxIno()
			}
		}
	}
	if highestReplayedIno > maxIno {
		maxIno = highestReplayedIno
	}
	if maxIno > 0 {
		next := ((maxIno + erofs.DefaultInodeStride) / erofs.DefaultInodeStride) * erofs.DefaultInodeStride
		if next > v.nextInode {
			v.nextInode = next
		}
	}
	if v.nextInode < erofs.DefaultInodeStride {
		v.nextInode = erofs.DefaultInodeStride
	}
	return nil
}

func (v *Volume) shouldDebugCheckStats() bool {
	if v.debugStatsCheck != nil {
		return *v.debugStatsCheck
	}
	return testing.Testing()
}

func (v *Volume) assertStatsMatchLocked(reason string) {
	if !v.shouldDebugCheckStats() || v.liveStats == nil || v.metadataView == nil {
		return
	}
	if v.metadataView.Lag(v.lastCommitSeq) > 0 || v.metadataView.UnappliedBytes() > 0 {
		return
	}
	var durable *pb.VolumeStats
	if m := v.metadataView.Stats(); m != nil {
		if vs, ok := m.(*pb.VolumeStats); ok && vs != nil {
			durable = vs
		}
	}
	if durable == nil {
		durable = &pb.VolumeStats{Name: VolumeStatsRowName}
	}
	if !proto.Equal(v.liveStats, durable) {
		panic(fmt.Sprintf("volume stats divergence [%s] (%s):\n  live:    %+v\n  durable: %+v", v.volumeID, reason, v.liveStats, durable))
	}
}

// checkStatsOnBatchApplied is called by the view's background applier outside the view lock
// when a batch is applied. In debug mode, it starts a goroutine per applied batch to check
// stats consistency without blocking the applier or deadlocking with callers holding v.mu;
// this is intended and safe for test and debug verification.
func (v *Volume) checkStatsOnBatchApplied(appliedPos uint64) {
	v.batchCheckWg.Add(1)
	go func() {
		defer v.batchCheckWg.Done()
		v.mu.Lock()
		defer v.mu.Unlock()
		if v.closed {
			return
		}
		if appliedPos >= v.lastCommitSeq && v.lastCommitSeq > 0 {
			v.assertStatsMatchLocked("batch brought appliedPos to lastCommitSeq")
		}
	}()
}

func (v *Volume) initLiveStatsLocked() {
	if v.metadataView != nil {
		if m := v.metadataView.Stats(); m != nil {
			if vs, ok := m.(*pb.VolumeStats); ok && vs != nil {
				v.liveStats = proto.Clone(vs).(*pb.VolumeStats)
				return
			}
		}
	}
	v.liveStats = &pb.VolumeStats{Name: VolumeStatsRowName}
}

func (v *Volume) rebuildIndex(ctx context.Context) (sds.LocalIndex, uint64, error) {
	if v.indexFactory == nil {
		f, err := sds.GetIndexFactory("sqlite")
		if err != nil {
			return nil, 0, err
		}
		v.indexFactory = f
	}

	var (
		rebuiltIdx sds.LocalIndex
		snapPos    uint64
		err        error
	)

	// 1. Try to restore latest snapshot from object storage
	if v.backend != nil {
		snapKey, _, err := v.indexFactory.FindLatestSnapshot(ctx, v.backend, v.streamID.String(), 0)
		if err == nil && snapKey != "" {
			restoredIdx, rPos, err := v.indexFactory.RestoreSnapshot(ctx, v.backend, v.streamID.String(), snapKey, v.localStorageDir)
			if err == nil && restoredIdx != nil {
				rebuiltIdx = restoredIdx
				snapPos = rPos
			}
		}
	}

	// 2. If no snapshot restored, create new empty local index
	if rebuiltIdx == nil {
		rebuiltIdx, err = v.indexFactory.NewEmpty(ctx, v.streamID.String(), v.localStorageDir)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to create new empty index: %w", err)
		}
		snapPos = 0
	}

	// 3. Sync registry
	if v.metadataStream != nil {
		if err := rebuiltIdx.SyncRegistry(ctx, v.metadataStream.Registry()); err != nil {
			_ = rebuiltIdx.Close()
			return nil, 0, fmt.Errorf("failed to sync registry to rebuilt index: %w", err)
		}
	}

	// 4. If starting from scratch (snapPos == 0), initialize root inode
	if snapPos == 0 {
		rootInodeMsg := &pb.Inode{
			Ino:       proto.Uint64(1),
			Mode:      0755 | syscall.S_IFDIR,
			Mtime:     timestamppb.Now(),
			IsDir:     true,
			Nlink:     2,
			ParentIno: proto.Uint64(1),
		}
		keyBytes, valBytes, _ := sds.SplitKeyAndNonKey(rootInodeMsg, []int32{1})
		initChanges := []sds.Change{
			{
				Seq:      0,
				TypeID:   16,
				TypeName: "objectfs.v1alpha1.Inode",
				Op:       sds.OpCreate,
				Key:      sds.NewKeyFromBytes(keyBytes),
				RawKey:   keyBytes,
				RawVal:   valBytes,
				Row:      rootInodeMsg,
			},
		}
		if err := rebuiltIdx.ApplyBatch(ctx, initChanges); err != nil {
			_ = rebuiltIdx.Close()
			return nil, 0, fmt.Errorf("failed to init root inode in rebuilt index: %w", err)
		}
	}

	// 5. Replay the stream from snapPos up to current position.
	if v.stream != nil {
		cr := sds.NewChangeReader(record.WithDecoderRegistry(v.metadataStream.Registry()))
		seenSeqs := make(map[uint64]bool)
		expectedSeq := snapPos + 1

		applyChanges := func(changes []sds.Change) error {
			if len(changes) == 0 {
				return nil
			}
			if err := rebuiltIdx.ApplyBatch(ctx, changes); err != nil {
				return fmt.Errorf("failed to apply replayed changes: %w", err)
			}
			for _, ch := range changes {
				if ch.Seq > snapPos {
					snapPos = ch.Seq
				}
			}
			return nil
		}

		_, witnessHead, _ := v.stream.Watermarks()

		// 5a. Authoritative tail from WAL buffer up to witnessHead
		if witnessHead > snapPos {
			tailIter, err := v.stream.Tail(ctx, snapPos)
			if err != nil {
				_ = rebuiltIdx.Close()
				return nil, 0, fmt.Errorf("failed to tail authoritative stream from wal-buffer during rebuild: %w", err)
			}
			for seq, payload := range tailIter {
				if seq <= snapPos || seenSeqs[seq] {
					continue
				}
				if seq != expectedSeq {
					_ = rebuiltIdx.Close()
					return nil, 0, fmt.Errorf("stream contiguity violation in wal-buffer tail during rebuild: expected seq %d, got %d", expectedSeq, seq)
				}
				expectedSeq++
				seenSeqs[seq] = true
				changes, err := cr.Feed(seq, payload)
				if err != nil {
					_ = rebuiltIdx.Close()
					return nil, 0, fmt.Errorf("failed to feed authoritative record at seq %d during rebuild: %w", seq, err)
				}
				if err := applyChanges(changes); err != nil {
					_ = rebuiltIdx.Close()
					return nil, 0, err
				}
				if seq >= witnessHead {
					break
				}
			}
			if expectedSeq-1 < witnessHead {
				_ = rebuiltIdx.Close()
				return nil, 0, fmt.Errorf("failed to rebuild authoritative stream from wal-buffer: expected head %d but only reached %d", witnessHead, expectedSeq-1)
			}
		}

		// 5b. Local unacknowledged records > witnessHead
		recovered := v.stream.RecoveredRecords()
		for _, rec := range recovered {
			if rec.StreamSeq <= snapPos || rec.StreamSeq <= witnessHead || seenSeqs[rec.StreamSeq] {
				continue
			}
			if rec.StreamSeq != expectedSeq {
				_ = rebuiltIdx.Close()
				return nil, 0, fmt.Errorf("stream contiguity violation in local records during rebuild: expected seq %d, got %d", expectedSeq, rec.StreamSeq)
			}
			expectedSeq++
			seenSeqs[rec.StreamSeq] = true
			changes, err := cr.Feed(rec.StreamSeq, rec.Payload)
			if err != nil {
				_ = rebuiltIdx.Close()
				return nil, 0, fmt.Errorf("failed to feed local record at seq %d during rebuild: %w", rec.StreamSeq, err)
			}
			if err := applyChanges(changes); err != nil {
				_ = rebuiltIdx.Close()
				return nil, 0, err
			}
		}
	} else if v.memAppender != nil {
		payloads := v.memAppender.Payloads()
		startSeq := v.memAppender.StartSeq()
		if len(payloads) > 0 {
			cr := sds.NewChangeReader(record.WithDecoderRegistry(v.metadataStream.Registry()))
			expectedSeq := snapPos + 1
			for seqOffset, p := range payloads {
				seq := startSeq + uint64(seqOffset)
				if seq <= snapPos {
					continue
				}
				if seq != expectedSeq {
					_ = rebuiltIdx.Close()
					return nil, 0, fmt.Errorf("stream contiguity violation in memAppender: expected seq %d, got %d", expectedSeq, seq)
				}
				expectedSeq++
				chs, err := cr.Feed(seq, p)
				if err != nil {
					_ = rebuiltIdx.Close()
					return nil, 0, fmt.Errorf("failed to feed memory record %d: %w", seq, err)
				}
				if len(chs) > 0 {
					if err := rebuiltIdx.ApplyBatch(ctx, chs); err != nil {
						_ = rebuiltIdx.Close()
						return nil, 0, fmt.Errorf("failed to apply replayed changes: %w", err)
					}
					for _, ch := range chs {
						if ch.Seq > snapPos {
							snapPos = ch.Seq
						}
					}
				}
			}
		}
	}

	// 6. Ensure stats row exists or backfill stats in the rebuilt index
	statsPK := sds.NewKeyFromBytes([]byte(VolumeStatsRowName))
	if _, ok, sErr := rebuiltIdx.Get(ctx, VolumeStatsTypeName, statsPK); sErr == nil && !ok {
		if stats, cErr := v.countStatsFromIndex(ctx, rebuiltIdx); cErr == nil && stats != nil {
			if def, _, ok := v.metadataStream.Registry().LookupByName(VolumeStatsTypeName); ok {
				kBytes, vBytes, _ := sds.SplitKeyAndNonKey(stats, def.GetKeyFields())
				_ = rebuiltIdx.ApplyBatch(ctx, []sds.Change{
					{
						Seq:      snapPos,
						TypeName: VolumeStatsTypeName,
						TypeID:   def.GetId(),
						Op:       sds.OpCreate,
						Key:      sds.NewKeyFromBytes(kBytes),
						RawKey:   kBytes,
						RawVal:   vBytes,
						Row:      stats,
					},
				})
			}
		}
	}
	return rebuiltIdx, snapPos, nil
}

func (v *Volume) countStatsFromIndex(ctx context.Context, target sds.LocalIndex) (*pb.VolumeStats, error) {
	st := &pb.VolumeStats{Name: VolumeStatsRowName}
	for msg, err := range target.Scan(ctx, "objectfs.v1alpha1.Inode", nil) {
		if err != nil {
			return nil, err
		}
		if inode, ok := msg.(*pb.Inode); ok {
			UpdateStatsForInodeChange(st, nil, inode)
		}
	}
	for msg, err := range target.Scan(ctx, "objectfs.v1alpha1.FileChunk", nil) {
		if err != nil {
			return nil, err
		}
		if chunk, ok := msg.(*pb.FileChunk); ok {
			UpdateStatsForFileChunkChange(st, nil, chunk)
		}
	}
	return st, nil
}

func (v *Volume) loadFromBackendMetadataLocked(ctx context.Context) error {
	if v.indexFactory == nil {
		f, err := sds.GetIndexFactory("sqlite")
		if err != nil {
			return err
		}
		v.indexFactory = f
	}

	var snapPos uint64
	var initialized bool

	var remoteSnapKey string
	var remoteSnapPos uint64
	if v.backend != nil && v.indexFactory != nil {
		snapKey, rPos, err := v.indexFactory.FindLatestSnapshot(ctx, v.backend, v.streamID.String(), 0)
		if err == nil && snapKey != "" {
			remoteSnapKey = snapKey
			remoteSnapPos = rPos
		}
	}

	// 1. Check if local index is already open or existing, and not stale compared to remote snapshot
	isRemoteWAL := (v.stream != nil && v.stream.ReplicationLevel() > walclient.Local)
	if !isRemoteWAL {
		if v.metadataView != nil {
			pos := v.metadataView.Position()
			if pos > 0 && pos >= remoteSnapPos {
				snapPos = pos
				initialized = true
			}
		} else if v.localStorageDir != "" {
			localIdx, found, err := v.indexFactory.OpenLocal(ctx, v.streamID.String(), v.localStorageDir)
			if err == nil && found && localIdx != nil {
				if localIdx.Position() >= remoteSnapPos {
					if err := v.initMetadataViewLocked(ctx, localIdx); err != nil {
						return fmt.Errorf("failed to init metadata view from local index: %w", err)
					}
					if localIdx.Position() > 0 {
						snapPos = localIdx.Position()
						initialized = true
					}
				} else {
					klog.Infof("Volume %s: local SQLite metadata index on disk is stale (local pos %d < remote snapshot pos %d); restoring snapshot %s", v.volumeID, localIdx.Position(), remoteSnapPos, remoteSnapKey)
					_ = localIdx.Close()
				}
			}
		}
	}

	// 2. Otherwise restore latest published snapshot
	if !initialized && remoteSnapKey != "" && v.backend != nil {
		if v.metadataView != nil {
			_ = v.metadataView.Close()
			v.metadataView = nil
		}
		restoredIdx, rPos, err := v.indexFactory.RestoreSnapshot(ctx, v.backend, v.streamID.String(), remoteSnapKey, v.localStorageDir)
		if err == nil && restoredIdx != nil {
			if err := v.initMetadataViewLocked(ctx, restoredIdx); err != nil {
				return fmt.Errorf("failed to init metadata view from restored snapshot: %w", err)
			}
			snapPos = rPos
			initialized = true
		}
	}

	if !initialized {
		if isRemoteWAL {
			if v.metadataView != nil {
				_ = v.metadataView.Close()
				v.metadataView = nil
			}
			if v.localStorageDir != "" {
				if err := os.RemoveAll(v.localStorageDir); err != nil && !os.IsNotExist(err) {
					return fmt.Errorf("failed to clear stale local storage dir %s: %w", v.localStorageDir, err)
				}
				if err := os.MkdirAll(v.localStorageDir, 0755); err != nil {
					return fmt.Errorf("failed to recreate local storage dir %s: %w", v.localStorageDir, err)
				}
			}
		}
		if v.metadataView == nil {
			if err := v.initMetadataViewLocked(ctx, nil); err != nil {
				return fmt.Errorf("failed to init empty metadata view: %w", err)
			}
		}
		rootKey, _ := pkInode.Extract(&pb.Inode{Ino: proto.Uint64(1)})
		msg, ok, _ := v.metadataView.Get(ctx, "objectfs.v1alpha1.Inode", rootKey)
		if !ok || msg == nil {
			rootInodeMsg := &pb.Inode{
				Ino:   proto.Uint64(1),
				Mode:  0755 | syscall.S_IFDIR,
				Mtime: timestamppb.Now(),
				IsDir: true,
			}
			keyBytes, valBytes, _ := sds.SplitKeyAndNonKey(rootInodeMsg, []int32{1})
			initChanges := []sds.Change{
				{
					Seq:      0,
					TypeID:   16,
					TypeName: "objectfs.v1alpha1.Inode",
					Op:       sds.OpCreate,
					Key:      sds.NewKeyFromBytes(keyBytes),
					RawKey:   keyBytes,
					RawVal:   valBytes,
					Row:      rootInodeMsg,
				},
			}
			_ = v.metadataView.ApplyChangesSync(ctx, initChanges)
		}
		v.rootInodeID = 1
		v.nextInode = erofs.DefaultInodeStride
		v.dirParents[1] = 1
	}

	// Replay stream from snapPos
	var highestReplayedIno uint64
	if v.stream != nil {
		cr := sds.NewChangeReader(record.WithDecoderRegistry(v.metadataStream.Registry()))
		seenSeqs := make(map[uint64]bool)
		expectedSeq := snapPos + 1

		applyChangeList := func(changes []sds.Change) error {
			if len(changes) == 0 {
				return nil
			}
			if err := v.metadataView.ApplyChangesSync(ctx, changes); err != nil {
				return fmt.Errorf("failed to apply recovered SDS changes: %w", err)
			}
			for _, ch := range changes {
				if ch.Seq > v.lastCommitSeq {
					v.lastCommitSeq = ch.Seq
				}
				if ch.TypeName == InodeTypeName {
					if in, ok := ch.Row.(*pb.Inode); ok && in != nil && in.GetIno() > highestReplayedIno {
						highestReplayedIno = in.GetIno()
					}
				}
			}
			return nil
		}

		_, witnessHead, _ := v.stream.Watermarks()
		dec := record.NewDecoder(record.WithDecoderRegistry(v.metadataStream.Registry()))

		// 1. Authoritative tail from WAL buffer up to witnessHead
		if witnessHead > snapPos {
			tailIter, err := v.stream.Tail(ctx, snapPos)
			if err != nil {
				return fmt.Errorf("failed to tail authoritative stream from wal-buffer: %w", err)
			}
			for seq, payload := range tailIter {
				if seq <= snapPos || seenSeqs[seq] {
					continue
				}
				if seq != expectedSeq {
					return fmt.Errorf("stream contiguity violation in wal-buffer tail: expected seq %d, got %d", expectedSeq, seq)
				}
				expectedSeq++
				seenSeqs[seq] = true
				rec, decErr := dec.Decode(payload)
				if decErr != nil {
					klog.Warningf("volume %s: failed to decode authoritative record at seq %d: %v", v.volumeID, seq, decErr)
				} else if rec.TypeID == record.TypeIDSnapshotPointer {
					if ptr, ok := rec.Message.(*sdsv1.SnapshotPointer); ok && ptr != nil {
						v.snapshotPointers = append(v.snapshotPointers, ptr)
					}
				}
				changes, err := cr.Feed(seq, payload)
				if err != nil {
					return fmt.Errorf("failed to feed authoritative record at seq %d: %w", seq, err)
				}
				if err := applyChangeList(changes); err != nil {
					return err
				}
				if seq >= witnessHead {
					break
				}
			}
			if expectedSeq-1 < witnessHead {
				return fmt.Errorf("failed to recover authoritative stream from wal-buffer: expected head %d but only reached %d", witnessHead, expectedSeq-1)
			}
		}

		// 2. Local records for the part the buffer doesn't have (unacknowledged writes > witnessHead, or local-only stream)
		recovered := v.stream.RecoveredRecords()
		for _, rec := range recovered {
			if rec.StreamSeq <= snapPos || rec.StreamSeq <= witnessHead || seenSeqs[rec.StreamSeq] {
				continue
			}
			if rec.StreamSeq != expectedSeq {
				return fmt.Errorf("stream contiguity violation in local recovered records: expected seq %d, got %d", expectedSeq, rec.StreamSeq)
			}
			expectedSeq++
			seenSeqs[rec.StreamSeq] = true
			r, decErr := dec.Decode(rec.Payload)
			if decErr != nil {
				klog.Warningf("volume %s: failed to decode local recovered record at seq %d: %v", v.volumeID, rec.StreamSeq, decErr)
			} else if r.TypeID == record.TypeIDSnapshotPointer {
				if ptr, ok := r.Message.(*sdsv1.SnapshotPointer); ok && ptr != nil {
					v.snapshotPointers = append(v.snapshotPointers, ptr)
				}
			}
			changes, err := cr.Feed(rec.StreamSeq, rec.Payload)
			if err != nil {
				return fmt.Errorf("failed to feed local recovered record at seq %d: %w", rec.StreamSeq, err)
			}
			if err := applyChangeList(changes); err != nil {
				return err
			}
		}

		cr.DiscardPending()
	}

	if v.metadataView != nil {
		v.metadataView.ClearCache()
	}

	v.initLiveStatsLocked()
	if err := v.initNextInodeFromStatsLocked(ctx, highestReplayedIno); err != nil {
		return fmt.Errorf("failed to initialize next inode from stats: %w", err)
	}
	v.assertStatsMatchLocked("after loadFromBackendMetadataLocked")
	return nil
}

func (v *Volume) LoadFromBackend(ctx context.Context) error {
	v.mu.Lock()
	defer v.mu.Unlock()

	return v.loadFromBackendMetadataLocked(ctx)
}

// CreateSnapshot creates and returns a new index snapshot of the current volume state.
func (v *Volume) CreateSnapshot(ctx context.Context, name string) (string, error) {
	v.snapshotMu.Lock()
	defer v.snapshotMu.Unlock()

	v.mu.Lock()
	defer v.mu.Unlock()

	if v.backend == nil {
		return "", fmt.Errorf("no backend configured")
	}

	snap, ok := v.metadataView.Index().(sds.Snapshotter)
	if !ok {
		return "", fmt.Errorf("volume metadata index does not support snapshots")
	}

	if err := v.flushOverlayLocked(ctx); err != nil {
		return "", fmt.Errorf("failed to flush overlay before snapshot: %w", err)
	}

	snapKey, actualPos, err := snap.PublishSnapshot(ctx, v.backend)
	if err != nil {
		return "", fmt.Errorf("failed to publish index snapshot: %w", err)
	}

	format := ""
	if v.indexFactory != nil {
		format = v.indexFactory.Format()
	}

	snapName := name
	if snapName == "" {
		snapName = path.Base(snapKey)
	}

	ptr := &sdsv1.SnapshotPointer{
		Position:  actualPos,
		Format:    format,
		Location:  snapKey,
		Name:      snapName,
		CreatedAt: timestamppb.Now(),
	}

	if v.metadataStream != nil && snapKey != "" {
		if _, err := v.metadataStream.AppendSnapshotPointer(ctx, ptr); err != nil {
			return "", fmt.Errorf("failed to append snapshot pointer: %w", err)
		}
	}
	v.snapshotPointers = append(v.snapshotPointers, ptr)

	return snapName, nil
}

// ListSnapshots returns all snapshot pointers for this volume sorted chronologically / by position.
func (v *Volume) ListSnapshots(ctx context.Context) ([]*sdsv1.SnapshotPointer, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.listSnapshotsLocked(ctx)
}

func (v *Volume) listSnapshotsLocked(ctx context.Context) ([]*sdsv1.SnapshotPointer, error) {
	pointersByLoc := make(map[string]*sdsv1.SnapshotPointer)
	for _, ptr := range v.snapshotPointers {
		pointersByLoc[ptr.GetLocation()] = ptr
	}

	if v.backend != nil && v.indexFactory != nil {
		format := v.indexFactory.Format()
		prefix := fmt.Sprintf("streams/%s/snapshots/%s/", v.streamID.String(), format)
		keys, err := v.backend.ListObjects(ctx, "", prefix)
		if err != nil {
			return nil, fmt.Errorf("failed to list snapshot objects from backend: %w", err)
		}
		for _, key := range keys {
			if _, ok := pointersByLoc[key]; !ok {
				var pos uint64
				var parseErr error
				if format == "sqlite" {
					_, pos, parseErr = sqlite.ParseSnapshotKey(key)
				} else if format == "memory" {
					_, pos, parseErr = memtable.ParseSnapshotKey(key)
				}
				if parseErr != nil {
					return nil, fmt.Errorf("failed to parse snapshot key %q: %w", key, parseErr)
				}
				ptr := &sdsv1.SnapshotPointer{
					Position: pos,
					Format:   format,
					Location: key,
					Name:     path.Base(key),
				}
				pointersByLoc[key] = ptr
			}
		}
	}

	var res []*sdsv1.SnapshotPointer
	for _, ptr := range pointersByLoc {
		res = append(res, ptr)
	}
	sort.Slice(res, func(i, j int) bool {
		if res[i].GetPosition() != res[j].GetPosition() {
			return res[i].GetPosition() < res[j].GetPosition()
		}
		return res[i].GetName() < res[j].GetName()
	})
	return res, nil
}

// GetSnapshotInfo returns metadata (position, created_at timestamp) for a snapshot.
func (v *Volume) GetSnapshotInfo(ctx context.Context, snapshotName string) (*pb.SnapshotInfo, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.getSnapshotInfoLocked(ctx, snapshotName)
}

func (v *Volume) getSnapshotInfoLocked(ctx context.Context, snapshotName string) (*pb.SnapshotInfo, error) {
	pointers, err := v.listSnapshotsLocked(ctx)
	if err != nil {
		return nil, err
	}
	for _, p := range pointers {
		if p.GetName() == snapshotName || p.GetLocation() == snapshotName || path.Base(p.GetLocation()) == snapshotName {
			info := &pb.SnapshotInfo{
				Name:      p.GetName(),
				Position:  p.GetPosition(),
				CreatedAt: p.GetCreatedAt(),
			}
			return info, nil
		}
	}
	return nil, fmt.Errorf("snapshot %q not found", snapshotName)
}

// RestoreSnapshot restores the volume filesystem state to a specific snapshot using indexFactory.RestoreSnapshot.
// Note: Snapshot restore is currently local to the running controller instance; a controller restart
// replays subsequent stream records against the latest published snapshot. Durable restore across restarts
// is tracked in issue #225.
func (v *Volume) RestoreSnapshot(ctx context.Context, snapshotName string) error {
	v.snapshotMu.Lock()
	defer v.snapshotMu.Unlock()

	v.mu.Lock()
	defer v.mu.Unlock()

	if v.backend == nil {
		return fmt.Errorf("no backend configured")
	}
	if v.indexFactory == nil {
		return fmt.Errorf("no index factory configured")
	}

	pointers, err := v.listSnapshotsLocked(ctx)
	if err != nil {
		return fmt.Errorf("failed to list snapshots: %w", err)
	}

	var targetPtr *sdsv1.SnapshotPointer
	for _, p := range pointers {
		if p.GetName() == snapshotName || p.GetLocation() == snapshotName || path.Base(p.GetLocation()) == snapshotName {
			targetPtr = p
			break
		}
	}
	if targetPtr == nil {
		return fmt.Errorf("snapshot %q not found", snapshotName)
	}

	if v.metadataView != nil {
		if err := v.metadataView.Close(); err != nil {
			return fmt.Errorf("failed to close metadata view: %w", err)
		}
		v.metadataView = nil
	}

	restoredIdx, rPos, err := v.indexFactory.RestoreSnapshot(ctx, v.backend, v.streamID.String(), targetPtr.GetLocation(), v.localStorageDir)
	if err != nil {
		return fmt.Errorf("failed to restore snapshot %q: %w", snapshotName, err)
	}

	if err := v.initMetadataViewLocked(ctx, restoredIdx); err != nil {
		return fmt.Errorf("failed to init metadata view from restored snapshot: %w", err)
	}
	v.lastCommitSeq = rPos

	v.metadataView.ClearCache()
	v.dirParents = make(map[uint64]uint64)
	v.dirParents[1] = 1
	// Active file handles (v.handles) and open inode refcounts (v.openInodes)
	// are retained across snapshot restore because they refer to the running clients.
	v.initLiveStatsLocked()
	if err := v.initNextInodeFromStatsLocked(ctx, 0); err != nil {
		return fmt.Errorf("failed to initialize next inode from stats: %w", err)
	}
	v.assertStatsMatchLocked("after RestoreSnapshot")

	return nil
}
