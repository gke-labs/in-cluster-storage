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
	"strconv"
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
	_ "github.com/gke-labs/in-cluster-storage/pkg/sds/memtable"
	_ "github.com/gke-labs/in-cluster-storage/pkg/sds/projection/sqlite"
	_ "github.com/gke-labs/in-cluster-storage/pkg/sds/projection/table"
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
	wg  sync.WaitGroup
	err error
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
	recoveredContent map[string][]byte
	liveStats        *pb.VolumeStats
	liveStatsStale   bool
	debugStatsCheck  *bool

	localStorageDir string
	indexFactory    sds.IndexFactory
	metadataView    *view.View
	viewOpts        []view.Option
	memAppender     *memoryAppender

	snapshotRaw    io.ReaderAt
	snapshotReader *erofs.Reader

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
		volumeID:         volumeID,
		rootInodeID:      1,
		nextInode:        erofs.DefaultInodeStride,
		backend:          backend,
		blobStore:        blobStore,
		broadcaster:      broadcaster,
		maxInlineLen:     4096,      // 4KB default inline threshold for tiny files
		chunkSize:        64 * 1024, // 64KB default chunk size
		durability:       walclient.Local,
		indexFactory:     f,
		streamID:         StreamIDForVolume(volumeID),
		closedCh:         make(chan struct{}),
		recoveredContent: make(map[string][]byte),
		dirParents:       make(map[uint64]uint64),
		handles:          make(map[uint64]uint64),
		openInodes:       make(map[uint64]int),
		inodeUploads:     make(map[uint64]*InodeUpload),
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
	if !exists {
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
	if upload := v.inodeUploads[ino]; upload != nil {
		err = upload.err
		delete(v.inodeUploads, ino)
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

func (v *Volume) ensureInodeChunksLoadedLocked(ctx context.Context, node *CachedInode) error {
	if node.Row.GetIsDir() || node.Row.GetSize() == 0 {
		return nil
	}
	if node.Row.GetChunkSize() > 0 && len(node.Chunks) > 0 {
		return nil
	}
	if v.metadataView != nil {
		prefixBytes, pErr := sds.EncodeKeyPrefix(&pb.FileChunk{Ino: proto.Uint64(node.Row.GetIno())}, 1)
		if pErr == nil {
			// Rows returned by scanSQLiteRowsLocked are shared/immutable; only read fields here.
			chunkMsgs, sErr := v.scanSQLiteRowsLocked(ctx, "objectfs.v1alpha1.FileChunk", prefixBytes)
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

func (v *Volume) readChunkLocked(ctx context.Context, node *CachedInode, chunkIdx int) ([]byte, error) {
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

	_ = v.ensureInodeChunksLoadedLocked(ctx, node)

	if chunkSha, ok := node.Chunks[uint32(chunkIdx)]; ok && chunkSha != "" {
		if v.recoveredContent != nil {
			if data, ok := v.recoveredContent[chunkSha]; ok {
				res := make([]byte, len(data))
				copy(res, data)
				return res, nil
			}
		}
		if v.blobStore != nil {
			stream, err := v.blobStore.GetBlob(ctx, chunkSha)
			if err == nil {
				defer stream.Close()
				return io.ReadAll(stream)
			}
		}
	}

	if chunkIdx == 0 && (node.Row.GetSha256() != "" || node.Row.GetContentSha256() != "") && len(node.Chunks) == 0 {
		sha := node.Row.GetSha256()
		if sha == "" {
			sha = node.Row.GetContentSha256()
		}
		if v.recoveredContent != nil {
			if data, ok := v.recoveredContent[sha]; ok {
				res := make([]byte, len(data))
				copy(res, data)
				return res, nil
			}
		}
		if v.blobStore != nil {
			stream, err := v.blobStore.GetBlob(ctx, sha)
			if err == nil {
				defer stream.Close()
				return io.ReadAll(stream)
			}
		}
	}

	if node.Data != nil && node.Row.GetChunkSize() > 0 {
		off := int64(chunkIdx) * int64(node.Row.GetChunkSize())
		if off < node.Row.GetSize() {
			readLen := int64(node.Row.GetChunkSize())
			if off+readLen > node.Row.GetSize() {
				readLen = node.Row.GetSize() - off
			}
			buf := make([]byte, readLen)
			_ = node.Data.Rewind()
			if _, err := node.Data.Seek(off, io.SeekStart); err == nil {
				n, _ := io.ReadFull(node.Data, buf)
				return buf[:n], nil
			}
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

	if v.liveStatsStale {
		v.reseedLiveStatsFromDurableLocked()
	}
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

func (v *Volume) allocInode() uint64 {
	return atomic.AddUint64(&v.nextInode, erofs.DefaultInodeStride) - erofs.DefaultInodeStride
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
	if !v.liveStatsStale && v.liveStats != nil {
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
		if v.liveStatsStale {
			v.reseedLiveStatsFromDurableLocked()
		}
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
	if v.liveStatsStale {
		v.reseedLiveStatsFromDurableLocked()
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

// scanSQLiteRowsLocked queries the view for proto rows matching prefixBytes.
// The returned messages are shared and immutable; callers must not modify them in place.
func (v *Volume) scanSQLiteRowsLocked(ctx context.Context, typeName string, prefixBytes []byte) ([]proto.Message, error) {
	if v.metadataView == nil {
		return nil, nil
	}
	return v.metadataView.ScanSlice(ctx, typeName, prefixBytes)
}

func (v *Volume) scanLimitSQLiteRowsLocked(ctx context.Context, typeName string, prefixBytes []byte, limit int) ([]proto.Message, error) {
	msgs, err := v.scanSQLiteRowsLocked(ctx, typeName, prefixBytes)
	if err != nil {
		return nil, err
	}
	if limit > 0 && len(msgs) > limit {
		msgs = msgs[:limit]
	}
	return msgs, nil
}

// getSQLiteRowLocked retrieves a single proto row from the view.
// The returned message is shared and immutable; callers must not modify it in place.
func (v *Volume) getSQLiteRowLocked(ctx context.Context, typeName string, key sds.Key) (proto.Message, bool, error) {
	if v.metadataView == nil {
		return nil, false, nil
	}
	return v.metadataView.Get(ctx, typeName, key)
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
	prefix, err := sds.EncodeKeyPrefix(&pb.Inode{}, 0)
	if err != nil {
		return fmt.Errorf("failed to encode inode key prefix: %w", err)
	}
	msgs, err := v.scanSQLiteRowsLocked(ctx, "objectfs.v1alpha1.Inode", prefix)
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
	tx := v.metadataStream.Begin()
	for _, ino := range orphanInos {
		if _, err := tx.Delete(ctx, &pb.Inode{Ino: proto.Uint64(ino)}); err != nil {
			return fmt.Errorf("failed to log orphan inode %d deletion: %w", ino, err)
		}
		chunkPrefix, err := sds.EncodeKeyPrefix(&pb.FileChunk{Ino: proto.Uint64(ino)}, 1)
		if err != nil {
			return fmt.Errorf("failed to encode chunk key prefix for inode %d: %w", ino, err)
		}
		chunkMsgs, err := v.scanSQLiteRowsLocked(ctx, "objectfs.v1alpha1.FileChunk", chunkPrefix)
		if err != nil {
			return fmt.Errorf("failed to scan chunk rows for orphan inode %d: %w", ino, err)
		}
		for _, cMsg := range chunkMsgs {
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
	if err := v.applyTxChangesLocked(ctx, tx); err != nil {
		return fmt.Errorf("failed to apply orphan deletion changes: %w", err)
	}
	waitFn := v.makeWaitFn(commitSeq, nil)
	if waitFn != nil {
		if err := waitFn(ctx); err != nil {
			return fmt.Errorf("failed to wait for orphan deletion sync: %w", err)
		}
	}
	return nil
}

func (v *Volume) getOrLoadInodeLocked(ctx context.Context, inodeID uint64) (*CachedInode, error) {
	inodeID = v.normalizeInodeID(inodeID)

	key, err := pkInode.Extract(&pb.Inode{Ino: proto.Uint64(inodeID)})
	if err != nil {
		return nil, err
	}
	// Loaded Inode message is shared/immutable; CachedInode.mutate must be used for any writes.
	msg, ok, err := v.getSQLiteRowLocked(ctx, "objectfs.v1alpha1.Inode", key)
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

	_ = v.ensureInodeChunksLoadedLocked(ctx, node)

	return node, nil
}

func (v *Volume) getDirEntrySQLiteLocked(ctx context.Context, parentInodeID uint64, name string) (*pb.DirEntry, bool, error) {
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
	msg, ok, err := v.getSQLiteRowLocked(ctx, "objectfs.v1alpha1.DirEntry", key)
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
		de, ok, err := v.getDirEntrySQLiteLocked(ctx, currInodeID, part)
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

func (v *Volume) toEntryAttrLocked(ctx context.Context, inodeID uint64, name string) (*pb.EntryAttr, error) {
	node, err := v.getOrLoadInodeLocked(ctx, inodeID)
	if err != nil {
		return nil, err
	}
	return toEntryAttr(node.Row, name, node.RedirectURL, v.rootInodeID), nil
}

func (v *Volume) GetAttr(ctx context.Context, inodeID uint64) (*pb.EntryAttr, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	inodeID = v.normalizeInodeID(inodeID)
	return v.toEntryAttrLocked(ctx, inodeID, "")
}

func (v *Volume) SetAttr(ctx context.Context, inodeID uint64, mode *uint32, uid *uint32, gid *uint32, atime *time.Time, atimeNow bool, mtime *time.Time, mtimeNow bool, ctime *time.Time, ctimeNow bool) (*pb.EntryAttr, error) {
	attr, waitFn, err := func() (*pb.EntryAttr, func(context.Context) error, error) {
		v.mu.Lock()
		defer v.mu.Unlock()

		if err := v.checkBackpressureLocked(ctx); err != nil {
			return nil, nil, err
		}

		inodeID = v.normalizeInodeID(inodeID)

		node, err := v.getOrLoadInodeLocked(ctx, inodeID)
		if err != nil {
			return nil, nil, err
		}

		now := time.Now()
		modified := (mode != nil) || (uid != nil) || (gid != nil) || atimeNow || (atime != nil) || mtimeNow || (mtime != nil) || ctimeNow || (ctime != nil)

		if !modified {
			attr, err := v.toEntryAttrLocked(ctx, inodeID, "")
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

		tx := v.metadataStream.Begin()
		if _, err := tx.Update(ctx, node.Row); err != nil {
			return nil, nil, fmt.Errorf("failed to log inode update: %w", err)
		}

		commitSeq, err := tx.Commit(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to commit setattr transaction: %w", err)
		}
		if err := v.applyTxChangesLocked(ctx, tx); err != nil {
			return nil, nil, err
		}

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
		return v.toEntryAttrLocked(ctx, parentInodeID, ".")
	}
	if name == ".." {
		if parentInodeID == v.rootInodeID || parentInodeID == 1 {
			return v.toEntryAttrLocked(ctx, v.rootInodeID, "..")
		}
		parentNode, err := v.getOrLoadInodeLocked(ctx, parentInodeID)
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
		return v.toEntryAttrLocked(ctx, pIno, "..")
	}

	de, ok, err := v.getDirEntrySQLiteLocked(ctx, parentInodeID, name)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("child %s not found in inode %d: %w", name, parentInodeID, syscall.ENOENT)
	}
	return v.toEntryAttrLocked(ctx, de.GetIno(), name)
}

func (v *Volume) ReadDir(ctx context.Context, dirInodeID uint64) ([]*pb.EntryAttr, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	dirInodeID = v.normalizeInodeID(dirInodeID)

	dirNode, err := v.getOrLoadInodeLocked(ctx, dirInodeID)
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
	entryMsgs, err := v.scanSQLiteRowsLocked(ctx, "objectfs.v1alpha1.DirEntry", prefixBytes)
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
		attr, err := v.toEntryAttrLocked(ctx, de.GetIno(), name)
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
//  3. At commit, the before-image of each changed key is what the view says; the after-image is the
//     buffered row; stats deltas come from those pairs with the applier's arithmetic. Calling
//     UpdateStatsFromChanges prior to recording the transaction into the overlay ensures that the view
//     lookup yields pre-commit rows, providing identical arithmetic between live and durable stats.
func (v *Volume) applyTxChangesLocked(ctx context.Context, tx *sds.Tx) error {
	if tx == nil {
		return nil
	}
	var statsErr error
	if v.liveStats != nil {
		var reg *record.Registry
		if v.metadataStream != nil {
			reg = v.metadataStream.Registry()
		}
		beforeLookup := func(typeName string, key sds.Key) (proto.Message, bool, error) {
			if v.metadataView == nil {
				return nil, false, nil
			}
			return v.metadataView.Get(ctx, typeName, key)
		}
		statsErr = view.UpdateStatsFromChanges(tx.Changes(), v.liveStats, UpdateVolumeStats, reg, beforeLookup)
	}
	v.recordOverlayTxChangesLocked(tx) // always: the tx is committed
	if statsErr != nil {
		if v.shouldDebugCheckStats() {
			panic(fmt.Sprintf("volume stats update failed [%s]: %v", v.volumeID, statsErr))
		}
		klog.Errorf("Volume %s: live stats update failed; marking stats stale until applier drains: %v", v.volumeID, statsErr)
		v.liveStatsStale = true
	}
	return nil
}

func (v *Volume) Mkdir(ctx context.Context, parentInodeID uint64, name string, mode uint32, uid, gid uint32) (*pb.EntryAttr, error) {
	if len(name) > MaxNameLength {
		return nil, fmt.Errorf("directory name %q exceeds maximum length: %w", name, syscall.ENAMETOOLONG)
	}

	attr, waitFn, err := func() (*pb.EntryAttr, func(context.Context) error, error) {
		v.mu.Lock()
		defer v.mu.Unlock()

		if err := v.checkBackpressureLocked(ctx); err != nil {
			return nil, nil, err
		}

		parentInodeID = v.normalizeInodeID(parentInodeID)
		if name == "" || name == "." || name == ".." {
			return nil, nil, fmt.Errorf("invalid directory name %q: %w", name, syscall.EINVAL)
		}

		parentInode, err := v.getOrLoadInodeLocked(ctx, parentInodeID)
		if err != nil {
			return nil, nil, err
		}
		if !parentInode.Row.GetIsDir() {
			return nil, nil, fmt.Errorf("parent inode %d is not a directory: %w", parentInodeID, syscall.ENOTDIR)
		}

		_, exists, err := v.getDirEntrySQLiteLocked(ctx, parentInodeID, name)
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
		childInodeID := v.allocInode()

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

		tx := v.metadataStream.Begin()
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
		if err := v.applyTxChangesLocked(ctx, tx); err != nil {
			delete(v.dirParents, childInodeID)
			return nil, nil, err
		}

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

		parentInode, err := v.getOrLoadInodeLocked(ctx, parentInodeID)
		if err != nil {
			return nil, nil, err
		}
		if !parentInode.Row.GetIsDir() {
			return nil, nil, fmt.Errorf("parent inode %d is not a directory: %w", parentInodeID, syscall.ENOTDIR)
		}

		var existingChildIno uint64
		var exists bool
		de, ok, err := v.getDirEntrySQLiteLocked(ctx, parentInodeID, name)
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
			childInode, err := v.getOrLoadInodeLocked(ctx, existingChildIno)
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

			tx := v.metadataStream.Begin()
			prefixBytes, _ := sds.EncodeKeyPrefix(&pb.FileChunk{Ino: proto.Uint64(childInode.Row.GetIno())}, 1)
			chunkMsgs, _ := v.scanSQLiteRowsLocked(ctx, "objectfs.v1alpha1.FileChunk", prefixBytes)

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
			if err := v.applyTxChangesLocked(ctx, tx); err != nil {
				return nil, nil, err
			}

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

		childInodeID := v.allocInode()

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

		tx := v.metadataStream.Begin()

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
		if err := v.applyTxChangesLocked(ctx, tx); err != nil {
			return nil, nil, err
		}

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

		if err := v.checkBackpressureLocked(ctx); err != nil {
			return nil, nil, err
		}

		parentInodeID = v.normalizeInodeID(parentInodeID)
		if name == "" || name == "." || name == ".." {
			return nil, nil, fmt.Errorf("invalid symlink name %q: %w", name, syscall.EINVAL)
		}

		parentInode, err := v.getOrLoadInodeLocked(ctx, parentInodeID)
		if err != nil {
			return nil, nil, err
		}
		if !parentInode.Row.GetIsDir() {
			return nil, nil, fmt.Errorf("parent inode %d is not a directory: %w", parentInodeID, syscall.ENOTDIR)
		}

		_, ok, err := v.getDirEntrySQLiteLocked(ctx, parentInodeID, name)
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
		childInodeID := v.allocInode()
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

		tx := v.metadataStream.Begin()

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
		if err := v.applyTxChangesLocked(ctx, tx); err != nil {
			return nil, nil, err
		}

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
	node, err := v.getOrLoadInodeLocked(ctx, inodeID)
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

		if err := v.checkBackpressureLocked(ctx); err != nil {
			return nil, nil, err
		}

		oldInodeID = v.normalizeInodeID(oldInodeID)
		newParentInodeID = v.normalizeInodeID(newParentInodeID)

		if newName == "" || newName == "." || newName == ".." {
			return nil, nil, fmt.Errorf("invalid link name %q: %w", newName, syscall.EINVAL)
		}

		oldNode, err := v.getOrLoadInodeLocked(ctx, oldInodeID)
		if err != nil {
			return nil, nil, err
		}
		if oldNode.Row.GetIsDir() || (oldNode.Row.GetMode()&syscall.S_IFMT) == syscall.S_IFDIR {
			return nil, nil, syscall.EPERM // POSIX: directories cannot be hard-linked
		}

		newParentInode, err := v.getOrLoadInodeLocked(ctx, newParentInodeID)
		if err != nil {
			return nil, nil, err
		}
		if !newParentInode.Row.GetIsDir() {
			return nil, nil, fmt.Errorf("parent inode %d is not a directory: %w", newParentInodeID, syscall.ENOTDIR)
		}

		_, ok, err := v.getDirEntrySQLiteLocked(ctx, newParentInodeID, newName)
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

		tx := v.metadataStream.Begin()

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
		if err := v.applyTxChangesLocked(ctx, tx); err != nil {
			return nil, nil, err
		}

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
	node, err := v.getOrLoadInodeLocked(ctx, inodeID)
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
		_ = v.ensureInodeChunksLoadedLocked(ctx, node)
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
			chunkData, _ := v.readChunkLocked(ctx, node, i)
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
			if err == nil {
				node.Data = stream
			}
		}
	}

	if node.Data == nil {
		return make([]byte, readLen), total, "", nil
	}

	if _, err := node.Data.Seek(offset, io.SeekStart); err == nil {
		res := make([]byte, readLen)
		n, err := io.ReadFull(node.Data, res)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			return nil, 0, "", fmt.Errorf("failed to read node data: %w", err)
		}
		return res[:n], total, "", nil
	}

	return make([]byte, readLen), total, "", nil
}

func (v *Volume) WriteFile(ctx context.Context, inodeID uint64, offset int64, data []byte, writeMode pb.WriteMode) (int64, int64, time.Time, error) {
	nWritten, newSize, modTime, waitFn, err := func() (int64, int64, time.Time, func(context.Context) error, error) {
		v.mu.Lock()
		defer v.mu.Unlock()

		if err := v.checkBackpressureLocked(ctx); err != nil {
			return 0, 0, time.Time{}, nil, err
		}

		if inodeID == 0 {
			inodeID = v.rootInodeID
		}

		node, err := v.getOrLoadInodeLocked(ctx, inodeID)
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
		node.IsDirty = true

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
					chunk0, _ := v.readChunkLocked(ctx, node, 0)
					newInline = make([]byte, neededLen)
					copy(newInline, chunk0)
					copy(newInline[offset:], data)
				}
			} else {
				newInline = make([]byte, neededLen)
				copy(newInline, node.InlineData)
				copy(newInline[offset:], data)
			}
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

			tx := v.metadataStream.Begin()
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
			if err := v.applyTxChangesLocked(ctx, tx); err != nil {
				return 0, 0, time.Time{}, nil, err
			}
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
		if len(node.InlineData) > 0 {
			if node.StagedChunks == nil {
				node.StagedChunks = make(map[int][]byte)
			}
			node.StagedChunks[0] = node.InlineData
			cSha := fmt.Sprintf("%x", sha256.Sum256(node.InlineData))
			node.Chunks = map[uint32]string{0: cSha}
			node.InlineData = nil
		}
		if node.Chunks == nil {
			node.Chunks = make(map[uint32]string)
		}
		if node.StagedChunks == nil {
			node.StagedChunks = make(map[int][]byte)
		}
		_ = v.ensureInodeChunksLoadedLocked(ctx, node)

		cs := int64(effectiveChunkSize)
		startChunk := int(offset / cs)
		endChunk := int((offset + int64(len(data)) - 1) / cs)

		touchedIndices := make([]int, 0, endChunk-startChunk+1)
		chunkBlobs := make(map[int][]byte)
		chunkShas := make(map[int]string)

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

			chunkData, _ := v.readChunkLocked(ctx, node, i)
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
			node.Chunks[uint32(i)] = chunkSha
			node.StagedChunks[i] = chunkData

			touchedIndices = append(touchedIndices, i)
			cCopy := make([]byte, len(chunkData))
			copy(cCopy, chunkData)
			chunkBlobs[i] = cCopy
			chunkShas[i] = chunkSha
		}

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

		tx := v.metadataStream.Begin()
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
		if err := v.applyTxChangesLocked(ctx, tx); err != nil {
			return 0, 0, time.Time{}, nil, err
		}

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

		if err := v.checkBackpressureLocked(ctx); err != nil {
			return nil, nil, err
		}

		inodeID = v.normalizeInodeID(inodeID)

		node, err := v.getOrLoadInodeLocked(ctx, inodeID)
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
		node.IsDirty = true
		_ = v.ensureInodeChunksLoadedLocked(ctx, node)
		oldChunks := node.Chunks

		tx := v.metadataStream.Begin()

		if size == 0 {
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
			chunk0, _ := v.readChunkLocked(ctx, node, 0)
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

			if newNumChunks > 0 && size < node.Row.GetSize() {
				lastIdx := newNumChunks - 1
				lastChunkLen := size - int64(lastIdx)*cs
				if chunkData, ok := node.StagedChunks[lastIdx]; ok && int64(len(chunkData)) > lastChunkLen {
					chunkData = chunkData[:lastChunkLen]
					chunkSha := fmt.Sprintf("%x", sha256.Sum256(chunkData))
					node.Chunks[uint32(lastIdx)] = chunkSha
					node.StagedChunks[lastIdx] = chunkData
					if v.blobStore != nil {
						_ = v.blobStore.PutBlobs(ctx, map[string]blob.ByteStream{
							chunkSha: blob.NewByteStreamFromBytes(chunkData),
						})
					}
					if _, err := tx.Insert(ctx, &pb.FileChunk{
						Ino:    proto.Uint64(node.Row.GetIno()),
						Index:  proto.Uint32(uint32(lastIdx)),
						Sha256: chunkSha,
					}); err != nil {
						return nil, nil, fmt.Errorf("failed to log truncated chunk: %w", err)
					}
				} else if _, exists := node.Chunks[uint32(lastIdx)]; exists {
					chunkData, _ := v.readChunkLocked(ctx, node, lastIdx)
					if int64(len(chunkData)) > lastChunkLen {
						chunkData = chunkData[:lastChunkLen]
						chunkSha := fmt.Sprintf("%x", sha256.Sum256(chunkData))
						node.Chunks[uint32(lastIdx)] = chunkSha
						if node.StagedChunks == nil {
							node.StagedChunks = make(map[int][]byte)
						}
						node.StagedChunks[lastIdx] = chunkData
						if v.blobStore != nil {
							_ = v.blobStore.PutBlobs(ctx, map[string]blob.ByteStream{
								chunkSha: blob.NewByteStreamFromBytes(chunkData),
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
		if err := v.applyTxChangesLocked(ctx, tx); err != nil {
			return nil, nil, err
		}

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
	de, ok, err := v.getDirEntrySQLiteLocked(ctx, parentInodeID, name)
	if err != nil {
		v.mu.Unlock()
		return err
	}
	if !ok {
		v.mu.Unlock()
		return fmt.Errorf("file %s not found under inode %d: %w", name, parentInodeID, syscall.ENOENT)
	}
	childInode, err := v.getOrLoadInodeLocked(ctx, de.GetIno())
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

		if err := v.checkBackpressureLocked(ctx); err != nil {
			return nil, err
		}

		childInode, err := v.getOrLoadInodeLocked(ctx, childInodeID)
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

		parentInode, _ := v.getOrLoadInodeLocked(ctx, parentInodeID)
		if parentInode != nil {
			parentInode.mutate(func(row *pb.Inode) {
				row.Mtime = timestamppb.New(now)
				row.Ctime = timestamppb.New(now)
			})
		}

		var commitSeq uint64
		tx := v.metadataStream.Begin()
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
			chunkPrefix, err := sds.EncodeKeyPrefix(&pb.FileChunk{Ino: proto.Uint64(childInodeID)}, 1)
			if err != nil {
				return nil, fmt.Errorf("failed to encode chunk key prefix for inode %d: %w", childInodeID, err)
			}
			chunkMsgs, err := v.scanSQLiteRowsLocked(ctx, "objectfs.v1alpha1.FileChunk", chunkPrefix)
			if err != nil {
				return nil, fmt.Errorf("failed to scan chunk rows for inode %d: %w", childInodeID, err)
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
		if err := v.applyTxChangesLocked(ctx, tx); err != nil {
			return nil, err
		}

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

		if err := v.checkBackpressureLocked(ctx); err != nil {
			return nil, err
		}

		parentInodeID = v.normalizeInodeID(parentInodeID)

		var childInodeID uint64
		de, ok, err := v.getDirEntrySQLiteLocked(ctx, parentInodeID, name)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("directory %s not found under inode %d: %w", name, parentInodeID, syscall.ENOENT)
		}
		childInodeID = de.GetIno()
		childInode, err := v.getOrLoadInodeLocked(ctx, childInodeID)
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
		subEntries, err := v.scanLimitSQLiteRowsLocked(ctx, "objectfs.v1alpha1.DirEntry", prefixBytes, 1)
		if err != nil {
			return nil, err
		}
		if len(subEntries) > 0 {
			return nil, fmt.Errorf("directory %s not empty: %w", name, syscall.ENOTEMPTY)
		}

		delete(v.dirParents, childInodeID)

		now := time.Now()
		parentInode, _ := v.getOrLoadInodeLocked(ctx, parentInodeID)
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
		tx := v.metadataStream.Begin()
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
		if err = v.applyTxChangesLocked(ctx, tx); err != nil {
			return nil, err
		}

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

		oldDe, ok, err := v.getDirEntrySQLiteLocked(ctx, oldParentInodeID, oldName)
		if err != nil {
			return nil, nil, err
		}
		if !ok {
			return nil, nil, fmt.Errorf("source %s not found in parent %d: %w", oldName, oldParentInodeID, syscall.ENOENT)
		}

		newParentInode, err := v.getOrLoadInodeLocked(ctx, newParentInodeID)
		if err != nil {
			return nil, nil, err
		}
		if !newParentInode.Row.GetIsDir() {
			return nil, nil, fmt.Errorf("target parent %d is not a directory: %w", newParentInodeID, syscall.ENOTDIR)
		}

		targetDe, ok, err := v.getDirEntrySQLiteLocked(ctx, newParentInodeID, newName)
		if err != nil {
			return nil, nil, err
		}
		if ok {
			targetExists = true
			targetInodeID = targetDe.GetIno()
		}

		childInode, err := v.getOrLoadInodeLocked(ctx, oldDe.GetIno())
		if err != nil {
			return nil, nil, err
		}

		oldParentInode, _ := v.getOrLoadInodeLocked(ctx, oldParentInodeID)

		var targetInode *CachedInode
		if targetExists {
			targetInode, err = v.getOrLoadInodeLocked(ctx, targetInodeID)
			if err != nil {
				return nil, nil, err
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
				subEntries, sErr := v.scanLimitSQLiteRowsLocked(ctx, "objectfs.v1alpha1.DirEntry", prefixBytes, 1)
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
		tx := v.metadataStream.Begin()
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
						chunkPrefix, err := sds.EncodeKeyPrefix(&pb.FileChunk{Ino: proto.Uint64(targetInodeID)}, 1)
						if err != nil {
							return nil, nil, fmt.Errorf("failed to encode chunk key prefix for target inode %d: %w", targetInodeID, err)
						}
						chunkMsgs, err := v.scanSQLiteRowsLocked(ctx, "objectfs.v1alpha1.FileChunk", chunkPrefix)
						if err != nil {
							return nil, nil, fmt.Errorf("failed to scan chunk rows for target inode %d: %w", targetInodeID, err)
						}
						for _, cMsg := range chunkMsgs {
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
		if err := v.applyTxChangesLocked(ctx, tx); err != nil {
			return nil, nil, err
		}

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
	_, err := v.getOrLoadInodeLocked(ctx, inodeID)
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
	node, err := v.getOrLoadInodeLocked(ctx, inodeID)
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

	node, err := v.getOrLoadInodeLocked(ctx, inodeID)
	if err != nil || node == nil {
		return nil
	}

	if !node.Row.GetIsDir() && node.Row.GetNlink() == 0 {
		tx := v.metadataStream.Begin()
		if _, err := tx.Delete(ctx, &pb.Inode{Ino: proto.Uint64(inodeID)}); err != nil {
			return fmt.Errorf("failed to log orphan inode deletion: %w", err)
		}
		chunkPrefix, err := sds.EncodeKeyPrefix(&pb.FileChunk{Ino: proto.Uint64(inodeID)}, 1)
		if err != nil {
			return fmt.Errorf("failed to encode chunk key prefix for inode %d: %w", inodeID, err)
		}
		chunkMsgs, err := v.scanSQLiteRowsLocked(ctx, "objectfs.v1alpha1.FileChunk", chunkPrefix)
		if err != nil {
			return fmt.Errorf("failed to scan chunk rows for inode %d: %w", inodeID, err)
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
		if err := v.applyTxChangesLocked(ctx, tx); err != nil {
			return fmt.Errorf("failed to apply orphan deletion changes: %w", err)
		}
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

func inodeToErofsNode(row *pb.Inode, name string, inlineData []byte, children []erofs.Node, rootXattrs *erofs.Xattrs, isRoot bool) erofs.Node {
	var xattrs erofs.Xattrs
	if row.GetContentSha256() != "" {
		xattrs.UserDigest = row.GetContentSha256()
	} else if row.GetSha256() != "" {
		xattrs.UserDigest = row.GetSha256()
	}
	if row.GetManifestSha256() != "" {
		xattrs.UserManifest = row.GetManifestSha256()
	}

	var mtimeUnix uint64
	if row.GetMtime() != nil {
		mtimeUnix = uint64(row.GetMtime().AsTime().Unix())
	}

	if row.GetIsDir() {
		dirNodeName := name
		if dirNodeName == "/" || isRoot {
			dirNodeName = ""
		}
		var dirOpts []erofs.MemoryNodeOption
		dirOpts = append(dirOpts,
			erofs.WithIno(row.GetIno()),
			erofs.WithMtime(mtimeUnix),
			erofs.WithUID(row.GetUid()),
			erofs.WithGID(row.GetGid()),
		)
		if rootXattrs != nil && isRoot {
			dirOpts = append(dirOpts, erofs.WithXattrs(*rootXattrs))
		}
		return erofs.NewMemoryNode(
			dirNodeName,
			true,
			uint16(row.GetMode()),
			nil,
			children,
			dirOpts...,
		)
	}

	if row.GetSymlinkTarget() != "" || (row.GetMode()&syscall.S_IFMT) == syscall.S_IFLNK {
		return erofs.NewMemoryNode(
			name,
			false,
			uint16(row.GetMode()),
			[]byte(row.GetSymlinkTarget()),
			nil,
			erofs.WithIno(row.GetIno()),
			erofs.WithSize(uint64(len(row.GetSymlinkTarget()))),
			erofs.WithMtime(mtimeUnix),
			erofs.WithUID(row.GetUid()),
			erofs.WithGID(row.GetGid()),
			erofs.WithXattrs(xattrs),
		)
	}

	if len(inlineData) > 0 {
		return erofs.NewMemoryNode(
			name,
			false,
			uint16(row.GetMode()),
			inlineData,
			nil,
			erofs.WithIno(row.GetIno()),
			erofs.WithSize(uint64(row.GetSize())),
			erofs.WithMtime(mtimeUnix),
			erofs.WithUID(row.GetUid()),
			erofs.WithGID(row.GetGid()),
			erofs.WithRdev(row.GetRdev()),
			erofs.WithXattrs(xattrs),
		)
	}

	return erofs.NewMemoryNode(
		name,
		false,
		uint16(row.GetMode()),
		nil,
		nil,
		erofs.WithIno(row.GetIno()),
		erofs.WithMetadataOnly(true),
		erofs.WithSize(uint64(row.GetSize())),
		erofs.WithMtime(mtimeUnix),
		erofs.WithUID(row.GetUid()),
		erofs.WithGID(row.GetGid()),
		erofs.WithRdev(row.GetRdev()),
		erofs.WithXattrs(xattrs),
	)
}

type snapshotResolver struct {
	vol        *Volume
	rootXattrs *erofs.Xattrs
}

func (r *snapshotResolver) getOrLoadInode(ctx context.Context, inodeID uint64) (*CachedInode, error) {
	if r.vol != nil {
		return r.vol.getOrLoadInodeLocked(ctx, inodeID)
	}
	return nil, fmt.Errorf("no volume attached")
}

func (r *snapshotResolver) buildErofsTree(ctx context.Context, dirInodeID uint64, dirName, currentPath string, dirtyBlobs map[string]blob.ByteStream) (erofs.Node, error) {
	dirInode, err := r.getOrLoadInode(ctx, dirInodeID)
	if err != nil {
		return nil, err
	}

	prefixBytes, pErr := sds.EncodeKeyPrefix(&pb.DirEntry{ParentIno: proto.Uint64(dirInodeID)}, 1)
	if pErr != nil {
		return nil, pErr
	}
	entryMsgs, err := r.vol.scanSQLiteRowsLocked(ctx, "objectfs.v1alpha1.DirEntry", prefixBytes)
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

	var children []erofs.Node
	for _, name := range names {
		de := entriesMap[name]
		childPath := path.Join(currentPath, name)
		if de.GetIsDir() {
			childDirNode, err := r.buildErofsTree(ctx, de.GetIno(), name, childPath, dirtyBlobs)
			if err != nil {
				return nil, err
			}
			children = append(children, childDirNode)
		} else {
			childInode, err := r.getOrLoadInode(ctx, de.GetIno())
			if err != nil {
				return nil, err
			}

			if childInode.Row.GetChunkSize() > 0 && len(childInode.Chunks) > 0 {
				_ = r.vol.ensureInodeChunksLoadedLocked(ctx, childInode)
				cs := int64(childInode.Row.GetChunkSize())
				numChunks := int((childInode.Row.GetSize() + cs - 1) / cs)
				manifestChunks := make([][32]byte, numChunks)
				for i := 0; i < numChunks; i++ {
					if c, ok := childInode.Chunks[uint32(i)]; ok && c != "" {
						raw, _ := hex.DecodeString(c)
						if len(raw) == 32 {
							copy(manifestChunks[i][:], raw)
						}
					}
				}
				manifest := &blob.Manifest{
					ChunkSize:   childInode.Row.GetChunkSize(),
					TotalLength: uint64(childInode.Row.GetSize()),
					Chunks:      manifestChunks,
				}
				manifestBlob, err := blob.EncodeManifest(manifest)
				if err == nil {
					childInode.mutate(func(row *pb.Inode) {
						row.ManifestSha256 = manifestBlob.SHA256Hex()
					})
					dirtyBlobs[manifestBlob.SHA256Hex()] = manifestBlob.Stream
				}
			}

			if childInode.Row.GetContentSha256() == "" && childInode.Row.GetSize() > 0 {
				hasher := sha256.New()
				if childInode.Row.GetManifestSha256() != "" || childInode.Row.GetChunkSize() > 0 {
					_ = r.vol.ensureInodeChunksLoadedLocked(ctx, childInode)
					cs := int64(childInode.Row.GetChunkSize())
					if cs == 0 {
						cs = 64 * 1024
					}
					numChunks := int((childInode.Row.GetSize() + cs - 1) / cs)
					for i := 0; i < numChunks; i++ {
						chunkBytes, err := r.vol.readChunkLocked(ctx, childInode, i)
						if err == nil && len(chunkBytes) > 0 {
							hasher.Write(chunkBytes)
						} else {
							chunkLen := cs
							if int64(i+1)*cs > childInode.Row.GetSize() {
								chunkLen = childInode.Row.GetSize() - int64(i)*cs
							}
							if chunkLen > 0 {
								hasher.Write(make([]byte, chunkLen))
							}
						}
					}
				} else if childInode.Data != nil {
					_ = childInode.Data.Rewind()
					_, _ = io.Copy(hasher, childInode.Data)
				} else if childInode.Row.GetSha256() != "" && r.vol.blobStore != nil {
					stream, err := r.vol.blobStore.GetBlob(ctx, childInode.Row.GetSha256())
					if err == nil {
						_, _ = io.Copy(hasher, stream)
						_ = stream.Close()
					}
				}
				contentSha := fmt.Sprintf("%x", hasher.Sum(nil))
				childInode.mutate(func(row *pb.Inode) {
					row.ContentSha256 = contentSha
				})
				if r.vol != nil {
					r.vol.mu.Lock()
					if r.vol.metadataView != nil {
						nodeMsg := childInode.Row
						kBytes, vBytes, _ := sds.SplitKeyAndNonKey(nodeMsg, []int32{1})
						ch := sds.Change{
							TypeID:   16,
							TypeName: "objectfs.v1alpha1.Inode",
							Op:       sds.OpUpdate,
							Key:      sds.NewKeyFromBytes(kBytes),
							RawKey:   kBytes,
							RawVal:   vBytes,
							Row:      nodeMsg,
						}
						_ = r.vol.metadataView.ApplyChangesSync(ctx, []sds.Change{ch})
					}
					r.vol.mu.Unlock()
				}
			}

			// Collect dirty / unpersisted blobs for flush
			if len(childInode.InlineData) > 0 {
				childInode.mutate(func(row *pb.Inode) {
					if row.GetContentSha256() == "" {
						row.ContentSha256 = fmt.Sprintf("%x", sha256.Sum256(childInode.InlineData))
					}
					if row.GetSha256() == "" {
						row.Sha256 = row.GetContentSha256()
					}
				})
				dirtyBlobs[childInode.Row.GetSha256()] = blob.NewByteStreamFromBytes(childInode.InlineData)
			}
			if childInode.Row.GetSha256() != "" {
				if cData, ok := r.vol.recoveredContent[childInode.Row.GetSha256()]; ok {
					dirtyBlobs[childInode.Row.GetSha256()] = blob.NewByteStreamFromBytes(cData)
				} else if childInode.Data != nil {
					_ = childInode.Data.Rewind()
					dataBytes, _ := io.ReadAll(childInode.Data)
					_ = childInode.Data.Rewind()
					dirtyBlobs[childInode.Row.GetSha256()] = blob.NewByteStreamFromBytes(dataBytes)
				}
			}
			if childInode.Row.GetManifestSha256() != "" {
				if mData, ok := r.vol.recoveredContent[childInode.Row.GetManifestSha256()]; ok {
					dirtyBlobs[childInode.Row.GetManifestSha256()] = blob.NewByteStreamFromBytes(mData)
				}
			}
			for _, chunkSha := range childInode.Chunks {
				if chunkData, ok := r.vol.recoveredContent[chunkSha]; ok {
					dirtyBlobs[chunkSha] = blob.NewByteStreamFromBytes(chunkData)
				}
			}
			for i, cBytes := range childInode.StagedChunks {
				if sha, ok := childInode.Chunks[uint32(i)]; ok && sha != "" {
					dirtyBlobs[sha] = blob.NewByteStreamFromBytes(cBytes)
				}
			}
			for i, cBytes := range childInode.DirtyChunks {
				if sha, ok := childInode.Chunks[uint32(i)]; ok && sha != "" {
					dirtyBlobs[sha] = blob.NewByteStreamFromBytes(cBytes)
				}
			}

			leafNode := inodeToErofsNode(childInode.Row, name, childInode.InlineData, nil, nil, false)
			children = append(children, leafNode)
		}
	}

	return inodeToErofsNode(dirInode.Row, dirName, nil, children, r.rootXattrs, currentPath == "/"), nil
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

	snapPos := v.safeSnapshotPositionLocked()

	if snap, ok := v.metadataView.Index().(sds.Snapshotter); ok && v.backend != nil {
		if err := v.flushOverlayLocked(ctx); err != nil {
			return err
		}
		snapKey, actualPos, err := snap.PublishSnapshot(ctx, v.backend)
		if err == nil && v.metadataStream != nil && snapKey != "" {
			format := ""
			if v.indexFactory != nil {
				format = v.indexFactory.Format()
			}
			ptr := &sdsv1.SnapshotPointer{
				Position: actualPos,
				Format:   format,
				Location: snapKey,
			}
			_, _ = v.metadataStream.AppendSnapshotPointer(ctx, ptr)
		}
	}

	var regBytes []byte
	if v.metadataStream != nil {
		regProto := v.metadataStream.Registry().Export()
		var err error
		regBytes, err = proto.Marshal(regProto)
		if err != nil {
			return fmt.Errorf("failed to marshal registry: %w", err)
		}
	}
	snapTime := time.Now().UTC()
	rootXattrs := erofs.Xattrs{
		Others: map[string]string{
			"trusted.sds.position":      strconv.FormatUint(snapPos, 10),
			"trusted.sds.registry":      string(regBytes),
			"trusted.sds.snapshot_time": snapTime.Format(time.RFC3339Nano),
		},
	}

	// Release v.mu during snapshot compilation and cloud storage upload to avoid stopping the world
	v.mu.Unlock()

	var erofsBuf bufferWriterAt
	snapshotName := fmt.Sprintf("%020d.erofs", snapPos)
	snapshotKey := path.Join("volumes", v.volumeID, "meta", snapshotName)

	snapErr := func() error {
		dirtyBlobs := make(map[string]blob.ByteStream)

		resolver := &snapshotResolver{
			vol:        v,
			rootXattrs: &rootXattrs,
		}

		erofsTree, err := resolver.buildErofsTree(ctx, v.rootInodeID, "/", "/", dirtyBlobs)
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

		erofsStream := blob.NewByteStreamFromBytes(erofsBuf.buf)
		if _, err := v.backend.PutObject(ctx, "", snapshotKey, erofsStream); err != nil {
			_ = erofsStream.Close()
			return fmt.Errorf("failed to save EROFS snapshot %s: %w", snapshotKey, err)
		}
		_ = erofsStream.Close()

		if v.metadataStream != nil {
			regFP := sha256.Sum256(regBytes)
			ptr := &sdsv1.SnapshotPointer{
				Position:            snapPos,
				Format:              "erofs",
				Location:            snapshotKey,
				RegistryFingerprint: regFP[:],
			}
			if _, err := v.metadataStream.AppendSnapshotPointer(ctx, ptr); err != nil {
				return fmt.Errorf("failed to append SnapshotPointer: %w", err)
			}
		}

		return nil
	}()

	// Re-acquire v.mu.Lock()
	v.mu.Lock()

	if snapErr != nil {
		return snapErr
	}

	v.snapshotRaw = bytes.NewReader(erofsBuf.buf)
	if r, err := erofs.NewReader(v.snapshotRaw); err == nil {
		v.snapshotReader = r
		v.rootInodeID = r.GetRootNID()
	}
	v.recoveredContent = make(map[string][]byte)

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

func (v *Volume) findLatestSnapshotNameLocked(ctx context.Context) (string, error) {
	metaPrefix := path.Join("volumes", v.volumeID, "meta") + "/"
	objects, err := v.backend.ListObjects(ctx, "", metaPrefix)
	if err != nil {
		return "", err
	}

	type snapItem struct {
		name string
		pos  uint64
	}
	var snapshots []snapItem
	for _, obj := range objects {
		if strings.HasSuffix(obj, ".erofs") {
			base := path.Base(obj)
			trimmed := strings.TrimSuffix(base, ".erofs")
			pos, _ := strconv.ParseUint(trimmed, 10, 64)
			snapshots = append(snapshots, snapItem{name: base, pos: pos})
		}
	}
	if len(snapshots) == 0 {
		return "", fmt.Errorf("no snapshots found for volume %s", v.volumeID)
	}
	sort.Slice(snapshots, func(i, j int) bool {
		if snapshots[i].pos != snapshots[j].pos {
			return snapshots[i].pos < snapshots[j].pos
		}
		return snapshots[i].name < snapshots[j].name
	})
	return snapshots[len(snapshots)-1].name, nil
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

func (v *Volume) reseedLiveStatsFromDurableLocked() {
	if v.metadataView == nil {
		return
	}
	if m := v.metadataView.Stats(); m != nil {
		if vs, ok := m.(*pb.VolumeStats); ok && vs != nil {
			v.liveStats = proto.Clone(vs).(*pb.VolumeStats)
			v.liveStatsStale = false
			return
		}
	}
	v.liveStats = &pb.VolumeStats{Name: VolumeStatsRowName}
	v.liveStatsStale = false
}

// checkStatsOnBatchApplied is called by the view's background applier outside the view lock
// when a batch is applied. In debug mode, it starts a goroutine per applied batch to check
// stats consistency without blocking the applier or deadlocking with callers holding v.mu;
// this is intended and safe for test and debug verification.
// When the applier catches up to stream head, it also re-seeds liveStats if marked stale.
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
			if v.liveStatsStale {
				v.reseedLiveStatsFromDurableLocked()
			}
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

func (v *Volume) importErofsToLocalIndexLocked(ctx context.Context, reader *erofs.Reader, snapPos uint64) error {
	if v.metadataView == nil || reader == nil {
		return nil
	}
	return v.importErofsToTargetIndex(ctx, v.metadataView.Index(), v.snapshotRaw, reader, snapPos)
}

func (v *Volume) importErofsToTargetIndex(ctx context.Context, target sds.LocalIndex, raw io.ReaderAt, reader *erofs.Reader, snapPos uint64) error {
	if target == nil || reader == nil {
		return nil
	}

	rootNID := reader.GetRootNID()
	var changes []sds.Change

	queue := []uint64{rootNID}
	visited := make(map[uint64]bool)

	for len(queue) > 0 {
		nid := queue[0]
		queue = queue[1:]
		if visited[nid] {
			continue
		}
		visited[nid] = true

		ino := nid
		if nid == rootNID || nid == 0 {
			ino = 1
		}

		rawSource := raw
		if rawSource == nil {
			rawSource = v.snapshotRaw
		}
		erofsInode, err := erofs.ReadInode(rawSource, reader.Superblock(), nid)
		if err != nil {
			return fmt.Errorf("ReadInode failed for nid %d: %w", nid, err)
		}

		isDir := (erofsInode.Mode & erofs.S_IFMT) == erofs.S_IFDIR
		isSymlink := (erofsInode.Mode & erofs.S_IFMT) == erofs.S_IFLNK
		mode := uint32(erofsInode.Mode)
		if isDir {
			mode |= syscall.S_IFDIR
		} else if isSymlink {
			mode |= syscall.S_IFLNK
		} else if (mode & syscall.S_IFMT) == 0 {
			mode |= syscall.S_IFREG
		}
		mtime := time.Unix(int64(erofsInode.Mtime), int64(erofsInode.MtimeNsec))
		if erofsInode.Mtime == 0 {
			mtime = time.Now()
		}

		var symlinkTarget string
		if isSymlink {
			if r, err := reader.ReadFileContent(nid); err == nil {
				data, _ := io.ReadAll(r)
				symlinkTarget = string(data)
			}
		}

		nlink := erofsInode.Nlink
		if nlink == 0 {
			if isDir {
				nlink = 2
			} else {
				nlink = 1
			}
		}

		var shaStr, manifestSha, contentSha string
		var chunkSize uint32
		xattrs, _ := reader.GetXattrs(nid)
		if !xattrs.IsEmpty() {
			if xattrs.UserDigest != "" {
				shaStr = xattrs.UserDigest
				contentSha = xattrs.UserDigest
			} else if xattrs.UserSHA256 != "" {
				shaStr = xattrs.UserSHA256
				contentSha = xattrs.UserSHA256
			}
			if xattrs.UserManifest != "" {
				manifestSha = xattrs.UserManifest
				shaStr = manifestSha
			}
		}
		if manifestSha == "" && contentSha == "" {
			contentSha = shaStr
		}

		inoMsg := &pb.Inode{
			Ino:            proto.Uint64(ino),
			Mode:           mode,
			Size:           int64(erofsInode.Size),
			Mtime:          timestamppb.New(mtime),
			IsDir:          isDir,
			Sha256:         shaStr,
			ManifestSha256: manifestSha,
			ContentSha256:  contentSha,
			ChunkSize:      chunkSize,
			Uid:            erofsInode.UID,
			Gid:            erofsInode.GID,
			Nlink:          nlink,
			SymlinkTarget:  symlinkTarget,
			Rdev:           erofsInode.Rdev,
		}

		keyBytes, valBytes, _ := sds.SplitKeyAndNonKey(inoMsg, []int32{1})
		changes = append(changes, sds.Change{
			Seq:      snapPos,
			TypeID:   16,
			TypeName: "objectfs.v1alpha1.Inode",
			Op:       sds.OpCreate,
			Key:      sds.NewKeyFromBytes(keyBytes),
			RawKey:   keyBytes,
			RawVal:   valBytes,
			Row:      inoMsg,
		})

		if isDir {
			dirents, err := reader.ListDirectory(nid)
			if err == nil {
				for _, de := range dirents {
					if de.Name == "." || de.Name == ".." {
						continue
					}
					childName := de.Name
					childNID := de.NID
					childIno := de.NID
					if childNID == rootNID || childNID == 0 {
						childIno = 1
					}
					childIsDir := de.FileType == erofs.FTDir
					childMode := uint32(0644 | syscall.S_IFREG)
					if childIsDir {
						childMode = uint32(0755 | syscall.S_IFDIR)
					} else {
						switch de.FileType {
						case erofs.FTChrDev:
							childMode = uint32(0600 | syscall.S_IFCHR)
						case erofs.FTBlkDev:
							childMode = uint32(0600 | syscall.S_IFBLK)
						case erofs.FTFifo:
							childMode = uint32(0600 | syscall.S_IFIFO)
						case erofs.FTSock:
							childMode = uint32(0600 | syscall.S_IFSOCK)
						case erofs.FTSymlink:
							childMode = uint32(0777 | syscall.S_IFLNK)
						}
					}

					dirEntryMsg := &pb.DirEntry{
						ParentIno: proto.Uint64(ino),
						Name:      proto.String(childName),
						Ino:       childIno,
						IsDir:     childIsDir,
						Mode:      childMode,
					}
					deKeyBytes, deValBytes, _ := sds.SplitKeyAndNonKey(dirEntryMsg, []int32{1, 2})
					changes = append(changes, sds.Change{
						Seq:      snapPos,
						TypeID:   17,
						TypeName: "objectfs.v1alpha1.DirEntry",
						Op:       sds.OpCreate,
						Key:      sds.NewKeyFromBytes(deKeyBytes),
						RawKey:   deKeyBytes,
						RawVal:   deValBytes,
						Row:      dirEntryMsg,
					})
					if v.dirParents == nil {
						v.dirParents = make(map[uint64]uint64)
					}
					v.dirParents[childIno] = ino
					queue = append(queue, childNID)
				}
			}
		} else {
			if erofsInode.Size <= 4096 && erofsInode.Size > 0 {
				var data []byte
				if r, err := reader.ReadFileContent(nid); err == nil {
					data, _ = io.ReadAll(r)
				}
				if (len(data) == 0 || bytes.Equal(data, make([]byte, len(data)))) && inoMsg.GetContentSha256() != "" && v.blobStore != nil {
					if bs, err := v.blobStore.GetBlob(ctx, inoMsg.GetContentSha256()); err == nil {
						data, _ = io.ReadAll(bs)
						_ = bs.Close()
					}
				}
				if len(data) > 0 {
					chunkMsg := &pb.FileChunk{
						Ino:        proto.Uint64(ino),
						Index:      proto.Uint32(0),
						InlineData: data,
					}
					cKeyBytes, cValBytes, _ := sds.SplitKeyAndNonKey(chunkMsg, []int32{1, 2})
					changes = append(changes, sds.Change{
						Seq:      snapPos,
						TypeID:   18,
						TypeName: "objectfs.v1alpha1.FileChunk",
						Op:       sds.OpCreate,
						Key:      sds.NewKeyFromBytes(cKeyBytes),
						RawKey:   cKeyBytes,
						RawVal:   cValBytes,
						Row:      chunkMsg,
					})
				}
			}
		}
	}

	if len(changes) > 0 {
		if err := target.ApplyBatch(ctx, changes); err != nil {
			return fmt.Errorf("failed to import EROFS snapshot into metadata index: %w", err)
		}
	}
	v.lastCommitSeq = snapPos
	return nil
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

	// 4. If starting from scratch (snapPos == 0), initialize root inode or import EROFS snapshot
	if snapPos == 0 {
		var imported bool
		if v.backend != nil {
			v.mu.RLock()
			latestSnapshotName, sErr := v.findLatestSnapshotNameLocked(ctx)
			v.mu.RUnlock()
			if sErr == nil && latestSnapshotName != "" {
				snapshotKey := path.Join("volumes", v.volumeID, "meta", latestSnapshotName)
				var imgBuf bytes.Buffer
				if err := v.backend.GetObject(ctx, "", snapshotKey, 0, 0, &imgBuf); err == nil && imgBuf.Len() > 0 {
					readerAt := bytes.NewReader(imgBuf.Bytes())
					if reader, err := erofs.NewReader(readerAt); err == nil {
						pos := uint64(0)
						if rootXattrs, xErr := reader.GetXattrs(reader.GetRootNID()); xErr == nil {
							if posStr, ok := rootXattrs.Others["trusted.sds.position"]; ok && posStr != "" {
								if p, pErr := strconv.ParseUint(posStr, 10, 64); pErr == nil {
									pos = p
								}
							}
						}
						if pos == 0 {
							trimmed := strings.TrimSuffix(latestSnapshotName, ".erofs")
							if p, pErr := strconv.ParseUint(trimmed, 10, 64); pErr == nil {
								pos = p
							}
						}
						snapPos = pos
						if err := v.importErofsToTargetIndex(ctx, rebuiltIdx, readerAt, reader, snapPos); err == nil {
							imported = true
						}
					}
				}
			}
		}
		if !imported {
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
					_ = v.initMetadataViewLocked(ctx, localIdx)
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
			_ = v.initMetadataViewLocked(ctx, restoredIdx)
			snapPos = rPos
			initialized = true
		}
	}

	// 3. Otherwise, for volumes that only have EROFS snapshots (or initial startup)
	if !initialized && v.backend != nil {
		latestSnapshotName, err := v.findLatestSnapshotNameLocked(ctx)
		if err == nil && latestSnapshotName != "" {
			snapshotKey := path.Join("volumes", v.volumeID, "meta", latestSnapshotName)
			var imgBuf bytes.Buffer
			if err := v.backend.GetObject(ctx, "", snapshotKey, 0, 0, &imgBuf); err == nil && imgBuf.Len() > 0 {
				snapBytes := imgBuf.Bytes()
				readerAt := bytes.NewReader(snapBytes)
				if reader, err := erofs.NewReader(readerAt); err == nil {
					v.snapshotRaw = readerAt
					v.snapshotReader = reader
					pos := uint64(0)
					if rootXattrs, xErr := reader.GetXattrs(reader.GetRootNID()); xErr == nil {
						if posStr, ok := rootXattrs.Others["trusted.sds.position"]; ok && posStr != "" {
							if p, pErr := strconv.ParseUint(posStr, 10, 64); pErr == nil {
								pos = p
							}
						}
					}
					if pos == 0 {
						trimmed := strings.TrimSuffix(latestSnapshotName, ".erofs")
						if p, pErr := strconv.ParseUint(trimmed, 10, 64); pErr == nil {
							pos = p
						}
					}
					snapPos = pos
					if v.metadataView == nil {
						_ = v.initMetadataViewLocked(ctx, nil)
					}
					if err := v.importErofsToLocalIndexLocked(ctx, reader, snapPos); err != nil {
						return fmt.Errorf("importErofsToLocalIndexLocked failed: %w", err)
					}
					initialized = true
				}
			}
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
			_ = v.initMetadataViewLocked(ctx, nil)
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

// CreateSnapshot creates and returns a new EROFS snapshot of the current volume state.
func (v *Volume) CreateSnapshot(ctx context.Context) (string, error) {
	if err := v.FlushToBackend(ctx); err != nil {
		return "", err
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.findLatestSnapshotNameLocked(ctx)
}

// ListSnapshots returns all EROFS snapshot filenames for this volume sorted chronologically / by position.
func (v *Volume) ListSnapshots(ctx context.Context) ([]string, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	metaPrefix := path.Join("volumes", v.volumeID, "meta") + "/"
	objects, err := v.backend.ListObjects(ctx, "", metaPrefix)
	if err != nil {
		return nil, err
	}

	type snapItem struct {
		name string
		pos  uint64
	}
	var snapshots []snapItem
	for _, obj := range objects {
		if strings.HasSuffix(obj, ".erofs") {
			base := path.Base(obj)
			trimmed := strings.TrimSuffix(base, ".erofs")
			pos, _ := strconv.ParseUint(trimmed, 10, 64)
			snapshots = append(snapshots, snapItem{name: base, pos: pos})
		}
	}
	sort.Slice(snapshots, func(i, j int) bool {
		if snapshots[i].pos != snapshots[j].pos {
			return snapshots[i].pos < snapshots[j].pos
		}
		return snapshots[i].name < snapshots[j].name
	})

	var res []string
	for _, s := range snapshots {
		res = append(res, s.name)
	}
	return res, nil
}

// GetSnapshotInfo returns metadata (position, created_at timestamp, size) for a snapshot.
func (v *Volume) GetSnapshotInfo(ctx context.Context, snapshotName string) (*pb.SnapshotInfo, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.getSnapshotInfoLocked(ctx, snapshotName)
}

func (v *Volume) getSnapshotInfoLocked(ctx context.Context, snapshotName string) (*pb.SnapshotInfo, error) {
	if v.backend == nil {
		return nil, fmt.Errorf("no backend configured")
	}

	trimmed := strings.TrimSuffix(snapshotName, ".erofs")
	pos, _ := strconv.ParseUint(trimmed, 10, 64)

	snapshotKey := path.Join("volumes", v.volumeID, "meta", snapshotName)
	var imgBuf bytes.Buffer
	if err := v.backend.GetObject(ctx, "", snapshotKey, 0, 0, &imgBuf); err != nil {
		return nil, fmt.Errorf("failed to get snapshot %s: %w", snapshotKey, err)
	}

	snapBytes := imgBuf.Bytes()
	info := &pb.SnapshotInfo{
		Name:     snapshotName,
		Position: pos,
		Size:     int64(len(snapBytes)),
	}

	readerAt := bytes.NewReader(snapBytes)
	if reader, err := erofs.NewReader(readerAt); err == nil {
		if rootXattrs, xErr := reader.GetXattrs(reader.GetRootNID()); xErr == nil {
			if posStr, ok := rootXattrs.Others["trusted.sds.position"]; ok && posStr != "" {
				if p, pErr := strconv.ParseUint(posStr, 10, 64); pErr == nil {
					info.Position = p
				}
			}
			if timeStr, ok := rootXattrs.Others["trusted.sds.snapshot_time"]; ok && timeStr != "" {
				if t, tErr := time.Parse(time.RFC3339Nano, timeStr); tErr == nil {
					info.CreatedAt = timestamppb.New(t)
				} else if t, tErr := time.Parse(time.RFC3339, timeStr); tErr == nil {
					info.CreatedAt = timestamppb.New(t)
				}
			}
		}
	}

	return info, nil
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
	v.rootInodeID = 1

	var snapPos uint64
	if rootXattrs, xErr := reader.GetXattrs(reader.GetRootNID()); xErr == nil {
		if posStr, ok := rootXattrs.Others["trusted.sds.position"]; ok && posStr != "" {
			if p, pErr := strconv.ParseUint(posStr, 10, 64); pErr == nil {
				snapPos = p
			}
		}
		if regStr, ok := rootXattrs.Others["trusted.sds.registry"]; ok && regStr != "" {
			var reg sdsv1.Registry
			if uErr := proto.Unmarshal([]byte(regStr), &reg); uErr == nil {
				if v.metadataStream != nil {
					_ = v.metadataStream.Registry().Import(&reg)
				}
			}
		}
	}
	if snapPos == 0 {
		trimmed := strings.TrimSuffix(snapshotName, ".erofs")
		if p, pErr := strconv.ParseUint(trimmed, 10, 64); pErr == nil {
			snapPos = p
		}
	}
	v.lastCommitSeq = snapPos

	// Reset index for restored snapshot
	if v.indexFactory == nil {
		f, err := sds.GetIndexFactory("sqlite")
		if err != nil {
			return err
		}
		v.indexFactory = f
	}
	_ = v.initMetadataViewLocked(ctx, nil)

	if err := v.importErofsToLocalIndexLocked(ctx, reader, snapPos); err != nil {
		return fmt.Errorf("failed to import restored EROFS snapshot into metadata index: %w", err)
	}

	if v.metadataView != nil {
		v.metadataView.ClearCache()
	}
	v.initLiveStatsLocked()
	if err := v.initNextInodeFromStatsLocked(ctx, 0); err != nil {
		return fmt.Errorf("failed to initialize next inode from stats: %w", err)
	}
	v.assertStatsMatchLocked("after RestoreSnapshot")

	return nil
}
