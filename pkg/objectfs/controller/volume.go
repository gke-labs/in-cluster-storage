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
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
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
	"github.com/gke-labs/in-cluster-storage/pkg/errutil"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/blob"
	"github.com/gke-labs/in-cluster-storage/pkg/scoped"
	"github.com/gke-labs/in-cluster-storage/pkg/sds"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/memtable"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/projection/sqlite"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/projection/table"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/record"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/view"
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/klog/v2"
)

var (
	// ErrNewestSnapshot indicates that a snapshot cannot be deleted because recovery requires the newest snapshot.
	ErrNewestSnapshot = errors.New("cannot delete newest snapshot")
	// ErrSnapshotInUse indicates that a snapshot cannot be deleted because the live stream still depends on it.
	ErrSnapshotInUse = errors.New("cannot delete snapshot: live stream depends on it")
)

const (
	spanVolumeSync           = "objectfs.volume.sync"
	spanVolumeSyncSeal       = "objectfs.volume.sync.seal"
	spanVolumeSyncUpload     = "objectfs.volume.sync.upload"
	spanVolumeSyncAppend     = "objectfs.volume.sync.append"
	spanVolumeSyncCommitWait = "objectfs.volume.sync.commit_wait"

	attributeVolume      = "objectfs.volume"
	attributeSyncTrigger = "objectfs.sync.trigger"
	// attributeSyncBytes measures the bytes of the packs this sync made durable.
	// With upload-at-seal, most packs in a sync's set were uploaded by the uploader
	// before the sync ran, so this reflects total durable pack bytes rather than bytes
	// transferred strictly during this sync invocation.
	attributeSyncBytes   = "objectfs.sync.bytes"
	attributeSyncRecords = "objectfs.sync.records"
	attributeSyncPacks   = "objectfs.sync.packs"

	defaultApplierBatchSize = 100

	// MaxNameLength is the maximum allowed byte length for a path component name
	// (matching the POSIX NAME_MAX limit of 255 bytes advertised by statfs).
	MaxNameLength = 255

	// MaxXAttrNameLength is the maximum allowed byte length for an extended attribute name (Linux XATTR_NAME_MAX)
	MaxXAttrNameLength = 255

	// MaxXAttrValueLength is the maximum allowed byte length for an extended attribute value (Linux XATTR_SIZE_MAX)
	MaxXAttrValueLength = 64 * 1024

	// closeCoalesceWindow is the short fixed floor window used to coalesce CLOSE-triggered
	// syncs. When multiple files are closed in rapid succession (e.g. untarring an archive or
	// compiler emit), this window batches their pack uploads and metadata commits into a
	// single packfile rather than creating one pack per closed file, avoiding write amplification
	// while bounding cross-node visibility latency for quiet files.
	closeCoalesceWindow = 75 * time.Millisecond
)

type syncResult struct {
	pos uint64
	err error
}

type syncWaiter struct {
	seq     uint64
	inodeID uint64
	ch      chan syncResult
}

// syncOperation describes the currently running in-flight sync operation.
type syncOperation struct {
	trigger       pb.FsyncRequest_SyncTrigger
	commitSeq     uint64
	startTime     time.Time
	syncingInodes map[uint64]struct{}
}

// stagedChunkReference references a chunk payload stored in an incremental v2 packfile.
type stagedChunkReference struct {
	PackID uint64
	Entry  blob.BlobEntry
	Length uint32
}

// packState represents the upload lifecycle of a staged packfile.
type packState int

const (
	packNotStarted packState = iota
	packUploading
	packUploaded
	packFailed
)

type packInfo struct {
	pack           *blob.IncrementalPack
	liveReferences int
	state          packState
	key            string
	uploadError    error
}

// syncCoalesceState groups the state used to coalesce non-blocking sync requests
// across a configurable time window, batching multiple writes and fsync calls into
// a single durable pack upload and metadata commit to reduce I/O overhead.
//
// All fields are guarded by Volume.stagingMu.
type syncCoalesceState struct {
	// timer triggers a background coalesce sync when the coalesce window expires,
	// or is nil if no timer is currently active.
	timer *time.Timer

	// waiters holds callers waiting for a staged sync sequence to commit,
	// including both volume-wide non-blocking syncs and close-triggered syncs
	// that are filtered and answered by inode ID.
	waiters []*syncWaiter

	// window is the duration to batch non-blocking sync requests before triggering
	// an asynchronous sync operation.
	window time.Duration
}

// armTimer arms the coalesce timer with trigger if it is not already armed.
// Must be called with Volume.stagingMu held.
func (c *syncCoalesceState) armTimer(trigger func()) {
	if c.timer == nil {
		c.timer = time.AfterFunc(c.window, trigger)
	}
}

// stopTimer stops the coalesce timer if armed and clears the timer reference.
// Must be called with Volume.stagingMu held.
func (c *syncCoalesceState) stopTimer() {
	if c.timer != nil {
		c.timer.Stop()
		c.timer = nil
	}
}

// addWaiter records a waiter waiting for a sync sequence to commit.
// Must be called with Volume.stagingMu held.
func (c *syncCoalesceState) addWaiter(sequence uint64, inodeID uint64, channel chan syncResult) {
	c.waiters = append(c.waiters, &syncWaiter{seq: sequence, inodeID: inodeID, ch: channel})
}

// answerWaiters completes all waiters whose sequence is at or before snapshotSeq
// and matching inodeFilter (or all if inodeFilter is nil), and retains remaining waiters.
// Must be called with Volume.stagingMu held.
func (c *syncCoalesceState) answerWaiters(snapshotSeq uint64, err error, pos uint64, inodeFilter map[uint64]struct{}) {
	var remainingWaiters []*syncWaiter
	for _, waiter := range c.waiters {
		shouldComplete := waiter.seq <= snapshotSeq
		if shouldComplete && inodeFilter != nil {
			_, shouldComplete = inodeFilter[waiter.inodeID]
		}

		if shouldComplete {
			waiter.ch <- syncResult{pos: pos, err: err}
			close(waiter.ch)
		} else {
			remainingWaiters = append(remainingWaiters, waiter)
		}
	}
	c.waiters = remainingWaiters
}

// failWaiters completes all pending waiters with err and clears the waiter list.
// Must be called with Volume.stagingMu held.
func (c *syncCoalesceState) failWaiters(err error) {
	for _, waiter := range c.waiters {
		waiter.ch <- syncResult{err: err}
		close(waiter.ch)
	}
	c.waiters = nil
}

type Volume struct {
	// Lock order: stagingMu may be held while taking mu for reading; mu must never be held while taking stagingMu.
	mu           sync.RWMutex
	volumeID     string
	rootInodeID  uint64
	nextInode    uint64
	backend      ObjectStorageBackend
	blobStore    *blob.Store
	broadcaster  *EventBroadcaster
	maxInlineLen int64
	chunkSize    uint32

	stagingDir      string
	activePack      *blob.IncrementalPack
	activePackID    uint64
	nextPackID      uint64
	packs           map[uint64]*packInfo
	slotSeals       int64
	uploadSemaphore chan struct{}
	uploadWaitGroup sync.WaitGroup
	uploadContext   context.Context
	cancelUpload    context.CancelFunc

	// Lock order: stagingMu may be held while taking mu for reading; mu must never be held while taking stagingMu.
	stagingMu       sync.Mutex
	stagingEpoch    int64
	stagingSeq      uint64
	stagedChunks    map[uint64]map[int]stagedChunkReference
	stagedSizes     map[uint64]int64
	stagedMtimes    map[uint64]time.Time
	stagedBytes     int64
	maxStagingBytes int64
	lastSyncError   map[uint64]error

	coalesce    syncCoalesceState
	closeTimer  *time.Timer
	closeInodes map[uint64]struct{}
	// currentSync tracks the single real sync operation currently in flight, or nil if idle.
	// At most one real sync is in flight at a time. Every path that starts one waits for
	// idle (waitForSyncIdleLocked) or skips if a sync is already running, all under stagingMu.
	// Empty syncs (which find no staged chunks) never install themselves and may run alongside
	// a real sync harmlessly; they only wait for durability and answer waiters. This is why
	// there is one pointer and not a list of operations.
	currentSync        *syncOperation
	syncTimeout        time.Duration
	openBarrierTimeout time.Duration
	consistencyMode    pb.ConsistencyMode
	syncCond           *sync.Cond
	stagingClosed      bool
	stopping           bool

	stream     walclient.Stream
	durability walclient.Level
	streamID   uuid.UUID

	metadataStream         *sds.Writer
	lastCommitSeq          uint64
	snapshotPointers       []*sdsv1.SnapshotPointer
	replayStartPos         uint64
	replaySnapshotLocation string
	liveStats              *pb.VolumeStats
	debugStatsCheck        *bool

	localStorageDir string
	indexFactory    sds.IndexFactory
	metadataView    *view.View
	viewOpts        []view.Option

	closed   bool
	closedCh chan struct{}

	snapshotMu       sync.Mutex
	batchCheckMu     sync.Mutex
	batchCheckCond   *sync.Cond
	batchCheckActive int

	dirParents map[uint64]uint64

	nextFh     atomic.Uint64
	handlesMu  sync.Mutex
	handles    map[uint64]uint64
	openInodes map[uint64]int

	acceptUnversioned bool
}

// VolumeOption configures a Volume instance.
type VolumeOption func(*Volume)

// WithAcceptUnversioned configures whether unversioned segments, streams, and snapshots are accepted.
func WithAcceptUnversioned(accept bool) VolumeOption {
	return func(v *Volume) {
		v.acceptUnversioned = accept
	}
}

// WithSyncCoalesceWindow sets the window duration for coalescing non-blocking sync requests.
func WithSyncCoalesceWindow(d time.Duration) VolumeOption {
	return func(v *Volume) {
		if d > 0 {
			v.coalesce.window = d
		}
	}
}

// WithMaxStagingBytes sets the maximum byte bound for in-memory staged writes.
func WithMaxStagingBytes(b int64) VolumeOption {
	return func(v *Volume) {
		if b > 0 {
			v.maxStagingBytes = b
		}
	}
}

// WithBlobStore sets the blob store used by the volume.
func WithBlobStore(store *blob.Store) VolumeOption {
	return func(v *Volume) {
		if store != nil {
			v.blobStore = store
		}
	}
}

// WithSyncTimeout sets the timeout duration for coalesced background syncs on the controller.
func WithSyncTimeout(d time.Duration) VolumeOption {
	return func(v *Volume) {
		if d > 0 {
			v.syncTimeout = d
		}
	}
}

// WithOpenBarrierTimeout sets the timeout duration for the open-side barrier on close-to-open volumes.
func WithOpenBarrierTimeout(d time.Duration) VolumeOption {
	return func(v *Volume) {
		if d > 0 {
			v.openBarrierTimeout = d
		}
	}
}

// WithConsistencyMode sets the consistency mode for the volume.
func WithConsistencyMode(mode pb.ConsistencyMode) VolumeOption {
	return func(v *Volume) {
		v.consistencyMode = mode
	}
}

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
		localIdx, found, openErr := v.indexFactory.OpenLocal(ctx, v.streamID.String(), v.localStorageDir)
		if openErr != nil {
			klog.Errorf("Volume %s: corrupt or unreadable local index at %s: %v; moving aside and initializing empty index", v.volumeID, v.localStorageDir, openErr)
			corruptDir := fmt.Sprintf("%s.corrupt-%d", v.localStorageDir, time.Now().UnixNano())
			if renErr := os.Rename(v.localStorageDir, corruptDir); renErr != nil {
				klog.Warningf("Volume %s: failed to rename corrupt directory %s to %s: %v", v.volumeID, v.localStorageDir, corruptDir, renErr)
			}
		} else if found && localIdx != nil {
			index = localIdx
		}
		if index == nil {
			var err error
			index, err = v.indexFactory.NewEmpty(ctx, v.streamID.String(), v.localStorageDir)
			if err != nil {
				return fmt.Errorf("failed to create local index: %w", err)
			}
		}
	}

	if v.metadataView != nil {
		if err := v.metadataView.Close(); err != nil {
			return fmt.Errorf("failed to close existing metadata view: %w", err)
		}
	}

	viewOpts := append([]view.Option{}, v.viewOpts...)
	initialStats := &pb.VolumeStats{Name: VolumeStatsRowName}
	viewOpts = append(viewOpts, view.WithStats(initialStats, UpdateVolumeStats))
	viewOpts = append(viewOpts, view.WithRebuildFunc(v.rebuildIndex))
	viewOpts = append(viewOpts, view.WithBatchAppliedHook(v.checkStatsOnBatchApplied))
	if v.metadataStream != nil {
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

func NewVolume(volumeID string, backend ObjectStorageBackend, broadcaster *EventBroadcaster, opts ...VolumeOption) (*Volume, error) {
	var blobStore *blob.Store
	if backend != nil {
		blobStore = blob.NewStore(backend, 0)
	}

	f, err := sds.GetIndexFactory("sqlite")
	if err != nil {
		return nil, fmt.Errorf("failed to get default index factory: %w", err)
	}

	v := &Volume{
		volumeID:           volumeID,
		rootInodeID:        1,
		nextInode:          erofs.DefaultInodeStride,
		backend:            backend,
		blobStore:          blobStore,
		broadcaster:        broadcaster,
		maxInlineLen:       4096,      // 4KB default inline threshold for tiny files
		chunkSize:          64 * 1024, // 64KB default chunk size
		durability:         walclient.Local,
		indexFactory:       f,
		streamID:           StreamIDForVolume(volumeID),
		closedCh:           make(chan struct{}),
		dirParents:         make(map[uint64]uint64),
		handles:            make(map[uint64]uint64),
		openInodes:         make(map[uint64]int),
		stagingEpoch:       time.Now().UnixNano(),
		stagedChunks:       make(map[uint64]map[int]stagedChunkReference),
		stagedSizes:        make(map[uint64]int64),
		stagedMtimes:       make(map[uint64]time.Time),
		lastSyncError:      make(map[uint64]error),
		maxStagingBytes:    64 * 1024 * 1024,
		packs:              make(map[uint64]*packInfo),
		uploadSemaphore:    make(chan struct{}, 2),
		coalesce:           syncCoalesceState{window: 1 * time.Second},
		syncTimeout:        10 * time.Minute,
		openBarrierTimeout: 30 * time.Second,
	}
	uploadContext, cancelUpload := context.WithCancel(context.Background())
	v.uploadContext = uploadContext
	v.cancelUpload = cancelUpload
	v.syncCond = sync.NewCond(&v.stagingMu)
	v.batchCheckCond = sync.NewCond(&v.batchCheckMu)

	for _, opt := range opts {
		opt(v)
	}

	if v.localStorageDir == "" {
		return nil, errors.New("volume requires local storage directory for metadata and write staging (use WithLocalStorageDir)")
	}
	v.stagingDir = filepath.Join(v.localStorageDir, "staging")
	v.stagingMu.Lock()
	if err := v.resetStagingDirLocked(); err != nil {
		v.stagingMu.Unlock()
		return nil, err
	}
	v.stagingMu.Unlock()

	if v.stream == nil {
		return nil, errors.New("volume requires a WAL stream (use WithStream)")
	}

	if err := v.initMetadataStreamLocked(); err != nil {
		return nil, fmt.Errorf("failed to init metadata stream: %w", err)
	}

	if err := v.initMetadataViewLocked(context.Background(), nil); err != nil {
		return nil, fmt.Errorf("failed to init metadata view: %w", err)
	}

	return v, nil
}

func (v *Volume) deletePackLocked(packID uint64) {
	info, ok := v.packs[packID]
	if !ok {
		return
	}
	if info.pack != nil {
		errutil.CloseAndLog(info.pack, "uploaded pack")
	}
	packPath := v.scratchPackPath(packID)
	if removeErr := os.Remove(packPath); removeErr != nil && !os.IsNotExist(removeErr) {
		klog.Warningf("failed removing uploaded pack %s: %v", packPath, removeErr)
	}
	if info.key != "" && v.blobStore != nil {
		v.blobStore.UnregisterLocalPack(info.key)
	}
	delete(v.packs, packID)
}

func (v *Volume) evictUploadedPacksLocked(neededBytes int64) {
	var total int64
	var eligible []uint64
	for packID, info := range v.packs {
		if info.pack != nil {
			total += info.pack.ByteSize()
		}
		if info.state == packUploaded && info.liveReferences <= 0 {
			eligible = append(eligible, packID)
		}
	}
	if total+neededBytes <= v.maxStagingBytes || len(eligible) == 0 {
		return
	}
	sort.Slice(eligible, func(i, j int) bool { return eligible[i] < eligible[j] })
	for _, packID := range eligible {
		info := v.packs[packID]
		var packSize int64
		if info != nil && info.pack != nil {
			packSize = info.pack.ByteSize()
		}
		v.deletePackLocked(packID)
		total -= packSize
		if total+neededBytes <= v.maxStagingBytes {
			break
		}
	}
}

func (v *Volume) dropPackReferenceLocked(packID uint64) {
	if info, ok := v.packs[packID]; ok {
		info.liveReferences--
	}
}

// finishRealSync completes an in-flight real sync operation.
//
// It acquires and releases v.stagingMu. Must be called WITHOUT v.stagingMu held.
//
// If v.currentSync matches operation, it clears v.currentSync (see Volume.currentSync
// for the single-operation rule).
//
// On failure (err != nil, with pos = 0):
// Staged chunk references remain in v.stagedChunks for retry, and err is recorded in
// v.lastSyncError for each inode in snapshotChunks. Note that if the failure occurred
// during the durability wait, packfiles have already been uploaded and metadata records
// committed to the WAL stream and applied in memory; because chunk references remain
// staged, a subsequent retry will append them again.
//
// On success (err == nil):
// It first calls evictUploadedPacksLocked(0) to delete unreferenced uploaded packfiles
// from disk if total pack usage exceeds v.maxStagingBytes (because live references for
// this sync's chunks are still held at this point, packs released by this sync are evicted
// by a later call). For each chunk in snapshotChunks whose reference in v.stagedChunks has
// not been overwritten by a newer write, it drops the reference via dropStagedChunkLocked
// (lowering v.stagedBytes and the packfile's live references). It clears any prior
// v.lastSyncError for those inodes.
//
// Finally, it delegates to completeWaitersAndRearmLocked under v.stagingMu.
//
// Parameters:
//   - operation: the syncOperation installed in v.currentSync when the sync began.
//   - snapshotSeq: staging sequence number cutoff; waiters with waiter.seq <= snapshotSeq
//     matching inodeFilter are completed.
//   - err: the sync error (pack upload, metadata append, or commit wait), or nil on success.
//   - snapshotChunks: the staged chunks that were snapshot and processed by this sync.
//   - pos: the WAL commit sequence number achieved by the sync (0 on failure), reported to waiters.
//   - inodeFilter: optional set of target inode IDs restricting waiter completion (nil matches all).
func (v *Volume) finishRealSync(operation *syncOperation, snapshotSeq uint64, err error, snapshotChunks map[uint64]map[int]stagedChunkReference, pos uint64, inodeFilter map[uint64]struct{}) error {
	v.stagingMu.Lock()
	defer v.stagingMu.Unlock()

	// Clear only the operation this sync installed; see Volume.currentSync for the single-operation rule.
	if v.currentSync == operation {
		v.currentSync = nil
	}

	if err != nil {
		for sIno := range snapshotChunks {
			v.lastSyncError[sIno] = err
		}
	} else {
		v.evictUploadedPacksLocked(0)

		for ino, chunks := range snapshotChunks {
			for index, reference := range chunks {
				if currentReference, ok := v.stagedChunks[ino][index]; ok && currentReference == reference {
					v.dropStagedChunkLocked(ino, index)
				}
			}
			delete(v.lastSyncError, ino)
		}
	}

	v.completeWaitersAndRearmLocked(snapshotSeq, err, pos, inodeFilter)
	return err
}

// finishEmptySync completes an empty sync operation that did not upload or commit new chunks.
//
// It acquires and releases v.stagingMu. Must be called WITHOUT v.stagingMu held.
//
// Unlike finishRealSync, it does not touch v.currentSync because empty syncs do not install
// a syncOperation. If a real sync started after this empty sync released v.stagingMu,
// finishEmptySync leaves v.currentSync intact (see Volume.currentSync).
//
// On failure (err != nil, e.g. from sealActivePackLocked or waitForSequence, with pos = 0):
// Unlike finishRealSync, it records nothing in v.lastSyncError.
//
// On success (err == nil):
// It calls evictUploadedPacksLocked(0) to delete unreferenced uploaded packfiles from disk
// if total pack usage exceeds v.maxStagingBytes.
//
// Finally, it delegates to completeWaitersAndRearmLocked under v.stagingMu to deliver pos
// and err to matching waiters.
//
// Parameters:
//   - snapshotSeq: staging sequence number cutoff; waiters with waiter.seq <= snapshotSeq
//     matching inodeFilter are completed.
//   - err: the error encountered (e.g. from sealActivePackLocked or waitForSequence), or nil on success.
//   - pos: the stream position (v.Position(), or 0 on failure) delivered to completed waiters.
//   - inodeFilter: optional set of target inode IDs restricting waiter completion (nil matches all).
func (v *Volume) finishEmptySync(snapshotSeq uint64, err error, pos uint64, inodeFilter map[uint64]struct{}) error {
	v.stagingMu.Lock()
	defer v.stagingMu.Unlock()

	if err == nil {
		v.evictUploadedPacksLocked(0)
	}

	v.completeWaitersAndRearmLocked(snapshotSeq, err, pos, inodeFilter)
	return err
}

// completeWaitersAndRearmLocked notifies matching coalesce waiters, re-arms staging timers,
// and broadcasts v.syncCond.
//
// Preconditions:
//   - Requires v.stagingMu to be held by the caller (indicated by the Locked suffix).
//   - Does not acquire or release v.stagingMu.
//
// Waiters:
// It scans v.coalesce.waiters and completes any waiter whose seq is <= snapshotSeq and whose
// inodeID is in inodeFilter (or all waiters with seq <= snapshotSeq if inodeFilter is nil).
// Completed waiters receive syncResult{pos: pos, err: err} on waiter.ch, which is then closed.
// Uncompleted waiters remain in v.coalesce.waiters.
//
// Timers:
// If the volume is accepting mutations (v.rejectMutationLocked() == nil, i.e. not stopping or closed):
//   - v.coalesce.timer is armed (if not already armed) when remaining waiters or staged chunks exist.
//   - v.closeTimer is armed (if not already armed) when pending closed inodes exist (v.closeInodes),
//     independent of staged chunks or waiters.
//
// Condition broadcast:
// Finally, it broadcasts v.syncCond to wake goroutines waiting on sync completion or idle
// (specifically: waitForSyncIdleLocked, Stop, openBarrier, and concurrent pack uploads in uploadPacks).
//
// Parameters:
//   - snapshotSeq: staging sequence number cutoff; only waiters with waiter.seq <= snapshotSeq
//     are eligible for completion.
//   - err: error result to deliver to completed waiters (nil on success).
//   - pos: stream position (commit sequence number or current position, or 0 on failure) delivered to completed waiters.
//   - inodeFilter: optional set of target inode IDs restricting waiter completion; if nil,
//     all waiters with seq <= snapshotSeq are completed.
func (v *Volume) completeWaitersAndRearmLocked(snapshotSeq uint64, err error, pos uint64, inodeFilter map[uint64]struct{}) {
	v.coalesce.answerWaiters(snapshotSeq, err, pos, inodeFilter)

	if v.rejectMutationLocked() == nil {
		if len(v.coalesce.waiters) > 0 || len(v.stagedChunks) > 0 {
			v.coalesce.armTimer(v.triggerCoalesceSync)
		}
		if len(v.closeInodes) > 0 && v.closeTimer == nil {
			v.closeTimer = time.AfterFunc(closeCoalesceWindow, v.triggerCloseSync)
		}
	}

	v.syncCond.Broadcast()
}

func (v *Volume) triggerCoalesceSync() {
	v.stagingMu.Lock()
	v.coalesce.timer = nil
	if v.rejectMutationLocked() != nil || v.currentSync != nil {
		v.stagingMu.Unlock()
		return
	}
	if len(v.stagedChunks) == 0 {
		v.stagingMu.Unlock()
		return
	}
	if v.closeTimer != nil {
		v.closeTimer.Stop()
		v.closeTimer = nil
	}
	v.closeInodes = nil

	timeout := v.syncTimeout
	if timeout <= 0 {
		timeout = 10 * time.Minute
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	requirePermanent := v.stream.ReplicationLevel() >= walclient.Permanent
	if _, err := v.executeSyncLocked(ctx, pb.FsyncRequest_SYNC_TRIGGER_PERIODIC, requirePermanent, nil); err != nil {
		klog.Warningf("ObjectFS volume %s coalesce sync failed: %v", v.volumeID, err)
	}
}

// syncInodesLocked executes a per-inode sync for targetInodes under stagingMu.
// If any of targetInodes have staged chunks, it commits them; otherwise it
// completes any waiters for targetInodes immediately.
// Invariant: called with stagingMu held, always returns with stagingMu unlocked.
func (v *Volume) syncInodesLocked(ctx context.Context, targetInodes map[uint64]struct{}, trigger pb.FsyncRequest_SyncTrigger) (uint64, error) {
	hasStaged := false
	for ino := range targetInodes {
		if len(v.stagedChunks[ino]) > 0 {
			hasStaged = true
			break
		}
	}
	if !hasStaged {
		snapSeq := v.stagingSeq
		pos := v.Position()
		v.stagingMu.Unlock()
		if err := v.finishEmptySync(snapSeq, nil, pos, targetInodes); err != nil {
			klog.Warningf("ObjectFS volume %s close finishEmptySync failed: %v", v.volumeID, err)
			return pos, err
		}
		return pos, nil
	}

	requirePermanent := v.stream.ReplicationLevel() >= walclient.Permanent
	return v.executeSyncLocked(ctx, trigger, requirePermanent, targetInodes)
}

func (v *Volume) waitForSyncIdleLocked() {
	for v.currentSync != nil {
		v.syncCond.Wait()
	}
}

func (v *Volume) triggerCloseSync() {
	v.stagingMu.Lock()
	v.closeTimer = nil
	if v.rejectMutationLocked() != nil {
		v.stagingMu.Unlock()
		return
	}
	v.waitForSyncIdleLocked()
	if v.rejectMutationLocked() != nil || len(v.closeInodes) == 0 {
		v.stagingMu.Unlock()
		return
	}
	targetInodes := v.closeInodes
	v.closeInodes = make(map[uint64]struct{})

	timeout := v.syncTimeout
	if timeout <= 0 {
		timeout = 10 * time.Minute
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if _, err := v.syncInodesLocked(ctx, targetInodes, pb.FsyncRequest_SYNC_TRIGGER_CLOSE); err != nil {
		klog.Warningf("ObjectFS volume %s close sync failed: %v", v.volumeID, err)
	}
}

func (v *Volume) Fsync(ctx context.Context, inodeID uint64) error {
	_, _, err := v.FsyncWithEpoch(ctx, inodeID, true, 0, pb.FsyncRequest_SYNC_TRIGGER_UNSPECIFIED)
	return err
}

func (v *Volume) FsyncWithEpoch(ctx context.Context, inodeID uint64, blocking bool, reqEpoch int64, trigger pb.FsyncRequest_SyncTrigger) (int64, uint64, error) {
	v.mu.RLock()
	if inodeID == 0 {
		inodeID = v.rootInodeID
	}
	inodeID = v.normalizeInodeID(inodeID)
	_, err := v.getOrLoadInodeLocked(ctx, nil, inodeID)
	v.mu.RUnlock()
	if err != nil {
		return 0, 0, err
	}

	v.stagingMu.Lock()
	if err := v.rejectMutationLocked(); err != nil {
		epoch := v.stagingEpoch
		pos := v.Position()
		v.stagingMu.Unlock()
		return epoch, pos, err
	}
	if reqEpoch != 0 && reqEpoch != v.stagingEpoch {
		epoch := v.stagingEpoch
		v.stagingMu.Unlock()
		return epoch, 0, syscall.ESTALE
	}

	epoch := v.stagingEpoch

	if blocking {
		if prevErr, ok := v.lastSyncError[inodeID]; ok {
			delete(v.lastSyncError, inodeID)
			v.stagingMu.Unlock()
			return epoch, 0, prevErr
		}

		v.coalesce.stopTimer()
		if v.closeTimer != nil {
			v.closeTimer.Stop()
			v.closeTimer = nil
		}
		v.closeInodes = nil

		v.waitForSyncIdleLocked()

		requirePermanent := v.stream.ReplicationLevel() >= walclient.Permanent
		if len(v.stagedChunks) == 0 {
			snapSeq := v.stagingSeq
			pos := v.Position()
			v.stagingMu.Unlock()
			if err := v.waitForSequence(ctx, pos); err != nil {
				return epoch, 0, v.finishEmptySync(snapSeq, err, 0, nil)
			}
			if requirePermanent {
				if err := v.stream.Flush(ctx); err != nil {
					return epoch, 0, v.finishEmptySync(snapSeq, err, 0, nil)
				}
			}
			return epoch, pos, v.finishEmptySync(snapSeq, nil, pos, nil)
		}

		pos, err := v.executeSyncLocked(ctx, trigger, requirePermanent, nil)
		return epoch, pos, err
	}

	// Non-blocking sync request
	if trigger == pb.FsyncRequest_SYNC_TRIGGER_CLOSE {
		if len(v.stagedChunks[inodeID]) == 0 {
			pos := v.Position()
			v.stagingMu.Unlock()
			if err := v.waitForSequence(ctx, pos); err != nil {
				return epoch, 0, err
			}
			return epoch, pos, nil
		}

		if v.closeInodes == nil {
			v.closeInodes = make(map[uint64]struct{})
		}
		v.closeInodes[inodeID] = struct{}{}

		waitSeq := v.stagingSeq
		waitCh := make(chan syncResult, 1)
		v.coalesce.addWaiter(waitSeq, inodeID, waitCh)

		if v.closeTimer == nil {
			v.closeTimer = time.AfterFunc(closeCoalesceWindow, v.triggerCloseSync)
		}

		v.stagingMu.Unlock()

		select {
		case res := <-waitCh:
			return epoch, res.pos, res.err
		case <-ctx.Done():
			return epoch, 0, ctx.Err()
		}
	}

	// Periodic or unspecified non-blocking sync request
	if len(v.stagedChunks) == 0 {
		pos := v.Position()
		v.stagingMu.Unlock()
		return epoch, pos, nil
	}

	waitSeq := v.stagingSeq
	waitCh := make(chan syncResult, 1)
	v.coalesce.addWaiter(waitSeq, inodeID, waitCh)
	v.coalesce.armTimer(v.triggerCoalesceSync)

	v.stagingMu.Unlock()

	select {
	case res := <-waitCh:
		return epoch, res.pos, res.err
	case <-ctx.Done():
		return epoch, 0, ctx.Err()
	}
}

func (v *Volume) sealActivePackLocked(ctx context.Context) error {
	_, span := scoped.StartSpan(ctx, spanVolumeSyncSeal)
	defer span.End()

	if v.activePack != nil {
		if err := v.activePack.Seal(ctx); err != nil {
			return fmt.Errorf("failed to seal active pack: %w", err)
		}
		v.activePack = nil
	}
	return nil
}

func (v *Volume) uploadPacks(ctx context.Context, packIDsToUpload []uint64) (int64, int64, error) {
	uploadCtx, span := scoped.StartSpan(ctx, spanVolumeSyncUpload)
	defer span.End()

	var syncBytes int64
	var syncPacks int64
	if v.blobStore != nil {
		stopTimer := context.AfterFunc(uploadCtx, func() {
			v.stagingMu.Lock()
			v.syncCond.Broadcast()
			v.stagingMu.Unlock()
		})
		defer stopTimer()
		stopUploadTimer := context.AfterFunc(v.uploadContext, func() {
			v.stagingMu.Lock()
			v.syncCond.Broadcast()
			v.stagingMu.Unlock()
		})
		defer stopUploadTimer()

		for _, packID := range packIDsToUpload {
			v.stagingMu.Lock()
			for {
				info, ok := v.packs[packID]
				if !ok || info.state != packUploading {
					break
				}
				if uploadCtx.Err() != nil || v.uploadContext.Err() != nil {
					v.stagingMu.Unlock()
					syncErr := uploadCtx.Err()
					if syncErr == nil {
						syncErr = v.uploadContext.Err()
					}
					return 0, 0, syncErr
				}
				v.syncCond.Wait()
			}
			info, ok := v.packs[packID]
			v.stagingMu.Unlock()

			if !ok || info.pack == nil {
				continue
			}

			if info.state != packUploaded {
				packKey, err := v.uploadPackFile(uploadCtx, packID)
				if err != nil {
					v.stagingMu.Lock()
					if info, ok := v.packs[packID]; ok {
						info.state = packFailed
						if info.uploadError == nil {
							info.uploadError = err
						}
					}
					v.syncCond.Broadcast()
					v.stagingMu.Unlock()
					klog.Warningf("ObjectFS volume %s: sync upload failed for pack %s (id %d): %v", v.volumeID, packKey, packID, err)
					return 0, 0, fmt.Errorf("failed to upload sealed pack %s: %w", packKey, err)
				}

				v.stagingMu.Lock()
				if info, ok := v.packs[packID]; ok {
					info.state = packUploaded
					info.key = packKey
					info.uploadError = nil
				}
				v.syncCond.Broadcast()
				v.stagingMu.Unlock()
			}

			syncBytes += info.pack.ByteSize()
			syncPacks++
		}
	} else {
		v.stagingMu.Lock()
		for _, packID := range packIDsToUpload {
			if info, ok := v.packs[packID]; ok {
				info.state = packUploaded
				if info.pack != nil {
					syncBytes += info.pack.ByteSize()
					syncPacks++
				}
			}
		}
		v.stagingMu.Unlock()
	}

	return syncBytes, syncPacks, nil
}

func (v *Volume) appendSyncRecordsLocked(
	ctx context.Context,
	snapshotChunks map[uint64]map[int]stagedChunkReference,
	snapshotSizes map[uint64]int64,
	snapshotMtimes map[uint64]time.Time,
) (uint64, int64, []*pb.EntryAttr, error) {
	appendCtx, span := scoped.StartSpan(ctx, spanVolumeSyncAppend)
	defer span.End()

	chunkShas := make(map[uint64]map[int]string, len(snapshotChunks))
	for ino, chunks := range snapshotChunks {
		chunkShas[ino] = make(map[int]string, len(chunks))
		sSize := snapshotSizes[ino]
		if sSize <= v.maxInlineLen && len(chunks) <= 1 {
			continue
		}
		for index, reference := range chunks {
			chunkShas[ino][index] = reference.Entry.SHA256Hex()
		}
	}

	tx := v.beginTxLocked("syncVolume")
	var modifiedAttrs []*pb.EntryAttr

	// Read phase: load all inodes before buffering any mutations in tx
	nodes := make(map[uint64]*CachedInode, len(snapshotChunks))
	for ino := range snapshotChunks {
		node, err := v.getOrLoadInodeLocked(appendCtx, tx, ino)
		if err != nil {
			return 0, 0, nil, err
		}
		nodes[ino] = node
	}

	for ino, chunks := range snapshotChunks {
		node := nodes[ino]
		cs := int64(v.chunkSize)
		if cs == 0 {
			cs = 64 * 1024
		}
		if node.Row.GetChunkSize() > 0 {
			cs = int64(node.Row.GetChunkSize())
		}

		newSize := snapshotSizes[ino]
		if newSize < node.Row.GetSize() {
			newSize = node.Row.GetSize()
		}
		mtime := snapshotMtimes[ino]
		if mtime.IsZero() {
			mtime = time.Now()
		}

		if newSize <= v.maxInlineLen && len(chunks) <= 1 {
			var inlineData []byte
			if reference0, ok := chunks[0]; ok {
				var err error
				inlineData, err = v.readStagedChunkBytes(appendCtx, reference0)
				if err != nil {
					return 0, 0, nil, fmt.Errorf("failed reading inline staged chunk: %w", err)
				}
			}
			node.InlineData = inlineData
			node.Chunks = nil

			if _, err := tx.Insert(appendCtx, &pb.FileChunk{
				Ino:        proto.Uint64(ino),
				Index:      proto.Uint32(0),
				InlineData: append([]byte(nil), inlineData...),
			}); err != nil {
				return 0, 0, nil, fmt.Errorf("failed to log inline FileChunk: %w", err)
			}

			node.mutate(func(row *pb.Inode) {
				row.ChunkSize = uint32(cs)
				row.Mtime = timestamppb.New(mtime)
				row.Ctime = timestamppb.New(mtime)
				row.Size = newSize
				row.ContentSha256 = fmt.Sprintf("%x", sha256.Sum256(inlineData))
				row.Sha256 = ""
				row.ManifestSha256 = ""
			})
		} else {
			node.InlineData = nil
			if node.Chunks == nil {
				node.Chunks = make(map[uint32]string)
			}
			for idx := range chunks {
				sha := chunkShas[ino][idx]
				node.Chunks[uint32(idx)] = sha
				if _, err := tx.Insert(appendCtx, &pb.FileChunk{
					Ino:    proto.Uint64(ino),
					Index:  proto.Uint32(uint32(idx)),
					Sha256: sha,
				}); err != nil {
					return 0, 0, nil, fmt.Errorf("failed to log FileChunk: %w", err)
				}
			}

			numChunks := int((newSize + cs - 1) / cs)
			manifestChunks := make([][32]byte, numChunks)
			for i := 0; i < numChunks; i++ {
				if cSha, ok := node.Chunks[uint32(i)]; ok && cSha != "" {
					raw, err := hex.DecodeString(cSha)
					if err == nil && len(raw) == 32 {
						copy(manifestChunks[i][:], raw)
					}
				}
			}
			manifest := &blob.Manifest{
				ChunkSize:   uint32(cs),
				TotalLength: uint64(newSize),
				Chunks:      manifestChunks,
			}
			manifestBlob, mErr := blob.EncodeManifest(manifest)
			if mErr != nil {
				return 0, 0, nil, fmt.Errorf("failed to encode manifest: %w", mErr)
			}
			manifestHex := manifestBlob.SHA256Hex()

			node.mutate(func(row *pb.Inode) {
				row.ChunkSize = uint32(cs)
				row.Mtime = timestamppb.New(mtime)
				row.Ctime = timestamppb.New(mtime)
				row.Size = newSize
				row.ContentSha256 = ""
				row.Sha256 = ""
				if manifestHex != "" {
					row.ManifestSha256 = manifestHex
				}
			})
		}

		if _, err := tx.Update(appendCtx, node.Row); err != nil {
			return 0, 0, nil, fmt.Errorf("failed to log inode update: %w", err)
		}
	}

	commitSeq, err := v.commitTxLocked(appendCtx, tx)
	if err != nil {
		return 0, 0, nil, fmt.Errorf("failed to commit sync transaction: %w", err)
	}

	for ino := range snapshotChunks {
		node := nodes[ino]
		attr := v.toEntryAttr(node.Row, "", node.RedirectURL)
		modifiedAttrs = append(modifiedAttrs, attr)
	}

	recordsAppended := int64(len(tx.Changes()) + 1)
	return commitSeq, recordsAppended, modifiedAttrs, nil
}

func (v *Volume) waitForCommit(ctx context.Context, waitFn func(context.Context) error, requirePermanent bool) error {
	commitWaitCtx, span := scoped.StartSpan(ctx, spanVolumeSyncCommitWait)
	defer span.End()

	if waitFn != nil {
		if err := waitFn(commitWaitCtx); err != nil {
			return err
		}
	}
	if requirePermanent {
		if err := v.stream.Flush(commitWaitCtx); err != nil {
			return err
		}
	}
	return nil
}

// executeSyncLocked executes a sync for the volume under stagingMu.
//
// What a sync achieves:
// Staged chunk payloads reside in packfiles on local disk, while v.stagedChunks holds
// in-memory references. A sync commits these staged writes to durable storage:
//   - Pack upload: It seals the active packfile and uploads un-uploaded sealed packfiles
//     to the blob store (or marks them uploaded if no blob store is configured).
//   - Metadata stream records: Under v.mu, it writes pb.FileChunk rows and updated pb.Inode rows
//     (with updated size and mtime/ctime) for the inodes that had staged chunks, committed via
//     commitTxLocked to the WAL metadata stream at commitSeq. For an inline file (newSize <= maxInlineLen
//     and len(chunks) <= 1), the chunk row carries the bytes (read from the staged pack, which is
//     still uploaded) and the inode records ContentSha256 in place of ManifestSha256; otherwise,
//     chunk rows carry hashes and the inode records ManifestSha256. Other metadata operations
//     (e.g. CreateFile, SetAttr, Unlink) append their own records when they run; because the WAL
//     stream is strictly ordered, both a real sync's commit wait and an empty sync's watermark wait
//     cover all preceding metadata operations logged on this stream.
//   - Durability wait: Outside v.mu (waitForCommit), it waits on the WAL stream up to commitSeq
//     according to the volume's configured durability level as defined by the WAL client:
//     walclient.Local (fsynced on this node), walclient.Witness (fsynced on the buffer service),
//     or walclient.Permanent (flushed to permanent object storage). If requirePermanent is true,
//     it additionally flushes the stream (v.stream.Flush) to ensure permanent replication.
//   - Modification broadcast: After waitForCommit succeeds, it broadcasts WatchVolumeResponse
//     modification events outside v.mu. (No broadcast occurs if the durability wait fails.)
//
// Staged data and waiters (snapshotSeq and inodeFilter):
// At the start of the sync, snapshotSeq captures v.stagingSeq. If inodeFilter is non-nil, only
// staged chunks for inodes in inodeFilter are snapshotted; if nil, all staged chunks are taken.
// Waiters with waiter.seq <= snapshotSeq matching inodeFilter (all waiters when nil) are completed
// when the sync finishes.
//
// Outcome and error handling:
//   - Early failure: If the volume is closed or a sync is already in flight (v.currentSync != nil),
//     it unlocks v.stagingMu and returns (0, err) immediately without touching waiters, v.currentSync,
//     or v.syncCond.
//   - Pack sealing failure: Unlocks v.stagingMu and delegates to finishEmptySync (pos = 0, nothing
//     recorded in v.lastSyncError).
//   - Empty-sync wait failure: If waitForSequence fails on an empty sync path, it delegates to
//     finishEmptySync with the error and pos = 0.
//   - Empty-sync flush failure: If v.stream.Flush fails on a permanent stream on an empty sync path,
//     it delegates to finishEmptySync with the error and pos = 0.
//   - Completion: On success or later failure (pack upload, metadata append, or durability wait),
//     it delegates to finishRealSync. On a failed durability wait, packfiles are uploaded and
//     metadata records already committed and applied in memory while chunk references remain staged
//     in v.stagedChunks, so a subsequent retry will append them again. See finishRealSync and
//     completeWaitersAndRearmLocked for details on chunk retirement, pack eviction, lastSyncError,
//     waiter notification, timer re-arming, and syncCond broadcast.
//
// Empty syncs:
// An empty sync runs when there are no staged chunks in the volume or none matching inodeFilter.
// It does not upload packfiles, append metadata records, or install v.currentSync. An empty sync
// waits for v.Position() to reach the volume's durability level and returns that error if the wait
// fails; on a permanent stream it then calls Flush and returns any error through finishEmptySync;
// requirePermanent is not consulted; the seal-failure path neither waits nor flushes.
//
// Concurrency rule:
// executeSyncLocked refuses to start when a sync is in flight (v.currentSync != nil), and callers wait
// for idle or skip. The only overlap between an empty sync and a real sync occurs if a real sync starts
// after the empty sync has released v.stagingMu and before its finishEmptySync; finishEmptySync leaves
// v.currentSync untouched (see Volume.currentSync).
//
// Preconditions:
//   - Must be called with v.stagingMu held.
//   - Must be called with no sync in flight (v.currentSync == nil).
//
// Invariant: ALWAYS returns with v.stagingMu UNLOCKED.
func (v *Volume) executeSyncLocked(ctx context.Context, trigger pb.FsyncRequest_SyncTrigger, requirePermanent bool, inodeFilter map[uint64]struct{}) (uint64, error) {
	if v.stagingClosed {
		v.stagingMu.Unlock()
		return 0, fmt.Errorf("volume closed")
	}
	if v.currentSync != nil {
		v.stagingMu.Unlock()
		return 0, fmt.Errorf("volume %s cannot start sync with trigger %v: sync with trigger %v is already in flight", v.volumeID, trigger, v.currentSync.trigger)
	}
	syncStartTime := time.Now()
	syncCtx, syncSpan := scoped.StartSpan(ctx, spanVolumeSync)
	defer syncSpan.End()

	syncSpan.SetAttributes(
		attribute.String(attributeVolume, v.volumeID),
		attribute.String(attributeSyncTrigger, trigger.String()),
	)

	if err := v.sealActivePackLocked(syncCtx); err != nil {
		snapSeq := v.stagingSeq
		v.stagingMu.Unlock()
		return 0, v.finishEmptySync(snapSeq, err, 0, inodeFilter)
	}

	if len(v.stagedChunks) == 0 {
		snapSeq := v.stagingSeq
		pos := v.Position()
		v.stagingMu.Unlock()
		if err := v.waitForSequence(ctx, pos); err != nil {
			return 0, v.finishEmptySync(snapSeq, err, 0, inodeFilter)
		}
		if v.stream.ReplicationLevel() == walclient.Permanent {
			if err := v.stream.Flush(ctx); err != nil {
				return 0, v.finishEmptySync(snapSeq, err, 0, inodeFilter)
			}
		}
		return pos, v.finishEmptySync(snapSeq, nil, pos, inodeFilter)
	}

	snapshotSeq := v.stagingSeq
	snapshotChunks := make(map[uint64]map[int]stagedChunkReference)
	for ino, chunks := range v.stagedChunks {
		if inodeFilter != nil {
			if _, ok := inodeFilter[ino]; !ok {
				continue
			}
		}
		snapshotChunks[ino] = make(map[int]stagedChunkReference, len(chunks))
		for index, reference := range chunks {
			snapshotChunks[ino][index] = reference
		}
	}

	if len(snapshotChunks) == 0 {
		snapSeq := v.stagingSeq
		pos := v.Position()
		v.stagingMu.Unlock()
		if err := v.waitForSequence(ctx, pos); err != nil {
			return 0, v.finishEmptySync(snapSeq, err, 0, inodeFilter)
		}
		if v.stream.ReplicationLevel() == walclient.Permanent {
			if err := v.stream.Flush(ctx); err != nil {
				return 0, v.finishEmptySync(snapSeq, err, 0, inodeFilter)
			}
		}
		return pos, v.finishEmptySync(snapSeq, nil, pos, inodeFilter)
	}

	snapshotSizes := make(map[uint64]int64, len(snapshotChunks))
	for ino := range snapshotChunks {
		if s, ok := v.stagedSizes[ino]; ok {
			snapshotSizes[ino] = s
		}
	}
	snapshotMtimes := make(map[uint64]time.Time, len(snapshotChunks))
	for ino := range snapshotChunks {
		if m, ok := v.stagedMtimes[ino]; ok {
			snapshotMtimes[ino] = m
		}
	}

	totalChunks := 0
	for _, chunks := range snapshotChunks {
		totalChunks += len(chunks)
	}

	syncingInodes := make(map[uint64]struct{}, len(snapshotChunks))
	for ino := range snapshotChunks {
		syncingInodes[ino] = struct{}{}
	}
	operation := &syncOperation{
		trigger:       trigger,
		startTime:     syncStartTime,
		syncingInodes: syncingInodes,
	}
	v.currentSync = operation

	var packIDsToUpload []uint64
	for packID, info := range v.packs {
		if info.state != packUploaded {
			packIDsToUpload = append(packIDsToUpload, packID)
		}
	}
	sort.Slice(packIDsToUpload, func(i, j int) bool {
		return packIDsToUpload[i] < packIDsToUpload[j]
	})
	v.stagingMu.Unlock()

	syncBytes, syncPacks, err := v.uploadPacks(syncCtx, packIDsToUpload)
	if err != nil {
		return 0, v.finishRealSync(operation, snapshotSeq, err, snapshotChunks, 0, inodeFilter)
	}

	v.mu.Lock()
	commitSeq, recordsAppended, modifiedAttrs, err := v.appendSyncRecordsLocked(syncCtx, snapshotChunks, snapshotSizes, snapshotMtimes)
	if err != nil {
		v.mu.Unlock()
		return 0, v.finishRealSync(operation, snapshotSeq, err, snapshotChunks, 0, inodeFilter)
	}

	reqLevel := v.durability
	waitFn := v.makeWaitFn(commitSeq, &reqLevel)
	v.mu.Unlock()

	v.stagingMu.Lock()
	operation.commitSeq = commitSeq
	v.stagingMu.Unlock()

	if err := v.waitForCommit(syncCtx, waitFn, requirePermanent); err != nil {
		return 0, v.finishRealSync(operation, snapshotSeq, err, snapshotChunks, 0, inodeFilter)
	}

	for _, attr := range modifiedAttrs {
		v.broadcaster.Broadcast(v.volumeID, &pb.WatchVolumeResponse{
			EventType:      pb.WatchEventType_EVENT_MODIFIED,
			Attr:           attr,
			Inode:          attr.GetInode().GetIno(),
			StreamPosition: commitSeq,
		})
	}

	syncDuration := time.Since(syncStartTime)
	klog.Infof("ObjectFS volume %s sync completed in %v (%d records appended, %d chunks, %d inodes, commitSeq=%d)",
		v.volumeID, syncDuration, recordsAppended, totalChunks, len(snapshotChunks), commitSeq)

	syncSpan.SetAttributes(
		attribute.Int64(attributeSyncBytes, syncBytes),
		attribute.Int64(attributeSyncRecords, recordsAppended),
		attribute.Int64(attributeSyncPacks, syncPacks),
	)

	return commitSeq, v.finishRealSync(operation, snapshotSeq, nil, snapshotChunks, commitSeq, inodeFilter)
}

type metadataStreamType struct {
	message   proto.Message
	keyFields []int32
}

var metadataStreamTypes = []metadataStreamType{
	{message: &pb.Inode{}, keyFields: []int32{1}},
	{message: &pb.DirEntry{}, keyFields: []int32{1, 2}},
	{message: &pb.FileChunk{}, keyFields: []int32{1, 2}},
	{message: &pb.Content{}, keyFields: []int32{1}},
	{message: &pb.VolumeStats{}, keyFields: []int32{1}},
	{message: &pb.XAttr{}, keyFields: []int32{1, 2}},
	{message: &pb.VolumeInfo{}, keyFields: []int32{1}},
}

func registerMetadataTypes(writer *sds.Writer) error {
	for _, entry := range metadataStreamTypes {
		if _, err := writer.RegisterType(entry.message, entry.keyFields...); err != nil {
			return fmt.Errorf("failed to register metadata type %s: %w", entry.message.ProtoReflect().Descriptor().FullName(), err)
		}
	}
	return nil
}

type registrySyncer interface {
	SyncRegistry(context.Context, *record.Registry) error
}

func (v *Volume) registerMetadataTypesLocked(ctx context.Context, syncer registrySyncer) error {
	if err := registerMetadataTypes(v.metadataStream); err != nil {
		return err
	}
	if syncer != nil && v.metadataStream != nil {
		if err := syncer.SyncRegistry(ctx, v.metadataStream.Registry()); err != nil {
			return fmt.Errorf("failed to sync registry: %w", err)
		}
	}
	return nil
}

func (v *Volume) importIndexRegistryLocked(index sds.LocalIndex) error {
	if v.metadataStream == nil {
		return nil
	}
	if err := v.metadataStream.Registry().Import(index.Registry().Export()); err != nil {
		return fmt.Errorf("failed to import restored snapshot registry: %w", err)
	}
	return nil
}

func (v *Volume) initMetadataStreamLocked() error {
	var writerOpts []record.WriterOption
	appender := sds.NewWALAppender(v.stream, walclient.Local)
	// If the stream already contains local records, mark the header as emitted to avoid
	// appending a duplicate stream header mid-stream upon controller restart.
	// Accepted edge: if all local segments were pruned after S3 ack and the buffer
	// reports witness 0, a header will be re-emitted at seq N; readers tolerate this.
	if local, _, _ := v.stream.Watermarks(); local > 0 {
		writerOpts = append(writerOpts, record.WithStreamHeaderEmitted(true))
	}
	v.metadataStream = sds.NewWriter(appender, writerOpts...)
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
		if pErr != nil {
			return fmt.Errorf("encode chunk prefix: %w", pErr)
		}
		// Rows returned by scanSQLiteRowsLocked are shared/immutable; only read fields here.
		chunkMsgs, sErr := v.scanSQLiteRowsLocked(ctx, tx, "objectfs.v1alpha1.FileChunk", prefixBytes)
		if sErr != nil {
			return fmt.Errorf("scan chunk rows: %w", sErr)
		}
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
		return nil
	}
	return nil
}

// resolveChunkSourceLocked: called with v.mu held; never does backend I/O.
// Returns in-memory bytes (inline/dirty/node.Data) OR a sha to fetch, OR neither (hole).
// Staged (unsynced) chunks in v.stagedChunks are deliberately not consulted:
// uncommitted staged data is only visible to the writing client's local cache until fsync (#245 contract).
func (v *Volume) resolveChunkSourceLocked(ctx context.Context, tx *sds.Tx, node *CachedInode, chunkIdx int) ([]byte, string, error) {
	if chunkIdx == 0 && len(node.InlineData) > 0 {
		res := make([]byte, len(node.InlineData))
		copy(res, node.InlineData)
		return res, "", nil
	}

	if node.DirtyChunks != nil {
		if data, ok := node.DirtyChunks[chunkIdx]; ok {
			res := make([]byte, len(data))
			copy(res, data)
			return res, "", nil
		}
	}

	if err := v.ensureInodeChunksLoadedLocked(ctx, tx, node); err != nil {
		return nil, "", fmt.Errorf("read chunk: inode %d chunks load: %w", node.Row.GetIno(), err)
	}

	if chunkSha, ok := node.Chunks[uint32(chunkIdx)]; ok && chunkSha != "" {
		return nil, chunkSha, nil
	}

	if chunkIdx == 0 && (node.Row.GetSha256() != "" || node.Row.GetContentSha256() != "") && len(node.Chunks) == 0 {
		sha := node.Row.GetSha256()
		if sha == "" {
			sha = node.Row.GetContentSha256()
		}
		return nil, sha, nil
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
				return nil, "", fmt.Errorf("read chunk: inode %d chunk %d rewind: %w", node.Row.GetIno(), chunkIdx, err)
			}
			if _, err := node.Data.Seek(off, io.SeekStart); err != nil {
				return nil, "", fmt.Errorf("read chunk: inode %d chunk %d seek: %w", node.Row.GetIno(), chunkIdx, err)
			}
			n, err := io.ReadFull(node.Data, buf)
			if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
				return nil, "", fmt.Errorf("read chunk: inode %d chunk %d read: %w", node.Row.GetIno(), chunkIdx, err)
			}
			return buf[:n], "", nil
		}
	}

	return nil, "", nil
}

func (v *Volume) readChunkLocked(ctx context.Context, tx *sds.Tx, node *CachedInode, chunkIdx int) ([]byte, error) {
	data, sha, err := v.resolveChunkSourceLocked(ctx, tx, node, chunkIdx)
	if err != nil {
		return nil, err
	}
	if len(data) > 0 || sha == "" {
		return data, nil
	}

	if v.blobStore == nil {
		return nil, fmt.Errorf("read chunk: inode %d chunk %d: blob store not configured", node.Row.GetIno(), chunkIdx)
	}
	stream, err := v.blobStore.GetBlob(ctx, sha)
	if err != nil {
		return nil, fmt.Errorf("read chunk: inode %d chunk %d sha %s: %w", node.Row.GetIno(), chunkIdx, sha, err)
	}
	defer errutil.CloseAndLog(stream, "blob stream")
	bData, err := io.ReadAll(stream)
	if err != nil {
		return nil, fmt.Errorf("read chunk: inode %d chunk %d sha %s read: %w", node.Row.GetIno(), chunkIdx, sha, err)
	}
	return bData, nil
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

// Degraded returns the DegradedError if the underlying WAL stream is degraded, or nil if healthy.
func (v *Volume) Degraded() *walclient.DegradedError {
	v.mu.RLock()
	stream := v.stream
	v.mu.RUnlock()
	return stream.Degraded()
}

// ResetStream updates the volume's underlying WAL stream after repair or reconfiguration.
func (v *Volume) ResetStream(stream walclient.Stream) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.stream = stream
	if err := v.initMetadataStreamLocked(); err != nil {
		return err
	}
	return v.registerMetadataTypesLocked(context.Background(), nil)
}

// ConsistencyMode returns the configured consistency mode for this volume.
func (v *Volume) ConsistencyMode() pb.ConsistencyMode {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.consistencyMode
}

// SetConsistencyMode updates the consistency mode for this volume.
func (v *Volume) SetConsistencyMode(mode pb.ConsistencyMode) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.consistencyMode = mode
}

// PersistConsistencyMode updates the volume's consistency mode and persists it to the metadata store,
// waiting until the persisted VolumeInfo record reaches the volume's durability level.
func (v *Volume) PersistConsistencyMode(ctx context.Context, mode pb.ConsistencyMode) error {
	commitSequence, err := v.persistVolumeInfo(ctx, mode)
	if err != nil {
		return err
	}
	// Wait for the VolumeInfo record's sequence to reach the volume's durability level
	// with no locks held. On wait failure, the record is already sequenced and applied
	// in memory, so the updated consistency mode is visible although the caller gets an error.
	if err := v.waitForSequence(ctx, commitSequence); err != nil {
		return fmt.Errorf("failed to wait for consistency mode durability: %w", err)
	}
	return nil
}

// persistVolumeInfo updates consistency mode in memory and commits VolumeInfo to the metadata stream.
// Takes v.mu.
func (v *Volume) persistVolumeInfo(ctx context.Context, mode pb.ConsistencyMode) (uint64, error) {
	v.mu.Lock()
	defer v.mu.Unlock()

	v.consistencyMode = mode

	volInfo := &pb.VolumeInfo{
		VolumeId:        v.volumeID,
		ConsistencyMode: mode,
	}

	volInfoKey, err := pkVolumeInfo.Extract(volInfo)
	if err != nil {
		return 0, fmt.Errorf("failed to extract VolumeInfo key: %w", err)
	}

	var beforeMsg proto.Message
	exists := false
	if v.metadataView != nil {
		msg, ok, err := v.metadataView.Get(ctx, VolumeInfoTypeName, volInfoKey)
		if err != nil {
			return 0, fmt.Errorf("failed to get VolumeInfo: %w", err)
		}
		if ok && msg != nil {
			beforeMsg = msg
			exists = true
		}
	}

	var commitSequence uint64
	tx := v.beginTxLocked("PersistConsistencyMode")
	if tx != nil {
		v.recordReadLocked(tx, VolumeInfoTypeName, volInfoKey, beforeMsg)
		if exists {
			if _, err := tx.Update(ctx, volInfo); err != nil {
				return 0, fmt.Errorf("failed to update VolumeInfo: %w", err)
			}
		} else {
			if _, err := tx.Insert(ctx, volInfo); err != nil {
				return 0, fmt.Errorf("failed to insert VolumeInfo: %w", err)
			}
		}
		sequence, err := v.commitTxLocked(ctx, tx)
		if err != nil {
			return 0, fmt.Errorf("failed to commit VolumeInfo transaction: %w", err)
		}
		commitSequence = sequence
		if err := v.flushOverlayLocked(ctx); err != nil {
			return 0, fmt.Errorf("failed to flush overlay for VolumeInfo: %w", err)
		}
	}
	return commitSequence, nil
}

func (v *Volume) commitTxLocked(ctx context.Context, tx *sds.Tx) (uint64, error) {
	commitSeq, err := tx.Commit(ctx)
	if err != nil {
		return 0, err
	}
	v.applyTxChangesLocked(ctx, tx)
	// This is the only place lastCommitSeq advances, under v.mu when the transaction
	// commits. It lives here (not in makeWaitFn, where it used to be a side effect
	// of building a waiter) because sync barriers read it whether or not anyone built
	// a waiter.
	if commitSeq > v.lastCommitSeq {
		v.lastCommitSeq = commitSeq
	}
	return commitSeq, nil
}

// ErrVolumeStopping indicates that the volume is stopping and no new mutations or syncs can be accepted.
var ErrVolumeStopping = errors.New("volume is stopping")

func (v *Volume) rejectMutation() error {
	v.stagingMu.Lock()
	defer v.stagingMu.Unlock()
	return v.rejectMutationLocked()
}

func (v *Volume) rejectMutationLocked() error {
	if v.stopping || v.stagingClosed {
		return ErrVolumeStopping
	}
	return nil
}

// Stop gracefully drains the volume by stopping new mutations and sync requests,
// initiating one final sync of all staged chunks if none is currently in flight,
// and waiting for any in-flight sync to complete, bounded by ctx.
// If ctx is already done, Stop returns immediately without marking the volume
// stopping or starting a sync, preserving crash semantics when followed by Close.
// It is idempotent; subsequent calls wait for the active drain without starting another sync.
func (v *Volume) Stop(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("volume %s stop: %w", v.volumeID, err)
	}

	v.stagingMu.Lock()
	if v.stagingClosed {
		v.stagingMu.Unlock()
		return nil
	}
	v.stopping = true
	v.startFinalSyncLocked()

	if v.currentSync == nil {
		return v.finishStopLocked()
	}

	stopTimer := context.AfterFunc(ctx, func() {
		v.stagingMu.Lock()
		v.syncCond.Broadcast()
		v.stagingMu.Unlock()
	})
	defer stopTimer()

	for v.currentSync != nil && ctx.Err() == nil {
		v.syncCond.Wait()
	}

	if v.currentSync != nil {
		elapsed := time.Since(v.currentSync.startTime)
		klog.Warningf("ObjectFS volume %s stop timed out waiting for sync in flight: trigger=%v, commitSeq=%d, elapsed=%v",
			v.volumeID, v.currentSync.trigger, v.currentSync.commitSeq, elapsed)
		v.stagingMu.Unlock()
		return fmt.Errorf("volume %s stop timed out waiting for in-flight sync: %w", v.volumeID, ctx.Err())
	}
	return v.finishStopLocked()
}

func (v *Volume) startFinalSyncLocked() {
	if len(v.stagedChunks) == 0 || v.currentSync != nil {
		return
	}
	v.coalesce.stopTimer()
	if v.closeTimer != nil {
		v.closeTimer.Stop()
		v.closeTimer = nil
	}
	requirePermanent := v.stream.ReplicationLevel() >= walclient.Permanent
	syncStarted := make(chan struct{})
	go func() {
		v.stagingMu.Lock()
		if v.stagingClosed || v.currentSync != nil || len(v.stagedChunks) == 0 {
			close(syncStarted)
			v.stagingMu.Unlock()
			return
		}
		close(syncStarted)
		if _, err := v.executeSyncLocked(v.uploadContext, pb.FsyncRequest_SYNC_TRIGGER_UNSPECIFIED, requirePermanent, nil); err != nil {
			klog.Warningf("ObjectFS volume %s stop sync failed: %v", v.volumeID, err)
		}
	}()
	v.stagingMu.Unlock()
	<-syncStarted
	v.stagingMu.Lock()
}

func (v *Volume) finishStopLocked() error {
	defer v.stagingMu.Unlock()
	if len(v.stagedChunks) > 0 {
		var drainErrors []error
		for _, err := range v.lastSyncError {
			if err != nil {
				drainErrors = append(drainErrors, err)
			}
		}
		stagedCount := len(v.stagedChunks)
		if len(drainErrors) > 0 {
			return fmt.Errorf("volume %s stop failed to drain %d staged chunks: %w", v.volumeID, stagedCount, errors.Join(drainErrors...))
		}
		return fmt.Errorf("volume %s stop failed to drain %d staged chunks", v.volumeID, stagedCount)
	}
	return nil
}

func (v *Volume) closeStagingLocked() bool {
	v.stagingMu.Lock()
	defer v.stagingMu.Unlock()
	v.stopping = true
	v.stagingClosed = true
	drained := v.currentSync == nil && len(v.stagedChunks) == 0
	v.coalesce.stopTimer()
	if v.closeTimer != nil {
		v.closeTimer.Stop()
		v.closeTimer = nil
	}
	v.closeInodes = nil
	v.coalesce.failWaiters(fmt.Errorf("volume closed"))
	if v.cancelUpload != nil {
		v.cancelUpload()
	}
	return drained
}

// Close closes the volume, local storage, and any underlying WAL streams.
// Close never blocks on the object store or the WAL.
func (v *Volume) Close() error {
	v.mu.Lock()
	if v.closed {
		v.mu.Unlock()
		return nil
	}
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

	drained := v.closeStagingLocked()
	v.uploadWaitGroup.Wait()

	v.stagingMu.Lock()
	if removeErr := v.removeStagingDirLocked(); removeErr != nil {
		klog.Warningf("failed removing staging directory: %v", removeErr)
	}
	v.syncCond.Broadcast()
	v.stagingMu.Unlock()

	var firstErr error
	if mView != nil {
		if err := mView.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	v.waitBatchChecks()

	v.mu.Lock()
	defer v.mu.Unlock()

	if drained {
		v.assertStatsMatchLocked("at Close")
	}

	if err := v.stream.Close(); err != nil && firstErr == nil {
		firstErr = err
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
//
// makeWaitFn only builds the wait and records nothing.
func (v *Volume) makeWaitFn(commitSeq uint64, reqLevel *walclient.Level) func(context.Context) error {
	if commitSeq == 0 {
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
			return v.stream.Wait(waitCtx, commitSeq, walclient.Local, false)
		}
		return nil
	}
}

// waitForSequence waits until sequence reaches the volume's configured durability level.
func (v *Volume) waitForSequence(ctx context.Context, sequence uint64) error {
	if waitFn := v.makeWaitFn(sequence, nil); waitFn != nil {
		return waitFn(ctx)
	}
	return nil
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
	_, _, s3Seq := v.stream.Watermarks()
	if s3Seq > 0 && snapPos > s3Seq {
		snapPos = s3Seq
	} else if s3Seq > 0 && snapPos == 0 {
		snapPos = s3Seq
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
	pkInode      = sds.NewPrimaryKey(1)
	pkDirEntry   = sds.NewPrimaryKey(1, 2)
	pkFileChunk  = sds.NewPrimaryKey(1, 2)
	pkXAttr      = sds.NewPrimaryKey(1, 2)
	pkVolumeInfo = sds.NewPrimaryKey(1)
)

// VolumeStats contains aggregated operational and cache metrics for a Volume.
type VolumeStats struct {
	Lag            uint64
	UnappliedBytes int64
	Failures       uint64
	IsDegraded     bool
	Cache          view.LRUCacheStats

	StagingSlotSeals int64

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

	v.stagingMu.Lock()
	slotSeals := v.slotSeals
	v.stagingMu.Unlock()

	return VolumeStats{
		Lag:              v.metadataView.Lag(v.lastCommitSeq),
		UnappliedBytes:   v.metadataView.UnappliedBytes(),
		Failures:         v.metadataView.ApplierFailures(),
		IsDegraded:       v.metadataView.IsDegraded(),
		Cache:            v.metadataView.CacheStats(),
		StagingSlotSeals: slotSeals,
		Stats:            st,
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
	v.waitBatchChecks()
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
			case "objectfs.v1alpha1.XAttr":
				pk = pkXAttr
			case VolumeInfoTypeName:
				pk = pkVolumeInfo
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

func (v *Volume) cleanOrphanInodesLocked(ctx context.Context) (int, error) {
	tx := v.beginTxLocked("cleanOrphanInodes")

	prefix, err := sds.EncodeKeyPrefix(&pb.Inode{}, 0)
	if err != nil {
		return 0, fmt.Errorf("failed to encode inode key prefix: %w", err)
	}
	msgs, err := v.scanSQLiteRowsLocked(ctx, tx, "objectfs.v1alpha1.Inode", prefix)
	if err != nil {
		return 0, fmt.Errorf("failed to scan orphan inodes: %w", err)
	}
	var orphanInos []uint64
	for _, msg := range msgs {
		node := msg.(*pb.Inode)
		if !node.GetIsDir() && node.GetNlink() == 0 {
			orphanInos = append(orphanInos, node.GetIno())
		}
	}
	if len(orphanInos) == 0 {
		return 0, nil
	}

	// Reads before writes: scan all orphan chunk rows before buffering any deletions into tx.
	orphanChunks := make(map[uint64][]proto.Message, len(orphanInos))
	for _, ino := range orphanInos {
		chunkPrefix, err := sds.EncodeKeyPrefix(&pb.FileChunk{Ino: proto.Uint64(ino)}, 1)
		if err != nil {
			return 0, fmt.Errorf("failed to encode chunk key prefix for inode %d: %w", ino, err)
		}
		chunkMsgs, err := v.scanSQLiteRowsLocked(ctx, tx, "objectfs.v1alpha1.FileChunk", chunkPrefix)
		if err != nil {
			return 0, fmt.Errorf("failed to scan chunk rows for orphan inode %d: %w", ino, err)
		}
		orphanChunks[ino] = chunkMsgs
	}

	for _, ino := range orphanInos {
		if _, err := tx.Delete(ctx, &pb.Inode{Ino: proto.Uint64(ino)}); err != nil {
			return 0, fmt.Errorf("failed to log orphan inode %d deletion: %w", ino, err)
		}
		for _, cMsg := range orphanChunks[ino] {
			c := cMsg.(*pb.FileChunk)
			if _, err := tx.Delete(ctx, &pb.FileChunk{Ino: proto.Uint64(ino), Index: proto.Uint32(c.GetIndex())}); err != nil {
				return 0, fmt.Errorf("failed to log chunk deletion for orphan inode %d: %w", ino, err)
			}
		}
	}
	commitSeq, err := v.commitTxLocked(ctx, tx)
	if err != nil {
		return 0, fmt.Errorf("failed to commit orphan deletion transaction: %w", err)
	}
	waitFn := v.makeWaitFn(commitSeq, nil)
	if waitFn != nil {
		if err := waitFn(ctx); err != nil {
			return 0, fmt.Errorf("failed to wait for orphan deletion sync: %w", err)
		}
	}
	return len(orphanInos), nil
}

// deleteInodeRowsLocked deletes the Inode row, all FileChunk rows, and all XAttr rows
// associated with ino. Reads (scans) are performed before any deletion is buffered in tx
// to preserve the transactional reads-before-writes assertion.
func (v *Volume) deleteInodeRowsLocked(ctx context.Context, tx *sds.Tx, ino uint64) error {
	chunkPrefix, err := sds.EncodeKeyPrefix(&pb.FileChunk{Ino: proto.Uint64(ino)}, 1)
	if err != nil {
		return fmt.Errorf("failed to encode chunk key prefix for inode %d: %w", ino, err)
	}
	chunkMsgs, err := v.scanSQLiteRowsLocked(ctx, tx, "objectfs.v1alpha1.FileChunk", chunkPrefix)
	if err != nil {
		return fmt.Errorf("failed to scan chunk rows for inode %d: %w", ino, err)
	}

	xattrPrefix, err := sds.EncodeKeyPrefix(&pb.XAttr{Ino: proto.Uint64(ino)}, 1)
	if err != nil {
		return fmt.Errorf("failed to encode xattr key prefix for inode %d: %w", ino, err)
	}
	xattrMsgs, err := v.scanSQLiteRowsLocked(ctx, tx, "objectfs.v1alpha1.XAttr", xattrPrefix)
	if err != nil {
		return fmt.Errorf("failed to scan xattr rows for inode %d: %w", ino, err)
	}

	if _, err := tx.Delete(ctx, &pb.Inode{Ino: proto.Uint64(ino)}); err != nil {
		return fmt.Errorf("failed to log inode %d deletion: %w", ino, err)
	}
	for _, cMsg := range chunkMsgs {
		c := cMsg.(*pb.FileChunk)
		if _, err := tx.Delete(ctx, &pb.FileChunk{Ino: proto.Uint64(ino), Index: proto.Uint32(c.GetIndex())}); err != nil {
			return fmt.Errorf("failed to log chunk deletion for inode %d: %w", ino, err)
		}
	}
	for _, xMsg := range xattrMsgs {
		x := xMsg.(*pb.XAttr)
		if _, err := tx.Delete(ctx, &pb.XAttr{Ino: proto.Uint64(ino), Name: proto.String(x.GetName())}); err != nil {
			return fmt.Errorf("failed to log xattr deletion for inode %d: %w", ino, err)
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

	if err := v.ensureInodeChunksLoadedLocked(ctx, tx, node); err != nil {
		return nil, fmt.Errorf("ensure inode chunks loaded: %w", err)
	}

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

func (v *Volume) toEntryAttr(row *pb.Inode, name, redirectURL string) *pb.EntryAttr {
	if row == nil {
		return nil
	}
	ino := row.GetIno()
	if name == "" && (ino == v.rootInodeID || ino == 1) {
		name = "/"
	}
	if v.rootInodeID != 0 && ino == v.rootInodeID && v.rootInodeID != 1 {
		rowCopy := proto.Clone(row).(*pb.Inode)
		rowCopy.Ino = proto.Uint64(1)
		row = rowCopy
	}
	return &pb.EntryAttr{
		Inode:          row,
		Name:           name,
		RedirectUrl:    redirectURL,
		StreamPosition: v.lastCommitSeq,
	}
}

func (v *Volume) toEntryAttrLocked(ctx context.Context, tx *sds.Tx, inodeID uint64, name string) (*pb.EntryAttr, error) {
	node, err := v.getOrLoadInodeLocked(ctx, tx, inodeID)
	if err != nil {
		return nil, err
	}
	return v.toEntryAttr(node.Row, name, node.RedirectURL), nil
}

func (v *Volume) GetAttr(ctx context.Context, inodeID uint64) (*pb.EntryAttr, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	inodeID = v.normalizeInodeID(inodeID)
	return v.toEntryAttrLocked(ctx, nil, inodeID, "")
}

func (v *Volume) SetAttr(ctx context.Context, inodeID uint64, mode *uint32, uid *uint32, gid *uint32, atime *time.Time, atimeNow bool, mtime *time.Time, mtimeNow bool, ctime *time.Time, ctimeNow bool) (*pb.EntryAttr, error) {
	attr, waitFn, err := func() (*pb.EntryAttr, func(context.Context) error, error) {
		if err := v.rejectMutation(); err != nil {
			return nil, nil, err
		}
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

		commitSeq, err := v.commitTxLocked(ctx, tx)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to commit setattr transaction: %w", err)
		}

		waitFn := v.makeWaitFn(commitSeq, nil)

		attr := v.toEntryAttr(node.Row, "", node.RedirectURL)
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
			EventType:      pb.WatchEventType_EVENT_MODIFIED,
			Attr:           attr,
			Inode:          attr.GetInode().GetIno(),
			StreamPosition: attr.GetStreamPosition(),
		})
	}
	return attr, nil
}

// GetXAttr reads the value of the named extended attribute on inodeID.
func (v *Volume) GetXAttr(ctx context.Context, inodeID uint64, name string) ([]byte, error) {
	if len(name) > MaxXAttrNameLength {
		return nil, fmt.Errorf("xattr name %q exceeds maximum length: %w", name, syscall.ENAMETOOLONG)
	}
	if strings.HasPrefix(name, "system.") {
		return nil, fmt.Errorf("system xattrs not supported: %w", syscall.ENOTSUP)
	}

	v.mu.RLock()
	defer v.mu.RUnlock()

	inodeID = v.normalizeInodeID(inodeID)
	if _, err := v.getOrLoadInodeLocked(ctx, nil, inodeID); err != nil {
		return nil, err
	}

	key, err := pkXAttr.Extract(&pb.XAttr{
		Ino:  proto.Uint64(inodeID),
		Name: proto.String(name),
	})
	if err != nil {
		return nil, err
	}

	msg, ok, err := v.getSQLiteRowLocked(ctx, nil, "objectfs.v1alpha1.XAttr", key)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("xattr %q not found on inode %d: %w", name, inodeID, syscall.ENODATA)
	}

	xattr := msg.(*pb.XAttr)
	return append([]byte(nil), xattr.GetValue()...), nil
}

// SetXAttr sets the value of the named extended attribute on inodeID.
func (v *Volume) SetXAttr(ctx context.Context, inodeID uint64, name string, value []byte, flags pb.SetXAttrRequest_Flags) error {
	if len(name) > MaxXAttrNameLength {
		return fmt.Errorf("xattr name %q exceeds maximum length: %w", name, syscall.ENAMETOOLONG)
	}
	if strings.HasPrefix(name, "system.") {
		return fmt.Errorf("system xattrs not supported: %w", syscall.ENOTSUP)
	}
	if len(value) > MaxXAttrValueLength {
		return fmt.Errorf("xattr value length %d exceeds maximum length %d: %w", len(value), MaxXAttrValueLength, syscall.E2BIG)
	}

	waitFn, attr, err := func() (func(context.Context) error, *pb.EntryAttr, error) {
		if err := v.rejectMutation(); err != nil {
			return nil, nil, err
		}
		v.mu.Lock()
		defer v.mu.Unlock()

		tx := v.beginTxLocked("SetXAttr")
		if err := v.checkBackpressureLocked(ctx); err != nil {
			return nil, nil, err
		}

		inodeID = v.normalizeInodeID(inodeID)
		node, err := v.getOrLoadInodeLocked(ctx, tx, inodeID)
		if err != nil {
			return nil, nil, err
		}

		key, err := pkXAttr.Extract(&pb.XAttr{
			Ino:  proto.Uint64(inodeID),
			Name: proto.String(name),
		})
		if err != nil {
			return nil, nil, err
		}

		_, exists, err := v.getSQLiteRowLocked(ctx, tx, "objectfs.v1alpha1.XAttr", key)
		if err != nil {
			return nil, nil, err
		}

		if flags == pb.SetXAttrRequest_FLAGS_CREATE && exists {
			return nil, nil, fmt.Errorf("xattr %q already exists: %w", name, syscall.EEXIST)
		}
		if flags == pb.SetXAttrRequest_FLAGS_REPLACE && !exists {
			return nil, nil, fmt.Errorf("xattr %q does not exist: %w", name, syscall.ENODATA)
		}

		now := time.Now()
		node.mutate(func(row *pb.Inode) {
			row.Ctime = timestamppb.New(now)
			if !exists {
				count := row.GetXattrCount() + 1
				row.XattrCount = &count
			}
		})

		xattrMsg := &pb.XAttr{
			Ino:   proto.Uint64(inodeID),
			Name:  proto.String(name),
			Value: append([]byte(nil), value...),
		}
		if exists {
			if _, err := tx.Update(ctx, xattrMsg); err != nil {
				return nil, nil, fmt.Errorf("failed to log xattr update: %w", err)
			}
		} else {
			if _, err := tx.Insert(ctx, xattrMsg); err != nil {
				return nil, nil, fmt.Errorf("failed to log xattr insert: %w", err)
			}
		}

		if _, err := tx.Update(ctx, node.Row); err != nil {
			return nil, nil, fmt.Errorf("failed to log inode ctime update: %w", err)
		}

		commitSeq, err := v.commitTxLocked(ctx, tx)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to commit setxattr transaction: %w", err)
		}

		waitFn := v.makeWaitFn(commitSeq, nil)
		attr := v.toEntryAttr(node.Row, "", node.RedirectURL)
		return waitFn, attr, nil
	}()
	if err != nil {
		return err
	}
	if waitFn != nil {
		if err := waitFn(ctx); err != nil {
			return err
		}
	}
	if v.broadcaster != nil {
		v.broadcaster.Broadcast(v.volumeID, &pb.WatchVolumeResponse{
			EventType:      pb.WatchEventType_EVENT_MODIFIED,
			Attr:           attr,
			Inode:          attr.GetInode().GetIno(),
			StreamPosition: attr.GetStreamPosition(),
		})
	}
	return nil
}

// ListXAttr returns all extended attributes set on inodeID as name and value pairs.
func (v *Volume) ListXAttr(ctx context.Context, inodeID uint64) ([]*pb.XAttr, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	inodeID = v.normalizeInodeID(inodeID)
	if _, err := v.getOrLoadInodeLocked(ctx, nil, inodeID); err != nil {
		return nil, err
	}

	prefixBytes, err := sds.EncodeKeyPrefix(&pb.XAttr{Ino: proto.Uint64(inodeID)}, 1)
	if err != nil {
		return nil, fmt.Errorf("failed to encode xattr key prefix for inode %d: %w", inodeID, err)
	}

	msgs, err := v.scanSQLiteRowsLocked(ctx, nil, "objectfs.v1alpha1.XAttr", prefixBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to scan xattr rows for inode %d: %w", inodeID, err)
	}

	xattrs := make([]*pb.XAttr, 0, len(msgs))
	for _, msg := range msgs {
		xattrs = append(xattrs, msg.(*pb.XAttr))
	}
	return xattrs, nil
}

// RemoveXAttr removes the named extended attribute from inodeID.
func (v *Volume) RemoveXAttr(ctx context.Context, inodeID uint64, name string) error {
	if len(name) > MaxXAttrNameLength {
		return fmt.Errorf("xattr name %q exceeds maximum length: %w", name, syscall.ENAMETOOLONG)
	}
	if strings.HasPrefix(name, "system.") {
		return fmt.Errorf("system xattrs not supported: %w", syscall.ENOTSUP)
	}

	waitFn, attr, err := func() (func(context.Context) error, *pb.EntryAttr, error) {
		if err := v.rejectMutation(); err != nil {
			return nil, nil, err
		}
		v.mu.Lock()
		defer v.mu.Unlock()

		tx := v.beginTxLocked("RemoveXAttr")
		if err := v.checkBackpressureLocked(ctx); err != nil {
			return nil, nil, err
		}

		inodeID = v.normalizeInodeID(inodeID)
		node, err := v.getOrLoadInodeLocked(ctx, tx, inodeID)
		if err != nil {
			return nil, nil, err
		}

		key, err := pkXAttr.Extract(&pb.XAttr{
			Ino:  proto.Uint64(inodeID),
			Name: proto.String(name),
		})
		if err != nil {
			return nil, nil, err
		}

		_, exists, err := v.getSQLiteRowLocked(ctx, tx, "objectfs.v1alpha1.XAttr", key)
		if err != nil {
			return nil, nil, err
		}
		if !exists {
			return nil, nil, fmt.Errorf("xattr %q not found on inode %d: %w", name, inodeID, syscall.ENODATA)
		}

		now := time.Now()
		node.mutate(func(row *pb.Inode) {
			row.Ctime = timestamppb.New(now)
			if row.GetXattrCount() > 0 {
				count := row.GetXattrCount() - 1
				row.XattrCount = &count
			} else {
				row.XattrCount = proto.Uint32(0)
			}
		})

		if _, err := tx.Delete(ctx, &pb.XAttr{Ino: proto.Uint64(inodeID), Name: proto.String(name)}); err != nil {
			return nil, nil, fmt.Errorf("failed to log xattr deletion: %w", err)
		}

		if _, err := tx.Update(ctx, node.Row); err != nil {
			return nil, nil, fmt.Errorf("failed to log inode ctime update: %w", err)
		}

		commitSeq, err := v.commitTxLocked(ctx, tx)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to commit removexattr transaction: %w", err)
		}

		waitFn := v.makeWaitFn(commitSeq, nil)
		attr := v.toEntryAttr(node.Row, "", node.RedirectURL)
		return waitFn, attr, nil
	}()
	if err != nil {
		return err
	}
	if waitFn != nil {
		if err := waitFn(ctx); err != nil {
			return err
		}
	}
	if v.broadcaster != nil {
		v.broadcaster.Broadcast(v.volumeID, &pb.WatchVolumeResponse{
			EventType:      pb.WatchEventType_EVENT_MODIFIED,
			Attr:           attr,
			Inode:          attr.GetInode().GetIno(),
			StreamPosition: attr.GetStreamPosition(),
		})
	}
	return nil
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
		return nil, fmt.Errorf("readdir %d: %w", dirInodeID, err)
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
		if err != nil {
			return nil, fmt.Errorf("readdir %d: entry %q (ino %d): %w", dirInodeID, name, de.GetIno(), err)
		}
		entries = append(entries, attr)
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
		if err := v.rejectMutation(); err != nil {
			return nil, nil, err
		}
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
			Ino:        proto.Uint64(childInodeID),
			Mode:       mode,
			Size:       0,
			Mtime:      timestamppb.New(now),
			Atime:      timestamppb.New(now),
			Ctime:      timestamppb.New(now),
			Uid:        uid,
			Gid:        gid,
			IsDir:      true,
			Nlink:      2,
			ParentIno:  proto.Uint64(parentInodeID),
			XattrCount: proto.Uint32(0),
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

		commitSeq, err := v.commitTxLocked(ctx, tx)
		if err != nil {
			delete(v.dirParents, childInodeID)
			return nil, nil, fmt.Errorf("failed to commit mkdir transaction: %w", err)
		}

		waitFn := v.makeWaitFn(commitSeq, nil)

		attr := v.toEntryAttr(childInodeMsg, name, "")
		v.broadcaster.Broadcast(v.volumeID, &pb.WatchVolumeResponse{
			EventType:      pb.WatchEventType_EVENT_CREATED,
			Attr:           attr,
			Inode:          childInodeID,
			ParentInode:    parentInodeID,
			Name:           name,
			StreamPosition: commitSeq,
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

func (v *Volume) CreateFile(ctx context.Context, parentInodeID uint64, name string, mode uint32, initialContent []byte, uid, gid, rdev uint32) (*pb.EntryAttr, error) {
	if len(name) > MaxNameLength {
		return nil, fmt.Errorf("file name %q exceeds maximum length: %w", name, syscall.ENAMETOOLONG)
	}

	attr, waitFn, err := func() (*pb.EntryAttr, func(context.Context) error, error) {
		if err := v.rejectMutation(); err != nil {
			return nil, nil, err
		}
		v.mu.Lock()
		defer v.mu.Unlock()

		tx := v.beginTxLocked("CreateFile")

		if err := v.checkBackpressureLocked(ctx); err != nil {
			return nil, nil, err
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
				errutil.CloseAndLog(childInode.Data, "inode data")
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
				row.Sha256 = ""
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

			commitSeq, err := v.commitTxLocked(ctx, tx)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to commit overwrite file transaction: %w", err)
			}

			waitFn := v.makeWaitFn(commitSeq, nil)

			attr := v.toEntryAttr(childInode.Row, name, childInode.RedirectURL)
			v.broadcaster.Broadcast(v.volumeID, &pb.WatchVolumeResponse{
				EventType:      pb.WatchEventType_EVENT_MODIFIED,
				Attr:           attr,
				Inode:          childInode.Row.GetIno(),
				ParentInode:    parentInodeID,
				Name:           name,
				StreamPosition: commitSeq,
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
				Rdev:          rdev,
				ContentSha256: hashStr,
				XattrCount:    proto.Uint32(0),
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
		commitSeq, err := v.commitTxLocked(ctx, tx)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to commit create file transaction: %w", err)
		}

		waitFn := v.makeWaitFn(commitSeq, nil)

		attr := v.toEntryAttr(childInode.Row, name, "")
		v.broadcaster.Broadcast(v.volumeID, &pb.WatchVolumeResponse{
			EventType:      pb.WatchEventType_EVENT_CREATED,
			Attr:           attr,
			Inode:          childInodeID,
			ParentInode:    parentInodeID,
			Name:           name,
			StreamPosition: commitSeq,
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
		if err := v.rejectMutation(); err != nil {
			return nil, nil, err
		}
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
				XattrCount:    proto.Uint32(0),
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

		commitSeq, err := v.commitTxLocked(ctx, tx)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to commit symlink transaction: %w", err)
		}

		waitFn := v.makeWaitFn(commitSeq, nil)

		attr := v.toEntryAttr(childInode.Row, name, "")
		v.broadcaster.Broadcast(v.volumeID, &pb.WatchVolumeResponse{
			EventType:      pb.WatchEventType_EVENT_CREATED,
			Attr:           attr,
			Inode:          childInodeID,
			ParentInode:    parentInodeID,
			Name:           name,
			StreamPosition: commitSeq,
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
		if err := v.rejectMutation(); err != nil {
			return nil, nil, err
		}
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

		commitSeq, err := v.commitTxLocked(ctx, tx)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to commit link transaction: %w", err)
		}

		waitFn := v.makeWaitFn(commitSeq, nil)

		attr := v.toEntryAttr(oldNode.Row, newName, "")
		v.broadcaster.Broadcast(v.volumeID, &pb.WatchVolumeResponse{
			EventType:      pb.WatchEventType_EVENT_CREATED,
			Attr:           attr,
			Inode:          oldNode.Row.GetIno(),
			ParentInode:    newParentInodeID,
			Name:           newName,
			StreamPosition: commitSeq,
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

func (v *Volume) readBaseChunk(ctx context.Context, node *CachedInode, chunkIdx int) ([]byte, error) {
	v.mu.Lock()
	data, sha, err := v.resolveChunkSourceLocked(ctx, nil, node, chunkIdx)
	ino := node.Row.GetIno()
	blobStore := v.blobStore
	v.mu.Unlock()

	if err != nil {
		return nil, err
	}
	if len(data) > 0 || sha == "" {
		return data, nil
	}

	if blobStore == nil {
		return nil, fmt.Errorf("read chunk: inode %d chunk %d: blob store not configured", ino, chunkIdx)
	}
	stream, err := blobStore.GetBlob(ctx, sha)
	if err != nil {
		return nil, fmt.Errorf("read chunk: inode %d chunk %d sha %s: %w", ino, chunkIdx, sha, err)
	}
	bData, err := io.ReadAll(stream)
	if cErr := stream.Close(); cErr != nil && err == nil {
		err = fmt.Errorf("read chunk: inode %d chunk %d sha %s close: %w", ino, chunkIdx, sha, cErr)
	}
	if err != nil {
		return nil, fmt.Errorf("read chunk: inode %d chunk %d sha %s read: %w", ino, chunkIdx, sha, err)
	}
	return bData, nil
}

func (v *Volume) ReadFile(ctx context.Context, inodeID uint64, offset, length int64) ([]byte, int64, string, error) {
	if inodeID == 0 {
		v.mu.RLock()
		inodeID = v.rootInodeID
		v.mu.RUnlock()
	}

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

	if len(node.InlineData) > 0 {
		res := make([]byte, readLen)
		if offset < int64(len(node.InlineData)) {
			copyEnd := end
			if copyEnd > int64(len(node.InlineData)) {
				copyEnd = int64(len(node.InlineData))
			}
			copy(res, node.InlineData[offset:copyEnd])
		}
		v.mu.Unlock()
		return res, total, "", nil
	}

	if node.Row.GetManifestSha256() != "" || node.Row.GetChunkSize() > 0 || len(node.Chunks) > 0 {
		if err := v.ensureInodeChunksLoadedLocked(ctx, nil, node); err != nil {
			v.mu.Unlock()
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
		numChunks := endChunk - startChunk + 1

		type chunkPlan struct {
			idx  int
			data []byte
			sha  string
		}
		plans := make([]chunkPlan, numChunks)

		// Staged (unsynced) chunks in v.stagedChunks are deliberately not consulted by ReadFile:
		// uncommitted staged data is only visible to the writing client's local cache until fsync (#245 contract).
		for i := startChunk; i <= endChunk; i++ {
			pIdx := i - startChunk
			plans[pIdx].idx = i

			data, sha, err := v.resolveChunkSourceLocked(ctx, nil, node, i)
			if err != nil {
				v.mu.Unlock()
				return nil, 0, "", err
			}
			plans[pIdx].data = data
			plans[pIdx].sha = sha
		}

		ino := node.Row.GetIno()
		blobStore := v.blobStore
		v.mu.Unlock()

		for _, p := range plans {
			if p.sha != "" && blobStore == nil {
				return nil, 0, "", fmt.Errorf("read chunk: inode %d chunk %d: blob store not configured", ino, p.idx)
			}
		}

		var missingCount int
		for _, p := range plans {
			if p.sha != "" && p.data == nil {
				missingCount++
			}
		}

		if missingCount > 0 {
			var wg sync.WaitGroup
			sem := make(chan struct{}, 16)
			var firstErr error
			var errOnce sync.Once

			fetchCtx, fetchCancel := context.WithCancel(ctx)
			defer fetchCancel()

			// Goroutines write to disjoint indices (plan.data) and plans is read only after wg.Wait(); safe without a mutex.
			for pIdx := range plans {
				plan := &plans[pIdx]
				if plan.sha == "" || plan.data != nil {
					continue
				}

				if fetchCtx.Err() != nil {
					break
				}

				wg.Add(1)
				sem <- struct{}{}
				go func(p *chunkPlan) {
					defer func() {
						<-sem
						wg.Done()
					}()

					if fetchCtx.Err() != nil {
						return
					}

					stream, err := blobStore.GetBlob(fetchCtx, p.sha)
					if err != nil {
						errOnce.Do(func() {
							firstErr = fmt.Errorf("read chunk: inode %d chunk %d sha %s: %w", ino, p.idx, p.sha, err)
							fetchCancel()
						})
						return
					}

					data, err := io.ReadAll(stream)
					if cErr := stream.Close(); cErr != nil && err == nil {
						err = cErr
					}
					if err != nil {
						errOnce.Do(func() {
							firstErr = fmt.Errorf("read chunk: inode %d chunk %d sha %s read: %w", ino, p.idx, p.sha, err)
							fetchCancel()
						})
						return
					}
					p.data = data
				}(plan)
			}
			wg.Wait()

			if firstErr != nil {
				return nil, 0, "", firstErr
			}
			if err := ctx.Err(); err != nil {
				return nil, 0, "", err
			}
		}

		res := make([]byte, readLen)
		for i := startChunk; i <= endChunk; i++ {
			chunkData := plans[i-startChunk].data
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
				outOffset := chunkStart + rStart - offset
				validEnd := rEnd
				if validEnd > int64(len(chunkData)) {
					validEnd = int64(len(chunkData))
				}
				if validEnd > rStart {
					copy(res[outOffset:outOffset+(validEnd-rStart)], chunkData[rStart:validEnd])
				}
			}
		}
		return res, total, "", nil
	}

	// Lazy load data from blob store if not currently in memory
	if node.Data == nil && node.Row.GetSize() > 0 {
		sha := node.Row.GetSha256()
		if sha != "" && v.blobStore != nil {
			blobStore := v.blobStore
			ino := node.Row.GetIno()
			v.mu.Unlock()
			stream, err := blobStore.GetBlob(ctx, sha)
			if err != nil {
				return nil, 0, "", fmt.Errorf("read inode %d blob %s: %w", ino, sha, err)
			}
			v.mu.Lock()
			if node.Data == nil {
				node.Data = stream
			} else {
				if cErr := stream.Close(); cErr != nil {
					klog.V(4).Infof("failed to close redundant stream: %v", cErr)
				}
			}
		}
	}

	if node.Data == nil {
		v.mu.Unlock()
		return make([]byte, readLen), total, "", nil
	}

	if _, err := node.Data.Seek(offset, io.SeekStart); err != nil {
		v.mu.Unlock()
		return nil, 0, "", fmt.Errorf("seek inode %d data at %d: %w", node.Row.GetIno(), offset, err)
	}
	res := make([]byte, readLen)
	n, err := io.ReadFull(node.Data, res)
	v.mu.Unlock()
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return nil, 0, "", fmt.Errorf("failed to read node data: %w", err)
	}
	return res[:n], total, "", nil
}

func (v *Volume) GetManifest(ctx context.Context, inodeID uint64) (*pb.GetManifestResponse, error) {
	if inodeID == 0 {
		v.mu.RLock()
		inodeID = v.rootInodeID
		v.mu.RUnlock()
	}

	v.mu.RLock()
	node, err := v.getOrLoadInodeLocked(ctx, nil, inodeID)
	v.mu.RUnlock()
	if err != nil {
		return nil, err
	}

	if node.Row.GetIsDir() {
		return nil, fmt.Errorf("cannot get manifest for directory: %w", syscall.EISDIR)
	}

	total := node.Row.GetSize()
	cs := uint32(node.Row.GetChunkSize())
	if cs == 0 {
		cs = uint32(v.chunkSize)
		if cs == 0 {
			cs = 64 * 1024
		}
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	var inlineData []byte
	if len(node.InlineData) > 0 {
		inlineData = make([]byte, len(node.InlineData))
		copy(inlineData, node.InlineData)
	}

	if err := v.ensureInodeChunksLoadedLocked(ctx, nil, node); err != nil {
		return nil, fmt.Errorf("manifest inode %d chunks load: %w", node.Row.GetIno(), err)
	}

	var chunkSHAs []string
	if total > 0 {
		numChunks := int((total + int64(cs) - 1) / int64(cs))
		chunkSHAs = make([]string, numChunks)
		for i := 0; i < numChunks; i++ {
			if sha, ok := node.Chunks[uint32(i)]; ok && sha != "" {
				chunkSHAs[i] = sha
			} else if i == 0 && numChunks == 1 && len(node.Chunks) == 0 {
				sha := node.Row.GetSha256()
				if sha == "" {
					sha = node.Row.GetContentSha256()
				}
				chunkSHAs[0] = sha
			}
		}
	}

	return &pb.GetManifestResponse{
		Inode:       node.Row.GetIno(),
		ChunkSize:   cs,
		TotalSize:   total,
		ChunkSha256: chunkSHAs,
		InlineData:  inlineData,
	}, nil
}

func (v *Volume) scratchPackPath(packID uint64) string {
	return filepath.Join(v.stagingDir, fmt.Sprintf("pack-%016x.pack", packID))
}

func (v *Volume) readStagedChunkStreamLocked(ctx context.Context, reference stagedChunkReference) (blob.ByteStream, error) {
	info, ok := v.packs[reference.PackID]
	if !ok || info.pack == nil {
		return nil, fmt.Errorf("staged pack %d not found", reference.PackID)
	}
	return info.pack.ReadBlob(ctx, reference.Entry)
}

// readStagedChunkBytesLocked reads a single staged chunk's payload into memory for modification.
// Staging memory overhead here is bounded by a single chunkSize buffer.
func (v *Volume) readStagedChunkBytesLocked(ctx context.Context, reference stagedChunkReference) ([]byte, error) {
	stream, err := v.readStagedChunkStreamLocked(ctx, reference)
	if err != nil {
		return nil, err
	}
	defer errutil.CloseAndLog(stream, "staged chunk stream")
	return io.ReadAll(stream)
}

func (v *Volume) readStagedChunkBytes(ctx context.Context, reference stagedChunkReference) ([]byte, error) {
	v.stagingMu.Lock()
	defer v.stagingMu.Unlock()
	return v.readStagedChunkBytesLocked(ctx, reference)
}

func (v *Volume) removeStagingDirLocked() error {
	if v.activePack != nil {
		errutil.CloseAndLog(v.activePack, "active scratch pack")
		v.activePack = nil
	}
	for _, info := range v.packs {
		if info.pack != nil {
			errutil.CloseAndLog(info.pack, "staged pack")
		}
		if info.key != "" && v.blobStore != nil {
			v.blobStore.UnregisterLocalPack(info.key)
		}
	}
	v.packs = make(map[uint64]*packInfo)
	if removeErr := os.RemoveAll(v.stagingDir); removeErr != nil && !os.IsNotExist(removeErr) {
		return fmt.Errorf("failed removing staging directory %s: %w", v.stagingDir, removeErr)
	}
	return nil
}

func (v *Volume) resetStagingDirLocked() error {
	if v.cancelUpload != nil {
		v.cancelUpload()
	}
	v.stagingMu.Unlock()
	v.uploadWaitGroup.Wait()
	v.stagingMu.Lock()

	v.uploadContext, v.cancelUpload = context.WithCancel(context.Background())
	if err := v.removeStagingDirLocked(); err != nil {
		return err
	}
	if mkdirErr := os.MkdirAll(v.stagingDir, 0755); mkdirErr != nil {
		return fmt.Errorf("failed creating staging directory %s: %w", v.stagingDir, mkdirErr)
	}
	return nil
}

func (v *Volume) uploadPackFile(ctx context.Context, packID uint64) (string, error) {
	v.stagingMu.Lock()
	info, ok := v.packs[packID]
	closeContext := v.uploadContext
	v.stagingMu.Unlock()
	if !ok || info.pack == nil {
		return "", fmt.Errorf("pack %d not found", packID)
	}
	if v.blobStore == nil {
		return "", nil
	}
	putContext, cancel := context.WithCancel(ctx)
	defer cancel()
	stopCancel := context.AfterFunc(closeContext, cancel)
	defer stopCancel()

	pack := info.pack
	packPath := v.scratchPackPath(packID)
	file, openErr := os.Open(packPath)
	if openErr != nil {
		return "", fmt.Errorf("failed to open sealed pack %s: %w", packPath, openErr)
	}
	hasher := sha256.New()
	if _, copyErr := io.Copy(hasher, file); copyErr != nil {
		errutil.CloseAndLog(file, "pack file")
		return "", fmt.Errorf("failed hashing sealed pack %s: %w", packPath, copyErr)
	}
	if _, seekErr := file.Seek(0, io.SeekStart); seekErr != nil {
		errutil.CloseAndLog(file, "pack file")
		return "", fmt.Errorf("failed seeking sealed pack %s: %w", packPath, seekErr)
	}
	packIDString := hex.EncodeToString(hasher.Sum(nil))
	packKey := v.blobStore.PackKey(packIDString, pack.Count())
	packStream := blob.NewByteStreamFromFile(file, pack.ByteSize(), false)

	uploadStartTime := time.Now()
	if err := v.blobStore.PutPackFile(putContext, packKey, packStream, pack.Entries(), pack); err != nil {
		errutil.CloseAndLog(packStream, "failed pack stream")
		return packKey, fmt.Errorf("failed to upload sealed pack %s: %w", packKey, err)
	}
	uploadDuration := time.Since(uploadStartTime)
	klog.Infof("ObjectFS volume %s uploaded sealed pack %s (%d bytes, %d entries) in %v", v.volumeID, packKey, pack.ByteSize(), pack.Count(), uploadDuration)
	if err := packStream.Close(); err != nil {
		klog.Warningf("failed to close pack stream %s: %v", packKey, err)
	}
	return packKey, nil
}

func (v *Volume) startPackUploadLocked(packID uint64) {
	info, ok := v.packs[packID]
	if !ok {
		return
	}
	if v.blobStore == nil {
		info.state = packUploaded
		return
	}
	if info.state == packUploading || info.state == packUploaded {
		return
	}
	info.state = packUploading
	uploadContext := v.uploadContext

	v.uploadWaitGroup.Add(1)
	go func() {
		defer v.uploadWaitGroup.Done()

		select {
		case v.uploadSemaphore <- struct{}{}:
		case <-uploadContext.Done():
			v.stagingMu.Lock()
			if info, ok := v.packs[packID]; ok {
				info.state = packNotStarted
			}
			v.syncCond.Broadcast()
			v.stagingMu.Unlock()
			return
		}
		defer func() { <-v.uploadSemaphore }()

		packKey, err := v.uploadPackFile(uploadContext, packID)

		v.stagingMu.Lock()
		defer v.stagingMu.Unlock()
		info, ok := v.packs[packID]
		if !ok {
			return
		}
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				info.state = packNotStarted
			} else {
				info.state = packFailed
				if info.uploadError == nil {
					info.uploadError = err
				}
				klog.Warningf("ObjectFS volume %s: background upload failed for pack %s (id %d): %v", v.volumeID, packKey, packID, err)
			}
		} else {
			info.state = packUploaded
			info.key = packKey
			info.uploadError = nil
		}
		v.syncCond.Broadcast()
	}()
}

func (v *Volume) appendChunkLocked(ctx context.Context, data []byte) (stagedChunkReference, error) {
	v.evictUploadedPacksLocked(int64(len(data)))
	if v.activePack == nil {
		v.nextPackID++
		packID := v.nextPackID
		packPath := v.scratchPackPath(packID)
		pack, err := blob.NewIncrementalPack(ctx, packPath, blob.DefaultPackSlots)
		if err != nil {
			return stagedChunkReference{}, fmt.Errorf("failed creating incremental pack %s: %w", packPath, err)
		}
		v.activePack = pack
		v.activePackID = packID
		v.packs[packID] = &packInfo{
			pack:  pack,
			state: packNotStarted,
		}
	}

	entry, err := v.activePack.Append(ctx, data)
	if err != nil {
		if errors.Is(err, blob.ErrPackSlotsFull) {
			return stagedChunkReference{}, fmt.Errorf("active pack %d is full despite eager sealing: %w", v.activePackID, err)
		}
		return stagedChunkReference{}, err
	}

	ref := stagedChunkReference{
		PackID: v.activePackID,
		Entry:  entry,
		Length: uint32(len(data)),
	}

	// Seal eagerly on the write that fills the pack so the active pack is never full
	// and the next write opens a fresh pack without retry recursion.
	if v.activePack.Count() >= int(blob.DefaultPackSlots) {
		sealedPackID := v.activePackID
		entryCount := v.activePack.Count()
		if sealErr := v.activePack.Seal(ctx); sealErr != nil {
			return stagedChunkReference{}, fmt.Errorf("failed sealing full pack: %w", sealErr)
		}
		v.slotSeals++
		v.activePack = nil
		klog.Infof("ObjectFS volume %s: sealed pack %d (%d entries) on slot cap", v.volumeID, sealedPackID, entryCount)
		v.startPackUploadLocked(sealedPackID)
	}

	return ref, nil
}

func (v *Volume) setStagedChunkLocked(inodeID uint64, index int, reference stagedChunkReference) {
	chunks, ok := v.stagedChunks[inodeID]
	if !ok {
		chunks = make(map[int]stagedChunkReference)
		v.stagedChunks[inodeID] = chunks
	} else if existing, ok := chunks[index]; ok {
		v.stagedBytes -= int64(existing.Length)
		v.dropPackReferenceLocked(existing.PackID)
	}
	chunks[index] = reference
	v.stagedBytes += int64(reference.Length)
	if info, ok := v.packs[reference.PackID]; ok {
		info.liveReferences++
	}
}

func (v *Volume) dropStagedChunkLocked(inodeID uint64, index int) {
	chunks, ok := v.stagedChunks[inodeID]
	if !ok {
		return
	}
	if existing, ok := chunks[index]; ok {
		v.stagedBytes -= int64(existing.Length)
		delete(chunks, index)
		v.dropPackReferenceLocked(existing.PackID)
		if len(chunks) == 0 {
			delete(v.stagedChunks, inodeID)
			delete(v.stagedSizes, inodeID)
			delete(v.stagedMtimes, inodeID)
		}
	}
}

func (v *Volume) dropStagedChunksLocked(inodeID uint64) {
	chunks, ok := v.stagedChunks[inodeID]
	if !ok {
		return
	}
	for _, reference := range chunks {
		v.stagedBytes -= int64(reference.Length)
		v.dropPackReferenceLocked(reference.PackID)
	}
	delete(v.stagedChunks, inodeID)
	delete(v.stagedSizes, inodeID)
	delete(v.stagedMtimes, inodeID)
}

// rewriteStagedChunkLocked modifies an existing staged chunk in-place by reading, transforming, and re-appending it.
// Kept for Truncate only.
func (v *Volume) rewriteStagedChunkLocked(ctx context.Context, inodeID uint64, index int, modify func([]byte) []byte) error {
	existing, ok := v.stagedChunks[inodeID][index]
	if !ok {
		return fmt.Errorf("staged chunk %d on inode %d not found for rewrite", index, inodeID)
	}
	chunkData, err := v.readStagedChunkBytesLocked(ctx, existing)
	if err != nil {
		return fmt.Errorf("failed reading staged chunk %d for rewrite: %w", index, err)
	}
	modifiedData := modify(chunkData)
	reference, err := v.appendChunkLocked(ctx, modifiedData)
	if err != nil {
		return err
	}
	v.setStagedChunkLocked(inodeID, index, reference)
	return nil
}

func (v *Volume) WriteFile(ctx context.Context, inodeID uint64, offset int64, data []byte, writeMode pb.WriteMode) (int64, int64, time.Time, error) {
	bytesWritten, newSize, modifiedTime, _, err := v.WriteFileWithEpoch(ctx, inodeID, offset, data, writeMode)
	return bytesWritten, newSize, modifiedTime, err
}

func (v *Volume) WriteFileWithEpoch(ctx context.Context, inodeID uint64, offset int64, data []byte, writeMode pb.WriteMode) (int64, int64, time.Time, int64, error) {
	v.mu.RLock()
	inodeID = v.normalizeInodeID(inodeID)
	v.mu.RUnlock()

	v.mu.RLock()
	node, err := v.getOrLoadInodeLocked(ctx, nil, inodeID)
	v.mu.RUnlock()
	if err != nil {
		return 0, 0, time.Time{}, 0, err
	}

	if node.Row.GetIsDir() {
		return 0, 0, time.Time{}, 0, fmt.Errorf("cannot write to directory: %w", syscall.EISDIR)
	}

	dataLength := int64(len(data))
	now := time.Now()
	chunkSize := int64(v.chunkSize)
	if chunkSize == 0 {
		chunkSize = 64 * 1024
	}
	if node.Row.GetChunkSize() > 0 {
		chunkSize = int64(node.Row.GetChunkSize())
	}

	startChunk := int(offset / chunkSize)
	endChunk := int((offset + dataLength - 1) / chunkSize)

	// Identify which chunks require base chunk reads (partial chunk writes not yet staged).
	// To avoid holding stagingMu across backend GetBlob, check staged chunks under stagingMu,
	// fetch missing base chunks outside stagingMu (and outside v.mu), then reacquire stagingMu.
	v.stagingMu.Lock()
	if err := v.rejectMutationLocked(); err != nil {
		v.stagingMu.Unlock()
		return 0, 0, time.Time{}, 0, err
	}
	if v.stagedBytes+dataLength > v.maxStagingBytes {
		v.stagingMu.Unlock()
		return 0, 0, time.Time{}, 0, syscall.ENOSPC
	}

	currentSize := node.Row.GetSize()
	if stagedSize, ok := v.stagedSizes[inodeID]; ok && stagedSize > currentSize {
		currentSize = stagedSize
	}
	calculatedSize := offset + dataLength
	if calculatedSize < currentSize {
		calculatedSize = currentSize
	}

	var neededBaseChunks []int
	for i := startChunk; i <= endChunk; i++ {
		if _, ok := v.stagedChunks[inodeID][i]; ok {
			continue
		}
		chunkStart := int64(i) * chunkSize
		writeStart := offset - chunkStart
		if writeStart < 0 {
			writeStart = 0
		}
		writeEnd := offset + dataLength - chunkStart
		if writeEnd > chunkSize {
			writeEnd = chunkSize
		}
		expectedChunkLength := chunkSize
		if int64(i+1)*chunkSize > calculatedSize {
			expectedChunkLength = calculatedSize - int64(i)*chunkSize
		}
		if writeStart > 0 || writeEnd < expectedChunkLength {
			neededBaseChunks = append(neededBaseChunks, i)
		}
	}
	v.stagingMu.Unlock()

	baseChunks := make(map[int][]byte, len(neededBaseChunks))
	for _, index := range neededBaseChunks {
		baseData, err := v.readBaseChunk(ctx, node, index)
		if err != nil {
			return 0, 0, time.Time{}, 0, err
		}
		if len(baseData) > 0 {
			baseChunks[index] = baseData
		}
	}

	v.stagingMu.Lock()
	defer v.stagingMu.Unlock()

	if err := v.rejectMutationLocked(); err != nil {
		return 0, 0, time.Time{}, 0, err
	}
	if v.stagedBytes+dataLength > v.maxStagingBytes {
		return 0, 0, time.Time{}, 0, syscall.ENOSPC
	}

	currentSize = node.Row.GetSize()
	if stagedSize, ok := v.stagedSizes[inodeID]; ok && stagedSize > currentSize {
		currentSize = stagedSize
	}
	calculatedSize = offset + dataLength
	if calculatedSize < currentSize {
		calculatedSize = currentSize
	}

	// If there's inline data committed on the base inode and startChunk > 0 or write is partial,
	// ensure base inline data is transferred to staged chunk 0
	if len(node.InlineData) > 0 {
		if _, ok := v.stagedChunks[inodeID][0]; !ok {
			reference, err := v.appendChunkLocked(ctx, node.InlineData)
			if err != nil {
				return 0, 0, time.Time{}, 0, err
			}
			v.setStagedChunkLocked(inodeID, 0, reference)
		}
	}

	for i := startChunk; i <= endChunk; i++ {
		chunkStart := int64(i) * chunkSize
		chunkEnd := chunkStart + chunkSize
		writeStart := offset - chunkStart
		if writeStart < 0 {
			writeStart = 0
		}
		writeEnd := offset + dataLength - chunkStart
		if writeEnd > chunkSize {
			writeEnd = chunkSize
		}
		dataStart := chunkStart - offset
		if dataStart < 0 {
			dataStart = 0
		}
		dataEnd := chunkEnd - offset
		if dataEnd > dataLength {
			dataEnd = dataLength
		}

		expectedChunkLength := chunkSize
		if int64(i+1)*chunkSize > calculatedSize {
			expectedChunkLength = calculatedSize - int64(i)*chunkSize
		}

		var chunkData []byte
		if existing, ok := v.stagedChunks[inodeID][i]; ok {
			var err error
			chunkData, err = v.readStagedChunkBytesLocked(ctx, existing)
			if err != nil {
				return 0, 0, time.Time{}, 0, fmt.Errorf("failed reading staged chunk %d: %w", i, err)
			}
		} else if baseData, ok := baseChunks[i]; ok && len(baseData) > 0 {
			chunkData = make([]byte, len(baseData))
			copy(chunkData, baseData)
		}

		if int64(len(chunkData)) < expectedChunkLength {
			newBuffer := make([]byte, expectedChunkLength)
			copy(newBuffer, chunkData)
			chunkData = newBuffer
		}
		copy(chunkData[writeStart:writeEnd], data[dataStart:dataEnd])
		reference, err := v.appendChunkLocked(ctx, chunkData)
		if err != nil {
			return 0, 0, time.Time{}, 0, err
		}
		v.setStagedChunkLocked(inodeID, i, reference)
	}

	v.stagedSizes[inodeID] = calculatedSize
	v.stagedMtimes[inodeID] = now
	v.stagingSeq++

	if v.currentSync != nil {
		v.coalesce.armTimer(v.triggerCoalesceSync)
	}

	return dataLength, calculatedSize, now, v.stagingEpoch, nil
}

func (v *Volume) TruncateFile(ctx context.Context, inodeID uint64, size int64) (*pb.EntryAttr, error) {
	v.mu.RLock()
	inodeID = v.normalizeInodeID(inodeID)
	v.mu.RUnlock()

	v.stagingMu.Lock()
	if err := v.rejectMutationLocked(); err != nil {
		v.stagingMu.Unlock()
		return nil, err
	}
	if chunks, ok := v.stagedChunks[inodeID]; ok {
		cs := int64(v.chunkSize)
		if cs == 0 {
			cs = 64 * 1024
		}
		for index, reference := range chunks {
			if int64(index)*cs >= size {
				v.dropStagedChunkLocked(inodeID, index)
			} else if int64(index+1)*cs > size {
				keep := size - int64(index)*cs
				if int64(reference.Length) > keep {
					err := v.rewriteStagedChunkLocked(ctx, inodeID, index, func(data []byte) []byte {
						return data[:keep]
					})
					if err != nil {
						v.stagingMu.Unlock()
						return nil, fmt.Errorf("failed appending truncated chunk: %w", err)
					}
				}
			}
		}
		if _, ok := v.stagedChunks[inodeID]; ok {
			v.stagedSizes[inodeID] = size
			v.stagedMtimes[inodeID] = time.Now()
		}
	}
	v.stagingMu.Unlock()

	attr, waitFn, err := func() (*pb.EntryAttr, func(context.Context) error, error) {
		if err := v.rejectMutation(); err != nil {
			return nil, nil, err
		}
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
				row.ContentSha256 = ""
				row.Sha256 = ""
			})
			node.Chunks = nil
			node.DirtyChunks = nil
			node.InlineData = nil
			if node.Data != nil {
				errutil.CloseAndLog(node.Data, "inode data")
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
			node.DirtyChunks = nil

			node.mutate(func(row *pb.Inode) {
				row.Mtime = timestamppb.New(now)
				row.Ctime = timestamppb.New(now)
				row.Size = size
				row.ContentSha256 = ""
				row.Sha256 = ""
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
				if _, exists := node.Chunks[uint32(lastIdx)]; exists {
					chunkData, err := v.readChunkLocked(ctx, tx, node, lastIdx)
					if err != nil {
						return nil, nil, err
					}
					lastChunkData = chunkData
					hasLastChunkData = true
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
					if v.blobStore != nil {
						if err := v.blobStore.PutBlobs(ctx, map[string]blob.ByteStream{
							chunkSha: blob.NewByteStreamFromBytes(truncatedData),
						}); err != nil {
							return nil, nil, fmt.Errorf("failed to upload truncated chunk: %w", err)
						}
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
				row.Sha256 = ""
			})
		}

		if _, err := tx.Update(ctx, node.Row); err != nil {
			return nil, nil, fmt.Errorf("failed to log node inode update: %w", err)
		}

		commitSeq, err := v.commitTxLocked(ctx, tx)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to commit truncate file transaction: %w", err)
		}

		waitFn := v.makeWaitFn(commitSeq, nil)

		attr := v.toEntryAttr(node.Row, "", node.RedirectURL)
		v.broadcaster.Broadcast(v.volumeID, &pb.WatchVolumeResponse{
			EventType:      pb.WatchEventType_EVENT_MODIFIED,
			Attr:           attr,
			Inode:          node.Row.GetIno(),
			StreamPosition: commitSeq,
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

func (v *Volume) PunchHole(ctx context.Context, inodeID uint64, offset, length int64, keepSize bool) (*pb.EntryAttr, error) {
	if offset < 0 || length <= 0 || offset+length < 0 {
		return nil, fmt.Errorf("invalid offset %d or length %d: %w", offset, length, syscall.EINVAL)
	}

	v.mu.RLock()
	inodeID = v.normalizeInodeID(inodeID)
	v.mu.RUnlock()

	// Sync any staged state for this inode first so we operate on committed state only.
	if err := v.Fsync(ctx, inodeID); err != nil {
		return nil, err
	}

	attr, waitFn, err := func() (*pb.EntryAttr, func(context.Context) error, error) {
		if err := v.rejectMutation(); err != nil {
			return nil, nil, err
		}
		v.mu.Lock()
		defer v.mu.Unlock()

		tx := v.beginTxLocked("PunchHole")

		if err := v.checkBackpressureLocked(ctx); err != nil {
			return nil, nil, err
		}

		inodeID = v.normalizeInodeID(inodeID)

		node, err := v.getOrLoadInodeLocked(ctx, tx, inodeID)
		if err != nil {
			return nil, nil, err
		}

		if node.Row.GetIsDir() {
			return nil, nil, fmt.Errorf("cannot punch hole on directory: %w", syscall.EISDIR)
		}

		now := time.Now()
		if err := v.ensureInodeChunksLoadedLocked(ctx, tx, node); err != nil {
			return nil, nil, fmt.Errorf("punch hole: ensure inode chunks loaded: %w", err)
		}

		currSize := node.Row.GetSize()
		newSize := currSize
		if !keepSize && offset+length > currSize {
			newSize = offset + length
		}

		punchStart := offset
		punchEnd := offset + length
		if keepSize && punchEnd > currSize {
			punchEnd = currSize
		}

		if keepSize && punchStart >= currSize {
			attr := v.toEntryAttr(node.Row, "", node.RedirectURL)
			return attr, nil, nil
		}

		cs := int64(node.Row.GetChunkSize())
		if cs == 0 {
			cs = int64(v.chunkSize)
			if cs == 0 {
				cs = 64 * 1024
			}
		}

		if len(node.InlineData) > 0 {
			node.IsDirty = true
			if newSize <= v.maxInlineLen {
				if newSize > int64(len(node.InlineData)) {
					newBuf := make([]byte, newSize)
					copy(newBuf, node.InlineData)
					node.InlineData = newBuf
				}
				zEnd := punchEnd
				if zEnd > int64(len(node.InlineData)) {
					zEnd = int64(len(node.InlineData))
				}
				if zEnd > punchStart {
					for b := punchStart; b < zEnd; b++ {
						node.InlineData[b] = 0
					}
				}
				node.mutate(func(row *pb.Inode) {
					row.Mtime = timestamppb.New(now)
					row.Ctime = timestamppb.New(now)
					row.Size = newSize
					row.ContentSha256 = fmt.Sprintf("%x", sha256.Sum256(node.InlineData))
					row.Sha256 = ""
					row.ManifestSha256 = ""
				})
				if _, err := tx.Insert(ctx, &pb.FileChunk{
					Ino:        proto.Uint64(node.Row.GetIno()),
					Index:      proto.Uint32(0),
					InlineData: append([]byte(nil), node.InlineData...),
				}); err != nil {
					return nil, nil, fmt.Errorf("failed to update inline FileChunk: %w", err)
				}
			} else {
				c0 := make([]byte, cs)
				copy(c0, node.InlineData)
				zEnd := punchEnd
				if zEnd > cs {
					zEnd = cs
				}
				if zEnd > punchStart {
					for b := punchStart; b < zEnd; b++ {
						c0[b] = 0
					}
				}
				c0Len := int64(len(node.InlineData))
				if cs < c0Len {
					c0Len = cs
				}
				truncatedC0 := c0[:c0Len]
				sha0 := fmt.Sprintf("%x", sha256.Sum256(truncatedC0))
				if v.blobStore != nil {
					if err := v.blobStore.PutBlobs(ctx, map[string]blob.ByteStream{
						sha0: blob.NewByteStreamFromBytes(truncatedC0),
					}); err != nil {
						return nil, nil, fmt.Errorf("failed to upload chunk 0: %w", err)
					}
				}
				node.InlineData = nil
				if node.Chunks == nil {
					node.Chunks = make(map[uint32]string)
				}
				node.Chunks[0] = sha0
				node.mutate(func(row *pb.Inode) {
					row.Mtime = timestamppb.New(now)
					row.Ctime = timestamppb.New(now)
					row.ChunkSize = uint32(cs)
					row.Size = newSize
					row.ContentSha256 = ""
					row.Sha256 = ""
					row.ManifestSha256 = ""
				})
				if _, err := tx.Insert(ctx, &pb.FileChunk{
					Ino:    proto.Uint64(node.Row.GetIno()),
					Index:  proto.Uint32(0),
					Sha256: sha0,
				}); err != nil {
					return nil, nil, fmt.Errorf("failed to update FileChunk 0: %w", err)
				}
			}
		} else if punchEnd > punchStart {
			if node.Chunks == nil {
				node.Chunks = make(map[uint32]string)
			}

			firstChunk := punchStart / cs
			lastChunk := (punchEnd - 1) / cs

			// Reads before writes: read all partially covered chunks that exist before mutating tx.
			partialData := make(map[uint32][]byte)
			for i := firstChunk; i <= lastChunk; i++ {
				cStart := i * cs
				cEnd := cStart + cs
				fullyCovered := punchStart <= cStart && punchEnd >= cEnd
				if !fullyCovered {
					if _, exists := node.Chunks[uint32(i)]; exists {
						data, err := v.readChunkLocked(ctx, tx, node, int(i))
						if err != nil {
							return nil, nil, fmt.Errorf("failed to read chunk %d: %w", i, err)
						}
						partialData[uint32(i)] = data
					}
				}
			}

			// Writes: delete fully covered chunks; zero and re-put partially covered chunks.
			for i := firstChunk; i <= lastChunk; i++ {
				cStart := i * cs
				cEnd := cStart + cs
				fullyCovered := punchStart <= cStart && punchEnd >= cEnd

				if fullyCovered {
					if _, exists := node.Chunks[uint32(i)]; exists {
						delete(node.Chunks, uint32(i))
						if _, err := tx.Delete(ctx, &pb.FileChunk{
							Ino:   proto.Uint64(node.Row.GetIno()),
							Index: proto.Uint32(uint32(i)),
						}); err != nil {
							return nil, nil, fmt.Errorf("failed to delete FileChunk row %d: %w", i, err)
						}
					}
				} else {
					if chunkData, exists := partialData[uint32(i)]; exists && len(chunkData) > 0 {
						overlapStart := punchStart - cStart
						if overlapStart < 0 {
							overlapStart = 0
						}
						overlapEnd := punchEnd - cStart
						if overlapEnd > int64(len(chunkData)) {
							overlapEnd = int64(len(chunkData))
						}
						if overlapEnd > overlapStart {
							for b := overlapStart; b < overlapEnd; b++ {
								chunkData[b] = 0
							}
							chunkSha := fmt.Sprintf("%x", sha256.Sum256(chunkData))
							node.Chunks[uint32(i)] = chunkSha
							if v.blobStore != nil {
								if err := v.blobStore.PutBlobs(ctx, map[string]blob.ByteStream{
									chunkSha: blob.NewByteStreamFromBytes(chunkData),
								}); err != nil {
									return nil, nil, fmt.Errorf("failed to upload modified chunk %d: %w", i, err)
								}
							}
							if _, err := tx.Insert(ctx, &pb.FileChunk{
								Ino:    proto.Uint64(node.Row.GetIno()),
								Index:  proto.Uint32(uint32(i)),
								Sha256: chunkSha,
							}); err != nil {
								return nil, nil, fmt.Errorf("failed to log modified chunk %d: %w", i, err)
							}
						}
					}
				}
			}

			node.mutate(func(row *pb.Inode) {
				row.Mtime = timestamppb.New(now)
				row.Ctime = timestamppb.New(now)
				row.ChunkSize = uint32(cs)
				row.Size = newSize
				row.ContentSha256 = ""
				row.Sha256 = ""
				row.ManifestSha256 = ""
			})
		} else {
			node.mutate(func(row *pb.Inode) {
				row.Mtime = timestamppb.New(now)
				row.Ctime = timestamppb.New(now)
				row.ChunkSize = uint32(cs)
				row.Size = newSize
				row.ContentSha256 = ""
				row.Sha256 = ""
				row.ManifestSha256 = ""
			})
		}

		if _, err := tx.Update(ctx, node.Row); err != nil {
			return nil, nil, fmt.Errorf("failed to log node inode update: %w", err)
		}

		commitSeq, err := v.commitTxLocked(ctx, tx)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to commit punch hole transaction: %w", err)
		}

		waitFn := v.makeWaitFn(commitSeq, nil)

		attr := v.toEntryAttr(node.Row, "", node.RedirectURL)
		v.broadcaster.Broadcast(v.volumeID, &pb.WatchVolumeResponse{
			EventType:      pb.WatchEventType_EVENT_MODIFIED,
			Attr:           attr,
			Inode:          node.Row.GetIno(),
			StreamPosition: commitSeq,
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

func (v *Volume) Unlink(ctx context.Context, parentInodeID uint64, name string) (uint64, error) {
	if len(name) > MaxNameLength {
		return 0, fmt.Errorf("file name %q exceeds maximum length: %w", name, syscall.ENAMETOOLONG)
	}

	v.mu.Lock()
	parentInodeID = v.normalizeInodeID(parentInodeID)

	var childInodeID uint64
	de, ok, err := v.getDirEntrySQLiteLocked(ctx, nil, parentInodeID, name)
	if err != nil {
		v.mu.Unlock()
		return 0, err
	}
	if !ok {
		v.mu.Unlock()
		return 0, fmt.Errorf("file %s not found under inode %d: %w", name, parentInodeID, syscall.ENOENT)
	}
	childInode, err := v.getOrLoadInodeLocked(ctx, nil, de.GetIno())
	if err != nil {
		v.mu.Unlock()
		return 0, err
	}
	if childInode.Row.GetIsDir() {
		v.mu.Unlock()
		return 0, fmt.Errorf("cannot unlink directory %s: %w", name, syscall.EISDIR)
	}
	childInodeID = childInode.Row.GetIno()
	v.mu.Unlock()

	v.stagingMu.Lock()
	v.dropStagedChunksLocked(childInodeID)
	v.stagingMu.Unlock()

	waitFn, commitSeq, err := func() (func(context.Context) error, uint64, error) {
		if err := v.rejectMutation(); err != nil {
			return nil, 0, err
		}
		v.mu.Lock()
		defer v.mu.Unlock()

		tx := v.beginTxLocked("Unlink")

		if err := v.checkBackpressureLocked(ctx); err != nil {
			return nil, 0, err
		}

		_, ok, err := v.getDirEntrySQLiteLocked(ctx, tx, parentInodeID, name)
		if err != nil {
			return nil, 0, err
		}
		if !ok {
			return nil, 0, fmt.Errorf("file %s not found under inode %d: %w", name, parentInodeID, syscall.ENOENT)
		}

		childInode, err := v.getOrLoadInodeLocked(ctx, tx, childInodeID)
		if err != nil {
			return nil, 0, err
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

		parentInode, err := v.getOrLoadInodeLocked(ctx, tx, parentInodeID)
		if err != nil {
			return nil, 0, err
		}
		parentInode.mutate(func(row *pb.Inode) {
			row.Mtime = timestamppb.New(now)
			row.Ctime = timestamppb.New(now)
		})

		var commitSeq uint64
		if childInode.Row.GetNlink() == 0 && !v.hasOpenHandlesLocked(childInodeID) {
			if err := v.deleteInodeRowsLocked(ctx, tx, childInodeID); err != nil {
				return nil, 0, err
			}
		} else {
			if _, err := tx.Update(ctx, childInode.Row); err != nil {
				return nil, 0, fmt.Errorf("failed to log child inode update: %w", err)
			}
		}

		if _, err := tx.Delete(ctx, &pb.DirEntry{ParentIno: proto.Uint64(parentInodeID), Name: proto.String(name)}); err != nil {
			return nil, 0, fmt.Errorf("failed to log dir entry deletion: %w", err)
		}

		if parentInode != nil {
			if _, err := tx.Update(ctx, parentInode.Row); err != nil {
				return nil, 0, fmt.Errorf("failed to log parent inode update: %w", err)
			}
		}
		commitSeq, err = v.commitTxLocked(ctx, tx)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to commit transaction: %w", err)
		}

		waitFn := v.makeWaitFn(commitSeq, nil)

		v.broadcaster.Broadcast(v.volumeID, &pb.WatchVolumeResponse{
			EventType:      pb.WatchEventType_EVENT_DELETED,
			Inode:          childInode.Row.GetIno(),
			ParentInode:    parentInodeID,
			Name:           name,
			StreamPosition: commitSeq,
		})

		return waitFn, commitSeq, nil
	}()
	if err != nil {
		return 0, err
	}
	if waitFn != nil {
		if err := waitFn(ctx); err != nil {
			return 0, err
		}
	}
	return commitSeq, nil
}

func (v *Volume) Rmdir(ctx context.Context, parentInodeID uint64, name string) (uint64, error) {
	if len(name) > MaxNameLength {
		return 0, fmt.Errorf("directory name %q exceeds maximum length: %w", name, syscall.ENAMETOOLONG)
	}

	waitFn, commitSeq, err := func() (func(context.Context) error, uint64, error) {
		if err := v.rejectMutation(); err != nil {
			return nil, 0, err
		}
		v.mu.Lock()
		defer v.mu.Unlock()

		tx := v.beginTxLocked("Rmdir")

		if err := v.checkBackpressureLocked(ctx); err != nil {
			return nil, 0, err
		}

		parentInodeID = v.normalizeInodeID(parentInodeID)

		var childInodeID uint64
		de, ok, err := v.getDirEntrySQLiteLocked(ctx, tx, parentInodeID, name)
		if err != nil {
			return nil, 0, err
		}
		if !ok {
			return nil, 0, fmt.Errorf("directory %s not found under inode %d: %w", name, parentInodeID, syscall.ENOENT)
		}
		childInodeID = de.GetIno()
		childInode, err := v.getOrLoadInodeLocked(ctx, tx, childInodeID)
		if err != nil {
			return nil, 0, err
		}
		if !childInode.Row.GetIsDir() {
			return nil, 0, fmt.Errorf("cannot rmdir non-directory %s: %w", name, syscall.ENOTDIR)
		}

		// Emptiness check: scan child directory with limit 1
		prefixBytes, pErr := sds.EncodeKeyPrefix(&pb.DirEntry{ParentIno: proto.Uint64(childInodeID)}, 1)
		if pErr != nil {
			return nil, 0, pErr
		}
		subEntries, err := v.scanLimitSQLiteRowsLocked(ctx, tx, "objectfs.v1alpha1.DirEntry", prefixBytes, 1)
		if err != nil {
			return nil, 0, err
		}
		if len(subEntries) > 0 {
			return nil, 0, fmt.Errorf("directory %s not empty: %w", name, syscall.ENOTEMPTY)
		}

		delete(v.dirParents, childInodeID)

		now := time.Now()
		parentInode, err := v.getOrLoadInodeLocked(ctx, tx, parentInodeID)
		if err != nil {
			return nil, 0, err
		}
		parentInode.mutate(func(row *pb.Inode) {
			if row.GetNlink() > 2 {
				row.Nlink--
			}
			row.Mtime = timestamppb.New(now)
			row.Ctime = timestamppb.New(now)
		})

		if err := v.deleteInodeRowsLocked(ctx, tx, childInodeID); err != nil {
			return nil, 0, err
		}
		var commitSeq uint64
		if _, err = tx.Delete(ctx, &pb.DirEntry{ParentIno: proto.Uint64(parentInodeID), Name: proto.String(name)}); err != nil {
			return nil, 0, fmt.Errorf("failed to log dir entry deletion: %w", err)
		}
		if parentInode != nil {
			if _, err = tx.Update(ctx, parentInode.Row); err != nil {
				return nil, 0, fmt.Errorf("failed to log parent inode update: %w", err)
			}
		}
		commitSeq, err = v.commitTxLocked(ctx, tx)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to commit transaction: %w", err)
		}

		waitFn := v.makeWaitFn(commitSeq, nil)

		v.broadcaster.Broadcast(v.volumeID, &pb.WatchVolumeResponse{
			EventType:      pb.WatchEventType_EVENT_DELETED,
			Inode:          childInodeID,
			ParentInode:    parentInodeID,
			Name:           name,
			StreamPosition: commitSeq,
		})

		return waitFn, commitSeq, nil
	}()
	if err != nil {
		return 0, err
	}
	if waitFn != nil {
		if err := waitFn(ctx); err != nil {
			return 0, err
		}
	}
	return commitSeq, nil
}

func (v *Volume) Rename(ctx context.Context, oldParentInodeID uint64, oldName string, newParentInodeID uint64, newName string) (*pb.EntryAttr, error) {
	if len(oldName) > MaxNameLength || len(newName) > MaxNameLength {
		return nil, fmt.Errorf("name exceeds maximum length: %w", syscall.ENAMETOOLONG)
	}

	attr, waitFn, err := func() (*pb.EntryAttr, func(context.Context) error, error) {
		if err := v.rejectMutation(); err != nil {
			return nil, nil, err
		}
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

		oldParentInode, err := v.getOrLoadInodeLocked(ctx, tx, oldParentInodeID)
		if err != nil {
			return nil, nil, err
		}

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
		commitSeq, err = v.commitTxLocked(ctx, tx)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to commit rename transaction: %w", err)
		}

		waitFn := v.makeWaitFn(commitSeq, nil)

		attr := v.toEntryAttr(childInode.Row, newName, childInode.RedirectURL)
		v.broadcaster.Broadcast(v.volumeID, &pb.WatchVolumeResponse{
			EventType:      pb.WatchEventType_EVENT_RENAMED,
			Attr:           attr,
			Inode:          childInode.Row.GetIno(),
			OldParentInode: oldParentInodeID,
			OldName:        oldName,
			ParentInode:    newParentInodeID,
			Name:           newName,
			StreamPosition: commitSeq,
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

func (v *Volume) openBarrier(ctx context.Context, inodeID uint64) error {
	if deg := v.Degraded(); deg != nil {
		return syscall.EIO
	}

	timeout := v.openBarrierTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	barrierCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	v.stagingMu.Lock()
	defer v.stagingMu.Unlock()

	stopTimer := context.AfterFunc(barrierCtx, func() {
		v.stagingMu.Lock()
		v.syncCond.Broadcast()
		v.stagingMu.Unlock()
	})
	defer stopTimer()

	barrierStartTime := time.Now()
	var waitReason string

	for {
		if deg := v.Degraded(); deg != nil {
			return syscall.EIO
		}
		if barrierCtx.Err() != nil {
			reason := waitReason
			if reason == "" {
				reason = "context_timeout"
			}
			klog.Warningf("ObjectFS volume %s inode %d open barrier timed out after %v waiting on %s", v.volumeID, inodeID, time.Since(barrierStartTime), reason)
			return syscall.EIO
		}

		hasStaged := len(v.stagedChunks[inodeID]) > 0
		inFlight := false
		if v.currentSync != nil {
			_, inFlight = v.currentSync.syncingInodes[inodeID]
		}

		if !hasStaged && !inFlight {
			if waitReason != "" {
				klog.Infof("ObjectFS volume %s inode %d open barrier satisfied in %v (waited on %s)", v.volumeID, inodeID, time.Since(barrierStartTime), waitReason)
			}
			return nil
		}

		if inFlight || v.currentSync != nil {
			if inFlight {
				waitReason = "in_flight_inode_sync"
			} else {
				waitReason = "in_flight_volume_sync"
			}
			v.syncCond.Wait()
			continue
		}

		trigger := pb.FsyncRequest_SYNC_TRIGGER_UNSPECIFIED
		if v.closeInodes != nil {
			if _, ok := v.closeInodes[inodeID]; ok {
				trigger = pb.FsyncRequest_SYNC_TRIGGER_CLOSE
			}
			delete(v.closeInodes, inodeID)
			if len(v.closeInodes) == 0 && v.closeTimer != nil {
				v.closeTimer.Stop()
				v.closeTimer = nil
			}
		}

		waitReason = "staged_inode_sync"
		_, err := v.syncInodesLocked(barrierCtx, map[uint64]struct{}{inodeID: {}}, trigger)
		v.stagingMu.Lock()
		if err != nil {
			klog.Warningf("ObjectFS volume %s inode %d open barrier sync failed after %v: %v", v.volumeID, inodeID, time.Since(barrierStartTime), err)
			return syscall.EIO
		}
		if deg := v.Degraded(); deg != nil {
			return syscall.EIO
		}
		if barrierCtx.Err() != nil {
			klog.Warningf("ObjectFS volume %s inode %d open barrier timed out after %v waiting on %s", v.volumeID, inodeID, time.Since(barrierStartTime), waitReason)
			return syscall.EIO
		}
		klog.Infof("ObjectFS volume %s inode %d open barrier satisfied in %v (executed %s)", v.volumeID, inodeID, time.Since(barrierStartTime), waitReason)
		return nil
	}
}

func (v *Volume) Open(ctx context.Context, inodeID uint64, flags uint32) (uint64, *pb.EntryAttr, uint64, error) {
	v.mu.RLock()
	inodeID = v.normalizeInodeID(inodeID)
	isCloseToOpen := v.consistencyMode == pb.ConsistencyMode_CLOSE_TO_OPEN
	v.mu.RUnlock()

	if isCloseToOpen {
		if err := v.openBarrier(ctx, inodeID); err != nil {
			return 0, nil, 0, err
		}
	}

	v.mu.RLock()
	node, err := v.getOrLoadInodeLocked(ctx, nil, inodeID)
	if err != nil {
		v.mu.RUnlock()
		return 0, nil, 0, err
	}
	if node == nil {
		v.mu.RUnlock()
		return 0, nil, 0, syscall.ENOENT
	}
	attr := v.toEntryAttr(node.Row, "", node.RedirectURL)
	pos := v.lastCommitSeq
	v.mu.RUnlock()

	fh := v.AllocFh(inodeID)
	return fh, attr, pos, nil
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
		if err := v.deleteInodeRowsLocked(ctx, tx, inodeID); err != nil {
			return err
		}
		commitSeq, err := v.commitTxLocked(ctx, tx)
		if err != nil {
			return fmt.Errorf("failed to commit orphan deletion transaction: %w", err)
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

func (v *Volume) flushToBackendLocked(ctx context.Context) error {
	if v.backend == nil {
		return nil
	}

	if v.stream.ReplicationLevel() == walclient.Permanent {
		flushCtx := ctx
		if _, hasDeadline := ctx.Deadline(); !hasDeadline {
			var cancel context.CancelFunc
			flushTimeout := v.syncTimeout
			if flushTimeout <= 0 {
				flushTimeout = 10 * time.Minute
			}
			flushCtx, cancel = context.WithTimeout(ctx, flushTimeout)
			defer cancel()
		}
		if err := v.stream.Flush(flushCtx); err != nil {
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
			errutil.CloseAndLog(streamObj, "stream id object")
			return fmt.Errorf("failed to save volume marker: %w", err)
		}
		errutil.CloseAndLog(streamObj, "stream id object")
	}

	return nil
}

func (v *Volume) FlushToBackend(ctx context.Context) error {
	v.stagingMu.Lock()
	if err := v.rejectMutationLocked(); err != nil {
		v.stagingMu.Unlock()
		return err
	}
	v.waitForSyncIdleLocked()
	if len(v.stagedChunks) > 0 {
		if _, err := v.executeSyncLocked(ctx, pb.FsyncRequest_SYNC_TRIGGER_UNSPECIFIED, false, nil); err != nil {
			return err
		}
	} else {
		v.stagingMu.Unlock()
	}

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
		if change.TypeName == InodeTypeName {
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
	v.batchCheckMu.Lock()
	v.batchCheckActive++
	v.batchCheckMu.Unlock()

	go func() {
		defer func() {
			v.batchCheckMu.Lock()
			v.batchCheckActive--
			if v.batchCheckActive == 0 {
				v.batchCheckCond.Broadcast()
			}
			v.batchCheckMu.Unlock()
		}()
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

func (v *Volume) waitBatchChecks() {
	v.batchCheckMu.Lock()
	for v.batchCheckActive > 0 {
		v.batchCheckCond.Wait()
	}
	v.batchCheckMu.Unlock()
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

func (v *Volume) rebuildRestoreSnapshot(ctx context.Context, oldIdx sds.LocalIndex, ptr *sdsv1.SnapshotPointer, seq uint64, highestReplayedIno uint64) (sds.LocalIndex, error) {
	if oldIdx != nil {
		statsPK := sds.NewKeyFromBytes([]byte(VolumeStatsRowName))
		if msg, ok, sErr := oldIdx.Get(ctx, VolumeStatsTypeName, statsPK); sErr == nil && ok && msg != nil {
			if vs, ok := msg.(*pb.VolumeStats); ok && vs != nil && vs.GetMaxIno() > highestReplayedIno {
				highestReplayedIno = vs.GetMaxIno()
			}
		}
		if err := oldIdx.Close(); err != nil {
			return nil, fmt.Errorf("failed to close previous index during rebuild: %w", err)
		}
	}
	restoredIdx, _, err := v.indexFactory.RestoreSnapshot(ctx, v.backend, v.streamID.String(), ptr.GetRestoredFrom(), v.localStorageDir)
	if err != nil {
		return nil, fmt.Errorf("failed to restore snapshot %q during rebuild: %w", ptr.GetRestoredFrom(), err)
	}
	if err := restoredIdx.SetPosition(seq); err != nil {
		if closeErr := restoredIdx.Close(); closeErr != nil {
			return nil, fmt.Errorf("failed to set position (%v) and close failed: %w", err, closeErr)
		}
		return nil, fmt.Errorf("failed to set position on restored index during rebuild: %w", err)
	}
	if highestReplayedIno > 0 {
		statsPK := sds.NewKeyFromBytes([]byte(VolumeStatsRowName))
		if msg, ok, sErr := restoredIdx.Get(ctx, VolumeStatsTypeName, statsPK); sErr == nil && ok && msg != nil {
			if vs, ok := msg.(*pb.VolumeStats); ok && vs != nil && highestReplayedIno > vs.GetMaxIno() {
				updatedStats := proto.Clone(vs).(*pb.VolumeStats)
				updatedStats.MaxIno = highestReplayedIno
				if v.metadataStream != nil {
					if def, _, ok := v.metadataStream.Registry().LookupByName(VolumeStatsTypeName); ok {
						kBytes, vBytes, err := sds.SplitKeyAndNonKey(updatedStats, def.GetKeyFields())
						if err != nil {
							if closeErr := restoredIdx.Close(); closeErr != nil {
								return nil, fmt.Errorf("failed to split stats row (%v) and close failed: %w", err, closeErr)
							}
							return nil, fmt.Errorf("failed to split stats row: %w", err)
						}
						if err := restoredIdx.ApplyBatch(ctx, []sds.Change{
							{
								Seq:      seq,
								TypeName: VolumeStatsTypeName,
								TypeID:   def.GetId(),
								Op:       sds.OpUpdate,
								Key:      sds.NewKeyFromBytes(kBytes),
								RawKey:   kBytes,
								RawVal:   vBytes,
								Row:      updatedStats,
							},
						}); err != nil {
							if closeErr := restoredIdx.Close(); closeErr != nil {
								return nil, fmt.Errorf("failed to update stats (%v) and close failed: %w", err, closeErr)
							}
							return nil, fmt.Errorf("failed to update stats in restored index: %w", err)
						}
					}
				}
			}
		}
	}
	if err := v.importIndexRegistryLocked(restoredIdx); err != nil {
		if closeErr := restoredIdx.Close(); closeErr != nil {
			return nil, fmt.Errorf("failed to import snapshot registry (%v) and close failed: %w", err, closeErr)
		}
		return nil, err
	}
	if err := v.registerMetadataTypesLocked(ctx, restoredIdx); err != nil {
		if closeErr := restoredIdx.Close(); closeErr != nil {
			return nil, fmt.Errorf("failed to register metadata types (%v) and close failed: %w", err, closeErr)
		}
		return nil, err
	}
	return restoredIdx, nil
}

func (v *Volume) applyRestoreRecordLocked(ctx context.Context, ptr *sdsv1.SnapshotPointer, seq uint64, highestReplayedIno *uint64) error {
	var curHigh uint64
	if highestReplayedIno != nil {
		curHigh = *highestReplayedIno
	}
	var oldIdx sds.LocalIndex
	if v.metadataView != nil {
		oldIdx = v.metadataView.Index()
		if err := v.metadataView.Close(); err != nil {
			return fmt.Errorf("failed to close metadata view during replay restore: %w", err)
		}
		v.metadataView = nil
	}
	restoredIdx, err := v.rebuildRestoreSnapshot(ctx, oldIdx, ptr, seq, curHigh)
	if err != nil {
		return err
	}
	if err := v.initMetadataViewLocked(ctx, restoredIdx); err != nil {
		return fmt.Errorf("failed to init metadata view from replay restored snapshot: %w", err)
	}
	v.replayStartPos = ptr.GetPosition()
	v.replaySnapshotLocation = ptr.GetRestoredFrom()
	v.lastCommitSeq = seq
	v.dirParents = make(map[uint64]uint64)
	v.dirParents[1] = 1
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
		rebuiltIdx  sds.LocalIndex
		snapPos     uint64
		snapshotKey string
		err         error
	)

	// 1. Try to restore latest snapshot from object storage
	if v.backend != nil {
		snapKey, _, err := v.indexFactory.FindLatestSnapshot(ctx, v.backend, v.streamID.String(), 0)
		if err == nil && snapKey != "" {
			restoredIdx, rPos, err := v.indexFactory.RestoreSnapshot(ctx, v.backend, v.streamID.String(), snapKey, v.localStorageDir)
			if err == nil && restoredIdx != nil {
				rebuiltIdx = restoredIdx
				snapPos = rPos
				snapshotKey = snapKey
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
	v.replayStartPos = snapPos
	if v.backend != nil && snapPos > 0 {
		v.replaySnapshotLocation = snapshotKey
	}

	// 3. Sync registry
	if v.metadataStream != nil {
		if err := rebuiltIdx.SyncRegistry(ctx, v.metadataStream.Registry()); err != nil {
			errutil.CloseAndLog(rebuiltIdx, "rebuilt index")
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
		keyBytes, valBytes, err := sds.SplitKeyAndNonKey(rootInodeMsg, []int32{1})
		if err != nil {
			if closeErr := rebuiltIdx.Close(); closeErr != nil {
				return nil, 0, fmt.Errorf("failed to split key for root inode in rebuilt index (%v) and close failed: %w", err, closeErr)
			}
			return nil, 0, fmt.Errorf("failed to split key for root inode in rebuilt index: %w", err)
		}
		initChanges := []sds.Change{
			{
				Seq:      0,
				TypeName: InodeTypeName,
				Op:       sds.OpCreate,
				Key:      sds.NewKeyFromBytes(keyBytes),
				RawKey:   keyBytes,
				RawVal:   valBytes,
				Row:      rootInodeMsg,
			},
		}
		if err := rebuiltIdx.ApplyBatch(ctx, initChanges); err != nil {
			errutil.CloseAndLog(rebuiltIdx, "rebuilt index")
			return nil, 0, fmt.Errorf("failed to init root inode in rebuilt index: %w", err)
		}
	}

	// 5. Replay the stream from snapPos up to current position.
	var highestReplayedIno uint64
	cr := sds.NewChangeReader(record.WithDecoderRegistry(v.metadataStream.Registry()))
	cr.SetAcceptUnversioned(v.acceptUnversioned)
	dec := record.NewDecoder(record.WithDecoderRegistry(v.metadataStream.Registry()))
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
			if ch.TypeName == InodeTypeName {
				if in, ok := ch.Row.(*pb.Inode); ok && in != nil && in.GetIno() > highestReplayedIno {
					highestReplayedIno = in.GetIno()
				}
			}
		}
		return nil
	}

	localHead, witnessHead, _ := v.stream.Watermarks()
	targetHead := witnessHead
	if witnessHead == 0 && (v.stream.ReplicationLevel() == walclient.Local || len(v.stream.RecoveredRecords()) == 0) {
		targetHead = localHead
	}

	// 5a. Authoritative tail from stream up to targetHead
	if targetHead > snapPos {
		tailIter, err := v.stream.Tail(ctx, snapPos)
		if err != nil {
			errutil.CloseAndLog(rebuiltIdx, "rebuilt index")
			return nil, 0, fmt.Errorf("failed to tail stream during rebuild: %w", err)
		}
		for seq, payload := range tailIter {
			if seq <= snapPos || seenSeqs[seq] {
				continue
			}
			if seq != expectedSeq {
				errutil.CloseAndLog(rebuiltIdx, "rebuilt index")
				return nil, 0, fmt.Errorf("stream contiguity violation in stream tail during rebuild: expected seq %d, got %d", expectedSeq, seq)
			}
			expectedSeq++
			seenSeqs[seq] = true
			rec, decErr := dec.Decode(payload)
			if decErr == nil && rec.TypeID == record.TypeIDSnapshotPointer {
				if ptr, ok := rec.Message.(*sdsv1.SnapshotPointer); ok && ptr != nil && ptr.GetRestoredFrom() != "" {
					newIdx, err := v.rebuildRestoreSnapshot(ctx, rebuiltIdx, ptr, seq, highestReplayedIno)
					if err != nil {
						return nil, 0, err
					}
					rebuiltIdx = newIdx
					snapPos = seq
					cr.DiscardPending()
					if seq >= targetHead {
						break
					}
					continue
				}
			}
			changes, err := cr.Feed(seq, payload)
			if err != nil {
				errutil.CloseAndLog(rebuiltIdx, "rebuilt index")
				return nil, 0, fmt.Errorf("failed to feed stream record at seq %d during rebuild: %w", seq, err)
			}
			if err := applyChanges(changes); err != nil {
				errutil.CloseAndLog(rebuiltIdx, "rebuilt index")
				return nil, 0, err
			}
			if seq >= targetHead {
				break
			}
		}
		if expectedSeq-1 < targetHead {
			errutil.CloseAndLog(rebuiltIdx, "rebuilt index")
			return nil, 0, fmt.Errorf("failed to rebuild stream: expected head %d but only reached %d", targetHead, expectedSeq-1)
		}
	}

	// 5b. Local unacknowledged records > witnessHead
	recovered := v.stream.RecoveredRecords()
	for _, rec := range recovered {
		if rec.StreamSeq <= snapPos || rec.StreamSeq <= witnessHead || seenSeqs[rec.StreamSeq] {
			continue
		}
		if rec.StreamSeq != expectedSeq {
			errutil.CloseAndLog(rebuiltIdx, "rebuilt index")
			return nil, 0, fmt.Errorf("stream contiguity violation in local records during rebuild: expected seq %d, got %d", expectedSeq, rec.StreamSeq)
		}
		expectedSeq++
		seenSeqs[rec.StreamSeq] = true
		r, decErr := dec.Decode(rec.Payload)
		if decErr == nil && r.TypeID == record.TypeIDSnapshotPointer {
			if ptr, ok := r.Message.(*sdsv1.SnapshotPointer); ok && ptr != nil && ptr.GetRestoredFrom() != "" {
				newIdx, err := v.rebuildRestoreSnapshot(ctx, rebuiltIdx, ptr, rec.StreamSeq, highestReplayedIno)
				if err != nil {
					return nil, 0, err
				}
				rebuiltIdx = newIdx
				snapPos = rec.StreamSeq
				cr.DiscardPending()
				continue
			}
		}
		changes, err := cr.Feed(rec.StreamSeq, rec.Payload)
		if err != nil {
			errutil.CloseAndLog(rebuiltIdx, "rebuilt index")
			return nil, 0, fmt.Errorf("failed to feed local record at seq %d during rebuild: %w", rec.StreamSeq, err)
		}
		if err := applyChanges(changes); err != nil {
			errutil.CloseAndLog(rebuiltIdx, "rebuilt index")
			return nil, 0, err
		}
	}

	cr.DiscardPending()

	if err := v.registerMetadataTypesLocked(ctx, rebuiltIdx); err != nil {
		errutil.CloseAndLog(rebuiltIdx, "rebuilt index")
		return nil, 0, fmt.Errorf("failed to register metadata types after rebuild replay: %w", err)
	}

	// 6. Ensure stats row exists or backfill stats in the rebuilt index
	statsPK := sds.NewKeyFromBytes([]byte(VolumeStatsRowName))
	if msg, ok, sErr := rebuiltIdx.Get(ctx, VolumeStatsTypeName, statsPK); sErr == nil && ok && msg != nil {
		if vs, ok := msg.(*pb.VolumeStats); ok && vs != nil && highestReplayedIno > vs.GetMaxIno() {
			updatedStats := proto.Clone(vs).(*pb.VolumeStats)
			updatedStats.MaxIno = highestReplayedIno
			if def, _, ok := v.metadataStream.Registry().LookupByName(VolumeStatsTypeName); ok {
				kBytes, vBytes, err := sds.SplitKeyAndNonKey(updatedStats, def.GetKeyFields())
				if err != nil {
					if closeErr := rebuiltIdx.Close(); closeErr != nil {
						return nil, 0, fmt.Errorf("failed to split updated stats row (%v) and close failed: %w", err, closeErr)
					}
					return nil, 0, fmt.Errorf("failed to split updated stats row: %w", err)
				}
				if err := rebuiltIdx.ApplyBatch(ctx, []sds.Change{
					{
						Seq:      snapPos,
						TypeName: VolumeStatsTypeName,
						TypeID:   def.GetId(),
						Op:       sds.OpUpdate,
						Key:      sds.NewKeyFromBytes(kBytes),
						RawKey:   kBytes,
						RawVal:   vBytes,
						Row:      updatedStats,
					},
				}); err != nil {
					if closeErr := rebuiltIdx.Close(); closeErr != nil {
						return nil, 0, fmt.Errorf("failed to update stats in rebuilt index (%v) and close failed: %w", err, closeErr)
					}
					return nil, 0, fmt.Errorf("failed to update stats in rebuilt index: %w", err)
				}
			}
		}
	} else if sErr == nil && !ok {
		if stats, cErr := v.countStatsFromIndex(ctx, rebuiltIdx); cErr == nil && stats != nil {
			if def, _, ok := v.metadataStream.Registry().LookupByName(VolumeStatsTypeName); ok {
				kBytes, vBytes, err := sds.SplitKeyAndNonKey(stats, def.GetKeyFields())
				if err != nil {
					if closeErr := rebuiltIdx.Close(); closeErr != nil {
						return nil, 0, fmt.Errorf("failed to split backfilled stats row (%v) and close failed: %w", err, closeErr)
					}
					return nil, 0, fmt.Errorf("failed to split backfilled stats row: %w", err)
				}
				if err := rebuiltIdx.ApplyBatch(ctx, []sds.Change{
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
				}); err != nil {
					if closeErr := rebuiltIdx.Close(); closeErr != nil {
						return nil, 0, fmt.Errorf("failed to apply backfilled stats (%v) and close failed: %w", err, closeErr)
					}
					return nil, 0, fmt.Errorf("failed to apply backfilled stats: %w", err)
				}
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
	isRemoteWAL := v.stream.ReplicationLevel() > walclient.Local
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
					errutil.CloseAndLog(localIdx, "stale local index")
				}
			}
		}
	}

	// 2. Otherwise restore latest published snapshot
	if !initialized && remoteSnapKey != "" && v.backend != nil {
		if v.metadataView != nil {
			errutil.CloseAndLog(v.metadataView, "metadata view")
			v.metadataView = nil
		}
		restoredIdx, rPos, err := v.indexFactory.RestoreSnapshot(ctx, v.backend, v.streamID.String(), remoteSnapKey, v.localStorageDir)
		if err == nil && restoredIdx != nil {
			if err := v.importIndexRegistryLocked(restoredIdx); err != nil {
				errutil.CloseAndLog(restoredIdx, "restored index")
				return err
			}
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
				errutil.CloseAndLog(v.metadataView, "metadata view")
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
	}

	if snapPos > v.lastCommitSeq {
		v.lastCommitSeq = snapPos
	}
	v.replayStartPos = snapPos
	v.replaySnapshotLocation = remoteSnapKey

	// Replay stream from snapPos
	var highestReplayedIno uint64
	cr := sds.NewChangeReader(record.WithDecoderRegistry(v.metadataStream.Registry()))
	cr.SetAcceptUnversioned(v.acceptUnversioned)
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
			if ch.TypeName == VolumeInfoTypeName {
				if vi, ok := ch.Row.(*pb.VolumeInfo); ok && vi != nil && vi.GetConsistencyMode() != pb.ConsistencyMode_CONSISTENCY_MODE_UNSPECIFIED {
					v.consistencyMode = vi.GetConsistencyMode()
				}
			}
		}
		return nil
	}

	localHead, witnessHead, _ := v.stream.Watermarks()
	targetHead := witnessHead
	if witnessHead == 0 && (v.stream.ReplicationLevel() == walclient.Local || len(v.stream.RecoveredRecords()) == 0) {
		targetHead = localHead
	}
	dec := record.NewDecoder(record.WithDecoderRegistry(v.metadataStream.Registry()))

	// 1. Authoritative tail from stream up to targetHead
	if targetHead > snapPos {
		tailIter, err := v.stream.Tail(ctx, snapPos)
		if err != nil {
			return fmt.Errorf("failed to tail stream: %w", err)
		}
		for seq, payload := range tailIter {
			if seq <= snapPos || seenSeqs[seq] {
				continue
			}
			if seq != expectedSeq {
				return fmt.Errorf("stream contiguity violation in stream tail: expected seq %d, got %d", expectedSeq, seq)
			}
			expectedSeq++
			seenSeqs[seq] = true
			rec, decErr := dec.Decode(payload)
			if decErr != nil {
				klog.Warningf("volume %s: failed to decode authoritative record at seq %d: %v", v.volumeID, seq, decErr)
			} else if rec.TypeID == record.TypeIDTypeDefinition {
				if v.metadataView != nil && v.metadataStream != nil {
					if err := v.metadataView.SyncRegistry(ctx, v.metadataStream.Registry()); err != nil {
						return fmt.Errorf("failed to sync registry with metadata view during replay: %w", err)
					}
				}
			} else if rec.TypeID == record.TypeIDSnapshotPointer {
				if ptr, ok := rec.Message.(*sdsv1.SnapshotPointer); ok && ptr != nil {
					if ptr.GetRestoredFrom() != "" {
						if err := v.applyRestoreRecordLocked(ctx, ptr, seq, &highestReplayedIno); err != nil {
							return err
						}
						cr.DiscardPending()
						if seq >= targetHead {
							break
						}
						continue
					}
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
			if seq >= targetHead {
				break
			}
		}
		if expectedSeq-1 < targetHead {
			return fmt.Errorf("failed to recover stream: expected head %d but only reached %d", targetHead, expectedSeq-1)
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
		} else if r.TypeID == record.TypeIDTypeDefinition {
			if v.metadataView != nil && v.metadataStream != nil {
				if err := v.metadataView.SyncRegistry(ctx, v.metadataStream.Registry()); err != nil {
					return fmt.Errorf("failed to sync registry with metadata view during replay: %w", err)
				}
			}
		} else if r.TypeID == record.TypeIDSnapshotPointer {
			if ptr, ok := r.Message.(*sdsv1.SnapshotPointer); ok && ptr != nil {
				if ptr.GetRestoredFrom() != "" {
					if err := v.applyRestoreRecordLocked(ctx, ptr, rec.StreamSeq, &highestReplayedIno); err != nil {
						return err
					}
					cr.DiscardPending()
					continue
				}
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

	if err := v.registerMetadataTypesLocked(ctx, v.metadataView); err != nil {
		return fmt.Errorf("failed to register metadata types after replay: %w", err)
	}
	if v.metadataView != nil {
		if err := v.metadataView.LoadStats(ctx); err != nil {
			return fmt.Errorf("failed to load metadata view stats after replay: %w", err)
		}
	}

	if !initialized {
		rootKey, err := pkInode.Extract(&pb.Inode{Ino: proto.Uint64(1)})
		if err != nil {
			return fmt.Errorf("failed to extract root inode key: %w", err)
		}
		msg, ok, err := v.metadataView.Get(ctx, InodeTypeName, rootKey)
		if err != nil {
			return fmt.Errorf("failed to get root inode: %w", err)
		}
		if !ok || msg == nil {
			rootInodeMsg := &pb.Inode{
				Ino:        proto.Uint64(1),
				Mode:       0755 | syscall.S_IFDIR,
				Mtime:      timestamppb.Now(),
				IsDir:      true,
				Nlink:      2,
				XattrCount: proto.Uint32(0),
			}
			keyBytes, valBytes, err := sds.SplitKeyAndNonKey(rootInodeMsg, []int32{1})
			if err != nil {
				return fmt.Errorf("failed to split key for root inode: %w", err)
			}
			initChanges := []sds.Change{
				{
					Seq:      0,
					TypeName: InodeTypeName,
					Op:       sds.OpCreate,
					Key:      sds.NewKeyFromBytes(keyBytes),
					RawKey:   keyBytes,
					RawVal:   valBytes,
					Row:      rootInodeMsg,
				},
			}
			if err := v.metadataView.ApplyChangesSync(ctx, initChanges); err != nil {
				return fmt.Errorf("failed to apply root inode change: %w", err)
			}
		}
		v.rootInodeID = 1
		v.nextInode = erofs.DefaultInodeStride
		v.dirParents[1] = 1
	}

	v.initLiveStatsLocked()

	cleanedCount, err := v.cleanOrphanInodesLocked(ctx)
	if err != nil {
		klog.Warningf("Failed to clean orphan inodes during volume load: %v", err)
	}
	if cleanedCount > 0 {
		if err := v.flushOverlayLocked(ctx); err != nil {
			return fmt.Errorf("failed to flush overlay after clean orphan inodes: %w", err)
		}
	}

	if v.metadataView != nil {
		v.metadataView.ClearCache()
		volInfoKey, err := pkVolumeInfo.Extract(&pb.VolumeInfo{VolumeId: v.volumeID})
		if err != nil {
			return fmt.Errorf("failed to extract VolumeInfo key: %w", err)
		}
		msg, ok, err := v.metadataView.Get(ctx, VolumeInfoTypeName, volInfoKey)
		if err != nil {
			return fmt.Errorf("failed to get VolumeInfo: %w", err)
		}
		if ok && msg != nil {
			if vi, ok := msg.(*pb.VolumeInfo); ok && vi != nil && vi.GetConsistencyMode() != pb.ConsistencyMode_CONSISTENCY_MODE_UNSPECIFIED {
				v.consistencyMode = vi.GetConsistencyMode()
			}
		}
	}

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

// CreateSnapshot creates and returns a new index snapshot of the current volume state,
// waiting until the appended SnapshotPointer record reaches the volume's durability level.
func (v *Volume) CreateSnapshot(ctx context.Context, name string) (string, error) {
	snapshotName, sequence, err := v.writeSnapshotPointer(ctx, name)
	if err != nil {
		return "", err
	}
	// Wait for the SnapshotPointer record's sequence to reach the volume's durability level
	// with no locks held. On wait failure, the pointer is already sequenced and applied
	// in memory, so a retry with the same name appends a second pointer with that name.
	if err := v.waitForSequence(ctx, sequence); err != nil {
		return "", fmt.Errorf("failed to wait for snapshot pointer durability: %w", err)
	}
	return snapshotName, nil
}

// writeSnapshotPointer publishes a snapshot to backend and records its pointer under locks.
// Takes snapshotMu and v.mu.
func (v *Volume) writeSnapshotPointer(ctx context.Context, name string) (string, uint64, error) {
	if err := v.Fsync(ctx, v.rootInodeID); err != nil {
		klog.Warningf("ObjectFS volume %s Fsync before CreateSnapshot: %v", v.volumeID, err)
	}

	v.snapshotMu.Lock()
	defer v.snapshotMu.Unlock()

	v.mu.Lock()
	defer v.mu.Unlock()

	if v.backend == nil {
		return "", 0, fmt.Errorf("no backend configured")
	}

	snap, ok := v.metadataView.Index().(sds.Snapshotter)
	if !ok {
		return "", 0, fmt.Errorf("volume metadata index does not support snapshots")
	}

	if err := v.flushOverlayLocked(ctx); err != nil {
		return "", 0, fmt.Errorf("failed to flush overlay before snapshot: %w", err)
	}

	snapKey, actualPos, err := snap.PublishSnapshot(ctx, v.backend)
	if err != nil {
		return "", 0, fmt.Errorf("failed to publish index snapshot: %w", err)
	}

	format := ""
	if v.indexFactory != nil {
		format = v.indexFactory.Format()
	}

	snapshotName := name
	if snapshotName == "" {
		snapshotName = path.Base(snapKey)
	}

	ptr := &sdsv1.SnapshotPointer{
		Position:  actualPos,
		Format:    format,
		Location:  snapKey,
		Name:      snapshotName,
		CreatedAt: timestamppb.Now(),
	}

	var sequence uint64
	if v.metadataStream != nil && snapKey != "" {
		appendedSequence, err := v.metadataStream.AppendSnapshotPointer(ctx, ptr)
		if err != nil {
			return "", 0, fmt.Errorf("failed to append snapshot pointer: %w", err)
		}
		sequence = appendedSequence
	}
	v.snapshotPointers = append(v.snapshotPointers, ptr)

	return snapshotName, sequence, nil
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
		if ptr.GetRestoredFrom() != "" {
			continue
		}
		pointersByLoc[ptr.GetLocation()] = ptr
	}

	if v.backend != nil && v.indexFactory != nil {
		format := v.indexFactory.Format()
		prefix := fmt.Sprintf("streams/%s/snapshots/%s/", v.streamID.String(), format)
		entries, err := v.backend.ListObjects(ctx, "", prefix)
		if err != nil {
			return nil, fmt.Errorf("failed to list snapshot objects from backend: %w", err)
		}
		existingKeys := make(map[string]bool, len(entries))
		for _, entry := range entries {
			existingKeys[entry.Key] = true
		}
		for location := range pointersByLoc {
			if strings.HasPrefix(location, prefix) && !existingKeys[location] {
				delete(pointersByLoc, location)
			}
		}
		for _, entry := range entries {
			key := entry.Key
			if _, ok := pointersByLoc[key]; !ok {
				var pos uint64
				var parseErr error
				if format == "sqlite" {
					_, pos, parseErr = sqlite.ParseSnapshotKey(key)
				} else if format == "memory" {
					_, pos, parseErr = memtable.ParseSnapshotKey(key)
				} else if format == "table" {
					_, pos, parseErr = table.ParseSnapshotKey(key)
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

// snapshotMatches reports whether pointer matches the specified name by Name, Location,
// or basename of Location.
func snapshotMatches(pointer *sdsv1.SnapshotPointer, name string) bool {
	if pointer == nil || name == "" {
		return false
	}
	return pointer.GetName() == name || pointer.GetLocation() == name || path.Base(pointer.GetLocation()) == name
}

func snapshotInfoFromPointer(pointer *sdsv1.SnapshotPointer) *pb.SnapshotInfo {
	if pointer == nil {
		return nil
	}
	return &pb.SnapshotInfo{
		Name:      pointer.GetName(),
		Position:  pointer.GetPosition(),
		CreatedAt: pointer.GetCreatedAt(),
	}
}

func (v *Volume) getSnapshotInfoLocked(ctx context.Context, snapshotName string) (*pb.SnapshotInfo, error) {
	pointers, err := v.listSnapshotsLocked(ctx)
	if err != nil {
		return nil, err
	}
	for _, pointer := range pointers {
		if snapshotMatches(pointer, snapshotName) {
			return snapshotInfoFromPointer(pointer), nil
		}
	}
	return nil, fmt.Errorf("snapshot %q not found", snapshotName)
}

// ErrSnapshotNotFound indicates that the requested snapshot was not found.
var ErrSnapshotNotFound = errors.New("snapshot not found")

// DeleteSnapshot removes the named snapshot pointer and its backend snapshot object.
// Rules enforced:
// - Never delete the newest snapshot (recovery needs it).
// - Never delete a snapshot the live stream still depends on (the one the current replay starts from).
// - Deletion is idempotent (returns success if the snapshot is not found).
// A failed delete leaves the snapshot listed and the call is safe to repeat.
// Blobs are not touched: they are content-addressed and global, and global GC reclaims them later.
func (v *Volume) DeleteSnapshot(ctx context.Context, name string) error {
	v.snapshotMu.Lock()
	defer v.snapshotMu.Unlock()

	v.mu.Lock()
	defer v.mu.Unlock()

	pointers, err := v.listSnapshotsLocked(ctx)
	if err != nil {
		return fmt.Errorf("failed to list snapshots: %w", err)
	}
	if len(pointers) == 0 {
		return nil
	}

	var targetPointer *sdsv1.SnapshotPointer
	for _, pointer := range pointers {
		if snapshotMatches(pointer, name) {
			targetPointer = pointer
			break
		}
	}
	if targetPointer == nil {
		return nil
	}

	newestPointer := pointers[len(pointers)-1]
	if targetPointer.GetLocation() == newestPointer.GetLocation() {
		return ErrNewestSnapshot
	}

	if (v.replayStartPos > 0 && targetPointer.GetPosition() == v.replayStartPos) ||
		(v.replaySnapshotLocation != "" && targetPointer.GetLocation() == v.replaySnapshotLocation) {
		return ErrSnapshotInUse
	}

	if v.backend != nil && targetPointer.GetLocation() != "" {
		if err := v.backend.DeleteObject(ctx, "", targetPointer.GetLocation()); err != nil {
			return fmt.Errorf("failed to delete snapshot object %s: %w", targetPointer.GetLocation(), err)
		}
	}

	var remainingPointers []*sdsv1.SnapshotPointer
	for _, pointer := range v.snapshotPointers {
		if pointer.GetLocation() == targetPointer.GetLocation() {
			continue
		}
		remainingPointers = append(remainingPointers, pointer)
	}
	v.snapshotPointers = remainingPointers

	return nil
}

// ErrVolumeAlreadyExists indicates that the volume already has durable state.
var ErrVolumeAlreadyExists = errors.New("volume already exists")

// RestoreSnapshot restores the volume filesystem state to a specific snapshot,
// waiting until the appended restore record reaches the volume's durability level.
// A restore is appended to the stream as a Restore record (SnapshotPointer with restored_from set)
// at the head (H+1). The index is restored from the snapshot, its position set to H+1, stats and
// allocator re-initialized (with inode numbers monotonic across the restore), and a fresh index
// snapshot published at H+1 so recovery has a snapshot that already reflects the restore.
// Returns the published snapshot info on success.
func (v *Volume) RestoreSnapshot(ctx context.Context, snapshotName string) (*pb.SnapshotInfo, error) {
	snapshotInfo, restoreSequence, err := v.writeRestoreRecord(ctx, snapshotName)
	if err != nil {
		return nil, err
	}
	// Wait for the restore record's sequence to reach the volume's durability level
	// with no locks held. On wait failure, the restore has already occurred in memory
	// and published a fresh snapshot, so a retry will perform the restore again.
	if err := v.waitForSequence(ctx, restoreSequence); err != nil {
		return nil, fmt.Errorf("failed to wait for restore record durability: %w", err)
	}
	return snapshotInfo, nil
}

// writeRestoreRecord restores the filesystem state under locks and returns the new snapshot info and restore sequence.
// Takes stagingMu (temporarily), snapshotMu, and v.mu.
func (v *Volume) writeRestoreRecord(ctx context.Context, snapshotName string) (*pb.SnapshotInfo, uint64, error) {
	v.stagingMu.Lock()
	v.waitForSyncIdleLocked()
	if err := v.resetStagingDirLocked(); err != nil {
		v.stagingMu.Unlock()
		return nil, 0, err
	}
	v.stagedChunks = make(map[uint64]map[int]stagedChunkReference)
	v.stagedSizes = make(map[uint64]int64)
	v.stagedMtimes = make(map[uint64]time.Time)
	v.stagedBytes = 0
	v.stagingMu.Unlock()

	v.snapshotMu.Lock()
	defer v.snapshotMu.Unlock()

	v.mu.Lock()
	defer v.mu.Unlock()

	if v.backend == nil {
		return nil, 0, fmt.Errorf("no backend configured")
	}
	if v.indexFactory == nil {
		return nil, 0, fmt.Errorf("no index factory configured")
	}

	pointers, err := v.listSnapshotsLocked(ctx)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to list snapshots: %w", err)
	}

	var targetPtr *sdsv1.SnapshotPointer
	for _, p := range pointers {
		if snapshotMatches(p, snapshotName) {
			targetPtr = p
			break
		}
	}
	if targetPtr == nil {
		return nil, 0, fmt.Errorf("%w: %q", ErrSnapshotNotFound, snapshotName)
	}

	if err := v.flushOverlayLocked(ctx); err != nil {
		return nil, 0, fmt.Errorf("failed to flush overlay before restore: %w", err)
	}

	var preRestoreMaxIno uint64
	if v.liveStats != nil {
		preRestoreMaxIno = v.liveStats.GetMaxIno()
	}
	if v.nextInode > preRestoreMaxIno {
		preRestoreMaxIno = v.nextInode
	}

	restorePtr := &sdsv1.SnapshotPointer{
		Position:     targetPtr.GetPosition(),
		Format:       targetPtr.GetFormat(),
		Location:     targetPtr.GetLocation(),
		Name:         snapshotName,
		CreatedAt:    timestamppb.Now(),
		RestoredFrom: targetPtr.GetLocation(),
	}

	var restoreSeq uint64
	if v.metadataStream != nil {
		seq, err := v.metadataStream.AppendSnapshotPointer(ctx, restorePtr)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to append restore record: %w", err)
		}
		restoreSeq = seq
	} else {
		restoreSeq = v.lastCommitSeq + 1
	}
	v.lastCommitSeq = restoreSeq

	if v.stream.ReplicationLevel() == walclient.Permanent {
		if err := v.stream.Flush(ctx); err != nil {
			return nil, 0, fmt.Errorf("failed to flush WAL stream after restore: %w", err)
		}
	}

	var oldIdx sds.LocalIndex
	if v.metadataView != nil {
		oldIdx = v.metadataView.Index()
		if err := v.metadataView.Close(); err != nil {
			return nil, 0, fmt.Errorf("failed to close metadata view: %w", err)
		}
		v.metadataView = nil
	}

	restoredIdx, err := v.rebuildRestoreSnapshot(ctx, oldIdx, restorePtr, restoreSeq, preRestoreMaxIno)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to restore snapshot %q: %w", snapshotName, err)
	}

	if err := v.initMetadataViewLocked(ctx, restoredIdx); err != nil {
		return nil, 0, fmt.Errorf("failed to init metadata view from restored snapshot: %w", err)
	}
	v.lastCommitSeq = restoreSeq

	v.metadataView.ClearCache()
	v.dirParents = make(map[uint64]uint64)
	v.dirParents[1] = 1
	// Active file handles (v.handles) and open inode refcounts (v.openInodes)
	// are retained across snapshot restore because they refer to the running clients.
	v.initLiveStatsLocked()
	if err := v.initNextInodeFromStatsLocked(ctx, preRestoreMaxIno); err != nil {
		return nil, 0, fmt.Errorf("failed to initialize next inode from stats: %w", err)
	}
	v.assertStatsMatchLocked("after RestoreSnapshot")

	snap, ok := v.metadataView.Index().(sds.Snapshotter)
	if !ok {
		return nil, 0, fmt.Errorf("volume metadata index does not support snapshots")
	}

	freshKey, freshPos, err := snap.PublishSnapshot(ctx, v.backend)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to publish fresh index snapshot after restore: %w", err)
	}

	format := ""
	if v.indexFactory != nil {
		format = v.indexFactory.Format()
	}

	freshPtr := &sdsv1.SnapshotPointer{
		Position:  freshPos,
		Format:    format,
		Location:  freshKey,
		Name:      path.Base(freshKey),
		CreatedAt: timestamppb.Now(),
	}
	v.replayStartPos = freshPos
	v.replaySnapshotLocation = freshKey
	v.snapshotPointers = append(v.snapshotPointers, restorePtr, freshPtr)

	return snapshotInfoFromPointer(freshPtr), restoreSeq, nil
}

// InitFromSnapshot initializes the volume with a fresh metadata stream whose initial state
// is loaded from the specified source snapshot, and publishes the new volume's first snapshot.
func (v *Volume) InitFromSnapshot(ctx context.Context, sourceVolumeID, snapshotName, snapshotLocation string) error {
	v.stagingMu.Lock()
	v.waitForSyncIdleLocked()
	if err := v.resetStagingDirLocked(); err != nil {
		v.stagingMu.Unlock()
		return err
	}
	v.stagedChunks = make(map[uint64]map[int]stagedChunkReference)
	v.stagedSizes = make(map[uint64]int64)
	v.stagedMtimes = make(map[uint64]time.Time)
	v.stagedBytes = 0
	v.stagingMu.Unlock()

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

	if v.stream != nil {
		localHead, witnessHead, permanentHead := v.stream.Watermarks()
		if localHead > 0 || witnessHead > 0 || permanentHead > 0 || len(v.stream.RecoveredRecords()) > 0 {
			return fmt.Errorf("%w: volume %s stream has existing records", ErrVolumeAlreadyExists, v.volumeID)
		}
	}
	existingSnapshots, err := v.listSnapshotsLocked(ctx)
	if err != nil {
		return fmt.Errorf("failed to check existing snapshots: %w", err)
	}
	if len(existingSnapshots) > 0 {
		return fmt.Errorf("%w: volume %s has existing snapshots", ErrVolumeAlreadyExists, v.volumeID)
	}

	if v.metadataView != nil {
		if err := v.metadataView.Close(); err != nil {
			return fmt.Errorf("failed to close existing metadata view: %w", err)
		}
		v.metadataView = nil
	}

	restoredIndex, _, err := v.indexFactory.RestoreSnapshot(ctx, v.backend, v.streamID.String(), snapshotLocation, v.localStorageDir)
	if err != nil {
		return fmt.Errorf("failed to restore snapshot index: %w", err)
	}

	if err := restoredIndex.SetPosition(0); err != nil {
		if closeErr := restoredIndex.Close(); closeErr != nil {
			return fmt.Errorf("failed to set position (%v) and close failed: %w", err, closeErr)
		}
		return fmt.Errorf("failed to set position on restored index: %w", err)
	}

	if err := v.importIndexRegistryLocked(restoredIndex); err != nil {
		if closeErr := restoredIndex.Close(); closeErr != nil {
			return fmt.Errorf("failed to import snapshot registry (%v) and close failed: %w", err, closeErr)
		}
		return err
	}

	if err := v.registerMetadataTypesLocked(ctx, restoredIndex); err != nil {
		if closeErr := restoredIndex.Close(); closeErr != nil {
			return fmt.Errorf("failed to register metadata types (%v) and close failed: %w", err, closeErr)
		}
		return err
	}

	if err := v.initMetadataViewLocked(ctx, restoredIndex); err != nil {
		return fmt.Errorf("failed to init metadata view from restored snapshot: %w", err)
	}
	v.lastCommitSeq = 0

	v.metadataView.ClearCache()
	v.dirParents = make(map[uint64]uint64)
	v.dirParents[1] = 1
	v.initLiveStatsLocked()
	if err := v.initNextInodeFromStatsLocked(ctx, 0); err != nil {
		return fmt.Errorf("failed to initialize next inode from stats: %w", err)
	}
	v.assertStatsMatchLocked("after InitFromSnapshot")

	snapshotter, ok := v.metadataView.Index().(sds.Snapshotter)
	if !ok {
		return fmt.Errorf("volume metadata index does not support snapshots")
	}

	freshKey, freshPosition, err := snapshotter.PublishSnapshot(ctx, v.backend)
	if err != nil {
		return fmt.Errorf("failed to publish initial snapshot: %w", err)
	}

	pointer := &sdsv1.SnapshotPointer{
		Position:   freshPosition,
		Format:     v.indexFactory.Format(),
		Location:   freshKey,
		Name:       path.Base(freshKey),
		CreatedAt:  timestamppb.Now(),
		ClonedFrom: fmt.Sprintf("%s/%s", sourceVolumeID, snapshotName),
	}

	if v.metadataStream != nil {
		if _, err := v.metadataStream.AppendSnapshotPointer(ctx, pointer); err != nil {
			return fmt.Errorf("failed to append initial snapshot pointer: %w", err)
		}
	}

	if v.stream != nil && v.stream.ReplicationLevel() == walclient.Permanent {
		if err := v.stream.Flush(ctx); err != nil {
			return fmt.Errorf("failed to flush WAL stream: %w", err)
		}
	}

	v.snapshotPointers = append(v.snapshotPointers, pointer)
	return nil
}
