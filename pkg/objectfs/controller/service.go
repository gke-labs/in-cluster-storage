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
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	sdsv1 "github.com/gke-labs/in-cluster-storage/pkg/api/sds/v1"
	walpb "github.com/gke-labs/in-cluster-storage/pkg/api/wal/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/errutil"
	"github.com/gke-labs/in-cluster-storage/pkg/grpcutil"
	"github.com/gke-labs/in-cluster-storage/pkg/metrics"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/blob"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/waldurability"
	"github.com/gke-labs/in-cluster-storage/pkg/scoped"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/view"
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/klog/v2"
)

func volErrToSyscall(err error) int32 {
	if err == nil {
		return 0
	}
	var degradedErr *walclient.DegradedError
	if errors.As(err, &degradedErr) {
		return int32(syscall.EIO)
	}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return int32(errno)
	}
	if errors.Is(err, syscall.ENOENT) {
		return int32(syscall.ENOENT)
	}
	if errors.Is(err, syscall.EEXIST) {
		return int32(syscall.EEXIST)
	}
	if errors.Is(err, syscall.ENOTEMPTY) {
		return int32(syscall.ENOTEMPTY)
	}
	if errors.Is(err, syscall.EISDIR) {
		return int32(syscall.EISDIR)
	}
	if errors.Is(err, syscall.ENOTDIR) {
		return int32(syscall.ENOTDIR)
	}
	if errors.Is(err, syscall.EBUSY) {
		return int32(syscall.EBUSY)
	}
	if errors.Is(err, syscall.EINVAL) {
		return int32(syscall.EINVAL)
	}
	if errors.Is(err, syscall.EACCES) {
		return int32(syscall.EACCES)
	}
	if errors.Is(err, syscall.EPERM) {
		return int32(syscall.EPERM)
	}
	if errors.Is(err, syscall.ENOSYS) {
		return int32(syscall.ENOSYS)
	}
	if errors.Is(err, syscall.ETIMEDOUT) {
		return int32(syscall.ETIMEDOUT)
	}
	if errors.Is(err, syscall.ENOSPC) {
		return int32(syscall.ENOSPC)
	}
	if errors.Is(err, syscall.ENAMETOOLONG) {
		return int32(syscall.ENAMETOOLONG)
	}
	return int32(syscall.EIO)
}

// isValidVolumeID validates that volumeID conforms to a safe alphanumeric and separator character set
// ([A-Za-z0-9._-]) and does not contain path traversal characters (such as . or .. or slashes).
func isValidVolumeID(volumeID string) bool {
	if volumeID == "" || volumeID == "." || volumeID == ".." || len(volumeID) > 255 {
		return false
	}
	for _, r := range volumeID {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '.' || r == '_' || r == '-') {
			return false
		}
	}
	return true
}

func volInitError(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	return status.Errorf(codes.Internal, "%v", err)
}

func checkRPCErr(err error) error {
	if errors.Is(err, ErrVolumeStopping) {
		return status.Errorf(codes.Unavailable, "%v", err)
	}
	if errors.Is(err, context.Canceled) {
		return status.Errorf(codes.Canceled, "%v", err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return status.Errorf(codes.DeadlineExceeded, "%v", err)
	}
	return nil
}

// serverWAL holds the WAL settings and the connection built from them.
type serverWAL struct {
	// dir is the base directory on the local filesystem where WAL segments are stored.
	dir string
	// dirErr records an error encountered when validating or preparing the WAL directory.
	dirErr error
	// target is the gRPC endpoint address for the remote WAL buffer service.
	target string
	// noFsync configures WAL client streams to skip per-record fsync (intended for tests and benchmarks).
	noFsync bool
	// tempDir is a temporary WAL directory to be removed when the server is closed.
	tempDir string
	// testCleanup is an optional cleanup callback executed when the server is closed.
	testCleanup func()
	// dialOpts contains additional gRPC dial options when dialing the WAL buffer.
	dialOpts []grpc.DialOption
	// conn is the active gRPC client connection to the WAL buffer.
	conn *grpc.ClientConn
	// client is the gRPC client stub for the WAL buffer service.
	client walpb.WalBufferClient
	// err records any dial or configuration error encountered when setting up the WAL buffer connection.
	err error
	// defaultDurability is the default sync durability level requested for volume WAL streams.
	defaultDurability walclient.Level
	// streamFactory overrides stream creation with a custom factory function (useful for testing).
	streamFactory func(volumeID string) (walclient.Stream, error)
}

// serverMetadataConfig groups configuration options for volume metadata indexing, caching, and views.
type serverMetadataConfig struct {
	// defaultDurability is the default durability mode for metadata operations (operation or barrier).
	defaultDurability pb.MetadataDurability
	// index configures the metadata index engine ("sqlite" or "memory").
	index string
	// dir is the base directory under which volume-specific metadata subdirectories are created.
	dir string
	// cacheEntries configures the maximum entry capacity for the metadata LRU read cache.
	cacheEntries int
	// cacheBytes configures the maximum byte capacity for the metadata LRU read cache.
	cacheBytes int64
	// overlayMaxBytes sets the maximum byte bound for unapplied overlay rows before applying backpressure.
	overlayMaxBytes int64
	// applierBatchSize sets the maximum batch size for the background SQLite applier.
	applierBatchSize int
	// localStorageDir sets the local directory for metadata storage across all volumes.
	localStorageDir string
	// debugStatsCheck enables debug stats assertions across all volumes.
	debugStatsCheck bool
	// debugMutationCheck enables debug mutation checking on metadata views created by the server.
	debugMutationCheck bool
	// viewOpts holds custom SDS view options applied to all volumes created by the server.
	viewOpts []view.Option
}

// serverDiskLimits groups configuration, accounting, and synchronization for controller-wide staging limits and local pack cache accounting.
//
// Local disk usage is bounded by two mechanisms: staged data is bounded by
// --max-staging-bytes per volume and --max-local-staging-bytes controller-wide;
// uploaded packs are kept near the controller-wide soft target only.
type serverDiskLimits struct {
	// mu guards controller-wide disk accounting fields and coordinates spaceAvailable.
	mu sync.Mutex
	// stagedBytes tracks the controller-wide total of unuploaded staged write bytes across all volumes. Guarded by mu.
	stagedBytes int64
	// maxStagingBytes configures the controller-wide ceiling of unuploaded staged write bytes across all volumes (0 disables). Guarded by mu.
	maxStagingBytes int64
	// packCacheBytes tracks the controller-wide total of uploaded pack bytes kept in the local cache across all volumes. Guarded by mu.
	packCacheBytes int64
	// packCacheTargetBytes configures the controller-wide target byte size for uploaded pack bytes kept in the local cache across all volumes (0 disables). Guarded by mu.
	packCacheTargetBytes int64
	// packCacheUnderflows tracks occurrences of packCacheBytes falling below zero, which indicates an accounting bug since the total is clamped to zero afterwards. Guarded by mu.
	packCacheUnderflows *metrics.Counter
	// spaceAvailable coordinates writers waiting for unuploaded staged bytes to fall below maxStagingBytes.
	spaceAvailable *sync.Cond
	// lastCeilingLog records the last time the ceiling log line was emitted for rate limiting. Guarded by mu.
	lastCeilingLog time.Time
}

// stagingDrain coordinates background draining of staged writes across volumes.
type stagingDrain struct {
	Runnable
	// wake signals the drainLoop goroutine to evaluate staging marks. Requires no lock.
	wake chan struct{}
	// loopDone is closed when drainLoop exits. Requires no lock.
	loopDone chan struct{}
	// passesCompleted counts evaluation passes completed by the drain loop.
	passesCompleted *metrics.Counter
	// volumeResultsHandled counts volume drain results handled by the drain loop.
	volumeResultsHandled *metrics.Counter
}

// wakeDrain sends a non-blocking signal to wake the drain loop.
// Safe to call with any or no locks held.
func (d *stagingDrain) wakeDrain() {
	select {
	case d.wake <- struct{}{}:
	default:
	}
}

const (
	// DefaultMaxLocalStagingBytes sets the default controller-wide ceiling of unuploaded staged bytes (16 GiB).
	DefaultMaxLocalStagingBytes int64 = 16 * 1024 * 1024 * 1024

	// DefaultLocalPackCacheTargetBytes sets the default controller-wide target byte size for uploaded pack bytes kept in the local cache across all volumes (1 GiB).
	DefaultLocalPackCacheTargetBytes int64 = 1024 * 1024 * 1024

	// stagingHighWaterPercent is the percentage of maxStagingBytes at which background draining begins (75%).
	stagingHighWaterPercent = 75

	// stagingLowWaterPercent is the percentage of maxStagingBytes at or below which background draining stops (50%).
	stagingLowWaterPercent = 50

	// stagingDrainMinimumBytes is the minimum unuploaded staged bytes a volume must hold to be eligible
	// for background drain syncing (1 MiB).
	stagingDrainMinimumBytes int64 = 1024 * 1024

	// stagingDrainMaxConcurrentSyncs is the maximum number of volume syncs started by the background drain
	// that can run concurrently. Four is the maintainer's provisional estimate to stagger uploads without
	// saturating the network and object store, not a measured number.
	stagingDrainMaxConcurrentSyncs = 4
)

type Server struct {
	pb.UnimplementedObjectFSControllerServer
	mu          sync.RWMutex
	closeOnce   sync.Once
	volumes     map[string]*Volume
	backend     ObjectStorageBackend
	blobStore   *blob.Store
	broadcaster *EventBroadcaster

	shuttingDownOnce sync.Once
	shuttingDown     chan struct{}

	// wal holds the WAL settings and the connection built from them.
	wal serverWAL
	// metadata groups configuration options for volume metadata indexing, caching, and views.
	metadata serverMetadataConfig
	// syncCoalesceWindow sets the window duration for coalescing non-blocking sync requests across all volumes.
	syncCoalesceWindow time.Duration
	// maxStagingBytes sets the maximum byte bound for in-memory staged writes across all volumes.
	maxStagingBytes int64
	// syncTimeout sets the timeout duration for coalesced background sync operations.
	syncTimeout time.Duration
	// openBarrierTimeout sets the timeout duration for the open-side barrier on close-to-open volumes.
	openBarrierTimeout time.Duration
	// acceptUnversioned configures whether unversioned segments, streams, and snapshots are accepted.
	acceptUnversioned bool

	// diskLimits groups configuration, accounting, and synchronization for controller-wide staging limits.
	diskLimits serverDiskLimits

	// drain coordinates background draining of staged writes across volumes.
	drain stagingDrain

	// evictor coordinates background eviction of uploaded packs in the local cache across volumes.
	evictor packCacheEvictor

	flushTicker *time.Ticker
	stopFlush   chan struct{}
	flushWg     sync.WaitGroup

	checkpointMu      sync.Mutex
	checkpointWaiters map[string]chan uint64

	exclusiveHoldsMu sync.RWMutex
	exclusiveHolds   map[string]*ExclusiveHold
	// exclusiveLeaseTTL configures the lease duration for exclusive-local holds.
	exclusiveLeaseTTL time.Duration

	restoringMu sync.Mutex
	restoring   map[string]bool
}

// ExclusiveHold tracks an active exclusive-local hold on a volume.
// An in-memory lease table is enough: the stream is the source of truth for data,
// and a controller restart simply forgets holds, which the next renew re-establishes.
type ExclusiveHold struct {
	VolumeID    string
	NodeID      string
	MountID     string
	AcquiredAt  time.Time
	LeaseExpiry time.Time
}

func (h *ExclusiveHold) toProto() *pb.ExclusiveHold {
	if h == nil {
		return nil
	}
	return &pb.ExclusiveHold{
		VolumeId:    h.VolumeID,
		NodeId:      h.NodeID,
		MountId:     h.MountID,
		AcquiredAt:  timestamppb.New(h.AcquiredAt),
		LeaseExpiry: timestamppb.New(h.LeaseExpiry),
	}
}

// WithExclusiveLeaseTTL configures the lease duration for exclusive-local holds.
func WithExclusiveLeaseTTL(ttl time.Duration) ServerOption {
	return func(s *Server) {
		s.exclusiveLeaseTTL = ttl
	}
}

// ServerOption configures the controller Server.
type ServerOption func(*Server)

// WithServerClock sets the clock used for pack read and upload timestamps.
func WithServerClock(clock blob.Clock) ServerOption {
	return func(s *Server) {
		if s.blobStore != nil {
			s.blobStore.SetClock(clock)
		}
	}
}

// WithServerMutationCheck enables debug mutation checking on metadata views created by the server.
func WithServerMutationCheck() ServerOption {
	return func(s *Server) {
		s.metadata.debugMutationCheck = true
		s.metadata.viewOpts = append(s.metadata.viewOpts, view.WithMutationCheck())
		s.metadata.debugStatsCheck = true
	}
}

// WithServerStatsCheck enables debug stats assertions across all volumes.
func WithServerStatsCheck() ServerOption {
	return func(s *Server) {
		s.metadata.debugStatsCheck = true
	}
}

// WithoutServerStatsCheck disables debug stats assertions.
func WithoutServerStatsCheck() ServerOption {
	return func(s *Server) {
		s.metadata.debugStatsCheck = false
	}
}

// WithServerViewOptions appends custom view options for all volumes created by the server.
func WithServerViewOptions(opts ...view.Option) ServerOption {
	return func(s *Server) {
		s.metadata.viewOpts = append(s.metadata.viewOpts, opts...)
	}
}

// WithServerMetadataIndex configures the metadata index engine ("sqlite" or "memory").
func WithServerMetadataIndex(index string) ServerOption {
	return func(s *Server) {
		s.metadata.index = index
	}
}

// WithServerMetadataDir configures the base directory for persistent local metadata storage (e.g. SQLite databases).
func WithServerMetadataDir(dir string) ServerOption {
	return func(s *Server) {
		s.metadata.dir = dir
	}
}

// WithServerWAL configures the WAL directory, buffer target, and default durability level.
func WithServerWAL(walDir, walTarget string, durability walclient.Level) ServerOption {
	return func(s *Server) {
		if walDir != "" {
			s.wal.dir = walDir
		}
		if walTarget != "" {
			s.wal.target = walTarget
		}
		s.wal.defaultDurability = durability
	}
}

// InspectServerTarget applies opts to a zero Server to read the configured WAL target, if any.
func InspectServerTarget(opts ...ServerOption) string {
	s := &Server{}
	for _, opt := range opts {
		opt(s)
	}
	return s.wal.target
}

// WithServerWALDialOptions configures additional gRPC dial options when dialing the WAL buffer.
// This is the extension point for TLS/mTLS credentials (e.g. issue #319).
func WithServerWALDialOptions(opts ...grpc.DialOption) ServerOption {
	return func(s *Server) {
		s.wal.dialOpts = append(s.wal.dialOpts, opts...)
	}
}

// WithServerStreamFactory sets a custom stream factory function (useful for testing).
func WithServerStreamFactory(factory func(volumeID string) (walclient.Stream, error)) ServerOption {
	return func(s *Server) {
		s.wal.streamFactory = factory
	}
}

// WithServerDurability sets the default durability level for streams.
func WithServerDurability(durability walclient.Level) ServerOption {
	return func(s *Server) {
		s.wal.defaultDurability = durability
	}
}

// WithServerMetadataDurability sets the default metadata durability mode for volumes.
// Takes no locks.
func WithServerMetadataDurability(mode pb.MetadataDurability) ServerOption {
	return func(s *Server) {
		if mode != pb.MetadataDurability_METADATA_DURABILITY_UNSPECIFIED {
			s.metadata.defaultDurability = mode
		}
	}
}

// WithServerMetadataCacheLimits configures the metadata read cache entry and byte capacity.
func WithServerMetadataCacheLimits(maxEntries int, maxBytes int64) ServerOption {
	return func(s *Server) {
		s.metadata.cacheEntries = maxEntries
		s.metadata.cacheBytes = maxBytes
	}
}

// WithServerMetadataOverlayMaxBytes sets the maximum byte bound for unapplied overlay rows before applying backpressure.
func WithServerMetadataOverlayMaxBytes(maxBytes int64) ServerOption {
	return func(s *Server) {
		s.metadata.overlayMaxBytes = maxBytes
	}
}

// WithServerMetadataApplierBatchSize sets the maximum batch size for the background SQLite applier.
func WithServerMetadataApplierBatchSize(batchSize int) ServerOption {
	return func(s *Server) {
		s.metadata.applierBatchSize = batchSize
	}
}

// WithServerLocalStorageDir sets the local directory for metadata storage for all volumes.
func WithServerLocalStorageDir(dir string) ServerOption {
	return func(s *Server) {
		s.metadata.localStorageDir = dir
	}
}

// WithServerSyncCoalesceWindow sets the window duration for coalescing non-blocking sync requests across all volumes.
func WithServerSyncCoalesceWindow(d time.Duration) ServerOption {
	return func(s *Server) {
		s.syncCoalesceWindow = d
	}
}

// WithServerMaxStagingBytes sets the maximum byte bound for in-memory staged writes across all volumes.
func WithServerMaxStagingBytes(b int64) ServerOption {
	return func(s *Server) {
		s.maxStagingBytes = b
	}
}

// WithServerMaxLocalStagingBytes sets the controller-wide ceiling for unuploaded staged bytes across all volumes (0 disables).
// Requires no locks held.
func WithServerMaxLocalStagingBytes(bytes int64) ServerOption {
	return func(s *Server) {
		s.diskLimits.maxStagingBytes = bytes
	}
}

// WithServerLocalPackCacheTargetBytes configures the controller-wide target byte size for uploaded packs kept in the local cache across all volumes (0 disables).
// Requires no locks held.
func WithServerLocalPackCacheTargetBytes(bytes int64) ServerOption {
	return func(s *Server) {
		s.diskLimits.packCacheTargetBytes = bytes
	}
}

// WithServerSyncTimeout sets the timeout duration for coalesced background sync operations.
func WithServerSyncTimeout(d time.Duration) ServerOption {
	return func(s *Server) {
		if d > 0 {
			s.syncTimeout = d
		}
	}
}

// WithServerOpenBarrierTimeout sets the timeout duration for the open-side barrier on close-to-open volumes.
func WithServerOpenBarrierTimeout(d time.Duration) ServerOption {
	return func(s *Server) {
		if d > 0 {
			s.openBarrierTimeout = d
		}
	}
}

// WithServerAcceptUnversioned configures whether unversioned segments, streams, and snapshots are accepted.
func WithServerAcceptUnversioned(accept bool) ServerOption {
	return func(s *Server) {
		s.acceptUnversioned = accept
	}
}

// WithServerWithoutFsync configures the server's WAL client streams to skip per-record fsync.
// This is intended only for tests and benchmarks.
func WithServerWithoutFsync() ServerOption {
	return func(s *Server) {
		s.wal.noFsync = true
	}
}

// WithServerWALClient sets an existing gRPC client for the WAL buffer service (for testing).
func WithServerWALClient(client walpb.WalBufferClient) ServerOption {
	return func(s *Server) {
		s.wal.client = client
	}
}

// WithServerTestCleanup sets a cleanup callback executed when the server is closed (for testing).
func WithServerTestCleanup(cleanup func()) ServerOption {
	return func(s *Server) {
		s.wal.testCleanup = cleanup
	}
}

// WithServerTempWALDir sets a temporary directory to be removed on server close (for testing).
func WithServerTempWALDir(dir string) ServerOption {
	return func(s *Server) {
		s.wal.dir = dir
		s.wal.tempDir = dir
	}
}

// NewServer creates a new Server. Callers must call Server.Start(ctx) to start background
// loops (including the staging drain loop).
func NewServer(backend ObjectStorageBackend, opts ...ServerOption) *Server {
	if backend == nil {
		backend = NewMemoryBackend()
	}
	s := &Server{
		shuttingDown:      make(chan struct{}),
		volumes:           make(map[string]*Volume),
		backend:           backend,
		blobStore:         blob.NewStore(backend, 0),
		broadcaster:       NewEventBroadcaster(),
		checkpointWaiters: make(map[string]chan uint64),
		exclusiveHolds:    make(map[string]*ExclusiveHold),
		exclusiveLeaseTTL: 30 * time.Second,
		syncTimeout:       10 * time.Minute,
		wal: serverWAL{
			defaultDurability: walclient.Local,
		},
		metadata: serverMetadataConfig{
			defaultDurability: pb.MetadataDurability_OPERATION,
		},
		diskLimits: serverDiskLimits{
			maxStagingBytes:      DefaultMaxLocalStagingBytes,
			packCacheTargetBytes: DefaultLocalPackCacheTargetBytes,
			packCacheUnderflows:  metrics.NewCounter("pack_cache_underflow_total"),
		},
		restoring: make(map[string]bool),
		drain: stagingDrain{
			wake:                 make(chan struct{}, 1),
			loopDone:             make(chan struct{}),
			passesCompleted:      metrics.NewCounter("staging_drain_passes_completed"),
			volumeResultsHandled: metrics.NewCounter("staging_drain_volume_results_handled"),
		},
		evictor: packCacheEvictor{
			wake:            make(chan struct{}, 1),
			passesCompleted: metrics.NewCounter("pack_cache_evictor_passes_completed"),
		},
	}
	s.diskLimits.spaceAvailable = sync.NewCond(&s.diskLimits.mu)
	for _, opt := range opts {
		opt(s)
	}
	if s.wal.client != nil {
		// Existing WAL client provided by test constructor
	} else if s.wal.target != "" {
		dialOpts := append(grpcutil.DialOptions(grpc.WithTransportCredentials(insecure.NewCredentials())), s.wal.dialOpts...)
		conn, err := grpc.NewClient(s.wal.target, dialOpts...)
		if err != nil {
			klog.Errorf("failed to dial WAL buffer target %s: %v", s.wal.target, err)
			s.wal.err = err
		} else {
			s.wal.conn = conn
			s.wal.client = walpb.NewWalBufferClient(conn)
		}
	} else if s.wal.streamFactory == nil {
		s.wal.err = errors.New("wal target is required: no --wal-target configured")
	}
	if s.wal.dir == "" && s.wal.streamFactory == nil && s.wal.testCleanup == nil {
		s.wal.dirErr = errors.New("wal directory is required: no --wal-dir configured")
	}
	return s
}

func (s *Server) openStreamForVolume(volumeID string, dir string) (walclient.Stream, error) {
	if s.wal.streamFactory != nil {
		return s.wal.streamFactory(volumeID)
	}
	if s.wal.dirErr != nil {
		return nil, fmt.Errorf("failed to initialize WAL directory: %w", s.wal.dirErr)
	}
	if dir == "" {
		dir = s.wal.dir
	}
	if s.wal.err != nil {
		return nil, fmt.Errorf("failed to connect to WAL buffer: %w", s.wal.err)
	}
	streamID := StreamIDForVolume(volumeID)
	var walOpts []walclient.Option
	if s.acceptUnversioned {
		walOpts = append(walOpts, walclient.WithAcceptUnversioned(true))
	}
	if s.wal.noFsync {
		walOpts = append(walOpts, walclient.WithoutFsync())
	}
	if s.wal.client != nil {
		walOpts = append(walOpts, walclient.WithGRPCClient(s.wal.client))
	}
	if len(s.wal.dialOpts) > 0 {
		walOpts = append(walOpts, walclient.WithDialOptions(s.wal.dialOpts...))
	}
	return walclient.Open(context.Background(), dir, streamID, s.wal.target, walOpts...)
}

func (s *Server) createVolumeInstanceLocked(volumeID string) (*Volume, error) {
	var volumeOptions []VolumeOption
	stream, err := s.openStreamForVolume(volumeID, s.wal.dir)
	if err != nil {
		return nil, fmt.Errorf("failed to open stream for volume %s: %w", volumeID, err)
	}
	if stream != nil {
		volumeOptions = append(volumeOptions, WithStream(stream))
	}
	volumeOptions = append(volumeOptions, WithWalDurability(waldurability.FromLevel(s.wal.defaultDurability)))
	volumeOptions = append(volumeOptions, WithMetadataDurability(s.metadata.defaultDurability))
	if s.metadata.index != "" {
		volumeOptions = append(volumeOptions, WithMetadataIndex(s.metadata.index))
	}
	if s.metadata.dir != "" {
		volumeDirectory := filepath.Join(s.metadata.dir, volumeID)
		if err := os.MkdirAll(volumeDirectory, 0755); err != nil {
			return nil, fmt.Errorf("failed to create metadata dir %s for volume %s: %w", volumeDirectory, volumeID, err)
		}
		volumeOptions = append(volumeOptions, WithLocalStorageDir(volumeDirectory))
	}
	if s.metadata.cacheEntries > 0 || s.metadata.cacheBytes > 0 {
		volumeOptions = append(volumeOptions, WithMetadataCacheLimits(s.metadata.cacheEntries, s.metadata.cacheBytes))
	}
	if s.metadata.overlayMaxBytes > 0 {
		volumeOptions = append(volumeOptions, WithMaxUnappliedBytes(s.metadata.overlayMaxBytes))
	}
	if s.metadata.applierBatchSize > 0 {
		volumeOptions = append(volumeOptions, WithApplierBatchSize(s.metadata.applierBatchSize))
	}
	if s.metadata.localStorageDir != "" {
		volumeOptions = append(volumeOptions, WithLocalStorageDir(s.metadata.localStorageDir))
	}
	if len(s.metadata.viewOpts) > 0 {
		volumeOptions = append(volumeOptions, WithVolumeViewOptions(s.metadata.viewOpts...))
	}
	if s.metadata.debugMutationCheck {
		volumeOptions = append(volumeOptions, WithVolumeMutationCheck())
	}
	if s.metadata.debugStatsCheck {
		volumeOptions = append(volumeOptions, WithVolumeStatsCheck())
	}
	if s.syncCoalesceWindow > 0 {
		volumeOptions = append(volumeOptions, WithSyncCoalesceWindow(s.syncCoalesceWindow))
	}
	if s.maxStagingBytes > 0 {
		volumeOptions = append(volumeOptions, WithMaxStagingBytes(s.maxStagingBytes))
	}
	if s.syncTimeout > 0 {
		volumeOptions = append(volumeOptions, WithSyncTimeout(s.syncTimeout))
	}
	if s.openBarrierTimeout > 0 {
		volumeOptions = append(volumeOptions, WithOpenBarrierTimeout(s.openBarrierTimeout))
	}
	if s.acceptUnversioned {
		volumeOptions = append(volumeOptions, WithAcceptUnversioned(true))
	}
	if s.blobStore != nil {
		volumeOptions = append(volumeOptions, WithBlobStore(s.blobStore))
	}
	volumeOptions = append(volumeOptions, WithServer(s))

	return NewVolume(volumeID, s.backend, s.broadcaster, volumeOptions...)
}

func (s *Server) getOrCreateVolume(volumeID string) (*Volume, error) {
	if !isValidVolumeID(volumeID) {
		return nil, status.Errorf(codes.InvalidArgument, "invalid volume ID %q (must match [A-Za-z0-9._-] and not be '.' or '..')", volumeID)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	volume, ok := s.volumes[volumeID]
	if ok && volume.IsDeleted() {
		return nil, status.Errorf(codes.NotFound, "volume %s has been deleted", volumeID)
	}
	if ok && volume.writerLockLost() {
		// Another controller wrote to the volume's stream, so everything this
		// Volume holds in memory is stale. Close (not Stop: nothing may drain
		// through the lost stream) and fall through to reload from the stream.
		if err := volume.Close(); err != nil {
			scoped.Log(context.Background()).Error(err, "failed to close volume after writer lock loss", "volume", volumeID)
		}
		delete(s.volumes, volumeID)
		ok = false
	}
	if !ok {
		var err error
		volume, err = s.createVolumeInstanceLocked(volumeID)
		if err != nil {
			return nil, fmt.Errorf("failed to create volume %s: %w", volumeID, err)
		}
		if err := volume.LoadFromBackend(context.Background()); err != nil {
			errutil.CloseAndLog(volume, "unloaded volume")
			return nil, fmt.Errorf("failed to load volume %s from backend: %w", volumeID, err)
		}
		if volume.IsDeleted() {
			errutil.CloseAndLog(volume, "deleted volume")
			return nil, status.Errorf(codes.NotFound, "volume %s has been deleted", volumeID)
		}
		s.volumes[volumeID] = volume
	}
	return volume, nil
}

// Start starts background loops tied to ctx lifecycle. Callers must call Server.Start
// to enable the staging drain loop and local pack cache evictor loop.
// Returns an error if called more than once or after the server is closed.
// Requires no locks held.
func (s *Server) Start(ctx context.Context) error {
	if err := s.drain.Start(ctx, s.drainLoop); err != nil {
		return fmt.Errorf("server %w", err)
	}
	if err := s.evictor.Start(ctx, s.evictorLoop); err != nil {
		s.drain.Stop()
		return fmt.Errorf("server %w", err)
	}
	return nil
}

// Close stops background loops, periodic flushing, and closes all active volumes and streams.
//
// Close discards unsynced staged writes; Server.Close alone does not drain.
// Writes that were staged but not yet acknowledged by an fsync are lost; Close
// has crash semantics. A caller who wants them kept must call Shutdown
// as the draining path and check its error. Close does not call Stop itself so
// it can still complete when the object store or stream server is unreachable,
// though closing an established WAL stream may wait up to the client's 2 s
// release timeout against a silent stream server (volumes are closed
// concurrently).
func (s *Server) Close() error {
	var errs []error
	var ran bool
	s.closeOnce.Do(func() {
		ran = true
		s.drain.Stop()
		s.evictor.Stop()

		if err := s.StopPeriodicFlush(); err != nil {
			errs = append(errs, err)
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		volumes := make([]*Volume, 0, len(s.volumes))
		for _, volume := range s.volumes {
			volumes = append(volumes, volume)
		}
		errs = append(errs, s.closeVolumes(volumes)...)
		clear(s.volumes)
		if s.wal.conn != nil {
			if err := s.wal.conn.Close(); err != nil {
				errs = append(errs, err)
			}
			s.wal.conn = nil
			s.wal.client = nil
		}
		if s.wal.tempDir != "" {
			if err := os.RemoveAll(s.wal.tempDir); err != nil {
				errs = append(errs, err)
			}
			s.wal.tempDir = ""
		}
		if s.wal.testCleanup != nil {
			s.wal.testCleanup()
			s.wal.testCleanup = nil
		}
	})
	if !ran {
		return nil
	}
	return errors.Join(errs...)
}

// closeVolumes closes all volumes concurrently so shutdown time scales with the
// slowest volume (for example, when closing a stream waits on an unresponsive
// stream server) rather than summing across volumes.
func (s *Server) closeVolumes(volumes []*Volume) []error {
	closeErrors := make([]error, len(volumes))
	var waitGroup sync.WaitGroup
	for index, volume := range volumes {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			closeErrors[index] = volume.Close()
		}()
	}
	waitGroup.Wait()
	return closeErrors
}

// stopVolumes runs Volume.Stop concurrently on volumes under timeout and returns
// the volumes that failed along with their errors.
func (s *Server) stopVolumes(ctx context.Context, volumes []*Volume, timeout time.Duration) ([]*Volume, []error) {
	attemptCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	stopErrors := make([]error, len(volumes))
	var waitGroup sync.WaitGroup
	for index, volume := range volumes {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			stopErrors[index] = volume.Stop(attemptCtx)
		}()
	}
	waitGroup.Wait()

	var failed []*Volume
	var failedErrors []error
	for index, volume := range volumes {
		if err := stopErrors[index]; err != nil {
			failed = append(failed, volume)
			failedErrors = append(failedErrors, err)
		}
	}
	return failed, failedErrors
}

// BeginShutdown marks the server as shutting down so open WatchVolume streams
// end promptly and new WatchVolume requests are refused with Unavailable.
// Call this before grpc.Server.GracefulStop so GracefulStop can return once
// in-flight unary calls finish. Safe to call more than once.
func (s *Server) BeginShutdown() {
	s.shuttingDownOnce.Do(func() {
		if s.shuttingDown != nil {
			close(s.shuttingDown)
		}
	})
}

// Shutdown marks the server as shutting down, stops background loops, and drains
// all volumes with Volume.Stop in a loop until every volume has drained or ctx
// is cancelled. Each attempt waits up to attemptTimeout per volume; failed
// volumes are logged once per attempt and retried after retryInterval. Close is
// called only after every volume has drained, so an early cancellation never
// discards staged data.
func (s *Server) Shutdown(ctx context.Context, attemptTimeout, retryInterval time.Duration) error {
	s.BeginShutdown()
	s.stopPeriodicFlushLoop()
	s.drain.Stop()
	s.evictor.Stop()

	s.mu.RLock()
	pending := make([]*Volume, 0, len(s.volumes))
	for _, volume := range s.volumes {
		pending = append(pending, volume)
	}
	s.mu.RUnlock()

	var lastErrors []error
	for attempt := 1; len(pending) > 0; attempt++ {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("shutdown cancelled with %d undrained volumes: %w", len(pending), errors.Join(append(lastErrors, err)...))
		}

		pending, lastErrors = s.stopVolumes(ctx, pending, attemptTimeout)
		if len(pending) == 0 {
			break
		}
		if ctx.Err() != nil {
			continue
		}

		scoped.Log(ctx).Error(errors.Join(lastErrors...), "volume drain attempt failed; retrying", "attempt", attempt, "retryIn", retryInterval)

		timer := time.NewTimer(retryInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}

	return s.Close()
}

func (s *Server) FlushAll(ctx context.Context) error {
	s.mu.RLock()
	vols := make([]*Volume, 0, len(s.volumes))
	for _, v := range s.volumes {
		vols = append(vols, v)
	}
	s.mu.RUnlock()

	var firstErr error
	for _, v := range vols {
		if err := v.FlushToBackend(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (s *Server) StartPeriodicFlush(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		return
	}
	s.mu.Lock()
	if s.flushTicker != nil {
		s.flushTicker.Stop()
		close(s.stopFlush)
	}
	ticker := time.NewTicker(interval)
	stopFlush := make(chan struct{})
	s.flushTicker = ticker
	s.stopFlush = stopFlush
	s.flushWg.Add(1)
	s.mu.Unlock()

	go func() {
		defer s.flushWg.Done()
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-stopFlush:
				return
			case <-ticker.C:
				if err := s.FlushAll(ctx); err != nil {
					klog.Warningf("periodic flush failed: %v", err)
				}
			}
		}
	}()
}

// stopPeriodicFlushLoop stops the periodic flush ticker and waits for its
// goroutine to exit without running a final FlushAll. It reports whether
// periodic flushing was active. Shutdown calls this directly so volumes are
// drained under its retry loop instead of an unbounded FlushAll.
func (s *Server) stopPeriodicFlushLoop() bool {
	s.mu.Lock()
	hadTicker := (s.flushTicker != nil)
	if hadTicker {
		s.flushTicker.Stop()
		close(s.stopFlush)
		s.flushTicker = nil
		s.stopFlush = nil
	}
	s.mu.Unlock()
	s.flushWg.Wait()
	return hadTicker
}

func (s *Server) StopPeriodicFlush() error {
	if s.stopPeriodicFlushLoop() {
		return s.FlushAll(context.Background())
	}
	return nil
}

// Target returns the configured WAL buffer target, if any.
func (s *Server) Target() string {
	return s.wal.target
}

// GetVolume returns the Volume instance for the given volumeID, or an error if creation fails.
func (s *Server) GetVolume(volumeID string) (*Volume, error) {
	return s.getOrCreateVolume(volumeID)
}

// CreateVolumeFromSnapshot creates a new volume initialized from a snapshot of a source volume.
func (s *Server) CreateVolumeFromSnapshot(ctx context.Context, newVolumeID, sourceVolumeID, snapshotName string) error {
	if !isValidVolumeID(newVolumeID) {
		return status.Errorf(codes.InvalidArgument, "invalid new volume ID %q (must match [A-Za-z0-9._-] and not be '.' or '..')", newVolumeID)
	}
	if !isValidVolumeID(sourceVolumeID) {
		return status.Errorf(codes.InvalidArgument, "invalid source volume ID %q (must match [A-Za-z0-9._-] and not be '.' or '..')", sourceVolumeID)
	}
	if snapshotName == "" {
		return status.Error(codes.InvalidArgument, "snapshot_name is required")
	}

	sourceVolume, err := s.getOrCreateVolume(sourceVolumeID)
	if err != nil {
		return volInitError(err)
	}

	sourceSnapshots, err := sourceVolume.ListSnapshots(ctx)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to list snapshots for source volume %s: %v", sourceVolumeID, err)
	}

	var targetPointer *sdsv1.SnapshotPointer
	for _, pointer := range sourceSnapshots {
		if snapshotMatches(pointer, snapshotName) {
			targetPointer = pointer
			break
		}
	}
	if targetPointer == nil {
		return status.Errorf(codes.NotFound, "snapshot %q not found on source volume %s", snapshotName, sourceVolumeID)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.volumes[newVolumeID]; exists {
		return status.Errorf(codes.AlreadyExists, "volume %s already exists", newVolumeID)
	}

	newVolume, err := s.createVolumeInstanceLocked(newVolumeID)
	if err != nil {
		return fmt.Errorf("failed to create volume %s: %w", newVolumeID, err)
	}

	targetName := targetPointer.GetName()
	if targetName == "" {
		targetName = snapshotName
	}
	if err := newVolume.InitFromSnapshot(ctx, sourceVolumeID, targetName, targetPointer.GetLocation()); err != nil {
		if closeErr := newVolume.Close(); closeErr != nil {
			return fmt.Errorf("failed to initialize volume from snapshot (%v) and close failed: %w", err, closeErr)
		}
		if errors.Is(err, ErrVolumeAlreadyExists) {
			return status.Errorf(codes.AlreadyExists, "volume %s already exists", newVolumeID)
		}
		return fmt.Errorf("failed to initialize volume %s from snapshot %s: %w", newVolumeID, snapshotName, err)
	}

	s.volumes[newVolumeID] = newVolume
	return nil
}

// RestoreSnapshot restores the volume filesystem state to a specific snapshot in-place.
// The restore operation is logged to the stream and published as a fresh snapshot,
// making it durable across controller restarts and rebuilds.
func (s *Server) RestoreSnapshot(ctx context.Context, req *pb.RestoreSnapshotRequest) (*pb.RestoreSnapshotResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	if req.GetSnapshotName() == "" {
		return nil, status.Error(codes.InvalidArgument, "snapshot_name is required")
	}

	volumeID := req.GetVolumeId()
	s.restoringMu.Lock()
	if s.restoring[volumeID] {
		s.restoringMu.Unlock()
		return nil, status.Errorf(codes.FailedPrecondition, "restore is already running on volume %s", volumeID)
	}
	s.restoring[volumeID] = true
	s.restoringMu.Unlock()

	defer func() {
		s.restoringMu.Lock()
		delete(s.restoring, volumeID)
		s.restoringMu.Unlock()
	}()

	vol, err := s.getOrCreateVolume(volumeID)
	if err != nil {
		return nil, volInitError(err)
	}

	snapshotInfo, err := vol.RestoreSnapshot(ctx, req.GetSnapshotName())
	if err != nil {
		if errors.Is(err, ErrSnapshotNotFound) {
			return nil, status.Errorf(codes.NotFound, "snapshot %q not found", req.GetSnapshotName())
		}
		return nil, status.Errorf(codes.Internal, "failed to restore snapshot: %v", err)
	}

	return &pb.RestoreSnapshotResponse{
		Snapshot: snapshotInfo,
	}, nil
}

func (s *Server) GetAttr(ctx context.Context, req *pb.GetAttrRequest) (*pb.GetAttrResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	vol, err := s.getOrCreateVolume(req.GetVolumeId())
	if err != nil {
		return nil, volInitError(err)
	}
	attr, err := vol.GetAttr(ctx, req.GetInode())
	if err != nil {
		return &pb.GetAttrResponse{Error: volErrToSyscall(err)}, nil
	}
	return &pb.GetAttrResponse{Attr: attr}, nil
}

func (s *Server) SetAttr(ctx context.Context, req *pb.SetAttrRequest) (*pb.SetAttrResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	vol, err := s.getOrCreateVolume(req.GetVolumeId())
	if err != nil {
		return nil, volInitError(err)
	}
	if req.Size != nil {
		if _, err := vol.TruncateFile(ctx, req.GetInode(), req.GetSize()); err != nil {
			if rpcErr := checkRPCErr(err); rpcErr != nil {
				return nil, rpcErr
			}
			return &pb.SetAttrResponse{Error: volErrToSyscall(err)}, nil
		}
	}

	var atime, mtime, ctime *time.Time
	if req.GetAtime() != nil {
		t := req.GetAtime().AsTime()
		atime = &t
	}
	if req.GetMtime() != nil {
		t := req.GetMtime().AsTime()
		mtime = &t
	}
	if req.GetCtime() != nil {
		t := req.GetCtime().AsTime()
		ctime = &t
	}

	attr, err := vol.SetAttr(ctx, req.GetInode(), req.Mode, req.Uid, req.Gid, atime, req.GetAtimeNow(), mtime, req.GetMtimeNow(), ctime, req.GetCtimeNow())
	if err != nil {
		if rpcErr := checkRPCErr(err); rpcErr != nil {
			return nil, rpcErr
		}
		return &pb.SetAttrResponse{Error: volErrToSyscall(err)}, nil
	}
	return &pb.SetAttrResponse{Attr: attr}, nil
}

func (s *Server) Lookup(ctx context.Context, req *pb.LookupRequest) (*pb.LookupResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	vol, err := s.getOrCreateVolume(req.GetVolumeId())
	if err != nil {
		return nil, volInitError(err)
	}
	attr, err := vol.Lookup(ctx, req.GetParentInode(), req.GetName())
	if err != nil {
		return &pb.LookupResponse{Error: volErrToSyscall(err)}, nil
	}
	return &pb.LookupResponse{Attr: attr}, nil
}

func (s *Server) ReadDir(ctx context.Context, req *pb.ReadDirRequest) (*pb.ReadDirResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	vol, err := s.getOrCreateVolume(req.GetVolumeId())
	if err != nil {
		return nil, volInitError(err)
	}
	entries, err := vol.ReadDir(ctx, req.GetInode())
	if err != nil {
		return &pb.ReadDirResponse{Error: volErrToSyscall(err)}, nil
	}
	return &pb.ReadDirResponse{Entries: entries}, nil
}

func (s *Server) Mkdir(ctx context.Context, req *pb.MkdirRequest) (*pb.MkdirResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	vol, err := s.getOrCreateVolume(req.GetVolumeId())
	if err != nil {
		return nil, volInitError(err)
	}
	attr, err := vol.Mkdir(ctx, req)
	if err != nil {
		if rpcErr := checkRPCErr(err); rpcErr != nil {
			return nil, rpcErr
		}
		return &pb.MkdirResponse{Error: volErrToSyscall(err)}, nil
	}
	return &pb.MkdirResponse{Attr: attr, StreamPosition: attr.GetStreamPosition()}, nil
}

func (s *Server) CreateFile(ctx context.Context, req *pb.CreateFileRequest) (*pb.CreateFileResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	vol, err := s.getOrCreateVolume(req.GetVolumeId())
	if err != nil {
		return nil, volInitError(err)
	}
	attr, err := vol.CreateFile(ctx, req)
	if err != nil {
		if rpcErr := checkRPCErr(err); rpcErr != nil {
			return nil, rpcErr
		}
		return &pb.CreateFileResponse{Error: volErrToSyscall(err)}, nil
	}
	fh := vol.AllocFh(attr.GetInode().GetIno())
	return &pb.CreateFileResponse{Attr: attr, Fh: fh, StreamPosition: attr.GetStreamPosition()}, nil
}

func (s *Server) ReadFile(ctx context.Context, req *pb.ReadFileRequest) (*pb.ReadFileResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	vol, err := s.getOrCreateVolume(req.GetVolumeId())
	if err != nil {
		return nil, volInitError(err)
	}
	data, totalSize, redirectURL, err := vol.ReadFile(ctx, req.GetInode(), req.GetOffset(), req.GetSize())
	if err != nil {
		return &pb.ReadFileResponse{Error: volErrToSyscall(err)}, nil
	}

	eof := (req.GetOffset() + int64(len(data))) >= totalSize
	return &pb.ReadFileResponse{
		Data:        data,
		RedirectUrl: redirectURL,
		Eof:         eof,
		TotalSize:   totalSize,
	}, nil
}

func (s *Server) WriteFile(ctx context.Context, req *pb.WriteFileRequest) (*pb.WriteFileResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	vol, err := s.getOrCreateVolume(req.GetVolumeId())
	if err != nil {
		return nil, volInitError(err)
	}
	bytesWritten, newSize, modTime, stagingEpoch, err := vol.WriteFileWithEpoch(ctx, req.GetInode(), req.GetOffset(), req.GetData(), req.GetWriteMode())
	if err != nil {
		if rpcErr := checkRPCErr(err); rpcErr != nil {
			return nil, rpcErr
		}
		return &pb.WriteFileResponse{Error: volErrToSyscall(err)}, nil
	}
	return &pb.WriteFileResponse{
		BytesWritten:   bytesWritten,
		NewSize:        newSize,
		ModTime:        timestamppb.New(modTime),
		StagingEpoch:   stagingEpoch,
		StreamPosition: vol.Position(),
	}, nil
}

func (s *Server) TruncateFile(ctx context.Context, req *pb.TruncateFileRequest) (*pb.TruncateFileResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	vol, err := s.getOrCreateVolume(req.GetVolumeId())
	if err != nil {
		return nil, volInitError(err)
	}
	attr, err := vol.TruncateFile(ctx, req.GetInode(), req.GetSize())
	if err != nil {
		if rpcErr := checkRPCErr(err); rpcErr != nil {
			return nil, rpcErr
		}
		return &pb.TruncateFileResponse{Error: volErrToSyscall(err)}, nil
	}
	return &pb.TruncateFileResponse{Attr: attr, StreamPosition: attr.GetStreamPosition()}, nil
}

func (s *Server) PunchHole(ctx context.Context, req *pb.PunchHoleRequest) (*pb.PunchHoleResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	vol, err := s.getOrCreateVolume(req.GetVolumeId())
	if err != nil {
		return nil, volInitError(err)
	}
	attr, err := vol.PunchHole(ctx, req.GetInode(), req.GetOffset(), req.GetLength(), req.GetKeepSize())
	if err != nil {
		if rpcErr := checkRPCErr(err); rpcErr != nil {
			return nil, rpcErr
		}
		return &pb.PunchHoleResponse{Error: volErrToSyscall(err)}, nil
	}
	return &pb.PunchHoleResponse{Attr: attr, StreamPosition: attr.GetStreamPosition()}, nil
}

func (s *Server) Unlink(ctx context.Context, req *pb.UnlinkRequest) (*pb.UnlinkResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	vol, err := s.getOrCreateVolume(req.GetVolumeId())
	if err != nil {
		return nil, volInitError(err)
	}
	pos, err := vol.Unlink(ctx, req)
	if err != nil {
		if rpcErr := checkRPCErr(err); rpcErr != nil {
			return nil, rpcErr
		}
		return &pb.UnlinkResponse{Success: false, Error: volErrToSyscall(err)}, nil
	}
	return &pb.UnlinkResponse{Success: true, StreamPosition: pos}, nil
}

func (s *Server) Rmdir(ctx context.Context, req *pb.RmdirRequest) (*pb.RmdirResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	vol, err := s.getOrCreateVolume(req.GetVolumeId())
	if err != nil {
		return nil, volInitError(err)
	}
	pos, err := vol.Rmdir(ctx, req)
	if err != nil {
		if rpcErr := checkRPCErr(err); rpcErr != nil {
			return nil, rpcErr
		}
		return &pb.RmdirResponse{Success: false, Error: volErrToSyscall(err)}, nil
	}
	return &pb.RmdirResponse{Success: true, StreamPosition: pos}, nil
}

func (s *Server) Rename(ctx context.Context, req *pb.RenameRequest) (*pb.RenameResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	vol, err := s.getOrCreateVolume(req.GetVolumeId())
	if err != nil {
		return nil, volInitError(err)
	}
	attr, err := vol.Rename(ctx, req)
	if err != nil {
		if rpcErr := checkRPCErr(err); rpcErr != nil {
			return nil, rpcErr
		}
		return &pb.RenameResponse{Error: volErrToSyscall(err)}, nil
	}
	return &pb.RenameResponse{Attr: attr, StreamPosition: attr.GetStreamPosition()}, nil
}

func (s *Server) Fsync(ctx context.Context, req *pb.FsyncRequest) (*pb.FsyncResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	vol, err := s.getOrCreateVolume(req.GetVolumeId())
	if err != nil {
		return nil, volInitError(err)
	}
	stagingEpoch, pos, err := vol.FsyncWithEpoch(ctx, req.GetInode(), req.GetBlocking(), req.GetStagingEpoch(), req.GetTrigger())
	if err != nil {
		if rpcErr := checkRPCErr(err); rpcErr != nil {
			return nil, rpcErr
		}
		return &pb.FsyncResponse{Success: false, Error: volErrToSyscall(err), StagingEpoch: stagingEpoch, StreamPosition: pos}, nil
	}
	return &pb.FsyncResponse{Success: true, StagingEpoch: stagingEpoch, StreamPosition: pos}, nil
}

func (s *Server) Symlink(ctx context.Context, req *pb.SymlinkRequest) (*pb.SymlinkResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	vol, err := s.getOrCreateVolume(req.GetVolumeId())
	if err != nil {
		return nil, volInitError(err)
	}
	attr, err := vol.Symlink(ctx, req)
	if err != nil {
		if rpcErr := checkRPCErr(err); rpcErr != nil {
			return nil, rpcErr
		}
		return &pb.SymlinkResponse{Error: volErrToSyscall(err)}, nil
	}
	return &pb.SymlinkResponse{Attr: attr, StreamPosition: attr.GetStreamPosition()}, nil
}

func (s *Server) Readlink(ctx context.Context, req *pb.ReadlinkRequest) (*pb.ReadlinkResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	vol, err := s.getOrCreateVolume(req.GetVolumeId())
	if err != nil {
		return nil, volInitError(err)
	}
	target, err := vol.Readlink(ctx, req.GetInode())
	if err != nil {
		return &pb.ReadlinkResponse{Error: volErrToSyscall(err)}, nil
	}
	return &pb.ReadlinkResponse{Target: target}, nil
}

func (s *Server) Link(ctx context.Context, req *pb.LinkRequest) (*pb.LinkResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	vol, err := s.getOrCreateVolume(req.GetVolumeId())
	if err != nil {
		return nil, volInitError(err)
	}
	attr, err := vol.Link(ctx, req)
	if err != nil {
		if rpcErr := checkRPCErr(err); rpcErr != nil {
			return nil, rpcErr
		}
		return &pb.LinkResponse{Error: volErrToSyscall(err)}, nil
	}
	return &pb.LinkResponse{Attr: attr, StreamPosition: attr.GetStreamPosition()}, nil
}

func (s *Server) Open(ctx context.Context, req *pb.OpenRequest) (*pb.OpenResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	vol, err := s.getOrCreateVolume(req.GetVolumeId())
	if err != nil {
		return nil, volInitError(err)
	}
	fh, attr, pos, err := vol.Open(ctx, req.GetInode(), req.GetFlags())
	if err != nil {
		return &pb.OpenResponse{Error: volErrToSyscall(err)}, nil
	}
	return &pb.OpenResponse{Fh: fh, Attr: attr, StreamPosition: pos}, nil
}

func (s *Server) Release(ctx context.Context, req *pb.ReleaseRequest) (*pb.ReleaseResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	vol, err := s.getOrCreateVolume(req.GetVolumeId())
	if err != nil {
		return nil, volInitError(err)
	}
	if err := vol.Release(ctx, req.GetInode(), req.GetFh()); err != nil {
		return &pb.ReleaseResponse{Error: volErrToSyscall(err)}, nil
	}
	return &pb.ReleaseResponse{}, nil
}

func (s *Server) GetXAttr(ctx context.Context, req *pb.GetXAttrRequest) (*pb.GetXAttrResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	vol, err := s.getOrCreateVolume(req.GetVolumeId())
	if err != nil {
		return nil, volInitError(err)
	}
	val, err := vol.GetXAttr(ctx, req.GetInode(), req.GetName())
	if err != nil {
		return &pb.GetXAttrResponse{Error: volErrToSyscall(err)}, nil
	}
	return &pb.GetXAttrResponse{Value: val}, nil
}

func (s *Server) SetXAttr(ctx context.Context, req *pb.SetXAttrRequest) (*pb.SetXAttrResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	vol, err := s.getOrCreateVolume(req.GetVolumeId())
	if err != nil {
		return nil, volInitError(err)
	}
	if err := vol.SetXAttr(ctx, req.GetInode(), req.GetName(), req.GetValue(), req.GetFlags()); err != nil {
		if rpcErr := checkRPCErr(err); rpcErr != nil {
			return nil, rpcErr
		}
		return &pb.SetXAttrResponse{Error: volErrToSyscall(err)}, nil
	}
	return &pb.SetXAttrResponse{}, nil
}

func (s *Server) ListXAttr(ctx context.Context, req *pb.ListXAttrRequest) (*pb.ListXAttrResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	vol, err := s.getOrCreateVolume(req.GetVolumeId())
	if err != nil {
		return nil, volInitError(err)
	}
	xattrs, err := vol.ListXAttr(ctx, req.GetInode())
	if err != nil {
		return &pb.ListXAttrResponse{Error: volErrToSyscall(err)}, nil
	}
	return &pb.ListXAttrResponse{Xattrs: xattrs}, nil
}

func (s *Server) RemoveXAttr(ctx context.Context, req *pb.RemoveXAttrRequest) (*pb.RemoveXAttrResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	vol, err := s.getOrCreateVolume(req.GetVolumeId())
	if err != nil {
		return nil, volInitError(err)
	}
	if err := vol.RemoveXAttr(ctx, req.GetInode(), req.GetName()); err != nil {
		if rpcErr := checkRPCErr(err); rpcErr != nil {
			return nil, rpcErr
		}
		return &pb.RemoveXAttrResponse{Error: volErrToSyscall(err)}, nil
	}
	return &pb.RemoveXAttrResponse{}, nil
}

// Broadcaster returns the EventBroadcaster for the server.
func (s *Server) Broadcaster() *EventBroadcaster {
	return s.broadcaster
}

func (s *Server) WatchVolume(req *pb.WatchVolumeRequest, stream pb.ObjectFSController_WatchVolumeServer) error {
	select {
	case <-s.shuttingDown:
		return status.Error(codes.Unavailable, "server shutting down")
	default:
	}

	if !isValidVolumeID(req.GetVolumeId()) {
		return status.Errorf(codes.InvalidArgument, "invalid volume ID %q (must match [A-Za-z0-9._-] and not be '.' or '..')", req.GetVolumeId())
	}

	// The volume is now always loaded so the initial bookmark can carry its position,
	// whereas previously it was loaded only when a consistency mode was sent.
	volume, err := s.getOrCreateVolume(req.GetVolumeId())
	if err != nil {
		return volInitError(err)
	}

	if req.GetConsistencyMode() != pb.ConsistencyMode_CONSISTENCY_MODE_UNSPECIFIED {
		currentMode := volume.StoredConsistencyMode()
		clientMode := req.GetConsistencyMode()
		if currentMode == pb.ConsistencyMode_CONSISTENCY_MODE_UNSPECIFIED {
			if clientMode == pb.ConsistencyMode_CLOSE_TO_OPEN {
				if err := volume.PersistConsistencyMode(stream.Context(), clientMode); err != nil {
					klog.Warningf("WatchVolume %s: failed to persist consistency mode: %v", req.GetVolumeId(), err)
				}
			}
		} else if clientMode != currentMode {
			klog.Warningf("WatchVolume %s: client requested consistency mode %v but volume has %v; rejecting connection", req.GetVolumeId(), clientMode, currentMode)
			return status.Errorf(codes.InvalidArgument, "consistency mode mismatch: volume is %v, client requested %v", currentMode, clientMode)
		}
	}

	ch := s.broadcaster.Subscribe(req.GetVolumeId())
	defer s.broadcaster.Unsubscribe(req.GetVolumeId(), ch)

	// Read the position after subscribing so that any change committed in between
	// is both at or below the bookmark and will still be delivered to this subscriber;
	// the other order leaves a gap.
	bookmark := &pb.WatchVolumeResponse{
		EventType:      pb.WatchEventType_EVENT_BOOKMARK,
		StreamPosition: volume.Position(),
	}
	if err := stream.Send(bookmark); err != nil {
		return fmt.Errorf("failed to send bookmark: %w", err)
	}

	ctx := stream.Context()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.shuttingDown:
			return status.Error(codes.Unavailable, "server shutting down")
		case event, ok := <-ch:
			if !ok {
				return nil
			}
			if err := stream.Send(event); err != nil {
				return fmt.Errorf("failed to send event: %w", err)
			}
		}
	}
}

func (s *Server) ListBlobs(ctx context.Context, req *pb.ListBlobsRequest) (*pb.ListBlobsResponse, error) {
	if s.blobStore == nil {
		return &pb.ListBlobsResponse{EndOfData: true}, nil
	}
	shas, endOfData, err := s.blobStore.ListBlobs(ctx, blob.ListBlobsOptions{
		FromSHA:   req.GetFromSha(),
		Limit:     int(req.GetLimit()),
		SHAPrefix: req.GetShaPrefix(),
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list blobs: %v", err)
	}
	return &pb.ListBlobsResponse{
		Sha256:    shas,
		EndOfData: endOfData,
	}, nil
}

func (s *Server) GetBlob(req *pb.GetBlobRequest, stream pb.ObjectFSController_GetBlobServer) error {
	sha := req.GetSha256()
	if sha == "" {
		return status.Error(codes.InvalidArgument, "sha256 is required")
	}
	if s.blobStore == nil {
		return status.Errorf(codes.NotFound, "blob %s not found", sha)
	}

	offset := req.GetOffset()
	if offset < 0 {
		return status.Error(codes.InvalidArgument, "offset cannot be negative")
	}
	limit := req.GetLimit()
	if limit < 0 {
		return status.Error(codes.InvalidArgument, "limit cannot be negative")
	}

	bStream, err := s.blobStore.GetBlob(stream.Context(), sha)
	if err != nil {
		return status.Errorf(codes.NotFound, "failed to get blob: %v", err)
	}
	defer errutil.CloseAndLog(bStream, "blob stream")

	if offset > 0 {
		if _, err := bStream.Seek(offset, io.SeekStart); err != nil {
			return status.Errorf(codes.Internal, "failed seeking to offset %d: %v", offset, err)
		}
	}

	var r io.Reader = bStream
	if limit > 0 {
		r = io.LimitReader(bStream, limit)
	}

	buf := make([]byte, 64*1024)
	for {
		n, rErr := r.Read(buf)
		if n > 0 {
			if err := stream.Send(&pb.GetBlobResponse{Data: buf[:n]}); err != nil {
				return status.Errorf(codes.Internal, "failed to send blob chunk: %v", err)
			}
		}
		if rErr == io.EOF {
			break
		}
		if rErr != nil {
			return status.Errorf(codes.Internal, "failed reading blob: %v", rErr)
		}
	}

	return nil
}

func (s *Server) GetManifest(ctx context.Context, req *pb.GetManifestRequest) (*pb.GetManifestResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	vol, err := s.getOrCreateVolume(req.GetVolumeId())
	if err != nil {
		return nil, volInitError(err)
	}
	resp, err := vol.GetManifest(ctx, req.GetInode())
	if err != nil {
		return &pb.GetManifestResponse{Error: volErrToSyscall(err)}, nil
	}
	return resp, nil
}

func parseSnapshotTime(name string) time.Time {
	trimmed := strings.TrimSuffix(name, ".erofs")
	formats := []string{
		"20060102T150405.000000Z",
		"20060102T150405Z",
		time.RFC3339Nano,
		time.RFC3339,
	}
	for _, f := range formats {
		t, err := time.Parse(f, trimmed)
		if err != nil {
			continue
		}
		return t
	}
	return time.Time{}
}

func (s *Server) ListVolumes(ctx context.Context, req *pb.ListVolumesRequest) (*pb.ListVolumesResponse, error) {
	volMap := make(map[string]struct{})

	s.mu.RLock()
	for id := range s.volumes {
		volMap[id] = struct{}{}
	}
	s.mu.RUnlock()

	if s.backend != nil {
		objects, err := s.backend.ListObjects(ctx, "", "volumes/")
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to list volumes from backend: %v", err)
		}
		for _, obj := range objects {
			trimmed := strings.TrimPrefix(obj.Key, "volumes/")
			parts := strings.Split(trimmed, "/")
			if len(parts) > 0 && parts[0] != "" {
				volMap[parts[0]] = struct{}{}
			}
		}
	}

	var allVols []string
	for id := range volMap {
		allVols = append(allVols, id)
	}
	sort.Strings(allVols)

	fromVolumeID := ""
	limit := 0
	if req != nil {
		fromVolumeID = req.GetFromVolumeId()
		limit = int(req.GetLimit())
	}

	var filtered []string
	for _, id := range allVols {
		if fromVolumeID != "" && id <= fromVolumeID {
			continue
		}
		filtered = append(filtered, id)
	}

	endOfData := true
	if limit > 0 && len(filtered) > limit {
		filtered = filtered[:limit]
		endOfData = false
	}

	var volInfos []*pb.VolumeInfo
	s.mu.RLock()
	for _, id := range filtered {
		info := &pb.VolumeInfo{
			VolumeId: id,
		}
		if volume, ok := s.volumes[id]; ok {
			info.ConsistencyMode = volume.StoredConsistencyMode()
			info.MetadataDurability = volume.MetadataDurability()
			info.WalDurability = volume.WalDurability()
			if deg := volume.Degraded(); deg != nil {
				info.Degraded = &pb.Degraded{
					Code:    pb.Degraded_WAL_STREAM_DEGRADED,
					Message: deg.Error(),
					Since:   timestamppb.New(deg.At),
				}
			}
		}
		volInfos = append(volInfos, info)
	}
	s.mu.RUnlock()

	return &pb.ListVolumesResponse{
		Volumes:   volInfos,
		EndOfData: endOfData,
	}, nil
}

func (s *Server) ListSnapshots(ctx context.Context, req *pb.ListSnapshotsRequest) (*pb.ListSnapshotsResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	vol, err := s.getOrCreateVolume(req.GetVolumeId())
	if err != nil {
		return nil, volInitError(err)
	}
	allSnapshots, err := vol.ListSnapshots(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list snapshots: %v", err)
	}

	var fromTime, toTime time.Time
	if req.GetFromTime() != nil {
		fromTime = req.GetFromTime().AsTime()
	}
	if req.GetToTime() != nil {
		toTime = req.GetToTime().AsTime()
	}
	fromSnapshot := req.GetFromSnapshot()

	var snapInfos []*pb.SnapshotInfo
	for _, ptr := range allSnapshots {
		name := ptr.GetName()
		if name == "" {
			name = path.Base(ptr.GetLocation())
		}
		if fromSnapshot != "" && (name <= fromSnapshot || ptr.GetLocation() <= fromSnapshot) {
			continue
		}
		info := &pb.SnapshotInfo{
			Name:      name,
			Position:  ptr.GetPosition(),
			CreatedAt: ptr.GetCreatedAt(),
		}
		if info.CreatedAt == nil {
			t := parseSnapshotTime(name)
			if !t.IsZero() {
				info.CreatedAt = timestamppb.New(t)
			}
		}
		if info.CreatedAt != nil {
			st := info.CreatedAt.AsTime()
			if !fromTime.IsZero() && st.Before(fromTime) {
				continue
			}
			if !toTime.IsZero() && st.After(toTime) {
				continue
			}
		}
		snapInfos = append(snapInfos, info)
	}

	limit := int(req.GetLimit())
	endOfData := true
	if limit > 0 && len(snapInfos) > limit {
		snapInfos = snapInfos[:limit]
		endOfData = false
	}

	return &pb.ListSnapshotsResponse{
		Snapshots: snapInfos,
		EndOfData: endOfData,
	}, nil
}

func (s *Server) CreateSnapshot(ctx context.Context, req *pb.CreateSnapshotRequest) (*pb.CreateSnapshotResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	vol, err := s.getOrCreateVolume(req.GetVolumeId())
	if err != nil {
		return nil, volInitError(err)
	}
	snapName, err := vol.CreateSnapshot(ctx, req.GetName())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create snapshot: %v", err)
	}

	snapInfo, err := vol.GetSnapshotInfo(ctx, snapName)
	if err != nil {
		snapInfo = &pb.SnapshotInfo{
			Name:      snapName,
			CreatedAt: timestamppb.Now(),
		}
	}

	return &pb.CreateSnapshotResponse{
		SnapshotName: snapName,
		Snapshot:     snapInfo,
	}, nil
}

func (s *Server) findVolume(ctx context.Context, volumeID string) (*Volume, error) {
	s.mu.RLock()
	volume, exists := s.volumes[volumeID]
	s.mu.RUnlock()
	if exists {
		return volume, nil
	}
	if s.backend != nil {
		objects, err := s.backend.ListObjects(ctx, "", fmt.Sprintf("volumes/%s/", volumeID))
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to check volume in backend: %v", err)
		}
		if len(objects) > 0 {
			return s.getOrCreateVolume(volumeID)
		}
	}
	return nil, status.Errorf(codes.NotFound, "volume %s not found", volumeID)
}

func (s *Server) DeleteSnapshot(ctx context.Context, request *pb.DeleteSnapshotRequest) (*pb.DeleteSnapshotResponse, error) {
	if request.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	if request.GetSnapshotName() == "" {
		return nil, status.Error(codes.InvalidArgument, "snapshot_name is required")
	}
	if !isValidVolumeID(request.GetVolumeId()) {
		return nil, status.Errorf(codes.InvalidArgument, "invalid volume ID %q", request.GetVolumeId())
	}

	volume, err := s.findVolume(ctx, request.GetVolumeId())
	if err != nil {
		return nil, err
	}
	if err := volume.DeleteSnapshot(ctx, request.GetSnapshotName()); err != nil {
		if errors.Is(err, ErrNewestSnapshot) {
			return nil, status.Errorf(codes.FailedPrecondition, "cannot delete newest snapshot %s", request.GetSnapshotName())
		}
		if errors.Is(err, ErrSnapshotInUse) {
			return nil, status.Errorf(codes.FailedPrecondition, "cannot delete snapshot %s: live stream depends on it", request.GetSnapshotName())
		}
		return nil, err
	}
	return &pb.DeleteSnapshotResponse{}, nil
}

const (
	defaultNominalCapacityBytes = 4 * 1024 * 1024 * 1024 * 1024 // 4 TiB nominal capacity
	defaultNominalTotalInodes   = 10000000                      // 10,000,000 nominal inodes
)

func (s *Server) GetVolumeStats(ctx context.Context, req *pb.GetVolumeStatsRequest) (*pb.GetVolumeStatsResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	vol, err := s.getOrCreateVolume(req.GetVolumeId())
	if err != nil {
		return nil, volInitError(err)
	}

	st := vol.Stats()

	totalBytes := int64(defaultNominalCapacityBytes)
	usedBytes := st.Stats.GetLogicalBytes()
	availBytes := totalBytes - usedBytes
	if availBytes < 0 {
		availBytes = 0
	}

	totalInodes := int64(defaultNominalTotalInodes)
	usedInodes := st.Stats.GetInodesFile() + st.Stats.GetInodesDir() + st.Stats.GetInodesSymlink() + st.Stats.GetInodesOther()
	freeInodes := totalInodes - usedInodes
	if freeInodes < 0 {
		freeInodes = 0
	}

	var degraded *pb.Degraded
	if deg := vol.Degraded(); deg != nil {
		degraded = &pb.Degraded{
			Code:    pb.Degraded_WAL_STREAM_DEGRADED,
			Message: deg.Error(),
			Since:   timestamppb.New(deg.At),
		}
	}

	return &pb.GetVolumeStatsResponse{
		Stats:          st.Stats,
		TotalBytes:     totalBytes,
		UsedBytes:      usedBytes,
		AvailableBytes: availBytes,
		TotalInodes:    totalInodes,
		UsedInodes:     usedInodes,
		FreeInodes:     freeInodes,
		Error:          0,
		Degraded:       degraded,
	}, nil
}

// Checkpoint requests an immediate checkpoint commit on an exclusive-local volume and waits
// for the mounting node daemon to acknowledge it. Only exclusive-local mounts act on the event;
// FUSE mounts ignore it.
func (s *Server) Checkpoint(ctx context.Context, req *pb.CheckpointRequest) (*pb.CheckpointResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	volumeID := req.GetVolumeId()

	if _, err := s.getOrCreateVolume(volumeID); err != nil {
		return nil, volInitError(err)
	}

	requestID := uuid.NewString()
	ch := make(chan uint64, 1)

	s.checkpointMu.Lock()
	s.checkpointWaiters[requestID] = ch
	s.checkpointMu.Unlock()

	defer func() {
		s.checkpointMu.Lock()
		delete(s.checkpointWaiters, requestID)
		s.checkpointMu.Unlock()
	}()

	// Broadcast checkpoint request to all active watchers on this volume
	s.broadcaster.Broadcast(volumeID, &pb.WatchVolumeResponse{
		EventType:           pb.WatchEventType_EVENT_CHECKPOINT_REQUEST,
		CheckpointRequestId: requestID,
	})

	// Server-side timeout: return DeadlineExceeded if no exclusive-local mount acks within 60s
	timer := time.NewTimer(60 * time.Second)
	defer timer.Stop()

	select {
	case pos := <-ch:
		return &pb.CheckpointResponse{
			StreamPosition: pos,
		}, nil
	case <-timer.C:
		return nil, status.Errorf(codes.DeadlineExceeded, "checkpoint timed out after 60s waiting for exclusive-local mount acknowledgement on volume %s", volumeID)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// AckCheckpoint acknowledges completion of an explicit checkpoint commit by a node daemon.
func (s *Server) AckCheckpoint(ctx context.Context, req *pb.AckCheckpointRequest) (*pb.AckCheckpointResponse, error) {
	if req.GetVolumeId() == "" || req.GetRequestId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id and request_id are required")
	}

	s.checkpointMu.Lock()
	ch, ok := s.checkpointWaiters[req.GetRequestId()]
	if ok {
		delete(s.checkpointWaiters, req.GetRequestId())
	}
	s.checkpointMu.Unlock()

	if ok {
		select {
		case ch <- req.GetStreamPosition():
		default:
		}
	}
	return &pb.AckCheckpointResponse{}, nil
}

// AcquireExclusive records an exclusive-local hold on the volume for a specific node and mount.
// If another holder exists and its lease is live, it returns codes.Aborted with the holder's node id,
// allowing the node daemon to surface this as a retryable CSI error so kubelet retries with backoff.
func (s *Server) AcquireExclusive(ctx context.Context, req *pb.AcquireExclusiveRequest) (*pb.AcquireExclusiveResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	if !isValidVolumeID(req.GetVolumeId()) {
		return nil, status.Errorf(codes.InvalidArgument, "invalid volume ID %q (must match [A-Za-z0-9._-] and not be '.' or '..')", req.GetVolumeId())
	}
	if req.GetNodeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}
	if req.GetMountId() == "" {
		return nil, status.Error(codes.InvalidArgument, "mount_id is required")
	}

	volumeID := req.GetVolumeId()
	s.exclusiveHoldsMu.Lock()
	defer s.exclusiveHoldsMu.Unlock()

	now := time.Now()
	ttl := s.exclusiveLeaseTTL
	if ttl <= 0 {
		ttl = 30 * time.Second
	}

	if hold, exists := s.exclusiveHolds[volumeID]; exists && now.Before(hold.LeaseExpiry) {
		// If held by the same node and mount, this is an idempotent re-acquire / lease extension.
		if hold.NodeID == req.GetNodeId() && hold.MountID == req.GetMountId() {
			hold.LeaseExpiry = now.Add(ttl)
			return &pb.AcquireExclusiveResponse{
				Hold: hold.toProto(),
			}, nil
		}
		return nil, status.Errorf(codes.Aborted, "volume %s is held by node %s (mount %s) until %s",
			volumeID, hold.NodeID, hold.MountID, hold.LeaseExpiry.Format(time.RFC3339))
	}

	newHold := &ExclusiveHold{
		VolumeID:    volumeID,
		NodeID:      req.GetNodeId(),
		MountID:     req.GetMountId(),
		AcquiredAt:  now,
		LeaseExpiry: now.Add(ttl),
	}
	s.exclusiveHolds[volumeID] = newHold

	return &pb.AcquireExclusiveResponse{
		Hold: newHold.toProto(),
	}, nil
}

// RenewExclusive renews the lease on an exclusive-local hold for the holding node and mount.
// If the controller restarted and holds were lost, the periodic renew re-establishes the hold.
func (s *Server) RenewExclusive(ctx context.Context, req *pb.RenewExclusiveRequest) (*pb.RenewExclusiveResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	if !isValidVolumeID(req.GetVolumeId()) {
		return nil, status.Errorf(codes.InvalidArgument, "invalid volume ID %q", req.GetVolumeId())
	}
	if req.GetNodeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}
	if req.GetMountId() == "" {
		return nil, status.Error(codes.InvalidArgument, "mount_id is required")
	}

	volumeID := req.GetVolumeId()
	s.exclusiveHoldsMu.Lock()
	defer s.exclusiveHoldsMu.Unlock()

	now := time.Now()
	ttl := s.exclusiveLeaseTTL
	if ttl <= 0 {
		ttl = 30 * time.Second
	}

	hold, exists := s.exclusiveHolds[volumeID]
	if !exists {
		// Controller restart: in-memory lease table was lost, renew re-establishes it.
		newHold := &ExclusiveHold{
			VolumeID:    volumeID,
			NodeID:      req.GetNodeId(),
			MountID:     req.GetMountId(),
			AcquiredAt:  now,
			LeaseExpiry: now.Add(ttl),
		}
		s.exclusiveHolds[volumeID] = newHold
		return &pb.RenewExclusiveResponse{
			Hold: newHold.toProto(),
		}, nil
	}

	if hold.NodeID == req.GetNodeId() && hold.MountID == req.GetMountId() {
		hold.LeaseExpiry = now.Add(ttl)
		return &pb.RenewExclusiveResponse{
			Hold: hold.toProto(),
		}, nil
	}

	// Held by a different node or mount
	if now.Before(hold.LeaseExpiry) {
		return nil, status.Errorf(codes.FailedPrecondition, "cannot renew hold for volume %s: held by node %s (mount %s) until %s",
			volumeID, hold.NodeID, hold.MountID, hold.LeaseExpiry.Format(time.RFC3339))
	}

	// Previous hold has expired, caller re-establishes
	newHold := &ExclusiveHold{
		VolumeID:    volumeID,
		NodeID:      req.GetNodeId(),
		MountID:     req.GetMountId(),
		AcquiredAt:  now,
		LeaseExpiry: now.Add(ttl),
	}
	s.exclusiveHolds[volumeID] = newHold
	return &pb.RenewExclusiveResponse{
		Hold: newHold.toProto(),
	}, nil
}

// ReleaseExclusive releases an active exclusive-local hold after the final unmount checkpoint has committed.
func (s *Server) ReleaseExclusive(ctx context.Context, req *pb.ReleaseExclusiveRequest) (*pb.ReleaseExclusiveResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	if req.GetNodeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}

	volumeID := req.GetVolumeId()
	s.exclusiveHoldsMu.Lock()
	defer s.exclusiveHoldsMu.Unlock()

	if hold, exists := s.exclusiveHolds[volumeID]; exists {
		if hold.NodeID == req.GetNodeId() && (req.GetMountId() == "" || hold.MountID == req.GetMountId()) {
			delete(s.exclusiveHolds, volumeID)
		} else if time.Now().After(hold.LeaseExpiry) {
			delete(s.exclusiveHolds, volumeID)
		}
	}

	return &pb.ReleaseExclusiveResponse{}, nil
}

// GetExclusiveHold returns the current exclusive-local hold for a volume, or empty if no active hold exists.
func (s *Server) GetExclusiveHold(ctx context.Context, req *pb.GetExclusiveHoldRequest) (*pb.GetExclusiveHoldResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}

	volumeID := req.GetVolumeId()
	s.exclusiveHoldsMu.RLock()
	defer s.exclusiveHoldsMu.RUnlock()

	hold, exists := s.exclusiveHolds[volumeID]
	if !exists || time.Now().After(hold.LeaseExpiry) {
		return &pb.GetExclusiveHoldResponse{}, nil
	}

	return &pb.GetExclusiveHoldResponse{
		Hold: hold.toProto(),
	}, nil
}

// RepairVolume repairs a degraded volume by closing its stream and segment store,
// moving the volume's local segment files aside as <dir>/stream-<id>.<unix-ts>.degraded, and reopening
// the stream to resume from the buffer witness watermark.
func (s *Server) RepairVolume(ctx context.Context, req *pb.RepairVolumeRequest) (*pb.RepairVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	if !isValidVolumeID(volumeID) {
		return nil, status.Errorf(codes.InvalidArgument, "invalid volume ID %q", volumeID)
	}

	s.mu.RLock()
	vol, ok := s.volumes[volumeID]
	s.mu.RUnlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "volume %s not found", volumeID)
	}

	if deg := vol.Degraded(); deg == nil && !req.GetForce() {
		return nil, status.Errorf(codes.FailedPrecondition, "volume %s is not degraded (use --force to repair anyway)", volumeID)
	}

	stream := vol.Stream()
	if stream == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "volume %s has no WAL stream configured", volumeID)
	}

	if err := stream.Close(); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to close stream for volume %s: %v", volumeID, err)
	}

	var oldPath string
	if s.wal.dir != "" {
		movedPath, err := walclient.MoveSegmentsAside(s.wal.dir, vol.StreamID())
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to move segment directory aside for volume %s: %v", volumeID, err)
		}
		oldPath = movedPath
	}

	newStream, err := s.openStreamForVolume(volumeID, s.wal.dir)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to reopen stream for volume %s: %v", volumeID, err)
	}
	if newStream == nil {
		return nil, status.Errorf(codes.Internal, "no stream opened for volume %s", volumeID)
	}

	if err := vol.ResetStream(newStream); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to reset volume stream for volume %s: %v", volumeID, err)
	}

	_, witnessWatermark, _ := newStream.Watermarks()

	return &pb.RepairVolumeResponse{
		OldPath:      oldPath,
		NewWatermark: witnessWatermark,
	}, nil
}

// ServerStats contains aggregated operational and capacity metrics for the controller server.
type ServerStats struct {
	LocalStagingBytes         int64
	MaxLocalStagingBytes      int64
	LocalPackCacheBytes       int64
	LocalPackCacheTargetBytes int64
}

// Stats returns a snapshot of controller operational and capacity metrics.
// Requires no locks held; acquires diskLimits.mu.
func (s *Server) Stats() ServerStats {
	s.diskLimits.mu.Lock()
	defer s.diskLimits.mu.Unlock()

	return ServerStats{
		LocalStagingBytes:         s.diskLimits.stagedBytes,
		MaxLocalStagingBytes:      s.diskLimits.maxStagingBytes,
		LocalPackCacheBytes:       s.diskLimits.packCacheBytes,
		LocalPackCacheTargetBytes: s.diskLimits.packCacheTargetBytes,
	}
}

// addLocalPackCacheBytes adds delta to the controller-wide total of uploaded pack bytes kept in the local cache across all volumes.
// Always called with the volume's stagingMu held; requires no Server lock held. Acquires diskLimits.mu.
// Lock order: Server.mu, then Volume.stagingMu, then serverDiskLimits.mu.
func (s *Server) addLocalPackCacheBytes(delta int64) {
	s.diskLimits.mu.Lock()
	s.diskLimits.packCacheBytes += delta
	wake := s.diskLimits.packCacheTargetBytes > 0 && s.diskLimits.packCacheBytes > s.diskLimits.packCacheTargetBytes
	s.diskLimits.mu.Unlock()
	if wake {
		s.evictor.wakeEvictor()
	}
}

// subLocalPackCacheBytes subtracts delta from the controller-wide total of uploaded pack bytes kept in the local cache across all volumes.
// Always called with the volume's stagingMu held; requires no Server lock held. Acquires diskLimits.mu.
// Lock order: Server.mu, then Volume.stagingMu, then serverDiskLimits.mu.
func (s *Server) subLocalPackCacheBytes(delta int64) {
	s.diskLimits.mu.Lock()
	s.diskLimits.packCacheBytes -= delta
	if s.diskLimits.packCacheBytes < 0 {
		s.diskLimits.packCacheBytes = 0
		s.diskLimits.packCacheUnderflows.Inc()
	}
	s.diskLimits.mu.Unlock()
}

// addStagedDiskBytes adds delta to the controller-wide staged byte count and wakes
// the background drain loop if the total is at or above the high-water mark.
func (s *Server) addStagedDiskBytes(delta int64) {
	if s.diskLimits.add(delta) {
		s.drain.wakeDrain()
	}
}

// onStagedBytesReleased notifies the server that a volume released staged bytes after a sync,
// waking the drain loop to evaluate whether further draining is needed.
func (s *Server) onStagedBytesReleased() {
	s.drain.wakeDrain()
}

// add adds delta to the controller-wide total of unuploaded staged bytes
// and returns whether the new total is at or above the high-water mark.
// Always called with the volume's stagingMu held. Lock order: Server.mu, then Volume.stagingMu, then serverDiskLimits.mu.
func (l *serverDiskLimits) add(delta int64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.stagedBytes += delta
	return l.maxStagingBytes > 0 && l.stagedBytes >= (l.maxStagingBytes*stagingHighWaterPercent/100)
}

// subtract subtracts delta from the controller-wide total of unuploaded staged bytes
// and signals waiters on spaceAvailable.
// Always called with the volume's stagingMu held. Lock order: Server.mu, then Volume.stagingMu, then serverDiskLimits.mu.
func (l *serverDiskLimits) subtract(delta int64) {
	l.mu.Lock()
	l.stagedBytes -= delta
	l.spaceAvailable.Broadcast()
	l.mu.Unlock()
}

const (
	// stagingWaitDeadlineMargin is the duration subtracted from an incoming context deadline
	// before which waiting stops, allowing the ENOSPC response to be delivered before the client gives up with DeadlineExceeded.
	stagingWaitDeadlineMargin = 500 * time.Millisecond

	// stagingWaitIndefiniteTimeout is the maximum duration a writer will wait when the incoming context has no deadline.
	stagingWaitIndefiniteTimeout = 30 * time.Second
)

// logCeilingReachedLocked emits a rate-limited log line when the staging ceiling wait times out.
// Requires l.mu held.
func (l *serverDiskLimits) logCeilingReachedLocked(ctx context.Context, volumeID string) {
	now := time.Now()
	if l.lastCeilingLog.IsZero() || now.Sub(l.lastCeilingLog) >= 5*time.Second {
		l.lastCeilingLog = now
		scoped.Log(ctx).Info("local staging ceiling reached",
			"volume", volumeID,
			"total", l.stagedBytes,
			"ceiling", l.maxStagingBytes)
	}
}

// wait enforces backpressure when total staged bytes would exceed maxStagingBytes.
// If the write can never fit (neededBytes > maxStagingBytes), it returns ENOSPC immediately.
// Otherwise, it initiates coalesce syncs and waits on spaceAvailable until total staged bytes
// fall far enough, the write call's deadline is within stagingWaitDeadlineMargin (500ms),
// or stagingWaitIndefiniteTimeout (30s) elapses for deadline-free contexts.
//
// The ceiling is a soft bound: concurrent writers that each pass capacity checks when space is available
// can proceed simultaneously and add on top of each other, exceeding the ceiling by up to the sum of
// in-flight touched chunk writes (or by up to one write's touched chunks, numChunks * chunkSize, for sequential writes).
// Other paths that stage bytes (such as inline data migration or metadata operations) are not covered by this check.
//
// If ctx is cancelled, wait returns ctx.Err() without logging. If the wait expires, it logs a rate-limited
// message and returns ENOSPC.
// Requires no locks held; acquires l.mu and calls triggerSyncs with no locks held.
func (l *serverDiskLimits) wait(ctx context.Context, volumeID string, neededBytes int64, triggerSyncs func()) error {
	if neededBytes <= 0 {
		return nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	ceiling := l.maxStagingBytes
	if ceiling <= 0 {
		return nil
	}
	if neededBytes > ceiling {
		return syscall.ENOSPC
	}
	if l.stagedBytes+neededBytes <= ceiling {
		return nil
	}

	now := time.Now()
	var waitCtx context.Context
	var waitCancel context.CancelFunc
	if deadline, hasDeadline := ctx.Deadline(); hasDeadline {
		waitDeadline := deadline.Add(-stagingWaitDeadlineMargin)
		if !now.Before(waitDeadline) {
			l.logCeilingReachedLocked(ctx, volumeID)
			return syscall.ENOSPC
		}
		waitCtx, waitCancel = context.WithDeadline(ctx, waitDeadline)
	} else {
		waitCtx, waitCancel = context.WithTimeout(ctx, stagingWaitIndefiniteTimeout)
	}
	defer waitCancel()

	stopTimer := context.AfterFunc(waitCtx, func() {
		l.mu.Lock()
		l.spaceAvailable.Broadcast()
		l.mu.Unlock()
	})
	defer stopTimer()

	for l.maxStagingBytes > 0 && l.stagedBytes+neededBytes > l.maxStagingBytes && waitCtx.Err() == nil {
		l.mu.Unlock()
		triggerSyncs()
		l.mu.Lock()
		if l.maxStagingBytes > 0 && l.stagedBytes+neededBytes > l.maxStagingBytes && waitCtx.Err() == nil {
			l.spaceAvailable.Wait()
		}
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}
	if waitCtx.Err() != nil {
		l.logCeilingReachedLocked(ctx, volumeID)
		return syscall.ENOSPC
	}
	return nil
}

// volumeStagedBytes pairs a registered volume with its snapshot of unuploaded staged bytes
// for sorting candidate volumes at the staging ceiling.
type volumeStagedBytes struct {
	// volume is the candidate Volume holding staged data.
	volume *Volume
	// stagedBytes is the number of unuploaded staged bytes measured on volume under stagingMu.
	stagedBytes int64
}

// triggerStagedVolumesSync initiates coalesce syncs for all volumes holding staged bytes,
// ordered descending by each volume's unuploaded staged byte count.
//
// Lock hierarchy and order:
// Requires no locks held upon entry.
// It acquires s.mu for reading to take a snapshot slice of registered volumes and releases s.mu
// before inspecting any volume. Each volume's stagingMu is then acquired individually via StagedBytes
// to read its unuploaded staged byte count without holding s.mu across any volume lock to prevent
// lock-order deadlocks. Volumes with staged bytes are sorted in descending order of staged bytes
// and triggerSyncIfStaged (which acquires stagingMu) is called in that order.
func (s *Server) triggerStagedVolumesSync() {
	s.mu.RLock()
	volumes := make([]*Volume, 0, len(s.volumes))
	for _, volume := range s.volumes {
		volumes = append(volumes, volume)
	}
	s.mu.RUnlock()

	stagedVolumes := make([]volumeStagedBytes, 0, len(volumes))
	for _, volume := range volumes {
		stagedBytes := volume.StagedBytes()
		if stagedBytes > 0 {
			stagedVolumes = append(stagedVolumes, volumeStagedBytes{
				volume:      volume,
				stagedBytes: stagedBytes,
			})
		}
	}

	sort.Slice(stagedVolumes, func(i, j int) bool {
		if stagedVolumes[i].stagedBytes != stagedVolumes[j].stagedBytes {
			return stagedVolumes[i].stagedBytes > stagedVolumes[j].stagedBytes
		}
		return stagedVolumes[i].volume.volumeID < stagedVolumes[j].volume.volumeID
	})

	for _, entry := range stagedVolumes {
		entry.volume.triggerSyncIfStaged()
	}
}

// volumeDrainResult reports the outcome of an individual volume coalesce sync started by the drain loop.
type volumeDrainResult struct {
	// volumeID identifies the volume whose sync finished.
	volumeID string
	// err is the error result from the sync, or nil on success.
	err error
}

// drainLoop runs a background loop responding to staging wake-up signals until ctx is cancelled.
// Draining starts when unuploaded staged bytes cross the 75% high-water mark, and stops
// once unuploaded staged bytes fall at or below the 50% low-water mark.
// Requires no locks held upon entry.
func (s *Server) drainLoop(ctx context.Context) {
	defer close(s.drain.loopDone)

	draining := false
	skippedThisEpisode := make(map[string]bool)
	// activeDrainSyncs tracks unuploaded staged bytes for volumes currently syncing from a drain start.
	// Only syncs the drain started count toward the limit; syncs started by a volume's own timer,
	// an fsync, a close or a writer at the ceiling do not.
	activeDrainSyncs := make(map[string]int64)
	drainDone := make(chan volumeDrainResult, 16)

	handleDrainResult := func(result volumeDrainResult) {
		s.drain.volumeResultsHandled.Inc()
		delete(activeDrainSyncs, result.volumeID)
		if result.err != nil {
			if draining {
				skippedThisEpisode[result.volumeID] = true
			}
			scoped.Log(ctx).Error(result.err, "drain sync failed", "volume", result.volumeID)
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.drain.wake:
		case result := <-drainDone:
			handleDrainResult(result)
		}

		for drainingMore := true; drainingMore; {
			select {
			case result := <-drainDone:
				handleDrainResult(result)
			default:
				drainingMore = false
			}
		}

		s.diskLimits.mu.Lock()
		maxStagingBytes := s.diskLimits.maxStagingBytes
		stagedBytes := s.diskLimits.stagedBytes
		s.diskLimits.mu.Unlock()

		if maxStagingBytes <= 0 {
			draining = false
			clear(skippedThisEpisode)
			s.drain.passesCompleted.Inc()
			continue
		}

		highWaterBytes := maxStagingBytes * stagingHighWaterPercent / 100
		lowWaterBytes := maxStagingBytes * stagingLowWaterPercent / 100

		if stagedBytes <= lowWaterBytes {
			draining = false
			clear(skippedThisEpisode)
			s.drain.passesCompleted.Inc()
			continue
		}

		if stagedBytes >= highWaterBytes {
			if !draining {
				draining = true
				clear(skippedThisEpisode)
			}
		}

		if !draining {
			s.drain.passesCompleted.Inc()
			continue
		}

		s.mu.RLock()
		volumes := make([]*Volume, 0, len(s.volumes))
		for _, registeredVolume := range s.volumes {
			volumes = append(volumes, registeredVolume)
		}
		s.mu.RUnlock()

		var totalInFlight int64
		for _, activeBytes := range activeDrainSyncs {
			totalInFlight += activeBytes
		}

		candidates := make([]volumeStagedBytes, 0, len(volumes))
		for _, candidateVolume := range volumes {
			candidateStagedBytes, isSyncRunning := candidateVolume.stagingStatus()
			if isSyncRunning {
				if _, isTrackedByDrain := activeDrainSyncs[candidateVolume.volumeID]; !isTrackedByDrain {
					totalInFlight += candidateStagedBytes
				}
				continue
			}
			if candidateStagedBytes <= 0 || skippedThisEpisode[candidateVolume.volumeID] {
				continue
			}
			if _, isTrackedByDrain := activeDrainSyncs[candidateVolume.volumeID]; isTrackedByDrain {
				continue
			}
			if candidateStagedBytes < stagingDrainMinimumBytes {
				continue
			}
			candidates = append(candidates, volumeStagedBytes{
				volume:      candidateVolume,
				stagedBytes: candidateStagedBytes,
			})
		}

		sort.Slice(candidates, func(i, j int) bool {
			if candidates[i].stagedBytes != candidates[j].stagedBytes {
				return candidates[i].stagedBytes > candidates[j].stagedBytes
			}
			return candidates[i].volume.volumeID < candidates[j].volume.volumeID
		})

		for _, candidate := range candidates {
			// Only syncs started by the drain count toward stagingDrainMaxConcurrentSyncs; syncs started
			// by a volume's own timer, an fsync, a close or a writer at the ceiling do not.
			if len(activeDrainSyncs) >= stagingDrainMaxConcurrentSyncs {
				break
			}
			if stagedBytes-totalInFlight <= lowWaterBytes {
				break
			}
			targetVolume := candidate.volume
			targetVolumeID := targetVolume.volumeID
			stagedBytesToDrain := targetVolume.triggerDrainSync()
			if stagedBytesToDrain > 0 {
				activeDrainSyncs[targetVolumeID] = stagedBytesToDrain
				totalInFlight += stagedBytesToDrain
				go func(syncVolume *Volume, syncVolumeID string) {
					syncErr := syncVolume.runCoalesceSync()
					select {
					case drainDone <- volumeDrainResult{volumeID: syncVolumeID, err: syncErr}:
					case <-ctx.Done():
					}
				}(targetVolume, targetVolumeID)
			}
		}

		s.drain.passesCompleted.Inc()
	}
}
