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
	walpb "github.com/gke-labs/in-cluster-storage/pkg/api/wal/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/errutil"
	"github.com/gke-labs/in-cluster-storage/pkg/grpcutil"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/blob"
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

type Server struct {
	pb.UnimplementedObjectFSControllerServer
	mu          sync.RWMutex
	volumes     map[string]*Volume
	backend     ObjectStorageBackend
	blobStore   *blob.Store
	broadcaster *EventBroadcaster

	walDir                   string
	walTarget                string
	walDialOpts              []grpc.DialOption
	walConn                  *grpc.ClientConn
	walClient                walpb.WalBufferClient
	walErr                   error
	defaultDurability        walclient.Level
	metadataIndex            string
	metadataDir              string
	metadataCacheEntries     int
	metadataCacheBytes       int64
	metadataOverlayMaxBytes  int64
	metadataApplierBatchSize int
	localStorageDir          string
	debugStatsCheck          bool
	debugMutationCheck       bool
	viewOpts                 []view.Option
	streamFactory            func(volumeID string) (walclient.Stream, error)
	syncCoalesceWindow       time.Duration
	maxStagingBytes          int64
	syncTimeout              time.Duration
	acceptUnversioned        bool

	flushTicker *time.Ticker
	stopFlush   chan struct{}
	flushWg     sync.WaitGroup

	checkpointMu      sync.Mutex
	checkpointWaiters map[string]chan uint64

	exclusiveHoldsMu  sync.RWMutex
	exclusiveHolds    map[string]*ExclusiveHold
	exclusiveLeaseTTL time.Duration
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

// WithServerMutationCheck enables debug mutation checking on metadata views created by the server.
func WithServerMutationCheck() ServerOption {
	return func(s *Server) {
		s.debugMutationCheck = true
		s.viewOpts = append(s.viewOpts, view.WithMutationCheck())
		s.debugStatsCheck = true
	}
}

// WithServerStatsCheck enables debug stats assertions across all volumes.
func WithServerStatsCheck() ServerOption {
	return func(s *Server) {
		s.debugStatsCheck = true
	}
}

// WithoutServerStatsCheck disables debug stats assertions.
func WithoutServerStatsCheck() ServerOption {
	return func(s *Server) {
		s.debugStatsCheck = false
	}
}

// WithServerViewOptions appends custom view options for all volumes created by the server.
func WithServerViewOptions(opts ...view.Option) ServerOption {
	return func(s *Server) {
		s.viewOpts = append(s.viewOpts, opts...)
	}
}

// WithServerMetadataIndex configures the metadata index engine ("sqlite" or "memory").
func WithServerMetadataIndex(index string) ServerOption {
	return func(s *Server) {
		s.metadataIndex = index
	}
}

// WithServerMetadataDir configures the base directory for persistent local metadata storage (e.g. SQLite databases).
func WithServerMetadataDir(dir string) ServerOption {
	return func(s *Server) {
		s.metadataDir = dir
	}
}

// WithServerWAL configures the WAL directory, buffer target, and default durability level.
func WithServerWAL(walDir, walTarget string, durability walclient.Level) ServerOption {
	return func(s *Server) {
		s.walDir = walDir
		s.walTarget = walTarget
		s.defaultDurability = durability
	}
}

// WithServerWALDialOptions configures additional gRPC dial options when dialing the WAL buffer.
// This is the extension point for TLS/mTLS credentials (e.g. issue #319).
func WithServerWALDialOptions(opts ...grpc.DialOption) ServerOption {
	return func(s *Server) {
		s.walDialOpts = append(s.walDialOpts, opts...)
	}
}

// WithServerStreamFactory sets a custom stream factory function (useful for testing).
func WithServerStreamFactory(factory func(volumeID string) (walclient.Stream, error)) ServerOption {
	return func(s *Server) {
		s.streamFactory = factory
	}
}

// WithServerDurability sets the default durability level for streams.
func WithServerDurability(durability walclient.Level) ServerOption {
	return func(s *Server) {
		s.defaultDurability = durability
	}
}

// WithServerMetadataCacheLimits configures the metadata read cache entry and byte capacity.
func WithServerMetadataCacheLimits(maxEntries int, maxBytes int64) ServerOption {
	return func(s *Server) {
		s.metadataCacheEntries = maxEntries
		s.metadataCacheBytes = maxBytes
	}
}

// WithServerMetadataOverlayMaxBytes sets the maximum byte bound for unapplied overlay rows before applying backpressure.
func WithServerMetadataOverlayMaxBytes(maxBytes int64) ServerOption {
	return func(s *Server) {
		s.metadataOverlayMaxBytes = maxBytes
	}
}

// WithServerMetadataApplierBatchSize sets the maximum batch size for the background SQLite applier.
func WithServerMetadataApplierBatchSize(batchSize int) ServerOption {
	return func(s *Server) {
		s.metadataApplierBatchSize = batchSize
	}
}

// WithServerLocalStorageDir sets the local directory for metadata storage for all volumes.
func WithServerLocalStorageDir(dir string) ServerOption {
	return func(s *Server) {
		s.localStorageDir = dir
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

// WithServerSyncTimeout sets the timeout duration for coalesced background sync operations.
func WithServerSyncTimeout(d time.Duration) ServerOption {
	return func(s *Server) {
		if d > 0 {
			s.syncTimeout = d
		}
	}
}

// WithServerAcceptUnversioned configures whether unversioned segments, streams, and snapshots are accepted.
func WithServerAcceptUnversioned(accept bool) ServerOption {
	return func(s *Server) {
		s.acceptUnversioned = accept
	}
}

func NewServer(backend ObjectStorageBackend, opts ...ServerOption) *Server {
	if backend == nil {
		backend = NewMemoryBackend()
	}
	s := &Server{
		volumes:           make(map[string]*Volume),
		backend:           backend,
		blobStore:         blob.NewStore(backend, 0),
		broadcaster:       NewEventBroadcaster(),
		checkpointWaiters: make(map[string]chan uint64),
		exclusiveHolds:    make(map[string]*ExclusiveHold),
		exclusiveLeaseTTL: 30 * time.Second,
		syncTimeout:       10 * time.Minute,
		defaultDurability: walclient.Local,
	}
	for _, opt := range opts {
		opt(s)
	}
	if s.walTarget != "" {
		dialOpts := append(grpcutil.DialOptions(grpc.WithTransportCredentials(insecure.NewCredentials())), s.walDialOpts...)
		conn, err := grpc.NewClient(s.walTarget, dialOpts...)
		if err != nil {
			klog.Errorf("failed to dial WAL buffer target %s: %v", s.walTarget, err)
			s.walErr = err
		} else {
			s.walConn = conn
			s.walClient = walpb.NewWalBufferClient(conn)
		}
	}
	return s
}

func (s *Server) openStreamForVolume(volumeID string, dir string) (walclient.Stream, error) {
	if s.streamFactory != nil {
		return s.streamFactory(volumeID)
	}
	if dir == "" {
		dir = s.walDir
	}
	if dir == "" {
		return nil, nil
	}
	if s.walErr != nil {
		return nil, fmt.Errorf("failed to dial WAL buffer target %s: %w", s.walTarget, s.walErr)
	}
	streamID := StreamIDForVolume(volumeID)
	var walOpts []walclient.Option
	if s.acceptUnversioned {
		walOpts = append(walOpts, walclient.WithAcceptUnversioned(true))
	}
	if s.walClient != nil {
		walOpts = append(walOpts, walclient.WithGRPCClient(s.walClient))
	}
	return walclient.Open(context.Background(), dir, streamID, s.walTarget, walOpts...)
}

func (s *Server) getOrCreateVolume(volumeID string) (*Volume, error) {
	if !isValidVolumeID(volumeID) {
		return nil, status.Errorf(codes.InvalidArgument, "invalid volume ID %q (must match [A-Za-z0-9._-] and not be '.' or '..')", volumeID)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	vol, ok := s.volumes[volumeID]
	if !ok {
		var volOpts []VolumeOption
		stream, err := s.openStreamForVolume(volumeID, s.walDir)
		if err != nil {
			return nil, fmt.Errorf("failed to open stream for volume %s: %w", volumeID, err)
		}
		if stream != nil {
			volOpts = append(volOpts, WithStream(stream))
		}
		volOpts = append(volOpts, WithDurability(s.defaultDurability))
		if s.metadataIndex != "" {
			volOpts = append(volOpts, WithMetadataIndex(s.metadataIndex))
		}
		if s.metadataDir != "" {
			volDir := filepath.Join(s.metadataDir, volumeID)
			if err := os.MkdirAll(volDir, 0755); err != nil {
				return nil, fmt.Errorf("failed to create metadata dir %s for volume %s: %w", volDir, volumeID, err)
			}
			volOpts = append(volOpts, WithLocalStorageDir(volDir))
		}
		if s.metadataCacheEntries > 0 || s.metadataCacheBytes > 0 {
			volOpts = append(volOpts, WithMetadataCacheLimits(s.metadataCacheEntries, s.metadataCacheBytes))
		}
		if s.metadataOverlayMaxBytes > 0 {
			volOpts = append(volOpts, WithMaxUnappliedBytes(s.metadataOverlayMaxBytes))
		}
		if s.metadataApplierBatchSize > 0 {
			volOpts = append(volOpts, WithApplierBatchSize(s.metadataApplierBatchSize))
		}
		if s.localStorageDir != "" {
			volOpts = append(volOpts, WithLocalStorageDir(s.localStorageDir))
		}
		if len(s.viewOpts) > 0 {
			volOpts = append(volOpts, WithVolumeViewOptions(s.viewOpts...))
		}
		if s.debugMutationCheck {
			volOpts = append(volOpts, WithVolumeMutationCheck())
		}
		if s.debugStatsCheck {
			volOpts = append(volOpts, WithVolumeStatsCheck())
		}
		if s.syncCoalesceWindow > 0 {
			volOpts = append(volOpts, WithSyncCoalesceWindow(s.syncCoalesceWindow))
		}
		if s.maxStagingBytes > 0 {
			volOpts = append(volOpts, WithMaxStagingBytes(s.maxStagingBytes))
		}
		if s.syncTimeout > 0 {
			volOpts = append(volOpts, WithSyncTimeout(s.syncTimeout))
		}
		if s.acceptUnversioned {
			volOpts = append(volOpts, WithAcceptUnversioned(true))
		}

		vol, err = NewVolume(volumeID, s.backend, s.broadcaster, volOpts...)
		if err != nil {
			return nil, fmt.Errorf("failed to create volume %s: %w", volumeID, err)
		}
		if err := vol.LoadFromBackend(context.Background()); err != nil {
			return nil, fmt.Errorf("failed to load volume %s from backend: %w", volumeID, err)
		}
		s.volumes[volumeID] = vol
	}
	return vol, nil
}

// Close stops periodic flushing and closes all active volumes and streams.
func (s *Server) Close() error {
	var firstErr error
	if err := s.StopPeriodicFlush(); err != nil && firstErr == nil {
		firstErr = err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, v := range s.volumes {
		if err := v.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if s.walConn != nil {
		if err := s.walConn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		s.walConn = nil
		s.walClient = nil
	}
	return firstErr
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

func (s *Server) StopPeriodicFlush() error {
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
	if hadTicker {
		return s.FlushAll(context.Background())
	}
	return nil
}

// GetVolume returns the Volume instance for the given volumeID, or an error if creation fails.
func (s *Server) GetVolume(volumeID string) (*Volume, error) {
	return s.getOrCreateVolume(volumeID)
}

// RestoreSnapshot restores the volume filesystem state to a specific snapshot.
// The restore operation is logged to the stream and published as a fresh snapshot,
// making it durable across controller restarts and rebuilds.
func (s *Server) RestoreSnapshot(ctx context.Context, volumeID, snapshotName string) error {
	vol, err := s.getOrCreateVolume(volumeID)
	if err != nil {
		return err
	}
	return vol.RestoreSnapshot(ctx, snapshotName)
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
	attr, err := vol.Mkdir(ctx, req.GetParentInode(), req.GetName(), req.GetMode(), req.GetUid(), req.GetGid())
	if err != nil {
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
	attr, err := vol.CreateFile(ctx, req.GetParentInode(), req.GetName(), req.GetMode(), req.GetInitialContent(), req.GetUid(), req.GetGid(), req.GetRdev())
	if err != nil {
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
	pos, err := vol.Unlink(ctx, req.GetParentInode(), req.GetName())
	if err != nil {
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
	pos, err := vol.Rmdir(ctx, req.GetParentInode(), req.GetName())
	if err != nil {
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
	attr, err := vol.Rename(ctx, req.GetOldParentInode(), req.GetOldName(), req.GetNewParentInode(), req.GetNewName())
	if err != nil {
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
	attr, err := vol.Symlink(ctx, req.GetParentInode(), req.GetName(), req.GetTarget(), req.GetUid(), req.GetGid())
	if err != nil {
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
	attr, err := vol.Link(ctx, req.GetOldInode(), req.GetNewParentInode(), req.GetNewName())
	if err != nil {
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
	fh, err := vol.Open(ctx, req.GetInode(), req.GetFlags())
	if err != nil {
		return &pb.OpenResponse{Error: volErrToSyscall(err)}, nil
	}
	return &pb.OpenResponse{Fh: fh}, nil
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

// Broadcaster returns the EventBroadcaster for the server.
func (s *Server) Broadcaster() *EventBroadcaster {
	return s.broadcaster
}

func (s *Server) WatchVolume(req *pb.WatchVolumeRequest, stream pb.ObjectFSController_WatchVolumeServer) error {
	if !isValidVolumeID(req.GetVolumeId()) {
		return status.Errorf(codes.InvalidArgument, "invalid volume ID %q (must match [A-Za-z0-9._-] and not be '.' or '..')", req.GetVolumeId())
	}

	ch := s.broadcaster.Subscribe(req.GetVolumeId())
	defer s.broadcaster.Unsubscribe(req.GetVolumeId(), ch)

	ctx := stream.Context()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
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
			trimmed := strings.TrimPrefix(obj, "volumes/")
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
		var degraded *pb.Degraded
		if vol, ok := s.volumes[id]; ok {
			if deg := vol.Degraded(); deg != nil {
				degraded = &pb.Degraded{
					Code:    pb.Degraded_WAL_STREAM_DEGRADED,
					Message: deg.Error(),
					Since:   timestamppb.New(deg.At),
				}
			}
		}
		volInfos = append(volInfos, &pb.VolumeInfo{
			VolumeId: id,
			Degraded: degraded,
		})
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
		return nil, status.Errorf(codes.Internal, "%v", err)
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
	if s.walDir != "" {
		movedPath, err := walclient.MoveSegmentsAside(s.walDir, vol.StreamID())
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to move segment directory aside for volume %s: %v", volumeID, err)
		}
		oldPath = movedPath
	}

	newStream, err := s.openStreamForVolume(volumeID, s.walDir)
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
