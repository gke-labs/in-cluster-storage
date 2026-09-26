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
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/blob"
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func volErrToStatus(op string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, syscall.ENOENT) {
		return status.Errorf(codes.NotFound, "%s failed: %v", op, err)
	}
	if errors.Is(err, syscall.EEXIST) {
		return status.Errorf(codes.AlreadyExists, "%s failed: %v", op, err)
	}
	if errors.Is(err, syscall.ENOTEMPTY) || errors.Is(err, syscall.EISDIR) || errors.Is(err, syscall.ENOTDIR) || errors.Is(err, syscall.EBUSY) {
		return status.Errorf(codes.FailedPrecondition, "%s failed: %v", op, err)
	}
	if errors.Is(err, syscall.EINVAL) {
		return status.Errorf(codes.InvalidArgument, "%s failed: %v", op, err)
	}
	if errors.Is(err, syscall.EACCES) || errors.Is(err, syscall.EPERM) {
		return status.Errorf(codes.PermissionDenied, "%s failed: %v", op, err)
	}
	if errors.Is(err, syscall.ENOSYS) {
		return status.Errorf(codes.Unimplemented, "%s failed: %v", op, err)
	}
	if errors.Is(err, syscall.ETIMEDOUT) {
		return status.Errorf(codes.DeadlineExceeded, "%s failed: %v", op, err)
	}
	if errors.Is(err, syscall.ENOSPC) {
		return status.Errorf(codes.ResourceExhausted, "%s failed: %v", op, err)
	}
	return status.Errorf(codes.Internal, "%s failed: %v", op, err)
}

type Server struct {
	pb.UnimplementedObjectFSControllerServer
	mu          sync.RWMutex
	volumes     map[string]*Volume
	backend     ObjectStorageBackend
	blobStore   *blob.Store
	broadcaster *EventBroadcaster

	walDir            string
	walTarget         string
	defaultDurability walclient.Level
	streamFactory     func(volumeID string) (walclient.Stream, error)

	flushTicker *time.Ticker
	stopFlush   chan struct{}
	flushWg     sync.WaitGroup
}

// ServerOption configures the controller Server.
type ServerOption func(*Server)

// WithServerWAL configures the WAL directory, buffer target, and default durability level.
func WithServerWAL(walDir, walTarget string, durability walclient.Level) ServerOption {
	return func(s *Server) {
		s.walDir = walDir
		s.walTarget = walTarget
		s.defaultDurability = durability
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

func NewServer(backend ObjectStorageBackend, opts ...ServerOption) *Server {
	if backend == nil {
		backend = NewMemoryBackend()
	}
	s := &Server{
		volumes:           make(map[string]*Volume),
		backend:           backend,
		blobStore:         blob.NewStore(backend, 0),
		broadcaster:       NewEventBroadcaster(),
		defaultDurability: walclient.Local,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (s *Server) getOrCreateVolume(volumeID string) *Volume {
	s.mu.Lock()
	defer s.mu.Unlock()

	vol, ok := s.volumes[volumeID]
	if !ok {
		var volOpts []VolumeOption
		if s.streamFactory != nil {
			stream, err := s.streamFactory(volumeID)
			if err == nil && stream != nil {
				volOpts = append(volOpts, WithStream(stream))
			}
		} else if s.walDir != "" {
			streamID := StreamIDForVolume(volumeID)
			stream, err := walclient.Open(context.Background(), s.walDir, streamID, s.walTarget)
			if err == nil {
				volOpts = append(volOpts, WithStream(stream))
			}
		}
		volOpts = append(volOpts, WithDurability(s.defaultDurability))

		vol = NewVolume(volumeID, s.backend, s.broadcaster, volOpts...)
		_ = vol.LoadFromBackend(context.Background())
		s.volumes[volumeID] = vol
	}
	return vol
}

// Close stops periodic flushing and closes all active volumes and streams.
func (s *Server) Close() error {
	s.StopPeriodicFlush()
	s.mu.Lock()
	defer s.mu.Unlock()
	var firstErr error
	for _, v := range s.volumes {
		if err := v.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
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
				_ = s.FlushAll(context.Background())
				return
			case <-stopFlush:
				_ = s.FlushAll(context.Background())
				return
			case <-ticker.C:
				_ = s.FlushAll(ctx)
			}
		}
	}()
}

func (s *Server) StopPeriodicFlush() {
	s.mu.Lock()
	if s.flushTicker != nil {
		s.flushTicker.Stop()
		close(s.stopFlush)
		s.flushTicker = nil
		s.stopFlush = nil
	}
	s.mu.Unlock()
	s.flushWg.Wait()
}

// GetVolume returns the Volume instance for the given volumeID.
func (s *Server) GetVolume(volumeID string) *Volume {
	return s.getOrCreateVolume(volumeID)
}

// RestoreSnapshot restores the volume filesystem state to a specific EROFS snapshot.
func (s *Server) RestoreSnapshot(ctx context.Context, volumeID, snapshotName string) error {
	vol := s.getOrCreateVolume(volumeID)
	return vol.RestoreSnapshot(ctx, snapshotName)
}

func (s *Server) GetAttr(ctx context.Context, req *pb.GetAttrRequest) (*pb.GetAttrResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	vol := s.getOrCreateVolume(req.GetVolumeId())
	attr, err := vol.GetAttr(ctx, req.GetPath())
	if err != nil {
		return nil, volErrToStatus("get attr", err)
	}
	return &pb.GetAttrResponse{Attr: attr}, nil
}

func (s *Server) Lookup(ctx context.Context, req *pb.LookupRequest) (*pb.LookupResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	vol := s.getOrCreateVolume(req.GetVolumeId())
	attr, err := vol.Lookup(ctx, req.GetParentPath(), req.GetName())
	if err != nil {
		return nil, volErrToStatus("lookup", err)
	}
	return &pb.LookupResponse{Attr: attr}, nil
}

func (s *Server) ReadDir(ctx context.Context, req *pb.ReadDirRequest) (*pb.ReadDirResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	vol := s.getOrCreateVolume(req.GetVolumeId())
	entries, err := vol.ReadDir(ctx, req.GetPath())
	if err != nil {
		return nil, volErrToStatus("readdir", err)
	}
	return &pb.ReadDirResponse{Entries: entries}, nil
}

func (s *Server) Mkdir(ctx context.Context, req *pb.MkdirRequest) (*pb.MkdirResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	vol := s.getOrCreateVolume(req.GetVolumeId())
	attr, err := vol.Mkdir(ctx, req.GetPath(), req.GetMode())
	if err != nil {
		return nil, volErrToStatus("mkdir", err)
	}
	return &pb.MkdirResponse{Attr: attr}, nil
}

func (s *Server) CreateFile(ctx context.Context, req *pb.CreateFileRequest) (*pb.CreateFileResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	vol := s.getOrCreateVolume(req.GetVolumeId())
	attr, err := vol.CreateFile(ctx, req.GetPath(), req.GetMode(), req.GetInitialContent())
	if err != nil {
		return nil, volErrToStatus("create file", err)
	}
	return &pb.CreateFileResponse{Attr: attr}, nil
}

func (s *Server) ReadFile(ctx context.Context, req *pb.ReadFileRequest) (*pb.ReadFileResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	vol := s.getOrCreateVolume(req.GetVolumeId())
	data, totalSize, redirectURL, err := vol.ReadFile(ctx, req.GetPath(), req.GetOffset(), req.GetSize())
	if err != nil {
		return nil, volErrToStatus("read file", err)
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
	vol := s.getOrCreateVolume(req.GetVolumeId())
	bytesWritten, newSize, modTime, err := vol.WriteFile(ctx, req.GetPath(), req.GetOffset(), req.GetData(), req.GetWriteMode())
	if err != nil {
		return nil, volErrToStatus("write file", err)
	}
	return &pb.WriteFileResponse{
		BytesWritten: bytesWritten,
		NewSize:      newSize,
		ModTime:      timestamppb.New(modTime),
	}, nil
}

func (s *Server) TruncateFile(ctx context.Context, req *pb.TruncateFileRequest) (*pb.TruncateFileResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	vol := s.getOrCreateVolume(req.GetVolumeId())
	attr, err := vol.TruncateFile(ctx, req.GetPath(), req.GetSize())
	if err != nil {
		return nil, volErrToStatus("truncate", err)
	}
	return &pb.TruncateFileResponse{Attr: attr}, nil
}

func (s *Server) Unlink(ctx context.Context, req *pb.UnlinkRequest) (*pb.UnlinkResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	vol := s.getOrCreateVolume(req.GetVolumeId())
	if err := vol.Unlink(ctx, req.GetPath()); err != nil {
		return nil, volErrToStatus("unlink", err)
	}
	return &pb.UnlinkResponse{Success: true}, nil
}

func (s *Server) Rmdir(ctx context.Context, req *pb.RmdirRequest) (*pb.RmdirResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	vol := s.getOrCreateVolume(req.GetVolumeId())
	if err := vol.Rmdir(ctx, req.GetPath()); err != nil {
		return nil, volErrToStatus("rmdir", err)
	}
	return &pb.RmdirResponse{Success: true}, nil
}

func (s *Server) Rename(ctx context.Context, req *pb.RenameRequest) (*pb.RenameResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	vol := s.getOrCreateVolume(req.GetVolumeId())
	attr, err := vol.Rename(ctx, req.GetOldPath(), req.GetNewPath())
	if err != nil {
		return nil, volErrToStatus("rename", err)
	}
	return &pb.RenameResponse{Attr: attr}, nil
}

func (s *Server) Fsync(ctx context.Context, req *pb.FsyncRequest) (*pb.FsyncResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	vol := s.getOrCreateVolume(req.GetVolumeId())
	if err := vol.Fsync(ctx, req.GetPath()); err != nil {
		return nil, volErrToStatus("fsync", err)
	}
	return &pb.FsyncResponse{Success: true}, nil
}

func (s *Server) WatchVolume(req *pb.WatchVolumeRequest, stream pb.ObjectFSController_WatchVolumeServer) error {
	if req.GetVolumeId() == "" {
		return status.Error(codes.InvalidArgument, "volume_id is required")
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
	defer bStream.Close()

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

func parseSnapshotTime(name string) time.Time {
	trimmed := strings.TrimSuffix(name, ".erofs")
	formats := []string{
		"20060102T150405.000000Z",
		"20060102T150405Z",
		time.RFC3339Nano,
		time.RFC3339,
	}
	for _, f := range formats {
		if t, err := time.Parse(f, trimmed); err == nil {
			return t
		}
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
		if err == nil {
			for _, obj := range objects {
				trimmed := strings.TrimPrefix(obj, "volumes/")
				parts := strings.Split(trimmed, "/")
				if len(parts) > 0 && parts[0] != "" {
					volMap[parts[0]] = struct{}{}
				}
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
	for _, id := range filtered {
		volInfos = append(volInfos, &pb.VolumeInfo{
			VolumeId: id,
		})
	}

	return &pb.ListVolumesResponse{
		Volumes:   volInfos,
		EndOfData: endOfData,
	}, nil
}

func (s *Server) ListSnapshots(ctx context.Context, req *pb.ListSnapshotsRequest) (*pb.ListSnapshotsResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	vol := s.getOrCreateVolume(req.GetVolumeId())
	allSnapshots, err := vol.ListSnapshots(ctx)
	if err != nil {
		return nil, volErrToStatus("list snapshots", err)
	}

	var fromTime, toTime time.Time
	if req.GetFromTime() != nil {
		fromTime = req.GetFromTime().AsTime()
	}
	if req.GetToTime() != nil {
		toTime = req.GetToTime().AsTime()
	}
	fromSnapshot := req.GetFromSnapshot()

	var filtered []string
	for _, snap := range allSnapshots {
		if fromSnapshot != "" && snap <= fromSnapshot {
			continue
		}
		snapTime := parseSnapshotTime(snap)
		if !fromTime.IsZero() && !snapTime.IsZero() && snapTime.Before(fromTime) {
			continue
		}
		if !toTime.IsZero() && !snapTime.IsZero() && snapTime.After(toTime) {
			continue
		}
		filtered = append(filtered, snap)
	}

	limit := int(req.GetLimit())
	endOfData := true
	if limit > 0 && len(filtered) > limit {
		filtered = filtered[:limit]
		endOfData = false
	}

	var snapInfos []*pb.SnapshotInfo
	for _, snap := range filtered {
		info := &pb.SnapshotInfo{
			Name: snap,
		}
		t := parseSnapshotTime(snap)
		if !t.IsZero() {
			info.CreatedAt = timestamppb.New(t)
		}
		snapInfos = append(snapInfos, info)
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
	vol := s.getOrCreateVolume(req.GetVolumeId())
	snapName, err := vol.CreateSnapshot(ctx)
	if err != nil {
		return nil, volErrToStatus("create snapshot", err)
	}

	snapInfo := &pb.SnapshotInfo{
		Name: snapName,
	}
	t := parseSnapshotTime(snapName)
	if !t.IsZero() {
		snapInfo.CreatedAt = timestamppb.New(t)
	}

	return &pb.CreateSnapshotResponse{
		SnapshotName: snapName,
		Snapshot:     snapInfo,
	}, nil
}
