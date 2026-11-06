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
	"iter"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/wal"
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type durabilityTrackingStream struct {
	appendCount atomic.Int64
	waitCount   atomic.Int64
	lastWaitSeq atomic.Uint64
	degradedErr atomic.Pointer[walclient.DegradedError]
}

func (s *durabilityTrackingStream) Append(ctx context.Context, payload []byte) (uint64, error) {
	if deg := s.degradedErr.Load(); deg != nil {
		return 0, deg
	}
	seq := uint64(s.appendCount.Add(1))
	return seq, nil
}

func (s *durabilityTrackingStream) Wait(ctx context.Context, seq uint64, level walclient.Level, requestFlush bool) error {
	if deg := s.degradedErr.Load(); deg != nil {
		return deg
	}
	s.waitCount.Add(1)
	s.lastWaitSeq.Store(seq)
	return nil
}

func (s *durabilityTrackingStream) Flush(ctx context.Context) error {
	if deg := s.degradedErr.Load(); deg != nil {
		return deg
	}
	return nil
}

func (s *durabilityTrackingStream) Watermarks() (uint64, uint64, uint64) {
	appends := uint64(s.appendCount.Load())
	return appends, appends, appends
}

func (s *durabilityTrackingStream) RecoveredRecords() []*wal.ClientRecord {
	return nil
}

func (s *durabilityTrackingStream) ReplicationLevel() walclient.Level {
	return walclient.Witness
}

func (s *durabilityTrackingStream) Tail(ctx context.Context, fromSeq uint64) (iter.Seq2[uint64, []byte], error) {
	return func(yield func(uint64, []byte) bool) {}, nil
}

func (s *durabilityTrackingStream) Degraded() *walclient.DegradedError {
	return s.degradedErr.Load()
}

func (s *durabilityTrackingStream) Close() error {
	return nil
}

func TestMetadataDurability_BarrierModeDoesNotWaitPerOperation(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	trackingStream := &durabilityTrackingStream{}

	server := NewTestServer(t, backend,
		WithServerStreamFactory(func(volumeID string) (walclient.Stream, error) {
			return trackingStream, nil
		}),
		WithServerDurability(walclient.Witness),
		WithServerMetadataDurability(pb.MetadataDurability_BARRIER),
	)
	defer func() {
		if err := server.Close(); err != nil {
			t.Errorf("server.Close failed: %v", err)
		}
	}()

	volumeID := "barrier-test-volume"

	// 1. CreateFile should not wait on durability under BARRIER mode
	createResponse, err := server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:    volumeID,
		ParentInode: 1,
		Name:        "file1.txt",
		Mode:        0644,
	})
	if err != nil || createResponse.GetError() != 0 {
		t.Fatalf("CreateFile failed: %v (code %d)", err, createResponse.GetError())
	}

	if waits := trackingStream.waitCount.Load(); waits != 0 {
		t.Fatalf("expected 0 durability waits for CreateFile in barrier mode, got %d", waits)
	}

	// 2. SetAttr should not wait on durability under BARRIER mode
	setAttrResponse, err := server.SetAttr(ctx, &pb.SetAttrRequest{
		VolumeId: volumeID,
		Inode:    createResponse.GetAttr().GetInode().GetIno(),
		Mode:     proto.Uint32(0600),
	})
	if err != nil || setAttrResponse.GetError() != 0 {
		t.Fatalf("SetAttr failed: %v (code %d)", err, setAttrResponse.GetError())
	}

	if waits := trackingStream.waitCount.Load(); waits != 0 {
		t.Fatalf("expected 0 durability waits after SetAttr in barrier mode, got %d", waits)
	}

	// 3. Explicit blocking Fsync is a barrier: collects durability for newest commit sequence
	fsyncResponse, err := server.Fsync(ctx, &pb.FsyncRequest{
		VolumeId: volumeID,
		Inode:    createResponse.GetAttr().GetInode().GetIno(),
		Blocking: true,
	})
	if err != nil || !fsyncResponse.GetSuccess() {
		t.Fatalf("Fsync barrier failed: %v (resp %v)", err, fsyncResponse)
	}

	if waits := trackingStream.waitCount.Load(); waits != 1 {
		t.Fatalf("expected exactly 1 durability wait from Fsync barrier, got %d", waits)
	}
	if lastWait := trackingStream.lastWaitSeq.Load(); lastWait == 0 {
		t.Fatalf("expected non-zero lastWaitSeq from Fsync barrier, got %d", lastWait)
	}
}

func TestMetadataDurability_OperationModeWaitsPerOperation(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	trackingStream := &durabilityTrackingStream{}

	server := NewTestServer(t, backend,
		WithServerStreamFactory(func(volumeID string) (walclient.Stream, error) {
			return trackingStream, nil
		}),
		WithServerDurability(walclient.Witness),
		WithServerMetadataDurability(pb.MetadataDurability_OPERATION),
	)
	defer func() {
		if err := server.Close(); err != nil {
			t.Errorf("server.Close failed: %v", err)
		}
	}()

	volumeID := "operation-test-volume"

	// CreateFile should wait on durability under OPERATION mode
	createResponse, err := server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:    volumeID,
		ParentInode: 1,
		Name:        "op_file.txt",
		Mode:        0644,
	})
	if err != nil || createResponse.GetError() != 0 {
		t.Fatalf("CreateFile failed: %v (code %d)", err, createResponse.GetError())
	}

	if waits := trackingStream.waitCount.Load(); waits != 1 {
		t.Fatalf("expected 1 durability wait for CreateFile in operation mode, got %d", waits)
	}
}

func TestMetadataDurability_DeferredCommitFailureSurfacesAtBarrier(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	trackingStream := &durabilityTrackingStream{}

	server := NewTestServer(t, backend,
		WithServerStreamFactory(func(volumeID string) (walclient.Stream, error) {
			return trackingStream, nil
		}),
		WithServerDurability(walclient.Witness),
		WithServerMetadataDurability(pb.MetadataDurability_BARRIER),
	)

	volumeID := "degraded-barrier-volume"

	// 1. Perform metadata mutation under barrier mode (succeeds without wait)
	createResponse, err := server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:    volumeID,
		ParentInode: 1,
		Name:        "deferred.txt",
		Mode:        0644,
	})
	if err != nil || createResponse.GetError() != 0 {
		t.Fatalf("CreateFile failed: %v (code %d)", err, createResponse.GetError())
	}

	// 2. Mark stream degraded
	degradedError := &walclient.DegradedError{Cause: errors.New("storage backend corrupted")}
	trackingStream.degradedErr.Store(degradedError)

	// 3. Barrier (Fsync) must surface the failure
	fsyncResponse, err := server.Fsync(ctx, &pb.FsyncRequest{
		VolumeId: volumeID,
		Inode:    createResponse.GetAttr().GetInode().GetIno(),
		Blocking: true,
	})
	if err != nil {
		t.Fatalf("unexpected gRPC error: %v", err)
	}
	if fsyncResponse.GetError() != int32(syscall.EIO) {
		t.Fatalf("expected EIO from Fsync barrier on degraded stream, got code %d", fsyncResponse.GetError())
	}

	// 4. Mutating operations return EIO once the stream is degraded
	secondCreate, err := server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:    volumeID,
		ParentInode: 1,
		Name:        "after_degraded.txt",
		Mode:        0644,
	})
	if err != nil {
		t.Fatalf("unexpected gRPC error: %v", err)
	}
	if secondCreate.GetError() != int32(syscall.EIO) {
		t.Fatalf("expected EIO for mutating operation after degradation, got code %d", secondCreate.GetError())
	}

	// Server close on degraded volume surfaces the degraded error
	closeErr := server.Close()
	var degErr *walclient.DegradedError
	if !errors.As(closeErr, &degErr) {
		t.Fatalf("server.Close error = %v, want DegradedError", closeErr)
	}
}

func TestCSICreateVolume_MetadataDurabilityParameter(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	server := NewTestServer(t, backend)
	defer func() {
		if err := server.Close(); err != nil {
			t.Errorf("server.Close failed: %v", err)
		}
	}()

	csiController := NewCSIController(server)
	caps := []*csi.VolumeCapability{{
		AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER},
	}}

	// Case 1: metadata-durability = operation
	respOp, err := csiController.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name: "test-csi-op-vol",
		Parameters: map[string]string{
			"metadata-durability": "operation",
		},
		VolumeCapabilities: caps,
	})
	if err != nil {
		t.Fatalf("CreateVolume operation failed: %v", err)
	}
	volOp := mustGetVolume(t, server, respOp.GetVolume().GetVolumeId())
	if volOp.MetadataDurability() != pb.MetadataDurability_OPERATION {
		t.Errorf("volume mode = %v, want %v", volOp.MetadataDurability(), pb.MetadataDurability_OPERATION)
	}

	// Case 2: metadata-durability = barrier
	respBarrier, err := csiController.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name: "test-csi-barrier-vol",
		Parameters: map[string]string{
			"metadata-durability": "barrier",
		},
		VolumeCapabilities: caps,
	})
	if err != nil {
		t.Fatalf("CreateVolume barrier failed: %v", err)
	}
	volBarrier := mustGetVolume(t, server, respBarrier.GetVolume().GetVolumeId())
	if volBarrier.MetadataDurability() != pb.MetadataDurability_BARRIER {
		t.Errorf("volume mode = %v, want %v", volBarrier.MetadataDurability(), pb.MetadataDurability_BARRIER)
	}

	// Case 3: invalid metadata-durability parameter
	_, err = csiController.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name: "test-csi-invalid-vol",
		Parameters: map[string]string{
			"metadata-durability": "unsupported_mode",
		},
		VolumeCapabilities: caps,
	})
	if err == nil {
		t.Fatal("expected error for invalid metadata-durability, got nil")
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument code, got: %v", status.Code(err))
	}
}
