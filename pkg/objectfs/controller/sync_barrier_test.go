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
	"iter"
	"sync/atomic"
	"testing"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/wal"
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
)

type recordingStream struct {
	appendCount      atomic.Int64
	waitCount        atomic.Int64
	lastWaitSequence atomic.Uint64
}

func (s *recordingStream) Append(ctx context.Context, payload []byte) (uint64, error) {
	sequence := uint64(s.appendCount.Add(1))
	return sequence, nil
}

func (s *recordingStream) Wait(ctx context.Context, sequence uint64, level walclient.Level, requestFlush bool) error {
	s.waitCount.Add(1)
	s.lastWaitSequence.Store(sequence)
	return nil
}

func (s *recordingStream) Flush(ctx context.Context) error { return nil }

func (s *recordingStream) Watermarks() (uint64, uint64, uint64) {
	appends := uint64(s.appendCount.Load())
	return appends, appends, appends
}

func (s *recordingStream) RecoveredRecords() []*wal.ClientRecord { return nil }

func (s *recordingStream) ReplicationLevel() walclient.Level { return walclient.Witness }

func (s *recordingStream) Tail(ctx context.Context, fromSequence uint64) (iter.Seq2[uint64, []byte], error) {
	return func(yield func(uint64, []byte) bool) {}, nil
}

func (s *recordingStream) Degraded() *walclient.DegradedError { return nil }

func (s *recordingStream) Close() error { return nil }

func TestSyncBarrier_FsyncWaitsOnNewestCommitSequence(t *testing.T) {
	ctx := t.Context()
	stream := &recordingStream{}
	server := NewTestServer(t, NewMemoryBackend(),
		WithServerStreamFactory(func(volumeID string) (walclient.Stream, error) {
			return stream, nil
		}),
		WithServerDurability(walclient.Witness),
	)
	defer func() {
		if err := server.Close(); err != nil {
			t.Errorf("server.Close failed: %v", err)
		}
	}()

	volumeID := "test-sync-barrier-fsync"
	createResponse, err := server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:    volumeID,
		ParentInode: 1,
		Name:        "file.txt",
		Mode:        0644,
	})
	if err != nil || createResponse.GetError() != 0 {
		t.Fatalf("CreateFile failed: %v (code %d)", err, createResponse.GetError())
	}

	volume, err := server.getOrCreateVolume(volumeID)
	if err != nil {
		t.Fatalf("getOrCreateVolume failed: %v", err)
	}

	expectedPosition := volume.Position()
	if expectedPosition == 0 {
		t.Fatal("expected volume position > 0 after CreateFile")
	}

	// Reset counters to isolate the Fsync barrier wait.
	stream.waitCount.Store(0)
	stream.lastWaitSequence.Store(0)

	fsyncResponse, err := server.Fsync(ctx, &pb.FsyncRequest{
		VolumeId: volumeID,
		Inode:    createResponse.GetAttr().GetInode().GetIno(),
		Blocking: true,
	})
	if err != nil || !fsyncResponse.GetSuccess() {
		t.Fatalf("Fsync failed: %v (response %v)", err, fsyncResponse)
	}

	if waits := stream.waitCount.Load(); waits != 1 {
		t.Fatalf("expected 1 durability wait from Fsync barrier, got %d", waits)
	}
	if lastWait := stream.lastWaitSequence.Load(); lastWait != expectedPosition {
		t.Fatalf("expected lastWaitSequence = %d, got %d", expectedPosition, lastWait)
	}
}

func TestSyncBarrier_EmptyFilteredSyncWaitsOnNewestCommitSequence(t *testing.T) {
	ctx := t.Context()
	stream := &recordingStream{}
	server := NewTestServer(t, NewMemoryBackend(),
		WithServerStreamFactory(func(volumeID string) (walclient.Stream, error) {
			return stream, nil
		}),
		WithServerDurability(walclient.Witness),
	)
	defer func() {
		if err := server.Close(); err != nil {
			t.Errorf("server.Close failed: %v", err)
		}
	}()

	volumeID := "test-sync-barrier-filtered"
	createResponse, err := server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:    volumeID,
		ParentInode: 1,
		Name:        "staged.bin",
		Mode:        0644,
	})
	if err != nil || createResponse.GetError() != 0 {
		t.Fatalf("CreateFile failed: %v (code %d)", err, createResponse.GetError())
	}
	stagedInodeID := createResponse.GetAttr().GetInode().GetIno()

	// Stage chunk data on stagedInodeID so len(v.stagedChunks) > 0.
	writeResponse, err := server.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId:  volumeID,
		Inode:     stagedInodeID,
		Offset:    0,
		Data:      []byte("hello world"),
		WriteMode: pb.WriteMode_LAZY,
	})
	if err != nil || writeResponse.GetError() != 0 {
		t.Fatalf("WriteFile failed: %v (code %d)", err, writeResponse.GetError())
	}

	volume, err := server.getOrCreateVolume(volumeID)
	if err != nil {
		t.Fatalf("getOrCreateVolume failed: %v", err)
	}

	expectedPosition := volume.Position()
	if expectedPosition == 0 {
		t.Fatal("expected volume position > 0")
	}

	// Reset counters to isolate the filtered sync wait.
	stream.waitCount.Store(0)
	stream.lastWaitSequence.Store(0)

	// Inode filter targets an inode with nothing staged, so len(v.stagedChunks) > 0
	// but len(snapshotChunks) == 0, exercising executeSyncLocked's second early return.
	volume.stagingMu.Lock()
	volume.waitForSyncIdleLocked()
	unfilteredInodeID := stagedInodeID + 1000
	_, err = volume.executeSyncLocked(ctx, pb.FsyncRequest_SYNC_TRIGGER_CLOSE, false, map[uint64]struct{}{unfilteredInodeID: {}})
	if err != nil {
		t.Fatalf("executeSyncLocked failed: %v", err)
	}

	if waits := stream.waitCount.Load(); waits != 1 {
		t.Fatalf("expected 1 durability wait from filtered sync barrier, got %d", waits)
	}
	if lastWait := stream.lastWaitSequence.Load(); lastWait != expectedPosition {
		t.Fatalf("expected lastWaitSequence = %d, got %d", expectedPosition, lastWait)
	}
}
