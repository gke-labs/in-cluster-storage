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
	"fmt"
	"io"
	"sync/atomic"
	"testing"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/grpcutil"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore"
)

type unavailableGetBackend struct {
	ObjectStorageBackend
	unavailable atomic.Bool
}

func (b *unavailableGetBackend) GetObject(ctx context.Context, volumeID, key string, offset, length int64, writer io.Writer) error {
	if b.unavailable.Load() {
		return fmt.Errorf("simulated object store outage: %w", objectstore.ErrUnavailable)
	}
	return b.ObjectStorageBackend.GetObject(ctx, volumeID, key, offset, length, writer)
}

func TestReadFile_ResendsWhenObjectStoreUnavailable(t *testing.T) {
	ctx := t.Context()
	backend := &unavailableGetBackend{ObjectStorageBackend: NewMemoryBackend()}
	server := NewTestServer(t, backend)
	t.Cleanup(func() { backend.unavailable.Store(false) })

	volumeID := "test-read-resend"
	created, err := server.CreateFile(ctx, &pb.CreateFileRequest{VolumeId: volumeID, ParentInode: 1, Name: "cold.bin", Mode: 0644})
	if err != nil || created.GetError() != 0 {
		t.Fatalf("CreateFile failed: err=%v errno=%d", err, created.GetError())
	}
	inodeID := created.GetAttr().GetInode().GetIno()

	payload := bytes.Repeat([]byte("cold-chunk-data-"), 4096)
	written, err := server.WriteFile(ctx, &pb.WriteFileRequest{VolumeId: volumeID, Inode: inodeID, Offset: 0, Data: payload})
	if err != nil || written.GetError() != 0 {
		t.Fatalf("WriteFile failed: err=%v errno=%d", err, written.GetError())
	}
	synced, err := server.Fsync(ctx, &pb.FsyncRequest{VolumeId: volumeID, Inode: inodeID, Blocking: true})
	if err != nil || synced.GetError() != 0 {
		t.Fatalf("Fsync failed: err=%v errno=%d", err, synced.GetError())
	}

	volume, err := server.getOrCreateVolume(volumeID)
	if err != nil {
		t.Fatalf("getOrCreateVolume failed: %v", err)
	}
	volume.evictAllUploadedPacksForTest()

	backend.unavailable.Store(true)
	response, err := server.ReadFile(ctx, &pb.ReadFileRequest{VolumeId: volumeID, Inode: inodeID, Offset: 0, Size: int64(len(payload))})
	if err == nil || response != nil {
		t.Fatalf("expected resend gRPC error with nil response, got response=%v err=%v", response, err)
	}
	if _, resend := grpcutil.ResendDelay(err); !resend {
		t.Fatalf("expected ResendDelay to report resend for %v", err)
	}

	backend.unavailable.Store(false)
	response, err = server.ReadFile(ctx, &pb.ReadFileRequest{VolumeId: volumeID, Inode: inodeID, Offset: 0, Size: int64(len(payload))})
	if err != nil || response.GetError() != 0 {
		t.Fatalf("ReadFile after recovery failed: err=%v errno=%d", err, response.GetError())
	}
	if !bytes.Equal(response.GetData(), payload) {
		t.Fatalf("ReadFile data mismatch: got %d bytes, want %d bytes", len(response.GetData()), len(payload))
	}
}
