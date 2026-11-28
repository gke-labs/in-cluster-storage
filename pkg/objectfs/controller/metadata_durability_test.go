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
	"fmt"
	"testing"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/wal/buffer/buffertest"
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
	"google.golang.org/protobuf/proto"
)

// newMetadataDurabilityTestServer creates a test server and volume backed by recordingStream.
// Registers server cleanup on t. Takes no locks.
func newMetadataDurabilityTestServer(t *testing.T, stream *recordingStream, mode pb.MetadataDurability) (*Server, *Volume) {
	t.Helper()
	var serverOptions []ServerOption
	serverOptions = append(serverOptions,
		WithServerStreamFactory(func(string) (walclient.Stream, error) {
			return stream, nil
		}),
		WithServerDurability(walclient.Witness),
	)
	if mode != pb.MetadataDurability_METADATA_DURABILITY_UNSPECIFIED {
		serverOptions = append(serverOptions, WithServerMetadataDurability(mode))
	}
	server := NewTestServer(t, NewMemoryBackend(), serverOptions...)
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Errorf("server.Close failed: %v", err)
		}
	})
	volume, err := server.getOrCreateVolume("test-vol")
	if err != nil {
		t.Fatalf("getOrCreateVolume failed: %v", err)
	}
	return server, volume
}

// runMetadataOperations executes each of the 14 metadata operations listed in the commit body,
// asserting the expected durability wait count after each operation. Takes no locks.
func runMetadataOperations(t *testing.T, ctx context.Context, server *Server, volume *Volume, stream *recordingStream, wantWaits int) {
	t.Helper()
	assertWaits := func(operationName string, expected int) {
		t.Helper()
		if got := stream.waitCount.Swap(0); got != int64(expected) {
			t.Fatalf("%s: expected %d durability waits, got %d", operationName, expected, got)
		}
	}

	// 1. CreateFile (new file)
	createResponse, err := server.CreateFile(ctx, &pb.CreateFileRequest{VolumeId: volume.volumeID, ParentInode: 1, Name: "file.txt", Mode: 0644})
	if err != nil || createResponse.GetError() != 0 {
		t.Fatalf("CreateFile: %v", err)
	}
	fileIno := createResponse.GetAttr().GetInode().GetIno()
	assertWaits("CreateFile (new)", wantWaits)

	// 2. CreateFile (overwrite existing file)
	if response, err := server.CreateFile(ctx, &pb.CreateFileRequest{VolumeId: volume.volumeID, ParentInode: 1, Name: "file.txt", Mode: 0644, Truncate: true}); err != nil || response.GetError() != 0 {
		t.Fatalf("CreateFile overwrite: %v", err)
	}
	assertWaits("CreateFile (overwrite)", wantWaits)

	// 3. Mkdir
	if response, err := server.Mkdir(ctx, &pb.MkdirRequest{VolumeId: volume.volumeID, ParentInode: 1, Name: "sub", Mode: 0755}); err != nil || response.GetError() != 0 {
		t.Fatalf("Mkdir: %v", err)
	}
	assertWaits("Mkdir", wantWaits)

	// 4. SetAttr
	if response, err := server.SetAttr(ctx, &pb.SetAttrRequest{VolumeId: volume.volumeID, Inode: fileIno, Mode: proto.Uint32(0600)}); err != nil || response.GetError() != 0 {
		t.Fatalf("SetAttr: %v", err)
	}
	assertWaits("SetAttr", wantWaits)

	// 5. SetXAttr
	if response, err := server.SetXAttr(ctx, &pb.SetXAttrRequest{VolumeId: volume.volumeID, Inode: fileIno, Name: "user.tag", Value: []byte("val")}); err != nil || response.GetError() != 0 {
		t.Fatalf("SetXAttr: %v", err)
	}
	assertWaits("SetXAttr", wantWaits)

	// 6. RemoveXAttr
	if response, err := server.RemoveXAttr(ctx, &pb.RemoveXAttrRequest{VolumeId: volume.volumeID, Inode: fileIno, Name: "user.tag"}); err != nil || response.GetError() != 0 {
		t.Fatalf("RemoveXAttr: %v", err)
	}
	assertWaits("RemoveXAttr", wantWaits)

	// 7. Symlink
	if response, err := server.Symlink(ctx, &pb.SymlinkRequest{VolumeId: volume.volumeID, ParentInode: 1, Name: "sym", Target: "file.txt"}); err != nil || response.GetError() != 0 {
		t.Fatalf("Symlink: %v", err)
	}
	assertWaits("Symlink", wantWaits)

	// 8. Link
	if response, err := server.Link(ctx, &pb.LinkRequest{VolumeId: volume.volumeID, OldInode: fileIno, NewParentInode: 1, NewName: "link"}); err != nil || response.GetError() != 0 {
		t.Fatalf("Link: %v", err)
	}
	assertWaits("Link", wantWaits)

	// 9. TruncateFile
	if response, err := server.TruncateFile(ctx, &pb.TruncateFileRequest{VolumeId: volume.volumeID, Inode: fileIno, Size: 10}); err != nil || response.GetError() != 0 {
		t.Fatalf("TruncateFile: %v", err)
	}
	assertWaits("TruncateFile", wantWaits)

	// 10. PunchHole (pre-syncs staged state via Fsync barrier, which adds 1 wait)
	if response, err := server.PunchHole(ctx, &pb.PunchHoleRequest{VolumeId: volume.volumeID, Inode: fileIno, Offset: 0, Length: 5}); err != nil || response.GetError() != 0 {
		t.Fatalf("PunchHole: %v", err)
	}
	assertWaits("PunchHole", wantWaits+1)

	// 11. Rename
	if response, err := server.Rename(ctx, &pb.RenameRequest{VolumeId: volume.volumeID, OldParentInode: 1, OldName: "link", NewParentInode: 1, NewName: "link2"}); err != nil || response.GetError() != 0 {
		t.Fatalf("Rename: %v", err)
	}
	assertWaits("Rename", wantWaits)

	// 12. Unlink
	if response, err := server.Unlink(ctx, &pb.UnlinkRequest{VolumeId: volume.volumeID, ParentInode: 1, Name: "link2"}); err != nil || response.GetError() != 0 {
		t.Fatalf("Unlink: %v", err)
	}
	assertWaits("Unlink", wantWaits)

	// 13. Rmdir
	if response, err := server.Rmdir(ctx, &pb.RmdirRequest{VolumeId: volume.volumeID, ParentInode: 1, Name: "sub"}); err != nil || response.GetError() != 0 {
		t.Fatalf("Rmdir: %v", err)
	}
	assertWaits("Rmdir", wantWaits)

	// 14. Release (orphan deletion)
	orphanResponse, err := server.CreateFile(ctx, &pb.CreateFileRequest{VolumeId: volume.volumeID, ParentInode: 1, Name: "orphan.txt", Mode: 0644})
	if err != nil || orphanResponse.GetError() != 0 {
		t.Fatalf("CreateFile orphan: %v", err)
	}
	if _, err := server.Unlink(ctx, &pb.UnlinkRequest{VolumeId: volume.volumeID, ParentInode: 1, Name: "orphan.txt"}); err != nil {
		t.Fatalf("Unlink orphan: %v", err)
	}
	stream.waitCount.Store(0)
	if err := volume.Release(ctx, orphanResponse.GetAttr().GetInode().GetIno(), orphanResponse.GetFh()); err != nil {
		t.Fatalf("Release: %v", err)
	}
	assertWaits("Release", wantWaits)
}

// TestMetadataDurability_Modes verifies that for each durability mode (barrier, per-operation,
// unset), each of the 14 metadata operations makes the expected number of durability wait calls
// (0 in barrier mode, 1 in per-operation and unset modes), and that a subsequent fsync barrier
// in barrier mode collects durability by waiting on the newest commit sequence.
func TestMetadataDurability_Modes(t *testing.T) {
	testCases := []struct {
		name      string
		mode      pb.MetadataDurability
		wantWaits int
	}{
		{name: "barrier", mode: pb.MetadataDurability_BARRIER, wantWaits: 0},
		{name: "operation", mode: pb.MetadataDurability_OPERATION, wantWaits: 1},
		{name: "unset", mode: pb.MetadataDurability_METADATA_DURABILITY_UNSPECIFIED, wantWaits: 1},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := t.Context()
			stream := &recordingStream{}
			server, volume := newMetadataDurabilityTestServer(t, stream, testCase.mode)

			// Reset counters after setup so assertions are not already true.
			stream.waitCount.Store(0)
			stream.lastWaitSequence.Store(0)

			runMetadataOperations(t, ctx, server, volume, stream, testCase.wantWaits)

			if testCase.wantWaits == 0 {
				// In barrier mode, verify that Fsync barrier collects durability with exactly one wait.
				fsyncResponse, err := server.Fsync(ctx, &pb.FsyncRequest{
					VolumeId: volume.volumeID,
					Inode:    1,
					Blocking: true,
				})
				if err != nil || !fsyncResponse.GetSuccess() {
					t.Fatalf("Fsync failed: %v", err)
				}
				if got := stream.waitCount.Swap(0); got != 1 {
					t.Fatalf("Fsync barrier: expected 1 durability wait, got %d", got)
				}
				if lastWait := stream.lastWaitSequence.Load(); lastWait < volume.Position() {
					t.Fatalf("expected lastWaitSequence >= volume position %d, got %d", volume.Position(), lastWait)
				}
			}
		})
	}
}

// TestMetadataDurability_BarrierMode_StopCollectsDurability verifies that in barrier mode,
// running operations and then Volume.Stop asserts a single wait at or above the volume's position.
func TestMetadataDurability_BarrierMode_StopCollectsDurability(t *testing.T) {
	ctx := t.Context()
	stream := &recordingStream{}
	server, volume := newMetadataDurabilityTestServer(t, stream, pb.MetadataDurability_BARRIER)

	// Reset counters after setup.
	stream.waitCount.Store(0)
	stream.lastWaitSequence.Store(0)

	runMetadataOperations(t, ctx, server, volume, stream, 0)

	expectedPosition := volume.Position()
	if expectedPosition == 0 {
		t.Fatal("expected non-zero volume position after metadata operations")
	}

	// Reset stream counters before Stop.
	stream.waitCount.Store(0)
	stream.lastWaitSequence.Store(0)

	if err := volume.Stop(ctx); err != nil {
		t.Fatalf("Volume.Stop failed: %v", err)
	}

	if got := stream.waitCount.Load(); got != 1 {
		t.Fatalf("expected 1 durability wait from Volume.Stop, got %d", got)
	}
	if lastWait := stream.lastWaitSequence.Load(); lastWait < expectedPosition {
		t.Fatalf("expected lastWaitSequence >= %d, got %d", expectedPosition, lastWait)
	}
}

// runBenchmarkCreateFile measures CreateFile performance across b.N iterations under
// the specified metadata durability mode. Takes no locks.
func runBenchmarkCreateFile(b *testing.B, mode pb.MetadataDurability) {
	ctx := b.Context()
	backend := NewMemoryBackend()
	bufferServer, bufferCleanup := buffertest.Start(ctx, b, buffertest.WithBackend(backend))
	b.Cleanup(bufferCleanup)

	walDirectory := b.TempDir()
	server := NewTestServer(b, backend,
		WithServerWAL(walDirectory, bufferServer.Target, walclient.Witness),
		WithServerWALDialOptions(bufferServer.DialOpts...),
		WithServerMetadataDurability(mode),
	)
	b.Cleanup(func() {
		if err := server.Close(); err != nil {
			b.Errorf("server.Close failed: %v", err)
		}
	})

	volumeID := fmt.Sprintf("bench-files-%v", mode)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		response, err := server.CreateFile(ctx, &pb.CreateFileRequest{
			VolumeId:    volumeID,
			ParentInode: 1,
			Name:        fmt.Sprintf("file_%d.txt", i),
			Mode:        0644,
		})
		if err != nil || response.GetError() != 0 {
			b.Fatalf("CreateFile %d failed: err=%v, response=%v", i, err, response)
		}
	}
}

// BenchmarkCreateFile_Barrier measures CreateFile in barrier metadata durability mode. Takes no locks.
func BenchmarkCreateFile_Barrier(b *testing.B) {
	runBenchmarkCreateFile(b, pb.MetadataDurability_BARRIER)
}

// BenchmarkCreateFile_Operation measures CreateFile in per-operation metadata durability mode. Takes no locks.
func BenchmarkCreateFile_Operation(b *testing.B) {
	runBenchmarkCreateFile(b, pb.MetadataDurability_OPERATION)
}
