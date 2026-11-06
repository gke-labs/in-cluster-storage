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
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/wal/buffer/buffertest"
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
)

// NewTestServer constructs a Server configured for testing with an in-process WAL buffer over bufconn,
// skipping per-record WAL fsync.
// The production controller binary uses NewServer, which always retains per-record fsync.
func NewTestServer(t testing.TB, backend ObjectStorageBackend, opts ...ServerOption) *Server {
	var server *Server
	if target := InspectServerTarget(opts...); target != "" {
		metadataDir := t.TempDir()
		opts = append([]ServerOption{
			WithServerWithoutFsync(),
			WithServerMetadataDir(metadataDir),
		}, opts...)
		server = NewServer(backend, opts...)
	} else {
		ctx := t.Context()
		bt, cleanup := buffertest.Start(ctx, t, buffertest.WithBackend(backend))
		walDir, err := os.MkdirTemp("", "controllertest-wal-*")
		if err != nil {
			panic(err)
		}
		t.Cleanup(func() {
			if err := os.RemoveAll(walDir); err != nil {
				t.Errorf("failed to remove test walDir %s: %v", walDir, err)
			}
		})

		metadataDir := t.TempDir()

		testOpts := []ServerOption{
			WithServerWAL(walDir, bt.Target, walclient.Witness),
			WithServerWALClient(bt.Client),
			WithServerWALDialOptions(bt.DialOpts...),
			WithServerWithoutFsync(),
			WithServerMetadataDir(metadataDir),
			WithServerTestCleanup(cleanup),
		}
		testOpts = append(testOpts, opts...)
		server = NewServer(backend, testOpts...)
	}

	t.Cleanup(func() {
		shutdownTestServer(t, server)
	})
	return server
}

func shutdownTestServer(t testing.TB, server *Server) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		t.Errorf("server.Shutdown failed: %v", err)
	}
}

func TestNewTestServer_CleanupWaitsForBufferLoops(t *testing.T) {
	t.Run("subtest_without_explicit_close", func(t *testing.T) {
		backend := NewMemoryBackend()
		_ = NewTestServer(t, backend)
	})

	// After subtest completion, its t.Cleanup must have stopped the WAL buffer.
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	stacks := string(buf[:n])

	if strings.Contains(stacks, "periodicFlushLoop") {
		t.Fatalf("periodicFlushLoop still running after test return:\n%s", stacks)
	}
	if strings.Contains(stacks, "groupCommitLoop") {
		t.Fatalf("groupCommitLoop still running after test return:\n%s", stacks)
	}
}

func TestNewTestServer_CleanupClosesWALClientStreams(t *testing.T) {
	stackBuffer := make([]byte, 1<<20)
	stackLength := runtime.Stack(stackBuffer, true)
	before := strings.Count(string(stackBuffer[:stackLength]), "backgroundReplicationLoop")

	t.Run("subtest_without_explicit_close", func(t *testing.T) {
		ctx := t.Context()
		backend := NewMemoryBackend()
		server := NewTestServer(t, backend)
		response, err := server.CreateFile(ctx, &pb.CreateFileRequest{
			VolumeId:    "test-leak-vol",
			ParentInode: 1,
			Name:        "leak.txt",
			Mode:        0644,
		})
		if err != nil || response.GetError() != 0 {
			t.Fatalf("CreateFile failed: %v (code %d)", err, response.GetError())
		}
	})

	stackLength = runtime.Stack(stackBuffer, true)
	stacks := string(stackBuffer[:stackLength])

	if before == 0 {
		if strings.Contains(stacks, "backgroundReplicationLoop") {
			t.Fatalf("backgroundReplicationLoop still running after test return:\n%s", stacks)
		}
	} else if strings.Count(stacks, "backgroundReplicationLoop") > before {
		t.Fatalf("backgroundReplicationLoop still running after test return:\n%s", stacks)
	}
}

func (v *Volume) evictAllUploadedPacksForTest() {
	v.stagingMu.Lock()
	defer v.stagingMu.Unlock()
	originalMaxStagingBytes := v.maxStagingBytes
	v.maxStagingBytes = 0
	v.evictUploadedPacksLocked(0)
	v.maxStagingBytes = originalMaxStagingBytes
}
