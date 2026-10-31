// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fuse

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/blob"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/controller"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/controller/controllertest"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type untarCountingBackend struct {
	controller.ObjectStorageBackend
	mu       sync.Mutex
	packKeys []string
}

func (b *untarCountingBackend) PutObject(ctx context.Context, volumeID, key string, stream blob.ByteStream) (string, error) {
	b.mu.Lock()
	if strings.HasSuffix(key, ".pack") || strings.Contains(key, "blobs/") {
		b.packKeys = append(b.packKeys, key)
	}
	b.mu.Unlock()
	return b.ObjectStorageBackend.PutObject(ctx, volumeID, key, stream)
}

func (b *untarCountingBackend) getPackCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.packKeys)
}

// TestUntarPackCounts measures and prints the exact packfile count produced by untarring
// 1,000 small files (8 KiB each) on one node with sync-on-close enabled, both with and
// without a concurrent 256 MiB streaming write.
func TestUntarPackCounts(t *testing.T) {
	runWorkload := func(concurrent256MBWrite bool) int {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		rawBackend := controller.NewMemoryBackend()
		backend := &untarCountingBackend{ObjectStorageBackend: rawBackend}
		server := controllertest.NewTestServer(backend)
		defer func() { _ = server.Close() }()

		lis := bufconn.Listen(4 * 1024 * 1024)
		grpcServer := grpc.NewServer()
		pb.RegisterObjectFSControllerServer(grpcServer, server)
		go func() { _ = grpcServer.Serve(lis) }()
		defer grpcServer.Stop()

		conn, err := grpc.NewClient("passthrough://bufnet",
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return lis.Dial()
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			t.Fatalf("Failed to dial bufnet: %v", err)
		}
		defer conn.Close()

		client := pb.NewObjectFSControllerClient(conn)
		volID := fmt.Sprintf("vol-untar-bench-%d", time.Now().UnixNano())
		cache := NewNodeCache(128 * 1024 * 1024)
		rawFS := NewObjectFS(client, volID, pb.WriteMode_WRITE_THROUGH_FSYNC, cache)
		rawFS.StartBackgroundSync(ctx)

		var bigWriteDone atomic.Bool
		if concurrent256MBWrite {
			var bigCreate gofuse.CreateOut
			if st := rawFS.Create(nil, &gofuse.CreateIn{
				InHeader: gofuse.InHeader{NodeId: gofuse.FUSE_ROOT_ID},
				Mode:     0644,
			}, "bigfile_256mb.dat", &bigCreate); st != gofuse.OK {
				t.Fatalf("Create bigfile failed: %v", st)
			}
			bigIno := bigCreate.EntryOut.NodeId
			bigFh := bigCreate.OpenOut.Fh

			go func() {
				defer bigWriteDone.Store(true)
				chunk := bytes.Repeat([]byte("B"), 64*1024)
				// 256 MiB = 4096 chunks of 64 KiB
				for i := 0; i < 4096; i++ {
					select {
					case <-ctx.Done():
						return
					default:
						_, _ = rawFS.Write(nil, &gofuse.WriteIn{
							InHeader: gofuse.InHeader{NodeId: bigIno},
							Fh:       bigFh,
							Offset:   uint64(i * 64 * 1024),
						}, chunk)
						time.Sleep(50 * time.Microsecond)
					}
				}
			}()
		}

		// Untar 1,000 small files of 8 KiB
		const numFiles = 1000
		fileData := bytes.Repeat([]byte("A"), 8192)
		for i := 0; i < numFiles; i++ {
			var createOut gofuse.CreateOut
			fname := fmt.Sprintf("file_%04d.txt", i)
			if st := rawFS.Create(nil, &gofuse.CreateIn{
				InHeader: gofuse.InHeader{NodeId: gofuse.FUSE_ROOT_ID},
				Mode:     0644,
			}, fname, &createOut); st != gofuse.OK {
				t.Fatalf("Create %s failed: %v", fname, st)
			}
			fIno := createOut.EntryOut.NodeId
			fFh := createOut.OpenOut.Fh

			if _, st := rawFS.Write(nil, &gofuse.WriteIn{
				InHeader: gofuse.InHeader{NodeId: fIno},
				Fh:       fFh,
				Offset:   0,
			}, fileData); st != gofuse.OK {
				t.Fatalf("Write %s failed: %v", fname, st)
			}

			rawFS.Release(nil, &gofuse.ReleaseIn{
				InHeader: gofuse.InHeader{NodeId: fIno},
				Fh:       fFh,
			})
		}

		// Wait for all closed inodes to be drained and flushed
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			rawFS.closedInodesMu.Lock()
			closedLen := len(rawFS.closedInodes)
			rawFS.closedInodesMu.Unlock()
			dirtyEntries := len(rawFS.cache.GetDirtyEntries())
			if concurrent256MBWrite {
				// Big write may remain dirty in cache, but all 1000 small files should be clean
				if closedLen == 0 && dirtyEntries <= 1 {
					break
				}
			} else {
				if closedLen == 0 && dirtyEntries == 0 {
					break
				}
			}
			time.Sleep(20 * time.Millisecond)
		}

		// Allow final in-flight floor window to finish committing
		time.Sleep(150 * time.Millisecond)

		return backend.getPackCount()
	}

	pWithout := runWorkload(false)
	t.Logf("Untar 1,000 files (8 KiB) without concurrent write: %d packs", pWithout)

	pWith := runWorkload(true)
	t.Logf("Untar 1,000 files (8 KiB) with concurrent 256 MiB write: %d packs", pWith)
}
