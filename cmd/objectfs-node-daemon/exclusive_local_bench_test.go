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

package main

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/controller/controllertest"
	objectfuse "github.com/gke-labs/in-cluster-storage/pkg/objectfs/fuse"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func setupTestControllerClient(t testing.TB) (pb.ObjectFSControllerClient, func()) {
	t.Helper()
	server := controllertest.NewTestServer(nil)
	lis := bufconn.Listen(4 * 1024 * 1024)
	grpcServer := grpc.NewServer()
	pb.RegisterObjectFSControllerServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(lis) }()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	client := pb.NewObjectFSControllerClient(conn)

	cleanup := func() {
		conn.Close()
		grpcServer.Stop()
	}
	return client, cleanup
}

func BenchmarkExclusiveLocalVsFUSE(b *testing.B) {
	client, cleanup := setupTestControllerClient(b)
	defer cleanup()

	ctx := b.Context()

	// -------------------------------------------------------------
	// Benchmark 1: Write Throughput (16 MiB sequential writes)
	// -------------------------------------------------------------
	dataSize := 16 * 1024 * 1024 // 16 MiB
	chunkSize := 64 * 1024       // 64 KiB
	chunk := bytes.Repeat([]byte("X"), chunkSize)

	b.Run("WriteThroughput_FUSE", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			volID := fmt.Sprintf("bench-wt-fuse-%d-%d", time.Now().UnixNano(), i)
			nodeCache := objectfuse.NewNodeCache(32 * 1024 * 1024)
			rawFS := objectfuse.NewObjectFSWithTimeouts(client, volID, pb.WriteMode_WRITE_THROUGH_FSYNC, nodeCache, 30*time.Second, 10*time.Minute)

			filename := fmt.Sprintf("large_%d.bin", i)
			var createOut gofuse.CreateOut
			errno := rawFS.Create(nil, &gofuse.CreateIn{
				InHeader: gofuse.InHeader{NodeId: 1},
				Mode:     0644,
			}, filename, &createOut)
			if errno != 0 {
				b.Fatalf("FUSE Create failed: %v", errno)
			}
			ino := createOut.NodeId

			for offset := int64(0); offset < int64(dataSize); offset += int64(chunkSize) {
				_, errno := rawFS.Write(nil, &gofuse.WriteIn{
					InHeader: gofuse.InHeader{NodeId: ino},
					Offset:   uint64(offset),
				}, chunk)
				if errno != 0 {
					b.Fatalf("FUSE Write failed: %v", errno)
				}
			}

			if errno := rawFS.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: ino}}); errno != 0 {
				b.Fatalf("FUSE Fsync failed: %v", errno)
			}
		}
		b.SetBytes(int64(dataSize))
	})

	b.Run("WriteThroughput_ExclusiveLocal_AppWriteOnly", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tmpDir := b.TempDir()
			upperDir := filepath.Join(tmpDir, "upper")
			_ = os.MkdirAll(upperDir, 0755)

			filename := filepath.Join(upperDir, fmt.Sprintf("large_%d.bin", i))
			f, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				b.Fatalf("OpenFile failed: %v", err)
			}
			for offset := int64(0); offset < int64(dataSize); offset += int64(chunkSize) {
				if _, err := f.Write(chunk); err != nil {
					b.Fatalf("Local write failed: %v", err)
				}
			}
			_ = f.Close()
		}
		b.SetBytes(int64(dataSize))
	})

	b.Run("WriteThroughput_ExclusiveLocal_TotalWithCheckpoint", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			volID := fmt.Sprintf("bench-wt-excl-%d-%d", time.Now().UnixNano(), i)
			tmpDir := b.TempDir()
			lowerDir := filepath.Join(tmpDir, "lower")
			upperDir := filepath.Join(tmpDir, "upper")
			_ = os.MkdirAll(lowerDir, 0755)
			_ = os.MkdirAll(upperDir, 0755)

			filename := filepath.Join(upperDir, fmt.Sprintf("large_%d.bin", i))
			f, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				b.Fatalf("OpenFile failed: %v", err)
			}
			for offset := int64(0); offset < int64(dataSize); offset += int64(chunkSize) {
				if _, err := f.Write(chunk); err != nil {
					b.Fatalf("Local write failed: %v", err)
				}
			}
			_ = f.Close()

			// Checkpoint at unmount
			ops, err := ScanUpperLayer(lowerDir, upperDir)
			if err != nil {
				b.Fatalf("ScanUpperLayer: %v", err)
			}
			if _, err := ReplayOperations(ctx, client, volID, ops); err != nil {
				b.Fatalf("ReplayOperations: %v", err)
			}
		}
		b.SetBytes(int64(dataSize))
	})

	// -------------------------------------------------------------
	// Benchmark 2: Untar-style workload (many small files)
	// -------------------------------------------------------------
	numFiles := 250
	smallPayload := []byte("package main\nfunc main() {}\n")

	b.Run("Untar_ManySmallFiles_FUSE", func(b *testing.B) {
		b.ResetTimer()
		for it := 0; it < b.N; it++ {
			volID := fmt.Sprintf("bench-untar-fuse-%d-%d", time.Now().UnixNano(), it)
			nodeCache := objectfuse.NewNodeCache(32 * 1024 * 1024)
			rawFS := objectfuse.NewObjectFSWithTimeouts(client, volID, pb.WriteMode_WRITE_THROUGH_FSYNC, nodeCache, 30*time.Second, 10*time.Minute)

			baseDirName := fmt.Sprintf("pkg_%d_%d", time.Now().UnixNano(), it)
			var mkdirOut gofuse.EntryOut
			if errno := rawFS.Mkdir(nil, &gofuse.MkdirIn{InHeader: gofuse.InHeader{NodeId: 1}, Mode: 0755}, baseDirName, &mkdirOut); errno != 0 {
				b.Fatalf("FUSE Mkdir failed: %v", errno)
			}
			dirIno := mkdirOut.NodeId

			for f := 0; f < numFiles; f++ {
				var createOut gofuse.CreateOut
				fname := fmt.Sprintf("file_%d.go", f)
				if errno := rawFS.Create(nil, &gofuse.CreateIn{InHeader: gofuse.InHeader{NodeId: dirIno}, Mode: 0644}, fname, &createOut); errno != 0 {
					b.Fatalf("FUSE Create failed: %v", errno)
				}
				fIno := createOut.NodeId
				if _, errno := rawFS.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: fIno}, Offset: 0}, smallPayload); errno != 0 {
					b.Fatalf("FUSE Write failed: %v", errno)
				}
				if errno := rawFS.Fsync(nil, &gofuse.FsyncIn{InHeader: gofuse.InHeader{NodeId: fIno}}); errno != 0 {
					b.Fatalf("FUSE Fsync failed: %v", errno)
				}
			}
		}
	})

	b.Run("Untar_ManySmallFiles_ExclusiveLocal_AppWriteOnly", func(b *testing.B) {
		b.ResetTimer()
		for it := 0; it < b.N; it++ {
			tmpDir := b.TempDir()
			upperDir := filepath.Join(tmpDir, "upper")
			_ = os.MkdirAll(upperDir, 0755)

			baseDirPath := filepath.Join(upperDir, fmt.Sprintf("pkg_%d_%d", time.Now().UnixNano(), it))
			if err := os.MkdirAll(baseDirPath, 0755); err != nil {
				b.Fatalf("MkdirAll failed: %v", err)
			}

			for f := 0; f < numFiles; f++ {
				fname := filepath.Join(baseDirPath, fmt.Sprintf("file_%d.go", f))
				if err := os.WriteFile(fname, smallPayload, 0644); err != nil {
					b.Fatalf("WriteFile failed: %v", err)
				}
			}
		}
	})

	b.Run("Untar_ManySmallFiles_ExclusiveLocal_TotalWithCheckpoint", func(b *testing.B) {
		b.ResetTimer()
		for it := 0; it < b.N; it++ {
			volID := fmt.Sprintf("bench-untar-excl-%d-%d", time.Now().UnixNano(), it)
			tmpDir := b.TempDir()
			lowerDir := filepath.Join(tmpDir, "lower")
			upperDir := filepath.Join(tmpDir, "upper")
			_ = os.MkdirAll(lowerDir, 0755)
			_ = os.MkdirAll(upperDir, 0755)

			baseDirPath := filepath.Join(upperDir, fmt.Sprintf("pkg_%d_%d", time.Now().UnixNano(), it))
			if err := os.MkdirAll(baseDirPath, 0755); err != nil {
				b.Fatalf("MkdirAll failed: %v", err)
			}

			for f := 0; f < numFiles; f++ {
				fname := filepath.Join(baseDirPath, fmt.Sprintf("file_%d.go", f))
				if err := os.WriteFile(fname, smallPayload, 0644); err != nil {
					b.Fatalf("WriteFile failed: %v", err)
				}
			}

			// Checkpoint at unmount
			ops, err := ScanUpperLayer(lowerDir, upperDir)
			if err != nil {
				b.Fatalf("ScanUpperLayer: %v", err)
			}
			if _, err := ReplayOperations(ctx, client, volID, ops); err != nil {
				b.Fatalf("ReplayOperations: %v", err)
			}
		}
	})
}
