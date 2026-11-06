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
	"fmt"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/wal/buffer/buffertest"
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
)

func runBenchmarkCreate1000Files(b *testing.B, mode pb.MetadataDurability) {
	ctx := b.Context()
	backend := NewMemoryBackend()
	bt, cleanup := buffertest.Start(ctx, b, buffertest.WithBackend(backend))
	defer cleanup()

	walDir := b.TempDir()
	server := NewTestServer(b, backend,
		WithServerWAL(walDir, bt.Target, walclient.Witness),
		WithServerWALDialOptions(bt.DialOpts...),
		WithServerMetadataDurability(mode),
	)
	defer func() { _ = server.Close() }()

	volumeID := fmt.Sprintf("bench-1000-files-%v", mode)
	b.ResetTimer()

	start := time.Now()
	for i := 0; i < 1000; i++ {
		resp, err := server.CreateFile(ctx, &pb.CreateFileRequest{
			VolumeId:    volumeID,
			ParentInode: 1,
			Name:        fmt.Sprintf("file_%d.txt", i),
			Mode:        0644,
		})
		if err != nil || resp.GetError() != 0 {
			b.Fatalf("CreateFile %d failed: err=%v, resp=%v", i, err, resp)
		}
	}
	elapsed := time.Since(start)
	b.Logf("[%v] Created 1000 files in %v (%v/op)", mode, elapsed, elapsed/1000)
}

func BenchmarkCreate1000Files_Barrier(b *testing.B) {
	runBenchmarkCreate1000Files(b, pb.MetadataDurability_BARRIER)
}

func BenchmarkCreate1000Files_Operation(b *testing.B) {
	runBenchmarkCreate1000Files(b, pb.MetadataDurability_OPERATION)
}
