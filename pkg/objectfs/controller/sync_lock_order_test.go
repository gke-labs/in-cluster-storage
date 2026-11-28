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
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
)

// TestSyncAndCloseFsyncLockOrder exercises the interleaving that triggered the
// controller lock-order deadlock (issue #890):
//
//  1. A blocking fsync after a 4096-byte (inline-sized) write enters
//     executeSyncLocked, takes v.mu.Lock(), and in appendSyncRecordsLocked's
//     inline-data branch calls readStagedChunkBytes, which acquires v.stagingMu.
//  2. A close-triggered non-blocking fsync on an inode with no staged chunks
//     enters FsyncWithEpoch, takes v.stagingMu.Lock(), and calls v.Position()
//     while holding v.stagingMu. Previously Position() took v.mu.RLock(),
//     inverting the lock order against (1).
//  3. A third goroutine calls Lookup on an unrelated volume to verify the
//     controller stays responsive across volumes (a deadlock on the primary
//     volume stalls getOrCreateVolume -> IsDeleted under server.mu).
func TestSyncAndCloseFsyncLockOrder(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	server := NewTestServer(t, NewMemoryBackend())

	const (
		primaryVolumeID   = "lock-order-primary"
		unrelatedVolumeID = "lock-order-unrelated"
		inlineFileSize    = 4096
		rounds            = 200
		callTimeout       = 5 * time.Second
	)

	createFile := func(volumeID, name string) uint64 {
		t.Helper()
		response, err := server.CreateFile(ctx, &pb.CreateFileRequest{
			VolumeId:    volumeID,
			ParentInode: 1,
			Name:        name,
			Mode:        0644,
		})
		if err != nil || response.GetError() != 0 {
			t.Fatalf("CreateFile(%s/%s) failed: err=%v code=%d", volumeID, name, err, response.GetError())
		}
		return response.GetAttr().GetInode().GetIno()
	}

	inlineInode := createFile(primaryVolumeID, "inline.bin")
	cleanInode := createFile(primaryVolumeID, "clean.bin")
	createFile(unrelatedVolumeID, "other.bin")

	payload := make([]byte, inlineFileSize)
	for i := range payload {
		payload[i] = byte(i % 251)
	}

	workerNames := [3]string{"blocking-inline-fsync", "close-nonblocking-fsync", "unrelated-volume-lookup"}
	var lastProgress [3]atomic.Int64
	startNanoseconds := time.Now().UnixNano()
	for i := range lastProgress {
		lastProgress[i].Store(startNanoseconds)
	}

	stopWorkers := make(chan struct{})
	workerErrors := make(chan error, 3)
	var wg sync.WaitGroup
	wg.Add(3)

	// Worker 0: repeatedly writes 4096 bytes and runs a blocking fsync (v.mu -> v.stagingMu).
	go func() {
		defer wg.Done()
		defer close(stopWorkers)
		for i := 0; i < rounds && ctx.Err() == nil; i++ {
			payload[0] = byte(i)
			writeResponse, err := server.WriteFile(ctx, &pb.WriteFileRequest{
				VolumeId:  primaryVolumeID,
				Inode:     inlineInode,
				Offset:    0,
				Data:      payload,
				WriteMode: pb.WriteMode_LAZY,
			})
			if err != nil || writeResponse.GetError() != 0 {
				workerErrors <- fmt.Errorf("WriteFile round %d failed: err=%v code=%d", i, err, writeResponse.GetError())
				return
			}
			fsyncResponse, err := server.Fsync(ctx, &pb.FsyncRequest{
				VolumeId: primaryVolumeID,
				Inode:    inlineInode,
				Blocking: true,
			})
			if err != nil || fsyncResponse.GetError() != 0 {
				workerErrors <- fmt.Errorf("blocking Fsync round %d failed: err=%v code=%d", i, err, fsyncResponse.GetError())
				return
			}
			lastProgress[0].Store(time.Now().UnixNano())
		}
	}()

	// Worker 1: repeatedly runs close-triggered non-blocking fsync on an inode with no staged chunks.
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopWorkers:
				return
			case <-ctx.Done():
				return
			default:
			}
			fsyncResponse, err := server.Fsync(ctx, &pb.FsyncRequest{
				VolumeId: primaryVolumeID,
				Inode:    cleanInode,
				Blocking: false,
				Trigger:  pb.FsyncRequest_SYNC_TRIGGER_CLOSE,
			})
			if err != nil || fsyncResponse.GetError() != 0 {
				if ctx.Err() == nil {
					workerErrors <- fmt.Errorf("close Fsync failed: err=%v code=%d", err, fsyncResponse.GetError())
				}
				return
			}
			lastProgress[1].Store(time.Now().UnixNano())
		}
	}()

	// Worker 2: repeatedly calls Lookup on an unrelated volume.
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopWorkers:
				return
			case <-ctx.Done():
				return
			default:
			}
			lookupResponse, err := server.Lookup(ctx, &pb.LookupRequest{
				VolumeId:    unrelatedVolumeID,
				ParentInode: 1,
				Name:        "other.bin",
			})
			if err != nil || lookupResponse.GetError() != 0 {
				if ctx.Err() == nil {
					workerErrors <- fmt.Errorf("unrelated Lookup failed: err=%v code=%d", err, lookupResponse.GetError())
				}
				return
			}
			lastProgress[2].Store(time.Now().UnixNano())
		}
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			select {
			case err := <-workerErrors:
				t.Fatal(err)
			default:
			}
			return
		case err := <-workerErrors:
			cancel()
			t.Fatal(err)
		case <-ticker.C:
			now := time.Now()
			for i := range lastProgress {
				elapsed := now.Sub(time.Unix(0, lastProgress[i].Load()))
				if elapsed > callTimeout {
					stackBuffer := make([]byte, 1<<20)
					stackLength := runtime.Stack(stackBuffer, true)
					cancel()
					t.Fatalf("deadlock detected: worker %q made no progress for %v (limit %v):\n%s",
						workerNames[i], elapsed, callTimeout, string(stackBuffer[:stackLength]))
				}
			}
		}
	}
}
