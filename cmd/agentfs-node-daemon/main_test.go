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
	"context"
	"sync"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/v1alpha1"
)

func TestLazyLoaderInitialization(t *testing.T) {
	// Set threshold to -1 (disabled)
	val := int64(-1)
	lazyLoadThreshold = &val

	d := &agentFSDriver{
		lazyLoader: lazyLoader{
			pending:            make(map[string]*pb.FileMetadata),
			downloadOperations: make(map[string]*downloadOperation),
		},
		fanotifyFd: -1,
	}

	err := d.startLazyLoader(context.Background())
	if err != nil {
		t.Fatalf("expected no error when threshold is -1, got: %v", err)
	}
	if d.fanotifyFd != -1 {
		t.Fatalf("expected fanotifyFd to remain -1, got: %d", d.fanotifyFd)
	}
}

func TestLazyLoaderCoordinationWithDownloadOperation(t *testing.T) {
	// Test that multiple goroutines requesting the same file coordinate using the downloadOperation type
	d := &agentFSDriver{
		lazyLoader: lazyLoader{
			pending:            make(map[string]*pb.FileMetadata),
			downloadOperations: make(map[string]*downloadOperation),
		},
		fanotifyFd: -1,
	}

	testPath := "/var/lib/agentfs/vol-1/lower/large-file.txt"
	meta := &pb.FileMetadata{
		Path:   "large-file.txt",
		Size:   1024 * 1024,
		Sha256: "dummy-sha",
	}

	d.lazyLoader.Lock()
	d.lazyLoader.pending[testPath] = meta
	d.lazyLoader.Unlock()

	var downloadCount int
	var mu sync.Mutex

	var wg sync.WaitGroup
	numRequests := 5

	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			d.lazyLoader.Lock()
			currentOp, found := d.lazyLoader.downloadOperations[testPath]
			var isInitiator bool
			if !found {
				currentOp = newDownloadOperation(testPath, meta, d)
				d.lazyLoader.downloadOperations[testPath] = currentOp
				isInitiator = true
			}
			d.lazyLoader.Unlock()

			if isInitiator {
				mu.Lock()
				downloadCount++
				mu.Unlock()

				time.Sleep(50 * time.Millisecond)

				currentOp.Lock()
				currentOp.done = true
				currentOp.Unlock()

				close(currentOp.waitCh)

				d.lazyLoader.Lock()
				delete(d.lazyLoader.pending, testPath)
				delete(d.lazyLoader.downloadOperations, testPath)
				d.lazyLoader.Unlock()
			} else {
				<-currentOp.waitCh
			}
		}(i)
	}

	wg.Wait()

	mu.Lock()
	count := downloadCount
	mu.Unlock()

	if count != 1 {
		t.Errorf("expected only one initiator download, got %d", count)
	}

	d.lazyLoader.Lock()
	_, stillPending := d.lazyLoader.pending[testPath]
	d.lazyLoader.Unlock()

	if stillPending {
		t.Errorf("expected file to be removed from pending")
	}
}
