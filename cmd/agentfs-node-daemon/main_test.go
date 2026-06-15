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
			pending:     make(map[string]*pb.FileMetadata),
			downloading: make(map[string]chan struct{}),
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

func TestLazyLoaderCoordination(t *testing.T) {
	// Test that multiple goroutines requesting the same file coordinate and only one downloads it
	d := &agentFSDriver{
		lazyLoader: lazyLoader{
			pending:     make(map[string]*pb.FileMetadata),
			downloading: make(map[string]chan struct{}),
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

	// Track how many times download is executed
	var downloadCount int
	var mu sync.Mutex

	mockDownload := func() error {
		mu.Lock()
		downloadCount++
		mu.Unlock()
		time.Sleep(100 * time.Millisecond) // simulate download time
		return nil
	}

	var wg sync.WaitGroup
	numRequests := 5

	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Simulate the coordination phase inside handleFanotifyEvent
			d.lazyLoader.Lock()
			ch, downloading := d.lazyLoader.downloading[testPath]
			if !downloading {
				ch = make(chan struct{})
				d.lazyLoader.downloading[testPath] = ch
				d.lazyLoader.Unlock()

				// Perform download
				err := mockDownload()
				if err == nil {
					d.lazyLoader.Lock()
					delete(d.lazyLoader.pending, testPath)
					d.lazyLoader.Unlock()
				}

				close(ch)
				d.lazyLoader.Lock()
				delete(d.lazyLoader.downloading, testPath)
				d.lazyLoader.Unlock()
			} else {
				d.lazyLoader.Unlock()
				<-ch
			}
		}()
	}

	wg.Wait()

	mu.Lock()
	count := downloadCount
	mu.Unlock()

	if count != 1 {
		t.Errorf("expected download to be executed exactly once, got %d times", count)
	}

	d.lazyLoader.Lock()
	_, stillPending := d.lazyLoader.pending[testPath]
	d.lazyLoader.Unlock()

	if stillPending {
		t.Errorf("expected file to be removed from pending list")
	}
}
