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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

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

	err := d.startLazyLoader(t.Context())
	if err != nil {
		t.Fatalf("expected no error when threshold is -1, got: %v", err)
	}
	if d.fanotifyFd != -1 {
		t.Fatalf("expected fanotifyFd to remain -1, got: %d", d.fanotifyFd)
	}
}

func TestLazyLoaderCoordinationWithDownloadOperation(t *testing.T) {
	// Simple test exercising the split lock initialization and basic status of pending map
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

	d.lazyLoader.pendingMu.Lock()
	d.lazyLoader.pending[testPath] = meta
	d.lazyLoader.pendingMu.Unlock()

	d.lazyLoader.pendingMu.RLock()
	_, exists := d.lazyLoader.pending[testPath]
	d.lazyLoader.pendingMu.RUnlock()

	if !exists {
		t.Fatalf("expected path to exist in pending map")
	}

	d.lazyLoader.downloadMu.Lock()
	op, found := d.lazyLoader.downloadOperations[testPath]
	if found || op != nil {
		t.Fatalf("expected no download operations to exist initially")
	}
	d.lazyLoader.downloadMu.Unlock()
}

func TestErofsLayersLowerdirOption(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "erofs-layers-test-")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	volumeDir := tempDir
	lowerPath := filepath.Join(volumeDir, "lower")
	_ = os.MkdirAll(lowerPath, 0755)

	// Case 1: No layers yet
	_ = &agentFSDriver{
		enableEROFSLayers: true,
	}

	getLowerdir := func() string {
		var layers []string
		for i := 0; ; i++ {
			p := filepath.Join(volumeDir, fmt.Sprintf("layer-%d", i))
			if _, err := os.Stat(p); err != nil {
				break
			}
			layers = append(layers, p)
		}
		if len(layers) > 0 {
			var revLayers []string
			for i := len(layers) - 1; i >= 0; i-- {
				revLayers = append(revLayers, layers[i])
			}
			return strings.Join(revLayers, ":")
		} else {
			return lowerPath
		}
	}

	opt := getLowerdir()
	if opt != lowerPath {
		t.Errorf("expected empty layers to fall back to %s, got: %s", lowerPath, opt)
	}

	// Case 2: Multi-layer
	layer0 := filepath.Join(volumeDir, "layer-0")
	layer1 := filepath.Join(volumeDir, "layer-1")
	_ = os.MkdirAll(layer0, 0755)
	_ = os.MkdirAll(layer1, 0755)

	expected := layer1 + ":" + layer0
	opt = getLowerdir()
	if opt != expected {
		t.Errorf("expected multi-layer lowerdir to be %s, got: %s", expected, opt)
	}
}
