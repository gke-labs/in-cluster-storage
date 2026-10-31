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

	"github.com/gke-labs/in-cluster-storage/pkg/wal/buffer/buffertest"
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
	"k8s.io/klog/v2"
)

// NewTestServer constructs a Server configured for testing with an in-process WAL buffer over bufconn,
// skipping per-record WAL fsync.
// The production controller binary uses NewServer, which always retains per-record fsync.
func NewTestServer(backend ObjectStorageBackend, opts ...ServerOption) *Server {
	if target := InspectServerTarget(opts...); target != "" {
		opts = append([]ServerOption{WithServerWithoutFsync()}, opts...)
		return NewServer(backend, opts...)
	}

	ctx := context.TODO()
	bt, cleanup := buffertest.Start(ctx, nil, buffertest.WithBackend(backend))
	walDir, err := os.MkdirTemp("", "controllertest-wal-*")
	if err != nil {
		panic(err)
	}
	combinedCleanup := func() {
		cleanup()
		if err := os.RemoveAll(walDir); err != nil {
			klog.V(4).Infof("failed to remove test walDir %s: %v", walDir, err)
		}
	}

	testOpts := []ServerOption{
		WithServerWAL(walDir, bt.Target, walclient.Witness),
		WithServerWALClient(bt.Client),
		WithServerWALDialOptions(bt.DialOpts...),
		WithServerWithoutFsync(),
		WithServerTestCleanup(combinedCleanup),
	}
	testOpts = append(testOpts, opts...)
	return NewServer(backend, testOpts...)
}
