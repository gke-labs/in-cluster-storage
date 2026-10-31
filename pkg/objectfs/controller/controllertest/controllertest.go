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

package controllertest

import (
	"context"
	"os"

	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/controller"
	"github.com/gke-labs/in-cluster-storage/pkg/wal/buffer/buffertest"
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
	"k8s.io/klog/v2"
)

// NewTestServer constructs a controller.Server backed by an in-process WAL buffer over bufconn,
// skipping per-record WAL fsync.
func NewTestServer(backend controller.ObjectStorageBackend, opts ...controller.ServerOption) *controller.Server {
	if target := controller.InspectServerTarget(opts...); target != "" {
		opts = append([]controller.ServerOption{controller.WithServerWithoutFsync()}, opts...)
		return controller.NewServer(backend, opts...)
	}

	ctx := context.Background()
	bt, cleanup := buffertest.Start(ctx, nil, buffertest.WithBackend(backend))
	walDir, err := os.MkdirTemp("", "controllertest-wal-*")
	if err != nil {
		panic(err)
	}
	combinedCleanup := func() {
		cleanup()
		if err := os.RemoveAll(walDir); err != nil {
			klog.V(4).Infof("controllertest: remove walDir %s: %v", walDir, err)
		}
	}

	testOpts := []controller.ServerOption{
		controller.WithServerWAL(walDir, bt.Target, walclient.Witness),
		controller.WithServerWALClient(bt.Client),
		controller.WithServerWALDialOptions(bt.DialOpts...),
		controller.WithServerWithoutFsync(),
		controller.WithServerTestCleanup(combinedCleanup),
	}
	testOpts = append(testOpts, opts...)
	return controller.NewServer(backend, testOpts...)
}
