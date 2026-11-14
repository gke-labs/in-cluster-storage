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
	"errors"
	"flag"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/fuseholder"
	"github.com/gke-labs/in-cluster-storage/pkg/scoped"
	"k8s.io/klog/v2"
)

var socketPath = flag.String("socket-path", "/run/objectfs/fuse-holder.sock", "Path to Unix domain socket")

func main() {
	klog.InitFlags(nil)
	flag.Parse()

	ctx := scoped.WithLog(context.Background(), klog.Background())

	if *socketPath == "" {
		klog.Fatal("--socket-path is required")
	}

	listener, err := fuseholder.Listen(*socketPath)
	if err != nil {
		klog.Fatalf("failed to listen on socket %s: %v", *socketPath, err)
	}
	defer func() {
		if closeErr := listener.Close(); closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
			scoped.Log(ctx).Info("failed to close listener", "err", closeErr)
		}
	}()

	holder := fuseholder.New()
	defer func() {
		if closeErr := holder.Close(); closeErr != nil {
			scoped.Log(ctx).Info("failed to close holder", "err", closeErr)
		}
	}()

	stopSignal := make(chan os.Signal, 1)
	signal.Notify(stopSignal, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-stopSignal
		if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			scoped.Log(ctx).Info("failed to close listener on signal", "err", err)
		}
		if err := holder.Close(); err != nil {
			scoped.Log(ctx).Info("failed to close holder on signal", "err", err)
		}
	}()

	scoped.Log(ctx).Info("FUSE holder listening", "path", *socketPath)
	if err := holder.Serve(listener); err != nil && !errors.Is(err, net.ErrClosed) {
		klog.Fatalf("holder serve error: %v", err)
	}
}
