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
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/wal/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore"
	"github.com/gke-labs/in-cluster-storage/pkg/wal/buffer"
	"google.golang.org/grpc"
	"k8s.io/klog/v2"
)

var (
	port                      = flag.Int("port", 50051, "gRPC server port")
	dataDir                   = flag.String("data-dir", "/data", "Local directory for caching WAL segments")
	backendType               = flag.String("backend", "", "Object storage backend URL (e.g. gs://bucket/prefix, s3://bucket/prefix, memory://)")
	flushInterval             = flag.Duration("flush-interval", 60*time.Second, "Periodic flush interval to backend object storage")
	flushBytes                = flag.Int64("flush-bytes", 64*1024*1024, "Unflushed bytes threshold to trigger S3 flush")
	tailCacheBytes            = flag.Int64("tail-cache-bytes", 64*1024*1024, "Retained bytes in local cache for tailing")
	batchMaxDelay             = flag.Duration("batch-max-delay", 5*time.Millisecond, "Maximum delay for group commit batching")
	batchMaxSize              = flag.Int64("batch-max-size", 1024*1024, "Maximum byte size for group commit batching")
	acceptUnversionedSegments = flag.Bool("accept-unversioned-segments", false, "Accept unversioned WAL segments and streams for one-time upgrade")
)

func main() {
	klog.InitFlags(nil)
	flag.Parse()

	if *backendType == "" {
		klog.Fatalf("missing required flag: --backend (e.g. gs://bucket/prefix, s3://bucket/prefix, memory://)")
	}

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		klog.Fatalf("failed to listen on port %d: %v", *port, err)
	}

	ctx := context.Background()
	backend, err := objectstore.Open(ctx, *backendType)
	if err != nil {
		klog.Fatalf("failed to initialize backend %q: %v", *backendType, err)
	}

	cfg := buffer.ServerConfig{
		Backend:           backend,
		DataDir:           *dataDir,
		FlushInterval:     *flushInterval,
		FlushBytes:        *flushBytes,
		TailCacheBytes:    *tailCacheBytes,
		BatchMaxDelay:     *batchMaxDelay,
		BatchMaxSize:      *batchMaxSize,
		AcceptUnversioned: *acceptUnversionedSegments,
	}

	srv, err := buffer.NewServer(ctx, cfg)
	if err != nil {
		klog.Fatalf("failed to initialize WAL buffer server: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterWalBufferServer(grpcServer, srv)

	// Graceful shutdown handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		klog.Infof("Shutting down WAL buffer server...")
		grpcServer.GracefulStop()
	}()

	klog.Infof("WAL Buffer Service listening on port %d (last_position=%d, backend=%s, data-dir=%s)", *port, srv.LastPosition(), *backendType, *dataDir)
	serveErr := grpcServer.Serve(listener)
	if err := srv.Close(); err != nil {
		klog.Errorf("failed to close server on shutdown: %v", err)
		klog.Flush()
		os.Exit(1)
	}
	if serveErr != nil {
		klog.Fatalf("failed to serve: %v", serveErr)
	}
}
