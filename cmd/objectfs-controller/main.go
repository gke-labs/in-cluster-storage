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
	"path/filepath"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/controller"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore"
	_ "github.com/gke-labs/in-cluster-storage/pkg/sds/projection/table"
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
	"google.golang.org/grpc"
	"k8s.io/klog/v2"
)

var (
	port                     = flag.Int("port", 50051, "The server port")
	csiEndpoint              = flag.String("csi-endpoint", "", "CSI endpoint (e.g. unix:///csi/csi.sock)")
	backendFlag              = flag.String("backend", "", "Object storage backend URL (e.g. gs://bucket/prefix, s3://bucket/prefix, memory://)")
	metadataDir              = flag.String("metadata-dir", "", "Local directory for persistent metadata indices (e.g. SQLite databases)")
	flushInterval            = flag.Duration("flush-interval", 1*time.Hour, "Periodic flush interval to backend object storage")
	walDir                   = flag.String("wal-dir", "", "Local directory for caching WAL segments (enables Streams metadata change-log if set)")
	walTarget                = flag.String("wal-target", "", "Target gRPC address for central WAL buffer (e.g. wal-buffer:50051)")
	walDurability            = flag.String("wal-durability", "local", "Default WAL durability level (local, witness, permanent)")
	metadataIndex            = flag.String("metadata-index", "sqlite", "Metadata local index type (sqlite, memory, table)")
	metadataCacheEntries     = flag.Int("metadata-cache-entries", 0, "Maximum number of entries in metadata read cache (0 for unconstrained, governed by --metadata-cache-bytes)")
	metadataCacheBytes       = flag.Int64("metadata-cache-bytes", 64*1024*1024, "Maximum byte size of metadata read cache")
	metadataOverlayMaxBytes  = flag.Int64("metadata-overlay-max-bytes", 64*1024*1024, "Maximum byte size of unapplied metadata overlay before applying backpressure")
	metadataApplierBatchSize = flag.Int("metadata-applier-batch-size", 100, "Maximum batch size for SQLite background applier")
	debugMutationCheck       = flag.Bool("debug-mutation-check", false, "Enable debug mutation checking on metadata view (for test/CI gates)")
	debugStatsCheck          = flag.Bool("debug-stats-check", false, "Enable debug volume stats checking (for test/CI gates)")
)

func parseEndpoint(endpoint string) (string, string, error) {
	parts := strings.SplitN(endpoint, "://", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid endpoint: %s", endpoint)
	}
	scheme, addr := parts[0], parts[1]
	if scheme != "unix" && scheme != "tcp" {
		return "", "", fmt.Errorf("invalid endpoint: %s", endpoint)
	}
	return scheme, addr, nil
}

func main() {
	klog.InitFlags(nil)
	flag.Parse()

	if *backendFlag == "" {
		klog.Fatalf("missing required flag: --backend (e.g. gs://bucket/prefix, s3://bucket/prefix, memory://)")
	}

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		klog.Fatalf("failed to listen: %v", err)
	}

	ctx := context.Background()
	backend, err := objectstore.Open(ctx, *backendFlag)
	if err != nil {
		klog.Fatalf("failed to initialize backend %q: %v", *backendFlag, err)
	}

	var serverOpts []controller.ServerOption
	if *metadataIndex != "" {
		serverOpts = append(serverOpts, controller.WithServerMetadataIndex(*metadataIndex))
	}
	if *metadataDir != "" {
		serverOpts = append(serverOpts, controller.WithServerMetadataDir(*metadataDir))
	}
	if *metadataCacheEntries > 0 || *metadataCacheBytes > 0 {
		serverOpts = append(serverOpts, controller.WithServerMetadataCacheLimits(*metadataCacheEntries, *metadataCacheBytes))
	}
	if *metadataOverlayMaxBytes > 0 {
		serverOpts = append(serverOpts, controller.WithServerMetadataOverlayMaxBytes(*metadataOverlayMaxBytes))
	}
	if *metadataApplierBatchSize > 0 {
		serverOpts = append(serverOpts, controller.WithServerMetadataApplierBatchSize(*metadataApplierBatchSize))
	}
	if *debugMutationCheck {
		serverOpts = append(serverOpts, controller.WithServerMutationCheck(), controller.WithServerStatsCheck())
	}
	if *debugStatsCheck && !*debugMutationCheck {
		serverOpts = append(serverOpts, controller.WithServerStatsCheck())
	}
	// TODO: We always want to assume a WAL, and maybe it would be nice to have a wal-client mode that was local-only e.g. for testing.
	if *walDir != "" {
		durability := walclient.Local
		switch strings.ToLower(*walDurability) {
		case "witness":
			durability = walclient.Witness
		case "permanent":
			durability = walclient.Permanent
		case "local":
			durability = walclient.Local
		default:
			klog.Fatalf("invalid wal-durability: %s (must be local, witness, or permanent)", *walDurability)
		}
		if *walTarget == "" && durability > walclient.Local {
			klog.Fatalf("invalid configuration: wal-durability %s requires --wal-target", *walDurability)
		}
		serverOpts = append(serverOpts, controller.WithServerWAL(*walDir, *walTarget, durability))
	}

	grpcServer := grpc.NewServer()
	server := controller.NewServer(backend, serverOpts...)
	defer server.Close()
	csiController := controller.NewCSIController(server)

	if *flushInterval > 0 {
		server.StartPeriodicFlush(ctx, *flushInterval)
		defer server.StopPeriodicFlush()
	}

	pb.RegisterObjectFSControllerServer(grpcServer, server)
	csi.RegisterIdentityServer(grpcServer, csiController)
	csi.RegisterControllerServer(grpcServer, csiController)

	if *csiEndpoint != "" {
		proto, addr, err := parseEndpoint(*csiEndpoint)
		if err != nil {
			klog.Fatalf("failed to parse csi-endpoint %s: %v", *csiEndpoint, err)
		}

		if proto == "unix" {
			addr = filepath.FromSlash(addr)
			if err := os.Remove(addr); err != nil && !os.IsNotExist(err) {
				klog.Fatalf("failed to remove %s: %v", addr, err)
			}
		}

		csiListener, err := net.Listen(proto, addr)
		if err != nil {
			klog.Fatalf("failed to listen on CSI endpoint %s: %v", *csiEndpoint, err)
		}

		csiServer := grpc.NewServer()
		csi.RegisterIdentityServer(csiServer, csiController)
		csi.RegisterControllerServer(csiServer, csiController)

		go func() {
			klog.Infof("ObjectFS CSI controller listening on %s", *csiEndpoint)
			if err := csiServer.Serve(csiListener); err != nil {
				klog.Fatalf("failed to serve CSI controller: %v", err)
			}
		}()
	}

	klog.Infof("ObjectFS Controller listening on port %d (flushInterval=%v)", *port, *flushInterval)
	if err := grpcServer.Serve(listener); err != nil {
		klog.Fatalf("failed to serve: %v", err)
	}
}
