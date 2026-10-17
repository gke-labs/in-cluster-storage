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
	"crypto/tls"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/container-storage-interface/spec/lib/go/csi"
	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/controller"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore"
	"github.com/gke-labs/in-cluster-storage/pkg/sds"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/projection/table"
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
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
	metadataIndexCacheMB     = flag.Int("metadata-index-cache-mb", 64, "Pebble shared block cache size in megabytes for table metadata index")
	metadataCacheEntries     = flag.Int("metadata-cache-entries", 0, "Maximum number of entries in metadata read cache (0 for unconstrained, governed by --metadata-cache-bytes)")
	metadataCacheBytes       = flag.Int64("metadata-cache-bytes", 64*1024*1024, "Maximum byte size of metadata read cache")
	metadataOverlayMaxBytes  = flag.Int64("metadata-overlay-max-bytes", 64*1024*1024, "Maximum byte size of unapplied metadata overlay before applying backpressure")
	metadataApplierBatchSize = flag.Int("metadata-applier-batch-size", 100, "Maximum batch size for SQLite background applier")
	debugMutationCheck       = flag.Bool("debug-mutation-check", false, "Enable debug mutation checking on metadata view (for test/CI gates)")
	debugStatsCheck          = flag.Bool("debug-stats-check", false, "Enable debug volume stats checking (for test/CI gates)")
	syncCoalesceWindow       = flag.Duration("sync-coalesce-window", 1*time.Second, "Coalescing window for non-blocking sync requests")
	maxStagingBytes          = flag.Int64("max-staging-bytes", 64*1024*1024, "Maximum byte size of in-memory write staging per volume before backpressure or rejection")
	controllerSyncTimeout    = flag.Duration("controller-sync-timeout", 10*time.Minute, "Timeout for controller coalesced background sync operations")
	tlsCertFile              = flag.String("tls-cert-file", "", "Path to TLS certificate file")
	tlsKeyFile               = flag.String("tls-key-file", "", "Path to TLS private key file")
	insecure                 = flag.Bool("insecure", false, "Disable TLS and token authentication (e.g. for local dev/testing)")
	authAudience             = flag.String("auth-audience", controller.DefaultAudience, "Audience required in projected ServiceAccount tokens")
	authAdminSA              = flag.String("auth-admin-sa", "objectfs-admin,objectfs-controller", "Comma-separated list of service accounts granted admin role")
	authNodeSA               = flag.String("auth-node-sa", "objectfs-node-daemon", "Comma-separated list of service accounts granted node role")
	authCacheTTL             = flag.Duration("auth-cache-ttl", 5*time.Minute, "Cache TTL for successful TokenReviews")
	authNegativeTTL          = flag.Duration("auth-negative-ttl", 10*time.Second, "Cache TTL for failed TokenReviews")
	kubeAPIURL               = flag.String("kube-api-url", "", "Kubernetes API server URL for TokenReview (defaults to in-cluster)")
	kubeTokenFile            = flag.String("kube-token-file", "", "Path to controller ServiceAccount token for TokenReview")
	kubeCAFile               = flag.String("kube-ca-file", "", "Path to Kubernetes cluster CA cert")
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

func fileExists(p string) bool {
	info, err := os.Stat(p)
	return err == nil && !info.IsDir()
}

func main() {
	klog.InitFlags(nil)
	flag.Parse()

	// Controller registration uses explicit options: WithDisableWAL(true)
	// (stream is the source of truth, position and rows move in one batch),
	// and one process-wide pebble.Cache shared by all volumes. Memtable size
	// keeps Pebble's ramp default (starts at min(256KB, MemTableSize) and doubles up to 4MB).
	var tableOpts []table.Option
	tableOpts = append(tableOpts, table.WithDisableWAL(true)) // stream is the source of truth, position and rows move in one batch
	if *metadataIndexCacheMB > 0 {
		tableCache := pebble.NewCache(int64(*metadataIndexCacheMB) * 1024 * 1024)
		defer tableCache.Unref()
		tableOpts = append(tableOpts, table.WithCache(tableCache))
	}
	sds.RegisterIndexFactory(table.NewFactory(tableOpts...))

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
	if *syncCoalesceWindow > 0 {
		serverOpts = append(serverOpts, controller.WithServerSyncCoalesceWindow(*syncCoalesceWindow))
	}
	if *maxStagingBytes > 0 {
		serverOpts = append(serverOpts, controller.WithServerMaxStagingBytes(*maxStagingBytes))
	}
	if *controllerSyncTimeout > 0 {
		serverOpts = append(serverOpts, controller.WithServerSyncTimeout(*controllerSyncTimeout))
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
	} else {
		klog.Warningf("objectfs-controller: no WAL stream configured (--wal-dir unset): fsync is NOT durable across controller restarts; volumes behave as ephemeral shared disks and metadata is persisted only by the periodic flush (--flush-interval=%s) and on shutdown", *flushInterval)
	}

	var grpcServerOpts []grpc.ServerOption
	if *insecure {
		klog.Warning("Running in insecure mode: TLS and token authentication are disabled on the gRPC port")
	} else {
		certPath := *tlsCertFile
		if certPath == "" && fileExists("/etc/objectfs/tls/tls.crt") {
			certPath = "/etc/objectfs/tls/tls.crt"
		}
		keyPath := *tlsKeyFile
		if keyPath == "" && fileExists("/etc/objectfs/tls/tls.key") {
			keyPath = "/etc/objectfs/tls/tls.key"
		}
		if certPath == "" || keyPath == "" {
			klog.Fatalf("TLS certificate and key required (--tls-cert-file, --tls-key-file) unless --insecure is specified")
		}

		cert, err := tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			klog.Fatalf("failed to load TLS certificate from %s / %s: %v", certPath, keyPath, err)
		}
		tlsCreds := credentials.NewServerTLSFromCert(&cert)

		kubeReviewer, err := controller.NewKubeTokenReviewer(*kubeAPIURL, *kubeTokenFile, *kubeCAFile)
		if err != nil {
			klog.Fatalf("failed to initialize Kubernetes TokenReview client: %v", err)
		}
		cachedReviewer := controller.NewCachedTokenReviewer(kubeReviewer, *authCacheTTL, *authNegativeTTL)

		var audiences []string
		for _, a := range strings.Split(*authAudience, ",") {
			if a = strings.TrimSpace(a); a != "" {
				audiences = append(audiences, a)
			}
		}
		var adminSAs []string
		for _, s := range strings.Split(*authAdminSA, ",") {
			if s = strings.TrimSpace(s); s != "" {
				adminSAs = append(adminSAs, s)
			}
		}
		var nodeSAs []string
		for _, s := range strings.Split(*authNodeSA, ",") {
			if s = strings.TrimSpace(s); s != "" {
				nodeSAs = append(nodeSAs, s)
			}
		}

		authConfig := controller.AuthConfig{
			Audiences:            audiences,
			AdminServiceAccounts: adminSAs,
			NodeServiceAccounts:  nodeSAs,
			Insecure:             false,
		}
		authInterceptor := controller.NewServerAuthInterceptor(cachedReviewer, authConfig)

		grpcServerOpts = append(grpcServerOpts,
			grpc.Creds(tlsCreds),
			grpc.UnaryInterceptor(authInterceptor.UnaryServerInterceptor()),
			grpc.StreamInterceptor(authInterceptor.StreamServerInterceptor()),
		)
	}

	grpcServer := grpc.NewServer(grpcServerOpts...)
	server := controller.NewServer(backend, serverOpts...)
	csiController := controller.NewCSIController(server)

	if *flushInterval > 0 {
		server.StartPeriodicFlush(ctx, *flushInterval)
	}

	pb.RegisterObjectFSControllerServer(grpcServer, server)
	csi.RegisterIdentityServer(grpcServer, csiController)
	csi.RegisterControllerServer(grpcServer, csiController)

	var csiServer *grpc.Server
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

		csiServer = grpc.NewServer()
		csi.RegisterIdentityServer(csiServer, csiController)
		csi.RegisterControllerServer(csiServer, csiController)

		go func() {
			klog.Infof("ObjectFS CSI controller listening on %s", *csiEndpoint)
			if err := csiServer.Serve(csiListener); err != nil {
				klog.Fatalf("failed to serve CSI controller: %v", err)
			}
		}()
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		klog.Infof("Shutting down ObjectFS controller...")
		if csiServer != nil {
			csiServer.GracefulStop()
		}
		grpcServer.GracefulStop()
	}()

	klog.Infof("ObjectFS Controller listening on port %d (flushInterval=%v)", *port, *flushInterval)
	serveErr := grpcServer.Serve(listener)
	if err := server.Close(); err != nil {
		klog.Errorf("failed to close server on shutdown: %v", err)
		klog.Flush()
		os.Exit(1)
	}
	if serveErr != nil {
		klog.Fatalf("failed to serve: %v", serveErr)
	}
}
