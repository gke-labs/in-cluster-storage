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
	"crypto/x509"
	"encoding/pem"
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
	"github.com/gke-labs/in-cluster-storage/pkg/grpcutil"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/controller"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore"
	"github.com/gke-labs/in-cluster-storage/pkg/scoped"
	"github.com/gke-labs/in-cluster-storage/pkg/sds"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/projection/sqlite"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/projection/table"
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
	"google.golang.org/grpc"
	"k8s.io/klog/v2"
)

// Options holds configuration for the ObjectFS controller.
type Options struct {
	Port                      int
	ServerCertDir             string
	Insecure                  bool
	Namespace                 string
	CSIEndpoint               string
	Backend                   string
	MetadataDir               string
	FlushInterval             time.Duration
	WALDir                    string
	WALTarget                 string
	WALDurability             walclient.Level
	MetadataIndex             string
	MetadataIndexCacheMB      int
	MetadataCacheEntries      int
	MetadataCacheBytes        int64
	MetadataOverlayMaxBytes   int64
	MetadataApplierBatchSize  int
	DebugMutationCheck        bool
	DebugStatsCheck           bool
	SyncCoalesceWindow        time.Duration
	MaxStagingBytes           int64
	MaxLocalPackCacheBytes    int64
	MaxLocalStagingBytes      int64
	ControllerSyncTimeout     time.Duration
	OpenBarrierTimeout        time.Duration
	AcceptUnversionedSegments bool
}

// NewOptions returns an Options struct populated with defaults.
func NewOptions() *Options {
	return &Options{
		Port:                     50051,
		ServerCertDir:            "/etc/objectfs/certs/server",
		FlushInterval:            1 * time.Hour,
		WALDurability:            walclient.Local,
		MetadataIndex:            "sqlite",
		MetadataIndexCacheMB:     64,
		MetadataCacheBytes:       64 * 1024 * 1024,
		MetadataOverlayMaxBytes:  64 * 1024 * 1024,
		MetadataApplierBatchSize: 100,
		SyncCoalesceWindow:       1 * time.Second,
		MaxStagingBytes:          64 * 1024 * 1024,
		MaxLocalPackCacheBytes:   8 * 1024 * 1024 * 1024,
		MaxLocalStagingBytes:     16 * 1024 * 1024 * 1024,
		ControllerSyncTimeout:    10 * time.Minute,
		OpenBarrierTimeout:       30 * time.Second,
	}
}

func validateMetadataIndex(value string) error {
	if value != "sqlite" && value != "table" {
		return fmt.Errorf("invalid metadata index %q; accepted values: sqlite, table", value)
	}
	return nil
}

type metadataIndexValue struct {
	target *string
}

func (v *metadataIndexValue) String() string {
	if v == nil || v.target == nil {
		return ""
	}
	return *v.target
}

func (v *metadataIndexValue) Set(value string) error {
	if err := validateMetadataIndex(value); err != nil {
		return err
	}
	*v.target = value
	return nil
}

// AddFlags registers flags into the provided FlagSet.
func (o *Options) AddFlags(flagSet *flag.FlagSet) {
	flagSet.IntVar(&o.Port, "port", o.Port, "The server port")
	flagSet.StringVar(&o.ServerCertDir, "server-cert-dir", o.ServerCertDir, "Directory containing tls.key, tls.crt, and trust/*.crt for mTLS")
	flagSet.BoolVar(&o.Insecure, "insecure", o.Insecure, "Disable TLS and client certificate authentication (dev only)")
	flagSet.StringVar(&o.Namespace, "namespace", o.Namespace, "Kubernetes namespace for caller authorization (default from POD_NAMESPACE)")
	flagSet.StringVar(&o.CSIEndpoint, "csi-endpoint", o.CSIEndpoint, "CSI endpoint (e.g. unix:///csi/csi.sock)")
	flagSet.StringVar(&o.Backend, "backend", o.Backend, "Object storage backend URL (e.g. gs://bucket/prefix, s3://bucket/prefix, memory://)")
	flagSet.StringVar(&o.MetadataDir, "metadata-dir", o.MetadataDir, "Local directory for persistent metadata indices (e.g. SQLite databases)")
	flagSet.DurationVar(&o.FlushInterval, "flush-interval", o.FlushInterval, "Periodic flush interval to backend object storage")
	flagSet.StringVar(&o.WALDir, "wal-dir", o.WALDir, "Local directory for caching WAL segments (enables Streams metadata change-log if set)")
	flagSet.StringVar(&o.WALTarget, "wal-target", o.WALTarget, "Target gRPC address for central WAL buffer (e.g. wal-buffer-client:50051)")
	flagSet.Var(walclient.NewLevelValue(&o.WALDurability), "wal-durability", "Default WAL durability level (local, witness, permanent)")
	flagSet.Var(&metadataIndexValue{target: &o.MetadataIndex}, "metadata-index", "Metadata local index type (sqlite, table)")
	flagSet.IntVar(&o.MetadataIndexCacheMB, "metadata-index-cache-mb", o.MetadataIndexCacheMB, "Pebble shared block cache size in megabytes for table metadata index")
	flagSet.IntVar(&o.MetadataCacheEntries, "metadata-cache-entries", o.MetadataCacheEntries, "Maximum number of entries in metadata read cache (0 for unconstrained, governed by --metadata-cache-bytes)")
	flagSet.Int64Var(&o.MetadataCacheBytes, "metadata-cache-bytes", o.MetadataCacheBytes, "Maximum byte size of metadata read cache")
	flagSet.Int64Var(&o.MetadataOverlayMaxBytes, "metadata-overlay-max-bytes", o.MetadataOverlayMaxBytes, "Maximum byte size of unapplied metadata overlay before applying backpressure")
	flagSet.IntVar(&o.MetadataApplierBatchSize, "metadata-applier-batch-size", o.MetadataApplierBatchSize, "Maximum batch size for SQLite background applier")
	flagSet.BoolVar(&o.DebugMutationCheck, "debug-mutation-check", o.DebugMutationCheck, "Enable debug mutation checking on metadata view (for test/CI gates)")
	flagSet.BoolVar(&o.DebugStatsCheck, "debug-stats-check", o.DebugStatsCheck, "Enable debug volume stats checking (for test/CI gates)")
	flagSet.DurationVar(&o.SyncCoalesceWindow, "sync-coalesce-window", o.SyncCoalesceWindow, "Coalescing window for non-blocking sync requests")
	flagSet.Int64Var(&o.MaxStagingBytes, "max-staging-bytes", o.MaxStagingBytes, "Maximum byte size of in-memory write staging per volume before backpressure or rejection")
	flagSet.Int64Var(&o.MaxLocalPackCacheBytes, "max-local-pack-cache-bytes", o.MaxLocalPackCacheBytes, "Maximum controller-wide uploaded pack cache bytes on local disk across all volumes before evicting")
	flagSet.Int64Var(&o.MaxLocalStagingBytes, "max-local-staging-bytes", o.MaxLocalStagingBytes, "Maximum controller-wide write staging bytes on local disk across all volumes before failing fast with ENOSPC")
	flagSet.DurationVar(&o.ControllerSyncTimeout, "controller-sync-timeout", o.ControllerSyncTimeout, "Timeout for controller coalesced background sync operations")
	flagSet.DurationVar(&o.OpenBarrierTimeout, "open-barrier-timeout", o.OpenBarrierTimeout, "Timeout for open-side barrier waiting on in-flight or staged syncs on close-to-open volumes")
	flagSet.BoolVar(&o.AcceptUnversionedSegments, "accept-unversioned-segments", o.AcceptUnversionedSegments, "Accept unversioned WAL segments, streams and snapshots for one-time upgrade")
}

// Validate validates that the configured options are valid.
func (o *Options) Validate() error {
	if o.Backend == "" {
		return fmt.Errorf("missing required flag: --backend (e.g. gs://bucket/prefix, s3://bucket/prefix, memory://)")
	}
	if o.MetadataDir == "" {
		return fmt.Errorf("missing required flag: --metadata-dir (local directory required for persistent metadata and write staging)")
	}
	if o.WALDir == "" {
		return fmt.Errorf("invalid configuration: --wal-dir is required")
	}
	if o.WALTarget == "" {
		return fmt.Errorf("invalid configuration: --wal-target is required")
	}
	if err := validateMetadataIndex(o.MetadataIndex); err != nil {
		return err
	}
	if o.CSIEndpoint != "" {
		if _, _, err := parseEndpoint(o.CSIEndpoint); err != nil {
			return fmt.Errorf("failed to parse csi-endpoint %s: %w", o.CSIEndpoint, err)
		}
	}
	return nil
}

func evaluateLeafExpiry(cert *x509.Certificate, now time.Time) {
	if cert == nil {
		return
	}
	remaining := cert.NotAfter.Sub(now)
	if remaining <= 0 {
		klog.Errorf("TLS leaf certificate expired at %s (expired %s ago). Rotate secret and rollout restart controller and node daemons immediately!", cert.NotAfter.Format(time.RFC3339), (-remaining).Round(time.Second))
	} else if remaining <= 30*24*time.Hour {
		days := int(remaining.Hours() / 24)
		klog.Warningf("TLS leaf certificate will expire in %d days (at %s). Rotate secret and rollout restart controller and node daemons to avoid outage!", days, cert.NotAfter.Format(time.RFC3339))
	}
}

func checkLeafExpiry(certFile string, now time.Time) {
	data, err := os.ReadFile(certFile)
	if err != nil {
		klog.Warningf("could not read leaf cert %s for expiry check: %v", certFile, err)
		return
	}
	block, _ := pem.Decode(data)
	if block == nil {
		klog.Warningf("could not decode PEM in leaf cert %s", certFile)
		return
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		klog.Warningf("could not parse leaf cert %s: %v", certFile, err)
		return
	}
	evaluateLeafExpiry(cert, now)
}

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
	options := NewOptions()
	options.AddFlags(flag.CommandLine)
	flag.Parse()

	if err := options.Validate(); err != nil {
		klog.Fatalf("%v", err)
	}

	if err := run(*options); err != nil {
		klog.Fatalf("%v", err)
	}
}

const defaultDiskMarginBytes = 1 * 1024 * 1024 * 1024

func checkStagingDiskSpace(directory string, maxLocalPackCacheBytes int64, maxLocalStagingBytes int64, marginBytes int64) error {
	if directory == "" {
		return fmt.Errorf("staging directory is not specified")
	}
	if err := os.MkdirAll(directory, 0755); err != nil {
		return fmt.Errorf("failed to create staging directory %s: %w", directory, err)
	}
	var statfs syscall.Statfs_t
	if err := syscall.Statfs(directory, &statfs); err != nil {
		return fmt.Errorf("failed to statfs staging directory %s: %w", directory, err)
	}
	availableBytes := int64(statfs.Bavail) * int64(statfs.Bsize)
	requiredBytes := maxLocalPackCacheBytes + maxLocalStagingBytes + marginBytes
	if availableBytes < requiredBytes {
		return fmt.Errorf("insufficient disk space on %s: required %d bytes (cache %d, staging %d, margin %d), available %d bytes; lower --max-local-staging-bytes or --max-local-pack-cache-bytes",
			directory, requiredBytes, maxLocalPackCacheBytes, maxLocalStagingBytes, marginBytes, availableBytes)
	}
	return nil
}

func run(options Options) error {
	if err := checkStagingDiskSpace(options.MetadataDir, options.MaxLocalPackCacheBytes, options.MaxLocalStagingBytes, defaultDiskMarginBytes); err != nil {
		return fmt.Errorf("staging disk space check failed: %w", err)
	}

	// Controller registration uses explicit options: WithDisableWAL(true)
	// (stream is the source of truth, position and rows move in one batch),
	// and one process-wide pebble.Cache shared by all volumes. Memtable size
	// keeps Pebble's ramp default (starts at min(256KB, MemTableSize) and doubles up to 4MB).
	var tableOptions []table.Option
	tableOptions = append(tableOptions, table.WithDisableWAL(true)) // stream is the source of truth, position and rows move in one batch
	if options.MetadataIndexCacheMB > 0 {
		tableCache := pebble.NewCache(int64(options.MetadataIndexCacheMB) * 1024 * 1024)
		defer tableCache.Unref()
		tableOptions = append(tableOptions, table.WithCache(tableCache))
	}
	if options.AcceptUnversionedSegments {
		tableOptions = append(tableOptions, table.WithAcceptUnversioned(true))
		sds.RegisterIndexFactory(sqlite.NewFactory(sqlite.WithAcceptUnversioned(true)))
	}
	sds.RegisterIndexFactory(table.NewFactory(tableOptions...))

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", options.Port))
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	ctx := scoped.WithLog(context.Background(), klog.Background())
	backend, err := objectstore.Open(ctx, options.Backend)
	if err != nil {
		return fmt.Errorf("failed to initialize backend %q: %w", options.Backend, err)
	}

	var serverOptions []controller.ServerOption
	if options.MetadataIndex != "" {
		serverOptions = append(serverOptions, controller.WithServerMetadataIndex(options.MetadataIndex))
	}
	if options.MetadataDir != "" {
		serverOptions = append(serverOptions, controller.WithServerMetadataDir(options.MetadataDir))
	}
	if options.MetadataCacheEntries > 0 || options.MetadataCacheBytes > 0 {
		serverOptions = append(serverOptions, controller.WithServerMetadataCacheLimits(options.MetadataCacheEntries, options.MetadataCacheBytes))
	}
	if options.MetadataOverlayMaxBytes > 0 {
		serverOptions = append(serverOptions, controller.WithServerMetadataOverlayMaxBytes(options.MetadataOverlayMaxBytes))
	}
	if options.MetadataApplierBatchSize > 0 {
		serverOptions = append(serverOptions, controller.WithServerMetadataApplierBatchSize(options.MetadataApplierBatchSize))
	}
	if options.DebugMutationCheck {
		serverOptions = append(serverOptions, controller.WithServerMutationCheck(), controller.WithServerStatsCheck())
	}
	if options.DebugStatsCheck && !options.DebugMutationCheck {
		serverOptions = append(serverOptions, controller.WithServerStatsCheck())
	}
	if options.SyncCoalesceWindow > 0 {
		serverOptions = append(serverOptions, controller.WithServerSyncCoalesceWindow(options.SyncCoalesceWindow))
	}
	if options.MaxStagingBytes > 0 {
		serverOptions = append(serverOptions, controller.WithServerMaxStagingBytes(options.MaxStagingBytes))
	}
	if options.MaxLocalPackCacheBytes > 0 {
		serverOptions = append(serverOptions, controller.WithServerMaxLocalPackCacheBytes(options.MaxLocalPackCacheBytes))
	}
	if options.MaxLocalStagingBytes > 0 {
		serverOptions = append(serverOptions, controller.WithServerMaxLocalStagingBytes(options.MaxLocalStagingBytes))
	}
	if options.ControllerSyncTimeout > 0 {
		serverOptions = append(serverOptions, controller.WithServerSyncTimeout(options.ControllerSyncTimeout))
	}
	if options.OpenBarrierTimeout > 0 {
		serverOptions = append(serverOptions, controller.WithServerOpenBarrierTimeout(options.OpenBarrierTimeout))
	}
	if options.AcceptUnversionedSegments {
		serverOptions = append(serverOptions, controller.WithServerAcceptUnversioned(true))
	}
	serverOptions = append(serverOptions, controller.WithServerWAL(options.WALDir, options.WALTarget, options.WALDurability))

	var grpcServer *grpc.Server
	if options.Insecure {
		klog.Warning("Serving plaintext gRPC without TLS or authentication (--insecure is set)")
		grpcServer = grpc.NewServer(
			grpc.UnaryInterceptor(grpcutil.UnaryServerInterceptor()),
			grpc.StreamInterceptor(grpcutil.StreamServerInterceptor()),
		)
	} else {
		if !grpcutil.DirExists(options.ServerCertDir) {
			return fmt.Errorf("server certificate directory %s does not exist; pass --insecure for plaintext local dev", options.ServerCertDir)
		}

		checkLeafExpiry(filepath.Join(options.ServerCertDir, "tls.crt"), time.Now())
		go func() {
			ticker := time.NewTicker(24 * time.Hour)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case t := <-ticker.C:
					checkLeafExpiry(filepath.Join(options.ServerCertDir, "tls.crt"), t)
				}
			}
		}()

		tlsCreds, err := grpcutil.NewServerTLSFromDir(options.ServerCertDir)
		if err != nil {
			return fmt.Errorf("failed to load server TLS credentials from %s: %w", options.ServerCertDir, err)
		}

		ns := options.Namespace
		if ns == "" {
			ns = os.Getenv("POD_NAMESPACE")
		}
		if ns == "" {
			ns = "kube-objectfs-system"
		}

		authInterceptor := controller.NewServerAuthInterceptor(&controller.AuthConfig{
			Namespace: ns,
			Insecure:  false,
		})

		grpcServer = grpc.NewServer(
			grpc.Creds(tlsCreds),
			grpc.ChainUnaryInterceptor(grpcutil.UnaryServerInterceptor(), authInterceptor.UnaryServerInterceptor()),
			grpc.ChainStreamInterceptor(grpcutil.StreamServerInterceptor(), authInterceptor.StreamServerInterceptor()),
		)
	}
	server := controller.NewServer(backend, serverOptions...)
	csiController := controller.NewCSIController(server)

	if options.FlushInterval > 0 {
		server.StartPeriodicFlush(ctx, options.FlushInterval)
	}

	pb.RegisterObjectFSControllerServer(grpcServer, server)
	csi.RegisterIdentityServer(grpcServer, csiController)
	csi.RegisterControllerServer(grpcServer, csiController)

	var csiServer *grpc.Server
	if options.CSIEndpoint != "" {
		proto, addr, err := parseEndpoint(options.CSIEndpoint)
		if err != nil {
			return fmt.Errorf("failed to parse csi-endpoint %s: %w", options.CSIEndpoint, err)
		}

		if proto == "unix" {
			addr = filepath.FromSlash(addr)
			if err := os.Remove(addr); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("failed to remove %s: %w", addr, err)
			}
		}

		csiListener, err := net.Listen(proto, addr)
		if err != nil {
			return fmt.Errorf("failed to listen on CSI endpoint %s: %w", options.CSIEndpoint, err)
		}

		csiServer = grpc.NewServer(
			grpc.UnaryInterceptor(grpcutil.UnaryServerInterceptor()),
			grpc.StreamInterceptor(grpcutil.StreamServerInterceptor()),
		)
		csi.RegisterIdentityServer(csiServer, csiController)
		csi.RegisterControllerServer(csiServer, csiController)

		go func() {
			klog.Infof("ObjectFS CSI controller listening on %s", options.CSIEndpoint)
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

	klog.Infof("ObjectFS Controller listening on port %d (flushInterval=%v)", options.Port, options.FlushInterval)
	serveErr := grpcServer.Serve(listener)
	if err := server.Close(); err != nil {
		klog.Errorf("failed to close server on shutdown: %v", err)
		klog.Flush()
		os.Exit(1)
	}
	if serveErr != nil {
		return fmt.Errorf("failed to serve: %w", serveErr)
	}
	return nil
}
