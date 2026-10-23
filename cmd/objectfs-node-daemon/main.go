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
	"sync"
	"syscall"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/grpcutil"
	objectfuse "github.com/gke-labs/in-cluster-storage/pkg/objectfs/fuse"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
)

var (
	endpoint               = flag.String("endpoint", "unix:///tmp/csi.sock", "CSI endpoint")
	nodeID                 = flag.String("nodeid", "", "node id")
	controllerAddress      = flag.String("controller-address", "objectfs-controller:50051", "ObjectFS Controller address")
	cacheSizeMB            = flag.Int64("cache-size-mb", 128, "Local node in-memory cache size in MB (for dirty write buffers)")
	diskCacheSizeMB        = flag.Int64("disk-cache-size-mb", 10240, "Local node on-disk blob cache size in MB (default 10 GiB)")
	cacheDir               = flag.String("cache-dir", "/var/cache/objectfs", "Path to node-local on-disk blob cache directory")
	controllerRPCTimeout   = flag.Duration("controller-rpc-timeout", 30*time.Second, "Timeout for RPCs to ObjectFS controller")
	controllerSyncTimeout  = flag.Duration("controller-sync-timeout", 10*time.Minute, "Timeout for sync RPCs (fsync, flush, write) to ObjectFS controller")
	syncOnClose            = flag.Bool("sync-on-close", true, "Schedule non-blocking sync on file release")
	backgroundSyncInterval = flag.Duration("background-sync-interval", 30*time.Second, "Background sync interval for dirty volumes")
	debug                  = flag.Bool("debug", false, "Log every FUSE request (for test/CI gates)")
	caCertFile             = flag.String("ca-cert-file", "", "Path to CA certificate file for controller TLS verification")
	tokenFile              = flag.String("token-file", "", "Path to projected ServiceAccount token file")
	insecureFlag           = flag.Bool("insecure", false, "Connect to controller using plaintext gRPC without authentication")
)

func main() {
	klog.InitFlags(nil)
	flag.Parse()

	if *nodeID == "" {
		klog.Fatal("nodeid must be provided")
	}

	proto, addr, err := parseEndpoint(*endpoint)
	if err != nil {
		klog.Fatal(err)
	}

	if proto == "unix" {
		addr = filepath.FromSlash(addr)
		if err := os.Remove(addr); err != nil && !os.IsNotExist(err) {
			klog.Fatalf("failed to remove %s: %v", addr, err)
		}
	}

	listener, err := net.Listen(proto, addr)
	if err != nil {
		klog.Fatalf("failed to listen: %v", err)
	}

	server := grpc.NewServer()

	driver, err := newDriver(*nodeID, *controllerAddress, *cacheDir, *cacheSizeMB*1024*1024, *diskCacheSizeMB*1024*1024, *controllerRPCTimeout, *controllerSyncTimeout, *syncOnClose, *backgroundSyncInterval)
	if err != nil {
		klog.Fatalf("failed to initialize driver: %v", err)
	}
	driver.debug = *debug
	driver.caCertFile = *caCertFile
	driver.tokenFile = *tokenFile
	driver.insecure = *insecureFlag
	defer func() {
		if closeErr := driver.Close(); closeErr != nil {
			klog.V(4).Infof("driver close error: %v", closeErr)
		}
	}()

	csi.RegisterIdentityServer(server, driver)
	csi.RegisterNodeServer(server, driver)

	klog.Infof("ObjectFS CSI driver listening on %s", *endpoint)
	if err := server.Serve(listener); err != nil {
		klog.Fatalf("failed to serve: %v", err)
	}
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

type activeMount struct {
	server      *gofuse.Server
	cancelFunc  context.CancelFunc
	syncWg      *sync.WaitGroup
	renewCancel context.CancelFunc
	renewWg     *sync.WaitGroup
	volumeID    string
	mode        string
	volumeDir   string
	mountID     string
}

type objectFSDriver struct {
	csi.UnimplementedIdentityServer
	csi.UnimplementedNodeServer

	nodeID                 string
	controllerAddress      string
	cacheDir               string
	cacheBytes             int64
	diskCacheBytes         int64
	diskCache              *objectfuse.DiskBlobCache
	rpcTimeout             time.Duration
	syncTimeout            time.Duration
	syncOnClose            bool
	backgroundSyncInterval time.Duration
	debug                  bool
	caCertFile             string
	tokenFile              string
	insecure               bool

	clientConn *grpc.ClientConn
	client     pb.ObjectFSControllerClient

	mu     sync.Mutex
	mounts map[string]*activeMount
}

func newDriver(nodeID, controllerAddress, cacheDir string, cacheBytes, diskCacheBytes int64, rpcTimeout, syncTimeout time.Duration, syncOnClose bool, backgroundSyncInterval time.Duration) (*objectFSDriver, error) {
	if rpcTimeout <= 0 {
		rpcTimeout = 30 * time.Second
	}
	if syncTimeout <= 0 {
		syncTimeout = 10 * time.Minute
	}
	if cacheDir == "" {
		cacheDir = "/var/cache/objectfs"
	}
	if diskCacheBytes <= 0 {
		diskCacheBytes = 10 * 1024 * 1024 * 1024 // 10 GiB
	}
	diskCache, err := objectfuse.NewDiskBlobCache(cacheDir, diskCacheBytes)
	if err != nil {
		klog.Warningf("Failed to initialize DiskBlobCache at %s: %v", cacheDir, err)
	}
	return &objectFSDriver{
		nodeID:                 nodeID,
		controllerAddress:      controllerAddress,
		cacheDir:               cacheDir,
		cacheBytes:             cacheBytes,
		diskCacheBytes:         diskCacheBytes,
		diskCache:              diskCache,
		rpcTimeout:             rpcTimeout,
		syncTimeout:            syncTimeout,
		syncOnClose:            syncOnClose,
		backgroundSyncInterval: backgroundSyncInterval,
		insecure:               true, // Default for unit tests; overridden by main() from --insecure flag
		mounts:                 make(map[string]*activeMount),
	}, nil
}

func (d *objectFSDriver) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.diskCache != nil {
		return d.diskCache.Close()
	}
	return nil
}

func (d *objectFSDriver) getClient() (pb.ObjectFSControllerClient, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.client != nil {
		return d.client, nil
	}

	var dialOpts []grpc.DialOption
	if d.insecure {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		caFile := d.caCertFile
		if caFile == "" && grpcutil.FileExists("/etc/objectfs/tls/ca.crt") {
			caFile = "/etc/objectfs/tls/ca.crt"
		}
		if caFile == "" {
			return nil, fmt.Errorf("CA certificate file required (provide --ca-cert-file, or specify --insecure for plaintext)")
		}
		if !grpcutil.FileExists(caFile) {
			return nil, fmt.Errorf("CA certificate file not found: %s", caFile)
		}

		tokFile := d.tokenFile
		if tokFile == "" && grpcutil.FileExists("/var/run/secrets/tokens/token") {
			tokFile = "/var/run/secrets/tokens/token"
		}
		if tokFile == "" {
			return nil, fmt.Errorf("token file required (provide --token-file, or specify --insecure for plaintext)")
		}
		if !grpcutil.FileExists(tokFile) {
			return nil, fmt.Errorf("token file not found: %s", tokFile)
		}

		tlsCreds, err := grpcutil.NewClientTLSFromFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load TLS credentials from %s: %w", caFile, err)
		}
		dialOpts = append(dialOpts,
			grpc.WithTransportCredentials(tlsCreds),
			grpc.WithPerRPCCredentials(grpcutil.NewTokenFileCredentials(tokFile)),
		)
	}
	dialOpts = append(dialOpts, grpc.WithDefaultCallOptions(grpc.WaitForReady(true)))

	conn, err := grpc.NewClient(d.controllerAddress, grpcutil.DialOptions(dialOpts...)...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to controller at %s: %w", d.controllerAddress, err)
	}

	d.clientConn = conn
	d.client = pb.NewObjectFSControllerClient(conn)
	return d.client, nil
}

func (d *objectFSDriver) GetPluginInfo(ctx context.Context, req *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{
		Name:          "objectfs.labs.gke.io",
		VendorVersion: "0.0.1",
	}, nil
}

func (d *objectFSDriver) GetPluginCapabilities(ctx context.Context, req *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	return &csi.GetPluginCapabilitiesResponse{
		Capabilities: []*csi.PluginCapability{
			{
				Type: &csi.PluginCapability_Service_{
					Service: &csi.PluginCapability_Service{
						Type: csi.PluginCapability_Service_CONTROLLER_SERVICE,
					},
				},
			},
		},
	}, nil
}

func (d *objectFSDriver) Probe(ctx context.Context, req *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	return &csi.ProbeResponse{}, nil
}

func (d *objectFSDriver) NodeGetInfo(ctx context.Context, req *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{
		NodeId: d.nodeID,
	}, nil
}

func (d *objectFSDriver) NodeGetCapabilities(ctx context.Context, req *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: []*csi.NodeServiceCapability{
			{
				Type: &csi.NodeServiceCapability_Rpc{
					Rpc: &csi.NodeServiceCapability_RPC{
						Type: csi.NodeServiceCapability_RPC_GET_VOLUME_STATS,
					},
				},
			},
			{
				Type: &csi.NodeServiceCapability_Rpc{
					Rpc: &csi.NodeServiceCapability_RPC{
						Type: csi.NodeServiceCapability_RPC_SINGLE_NODE_MULTI_WRITER,
					},
				},
			},
		},
	}, nil
}

func parseWriteMode(val string) pb.WriteMode {
	switch strings.ToLower(val) {
	case "lazy", "lazywrite", "lazy_write":
		return pb.WriteMode_LAZY_WRITE
	case "eager", "eagerreplication", "eager_replication":
		return pb.WriteMode_EAGER_REPLICATION
	case "fsync", "writethrough", "write_through_fsync":
		return pb.WriteMode_WRITE_THROUGH_FSYNC
	default:
		return pb.WriteMode_WRITE_THROUGH_FSYNC
	}
}

func (d *objectFSDriver) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	targetPath := req.GetTargetPath()
	volumeID := req.GetVolumeContext()["volumeID"]
	if volumeID == "" {
		volumeID = req.GetVolumeId()
	}
	if volumeID == "" {
		return nil, fmt.Errorf("volumeID is required")
	}

	mode := strings.ToLower(req.GetVolumeContext()["mode"])
	if mode == "exclusive-local" {
		cap := req.GetVolumeCapability()
		if cap == nil || cap.GetAccessMode() == nil || cap.GetAccessMode().GetMode() != csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER {
			return nil, status.Errorf(codes.InvalidArgument, "exclusive-local mode requires SINGLE_NODE_SINGLE_WRITER access mode (ReadWriteOncePod)")
		}
		return d.publishExclusiveLocal(ctx, req, volumeID)
	}

	writeModeStr := req.GetVolumeContext()["writeMode"]
	writeMode := parseWriteMode(writeModeStr)

	klog.Infof("Publishing ObjectFS volume %s (writeMode=%v) to %s", volumeID, writeMode, targetPath)

	if err := os.MkdirAll(targetPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create target path %s: %w", targetPath, err)
	}

	client, err := d.getClient()
	if err != nil {
		return nil, err
	}

	nodeCache := objectfuse.NewNodeCache(d.cacheBytes)
	rawFS := objectfuse.NewObjectFSWithTimeouts(client, volumeID, writeMode, nodeCache, d.rpcTimeout, d.syncTimeout)
	rawFS.SetSyncOnClose(d.syncOnClose)
	rawFS.SetBackgroundSyncInterval(d.backgroundSyncInterval)

	if d.diskCache != nil {
		rawFS.SetDiskBlobCache(d.diskCache)
	}

	mountOpts := &gofuse.MountOptions{
		AllowOther: true,
		FsName:     "objectfs",
		Name:       "objectfs",
		Options:    []string{"default_permissions", "allow_other"},
		Debug:      d.debug,
	}

	server, err := gofuse.NewServer(rawFS, targetPath, mountOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to mount FUSE filesystem at %s: %w", targetPath, err)
	}
	go server.Serve()
	if err := server.WaitMount(); err != nil {
		if unmountErr := server.Unmount(); unmountErr != nil {
			klog.Warningf("failed to unmount %s after WaitMount failure: %v", targetPath, unmountErr)
		}
		return nil, fmt.Errorf("failed waiting for FUSE mount at %s: %w", targetPath, err)
	}

	watchCtx, cancel := context.WithCancel(context.Background())
	objectfuse.StartWatcher(watchCtx, client, volumeID, d.nodeID, nodeCache, rawFS)
	rawFS.StartBackgroundSync(watchCtx)

	d.mu.Lock()
	// Clean up any previous mount at this targetPath if any
	if old, ok := d.mounts[targetPath]; ok {
		if old.cancelFunc != nil {
			old.cancelFunc()
		}
		if old.server != nil {
			if unmountErr := old.server.Unmount(); unmountErr != nil {
				klog.Warningf("failed to unmount previous mount at %s: %v", targetPath, unmountErr)
			}
		}
	}
	d.mounts[targetPath] = &activeMount{
		server:     server,
		cancelFunc: cancel,
		volumeID:   volumeID,
		mode:       "fuse",
	}
	d.mu.Unlock()

	klog.Infof("Successfully mounted ObjectFS FUSE volume %s at %s", volumeID, targetPath)
	return &csi.NodePublishVolumeResponse{}, nil
}

func (d *objectFSDriver) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	targetPath := req.GetTargetPath()
	klog.Infof("Unpublishing ObjectFS volume from %s", targetPath)

	d.mu.Lock()
	mountInfo, ok := d.mounts[targetPath]
	if ok {
		delete(d.mounts, targetPath)
	}
	d.mu.Unlock()

	volumeID := req.GetVolumeId()
	if ok && mountInfo != nil && mountInfo.volumeID != "" {
		volumeID = mountInfo.volumeID
	}
	volumeDir := filepath.Join(d.cacheDir, volumeID)
	if (ok && mountInfo != nil && mountInfo.mode == "exclusive-local") || isExclusiveLocalDir(volumeDir) {
		return d.unpublishExclusiveLocal(ctx, req, volumeID, volumeDir, mountInfo)
	}

	if ok && mountInfo != nil {
		client, err := d.getClient()
		if err != nil {
			klog.Warningf("Failed to get ObjectFS client to fsync volume %s: %v", mountInfo.volumeID, err)
			return nil, fmt.Errorf("failed to get ObjectFS client to fsync volume: %w", err)
		}
		if client != nil {
			fsyncCtx, cancel := context.WithTimeout(ctx, d.syncTimeout)
			resp, err := client.Fsync(fsyncCtx, &pb.FsyncRequest{
				VolumeId: mountInfo.volumeID,
				Inode:    1,
			})
			cancel()
			if err != nil {
				klog.Warningf("Failed to fsync ObjectFS volume %s during unpublish: %v", mountInfo.volumeID, err)
				return nil, fmt.Errorf("failed to fsync ObjectFS volume %s: %w", mountInfo.volumeID, err)
			}
			if resp.GetError() != 0 {
				klog.Warningf("Fsync ObjectFS volume %s returned error code %d during unpublish", mountInfo.volumeID, resp.GetError())
				return nil, fmt.Errorf("fsync ObjectFS volume %s returned error: %d", mountInfo.volumeID, resp.GetError())
			}
		}
		if mountInfo.cancelFunc != nil {
			mountInfo.cancelFunc()
		}
		if mountInfo.server != nil {
			if err := mountInfo.server.Unmount(); err != nil {
				klog.Warningf("FUSE server unmount returned error: %v, attempting system unmount", err)
				if err := syscall.Unmount(targetPath, 0); err != nil && err != syscall.EINVAL {
					return nil, fmt.Errorf("failed to unmount %s: %w", targetPath, err)
				}
			}
		} else {
			if err := syscall.Unmount(targetPath, 0); err != nil && err != syscall.EINVAL {
				return nil, fmt.Errorf("failed to unmount %s: %w", targetPath, err)
			}
		}
	} else {
		// Fallback unmount if not tracked
		if err := syscall.Unmount(targetPath, 0); err != nil && err != syscall.EINVAL {
			return nil, fmt.Errorf("failed to unmount %s: %w", targetPath, err)
		}
	}

	klog.Infof("Successfully unpublished ObjectFS volume from %s", targetPath)
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (d *objectFSDriver) NodeGetVolumeStats(ctx context.Context, req *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	volumeID := req.GetVolumeId()
	volumePath := req.GetVolumePath()

	if volumeID == "" && volumePath != "" {
		d.mu.Lock()
		if m, ok := d.mounts[volumePath]; ok {
			volumeID = m.volumeID
		}
		d.mu.Unlock()
	}

	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id or volume_path is required")
	}

	client, err := d.getClient()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get ObjectFS client: %v", err)
	}

	statsCtx, cancel := context.WithTimeout(ctx, d.rpcTimeout)
	defer cancel()
	resp, err := client.GetVolumeStats(statsCtx, &pb.GetVolumeStatsRequest{VolumeId: volumeID})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "GetVolumeStats failed: %v", err)
	}
	if resp.GetError() != 0 {
		return nil, status.Errorf(codes.Internal, "GetVolumeStats returned error code: %d", resp.GetError())
	}

	return &csi.NodeGetVolumeStatsResponse{
		Usage: []*csi.VolumeUsage{
			{
				Total:     resp.GetTotalBytes(),
				Used:      resp.GetUsedBytes(),
				Available: resp.GetAvailableBytes(),
				Unit:      csi.VolumeUsage_BYTES,
			},
			{
				Total:     resp.GetTotalInodes(),
				Used:      resp.GetUsedInodes(),
				Available: resp.GetFreeInodes(),
				Unit:      csi.VolumeUsage_INODES,
			},
		},
	}, nil
}
