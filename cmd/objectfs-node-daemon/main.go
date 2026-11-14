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
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/grpcutil"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/consistency"
	objectfuse "github.com/gke-labs/in-cluster-storage/pkg/objectfs/fuse"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/fuseholder"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/volumemode"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/writemode"
	"github.com/gke-labs/in-cluster-storage/pkg/scoped"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
)

var (
	endpoint               = flag.String("endpoint", "unix:///tmp/csi.sock", "CSI endpoint")
	nodeID                 = flag.String("nodeid", "", "node id")
	controllerAddress      = flag.String("controller-address", "objectfs-controller:50051", "ObjectFS Controller address")
	clientCertDir          = flag.String("client-cert-dir", "/etc/objectfs/certs/client", "Directory containing tls.key, tls.crt, and trust/*.crt for mTLS")
	insecureFlag           = flag.Bool("insecure", false, "Connect to ObjectFS controller using plaintext gRPC without TLS (dev only)")
	cacheSizeMB            = flag.Int64("cache-size-mb", 128, "Local node in-memory cache size in MB (for dirty write buffers)")
	diskCacheSizeMB        = flag.Int64("disk-cache-size-mb", 10240, "Local node on-disk blob cache size in MB (default 10 GiB)")
	cacheDir               = flag.String("cache-dir", "/var/cache/objectfs", "Path to node-local on-disk blob cache directory")
	controllerRPCTimeout   = flag.Duration("controller-rpc-timeout", 30*time.Second, "Timeout for RPCs to ObjectFS controller")
	controllerSyncTimeout  = flag.Duration("controller-sync-timeout", 10*time.Minute, "Timeout for sync RPCs (fsync, flush, write) to ObjectFS controller")
	syncOnClose            = flag.Bool("sync-on-close", true, "Schedule non-blocking sync on file release")
	backgroundSyncInterval = flag.Duration("background-sync-interval", 30*time.Second, "Background sync interval for dirty volumes")
	fuseHolderSocket       = flag.String("fuse-holder-socket", "", "Path to FUSE holder Unix domain socket")
	debug                  = flag.Bool("debug", false, "Log every FUSE request (for test/CI gates)")
)

func main() {
	klog.InitFlags(nil)
	flag.Parse()

	if *nodeID == "" {
		klog.Fatal("nodeid must be provided")
	}
	if *fuseHolderSocket == "" {
		klog.Fatal("--fuse-holder-socket must be provided")
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

	server := grpc.NewServer(
		grpc.UnaryInterceptor(grpcutil.UnaryServerInterceptor()),
		grpc.StreamInterceptor(grpcutil.StreamServerInterceptor()),
	)

	driver, err := newDriver(*nodeID, *controllerAddress, *cacheDir, *cacheSizeMB*1024*1024, *diskCacheSizeMB*1024*1024, *controllerRPCTimeout, *controllerSyncTimeout, *syncOnClose, *backgroundSyncInterval, *clientCertDir, *insecureFlag, *fuseHolderSocket)
	if err != nil {
		klog.Fatalf("failed to initialize driver: %v", err)
	}
	driver.debug = *debug
	ctx := scoped.WithLog(context.Background(), klog.Background())
	if err := driver.reattachHeldMounts(ctx); err != nil {
		klog.Fatalf("failed to re-attach held mounts: %v", err)
	}
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

	// consistencyMode and writeMode record the published modes for tests verifying re-attach.
	consistencyMode pb.ConsistencyMode
	writeMode       pb.WriteMode
}

type fuseHolderClient interface {
	Mount(request fuseholder.MountRequest) (int, error)
	Get(target string) (int, fuseholder.Entry, error)
	List() ([]fuseholder.Entry, error)
	Unmount(target string, detach bool) error
	Close() error
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
	clientCertDir          string
	insecure               bool
	fuseHolderSocket       string
	debug                  bool

	clientConn *grpc.ClientConn
	client     pb.ObjectFSControllerClient
	dialHolder func(socketPath string) (fuseHolderClient, error)

	mu     sync.Mutex
	mounts map[string]*activeMount
}

func dialFuseHolder(socketPath string) (fuseHolderClient, error) {
	if socketPath == "" {
		return nil, errors.New("--fuse-holder-socket is required for FUSE mode")
	}
	client, err := fuseholder.Dial(socketPath)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to fuse holder at %s: %w", socketPath, err)
	}
	return client, nil
}

func newDriver(nodeID, controllerAddress, cacheDir string, cacheBytes, diskCacheBytes int64, rpcTimeout, syncTimeout time.Duration, syncOnClose bool, backgroundSyncInterval time.Duration, clientCertDir string, insecure bool, fuseHolderSocket string) (*objectFSDriver, error) {
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
		clientCertDir:          clientCertDir,
		insecure:               insecure,
		fuseHolderSocket:       fuseHolderSocket,
		dialHolder:             dialFuseHolder,
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

func (d *objectFSDriver) unmountFUSEViaHolder(targetPath string) error {
	holder, err := d.dialHolder(d.fuseHolderSocket)
	if err != nil {
		return fmt.Errorf("failed to get fuse holder client: %w", err)
	}
	defer func() {
		if closeErr := holder.Close(); closeErr != nil {
			klog.Warningf("failed to close fuse holder client: %v", closeErr)
		}
	}()

	if err := holder.Unmount(targetPath, false); err != nil && !errors.Is(err, fuseholder.ErrNotFound) {
		return fmt.Errorf("failed to unmount %s via holder: %w", targetPath, err)
	}
	return nil
}

const (
	volumeAttributeConsistency = "consistency"
	volumeAttributeWriteMode   = "writeMode"
)

func parseMountModes(values map[string]string) (pb.WriteMode, pb.ConsistencyMode, error) {
	consistencyMode, err := consistency.Parse(values[volumeAttributeConsistency])
	if err != nil {
		return pb.WriteMode_WRITE_MODE_UNSPECIFIED, pb.ConsistencyMode_CONSISTENCY_MODE_UNSPECIFIED, fmt.Errorf("invalid consistency: %w", err)
	}

	writeMode, err := writemode.Parse(values[volumeAttributeWriteMode])
	if err != nil {
		return pb.WriteMode_WRITE_MODE_UNSPECIFIED, pb.ConsistencyMode_CONSISTENCY_MODE_UNSPECIFIED, fmt.Errorf("invalid writeMode: %w", err)
	}
	if writeMode == pb.WriteMode_WRITE_MODE_UNSPECIFIED {
		writeMode = pb.WriteMode_FSYNC
	}

	return writeMode, consistencyMode, nil
}

func (d *objectFSDriver) reattachHeldMounts(ctx context.Context) error {
	if d.fuseHolderSocket == "" {
		return nil
	}
	holder, err := d.dialHolder(d.fuseHolderSocket)
	if err != nil {
		return fmt.Errorf("failed to connect to fuse holder: %w", err)
	}
	defer func() {
		if closeErr := holder.Close(); closeErr != nil {
			klog.Warningf("failed to close fuse holder client: %v", closeErr)
		}
	}()

	entries, err := holder.List()
	if err != nil {
		return fmt.Errorf("failed to list held mounts: %w", err)
	}

	for _, entry := range entries {
		if entry.Mode != volumemode.String(pb.VolumeMode_FUSE) {
			continue
		}
		mountLogger := scoped.Log(ctx).WithValues("volume", entry.VolumeID, "target", entry.Target)
		if entry.Attributes == nil {
			mountLogger.Info("skipping held mount: missing attributes")
			continue
		}
		writeMode, consistencyMode, err := parseMountModes(entry.Attributes)
		if err != nil {
			mountLogger.Error(err, "skipping held mount: invalid mount modes")
			continue
		}

		descriptor, _, err := holder.Get(entry.Target)
		if err != nil {
			mountLogger.Error(err, "failed to get descriptor for held mount")
			continue
		}
		maxWrite := entry.MaxRead
		if maxWrite <= 0 {
			maxWrite = 128 * 1024
		}
		if err := d.serveFUSE(ctx, entry.Target, entry.VolumeID, descriptor, maxWrite, writeMode, consistencyMode); err != nil {
			mountLogger.Error(err, "failed to serve reattached mount")
			continue
		}
		mountLogger.Info("reattached held FUSE mount")
	}
	return nil
}

func (d *objectFSDriver) newObjectFS(ctx context.Context, volumeID string, writeMode pb.WriteMode, consistencyMode pb.ConsistencyMode) (pb.ObjectFSControllerClient, *objectfuse.NodeCache, *objectfuse.ObjectFS, error) {
	client, err := d.getClient(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	nodeCache := objectfuse.NewNodeCache(d.cacheBytes)
	rawFS := objectfuse.NewObjectFSWithTimeouts(client, volumeID, writeMode, nodeCache, d.rpcTimeout, d.syncTimeout)
	rawFS.SetSyncOnClose(d.syncOnClose)
	rawFS.SetBackgroundSyncInterval(d.backgroundSyncInterval)
	rawFS.SetConsistencyMode(consistencyMode)

	if d.diskCache != nil {
		rawFS.SetDiskBlobCache(d.diskCache)
	}
	return client, nodeCache, rawFS, nil
}

func (d *objectFSDriver) serveFUSE(ctx context.Context, targetPath, volumeID string, descriptor int, maxWrite int, writeMode pb.WriteMode, consistencyMode pb.ConsistencyMode) error {
	client, nodeCache, rawFS, err := d.newObjectFS(ctx, volumeID, writeMode, consistencyMode)
	if err != nil {
		if closeErr := unix.Close(descriptor); closeErr != nil {
			klog.Warningf("failed to close descriptor %d: %v", descriptor, closeErr)
		}
		return err
	}

	mountOptions := &gofuse.MountOptions{
		AllowOther: true,
		FsName:     "objectfs",
		Name:       "objectfs",
		Options:    []string{"default_permissions", "allow_other"},
		MaxWrite:   maxWrite,
		Debug:      d.debug,
	}

	mountLogger := scoped.Log(context.Background()).WithValues("volume", volumeID, "target", targetPath)
	rawFS.SetLogger(mountLogger)

	mountPoint := fmt.Sprintf("/dev/fd/%d", descriptor)
	server, err := gofuse.NewServer(rawFS, mountPoint, mountOptions)
	if err != nil {
		if closeErr := unix.Close(descriptor); closeErr != nil {
			klog.Warningf("failed to close descriptor %d after NewServer error: %v", descriptor, closeErr)
		}
		return fmt.Errorf("failed to create FUSE server for %s at %s: %w", targetPath, mountPoint, err)
	}
	go server.Serve()
	if err := server.WaitMount(); err != nil {
		// WaitMount is unreachable for /dev/fd/N mountpoints because go-fuse closes
		// ready during mount before Serve runs, but the error check is kept for
		// symmetry with the fusermount path.
		return fmt.Errorf("failed waiting for FUSE mount at %s: %w", targetPath, err)
	}

	watchCtx, cancel := context.WithCancel(scoped.WithLog(context.Background(), mountLogger))
	objectfuse.StartWatcher(watchCtx, client, volumeID, d.nodeID, consistencyMode, nodeCache, rawFS)
	scoped.Log(watchCtx).Info("started volume watcher")
	rawFS.StartBackgroundSync(watchCtx)

	d.mu.Lock()
	if old, ok := d.mounts[targetPath]; ok {
		if old.cancelFunc != nil {
			old.cancelFunc()
		}
		if old.server != nil {
			if unmountErr := old.server.Unmount(); unmountErr != nil {
				mountLogger.Error(unmountErr, "failed to unmount previous mount")
			}
		}
	}
	d.mounts[targetPath] = &activeMount{
		server:          server,
		cancelFunc:      cancel,
		volumeID:        volumeID,
		mode:            volumemode.String(pb.VolumeMode_FUSE),
		volumeDir:       filepath.Join(d.cacheDir, volumeID),
		consistencyMode: consistencyMode,
		writeMode:       writeMode,
	}
	d.mu.Unlock()

	return nil
}

func (d *objectFSDriver) getClient(ctx context.Context) (pb.ObjectFSControllerClient, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.client != nil {
		return d.client, nil
	}

	var creds credentials.TransportCredentials
	if d.insecure {
		creds = insecure.NewCredentials()
	} else {
		if !grpcutil.DirExists(d.clientCertDir) {
			return nil, fmt.Errorf("client certificate directory %s does not exist; pass --insecure for plaintext local dev", d.clientCertDir)
		}
		tlsCreds, err := grpcutil.NewClientTLSFromDir(ctx, d.clientCertDir)
		if err != nil {
			return nil, fmt.Errorf("failed to load client TLS credentials from %s: %w", d.clientCertDir, err)
		}
		creds = tlsCreds
	}

	conn, err := grpc.NewClient(d.controllerAddress,
		grpcutil.DialOptions(
			grpc.WithTransportCredentials(creds),
			grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
		)...,
	)
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

func (d *objectFSDriver) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	targetPath := req.GetTargetPath()
	volumeID := req.GetVolumeContext()["volumeID"]
	if volumeID == "" {
		volumeID = req.GetVolumeId()
	}
	if volumeID == "" {
		return nil, fmt.Errorf("volumeID is required")
	}

	mode, err := volumemode.Parse(req.GetVolumeContext()["mode"])
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if mode == pb.VolumeMode_EXCLUSIVE_LOCAL {
		cap := req.GetVolumeCapability()
		if cap == nil || cap.GetAccessMode() == nil || cap.GetAccessMode().GetMode() != csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER {
			return nil, status.Errorf(codes.InvalidArgument, "exclusive-local mode requires SINGLE_NODE_SINGLE_WRITER access mode (ReadWriteOncePod)")
		}
		return d.publishExclusiveLocal(ctx, req, volumeID)
	}

	writeMode, consistencyMode, err := parseMountModes(req.GetVolumeContext())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	klog.Infof("Publishing ObjectFS volume %s (writeMode=%v, consistency=%v) to %s", volumeID, writeMode, consistencyMode, targetPath)

	d.mu.Lock()
	if active, ok := d.mounts[targetPath]; ok {
		d.mu.Unlock()
		if active.volumeID != volumeID {
			return nil, status.Errorf(codes.AlreadyExists, "target path %s is already mounted for volume %s, not %s", targetPath, active.volumeID, volumeID)
		}
		klog.Infof("Volume %s is already published at %s", volumeID, targetPath)
		return &csi.NodePublishVolumeResponse{}, nil
	}
	d.mu.Unlock()

	if err := os.MkdirAll(targetPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create target path %s: %w", targetPath, err)
	}

	maxWrite := 128 * 1024
	holder, err := d.dialHolder(d.fuseHolderSocket)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := holder.Close(); closeErr != nil {
			klog.Warningf("failed to close fuse holder client: %v", closeErr)
		}
	}()

	descriptor, heldEntry, err := holder.Get(targetPath)
	if errors.Is(err, fuseholder.ErrNotFound) {
		descriptor, err = holder.Mount(fuseholder.MountRequest{
			Entry: fuseholder.Entry{
				Target:   targetPath,
				VolumeID: volumeID,
				Mode:     volumemode.String(pb.VolumeMode_FUSE),
				FsName:   "objectfs",
				Name:     "objectfs",
				MaxRead:  maxWrite,
				Options:  []string{"default_permissions", "allow_other"},
				Attributes: map[string]string{
					volumeAttributeConsistency: consistency.String(consistencyMode),
					volumeAttributeWriteMode:   writemode.String(writeMode),
				},
			},
		})
		if err != nil {
			return nil, fmt.Errorf("failed to mount FUSE filesystem at %s via holder: %w", targetPath, err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("failed to check existing mount for %s via holder: %w", targetPath, err)
	} else if heldEntry.VolumeID != "" && heldEntry.VolumeID != volumeID {
		if closeErr := unix.Close(descriptor); closeErr != nil {
			klog.Warningf("failed to close descriptor %d for %s: %v", descriptor, targetPath, closeErr)
		}
		return nil, status.Errorf(codes.AlreadyExists, "target %s is already held for volume %s, not %s", targetPath, heldEntry.VolumeID, volumeID)
	} else if heldEntry.MaxRead > 0 {
		maxWrite = heldEntry.MaxRead
	}

	if err := d.serveFUSE(ctx, targetPath, volumeID, descriptor, maxWrite, writeMode, consistencyMode); err != nil {
		if unmountErr := holder.Unmount(targetPath, false); unmountErr != nil {
			klog.Warningf("failed to unmount %s via holder after serve error: %v", targetPath, unmountErr)
		}
		return nil, fmt.Errorf("failed to serve FUSE mount for %s: %w", targetPath, err)
	}

	klog.Infof("Successfully mounted ObjectFS FUSE volume %s at %s", volumeID, targetPath)
	return &csi.NodePublishVolumeResponse{}, nil
}

func (d *objectFSDriver) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	targetPath := req.GetTargetPath()
	klog.Infof("Unpublishing ObjectFS volume from %s", targetPath)

	d.mu.Lock()
	mountInfo, ok := d.mounts[targetPath]
	d.mu.Unlock()

	volumeID := req.GetVolumeId()
	if ok && mountInfo != nil && mountInfo.volumeID != "" {
		volumeID = mountInfo.volumeID
	}
	volumeDir := filepath.Join(d.cacheDir, volumeID)
	if (ok && mountInfo != nil && mountInfo.mode == volumemode.String(pb.VolumeMode_EXCLUSIVE_LOCAL)) || isExclusiveLocalDir(volumeDir) {
		d.mu.Lock()
		delete(d.mounts, targetPath)
		d.mu.Unlock()
		return d.unpublishExclusiveLocal(ctx, req, volumeID, volumeDir, mountInfo)
	}

	if ok && mountInfo != nil {
		client, err := d.getClient(ctx)
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
	}

	if err := d.unmountFUSEViaHolder(targetPath); err != nil {
		return nil, err
	}

	d.mu.Lock()
	delete(d.mounts, targetPath)
	d.mu.Unlock()

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

	client, err := d.getClient(ctx)
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
