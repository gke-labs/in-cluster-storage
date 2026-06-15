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
	"crypto/sha256"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/container-storage-interface/spec/lib/go/csi"
	pb "github.com/gke-labs/in-cluster-storage/pkg/api/v1alpha1"
	unix "golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/klog/v2"
)

var (
	endpoint          = flag.String("endpoint", "unix:///tmp/csi.sock", "CSI endpoint")
	nodeID            = flag.String("nodeid", "", "node id")
	storagePath       = flag.String("storage-path", "/var/lib/agentfs", "Base path for storage")
	controllerAddress = flag.String("controller-address", "agentfs-controller:50051", "AgentFS Controller address")
	lazyLoadThreshold = flag.Int64("lazy-load-threshold", -1, "Threshold in bytes. Files larger than or equal to this will be lazy loaded. Set to -1 to disable.")
)

func main() {
	klog.InitFlags(nil)
	flag.Parse()

	if *nodeID == "" {
		klog.Fatal("nodeid must be provided")
	}

	if err := os.MkdirAll(*storagePath, 0755); err != nil {
		klog.Fatalf("failed to create storage path %s: %v", *storagePath, err)
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	driver := &agentFSDriver{
		nodeID: *nodeID,
		lazyLoader: lazyLoader{
			pending:            make(map[string]*pb.FileMetadata),
			downloadOperations: make(map[string]*downloadOperation),
			mounts:             make(map[string]string),
		},
		fanotifyFd:            -1,
		rootContextCancelFunc: cancel,
	}
	defer driver.closeControllerConn()

	// Stop gRPC server gracefully when context is cancelled (including fatal lazy load errors)
	go func() {
		<-ctx.Done()
		klog.Info("Shutting down gRPC server cleanly...")
		server.GracefulStop()
	}()

	if *lazyLoadThreshold >= 0 {
		if err := driver.startLazyLoader(ctx); err != nil {
			klog.Warningf("Failed to initialize lazy loader: %v. Falling back to non-lazy-load mode (all files will be pre-downloaded).", err)
			*lazyLoadThreshold = -1
		}
	}

	csi.RegisterIdentityServer(server, driver)
	csi.RegisterNodeServer(server, driver)

	klog.Infof("Listening on %s", *endpoint)
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

type downloadOperation struct {
	sync.Mutex
	path   string
	meta   *pb.FileMetadata
	driver *agentFSDriver
	done   bool
	err    error
	waitCh chan struct{}
}

func newDownloadOperation(path string, meta *pb.FileMetadata, driver *agentFSDriver) *downloadOperation {
	return &downloadOperation{
		path:   path,
		meta:   meta,
		driver: driver,
		waitCh: make(chan struct{}),
	}
}

func (op *downloadOperation) Download(ctx context.Context) error {
	op.Lock()
	if op.done {
		err := op.err
		op.Unlock()
		return err
	}
	op.Unlock()

	err := op.driver.lazyDownloadFile(ctx, op.path, op.meta)

	op.Lock()
	op.err = err
	op.done = true
	op.Unlock()

	close(op.waitCh)
	return err
}

type lazyLoader struct {
	pendingMu sync.RWMutex
	pending   map[string]*pb.FileMetadata // absolute path on disk -> metadata

	downloadMu         sync.Mutex
	downloadOperations map[string]*downloadOperation

	mountsMu sync.RWMutex
	mounts   map[string]string // targetPath -> volumeDir
}

type agentFSDriver struct {
	csi.UnimplementedIdentityServer
	csi.UnimplementedNodeServer

	nodeID string

	// volumeMappings maps K8s volume ID to logical volume ID (if provided in volumeContext)
	volumeMappings sync.Map

	lazyLoader lazyLoader
	fanotifyFd int

	// Clean shutdown / context integration
	rootContextCancelFunc context.CancelFunc

	// Controller connection cache
	controllerConn     *grpc.ClientConn
	controllerConnLock sync.Mutex
}

func (d *agentFSDriver) getControllerConn() (*grpc.ClientConn, error) {
	d.controllerConnLock.Lock()
	defer d.controllerConnLock.Unlock()

	if d.controllerConn != nil {
		return d.controllerConn, nil
	}

	conn, err := grpc.Dial(*controllerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	d.controllerConn = conn
	return conn, nil
}

func (d *agentFSDriver) closeControllerConn() {
	d.controllerConnLock.Lock()
	defer d.controllerConnLock.Unlock()
	if d.controllerConn != nil {
		d.controllerConn.Close()
		d.controllerConn = nil
	}
}

func (d *agentFSDriver) GetPluginInfo(ctx context.Context, req *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{
		Name:          "agentfs.labs.gke.io",
		VendorVersion: "0.0.1",
	}, nil
}

func (d *agentFSDriver) GetPluginCapabilities(ctx context.Context, req *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
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

func (d *agentFSDriver) Probe(ctx context.Context, req *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	return &csi.ProbeResponse{}, nil
}

func (d *agentFSDriver) NodeGetInfo(ctx context.Context, req *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{
		NodeId: d.nodeID,
	}, nil
}

func (d *agentFSDriver) NodeGetCapabilities(ctx context.Context, req *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: []*csi.NodeServiceCapability{
			{
				Type: &csi.NodeServiceCapability_Rpc{
					Rpc: &csi.NodeServiceCapability_RPC{
						Type: csi.NodeServiceCapability_RPC_GET_VOLUME_STATS,
					},
				},
			},
		},
	}, nil
}

func (d *agentFSDriver) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	k8sVolumeID := req.GetVolumeId()
	logicalVolumeID := k8sVolumeID
	if v, ok := req.GetVolumeContext()["volumeID"]; ok {
		logicalVolumeID = v
	}
	d.volumeMappings.Store(k8sVolumeID, logicalVolumeID)

	targetPath := req.GetTargetPath()
	klog.Infof("Publishing volume %s (logical: %s) to %s", k8sVolumeID, logicalVolumeID, targetPath)

	volumeDir := filepath.Join(*storagePath, k8sVolumeID)
	lowerPath := filepath.Join(volumeDir, "lower")
	upperPath := filepath.Join(volumeDir, "upper")
	workPath := filepath.Join(volumeDir, "work")

	if err := os.MkdirAll(lowerPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create lower path %s: %v", lowerPath, err)
	}
	if err := os.MkdirAll(upperPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create upper path %s: %v", upperPath, err)
	}
	if err := os.MkdirAll(workPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create work path %s: %v", workPath, err)
	}

	// Pull snapshot from controller to lower directory
	if err := d.pullSnapshot(ctx, logicalVolumeID, lowerPath); err != nil {
		klog.Errorf("failed to pull snapshot for volume %s (logical: %s): %v", k8sVolumeID, logicalVolumeID, err)
	}

	if err := os.MkdirAll(targetPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create target path %s: %v", targetPath, err)
	}

	// Check if already mounted
	notMnt, err := isNotMountPoint(targetPath)
	if err != nil {
		return nil, fmt.Errorf("failed to check if %s is a mount point: %v", targetPath, err)
	}
	if !notMnt {
		klog.Infof("Volume %s already mounted at %s", k8sVolumeID, targetPath)
		return &csi.NodePublishVolumeResponse{}, nil
	}

	// Mount overlayfs to target path
	opts := fmt.Sprintf("lowerdir=%s,upperdir=%s,workdir=%s", lowerPath, upperPath, workPath)
	if err := syscall.Mount("overlay", targetPath, "overlay", 0, opts); err != nil {
		return nil, fmt.Errorf("failed to mount overlayfs to %s: %v", targetPath, err)
	}

	var fanotifyActive bool
	if d.fanotifyFd >= 0 {
		err = unix.FanotifyMark(d.fanotifyFd, uint(unix.FAN_MARK_ADD|unix.FAN_MARK_FILESYSTEM), uint64(unix.FAN_OPEN_PERM), unix.AT_FDCWD, targetPath)
		if err != nil {
			klog.Errorf("failed to mark fanotify on targetPath %s: %v", targetPath, err)
		} else {
			klog.Infof("Successfully marked fanotify on targetPath %s. Running self-test...", targetPath)

			// 1. Create a temporary self-test file metadata
			testFileRel := ".fanotify-self-test.txt"
			testFileLower := filepath.Join(lowerPath, testFileRel)
			testFileMerged := filepath.Join(targetPath, testFileRel)

			// Create dummy file
			if err := os.WriteFile(testFileLower, []byte("self-test"), 0644); err == nil {
				d.lazyLoader.pendingMu.Lock()
				d.lazyLoader.pending[testFileLower] = &pb.FileMetadata{
					Path:   testFileRel,
					Size:   9,
					Sha256: "self-test-sha",
				}
				d.lazyLoader.pendingMu.Unlock()

				d.lazyLoader.mountsMu.Lock()
				d.lazyLoader.mounts[targetPath] = volumeDir
				d.lazyLoader.mountsMu.Unlock()

				// 2. Attempt to open the merged file
				go func() {
					f, err := os.Open(testFileMerged)
					if err == nil {
						f.Close()
					}
				}()

				// 3. Wait up to 100ms for fanotify to intercept and remove the file from pending
				success := false
				for i := 0; i < 10; i++ {
					time.Sleep(10 * time.Millisecond)
					d.lazyLoader.pendingMu.RLock()
					_, pending := d.lazyLoader.pending[testFileLower]
					d.lazyLoader.pendingMu.RUnlock()
					if !pending {
						success = true
						break
					}
				}

				if success {
					klog.Infof("Fanotify self-test passed for %s!", targetPath)
					fanotifyActive = true
				} else {
					klog.Warningf("Fanotify self-test failed (no events received) on %s. Falling back to full pre-download.", targetPath)
					d.lazyLoader.mountsMu.Lock()
					delete(d.lazyLoader.mounts, targetPath)
					d.lazyLoader.mountsMu.Unlock()

					d.lazyLoader.pendingMu.Lock()
					delete(d.lazyLoader.pending, testFileLower)
					d.lazyLoader.pendingMu.Unlock()

					if removeErr := unix.FanotifyMark(d.fanotifyFd, uint(unix.FAN_MARK_REMOVE|unix.FAN_MARK_FILESYSTEM), uint64(unix.FAN_OPEN_PERM), unix.AT_FDCWD, targetPath); removeErr != nil {
						klog.Warningf("failed to remove fanotify mark: %v", removeErr)
					}
				}
				os.Remove(testFileLower)
			}
		}
	}

	if !fanotifyActive {
		d.downloadAllPending(ctx, volumeDir)
	}

	return &csi.NodePublishVolumeResponse{}, nil
}

func (d *agentFSDriver) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	k8sVolumeID := req.GetVolumeId()
	logicalVolumeID := k8sVolumeID
	if v, ok := d.volumeMappings.Load(k8sVolumeID); ok {
		logicalVolumeID = v.(string)
	}
	d.volumeMappings.Delete(k8sVolumeID)

	targetPath := req.GetTargetPath()
	klog.Infof("Unpublishing volume %s (logical: %s) from %s", k8sVolumeID, logicalVolumeID, targetPath)

	// Clean up lazyLoader mounts mapping
	d.lazyLoader.mountsMu.Lock()
	delete(d.lazyLoader.mounts, targetPath)
	d.lazyLoader.mountsMu.Unlock()

	// Try to unmount the target path. Ignore if not a mount point.
	if err := syscall.Unmount(targetPath, 0); err != nil {
		if err != syscall.EINVAL {
			klog.Warningf("Failed to unmount %s: %v", targetPath, err)
		} else {
			klog.Infof("Volume %s not mounted at %s (or already unmounted)", k8sVolumeID, targetPath)
		}
	}

	// Push snapshot to controller
	volumeDir := filepath.Join(*storagePath, k8sVolumeID)
	if err := d.pushSnapshot(ctx, logicalVolumeID, volumeDir); err != nil {
		klog.Errorf("failed to push snapshot for volume %s (logical: %s): %v", k8sVolumeID, logicalVolumeID, err)
	} else {
		// Clean up lazyLoader pending entries for this volume
		d.lazyLoader.pendingMu.Lock()
		for path := range d.lazyLoader.pending {
			if strings.HasPrefix(path, volumeDir) {
				delete(d.lazyLoader.pending, path)
			}
		}
		d.lazyLoader.pendingMu.Unlock()

		d.lazyLoader.downloadMu.Lock()
		for path := range d.lazyLoader.downloadOperations {
			if strings.HasPrefix(path, volumeDir) {
				delete(d.lazyLoader.downloadOperations, path)
			}
		}
		d.lazyLoader.downloadMu.Unlock()

		if err := os.RemoveAll(volumeDir); err != nil {
			klog.Errorf("failed to cleanup source path %s: %v", volumeDir, err)
		}
	}

	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (d *agentFSDriver) pullSnapshot(ctx context.Context, volumeID, sourcePath string) error {
	conn, err := d.getControllerConn()
	if err != nil {
		return err
	}

	client := pb.NewAgentFSControllerClient(conn)
	resp, err := client.GetLatestSnapshot(ctx, &pb.GetLatestSnapshotRequest{VolumeId: volumeID})
	if err != nil {
		return err
	}

	if resp.Snapshot == nil {
		klog.Infof("No snapshot found for volume %s", volumeID)
		return nil
	}

	for _, file := range resp.Snapshot.Files {
		targetFile := filepath.Join(sourcePath, file.Path)

		// Ensure directory exists
		if err := os.MkdirAll(filepath.Dir(targetFile), 0755); err != nil {
			return err
		}

		// Check if file exists and has correct sha256
		if _, err := os.Stat(targetFile); err == nil {
			sha, _ := calculateSHA256(targetFile)
			if sha == file.Sha256 {
				continue
			}
		}

		// Check if we should lazy load this file
		if *lazyLoadThreshold >= 0 && file.Size >= *lazyLoadThreshold {
			klog.Infof("Registering lazy load placeholder for %s (size %d)", targetFile, file.Size)

			// Create a sparse file of the original size so metadata (like size) is correct
			f, err := os.OpenFile(targetFile, os.O_CREATE|os.O_WRONLY, os.FileMode(file.Mode))
			if err != nil {
				return fmt.Errorf("failed to create placeholder for %s: %v", targetFile, err)
			}
			if err := f.Truncate(file.Size); err != nil {
				f.Close()
				return fmt.Errorf("failed to truncate placeholder for %s: %v", targetFile, err)
			}
			f.Close()

			// Set mod time so metadata matches
			if err := os.Chtimes(targetFile, file.ModTime.AsTime(), file.ModTime.AsTime()); err != nil {
				klog.Warningf("failed to set times for placeholder %s: %v", targetFile, err)
			}

			// Register in lazyLoader
			d.lazyLoader.pendingMu.Lock()
			d.lazyLoader.pending[targetFile] = file
			d.lazyLoader.pendingMu.Unlock()

			continue
		}

		// Download blob
		if err := d.downloadBlob(ctx, client, file.Sha256, targetFile); err != nil {
			return fmt.Errorf("failed to download blob %s: %v", file.Sha256, err)
		}

		// Set mode and mod time
		if err := os.Chmod(targetFile, os.FileMode(file.Mode)); err != nil {
			klog.Warningf("failed to set mode for %s: %v", targetFile, err)
		}
		if err := os.Chtimes(targetFile, file.ModTime.AsTime(), file.ModTime.AsTime()); err != nil {
			klog.Warningf("failed to set times for %s: %v", targetFile, err)
		}
	}

	return nil
}

func (d *agentFSDriver) pushSnapshot(ctx context.Context, volumeID, sourcePath string) error {
	conn, err := d.getControllerConn()
	if err != nil {
		return err
	}

	client := pb.NewAgentFSControllerClient(conn)

	// Fetch latest snapshot from the controller to use as base
	resp, err := client.GetLatestSnapshot(ctx, &pb.GetLatestSnapshotRequest{VolumeId: volumeID})
	if err != nil {
		return fmt.Errorf("failed to get latest snapshot: %v", err)
	}

	filesMap := make(map[string]*pb.FileMetadata)
	if resp != nil && resp.Snapshot != nil {
		for _, file := range resp.Snapshot.Files {
			filesMap[file.Path] = file
		}
	}

	upperPath := filepath.Join(sourcePath, "upper")

	err = filepath.Walk(upperPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		relPath, err := filepath.Rel(upperPath, path)
		if err != nil {
			return err
		}
		if relPath == "." {
			return nil
		}

		// Check if it is a whiteout (deleted file/directory)
		sys, ok := info.Sys().(*syscall.Stat_t)
		isWhiteout := ok && (info.Mode()&os.ModeCharDevice != 0) && (sys.Rdev == 0)

		if isWhiteout {
			// Delete this file and any of its descendants (if it was a directory)
			delete(filesMap, relPath)
			prefix := relPath + "/"
			for k := range filesMap {
				if strings.HasPrefix(k, prefix) {
					delete(filesMap, k)
				}
			}
			return nil
		}

		if info.IsDir() {
			return nil
		}

		sha, err := calculateSHA256(path)
		if err != nil {
			return err
		}

		filesMap[relPath] = &pb.FileMetadata{
			Path:    relPath,
			Mode:    uint32(info.Mode()),
			Size:    info.Size(),
			ModTime: timestamppb.New(info.ModTime()),
			Sha256:  sha,
		}

		// Check if controller has the blob
		hasResp, err := client.HasBlob(ctx, &pb.HasBlobRequest{Sha256: sha})
		if err != nil {
			return err
		}

		if !hasResp.Exists {
			if err := d.uploadBlob(ctx, client, sha, path); err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		return err
	}

	// Rebuild the snapshot files list
	snapshot := &pb.SnapshotMetadata{}
	for _, file := range filesMap {
		snapshot.Files = append(snapshot.Files, file)
	}

	_, err = client.UploadSnapshot(ctx, &pb.UploadSnapshotRequest{
		VolumeId: volumeID,
		Snapshot: snapshot,
	})
	return err
}

func (d *agentFSDriver) downloadBlob(ctx context.Context, client pb.AgentFSControllerClient, sha, targetPath string) error {
	stream, err := client.DownloadBlob(ctx, &pb.DownloadBlobRequest{Sha256: sha})
	if err != nil {
		return err
	}

	f, err := os.Create(targetPath)
	if err != nil {
		return err
	}
	defer f.Close()

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if _, err := f.Write(resp.Content); err != nil {
			return err
		}
	}
	return nil
}

func (d *agentFSDriver) uploadBlob(ctx context.Context, client pb.AgentFSControllerClient, sha, sourceFile string) error {
	stream, err := client.UploadBlob(ctx)
	if err != nil {
		return err
	}

	if err := stream.Send(&pb.UploadBlobRequest{
		Data: &pb.UploadBlobRequest_Sha256{Sha256: sha},
	}); err != nil {
		return err
	}

	f, err := os.Open(sourceFile)
	if err != nil {
		return err
	}
	defer f.Close()

	buffer := make([]byte, 1024*1024) // 1MB buffer
	for {
		n, err := f.Read(buffer)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if err := stream.Send(&pb.UploadBlobRequest{
			Data: &pb.UploadBlobRequest_Content{Content: buffer[:n]},
		}); err != nil {
			return err
		}
	}

	_, err = stream.CloseAndRecv()
	return err
}

func calculateSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

// isNotMountPoint checks if a directory is NOT a mount point.
// Very simple implementation for now.
func isNotMountPoint(path string) (bool, error) {
	stat, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return true, nil
		}
		return false, err
	}

	parentPath := filepath.Dir(path)
	parentStat, err := os.Stat(parentPath)
	if err != nil {
		return false, err
	}

	return stat.Sys().(*syscall.Stat_t).Dev == parentStat.Sys().(*syscall.Stat_t).Dev, nil
}

func (d *agentFSDriver) startLazyLoader(ctx context.Context) error {
	if *lazyLoadThreshold < 0 {
		return nil
	}

	fd, err := unix.FanotifyInit(uint(unix.FAN_CLASS_PRE_CONTENT|unix.FAN_CLOEXEC), uint(unix.O_RDONLY))
	if err != nil {
		return fmt.Errorf("failed to initialize fanotify (is CAP_SYS_ADMIN missing?): %v", err)
	}
	d.fanotifyFd = fd

	klog.Infof("Successfully initialized fanotify lazy loader with threshold %d bytes", *lazyLoadThreshold)

	go func() {
		if err := d.fanotifyLoop(ctx); err != nil {
			klog.Errorf("fanotify loop terminated with error: %v", err)
			d.shutdownCleanly()
		}
	}()
	return nil
}

func (d *agentFSDriver) shutdownCleanly() {
	klog.Errorf("Cleanly shutting down agentfs-node-daemon due to a fatal fanotify/lazy-loading error")
	d.rootContextCancelFunc()
}

func (d *agentFSDriver) fanotifyLoop(ctx context.Context) error {
	buf := make([]byte, 4096)
	myPid := int32(os.Getpid())
	klog.Infof("Starting fanotify loop (my PID: %d)", myPid)

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		n, err := unix.Read(d.fanotifyFd, buf)
		if n > 0 {
			var offset int
			sizeofMetadata := int(unsafe.Sizeof(unix.FanotifyEventMetadata{}))
			for offset+sizeofMetadata <= n {
				metadata := (*unix.FanotifyEventMetadata)(unsafe.Pointer(&buf[offset]))
				eventLen := int(metadata.Event_len)

				if eventLen < sizeofMetadata {
					return fmt.Errorf("corrupt fanotify event received: eventLen %d is smaller than sizeofMetadata %d", eventLen, sizeofMetadata)
				}

				if (metadata.Mask & uint64(unix.FAN_OPEN_PERM)) == 0 {
					if metadata.Fd != int32(unix.FAN_NOFD) {
						if closeErr := unix.Close(int(metadata.Fd)); closeErr != nil {
							klog.Errorf("failed to close unexpected event fd %d: %v", metadata.Fd, closeErr)
						}
					}
					return fmt.Errorf("received unexpected fanotify event mask: 0x%x (expected FAN_OPEN_PERM bit to be set)", metadata.Mask)
				}

				eventFd := int(metadata.Fd)

				// Skip if event caused by our own daemon process to avoid deadlock
				if metadata.Pid == myPid {
					d.sendFanotifyResponse(eventFd, unix.FAN_ALLOW)
					if closeErr := unix.Close(eventFd); closeErr != nil {
						klog.Errorf("failed to close self-event fd %d: %v", eventFd, closeErr)
					}
				} else {
					// Handle the event concurrently to allow other processes to open files in parallel
					go d.handleFanotifyEvent(ctx, metadata.Pid, eventFd)
				}

				offset += eventLen
			}
		}

		if err != nil {
			if err == unix.EINTR {
				continue
			}
			return fmt.Errorf("fanotify read error: %v", err)
		}
	}
}

func (d *agentFSDriver) handleFanotifyEvent(ctx context.Context, pid int32, eventFd int) {
	defer func() {
		if closeErr := unix.Close(eventFd); closeErr != nil {
			klog.Errorf("failed to close event fd %d: %v", eventFd, closeErr)
		}
	}()

	// Resolving symbolic links under /proc/self/fd is the standard, documented method for
	// fanotify applications to map an open event file descriptor back to its full path.
	// Note: We cannot use FAN_REPORT_FID here because:
	// 1. Combining FAN_REPORT_FID with permission classes (FAN_CLASS_PRE_CONTENT) requires Linux 6.13+.
	//    On older kernels (including 6.12 and below), FanotifyInit will fail with EINVAL.
	// 2. FAN_REPORT_FID reports file handles instead of file descriptors, which do not contain
	//    paths. Reconstructing paths from handles would be highly complex and negate efficiency benefits,
	//    whereas reading /proc/self/fd/<fd> leverages the file descriptor already opened by the kernel.
	path, err := os.Readlink(fmt.Sprintf("/proc/self/fd/%d", eventFd))
	if err != nil {
		klog.Warningf("failed to resolve path for event fd %d: %v. Failing closed.", eventFd, err)
		d.sendFanotifyResponse(eventFd, unix.FAN_DENY)
		return
	}

	// Since we watch the OverlayFS mount directly, 'path' will be a path under the merged mount
	// (e.g. /var/lib/kubelet/pods/.../mount/test.txt). We map this back to its underlying 'lower'
	// file path (e.g. /var/lib/agentfs/<volumeID>/lower/test.txt) where we write the actual data.
	var matchedTarget, matchedVolDir string
	d.lazyLoader.mountsMu.RLock()
	for target, volDir := range d.lazyLoader.mounts {
		if strings.HasPrefix(path, target) {
			if len(target) > len(matchedTarget) {
				matchedTarget = target
				matchedVolDir = volDir
			}
		}
	}
	d.lazyLoader.mountsMu.RUnlock()

	targetFile := path
	if matchedTarget != "" {
		relPath := strings.TrimPrefix(path[len(matchedTarget):], "/")
		targetFile = filepath.Join(matchedVolDir, "lower", relPath)
	}

	// Check if path is in our pending list
	d.lazyLoader.pendingMu.RLock()
	meta, exists := d.lazyLoader.pending[targetFile]
	d.lazyLoader.pendingMu.RUnlock()

	if !exists {
		// Not a pending lazy-loaded file, allow immediately
		d.sendFanotifyResponse(eventFd, unix.FAN_ALLOW)
		return
	}

	d.lazyLoader.downloadMu.Lock()
	op, found := d.lazyLoader.downloadOperations[targetFile]
	var isInitiator bool
	if !found {
		op = newDownloadOperation(targetFile, meta, d)
		d.lazyLoader.downloadOperations[targetFile] = op
		isInitiator = true
	}
	d.lazyLoader.downloadMu.Unlock()

	klog.Infof("Lazy loading file requested by PID %d: %s (resolves to %s) (initiator: %t)", pid, path, targetFile, isInitiator)

	if isInitiator {
		err := op.Download(ctx)
		if err != nil {
			klog.Errorf("Failed to lazy download file %s: %v", targetFile, err)
			d.sendFanotifyResponse(eventFd, unix.FAN_DENY)
		} else {
			// Success! Remove from pending list
			d.lazyLoader.pendingMu.Lock()
			delete(d.lazyLoader.pending, targetFile)
			d.lazyLoader.pendingMu.Unlock()

			d.sendFanotifyResponse(eventFd, unix.FAN_ALLOW)
		}

		d.lazyLoader.downloadMu.Lock()
		delete(d.lazyLoader.downloadOperations, targetFile)
		d.lazyLoader.downloadMu.Unlock()
	} else {
		// Wait for the download to complete, then fallthrough to evaluate success status
		select {
		case <-op.waitCh:
		case <-ctx.Done():
			d.sendFanotifyResponse(eventFd, unix.FAN_DENY)
			return
		}

		op.Lock()
		opErr := op.err
		op.Unlock()

		if opErr != nil {
			d.sendFanotifyResponse(eventFd, unix.FAN_DENY)
		} else {
			d.sendFanotifyResponse(eventFd, unix.FAN_ALLOW)
		}
	}
}

func (d *agentFSDriver) sendFanotifyResponse(fd int, response uint32) {
	resp := unix.FanotifyResponse{
		Fd:       int32(fd),
		Response: response,
	}
	var buf [8]byte // sizeof(FanotifyResponse) is 8 bytes
	*(*unix.FanotifyResponse)(unsafe.Pointer(&buf[0])) = resp
	if _, err := unix.Write(d.fanotifyFd, buf[:]); err != nil {
		klog.Errorf("failed to write fanotify response: %v", err)
		d.shutdownCleanly()
	}
}

func (d *agentFSDriver) lazyDownloadFile(ctx context.Context, path string, meta *pb.FileMetadata) error {
	if meta.Sha256 == "self-test-sha" {
		return nil // succeed immediately for the fanotify self-test
	}

	conn, err := d.getControllerConn()
	if err != nil {
		return err
	}

	client := pb.NewAgentFSControllerClient(conn)

	// Download blob
	if err := d.downloadBlob(ctx, client, meta.Sha256, path); err != nil {
		return fmt.Errorf("failed to download blob %s: %v", meta.Sha256, err)
	}

	// Set mode and mod time
	if err := os.Chmod(path, os.FileMode(meta.Mode)); err != nil {
		klog.Warningf("failed to set mode for %s: %v", path, err)
	}
	if err := os.Chtimes(path, meta.ModTime.AsTime(), meta.ModTime.AsTime()); err != nil {
		klog.Warningf("failed to set times for %s: %v", path, err)
	}

	return nil
}

func (d *agentFSDriver) downloadAllPending(ctx context.Context, volumeDir string) {
	d.lazyLoader.pendingMu.Lock()
	var toDownload []string
	for path := range d.lazyLoader.pending {
		if strings.HasPrefix(path, volumeDir) {
			toDownload = append(toDownload, path)
		}
	}
	d.lazyLoader.pendingMu.Unlock()

	if len(toDownload) == 0 {
		return
	}

	klog.Infof("Downloading %d files due to fanotify fallback for %s", len(toDownload), volumeDir)
	for _, path := range toDownload {
		d.lazyLoader.pendingMu.RLock()
		meta, exists := d.lazyLoader.pending[path]
		d.lazyLoader.pendingMu.RUnlock()

		if exists {
			if err := d.lazyDownloadFile(ctx, path, meta); err != nil {
				klog.Errorf("Fallback download failed for %s: %v", path, err)
			} else {
				d.lazyLoader.pendingMu.Lock()
				delete(d.lazyLoader.pending, path)
				d.lazyLoader.pendingMu.Unlock()
			}
		}
	}
}
