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

	"github.com/container-storage-interface/spec/lib/go/csi"
	pb "github.com/gke-labs/in-cluster-storage/pkg/api/v1alpha1"
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
	driver := &agentFSDriver{
		nodeID: *nodeID,
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

type agentFSDriver struct {
	csi.UnimplementedIdentityServer
	csi.UnimplementedNodeServer

	nodeID string

	// volumeMappings maps K8s volume ID to logical volume ID (if provided in volumeContext)
	volumeMappings sync.Map
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
		if err := os.RemoveAll(volumeDir); err != nil {
			klog.Errorf("failed to cleanup source path %s: %v", volumeDir, err)
		}
	}

	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (d *agentFSDriver) pullSnapshot(ctx context.Context, volumeID, sourcePath string) error {
	conn, err := grpc.Dial(*controllerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()

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
	conn, err := grpc.Dial(*controllerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()

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
