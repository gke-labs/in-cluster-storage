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
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"github.com/container-storage-interface/spec/lib/go/csi"
	pb "github.com/gke-labs/in-cluster-storage/pkg/api/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/cas/pkg/cas"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"k8s.io/klog/v2"
)

var (
	endpoint          = flag.String("endpoint", "unix:///tmp/csi.sock", "CSI endpoint")
	nodeID            = flag.String("nodeid", "", "node id")
	storagePath       = flag.String("storage-path", "/var/lib/cas", "Base path for storage")
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

	downloader := &controllerDownloader{
		controllerAddress: *controllerAddress,
	}

	driver := &casCSIDriver{
		nodeID:     *nodeID,
		downloader: downloader,
		casServers: make(map[string]*cas.Server),
	}

	csi.RegisterIdentityServer(server, driver)
	csi.RegisterNodeServer(server, driver)

	klog.Infof("CAS CSI driver listening on %s", *endpoint)
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

type controllerDownloader struct {
	controllerAddress string
	conn              *grpc.ClientConn
	mu                sync.Mutex
}

func (d *controllerDownloader) DownloadBlob(ctx context.Context, sha string, destPath string) error {
	d.mu.Lock()
	if d.conn == nil {
		conn, err := grpc.Dial(d.controllerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			d.mu.Unlock()
			return err
		}
		d.conn = conn
	}
	d.mu.Unlock()

	client := pb.NewAgentFSControllerClient(d.conn)
	stream, err := client.DownloadBlob(ctx, &pb.DownloadBlobRequest{Sha256: sha})
	if err != nil {
		return err
	}

	f, err := os.Create(destPath)
	if err != nil {
		return err
	}
	defer f.Close()

	buffer := make([]byte, 1024*1024)
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		// Write in chunks
		if _, err := f.Write(resp.Content); err != nil {
			return err
		}
		_ = buffer
	}
	return nil
}

type casCSIDriver struct {
	csi.UnimplementedIdentityServer
	csi.UnimplementedNodeServer

	nodeID     string
	downloader *controllerDownloader

	casServersMu sync.Mutex
	casServers   map[string]*cas.Server
}

func (d *casCSIDriver) GetPluginInfo(ctx context.Context, req *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{
		Name:          "cas.labs.gke.io",
		VendorVersion: "0.0.1",
	}, nil
}

func (d *casCSIDriver) GetPluginCapabilities(ctx context.Context, req *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
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

func (d *casCSIDriver) Probe(ctx context.Context, req *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	return &csi.ProbeResponse{}, nil
}

func (d *casCSIDriver) NodeGetInfo(ctx context.Context, req *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{
		NodeId: d.nodeID,
	}, nil
}

func (d *casCSIDriver) NodeGetCapabilities(ctx context.Context, req *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
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

func (d *casCSIDriver) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	targetPath := req.GetTargetPath()
	klog.Infof("Publishing CAS volume to %s", targetPath)

	if err := os.MkdirAll(targetPath, 0755); err != nil {
		klog.Errorf("Failed to create target path %s: %v", targetPath, err)
		return nil, fmt.Errorf("failed to create target path %s: %v", targetPath, err)
	}

	// Check if already mounted
	notMnt, err := isNotMountPoint(targetPath)
	if err != nil {
		klog.Errorf("Failed to check if %s is a mount point: %v", targetPath, err)
		return nil, fmt.Errorf("failed to check if %s is a mount point: %v", targetPath, err)
	}

	if notMnt {
		// Mount tmpfs filesystem to targetPath
		klog.Infof("Mounting tmpfs on %s", targetPath)
		if err := syscall.Mount("tmpfs", targetPath, "tmpfs", 0, ""); err != nil {
			klog.Errorf("Failed to mount tmpfs to %s: %v", targetPath, err)
			return nil, fmt.Errorf("failed to mount tmpfs to %s: %v", targetPath, err)
		}
	}

	// Start CAS server under targetPath/.in-cluster-storage/api
	casDir := filepath.Join(targetPath, ".in-cluster-storage")
	if err := os.MkdirAll(casDir, 0777); err != nil {
		klog.Errorf("Failed to create CAS socket directory %s: %v", casDir, err)
		return nil, fmt.Errorf("failed to create CAS socket directory: %v", err)
	}
	if err := os.Chmod(casDir, 0777); err != nil {
		klog.Warningf("failed to chmod CAS socket directory %s: %v", casDir, err)
	}

	casSocketPath := filepath.Join(casDir, "api")
	_ = os.Remove(casSocketPath)

	klog.Infof("Starting CAS server on %s", casSocketPath)
	casSrv, err := cas.StartServer(casSocketPath, *storagePath, d.downloader)
	if err != nil {
		klog.Errorf("Failed to start CAS server at %s: %v", casSocketPath, err)
		return nil, fmt.Errorf("failed to start CAS server at %s: %v", casSocketPath, err)
	}

	d.casServersMu.Lock()
	d.casServers[targetPath] = casSrv
	d.casServersMu.Unlock()

	klog.Infof("Successfully published CAS volume to %s", targetPath)
	return &csi.NodePublishVolumeResponse{}, nil
}

func (d *casCSIDriver) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	targetPath := req.GetTargetPath()
	klog.Infof("Unpublishing CAS volume from %s", targetPath)

	// Stop and remove the CAS server
	d.casServersMu.Lock()
	if srv, exists := d.casServers[targetPath]; exists {
		srv.Stop()
		delete(d.casServers, targetPath)
	}
	d.casServersMu.Unlock()

	// Unmount target path
	if err := syscall.Unmount(targetPath, 0); err != nil {
		if err != syscall.EINVAL {
			klog.Errorf("Failed to unmount target path %s: %v", targetPath, err)
			return nil, fmt.Errorf("failed to unmount target path %s: %v", targetPath, err)
		}
		klog.Infof("CAS volume not mounted at %s (or already unmounted)", targetPath)
	}

	klog.Infof("Successfully unpublished CAS volume from %s", targetPath)
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

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
