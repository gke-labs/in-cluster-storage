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

package controller

import (
	"context"

	"github.com/container-storage-interface/spec/lib/go/csi"
	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/consistency"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/metadatadurability"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CSIController implements the CSI IdentityServer and ControllerServer interfaces.
type CSIController struct {
	csi.UnimplementedIdentityServer
	csi.UnimplementedControllerServer
	server *Server
}

func NewCSIController(server *Server) *CSIController {
	return &CSIController{
		server: server,
	}
}

// Identity Server Implementation

func (c *CSIController) GetPluginInfo(ctx context.Context, req *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{
		Name:          "objectfs.labs.gke.io",
		VendorVersion: "0.0.1",
	}, nil
}

func (c *CSIController) GetPluginCapabilities(ctx context.Context, req *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
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

func (c *CSIController) Probe(ctx context.Context, req *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	return &csi.ProbeResponse{}, nil
}

// Controller Server Implementation

func (c *CSIController) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume name is required")
	}
	if req.GetVolumeCapabilities() == nil || len(req.GetVolumeCapabilities()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume capabilities are required")
	}

	volumeID := req.GetName()
	volParams := make(map[string]string)
	for k, v := range req.GetParameters() {
		volParams[k] = v
	}
	volParams["volumeID"] = volumeID

	var reqBytes int64 = 10 * 1024 * 1024 * 1024 // default 10GiB
	if req.GetCapacityRange() != nil && req.GetCapacityRange().GetRequiredBytes() > 0 {
		reqBytes = req.GetCapacityRange().GetRequiredBytes()
	}

	// Initialize the volume in the server
	vol, err := c.server.getOrCreateVolume(volumeID)
	if err != nil {
		if _, ok := status.FromError(err); ok {
			return nil, err
		}
		return nil, status.Errorf(codes.Internal, "failed to get volume: %v", err)
	}

	consistencyMode, err := consistency.Parse(req.GetParameters()["consistency"])
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if consistencyMode != pb.ConsistencyMode_CONSISTENCY_MODE_UNSPECIFIED {
		if err := vol.PersistConsistencyMode(ctx, consistencyMode); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to persist consistency mode: %v", err)
		}
	}

	metadataDurability, err := metadatadurability.Parse(req.GetParameters()["metadata-durability"])
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if metadataDurability != pb.MetadataDurability_METADATA_DURABILITY_UNSPECIFIED {
		if err := vol.PersistMetadataDurability(ctx, metadataDurability); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to persist metadata durability: %v", err)
		}
	}

	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      volumeID,
			CapacityBytes: reqBytes,
			VolumeContext: volParams,
		},
	}, nil
}

func (c *CSIController) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}

	c.server.mu.Lock()
	delete(c.server.volumes, req.GetVolumeId())
	c.server.mu.Unlock()

	return &csi.DeleteVolumeResponse{}, nil
}

func (c *CSIController) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	return &csi.ControllerGetCapabilitiesResponse{
		Capabilities: []*csi.ControllerServiceCapability{
			{
				Type: &csi.ControllerServiceCapability_Rpc{
					Rpc: &csi.ControllerServiceCapability_RPC{
						Type: csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
					},
				},
			},
			{
				Type: &csi.ControllerServiceCapability_Rpc{
					Rpc: &csi.ControllerServiceCapability_RPC{
						Type: csi.ControllerServiceCapability_RPC_SINGLE_NODE_MULTI_WRITER,
					},
				},
			},
		},
	}, nil
}

func (c *CSIController) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if req.GetVolumeCapabilities() == nil || len(req.GetVolumeCapabilities()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume capabilities are required")
	}

	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
			VolumeCapabilities: req.GetVolumeCapabilities(),
		},
	}, nil
}
