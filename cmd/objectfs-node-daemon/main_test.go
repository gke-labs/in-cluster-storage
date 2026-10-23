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
	"net"
	"strings"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/controller"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestParseEndpoint(t *testing.T) {
	tests := []struct {
		endpoint  string
		wantProto string
		wantAddr  string
		wantErr   bool
	}{
		{"unix:///tmp/csi.sock", "unix", "/tmp/csi.sock", false},
		{"tcp://127.0.0.1:10000", "tcp", "127.0.0.1:10000", false},
		{"http://127.0.0.1:10000", "", "", true},
		{"invalid", "", "", true},
	}

	for _, tt := range tests {
		proto, addr, err := parseEndpoint(tt.endpoint)
		if (err != nil) != tt.wantErr {
			t.Errorf("parseEndpoint(%q) err = %v, wantErr %v", tt.endpoint, err, tt.wantErr)
		}
		if proto != tt.wantProto || addr != tt.wantAddr {
			t.Errorf("parseEndpoint(%q) = (%q, %q), want (%q, %q)", tt.endpoint, proto, addr, tt.wantProto, tt.wantAddr)
		}
	}
}

func TestParseWriteMode(t *testing.T) {
	tests := []struct {
		input string
		want  pb.WriteMode
	}{
		{"lazy", pb.WriteMode_LAZY_WRITE},
		{"lazy_write", pb.WriteMode_LAZY_WRITE},
		{"eager", pb.WriteMode_EAGER_REPLICATION},
		{"eager_replication", pb.WriteMode_EAGER_REPLICATION},
		{"fsync", pb.WriteMode_WRITE_THROUGH_FSYNC},
		{"writethrough", pb.WriteMode_WRITE_THROUGH_FSYNC},
		{"unknown", pb.WriteMode_WRITE_THROUGH_FSYNC},
	}

	for _, tt := range tests {
		got := parseWriteMode(tt.input)
		if got != tt.want {
			t.Errorf("parseWriteMode(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestPluginInfo(t *testing.T) {
	driver, err := newDriver("test-node", "localhost:50051", t.TempDir(), 1024*1024, 10*1024*1024, 30*time.Second, 10*time.Minute, true, 30*time.Second)
	if err != nil {
		t.Fatalf("newDriver failed: %v", err)
	}
	defer func() { _ = driver.Close() }()
	ctx := t.Context()

	info, err := driver.GetPluginInfo(ctx, &csi.GetPluginInfoRequest{})
	if err != nil {
		t.Fatalf("GetPluginInfo failed: %v", err)
	}
	if info.GetName() != "objectfs.labs.gke.io" {
		t.Errorf("Expected driver name objectfs.labs.gke.io, got %s", info.GetName())
	}

	nodeInfo, err := driver.NodeGetInfo(ctx, &csi.NodeGetInfoRequest{})
	if err != nil {
		t.Fatalf("NodeGetInfo failed: %v", err)
	}
	if nodeInfo.GetNodeId() != "test-node" {
		t.Errorf("Expected node ID test-node, got %s", nodeInfo.GetNodeId())
	}
}

func TestNodeGetVolumeStats(t *testing.T) {
	ctx := t.Context()
	server := controller.NewServer(nil)
	volumeID := "test-csi-stats-vol"

	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	pb.RegisterObjectFSControllerServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(lis) }()
	defer grpcServer.Stop()

	driver, err := newDriver("test-node", "passthrough://bufnet", t.TempDir(), 1024*1024, 10*1024*1024, 30*time.Second, 10*time.Minute, true, 30*time.Second)
	if err != nil {
		t.Fatalf("newDriver failed: %v", err)
	}
	defer func() { _ = driver.Close() }()
	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient failed: %v", err)
	}
	defer conn.Close()
	driver.client = pb.NewObjectFSControllerClient(conn)

	resp, err := driver.NodeGetVolumeStats(ctx, &csi.NodeGetVolumeStatsRequest{
		VolumeId: volumeID,
	})
	if err != nil {
		t.Fatalf("NodeGetVolumeStats failed: %v", err)
	}
	if len(resp.GetUsage()) != 2 {
		t.Fatalf("expected 2 usage entries (bytes and inodes), got %d", len(resp.GetUsage()))
	}
	bytesUsage := resp.GetUsage()[0]
	if bytesUsage.GetUnit() != csi.VolumeUsage_BYTES {
		t.Errorf("expected unit BYTES, got %v", bytesUsage.GetUnit())
	}
	inodesUsage := resp.GetUsage()[1]
	if inodesUsage.GetUnit() != csi.VolumeUsage_INODES {
		t.Errorf("expected unit INODES, got %v", inodesUsage.GetUnit())
	}
	if inodesUsage.GetUsed() != 1 { // root directory inode
		t.Errorf("expected 1 used inode, got %d", inodesUsage.GetUsed())
	}
}

func TestNodeDaemonFailClosed(t *testing.T) {
	driver, err := newDriver("test-node", "localhost:50051", t.TempDir(), 1024*1024, 10*1024*1024, 30*time.Second, 10*time.Minute, true, 30*time.Second)
	if err != nil {
		t.Fatalf("newDriver failed: %v", err)
	}
	defer func() { _ = driver.Close() }()

	// Explicitly set insecure to false (mimicking production flag default)
	driver.insecure = false
	driver.caCertFile = ""
	driver.tokenFile = ""

	_, err = driver.getClient()
	if err == nil || !strings.Contains(err.Error(), "CA certificate file required") {
		t.Fatalf("expected CA certificate file required error, got: %v", err)
	}

	driver.caCertFile = "/nonexistent/ca.crt"
	_, err = driver.getClient()
	if err == nil || !strings.Contains(err.Error(), "CA certificate file not found: /nonexistent/ca.crt") {
		t.Fatalf("expected CA certificate file not found error, got: %v", err)
	}
}
