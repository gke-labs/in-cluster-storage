// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controller

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type watchGateBackend struct {
	ObjectStorageBackend
	targetStreamID string
	gate           chan struct{}
	enteredGate    chan struct{}
	once           sync.Once
}

func (g *watchGateBackend) ListObjects(ctx context.Context, volumeID string, prefix string) ([]objectstore.ObjectEntry, error) {
	if g.targetStreamID != "" && strings.Contains(prefix, g.targetStreamID) {
		g.once.Do(func() {
			close(g.enteredGate)
		})
		if g.gate != nil {
			select {
			case <-g.gate:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
	return g.ObjectStorageBackend.ListObjects(ctx, volumeID, prefix)
}

func newWatchTestClient(t *testing.T, server *Server) pb.ObjectFSControllerClient {
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	pb.RegisterObjectFSControllerServer(grpcServer, server)
	go func() {
		if err := grpcServer.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("grpcServer.Serve error: %v", err)
		}
	}()
	t.Cleanup(func() {
		grpcServer.Stop()
		if err := listener.Close(); err != nil {
			t.Errorf("listener.Close error: %v", err)
		}
	})

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Errorf("conn.Close error: %v", err)
		}
	})

	return pb.NewObjectFSControllerClient(conn)
}

func TestWatchVolume_HeaderGuaranteesSubscription(t *testing.T) {
	server := NewTestServer(t, NewMemoryBackend())
	client := newWatchTestClient(t, server)
	volumeID := "test-watch-header-vol"

	watchCtx, cancelWatch := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancelWatch()

	stream, err := client.WatchVolume(watchCtx, &pb.WatchVolumeRequest{
		VolumeId: volumeID,
		NodeId:   "node-1",
	})
	if err != nil {
		t.Fatalf("WatchVolume failed: %v", err)
	}

	header, err := stream.Header()
	if err != nil {
		if errors.Is(watchCtx.Err(), context.DeadlineExceeded) {
			t.Fatalf("stream.Header() timed out after 30s guard: %v", err)
		}
		t.Fatalf("stream.Header() failed: %v", err)
	}
	if header == nil {
		t.Fatalf("stream.Header() returned nil metadata")
	}

	expectedEvent := &pb.WatchVolumeResponse{
		EventType: pb.WatchEventType_EVENT_CREATED,
		Inode:     42,
		Name:      "test.txt",
	}
	server.Broadcaster().Broadcast(volumeID, expectedEvent)

	receivedEvent, err := stream.Recv()
	if err != nil {
		if errors.Is(watchCtx.Err(), context.DeadlineExceeded) {
			t.Fatalf("stream.Recv() timed out after 30s guard: %v", err)
		}
		t.Fatalf("stream.Recv() failed: %v", err)
	}
	if receivedEvent.GetEventType() != expectedEvent.GetEventType() || receivedEvent.GetInode() != expectedEvent.GetInode() || receivedEvent.GetName() != expectedEvent.GetName() {
		t.Fatalf("Received event mismatch: got %+v, want %+v", receivedEvent, expectedEvent)
	}
}

func TestWatchVolume_HeaderWaitsForSubscribeGate(t *testing.T) {
	volumeID := "test-watch-gate-vol"
	targetStreamID := StreamIDForVolume(volumeID).String()

	gate := make(chan struct{})
	enteredGate := make(chan struct{})
	backend := &watchGateBackend{
		ObjectStorageBackend: NewMemoryBackend(),
		targetStreamID:       targetStreamID,
		gate:                 gate,
		enteredGate:          enteredGate,
	}

	server := NewTestServer(t, backend)
	client := newWatchTestClient(t, server)

	watchCtx, cancelWatch := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancelWatch()

	stream, err := client.WatchVolume(watchCtx, &pb.WatchVolumeRequest{
		VolumeId:        volumeID,
		NodeId:          "node-gate",
		ConsistencyMode: pb.ConsistencyMode_STANDARD,
	})
	if err != nil {
		t.Fatalf("WatchVolume failed: %v", err)
	}

	headerResult := make(chan error, 1)
	go func() {
		_, headerErr := stream.Header()
		headerResult <- headerErr
	}()

	select {
	case <-enteredGate:
	case <-time.After(30 * time.Second):
		t.Fatalf("getOrCreateVolume did not reach gate after 30s guard")
	}

	select {
	case headerErr := <-headerResult:
		t.Fatalf("stream.Header() returned before getOrCreateVolume completed: %v", headerErr)
	case <-time.After(100 * time.Millisecond):
	}

	earlyEvent := &pb.WatchVolumeResponse{
		EventType: pb.WatchEventType_EVENT_CREATED,
		Inode:     100,
		Name:      "early.txt",
	}
	server.Broadcaster().Broadcast(volumeID, earlyEvent)

	close(gate)

	select {
	case headerErr := <-headerResult:
		if headerErr != nil {
			t.Fatalf("stream.Header() returned error after gate release: %v", headerErr)
		}
	case <-time.After(30 * time.Second):
		t.Fatalf("stream.Header() timed out after gate release")
	}

	subscribedEvent := &pb.WatchVolumeResponse{
		EventType: pb.WatchEventType_EVENT_CREATED,
		Inode:     101,
		Name:      "subscribed.txt",
	}
	server.Broadcaster().Broadcast(volumeID, subscribedEvent)

	receivedEvent, err := stream.Recv()
	if err != nil {
		if errors.Is(watchCtx.Err(), context.DeadlineExceeded) {
			t.Fatalf("stream.Recv() timed out after 30s guard: %v", err)
		}
		t.Fatalf("stream.Recv() failed: %v", err)
	}
	if receivedEvent.GetInode() != subscribedEvent.GetInode() || receivedEvent.GetName() != subscribedEvent.GetName() {
		t.Fatalf("Expected subscribed event (%d, %s), got: (%d, %s)", subscribedEvent.GetInode(), subscribedEvent.GetName(), receivedEvent.GetInode(), receivedEvent.GetName())
	}
}

func TestWatchVolume_ConsistencyModeMismatch_TrailersOnly(t *testing.T) {
	server := NewTestServer(t, NewMemoryBackend())
	client := newWatchTestClient(t, server)
	volumeID := "test-watch-mismatch-vol"

	volume, err := server.getOrCreateVolume(volumeID)
	if err != nil {
		t.Fatalf("getOrCreateVolume failed: %v", err)
	}
	volume.SetConsistencyMode(pb.ConsistencyMode_CLOSE_TO_OPEN)

	watchCtx, cancelWatch := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancelWatch()

	stream, err := client.WatchVolume(watchCtx, &pb.WatchVolumeRequest{
		VolumeId:        volumeID,
		NodeId:          "node-mismatch",
		ConsistencyMode: pb.ConsistencyMode_STANDARD,
	})
	if err != nil {
		t.Fatalf("WatchVolume failed: %v", err)
	}

	header, err := stream.Header()
	if err != nil {
		t.Fatalf("stream.Header() returned non-nil error: %v", err)
	}
	if header != nil {
		t.Fatalf("stream.Header() returned non-nil metadata on trailers-only reply: %v", header)
	}

	_, receiveErr := stream.Recv()
	if receiveErr == nil {
		t.Fatalf("stream.Recv() succeeded, expected InvalidArgument error")
	}
	if status.Code(receiveErr) != codes.InvalidArgument {
		t.Fatalf("stream.Recv() status code = %v, want %v; err: %v", status.Code(receiveErr), codes.InvalidArgument, receiveErr)
	}
}
