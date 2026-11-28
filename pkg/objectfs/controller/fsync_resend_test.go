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
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/grpcutil"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/blob"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type unavailablePutBackend struct {
	ObjectStorageBackend
	unavailable atomic.Bool
	putFailed   chan struct{}
}

func (b *unavailablePutBackend) PutObject(ctx context.Context, volumeID, key string, stream blob.ByteStream) (string, error) {
	if b.unavailable.Load() {
		select {
		case b.putFailed <- struct{}{}:
		default:
		}
		return "", fmt.Errorf("simulated object store outage: %w", objectstore.ErrUnavailable)
	}
	return b.ObjectStorageBackend.PutObject(ctx, volumeID, key, stream)
}

func startResendTest(t *testing.T) (*unavailablePutBackend, pb.ObjectFSControllerClient, pb.ObjectFSControllerClient) {
	t.Helper()

	backend := &unavailablePutBackend{
		ObjectStorageBackend: NewMemoryBackend(),
		putFailed:            make(chan struct{}, 1),
	}
	server := NewTestServer(t, backend)
	t.Cleanup(func() { backend.unavailable.Store(false) })

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

	dial := func(extraOptions ...grpc.DialOption) pb.ObjectFSControllerClient {
		options := append([]grpc.DialOption{
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		}, extraOptions...)
		connection, err := grpc.NewClient("passthrough://bufnet", options...)
		if err != nil {
			t.Fatalf("grpc.NewClient failed: %v", err)
		}
		t.Cleanup(func() {
			if err := connection.Close(); err != nil {
				t.Errorf("connection.Close error: %v", err)
			}
		})
		return pb.NewObjectFSControllerClient(connection)
	}

	return backend, dial(grpc.WithChainUnaryInterceptor(grpcutil.ResendUnaryClientInterceptor())), dial()
}

func stageTestFile(t *testing.T, ctx context.Context, client pb.ObjectFSControllerClient, volumeID, name string, payload []byte) uint64 {
	t.Helper()

	created, err := client.CreateFile(ctx, &pb.CreateFileRequest{VolumeId: volumeID, ParentInode: 1, Name: name, Mode: 0644})
	if err != nil || created.GetError() != 0 {
		t.Fatalf("CreateFile(%s) failed: err=%v errno=%d", name, err, created.GetError())
	}
	inodeID := created.GetAttr().GetInode().GetIno()
	written, err := client.WriteFile(ctx, &pb.WriteFileRequest{VolumeId: volumeID, Inode: inodeID, Offset: 0, Data: payload})
	if err != nil || written.GetError() != 0 {
		t.Fatalf("WriteFile(%s) failed: err=%v errno=%d", name, err, written.GetError())
	}
	return inodeID
}

func TestFsync_ResendsWhileObjectStoreUnavailable(t *testing.T) {
	payload := bytes.Repeat([]byte("objectfs-outage-payload-"), 512)

	t.Run("PausesDuringOutageAndRecovers", func(t *testing.T) {
		ctx := t.Context()
		backend, client, _ := startResendTest(t)
		volumeID := "test-fsync-resend-recover"
		inodeID := stageTestFile(t, ctx, client, volumeID, "data.bin", payload)

		backend.unavailable.Store(true)

		type fsyncOutcome struct {
			response *pb.FsyncResponse
			err      error
		}
		fsyncDone := make(chan fsyncOutcome, 1)
		go func() {
			response, err := client.Fsync(ctx, &pb.FsyncRequest{VolumeId: volumeID, Inode: inodeID, Blocking: true})
			fsyncDone <- fsyncOutcome{response: response, err: err}
		}()

		select {
		case <-backend.putFailed:
		case outcome := <-fsyncDone:
			t.Fatalf("Fsync returned before PutObject outage: response=%v err=%v", outcome.response, outcome.err)
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for Fsync to hit PutObject during outage")
		}

		mkdirResponse, err := client.Mkdir(ctx, &pb.MkdirRequest{VolumeId: volumeID, ParentInode: 1, Name: "during-outage", Mode: 0755})
		if err != nil || mkdirResponse.GetError() != 0 {
			t.Fatalf("Mkdir during outage failed: err=%v errno=%d", err, mkdirResponse.GetError())
		}

		select {
		case outcome := <-fsyncDone:
			t.Fatalf("Fsync returned while store was still unavailable: response=%v err=%v", outcome.response, outcome.err)
		default:
		}

		backend.unavailable.Store(false)

		select {
		case outcome := <-fsyncDone:
			if outcome.err != nil || !outcome.response.GetSuccess() || outcome.response.GetError() != 0 {
				t.Fatalf("Fsync failed after store recovered: err=%v response=%v", outcome.err, outcome.response)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for Fsync to succeed after store recovered")
		}

		readResponse, err := client.ReadFile(ctx, &pb.ReadFileRequest{VolumeId: volumeID, Inode: inodeID, Offset: 0, Size: int64(len(payload))})
		if err != nil || readResponse.GetError() != 0 {
			t.Fatalf("ReadFile failed: err=%v errno=%d", err, readResponse.GetError())
		}
		if !bytes.Equal(readResponse.GetData(), payload) {
			t.Fatalf("ReadFile data mismatch: got %d bytes, want %d bytes", len(readResponse.GetData()), len(payload))
		}
	})

	t.Run("WithoutInterceptorReturnsResendStatus", func(t *testing.T) {
		ctx := t.Context()
		backend, _, rawClient := startResendTest(t)
		volumeID := "test-fsync-resend-raw"
		inodeID := stageTestFile(t, ctx, rawClient, volumeID, "raw.bin", payload)

		backend.unavailable.Store(true)

		response, err := rawClient.Fsync(ctx, &pb.FsyncRequest{VolumeId: volumeID, Inode: inodeID, Blocking: true})
		if err == nil || response != nil {
			t.Fatalf("expected resend gRPC status with nil response, got response=%v err=%v", response, err)
		}
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("expected Unavailable code, got %v (err=%v)", status.Code(err), err)
		}
		if delay, resend := grpcutil.ResendDelay(err); !resend || delay != time.Second {
			t.Fatalf("ResendDelay(%v) = (%v, %v), want (%v, true)", err, delay, resend, time.Second)
		}
	})

	t.Run("ReturnsAtDeadlineWhenOutagePersists", func(t *testing.T) {
		ctx := t.Context()
		backend, client, _ := startResendTest(t)
		volumeID := "test-fsync-resend-deadline"
		inodeID := stageTestFile(t, ctx, client, volumeID, "deadline.bin", payload)

		backend.unavailable.Store(true)

		deadline := 500 * time.Millisecond
		deadlineCtx, cancel := context.WithTimeout(ctx, deadline)
		defer cancel()

		start := time.Now()
		_, err := client.Fsync(deadlineCtx, &pb.FsyncRequest{VolumeId: volumeID, Inode: inodeID, Blocking: true})
		elapsed := time.Since(start)

		if err == nil {
			t.Fatal("expected Fsync to fail at deadline while store stayed unavailable")
		}
		if !errors.Is(deadlineCtx.Err(), context.DeadlineExceeded) {
			t.Fatalf("expected context DeadlineExceeded, got %v (rpc err=%v)", deadlineCtx.Err(), err)
		}
		if elapsed < 450*time.Millisecond || elapsed > 950*time.Millisecond {
			t.Fatalf("expected Fsync to return near %v deadline, elapsed %v", deadline, elapsed)
		}
	})
}
