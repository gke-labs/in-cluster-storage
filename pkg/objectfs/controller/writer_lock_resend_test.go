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
	"errors"
	"net"
	"syscall"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/grpcutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func serveAndDialController(t *testing.T, server *Server, withClientResend bool) pb.ObjectFSControllerClient {
	t.Helper()
	listener := bufconn.Listen(4 * 1024 * 1024)
	grpcServer := grpc.NewServer(grpc.ChainUnaryInterceptor(grpcutil.UnaryServerInterceptor(), server.ResendUnaryServerInterceptor()))
	pb.RegisterObjectFSControllerServer(grpcServer, server)
	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		if err := grpcServer.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("controller grpcServer.Serve failed: %v", err)
		}
	}()
	t.Cleanup(func() {
		grpcServer.Stop()
		<-serveDone
		if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Errorf("controller listener.Close failed: %v", err)
		}
	})

	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	if withClientResend {
		dialOptions = append(dialOptions, grpc.WithChainUnaryInterceptor(grpcutil.ResendUnaryClientInterceptor()))
	}
	connection, err := grpc.NewClient("passthrough:///bufnet", dialOptions...)
	if err != nil {
		t.Fatalf("grpc.NewClient failed: %v", err)
	}
	t.Cleanup(func() {
		if err := connection.Close(); err != nil {
			t.Errorf("controller client connection.Close failed: %v", err)
		}
	})
	return pb.NewObjectFSControllerClient(connection)
}

func runMidRequestWriterLockLoss(t *testing.T, withClientResend bool) (pb.ObjectFSControllerClient, *pb.MkdirResponse, error) {
	t.Helper()
	ctx := t.Context()
	const volumeID = "test-writer-lock-resend-volume"
	fixture := newWriterLockFixture(t)
	serverA := fixture.startServer("controller-A", fixture.dialOptionsA)
	client := serveAndDialController(t, serverA, withClientResend)

	if response, err := client.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId: volumeID, ParentInode: 1, Name: "from-a-initial.txt", Mode: 0644, InitialContent: []byte("initial"),
	}); err != nil || response.GetError() != 0 {
		t.Fatalf("initial CreateFile failed: err=%v errno=%v", err, syscall.Errno(response.GetError()))
	}

	// Synchronously guarantee the writer lock is lost while Mkdir is blocked:
	// Volume.Mkdir (OPERATION metadata durability, Witness WAL durability)
	// appends its WAL record locally and blocks in stream.Wait until the WAL
	// buffer witnesses that record. appendNotifyWalServer intercepts that
	// record in Recv, closes recordBlocked, and holds it without forwarding to
	// the underlying WalBufferServer until A's connection is cut. Because the
	// record cannot be witnessed, Mkdir is guaranteed to be inside the handler
	// and blocked on A's stream while we hand off the lock to B.
	recordBlocked := fixture.walServerA.armNextRecordBlock()

	type mkdirResult struct {
		response *pb.MkdirResponse
		err      error
	}
	results := make(chan mkdirResult, 1)
	go func() {
		response, err := client.Mkdir(ctx, &pb.MkdirRequest{VolumeId: volumeID, ParentInode: 1, Name: "blocked-dir", Mode: 0755})
		results <- mkdirResult{response: response, err: err}
	}()

	select {
	case <-recordBlocked:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Mkdir WAL record to reach stream server")
	}

	fixture.handOffLockToB(volumeID, "/from-b.txt", []byte("from B"))

	select {
	case result := <-results:
		return client, result.response, result.err
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for blocked Mkdir to complete")
		return nil, nil, nil
	}
}

func TestServer_ResendAfterMidRequestWriterLockLoss(t *testing.T) {
	const volumeID = "test-writer-lock-resend-volume"

	t.Run("with_client_interceptor", func(t *testing.T) {
		client, response, err := runMidRequestWriterLockLoss(t, true)
		if err != nil || response.GetError() != 0 {
			t.Fatalf("Mkdir with resend client interceptor failed: err=%v errno=%v", err, syscall.Errno(response.GetError()))
		}
		readDirResponse, err := client.ReadDir(t.Context(), &pb.ReadDirRequest{VolumeId: volumeID, Inode: 1})
		if err != nil || readDirResponse.GetError() != 0 {
			t.Fatalf("ReadDir failed: err=%v errno=%v", err, syscall.Errno(readDirResponse.GetError()))
		}
		dirCount := 0
		sawFileFromB := false
		for _, entry := range readDirResponse.GetEntries() {
			if entry.GetName() == "blocked-dir" {
				dirCount++
			}
			if entry.GetName() == "from-b.txt" {
				sawFileFromB = true
			}
		}
		if dirCount != 1 || !sawFileFromB {
			t.Fatalf("ReadDir: blocked-dir count=%d sawFileFromB=%v, want (1, true)", dirCount, sawFileFromB)
		}
	})

	t.Run("without_client_interceptor", func(t *testing.T) {
		_, _, err := runMidRequestWriterLockLoss(t, false)
		delay, resend := grpcutil.ResendDelay(err)
		if !resend || delay != writerLockLostResendDelay {
			t.Fatalf("ResendDelay(%v) = (%v, %v), want (%v, true)", err, delay, resend, writerLockLostResendDelay)
		}
	})
}

func TestServer_ResendInterceptorPreservesSuccessAfterLockLoss(t *testing.T) {
	ctx := t.Context()
	const volumeID = "test-writer-lock-success-volume"
	fixture := newWriterLockFixture(t)
	serverA := fixture.startServer("controller-A", fixture.dialOptionsA)
	if response, err := testCreateFile(ctx, serverA, volumeID, "/initial.txt", 0644, []byte("init"), 0, 0); err != nil || response.GetError() != 0 {
		t.Fatalf("initial CreateFile failed: err=%v errno=%v", err, syscall.Errno(response.GetError()))
	}
	volumeA := serverA.lookupVolume(volumeID)

	expected := &pb.MkdirResponse{StreamPosition: 42}
	got, err := serverA.ResendUnaryServerInterceptor()(ctx, &pb.MkdirRequest{VolumeId: volumeID}, &grpc.UnaryServerInfo{
		FullMethod: "/objectfs.v1alpha1.ObjectFSController/Mkdir",
	}, func(context.Context, any) (any, error) {
		fixture.handOffLockToB(volumeID, "/from-b.txt", []byte("from B"))
		fixture.waitWriterLockLost(volumeA)
		return expected, nil
	})
	if err != nil || got != expected {
		t.Fatalf("got (%v, %v), want (%v, nil)", got, err, expected)
	}
}

func TestServer_ResendInterceptorOnlyForLostLock(t *testing.T) {
	ctx := t.Context()
	const volumeID = "only-for-lost-lock-volume"
	fixture := newWriterLockFixture(t)
	server := fixture.startServer("controller-A", fixture.bufferTest.DialOpts)
	client := serveAndDialController(t, server, false)

	if response, err := client.Mkdir(ctx, &pb.MkdirRequest{VolumeId: volumeID, ParentInode: 1, Name: "existing", Mode: 0755}); err != nil || response.GetError() != 0 {
		t.Fatalf("first Mkdir failed: err=%v errno=%v", err, syscall.Errno(response.GetError()))
	}
	if response, err := client.Lookup(ctx, &pb.LookupRequest{VolumeId: volumeID, ParentInode: 1, Name: "nonexistent"}); err != nil || response.GetError() != int32(syscall.ENOENT) {
		t.Fatalf("Lookup: err=%v errno=%v, want ENOENT", err, syscall.Errno(response.GetError()))
	}
	if response, err := client.Mkdir(ctx, &pb.MkdirRequest{VolumeId: volumeID, ParentInode: 1, Name: "existing", Mode: 0755}); err != nil || response.GetError() != int32(syscall.EEXIST) {
		t.Fatalf("second Mkdir: err=%v errno=%v, want EEXIST", err, syscall.Errno(response.GetError()))
	}
	volume := server.lookupVolume(volumeID)
	faultStream := &faultInjectingStream{Stream: volume.Stream(), streamID: StreamIDForVolume(volumeID)}
	if err := volume.ResetStream(faultStream); err != nil {
		t.Fatalf("ResetStream failed: %v", err)
	}
	faultStream.degrade(errors.New("segment store I/O error"))
	if response, err := client.Mkdir(ctx, &pb.MkdirRequest{VolumeId: volumeID, ParentInode: 1, Name: "after-degrade", Mode: 0755}); err != nil || response.GetError() != int32(syscall.EIO) {
		t.Fatalf("Mkdir after non-lock-loss degrade: err=%v errno=%v, want EIO", err, syscall.Errno(response.GetError()))
	}
}
