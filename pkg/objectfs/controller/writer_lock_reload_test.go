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
	"net"
	"sync"
	"syscall"
	"testing"
	"time"

	walpb "github.com/gke-labs/in-cluster-storage/pkg/api/wal/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/wal/buffer/buffertest"
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type appendNotifyWalServer struct {
	walpb.WalBufferServer
	sessionEnded    chan struct{}
	mu              sync.Mutex
	blockNextRecord bool
	recordBlocked   chan struct{}
}

func (s *appendNotifyWalServer) armNextRecordBlock() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.blockNextRecord = true
	s.recordBlocked = make(chan struct{})
	return s.recordBlocked
}

func (s *appendNotifyWalServer) Append(stream walpb.WalBuffer_AppendServer) error {
	err := s.WalBufferServer.Append(&blockingAppendStream{WalBuffer_AppendServer: stream, server: s})
	select {
	case s.sessionEnded <- struct{}{}:
	default:
	}
	return err
}

type blockingAppendStream struct {
	walpb.WalBuffer_AppendServer
	server *appendNotifyWalServer
}

func (s *blockingAppendStream) Recv() (*walpb.AppendRequest, error) {
	request, err := s.WalBuffer_AppendServer.Recv()
	if err != nil || request.GetRecord() == nil {
		return request, err
	}
	s.server.mu.Lock()
	shouldBlock := s.server.blockNextRecord
	blocked := s.server.recordBlocked
	s.server.blockNextRecord = false
	s.server.mu.Unlock()
	if shouldBlock {
		close(blocked)
		<-s.Context().Done()
		return nil, s.Context().Err()
	}
	return request, nil
}

type writerLockFixture struct {
	t            *testing.T
	backend      ObjectStorageBackend
	clock        *buffertest.FakeClock
	bufferTest   *buffertest.BufferTest
	walServerA   *appendNotifyWalServer
	dialOptionsA []grpc.DialOption
	setCut       func(bool)
}

func newWriterLockFixture(t *testing.T) *writerLockFixture {
	t.Helper()
	ctx := t.Context()
	backend := NewMemoryBackend()
	clock := buffertest.NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	bufferTest, _ := buffertest.Start(ctx, t,
		buffertest.WithBackend(backend),
		buffertest.WithWriterLockGrace(15*time.Second),
		buffertest.WithClock(clock),
	)

	sessionEnded := make(chan struct{}, 8)
	walServerA := &appendNotifyWalServer{
		WalBufferServer: bufferTest.Server,
		sessionEnded:    sessionEnded,
	}
	listenerA := bufconn.Listen(4 * 1024 * 1024)
	grpcServerA := grpc.NewServer()
	walpb.RegisterWalBufferServer(grpcServerA, walServerA)
	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		if err := grpcServerA.Serve(listenerA); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("grpcServerA.Serve failed: %v", err)
		}
	}()
	t.Cleanup(func() {
		grpcServerA.Stop()
		<-serveDone
		if err := listenerA.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Errorf("listenerA.Close failed: %v", err)
		}
	})

	var connectionMu sync.Mutex
	var connectionCut bool
	var connectionsA []net.Conn
	dialOptionsA := []grpc.DialOption{
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			connectionMu.Lock()
			defer connectionMu.Unlock()
			if connectionCut {
				return nil, errors.New("connection cut")
			}
			connection, err := listenerA.Dial()
			if err == nil {
				connectionsA = append(connectionsA, connection)
			}
			return connection, err
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	return &writerLockFixture{
		t: t, backend: backend, clock: clock, bufferTest: bufferTest, walServerA: walServerA, dialOptionsA: dialOptionsA,
		setCut: func(cut bool) {
			connectionMu.Lock()
			connectionCut = cut
			if cut {
				for _, connection := range connectionsA {
					if err := connection.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
						t.Errorf("connection.Close failed: %v", err)
					}
				}
				connectionsA = nil
			}
			connectionMu.Unlock()
		},
	}
}

func (f *writerLockFixture) startServer(clientID string, dialOptions []grpc.DialOption) *Server {
	f.t.Helper()
	ctx := f.t.Context()
	walDirectory := f.t.TempDir()
	server := NewServer(f.backend,
		WithServerWAL(walDirectory, f.bufferTest.Target, walclient.Witness),
		WithServerWALClient(f.bufferTest.Client),
		WithServerMetadataDir(f.t.TempDir()),
		WithServerWithoutFsync(),
		WithServerStreamFactory(func(volumeID string) (walclient.Stream, error) {
			return walclient.Open(ctx, walDirectory, StreamIDForVolume(volumeID), f.bufferTest.Target,
				walclient.WithDialOptions(dialOptions...),
				walclient.WithoutFsync(),
				walclient.WithClientID(clientID),
			)
		}),
	)
	if err := server.Start(ctx); err != nil {
		f.t.Fatalf("server.Start (%s) failed: %v", clientID, err)
	}
	f.t.Cleanup(func() {
		if err := server.Close(); err != nil {
			f.t.Errorf("server.Close (%s) failed: %v", clientID, err)
		}
	})
	return server
}

func (f *writerLockFixture) shutdownServer(server *Server) {
	f.t.Helper()
	shutdownCtx, cancel := context.WithTimeout(f.t.Context(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx, 5*time.Second, time.Second); err != nil {
		f.t.Fatalf("server.Shutdown failed: %v", err)
	}
}

// handOffLockToB cuts controller A synchronously, advances past the grace
// period so controller B takes over and writes path, shuts B down cleanly, and
// heals A so its stream reconnects and observes writer lock loss.
func (f *writerLockFixture) handOffLockToB(volumeID, path string, data []byte) {
	f.t.Helper()
	f.setCut(true)
	select {
	case <-f.walServerA.sessionEnded:
	case <-time.After(5 * time.Second):
		f.t.Fatal("timed out waiting for controller A's Append session to end")
	}
	f.clock.Advance(16 * time.Second)
	serverB := f.startServer("controller-B", f.bufferTest.DialOpts)
	response, err := testCreateFile(f.t.Context(), serverB, volumeID, path, 0644, data, 0, 0)
	if err != nil || response.GetError() != 0 {
		f.t.Fatalf("CreateFile(%s) on B failed: err=%v errno=%v", path, err, syscall.Errno(response.GetError()))
	}
	f.shutdownServer(serverB)
	f.setCut(false)
}

func (f *writerLockFixture) waitWriterLockLost(volume *Volume) {
	f.t.Helper()
	for deadline := time.Now().Add(5 * time.Second); !volume.writerLockLost(); {
		if time.Now().After(deadline) {
			f.t.Fatalf("timed out waiting for A's stream to report writer lock lost; degraded=%v", volume.Degraded())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestServer_ReloadsVolumeAfterWriterLockLost(t *testing.T) {
	ctx := t.Context()
	const volumeID = "test-writer-lock-reload-volume"
	fixture := newWriterLockFixture(t)
	mustCreate := func(server *Server, name string, data []byte) {
		t.Helper()
		response, err := testCreateFile(ctx, server, volumeID, name, 0644, data, 0, 0)
		if err != nil || response.GetError() != 0 {
			t.Fatalf("CreateFile(%s) failed: err=%v errno=%v", name, err, syscall.Errno(response.GetError()))
		}
	}
	mustRead := func(server *Server, name string, expected []byte) {
		t.Helper()
		response, err := testReadFile(ctx, server, volumeID, name, 0, int64(len(expected)))
		if err != nil || response.GetError() != 0 || !bytes.Equal(response.GetData(), expected) {
			t.Fatalf("ReadFile(%s) failed: err=%v errno=%v data=%q", name, err, syscall.Errno(response.GetError()), string(response.GetData()))
		}
	}

	serverA := fixture.startServer("controller-A", fixture.dialOptionsA)
	mustCreate(serverA, "/from-a-1.txt", []byte("written by A before lock loss"))
	volumeA := serverA.lookupVolume(volumeID)

	dataB := []byte("written by B after takeover")
	fixture.handOffLockToB(volumeID, "/from-b.txt", dataB)
	fixture.waitWriterLockLost(volumeA)

	// A reloads on the next request: reading B's file and creating a new file both succeed.
	dataA2 := []byte("written by A after reload")
	mustRead(serverA, "/from-b.txt", dataB)
	mustCreate(serverA, "/from-a-2.txt", dataA2)
	fixture.shutdownServer(serverA)

	// A third server opened afterwards sees both B's file and A's later file.
	serverC := fixture.startServer("controller-C", fixture.bufferTest.DialOpts)
	mustRead(serverC, "/from-b.txt", dataB)
	mustRead(serverC, "/from-a-2.txt", dataA2)
}
