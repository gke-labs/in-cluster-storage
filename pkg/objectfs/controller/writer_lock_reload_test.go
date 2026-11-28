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
	sessionEnded chan struct{}
}

func (s *appendNotifyWalServer) Append(stream walpb.WalBuffer_AppendServer) error {
	err := s.WalBufferServer.Append(stream)
	select {
	case s.sessionEnded <- struct{}{}:
	default:
	}
	return err
}

func TestServer_ReloadsVolumeAfterWriterLockLost(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	clock := buffertest.NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	bufferTest, _ := buffertest.Start(ctx, t,
		buffertest.WithBackend(backend),
		buffertest.WithWriterLockGrace(15*time.Second),
		buffertest.WithClock(clock),
	)

	sessionEnded := make(chan struct{}, 8)
	listenerA := bufconn.Listen(4 * 1024 * 1024)
	grpcServerA := grpc.NewServer()
	walpb.RegisterWalBufferServer(grpcServerA, &appendNotifyWalServer{
		WalBufferServer: bufferTest.Server,
		sessionEnded:    sessionEnded,
	})
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

	startServer := func(clientID string, dialOptions []grpc.DialOption) *Server {
		walDirectory := t.TempDir()
		server := NewServer(backend,
			WithServerWAL(walDirectory, bufferTest.Target, walclient.Witness),
			WithServerWALClient(bufferTest.Client),
			WithServerMetadataDir(t.TempDir()),
			WithServerWithoutFsync(),
			WithServerStreamFactory(func(volumeID string) (walclient.Stream, error) {
				return walclient.Open(ctx, walDirectory, StreamIDForVolume(volumeID), bufferTest.Target,
					walclient.WithDialOptions(dialOptions...),
					walclient.WithoutFsync(),
					walclient.WithClientID(clientID),
				)
			}),
		)
		if err := server.Start(ctx); err != nil {
			t.Fatalf("server.Start (%s) failed: %v", clientID, err)
		}
		t.Cleanup(func() {
			if err := server.Close(); err != nil {
				t.Errorf("server.Close (%s) failed: %v", clientID, err)
			}
		})
		return server
	}
	const volumeID = "test-writer-lock-reload-volume"
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
	mustShutdown := func(server *Server) {
		t.Helper()
		shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx, 5*time.Second, time.Second); err != nil {
			t.Fatalf("server.Shutdown failed: %v", err)
		}
	}

	serverA := startServer("controller-A", dialOptionsA)
	mustCreate(serverA, "/from-a-1.txt", []byte("written by A before lock loss"))

	serverA.mu.RLock()
	volumeA := serverA.volumes[volumeID]
	serverA.mu.RUnlock()

	// Cut A's connection synchronously so the server records session end before the clock advances.
	connectionMu.Lock()
	connectionCut = true
	for _, connection := range connectionsA {
		if err := connection.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Errorf("connection.Close failed: %v", err)
		}
	}
	connectionsA = nil
	connectionMu.Unlock()
	select {
	case <-sessionEnded:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for controller A's Append session to end")
	}

	// Advance past the 15s grace period, let B take over, write a second file, and shut down cleanly.
	clock.Advance(16 * time.Second)
	serverB := startServer("controller-B", bufferTest.DialOpts)
	dataB := []byte("written by B after takeover")
	mustCreate(serverB, "/from-b.txt", dataB)
	mustShutdown(serverB)

	// Heal A's connection and wait for A's existing stream to report writer lock loss.
	connectionMu.Lock()
	connectionCut = false
	connectionMu.Unlock()
	for deadline := time.Now().Add(5 * time.Second); !errors.Is(volumeA.Degraded(), walclient.ErrWriterLockLost); {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for A's stream to report writer lock lost; degraded=%v", volumeA.Degraded())
		}
		time.Sleep(10 * time.Millisecond)
	}

	// A reloads on the next request: reading B's file and creating a new file both succeed.
	dataA2 := []byte("written by A after reload")
	mustRead(serverA, "/from-b.txt", dataB)
	mustCreate(serverA, "/from-a-2.txt", dataA2)
	mustShutdown(serverA)

	// A third server opened afterwards sees both B's file and A's later file.
	serverC := startServer("controller-C", bufferTest.DialOpts)
	mustRead(serverC, "/from-b.txt", dataB)
	mustRead(serverC, "/from-a-2.txt", dataA2)
}
