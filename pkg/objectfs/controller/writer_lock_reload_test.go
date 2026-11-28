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
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
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

type writerLockReloadFixture struct {
	t             *testing.T
	ctx           context.Context
	backend       ObjectStorageBackend
	clock         *buffertest.FakeClock
	bufferTest    *buffertest.BufferTest
	sessionEnded  chan struct{}
	connectionMu  sync.Mutex
	connectionCut bool
	connectionsA  []net.Conn
	dialOptionsA  []grpc.DialOption
}

func newWriterLockReloadFixture(t *testing.T) *writerLockReloadFixture {
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

	fixture := &writerLockReloadFixture{
		t:            t,
		ctx:          ctx,
		backend:      backend,
		clock:        clock,
		bufferTest:   bufferTest,
		sessionEnded: sessionEnded,
	}
	fixture.dialOptionsA = []grpc.DialOption{
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			fixture.connectionMu.Lock()
			defer fixture.connectionMu.Unlock()
			if fixture.connectionCut {
				return nil, errors.New("connection cut")
			}
			connection, err := listenerA.Dial()
			if err == nil {
				fixture.connectionsA = append(fixture.connectionsA, connection)
			}
			return connection, err
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	return fixture
}

func (f *writerLockReloadFixture) startServer(clientID string, dialOptions []grpc.DialOption) (*Server, string) {
	f.t.Helper()
	walDirectory := f.t.TempDir()
	server := NewServer(f.backend,
		WithServerWAL(walDirectory, f.bufferTest.Target, walclient.Witness),
		WithServerWALClient(f.bufferTest.Client),
		WithServerMetadataDir(f.t.TempDir()),
		WithServerWithoutFsync(),
		WithServerStreamFactory(func(volumeID string) (walclient.Stream, error) {
			return walclient.Open(f.ctx, walDirectory, StreamIDForVolume(volumeID), f.bufferTest.Target,
				walclient.WithDialOptions(dialOptions...),
				walclient.WithoutFsync(),
				walclient.WithClientID(clientID),
			)
		}),
	)
	if err := server.Start(f.ctx); err != nil {
		f.t.Fatalf("server.Start (%s) failed: %v", clientID, err)
	}
	f.t.Cleanup(func() {
		if err := server.Close(); err != nil {
			f.t.Errorf("server.Close (%s) failed: %v", clientID, err)
		}
	})
	return server, walDirectory
}

func (f *writerLockReloadFixture) cutControllerA() {
	f.t.Helper()
	f.connectionMu.Lock()
	f.connectionCut = true
	for _, connection := range f.connectionsA {
		if err := connection.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			f.t.Errorf("connection.Close failed: %v", err)
		}
	}
	f.connectionsA = nil
	f.connectionMu.Unlock()
	select {
	case <-f.sessionEnded:
	case <-time.After(5 * time.Second):
		f.t.Fatal("timed out waiting for controller A's Append session to end")
	}
}

func (f *writerLockReloadFixture) takeoverByControllerBAndHealA(volumeID string, volumeA *Volume, dataB []byte) {
	f.t.Helper()
	f.clock.Advance(16 * time.Second)
	serverB, _ := f.startServer("controller-B", f.bufferTest.DialOpts)
	f.mustCreate(serverB, volumeID, "/from-b.txt", dataB)
	f.mustShutdown(serverB)

	f.connectionMu.Lock()
	f.connectionCut = false
	f.connectionMu.Unlock()
	for deadline := time.Now().Add(5 * time.Second); !errors.Is(volumeA.Degraded(), walclient.ErrWriterLockLost); {
		if time.Now().After(deadline) {
			f.t.Fatalf("timed out waiting for A's stream to report writer lock lost; degraded=%v", volumeA.Degraded())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (f *writerLockReloadFixture) mustCreate(server *Server, volumeID, name string, data []byte) {
	f.t.Helper()
	response, err := testCreateFile(f.ctx, server, volumeID, name, 0644, data, 0, 0)
	if err != nil || response.GetError() != 0 {
		f.t.Fatalf("CreateFile(%s) failed: err=%v errno=%v", name, err, syscall.Errno(response.GetError()))
	}
}

func (f *writerLockReloadFixture) mustRead(server *Server, volumeID, name string, expected []byte) {
	f.t.Helper()
	response, err := testReadFile(f.ctx, server, volumeID, name, 0, int64(len(expected)))
	if err != nil || response.GetError() != 0 || !bytes.Equal(response.GetData(), expected) {
		f.t.Fatalf("ReadFile(%s) failed: err=%v errno=%v data=%q", name, err, syscall.Errno(response.GetError()), string(response.GetData()))
	}
}

func (f *writerLockReloadFixture) mustShutdown(server *Server) {
	f.t.Helper()
	shutdownCtx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx, 5*time.Second, time.Second); err != nil {
		f.t.Fatalf("server.Shutdown failed: %v", err)
	}
}

func TestServer_ReloadsVolumeAfterWriterLockLost(t *testing.T) {
	fixture := newWriterLockReloadFixture(t)
	const volumeID = "test-writer-lock-reload-volume"

	serverA, _ := fixture.startServer("controller-A", fixture.dialOptionsA)
	fixture.mustCreate(serverA, volumeID, "/from-a-1.txt", []byte("written by A before lock loss"))

	serverA.mu.RLock()
	volumeA := serverA.volumes[volumeID]
	serverA.mu.RUnlock()

	fixture.cutControllerA()
	dataB := []byte("written by B after takeover")
	fixture.takeoverByControllerBAndHealA(volumeID, volumeA, dataB)

	// A reloads on the next request: reading B's file and creating a new file both succeed.
	dataA2 := []byte("written by A after reload")
	fixture.mustRead(serverA, volumeID, "/from-b.txt", dataB)
	fixture.mustCreate(serverA, volumeID, "/from-a-2.txt", dataA2)
	fixture.mustShutdown(serverA)

	// A third server opened afterwards sees both B's file and A's later file.
	serverC, _ := fixture.startServer("controller-C", fixture.bufferTest.DialOpts)
	fixture.mustRead(serverC, volumeID, "/from-b.txt", dataB)
	fixture.mustRead(serverC, volumeID, "/from-a-2.txt", dataA2)
}

func TestServer_ReloadMovesAsideStagedDataAndWALSegments(t *testing.T) {
	fixture := newWriterLockReloadFixture(t)
	const volumeID = "test-writer-lock-move-aside-volume"

	serverA, walDirectoryA := fixture.startServer("controller-A", fixture.dialOptionsA)
	initialDataA := []byte("initial data from A")
	fixture.mustCreate(serverA, volumeID, "/from-a-1.txt", initialDataA)

	serverA.mu.RLock()
	volumeA := serverA.volumes[volumeID]
	serverA.mu.RUnlock()
	localStorageDirA := volumeA.localStorageDir
	streamID := volumeA.StreamID()

	// Cut A off, stage a write (no fsync), and append a WAL record the stream never accepts.
	fixture.cutControllerA()
	stagedData := []byte("unsynced-staged-write-from-A-0123456789")
	if response, err := testWriteFile(fixture.ctx, serverA, volumeID, "/from-a-1.txt", 0, stagedData, pb.WriteMode_LAZY); err != nil || response.GetError() != 0 {
		t.Fatalf("staged WriteFile failed: err=%v errno=%v", err, syscall.Errno(response.GetError()))
	}

	localBefore, _, _ := volumeA.Stream().Watermarks()
	unacceptedPayload := []byte("unaccepted-record-payload-from-A")
	unacceptedDone := make(chan error, 1)
	go func() {
		response, err := testCreateFile(fixture.ctx, serverA, volumeID, "/unaccepted-from-a.txt", 0644, unacceptedPayload, 0, 0)
		if err == nil && response.GetError() != 0 {
			err = syscall.Errno(response.GetError())
		}
		unacceptedDone <- err
	}()
	for deadline := time.Now().Add(5 * time.Second); ; {
		if localNow, _, _ := volumeA.Stream().Watermarks(); localNow > localBefore {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for unaccepted record to append to local WAL segment")
		}
		time.Sleep(10 * time.Millisecond)
	}

	dataB := []byte("written by B after takeover")
	fixture.takeoverByControllerBAndHealA(volumeID, volumeA, dataB)
	if err := <-unacceptedDone; err == nil {
		t.Fatal("expected unaccepted CreateFile to fail after writer lock loss")
	}

	// Reload A by reading B's file; neither staged bytes nor unaccepted record appears.
	fixture.mustRead(serverA, volumeID, "/from-b.txt", dataB)
	attrA1, err := testGetAttr(fixture.ctx, serverA, volumeID, "/from-a-1.txt")
	if err != nil || attrA1.GetError() != 0 || attrA1.GetAttr().GetInode().GetSize() != int64(len(initialDataA)) {
		t.Fatalf("GetAttr(/from-a-1.txt) = (err=%v errno=%v size=%d), want size %d",
			err, syscall.Errno(attrA1.GetError()), attrA1.GetAttr().GetInode().GetSize(), len(initialDataA))
	}
	fixture.mustRead(serverA, volumeID, "/from-a-1.txt", initialDataA)
	if attr, err := testGetAttr(fixture.ctx, serverA, volumeID, "/unaccepted-from-a.txt"); err != nil || syscall.Errno(attr.GetError()) != syscall.ENOENT {
		t.Fatalf("expected ENOENT for unaccepted file after reload, got err=%v errno=%v", err, syscall.Errno(attr.GetError()))
	}

	// Staged bytes are preserved under the sibling <localStorageDir>.writer-lock-lost-* directory.
	stagingMatches, err := filepath.Glob(localStorageDirA + ".writer-lock-lost-*")
	if err != nil || len(stagingMatches) != 1 || filepath.Dir(stagingMatches[0]) != filepath.Dir(localStorageDirA) {
		t.Fatalf("expected 1 sibling staging move-aside for %s, got %v (err=%v)", localStorageDirA, stagingMatches, err)
	}
	if !directoryContainsBytes(t, stagingMatches[0], stagedData) {
		t.Fatalf("staged payload %q not found under %s", stagedData, stagingMatches[0])
	}

	// Old WAL segments are moved where MoveSegmentsAside puts them and hold the unaccepted record.
	degradedMatches, err := filepath.Glob(filepath.Join(walDirectoryA, fmt.Sprintf("stream-%s.*.degraded", streamID)))
	if err != nil || len(degradedMatches) != 1 {
		t.Fatalf("expected 1 degraded WAL directory in %s, got %v (err=%v)", walDirectoryA, degradedMatches, err)
	}
	if !directoryContainsBytes(t, degradedMatches[0], unacceptedPayload) {
		t.Fatalf("unaccepted WAL record %q not found under %s", unacceptedPayload, degradedMatches[0])
	}
	if directoryContainsBytes(t, walDirectoryA, unacceptedPayload) {
		t.Fatalf("unaccepted WAL record %q still present directly in %s after move-aside", unacceptedPayload, walDirectoryA)
	}
}

func TestServer_ReloadRetriesWhenMoveSegmentsAsideFails(t *testing.T) {
	fixture := newWriterLockReloadFixture(t)
	const volumeID = "test-writer-lock-move-aside-retry-volume"

	serverA, walDirectoryA := fixture.startServer("controller-A", fixture.dialOptionsA)
	fixture.mustCreate(serverA, volumeID, "/from-a-1.txt", []byte("written by A"))

	serverA.mu.RLock()
	volumeA := serverA.volumes[volumeID]
	serverA.mu.RUnlock()
	localStorageDirA := volumeA.localStorageDir
	streamID := volumeA.StreamID()

	fixture.cutControllerA()
	stagedData := []byte("staged-write-preserved-across-retry")
	if response, err := testWriteFile(fixture.ctx, serverA, volumeID, "/from-a-1.txt", 0, stagedData, pb.WriteMode_LAZY); err != nil || response.GetError() != 0 {
		t.Fatalf("staged WriteFile failed: err=%v errno=%v", err, syscall.Errno(response.GetError()))
	}
	dataB := []byte("written by B")
	fixture.takeoverByControllerBAndHealA(volumeID, volumeA, dataB)

	// Point serverA.wal.dir at a regular file so MoveSegmentsAside fails even as root.
	notADirectory := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(notADirectory, []byte("block"), 0644); err != nil {
		t.Fatalf("WriteFile(%s) failed: %v", notADirectory, err)
	}
	serverA.wal.dir = notADirectory

	if _, err := serverA.GetAttr(fixture.ctx, &pb.GetAttrRequest{VolumeId: volumeID, Inode: 1}); err == nil {
		t.Fatal("expected request to fail when MoveSegmentsAside fails")
	}

	serverA.mu.RLock()
	retainedVolume := serverA.volumes[volumeID]
	serverA.mu.RUnlock()
	volumeA.mu.RLock()
	closed := volumeA.closed
	volumeA.mu.RUnlock()
	if retainedVolume != volumeA || !closed {
		t.Fatalf("expected failed reload to leave closed volumeA in s.volumes (same=%v closed=%v)", retainedVolume == volumeA, closed)
	}

	// Restore wal.dir; the next request retries MoveSegmentsAside and reloads cleanly.
	serverA.wal.dir = walDirectoryA
	fixture.mustRead(serverA, volumeID, "/from-b.txt", dataB)

	stagingMatches, err := filepath.Glob(localStorageDirA + ".writer-lock-lost-*")
	if err != nil || len(stagingMatches) != 1 || !directoryContainsBytes(t, stagingMatches[0], stagedData) {
		t.Fatalf("expected 1 staging move-aside directory with staged bytes across retry, got %v (err=%v)", stagingMatches, err)
	}
	degradedMatches, err := filepath.Glob(filepath.Join(walDirectoryA, fmt.Sprintf("stream-%s.*.degraded", streamID)))
	if err != nil || len(degradedMatches) != 1 {
		t.Fatalf("expected 1 degraded WAL directory after retry, got %v (err=%v)", degradedMatches, err)
	}
}

func directoryContainsBytes(t *testing.T, directory string, needle []byte) bool {
	t.Helper()
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatalf("ReadDir(%s) failed: %v", directory, err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		contents, err := os.ReadFile(filepath.Join(directory, entry.Name()))
		if err != nil {
			t.Fatalf("ReadFile(%s) failed: %v", filepath.Join(directory, entry.Name()), err)
		}
		if bytes.Contains(contents, needle) {
			return true
		}
	}
	return false
}
