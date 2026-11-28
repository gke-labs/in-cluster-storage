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
	"sync/atomic"
	"testing"
	"time"

	"github.com/gke-labs/in-cluster-storage/pkg/wal/buffer/buffertest"
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
)

type slowCloseStream struct {
	recordingStream
	closeDelay time.Duration
	closeError error
	closeCount atomic.Int64
}

func (s *slowCloseStream) Close() error {
	s.closeCount.Add(1)
	time.Sleep(s.closeDelay)
	return s.closeError
}

func TestServer_Close_ClosesVolumesConcurrently(t *testing.T) {
	const (
		volumeCount      = 8
		streamCloseDelay = 200 * time.Millisecond
		maxCloseDuration = 800 * time.Millisecond
	)

	injectedError := errors.New("stream close failed")
	streamsByVolume := make(map[string]*slowCloseStream, volumeCount)
	for i := range volumeCount {
		volumeID := fmt.Sprintf("volume-%d", i)
		var closeError error
		if i == 3 {
			closeError = injectedError
		}
		streamsByVolume[volumeID] = &slowCloseStream{
			closeDelay: streamCloseDelay,
			closeError: closeError,
		}
	}

	server := NewServer(
		NewMemoryBackend(),
		WithServerMetadataDir(t.TempDir()),
		WithServerStreamFactory(func(volumeID string) (walclient.Stream, error) {
			stream, ok := streamsByVolume[volumeID]
			if !ok {
				return nil, fmt.Errorf("unexpected volume %s", volumeID)
			}
			return stream, nil
		}),
	)
	if err := server.Start(t.Context()); err != nil {
		t.Fatalf("server.Start failed: %v", err)
	}
	t.Cleanup(func() {
		if err := server.Close(); err != nil && !errors.Is(err, injectedError) {
			t.Errorf("server.Close failed on cleanup: %v", err)
		}
	})

	for i := range volumeCount {
		volumeID := fmt.Sprintf("volume-%d", i)
		if _, err := server.GetVolume(volumeID); err != nil {
			t.Fatalf("GetVolume(%s) failed: %v", volumeID, err)
		}
	}

	start := time.Now()
	closeError := server.Close()
	elapsed := time.Since(start)

	if !errors.Is(closeError, injectedError) {
		t.Fatalf("expected Server.Close error to contain %v, got %v", injectedError, closeError)
	}
	for volumeID, stream := range streamsByVolume {
		if calls := stream.closeCount.Load(); calls != 1 {
			t.Errorf("stream %s: expected Close to be called once, got %d", volumeID, calls)
		}
	}
	if elapsed >= maxCloseDuration {
		t.Fatalf("Server.Close took %v, want less than %v", elapsed, maxCloseDuration)
	}
}

func TestServer_ShutdownReleasesWriterLockForRestartedController(t *testing.T) {
	ctx := t.Context()
	backend := NewMemoryBackend()
	clock := &testClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	bufferTest, _ := buffertest.Start(ctx, t,
		buffertest.WithBackend(backend),
		buffertest.WithWriterLockGrace(15*time.Second),
		buffertest.WithClock(clock),
	)

	startServer := func(extraOptions ...ServerOption) *Server {
		options := append([]ServerOption{
			WithServerWAL(t.TempDir(), bufferTest.Target, walclient.Witness),
			WithServerWALClient(bufferTest.Client),
			WithServerWALDialOptions(bufferTest.DialOpts...),
			WithServerMetadataDir(t.TempDir()),
			WithServerWithoutFsync(),
		}, extraOptions...)
		server := NewServer(backend, options...)
		if err := server.Start(ctx); err != nil {
			t.Fatalf("server.Start failed: %v", err)
		}
		t.Cleanup(func() {
			if err := server.Close(); err != nil {
				t.Errorf("server.Close failed: %v", err)
			}
		})
		return server
	}

	volumeID := "test-restart-writer-lock-volume"
	server1 := startServer()
	data := []byte("persisted before clean shutdown")
	if response, err := testCreateFile(ctx, server1, volumeID, "/hello.txt", 0644, data, 0, 0); err != nil || response.GetError() != 0 {
		t.Fatalf("CreateFile failed: %v (code %d)", err, response.GetError())
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(ctx, 5*time.Second)
	defer shutdownCancel()
	if err := server1.Shutdown(shutdownCtx, 5*time.Second, time.Second); err != nil {
		t.Fatalf("server1.Shutdown failed: %v", err)
	}

	// Second controller with a distinct client ID; the clock does not advance, so the
	// first read succeeds at once only because server1 released the writer lock on shutdown.
	walDirectory2 := t.TempDir()
	server2 := startServer(WithServerStreamFactory(func(volumeID string) (walclient.Stream, error) {
		return walclient.Open(ctx, walDirectory2, StreamIDForVolume(volumeID), bufferTest.Target,
			walclient.WithGRPCClient(bufferTest.Client),
			walclient.WithDialOptions(bufferTest.DialOpts...),
			walclient.WithoutFsync(),
			walclient.WithClientID("controller-restarted"),
		)
	}))

	response, err := testReadFile(ctx, server2, volumeID, "/hello.txt", 0, int64(len(data)))
	if err != nil || response.GetError() != 0 || !bytes.Equal(response.GetData(), data) {
		t.Fatalf("ReadFile on restarted server failed: err=%v code=%d data=%q", err, response.GetError(), string(response.GetData()))
	}
}
