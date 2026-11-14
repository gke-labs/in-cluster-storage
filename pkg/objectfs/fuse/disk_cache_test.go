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

package fuse

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"google.golang.org/grpc"
)

type mockBlobControllerClient struct {
	pb.ObjectFSControllerClient
	mu       sync.Mutex
	blobs    map[string][]byte
	getCalls atomic.Int64
}

type mockBlobStream struct {
	grpc.ClientStream
	data   []byte
	offset int
}

func (s *mockBlobStream) Recv() (*pb.GetBlobResponse, error) {
	if s.offset >= len(s.data) {
		return nil, io.EOF
	}
	end := s.offset + 1024
	if end > len(s.data) {
		end = len(s.data)
	}
	chunk := s.data[s.offset:end]
	s.offset = end
	return &pb.GetBlobResponse{Data: chunk}, nil
}

func (m *mockBlobControllerClient) GetBlob(ctx context.Context, in *pb.GetBlobRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[pb.GetBlobResponse], error) {
	m.getCalls.Add(1)
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.blobs[in.GetSha256()]
	if !ok {
		return nil, fmt.Errorf("blob not found: %s", in.GetSha256())
	}
	return &mockBlobStream{data: data}, nil
}

func (m *mockBlobControllerClient) ListBlobs(ctx context.Context, in *pb.ListBlobsRequest, opts ...grpc.CallOption) (*pb.ListBlobsResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var shas []string
	for sha := range m.blobs {
		shas = append(shas, sha)
	}
	return &pb.ListBlobsResponse{Sha256: shas, EndOfData: true}, nil
}

func shaString(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func TestDiskBlobCachePutAndRead(t *testing.T) {
	ctx := t.Context()
	tempDir, err := os.MkdirTemp("", "disk-cache-put-read-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	cache, err := NewDiskBlobCache(tempDir, 10*1024*1024)
	if err != nil {
		t.Fatalf("NewDiskBlobCache failed: %v", err)
	}
	defer cache.Close()

	data := []byte("hello world, persistent disk blob cache!")
	sha := shaString(data)

	if cache.HasBlob(sha) {
		t.Fatalf("expected HasBlob to return false before put")
	}

	if err := cache.PutBlob(ctx, sha, data); err != nil {
		t.Fatalf("PutBlob failed: %v", err)
	}

	if !cache.HasBlob(sha) {
		t.Fatalf("expected HasBlob to return true after put")
	}

	readBytes, err := cache.ReadBlob(ctx, sha)
	if err != nil {
		t.Fatalf("ReadBlob failed: %v", err)
	}
	if !bytes.Equal(readBytes, data) {
		t.Fatalf("ReadBlob returned unexpected data: %q != %q", readBytes, data)
	}
}

func TestDiskBlobCacheVerifyIntegrity(t *testing.T) {
	ctx := t.Context()
	tempDir, err := os.MkdirTemp("", "disk-cache-verify-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	cache, err := NewDiskBlobCache(tempDir, 10*1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	data := []byte("legitimate data")
	wrongSha := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

	err = cache.PutBlob(ctx, wrongSha, data)
	if err == nil {
		t.Fatalf("expected PutBlob to fail on sha mismatch")
	}

	// Verify no stray files left in tmp or blobs
	blobs, _ := os.ReadDir(filepath.Join(tempDir, "blobs"))
	if len(blobs) != 0 {
		t.Fatalf("expected 0 blobs on mismatch, got %d", len(blobs))
	}
	tmps, _ := os.ReadDir(filepath.Join(tempDir, "tmp"))
	if len(tmps) != 0 {
		t.Fatalf("expected 0 tmp files on mismatch, got %d", len(tmps))
	}
}

func TestDiskBlobCachePersistenceAcrossRestart(t *testing.T) {
	ctx := t.Context()
	tempDir, err := os.MkdirTemp("", "disk-cache-restart-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	data1 := []byte("chunk 1 data")
	sha1 := shaString(data1)
	data2 := []byte("chunk 2 data")
	sha2 := shaString(data2)

	// Session 1: create cache, store blobs, close
	cache1, err := NewDiskBlobCache(tempDir, 10*1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	if err := cache1.PutBlob(ctx, sha1, data1); err != nil {
		t.Fatal(err)
	}
	if err := cache1.PutBlob(ctx, sha2, data2); err != nil {
		t.Fatal(err)
	}
	if err := cache1.Close(); err != nil {
		t.Fatal(err)
	}

	// Session 2: reopen same directory, verify blobs and SQLite state
	cache2, err := NewDiskBlobCache(tempDir, 10*1024*1024)
	if err != nil {
		t.Fatalf("reopening cache failed: %v", err)
	}
	defer cache2.Close()

	if !cache2.HasBlob(sha1) || !cache2.HasBlob(sha2) {
		t.Fatalf("expected reopened cache to have both blobs")
	}

	r1, err := cache2.ReadBlob(ctx, sha1)
	if err != nil || !bytes.Equal(r1, data1) {
		t.Fatalf("failed reading blob 1 after restart: %v", err)
	}
	r2, err := cache2.ReadBlob(ctx, sha2)
	if err != nil || !bytes.Equal(r2, data2) {
		t.Fatalf("failed reading blob 2 after restart: %v", err)
	}

	if cache2.CurrentBytes() != int64(len(data1)+len(data2)) {
		t.Fatalf("unexpected cached bytes after restart: got %d, want %d", cache2.CurrentBytes(), len(data1)+len(data2))
	}
}

func TestDiskBlobCacheLRUEviction(t *testing.T) {
	ctx := t.Context()
	tempDir, err := os.MkdirTemp("", "disk-cache-evict-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// 300 byte max capacity
	cache, err := NewDiskBlobCache(tempDir, 300)
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	// Add blob 1 (100 bytes)
	data1 := bytes.Repeat([]byte("1"), 100)
	sha1 := shaString(data1)
	if err := cache.PutBlob(ctx, sha1, data1); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)

	// Add blob 2 (100 bytes)
	data2 := bytes.Repeat([]byte("2"), 100)
	sha2 := shaString(data2)
	if err := cache.PutBlob(ctx, sha2, data2); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)

	// Add blob 3 (100 bytes) -> total 300 bytes, at capacity
	data3 := bytes.Repeat([]byte("3"), 100)
	sha3 := shaString(data3)
	if err := cache.PutBlob(ctx, sha3, data3); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)

	// Access blob 1 so it becomes more recently accessed than blob 2
	if _, err := cache.ReadBlob(ctx, sha1); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)

	// Add blob 4 (100 bytes) -> triggers eviction
	data4 := bytes.Repeat([]byte("4"), 100)
	sha4 := shaString(data4)
	if err := cache.PutBlob(ctx, sha4, data4); err != nil {
		t.Fatal(err)
	}

	// Because blob 2 was accessed least recently, blob 2 must be evicted first
	if cache.HasBlob(sha2) {
		t.Fatalf("expected least recently used blob 2 to be evicted")
	}
	// Blob 1 was recently accessed, should still be present
	if !cache.HasBlob(sha1) {
		t.Fatalf("expected recently read blob 1 to remain in cache")
	}
	// Blob 4 must be present
	if !cache.HasBlob(sha4) {
		t.Fatalf("expected new blob 4 to be in cache")
	}
}

func TestDiskBlobCacheFetchAndSingleflight(t *testing.T) {
	ctx := t.Context()
	tempDir, err := os.MkdirTemp("", "disk-cache-fetch-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	data := bytes.Repeat([]byte("A"), 65536)
	sha := shaString(data)

	mockClient := &mockBlobControllerClient{
		blobs: map[string][]byte{
			sha: data,
		},
	}

	cache, err := NewDiskBlobCache(tempDir, 10*1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	// Launch 10 concurrent fetches for the same blob
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fetched, err := cache.FetchBlob(ctx, mockClient, sha)
			if err != nil {
				t.Errorf("FetchBlob failed: %v", err)
			}
			if !bytes.Equal(fetched, data) {
				t.Errorf("fetched data does not match")
			}
		}()
	}
	wg.Wait()

	// Because of singleflight, GetBlob RPC should only have been called once!
	if calls := mockClient.getCalls.Load(); calls != 1 {
		t.Fatalf("expected exactly 1 GetBlob RPC call across concurrent fetches, got %d", calls)
	}

	// Verify on-disk presence
	if !cache.HasBlob(sha) {
		t.Fatalf("expected blob to be cached on disk")
	}
}

type failCloseWriter struct {
	io.WriteCloser
	closeErr error
}

func (w *failCloseWriter) Close() error {
	_ = w.WriteCloser.Close()
	return w.closeErr
}

func TestDiskBlobCacheFetchCloseError(t *testing.T) {
	ctx := t.Context()
	tempDir, err := os.MkdirTemp("", "disk-cache-close-err-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	data := []byte("test blob data for close error")
	sha := shaString(data)

	mockClient := &mockBlobControllerClient{
		blobs: map[string][]byte{
			sha: data,
		},
	}

	cache, err := NewDiskBlobCache(tempDir, 10*1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	injectedErr := errors.New("simulated flush/close error on writer")
	cache.wrapWriter = func(wc io.WriteCloser) io.WriteCloser {
		return &failCloseWriter{WriteCloser: wc, closeErr: injectedErr}
	}

	_, err = cache.FetchBlob(ctx, mockClient, sha)
	if err == nil {
		t.Fatal("expected FetchBlob to return error on failing Close(), got nil")
	}
	if !errors.Is(err, injectedErr) {
		t.Fatalf("expected error to wrap injectedErr, got: %v", err)
	}

	// Verify no partial files remain in tmp or blobs
	if cache.HasBlob(sha) {
		t.Fatalf("expected HasBlob to return false after failed fetch")
	}
	blobs, err := os.ReadDir(filepath.Join(tempDir, "blobs"))
	if err != nil {
		t.Fatal(err)
	}
	if len(blobs) != 0 {
		t.Fatalf("expected 0 blobs on close failure, got %d", len(blobs))
	}
	tmps, err := os.ReadDir(filepath.Join(tempDir, "tmp"))
	if err != nil {
		t.Fatal(err)
	}
	if len(tmps) != 0 {
		t.Fatalf("expected 0 tmp files on close failure, got %d", len(tmps))
	}

	// Verify subsequent fetch without failing Close succeeds and retries
	cache.wrapWriter = nil
	fetched, err := cache.FetchBlob(ctx, mockClient, sha)
	if err != nil {
		t.Fatalf("expected subsequent FetchBlob to succeed, got: %v", err)
	}
	if !bytes.Equal(fetched, data) {
		t.Fatalf("subsequent FetchBlob returned unexpected data")
	}
}
