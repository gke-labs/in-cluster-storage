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
	"bytes"
	"context"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
)

type sleepingBackend struct {
	ObjectStorageBackend
	sleepDuration time.Duration
	getCalls      atomic.Int64
}

func newSleepingBackend(base ObjectStorageBackend, sleepDuration time.Duration) *sleepingBackend {
	return &sleepingBackend{
		ObjectStorageBackend: base,
		sleepDuration:        sleepDuration,
	}
}

func (b *sleepingBackend) GetObject(ctx context.Context, volumeID, key string, offset, length int64, w io.Writer) error {
	b.getCalls.Add(1)
	if b.sleepDuration > 0 {
		time.Sleep(b.sleepDuration)
	}
	return b.ObjectStorageBackend.GetObject(ctx, volumeID, key, offset, length, w)
}

type gatedGetBackend struct {
	ObjectStorageBackend
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newGatedGetBackend(base ObjectStorageBackend, entered, release chan struct{}) *gatedGetBackend {
	return &gatedGetBackend{
		ObjectStorageBackend: base,
		entered:              entered,
		release:              release,
	}
}

func (b *gatedGetBackend) GetObject(ctx context.Context, volumeID, key string, offset, length int64, writer io.Writer) error {
	if b.entered != nil {
		b.once.Do(func() {
			close(b.entered)
		})
	}
	if b.release != nil {
		select {
		case <-b.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return b.ObjectStorageBackend.GetObject(ctx, volumeID, key, offset, length, writer)
}

func TestParallelReadFile_SleepingBackend(t *testing.T) {
	ctx := t.Context()
	sleepDur := 60 * time.Millisecond
	rawBackend := newSleepingBackend(NewMemoryBackend(), sleepDur)
	server := NewTestServer(t, rawBackend)

	volumeID := "test-parallel-read-vol"
	vol, err := server.getOrCreateVolume(volumeID)
	if err != nil {
		t.Fatalf("getOrCreateVolume failed: %v", err)
	}

	createResp, err := testCreateFile(ctx, server, volumeID, "/test.bin", 0644, nil, 0, 0)
	if err != nil || createResp.GetError() != 0 {
		t.Fatalf("CreateFile failed: %v", err)
	}
	ino := createResp.GetAttr().GetInode().GetIno()

	chunkSize := 64 * 1024
	numChunks := 4
	payload := make([]byte, numChunks*chunkSize)
	for i := range payload {
		payload[i] = byte(i % 251)
	}

	// Write chunks to controller staging
	wResp, err := server.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId:  volumeID,
		Inode:     ino,
		Offset:    0,
		Data:      payload,
		WriteMode: pb.WriteMode_LAZY,
	})
	if err != nil || wResp.GetError() != 0 {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Flush/pack chunks to backend so they reside in packfiles
	fResp, err := server.Fsync(ctx, &pb.FsyncRequest{
		VolumeId: volumeID,
		Inode:    ino,
		Blocking: true,
	})
	if err != nil || fResp.GetError() != 0 {
		t.Fatalf("Fsync failed: %v", err)
	}

	// Reset getCalls counter before ReadFile
	rawBackend.getCalls.Store(0)
	vol.evictAllUploadedPacksForTest()

	start := time.Now()
	data, total, _, err := vol.ReadFile(ctx, ino, 0, int64(len(payload)))
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if total != int64(len(payload)) {
		t.Fatalf("ReadFile total size = %d, want %d", total, len(payload))
	}
	if !bytes.Equal(data, payload) {
		t.Fatalf("ReadFile data mismatch")
	}

	calls := rawBackend.getCalls.Load()
	if calls < int64(numChunks) {
		t.Fatalf("Expected at least %d GetObject calls, got %d", numChunks, calls)
	}

	// Serial read of 4 chunks with 60ms sleep per chunk would take >= 240ms.
	// Parallel fetch bounded to 16 in flight should take ~1 sleep (~60-120ms).
	serialMin := time.Duration(numChunks) * sleepDur
	t.Logf("ReadFile for %d chunks took %v (calls=%d, serial min=%v)", numChunks, elapsed, calls, serialMin)
	if elapsed >= serialMin {
		t.Fatalf("ReadFile was serial: took %v, want < %v", elapsed, serialMin)
	}
}

func TestParallelReadFile_DropVolumeLockDuringBlobIO(t *testing.T) {
	ctx := t.Context()
	entered := make(chan struct{})
	release := make(chan struct{})
	rawBackend := newGatedGetBackend(NewMemoryBackend(), entered, release)
	server := NewTestServer(t, rawBackend)

	volumeID := "test-lock-drop-vol"
	vol, err := server.getOrCreateVolume(volumeID)
	if err != nil {
		t.Fatalf("getOrCreateVolume failed: %v", err)
	}

	// Create file A and file B
	respA, err := testCreateFile(ctx, server, volumeID, "/fileA.bin", 0644, nil, 0, 0)
	if err != nil || respA.GetError() != 0 {
		t.Fatalf("CreateFile A failed: %v", err)
	}
	inoA := respA.GetAttr().GetInode().GetIno()

	respB, err := testCreateFile(ctx, server, volumeID, "/fileB.bin", 0644, nil, 0, 0)
	if err != nil || respB.GetError() != 0 {
		t.Fatalf("CreateFile B failed: %v", err)
	}
	inoB := respB.GetAttr().GetInode().GetIno()

	chunkSize := 64 * 1024
	dataA := bytes.Repeat([]byte("A"), chunkSize)
	dataB := bytes.Repeat([]byte("B"), chunkSize)

	if _, err := server.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId: volumeID, Inode: inoA, Offset: 0, Data: dataA, WriteMode: pb.WriteMode_LAZY,
	}); err != nil {
		t.Fatalf("WriteFile A failed: %v", err)
	}
	if _, err := server.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId: volumeID, Inode: inoB, Offset: 0, Data: dataB, WriteMode: pb.WriteMode_LAZY,
	}); err != nil {
		t.Fatalf("WriteFile B failed: %v", err)
	}

	if _, err := server.Fsync(ctx, &pb.FsyncRequest{VolumeId: volumeID, Inode: inoA, Blocking: true}); err != nil {
		t.Fatalf("Fsync failed: %v", err)
	}
	vol.evictAllUploadedPacksForTest()

	readADone := make(chan error, 1)

	go func() {
		_, _, _, readError := vol.ReadFile(ctx, inoA, 0, int64(len(dataA)))
		readADone <- readError
	}()

	select {
	case <-entered:
	case readErrorA := <-readADone:
		t.Fatalf("ReadFile A finished before entering backend GetObject: %v", readErrorA)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for file A to enter backend GetObject")
	}

	// An independent volume operation (GetAttr) must NOT block on the backend I/O of file A
	getAttrDone := make(chan error, 1)
	var attrB *pb.EntryAttr
	go func() {
		var getAttrError error
		attrB, getAttrError = vol.GetAttr(ctx, inoB)
		getAttrDone <- getAttrError
	}()

	select {
	case getAttrError := <-getAttrDone:
		if getAttrError != nil {
			t.Fatalf("GetAttr B failed: %v", getAttrError)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("GetAttr B blocked on volume lock during blob I/O")
	}
	if attrB.GetInode().GetSize() != int64(len(dataB)) {
		t.Fatalf("GetAttr B size = %d, want %d", attrB.GetInode().GetSize(), len(dataB))
	}

	close(release)

	select {
	case readErrorA := <-readADone:
		if readErrorA != nil {
			t.Fatalf("ReadFile A failed: %v", readErrorA)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for ReadFile A to complete")
	}
}

func TestWriteFileWithEpoch_DoesNotHoldStagingMuDuringBaseChunkGet(t *testing.T) {
	ctx := t.Context()
	entered := make(chan struct{})
	release := make(chan struct{})
	rawBackend := newGatedGetBackend(NewMemoryBackend(), entered, release)
	server := NewTestServer(t, rawBackend)

	volumeID := "test-staging-lock-drop-vol"
	vol, err := server.getOrCreateVolume(volumeID)
	if err != nil {
		t.Fatalf("getOrCreateVolume failed: %v", err)
	}

	// Create file A and file B
	respA, err := testCreateFile(ctx, server, volumeID, "/fileA.bin", 0644, nil, 0, 0)
	if err != nil || respA.GetError() != 0 {
		t.Fatalf("CreateFile A failed: %v", err)
	}
	inoA := respA.GetAttr().GetInode().GetIno()

	respB, err := testCreateFile(ctx, server, volumeID, "/fileB.bin", 0644, nil, 0, 0)
	if err != nil || respB.GetError() != 0 {
		t.Fatalf("CreateFile B failed: %v", err)
	}
	inoB := respB.GetAttr().GetInode().GetIno()

	chunkSize := 64 * 1024
	dataA := bytes.Repeat([]byte("A"), chunkSize)
	if _, err := server.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId: volumeID, Inode: inoA, Offset: 0, Data: dataA, WriteMode: pb.WriteMode_LAZY,
	}); err != nil {
		t.Fatalf("WriteFile A failed: %v", err)
	}
	if _, err := server.Fsync(ctx, &pb.FsyncRequest{VolumeId: volumeID, Inode: inoA, Blocking: true}); err != nil {
		t.Fatalf("Fsync failed: %v", err)
	}
	vol.evictAllUploadedPacksForTest()

	writeADone := make(chan error, 1)

	// Partial overwrite of file A chunk 0: requires fetching base chunk from backend
	go func() {
		patch := []byte("XXXX")
		_, _, _, _, writeError := vol.WriteFileWithEpoch(ctx, inoA, 100, patch, pb.WriteMode_LAZY)
		writeADone <- writeError
	}()

	select {
	case <-entered:
	case writeErrorA := <-writeADone:
		t.Fatalf("WriteFileWithEpoch A finished before entering backend GetObject: %v", writeErrorA)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for file A to enter backend GetObject")
	}

	// An independent write to file B must NOT block on stagingMu while file A fetches base chunk
	writeBDone := make(chan error, 1)
	go func() {
		dataB := []byte("fast write to file B")
		_, _, _, _, writeError := vol.WriteFileWithEpoch(ctx, inoB, 0, dataB, pb.WriteMode_LAZY)
		writeBDone <- writeError
	}()

	select {
	case writeErrorB := <-writeBDone:
		if writeErrorB != nil {
			t.Fatalf("WriteFileWithEpoch B failed: %v", writeErrorB)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("WriteFileWithEpoch B blocked on stagingMu during base chunk GET")
	}

	close(release)

	select {
	case writeErrorA := <-writeADone:
		if writeErrorA != nil {
			t.Fatalf("WriteFileWithEpoch A failed: %v", writeErrorA)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for WriteFileWithEpoch A to complete")
	}
}
