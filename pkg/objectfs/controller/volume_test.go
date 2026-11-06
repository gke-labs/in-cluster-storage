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
	"errors"
	"fmt"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore/inmemorystorage"
	"github.com/gke-labs/in-cluster-storage/pkg/sds"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/record"
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
)

func TestMetadataStreamTypeResolution_ReversedTableOrder(t *testing.T) {
	broadcaster := NewEventBroadcaster()
	volumeID := "vol-reversed-table-test"
	vol1 := MustNewVolume(t, volumeID, nil, broadcaster)

	ctx := t.Context()
	if _, err := vol1.CreateFile(ctx, 1, "test.txt", 0644, []byte("data"), 0, 0, 0); err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}
	if err := vol1.PersistConsistencyMode(ctx, pb.ConsistencyMode_CLOSE_TO_OPEN); err != nil {
		t.Fatalf("PersistConsistencyMode failed: %v", err)
	}
	if err := vol1.SetXAttr(ctx, 1, "user.tag", []byte("val"), 0); err != nil {
		t.Fatalf("SetXAttr failed: %v", err)
	}
	if err := vol1.stream.Flush(ctx); err != nil {
		t.Fatalf("stream.Flush failed: %v", err)
	}

	initialRegistry := vol1.metadataStream.Registry()
	stream := vol1.stream
	if err := vol1.Close(); err != nil {
		t.Fatalf("vol1.Close failed: %v", err)
	}

	originalTypes := append([]metadataStreamType(nil), metadataStreamTypes...)
	defer func() { metadataStreamTypes = originalTypes }()
	reversed := make([]metadataStreamType, len(metadataStreamTypes))
	for i, entry := range metadataStreamTypes {
		reversed[len(metadataStreamTypes)-1-i] = entry
	}
	metadataStreamTypes = reversed

	vol2 := MustNewVolume(t, volumeID, nil, broadcaster, WithStream(stream))
	t.Cleanup(func() {
		if closeErr := vol2.Close(); closeErr != nil {
			t.Errorf("vol2.Close failed: %v", closeErr)
		}
	})

	reversedRegistry := vol2.metadataStream.Registry()
	streamRecordTypes := []string{
		InodeTypeName,
		"objectfs.v1alpha1.DirEntry",
		"objectfs.v1alpha1.FileChunk",
		"objectfs.v1alpha1.XAttr",
		VolumeInfoTypeName,
	}
	for _, name := range streamRecordTypes {
		oldDefinition, _, okOld := initialRegistry.LookupByName(name)
		newDefinition, _, okNew := reversedRegistry.LookupByName(name)
		if !okOld || !okNew {
			t.Fatalf("type %s missing in registry", name)
		}
		if oldDefinition.GetId() != newDefinition.GetId() {
			t.Errorf("type %s ID mismatch: old=%d, new=%d", name, oldDefinition.GetId(), newDefinition.GetId())
		}
	}

	attr, err := vol2.GetAttr(ctx, 1)
	if err != nil || !attr.GetInode().GetIsDir() {
		t.Fatalf("GetAttr root inode failed: attr=%v, err=%v", attr, err)
	}
	entries, err := vol2.ReadDir(ctx, 1)
	if err != nil || len(entries) != 1 || entries[0].GetName() != "test.txt" {
		t.Fatalf("ReadDir failed or unexpected entries: %v, %v", entries, err)
	}
	xattrValue, err := vol2.GetXAttr(ctx, 1, "user.tag")
	if err != nil || string(xattrValue) != "val" {
		t.Fatalf("GetXAttr failed or mismatch: val=%q, err=%v", string(xattrValue), err)
	}
	if vol2.ConsistencyMode() != pb.ConsistencyMode_CLOSE_TO_OPEN {
		t.Errorf("got consistency mode %v, want CLOSE_TO_OPEN", vol2.ConsistencyMode())
	}
}

func TestMetadataStreamTypeResolution_SecondWriterRegistersOnlyVolumeInfo(t *testing.T) {
	broadcaster := NewEventBroadcaster()
	vol := MustNewVolume(t, "vol-second-writer-test", nil, broadcaster)
	t.Cleanup(func() {
		if closeErr := vol.Close(); closeErr != nil {
			t.Errorf("vol.Close failed: %v", closeErr)
		}
	})

	ctx := t.Context()
	if _, err := vol.CreateFile(ctx, 1, "test.txt", 0644, []byte("data"), 0, 0, 0); err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}
	if err := vol.PersistConsistencyMode(ctx, pb.ConsistencyMode_CLOSE_TO_OPEN); err != nil {
		t.Fatalf("PersistConsistencyMode failed: %v", err)
	}
	if err := vol.stream.Flush(ctx); err != nil {
		t.Fatalf("stream.Flush failed: %v", err)
	}

	volumeInfoDefinition, _, ok := vol.metadataStream.Registry().LookupByName(VolumeInfoTypeName)
	if !ok {
		t.Fatalf("expected %s in registry", VolumeInfoTypeName)
	}
	expectedID := volumeInfoDefinition.GetId()

	localHead, witnessHead, _ := vol.stream.Watermarks()
	targetHead := witnessHead
	if targetHead == 0 {
		targetHead = localHead
	}

	secondWriter := sds.NewWriter(sds.NewWALAppender(vol.stream, walclient.Local))
	decoder := record.NewDecoder(record.WithDecoderRegistry(secondWriter.Registry()))
	tailIterator, err := vol.stream.Tail(ctx, 0)
	if err != nil {
		t.Fatalf("Tail failed: %v", err)
	}
	for seq, payload := range tailIterator {
		if _, decodeErr := decoder.Decode(payload); decodeErr != nil {
			t.Fatalf("Decode failed: %v", decodeErr)
		}
		if seq >= targetHead {
			break
		}
	}

	typeID, err := secondWriter.RegisterType(&pb.VolumeInfo{}, 1)
	if err != nil {
		t.Fatalf("RegisterType failed: %v", err)
	}
	if typeID != expectedID {
		t.Errorf("second writer got VolumeInfo ID %d, want stream ID %d", typeID, expectedID)
	}
	if typeID == 16 {
		t.Errorf("second writer got ID 16 instead of stream-defined ID %d", expectedID)
	}
}

func TestMetadataStreamTypeResolution_NewStreamRoundTrip(t *testing.T) {
	broadcaster := NewEventBroadcaster()
	vol := MustNewVolume(t, "vol-new-stream-roundtrip", nil, broadcaster)
	t.Cleanup(func() {
		if closeErr := vol.Close(); closeErr != nil {
			t.Errorf("vol.Close failed: %v", closeErr)
		}
	})

	ctx := t.Context()
	fileAttr, err := vol.CreateFile(ctx, 1, "roundtrip.txt", 0644, []byte("roundtrip content"), 0, 0, 0)
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}
	if err := vol.stream.Flush(ctx); err != nil {
		t.Fatalf("stream.Flush failed: %v", err)
	}

	localHead, witnessHead, _ := vol.stream.Watermarks()
	targetHead := witnessHead
	if targetHead == 0 {
		targetHead = localHead
	}

	tailIterator, err := vol.stream.Tail(ctx, 0)
	if err != nil {
		t.Fatalf("Tail failed: %v", err)
	}

	freshRegistry := record.NewRegistry()
	decoder := record.NewDecoder(record.WithDecoderRegistry(freshRegistry))
	changeReader := sds.NewChangeReader(record.WithDecoderRegistry(freshRegistry))

	var typeDefinitionCount int
	var changesCount int
	for seq, payload := range tailIterator {
		rec, decodeErr := decoder.Decode(payload)
		if decodeErr != nil {
			t.Fatalf("failed decoding record at seq %d: %v", seq, decodeErr)
		}
		if rec.TypeID == record.TypeIDTypeDefinition {
			typeDefinitionCount++
		}
		changes, feedErr := changeReader.Feed(seq, payload)
		if feedErr != nil {
			t.Fatalf("failed feeding record at seq %d: %v", seq, feedErr)
		}
		changesCount += len(changes)
		if seq >= targetHead {
			break
		}
	}

	if typeDefinitionCount == 0 || changesCount == 0 {
		t.Fatalf("expected TypeDefinitions (%d) and changes (%d)", typeDefinitionCount, changesCount)
	}

	attr, err := vol.GetAttr(ctx, fileAttr.GetInode().GetIno())
	if err != nil {
		t.Fatalf("GetAttr failed: %v", err)
	}
	if attr.GetInode().GetSize() != int64(len("roundtrip content")) {
		t.Errorf("size mismatch: got %d, want %d", attr.GetInode().GetSize(), len("roundtrip content"))
	}
}

func TestConcurrentGetAttrLookup_ColdVolume_NoDataRace(t *testing.T) {
	ctx := t.Context()
	storageBackend := inmemorystorage.New()
	broadcaster := NewEventBroadcaster()
	volumeID := "test-cold-volume-race"
	localDirectory := t.TempDir()

	initialVolume := MustNewVolume(t, volumeID, storageBackend, broadcaster,
		WithMetadataIndex("sqlite"),
		WithLocalStorageDir(localDirectory),
	)
	if err := initialVolume.LoadFromBackend(ctx); err != nil {
		t.Fatalf("LoadFromBackend failed on initial volume: %v", err)
	}

	var directoryInodeIDs []uint64
	var directoryNames []string
	for index := 0; index < 4; index++ {
		directoryName := fmt.Sprintf("test_dir_%d", index)
		entryAttribute, err := initialVolume.Mkdir(ctx, 1, directoryName, 0755, 0, 0)
		if err != nil {
			t.Fatalf("Mkdir %s failed: %v", directoryName, err)
		}
		directoryInodeIDs = append(directoryInodeIDs, entryAttribute.GetInode().GetIno())
		directoryNames = append(directoryNames, directoryName)
	}

	if err := initialVolume.Close(); err != nil {
		t.Fatalf("Close failed on initial volume: %v", err)
	}

	coldVolume := MustNewVolume(t, volumeID, storageBackend, broadcaster,
		WithMetadataIndex("sqlite"),
		WithLocalStorageDir(localDirectory),
	)
	t.Cleanup(func() {
		if closeErr := coldVolume.Close(); closeErr != nil {
			t.Errorf("coldVolume.Close failed: %v", closeErr)
		}
	})
	if err := coldVolume.LoadFromBackend(ctx); err != nil {
		t.Fatalf("LoadFromBackend failed on cold volume: %v", err)
	}

	const workerCount = 16
	const iterationsPerWorker = 50
	var waitGroup sync.WaitGroup
	startSignal := make(chan struct{})

	for workerIndex := 0; workerIndex < workerCount; workerIndex++ {
		waitGroup.Add(1)
		go func(workerID int) {
			defer waitGroup.Done()
			<-startSignal
			for iteration := 0; iteration < iterationsPerWorker; iteration++ {
				targetIndex := (workerID + iteration) % len(directoryInodeIDs)
				entryAttribute, err := coldVolume.GetAttr(ctx, directoryInodeIDs[targetIndex])
				if err != nil {
					t.Errorf("GetAttr failed for inode %d: %v", directoryInodeIDs[targetIndex], err)
					return
				}
				if !entryAttribute.GetInode().GetIsDir() {
					t.Errorf("expected directory for inode %d", directoryInodeIDs[targetIndex])
					return
				}

				lookupAttribute, err := coldVolume.Lookup(ctx, 1, directoryNames[targetIndex])
				if err != nil {
					t.Errorf("Lookup failed for %s: %v", directoryNames[targetIndex], err)
					return
				}
				if lookupAttribute.GetInode().GetIno() != directoryInodeIDs[targetIndex] {
					t.Errorf("Lookup ino mismatch for %s: got %d, want %d", directoryNames[targetIndex], lookupAttribute.GetInode().GetIno(), directoryInodeIDs[targetIndex])
					return
				}
			}
		}(workerIndex)
	}

	close(startSignal)
	waitGroup.Wait()
}

func TestRename_DirectoryIntoOwnSubtree_ColdVolumeFailsWithEINVAL(t *testing.T) {
	ctx := t.Context()
	storageBackend := inmemorystorage.New()
	broadcaster := NewEventBroadcaster()
	volumeID := "test-cold-rename-cycle"
	localDirectory := t.TempDir()

	initialVolume := MustNewVolume(t, volumeID, storageBackend, broadcaster,
		WithMetadataIndex("sqlite"),
		WithLocalStorageDir(localDirectory),
	)
	if err := initialVolume.LoadFromBackend(ctx); err != nil {
		t.Fatalf("LoadFromBackend failed on initial volume: %v", err)
	}

	entryAttributeA, err := initialVolume.Mkdir(ctx, 1, "a", 0755, 0, 0)
	if err != nil {
		t.Fatalf("Mkdir a failed: %v", err)
	}
	directoryInodeIDA := entryAttributeA.GetInode().GetIno()

	entryAttributeB, err := initialVolume.Mkdir(ctx, directoryInodeIDA, "b", 0755, 0, 0)
	if err != nil {
		t.Fatalf("Mkdir b failed: %v", err)
	}
	directoryInodeIDB := entryAttributeB.GetInode().GetIno()

	entryAttributeC, err := initialVolume.Mkdir(ctx, directoryInodeIDB, "c", 0755, 0, 0)
	if err != nil {
		t.Fatalf("Mkdir c failed: %v", err)
	}
	directoryInodeIDC := entryAttributeC.GetInode().GetIno()

	if err := initialVolume.Close(); err != nil {
		t.Fatalf("Close failed on initial volume: %v", err)
	}

	coldVolume := MustNewVolume(t, volumeID, storageBackend, broadcaster,
		WithMetadataIndex("sqlite"),
		WithLocalStorageDir(localDirectory),
	)
	t.Cleanup(func() {
		if closeErr := coldVolume.Close(); closeErr != nil {
			t.Errorf("coldVolume.Close failed: %v", closeErr)
		}
	})
	if err := coldVolume.LoadFromBackend(ctx); err != nil {
		t.Fatalf("LoadFromBackend failed on cold volume: %v", err)
	}

	// Renaming a into its direct child b must fail with EINVAL.
	_, renameToChildError := coldVolume.Rename(ctx, 1, "a", directoryInodeIDB, "loop")
	if !errors.Is(renameToChildError, syscall.EINVAL) {
		t.Fatalf("expected EINVAL when renaming a into child b, got: %v", renameToChildError)
	}

	// Renaming a into its grandchild c must fail with EINVAL.
	_, renameToGrandchildError := coldVolume.Rename(ctx, 1, "a", directoryInodeIDC, "loop")
	if !errors.Is(renameToGrandchildError, syscall.EINVAL) {
		t.Fatalf("expected EINVAL when renaming a into grandchild c, got: %v", renameToGrandchildError)
	}
}

func TestRename_DirectoryAncestryCycle_FailsWithELOOP(t *testing.T) {
	ctx := t.Context()
	storageBackend := inmemorystorage.New()
	broadcaster := NewEventBroadcaster()
	volumeID := "test-rename-cycle-guard"
	localDirectory := t.TempDir()

	initialVolume := MustNewVolume(t, volumeID, storageBackend, broadcaster,
		WithMetadataIndex("sqlite"),
		WithLocalStorageDir(localDirectory),
	)
	if err := initialVolume.LoadFromBackend(ctx); err != nil {
		t.Fatalf("LoadFromBackend failed on initial volume: %v", err)
	}

	entryAttributeA, err := initialVolume.Mkdir(ctx, 1, "dir_a", 0755, 0, 0)
	if err != nil {
		t.Fatalf("Mkdir dir_a failed: %v", err)
	}
	directoryInodeIDA := entryAttributeA.GetInode().GetIno()

	entryAttributeB, err := initialVolume.Mkdir(ctx, 1, "dir_b", 0755, 0, 0)
	if err != nil {
		t.Fatalf("Mkdir dir_b failed: %v", err)
	}
	directoryInodeIDB := entryAttributeB.GetInode().GetIno()

	_, err = initialVolume.Mkdir(ctx, 1, "dir_target", 0755, 0, 0)
	if err != nil {
		t.Fatalf("Mkdir dir_target failed: %v", err)
	}

	if err := initialVolume.Close(); err != nil {
		t.Fatalf("Close failed on initial volume: %v", err)
	}

	coldVolume := MustNewVolume(t, volumeID, storageBackend, broadcaster,
		WithMetadataIndex("sqlite"),
		WithLocalStorageDir(localDirectory),
	)
	t.Cleanup(func() {
		if closeErr := coldVolume.Close(); closeErr != nil {
			t.Errorf("coldVolume.Close failed: %v", closeErr)
		}
	})
	if err := coldVolume.LoadFromBackend(ctx); err != nil {
		t.Fatalf("LoadFromBackend failed on cold volume: %v", err)
	}

	// Synthesize a cycle where dir_a's parent is dir_b and dir_b's parent is dir_a.
	inodeRowA := &pb.Inode{
		Ino:       proto.Uint64(directoryInodeIDA),
		Mode:      0755 | syscall.S_IFDIR,
		IsDir:     true,
		Nlink:     2,
		ParentIno: proto.Uint64(directoryInodeIDB),
	}
	keyBytesA, valueBytesA, err := sds.SplitKeyAndNonKey(inodeRowA, []int32{1})
	if err != nil {
		t.Fatalf("SplitKeyAndNonKey inode A failed: %v", err)
	}

	inodeRowB := &pb.Inode{
		Ino:       proto.Uint64(directoryInodeIDB),
		Mode:      0755 | syscall.S_IFDIR,
		IsDir:     true,
		Nlink:     2,
		ParentIno: proto.Uint64(directoryInodeIDA),
	}
	keyBytesB, valueBytesB, err := sds.SplitKeyAndNonKey(inodeRowB, []int32{1})
	if err != nil {
		t.Fatalf("SplitKeyAndNonKey inode B failed: %v", err)
	}

	cycleChanges := []sds.Change{
		{
			TypeName: InodeTypeName,
			Op:       sds.OpUpdate,
			Key:      sds.NewKeyFromBytes(keyBytesA),
			RawKey:   keyBytesA,
			RawVal:   valueBytesA,
			Row:      inodeRowA,
		},
		{
			TypeName: InodeTypeName,
			Op:       sds.OpUpdate,
			Key:      sds.NewKeyFromBytes(keyBytesB),
			RawKey:   keyBytesB,
			RawVal:   valueBytesB,
			Row:      inodeRowB,
		},
	}
	if err := coldVolume.metadataView.ApplyChangesSync(ctx, cycleChanges); err != nil {
		t.Fatalf("ApplyChangesSync failed: %v", err)
	}
	coldVolume.metadataView.ClearCache()

	// Attempting to move dir_target into dir_a must fail with ELOOP without hanging.
	renameDoneChannel := make(chan error, 1)
	go func() {
		_, renameError := coldVolume.Rename(ctx, 1, "dir_target", directoryInodeIDA, "dir_target")
		renameDoneChannel <- renameError
	}()

	select {
	case renameError := <-renameDoneChannel:
		if !errors.Is(renameError, syscall.ELOOP) {
			t.Fatalf("expected error wrapping syscall.ELOOP, got: %v", renameError)
		}
		if !strings.Contains(renameError.Error(), "directory ancestry forms a cycle") {
			t.Fatalf("expected error to mention 'directory ancestry forms a cycle', got: %v", renameError)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Rename hung due to cycle in directory ancestry")
	}
}
