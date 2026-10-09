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

package table_test

import (
	"bytes"
	"errors"
	"testing"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/blob"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore/inmemorystorage"
	"github.com/gke-labs/in-cluster-storage/pkg/sds"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/projection/table"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

func newTestDescriptor(t *testing.T, name string) protoreflect.MessageDescriptor {
	t.Helper()
	msgProto := &descriptorpb.DescriptorProto{
		Name: proto.String(name),
		Field: []*descriptorpb.FieldDescriptorProto{
			{
				Name:   proto.String("id"),
				Number: proto.Int32(1),
				Type:   descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(),
				Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
			},
			{
				Name:   proto.String("value"),
				Number: proto.Int32(2),
				Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
				Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
			},
		},
	}
	fileProto := &descriptorpb.FileDescriptorProto{
		Name:        proto.String(name + ".proto"),
		Package:     proto.String("testpkg"),
		MessageType: []*descriptorpb.DescriptorProto{msgProto},
		Syntax:      proto.String("proto2"),
	}
	fds := &descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{fileProto},
	}
	files, err := protodesc.NewFiles(fds)
	if err != nil {
		t.Fatalf("protodesc.NewFiles failed: %v", err)
	}
	d, err := files.FindDescriptorByName(protoreflect.FullName("testpkg." + name))
	if err != nil {
		t.Fatalf("FindDescriptorByName failed: %v", err)
	}
	return d.(protoreflect.MessageDescriptor)
}

func TestTableBasicCRUDAndPosition(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()

	db, err := table.Open(ctx, dir, table.WithStreamID("stream-basic"))
	if err != nil {
		t.Fatalf("table.Open failed: %v", err)
	}
	defer db.Close()

	md := newTestDescriptor(t, "Item")
	def, err := db.RegisterType(ctx, dynamicpb.NewMessage(md), 1)
	if err != nil {
		t.Fatalf("RegisterType failed: %v", err)
	}

	pk := sds.NewPrimaryKey(1)

	// Create row 1
	msg1 := dynamicpb.NewMessage(md)
	msg1.Set(md.Fields().ByName("id"), protoreflect.ValueOfInt64(100))
	msg1.Set(md.Fields().ByName("value"), protoreflect.ValueOfString("hello"))
	k1Bytes, v1Bytes, err := pk.Split(msg1)
	if err != nil {
		t.Fatalf("pk.Split failed: %v", err)
	}

	ch1 := sds.Change{
		Op:       sds.OpCreate,
		TypeID:   def.GetId(),
		TypeName: def.GetName(),
		RawKey:   k1Bytes,
		RawVal:   v1Bytes,
		Seq:      10,
	}

	if err := db.ApplyBatch(ctx, []sds.Change{ch1}); err != nil {
		t.Fatalf("ApplyBatch failed: %v", err)
	}

	if db.Position() != 10 {
		t.Fatalf("Position = %d, want 10", db.Position())
	}

	// Read row 1
	got1, found, err := db.Get(ctx, def.GetName(), sds.NewKeyFromBytes(k1Bytes))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !found {
		t.Fatalf("expected row 1 to be found")
	}
	gotDyn := got1.(*dynamicpb.Message)
	if gotDyn.Get(gotDyn.Descriptor().Fields().ByName("value")).String() != "hello" {
		t.Fatalf("got value %q, want hello", gotDyn.Get(gotDyn.Descriptor().Fields().ByName("value")).String())
	}

	// Update row 1
	msg1Update := dynamicpb.NewMessage(md)
	msg1Update.Set(md.Fields().ByName("id"), protoreflect.ValueOfInt64(100))
	msg1Update.Set(md.Fields().ByName("value"), protoreflect.ValueOfString("world"))
	_, v1UpdateBytes, _ := pk.Split(msg1Update)

	ch1Update := sds.Change{
		Op:       sds.OpUpdate,
		TypeID:   def.GetId(),
		TypeName: def.GetName(),
		RawKey:   k1Bytes,
		RawVal:   v1UpdateBytes,
		Seq:      12,
	}
	if err := db.Apply(ctx, ch1Update); err != nil {
		t.Fatalf("Apply update failed: %v", err)
	}
	if db.Position() != 12 {
		t.Fatalf("Position = %d, want 12", db.Position())
	}

	got1Up, found, err := db.Get(ctx, def.GetName(), sds.NewKeyFromBytes(k1Bytes))
	if err != nil || !found {
		t.Fatalf("Get updated failed: found=%v, err=%v", found, err)
	}
	got1UpDyn := got1Up.(*dynamicpb.Message)
	if got1UpDyn.Get(got1UpDyn.Descriptor().Fields().ByName("value")).String() != "world" {
		t.Fatalf("expected updated value 'world'")
	}

	// Delete row 1
	ch1Del := sds.Change{
		Op:       sds.OpDelete,
		TypeID:   def.GetId(),
		TypeName: def.GetName(),
		RawKey:   k1Bytes,
		Seq:      15,
	}
	if err := db.Apply(ctx, ch1Del); err != nil {
		t.Fatalf("Apply delete failed: %v", err)
	}
	if db.Position() != 15 {
		t.Fatalf("Position = %d, want 15", db.Position())
	}

	got1Del, found, err := db.Get(ctx, def.GetName(), sds.NewKeyFromBytes(k1Bytes))
	if err != nil {
		t.Fatalf("Get after delete failed: %v", err)
	}
	if found || got1Del != nil {
		t.Fatalf("expected row 1 to be not found after delete")
	}
}

func TestTableReopenPersistence(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()

	// 1. Open and write
	db, err := table.Open(ctx, dir, table.WithStreamID("stream-persist"))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	md := newTestDescriptor(t, "Entry")
	def, err := db.RegisterType(ctx, dynamicpb.NewMessage(md), 1)
	if err != nil {
		t.Fatalf("RegisterType failed: %v", err)
	}

	pk := sds.NewPrimaryKey(1)
	msg := dynamicpb.NewMessage(md)
	msg.Set(md.Fields().ByName("id"), protoreflect.ValueOfInt64(42))
	msg.Set(md.Fields().ByName("value"), protoreflect.ValueOfString("persisted-value"))
	kBytes, vBytes, _ := pk.Split(msg)

	err = db.Apply(ctx, sds.Change{
		Op:       sds.OpCreate,
		TypeID:   def.GetId(),
		TypeName: def.GetName(),
		RawKey:   kBytes,
		RawVal:   vBytes,
		Seq:      99,
	})
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// 2. Reopen and verify state, position, and registry
	db2, err := table.Open(ctx, dir)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer db2.Close()

	if db2.StreamID() != "stream-persist" {
		t.Fatalf("StreamID = %q, want stream-persist", db2.StreamID())
	}
	if db2.Position() != 99 {
		t.Fatalf("Position = %d, want 99", db2.Position())
	}

	got, found, err := db2.Get(ctx, def.GetName(), sds.NewKeyFromBytes(kBytes))
	if err != nil || !found {
		t.Fatalf("Get after reopen failed: found=%v, err=%v", found, err)
	}
	gotDynReopen := got.(*dynamicpb.Message)
	if gotDynReopen.Get(gotDynReopen.Descriptor().Fields().ByName("value")).String() != "persisted-value" {
		t.Fatalf("unexpected value after reopen")
	}
}

func TestTableScanOrderAndPrefix(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()

	db, err := table.Open(ctx, dir, table.WithStreamID("stream-scan"))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	def, err := db.RegisterType(ctx, &pb.Inode{}, 1)
	if err != nil {
		t.Fatalf("RegisterType failed: %v", err)
	}

	pk := sds.NewPrimaryKey(1)
	var changes []sds.Change

	// Insert in non-sorted order
	ids := []uint64{50, 10, 30, 20, 40}
	for i, id := range ids {
		node := &pb.Inode{
			Ino:  proto.Uint64(id),
			Mode: uint32(0644 + id),
		}
		k, v, _ := pk.Split(node)
		changes = append(changes, sds.Change{
			Op:       sds.OpCreate,
			TypeID:   def.GetId(),
			TypeName: def.GetName(),
			RawKey:   k,
			RawVal:   v,
			Seq:      uint64(i + 1),
		})
	}

	if err := db.ApplyBatch(ctx, changes); err != nil {
		t.Fatalf("ApplyBatch failed: %v", err)
	}

	// Scan all rows
	rows, err := db.Rows(ctx, def.GetName())
	if err != nil {
		t.Fatalf("Rows failed: %v", err)
	}
	if len(rows) != 5 {
		t.Fatalf("got %d rows, want 5", len(rows))
	}

	// Verify canonical key ordering:
	var prevKey []byte
	for i, r := range rows {
		node := r.(*pb.Inode)
		k, _, _ := pk.Split(node)
		if i > 0 && bytes.Compare(prevKey, k) >= 0 {
			t.Fatalf("row %d key not strictly greater than previous", i)
		}
		prevKey = k
	}
}

func TestTableSnapshotPublishRestoreAndReader(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	backend := inmemorystorage.New()

	db, err := table.Open(ctx, dir, table.WithStreamID("stream-snap"))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	def, err := db.RegisterType(ctx, &pb.Inode{}, 1)
	if err != nil {
		t.Fatalf("RegisterType failed: %v", err)
	}

	pk := sds.NewPrimaryKey(1)
	for i := uint64(1); i <= 10; i++ {
		node := &pb.Inode{
			Ino:  proto.Uint64(i),
			Mode: uint32(0644),
		}
		k, v, _ := pk.Split(node)
		err := db.Apply(ctx, sds.Change{
			Op:       sds.OpCreate,
			TypeID:   def.GetId(),
			TypeName: def.GetName(),
			RawKey:   k,
			RawVal:   v,
			Seq:      i * 10,
		})
		if err != nil {
			t.Fatalf("Apply failed: %v", err)
		}
	}

	if db.Position() != 100 {
		t.Fatalf("Position = %d, want 100", db.Position())
	}

	// Publish snapshot
	snapKey, snapPos, err := db.PublishSnapshot(ctx, backend)
	if err != nil {
		t.Fatalf("PublishSnapshot failed: %v", err)
	}
	if snapPos != 100 {
		t.Fatalf("snapPos = %d, want 100", snapPos)
	}

	// 1. Standalone SnapshotReader from Object Storage byte stream
	var buf bytes.Buffer
	if err := backend.GetObject(ctx, "", snapKey, 0, 0, &buf); err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}

	sr, err := table.OpenSnapshotBytes(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSnapshotBytes failed: %v", err)
	}
	defer sr.Close()

	if sr.Position() != 100 {
		t.Fatalf("sr.Position = %d, want 100", sr.Position())
	}
	if sr.StreamID() != "stream-snap" {
		t.Fatalf("sr.StreamID = %q, want stream-snap", sr.StreamID())
	}

	// Verify reading row via SnapshotReader
	k5, _, _ := pk.Split(&pb.Inode{Ino: proto.Uint64(5)})
	gotMsg, found, err := sr.Get(def.GetName(), k5)
	if err != nil || !found {
		t.Fatalf("sr.Get failed: found=%v, err=%v", found, err)
	}
	if gotMsg.(*pb.Inode).GetIno() != 5 {
		t.Fatalf("got ino %d, want 5", gotMsg.(*pb.Inode).GetIno())
	}

	// Verify scanning via SnapshotReader
	srRows, err := sr.ScanSlice(def.GetName(), nil)
	if err != nil {
		t.Fatalf("sr.ScanSlice failed: %v", err)
	}
	if len(srRows) != 10 {
		t.Fatalf("srRows len = %d, want 10", len(srRows))
	}

	// 2. RestoreSnapshot into a new directory via Factory / ingest
	factory, err := sds.GetIndexFactory("table")
	if err != nil {
		t.Fatalf("GetIndexFactory(table) failed: %v", err)
	}

	restoreDir := t.TempDir()
	restoredIdx, restoredPos, err := factory.RestoreSnapshot(ctx, backend, "stream-snap", snapKey, restoreDir)
	if err != nil {
		t.Fatalf("factory.RestoreSnapshot failed: %v", err)
	}
	defer restoredIdx.Close()

	if restoredPos != 100 {
		t.Fatalf("restoredPos = %d, want 100", restoredPos)
	}
	if restoredIdx.Position() != 100 {
		t.Fatalf("restoredIdx.Position = %d, want 100", restoredIdx.Position())
	}

	restoredMsg, found, err := restoredIdx.Get(ctx, def.GetName(), sds.NewKeyFromBytes(k5))
	if err != nil || !found {
		t.Fatalf("restoredIdx.Get failed: found=%v, err=%v", found, err)
	}
	if restoredMsg.(*pb.Inode).GetIno() != 5 {
		t.Fatalf("got ino %d, want 5", restoredMsg.(*pb.Inode).GetIno())
	}
}

func TestFactoryOpenLocalAndNewEmpty(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()

	factory, err := sds.GetIndexFactory("table")
	if err != nil {
		t.Fatalf("GetIndexFactory(table) failed: %v", err)
	}

	// OpenLocal on non-existent
	idx, found, err := factory.OpenLocal(ctx, "stream-fact", dir)
	if err != nil || found || idx != nil {
		t.Fatalf("expected OpenLocal to return not found: found=%v, err=%v", found, err)
	}

	// NewEmpty
	idx, err = factory.NewEmpty(ctx, "stream-fact", dir)
	if err != nil {
		t.Fatalf("NewEmpty failed: %v", err)
	}
	if idx.Position() != 0 {
		t.Fatalf("Position = %d, want 0", idx.Position())
	}
	_ = idx.Close()

	// OpenLocal on existent
	idx2, found, err := factory.OpenLocal(ctx, "stream-fact", dir)
	if err != nil || !found || idx2 == nil {
		t.Fatalf("OpenLocal on existent failed: found=%v, err=%v", found, err)
	}
	_ = idx2.Close()
}

func TestFindLatestSnapshot(t *testing.T) {
	ctx := t.Context()
	backend := inmemorystorage.New()

	for _, pos := range []uint64{10, 20, 50, 80} {
		key := table.SnapshotKey("str-find", pos)
		_, err := backend.PutObject(ctx, "", key, blob.NewByteStreamFromBytes([]byte("dummy")))
		if err != nil {
			t.Fatalf("PutObject failed: %v", err)
		}
	}

	// Find latest overall
	latestKey, latestPos, err := table.FindLatestSnapshot(ctx, backend, "str-find", 0)
	if err != nil {
		t.Fatalf("FindLatestSnapshot failed: %v", err)
	}
	if latestPos != 80 {
		t.Fatalf("latestPos = %d, want 80", latestPos)
	}
	if latestKey != table.SnapshotKey("str-find", 80) {
		t.Fatalf("latestKey = %q, want %q", latestKey, table.SnapshotKey("str-find", 80))
	}

	// Find with maxPos = 30
	k30, p30, err := table.FindLatestSnapshot(ctx, backend, "str-find", 30)
	if err != nil {
		t.Fatalf("FindLatestSnapshot(30) failed: %v", err)
	}
	if p30 != 20 {
		t.Fatalf("p30 = %d, want 20", p30)
	}
	if k30 != table.SnapshotKey("str-find", 20) {
		t.Fatalf("k30 = %q, want %q", k30, table.SnapshotKey("str-find", 20))
	}
}

func TestIsUnrecoverable(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()

	db, err := table.Open(ctx, dir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	if db.IsUnrecoverable(nil) {
		t.Fatalf("nil error should not be unrecoverable")
	}
	if db.IsUnrecoverable(errors.New("some generic error")) {
		t.Fatalf("generic error should not be unrecoverable")
	}
}
