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

package sds

import (
	"context"
	"testing"
	"time"

	"github.com/gke-labs/in-cluster-storage/pkg/objectstore"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore/inmemorystorage"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/record"
	"github.com/gke-labs/in-cluster-storage/pkg/wal/buffer/buffertest"
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
	"github.com/google/uuid"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

type testServerHandle struct {
	bt   *buffertest.BufferTest
	addr string
}

func (h *testServerHandle) StopGraceful() {
	h.bt.Close()
}

func (h *testServerHandle) StreamReader(streamID uuid.UUID, opts ...StreamReaderOption) *StreamReader {
	opts = append([]StreamReaderOption{
		WithTailOptions(
			walclient.WithGRPCClient(h.bt.Client),
			walclient.WithDialOptions(h.bt.DialOpts...),
		),
	}, opts...)
	return NewStreamReader(h.addr, streamID, opts...)
}

func startBufferServer(t *testing.T, backend objectstore.Backend, dataDir string) *testServerHandle {
	t.Helper()
	bt, _ := buffertest.Start(t.Context(), t, buffertest.WithBackend(backend), buffertest.WithDataDir(dataDir))
	return &testServerHandle{bt: bt, addr: bt.Target}
}

func TestWALAppenderAndStreamReader(t *testing.T) {
	backend := inmemorystorage.New()
	handle := startBufferServer(t, backend, t.TempDir())
	defer handle.StopGraceful()

	clientDir := t.TempDir()
	streamID := uuid.New()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	walStream, err := walclient.Open(ctx, clientDir, streamID, handle.bt.Target,
		walclient.WithGRPCClient(handle.bt.Client),
		walclient.WithDialOptions(handle.bt.DialOpts...),
	)
	if err != nil {
		t.Fatalf("failed to open WAL stream: %v", err)
	}
	defer walStream.Close()

	appender := NewWALAppender(walStream, walclient.Witness)
	if appender.Stream() != walStream {
		t.Errorf("expected appender.Stream() to match walStream")
	}
	if appender.Durability() != walclient.Witness {
		t.Errorf("expected durability witness, got %v", appender.Durability())
	}

	writer := NewWriter(appender)

	// Create dynamic test type
	fields := []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("name", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("score", 3, descriptorpb.FieldDescriptorProto_TYPE_DOUBLE, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	}
	md := buildDynamicMessageDescriptor(t, "Player", fields)
	typeID, err := writer.RegisterDescriptor(md, 1)
	if err != nil {
		t.Fatalf("RegisterDescriptor failed: %v", err)
	}
	if typeID != 16 {
		t.Fatalf("expected typeID 16, got %d", typeID)
	}

	// 1. Write an autocommit Insert
	p1 := dynamicpb.NewMessage(md)
	p1.Set(md.Fields().ByName("id"), protoreflect.ValueOfInt64(101))
	p1.Set(md.Fields().ByName("name"), protoreflect.ValueOfString("Alice"))
	p1.Set(md.Fields().ByName("score"), protoreflect.ValueOfFloat64(99.5))

	seq1, err := writer.Insert(ctx, p1)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// 2. Write a multi-row transaction
	tx := writer.Begin()
	p2 := dynamicpb.NewMessage(md)
	p2.Set(md.Fields().ByName("id"), protoreflect.ValueOfInt64(102))
	p2.Set(md.Fields().ByName("name"), protoreflect.ValueOfString("Bob"))
	p2.Set(md.Fields().ByName("score"), protoreflect.ValueOfFloat64(88.0))
	if _, err := tx.Insert(ctx, p2); err != nil {
		t.Fatalf("tx.Insert failed: %v", err)
	}

	p1Updated := dynamicpb.NewMessage(md)
	p1Updated.Set(md.Fields().ByName("id"), protoreflect.ValueOfInt64(101))
	p1Updated.Set(md.Fields().ByName("name"), protoreflect.ValueOfString("Alice M"))
	p1Updated.Set(md.Fields().ByName("score"), protoreflect.ValueOfFloat64(100.0))
	if _, err := tx.Update(ctx, p1Updated); err != nil {
		t.Fatalf("tx.Update failed: %v", err)
	}

	commitSeq, err := tx.Commit(ctx)
	if err != nil {
		t.Fatalf("tx.Commit failed: %v", err)
	}

	// Read everything back using StreamReader
	reader := handle.StreamReader(streamID)
	readCtx, readCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer readCancel()

	iter, err := reader.Read(readCtx, 0)
	if err != nil {
		t.Fatalf("reader.Read failed: %v", err)
	}

	var allChanges []Change
	for seq, changes := range iter {
		for _, ch := range changes {
			allChanges = append(allChanges, ch)
		}
		if seq >= commitSeq {
			break
		}
	}

	// Expect 3 committed changes total:
	// 1: Autocommit Alice (CREATE)
	// 2: Tx Bob (CREATE)
	// 3: Tx Alice (UPDATE)
	if len(allChanges) != 3 {
		t.Fatalf("expected 3 committed changes, got %d: %+v", len(allChanges), allChanges)
	}

	if allChanges[0].Seq != seq1 || !allChanges[0].IsCreate() || allChanges[0].TypeName != "testpkg.Player" {
		t.Errorf("unexpected change 0: %+v", allChanges[0])
	}
	if allChanges[1].TxID == 0 || !allChanges[1].IsCreate() {
		t.Errorf("unexpected change 1: %+v", allChanges[1])
	}
	if allChanges[2].TxID == 0 || !allChanges[2].IsUpdate() {
		t.Errorf("unexpected change 2: %+v", allChanges[2])
	}

	// Verify reader can resume from sequence number using the registry seen up to that position
	exportedReg := reader.Registry().Export()
	resumedReg := record.NewRegistry()
	if err := resumedReg.Import(exportedReg); err != nil {
		t.Fatalf("Import failed: %v", err)
	}

	reader2 := handle.StreamReader(streamID, WithChangeReader(NewChangeReader(record.WithDecoderRegistry(resumedReg))))
	iter2, err := reader2.Read(readCtx, seq1)
	if err != nil {
		t.Fatalf("reader2.Read failed: %v", err)
	}

	var resumedChanges []Change
	for seq, changes := range iter2 {
		for _, ch := range changes {
			resumedChanges = append(resumedChanges, ch)
		}
		if seq >= commitSeq {
			break
		}
	}

	if len(resumedChanges) != 2 {
		t.Fatalf("expected 2 resumed changes after seq1, got %d", len(resumedChanges))
	}
}

func TestStreamReaderWithRecoveredRecords(t *testing.T) {
	backend := inmemorystorage.New()
	handle := startBufferServer(t, backend, t.TempDir())
	defer handle.StopGraceful()

	clientDir := t.TempDir()
	streamID := uuid.New()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	walStream, err := walclient.Open(ctx, clientDir, streamID, handle.bt.Target,
		walclient.WithGRPCClient(handle.bt.Client),
		walclient.WithDialOptions(handle.bt.DialOpts...),
	)
	if err != nil {
		t.Fatalf("failed to open WAL stream: %v", err)
	}

	appender := NewWALAppender(walStream, walclient.Witness)
	writer := NewWriter(appender)

	fields := []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("val", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	}
	md := buildDynamicMessageDescriptor(t, "Item", fields)
	_, err = writer.RegisterDescriptor(md, 1)
	if err != nil {
		t.Fatalf("RegisterDescriptor failed: %v", err)
	}

	item := dynamicpb.NewMessage(md)
	item.Set(md.Fields().ByName("id"), protoreflect.ValueOfInt64(1))
	item.Set(md.Fields().ByName("val"), protoreflect.ValueOfString("hello"))
	_, err = writer.Insert(ctx, item)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Close writer stream and re-open to get RecoveredRecords
	_ = walStream.Close()

	walStreamRecovered, err := walclient.Open(ctx, clientDir, streamID, handle.bt.Target,
		walclient.WithGRPCClient(handle.bt.Client),
		walclient.WithDialOptions(handle.bt.DialOpts...),
	)
	if err != nil {
		t.Fatalf("failed to re-open WAL stream: %v", err)
	}
	defer walStreamRecovered.Close()

	recovered := walStreamRecovered.RecoveredRecords()
	if len(recovered) == 0 {
		t.Fatalf("expected recovered records from local segments, got 0")
	}

	reader := NewStreamReader("", streamID, WithRecoveredRecords(recovered))
	changes, err := reader.FeedRecovered(0)
	if err != nil {
		t.Fatalf("FeedRecovered failed: %v", err)
	}

	if len(changes) != 1 {
		t.Fatalf("expected 1 change from recovered records, got %d", len(changes))
	}
	if changes[0].TypeName != "testpkg.Item" {
		t.Errorf("expected type name testpkg.Item, got %s", changes[0].TypeName)
	}
}

func TestWALAppenderPermanentDurability(t *testing.T) {
	backend := inmemorystorage.New()
	handle := startBufferServer(t, backend, t.TempDir())
	defer handle.StopGraceful()

	clientDir := t.TempDir()
	streamID := uuid.New()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	walStream, err := walclient.Open(ctx, clientDir, streamID, handle.bt.Target,
		walclient.WithGRPCClient(handle.bt.Client),
		walclient.WithDialOptions(handle.bt.DialOpts...),
	)
	if err != nil {
		t.Fatalf("failed to open WAL stream: %v", err)
	}
	defer walStream.Close()

	appender := NewWALAppender(walStream, walclient.Permanent)
	writer := NewWriter(appender)

	fields := []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	}
	md := buildDynamicMessageDescriptor(t, "Record", fields)
	_, err = writer.RegisterDescriptor(md, 1)
	if err != nil {
		t.Fatalf("RegisterDescriptor failed: %v", err)
	}

	rec := dynamicpb.NewMessage(md)
	rec.Set(md.Fields().ByName("id"), protoreflect.ValueOfInt64(42))

	seq, err := writer.Insert(ctx, rec)
	if err != nil {
		t.Fatalf("Insert with Permanent durability failed: %v", err)
	}

	_, _, permanent := walStream.Watermarks()
	if permanent < seq {
		t.Fatalf("expected permanent watermark >= %d, got %d", seq, permanent)
	}
}
