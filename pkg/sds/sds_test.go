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
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"

	sdsv1 "github.com/gke-labs/in-cluster-storage/pkg/api/sds/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

type memoryAppender struct {
	mu       sync.Mutex
	payloads [][]byte
	seq      uint64
}

func (m *memoryAppender) Append(_ context.Context, payload []byte) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seq++
	cp := make([]byte, len(payload))
	copy(cp, payload)
	m.payloads = append(m.payloads, cp)
	return m.seq, nil
}

func (m *memoryAppender) Payloads() [][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([][]byte, len(m.payloads))
	copy(out, m.payloads)
	return out
}

func field(name string, number int32, typeKind descriptorpb.FieldDescriptorProto_Type, label descriptorpb.FieldDescriptorProto_Label) *descriptorpb.FieldDescriptorProto {
	f := &descriptorpb.FieldDescriptorProto{
		Name:   proto.String(name),
		Number: proto.Int32(number),
		Type:   typeKind.Enum(),
		Label:  label.Enum(),
	}
	if label == descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL {
		f.Proto3Optional = proto.Bool(true)
	}
	return f
}

func buildDynamicMessageDescriptor(t *testing.T, name string, fields []*descriptorpb.FieldDescriptorProto, extraFiles ...*descriptorpb.FileDescriptorProto) protoreflect.MessageDescriptor {
	t.Helper()
	var oneofs []*descriptorpb.OneofDescriptorProto
	for _, f := range fields {
		if f.GetProto3Optional() {
			f.OneofIndex = proto.Int32(int32(len(oneofs)))
			oneofs = append(oneofs, &descriptorpb.OneofDescriptorProto{
				Name: proto.String("_" + f.GetName()),
			})
		}
	}
	msgProto := &descriptorpb.DescriptorProto{
		Name:      proto.String(name),
		Field:     fields,
		OneofDecl: oneofs,
	}

	mainFile := &descriptorpb.FileDescriptorProto{
		Name:        proto.String(name + ".proto"),
		Package:     proto.String("testpkg"),
		MessageType: []*descriptorpb.DescriptorProto{msgProto},
		Syntax:      proto.String("proto3"),
	}

	fds := &descriptorpb.FileDescriptorSet{
		File: append([]*descriptorpb.FileDescriptorProto{mainFile}, extraFiles...),
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

func TestPrimaryKeyExtractionAndSplit(t *testing.T) {
	// Message with composite key (id tag 1, tenant_id tag 2)
	fields := []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("tenant_id", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("data", 3, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	}
	md := buildDynamicMessageDescriptor(t, "TenantEntity", fields)
	pk := NewPrimaryKey(2, 1) // Provide in reverse order to verify sorting by field number

	if len(pk.FieldNumbers()) != 2 || pk.FieldNumbers()[0] != 1 || pk.FieldNumbers()[1] != 2 {
		t.Fatalf("pk.FieldNumbers() not sorted: %v", pk.FieldNumbers())
	}

	// Valid message
	msg := dynamicpb.NewMessage(md)
	msg.Set(md.Fields().ByName("id"), protoreflect.ValueOfInt64(42))
	msg.Set(md.Fields().ByName("tenant_id"), protoreflect.ValueOfString("tenant-a"))
	msg.Set(md.Fields().ByName("data"), protoreflect.ValueOfString("payload"))

	key, err := pk.Extract(msg)
	if err != nil {
		t.Fatalf("Extract error: %v", err)
	}
	if len(key.Bytes()) == 0 {
		t.Errorf("key bytes empty")
	}

	// Verify Split
	keyBytes, valBytes, err := pk.Split(msg)
	if err != nil {
		t.Fatalf("Split error: %v", err)
	}
	if !bytes.Equal(keyBytes, key.Bytes()) {
		t.Errorf("keyBytes (%x) != key.Bytes() (%x)", keyBytes, key.Bytes())
	}

	// Recombine via MergeKeyAndNonKey
	recombined := dynamicpb.NewMessage(md)
	if err := MergeKeyAndNonKey(recombined, keyBytes, valBytes); err != nil {
		t.Fatalf("MergeKeyAndNonKey error: %v", err)
	}

	if recombined.Get(md.Fields().ByName("id")).Int() != 42 {
		t.Errorf("recombined id mismatch")
	}
	if recombined.Get(md.Fields().ByName("tenant_id")).String() != "tenant-a" {
		t.Errorf("recombined tenant_id mismatch")
	}
	if recombined.Get(md.Fields().ByName("data")).String() != "payload" {
		t.Errorf("recombined data mismatch")
	}

	// Missing primary key field (id unset)
	msgMissingKey := dynamicpb.NewMessage(md)
	msgMissingKey.Set(md.Fields().ByName("tenant_id"), protoreflect.ValueOfString("tenant-a"))

	_, err = pk.Extract(msgMissingKey)
	if !errors.Is(err, ErrMissingPrimaryKey) {
		t.Errorf("expected ErrMissingPrimaryKey, got %v", err)
	}

	// Nil message
	_, err = pk.Extract(nil)
	if !errors.Is(err, ErrMissingPrimaryKey) {
		t.Errorf("expected ErrMissingPrimaryKey for nil message, got %v", err)
	}
}

func TestAutocommitOperations(t *testing.T) {
	ctx := t.Context()
	appender := &memoryAppender{}
	writer := NewWriter(appender)

	fields := []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("name", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	}
	md := buildDynamicMessageDescriptor(t, "User", fields)
	writer.RegisterDescriptor(md, 1)

	// 1. Insert (Create)
	u1 := dynamicpb.NewMessage(md)
	u1.Set(md.Fields().ByName("id"), protoreflect.ValueOfInt64(1))
	u1.Set(md.Fields().ByName("name"), protoreflect.ValueOfString("Alice"))

	if _, err := writer.Insert(ctx, u1); err != nil {
		t.Fatalf("Insert error: %v", err)
	}

	// 2. Update
	u1Updated := dynamicpb.NewMessage(md)
	u1Updated.Set(md.Fields().ByName("id"), protoreflect.ValueOfInt64(1))
	u1Updated.Set(md.Fields().ByName("name"), protoreflect.ValueOfString("Alice Smith"))

	if _, err := writer.Update(ctx, u1Updated); err != nil {
		t.Fatalf("Update error: %v", err)
	}

	// 3. Delete
	u1Delete := dynamicpb.NewMessage(md)
	u1Delete.Set(md.Fields().ByName("id"), protoreflect.ValueOfInt64(1))

	if _, err := writer.Delete(ctx, u1Delete); err != nil {
		t.Fatalf("Delete error: %v", err)
	}

	// Delete missing key should fail
	uEmpty := dynamicpb.NewMessage(md)
	if _, err := writer.Delete(ctx, uEmpty); err == nil {
		t.Fatalf("expected error on delete without key fields, got nil")
	}

	// Read stream with ChangeReader
	reader := NewChangeReader()
	var committedChanges []Change

	for i, payload := range appender.Payloads() {
		seq := uint64(i + 1)
		changes, err := reader.Feed(seq, payload)
		if err != nil {
			t.Fatalf("Feed seq %d error: %v", seq, err)
		}
		committedChanges = append(committedChanges, changes...)
	}

	if len(committedChanges) != 3 {
		t.Fatalf("expected 3 committed changes, got %d", len(committedChanges))
	}

	k1, _ := ExtractKey(u1, []int32{1})

	// Verify operations in order
	if !committedChanges[0].IsInsert() || committedChanges[0].Key != k1 {
		t.Errorf("change 0 mismatch: %+v", committedChanges[0])
	}
	if !committedChanges[1].IsUpdate() || committedChanges[1].Key != k1 {
		t.Errorf("change 1 mismatch: %+v", committedChanges[1])
	}
	if !committedChanges[2].IsDelete() || committedChanges[2].Key != k1 {
		t.Errorf("change 2 mismatch: %+v", committedChanges[2])
	}

	// Verify safe snapshot position is advanced after each autocommit
	if reader.SafeSnapshotPosition() != uint64(len(appender.Payloads())) {
		t.Errorf("SafeSnapshotPosition = %d, want %d", reader.SafeSnapshotPosition(), len(appender.Payloads()))
	}
}

func TestInterleavedTransactions(t *testing.T) {
	ctx := t.Context()
	appender := &memoryAppender{}
	writer := NewWriter(appender)

	fields := []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("value", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	}
	md := buildDynamicMessageDescriptor(t, "Item", fields)
	writer.RegisterDescriptor(md, 1)

	reader := NewChangeReader()

	// Begin Tx 1 and Tx 2
	tx1 := writer.Begin()
	tx2 := writer.Begin()

	item1 := dynamicpb.NewMessage(md)
	item1.Set(md.Fields().ByName("id"), protoreflect.ValueOfInt64(101))
	item1.Set(md.Fields().ByName("value"), protoreflect.ValueOfString("val1"))

	item2 := dynamicpb.NewMessage(md)
	item2.Set(md.Fields().ByName("id"), protoreflect.ValueOfInt64(201))
	item2.Set(md.Fields().ByName("value"), protoreflect.ValueOfString("val2"))

	item1Up := dynamicpb.NewMessage(md)
	item1Up.Set(md.Fields().ByName("id"), protoreflect.ValueOfInt64(101))
	item1Up.Set(md.Fields().ByName("value"), protoreflect.ValueOfString("val1-updated"))

	// 1. Tx1 insert item 101
	tx1.Insert(ctx, item1)
	// 2. Tx2 insert item 201
	tx2.Insert(ctx, item2)
	// 3. Tx1 update item 101
	tx1.Update(ctx, item1Up)

	// Feed all payloads so far to reader
	payloads := appender.Payloads()
	for i := 0; i < len(payloads); i++ {
		changes, err := reader.Feed(uint64(i+1), payloads[i])
		if err != nil {
			t.Fatalf("Feed error: %v", err)
		}
		if len(changes) != 0 {
			t.Fatalf("expected 0 committed changes while transactions are pending, got %d", len(changes))
		}
	}

	if !reader.HasPending() {
		t.Fatalf("expected pending transactions")
	}

	// 4. Tx1 commits
	if _, err := tx1.Commit(ctx); err != nil {
		t.Fatalf("Tx1 Commit error: %v", err)
	}

	// Feed Tx1 commit payload
	payloads = appender.Payloads()
	commit1Seq := uint64(len(payloads))
	tx1Committed, err := reader.Feed(commit1Seq, payloads[commit1Seq-1])
	if err != nil {
		t.Fatalf("Feed Tx1 commit error: %v", err)
	}

	if len(tx1Committed) != 2 {
		t.Fatalf("expected 2 committed changes for Tx1, got %d", len(tx1Committed))
	}
	if tx1Committed[0].Op != OpInsert || tx1Committed[1].Op != OpUpdate {
		t.Errorf("Tx1 committed operations mismatch: %+v", tx1Committed)
	}

	// Safe position must NOT advance to commit1Seq because Tx2 is still open!
	// (Safe position should be the position before Tx1 started, which was the TypeDefinition at seq 2)
	if reader.SafeSnapshotPosition() > 2 {
		t.Errorf("SafeSnapshotPosition advanced while Tx2 was still pending! got %d", reader.SafeSnapshotPosition())
	}

	// 5. Tx2 commits
	if _, err := tx2.Commit(ctx); err != nil {
		t.Fatalf("Tx2 Commit error: %v", err)
	}

	payloads = appender.Payloads()
	commit2Seq := uint64(len(payloads))
	tx2Committed, err := reader.Feed(commit2Seq, payloads[commit2Seq-1])
	if err != nil {
		t.Fatalf("Feed Tx2 commit error: %v", err)
	}

	if len(tx2Committed) != 1 {
		t.Fatalf("expected 1 committed change for Tx2, got %d", len(tx2Committed))
	}
	k201, _ := ExtractKey(item2, []int32{1})
	if tx2Committed[0].Key != k201 {
		t.Errorf("Tx2 key mismatch: %v", tx2Committed[0].Key)
	}

	// Now that all transactions are committed, safe position advances to commit2Seq!
	if reader.SafeSnapshotPosition() != commit2Seq {
		t.Errorf("SafeSnapshotPosition = %d, want %d", reader.SafeSnapshotPosition(), commit2Seq)
	}
}

func TestUncommittedTailDiscarded(t *testing.T) {
	ctx := t.Context()
	appender := &memoryAppender{}
	writer := NewWriter(appender)

	fields := []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	}
	md := buildDynamicMessageDescriptor(t, "Entry", fields)
	writer.RegisterDescriptor(md, 1)

	// Autocommit record 1
	e1 := dynamicpb.NewMessage(md)
	e1.Set(md.Fields().ByName("id"), protoreflect.ValueOfInt64(1))
	writer.Insert(ctx, e1)

	// Begin Tx and write two records
	tx := writer.Begin()
	e2 := dynamicpb.NewMessage(md)
	e2.Set(md.Fields().ByName("id"), protoreflect.ValueOfInt64(2))
	tx.Insert(ctx, e2)

	e3 := dynamicpb.NewMessage(md)
	e3.Set(md.Fields().ByName("id"), protoreflect.ValueOfInt64(3))
	tx.Insert(ctx, e3)

	// Reader consumes all payloads (no commit frame in stream)
	reader := NewChangeReader()
	var committed []Change

	for i, payload := range appender.Payloads() {
		seq := uint64(i + 1)
		c, err := reader.Feed(seq, payload)
		if err != nil {
			t.Fatalf("Feed error: %v", err)
		}
		committed = append(committed, c...)
	}

	// Only record 1 was committed
	if len(committed) != 1 {
		t.Fatalf("expected 1 committed change, got %d", len(committed))
	}

	// Safe snapshot position must be at the autocommit record (seq 3)
	if reader.SafeSnapshotPosition() != 3 {
		t.Errorf("SafeSnapshotPosition = %d, want 3", reader.SafeSnapshotPosition())
	}

	// Verify pending transactions
	pending := reader.PendingTransactions()
	if len(pending) != 1 {
		t.Fatalf("expected 1 pending transaction, got %d", len(pending))
	}
	if len(pending[tx.TxID()]) != 2 {
		t.Fatalf("expected 2 changes in pending transaction, got %d", len(pending[tx.TxID()]))
	}

	// Discard pending tail
	discarded := reader.DiscardPending()
	if len(discarded) != 2 {
		t.Fatalf("expected 2 discarded changes, got %d", len(discarded))
	}

	if reader.HasPending() {
		t.Errorf("expected no pending transactions after DiscardPending")
	}
}

func TestSafePositionNeverInsideTransaction(t *testing.T) {
	ctx := t.Context()
	appender := &memoryAppender{}
	writer := NewWriter(appender)

	fields := []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	}
	md := buildDynamicMessageDescriptor(t, "Doc", fields)
	writer.RegisterDescriptor(md, 1)

	reader := NewChangeReader()

	// Step 1: Autocommit insert at seq 1 (StreamHeader), seq 2 (TypeDef) & seq 3 (record)
	d1 := dynamicpb.NewMessage(md)
	d1.Set(md.Fields().ByName("id"), protoreflect.ValueOfInt64(1))
	writer.Insert(ctx, d1)

	for i := 0; i < len(appender.Payloads()); i++ {
		reader.Feed(uint64(i+1), appender.Payloads()[i])
	}
	if reader.SafeSnapshotPosition() != 3 {
		t.Errorf("step 1 safe pos = %d, want 3", reader.SafeSnapshotPosition())
	}

	// Step 2: Open Tx1 and write record at seq 4
	tx1 := writer.Begin()
	d2 := dynamicpb.NewMessage(md)
	d2.Set(md.Fields().ByName("id"), protoreflect.ValueOfInt64(2))
	tx1.Insert(ctx, d2)

	reader.Feed(4, appender.Payloads()[3])
	if reader.SafeSnapshotPosition() != 3 {
		t.Errorf("step 2 safe pos inside tx1 = %d, want 3", reader.SafeSnapshotPosition())
	}

	// Step 3: Open Tx2 and write record at seq 5
	tx2 := writer.Begin()
	d3 := dynamicpb.NewMessage(md)
	d3.Set(md.Fields().ByName("id"), protoreflect.ValueOfInt64(3))
	tx2.Insert(ctx, d3)

	reader.Feed(5, appender.Payloads()[4])
	if reader.SafeSnapshotPosition() != 3 {
		t.Errorf("step 3 safe pos inside tx2 = %d, want 3", reader.SafeSnapshotPosition())
	}

	// Step 4: Write another record for Tx1 at seq 6
	d4 := dynamicpb.NewMessage(md)
	d4.Set(md.Fields().ByName("id"), protoreflect.ValueOfInt64(4))
	tx1.Insert(ctx, d4)

	reader.Feed(6, appender.Payloads()[5])
	if reader.SafeSnapshotPosition() != 3 {
		t.Errorf("step 4 safe pos inside tx1 = %d, want 3", reader.SafeSnapshotPosition())
	}

	// Step 5: Commit Tx1 at seq 7
	tx1.Commit(ctx)
	reader.Feed(7, appender.Payloads()[6])
	// Tx2 is still open! Safe pos must STILL be 3.
	if reader.SafeSnapshotPosition() != 3 {
		t.Errorf("step 5 safe pos with Tx2 still open = %d, want 3", reader.SafeSnapshotPosition())
	}

	// Step 6: Commit Tx2 at seq 8
	tx2.Commit(ctx)
	reader.Feed(8, appender.Payloads()[7])
	// All open transactions closed! Safe pos jumps to 8.
	if reader.SafeSnapshotPosition() != 8 {
		t.Errorf("step 6 safe pos after all tx committed = %d, want 8", reader.SafeSnapshotPosition())
	}
}

func TestCompatibleSchemaEvolutionBetweenRecords(t *testing.T) {
	ctx := t.Context()
	appender := &memoryAppender{}
	writer := NewWriter(appender)

	// Schema V1
	fieldsV1 := []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("customer", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	}
	mdV1 := buildDynamicMessageDescriptor(t, "Order", fieldsV1)
	writer.RegisterDescriptor(mdV1, 1)

	// Write record under V1
	order1 := dynamicpb.NewMessage(mdV1)
	order1.Set(mdV1.Fields().ByName("id"), protoreflect.ValueOfInt64(101))
	order1.Set(mdV1.Fields().ByName("customer"), protoreflect.ValueOfString("Alice"))
	writer.Insert(ctx, order1)

	// Schema V2 (evolved: added total tag 3, notes tag 4)
	fieldsV2 := []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("customer", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("total", 3, descriptorpb.FieldDescriptorProto_TYPE_DOUBLE, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("notes", 4, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	}
	mdV2 := buildDynamicMessageDescriptor(t, "Order", fieldsV2)
	writer.RegisterDescriptor(mdV2, 1)

	// Write record under V2
	order2 := dynamicpb.NewMessage(mdV2)
	order2.Set(mdV2.Fields().ByName("id"), protoreflect.ValueOfInt64(102))
	order2.Set(mdV2.Fields().ByName("customer"), protoreflect.ValueOfString("Bob"))
	order2.Set(mdV2.Fields().ByName("total"), protoreflect.ValueOfFloat64(199.95))
	order2.Set(mdV2.Fields().ByName("notes"), protoreflect.ValueOfString("Priority shipping"))
	writer.Insert(ctx, order2)

	// Decode all with ChangeReader
	reader := NewChangeReader()
	var changes []Change

	for i, payload := range appender.Payloads() {
		seq := uint64(i + 1)
		c, err := reader.Feed(seq, payload)
		if err != nil {
			t.Fatalf("Feed error: %v", err)
		}
		changes = append(changes, c...)
	}

	if len(changes) != 2 {
		t.Fatalf("expected 2 changes, got %d", len(changes))
	}

	// Change 0 (V1 record): customer is Alice
	ch0Msg := changes[0].Row.(*dynamicpb.Message)
	if ch0Msg.Get(ch0Msg.Descriptor().Fields().ByName("customer")).String() != "Alice" {
		t.Errorf("ch0 customer mismatch")
	}

	// Change 1 (V2 record): has all fields
	ch1Msg := changes[1].Row.(*dynamicpb.Message)
	if ch1Msg.Get(ch1Msg.Descriptor().Fields().ByName("customer")).String() != "Bob" {
		t.Errorf("ch1 customer mismatch")
	}
	if ch1Msg.Get(ch1Msg.Descriptor().Fields().ByName("total")).Float() != 199.95 {
		t.Errorf("ch1 total mismatch")
	}
}

func TestFrameworkRecordsPassthrough(t *testing.T) {
	ctx := t.Context()
	appender := &memoryAppender{}
	writer := NewWriter(appender)

	ptr := &sdsv1.SnapshotPointer{
		Position: 50,
		Format:   "parquet",
		Location: "s3://bucket/snap50",
	}

	if _, err := writer.AppendSnapshotPointer(ctx, ptr); err != nil {
		t.Fatalf("AppendSnapshotPointer error: %v", err)
	}

	if _, err := writer.AppendPadding(ctx, 16); err != nil {
		t.Fatalf("AppendPadding error: %v", err)
	}

	reader := NewChangeReader()
	for i, payload := range appender.Payloads() {
		seq := uint64(i + 1)
		changes, err := reader.Feed(seq, payload)
		if err != nil {
			t.Fatalf("Feed error: %v", err)
		}
		if len(changes) != 0 {
			t.Errorf("expected 0 changes for framework records, got %d", len(changes))
		}
	}

	if reader.SafeSnapshotPosition() != 3 {
		t.Errorf("SafeSnapshotPosition = %d, want 3", reader.SafeSnapshotPosition())
	}
}
