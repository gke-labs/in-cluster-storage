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

package record

import (
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
	"google.golang.org/protobuf/types/known/timestamppb"
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

func TestWriterAndDecoderEndToEnd(t *testing.T) {
	ctx := t.Context()
	appender := &memoryAppender{}
	writer := NewWriter(appender, WithoutStreamHeader())

	// Appending a compiled Go type: sdsv1.SnapshotPointer
	ptr := &sdsv1.SnapshotPointer{
		Position:            100,
		Format:              "sqlite",
		Location:            "s3://bucket/path",
		RegistryFingerprint: []byte("01234567890123456789012345678901"),
	}

	seq, err := writer.AppendSnapshotPointer(ctx, ptr)
	if err != nil {
		t.Fatalf("AppendSnapshotPointer error: %v", err)
	}
	if seq != 1 {
		t.Errorf("seq = %d, want 1", seq)
	}

	// Appending padding
	paddingSeq, err := writer.AppendPadding(ctx, 32)
	if err != nil {
		t.Fatalf("AppendPadding error: %v", err)
	}
	if paddingSeq != 2 {
		t.Errorf("paddingSeq = %d, want 2", paddingSeq)
	}

	// Appending TxCommit
	commit := &sdsv1.TxCommit{
		TxId:       42,
		CommitTime: timestamppb.Now(),
	}
	commitSeq, err := writer.AppendTxCommit(ctx, commit)
	if err != nil {
		t.Fatalf("AppendTxCommit error: %v", err)
	}
	if commitSeq != 3 {
		t.Errorf("commitSeq = %d, want 3", commitSeq)
	}

	// Now decode with Decoder
	decoder := NewDecoder()
	payloads := appender.Payloads()

	// 1. SnapshotPointer
	rec1, err := decoder.Decode(payloads[0])
	if err != nil {
		t.Fatalf("Decode record 1 error: %v", err)
	}
	if rec1.TypeID != TypeIDSnapshotPointer {
		t.Errorf("rec1.TypeID = %d, want %d", rec1.TypeID, TypeIDSnapshotPointer)
	}
	gotPtr, ok := rec1.Message.(*sdsv1.SnapshotPointer)
	if !ok || gotPtr.GetPosition() != 100 || gotPtr.GetFormat() != "sqlite" {
		t.Errorf("rec1.Message mismatch: %+v", rec1.Message)
	}

	// 2. Padding
	rec2, err := decoder.Decode(payloads[1])
	if err != nil {
		t.Fatalf("Decode record 2 error: %v", err)
	}
	if rec2.TypeID != TypeIDPadding {
		t.Errorf("rec2.TypeID = %d, want %d", rec2.TypeID, TypeIDPadding)
	}
	if rec2.Message != nil {
		t.Errorf("rec2.Message = %v, want nil for padding", rec2.Message)
	}
	if len(rec2.Raw) != 32 {
		t.Errorf("len(rec2.Raw) = %d, want 32", len(rec2.Raw))
	}

	// 3. TxCommit
	rec3, err := decoder.Decode(payloads[2])
	if err != nil {
		t.Fatalf("Decode record 3 error: %v", err)
	}
	if rec3.TypeID != TypeIDTxCommit {
		t.Errorf("rec3.TypeID = %d, want %d", rec3.TypeID, TypeIDTxCommit)
	}
	gotCommit, ok := rec3.Message.(*sdsv1.TxCommit)
	if !ok || gotCommit.GetTxId() != 42 {
		t.Errorf("rec3.Message mismatch: %+v", rec3.Message)
	}
}

func appendOp(ctx context.Context, w *Writer, op sdsv1.OpRecord_Op, tableTypeID uint32, txID uint64, keyMsg proto.Message) (uint64, error) {
	keyBytes, err := proto.Marshal(keyMsg)
	if err != nil {
		return 0, err
	}
	return w.AppendOp(ctx, &sdsv1.OpRecord{
		Op:     op,
		TypeId: tableTypeID,
		TxId:   txID,
		Key:    keyBytes,
	})
}

func TestWriterAutomaticTypeAnnouncementAndRestart(t *testing.T) {
	ctx := t.Context()
	appender := &memoryAppender{}
	writer1 := NewWriter(appender, WithoutStreamHeader())

	// Build dynamic message Order V1
	fieldsV1 := []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("customer", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	}
	defV1 := makeTestTypeDef("Order", fieldsV1, nil, nil, []int32{1}, nil)
	defV1.Id = 16

	filesV1, err := protodesc.NewFiles(defV1.GetDescriptors())
	if err != nil {
		t.Fatalf("NewFiles error: %v", err)
	}
	d1, err := filesV1.FindDescriptorByName("testpkg.Order")
	if err != nil {
		t.Fatalf("FindDescriptorByName error: %v", err)
	}
	orderDescV1 := d1.(protoreflect.MessageDescriptor)

	// Register in writer1
	if _, err := writer1.RegisterDescriptor(orderDescV1, 1); err != nil {
		t.Fatalf("RegisterDescriptor error: %v", err)
	}

	// Create dynamic message instance
	order1 := dynamicpb.NewMessage(orderDescV1)
	order1.Set(orderDescV1.Fields().ByName("id"), protoreflect.ValueOfInt64(101))
	order1.Set(orderDescV1.Fields().ByName("customer"), protoreflect.ValueOfString("Alice"))

	// Append first record (OpCreate): should emit TypeDefinition (payload 0) + record (payload 1)
	if _, err := appendOp(ctx, writer1, sdsv1.OpRecord_CREATE, 16, 0, order1); err != nil {
		t.Fatalf("Append order1 error: %v", err)
	}

	if len(appender.Payloads()) != 2 {
		t.Fatalf("expected 2 payloads (TypeDef + record), got %d", len(appender.Payloads()))
	}

	// Append second record of same type: should NOT emit another TypeDefinition
	order2 := dynamicpb.NewMessage(orderDescV1)
	order2.Set(orderDescV1.Fields().ByName("id"), protoreflect.ValueOfInt64(102))
	order2.Set(orderDescV1.Fields().ByName("customer"), protoreflect.ValueOfString("Bob"))

	if _, err := appendOp(ctx, writer1, sdsv1.OpRecord_CREATE, 16, 0, order2); err != nil {
		t.Fatalf("Append order2 error: %v", err)
	}

	if len(appender.Payloads()) != 3 {
		t.Fatalf("expected 3 payloads total, got %d", len(appender.Payloads()))
	}

	// --- Simulate Writer Restart (new Writer instance) ---
	writer2 := NewWriter(appender, WithoutStreamHeader())
	// On restart, writer2 registers the same descriptor (rule 4)
	if _, err := writer2.RegisterDescriptor(orderDescV1, 1); err != nil {
		t.Fatalf("writer2 RegisterDescriptor error: %v", err)
	}

	order3 := dynamicpb.NewMessage(orderDescV1)
	order3.Set(orderDescV1.Fields().ByName("id"), protoreflect.ValueOfInt64(103))
	order3.Set(orderDescV1.Fields().ByName("customer"), protoreflect.ValueOfString("Charlie"))

	// writer2 emits TypeDefinition again + record
	if _, err := appendOp(ctx, writer2, sdsv1.OpRecord_CREATE, 16, 0, order3); err != nil {
		t.Fatalf("writer2 Append order3 error: %v", err)
	}

	if len(appender.Payloads()) != 5 {
		t.Fatalf("expected 5 payloads total (TypeDef + order1 + order2 + TypeDef + order3), got %d", len(appender.Payloads()))
	}

	// --- Now decode all 5 payloads with a single Decoder ---
	decoder := NewDecoder()
	var decodedOrders []string

	for i, payload := range appender.Payloads() {
		rec, err := decoder.Decode(payload)
		if err != nil {
			t.Fatalf("decoder failed on payload %d: %v", i, err)
		}
		if rec.TypeID == TypeIDOpRecord {
			dynMsg := dynamicpb.NewMessage(orderDescV1)
			if err := proto.Unmarshal(rec.OpRecord.GetKey(), dynMsg); err != nil {
				t.Fatalf("Unmarshal op key error: %v", err)
			}
			cust := dynMsg.Get(dynMsg.Descriptor().Fields().ByName("customer")).String()
			decodedOrders = append(decodedOrders, cust)
		}
	}

	if len(decodedOrders) != 3 {
		t.Fatalf("expected 3 decoded orders, got %d", len(decodedOrders))
	}
	if decodedOrders[0] != "Alice" || decodedOrders[1] != "Bob" || decodedOrders[2] != "Charlie" {
		t.Errorf("decoded orders mismatch: %v", decodedOrders)
	}
}

func TestCrossSchemaEvolutionAndUnknownFieldsPreservation(t *testing.T) {
	ctx := t.Context()

	// V1 Descriptor: id (1), customer (2)
	fieldsV1 := []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("customer", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	}
	defV1 := makeTestTypeDef("Order", fieldsV1, nil, nil, []int32{1}, nil)
	defV1.Id = 16

	filesV1, _ := protodesc.NewFiles(defV1.GetDescriptors())
	d1, _ := filesV1.FindDescriptorByName("testpkg.Order")
	descV1 := d1.(protoreflect.MessageDescriptor)

	// V2 Descriptor (compatible addition of 'total' tag 3 and 'notes' tag 4)
	fieldsV2 := []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("customer", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("total", 3, descriptorpb.FieldDescriptorProto_TYPE_DOUBLE, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("notes", 4, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	}
	defV2 := makeTestTypeDef("Order", fieldsV2, nil, nil, []int32{1}, nil)
	defV2.Id = 16

	filesV2, _ := protodesc.NewFiles(defV2.GetDescriptors())
	d2, _ := filesV2.FindDescriptorByName("testpkg.Order")
	descV2 := d2.(protoreflect.MessageDescriptor)

	// Scenario A: Write record under V1, decode with decoder that receives V1 then evolves to V2
	appender := &memoryAppender{}
	writer := NewWriter(appender)

	// 1. Writer emits under V1
	writer.RegisterDescriptor(descV1, 1)
	orderV1 := dynamicpb.NewMessage(descV1)
	orderV1.Set(descV1.Fields().ByName("id"), protoreflect.ValueOfInt64(1))
	orderV1.Set(descV1.Fields().ByName("customer"), protoreflect.ValueOfString("Alice"))

	if _, err := appendOp(ctx, writer, sdsv1.OpRecord_CREATE, 16, 0, orderV1); err != nil {
		t.Fatalf("Append orderV1 error: %v", err)
	}

	// 2. Writer evolves to V2 and emits under V2
	writer.RegisterDescriptor(descV2, 1)
	orderV2 := dynamicpb.NewMessage(descV2)
	orderV2.Set(descV2.Fields().ByName("id"), protoreflect.ValueOfInt64(2))
	orderV2.Set(descV2.Fields().ByName("customer"), protoreflect.ValueOfString("Bob"))
	orderV2.Set(descV2.Fields().ByName("total"), protoreflect.ValueOfFloat64(99.50))
	orderV2.Set(descV2.Fields().ByName("notes"), protoreflect.ValueOfString("Rush order"))

	if _, err := appendOp(ctx, writer, sdsv1.OpRecord_CREATE, 16, 0, orderV2); err != nil {
		t.Fatalf("Append orderV2 error: %v", err)
	}

	// Decoder consumes all payloads in stream order
	decoder := NewDecoder()
	var decodedRecords []*dynamicpb.Message

	for _, payload := range appender.Payloads() {
		rec, err := decoder.Decode(payload)
		if err != nil {
			t.Fatalf("Decode error: %v", err)
		}
		if rec.TypeID == TypeIDOpRecord {
			msgType, err := decoder.Registry().ResolveMessageType(rec.OpRecord.GetTypeId())
			if err != nil {
				t.Fatalf("ResolveMessageType error: %v", err)
			}
			dynMsg := msgType.New().Interface().(*dynamicpb.Message)
			if err := proto.Unmarshal(rec.OpRecord.GetKey(), dynMsg); err != nil {
				t.Fatalf("Unmarshal key error: %v", err)
			}
			decodedRecords = append(decodedRecords, dynMsg)
		}
	}

	if len(decodedRecords) != 2 {
		t.Fatalf("expected 2 decoded order records, got %d", len(decodedRecords))
	}

	// First record (written as V1): total field is unset in V1 decode
	if decodedRecords[0].Get(decodedRecords[0].Descriptor().Fields().ByName("customer")).String() != "Alice" {
		t.Errorf("record 0 customer mismatch")
	}

	// Second record (written as V2): has all fields
	if decodedRecords[1].Get(decodedRecords[1].Descriptor().Fields().ByName("customer")).String() != "Bob" {
		t.Errorf("record 1 customer mismatch")
	}
	if decodedRecords[1].Get(decodedRecords[1].Descriptor().Fields().ByName("total")).Float() != 99.50 {
		t.Errorf("record 1 total mismatch")
	}

	// Scenario B: Decode a V2 payload using a V1 descriptor to verify unknown fields are preserved!
	v2Bytes, err := proto.Marshal(orderV2)
	if err != nil {
		t.Fatalf("Marshal orderV2 error: %v", err)
	}

	// Unmarshal into dynamicpb Message with V1 descriptor (which doesn't know about tag 3 'total' and tag 4 'notes')
	v1Instance := dynamicpb.NewMessage(descV1)
	if err := proto.Unmarshal(v2Bytes, v1Instance); err != nil {
		t.Fatalf("Unmarshal V2 into V1 error: %v", err)
	}

	// Check known fields are parsed
	if v1Instance.Get(descV1.Fields().ByName("customer")).String() != "Bob" {
		t.Errorf("known field customer failed to decode")
	}

	// Check that unknown fields are preserved in raw proto reflection
	unknownFields := v1Instance.GetUnknown()
	if len(unknownFields) == 0 {
		t.Errorf("expected unknown fields to be preserved, got empty")
	}

	// Re-marshal v1Instance (carrying unknown fields) and unmarshal into V2 descriptor: should restore total and notes!
	roundtripBytes, err := proto.Marshal(v1Instance)
	if err != nil {
		t.Fatalf("Marshal v1Instance error: %v", err)
	}

	restoredV2 := dynamicpb.NewMessage(descV2)
	if err := proto.Unmarshal(roundtripBytes, restoredV2); err != nil {
		t.Fatalf("Unmarshal roundtrip into V2 error: %v", err)
	}

	if restoredV2.Get(descV2.Fields().ByName("notes")).String() != "Rush order" {
		t.Errorf("restored notes = %q, want 'Rush order'", restoredV2.Get(descV2.Fields().ByName("notes")).String())
	}
	if restoredV2.Get(descV2.Fields().ByName("total")).Float() != 99.50 {
		t.Errorf("restored total = %v, want 99.50", restoredV2.Get(descV2.Fields().ByName("total")).Float())
	}
}

func TestDefineBeforeUseRule(t *testing.T) {
	decoder := NewDecoder()

	// Create payload for type ID 16 with arbitrary body without prior TypeDefinition
	payload, err := EncodeFrame(16, []byte("arbitrary-data"))
	if err != nil {
		t.Fatalf("EncodeFrame error: %v", err)
	}

	// Decoder must reject with ErrTypeNotRegistered
	_, err = decoder.Decode(payload)
	if !errors.Is(err, ErrTypeNotRegistered) {
		t.Errorf("Decode without definition got error %v, want ErrTypeNotRegistered", err)
	}
}
