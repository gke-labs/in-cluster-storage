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

package sds_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	sdsv1 "github.com/gke-labs/in-cluster-storage/pkg/api/sds/v1"
	"github.com/gke-labs/in-cluster-storage/pkg/sds"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/memtable"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/record"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var regenerateVectors = flag.Bool("regenerate", false, "regenerate golden test vectors")

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
	return &descriptorpb.FieldDescriptorProto{
		Name:   proto.String(name),
		Number: proto.Int32(number),
		Type:   typeKind.Enum(),
		Label:  label.Enum(),
	}
}

type DecodedRecordVector struct {
	Seq       uint64          `json:"seq"`
	TypeID    uint32          `json:"type_id"`
	TypeName  string          `json:"type_name,omitempty"`
	ProtoJSON json.RawMessage `json:"protojson,omitempty"`
}

type GoldenVector struct {
	Name                 string                       `json:"name"`
	Description          string                       `json:"description"`
	PayloadsHex          []string                     `json:"payloads_hex"`
	ExpectedRecords      []DecodedRecordVector        `json:"expected_records,omitempty"`
	ExpectedTables       map[string][]json.RawMessage `json:"expected_tables,omitempty"`
	ExpectedSafePosition uint64                       `json:"expected_safe_position"`
	ExpectedError        string                       `json:"expected_error,omitempty"`
	ErrorAtSeq           uint64                       `json:"error_at_seq,omitempty"`
}

func buildMessageDescriptor(name string, fields []*descriptorpb.FieldDescriptorProto, reservedRanges []*descriptorpb.DescriptorProto_ReservedRange) (protoreflect.MessageDescriptor, error) {
	msgProto := &descriptorpb.DescriptorProto{
		Name:          proto.String(name),
		Field:         fields,
		ReservedRange: reservedRanges,
	}

	fileProto := &descriptorpb.FileDescriptorProto{
		Name:        proto.String(name + ".proto"),
		Package:     proto.String("testpkg"),
		MessageType: []*descriptorpb.DescriptorProto{msgProto},
		Syntax:      proto.String("proto3"),
	}

	fds := &descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{fileProto},
	}

	files, err := protodesc.NewFiles(fds)
	if err != nil {
		return nil, err
	}

	d, err := files.FindDescriptorByName(protoreflect.FullName("testpkg." + name))
	if err != nil {
		return nil, err
	}
	return d.(protoreflect.MessageDescriptor), nil
}

func generateAllVectors(ctx context.Context) ([]GoldenVector, error) {

	// Vector 1: 01_basic_crud
	var v1 GoldenVector
	{
		md, err := buildMessageDescriptor("Order", []*descriptorpb.FieldDescriptorProto{
			field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
			field("customer", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
			field("total", 3, descriptorpb.FieldDescriptorProto_TYPE_DOUBLE, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		}, nil)
		if err != nil {
			return nil, err
		}

		appender := &memoryAppender{}
		w := sds.NewWriter(appender)
		typeID, err := w.RegisterDescriptor(md, 1)
		if err != nil {
			return nil, err
		}

		// Ensure TypeDefinition is written
		if err := w.RecordWriter().EnsureAnnounced(ctx, typeID); err != nil {
			return nil, err
		}

		// CREATE id=1
		msg1 := dynamicpb.NewMessage(md)
		msg1.Set(md.Fields().ByName("id"), protoreflect.ValueOfInt64(1))
		msg1.Set(md.Fields().ByName("customer"), protoreflect.ValueOfString("Alice"))
		msg1.Set(md.Fields().ByName("total"), protoreflect.ValueOfFloat64(100.0))
		if _, err := w.Create(ctx, msg1); err != nil {
			return nil, err
		}

		// CREATE id=2
		msg2 := dynamicpb.NewMessage(md)
		msg2.Set(md.Fields().ByName("id"), protoreflect.ValueOfInt64(2))
		msg2.Set(md.Fields().ByName("customer"), protoreflect.ValueOfString("Bob"))
		msg2.Set(md.Fields().ByName("total"), protoreflect.ValueOfFloat64(50.0))
		if _, err := w.Create(ctx, msg2); err != nil {
			return nil, err
		}

		// UPDATE id=1
		msg1Up := dynamicpb.NewMessage(md)
		msg1Up.Set(md.Fields().ByName("id"), protoreflect.ValueOfInt64(1))
		msg1Up.Set(md.Fields().ByName("customer"), protoreflect.ValueOfString("Alice Updated"))
		msg1Up.Set(md.Fields().ByName("total"), protoreflect.ValueOfFloat64(120.0))
		if _, err := w.Update(ctx, msg1Up); err != nil {
			return nil, err
		}

		// DELETE id=2
		msg2Del := dynamicpb.NewMessage(md)
		msg2Del.Set(md.Fields().ByName("id"), protoreflect.ValueOfInt64(2))
		if _, err := w.Delete(ctx, msg2Del); err != nil {
			return nil, err
		}

		v1 = GoldenVector{
			Name:        "01_basic_crud",
			Description: "Table definition and autocommit CREATE, UPDATE, and DELETE operations",
		}
		for _, p := range appender.Payloads() {
			v1.PayloadsHex = append(v1.PayloadsHex, hex.EncodeToString(p))
		}
	}

	// Vector 2: 02_multi_row_tx
	var v2 GoldenVector
	{
		md, err := buildMessageDescriptor("Account", []*descriptorpb.FieldDescriptorProto{
			field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
			field("balance", 2, descriptorpb.FieldDescriptorProto_TYPE_DOUBLE, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		}, nil)
		if err != nil {
			return nil, err
		}

		appender := &memoryAppender{}
		w := sds.NewWriter(appender)
		typeID, err := w.RegisterDescriptor(md, 1)
		if err != nil {
			return nil, err
		}
		if err := w.RecordWriter().EnsureAnnounced(ctx, typeID); err != nil {
			return nil, err
		}

		msg1 := dynamicpb.NewMessage(md)
		msg1.Set(md.Fields().ByName("id"), protoreflect.ValueOfInt64(1))
		msg1.Set(md.Fields().ByName("balance"), protoreflect.ValueOfFloat64(200.0))
		if _, err := w.Create(ctx, msg1); err != nil {
			return nil, err
		}

		msg2 := dynamicpb.NewMessage(md)
		msg2.Set(md.Fields().ByName("id"), protoreflect.ValueOfInt64(2))
		msg2.Set(md.Fields().ByName("balance"), protoreflect.ValueOfFloat64(100.0))
		if _, err := w.Create(ctx, msg2); err != nil {
			return nil, err
		}

		tx := w.Begin()
		msg1Up := dynamicpb.NewMessage(md)
		msg1Up.Set(md.Fields().ByName("id"), protoreflect.ValueOfInt64(1))
		msg1Up.Set(md.Fields().ByName("balance"), protoreflect.ValueOfFloat64(150.0))
		if _, err := tx.Update(ctx, msg1Up); err != nil {
			return nil, err
		}

		msg2Up := dynamicpb.NewMessage(md)
		msg2Up.Set(md.Fields().ByName("id"), protoreflect.ValueOfInt64(2))
		msg2Up.Set(md.Fields().ByName("balance"), protoreflect.ValueOfFloat64(150.0))
		if _, err := tx.Update(ctx, msg2Up); err != nil {
			return nil, err
		}

		// Fixed timestamp for deterministic commit
		commit := &sdsv1.TxCommit{
			TxId:       tx.TxID(),
			CommitTime: &timestamppb.Timestamp{Seconds: 1760000000, Nanos: 0},
		}
		if _, err := w.RecordWriter().AppendTxCommit(ctx, commit); err != nil {
			return nil, err
		}

		v2 = GoldenVector{
			Name:        "02_multi_row_tx",
			Description: "Multi-row atomic transaction committed by TxCommit",
		}
		for _, p := range appender.Payloads() {
			v2.PayloadsHex = append(v2.PayloadsHex, hex.EncodeToString(p))
		}
	}

	// Vector 3: 03_schema_evolution_add_field
	var v3 GoldenVector
	{
		md1, err := buildMessageDescriptor("User", []*descriptorpb.FieldDescriptorProto{
			field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
			field("name", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		}, nil)
		if err != nil {
			return nil, err
		}

		md2, err := buildMessageDescriptor("User", []*descriptorpb.FieldDescriptorProto{
			field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
			field("name", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
			field("email", 3, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		}, nil)
		if err != nil {
			return nil, err
		}

		appender := &memoryAppender{}
		w := sds.NewWriter(appender)
		tID, err := w.RegisterDescriptor(md1, 1)
		if err != nil {
			return nil, err
		}
		if err := w.RecordWriter().EnsureAnnounced(ctx, tID); err != nil {
			return nil, err
		}

		msg1 := dynamicpb.NewMessage(md1)
		msg1.Set(md1.Fields().ByName("id"), protoreflect.ValueOfInt64(1))
		msg1.Set(md1.Fields().ByName("name"), protoreflect.ValueOfString("Alice"))
		if _, err := w.Create(ctx, msg1); err != nil {
			return nil, err
		}

		if _, err := w.RegisterDescriptor(md2, 1); err != nil {
			return nil, err
		}
		if err := w.RecordWriter().EnsureAnnounced(ctx, tID); err != nil {
			return nil, err
		}

		msg2 := dynamicpb.NewMessage(md2)
		msg2.Set(md2.Fields().ByName("id"), protoreflect.ValueOfInt64(2))
		msg2.Set(md2.Fields().ByName("name"), protoreflect.ValueOfString("Bob"))
		msg2.Set(md2.Fields().ByName("email"), protoreflect.ValueOfString("bob@example.com"))
		if _, err := w.Create(ctx, msg2); err != nil {
			return nil, err
		}

		v3 = GoldenVector{
			Name:        "03_schema_evolution_add_field",
			Description: "Schema evolution: adding a new optional field",
		}
		for _, p := range appender.Payloads() {
			v3.PayloadsHex = append(v3.PayloadsHex, hex.EncodeToString(p))
		}
	}

	// Vector 4: 04_schema_evolution_rename_field
	var v4 GoldenVector
	{
		md1, err := buildMessageDescriptor("Item", []*descriptorpb.FieldDescriptorProto{
			field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
			field("name", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		}, nil)
		if err != nil {
			return nil, err
		}

		md2, err := buildMessageDescriptor("Item", []*descriptorpb.FieldDescriptorProto{
			field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
			field("title", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		}, nil)
		if err != nil {
			return nil, err
		}

		appender := &memoryAppender{}
		w := sds.NewWriter(appender)
		tID, err := w.RegisterDescriptor(md1, 1)
		if err != nil {
			return nil, err
		}
		if err := w.RecordWriter().EnsureAnnounced(ctx, tID); err != nil {
			return nil, err
		}

		msg1 := dynamicpb.NewMessage(md1)
		msg1.Set(md1.Fields().ByName("id"), protoreflect.ValueOfInt64(1))
		msg1.Set(md1.Fields().ByName("name"), protoreflect.ValueOfString("Widget"))
		if _, err := w.Create(ctx, msg1); err != nil {
			return nil, err
		}

		if _, err := w.RegisterDescriptor(md2, 1); err != nil {
			return nil, err
		}
		if err := w.RecordWriter().EnsureAnnounced(ctx, tID); err != nil {
			return nil, err
		}

		msg2 := dynamicpb.NewMessage(md2)
		msg2.Set(md2.Fields().ByName("id"), protoreflect.ValueOfInt64(2))
		msg2.Set(md2.Fields().ByName("title"), protoreflect.ValueOfString("Gadget"))
		if _, err := w.Create(ctx, msg2); err != nil {
			return nil, err
		}

		v4 = GoldenVector{
			Name:        "04_schema_evolution_rename_field",
			Description: "Schema evolution: renaming a field while preserving field tag number",
		}
		for _, p := range appender.Payloads() {
			v4.PayloadsHex = append(v4.PayloadsHex, hex.EncodeToString(p))
		}
	}

	// Vector 5: 05_schema_evolution_reserved_field
	var v5 GoldenVector
	{
		md1, err := buildMessageDescriptor("Product", []*descriptorpb.FieldDescriptorProto{
			field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
			field("sku", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
			field("price", 3, descriptorpb.FieldDescriptorProto_TYPE_DOUBLE, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		}, nil)
		if err != nil {
			return nil, err
		}

		md2, err := buildMessageDescriptor("Product", []*descriptorpb.FieldDescriptorProto{
			field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
			field("sku", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		}, []*descriptorpb.DescriptorProto_ReservedRange{
			{Start: proto.Int32(3), End: proto.Int32(4)},
		})
		if err != nil {
			return nil, err
		}

		appender := &memoryAppender{}
		w := sds.NewWriter(appender)
		tID, err := w.RegisterDescriptor(md1, 1)
		if err != nil {
			return nil, err
		}
		if err := w.RecordWriter().EnsureAnnounced(ctx, tID); err != nil {
			return nil, err
		}

		msg1 := dynamicpb.NewMessage(md1)
		msg1.Set(md1.Fields().ByName("id"), protoreflect.ValueOfInt64(1))
		msg1.Set(md1.Fields().ByName("sku"), protoreflect.ValueOfString("ABC"))
		msg1.Set(md1.Fields().ByName("price"), protoreflect.ValueOfFloat64(10.0))
		if _, err := w.Create(ctx, msg1); err != nil {
			return nil, err
		}

		if _, err := w.RegisterDescriptor(md2, 1); err != nil {
			return nil, err
		}
		if err := w.RecordWriter().EnsureAnnounced(ctx, tID); err != nil {
			return nil, err
		}

		msg2 := dynamicpb.NewMessage(md2)
		msg2.Set(md2.Fields().ByName("id"), protoreflect.ValueOfInt64(2))
		msg2.Set(md2.Fields().ByName("sku"), protoreflect.ValueOfString("XYZ"))
		if _, err := w.Create(ctx, msg2); err != nil {
			return nil, err
		}

		v5 = GoldenVector{
			Name:        "05_schema_evolution_reserved_field",
			Description: "Schema evolution: removing a field and reserving its field tag number",
		}
		for _, p := range appender.Payloads() {
			v5.PayloadsHex = append(v5.PayloadsHex, hex.EncodeToString(p))
		}
	}

	// Vector 6: 06_raw_application_records
	var v6 GoldenVector
	{
		md, err := buildMessageDescriptor("LogEvent", []*descriptorpb.FieldDescriptorProto{
			field("event_id", 1, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
			field("message", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		}, nil)
		if err != nil {
			return nil, err
		}

		appender := &memoryAppender{}
		w := record.NewWriter(appender)
		tID, err := w.RegisterDescriptor(md)
		if err != nil {
			return nil, err
		}
		if err := w.EnsureAnnounced(ctx, tID); err != nil {
			return nil, err
		}

		evt1 := dynamicpb.NewMessage(md)
		evt1.Set(md.Fields().ByName("event_id"), protoreflect.ValueOfString("evt-1"))
		evt1.Set(md.Fields().ByName("message"), protoreflect.ValueOfString("service starting"))
		b1, err := proto.MarshalOptions{Deterministic: true}.Marshal(evt1)
		if err != nil {
			return nil, err
		}
		if _, err := w.AppendRaw(ctx, tID, b1); err != nil {
			return nil, err
		}

		evt2 := dynamicpb.NewMessage(md)
		evt2.Set(md.Fields().ByName("event_id"), protoreflect.ValueOfString("evt-2"))
		evt2.Set(md.Fields().ByName("message"), protoreflect.ValueOfString("ready"))
		b2, err := proto.MarshalOptions{Deterministic: true}.Marshal(evt2)
		if err != nil {
			return nil, err
		}
		if _, err := w.AppendRaw(ctx, tID, b2); err != nil {
			return nil, err
		}

		v6 = GoldenVector{
			Name:        "06_raw_application_records",
			Description: "Raw application-typed records (Type IDs 16+) representing keyless events",
		}
		for _, p := range appender.Payloads() {
			v6.PayloadsHex = append(v6.PayloadsHex, hex.EncodeToString(p))
		}
	}

	// Vector 7: 07_snapshot_pointer_and_padding
	var v7 GoldenVector
	{
		appender := &memoryAppender{}
		w := record.NewWriter(appender)

		// Padding frame (16 bytes)
		paddingBytes := []byte("padding-bytes-16")
		if _, err := w.AppendPaddingBytes(ctx, paddingBytes); err != nil {
			return nil, err
		}

		// SnapshotPointer frame
		ptr := &sdsv1.SnapshotPointer{
			Position:            100,
			Format:              "sqlite",
			Location:            "s3://bucket/snapshots/sqlite/00000000000000000100.db",
			RegistryFingerprint: make([]byte, 32),
		}
		if _, err := w.AppendSnapshotPointer(ctx, ptr); err != nil {
			return nil, err
		}

		v7 = GoldenVector{
			Name:        "07_snapshot_pointer_and_padding",
			Description: "Framework Padding (Type ID 4) and SnapshotPointer (Type ID 3) frames",
		}
		for _, p := range appender.Payloads() {
			v7.PayloadsHex = append(v7.PayloadsHex, hex.EncodeToString(p))
		}
	}

	// Vector 8: 08_error_reserved_type_id
	var v8 GoldenVector
	{
		// Encode raw frame with reserved type ID 7
		buf := make([]byte, binary.MaxVarintLen32+3)
		n := binary.PutUvarint(buf, 7)
		copy(buf[n:], []byte{0x01, 0x02, 0x03})
		payload := buf[:n+3]
		v8 = GoldenVector{
			Name:          "08_error_reserved_type_id",
			Description:   "Error case: decoding frame with reserved framework Type ID (6..15)",
			PayloadsHex:   []string{hex.EncodeToString(payload)},
			ExpectedError: "reserved type ID",
			ErrorAtSeq:    1,
		}
	}

	// Vector 9: 09_error_use_before_define
	var v9 GoldenVector
	{
		opRec := &sdsv1.OpRecord{
			Op:     sdsv1.OpRecord_CREATE,
			TypeId: 16,
			Key:    []byte{0x08, 0x01},
			Value:  []byte{0x12, 0x04, 't', 'e', 's', 't'},
		}
		opBytes, _ := proto.Marshal(opRec)
		payload, _ := record.EncodeFrame(record.TypeIDOpRecord, opBytes)

		v9 = GoldenVector{
			Name:          "09_error_use_before_define",
			Description:   "Error case: OpRecord referencing table Type ID 16 before TypeDefinition",
			PayloadsHex:   []string{hex.EncodeToString(payload)},
			ExpectedError: "type ID used before definition",
			ErrorAtSeq:    1,
		}
	}

	// Vector 10: 10_error_incompatible_redefinition
	var v10 GoldenVector
	{
		md1, err := buildMessageDescriptor("Metric", []*descriptorpb.FieldDescriptorProto{
			field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
			field("val", 2, descriptorpb.FieldDescriptorProto_TYPE_DOUBLE, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		}, nil)
		if err != nil {
			return nil, err
		}

		md2, err := buildMessageDescriptor("Metric", []*descriptorpb.FieldDescriptorProto{
			field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
			field("val", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		}, nil)
		if err != nil {
			return nil, err
		}

		def1, err := record.BuildTypeDefinition(16, md1, []int32{1})
		if err != nil {
			return nil, err
		}
		def1Bytes, _ := proto.Marshal(def1)
		p1, _ := record.EncodeFrame(record.TypeIDTypeDefinition, def1Bytes)

		def2, err := record.BuildTypeDefinition(16, md2, []int32{1})
		if err != nil {
			return nil, err
		}
		def2Bytes, _ := proto.Marshal(def2)
		p2, _ := record.EncodeFrame(record.TypeIDTypeDefinition, def2Bytes)

		v10 = GoldenVector{
			Name:          "10_error_incompatible_redefinition",
			Description:   "Error case: incompatible schema evolution (field type changed from double to string)",
			PayloadsHex:   []string{hex.EncodeToString(p1), hex.EncodeToString(p2)},
			ExpectedError: "incompatible schema evolution",
			ErrorAtSeq:    2,
		}
	}

	// Vector 11: 11_error_key_fields_mismatch
	var v11 GoldenVector
	{
		md, err := buildMessageDescriptor("Entity", []*descriptorpb.FieldDescriptorProto{
			field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
			field("data", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		}, nil)
		if err != nil {
			return nil, err
		}

		def, err := record.BuildTypeDefinition(16, md, []int32{1})
		if err != nil {
			return nil, err
		}
		defBytes, _ := proto.Marshal(def)
		p1, _ := record.EncodeFrame(record.TypeIDTypeDefinition, defBytes)

		// Invalid key bytes containing non-key field tag 2 (data="bad") instead of field 1
		badKeyMsg := dynamicpb.NewMessage(md)
		badKeyMsg.Set(md.Fields().ByName("data"), protoreflect.ValueOfString("bad"))
		badKeyBytes, _ := proto.MarshalOptions{Deterministic: true}.Marshal(badKeyMsg)

		opRec := &sdsv1.OpRecord{
			Op:     sdsv1.OpRecord_CREATE,
			TypeId: 16,
			Key:    badKeyBytes,
		}
		opBytes, _ := proto.Marshal(opRec)
		p2, _ := record.EncodeFrame(record.TypeIDOpRecord, opBytes)

		v11 = GoldenVector{
			Name:          "11_error_key_fields_mismatch",
			Description:   "Error case: OpRecord whose key bytes do not match declared key_fields",
			PayloadsHex:   []string{hex.EncodeToString(p1), hex.EncodeToString(p2)},
			ExpectedError: "key bytes do not match key_fields",
			ErrorAtSeq:    2,
		}
	}

	// Vector 12: 12_uncommitted_tail
	var v12 GoldenVector
	{
		md, err := buildMessageDescriptor("Order", []*descriptorpb.FieldDescriptorProto{
			field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
			field("customer", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
			field("total", 3, descriptorpb.FieldDescriptorProto_TYPE_DOUBLE, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		}, nil)
		if err != nil {
			return nil, err
		}

		appender := &memoryAppender{}
		w := sds.NewWriter(appender)
		typeID, err := w.RegisterDescriptor(md, 1)
		if err != nil {
			return nil, err
		}
		if err := w.RecordWriter().EnsureAnnounced(ctx, typeID); err != nil {
			return nil, err
		}

		msg1 := dynamicpb.NewMessage(md)
		msg1.Set(md.Fields().ByName("id"), protoreflect.ValueOfInt64(1))
		msg1.Set(md.Fields().ByName("customer"), protoreflect.ValueOfString("Alice"))
		msg1.Set(md.Fields().ByName("total"), protoreflect.ValueOfFloat64(100.0))
		if _, err := w.Create(ctx, msg1); err != nil {
			return nil, err
		}

		tx := w.Begin()
		msg2 := dynamicpb.NewMessage(md)
		msg2.Set(md.Fields().ByName("id"), protoreflect.ValueOfInt64(2))
		msg2.Set(md.Fields().ByName("customer"), protoreflect.ValueOfString("Bob"))
		msg2.Set(md.Fields().ByName("total"), protoreflect.ValueOfFloat64(200.0))
		if _, err := tx.Create(ctx, msg2); err != nil {
			return nil, err
		}
		// Notice: tx is never committed!

		v12 = GoldenVector{
			Name:        "12_uncommitted_tail",
			Description: "Uncommitted transaction at stream tail is discarded on reader recovery",
		}
		for _, p := range appender.Payloads() {
			v12.PayloadsHex = append(v12.PayloadsHex, hex.EncodeToString(p))
		}
	}

	vectors := []GoldenVector{v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12}

	// Compute expected decoded records and final table state for each vector
	marshalOpts := protojson.MarshalOptions{
		Multiline:       false,
		Indent:          "",
		EmitUnpopulated: false,
	}

	for i := range vectors {
		v := &vectors[i]
		reader := sds.NewChangeReader()
		store := memtable.New()

		var decodedRecords []DecodedRecordVector
		var gotError error
		var errorSeq uint64

		for idx, hexPayload := range v.PayloadsHex {
			seq := uint64(idx + 1)
			payload, err := hex.DecodeString(hexPayload)
			if err != nil {
				return nil, fmt.Errorf("invalid hex in vector %s: %w", v.Name, err)
			}

			// First decode frame
			rec, dErr := reader.Decoder().Decode(payload)
			if dErr != nil {
				gotError = dErr
				errorSeq = seq
				break
			}

			var protoJSON json.RawMessage
			if rec.Message != nil {
				b, mErr := marshalOpts.Marshal(rec.Message)
				if mErr != nil {
					return nil, fmt.Errorf("protojson marshal error: %w", mErr)
				}
				protoJSON = json.RawMessage(b)
			}

			var typeName string
			if rec.Message != nil {
				typeName = string(rec.Message.ProtoReflect().Descriptor().FullName())
			}

			decodedRecords = append(decodedRecords, DecodedRecordVector{
				Seq:       seq,
				TypeID:    rec.TypeID,
				TypeName:  typeName,
				ProtoJSON: protoJSON,
			})

			// Then feed to ChangeReader
			changes, fErr := reader.Feed(seq, payload)
			if fErr != nil {
				gotError = fErr
				errorSeq = seq
				break
			}

			if err := store.ApplyBatch(ctx, changes); err != nil {
				return nil, fmt.Errorf("memtable apply error in vector %s: %w", v.Name, err)
			}
		}

		if gotError != nil {
			v.ExpectedRecords = decodedRecords
			v.ErrorAtSeq = errorSeq
			continue
		}

		// Discard pending transactions on stream end (recovering safe state)
		reader.DiscardPending()

		v.ExpectedRecords = decodedRecords
		v.ExpectedSafePosition = reader.SafeSnapshotPosition()

		// Extract final tables
		expectedTables := make(map[string][]json.RawMessage)
		for _, tableName := range store.Tables() {
			rows, err := store.Rows(tableName)
			if err != nil {
				return nil, fmt.Errorf("failed to get rows for %s: %w", tableName, err)
			}
			var rowJSONs []json.RawMessage
			for _, row := range rows {
				b, err := marshalOpts.Marshal(row)
				if err != nil {
					return nil, fmt.Errorf("failed to marshal row protojson: %w", err)
				}
				rowJSONs = append(rowJSONs, json.RawMessage(b))
			}
			expectedTables[tableName] = rowJSONs
		}
		v.ExpectedTables = expectedTables
	}

	return vectors, nil
}

func TestGoldenVectors(t *testing.T) {
	vectorsDir := filepath.Join("testdata", "vectors")

	if *regenerateVectors {
		if err := os.MkdirAll(vectorsDir, 0755); err != nil {
			t.Fatalf("failed to create vectors dir: %v", err)
		}

		vectors, err := generateAllVectors(t.Context())
		if err != nil {
			t.Fatalf("failed to generate vectors: %v", err)
		}

		for _, v := range vectors {
			filePath := filepath.Join(vectorsDir, v.Name+".json")
			buf := &bytes.Buffer{}
			enc := json.NewEncoder(buf)
			enc.SetEscapeHTML(false)
			enc.SetIndent("", "  ")
			if err := enc.Encode(v); err != nil {
				t.Fatalf("failed to encode vector %s: %v", v.Name, err)
			}
			if err := os.WriteFile(filePath, buf.Bytes(), 0644); err != nil {
				t.Fatalf("failed to write vector file %s: %v", filePath, err)
			}
			t.Logf("Wrote golden vector: %s", filePath)
		}
	}

	files, err := filepath.Glob(filepath.Join(vectorsDir, "*.json"))
	if err != nil {
		t.Fatalf("failed to glob vector files: %v", err)
	}
	if len(files) == 0 {
		t.Fatalf("no golden vector files found in %s (run with -regenerate to generate)", vectorsDir)
	}
	sort.Strings(files)

	for _, file := range files {
		fileName := filepath.Base(file)
		t.Run(fileName, func(t *testing.T) {
			data, err := os.ReadFile(file)
			if err != nil {
				t.Fatalf("failed to read vector file %s: %v", file, err)
			}

			var vector GoldenVector
			if err := json.Unmarshal(data, &vector); err != nil {
				t.Fatalf("failed to parse JSON from %s: %v", file, err)
			}

			reader := sds.NewChangeReaderWithOptions(true)
			store := memtable.New()

			var actualRecords []DecodedRecordVector
			var actualErr error
			var errSeq uint64

			for idx, hexPayload := range vector.PayloadsHex {
				seq := uint64(idx + 1)
				payload, err := hex.DecodeString(hexPayload)
				if err != nil {
					t.Fatalf("invalid hex payload at seq %d in %s: %v", seq, file, err)
				}

				rec, dErr := reader.Decoder().Decode(payload)
				if dErr != nil {
					actualErr = dErr
					errSeq = seq
					break
				}

				var protoJSON json.RawMessage
				if rec.Message != nil {
					b, mErr := protojson.Marshal(rec.Message)
					if mErr != nil {
						t.Fatalf("failed to protojson marshal decoded record at seq %d: %v", seq, mErr)
					}
					protoJSON = json.RawMessage(b)
				}

				var typeName string
				if rec.Message != nil {
					typeName = string(rec.Message.ProtoReflect().Descriptor().FullName())
				}

				actualRecords = append(actualRecords, DecodedRecordVector{
					Seq:       seq,
					TypeID:    rec.TypeID,
					TypeName:  typeName,
					ProtoJSON: protoJSON,
				})

				changes, fErr := reader.Feed(seq, payload)
				if fErr != nil {
					actualErr = fErr
					errSeq = seq
					break
				}

				if err := store.ApplyBatch(t.Context(), changes); err != nil {
					t.Fatalf("failed to apply changes at seq %d: %v", seq, err)
				}
			}

			// Validate error cases
			if vector.ExpectedError != "" {
				if actualErr == nil {
					t.Fatalf("expected error containing %q at seq %d, got nil", vector.ExpectedError, vector.ErrorAtSeq)
				}
				if !strings.Contains(actualErr.Error(), vector.ExpectedError) {
					t.Fatalf("expected error containing %q, got: %v", vector.ExpectedError, actualErr)
				}
				if errSeq != vector.ErrorAtSeq {
					t.Fatalf("error occurred at seq %d, expected at seq %d", errSeq, vector.ErrorAtSeq)
				}
				return
			}

			if actualErr != nil {
				t.Fatalf("unexpected error at seq %d: %v", errSeq, actualErr)
			}

			// Discard pending transactions at stream end
			reader.DiscardPending()

			// Validate safe snapshot position
			if reader.SafeSnapshotPosition() != vector.ExpectedSafePosition {
				t.Errorf("safe position mismatch: got %d, expected %d", reader.SafeSnapshotPosition(), vector.ExpectedSafePosition)
			}

			// Validate decoded records
			if len(actualRecords) != len(vector.ExpectedRecords) {
				t.Fatalf("decoded records length mismatch: got %d, expected %d", len(actualRecords), len(vector.ExpectedRecords))
			}

			for i := range actualRecords {
				act := actualRecords[i]
				exp := vector.ExpectedRecords[i]
				if act.Seq != exp.Seq || act.TypeID != exp.TypeID {
					t.Errorf("record[%d] header mismatch: got seq=%d type=%d, expected seq=%d type=%d",
						i, act.Seq, act.TypeID, exp.Seq, exp.TypeID)
				}
			}

			// Validate final table states
			for tableName, expRowsJSON := range vector.ExpectedTables {
				actRows, err := store.Rows(tableName)
				if err != nil {
					t.Fatalf("table %q rows failed: %v", tableName, err)
				}
				if len(actRows) != len(expRowsJSON) {
					t.Fatalf("table %q rows count mismatch: got %d, expected %d", tableName, len(actRows), len(expRowsJSON))
				}

				for rIdx, expRowJSON := range expRowsJSON {
					actRowBytes, err := protojson.Marshal(actRows[rIdx])
					if err != nil {
						t.Fatalf("failed to marshal actual row protojson: %v", err)
					}

					var actMap, expMap any
					if err := json.Unmarshal(actRowBytes, &actMap); err != nil {
						t.Fatalf("failed to unmarshal actual row map: %v", err)
					}
					if err := json.Unmarshal(expRowJSON, &expMap); err != nil {
						t.Fatalf("failed to unmarshal expected row map: %v", err)
					}

					actNorm, _ := json.Marshal(actMap)
					expNorm, _ := json.Marshal(expMap)
					if !bytes.Equal(actNorm, expNorm) {
						t.Errorf("table %q row[%d] mismatch: got %s, expected %s", tableName, rIdx, actNorm, expNorm)
					}
				}
			}
		})
	}
}
