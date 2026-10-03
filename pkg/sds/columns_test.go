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
	"testing"
	"time"

	"github.com/gke-labs/in-cluster-storage/pkg/sds"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func optionalField(name string, number int32, typeKind descriptorpb.FieldDescriptorProto_Type) *descriptorpb.FieldDescriptorProto {
	f := field(name, number, typeKind, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL)
	f.Proto3Optional = proto.Bool(true)
	return f
}

func buildTestMD(t *testing.T, name string, fields []*descriptorpb.FieldDescriptorProto, nestedTypes ...*descriptorpb.DescriptorProto) protoreflect.MessageDescriptor {
	t.Helper()
	tsProto := &descriptorpb.DescriptorProto{
		Name: proto.String("Timestamp"),
		Field: []*descriptorpb.FieldDescriptorProto{
			field("seconds", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
			field("nanos", 2, descriptorpb.FieldDescriptorProto_TYPE_INT32, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		},
	}
	tsFile := &descriptorpb.FileDescriptorProto{
		Name:        proto.String("google/protobuf/timestamp.proto"),
		Package:     proto.String("google.protobuf"),
		MessageType: []*descriptorpb.DescriptorProto{tsProto},
		Syntax:      proto.String("proto3"),
	}

	var oneofDecls []*descriptorpb.OneofDescriptorProto
	for _, f := range fields {
		if f.GetProto3Optional() {
			f.OneofIndex = proto.Int32(int32(len(oneofDecls)))
			oneofDecls = append(oneofDecls, &descriptorpb.OneofDescriptorProto{
				Name: proto.String("_" + f.GetName()),
			})
		}
	}

	msgProto := &descriptorpb.DescriptorProto{
		Name:       proto.String(name),
		Field:      fields,
		OneofDecl:  oneofDecls,
		NestedType: nestedTypes,
	}

	mainFile := &descriptorpb.FileDescriptorProto{
		Name:        proto.String(name + ".proto"),
		Package:     proto.String("testpkg"),
		MessageType: []*descriptorpb.DescriptorProto{msgProto},
		Dependency:  []string{"google/protobuf/timestamp.proto"},
		Syntax:      proto.String("proto3"),
	}

	fds := &descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{tsFile, mainFile},
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

func TestColumns_DerivationAndTypes(t *testing.T) {
	nestedChild := &descriptorpb.DescriptorProto{
		Name: proto.String("ChildMsg"),
		Field: []*descriptorpb.FieldDescriptorProto{
			field("child_val", 1, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		},
	}

	fields := []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("name", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("active", 3, descriptorpb.FieldDescriptorProto_TYPE_BOOL, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("score", 4, descriptorpb.FieldDescriptorProto_TYPE_DOUBLE, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("ratio", 5, descriptorpb.FieldDescriptorProto_TYPE_FLOAT, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("count32", 6, descriptorpb.FieldDescriptorProto_TYPE_INT32, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("ucount32", 7, descriptorpb.FieldDescriptorProto_TYPE_UINT32, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("ucount64", 8, descriptorpb.FieldDescriptorProto_TYPE_UINT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("data", 9, descriptorpb.FieldDescriptorProto_TYPE_BYTES, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		optionalField("opt_int", 10, descriptorpb.FieldDescriptorProto_TYPE_INT64),
		{
			Name:     proto.String("created_at"),
			Number:   proto.Int32(11),
			Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
			TypeName: proto.String(".google.protobuf.Timestamp"),
			Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
		},
		// Nested message (not Timestamp) - should be skipped
		{
			Name:     proto.String("child"),
			Number:   proto.Int32(12),
			Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
			TypeName: proto.String(".testpkg.TestMsg.ChildMsg"),
			Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
		},
		// Repeated field - should be skipped
		field("tags", 13, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_REPEATED),
		// Reserved collision with projection internal column
		field("keydata", 14, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("rowdata", 15, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	}

	md := buildTestMD(t, "TestMsg", fields, nestedChild)
	cols := sds.Columns(md)

	expectedCols := []struct {
		num      int32
		name     string
		colType  sds.ColumnType
		sqlType  string
		nullable bool
	}{
		{1, "id", sds.TypeInt64, "INTEGER", false},
		{2, "name", sds.TypeString, "TEXT", false},
		{3, "active", sds.TypeBool, "INTEGER", false},
		{4, "score", sds.TypeDouble, "REAL", false},
		{5, "ratio", sds.TypeFloat, "REAL", false},
		{6, "count32", sds.TypeInt32, "INTEGER", false},
		{7, "ucount32", sds.TypeUint32, "INTEGER", false},
		{8, "ucount64", sds.TypeUint64, "INTEGER", false},
		{9, "data", sds.TypeBytes, "BLOB", false},
		{10, "opt_int", sds.TypeInt64, "INTEGER", true},
		{11, "created_at", sds.TypeTimestamp, "INTEGER", true},
		{14, "keydata_", sds.TypeString, "TEXT", false},
		{15, "rowdata_", sds.TypeString, "TEXT", false},
	}

	if len(cols) != len(expectedCols) {
		t.Fatalf("got %d columns, expected %d", len(cols), len(expectedCols))
	}

	for i, exp := range expectedCols {
		col := cols[i]
		if int32(col.FieldNumber) != exp.num {
			t.Errorf("col[%d] FieldNumber: got %d, want %d", i, col.FieldNumber, exp.num)
		}
		if col.Name != exp.name {
			t.Errorf("col[%d] Name: got %q, want %q", i, col.Name, exp.name)
		}
		if col.Type != exp.colType {
			t.Errorf("col[%d] Type: got %v, want %v", i, col.Type, exp.colType)
		}
		if col.SQLiteType() != exp.sqlType {
			t.Errorf("col[%d] SQLiteType: got %q, want %q", i, col.SQLiteType(), exp.sqlType)
		}
		if col.Nullable != exp.nullable {
			t.Errorf("col[%d] Nullable: got %v, want %v", i, col.Nullable, exp.nullable)
		}
	}
}

func TestExtractColumnValue(t *testing.T) {
	fields := []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("name", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("active", 3, descriptorpb.FieldDescriptorProto_TYPE_BOOL, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		optionalField("opt_int", 4, descriptorpb.FieldDescriptorProto_TYPE_INT64),
		{
			Name:     proto.String("created_at"),
			Number:   proto.Int32(5),
			Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
			TypeName: proto.String(".google.protobuf.Timestamp"),
			Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
		},
	}

	md := buildTestMD(t, "ExtractMsg", fields)
	cols := sds.Columns(md)

	// Build a dynamic message
	dynMsg := dynamicpb.NewMessage(md)
	fID := md.Fields().ByNumber(1)
	fName := md.Fields().ByNumber(2)
	fActive := md.Fields().ByNumber(3)
	fCreatedAt := md.Fields().ByNumber(5)

	dynMsg.Set(fID, protoreflect.ValueOfInt64(42))
	dynMsg.Set(fName, protoreflect.ValueOfString("test-row"))
	dynMsg.Set(fActive, protoreflect.ValueOfBool(true))

	now := time.Unix(1700000000, 123456000)
	ts := timestamppb.New(now)
	tsDyn := dynamicpb.NewMessage(fCreatedAt.Message())
	tsDyn.Set(tsDyn.Descriptor().Fields().ByNumber(1), protoreflect.ValueOfInt64(ts.GetSeconds()))
	tsDyn.Set(tsDyn.Descriptor().Fields().ByNumber(2), protoreflect.ValueOfInt32(ts.GetNanos()))
	dynMsg.Set(fCreatedAt, protoreflect.ValueOfMessage(tsDyn))

	mReflect := dynMsg.ProtoReflect()

	// 1. id
	if v := sds.ExtractColumnValue(cols[0], mReflect); v != int64(42) {
		t.Errorf("id: got %v, want 42", v)
	}

	// 2. name
	if v := sds.ExtractColumnValue(cols[1], mReflect); v != "test-row" {
		t.Errorf("name: got %v, want %q", v, "test-row")
	}

	// 3. active
	if v := sds.ExtractColumnValue(cols[2], mReflect); v != int64(1) {
		t.Errorf("active: got %v, want 1", v)
	}

	// 4. opt_int (unset nullable)
	if v := sds.ExtractColumnValue(cols[3], mReflect); v != nil {
		t.Errorf("opt_int (unset): got %v, want nil", v)
	}

	// 5. created_at
	expectedMicros := now.UnixMicro()
	if v := sds.ExtractColumnValue(cols[4], mReflect); v != expectedMicros {
		t.Errorf("created_at: got %v, want %d", v, expectedMicros)
	}
}
