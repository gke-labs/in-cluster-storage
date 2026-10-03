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
	"google.golang.org/protobuf/reflect/protoreflect"
)

// ColumnType represents the logical data type of a derived column.
type ColumnType int

const (
	TypeUnspecified ColumnType = iota
	TypeBool
	TypeInt32
	TypeInt64
	TypeUint32
	TypeUint64
	TypeFloat
	TypeDouble
	TypeString
	TypeBytes
	TypeEnum
	TypeTimestamp
)

// String returns the uppercase name of the logical column type.
func (t ColumnType) String() string {
	switch t {
	case TypeBool:
		return "BOOL"
	case TypeInt32:
		return "INT32"
	case TypeInt64:
		return "INT64"
	case TypeUint32:
		return "UINT32"
	case TypeUint64:
		return "UINT64"
	case TypeFloat:
		return "FLOAT"
	case TypeDouble:
		return "DOUBLE"
	case TypeString:
		return "STRING"
	case TypeBytes:
		return "BYTES"
	case TypeEnum:
		return "ENUM"
	case TypeTimestamp:
		return "TIMESTAMP"
	default:
		return "UNSPECIFIED"
	}
}

// SQLiteType returns the SQLite storage affinity/type for this logical ColumnType.
func (t ColumnType) SQLiteType() string {
	switch t {
	case TypeBool, TypeInt32, TypeInt64, TypeUint32, TypeUint64, TypeEnum, TypeTimestamp:
		return "INTEGER"
	case TypeFloat, TypeDouble:
		return "REAL"
	case TypeString:
		return "TEXT"
	case TypeBytes:
		return "BLOB"
	default:
		return "BLOB"
	}
}

// Column describes a relational/tabular column derived from a protobuf field descriptor.
// It is shared by tabular projections such as SQLite and Parquet.
type Column struct {
	// FieldNumber is the protobuf numeric tag.
	FieldNumber protoreflect.FieldNumber
	// Name is the derived SQL/column name.
	Name string
	// Type is the logical data type of the column.
	Type ColumnType
	// Nullable indicates whether the column is nullable (e.g. proto3 optional, message fields).
	Nullable bool
	// Field is the underlying protobuf field descriptor.
	Field protoreflect.FieldDescriptor
}

// SQLiteType returns the SQLite column type for this column.
func (c Column) SQLiteType() string {
	return c.Type.SQLiteType()
}

// Columns extracts tabular column definitions from a protobuf MessageDescriptor.
//
// Column Derivation Rules:
//   - Scalar fields and google.protobuf.Timestamp are mapped to columns.
//   - Repeated fields, map fields, and other nested message types are excluded from column derivation
//     (they remain preserved losslessly in the full row blob).
//   - Column names default to the protobuf field name (e.g. "ino", "parent_ino", "name").
//   - If a field name collides with internal projection columns ("keydata", "rowdata", "valuedata")
//     or with an earlier derived column in the message, underscores are appended until the name is unique (e.g. "keydata_").
//   - Nullability is true if the field has explicit presence (proto3 optional, proto2 optional, oneof)
//     or is a message field (google.protobuf.Timestamp).
func Columns(md protoreflect.MessageDescriptor) []Column {
	if md == nil {
		return nil
	}

	reservedInternal := map[string]bool{
		"keydata":   true,
		"rowdata":   true,
		"valuedata": true,
	}

	usedNames := make(map[string]bool)
	fields := md.Fields()
	var cols []Column

	for i := 0; i < fields.Len(); i++ {
		f := fields.Get(i)

		// Repeated fields and map fields are excluded
		if f.IsList() || f.IsMap() || f.Cardinality() == protoreflect.Repeated {
			continue
		}

		var colType ColumnType
		var nullable bool

		switch f.Kind() {
		case protoreflect.BoolKind:
			colType = TypeBool
			nullable = f.HasPresence()
		case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
			colType = TypeInt32
			nullable = f.HasPresence()
		case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
			colType = TypeInt64
			nullable = f.HasPresence()
		case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
			colType = TypeUint32
			nullable = f.HasPresence()
		case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
			colType = TypeUint64
			nullable = f.HasPresence()
		case protoreflect.FloatKind:
			colType = TypeFloat
			nullable = f.HasPresence()
		case protoreflect.DoubleKind:
			colType = TypeDouble
			nullable = f.HasPresence()
		case protoreflect.StringKind:
			colType = TypeString
			nullable = f.HasPresence()
		case protoreflect.BytesKind:
			colType = TypeBytes
			nullable = f.HasPresence()
		case protoreflect.EnumKind:
			colType = TypeEnum
			nullable = f.HasPresence()
		case protoreflect.MessageKind:
			if f.Message() != nil && f.Message().FullName() == "google.protobuf.Timestamp" {
				colType = TypeTimestamp
				nullable = true
			} else {
				// Other nested messages are skipped for column derivation
				continue
			}
		default:
			continue
		}

		colName := string(f.Name())
		for reservedInternal[colName] || usedNames[colName] {
			colName += "_"
		}
		usedNames[colName] = true

		cols = append(cols, Column{
			FieldNumber: f.Number(),
			Name:        colName,
			Type:        colType,
			Nullable:    nullable,
			Field:       f,
		})
	}

	return cols
}

// ExtractColumnValue extracts the column value from a protoreflect.Message,
// converting it to standard Go types compatible with database drivers
// (e.g. Timestamp -> unix microseconds, uint64 -> int64 two's complement, enums -> int64).
// Returns nil if the field is unset and nullable.
func ExtractColumnValue(col Column, msg protoreflect.Message) any {
	if msg == nil {
		return nil
	}

	f := col.Field
	if f == nil || f.ContainingMessage() != msg.Descriptor() {
		f = msg.Descriptor().Fields().ByNumber(col.FieldNumber)
	}
	if f == nil {
		return nil
	}

	if !msg.Has(f) {
		if col.Nullable || f.HasPresence() {
			return nil
		}
	}

	switch col.Type {
	case TypeBool:
		if !msg.Has(f) && (col.Nullable || f.HasPresence()) {
			return nil
		}
		if msg.Get(f).Bool() {
			return int64(1)
		}
		return int64(0)

	case TypeInt32, TypeInt64:
		if !msg.Has(f) && (col.Nullable || f.HasPresence()) {
			return nil
		}
		return msg.Get(f).Int()

	case TypeUint32, TypeUint64:
		if !msg.Has(f) && (col.Nullable || f.HasPresence()) {
			return nil
		}
		return int64(msg.Get(f).Uint())

	case TypeFloat, TypeDouble:
		if !msg.Has(f) && (col.Nullable || f.HasPresence()) {
			return nil
		}
		return msg.Get(f).Float()

	case TypeString:
		if !msg.Has(f) && (col.Nullable || f.HasPresence()) {
			return nil
		}
		return msg.Get(f).String()

	case TypeBytes:
		if !msg.Has(f) && (col.Nullable || f.HasPresence()) {
			return nil
		}
		return msg.Get(f).Bytes()

	case TypeEnum:
		if !msg.Has(f) && (col.Nullable || f.HasPresence()) {
			return nil
		}
		return int64(msg.Get(f).Enum())

	case TypeTimestamp:
		if !msg.Has(f) {
			return nil
		}
		tsVal := msg.Get(f).Message()
		if tsVal == nil || !tsVal.IsValid() {
			return nil
		}
		secField := tsVal.Descriptor().Fields().ByNumber(1)
		nanosField := tsVal.Descriptor().Fields().ByNumber(2)
		if secField == nil || nanosField == nil {
			return nil
		}
		sec := tsVal.Get(secField).Int()
		nanos := tsVal.Get(nanosField).Int()
		micros := sec*1_000_000 + nanos/1_000
		return micros

	default:
		return nil
	}
}
