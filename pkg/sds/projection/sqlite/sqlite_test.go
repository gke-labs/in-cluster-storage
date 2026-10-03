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

package sqlite_test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gke-labs/in-cluster-storage/pkg/objectstore/inmemorystorage"
	"github.com/gke-labs/in-cluster-storage/pkg/sds"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/memtable"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/projection/sqlite"
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

func field(name string, number int32, typeKind descriptorpb.FieldDescriptorProto_Type, label descriptorpb.FieldDescriptorProto_Label) *descriptorpb.FieldDescriptorProto {
	return &descriptorpb.FieldDescriptorProto{
		Name:   proto.String(name),
		Number: proto.Int32(number),
		Type:   typeKind.Enum(),
		Label:  label.Enum(),
	}
}

func optionalField(name string, number int32, typeKind descriptorpb.FieldDescriptorProto_Type) *descriptorpb.FieldDescriptorProto {
	f := field(name, number, typeKind, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL)
	f.Proto3Optional = proto.Bool(true)
	return f
}

func buildMD(t *testing.T, name string, fields []*descriptorpb.FieldDescriptorProto, nestedTypes ...*descriptorpb.DescriptorProto) protoreflect.MessageDescriptor {
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

func verifySQLiteMatchesMemStore(t *testing.T, ctx context.Context, db *sqlite.DB, store *memtable.MemStore) {
	t.Helper()

	if db.Position() != store.Position() {
		t.Errorf("position mismatch: sqlite %d != memstore %d", db.Position(), store.Position())
	}

	tables := store.Tables()
	for _, fullTableName := range tables {
		memTable := store.Table(fullTableName)
		sqlTableName := sqlite.TableName(fullTableName)

		// Check row count in SQLite
		var count int
		countSQL := fmt.Sprintf("SELECT COUNT(*) FROM %q;", sqlTableName)
		if err := db.SQLDB().QueryRowContext(ctx, countSQL).Scan(&count); err != nil {
			t.Fatalf("failed to query count for %q: %v", sqlTableName, err)
		}

		if count != memTable.Count() {
			t.Errorf("table %q row count mismatch: sqlite %d != memstore %d", fullTableName, count, memTable.Count())
		}

		// Read all (keydata, rowdata) rows from SQLite
		querySQL := fmt.Sprintf("SELECT keydata, rowdata FROM %q;", sqlTableName)
		rows, err := db.SQLDB().QueryContext(ctx, querySQL)
		if err != nil {
			t.Fatalf("failed to query rows from %q: %v", sqlTableName, err)
		}
		defer rows.Close()

		sqliteRows := make(map[string]proto.Message)
		for rows.Next() {
			var keydata, rowdata []byte
			if err := rows.Scan(&keydata, &rowdata); err != nil {
				t.Fatalf("failed to scan row for %q: %v", sqlTableName, err)
			}

			def, _, ok := db.Registry().LookupByName(fullTableName)
			if !ok {
				t.Fatalf("type not found for table %q in registry", fullTableName)
			}

			msgType, err := db.Registry().ResolveMessageType(def.GetId())
			if err != nil {
				t.Fatalf("failed to resolve message type for %q: %v", fullTableName, err)
			}

			msg := msgType.New().Interface()
			if err := proto.Unmarshal(rowdata, msg); err != nil {
				t.Fatalf("failed to unmarshal rowdata for %q: %v", fullTableName, err)
			}

			key, err := sds.ExtractKey(msg, def.GetKeyFields())
			if err != nil {
				t.Fatalf("failed to extract key from sqlite row: %v", err)
			}

			sqliteRows[key.String()] = msg
		}

		// Compare with memtable rows
		for _, memRow := range memTable.Rows() {
			def, _, _ := db.Registry().LookupByName(fullTableName)
			key, err := sds.ExtractKey(memRow, def.GetKeyFields())
			if err != nil {
				t.Fatalf("failed to extract key from memtable row: %v", err)
			}

			sqlRow, ok := sqliteRows[key.String()]
			if !ok {
				t.Errorf("row with key %s in memtable missing from sqlite table %q", key.String(), fullTableName)
				continue
			}

			memBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(memRow)
			if err != nil {
				t.Fatalf("failed to marshal memtable row: %v", err)
			}
			sqlBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(sqlRow)
			if err != nil {
				t.Fatalf("failed to marshal sqlite row: %v", err)
			}

			if !bytes.Equal(memBytes, sqlBytes) {
				t.Errorf("row mismatch for key %s in table %q:\n  memstore: %v\n  sqlite:   %v", key.String(), fullTableName, memRow, sqlRow)
			}

			// Also verify db.Get convenience method
			getMsg, getOk, getErr := db.Get(ctx, fullTableName, key)
			if getErr != nil {
				t.Errorf("db.Get error for %s: %v", key.String(), getErr)
			} else if !getOk {
				t.Errorf("db.Get returned false for %s", key.String())
			} else {
				getBytes, _ := proto.MarshalOptions{Deterministic: true}.Marshal(getMsg)
				if !bytes.Equal(memBytes, getBytes) {
					t.Errorf("db.Get mismatch for key %s", key.String())
				}
			}
		}
	}
}

func TestReplayAndCompareWithMemTable(t *testing.T) {
	ctx := t.Context()
	appender := &memoryAppender{}
	writer := sds.NewWriter(appender)

	userMD := buildMD(t, "User", []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("name", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("email", 3, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	})
	writer.RegisterDescriptor(userMD, 1)

	orderMD := buildMD(t, "Order", []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("user_id", 2, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("amount", 3, descriptorpb.FieldDescriptorProto_TYPE_DOUBLE, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	})
	writer.RegisterDescriptor(orderMD, 1)

	// Step 1: Autocommit Inserts
	u1 := dynamicpb.NewMessage(userMD)
	u1.Set(userMD.Fields().ByName("id"), protoreflect.ValueOfInt64(1))
	u1.Set(userMD.Fields().ByName("name"), protoreflect.ValueOfString("Alice"))
	u1.Set(userMD.Fields().ByName("email"), protoreflect.ValueOfString("alice@example.com"))
	writer.Insert(ctx, u1)

	u2 := dynamicpb.NewMessage(userMD)
	u2.Set(userMD.Fields().ByName("id"), protoreflect.ValueOfInt64(2))
	u2.Set(userMD.Fields().ByName("name"), protoreflect.ValueOfString("Bob"))
	u2.Set(userMD.Fields().ByName("email"), protoreflect.ValueOfString("bob@example.com"))
	writer.Insert(ctx, u2)

	// Step 2: Multi-record Transaction
	tx := writer.Begin()
	o1 := dynamicpb.NewMessage(orderMD)
	o1.Set(orderMD.Fields().ByName("id"), protoreflect.ValueOfInt64(101))
	o1.Set(orderMD.Fields().ByName("user_id"), protoreflect.ValueOfInt64(1))
	o1.Set(orderMD.Fields().ByName("amount"), protoreflect.ValueOfFloat64(99.50))
	tx.Insert(ctx, o1)

	u1Up := dynamicpb.NewMessage(userMD)
	u1Up.Set(userMD.Fields().ByName("id"), protoreflect.ValueOfInt64(1))
	u1Up.Set(userMD.Fields().ByName("name"), protoreflect.ValueOfString("Alice Wonderland"))
	u1Up.Set(userMD.Fields().ByName("email"), protoreflect.ValueOfString("alice.w@example.com"))
	tx.Update(ctx, u1Up)
	tx.Commit(ctx)

	// Step 3: Autocommit Delete
	u2Del := dynamicpb.NewMessage(userMD)
	u2Del.Set(userMD.Fields().ByName("id"), protoreflect.ValueOfInt64(2))
	writer.Delete(ctx, u2Del)

	// Feed all payloads to both MemStore and SQLite
	payloads := appender.Payloads()

	memStore := memtable.New()
	memReader := sds.NewChangeReader()

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.sqlite")
	sqliteDB, err := sqlite.Open(ctx, dbPath, sqlite.WithStreamID("test-stream-1"))
	if err != nil {
		t.Fatalf("sqlite.Open failed: %v", err)
	}
	defer sqliteDB.Close()

	for i, payload := range payloads {
		seq := uint64(i + 1)
		changes, err := memReader.Feed(seq, payload)
		if err != nil {
			t.Fatalf("memReader.Feed failed: %v", err)
		}
		if err := memStore.ApplyBatch(changes); err != nil {
			t.Fatalf("memStore.ApplyBatch failed: %v", err)
		}

		if _, err := sqliteDB.Feed(ctx, seq, payload); err != nil {
			t.Fatalf("sqliteDB.Feed failed: %v", err)
		}
	}

	// Verify SQLite exactly matches MemStore
	verifySQLiteMatchesMemStore(t, ctx, sqliteDB, memStore)
}

func TestPlainSQLQueries(t *testing.T) {
	ctx := t.Context()
	appender := &memoryAppender{}
	writer := sds.NewWriter(appender)

	inodeMD := buildMD(t, "Inode", []*descriptorpb.FieldDescriptorProto{
		field("ino", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("size", 2, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("mode", 3, descriptorpb.FieldDescriptorProto_TYPE_UINT32, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("is_dir", 4, descriptorpb.FieldDescriptorProto_TYPE_BOOL, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("ratio", 5, descriptorpb.FieldDescriptorProto_TYPE_DOUBLE, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("name", 6, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("sha256", 7, descriptorpb.FieldDescriptorProto_TYPE_BYTES, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		{
			Name:     proto.String("mtime"),
			Number:   proto.Int32(8),
			Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
			TypeName: proto.String(".google.protobuf.Timestamp"),
			Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
		},
	})
	writer.RegisterDescriptor(inodeMD, 1)

	// Insert test rows
	now := time.Unix(1700000000, 500000000)
	ts1 := timestamppb.New(now)
	tsDyn1 := dynamicpb.NewMessage(inodeMD.Fields().ByName("mtime").Message())
	tsDyn1.Set(tsDyn1.Descriptor().Fields().ByNumber(1), protoreflect.ValueOfInt64(ts1.GetSeconds()))
	tsDyn1.Set(tsDyn1.Descriptor().Fields().ByNumber(2), protoreflect.ValueOfInt32(ts1.GetNanos()))

	in1 := dynamicpb.NewMessage(inodeMD)
	in1.Set(inodeMD.Fields().ByName("ino"), protoreflect.ValueOfInt64(100))
	in1.Set(inodeMD.Fields().ByName("size"), protoreflect.ValueOfInt64(4096))
	in1.Set(inodeMD.Fields().ByName("mode"), protoreflect.ValueOfUint32(0755))
	in1.Set(inodeMD.Fields().ByName("is_dir"), protoreflect.ValueOfBool(true))
	in1.Set(inodeMD.Fields().ByName("ratio"), protoreflect.ValueOfFloat64(1.25))
	in1.Set(inodeMD.Fields().ByName("name"), protoreflect.ValueOfString("root_dir"))
	in1.Set(inodeMD.Fields().ByName("sha256"), protoreflect.ValueOfBytes([]byte{0xaa, 0xbb, 0xcc}))
	in1.Set(inodeMD.Fields().ByName("mtime"), protoreflect.ValueOfMessage(tsDyn1))
	writer.Insert(ctx, in1)

	in2 := dynamicpb.NewMessage(inodeMD)
	in2.Set(inodeMD.Fields().ByName("ino"), protoreflect.ValueOfInt64(101))
	in2.Set(inodeMD.Fields().ByName("size"), protoreflect.ValueOfInt64(1024))
	in2.Set(inodeMD.Fields().ByName("mode"), protoreflect.ValueOfUint32(0644))
	in2.Set(inodeMD.Fields().ByName("is_dir"), protoreflect.ValueOfBool(false))
	in2.Set(inodeMD.Fields().ByName("ratio"), protoreflect.ValueOfFloat64(0.50))
	in2.Set(inodeMD.Fields().ByName("name"), protoreflect.ValueOfString("file.txt"))
	in2.Set(inodeMD.Fields().ByName("sha256"), protoreflect.ValueOfBytes([]byte{0x01, 0x02, 0x03}))
	writer.Insert(ctx, in2)

	dbPath := filepath.Join(t.TempDir(), "sql_test.sqlite")
	db, err := sqlite.Open(ctx, dbPath, sqlite.WithStreamID("stream-sql"))
	if err != nil {
		t.Fatalf("sqlite.Open failed: %v", err)
	}
	defer db.Close()

	for i, p := range appender.Payloads() {
		seq := uint64(i + 1)
		if _, err := db.Feed(ctx, seq, p); err != nil {
			t.Fatalf("Feed failed: %v", err)
		}
	}

	// 1. Plain SQL query with filters and projections
	var ino, size int64
	var mode uint32
	var isDir bool
	var ratio float64
	var name string
	var sha256 []byte
	var mtimeMicros sql.NullInt64

	row := db.SQLDB().QueryRowContext(ctx, `
		SELECT ino, size, mode, is_dir, ratio, name, sha256, mtime
		FROM testpkg_Inode
		WHERE ino = 100;
	`)
	if err := row.Scan(&ino, &size, &mode, &isDir, &ratio, &name, &sha256, &mtimeMicros); err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	if ino != 100 || size != 4096 || mode != 0755 || !isDir || ratio != 1.25 || name != "root_dir" {
		t.Errorf("unexpected values scanned for ino 100: ino=%d, size=%d, mode=%o, isDir=%v, ratio=%f, name=%s",
			ino, size, mode, isDir, ratio, name)
	}
	if !bytes.Equal(sha256, []byte{0xaa, 0xbb, 0xcc}) {
		t.Errorf("sha256 mismatch: got %x", sha256)
	}
	if !mtimeMicros.Valid || mtimeMicros.Int64 != now.UnixMicro() {
		t.Errorf("mtime mismatch: valid=%v, got=%d, want=%d", mtimeMicros.Valid, mtimeMicros.Int64, now.UnixMicro())
	}

	// 2. Query with non-set timestamp returning NULL
	row2 := db.SQLDB().QueryRowContext(ctx, `SELECT is_dir, mtime FROM testpkg_Inode WHERE ino = 101;`)
	if err := row2.Scan(&isDir, &mtimeMicros); err != nil {
		t.Fatalf("Scan row 2 failed: %v", err)
	}
	if isDir {
		t.Errorf("expected is_dir = false for ino 101")
	}
	if mtimeMicros.Valid {
		t.Errorf("expected NULL mtime for ino 101, got %d", mtimeMicros.Int64)
	}

	// 3. Plain SQL aggregation / ORDER BY
	var totalSize int64
	if err := db.SQLDB().QueryRowContext(ctx, "SELECT SUM(size) FROM testpkg_Inode;").Scan(&totalSize); err != nil {
		t.Fatalf("SUM query failed: %v", err)
	}
	if totalSize != 5120 {
		t.Errorf("totalSize = %d, want 5120", totalSize)
	}
}

func TestCompatibleSchemaEvolution_MidStreamNullForOldRows(t *testing.T) {
	ctx := t.Context()
	appender := &memoryAppender{}
	writer := sds.NewWriter(appender)

	// Schema V1: (id tag 1, name tag 2)
	fieldsV1 := []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("name", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	}
	mdV1 := buildMD(t, "Article", fieldsV1)
	writer.RegisterDescriptor(mdV1, 1)

	// Insert row 1 and row 2 under V1
	a1 := dynamicpb.NewMessage(mdV1)
	a1.Set(mdV1.Fields().ByName("id"), protoreflect.ValueOfInt64(1))
	a1.Set(mdV1.Fields().ByName("name"), protoreflect.ValueOfString("Post 1"))
	writer.Insert(ctx, a1)

	a2 := dynamicpb.NewMessage(mdV1)
	a2.Set(mdV1.Fields().ByName("id"), protoreflect.ValueOfInt64(2))
	a2.Set(mdV1.Fields().ByName("name"), protoreflect.ValueOfString("Post 2"))
	writer.Insert(ctx, a2)

	// Schema V2: evolved with category tag 3, views tag 4
	fieldsV2 := []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("name", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		optionalField("category", 3, descriptorpb.FieldDescriptorProto_TYPE_STRING),
		field("views", 4, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	}
	mdV2 := buildMD(t, "Article", fieldsV2)
	writer.RegisterDescriptor(mdV2, 1)

	// Insert row 3 under V2
	a3 := dynamicpb.NewMessage(mdV2)
	a3.Set(mdV2.Fields().ByName("id"), protoreflect.ValueOfInt64(3))
	a3.Set(mdV2.Fields().ByName("name"), protoreflect.ValueOfString("Post 3"))
	a3.Set(mdV2.Fields().ByName("category"), protoreflect.ValueOfString("Tech"))
	a3.Set(mdV2.Fields().ByName("views"), protoreflect.ValueOfInt64(500))
	writer.Insert(ctx, a3)

	dbPath := filepath.Join(t.TempDir(), "evolve_null.sqlite")
	db, err := sqlite.Open(ctx, dbPath, sqlite.WithStreamID("stream-evolve-null"))
	if err != nil {
		t.Fatalf("sqlite.Open failed: %v", err)
	}
	defer db.Close()

	for i, p := range appender.Payloads() {
		seq := uint64(i + 1)
		if _, err := db.Feed(ctx, seq, p); err != nil {
			t.Fatalf("Feed failed at seq %d: %v", seq, err)
		}
	}

	// Verify old rows have NULL for newly added category column
	var cat1 sql.NullString
	var views1 sql.NullInt64
	err = db.SQLDB().QueryRowContext(ctx, "SELECT category, views FROM testpkg_Article WHERE id = 1;").Scan(&cat1, &views1)
	if err != nil {
		t.Fatalf("Query row 1 failed: %v", err)
	}
	if cat1.Valid {
		t.Errorf("expected NULL category for row 1, got %q", cat1.String)
	}
	if views1.Valid {
		t.Errorf("expected NULL views for row 1 from ALTER TABLE, got %d", views1.Int64)
	}

	// Verify row 3 has category = 'Tech' and views = 500
	var cat3 string
	var views3 int64
	err = db.SQLDB().QueryRowContext(ctx, "SELECT category, views FROM testpkg_Article WHERE id = 3;").Scan(&cat3, &views3)
	if err != nil {
		t.Fatalf("Query row 3 failed: %v", err)
	}
	if cat3 != "Tech" || views3 != 500 {
		t.Errorf("row 3 values: got category=%q, views=%d, want 'Tech', 500", cat3, views3)
	}
}

func TestSecondaryIndex_ExplainQueryPlan(t *testing.T) {
	ctx := t.Context()
	appender := &memoryAppender{}
	writer := sds.NewWriter(appender)

	dirEntryMD := buildMD(t, "DirEntry", []*descriptorpb.FieldDescriptorProto{
		field("parent_ino", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("name", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("ino", 3, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("type", 4, descriptorpb.FieldDescriptorProto_TYPE_UINT32, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	})
	writer.RegisterDescriptor(dirEntryMD, 1, 2) // Composite PK: parent_ino, name

	// Insert directory entries
	for i := 1; i <= 20; i++ {
		de := dynamicpb.NewMessage(dirEntryMD)
		de.Set(dirEntryMD.Fields().ByName("parent_ino"), protoreflect.ValueOfInt64(1))
		de.Set(dirEntryMD.Fields().ByName("name"), protoreflect.ValueOfString(fmt.Sprintf("file_%02d.txt", i)))
		de.Set(dirEntryMD.Fields().ByName("ino"), protoreflect.ValueOfInt64(int64(100+i)))
		de.Set(dirEntryMD.Fields().ByName("type"), protoreflect.ValueOfUint32(1))
		writer.Insert(ctx, de)
	}

	dbPath := filepath.Join(t.TempDir(), "indexed.sqlite")
	// Declare secondary index on DirEntry.ino
	db, err := sqlite.Open(ctx, dbPath,
		sqlite.WithStreamID("stream-idx"),
		sqlite.WithIndex("testpkg.DirEntry", "ino"),
	)
	if err != nil {
		t.Fatalf("sqlite.Open failed: %v", err)
	}
	defer db.Close()

	for i, p := range appender.Payloads() {
		seq := uint64(i + 1)
		if _, err := db.Feed(ctx, seq, p); err != nil {
			t.Fatalf("Feed failed: %v", err)
		}
	}

	// Verify secondary index exists and is used in EXPLAIN QUERY PLAN
	queryPlanSQL := `EXPLAIN QUERY PLAN SELECT * FROM testpkg_DirEntry WHERE ino = 105;`
	rows, err := db.SQLDB().QueryContext(ctx, queryPlanSQL)
	if err != nil {
		t.Fatalf("EXPLAIN QUERY PLAN failed: %v", err)
	}
	defer rows.Close()

	var planDetails []string
	for rows.Next() {
		var id, parent, notused int
		var detail string
		if err := rows.Scan(&id, &parent, &notused, &detail); err != nil {
			t.Fatalf("Scan plan row failed: %v", err)
		}
		planDetails = append(planDetails, detail)
	}

	fullPlan := strings.Join(planDetails, "\n")
	if !strings.Contains(fullPlan, "USING INDEX idx_testpkg_DirEntry_ino") && !strings.Contains(fullPlan, "USING INDEX") {
		t.Errorf("expected query plan to use secondary index, got:\n%s", fullPlan)
	}
}

func TestFullRowColumn_LosslessRoundTrip(t *testing.T) {
	ctx := t.Context()
	appender := &memoryAppender{}
	writer := sds.NewWriter(appender)

	nestedChild := &descriptorpb.DescriptorProto{
		Name: proto.String("Metadata"),
		Field: []*descriptorpb.FieldDescriptorProto{
			field("author", 1, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
			field("tags", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_REPEATED),
		},
	}

	docMD := buildMD(t, "Document", []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("title", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		{
			Name:     proto.String("meta"),
			Number:   proto.Int32(3),
			Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
			TypeName: proto.String(".testpkg.Document.Metadata"),
			Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
		},
		field("keywords", 4, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_REPEATED),
	}, nestedChild)
	writer.RegisterDescriptor(docMD, 1)

	// Create document with nested message and repeated fields
	doc := dynamicpb.NewMessage(docMD)
	doc.Set(docMD.Fields().ByName("id"), protoreflect.ValueOfInt64(42))
	doc.Set(docMD.Fields().ByName("title"), protoreflect.ValueOfString("Architecture Guide"))

	metaField := docMD.Fields().ByName("meta")
	metaMsg := dynamicpb.NewMessage(metaField.Message())
	metaMsg.Set(metaField.Message().Fields().ByName("author"), protoreflect.ValueOfString("Google"))
	metaTags := metaMsg.Mutable(metaField.Message().Fields().ByName("tags")).List()
	metaTags.Append(protoreflect.ValueOfString("storage"))
	metaTags.Append(protoreflect.ValueOfString("database"))
	doc.Set(metaField, protoreflect.ValueOfMessage(metaMsg))

	keywords := doc.Mutable(docMD.Fields().ByName("keywords")).List()
	keywords.Append(protoreflect.ValueOfString("sqlite"))
	keywords.Append(protoreflect.ValueOfString("sds"))
	keywords.Append(protoreflect.ValueOfString("oltp"))

	writer.Insert(ctx, doc)

	dbPath := filepath.Join(t.TempDir(), "roundtrip.sqlite")
	db, err := sqlite.Open(ctx, dbPath, sqlite.WithStreamID("stream-roundtrip"))
	if err != nil {
		t.Fatalf("sqlite.Open failed: %v", err)
	}
	defer db.Close()

	for i, p := range appender.Payloads() {
		seq := uint64(i + 1)
		if _, err := db.Feed(ctx, seq, p); err != nil {
			t.Fatalf("Feed failed: %v", err)
		}
	}

	// 1. Read rowdata directly from SQLite table
	var rowdata []byte
	err = db.SQLDB().QueryRowContext(ctx, "SELECT rowdata FROM testpkg_Document WHERE id = 42;").Scan(&rowdata)
	if err != nil {
		t.Fatalf("Query rowdata failed: %v", err)
	}

	// 2. Canonical serialization of original doc
	expectedBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(doc)
	if err != nil {
		t.Fatalf("Marshal doc failed: %v", err)
	}

	// 3. Verify byte-for-byte equivalence
	if !bytes.Equal(rowdata, expectedBytes) {
		t.Errorf("rowdata byte mismatch:\n  got:  %x\n  want: %x", rowdata, expectedBytes)
	}

	// 4. Unmarshal rowdata and check proto equality
	reconstructed := dynamicpb.NewMessage(docMD)
	if err := proto.Unmarshal(rowdata, reconstructed); err != nil {
		t.Fatalf("Unmarshal rowdata failed: %v", err)
	}
	if !proto.Equal(doc, reconstructed) {
		t.Errorf("proto mismatch:\n  got:  %v\n  want: %v", reconstructed, doc)
	}

	// 5. Test db.Get and db.Rows convenience methods
	k42, _ := sds.ExtractKey(doc, []int32{1})
	gotMsg, ok, err := db.Get(ctx, "testpkg.Document", k42)
	if err != nil || !ok {
		t.Fatalf("db.Get failed: ok=%v, err=%v", ok, err)
	}
	gotBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(gotMsg)
	if err != nil {
		t.Fatalf("Marshal gotMsg failed: %v", err)
	}
	if !bytes.Equal(expectedBytes, gotBytes) {
		t.Errorf("db.Get bytes mismatch:\n  got:  %x\n  want: %x", gotBytes, expectedBytes)
	}

	allRows, err := db.Rows(ctx, "testpkg.Document")
	if err != nil {
		t.Fatalf("db.Rows failed: %v", err)
	}
	if len(allRows) != 1 {
		t.Fatalf("db.Rows expected 1 row, got %d", len(allRows))
	}
	row0Bytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(allRows[0])
	if err != nil {
		t.Fatalf("Marshal row0 failed: %v", err)
	}
	if !bytes.Equal(expectedBytes, row0Bytes) {
		t.Errorf("db.Rows bytes mismatch:\n  got:  %x\n  want: %x", row0Bytes, expectedBytes)
	}
}

func TestSnapshotRestoreAndTailFollow(t *testing.T) {
	ctx := t.Context()
	appender := &memoryAppender{}
	writer := sds.NewWriter(appender)
	backend := inmemorystorage.New()
	streamID := "stream-snap-test"

	itemMD := buildMD(t, "Item", []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("val", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	})
	writer.RegisterDescriptor(itemMD, 1)

	// Record 1..10
	for i := 1; i <= 10; i++ {
		item := dynamicpb.NewMessage(itemMD)
		item.Set(itemMD.Fields().ByName("id"), protoreflect.ValueOfInt64(int64(i)))
		item.Set(itemMD.Fields().ByName("val"), protoreflect.ValueOfString(fmt.Sprintf("item-%d", i)))
		writer.Insert(ctx, item)
	}

	payloadsPart1 := appender.Payloads()
	snapKey, snapPos, err := sqlite.BuildSnapshot(ctx, streamID, payloadsPart1, backend, t.TempDir())
	if err != nil {
		t.Fatalf("BuildSnapshot failed: %v", err)
	}
	if snapPos != uint64(len(payloadsPart1)) {
		t.Errorf("snapPos = %d, want %d", snapPos, len(payloadsPart1))
	}

	// Record 11..20 (live tail)
	for i := 11; i <= 20; i++ {
		item := dynamicpb.NewMessage(itemMD)
		item.Set(itemMD.Fields().ByName("id"), protoreflect.ValueOfInt64(int64(i)))
		item.Set(itemMD.Fields().ByName("val"), protoreflect.ValueOfString(fmt.Sprintf("item-%d", i)))
		writer.Insert(ctx, item)
	}

	allPayloads := appender.Payloads()

	// Compute reference state in MemStore
	memStore := memtable.New()
	memReader := sds.NewChangeReader()
	for i, p := range allPayloads {
		changes, _ := memReader.Feed(uint64(i+1), p)
		memStore.ApplyBatch(changes)
	}

	// Restore SQLite projection from snapshot
	restoredPath := filepath.Join(t.TempDir(), "restored.sqlite")
	restoredDB, restoredPos, err := sqlite.RestoreSnapshot(ctx, backend, streamID, snapPos, restoredPath)
	if err != nil {
		t.Fatalf("RestoreSnapshot failed: %v", err)
	}
	defer restoredDB.Close()

	if restoredPos != snapPos {
		t.Errorf("restoredPos = %d, want %d", restoredPos, snapPos)
	}

	// Apply live tail (records from snapPos to end)
	for i := int(snapPos); i < len(allPayloads); i++ {
		seq := uint64(i + 1)
		if _, err := restoredDB.Feed(ctx, seq, allPayloads[i]); err != nil {
			t.Fatalf("restoredDB.Feed tail at seq %d failed: %v", seq, err)
		}
	}

	// Restored + replayed state must equal full reference
	verifySQLiteMatchesMemStore(t, ctx, restoredDB, memStore)

	// Test publishing snapshot from active DB
	pubKey, pubPos, err := sqlite.PublishSnapshot(ctx, restoredDB, backend, t.TempDir())
	if err != nil {
		t.Fatalf("PublishSnapshot failed: %v", err)
	}
	if pubPos != uint64(len(allPayloads)) {
		t.Errorf("pubPos = %d, want %d", pubPos, len(allPayloads))
	}
	if pubKey != sqlite.SnapshotKey(streamID, pubPos) {
		t.Errorf("pubKey = %q, want %q", pubKey, sqlite.SnapshotKey(streamID, pubPos))
	}

	_ = snapKey
}

func TestCompatibleSchemaEvolution(t *testing.T) {
	ctx := t.Context()
	appender := &memoryAppender{}
	writer := sds.NewWriter(appender)

	// Schema V1: Order (id tag 1, customer tag 2)
	fieldsV1 := []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("customer", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	}
	mdV1 := buildMD(t, "Order", fieldsV1)
	writer.RegisterDescriptor(mdV1, 1)

	// Insert row under V1
	o1 := dynamicpb.NewMessage(mdV1)
	o1.Set(mdV1.Fields().ByName("id"), protoreflect.ValueOfInt64(101))
	o1.Set(mdV1.Fields().ByName("customer"), protoreflect.ValueOfString("Alice"))
	writer.Insert(ctx, o1)

	// Schema V2: Order evolved with total tag 3, discount tag 4
	fieldsV2 := []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("customer", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("total", 3, descriptorpb.FieldDescriptorProto_TYPE_DOUBLE, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("discount", 4, descriptorpb.FieldDescriptorProto_TYPE_DOUBLE, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	}
	mdV2 := buildMD(t, "Order", fieldsV2)
	writer.RegisterDescriptor(mdV2, 1)

	// Insert row under V2
	o2 := dynamicpb.NewMessage(mdV2)
	o2.Set(mdV2.Fields().ByName("id"), protoreflect.ValueOfInt64(102))
	o2.Set(mdV2.Fields().ByName("customer"), protoreflect.ValueOfString("Bob"))
	o2.Set(mdV2.Fields().ByName("total"), protoreflect.ValueOfFloat64(150.00))
	o2.Set(mdV2.Fields().ByName("discount"), protoreflect.ValueOfFloat64(10.00))
	writer.Insert(ctx, o2)

	// Update row 101 under V2
	o1Up := dynamicpb.NewMessage(mdV2)
	o1Up.Set(mdV2.Fields().ByName("id"), protoreflect.ValueOfInt64(101))
	o1Up.Set(mdV2.Fields().ByName("customer"), protoreflect.ValueOfString("Alice"))
	o1Up.Set(mdV2.Fields().ByName("total"), protoreflect.ValueOfFloat64(75.00))
	o1Up.Set(mdV2.Fields().ByName("discount"), protoreflect.ValueOfFloat64(5.00))
	writer.Update(ctx, o1Up)

	// Feed all payloads to SQLite
	dbPath := filepath.Join(t.TempDir(), "evolved.sqlite")
	db, err := sqlite.Open(ctx, dbPath, sqlite.WithStreamID("stream-evolve"))
	if err != nil {
		t.Fatalf("sqlite.Open failed: %v", err)
	}
	defer db.Close()

	for i, p := range appender.Payloads() {
		seq := uint64(i + 1)
		if _, err := db.Feed(ctx, seq, p); err != nil {
			t.Fatalf("Feed at seq %d failed: %v", seq, err)
		}
	}

	// Verify row count in SQLite table
	count, err := db.Count(ctx, "testpkg.Order")
	if err != nil {
		t.Fatalf("Count failed: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 orders, got %d", count)
	}

	// Verify evolved row contents using Get
	k101, _ := sds.ExtractKey(o1Up, []int32{1})
	msg101, ok, err := db.Get(ctx, "testpkg.Order", k101)
	if err != nil || !ok {
		t.Fatalf("Get k101 failed: ok=%v, err=%v", ok, err)
	}
	dyn101 := msg101.(*dynamicpb.Message)
	if dyn101.Get(dyn101.Descriptor().Fields().ByName("total")).Float() != 75.00 {
		t.Errorf("order 101 total = %v, want 75.00", dyn101.Get(dyn101.Descriptor().Fields().ByName("total")).Float())
	}
	if dyn101.Get(dyn101.Descriptor().Fields().ByName("discount")).Float() != 5.00 {
		t.Errorf("order 101 discount = %v, want 5.00", dyn101.Get(dyn101.Descriptor().Fields().ByName("discount")).Float())
	}

	k102, _ := sds.ExtractKey(o2, []int32{1})
	msg102, ok, err := db.Get(ctx, "testpkg.Order", k102)
	if err != nil || !ok {
		t.Fatalf("Get k102 failed: ok=%v, err=%v", ok, err)
	}
	dyn102 := msg102.(*dynamicpb.Message)
	if dyn102.Get(dyn102.Descriptor().Fields().ByName("total")).Float() != 150.00 {
		t.Errorf("order 102 total = %v, want 150.00", dyn102.Get(dyn102.Descriptor().Fields().ByName("total")).Float())
	}
}

func TestIdempotentReapplySuffix(t *testing.T) {
	ctx := t.Context()
	appender := &memoryAppender{}
	writer := sds.NewWriter(appender)

	entityMD := buildMD(t, "Entity", []*descriptorpb.FieldDescriptorProto{
		field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("data", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	})
	writer.RegisterDescriptor(entityMD, 1)

	// Write 5 entities
	for i := 1; i <= 5; i++ {
		e := dynamicpb.NewMessage(entityMD)
		e.Set(entityMD.Fields().ByName("id"), protoreflect.ValueOfInt64(int64(i)))
		e.Set(entityMD.Fields().ByName("data"), protoreflect.ValueOfString(fmt.Sprintf("initial-%d", i)))
		writer.Insert(ctx, e)
	}

	// Update entity 3 and delete entity 4
	e3Up := dynamicpb.NewMessage(entityMD)
	e3Up.Set(entityMD.Fields().ByName("id"), protoreflect.ValueOfInt64(3))
	e3Up.Set(entityMD.Fields().ByName("data"), protoreflect.ValueOfString("updated-3"))
	writer.Update(ctx, e3Up)

	e4Del := dynamicpb.NewMessage(entityMD)
	e4Del.Set(entityMD.Fields().ByName("id"), protoreflect.ValueOfInt64(4))
	writer.Delete(ctx, e4Del)

	payloads := appender.Payloads()

	// Initial apply of all records
	dbPath := filepath.Join(t.TempDir(), "idempotent.sqlite")
	db, err := sqlite.Open(ctx, dbPath, sqlite.WithStreamID("stream-idem"))
	if err != nil {
		t.Fatalf("sqlite.Open failed: %v", err)
	}
	defer db.Close()

	for i, p := range payloads {
		seq := uint64(i + 1)
		if _, err := db.Feed(ctx, seq, p); err != nil {
			t.Fatalf("Feed failed: %v", err)
		}
	}

	// Reference memstore
	memStore := memtable.New()
	memReader := sds.NewChangeReader()
	for i, p := range payloads {
		changes, _ := memReader.Feed(uint64(i+1), p)
		memStore.ApplyBatch(changes)
	}

	verifySQLiteMatchesMemStore(t, ctx, db, memStore)

	// Re-apply the entire suffix (from record 3 onward) using a fresh reader
	suffixReader := sds.NewChangeReader()
	for i := 0; i < len(payloads); i++ {
		seq := uint64(i + 1)
		changes, err := suffixReader.Feed(seq, payloads[i])
		if err != nil {
			t.Fatalf("suffixReader.Feed failed: %v", err)
		}
		if i >= 3 {
			// Re-apply changes to SQLite
			if err := db.ApplyBatch(ctx, changes); err != nil {
				t.Fatalf("ApplyBatch re-apply failed: %v", err)
			}
		}
	}

	// State must be completely unchanged and match memStore
	verifySQLiteMatchesMemStore(t, ctx, db, memStore)
}

func TestRandomLogReplayConformance(t *testing.T) {
	ctx := t.Context()
	appender := &memoryAppender{}
	writer := sds.NewWriter(appender)

	accountMD := buildMD(t, "Account", []*descriptorpb.FieldDescriptorProto{
		field("account_id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("holder", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		field("balance", 3, descriptorpb.FieldDescriptorProto_TYPE_DOUBLE, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	})
	writer.RegisterDescriptor(accountMD, 1)

	r := rand.New(rand.NewSource(42))
	accounts := make(map[int64]bool)

	// Generate 100 random operations (autocommit & transactions)
	for opIdx := 0; opIdx < 100; opIdx++ {
		isTx := r.Float64() < 0.3
		var activeWriter interface {
			Insert(ctx context.Context, msg proto.Message) (uint64, error)
			Update(ctx context.Context, msg proto.Message) (uint64, error)
			Delete(ctx context.Context, msg proto.Message) (uint64, error)
		}
		var tx *sds.Tx
		if isTx {
			tx = writer.Begin()
			activeWriter = tx
		} else {
			activeWriter = writer
		}

		numOpsInGroup := 1
		if isTx {
			numOpsInGroup = r.Intn(4) + 1
		}

		for g := 0; g < numOpsInGroup; g++ {
			id := int64(r.Intn(20) + 1)
			exists := accounts[id]

			if !exists {
				// Insert
				acc := dynamicpb.NewMessage(accountMD)
				acc.Set(accountMD.Fields().ByName("account_id"), protoreflect.ValueOfInt64(id))
				acc.Set(accountMD.Fields().ByName("holder"), protoreflect.ValueOfString(fmt.Sprintf("User-%d", id)))
				acc.Set(accountMD.Fields().ByName("balance"), protoreflect.ValueOfFloat64(float64(r.Intn(1000))))
				activeWriter.Insert(ctx, acc)
				accounts[id] = true
			} else {
				if r.Float64() < 0.7 {
					// Update
					acc := dynamicpb.NewMessage(accountMD)
					acc.Set(accountMD.Fields().ByName("account_id"), protoreflect.ValueOfInt64(id))
					acc.Set(accountMD.Fields().ByName("holder"), protoreflect.ValueOfString(fmt.Sprintf("User-%d-up", id)))
					acc.Set(accountMD.Fields().ByName("balance"), protoreflect.ValueOfFloat64(float64(r.Intn(1000))))
					activeWriter.Update(ctx, acc)
				} else {
					// Delete
					acc := dynamicpb.NewMessage(accountMD)
					acc.Set(accountMD.Fields().ByName("account_id"), protoreflect.ValueOfInt64(id))
					activeWriter.Delete(ctx, acc)
					delete(accounts, id)
				}
			}
		}

		if isTx {
			tx.Commit(ctx)
		}
	}

	payloads := appender.Payloads()

	// 1. Full replay comparison
	memStore := memtable.New()
	memReader := sds.NewChangeReader()

	dbPath := filepath.Join(t.TempDir(), "random.sqlite")
	db, err := sqlite.Open(ctx, dbPath, sqlite.WithStreamID("stream-random"))
	if err != nil {
		t.Fatalf("sqlite.Open failed: %v", err)
	}
	defer db.Close()

	for i, p := range payloads {
		seq := uint64(i + 1)
		changes, _ := memReader.Feed(seq, p)
		memStore.ApplyBatch(changes)
		db.Feed(ctx, seq, p)
	}

	verifySQLiteMatchesMemStore(t, ctx, db, memStore)

	// 2. Snapshot at random safe position + replay test
	safePos := memReader.SafeSnapshotPosition()
	if safePos > 10 {
		snapPosTarget := uint64(r.Intn(int(safePos-5)) + 5)
		backend := inmemorystorage.New()
		streamID := "stream-random-snap"

		_, actualSnapPos, err := sqlite.BuildSnapshot(ctx, streamID, payloads[:snapPosTarget], backend, t.TempDir())
		if err != nil {
			t.Fatalf("BuildSnapshot failed: %v", err)
		}

		// Restore and tail replay
		restoredPath := filepath.Join(t.TempDir(), "random_restored.sqlite")
		restoredDB, _, err := sqlite.RestoreSnapshot(ctx, backend, streamID, actualSnapPos, restoredPath)
		if err != nil {
			t.Fatalf("RestoreSnapshot failed: %v", err)
		}
		defer restoredDB.Close()

		for i := int(actualSnapPos); i < len(payloads); i++ {
			seq := uint64(i + 1)
			if _, err := restoredDB.Feed(ctx, seq, payloads[i]); err != nil {
				t.Fatalf("Feed to restored DB failed at seq %d: %v", seq, err)
			}
		}

		verifySQLiteMatchesMemStore(t, ctx, restoredDB, memStore)
	}
}
