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
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/gke-labs/in-cluster-storage/pkg/objectstore"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore/inmemorystorage"
	"github.com/gke-labs/in-cluster-storage/pkg/sds"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/memtable"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/projection/sqlite"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/projection/table"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

type localIndexFactory struct {
	name    string
	create  func(t *testing.T, ctx context.Context, streamID string) (sds.LocalIndex, func())
	publish func(ctx context.Context, idx sds.LocalIndex, backend objectstore.Backend) (string, uint64, error)
	restore func(ctx context.Context, backend objectstore.Backend, streamID string, maxPos uint64, targetDir string) (sds.LocalIndex, uint64, error)
}

func localIndexFactories(t *testing.T) []localIndexFactory {
	return []localIndexFactory{
		{
			name: "SQLite",
			create: func(t *testing.T, ctx context.Context, streamID string) (sds.LocalIndex, func()) {
				dbDir := t.TempDir()
				dbPath := filepath.Join(dbDir, "index.sqlite")
				db, err := sqlite.Open(ctx, dbPath,
					sqlite.WithStreamID(streamID),
					sqlite.WithJournalMode("WAL"),
					sqlite.WithSynchronous("NORMAL"),
				)
				if err != nil {
					t.Fatalf("sqlite.Open failed: %v", err)
				}
				return db, func() { _ = db.Close() }
			},
			publish: func(ctx context.Context, idx sds.LocalIndex, backend objectstore.Backend) (string, uint64, error) {
				return sqlite.PublishSnapshot(ctx, idx.(*sqlite.DB), backend, os.TempDir())
			},
			restore: func(ctx context.Context, backend objectstore.Backend, streamID string, maxPos uint64, targetDir string) (sds.LocalIndex, uint64, error) {
				dbPath := filepath.Join(targetDir, "restored.sqlite")
				return sqlite.RestoreSnapshot(ctx, backend, streamID, maxPos, dbPath)
			},
		},
		{
			name: "MemTable",
			create: func(t *testing.T, ctx context.Context, streamID string) (sds.LocalIndex, func()) {
				store := memtable.New(memtable.WithStreamID(streamID))
				return store, func() { _ = store.Close() }
			},
			publish: func(ctx context.Context, idx sds.LocalIndex, backend objectstore.Backend) (string, uint64, error) {
				return memtable.PublishSnapshot(ctx, idx.(*memtable.MemStore), backend, os.TempDir())
			},
			restore: func(ctx context.Context, backend objectstore.Backend, streamID string, maxPos uint64, targetDir string) (sds.LocalIndex, uint64, error) {
				return memtable.RestoreSnapshot(ctx, backend, streamID, maxPos, targetDir)
			},
		},
		{
			name: "Table",
			create: func(t *testing.T, ctx context.Context, streamID string) (sds.LocalIndex, func()) {
				dbDir := t.TempDir()
				db, err := table.Open(ctx, dbDir,
					table.WithStreamID(streamID),
				)
				if err != nil {
					t.Fatalf("table.Open failed: %v", err)
				}
				return db, func() { _ = db.Close() }
			},
			publish: func(ctx context.Context, idx sds.LocalIndex, backend objectstore.Backend) (string, uint64, error) {
				return table.PublishSnapshot(ctx, idx.(*table.DB), backend, os.TempDir())
			},
			restore: func(ctx context.Context, backend objectstore.Backend, streamID string, maxPos uint64, targetDir string) (sds.LocalIndex, uint64, error) {
				return table.RestoreSnapshot(ctx, backend, streamID, maxPos, targetDir)
			},
		},
	}
}

func protoField(name string, number int32, typeKind descriptorpb.FieldDescriptorProto_Type, label descriptorpb.FieldDescriptorProto_Label) *descriptorpb.FieldDescriptorProto {
	return &descriptorpb.FieldDescriptorProto{
		Name:   proto.String(name),
		Number: proto.Int32(number),
		Type:   typeKind.Enum(),
		Label:  label.Enum(),
	}
}

func dynamicDescriptor(t *testing.T, name string, fields []*descriptorpb.FieldDescriptorProto) protoreflect.MessageDescriptor {
	t.Helper()
	msgProto := &descriptorpb.DescriptorProto{
		Name:  proto.String(name),
		Field: fields,
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

type memoryTransport struct {
	payloads [][]byte
	seq      uint64
}

func (m *memoryTransport) Append(_ context.Context, payload []byte) (uint64, error) {
	m.seq++
	cp := make([]byte, len(payload))
	copy(cp, payload)
	m.payloads = append(m.payloads, cp)
	return m.seq, nil
}

// TestLocalIndexRandomLogsAndReplayConformance tests random change streams applied in batches vs full replay.
func TestLocalIndexRandomLogsAndReplayConformance(t *testing.T) {
	for _, factory := range localIndexFactories(t) {
		t.Run(factory.name, func(t *testing.T) {
			ctx := t.Context()
			streamID := uuid.New().String()

			transport := &memoryTransport{}
			writer := sds.NewWriter(transport)

			accountMD := dynamicDescriptor(t, "Account", []*descriptorpb.FieldDescriptorProto{
				protoField("account_id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
				protoField("owner", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
				protoField("balance", 3, descriptorpb.FieldDescriptorProto_TYPE_DOUBLE, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
			})
			_, err := writer.RegisterDescriptor(accountMD, 1)
			if err != nil {
				t.Fatalf("RegisterDescriptor failed: %v", err)
			}

			rng := rand.New(rand.NewSource(42))
			numAccounts := 30
			numOps := 150

			for i := 0; i < numOps; i++ {
				accID := int64(rng.Intn(numAccounts) + 1)
				bal := float64(rng.Intn(10000)) / 100.0

				acc := dynamicpb.NewMessage(accountMD)
				acc.Set(accountMD.Fields().ByName("account_id"), protoreflect.ValueOfInt64(accID))
				acc.Set(accountMD.Fields().ByName("owner"), protoreflect.ValueOfString(fmt.Sprintf("User-%d", accID)))
				acc.Set(accountMD.Fields().ByName("balance"), protoreflect.ValueOfFloat64(bal))

				opType := rng.Intn(3)
				switch opType {
				case 0:
					_, _ = writer.Insert(ctx, acc)
				case 1:
					_, _ = writer.Update(ctx, acc)
				case 2:
					_, _ = writer.Delete(ctx, acc)
				}
			}

			// Play into index 1 incrementally in batches
			idx1, cleanup1 := factory.create(t, ctx, streamID)
			defer cleanup1()
			if err := idx1.SyncRegistry(ctx, writer.Registry()); err != nil {
				t.Fatalf("SyncRegistry failed: %v", err)
			}

			reader1 := sds.NewChangeReader()
			for i, p := range transport.payloads {
				changes, err := reader1.Feed(uint64(i+1), p)
				if err != nil {
					t.Fatalf("reader1.Feed failed: %v", err)
				}
				if len(changes) > 0 {
					if err := idx1.ApplyBatch(ctx, changes); err != nil {
						t.Fatalf("idx1.ApplyBatch failed: %v", err)
					}
				}
			}

			// Play into index 2 all at once (full replay)
			idx2, cleanup2 := factory.create(t, ctx, streamID)
			defer cleanup2()
			if err := idx2.SyncRegistry(ctx, writer.Registry()); err != nil {
				t.Fatalf("SyncRegistry failed: %v", err)
			}

			reader2 := sds.NewChangeReader()
			var allChanges []sds.Change
			for i, p := range transport.payloads {
				changes, err := reader2.Feed(uint64(i+1), p)
				if err != nil {
					t.Fatalf("reader2.Feed failed: %v", err)
				}
				allChanges = append(allChanges, changes...)
			}
			if err := idx2.ApplyBatch(ctx, allChanges); err != nil {
				t.Fatalf("idx2.ApplyBatch failed: %v", err)
			}

			if idx1.Position() != idx2.Position() {
				t.Fatalf("Position mismatch: idx1 %d != idx2 %d", idx1.Position(), idx2.Position())
			}

			// Compare scan results between idx1 and idx2
			var rows1 []proto.Message
			for msg, err := range idx1.Scan(ctx, "testpkg.Account", nil) {
				if err != nil {
					t.Fatalf("idx1.Scan failed: %v", err)
				}
				rows1 = append(rows1, msg)
			}

			var rows2 []proto.Message
			for msg, err := range idx2.Scan(ctx, "testpkg.Account", nil) {
				if err != nil {
					t.Fatalf("idx2.Scan failed: %v", err)
				}
				rows2 = append(rows2, msg)
			}

			if len(rows1) != len(rows2) {
				t.Fatalf("Row count mismatch: idx1 has %d rows, idx2 has %d rows", len(rows1), len(rows2))
			}

			for i := range rows1 {
				k1, _ := sds.ExtractKey(rows1[i], []int32{1})
				k2, _ := sds.ExtractKey(rows2[i], []int32{1})
				if k1.String() != k2.String() {
					t.Fatalf("Row %d key mismatch: %v vs %v", i, k1, k2)
				}
			}
		})
	}
}

// TestLocalIndexIdempotentSuffixReapply tests that re-applying an already applied suffix is idempotent.
func TestLocalIndexIdempotentSuffixReapply(t *testing.T) {
	for _, factory := range localIndexFactories(t) {
		t.Run(factory.name, func(t *testing.T) {
			ctx := t.Context()
			streamID := uuid.New().String()

			transport := &memoryTransport{}
			writer := sds.NewWriter(transport)

			itemMD := dynamicDescriptor(t, "Item", []*descriptorpb.FieldDescriptorProto{
				protoField("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
				protoField("title", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
			})
			_, err := writer.RegisterDescriptor(itemMD, 1)
			if err != nil {
				t.Fatalf("RegisterDescriptor failed: %v", err)
			}

			for i := 1; i <= 10; i++ {
				it := dynamicpb.NewMessage(itemMD)
				it.Set(itemMD.Fields().ByName("id"), protoreflect.ValueOfInt64(int64(i)))
				it.Set(itemMD.Fields().ByName("title"), protoreflect.ValueOfString(fmt.Sprintf("Item %d", i)))
				writer.Insert(ctx, it)
			}

			idx, cleanup := factory.create(t, ctx, streamID)
			defer cleanup()
			_ = idx.SyncRegistry(ctx, writer.Registry())

			reader := sds.NewChangeReader()
			var changesList [][]sds.Change
			for i, p := range transport.payloads {
				changes, _ := reader.Feed(uint64(i+1), p)
				changesList = append(changesList, changes)
				if len(changes) > 0 {
					_ = idx.ApplyBatch(ctx, changes)
				}
			}

			posBefore := idx.Position()

			// Re-apply the last 5 changes
			for i := 5; i < len(changesList); i++ {
				if len(changesList[i]) > 0 {
					if err := idx.ApplyBatch(ctx, changesList[i]); err != nil {
						t.Fatalf("re-applying changes failed: %v", err)
					}
				}
			}

			if idx.Position() != posBefore {
				t.Fatalf("Position changed after suffix reapply: %d vs %d", idx.Position(), posBefore)
			}

			// Verify row count is unchanged (10 items)
			count := 0
			for _, err := range idx.Scan(ctx, "testpkg.Item", nil) {
				if err != nil {
					t.Fatalf("Scan failed: %v", err)
				}
				count++
			}
			if count != 10 {
				t.Fatalf("expected 10 items, got %d", count)
			}
		})
	}
}

// TestLocalIndexScanOrderAndPrefix tests canonical key-byte ordering and prefix scanning.
func TestLocalIndexScanOrderAndPrefix(t *testing.T) {
	for _, factory := range localIndexFactories(t) {
		t.Run(factory.name, func(t *testing.T) {
			ctx := t.Context()
			streamID := uuid.New().String()

			transport := &memoryTransport{}
			writer := sds.NewWriter(transport)

			dirEntryMD := dynamicDescriptor(t, "DirEntry", []*descriptorpb.FieldDescriptorProto{
				protoField("parent_id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
				protoField("name", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
				protoField("ino", 3, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
			})
			_, err := writer.RegisterDescriptor(dirEntryMD, 1, 2)
			if err != nil {
				t.Fatalf("RegisterDescriptor failed: %v", err)
			}

			p1Names := []string{"alpha.txt", "beta.txt", "gamma.txt"}
			for _, name := range p1Names {
				de := dynamicpb.NewMessage(dirEntryMD)
				de.Set(dirEntryMD.Fields().ByName("parent_id"), protoreflect.ValueOfInt64(1))
				de.Set(dirEntryMD.Fields().ByName("name"), protoreflect.ValueOfString(name))
				de.Set(dirEntryMD.Fields().ByName("ino"), protoreflect.ValueOfInt64(10))
				writer.Insert(ctx, de)
			}

			p2Names := []string{"foo.txt", "bar.txt"}
			for _, name := range p2Names {
				de := dynamicpb.NewMessage(dirEntryMD)
				de.Set(dirEntryMD.Fields().ByName("parent_id"), protoreflect.ValueOfInt64(2))
				de.Set(dirEntryMD.Fields().ByName("name"), protoreflect.ValueOfString(name))
				de.Set(dirEntryMD.Fields().ByName("ino"), protoreflect.ValueOfInt64(20))
				writer.Insert(ctx, de)
			}

			idx, cleanup := factory.create(t, ctx, streamID)
			defer cleanup()
			_ = idx.SyncRegistry(ctx, writer.Registry())

			reader := sds.NewChangeReader()
			for i, p := range transport.payloads {
				changes, _ := reader.Feed(uint64(i+1), p)
				if len(changes) > 0 {
					_ = idx.ApplyBatch(ctx, changes)
				}
			}

			// Prefix query for parent_id = 1
			p1PrefixMsg := dynamicpb.NewMessage(dirEntryMD)
			p1PrefixMsg.Set(dirEntryMD.Fields().ByName("parent_id"), protoreflect.ValueOfInt64(1))
			p1Prefix, err := sds.EncodeKeyPrefix(p1PrefixMsg, 1)
			if err != nil {
				t.Fatalf("EncodeKeyPrefix failed: %v", err)
			}

			var p1GotNames []string
			var lastKeyBytes []byte
			for msg, err := range idx.Scan(ctx, "testpkg.DirEntry", p1Prefix) {
				if err != nil {
					t.Fatalf("Scan failed: %v", err)
				}
				dyn := msg.(*dynamicpb.Message)
				name := dyn.Get(dyn.Descriptor().Fields().ByName("name")).String()
				p1GotNames = append(p1GotNames, name)

				k, _ := sds.ExtractKey(msg, []int32{1, 2})
				if len(lastKeyBytes) > 0 && bytes.Compare(lastKeyBytes, k.Bytes()) >= 0 {
					t.Fatalf("Scan results not in strict canonical key-byte order! %v >= %v", lastKeyBytes, k.Bytes())
				}
				lastKeyBytes = k.Bytes()
			}

			if len(p1GotNames) != 3 {
				t.Fatalf("expected 3 entries for parent 1, got %d", len(p1GotNames))
			}
		})
	}
}

// TestLocalIndexSnapshotPublishRestoreAndReplay tests snapshot publish, restore and replaying remainder.
func TestLocalIndexSnapshotPublishRestoreAndReplay(t *testing.T) {
	for _, factory := range localIndexFactories(t) {
		t.Run(factory.name, func(t *testing.T) {
			ctx := t.Context()
			streamID := uuid.New().String()
			backend := inmemorystorage.New()

			transport := &memoryTransport{}
			writer := sds.NewWriter(transport)

			userMD := dynamicDescriptor(t, "User", []*descriptorpb.FieldDescriptorProto{
				protoField("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
				protoField("name", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
			})
			_, err := writer.RegisterDescriptor(userMD, 1)
			if err != nil {
				t.Fatalf("RegisterDescriptor failed: %v", err)
			}

			// Step 1: Write users 1..5
			for i := 1; i <= 5; i++ {
				u := dynamicpb.NewMessage(userMD)
				u.Set(userMD.Fields().ByName("id"), protoreflect.ValueOfInt64(int64(i)))
				u.Set(userMD.Fields().ByName("name"), protoreflect.ValueOfString(fmt.Sprintf("User-%d", i)))
				writer.Insert(ctx, u)
			}

			liveIdx, cleanupLive := factory.create(t, ctx, streamID)
			defer cleanupLive()
			_ = liveIdx.SyncRegistry(ctx, writer.Registry())

			reader := sds.NewChangeReader()
			for i, p := range transport.payloads {
				changes, _ := reader.Feed(uint64(i+1), p)
				if len(changes) > 0 {
					_ = liveIdx.ApplyBatch(ctx, changes)
				}
			}

			snapKey, snapPos, err := factory.publish(ctx, liveIdx, backend)
			if err != nil {
				t.Fatalf("publish failed: %v", err)
			}
			if snapKey == "" || snapPos == 0 {
				t.Fatalf("invalid published snapshot key %q pos %d", snapKey, snapPos)
			}

			// Step 2: Write users 6..10
			for i := 6; i <= 10; i++ {
				u := dynamicpb.NewMessage(userMD)
				u.Set(userMD.Fields().ByName("id"), protoreflect.ValueOfInt64(int64(i)))
				u.Set(userMD.Fields().ByName("name"), protoreflect.ValueOfString(fmt.Sprintf("User-%d", i)))
				writer.Insert(ctx, u)
			}

			// Apply to live index
			for i := int(snapPos); i < len(transport.payloads); i++ {
				changes, _ := reader.Feed(uint64(i+1), transport.payloads[i])
				if len(changes) > 0 {
					_ = liveIdx.ApplyBatch(ctx, changes)
				}
			}

			// Step 3: Restore snapshot into a new index and replay forward from snapPos
			restoredDir := t.TempDir()
			restoredIdx, restoredPos, err := factory.restore(ctx, backend, streamID, snapPos, restoredDir)
			if err != nil {
				t.Fatalf("restore failed: %v", err)
			}
			defer restoredIdx.Close()
			if restoredPos != snapPos {
				t.Fatalf("restoredPos %d != snapPos %d", restoredPos, snapPos)
			}

			replayReader := sds.NewChangeReader()
			_ = replayReader.Registry().Import(writer.Registry().Export())
			for i := int(snapPos); i < len(transport.payloads); i++ {
				changes, _ := replayReader.Feed(uint64(i+1), transport.payloads[i])
				if len(changes) > 0 {
					if err := restoredIdx.ApplyBatch(ctx, changes); err != nil {
						t.Fatalf("restoredIdx.ApplyBatch failed: %v", err)
					}
				}
			}

			// Compare live state vs restored + replayed state
			var liveRows []proto.Message
			for msg, err := range liveIdx.Scan(ctx, "testpkg.User", nil) {
				if err != nil {
					t.Fatalf("liveIdx.Scan failed: %v", err)
				}
				liveRows = append(liveRows, msg)
			}

			var restoredRows []proto.Message
			for msg, err := range restoredIdx.Scan(ctx, "testpkg.User", nil) {
				if err != nil {
					t.Fatalf("restoredIdx.Scan failed: %v", err)
				}
				restoredRows = append(restoredRows, msg)
			}

			if len(liveRows) != 10 || len(restoredRows) != 10 {
				t.Fatalf("expected 10 rows each, got live=%d restored=%d", len(liveRows), len(restoredRows))
			}
		})
	}
}
