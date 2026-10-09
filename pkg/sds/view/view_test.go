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

package view_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/gke-labs/in-cluster-storage/pkg/sds"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/memtable"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/projection/sqlite"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/projection/table"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/record"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/view"
)

type indexFactory struct {
	name   string
	create func(t *testing.T, ctx context.Context, streamID string) (sds.LocalIndex, func())
}

func testFactories(t *testing.T) []indexFactory {
	return []indexFactory{
		{
			name: "SQLite",
			create: func(t *testing.T, ctx context.Context, streamID string) (sds.LocalIndex, func()) {
				dbPath := filepath.Join(t.TempDir(), "view_test.sqlite")
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
		},
		{
			name: "MemTable",
			create: func(t *testing.T, ctx context.Context, streamID string) (sds.LocalIndex, func()) {
				store := memtable.New(memtable.WithStreamID(streamID))
				return store, func() { _ = store.Close() }
			},
		},
		{
			name: "Table",
			create: func(t *testing.T, ctx context.Context, streamID string) (sds.LocalIndex, func()) {
				dbDir := t.TempDir()
				db, err := table.Open(ctx, dbDir, table.WithStreamID(streamID))
				if err != nil {
					t.Fatalf("table.Open failed: %v", err)
				}
				return db, func() { _ = db.Close() }
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
	if t != nil {
		t.Helper()
	}
	msgProto := &descriptorpb.DescriptorProto{
		Name:  proto.String(name),
		Field: fields,
	}
	fileProto := &descriptorpb.FileDescriptorProto{
		Name:        proto.String(name + ".proto"),
		Package:     proto.String("viewtest"),
		MessageType: []*descriptorpb.DescriptorProto{msgProto},
		Syntax:      proto.String("proto2"),
	}
	fds := &descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{fileProto},
	}
	files, err := protodesc.NewFiles(fds)
	if err != nil {
		if t != nil {
			t.Fatalf("protodesc.NewFiles failed: %v", err)
		}
		panic(err)
	}
	d, err := files.FindDescriptorByName(protoreflect.FullName("viewtest." + name))
	if err != nil {
		if t != nil {
			t.Fatalf("FindDescriptorByName failed: %v", err)
		}
		panic(err)
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

// TestViewOverlayReadsDuringLag verifies that reads via Get and Scan see unapplied mutations immediately even when the applier is lagging or blocked.
func TestViewOverlayReadsDuringLag(t *testing.T) {
	for _, factory := range testFactories(t) {
		t.Run(factory.name, func(t *testing.T) {
			ctx := t.Context()
			streamID := uuid.New().String()
			idx, cleanup := factory.create(t, ctx, streamID)
			defer cleanup()

			var blockApplier atomic.Bool
			blockApplier.Store(true)

			faultHook := func() error {
				if blockApplier.Load() {
					return errors.New("simulated applier stall")
				}
				return nil
			}

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

			v := view.New(idx,
				view.WithBatchSize(10),
				view.WithFaultHook(faultHook),
				view.WithRegistry(writer.Registry()),
			)
			defer v.Close()
			_ = v.SyncRegistry(ctx, writer.Registry())

			// 1. Create User 1 and User 2
			reader := sds.NewChangeReader()
			_ = reader.Registry().Import(writer.Registry().Export())
			u1 := dynamicpb.NewMessage(userMD)
			u1.Set(userMD.Fields().ByName("id"), protoreflect.ValueOfInt64(1))
			u1.Set(userMD.Fields().ByName("name"), protoreflect.ValueOfString("Alice"))
			seq1, _ := writer.Insert(ctx, u1)
			changes1, _ := reader.Feed(seq1, transport.payloads[len(transport.payloads)-1])
			v.ApplyChanges(changes1)

			u2 := dynamicpb.NewMessage(userMD)
			u2.Set(userMD.Fields().ByName("id"), protoreflect.ValueOfInt64(2))
			u2.Set(userMD.Fields().ByName("name"), protoreflect.ValueOfString("Bob"))
			seq2, _ := writer.Insert(ctx, u2)
			changes2, _ := reader.Feed(seq2, transport.payloads[len(transport.payloads)-1])
			v.ApplyChanges(changes2)

			// Verify applier is lagging (applied position is 0)
			if v.AppliedPosition() != 0 {
				t.Fatalf("expected applied position 0 during lag, got %d", v.AppliedPosition())
			}

			// Point lookup via Get should immediately return Alice from overlay
			k1, _ := sds.ExtractKey(u1, []int32{1})
			msg1, ok, err := v.Get(ctx, "viewtest.User", k1)
			if err != nil || !ok {
				t.Fatalf("v.Get(User 1) failed: ok=%v err=%v", ok, err)
			}
			dyn1 := msg1.(*dynamicpb.Message)
			if dyn1.Get(dyn1.Descriptor().Fields().ByName("name")).String() != "Alice" {
				t.Fatalf("name mismatch: %s", dyn1.Get(dyn1.Descriptor().Fields().ByName("name")).String())
			}

			// Scan should return both User 1 and User 2 in canonical order from overlay
			var scanUsers []string
			for msg, err := range v.Scan(ctx, "viewtest.User", nil) {
				if err != nil {
					t.Fatalf("v.Scan failed: %v", err)
				}
				dyn := msg.(*dynamicpb.Message)
				scanUsers = append(scanUsers, dyn.Get(dyn.Descriptor().Fields().ByName("name")).String())
			}
			if len(scanUsers) != 2 || scanUsers[0] != "Alice" || scanUsers[1] != "Bob" {
				t.Fatalf("unexpected scan results during lag: %v", scanUsers)
			}

			// 2. Unblock applier and flush
			blockApplier.Store(false)
			if err := v.FlushTo(ctx, seq2); err != nil {
				t.Fatalf("FlushTo failed: %v", err)
			}

			if v.AppliedPosition() < seq2 {
				t.Fatalf("applied position %d < seq2 %d after flush", v.AppliedPosition(), seq2)
			}

			// Reads should still return Alice and Bob cleanly from cache/index
			msg1After, ok, err := v.Get(ctx, "viewtest.User", k1)
			if err != nil || !ok {
				t.Fatalf("v.Get after flush failed: %v", err)
			}
			dyn1After := msg1After.(*dynamicpb.Message)
			if dyn1After.Get(dyn1After.Descriptor().Fields().ByName("name")).String() != "Alice" {
				t.Fatalf("name mismatch after flush: %s", dyn1After.Get(dyn1After.Descriptor().Fields().ByName("name")).String())
			}
		})
	}
}

// TestViewCoalescing verifies that multiple updates to the same row in a batch coalesce to the latest state.
func TestViewCoalescing(t *testing.T) {
	for _, factory := range testFactories(t) {
		t.Run(factory.name, func(t *testing.T) {
			ctx := t.Context()
			streamID := uuid.New().String()
			idx, cleanup := factory.create(t, ctx, streamID)
			defer cleanup()

			transport := &memoryTransport{}
			writer := sds.NewWriter(transport)

			counterMD := dynamicDescriptor(t, "Counter", []*descriptorpb.FieldDescriptorProto{
				protoField("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
				protoField("val", 2, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
			})
			_, err := writer.RegisterDescriptor(counterMD, 1)
			if err != nil {
				t.Fatalf("RegisterDescriptor failed: %v", err)
			}

			v := view.New(idx,
				view.WithBatchSize(50),
				view.WithRegistry(writer.Registry()),
			)
			defer v.Close()
			_ = v.SyncRegistry(ctx, writer.Registry())

			reader := sds.NewChangeReader()
			_ = reader.Registry().Import(writer.Registry().Export())

			// Create counter id 1 val 0
			c := dynamicpb.NewMessage(counterMD)
			c.Set(counterMD.Fields().ByName("id"), protoreflect.ValueOfInt64(1))
			c.Set(counterMD.Fields().ByName("val"), protoreflect.ValueOfInt64(0))
			seq, _ := writer.Insert(ctx, c)
			ch, _ := reader.Feed(seq, transport.payloads[len(transport.payloads)-1])
			v.ApplyChanges(ch)

			// Rapidly update counter 1 from 1 to 20
			for i := int64(1); i <= 20; i++ {
				c.Set(counterMD.Fields().ByName("val"), protoreflect.ValueOfInt64(i))
				seq, _ = writer.Update(ctx, c)
				ch, _ = reader.Feed(seq, transport.payloads[len(transport.payloads)-1])
				v.ApplyChanges(ch)
			}

			if err := v.FlushTo(ctx, seq); err != nil {
				t.Fatalf("FlushTo failed: %v", err)
			}

			k1, _ := sds.ExtractKey(c, []int32{1})
			msg, ok, err := v.Get(ctx, "viewtest.Counter", k1)
			if err != nil || !ok {
				t.Fatalf("Get counter failed: %v", err)
			}
			dyn := msg.(*dynamicpb.Message)
			if dyn.Get(dyn.Descriptor().Fields().ByName("val")).Int() != 20 {
				t.Fatalf("expected val 20, got %d", dyn.Get(dyn.Descriptor().Fields().ByName("val")).Int())
			}
		})
	}
}

// TestViewFaultInjectionAndDegradedState verifies applier degraded state and recovery.
func TestViewFaultInjectionAndDegradedState(t *testing.T) {
	for _, factory := range testFactories(t) {
		t.Run(factory.name, func(t *testing.T) {
			ctx := t.Context()
			streamID := uuid.New().String()
			idx, cleanup := factory.create(t, ctx, streamID)
			defer cleanup()

			var failCount atomic.Int32
			failCount.Store(5) // Fail 5 times then recover

			faultHook := func() error {
				if failCount.Add(-1) >= 0 {
					return errors.New("simulated transient fault")
				}
				return nil
			}

			transport := &memoryTransport{}
			writer := sds.NewWriter(transport)

			itemMD := dynamicDescriptor(t, "Item", []*descriptorpb.FieldDescriptorProto{
				protoField("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
				protoField("name", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
			})
			_, _ = writer.RegisterDescriptor(itemMD, 1)

			v := view.New(idx,
				view.WithBatchSize(1),
				view.WithFaultHook(faultHook),
				view.WithRegistry(writer.Registry()),
			)
			defer v.Close()
			_ = v.SyncRegistry(ctx, writer.Registry())

			reader := sds.NewChangeReader()
			_ = reader.Registry().Import(writer.Registry().Export())
			it := dynamicpb.NewMessage(itemMD)
			it.Set(itemMD.Fields().ByName("id"), protoreflect.ValueOfInt64(1))
			it.Set(itemMD.Fields().ByName("name"), protoreflect.ValueOfString("FaultTest"))
			seq, _ := writer.Insert(ctx, it)
			ch, _ := reader.Feed(seq, transport.payloads[len(transport.payloads)-1])
			v.ApplyChanges(ch)

			// Wait for retry loop to recover and flush
			flushCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			if err := v.FlushTo(flushCtx, seq); err != nil {
				t.Fatalf("FlushTo failed after retries: %v", err)
			}

			if v.ApplierFailures() < 5 {
				t.Fatalf("expected at least 5 failures, got %d", v.ApplierFailures())
			}
			if v.IsDegraded() {
				t.Fatalf("expected applier to be non-degraded after recovery")
			}
		})
	}
}

// TestViewBackpressure verifies that WaitBackpressure halts writers when unapplied bytes exceed limit.
func TestViewBackpressure(t *testing.T) {
	for _, factory := range testFactories(t) {
		t.Run(factory.name, func(t *testing.T) {
			ctx := t.Context()
			streamID := uuid.New().String()
			idx, cleanup := factory.create(t, ctx, streamID)
			defer cleanup()

			var holdApplier atomic.Bool
			holdApplier.Store(true)

			faultHook := func() error {
				if holdApplier.Load() {
					return errors.New("applier paused for backpressure test")
				}
				return nil
			}

			transport := &memoryTransport{}
			writer := sds.NewWriter(transport)

			dataMD := dynamicDescriptor(t, "Data", []*descriptorpb.FieldDescriptorProto{
				protoField("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
				protoField("payload", 2, descriptorpb.FieldDescriptorProto_TYPE_BYTES, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
			})
			_, _ = writer.RegisterDescriptor(dataMD, 1)

			v := view.New(idx,
				view.WithOverlayMaxBytes(500),
				view.WithFaultHook(faultHook),
				view.WithRegistry(writer.Registry()),
			)
			defer v.Close()
			_ = v.SyncRegistry(ctx, writer.Registry())

			reader := sds.NewChangeReader()
			_ = reader.Registry().Import(writer.Registry().Export())

			// Add entries to exceed 500 bytes limit
			largeBytes := make([]byte, 300)
			for i := int64(1); i <= 3; i++ {
				d := dynamicpb.NewMessage(dataMD)
				d.Set(dataMD.Fields().ByName("id"), protoreflect.ValueOfInt64(i))
				d.Set(dataMD.Fields().ByName("payload"), protoreflect.ValueOfBytes(largeBytes))
				seq, _ := writer.Insert(ctx, d)
				ch, _ := reader.Feed(seq, transport.payloads[len(transport.payloads)-1])
				v.ApplyChanges(ch)
			}

			if v.UnappliedBytes() <= 500 {
				t.Fatalf("expected unapplied bytes > 500, got %d", v.UnappliedBytes())
			}

			var unblocked atomic.Bool
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = v.WaitBackpressure(ctx)
				unblocked.Store(true)
			}()

			time.Sleep(50 * time.Millisecond)
			if unblocked.Load() {
				t.Fatalf("WaitBackpressure should have blocked while unapplied bytes exceeded limit")
			}

			// Release applier
			holdApplier.Store(false)
			wg.Wait()

			if !unblocked.Load() {
				t.Fatalf("WaitBackpressure failed to unblock after applier processed queue")
			}
		})
	}
}

// TestViewTruncateDeletesAndScanOrder verifies that tombstones in overlay and applied deletes
// suppress deleted chunks and correctly return live chunks during prefix scan and point lookups.
func TestViewTruncateDeletesAndScanOrder(t *testing.T) {
	for _, factory := range testFactories(t) {
		t.Run(factory.name, func(t *testing.T) {
			ctx := t.Context()
			streamID := uuid.New().String()
			idx, cleanup := factory.create(t, ctx, streamID)
			defer cleanup()

			transport := &memoryTransport{}
			writer := sds.NewWriter(transport)

			chunkMD := dynamicDescriptor(t, "FileChunk", []*descriptorpb.FieldDescriptorProto{
				protoField("ino", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
				protoField("index", 2, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
				protoField("sha256", 3, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
			})
			_, _ = writer.RegisterDescriptor(chunkMD, 1, 2)

			v := view.New(idx,
				view.WithBatchSize(100),
				view.WithRegistry(writer.Registry()),
			)
			defer v.Close()
			_ = v.SyncRegistry(ctx, writer.Registry())

			reader := sds.NewChangeReader()
			_ = reader.Registry().Import(writer.Registry().Export())

			readOffset := 0
			consumeNewPayloads := func() {
				for readOffset < len(transport.payloads) {
					seq := uint64(readOffset + 1)
					ch, err := reader.Feed(seq, transport.payloads[readOffset])
					if err != nil {
						t.Fatalf("reader.Feed failed at seq %d: %v", seq, err)
					}
					if len(ch) > 0 {
						v.ApplyChanges(ch)
					}
					readOffset++
				}
			}

			// 1. Insert 20 chunks (indices 0..19) for ino 10
			ino := int64(10)
			for i := int64(0); i < 20; i++ {
				c := dynamicpb.NewMessage(chunkMD)
				c.Set(chunkMD.Fields().ByName("ino"), protoreflect.ValueOfInt64(ino))
				c.Set(chunkMD.Fields().ByName("index"), protoreflect.ValueOfInt64(i))
				c.Set(chunkMD.Fields().ByName("sha256"), protoreflect.ValueOfString(fmt.Sprintf("sha-chunk-%d", i)))
				_, _ = writer.Insert(ctx, c)
			}
			consumeNewPayloads()

			// Flush to index
			if err := v.Flush(ctx); err != nil {
				t.Fatalf("Flush failed: %v", err)
			}

			// 2. Truncate down: delete chunks 15..19
			tx1 := writer.Begin()
			for i := int64(15); i < 20; i++ {
				c := dynamicpb.NewMessage(chunkMD)
				c.Set(chunkMD.Fields().ByName("ino"), protoreflect.ValueOfInt64(ino))
				c.Set(chunkMD.Fields().ByName("index"), protoreflect.ValueOfInt64(i))
				if _, err := tx1.Delete(ctx, c); err != nil {
					t.Fatalf("tx1.Delete failed for chunk %d: %v", i, err)
				}
			}
			if _, err := tx1.Commit(ctx); err != nil {
				t.Fatalf("tx1.Commit failed: %v", err)
			}
			consumeNewPayloads()

			// 3. Truncate down again: delete chunks 12..14
			tx2 := writer.Begin()
			for i := int64(12); i < 15; i++ {
				c := dynamicpb.NewMessage(chunkMD)
				c.Set(chunkMD.Fields().ByName("ino"), protoreflect.ValueOfInt64(ino))
				c.Set(chunkMD.Fields().ByName("index"), protoreflect.ValueOfInt64(i))
				if _, err := tx2.Delete(ctx, c); err != nil {
					t.Fatalf("tx2.Delete failed for chunk %d: %v", i, err)
				}
			}
			if _, err := tx2.Commit(ctx); err != nil {
				t.Fatalf("tx2.Commit failed: %v", err)
			}
			consumeNewPayloads()

			// 4. Test Scan prefix while unapplied overlay holds tombstones
			prefixMsg := dynamicpb.NewMessage(chunkMD)
			prefixMsg.Set(chunkMD.Fields().ByName("ino"), protoreflect.ValueOfInt64(ino))
			prefixBytes, _ := sds.EncodeKeyPrefix(prefixMsg, 1)

			for idxMsg, err := range idx.Scan(ctx, "viewtest.FileChunk", prefixBytes) {
				if err != nil {
					t.Fatalf("idx.Scan error: %v", err)
				}
				k, _ := sds.ExtractKey(idxMsg, []int32{1, 2})
				dyn := idxMsg.(*dynamicpb.Message)
				t.Logf("[%s] idx.Scan row: index=%v key=%x", factory.name, dyn.Get(dyn.Descriptor().Fields().ByName("index")).Int(), k.Bytes())
			}

			scanChunks := make(map[int64]string)
			for msg, err := range v.Scan(ctx, "viewtest.FileChunk", prefixBytes) {
				if err != nil {
					t.Fatalf("Scan error: %v", err)
				}
				dyn := msg.(*dynamicpb.Message)
				idxVal := dyn.Get(dyn.Descriptor().Fields().ByName("index")).Int()
				shaVal := dyn.Get(dyn.Descriptor().Fields().ByName("sha256")).String()
				scanChunks[idxVal] = shaVal
			}

			if len(scanChunks) != 12 {
				t.Fatalf("expected 12 live chunks (0..11), got %d: %v", len(scanChunks), scanChunks)
			}
			if scanChunks[5] != "sha-chunk-5" {
				t.Fatalf("chunk 5 sha mismatch: %s", scanChunks[5])
			}
			if scanChunks[10] != "sha-chunk-10" {
				t.Fatalf("chunk 10 sha mismatch: %s", scanChunks[10])
			}
			if _, exists := scanChunks[13]; exists {
				t.Fatalf("deleted chunk 13 was returned in Scan")
			}

			// 5. Test point lookup via Get
			targetChunk := dynamicpb.NewMessage(chunkMD)
			targetChunk.Set(chunkMD.Fields().ByName("ino"), protoreflect.ValueOfInt64(ino))
			targetChunk.Set(chunkMD.Fields().ByName("index"), protoreflect.ValueOfInt64(10))
			k10, _ := sds.ExtractKey(targetChunk, []int32{1, 2})
			msg10, ok, err := v.Get(ctx, "viewtest.FileChunk", k10)
			if err != nil || !ok {
				t.Fatalf("Get chunk 10 failed: ok=%v err=%v", ok, err)
			}
			dyn10 := msg10.(*dynamicpb.Message)
			if dyn10.Get(dyn10.Descriptor().Fields().ByName("sha256")).String() != "sha-chunk-10" {
				t.Fatalf("chunk 10 sha mismatch on Get: %s", dyn10.Get(dyn10.Descriptor().Fields().ByName("sha256")).String())
			}

			// Get deleted chunk 13 -> must return !ok
			targetChunk.Set(chunkMD.Fields().ByName("index"), protoreflect.ValueOfInt64(13))
			k13, _ := sds.ExtractKey(targetChunk, []int32{1, 2})
			_, ok13, _ := v.Get(ctx, "viewtest.FileChunk", k13)
			if ok13 {
				t.Fatalf("Get chunk 13 returned true for deleted chunk")
			}

			// 6. Flush all and verify again
			if err := v.Flush(ctx); err != nil {
				t.Fatalf("Flush failed: %v", err)
			}

			scanChunksAfter := make(map[int64]string)
			for msg, err := range v.Scan(ctx, "viewtest.FileChunk", prefixBytes) {
				if err != nil {
					t.Fatalf("Scan error after flush: %v", err)
				}
				dyn := msg.(*dynamicpb.Message)
				idxVal := dyn.Get(dyn.Descriptor().Fields().ByName("index")).Int()
				shaVal := dyn.Get(dyn.Descriptor().Fields().ByName("sha256")).String()
				scanChunksAfter[idxVal] = shaVal
			}

			if len(scanChunksAfter) != 12 {
				t.Fatalf("expected 12 live chunks after flush, got %d", len(scanChunksAfter))
			}
		})
	}
}

// TestViewConcurrentScanAndDeletes verifies that continuous Scan calls never return rows that
// were deleted at or before the scan's start, even while writer commits deletes and the background
// applier asynchronously moves changes from the overlay to the underlying index.
func TestViewConcurrentScanAndDeletes(t *testing.T) {
	for _, factory := range testFactories(t) {
		t.Run(factory.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()

			streamID := uuid.New().String()
			idx, cleanup := factory.create(t, ctx, streamID)
			defer cleanup()

			transport := &memoryTransport{}
			writer := sds.NewWriter(transport)

			chunkMD := dynamicDescriptor(t, "FileChunk", []*descriptorpb.FieldDescriptorProto{
				protoField("ino", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
				protoField("index", 2, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
				protoField("sha256", 3, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
			})
			if _, err := writer.RegisterDescriptor(chunkMD, 1, 2); err != nil {
				t.Fatalf("RegisterDescriptor failed: %v", err)
			}

			// Use small batch size so the applier processes batches frequently.
			v := view.New(idx,
				view.WithBatchSize(5),
				view.WithRegistry(writer.Registry()),
			)
			defer v.Close()
			if err := v.SyncRegistry(ctx, writer.Registry()); err != nil {
				t.Fatalf("SyncRegistry failed: %v", err)
			}

			reader := sds.NewChangeReader()
			if err := reader.Registry().Import(writer.Registry().Export()); err != nil {
				t.Fatalf("Import failed: %v", err)
			}

			var feedMu sync.Mutex
			readOffset := 0
			consumeNewPayloadsLocked := func() {
				for readOffset < len(transport.payloads) {
					seq := uint64(readOffset + 1)
					ch, err := reader.Feed(seq, transport.payloads[readOffset])
					if err != nil {
						t.Errorf("reader.Feed failed at seq %d: %v", seq, err)
						return
					}
					if len(ch) > 0 {
						v.ApplyChanges(ch)
					}
					readOffset++
				}
			}

			// Pre-populate 100 chunks and flush to index
			ino := int64(42)
			totalChunks := int64(100)
			for i := int64(0); i < totalChunks; i++ {
				c := dynamicpb.NewMessage(chunkMD)
				c.Set(chunkMD.Fields().ByName("ino"), protoreflect.ValueOfInt64(ino))
				c.Set(chunkMD.Fields().ByName("index"), protoreflect.ValueOfInt64(i))
				c.Set(chunkMD.Fields().ByName("sha256"), protoreflect.ValueOfString(fmt.Sprintf("sha-%d", i)))
				if _, err := writer.Insert(ctx, c); err != nil {
					t.Fatalf("Insert failed: %v", err)
				}
			}
			consumeNewPayloadsLocked()
			if err := v.Flush(ctx); err != nil {
				t.Fatalf("Flush failed: %v", err)
			}

			prefixMsg := dynamicpb.NewMessage(chunkMD)
			prefixMsg.Set(chunkMD.Fields().ByName("ino"), protoreflect.ValueOfInt64(ino))
			prefixBytes, err := sds.EncodeKeyPrefix(prefixMsg, 1)
			if err != nil {
				t.Fatalf("EncodeKeyPrefix failed: %v", err)
			}

			var deletedMu sync.RWMutex
			deletedKeys := make(map[int64]bool)

			var wg sync.WaitGroup
			var done atomic.Bool
			var scanCount atomic.Int64

			// Reader goroutine: scans continuously while writer deletes and applier applies
			wg.Add(1)
			go func() {
				defer wg.Done()
				for !done.Load() {
					// Snapshot the keys committed as deleted prior to starting this scan
					deletedMu.RLock()
					deletedAtStart := make(map[int64]bool, len(deletedKeys))
					for k, val := range deletedKeys {
						deletedAtStart[k] = val
					}
					deletedMu.RUnlock()

					// Execute Scan
					for msg, scanErr := range v.Scan(ctx, "viewtest.FileChunk", prefixBytes) {
						if scanErr != nil {
							t.Errorf("Scan error: %v", scanErr)
							return
						}
						dyn := msg.(*dynamicpb.Message)
						chunkIdx := dyn.Get(dyn.Descriptor().Fields().ByName("index")).Int()
						if deletedAtStart[chunkIdx] {
							t.Errorf("Scan returned chunk %d which was committed-deleted before scan started", chunkIdx)
							return
						}
					}
					scanCount.Add(1)
				}
			}()

			// Writer goroutine: deletes chunks in batches of 5
			batchSize := int64(5)
			for startIdx := int64(0); startIdx < totalChunks; startIdx += batchSize {
				tx := writer.Begin()
				var batchDeleted []int64
				for i := startIdx; i < startIdx+batchSize && i < totalChunks; i++ {
					c := dynamicpb.NewMessage(chunkMD)
					c.Set(chunkMD.Fields().ByName("ino"), protoreflect.ValueOfInt64(ino))
					c.Set(chunkMD.Fields().ByName("index"), protoreflect.ValueOfInt64(i))
					if _, err := tx.Delete(ctx, c); err != nil {
						t.Fatalf("Delete failed for chunk %d: %v", i, err)
					}
					batchDeleted = append(batchDeleted, i)
				}
				if _, err := tx.Commit(ctx); err != nil {
					t.Fatalf("Commit failed: %v", err)
				}

				feedMu.Lock()
				consumeNewPayloadsLocked()
				feedMu.Unlock()

				deletedMu.Lock()
				for _, idxVal := range batchDeleted {
					deletedKeys[idxVal] = true
				}
				deletedMu.Unlock()

				time.Sleep(2 * time.Millisecond)
			}

			done.Store(true)
			wg.Wait()

			t.Logf("[%s] Completed %d scans during concurrent deletion and background apply", factory.name, scanCount.Load())
		})
	}
}

type customUnrecoverableError struct {
	msg string
}

func (e *customUnrecoverableError) Error() string {
	return e.msg
}

func (e *customUnrecoverableError) IsUnrecoverable(_ error) bool {
	return true
}

func TestViewRebuildOnUnrecoverableError(t *testing.T) {
	ctx := t.Context()
	orderMD := dynamicDescriptor(t, "OrderRebuild", []*descriptorpb.FieldDescriptorProto{
		protoField("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		protoField("item", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	})

	for _, factory := range testFactories(t) {
		t.Run(factory.name, func(t *testing.T) {
			streamID := uuid.New().String()
			initialIndex, cleanup := factory.create(t, ctx, streamID)
			defer cleanup()

			transport := &memoryTransport{}
			writer := sds.NewWriter(transport)
			_, err := writer.RegisterDescriptor(orderMD, 1)
			if err != nil {
				t.Fatalf("RegisterDescriptor failed: %v", err)
			}
			reg := writer.Registry()
			_ = initialIndex.SyncRegistry(ctx, reg)

			var streamRecords [][]sds.Change
			var streamMu sync.Mutex

			appendStream := func(chs []sds.Change) {
				streamMu.Lock()
				defer streamMu.Unlock()
				cp := make([]sds.Change, len(chs))
				copy(cp, chs)
				streamRecords = append(streamRecords, cp)
			}

			rebuildCount := atomic.Int32{}
			rebuildFunc := func(rctx context.Context) (sds.LocalIndex, uint64, error) {
				rebuildCount.Add(1)
				newIdx, _ := factory.create(t, rctx, streamID)
				_ = newIdx.SyncRegistry(rctx, reg)

				streamMu.Lock()
				defer streamMu.Unlock()
				var maxSeq uint64
				for _, batch := range streamRecords {
					_ = newIdx.ApplyBatch(rctx, batch)
					for _, ch := range batch {
						if ch.Seq > maxSeq {
							maxSeq = ch.Seq
						}
					}
				}
				return newIdx, maxSeq, nil
			}

			var faultInjected atomic.Bool
			faultHook := func() error {
				if faultInjected.Load() {
					return &customUnrecoverableError{msg: "simulated unrecoverable failure"}
				}
				return nil
			}

			v := view.New(initialIndex,
				view.WithRegistry(reg),
				view.WithBatchSize(10),
				view.WithFaultHook(faultHook),
				view.WithRebuildFunc(rebuildFunc),
			)
			defer v.Close()

			// 1. Apply some changes
			msg1 := dynamicpb.NewMessage(orderMD)
			msg1.Set(orderMD.Fields().ByName("id"), protoreflect.ValueOfInt64(100))
			msg1.Set(orderMD.Fields().ByName("item"), protoreflect.ValueOfString("Item A"))
			k1, _ := sds.ExtractKey(msg1, []int32{1})
			k1Bytes, v1Bytes, _ := sds.SplitKeyAndNonKey(msg1, []int32{1})

			ch1 := []sds.Change{
				{
					Seq:      1,
					TypeID:   16,
					TypeName: "viewtest.OrderRebuild",
					Op:       sds.OpCreate,
					Key:      k1,
					RawKey:   k1Bytes,
					RawVal:   v1Bytes,
					Row:      msg1,
				},
			}
			appendStream(ch1)
			v.ApplyChanges(ch1)
			if err := v.Flush(ctx); err != nil {
				t.Fatalf("Flush 1 failed: %v", err)
			}

			// 2. Inject unrecoverable fault and apply more changes
			faultInjected.Store(true)

			msg2 := dynamicpb.NewMessage(orderMD)
			msg2.Set(orderMD.Fields().ByName("id"), protoreflect.ValueOfInt64(200))
			msg2.Set(orderMD.Fields().ByName("item"), protoreflect.ValueOfString("Item B"))
			k2, _ := sds.ExtractKey(msg2, []int32{1})
			k2Bytes, v2Bytes, _ := sds.SplitKeyAndNonKey(msg2, []int32{1})

			ch2 := []sds.Change{
				{
					Seq:      2,
					TypeID:   16,
					TypeName: "viewtest.OrderRebuild",
					Op:       sds.OpCreate,
					Key:      k2,
					RawKey:   k2Bytes,
					RawVal:   v2Bytes,
					Row:      msg2,
				},
			}
			appendStream(ch2)
			v.ApplyChanges(ch2)

			// Reads for both items must succeed immediately through overlay and read cache
			row1, ok1, err1 := v.Get(ctx, "viewtest.OrderRebuild", k1)
			if err1 != nil || !ok1 || row1 == nil {
				t.Fatalf("Get item 1 failed during fault: ok=%v err=%v", ok1, err1)
			}
			row2, ok2, err2 := v.Get(ctx, "viewtest.OrderRebuild", k2)
			if err2 != nil || !ok2 || row2 == nil {
				t.Fatalf("Get item 2 failed during fault: ok=%v err=%v", ok2, err2)
			}

			// 3. Wait for automatic rebuild to trigger
			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) {
				if rebuildCount.Load() > 0 {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			if rebuildCount.Load() == 0 {
				t.Fatalf("expected index rebuild to have been triggered")
			}

			// Clear fault and flush
			faultInjected.Store(false)
			if err := v.Flush(ctx); err != nil {
				t.Fatalf("Flush after rebuild failed: %v", err)
			}

			// 4. Verify converged state
			finalRow2, finalOk2, _ := v.Get(ctx, "viewtest.OrderRebuild", k2)
			if !finalOk2 || finalRow2 == nil {
				t.Fatalf("final Get item 2 failed after rebuild")
			}
			dyn2 := finalRow2.(*dynamicpb.Message)
			if dyn2.Get(dyn2.Descriptor().Fields().ByName("item")).String() != "Item B" {
				t.Fatalf("unexpected item value: %v", dyn2)
			}
		})
	}
}

func TestViewRebuildOnDegradedTimeout(t *testing.T) {
	ctx := t.Context()
	orderMD := dynamicDescriptor(t, "OrderDegraded", []*descriptorpb.FieldDescriptorProto{
		protoField("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		protoField("item", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	})

	for _, factory := range testFactories(t) {
		t.Run(factory.name, func(t *testing.T) {
			streamID := uuid.New().String()
			initialIndex, cleanup := factory.create(t, ctx, streamID)
			defer cleanup()

			transport := &memoryTransport{}
			writer := sds.NewWriter(transport)
			_, err := writer.RegisterDescriptor(orderMD, 1)
			if err != nil {
				t.Fatalf("RegisterDescriptor failed: %v", err)
			}
			reg := writer.Registry()
			_ = initialIndex.SyncRegistry(ctx, reg)

			rebuildCount := atomic.Int32{}
			rebuildFunc := func(rctx context.Context) (sds.LocalIndex, uint64, error) {
				rebuildCount.Add(1)
				newIdx, _ := factory.create(t, rctx, streamID)
				_ = newIdx.SyncRegistry(rctx, reg)
				return newIdx, 0, nil
			}

			var faultInjected atomic.Bool
			faultInjected.Store(true)
			faultHook := func() error {
				if faultInjected.Load() {
					return errors.New("transient lock busy error")
				}
				return nil
			}

			v := view.New(initialIndex,
				view.WithRegistry(reg),
				view.WithBatchSize(10),
				view.WithFaultHook(faultHook),
				view.WithDegradedTimeout(50*time.Millisecond),
				view.WithRebuildFunc(rebuildFunc),
			)
			defer v.Close()

			msg := dynamicpb.NewMessage(orderMD)
			msg.Set(orderMD.Fields().ByName("id"), protoreflect.ValueOfInt64(10))
			msg.Set(orderMD.Fields().ByName("item"), protoreflect.ValueOfString("Item X"))
			k, _ := sds.ExtractKey(msg, []int32{1})

			v.ApplyChanges([]sds.Change{
				{
					Seq:      1,
					TypeID:   16,
					TypeName: "viewtest.OrderDegraded",
					Op:       sds.OpCreate,
					Key:      k,
					Row:      msg,
				},
			})

			// Wait for degraded timeout to elapse and trigger rebuild
			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) {
				if rebuildCount.Load() > 0 {
					break
				}
				time.Sleep(20 * time.Millisecond)
			}

			if rebuildCount.Load() == 0 {
				t.Fatalf("expected rebuild to trigger after degraded timeout")
			}

			// Clear fault and flush
			faultInjected.Store(false)
			if err := v.Flush(ctx); err != nil {
				t.Fatalf("Flush failed: %v", err)
			}
		})
	}
}

func TestViewGetAndScanNoCloning(t *testing.T) {
	ctx := t.Context()
	orderMD := dynamicDescriptor(t, "OrderNoClone", []*descriptorpb.FieldDescriptorProto{
		protoField("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_REQUIRED),
		protoField("item", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	})

	store := memtable.New(memtable.WithStreamID("test-no-clone"))
	defer store.Close()

	reg := record.NewRegistry()
	_, _ = reg.RegisterDescriptor(orderMD, 1)
	_ = store.SyncRegistry(ctx, reg)

	v := view.New(store,
		view.WithRegistry(reg),
		view.WithMutationCheck(),
	)
	defer v.Close()

	msg1 := dynamicpb.NewMessage(orderMD)
	msg1.Set(orderMD.Fields().ByName("id"), protoreflect.ValueOfInt64(100))
	msg1.Set(orderMD.Fields().ByName("item"), protoreflect.ValueOfString("Widget A"))
	k1, _ := sds.ExtractKey(msg1, []int32{1})

	// 1. Overlay hit (before flush)
	v.ApplyChanges([]sds.Change{
		{
			Seq:      1,
			TypeID:   1,
			TypeName: "viewtest.OrderNoClone",
			Op:       sds.OpCreate,
			Key:      k1,
			Row:      msg1,
		},
	})

	gotOverlay, ok, err := v.Get(ctx, "viewtest.OrderNoClone", k1)
	if err != nil || !ok {
		t.Fatalf("Get failed: %v, ok=%v", err, ok)
	}
	if gotOverlay != msg1 {
		t.Fatalf("expected overlay Get to return identical pointer (%p != %p)", gotOverlay, msg1)
	}

	// 2. Scan from overlay
	for scanned, sErr := range v.Scan(ctx, "viewtest.OrderNoClone", nil) {
		if sErr != nil {
			t.Fatalf("Scan error: %v", sErr)
		}
		if scanned != msg1 {
			t.Fatalf("expected Scan to yield identical pointer (%p != %p)", scanned, msg1)
		}
	}

	// 3. Flush to underlying memtable and verify cache hit / memtable Get returns identical pointer
	if err := v.Flush(ctx); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	gotCache, ok, err := v.Get(ctx, "viewtest.OrderNoClone", k1)
	if err != nil || !ok {
		t.Fatalf("Get failed after flush: %v, ok=%v", err, ok)
	}
	if gotCache != msg1 {
		t.Fatalf("expected read-cache Get to return identical pointer (%p != %p)", gotCache, msg1)
	}

	// 4. Clear cache and query memtable directly through view
	v.ClearCache()
	gotMemStore, ok, err := v.Get(ctx, "viewtest.OrderNoClone", k1)
	if err != nil || !ok {
		t.Fatalf("Get after ClearCache failed: %v, ok=%v", err, ok)
	}
	if gotMemStore != msg1 {
		t.Fatalf("expected memstore Get to return identical pointer (%p != %p)", gotMemStore, msg1)
	}

	// 5. Scan after flush
	for scanned, sErr := range v.Scan(ctx, "viewtest.OrderNoClone", nil) {
		if sErr != nil {
			t.Fatalf("Scan error after flush: %v", sErr)
		}
		if scanned != msg1 {
			t.Fatalf("expected Scan after flush to yield identical pointer (%p != %p)", scanned, msg1)
		}
	}
}

func TestMutationCheck_CallerModifiesReturnedRow(t *testing.T) {
	ctx := t.Context()
	orderMD := dynamicDescriptor(t, "OrderModRet", []*descriptorpb.FieldDescriptorProto{
		protoField("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_REQUIRED),
		protoField("item", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	})

	store := memtable.New(memtable.WithStreamID("test-mod-ret"))
	defer store.Close()

	reg := record.NewRegistry()
	_, _ = reg.RegisterDescriptor(orderMD, 1)
	_ = store.SyncRegistry(ctx, reg)

	v := view.New(store,
		view.WithRegistry(reg),
		view.WithMutationCheck(),
	)

	msg := dynamicpb.NewMessage(orderMD)
	msg.Set(orderMD.Fields().ByName("id"), protoreflect.ValueOfInt64(42))
	msg.Set(orderMD.Fields().ByName("item"), protoreflect.ValueOfString("Original"))
	k, _ := sds.ExtractKey(msg, []int32{1})

	v.ApplyChanges([]sds.Change{
		{
			Seq:      1,
			TypeID:   1,
			TypeName: "viewtest.OrderModRet",
			Op:       sds.OpCreate,
			Key:      k,
			Row:      msg,
		},
	})

	got, ok, err := v.Get(ctx, "viewtest.OrderModRet", k)
	if err != nil || !ok {
		t.Fatalf("Get failed: %v", err)
	}

	// Caller illegally mutates returned row in place
	dyn := got.(*dynamicpb.Message)
	dyn.Set(orderMD.Fields().ByName("item"), protoreflect.ValueOfString("Mutated!"))

	// Next Get or Scan must panic
	didPanic := false
	func() {
		defer func() {
			if r := recover(); r != nil {
				didPanic = true
			}
		}()
		_, _, _ = v.Get(ctx, "viewtest.OrderModRet", k)
	}()

	if !didPanic {
		t.Fatalf("expected Get to panic when row was mutated in place")
	}
}

func TestMutationCheck_CallerModifiesRowAfterRecording(t *testing.T) {
	ctx := t.Context()
	orderMD := dynamicDescriptor(t, "OrderModPost", []*descriptorpb.FieldDescriptorProto{
		protoField("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_REQUIRED),
		protoField("item", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	})

	store := memtable.New(memtable.WithStreamID("test-mod-post"))
	defer store.Close()

	reg := record.NewRegistry()
	_, _ = reg.RegisterDescriptor(orderMD, 1)
	_ = store.SyncRegistry(ctx, reg)

	v := view.New(store,
		view.WithRegistry(reg),
		view.WithMutationCheck(),
	)

	msg := dynamicpb.NewMessage(orderMD)
	msg.Set(orderMD.Fields().ByName("id"), protoreflect.ValueOfInt64(77))
	msg.Set(orderMD.Fields().ByName("item"), protoreflect.ValueOfString("Recorded"))
	k, _ := sds.ExtractKey(msg, []int32{1})

	v.ApplyChanges([]sds.Change{
		{
			Seq:      1,
			TypeID:   1,
			TypeName: "viewtest.OrderModPost",
			Op:       sds.OpCreate,
			Key:      k,
			Row:      msg,
		},
	})

	// Caller illegally mutates msg after passing it to ApplyChanges
	msg.Set(orderMD.Fields().ByName("item"), protoreflect.ValueOfString("Mutated After Apply"))

	didPanic := false
	func() {
		defer func() {
			if r := recover(); r != nil {
				didPanic = true
			}
		}()
		_, _, _ = v.Get(ctx, "viewtest.OrderModPost", k)
	}()

	if !didPanic {
		t.Fatalf("expected Get to panic when recorded row was mutated in place")
	}
}

func TestMutationCheck_DetectedOnClose(t *testing.T) {
	orderMD := dynamicDescriptor(t, "OrderModClose", []*descriptorpb.FieldDescriptorProto{
		protoField("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_REQUIRED),
		protoField("item", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	})

	store := memtable.New(memtable.WithStreamID("test-mod-close"))
	defer store.Close()

	reg := record.NewRegistry()
	_, _ = reg.RegisterDescriptor(orderMD, 1)
	_ = store.SyncRegistry(t.Context(), reg)

	v := view.New(store,
		view.WithRegistry(reg),
		view.WithMutationCheck(),
	)

	msg := dynamicpb.NewMessage(orderMD)
	msg.Set(orderMD.Fields().ByName("id"), protoreflect.ValueOfInt64(88))
	msg.Set(orderMD.Fields().ByName("item"), protoreflect.ValueOfString("Before Close"))
	k, _ := sds.ExtractKey(msg, []int32{1})

	v.ApplyChanges([]sds.Change{
		{
			Seq:      1,
			TypeID:   1,
			TypeName: "viewtest.OrderModClose",
			Op:       sds.OpCreate,
			Key:      k,
			Row:      msg,
		},
	})

	// Mutate before Close
	msg.Set(orderMD.Fields().ByName("item"), protoreflect.ValueOfString("Mutated!"))

	didPanic := false
	func() {
		defer func() {
			if r := recover(); r != nil {
				didPanic = true
			}
		}()
		_ = v.Close()
	}()

	if !didPanic {
		t.Fatalf("expected Close to panic when row was mutated in place")
	}
}

func TestMutationCheck_DetectedOnEviction(t *testing.T) {
	ctx := t.Context()
	orderMD := dynamicDescriptor(t, "OrderModEvict", []*descriptorpb.FieldDescriptorProto{
		protoField("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_REQUIRED),
		protoField("item", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	})

	store := memtable.New(memtable.WithStreamID("test-mod-evict"))
	defer store.Close()

	reg := record.NewRegistry()
	_, _ = reg.RegisterDescriptor(orderMD, 1)
	_ = store.SyncRegistry(ctx, reg)

	// Capacity 1 cache
	v := view.New(store,
		view.WithRegistry(reg),
		view.WithCacheLimits(1, 1024*1024),
		view.WithMutationCheck(),
	)

	msg1 := dynamicpb.NewMessage(orderMD)
	msg1.Set(orderMD.Fields().ByName("id"), protoreflect.ValueOfInt64(1))
	msg1.Set(orderMD.Fields().ByName("item"), protoreflect.ValueOfString("Item 1"))
	k1, _ := sds.ExtractKey(msg1, []int32{1})

	// Apply and flush so msg1 is in cache
	_ = v.ApplyChangesSync(ctx, []sds.Change{
		{
			Seq:      1,
			TypeID:   1,
			TypeName: "viewtest.OrderModEvict",
			Op:       sds.OpCreate,
			Key:      k1,
			Row:      msg1,
		},
	})

	// Mutate msg1 in place
	msg1.Set(orderMD.Fields().ByName("item"), protoreflect.ValueOfString("Mutated in Cache!"))

	// Insert msg2 to evict msg1 from cache (capacity 1)
	msg2 := dynamicpb.NewMessage(orderMD)
	msg2.Set(orderMD.Fields().ByName("id"), protoreflect.ValueOfInt64(2))
	msg2.Set(orderMD.Fields().ByName("item"), protoreflect.ValueOfString("Item 2"))
	k2, _ := sds.ExtractKey(msg2, []int32{1})

	didPanic := false
	func() {
		defer func() {
			if r := recover(); r != nil {
				didPanic = true
			}
		}()
		_ = v.ApplyChangesSync(ctx, []sds.Change{
			{
				Seq:      2,
				TypeID:   1,
				TypeName: "viewtest.OrderModEvict",
				Op:       sds.OpCreate,
				Key:      k2,
				Row:      msg2,
			},
		})
	}()

	if !didPanic {
		t.Fatalf("expected cache eviction to panic when cached row was mutated in place")
	}
}
