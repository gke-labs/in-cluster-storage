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
	"path/filepath"
	"sync"
	"testing"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
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

func updateTestStats(stats, before, after proto.Message) error {
	s := stats.ProtoReflect()
	fields := s.Descriptor().Fields()
	countField := fields.ByName("item_count")
	priceField := fields.ByName("total_price")
	maxField := fields.ByName("max_id")

	var bPrice, aPrice, aID int64
	var bOK, aOK bool

	if before != nil {
		bm := before.ProtoReflect()
		if f := bm.Descriptor().Fields().ByName("price"); f != nil && bm.Has(f) {
			bPrice = bm.Get(f).Int()
			bOK = true
		}
	}
	if after != nil {
		am := after.ProtoReflect()
		if f := am.Descriptor().Fields().ByName("price"); f != nil && am.Has(f) {
			aPrice = am.Get(f).Int()
			aOK = true
		}
		if f := am.Descriptor().Fields().ByName("id"); f != nil && am.Has(f) {
			aID = am.Get(f).Int()
		}
	}

	if !bOK && aOK {
		s.Set(countField, protoreflect.ValueOfInt64(s.Get(countField).Int()+1))
		s.Set(priceField, protoreflect.ValueOfInt64(s.Get(priceField).Int()+aPrice))
		if aID > s.Get(maxField).Int() {
			s.Set(maxField, protoreflect.ValueOfInt64(aID))
		}
	} else if bOK && aOK {
		s.Set(priceField, protoreflect.ValueOfInt64(s.Get(priceField).Int()+(aPrice-bPrice)))
	} else if bOK && !aOK {
		s.Set(countField, protoreflect.ValueOfInt64(s.Get(countField).Int()-1))
		s.Set(priceField, protoreflect.ValueOfInt64(s.Get(priceField).Int()-bPrice))
	}
	return nil
}

func statsDescriptor(t *testing.T) protoreflect.MessageDescriptor {
	return dynamicDescriptor(t, "ItemStats", []*descriptorpb.FieldDescriptorProto{
		protoField("name", 1, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		protoField("item_count", 2, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		protoField("total_price", 3, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		protoField("max_id", 4, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	})
}

func itemDescriptor(t *testing.T) protoreflect.MessageDescriptor {
	return dynamicDescriptor(t, "Item", []*descriptorpb.FieldDescriptorProto{
		protoField("id", 1, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
		protoField("price", 2, descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL),
	})
}

func TestView_StatsAggregation(t *testing.T) {
	factories := []struct {
		name   string
		create func(t *testing.T, ctx context.Context, streamID string) (sds.LocalIndex, func())
	}{
		{
			name: "SQLite",
			create: func(t *testing.T, ctx context.Context, streamID string) (sds.LocalIndex, func()) {
				dbPath := filepath.Join(t.TempDir(), "view_stats_test.sqlite")
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

	for _, tc := range factories {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			streamID := uuid.New().String()
			index, cleanup := tc.create(t, ctx, streamID)
			defer cleanup()

			reg := record.NewRegistry()
			statsMD := statsDescriptor(t)
			itemMD := itemDescriptor(t)

			if _, err := reg.RegisterDescriptor(statsMD, 1); err != nil {
				t.Fatalf("RegisterDescriptor stats failed: %v", err)
			}
			itemDef, err := reg.RegisterDescriptor(itemMD, 1)
			if err != nil {
				t.Fatalf("RegisterDescriptor item failed: %v", err)
			}
			if err := index.SyncRegistry(ctx, reg); err != nil {
				t.Fatalf("SyncRegistry failed: %v", err)
			}

			initialStats := dynamicpb.NewMessage(statsMD)
			initialStats.Set(statsMD.Fields().ByName("name"), protoreflect.ValueOfString("stats"))

			v := view.New(index,
				view.WithRegistry(reg),
				view.WithStats(initialStats, updateTestStats),
				view.WithBatchSize(10),
			)
			defer func() { _ = v.Close() }()

			// Create items (id=10, price=100) and (id=20, price=250)
			msg1 := dynamicpb.NewMessage(itemMD)
			msg1.Set(itemMD.Fields().ByName("id"), protoreflect.ValueOfInt64(10))
			msg1.Set(itemMD.Fields().ByName("price"), protoreflect.ValueOfInt64(100))
			k1, val1, err := sds.SplitKeyAndNonKey(msg1, []int32{1})
			if err != nil {
				t.Fatalf("SplitKeyAndNonKey msg1 failed: %v", err)
			}

			msg2 := dynamicpb.NewMessage(itemMD)
			msg2.Set(itemMD.Fields().ByName("id"), protoreflect.ValueOfInt64(20))
			msg2.Set(itemMD.Fields().ByName("price"), protoreflect.ValueOfInt64(250))
			k2, val2, err := sds.SplitKeyAndNonKey(msg2, []int32{1})
			if err != nil {
				t.Fatalf("SplitKeyAndNonKey msg2 failed: %v", err)
			}

			v.ApplyChanges([]sds.Change{
				{
					Seq:      1,
					TypeID:   itemDef.GetId(),
					TypeName: "viewtest.Item",
					Op:       sds.OpCreate,
					Key:      sds.NewKeyFromBytes(k1),
					RawKey:   k1,
					RawVal:   val1,
					Row:      msg1,
				},
				{
					Seq:      1,
					TypeID:   itemDef.GetId(),
					TypeName: "viewtest.Item",
					Op:       sds.OpCreate,
					Key:      sds.NewKeyFromBytes(k2),
					RawKey:   k2,
					RawVal:   val2,
					Row:      msg2,
				},
			})

			if err := v.Flush(ctx); err != nil {
				t.Fatalf("Flush failed: %v", err)
			}

			st := v.Stats().ProtoReflect()
			if st.Get(statsMD.Fields().ByName("max_id")).Int() != 20 {
				t.Errorf("expected max_id 20, got %d", st.Get(statsMD.Fields().ByName("max_id")).Int())
			}
			if st.Get(statsMD.Fields().ByName("item_count")).Int() != 2 {
				t.Errorf("expected item_count 2, got %d", st.Get(statsMD.Fields().ByName("item_count")).Int())
			}
			if st.Get(statsMD.Fields().ByName("total_price")).Int() != 350 {
				t.Errorf("expected total_price 350, got %d", st.Get(statsMD.Fields().ByName("total_price")).Int())
			}

			// In-batch update: Update item 1 price 100 -> 300, and delete item 2
			msg1Updated := dynamicpb.NewMessage(itemMD)
			msg1Updated.Set(itemMD.Fields().ByName("id"), protoreflect.ValueOfInt64(10))
			msg1Updated.Set(itemMD.Fields().ByName("price"), protoreflect.ValueOfInt64(300))
			k1u, val1u, err := sds.SplitKeyAndNonKey(msg1Updated, []int32{1})
			if err != nil {
				t.Fatalf("SplitKeyAndNonKey msg1Updated failed: %v", err)
			}

			v.ApplyChanges([]sds.Change{
				{
					Seq:      2,
					TypeID:   itemDef.GetId(),
					TypeName: "viewtest.Item",
					Op:       sds.OpUpdate,
					Key:      sds.NewKeyFromBytes(k1u),
					RawKey:   k1u,
					RawVal:   val1u,
					Row:      msg1Updated,
				},
				{
					Seq:      2,
					TypeID:   itemDef.GetId(),
					TypeName: "viewtest.Item",
					Op:       sds.OpDelete,
					Key:      sds.NewKeyFromBytes(k2),
					RawKey:   k2,
				},
			})

			if err := v.Flush(ctx); err != nil {
				t.Fatalf("Flush failed: %v", err)
			}

			st = v.Stats().ProtoReflect()
			if st.Get(statsMD.Fields().ByName("item_count")).Int() != 1 || st.Get(statsMD.Fields().ByName("total_price")).Int() != 300 {
				t.Errorf("stats after update mismatch: count=%d, price=%d", st.Get(statsMD.Fields().ByName("item_count")).Int(), st.Get(statsMD.Fields().ByName("total_price")).Int())
			}

			// Reload stats in a new view
			v2 := view.New(index,
				view.WithRegistry(reg),
				view.WithStats(initialStats, updateTestStats),
			)
			defer func() { _ = v2.Close() }()

			st2 := v2.Stats().ProtoReflect()
			fCount := st2.Descriptor().Fields().ByName("item_count")
			fPrice := st2.Descriptor().Fields().ByName("total_price")
			if st2.Get(fCount).Int() != 1 || st2.Get(fPrice).Int() != 300 {
				t.Errorf("reloaded view stats mismatch: %+v", st2)
			}

			// Delete stats row and test full-scan recomputation
			statsKeyBytes, _, err := sds.SplitKeyAndNonKey(initialStats, []int32{1})
			if err != nil {
				t.Fatalf("SplitKeyAndNonKey stats failed: %v", err)
			}
			statsDelChange := sds.Change{
				Seq:      index.Position(),
				TypeName: "viewtest.ItemStats",
				Op:       sds.OpDelete,
				Key:      sds.NewKeyFromBytes(statsKeyBytes),
				RawKey:   statsKeyBytes,
			}
			if err := index.ApplyBatch(ctx, []sds.Change{statsDelChange}); err != nil {
				t.Fatalf("deleting stats row failed: %v", err)
			}

			v3 := view.New(index,
				view.WithRegistry(reg),
				view.WithStats(initialStats, updateTestStats),
			)
			defer func() { _ = v3.Close() }()

			st3 := v3.Stats().ProtoReflect()
			f3Count := st3.Descriptor().Fields().ByName("item_count")
			f3Price := st3.Descriptor().Fields().ByName("total_price")
			f3Max := st3.Descriptor().Fields().ByName("max_id")
			if st3.Get(f3Count).Int() != 1 || st3.Get(f3Price).Int() != 300 || st3.Get(f3Max).Int() != 10 {
				t.Errorf("recomputed view stats mismatch: %+v", st3)
			}
		})
	}
}

func TestView_InterleavedSyncAndAsyncApplies(t *testing.T) {
	ctx := t.Context()
	store := memtable.New(memtable.WithStreamID("test-interleaved"))
	defer func() {
		if err := store.Close(); err != nil {
			t.Errorf("store.Close failed: %v", err)
		}
	}()

	reg := record.NewRegistry()
	statsMD := statsDescriptor(t)
	itemMD := itemDescriptor(t)

	if _, err := reg.RegisterDescriptor(statsMD, 1); err != nil {
		t.Fatalf("RegisterDescriptor stats failed: %v", err)
	}
	itemDef, err := reg.RegisterDescriptor(itemMD, 1)
	if err != nil {
		t.Fatalf("RegisterDescriptor item failed: %v", err)
	}
	if err := store.SyncRegistry(ctx, reg); err != nil {
		t.Fatalf("SyncRegistry failed: %v", err)
	}

	initialStats := dynamicpb.NewMessage(statsMD)
	initialStats.Set(statsMD.Fields().ByName("name"), protoreflect.ValueOfString("stats"))

	v := view.New(store,
		view.WithRegistry(reg),
		view.WithStats(initialStats, updateTestStats),
		view.WithBatchSize(5),
	)
	defer func() {
		if err := v.Close(); err != nil {
			t.Errorf("v.Close failed: %v", err)
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		id := int64(i + 1)
		msg := dynamicpb.NewMessage(itemMD)
		msg.Set(itemMD.Fields().ByName("id"), protoreflect.ValueOfInt64(id))
		msg.Set(itemMD.Fields().ByName("price"), protoreflect.ValueOfInt64(10))
		k, val, err := sds.SplitKeyAndNonKey(msg, []int32{1})
		if err != nil {
			t.Fatalf("SplitKeyAndNonKey failed: %v", err)
		}

		ch := sds.Change{
			Seq:      uint64(i + 1),
			TypeID:   itemDef.GetId(),
			TypeName: "viewtest.Item",
			Op:       sds.OpCreate,
			Key:      sds.NewKeyFromBytes(k),
			RawKey:   k,
			RawVal:   val,
			Row:      msg,
		}

		if i%2 == 0 {
			v.ApplyChanges([]sds.Change{ch})
		} else {
			wg.Add(1)
			go func(c sds.Change) {
				defer wg.Done()
				if err := v.ApplyChangesSync(ctx, []sds.Change{c}); err != nil {
					t.Errorf("ApplyChangesSync failed: %v", err)
				}
			}(ch)
		}
	}

	wg.Wait()
	if err := v.Flush(ctx); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	st := v.Stats().ProtoReflect()
	if st.Get(statsMD.Fields().ByName("item_count")).Int() != 20 {
		t.Errorf("expected 20 items, got %d", st.Get(statsMD.Fields().ByName("item_count")).Int())
	}
	if st.Get(statsMD.Fields().ByName("total_price")).Int() != 200 {
		t.Errorf("expected total_price 200, got %d", st.Get(statsMD.Fields().ByName("total_price")).Int())
	}
}

type faultInjectingIndex struct {
	sds.LocalIndex
	failGetForType string
	failGetErr     error
	getFailCount   int
	mu             sync.Mutex
}

func (f *faultInjectingIndex) Get(ctx context.Context, typeName string, key sds.Key) (proto.Message, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failGetForType != "" && (typeName == f.failGetForType || f.failGetForType == "*") {
		if f.getFailCount > 0 {
			f.getFailCount--
			return nil, false, f.failGetErr
		}
	}
	return f.LocalIndex.Get(ctx, typeName, key)
}

func TestView_FailedStatsReadFailsLoad(t *testing.T) {
	ctx := t.Context()
	store := memtable.New(memtable.WithStreamID("test-fail-stats"))
	defer store.Close()

	statsMD := statsDescriptor(t)
	initialStats := dynamicpb.NewMessage(statsMD)
	initialStats.Set(statsMD.Fields().ByName("name"), protoreflect.ValueOfString("stats"))

	faulty := &faultInjectingIndex{
		LocalIndex:     store,
		failGetForType: "viewtest.ItemStats",
		failGetErr:     context.DeadlineExceeded,
		getFailCount:   10,
	}

	v := view.New(faulty, view.WithStats(initialStats, updateTestStats))
	defer v.Close()

	if err := v.LoadStats(ctx); err == nil {
		t.Errorf("expected LoadStats to fail when reading stats errors, got nil")
	}
	if err := v.SetIndex(faulty); err == nil {
		t.Errorf("expected SetIndex to fail when reading stats errors, got nil")
	}
}

func TestView_FailedIndexGetInBatchRetries(t *testing.T) {
	ctx := t.Context()
	store := memtable.New(memtable.WithStreamID("test-fail-batch-get"))
	defer func() {
		if err := store.Close(); err != nil {
			t.Errorf("store.Close failed: %v", err)
		}
	}()

	reg := record.NewRegistry()
	statsMD := statsDescriptor(t)
	itemMD := itemDescriptor(t)

	if _, err := reg.RegisterDescriptor(statsMD, 1); err != nil {
		t.Fatalf("RegisterDescriptor stats failed: %v", err)
	}
	itemDef, err := reg.RegisterDescriptor(itemMD, 1)
	if err != nil {
		t.Fatalf("RegisterDescriptor item failed: %v", err)
	}
	if err := store.SyncRegistry(ctx, reg); err != nil {
		t.Fatalf("SyncRegistry failed: %v", err)
	}

	initialStats := dynamicpb.NewMessage(statsMD)
	initialStats.Set(statsMD.Fields().ByName("name"), protoreflect.ValueOfString("stats"))

	faulty := &faultInjectingIndex{LocalIndex: store}

	v := view.New(faulty,
		view.WithRegistry(reg),
		view.WithStats(initialStats, updateTestStats),
		view.WithBatchSize(10),
	)
	defer func() {
		if err := v.Close(); err != nil {
			t.Errorf("v.Close failed: %v", err)
		}
	}()

	msg1 := dynamicpb.NewMessage(itemMD)
	msg1.Set(itemMD.Fields().ByName("id"), protoreflect.ValueOfInt64(1))
	msg1.Set(itemMD.Fields().ByName("price"), protoreflect.ValueOfInt64(50))
	k1, val1, err := sds.SplitKeyAndNonKey(msg1, []int32{1})
	if err != nil {
		t.Fatalf("SplitKeyAndNonKey msg1 failed: %v", err)
	}

	v.ApplyChanges([]sds.Change{
		{
			Seq:      1,
			TypeID:   itemDef.GetId(),
			TypeName: "viewtest.Item",
			Op:       sds.OpCreate,
			Key:      sds.NewKeyFromBytes(k1),
			RawKey:   k1,
			RawVal:   val1,
			Row:      msg1,
		},
	})
	if err := v.Flush(ctx); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	faulty.mu.Lock()
	faulty.failGetForType = "viewtest.Item"
	faulty.failGetErr = context.DeadlineExceeded
	faulty.getFailCount = 2
	faulty.mu.Unlock()

	msg1Updated := dynamicpb.NewMessage(itemMD)
	msg1Updated.Set(itemMD.Fields().ByName("id"), protoreflect.ValueOfInt64(1))
	msg1Updated.Set(itemMD.Fields().ByName("price"), protoreflect.ValueOfInt64(100))
	k1u, val1u, err := sds.SplitKeyAndNonKey(msg1Updated, []int32{1})
	if err != nil {
		t.Fatalf("SplitKeyAndNonKey msg1Updated failed: %v", err)
	}

	v.ApplyChanges([]sds.Change{
		{
			Seq:      2,
			TypeID:   itemDef.GetId(),
			TypeName: "viewtest.Item",
			Op:       sds.OpUpdate,
			Key:      sds.NewKeyFromBytes(k1u),
			RawKey:   k1u,
			RawVal:   val1u,
			Row:      msg1Updated,
		},
	})

	if err := v.Flush(ctx); err != nil {
		t.Fatalf("Flush failed after retry: %v", err)
	}

	st := v.Stats().ProtoReflect()
	if st.Get(statsMD.Fields().ByName("total_price")).Int() != 100 {
		t.Errorf("expected total_price 100 after retry, got %d", st.Get(statsMD.Fields().ByName("total_price")).Int())
	}
}

func BenchmarkApplierThroughput_WithAggregator(b *testing.B) {
	ctx := b.Context()
	dbPath := filepath.Join(b.TempDir(), "bench_applier.sqlite")
	db, err := sqlite.Open(ctx, dbPath,
		sqlite.WithStreamID("bench-stream"),
		sqlite.WithJournalMode("WAL"),
		sqlite.WithSynchronous("NORMAL"),
	)
	if err != nil {
		b.Fatalf("sqlite.Open failed: %v", err)
	}
	defer func() { _ = db.Close() }()

	reg := record.NewRegistry()
	statsMD := statsDescriptor(nil)
	itemMD := itemDescriptor(nil)

	_, _ = reg.RegisterDescriptor(statsMD, 1)
	itemDef, _ := reg.RegisterDescriptor(itemMD, 1)
	_ = db.SyncRegistry(ctx, reg)

	initialStats := dynamicpb.NewMessage(statsMD)
	initialStats.Set(statsMD.Fields().ByName("name"), protoreflect.ValueOfString("stats"))

	v := view.New(db,
		view.WithRegistry(reg),
		view.WithStats(initialStats, updateTestStats),
		view.WithBatchSize(100),
	)
	defer func() { _ = v.Close() }()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg := dynamicpb.NewMessage(itemMD)
		msg.Set(itemMD.Fields().ByName("id"), protoreflect.ValueOfInt64(int64(i+1)))
		msg.Set(itemMD.Fields().ByName("price"), protoreflect.ValueOfInt64(100))
		k, val, _ := sds.SplitKeyAndNonKey(msg, []int32{1})

		v.ApplyChanges([]sds.Change{
			{
				Seq:      uint64(i + 1),
				TypeID:   itemDef.GetId(),
				TypeName: "viewtest.Item",
				Op:       sds.OpCreate,
				Key:      sds.NewKeyFromBytes(k),
				RawKey:   k,
				RawVal:   val,
				Row:      msg,
			},
		})
	}
	_ = v.Flush(ctx)
}

func BenchmarkApplierThroughput_WithoutAggregator(b *testing.B) {
	ctx := b.Context()
	dbPath := filepath.Join(b.TempDir(), "bench_applier_noagg.sqlite")
	db, err := sqlite.Open(ctx, dbPath,
		sqlite.WithStreamID("bench-stream-noagg"),
		sqlite.WithJournalMode("WAL"),
		sqlite.WithSynchronous("NORMAL"),
	)
	if err != nil {
		b.Fatalf("sqlite.Open failed: %v", err)
	}
	defer func() { _ = db.Close() }()

	reg := record.NewRegistry()
	itemMD := itemDescriptor(nil)
	itemDef, _ := reg.RegisterDescriptor(itemMD, 1)
	_ = db.SyncRegistry(ctx, reg)

	v := view.New(db,
		view.WithRegistry(reg),
		view.WithBatchSize(100),
	)
	defer func() { _ = v.Close() }()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg := dynamicpb.NewMessage(itemMD)
		msg.Set(itemMD.Fields().ByName("id"), protoreflect.ValueOfInt64(int64(i+1)))
		msg.Set(itemMD.Fields().ByName("price"), protoreflect.ValueOfInt64(100))
		k, val, _ := sds.SplitKeyAndNonKey(msg, []int32{1})

		v.ApplyChanges([]sds.Change{
			{
				Seq:      uint64(i + 1),
				TypeID:   itemDef.GetId(),
				TypeName: "viewtest.Item",
				Op:       sds.OpCreate,
				Key:      sds.NewKeyFromBytes(k),
				RawKey:   k,
				RawVal:   val,
				Row:      msg,
			},
		})
	}
	_ = v.Flush(ctx)
}
