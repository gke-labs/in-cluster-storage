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

package table_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/controller"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore/inmemorystorage"
	"github.com/gke-labs/in-cluster-storage/pkg/sds"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/projection/sqlite"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/projection/table"
	"github.com/gke-labs/in-cluster-storage/pkg/wal/buffer/buffertest"
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
	"google.golang.org/protobuf/proto"
)

func readProcSelfWriteBytes() int64 {
	data, err := os.ReadFile("/proc/self/io")
	if err != nil {
		return -1
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "write_bytes:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				v, err := strconv.ParseInt(fields[1], 10, 64)
				if err == nil {
					return v
				}
			}
		}
	}
	return -1
}

type countWriter struct {
	n int64
}

func (c *countWriter) Write(p []byte) (int, error) {
	c.n += int64(len(p))
	return len(p), nil
}

func getObjectSize(ctx context.Context, backend *inmemorystorage.Backend, key string) int64 {
	var cw countWriter
	_ = backend.GetObject(ctx, "", key, 0, 0, &cw)
	return cw.n
}

func testStream(t testing.TB, volID string) walclient.Stream {
	t.Helper()
	bt, _ := buffertest.Start(t.Context(), t)
	s, err := walclient.Open(t.Context(), t.TempDir(), controller.StreamIDForVolume(volID), bt.Target,
		walclient.WithGRPCClient(bt.Client),
		walclient.WithDialOptions(bt.DialOpts...),
		walclient.WithoutFsync(),
	)
	if err != nil {
		t.Fatalf("failed to open test stream: %v", err)
	}
	return s
}

// TestBenchmarkMeasurements runs the comparison benchmark measurements between SQLite and Table
// as specified in Issue #211.
func TestBenchmarkMeasurements(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping benchmark measurements in short mode")
	}

	ctx := t.Context()
	isBench := os.Getenv("OBJECTFS_BENCH") != ""

	t.Log("==================================================================")
	t.Log("      SQLITE VS TABLE PROTOTYPE BENCHMARK MEASUREMENTS            ")
	t.Log("==================================================================")
	if !isBench {
		t.Log("(Running fast smoke variant. Set OBJECTFS_BENCH=1 for full 1,000,000-row scale)")
	}

	// ------------------------------------------------------------------
	// 1. Snapshot Publish, File Size & Restore (10k and 1M rows)
	// ------------------------------------------------------------------
	t.Log("\n--- 1. Snapshot Publish Time, File Size & Restore Time ---")
	scaleList := []int{1000}
	if isBench {
		scaleList = []int{10000, 1000000}
	}

	for _, numRows := range scaleList {
		t.Logf("\n>>> Testing Scale: %d Rows <<<", numRows)

		// --- SQLite ---
		{
			sqliteDir := t.TempDir()
			sqliteDBPath := filepath.Join(sqliteDir, "test.sqlite")
			sDB, err := sqlite.Open(ctx, sqliteDBPath,
				sqlite.WithStreamID("bench-sqlite"),
				sqlite.WithJournalMode("WAL"),
				sqlite.WithSynchronous("NORMAL"),
			)
			if err != nil {
				t.Fatalf("sqlite.Open failed: %v", err)
			}
			def, err := sDB.RegisterType(ctx, &pb.Inode{}, 1)
			if err != nil {
				t.Fatalf("sqlite.RegisterType failed: %v", err)
			}

			pk := sds.NewPrimaryKey(1)
			batch := make([]sds.Change, 0, 500)
			for i := 1; i <= numRows; i++ {
				node := &pb.Inode{Ino: proto.Uint64(uint64(i)), Mode: 0644}
				k, v, _ := pk.Split(node)
				batch = append(batch, sds.Change{
					Op:       sds.OpCreate,
					TypeID:   def.GetId(),
					TypeName: def.GetName(),
					RawKey:   k,
					RawVal:   v,
					Seq:      uint64(i),
				})
				if len(batch) >= 500 || i == numRows {
					if err := sDB.ApplyBatch(ctx, batch); err != nil {
						t.Fatalf("sqlite ApplyBatch failed: %v", err)
					}
					batch = batch[:0]
				}
			}

			backend := inmemorystorage.New()
			startPub := time.Now()
			snapKey, _, err := sqlite.PublishSnapshot(ctx, sDB, backend, t.TempDir())
			pubDur := time.Since(startPub)
			if err != nil {
				t.Fatalf("sqlite PublishSnapshot failed: %v", err)
			}
			if err := sDB.Close(); err != nil {
				t.Fatalf("sqlite Close failed: %v", err)
			}

			snapSize := getObjectSize(ctx, backend, snapKey)

			// Restore SQLite: download and open
			startRest := time.Now()
			restDir := t.TempDir()
			restPath := filepath.Join(restDir, "restored.sqlite")
			rDB, _, err := sqlite.RestoreSnapshot(ctx, backend, "bench-sqlite", 0, restPath)
			restDur := time.Since(startRest)
			if err != nil {
				t.Fatalf("sqlite RestoreSnapshot failed: %v", err)
			}
			if err := rDB.Close(); err != nil {
				t.Fatalf("sqlite restored Close failed: %v", err)
			}

			t.Logf("[SQLite @ %7d rows] Publish: %v | Snap Size: %9d B (%6.2f MB) | Restore (Download+Open): %v",
				numRows, pubDur, snapSize, float64(snapSize)/(1024*1024), restDur)
		}

		// --- Table ---
		{
			tableDir := t.TempDir()
			tDB, err := table.Open(ctx, tableDir,
				table.WithStreamID("bench-table"),
				table.WithDisableWAL(true),
			)
			if err != nil {
				t.Fatalf("table.Open failed: %v", err)
			}
			def, err := tDB.RegisterType(ctx, &pb.Inode{}, 1)
			if err != nil {
				t.Fatalf("table.RegisterType failed: %v", err)
			}

			pk := sds.NewPrimaryKey(1)
			batch := make([]sds.Change, 0, 500)
			for i := 1; i <= numRows; i++ {
				node := &pb.Inode{Ino: proto.Uint64(uint64(i)), Mode: 0644}
				k, v, _ := pk.Split(node)
				batch = append(batch, sds.Change{
					Op:       sds.OpCreate,
					TypeID:   def.GetId(),
					TypeName: def.GetName(),
					RawKey:   k,
					RawVal:   v,
					Seq:      uint64(i),
				})
				if len(batch) >= 500 || i == numRows {
					if err := tDB.ApplyBatch(ctx, batch); err != nil {
						t.Fatalf("table ApplyBatch failed: %v", err)
					}
					batch = batch[:0]
				}
			}

			backend := inmemorystorage.New()
			startPub := time.Now()
			snapKey, _, err := table.PublishSnapshot(ctx, tDB, backend, t.TempDir())
			pubDur := time.Since(startPub)
			if err != nil {
				t.Fatalf("table PublishSnapshot failed: %v", err)
			}
			if err := tDB.Close(); err != nil {
				t.Fatalf("table Close failed: %v", err)
			}

			snapSize := getObjectSize(ctx, backend, snapKey)

			// Restore Table: download and ingest
			startRest := time.Now()
			restDir := t.TempDir()
			rDB, _, err := table.RestoreSnapshot(ctx, backend, "bench-table", 0, restDir)
			restDur := time.Since(startRest)
			if err != nil {
				t.Fatalf("table RestoreSnapshot failed: %v", err)
			}
			if err := rDB.Close(); err != nil {
				t.Fatalf("table restored Close failed: %v", err)
			}

			t.Logf("[Table  @ %7d rows] Publish: %v | Snap Size: %9d B (%6.2f MB) | Restore (Download+Ingest): %v",
				numRows, pubDur, snapSize, float64(snapSize)/(1024*1024), restDur)
		}
	}

	// ------------------------------------------------------------------
	// 2. Point-Read Latency (Cache Miss) and ReadDir / Scan
	// ------------------------------------------------------------------
	t.Log("\n--- 2. Point-Read Latency (Cache Miss) & ReadDir / Scan ---")

	// A. Entries at Volume layer
	numVolEntries := 200
	if isBench {
		numVolEntries = 10000
	}
	t.Logf("\n>>> Scale: %d Entries (Volume Layer) <<<", numVolEntries)
	for _, mode := range []string{"sqlite", "table"} {
		volID := fmt.Sprintf("bench-read-%s-%d", mode, numVolEntries)
		vol, err := controller.NewVolume(volID, inmemorystorage.New(), controller.NewEventBroadcaster(),
			controller.WithMetadataIndex(mode),
			controller.WithMetadataCacheDisabled(true), // Cache miss
			controller.WithLocalStorageDir(t.TempDir()),
			controller.WithStream(testStream(t, volID)),
		)
		if err != nil {
			t.Fatalf("failed to create volume (%s): %v", mode, err)
		}
		if err := vol.LoadFromBackend(ctx); err != nil {
			t.Fatalf("failed to load volume (%s): %v", mode, err)
		}

		var inos []uint64
		step := 10
		if !isBench {
			step = 1
		}
		for i := 0; i < numVolEntries; i++ {
			fAttr, err := vol.CreateFile(ctx, 1, fmt.Sprintf("file_%d.txt", i), 0644, []byte("x"), 0, 0, 0)
			if err != nil {
				t.Fatalf("CreateFile failed: %v", err)
			}
			if i%step == 0 {
				inos = append(inos, fAttr.GetInode().GetIno())
			}
		}
		if err := vol.FlushOverlay(ctx); err != nil {
			t.Fatalf("FlushOverlay failed: %v", err)
		}

		startPoint := time.Now()
		for i := 0; i < len(inos); i++ {
			if _, err := vol.GetAttr(ctx, inos[i]); err != nil {
				t.Fatalf("GetAttr failed: %v", err)
			}
		}
		pointDur := time.Since(startPoint)

		startScan := time.Now()
		entries, err := vol.ReadDir(ctx, 1)
		scanDur := time.Since(startScan)
		if err != nil {
			t.Fatalf("ReadDir failed: %v", err)
		}

		if err := vol.Close(); err != nil {
			t.Fatalf("failed to close volume (%s): %v", mode, err)
		}

		t.Logf("[%s @ %7d entries] Point-Read (cache miss): %v (avg %v/op) | ReadDir: %v (count=%d)",
			mode, numVolEntries, pointDur, pointDur/time.Duration(len(inos)), scanDur, len(entries))
	}

	// B. LocalIndex layer (1,000,000 entries if OBJECTFS_BENCH=1, else 1,000 entries smoke)
	numEntries := 1000
	sampleLookups := 100
	if isBench {
		numEntries = 1000000
		sampleLookups = 2000
	}
	t.Logf("\n>>> Scale: %d Entries (LocalIndex Layer) <<<", numEntries)
	pk := sds.NewPrimaryKey(1)

	// --- SQLite ---
	{
		sqliteDir := t.TempDir()
		sDB, err := sqlite.Open(ctx, filepath.Join(sqliteDir, "read1m.sqlite"),
			sqlite.WithStreamID("read-sqlite"),
			sqlite.WithJournalMode("WAL"),
			sqlite.WithSynchronous("NORMAL"),
		)
		if err != nil {
			t.Fatalf("sqlite.Open failed: %v", err)
		}
		def, err := sDB.RegisterType(ctx, &pb.Inode{}, 1)
		if err != nil {
			t.Fatalf("sqlite.RegisterType failed: %v", err)
		}
		batch := make([]sds.Change, 0, 1000)
		for i := 1; i <= numEntries; i++ {
			node := &pb.Inode{Ino: proto.Uint64(uint64(i)), Mode: 0644}
			k, v, _ := pk.Split(node)
			batch = append(batch, sds.Change{
				Op:       sds.OpCreate,
				TypeID:   def.GetId(),
				TypeName: def.GetName(),
				RawKey:   k,
				RawVal:   v,
				Seq:      uint64(i),
			})
			if len(batch) >= 1000 || i == numEntries {
				if err := sDB.ApplyBatch(ctx, batch); err != nil {
					t.Fatalf("sqlite ApplyBatch failed: %v", err)
				}
				batch = batch[:0]
			}
		}

		// Point-reads
		startPoint := time.Now()
		step := numEntries / sampleLookups
		if step == 0 {
			step = 1
		}
		actualLookups := 0
		for i := 1; i <= numEntries; i += step {
			kBytes, _, _ := pk.Split(&pb.Inode{Ino: proto.Uint64(uint64(i))})
			msg, found, err := sDB.Get(ctx, def.GetName(), sds.NewKeyFromBytes(kBytes))
			if err != nil || !found || msg == nil {
				t.Fatalf("sqlite Get failed for ino %d: found=%v, err=%v", i, found, err)
			}
			actualLookups++
		}
		pointDur := time.Since(startPoint)

		// Full Scan
		startScan := time.Now()
		scanCount := 0
		for _, err := range sDB.Scan(ctx, def.GetName(), nil) {
			if err != nil {
				break
			}
			scanCount++
		}
		scanDur := time.Since(startScan)
		if err := sDB.Close(); err != nil {
			t.Fatalf("sqlite Close failed: %v", err)
		}

		t.Logf("[sqlite @ %d entries] Point-Read: %v (avg %v/op, lookups=%d) | Full Scan: %v (count=%d)",
			numEntries, pointDur, pointDur/time.Duration(actualLookups), actualLookups, scanDur, scanCount)
	}

	// --- Table with Bloom Filter (bloom.FilterPolicy(10), NoCompression, 8MB cache) ---
	{
		tableDir := t.TempDir()
		tDB, err := table.Open(ctx, tableDir,
			table.WithStreamID("read-table-bloom"),
			table.WithDisableWAL(true),
		)
		if err != nil {
			t.Fatalf("table.Open failed: %v", err)
		}
		def, err := tDB.RegisterType(ctx, &pb.Inode{}, 1)
		if err != nil {
			t.Fatalf("table.RegisterType failed: %v", err)
		}
		batch := make([]sds.Change, 0, 1000)
		for i := 1; i <= numEntries; i++ {
			node := &pb.Inode{Ino: proto.Uint64(uint64(i)), Mode: 0644}
			k, v, _ := pk.Split(node)
			batch = append(batch, sds.Change{
				Op:       sds.OpCreate,
				TypeID:   def.GetId(),
				TypeName: def.GetName(),
				RawKey:   k,
				RawVal:   v,
				Seq:      uint64(i),
			})
			if len(batch) >= 1000 || i == numEntries {
				if err := tDB.ApplyBatch(ctx, batch); err != nil {
					t.Fatalf("table ApplyBatch failed: %v", err)
				}
				batch = batch[:0]
			}
		}

		// 1. Unflushed Point-reads (rows residing in memtables)
		startUnflushed := time.Now()
		step := numEntries / sampleLookups
		if step == 0 {
			step = 1
		}
		actualLookups := 0
		for i := 1; i <= numEntries; i += step {
			kBytes, _, _ := pk.Split(&pb.Inode{Ino: proto.Uint64(uint64(i))})
			msg, found, err := tDB.Get(ctx, def.GetName(), sds.NewKeyFromBytes(kBytes))
			if err != nil || !found || msg == nil {
				t.Fatalf("table Get (unflushed) failed for ino %d: found=%v, err=%v", i, found, err)
			}
			actualLookups++
		}
		durUnflushed := time.Since(startUnflushed)
		t.Logf("[table @ %d entries] Point-Read (unflushed memtable): %v (avg %v/op, lookups=%d)",
			numEntries, durUnflushed, durUnflushed/time.Duration(actualLookups), actualLookups)

		// Flush memtables to stable storage
		if err := tDB.Pebble().Flush(); err != nil {
			t.Fatalf("tDB.Pebble().Flush failed: %v", err)
		}

		// Compact over the whole key range into steady state
		if err := tDB.Pebble().Compact(ctx, []byte{0x00}, []byte{0xff}, false); err != nil {
			t.Fatalf("tDB.Pebble().Compact failed: %v", err)
		}

		// Print Metrics()
		metrics := tDB.Pebble().Metrics()
		l0Files := metrics.Levels[0].TablesCount
		totalTables := metrics.Total().TablesCount
		memtableCount := metrics.MemTable.Count
		t.Logf("[table @ %d entries] Steady-state metrics: L0 files=%d, live tables=%d, memtables=%d",
			numEntries, l0Files, totalTables, memtableCount)

		// 2. Steady-State Point-reads (flushed and compacted into SSTables with bloom filters)
		startSteady := time.Now()
		actualSteadyLookups := 0
		for i := 1; i <= numEntries; i += step {
			kBytes, _, _ := pk.Split(&pb.Inode{Ino: proto.Uint64(uint64(i))})
			msg, found, err := tDB.Get(ctx, def.GetName(), sds.NewKeyFromBytes(kBytes))
			if err != nil || !found || msg == nil {
				t.Fatalf("table Get (steady-state) failed for ino %d: found=%v, err=%v", i, found, err)
			}
			actualSteadyLookups++
		}
		durSteady := time.Since(startSteady)
		t.Logf("[table (bloom 10b/key, no compression, 8MB cache) @ %d entries] Point-Read (steady state): %v (avg %v/op, lookups=%d)",
			numEntries, durSteady, durSteady/time.Duration(actualSteadyLookups), actualSteadyLookups)

		// Full Scan
		startScan := time.Now()
		scanCount := 0
		for _, err := range tDB.Scan(ctx, def.GetName(), nil) {
			if err != nil {
				break
			}
			scanCount++
		}
		scanDur := time.Since(startScan)
		if err := tDB.Close(); err != nil {
			t.Fatalf("table Close failed: %v", err)
		}

		t.Logf("[table (bloom 10b/key, no compression, 8MB cache) @ %d entries] Full Scan: %v (count=%d)",
			numEntries, scanDur, scanCount)
	}

	// --- Table WITHOUT Bloom Filter (for comparison) ---
	if isBench {
		tableDir := t.TempDir()
		rawOpts := &pebble.Options{}
		rawOpts.EnsureDefaults()
		for i := range rawOpts.Levels {
			rawOpts.Levels[i].FilterPolicy = pebble.NoFilterPolicy
		}
		tDB, err := table.Open(ctx, tableDir,
			table.WithStreamID("read-table-nobloom"),
			table.WithDisableWAL(true),
			table.WithPebbleOptions(rawOpts),
		)
		if err != nil {
			t.Fatalf("table.Open (nobloom) failed: %v", err)
		}
		def, err := tDB.RegisterType(ctx, &pb.Inode{}, 1)
		if err != nil {
			t.Fatalf("table.RegisterType failed: %v", err)
		}
		batch := make([]sds.Change, 0, 1000)
		for i := 1; i <= numEntries; i++ {
			node := &pb.Inode{Ino: proto.Uint64(uint64(i)), Mode: 0644}
			k, v, _ := pk.Split(node)
			batch = append(batch, sds.Change{
				Op:       sds.OpCreate,
				TypeID:   def.GetId(),
				TypeName: def.GetName(),
				RawKey:   k,
				RawVal:   v,
				Seq:      uint64(i),
			})
			if len(batch) >= 1000 || i == numEntries {
				if err := tDB.ApplyBatch(ctx, batch); err != nil {
					t.Fatalf("table ApplyBatch failed: %v", err)
				}
				batch = batch[:0]
			}
		}

		// 1. Unflushed Point-reads without bloom filter
		startNoBloomUnflushed := time.Now()
		step := numEntries / sampleLookups
		actualNoBloomLookups := 0
		for i := 1; i <= numEntries; i += step {
			kBytes, _, _ := pk.Split(&pb.Inode{Ino: proto.Uint64(uint64(i))})
			msg, found, err := tDB.Get(ctx, def.GetName(), sds.NewKeyFromBytes(kBytes))
			if err != nil || !found || msg == nil {
				t.Fatalf("table Get (no bloom, unflushed) failed for ino %d: found=%v, err=%v", i, found, err)
			}
			actualNoBloomLookups++
		}
		durNoBloomUnflushed := time.Since(startNoBloomUnflushed)
		t.Logf("[table (no bloom filter) @ %d entries] Point-Read (unflushed memtable): %v (avg %v/op, lookups=%d)",
			numEntries, durNoBloomUnflushed, durNoBloomUnflushed/time.Duration(actualNoBloomLookups), actualNoBloomLookups)

		// Flush and compact into steady state
		if err := tDB.Pebble().Flush(); err != nil {
			t.Fatalf("tDB (no bloom).Pebble().Flush failed: %v", err)
		}
		if err := tDB.Pebble().Compact(ctx, []byte{0x00}, []byte{0xff}, false); err != nil {
			t.Fatalf("tDB (no bloom).Pebble().Compact failed: %v", err)
		}

		startNoBloomSteady := time.Now()
		actualNoBloomSteadyLookups := 0
		for i := 1; i <= numEntries; i += step {
			kBytes, _, _ := pk.Split(&pb.Inode{Ino: proto.Uint64(uint64(i))})
			msg, found, err := tDB.Get(ctx, def.GetName(), sds.NewKeyFromBytes(kBytes))
			if err != nil || !found || msg == nil {
				t.Fatalf("table Get (no bloom, steady-state) failed for ino %d: found=%v, err=%v", i, found, err)
			}
			actualNoBloomSteadyLookups++
		}
		durNoBloomSteady := time.Since(startNoBloomSteady)
		t.Logf("[table (no bloom filter) @ %d entries] Point-Read (steady state): %v (avg %v/op, lookups=%d)",
			numEntries, durNoBloomSteady, durNoBloomSteady/time.Duration(actualNoBloomSteadyLookups), actualNoBloomSteadyLookups)

		if err := tDB.Close(); err != nil {
			t.Fatalf("table Close failed: %v", err)
		}
	}

	// ------------------------------------------------------------------
	// 3. Write Amplification per Metadata Op via /proc/self/io
	// ------------------------------------------------------------------
	t.Log("\n--- 3. Write Amplification per Metadata Op via /proc/self/io (DisableWAL, NoSync) ---")
	opCount := 20
	if isBench {
		opCount = 500
	}
	for _, mode := range []string{"sqlite", "table"} {
		volID := "bench-wa-" + mode
		vol, err := controller.NewVolume(volID, inmemorystorage.New(), controller.NewEventBroadcaster(),
			controller.WithMetadataIndex(mode),
			controller.WithLocalStorageDir(t.TempDir()),
			controller.WithApplierBatchSize(10),
			controller.WithStream(testStream(t, volID)),
		)
		if err != nil {
			t.Fatalf("failed to create volume (%s): %v", mode, err)
		}
		if err := vol.LoadFromBackend(ctx); err != nil {
			t.Fatalf("failed to load volume (%s): %v", mode, err)
		}

		flushAndCompact := func() error {
			idx := vol.Index()
			if tDB, ok := idx.(*table.DB); ok {
				if err := tDB.Pebble().Flush(); err != nil {
					return fmt.Errorf("pebble flush: %w", err)
				}
				if err := tDB.Pebble().Compact(ctx, []byte{0x00}, []byte{0xff}, false); err != nil {
					return fmt.Errorf("pebble compact: %w", err)
				}
			} else if sDB, ok := idx.(*sqlite.DB); ok {
				if _, err := sDB.SQLDB().ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
					return fmt.Errorf("sqlite wal_checkpoint: %w", err)
				}
			}
			return nil
		}

		// A. CreateFile
		dirAttr, err := vol.Mkdir(ctx, 1, "test_dir", 0755, 0, 0)
		if err != nil {
			t.Fatalf("Mkdir failed: %v", err)
		}
		if err := vol.FlushOverlay(ctx); err != nil {
			t.Fatalf("FlushOverlay failed: %v", err)
		}
		if err := flushAndCompact(); err != nil {
			t.Fatalf("flushAndCompact failed: %v", err)
		}
		ioBeforeCreate := readProcSelfWriteBytes()
		for i := 0; i < opCount; i++ {
			if _, err := vol.CreateFile(ctx, dirAttr.GetInode().GetIno(), fmt.Sprintf("cr_%d.txt", i), 0644, []byte("val"), 0, 0, 0); err != nil {
				t.Fatalf("CreateFile failed: %v", err)
			}
		}
		if err := vol.FlushOverlay(ctx); err != nil {
			t.Fatalf("FlushOverlay failed: %v", err)
		}
		ioAfterCreateInWindow := readProcSelfWriteBytes()
		if err := flushAndCompact(); err != nil {
			t.Fatalf("flushAndCompact failed: %v", err)
		}
		ioAfterCreateAmortized := readProcSelfWriteBytes()

		// B. 64 KiB Write
		fAttr, err := vol.CreateFile(ctx, 1, "target.bin", 0644, nil, 0, 0, 0)
		if err != nil {
			t.Fatalf("CreateFile failed: %v", err)
		}
		if err := vol.FlushOverlay(ctx); err != nil {
			t.Fatalf("FlushOverlay failed: %v", err)
		}
		if err := flushAndCompact(); err != nil {
			t.Fatalf("flushAndCompact failed: %v", err)
		}
		data64K := make([]byte, 64*1024)
		ioBeforeWrite := readProcSelfWriteBytes()
		for i := 0; i < opCount; i++ {
			if _, _, _, err := vol.WriteFile(ctx, fAttr.GetInode().GetIno(), 0, data64K, 0); err != nil {
				t.Fatalf("WriteFile failed: %v", err)
			}
		}
		if err := vol.FlushOverlay(ctx); err != nil {
			t.Fatalf("FlushOverlay failed: %v", err)
		}
		ioAfterWriteInWindow := readProcSelfWriteBytes()
		if err := flushAndCompact(); err != nil {
			t.Fatalf("flushAndCompact failed: %v", err)
		}
		ioAfterWriteAmortized := readProcSelfWriteBytes()

		// C. Rename
		if _, err := vol.CreateFile(ctx, 1, "ren_a.txt", 0644, []byte("x"), 0, 0, 0); err != nil {
			t.Fatalf("CreateFile failed: %v", err)
		}
		if err := vol.FlushOverlay(ctx); err != nil {
			t.Fatalf("FlushOverlay failed: %v", err)
		}
		if err := flushAndCompact(); err != nil {
			t.Fatalf("flushAndCompact failed: %v", err)
		}
		ioBeforeRename := readProcSelfWriteBytes()
		for i := 0; i < opCount; i++ {
			from, to := "ren_a.txt", "ren_b.txt"
			if i%2 == 1 {
				from, to = "ren_b.txt", "ren_a.txt"
			}
			if _, err := vol.Rename(ctx, 1, from, 1, to); err != nil {
				t.Fatalf("Rename failed: %v", err)
			}
		}
		if err := vol.FlushOverlay(ctx); err != nil {
			t.Fatalf("FlushOverlay failed: %v", err)
		}
		ioAfterRenameInWindow := readProcSelfWriteBytes()
		if err := flushAndCompact(); err != nil {
			t.Fatalf("flushAndCompact failed: %v", err)
		}
		ioAfterRenameAmortized := readProcSelfWriteBytes()

		// Close volume and check error at the end of each iteration to prevent goroutine leak / data race
		if err := vol.Close(); err != nil {
			t.Fatalf("failed to close volume (%s): %v", mode, err)
		}

		createInWindow := (ioAfterCreateInWindow - ioBeforeCreate) / int64(opCount)
		createAmortized := (ioAfterCreateAmortized - ioBeforeCreate) / int64(opCount)
		writeInWindow := (ioAfterWriteInWindow - ioBeforeWrite) / int64(opCount)
		writeAmortized := (ioAfterWriteAmortized - ioBeforeWrite) / int64(opCount)
		renameInWindow := (ioAfterRenameInWindow - ioBeforeRename) / int64(opCount)
		renameAmortized := (ioAfterRenameAmortized - ioBeforeRename) / int64(opCount)

		t.Logf("[%s] Write Amp /proc/self/io (in-window, deferred to flush): Create: %d B/op | 64K Write: %d B/op | Rename: %d B/op",
			mode, createInWindow, writeInWindow, renameInWindow)
		t.Logf("[%s] Write Amp /proc/self/io (amortized Flush+Compact):      Create: %d B/op | 64K Write: %d B/op | Rename: %d B/op",
			mode, createAmortized, writeAmortized, renameAmortized)
	}

	// ------------------------------------------------------------------
	// 4. Steady-State Memory with Volumes Open
	// ------------------------------------------------------------------
	numVols := 5
	if isBench {
		numVols = 100
	}
	t.Logf("\n--- 4. Steady-State Memory with %d Volumes Open ---", numVols)
	for _, mode := range []string{"sqlite", "table"} {
		runtime.GC()
		var m1 runtime.MemStats
		runtime.ReadMemStats(&m1)

		var vols []*controller.Volume
		for i := 0; i < numVols; i++ {
			volID := fmt.Sprintf("vol-%s-%d", mode, i)
			v, err := controller.NewVolume(volID, inmemorystorage.New(), controller.NewEventBroadcaster(),
				controller.WithMetadataIndex(mode),
				controller.WithLocalStorageDir(t.TempDir()),
				controller.WithStream(testStream(t, volID)),
			)
			if err != nil {
				t.Fatalf("failed to create volume (%s-%d): %v", mode, i, err)
			}
			if err := v.LoadFromBackend(ctx); err != nil {
				t.Fatalf("failed to load volume (%s-%d): %v", mode, i, err)
			}
			vols = append(vols, v)
		}

		runtime.GC()
		var m2 runtime.MemStats
		runtime.ReadMemStats(&m2)

		diffHeap := int64(m2.HeapAlloc) - int64(m1.HeapAlloc)
		avgPerVol := diffHeap / int64(numVols)

		for _, v := range vols {
			if err := v.Close(); err != nil {
				t.Fatalf("failed to close volume (%s): %v", mode, err)
			}
		}

		t.Logf("[%s] Total HeapAlloc for %d volumes: %6.2f MB (avg %6.2f KB/vol)",
			mode, numVols, float64(diffHeap)/(1024*1024), float64(avgPerVol)/1024)
	}

	t.Log("==================================================================")
}

func init() {
	// Controller registration uses explicit options:
	// stream is the source of truth, position and rows move in one batch.
	tableCache := pebble.NewCache(64 * 1024 * 1024)
	sds.RegisterIndexFactory(table.NewFactory(
		table.WithDisableWAL(true), // stream is the source of truth, position and rows move in one batch
		table.WithCache(tableCache),
	))
}
