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

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/controller"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore/inmemorystorage"
	"github.com/gke-labs/in-cluster-storage/pkg/sds"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/projection/sqlite"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/projection/table"
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

// TestBenchmarkMeasurements runs the comparison benchmark measurements between SQLite and Table
// as specified in Issue #211.
func TestBenchmarkMeasurements(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping benchmark measurements in short mode")
	}

	ctx := t.Context()

	t.Log("==================================================================")
	t.Log("      SQLITE VS TABLE PROTOTYPE BENCHMARK MEASUREMENTS            ")
	t.Log("==================================================================")

	// ------------------------------------------------------------------
	// 1. Snapshot Publish, File Size & Restore (10k and 1M rows)
	// ------------------------------------------------------------------
	t.Log("\n--- 1. Snapshot Publish Time, File Size & Restore Time ---")
	scaleList := []int{10000}
	if os.Getenv("OBJECTFS_BENCH") != "" {
		scaleList = append(scaleList, 1000000)
	} else {
		t.Log("(Set OBJECTFS_BENCH=1 to include full 1,000,000-row scale; running 10,000 and 100,000)")
		scaleList = append(scaleList, 100000)
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
					_ = sDB.ApplyBatch(ctx, batch)
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
			_ = sDB.Close()

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
			_ = rDB.Close()

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
					_ = tDB.ApplyBatch(ctx, batch)
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
			_ = tDB.Close()

			snapSize := getObjectSize(ctx, backend, snapKey)

			// Restore Table: download and ingest
			startRest := time.Now()
			restDir := t.TempDir()
			rDB, _, err := table.RestoreSnapshot(ctx, backend, "bench-table", 0, restDir)
			restDur := time.Since(startRest)
			if err != nil {
				t.Fatalf("table RestoreSnapshot failed: %v", err)
			}
			_ = rDB.Close()

			t.Logf("[Table  @ %7d rows] Publish: %v | Snap Size: %9d B (%6.2f MB) | Restore (Download+Ingest): %v",
				numRows, pubDur, snapSize, float64(snapSize)/(1024*1024), restDur)
		}
	}

	// ------------------------------------------------------------------
	// 2. Point-Read Latency (Cache Miss) and ReadDir / Scan
	// ------------------------------------------------------------------
	t.Log("\n--- 2. Point-Read Latency (Cache Miss) & ReadDir / Scan at 10k and 1M Entries ---")

	// A. 10,000 entries (at Volume layer)
	t.Logf("\n>>> Scale: 10,000 Entries (Volume Layer) <<<")
	for _, mode := range []string{"sqlite", "table"} {
		vol := controller.NewVolume(fmt.Sprintf("bench-read-%s-10k", mode), inmemorystorage.New(), controller.NewEventBroadcaster(),
			controller.WithMetadataIndex(mode),
			controller.WithMetadataCacheDisabled(true), // Cache miss
			controller.WithLocalStorageDir(t.TempDir()),
		)
		_ = vol.LoadFromBackend(ctx)

		var inos []uint64
		for i := 0; i < 10000; i++ {
			fAttr, _ := vol.CreateFile(ctx, 1, fmt.Sprintf("file_%d.txt", i), 0644, []byte("x"), 0, 0)
			if i%10 == 0 {
				inos = append(inos, fAttr.GetInode().GetIno())
			}
		}
		_ = vol.FlushOverlay(ctx)

		startPoint := time.Now()
		for i := 0; i < len(inos); i++ {
			_, _ = vol.GetAttr(ctx, inos[i])
		}
		pointDur := time.Since(startPoint)

		startScan := time.Now()
		entries, _ := vol.ReadDir(ctx, 1)
		scanDur := time.Since(startScan)

		_ = vol.Close()

		t.Logf("[%s @  10,000 entries] Point-Read (cache miss): %v (avg %v/op) | ReadDir: %v (count=%d)",
			mode, pointDur, pointDur/time.Duration(len(inos)), scanDur, len(entries))
	}

	// B. 1,000,000 entries (at LocalIndex layer)
	t.Logf("\n>>> Scale: 1,000,000 Entries (LocalIndex Layer) <<<")
	pk := sds.NewPrimaryKey(1)
	const numEntries1M = 1000000
	const sampleLookups = 2000

	// --- SQLite at 1M ---
	{
		sqliteDir := t.TempDir()
		sDB, _ := sqlite.Open(ctx, filepath.Join(sqliteDir, "read1m.sqlite"),
			sqlite.WithStreamID("read1m-sqlite"),
			sqlite.WithJournalMode("WAL"),
			sqlite.WithSynchronous("NORMAL"),
		)
		def, _ := sDB.RegisterType(ctx, &pb.Inode{}, 1)
		batch := make([]sds.Change, 0, 1000)
		for i := 1; i <= numEntries1M; i++ {
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
			if len(batch) >= 1000 || i == numEntries1M {
				_ = sDB.ApplyBatch(ctx, batch)
				batch = batch[:0]
			}
		}

		// Point-reads
		startPoint := time.Now()
		step := numEntries1M / sampleLookups
		for i := 1; i <= numEntries1M; i += step {
			kBytes, _, _ := pk.Split(&pb.Inode{Ino: proto.Uint64(uint64(i))})
			_, _, _ = sDB.Get(ctx, def.GetName(), sds.NewKeyFromBytes(kBytes))
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
		_ = sDB.Close()

		t.Logf("[sqlite @ 1,000,000 entries] Point-Read (cache miss): %v (avg %v/op) | Full Scan (1M rows): %v (count=%d)",
			pointDur, pointDur/time.Duration(sampleLookups), scanDur, scanCount)
	}

	// --- Table at 1M ---
	{
		tableDir := t.TempDir()
		tDB, _ := table.Open(ctx, tableDir,
			table.WithStreamID("read1m-table"),
			table.WithDisableWAL(true),
		)
		def, _ := tDB.RegisterType(ctx, &pb.Inode{}, 1)
		batch := make([]sds.Change, 0, 1000)
		for i := 1; i <= numEntries1M; i++ {
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
			if len(batch) >= 1000 || i == numEntries1M {
				_ = tDB.ApplyBatch(ctx, batch)
				batch = batch[:0]
			}
		}

		// Point-reads
		startPoint := time.Now()
		step := numEntries1M / sampleLookups
		for i := 1; i <= numEntries1M; i += step {
			kBytes, _, _ := pk.Split(&pb.Inode{Ino: proto.Uint64(uint64(i))})
			_, _, _ = tDB.Get(ctx, def.GetName(), sds.NewKeyFromBytes(kBytes))
		}
		pointDur := time.Since(startPoint)

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
		_ = tDB.Close()

		t.Logf("[table  @ 1,000,000 entries] Point-Read (cache miss): %v (avg %v/op) | Full Scan (1M rows): %v (count=%d)",
			pointDur, pointDur/time.Duration(sampleLookups), scanDur, scanCount)
	}

	// ------------------------------------------------------------------
	// 3. Write Amplification per Metadata Op via /proc/self/io
	// ------------------------------------------------------------------
	t.Log("\n--- 3. Write Amplification per Metadata Op via /proc/self/io ---")
	for _, mode := range []string{"sqlite", "table"} {
		vol := controller.NewVolume("bench-wa-"+mode, inmemorystorage.New(), controller.NewEventBroadcaster(),
			controller.WithMetadataIndex(mode),
			controller.WithLocalStorageDir(t.TempDir()),
			controller.WithApplierBatchSize(10),
		)
		_ = vol.LoadFromBackend(ctx)

		const opCount = 500

		// A. CreateFile
		dirAttr, _ := vol.Mkdir(ctx, 1, "test_dir", 0755, 0, 0)
		_ = vol.FlushOverlay(ctx)
		ioBeforeCreate := readProcSelfWriteBytes()
		for i := 0; i < opCount; i++ {
			_, _ = vol.CreateFile(ctx, dirAttr.GetInode().GetIno(), fmt.Sprintf("cr_%d.txt", i), 0644, []byte("val"), 0, 0)
		}
		_ = vol.FlushOverlay(ctx)
		ioAfterCreate := readProcSelfWriteBytes()

		// B. 64 KiB Write
		fAttr, _ := vol.CreateFile(ctx, 1, "target.bin", 0644, nil, 0, 0)
		_ = vol.FlushOverlay(ctx)
		data64K := make([]byte, 64*1024)
		ioBeforeWrite := readProcSelfWriteBytes()
		for i := 0; i < opCount; i++ {
			_, _, _, _ = vol.WriteFile(ctx, fAttr.GetInode().GetIno(), 0, data64K, 0)
		}
		_ = vol.FlushOverlay(ctx)
		ioAfterWrite := readProcSelfWriteBytes()

		// C. Rename
		rAttr, _ := vol.CreateFile(ctx, 1, "ren_a.txt", 0644, []byte("x"), 0, 0)
		_ = vol.FlushOverlay(ctx)
		ioBeforeRename := readProcSelfWriteBytes()
		for i := 0; i < opCount; i++ {
			from, to := "ren_a.txt", "ren_b.txt"
			if i%2 == 1 {
				from, to = "ren_b.txt", "ren_a.txt"
			}
			_, _ = vol.Rename(ctx, 1, from, 1, to)
		}
		_ = vol.FlushOverlay(ctx)
		ioAfterRename := readProcSelfWriteBytes()

		_ = vol.Close()

		createWA := (ioAfterCreate - ioBeforeCreate) / int64(opCount)
		writeWA := (ioAfterWrite - ioBeforeWrite) / int64(opCount)
		renameWA := (ioAfterRename - ioBeforeRename) / int64(opCount)

		t.Logf("[%s] Write Amp /proc/self/io: Create: %d B/op | 64K Write: %d B/op | Rename: %d B/op",
			mode, createWA, writeWA, renameWA)
		_ = dirAttr
		_ = rAttr
	}

	// ------------------------------------------------------------------
	// 4. Steady-State Memory with 100 Volumes Open
	// ------------------------------------------------------------------
	t.Log("\n--- 4. Steady-State Memory with 100 Volumes Open ---")
	for _, mode := range []string{"sqlite", "table"} {
		runtime.GC()
		var m1 runtime.MemStats
		runtime.ReadMemStats(&m1)

		var vols []*controller.Volume
		for i := 0; i < 100; i++ {
			v := controller.NewVolume(fmt.Sprintf("vol-100-%s-%d", mode, i), inmemorystorage.New(), controller.NewEventBroadcaster(),
				controller.WithMetadataIndex(mode),
				controller.WithLocalStorageDir(t.TempDir()),
			)
			_ = v.LoadFromBackend(ctx)
			vols = append(vols, v)
		}

		runtime.GC()
		var m2 runtime.MemStats
		runtime.ReadMemStats(&m2)

		diffHeap := int64(m2.HeapAlloc) - int64(m1.HeapAlloc)
		avgPerVol := diffHeap / 100

		for _, v := range vols {
			_ = v.Close()
		}

		t.Logf("[%s] Total HeapAlloc for 100 volumes: %6.2f MB (avg %6.2f KB/vol)",
			mode, float64(diffHeap)/(1024*1024), float64(avgPerVol)/1024)
	}

	t.Log("==================================================================")
}
