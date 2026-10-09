/*
Copyright 2026 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore/inmemorystorage"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/projection/sqlite"
	_ "github.com/gke-labs/in-cluster-storage/pkg/sds/projection/table"
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
	"github.com/google/uuid"
)

func BenchmarkMetadataStoreOperations(b *testing.B) {
	configs := []struct {
		name         string
		store        string
		cacheDisable bool
	}{
		{name: "sqlite_cache_on", store: "sqlite", cacheDisable: false},
		{name: "sqlite_cache_off", store: "sqlite", cacheDisable: true},
	}

	for _, cfg := range configs {
		b.Run(fmt.Sprintf("mode=%s", cfg.name), func(b *testing.B) {
			b.Run("Create", func(b *testing.B) {
				ctx := b.Context()
				backend := inmemorystorage.New()
				broadcaster := NewEventBroadcaster()
				vol := NewVolume("bench-create-"+cfg.name, backend, broadcaster,
					WithMetadataIndex(cfg.store),
					WithMetadataCacheDisabled(cfg.cacheDisable),
					WithLocalStorageDir(b.TempDir()),
					WithoutVolumeMutationCheck(),
				)
				_ = vol.LoadFromBackend(ctx)
				defer vol.Close()

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					name := fmt.Sprintf("file_%d.txt", i)
					_, err := vol.CreateFile(ctx, 1, name, 0644, []byte("data"), 0, 0)
					if err != nil {
						b.Fatalf("CreateFile failed: %v", err)
					}
				}
			})

			b.Run("Stat", func(b *testing.B) {
				ctx := b.Context()
				backend := inmemorystorage.New()
				broadcaster := NewEventBroadcaster()
				vol := NewVolume("bench-stat-"+cfg.name, backend, broadcaster,
					WithMetadataIndex(cfg.store),
					WithMetadataCacheDisabled(cfg.cacheDisable),
					WithLocalStorageDir(b.TempDir()),
					WithoutVolumeMutationCheck(),
				)
				_ = vol.LoadFromBackend(ctx)
				defer vol.Close()

				attr, err := vol.CreateFile(ctx, 1, "target.txt", 0644, []byte("data"), 0, 0)
				if err != nil {
					b.Fatalf("CreateFile failed: %v", err)
				}

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_, err := vol.GetAttr(ctx, attr.GetInode().GetIno())
					if err != nil {
						b.Fatalf("GetAttr failed: %v", err)
					}
				}
			})

			b.Run("StatParallel", func(b *testing.B) {
				ctx := b.Context()
				backend := inmemorystorage.New()
				broadcaster := NewEventBroadcaster()
				vol := NewVolume("bench-stat-par-"+cfg.name, backend, broadcaster,
					WithMetadataIndex(cfg.store),
					WithMetadataCacheDisabled(cfg.cacheDisable),
					WithLocalStorageDir(b.TempDir()),
					WithoutVolumeMutationCheck(),
				)
				_ = vol.LoadFromBackend(ctx)
				defer vol.Close()

				attr, err := vol.CreateFile(ctx, 1, "target.txt", 0644, []byte("data"), 0, 0)
				if err != nil {
					b.Fatalf("CreateFile failed: %v", err)
				}

				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						_, err := vol.GetAttr(ctx, attr.GetInode().GetIno())
						if err != nil {
							b.Fatalf("GetAttr failed: %v", err)
						}
					}
				})
			})

			b.Run("Lookup", func(b *testing.B) {
				ctx := b.Context()
				backend := inmemorystorage.New()
				broadcaster := NewEventBroadcaster()
				vol := NewVolume("bench-lookup-"+cfg.name, backend, broadcaster,
					WithMetadataIndex(cfg.store),
					WithMetadataCacheDisabled(cfg.cacheDisable),
					WithLocalStorageDir(b.TempDir()),
					WithoutVolumeMutationCheck(),
				)
				_ = vol.LoadFromBackend(ctx)
				defer vol.Close()

				_, err := vol.CreateFile(ctx, 1, "target.txt", 0644, []byte("data"), 0, 0)
				if err != nil {
					b.Fatalf("CreateFile failed: %v", err)
				}

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_, err := vol.Lookup(ctx, 1, "target.txt")
					if err != nil {
						b.Fatalf("Lookup failed: %v", err)
					}
				}
			})

			b.Run("LookupParallel", func(b *testing.B) {
				ctx := b.Context()
				backend := inmemorystorage.New()
				broadcaster := NewEventBroadcaster()
				vol := NewVolume("bench-lookup-par-"+cfg.name, backend, broadcaster,
					WithMetadataIndex(cfg.store),
					WithMetadataCacheDisabled(cfg.cacheDisable),
					WithLocalStorageDir(b.TempDir()),
					WithoutVolumeMutationCheck(),
				)
				_ = vol.LoadFromBackend(ctx)
				defer vol.Close()

				_, err := vol.CreateFile(ctx, 1, "target.txt", 0644, []byte("data"), 0, 0)
				if err != nil {
					b.Fatalf("CreateFile failed: %v", err)
				}

				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						_, err := vol.Lookup(ctx, 1, "target.txt")
						if err != nil {
							b.Fatalf("Lookup failed: %v", err)
						}
					}
				})
			})

			b.Run("ReadDir", func(b *testing.B) {
				ctx := b.Context()
				backend := inmemorystorage.New()
				broadcaster := NewEventBroadcaster()
				vol := NewVolume("bench-readdir-"+cfg.name, backend, broadcaster,
					WithMetadataIndex(cfg.store),
					WithMetadataCacheDisabled(cfg.cacheDisable),
					WithLocalStorageDir(b.TempDir()),
					WithoutVolumeMutationCheck(),
				)
				_ = vol.LoadFromBackend(ctx)
				defer vol.Close()

				dirAttr, err := vol.Mkdir(ctx, 1, "dir", 0755, 0, 0)
				if err != nil {
					b.Fatalf("Mkdir failed: %v", err)
				}
				for j := 0; j < 100; j++ {
					_, _ = vol.CreateFile(ctx, dirAttr.GetInode().GetIno(), fmt.Sprintf("f%d.txt", j), 0644, []byte("x"), 0, 0)
				}

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					entries, err := vol.ReadDir(ctx, dirAttr.GetInode().GetIno())
					if err != nil || len(entries) != 100 {
						b.Fatalf("ReadDir failed: %v (entries=%d)", err, len(entries))
					}
				}
			})

			b.Run("ReadDir1k", func(b *testing.B) {
				ctx := b.Context()
				backend := inmemorystorage.New()
				broadcaster := NewEventBroadcaster()
				vol := NewVolume("bench-readdir1k-"+cfg.name, backend, broadcaster,
					WithMetadataIndex(cfg.store),
					WithMetadataCacheDisabled(cfg.cacheDisable),
					WithLocalStorageDir(b.TempDir()),
					WithoutVolumeMutationCheck(),
				)
				_ = vol.LoadFromBackend(ctx)
				defer vol.Close()

				dirAttr, err := vol.Mkdir(ctx, 1, "dir1k", 0755, 0, 0)
				if err != nil {
					b.Fatalf("Mkdir failed: %v", err)
				}
				for j := 0; j < 1000; j++ {
					_, _ = vol.CreateFile(ctx, dirAttr.GetInode().GetIno(), fmt.Sprintf("f%d.txt", j), 0644, []byte("x"), 0, 0)
				}

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					entries, err := vol.ReadDir(ctx, dirAttr.GetInode().GetIno())
					if err != nil || len(entries) != 1000 {
						b.Fatalf("ReadDir failed: %v (entries=%d)", err, len(entries))
					}
				}
			})

			b.Run("ReadDir10k", func(b *testing.B) {
				ctx := b.Context()
				backend := inmemorystorage.New()
				broadcaster := NewEventBroadcaster()
				vol := NewVolume("bench-readdir10k-"+cfg.name, backend, broadcaster,
					WithMetadataIndex(cfg.store),
					WithMetadataCacheDisabled(cfg.cacheDisable),
					WithLocalStorageDir(b.TempDir()),
					WithoutVolumeMutationCheck(),
				)
				_ = vol.LoadFromBackend(ctx)
				defer vol.Close()

				dirAttr, err := vol.Mkdir(ctx, 1, "dir10k", 0755, 0, 0)
				if err != nil {
					b.Fatalf("Mkdir failed: %v", err)
				}
				for j := 0; j < 10000; j++ {
					_, _ = vol.CreateFile(ctx, dirAttr.GetInode().GetIno(), fmt.Sprintf("f%d.txt", j), 0644, []byte("x"), 0, 0)
				}

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					entries, err := vol.ReadDir(ctx, dirAttr.GetInode().GetIno())
					if err != nil || len(entries) != 10000 {
						b.Fatalf("ReadDir failed: %v (entries=%d)", err, len(entries))
					}
				}
			})

			b.Run("Rename", func(b *testing.B) {
				ctx := b.Context()
				backend := inmemorystorage.New()
				broadcaster := NewEventBroadcaster()
				vol := NewVolume("bench-rename-"+cfg.name, backend, broadcaster,
					WithMetadataIndex(cfg.store),
					WithMetadataCacheDisabled(cfg.cacheDisable),
					WithLocalStorageDir(b.TempDir()),
					WithoutVolumeMutationCheck(),
				)
				_ = vol.LoadFromBackend(ctx)
				defer vol.Close()

				_, err := vol.CreateFile(ctx, 1, "name_a.txt", 0644, []byte("data"), 0, 0)
				if err != nil {
					b.Fatalf("CreateFile failed: %v", err)
				}

				var cnt uint64
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					c := atomic.AddUint64(&cnt, 1)
					from := "name_a.txt"
					to := "name_b.txt"
					if c%2 == 0 {
						from = "name_b.txt"
						to = "name_a.txt"
					}
					_, err := vol.Rename(ctx, 1, from, 1, to)
					if err != nil {
						b.Fatalf("Rename failed: %v", err)
					}
				}
			})
		})
	}
}

func Benchmark64KiBFsyncedWrite(b *testing.B) {
	data64KiB := make([]byte, 64*1024)
	for i := range data64KiB {
		data64KiB[i] = byte(i % 256)
	}

	ctx := b.Context()
	backend := inmemorystorage.New()
	broadcaster := NewEventBroadcaster()
	vol := NewVolume("bench-fsync-sqlite", backend, broadcaster,
		WithMetadataIndex("sqlite"),
		WithLocalStorageDir(b.TempDir()),
	)
	_ = vol.LoadFromBackend(ctx)
	defer vol.Close()

	fileAttr, err := vol.CreateFile(ctx, 1, "write_test.bin", 0644, nil, 0, 0)
	if err != nil {
		b.Fatalf("CreateFile failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _, err := vol.WriteFile(ctx, fileAttr.GetInode().GetIno(), 0, data64KiB, pb.WriteMode_WRITE_MODE_UNSPECIFIED)
		if err != nil {
			b.Fatalf("WriteFile failed: %v", err)
		}
		if err := vol.Fsync(ctx, fileAttr.GetInode().GetIno()); err != nil {
			b.Fatalf("Fsync failed: %v", err)
		}
	}
}

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

func calculatePercentiles(durations []time.Duration) (p50, p90, p99, max time.Duration) {
	if len(durations) == 0 {
		return 0, 0, 0, 0
	}
	sorted := make([]time.Duration, len(durations))
	copy(sorted, durations)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	p50 = sorted[len(sorted)*50/100]
	p90 = sorted[len(sorted)*90/100]
	p99 = sorted[len(sorted)*99/100]
	max = sorted[len(sorted)-1]
	return
}

func getStreamBytes(vol *Volume) int64 {
	if vol.memAppender != nil {
		payloads := vol.memAppender.Payloads()
		var total int64
		for _, p := range payloads {
			total += int64(len(p))
		}
		return total
	}
	return 0
}

func TestBenchmarkMetricsReport(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping slow benchmark report in short mode")
	}
	ctx := t.Context()

	t.Log("================ METADATA STORE COMPARISON REPORT ================")

	// 1. Directory Entry Addition Scaling (Empty vs 10,000 Entry Directory)
	t.Log("\n--- 1. Directory Entry Addition Scaling ---")
	for _, mode := range []string{"memory", "sqlite", "table"} {
		backend := inmemorystorage.New()
		vol := NewVolume("scale-"+mode, backend, NewEventBroadcaster(),
			WithMetadataIndex(mode),
			WithLocalStorageDir(t.TempDir()),
		)
		_ = vol.LoadFromBackend(ctx)

		// Empty directory addition
		emptyDir, _ := vol.Mkdir(ctx, 1, "empty_dir", 0755, 0, 0)
		start := time.Now()
		_, _ = vol.CreateFile(ctx, emptyDir.GetInode().GetIno(), "first_entry.txt", 0644, []byte("x"), 0, 0)
		emptyDur := time.Since(start)

		// Populate large directory with 10,000 entries
		largeDir, _ := vol.Mkdir(ctx, 1, "large_dir", 0755, 0, 0)
		for i := 0; i < 10000; i++ {
			_, _ = vol.CreateFile(ctx, largeDir.GetInode().GetIno(), fmt.Sprintf("f%d.txt", i), 0644, []byte("x"), 0, 0)
		}

		// Add one entry to 10,000 entry directory
		start = time.Now()
		_, _ = vol.CreateFile(ctx, largeDir.GetInode().GetIno(), "extra_entry.txt", 0644, []byte("x"), 0, 0)
		largeDur := time.Since(start)

		_ = vol.Close()
		t.Logf("[%s] Add to empty dir: %v | Add to 10k entry dir: %v", mode, emptyDur, largeDur)
	}

	// 2. Cache-Miss & Scale Latency (15,000 files across 100 dirs)
	t.Log("\n--- 2. Cache-Miss & Scale Latency (15,000 files across 100 dirs) ---")
	for _, cfg := range []struct {
		name         string
		store        string
		cacheDisable bool
	}{
		{name: "memory", store: "memory", cacheDisable: false},
		{name: "sqlite (cache ON)", store: "sqlite", cacheDisable: false},
		{name: "sqlite (cache OFF)", store: "sqlite", cacheDisable: true},
		{name: "table (cache ON)", store: "table", cacheDisable: false},
		{name: "table (cache OFF)", store: "table", cacheDisable: true},
	} {
		backend := inmemorystorage.New()
		vol := NewVolume("cachemiss-"+cfg.name, backend, NewEventBroadcaster(),
			WithMetadataIndex(cfg.store),
			WithMetadataCacheDisabled(cfg.cacheDisable),
			WithLocalStorageDir(t.TempDir()),
		)
		_ = vol.LoadFromBackend(ctx)

		var fileInos []uint64
		// Create 15,000 files (exceeds legacy 10,000 cache capacity)
		for d := 0; d < 150; d++ {
			dirAttr, _ := vol.Mkdir(ctx, 1, fmt.Sprintf("dir_%d", d), 0755, 0, 0)
			for f := 0; f < 100; f++ {
				fAttr, _ := vol.CreateFile(ctx, dirAttr.GetInode().GetIno(), fmt.Sprintf("file_%d.txt", f), 0644, []byte("val"), 0, 0)
				fileInos = append(fileInos, fAttr.GetInode().GetIno())
			}
		}

		// Lookup earliest created files (evicted from memory in legacy store)
		vol.MetadataCacheResetStats()
		start := time.Now()
		for i := 0; i < 1000; i++ {
			_, _ = vol.GetAttr(ctx, fileInos[i])
		}
		statDur := time.Since(start)

		start = time.Now()
		for d := 0; d < 10; d++ {
			_, _ = vol.Lookup(ctx, 1, fmt.Sprintf("dir_%d", d))
		}
		lookupDur := time.Since(start)

		stats := vol.MetadataCacheStats()
		t.Logf("[%s] Stat 1,000 inodes: %v (avg %v/op) | Lookup 10 dirs: %v (avg %v/op) | Cache: hits=%d misses=%d hitRate=%.2f%% entries=%d bytes=%d KB",
			cfg.name, statDur, statDur/1000, lookupDur, lookupDur/10, stats.Hits, stats.Misses, stats.HitRate*100, stats.Entries, stats.Bytes/1024)
		_ = vol.Close()
	}

	// 3. SQLite Read Cache Scale & Hit Rate Analysis (10,000 and 50,000 files)
	t.Log("\n--- 3. SQLite Read Cache Scale & Hit Rate Analysis (Byte-Bound Governed) ---")
	for _, numFiles := range []int{10000, 50000} {
		for _, cacheOn := range []bool{true, false} {
			backend := inmemorystorage.New()
			vol := NewVolume(fmt.Sprintf("hitrate-scale-%d", numFiles), backend, NewEventBroadcaster(),
				WithMetadataIndex("sqlite"),
				WithMetadataCacheDisabled(!cacheOn),
				WithMetadataCacheLimits(0, 64*1024*1024),
				WithLocalStorageDir(t.TempDir()),
			)
			_ = vol.LoadFromBackend(ctx)

			var createdInos []uint64
			for i := 0; i < numFiles; i++ {
				fAttr, _ := vol.CreateFile(ctx, 1, fmt.Sprintf("f%d.txt", i), 0644, []byte("data"), 0, 0)
				createdInos = append(createdInos, fAttr.GetInode().GetIno())
			}

			// Measure repeat Stat lookups (hot path)
			vol.MetadataCacheResetStats()
			start := time.Now()
			for i := 0; i < 5000; i++ {
				_, _ = vol.GetAttr(ctx, createdInos[i])
			}
			hotStatDur := time.Since(start)

			// Measure repeat name Lookups
			start = time.Now()
			for i := 0; i < 5000; i++ {
				_, _ = vol.Lookup(ctx, 1, fmt.Sprintf("f%d.txt", i))
			}
			hotLookupDur := time.Since(start)

			// Measure negative lookups with repeated misses (100 distinct missing names x 50 repetitions = 5,000 lookups)
			start = time.Now()
			for rep := 0; rep < 50; rep++ {
				for i := 0; i < 100; i++ {
					_, _ = vol.Lookup(ctx, 1, fmt.Sprintf("missing_%d.txt", i))
				}
			}
			enoentDur := time.Since(start)

			stats := vol.MetadataCacheStats()
			cacheStatus := "cache ON"
			if !cacheOn {
				cacheStatus = "cache OFF"
			}
			t.Logf("[%s @ %d files] Stat 5k: %v (%v/op) | Lookup 5k: %v (%v/op) | ENOENT 5k (100x50): %v (%v/op) | Hits=%d Misses=%d HitRate=%.2f%% | Mem=%d KB / %d KB (entries=%d)",
				cacheStatus, numFiles, hotStatDur, hotStatDur/5000, hotLookupDur, hotLookupDur/5000, enoentDur, enoentDur/5000, stats.Hits, stats.Misses, stats.HitRate*100, stats.Bytes/1024, stats.MaxBytes/1024, stats.Entries)
			_ = vol.Close()
		}
	}

	// 4. Cold Start Recovery Times
	t.Log("\n--- 4. Cold Start Time to Serve First Request ---")
	{
		backend := inmemorystorage.New()
		streamID := uuid.New()
		localDir := t.TempDir()

		// Setup populated volume
		vol1 := NewVolume("cold-start-vol", backend, NewEventBroadcaster(),
			WithMetadataIndex("sqlite"),
			WithLocalStorageDir(localDir),
			WithStreamID(streamID),
		)
		_ = vol1.LoadFromBackend(ctx)
		for i := 0; i < 500; i++ {
			_, _ = vol1.CreateFile(ctx, 1, fmt.Sprintf("init_%d.txt", i), 0644, []byte("data"), 0, 0)
		}
		_ = vol1.FlushToBackend(ctx)
		_ = vol1.Close()

		// A. Restart from local SQLite file
		start := time.Now()
		volLocal := NewVolume("cold-start-vol", backend, NewEventBroadcaster(),
			WithMetadataIndex("sqlite"),
			WithLocalStorageDir(localDir),
			WithStreamID(streamID),
		)
		_ = volLocal.LoadFromBackend(ctx)
		_, _ = volLocal.Lookup(ctx, 1, "init_0.txt")
		localColdStartDur := time.Since(start)
		_ = volLocal.Close()

		// B. Restart from published SQLite snapshot (new local dir)
		start = time.Now()
		volSnap := NewVolume("cold-start-vol", backend, NewEventBroadcaster(),
			WithMetadataIndex("sqlite"),
			WithLocalStorageDir(t.TempDir()),
			WithStreamID(streamID),
		)
		_ = volSnap.LoadFromBackend(ctx)
		_, _ = volSnap.Lookup(ctx, 1, "init_0.txt")
		snapColdStartDur := time.Since(start)
		_ = volSnap.Close()

		// C. Restart from EROFS snapshot
		erofsBackend := inmemorystorage.New()
		volErofsInit := NewVolume("erofs-cold-vol", erofsBackend, NewEventBroadcaster(),
			WithMetadataIndex("sqlite"),
			WithLocalStorageDir(t.TempDir()),
			WithStreamID(streamID),
		)
		_ = volErofsInit.LoadFromBackend(ctx)
		for i := 0; i < 500; i++ {
			_, _ = volErofsInit.CreateFile(ctx, 1, fmt.Sprintf("init_%d.txt", i), 0644, []byte("data"), 0, 0)
		}
		_ = volErofsInit.FlushToBackend(ctx)
		_ = volErofsInit.Close()

		start = time.Now()
		volErofsImport := NewVolume("erofs-cold-vol", erofsBackend, NewEventBroadcaster(),
			WithMetadataIndex("sqlite"),
			WithLocalStorageDir(t.TempDir()),
			WithStreamID(streamID),
		)
		_ = volErofsImport.LoadFromBackend(ctx)
		_, _ = volErofsImport.Lookup(ctx, 1, "init_0.txt")
		erofsColdStartDur := time.Since(start)
		_ = volErofsImport.Close()

		t.Logf("Cold start from local SQLite file:        %v", localColdStartDur)
		t.Logf("Cold start from published SQLite snapshot:  %v", snapColdStartDur)
		t.Logf("Cold start from EROFS snapshot:            %v", erofsColdStartDur)
	}

	// 5. Memory Consumption Comparison
	t.Log("\n--- 5. Memory Consumption Comparison (15,000 files) ---")
	for _, mode := range []string{"memory", "sqlite", "table"} {
		runtime.GC()
		var m1 runtime.MemStats
		runtime.ReadMemStats(&m1)

		backend := inmemorystorage.New()
		vol := NewVolume("mem-"+mode, backend, NewEventBroadcaster(),
			WithMetadataIndex(mode),
			WithLocalStorageDir(t.TempDir()),
		)
		_ = vol.LoadFromBackend(ctx)

		for i := 0; i < 15000; i++ {
			_, _ = vol.CreateFile(ctx, 1, fmt.Sprintf("file_%d.txt", i), 0644, []byte("hello"), 0, 0)
		}

		runtime.GC()
		var m2 runtime.MemStats
		runtime.ReadMemStats(&m2)

		diffHeap := int64(m2.HeapAlloc) - int64(m1.HeapAlloc)
		t.Logf("[%s] HeapAlloc after 15,000 files: %d KB", mode, diffHeap/1024)
		_ = vol.Close()
	}

	// 6. Async SQLite Overlay & Batch Applier Scaling & Write Amplification
	t.Log("\n--- 6. Async SQLite Overlay & Write Amplification (PRAGMA wal_autocheckpoint=0 & /proc/self/io) ---")
	getDiskFootprint := func(dir string) (dbBytes int64, walBytes int64) {
		if fi, err := os.Stat(filepath.Join(dir, "metadata.sqlite")); err == nil {
			dbBytes = fi.Size()
		}
		if fi, err := os.Stat(filepath.Join(dir, "metadata.sqlite-wal")); err == nil {
			walBytes = fi.Size()
		}
		return
	}

	for _, batchSize := range []int{1, 10, 100} {
		localDir := t.TempDir()
		// Open with wal_autocheckpoint=0 to accurately capture WAL frames written without reuse/truncation
		sqliteFactory := sqlite.NewFactory(sqlite.WithAutoCheckpoint(0))
		vol := NewVolume("batch-wa-bench", inmemorystorage.New(), NewEventBroadcaster(),
			WithIndexFactory(sqliteFactory),
			WithLocalStorageDir(localDir),
			WithApplierBatchSize(batchSize),
		)
		_ = vol.LoadFromBackend(ctx)

		const opCount = 200

		// --- A. Create Operations ---
		dirAttr, _ := vol.Mkdir(ctx, 1, "hot_dir", 0755, 0, 0)
		_ = vol.FlushOverlay(ctx)
		streamBeforeCreate := getStreamBytes(vol)
		_, walBeforeCreate := getDiskFootprint(localDir)
		procBeforeCreate := readProcSelfWriteBytes()

		startCreate := time.Now()
		for i := 0; i < opCount; i++ {
			_, _ = vol.CreateFile(ctx, dirAttr.GetInode().GetIno(), fmt.Sprintf("f_%d.txt", i), 0644, []byte("val"), 0, 0)
		}
		createDur := time.Since(startCreate)
		_ = vol.FlushOverlay(ctx)

		streamAfterCreate := getStreamBytes(vol)
		_, walAfterCreate := getDiskFootprint(localDir)
		procAfterCreate := readProcSelfWriteBytes()

		walDeltaCreate := walAfterCreate - walBeforeCreate
		streamDeltaCreate := streamAfterCreate - streamBeforeCreate
		procDeltaCreate := int64(0)
		if procBeforeCreate >= 0 && procAfterCreate >= procBeforeCreate {
			procDeltaCreate = procAfterCreate - procBeforeCreate
		}

		sqliteBytesPerCreate := walDeltaCreate / int64(opCount)
		streamBytesPerCreate := streamDeltaCreate / int64(opCount)
		totalBytesPerCreate := sqliteBytesPerCreate + streamBytesPerCreate

		// --- B. 64 KiB Writes (Metadata Footprint) ---
		fAttr, _ := vol.CreateFile(ctx, 1, "large_write.bin", 0644, nil, 0, 0)
		_ = vol.FlushOverlay(ctx)
		data64K := make([]byte, 64*1024)

		streamBeforeWrite := getStreamBytes(vol)
		_, walBeforeWrite := getDiskFootprint(localDir)
		procBeforeWrite := readProcSelfWriteBytes()

		startWrite := time.Now()
		for i := 0; i < opCount; i++ {
			_, _, _, _ = vol.WriteFile(ctx, fAttr.GetInode().GetIno(), 0, data64K, 0)
		}
		writeDur := time.Since(startWrite)
		_ = vol.FlushOverlay(ctx)

		streamAfterWrite := getStreamBytes(vol)
		_, walAfterWrite := getDiskFootprint(localDir)
		procAfterWrite := readProcSelfWriteBytes()

		walDeltaWrite := walAfterWrite - walBeforeWrite
		streamDeltaWrite := streamAfterWrite - streamBeforeWrite
		procDeltaWrite := int64(0)
		if procBeforeWrite >= 0 && procAfterWrite >= procBeforeWrite {
			procDeltaWrite = procAfterWrite - procBeforeWrite
		}

		sqliteBytesPerWrite := walDeltaWrite / int64(opCount)
		streamBytesPerWrite := streamDeltaWrite / int64(opCount)
		totalBytesPerWrite := sqliteBytesPerWrite + streamBytesPerWrite

		// --- C. Rename Operations ---
		rAttr, _ := vol.CreateFile(ctx, 1, "r_a.txt", 0644, []byte("x"), 0, 0)
		_ = vol.FlushOverlay(ctx)

		streamBeforeRename := getStreamBytes(vol)
		_, walBeforeRename := getDiskFootprint(localDir)
		procBeforeRename := readProcSelfWriteBytes()

		startRename := time.Now()
		for i := 0; i < opCount; i++ {
			from, to := "r_a.txt", "r_b.txt"
			if i%2 == 1 {
				from, to = "r_b.txt", "r_a.txt"
			}
			_, _ = vol.Rename(ctx, 1, from, 1, to)
		}
		renameDur := time.Since(startRename)
		_ = vol.FlushOverlay(ctx)

		streamAfterRename := getStreamBytes(vol)
		_, walAfterRename := getDiskFootprint(localDir)
		procAfterRename := readProcSelfWriteBytes()

		walDeltaRename := walAfterRename - walBeforeRename
		streamDeltaRename := streamAfterRename - streamBeforeRename
		procDeltaRename := int64(0)
		if procBeforeRename >= 0 && procAfterRename >= procBeforeRename {
			procDeltaRename = procAfterRename - procBeforeRename
		}

		sqliteBytesPerRename := walDeltaRename / int64(opCount)
		streamBytesPerRename := streamDeltaRename / int64(opCount)
		totalBytesPerRename := sqliteBytesPerRename + streamBytesPerRename

		_ = vol.Close()

		t.Logf("[batch_size=%3d] CREATE:  %v (avg %v/op) | SQLite: %d B/op | Stream: %d B/op | Total Metadata: %d B/op (Proc I/O: %d B/op)",
			batchSize, createDur, createDur/time.Duration(opCount), sqliteBytesPerCreate, streamBytesPerCreate, totalBytesPerCreate, procDeltaCreate/int64(opCount))
		t.Logf("[batch_size=%3d] 64K WR:  %v (avg %v/op) | SQLite: %d B/op | Stream: %d B/op | Total Metadata: %d B/op (Proc I/O: %d B/op)",
			batchSize, writeDur, writeDur/time.Duration(opCount), sqliteBytesPerWrite, streamBytesPerWrite, totalBytesPerWrite, procDeltaWrite/int64(opCount))
		t.Logf("[batch_size=%3d] RENAME:  %v (avg %v/op) | SQLite: %d B/op | Stream: %d B/op | Total Metadata: %d B/op (Proc I/O: %d B/op)",
			batchSize, renameDur, renameDur/time.Duration(opCount), sqliteBytesPerRename, streamBytesPerRename, totalBytesPerRename, procDeltaRename/int64(opCount))
		_ = rAttr
	}

	// 7. Read Latency During Write Bursts (Cache-Miss Point Lookups & Stats)
	t.Log("\n--- 7. Read Latency During Concurrent Write Bursts (Cache Misses) ---")
	for _, batchSize := range []int{1, 10, 100} {
		localDir := t.TempDir()
		vol := NewVolume("burst-read-bench", inmemorystorage.New(), NewEventBroadcaster(),
			WithMetadataIndex("sqlite"),
			WithMetadataCacheDisabled(true), // Ensure all reads are SQLite cache misses
			WithLocalStorageDir(localDir),
			WithApplierBatchSize(batchSize),
		)
		_ = vol.LoadFromBackend(ctx)

		// Pre-create 1,000 files for reading
		var readInos []uint64
		for i := 0; i < 1000; i++ {
			fAttr, _ := vol.CreateFile(ctx, 1, fmt.Sprintf("read_target_%d.txt", i), 0644, []byte("val"), 0, 0)
			readInos = append(readInos, fAttr.GetInode().GetIno())
		}
		_ = vol.FlushOverlay(ctx)

		// Start concurrent background writer
		var writerStop atomic.Bool
		var writerDone sync.WaitGroup
		writerDone.Add(1)
		go func() {
			defer writerDone.Done()
			var counter uint64
			for !writerStop.Load() {
				c := atomic.AddUint64(&counter, 1)
				name := fmt.Sprintf("burst_writer_%d.txt", c)
				_, _ = vol.CreateFile(ctx, 1, name, 0644, []byte("burst_data"), 0, 0)
				time.Sleep(50 * time.Microsecond)
			}
		}()

		// Measure 1,000 Stat latency durations
		statDurations := make([]time.Duration, 1000)
		for i := 0; i < 1000; i++ {
			start := time.Now()
			_, _ = vol.GetAttr(ctx, readInos[i])
			statDurations[i] = time.Since(start)
		}

		// Measure 1,000 Lookup latency durations
		lookupDurations := make([]time.Duration, 1000)
		for i := 0; i < 1000; i++ {
			start := time.Now()
			_, _ = vol.Lookup(ctx, 1, fmt.Sprintf("read_target_%d.txt", i))
			lookupDurations[i] = time.Since(start)
		}

		writerStop.Store(true)
		writerDone.Wait()
		_ = vol.FlushOverlay(ctx)
		_ = vol.Close()

		statP50, statP90, statP99, statMax := calculatePercentiles(statDurations)
		lookupP50, lookupP90, lookupP99, lookupMax := calculatePercentiles(lookupDurations)

		t.Logf("[batch_size=%3d] STAT   Cache-Miss Latency: p50=%v | p90=%v | p99=%v | max=%v",
			batchSize, statP50, statP90, statP99, statMax)
		t.Logf("[batch_size=%3d] LOOKUP Cache-Miss Latency: p50=%v | p90=%v | p99=%v | max=%v",
			batchSize, lookupP50, lookupP90, lookupP99, lookupMax)
	}

	// 8. Benchmark Gate: 10,000 Files End-to-End Latency
	t.Log("\n--- 8. Benchmark Gate: 10,000 Files End-to-End Latency (stat, create, readdir, rename, 64 KiB fsynced write) ---")
	for _, mode := range []string{"sqlite", "table", "memory"} {
		localDir := t.TempDir()
		vol := NewVolume("gate-10k-"+mode, inmemorystorage.New(), NewEventBroadcaster(),
			WithMetadataIndex(mode),
			WithLocalStorageDir(localDir),
		)
		_ = vol.LoadFromBackend(ctx)

		const defaultNumFiles = 10000
		numFiles := defaultNumFiles
		fsyncCount := 1000
		if os.Getenv("OBJECTFS_BENCH") == "" {
			numFiles = 200
			fsyncCount = 50
		}

		// A. Create files
		var createdInos []uint64
		startCreate := time.Now()
		for i := 0; i < numFiles; i++ {
			fAttr, err := vol.CreateFile(ctx, 1, fmt.Sprintf("gate_f%d.txt", i), 0644, []byte("x"), 0, 0)
			if err != nil {
				t.Fatalf("[%s] CreateFile %d failed: %v", mode, i, err)
			}
			createdInos = append(createdInos, fAttr.GetInode().GetIno())
		}
		createDur := time.Since(startCreate)

		// B. Stat files
		startStat := time.Now()
		for i := 0; i < numFiles; i++ {
			_, err := vol.GetAttr(ctx, createdInos[i])
			if err != nil {
				t.Fatalf("[%s] GetAttr %d failed: %v", mode, i, err)
			}
		}
		statDur := time.Since(startStat)

		// C. ReadDir entries
		startReadDir := time.Now()
		entries, err := vol.ReadDir(ctx, 1)
		if err != nil {
			t.Fatalf("[%s] ReadDir failed: %v", mode, err)
		}
		readDirDur := time.Since(startReadDir)

		// D. Rename files
		startRename := time.Now()
		for i := 0; i < numFiles; i++ {
			from := fmt.Sprintf("gate_f%d.txt", i)
			to := fmt.Sprintf("gate_renamed_%d.txt", i)
			_, err := vol.Rename(ctx, 1, from, 1, to)
			if err != nil {
				t.Fatalf("[%s] Rename %d failed: %v", mode, i, err)
			}
		}
		renameDur := time.Since(startRename)

		// E. 64 KiB fsynced writes
		data64K := make([]byte, 64*1024)
		fsyncTargetIno := createdInos[0]
		startFsync := time.Now()
		for i := 0; i < fsyncCount; i++ {
			_, _, _, _ = vol.WriteFile(ctx, fsyncTargetIno, 0, data64K, 0)
			_ = vol.Fsync(ctx, fsyncTargetIno)
		}
		fsyncDur := time.Since(startFsync)

		_ = vol.FlushOverlay(ctx)
		_ = vol.Close()

		t.Logf("[%s @ %d files] Create:   %v (%v/op)", mode, numFiles, createDur, createDur/time.Duration(numFiles))
		t.Logf("[%s @ %d files] Stat:     %v (%v/op)", mode, numFiles, statDur, statDur/time.Duration(numFiles))
		t.Logf("[%s @ %d files] ReadDir:  %v (entries=%d)", mode, numFiles, readDirDur, len(entries))
		t.Logf("[%s @ %d files] Rename:   %v (%v/op)", mode, numFiles, renameDur, renameDur/time.Duration(numFiles))
		t.Logf("[%s @ %d files] 64K Fsync: %v (%v/op)", mode, numFiles, fsyncDur, fsyncDur/time.Duration(fsyncCount))
	}

	// 9. Benchmark Gate: Scale & Memory (1,000,000 files across 10,000 dirs & 1,000,000-entry dir)
	t.Log("\n--- 9. Benchmark Gate: Controller Memory & Per-Operation Latency at Scale ---")
	if os.Getenv("OBJECTFS_BENCH") == "" {
		t.Log("Skipping 1,000,000 scale benchmarks in default mode (set OBJECTFS_BENCH=1 to run full scale benchmark gate)")
		t.Log("Running quick 10,000-file smoke scale test...")

		for _, mode := range []string{"sqlite", "table", "memory"} {
			localDir := t.TempDir()
			vol := NewVolume("gate-smoke-"+mode, inmemorystorage.New(), NewEventBroadcaster(),
				WithMetadataIndex(mode),
				WithLocalStorageDir(localDir),
			)
			_ = vol.LoadFromBackend(ctx)

			const smokeDirs = 100
			const smokeFilesPerDir = 100
			const smokeTotal = smokeDirs * smokeFilesPerDir

			var dirInos [smokeDirs]uint64
			for d := 0; d < smokeDirs; d++ {
				dAttr, _ := vol.Mkdir(ctx, 1, fmt.Sprintf("d_%d", d), 0755, 0, 0)
				dirInos[d] = dAttr.GetInode().GetIno()
			}
			for d := 0; d < smokeDirs; d++ {
				pIno := dirInos[d]
				for f := 0; f < smokeFilesPerDir; f++ {
					_, _ = vol.CreateFile(ctx, pIno, fmt.Sprintf("f_%d.txt", f), 0644, []byte("x"), 0, 0)
				}
			}
			_ = vol.FlushOverlay(ctx)
			_ = vol.Close()
			t.Logf("[%s @ smoke scale] Populated %d files across %d dirs successfully", mode, smokeTotal, smokeDirs)
		}
		t.Log("==================================================================")
		return
	}

	oldGC := debug.SetGCPercent(500)
	defer debug.SetGCPercent(oldGC)

	for _, mode := range []string{"sqlite", "memory"} {
		runtime.GC()
		var m1 runtime.MemStats
		runtime.ReadMemStats(&m1)

		localDir := t.TempDir()
		vol := NewVolume("gate-scale-"+mode, inmemorystorage.New(), NewEventBroadcaster(),
			WithMetadataIndex(mode),
			WithLocalStorageDir(localDir),
		)
		_ = vol.LoadFromBackend(ctx)

		// 1,000,000 files across 10,000 directories (100 files per dir)
		const numDirs = 10000
		const filesPerDir = 100
		const totalFiles = numDirs * filesPerDir

		t.Logf("[%s] Populating %d files across %d directories...", mode, totalFiles, numDirs)
		startPop := time.Now()

		var dirInos [numDirs]uint64
		for d := 0; d < numDirs; d++ {
			dAttr, _ := vol.Mkdir(ctx, 1, fmt.Sprintf("d_%d", d), 0755, 0, 0)
			dirInos[d] = dAttr.GetInode().GetIno()
		}

		for d := 0; d < numDirs; d++ {
			pIno := dirInos[d]
			for f := 0; f < filesPerDir; f++ {
				_, _ = vol.CreateFile(ctx, pIno, fmt.Sprintf("f_%d.txt", f), 0644, []byte("x"), 0, 0)
			}
			if (d+1)%200 == 0 {
				fmt.Printf("[%s] Populated %d/%d files across %d directories (elapsed: %v)\n", mode, (d+1)*filesPerDir, totalFiles, d+1, time.Since(startPop))
			}
		}
		popDur := time.Since(startPop)

		runtime.GC()
		var m2 runtime.MemStats
		runtime.ReadMemStats(&m2)
		heapAllocMB := float64(m2.HeapAlloc-m1.HeapAlloc) / (1024 * 1024)

		// Measure per-operation latency sample at 1,000,000 files
		startSample := time.Now()
		for i := 0; i < 5000; i++ {
			_, _ = vol.Lookup(ctx, dirInos[i%numDirs], fmt.Sprintf("f_%d.txt", i%filesPerDir))
		}
		lookupSampleDur := time.Since(startSample)

		startAdd := time.Now()
		for i := 0; i < 1000; i++ {
			_, _ = vol.CreateFile(ctx, dirInos[i%numDirs], fmt.Sprintf("extra_%d.txt", i), 0644, []byte("x"), 0, 0)
		}
		addSampleDur := time.Since(startAdd)

		_ = vol.FlushOverlay(ctx)
		_ = vol.Close()

		t.Logf("[%s @ 1M files across 10k dirs] Populate 1M files: %v (avg %v/op) | HeapAlloc: %.2f MB",
			mode, popDur, popDur/totalFiles, heapAllocMB)
		t.Logf("[%s @ 1M files across 10k dirs] Sample Lookup 5k:   %v (%v/op)",
			mode, lookupSampleDur, lookupSampleDur/5000)
		t.Logf("[%s @ 1M files across 10k dirs] Sample AddFile 1k:   %v (%v/op)",
			mode, addSampleDur, addSampleDur/1000)
	}

	// 1,000,000-entry single directory test
	for _, mode := range []string{"sqlite", "memory"} {
		runtime.GC()
		var m1 runtime.MemStats
		runtime.ReadMemStats(&m1)

		localDir := t.TempDir()
		vol := NewVolume("gate-1m-single-dir-"+mode, inmemorystorage.New(), NewEventBroadcaster(),
			WithMetadataIndex(mode),
			WithLocalStorageDir(localDir),
		)
		_ = vol.LoadFromBackend(ctx)

		const singleDirCount = 1000000
		t.Logf("[%s] Populating 1,000,000 entries into single directory...", mode)
		startSingleDir := time.Now()
		for i := 0; i < singleDirCount; i++ {
			_, _ = vol.CreateFile(ctx, 1, fmt.Sprintf("file_%d.txt", i), 0644, []byte("x"), 0, 0)
			if (i+1)%20000 == 0 {
				fmt.Printf("[%s] Populated %d/%d single-dir entries (elapsed: %v)\n", mode, i+1, singleDirCount, time.Since(startSingleDir))
			}
		}
		singleDirPopDur := time.Since(startSingleDir)

		runtime.GC()
		var m2 runtime.MemStats
		runtime.ReadMemStats(&m2)
		singleDirHeapMB := float64(m2.HeapAlloc-m1.HeapAlloc) / (1024 * 1024)

		// Adding to 1,000,000-entry directory: measure latency
		startAdd := time.Now()
		for i := 0; i < 1000; i++ {
			_, _ = vol.CreateFile(ctx, 1, fmt.Sprintf("extra_one_million_%d.txt", i), 0644, []byte("x"), 0, 0)
		}
		addOneMillionDur := time.Since(startAdd)

		_ = vol.FlushOverlay(ctx)
		_ = vol.Close()

		t.Logf("[%s @ 1M-entry dir] Populate 1M dir: %v (avg %v/op) | HeapAlloc: %.2f MB",
			mode, singleDirPopDur, singleDirPopDur/singleDirCount, singleDirHeapMB)
		t.Logf("[%s @ 1M-entry dir] Add to 1M dir 1k: %v (%v/op)",
			mode, addOneMillionDur, addOneMillionDur/1000)
	}

	t.Log("==================================================================")
}

func init() {
	_ = walclient.Local
}
