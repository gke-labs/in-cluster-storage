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
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore/inmemorystorage"
	"github.com/gke-labs/in-cluster-storage/pkg/sds"
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
	"google.golang.org/protobuf/proto"
	_ "modernc.org/sqlite"
)

func TestVolume_StatsTracking(t *testing.T) {
	ctx := t.Context()
	storage := inmemorystorage.New()
	server := NewServer(storage)

	volumeID := "test-stats-vol"
	vol, err := server.getOrCreateVolume(volumeID)
	if err != nil {
		t.Fatalf("getOrCreateVolume failed: %v", err)
	}

	st := vol.Stats()
	if st.Stats.GetInodesDir() != 1 {
		t.Errorf("initial stats InodesDir: got %d, want 1", st.Stats.GetInodesDir())
	}

	// Create file1 (100 bytes)
	f1Resp, err := server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:    volumeID,
		ParentInode: 1,
		Name:        "file1.txt",
		Mode:        0644,
	})
	if err != nil || f1Resp.GetError() != 0 {
		t.Fatalf("CreateFile file1 failed: err=%v, resp=%v", err, f1Resp)
	}
	w1Resp, err := server.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId: volumeID,
		Inode:    f1Resp.GetAttr().GetInode().GetIno(),
		Offset:   0,
		Data:     make([]byte, 100),
	})
	if err != nil || w1Resp.GetError() != 0 {
		t.Fatalf("WriteFile file1 failed: err=%v, resp=%v", err, w1Resp)
	}
	rel1Resp, err := server.Release(ctx, &pb.ReleaseRequest{
		VolumeId: volumeID,
		Inode:    f1Resp.GetAttr().GetInode().GetIno(),
		Fh:       f1Resp.GetFh(),
	})
	if err != nil || rel1Resp.GetError() != 0 {
		t.Fatalf("Release file1 failed: err=%v, resp=%v", err, rel1Resp)
	}

	// Create file2 (350 bytes)
	f2Resp, err := server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:    volumeID,
		ParentInode: 1,
		Name:        "file2.txt",
		Mode:        0644,
	})
	if err != nil || f2Resp.GetError() != 0 {
		t.Fatalf("CreateFile file2 failed: err=%v, resp=%v", err, f2Resp)
	}
	w2Resp, err := server.WriteFile(ctx, &pb.WriteFileRequest{
		VolumeId: volumeID,
		Inode:    f2Resp.GetAttr().GetInode().GetIno(),
		Offset:   0,
		Data:     make([]byte, 350),
	})
	if err != nil || w2Resp.GetError() != 0 {
		t.Fatalf("WriteFile file2 failed: err=%v, resp=%v", err, w2Resp)
	}
	rel2Resp, err := server.Release(ctx, &pb.ReleaseRequest{
		VolumeId: volumeID,
		Inode:    f2Resp.GetAttr().GetInode().GetIno(),
		Fh:       f2Resp.GetFh(),
	})
	if err != nil || rel2Resp.GetError() != 0 {
		t.Fatalf("Release file2 failed: err=%v, resp=%v", err, rel2Resp)
	}

	// Create directory
	d1Resp, err := server.Mkdir(ctx, &pb.MkdirRequest{
		VolumeId:    volumeID,
		ParentInode: 1,
		Name:        "subdir",
		Mode:        0755,
	})
	if err != nil || d1Resp.GetError() != 0 {
		t.Fatalf("Mkdir failed: err=%v, resp=%v", err, d1Resp)
	}

	// Create symlink
	symResp, err := server.Symlink(ctx, &pb.SymlinkRequest{
		VolumeId:    volumeID,
		ParentInode: 1,
		Name:        "link_to_f1",
		Target:      "file1.txt",
	})
	if err != nil || symResp.GetError() != 0 {
		t.Fatalf("Symlink failed: err=%v, resp=%v", err, symResp)
	}

	st = vol.Stats()
	if st.Stats.GetInodesDir() != 2 || st.Stats.GetInodesFile() != 2 || st.Stats.GetInodesSymlink() != 1 {
		t.Errorf("inodes counts mismatch: %v", st.Stats)
	}
	if st.Stats.GetLogicalBytes() != 450 {
		t.Errorf("LogicalBytes: got %d, want 450", st.Stats.GetLogicalBytes())
	}
	if st.Stats.GetMaxIno() < f2Resp.GetAttr().GetInode().GetIno() {
		t.Errorf("MaxIno %d is smaller than file2 inode %d", st.Stats.GetMaxIno(), f2Resp.GetAttr().GetInode().GetIno())
	}

	// RPC GetVolumeStats
	rpcResp, err := server.GetVolumeStats(ctx, &pb.GetVolumeStatsRequest{VolumeId: volumeID})
	if err != nil {
		t.Fatalf("GetVolumeStats RPC failed: %v", err)
	}
	if rpcResp.GetUsedBytes() != 450 {
		t.Errorf("RPC UsedBytes: got %d, want 450", rpcResp.GetUsedBytes())
	}
	if rpcResp.GetUsedInodes() != 5 {
		t.Errorf("RPC UsedInodes: got %d, want 5", rpcResp.GetUsedInodes())
	}
	if rpcResp.GetStats().GetInodesFile() != 2 {
		t.Errorf("RPC InodesFile: got %d, want 2", rpcResp.GetStats().GetInodesFile())
	}

	// Delete file1
	unResp, err := server.Unlink(ctx, &pb.UnlinkRequest{
		VolumeId:    volumeID,
		ParentInode: 1,
		Name:        "file1.txt",
	})
	if err != nil || unResp.GetError() != 0 {
		t.Fatalf("Unlink file1 failed: err=%v, resp=%v", err, unResp)
	}

	st = vol.Stats()
	if st.Stats.GetInodesFile() != 1 || st.Stats.GetLogicalBytes() != 350 {
		t.Errorf("stats after delete: %v", st.Stats)
	}

	// Finally, flush to backend and verify debug check live == durable passes
	if err := vol.FlushToBackend(ctx); err != nil {
		t.Fatalf("FlushToBackend at end failed: %v", err)
	}
}

func TestVolume_InodeAllocatorNoReuseAcrossRestart(t *testing.T) {
	ctx := t.Context()
	storage := inmemorystorage.New()
	dir := t.TempDir()

	volumeID := "test-allocator-vol"
	var createdInodes []uint64

	// Session 1: Create several files
	{
		server1 := NewServer(storage, WithServerLocalStorageDir(filepath.Join(dir, "session1")))
		vol1, err := server1.getOrCreateVolume(volumeID)
		if err != nil {
			t.Fatalf("getOrCreateVolume failed: %v", err)
		}

		for i := 0; i < 5; i++ {
			resp, err := server1.CreateFile(ctx, &pb.CreateFileRequest{
				VolumeId:    volumeID,
				ParentInode: 1,
				Name:        filepath.Join("", string(rune('a'+i))),
				Mode:        0644,
			})
			if err != nil || resp.GetError() != 0 {
				t.Fatalf("CreateFile %d failed: err=%v, resp=%v", i, err, resp)
			}
			createdInodes = append(createdInodes, resp.GetAttr().GetInode().GetIno())
			relResp, err := server1.Release(ctx, &pb.ReleaseRequest{
				VolumeId: volumeID,
				Inode:    resp.GetAttr().GetInode().GetIno(),
				Fh:       resp.GetFh(),
			})
			if err != nil || relResp.GetError() != 0 {
				t.Fatalf("Release %d failed: err=%v, resp=%v", i, err, relResp)
			}
		}
		if err := vol1.FlushToBackend(ctx); err != nil {
			t.Fatalf("FlushToBackend failed: %v", err)
		}
	}

	// Session 2: Reload volume from backend
	{
		server2 := NewServer(storage, WithServerLocalStorageDir(filepath.Join(dir, "session2")))
		vol2, err := server2.getOrCreateVolume(volumeID)
		if err != nil {
			t.Fatalf("getOrCreateVolume failed: %v", err)
		}
		if err := vol2.LoadFromBackend(ctx); err != nil {
			t.Fatalf("LoadFromBackend failed: %v", err)
		}

		for i := 0; i < 5; i++ {
			resp, err := server2.CreateFile(ctx, &pb.CreateFileRequest{
				VolumeId:    volumeID,
				ParentInode: 1,
				Name:        filepath.Join("", string(rune('z'-i))),
				Mode:        0644,
			})
			if err != nil || resp.GetError() != 0 {
				t.Fatalf("CreateFile session2 %d failed: err=%v, resp=%v", i, err, resp)
			}
			newIno := resp.GetAttr().GetInode().GetIno()
			for _, oldIno := range createdInodes {
				if newIno == oldIno {
					t.Fatalf("duplicate inode allocated across restart: %d (previously allocated: %v)", newIno, createdInodes)
				}
			}
		}
	}
}

func TestVolume_InodeAllocatorRecomputeWhenStatsDeleted(t *testing.T) {
	ctx := t.Context()
	storage := inmemorystorage.New()
	dir := t.TempDir()
	localDir := filepath.Join(dir, "local")

	volumeID := "test-deleted-stats-vol"

	// Session 1: create 5 files (inodes 8, 16, 24, 32, 40)
	var createdInodes []uint64
	{
		server1 := NewServer(storage,
			WithServerMetadataIndex("sqlite"),
			WithServerLocalStorageDir(localDir),
		)
		vol1, err := server1.getOrCreateVolume(volumeID)
		if err != nil {
			t.Fatalf("getOrCreateVolume failed: %v", err)
		}

		for i := 0; i < 5; i++ {
			resp, err := server1.CreateFile(ctx, &pb.CreateFileRequest{
				VolumeId:    volumeID,
				ParentInode: 1,
				Name:        filepath.Join("", string(rune('a'+i))),
				Mode:        0644,
			})
			if err != nil || resp.GetError() != 0 {
				t.Fatalf("CreateFile %d failed: err=%v, resp=%v", i, err, resp)
			}
			createdInodes = append(createdInodes, resp.GetAttr().GetInode().GetIno())
			relResp, err := server1.Release(ctx, &pb.ReleaseRequest{
				VolumeId: volumeID,
				Inode:    resp.GetAttr().GetInode().GetIno(),
				Fh:       resp.GetFh(),
			})
			if err != nil || relResp.GetError() != 0 {
				t.Fatalf("Release %d failed: err=%v, resp=%v", i, err, relResp)
			}
		}
		if err := vol1.FlushToBackend(ctx); err != nil {
			t.Fatalf("FlushToBackend failed: %v", err)
		}
		if err := server1.Close(); err != nil {
			t.Fatalf("server1.Close failed: %v", err)
		}
	}

	// Delete VolumeStats row from SQLite to simulate pre-existing index without stats
	dbFiles, err := filepath.Glob(filepath.Join(localDir, "*.sqlite"))
	if err != nil || len(dbFiles) == 0 {
		t.Fatalf("no sqlite file found in %s: %v", localDir, err)
	}
	db, err := sql.Open("sqlite", dbFiles[0])
	if err != nil {
		t.Fatalf("sql.Open failed: %v", err)
	}
	if _, err := db.Exec("DELETE FROM objectfs_v1alpha1_VolumeStats;"); err != nil {
		t.Fatalf("db.Exec DELETE failed: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("db.Close failed: %v", err)
	}

	// Session 2: reopen volume with same local directory
	{
		server2 := NewServer(storage,
			WithServerMetadataIndex("sqlite"),
			WithServerLocalStorageDir(localDir),
		)
		vol2, err := server2.getOrCreateVolume(volumeID)
		if err != nil {
			t.Fatalf("getOrCreateVolume run 2 failed: %v", err)
		}
		defer func() {
			if err := server2.Close(); err != nil {
				t.Errorf("server2.Close failed: %v", err)
			}
		}()

		st := vol2.Stats()
		if st.Stats.GetMaxIno() != 40 {
			t.Errorf("MaxIno after recomputing stats from scan: got %d, want 40", st.Stats.GetMaxIno())
		}

		// Next CreateFile must not reuse any existing inode (allocates 48)
		resp, err := server2.CreateFile(ctx, &pb.CreateFileRequest{
			VolumeId:    volumeID,
			ParentInode: 1,
			Name:        "new_file.txt",
			Mode:        0644,
		})
		if err != nil || resp.GetError() != 0 {
			t.Fatalf("CreateFile run 2 failed: err=%v, resp=%v", err, resp)
		}
		newIno := resp.GetAttr().GetInode().GetIno()
		for _, oldIno := range createdInodes {
			if newIno == oldIno {
				t.Fatalf("reused existing inode %d after stats row deletion! (existing: %v)", newIno, createdInodes)
			}
		}

		st = vol2.Stats()
		if st.Stats.GetMaxIno() != newIno {
			t.Errorf("MaxIno immediate: got %d, want %d", st.Stats.GetMaxIno(), newIno)
		}
		if err := vol2.FlushToBackend(ctx); err != nil {
			t.Fatalf("FlushToBackend run 2 failed: %v", err)
		}
	}
}

func TestVolume_CrashWithUnappliedBacklogAndRecovery(t *testing.T) {
	ctx := t.Context()
	storage := inmemorystorage.New()
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")

	volumeID := "test-crash-backlog-vol"
	var unappliedInodes []uint64

	// Session 1: Create files remaining in overlay
	{
		server1 := NewServer(storage,
			WithServerWAL(walDir, "", walclient.Local),
			WithServerLocalStorageDir(filepath.Join(dir, "session1")),
			WithServerMetadataApplierBatchSize(1000),
		)
		vol1, err := server1.getOrCreateVolume(volumeID)
		if err != nil {
			t.Fatalf("getOrCreateVolume failed: %v", err)
		}

		if _, err := vol1.CreateSnapshot(ctx, ""); err != nil {
			t.Fatalf("CreateSnapshot failed: %v", err)
		}

		for i := 0; i < 5; i++ {
			resp, err := server1.CreateFile(ctx, &pb.CreateFileRequest{
				VolumeId:    volumeID,
				ParentInode: 1,
				Name:        filepath.Join("", string(rune('a'+i))),
				Mode:        0644,
			})
			if err != nil || resp.GetError() != 0 {
				t.Fatalf("CreateFile %d failed: err=%v, resp=%v", i, err, resp)
			}
			unappliedInodes = append(unappliedInodes, resp.GetAttr().GetInode().GetIno())
			relResp, err := server1.Release(ctx, &pb.ReleaseRequest{
				VolumeId: volumeID,
				Inode:    resp.GetAttr().GetInode().GetIno(),
				Fh:       resp.GetFh(),
			})
			if err != nil || relResp.GetError() != 0 {
				t.Fatalf("Release %d failed: err=%v, resp=%v", i, err, relResp)
			}
		}
		if err := server1.Close(); err != nil {
			t.Fatalf("server1.Close failed: %v", err)
		}
	}

	// Session 2: Recover from backend + WAL stream
	{
		server2 := NewServer(storage,
			WithServerWAL(walDir, "", walclient.Local),
			WithServerLocalStorageDir(filepath.Join(dir, "session2")),
		)
		_, err := server2.getOrCreateVolume(volumeID)
		if err != nil {
			t.Fatalf("getOrCreateVolume failed: %v", err)
		}
		defer func() {
			if err := server2.Close(); err != nil {
				t.Errorf("server2.Close failed: %v", err)
			}
		}()

		for i := 0; i < 5; i++ {
			resp, err := server2.CreateFile(ctx, &pb.CreateFileRequest{
				VolumeId:    volumeID,
				ParentInode: 1,
				Name:        filepath.Join("", string(rune('z'-i))),
				Mode:        0644,
			})
			if err != nil || resp.GetError() != 0 {
				t.Fatalf("CreateFile session2 %d failed: err=%v, resp=%v", i, err, resp)
			}
			newIno := resp.GetAttr().GetInode().GetIno()
			for _, oldIno := range unappliedInodes {
				if newIno == oldIno {
					t.Fatalf("duplicate inode allocated after crash recovery with unapplied backlog: %d (unapplied backlog had: %v)", newIno, unappliedInodes)
				}
			}
		}
	}
}

func TestVolume_LiveStatsMatchesFullRecountAfterRecovery(t *testing.T) {
	ctx := t.Context()
	storage := inmemorystorage.New()
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")

	volumeID := "test-recovery-stats-vol"

	// Session 1: Create files with unapplied backlog (large batch size)
	session1Dir := filepath.Join(dir, "session1")
	{
		server1 := NewServer(storage,
			WithServerWAL(walDir, "", walclient.Local),
			WithServerLocalStorageDir(session1Dir),
			WithServerMetadataApplierBatchSize(1000),
		)
		vol1, err := server1.getOrCreateVolume(volumeID)
		if err != nil {
			t.Fatalf("getOrCreateVolume failed: %v", err)
		}

		// 1. Create directory
		_, err = server1.Mkdir(ctx, &pb.MkdirRequest{
			VolumeId:    volumeID,
			ParentInode: 1,
			Name:        "subdir",
			Mode:        0755,
		})
		if err != nil {
			t.Fatalf("Mkdir failed: %v", err)
		}

		// 2. Create small file
		f1Resp, err := server1.CreateFile(ctx, &pb.CreateFileRequest{
			VolumeId:    volumeID,
			ParentInode: 1,
			Name:        "small.txt",
			Mode:        0644,
		})
		if err != nil || f1Resp.GetError() != 0 {
			t.Fatalf("CreateFile small.txt failed: %v", err)
		}
		_, err = server1.WriteFile(ctx, &pb.WriteFileRequest{
			VolumeId: volumeID,
			Inode:    f1Resp.GetAttr().GetInode().GetIno(),
			Offset:   0,
			Data:     []byte("hello world"),
		})
		if err != nil {
			t.Fatalf("WriteFile small.txt failed: %v", err)
		}

		// 3. Create chunked file (>64KB)
		f2Resp, err := server1.CreateFile(ctx, &pb.CreateFileRequest{
			VolumeId:    volumeID,
			ParentInode: 1,
			Name:        "large.bin",
			Mode:        0644,
		})
		if err != nil || f2Resp.GetError() != 0 {
			t.Fatalf("CreateFile large.bin failed: %v", err)
		}
		chunkedData := make([]byte, 128*1024)
		for i := range chunkedData {
			chunkedData[i] = byte(i % 251)
		}
		_, err = server1.WriteFile(ctx, &pb.WriteFileRequest{
			VolumeId: volumeID,
			Inode:    f2Resp.GetAttr().GetInode().GetIno(),
			Offset:   0,
			Data:     chunkedData,
		})
		if err != nil {
			t.Fatalf("WriteFile large.bin failed: %v", err)
		}

		// 4. Create symlink
		_, err = server1.Symlink(ctx, &pb.SymlinkRequest{
			VolumeId:    volumeID,
			ParentInode: 1,
			Name:        "link_to_small",
			Target:      "small.txt",
		})
		if err != nil {
			t.Fatalf("Symlink failed: %v", err)
		}

		// Verify live stats reflect the write path immediately
		live1 := vol1.Stats().Stats
		if live1.GetInodesDir() != 2 || live1.GetInodesFile() != 2 || live1.GetInodesSymlink() != 1 {
			t.Fatalf("unexpected live inode counts: %+v", live1)
		}
		if live1.GetLogicalBytes() != int64(11+len(chunkedData)) {
			t.Fatalf("unexpected live logical bytes: %d", live1.GetLogicalBytes())
		}

		// Simulate crash: close server1 stream, without flushing applier to SQLite
		_ = server1.Close()
	}

	// Session 2: Crash restart with SAME local directory
	{
		server2 := NewServer(storage,
			WithServerWAL(walDir, "", walclient.Local),
			WithServerLocalStorageDir(session1Dir),
		)
		vol2, err := server2.getOrCreateVolume(volumeID)
		if err != nil {
			t.Fatalf("getOrCreateVolume session 2 failed: %v", err)
		}

		live2 := vol2.Stats().Stats
		recount, err := vol2.countStatsFromIndex(ctx, vol2.metadataView.Index())
		if err != nil {
			t.Fatalf("countStatsFromIndex failed: %v", err)
		}

		if !proto.Equal(live2, recount) {
			t.Fatalf("live stats after crash recovery does not match recount:\n  live:    %+v\n  recount: %+v", live2, recount)
		}
		_ = server2.Close()
	}

	// Session 3: Node move with FRESH local storage directory in WAL mode
	session3Dir := filepath.Join(dir, "session3-fresh")
	{
		server3 := NewServer(storage,
			WithServerWAL(walDir, "", walclient.Local),
			WithServerLocalStorageDir(session3Dir),
		)
		vol3, err := server3.getOrCreateVolume(volumeID)
		if err != nil {
			t.Fatalf("getOrCreateVolume session 3 failed: %v", err)
		}

		live3 := vol3.Stats().Stats
		recount, err := vol3.countStatsFromIndex(ctx, vol3.metadataView.Index())
		if err != nil {
			t.Fatalf("countStatsFromIndex session 3 failed: %v", err)
		}

		if !proto.Equal(live3, recount) {
			t.Fatalf("live stats after node move does not match recount:\n  live:    %+v\n  recount: %+v", live3, recount)
		}
		_ = server3.Close()
	}
}

func TestVolume_DebugStatsCheckCatchesSkippedUpdate(t *testing.T) {
	ctx := t.Context()
	storage := inmemorystorage.New()
	server := NewServer(storage, WithServerStatsCheck())
	defer server.Close()

	vol, err := server.getOrCreateVolume("test-debug-check-vol")
	if err != nil {
		t.Fatalf("getOrCreateVolume failed: %v", err)
	}

	// Create a file
	_, err = server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:    "test-debug-check-vol",
		ParentInode: 1,
		Name:        "file.txt",
		Mode:        0644,
	})
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}

	// Drain everything cleanly first
	if err := vol.FlushOverlay(ctx); err != nil {
		t.Fatalf("FlushOverlay failed: %v", err)
	}

	// Deliberately tamper with live stats to simulate a skipped update
	vol.mu.Lock()
	vol.liveStats.InodesFile += 10
	vol.mu.Unlock()

	defer func() {
		r := recover()
		vol.mu.Lock()
		vol.liveStats.InodesFile -= 10
		vol.mu.Unlock()
		if r == nil {
			t.Fatal("expected panic from debug stats check divergence, but did not panic")
		}
		msg, ok := r.(string)
		if !ok || !strings.Contains(msg, "volume stats divergence") {
			t.Fatalf("unexpected panic message: %v", r)
		}
	}()

	// FlushOverlay drains the backlog and triggers assertStatsMatchLocked on the test goroutine.
	if err := vol.FlushOverlay(ctx); err != nil {
		t.Fatalf("FlushOverlay failed: %v", err)
	}
}

type faultyGetIndex struct {
	sds.LocalIndex
	getCalls   atomic.Int64
	failOnCall atomic.Int64
}

func (f *faultyGetIndex) Get(ctx context.Context, typeName string, key sds.Key) (proto.Message, bool, error) {
	call := f.getCalls.Add(1)
	if target := f.failOnCall.Load(); target > 0 && call == target {
		return nil, false, errors.New("injected index Get failure")
	}
	return f.LocalIndex.Get(ctx, typeName, key)
}

type faultyGetFactory struct {
	sds.IndexFactory
	idx *faultyGetIndex
}

func (f *faultyGetFactory) NewEmpty(ctx context.Context, streamID string, dir string) (sds.LocalIndex, error) {
	inner, err := f.IndexFactory.NewEmpty(ctx, streamID, dir)
	if err != nil {
		return nil, err
	}
	f.idx.LocalIndex = inner
	return f.idx, nil
}

func (f *faultyGetFactory) OpenLocal(ctx context.Context, streamID string, dir string) (sds.LocalIndex, bool, error) {
	inner, found, err := f.IndexFactory.OpenLocal(ctx, streamID, dir)
	if err != nil || !found {
		return nil, found, err
	}
	f.idx.LocalIndex = inner
	return f.idx, true, nil
}

func TestVolume_CommitStepInfallibleFaultInjectedIndexGetCannotAffectCommit(t *testing.T) {
	ctx := t.Context()
	backend := inmemorystorage.New()

	baseFactory, err := sds.GetIndexFactory("memory")
	if err != nil {
		t.Fatalf("GetIndexFactory failed: %v", err)
	}

	faultyIdx := &faultyGetIndex{}
	factory := &faultyGetFactory{IndexFactory: baseFactory, idx: faultyIdx}

	vol := NewVolume("vol-faulty-get-commit", backend, NewEventBroadcaster(),
		WithIndexFactory(factory),
		WithMetadataCacheDisabled(true),
		WithVolumeStatsCheck(),
	)
	defer vol.Close()

	if err := vol.LoadFromBackend(ctx); err != nil {
		t.Fatalf("LoadFromBackend failed: %v", err)
	}

	attr, err := vol.CreateFile(ctx, 1, "test.txt", 0644, []byte("hello"), 1000, 1000)
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}
	ino := attr.GetInode().GetIno()

	if err := vol.FlushOverlay(ctx); err != nil {
		t.Fatalf("FlushOverlay failed: %v", err)
	}

	// Case 1: Injected fault during read phase fails operation BEFORE stream append.
	currentCalls := faultyIdx.getCalls.Load()
	faultyIdx.failOnCall.Store(currentCalls + 1) // next Get is SetAttr's read phase

	newMode := uint32(0755)
	_, err = vol.SetAttr(ctx, ino, &newMode, nil, nil, nil, false, nil, false, nil, false)
	if err == nil {
		t.Fatal("expected SetAttr to fail during read phase when index Get fails, got nil")
	}

	// Case 2: Fault injected on call currentCalls + 2 (which in #198 was commit-time lookup).
	// Because commit step takes before-images from the read set, zero index Get calls
	// happen during commit. SetAttr must succeed without encountering any index Get.
	currentCalls = faultyIdx.getCalls.Load()
	faultyIdx.failOnCall.Store(currentCalls + 2) // Will not be reached during SetAttr!

	_, err = vol.SetAttr(ctx, ino, &newMode, nil, nil, nil, false, nil, false, nil, false)
	if err != nil {
		t.Fatalf("expected SetAttr to succeed because commit performs zero index Get lookups, got: %v", err)
	}

	// Verify the commit step was infallible and live stats match durable on flush
	if err := vol.FlushOverlay(ctx); err != nil {
		t.Fatalf("FlushOverlay failed: %v", err)
	}
}

func TestVolume_WritingKeyNotReadPanics(t *testing.T) {
	ctx := t.Context()
	backend := inmemorystorage.New()
	vol := NewVolume("vol-not-read-panic", backend, NewEventBroadcaster(),
		WithMetadataIndex("memory"),
	)
	defer vol.Close()
	if err := vol.LoadFromBackend(ctx); err != nil {
		t.Fatalf("LoadFromBackend failed: %v", err)
	}

	vol.mu.Lock()
	defer vol.mu.Unlock()

	// Deliberately bypass any read helper: begin a tx and write an unread inode.
	tx := vol.metadataStream.Begin()
	_, err := tx.Update(ctx, &pb.Inode{
		Ino:   proto.Uint64(99999),
		Mode:  0644,
		Nlink: 1,
	})
	if err != nil {
		t.Fatalf("tx.Update failed: %v", err)
	}
	_, err = tx.Commit(ctx)
	if err != nil {
		t.Fatalf("tx.Commit failed: %v", err)
	}

	didPanic := false
	func() {
		defer func() {
			if r := recover(); r != nil {
				didPanic = true
				msg, ok := r.(string)
				if !ok || !strings.Contains(msg, "key written but never read") {
					t.Fatalf("unexpected panic message: %v", r)
				}
			}
		}()
		vol.applyTxChangesLocked(ctx, tx)
	}()

	if !didPanic {
		t.Fatal("expected panic on writing key that was never read, but did not panic")
	}
}

func TestVolume_ReadsBeforeWritesAssertionPanicsInAllModes(t *testing.T) {
	ctx := t.Context()
	backend := inmemorystorage.New()
	vol := NewVolume("vol-reads-before-writes-panic", backend, NewEventBroadcaster(),
		WithMetadataIndex("memory"),
		WithoutVolumeMutationCheck(),
		WithoutVolumeStatsCheck(),
	)
	defer vol.Close()
	if err := vol.LoadFromBackend(ctx); err != nil {
		t.Fatalf("LoadFromBackend failed: %v", err)
	}

	vol.mu.Lock()
	defer vol.mu.Unlock()

	tx := vol.beginTxLocked("TestOp")

	// Buffer a write first
	key, err := pkInode.Extract(&pb.Inode{Ino: proto.Uint64(1234)})
	if err != nil {
		t.Fatalf("pkInode.Extract failed: %v", err)
	}
	vol.recordReadLocked(tx, "objectfs.v1alpha1.Inode", key, nil)
	if _, err := tx.Insert(ctx, &pb.Inode{Ino: proto.Uint64(1234), Mode: 0644}); err != nil {
		t.Fatalf("tx.Insert failed: %v", err)
	}

	// Verify that a read with tx == nil does NOT enforce the assertion and does NOT panic
	if _, _, err := vol.getSQLiteRowLocked(ctx, nil, "objectfs.v1alpha1.Inode", key); err != nil {
		t.Fatalf("getSQLiteRowLocked with nil tx failed: %v", err)
	}
	if _, err := vol.scanLimitSQLiteRowsLocked(ctx, nil, "objectfs.v1alpha1.Inode", nil, 1); err != nil {
		t.Fatalf("scanLimitSQLiteRowsLocked with nil tx failed: %v", err)
	}

	// Now attempt a read after write has been buffered
	didPanic := false
	func() {
		defer func() {
			if r := recover(); r != nil {
				didPanic = true
				msg, ok := r.(string)
				if !ok || !strings.Contains(msg, "read") || !strings.Contains(msg, "after transaction buffered write") {
					t.Fatalf("unexpected panic message: %v", r)
				}
			}
		}()
		_, _, _ = vol.getSQLiteRowLocked(ctx, tx, "objectfs.v1alpha1.Inode", key)
	}()

	if !didPanic {
		t.Fatal("expected panic on reading after transaction buffered write in all modes, but did not panic")
	}

	// Also verify that scanLimitSQLiteRowsLocked enforces the reads-before-writes assertion
	didPanicLimit := false
	func() {
		defer func() {
			if r := recover(); r != nil {
				didPanicLimit = true
				msg, ok := r.(string)
				if !ok || !strings.Contains(msg, "read") || !strings.Contains(msg, "after transaction buffered write") {
					t.Fatalf("unexpected panic message: %v", r)
				}
			}
		}()
		_, _ = vol.scanLimitSQLiteRowsLocked(ctx, tx, "objectfs.v1alpha1.Inode", nil, 1)
	}()

	if !didPanicLimit {
		t.Fatal("expected panic on scanLimitSQLiteRowsLocked after transaction buffered write in all modes, but did not panic")
	}
}
