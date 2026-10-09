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
	"encoding/hex"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"testing"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/blob"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore/inmemorystorage"
	"github.com/gke-labs/in-cluster-storage/pkg/sds"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/projection/table"
	"google.golang.org/protobuf/proto"
)

var regenerateVectors = flag.Bool("regenerate", false, "regenerate golden snapshot vectors")

type SnapshotVector struct {
	Name        string   `json:"name"`
	Description string   `json:"description"`
	StreamID    string   `json:"stream_id"`
	Position    uint64   `json:"position"`
	TableNames  []string `json:"table_names"`
	RowCount    int      `json:"row_count"`
	SSTableHex  string   `json:"sstable_hex"`
}

func TestSnapshotGoldenVectors(t *testing.T) {
	ctx := t.Context()
	vectorDir := filepath.Join("testdata", "vectors")

	if *regenerateVectors {
		if err := os.MkdirAll(vectorDir, 0755); err != nil {
			t.Fatalf("MkdirAll failed: %v", err)
		}
		generateGoldenVector(t, ctx, vectorDir)
	}

	files, err := filepath.Glob(filepath.Join(vectorDir, "*.json"))
	if err != nil {
		t.Fatalf("Glob failed: %v", err)
	}
	if len(files) == 0 {
		t.Fatalf("No snapshot golden vectors found in %s; run with -regenerate", vectorDir)
	}

	for _, file := range files {
		t.Run(filepath.Base(file), func(t *testing.T) {
			data, err := os.ReadFile(file)
			if err != nil {
				t.Fatalf("ReadFile failed: %v", err)
			}

			var vec SnapshotVector
			if err := json.Unmarshal(data, &vec); err != nil {
				t.Fatalf("Unmarshal failed: %v", err)
			}

			sstBytes, err := hex.DecodeString(vec.SSTableHex)
			if err != nil {
				t.Fatalf("DecodeString failed: %v", err)
			}

			// 1. Read via standalone SnapshotReader
			sr, err := table.OpenSnapshotBytes(sstBytes)
			if err != nil {
				t.Fatalf("OpenSnapshotBytes failed: %v", err)
			}
			defer sr.Close()

			if sr.Position() != vec.Position {
				t.Fatalf("sr.Position = %d, want %d", sr.Position(), vec.Position)
			}
			if sr.StreamID() != vec.StreamID {
				t.Fatalf("sr.StreamID = %q, want %q", sr.StreamID(), vec.StreamID)
			}

			pk := sds.NewPrimaryKey(1)
			for _, tblName := range vec.TableNames {
				rows, err := sr.ScanSlice(tblName, nil)
				if err != nil {
					t.Fatalf("sr.ScanSlice(%q) failed: %v", tblName, err)
				}
				if len(rows) != vec.RowCount {
					t.Fatalf("table %q row count = %d, want %d", tblName, len(rows), vec.RowCount)
				}

				// Verify Point Get for each row
				for _, r := range rows {
					node := r.(*pb.Inode)
					k, _, _ := pk.Split(node)
					got, found, err := sr.Get(tblName, k)
					if err != nil || !found {
						t.Fatalf("sr.Get row failed: found=%v, err=%v", found, err)
					}
					if got.(*pb.Inode).GetIno() != node.GetIno() {
						t.Fatalf("got ino %d, want %d", got.(*pb.Inode).GetIno(), node.GetIno())
					}
				}
			}

			// 2. Restore via Ingest into a new DB
			backend := inmemorystorage.New()
			snapKey := table.SnapshotKey(vec.StreamID, vec.Position)
			_, err = backend.PutObject(ctx, "", snapKey, blob.NewByteStreamFromBytes(sstBytes))
			if err != nil {
				t.Fatalf("PutObject failed: %v", err)
			}

			restoreDir := t.TempDir()
			restoredDB, restoredPos, err := table.RestoreSnapshotKey(ctx, backend, vec.StreamID, snapKey, restoreDir)
			if err != nil {
				t.Fatalf("RestoreSnapshotKey failed: %v", err)
			}
			defer restoredDB.Close()

			if restoredPos != vec.Position {
				t.Fatalf("restoredPos = %d, want %d", restoredPos, vec.Position)
			}
			if restoredDB.Position() != vec.Position {
				t.Fatalf("restoredDB.Position = %d, want %d", restoredDB.Position(), vec.Position)
			}

			for _, tblName := range vec.TableNames {
				cnt, err := restoredDB.Count(ctx, tblName)
				if err != nil {
					t.Fatalf("restoredDB.Count(%q) failed: %v", tblName, err)
				}
				if cnt != vec.RowCount {
					t.Fatalf("restoredDB count = %d, want %d", cnt, vec.RowCount)
				}
			}
		})
	}
}

func generateGoldenVector(t *testing.T, ctx context.Context, vectorDir string) {
	dir := t.TempDir()
	streamID := "stream-golden-01"
	db, err := table.Open(ctx, dir, table.WithStreamID(streamID))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	def, err := db.RegisterType(ctx, &pb.Inode{}, 1)
	if err != nil {
		t.Fatalf("RegisterType failed: %v", err)
	}

	pk := sds.NewPrimaryKey(1)
	rowCount := 5
	for i := 1; i <= rowCount; i++ {
		node := &pb.Inode{
			Ino:  proto.Uint64(uint64(i)),
			Mode: uint32(0644),
		}
		k, v, _ := pk.Split(node)
		err := db.Apply(ctx, sds.Change{
			Op:       sds.OpCreate,
			TypeID:   def.GetId(),
			TypeName: def.GetName(),
			RawKey:   k,
			RawVal:   v,
			Seq:      uint64(i * 10),
		})
		if err != nil {
			t.Fatalf("Apply failed: %v", err)
		}
	}

	backend := inmemorystorage.New()
	snapKey, snapPos, err := db.PublishSnapshot(ctx, backend)
	if err != nil {
		t.Fatalf("PublishSnapshot failed: %v", err)
	}

	var buf []byte
	wBuf := &byteSliceWriter{buf: &buf}
	if err := backend.GetObject(ctx, "", snapKey, 0, 0, wBuf); err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}

	vec := SnapshotVector{
		Name:        "01_snapshot_basic",
		Description: "Single-table Inode snapshot at position 50 with 5 rows and reserved metadata keys",
		StreamID:    streamID,
		Position:    snapPos,
		TableNames:  []string{def.GetName()},
		RowCount:    rowCount,
		SSTableHex:  hex.EncodeToString(buf),
	}

	vecJSON, err := json.MarshalIndent(vec, "", "  ")
	if err != nil {
		t.Fatalf("MarshalIndent failed: %v", err)
	}

	outPath := filepath.Join(vectorDir, "01_snapshot_basic.json")
	if err := os.WriteFile(outPath, vecJSON, 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}
	t.Logf("Wrote golden vector to %s", outPath)
}

type byteSliceWriter struct {
	buf *[]byte
}

func (w *byteSliceWriter) Write(p []byte) (int, error) {
	*w.buf = append(*w.buf, p...)
	return len(p), nil
}
