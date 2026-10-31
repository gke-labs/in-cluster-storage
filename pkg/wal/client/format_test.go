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

package client

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/gke-labs/in-cluster-storage/pkg/wal"
	"github.com/gke-labs/in-cluster-storage/pkg/wal/buffer/buffertest"
	"github.com/google/uuid"
)

func TestClientOpen_UnversionedUpgrade(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	streamID := uuid.New()
	bt, _ := buffertest.Start(ctx, t)

	// Create legacy unversioned client segment file
	clientRec := &wal.ClientRecord{
		StreamID:  streamID,
		StreamSeq: 1,
		Payload:   []byte("payload-legacy-1"),
	}
	clientRec.CRC32C = clientRec.ComputeCRC32C()
	segPath := filepath.Join(dir, fmt.Sprintf("stream-%s-%019d.wal", streamID.String(), 1))
	if err := os.WriteFile(segPath, clientRec.Encode(), 0644); err != nil {
		t.Fatalf("failed to write unversioned client segment: %v", err)
	}

	// 1. Without flag: Open must fail with ErrUnversionedSegment
	_, err := Open(ctx, dir, streamID, bt.Target, WithGRPCClient(bt.Client), WithDialOptions(bt.DialOpts...))
	if err == nil {
		t.Fatal("expected error without WithAcceptUnversioned, got nil")
	}
	if !errors.Is(err, wal.ErrUnversionedSegment) {
		t.Fatalf("expected ErrUnversionedSegment, got: %v", err)
	}

	// 2. With flag: Open must succeed, recover the record, and set LocalSequence to 1
	stream, err := Open(ctx, dir, streamID, bt.Target, WithGRPCClient(bt.Client), WithDialOptions(bt.DialOpts...), WithAcceptUnversioned(true))
	if err != nil {
		t.Fatalf("unexpected error with WithAcceptUnversioned: %v", err)
	}
	defer stream.Close()

	local, _, _ := stream.Watermarks()
	if local != 1 {
		t.Errorf("local watermark = %d, want 1", local)
	}
	recs := stream.RecoveredRecords()
	if len(recs) != 1 || recs[0].StreamSeq != 1 {
		t.Fatalf("expected 1 recovered record at seq 1, got %d (%+v)", len(recs), recs)
	}
	tailIter, err := stream.Tail(ctx, 0)
	if err != nil {
		t.Fatalf("Tail failed: %v", err)
	}
	var tailedSeqs []uint64
	for seq := range tailIter {
		tailedSeqs = append(tailedSeqs, seq)
		break
	}
	if len(tailedSeqs) != 1 || tailedSeqs[0] != 1 {
		t.Fatalf("expected 1 tailed record at seq 1, got %v", tailedSeqs)
	}
}
