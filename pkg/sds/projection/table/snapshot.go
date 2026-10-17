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

package table

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/pebble/v2/bloom"
	"github.com/cockroachdb/pebble/v2/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/v2/sstable"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/gke-labs/in-cluster-storage/pkg/errutil"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/blob"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore"
)

var (
	// ErrSnapshotNotFound is returned when no matching snapshot is found in object storage.
	ErrSnapshotNotFound = errors.New("table snapshot not found")

	// ErrInvalidSnapshotKey is returned when a snapshot object key cannot be parsed.
	ErrInvalidSnapshotKey = errors.New("invalid table snapshot key")

	// ErrPendingTransaction is returned when attempting to publish a snapshot while a transaction is in flight.
	ErrPendingTransaction = errors.New("cannot publish table snapshot with pending transaction")
)

// SnapshotKey formats the canonical object storage key for a table snapshot:
//
//	streams/<streamID>/snapshots/table/<position, 20 digits>.sst
func SnapshotKey(streamID string, position uint64) string {
	return fmt.Sprintf("streams/%s/snapshots/table/%020d.sst", streamID, position)
}

// ParseSnapshotKey parses the stream ID and position from a canonical snapshot key.
func ParseSnapshotKey(key string) (streamID string, position uint64, err error) {
	if !strings.HasPrefix(key, "streams/") {
		return "", 0, fmt.Errorf("%w: %q", ErrInvalidSnapshotKey, key)
	}
	trimmed := strings.TrimPrefix(key, "streams/")
	parts := strings.Split(trimmed, "/")
	if len(parts) != 4 || parts[0] == "wal" || parts[1] != "snapshots" || parts[2] != "table" {
		return "", 0, fmt.Errorf("%w: %q", ErrInvalidSnapshotKey, key)
	}

	streamID = parts[0]
	filename := parts[3]
	if !strings.HasSuffix(filename, ".sst") {
		return "", 0, fmt.Errorf("%w: missing .sst extension in %q", ErrInvalidSnapshotKey, key)
	}

	posStr := strings.TrimSuffix(filename, ".sst")
	if len(posStr) != 20 {
		return "", 0, fmt.Errorf("%w: expected 20-digit position, got %q in %q", ErrInvalidSnapshotKey, posStr, key)
	}

	pos, err := strconv.ParseUint(posStr, 10, 64)
	if err != nil {
		return "", 0, fmt.Errorf("%w: invalid position in %q: %w", ErrInvalidSnapshotKey, key, err)
	}

	return streamID, pos, nil
}

// FindLatestSnapshot finds the latest table snapshot in object storage at or before maxPosition.
// If maxPosition is 0, the latest snapshot overall is returned.
func FindLatestSnapshot(ctx context.Context, backend objectstore.Backend, streamID string, maxPosition uint64) (key string, position uint64, err error) {
	if backend == nil {
		return "", 0, errors.New("nil backend")
	}

	prefix := fmt.Sprintf("streams/%s/snapshots/table/", streamID)
	keys, err := backend.ListObjects(ctx, "", prefix)
	if err != nil {
		return "", 0, fmt.Errorf("failed to list snapshots for stream %s: %w", streamID, err)
	}

	type match struct {
		key string
		pos uint64
	}
	var matches []match

	for _, k := range keys {
		sID, pos, parseErr := ParseSnapshotKey(k)
		if parseErr != nil || sID != streamID {
			continue
		}
		if maxPosition == 0 || pos <= maxPosition {
			matches = append(matches, match{key: k, pos: pos})
		}
	}

	if len(matches) == 0 {
		return "", 0, ErrSnapshotNotFound
	}

	sort.Slice(matches, func(i, j int) bool {
		return matches[i].pos < matches[j].pos
	})

	latest := matches[len(matches)-1]
	return latest.key, latest.pos, nil
}

// RestoreSnapshot downloads the latest table snapshot at or before maxPosition and restores it into targetDir.
func RestoreSnapshot(ctx context.Context, backend objectstore.Backend, streamID string, maxPosition uint64, targetDir string, opts ...Option) (*DB, uint64, error) {
	key, _, err := FindLatestSnapshot(ctx, backend, streamID, maxPosition)
	if err != nil {
		return nil, 0, err
	}
	return RestoreSnapshotKey(ctx, backend, streamID, key, targetDir, opts...)
}

// RestoreSnapshotKey downloads a specific table snapshot key and ingests it into a new database in targetDir.
func RestoreSnapshotKey(ctx context.Context, backend objectstore.Backend, streamID string, key string, targetDir string, opts ...Option) (*DB, uint64, error) {
	if backend == nil {
		return nil, 0, errors.New("nil backend")
	}

	_, pos, err := ParseSnapshotKey(key)
	if err != nil {
		return nil, 0, err
	}

	if err := os.MkdirAll(targetDir, 0755); err != nil {
		return nil, 0, fmt.Errorf("failed to create target directory %q: %w", targetDir, err)
	}

	// Download the snapshot SST file into targetDir so it is on the same filesystem for hard-link ingestion.
	tmpSST := filepath.Join(targetDir, fmt.Sprintf("restore-snap-%d.sst", time.Now().UnixNano()))
	tmpFile, err := os.OpenFile(tmpSST, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to create temp file for snapshot download: %w", err)
	}
	defer func() {
		if tmpFile != nil {
			errutil.CloseAndLog(tmpFile, "downloaded snapshot file")
		}
	}()

	if err := backend.GetObject(ctx, "", key, 0, 0, tmpFile); err != nil {
		removeAndLog(tmpSST, "incomplete snapshot download")
		return nil, 0, fmt.Errorf("failed to get snapshot object %q: %w", key, err)
	}
	if err := tmpFile.Sync(); err != nil {
		removeAndLog(tmpSST, "incomplete snapshot download")
		return nil, 0, fmt.Errorf("failed to sync downloaded snapshot file: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		tmpFile = nil
		removeAndLog(tmpSST, "incomplete snapshot download")
		return nil, 0, fmt.Errorf("failed to close downloaded snapshot file: %w", err)
	}
	tmpFile = nil
	defer removeAndLog(tmpSST, "downloaded snapshot file")

	// Validate the snapshot SST before creating or ingesting into the database
	var readerOpts []SnapshotReaderOption
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	if o.acceptUnversioned {
		readerOpts = append(readerOpts, WithReaderAcceptUnversioned(true))
	}
	sr, err := OpenSnapshotFile(tmpSST, readerOpts...)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to validate snapshot %q: %w", key, err)
	}
	errutil.CloseAndLog(sr, "snapshot reader")

	dbDir := filepath.Join(targetDir, "table.db")
	removeAllAndLog(dbDir, "table db dir")

	allOpts := append([]Option{WithStreamID(streamID)}, opts...)
	db, err := Open(ctx, dbDir, allOpts...)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to open new table db at %q: %w", dbDir, err)
	}

	if err := db.db.Ingest(ctx, []string{tmpSST}); err != nil {
		errutil.CloseAndLog(db, "table db")
		return nil, 0, fmt.Errorf("failed to ingest snapshot %q into db: %w", key, err)
	}

	db.mu.Lock()
	err = db.loadMetadataLocked()
	db.mu.Unlock()
	if err != nil {
		errutil.CloseAndLog(db, "table db")
		return nil, 0, fmt.Errorf("failed to reload metadata after snapshot ingestion: %w", err)
	}

	return db, pos, nil
}

// PublishSnapshot creates an atomic snapshot of an active table DB at its current safe position,
// writes a pinned-format SSTable containing reserved metadata keys and all live rows, and uploads it to object storage.
func PublishSnapshot(ctx context.Context, db *DB, backend objectstore.Backend, tempDir string) (string, uint64, error) {
	if db == nil {
		return "", 0, errors.New("nil db")
	}
	if backend == nil {
		return "", 0, errors.New("nil backend")
	}

	db.mu.RLock()
	hasPending := db.changeReader.HasPending()
	pos := db.position
	streamID := db.streamID
	db.mu.RUnlock()

	if hasPending {
		return "", 0, ErrPendingTransaction
	}

	snap := db.db.NewSnapshot()
	defer func() {
		errutil.CloseAndLog(snap, "db snapshot")
	}()

	tmpFile, err := os.CreateTemp(tempDir, "sds-table-snap-*.sst")
	if err != nil {
		return "", 0, fmt.Errorf("failed to create temp file for snapshot: %w", err)
	}
	tmpPath := tmpFile.Name()
	if err := tmpFile.Close(); err != nil {
		removeAndLog(tmpPath, "temp snapshot file")
		return "", 0, fmt.Errorf("failed to close temp file %q: %w", tmpPath, err)
	}
	defer removeAndLog(tmpPath, "temp snapshot file")

	f, err := vfs.Default.Create(tmpPath, vfs.WriteCategoryUnspecified)
	if err != nil {
		return "", 0, fmt.Errorf("failed to open vfs file %q: %w", tmpPath, err)
	}

	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{
		TableFormat:  SnapshotTableFormat,
		FilterPolicy: bloom.FilterPolicy(10),
		Compression:  sstable.NoCompression,
	})

	iter, err := snap.NewIter(nil)
	if err != nil {
		errutil.CloseAndLog(w, "sstable writer")
		return "", 0, fmt.Errorf("failed to open snapshot iterator: %w", err)
	}

	for iter.First(); iter.Valid(); iter.Next() {
		if err := w.Set(iter.Key(), iter.Value()); err != nil {
			errutil.CloseAndLog(iter, "snapshot iterator")
			errutil.CloseAndLog(w, "sstable writer")
			return "", 0, fmt.Errorf("failed to write key to snapshot sstable: %w", err)
		}
	}

	if err := iter.Close(); err != nil {
		errutil.CloseAndLog(w, "sstable writer")
		return "", 0, fmt.Errorf("failed to close snapshot iterator: %w", err)
	}

	if err := w.Close(); err != nil {
		return "", 0, fmt.Errorf("failed to finish snapshot sstable writer: %w", err)
	}

	stat, err := os.Stat(tmpPath)
	if err != nil {
		return "", 0, fmt.Errorf("failed to stat snapshot sstable: %w", err)
	}

	fIn, err := os.Open(tmpPath)
	if err != nil {
		return "", 0, fmt.Errorf("failed to open snapshot sstable for upload: %w", err)
	}
	defer func() {
		errutil.CloseAndLog(fIn, "snapshot upload file")
	}()

	key := SnapshotKey(streamID, pos)
	stream := blob.NewByteStreamFromFile(fIn, stat.Size(), false)

	if _, err := backend.PutObject(ctx, "", key, stream); err != nil {
		return "", 0, fmt.Errorf("failed to upload snapshot %q: %w", key, err)
	}

	return key, pos, nil
}

// BuildSnapshot processes stream payloads into a new temporary table DB,
// publishes the snapshot to object storage, and cleans up the temporary database.
func BuildSnapshot(ctx context.Context, streamID string, payloads [][]byte, backend objectstore.Backend, tempDir string) (string, uint64, error) {
	if backend == nil {
		return "", 0, errors.New("nil backend")
	}

	buildDir, err := os.MkdirTemp(tempDir, "sds-table-build-*")
	if err != nil {
		return "", 0, fmt.Errorf("failed to create temp directory for build snapshot: %w", err)
	}
	defer removeAllAndLog(buildDir, "build directory")

	db, err := Open(ctx, buildDir,
		WithStreamID(streamID),
		WithDisableWAL(true),
	)
	if err != nil {
		return "", 0, fmt.Errorf("failed to open build table db: %w", err)
	}
	defer func() {
		errutil.CloseAndLog(db, "build table db")
	}()

	for i, payload := range payloads {
		seq := uint64(i + 1)
		if _, err := db.Feed(ctx, seq, payload); err != nil {
			return "", 0, fmt.Errorf("feed failed at seq %d: %w", seq, err)
		}
	}

	if db.ChangeReader().HasPending() {
		return "", 0, ErrPendingTransaction
	}

	return PublishSnapshot(ctx, db, backend, tempDir)
}
