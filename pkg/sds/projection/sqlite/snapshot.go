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

package sqlite

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/blob"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore"
)

var (
	// ErrSnapshotNotFound is returned when no snapshot is found matching the search criteria.
	ErrSnapshotNotFound = errors.New("snapshot not found")
	// ErrInvalidSnapshotKey is returned when a snapshot object key does not match the expected naming pattern.
	ErrInvalidSnapshotKey = errors.New("invalid snapshot key")
)

// SnapshotKey returns the canonical object storage key for a SQLite snapshot at the given position:
// "streams/<streamID>/snapshots/sqlite/<position, 20 digits>.sqlite".
func SnapshotKey(streamID string, position uint64) string {
	return fmt.Sprintf("streams/%s/snapshots/sqlite/%020d.sqlite", streamID, position)
}

// ParseSnapshotKey parses a canonical snapshot key into its stream ID and position.
func ParseSnapshotKey(key string) (streamID string, position uint64, err error) {
	// Expected format: streams/<stream-uuid>/snapshots/sqlite/<20-digits>.sqlite
	parts := strings.Split(key, "/")
	if len(parts) != 5 || parts[0] != "streams" || parts[2] != "snapshots" || parts[3] != "sqlite" {
		return "", 0, fmt.Errorf("%w: %q", ErrInvalidSnapshotKey, key)
	}

	streamID = parts[1]
	filePart := parts[4]
	if !strings.HasSuffix(filePart, ".sqlite") {
		return "", 0, fmt.Errorf("%w: missing .sqlite suffix in %q", ErrInvalidSnapshotKey, key)
	}

	posStr := strings.TrimSuffix(filePart, ".sqlite")
	if len(posStr) != 20 {
		return "", 0, fmt.Errorf("%w: expected 20-digit position, got %q in %q", ErrInvalidSnapshotKey, posStr, key)
	}

	pos, err := strconv.ParseUint(posStr, 10, 64)
	if err != nil {
		return "", 0, fmt.Errorf("%w: invalid position in %q: %w", ErrInvalidSnapshotKey, key, err)
	}

	return streamID, pos, nil
}

// FindLatestSnapshot finds the latest SQLite snapshot in object storage at or before maxPosition.
// If maxPosition is 0, any position is considered (or pass ^uint64(0) for latest).
func FindLatestSnapshot(ctx context.Context, backend objectstore.Backend, streamID string, maxPosition uint64) (key string, position uint64, err error) {
	if backend == nil {
		return "", 0, errors.New("nil backend")
	}

	prefix := fmt.Sprintf("streams/%s/snapshots/sqlite/", streamID)
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

// RestoreSnapshot downloads the latest SQLite snapshot at or before maxPosition,
// opens it at targetPath, loads the type registry and stream position, and returns the DB.
func RestoreSnapshot(ctx context.Context, backend objectstore.Backend, streamID string, maxPosition uint64, targetPath string, opts ...Option) (*DB, uint64, error) {
	key, pos, err := FindLatestSnapshot(ctx, backend, streamID, maxPosition)
	if err != nil {
		return nil, 0, err
	}

	if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
		return nil, 0, fmt.Errorf("failed to create directory for %q: %w", targetPath, err)
	}

	outFile, err := os.Create(targetPath)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to create restore file %q: %w", targetPath, err)
	}
	defer outFile.Close()

	if err := backend.GetObject(ctx, "", key, 0, 0, outFile); err != nil {
		_ = os.Remove(targetPath)
		return nil, 0, fmt.Errorf("failed to download snapshot %q: %w", key, err)
	}
	if err := outFile.Sync(); err != nil {
		return nil, 0, fmt.Errorf("failed to sync restored file %q: %w", targetPath, err)
	}

	allOpts := append([]Option{WithStreamID(streamID)}, opts...)
	db, err := Open(ctx, targetPath, allOpts...)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to open restored sqlite database: %w", err)
	}

	return db, pos, nil
}

// PublishSnapshot creates an atomic snapshot of an active DB at its current safe position,
// fsyncs the snapshot file, and uploads it to object storage.
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

	tmpFile, err := os.CreateTemp(tempDir, "sds-sqlite-snap-*.sqlite")
	if err != nil {
		return "", 0, fmt.Errorf("failed to create temp file for snapshot: %w", err)
	}
	tmpPath := tmpFile.Name()
	_ = tmpFile.Close()
	defer os.Remove(tmpPath)

	// Use SQLite VACUUM INTO to create a defragmented, clean snapshot file
	vacuumSQL := fmt.Sprintf("VACUUM INTO %q;", tmpPath)
	if _, err := db.SQLDB().ExecContext(ctx, vacuumSQL); err != nil {
		return "", 0, fmt.Errorf("failed to vacuum snapshot into %q: %w", tmpPath, err)
	}

	stat, err := os.Stat(tmpPath)
	if err != nil {
		return "", 0, fmt.Errorf("failed to stat snapshot file: %w", err)
	}

	f, err := os.Open(tmpPath)
	if err != nil {
		return "", 0, fmt.Errorf("failed to open snapshot file for reading: %w", err)
	}
	defer f.Close()

	key := SnapshotKey(streamID, pos)
	stream := blob.NewByteStreamFromFile(f, stat.Size(), false)

	if _, err := backend.PutObject(ctx, "", key, stream); err != nil {
		return "", 0, fmt.Errorf("failed to upload snapshot %q to backend: %w", key, err)
	}

	return key, pos, nil
}

// BuildSnapshot processes stream payloads into a new temporary SQLite database
// configured with journal_mode=OFF during bulk load, and publishes the snapshot to object storage.
func BuildSnapshot(ctx context.Context, streamID string, payloads [][]byte, backend objectstore.Backend, tempDir string, opts ...Option) (string, uint64, error) {
	if backend == nil {
		return "", 0, errors.New("nil backend")
	}

	tmpFile, err := os.CreateTemp(tempDir, "sds-sqlite-build-*.sqlite")
	if err != nil {
		return "", 0, fmt.Errorf("failed to create temp file for snapshot: %w", err)
	}
	tmpPath := tmpFile.Name()
	_ = tmpFile.Close()
	defer os.Remove(tmpPath)

	baseOpts := []Option{
		WithStreamID(streamID),
		WithJournalMode("OFF"),
		WithSynchronous("OFF"),
	}
	allOpts := append(baseOpts, opts...)

	db, err := Open(ctx, tmpPath, allOpts...)
	if err != nil {
		return "", 0, fmt.Errorf("failed to open build sqlite db: %w", err)
	}

	for i, payload := range payloads {
		seq := uint64(i + 1)
		if _, err := db.Feed(ctx, seq, payload); err != nil {
			_ = db.Close()
			return "", 0, fmt.Errorf("feed failed at seq %d: %w", seq, err)
		}
	}

	if db.ChangeReader().HasPending() {
		_ = db.Close()
		return "", 0, ErrPendingTransaction
	}

	pos := db.Position()
	if err := db.Close(); err != nil {
		return "", 0, fmt.Errorf("failed to close build db: %w", err)
	}

	// Reopen file to upload
	f, err := os.Open(tmpPath)
	if err != nil {
		return "", 0, fmt.Errorf("failed to open build db file: %w", err)
	}
	defer f.Close()

	if err := f.Sync(); err != nil {
		return "", 0, fmt.Errorf("failed to fsync build db file: %w", err)
	}

	stat, err := f.Stat()
	if err != nil {
		return "", 0, fmt.Errorf("failed to stat build db file: %w", err)
	}

	key := SnapshotKey(streamID, pos)
	stream := blob.NewByteStreamFromFile(f, stat.Size(), false)

	if _, err := backend.PutObject(ctx, "", key, stream); err != nil {
		return "", 0, fmt.Errorf("failed to upload snapshot %q to backend: %w", key, err)
	}

	return key, pos, nil
}

// DownloadSnapshotBytes retrieves raw snapshot SQLite file bytes from the object store.
func DownloadSnapshotBytes(ctx context.Context, backend objectstore.Backend, key string) ([]byte, error) {
	var buf bytes.Buffer
	if err := backend.GetObject(ctx, "", key, 0, 0, &buf); err != nil {
		return nil, fmt.Errorf("failed to get snapshot bytes for %q: %w", key, err)
	}
	return buf.Bytes(), nil
}
