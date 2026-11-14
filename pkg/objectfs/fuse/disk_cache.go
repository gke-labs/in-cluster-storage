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

package fuse

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/scoped"
	"golang.org/x/sync/singleflight"
	_ "modernc.org/sqlite"
)

// DiskBlobCache implements a persistent on-disk blob cache backed by a local
// SQLite index. Blobs are content-addressed by their SHA256 hex string and verified
// on write. Clean data is served from disk and evicted via LRU size capping.
type DiskBlobCache struct {
	dir        string
	blobsDir   string
	tmpDir     string
	db         *sql.DB
	maxBytes   int64
	curBytes   int64
	sf         singleflight.Group
	mu         sync.Mutex
	closed     atomic.Bool
	wrapWriter func(io.WriteCloser) io.WriteCloser
}

// NewDiskBlobCache initializes or opens an on-disk blob cache in dir.
func NewDiskBlobCache(ctx context.Context, dir string, maxBytes int64) (*DiskBlobCache, error) {
	if dir == "" {
		dir = "/var/cache/objectfs"
	}
	if maxBytes <= 0 {
		maxBytes = 10 * 1024 * 1024 * 1024 // 10 GiB default
	}

	blobsDir := filepath.Join(dir, "blobs")
	tmpDir := filepath.Join(dir, "tmp")

	if err := os.MkdirAll(blobsDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create blobs directory %s: %w", blobsDir, err)
	}
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create tmp directory %s: %w", tmpDir, err)
	}

	// Clean up stale tmp files left from previous crashes or interrupted fetches
	tmpEntries, err := os.ReadDir(tmpDir)
	if err == nil {
		for _, e := range tmpEntries {
			if remErr := os.Remove(filepath.Join(tmpDir, e.Name())); remErr != nil {
				scoped.Log(ctx).V(4).Info("DiskBlobCache: error removing stale tmp entry", "entry", e.Name(), "err", remErr)
			}
		}
	} else if !os.IsNotExist(err) {
		scoped.Log(ctx).V(4).Info("DiskBlobCache: error reading tmp dir", "dir", tmpDir, "err", err)
	}

	dbPath := filepath.Join(dir, "index.db")
	db, err := sql.Open("sqlite", dbPath+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)")
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite index at %s: %w", dbPath, err)
	}
	db.SetMaxOpenConns(1)

	schema := `
	CREATE TABLE IF NOT EXISTS blobs (
		sha256 TEXT PRIMARY KEY,
		size INTEGER NOT NULL,
		is_cached INTEGER NOT NULL DEFAULT 0,
		last_access INTEGER NOT NULL
	);
	CREATE INDEX IF NOT EXISTS idx_blobs_cached_lru ON blobs (is_cached, last_access);
	`
	if _, err := db.Exec(schema); err != nil {
		if closeErr := db.Close(); closeErr != nil {
			scoped.Log(ctx).V(4).Info("DiskBlobCache: error closing db after schema init failure", "err", closeErr)
		}
		return nil, fmt.Errorf("failed to initialize sqlite index schema: %w", err)
	}

	cache := &DiskBlobCache{
		dir:      dir,
		blobsDir: blobsDir,
		tmpDir:   tmpDir,
		db:       db,
		maxBytes: maxBytes,
	}

	if err := cache.reconcileDiskState(ctx); err != nil {
		scoped.Log(ctx).Info("DiskBlobCache: error reconciling disk state", "err", err)
	}

	return cache, nil
}

// reconcileDiskState reconciles files present in blobsDir with the SQLite index.
func (c *DiskBlobCache) reconcileDiskState(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	entries, err := os.ReadDir(c.blobsDir)
	if err != nil {
		return err
	}

	now := time.Now().UnixNano()
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if len(name) != 64 {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		size := info.Size()
		_, err = c.db.Exec(`
			INSERT INTO blobs (sha256, size, is_cached, last_access)
			VALUES (?, ?, 1, ?)
			ON CONFLICT(sha256) DO UPDATE SET
				is_cached = 1,
				size = excluded.size
		`, name, size, now)
		if err != nil {
			scoped.Log(ctx).Info("DiskBlobCache: failed indexing disk entry", "entry", name, "err", err)
		}
	}

	var totalBytes int64
	row := c.db.QueryRow("SELECT COALESCE(SUM(size), 0) FROM blobs WHERE is_cached = 1")
	if err := row.Scan(&totalBytes); err != nil {
		return err
	}
	c.curBytes = totalBytes

	c.evictIfNeededLocked(ctx)
	return nil
}

// HasBlob returns true if the blob file exists on disk and is recorded in cache.
func (c *DiskBlobCache) HasBlob(sha string) bool {
	if sha == "" {
		return false
	}
	p := filepath.Join(c.blobsDir, sha)
	info, err := os.Stat(p)
	return err == nil && !info.IsDir()
}

// ReadBlob reads a blob directly from disk and updates its access time.
func (c *DiskBlobCache) ReadBlob(ctx context.Context, sha string) ([]byte, error) {
	if sha == "" {
		return nil, fmt.Errorf("sha256 is required")
	}
	p := filepath.Join(c.blobsDir, sha)
	data, err := os.ReadFile(p)
	if err != nil {
		return nil, err
	}

	// Update last_access asynchronously/non-blockingly
	now := time.Now().UnixNano()
	go func() {
		if c.closed.Load() {
			return
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		if _, err := c.db.Exec("UPDATE blobs SET last_access = ? WHERE sha256 = ?", now, sha); err != nil {
			scoped.Log(ctx).V(4).Info("DiskBlobCache: error updating last_access", "sha", sha, "err", err)
		}
	}()

	return data, nil
}

// PutBlob stores data directly into the blob cache under its sha256 hash,
// verifying the content hash before renaming into place.
func (c *DiskBlobCache) PutBlob(ctx context.Context, sha string, data []byte) error {
	if sha == "" {
		return fmt.Errorf("sha256 is required")
	}
	if c.closed.Load() {
		return fmt.Errorf("cache is closed")
	}

	// Verify hash
	sum := sha256.Sum256(data)
	actualSHA := hex.EncodeToString(sum[:])
	if actualSHA != sha {
		return fmt.Errorf("sha256 mismatch: got %s, want %s", actualSHA, sha)
	}

	tmpPath := filepath.Join(c.tmpDir, fmt.Sprintf("%s-%d.tmp", sha, time.Now().UnixNano()))
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("failed writing tmp blob %s: %w", tmpPath, err)
	}

	finalPath := filepath.Join(c.blobsDir, sha)
	if err := os.Rename(tmpPath, finalPath); err != nil {
		if remErr := os.Remove(tmpPath); remErr != nil {
			scoped.Log(ctx).V(4).Info("DiskBlobCache: failed removing tmp file", "path", tmpPath, "err", remErr)
		}
		return fmt.Errorf("failed renaming tmp blob to %s: %w", finalPath, err)
	}

	now := time.Now().UnixNano()
	c.mu.Lock()
	defer c.mu.Unlock()

	_, err := c.db.Exec(`
		INSERT INTO blobs (sha256, size, is_cached, last_access)
		VALUES (?, ?, 1, ?)
		ON CONFLICT(sha256) DO UPDATE SET
			is_cached = 1,
			size = excluded.size,
			last_access = excluded.last_access
	`, sha, int64(len(data)), now)
	if err != nil {
		scoped.Log(ctx).Info("DiskBlobCache: failed recording blob in SQLite", "sha", sha, "err", err)
	}

	c.curBytes += int64(len(data))
	c.evictIfNeededLocked(ctx)
	return nil
}

// FetchBlob fetches a blob from the controller via GetBlob RPC, writes it to a
// temp file, verifies its sha256, renames it into place, and records it in SQLite.
// Concurrent calls for the same sha256 are coalesced via singleflight.
func (c *DiskBlobCache) FetchBlob(ctx context.Context, client pb.ObjectFSControllerClient, sha string) ([]byte, error) {
	if sha == "" {
		return nil, fmt.Errorf("sha256 is required")
	}

	if c.HasBlob(sha) {
		return c.ReadBlob(ctx, sha)
	}

	res, err, _ := c.sf.Do(sha, func() (any, error) {
		// Double check if another call completed it
		if c.HasBlob(sha) {
			return c.ReadBlob(ctx, sha)
		}

		if client == nil {
			return nil, fmt.Errorf("controller client not configured")
		}

		stream, err := client.GetBlob(ctx, &pb.GetBlobRequest{Sha256: sha})
		if err != nil {
			return nil, fmt.Errorf("GetBlob RPC failed for sha %s: %w", sha, err)
		}

		tmpPath := filepath.Join(c.tmpDir, fmt.Sprintf("%s-%d.tmp", sha, time.Now().UnixNano()))
		tmpFile, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {
			return nil, fmt.Errorf("failed creating tmp file %s: %w", tmpPath, err)
		}

		success := false
		defer func() {
			if !success {
				if remErr := os.Remove(tmpPath); remErr != nil && !os.IsNotExist(remErr) {
					scoped.Log(ctx).V(4).Info("DiskBlobCache: error removing tmp file", "path", tmpPath, "err", remErr)
				}
			}
		}()

		var writerCloser io.WriteCloser = tmpFile
		if c.wrapWriter != nil {
			writerCloser = c.wrapWriter(tmpFile)
		}

		hasher := sha256.New()
		writer := io.MultiWriter(writerCloser, hasher)

		var totalBytes int64
		for {
			chunkResp, rErr := stream.Recv()
			if rErr == io.EOF {
				break
			}
			if rErr != nil {
				closeErr := writerCloser.Close()
				return nil, errors.Join(fmt.Errorf("failed receiving blob %s stream: %w", sha, rErr), closeErr)
			}
			data := chunkResp.GetData()
			if len(data) > 0 {
				n, wErr := writer.Write(data)
				if wErr != nil {
					closeErr := writerCloser.Close()
					return nil, errors.Join(fmt.Errorf("failed writing blob %s to tmp: %w", sha, wErr), closeErr)
				}
				totalBytes += int64(n)
			}
		}

		if err := writerCloser.Close(); err != nil {
			return nil, fmt.Errorf("failed closing tmp file %s: %w", tmpPath, err)
		}

		actualSHA := hex.EncodeToString(hasher.Sum(nil))
		if actualSHA != sha {
			return nil, fmt.Errorf("sha256 mismatch for blob %s: computed %s", sha, actualSHA)
		}

		finalPath := filepath.Join(c.blobsDir, sha)
		if err := os.Rename(tmpPath, finalPath); err != nil {
			return nil, fmt.Errorf("failed renaming tmp blob to %s: %w", finalPath, err)
		}

		now := time.Now().UnixNano()
		c.mu.Lock()
		if _, err := c.db.Exec(`
			INSERT INTO blobs (sha256, size, is_cached, last_access)
			VALUES (?, ?, 1, ?)
			ON CONFLICT(sha256) DO UPDATE SET
				is_cached = 1,
				size = excluded.size,
				last_access = excluded.last_access
		`, sha, totalBytes, now); err != nil {
			scoped.Log(ctx).Info("DiskBlobCache: failed recording blob in SQLite", "sha", sha, "err", err)
		}
		c.curBytes += totalBytes
		c.evictIfNeededLocked(ctx)
		c.mu.Unlock()

		success = true
		return c.ReadBlob(ctx, sha)
	})

	if err != nil {
		return nil, err
	}
	return res.([]byte), nil
}

// EnsureBlob ensures the blob is present on disk, fetching it via client if missing.
func (c *DiskBlobCache) EnsureBlob(ctx context.Context, client pb.ObjectFSControllerClient, sha string) error {
	if sha == "" {
		return nil
	}
	if c.HasBlob(sha) {
		return nil
	}
	_, err := c.FetchBlob(ctx, client, sha)
	return err
}

// RecordKnownBlobs inserts known sha256 hashes into the SQLite index without fetching them.
func (c *DiskBlobCache) RecordKnownBlobs(ctx context.Context, shas []string) {
	if len(shas) == 0 || c.closed.Load() {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now().UnixNano()
	for _, sha := range shas {
		if sha == "" {
			continue
		}
		if _, err := c.db.Exec(`
			INSERT OR IGNORE INTO blobs (sha256, size, is_cached, last_access)
			VALUES (?, 0, 0, ?)
		`, sha, now); err != nil {
			scoped.Log(ctx).V(4).Info("DiskBlobCache: error inserting known blob", "sha", sha, "err", err)
		}
	}
}

// evictIfNeededLocked evicts least recently accessed cached blobs when curBytes exceeds maxBytes.
func (c *DiskBlobCache) evictIfNeededLocked(ctx context.Context) {
	if c.curBytes <= c.maxBytes {
		return
	}

	target := c.maxBytes * 9 / 10 // target 90% of max capacity
	for c.curBytes > target {
		rows, err := c.db.Query(`
			SELECT sha256, size
			FROM blobs
			WHERE is_cached = 1
			ORDER BY last_access ASC
			LIMIT 50
		`)
		if err != nil {
			scoped.Log(ctx).Info("DiskBlobCache: eviction query failed", "err", err)
			break
		}

		type toEvict struct {
			sha  string
			size int64
		}
		var batch []toEvict
		for rows.Next() {
			var e toEvict
			if err := rows.Scan(&e.sha, &e.size); err != nil {
				scoped.Log(ctx).V(4).Info("DiskBlobCache: eviction scan error", "err", err)
				continue
			}
			batch = append(batch, e)
		}
		if err := rows.Close(); err != nil {
			scoped.Log(ctx).V(4).Info("DiskBlobCache: error closing eviction rows", "err", err)
		}
		if err := rows.Err(); err != nil {
			scoped.Log(ctx).V(4).Info("DiskBlobCache: eviction rows error", "err", err)
		}

		if len(batch) == 0 {
			break
		}

		for _, item := range batch {
			p := filepath.Join(c.blobsDir, item.sha)
			if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
				scoped.Log(ctx).V(4).Info("DiskBlobCache: failed removing evicted file", "path", p, "err", err)
			}
			if _, err := c.db.Exec("UPDATE blobs SET is_cached = 0 WHERE sha256 = ?", item.sha); err != nil {
				scoped.Log(ctx).V(4).Info("DiskBlobCache: failed updating evicted entry", "sha", item.sha, "err", err)
			}
			c.curBytes -= item.size
			if c.curBytes <= target {
				break
			}
		}
	}
}

// CurrentBytes returns the current byte count of cached blobs on disk.
func (c *DiskBlobCache) CurrentBytes() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.curBytes
}

// MaxBytes returns the maximum byte limit of the cache.
func (c *DiskBlobCache) MaxBytes() int64 {
	return c.maxBytes
}

// Close closes the underlying SQLite database.
func (c *DiskBlobCache) Close() error {
	if c.closed.Swap(true) {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}
