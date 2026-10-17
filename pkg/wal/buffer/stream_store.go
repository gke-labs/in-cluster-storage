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

package buffer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/gke-labs/in-cluster-storage/pkg/errutil"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/blob"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore"
	"github.com/gke-labs/in-cluster-storage/pkg/wal"
	"github.com/google/uuid"
	"k8s.io/klog/v2"
)

const (
	DefaultStreamSealInterval = 1 * time.Hour
	DefaultStreamSealBytes    = 64 * 1024 * 1024
)

// StreamMeta describes a sealed per-stream WAL file.
type StreamMeta struct {
	FromSeq uint64
	ToSeq   uint64
	Path    string // local path on disk if present
	Key     string // object storage key if uploaded
}

type streamWriter struct {
	mu         sync.Mutex
	streamID   uuid.UUID
	dir        string
	openFrom   uint64
	lastSeq    uint64
	recCount   int
	fileSize   int64
	openTime   time.Time
	pending    []*wal.LogRecord
	sealed     []*StreamMeta
	uploadedTo uint64
}

func (w *streamWriter) appendRecords(records []*wal.LogRecord) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.pending = append(w.pending, records...)
}

func (w *streamWriter) flushPending() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.flushPendingLocked()
}

func (w *streamWriter) flushPendingLocked() error {
	if len(w.pending) == 0 {
		return nil
	}
	if err := os.MkdirAll(w.dir, 0755); err != nil {
		return fmt.Errorf("failed to create stream dir %s: %w", w.dir, err)
	}
	if w.recCount == 0 && len(w.pending) > 0 {
		w.openFrom = w.pending[0].StreamSeq
	} else if w.openFrom == 0 {
		w.openFrom = w.pending[0].StreamSeq
	}
	if w.openTime.IsZero() {
		w.openTime = time.Now()
	}

	openPath := filepath.Join(w.dir, wal.FormatStreamFileName(w.openFrom, 0, false))
	f, err := os.OpenFile(openPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open stream file %s: %w", openPath, err)
	}

	var buf bytes.Buffer
	for _, rec := range w.pending {
		buf.Write(rec.Encode())
		if rec.StreamSeq > w.lastSeq {
			w.lastSeq = rec.StreamSeq
		}
		w.recCount++
	}
	w.pending = nil

	n, writeErr := f.Write(buf.Bytes())
	closeErr := f.Close()
	if writeErr != nil {
		return fmt.Errorf("failed to write to stream file %s: %w", openPath, writeErr)
	}
	if closeErr != nil {
		return fmt.Errorf("failed to close stream file %s: %w", openPath, closeErr)
	}
	w.fileSize += int64(n)
	return nil
}

func (w *streamWriter) uploadSealedMetaLocked(ctx context.Context, backend objectstore.Backend, m *StreamMeta) error {
	if backend == nil || m.Key != "" || m.Path == "" {
		return nil
	}
	data, err := os.ReadFile(m.Path)
	if err != nil {
		return err
	}
	stream := blob.NewByteStreamFromBytes(data)
	defer errutil.CloseAndLog(stream, fmt.Sprintf("sealed stream %s", m.Path))
	objKey := wal.FormatStreamObjectKey(w.streamID, m.FromSeq, m.ToSeq)
	if _, err := backend.PutObject(ctx, "", objKey, stream); err != nil {
		return fmt.Errorf("failed to upload sealed stream file %s: %w", objKey, err)
	}
	m.Key = objKey
	if m.ToSeq > w.uploadedTo {
		w.uploadedTo = m.ToSeq
	}
	return nil
}

func (w *streamWriter) sealLocked(ctx context.Context, backend objectstore.Backend) (*StreamMeta, error) {
	if err := w.flushPendingLocked(); err != nil {
		return nil, err
	}
	if w.recCount == 0 || w.openFrom == 0 {
		return nil, nil // idle stream, do not seal empty file
	}

	openPath := filepath.Join(w.dir, wal.FormatStreamFileName(w.openFrom, 0, false))
	f, err := os.OpenFile(openPath, os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open stream file %s for fsync: %w", openPath, err)
	}
	if err := f.Sync(); err != nil {
		errutil.CloseAndLog(f, fmt.Sprintf("stream file %s", openPath))
		return nil, fmt.Errorf("failed to fsync stream file %s: %w", openPath, err)
	}
	if err := f.Close(); err != nil {
		return nil, fmt.Errorf("failed to close stream file %s: %w", openPath, err)
	}

	sealedName := wal.FormatStreamFileName(w.openFrom, w.lastSeq, true)
	sealedPath := filepath.Join(w.dir, sealedName)
	if err := os.Rename(openPath, sealedPath); err != nil {
		return nil, fmt.Errorf("failed to rename stream file %s to %s: %w", openPath, sealedPath, err)
	}

	meta := &StreamMeta{
		FromSeq: w.openFrom,
		ToSeq:   w.lastSeq,
		Path:    sealedPath,
	}

	// Rename completes the seal: update sealed list and advance openFrom immediately.
	w.sealed = append(w.sealed, meta)
	w.openFrom = w.lastSeq + 1
	w.recCount = 0
	w.fileSize = 0
	w.openTime = time.Time{}

	// Upload is an idempotent follow-up; if it fails, meta remains in w.sealed to retry.
	if err := w.uploadSealedMetaLocked(ctx, backend, meta); err != nil {
		klog.Warningf("Failed to upload sealed stream file %s: %v", sealedPath, err)
		return meta, err
	}

	return meta, nil
}

func (w *streamWriter) uploadPendingSealedLocked(ctx context.Context, backend objectstore.Backend) error {
	var firstErr error
	for _, m := range w.sealed {
		if err := w.uploadSealedMetaLocked(ctx, backend, m); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (w *streamWriter) discoverRemoteSealedLocked(ctx context.Context, backend objectstore.Backend) {
	if backend == nil {
		return
	}
	prefix := fmt.Sprintf("streams/%s/", w.streamID.String())
	keys, err := backend.ListObjects(ctx, "", prefix)
	if err != nil {
		return
	}
	for _, k := range keys {
		from, to, sealed, err := wal.ParseStreamPath(k)
		if err != nil || !sealed {
			continue
		}
		if to > w.uploadedTo {
			w.uploadedTo = to
		}
		if to > w.lastSeq {
			w.lastSeq = to
		}
		found := false
		for _, sm := range w.sealed {
			if sm.FromSeq == from && sm.ToSeq == to {
				if sm.Key == "" {
					sm.Key = k
				}
				found = true
				break
			}
		}
		if !found {
			w.sealed = append(w.sealed, &StreamMeta{FromSeq: from, ToSeq: to, Key: k})
		}
	}
	sort.Slice(w.sealed, func(i, j int) bool {
		return w.sealed[i].FromSeq < w.sealed[j].FromSeq
	})
}

func (w *streamWriter) getSealedMetas(ctx context.Context, backend objectstore.Backend) []*StreamMeta {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.discoverRemoteSealedLocked(ctx, backend)
	res := make([]*StreamMeta, len(w.sealed))
	copy(res, w.sealed)
	return res
}

func (w *streamWriter) recoverStream(ctx context.Context, backend objectstore.Backend, aggRecords []*wal.LogRecord) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 1. Scan local directory for sealed and open files
	entries, err := os.ReadDir(w.dir)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to read stream directory %s: %w", w.dir, err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		from, to, sealed, err := wal.ParseStreamPath(e.Name())
		if err != nil {
			continue
		}
		filePath := filepath.Join(w.dir, e.Name())
		if sealed {
			w.sealed = append(w.sealed, &StreamMeta{FromSeq: from, ToSeq: to, Path: filePath})
			if to > w.lastSeq {
				w.lastSeq = to
			}
		} else {
			recs, meta, err := wal.ScanLogSegmentFile(filePath)
			if err != nil {
				return fmt.Errorf("failed to scan open segment file %s: %w", filePath, err)
			}
			if meta != nil {
				w.openFrom = from
				w.recCount = meta.RecordCount
				w.fileSize = meta.Size
				if len(recs) > 0 && recs[len(recs)-1].StreamSeq > w.lastSeq {
					w.lastSeq = recs[len(recs)-1].StreamSeq
				}
			}
		}
	}

	// 2. Discover remote sealed files
	w.discoverRemoteSealedLocked(ctx, backend)

	// 3. Upload any local sealed files that were not uploaded
	if err := w.uploadPendingSealedLocked(ctx, backend); err != nil {
		klog.Warningf("Failed to upload pending sealed files during recovery for stream %s: %v", w.streamID, err)
	}

	// 4. Catch up missing records from aggregated log
	for _, rec := range aggRecords {
		if rec.StreamID == w.streamID && rec.StreamSeq > w.lastSeq {
			w.pending = append(w.pending, rec)
		}
	}
	return w.flushPendingLocked()
}

// ReadRecordsFromOffset reads valid LogRecords from path starting at startOffset, returning records and nextOffset.
func ReadRecordsFromOffset(path string, startOffset int64) ([]*wal.LogRecord, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, startOffset, nil
		}
		return nil, startOffset, err
	}
	defer errutil.CloseAndLog(f, fmt.Sprintf("stream file %s", path))

	if startOffset > 0 {
		if _, err := f.Seek(startOffset, io.SeekStart); err != nil {
			return nil, startOffset, err
		}
	}

	nextOffset := startOffset
	var records []*wal.LogRecord
	for {
		rec, err := wal.DecodeLogRecord(f)
		if err != nil {
			break
		}
		records = append(records, rec)
		nextOffset += int64(wal.LogHeaderSize + len(rec.Payload))
	}
	return records, nextOffset, nil
}

// StreamStore manages per-stream WAL files on local disk and object storage.
type StreamStore struct {
	mu           sync.RWMutex
	dataDir      string
	backend      objectstore.Backend
	sealInterval time.Duration
	sealBytes    int64

	streams     map[string]*streamWriter
	writeNotify chan struct{}
	coverageMu  sync.Mutex
	segmentReqs map[string]map[string]uint64 // segPath -> streamID string -> maxSeq

	stopChan chan struct{}
	wg       sync.WaitGroup
}

// NewStreamStore creates and initializes a StreamStore.
func NewStreamStore(dataDir string, backend objectstore.Backend, sealInterval time.Duration, sealBytes int64) *StreamStore {
	if sealInterval <= 0 {
		sealInterval = DefaultStreamSealInterval
	}
	if sealBytes <= 0 {
		sealBytes = DefaultStreamSealBytes
	}
	s := &StreamStore{
		dataDir:      dataDir,
		backend:      backend,
		sealInterval: sealInterval,
		sealBytes:    sealBytes,
		streams:      make(map[string]*streamWriter),
		writeNotify:  make(chan struct{}, 1),
		segmentReqs:  make(map[string]map[string]uint64),
		stopChan:     make(chan struct{}),
	}

	s.wg.Add(1)
	go s.periodicWorker()

	return s
}

func (s *StreamStore) periodicWorker() {
	defer s.wg.Done()
	tickInterval := s.sealInterval
	if tickInterval > 50*time.Millisecond && tickInterval > time.Second {
		tickInterval = time.Second
	} else if tickInterval > 50*time.Millisecond {
		tickInterval = 50 * time.Millisecond
	}
	sealTicker := time.NewTicker(tickInterval)
	defer sealTicker.Stop()

	flushTicker := time.NewTicker(10 * time.Millisecond)
	defer flushTicker.Stop()

	for {
		select {
		case <-s.stopChan:
			return
		case <-s.writeNotify:
			s.flushAllPending()
		case <-flushTicker.C:
			s.flushAllPending()
		case <-sealTicker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			s.checkSealsAndGC(ctx)
			cancel()
		}
	}
}

// StreamWatermarks describes the witness and S3 watermark sequence numbers for a stream.
type StreamWatermarks struct {
	WitnessSeq uint64
	S3Seq      uint64
}

// StreamWatermarks returns a snapshot of watermarks for all tracked streams.
func (s *StreamStore) StreamWatermarks() map[string]StreamWatermarks {
	s.mu.RLock()
	defer s.mu.RUnlock()
	res := make(map[string]StreamWatermarks, len(s.streams))
	for sid, w := range s.streams {
		w.mu.Lock()
		res[sid] = StreamWatermarks{
			WitnessSeq: w.lastSeq,
			S3Seq:      w.uploadedTo,
		}
		w.mu.Unlock()
	}
	return res
}

func (s *StreamStore) allWriters() []*streamWriter {
	s.mu.RLock()
	defer s.mu.RUnlock()
	res := make([]*streamWriter, 0, len(s.streams))
	for _, w := range s.streams {
		res = append(res, w)
	}
	return res
}

func (s *StreamStore) flushAllPending() {
	for _, w := range s.allWriters() {
		w.mu.Lock()
		if len(w.pending) > 0 {
			if err := w.flushPendingLocked(); err != nil {
				klog.Warningf("Failed to flush pending records for stream %s: %v", w.streamID, err)
			}
		}
		w.mu.Unlock()
	}
}

func (s *StreamStore) checkSealsAndGC(ctx context.Context) {
	for _, w := range s.allWriters() {
		w.mu.Lock()
		if err := w.flushPendingLocked(); err != nil {
			klog.Warningf("Failed to flush pending records for stream %s: %v", w.streamID, err)
		}
		if err := w.uploadPendingSealedLocked(ctx, s.backend); err != nil {
			klog.Warningf("Failed to upload pending sealed files for stream %s: %v", w.streamID, err)
		}
		if (w.recCount > 0) && (w.fileSize >= s.sealBytes || (!w.openTime.IsZero() && time.Since(w.openTime) >= s.sealInterval)) {
			if _, err := w.sealLocked(ctx, s.backend); err != nil {
				klog.Warningf("Failed to seal stream %s: %v", w.streamID, err)
			}
		}
		w.mu.Unlock()
	}

	if _, err := s.CheckSegmentGC(ctx); err != nil {
		klog.Warningf("Failed to run aggregated segment GC: %v", err)
	}
}

func (s *StreamStore) getOrCreateStream(streamID uuid.UUID) *streamWriter {
	s.mu.Lock()
	defer s.mu.Unlock()
	sid := streamID.String()
	w, exists := s.streams[sid]
	if !exists {
		w = &streamWriter{
			streamID: streamID,
			dir:      filepath.Join(s.dataDir, "streams", sid),
		}
		s.streams[sid] = w
	}
	return w
}

func (s *StreamStore) getStream(streamID uuid.UUID) *streamWriter {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.streams[streamID.String()]
}

// AppendRecords enqueues records to their respective per-stream in-memory buffers.
// Disk I/O is performed strictly off the critical path.
func (s *StreamStore) AppendRecords(records []*wal.LogRecord) {
	if len(records) == 0 {
		return
	}
	byStream := make(map[uuid.UUID][]*wal.LogRecord)
	for _, rec := range records {
		byStream[rec.StreamID] = append(byStream[rec.StreamID], rec)
	}

	for streamID, recs := range byStream {
		w := s.getOrCreateStream(streamID)
		w.appendRecords(recs)
	}

	select {
	case s.writeNotify <- struct{}{}:
	default:
	}
}

// SealStream seals and uploads the open file for streamID.
func (s *StreamStore) SealStream(ctx context.Context, streamID uuid.UUID) (*StreamMeta, error) {
	w := s.getOrCreateStream(streamID)
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.uploadPendingSealedLocked(ctx, s.backend); err != nil {
		klog.Warningf("Failed to upload pending sealed files for stream %s: %v", streamID, err)
	}
	return w.sealLocked(ctx, s.backend)
}

// SealAll flushes, seals, and uploads all active streams with records.
func (s *StreamStore) SealAll(ctx context.Context) ([]*StreamMeta, error) {
	var sealed []*StreamMeta
	var errs []error
	for _, w := range s.allWriters() {
		w.mu.Lock()
		if err := w.uploadPendingSealedLocked(ctx, s.backend); err != nil {
			errs = append(errs, err)
		}
		meta, err := w.sealLocked(ctx, s.backend)
		w.mu.Unlock()
		if err != nil {
			errs = append(errs, err)
		}
		if meta != nil {
			sealed = append(sealed, meta)
		}
	}
	return sealed, errors.Join(errs...)
}

// TrackSegmentCoverage records the highest stream sequences contained in an aggregated segment.
func (s *StreamStore) TrackSegmentCoverage(segPath string, records []*wal.LogRecord) {
	s.coverageMu.Lock()
	defer s.coverageMu.Unlock()

	reqs := make(map[string]uint64)
	for _, rec := range records {
		sid := rec.StreamID.String()
		if rec.StreamSeq > reqs[sid] {
			reqs[sid] = rec.StreamSeq
		}
	}
	s.segmentReqs[segPath] = reqs
}

// CheckSegmentGC deletes aggregated segments from object storage once all streams
// in the segment have sealed and uploaded files past their respective sequences in that segment.
func (s *StreamStore) CheckSegmentGC(ctx context.Context) ([]string, error) {
	s.coverageMu.Lock()
	defer s.coverageMu.Unlock()

	var deleted []string
	var errs []error
	for segPath, reqs := range s.segmentReqs {
		allCovered := true
		for sid, maxSeq := range reqs {
			id, err := uuid.Parse(sid)
			if err != nil {
				continue
			}
			w := s.getStream(id)
			if w == nil {
				allCovered = false
				break
			}
			w.mu.Lock()
			upTo := w.uploadedTo
			w.mu.Unlock()
			if upTo < maxSeq {
				allCovered = false
				break
			}
		}
		if allCovered && len(reqs) > 0 {
			if s.backend != nil {
				if err := s.backend.DeleteObject(ctx, "", segPath); err != nil {
					klog.Warningf("Failed to delete covered aggregated segment %s: %v", segPath, err)
					errs = append(errs, err)
					continue
				}
			}
			delete(s.segmentReqs, segPath)
			deleted = append(deleted, segPath)
			klog.Infof("Deleted aggregated WAL segment %s covered by per-stream files", segPath)
		}
	}
	return deleted, errors.Join(errs...)
}

// Recover scans local per-stream directories, recovers torn records, discovers
// remote sealed objects, and replays missing records from the aggregated log.
func (s *StreamStore) Recover(ctx context.Context, aggRecords []*wal.LogRecord) error {
	streamIDs := make(map[uuid.UUID]struct{})
	streamsDir := filepath.Join(s.dataDir, "streams")
	entries, err := os.ReadDir(streamsDir)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to read streams directory %s: %w", streamsDir, err)
	}
	for _, e := range entries {
		if e.IsDir() {
			id, err := uuid.Parse(e.Name())
			if err != nil {
				klog.Warningf("ignoring non-UUID directory in %s: %s: %v", streamsDir, e.Name(), err)
				continue
			}
			streamIDs[id] = struct{}{}
		}
	}
	for _, rec := range aggRecords {
		streamIDs[rec.StreamID] = struct{}{}
	}

	var errs []error
	for id := range streamIDs {
		w := s.getOrCreateStream(id)
		if err := w.recoverStream(ctx, s.backend, aggRecords); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// TrimStreamHistory deletes sealed per-stream files in object storage whose records are all <= trimSeq,
// strictly guarded by snapshotSeq. Default policy is to retain all history; this is only invoked
// when an explicit retention policy is configured.
func (s *StreamStore) TrimStreamHistory(ctx context.Context, streamID uuid.UUID, trimSeq, snapshotSeq uint64) ([]string, error) {
	if snapshotSeq == 0 || trimSeq == 0 {
		return nil, nil
	}
	if trimSeq > snapshotSeq {
		trimSeq = snapshotSeq
	}

	w := s.getStream(streamID)
	if w == nil {
		return nil, nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	var deleted []string
	var remaining []*StreamMeta
	var errs []error
	for _, sm := range w.sealed {
		if sm.ToSeq <= trimSeq && sm.ToSeq <= snapshotSeq && sm.Key != "" {
			if s.backend != nil {
				if err := s.backend.DeleteObject(ctx, "", sm.Key); err != nil {
					klog.Warningf("Failed to delete trimmed stream file %s: %v", sm.Key, err)
					errs = append(errs, err)
					remaining = append(remaining, sm)
					continue
				}
			}
			deleted = append(deleted, sm.Key)
		} else {
			remaining = append(remaining, sm)
		}
	}
	w.sealed = remaining
	return deleted, errors.Join(errs...)
}

// Close gracefully closes the periodic worker and flushes pending in-memory records.
func (s *StreamStore) Close(ctx context.Context) error {
	close(s.stopChan)
	s.wg.Wait()

	var firstErr error
	for _, w := range s.allWriters() {
		w.mu.Lock()
		if err := w.flushPendingLocked(); err != nil && firstErr == nil {
			firstErr = err
		}
		w.mu.Unlock()
	}
	return firstErr
}
