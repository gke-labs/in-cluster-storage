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

package wal

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gke-labs/in-cluster-storage/pkg/errutil"
	"github.com/google/uuid"
	"k8s.io/klog/v2"
)

const (
	// DefaultMaxSegmentSize is 64 MB.
	DefaultMaxSegmentSize int64 = 64 * 1024 * 1024

	// DefaultMaxRetainedBytes is 256 MB.
	DefaultMaxRetainedBytes int64 = 256 * 1024 * 1024
)

// SegmentMeta tracks metadata of a single local WAL segment file.
type SegmentMeta struct {
	Path        string
	FirstSeq    uint64
	LastSeq     uint64
	Size        int64
	RecordCount int
}

// backupCorruptedSegment copies the file to <name>.<timestamp>.recover before truncation.
func backupCorruptedSegment(path string) {
	recoverPath := fmt.Sprintf("%s.%d.recover", path, time.Now().UnixNano())
	data, err := os.ReadFile(path)
	if err != nil {
		klog.Warningf("failed to read corrupted segment file %s for backup: %v", path, err)
		return
	}
	if writeErr := os.WriteFile(recoverPath, data, 0644); writeErr != nil {
		klog.Warningf("failed to write corrupted segment backup to %s: %v", recoverPath, writeErr)
		return
	}
	klog.Infof("Saved copy of corrupted segment file to %s", recoverPath)
}

// ScanClientSegmentFile reads a client segment file from disk, validating all records.
// If a torn trailing record is detected at the end of the file, it backs up the file,
// truncates to the last valid record boundary, and returns only the valid records.
func ScanClientSegmentFile(path string, opts ...ScanOption) (records []*ClientRecord, meta *SegmentMeta, err error) {
	var opt ScanOptions
	for _, o := range opts {
		o(&opt)
	}

	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open segment file %s: %w", path, err)
	}
	defer errutil.CloseWithErr(f, &err)

	_, hdrOffset, err := ReadSegmentHeader(f, opt.AcceptUnversioned)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, &SegmentMeta{Path: path, Size: 0, RecordCount: 0}, nil
		}
		return nil, nil, fmt.Errorf("failed to read segment header %s: %w", path, err)
	}

	var validRecords []*ClientRecord
	validOffset := hdrOffset

	for {
		rec, decErr := DecodeClientRecord(f)
		if decErr != nil {
			if errors.Is(decErr, io.EOF) {
				break
			}
			if errors.Is(decErr, ErrTruncatedRecord) || errors.Is(decErr, ErrChecksumMismatch) || errors.Is(decErr, ErrCorruptedRecord) {
				klog.Warningf("Torn trailing record detected in client segment %s at offset %d (%v), creating recover backup and truncating file", path, validOffset, decErr)
				backupCorruptedSegment(path)
				if err := f.Truncate(validOffset); err != nil {
					return nil, nil, fmt.Errorf("failed to truncate segment file %s at %d: %w", path, validOffset, err)
				}
				if err := f.Sync(); err != nil {
					return nil, nil, fmt.Errorf("failed to sync truncated segment file %s: %w", path, err)
				}
				break
			}
			return nil, nil, fmt.Errorf("read error while scanning segment file %s: %w", path, decErr)
		}

		validRecords = append(validRecords, rec)
		validOffset += int64(ClientHeaderSize + len(rec.Payload))
	}

	meta = &SegmentMeta{
		Path:        path,
		Size:        validOffset,
		RecordCount: len(validRecords),
	}
	if len(validRecords) > 0 {
		meta.FirstSeq = validRecords[0].StreamSeq
		meta.LastSeq = validRecords[len(validRecords)-1].StreamSeq
	}

	return validRecords, meta, nil
}

// ScanLogSegmentFile reads a witness log segment file from disk, validating all records.
// If a torn trailing record is detected, it backs up the file, truncates, and returns valid records.
func ScanLogSegmentFile(path string, opts ...ScanOption) (records []*LogRecord, meta *SegmentMeta, err error) {
	var opt ScanOptions
	for _, o := range opts {
		o(&opt)
	}

	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open segment file %s: %w", path, err)
	}
	defer errutil.CloseWithErr(f, &err)

	_, hdrOffset, err := ReadSegmentHeader(f, opt.AcceptUnversioned)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, &SegmentMeta{Path: path, Size: 0, RecordCount: 0}, nil
		}
		return nil, nil, fmt.Errorf("failed to read segment header %s: %w", path, err)
	}

	var validRecords []*LogRecord
	validOffset := hdrOffset

	for {
		rec, decErr := DecodeLogRecord(f)
		if decErr != nil {
			if errors.Is(decErr, io.EOF) {
				break
			}
			if errors.Is(decErr, ErrTruncatedRecord) || errors.Is(decErr, ErrChecksumMismatch) || errors.Is(decErr, ErrCorruptedRecord) {
				klog.Warningf("Torn trailing record detected in log segment %s at offset %d (%v), creating recover backup and truncating file", path, validOffset, decErr)
				backupCorruptedSegment(path)
				if err := f.Truncate(validOffset); err != nil {
					return nil, nil, fmt.Errorf("failed to truncate segment file %s at %d: %w", path, validOffset, err)
				}
				if err := f.Sync(); err != nil {
					return nil, nil, fmt.Errorf("failed to sync truncated segment file %s: %w", path, err)
				}
				break
			}
			return nil, nil, fmt.Errorf("read error while scanning segment file %s: %w", path, decErr)
		}

		validRecords = append(validRecords, rec)
		validOffset += int64(LogHeaderSize + len(rec.Payload))
	}

	meta = &SegmentMeta{
		Path:        path,
		Size:        validOffset,
		RecordCount: len(validRecords),
	}
	if len(validRecords) > 0 {
		meta.FirstSeq = validRecords[0].Position
		meta.LastSeq = validRecords[len(validRecords)-1].Position
	}

	return validRecords, meta, nil
}

// ClientSegmentStore manages local segment files for a client.
type ClientSegmentStore struct {
	mu                sync.RWMutex
	dir               string
	maxSegmentSize    int64
	filePrefix        string
	acceptUnversioned bool

	activeFile *os.File
	activeMeta *SegmentMeta
	segments   []*SegmentMeta
}

// NewClientSegmentStore initializes or reopens a client segment store.
func NewClientSegmentStore(dir string, filePrefix string, maxSegmentSize int64, opts ...ScanOption) (*ClientSegmentStore, []*ClientRecord, error) {
	var opt ScanOptions
	for _, o := range opts {
		o(&opt)
	}

	if maxSegmentSize <= 0 {
		maxSegmentSize = DefaultMaxSegmentSize
	}
	if filePrefix == "" {
		filePrefix = "stream"
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, nil, fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read directory %s: %w", dir, err)
	}

	var walFiles []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), filePrefix+"-") && strings.HasSuffix(entry.Name(), ".wal") {
			walFiles = append(walFiles, filepath.Join(dir, entry.Name()))
		}
	}
	sort.Strings(walFiles)

	store := &ClientSegmentStore{
		dir:               dir,
		maxSegmentSize:    maxSegmentSize,
		filePrefix:        filePrefix,
		acceptUnversioned: opt.AcceptUnversioned,
	}

	var allRecords []*ClientRecord
	var removeErrs []error
	for _, file := range walFiles {
		records, meta, err := ScanClientSegmentFile(file, opts...)
		if err != nil {
			return nil, nil, err
		}
		if meta.RecordCount > 0 {
			store.segments = append(store.segments, meta)
			allRecords = append(allRecords, records...)
		} else {
			if err := os.Remove(file); err != nil && !os.IsNotExist(err) {
				removeErrs = append(removeErrs, fmt.Errorf("failed to remove empty segment file %s: %w", file, err))
			}
		}
	}
	if len(removeErrs) > 0 {
		return nil, nil, errors.Join(removeErrs...)
	}

	return store, allRecords, nil
}

func (s *ClientSegmentStore) segmentFilename(firstSeq uint64) string {
	return filepath.Join(s.dir, fmt.Sprintf("%s-%019d.wal", s.filePrefix, firstSeq))
}

// Append writes and fsyncs a single ClientRecord.
func (s *ClientSegmentStore) Append(rec *ClientRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	encoded := rec.Encode()
	recordLen := int64(len(encoded))

	if s.activeFile == nil || (s.activeMeta.Size+recordLen > s.maxSegmentSize && s.activeMeta.RecordCount > 0) {
		if err := s.rotateLocked(rec.StreamSeq); err != nil {
			return err
		}
	}

	if _, err := s.activeFile.Write(encoded); err != nil {
		return fmt.Errorf("failed writing to segment file %s: %w", s.activeMeta.Path, err)
	}

	if err := s.activeFile.Sync(); err != nil {
		return fmt.Errorf("failed syncing segment file %s: %w", s.activeMeta.Path, err)
	}

	if s.activeMeta.RecordCount == 0 {
		s.activeMeta.FirstSeq = rec.StreamSeq
	}
	s.activeMeta.LastSeq = rec.StreamSeq
	s.activeMeta.Size += recordLen
	s.activeMeta.RecordCount++

	return nil
}

func (s *ClientSegmentStore) rotateLocked(firstSeq uint64) error {
	if s.activeFile != nil {
		syncErr := s.activeFile.Sync()
		closeErr := s.activeFile.Close()
		s.activeFile = nil
		if err := errors.Join(syncErr, closeErr); err != nil {
			return fmt.Errorf("failed to sync/close previous segment file: %w", err)
		}
	}

	path := s.segmentFilename(firstSeq)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to create new segment file %s: %w", path, err)
	}

	hdr := DefaultSegmentHeader().Encode()
	if _, err := f.Write(hdr); err != nil {
		errutil.CloseAndLog(f, fmt.Sprintf("segment file %s", path))
		return fmt.Errorf("failed to write header to segment file %s: %w", path, err)
	}

	meta := &SegmentMeta{
		Path:     path,
		FirstSeq: firstSeq,
		LastSeq:  firstSeq,
		Size:     int64(len(hdr)),
	}

	s.activeFile = f
	s.activeMeta = meta
	s.segments = append(s.segments, meta)
	return nil
}

// TruncateAfter removes all records with StreamSeq > maxSeq from the segment store and disk.
func (s *ClientSegmentStore) TruncateAfter(maxSeq uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var errs []error
	if s.activeFile != nil {
		syncErr := s.activeFile.Sync()
		closeErr := s.activeFile.Close()
		s.activeFile = nil
		s.activeMeta = nil
		if err := errors.Join(syncErr, closeErr); err != nil {
			errs = append(errs, fmt.Errorf("failed to sync/close active segment file: %w", err))
		}
	}

	var remaining []*SegmentMeta
	for _, seg := range s.segments {
		if seg.FirstSeq > maxSeq {
			// Entire segment is after maxSeq -> delete file
			if err := os.Remove(seg.Path); err != nil && !os.IsNotExist(err) {
				errs = append(errs, fmt.Errorf("failed to remove segment file %s: %w", seg.Path, err))
				remaining = append(remaining, seg)
			}
			continue
		}

		if seg.LastSeq <= maxSeq {
			// Entire segment is within maxSeq -> keep as is
			remaining = append(remaining, seg)
			continue
		}

		// Partial segment: scan records, find offset where StreamSeq <= maxSeq, truncate file
		f, err := os.OpenFile(seg.Path, os.O_RDWR, 0644)
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to open segment file %s for truncate: %w", seg.Path, err))
			remaining = append(remaining, seg)
			continue
		}

		_, hdrOffset, err := ReadSegmentHeader(f, s.acceptUnversioned)
		if err != nil {
			errutil.CloseAndLog(f, fmt.Sprintf("segment file %s", seg.Path))
			errs = append(errs, fmt.Errorf("failed to read segment header %s: %w", seg.Path, err))
			remaining = append(remaining, seg)
			continue
		}

		validOffset := hdrOffset
		var validCount int
		var firstSeq, lastSeq uint64

		for {
			rec, err := DecodeClientRecord(f)
			if err != nil || rec.StreamSeq > maxSeq {
				break
			}
			if validCount == 0 {
				firstSeq = rec.StreamSeq
			}
			lastSeq = rec.StreamSeq
			validCount++
			validOffset += int64(ClientHeaderSize + len(rec.Payload))
		}

		if validCount == 0 {
			if closeErr := f.Close(); closeErr != nil {
				errs = append(errs, fmt.Errorf("failed to close segment file %s: %w", seg.Path, closeErr))
			}
			if remErr := os.Remove(seg.Path); remErr != nil && !os.IsNotExist(remErr) {
				errs = append(errs, fmt.Errorf("failed to remove segment file %s: %w", seg.Path, remErr))
				remaining = append(remaining, seg)
			}
			continue
		}

		if err := f.Truncate(validOffset); err != nil {
			errutil.CloseAndLog(f, fmt.Sprintf("segment file %s", seg.Path))
			errs = append(errs, fmt.Errorf("failed to truncate segment file %s to %d: %w", seg.Path, validOffset, err))
			remaining = append(remaining, seg)
			continue
		}
		syncErr := f.Sync()
		closeErr := f.Close()
		if err := errors.Join(syncErr, closeErr); err != nil {
			errs = append(errs, fmt.Errorf("failed to sync/close segment file %s after truncate: %w", seg.Path, err))
			remaining = append(remaining, seg)
			continue
		}

		seg.Size = validOffset
		seg.RecordCount = validCount
		seg.FirstSeq = firstSeq
		seg.LastSeq = lastSeq
		remaining = append(remaining, seg)
	}

	s.segments = remaining
	return errors.Join(errs...)
}

// DeleteSegmentsBeforeS3Ack deletes completed segment files whose records are all <= s3AckedSeq.
func (s *ClientSegmentStore) DeleteSegmentsBeforeS3Ack(s3AckedSeq uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var remaining []*SegmentMeta
	for _, seg := range s.segments {
		if seg == s.activeMeta {
			remaining = append(remaining, seg)
			continue
		}

		if seg.LastSeq <= s3AckedSeq && seg.RecordCount > 0 {
			if err := os.Remove(seg.Path); err != nil && !os.IsNotExist(err) {
				klog.Warningf("Failed to remove old segment file %s: %v", seg.Path, err)
				remaining = append(remaining, seg)
				continue
			}
		} else {
			remaining = append(remaining, seg)
		}
	}
	s.segments = remaining
	return nil
}

// Close syncs and closes the active file.
func (s *ClientSegmentStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.activeFile != nil {
		syncErr := s.activeFile.Sync()
		closeErr := s.activeFile.Close()
		s.activeFile = nil
		return errors.Join(syncErr, closeErr)
	}
	return nil
}

// LogSegmentStore manages local segment cache for the buffer service.
type LogSegmentStore struct {
	mu                sync.RWMutex
	dir               string
	maxSegmentSize    int64
	filePrefix        string
	acceptUnversioned bool

	activeFile *os.File
	activeMeta *SegmentMeta
	segments   []*SegmentMeta
}

// NewLogSegmentStore initializes a log segment store with a fixed prefix.
// Any segment files from previous incarnations in dir are cleaned up on startup.
func NewLogSegmentStore(dir string, filePrefix string, maxSegmentSize int64, opts ...ScanOption) (*LogSegmentStore, error) {
	var opt ScanOptions
	for _, o := range opts {
		o(&opt)
	}

	if maxSegmentSize <= 0 {
		maxSegmentSize = DefaultMaxSegmentSize
	}
	if filePrefix == "" {
		filePrefix = "log"
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory %s: %w", dir, err)
	}

	var removeErrs []error
	for _, entry := range entries {
		if !entry.IsDir() && (strings.HasSuffix(entry.Name(), ".wal") || strings.Contains(entry.Name(), ".wal.")) {
			p := filepath.Join(dir, entry.Name())
			if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
				removeErrs = append(removeErrs, fmt.Errorf("failed to remove stale segment file %s: %w", p, err))
			}
		}
	}
	if len(removeErrs) > 0 {
		return nil, errors.Join(removeErrs...)
	}

	store := &LogSegmentStore{
		dir:               dir,
		maxSegmentSize:    maxSegmentSize,
		filePrefix:        filePrefix,
		acceptUnversioned: opt.AcceptUnversioned,
	}

	return store, nil
}

func (s *LogSegmentStore) segmentFilename(firstPos uint64) string {
	return filepath.Join(s.dir, fmt.Sprintf("%s-%019d.wal", s.filePrefix, firstPos))
}

// AppendBatch writes a slice of LogRecords in a batch with a single fsync.
func (s *LogSegmentStore) AppendBatch(records []*LogRecord) error {
	if len(records) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, rec := range records {
		encoded := rec.Encode()
		recordLen := int64(len(encoded))

		if s.activeFile == nil || (s.activeMeta.Size+recordLen > s.maxSegmentSize && s.activeMeta.RecordCount > 0) {
			if err := s.rotateLocked(rec.Position); err != nil {
				return err
			}
		}

		if _, err := s.activeFile.Write(encoded); err != nil {
			return fmt.Errorf("failed writing to segment file %s: %w", s.activeMeta.Path, err)
		}

		if s.activeMeta.RecordCount == 0 {
			s.activeMeta.FirstSeq = rec.Position
		}
		s.activeMeta.LastSeq = rec.Position
		s.activeMeta.Size += recordLen
		s.activeMeta.RecordCount++
	}

	if s.activeFile != nil {
		if err := s.activeFile.Sync(); err != nil {
			return fmt.Errorf("failed syncing segment file %s: %w", s.activeMeta.Path, err)
		}
	}

	return nil
}

func (s *LogSegmentStore) rotateLocked(firstPos uint64) error {
	if s.activeFile != nil {
		syncErr := s.activeFile.Sync()
		closeErr := s.activeFile.Close()
		s.activeFile = nil
		if err := errors.Join(syncErr, closeErr); err != nil {
			return fmt.Errorf("failed to sync/close previous segment file: %w", err)
		}
	}

	path := s.segmentFilename(firstPos)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to create new segment file %s: %w", path, err)
	}

	hdr := DefaultSegmentHeader().Encode()
	if _, err := f.Write(hdr); err != nil {
		errutil.CloseAndLog(f, fmt.Sprintf("segment file %s", path))
		return fmt.Errorf("failed to write header to segment file %s: %w", path, err)
	}

	meta := &SegmentMeta{
		Path:     path,
		FirstSeq: firstPos,
		LastSeq:  firstPos,
		Size:     int64(len(hdr)),
	}

	s.activeFile = f
	s.activeMeta = meta
	s.segments = append(s.segments, meta)
	return nil
}

// ReadFrom scans log segment files starting from the segment containing or preceding position,
// decodes LogRecords, and invokes fn for every record where Position >= position.
// It tolerates the active file growing while being read by stopping at the last fully-written record.
func (s *LogSegmentStore) ReadFrom(position uint64, fn func(*LogRecord) error) error {
	s.mu.RLock()
	if len(s.segments) == 0 {
		s.mu.RUnlock()
		return nil
	}

	segs := make([]*SegmentMeta, len(s.segments))
	copy(segs, s.segments)
	s.mu.RUnlock()

	// Binary search for the first segment that could contain position
	idx := sort.Search(len(segs), func(i int) bool {
		return segs[i].FirstSeq > position
	})
	startIdx := 0
	if idx > 0 {
		startIdx = idx - 1
	}

	for _, seg := range segs[startIdx:] {
		if err := func() error {
			f, err := os.Open(seg.Path)
			if err != nil {
				if os.IsNotExist(err) {
					return nil
				}
				return fmt.Errorf("failed to open segment %s: %w", seg.Path, err)
			}
			defer errutil.CloseAndLog(f, fmt.Sprintf("segment %s", seg.Path))

			if _, _, err := ReadSegmentHeader(f, s.acceptUnversioned); err != nil && !errors.Is(err, io.EOF) {
				return fmt.Errorf("failed to read segment header %s: %w", seg.Path, err)
			}

			for {
				rec, err := DecodeLogRecord(f)
				if err != nil {
					if errors.Is(err, io.EOF) || errors.Is(err, ErrTruncatedRecord) || errors.Is(err, io.ErrUnexpectedEOF) {
						break
					}
					return fmt.Errorf("failed decoding log record in %s: %w", seg.Path, err)
				}
				if rec.Position >= position {
					if err := fn(rec); err != nil {
						return err
					}
				}
			}
			return nil
		}(); err != nil {
			return err
		}
	}

	return nil
}

// DeleteSegmentsThrough removes flushed segment files to stay within maxRetainedBytes.
// A segment file is eligible for deletion only if it is not the active segment and all its records
// are <= flushedPosition (i.e. LastSeq <= flushedPosition).
// A segment containing any unflushed record is never deleted.
func (s *LogSegmentStore) DeleteSegmentsThrough(flushedPosition uint64, maxRetainedBytes int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var totalBytes int64
	for _, seg := range s.segments {
		totalBytes += seg.Size
	}

	if totalBytes <= maxRetainedBytes {
		return nil
	}

	var remaining []*SegmentMeta
	for _, seg := range s.segments {
		// Never delete active file or segments containing unflushed records
		if seg == s.activeMeta || seg.LastSeq > flushedPosition || totalBytes <= maxRetainedBytes {
			remaining = append(remaining, seg)
			continue
		}

		if err := os.Remove(seg.Path); err != nil && !os.IsNotExist(err) {
			klog.Warningf("Failed to remove flushed segment file %s: %v", seg.Path, err)
			remaining = append(remaining, seg)
			continue
		}
		totalBytes -= seg.Size
	}
	s.segments = remaining
	return nil
}

// Close syncs and closes the active file.
func (s *LogSegmentStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.activeFile != nil {
		syncErr := s.activeFile.Sync()
		closeErr := s.activeFile.Close()
		s.activeFile = nil
		return errors.Join(syncErr, closeErr)
	}
	return nil
}

// ParseSegmentPath parses a segment object path into (firstPosition, lastPosition).
// Format: streams/wal/segments/<first_position>-<last_position>.wal
func ParseSegmentPath(p string) (firstPos, lastPos uint64, err error) {
	base := filepath.Base(p)
	base = strings.TrimSuffix(base, ".wal")
	parts := strings.Split(base, "-")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid segment filename %s", p)
	}

	firstPos, err = strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid first position in segment filename %s: %w", p, err)
	}
	lastPos, err = strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid last position in segment filename %s: %w", p, err)
	}
	return firstPos, lastPos, nil
}

// ParseStreamPath parses a per-stream file path or filename into (fromSeq, toSeq, isSealed, err).
// Sealed format: streams/<stream-id>/from<from>-to<to>.wal
// Open format: streams/<stream-id>/from<from>.wal
func ParseStreamPath(p string) (fromSeq, toSeq uint64, isSealed bool, err error) {
	clean := strings.TrimPrefix(filepath.ToSlash(p), "./")
	clean = strings.TrimPrefix(clean, "/")
	if strings.HasPrefix(clean, "streams/wal/") || clean == "streams/wal" {
		return 0, 0, false, fmt.Errorf("reserved prefix streams/wal: %s", p)
	}
	base := filepath.Base(p)
	if !strings.HasSuffix(base, ".wal") {
		return 0, 0, false, fmt.Errorf("invalid stream file extension %s", p)
	}
	stem := strings.TrimSuffix(base, ".wal")
	if idx := strings.Index(stem, "from"); idx != -1 {
		stem = stem[idx:]
	}
	if !strings.HasPrefix(stem, "from") {
		return 0, 0, false, fmt.Errorf("invalid stream file format %s", p)
	}
	stem = strings.TrimPrefix(stem, "from")
	if strings.Contains(stem, "-to") {
		parts := strings.Split(stem, "-to")
		if len(parts) != 2 {
			return 0, 0, false, fmt.Errorf("invalid sealed stream file format %s", p)
		}
		from, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil {
			return 0, 0, false, fmt.Errorf("invalid from sequence in %s: %w", p, err)
		}
		to, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			return 0, 0, false, fmt.Errorf("invalid to sequence in %s: %w", p, err)
		}
		return from, to, true, nil
	}
	from, err := strconv.ParseUint(stem, 10, 64)
	if err != nil {
		return 0, 0, false, fmt.Errorf("invalid from sequence in %s: %w", p, err)
	}
	return from, 0, false, nil
}

// FormatStreamFileName formats a per-stream filename.
func FormatStreamFileName(fromSeq, toSeq uint64, isSealed bool) string {
	if isSealed {
		return fmt.Sprintf("from%020d-to%020d.wal", fromSeq, toSeq)
	}
	return fmt.Sprintf("from%020d.wal", fromSeq)
}

// FormatStreamObjectKey formats the object storage key for a sealed per-stream file.
func FormatStreamObjectKey(streamID uuid.UUID, fromSeq, toSeq uint64) string {
	return fmt.Sprintf("streams/%s/from%020d-to%020d.wal", streamID.String(), fromSeq, toSeq)
}
