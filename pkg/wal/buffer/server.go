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
	"path/filepath"
	"sync"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/wal/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/errutil"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/blob"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore"
	"github.com/gke-labs/in-cluster-storage/pkg/wal"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
)

const (
	DefaultFlushInterval = 60 * time.Second
	DefaultFlushBytes    = 64 * 1024 * 1024
	DefaultTailCache     = 64 * 1024 * 1024
	DefaultBatchDelay    = 5 * time.Millisecond
	DefaultBatchSize     = 1024 * 1024
)

// ServerConfig configures the WAL Buffer service.
type ServerConfig struct {
	Backend            objectstore.Backend
	DataDir            string
	FlushInterval      time.Duration
	FlushBytes         int64
	TailCacheBytes     int64
	BatchMaxDelay      time.Duration
	BatchMaxSize       int64
	StreamSealInterval time.Duration
	StreamSealBytes    int64
	AcceptUnversioned  bool
}

type incomingItem struct {
	streamID  uuid.UUID
	streamSeq uint64
	payload   []byte
	ackChan   chan ackResult
}

type ackResult struct {
	witnessSeq uint64
	s3Seq      uint64
	err        error
}

type streamState struct {
	mu         sync.Mutex
	cond       *sync.Cond
	witnessSeq uint64
	s3Seq      uint64
}

func newStreamState(witnessSeq, s3Seq uint64) *streamState {
	st := &streamState{
		witnessSeq: witnessSeq,
		s3Seq:      s3Seq,
	}
	st.cond = sync.NewCond(&st.mu)
	return st
}

// Server implements the WalBuffer gRPC service.
type Server struct {
	pb.UnimplementedWalBufferServer

	cfg     ServerConfig
	backend objectstore.Backend

	mu                  sync.RWMutex
	lastPosition        uint64
	lastFlushedPosition uint64
	flushedSegments     []string
	streams             map[string]*streamState // streamID string -> streamState
	streamStore         *StreamStore

	localStore *wal.LogSegmentStore

	incomingChan chan incomingItem

	unflushedMu      sync.Mutex
	unflushedRecords []*wal.LogRecord
	unflushedBytes   int64

	// Tail broadcasting
	tailMu      sync.RWMutex
	tailWaiters map[chan struct{}]struct{}

	flushMu    sync.Mutex
	flushCond  *sync.Cond
	isFlushing bool
	flushSeq   uint64 // incremented on each successful flush

	stopChan chan struct{}
	wg       sync.WaitGroup
}

// NewServer creates and initializes a new WAL buffer Server.
func NewServer(ctx context.Context, cfg ServerConfig) (*Server, error) {
	if cfg.Backend == nil {
		return nil, errors.New("backend cannot be nil")
	}
	if cfg.DataDir == "" {
		return nil, errors.New("DataDir is required")
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = DefaultFlushInterval
	}
	if cfg.FlushBytes <= 0 {
		cfg.FlushBytes = DefaultFlushBytes
	}
	if cfg.TailCacheBytes <= 0 {
		cfg.TailCacheBytes = DefaultTailCache
	}
	if cfg.BatchMaxDelay <= 0 {
		cfg.BatchMaxDelay = DefaultBatchDelay
	}
	if cfg.BatchMaxSize <= 0 {
		cfg.BatchMaxSize = DefaultBatchSize
	}
	if cfg.StreamSealInterval <= 0 {
		cfg.StreamSealInterval = DefaultStreamSealInterval
	}
	if cfg.StreamSealBytes <= 0 {
		cfg.StreamSealBytes = DefaultStreamSealBytes
	}

	dataDir := cfg.DataDir
	streamStore := NewStreamStore(dataDir, cfg.Backend, cfg.StreamSealInterval, cfg.StreamSealBytes)
	if cfg.AcceptUnversioned {
		streamStore.SetAcceptUnversioned(true)
	}

	// 1. Discover existing segments from backend
	discoveredSegments, lastFlushedPos, err := ListSegmentsFromBackend(ctx, cfg.Backend)
	if err != nil {
		return nil, fmt.Errorf("failed to list segments from backend: %w", err)
	}

	var allDiscoveredRecords []*wal.LogRecord
	backendWatermarks := make(map[string]uint64)
	for _, segPath := range discoveredSegments {
		records, err := ReadSegmentFromBackend(ctx, cfg.Backend, segPath, wal.WithAcceptUnversioned(cfg.AcceptUnversioned))
		if err != nil {
			return nil, fmt.Errorf("failed to recover stream watermarks from segment %s: %w", segPath, err)
		}
		streamStore.TrackSegmentCoverage(segPath, records)
		allDiscoveredRecords = append(allDiscoveredRecords, records...)
		for _, rec := range records {
			sid := rec.StreamID.String()
			if rec.StreamSeq > backendWatermarks[sid] {
				backendWatermarks[sid] = rec.StreamSeq
			}
		}
	}

	// 2. Setup local segment store and recover local segments
	localStore, localRecords, err := wal.NewLogSegmentStore(dataDir, "log", cfg.FlushBytes, wal.WithAcceptUnversioned(cfg.AcceptUnversioned))
	if err != nil {
		return nil, fmt.Errorf("failed to initialize local segment store: %w", err)
	}

	localWatermarks := make(map[string]uint64)
	localMaxPos := lastFlushedPos
	for _, rec := range localRecords {
		sid := rec.StreamID.String()
		if rec.StreamSeq > localWatermarks[sid] {
			localWatermarks[sid] = rec.StreamSeq
		}
		if rec.Position > localMaxPos {
			localMaxPos = rec.Position
		}
	}

	// Build combined stream watermarks across backend and local segments
	allStreams := make(map[string]struct{})
	for sid := range backendWatermarks {
		allStreams[sid] = struct{}{}
	}
	for sid := range localWatermarks {
		allStreams[sid] = struct{}{}
	}

	streamWatermarks := make(map[string]uint64)
	for sid := range allStreams {
		lHead := localWatermarks[sid]
		fHead := backendWatermarks[sid]
		watermark := lHead
		if fHead > watermark {
			watermark = fHead
		}
		streamWatermarks[sid] = watermark
		klog.Infof("WAL buffer: recovered stream %s: local head=%d, flushed head=%d, resulting watermark=%d",
			sid, lHead, fHead, watermark)
	}

	// StreamStore recovery includes all discovered records (backend + local)
	allDiscoveredRecords = append(allDiscoveredRecords, localRecords...)
	if err := streamStore.Recover(ctx, allDiscoveredRecords); err != nil {
		klog.Warningf("Error during stream store recovery: %v", err)
	}

	nextPosition := localMaxPos + 1

	klog.Infof("WAL Buffer initializing: assigning positions starting at %d (last_flushed_position=%d, local_max_position=%d, segments=%d, streams=%d)",
		nextPosition, lastFlushedPos, localMaxPos, len(discoveredSegments), len(streamWatermarks))

	var unflushed []*wal.LogRecord
	var unflushedBytes int64
	for _, rec := range localRecords {
		if rec.Position > lastFlushedPos {
			unflushed = append(unflushed, rec)
			unflushedBytes += int64(wal.LogHeaderSize + len(rec.Payload))
		}
	}

	s := &Server{
		cfg:                 cfg,
		backend:             cfg.Backend,
		lastPosition:        localMaxPos,
		lastFlushedPosition: lastFlushedPos,
		flushedSegments:     discoveredSegments,
		streams:             make(map[string]*streamState),
		streamStore:         streamStore,
		localStore:          localStore,
		unflushedRecords:    unflushed,
		unflushedBytes:      unflushedBytes,
		incomingChan:        make(chan incomingItem, 1024),
		tailWaiters:         make(map[chan struct{}]struct{}),
		stopChan:            make(chan struct{}),
	}
	s.flushCond = sync.NewCond(&s.flushMu)

	// Populate initial streams state
	for sid, watermark := range streamWatermarks {
		s.streams[sid] = newStreamState(watermark, backendWatermarks[sid])
	}

	// Flush the local-only tail before serving
	if len(unflushed) > 0 {
		if _, err := s.flushInternal(ctx); err != nil {
			klog.Warningf("Failed to flush local-only tail during startup recovery: %v", err)
		}
	}

	s.checkAggregatedSegmentGC(ctx)

	// Start background group commit worker
	s.wg.Add(1)
	go s.groupCommitLoop()

	// Start background flush worker
	if cfg.FlushInterval > 0 {
		s.wg.Add(1)
		go s.periodicFlushLoop(cfg.FlushInterval)
	}

	return s, nil
}

// Close gracefully stops workers, flushes to permanent storage, and closes local stores.
func (s *Server) Close() error {
	close(s.stopChan)
	// Wake any stream Append notification forwarders waiting on st.cond so they
	// observe s.stopChan closure and exit their goroutine.
	s.mu.RLock()
	for _, st := range s.streams {
		st.mu.Lock()
		st.cond.Broadcast()
		st.mu.Unlock()
	}
	s.mu.RUnlock()
	s.wg.Wait()

	var errs []error

	// Perform a final flush on shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := s.flushInternal(ctx); err != nil {
		klog.Warningf("Error during shutdown flush: %v", err)
		errs = append(errs, err)
	}

	if err := s.streamStore.Close(ctx); err != nil {
		klog.Warningf("Error closing stream store: %v", err)
		errs = append(errs, err)
	}

	if s.localStore != nil {
		if err := s.localStore.Close(); err != nil {
			klog.Warningf("Error closing local store: %v", err)
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// LastPosition returns current lastPosition assigned by this buffer server.
func (s *Server) LastPosition() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastPosition
}

// LastFlushedPosition returns the latest position flushed to permanent storage.
func (s *Server) LastFlushedPosition() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastFlushedPosition
}

func (s *Server) getOrCreateStream(streamID uuid.UUID) *streamState {
	s.mu.Lock()
	defer s.mu.Unlock()

	sid := streamID.String()
	st, exists := s.streams[sid]
	if !exists {
		st = newStreamState(0, 0)
		s.streams[sid] = st
	}
	return st
}

// Append handles the bidirectional client append stream.
func (s *Server) Append(stream pb.WalBuffer_AppendServer) error {
	ctx := stream.Context()

	// 1. First message MUST be Hello
	firstReq, err := stream.Recv()
	if err != nil {
		return err
	}
	hello := firstReq.GetHello()
	if hello == nil {
		return status.Errorf(codes.InvalidArgument, "first message must be Hello")
	}

	if len(hello.StreamId) != 16 {
		return status.Errorf(codes.InvalidArgument, "stream_id must be exactly 16 bytes")
	}
	var streamID uuid.UUID
	copy(streamID[:], hello.StreamId)

	st := s.getOrCreateStream(streamID)

	st.mu.Lock()
	witnessSeq := st.witnessSeq
	s3Seq := st.s3Seq
	st.mu.Unlock()

	klog.Infof("WAL buffer: Hello received for stream %s: witness watermark=%d, s3 watermark=%d", streamID, witnessSeq, s3Seq)

	// Deduped / single sender channel to prevent concurrent stream.Send calls
	outCh := make(chan *pb.AppendResponse, 64)
	sendErrCh := make(chan error, 1)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-s.stopChan:
				return
			case resp, ok := <-outCh:
				if !ok {
					return
				}
				if err := stream.Send(resp); err != nil {
					select {
					case sendErrCh <- err:
					default:
					}
					return
				}
			}
		}
	}()

	// Send initial HelloAck
	select {
	case outCh <- &pb.AppendResponse{
		Msg: &pb.AppendResponse_HelloAck{
			HelloAck: &pb.HelloAck{
				WitnessAckedStreamSeq: witnessSeq,
				S3AckedStreamSeq:      s3Seq,
			},
		},
	}:
	case <-ctx.Done():
		return ctx.Err()
	case <-s.stopChan:
		return status.Errorf(codes.Unavailable, "server shutting down")
	}

	// Wake the S3 ack forwarding goroutine on context cancellation or stream return so it can exit.
	stopCtxWatcher := context.AfterFunc(ctx, func() {
		st.mu.Lock()
		st.cond.Broadcast()
		st.mu.Unlock()
	})
	defer stopCtxWatcher()
	defer func() {
		st.mu.Lock()
		st.cond.Broadcast()
		st.mu.Unlock()
	}()

	// Forward S3 acks notifications to outCh
	go func() {
		lastAckedS3 := s3Seq
		for {
			st.mu.Lock()
			for st.s3Seq <= lastAckedS3 {
				select {
				case <-ctx.Done():
					st.mu.Unlock()
					return
				case <-s.stopChan:
					st.mu.Unlock()
					return
				default:
				}
				st.cond.Wait()
			}
			curWitness := st.witnessSeq
			curS3 := st.s3Seq
			lastAckedS3 = curS3
			st.mu.Unlock()

			select {
			case <-ctx.Done():
				return
			case <-s.stopChan:
				return
			case outCh <- &pb.AppendResponse{
				Msg: &pb.AppendResponse_Ack{
					Ack: &pb.Ack{
						WitnessAckedStreamSeq: curWitness,
						S3AckedStreamSeq:      curS3,
					},
				},
			}:
			}
		}
	}()

	// Read loop for AppendRequest records
	for {
		req, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}

		pbRec := req.GetRecord()
		if pbRec == nil {
			continue
		}

		st.mu.Lock()
		curWitness := st.witnessSeq
		curS3 := st.s3Seq
		st.mu.Unlock()

		// Idempotency: duplicate records at or below witness watermark are acked and dropped
		if pbRec.StreamSeq <= curWitness {
			select {
			case outCh <- &pb.AppendResponse{
				Msg: &pb.AppendResponse_Ack{
					Ack: &pb.Ack{
						WitnessAckedStreamSeq: curWitness,
						S3AckedStreamSeq:      curS3,
					},
				},
			}:
			case <-ctx.Done():
				return ctx.Err()
			case <-s.stopChan:
				return status.Errorf(codes.Unavailable, "server shutting down")
			}
			continue
		}

		// Submit to group commit
		ackChan := make(chan ackResult, 1)
		select {
		case s.incomingChan <- incomingItem{
			streamID:  streamID,
			streamSeq: pbRec.StreamSeq,
			payload:   pbRec.Payload,
			ackChan:   ackChan,
		}:
		case <-ctx.Done():
			return ctx.Err()
		case <-s.stopChan:
			return status.Errorf(codes.Unavailable, "server shutting down")
		case err := <-sendErrCh:
			return err
		}

		// Wait for witness commit result
		select {
		case res := <-ackChan:
			if res.err != nil {
				return status.Errorf(codes.Internal, "commit failed: %v", res.err)
			}
			select {
			case outCh <- &pb.AppendResponse{
				Msg: &pb.AppendResponse_Ack{
					Ack: &pb.Ack{
						WitnessAckedStreamSeq: res.witnessSeq,
						S3AckedStreamSeq:      res.s3Seq,
					},
				},
			}:
			case <-ctx.Done():
				return ctx.Err()
			case <-s.stopChan:
				return status.Errorf(codes.Unavailable, "server shutting down")
			}
		case <-ctx.Done():
			return ctx.Err()
		case <-s.stopChan:
			return status.Errorf(codes.Unavailable, "server shutting down")
		case err := <-sendErrCh:
			return err
		}
	}
}

func (s *Server) groupCommitLoop() {
	defer s.wg.Done()

	ticker := time.NewTicker(s.cfg.BatchMaxDelay)
	defer ticker.Stop()

	var batch []incomingItem
	var batchBytes int64

	commitBatch := func() {
		if len(batch) == 0 {
			return
		}
		s.commitItems(batch)
		batch = nil
		batchBytes = 0
	}

	for {
		select {
		case <-s.stopChan:
			commitBatch()
			return
		case item := <-s.incomingChan:
			itemBytes := int64(wal.LogHeaderSize + len(item.payload))
			batch = append(batch, item)
			batchBytes += itemBytes

			if batchBytes >= s.cfg.BatchMaxSize {
				commitBatch()
			}
		case <-ticker.C:
			commitBatch()
		}
	}
}

func (s *Server) commitItems(items []incomingItem) {
	if len(items) == 0 {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var recordsToStore []*wal.LogRecord

	for _, item := range items {
		s.lastPosition++
		rec := &wal.LogRecord{
			Position:  s.lastPosition,
			StreamID:  item.streamID,
			StreamSeq: item.streamSeq,
			Payload:   item.payload,
		}
		rec.CRC32C = rec.ComputeCRC32C()
		recordsToStore = append(recordsToStore, rec)
	}

	// 1. Write batch to local log segment store and fsync
	if err := s.localStore.AppendBatch(recordsToStore); err != nil {
		klog.Errorf("Failed writing batch to local log segment store: %v", err)
		for _, item := range items {
			item.ackChan <- ackResult{err: err}
		}
		return
	}

	s.streamStore.AppendRecords(recordsToStore)

	// 2. Add to unflushed queue and notify tail waiters
	s.unflushedMu.Lock()
	for _, rec := range recordsToStore {
		s.unflushedRecords = append(s.unflushedRecords, rec)
		s.unflushedBytes += int64(wal.LogHeaderSize + len(rec.Payload))
	}
	triggerFlush := s.unflushedBytes >= s.cfg.FlushBytes
	s.unflushedMu.Unlock()

	s.tailMu.Lock()
	for waiter := range s.tailWaiters {
		select {
		case waiter <- struct{}{}:
		default:
		}
	}
	s.tailMu.Unlock()

	// 3. Update stream witness watermarks and send results
	streamCounts := make(map[string]int)
	streamMinSeq := make(map[string]uint64)
	streamMaxSeq := make(map[string]uint64)

	for i, item := range items {
		rec := recordsToStore[i]
		sid := rec.StreamID.String()
		st := s.streams[sid]
		if st == nil {
			st = newStreamState(rec.StreamSeq, 0)
			s.streams[sid] = st
		} else {
			st.mu.Lock()
			if rec.StreamSeq > st.witnessSeq {
				st.witnessSeq = rec.StreamSeq
			}
			st.mu.Unlock()
		}

		st.mu.Lock()
		wSeq := st.witnessSeq
		sSeq := st.s3Seq
		st.mu.Unlock()

		streamCounts[sid]++
		if _, ok := streamMinSeq[sid]; !ok || rec.StreamSeq < streamMinSeq[sid] {
			streamMinSeq[sid] = rec.StreamSeq
		}
		if rec.StreamSeq > streamMaxSeq[sid] {
			streamMaxSeq[sid] = rec.StreamSeq
		}

		item.ackChan <- ackResult{
			witnessSeq: wSeq,
			s3Seq:      sSeq,
		}
	}

	for sid, count := range streamCounts {
		st := s.streams[sid]
		st.mu.Lock()
		wSeq := st.witnessSeq
		st.mu.Unlock()
		klog.Infof("WAL buffer stream %s: accepted %d records (%d..%d, witness watermark=%d)",
			sid, count, streamMinSeq[sid], streamMaxSeq[sid], wSeq)
	}

	if triggerFlush {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if _, err := s.flushInternal(ctx); err != nil {
				klog.Warningf("background flush triggered by commit failed: %v", err)
			}
		}()
	}
}

// Flush RPC forces an S3 flush and returns once the segment is durable.
func (s *Server) Flush(ctx context.Context, req *pb.FlushRequest) (*pb.FlushResponse, error) {
	return s.flushInternal(ctx)
}

func (s *Server) periodicFlushLoop(interval time.Duration) {
	defer s.wg.Done()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopChan:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			if _, err := s.flushInternal(ctx); err != nil {
				klog.Warningf("periodic flush failed: %v", err)
			}
			cancel()
		}
	}
}

func (s *Server) flushInternal(ctx context.Context) (*pb.FlushResponse, error) {
	s.flushMu.Lock()
	for {
		if s.isFlushing {
			s.flushCond.Wait()
			continue
		}

		s.unflushedMu.Lock()
		if len(s.unflushedRecords) == 0 {
			s.unflushedMu.Unlock()
			s.flushMu.Unlock()
			s.mu.RLock()
			resp := &pb.FlushResponse{
				LastPosition: s.lastPosition,
			}
			s.mu.RUnlock()
			return resp, nil
		}

		recordsToFlush := s.unflushedRecords
		s.unflushedRecords = nil
		s.unflushedBytes = 0
		s.unflushedMu.Unlock()

		s.isFlushing = true
		s.flushMu.Unlock()

		err := s.doFlush(ctx, recordsToFlush)

		s.flushMu.Lock()
		s.isFlushing = false
		if err == nil {
			s.flushSeq++
		} else {
			s.unflushedMu.Lock()
			s.unflushedRecords = append(recordsToFlush, s.unflushedRecords...)
			for _, r := range recordsToFlush {
				s.unflushedBytes += int64(wal.LogHeaderSize + len(r.Payload))
			}
			s.unflushedMu.Unlock()
		}
		s.flushCond.Broadcast()

		if err != nil {
			s.flushMu.Unlock()
			return nil, err
		}

		// Check if new records arrived while doFlush was executing
		s.unflushedMu.Lock()
		unflushedLen := len(s.unflushedRecords)
		s.unflushedMu.Unlock()
		if unflushedLen == 0 {
			s.flushMu.Unlock()
			s.mu.RLock()
			resp := &pb.FlushResponse{
				LastPosition: s.lastPosition,
			}
			s.mu.RUnlock()
			return resp, nil
		}
	}
}

func (s *Server) doFlush(ctx context.Context, records []*wal.LogRecord) error {
	if len(records) == 0 {
		return nil
	}

	firstPos := records[0].Position
	lastPos := records[len(records)-1].Position

	// 1. Encode all records into segment byte buffer
	var segBuf bytes.Buffer
	segBuf.Write(wal.DefaultSegmentHeader().Encode())
	streamMaxSeq := make(map[string]uint64)

	for _, rec := range records {
		encoded := rec.Encode()
		segBuf.Write(encoded)
		sid := rec.StreamID.String()
		if rec.StreamSeq > streamMaxSeq[sid] {
			streamMaxSeq[sid] = rec.StreamSeq
		}
	}

	segPath := fmt.Sprintf("%s%012d-%012d.wal", SegmentsPrefix, firstPos, lastPos)
	segStream := blob.NewByteStreamFromBytes(segBuf.Bytes())
	defer errutil.CloseAndLog(segStream, fmt.Sprintf("segment stream %s", segPath))

	// 2. Put segment object to object storage
	if _, err := s.backend.PutObject(ctx, "", segPath, segStream); err != nil {
		return fmt.Errorf("failed to upload segment %s: %w", segPath, err)
	}

	// 3. Update flushed segments list and stream watermarks
	s.mu.Lock()
	s.flushedSegments = append(s.flushedSegments, segPath)
	if lastPos > s.lastFlushedPosition {
		s.lastFlushedPosition = lastPos
	}

	var notifiedStreams []*streamState
	for sid, maxSeq := range streamMaxSeq {
		if st, exists := s.streams[sid]; exists {
			st.mu.Lock()
			if maxSeq > st.s3Seq {
				st.s3Seq = maxSeq
				notifiedStreams = append(notifiedStreams, st)
			}
			st.mu.Unlock()
		}
	}
	s.mu.Unlock()

	s.streamStore.TrackSegmentCoverage(segPath, records)
	s.checkAggregatedSegmentGC(ctx)

	// 4. Clean up local segment files that have been flushed to permanent storage
	if err := s.localStore.DeleteSegmentsThrough(s.lastFlushedPosition, s.cfg.TailCacheBytes); err != nil {
		klog.Warningf("Error cleaning local segment files: %v", err)
	}

	// 5. Notify streams whose S3 acks advanced
	for _, st := range notifiedStreams {
		st.cond.Broadcast()
	}

	klog.Infof("Flushed WAL segment %s (%d records, %d bytes) to permanent storage", segPath, len(records), segBuf.Len())
	return nil
}

// Tail streams merged records in position order from object storage, local disk, and live incoming commits.
// When stream_id is set in req, Tail delegates directly to tailPerStream, streaming only that stream's
// records from per-stream files.
// When stream_id is unset, Tail serves as an unfiltered, position-ordered tooling and debug path
// streaming across all aggregated segments.
func (s *Server) Tail(req *pb.TailRequest, stream pb.WalBuffer_TailServer) error {
	ctx := stream.Context()

	if len(req.StreamId) > 0 {
		if len(req.StreamId) != 16 {
			return status.Errorf(codes.InvalidArgument, "stream_id must be exactly 16 bytes")
		}
		var filterStreamID uuid.UUID
		copy(filterStreamID[:], req.StreamId)
		return s.tailPerStream(ctx, filterStreamID, req, stream)
	}

	fromPos := req.FromPosition
	if fromPos == 0 {
		fromPos = 1
	}

	s.mu.RLock()
	lastFlushedPos := s.lastFlushedPosition
	s.mu.RUnlock()

	maxAllowedFromPos := lastFlushedPos + 1
	if fromPos > maxAllowedFromPos {
		fromPos = maxAllowedFromPos
	}

	currentPos := fromPos
	firstSent := false

	sendRecord := func(rec *wal.LogRecord) error {
		resp := &pb.TailResponse{Record: rec.ToProto()}
		if !firstSent {
			resp.ResumedFrom = fromPos
			firstSent = true
		}
		return stream.Send(resp)
	}

	waitChan := make(chan struct{}, 10)
	s.tailMu.Lock()
	s.tailWaiters[waitChan] = struct{}{}
	s.tailMu.Unlock()
	defer func() {
		s.tailMu.Lock()
		delete(s.tailWaiters, waitChan)
		s.tailMu.Unlock()
	}()

	for {
		// 1. Snapshot flushed segments and lastPosition under s.mu.RLock
		s.mu.RLock()
		flushedLastPos := s.lastFlushedPosition
		segments := make([]string, len(s.flushedSegments))
		copy(segments, s.flushedSegments)
		serverLastPos := s.lastPosition
		s.mu.RUnlock()

		// 2. Read flushed segments from object storage if currentPos <= flushedLastPos
		if currentPos <= flushedLastPos {
			for _, segPath := range segments {
				_, segLast, err := wal.ParseSegmentPath(segPath)
				if err != nil {
					continue
				}
				if segLast < currentPos {
					continue
				}

				records, err := ReadSegmentFromBackend(ctx, s.backend, segPath)
				if err != nil {
					klog.Warningf("Tail failed to read segment %s: %v", segPath, err)
					return fmt.Errorf("failed to read flushed segment %s: %w", segPath, err)
				}

				for _, rec := range records {
					if rec.Position >= currentPos {
						if err := sendRecord(rec); err != nil {
							return err
						}
						currentPos = rec.Position + 1
					}
				}
			}
		}

		// 3. Read committed unflushed records from local LogSegmentStore
		if currentPos <= serverLastPos {
			err := s.localStore.ReadFrom(currentPos, func(rec *wal.LogRecord) error {
				if rec.Position >= currentPos {
					if err := sendRecord(rec); err != nil {
						return err
					}
					currentPos = rec.Position + 1
				}
				return nil
			})
			if err != nil {
				return err
			}
		}

		// 4. Re-snapshot to check if a flush moved records from local disk to object storage
		s.mu.RLock()
		latestFlushedLastPos := s.lastFlushedPosition
		latestServerLastPos := s.lastPosition
		s.mu.RUnlock()

		if currentPos <= latestFlushedLastPos {
			// Flushed position advanced past currentPos; continue from object storage without blocking
			continue
		}

		if currentPos <= latestServerLastPos {
			// New records committed locally; continue from local store without blocking
			continue
		}

		// 5. Block on new commits, cancellation, or shutdown
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.stopChan:
			return nil
		case <-waitChan:
		}
	}
}

func (s *Server) checkAggregatedSegmentGC(ctx context.Context) {
	deleted, err := s.streamStore.CheckSegmentGC(ctx)
	if err != nil {
		klog.Warningf("Aggregated segment GC error: %v", err)
	}
	if len(deleted) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	deletedMap := make(map[string]bool)
	for _, d := range deleted {
		deletedMap[d] = true
	}
	var remaining []string
	for _, seg := range s.flushedSegments {
		if !deletedMap[seg] {
			remaining = append(remaining, seg)
		}
	}
	s.flushedSegments = remaining
}

// SealStreams flushes, seals, and uploads all active streams with records.
func (s *Server) SealStreams(ctx context.Context) error {
	_, err := s.streamStore.SealAll(ctx)
	s.checkAggregatedSegmentGC(ctx)
	return err
}

// SealStream seals and uploads a specific stream with records.
func (s *Server) SealStream(ctx context.Context, streamID uuid.UUID) error {
	_, err := s.streamStore.SealStream(ctx, streamID)
	s.checkAggregatedSegmentGC(ctx)
	return err
}

// TrimStreamHistory deletes sealed per-stream files in object storage <= trimSeq, guarded strictly by snapshotSeq.
func (s *Server) TrimStreamHistory(ctx context.Context, streamID uuid.UUID, trimSeq, snapshotSeq uint64) ([]string, error) {
	return s.streamStore.TrimStreamHistory(ctx, streamID, trimSeq, snapshotSeq)
}

func (s *Server) tailPerStream(ctx context.Context, streamID uuid.UUID, req *pb.TailRequest, stream pb.WalBuffer_TailServer) error {
	fromSeq := req.FromStreamSeq
	fromPos := req.FromPosition
	firstSent := false

	resumedFrom := fromPos
	if resumedFrom == 0 {
		resumedFrom = 1
	}

	sendRecord := func(rec *wal.LogRecord) error {
		if rec.StreamID != streamID || rec.StreamSeq <= fromSeq {
			return nil
		}
		if fromPos > 0 && rec.Position < fromPos {
			return nil
		}
		resp := &pb.TailResponse{Record: rec.ToProto()}
		if !firstSent {
			resp.ResumedFrom = resumedFrom
			firstSent = true
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
		fromSeq = rec.StreamSeq
		return nil
	}

	w := s.streamStore.getOrCreateStream(streamID)
	if err := w.flushPending(); err != nil {
		return fmt.Errorf("failed to flush pending stream records for tail: %w", err)
	}

	// 1. Stream records from sealed files
	sealedList := w.getSealedMetas(ctx, s.backend)
	for _, sm := range sealedList {
		if sm.ToSeq <= fromSeq {
			continue
		}
		var records []*wal.LogRecord
		if sm.Path != "" {
			if recs, _, err := ReadRecordsFromOffset(sm.Path, 0, wal.WithAcceptUnversioned(s.cfg.AcceptUnversioned)); err == nil && len(recs) > 0 {
				records = recs
			} else if err != nil {
				klog.Warningf("failed to read local sealed file %s: %v, falling back to backend", sm.Path, err)
			}
		}
		if records == nil && sm.Key != "" && s.backend != nil {
			recs, err := ReadSegmentFromBackend(ctx, s.backend, sm.Key, wal.WithAcceptUnversioned(s.cfg.AcceptUnversioned))
			if err != nil {
				return fmt.Errorf("failed to read sealed segment %s from backend: %w", sm.Key, err)
			}
			records = recs
		}
		for _, rec := range records {
			if err := sendRecord(rec); err != nil {
				return err
			}
		}
	}

	// 2. Track open file state and offset
	w.mu.Lock()
	curOpenFrom := w.openFrom
	openDir := w.dir
	w.mu.Unlock()

	var openOffset int64
	if curOpenFrom > 0 {
		openPath := filepath.Join(openDir, wal.FormatStreamFileName(curOpenFrom, 0, false))
		recs, nextOffset, err := ReadRecordsFromOffset(openPath, 0, wal.WithAcceptUnversioned(s.cfg.AcceptUnversioned))
		if err != nil {
			return fmt.Errorf("failed to read open stream file %s: %w", openPath, err)
		}
		openOffset = nextOffset
		for _, rec := range recs {
			if err := sendRecord(rec); err != nil {
				return err
			}
		}
	}

	// 3. Live streaming loop: reads only newly appended bytes starting at openOffset
	waitChan := make(chan struct{}, 10)
	s.tailMu.Lock()
	s.tailWaiters[waitChan] = struct{}{}
	s.tailMu.Unlock()
	defer func() {
		s.tailMu.Lock()
		delete(s.tailWaiters, waitChan)
		s.tailMu.Unlock()
	}()

	for {
		if err := w.flushPending(); err != nil {
			return fmt.Errorf("failed to flush pending stream records for tail: %w", err)
		}

		w.mu.Lock()
		latestOpenFrom := w.openFrom
		w.mu.Unlock()

		if latestOpenFrom != curOpenFrom && curOpenFrom > 0 {
			sealedList = w.getSealedMetas(ctx, s.backend)
			for _, sm := range sealedList {
				if sm.FromSeq == curOpenFrom && sm.Path != "" {
					recs, _, err := ReadRecordsFromOffset(sm.Path, openOffset)
					if err != nil {
						return fmt.Errorf("failed to read sealed stream file %s: %w", sm.Path, err)
					}
					for _, rec := range recs {
						if err := sendRecord(rec); err != nil {
							return err
						}
					}
					break
				}
			}
			curOpenFrom = latestOpenFrom
			openOffset = 0
		}

		if curOpenFrom > 0 {
			openPath := filepath.Join(openDir, wal.FormatStreamFileName(curOpenFrom, 0, false))
			recs, nextOffset, err := ReadRecordsFromOffset(openPath, openOffset)
			if err != nil {
				return fmt.Errorf("failed to read open stream file %s: %w", openPath, err)
			}
			openOffset = nextOffset
			for _, rec := range recs {
				if err := sendRecord(rec); err != nil {
					return err
				}
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.stopChan:
			return nil
		case <-waitChan:
		}
	}
}
