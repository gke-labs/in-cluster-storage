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
	"sync/atomic"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/wal/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/errutil"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/blob"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore"
	"github.com/gke-labs/in-cluster-storage/pkg/scoped"
	"github.com/gke-labs/in-cluster-storage/pkg/wal"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
)

const (
	DefaultFlushInterval   = 60 * time.Second
	DefaultFlushBytes      = 64 * 1024 * 1024
	DefaultTailCache       = 64 * 1024 * 1024
	DefaultBatchDelay      = 5 * time.Millisecond
	DefaultBatchSize       = 1024 * 1024
	DefaultWriterLockGrace = 15 * time.Second

	defaultStartupRetryInitialBackoff = 1 * time.Second
	defaultStartupRetryMaxBackoff     = 15 * time.Second
)

// Clock provides time operations for Server.
type Clock interface {
	Now() time.Time
}

// realClock implements Clock using the standard time package.
type realClock struct{}

func (realClock) Now() time.Time {
	return time.Now()
}

// ServerConfig configures the WAL Buffer service.
type ServerConfig struct {
	Backend            objectstore.Backend
	DataDir            string
	DataDirLockTimeout time.Duration
	FlushInterval      time.Duration
	FlushBytes         int64
	TailCacheBytes     int64
	BatchMaxDelay      time.Duration
	BatchMaxSize       int64
	StreamSealInterval time.Duration
	StreamSealBytes    int64
	AcceptUnversioned  bool
	NoFsync            bool
	WriterLockGrace    time.Duration
	Clock              Clock
}

type incomingItem struct {
	streamID  uuid.UUID
	streamSeq uint64
	payload   []byte
	// epoch is the writer epoch presented by the session that submitted this item.
	epoch uint64
}

type streamState struct {
	// holderFile tracks the stream's writer lock holder file on disk.
	holderFile holderFile

	// mu guards all mutable fields below.
	mu sync.Mutex

	// cond coordinates waiters on stream watermark advances and state changes.
	cond *sync.Cond

	// witnessSeq is the highest sequence number committed to local storage for this stream. Guarded by mu.
	witnessSeq uint64

	// s3Seq is the highest sequence number flushed to object storage for this stream. Guarded by mu.
	s3Seq uint64

	// commitErr is the terminal error encountered during batch commit, if any. Guarded by mu.
	commitErr error

	// inFlight is the number of append requests currently queued or committing for this stream. Guarded by mu.
	inFlight int

	// ackNeeded indicates an acknowledgement must be sent to attached sessions even if watermarks have not advanced. Guarded by mu.
	ackNeeded bool

	// attachments is the number of active client sessions attached to this stream. Guarded by mu.
	attachments int

	// waiters is the number of goroutines currently waiting on cond. Guarded by mu.
	waiters int

	// epoch is the highest accepted writer epoch for this stream. Guarded by mu.
	epoch uint64

	// holder is the client ID holding the writer lock for this stream. Guarded by mu.
	holder string

	// holderSessions is the number of active sessions for holder attached to this stream. Guarded by mu.
	holderSessions int

	// lastSessionEnd records when the holder's last session ended. Guarded by mu.
	lastSessionEnd time.Time
}

// grantDecisionInputs holds the stream writer lock state evaluated by decideGrant.
type grantDecisionInputs struct {
	// holder is the clientID currently holding the stream writer lock.
	holder string

	// attachedSessions is the number of active sessions for holder attached to this stream.
	attachedSessions int

	// inFlight is the number of records currently queued or committing for this stream.
	inFlight int

	// lastSessionEnd records when the holder's last session ended.
	lastSessionEnd time.Time

	// gracePeriod is the duration the writer lock stays reserved for holder after disconnect.
	gracePeriod time.Duration
}

// grantDecision represents the outcome of evaluating a stream writer lock grant.
type grantDecision int

const (
	// grantDecisionLockGranted indicates that the writer lock is granted to the requester.
	grantDecisionLockGranted grantDecision = iota

	// grantDecisionRefusedLockAlreadyHeld indicates the lock is currently held by active sessions of another client.
	grantDecisionRefusedLockAlreadyHeld

	// grantDecisionRefusedRecordsInFlight indicates records of the previous holder are still queued or committing.
	grantDecisionRefusedRecordsInFlight

	// grantDecisionRefusedGracePeriod indicates the lock is reserved for its previous holder for the grace duration.
	grantDecisionRefusedGracePeriod
)

// decideGrant determines whether the writer lock can be granted to requester, and if so,
// whether it is a resumed grant. When refused, it returns the refusal decision and when
// the grant decision can next change by time alone.
func decideGrant(inputs grantDecisionInputs, requester string, now time.Time) (decision grantDecision, resumed bool, nextWake time.Time) {
	if requester == "" {
		return grantDecisionLockGranted, false, time.Time{}
	}
	if inputs.holder == "" {
		return grantDecisionLockGranted, false, time.Time{}
	}
	if inputs.holder == requester {
		return grantDecisionLockGranted, true, time.Time{}
	}
	if inputs.attachedSessions != 0 {
		return grantDecisionRefusedLockAlreadyHeld, false, time.Time{}
	}
	if inputs.inFlight != 0 {
		return grantDecisionRefusedRecordsInFlight, false, time.Time{}
	}
	if inputs.gracePeriod > 0 && !inputs.lastSessionEnd.IsZero() && now.Before(inputs.lastSessionEnd.Add(inputs.gracePeriod)) {
		return grantDecisionRefusedGracePeriod, false, inputs.lastSessionEnd.Add(inputs.gracePeriod)
	}
	return grantDecisionLockGranted, false, time.Time{}
}

// grantDecisionStatus converts a refused grant decision to an Unavailable gRPC status with a descriptive message.
func grantDecisionStatus(decision grantDecision, remaining time.Duration) error {
	switch decision {
	case grantDecisionRefusedLockAlreadyHeld:
		return status.Errorf(codes.Unavailable, "stream writer lock is held by another client")
	case grantDecisionRefusedRecordsInFlight:
		return status.Errorf(codes.Unavailable, "stream writer lock is waiting for records of its previous holder to be committed")
	case grantDecisionRefusedGracePeriod:
		if remaining < 0 {
			remaining = 0
		}
		return status.Errorf(codes.Unavailable, "stream writer lock is reserved for its previous holder for %v", remaining)
	default:
		return status.Errorf(codes.Unavailable, "stream writer lock is unavailable")
	}
}

// newStreamState creates a new streamState initialized with watermarks.
func newStreamState(witnessSeq, s3Seq uint64) *streamState {
	st := &streamState{
		witnessSeq: witnessSeq,
		s3Seq:      s3Seq,
	}
	st.cond = sync.NewCond(&st.mu)
	return st
}

// raiseWatermarksLocked advances witnessSeq and s3Seq to uploadedTo if uploadedTo is higher.
// Requires streamState.mu to be held.
func (st *streamState) raiseWatermarksLocked(uploadedTo uint64) {
	if uploadedTo > st.witnessSeq {
		st.witnessSeq = uploadedTo
	}
	if uploadedTo > st.s3Seq {
		st.s3Seq = uploadedTo
	}
}

// fencedStatus creates a gRPC status error with code FailedPrecondition carrying pb.Fenced details.
// Attaching the details must not fail silently.
func fencedStatus(streamID uuid.UUID, writerEpoch, acceptedEpoch uint64) error {
	statusObject := status.New(codes.FailedPrecondition, fmt.Sprintf("stream %s writer epoch %d is lower than accepted epoch %d", streamID, writerEpoch, acceptedEpoch))
	statusWithDetails, err := statusObject.WithDetails(&pb.Fenced{
		StreamId:      streamID[:],
		WriterEpoch:   writerEpoch,
		AcceptedEpoch: acceptedEpoch,
	})
	if err != nil {
		return status.Errorf(codes.Internal, "failed to attach Fenced detail to status: %v", err)
	}
	return statusWithDetails.Err()
}

// Server implements the WalBuffer gRPC service.
type Server struct {
	// UnimplementedWalBufferServer provides forward-compatible gRPC server stubs.
	pb.UnimplementedWalBufferServer

	// Immutable fields (never change after construction).

	// cfg holds the buffer server configuration options.
	cfg ServerConfig

	// writerLockGrace is the duration a writer lock stays reserved for its holder after disconnect.
	writerLockGrace time.Duration

	// clock provides time operations for lock grace periods.
	clock Clock

	// holderStore persists and loads stream writer lock holders.
	holderStore writerLockStore

	// backend is the object storage backend used for flushing segments and stream discovery.
	backend objectstore.Backend

	// streamStore manages per-stream file storage, sealing, and object storage uploads.
	streamStore *StreamStore

	// localStore is the node-local append-only log segment store for micro-batched commits.
	localStore *wal.LogSegmentStore

	// dataDirectoryLock holds the exclusive advisory lock on cfg.DataDir.
	dataDirectoryLock *directoryLock

	// incomingChan buffers incoming append items from client sessions for group commit batching.
	incomingChan chan incomingItem

	// mu guards global log sequencing, flushed segment tracking, and active stream state mappings.
	mu sync.RWMutex

	// lastPosition is the highest global position assigned to any committed log record. Guarded by mu.
	lastPosition uint64

	// lastFlushedPosition is the highest global position successfully flushed to object storage. Guarded by mu.
	lastFlushedPosition uint64

	// flushedSegments lists segment object paths successfully uploaded to object storage. Guarded by mu.
	flushedSegments []string

	// streams maps stream ID strings to active streamState instances. Guarded by mu.
	streams map[string]*streamState

	// unflushedMu guards in-memory unflushed log records awaiting upload to object storage.
	unflushedMu sync.Mutex

	// unflushedRecords holds committed records retained in memory until flushed to object storage. Guarded by unflushedMu.
	unflushedRecords []*wal.LogRecord

	// unflushedBytes is the total byte size of unflushedRecords, used to trigger threshold flushes. Guarded by unflushedMu.
	unflushedBytes int64

	// tailMu guards tail wait channels registered by streaming readers.
	tailMu sync.RWMutex

	// tailWaiters tracks notification channels signaled when new records are committed. Guarded by tailMu.
	tailWaiters map[chan struct{}]struct{}

	// flushMu coordinates flushes of buffered records to object storage.
	flushMu sync.Mutex

	// flushCond coordinates goroutines waiting for an in-progress flush to complete.
	flushCond *sync.Cond

	// isFlushing indicates whether a flush to object storage is currently in progress. Guarded by flushMu.
	isFlushing bool

	// flushSeq is incremented on each successful flush. Guarded by flushMu.
	flushSeq uint64

	// Lifecycle and background worker coordination (self-synchronized; not guarded by a mutex).

	// stopChan is closed on Server.Close to signal background workers and sessions to terminate.
	stopChan chan struct{}

	// stopOnce ensures stopChan is closed exactly once during Close.
	stopOnce sync.Once

	// groupCommitWg coordinates graceful shutdown of the groupCommitLoop worker goroutine.
	groupCommitWg sync.WaitGroup

	// periodicFlushWg coordinates graceful shutdown of the periodicFlushLoop worker goroutine.
	periodicFlushWg sync.WaitGroup

	// backgroundFlushWg coordinates graceful shutdown of threshold-triggered background flush goroutines.
	backgroundFlushWg sync.WaitGroup

	// activeLoops tracks the number of running background loop goroutines atomically.
	activeLoops atomic.Int32
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

	dataDirectoryLock, err := acquireDirectoryLock(ctx, cfg.DataDir, cfg.DataDirLockTimeout)
	if err != nil {
		return nil, err
	}
	defer func() {
		if dataDirectoryLock != nil {
			errutil.CloseAndLog(dataDirectoryLock, "directory lock")
		}
	}()

	if cfg.Clock == nil {
		cfg.Clock = realClock{}
	}

	holderStore := newDiskWriterLockStore(filepath.Join(cfg.DataDir, "writer-locks"))

	dataDir := cfg.DataDir

	// 1. Discover existing segments from backend
	discoveredSegments, lastFlushedPos, err := ListSegmentsFromBackend(ctx, cfg.Backend)
	if err != nil {
		return nil, fmt.Errorf("failed to list segments from backend: %w", err)
	}

	streamStore := NewStreamStore(dataDir, cfg.Backend, cfg.StreamSealInterval, cfg.StreamSealBytes)
	defer func() {
		if streamStore != nil {
			if closeError := streamStore.Close(ctx); closeError != nil {
				scoped.Log(ctx).Error(closeError, "failed to close stream store after startup error")
			}
		}
	}()
	if cfg.AcceptUnversioned {
		streamStore.SetAcceptUnversioned(true)
	}

	minCoveredPos, err := streamStore.LoadCoverage()
	if err != nil {
		return nil, fmt.Errorf("failed to load stream coverage: %w", err)
	}

	var allDiscoveredRecords []*wal.LogRecord
	backendWatermarks := make(map[string]uint64)

	for _, segPath := range discoveredSegments {
		_, segLast, err := wal.ParseSegmentPath(segPath)
		if err == nil && minCoveredPos > 0 && segLast <= minCoveredPos {
			// Segment is fully covered by sealed per-stream files; skip reading from backend
			continue
		}
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
	var scanOpts []wal.ScanOption
	if cfg.AcceptUnversioned {
		scanOpts = append(scanOpts, wal.WithAcceptUnversioned(true))
	}
	if cfg.NoFsync {
		scanOpts = append(scanOpts, wal.WithNoFsync(true))
	}
	localStore, localRecords, err := wal.NewLogSegmentStore(dataDir, "log", cfg.FlushBytes, scanOpts...)
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

	// StreamStore recovery includes all discovered records (backend + local).
	// If a position exists in both backend and local segments with differing content
	// (not currently detected), the local record appended last wins during stream store recovery.
	allDiscoveredRecords = append(allDiscoveredRecords, localRecords...)
	if err := streamStore.Recover(ctx, allDiscoveredRecords); err != nil {
		klog.Warningf("Error during stream store recovery: %v", err)
	}

	nextPosition := localMaxPos + 1

	klog.Infof("WAL Buffer initializing: assigning positions starting at %d (last_flushed_position=%d, local_max_position=%d, segments=%d, streams=%d)",
		nextPosition, lastFlushedPos, localMaxPos, len(discoveredSegments), len(streamWatermarks))

	// All unflushed local records are held in memory until flushed to backend.
	// This in-memory retention is bounded by cfg.FlushBytes (the maximum size of unflushed local segments).
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
		writerLockGrace:     cfg.WriterLockGrace,
		clock:               cfg.Clock,
		holderStore:         holderStore,
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
	s.streamStore.SetEvictHook(s.evictIdleStreams)

	// Flush the local-only tail before serving
	if len(unflushed) > 0 {
		if _, err := s.flushInternal(ctx); err != nil {
			klog.Warningf("Failed to flush local-only tail during startup recovery: %v", err)
		}
	}

	s.checkAggregatedSegmentGC(ctx)

	// Start background group commit worker
	s.groupCommitWg.Add(1)
	s.activeLoops.Add(1)
	go s.groupCommitLoop(ctx)

	// Start background flush worker
	if cfg.FlushInterval > 0 {
		s.periodicFlushWg.Add(1)
		s.activeLoops.Add(1)
		go s.periodicFlushLoop(cfg.FlushInterval)
	}

	s.dataDirectoryLock = dataDirectoryLock
	dataDirectoryLock = nil
	streamStore = nil

	return s, nil
}

// NewServerWithRetry calls NewServer, retrying with exponential back-off (1s doubling to 15s)
// while the error is objectstore.ErrUnavailable, until initialization succeeds or ctx ends.
// Any other error is returned immediately.
func NewServerWithRetry(ctx context.Context, config ServerConfig) (*Server, error) {
	return newServerWithRetry(ctx, config, defaultStartupRetryInitialBackoff, defaultStartupRetryMaxBackoff)
}

// newServerWithRetry retries NewServer on objectstore.ErrUnavailable with exponential back-off
// from initialBackoff up to maxBackoff until startup succeeds or ctx is cancelled.
func newServerWithRetry(ctx context.Context, config ServerConfig, initialBackoff, maxBackoff time.Duration) (*Server, error) {
	backoff := initialBackoff
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		server, err := NewServer(ctx, config)
		if err == nil || !errors.Is(err, objectstore.ErrUnavailable) {
			return server, err
		}
		scoped.Log(ctx).Error(err, "object store unavailable during WAL buffer startup; retrying", "backoff", backoff)
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
		backoff = min(backoff*2, maxBackoff)
	}
}

const loopShutdownTimeout = 5 * time.Second

func waitWithTimeout(wg *sync.WaitGroup, timeout time.Duration, name string) error {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return nil
	case <-timer.C:
		return fmt.Errorf("buffer loop %s timed out waiting to exit", name)
	}
}

// Close gracefully stops workers, flushes to permanent storage, and closes local stores.
func (s *Server) Close() error {
	var closed bool
	s.stopOnce.Do(func() {
		close(s.stopChan)
		closed = true
	})
	if !closed {
		return nil
	}
	// Wake any stream Append notification forwarders waiting on st.cond so they
	// observe s.stopChan closure and exit their goroutine.
	s.mu.RLock()
	for _, st := range s.streams {
		st.mu.Lock()
		st.cond.Broadcast()
		st.mu.Unlock()
	}
	s.mu.RUnlock()

	var errs []error
	if err := waitWithTimeout(&s.groupCommitWg, loopShutdownTimeout, "groupCommitLoop"); err != nil {
		errs = append(errs, err)
	}
	if err := waitWithTimeout(&s.periodicFlushWg, loopShutdownTimeout, "periodicFlushLoop"); err != nil {
		errs = append(errs, err)
	}
	if err := waitWithTimeout(&s.backgroundFlushWg, loopShutdownTimeout, "backgroundFlush"); err != nil {
		errs = append(errs, err)
	}

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

	if s.dataDirectoryLock != nil {
		if err := s.dataDirectoryLock.Close(); err != nil {
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

// StreamCount returns the current number of streams tracked in s.streams.
func (s *Server) StreamCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.streams)
}

func (s *Server) evictIdleStreams() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for sid, st := range s.streams {
		st.mu.Lock()
		inGrace := st.holder != "" && s.writerLockGrace > 0 && !st.lastSessionEnd.IsZero() && s.clock.Now().Before(st.lastSessionEnd.Add(s.writerLockGrace))
		// The state must not be evicted if the holder file's last save failed or if an attach
		// is currently in progress: the knowledge lives only in memory, so the state must outlive it.
		active := st.attachments > 0 || st.waiters > 0 || st.inFlight > 0 || inGrace || !st.holderFile.contentsKnown()
		witnessSeq := st.witnessSeq
		s3Seq := st.s3Seq
		st.mu.Unlock()

		if active {
			continue
		}

		streamID, err := uuid.Parse(sid)
		if err != nil {
			klog.Warningf("Failed to parse stream ID %s during idle stream eviction: %v", sid, err)
			continue
		}

		if s.streamStore != nil && !s.streamStore.canEvictStream(streamID, witnessSeq, s3Seq) {
			continue
		}

		delete(s.streams, sid)
	}
}

// getOrCreateStreamLocked finds or creates the stream state for streamID in s.streams.
// A new state is initialized with the discovered uploadedTo watermark.
// Requires s.mu to be held. Returns the streamState unlocked without modifying its fields.
func (s *Server) getOrCreateStreamLocked(streamID uuid.UUID, uploadedTo uint64) *streamState {
	streamKey := streamID.String()
	state, exists := s.streams[streamKey]
	if !exists {
		state = newStreamState(uploadedTo, uploadedTo)
		s.streams[streamKey] = state
	}
	return state
}

func (s *Server) getOrCreateStream(ctx context.Context, streamID uuid.UUID) *streamState {
	var uploadedTo uint64
	if s.streamStore != nil {
		uploadedTo = s.streamStore.EnsureDiscovered(ctx, streamID)
	}

	s.mu.Lock()
	state := s.getOrCreateStreamLocked(streamID, uploadedTo)
	s.mu.Unlock()

	state.mu.Lock()
	state.raiseWatermarksLocked(uploadedTo)
	state.mu.Unlock()

	return state
}

// attachSession attaches an Append session to its stream state after acquiring
// the writer lock if a client ID is provided.
//
// Remote discovery (EnsureDiscovered) performs backend I/O and must happen before
// acquiring s.mu.
//
// When the writer lock cannot be granted immediately (another client is attached,
// records of the previous holder are still being committed, or the lock is reserved
// for its previous holder during the grace period), the session returns at once with
// codes.Unavailable naming the reason so the client retries with back-off.
//
// The stream's writer lock holder file (<dataDir>/writer-locks/<streamID>) is loaded
// once after the stream state is created and written only when the holder changes.
//
// Lock order: holderFile.mu, then Server.mu, then streamState.mu.
func (s *Server) attachSession(ctx context.Context, streamID uuid.UUID, sessionEpoch uint64, clientID string) (retState *streamState, retWitnessSeq uint64, retS3Seq uint64, retResumed bool, retErr error) {
	var uploadedTo uint64
	if s.streamStore != nil {
		uploadedTo = s.streamStore.EnsureDiscovered(ctx, streamID)
	}

	s.mu.Lock()
	state := s.getOrCreateStreamLocked(streamID, uploadedTo)

	state.mu.Lock()
	// raiseWatermarksLocked runs before the epoch check because uploadedTo comes
	// from what EnsureDiscovered found in object storage for the stream, not from
	// the connecting writer. It only moves the watermarks up, so applying it even
	// for a session that is then refused as fenced changes nothing a stale writer
	// controls. A refused session must not change anything else.
	state.raiseWatermarksLocked(uploadedTo)
	state.attachments++
	state.mu.Unlock()
	s.mu.Unlock()

	defer func() {
		if retErr != nil {
			state.mu.Lock()
			state.attachments--
			state.mu.Unlock()
		}
	}()

	state.holderFile.mu.Lock()
	defer state.holderFile.mu.Unlock()

	var (
		holder    string
		firstLoad bool
	)
	if clientID != "" {
		var err error
		holder, firstLoad, err = state.holderFile.loadLocked(s.holderStore, streamID)
		if err != nil {
			scoped.Log(ctx).Error(err, "failed to load stream writer lock holder", "stream", streamID)
			return nil, 0, 0, false, status.Errorf(codes.Internal, "failed to load stream writer lock holder")
		}
	}

	s.mu.Lock()
	state.mu.Lock()

	if firstLoad && holder != "" {
		state.holder = holder
		state.holderSessions = 0
		state.lastSessionEnd = s.clock.Now()
	}

	select {
	case <-ctx.Done():
		state.mu.Unlock()
		s.mu.Unlock()
		return nil, 0, 0, false, ctx.Err()
	case <-s.stopChan:
		state.mu.Unlock()
		s.mu.Unlock()
		return nil, 0, 0, false, status.Errorf(codes.Unavailable, "server shutting down")
	default:
	}

	inputs := grantDecisionInputs{
		holder:           state.holder,
		attachedSessions: state.holderSessions,
		inFlight:         state.inFlight,
		lastSessionEnd:   state.lastSessionEnd,
		gracePeriod:      s.writerLockGrace,
	}
	now := s.clock.Now()
	decision, resumed, nextWake := decideGrant(inputs, clientID, now)
	if decision != grantDecisionLockGranted {
		state.mu.Unlock()
		s.mu.Unlock()
		var remaining time.Duration
		if !nextWake.IsZero() {
			remaining = nextWake.Sub(now)
			if remaining < 0 {
				remaining = 0
			}
		}
		return nil, 0, 0, false, grantDecisionStatus(decision, remaining)
	}

	if sessionEpoch < state.epoch {
		acceptedEpoch := state.epoch
		state.mu.Unlock()
		s.mu.Unlock()
		return nil, 0, 0, false, fencedStatus(streamID, sessionEpoch, acceptedEpoch)
	}

	if clientID != "" && state.holderFile.needsSaveLocked(clientID) {
		// Unlock Server.mu and streamState.mu to perform file I/O.
		// No re-checks are needed after the save: every attaching session of
		// this stream is waiting on holderFile.mu, so no session attaches and the epoch cannot rise.
		state.mu.Unlock()
		s.mu.Unlock()

		if err := state.holderFile.saveLocked(s.holderStore, streamID, clientID); err != nil {
			scoped.Log(ctx).Error(err, "failed to save stream writer lock holder", "stream", streamID)
			return nil, 0, 0, false, status.Errorf(codes.Unavailable, "failed to save stream writer lock holder")
		}

		s.mu.Lock()
		state.mu.Lock()
	}

	if sessionEpoch > state.epoch {
		state.epoch = sessionEpoch
		state.cond.Broadcast()
	}
	if clientID != "" {
		state.holder = clientID
		state.holderSessions++
		state.lastSessionEnd = time.Time{}
	}
	witnessSeq, s3Seq := state.witnessSeq, state.s3Seq
	state.mu.Unlock()
	s.mu.Unlock()
	return state, witnessSeq, s3Seq, resumed, nil
}

// detachSession detaches an Append session from its stream state.
//
// released reports whether the session ended cleanly (the client half-closed the
// stream, all in-flight records were committed, and Append returned nil). Only a
// clean end-of-stream path sets released to true to release the writer lock
// immediately, clearing the holder in memory and removing its holder file so the
// release survives idle eviction and server restarts. Any other termination
// (context cancellation, severed connection, server shutdown, or commit errors)
// leaves released false so the lock remains reserved for the previous holder for
// the grace period.
//
// Lock order: holderFile.mu, then Server.mu, then streamState.mu.
// Taking holderFile.mu here cannot deadlock against an attach in progress:
// attachSession takes the same locks in the same order and never waits for a
// session to end.
func (s *Server) detachSession(ctx context.Context, streamID uuid.UUID, state *streamState, clientID string, released bool) {
	if released && clientID != "" {
		state.holderFile.mu.Lock()
		defer state.holderFile.mu.Unlock()
	}

	var clearHolder bool
	state.mu.Lock()
	state.attachments--
	if clientID != "" && state.holder == clientID && state.holderSessions > 0 {
		state.holderSessions--
		if state.holderSessions == 0 {
			if released {
				state.holder = ""
				state.lastSessionEnd = time.Time{}
				clearHolder = true
			} else {
				state.lastSessionEnd = s.clock.Now()
			}
		}
	}
	state.mu.Unlock()

	if clearHolder {
		if err := state.holderFile.clearLocked(s.holderStore, streamID); err != nil {
			scoped.Log(ctx).Error(err, "failed to remove stream writer lock holder", "stream", streamID)
		}
	}
}

type appendServerWithContext struct {
	pb.WalBuffer_AppendServer
	ctx context.Context
}

func (s *appendServerWithContext) Context() context.Context {
	return s.ctx
}

// Append handles the bidirectional client append stream.
func (s *Server) Append(stream pb.WalBuffer_AppendServer) error {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

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
	sessionEpoch := hello.GetEpoch()
	clientID := string(hello.GetClientId())

	logger := scoped.Log(ctx).WithValues("stream", streamID.String(), "epoch", sessionEpoch)
	ctx = scoped.WithLog(ctx, logger)
	stream = &appendServerWithContext{
		WalBuffer_AppendServer: stream,
		ctx:                    ctx,
	}

	st, witnessSeq, s3Seq, resumed, err := s.attachSession(ctx, streamID, sessionEpoch, clientID)
	if err != nil {
		return err
	}

	var released bool
	defer func() {
		s.detachSession(ctx, streamID, st, clientID, released)
	}()

	scoped.Log(ctx).Info("WAL buffer: Hello received", "witnessWatermark", witnessSeq, "s3Watermark", s3Seq)

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
				Resumed:               resumed,
			},
		},
	}:
	case <-ctx.Done():
		return ctx.Err()
	case <-s.stopChan:
		return status.Errorf(codes.Unavailable, "server shutting down")
	}

	// Wake the condition variable on context cancellation or stream return so it can exit.
	stopCtxWatcher := context.AfterFunc(ctx, func() {
		st.mu.Lock()
		st.cond.Broadcast()
		st.mu.Unlock()
	})
	defer stopCtxWatcher()
	// Defers run LIFO: cancelling ctx here, while the watcher is still registered,
	// guarantees the watcher's function starts; stopCtxWatcher cannot recall a
	// function that has started; and the forwarder checks ctx.Done() under the stream
	// lock before each wait, so it cannot miss the wake-up.
	defer cancel()

	// Forward watermark acks (witness and S3) to outCh
	forwarderErrChan := make(chan error, 1)
	go func() {
		lastAckedWitness := witnessSeq
		lastAckedS3 := s3Seq
		for {
			st.mu.Lock()
			for st.witnessSeq <= lastAckedWitness && st.s3Seq <= lastAckedS3 && !st.ackNeeded && st.commitErr == nil && sessionEpoch >= st.epoch {
				select {
				case <-ctx.Done():
					st.mu.Unlock()
					return
				case <-s.stopChan:
					st.mu.Unlock()
					return
				default:
				}
				st.waiters++
				st.cond.Wait()
				st.waiters--
			}

			if sessionEpoch < st.epoch {
				acceptedEpoch := st.epoch
				st.mu.Unlock()
				select {
				case forwarderErrChan <- fencedStatus(streamID, sessionEpoch, acceptedEpoch):
				default:
				}
				return
			}

			if st.commitErr != nil {
				commitErr := st.commitErr
				st.mu.Unlock()
				select {
				case forwarderErrChan <- status.Errorf(codes.Internal, "commit failed: %v", commitErr):
				default:
				}
				return
			}

			currentWitness := st.witnessSeq
			currentS3 := st.s3Seq
			lastAckedWitness = currentWitness
			lastAckedS3 = currentS3
			st.ackNeeded = false
			st.mu.Unlock()

			select {
			case <-ctx.Done():
				return
			case <-s.stopChan:
				return
			case outCh <- &pb.AppendResponse{
				Msg: &pb.AppendResponse_Ack{
					Ack: &pb.Ack{
						WitnessAckedStreamSeq: currentWitness,
						S3AckedStreamSeq:      currentS3,
					},
				},
			}:
			}
		}
	}()

	type receiveResult struct {
		request *pb.AppendRequest
		err     error
	}

	// This receive goroutine exists solely so the handler can return Internal
	// promptly when a commit fails while the client is idle waiting for
	// acknowledgements, because gRPC provides no server-side mechanism to
	// interrupt stream.Recv().
	receiveChan := make(chan receiveResult)
	go func() {
		for {
			request, err := stream.Recv()
			select {
			case receiveChan <- receiveResult{request: request, err: err}:
				if err != nil {
					return
				}
			case <-ctx.Done():
				return
			case <-s.stopChan:
				return
			}
		}
	}()

	decInFlight := func() {
		st.mu.Lock()
		st.inFlight--
		st.cond.Broadcast()
		st.mu.Unlock()
	}

	// Read loop for AppendRequest records
	for {
		var result receiveResult
		select {
		case result = <-receiveChan:
		case <-ctx.Done():
			return ctx.Err()
		case <-s.stopChan:
			return status.Errorf(codes.Unavailable, "server shutting down")
		case err := <-forwarderErrChan:
			return err
		case err := <-sendErrCh:
			return err
		}

		if result.err != nil {
			if errors.Is(result.err, io.EOF) {
				st.mu.Lock()
				for st.inFlight > 0 && st.commitErr == nil {
					select {
					case <-ctx.Done():
						st.mu.Unlock()
						return ctx.Err()
					case <-s.stopChan:
						st.mu.Unlock()
						return status.Errorf(codes.Unavailable, "server shutting down")
					case err := <-sendErrCh:
						st.mu.Unlock()
						return err
					default:
					}
					st.cond.Wait()
				}
				if st.commitErr != nil {
					commitErr := st.commitErr
					st.mu.Unlock()
					return status.Errorf(codes.Internal, "commit failed: %v", commitErr)
				}
				st.mu.Unlock()
				released = true
				return nil
			}
			return result.err
		}

		pbRec := result.request.GetRecord()
		if pbRec == nil {
			continue
		}

		st.mu.Lock()
		if st.commitErr != nil {
			commitErr := st.commitErr
			st.mu.Unlock()
			return status.Errorf(codes.Internal, "commit failed: %v", commitErr)
		}

		// Idempotency: duplicate records at or below witness watermark trigger an Ack from the forwarder
		if pbRec.StreamSeq <= st.witnessSeq {
			st.ackNeeded = true
			st.cond.Broadcast()
			st.mu.Unlock()
			continue
		}

		// Backpressure: wait while inFlight >= 1024
		for st.inFlight >= 1024 && st.commitErr == nil {
			select {
			case <-ctx.Done():
				st.mu.Unlock()
				return ctx.Err()
			case <-s.stopChan:
				st.mu.Unlock()
				return status.Errorf(codes.Unavailable, "server shutting down")
			case err := <-forwarderErrChan:
				st.mu.Unlock()
				return err
			case err := <-sendErrCh:
				st.mu.Unlock()
				return err
			default:
			}
			st.cond.Wait()
		}
		if st.commitErr != nil {
			commitErr := st.commitErr
			st.mu.Unlock()
			return status.Errorf(codes.Internal, "commit failed: %v", commitErr)
		}

		st.inFlight++
		st.mu.Unlock()

		select {
		case s.incomingChan <- incomingItem{
			streamID:  streamID,
			streamSeq: pbRec.StreamSeq,
			payload:   pbRec.Payload,
			epoch:     sessionEpoch,
		}:
		case <-ctx.Done():
			decInFlight()
			return ctx.Err()
		case <-s.stopChan:
			decInFlight()
			return status.Errorf(codes.Unavailable, "server shutting down")
		case err := <-forwarderErrChan:
			decInFlight()
			return err
		case err := <-sendErrCh:
			decInFlight()
			return err
		}
	}
}

func (s *Server) groupCommitLoop(ctx context.Context) {
	defer s.groupCommitWg.Done()
	defer s.activeLoops.Add(-1)

	ticker := time.NewTicker(s.cfg.BatchMaxDelay)
	defer ticker.Stop()

	var batch []incomingItem
	var batchBytes int64

	commitBatch := func() {
		if len(batch) == 0 {
			return
		}
		s.commitItems(ctx, batch)
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

// batchStream tracks per-stream planning and progress within a single commit batch.
type batchStream struct {
	// count is the total number of items submitted for this stream in the batch (including duplicates).
	count int
	// firstSeq is the stream sequence of the first record accepted in this batch.
	firstSeq uint64
	// lastSeq is the stream sequence of the last record accepted in this batch.
	lastSeq uint64
	// accepted is the highest sequence accepted for the stream so far in this batch, starting from the witness watermark.
	accepted uint64
	// written indicates whether any record of this stream was accepted in this batch.
	written bool
}

// batchPlan holds the per-stream planning state and the log records to be written for a commit batch.
type batchPlan struct {
	// streams maps stream ID strings to their per-stream batch planning state.
	streams map[string]*batchStream
	// records contains the deduplicated log records to append to storage.
	records []*wal.LogRecord
}

// planBatchLocked builds the per-stream batch planning state and constructs deduplicated log records.
// It must be called with s.mu held. planBatchLocked assigns log positions (it advances s.lastPosition)
// and creates the stream state in s.streams if it is missing.
func (s *Server) planBatchLocked(items []incomingItem) *batchPlan {
	plan := &batchPlan{
		streams: make(map[string]*batchStream),
	}

	for _, item := range items {
		streamID := item.streamID.String()
		stream := plan.streams[streamID]
		if stream == nil {
			streamState := s.streams[streamID]
			if streamState == nil {
				streamState = newStreamState(0, 0)
				s.streams[streamID] = streamState
			}
			streamState.mu.Lock()
			witnessSeq := streamState.witnessSeq
			streamState.mu.Unlock()

			stream = &batchStream{
				accepted: witnessSeq,
			}
			plan.streams[streamID] = stream
		}
		stream.count++

		if item.streamSeq > stream.accepted {
			stream.accepted = item.streamSeq
			if !stream.written {
				stream.firstSeq = item.streamSeq
				stream.written = true
			}
			stream.lastSeq = item.streamSeq

			s.lastPosition++
			record := &wal.LogRecord{
				Position:  s.lastPosition,
				StreamID:  item.streamID,
				StreamSeq: item.streamSeq,
				Payload:   item.payload,
			}
			record.CRC32C = record.ComputeCRC32C()
			plan.records = append(plan.records, record)
		}
	}

	return plan
}

// applyBatchLocked updates the per-stream state after writing records or upon a write failure.
// It must be called with s.mu held. On error, applyBatchLocked sets commitErr on each stream in the plan.
// On success, it advances witnessSeq to lastSeq if records were written, and sets ackNeeded if a stream
// had only skipped records. For both error and success, it decrements inFlight by count,
// broadcasts on the stream condition variable, and logs accepted records on success.
func (s *Server) applyBatchLocked(plan *batchPlan, err error) {
	for streamID, stream := range plan.streams {
		streamState := s.streams[streamID]
		streamState.mu.Lock()
		if err != nil {
			if streamState.commitErr == nil {
				streamState.commitErr = err
			}
		} else {
			if stream.written && stream.lastSeq > streamState.witnessSeq {
				streamState.witnessSeq = stream.lastSeq
			}
			if !stream.written {
				streamState.ackNeeded = true
			}
		}
		streamState.inFlight -= stream.count
		if streamState.inFlight < 0 {
			// A negative inFlight count indicates counter imbalance.
			klog.Errorf("WAL buffer stream %s: negative inFlight count %d after committing %d items", streamID, streamState.inFlight, stream.count)
		}
		streamState.cond.Broadcast()
		witnessSeq := streamState.witnessSeq
		streamState.mu.Unlock()

		if err == nil && stream.written {
			klog.Infof("WAL buffer stream %s: accepted %d records (%d..%d, witness watermark=%d)",
				streamID, stream.count, stream.firstSeq, stream.lastSeq, witnessSeq)
		}
	}
}

func (s *Server) commitItems(ctx context.Context, items []incomingItem) {
	if len(items) == 0 {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// We check the writer epoch again at commit under s.mu and discard
	// in-flight records from superseded sessions whose epoch is lower than
	// the stream's highest accepted epoch.
	//
	// Why nothing can slip between:
	// s.mu is held across the entirety of commitItems and is also held
	// during the Hello handshake when validating and updating st.epoch.
	// Therefore, batch commit and epoch acceptance are mutually exclusive.
	// Any in-flight record already queued by an earlier session was either
	// fully committed before the higher epoch was accepted, or st.epoch
	// was updated first, in which case this check discards the record before
	// it can be appended to localStore or advance st.witnessSeq. Thus, no
	// record from a superseded session is ever committed after the new
	// session's HelloAck, and the new session will never receive an
	// acknowledgement for a record it did not send.
	var validItems []incomingItem
	for _, item := range items {
		streamKey := item.streamID.String()
		stream := s.streams[streamKey]
		var acceptedEpoch uint64
		if stream != nil {
			stream.mu.Lock()
			acceptedEpoch = stream.epoch
			stream.mu.Unlock()
		}
		if item.epoch < acceptedEpoch {
			if stream != nil {
				stream.mu.Lock()
				stream.inFlight--
				stream.cond.Broadcast()
				stream.mu.Unlock()
			}
			scoped.Log(ctx).Info("WAL buffer: discarded in-flight record from superseded session",
				"stream", streamKey, "recordEpoch", item.epoch, "acceptedEpoch", acceptedEpoch, "streamSeq", item.streamSeq)
			// The discarded item is dropped without sending a per-record error.
			// The session that submitted it learns of the fence at session level:
			// the Hello handshake that accepted the higher epoch broadcast the
			// stream's condition, causing the superseded session's forwarder to
			// terminate the session with the Fenced status. If the stream state
			// was evicted, no session remains attached.
			// Asserted by TestAppend_InFlightRecordFromSupersededSessionDiscarded.
			continue
		}
		validItems = append(validItems, item)
	}

	if len(validItems) == 0 {
		return
	}
	items = validItems

	// De-duplicate replayed in-flight records: accept a record only if its
	// sequence is above the highest one accepted so far in this commit
	// (starting at the stream's witness watermark).
	plan := s.planBatchLocked(items)

	// 1. Write batch to local log segment store and fsync
	if err := s.localStore.AppendBatch(plan.records); err != nil {
		klog.Errorf("Failed writing batch to local log segment store: %v", err)
		s.applyBatchLocked(plan, err)
		return
	}

	s.streamStore.AppendRecords(plan.records)

	// 2. Add to unflushed queue and notify tail waiters
	s.unflushedMu.Lock()
	for _, record := range plan.records {
		s.unflushedRecords = append(s.unflushedRecords, record)
		s.unflushedBytes += int64(wal.LogHeaderSize + len(record.Payload))
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

	// 3. Update stream witness watermarks, decrement inFlight, and broadcast
	s.applyBatchLocked(plan, nil)

	if triggerFlush {
		s.backgroundFlushWg.Add(1)
		go func() {
			defer s.backgroundFlushWg.Done()
			select {
			case <-s.stopChan:
				return
			default:
			}
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

// ActiveLoops returns the number of active background loop goroutines.
func (s *Server) ActiveLoops() int {
	return int(s.activeLoops.Load())
}

func (s *Server) periodicFlushLoop(interval time.Duration) {
	defer s.periodicFlushWg.Done()
	defer s.activeLoops.Add(-1)

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
	lastFlushedPosition := s.lastFlushedPosition

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
	if err := s.localStore.DeleteSegmentsThrough(lastFlushedPosition, s.cfg.TailCacheBytes); err != nil {
		klog.Warningf("Error cleaning local segment files: %v", err)
	}

	// 5. Notify streams whose S3 acks advanced
	for _, st := range notifiedStreams {
		// Calling Broadcast without holding st.mu is allowed for sync.Cond; safe because the watermark was changed under the lock just before.
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
	_ = s.getOrCreateStream(ctx, streamID)
	_, err := s.streamStore.SealStream(ctx, streamID)
	s.checkAggregatedSegmentGC(ctx)
	return err
}

// TrimStreamHistory deletes sealed per-stream files in object storage <= trimSeq, guarded strictly by snapshotSeq.
func (s *Server) TrimStreamHistory(ctx context.Context, streamID uuid.UUID, trimSeq, snapshotSeq uint64) ([]string, error) {
	return s.streamStore.TrimStreamHistory(ctx, streamID, trimSeq, snapshotSeq)
}

func (s *Server) tailPerStream(ctx context.Context, streamID uuid.UUID, req *pb.TailRequest, stream pb.WalBuffer_TailServer) error {
	_ = s.getOrCreateStream(ctx, streamID)
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

	waitChan := make(chan struct{}, 10)
	s.tailMu.Lock()
	s.tailWaiters[waitChan] = struct{}{}
	s.tailMu.Unlock()
	defer func() {
		s.tailMu.Lock()
		delete(s.tailWaiters, waitChan)
		s.tailMu.Unlock()
	}()

	var curOpenFrom uint64
	var openOffset int64
	var openDir string

	for {
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

		// 2. Track open file state and read newly appended bytes
		w.mu.Lock()
		latestOpenFrom := w.openFrom
		openDir = w.dir
		w.mu.Unlock()

		if latestOpenFrom != curOpenFrom {
			curOpenFrom = latestOpenFrom
			openOffset = 0
		}

		if curOpenFrom > 0 {
			openPath := filepath.Join(openDir, wal.FormatStreamFileName(curOpenFrom, 0, false))
			recs, nextOffset, err := ReadRecordsFromOffset(openPath, openOffset, wal.WithAcceptUnversioned(s.cfg.AcceptUnversioned))
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
