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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/wal/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/errutil"
	"github.com/gke-labs/in-cluster-storage/pkg/grpcutil"
	"github.com/gke-labs/in-cluster-storage/pkg/scoped"
	"github.com/gke-labs/in-cluster-storage/pkg/wal"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// Level represents the durability level of an appended record.
type Level int

const (
	Local     Level = iota // fsynced on this node
	Witness                // fsynced on the buffer service
	Permanent              // flushed to permanent object storage
)

func (l Level) String() string {
	switch l {
	case Local:
		return "local"
	case Witness:
		return "witness"
	case Permanent:
		return "permanent"
	default:
		return fmt.Sprintf("Level(%d)", int(l))
	}
}

// ErrDurabilityUnavailable is returned when a durability level requiring a remote target
// (Witness or Permanent) is requested on a stream with no remote target configured.
var ErrDurabilityUnavailable = errors.New("WAL has no remote target")

// ErrRecordDiscarded is returned when a client waiter was waiting on a sequence
// that was discarded due to divergence from authoritative buffer history.
var ErrRecordDiscarded = errors.New("WAL record discarded due to history divergence")

// ErrClosed is returned when an operation is attempted on a closed stream or when the stream is closed while waiting.
var ErrClosed = errors.New("stream closed")

// processClientID is generated once per process and sent in Hello.client_id
// so the WAL buffer can enforce per-process stream writer lock ownership.
var processClientID = uuid.New().String()

// releaseTimeout is the maximum time Close waits for the replication session to
// half-close and end cleanly with the WAL buffer before cancelling it.
const releaseTimeout = 2 * time.Second

// DegradedError indicates that the WAL stream encountered an unrecoverable local store failure
// (such as segment truncation error) and cannot safely accept or wait for further writes.
type DegradedError struct {
	Cause error
	At    time.Time
}

func (e *DegradedError) Error() string {
	if e == nil {
		return ""
	}
	if e.Cause != nil {
		return fmt.Sprintf("WAL stream degraded: %v", e.Cause)
	}
	return "WAL stream degraded"
}

func (e *DegradedError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// Stream is the interface used by node daemons to append to the WAL.
type Stream interface {
	// Append writes locally, then returns the stream_seq.
	Append(ctx context.Context, payload []byte) (uint64, error)
	// Wait blocks until seq has reached level. If requestFlush is true and level is Permanent,
	// a Flush RPC is triggered on the active buffer once seq reaches Witness durability,
	// rather than relying solely on the periodic flush.
	Wait(ctx context.Context, seq uint64, level Level, requestFlush bool) error
	// Flush asks the service to flush to permanent storage and waits for the permanent ack of everything appended so far.
	Flush(ctx context.Context) error
	Watermarks() (local, witness, permanent uint64)
	// RecoveredRecords returns any existing records scanned from local segment files at open time.
	RecoveredRecords() []*wal.ClientRecord
	// ReplicationLevel returns the maximum durability level supported by this stream's replication configuration.
	ReplicationLevel() Level
	// Tail returns an iterator yielding (stream_seq, payload) pairs from the remote buffer service starting after fromSeq.
	Tail(ctx context.Context, fromSeq uint64) (iter.Seq2[uint64, []byte], error)
	// Degraded returns any fatal, unrecoverable stream error encountered by the stream.
	Degraded() *DegradedError
	Close() error
}

// SegmentStore defines the interface for local segment file operations.
type SegmentStore interface {
	Append(rec *wal.ClientRecord) error
	Sync() error
	Close() error
	TruncateAfter(ctx context.Context, seq uint64) error
	DeleteSegmentsBeforeS3Ack(ctx context.Context, seq uint64) error
}

type options struct {
	maxRetainedBytes            int64
	maxSegmentSize              int64
	dialOpts                    []grpc.DialOption
	grpcClient                  pb.WalBufferClient
	acceptUnversioned           bool
	noFsync                     bool
	store                       SegmentStore
	recoveredRecords            []*wal.ClientRecord
	degradedRetryInitialBackoff time.Duration
	degradedRetryMaxBackoff     time.Duration
	clientID                    string
}

// Option configures Stream options.
type Option func(*options)

// WithClientID overrides the per-process client ID sent in Hello.
// Production code does not pass this; tests use it to act as a second process.
func WithClientID(id string) Option {
	return func(o *options) {
		if id != "" {
			o.clientID = id
		}
	}
}

// WithoutFsync configures the local segment store to skip per-record fsync.
// This is intended only for tests and benchmarks where fsync latency is not desired.
func WithoutFsync() Option {
	return func(o *options) {
		o.noFsync = true
	}
}

// WithStore injects a custom SegmentStore and recovered records (useful for testing and fault injection).
func WithStore(store SegmentStore, recovered []*wal.ClientRecord) Option {
	return func(o *options) {
		o.store = store
		o.recoveredRecords = recovered
	}
}

// WithDegradedRetryBackoff sets the initial and max backoff durations for retrying failed divergence truncates.
func WithDegradedRetryBackoff(initial, max time.Duration) Option {
	return func(o *options) {
		o.degradedRetryInitialBackoff = initial
		o.degradedRetryMaxBackoff = max
	}
}

// WithAcceptUnversioned configures whether unversioned legacy segments are accepted.
func WithAcceptUnversioned(accept bool) Option {
	return func(o *options) {
		o.acceptUnversioned = accept
	}
}

// WithAcceptUnversionedSegments is an alias for WithAcceptUnversioned.
func WithAcceptUnversionedSegments(accept bool) Option {
	return WithAcceptUnversioned(accept)
}

// WithMaxRetainedBytes sets the maximum retained bytes before backpressure blocks Append.
func WithMaxRetainedBytes(bytes int64) Option {
	return func(o *options) {
		o.maxRetainedBytes = bytes
	}
}

// WithMaxSegmentSize sets the local segment rotation size.
func WithMaxSegmentSize(bytes int64) Option {
	return func(o *options) {
		o.maxSegmentSize = bytes
	}
}

// WithDialOptions adds gRPC dial options.
func WithDialOptions(opts ...grpc.DialOption) Option {
	return func(o *options) {
		o.dialOpts = append(o.dialOpts, opts...)
	}
}

// WithGRPCClient injects an existing WalBufferClient (useful for unit tests).
func WithGRPCClient(client pb.WalBufferClient) Option {
	return func(o *options) {
		o.grpcClient = client
	}
}

type syncWaiter struct {
	seq          uint64
	level        Level
	requestFlush bool // indicates whether caller requested an active flush for Permanent durability
	done         chan error
}

type streamImpl struct {
	dir      string
	streamID uuid.UUID
	// clientID identifies this process on the WAL buffer for stream writer lock ownership.
	clientID string
	target   string
	opts     options

	store SegmentStore

	mu            sync.RWMutex
	cond          *sync.Cond
	localSeq      uint64
	witnessSeq    uint64
	s3Seq         uint64
	retainedBytes int64
	waiters       []*syncWaiter
	// flushInFlight tracks whether a Flush RPC is currently executing.
	flushInFlight bool
	// flushRequestedWhileInFlight tracks whether a flush was requested while a Flush RPC was in flight.
	flushRequestedWhileInFlight bool
	degradedTruncateSeq         uint64
	reconciling                 bool
	// lastSessionErr records the most recent error from a failed replication session. Guarded by mu.
	lastSessionErr error

	initialHelloAckDone chan struct{}
	initialHelloAckOnce sync.Once
	initialHelloAckErr  error
	degradedErr         *DegradedError

	// Retained in-memory records queue for network sending & replay
	retainedMu      sync.RWMutex
	retainedRecords []*wal.ClientRecord

	newRecordSignal chan struct{}
	cancelCtx       context.Context
	cancelFunc      context.CancelFunc
	// closing is closed by Close to ask the replication loop and active session to end cleanly.
	closing chan struct{}
	// replicationDone is closed when backgroundReplicationLoop returns.
	replicationDone chan struct{}
	// helloAckReceived is set once the first HelloAck is received, so Close only waits
	// for a clean writer-lock release when a lock may have been granted.
	helloAckReceived atomic.Bool
	// releaseTimeout is the maximum time Close waits for a clean writer-lock release.
	releaseTimeout time.Duration
	closed         atomic.Bool
	wg             sync.WaitGroup

	replicationLevel Level
	grpcClient       pb.WalBufferClient
	grpcConn         *grpc.ClientConn
}

// Open opens or recovers a WAL stream on the local node.
func Open(ctx context.Context, dir string, streamID uuid.UUID, target string, opts ...Option) (Stream, error) {
	return openStream(ctx, dir, streamID, target, releaseTimeout, opts...)
}

// openStream opens or recovers a WAL stream using lockReleaseTimeout as the
// maximum time Close waits for a clean writer-lock release before cancelling
// the replication session. Tests call openStream directly to bound that wait.
func openStream(ctx context.Context, dir string, streamID uuid.UUID, target string, lockReleaseTimeout time.Duration, opts ...Option) (Stream, error) {
	var opt options
	opt.maxRetainedBytes = wal.DefaultMaxRetainedBytes
	opt.maxSegmentSize = wal.DefaultMaxSegmentSize
	opt.clientID = processClientID
	for _, o := range opts {
		o(&opt)
	}

	if opt.grpcClient == nil && target == "" {
		return nil, errors.New("walclient: target or WithGRPCClient required")
	}

	var scanOpts []wal.ScanOption
	if opt.acceptUnversioned {
		scanOpts = append(scanOpts, wal.WithAcceptUnversioned(true))
	}
	if opt.noFsync {
		scanOpts = append(scanOpts, wal.WithNoFsync(true))
	}

	var store SegmentStore
	var recovered []*wal.ClientRecord
	if opt.store != nil {
		store = opt.store
		recovered = opt.recoveredRecords
	} else {
		realStore, recs, err := wal.NewClientSegmentStore(dir, fmt.Sprintf("stream-%s", streamID.String()), opt.maxSegmentSize, scanOpts...)
		if err != nil {
			return nil, fmt.Errorf("failed to open local segment store in %s: %w", dir, err)
		}
		store = realStore
		recovered = recs
	}

	var highestLocalSeq uint64
	var retainedBytes int64
	for _, rec := range recovered {
		if rec.StreamSeq > highestLocalSeq {
			highestLocalSeq = rec.StreamSeq
		}
		retainedBytes += int64(wal.ClientHeaderSize + len(rec.Payload))
	}

	logger := scoped.Log(ctx).WithValues("stream", streamID.String())
	cancelCtx, cancelFunc := context.WithCancel(scoped.WithLog(context.Background(), logger))

	s := &streamImpl{
		dir:                 dir,
		streamID:            streamID,
		clientID:            opt.clientID,
		target:              target,
		opts:                opt,
		store:               store,
		localSeq:            highestLocalSeq,
		witnessSeq:          0,
		s3Seq:               0,
		retainedBytes:       retainedBytes,
		retainedRecords:     recovered,
		newRecordSignal:     make(chan struct{}, 1),
		cancelCtx:           cancelCtx,
		cancelFunc:          cancelFunc,
		closing:             make(chan struct{}),
		replicationDone:     make(chan struct{}),
		releaseTimeout:      lockReleaseTimeout,
		replicationLevel:    Permanent,
		initialHelloAckDone: make(chan struct{}),
	}
	s.cond = sync.NewCond(&s.mu)

	if opt.grpcClient != nil {
		s.grpcClient = opt.grpcClient
	} else {
		dialOpts := append(grpcutil.DialOptions(grpc.WithTransportCredentials(insecure.NewCredentials())), opt.dialOpts...)
		conn, err := grpc.NewClient(target, dialOpts...)
		if err != nil {
			cancelFunc()
			errutil.CloseAndLog(store, "client segment store")
			return nil, fmt.Errorf("failed to dial target %s: %w", target, err)
		}
		s.grpcConn = conn
		s.grpcClient = pb.NewWalBufferClient(conn)
	}

	// Start background replication loop
	s.wg.Add(1)
	go s.backgroundReplicationLoop()

	helloTimeout := 5 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		if d := time.Until(deadline); d < helloTimeout {
			helloTimeout = d
		}
	}

	helloTimer := time.NewTimer(helloTimeout)
	defer helloTimer.Stop()

	select {
	case <-s.initialHelloAckDone:
		s.mu.RLock()
		initErr := s.initialHelloAckErr
		isDegraded := s.degradedErr != nil
		s.mu.RUnlock()
		if initErr != nil && !isDegraded {
			errutil.CloseAndLog(s, "wal stream")
			return nil, initErr
		}
	case <-helloTimer.C:
		errutil.CloseAndLog(s, "wal stream")
		s.mu.RLock()
		lastErr := s.lastSessionErr
		s.mu.RUnlock()
		if lastErr != nil {
			return nil, fmt.Errorf("timeout waiting for initial HelloAck from WAL buffer target %s: %w", target, lastErr)
		}
		return nil, fmt.Errorf("timeout waiting for initial HelloAck from WAL buffer target %s", target)
	case <-ctx.Done():
		errutil.CloseAndLog(s, "wal stream")
		s.mu.RLock()
		lastErr := s.lastSessionErr
		s.mu.RUnlock()
		if lastErr != nil {
			return nil, fmt.Errorf("failed to receive initial HelloAck from WAL buffer: %w: %w", ctx.Err(), lastErr)
		}
		return nil, fmt.Errorf("failed to receive initial HelloAck from WAL buffer: %w", ctx.Err())
	}

	return s, nil
}

// Append writes locally without per-record fsync, applying backpressure if retained bytes exceed maxRetainedBytes.
func (s *streamImpl) Append(ctx context.Context, payload []byte) (uint64, error) {
	if s.closed.Load() {
		return 0, ErrClosed
	}

	s.mu.RLock()
	if s.degradedErr != nil {
		err := s.degradedErr
		s.mu.RUnlock()
		return 0, err
	}
	s.mu.RUnlock()

	select {
	case <-s.initialHelloAckDone:
		s.mu.RLock()
		initErr := s.initialHelloAckErr
		s.mu.RUnlock()
		if initErr != nil {
			return 0, initErr
		}
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-s.cancelCtx.Done():
		return 0, ErrClosed
	}

	recordSize := int64(wal.ClientHeaderSize + len(payload))

	// Degraded mode / backpressure / divergence-reconciliation check: block if retained bytes exceed maxRetainedBytes or while reconciling handshake
	s.mu.Lock()
	for (s.reconciling || s.retainedBytes+recordSize > s.opts.maxRetainedBytes) && !s.closed.Load() && s.degradedErr == nil {
		doneCh := make(chan struct{})
		go func() {
			s.mu.Lock()
			for (s.reconciling || s.retainedBytes+recordSize > s.opts.maxRetainedBytes) && !s.closed.Load() && s.degradedErr == nil {
				s.cond.Wait()
			}
			s.mu.Unlock()
			close(doneCh)
		}()

		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-s.cancelCtx.Done():
			return 0, ErrClosed
		case <-doneCh:
		}
		s.mu.Lock()
	}

	if s.closed.Load() {
		s.mu.Unlock()
		return 0, ErrClosed
	}

	if s.degradedErr != nil {
		err := s.degradedErr
		s.mu.Unlock()
		return 0, err
	}

	s.localSeq++
	seq := s.localSeq
	s.retainedBytes += recordSize
	s.mu.Unlock()

	rec := &wal.ClientRecord{
		StreamID:  s.streamID,
		StreamSeq: seq,
		Payload:   payload,
	}

	// 1. Write locally first (fsync is deferred to Wait/Flush at commit time)
	if err := s.store.Append(rec); err != nil {
		s.mu.Lock()
		s.retainedBytes -= recordSize
		s.localSeq--
		s.mu.Unlock()
		return 0, fmt.Errorf("local append failed: %w", err)
	}

	// 2. Add to retained queue for background network replication
	s.retainedMu.Lock()
	s.retainedRecords = append(s.retainedRecords, rec)
	s.retainedMu.Unlock()

	// Signal background sender
	select {
	case s.newRecordSignal <- struct{}{}:
	default:
	}

	return seq, nil
}

// waitFor blocks until seq has reached level (Witness or Permanent).
// If requestFlush is true and level is Permanent, triggerFlushLocked is called.
func (s *streamImpl) waitFor(ctx context.Context, seq uint64, level Level, requestFlush bool) error {
	if level > s.replicationLevel {
		return fmt.Errorf("durability level %s requested but %w", level, ErrDurabilityUnavailable)
	}

	s.mu.Lock()
	if s.degradedErr != nil {
		err := s.degradedErr
		s.mu.Unlock()
		return err
	}
	if s.closed.Load() {
		s.mu.Unlock()
		return ErrClosed
	}
	if seq > s.localSeq {
		s.mu.Unlock()
		return fmt.Errorf("sequence %d was discarded due to divergence: %w", seq, ErrRecordDiscarded)
	}
	if level == Witness && s.witnessSeq >= seq {
		s.mu.Unlock()
		return nil
	}
	if level == Permanent && s.s3Seq >= seq {
		s.mu.Unlock()
		return nil
	}

	w := &syncWaiter{
		seq:          seq,
		level:        level,
		requestFlush: requestFlush,
		done:         make(chan error, 1),
	}
	s.waiters = append(s.waiters, w)
	if level == Permanent && requestFlush {
		s.triggerFlushLocked()
	}
	s.mu.Unlock()

	select {
	case <-ctx.Done():
		s.mu.Lock()
		s.removeWaiterLocked(w)
		s.mu.Unlock()
		return ctx.Err()
	case <-s.cancelCtx.Done():
		return ErrClosed
	case err := <-w.done:
		return err
	}
}

// triggerFlushLocked initiates a Flush RPC on the active WAL buffer if there
// are pending permanent waiters that requested a flush and whose records have
// reached witness durability. s.mu must be held by the caller. The Flush RPC
// runs asynchronously in its own goroutine with no locks held.
func (s *streamImpl) triggerFlushLocked() {
	if s.closed.Load() {
		return
	}
	if s.flushInFlight {
		s.flushRequestedWhileInFlight = true
		return
	}
	var needed bool
	for _, w := range s.waiters {
		if w.level == Permanent && w.requestFlush && w.seq <= s.witnessSeq && w.seq > s.s3Seq {
			needed = true
			break
		}
	}
	if !needed {
		return
	}
	s.flushInFlight = true
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		ctx, cancel := context.WithTimeout(s.cancelCtx, 30*time.Second)
		defer cancel()

		_, err := s.grpcClient.Flush(ctx, &pb.FlushRequest{})
		if err != nil && status.Code(err) != codes.Canceled && !errors.Is(err, context.Canceled) {
			scoped.Log(s.cancelCtx).Info("WalBuffer.Flush RPC error (will still wait for permanent ack)", "err", err, "stream", s.streamID)
		}

		s.mu.Lock()
		s.flushInFlight = false
		if s.flushRequestedWhileInFlight {
			s.flushRequestedWhileInFlight = false
			s.triggerFlushLocked()
		}
		s.mu.Unlock()
	}()
}

func (s *streamImpl) removeWaiterLocked(target *syncWaiter) {
	for i, w := range s.waiters {
		if w == target {
			s.waiters = append(s.waiters[:i], s.waiters[i+1:]...)
			break
		}
	}
}

func (s *streamImpl) notifyWaitersLocked() {
	var remaining []*syncWaiter
	for _, w := range s.waiters {
		if (w.level == Witness && s.witnessSeq >= w.seq) || (w.level == Permanent && s.s3Seq >= w.seq) {
			w.done <- nil
		} else {
			remaining = append(remaining, w)
		}
	}
	s.waiters = remaining
}

// Wait blocks until seq has reached level. If requestFlush is true and level is Permanent,
// a Flush RPC is triggered on the active buffer once seq reaches Witness durability,
// rather than relying solely on the periodic flush.
func (s *streamImpl) Wait(ctx context.Context, seq uint64, level Level, requestFlush bool) error {
	if s.closed.Load() {
		return ErrClosed
	}

	s.mu.RLock()
	if s.degradedErr != nil {
		err := s.degradedErr
		s.mu.RUnlock()
		return err
	}
	local := s.localSeq
	s.mu.RUnlock()

	if seq > local {
		return fmt.Errorf("seq %d has not been appended locally or was discarded due to divergence (local=%d): %w", seq, local, ErrRecordDiscarded)
	}

	if level != Local && level != Witness && level != Permanent {
		return fmt.Errorf("invalid durability level: %v", level)
	}

	if s.store != nil {
		if err := s.store.Sync(); err != nil {
			return fmt.Errorf("local fsync failed: %w", err)
		}
	}

	if level == Local {
		return nil
	}

	if level > s.replicationLevel {
		return fmt.Errorf("durability level %s requested but %w", level, ErrDurabilityUnavailable)
	}

	// 1. Always reach Witness first, even for Permanent.
	if err := s.waitFor(ctx, seq, Witness, false); err != nil {
		return err
	}
	if level == Witness {
		return nil
	}

	// 2. Request flush and wait for the permanent watermark.
	// Reaching Witness on the current server guarantees the record is in that server's
	// unflushed set. However, across a buffer restart, the replacement server only has
	// the record in its unflushed set after reconnect and replay witness acknowledgment,
	// at which point triggerFlushLocked re-requests the flush.
	return s.waitFor(ctx, seq, Permanent, requestFlush)
}

// Flush asks the service to flush to permanent storage and waits for the permanent ack of everything appended so far.
func (s *streamImpl) Flush(ctx context.Context) error {
	if s.closed.Load() {
		return ErrClosed
	}

	s.mu.RLock()
	if s.degradedErr != nil {
		err := s.degradedErr
		s.mu.RUnlock()
		return err
	}
	targetSeq := s.localSeq
	s3 := s.s3Seq
	s.mu.RUnlock()

	if targetSeq == 0 || s3 >= targetSeq {
		return nil
	}

	if s.store != nil {
		if err := s.store.Sync(); err != nil {
			return fmt.Errorf("local fsync failed: %w", err)
		}
	}

	if Permanent > s.replicationLevel {
		return fmt.Errorf("durability level permanent requested but %w", ErrDurabilityUnavailable)
	}

	// 1. Wait for everything appended so far to reach Witness
	if err := s.waitFor(ctx, targetSeq, Witness, false); err != nil {
		return err
	}

	// 2. Request flush and wait for permanent watermark
	return s.waitFor(ctx, targetSeq, Permanent, true)
}

// ReplicationLevel returns the maximum durability level supported by this stream's replication configuration.
func (s *streamImpl) ReplicationLevel() Level {
	return s.replicationLevel
}

// Watermarks returns current local, witness, and permanent watermarks.
func (s *streamImpl) Watermarks() (local, witness, permanent uint64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.localSeq, s.witnessSeq, s.s3Seq
}

// RecoveredRecords returns any existing records scanned from local segment files at open time.
func (s *streamImpl) RecoveredRecords() []*wal.ClientRecord {
	s.retainedMu.RLock()
	defer s.retainedMu.RUnlock()
	recs := make([]*wal.ClientRecord, len(s.retainedRecords))
	copy(recs, s.retainedRecords)
	return recs
}

// Degraded returns any fatal, unrecoverable stream error encountered by the stream.
func (s *streamImpl) Degraded() *DegradedError {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.degradedErr
}

func (s *streamImpl) setDegradedLocked(ctx context.Context, cause error, truncateSeq uint64) *DegradedError {
	if s.degradedErr != nil {
		return s.degradedErr
	}
	s.degradedErr = &DegradedError{
		Cause: cause,
		At:    time.Now(),
	}
	s.degradedTruncateSeq = truncateSeq
	s.reconciling = true
	scoped.Log(ctx).Error(s.degradedErr, "WAL client stream degraded")
	for _, w := range s.waiters {
		w.done <- s.degradedErr
	}
	s.waiters = nil
	if s.cond != nil {
		s.cond.Broadcast()
	}
	return s.degradedErr
}

// Close closes the stream, asking the active replication session to end cleanly
// so the server releases the writer lock, then stopping background tasks and closing local store.
func (s *streamImpl) Close() error {
	if s.closed.Swap(true) {
		return nil
	}

	close(s.closing)
	s.initialHelloAckOnce.Do(func() {
		close(s.initialHelloAckDone)
	})
	s.mu.Lock()
	for _, w := range s.waiters {
		w.done <- ErrClosed
	}
	s.waiters = nil
	s.cond.Broadcast()
	s.mu.Unlock()

	if s.helloAckReceived.Load() {
		releaseTimer := time.NewTimer(s.releaseTimeout)
		select {
		case <-s.replicationDone:
			releaseTimer.Stop()
		case <-releaseTimer.C:
			scoped.Log(s.cancelCtx).Info("timed out waiting for WAL replication session to release writer lock; cancelling",
				"timeout", s.releaseTimeout)
		}
	}

	s.cancelFunc()
	s.wg.Wait()

	if s.grpcConn != nil {
		errutil.CloseAndLog(s.grpcConn, "grpc connection")
	}

	if s.store != nil {
		return s.store.Close()
	}
	return nil
}

func (s *streamImpl) retryDegradedTruncate(ctx context.Context) bool {
	backoff := 1 * time.Second
	if s.opts.degradedRetryInitialBackoff > 0 {
		backoff = s.opts.degradedRetryInitialBackoff
	}
	maxBackoff := 1 * time.Minute
	if s.opts.degradedRetryMaxBackoff > 0 {
		maxBackoff = s.opts.degradedRetryMaxBackoff
	}

	for {
		select {
		case <-s.cancelCtx.Done():
			return false
		case <-s.closing:
			return false
		case <-time.After(backoff):
		}

		s.mu.RLock()
		if s.degradedErr == nil {
			s.mu.RUnlock()
			return true
		}
		targetSeq := s.degradedTruncateSeq
		degradedAt := s.degradedErr.At
		s.mu.RUnlock()

		if s.store != nil {
			if err := s.store.TruncateAfter(ctx, targetSeq); err != nil {
				scoped.Log(ctx).V(4).Info("WAL client stream retry truncate failed",
					"targetSeq", targetSeq, "err", err, "backoff", backoff)
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
				continue
			}
		}

		// Truncate succeeded: clear degraded state, log once at info, and return to loop to re-enter handshake
		s.mu.Lock()
		s.degradedErr = nil
		s.initialHelloAckErr = nil
		s.cond.Broadcast()
		s.mu.Unlock()

		scoped.Log(ctx).Info("WAL client stream recovered from degraded state",
			"duration", time.Since(degradedAt))
		return true
	}
}

func (s *streamImpl) backgroundReplicationLoop() {
	defer close(s.replicationDone)
	defer s.wg.Done()

	backoff := 50 * time.Millisecond
	maxBackoff := 2 * time.Second
	firstFailure := true

	for {
		select {
		case <-s.cancelCtx.Done():
			return
		case <-s.closing:
			return
		default:
		}

		s.mu.RLock()
		degErr := s.degradedErr
		s.mu.RUnlock()
		if degErr != nil {
			if !s.retryDegradedTruncate(s.cancelCtx) {
				return
			}
			backoff = 50 * time.Millisecond
			firstFailure = true
			continue
		}

		err := s.runStreamSession()
		if err != nil && !errors.Is(err, context.Canceled) && status.Code(err) != codes.Canceled {
			s.mu.Lock()
			s.lastSessionErr = err
			s.mu.Unlock()
		}
		if _, resend := grpcutil.ResendDelay(err); resend && !s.helloAckReceived.Load() {
			s.initialHelloAckOnce.Do(func() {
				s.mu.Lock()
				s.initialHelloAckErr = err
				s.mu.Unlock()
				close(s.initialHelloAckDone)
			})
			return
		}
		if err != nil && !s.closed.Load() {
			s.mu.RLock()
			degErr := s.degradedErr
			s.mu.RUnlock()
			if degErr != nil {
				if !s.retryDegradedTruncate(s.cancelCtx) {
					return
				}
				backoff = 50 * time.Millisecond
				firstFailure = true
				continue
			}

			if firstFailure || backoff >= maxBackoff {
				scoped.Log(s.cancelCtx).Info("WAL client stream replication session failed",
					"err", err, "backoff", backoff)
				firstFailure = false
			} else {
				scoped.Log(s.cancelCtx).V(4).Info("WAL client replication session ended",
					"err", err, "backoff", backoff)
			}

			select {
			case <-s.cancelCtx.Done():
				return
			case <-s.closing:
				return
			case <-time.After(backoff):
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
			}
		} else {
			backoff = 50 * time.Millisecond
			firstFailure = true
		}
	}
}

func (s *streamImpl) runStreamSession() error {
	select {
	case <-s.closing:
		return nil
	default:
	}

	ctx, cancel := context.WithCancel(s.cancelCtx)
	defer cancel()

	client := s.grpcClient
	if client == nil {
		return errors.New("gRPC client is nil")
	}

	stream, err := client.Append(ctx)
	if err != nil {
		return fmt.Errorf("failed to call WalBuffer.Append: %w", err)
	}

	// 1. Send Hello
	err = stream.Send(&pb.AppendRequest{
		Msg: &pb.AppendRequest_Hello{
			Hello: &pb.Hello{
				StreamId: s.streamID[:],
				ClientId: []byte(s.clientID),
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to send Hello: %w", err)
	}

	// 2. Receive HelloAck
	resp, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("failed to receive HelloAck: %w", err)
	}
	helloAck := resp.GetHelloAck()
	if helloAck == nil {
		return fmt.Errorf("expected HelloAck as first response, got: %+v", resp)
	}
	s.helloAckReceived.Store(true)

	if err := s.handleHelloAck(ctx, helloAck); err != nil {
		s.initialHelloAckOnce.Do(func() {
			s.initialHelloAckErr = err
			close(s.initialHelloAckDone)
		})
		return fmt.Errorf("handleHelloAck failed: %w", err)
	}
	s.mu.Lock()
	s.initialHelloAckErr = nil
	s.lastSessionErr = nil
	s.mu.Unlock()
	s.initialHelloAckOnce.Do(func() {
		close(s.initialHelloAckDone)
	})

	// The stream ID is kept in the message string because tests/e2e matches it verbatim.
	if s.target != "" {
		scoped.Log(ctx).Info(fmt.Sprintf("WAL client stream %s: replication session established", s.streamID),
			"target", s.target)
	} else {
		scoped.Log(ctx).Info(fmt.Sprintf("WAL client stream %s: replication session established", s.streamID))
	}

	// 3. Concurrently receive Acks while replaying and sending newly appended records.
	// Starting the receiver before replay ensures replayed records can be acked and drained
	// without filling gRPC/HTTP2 flow control buffers.
	recvErrCh := make(chan error, 1)
	var firstAck sync.Once
	go func() {
		for {
			ackResp, err := stream.Recv()
			if err != nil {
				recvErrCh <- err
				return
			}
			if ack := ackResp.GetAck(); ack != nil {
				firstAck.Do(func() {
					scoped.Log(ctx).Info("WAL client stream first ack after reconnect",
						"witnessWatermark", ack.WitnessAckedStreamSeq, "s3Watermark", ack.S3AckedStreamSeq)
				})
				s.handleAck(ctx, ack)
			}
		}
	}()

	closeCleanly := func() error {
		if err := stream.CloseSend(); err != nil {
			return fmt.Errorf("failed to half-close Append stream: %w", err)
		}
		select {
		case err := <-recvErrCh:
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	select {
	case <-s.closing:
		return closeCleanly()
	default:
	}

	// 4. Replay unacknowledged records (stream_seq > witnessAckedStreamSeq)
	s.retainedMu.RLock()
	var recordsToReplay []*wal.ClientRecord
	for _, r := range s.retainedRecords {
		if r.StreamSeq > helloAck.WitnessAckedStreamSeq {
			recordsToReplay = append(recordsToReplay, r)
		}
	}
	s.retainedMu.RUnlock()

	if len(recordsToReplay) > 0 {
		scoped.Log(ctx).Info("WAL client stream replaying unacknowledged records",
			"count", len(recordsToReplay), "fromSeq", recordsToReplay[0].StreamSeq, "toSeq", recordsToReplay[len(recordsToReplay)-1].StreamSeq)
	} else {
		scoped.Log(ctx).Info("WAL client stream no unacknowledged records to replay")
	}

	for _, rec := range recordsToReplay {
		select {
		case <-s.closing:
			return closeCleanly()
		default:
		}
		if err := stream.Send(&pb.AppendRequest{
			Msg: &pb.AppendRequest_Record{
				Record: rec.ToAppendProto(),
			},
		}); err != nil {
			return fmt.Errorf("failed to replay record %d: %w", rec.StreamSeq, err)
		}
	}

	nextSeqToSend := uint64(0)
	if len(recordsToReplay) > 0 {
		nextSeqToSend = recordsToReplay[len(recordsToReplay)-1].StreamSeq + 1
	} else {
		nextSeqToSend = helloAck.WitnessAckedStreamSeq + 1
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.closing:
			return closeCleanly()
		case err := <-recvErrCh:
			return err
		case <-s.newRecordSignal:
			for {
				s.retainedMu.RLock()
				var nextRec *wal.ClientRecord
				for _, r := range s.retainedRecords {
					if r.StreamSeq >= nextSeqToSend {
						nextRec = r
						break
					}
				}
				s.retainedMu.RUnlock()

				if nextRec == nil {
					break
				}

				if err := stream.Send(&pb.AppendRequest{
					Msg: &pb.AppendRequest_Record{
						Record: nextRec.ToAppendProto(),
					},
				}); err != nil {
					return fmt.Errorf("failed to send record %d: %w", nextRec.StreamSeq, err)
				}
				nextSeqToSend = nextRec.StreamSeq + 1
			}
		}
	}
}

func (s *streamImpl) handleHelloAck(ctx context.Context, helloAck *pb.HelloAck) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	witnessHead := helloAck.WitnessAckedStreamSeq
	scoped.Log(ctx).Info("WAL client stream HelloAck received",
		"witnessWatermark", witnessHead, "s3Watermark", helloAck.S3AckedStreamSeq, "localHead", s.localSeq)
	if witnessHead > s.localSeq {
		scoped.Log(ctx).Info("buffer witness watermark is ahead of local stream head; continuing sequence numbering from buffer head",
			"witnessWatermark", witnessHead, "localHead", s.localSeq)
		s.localSeq = witnessHead
	} else if witnessHead > 0 && s.localSeq > witnessHead {
		// Local history has records above the buffer's witness watermark.
		// Check if local record at witnessHead matches the authoritative buffer record.
		s.retainedMu.RLock()
		var localRecAtHead *wal.ClientRecord
		for _, r := range s.retainedRecords {
			if r.StreamSeq == witnessHead {
				localRecAtHead = r
				break
			}
		}
		s.retainedMu.RUnlock()

		if localRecAtHead != nil {
			// Query buffer for record at witnessHead.
			// Release s.mu while tailing buffer to avoid blocking callers/cond.
			s.mu.Unlock()
			tailIter, err := s.Tail(s.cancelCtx, witnessHead-1)
			if err != nil {
				s.mu.Lock()
				return fmt.Errorf("failed to tail buffer at witness head %d: %w", witnessHead, err)
			}
			var bufferPayload []byte
			var found bool
			for seq, payload := range tailIter {
				if seq == witnessHead {
					bufferPayload = payload
					found = true
					break
				}
			}
			s.mu.Lock()
			if !found {
				return fmt.Errorf("record at witness head %d not found in buffer stream tail", witnessHead)
			}

			// Only discard if buffer returns a record at witnessHead and its content differs
			if !bytes.Equal(localRecAtHead.Payload, bufferPayload) {
				// 1. Order on-disk truncate before the memory mutation so that a failure
				// leaves memory and disk consistent, allowing divergence to be re-detected on retry.
				if s.store != nil {
					if err := s.store.TruncateAfter(ctx, witnessHead); err != nil {
						truncErr := s.setDegradedLocked(ctx, fmt.Errorf("failed to truncate local segment store to %d: %w", witnessHead, err), witnessHead)
						s.initialHelloAckOnce.Do(func() {
							s.initialHelloAckErr = truncErr
							close(s.initialHelloAckDone)
						})
						return truncErr
					}
				}

				// 2. Only trim in-memory retained records and localSeq once on-disk truncation succeeds
				s.retainedMu.Lock()
				var remaining []*wal.ClientRecord
				var newRetainedBytes int64
				discarded := 0
				for _, r := range s.retainedRecords {
					if r.StreamSeq <= witnessHead {
						remaining = append(remaining, r)
						newRetainedBytes += int64(wal.ClientHeaderSize + len(r.Payload))
					} else {
						discarded++
					}
				}
				s.retainedRecords = remaining
				s.retainedMu.Unlock()
				s.retainedBytes = newRetainedBytes
				s.localSeq = witnessHead

				// Fail waiters for discarded sequences
				var remainingWaiters []*syncWaiter
				for _, w := range s.waiters {
					if w.seq > witnessHead {
						w.done <- fmt.Errorf("sequence %d was discarded due to divergence: %w", w.seq, ErrRecordDiscarded)
					} else {
						remainingWaiters = append(remainingWaiters, w)
					}
				}
				s.waiters = remainingWaiters

				scoped.Log(ctx).Info("local records above buffer witness watermark diverged from authoritative buffer history",
					"witnessWatermark", witnessHead, "discarded", discarded)

				s.reconciling = false
				if s.cond != nil {
					s.cond.Broadcast()
				}
			}
		}
	}

	if witnessHead < s.witnessSeq {
		scoped.Log(ctx).Info("witness watermark dropped on restart (replaying unacknowledged records)",
			"from", s.witnessSeq, "to", witnessHead)
	}
	s.witnessSeq = witnessHead

	if helloAck.S3AckedStreamSeq > s.s3Seq {
		s.s3Seq = helloAck.S3AckedStreamSeq
		s.cleanupS3AckedLocked(ctx, helloAck.S3AckedStreamSeq)
	}

	s.notifyWaitersLocked()
	s.reconciling = false
	if s.cond != nil {
		s.cond.Broadcast()
	}
	s.triggerFlushLocked()
	s.initialHelloAckOnce.Do(func() {
		close(s.initialHelloAckDone)
	})
	return nil
}

func (s *streamImpl) handleAck(ctx context.Context, ack *pb.Ack) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if ack.WitnessAckedStreamSeq > s.witnessSeq {
		s.witnessSeq = ack.WitnessAckedStreamSeq
	}
	if ack.S3AckedStreamSeq > s.s3Seq {
		s.s3Seq = ack.S3AckedStreamSeq
		s.cleanupS3AckedLocked(ctx, ack.S3AckedStreamSeq)
	}

	s.notifyWaitersLocked()

	if s.cond != nil {
		s.cond.Broadcast()
	}

	s.triggerFlushLocked()
}

func (s *streamImpl) cleanupS3AckedLocked(ctx context.Context, s3AckedSeq uint64) {
	// Clean up local segment files where all records are <= s3AckedSeq
	if err := s.store.DeleteSegmentsBeforeS3Ack(ctx, s3AckedSeq); err != nil {
		scoped.Log(ctx).Info("failed to delete S3-acked segment files", "err", err)
	}

	// Clean up in-memory retained records
	s.retainedMu.Lock()
	var remaining []*wal.ClientRecord
	var newRetainedBytes int64
	for _, rec := range s.retainedRecords {
		if rec.StreamSeq > s3AckedSeq {
			remaining = append(remaining, rec)
			newRetainedBytes += int64(wal.ClientHeaderSize + len(rec.Payload))
		}
	}
	s.retainedRecords = remaining
	s.retainedMu.Unlock()

	s.retainedBytes = newRetainedBytes
}

// Tail returns an iterator yielding (stream_seq, payload) pairs from the remote buffer service starting after fromSeq.
func (s *streamImpl) Tail(ctx context.Context, fromSeq uint64) (iter.Seq2[uint64, []byte], error) {
	return TailStream(ctx, s.target, s.streamID, fromSeq, WithGRPCClient(s.grpcClient), WithDialOptions(s.opts.dialOpts...))
}

// TailStream tails records belonging to a specific stream starting after fromSeq (stream_seq > fromSeq).
// It returns an iterator yielding (stream_seq, payload) pairs.
func TailStream(ctx context.Context, target string, streamID uuid.UUID, fromSeq uint64, opts ...Option) (iter.Seq2[uint64, []byte], error) {
	var opt options
	for _, o := range opts {
		o(&opt)
	}

	if opt.grpcClient == nil && target == "" {
		return nil, errors.New("target or WithGRPCClient must be provided")
	}

	iterator := func(yield func(uint64, []byte) bool) {
		var localConn *grpc.ClientConn
		grpcClient := opt.grpcClient
		if grpcClient == nil {
			dialOpts := append(grpcutil.DialOptions(grpc.WithTransportCredentials(insecure.NewCredentials())), opt.dialOpts...)
			conn, err := grpc.NewClient(target, dialOpts...)
			if err != nil {
				scoped.Log(ctx).Error(err, "TailStream failed to dial target", "target", target, "stream", streamID)
				return
			}
			localConn = conn
			defer errutil.CloseAndLog(localConn, "grpc connection")
			grpcClient = pb.NewWalBufferClient(conn)
		}

		tailCtx, tailCancel := context.WithCancel(ctx)
		defer tailCancel()

		tailStream, err := grpcClient.Tail(tailCtx, &pb.TailRequest{
			StreamId:      streamID[:],
			FromStreamSeq: fromSeq,
		})
		if err != nil {
			scoped.Log(tailCtx).V(4).Info("TailStream RPC failed", "stream", streamID, "err", err)
			return
		}

		for {
			resp, err := tailStream.Recv()
			if err != nil {
				return
			}
			rec := resp.GetRecord()
			if rec == nil {
				continue
			}
			if !yield(rec.StreamSeq, rec.Payload) {
				return
			}
		}
	}

	return iterator, nil
}

// MoveSegmentsAside moves all segment files and any .recover backup files for streamID
// in dir aside into a subdirectory named stream-<id>.<unix-ts>.degraded.
// It returns the path to the degraded directory.
func MoveSegmentsAside(dir string, streamID uuid.UUID) (string, error) {
	if dir == "" {
		return "", nil
	}

	targetDir := filepath.Join(dir, fmt.Sprintf("stream-%s.%d.degraded", streamID.String(), time.Now().Unix()))
	if _, err := os.Stat(targetDir); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("failed to stat degraded directory %s: %w", targetDir, err)
		}
	} else {
		targetDir = filepath.Join(dir, fmt.Sprintf("stream-%s.%d.degraded", streamID.String(), time.Now().UnixNano()))
	}

	if err := os.MkdirAll(targetDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create degraded directory %s: %w", targetDir, err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return targetDir, nil
		}
		return "", fmt.Errorf("failed to read directory %s: %w", dir, err)
	}

	prefix := fmt.Sprintf("stream-%s-", streamID.String())
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasPrefix(name, prefix) && (strings.HasSuffix(name, ".wal") || strings.Contains(name, ".recover")) {
			src := filepath.Join(dir, name)
			dst := filepath.Join(targetDir, name)
			if err := os.Rename(src, dst); err != nil {
				return "", fmt.Errorf("failed to move segment file %s aside to %s: %w", src, dst, err)
			}
		}
	}

	return targetDir, nil
}
