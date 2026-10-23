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
	"iter"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/wal/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/errutil"
	"github.com/gke-labs/in-cluster-storage/pkg/grpcutil"
	"github.com/gke-labs/in-cluster-storage/pkg/wal"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"k8s.io/klog/v2"
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
	// Append writes and fsyncs locally, then returns the stream_seq.
	Append(ctx context.Context, payload []byte) (uint64, error)
	// Wait blocks until seq has reached level. If requestFlush is true and level is Permanent,
	// a Flush RPC is issued (coalesced server-side) before waiting instead of relying on the periodic flush.
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

type options struct {
	maxRetainedBytes  int64
	maxSegmentSize    int64
	dialOpts          []grpc.DialOption
	grpcClient        pb.WalBufferClient
	acceptUnversioned bool
}

// Option configures Stream options.
type Option func(*options)

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
	seq   uint64
	level Level
	done  chan error
}

type streamImpl struct {
	dir      string
	streamID uuid.UUID
	target   string
	opts     options

	store *wal.ClientSegmentStore

	mu            sync.RWMutex
	cond          *sync.Cond
	localSeq      uint64
	witnessSeq    uint64
	s3Seq         uint64
	retainedBytes int64
	waiters       []*syncWaiter

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
	closed          atomic.Bool
	wg              sync.WaitGroup

	replicationLevel Level
	grpcClient       pb.WalBufferClient
	grpcConn         *grpc.ClientConn
}

// Open opens or recovers a WAL stream on the local node.
func Open(ctx context.Context, dir string, streamID uuid.UUID, target string, opts ...Option) (Stream, error) {
	var opt options
	opt.maxRetainedBytes = wal.DefaultMaxRetainedBytes
	opt.maxSegmentSize = wal.DefaultMaxSegmentSize
	for _, o := range opts {
		o(&opt)
	}

	var scanOpts []wal.ScanOption
	if opt.acceptUnversioned {
		scanOpts = append(scanOpts, wal.WithAcceptUnversioned(true))
	}

	store, recovered, err := wal.NewClientSegmentStore(dir, fmt.Sprintf("stream-%s", streamID.String()), opt.maxSegmentSize, scanOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to open local segment store in %s: %w", dir, err)
	}

	var highestLocalSeq uint64
	var retainedBytes int64
	for _, rec := range recovered {
		if rec.StreamSeq > highestLocalSeq {
			highestLocalSeq = rec.StreamSeq
		}
		retainedBytes += int64(wal.ClientHeaderSize + len(rec.Payload))
	}

	cancelCtx, cancelFunc := context.WithCancel(context.Background())

	replicationLevel := Local
	if opt.grpcClient != nil || target != "" {
		replicationLevel = Permanent
	}

	s := &streamImpl{
		dir:                 dir,
		streamID:            streamID,
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
		replicationLevel:    replicationLevel,
		initialHelloAckDone: make(chan struct{}),
	}
	s.cond = sync.NewCond(&s.mu)

	if opt.grpcClient != nil {
		s.grpcClient = opt.grpcClient
	} else if target != "" {
		dialOpts := append(grpcutil.DialOptions(grpc.WithTransportCredentials(insecure.NewCredentials())), opt.dialOpts...)
		conn, err := grpc.NewClient(target, dialOpts...)
		if err != nil {
			cancelFunc()
			errutil.CloseAndLog(store, "client segment store")
			return nil, fmt.Errorf("failed to dial target %s: %w", target, err)
		}
		s.grpcConn = conn
		s.grpcClient = pb.NewWalBufferClient(conn)
	} else {
		s.initialHelloAckOnce.Do(func() {
			close(s.initialHelloAckDone)
		})
	}

	// Start background replication loop
	if s.grpcClient != nil {
		s.wg.Add(1)
		go s.backgroundReplicationLoop()

		helloTimeout := 5 * time.Second
		if deadline, ok := ctx.Deadline(); ok {
			if d := time.Until(deadline); d < helloTimeout {
				helloTimeout = d
			}
		}

		select {
		case <-s.initialHelloAckDone:
			if s.initialHelloAckErr != nil {
				errutil.CloseAndLog(s, "wal stream")
				return nil, s.initialHelloAckErr
			}
		case <-time.After(helloTimeout):
			errutil.CloseAndLog(s, "wal stream")
			return nil, fmt.Errorf("timeout waiting for initial HelloAck from WAL buffer target %s", target)
		case <-ctx.Done():
			errutil.CloseAndLog(s, "wal stream")
			return nil, fmt.Errorf("failed to receive initial HelloAck from WAL buffer: %w", ctx.Err())
		}
	}

	return s, nil
}

// Append writes and fsyncs locally, applying backpressure if retained bytes exceed maxRetainedBytes.
func (s *streamImpl) Append(ctx context.Context, payload []byte) (uint64, error) {
	if s.closed.Load() {
		return 0, errors.New("stream closed")
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
		if s.initialHelloAckErr != nil {
			return 0, s.initialHelloAckErr
		}
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-s.cancelCtx.Done():
		return 0, errors.New("stream closed")
	}

	recordSize := int64(wal.ClientHeaderSize + len(payload))

	// Degraded mode / backpressure check: block if retained bytes exceed maxRetainedBytes
	s.mu.Lock()
	for s.retainedBytes+recordSize > s.opts.maxRetainedBytes && !s.closed.Load() && s.degradedErr == nil {
		doneCh := make(chan struct{})
		go func() {
			s.mu.Lock()
			for s.retainedBytes+recordSize > s.opts.maxRetainedBytes && !s.closed.Load() && s.degradedErr == nil {
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
			return 0, errors.New("stream closed")
		case <-doneCh:
		}
		s.mu.Lock()
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

	// 1. Write and fsync locally first
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
func (s *streamImpl) waitFor(ctx context.Context, seq uint64, level Level) error {
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
		return errors.New("stream closed")
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
		seq:   seq,
		level: level,
		done:  make(chan error, 1),
	}
	s.waiters = append(s.waiters, w)
	s.mu.Unlock()

	select {
	case <-ctx.Done():
		s.mu.Lock()
		s.removeWaiterLocked(w)
		s.mu.Unlock()
		return ctx.Err()
	case <-s.cancelCtx.Done():
		return errors.New("stream closed")
	case err := <-w.done:
		return err
	}
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
// a Flush RPC is issued (coalesced server-side) before waiting instead of relying on the periodic flush.
func (s *streamImpl) Wait(ctx context.Context, seq uint64, level Level, requestFlush bool) error {
	if s.closed.Load() {
		return errors.New("stream closed")
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

	if level == Local {
		return nil
	}

	if level > s.replicationLevel {
		return fmt.Errorf("durability level %s requested but %w", level, ErrDurabilityUnavailable)
	}

	// 1. Always reach Witness first, even for Permanent.
	if err := s.waitFor(ctx, seq, Witness); err != nil {
		return err
	}
	if level == Witness {
		return nil
	}

	// 2. Only now is the record guaranteed to be in the server's unflushed set
	//    (commitItems appends to unflushedRecords before sending the ack).
	if requestFlush && s.grpcClient != nil {
		if _, err := s.grpcClient.Flush(ctx, &pb.FlushRequest{}); err != nil {
			klog.Warningf("WalBuffer.Flush RPC error (will still wait for permanent ack): %v", err)
		}
	}

	// 3. Wait for the permanent watermark.
	return s.waitFor(ctx, seq, Permanent)
}

// Flush asks the service to flush to permanent storage and waits for the permanent ack of everything appended so far.
func (s *streamImpl) Flush(ctx context.Context) error {
	if s.closed.Load() {
		return errors.New("stream closed")
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

	if Permanent > s.replicationLevel {
		return fmt.Errorf("durability level permanent requested but %w", ErrDurabilityUnavailable)
	}

	// 1. Wait for everything appended so far to reach Witness
	if err := s.waitFor(ctx, targetSeq, Witness); err != nil {
		return err
	}

	// 2. Issue Flush RPC
	if s.grpcClient != nil {
		if _, err := s.grpcClient.Flush(ctx, &pb.FlushRequest{}); err != nil {
			klog.Warningf("WalBuffer.Flush RPC error (will still wait for permanent ack): %v", err)
		}
	}

	// 3. Wait for permanent watermark
	return s.waitFor(ctx, targetSeq, Permanent)
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

func (s *streamImpl) setDegradedLocked(cause error) *DegradedError {
	if s.degradedErr != nil {
		return s.degradedErr
	}
	s.degradedErr = &DegradedError{
		Cause: cause,
		At:    time.Now(),
	}
	klog.Errorf("WAL client stream %s degraded: %v", s.streamID, s.degradedErr)
	for _, w := range s.waiters {
		w.done <- s.degradedErr
	}
	s.waiters = nil
	if s.cond != nil {
		s.cond.Broadcast()
	}
	return s.degradedErr
}

// Close closes the stream, stopping background tasks and closing local store.
func (s *streamImpl) Close() error {
	if s.closed.Swap(true) {
		return nil
	}

	s.cancelFunc()
	s.initialHelloAckOnce.Do(func() {
		close(s.initialHelloAckDone)
	})
	s.mu.Lock()
	for _, w := range s.waiters {
		w.done <- errors.New("stream closed")
	}
	s.waiters = nil
	s.cond.Broadcast()
	s.mu.Unlock()

	s.wg.Wait()

	if s.grpcConn != nil {
		errutil.CloseAndLog(s.grpcConn, "grpc connection")
	}

	if s.store != nil {
		return s.store.Close()
	}
	return nil
}

func (s *streamImpl) backgroundReplicationLoop() {
	defer s.wg.Done()

	backoff := 50 * time.Millisecond
	maxBackoff := 2 * time.Second
	firstFailure := true

	for {
		select {
		case <-s.cancelCtx.Done():
			return
		default:
		}

		s.mu.RLock()
		degErr := s.degradedErr
		s.mu.RUnlock()
		if degErr != nil {
			return
		}

		err := s.runStreamSession()
		if err != nil && !s.closed.Load() {
			if firstFailure || backoff >= maxBackoff {
				klog.Warningf("WAL client stream %s: replication session failed: %v, reconnecting in %v", s.streamID, err, backoff)
				firstFailure = false
			} else {
				klog.V(4).Infof("WAL client replication session ended: %v, reconnecting in %v", err, backoff)
			}

			s.mu.RLock()
			degErr := s.degradedErr
			s.mu.RUnlock()
			if degErr != nil {
				return
			}

			select {
			case <-s.cancelCtx.Done():
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

	if err := s.handleHelloAck(helloAck); err != nil {
		s.initialHelloAckOnce.Do(func() {
			s.initialHelloAckErr = err
			close(s.initialHelloAckDone)
		})
		return fmt.Errorf("handleHelloAck failed: %w", err)
	}

	if s.target != "" {
		klog.Infof("WAL client stream %s: replication session established with %s", s.streamID, s.target)
	} else {
		klog.Infof("WAL client stream %s: replication session established", s.streamID)
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
					klog.Infof("WAL client stream %s: first ack after reconnect: witness=%d, s3=%d",
						s.streamID, ack.WitnessAckedStreamSeq, ack.S3AckedStreamSeq)
				})
				s.handleAck(ack)
			}
		}
	}()

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
		klog.Infof("WAL client stream %s: replaying %d unacknowledged records (%d..%d)",
			s.streamID, len(recordsToReplay), recordsToReplay[0].StreamSeq, recordsToReplay[len(recordsToReplay)-1].StreamSeq)
	} else {
		klog.Infof("WAL client stream %s: no unacknowledged records to replay", s.streamID)
	}

	for _, rec := range recordsToReplay {
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

func (s *streamImpl) handleHelloAck(helloAck *pb.HelloAck) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	witnessHead := helloAck.WitnessAckedStreamSeq
	klog.Infof("WAL client stream %s: HelloAck received: witness watermark=%d, s3 watermark=%d (local head=%d)",
		s.streamID, witnessHead, helloAck.S3AckedStreamSeq, s.localSeq)
	if witnessHead > s.localSeq {
		klog.Infof("Stream %s: buffer witness watermark (%d) is ahead of local stream head (%d); continuing sequence numbering from buffer head",
			s.streamID, witnessHead, s.localSeq)
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
					if err := s.store.TruncateAfter(witnessHead); err != nil {
						truncErr := s.setDegradedLocked(fmt.Errorf("failed to truncate local segment store to %d: %w", witnessHead, err))
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

				klog.Warningf("Stream %s: local records above buffer witness watermark (%d) diverged from authoritative buffer history; discarded %d local unacknowledged records",
					s.streamID, witnessHead, discarded)
			}
		}
	}

	if witnessHead < s.witnessSeq {
		klog.Warningf("Witness watermark dropped on restart from %d to %d (replaying unacknowledged records)", s.witnessSeq, witnessHead)
	}
	s.witnessSeq = witnessHead

	if helloAck.S3AckedStreamSeq > s.s3Seq {
		s.s3Seq = helloAck.S3AckedStreamSeq
		s.cleanupS3AckedLocked(helloAck.S3AckedStreamSeq)
	}

	s.notifyWaitersLocked()

	if s.cond != nil {
		s.cond.Broadcast()
	}
	s.initialHelloAckOnce.Do(func() {
		close(s.initialHelloAckDone)
	})
	return nil
}

func (s *streamImpl) handleAck(ack *pb.Ack) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if ack.WitnessAckedStreamSeq > s.witnessSeq {
		s.witnessSeq = ack.WitnessAckedStreamSeq
	}
	if ack.S3AckedStreamSeq > s.s3Seq {
		s.s3Seq = ack.S3AckedStreamSeq
		s.cleanupS3AckedLocked(ack.S3AckedStreamSeq)
	}

	s.notifyWaitersLocked()

	if s.cond != nil {
		s.cond.Broadcast()
	}
}

func (s *streamImpl) cleanupS3AckedLocked(s3AckedSeq uint64) {
	// Clean up local segment files where all records are <= s3AckedSeq
	if err := s.store.DeleteSegmentsBeforeS3Ack(s3AckedSeq); err != nil {
		klog.Warningf("Failed to delete S3-acked segment files: %v", err)
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
	client := s.grpcClient
	if client == nil && s.target == "" {
		return func(yield func(uint64, []byte) bool) {}, nil
	}
	return TailStream(ctx, s.target, s.streamID, fromSeq, WithGRPCClient(client), WithDialOptions(s.opts.dialOpts...))
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
				klog.Errorf("TailStream failed to dial target %s: %v", target, err)
				return
			}
			localConn = conn
			defer errutil.CloseAndLog(localConn, "grpc connection")
			grpcClient = pb.NewWalBufferClient(conn)
		}

		tailStream, err := grpcClient.Tail(ctx, &pb.TailRequest{
			StreamId:      streamID[:],
			FromStreamSeq: fromSeq,
		})
		if err != nil {
			klog.V(4).Infof("TailStream RPC failed: %v", err)
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
