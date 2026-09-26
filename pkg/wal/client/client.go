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
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/wal/v1alpha1"
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
	// HasTarget returns whether the stream has a remote replication target configured.
	HasTarget() bool
	Close() error
}

type options struct {
	maxRetainedBytes int64
	maxSegmentSize   int64
	dialOpts         []grpc.DialOption
	grpcClient       pb.WalBufferClient
}

// Option configures Stream options.
type Option func(*options)

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

	// Retained in-memory records queue for network sending & replay
	retainedMu      sync.RWMutex
	retainedRecords []*wal.ClientRecord

	newRecordSignal chan struct{}
	cancelCtx       context.Context
	cancelFunc      context.CancelFunc
	closed          atomic.Bool
	wg              sync.WaitGroup

	hasTarget  bool
	grpcClient pb.WalBufferClient
	grpcConn   *grpc.ClientConn
}

// Open opens or recovers a WAL stream on the local node.
func Open(ctx context.Context, dir string, streamID uuid.UUID, target string, opts ...Option) (Stream, error) {
	var opt options
	opt.maxRetainedBytes = wal.DefaultMaxRetainedBytes
	opt.maxSegmentSize = wal.DefaultMaxSegmentSize
	for _, o := range opts {
		o(&opt)
	}

	store, recovered, err := wal.NewClientSegmentStore(dir, fmt.Sprintf("stream-%s", streamID.String()), opt.maxSegmentSize)
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

	s := &streamImpl{
		dir:             dir,
		streamID:        streamID,
		target:          target,
		opts:            opt,
		store:           store,
		localSeq:        highestLocalSeq,
		witnessSeq:      0,
		s3Seq:           0,
		retainedBytes:   retainedBytes,
		retainedRecords: recovered,
		newRecordSignal: make(chan struct{}, 1),
		cancelCtx:       cancelCtx,
		cancelFunc:      cancelFunc,
		hasTarget:       opt.grpcClient != nil || target != "",
	}
	s.cond = sync.NewCond(&s.mu)

	if opt.grpcClient != nil {
		s.grpcClient = opt.grpcClient
	} else if target != "" {
		dialOpts := append([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, opt.dialOpts...)
		conn, err := grpc.NewClient(target, dialOpts...)
		if err != nil {
			cancelFunc()
			_ = store.Close()
			return nil, fmt.Errorf("failed to dial target %s: %w", target, err)
		}
		s.grpcConn = conn
		s.grpcClient = pb.NewWalBufferClient(conn)
	}

	// Start background replication loop
	if s.grpcClient != nil {
		s.wg.Add(1)
		go s.backgroundReplicationLoop()
	}

	return s, nil
}

// Append writes and fsyncs locally, applying backpressure if retained bytes exceed maxRetainedBytes.
func (s *streamImpl) Append(ctx context.Context, payload []byte) (uint64, error) {
	if s.closed.Load() {
		return 0, errors.New("stream closed")
	}

	recordSize := int64(wal.ClientHeaderSize + len(payload))

	// Degraded mode / backpressure check: block if retained bytes exceed maxRetainedBytes
	s.mu.Lock()
	for s.retainedBytes+recordSize > s.opts.maxRetainedBytes && !s.closed.Load() {
		doneCh := make(chan struct{})
		go func() {
			s.mu.Lock()
			for s.retainedBytes+recordSize > s.opts.maxRetainedBytes && !s.closed.Load() {
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
	if !s.hasTarget && (level == Witness || level == Permanent) {
		return fmt.Errorf("durability level %s requested but %w", level, ErrDurabilityUnavailable)
	}

	s.mu.RLock()
	witness := s.witnessSeq
	s3 := s.s3Seq
	s.mu.RUnlock()

	if level == Witness && witness >= seq {
		return nil
	}
	if level == Permanent && s3 >= seq {
		return nil
	}

	for {
		waitChan := make(chan struct{})
		go func() {
			s.mu.Lock()
			for !s.closed.Load() {
				if level == Witness && s.witnessSeq >= seq {
					break
				}
				if level == Permanent && s.s3Seq >= seq {
					break
				}
				s.cond.Wait()
			}
			s.mu.Unlock()
			close(waitChan)
		}()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.cancelCtx.Done():
			return errors.New("stream closed")
		case <-waitChan:
			s.mu.RLock()
			w, s3w := s.witnessSeq, s.s3Seq
			s.mu.RUnlock()
			if level == Witness && w >= seq {
				return nil
			}
			if level == Permanent && s3w >= seq {
				return nil
			}
			if s.closed.Load() {
				return errors.New("stream closed")
			}
		}
	}
}

// Wait blocks until seq has reached level. If requestFlush is true and level is Permanent,
// a Flush RPC is issued (coalesced server-side) before waiting instead of relying on the periodic flush.
func (s *streamImpl) Wait(ctx context.Context, seq uint64, level Level, requestFlush bool) error {
	if s.closed.Load() {
		return errors.New("stream closed")
	}

	s.mu.RLock()
	local := s.localSeq
	s.mu.RUnlock()

	if seq > local {
		return fmt.Errorf("seq %d has not been appended locally (local=%d)", seq, local)
	}

	if level != Local && level != Witness && level != Permanent {
		return fmt.Errorf("invalid durability level: %v", level)
	}

	if level == Local {
		return nil
	}

	if !s.hasTarget {
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
	targetSeq := s.localSeq
	s3 := s.s3Seq
	s.mu.RUnlock()

	if targetSeq == 0 || s3 >= targetSeq {
		return nil
	}

	if !s.hasTarget {
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

// HasTarget returns whether the stream has a remote replication target configured.
func (s *streamImpl) HasTarget() bool {
	return s.hasTarget
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

// Close closes the stream, stopping background tasks and closing local store.
func (s *streamImpl) Close() error {
	if s.closed.Swap(true) {
		return nil
	}

	s.cancelFunc()
	s.mu.Lock()
	s.cond.Broadcast()
	s.mu.Unlock()

	s.wg.Wait()

	if s.grpcConn != nil {
		_ = s.grpcConn.Close()
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

	for {
		select {
		case <-s.cancelCtx.Done():
			return
		default:
		}

		err := s.runStreamSession()
		if err != nil && !s.closed.Load() {
			klog.V(4).Infof("WAL client replication session ended: %v, reconnecting in %v", err, backoff)
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
		}
	}
}

func (s *streamImpl) runStreamSession() error {
	ctx, cancel := context.WithCancel(s.cancelCtx)
	defer cancel()

	stream, err := s.grpcClient.Append(ctx)
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

	s.handleHelloAck(helloAck)

	// 3. Replay unacknowledged records (stream_seq > witnessAckedStreamSeq)
	s.retainedMu.RLock()
	var recordsToReplay []*wal.ClientRecord
	for _, r := range s.retainedRecords {
		if r.StreamSeq > helloAck.WitnessAckedStreamSeq {
			recordsToReplay = append(recordsToReplay, r)
		}
	}
	s.retainedMu.RUnlock()

	for _, rec := range recordsToReplay {
		if err := stream.Send(&pb.AppendRequest{
			Msg: &pb.AppendRequest_Record{
				Record: rec.ToAppendProto(),
			},
		}); err != nil {
			return fmt.Errorf("failed to replay record %d: %w", rec.StreamSeq, err)
		}
	}

	// 4. Concurrently receive Acks and send newly appended records
	recvErrCh := make(chan error, 1)
	go func() {
		for {
			ackResp, err := stream.Recv()
			if err != nil {
				recvErrCh <- err
				return
			}
			if ack := ackResp.GetAck(); ack != nil {
				s.handleAck(ack)
			}
		}
	}()

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

func (s *streamImpl) handleHelloAck(helloAck *pb.HelloAck) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if helloAck.WitnessAckedStreamSeq < s.witnessSeq {
		klog.Warningf("Witness watermark dropped on restart from %d to %d (replaying unacknowledged records)", s.witnessSeq, helloAck.WitnessAckedStreamSeq)
	}
	s.witnessSeq = helloAck.WitnessAckedStreamSeq

	if helloAck.S3AckedStreamSeq > s.s3Seq {
		s.s3Seq = helloAck.S3AckedStreamSeq
		s.cleanupS3AckedLocked(helloAck.S3AckedStreamSeq)
	}

	s.cond.Broadcast()
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

	s.cond.Broadcast()
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
