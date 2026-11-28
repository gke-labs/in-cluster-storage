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
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/wal/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/grpcutil"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore/inmemorystorage"
	"github.com/gke-labs/in-cluster-storage/pkg/wal/buffer/buffertest"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type appendFuncServer struct {
	pb.WalBufferServer
	appendFunc func(pb.WalBuffer_AppendServer) error
	tailFunc   func(*pb.TailRequest, pb.WalBuffer_TailServer) error
}

func (s *appendFuncServer) Append(stream pb.WalBuffer_AppendServer) error {
	return s.appendFunc(stream)
}

func (s *appendFuncServer) Tail(request *pb.TailRequest, stream pb.WalBuffer_TailServer) error {
	if s.tailFunc != nil {
		return s.tailFunc(request, stream)
	}
	return s.WalBufferServer.Tail(request, stream)
}

type helloRecordingStream struct {
	pb.WalBuffer_AppendServer
	recorder *recordingBuffer
	clientID string
}

func (s *helloRecordingStream) Recv() (*pb.AppendRequest, error) {
	request, err := s.WalBuffer_AppendServer.Recv()
	if err == nil {
		if hello := request.GetHello(); hello != nil {
			s.clientID = string(hello.GetClientId())
			s.recorder.mu.Lock()
			s.recorder.recordedIDs = append(s.recorder.recordedIDs, s.clientID)
			s.recorder.mu.Unlock()
		}
	}
	return request, err
}

func (s *helloRecordingStream) Send(response *pb.AppendResponse) error {
	if helloAck := response.GetHelloAck(); helloAck != nil {
		s.recorder.mu.Lock()
		s.recorder.recordedResumed[s.clientID] = append(s.recorder.recordedResumed[s.clientID], helloAck.GetResumed())
		s.recorder.mu.Unlock()
	}
	return s.WalBuffer_AppendServer.Send(response)
}

type recordingBuffer struct {
	clock        *buffertest.FakeClock
	dialOptions  []grpc.DialOption
	listener     *bufconn.Listener
	sessionEnded chan struct{}
	failTailOnce atomic.Bool

	mu              sync.Mutex
	recordedIDs     []string
	recordedResumed map[string][]bool
}

func (r *recordingBuffer) helloCount(clientID string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	count := 0
	for _, id := range r.recordedIDs {
		if id == clientID {
			count++
		}
	}
	return count
}

func (r *recordingBuffer) allIDs() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.recordedIDs...)
}

func (r *recordingBuffer) resumedFlags(clientID string) []bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]bool(nil), r.recordedResumed[clientID]...)
}

func serveWalBuffer(t *testing.T, base pb.WalBufferServer, appendFunc func(pb.WalBuffer_AppendServer) error, tailFunc func(*pb.TailRequest, pb.WalBuffer_TailServer) error) (*bufconn.Listener, []grpc.DialOption) {
	t.Helper()
	listener := bufconn.Listen(4 * 1024 * 1024)
	grpcServer := grpc.NewServer()
	pb.RegisterWalBufferServer(grpcServer, &appendFuncServer{WalBufferServer: base, appendFunc: appendFunc, tailFunc: tailFunc})
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
	})
	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	return listener, dialOptions
}

func serveAppendFunc(t *testing.T, appendFunc func(pb.WalBuffer_AppendServer) error) (*bufconn.Listener, []grpc.DialOption) {
	t.Helper()
	return serveWalBuffer(t, pb.UnimplementedWalBufferServer{}, appendFunc, nil)
}

func startRecordingBuffer(t *testing.T) *recordingBuffer {
	t.Helper()
	clock := buffertest.NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	bufferTest, _ := buffertest.Start(t.Context(), t, buffertest.WithWriterLockGrace(15*time.Second), buffertest.WithClock(clock))
	recorder := &recordingBuffer{
		clock:           clock,
		sessionEnded:    make(chan struct{}, 8),
		recordedResumed: make(map[string][]bool),
	}
	recorder.listener, recorder.dialOptions = serveWalBuffer(t, bufferTest.Server,
		func(stream pb.WalBuffer_AppendServer) error {
			err := bufferTest.Server.Append(&helloRecordingStream{WalBuffer_AppendServer: stream, recorder: recorder})
			select {
			case recorder.sessionEnded <- struct{}{}:
			default:
			}
			return err
		},
		func(request *pb.TailRequest, stream pb.WalBuffer_TailServer) error {
			if recorder.failTailOnce.CompareAndSwap(true, false) {
				return status.Error(codes.Unavailable, "transient tail failure")
			}
			return bufferTest.Server.Tail(request, stream)
		},
	)
	return recorder
}

func openTestStream(t *testing.T, streamID uuid.UUID, dialOptions []grpc.DialOption, extraOptions ...Option) Stream {
	t.Helper()
	options := append([]Option{WithDialOptions(dialOptions...), WithoutFsync()}, extraOptions...)
	stream, err := Open(t.Context(), t.TempDir(), streamID, "passthrough://bufnet", options...)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	t.Cleanup(func() {
		if err := stream.Close(); err != nil {
			t.Errorf("stream.Close failed: %v", err)
		}
	})
	return stream
}

func appendAndWaitWitness(t *testing.T, stream Stream, payload string) {
	t.Helper()
	sequence, err := stream.Append(t.Context(), []byte(payload))
	if err != nil {
		t.Fatalf("Append(%q) failed: %v", payload, err)
	}
	if err := stream.Wait(t.Context(), sequence, Witness, false); err != nil {
		t.Fatalf("Wait(Witness) for %q failed: %v", payload, err)
	}
}

func sendHelloAck(stream pb.WalBuffer_AppendServer) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	return stream.Send(&pb.AppendResponse{Msg: &pb.AppendResponse_HelloAck{HelloAck: &pb.HelloAck{}}})
}

func TestWriterLock_TwoClientsReleaseOnCloseAndRefusalFailsAtOnce(t *testing.T) {
	recorder := startRecordingBuffer(t)
	streamID := uuid.New()
	streamA := openTestStream(t, streamID, recorder.dialOptions, WithClientID("client-A"))
	appendAndWaitWitness(t, streamA, "from-client-A")

	// Refused first Hello: Open returns at once, satisfies grpcutil.ResendDelay,
	// and sends exactly one Hello from client B.
	start := time.Now()
	_, err := Open(t.Context(), t.TempDir(), streamID, "passthrough://bufnet",
		WithDialOptions(recorder.dialOptions...), WithClientID("client-B"), WithoutFsync())
	if elapsed := time.Since(start); err == nil || elapsed >= 500*time.Millisecond {
		t.Fatalf("expected immediate refusal from client B Open, err=%v elapsed=%v", err, elapsed)
	}
	if status.Code(err) != codes.Unavailable || !strings.Contains(err.Error(), "held by another client") {
		t.Fatalf("expected Unavailable 'held by another client', got code=%v err=%v", status.Code(err), err)
	}
	if delay, resend := grpcutil.ResendDelay(err); !resend || delay <= 0 {
		t.Fatalf("expected grpcutil.ResendDelay true with positive delay, got delay=%v resend=%v err=%v", delay, resend, err)
	}
	if got := recorder.helloCount("client-B"); got != 1 {
		t.Fatalf("expected 1 Hello from refused client B, got %d", got)
	}

	// Client A closes synchronously, releasing the lock without reconnecting;
	// client B succeeds immediately without advancing the clock.
	if err := streamA.Close(); err != nil {
		t.Fatalf("client A Close failed: %v", err)
	}
	if got := recorder.helloCount("client-A"); got != 1 {
		t.Fatalf("expected no reconnect Hello from closed client A (want 1), got %d", got)
	}

	streamB := openTestStream(t, streamID, recorder.dialOptions, WithClientID("client-B"))
	appendAndWaitWitness(t, streamB, "from-client-B")
}

func TestWriterLock_CutConnectionIsNotRelease(t *testing.T) {
	recorder := startRecordingBuffer(t)

	var mu sync.Mutex
	var connectionCut bool
	var connectionsA []net.Conn
	dialOptionsA := []grpc.DialOption{
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			mu.Lock()
			defer mu.Unlock()
			if connectionCut {
				return nil, errors.New("connection cut")
			}
			connection, err := recorder.listener.Dial()
			if err == nil {
				connectionsA = append(connectionsA, connection)
			}
			return connection, err
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	streamID := uuid.New()
	streamA := openTestStream(t, streamID, dialOptionsA, WithClientID("client-A"))
	appendAndWaitWitness(t, streamA, "before-cut")

	mu.Lock()
	connectionCut = true
	for _, connection := range connectionsA {
		_ = connection.Close()
	}
	mu.Unlock()
	<-recorder.sessionEnded

	_, err := Open(t.Context(), t.TempDir(), streamID, "passthrough://bufnet",
		WithDialOptions(recorder.dialOptions...), WithClientID("client-B"), WithoutFsync())
	if status.Code(err) != codes.Unavailable || !strings.Contains(err.Error(), "reserved for its previous holder") {
		t.Fatalf("expected Unavailable grace-period refusal, got code=%v err=%v", status.Code(err), err)
	}
	if _, resend := grpcutil.ResendDelay(err); !resend {
		t.Fatalf("expected grpcutil.ResendDelay true for grace-period refusal, got err=%v", err)
	}

	recorder.clock.Advance(15 * time.Second)
	openTestStream(t, streamID, recorder.dialOptions, WithClientID("client-B"))
}

func TestWriterLock_DefaultClientIDSharedInProcess(t *testing.T) {
	recorder := startRecordingBuffer(t)
	streamID := uuid.New()
	openTestStream(t, streamID, recorder.dialOptions)
	openTestStream(t, streamID, recorder.dialOptions)

	recorded := recorder.allIDs()
	if len(recorded) != 2 || recorded[0] == "" || recorded[0] != recorded[1] {
		t.Fatalf("expected two handles to send the same non-empty default client ID, got %v", recorded)
	}
}

func TestOpen_SilentServerRespectsDeadline(t *testing.T) {
	_, dialOptions := serveAppendFunc(t, func(stream pb.WalBuffer_AppendServer) error {
		<-stream.Context().Done()
		return stream.Context().Err()
	})

	deadlineCtx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := Open(deadlineCtx, t.TempDir(), uuid.New(), "passthrough://bufnet", WithDialOptions(dialOptions...), WithoutFsync())
	if elapsed := time.Since(start); err == nil || elapsed >= 600*time.Millisecond {
		t.Fatalf("expected Open with 300ms deadline against silent server to fail within 600ms, err=%v elapsed=%v", err, elapsed)
	}
}

func TestClose_ReleaseTimeoutBoundsWait(t *testing.T) {
	_, dialOptions := serveAppendFunc(t, func(stream pb.WalBuffer_AppendServer) error {
		if err := sendHelloAck(stream); err != nil {
			return err
		}
		<-stream.Context().Done()
		return stream.Context().Err()
	})

	const testReleaseTimeout = 150 * time.Millisecond
	stream, err := openStream(t.Context(), t.TempDir(), uuid.New(), "passthrough://bufnet", testReleaseTimeout, WithDialOptions(dialOptions...), WithoutFsync())
	if err != nil {
		t.Fatalf("openStream failed: %v", err)
	}

	start := time.Now()
	if err := stream.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if elapsed := time.Since(start); elapsed < testReleaseTimeout-30*time.Millisecond || elapsed >= time.Second {
		t.Fatalf("expected Close to wait ~%v then cancel, took %v", testReleaseTimeout, elapsed)
	}
}

func TestClose_StopsReconnectBackoff(t *testing.T) {
	var attempts atomic.Int32
	dropFirst := make(chan struct{})
	fourthFailed := make(chan struct{})
	_, dialOptions := serveAppendFunc(t, func(stream pb.WalBuffer_AppendServer) error {
		attempt := attempts.Add(1)
		if attempt == 1 {
			if err := sendHelloAck(stream); err != nil {
				return err
			}
			select {
			case <-dropFirst:
				return status.Error(codes.Unavailable, "transient drop")
			case <-stream.Context().Done():
				return stream.Context().Err()
			}
		}
		if attempt == 4 {
			close(fourthFailed)
		}
		return status.Error(codes.Unavailable, "still down")
	})

	stream, err := Open(t.Context(), t.TempDir(), uuid.New(), "passthrough://bufnet", WithDialOptions(dialOptions...), WithoutFsync())
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	close(dropFirst)
	<-fourthFailed
	time.Sleep(50 * time.Millisecond)

	start := time.Now()
	if err := stream.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if elapsed := time.Since(start); elapsed >= 150*time.Millisecond {
		t.Fatalf("expected Close to interrupt reconnect backoff promptly, took %v", elapsed)
	}
	if got := attempts.Load(); got != 4 {
		t.Fatalf("expected no reconnect attempt after Close (want 4), got %d", got)
	}
}

type helloAckCountingStream struct {
	pb.WalBuffer_AppendServer
	nonResumedCount *atomic.Int32
}

func (s *helloAckCountingStream) Send(response *pb.AppendResponse) error {
	if helloAck := response.GetHelloAck(); helloAck != nil && !helloAck.GetResumed() {
		s.nonResumedCount.Add(1)
	}
	return s.WalBuffer_AppendServer.Send(response)
}

func waitForPendingWaiter(t *testing.T, implementation *streamImpl) {
	t.Helper()
	for start := time.Now(); ; {
		implementation.mu.RLock()
		waiting := len(implementation.waiters) > 0
		implementation.mu.RUnlock()
		if waiting {
			return
		}
		if time.Since(start) > 2*time.Second {
			t.Fatal("timed out waiting for pending Wait to register")
		}
		time.Sleep(time.Millisecond)
	}
}

func testWriterLockLost(t *testing.T, recordsByClientB int, failFirstTail bool) {
	t.Helper()
	recorder := startRecordingBuffer(t)
	if failFirstTail {
		recorder.failTailOnce.Store(true)
	}

	var mu sync.Mutex
	var connectionCut bool
	var connectionsA []net.Conn
	dialOptionsA := []grpc.DialOption{
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			mu.Lock()
			defer mu.Unlock()
			if connectionCut {
				return nil, errors.New("connection cut")
			}
			connection, err := recorder.listener.Dial()
			if err == nil {
				connectionsA = append(connectionsA, connection)
			}
			return connection, err
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	streamID := uuid.New()
	streamA := openTestStream(t, streamID, dialOptionsA, WithClientID("client-A"))
	implementationA := streamA.(*streamImpl)

	// Cut client A's connection synchronously before advancing the clock, then let A
	// append record 1 locally so A has local head 1 and a pending Wait.
	mu.Lock()
	connectionCut = true
	for _, connection := range connectionsA {
		_ = connection.Close()
	}
	mu.Unlock()
	<-recorder.sessionEnded

	sequenceA1, err := streamA.Append(t.Context(), []byte("from-client-A-1"))
	if err != nil {
		t.Fatalf("client A Append failed: %v", err)
	}
	waitErrors := make(chan error, 1)
	go func() {
		waitErrors <- streamA.Wait(t.Context(), sequenceA1, Witness, false)
	}()
	waitForPendingWaiter(t, implementationA)

	// After the 15s grace period, client B is granted the lock, writes recordsByClientB
	// acknowledged records, and releases the lock cleanly on Close.
	recorder.clock.Advance(15 * time.Second)
	streamB := openTestStream(t, streamID, recorder.dialOptions, WithClientID("client-B"))
	for i := 1; i <= recordsByClientB; i++ {
		appendAndWaitWitness(t, streamB, fmt.Sprintf("from-client-B-%d", i))
	}
	if err := streamB.Close(); err != nil {
		t.Fatalf("client B Close failed: %v", err)
	}

	// Let client A reconnect: it detects writer lock loss, returns the grant, and stops.
	mu.Lock()
	connectionCut = false
	mu.Unlock()

	select {
	case err := <-waitErrors:
		if !errors.Is(err, ErrWriterLockLost) {
			t.Fatalf("expected pending Wait to fail with ErrWriterLockLost, got: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for client A pending Wait to fail")
	}

	if degraded := streamA.Degraded(); !errors.Is(degraded, ErrWriterLockLost) {
		t.Fatalf("expected Degraded() to report ErrWriterLockLost, got: %v", degraded)
	}
	if _, err := streamA.Append(t.Context(), []byte("after-loss")); !errors.Is(err, ErrWriterLockLost) {
		t.Fatalf("expected Append after loss to fail with ErrWriterLockLost, got: %v", err)
	}
	if err := streamA.Flush(t.Context()); !errors.Is(err, ErrWriterLockLost) {
		t.Fatalf("expected Flush after loss to fail with ErrWriterLockLost, got: %v", err)
	}

	select {
	case <-implementationA.replicationDone:
	case <-time.After(2 * time.Second):
		t.Fatal("expected client A replication loop to return after writer lock loss")
	}

	wantHellos := 2
	if failFirstTail {
		wantHellos = 3
		if recorder.failTailOnce.Load() {
			t.Fatal("expected injected first Tail failure to be triggered")
		}
		resumed := recorder.resumedFlags("client-A")
		if len(resumed) != 3 || resumed[0] || resumed[1] || !resumed[2] {
			t.Fatalf("expected client A HelloAck resumed flags [false false true], got %v", resumed)
		}
	}
	if got := recorder.helloCount("client-A"); got != wantHellos {
		t.Fatalf("expected client A to stop after detecting loss (want %d Hellos), got %d", wantHellos, got)
	}

	// Client A half-closed its reconnect session, so client C is granted without advancing the clock.
	streamC := openTestStream(t, streamID, recorder.dialOptions, WithClientID("client-C"))
	appendAndWaitWitness(t, streamC, "from-client-C")
}

func TestWriterLock_LostEqualHeads(t *testing.T) {
	testWriterLockLost(t, 1, false)
}

func TestWriterLock_LostServerHeadBeyond(t *testing.T) {
	testWriterLockLost(t, 2, false)
}

func TestWriterLock_LostAfterTailErrorAndResumedReconnect(t *testing.T) {
	testWriterLockLost(t, 1, true)
}

func TestWriterLock_NotLostOnServerRestart(t *testing.T) {
	backend := inmemorystorage.New()
	dataDirectory := t.TempDir()
	clock := buffertest.NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))

	var mu sync.Mutex
	var activeListener *bufconn.Listener
	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			mu.Lock()
			listener := activeListener
			mu.Unlock()
			if listener == nil {
				return nil, errors.New("server stopped")
			}
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	var nonResumedHellos atomic.Int32
	startServer := func() func() {
		bufferTest, stopBuffer := buffertest.Start(t.Context(), t,
			buffertest.WithBackend(backend),
			buffertest.WithDataDir(dataDirectory),
			buffertest.WithWriterLockGrace(15*time.Second),
			buffertest.WithClock(clock),
		)
		listener := bufconn.Listen(4 * 1024 * 1024)
		grpcServer := grpc.NewServer()
		pb.RegisterWalBufferServer(grpcServer, &appendFuncServer{
			WalBufferServer: bufferTest.Server,
			appendFunc: func(stream pb.WalBuffer_AppendServer) error {
				return bufferTest.Server.Append(&helloAckCountingStream{
					WalBuffer_AppendServer: stream,
					nonResumedCount:        &nonResumedHellos,
				})
			},
		})
		go func() { _ = grpcServer.Serve(listener) }()
		mu.Lock()
		activeListener = listener
		mu.Unlock()
		var stopOnce sync.Once
		stop := func() {
			stopOnce.Do(func() {
				mu.Lock()
				if activeListener == listener {
					activeListener = nil
				}
				mu.Unlock()
				grpcServer.Stop()
				_ = listener.Close()
				stopBuffer()
			})
		}
		t.Cleanup(stop)
		return stop
	}

	stop1 := startServer()
	streamID := uuid.New()
	streamA := openTestStream(t, streamID, dialOptions, WithClientID("client-A"))
	implementationA := streamA.(*streamImpl)
	appendAndWaitWitness(t, streamA, "from-client-A-1")

	// Restart the stream server on the same data directory while client A is connected.
	stop1()
	sequence2, err := streamA.Append(t.Context(), []byte("from-client-A-2"))
	if err != nil {
		t.Fatalf("client A Append while server stopped failed: %v", err)
	}
	waitErrors := make(chan error, 1)
	go func() {
		waitErrors <- streamA.Wait(t.Context(), sequence2, Witness, false)
	}()
	waitForPendingWaiter(t, implementationA)

	stop2 := startServer()
	select {
	case err := <-waitErrors:
		if err != nil {
			t.Fatalf("expected pending Wait to complete after server restart, got: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for client A pending Wait after server restart")
	}
	if degraded := streamA.Degraded(); degraded != nil {
		t.Fatalf("expected stream A not degraded after server restart, got: %v", degraded)
	}

	// Also verify a restart where the server forgets the lock holder (Resumed=false)
	// and nobody else wrote: equal heads with matching record at head is not a lock loss.
	stop2()
	if err := os.RemoveAll(filepath.Join(dataDirectory, "writer-locks")); err != nil {
		t.Fatalf("RemoveAll writer-locks failed: %v", err)
	}
	startServer()

	for start := time.Now(); nonResumedHellos.Load() < 2; {
		if time.Since(start) > 5*time.Second {
			t.Fatal("timed out waiting for non-resumed HelloAck after restart")
		}
		time.Sleep(5 * time.Millisecond)
	}
	appendAndWaitWitness(t, streamA, "from-client-A-3")
	if degraded := streamA.Degraded(); degraded != nil {
		t.Fatalf("expected stream A not degraded after non-resumed restart, got: %v", degraded)
	}
}
