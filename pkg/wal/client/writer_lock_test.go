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
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/wal/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/grpcutil"
	"github.com/gke-labs/in-cluster-storage/pkg/wal/buffer/buffertest"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type appendFuncServer struct {
	pb.UnimplementedWalBufferServer
	appendFunc func(pb.WalBuffer_AppendServer) error
}

func (s *appendFuncServer) Append(stream pb.WalBuffer_AppendServer) error {
	return s.appendFunc(stream)
}

type helloRecordingStream struct {
	pb.WalBuffer_AppendServer
	recorder *recordingBuffer
}

func (s *helloRecordingStream) Recv() (*pb.AppendRequest, error) {
	request, err := s.WalBuffer_AppendServer.Recv()
	if err == nil {
		if hello := request.GetHello(); hello != nil {
			s.recorder.mu.Lock()
			s.recorder.recordedIDs = append(s.recorder.recordedIDs, string(hello.GetClientId()))
			s.recorder.mu.Unlock()
		}
	}
	return request, err
}

type recordingBuffer struct {
	clock        *buffertest.FakeClock
	dialOptions  []grpc.DialOption
	listener     *bufconn.Listener
	sessionEnded chan struct{}

	mu          sync.Mutex
	recordedIDs []string
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

func serveAppendFunc(t *testing.T, appendFunc func(pb.WalBuffer_AppendServer) error) (*bufconn.Listener, []grpc.DialOption) {
	t.Helper()
	listener := bufconn.Listen(4 * 1024 * 1024)
	grpcServer := grpc.NewServer()
	pb.RegisterWalBufferServer(grpcServer, &appendFuncServer{appendFunc: appendFunc})
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

func startRecordingBuffer(t *testing.T) *recordingBuffer {
	t.Helper()
	clock := buffertest.NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	bufferTest, _ := buffertest.Start(t.Context(), t, buffertest.WithWriterLockGrace(15*time.Second), buffertest.WithClock(clock))
	recorder := &recordingBuffer{
		clock:        clock,
		sessionEnded: make(chan struct{}, 8),
	}
	recorder.listener, recorder.dialOptions = serveAppendFunc(t, func(stream pb.WalBuffer_AppendServer) error {
		err := bufferTest.Server.Append(&helloRecordingStream{WalBuffer_AppendServer: stream, recorder: recorder})
		select {
		case recorder.sessionEnded <- struct{}{}:
		default:
		}
		return err
	})
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
