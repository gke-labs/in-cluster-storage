// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package buffer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/wal/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/errutil"
	"github.com/gke-labs/in-cluster-storage/pkg/grpcutil"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore/inmemorystorage"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock(start time.Time) *fakeClock {
	return &fakeClock{now: start}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func newTestServer(t *testing.T) *Server {
	t.Helper()
	return newTestServerWithGrace(t, 0, nil)
}

func newTestServerWithGrace(t *testing.T, grace time.Duration, clock Clock) *Server {
	t.Helper()
	server, err := NewServer(t.Context(), ServerConfig{
		Backend:         inmemorystorage.New(),
		DataDir:         t.TempDir(),
		FlushInterval:   10 * time.Second,
		BatchMaxDelay:   time.Millisecond,
		WriterLockGrace: grace,
		Clock:           clock,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Errorf("server.Close failed: %v", err)
		}
	})
	return server
}

func TestDecideGrant(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	grace := 15 * time.Second

	for _, testCase := range []struct {
		name         string
		inputs       grantDecisionInputs
		requester    string
		now          time.Time
		wantDecision grantDecision
		wantResume   bool
		wantNextWake time.Time
	}{
		{"empty requester", grantDecisionInputs{holder: "client-1", attachedSessions: 1, lastSessionEnd: t0}, "", t0, grantDecisionLockGranted, false, time.Time{}},
		{"empty holder", grantDecisionInputs{holder: "", lastSessionEnd: t0}, "client-1", t0, grantDecisionLockGranted, false, time.Time{}},
		{"same client reacquires with session", grantDecisionInputs{holder: "client-1", attachedSessions: 1, lastSessionEnd: t0}, "client-1", t0, grantDecisionLockGranted, true, time.Time{}},
		{"same client reacquires after detach", grantDecisionInputs{holder: "client-1", inFlight: 1, lastSessionEnd: t0}, "client-1", t0, grantDecisionLockGranted, true, time.Time{}},
		{"another client refused while attached", grantDecisionInputs{holder: "client-1", attachedSessions: 1, lastSessionEnd: t0}, "client-2", t0, grantDecisionRefusedLockAlreadyHeld, false, time.Time{}},
		{"another client refused on in flight", grantDecisionInputs{holder: "client-1", inFlight: 1, lastSessionEnd: t0}, "client-2", t0, grantDecisionRefusedRecordsInFlight, false, time.Time{}},
		{"another client granted when free", grantDecisionInputs{holder: "client-1", lastSessionEnd: t0}, "client-2", t0, grantDecisionLockGranted, false, time.Time{}},
		{"another client refused inside grace period", grantDecisionInputs{holder: "client-1", lastSessionEnd: t0, gracePeriod: grace}, "client-2", t0.Add(5 * time.Second), grantDecisionRefusedGracePeriod, false, t0.Add(grace)},
		{"another client granted at grace expiry", grantDecisionInputs{holder: "client-1", lastSessionEnd: t0, gracePeriod: grace}, "client-2", t0.Add(grace), grantDecisionLockGranted, false, time.Time{}},
		{"another client granted after grace period", grantDecisionInputs{holder: "client-1", lastSessionEnd: t0, gracePeriod: grace}, "client-2", t0.Add(20 * time.Second), grantDecisionLockGranted, false, time.Time{}},
		{"same client granted inside grace period", grantDecisionInputs{holder: "client-1", lastSessionEnd: t0, gracePeriod: grace}, "client-1", t0.Add(5 * time.Second), grantDecisionLockGranted, true, time.Time{}},
		{"same client granted after grace period", grantDecisionInputs{holder: "client-1", lastSessionEnd: t0, gracePeriod: grace}, "client-1", t0.Add(20 * time.Second), grantDecisionLockGranted, true, time.Time{}},
		{"another client refused on in flight after grace period", grantDecisionInputs{holder: "client-1", inFlight: 1, lastSessionEnd: t0, gracePeriod: grace}, "client-2", t0.Add(20 * time.Second), grantDecisionRefusedRecordsInFlight, false, time.Time{}},
		{"another client refused on in flight inside grace period", grantDecisionInputs{holder: "client-1", inFlight: 1, lastSessionEnd: t0, gracePeriod: grace}, "client-2", t0.Add(5 * time.Second), grantDecisionRefusedRecordsInFlight, false, time.Time{}},
		{"zero lastSessionEnd with grace period is granted", grantDecisionInputs{holder: "client-1", gracePeriod: grace}, "client-2", t0, grantDecisionLockGranted, false, time.Time{}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			gotDecision, gotResume, gotNextWake := decideGrant(testCase.inputs, testCase.requester, testCase.now)
			if gotDecision != testCase.wantDecision || gotResume != testCase.wantResume || !gotNextWake.Equal(testCase.wantNextWake) {
				t.Fatalf("decideGrant(%+v, %q, %v) = (%v, %v, %v), want (%v, %v, %v)",
					testCase.inputs, testCase.requester, testCase.now,
					gotDecision, gotResume, gotNextWake,
					testCase.wantDecision, testCase.wantResume, testCase.wantNextWake)
			}
		})
	}
}

func TestWriterLock_GrantAndResume(t *testing.T) {
	server := newTestServer(t)
	ctx := t.Context()
	streamID := uuid.New()

	state, _, _, resumed, err := server.attachSession(ctx, streamID, 1, "client-1")
	if err != nil || resumed {
		t.Fatalf("attach client-1: err=%v, resumed=%v", err, resumed)
	}
	if _, _, _, resumed, err = server.attachSession(ctx, streamID, 1, "client-1"); err != nil || !resumed {
		t.Fatalf("second attach client-1: err=%v, resumed=%v", err, resumed)
	}

	// Another client while client-1 is attached is refused with Unavailable.
	if _, _, _, _, err = server.attachSession(ctx, streamID, 1, "client-2"); status.Code(err) != codes.Unavailable {
		t.Fatalf("expected Unavailable while attached, got %v", err)
	}

	server.detachSession(ctx, streamID, state, "client-1", false)

	// Another client while client-1 still has one attached session is refused with Unavailable.
	if _, _, _, _, err = server.attachSession(ctx, streamID, 1, "client-2"); status.Code(err) != codes.Unavailable {
		t.Fatalf("expected Unavailable while first session attached, got %v", err)
	}

	server.detachSession(ctx, streamID, state, "client-1", false)

	// Both sessions of client-1 detached: client-2 is granted.
	client2State, _, _, resumed, err := server.attachSession(ctx, streamID, 1, "client-2")
	if err != nil || resumed {
		t.Fatalf("attach client-2: err=%v, resumed=%v", err, resumed)
	}
	server.detachSession(ctx, streamID, client2State, "client-2", false)
	if _, _, _, resumed, err = server.attachSession(ctx, streamID, 1, "client-2"); err != nil || !resumed {
		t.Fatalf("reacquire client-2: err=%v, resumed=%v", err, resumed)
	}
	server.detachSession(ctx, streamID, client2State, "client-2", false)
	if _, _, _, resumed, err = server.attachSession(ctx, streamID, 1, "client-1"); err != nil || resumed {
		t.Fatalf("acquire client-1: err=%v, resumed=%v", err, resumed)
	}
	server.detachSession(ctx, streamID, state, "client-1", false)
}

func TestWriterLock_RefusedWhileHolderAttached(t *testing.T) {
	server := newTestServer(t)
	ctx := t.Context()
	streamID := uuid.New()

	state, _, _, _, err := server.attachSession(ctx, streamID, 1, "client-1")
	if err != nil {
		t.Fatalf("attach client-1: %v", err)
	}
	defer server.detachSession(ctx, streamID, state, "client-1", false)

	_, _, _, _, err = server.attachSession(ctx, streamID, 1, "client-2")
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected codes.Unavailable, got %v", err)
	}
	if delay, resend := grpcutil.ResendDelay(err); !resend || delay != writerLockResendDelay {
		t.Fatalf("ResendDelay(%v) = (%v, %v), want (%v, true)", err, delay, resend, writerLockResendDelay)
	}
	if !strings.Contains(err.Error(), "stream writer lock is held by another client") {
		t.Fatalf("expected lock held message, got %v", err)
	}
	if strings.Contains(err.Error(), "client-1") || strings.Contains(err.Error(), "client-2") {
		t.Fatalf("message must not leak client IDs: %v", err)
	}
}

func TestWriterLock_InFlightRefusedUntilCommitted(t *testing.T) {
	server := newTestServer(t)
	ctx := t.Context()
	streamID := uuid.New()

	state, _, _, _, err := server.attachSession(ctx, streamID, 1, "client-1")
	if err != nil {
		t.Fatalf("attach client-1: %v", err)
	}
	state.mu.Lock()
	state.inFlight = 1
	state.mu.Unlock()
	server.detachSession(ctx, streamID, state, "client-1", false)

	// Another client while records are still in flight is refused with Unavailable.
	_, _, _, _, err = server.attachSession(ctx, streamID, 1, "client-2")
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected codes.Unavailable, got %v", err)
	}
	if delay, resend := grpcutil.ResendDelay(err); !resend || delay != writerLockResendDelay {
		t.Fatalf("ResendDelay(%v) = (%v, %v), want (%v, true)", err, delay, resend, writerLockResendDelay)
	}
	if !strings.Contains(err.Error(), "stream writer lock is waiting for records of its previous holder to be committed") {
		t.Fatalf("expected records in flight message, got %v", err)
	}
	if strings.Contains(err.Error(), "client-1") || strings.Contains(err.Error(), "client-2") {
		t.Fatalf("message must not leak client IDs: %v", err)
	}

	// Commit the in-flight item.
	server.commitItems(ctx, []incomingItem{{streamID: streamID, streamSeq: 42, epoch: 1, payload: []byte("rec")}})

	// Client-2 connects again: granted, and watermark equals stream's watermark (42).
	client2State, witnessSeq, _, resumed, err := server.attachSession(ctx, streamID, 1, "client-2")
	if err != nil || resumed {
		t.Fatalf("attach client-2 after commit: err=%v, resumed=%v", err, resumed)
	}
	if witnessSeq != 42 {
		t.Fatalf("expected witness watermark 42, got %d", witnessSeq)
	}
	server.detachSession(ctx, streamID, client2State, "client-2", false)
}

func TestWriterLock_EmptyClientID(t *testing.T) {
	server := newTestServer(t)
	ctx := t.Context()
	streamID := uuid.New()

	state1, _, _, resumed1, err := server.attachSession(ctx, streamID, 1, "")
	if err != nil || resumed1 {
		t.Fatalf("attach 1: err=%v, resumed=%v", err, resumed1)
	}
	defer server.detachSession(ctx, streamID, state1, "", false)
	state2, _, _, resumed2, err := server.attachSession(ctx, streamID, 1, "")
	if err != nil || resumed2 {
		t.Fatalf("attach 2: err=%v, resumed=%v", err, resumed2)
	}
	defer server.detachSession(ctx, streamID, state2, "", false)
	state1.mu.Lock()
	defer state1.mu.Unlock()
	if state1.holder != "" || state1.holderSessions != 0 || state1.attachments != 2 {
		t.Fatalf("expected empty holder and attachments=2, got holder=%q, sessions=%d, attachments=%d",
			state1.holder, state1.holderSessions, state1.attachments)
	}
}

func TestWriterLock_FencedLeavesNoHolderOrSessionCount(t *testing.T) {
	server := newTestServer(t)
	ctx := t.Context()
	streamID := uuid.New()

	state, _, _, _, err := server.attachSession(ctx, streamID, 5, "client-1")
	if err != nil {
		t.Fatalf("initial attach: %v", err)
	}
	server.detachSession(ctx, streamID, state, "client-1", false)

	// Attempt attach with a lower epoch. The lock is evaluated and granted first,
	// but the epoch check fails and returns fencedStatus.
	_, _, _, _, err = server.attachSession(ctx, streamID, 2, "client-fenced")
	if err == nil {
		t.Fatal("expected fenced error, got nil")
	}
	if delay, resend := grpcutil.ResendDelay(err); resend {
		t.Fatalf("expected fenced status not to satisfy ResendDelay, got delay %v", delay)
	}

	state.mu.Lock()
	defer state.mu.Unlock()
	if state.holder != "client-1" {
		t.Fatalf("expected holder to remain client-1, got %q", state.holder)
	}
	if state.holderSessions != 0 {
		t.Fatalf("expected holderSessions 0, got %d", state.holderSessions)
	}
	if state.attachments != 0 {
		t.Fatalf("expected attachments 0, got %d", state.attachments)
	}
	if state.waiters != 0 {
		t.Fatalf("expected waiters 0, got %d", state.waiters)
	}
}

func TestAppend_WriterLockOverWire(t *testing.T) {
	ctx := t.Context()
	server, address, cleanup := startTestServer(t, inmemorystorage.New(), t.TempDir())
	t.Cleanup(func() {
		cleanup()
	})

	streamID := uuid.New()
	clientID1 := []byte("client-wire-1")
	clientID2 := []byte("client-wire-2")

	openAppend := func(sessionCtx context.Context, clientID []byte) (*grpc.ClientConn, *pb.HelloAck) {
		connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatalf("grpc.NewClient: %v", err)
		}
		stream, err := pb.NewWalBufferClient(connection).Append(sessionCtx)
		if err != nil {
			errutil.CloseAndLog(connection, "client connection")
			t.Fatalf("Append: %v", err)
		}
		if err := stream.Send(&pb.AppendRequest{
			Msg: &pb.AppendRequest_Hello{
				Hello: &pb.Hello{
					StreamId: streamID[:],
					Epoch:    1,
					ClientId: clientID,
				},
			},
		}); err != nil {
			errutil.CloseAndLog(connection, "client connection")
			t.Fatalf("Send Hello: %v", err)
		}
		response, err := stream.Recv()
		if err != nil {
			errutil.CloseAndLog(connection, "client connection")
			t.Fatalf("Recv HelloAck: %v", err)
		}
		helloAck := response.GetHelloAck()
		if helloAck == nil {
			errutil.CloseAndLog(connection, "client connection")
			t.Fatalf("expected HelloAck, got %v", response)
		}
		return connection, helloAck
	}

	// 1. Client 1 attaches: HelloAck has resumed=false.
	session1Ctx, cancel1 := context.WithCancel(ctx)
	connection1, helloAck1 := openAppend(session1Ctx, clientID1)
	defer func() {
		if err := connection1.Close(); err != nil {
			t.Errorf("connection1.Close failed: %v", err)
		}
	}()
	if helloAck1.GetResumed() {
		t.Fatalf("expected resumed=false on first attach, got true")
	}

	// 2. Client 1 attaches second session with same client ID: HelloAck has resumed=true.
	session2Ctx, cancel2 := context.WithCancel(ctx)
	connection2, helloAck2 := openAppend(session2Ctx, clientID1)
	defer func() {
		if err := connection2.Close(); err != nil {
			t.Errorf("connection2.Close failed: %v", err)
		}
	}()
	if !helloAck2.GetResumed() {
		t.Fatalf("expected resumed=true on second attach with same client ID, got false")
	}

	// 3. Client 1 leaves (cancel both sessions).
	cancel1()
	cancel2()

	server.mu.RLock()
	state := server.streams[streamID.String()]
	server.mu.RUnlock()

	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		state.mu.Lock()
		sessions := state.holderSessions
		state.mu.Unlock()
		if sessions == 0 {
			break
		}
	}
	state.mu.Lock()
	sessions := state.holderSessions
	state.mu.Unlock()
	if sessions != 0 {
		t.Fatalf("timed out waiting for client 1 sessions to detach")
	}

	// 4. Different client attaches after client 1 leaves: HelloAck has resumed=false.
	session3Ctx, cancel3 := context.WithCancel(ctx)
	defer cancel3()
	connection3, helloAck3 := openAppend(session3Ctx, clientID2)
	defer func() {
		if err := connection3.Close(); err != nil {
			t.Errorf("connection3.Close failed: %v", err)
		}
	}()
	if helloAck3.GetResumed() {
		t.Fatalf("expected resumed=false for different client, got true")
	}
}

func setupGraceTest(t *testing.T) (*Server, *fakeClock, *streamState, uuid.UUID) {
	t.Helper()
	clock := newFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	server := newTestServerWithGrace(t, 15*time.Second, clock)
	streamID := uuid.New()
	state, _, _, _, err := server.attachSession(t.Context(), streamID, 1, "client-1")
	if err != nil {
		t.Fatalf("attach client-1: %v", err)
	}
	server.detachSession(t.Context(), streamID, state, "client-1", false)
	return server, clock, state, streamID
}

func TestWriterLock_GracePeriodRefusesOtherClient(t *testing.T) {
	server, clock, state, streamID := setupGraceTest(t)
	ctx := t.Context()

	// Advance clock within grace period.
	clock.Advance(5 * time.Second)

	// Another client inside the grace period is refused with Unavailable and leaves waiters at zero.
	_, _, _, _, err := server.attachSession(ctx, streamID, 1, "client-2")
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected codes.Unavailable, got %v", err)
	}
	if delay, resend := grpcutil.ResendDelay(err); !resend || delay != 10*time.Second {
		t.Fatalf("ResendDelay(%v) = (%v, %v), want (%v, true)", err, delay, resend, 10*time.Second)
	}
	if !strings.Contains(err.Error(), "stream writer lock is reserved for its previous holder") {
		t.Fatalf("expected reservation message, got %v", err)
	}
	if strings.Contains(err.Error(), "client-1") || strings.Contains(err.Error(), "client-2") {
		t.Fatalf("message must not leak client IDs: %v", err)
	}
	state.mu.Lock()
	waiters := state.waiters
	state.mu.Unlock()
	if waiters != 0 {
		t.Fatalf("expected waiters=0, got %d", waiters)
	}

	// The holder itself is still granted at once with resumed true.
	holderState, _, _, resumed, err := server.attachSession(ctx, streamID, 1, "client-1")
	if err != nil || !resumed {
		t.Fatalf("attach client-1 within grace: err=%v, resumed=%v", err, resumed)
	}
	server.detachSession(ctx, streamID, holderState, "client-1", false)

	// Advance clock past the grace period.
	clock.Advance(20 * time.Second)

	// After the clock passes the grace period the other client is granted with resumed false.
	client2State, _, _, resumed, err := server.attachSession(ctx, streamID, 1, "client-2")
	if err != nil || resumed {
		t.Fatalf("attach client-2 after grace period: err=%v, resumed=%v", err, resumed)
	}
	server.detachSession(ctx, streamID, client2State, "client-2", false)
}

func TestWriterLock_SameClientGrantedInsideGracePeriod(t *testing.T) {
	server, clock, state, streamID := setupGraceTest(t)

	// Advance clock within grace period.
	clock.Advance(5 * time.Second)

	// Same client reconnects: granted at once with resumed=true.
	_, _, _, resumed, err := server.attachSession(t.Context(), streamID, 1, "client-1")
	if err != nil || !resumed {
		t.Fatalf("attach client-1 within grace: err=%v, resumed=%v", err, resumed)
	}
	server.detachSession(t.Context(), streamID, state, "client-1", false)
}

func TestAppend_RefusedHelloOverWire(t *testing.T) {
	ctx := t.Context()
	_, address, cleanup := startTestServer(t, inmemorystorage.New(), t.TempDir())
	t.Cleanup(func() {
		cleanup()
	})

	streamID := uuid.New()
	clientID1 := []byte("client-wire-1")
	clientID2 := []byte("client-wire-2")

	// Client 1 connects and attaches.
	session1Ctx, cancel1 := context.WithCancel(ctx)
	defer cancel1()

	connection1, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer func() {
		if err := connection1.Close(); err != nil {
			t.Errorf("connection1.Close: %v", err)
		}
	}()

	stream1, err := pb.NewWalBufferClient(connection1).Append(session1Ctx)
	if err != nil {
		t.Fatalf("Append 1: %v", err)
	}
	if err := stream1.Send(&pb.AppendRequest{
		Msg: &pb.AppendRequest_Hello{
			Hello: &pb.Hello{
				StreamId: streamID[:],
				Epoch:    1,
				ClientId: clientID1,
			},
		},
	}); err != nil {
		t.Fatalf("Send Hello 1: %v", err)
	}
	response1, err := stream1.Recv()
	if err != nil {
		t.Fatalf("Recv HelloAck 1: %v", err)
	}
	if response1.GetHelloAck() == nil {
		t.Fatalf("expected HelloAck, got %v", response1)
	}

	// Client 2 connects to the same stream: Hello is refused and ends RPC with Unavailable.
	connection2, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer func() {
		if err := connection2.Close(); err != nil {
			t.Errorf("connection2.Close: %v", err)
		}
	}()

	stream2, err := pb.NewWalBufferClient(connection2).Append(ctx)
	if err != nil {
		t.Fatalf("Append 2: %v", err)
	}
	if err := stream2.Send(&pb.AppendRequest{
		Msg: &pb.AppendRequest_Hello{
			Hello: &pb.Hello{
				StreamId: streamID[:],
				Epoch:    1,
				ClientId: clientID2,
			},
		},
	}); err != nil {
		t.Fatalf("Send Hello 2: %v", err)
	}
	_, err = stream2.Recv()
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected codes.Unavailable, got %v", err)
	}
	if delay, resend := grpcutil.ResendDelay(err); !resend || delay != writerLockResendDelay {
		t.Fatalf("ResendDelay(%v) = (%v, %v), want (%v, true)", err, delay, resend, writerLockResendDelay)
	}
	if !strings.Contains(err.Error(), "stream writer lock is held by another client") {
		t.Fatalf("expected lock held message, got %v", err)
	}
	if strings.Contains(err.Error(), "client-wire-1") || strings.Contains(err.Error(), "client-wire-2") {
		t.Fatalf("message must not leak client IDs: %v", err)
	}
}

func TestAppend_CleanlyEndedSessionExitsForwarder(t *testing.T) {
	ctx := t.Context()
	server, address, cleanup := startTestServer(t, inmemorystorage.New(), t.TempDir())
	t.Cleanup(func() {
		cleanup()
	})

	streamID := uuid.New()
	clientID := []byte("client-clean-close")

	connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer func() {
		if err := connection.Close(); err != nil {
			t.Errorf("connection.Close: %v", err)
		}
	}()

	stream, err := pb.NewWalBufferClient(connection).Append(ctx)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	if err := stream.Send(&pb.AppendRequest{
		Msg: &pb.AppendRequest_Hello{
			Hello: &pb.Hello{
				StreamId: streamID[:],
				Epoch:    1,
				ClientId: clientID,
			},
		},
	}); err != nil {
		t.Fatalf("Send Hello: %v", err)
	}

	response, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv HelloAck: %v", err)
	}
	if response.GetHelloAck() == nil {
		t.Fatalf("expected HelloAck, got %v", response)
	}

	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}

	// Read until the stream closes (io.EOF).
	for {
		_, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Recv to end: %v", err)
		}
	}

	// Verify that waiters and attachments reach zero.
	server.mu.RLock()
	state := server.streams[streamID.String()]
	server.mu.RUnlock()

	if state == nil {
		t.Fatalf("stream state not found for %s", streamID)
	}

	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(time.Millisecond) {
		state.mu.Lock()
		waiters := state.waiters
		attachments := state.attachments
		state.mu.Unlock()

		if waiters == 0 && attachments == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for forwarder to exit: waiters=%d, attachments=%d", waiters, attachments)
		}
	}
}

func TestAppend_CleanEndReleasesWriterLock(t *testing.T) {
	ctx := t.Context()
	clock := newFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	server, address, cleanup := startTestServerWithConfig(t, ServerConfig{
		Backend:         inmemorystorage.New(),
		DataDir:         t.TempDir(),
		WriterLockGrace: 15 * time.Second,
		Clock:           clock,
	})
	t.Cleanup(cleanup)

	openAppendStream := func(sessionCtx context.Context) (*grpc.ClientConn, pb.WalBuffer_AppendClient) {
		conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatalf("grpc.NewClient: %v", err)
		}
		stream, err := pb.NewWalBufferClient(conn).Append(sessionCtx)
		if err != nil {
			errutil.CloseAndLog(conn, "client connection")
			t.Fatalf("Append: %v", err)
		}
		return conn, stream
	}

	sendHello := func(stream pb.WalBuffer_AppendClient, streamID uuid.UUID, clientID []byte) error {
		return stream.Send(&pb.AppendRequest{
			Msg: &pb.AppendRequest_Hello{
				Hello: &pb.Hello{
					StreamId: streamID[:],
					Epoch:    1,
					ClientId: clientID,
				},
			},
		})
	}

	recvHelloAck := func(stream pb.WalBuffer_AppendClient) (*pb.HelloAck, error) {
		resp, err := stream.Recv()
		if err != nil {
			return nil, err
		}
		ack := resp.GetHelloAck()
		if ack == nil {
			return nil, fmt.Errorf("expected HelloAck, got %v", resp)
		}
		return ack, nil
	}

	closeSendAndDrainEOF := func(stream pb.WalBuffer_AppendClient) {
		if err := stream.CloseSend(); err != nil {
			t.Fatalf("CloseSend: %v", err)
		}
		for {
			_, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatalf("Recv until EOF: %v", err)
			}
		}
	}

	// 1. Client 1 says Hello, half-closes, the handler returns; client 2's Hello is granted, not resumed.
	t.Run("clean end releases lock", func(t *testing.T) {
		streamID := uuid.New()
		clientID1 := []byte("client-1")
		clientID2 := []byte("client-2")

		conn1, stream1 := openAppendStream(ctx)
		defer func() {
			_ = conn1.Close()
		}()

		if err := sendHello(stream1, streamID, clientID1); err != nil {
			t.Fatalf("sendHello client-1: %v", err)
		}
		ack1, err := recvHelloAck(stream1)
		if err != nil {
			t.Fatalf("recvHelloAck client-1: %v", err)
		}
		if ack1.GetResumed() {
			t.Fatalf("expected resumed=false for first client-1 attach, got true")
		}

		closeSendAndDrainEOF(stream1)

		conn2, stream2 := openAppendStream(ctx)
		defer func() {
			_ = conn2.Close()
		}()

		if err := sendHello(stream2, streamID, clientID2); err != nil {
			t.Fatalf("sendHello client-2: %v", err)
		}
		ack2, err := recvHelloAck(stream2)
		if err != nil {
			t.Fatalf("expected client-2 Hello to be granted, got: %v", err)
		}
		if ack2.GetResumed() {
			t.Fatalf("expected client-2 Hello granted with resumed=false, got resumed=true")
		}
		closeSendAndDrainEOF(stream2)
	})

	// 2. Client 1 says Hello and its session is cancelled instead; client 2's Hello is refused with the grace-period reason.
	t.Run("cancelled session retains lock for grace period", func(t *testing.T) {
		streamID := uuid.New()
		clientID1 := []byte("client-1")
		clientID2 := []byte("client-2")

		session1Ctx, cancel1 := context.WithCancel(ctx)
		conn1, stream1 := openAppendStream(session1Ctx)
		defer func() {
			_ = conn1.Close()
		}()

		if err := sendHello(stream1, streamID, clientID1); err != nil {
			t.Fatalf("sendHello client-1: %v", err)
		}
		ack1, err := recvHelloAck(stream1)
		if err != nil {
			t.Fatalf("recvHelloAck client-1: %v", err)
		}
		if ack1.GetResumed() {
			t.Fatalf("expected resumed=false for first client-1 attach, got true")
		}

		cancel1()

		// Wait for client 1 session to finish detaching on server.
		server.mu.RLock()
		st := server.streams[streamID.String()]
		server.mu.RUnlock()
		if st == nil {
			t.Fatalf("stream state not found for %s", streamID)
		}
		for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
			st.mu.Lock()
			sessions := st.holderSessions
			st.mu.Unlock()
			if sessions == 0 {
				break
			}
		}
		st.mu.Lock()
		sessions := st.holderSessions
		st.mu.Unlock()
		if sessions != 0 {
			t.Fatalf("timed out waiting for client 1 session to detach after cancel")
		}

		conn2, stream2 := openAppendStream(ctx)
		defer func() {
			_ = conn2.Close()
		}()

		if err := sendHello(stream2, streamID, clientID2); err != nil {
			t.Fatalf("sendHello client-2: %v", err)
		}
		_, err = recvHelloAck(stream2)
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("expected codes.Unavailable inside grace period, got %v", err)
		}
		if !strings.Contains(err.Error(), "stream writer lock is reserved for its previous holder") {
			t.Fatalf("expected grace period reservation message, got %v", err)
		}
	})

	// 3. Client 1 has two sessions and half-closes one; client 2 is refused as held by another client.
	t.Run("two sessions and half-close one keeps lock held", func(t *testing.T) {
		streamID := uuid.New()
		clientID1 := []byte("client-1")
		clientID2 := []byte("client-2")

		session1Ctx, cancel1 := context.WithCancel(ctx)
		defer cancel1()
		conn1, stream1 := openAppendStream(session1Ctx)
		defer func() {
			_ = conn1.Close()
		}()

		if err := sendHello(stream1, streamID, clientID1); err != nil {
			t.Fatalf("sendHello client-1 session 1: %v", err)
		}
		ack1, err := recvHelloAck(stream1)
		if err != nil {
			t.Fatalf("recvHelloAck client-1 session 1: %v", err)
		}
		if ack1.GetResumed() {
			t.Fatalf("expected resumed=false for first session, got true")
		}

		session2Ctx, cancel2 := context.WithCancel(ctx)
		defer cancel2()
		conn2, stream2 := openAppendStream(session2Ctx)
		defer func() {
			_ = conn2.Close()
		}()

		if err := sendHello(stream2, streamID, clientID1); err != nil {
			t.Fatalf("sendHello client-1 session 2: %v", err)
		}
		ack2, err := recvHelloAck(stream2)
		if err != nil {
			t.Fatalf("recvHelloAck client-1 session 2: %v", err)
		}
		if !ack2.GetResumed() {
			t.Fatalf("expected resumed=true for second session with same client ID, got false")
		}

		closeSendAndDrainEOF(stream1)

		conn3, stream3 := openAppendStream(ctx)
		defer func() {
			_ = conn3.Close()
		}()

		if err := sendHello(stream3, streamID, clientID2); err != nil {
			t.Fatalf("sendHello client-2: %v", err)
		}
		_, err = recvHelloAck(stream3)
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("expected codes.Unavailable while another session is held, got %v", err)
		}
		if !strings.Contains(err.Error(), "stream writer lock is held by another client") {
			t.Fatalf("expected lock held message, got %v", err)
		}
	})
}

func TestWriterLock_IdleEvictionKeepsStreamInsideGracePeriod(t *testing.T) {
	server, clock, _, _ := setupGraceTest(t)

	// Advance within grace period: eviction must keep the stream.
	clock.Advance(5 * time.Second)
	server.evictIdleStreams()
	if count := server.StreamCount(); count != 1 {
		t.Fatalf("expected stream retained during grace period, got count %d", count)
	}

	// Advance past grace period: eviction removes the idle stream.
	clock.Advance(15 * time.Second)
	server.evictIdleStreams()
	if count := server.StreamCount(); count != 0 {
		t.Fatalf("expected stream evicted after grace period, got count %d", count)
	}
}

func startTestServerWithConfig(t *testing.T, config ServerConfig) (*Server, string, func()) {
	t.Helper()
	ctx := t.Context()
	if config.FlushInterval <= 0 {
		config.FlushInterval = 10 * time.Second
	}
	if config.BatchMaxDelay <= 0 {
		config.BatchMaxDelay = 2 * time.Millisecond
	}
	server, err := NewServer(ctx, config)
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterWalBufferServer(grpcServer, server)

	go func() {
		if serveErr := grpcServer.Serve(listener); serveErr != nil && !errors.Is(serveErr, net.ErrClosed) {
			t.Errorf("grpc serve error: %v", serveErr)
		}
	}()

	cleanup := func() {
		grpcServer.GracefulStop()
		if closeErr := listener.Close(); closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
			t.Errorf("listener.Close: %v", closeErr)
		}
		if err := server.Close(); err != nil {
			t.Errorf("server.Close: %v", err)
		}
	}
	return server, listener.Addr().String(), cleanup
}

func TestWriterLock_RestartRemembersHolder(t *testing.T) {
	ctx := t.Context()
	dataDir := t.TempDir()
	backend := inmemorystorage.New()
	streamID := uuid.New()
	clientID1 := []byte("client-wire-1")
	clientID2 := []byte("client-wire-2")

	openAppend := func(address string, sessionCtx context.Context, clientID []byte) (*grpc.ClientConn, *pb.HelloAck, error) {
		connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, nil, err
		}
		stream, err := pb.NewWalBufferClient(connection).Append(sessionCtx)
		if err != nil {
			errutil.CloseAndLog(connection, "client connection")
			return nil, nil, err
		}
		if err := stream.Send(&pb.AppendRequest{
			Msg: &pb.AppendRequest_Hello{
				Hello: &pb.Hello{
					StreamId: streamID[:],
					Epoch:    1,
					ClientId: clientID,
				},
			},
		}); err != nil {
			errutil.CloseAndLog(connection, "client connection")
			return nil, nil, err
		}
		response, err := stream.Recv()
		if err != nil {
			errutil.CloseAndLog(connection, "client connection")
			return nil, nil, err
		}
		helloAck := response.GetHelloAck()
		if helloAck == nil {
			errutil.CloseAndLog(connection, "client connection")
			return nil, nil, fmt.Errorf("expected HelloAck, got %v", response)
		}
		return connection, helloAck, nil
	}

	// 1. Start server 1 with a grace period of 15 seconds.
	_, address1, cleanup1 := startTestServerWithConfig(t, ServerConfig{
		Backend:         backend,
		DataDir:         dataDir,
		WriterLockGrace: 15 * time.Second,
	})

	// Client 1 connects: first attach has resumed=false.
	session1Ctx, cancel1 := context.WithCancel(ctx)
	connection1, helloAck1, err := openAppend(address1, session1Ctx, clientID1)
	if err != nil {
		t.Fatalf("openAppend client 1 on server 1: %v", err)
	}
	if helloAck1.GetResumed() {
		t.Fatalf("expected resumed=false on first attach, got true")
	}

	// Client 1 closes session and server 1 shuts down.
	cancel1()
	if err := connection1.Close(); err != nil {
		t.Errorf("connection1.Close failed: %v", err)
	}
	cleanup1()

	// 2. Start server 2 with the same data directory and grace period.
	_, address2, cleanup2 := startTestServerWithConfig(t, ServerConfig{
		Backend:         backend,
		DataDir:         dataDir,
		WriterLockGrace: 15 * time.Second,
	})
	defer cleanup2()

	// Client 2 connects: server 2 lazily loads holder file, sees client 1 was holder,
	// and refuses client 2 during the grace period.
	session2Ctx, cancel2 := context.WithCancel(ctx)
	defer cancel2()
	connection2, _, err := openAppend(address2, session2Ctx, clientID2)
	if connection2 != nil {
		t.Cleanup(func() {
			if err := connection2.Close(); err != nil {
				t.Errorf("connection2.Close: %v", err)
			}
		})
	}
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected Unavailable for different client during grace period, got %v", err)
	}
	if !strings.Contains(err.Error(), "stream writer lock is reserved for its previous holder") {
		t.Fatalf("expected reservation message, got %v", err)
	}

	// Client 1 connects: recognized as holder and resumes at once.
	session3Ctx, cancel3 := context.WithCancel(ctx)
	defer cancel3()
	connection3, helloAck3, err := openAppend(address2, session3Ctx, clientID1)
	if err != nil {
		t.Fatalf("openAppend client 1 on server 2: %v", err)
	}
	t.Cleanup(func() {
		if err := connection3.Close(); err != nil {
			t.Errorf("connection3.Close: %v", err)
		}
	})
	if !helloAck3.GetResumed() {
		t.Fatalf("expected resumed=true for same client on server 2, got false")
	}
}

// testHolderStore wraps writerLockStore to allow blocking or failing save and remove calls.
type testHolderStore struct {
	writerLockStore
	saveStarted chan struct{}
	unblockSave chan struct{}
	failSave    atomic.Bool
	saves       atomic.Int32
	failRemove  atomic.Bool
	removes     atomic.Int32
}

func (s *testHolderStore) save(streamID uuid.UUID, clientID string) error {
	s.saves.Add(1)
	if s.saveStarted != nil {
		select {
		case <-s.saveStarted:
		default:
			close(s.saveStarted)
		}
	}
	if s.unblockSave != nil {
		<-s.unblockSave
	}
	if s.failSave.Swap(false) {
		return errors.New("simulated save failure")
	}
	return s.writerLockStore.save(streamID, clientID)
}

func (s *testHolderStore) remove(streamID uuid.UUID) error {
	s.removes.Add(1)
	if s.failRemove.Swap(false) {
		return errors.New("simulated remove failure")
	}
	return s.writerLockStore.remove(streamID)
}

var (
	_ = (*testHolderStore).save
	_ = (*testHolderStore).remove
)

func TestWriterLock_BlockedSave(t *testing.T) {
	ctx := t.Context()
	server := newTestServer(t)

	blocker := &testHolderStore{
		writerLockStore: server.holderStore,
		saveStarted:     make(chan struct{}),
		unblockSave:     make(chan struct{}),
	}
	server.holderStore = blocker

	streamID := uuid.New()

	client1Err := make(chan error, 1)
	go func() {
		_, _, _, _, err := server.attachSession(ctx, streamID, 1, "client-1")
		client1Err <- err
	}()

	select {
	case <-blocker.saveStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for save to start")
	}

	// A second claimant's Hello does not return while the save is blocked.
	client2Err := make(chan error, 1)
	go func() {
		_, _, _, _, err := server.attachSession(ctx, streamID, 1, "client-2")
		client2Err <- err
	}()

	select {
	case err := <-client2Err:
		t.Fatalf("client 2 returned while save was blocked: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	// Server.Close returns promptly meanwhile.
	closeServer := newTestServer(t)
	closeBlocker := &testHolderStore{
		writerLockStore: closeServer.holderStore,
		saveStarted:     make(chan struct{}),
		unblockSave:     make(chan struct{}),
	}
	closeServer.holderStore = closeBlocker

	go func() {
		_, _, _, _, _ = closeServer.attachSession(ctx, uuid.New(), 1, "client-close")
	}()
	select {
	case <-closeBlocker.saveStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for close save to start")
	}

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- closeServer.Close()
	}()
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close failed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Server.Close blocked waiting for save in progress")
	}
	close(closeBlocker.unblockSave)

	// After the save is released the second claimant is refused as held by another client.
	close(blocker.unblockSave)
	if err1 := <-client1Err; err1 != nil {
		t.Fatalf("client 1 attach failed: %v", err1)
	}

	err2 := <-client2Err
	if status.Code(err2) != codes.Unavailable {
		t.Fatalf("expected Unavailable for second claimant, got %v", err2)
	}
	if !strings.Contains(err2.Error(), "stream writer lock is held by another client") {
		t.Fatalf("expected 'held by another client', got %v", err2)
	}
}

func TestWriterLock_FailedSave(t *testing.T) {
	ctx := t.Context()
	clock := newFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	server := newTestServerWithGrace(t, 15*time.Second, clock)

	failingStore := &testHolderStore{
		writerLockStore: server.holderStore,
	}
	failingStore.failSave.Store(true)
	server.holderStore = failingStore

	streamID := uuid.New()

	// 1. Initial attach fails during save: no grant, holder in memory unchanged.
	_, _, _, _, err := server.attachSession(ctx, streamID, 1, "client-1")
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected Unavailable on failed save, got %v", err)
	}

	server.mu.RLock()
	state := server.streams[streamID.String()]
	server.mu.RUnlock()
	if state == nil {
		t.Fatal("expected stream state to exist")
	}

	state.mu.Lock()
	holder := state.holder
	sessions := state.holderSessions
	state.mu.Unlock()
	if holder != "" || sessions != 0 {
		t.Fatalf("expected empty holder and 0 sessions after failed save, got holder=%q, sessions=%d", holder, sessions)
	}

	// 2. Run the eviction pass: the state must survive it because saveFailed is in memory.
	server.evictIdleStreams()
	if count := server.StreamCount(); count != 1 {
		t.Fatalf("expected stream state to survive eviction after failed save, got count %d", count)
	}

	// 3. The same client's next Hello writes the file again and is granted.
	state, _, _, resumed, err := server.attachSession(ctx, streamID, 1, "client-1")
	if err != nil {
		t.Fatalf("attach client-1 retry: %v", err)
	}
	if resumed {
		t.Fatal("expected resumed=false on first successful grant, got true")
	}

	if failingStore.saves.Load() != 2 {
		t.Fatalf("expected 2 save calls, got %d", failingStore.saves.Load())
	}

	state.mu.Lock()
	holder = state.holder
	state.mu.Unlock()
	if holder != "client-1" {
		t.Fatalf("expected holder=client-1, got %q", holder)
	}

	loaded, err := server.holderStore.load(streamID)
	if err != nil || loaded != "client-1" {
		t.Fatalf("load on disk: got (%q, %v), want (client-1, nil)", loaded, err)
	}

	// Repeat grant to the same client writes nothing.
	state2, _, _, resumed, err := server.attachSession(ctx, streamID, 1, "client-1")
	if err != nil || !resumed {
		t.Fatalf("repeat attach client-1: err=%v, resumed=%v", err, resumed)
	}

	if failingStore.saves.Load() != 2 {
		t.Fatalf("repeat grant must not save, got %d saves", failingStore.saves.Load())
	}

	// Detach both sessions of client A so holderSessions is 0.
	server.detachSession(ctx, streamID, state, "client-1", false)
	server.detachSession(ctx, streamID, state2, "client-1", false)

	// 4. Client A has detached. After the grace period, client B's save fails
	// (the wrapper fails it after writing nothing).
	clock.Advance(20 * time.Second)
	failingStore.failSave.Store(true)

	_, _, _, _, err = server.attachSession(ctx, streamID, 1, "client-2")
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected Unavailable on client-2 failed save, got %v", err)
	}
	if failingStore.saves.Load() != 3 {
		t.Fatalf("expected 3 save calls after client-2 failed save, got %d", failingStore.saves.Load())
	}

	// Client A says Hello again: granted, and save must have been called for A,
	// even though the file was last known to name A.
	state3, _, _, _, err := server.attachSession(ctx, streamID, 1, "client-1")
	if err != nil {
		t.Fatalf("attach client-1 after client-2 failed save: %v", err)
	}
	defer server.detachSession(ctx, streamID, state3, "client-1", false)

	if failingStore.saves.Load() != 4 {
		t.Fatalf("expected 4 save calls (save must be called for client-1 after failed save), got %d", failingStore.saves.Load())
	}
}

func TestWriterLock_FailedRemove(t *testing.T) {
	ctx := t.Context()
	clock := newFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	server := newTestServerWithGrace(t, 15*time.Second, clock)

	failingStore := &testHolderStore{
		writerLockStore: server.holderStore,
	}
	server.holderStore = failingStore

	streamID := uuid.New()

	state, _, _, resumed, err := server.attachSession(ctx, streamID, 1, "client-1")
	if err != nil || resumed {
		t.Fatalf("attach client-1: err=%v, resumed=%v", err, resumed)
	}

	// Clean release with remove failing still frees the lock in memory.
	failingStore.failRemove.Store(true)
	server.detachSession(ctx, streamID, state, "client-1", true)

	if failingStore.removes.Load() != 1 {
		t.Fatalf("expected 1 remove call, got %d", failingStore.removes.Load())
	}

	state.mu.Lock()
	holder := state.holder
	state.mu.Unlock()
	if holder != "" {
		t.Fatalf("expected lock freed in memory after failed remove, got holder %q", holder)
	}

	state.holderFile.mu.Lock()
	recordedContents := state.holderFile.contents
	state.holderFile.mu.Unlock()
	if recordedContents != "client-1" {
		t.Fatalf("expected holderFile to keep last holder on failed remove, got %q", recordedContents)
	}

	// Without advancing the clock, another client is granted and writes the file.
	client2State, _, _, resumed, err := server.attachSession(ctx, streamID, 1, "client-2")
	if err != nil || resumed {
		t.Fatalf("attach client-2 after failed remove: err=%v, resumed=%v", err, resumed)
	}
	defer server.detachSession(ctx, streamID, client2State, "client-2", false)

	if failingStore.saves.Load() != 2 {
		t.Fatalf("expected 2 save calls (client-1 then client-2), got %d", failingStore.saves.Load())
	}
	loaded, err := server.holderStore.load(streamID)
	if err != nil || loaded != "client-2" {
		t.Fatalf("load on disk after client-2 grant: got (%q, %v), want (client-2, nil)", loaded, err)
	}
}

// schedHolderStore wraps writerLockStore to yield execution inside save so
// idle eviction can run while a save is in progress.
type schedHolderStore struct {
	writerLockStore
}

func (s *schedHolderStore) save(streamID uuid.UUID, clientID string) error {
	runtime.Gosched()
	return s.writerLockStore.save(streamID, clientID)
}

var _ = (*schedHolderStore).save

func TestWriterLock_ContentionWithEviction(t *testing.T) {
	ctx := t.Context()
	server := newTestServer(t)

	diskStore, ok := server.holderStore.(*diskWriterLockStore)
	if !ok {
		t.Fatalf("expected *diskWriterLockStore, got %T", server.holderStore)
	}
	locksDir := diskStore.directory
	server.holderStore = &schedHolderStore{writerLockStore: server.holderStore}

	streamID := uuid.New()
	stopEviction := make(chan struct{})
	var evictionWg sync.WaitGroup

	evictionWg.Add(1)
	go func() {
		defer evictionWg.Done()
		for {
			select {
			case <-stopEviction:
				return
			default:
				server.evictIdleStreams()
				runtime.Gosched()
			}
		}
	}()

	var activeHolders atomic.Int32
	var attachWg sync.WaitGroup

	const targetGrants = 5
	clients := []string{"client-A", "client-B", "client-C", "client-D"}
	for _, clientID := range clients {
		attachWg.Add(1)
		go func(clientID string) {
			defer attachWg.Done()
			grants := 0
			for grants < targetGrants {
				state, _, _, _, err := server.attachSession(ctx, streamID, 1, clientID)
				if err != nil {
					runtime.Gosched()
					continue
				}
				grants++
				current := activeHolders.Add(1)
				if current != 1 {
					t.Errorf("client %s: activeHolders = %d, want exactly 1", clientID, current)
				}
				freshStore := newDiskWriterLockStore(locksDir)
				holder, err := freshStore.load(streamID)
				if err != nil {
					t.Errorf("client %s: freshStore.load failed: %v", clientID, err)
				} else if holder != clientID {
					t.Errorf("client %s: holder file on disk names %q, want %q", clientID, holder, clientID)
				}
				activeHolders.Add(-1)
				server.detachSession(ctx, streamID, state, clientID, false)
				runtime.Gosched()
			}
		}(clientID)
	}

	attachWg.Wait()
	close(stopEviction)
	evictionWg.Wait()
}

func TestWriterLock_CorruptAndUnreadableFile(t *testing.T) {
	ctx := t.Context()
	dataDir := t.TempDir()
	server, err := NewServer(ctx, ServerConfig{
		Backend: inmemorystorage.New(),
		DataDir: dataDir,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Errorf("server.Close: %v", err)
		}
	})

	locksDir := filepath.Join(dataDir, "writer-locks")
	if err := os.MkdirAll(locksDir, 0755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	// 1. Corrupt file: not valid hex.
	corruptID := uuid.New()
	corruptPath := filepath.Join(locksDir, corruptID.String())
	if err := os.WriteFile(corruptPath, []byte("not-valid-hex\n"), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, _, _, _, err = server.attachSession(ctx, corruptID, 1, "client-1")
	if status.Code(err) != codes.Internal {
		t.Fatalf("expected codes.Internal for corrupt file, got %v", err)
	}
	if strings.Contains(err.Error(), corruptPath) || strings.Contains(err.Error(), dataDir) {
		t.Fatalf("error message must not contain file paths: %v", err)
	}

	// 2. Corrupt file: missing trailing newline.
	noNewlineID := uuid.New()
	noNewlinePath := filepath.Join(locksDir, noNewlineID.String())
	if err := os.WriteFile(noNewlinePath, []byte("636c69656e74"), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	_, _, _, _, err = server.attachSession(ctx, noNewlineID, 1, "client-1")
	if status.Code(err) != codes.Internal {
		t.Fatalf("expected codes.Internal for file without newline, got %v", err)
	}
	if strings.Contains(err.Error(), noNewlinePath) || strings.Contains(err.Error(), dataDir) {
		t.Fatalf("error message must not contain file paths: %v", err)
	}

	// 3. Unreadable file: directory instead of a regular file triggers EISDIR.
	unreadableID := uuid.New()
	unreadablePath := filepath.Join(locksDir, unreadableID.String())
	if err := os.MkdirAll(unreadablePath, 0755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	_, _, _, _, err = server.attachSession(ctx, unreadableID, 1, "client-1")
	if status.Code(err) != codes.Internal {
		t.Fatalf("expected codes.Internal for unreadable file, got %v", err)
	}
	if strings.Contains(err.Error(), unreadablePath) || strings.Contains(err.Error(), dataDir) {
		t.Fatalf("error message must not contain file paths: %v", err)
	}
}

func TestAppend_CleanEndSurvivesEvictionAndRestart(t *testing.T) {
	sayHelloAndClose := func(t *testing.T, address string, streamID uuid.UUID, clientID []byte) *pb.HelloAck {
		t.Helper()
		connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatalf("grpc.NewClient: %v", err)
		}
		defer func() {
			if err := connection.Close(); err != nil {
				t.Errorf("connection.Close: %v", err)
			}
		}()

		stream, err := pb.NewWalBufferClient(connection).Append(t.Context())
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		if err := stream.Send(&pb.AppendRequest{
			Msg: &pb.AppendRequest_Hello{
				Hello: &pb.Hello{
					StreamId: streamID[:],
					Epoch:    1,
					ClientId: clientID,
				},
			},
		}); err != nil {
			t.Fatalf("Send Hello: %v", err)
		}
		response, err := stream.Recv()
		if err != nil {
			t.Fatalf("Recv HelloAck for %s: %v", clientID, err)
		}
		helloAck := response.GetHelloAck()
		if helloAck == nil {
			t.Fatalf("expected HelloAck, got %v", response)
		}
		if err := stream.CloseSend(); err != nil {
			t.Fatalf("CloseSend: %v", err)
		}
		for {
			_, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatalf("Recv until EOF: %v", err)
			}
		}
		return helloAck
	}

	t.Run("idle eviction", func(t *testing.T) {
		clock := newFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
		server, address, cleanup := startTestServerWithConfig(t, ServerConfig{
			Backend:         inmemorystorage.New(),
			DataDir:         t.TempDir(),
			WriterLockGrace: 15 * time.Second,
			Clock:           clock,
		})
		t.Cleanup(cleanup)

		streamID := uuid.New()
		helloAckA := sayHelloAndClose(t, address, streamID, []byte("client-A"))
		if helloAckA.GetResumed() {
			t.Fatalf("expected resumed=false for client-A, got true")
		}

		// Wait for the forwarder goroutine to exit before running the eviction pass.
		server.mu.RLock()
		state := server.streams[streamID.String()]
		server.mu.RUnlock()
		if state == nil {
			t.Fatalf("expected stream state for %s before eviction", streamID)
		}
		for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(time.Millisecond) {
			state.mu.Lock()
			waiters := state.waiters
			state.mu.Unlock()
			if waiters == 0 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for forwarder to exit: waiters=%d", waiters)
			}
		}

		server.evictIdleStreams()
		if count := server.StreamCount(); count != 0 {
			t.Fatalf("expected stream state to be evicted, got count %d", count)
		}

		helloAckB := sayHelloAndClose(t, address, streamID, []byte("client-B"))
		if helloAckB.GetResumed() {
			t.Fatalf("expected resumed=false for client-B after eviction, got true")
		}
	})

	t.Run("server restart", func(t *testing.T) {
		clock := newFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
		backend := inmemorystorage.New()
		dataDir := t.TempDir()
		streamID := uuid.New()

		_, address1, cleanup1 := startTestServerWithConfig(t, ServerConfig{
			Backend:         backend,
			DataDir:         dataDir,
			WriterLockGrace: 15 * time.Second,
			Clock:           clock,
		})
		helloAckA := sayHelloAndClose(t, address1, streamID, []byte("client-A"))
		if helloAckA.GetResumed() {
			t.Fatalf("expected resumed=false for client-A, got true")
		}
		cleanup1()

		_, address2, cleanup2 := startTestServerWithConfig(t, ServerConfig{
			Backend:         backend,
			DataDir:         dataDir,
			WriterLockGrace: 15 * time.Second,
			Clock:           clock,
		})
		t.Cleanup(cleanup2)

		helloAckB := sayHelloAndClose(t, address2, streamID, []byte("client-B"))
		if helloAckB.GetResumed() {
			t.Fatalf("expected resumed=false for client-B after restart, got true")
		}
	})
}
