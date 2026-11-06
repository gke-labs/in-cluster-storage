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
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/wal/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore/inmemorystorage"
	"github.com/gke-labs/in-cluster-storage/pkg/wal"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func openStreamForID(t *testing.T, address string, streamID uuid.UUID) pb.WalBuffer_AppendClient {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := conn.Close(); closeErr != nil {
			t.Errorf("connection close failed: %v", closeErr)
		}
	})

	stream, err := pb.NewWalBufferClient(conn).Append(t.Context())
	if err != nil {
		t.Fatalf("failed to start stream: %v", err)
	}
	if err := stream.Send(&pb.AppendRequest{
		Msg: &pb.AppendRequest_Hello{Hello: &pb.Hello{StreamId: streamID[:]}},
	}); err != nil {
		t.Fatalf("failed to send hello: %v", err)
	}
	response, err := stream.Recv()
	if err != nil || response.GetHelloAck() == nil {
		t.Fatalf("expected HelloAck, got %v (err: %v)", response, err)
	}
	return stream
}

func assertStoredRecord(t *testing.T, server *Server, expectedSequence uint64) {
	var storedRecords []*wal.LogRecord
	if err := server.localStore.ReadFrom(1, func(storedRecord *wal.LogRecord) error {
		storedRecords = append(storedRecords, storedRecord)
		return nil
	}); err != nil {
		t.Fatalf("failed to read records from local store: %v", err)
	}
	if len(storedRecords) != 1 || storedRecords[0].StreamSeq != expectedSequence {
		t.Fatalf("expected 1 record with sequence %d, got %d records", expectedSequence, len(storedRecords))
	}
}

func TestAppend_DuplicateWithinSameBatch(t *testing.T) {
	backend := inmemorystorage.New()
	server, address, cleanup := startTestServerWithBatchDelay(t, backend, t.TempDir(), 100*time.Millisecond)
	defer cleanup()

	streamID := uuid.New()
	stream1 := openStreamForID(t, address, streamID)
	stream2 := openStreamForID(t, address, streamID)

	sendRecord := func(stream pb.WalBuffer_AppendClient) {
		if err := stream.Send(&pb.AppendRequest{
			Msg: &pb.AppendRequest_Record{
				Record: &pb.AppendRecord{StreamSeq: 1, Payload: []byte("duplicate-payload")},
			},
		}); err != nil {
			t.Fatalf("failed to send record: %v", err)
		}
	}
	sendRecord(stream1)
	sendRecord(stream2)

	recvAck := func(stream pb.WalBuffer_AppendClient) {
		response, err := stream.Recv()
		if err != nil {
			t.Fatalf("recv failed: %v", err)
		}
		if ack := response.GetAck(); ack == nil || ack.WitnessAckedStreamSeq < 1 {
			t.Fatalf("expected ack >= 1, got: %+v", response)
		}
	}
	recvAck(stream1)
	recvAck(stream2)

	assertStoredRecord(t, server, 1)
}

func TestAppend_DuplicateSplitAcrossBatches(t *testing.T) {
	backend := inmemorystorage.New()
	server, _, cleanup := startTestServer(t, backend, t.TempDir())
	defer cleanup()

	ctx := t.Context()
	streamID := uuid.New()
	appendDirectRecord(t, server, streamID, 1, []byte("record-1"))

	streamState := server.getOrCreateStream(ctx, streamID)
	streamState.mu.Lock()
	streamState.inFlight++
	streamState.mu.Unlock()

	server.incomingChan <- incomingItem{
		streamID:  streamID,
		streamSeq: 1,
		payload:   []byte("record-1-duplicate"),
	}

	streamState.mu.Lock()
	for streamState.inFlight > 0 && streamState.commitErr == nil {
		streamState.cond.Wait()
	}
	commitError := streamState.commitErr
	streamState.mu.Unlock()
	if commitError != nil {
		t.Fatalf("commit failed: %v", commitError)
	}

	assertStoredRecord(t, server, 1)
}
