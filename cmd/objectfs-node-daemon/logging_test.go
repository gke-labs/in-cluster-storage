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

package main

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/grpcutil"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/controller/controllertest"
	"github.com/gke-labs/in-cluster-storage/pkg/scoped"
	"github.com/go-logr/logr"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"k8s.io/klog/v2"
)

type sharedNodeLogState struct {
	mu      sync.Mutex
	entries []map[string]any
}

type nodeLoggingTestSink struct {
	state  *sharedNodeLogState
	values map[string]any
}

func newNodeLoggingTestSink() *nodeLoggingTestSink {
	return &nodeLoggingTestSink{
		state:  &sharedNodeLogState{},
		values: make(map[string]any),
	}
}

func (s *nodeLoggingTestSink) Init(info logr.RuntimeInfo) {}

func (s *nodeLoggingTestSink) Enabled(level int) bool {
	return true
}

func (s *nodeLoggingTestSink) Info(level int, message string, keysAndValues ...any) {
	entry := make(map[string]any, len(s.values)+len(keysAndValues)/2+1)
	for key, value := range s.values {
		entry[key] = value
	}
	for index := 0; index < len(keysAndValues)-1; index += 2 {
		if key, ok := keysAndValues[index].(string); ok {
			entry[key] = keysAndValues[index+1]
		}
	}
	entry["message"] = message

	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	s.state.entries = append(s.state.entries, entry)
}

func (s *nodeLoggingTestSink) Error(err error, message string, keysAndValues ...any) {
	entry := make(map[string]any, len(s.values)+len(keysAndValues)/2+2)
	for key, value := range s.values {
		entry[key] = value
	}
	for index := 0; index < len(keysAndValues)-1; index += 2 {
		if key, ok := keysAndValues[index].(string); ok {
			entry[key] = keysAndValues[index+1]
		}
	}
	entry["message"] = message
	entry["err"] = err

	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	s.state.entries = append(s.state.entries, entry)
}

func (s *nodeLoggingTestSink) WithName(name string) logr.LogSink {
	return s
}

func (s *nodeLoggingTestSink) WithValues(keysAndValues ...any) logr.LogSink {
	newValues := make(map[string]any, len(s.values)+len(keysAndValues)/2)
	for key, value := range s.values {
		newValues[key] = value
	}
	for index := 0; index < len(keysAndValues)-1; index += 2 {
		if key, ok := keysAndValues[index].(string); ok {
			newValues[key] = keysAndValues[index+1]
		}
	}
	return &nodeLoggingTestSink{state: s.state, values: newValues}
}

func (s *nodeLoggingTestSink) Value(key string) any {
	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	return s.values[key]
}

func (s *nodeLoggingTestSink) SawMount(expectedVolumeID, expectedTarget string) bool {
	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	for _, entry := range s.state.entries {
		if entry["message"] == "started volume watcher" && entry["volume"] == expectedVolumeID && entry["target"] == expectedTarget {
			return true
		}
	}
	return false
}

func (s *nodeLoggingTestSink) FindEntry(message string) (map[string]any, bool) {
	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	for _, entry := range s.state.entries {
		if entry["message"] == message {
			return entry, true
		}
	}
	return nil, false
}

func TestNodeDaemon_FuseMountContext_LoggerHasVolumeAndTarget(t *testing.T) {
	ctx := t.Context()
	sink := newNodeLoggingTestSink()
	rootLogger := logr.New(sink)
	klog.SetLogger(rootLogger)
	defer klog.ClearLogger()

	server := controllertest.NewTestServer(nil)
	volumeID := "test-fuse-logging-vol"

	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	pb.RegisterObjectFSControllerServer(grpcServer, server)
	go func() {
		if err := grpcServer.Serve(listener); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Logf("grpc serve error: %v", err)
		}
	}()
	defer grpcServer.Stop()

	driver, err := newDriver(ctx, "test-node", "passthrough://bufnet", t.TempDir(), 1024*1024, 10*1024*1024, 30*time.Second, 10*time.Minute, true, 30*time.Second, "", true, "/run/objectfs/fuse-holder.sock")
	if err != nil {
		t.Fatalf("newDriver failed: %v", err)
	}
	defer func() {
		if closeErr := driver.Close(); closeErr != nil {
			t.Errorf("driver.Close failed: %v", closeErr)
		}
	}()

	connection, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient failed: %v", err)
	}
	defer connection.Close()
	driver.client = pb.NewObjectFSControllerClient(connection)

	socketDescriptors, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		t.Fatalf("unix.Socketpair failed: %v", err)
	}
	defer func() {
		if closeErr := unix.Close(socketDescriptors[1]); closeErr != nil {
			t.Errorf("failed to close peer descriptor: %v", closeErr)
		}
	}()

	fakeHolder := &fakeHolderClient{
		mountDescriptor: socketDescriptors[0],
		peerDescriptor:  socketDescriptors[1],
	}
	driver.dialHolder = func(socketPath string) (fuseHolderClient, error) {
		return fakeHolder, nil
	}

	targetDirectory := t.TempDir()
	publishResponse, err := driver.NodePublishVolume(ctx, &csi.NodePublishVolumeRequest{
		VolumeId:   volumeID,
		TargetPath: targetDirectory,
		VolumeContext: map[string]string{
			"mode": "fuse",
		},
	})
	if err != nil {
		t.Fatalf("NodePublishVolume failed: %v", err)
	}
	if publishResponse == nil {
		t.Fatal("expected non-nil NodePublishVolumeResponse")
	}

	// Wait for watcher to establish subscription and emit log with volume and target
	deadline := time.Now().Add(5 * time.Second)
	sawMount := false
	for time.Now().Before(deadline) {
		if sink.SawMount(volumeID, targetDirectory) {
			sawMount = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !sawMount {
		t.Fatalf("expected watcher log to carry volume=%q and target=%q", volumeID, targetDirectory)
	}

	if _, unpublishErr := driver.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{
		VolumeId:   volumeID,
		TargetPath: targetDirectory,
	}); unpublishErr != nil {
		t.Fatalf("NodeUnpublishVolume failed: %v", unpublishErr)
	}
}

func TestNodeDaemon_CSINodeServiceInterceptor(t *testing.T) {
	sink := newNodeLoggingTestSink()
	rootLogger := logr.New(sink)
	ctx := scoped.WithLog(t.Context(), rootLogger)

	interceptor := grpcutil.UnaryServerInterceptor()
	request := &csi.NodePublishVolumeRequest{
		VolumeId:   "vol-csi-456",
		TargetPath: "/test/target",
	}
	info := &grpc.UnaryServerInfo{
		FullMethod: "/csi.v1.Node/NodePublishVolume",
	}

	called := false
	handler := func(handlerCtx context.Context, req any) (any, error) {
		called = true
		logger := scoped.Log(handlerCtx)
		extractedSink, ok := logger.GetSink().(*nodeLoggingTestSink)
		if !ok {
			t.Fatalf("expected *nodeLoggingTestSink, got %T", logger.GetSink())
		}
		if got := extractedSink.Value("rpc.service"); got != "csi.v1.Node" {
			t.Fatalf("expected rpc.service=%q, got %v", "csi.v1.Node", got)
		}
		if got := extractedSink.Value("rpc.method"); got != "NodePublishVolume" {
			t.Fatalf("expected rpc.method=%q, got %v", "NodePublishVolume", got)
		}
		if got := extractedSink.Value("volume"); got != "vol-csi-456" {
			t.Fatalf("expected volume=%q, got %v", "vol-csi-456", got)
		}
		return &csi.NodePublishVolumeResponse{}, nil
	}

	_, err := interceptor(ctx, request, info, handler)
	if err != nil {
		t.Fatalf("interceptor returned error: %v", err)
	}
	if !called {
		t.Fatal("handler was not called")
	}
}

func TestNodeDaemon_ExclusiveLocalLogging(t *testing.T) {
	sink := newNodeLoggingTestSink()
	rootLogger := logr.New(sink)
	csiHandle := "csi-vol-handle-1234"
	objectFSVolumeID := "test-exclusive-logging-vol"
	targetPath := "/var/lib/kubelet/exclusive-target"

	// Simulate gRPC interceptor which sets the CSI handle as volume on ctx.
	ctx := scoped.WithLog(t.Context(), rootLogger.WithValues("volume", csiHandle))

	server := controllertest.NewTestServer(nil)
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	pb.RegisterObjectFSControllerServer(grpcServer, server)
	go func() {
		if err := grpcServer.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("grpcServer.Serve failed: %v", err)
		}
	}()
	defer grpcServer.Stop()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Errorf("conn.Close failed: %v", err)
		}
	}()
	client := pb.NewObjectFSControllerClient(conn)

	// Simulate node-1 holding the lease so publish encounters an Aborted conflict
	_, err = server.AcquireExclusive(ctx, &pb.AcquireExclusiveRequest{
		VolumeId: objectFSVolumeID,
		NodeId:   "node-1",
		MountId:  "/var/lib/kubelet/target-1",
	})
	if err != nil {
		t.Fatalf("AcquireExclusive failed: %v", err)
	}

	driver := &objectFSDriver{
		nodeID:   "node-2",
		cacheDir: t.TempDir(),
		client:   client,
		mounts:   make(map[string]*activeMount),
	}
	defer func() {
		if err := driver.Close(); err != nil {
			t.Errorf("driver.Close failed: %v", err)
		}
	}()

	req := &csi.NodePublishVolumeRequest{
		VolumeId:   csiHandle,
		TargetPath: targetPath,
	}

	_, err = driver.publishExclusiveLocal(ctx, req, objectFSVolumeID)
	if err == nil {
		t.Fatal("expected publishExclusiveLocal to fail due to hold conflict")
	}

	// Verify publish event log carries the objectfs volumeID argument, not the CSI handle
	publishEntry, found := sink.FindEntry("publishing ObjectFS volume in exclusive-local mode")
	if !found {
		t.Fatal("expected log entry for publishing ObjectFS volume in exclusive-local mode")
	}
	if publishEntry["target"] != targetPath {
		t.Fatalf("expected target=%q, got %v", targetPath, publishEntry["target"])
	}
	if publishEntry["volume"] != objectFSVolumeID {
		t.Fatalf("expected volume=%q (ObjectFS volume ID), got %v", objectFSVolumeID, publishEntry["volume"])
	}

	// Verify conflict warning logged at Info level carries the objectfs volumeID argument and err
	conflictEntry, found := sink.FindEntry("volume is held by another node, retrying publish")
	if !found {
		t.Fatal("expected log entry for volume held by another node")
	}
	if conflictEntry["volume"] != objectFSVolumeID {
		t.Fatalf("expected volume=%q (ObjectFS volume ID), got %v", objectFSVolumeID, conflictEntry["volume"])
	}
	if conflictEntry["err"] == nil {
		t.Fatal("expected non-nil err field in conflict log entry")
	}

	// Verify unpublish event log also carries the objectfs volumeID argument
	volumeDir := filepath.Join(t.TempDir(), objectFSVolumeID)
	unpublishRequest := &csi.NodeUnpublishVolumeRequest{
		VolumeId:   csiHandle,
		TargetPath: targetPath,
	}
	unpublishResponse, unpublishErr := driver.unpublishExclusiveLocal(ctx, unpublishRequest, objectFSVolumeID, volumeDir, nil)
	if unpublishErr != nil {
		t.Fatalf("unpublishExclusiveLocal failed: %v", unpublishErr)
	}
	if unpublishResponse == nil {
		t.Fatal("expected non-nil NodeUnpublishVolumeResponse")
	}

	unpublishEntry, found := sink.FindEntry("unpublishing ObjectFS exclusive-local volume")
	if !found {
		t.Fatal("expected log entry for unpublishing ObjectFS exclusive-local volume")
	}
	if unpublishEntry["target"] != targetPath {
		t.Fatalf("expected target=%q, got %v", targetPath, unpublishEntry["target"])
	}
	if unpublishEntry["volume"] != objectFSVolumeID {
		t.Fatalf("expected volume=%q (ObjectFS volume ID), got %v", objectFSVolumeID, unpublishEntry["volume"])
	}
}

// TestNodeDaemon_MaterialiseLogging verifies that volume materialisation logs progress and completion carrying the resolved ObjectFS volume ID.
func TestNodeDaemon_MaterialiseLogging(t *testing.T) {
	sink := newNodeLoggingTestSink()
	rootLogger := logr.New(sink)
	csiHandle := "csi-vol-handle-mat"
	objectFSVolumeID := "test-mat-logging-vol"

	// Simulate gRPC interceptor setting CSI handle as volume on ctx.
	ctx := scoped.WithLog(t.Context(), rootLogger.WithValues("volume", csiHandle))

	server := controllertest.NewTestServer(nil)
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	pb.RegisterObjectFSControllerServer(grpcServer, server)
	go func() {
		if err := grpcServer.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("grpcServer.Serve failed: %v", err)
		}
	}()
	defer grpcServer.Stop()

	connection, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer func() {
		if err := connection.Close(); err != nil {
			t.Errorf("connection.Close failed: %v", err)
		}
	}()
	client := pb.NewObjectFSControllerClient(connection)

	tempDirectory := t.TempDir()
	lowerPath := filepath.Join(tempDirectory, "lower")

	createResponse, err := server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:       objectFSVolumeID,
		ParentInode:    1,
		Name:           "file.txt",
		Mode:           0644,
		InitialContent: []byte("materialised-content"),
	})
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}
	if createResponse.GetError() != 0 {
		t.Fatalf("CreateFile returned error code %d", createResponse.GetError())
	}

	driver := &objectFSDriver{
		nodeID:   "test-node",
		cacheDir: tempDirectory,
		client:   client,
	}

	if err := driver.materialiseVolume(ctx, client, objectFSVolumeID, lowerPath); err != nil {
		t.Fatalf("materialiseVolume failed: %v", err)
	}

	entry, found := sink.FindEntry("completed materialisation of volume")
	if !found {
		t.Fatal("expected log entry for completed materialisation of volume")
	}
	if entry["volume"] != objectFSVolumeID {
		t.Fatalf("expected volume=%q (ObjectFS volume ID), got %v", objectFSVolumeID, entry["volume"])
	}
	if entry["path"] != lowerPath {
		t.Fatalf("expected path=%q, got %v", lowerPath, entry["path"])
	}
	if entry["files"] != int64(1) {
		t.Fatalf("expected files=1, got %v", entry["files"])
	}
	if entry["bytes"] != int64(len("materialised-content")) {
		t.Fatalf("expected bytes=%d, got %v", len("materialised-content"), entry["bytes"])
	}
}

// TestNodeDaemon_CheckpointLogging verifies that checkpoint scanning and replay log with the resolved ObjectFS volume ID and structured metadata.
func TestNodeDaemon_CheckpointLogging(t *testing.T) {
	sink := newNodeLoggingTestSink()
	rootLogger := logr.New(sink)
	csiHandle := "csi-vol-handle-checkpoint"
	objectFSVolumeID := "test-checkpoint-logging-vol"

	// Simulate gRPC interceptor setting CSI handle as volume on ctx.
	ctx := scoped.WithLog(t.Context(), rootLogger.WithValues("volume", csiHandle))

	// 1. Verify ScanUpperLayer skips unsupported special files and logs warning carrying ObjectFS volume ID
	baseDirectory := t.TempDir()
	lowerDirectory := filepath.Join(baseDirectory, "lower")
	upperDirectory := filepath.Join(baseDirectory, "upper")
	if err := os.MkdirAll(lowerDirectory, 0755); err != nil {
		t.Fatalf("MkdirAll lowerDirectory failed: %v", err)
	}
	if err := os.MkdirAll(upperDirectory, 0755); err != nil {
		t.Fatalf("MkdirAll upperDirectory failed: %v", err)
	}

	fifoPath := filepath.Join(upperDirectory, "test_fifo")
	if err := unix.Mkfifo(fifoPath, 0666); err != nil {
		t.Skipf("mkfifo not supported in environment: %v", err)
	}

	scanContext := scoped.WithLog(ctx, scoped.Log(ctx).WithValues("volume", objectFSVolumeID))
	operations, err := ScanUpperLayer(scanContext, lowerDirectory, upperDirectory)
	if err != nil {
		t.Fatalf("ScanUpperLayer failed: %v", err)
	}
	if len(operations) != 0 {
		t.Fatalf("expected special file to be skipped resulting in 0 operations, got %d: %+v", len(operations), operations)
	}
	entry, found := sink.FindEntry("exclusive-local: skipping unsupported special file")
	if !found {
		t.Fatal("expected log entry for skipping unsupported special file")
	}
	if entry["volume"] != objectFSVolumeID {
		t.Fatalf("expected volume=%q (ObjectFS volume ID), got %v", objectFSVolumeID, entry["volume"])
	}
	if entry["path"] != "test_fifo" {
		t.Fatalf("expected path=%q, got %v", "test_fifo", entry["path"])
	}

	// 2. Verify ReplayOperations logs attribute already removed on ENODATA carrying ObjectFS volume ID
	fakeClient := &fakeRemoveXAttrClient{
		errCode: int32(syscall.ENODATA),
	}
	replayOperations := []FileOp{
		{
			Type: OpRemoveXAttr,
			Path: "file.txt",
			Name: "user.missing",
		},
	}
	position, err := ReplayOperations(ctx, fakeClient, objectFSVolumeID, replayOperations)
	if err != nil {
		t.Fatalf("ReplayOperations failed: %v", err)
	}
	if position != 42 {
		t.Fatalf("expected stream position 42, got %d", position)
	}
	xattrEntry, found := sink.FindEntry("removeXAttr attribute already removed on inode")
	if !found {
		t.Fatal("expected log entry for removeXAttr attribute already removed on inode")
	}
	if xattrEntry["volume"] != objectFSVolumeID {
		t.Fatalf("expected volume=%q (ObjectFS volume ID), got %v", objectFSVolumeID, xattrEntry["volume"])
	}
	if xattrEntry["path"] != "file.txt" {
		t.Fatalf("expected path=%q, got %v", "file.txt", xattrEntry["path"])
	}
	if xattrEntry["name"] != "user.missing" {
		t.Fatalf("expected name=%q, got %v", "user.missing", xattrEntry["name"])
	}
}

// TestNodeDaemon_FUSELogging verifies that NodePublishVolume and NodeUnpublishVolume
// emit structured log entries with the resolved ObjectFS volume ID rather than the CSI volume handle.
func TestNodeDaemon_FUSELogging(t *testing.T) {
	sink := newNodeLoggingTestSink()
	rootLogger := logr.New(sink)
	csiHandle := "csi-vol-fuse-handle-9999"
	objectFSVolumeID := "test-fuse-logging-vol"
	targetDirectory := t.TempDir()

	// Simulate gRPC interceptor which sets the CSI handle as volume on ctx.
	ctx := scoped.WithLog(t.Context(), rootLogger.WithValues("volume", csiHandle))

	server := controllertest.NewTestServer(nil)
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	pb.RegisterObjectFSControllerServer(grpcServer, server)
	go func() {
		if err := grpcServer.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("grpcServer.Serve failed: %v", err)
		}
	}()
	defer grpcServer.Stop()

	connection, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer func() {
		if closeErr := connection.Close(); closeErr != nil {
			t.Errorf("connection.Close failed: %v", closeErr)
		}
	}()
	client := pb.NewObjectFSControllerClient(connection)

	socketDescriptors, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		t.Fatalf("unix.Socketpair failed: %v", err)
	}
	defer func() {
		if closeErr := unix.Close(socketDescriptors[1]); closeErr != nil {
			t.Errorf("failed to close peer descriptor: %v", closeErr)
		}
	}()

	fakeHolder := &fakeHolderClient{
		mountDescriptor: socketDescriptors[0],
		peerDescriptor:  socketDescriptors[1],
	}

	driver := &objectFSDriver{
		nodeID:           "test-node",
		cacheDir:         t.TempDir(),
		client:           client,
		dialHolder:       func(string) (fuseHolderClient, error) { return fakeHolder, nil },
		mounts:           make(map[string]*activeMount),
		fuseHolderSocket: "/run/objectfs/fuse-holder.sock",
		rpcTimeout:       30 * time.Second,
		syncTimeout:      10 * time.Minute,
	}
	defer func() {
		if closeErr := driver.Close(); closeErr != nil {
			t.Errorf("driver.Close failed: %v", closeErr)
		}
	}()

	publishRequest := &csi.NodePublishVolumeRequest{
		VolumeId:      csiHandle,
		TargetPath:    targetDirectory,
		VolumeContext: map[string]string{"mode": "fuse", "volumeID": objectFSVolumeID},
	}

	publishResponse, err := driver.NodePublishVolume(ctx, publishRequest)
	if err != nil {
		t.Fatalf("NodePublishVolume failed: %v", err)
	}
	if publishResponse == nil {
		t.Fatal("expected non-nil NodePublishVolumeResponse")
	}

	duplicateResponse, err := driver.NodePublishVolume(ctx, publishRequest)
	if err != nil {
		t.Fatalf("duplicate NodePublishVolume failed: %v", err)
	}
	if duplicateResponse == nil {
		t.Fatal("expected non-nil duplicate NodePublishVolumeResponse")
	}

	unpublishRequest := &csi.NodeUnpublishVolumeRequest{
		VolumeId:   csiHandle,
		TargetPath: targetDirectory,
	}
	unpublishResponse, err := driver.NodeUnpublishVolume(ctx, unpublishRequest)
	if err != nil {
		t.Fatalf("NodeUnpublishVolume failed: %v", err)
	}
	if unpublishResponse == nil {
		t.Fatal("expected non-nil NodeUnpublishVolumeResponse")
	}

	for _, expectedMessage := range []string{
		"publishing ObjectFS volume",
		"successfully mounted ObjectFS FUSE volume",
		"volume is already published",
		"successfully unpublished ObjectFS volume",
	} {
		entry, found := sink.FindEntry(expectedMessage)
		if !found {
			t.Fatalf("expected log entry for %q", expectedMessage)
		}
		if entry["target"] != targetDirectory {
			t.Fatalf("%s: expected target=%q, got %v", expectedMessage, targetDirectory, entry["target"])
		}
		if entry["volume"] != objectFSVolumeID {
			t.Fatalf("%s: expected volume=%q (ObjectFS volume ID), got %v", expectedMessage, objectFSVolumeID, entry["volume"])
		}
	}

	unpublishEntry, found := sink.FindEntry("unpublishing ObjectFS volume")
	if !found {
		t.Fatal("expected log entry for unpublishing ObjectFS volume")
	}
	if unpublishEntry["target"] != targetDirectory {
		t.Fatalf("expected target=%q, got %v", targetDirectory, unpublishEntry["target"])
	}
}
