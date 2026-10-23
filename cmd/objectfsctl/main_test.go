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
	"bytes"
	"crypto/sha256"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/objectfs/controller"
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
	"google.golang.org/grpc"
)

func startTestServer(t *testing.T) (*controller.Server, string, func()) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	backend := controller.NewMemoryBackend()
	server := controller.NewServer(backend)
	pb.RegisterObjectFSControllerServer(grpcServer, server)

	go func() {
		_ = grpcServer.Serve(lis)
	}()

	cleanup := func() {
		grpcServer.Stop()
		_ = lis.Close()
	}

	return server, lis.Addr().String(), cleanup
}

func startTestUnixServer(t *testing.T) (*controller.Server, string, func()) {
	t.Helper()

	tempDir, err := os.MkdirTemp("", "objectfsctl-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	sockPath := filepath.Join(tempDir, "test.sock")

	lis, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("failed to listen on unix socket: %v", err)
	}

	grpcServer := grpc.NewServer()
	backend := controller.NewMemoryBackend()
	server := controller.NewServer(backend)
	pb.RegisterObjectFSControllerServer(grpcServer, server)

	go func() {
		_ = grpcServer.Serve(lis)
	}()

	cleanup := func() {
		grpcServer.Stop()
		_ = lis.Close()
		_ = os.RemoveAll(tempDir)
	}

	return server, "unix://" + sockPath, cleanup
}

func executeCommand(args ...string) (string, error) {
	cmd := NewRootCommand()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return buf.String(), err
}

func TestBlobsListAndGet(t *testing.T) {
	ctx := t.Context()
	server, addr, cleanup := startTestServer(t)
	defer cleanup()

	// 1. Missing --server flag
	_, err := executeCommand("blobs", "list")
	if err == nil || !strings.Contains(err.Error(), "--server is required") {
		t.Fatalf("expected --server is required error, got: %v", err)
	}

	// 2. Empty list when no blobs exist
	out, err := executeCommand("--server", addr, "blobs", "list")
	if err != nil {
		t.Fatalf("blobs list failed: %v", err)
	}
	if strings.TrimSpace(out) != "" {
		t.Fatalf("expected empty output, got: %q", out)
	}

	// 3. Create files in a volume and flush to generate blobs
	volumeID := "vol-test-ctl"
	content1 := bytes.Repeat([]byte("Hello World from ObjectFS Blobs!"), 200)
	content2 := bytes.Repeat([]byte("Second blob data for verifying multiple blobs in listing"), 100)

	h1 := sha256.Sum256(content1)
	sha1 := fmt.Sprintf("%x", h1)
	h2 := sha256.Sum256(content2)
	sha2 := fmt.Sprintf("%x", h2)

	_, err = server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:       volumeID,
		ParentInode:    1,
		Name:           "test1.txt",
		InitialContent: content1,
	})
	if err != nil {
		t.Fatalf("failed to create file 1: %v", err)
	}

	_, err = server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:       volumeID,
		ParentInode:    1,
		Name:           "test2.txt",
		InitialContent: content2,
	})
	if err != nil {
		t.Fatalf("failed to create file 2: %v", err)
	}

	if err := server.FlushAll(ctx); err != nil {
		t.Fatalf("FlushAll failed: %v", err)
	}

	// 4. List blobs
	out, err = executeCommand("--server", addr, "blobs", "list")
	if err != nil {
		t.Fatalf("blobs list failed: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 blobs in list, got %d (output: %q)", len(lines), out)
	}
	found := make(map[string]bool)
	for _, l := range lines {
		found[strings.TrimSpace(l)] = true
	}
	if !found[sha1] || !found[sha2] {
		t.Fatalf("missing expected SHAs in output: %v", found)
	}

	// 5. Get blob 1
	out1, err := executeCommand("--server", addr, "blobs", "get", sha1)
	if err != nil {
		t.Fatalf("blobs get %s failed: %v", sha1, err)
	}
	if out1 != string(content1) {
		t.Fatalf("blob 1 content mismatch: got %q, expected %q", out1, string(content1))
	}

	// Verify sha256 of downloaded content
	outH1 := sha256.Sum256([]byte(out1))
	if fmt.Sprintf("%x", outH1) != sha1 {
		t.Fatalf("downloaded content SHA mismatch: got %x, expected %s", outH1, sha1)
	}

	// 6. Get blob 2
	out2, err := executeCommand("--server", addr, "blobs", "get", sha2)
	if err != nil {
		t.Fatalf("blobs get %s failed: %v", sha2, err)
	}
	if out2 != string(content2) {
		t.Fatalf("blob 2 content mismatch: got %q, expected %q", out2, string(content2))
	}

	// 7. Get non-existent blob
	_, err = executeCommand("--server", addr, "blobs", "get", "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789")
	if err == nil {
		t.Fatalf("expected error for non-existent blob, got nil")
	}

	// 8. Missing arg for get
	_, err = executeCommand("--server", addr, "blobs", "get")
	if err == nil {
		t.Fatalf("expected error for missing get argument, got nil")
	}

	// 9. Blobs list with --limit
	limOut, err := executeCommand("--server", addr, "blobs", "list", "--limit", "1")
	if err != nil {
		t.Fatalf("blobs list --limit 1 failed: %v", err)
	}
	limLines := strings.Split(strings.TrimSpace(limOut), "\n")
	if len(limLines) != 1 {
		t.Fatalf("expected 1 blob with --limit 1, got %d (output: %q)", len(limLines), limOut)
	}

	// 10. Blobs list with --from
	fromOut, err := executeCommand("--server", addr, "blobs", "list", "--from", limLines[0])
	if err != nil {
		t.Fatalf("blobs list --from failed: %v", err)
	}
	fromLines := strings.Split(strings.TrimSpace(fromOut), "\n")
	if len(fromLines) != 1 {
		t.Fatalf("expected 1 blob with --from, got %d (output: %q)", len(fromLines), fromOut)
	}

	// 11. Blobs list with --prefix
	pref := sha1[:4]
	prefOut, err := executeCommand("--server", addr, "blobs", "list", "--prefix", pref)
	if err != nil {
		t.Fatalf("blobs list --prefix failed: %v", err)
	}
	for _, l := range strings.Split(strings.TrimSpace(prefOut), "\n") {
		if l != "" && !strings.HasPrefix(l, pref) {
			t.Fatalf("expected line %s to have prefix %s", l, pref)
		}
	}

	// 12. Blobs get with --offset and --limit
	partialOut, err := executeCommand("--server", addr, "blobs", "get", sha1, "--offset", "6", "--limit", "5")
	if err != nil {
		t.Fatalf("blobs get with offset/limit failed: %v", err)
	}
	if partialOut != "World" {
		t.Fatalf("expected %q, got %q", "World", partialOut)
	}
}

func TestBlobsOverUnixSocket(t *testing.T) {
	ctx := t.Context()
	server, unixAddr, cleanup := startTestUnixServer(t)
	defer cleanup()

	volumeID := "unix-vol"
	content := bytes.Repeat([]byte("Testing objectfsctl over Unix domain socket "), 100)
	h := sha256.Sum256(content)
	sha := fmt.Sprintf("%x", h)

	_, err := server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:       volumeID,
		ParentInode:    1,
		Name:           "unix_file.txt",
		InitialContent: content,
	})
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	if err := server.FlushAll(ctx); err != nil {
		t.Fatalf("FlushAll failed: %v", err)
	}

	// Test blobs list
	out, err := executeCommand("--server", unixAddr, "blobs", "list")
	if err != nil {
		t.Fatalf("blobs list failed over unix socket: %v", err)
	}
	if !strings.Contains(out, sha) {
		t.Fatalf("expected %s in output %q", sha, out)
	}

	// Test blobs get
	getOut, err := executeCommand("--server", unixAddr, "blobs", "get", sha)
	if err != nil {
		t.Fatalf("blobs get failed over unix socket: %v", err)
	}
	if getOut != string(content) {
		t.Fatalf("mismatch: got %q, expected %q", getOut, string(content))
	}
}

func TestVolumesList(t *testing.T) {
	ctx := t.Context()
	server, addr, cleanup := startTestServer(t)
	defer cleanup()

	// 1. Missing --server flag
	_, err := executeCommand("volumes", "list")
	if err == nil || !strings.Contains(err.Error(), "--server is required") {
		t.Fatalf("expected --server is required error, got: %v", err)
	}

	// 2. Empty list when no volumes exist
	out, err := executeCommand("--server", addr, "volumes", "list")
	if err != nil {
		t.Fatalf("volumes list failed: %v", err)
	}
	if strings.TrimSpace(out) != "" {
		t.Fatalf("expected empty output, got: %q", out)
	}

	// 3. Create files in 3 volumes
	volNames := []string{"vol-z", "vol-a", "vol-m"}
	for _, v := range volNames {
		_, err := server.CreateFile(ctx, &pb.CreateFileRequest{
			VolumeId:       v,
			ParentInode:    1,
			Name:           "test.txt",
			InitialContent: []byte("sample content for " + v),
		})
		if err != nil {
			t.Fatalf("CreateFile failed for %s: %v", v, err)
		}
	}

	// 4. List volumes -> sorted: vol-a, vol-m, vol-z
	out, err = executeCommand("--server", addr, "volumes", "list")
	if err != nil {
		t.Fatalf("volumes list failed: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 3 {
		t.Fatalf("expected 3 volumes, got %d (output: %q)", len(lines), out)
	}
	expected := []string{"vol-a", "vol-m", "vol-z"}
	for i, exp := range expected {
		if lines[i] != exp {
			t.Fatalf("line %d: expected %s, got %s", i, exp, lines[i])
		}
	}

	// 5. List with alias 'vol ls'
	aliasOut, err := executeCommand("--server", addr, "vol", "ls")
	if err != nil {
		t.Fatalf("vol ls failed: %v", err)
	}
	if strings.TrimSpace(aliasOut) != strings.TrimSpace(out) {
		t.Fatalf("vol ls output mismatch: got %q, expected %q", aliasOut, out)
	}

	// 6. List with --limit
	limOut, err := executeCommand("--server", addr, "volumes", "list", "--limit", "2")
	if err != nil {
		t.Fatalf("volumes list --limit 2 failed: %v", err)
	}
	limLines := strings.Split(strings.TrimSpace(limOut), "\n")
	if len(limLines) != 2 || limLines[0] != "vol-a" || limLines[1] != "vol-m" {
		t.Fatalf("unexpected output with limit 2: %v", limLines)
	}

	// 7. List with --from
	fromOut, err := executeCommand("--server", addr, "volumes", "list", "--from", "vol-a")
	if err != nil {
		t.Fatalf("volumes list --from failed: %v", err)
	}
	fromLines := strings.Split(strings.TrimSpace(fromOut), "\n")
	if len(fromLines) != 2 || fromLines[0] != "vol-m" || fromLines[1] != "vol-z" {
		t.Fatalf("unexpected output with --from vol-a: %v", fromLines)
	}
}

func TestSnapshotsListAndCreate(t *testing.T) {
	ctx := t.Context()
	server, addr, cleanup := startTestServer(t)
	defer cleanup()

	volumeID := "test-snap-cli"

	// 1. Missing --server flag
	_, err := executeCommand("snapshot", "create", volumeID)
	if err == nil || !strings.Contains(err.Error(), "--server is required") {
		t.Fatalf("expected --server is required error, got: %v", err)
	}

	_, err = executeCommand("snapshots", "list", volumeID)
	if err == nil || !strings.Contains(err.Error(), "--server is required") {
		t.Fatalf("expected --server is required error, got: %v", err)
	}

	// 2. Missing volume_id
	_, err = executeCommand("--server", addr, "snapshot", "create")
	if err == nil || !strings.Contains(err.Error(), "volume_id is required") {
		t.Fatalf("expected volume_id is required error, got: %v", err)
	}

	_, err = executeCommand("--server", addr, "snapshots", "list")
	if err == nil || !strings.Contains(err.Error(), "volume_id is required") {
		t.Fatalf("expected volume_id is required error, got: %v", err)
	}

	// 3. Create file in volume
	_, err = server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:       volumeID,
		ParentInode:    1,
		Name:           "doc1.txt",
		InitialContent: []byte("doc 1 content"),
	})
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}

	// 4. Create snapshot 1 via CLI
	snap1Out, err := executeCommand("--server", addr, "snapshot", "create", volumeID)
	if err != nil {
		t.Fatalf("snapshot create failed: %v", err)
	}
	snap1 := strings.TrimSpace(snap1Out)
	if snap1 == "" || (!strings.HasSuffix(snap1, ".sqlite") && !strings.HasSuffix(snap1, ".snap")) {
		t.Fatalf("expected .sqlite snapshot output, got: %q", snap1Out)
	}

	time.Sleep(15 * time.Millisecond)

	// Modify and create snapshot 2 via CLI
	_, err = server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:       volumeID,
		ParentInode:    1,
		Name:           "doc2.txt",
		InitialContent: []byte("doc 2 content"),
	})
	if err != nil {
		t.Fatalf("CreateFile 2 failed: %v", err)
	}

	snap2Out, err := executeCommand("--server", addr, "snapshots", "create", "--volume", volumeID)
	if err != nil {
		t.Fatalf("snapshots create with --volume flag failed: %v", err)
	}
	snap2 := strings.TrimSpace(snap2Out)
	if snap2 == "" || (!strings.HasSuffix(snap2, ".sqlite") && !strings.HasSuffix(snap2, ".snap")) {
		t.Fatalf("expected .sqlite snapshot output, got: %q", snap2Out)
	}

	time.Sleep(15 * time.Millisecond)

	// Modify and create snapshot 3 via CLI
	_, err = server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:       volumeID,
		ParentInode:    1,
		Name:           "doc3.txt",
		InitialContent: []byte("doc 3 content"),
	})
	if err != nil {
		t.Fatalf("CreateFile 3 failed: %v", err)
	}

	snap3Out, err := executeCommand("--server", addr, "snapshot", "create", volumeID)
	if err != nil {
		t.Fatalf("snapshot create 3 failed: %v", err)
	}
	snap3 := strings.TrimSpace(snap3Out)

	// 5. List snapshots via CLI: objectfsctl snapshots list <volume_id>
	listOut, err := executeCommand("--server", addr, "snapshots", "list", volumeID)
	if err != nil {
		t.Fatalf("snapshots list failed: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(listOut), "\n")
	if len(lines) != 3 {
		t.Fatalf("expected 3 snapshots, got %d (output: %q)", len(lines), listOut)
	}
	if lines[0] != snap1 || lines[1] != snap2 || lines[2] != snap3 {
		t.Fatalf("snapshots not in expected chronological order: %v (expected [%s, %s, %s])", lines, snap1, snap2, snap3)
	}

	// 6. Test snapshot list with aliases 'snap ls' and '--volume' flag
	aliasListOut, err := executeCommand("--server", addr, "snap", "ls", "--volume", volumeID)
	if err != nil {
		t.Fatalf("snap ls failed: %v", err)
	}
	if strings.TrimSpace(aliasListOut) != strings.TrimSpace(listOut) {
		t.Fatalf("snap ls output mismatch: got %q, expected %q", aliasListOut, listOut)
	}

	// 7. List with --limit 2
	limOut, err := executeCommand("--server", addr, "snapshots", "list", volumeID, "--limit", "2")
	if err != nil {
		t.Fatalf("snapshots list --limit 2 failed: %v", err)
	}
	limLines := strings.Split(strings.TrimSpace(limOut), "\n")
	if len(limLines) != 2 || limLines[0] != snap1 || limLines[1] != snap2 {
		t.Fatalf("unexpected output with limit 2: %v", limLines)
	}

	// 8. List with --from cursor
	fromOut, err := executeCommand("--server", addr, "snapshots", "list", volumeID, "--from", snap1)
	if err != nil {
		t.Fatalf("snapshots list --from failed: %v", err)
	}
	fromLines := strings.Split(strings.TrimSpace(fromOut), "\n")
	if len(fromLines) != 2 || fromLines[0] != snap2 || fromLines[1] != snap3 {
		t.Fatalf("unexpected output with --from %s: %v", snap1, fromLines)
	}
}

func TestVolumesAndSnapshotsOverUnixSocket(t *testing.T) {
	ctx := t.Context()
	server, unixAddr, cleanup := startTestUnixServer(t)
	defer cleanup()

	volumeID := "unix-snap-vol"
	_, err := server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:       volumeID,
		ParentInode:    1,
		Name:           "file.txt",
		InitialContent: []byte("content on unix socket"),
	})
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}

	// Test volumes list over unix socket
	volsOut, err := executeCommand("--server", unixAddr, "volumes", "list")
	if err != nil {
		t.Fatalf("volumes list failed over unix socket: %v", err)
	}
	if !strings.Contains(volsOut, volumeID) {
		t.Fatalf("expected %s in output %q", volumeID, volsOut)
	}

	// Test snapshot create over unix socket
	snapOut, err := executeCommand("--server", unixAddr, "snapshot", "create", volumeID)
	if err != nil {
		t.Fatalf("snapshot create failed over unix socket: %v", err)
	}
	snapName := strings.TrimSpace(snapOut)
	if snapName == "" || (!strings.HasSuffix(snapName, ".sqlite") && !strings.HasSuffix(snapName, ".snap")) {
		t.Fatalf("expected .sqlite snapshot, got: %q", snapName)
	}

	// Test snapshots list over unix socket
	snapListOut, err := executeCommand("--server", unixAddr, "snapshots", "list", volumeID)
	if err != nil {
		t.Fatalf("snapshots list failed over unix socket: %v", err)
	}
	if !strings.Contains(snapListOut, snapName) {
		t.Fatalf("expected %s in snapshots list output %q", snapName, snapListOut)
	}
}

func TestSnapshotsListInvalidTimeFlags(t *testing.T) {
	_, serverAddr, cleanup := startTestServer(t)
	defer cleanup()

	// Invalid --from-time should fail
	_, err := executeCommand("--server", serverAddr, "snapshots", "list", "any-vol", "--from-time", "not-a-timestamp")
	if err == nil {
		t.Fatalf("expected error with invalid --from-time, got nil")
	}
	if !strings.Contains(err.Error(), "invalid --from-time") {
		t.Errorf("expected 'invalid --from-time' in error, got: %v", err)
	}

	// Invalid --to-time should fail
	_, err = executeCommand("--server", serverAddr, "snapshots", "list", "any-vol", "--to-time", "not-a-timestamp")
	if err == nil {
		t.Fatalf("expected error with invalid --to-time, got nil")
	}
	if !strings.Contains(err.Error(), "invalid --to-time") {
		t.Errorf("expected 'invalid --to-time' in error, got: %v", err)
	}

	// Invalid --to should fail
	_, err = executeCommand("--server", serverAddr, "snapshots", "list", "any-vol", "--to", "not-a-timestamp")
	if err == nil {
		t.Fatalf("expected error with invalid --to, got nil")
	}
	if !strings.Contains(err.Error(), "invalid --to") {
		t.Errorf("expected 'invalid --to' in error, got: %v", err)
	}
}

func TestCheckpointCommand(t *testing.T) {
	server, serverAddr, cleanup := startTestServer(t)
	defer cleanup()

	volumeID := "test-checkpoint-vol"

	// Missing volume argument
	_, err := executeCommand("--server", serverAddr, "checkpoint")
	if err == nil {
		t.Fatalf("expected error for missing volume argument")
	}

	// Subscribe watcher to simulate node daemon receiving EVENT_CHECKPOINT_REQUEST
	watcherCh := server.Broadcaster().Subscribe(volumeID)
	defer server.Broadcaster().Unsubscribe(volumeID, watcherCh)

	// In a goroutine, wait for EVENT_CHECKPOINT_REQUEST and simulate daemon acknowledgement
	go func() {
		select {
		case ev := <-watcherCh:
			if ev.GetEventType() == pb.WatchEventType_EVENT_CHECKPOINT_REQUEST {
				_, _ = server.AckCheckpoint(t.Context(), &pb.AckCheckpointRequest{
					VolumeId:       volumeID,
					RequestId:      ev.GetCheckpointRequestId(),
					StreamPosition: 9876,
				})
			}
		case <-time.After(5 * time.Second):
		}
	}()

	out, err := executeCommand("--server", serverAddr, "checkpoint", volumeID)
	if err != nil {
		t.Fatalf("checkpoint command failed: %v", err)
	}
	expected := fmt.Sprintf("Checkpoint completed for volume %s at stream position 9876", volumeID)
	if !strings.Contains(out, expected) {
		t.Fatalf("expected output %q to contain %q", out, expected)
	}
}

func TestVolumesDescribeCommand(t *testing.T) {
	server, serverAddr, cleanup := startTestServer(t)
	defer cleanup()

	volumeID := "test-desc-vol"

	// 1. Describe volume when no hold exists
	out, err := executeCommand("--server", serverAddr, "volumes", "describe", volumeID)
	if err != nil {
		t.Fatalf("volumes describe failed: %v", err)
	}
	if !strings.Contains(out, "Volume: test-desc-vol") {
		t.Errorf("expected volume name in output, got: %s", out)
	}
	if !strings.Contains(out, "Exclusive Hold: none") {
		t.Errorf("expected 'Exclusive Hold: none' in output, got: %s", out)
	}

	// 2. Acquire hold on server
	_, err = server.AcquireExclusive(t.Context(), &pb.AcquireExclusiveRequest{
		VolumeId: volumeID,
		NodeId:   "node-worker-1",
		MountId:  "/var/lib/kubelet/pods/uuid-1/volumes/mount",
	})
	if err != nil {
		t.Fatalf("AcquireExclusive failed: %v", err)
	}

	// 3. Describe volume with active hold
	out, err = executeCommand("--server", serverAddr, "volumes", "describe", volumeID)
	if err != nil {
		t.Fatalf("volumes describe with hold failed: %v", err)
	}
	if !strings.Contains(out, "Holder Node:  node-worker-1") {
		t.Errorf("expected holder node in output, got: %s", out)
	}
	if !strings.Contains(out, "Mount ID:     /var/lib/kubelet/pods/uuid-1/volumes/mount") {
		t.Errorf("expected mount ID in output, got: %s", out)
	}
	if !strings.Contains(out, "Lease Expiry:") {
		t.Errorf("expected lease expiry in output, got: %s", out)
	}

	// 4. Release hold and verify describe shows none
	_, err = server.ReleaseExclusive(t.Context(), &pb.ReleaseExclusiveRequest{
		VolumeId: volumeID,
		NodeId:   "node-worker-1",
		MountId:  "/var/lib/kubelet/pods/uuid-1/volumes/mount",
	})
	if err != nil {
		t.Fatalf("ReleaseExclusive failed: %v", err)
	}

	out, err = executeCommand("--server", serverAddr, "volumes", "describe", volumeID)
	if err != nil {
		t.Fatalf("volumes describe after release failed: %v", err)
	}
	if !strings.Contains(out, "Exclusive Hold: none") {
		t.Errorf("expected 'Exclusive Hold: none' after release, got: %s", out)
	}
}

type mockRepairStream struct {
	walclient.Stream
	degraded atomic.Pointer[walclient.DegradedError]
}

func (m *mockRepairStream) Degraded() *walclient.DegradedError {
	return m.degraded.Load()
}

func TestVolumesRepairAndDegradedDisplay(t *testing.T) {
	ctx := t.Context()
	backend := controller.NewMemoryBackend()

	volRepair := "vol-repair"
	volHealthy := "vol-healthy"

	walDir := t.TempDir()

	streamRepair, err := walclient.Open(ctx, walDir, controller.StreamIDForVolume(volRepair), "")
	if err != nil {
		t.Fatalf("Open streamRepair failed: %v", err)
	}
	defer streamRepair.Close()

	streamHealthy, err := walclient.Open(ctx, walDir, controller.StreamIDForVolume(volHealthy), "")
	if err != nil {
		t.Fatalf("Open streamHealthy failed: %v", err)
	}
	defer streamHealthy.Close()

	mockRepair := &mockRepairStream{Stream: streamRepair}
	mockRepair.degraded.Store(&walclient.DegradedError{
		Cause: fmt.Errorf("read-only file system"),
		At:    time.Now().Add(-5 * time.Minute),
	})

	var mu sync.Mutex
	repaired := false

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()

	server := controller.NewServer(backend,
		controller.WithServerWAL(walDir, "", walclient.Local),
		controller.WithServerStreamFactory(func(volID string) (walclient.Stream, error) {
			mu.Lock()
			defer mu.Unlock()
			if volID == volRepair {
				if repaired {
					return walclient.Open(ctx, walDir, controller.StreamIDForVolume(volRepair), "")
				}
				return mockRepair, nil
			}
			if volID == volHealthy {
				return streamHealthy, nil
			}
			return nil, fmt.Errorf("unknown volume %s", volID)
		}),
	)
	defer server.Close()

	grpcServer := grpc.NewServer()
	pb.RegisterObjectFSControllerServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(lis) }()
	defer grpcServer.Stop()

	serverAddr := lis.Addr().String()

	// 1. Initial creation of both volumes
	_, err = server.CreateFile(ctx, &pb.CreateFileRequest{
		VolumeId:       volHealthy,
		ParentInode:    1,
		Name:           "h.txt",
		InitialContent: []byte("healthy"),
	})
	if err != nil {
		t.Fatalf("CreateFile volHealthy failed: %v", err)
	}
	// For volRepair, access via GetVolumeStats so it's registered
	_, err = server.GetVolumeStats(ctx, &pb.GetVolumeStatsRequest{VolumeId: volRepair})
	if err != nil {
		t.Fatalf("GetVolumeStats volRepair failed: %v", err)
	}

	// 2. volumes list shows degraded status with reason and since
	out, err := executeCommand("--server", serverAddr, "volumes", "list")
	if err != nil {
		t.Fatalf("volumes list failed: %v", err)
	}
	if !strings.Contains(out, "vol-healthy") {
		t.Errorf("expected vol-healthy in list output, got: %s", out)
	}
	if !strings.Contains(out, "vol-repair\t(degraded: WAL stream degraded: read-only file system, since:") {
		t.Errorf("expected degraded details in volumes list, got: %s", out)
	}

	// 3. volumes describe on degraded volume shows Degraded section
	descOut, err := executeCommand("--server", serverAddr, "volumes", "describe", volRepair)
	if err != nil {
		t.Fatalf("volumes describe volRepair failed: %v", err)
	}
	if !strings.Contains(descOut, "Degraded:") || !strings.Contains(descOut, "read-only file system") {
		t.Errorf("expected Degraded info in volumes describe output, got: %s", descOut)
	}

	// 4. volumes repair on healthy volume without --force is refused
	_, err = executeCommand("--server", serverAddr, "volumes", "repair", volHealthy)
	if err == nil {
		t.Fatalf("expected repair on healthy volume without --force to fail")
	}
	if !strings.Contains(err.Error(), "is not degraded") {
		t.Errorf("expected 'is not degraded' error, got: %v", err)
	}

	// 5. volumes repair on healthy volume with --force succeeds
	forceOut, err := executeCommand("--server", serverAddr, "volumes", "repair", volHealthy, "--force")
	if err != nil {
		t.Fatalf("repair with --force failed: %v", err)
	}
	if !strings.Contains(forceOut, "Repaired volume vol-healthy") {
		t.Errorf("expected Repaired volume vol-healthy in output, got: %s", forceOut)
	}

	// 6. volumes repair on degraded volume succeeds
	mu.Lock()
	repaired = true
	mu.Unlock()

	repairOut, err := executeCommand("--server", serverAddr, "volumes", "repair", volRepair)
	if err != nil {
		t.Fatalf("repair on volRepair failed: %v", err)
	}
	if !strings.Contains(repairOut, "Repaired volume vol-repair") {
		t.Errorf("expected Repaired volume vol-repair in output, got: %s", repairOut)
	}
	if !strings.Contains(repairOut, "Old segment directory:") || !strings.Contains(repairOut, ".degraded") {
		t.Errorf("expected old segment directory with .degraded in output, got: %s", repairOut)
	}
	if !strings.Contains(repairOut, "New watermark:") {
		t.Errorf("expected New watermark in output, got: %s", repairOut)
	}

	// 7. volumes list now shows vol-repair as healthy (no degraded tag)
	afterList, err := executeCommand("--server", serverAddr, "volumes", "list")
	if err != nil {
		t.Fatalf("volumes list after repair failed: %v", err)
	}
	if strings.Contains(afterList, "(degraded:") {
		t.Errorf("expected no degraded volumes after repair, got: %s", afterList)
	}
}
