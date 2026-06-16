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

package e2e

import (
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/v1alpha1"
	"google.golang.org/protobuf/proto"
)

func TestE2E(t *testing.T) {
	if os.Getenv("RUN_E2E") == "" {
		t.Skip("Skipping E2E test; RUN_E2E not set")
	}

	h := NewHarness(t, "agentfs-e2e")
	h.Setup()

	gitRoot := h.GetGitRoot()
	experimentRoot := gitRoot

	// Build images
	h.DockerBuild("agentfs-controller:e2e", filepath.Join(experimentRoot, "images/agentfs-controller/Dockerfile"), experimentRoot)
	h.DockerBuild("agentfs-node-daemon:e2e", filepath.Join(experimentRoot, "images/agentfs-node-daemon/Dockerfile"), experimentRoot)

	// Load images into Kind
	h.KindLoad("agentfs-controller:e2e")
	h.KindLoad("agentfs-node-daemon:e2e")

	// Read manifest and replace placeholders
	manifestPath := filepath.Join(experimentRoot, "k8s/manifest.yaml")
	b, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("Failed to read manifest: %v", err)
	}
	manifest := string(b)
	manifest = strings.ReplaceAll(manifest, "namespace: kube-agentfs-system", "namespace: default")
	manifest = strings.ReplaceAll(manifest, "image: agentfs-controller:latest", "image: agentfs-controller:e2e\n          imagePullPolicy: Never")
	manifest = strings.ReplaceAll(manifest, "image: agentfs-node-daemon:latest", "image: agentfs-node-daemon:e2e\n          imagePullPolicy: Never")

	erofsFlag := ""
	if os.Getenv("ENABLE_EROFS") != "false" {
		t.Logf("Enabling EROFS mode for e2e tests (default)!")
		erofsFlag = "\n            - \"--enable-erofs=true\""
	}
	manifest = strings.ReplaceAll(manifest, `"--controller-address=agentfs-controller:50051"`, `"--controller-address=agentfs-controller:50051"`+"\n            - \"--lazy-load-threshold=1\""+erofsFlag)

	// Apply manifests
	h.KubectlApplyContent("agentfs", manifest)

	// Wait for controller
	if err := h.WaitForStatefulSet("agentfs-controller", "default", 2*time.Minute); err != nil {
		t.Logf("Events:\n%s\n", h.GetEvents("default"))
		t.Fatalf("AgentFS Controller failed to start: %v", err)
	}

	// Wait for node-daemon
	if err := h.WaitForDaemonSet("agentfs-node-daemon", "default", 2*time.Minute); err != nil {
		t.Logf("Events:\n%s\n", h.GetEvents("default"))
		t.Fatalf("AgentFS Node Daemon failed to start: %v", err)
	}

	// Step 1: Run a test Pod that writes a file
	pod1Name := "test-pod-1"
	volumeID := "test-volume-123"
	pod1Yaml := fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: %s
spec:
  restartPolicy: Never
  containers:
    - name: app
      image: alpine
      command: ["/bin/sh", "-c", "echo 'hello agentfs' > /data/test.txt && sync && sleep 10"]
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      csi:
        driver: agentfs.labs.gke.io
        volumeAttributes:
          volumeID: %s
`, pod1Name, volumeID)

	t.Logf("Creating Pod 1")
	startPod1 := time.Now()
	h.KubectlApplyContent(pod1Name, pod1Yaml)
	if err := h.WaitForPodReady(pod1Name, "default", 1*time.Minute); err != nil {
		t.Logf("Pod YAML:\n%s\n", h.GetPodYaml("app="+pod1Name, "default"))
		t.Logf("Events:\n%s\n", h.GetEvents("default"))
		t.Logf("Node Daemon Logs:\n%s\n", h.GetPodLogs("app=agentfs-node-daemon", "default"))
		t.Fatalf("Test Pod 1 failed to start: %v", err)
	}
	t.Logf("[PERFORMANCE] Pod 1 startup took %v", time.Since(startPod1))

	t.Logf("Pod 1 is ready, waiting a few seconds")
	time.Sleep(5 * time.Second)

	t.Logf("Deleting Pod 1 to trigger snapshot push")
	h.DeletePod(pod1Name, "default")
	t.Logf("Pod 1 deleted")

	// Step 2: Start a new Pod, read the file, and update it
	pod2Name := "test-pod-2"
	pod2Yaml := fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: %s
spec:
  restartPolicy: Never
  containers:
    - name: app
      image: alpine
      command: ["/bin/sh", "-c", "cat /data/test.txt && echo 'updated agentfs' > /data/test.txt && sync && sleep 10"]
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      csi:
        driver: agentfs.labs.gke.io
        volumeAttributes:
          volumeID: %s
`, pod2Name, volumeID)

	t.Logf("Creating Pod 2")
	startPod2 := time.Now()
	h.KubectlApplyContent(pod2Name, pod2Yaml)
	if err := h.WaitForPodReady(pod2Name, "default", 1*time.Minute); err != nil {
		t.Logf("Node Daemon Logs:\n%s\n", h.GetPodLogs("app=agentfs-node-daemon", "default"))
		t.Fatalf("Test Pod 2 failed to start: %v", err)
	}
	t.Logf("[PERFORMANCE] Pod 2 startup took %v", time.Since(startPod2))

	t.Logf("Pod 2 is ready, verifying content")
	time.Sleep(5 * time.Second)
	logs2 := h.GetPodLogsByName(pod2Name, "default")
	if !strings.Contains(logs2, "hello agentfs") {
		t.Logf("Pod 2 Logs:\n%s\n", logs2)
		t.Logf("Node Daemon Logs:\n%s\n", h.GetPodLogs("app=agentfs-node-daemon", "default"))
		t.Fatalf("Pod 2 did not see 'hello agentfs' in its logs")
	}
	t.Logf("Pod 2 verified, deleting")

	h.DeletePod(pod2Name, "default")
	t.Logf("Pod 2 deleted")

	// Step 3: Launch another Pod to read the file and verify the value again
	pod3Name := "test-pod-3"
	pod3Yaml := fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: %s
spec:
  containers:
    - name: app
      image: alpine
      command: ["/bin/sh", "-c", "cat /data/test.txt && sleep 3600"]
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      csi:
        driver: agentfs.labs.gke.io
        volumeAttributes:
          volumeID: %s
`, pod3Name, volumeID)

	h.KubectlApplyContent(pod3Name, pod3Yaml)
	if err := h.WaitForPodReady(pod3Name, "default", 1*time.Minute); err != nil {
		t.Fatalf("Test Pod 3 failed to start: %v", err)
	}

	out, err := h.RunInPod(pod3Name, "default", "cat", "/data/test.txt")
	if err != nil {
		t.Fatalf("Failed to read file in Pod 3: %v\nOutput: %s", err, out)
	}

	expected := "updated agentfs"
	if !strings.Contains(out, expected) {
		t.Fatalf("Expected content %q, got %q", expected, out)
	}

	h.DeletePod(pod3Name, "default")

	// Step 4: Verify Incremental Snapshot & Deletion/Whiteout behavior
	incVolumeID := "test-incremental-vol"
	pod4Name := "test-pod-4"
	pod4Yaml := fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: %s
spec:
  restartPolicy: Never
  containers:
    - name: app
      image: alpine
      command: ["/bin/sh", "-c", "echo 'first file' > /data/file1.txt && echo 'second file' > /data/file2.txt && sync && sleep 10"]
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      csi:
        driver: agentfs.labs.gke.io
        volumeAttributes:
          volumeID: %s
`, pod4Name, incVolumeID)

	t.Logf("Creating Pod 4 (Incremental initial state)")
	startPod4 := time.Now()
	h.KubectlApplyContent(pod4Name, pod4Yaml)
	if err := h.WaitForPodReady(pod4Name, "default", 1*time.Minute); err != nil {
		t.Fatalf("Test Pod 4 failed to start: %v", err)
	}
	t.Logf("[PERFORMANCE] Pod 4 startup took %v", time.Since(startPod4))

	time.Sleep(5 * time.Second)
	t.Logf("Deleting Pod 4 to trigger snapshot push")
	h.DeletePod(pod4Name, "default")
	t.Logf("Pod 4 deleted")

	// Verify snapshot has file1.txt and file2.txt
	snap1 := getLatestSnapshot(t, h, incVolumeID)
	t.Logf("Snapshot 1: %v", snap1)
	if len(snap1.Files) != 2 {
		t.Fatalf("Expected 2 files in initial snapshot, got %d", len(snap1.Files))
	}

	hasFile1 := false
	hasFile2 := false
	var file2Metadata *pb.FileMetadata

	for _, file := range snap1.Files {
		if file.Path == "file1.txt" {
			hasFile1 = true
		}
		if file.Path == "file2.txt" {
			hasFile2 = true
			file2Metadata = file
		}
	}
	if !hasFile1 || !hasFile2 {
		t.Fatalf("Snapshot 1 is missing file1.txt or file2.txt")
	}

	// Now start Pod 5:
	// - Reads both file1.txt and file2.txt to verify they exist.
	// - Deletes file1.txt.
	// - Creates file3.txt.
	pod5Name := "test-pod-5"
	pod5Yaml := fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: %s
spec:
  restartPolicy: Never
  containers:
    - name: app
      image: alpine
      command: ["/bin/sh", "-c", "cat /data/file1.txt && cat /data/file2.txt && rm /data/file1.txt && echo 'third file' > /data/file3.txt && sync && sleep 10"]
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      csi:
        driver: agentfs.labs.gke.io
        volumeAttributes:
          volumeID: %s
`, pod5Name, incVolumeID)

	t.Logf("Creating Pod 5 (modifying volume)")
	startPod5 := time.Now()
	h.KubectlApplyContent(pod5Name, pod5Yaml)
	if err := h.WaitForPodReady(pod5Name, "default", 1*time.Minute); err != nil {
		t.Fatalf("Test Pod 5 failed to start: %v", err)
	}
	t.Logf("[PERFORMANCE] Pod 5 startup took %v", time.Since(startPod5))

	time.Sleep(5 * time.Second)
	logs5 := h.GetPodLogsByName(pod5Name, "default")
	if !strings.Contains(logs5, "first file") || !strings.Contains(logs5, "second file") {
		t.Fatalf("Pod 5 did not read file1.txt or file2.txt successfully. Logs:\n%s", logs5)
	}

	t.Logf("Deleting Pod 5 to trigger second snapshot push")
	h.DeletePod(pod5Name, "default")
	t.Logf("Pod 5 deleted")

	// Verify second snapshot
	snap2 := getLatestSnapshot(t, h, incVolumeID)
	t.Logf("Snapshot 2: %v", snap2)

	// It should have exactly file2.txt and file3.txt, but NOT file1.txt (which was deleted)
	hasFile1_2 := false
	hasFile2_2 := false
	hasFile3_2 := false
	var file2Metadata_2 *pb.FileMetadata

	for _, file := range snap2.Files {
		if file.Path == "file1.txt" {
			hasFile1_2 = true
		}
		if file.Path == "file2.txt" {
			hasFile2_2 = true
			file2Metadata_2 = file
		}
		if file.Path == "file3.txt" {
			hasFile3_2 = true
		}
	}

	if hasFile1_2 {
		t.Fatalf("Deleted file 'file1.txt' still exists in the second snapshot!")
	}
	if !hasFile2_2 {
		t.Fatalf("Expected file 'file2.txt' to exist in the second snapshot")
	}
	if !hasFile3_2 {
		t.Fatalf("Expected new file 'file3.txt' to exist in the second snapshot")
	}

	// Verify that the snapshot was incremental:
	// file2.txt was not modified, so its metadata (like Sha256 and ModTime) should be identical!
	if file2Metadata_2.Sha256 != file2Metadata.Sha256 {
		t.Fatalf("file2.txt Sha256 mismatch: expected %q, got %q (snapshot was not incremental or file was corrupted)", file2Metadata.Sha256, file2Metadata_2.Sha256)
	}
	if !file2Metadata_2.ModTime.AsTime().Equal(file2Metadata.ModTime.AsTime()) {
		t.Fatalf("file2.txt ModTime changed: expected %v, got %v", file2Metadata.ModTime.AsTime(), file2Metadata_2.ModTime.AsTime())
	}

	t.Logf("Successfully verified incremental and whiteout snapshotting behavior!")
}

func getLatestSnapshot(t *testing.T, h *Harness, volumeID string) *pb.SnapshotMetadata {
	out, err := h.RunInPod("agentfs-controller-0", "default", "base64", filepath.Join("/data/snapshots", volumeID, "latest.pb"))
	if err != nil {
		t.Fatalf("Failed to get snapshot from controller: %v", err)
	}
	cleanOut := strings.Join(strings.Fields(out), "")
	data, err := base64.StdEncoding.DecodeString(cleanOut)
	if err != nil {
		t.Fatalf("Failed to base64 decode snapshot output: %v\nRaw output:\n%s", err, out)
	}
	snapshot := &pb.SnapshotMetadata{}
	if err := proto.Unmarshal(data, snapshot); err != nil {
		t.Fatalf("Failed to unmarshal snapshot: %v", err)
	}
	return snapshot
}
