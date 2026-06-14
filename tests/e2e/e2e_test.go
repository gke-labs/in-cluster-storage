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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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
	h.KubectlApplyContent(pod1Name, pod1Yaml)
	if err := h.WaitForPodReady(pod1Name, "default", 1*time.Minute); err != nil {
		t.Logf("Pod YAML:\n%s\n", h.GetPodYaml("app="+pod1Name, "default"))
		t.Logf("Events:\n%s\n", h.GetEvents("default"))
		t.Logf("Node Daemon Logs:\n%s\n", h.GetPodLogs("app=agentfs-node-daemon", "default"))
		t.Fatalf("Test Pod 1 failed to start: %v", err)
	}

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
	h.KubectlApplyContent(pod2Name, pod2Yaml)
	if err := h.WaitForPodReady(pod2Name, "default", 1*time.Minute); err != nil {
		t.Logf("Node Daemon Logs:\n%s\n", h.GetPodLogs("app=agentfs-node-daemon", "default"))
		t.Fatalf("Test Pod 2 failed to start: %v", err)
	}

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
}
