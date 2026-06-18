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

package e2e

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCASE2E(t *testing.T) {
	if os.Getenv("RUN_E2E") == "" {
		t.Skip("Skipping CAS E2E test; RUN_E2E not set")
	}

	h := NewHarness(t, "cas-e2e")
	h.Setup()

	gitRoot := h.GetGitRoot()
	experimentRoot := gitRoot

	// Build images
	h.DockerBuild("agentfs-controller:e2e", filepath.Join(experimentRoot, "images/agentfs-controller/Dockerfile"), experimentRoot)
	h.DockerBuild("agentfs-node-daemon:e2e", filepath.Join(experimentRoot, "images/agentfs-node-daemon/Dockerfile"), experimentRoot)
	h.DockerBuild("cas-node-daemon:e2e", filepath.Join(experimentRoot, "images/cas-node-daemon/Dockerfile"), experimentRoot)
	h.DockerBuild("cas-client-test:e2e", filepath.Join(experimentRoot, "images/cas-client-test/Dockerfile"), experimentRoot)

	// Load images into Kind
	h.KindLoad("agentfs-controller:e2e")
	h.KindLoad("agentfs-node-daemon:e2e")
	h.KindLoad("cas-node-daemon:e2e")
	h.KindLoad("cas-client-test:e2e")

	// Read manifest and replace placeholders for AgentFS Controller
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

	// Apply AgentFS (contains Controller and Node Daemon)
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

	// Deploy and wait for CAS CSI driver (using /var/cache/cas as the cache storage path)
	t.Logf("Deploying CAS CSI driver")
	casManifest := `
apiVersion: storage.k8s.io/v1
kind: CSIDriver
metadata:
  name: cas.labs.gke.io
spec:
  attachRequired: false
  podInfoOnMount: true
  volumeLifecycleModes:
    - Ephemeral
---
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: cas-node-daemon
  namespace: default
spec:
  selector:
    matchLabels:
      app: cas-node-daemon
  template:
    metadata:
      labels:
        app: cas-node-daemon
    spec:
      hostPID: true
      containers:
        - name: node-driver-registrar
          image: registry.k8s.io/sig-storage/csi-node-driver-registrar:v2.13.0
          args:
            - "--v=5"
            - "--csi-address=$(ADDRESS)"
            - "--kubelet-registration-path=$(DRIVER_REG_SOCK_PATH)"
          env:
            - name: ADDRESS
              value: /csi/csi.sock
            - name: DRIVER_REG_SOCK_PATH
              value: /var/lib/kubelet/plugins/cas.labs.gke.io/csi.sock
          volumeMounts:
            - name: socket-dir
              mountPath: /csi
            - name: registration-dir
              mountPath: /registration
        - name: cas-node-daemon
          securityContext:
            privileged: true
            capabilities:
              add: ["SYS_ADMIN"]
          image: cas-node-daemon:e2e
          imagePullPolicy: Never
          args:
            - "--v=5"
            - "--endpoint=unix:///csi/csi.sock"
            - "--nodeid=$(NODE_ID)"
            - "--storage-path=/var/cache/cas"
            - "--controller-address=agentfs-controller:50051"
          env:
            - name: NODE_ID
              valueFrom:
                fieldRef:
                  fieldPath: spec.nodeName
          volumeMounts:
            - name: socket-dir
              mountPath: /csi
            - name: kubelet-dir
              mountPath: /var/lib/kubelet
              mountPropagation: "Bidirectional"
            - name: storage-dir
              mountPath: /var/cache/cas
              mountPropagation: "Bidirectional"
      volumes:
        - name: socket-dir
          hostPath:
            path: /var/lib/kubelet/plugins/cas.labs.gke.io/
            type: DirectoryOrCreate
        - name: registration-dir
          hostPath:
            path: /var/lib/kubelet/plugins_registry/
            type: Directory
        - name: kubelet-dir
          hostPath:
            path: /var/lib/kubelet
            type: Directory
        - name: storage-dir
          hostPath:
            path: /var/cache/cas
            type: DirectoryOrCreate
`
	h.KubectlApplyContent("cas-driver", casManifest)

	if err := h.WaitForDaemonSet("cas-node-daemon", "default", 2*time.Minute); err != nil {
		t.Logf("Events:\n%s\n", h.GetEvents("default"))
		t.Logf("CAS Node Daemon Logs:\n%s\n", h.GetPodLogs("app=cas-node-daemon", "default"))
		t.Fatalf("CAS Node Daemon failed to start: %v", err)
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

	// CAS CSI Driver E2E Test
	t.Logf("[CAS TEST] Getting snapshot metadata to find SHA256 of 'test.txt'...")
	snapInfo := getLatestSnapshot(t, h, volumeID)
	var testFileSha string
	for _, file := range snapInfo.Files {
		if file.Path == "test.txt" {
			testFileSha = file.Sha256
			break
		}
	}
	if testFileSha == "" {
		t.Fatalf("[CAS TEST] Expected to find 'test.txt' in snapshot metadata")
	}
	t.Logf("[CAS TEST] Found test file SHA256: %s", testFileSha)

	casTestPodName := "cas-test-pod"
	casTestPodYaml := fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: %s
spec:
  restartPolicy: Never
  containers:
    - name: app
      image: cas-client-test:e2e
      imagePullPolicy: Never
      args: ["/data/.in-cluster-storage/api", "%s"]
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      csi:
        driver: cas.labs.gke.io
        volumeAttributes:
          volumeID: %s
`, casTestPodName, testFileSha, volumeID)

	t.Logf("[CAS TEST] Creating CAS test pod %s...", casTestPodName)
	h.KubectlApplyContent(casTestPodName, casTestPodYaml)

	t.Logf("[CAS TEST] Waiting for CAS test pod to reach a terminal status...")
	var finalPhase string
	for i := 0; i < 60; i++ {
		phase := getPodPhase(casTestPodName, "default")
		if phase == "Succeeded" || phase == "Failed" {
			finalPhase = phase
			break
		}
		time.Sleep(1 * time.Second)
	}

	if finalPhase != "Succeeded" {
		t.Logf("CAS Node Daemon Logs:\n%s\n", h.GetPodLogs("app=cas-node-daemon", "default"))
		t.Logf("CAS Test Pod Logs:\n%s\n", h.GetPodLogsByName(casTestPodName, "default"))
		t.Fatalf("[CAS TEST] CAS test pod failed to finish successfully, phase: %s", finalPhase)
	}

	t.Logf("[CAS TEST] CAS test pod finished successfully. Checking logs...")
	casLogs := h.GetPodLogsByName(casTestPodName, "default")
	t.Logf("[CAS TEST] CAS pod logs:\n%s", casLogs)

	if !strings.Contains(casLogs, "SUCCESS") {
		t.Fatalf("[CAS TEST] CAS test client did not print SUCCESS: %s", casLogs)
	}
	if !strings.Contains(casLogs, "updated agentfs") {
		t.Fatalf("[CAS TEST] CAS test client did not successfully retrieve 'updated agentfs'")
	}
	t.Logf("[CAS TEST] Successfully verified Content Addressable Storage via CSI using SCM_RIGHTS!")

	h.DeletePod(casTestPodName, "default")
}

func getPodPhase(podName, namespace string) string {
	cmd := exec.Command("kubectl", "-n", namespace, "get", "pod", podName, "-o", "jsonpath={.status.phase}")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}
