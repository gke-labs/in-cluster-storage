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
	"context"
	"encoding/base64"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type outageTracker struct {
	mu                sync.RWMutex
	inOutage          bool
	outageStart       time.Time
	outageEnd         time.Time
	recoveryWindowEnd time.Time
	eioDuringOutage   int
	eioOutsideOutage  int
	unexpectedErrors  []string
}

func (ot *outageTracker) StartOutage() {
	ot.mu.Lock()
	defer ot.mu.Unlock()
	ot.inOutage = true
	ot.outageStart = time.Now()
}

func (ot *outageTracker) EndOutage(recoveryBound time.Duration) {
	ot.mu.Lock()
	defer ot.mu.Unlock()
	ot.inOutage = false
	ot.outageEnd = time.Now()
	ot.recoveryWindowEnd = ot.outageEnd.Add(recoveryBound)
}

func (ot *outageTracker) IsInOutageWindow() bool {
	ot.mu.RLock()
	defer ot.mu.RUnlock()
	now := time.Now()
	if ot.inOutage {
		return true
	}
	if !ot.outageEnd.IsZero() && now.Before(ot.recoveryWindowEnd) {
		return true
	}
	return false
}

func (ot *outageTracker) RecordResult(stepName, out string, err error) {
	if err == nil {
		return
	}
	isEIO := strings.Contains(err.Error(), "Input/output error") ||
		strings.Contains(out, "Input/output error") ||
		strings.Contains(err.Error(), "exit status 5") ||
		strings.Contains(out, "EIO")

	ot.mu.Lock()
	defer ot.mu.Unlock()
	if isEIO {
		now := time.Now()
		inWindow := ot.inOutage || (!ot.outageEnd.IsZero() && now.Before(ot.recoveryWindowEnd))
		if inWindow {
			ot.eioDuringOutage++
		} else {
			ot.eioOutsideOutage++
			ot.unexpectedErrors = append(ot.unexpectedErrors, fmt.Sprintf("%s at %s: %v (out: %s)", stepName, now.Format(time.RFC3339), err, strings.TrimSpace(out)))
		}
	}
}

func runInPodWithDeadline(ctx context.Context, h *Harness, pod, stepName string, timeout time.Duration, cmd ...string) (string, error) {
	stepCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	args := append([]string{"exec", pod, "-n", "default", "--"}, cmd...)
	c := exec.CommandContext(stepCtx, "kubectl", args...)
	out, err := c.CombinedOutput()
	outStr := string(out)
	if stepCtx.Err() == context.DeadlineExceeded {
		return outStr, fmt.Errorf("HANG_DETECTED: step %q timed out after %v", stepName, timeout)
	}
	return outStr, err
}

func getPodUID(podName, namespace string) string {
	out, err := exec.Command("kubectl", "get", "pod", podName, "-n", namespace, "-o", "jsonpath={.metadata.uid}").Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func waitForNewPodReady(t *testing.T, h *Harness, podName, namespace, oldUID string, timeout time.Duration) error {
	t.Helper()
	t.Logf("Waiting up to %v for pod %s to be replaced (new UID != %q) and ready in namespace %s", timeout, podName, oldUID, namespace)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		uid := getPodUID(podName, namespace)
		if uid != "" && uid != oldUID {
			out, err := exec.Command("kubectl", "get", "pod", podName, "-n", namespace, "-o", "jsonpath={.status.conditions[?(@.type==\"Ready\")].status}").Output()
			if err == nil && strings.TrimSpace(string(out)) == "True" {
				t.Logf("Pod %s successfully replaced and ready (UID: %s)", podName, uid)
				return nil
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting %v for pod %s to be replaced and ready (old UID: %s)", timeout, podName, oldUID)
}

func dumpAllDiagnosticLogs(t *testing.T, h *Harness, pod1, pod2 string) {
	t.Log("======= DIAGNOSTIC LOG DUMP =======")
	t.Logf("ObjectFS Controller Logs:\n%s", h.GetPodLogsByName("objectfs-controller-0", "default"))
	t.Logf("WAL Buffer Logs:\n%s", h.GetPodLogsByName("wal-buffer-0", "default"))
	t.Logf("Node Daemon Logs:\n%s", h.GetPodLogs("app=objectfs-node-daemon", "default"))
	if pod1 != "" {
		out, _ := h.RunInPod(pod1, "default", "/bin/sh", "-c", "tail -n 200 /tmp/fsx_*.log 2>/dev/null || true")
		t.Logf("Pod 1 (%s) fsx Logs:\n%s", pod1, out)
	}
	if pod2 != "" {
		out, _ := h.RunInPod(pod2, "default", "/bin/sh", "-c", "tail -n 200 /tmp/fsx_*.log 2>/dev/null || true")
		t.Logf("Pod 2 (%s) fsx Logs:\n%s", pod2, out)
	}
	t.Log("===================================")
}

func copyFsxToPod(t *testing.T, h *Harness, podName string) {
	t.Helper()
	localFsx := filepath.Join(h.GetGitRoot(), ".build/bin/fsx")
	if _, err := os.Stat(localFsx); os.IsNotExist(err) {
		cmd := exec.Command(filepath.Join(h.GetGitRoot(), "dev/tasks/build-fsx"))
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("failed to build fsx: %v (out: %s)", err, out)
		}
	}

	_, _ = h.RunInPod(podName, "default", "/bin/sh", "-c", "mkdir -p /usr/local/bin")

	// Attempt fast transfer via kubectl cp
	cmd := exec.Command("kubectl", "cp", localFsx, fmt.Sprintf("%s:/usr/local/bin/fsx", podName), "-n", "default")
	out, err := cmd.CombinedOutput()
	if err == nil {
		_, _ = h.RunInPod(podName, "default", "chmod", "+x", "/usr/local/bin/fsx")
		return
	}

	t.Logf("kubectl cp failed (%v, out: %s); falling back to base64 transfer", err, out)
	data, err := os.ReadFile(localFsx)
	if err != nil {
		t.Fatalf("failed to read local fsx: %v", err)
	}
	encoded := base64.StdEncoding.EncodeToString(data)
	const chunkSize = 32 * 1024
	_, _ = h.RunInPod(podName, "default", "/bin/sh", "-c", "rm -f /tmp/fsx.b64 /usr/local/bin/fsx")
	for i := 0; i < len(encoded); i += chunkSize {
		end := i + chunkSize
		if end > len(encoded) {
			end = len(encoded)
		}
		chunk := encoded[i:end]
		_, err := h.RunInPod(podName, "default", "/bin/sh", "-c", fmt.Sprintf("printf '%%s' '%s' >> /tmp/fsx.b64", chunk))
		if err != nil {
			t.Fatalf("failed to write base64 chunk to pod %s: %v", podName, err)
		}
	}
	_, err = h.RunInPod(podName, "default", "/bin/sh", "-c", "base64 -d /tmp/fsx.b64 > /usr/local/bin/fsx && chmod +x /usr/local/bin/fsx && rm -f /tmp/fsx.b64")
	if err != nil {
		t.Fatalf("failed to decode fsx in pod %s: %v", podName, err)
	}
}

func setupObjectFSTortureCluster(t *testing.T, clusterName string) (*Harness, string, string) {
	t.Helper()
	h := NewHarness(t, clusterName)
	h.SetupMultiNode(2)
	t.Cleanup(func() {
		if t.Failed() {
			h.DumpDiagnosticLogs(t)
		}
	})

	nodes := h.GetWorkerNodeNames()
	if len(nodes) < 2 {
		nodes = h.GetNodeNames()
	}
	if len(nodes) < 2 {
		t.Fatalf("Expected at least 2 nodes in kind cluster, found %d: %v", len(nodes), nodes)
	}
	node1 := nodes[0]
	node2 := nodes[1]

	experimentRoot := h.GetGitRoot()

	// Ensure fsx binary is built on host
	fsxPath := filepath.Join(experimentRoot, ".build/bin/fsx")
	if _, err := os.Stat(fsxPath); os.IsNotExist(err) {
		t.Logf("Building fsx binary via dev/tasks/build-fsx...")
		cmd := exec.Command(filepath.Join(experimentRoot, "dev/tasks/build-fsx"))
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("dev/tasks/build-fsx failed: %v\nOutput: %s", err, string(out))
		}
	}

	// Build and load docker images
	h.DockerBuild("objectfs-controller:e2e", filepath.Join(experimentRoot, "images/objectfs-controller/Dockerfile"), experimentRoot)
	h.DockerBuild("objectfs-node-daemon:e2e", filepath.Join(experimentRoot, "images/objectfs-node-daemon/Dockerfile"), experimentRoot)
	h.DockerBuild("wal-buffer:e2e", filepath.Join(experimentRoot, "images/wal-buffer/Dockerfile"), experimentRoot)
	h.DockerBuild("fakes3:e2e", filepath.Join(experimentRoot, "fakes3/images/fakes3/Dockerfile"), filepath.Join(experimentRoot, "fakes3"))

	h.KindLoad("objectfs-controller:e2e")
	h.KindLoad("objectfs-node-daemon:e2e")
	h.KindLoad("wal-buffer:e2e")
	h.KindLoad("fakes3:e2e")

	// Deploy FakeS3
	fakes3Manifest := `
apiVersion: v1
kind: Service
metadata:
  name: fakes3
  namespace: default
spec:
  ports:
    - port: 9000
      targetPort: 9000
  selector:
    app: fakes3
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: fakes3
  namespace: default
spec:
  replicas: 1
  selector:
    matchLabels:
      app: fakes3
  template:
    metadata:
      labels:
        app: fakes3
    spec:
      containers:
        - name: fakes3
          image: fakes3:e2e
          imagePullPolicy: Never
          args: ["--listen=0.0.0.0:9000", "--buckets=objectfs-bucket", "--quiet"]
          ports:
            - containerPort: 9000
`
	t.Logf("Deploying FakeS3 backing object storage")
	h.KubectlApplyContent("fakes3", fakes3Manifest)
	if err := h.WaitForDeployment("fakes3", "default", 2*time.Minute); err != nil {
		t.Fatalf("FakeS3 deployment failed to start: %v", err)
	}

	s3BackendURL := "s3://objectfs-bucket?endpoint=http://fakes3:9000&region=us-east-1&s3ForcePathStyle=true"

	// ObjectFS manifest with S3 backend, --debug-mutation-check on controller and --debug on node-daemon
	objectfsManifestPath := filepath.Join(experimentRoot, "k8s/objectfs.yaml")
	b, err := os.ReadFile(objectfsManifestPath)
	if err != nil {
		t.Fatalf("Failed to read objectfs manifest: %v", err)
	}
	objectfsManifest := string(b)
	objectfsManifest = strings.ReplaceAll(objectfsManifest, "namespace: kube-objectfs-system", "namespace: default")
	objectfsManifest = strings.ReplaceAll(objectfsManifest, "image: objectfs-controller:latest", "image: objectfs-controller:e2e\n          imagePullPolicy: Never")
	objectfsManifest = strings.ReplaceAll(objectfsManifest, "image: objectfs-node-daemon:latest", "image: objectfs-node-daemon:e2e\n          imagePullPolicy: Never")
	objectfsManifest = strings.ReplaceAll(objectfsManifest, `backend: ""`, fmt.Sprintf("backend: %q", s3BackendURL))

	// Enable --debug-mutation-check on controller
	objectfsManifest = strings.ReplaceAll(objectfsManifest, `"--wal-durability=witness"`, `"--wal-durability=witness"`+"\n            - \"--debug-mutation-check\"")

	// Enable --debug on node daemon (mounts --debug)
	objectfsManifest = strings.ReplaceAll(objectfsManifest, `"--cache-dir=/var/cache/objectfs"`, `"--cache-dir=/var/cache/objectfs"`+"\n            - \"--debug\"")

	awsEnvController := `env:
            - name: AWS_ACCESS_KEY_ID
              value: "fakes3"
            - name: AWS_SECRET_ACCESS_KEY
              value: "fakes3"
            - name: AWS_REGION
              value: "us-east-1"
            - name: OBJECT_STORAGE_BACKEND`
	objectfsManifest = strings.ReplaceAll(objectfsManifest, "env:\n            - name: OBJECT_STORAGE_BACKEND", awsEnvController)

	// Pin controller to node1 initially and set 5s termination grace period for fast pod recycling
	nodeSelectorNode1 := fmt.Sprintf("nodeSelector:\n        kubernetes.io/hostname: %s\n      terminationGracePeriodSeconds: 5\n      tolerations:\n        - key: \"node-role.kubernetes.io/control-plane\"\n          operator: \"Exists\"\n          effect: \"NoSchedule\"\n      serviceAccountName: objectfs-controller", node1)
	objectfsManifest = strings.ReplaceAll(objectfsManifest, "serviceAccountName: objectfs-controller", nodeSelectorNode1)

	// WAL buffer manifest
	walManifestPath := filepath.Join(experimentRoot, "k8s/wal.yaml")
	b, err = os.ReadFile(walManifestPath)
	if err != nil {
		t.Fatalf("Failed to read wal manifest: %v", err)
	}
	walManifest := string(b)
	walManifest = strings.ReplaceAll(walManifest, "namespace: kube-objectfs-system", "namespace: default")
	walManifest = strings.ReplaceAll(walManifest, "image: wal-buffer:latest", "image: wal-buffer:e2e\n          imagePullPolicy: Never")
	walNodeSelectorNode1 := fmt.Sprintf("nodeSelector:\n        kubernetes.io/hostname: %s\n      terminationGracePeriodSeconds: 5\n      tolerations:\n        - key: \"node-role.kubernetes.io/control-plane\"\n          operator: \"Exists\"\n          effect: \"NoSchedule\"\n      serviceAccountName: wal-buffer", node1)
	walManifest = strings.ReplaceAll(walManifest, "serviceAccountName: wal-buffer", walNodeSelectorNode1)

	awsEnvWAL := `env:
            - name: AWS_ACCESS_KEY_ID
              value: "fakes3"
            - name: AWS_SECRET_ACCESS_KEY
              value: "fakes3"
            - name: AWS_REGION
              value: "us-east-1"
            - name: OBJECT_STORAGE_BACKEND`
	walManifest = strings.ReplaceAll(walManifest, "env:\n            - name: OBJECT_STORAGE_BACKEND", awsEnvWAL)

	t.Logf("Deploying ObjectFS (on %s) and WAL Buffer with --debug-mutation-check and mounts --debug", node1)
	h.KubectlApplyContent("objectfs", objectfsManifest)
	h.KubectlApplyContent("wal-buffer", walManifest)

	if err := h.WaitForStatefulSet("wal-buffer", "default", 2*time.Minute); err != nil {
		t.Fatalf("WAL Buffer failed to start: %v", err)
	}
	if err := h.WaitForStatefulSet("objectfs-controller", "default", 2*time.Minute); err != nil {
		t.Fatalf("ObjectFS Controller failed to start on %s: %v", node1, err)
	}
	if err := h.WaitForDaemonSet("objectfs-node-daemon", "default", 2*time.Minute); err != nil {
		t.Fatalf("ObjectFS Node Daemon failed to start: %v", err)
	}

	return h, node1, node2
}

func TestObjectFSTortureE2E(t *testing.T) {
	// Gated by OBJECTFS_TORTURE environment variable so normal e2e skips it
	if os.Getenv("OBJECTFS_TORTURE") == "" {
		t.Skip("Skipping ObjectFS torture E2E test; OBJECTFS_TORTURE not set")
	}

	duration := 10 * time.Minute
	if dStr := os.Getenv("OBJECTFS_TORTURE_DURATION"); dStr != "" {
		if d, err := time.ParseDuration(dStr); err == nil && d > 0 {
			duration = d
		}
	}

	kPerNode := 4
	if kStr := os.Getenv("OBJECTFS_TORTURE_K"); kStr != "" {
		if k, err := strconv.Atoi(kStr); err == nil && k > 0 {
			kPerNode = k
		}
	}

	masterSeed := time.Now().UnixNano()
	if sStr := os.Getenv("OBJECTFS_TORTURE_SEED"); sStr != "" {
		if s, err := strconv.ParseInt(sStr, 10, 64); err == nil {
			masterSeed = s
		}
	}

	t.Logf("================================================================================")
	t.Logf("ObjectFS Torture Test: duration=%v, K=%d/node, masterSeed=%d", duration, kPerNode, masterSeed)
	t.Logf("Disruptions active: controller kill (~90s), wal-buffer kill (~120s), outage, snapshot")
	t.Logf("Deferred disruptions: node daemon restart pending #132 (FUSE fd handoff)")
	t.Logf("================================================================================")

	h, node1, node2 := setupObjectFSTortureCluster(t, "objectfs-torture-e2e")

	volumeID := "torture-shared-vol"
	pod1Name := "torture-worker-node1"
	pod2Name := "torture-worker-node2"

	t.Cleanup(func() {
		if t.Failed() {
			dumpAllDiagnosticLogs(t, h, pod1Name, pod2Name)
		}
	})

	pod1Yaml := fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: %s
spec:
  nodeSelector:
    kubernetes.io/hostname: %s
  containers:
    - name: app
      image: alpine
      command: ["/bin/sh", "-c", "sleep 7200"]
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      csi:
        driver: objectfs.labs.gke.io
        volumeAttributes:
          volumeID: %s
`, pod1Name, node1, volumeID)

	pod2Yaml := fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: %s
spec:
  nodeSelector:
    kubernetes.io/hostname: %s
  containers:
    - name: app
      image: alpine
      command: ["/bin/sh", "-c", "sleep 7200"]
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      csi:
        driver: objectfs.labs.gke.io
        volumeAttributes:
          volumeID: %s
`, pod2Name, node2, volumeID)

	t.Logf("Deploying torture worker Pod 1 on %s and Pod 2 on %s", node1, node2)
	h.KubectlApplyContent(pod1Name, pod1Yaml)
	h.KubectlApplyContent(pod2Name, pod2Yaml)

	if err := h.WaitForPodReady(pod1Name, "default", 2*time.Minute); err != nil {
		t.Fatalf("Pod 1 failed to start: %v", err)
	}
	if err := h.WaitForPodReady(pod2Name, "default", 2*time.Minute); err != nil {
		t.Fatalf("Pod 2 failed to start: %v", err)
	}

	// Copy statically built fsx binary to both pods
	t.Logf("Installing fsx binary to worker pods...")
	copyFsxToPod(t, h, pod1Name)
	copyFsxToPod(t, h, pod2Name)

	// Warm both mounts
	t.Logf("Warming mounts and establishing WatchVolume streams")
	_, err := h.RunInPod(pod1Name, "default", "/bin/sh", "-c", "echo 'warm-pod1' > /data/warm1.txt && sync /data/warm1.txt && cat /data/warm1.txt")
	if err != nil {
		t.Fatalf("Warm write on Pod 1 failed: %v", err)
	}
	_, err = h.RunInPod(pod2Name, "default", "/bin/sh", "-c", "echo 'warm-pod2' > /data/warm2.txt && sync /data/warm2.txt && cat /data/warm2.txt")
	if err != nil {
		t.Fatalf("Warm write on Pod 2 failed: %v", err)
	}

	testCtx, testCancel := context.WithCancel(t.Context())
	defer testCancel()

	var fatalErrMu sync.Mutex
	var fatalErr error
	recordFatal := func(err error) {
		fatalErrMu.Lock()
		defer fatalErrMu.Unlock()
		if fatalErr == nil {
			fatalErr = err
			testCancel()
		}
	}

	tracker := &outageTracker{}

	const stepDeadline = 60 * time.Second

	// Watchdog goroutine (Invariant 1: Nothing hangs)
	watchdogDone := make(chan struct{})
	defer close(watchdogDone)
	go func() {
		select {
		case <-watchdogDone:
			return
		case <-time.After(duration + 10*time.Minute):
			t.Errorf("WATCHDOG TIMEOUT: Total torture test duration exceeded")
			dumpAllDiagnosticLogs(t, h, pod1Name, pod2Name)
		}
	}()

	// -------------------------------------------------------------------------
	// Workload 1: fsx x K per node (all concurrent, mapped writes on)
	// -------------------------------------------------------------------------
	rng := rand.New(rand.NewSource(masterSeed))
	type fsxInstance struct {
		pod      string
		instance int
		seed     int64
		fileName string
		logFile  string
		pidFile  string
	}

	var fsxList []fsxInstance
	for i := 0; i < kPerNode; i++ {
		seed1 := rng.Int63()
		fn1 := fmt.Sprintf("/data/fsx_n1_%d.bin", i)
		log1 := fmt.Sprintf("/tmp/fsx_n1_%d.log", i)
		pid1 := fmt.Sprintf("/tmp/fsx_n1_%d.pid", i)
		fsxList = append(fsxList, fsxInstance{pod1Name, i, seed1, fn1, log1, pid1})
		t.Logf("fsx Node 1 instance %d: seed=%d file=%s", i, seed1, fn1)

		seed2 := rng.Int63()
		fn2 := fmt.Sprintf("/data/fsx_n2_%d.bin", i)
		log2 := fmt.Sprintf("/tmp/fsx_n2_%d.log", i)
		pid2 := fmt.Sprintf("/tmp/fsx_n2_%d.pid", i)
		fsxList = append(fsxList, fsxInstance{pod2Name, i, seed2, fn2, log2, pid2})
		t.Logf("fsx Node 2 instance %d: seed=%d file=%s", i, seed2, fn2)
	}

	for _, fi := range fsxList {
		startCmd := fmt.Sprintf(
			"rm -f %s %s %s %s.fsxops %s.fsxgood; (/usr/local/bin/fsx -N 1000000 -S %d -l 16777216 -o 65536 -p 200 -C -I -0 -B -J -E %s > %s 2>&1 & echo $! > %s)",
			fi.fileName, fi.logFile, fi.pidFile, fi.fileName, fi.fileName,
			fi.seed, fi.fileName, fi.logFile, fi.pidFile,
		)
		_, err := runInPodWithDeadline(testCtx, h, fi.pod, fmt.Sprintf("start_fsx_%s_%d", fi.pod, fi.instance), stepDeadline, "/bin/sh", "-c", startCmd)
		if err != nil {
			t.Fatalf("Failed to launch fsx instance %d on %s: %v", fi.instance, fi.pod, err)
		}
	}

	// -------------------------------------------------------------------------
	// Workload 2: Metadata churn on Node 1 (untar-plus-rename-plus-delete)
	//
	// Cross-node content checks are made only on fsynced files (file1.bin and
	// file2_renamed.bin). Data closed without fsync becomes visible on other
	// nodes asynchronously, once the sync-on-close pack is uploaded and metadata
	// is durable; the 30s background interval is the fallback, not the bound.
	// Expected hashes are recorded only from files that were fsynced.
	// -------------------------------------------------------------------------
	type churnState struct {
		step    int
		entries map[string]string // relPath -> sha256 or "dir"
	}

	var churnMu sync.RWMutex
	var latestChurn *churnState

	if _, err := h.RunInPod(pod1Name, "default", "/bin/sh", "-c", "rm -rf /data/churn && mkdir -p /data/churn"); err != nil {
		t.Fatalf("Failed to initialize /data/churn on %s: %v", pod1Name, err)
	}

	churnStop := make(chan struct{})
	var churnWg sync.WaitGroup
	churnWg.Add(1)
	go func() {
		defer churnWg.Done()
		step := 0
		for {
			select {
			case <-churnStop:
				return
			default:
			}
			step++
			stepName := fmt.Sprintf("churn_step_%d", step)
			churnCmd := fmt.Sprintf(`
set -e
mkdir -p /data/churn/dir_%d/sub
dd if=/dev/urandom of=/data/churn/dir_%d/file1.bin bs=1k count=4 conv=fsync 2>/dev/null
dd if=/dev/urandom of=/data/churn/dir_%d/sub/file2.bin bs=2k count=2 conv=fsync 2>/dev/null
ln -s file1.bin /data/churn/dir_%d/sym1 2>/dev/null || true
mv /data/churn/dir_%d/sub/file2.bin /data/churn/dir_%d/sub/file2_renamed.bin
sync /data/churn/dir_%d/sub/file2_renamed.bin
if [ %d -gt 3 ]; then
  old=%d
  rm -rf /data/churn/dir_$old
fi
sync /data/churn
`, step, step, step, step, step, step, step, step, step-3)

			out, err := runInPodWithDeadline(testCtx, h, pod1Name, stepName, stepDeadline, "/bin/sh", "-c", churnCmd)
			tracker.RecordResult(stepName, out, err)
			if err != nil {
				if strings.Contains(err.Error(), "HANG_DETECTED") {
					dumpAllDiagnosticLogs(t, h, pod1Name, pod2Name)
					recordFatal(fmt.Errorf("Invariant 1 violated: %v", err))
					return
				}
				time.Sleep(500 * time.Millisecond)
				continue
			}

			// Record tree state only after sync completes successfully for fsynced files
			listOut, listErr := runInPodWithDeadline(testCtx, h, pod1Name, "churn_stat_"+stepName, stepDeadline, "/bin/sh", "-c", "cd /data/churn && for f in $(find . -type f | sort); do echo \"$(sha256sum \"$f\" | awk '{print $1}') $f\"; done")
			if listErr == nil {
				entries := make(map[string]string)
				for _, line := range strings.Split(strings.TrimSpace(listOut), "\n") {
					parts := strings.Fields(line)
					if len(parts) >= 2 {
						entries[parts[1]] = parts[0]
					}
				}
				churnMu.Lock()
				latestChurn = &churnState{step: step, entries: entries}
				churnMu.Unlock()
			}
			time.Sleep(200 * time.Millisecond)
		}
	}()

	// -------------------------------------------------------------------------
	// Workload 3: Checkpoint writer on Node 1 & Cross-node readers on Node 2
	// -------------------------------------------------------------------------
	type durabilityEntry struct {
		path string
		hash string
	}

	var manifestMu sync.RWMutex
	manifest := make(map[string]string)

	if _, err := h.RunInPod(pod1Name, "default", "/bin/sh", "-c", "rm -rf /data/ckpt && mkdir -p /data/ckpt"); err != nil {
		t.Fatalf("Failed to initialize /data/ckpt on %s: %v", pod1Name, err)
	}

	writerStop := make(chan struct{})
	var writerWg sync.WaitGroup
	writerWg.Add(1)
	go func() {
		defer writerWg.Done()
		ckptNum := 0
		for {
			select {
			case <-writerStop:
				return
			default:
			}
			ckptNum++
			relPath := fmt.Sprintf("ckpt/f_%d.bin", ckptNum)
			fullPath := fmt.Sprintf("/data/%s", relPath)
			stepName := fmt.Sprintf("writer_ckpt_%d", ckptNum)
			writeCmd := fmt.Sprintf(`
set -e
dd if=/dev/urandom of=%s bs=4k count=4 conv=fsync 2>/dev/null
sha256sum %s | awk '{print $1}'
`, fullPath, fullPath)

			out, err := runInPodWithDeadline(testCtx, h, pod1Name, stepName, stepDeadline, "/bin/sh", "-c", writeCmd)
			tracker.RecordResult(stepName, out, err)
			if err != nil {
				if strings.Contains(err.Error(), "HANG_DETECTED") {
					dumpAllDiagnosticLogs(t, h, pod1Name, pod2Name)
					recordFatal(fmt.Errorf("Invariant 1 violated: %v", err))
					return
				}
				time.Sleep(500 * time.Millisecond)
				continue
			}

			hsh := strings.TrimSpace(out)
			if len(hsh) == 64 {
				manifestMu.Lock()
				manifest[relPath] = hsh
				manifestMu.Unlock()
			}
			time.Sleep(300 * time.Millisecond)
		}
	}()

	readerStop := make(chan struct{})
	var readerWg sync.WaitGroup
	readerWg.Add(1)
	go func() {
		defer readerWg.Done()
		checkedIndex := 0
		for {
			select {
			case <-readerStop:
				return
			default:
			}

			// 1. Cross-node reader: read manifest files via dd & sha256sum (plain read(2), not cat #278)
			manifestMu.RLock()
			var toCheck []durabilityEntry
			keys := make([]string, 0, len(manifest))
			for k := range manifest {
				keys = append(keys, k)
			}
			for i := checkedIndex; i < len(keys); i++ {
				k := keys[i]
				toCheck = append(toCheck, durabilityEntry{path: k, hash: manifest[k]})
			}
			manifestMu.RUnlock()

			for _, entry := range toCheck {
				stepName := fmt.Sprintf("reader_check_%s", entry.path)
				readCmd := fmt.Sprintf("dd if=/data/%s of=/tmp/reader.bin bs=4k 2>/dev/null && sha256sum /tmp/reader.bin | awk '{print $1}'", entry.path)
				out, err := runInPodWithDeadline(testCtx, h, pod2Name, stepName, stepDeadline, "/bin/sh", "-c", readCmd)
				tracker.RecordResult(stepName, out, err)
				if err != nil {
					if strings.Contains(err.Error(), "HANG_DETECTED") {
						dumpAllDiagnosticLogs(t, h, pod1Name, pod2Name)
						recordFatal(fmt.Errorf("Invariant 1 violated: %v", err))
						return
					}
					continue
				}
				gotHash := strings.TrimSpace(out)
				if gotHash != entry.hash {
					dumpAllDiagnosticLogs(t, h, pod1Name, pod2Name)
					recordFatal(fmt.Errorf("Invariant 3 violated (Cross-node durability): %s expected %s, got %s", entry.path, entry.hash, gotHash))
					return
				}
				checkedIndex++
			}

			// 2. Cross-node reader: verify committed churn tree against recorded expected state
			churnMu.RLock()
			snapChurn := latestChurn
			churnMu.RUnlock()

			if snapChurn != nil && len(snapChurn.entries) > 0 {
				stepName := fmt.Sprintf("reader_churn_verify_step_%d", snapChurn.step)
				checkDir := fmt.Sprintf("/data/churn/dir_%d", snapChurn.step)
				f1Path := checkDir + "/file1.bin"
				f2Path := checkDir + "/sub/file2_renamed.bin"
				expectedF1 := snapChurn.entries[fmt.Sprintf("./dir_%d/file1.bin", snapChurn.step)]
				expectedF2 := snapChurn.entries[fmt.Sprintf("./dir_%d/sub/file2_renamed.bin", snapChurn.step)]

				verifyCmd := fmt.Sprintf(`
set -e
[ -d %s ]
h1=$(sha256sum %s 2>/dev/null | awk '{print $1}')
h2=$(sha256sum %s 2>/dev/null | awk '{print $1}')
echo "$h1 $h2"
`, checkDir, f1Path, f2Path)

				out, err := runInPodWithDeadline(testCtx, h, pod2Name, stepName, stepDeadline, "/bin/sh", "-c", verifyCmd)
				tracker.RecordResult(stepName, out, err)
				if err == nil {
					fields := strings.Fields(strings.TrimSpace(out))
					if len(fields) == 2 {
						if expectedF1 != "" && fields[0] != expectedF1 {
							dumpAllDiagnosticLogs(t, h, pod1Name, pod2Name)
							recordFatal(fmt.Errorf("Invariant 3 / Churn verification failed: %s expected hash %s, got %s", f1Path, expectedF1, fields[0]))
							return
						}
						if expectedF2 != "" && fields[1] != expectedF2 {
							dumpAllDiagnosticLogs(t, h, pod1Name, pod2Name)
							recordFatal(fmt.Errorf("Invariant 3 / Churn verification failed: %s expected hash %s, got %s", f2Path, expectedF2, fields[1]))
							return
						}
					}
				} else if strings.Contains(err.Error(), "HANG_DETECTED") {
					dumpAllDiagnosticLogs(t, h, pod1Name, pod2Name)
					recordFatal(fmt.Errorf("Invariant 1 violated: %v", err))
					return
				}
			}

			time.Sleep(500 * time.Millisecond)
		}
	}()

	// -------------------------------------------------------------------------
	// Disruption Schedule
	// -------------------------------------------------------------------------
	t.Logf("Starting disruption schedule for %v...", duration)
	runStart := time.Now()
	runDeadline := runStart.Add(duration)

	streamCount := countControllerReplicationSessions(h)
	if streamCount <= 0 {
		streamCount = 1
	}

	nextWalBufferKill := runStart.Add(45 * time.Second) // offset from controller kills
	nextControllerKill := runStart.Add(120 * time.Second)
	outageTriggerTime := runStart.Add(duration / 3)
	snapshotTriggerTime := runStart.Add(duration / 2)
	outageDone := false
	snapshotDone := false

	disruptionTicker := time.NewTicker(1 * time.Second)
	defer disruptionTicker.Stop()

disruptionLoop:
	for time.Now().Before(runDeadline) {
		select {
		case <-testCtx.Done():
			break disruptionLoop
		case now := <-disruptionTicker.C:
			if testCtx.Err() != nil {
				break disruptionLoop
			}
			// Disruption 1: Force-kill objectfs-controller-0 (~90s)
			if now.After(nextControllerKill) {
				t.Logf("[%s] DISRUPTION: Force-killing objectfs-controller-0", now.Format("15:04:05"))
				oldUID := getPodUID("objectfs-controller-0", "default")
				h.RunCommand("kubectl", "delete", "pod", "objectfs-controller-0", "-n", "default", "--force", "--grace-period=0")
				if err := waitForNewPodReady(t, h, "objectfs-controller-0", "default", oldUID, 2*time.Minute); err != nil {
					dumpAllDiagnosticLogs(t, h, pod1Name, pod2Name)
					t.Fatalf("ObjectFS Controller failed to recover after kill: %v", err)
				}
				if err := h.WaitForStatefulSet("objectfs-controller", "default", 2*time.Minute); err != nil {
					dumpAllDiagnosticLogs(t, h, pod1Name, pod2Name)
					t.Fatalf("ObjectFS Controller failed to recover after kill: %v", err)
				}
				t.Logf("[%s] DISRUPTION: objectfs-controller-0 recovered", time.Now().Format("15:04:05"))
				nextControllerKill = time.Now().Add(90 * time.Second)
				if time.Until(nextWalBufferKill) < 60*time.Second {
					nextWalBufferKill = time.Now().Add(60 * time.Second)
				}
			}

			// Disruption 2: Force-kill wal-buffer-0 (~120s, offset)
			if now.After(nextWalBufferKill) {
				t.Logf("[%s] DISRUPTION: Force-killing wal-buffer-0", now.Format("15:04:05"))
				sessionsBeforeKill := countControllerReplicationSessions(h)
				oldWalUID := getPodUID("wal-buffer-0", "default")
				h.RunCommand("kubectl", "delete", "pod", "wal-buffer-0", "-n", "default", "--force", "--grace-period=0")
				if err := waitForNewPodReady(t, h, "wal-buffer-0", "default", oldWalUID, 2*time.Minute); err != nil {
					dumpAllDiagnosticLogs(t, h, pod1Name, pod2Name)
					t.Fatalf("WAL Buffer failed to recover after kill: %v", err)
				}
				if err := h.WaitForStatefulSet("wal-buffer", "default", 2*time.Minute); err != nil {
					dumpAllDiagnosticLogs(t, h, pod1Name, pod2Name)
					t.Fatalf("WAL Buffer failed to recover after kill: %v", err)
				}
				waitForControllerReplicationSessionIncrease(t, h, sessionsBeforeKill, streamCount, 2*time.Minute)
				t.Logf("[%s] DISRUPTION: wal-buffer-0 recovered", time.Now().Format("15:04:05"))
				nextWalBufferKill = time.Now().Add(120 * time.Second)
				if time.Until(nextControllerKill) < 60*time.Second {
					nextControllerKill = time.Now().Add(60 * time.Second)
				}
			}

			// Disruption 3: Outage (scale controller to 0 for >30s metadata timeout once per run)
			if !outageDone && now.After(outageTriggerTime) {
				outageDone = true
				t.Logf("[%s] DISRUPTION: Scaling objectfs-controller to 0 (starting outage window)", now.Format("15:04:05"))
				tracker.StartOutage()
				h.RunCommand("kubectl", "scale", "statefulset", "objectfs-controller", "-n", "default", "--replicas=0")
				// Sleep 35s (> 30s metadata timeout)
				time.Sleep(35 * time.Second)
				t.Logf("[%s] DISRUPTION: Scaling objectfs-controller back to 1", time.Now().Format("15:04:05"))
				h.RunCommand("kubectl", "scale", "statefulset", "objectfs-controller", "-n", "default", "--replicas=1")
				if err := h.WaitForStatefulSet("objectfs-controller", "default", 2*time.Minute); err != nil {
					dumpAllDiagnosticLogs(t, h, pod1Name, pod2Name)
					t.Fatalf("ObjectFS Controller failed to recover from scale to 0: %v", err)
				}
				tracker.EndOutage(5 * time.Second)
				t.Logf("[%s] DISRUPTION: Outage recovery complete (5s recovery window bounded)", time.Now().Format("15:04:05"))
				if time.Until(nextControllerKill) < 60*time.Second {
					nextControllerKill = time.Now().Add(60 * time.Second)
				}
				if time.Until(nextWalBufferKill) < 60*time.Second {
					nextWalBufferKill = time.Now().Add(60 * time.Second)
				}
			}

			// Disruption 4: Mid-run snapshot
			if !snapshotDone && now.After(snapshotTriggerTime) {
				snapshotDone = true
				t.Logf("[%s] DISRUPTION: Creating mid-run snapshot via objectfsctl", now.Format("15:04:05"))
				snapCmd := exec.Command("kubectl", "exec", "objectfs-controller-0", "-n", "default", "-c", "objectfs-controller", "--", "objectfsctl", "--server", "localhost:50051", "snapshots", "create", volumeID)
				snapOut, snapErr := snapCmd.CombinedOutput()
				if snapErr != nil {
					t.Logf("[%s] Warning: Mid-run snapshot create error: %v (out: %s)", time.Now().Format("15:04:05"), snapErr, string(snapOut))
				} else {
					t.Logf("[%s] Mid-run snapshot created: %s", time.Now().Format("15:04:05"), strings.TrimSpace(string(snapOut)))
				}
			}
		}
	}

	fatalErrMu.Lock()
	if fatalErr != nil {
		t.Logf("Torture workload stopped early by fatal error; shutting down workloads...")
	} else {
		t.Logf("Torture workload duration completed; shutting down workloads and verifying invariants...")
	}
	fatalErrMu.Unlock()
	close(churnStop)
	close(writerStop)
	close(readerStop)
	churnWg.Wait()
	writerWg.Wait()
	readerWg.Wait()

	if fatalErr != nil {
		t.Fatalf("%v", fatalErr)
	}

	// Stop all background fsx instances gracefully and wait for them to exit
	t.Logf("Stopping fsx processes on all nodes...")
	for _, fi := range fsxList {
		_, _ = h.RunInPod(fi.pod, "default", "/bin/sh", "-c", fmt.Sprintf("kill -TERM $(cat %s 2>/dev/null) 2>/dev/null || true", fi.pidFile))
	}

	// Poll bounded up to 20s until all fsx processes have terminated
	pollDeadline := time.Now().Add(20 * time.Second)
	for _, fi := range fsxList {
		for time.Now().Before(pollDeadline) {
			out, err := h.RunInPod(fi.pod, "default", "/bin/sh", "-c", fmt.Sprintf("kill -0 $(cat %s 2>/dev/null) 2>/dev/null && echo alive || echo dead", fi.pidFile))
			if err != nil || strings.TrimSpace(out) == "dead" {
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
	// Force-kill any lingering process if needed
	for _, fi := range fsxList {
		_, _ = h.RunInPod(fi.pod, "default", "/bin/sh", "-c", fmt.Sprintf("kill -KILL $(cat %s 2>/dev/null) 2>/dev/null || true", fi.pidFile))
	}

	// Run explicit sync on both pods to ensure all written dirty buffers and metadata are committed
	t.Logf("Flushing and syncing filesystems on both worker nodes...")
	if out, err := runInPodWithDeadline(testCtx, h, pod1Name, "final_sync_pod1", stepDeadline, "sync", "/data"); err != nil {
		dumpAllDiagnosticLogs(t, h, pod1Name, pod2Name)
		t.Fatalf("Invariant 1 violated: final sync on %s failed: %v (out: %s)", pod1Name, err, out)
	}
	if out, err := runInPodWithDeadline(testCtx, h, pod2Name, "final_sync_pod2", stepDeadline, "sync", "/data"); err != nil {
		dumpAllDiagnosticLogs(t, h, pod1Name, pod2Name)
		t.Fatalf("Invariant 1 violated: final sync on %s failed: %v (out: %s)", pod2Name, err, out)
	}

	// -------------------------------------------------------------------------
	// Invariant 2: fsx integrity holds for every instance
	// -------------------------------------------------------------------------
	t.Logf("Checking Invariant 2 (fsx data integrity across all instances)...")
	for _, fi := range fsxList {
		logOut, _ := h.RunInPod(fi.pod, "default", "cat", fi.logFile)
		if strings.Contains(logOut, "bad data") || strings.Contains(logOut, "fsx: read:") || strings.Contains(logOut, "fsx: mappedwrite:") {
			dumpAllDiagnosticLogs(t, h, pod1Name, pod2Name)
			t.Fatalf("Invariant 2 violated (fsx data corruption): %s instance %d (seed=%d):\n%s", fi.pod, fi.instance, fi.seed, logOut)
		}
	}
	t.Logf("Invariant 2: PASS (all fsx instances verified clean with no data corruption)")

	// -------------------------------------------------------------------------
	// Invariant 4: Bounded failure during outages (EIO only during outage window)
	// -------------------------------------------------------------------------
	t.Logf("Checking Invariant 4 (Bounded failure during outages)...")
	t.Logf("Outage EIO count: %d during outage window (expected), %d outside outage window", tracker.eioDuringOutage, tracker.eioOutsideOutage)
	if tracker.eioOutsideOutage > 0 {
		dumpAllDiagnosticLogs(t, h, pod1Name, pod2Name)
		t.Fatalf("Invariant 4 violated: observed %d EIO errors outside outage window:\n%s", tracker.eioOutsideOutage, strings.Join(tracker.unexpectedErrors, "\n"))
	}
	t.Logf("Invariant 4: PASS (all EIO errors strictly bounded to outage window)")

	// -------------------------------------------------------------------------
	// Take final snapshot and create fresh mount verifier pod
	// -------------------------------------------------------------------------
	t.Logf("Creating final snapshot at end of torture run...")
	finalSnapCmd := exec.Command("kubectl", "exec", "objectfs-controller-0", "-n", "default", "-c", "objectfs-controller", "--", "objectfsctl", "--server", "localhost:50051", "snapshots", "create", volumeID)
	finalSnapOut, err := finalSnapCmd.CombinedOutput()
	if err != nil {
		dumpAllDiagnosticLogs(t, h, pod1Name, pod2Name)
		t.Fatalf("Failed to create final snapshot: %v (out: %s)", err, string(finalSnapOut))
	}
	t.Logf("Final snapshot: %s", strings.TrimSpace(string(finalSnapOut)))

	verifierPodName := "torture-fresh-mount-verifier"
	verifierPodYaml := fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: %s
spec:
  nodeSelector:
    kubernetes.io/hostname: %s
  containers:
    - name: app
      image: alpine
      command: ["/bin/sh", "-c", "sleep 3600"]
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      csi:
        driver: objectfs.labs.gke.io
        volumeAttributes:
          volumeID: %s
`, verifierPodName, node2, volumeID)

	t.Logf("Deploying fresh cross-node verifier pod on %s", node2)
	h.KubectlApplyContent(verifierPodName, verifierPodYaml)
	if err := h.WaitForPodReady(verifierPodName, "default", 2*time.Minute); err != nil {
		dumpAllDiagnosticLogs(t, h, pod1Name, pod2Name)
		t.Fatalf("Verifier pod failed to start: %v", err)
	}

	// -------------------------------------------------------------------------
	// Invariant 3: Durability across restarts and fresh cross-node mount
	// (Note: snapshot restore is pending a restore RPC/dataSource path, tracked in #320)
	// -------------------------------------------------------------------------
	t.Logf("Checking Invariant 3 (Durability across acknowledged writes on fresh cross-node mount; snapshot restore path pending #320)...")
	manifestMu.RLock()
	manifestCopy := make(map[string]string)
	for k, v := range manifest {
		manifestCopy[k] = v
	}
	manifestMu.RUnlock()

	durabilityChecked := 0
	for relPath, expectedHash := range manifestCopy {
		checkCmd := fmt.Sprintf("dd if=/data/%s of=/tmp/durability.bin bs=4k 2>/dev/null && sha256sum /tmp/durability.bin | awk '{print $1}'", relPath)
		out, err := runInPodWithDeadline(testCtx, h, verifierPodName, "durability_verify_"+relPath, stepDeadline, "/bin/sh", "-c", checkCmd)
		if err != nil {
			dumpAllDiagnosticLogs(t, h, pod1Name, pod2Name)
			t.Fatalf("Invariant 3 violated: failed to read %s on verifier pod: %v", relPath, err)
		}
		gotHash := strings.TrimSpace(out)
		if gotHash != expectedHash {
			dumpAllDiagnosticLogs(t, h, pod1Name, pod2Name)
			t.Fatalf("Invariant 3 violated: file %s hash mismatch on fresh mount: expected %s, got %s", relPath, expectedHash, gotHash)
		}
		durabilityChecked++
	}
	t.Logf("Invariant 3: PASS (%d checkpointed files verified with exact matching hashes on fresh cross-node mount; snapshot restore path pending #320)", durabilityChecked)

	// -------------------------------------------------------------------------
	// Invariant 5: No silent divergence (--debug-mutation-check and byte-identical tree)
	// -------------------------------------------------------------------------
	t.Logf("Checking Invariant 5 (No silent divergence)...")
	ctrlLogs := h.GetPodLogsByName("objectfs-controller-0", "default")
	if strings.Contains(ctrlLogs, "mutation check failed") || strings.Contains(ctrlLogs, "stats check failed") {
		dumpAllDiagnosticLogs(t, h, pod1Name, pod2Name)
		t.Fatalf("Invariant 5 violated: controller logged mutation or stats check failure:\n%s", ctrlLogs)
	}

	treeHashCmd := "/bin/sh -c 'find /data -type f | sort | xargs sha256sum | sha256sum | awk \"{print \\$1}\"'"
	hashPod1, err1 := runInPodWithDeadline(testCtx, h, pod1Name, "tree_hash_pod1", stepDeadline, "/bin/sh", "-c", treeHashCmd)
	hashPod2, err2 := runInPodWithDeadline(testCtx, h, pod2Name, "tree_hash_pod2", stepDeadline, "/bin/sh", "-c", treeHashCmd)
	hashVerifier, err3 := runInPodWithDeadline(testCtx, h, verifierPodName, "tree_hash_verifier", stepDeadline, "/bin/sh", "-c", treeHashCmd)

	if err1 != nil || err2 != nil || err3 != nil {
		dumpAllDiagnosticLogs(t, h, pod1Name, pod2Name)
		t.Fatalf("Failed to calculate tree hashes: err1=%v, err2=%v, err3=%v", err1, err2, err3)
	}

	h1 := strings.TrimSpace(hashPod1)
	h2 := strings.TrimSpace(hashPod2)
	hV := strings.TrimSpace(hashVerifier)
	t.Logf("Recursive file tree hashes: Node 1=%s, Node 2=%s, Fresh Mount Verifier=%s", h1, h2, hV)

	if h1 != h2 || h2 != hV {
		dumpAllDiagnosticLogs(t, h, pod1Name, pod2Name)
		t.Fatalf("Invariant 5 violated: silent tree divergence detected: Node 1=%s, Node 2=%s, Verifier=%s", h1, h2, hV)
	}
	t.Logf("Invariant 5: PASS (tree hashes are byte-identical across all nodes and fresh mount verifier; snapshot restore path pending #320)")

	t.Logf("================================================================================")
	t.Logf("ALL 5 INVARIANTS PASSED! ObjectFS Torture E2E Test SUCCESSFUL")
	t.Logf("================================================================================")
}
