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
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

var (
	sharedSingleNodeClusterOnce sync.Once
	sharedSingleNodeErr         error
	sharedSingleNodeCreated     bool
)

const sharedSingleNodeClusterName = "objectfs-shared-single"

// requireE2EGroup skips the test unless RUN_E2E is set and OBJECTFS_E2E_GROUP matches.
// When OBJECTFS_E2E_GROUP is empty or unset, it matches only "core".
func requireE2EGroup(t *testing.T, group string) {
	t.Helper()
	if os.Getenv("RUN_E2E") == "" {
		t.Skip("Skipping E2E test; RUN_E2E not set")
	}
	currentGroup := os.Getenv("OBJECTFS_E2E_GROUP")
	if currentGroup == "" {
		currentGroup = "core"
	}
	if currentGroup != group {
		t.Skipf("Skipping test for group %q; OBJECTFS_E2E_GROUP is %q", group, currentGroup)
	}
}

func TestMain(m *testing.M) {
	code := m.Run()

	if os.Getenv("OBJECTFS_E2E_KEEP_CLUSTERS") == "" {
		if sharedSingleNodeCreated {
			if out, err := exec.Command("kind", "delete", "cluster", "--name", sharedSingleNodeClusterName).CombinedOutput(); err != nil {
				fmt.Printf("Warning: failed to delete cluster %s on exit: %v (%s)\n", sharedSingleNodeClusterName, err, strings.TrimSpace(string(out)))
			}
		}
		if sharedMultiNodeCreated {
			if out, err := exec.Command("kind", "delete", "cluster", "--name", sharedMultiNodeClusterName).CombinedOutput(); err != nil {
				fmt.Printf("Warning: failed to delete cluster %s on exit: %v (%s)\n", sharedMultiNodeClusterName, err, strings.TrimSpace(string(out)))
			}
		}
	}

	os.Exit(code)
}

func setupSharedObjectFSCluster(t *testing.T) (*Harness, string) {
	t.Helper()

	sharedSingleNodeClusterOnce.Do(func() {
		// Clean up any stale cluster from a previous aborted local run
		if out, err := exec.Command("kind", "delete", "cluster", "--name", sharedSingleNodeClusterName).CombinedOutput(); err != nil {
			t.Logf("Note: clean up existing cluster %s returned: %v (%s)", sharedSingleNodeClusterName, err, strings.TrimSpace(string(out)))
		}

		cmd := exec.Command("kind", "create", "cluster", "--name", sharedSingleNodeClusterName)
		if out, err := cmd.CombinedOutput(); err != nil {
			sharedSingleNodeErr = fmt.Errorf("failed to create kind cluster %s: %w\n%s", sharedSingleNodeClusterName, err, strings.TrimSpace(string(out)))
			return
		}
		sharedSingleNodeCreated = true

		if out, err := exec.Command("kind", "export", "kubeconfig", "--name", sharedSingleNodeClusterName).CombinedOutput(); err != nil {
			sharedSingleNodeErr = fmt.Errorf("failed to export kubeconfig for %s: %w (%s)", sharedSingleNodeClusterName, err, strings.TrimSpace(string(out)))
			return
		}

		initH := NewHarness(t, sharedSingleNodeClusterName)
		gitRoot := initH.GetGitRoot()

		// Deploy FakeS3 backing object storage in default namespace
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
          image: fakes3
          args: ["--listen=0.0.0.0:9000", "--buckets=objectfs-bucket", "--quiet"]
          ports:
            - containerPort: 9000
`

		s3BackendURL := "s3://objectfs-bucket?endpoint=http://fakes3:9000&region=us-east-1&s3ForcePathStyle=true"

		// Read and adapt ObjectFS manifest with S3 backend
		objectfsManifestPath := filepath.Join(gitRoot, "k8s/objectfs.yaml")
		b, err := os.ReadFile(objectfsManifestPath)
		if err != nil {
			sharedSingleNodeErr = fmt.Errorf("failed to read objectfs manifest: %w", err)
			return
		}
		objectfsManifest := string(b)
		objectfsManifest = strings.ReplaceAll(objectfsManifest, "namespace: kube-objectfs-system", "namespace: default")
		objectfsManifest = strings.ReplaceAll(objectfsManifest, `backend: ""`, fmt.Sprintf("backend: %q", s3BackendURL))

		awsEnvController := `env:
            - name: AWS_ACCESS_KEY_ID
              value: "fakes3"
            - name: AWS_SECRET_ACCESS_KEY
              value: "fakes3"
            - name: AWS_REGION
              value: "us-east-1"
            - name: OBJECT_STORAGE_BACKEND`
		objectfsManifest = strings.ReplaceAll(objectfsManifest, "env:\n            - name: OBJECT_STORAGE_BACKEND", awsEnvController)

		// Read and adapt WAL buffer manifest
		walManifestPath := filepath.Join(gitRoot, "k8s/wal.yaml")
		b, err = os.ReadFile(walManifestPath)
		if err != nil {
			sharedSingleNodeErr = fmt.Errorf("failed to read wal manifest: %w", err)
			return
		}
		walManifest := string(b)
		walManifest = strings.ReplaceAll(walManifest, "namespace: kube-objectfs-system", "namespace: default")

		awsEnvWAL := `env:
            - name: AWS_ACCESS_KEY_ID
              value: "fakes3"
            - name: AWS_SECRET_ACCESS_KEY
              value: "fakes3"
            - name: AWS_REGION
              value: "us-east-1"
            - name: OBJECT_STORAGE_BACKEND`
		walManifest = strings.ReplaceAll(walManifest, "env:\n            - name: OBJECT_STORAGE_BACKEND", awsEnvWAL)

		initH.ApDeploy(t, map[string]string{
			"fakes3":     fakes3Manifest,
			"objectfs":   objectfsManifest,
			"wal-buffer": walManifest,
		})

		if err := initH.WaitForDeployment("fakes3", "default", 2*time.Minute); err != nil {
			sharedSingleNodeErr = fmt.Errorf("fakes3 deployment failed to start: %w", err)
			return
		}

		initH.RunCommand("kubectl", "wait", "--for=condition=complete", "job/objectfs-certsigner", "-n", "default", "--timeout=60s")

		if err := initH.WaitForStatefulSet("wal-buffer", "default", 2*time.Minute); err != nil {
			sharedSingleNodeErr = fmt.Errorf("wal-buffer failed to start: %w", err)
			return
		}
		if err := initH.WaitForStatefulSet("objectfs-controller", "default", 2*time.Minute); err != nil {
			sharedSingleNodeErr = fmt.Errorf("objectfs-controller failed to start: %w", err)
			return
		}
		if err := initH.WaitForDaemonSet("objectfs-fuse-holder", "default", 2*time.Minute); err != nil {
			sharedSingleNodeErr = fmt.Errorf("objectfs-fuse-holder failed to start: %w", err)
			return
		}
		if err := initH.WaitForDaemonSet("objectfs-node-daemon", "default", 2*time.Minute); err != nil {
			sharedSingleNodeErr = fmt.Errorf("objectfs-node-daemon failed to start: %w", err)
			return
		}

		adminPodYaml := `
apiVersion: v1
kind: Pod
metadata:
  name: objectfs-admin-pod
  namespace: default
  labels:
    app: objectfs-admin
spec:
  containers:
    - name: client
      image: images.local/objectfs-controller:e2e
      imagePullPolicy: Never
      command: ["/bin/sh", "-c", "sleep 36000"]
      volumeMounts:
        - name: certs-client
          mountPath: /etc/objectfs/certs/client
          readOnly: true
  volumes:
    - name: certs-client
      secret:
        secretName: objectfs-admin-client
        items:
          - key: tls.key
            path: tls.key
          - key: tls.crt
            path: tls.crt
          - key: ca.crt
            path: trust/ca.crt
`
		initH.KubectlApplyContent("objectfs-admin-pod", adminPodYaml)
		if err := initH.WaitForPodReady("objectfs-admin-pod", "default", 2*time.Minute); err != nil {
			sharedSingleNodeErr = fmt.Errorf("objectfs-admin-pod failed to start: %w", err)
			return
		}
	})

	if sharedSingleNodeErr != nil {
		t.Fatalf("setupSharedObjectFSCluster failed: %v", sharedSingleNodeErr)
	}

	h := NewHarness(t, sharedSingleNodeClusterName)
	h.TrackNamespace("default")

	// Ensure system statefulsets are back and healthy before the test begins
	if err := h.WaitForStatefulSet("wal-buffer", "default", 2*time.Minute); err != nil {
		t.Fatalf("WAL buffer not ready: %v", err)
	}
	if err := h.WaitForStatefulSet("objectfs-controller", "default", 2*time.Minute); err != nil {
		t.Fatalf("ObjectFS controller not ready: %v", err)
	}
	if err := h.WaitForDaemonSet("objectfs-node-daemon", "default", 2*time.Minute); err != nil {
		t.Fatalf("ObjectFS node daemon not ready: %v", err)
	}

	ns := h.CreateTestNamespace("objectfs-test")

	return h, ns
}

func TestObjectFSE2E(t *testing.T) {
	requireE2EGroup(t, "core")

	h, ns := setupSharedObjectFSCluster(t)
	volumeID := fmt.Sprintf("objectfs-test-vol-%s", ns)

	// Step 1: Run Pod 1 that writes a file
	pod1Name := "objectfs-pod-1"
	pod1Yaml := fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: %s
  namespace: %s
spec:
  restartPolicy: Never
  containers:
    - name: app
      image: alpine
      command: ["/bin/sh", "-c", "echo 'hello objectfs' > /data/hello.txt && sync && sleep 10"]
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      csi:
        driver: objectfs.labs.gke.io
        volumeAttributes:
          volumeID: %s
`, pod1Name, ns, volumeID)

	t.Logf("Creating Pod 1 in namespace %s", ns)
	h.KubectlApplyContent(pod1Name, pod1Yaml)
	if err := h.WaitForPodReady(pod1Name, ns, 1*time.Minute); err != nil {
		t.Logf("Events:\n%s\n", h.GetEvents(ns))
		t.Logf("Node Daemon Logs:\n%s\n", h.GetPodLogs("app=objectfs-node-daemon", "default"))
		t.Fatalf("Test Pod 1 failed to start: %v", err)
	}

	time.Sleep(5 * time.Second)
	t.Logf("Deleting Pod 1")
	h.DeletePod(pod1Name, ns)

	// Step 2: Start Pod 2, read the file, verify content, create subdir and second file
	pod2Name := "objectfs-pod-2"
	pod2Yaml := fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: %s
  namespace: %s
spec:
  restartPolicy: Never
  containers:
    - name: app
      image: alpine
      command: ["/bin/sh", "-c", "cat /data/hello.txt && mkdir /data/nested && echo 'nested data' > /data/nested/data.txt && echo 'updated objectfs' > /data/hello.txt && sync && sleep 10"]
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      csi:
        driver: objectfs.labs.gke.io
        volumeAttributes:
          volumeID: %s
`, pod2Name, ns, volumeID)

	t.Logf("Creating Pod 2 in namespace %s", ns)
	h.KubectlApplyContent(pod2Name, pod2Yaml)
	if err := h.WaitForPodReady(pod2Name, ns, 1*time.Minute); err != nil {
		t.Logf("Node Daemon Logs:\n%s\n", h.GetPodLogs("app=objectfs-node-daemon", "default"))
		t.Fatalf("Test Pod 2 failed to start: %v", err)
	}

	time.Sleep(5 * time.Second)
	logs2 := h.GetPodLogsByName(pod2Name, ns)
	if !strings.Contains(logs2, "hello objectfs") {
		t.Logf("Pod 2 Logs:\n%s\n", logs2)
		t.Fatalf("Pod 2 did not see 'hello objectfs'")
	}
	t.Logf("Pod 2 verified, deleting")
	h.DeletePod(pod2Name, ns)

	// Step 3: Start Pod 3 to verify updated file and nested directory
	pod3Name := "objectfs-pod-3"
	pod3Yaml := fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: %s
  namespace: %s
spec:
  containers:
    - name: app
      image: alpine
      command: ["/bin/sh", "-c", "cat /data/hello.txt && cat /data/nested/data.txt && sleep 3600"]
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      csi:
        driver: objectfs.labs.gke.io
        volumeAttributes:
          volumeID: %s
`, pod3Name, ns, volumeID)

	t.Logf("Creating Pod 3 in namespace %s", ns)
	h.KubectlApplyContent(pod3Name, pod3Yaml)
	if err := h.WaitForPodReady(pod3Name, ns, 1*time.Minute); err != nil {
		t.Fatalf("Test Pod 3 failed to start: %v", err)
	}

	out1, err := h.RunInPod(pod3Name, ns, "cat", "/data/hello.txt")
	if err != nil {
		t.Fatalf("Failed to read hello.txt in Pod 3: %v", err)
	}
	if !strings.Contains(out1, "updated objectfs") {
		t.Fatalf("Expected hello.txt content 'updated objectfs', got: %s", out1)
	}

	out2, err := h.RunInPod(pod3Name, ns, "cat", "/data/nested/data.txt")
	if err != nil {
		t.Fatalf("Failed to read nested/data.txt in Pod 3: %v", err)
	}
	if !strings.Contains(out2, "nested data") {
		t.Fatalf("Expected nested/data.txt content 'nested data', got: %s", out2)
	}

	h.DeletePod(pod3Name, ns)
	t.Logf("Successfully verified ObjectFS multi-writer FUSE CSI driver with node daemon authenticated via mTLS!")

	// Step 4: Verify mTLS, certificate authorization, and NetworkPolicy
	authClientPod := "objectfs-auth-test-pod"
	authClientYaml := fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: %s
  namespace: default
  labels:
    app: objectfs-admin
spec:
  containers:
    - name: client
      image: images.local/objectfs-controller:e2e
      imagePullPolicy: Never
      command: ["/bin/sh", "-c", "sleep 3600"]
      volumeMounts:
        - name: admin-certs
          mountPath: /etc/objectfs/certs/client
          readOnly: true
        - name: node-certs
          mountPath: /etc/objectfs/certs/node
          readOnly: true
        - name: nocert-dir
          mountPath: /etc/objectfs/certs/nocert
  volumes:
    - name: admin-certs
      secret:
        secretName: objectfs-admin-client
        items:
          - key: tls.key
            path: tls.key
          - key: tls.crt
            path: tls.crt
          - key: ca.crt
            path: trust/ca.crt
    - name: node-certs
      secret:
        secretName: objectfs-node-daemon-client
        items:
          - key: tls.key
            path: tls.key
          - key: tls.crt
            path: tls.crt
          - key: ca.crt
            path: trust/ca.crt
    - name: nocert-dir
      emptyDir: {}
`, authClientPod)

	h.KubectlApplyContent(authClientPod, authClientYaml)
	if err := h.WaitForPodReady(authClientPod, "default", 1*time.Minute); err != nil {
		t.Fatalf("auth test pod failed to start: %v", err)
	}

	// 1. Pod with no certificate fails TLS handshake
	noCertOut, noCertErr := h.RunInPod(authClientPod, "default", "objectfsctl", "--server", "objectfs-controller:50051", "--client-cert-dir", "/etc/objectfs/certs/nocert", "volumes", "list")
	if noCertErr == nil {
		t.Fatalf("expected call without client certificate to fail, but succeeded with output: %s", noCertOut)
	}
	t.Logf("Client without certificate correctly rejected: %v (%s)", noCertErr, strings.TrimSpace(noCertOut))

	// 2. Pod with node-daemon client certificate calling admin RPC (volumes list) gets PermissionDenied
	nodeCertOut, nodeCertErr := h.RunInPod(authClientPod, "default", "objectfsctl", "--server", "objectfs-controller:50051", "--client-cert-dir", "/etc/objectfs/certs/node", "volumes", "list")
	if nodeCertErr == nil {
		t.Fatalf("expected node-daemon certificate calling admin RPC to fail with PermissionDenied, but got: %s", nodeCertOut)
	}
	if !strings.Contains(nodeCertOut, "PermissionDenied") && !strings.Contains(fmt.Sprintf("%v", nodeCertErr), "PermissionDenied") {
		t.Fatalf("expected PermissionDenied for node-daemon role, got: %v (out: %s)", nodeCertErr, nodeCertOut)
	}
	t.Logf("Node daemon certificate correctly denied for admin RPC: %s", strings.TrimSpace(nodeCertOut))

	// 3. objectfsctl with admin directory succeeds
	adminOut, adminErr := h.RunInPod(authClientPod, "default", "objectfsctl", "--server", "objectfs-controller:50051", "volumes", "list")
	if adminErr != nil {
		t.Fatalf("expected admin certificate to succeed, got error: %v (out: %s)", adminErr, adminOut)
	}
	t.Logf("Admin client certificate succeeded: %s", strings.TrimSpace(adminOut))

	h.DeletePod(authClientPod, "default")

	// 4. NetworkPolicy step: unprivileged pod without allowed label is dropped at network level
	unauthPod := "objectfs-unauth-netpol-pod"
	unauthYaml := fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: %s
  namespace: default
  labels:
    app: unprivileged-client
spec:
  containers:
    - name: client
      image: images.local/objectfs-controller:e2e
      imagePullPolicy: Never
      command: ["/bin/sh", "-c", "sleep 3600"]
      volumeMounts:
        - name: admin-certs
          mountPath: /etc/objectfs/certs/client
          readOnly: true
  volumes:
    - name: admin-certs
      secret:
        secretName: objectfs-admin-client
        items:
          - key: tls.key
            path: tls.key
          - key: tls.crt
            path: tls.crt
          - key: ca.crt
            path: trust/ca.crt
`, unauthPod)

	h.KubectlApplyContent(unauthPod, unauthYaml)
	if err := h.WaitForPodReady(unauthPod, "default", 1*time.Minute); err != nil {
		t.Fatalf("unauth netpol pod failed to start: %v", err)
	}

	netpolOut, netpolErr := h.RunInPod(unauthPod, "default", "timeout", "5", "objectfsctl", "--server", "objectfs-controller:50051", "volumes", "list")
	if netpolErr == nil {
		t.Fatalf("expected NetworkPolicy to drop traffic from unlabelled pod, but command succeeded: %s", netpolOut)
	}
	h.DeletePod(unauthPod, "default")
	t.Logf("NetworkPolicy correctly blocked unlabelled pod at network level: %v (%s)", netpolErr, strings.TrimSpace(netpolOut))

	// Step 5: Live reload for certificate rotation without restart
	controllerRestartsCmd := exec.Command("kubectl", "get", "pod", "objectfs-controller-0", "-n", "default", "-o", "jsonpath={.status.containerStatuses[0].restartCount}")
	controllerRestartsBeforeOut, err := controllerRestartsCmd.CombinedOutput()
	if err != nil {
		t.Fatalf("failed to get controller restart count: %v (%s)", err, controllerRestartsBeforeOut)
	}
	controllerRestartsBefore := string(controllerRestartsBeforeOut)

	origAdminCert, err := h.RunInPod("objectfs-admin-pod", "default", "cat", "/etc/objectfs/certs/client/tls.crt")
	if err != nil {
		t.Fatalf("failed to read admin cert before rotation: %v (%s)", err, origAdminCert)
	}

	// Delete completed Job (preserving existing Secrets so certsigner carries forward previous CA in trust bundle)
	h.RunCommand("kubectl", "delete", "job", "objectfs-certsigner", "-n", "default")

	// Re-run certsigner Job
	certsignerJobYaml := `
apiVersion: batch/v1
kind: Job
metadata:
  name: objectfs-certsigner
  namespace: default
spec:
  ttlSecondsAfterFinished: 600
  template:
    metadata:
      labels:
        app: objectfs-certsigner
    spec:
      restartPolicy: OnFailure
      serviceAccountName: objectfs-certsigner
      containers:
        - name: certsigner
          image: images.local/certsigner:e2e
          imagePullPolicy: Never
          env:
            - name: POD_NAMESPACE
              value: default
`
	h.KubectlApplyContent("objectfs-certsigner", certsignerJobYaml)
	h.RunCommand("kubectl", "wait", "--for=condition=complete", "job/objectfs-certsigner", "-n", "default", "--timeout=60s")
	t.Logf("Successfully re-ran certsigner Job to rotate TLS Secrets")

	// Poll until kubelet syncs rotated certificate Secret into objectfs-admin-pod
	deadline := time.Now().Add(2 * time.Minute)
	var newAdminCert string
	for time.Now().Before(deadline) {
		out, err := h.RunInPod("objectfs-admin-pod", "default", "cat", "/etc/objectfs/certs/client/tls.crt")
		if err == nil && strings.TrimSpace(out) != "" && strings.TrimSpace(out) != strings.TrimSpace(origAdminCert) {
			newAdminCert = out
			break
		}
		time.Sleep(2 * time.Second)
	}
	if strings.TrimSpace(newAdminCert) == strings.TrimSpace(origAdminCert) {
		t.Fatalf("kubelet did not sync rotated Secret into objectfs-admin-pod within timeout")
	}
	t.Logf("Observed rotated certificate in objectfs-admin-pod mount")

	// Wait briefly for reloader to pick up file changes
	time.Sleep(2 * time.Second)

	// 1. Existing daemon keeps working: mount and write to volume via CSI driver
	pod4Name := "objectfs-pod-rotation-verify"
	pod4Yaml := fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: %s
  namespace: %s
spec:
  containers:
    - name: app
      image: alpine
      command: ["/bin/sh", "-c", "echo 'post-rotation data' > /data/post-rotation.txt && cat /data/post-rotation.txt && sleep 3600"]
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      csi:
        driver: objectfs.labs.gke.io
        volumeAttributes:
          volumeID: %s
`, pod4Name, ns, volumeID)

	t.Logf("Creating Pod 4 to verify existing node daemon continues to function")
	h.KubectlApplyContent(pod4Name, pod4Yaml)
	if err := h.WaitForPodReady(pod4Name, ns, 1*time.Minute); err != nil {
		t.Fatalf("Test Pod 4 failed to start on existing daemon: %v", err)
	}

	out4, err := h.RunInPod(pod4Name, ns, "cat", "/data/post-rotation.txt")
	if err != nil {
		t.Fatalf("Failed to read post-rotation.txt in Pod 4: %v", err)
	}
	if !strings.Contains(out4, "post-rotation data") {
		t.Fatalf("Expected post-rotation.txt content 'post-rotation data', got: %s", out4)
	}
	h.DeletePod(pod4Name, ns)
	t.Logf("Existing daemon successfully processed I/O after certificate rotation without restart")

	// 2. New connection from admin pod uses newly rotated certificate and succeeds.
	// Poll with a deadline of two minutes to allow controller volume mount to sync.
	deadlineConn := time.Now().Add(2 * time.Minute)
	var adminOutAfter string
	var adminErrAfter error
	for time.Now().Before(deadlineConn) {
		adminOutAfter, adminErrAfter = h.RunInPod("objectfs-admin-pod", "default", "objectfsctl", "--server", "objectfs-controller:50051", "volumes", "list")
		if adminErrAfter == nil {
			break
		}
		time.Sleep(2 * time.Second)
	}
	if adminErrAfter != nil {
		t.Fatalf("new connection with rotated admin certificate failed within 2m timeout: %v (%s)", adminErrAfter, adminOutAfter)
	}
	t.Logf("New connection successfully connected using rotated certificate: %s", strings.TrimSpace(adminOutAfter))

	// Verify controller was not restarted
	controllerRestartsAfterCmd := exec.Command("kubectl", "get", "pod", "objectfs-controller-0", "-n", "default", "-o", "jsonpath={.status.containerStatuses[0].restartCount}")
	controllerRestartsAfterOut, err := controllerRestartsAfterCmd.CombinedOutput()
	if err != nil {
		t.Fatalf("failed to get controller restart count after rotation: %v (%s)", err, controllerRestartsAfterOut)
	}
	if strings.TrimSpace(controllerRestartsBefore) != strings.TrimSpace(string(controllerRestartsAfterOut)) {
		t.Fatalf("controller restarted during rotation (before: %s, after: %s)", controllerRestartsBefore, string(controllerRestartsAfterOut))
	}
}

func TestObjectFSStatefulSetE2E(t *testing.T) {
	requireE2EGroup(t, "core")

	h, ns := setupSharedObjectFSCluster(t)

	// Deploy StatefulSet with Dynamic Volume Provisioning via StorageClass
	statefulSetYaml := fmt.Sprintf(`
apiVersion: v1
kind: ConfigMap
metadata:
  name: dev-vscode
  namespace: %s
data:
  settings.json: |
    {
      "editor.fontSize": 14
    }
---
apiVersion: v1
kind: Secret
metadata:
  name: dev-vscode
  namespace: %s
type: Opaque
data:
  password: cGFzc3dvcmQ=
---
apiVersion: v1
kind: Service
metadata:
  name: dev-vscode
  namespace: %s
spec:
  ports:
    - port: 80
      targetPort: 80
  selector:
    app: dev-vscode
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: dev-vscode
  namespace: %s
spec:
  serviceName: dev-vscode
  replicas: 1
  selector:
    matchLabels:
      app: dev-vscode
  template:
    metadata:
      labels:
        app: dev-vscode
    spec:
      initContainers:
        - name: init
          image: debian:bookworm-slim
          command: ["/bin/sh", "-c", "mkdir -p /vscode-server/workspace"]
          volumeMounts:
            - name: dev-vscode-data
              mountPath: /vscode-server
      containers:
        - name: app
          image: debian:bookworm-slim
          ports:
            - containerPort: 80
          volumeMounts:
            - name: dev-vscode-data
              mountPath: /vscode-server
          env:
            - name: PASSWORD
              valueFrom:
                secretKeyRef:
                  name: dev-vscode
                  key: password
          command: ["/bin/sh", "-c", "sleep infinity"]
  volumeClaimTemplates:
    - metadata:
        name: dev-vscode-data
      spec:
        storageClassName: objectfs-standard
        accessModes:
          - ReadWriteOnce
        resources:
          requests:
            storage: 10Gi
`, ns, ns, ns, ns)

	t.Logf("Applying VSCode StatefulSet with ObjectFS StorageClass in namespace %s", ns)
	h.KubectlApplyContent("dev-vscode", statefulSetYaml)

	podName := "dev-vscode-0"
	t.Logf("Waiting for StatefulSet pod %s to be ready", podName)
	if err := h.WaitForPodReady(podName, ns, 2*time.Minute); err != nil {
		t.Logf("Events:\n%s\n", h.GetEvents(ns))
		t.Logf("Controller Logs:\n%s\n", h.GetPodLogsByName("objectfs-controller-0", "default"))
		t.Logf("Node Daemon Logs:\n%s\n", h.GetPodLogs("app=objectfs-node-daemon", "default"))
		t.Fatalf("StatefulSet pod %s failed to start: %v", podName, err)
	}

	// Step 1: Write a file via kubectl exec inside the pod
	t.Logf("Writing data inside %s", podName)
	_, err := h.RunInPod(podName, ns, "/bin/sh", "-c", "echo 'hello from vscode statefulset' > /vscode-server/workspace/test.txt && sync")
	if err != nil {
		t.Fatalf("Failed to write test.txt in %s: %v", podName, err)
	}

	// Verify the file can be read back
	out, err := h.RunInPod(podName, ns, "cat", "/vscode-server/workspace/test.txt")
	if err != nil {
		t.Fatalf("Failed to read test.txt in %s: %v", podName, err)
	}
	if !strings.Contains(out, "hello from vscode statefulset") {
		t.Fatalf("Expected content 'hello from vscode statefulset', got: %s", out)
	}

	// Step 2: Delete the pod and verify persistence across StatefulSet pod recreation
	t.Logf("Deleting pod %s to test data persistence", podName)
	h.DeletePod(podName, ns)

	t.Logf("Waiting for recreated pod %s to be ready", podName)
	if err := h.WaitForPodReady(podName, ns, 2*time.Minute); err != nil {
		t.Logf("Events:\n%s\n", h.GetEvents(ns))
		t.Fatalf("Recreated pod %s failed to become ready: %v", podName, err)
	}

	// Verify persisted data after recreation
	t.Logf("Reading data from recreated pod %s", podName)
	outAfterRestart, err := h.RunInPod(podName, ns, "cat", "/vscode-server/workspace/test.txt")
	if err != nil {
		t.Fatalf("Failed to read test.txt after recreation in %s: %v", podName, err)
	}
	if !strings.Contains(outAfterRestart, "hello from vscode statefulset") {
		t.Fatalf("Expected content 'hello from vscode statefulset' after recreation, got: %s", outAfterRestart)
	}

	// Step 3: Delete StatefulSet, pod, and PVC, and verify that the PV is deleted by csi-provisioner
	t.Logf("Verifying PV cleanup upon PVC deletion")
	pvcName := "dev-vscode-data-" + podName
	pvName, err := h.GetPVForPVC(pvcName, ns)
	if err != nil {
		t.Fatalf("Failed to get PV for PVC %s: %v", pvcName, err)
	}
	t.Logf("Discovered bound PV %s for PVC %s", pvName, pvcName)

	t.Logf("Deleting StatefulSet dev-vscode")
	h.DeleteStatefulSet("dev-vscode", ns)

	t.Logf("Deleting Pod %s", podName)
	h.DeletePod(podName, ns)

	t.Logf("Deleting PVC %s", pvcName)
	h.DeletePVC(pvcName, ns)

	t.Logf("Waiting for PV %s to disappear", pvName)
	if err := h.WaitForPVDeleted(pvName, 60*time.Second); err != nil {
		t.Logf("Events:\n%s\n", h.GetEvents(ns))
		t.Logf("Controller Logs:\n%s\n", h.GetPodLogsByName("objectfs-controller-0", "default"))
		t.Fatalf("PersistentVolume %s failed to be deleted: %v", pvName, err)
	}

	t.Logf("Successfully verified ObjectFS dynamic provisioning with StatefulSet and PV cleanup!")
}

func TestObjectFSPersistentE2E(t *testing.T) {
	requireE2EGroup(t, "core")

	h, ns := setupSharedObjectFSCluster(t)
	volumeID := fmt.Sprintf("persistent-s3-test-vol-%s", ns)

	// Step 1: Write initial files to the persistent volume
	pod1Name := "persistent-pod-1"
	pod1Yaml := fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: %s
  namespace: %s
spec:
  restartPolicy: Never
  containers:
    - name: app
      image: alpine
      command: ["/bin/sh", "-c", "echo 'root-persistent-data' > /data/root.txt && mkdir /data/models && echo 'model-weights-persisted' > /data/models/weights.bin && sync && sleep 10"]
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      csi:
        driver: objectfs.labs.gke.io
        volumeAttributes:
          volumeID: %s
`, pod1Name, ns, volumeID)

	t.Logf("Creating Pod 1 (writing data before controller restart) in namespace %s", ns)
	h.KubectlApplyContent(pod1Name, pod1Yaml)
	if err := h.WaitForPodReady(pod1Name, ns, 1*time.Minute); err != nil {
		t.Logf("Events:\n%s\n", h.GetEvents(ns))
		t.Fatalf("Pod 1 failed to start: %v", err)
	}

	time.Sleep(5 * time.Second)
	t.Logf("Deleting Pod 1")
	h.DeletePod(pod1Name, ns)

	// Step 2: Restart ObjectFS Controller (simulating crash / pod restart)
	t.Logf("Restarting ObjectFS Controller pod to verify persistence across restarts")
	h.DeletePod("objectfs-controller-0", "default")

	// Wait for controller pod to be recreated and ready
	if err := h.WaitForStatefulSet("objectfs-controller", "default", 2*time.Minute); err != nil {
		t.Logf("Events:\n%s\n", h.GetEvents("default"))
		t.Fatalf("ObjectFS Controller failed to restart: %v", err)
	}

	// Step 3: Start Pod 2, read back existing data, and append new files
	pod2Name := "persistent-pod-2"
	pod2Yaml := fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: %s
  namespace: %s
spec:
  restartPolicy: Never
  containers:
    - name: app
      image: alpine
      command: ["/bin/sh", "-c", "cat /data/root.txt && cat /data/models/weights.bin && echo 'after-restart-file' > /data/models/config.json && echo 'updated-root-data' > /data/root.txt && sync && sleep 10"]
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      csi:
        driver: objectfs.labs.gke.io
        volumeAttributes:
          volumeID: %s
`, pod2Name, ns, volumeID)

	t.Logf("Creating Pod 2 (reading data after controller restart and making further writes) in namespace %s", ns)
	h.KubectlApplyContent(pod2Name, pod2Yaml)
	if err := h.WaitForPodReady(pod2Name, ns, 1*time.Minute); err != nil {
		t.Logf("Events:\n%s\n", h.GetEvents(ns))
		t.Logf("Controller Logs:\n%s\n", h.GetPodLogsByName("objectfs-controller-0", "default"))
		t.Fatalf("Pod 2 failed to start: %v", err)
	}

	time.Sleep(5 * time.Second)
	logs2 := h.GetPodLogsByName(pod2Name, ns)
	if !strings.Contains(logs2, "root-persistent-data") || !strings.Contains(logs2, "model-weights-persisted") {
		t.Fatalf("Pod 2 did not recover pre-restart data! Logs:\n%s", logs2)
	}
	t.Logf("Pod 2 successfully verified recovered data across controller restart!")
	h.DeletePod(pod2Name, ns)

	// Step 4: Restart ObjectFS Controller a second time
	t.Logf("Restarting ObjectFS Controller a second time")
	h.DeletePod("objectfs-controller-0", "default")
	if err := h.WaitForStatefulSet("objectfs-controller", "default", 2*time.Minute); err != nil {
		t.Logf("Events:\n%s\n", h.GetEvents("default"))
		t.Fatalf("ObjectFS Controller failed to restart (2nd time): %v", err)
	}

	// Step 5: Start Pod 3 to verify both original and post-restart modifications
	pod3Name := "persistent-pod-3"
	pod3Yaml := fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: %s
  namespace: %s
spec:
  containers:
    - name: app
      image: alpine
      command: ["/bin/sh", "-c", "cat /data/root.txt && cat /data/models/config.json && sleep 3600"]
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      csi:
        driver: objectfs.labs.gke.io
        volumeAttributes:
          volumeID: %s
`, pod3Name, ns, volumeID)

	t.Logf("Creating Pod 3 (verifying state after 2nd controller restart) in namespace %s", ns)
	h.KubectlApplyContent(pod3Name, pod3Yaml)
	if err := h.WaitForPodReady(pod3Name, ns, 1*time.Minute); err != nil {
		t.Fatalf("Pod 3 failed to start: %v", err)
	}

	outRoot, err := h.RunInPod(pod3Name, ns, "cat", "/data/root.txt")
	if err != nil {
		t.Fatalf("Failed to read /data/root.txt in Pod 3: %v", err)
	}
	if !strings.Contains(outRoot, "updated-root-data") {
		t.Fatalf("Expected updated-root-data, got: %s", outRoot)
	}

	outConfig, err := h.RunInPod(pod3Name, ns, "cat", "/data/models/config.json")
	if err != nil {
		t.Fatalf("Failed to read /data/models/config.json in Pod 3: %v", err)
	}
	if !strings.Contains(outConfig, "after-restart-file") {
		t.Fatalf("Expected after-restart-file, got: %s", outConfig)
	}

	h.DeletePod(pod3Name, ns)
	t.Logf("Successfully verified ObjectFS persistent deployment with S3 backend, WAL buffer, and restart recovery!")
}

func TestObjectFSXAttrE2E(t *testing.T) {
	requireE2EGroup(t, "core")

	h, ns := setupSharedObjectFSCluster(t)
	volumeID := fmt.Sprintf("objectfs-xattr-vol-%s", ns)

	podName := "objectfs-xattr-pod"
	podYaml := fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: %s
  namespace: %s
spec:
  restartPolicy: Never
  containers:
    - name: app
      image: images.local/objectfs-node-daemon:e2e
      imagePullPolicy: Never
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
`, podName, ns, volumeID)

	h.KubectlApplyContent(podName, podYaml)
	if err := h.WaitForPodReady(podName, ns, 2*time.Minute); err != nil {
		t.Fatalf("Pod %s failed to become ready: %v", podName, err)
	}
	defer h.DeletePod(podName, ns)

	// Step 1: Create a file
	_, err := h.RunInPod(podName, ns, "/bin/sh", "-c", "echo 'xattr content' > /data/file.txt")
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Step 2: Set a user.* extended attribute
	_, err = h.RunInPod(podName, ns, "setfattr", "-n", "user.testcomment", "-v", "test-val-123", "/data/file.txt")
	if err != nil {
		t.Fatalf("failed to setfattr: %v", err)
	}

	// Step 3: Read it back
	out, err := h.RunInPod(podName, ns, "getfattr", "--absolute-names", "-n", "user.testcomment", "--only-values", "/data/file.txt")
	if err != nil {
		t.Fatalf("failed to getfattr: %v", err)
	}
	if strings.TrimSpace(out) != "test-val-123" {
		t.Fatalf("expected 'test-val-123', got: %q", out)
	}

	// Step 4: cp -a the file and confirm copy carries the xattr
	_, err = h.RunInPod(podName, ns, "cp", "-a", "/data/file.txt", "/data/file_copy.txt")
	if err != nil {
		t.Fatalf("failed to cp -a: %v", err)
	}

	copyOut, err := h.RunInPod(podName, ns, "getfattr", "--absolute-names", "-n", "user.testcomment", "--only-values", "/data/file_copy.txt")
	if err != nil {
		t.Fatalf("failed to getfattr on copy: %v", err)
	}
	if strings.TrimSpace(copyOut) != "test-val-123" {
		t.Fatalf("expected 'test-val-123' on copied file, got: %q", copyOut)
	}
}

var (
	sharedMultiNodeClusterOnce sync.Once
	sharedMultiNodeErr         error
	sharedMultiNodeCreated     bool
	sharedMultiNode1           string
	sharedMultiNode2           string
)

const sharedMultiNodeClusterName = "objectfs-shared-multinode"

// isSharedClusterSystemPod returns true if the pod name matches one of the long-lived
// system workloads in the default namespace that should not be cleaned up between tests.
func isSharedClusterSystemPod(p string) bool {
	systemPrefixes := []string{
		"fakes3",
		"objectfs-controller",
		"wal-buffer",
		"objectfs-node-daemon",
		"objectfs-fuse-holder",
		"objectfs-admin-pod",
	}
	for _, prefix := range systemPrefixes {
		if strings.HasPrefix(p, prefix) {
			return true
		}
	}
	return false
}

func setupObjectFSMultiNodeCluster(t *testing.T) (*Harness, string, string) {
	t.Helper()

	sharedMultiNodeClusterOnce.Do(func() {
		// Clean up any stale cluster from a previous aborted local run
		if out, err := exec.Command("kind", "delete", "cluster", "--name", sharedMultiNodeClusterName).CombinedOutput(); err != nil {
			t.Logf("Note: clean up existing cluster %s returned: %v (%s)", sharedMultiNodeClusterName, err, strings.TrimSpace(string(out)))
		}

		var configBuilder strings.Builder
		configBuilder.WriteString("kind: Cluster\napiVersion: kind.x-k8s.io/v1alpha4\nnodes:\n- role: control-plane\n- role: worker\n- role: worker\n")
		tmpConfig, err := os.CreateTemp("", "kind-config-*.yaml")
		if err != nil {
			sharedMultiNodeErr = fmt.Errorf("failed to create temp kind config: %w", err)
			return
		}
		defer func() {
			if err := os.Remove(tmpConfig.Name()); err != nil && !os.IsNotExist(err) {
				t.Logf("Warning: failed to remove %s: %v", tmpConfig.Name(), err)
			}
		}()
		if _, err := tmpConfig.WriteString(configBuilder.String()); err != nil {
			sharedMultiNodeErr = fmt.Errorf("failed to write kind config: %w", err)
			return
		}
		if err := tmpConfig.Close(); err != nil {
			sharedMultiNodeErr = fmt.Errorf("failed to close temp kind config: %w", err)
			return
		}

		cmd := exec.Command("kind", "create", "cluster", "--name", sharedMultiNodeClusterName, "--config", tmpConfig.Name())
		if out, err := cmd.CombinedOutput(); err != nil {
			sharedMultiNodeErr = fmt.Errorf("failed to create kind cluster %s: %w\n%s", sharedMultiNodeClusterName, err, strings.TrimSpace(string(out)))
			return
		}
		sharedMultiNodeCreated = true

		if out, err := exec.Command("kind", "export", "kubeconfig", "--name", sharedMultiNodeClusterName).CombinedOutput(); err != nil {
			sharedMultiNodeErr = fmt.Errorf("failed to export kubeconfig for %s: %w (%s)", sharedMultiNodeClusterName, err, strings.TrimSpace(string(out)))
			return
		}

		initH := NewHarness(t, sharedMultiNodeClusterName)
		nodes := initH.GetWorkerNodeNames()
		if len(nodes) < 2 {
			nodes = initH.GetNodeNames()
		}
		if len(nodes) < 2 {
			sharedMultiNodeErr = fmt.Errorf("expected at least 2 nodes in kind cluster, found %d: %v", len(nodes), nodes)
			return
		}
		sharedMultiNode1 = nodes[0]
		sharedMultiNode2 = nodes[1]

		gitRoot := initH.GetGitRoot()

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
          image: fakes3
          args: ["--listen=0.0.0.0:9000", "--buckets=objectfs-bucket", "--quiet"]
          ports:
            - containerPort: 9000
`

		s3BackendURL := "s3://objectfs-bucket?endpoint=http://fakes3:9000&region=us-east-1&s3ForcePathStyle=true"

		// ObjectFS manifest with S3 backend and pinned initially to node1
		objectfsManifestPath := filepath.Join(gitRoot, "k8s/objectfs.yaml")
		b, err := os.ReadFile(objectfsManifestPath)
		if err != nil {
			sharedMultiNodeErr = fmt.Errorf("failed to read objectfs manifest: %w", err)
			return
		}
		objectfsManifest := string(b)
		objectfsManifest = strings.ReplaceAll(objectfsManifest, "namespace: kube-objectfs-system", "namespace: default")
		objectfsManifest = strings.ReplaceAll(objectfsManifest, `backend: ""`, fmt.Sprintf("backend: %q", s3BackendURL))

		awsEnvController := `env:
            - name: AWS_ACCESS_KEY_ID
              value: "fakes3"
            - name: AWS_SECRET_ACCESS_KEY
              value: "fakes3"
            - name: AWS_REGION
              value: "us-east-1"
            - name: OBJECT_STORAGE_BACKEND`
		objectfsManifest = strings.ReplaceAll(objectfsManifest, "env:\n            - name: OBJECT_STORAGE_BACKEND", awsEnvController)

		// Pin controller to node1 initially and add toleration
		nodeSelectorNode1 := fmt.Sprintf("nodeSelector:\n        kubernetes.io/hostname: %s\n      tolerations:\n        - key: \"node-role.kubernetes.io/control-plane\"\n          operator: \"Exists\"\n          effect: \"NoSchedule\"\n      serviceAccountName: objectfs-controller", sharedMultiNode1)
		objectfsManifest = strings.ReplaceAll(objectfsManifest, "serviceAccountName: objectfs-controller", nodeSelectorNode1)

		// WAL buffer manifest
		walManifestPath := filepath.Join(gitRoot, "k8s/wal.yaml")
		b, err = os.ReadFile(walManifestPath)
		if err != nil {
			sharedMultiNodeErr = fmt.Errorf("failed to read wal manifest: %w", err)
			return
		}
		walManifest := string(b)
		walManifest = strings.ReplaceAll(walManifest, "namespace: kube-objectfs-system", "namespace: default")
		walNodeSelectorNode1 := fmt.Sprintf("nodeSelector:\n        kubernetes.io/hostname: %s\n      tolerations:\n        - key: \"node-role.kubernetes.io/control-plane\"\n          operator: \"Exists\"\n          effect: \"NoSchedule\"\n      serviceAccountName: wal-buffer", sharedMultiNode1)
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

		initH.ApDeploy(t, map[string]string{
			"fakes3":     fakes3Manifest,
			"objectfs":   objectfsManifest,
			"wal-buffer": walManifest,
		})

		if err := initH.WaitForDeployment("fakes3", "default", 2*time.Minute); err != nil {
			sharedMultiNodeErr = fmt.Errorf("fakes3 deployment failed to start: %w", err)
			return
		}

		initH.RunCommand("kubectl", "wait", "--for=condition=complete", "job/objectfs-certsigner", "-n", "default", "--timeout=60s")

		if err := initH.WaitForStatefulSet("wal-buffer", "default", 2*time.Minute); err != nil {
			sharedMultiNodeErr = fmt.Errorf("wal-buffer failed to start: %w", err)
			return
		}
		if err := initH.WaitForStatefulSet("objectfs-controller", "default", 2*time.Minute); err != nil {
			sharedMultiNodeErr = fmt.Errorf("objectfs-controller failed to start on %s: %w", sharedMultiNode1, err)
			return
		}
		if err := initH.WaitForDaemonSet("objectfs-fuse-holder", "default", 2*time.Minute); err != nil {
			sharedMultiNodeErr = fmt.Errorf("objectfs-fuse-holder failed to start: %w", err)
			return
		}
		if err := initH.WaitForDaemonSet("objectfs-node-daemon", "default", 2*time.Minute); err != nil {
			sharedMultiNodeErr = fmt.Errorf("objectfs-node-daemon failed to start: %w", err)
			return
		}

		adminPodYaml := `
apiVersion: v1
kind: Pod
metadata:
  name: objectfs-admin-pod
  namespace: default
  labels:
    app: objectfs-admin
spec:
  containers:
    - name: client
      image: images.local/objectfs-controller:e2e
      imagePullPolicy: Never
      command: ["/bin/sh", "-c", "sleep 36000"]
      volumeMounts:
        - name: certs-client
          mountPath: /etc/objectfs/certs/client
          readOnly: true
  volumes:
    - name: certs-client
      secret:
        secretName: objectfs-admin-client
        items:
          - key: tls.key
            path: tls.key
          - key: tls.crt
            path: tls.crt
          - key: ca.crt
            path: trust/ca.crt
`
		initH.KubectlApplyContent("objectfs-admin-pod", adminPodYaml)
		if err := initH.WaitForPodReady("objectfs-admin-pod", "default", 2*time.Minute); err != nil {
			sharedMultiNodeErr = fmt.Errorf("objectfs-admin-pod failed to start: %w", err)
			return
		}
	})

	if sharedMultiNodeErr != nil {
		t.Fatalf("setupObjectFSMultiNodeCluster failed: %v", sharedMultiNodeErr)
	}

	h := NewHarness(t, sharedMultiNodeClusterName)
	node1 := sharedMultiNode1
	node2 := sharedMultiNode2

	// Clean up any leftover test pods or PVCs in default from a prior test
	pods := h.getPodNames("", "default")
	for _, p := range pods {
		if !isSharedClusterSystemPod(p) {
			if err := exec.Command("kubectl", "delete", "pod", p, "-n", "default", "--force", "--grace-period=0", "--ignore-not-found").Run(); err != nil {
				t.Logf("Warning: failed to delete leftover pod %s in default: %v", p, err)
			}
		}
	}
	outPVC, err := exec.Command("kubectl", "get", "pvc", "-n", "default", "-o", "jsonpath={.items[*].metadata.name}").CombinedOutput()
	if err != nil {
		t.Logf("Warning: failed to query pvcs in default: %v", err)
	} else {
		for _, pvc := range strings.Fields(string(outPVC)) {
			if strings.HasPrefix(pvc, "data-dir-wal-buffer") {
				continue
			}
			if err := exec.Command("kubectl", "delete", "pvc", pvc, "-n", "default", "--ignore-not-found").Run(); err != nil {
				t.Logf("Warning: failed to delete leftover pvc %s in default: %v", pvc, err)
			}
		}
	}

	// Restore controller nodeSelector to node1 and replicas to 1 (in case a prior test moved or scaled it down)
	patchNode1JSON := fmt.Sprintf(`{"spec":{"replicas":1,"template":{"spec":{"nodeSelector":{"kubernetes.io/hostname":%q}}}}}`, node1)
	h.RunCommand("kubectl", "patch", "statefulset", "objectfs-controller", "-n", "default", "-p", patchNode1JSON)

	// Ensure system statefulsets are back and healthy before the test begins
	if err := h.WaitForStatefulSet("wal-buffer", "default", 2*time.Minute); err != nil {
		t.Fatalf("WAL buffer not ready: %v", err)
	}
	if err := h.WaitForStatefulSet("objectfs-controller", "default", 2*time.Minute); err != nil {
		t.Fatalf("ObjectFS Controller not ready on %s: %v", node1, err)
	}
	if err := h.WaitForDaemonSet("objectfs-fuse-holder", "default", 2*time.Minute); err != nil {
		t.Fatalf("ObjectFS FUSE Holder not ready: %v", err)
	}
	if err := h.WaitForDaemonSet("objectfs-node-daemon", "default", 2*time.Minute); err != nil {
		t.Fatalf("ObjectFS Node Daemon not ready: %v", err)
	}

	t.Cleanup(func() {
		if t.Failed() {
			h.DumpDiagnosticLogs(t)
			h.CollectArtifacts(t.Name())
		}
		// If controller was scaled down or moved during a failed test, restore it
		restoreJSON := fmt.Sprintf(`{"spec":{"replicas":1,"template":{"spec":{"nodeSelector":{"kubernetes.io/hostname":%q}}}}}`, node1)
		if err := exec.Command("kubectl", "patch", "statefulset", "objectfs-controller", "-n", "default", "-p", restoreJSON).Run(); err != nil {
			t.Logf("Warning: failed to restore objectfs-controller on cleanup: %v", err)
		}
	})

	return h, node1, node2
}

func retryCatInPod(h *Harness, pod, path string, timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	var lastOut string
	var lastErr error
	for time.Now().Before(deadline) {
		lastOut, lastErr = h.RunInPod(pod, "default", "cat", path)
		if lastErr == nil {
			return lastOut, nil
		}
		if strings.Contains(strings.ToLower(lastOut), "resource busy") || strings.Contains(strings.ToLower(fmt.Sprintf("%v", lastErr)), "resource busy") {
			return lastOut, fmt.Errorf("unexpected EBUSY / Resource busy error: %v (out: %s)", lastErr, lastOut)
		}
		time.Sleep(500 * time.Millisecond)
	}
	return lastOut, fmt.Errorf("timed out reading %s after %v: last err=%v out=%s", path, timeout, lastErr, lastOut)
}

func TestObjectFSMultiNodeFailoverE2E(t *testing.T) {
	requireE2EGroup(t, "multinode")

	h, node1, node2 := setupObjectFSMultiNodeCluster(t)
	t.Logf("Running 2-node failover test across Node 1 (%s) and Node 2 (%s)", node1, node2)

	volumeID := "failover-s3-test-vol"

	// Step 1: Start a long-running writer pod on node1 that writes files and fsyncs
	writerPodName := "failover-writer-pod"
	writerPodYaml := fmt.Sprintf(`
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
      command: ["/bin/sh", "-c", "echo 'node1-file-content' > /data/file1.txt && mkdir /data/nested && echo 'nested-data-1' > /data/nested/data.txt && sync && sleep 3600"]
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      csi:
        driver: objectfs.labs.gke.io
        volumeAttributes:
          volumeID: %s
`, writerPodName, node1, volumeID)

	t.Logf("Creating writer pod on %s (writing data while keeping pod alive)", node1)
	h.KubectlApplyContent(writerPodName, writerPodYaml)
	if err := h.WaitForPodReady(writerPodName, "default", 1*time.Minute); err != nil {
		t.Fatalf("Writer pod failed to start: %v", err)
	}

	time.Sleep(3 * time.Second)

	// Step 2: Force-kill controller pod while writer pod is STILL RUNNING (no pod deletion / no snapshot push)
	t.Logf("Force-killing ObjectFS Controller pod on %s while writer pod is still running", node1)
	h.RunCommand("kubectl", "delete", "pod", "objectfs-controller-0", "-n", "default", "--force", "--grace-period=0")

	// Step 3: Move controller to node2 by patching nodeSelector
	t.Logf("Moving ObjectFS Controller to Node 2 (%s)", node2)
	patchJSON := fmt.Sprintf(`{"spec":{"template":{"spec":{"nodeSelector":{"kubernetes.io/hostname":%q}}}}}`, node2)
	h.RunCommand("kubectl", "patch", "statefulset", "objectfs-controller", "-n", "default", "-p", patchJSON)

	// Wait for controller to be ready on node2
	if err := h.WaitForStatefulSet("objectfs-controller", "default", 2*time.Minute); err != nil {
		t.Fatalf("ObjectFS Controller failed to restart on %s: %v", node2, err)
	}

	// Step 4: Verify files are intact on node2, and perform new writes on node2
	readerPod2Name := "failover-reader-node2"
	readerPod2Yaml := fmt.Sprintf(`
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
      command: ["/bin/sh", "-c", "cat /data/file1.txt && cat /data/nested/data.txt && echo 'node2-file-content' > /data/file2.txt && sync && sleep 3600"]
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      csi:
        driver: objectfs.labs.gke.io
        volumeAttributes:
          volumeID: %s
`, readerPod2Name, node2, volumeID)

	t.Logf("Creating reader/writer pod on Node 2 (%s) to verify recovery from wal-buffer", node2)
	h.KubectlApplyContent(readerPod2Name, readerPod2Yaml)
	if err := h.WaitForPodReady(readerPod2Name, "default", 1*time.Minute); err != nil {
		t.Fatalf("Pod on Node 2 failed to start: %v", err)
	}

	time.Sleep(1 * time.Second)
	outF1, err := retryCatInPod(h, readerPod2Name, "/data/file1.txt", 30*time.Second)
	if err != nil || !strings.Contains(outF1, "node1-file-content") {
		t.Fatalf("Expected 'node1-file-content' on Node 2, got: %q (err: %v)", outF1, err)
	}

	outNested, err := retryCatInPod(h, readerPod2Name, "/data/nested/data.txt", 30*time.Second)
	if err != nil || !strings.Contains(outNested, "nested-data-1") {
		t.Fatalf("Expected 'nested-data-1' on Node 2, got: %q (err: %v)", outNested, err)
	}
	t.Logf("Successfully verified data recovered from wal-buffer on Node 2!")

	// Step 5: Force-kill controller pod on node2 and move it BACK to node1 (whose hostPath segments are now stale!)
	t.Logf("Force-killing controller on Node 2 and moving BACK to Node 1 (%s) with stale local segments", node1)
	h.RunCommand("kubectl", "delete", "pod", "objectfs-controller-0", "-n", "default", "--force", "--grace-period=0")

	patchNode1JSON := fmt.Sprintf(`{"spec":{"template":{"spec":{"nodeSelector":{"kubernetes.io/hostname":%q}}}}}`, node1)
	h.RunCommand("kubectl", "patch", "statefulset", "objectfs-controller", "-n", "default", "-p", patchNode1JSON)

	// Repeatedly read file in writer pod while controller is restarting to verify no EBUSY errors
	readDone := make(chan struct{})
	readErrors := make(chan error, 100)
	go func() {
		defer close(readDone)
		for i := 0; i < 20; i++ {
			out, err := h.RunInPod(writerPodName, "default", "cat", "/data/file1.txt")
			if err != nil {
				if strings.Contains(strings.ToLower(out), "resource busy") || strings.Contains(strings.ToLower(fmt.Sprintf("%v", err)), "resource busy") {
					readErrors <- fmt.Errorf("unexpected EBUSY / Resource busy while controller restarting: %v (%s)", err, out)
				}
			}
			time.Sleep(200 * time.Millisecond)
		}
	}()

	if err := h.WaitForStatefulSet("objectfs-controller", "default", 2*time.Minute); err != nil {
		t.Fatalf("ObjectFS Controller failed to restart back on %s: %v", node1, err)
	}
	<-readDone
	close(readErrors)
	for rErr := range readErrors {
		t.Fatalf("Read during restart failed: %v", rErr)
	}

	// Step 6: Verify all files (node1 writes + node2 writes) are intact on node1 with retries
	outF2, err := retryCatInPod(h, writerPodName, "/data/file2.txt", 30*time.Second)
	if err != nil || !strings.Contains(outF2, "node2-file-content") {
		t.Fatalf("Expected 'node2-file-content' back on Node 1, got: %q (err: %v)", outF2, err)
	}

	// Write additional file back on node1 to ensure sequence numbering continues cleanly.
	// Under the stage-and-sync model (#245), even if the close-triggered sync is in-flight when the controller
	// restarts, the client holds the chunks dirty and serves subsequent reads from its cache.
	t.Logf("Writing final file on Node 1 (%s)", node1)
	_, err = h.RunInPod(writerPodName, "default", "/bin/sh", "-c", "echo 'node1-final-content' > /data/final.txt && sync")
	if err != nil {
		t.Fatalf("Failed to write final.txt on Node 1: %v", err)
	}

	outFinal, err := retryCatInPod(h, writerPodName, "/data/final.txt", 30*time.Second)
	if err != nil || !strings.Contains(outFinal, "node1-final-content") {
		t.Fatalf("Expected 'node1-final-content', got: %q (err: %v)", outFinal, err)
	}

	// Step 7: Restart controller on Node 1 once more and verify all files persist without duplicate sequence errors
	t.Logf("Restarting controller on Node 1 once more to verify persistence after truncation")
	h.RunCommand("kubectl", "delete", "pod", "objectfs-controller-0", "-n", "default", "--force", "--grace-period=0")
	if err := h.WaitForStatefulSet("objectfs-controller", "default", 2*time.Minute); err != nil {
		t.Fatalf("ObjectFS Controller failed to restart on Node 1: %v", err)
	}

	if logs := h.getContainerLogs("objectfs-controller-0", "objectfs-controller", "default", false); logs != "" {
		t.Logf("ObjectFS Controller post-restart recovery logs:\n%s\n", logs)
	}

	outFinal2, err := retryCatInPod(h, writerPodName, "/data/final.txt", 30*time.Second)
	if err != nil || !strings.Contains(outFinal2, "node1-final-content") {
		t.Fatalf("Expected 'node1-final-content' after 2nd restart on Node 1, got: %q (err: %v)", outFinal2, err)
	}
	outF1Final, err := retryCatInPod(h, writerPodName, "/data/file1.txt", 30*time.Second)
	if err != nil || !strings.Contains(outF1Final, "node1-file-content") {
		t.Fatalf("Expected 'node1-file-content' after 2nd restart on Node 1, got: %q (err: %v)", outF1Final, err)
	}
	outF2Final, err := retryCatInPod(h, writerPodName, "/data/file2.txt", 30*time.Second)
	if err != nil || !strings.Contains(outF2Final, "node2-file-content") {
		t.Fatalf("Expected 'node2-file-content' after 2nd restart on Node 1, got: %q (err: %v)", outF2Final, err)
	}

	h.DeletePod(readerPod2Name, "default")
	h.DeletePod(writerPodName, "default")
	t.Logf("Successfully verified ObjectFS multi-node failover and stale segment recovery across nodes!")
}

// watchVolumeSubscribed reports whether logs contain a WatchVolume subscription line for the given volume and node.
func watchVolumeSubscribed(logs, volumeID, nodeID string) bool {
	return countWatchVolumeSubscriptions(logs, volumeID, nodeID) > 0
}

// countWatchVolumeSubscriptions counts WatchVolume subscription lines for the given volume and node in logs.
func countWatchVolumeSubscriptions(logs, volumeID, nodeID string) int {
	count := 0
	volMatch := fmt.Sprintf("volume=%q", volumeID)
	nodeMatch := fmt.Sprintf("node=%q", nodeID)
	legacyMatch := fmt.Sprintf("WatchVolume subscription established for volume %s, node %s", volumeID, nodeID)
	for _, line := range strings.Split(logs, "\n") {
		if strings.Contains(line, legacyMatch) {
			count++
		} else if strings.Contains(line, "WatchVolume subscription established") &&
			strings.Contains(line, volMatch) &&
			strings.Contains(line, nodeMatch) {
			count++
		}
	}
	return count
}

func TestObjectFSCrossNodeVisibilityE2E(t *testing.T) {
	requireE2EGroup(t, "multinode")

	h, node1, node2 := setupObjectFSMultiNodeCluster(t)
	volumeID := "cross-node-vis-vol"

	t.Cleanup(func() {
		if t.Failed() {
			h.DumpDiagnosticLogs(t)
			t.Logf("Node Daemon Logs:\n%s\n", h.GetPodLogs("app=objectfs-node-daemon", "default"))
			t.Logf("Controller Logs:\n%s\n", h.GetPodLogsByName("objectfs-controller-0", "default"))
		}
	})

	pod1Name := "cross-vis-pod-node1"
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
`, pod1Name, node1, volumeID)

	pod2Name := "cross-vis-pod-node2"
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
`, pod2Name, node2, volumeID)

	t.Logf("Starting Pod 1 on Node 1 (%s) and Pod 2 on Node 2 (%s)", node1, node2)
	h.KubectlApplyContent(pod1Name, pod1Yaml)
	h.KubectlApplyContent(pod2Name, pod2Yaml)

	if err := h.WaitForPodReady(pod1Name, "default", 1*time.Minute); err != nil {
		t.Fatalf("Pod 1 failed to become ready: %v", err)
	}
	if err := h.WaitForPodReady(pod2Name, "default", 1*time.Minute); err != nil {
		t.Fatalf("Pod 2 failed to become ready: %v", err)
	}

	// Warm both mounts: have each do one write + fsync + read so mounts and watch streams are initialized
	t.Logf("Warming mounts on Pod 1 and Pod 2")
	_, err := h.RunInPod(pod1Name, "default", "/bin/sh", "-c", "echo 'warm-pod1' > /data/warm1.txt && sync /data/warm1.txt && cat /data/warm1.txt")
	if err != nil {
		t.Fatalf("Warm write on Pod 1 failed: %v", err)
	}
	_, err = h.RunInPod(pod2Name, "default", "/bin/sh", "-c", "echo 'warm-pod2' > /data/warm2.txt && sync /data/warm2.txt && cat /data/warm2.txt")
	if err != nil {
		t.Fatalf("Warm write on Pod 2 failed: %v", err)
	}

	// Prove the precondition: poll until both node daemons show WatchVolume subscription established for this volume
	t.Logf("Waiting for WatchVolume subscriptions to be established on both node daemons for volume %s", volumeID)
	deadline := time.Now().Add(30 * time.Second)
	var daemonLogs string
	subscribed := false
	for time.Now().Before(deadline) {
		daemonLogs = h.GetPodLogs("app=objectfs-node-daemon", "default")
		hasNode1 := watchVolumeSubscribed(daemonLogs, volumeID, node1)
		hasNode2 := watchVolumeSubscribed(daemonLogs, volumeID, node2)
		if hasNode1 && hasNode2 {
			subscribed = true
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if !subscribed {
		t.Fatalf("Precondition failed: WatchVolume subscriptions were not established on both nodes within 30s for volume %s (logs: %s)", volumeID, daemonLogs)
	}
	t.Logf("Both node daemons confirmed subscribed to WatchVolume for %s", volumeID)

	// Short settle delay so watcher goroutines are idle and in steady state
	time.Sleep(500 * time.Millisecond)

	// Check 1: Overwrite and immediate cat & stat (Node 2 writes, Node 1 reads with no sleeps)
	t.Logf("1. Testing immediate visibility on overwrite: Pod 1 primes cache, Pod 2 overwrites")
	_, err = h.RunInPod(pod1Name, "default", "/bin/sh", "-c", "echo 'initial-val-1' > /data/watched1.txt && sync /data/watched1.txt && cat /data/watched1.txt")
	if err != nil {
		t.Fatalf("Failed to prime watched1.txt on Pod 1: %v", err)
	}
	_, err = h.RunInPod(pod2Name, "default", "/bin/sh", "-c", "echo 'updated-val-from-pod2' > /data/watched1.txt && sync /data/watched1.txt")
	if err != nil {
		t.Fatalf("Failed to overwrite watched1.txt from Pod 2: %v", err)
	}
	out1, err := h.RunInPod(pod1Name, "default", "cat", "/data/watched1.txt")
	if err != nil || !strings.Contains(out1, "updated-val-from-pod2") {
		t.Fatalf("Immediate WatchVolume invalidation failed: Pod 1 did not see updated watched1.txt: %v (out: %s)", err, out1)
	}
	outStat1, err := h.RunInPod(pod1Name, "default", "stat", "-c", "%s", "/data/watched1.txt")
	if err != nil || strings.TrimSpace(outStat1) != fmt.Sprintf("%d", len("updated-val-from-pod2\n")) {
		t.Fatalf("Immediate stat invalidation failed: Pod 1 stat returned %q (err: %v)", outStat1, err)
	}

	// Check 2: Reverse overwrite and immediate cat & stat (Node 1 writes, Node 2 reads with no sleeps)
	t.Logf("2. Testing immediate visibility on reverse overwrite: Pod 2 primes cache, Pod 1 overwrites")
	_, err = h.RunInPod(pod2Name, "default", "/bin/sh", "-c", "echo 'initial-val-2' > /data/watched2.txt && sync /data/watched2.txt && cat /data/watched2.txt")
	if err != nil {
		t.Fatalf("Failed to prime watched2.txt on Pod 2: %v", err)
	}
	_, err = h.RunInPod(pod1Name, "default", "/bin/sh", "-c", "echo 'updated-val-from-pod1' > /data/watched2.txt && sync /data/watched2.txt")
	if err != nil {
		t.Fatalf("Failed to overwrite watched2.txt from Pod 1: %v", err)
	}
	out2, err := h.RunInPod(pod2Name, "default", "cat", "/data/watched2.txt")
	if err != nil || !strings.Contains(out2, "updated-val-from-pod1") {
		t.Fatalf("Immediate WatchVolume invalidation failed: Pod 2 did not see updated watched2.txt: %v (out: %s)", err, out2)
	}
	outStat2, err := h.RunInPod(pod2Name, "default", "stat", "-c", "%s", "/data/watched2.txt")
	if err != nil || strings.TrimSpace(outStat2) != fmt.Sprintf("%d", len("updated-val-from-pod1\n")) {
		t.Fatalf("Immediate stat invalidation failed: Pod 2 stat returned %q (err: %v)", outStat2, err)
	}

	// Check 3: Newly created file immediate visibility
	t.Logf("3. Testing newly created file immediate visibility: Pod 1 probes non-existent file, Pod 2 creates")
	_, _ = h.RunInPod(pod1Name, "default", "/bin/sh", "-c", "cat /data/new_file.txt 2>/dev/null || true")
	_, err = h.RunInPod(pod2Name, "default", "/bin/sh", "-c", "echo 'new-file-content' > /data/new_file.txt && sync /data/new_file.txt")
	if err != nil {
		t.Fatalf("Failed to create new_file.txt from Pod 2: %v", err)
	}
	outNew, err := h.RunInPod(pod1Name, "default", "cat", "/data/new_file.txt")
	if err != nil || !strings.Contains(outNew, "new-file-content") {
		t.Fatalf("Immediate visibility failed: Pod 1 did not see new_file.txt: %v (out: %s)", err, outNew)
	}
	outLs, err := h.RunInPod(pod1Name, "default", "ls", "/data")
	if err != nil || !strings.Contains(outLs, "new_file.txt") {
		t.Fatalf("Immediate ls failed: Pod 1 ls did not list new_file.txt: %v (out: %s)", err, outLs)
	}

	// Check 4: Immediate visibility through an already-open file descriptor
	t.Logf("4. Testing immediate visibility through an already-open file descriptor")
	_, err = h.RunInPod(pod1Name, "default", "/bin/sh", "-c", "echo 'open-initial-content' > /data/open_file.txt && sync /data/open_file.txt")
	if err != nil {
		t.Fatalf("Failed to write open_file.txt: %v", err)
	}

	// Start a background reader in Pod 1 that holds descriptor 3 open and reads initial data
	_, err = h.RunInPod(pod1Name, "default", "/bin/sh", "-c", `(
		exec 3< /data/open_file.txt
		head -n 1 <&3 > /tmp/open_initial.txt
		echo ready > /tmp/open_ready
		i=0
		while [ ! -f /tmp/open_trigger ] && [ $i -lt 60 ]; do
			usleep 50000 2>/dev/null || sleep 0.05
			i=$((i+1))
		done
		dd bs=65536 <&3 > /tmp/open_updated.txt 2>/dev/null
		echo done > /tmp/open_done
	) </dev/null >/dev/null 2>&1 &`)
	if err != nil {
		t.Fatalf("Failed to start open descriptor process on Pod 1: %v", err)
	}

	// Wait for the background process to be ready with the file open
	var readyOut string
	for i := 0; i < 50; i++ {
		readyOut, err = h.RunInPod(pod1Name, "default", "cat", "/tmp/open_ready")
		if err == nil && strings.Contains(readyOut, "ready") {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !strings.Contains(readyOut, "ready") {
		t.Fatalf("Background process on Pod 1 failed to open file: %v (out: %s)", err, readyOut)
	}

	// Pod 2 appends new content to open_file.txt and syncs
	_, err = h.RunInPod(pod2Name, "default", "/bin/sh", "-c", "echo 'appended-across-nodes' >> /data/open_file.txt && sync /data/open_file.txt")
	if err != nil {
		t.Fatalf("Failed to append to open_file.txt from Pod 2: %v", err)
	}

	// Trigger Pod 1's already-open descriptor to read without any delay
	_, err = h.RunInPod(pod1Name, "default", "touch", "/tmp/open_trigger")
	if err != nil {
		t.Fatalf("Failed to trigger read on Pod 1: %v", err)
	}

	// Wait for background reader to complete
	var doneOut string
	for i := 0; i < 50; i++ {
		doneOut, err = h.RunInPod(pod1Name, "default", "cat", "/tmp/open_done")
		if err == nil && strings.Contains(doneOut, "done") {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !strings.Contains(doneOut, "done") {
		t.Fatalf("Background process on Pod 1 failed to complete read: %v (out: %s)", err, doneOut)
	}

	outOpenUpdated, err := h.RunInPod(pod1Name, "default", "cat", "/tmp/open_updated.txt")
	if err != nil || !strings.Contains(outOpenUpdated, "appended-across-nodes") {
		t.Fatalf("Immediate visibility on already-open file failed: expected 'appended-across-nodes', got: %q (err: %v)", outOpenUpdated, err)
	}

	// Also verify that stat from a fresh process reflects the appended size immediately
	expectedSize := len("open-initial-content\n") + len("appended-across-nodes\n")
	outStatOpen, err := h.RunInPod(pod1Name, "default", "stat", "-c", "%s", "/data/open_file.txt")
	if err != nil || strings.TrimSpace(outStatOpen) != fmt.Sprintf("%d", expectedSize) {
		t.Fatalf("Immediate stat invalidation on open file failed: expected size %d, got %q (err: %v)", expectedSize, outStatOpen, err)
	}

	// 5. Testing dentry revalidation across watcher reconnection gap:
	// A creates d/x, B lists d (dentry cached with long timeout),
	// kill B's watch stream (restart controller), A deletes d/x during gap,
	// wait for B's daemon to re-establish subscription, then ls d on B must not show x.
	t.Logf("5. Testing dentry revalidation across watcher reconnection gap")

	// Pod 1 (Client A) creates d/x and syncs
	_, err = h.RunInPod(pod1Name, "default", "/bin/sh", "-c", "mkdir -p /data/gap_dir && echo 'gap-content' > /data/gap_dir/gap_file.txt && sync /data/gap_dir/gap_file.txt")
	if err != nil {
		t.Fatalf("Failed to create gap_dir/gap_file.txt from Pod 1: %v", err)
	}

	// Pod 2 (Client B) lists d (dentry cached with 60s timeout)
	lsOut1, err := h.RunInPod(pod2Name, "default", "ls", "/data/gap_dir")
	if err != nil || !strings.Contains(lsOut1, "gap_file.txt") {
		t.Fatalf("Pod 2 failed to list gap_dir: %v, out: %q", err, lsOut1)
	}

	// Record initial subscription count on Node 2's daemon
	var daemonPodNode2 string
	for _, pod := range h.getPodNames("app=objectfs-node-daemon", "default") {
		logs := h.getContainerLogs(pod, "objectfs-node-daemon", "default", false)
		if strings.Contains(logs, "node "+node2) || strings.Contains(logs, fmt.Sprintf("node=%q", node2)) {
			daemonPodNode2 = pod
			break
		}
	}
	if daemonPodNode2 == "" {
		t.Fatalf("Failed to identify daemon pod on Node 2 (%s)", node2)
	}

	initialLogs := h.getContainerLogs(daemonPodNode2, "objectfs-node-daemon", "default", false)
	initialSubCount := countWatchVolumeSubscriptions(initialLogs, volumeID, node2)

	// Restart controller to create a watcher outage
	t.Logf("Restarting objectfs-controller to simulate watch stream outage")
	h.RunCommand("kubectl", "delete", "pod", "objectfs-controller-0", "-n", "default", "--force", "--grace-period=0")
	if err := h.WaitForStatefulSet("objectfs-controller", "default", 2*time.Minute); err != nil {
		t.Fatalf("Controller failed to restart: %v", err)
	}

	// Pod 1 deletes d/x and syncs
	_, err = h.RunInPod(pod1Name, "default", "/bin/sh", "-c", "rm -f /data/gap_dir/gap_file.txt && sync")
	if err != nil {
		t.Fatalf("Failed to delete gap_file.txt from Pod 1: %v", err)
	}

	// Wait for Node 2's daemon log to show subscription re-established
	t.Logf("Waiting for Node 2 daemon to re-establish WatchVolume subscription")
	reconnectDeadline := time.Now().Add(30 * time.Second)
	reconnected := false
	for time.Now().Before(reconnectDeadline) {
		logs := h.getContainerLogs(daemonPodNode2, "objectfs-node-daemon", "default", false)
		currentCount := countWatchVolumeSubscriptions(logs, volumeID, node2)
		if currentCount > initialSubCount {
			reconnected = true
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if !reconnected {
		t.Fatalf("Node 2 daemon failed to re-establish subscription within 30s")
	}

	// Immediately ls d on Pod 2: gap_file.txt must not show up, without any further sleep!
	lsOut2, err := h.RunInPod(pod2Name, "default", "ls", "/data/gap_dir")
	if err != nil {
		t.Fatalf("Pod 2 failed to list gap_dir after reconnect: %v", err)
	}
	if strings.Contains(lsOut2, "gap_file.txt") {
		t.Fatalf("Dentry revalidation after reconnect failed: gap_file.txt still visible in ls output: %q", lsOut2)
	}

	h.DeletePod(pod1Name, "default")
	h.DeletePod(pod2Name, "default")
	t.Logf("Successfully verified immediate cross-node visibility in steady state!")
}

func TestObjectFSXAttrCrossNodeE2E(t *testing.T) {
	requireE2EGroup(t, "multinode")

	h, node1, node2 := setupObjectFSMultiNodeCluster(t)
	volumeID := "cross-node-xattr-vol"

	t.Cleanup(func() {
		if t.Failed() {
			h.DumpDiagnosticLogs(t)
			t.Logf("Node Daemon Logs:\n%s\n", h.GetPodLogs("app=objectfs-node-daemon", "default"))
			t.Logf("Controller Logs:\n%s\n", h.GetPodLogsByName("objectfs-controller-0", "default"))
		}
	})

	pod1Name := "xattr-node1-pod"
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
      image: images.local/objectfs-node-daemon:e2e
      imagePullPolicy: Never
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
`, pod1Name, node1, volumeID)

	pod2Name := "xattr-node2-pod"
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
      image: images.local/objectfs-node-daemon:e2e
      imagePullPolicy: Never
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
`, pod2Name, node2, volumeID)

	h.KubectlApplyContent(pod1Name, pod1Yaml)
	h.KubectlApplyContent(pod2Name, pod2Yaml)
	defer h.DeletePod(pod1Name, "default")
	defer h.DeletePod(pod2Name, "default")

	if err := h.WaitForPodReady(pod1Name, "default", 1*time.Minute); err != nil {
		t.Fatalf("Pod 1 failed to become ready: %v", err)
	}
	if err := h.WaitForPodReady(pod2Name, "default", 1*time.Minute); err != nil {
		t.Fatalf("Pod 2 failed to become ready: %v", err)
	}

	// Step 1: Create file on Node 1 and fsync so it becomes visible to Node 2
	_, err := h.RunInPod(pod1Name, "default", "/bin/sh", "-c", "echo 'multinode xattr test' > /data/multi.txt && sync /data/multi.txt")
	if err != nil {
		t.Fatalf("failed to create and sync file on Node 1: %v", err)
	}

	// Step 2: Node 2 stats the file before Node 1 sets the xattr (caches xattr_count == 0)
	statOut, err := h.RunInPod(pod2Name, "default", "stat", "/data/multi.txt")
	if err != nil {
		t.Fatalf("failed to stat file on Node 2: %v (out: %s)", err, statOut)
	}

	// Step 3: Node 1 sets user.* xattr and fsyncs, triggering EVENT_MODIFIED broadcast to Node 2
	_, err = h.RunInPod(pod1Name, "default", "/bin/sh", "-c", "setfattr -n user.sharedmeta -v 'shared-value-99' /data/multi.txt && sync /data/multi.txt")
	if err != nil {
		t.Fatalf("failed to setfattr and sync on Node 1: %v", err)
	}

	// Verify readable on Node 1
	node1Out, err := h.RunInPod(pod1Name, "default", "getfattr", "--absolute-names", "-n", "user.sharedmeta", "--only-values", "/data/multi.txt")
	if err != nil {
		t.Fatalf("failed to getfattr from Node 1: %v", err)
	}
	if strings.TrimSpace(node1Out) != "shared-value-99" {
		t.Fatalf("expected 'shared-value-99' on Node 1, got: %q", node1Out)
	}

	// Step 4: Read xattr on Node 2 (verifies watch event invalidated cached count 0).
	// A short retry loop allows asynchronous watch event delivery to arrive on slow runners.
	var out string
	var lastErr error
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		out, lastErr = h.RunInPod(pod2Name, "default", "getfattr", "--absolute-names", "-n", "user.sharedmeta", "--only-values", "/data/multi.txt")
		if lastErr == nil && strings.TrimSpace(out) == "shared-value-99" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if lastErr != nil {
		t.Fatalf("failed to getfattr from Node 2: %v", lastErr)
	}
	if strings.TrimSpace(out) != "shared-value-99" {
		t.Fatalf("expected 'shared-value-99' on Node 2, got: %q", out)
	}
}

func TestObjectFSCloseToOpenE2E(t *testing.T) {
	requireE2EGroup(t, "multinode")

	h, node1, node2 := setupObjectFSMultiNodeCluster(t)
	volumeID := "c2o-barrier-e2e-vol"

	t.Cleanup(func() {
		if t.Failed() {
			h.DumpDiagnosticLogs(t)
			t.Logf("Node Daemon Logs:\n%s\n", h.GetPodLogs("app=objectfs-node-daemon", "default"))
			t.Logf("Controller Logs:\n%s\n", h.GetPodLogsByName("objectfs-controller-0", "default"))
		}
	})

	pod1Name := "c2o-writer-node1"
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
          consistency: close-to-open
`, pod1Name, node1, volumeID)

	pod2Name := "c2o-reader-node2"
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
          consistency: close-to-open
`, pod2Name, node2, volumeID)

	t.Logf("Starting close-to-open writer on %s and reader on %s", node1, node2)
	h.KubectlApplyContent(pod1Name, pod1Yaml)
	h.KubectlApplyContent(pod2Name, pod2Yaml)

	if err := h.WaitForPodReady(pod1Name, "default", 1*time.Minute); err != nil {
		t.Fatalf("Writer Pod failed to become ready: %v", err)
	}
	if err := h.WaitForPodReady(pod2Name, "default", 1*time.Minute); err != nil {
		t.Fatalf("Reader Pod failed to become ready: %v", err)
	}

	defer func() {
		h.DeletePod(pod1Name, "default")
		h.DeletePod(pod2Name, "default")
	}()

	// Case 1: 4 KiB inline file
	t.Logf("--- Testing Case 1: 4 KiB inline file ---")
	// 1. Writer creates empty file and syncs so reader can see dentry
	if _, err := h.RunInPod(pod1Name, "default", "/bin/sh", "-c", "touch /data/inline_4k.bin && sync /data/inline_4k.bin"); err != nil {
		t.Fatalf("Failed to initialize empty inline_4k.bin: %v", err)
	}
	// 2. Reader reads/stats the file first to populate its local cache with a stale size (size=0)
	outStat0, err := h.RunInPod(pod2Name, "default", "/bin/sh", "-c", "stat -c %s /data/inline_4k.bin && cat /data/inline_4k.bin")
	if err != nil || strings.TrimSpace(outStat0) != "0" {
		t.Fatalf("Reader failed to seed size=0 in cache: %v, out: %q", err, outStat0)
	}
	// 3. Writer writes 4 KiB and closes WITHOUT fsync
	write4kCmd := "dd if=/dev/urandom of=/data/inline_4k.bin bs=4096 count=1 conv=notrunc 2>/dev/null && sha256sum /data/inline_4k.bin | cut -d' ' -f1"
	expectedSha4k, err := h.RunInPod(pod1Name, "default", "/bin/sh", "-c", write4kCmd)
	if err != nil {
		t.Fatalf("Writer failed to write 4 KiB file: %v", err)
	}
	expectedSha4k = strings.TrimSpace(expectedSha4k)

	// 4. Reader immediately executes open(2) without sleep, measuring barrier latency
	read4kCmd := `T0=$(date +%s%N); exec 3< /data/inline_4k.bin; T1=$(date +%s%N); SHA=$(cat <&3 | sha256sum | cut -d' ' -f1); exec 3<&-; echo "LAT_NS:$((T1 - T0)) SHA:$SHA"`
	outReader4k, err := h.RunInPod(pod2Name, "default", "/bin/sh", "-c", read4kCmd)
	if err != nil {
		t.Fatalf("Reader failed to read 4 KiB file: %v (output: %q)", err, outReader4k)
	}
	t.Logf("Reader 4 KiB output: %s", outReader4k)
	if !strings.Contains(outReader4k, expectedSha4k) {
		t.Fatalf("Reader 4 KiB content mismatch: expected SHA %s, got output: %s", expectedSha4k, outReader4k)
	}
	stat4k, err := h.RunInPod(pod2Name, "default", "stat", "-c", "%s", "/data/inline_4k.bin")
	if err != nil || strings.TrimSpace(stat4k) != "4096" {
		t.Fatalf("Reader 4 KiB stat size mismatch: expected 4096, got %q (err: %v)", stat4k, err)
	}

	// Case 2: 16 MiB multi-chunk file
	t.Logf("--- Testing Case 2: 16 MiB multi-chunk file ---")
	// 1. Writer creates empty file and syncs so reader can see dentry
	if _, err := h.RunInPod(pod1Name, "default", "/bin/sh", "-c", "touch /data/multi_16m.bin && sync /data/multi_16m.bin"); err != nil {
		t.Fatalf("Failed to initialize empty multi_16m.bin: %v", err)
	}
	// 2. Reader reads/stats the file first to populate its local cache with a stale size (size=0)
	outStat16m0, err := h.RunInPod(pod2Name, "default", "/bin/sh", "-c", "stat -c %s /data/multi_16m.bin && cat /data/multi_16m.bin")
	if err != nil || strings.TrimSpace(outStat16m0) != "0" {
		t.Fatalf("Reader failed to seed size=0 in cache for 16m: %v, out: %q", err, outStat16m0)
	}
	// 3. Writer writes 16 MiB and closes WITHOUT fsync
	write16mCmd := "dd if=/dev/urandom of=/data/multi_16m.bin bs=1048576 count=16 conv=notrunc 2>/dev/null && sha256sum /data/multi_16m.bin | cut -d' ' -f1"
	expectedSha16m, err := h.RunInPod(pod1Name, "default", "/bin/sh", "-c", write16mCmd)
	if err != nil {
		t.Fatalf("Writer failed to write 16 MiB file: %v", err)
	}
	expectedSha16m = strings.TrimSpace(expectedSha16m)

	// 4. Reader immediately executes open(2) without sleep, measuring barrier latency
	read16mCmd := `T0=$(date +%s%N); exec 3< /data/multi_16m.bin; T1=$(date +%s%N); SHA=$(cat <&3 | sha256sum | cut -d' ' -f1); exec 3<&-; echo "LAT_NS:$((T1 - T0)) SHA:$SHA"`
	outReader16m, err := h.RunInPod(pod2Name, "default", "/bin/sh", "-c", read16mCmd)
	if err != nil {
		t.Fatalf("Reader failed to read 16 MiB file: %v (output: %q)", err, outReader16m)
	}
	t.Logf("Reader 16 MiB output: %s", outReader16m)
	if !strings.Contains(outReader16m, expectedSha16m) {
		t.Fatalf("Reader 16 MiB content mismatch: expected SHA %s, got output: %s", expectedSha16m, outReader16m)
	}
	stat16m, err := h.RunInPod(pod2Name, "default", "stat", "-c", "%s", "/data/multi_16m.bin")
	if err != nil || strings.TrimSpace(stat16m) != "16777216" {
		t.Fatalf("Reader 16 MiB stat size mismatch: expected 16777216, got %q (err: %v)", stat16m, err)
	}
	t.Logf("Successfully verified two-node close-to-open consistency for 4 KiB inline and 16 MiB multi-chunk files!")

	controllerLogs := h.GetPodLogsByName("objectfs-controller-0", "default")
	for _, logLine := range strings.Split(controllerLogs, "\n") {
		if strings.Contains(logLine, "sync completed in") {
			t.Logf("Controller: %s", logLine)
		}
	}
}

func TestObjectFSControllerCrashMidWriteE2E(t *testing.T) {
	requireE2EGroup(t, "chaos")

	h, node1, node2 := setupObjectFSMultiNodeCluster(t)
	volumeID := "crash-midwrite-vol"

	writerPodName := "crash-writer-pod"
	writerPodYaml := fmt.Sprintf(`
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
`, writerPodName, node1, volumeID)

	t.Logf("Creating writer pod on %s", node1)
	h.KubectlApplyContent(writerPodName, writerPodYaml)
	if err := h.WaitForPodReady(writerPodName, "default", 1*time.Minute); err != nil {
		t.Fatalf("Writer pod failed to start: %v", err)
	}

	// Prepare workload script inside writer pod.
	// Golden data is generated in /tmp/src/, hashed from /tmp/src/,
	// written to /data/ with conv=fsync (or sync FILE after truncate),
	// and recorded in /tmp/manifest/ only after fsync returns.
	workloadScript := `#!/bin/sh
set -e
mkdir -p /tmp/manifest /tmp/src/dir1 /tmp/src/dir2 /data/dir1 /data/dir2

write_file() {
  file="$1"
  size_kb="$2"
  src="/tmp/src/$file"
  dd if=/dev/urandom of="$src" bs=1024 count="$size_kb" 2>/dev/null
  hash=$(sha256sum "$src" | awk '{print $1}')
  dd if="$src" of="/data/$file" bs=1024 conv=fsync 2>/dev/null
  echo "$hash" > "/tmp/manifest/$(echo "$file" | tr '/' '_').sha"
}

truncate_file() {
  file="$1"
  size_bytes="$2"
  src="/tmp/src/$file"
  truncate -s "$size_bytes" "$src"
  hash=$(sha256sum "$src" | awk '{print $1}')
  truncate -s "$size_bytes" "/data/$file"
  sync "/data/$file"
  echo "$hash" > "/tmp/manifest/$(echo "$file" | tr '/' '_').sha"
}

for i in $(seq 1 10); do
  write_file "small_${i}.bin" 2
  write_file "med_${i}.bin" 24
  write_file "large_${i}.bin" 128
  write_file "dir1/nested_${i}.bin" 8
  write_file "dir2/nested_${i}.bin" 80

  if [ "$i" -gt 3 ]; then
    prev=$((i - 2))
    write_file "small_${prev}.bin" 16
    write_file "large_${prev}.bin" 96
  fi

  if [ "$i" -gt 6 ]; then
    prev=$((i - 4))
    truncate_file "med_${prev}.bin" 512
    truncate_file "dir2/nested_${prev}.bin" 1024
  fi

  sleep 0.1
done
`

	_, err := h.RunInPod(writerPodName, "default", "/bin/sh", "-c", fmt.Sprintf("cat << 'EOF' > /tmp/workload.sh\n%s\nEOF\nchmod +x /tmp/workload.sh", workloadScript))
	if err != nil {
		t.Fatalf("Failed to write workload script to writer pod: %v", err)
	}

	t.Logf("Starting background workload in writer pod")
	runnerScript := `/tmp/workload.sh > /tmp/workload.log 2>&1
echo $? > /tmp/workload.status
`
	_, err = h.RunInPod(writerPodName, "default", "/bin/sh", "-c", fmt.Sprintf("cat << 'EOF' > /tmp/runner.sh\n%s\nEOF\nchmod +x /tmp/runner.sh\n/tmp/runner.sh &", runnerScript))
	if err != nil {
		t.Fatalf("Failed to launch background runner: %v", err)
	}

	crashes := 3
	if envVal := os.Getenv("OBJECTFS_CRASH_ITERATIONS"); envVal != "" {
		if n, err := strconv.Atoi(envVal); err == nil && n > 0 {
			crashes = n
		}
	}

	currentNode := node1
	for i := 0; i < crashes; i++ {
		time.Sleep(time.Duration(500+rand.Intn(1000)) * time.Millisecond)

		t.Logf("Crash iteration %d/%d: force-killing objectfs-controller on %s", i+1, crashes, currentNode)
		h.RunCommand("kubectl", "delete", "pod", "objectfs-controller-0", "-n", "default", "--force", "--grace-period=0")

		targetNode := node1
		if i%2 == 0 {
			targetNode = node2
		}
		if targetNode != currentNode {
			t.Logf("Moving controller to %s", targetNode)
			patchJSON := fmt.Sprintf(`{"spec":{"template":{"spec":{"nodeSelector":{"kubernetes.io/hostname":%q}}}}}`, targetNode)
			h.RunCommand("kubectl", "patch", "statefulset", "objectfs-controller", "-n", "default", "-p", patchJSON)
			currentNode = targetNode
		}

		if err := h.WaitForStatefulSet("objectfs-controller", "default", 2*time.Minute); err != nil {
			t.Fatalf("ObjectFS Controller failed to restart on %s during crash iteration %d: %v", currentNode, i+1, err)
		}
	}

	t.Logf("Waiting for workload in writer pod to finish")
	deadline := time.Now().Add(2 * time.Minute)
	workloadFinished := false
	var exitCodeStr string
	for time.Now().Before(deadline) {
		out, err := h.RunInPod(writerPodName, "default", "cat", "/tmp/workload.status")
		if err == nil && strings.TrimSpace(out) != "" {
			exitCodeStr = strings.TrimSpace(out)
			workloadFinished = true
			break
		}
		time.Sleep(1 * time.Second)
	}
	if !workloadFinished {
		wLogs, _ := h.RunInPod(writerPodName, "default", "cat", "/tmp/workload.log")
		t.Fatalf("Workload did not complete in time. Logs:\n%s", wLogs)
	}
	if exitCodeStr != "0" {
		wLogs, _ := h.RunInPod(writerPodName, "default", "cat", "/tmp/workload.log")
		t.Fatalf("Workload exited with error code %s. Logs:\n%s", exitCodeStr, wLogs)
	}

	manifestListRaw, err := h.RunInPod(writerPodName, "default", "/bin/sh", "-c", "for f in /tmp/manifest/*.sha; do [ -f \"$f\" ] || continue; echo \"$(cat \"$f\") $(basename \"$f\" .sha)\"; done")
	if err != nil {
		t.Fatalf("Failed to read manifest list from writer pod: %v", err)
	}

	manifestLines := strings.Split(strings.TrimSpace(manifestListRaw), "\n")
	if len(manifestLines) == 0 || manifestLines[0] == "" {
		t.Fatalf("No fsynced files were recorded in writer pod manifest!")
	}
	t.Logf("Collected %d fsynced files in manifest to verify", len(manifestLines))

	readerPodName := "crash-reader-pod"
	readerPodYaml := fmt.Sprintf(`
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
`, readerPodName, node2, volumeID)

	t.Logf("Creating reader pod on %s to verify data integrity", node2)
	h.KubectlApplyContent(readerPodName, readerPodYaml)
	if err := h.WaitForPodReady(readerPodName, "default", 1*time.Minute); err != nil {
		t.Fatalf("Reader pod failed to start: %v", err)
	}

	for _, line := range manifestLines {
		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}
		expectedHash := parts[0]
		escapedName := parts[1]
		var relativePath string
		if strings.HasPrefix(escapedName, "dir1_") {
			relativePath = "dir1/" + strings.TrimPrefix(escapedName, "dir1_")
		} else if strings.HasPrefix(escapedName, "dir2_") {
			relativePath = "dir2/" + strings.TrimPrefix(escapedName, "dir2_")
		} else {
			relativePath = escapedName
		}

		out, err := h.RunInPod(readerPodName, "default", "/bin/sh", "-c", fmt.Sprintf("sha256sum /data/%s | awk '{print $1}'", relativePath))
		if err != nil {
			t.Fatalf("Failed to sha256sum /data/%s in reader pod: %v (out: %s)", relativePath, err, out)
		}
		actualHash := strings.TrimSpace(out)
		if actualHash != expectedHash {
			t.Fatalf("Data corruption detected for %s: expected sha256 %s, got %s", relativePath, expectedHash, actualHash)
		}
	}

	h.DeletePod(writerPodName, "default")
	h.DeletePod(readerPodName, "default")
	t.Logf("Successfully verified ObjectFS controller crash mid-write data integrity across restarts and node failovers!")
}

func TestObjectFSControllerKillWithStagedDataE2E(t *testing.T) {
	requireE2EGroup(t, "chaos")

	h, node1, node2 := setupObjectFSMultiNodeCluster(t)
	volumeID := "kill-staged-data-vol"

	writerPodName := "kill-staged-writer-pod"
	writerPodYaml := fmt.Sprintf(`
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
`, writerPodName, node1, volumeID)

	t.Logf("Creating writer pod on %s", node1)
	h.KubectlApplyContent(writerPodName, writerPodYaml)
	if err := h.WaitForPodReady(writerPodName, "default", 1*time.Minute); err != nil {
		t.Fatalf("Writer pod failed to start: %v", err)
	}

	t.Cleanup(func() {
		h.DeletePod(writerPodName, "default")
		if t.Failed() {
			h.DumpDiagnosticLogs(t)
		}
	})

	// Step 1: Write data in the writer pod while holding the file open in the background so
	// that data remains dirty in the node daemon's cache across the controller restart.
	// Note: The "pushed to staging but not yet committed" window cannot be targeted without
	// a controller hook; this test covers the dirty-in-daemon case and deterministically
	// verifies that the daemon retains unsynced data across a controller restart and re-pushes it.
	t.Logf("Writing staged data and holding file open in writer pod")
	expectedContent := "durable staged data surviving controller kill and daemon re-push"
	holderCmd := fmt.Sprintf(
		`sh -c 'echo $$ > /tmp/holder.pid; exec 3>/data/staged.txt; printf "%%s" %q >&3; echo ready > /tmp/holder.ready; exec sleep 600' </dev/null >/dev/null 2>&1 &`,
		expectedContent,
	)
	if _, err := h.RunInPod(writerPodName, "default", "/bin/sh", "-c", holderCmd); err != nil {
		t.Fatalf("Failed to launch holder process in writer pod: %v", err)
	}

	// Wait for holder process to finish writing and hold the descriptor open
	deadlineReady := time.Now().Add(10 * time.Second)
	ready := false
	for time.Now().Before(deadlineReady) {
		out, err := h.RunInPod(writerPodName, "default", "cat", "/tmp/holder.ready")
		if err == nil && strings.Contains(out, "ready") {
			ready = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !ready {
		t.Fatalf("Holder process in writer pod failed to become ready")
	}

	// Step 2: Force-kill the controller pod while data is held dirty in the daemon.
	t.Logf("Force-killing objectfs-controller-0")
	h.RunCommand("kubectl", "delete", "pod", "objectfs-controller-0", "-n", "default", "--force", "--grace-period=0")

	// Wait for controller to recover
	if err := h.WaitForStatefulSet("objectfs-controller", "default", 2*time.Minute); err != nil {
		t.Fatalf("Controller failed to recover: %v", err)
	}

	// Step 3: Close the descriptor by terminating the holder process so the close push and sync
	// go to the new controller.
	t.Logf("Closing descriptor by terminating holder process")
	if _, err := h.RunInPod(writerPodName, "default", "/bin/sh", "-c", "kill $(cat /tmp/holder.pid)"); err != nil {
		t.Fatalf("Failed to kill holder process: %v", err)
	}

	// Step 4: Create a reader pod on node2 to verify visibility after daemon re-push and sync
	readerPodName := "kill-staged-reader-pod"
	readerPodYaml := fmt.Sprintf(`
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
`, readerPodName, node2, volumeID)

	t.Logf("Creating reader pod on %s to verify data visibility after recovery", node2)
	h.KubectlApplyContent(readerPodName, readerPodYaml)
	if err := h.WaitForPodReady(readerPodName, "default", 1*time.Minute); err != nil {
		t.Fatalf("Reader pod failed to start: %v", err)
	}
	t.Cleanup(func() {
		h.DeletePod(readerPodName, "default")
	})

	// Poll until the data is visible and matches golden content
	t.Logf("Waiting for staged data to be visible in reader pod")
	deadline := time.Now().Add(60 * time.Second)
	var lastContent string
	var lastErr error
	recovered := false
	for time.Now().Before(deadline) {
		out, err := h.RunInPod(readerPodName, "default", "cat", "/data/staged.txt")
		if err == nil && strings.TrimSpace(out) == expectedContent {
			recovered = true
			break
		}
		lastContent = strings.TrimSpace(out)
		lastErr = err
		time.Sleep(1 * time.Second)
	}

	if !recovered {
		t.Fatalf("Staged data not visible: err=%v, content=%q, expected=%q", lastErr, lastContent, expectedContent)
	}
	t.Logf("Successfully verified staged data visibility after controller kill and daemon re-push sync!")
}

func TestObjectFSControllerOutageE2E(t *testing.T) {
	requireE2EGroup(t, "chaos")

	h, node1, node2 := setupObjectFSMultiNodeCluster(t)
	volumeID := "outage-test-vol"

	pod1Name := "outage-pod-1"
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
`, pod1Name, node1, volumeID)

	pod2Name := "outage-pod-2"
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
`, pod2Name, node2, volumeID)

	t.Logf("Creating test pods on Node 1 (%s) and Node 2 (%s)", node1, node2)
	h.KubectlApplyContent(pod1Name, pod1Yaml)
	h.KubectlApplyContent(pod2Name, pod2Yaml)

	if err := h.WaitForPodReady(pod1Name, "default", 1*time.Minute); err != nil {
		t.Fatalf("Pod 1 failed to start: %v", err)
	}
	if err := h.WaitForPodReady(pod2Name, "default", 1*time.Minute); err != nil {
		t.Fatalf("Pod 2 failed to start: %v", err)
	}

	t.Logf("Writing initial files and priming client node caches before outage")
	_, err := h.RunInPod(pod1Name, "default", "/bin/sh", "-c", "echo 'initial-outage-data' > /data/initial.txt && sync /data/initial.txt")
	if err != nil {
		t.Fatalf("Failed to write initial file: %v", err)
	}

	// Prime Pod 1 cache for watched_node1.txt
	_, err = h.RunInPod(pod1Name, "default", "/bin/sh", "-c", "echo 'node1-watched-initial' > /data/watched_node1.txt && sync /data/watched_node1.txt && cat /data/watched_node1.txt")
	if err != nil {
		t.Fatalf("Failed to create and cache watched_node1.txt on Pod 1: %v", err)
	}

	// Prime Pod 2 cache for watched_node2.txt
	_, err = h.RunInPod(pod2Name, "default", "/bin/sh", "-c", "echo 'node2-watched-initial' > /data/watched_node2.txt && sync /data/watched_node2.txt && cat /data/watched_node2.txt")
	if err != nil {
		t.Fatalf("Failed to create and cache watched_node2.txt on Pod 2: %v", err)
	}

	t.Logf("Scaling ObjectFS Controller to 0 to simulate outage longer than 30s metadata timeout")
	h.RunCommand("kubectl", "scale", "statefulset", "objectfs-controller", "-n", "default", "--replicas=0")

	deadline := time.Now().Add(1 * time.Minute)
	for time.Now().Before(deadline) {
		pods := h.getPodNames("app=objectfs-controller", "default")
		if len(pods) == 0 {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	t.Logf("Executing operation on Pod 1 during controller outage (expecting EIO within ~30s)")
	start := time.Now()
	out, err := h.RunInPod(pod1Name, "default", "/bin/sh", "-c", "cat /data/nonexistent_file.txt")
	duration := time.Since(start)

	t.Logf("Operation during outage finished in %v with err=%v (out: %s)", duration, err, out)
	if err == nil {
		t.Fatalf("Expected operation to fail while controller is down, but it succeeded: %s", out)
	}
	if duration < 20*time.Second {
		t.Fatalf("Operation failed too quickly (%v), expected ~30s metadata timeout", duration)
	}
	if duration > 50*time.Second {
		t.Fatalf("Operation took too long (%v), mount hung instead of returning bounded EIO", duration)
	}
	combinedOut := strings.ToLower(out + " " + fmt.Sprintf("%v", err))
	if !strings.Contains(combinedOut, "i/o error") && !strings.Contains(combinedOut, "input/output error") {
		t.Fatalf("Expected EIO / I/O error message during outage, got: out=%q, err=%v", out, err)
	}

	t.Logf("Scaling ObjectFS Controller back up to 1")
	h.RunCommand("kubectl", "scale", "statefulset", "objectfs-controller", "-n", "default", "--replicas=1")
	if err := h.WaitForStatefulSet("objectfs-controller", "default", 2*time.Minute); err != nil {
		t.Fatalf("ObjectFS Controller failed to restart after outage: %v", err)
	}

	t.Logf("Verifying existing mount on Pod 1 works after controller is back up")
	recStart1 := time.Now()
	outInitial, err := retryCatInPod(h, pod1Name, "/data/initial.txt", 30*time.Second)
	recDur1 := time.Since(recStart1)
	if err != nil || !strings.Contains(outInitial, "initial-outage-data") {
		t.Fatalf("Failed to read initial.txt on Pod 1 after outage recovery: %v (out: %s)", err, outInitial)
	}
	t.Logf("Pod 1 mount recovered in %v", recDur1)

	t.Logf("Verifying existing mount on Pod 2 works after controller is back up")
	recStart2 := time.Now()
	outInitial2, err := retryCatInPod(h, pod2Name, "/data/initial.txt", 30*time.Second)
	recDur2 := time.Since(recStart2)
	if err != nil || !strings.Contains(outInitial2, "initial-outage-data") {
		t.Fatalf("Failed to read initial.txt on Pod 2 after outage recovery: %v (out: %s)", err, outInitial2)
	}
	t.Logf("Pod 2 mount recovered in %v", recDur2)

	_, err = h.RunInPod(pod1Name, "default", "/bin/sh", "-c", "echo 'post-outage-from-node1' > /data/from-node1.txt && sync /data/from-node1.txt")
	if err != nil {
		t.Fatalf("Failed to write new file from Pod 1 after outage: %v", err)
	}

	t.Logf("Testing WatchVolume cache invalidation: Pod 2 overwrites watched_node1.txt (cached on Node 1)")
	_, err = h.RunInPod(pod2Name, "default", "/bin/sh", "-c", "echo 'node2-updated-watched-data' > /data/watched_node1.txt && sync /data/watched_node1.txt")
	if err != nil {
		t.Fatalf("Failed to overwrite watched_node1.txt from Pod 2: %v", err)
	}

	outWatched1, err := retryCatInPod(h, pod1Name, "/data/watched_node1.txt", 15*time.Second)
	if err != nil || !strings.Contains(outWatched1, "node2-updated-watched-data") {
		t.Fatalf("WatchVolume invalidation failed: Pod 1 did not see updated watched_node1.txt: %v (out: %s)", err, outWatched1)
	}

	t.Logf("Testing WatchVolume cache invalidation in reverse: Pod 1 overwrites watched_node2.txt (cached on Node 2)")
	_, err = h.RunInPod(pod1Name, "default", "/bin/sh", "-c", "echo 'node1-updated-watched-data' > /data/watched_node2.txt && sync /data/watched_node2.txt")
	if err != nil {
		t.Fatalf("Failed to overwrite watched_node2.txt from Pod 1: %v", err)
	}

	outWatched2, err := retryCatInPod(h, pod2Name, "/data/watched_node2.txt", 15*time.Second)
	if err != nil || !strings.Contains(outWatched2, "node1-updated-watched-data") {
		t.Fatalf("WatchVolume invalidation failed: Pod 2 did not see updated watched_node2.txt: %v (out: %s)", err, outWatched2)
	}

	h.DeletePod(pod1Name, "default")
	h.DeletePod(pod2Name, "default")
	t.Logf("Successfully verified client behavior during outage, bounded EIO, node daemon reconnection, and WatchVolume cache invalidations!")
}

func TestObjectFSWalBufferRestartE2E(t *testing.T) {
	requireE2EGroup(t, "chaos")

	h, node1, _ := setupObjectFSMultiNodeCluster(t)
	volumeID := "wal-restart-vol"

	writerPodName := "wal-writer-pod"
	writerPodYaml := fmt.Sprintf(`
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
`, writerPodName, node1, volumeID)

	t.Logf("Creating writer pod on %s", node1)
	h.KubectlApplyContent(writerPodName, writerPodYaml)
	if err := h.WaitForPodReady(writerPodName, "default", 1*time.Minute); err != nil {
		t.Fatalf("Writer pod failed to start: %v", err)
	}

	t.Logf("Writing initial files before wal-buffer restart")
	initialScript := `
set -e
mkdir -p /tmp/manifest /tmp/src/waldir /data/waldir

echo "witness-ack-data-1" > /tmp/src/file1.txt
hash1=$(sha256sum /tmp/src/file1.txt | awk '{print $1}')
dd if=/tmp/src/file1.txt of=/data/file1.txt conv=fsync 2>/dev/null
echo "$hash1" > /tmp/manifest/file1.txt.sha

dd if=/dev/urandom of=/tmp/src/large1.bin bs=1024 count=128 2>/dev/null
hash2=$(sha256sum /tmp/src/large1.bin | awk '{print $1}')
dd if=/tmp/src/large1.bin of=/data/large1.bin bs=1024 conv=fsync 2>/dev/null
echo "$hash2" > /tmp/manifest/large1.bin.sha

echo "nested-wal-data" > /tmp/src/waldir/nested.txt
hash3=$(sha256sum /tmp/src/waldir/nested.txt | awk '{print $1}')
dd if=/tmp/src/waldir/nested.txt of=/data/waldir/nested.txt conv=fsync 2>/dev/null
echo "$hash3" > /tmp/manifest/waldir_nested.txt.sha
`
	_, err := h.RunInPod(writerPodName, "default", "/bin/sh", "-c", initialScript)
	if err != nil {
		t.Fatalf("Failed to write initial files: %v", err)
	}

	sessionsBeforeKill := countControllerReplicationSessions(h)
	streamCount := countDistinctControllerReplicationStreams(h)

	t.Logf("Force-killing wal-buffer-0 pod (testing witness-acknowledged durability across buffer restart, observed streams: %d)", streamCount)
	h.RunCommand("kubectl", "delete", "pod", "wal-buffer-0", "-n", "default", "--force", "--grace-period=0")
	if err := h.WaitForStatefulSet("wal-buffer", "default", 2*time.Minute); err != nil {
		t.Fatalf("wal-buffer failed to restart: %v", err)
	}
	waitForControllerReplicationSessionIncrease(t, h, sessionsBeforeKill, streamCount, 30*time.Second)

	t.Logf("Verifying initial files persist after wal-buffer restart")
	for _, check := range []struct {
		path         string
		manifestName string
	}{
		{"/data/file1.txt", "file1.txt.sha"},
		{"/data/large1.bin", "large1.bin.sha"},
		{"/data/waldir/nested.txt", "waldir_nested.txt.sha"},
	} {
		expectedHashRaw, err := h.RunInPod(writerPodName, "default", "cat", "/tmp/manifest/"+check.manifestName)
		if err != nil {
			t.Fatalf("Failed to read manifest for %s: %v", check.path, err)
		}
		expectedHash := strings.TrimSpace(expectedHashRaw)

		hashOut, err := h.RunInPod(writerPodName, "default", "/bin/sh", "-c", fmt.Sprintf("sha256sum %s | awk '{print $1}'", check.path))
		if err != nil {
			t.Fatalf("Failed to compute sha256 for %s: %v (out: %s)", check.path, err, hashOut)
		}
		if strings.TrimSpace(hashOut) != expectedHash {
			t.Fatalf("Data mismatch for %s after wal-buffer restart: expected %s, got %s", check.path, expectedHash, strings.TrimSpace(hashOut))
		}
	}

	t.Logf("Starting active workload and restarting wal-buffer mid-write")
	workloadScript := `#!/bin/sh
set -e
mkdir -p /tmp/manifest /tmp/src/wal_batch /data/wal_batch

write_file() {
  file="$1"
  size_kb="$2"
  src="/tmp/src/$file"
  dd if=/dev/urandom of="$src" bs=1024 count="$size_kb" 2>/dev/null
  hash=$(sha256sum "$src" | awk '{print $1}')
  dd if="$src" of="/data/$file" bs=1024 conv=fsync 2>/dev/null
  echo "$hash" > "/tmp/manifest/$(echo "$file" | tr '/' '_').sha"
}

for i in $(seq 1 10); do
  write_file "wal_batch/f_${i}.bin" 32
  if [ "$i" -gt 4 ]; then
    prev=$((i - 2))
    write_file "wal_batch/f_${prev}.bin" 64
  fi
  sleep 0.1
done
`
	_, err = h.RunInPod(writerPodName, "default", "/bin/sh", "-c", fmt.Sprintf("cat << 'EOF' > /tmp/wal_workload.sh\n%s\nEOF\nchmod +x /tmp/wal_workload.sh", workloadScript))
	if err != nil {
		t.Fatalf("Failed to write mid-workload script: %v", err)
	}

	runnerScript := `/tmp/wal_workload.sh > /tmp/wal_workload.log 2>&1
echo $? > /tmp/wal_workload.status
`
	_, err = runInPodWithTimeout(t, h, writerPodName, "default", 3*time.Minute, "/bin/sh", "-c", fmt.Sprintf("cat << 'EOF' > /tmp/wal_runner.sh\n%s\nEOF\nchmod +x /tmp/wal_runner.sh\n/tmp/wal_runner.sh </dev/null >/dev/null 2>&1 &", runnerScript))
	if err != nil {
		t.Fatalf("Failed to start mid-workload in writer pod: %v", err)
	}

	time.Sleep(1 * time.Second)
	sessionsBeforeMidKill := countControllerReplicationSessions(h)
	streamCount = countDistinctControllerReplicationStreams(h)
	t.Logf("Force-killing wal-buffer-0 mid-workload (observed streams: %d)", streamCount)
	h.RunCommand("kubectl", "delete", "pod", "wal-buffer-0", "-n", "default", "--force", "--grace-period=0")
	if err := h.WaitForStatefulSet("wal-buffer", "default", 2*time.Minute); err != nil {
		t.Fatalf("wal-buffer failed to restart mid-workload: %v", err)
	}
	waitForControllerReplicationSessionIncrease(t, h, sessionsBeforeMidKill, streamCount, 30*time.Second)

	deadline := time.Now().Add(3 * time.Minute)
	workloadFinished := false
	var exitCodeStr string
	for time.Now().Before(deadline) {
		out, err := runInPodWithTimeout(t, h, writerPodName, "default", 30*time.Second, "cat", "/tmp/wal_workload.status")
		if err == nil && strings.TrimSpace(out) != "" {
			exitCodeStr = strings.TrimSpace(out)
			workloadFinished = true
			break
		}
		time.Sleep(1 * time.Second)
	}
	if !workloadFinished {
		wLogs, _ := runInPodWithTimeout(t, h, writerPodName, "default", 30*time.Second, "cat", "/tmp/wal_workload.log")
		t.Logf("======= DUMPING DIAGNOSTICS FOR HUNG WAL WORKLOAD (3-minute timeout) =======")
		if desc, err := exec.Command("kubectl", "describe", "pod", writerPodName, "-n", "default").CombinedOutput(); err == nil {
			t.Logf("kubectl describe pod %s:\n%s\n", writerPodName, string(desc))
		}
		t.Logf("Node Daemon Logs:\n%s\n", h.GetPodLogs("app=objectfs-node-daemon", "default"))
		t.Logf("Controller Logs:\n%s\n", h.GetPodLogsByName("objectfs-controller-0", "default"))
		if walLogs := h.getContainerLogs("wal-buffer-0", "wal-buffer", "default", false); walLogs != "" {
			t.Logf("WAL Buffer Logs:\n%s\n", walLogs)
		}
		t.Fatalf("WAL mid-workload did not complete within 3 minutes. Logs:\n%s", wLogs)
	}
	if exitCodeStr != "0" {
		wLogs, _ := h.RunInPod(writerPodName, "default", "cat", "/tmp/wal_workload.log")
		t.Fatalf("WAL mid-workload exited with code %s. Logs:\n%s", exitCodeStr, wLogs)
	}

	manifestListRaw, err := h.RunInPod(writerPodName, "default", "/bin/sh", "-c", "for f in /tmp/manifest/*.sha; do [ -f \"$f\" ] || continue; echo \"$(cat \"$f\") $(basename \"$f\" .sha)\"; done")
	if err != nil {
		t.Fatalf("Failed to read manifest list from writer pod: %v", err)
	}

	for _, line := range strings.Split(strings.TrimSpace(manifestListRaw), "\n") {
		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}
		expectedHash := parts[0]
		escapedName := parts[1]
		var relativePath string
		if strings.HasPrefix(escapedName, "waldir_") {
			relativePath = "waldir/" + strings.TrimPrefix(escapedName, "waldir_")
		} else if strings.HasPrefix(escapedName, "wal_batch_") {
			relativePath = "wal_batch/" + strings.TrimPrefix(escapedName, "wal_batch_")
		} else {
			relativePath = escapedName
		}

		out, err := h.RunInPod(writerPodName, "default", "/bin/sh", "-c", fmt.Sprintf("sha256sum /data/%s | awk '{print $1}'", relativePath))
		if err != nil {
			t.Fatalf("Failed to calculate sha256 for /data/%s: %v (out: %s)", relativePath, err, out)
		}
		if strings.TrimSpace(out) != expectedHash {
			t.Fatalf("Data mismatch for /data/%s after mid-workload wal-buffer restart: expected %s, got %s", relativePath, expectedHash, strings.TrimSpace(out))
		}
	}

	t.Logf("Writing final post-restart file")
	_, err = runInPodWithTimeout(t, h, writerPodName, "default", 3*time.Minute, "/bin/sh", "-c", "echo 'final-wal-buffer-data' | dd of=/data/final_wal.txt conv=fsync 2>/dev/null")
	if err != nil {
		t.Fatalf("Failed to write final post-restart file: %v", err)
	}
	outFinal, err := retryCatInPod(h, writerPodName, "/data/final_wal.txt", 10*time.Second)
	if err != nil || !strings.Contains(outFinal, "final-wal-buffer-data") {
		t.Fatalf("Failed to read final_wal.txt: %v (out: %s)", err, outFinal)
	}

	deletePodBounded(t, h, writerPodName, "default", 3*time.Minute)
	t.Logf("Successfully verified WAL buffer restart durability, mid-workload restart, and continued writes!")
}

func TestObjectFSWalBufferInFlightFsyncAcrossKillE2E(t *testing.T) {
	requireE2EGroup(t, "chaos")

	h, node1, _ := setupObjectFSMultiNodeCluster(t)
	volumeID := "wal-inflight-fsync-vol"

	writerPodName := "wal-inflight-writer-pod"
	writerPodYaml := fmt.Sprintf(`
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
`, writerPodName, node1, volumeID)

	t.Logf("Creating writer pod on %s", node1)
	h.KubectlApplyContent(writerPodName, writerPodYaml)
	if err := h.WaitForPodReady(writerPodName, "default", 1*time.Minute); err != nil {
		t.Fatalf("Writer pod failed to start: %v", err)
	}
	defer deletePodBounded(t, h, writerPodName, "default", 3*time.Minute)

	// Script running concurrent workers doing witness-level mutations (dd conv=fsync) in parallel
	inflightScript := `#!/bin/sh
set -e
mkdir -p /tmp/manifest /tmp/src /data/inflight

worker() {
  id="$1"
  for j in $(seq 1 15); do
    src="/tmp/src/w_${id}_${j}.bin"
    dst="/data/inflight/w_${id}_${j}.bin"
    dd if=/dev/urandom of="$src" bs=4096 count=8 2>/dev/null
    hash=$(sha256sum "$src" | awk '{print $1}')
    dd if="$src" of="$dst" bs=4096 conv=fsync 2>/dev/null
    echo "$hash" > "/tmp/manifest/w_${id}_${j}.sha"
  done
}

worker 1 &
p1=$!
worker 2 &
p2=$!
worker 3 &
p3=$!
worker 4 &
p4=$!

wait $p1 && wait $p2 && wait $p3 && wait $p4
echo 0 > /tmp/inflight.status
`
	_, err := h.RunInPod(writerPodName, "default", "/bin/sh", "-c", fmt.Sprintf("cat << 'EOF' > /tmp/inflight.sh\n%s\nEOF\nchmod +x /tmp/inflight.sh", inflightScript))
	if err != nil {
		t.Fatalf("Failed to write in-flight script: %v", err)
	}

	runnerScript := `/tmp/inflight.sh > /tmp/inflight.log 2>&1 || echo $? > /tmp/inflight.status
`
	_, err = runInPodWithTimeout(t, h, writerPodName, "default", 3*time.Minute, "/bin/sh", "-c", fmt.Sprintf("cat << 'EOF' > /tmp/inflight_runner.sh\n%s\nEOF\nchmod +x /tmp/inflight_runner.sh\n/tmp/inflight_runner.sh </dev/null >/dev/null 2>&1 &", runnerScript))
	if err != nil {
		t.Fatalf("Failed to start in-flight workload in writer pod: %v", err)
	}

	// Give workers a brief moment to get mutations in flight
	time.Sleep(300 * time.Millisecond)

	sessionsBeforeKill := countControllerReplicationSessions(h)
	streamCount := countDistinctControllerReplicationStreams(h)

	t.Logf("Force-killing wal-buffer-0 while mutations are in flight (observed streams: %d)", streamCount)
	h.RunCommand("kubectl", "delete", "pod", "wal-buffer-0", "-n", "default", "--force", "--grace-period=0")
	if err := h.WaitForStatefulSet("wal-buffer", "default", 2*time.Minute); err != nil {
		t.Fatalf("wal-buffer failed to restart: %v", err)
	}
	readyTime := time.Now()
	waitForControllerReplicationSessionIncrease(t, h, sessionsBeforeKill, streamCount, 30*time.Second)

	// Assert: every in-flight fsync must complete within 15 seconds of new pod's readiness, and no fsync returns an error
	bound := readyTime.Add(15 * time.Second)
	completed := false
	var exitCodeStr string
	for time.Now().Before(bound) {
		out, err := h.RunInPod(writerPodName, "default", "cat", "/tmp/inflight.status")
		if err == nil && strings.TrimSpace(out) != "" {
			exitCodeStr = strings.TrimSpace(out)
			completed = true
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	if !completed {
		wLogs, _ := h.RunInPod(writerPodName, "default", "cat", "/tmp/inflight.log")
		t.Logf("Node Daemon Logs:\n%s\n", h.GetPodLogs("app=objectfs-node-daemon", "default"))
		t.Logf("Controller Logs:\n%s\n", h.GetPodLogsByName("objectfs-controller-0", "default"))
		if walLogs := h.getContainerLogs("wal-buffer-0", "wal-buffer", "default", false); walLogs != "" {
			t.Logf("WAL Buffer Logs:\n%s\n", walLogs)
		}
		t.Fatalf("In-flight fsyncs did not complete within 15s of wal-buffer readiness. Logs:\n%s", wLogs)
	}

	if exitCodeStr != "0" {
		wLogs, _ := h.RunInPod(writerPodName, "default", "cat", "/tmp/inflight.log")
		t.Fatalf("In-flight fsync workload failed with exit code %s. Logs:\n%s", exitCodeStr, wLogs)
	}

	// Verify all written files match manifest sha256 sums
	manifestListRaw, err := h.RunInPod(writerPodName, "default", "/bin/sh", "-c", "for f in /tmp/manifest/*.sha; do [ -f \"$f\" ] || continue; echo \"$(cat \"$f\") $(basename \"$f\" .sha)\"; done")
	if err != nil {
		t.Fatalf("Failed to read manifest list from writer pod: %v", err)
	}
	manifestLines := strings.Split(strings.TrimSpace(manifestListRaw), "\n")
	if len(manifestLines) == 0 || manifestLines[0] == "" {
		t.Fatalf("No manifest files found")
	}

	for _, line := range manifestLines {
		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}
		expectedHash := parts[0]
		fileName := parts[1] + ".bin"
		out, err := h.RunInPod(writerPodName, "default", "/bin/sh", "-c", fmt.Sprintf("sha256sum /data/inflight/%s | awk '{print $1}'", fileName))
		if err != nil {
			t.Fatalf("Failed to calculate sha256 for /data/inflight/%s: %v (out: %s)", fileName, err, out)
		}
		if strings.TrimSpace(out) != expectedHash {
			t.Fatalf("Data mismatch for /data/inflight/%s: expected %s, got %s", fileName, expectedHash, strings.TrimSpace(out))
		}
	}

	t.Logf("Successfully verified in-flight fsync across wal-buffer kill completed within 15s bound without error!")
}

// runInPodWithTimeout executes a command in a pod and aborts with a diagnostic dump if it exceeds timeout.
func runInPodWithTimeout(t *testing.T, h *Harness, podName, namespace string, timeout time.Duration, cmd ...string) (string, error) {
	t.Helper()
	type res struct {
		out string
		err error
	}
	ch := make(chan res, 1)
	go func() {
		out, err := h.RunInPod(podName, namespace, cmd...)
		ch <- res{out: out, err: err}
	}()

	select {
	case r := <-ch:
		return r.out, r.err
	case <-time.After(timeout):
		t.Logf("======= DUMPING DIAGNOSTICS FOR HUNG COMMAND IN POD %s (timeout: %v): %v =======", podName, timeout, cmd)
		if desc, err := exec.Command("kubectl", "describe", "pod", podName, "-n", namespace).CombinedOutput(); err == nil {
			t.Logf("kubectl describe pod %s:\n%s\n", podName, string(desc))
		}
		t.Logf("Node Daemon Logs:\n%s\n", h.GetPodLogs("app=objectfs-node-daemon", namespace))
		t.Logf("Controller Logs:\n%s\n", h.GetPodLogsByName("objectfs-controller-0", namespace))
		if walLogs := h.getContainerLogs("wal-buffer-0", "wal-buffer", namespace, false); walLogs != "" {
			t.Logf("WAL Buffer Logs:\n%s\n", walLogs)
		}
		t.Fatalf("Command %v timed out after %v in pod %s", cmd, timeout, podName)
		return "", fmt.Errorf("command timed out in pod %s", podName)
	}
}

// deletePodBounded deletes a pod with --wait=false and polls until it is completely deleted
// within the specified timeout. On timeout expiry, it dumps diagnostic evidence (kubectl describe,
// node daemon logs, controller logs) and calls t.Fatalf.
func deletePodBounded(t *testing.T, h *Harness, podName, namespace string, timeout time.Duration) {
	t.Helper()
	t.Logf("Deleting pod %s in namespace %s with timeout %v", podName, namespace, timeout)
	h.RunCommand("kubectl", "delete", "pod", podName, "-n", namespace, "--wait=false", "--ignore-not-found")

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		out, err := exec.Command("kubectl", "get", "pod", podName, "-n", namespace).CombinedOutput()
		if err != nil && (strings.Contains(string(out), "NotFound") || strings.Contains(string(out), "not found")) {
			t.Logf("Pod %s successfully deleted", podName)
			return
		}
		time.Sleep(2 * time.Second)
	}

	// Timeout expired: dump diagnostics before failing
	t.Logf("======= DUMPING DIAGNOSTICS FOR HUNG POD DELETION: %s =======", podName)
	if desc, err := exec.Command("kubectl", "describe", "pod", podName, "-n", namespace).CombinedOutput(); err == nil {
		t.Logf("kubectl describe pod %s:\n%s\n", podName, string(desc))
	}
	t.Logf("Node Daemon Logs:\n%s\n", h.GetPodLogs("app=objectfs-node-daemon", namespace))
	t.Logf("Controller Logs:\n%s\n", h.GetPodLogsByName("objectfs-controller-0", namespace))
	if walLogs := h.getContainerLogs("wal-buffer-0", "wal-buffer", namespace, false); walLogs != "" {
		t.Logf("WAL Buffer Logs:\n%s\n", walLogs)
	}
	t.Fatalf("Timed out waiting %v for pod %s to be deleted", timeout, podName)
}

var controllerReplicationSessionRegex = regexp.MustCompile(`WAL client stream (\S+): replication session established`)

// countDistinctControllerReplicationStreams parses the controller logs and returns
// the number of distinct stream IDs that have established a replication session.
// If no established sessions are found in the logs yet, it returns 1 as the default stream count.
func countDistinctControllerReplicationStreams(h *Harness) int {
	logs := h.GetPodLogsByName("objectfs-controller-0", "default")
	matches := controllerReplicationSessionRegex.FindAllStringSubmatch(logs, -1)
	streams := make(map[string]struct{})
	for _, m := range matches {
		if len(m) > 1 {
			streams[m[1]] = struct{}{}
		}
	}
	if len(streams) == 0 {
		return 1
	}
	return len(streams)
}

// countControllerReplicationSessions returns the number of "replication session established"
// log occurrences in the ObjectFS controller logs.
func countControllerReplicationSessions(h *Harness) int {
	logs := h.GetPodLogsByName("objectfs-controller-0", "default")
	return strings.Count(logs, "replication session established")
}

// waitForControllerReplicationSessionIncrease polls the ObjectFS controller logs until the count
// of established replication sessions increases by at least minIncrease over initialCount.
// It returns the new total count.
func waitForControllerReplicationSessionIncrease(t *testing.T, h *Harness, initialCount, minIncrease int, timeout time.Duration) int {
	t.Helper()
	target := initialCount + minIncrease
	t.Logf("Waiting up to %v for controller WAL replication session count to increase by at least %d (from %d to >= %d)", timeout, minIncrease, initialCount, target)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		logs := h.GetPodLogsByName("objectfs-controller-0", "default")
		curr := strings.Count(logs, "replication session established")
		if curr >= target {
			t.Logf("Controller WAL replication session confirmed re-established (count: %d >= target %d)", curr, target)
			return curr
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Logf("Controller logs:\n%s", h.GetPodLogsByName("objectfs-controller-0", "default"))
	t.Fatalf("Timed out waiting %v for controller to re-establish WAL replication session (initial: %d, expected >= %d)", timeout, initialCount, target)
	return 0
}

func TestObjectFSExclusiveLocalE2E(t *testing.T) {
	requireE2EGroup(t, "exclusive")

	h, node1, node2 := setupObjectFSMultiNodeCluster(t)
	volumeID := "exclusive-local-e2e-vol"

	t.Cleanup(func() {
		if t.Failed() {
			h.DumpDiagnosticLogs(t)
			t.Logf("Node Daemon Logs:\n%s\n", h.GetPodLogs("app=objectfs-node-daemon", "default"))
			t.Logf("Controller Logs:\n%s\n", h.GetPodLogsByName("objectfs-controller-0", "default"))
		}
	})

	// Setup PVC with storageClassName: objectfs-exclusive-local and ReadWriteOncePod
	pvcYaml := fmt.Sprintf(`
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: %s-pvc
spec:
  accessModes:
    - ReadWriteOncePod
  storageClassName: objectfs-exclusive-local
  resources:
    requests:
      storage: 1Gi
`, volumeID)

	t.Logf("Creating PVC for exclusive-local volume")
	h.KubectlApplyContent("exclusive-pvc", pvcYaml)

	// Step 1: Deploy Pod A on Node 1 in exclusive-local mode
	podAName := "exclusive-pod-a"
	podAYaml := fmt.Sprintf(`
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
      persistentVolumeClaim:
        claimName: %s-pvc
`, podAName, node1, volumeID)

	t.Logf("Starting Pod A on Node 1 (%s)", node1)
	h.KubectlApplyContent(podAName, podAYaml)
	if err := h.WaitForPodReady(podAName, "default", 2*time.Minute); err != nil {
		t.Fatalf("Pod A failed to start: %v", err)
	}

	// Write files inside Pod A
	t.Logf("Pod A writing files in exclusive-local mode")
	_, err := h.RunInPod(podAName, "default", "/bin/sh", "-c", "echo 'hello-from-pod-a' > /data/hello.txt && mkdir /data/subdir && echo 'nested-content-a' > /data/subdir/nested.txt && sync")
	if err != nil {
		t.Fatalf("Failed to write files in Pod A: %v", err)
	}

	// Step 2: Delete Pod A (triggers unmount checkpoint)
	t.Logf("Deleting Pod A on Node 1 to trigger unmount checkpoint")
	deletePodBounded(t, h, podAName, "default", 3*time.Minute)

	// Step 3: Deploy Pod B on Node 2 in exclusive-local mode
	podBName := "exclusive-pod-b"
	podBYaml := fmt.Sprintf(`
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
      persistentVolumeClaim:
        claimName: %s-pvc
`, podBName, node2, volumeID)

	t.Logf("Starting Pod B on Node 2 (%s) mounting the checkpointed volume", node2)
	h.KubectlApplyContent(podBName, podBYaml)
	if err := h.WaitForPodReady(podBName, "default", 2*time.Minute); err != nil {
		t.Fatalf("Pod B failed to start: %v", err)
	}

	// Verify files in Pod B
	outHelloB, err := retryCatInPod(h, podBName, "/data/hello.txt", 15*time.Second)
	if err != nil || !strings.Contains(outHelloB, "hello-from-pod-a") {
		t.Fatalf("Pod B did not see /data/hello.txt: err=%v, out=%s", err, outHelloB)
	}
	outNestedB, err := retryCatInPod(h, podBName, "/data/subdir/nested.txt", 15*time.Second)
	if err != nil || !strings.Contains(outNestedB, "nested-content-a") {
		t.Fatalf("Pod B did not see /data/subdir/nested.txt: err=%v, out=%s", err, outNestedB)
	}
	t.Logf("Pod B on Node 2 successfully verified checkpointed files from Pod A!")

	// Delete Pod B
	t.Logf("Deleting Pod B on Node 2")
	h.DeletePod(podBName, "default")
	h.RunCommand("kubectl", "wait", "--for=delete", "pod/"+podBName, "-n", "default", "--timeout=60s")

	// Step 4: Verify via FUSE mount that same volume has same content
	actualVolumeID, err := h.GetPVForPVC(fmt.Sprintf("%s-pvc", volumeID), "default")
	if err != nil {
		t.Fatalf("Failed to get PV volumeName for PVC %s-pvc: %v", volumeID, err)
	}

	fusePodName := "fuse-reader-pod"
	fusePodYaml := fmt.Sprintf(`
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
`, fusePodName, node1, actualVolumeID)

	t.Logf("Starting FUSE reader Pod on Node 1 to verify standard FUSE visibility of checkpoint")
	h.KubectlApplyContent(fusePodName, fusePodYaml)
	if err := h.WaitForPodReady(fusePodName, "default", 2*time.Minute); err != nil {
		t.Fatalf("FUSE reader pod failed to start: %v", err)
	}

	outHelloFUSE, err := retryCatInPod(h, fusePodName, "/data/hello.txt", 15*time.Second)
	if err != nil || !strings.Contains(outHelloFUSE, "hello-from-pod-a") {
		t.Fatalf("FUSE reader pod did not see /data/hello.txt: err=%v, out=%s", err, outHelloFUSE)
	}
	outNestedFUSE, err := retryCatInPod(h, fusePodName, "/data/subdir/nested.txt", 15*time.Second)
	if err != nil || !strings.Contains(outNestedFUSE, "nested-content-a") {
		t.Fatalf("FUSE reader pod did not see /data/subdir/nested.txt: err=%v, out=%s", err, outNestedFUSE)
	}
	t.Logf("FUSE reader pod successfully saw same content committed by exclusive-local unmount checkpoint!")

	deletePodBounded(t, h, fusePodName, "default", 3*time.Minute)
	deletePodBounded(t, h, podBName, "default", 3*time.Minute)
	h.DeletePVC(fmt.Sprintf("%s-pvc", volumeID), "default")
}

func TestObjectFSExclusiveLocalKillE2E(t *testing.T) {
	requireE2EGroup(t, "exclusive")

	h, node1, node2 := setupObjectFSMultiNodeCluster(t)
	volumeID := "exclusive-kill-e2e-vol"

	t.Cleanup(func() {
		if t.Failed() {
			h.DumpDiagnosticLogs(t)
			t.Logf("Node Daemon Logs:\n%s\n", h.GetPodLogs("app=objectfs-node-daemon", "default"))
			t.Logf("Controller Logs:\n%s\n", h.GetPodLogsByName("objectfs-controller-0", "default"))
		}
	})

	// Setup PVC with storageClassName: objectfs-exclusive-local and ReadWriteOncePod
	pvcYaml := fmt.Sprintf(`
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: %s-pvc
spec:
  accessModes:
    - ReadWriteOncePod
  storageClassName: objectfs-exclusive-local
  resources:
    requests:
      storage: 1Gi
`, volumeID)

	t.Logf("Creating PVC for exclusive-local volume")
	h.KubectlApplyContent("exclusive-kill-pvc", pvcYaml)

	// Step 1: Deploy Pod A on Node 1 in exclusive-local mode
	podAName := "exclusive-kill-pod-a"
	podAYaml := fmt.Sprintf(`
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
      persistentVolumeClaim:
        claimName: %s-pvc
`, podAName, node1, volumeID)

	t.Logf("Starting Pod A on Node 1 (%s)", node1)
	h.KubectlApplyContent(podAName, podAYaml)
	if err := h.WaitForPodReady(podAName, "default", 2*time.Minute); err != nil {
		t.Fatalf("Pod A failed to start: %v", err)
	}

	// Pod A writes initial checkpointed files
	t.Logf("Pod A writing files before checkpoint")
	_, err := h.RunInPod(podAName, "default", "/bin/sh", "-c", "echo 'durable-checkpoint-data' > /data/checkpointed.txt && sync")
	if err != nil {
		t.Fatalf("Failed to write initial file in Pod A: %v", err)
	}

	// Trigger explicit checkpoint via objectfsctl checkpoint <volume>
	actualVolumeID, err := h.GetPVForPVC(fmt.Sprintf("%s-pvc", volumeID), "default")
	if err != nil {
		t.Fatalf("Failed to get PV volumeName for PVC %s-pvc: %v", volumeID, err)
	}

	t.Logf("Executing objectfsctl checkpoint for volume %s", actualVolumeID)
	h.RunCommand("kubectl", "exec", "objectfs-admin-pod", "-n", "default", "-c", "client", "--", "objectfsctl", "--server", "objectfs-controller:50051", "checkpoint", actualVolumeID)

	// Pod A writes more files AFTER the checkpoint
	t.Logf("Pod A writing files after checkpoint")
	_, err = h.RunInPod(podAName, "default", "/bin/sh", "-c", "echo 'post-checkpoint-data' > /data/post_checkpoint.txt && sync")
	if err != nil {
		t.Fatalf("Failed to write post-checkpoint file in Pod A: %v", err)
	}

	// Step 2: Pod A is deleted via force delete (--force --grace-period=0)
	// Kubelet still unpublishes the volume on force delete, so unmount checkpoint runs and commits.
	t.Logf("Force-deleting Pod A on Node 1 (--force --grace-period=0)")
	h.RunCommand("kubectl", "delete", "pod", podAName, "-n", "default", "--force", "--grace-period=0", "--wait=false")

	// Step 3: Deploy Pod B on Node 2 in exclusive-local mode immediately without test-side wait.
	// Thanks to the controller-side exclusive hold (#307), Pod B's publish attempts on Node 2
	// are held off with codes.Aborted while Node 1 holds the lease and completes its unmount checkpoint.
	podBName := "exclusive-kill-pod-b"
	podBYaml := fmt.Sprintf(`
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
      persistentVolumeClaim:
        claimName: %s-pvc
`, podBName, node2, volumeID)

	t.Logf("Starting Pod B on Node 2 (%s) mounting the volume after force-delete", node2)
	h.KubectlApplyContent(podBName, podBYaml)
	if err := h.WaitForPodReady(podBName, "default", 2*time.Minute); err != nil {
		t.Fatalf("Pod B failed to start: %v", err)
	}

	// Verify contract: Pod B MUST have the checkpointed file and MUST ALSO have the post-checkpoint file
	// because kubelet still unpublished the volume, ensuring unmount durability.
	outCheckpointed, err := retryCatInPod(h, podBName, "/data/checkpointed.txt", 15*time.Second)
	if err != nil || !strings.Contains(outCheckpointed, "durable-checkpoint-data") {
		t.Fatalf("Pod B did not see checkpointed file /data/checkpointed.txt: err=%v, out=%s", err, outCheckpointed)
	}
	t.Logf("Pod B successfully found checkpointed file /data/checkpointed.txt!")

	outPostCheckpoint, err := retryCatInPod(h, podBName, "/data/post_checkpoint.txt", 15*time.Second)
	if err != nil || !strings.Contains(outPostCheckpoint, "post-checkpoint-data") {
		t.Fatalf("Pod B did not see post-checkpoint file /data/post_checkpoint.txt: err=%v, out=%s", err, outPostCheckpoint)
	}
	t.Logf("Pod B successfully found post-checkpoint file /data/post_checkpoint.txt (unmount durability verified)!")

	deletePodBounded(t, h, podBName, "default", 3*time.Minute)
	h.DeletePVC(fmt.Sprintf("%s-pvc", volumeID), "default")
}

func TestObjectFSExclusiveLocalIntervalE2E(t *testing.T) {
	requireE2EGroup(t, "exclusive")

	h, node1, node2 := setupObjectFSMultiNodeCluster(t)
	volumeID := "exclusive-interval-e2e-vol"

	t.Cleanup(func() {
		if t.Failed() {
			h.DumpDiagnosticLogs(t)
			t.Logf("Node Daemon Logs:\n%s\n", h.GetPodLogs("app=objectfs-node-daemon", "default"))
			t.Logf("Controller Logs:\n%s\n", h.GetPodLogsByName("objectfs-controller-0", "default"))
		}
	})

	// StorageClass with checkpointInterval: 2s
	scYaml := `
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: objectfs-exclusive-local-interval
provisioner: objectfs.labs.gke.io
volumeBindingMode: WaitForFirstConsumer
reclaimPolicy: Delete
parameters:
  mode: exclusive-local
  checkpointInterval: "2s"
`
	h.KubectlApplyContent("sc-interval", scYaml)

	// Setup PVC
	pvcYaml := fmt.Sprintf(`
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: %s-pvc
spec:
  accessModes:
    - ReadWriteOncePod
  storageClassName: objectfs-exclusive-local-interval
  resources:
    requests:
      storage: 1Gi
`, volumeID)

	t.Logf("Creating PVC for interval exclusive-local volume")
	h.KubectlApplyContent("exclusive-interval-pvc", pvcYaml)

	// Step 1: Deploy Pod A on Node 1 in exclusive-local mode
	podAName := "exclusive-interval-pod-a"
	podAYaml := fmt.Sprintf(`
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
      persistentVolumeClaim:
        claimName: %s-pvc
`, podAName, node1, volumeID)

	t.Logf("Starting Pod A on Node 1 (%s)", node1)
	h.KubectlApplyContent(podAName, podAYaml)
	if err := h.WaitForPodReady(podAName, "default", 2*time.Minute); err != nil {
		t.Fatalf("Pod A failed to start: %v", err)
	}

	actualVolumeID, err := h.GetPVForPVC(fmt.Sprintf("%s-pvc", volumeID), "default")
	if err != nil {
		t.Fatalf("Failed to get PV volumeName for PVC %s-pvc: %v", volumeID, err)
	}

	// Step 2: Deploy FUSE reader Pod on Node 2
	fusePodName := "fuse-interval-reader-node2"
	fusePodYaml := fmt.Sprintf(`
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
`, fusePodName, node2, actualVolumeID)

	t.Logf("Starting FUSE reader Pod on Node 2 (%s)", node2)
	h.KubectlApplyContent(fusePodName, fusePodYaml)
	if err := h.WaitForPodReady(fusePodName, "default", 2*time.Minute); err != nil {
		t.Fatalf("FUSE reader pod failed to start: %v", err)
	}

	// Step 3: Pod A writes file while keeping running (no unmount!)
	t.Logf("Pod A writing file without unmounting")
	_, err = h.RunInPod(podAName, "default", "/bin/sh", "-c", "echo 'interval-checkpoint-data' > /data/interval_file.txt && sync")
	if err != nil {
		t.Fatalf("Failed to write file in Pod A: %v", err)
	}

	// Step 4: Within one interval (checkpointInterval=2s), FUSE mount on Node 2 observes the write!
	t.Logf("FUSE reader on Node 2 observing write within interval window")
	outInterval, err := retryCatInPod(h, fusePodName, "/data/interval_file.txt", 15*time.Second)
	if err != nil || !strings.Contains(outInterval, "interval-checkpoint-data") {
		t.Fatalf("FUSE reader pod on Node 2 did not observe interval checkpoint write: err=%v, out=%s", err, outInterval)
	}
	t.Logf("FUSE reader on Node 2 successfully observed file written by running exclusive-local pod via interval checkpoint!")

	deletePodBounded(t, h, fusePodName, "default", 3*time.Minute)
	deletePodBounded(t, h, podAName, "default", 3*time.Minute)
	h.DeletePVC(fmt.Sprintf("%s-pvc", volumeID), "default")
}

func TestObjectFSExclusiveLocalDaemonKillE2E(t *testing.T) {
	requireE2EGroup(t, "exclusive")

	h, node1, node2 := setupObjectFSMultiNodeCluster(t)
	volumeID := "exclusive-daemonkill-e2e-vol"

	podAName := "exclusive-daemonkill-pod-a"
	podBName := "exclusive-daemonkill-pod-b"

	t.Cleanup(func() {
		// Restore daemonset nodeSelector and wait for rollout to restore on both nodes before returning
		if err := exec.Command("kubectl", "patch", "daemonset", "objectfs-node-daemon", "-n", "default", "-p", `{"spec":{"template":{"spec":{"nodeSelector":null}}}}`).Run(); err != nil {
			t.Logf("Cleanup: failed to unpatch daemonset nodeSelector: %v", err)
		}
		if err := h.WaitForDaemonSet("objectfs-node-daemon", "default", 2*time.Minute); err != nil {
			t.Errorf("Cleanup: objectfs-node-daemon failed to restore on all nodes: %v", err)
			h.DumpDiagnosticLogs(t)
			t.Logf("Node Daemon Logs:\n%s\n", h.GetPodLogs("app=objectfs-node-daemon", "default"))
		}
		deletePodBounded(t, h, podAName, "default", 3*time.Minute)
		deletePodBounded(t, h, podBName, "default", 3*time.Minute)
		h.DeletePVC(fmt.Sprintf("%s-pvc", volumeID), "default")
		if t.Failed() {
			h.DumpDiagnosticLogs(t)
			t.Logf("Node Daemon Logs:\n%s\n", h.GetPodLogs("app=objectfs-node-daemon", "default"))
			t.Logf("Controller Logs:\n%s\n", h.GetPodLogsByName("objectfs-controller-0", "default"))
		}
	})

	// Setup PVC with storageClassName: objectfs-exclusive-local and ReadWriteOncePod
	pvcYaml := fmt.Sprintf(`
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: %s-pvc
spec:
  accessModes:
    - ReadWriteOncePod
  storageClassName: objectfs-exclusive-local
  resources:
    requests:
      storage: 1Gi
`, volumeID)
	h.KubectlApplyContent("exclusive-daemonkill-pvc", pvcYaml)

	// Step 1: Deploy Pod A on Node 1 in exclusive-local mode
	podAYaml := fmt.Sprintf(`
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
      persistentVolumeClaim:
        claimName: %s-pvc
`, podAName, node1, volumeID)

	t.Logf("Starting Pod A on Node 1 (%s)", node1)
	h.KubectlApplyContent(podAName, podAYaml)
	if err := h.WaitForPodReady(podAName, "default", 3*time.Minute); err != nil {
		t.Fatalf("Pod A failed to start: %v", err)
	}
	t.Logf("Pod A is ready on Node 1")

	// Pod A writes a file to be checkpointed
	t.Logf("Pod A writing checkpointed file")
	_, err := h.RunInPod(podAName, "default", "/bin/sh", "-c", "echo 'checkpointed-data' > /data/checkpointed.txt && sync")
	if err != nil {
		t.Fatalf("Failed to write initial file in Pod A: %v", err)
	}

	actualVolumeID, err := h.GetPVForPVC(fmt.Sprintf("%s-pvc", volumeID), "default")
	if err != nil {
		t.Fatalf("Failed to get PV volumeName for PVC %s-pvc: %v", volumeID, err)
	}

	// Trigger explicit checkpoint via objectfsctl checkpoint <volume>
	t.Logf("Executing objectfsctl checkpoint for volume %s", actualVolumeID)
	h.RunCommand("kubectl", "exec", "objectfs-admin-pod", "-n", "default", "-c", "client", "--", "objectfsctl", "--server", "objectfs-controller:50051", "checkpoint", actualVolumeID)

	// Pod A writes post-checkpoint file
	t.Logf("Pod A writing files after checkpoint")
	_, err = h.RunInPod(podAName, "default", "/bin/sh", "-c", "echo 'post-checkpoint-data' > /data/post_checkpoint.txt && sync")
	if err != nil {
		t.Fatalf("Failed to write post-checkpoint file in Pod A: %v", err)
	}

	// Step 2: Kill node daemon on Node 1 (restrict DaemonSet to Node 2)
	t.Logf("Killing node daemon pod on Node 1 by restricting DaemonSet to Node 2 (%s)", node2)
	h.RunCommand("kubectl", "patch", "daemonset", "objectfs-node-daemon", "-n", "default", "-p",
		fmt.Sprintf(`{"spec":{"template":{"spec":{"nodeSelector":{"kubernetes.io/hostname":"%s"}}}}}`, node2))

	// Wait for daemon pod on Node 1 to terminate
	t.Logf("Waiting for node daemon pod on Node 1 to terminate")
	waitDeadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(waitDeadline) {
		out, _ := exec.Command("kubectl", "get", "pods", "-n", "default", "-l", "app=objectfs-node-daemon",
			"--field-selector", fmt.Sprintf("spec.nodeName=%s", node1), "--no-headers").CombinedOutput()
		if strings.TrimSpace(string(out)) == "" {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Logf("Node daemon on Node 1 terminated")

	// Force-delete Pod A on Node 1
	t.Logf("Force-deleting Pod A on Node 1 (--force --grace-period=0)")
	h.RunCommand("kubectl", "delete", "pod", podAName, "-n", "default", "--force", "--grace-period=0", "--wait=false")

	// Inspect hold via objectfsctl volumes describe: hold is still recorded for Node 1
	if descOut, descErr := exec.Command("kubectl", "exec", "objectfs-controller-0", "-n", "default", "-c", "objectfs-controller", "--",
		"objectfsctl", "--server", "localhost:50051", "volumes", "describe", actualVolumeID).CombinedOutput(); descErr == nil {
		t.Logf("objectfsctl volumes describe output after daemon kill:\n%s", string(descOut))
	}

	// Step 3: Deploy Pod B on Node 2 in exclusive-local mode
	// Pod B publish is held off while Node 1 lease is live, admitted after 30s lease expiry
	podBYaml := fmt.Sprintf(`
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
      persistentVolumeClaim:
        claimName: %s-pvc
`, podBName, node2, volumeID)

	t.Logf("Deploying Pod B on Node 2 (%s), expecting admission after lease expiry", node2)
	startTime := time.Now()
	h.KubectlApplyContent(podBName, podBYaml)
	if err := h.WaitForPodReady(podBName, "default", 3*time.Minute); err != nil {
		t.Fatalf("Pod B failed to become ready: %v", err)
	}
	elapsed := time.Since(startTime)
	t.Logf("Pod B became ready after %v (lease expiry admission verified)", elapsed)

	// Step 4: Verify data durability contract:
	// - checkpointed.txt MUST exist with 'checkpointed-data'
	// - post_checkpoint.txt MUST NOT exist (uncheckpointed data lost on node death, matching contract)
	var outCheckpointed string
	outCheckpointed, err = retryCatInPod(h, podBName, "/data/checkpointed.txt", 15*time.Second)
	if err != nil || !strings.Contains(outCheckpointed, "checkpointed-data") {
		t.Fatalf("Expected checkpointed-data, got %q (err=%v)", outCheckpointed, err)
	}
	t.Logf("Pod B successfully found checkpointed file /data/checkpointed.txt")

	_, err = h.RunInPod(podBName, "default", "/bin/sh", "-c", "cat /data/post_checkpoint.txt")
	if err == nil {
		t.Fatalf("post_checkpoint.txt unexpectedly exists: writes after last checkpoint must be lost on node daemon death")
	}
	t.Logf("Pod B confirmed post_checkpoint.txt does not exist (contract verified: uncheckpointed data lost on node death)!")
}

func TestObjectFSExclusiveLocalXAttrE2E(t *testing.T) {
	requireE2EGroup(t, "exclusive")

	h, node1, node2 := setupObjectFSMultiNodeCluster(t)
	volumeID := "exclusive-local-xattr-vol"

	t.Cleanup(func() {
		if t.Failed() {
			h.DumpDiagnosticLogs(t)
			t.Logf("Node Daemon Logs:\n%s\n", h.GetPodLogs("app=objectfs-node-daemon", "default"))
			t.Logf("Controller Logs:\n%s\n", h.GetPodLogsByName("objectfs-controller-0", "default"))
		}
	})

	// Setup PVC with storageClassName: objectfs-exclusive-local and ReadWriteOncePod
	pvcYaml := fmt.Sprintf(`
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: %s-pvc
spec:
  accessModes:
    - ReadWriteOncePod
  storageClassName: objectfs-exclusive-local
  resources:
    requests:
      storage: 1Gi
`, volumeID)

	t.Logf("Creating PVC for exclusive-local xattr volume")
	h.KubectlApplyContent("exclusive-xattr-pvc", pvcYaml)

	// Step 1: Deploy Pod A on Node 1 in exclusive-local mode
	podAName := "exclusive-xattr-pod-a"
	podAYaml := fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: %s
spec:
  nodeSelector:
    kubernetes.io/hostname: %s
  containers:
    - name: app
      image: images.local/objectfs-node-daemon:e2e
      imagePullPolicy: Never
      command: ["/bin/sh", "-c", "sleep 3600"]
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: %s-pvc
`, podAName, node1, volumeID)

	t.Logf("Starting Pod A on Node 1 (%s)", node1)
	h.KubectlApplyContent(podAName, podAYaml)
	if err := h.WaitForPodReady(podAName, "default", 2*time.Minute); err != nil {
		t.Fatalf("Pod A failed to start: %v", err)
	}

	// In Pod A: write file and directory, set xattrs, then remove one xattr before checkpoint
	t.Logf("Pod A setting and removing extended attributes")
	_, err := h.RunInPod(podAName, "default", "/bin/sh", "-c", `
echo 'file-xattr-content' > /data/test_xattr.txt
mkdir /data/xattr_dir
setfattr -n user.preserved -v 'hello-xattr-world' /data/test_xattr.txt
setfattr -n user.removed -v 'should-not-exist' /data/test_xattr.txt
setfattr -x user.removed /data/test_xattr.txt
setfattr -n user.dir_attr -v 'dir-xattr-value' /data/xattr_dir
sync
`)
	if err != nil {
		t.Fatalf("Failed to set xattrs in Pod A: %v", err)
	}

	// Verify in Pod A
	outPreservedA, err := h.RunInPod(podAName, "default", "getfattr", "--absolute-names", "-n", "user.preserved", "--only-values", "/data/test_xattr.txt")
	if err != nil || strings.TrimSpace(outPreservedA) != "hello-xattr-world" {
		t.Fatalf("Pod A getfattr user.preserved failed: %v, out=%s", err, outPreservedA)
	}
	_, err = h.RunInPod(podAName, "default", "getfattr", "--absolute-names", "-n", "user.removed", "/data/test_xattr.txt")
	if err == nil {
		t.Fatalf("Pod A expected user.removed to be deleted before checkpoint, but getfattr succeeded")
	}

	// Step 2: Delete Pod A (triggers unmount checkpoint)
	t.Logf("Deleting Pod A on Node 1 to trigger unmount checkpoint")
	deletePodBounded(t, h, podAName, "default", 3*time.Minute)

	// Step 3: Deploy Pod B on Node 2 in exclusive-local mode
	podBName := "exclusive-xattr-pod-b"
	podBYaml := fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: %s
spec:
  nodeSelector:
    kubernetes.io/hostname: %s
  containers:
    - name: app
      image: images.local/objectfs-node-daemon:e2e
      imagePullPolicy: Never
      command: ["/bin/sh", "-c", "sleep 3600"]
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: %s-pvc
`, podBName, node2, volumeID)

	t.Logf("Starting Pod B on Node 2 (%s) mounting the checkpointed volume", node2)
	h.KubectlApplyContent(podBName, podBYaml)
	if err := h.WaitForPodReady(podBName, "default", 2*time.Minute); err != nil {
		t.Fatalf("Pod B failed to start: %v", err)
	}

	// Step 4: Verify extended attributes in Pod B on Node 2
	t.Logf("Pod B verifying preserved and removed extended attributes across remount")
	outPreservedB, err := h.RunInPod(podBName, "default", "getfattr", "--absolute-names", "-n", "user.preserved", "--only-values", "/data/test_xattr.txt")
	if err != nil || strings.TrimSpace(outPreservedB) != "hello-xattr-world" {
		t.Fatalf("Pod B did not see preserved xattr: err=%v, out=%s", err, outPreservedB)
	}

	_, err = h.RunInPod(podBName, "default", "getfattr", "--absolute-names", "-n", "user.removed", "/data/test_xattr.txt")
	if err == nil {
		t.Fatalf("Pod B expected user.removed to stay removed after checkpoint, but getfattr succeeded")
	}

	outDirB, err := h.RunInPod(podBName, "default", "getfattr", "--absolute-names", "-n", "user.dir_attr", "--only-values", "/data/xattr_dir")
	if err != nil || strings.TrimSpace(outDirB) != "dir-xattr-value" {
		t.Fatalf("Pod B did not see directory xattr: err=%v, out=%s", err, outDirB)
	}

	t.Logf("Pod B on Node 2 successfully verified xattrs across checkpoint and materialisation!")

	// Delete Pod B
	t.Logf("Deleting Pod B on Node 2")
	h.DeletePod(podBName, "default")
}
