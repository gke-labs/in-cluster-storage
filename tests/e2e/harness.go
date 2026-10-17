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
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gke-labs/gke-labs-infra/ktesting/e2e"
)

type Harness struct {
	*e2e.Harness
	t *testing.T
}

func NewHarness(t *testing.T, clusterName string) *Harness {
	return &Harness{
		Harness: e2e.NewHarness(t, clusterName),
		t:       t,
	}
}

// SetupMultiNode sets up a Kind cluster with a control plane and the requested number of worker nodes.
func (h *Harness) SetupMultiNode(workerNodes int) {
	if workerNodes <= 0 {
		h.Setup()
		return
	}

	out, err := exec.Command("kind", "get", "clusters").Output()
	if err != nil {
		h.t.Fatalf("failed to list kind clusters: %v", err)
	}
	clusters := strings.Fields(string(out))
	exists := false
	for _, c := range clusters {
		if c == h.ClusterName {
			exists = true
			break
		}
	}

	if !exists {
		var b strings.Builder
		b.WriteString("kind: Cluster\napiVersion: kind.x-k8s.io/v1alpha4\nnodes:\n- role: control-plane\n")
		for i := 0; i < workerNodes; i++ {
			b.WriteString("- role: worker\n")
		}
		configFile := filepath.Join(h.t.TempDir(), "kind-config.yaml")
		if err := os.WriteFile(configFile, []byte(b.String()), 0644); err != nil {
			h.t.Fatalf("failed to write kind config %s: %v", configFile, err)
		}
		cmd := exec.Command("kind", "create", "cluster", "--name", h.ClusterName, "--config", configFile)
		if out, err := cmd.CombinedOutput(); err != nil {
			h.t.Fatalf("failed to create kind cluster %s: %v (output: %s)", h.ClusterName, err, strings.TrimSpace(string(out)))
		}
	}

	h.Setup()
}

// GetWorkerNodeNames returns the names of all worker Kubernetes nodes in the cluster.
func (h *Harness) GetWorkerNodeNames() []string {
	out, err := exec.Command("kubectl", "get", "nodes", "-o", "jsonpath={.items[*].metadata.name}").Output()
	if err != nil {
		return nil
	}
	var workers []string
	for _, node := range strings.Fields(string(out)) {
		if !strings.Contains(node, "control-plane") {
			workers = append(workers, node)
		}
	}
	return workers
}

// GetNodeNames returns the names of all Kubernetes nodes in the cluster.
func (h *Harness) GetNodeNames() []string {
	out, err := exec.Command("kubectl", "get", "nodes", "-o", "jsonpath={.items[*].metadata.name}").Output()
	if err != nil {
		return nil
	}
	return strings.Fields(string(out))
}

func (h *Harness) getPodNames(selector, namespace string) []string {
	args := []string{"get", "pods", "-n", namespace}
	if selector != "" {
		args = append(args, "-l", selector)
	}
	args = append(args, "-o", "jsonpath={.items[*].metadata.name}")
	out, err := exec.Command("kubectl", args...).Output()
	if err != nil {
		return nil
	}
	return strings.Fields(string(out))
}

func (h *Harness) getContainerLogs(pod, container, namespace string, previous bool) string {
	args := []string{"logs", pod, "-n", namespace}
	if container != "" {
		args = append(args, "-c", container)
	}
	if previous {
		args = append(args, "--previous")
	}
	out, err := exec.Command("kubectl", args...).CombinedOutput()
	if err != nil {
		if previous {
			return ""
		}
		return fmt.Sprintf("failed to get logs: %v\n%s", err, strings.TrimSpace(string(out)))
	}
	return strings.TrimSpace(string(out))
}

func (h *Harness) DumpDiagnosticLogs(t *testing.T) {
	t.Log("======= DUMPING DIAGNOSTIC LOGS =======")

	// Pod overview
	if out, err := exec.Command("kubectl", "get", "pods", "-n", "default", "-o", "wide").CombinedOutput(); err != nil {
		t.Logf("Warning: failed to get pods in default namespace: %v (out: %s)", err, strings.TrimSpace(string(out)))
	} else {
		t.Logf("Pods in default namespace:\n%s\n", strings.TrimSpace(string(out)))
	}

	// Events
	t.Logf("Events:\n%s\n", h.GetEvents("default"))

	// Controller Logs (current & previous)
	if logs := h.getContainerLogs("agentfs-controller-0", "agentfs-controller", "default", false); logs != "" {
		t.Logf("AgentFS Controller Logs:\n%s\n", logs)
	}
	if prevLogs := h.getContainerLogs("agentfs-controller-0", "agentfs-controller", "default", true); prevLogs != "" {
		t.Logf("AgentFS Controller Previous Logs:\n%s\n", prevLogs)
	}
	ctrlPods := h.getPodNames("app=objectfs-controller", "default")
	if len(ctrlPods) == 0 {
		ctrlPods = []string{"objectfs-controller-0"}
	}
	for _, pod := range ctrlPods {
		for _, container := range []string{"objectfs-controller", "csi-provisioner"} {
			if logs := h.getContainerLogs(pod, container, "default", false); logs != "" {
				t.Logf("ObjectFS Controller (%s / %s) Logs:\n%s\n", pod, container, logs)
			}
			if prevLogs := h.getContainerLogs(pod, container, "default", true); prevLogs != "" {
				t.Logf("ObjectFS Controller (%s / %s) Previous Logs:\n%s\n", pod, container, prevLogs)
			}
		}
	}
	if logs := h.getContainerLogs("wal-buffer-0", "wal-buffer", "default", false); logs != "" {
		t.Logf("WAL Buffer Logs:\n%s\n", logs)
	}
	if prevLogs := h.getContainerLogs("wal-buffer-0", "wal-buffer", "default", true); prevLogs != "" {
		t.Logf("WAL Buffer Previous Logs:\n%s\n", prevLogs)
	}

	// PersistentVolumes overview
	out, err := exec.Command("kubectl", "get", "pv").CombinedOutput()
	if err != nil {
		t.Logf("Warning: failed to get persistentvolumes: %v (out: %s)", err, strings.TrimSpace(string(out)))
	} else {
		t.Logf("PersistentVolumes:\n%s\n", strings.TrimSpace(string(out)))
	}

	// Node Daemon Logs (current & previous, both node-daemon & node-driver-registrar containers)
	daemonPods := h.getPodNames("", "default")
	for _, pod := range daemonPods {
		if strings.Contains(pod, "node-daemon") || strings.Contains(pod, "agentfs") || strings.Contains(pod, "objectfs") {
			for _, container := range []string{"agentfs-node-daemon", "objectfs-node-daemon", "node-driver-registrar", "cas-node-daemon"} {
				if logs := h.getContainerLogs(pod, container, "default", false); logs != "" {
					t.Logf("Node Daemon (%s / %s) Logs:\n%s\n", pod, container, logs)
				}
				if prev := h.getContainerLogs(pod, container, "default", true); prev != "" {
					t.Logf("Node Daemon (%s / %s) Previous Logs:\n%s\n", pod, container, prev)
				}
			}
		}
	}

	// Any test pods: describe & logs
	allPods := h.getPodNames("", "default")
	for _, pod := range allPods {
		if strings.HasPrefix(pod, "layers-pod-") || strings.HasPrefix(pod, "test-pod-") || strings.HasPrefix(pod, "dev-vscode") || strings.HasPrefix(pod, "persistent-pod-") || strings.HasPrefix(pod, "failover-") || strings.HasPrefix(pod, "crash-") || strings.HasPrefix(pod, "outage-") || strings.HasPrefix(pod, "wal-") {
			if desc, err := exec.Command("kubectl", "describe", "pod", pod, "-n", "default").CombinedOutput(); err != nil {
				t.Logf("Warning: failed to describe pod %s: %v (out: %s)", pod, err, strings.TrimSpace(string(desc)))
			} else {
				t.Logf("Pod %s Description:\n%s\n", pod, strings.TrimSpace(string(desc)))
			}
			if logs := h.getContainerLogs(pod, "", "default", false); logs != "" {
				t.Logf("Pod %s Logs:\n%s\n", pod, logs)
			}
		}
	}

	t.Log("=========================================")
}

func (h *Harness) DeletePodWithTimeout(t *testing.T, name, namespace string, timeout time.Duration) {
	t.Logf("Deleting Pod %s in namespace %s with a timeout of %v", name, namespace, timeout)

	done := make(chan struct{})
	go func() {
		h.DeletePod(name, namespace)
		close(done)
	}()

	select {
	case <-done:
		t.Logf("Successfully deleted Pod %s", name)
	case <-time.After(timeout):
		t.Errorf("TIMED OUT waiting for Pod %s to be deleted (timeout: %v)", name, timeout)
		h.DumpDiagnosticLogs(t)
		t.FailNow()
	}
}

// RunInPodWithTimeout executes a command inside a pod within a timeout duration.
func (h *Harness) RunInPodWithTimeout(timeout time.Duration, name, namespace string, cmd ...string) (string, error) {
	h.t.Helper()
	type result struct {
		out string
		err error
	}
	ch := make(chan result, 1)
	go func() {
		out, err := h.RunInPod(name, namespace, cmd...)
		ch <- result{out: out, err: err}
	}()
	select {
	case res := <-ch:
		return res.out, res.err
	case <-time.After(timeout):
		return "", fmt.Errorf("timed out running command in pod %s/%s after %v: %v", namespace, name, timeout, cmd)
	}
}

// GetPVForPVC returns the volume name (PV name) bound to the given PVC.
func (h *Harness) GetPVForPVC(pvcName, namespace string) (string, error) {
	h.t.Helper()
	out, err := exec.Command("kubectl", "get", "pvc", pvcName, "-n", namespace, "-o", "jsonpath={.spec.volumeName}").CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("failed to get PV for PVC %s/%s: %w (output: %s)", namespace, pvcName, err, string(out))
	}
	pvName := strings.TrimSpace(string(out))
	if pvName == "" {
		return "", fmt.Errorf("PV for PVC %s/%s is empty", namespace, pvcName)
	}
	return pvName, nil
}

// DeletePVC deletes a PersistentVolumeClaim by name in the specified namespace.
func (h *Harness) DeletePVC(name, namespace string) {
	h.t.Helper()
	if err := exec.Command("kubectl", "delete", "pvc", name, "-n", namespace, "--ignore-not-found").Run(); err != nil {
		h.t.Logf("Warning: failed to delete pvc %s in namespace %s: %v", name, namespace, err)
	}
}

// WaitForPVDeleted waits until a PersistentVolume is completely deleted from the cluster.
func (h *Harness) WaitForPVDeleted(name string, timeout time.Duration) error {
	h.t.Helper()
	h.t.Logf("Waiting for persistentvolume %s to be deleted", name)
	start := time.Now()
	for {
		if time.Since(start) > timeout {
			desc, err := exec.Command("kubectl", "get", "persistentvolume", name, "-o", "yaml").CombinedOutput()
			if err != nil {
				h.t.Logf("Warning: failed to get persistentvolume %s yaml: %v (out: %s)", name, err, strings.TrimSpace(string(desc)))
			} else {
				h.t.Logf("Stuck PersistentVolume %s:\n%s\n", name, strings.TrimSpace(string(desc)))
			}
			return fmt.Errorf("timed out waiting for persistentvolume %s to be deleted after %s", name, timeout)
		}
		cmd := exec.Command("kubectl", "get", "persistentvolume", name, "--ignore-not-found", "-o", "jsonpath={.metadata.name}")
		out, err := cmd.CombinedOutput()
		outputStr := strings.TrimSpace(string(out))
		if err != nil {
			if strings.Contains(outputStr, "NotFound") || strings.Contains(outputStr, "not found") {
				h.t.Logf("PersistentVolume %s successfully deleted", name)
				return nil
			}
			h.t.Logf("Warning: error querying persistentvolume %s: %v (output: %s)", name, err, outputStr)
		} else if outputStr == "" {
			h.t.Logf("PersistentVolume %s successfully deleted", name)
			return nil
		}
		time.Sleep(2 * time.Second)
	}
}
