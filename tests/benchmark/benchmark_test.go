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

package benchmark

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gke-labs/in-cluster-storage/tests/e2e"
)

func TestBenchmark(t *testing.T) {
	if os.Getenv("RUN_BENCH") != "1" {
		t.Skip("Skipping benchmark; RUN_BENCH not set")
	}

	h := e2e.NewHarness(t, "agentfs-bench")
	h.Setup()

	gitRoot := h.GetGitRoot()
	experimentRoot := gitRoot

	// Build images
	h.DockerBuild("agentfs-controller:bench", filepath.Join(experimentRoot, "images/agentfs-controller/Dockerfile"), experimentRoot)
	h.DockerBuild("agentfs-node-daemon:bench", filepath.Join(experimentRoot, "images/agentfs-node-daemon/Dockerfile"), experimentRoot)
	h.DockerBuild("benchmark-agentfs:latest", filepath.Join(experimentRoot, "images/benchmark-agentfs/Dockerfile"), experimentRoot)

	// Load images into Kind
	h.KindLoad("agentfs-controller:bench")
	h.KindLoad("agentfs-node-daemon:bench")
	h.KindLoad("benchmark-agentfs:latest")

	// Read manifest and replace placeholders
	manifestPath := filepath.Join(experimentRoot, "k8s/manifest.yaml")
	b, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("Failed to read manifest: %v", err)
	}
	manifest := string(b)
	manifest = strings.ReplaceAll(manifest, "namespace: kube-agentfs-system", "namespace: default")

	manifest = strings.ReplaceAll(manifest, "image: agentfs-controller:latest", "image: agentfs-controller:bench\n          imagePullPolicy: Never")
	manifest = strings.ReplaceAll(manifest, "image: agentfs-node-daemon:latest", "image: agentfs-node-daemon:bench\n          imagePullPolicy: Never")

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

	// Apply benchmark job
	jobPath := filepath.Join(experimentRoot, "tests/bench/testdata/benchmark-job.yaml")
	jobBytes, err := os.ReadFile(jobPath)
	if err != nil {
		t.Fatalf("Failed to read job manifest: %v", err)
	}

	h.KubectlApplyContent("benchmark-job", string(jobBytes))

	// Wait for job
	if err := h.WaitForJobSuccess("agentfs-bench-job", "default", 5*time.Minute); err != nil {
		t.Logf("Job Logs:\n%s\n", h.GetPodLogs("job-name=agentfs-bench-job", "default"))
		t.Logf("Node Daemon Logs:\n%s\n", h.GetPodLogs("app=agentfs-node-daemon", "default"))
		t.Fatalf("Benchmark Job failed: %v", err)
	}

	t.Logf("Benchmark Job succeeded")
	t.Logf("Job Logs:\n%s\n", h.GetPodLogs("job-name=agentfs-bench-job", "default"))
}
