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
	"testing"
	"time"

	"github.com/gke-labs/gke-labs-infra/ktesting/e2e"
)

type Harness struct {
	*e2e.Harness
}

func NewHarness(t *testing.T, clusterName string) *Harness {
	return &Harness{
		Harness: e2e.NewHarness(t, clusterName),
	}
}

func (h *Harness) DumpDiagnosticLogs(t *testing.T) {
	t.Log("======= DUMPING DIAGNOSTIC LOGS =======")
	t.Logf("Events:\n%s\n", h.GetEvents("default"))
	t.Logf("AgentFS Controller Logs:\n%s\n", h.GetPodLogsByName("agentfs-controller-0", "default"))
	t.Logf("AgentFS Node Daemon Logs:\n%s\n", h.GetPodLogs("app=agentfs-node-daemon", "default"))
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
