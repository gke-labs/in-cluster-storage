# Developer & Agent Guidelines: Running E2E Tests Locally

This document describes how to set up your environment and run the full end-to-end (E2E) test suite (`dev/ci/presubmits/ap-e2e`) locally or within autonomous agent containers before submitting code changes.

---

## Prerequisites

To run the E2E tests, the following tools must be available in your execution environment:

1. **Go Toolchain** (Go 1.26.4 or later, or run with `GOTOOLCHAIN=auto`)
2. **Docker Client CLI** (pointing to a running Docker daemon via `DOCKER_HOST` if containerized)
3. **kind** (Kubernetes in Docker)
4. **kubectl** (Kubernetes command-line tool)

---

## 1. Setting Up the Environment (Inside Agent Containers)

If you are running within a headless agent pod or similar isolated CI environment, you can dynamically download and prepare the missing client binaries using these commands:

```bash
# 1. Download and install kubectl
curl -Lo /usr/local/bin/kubectl "https://dl.k8s.io/release/v1.30.0/bin/linux/amd64/kubectl"
chmod +x /usr/local/bin/kubectl

# 2. Download and install kind
curl -Lo /usr/local/bin/kind "https://kind.sigs.k8s.io/dl/v0.23.0/kind-linux-amd64"
chmod +x /usr/local/bin/kind

# 3. Extract and install the Docker client CLI
curl -fsSL https://download.docker.com/linux/static/stable/x86_64/docker-26.1.3.tgz -o /tmp/docker.tgz
tar -C /usr/local/bin -xzvf /tmp/docker.tgz docker/docker --strip-components=1
rm /tmp/docker.tgz
```

Ensure your `DOCKER_HOST` environment variable is exported and points to the running dockerd socket.

---

## 2. Running the E2E Tests

Once the prerequisites are satisfied, you can execute the E2E test suite locally using the repository's presubmit wrapper script:

```bash
# Force the Go toolchain to auto-resolve matching versions and execute the E2E runner
GOTOOLCHAIN=auto ./dev/ci/presubmits/ap-e2e
```

The E2E suite will:
1. Spin up a local `kind` Kubernetes cluster named `agentfs-e2e`.
2. Compile and build the `agentfs-controller:e2e` and `agentfs-node-daemon:e2e` Docker images.
3. Load the built images directly into the `kind` cluster nodes.
4. Deploy the Kubernetes CSI controller and daemonsets.
5. Create and run test pods verifying normal OverlayFS operations, incremental snapshitting, whiteouts, and (if active) hybrid lazy-loading.
6. Automatically clean up and teardown the cluster.

---

## 3. Troubleshooting & Diagnostics

- **Docker Communication Errors**: Run `docker ps` to verify that the Docker CLI can communicate successfully with the daemon.
- **YAML Manifest Errors**: Ensure that modifications to the CSI driver arguments in `tests/e2e/e2e_test.go` or `k8s/manifest.yaml` are clean, properly indented, and have correctly matched quotes.
- **Interception Deadlocks**: If testing on kernels `< 6.5` with fanotify, do not monitor the underlying physical directory (`/var/lib/agentfs`) directly. Instead, mark the active OverlayFS virtual mount points (`targetPath`) to ensure events are captured reliably across all host kernel distributions.
- **Inspecting Node Daemon Logs**: If a test pod hangs, read the node driver logs via `kubectl logs daemonset/agentfs-node-daemon -n default -c agentfs-node-daemon` to see if events are triggering and being allowed cleanly.
