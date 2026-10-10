# Developer & Agent Guidelines

This document describes how to set up your environment and run the full end-to-end (E2E) test suite (`dev/ci/presubmits/ap-e2e`) locally or within autonomous agent containers before submitting code changes (sections 1–3), how to handle errors (section 4), and how to write commit messages and PR descriptions (section 5).

---

## Prerequisites

To run the E2E tests, the following tools must be available in your execution environment:

1. **Go Toolchain** (Go 1.27.2 or later, or run with `GOTOOLCHAIN=auto`)
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

### Race Detection Gate (`ap-test-race`)

`dev/ci/presubmits/ap-test-race` is the race gate that runs unit and integration tests with the Go race detector enabled (`-race -count=1 -short`). Packages are added to it as they become race-clean (currently `pkg/sds/...`, `pkg/wal/...`, `pkg/objectfs/fuse/...`, `pkg/objectfs/blob/...`, and `pkg/objectfs/controller/...`). Always run `ap-test-race` locally (e.g., `GOTOOLCHAIN=auto ./dev/ci/presubmits/ap-test-race`) before pushing changes to those packages.

---

## 3. Troubleshooting & Diagnostics

- **Docker Communication Errors**: Run `docker ps` to verify that the Docker CLI can communicate successfully with the daemon.
- **YAML Manifest Errors**: Ensure that modifications to the CSI driver arguments in `tests/e2e/e2e_test.go` or `k8s/manifest.yaml` are clean, properly indented, and have correctly matched quotes.
- **Interception Deadlocks**: If testing on kernels `< 6.5` with fanotify, do not monitor the underlying physical directory (`/var/lib/agentfs`) directly. Instead, mark the active OverlayFS virtual mount points (`targetPath`) to ensure events are captured reliably across all host kernel distributions.
- **Inspecting Node Daemon Logs**: If a test pod hangs, read the node driver logs via `kubectl logs daemonset/agentfs-node-daemon -n default -c agentfs-node-daemon` to see if events are triggering and being allowed cleanly.

---

## 4. Error Handling Mandate

**Never ignore errors silently.** If an error cannot be proven safe to discard or recover from, it must be returned to the caller or explicitly logged with technical context. Silently discarding errors hides corrupted state, data loss, and race conditions.

---

## 5. Commit Messages and PR Descriptions

**The code and its commit history must stand on their own.** PR descriptions do not become part of the repository; commit messages do. Anything a future reader needs to understand a change belongs in the commit messages, and the PR description should simply repeat them.

- **Subject line:** `area: imperative summary`, about 72 characters at most, where `area` is the package or component touched (for example `objectfs`, `objectfs/fuse`, `sds/view`, `proto/objectfs`, `ci/posix`, `docs`).
- **Body: explain why, not just what.** Include whatever the diff alone doesn't make obvious:
  - **Bug fixes:** the symptom, the root cause, why the change fixes it, and the test that reproduces it.
  - **Design changes:** the approach taken, and the alternatives considered and why they were rejected.
  - **Performance changes:** the numbers, how they were measured, and in what configuration.
  - **Review answers:** if a reviewer had to ask, the answer probably belongs in the message.
- **One logical change per commit**, each building and passing tests where practical (for example: proto changes, then implementation, then tests or CI).
- **Keep messages true to the final code.** When review changes the approach, rewrite the affected commit messages (amend, or `fixup!` commits squashed with `git rebase --autosquash`) so they describe what the code does now, not the history of attempts. If you add follow-up commits instead, give each one a body explaining what changed and why; a bare `fixup!` subject is not enough. Never leave a message describing code that was later removed.
- **Reference issues** in the commit body: `Fixes #N` for the commit that completes an issue, `Part of #N` otherwise.
- **PR description = the commit messages**, in order, for multi-commit PRs. If a reviewer would need information that isn't in the commits, add it to the commits rather than only to the PR description.
