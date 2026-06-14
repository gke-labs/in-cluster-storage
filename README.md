# AgentFS

AgentFS is an in-cluster, container-native Container Storage Interface (CSI) filesystem driver designed for Kubernetes. It allows pods to mount storage volumes backed by versioned, snapshot-centric storage, with files and blobs managed dynamically within the cluster.

---

## Architecture

AgentFS consists of two main components:

1. **AgentFS Controller (`agentfs-controller`)**
   - Managed as a stateful service within the cluster (e.g., as a `StatefulSet`).
   - Serves as the central registry for volume snapshots and binary data blobs.
   - Stores and tracks snapshot metadata (`latest.pb`) and provides gRPC APIs for retrieving (`GetLatestSnapshot`) and uploading (`UploadSnapshot`, `UploadBlob`, `HasBlob`) files.

2. **AgentFS Node Daemon (`agentfs-node-daemon`)**
   - Deployed as a `DaemonSet` on every node in the Kubernetes cluster.
   - Implements the CSI node plugin interfaces (`NodePublishVolume`, `NodeUnpublishVolume`).
   - Dynamically mounts and unmounts volumes in response to pod lifecycle events.

---

## Optimized Volume Layering with OverlayFS

To optimize both CPU, disk I/O, and network usage on large volumes, AgentFS leverages **OverlayFS-based volume layering**:

### Traditional vs. Layered Mounts
- **Traditional Approach:** On mount, every file was copied down from the controller. On unmount, the node daemon recursively walked the entire volume, computed SHA256 hashes of every file to identify modified ones, uploaded missing files, and published a new snapshot. For large volumes, this full-scan process is extremely resource-intensive.
- **Layered (OverlayFS) Approach:** AgentFS avoids full-volume scans entirely. The daemon structures each volume with three separate directories:
  - **Lower Layer (`lower/`):** A read-only directory containing the base files downloaded during mount (`pullSnapshot`).
  - **Upper Layer (`upper/`):** A read-write directory that captures all new files, modifications, and deletions.
  - **Work Layer (`work/`):** Internal metadata workspace required by OverlayFS.
  - **Merged View (`targetPath`):** The final OverlayFS mount that is exposed to the Pod.

### Delta-Based Snapshot Pushes
During unmount (`NodeUnpublishVolume`), AgentFS scans **only the upper layer (`upper/`)** instead of the entire filesystem:
- **New & Modified Files:** Directly read from `upper/`, hashed, uploaded if missing on the controller, and updated in the snapshot metadata.
- **Deleted Files & Directories:** Identified by **OverlayFS whiteouts** (special character devices with major/minor numbers `0, 0` or opaque directory markers). Deleted files and their descendants are pruned from the final snapshot metadata.
- **Unmodified Files:** Remain intact from the base snapshot without being read, hashed, or processed.

---

## Deployment

Deploy the AgentFS components directly to your cluster:

```bash
kubectl apply -f k8s/manifest.yaml
```

This sets up the `CSIDriver` definition, Service Accounts, RBAC, the AgentFS Controller `StatefulSet`, and the Node Daemon `DaemonSet`.

### Example Pod Usage

Once deployed, pods can mount AgentFS volumes by referencing the CSI driver in their spec:

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: agentfs-example-pod
spec:
  containers:
    - name: app
      image: alpine
      command: ["/bin/sh", "-c", "echo 'hello from agentfs' > /data/message.txt && sleep 3600"]
      volumeMounts:
        - name: agentfs-vol
          mountPath: /data
  volumes:
    - name: agentfs-vol
      csi:
        driver: agentfs.labs.gke.io
        volumeAttributes:
          volumeID: my-shared-volume
```

---

## Testing & Benchmarks

The project comes with a comprehensive testing and benchmark suite.

### Running Unit Tests
Execute standard Go unit tests:
```bash
GOTOOLCHAIN=auto go test -v ./...
```

### Running E2E Integration Tests
The end-to-end integration test launches a local Kind cluster, builds current Docker images, deploys AgentFS, and verifies write-read compatibility across multiple sequentially scheduled pods:
```bash
GOTOOLCHAIN=auto RUN_E2E=1 go test -v ./tests/e2e/...
```

### Running Simulation Benchmarks
Run a randomized simulation benchmark that stresses create, append, overwrite, delete, and permission change operations:
```bash
GOTOOLCHAIN=auto RUN_BENCH=1 go test -v ./tests/benchmark/...
```

---

## Contributing

This project is licensed under the [Apache 2.0 License](LICENSE).

We welcome contributions! Please see [docs/contributing.md](docs/contributing.md) for more information.

We follow [Google's Open Source Community Guidelines](https://opensource.google.com/conduct/).

## Disclaimer

This is not an officially supported Google product.

This project is not eligible for the Google Open Source Software Vulnerability Rewards Program.
