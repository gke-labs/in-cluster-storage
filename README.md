# ObjectFS

ObjectFS is an in-cluster, container-native distributed filesystem and Container Storage Interface (CSI) driver designed for Kubernetes. Engineered to bridge cloud object storage (Google Cloud Storage, Amazon S3) with low-latency local execution, ObjectFS provides POSIX filesystem semantics backed by structured data streams and content-addressable blob storage.

---

## Architecture

ObjectFS separates metadata management from object payload storage, composed of the following core components:

1. **ObjectFS Controller (`objectfs-controller`)**
   - Managed as a stateful service within the cluster (e.g., as a `StatefulSet`).
   - Maintains authoritative volume metadata, directory hierarchies, and inode attributes.
   - Coordinates with the write-ahead log (WAL) and Structured Data Streams (SDS) for deterministic crash recovery and linearizable mutation sequencing.
   - Persists point-in-time index snapshots and manages content-addressable blob manifests.

2. **ObjectFS Node Daemon (`objectfs-node-daemon`)**
   - Deployed as a `DaemonSet` on every node in the Kubernetes cluster.
   - Implements the CSI node plugin interfaces (`NodePublishVolume`, `NodeUnpublishVolume`).
   - Serves standard FUSE mounts via a high-performance user-space client (`go-fuse`) with local node caching and push-based cache invalidation.
   - Manages kernel-native `exclusive-local` OverlayFS mounts with eager materialisation and automated checkpointing.

3. **WAL Buffer (`wal-buffer`)**
   - Central append-only streaming buffer providing sub-millisecond durable logging for metadata mutations before they are batched to cloud object storage.

---

## Mount Modes

ObjectFS offers two distinct mount modes depending on workload access patterns:

### 1. Standard FUSE Mode
- Ideal for shared, multi-reader, or distributed workloads.
- Near-instant volume readiness without upfront data synchronization.
- Read and write requests are handled on-demand via the node FUSE daemon, backed by intelligent in-memory and disk caching with write-through fsync semantics.

### 2. Exclusive-Local Mode (`mode: exclusive-local`)
- Designed for high-throughput single-writer workloads (e.g., machine learning training, compilation, local databases) requiring native disk I/O performance.
- Dynamically provisions via the `objectfs-exclusive-local` `StorageClass` with `ReadWriteOncePod` access mode.
- Eagerly materialises volume files to node-local storage and mounts them using kernel-native **OverlayFS**.
- The pod runs at raw local filesystem speed without FUSE indirection.
- Upper-layer mutations are cleanly checkpointed back to the controller:
  - Automatically upon pod termination (`NodeUnpublishVolume`).
  - At configurable periodic intervals (`interval` attribute).
  - On-demand via explicit `objectfsctl checkpoint` RPC triggers.

---

## Deployment

Deploy the ObjectFS components to your Kubernetes cluster:

```bash
# 1. Deploy the WAL buffer service
kubectl apply -f k8s/wal.yaml

# 2. Deploy ObjectFS controller, node daemon, and storage classes
kubectl apply -f k8s/objectfs.yaml
```

### Example Pod Usage (CSI Ephemeral Volume)

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: objectfs-example-pod
spec:
  containers:
    - name: app
      image: alpine
      command: ["/bin/sh", "-c", "echo 'hello from objectfs' > /data/message.txt && sleep 3600"]
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      csi:
        driver: objectfs.labs.gke.io
        volumeAttributes:
          volumeID: my-shared-volume
```

For persistent volume claim (PVC) workflows and detailed configuration options, see the [ObjectFS Documentation](docs/objectfs.md) and [Installation Guide](docs/install.md).

---

## Testing & Verification

The repository contains comprehensive unit, integration, chaos, and end-to-end test suites.

### Running Unit Tests
Execute standard Go unit tests across all packages:
```bash
GOTOOLCHAIN=auto go test -v ./...
```

### Running E2E Presubmit Groups
The end-to-end test suite runs against local `kind` clusters and is divided into focused presubmit groups:
```bash
# Core single-node ObjectFS and WAL tests
GOTOOLCHAIN=auto ./dev/ci/presubmits/ap-e2e-core

# Multi-node failover, cross-node visibility, and exclusive-local checkpoint tests
GOTOOLCHAIN=auto ./dev/ci/presubmits/ap-e2e-multinode

# Fault injection, crash mid-write, outage, and WAL restart tests
GOTOOLCHAIN=auto ./dev/ci/presubmits/ap-e2e-chaos
```

### Race Detection
Run unit and integration tests with the Go race detector enabled:
```bash
GOTOOLCHAIN=auto ./dev/ci/presubmits/ap-test-race
```

---

## Contributing

This project is licensed under the [Apache 2.0 License](LICENSE).

We welcome contributions! Please see [docs/contributing.md](docs/contributing.md) for more information.

We follow [Google's Open Source Community Guidelines](https://opensource.google.com/conduct/).

## Disclaimer

This is not an officially supported Google product.

This project is not eligible for the Google Open Source Software Vulnerability Rewards Program.
