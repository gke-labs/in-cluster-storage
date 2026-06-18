# Research: Content Addressable Storage (CAS) via CSI

This document records the research, design decisions, and implementation details for supporting **Content Addressable Storage (CAS)** via a custom Unix Domain Socket (UDS) API in the `in-cluster-storage` (AgentFS) CSI driver.

## Objective

The goal is to implement high-throughput, low-latency access to cached blob data directly from containers inside a Pod without having the data go through user-space memory of the Pod during data retrieval, particularly when the blob is already cached locally on the Kubernetes node.

We wanted to explore and validate an approach using:
1. A well-known Unix Domain Socket (UDS) path inside the volume: `.in-cluster-storage/api`.
2. A lightweight request-response protocol to query blobs by their content hash (SHA256).
3. The use of **`SCM_RIGHTS`** to transfer an open file descriptor (FD) of the locally stored/cached blob from the CSI Node Daemon directly to the container process.

---

## Architectural Design

### 1. Unix Domain Socket (UDS) in Mounted Volume
When `NodePublishVolume` is called, the CSI driver mounts an OverlayFS filesystem to `targetPath`.
- After mounting, the Node Daemon creates a directory `.in-cluster-storage` and binds a Unix Domain Socket at `api` (full path `targetPath/.in-cluster-storage/api`).
- Because UDS files reside in the mounted directory structure, they are visible and accessible to any container inside the Pod mounting this volume.
- Containers can establish connections to this UDS across the container/pod mount and PID namespaces.

### 2. SCM_RIGHTS File Descriptor Passing
Instead of streaming bytes over a network socket or copying them through multiple user-space buffers (which incurs high CPU, context switching, and memory copying overhead), the Node Daemon accepts requests over the UDS and uses the auxiliary control message **`SCM_RIGHTS`** to return an open file descriptor pointing to the cached blob on the node's host filesystem.

- **Zero-Copy Retrieval:** Once the container process receives the file descriptor, it can use system calls like `mmap`, `sendfile`, or direct `read` to access the blob content. The data transfer bypasses any middleman process, avoiding user-space memory copies in the Pod.
- **Node-level Cache sharing:** Multiple pods or containers requesting the same blob can be handed file descriptors pointing to the same read-only underlying host file, maximizing page cache sharing across Pods.

---

## Protocol Specification

The custom UDS API implements a simple, lightweight text-based protocol:

### Request
The client sends a single line specifying the command and the SHA256 of the requested content:
```text
GET <64-char-hex-sha256>\n
```

### Success Response
The server retrieves/downloads the blob, opens it on the host filesystem, and responds with:
```text
OK <file_size>\n
```
*Crucially*, the server includes the open file descriptor in the socket's Out-Of-Band (OOB) / Ancillary data using `SCM_RIGHTS` (via `unix.WriteMsgUnix`).

### Error Response
If the blob cannot be found, cannot be downloaded, or the request format is invalid, the server responds with:
```text
ERR <error_message>\n
```

---

## Findings & Key Lessons

### 1. OverlayFS Compatibility
Creating and binding a Unix Domain Socket inside an active OverlayFS mount (specifically in the merged directory `targetPath`) works seamlessly. Since the `targetPath` has a writable `upperdir` and `workdir`, OverlayFS supports creating special files, including sockets.

### 2. Namespace Traversal
Unix Domain Sockets are extremely robust for cross-namespace communication. A process inside a container namespace (the Pod) can connect to a socket file created by a process in the host namespace (the Node Daemon) as long as the file path is accessible in the container's mount namespace.

### 3. Preventing Mount Pollution and `EBUSY` Failures
When a Kubernetes volume is unmounted (`NodeUnpublishVolume`), any open files or active sockets inside the mount can cause the `umount` syscall to fail with `EBUSY`.
- **Mitigation:** In our implementation, we explicitly stop the CAS server corresponding to `targetPath` before attempting to unmount. Stopping the server:
  1. Closes the listener socket.
  2. Forcibly closes all active/accepted connections to the socket.
  3. Deletes the socket file from the filesystem.
- This ensures that unmounting proceeds smoothly without resource leaks or system hangs.

### 4. Node-Wide Shared Cache
By routing all CAS queries to a central node-wide cache directory (e.g., `/var/lib/agentfs/blobs`), we avoid duplicate downloads across different volumes or different pods on the same node. If a requested blob is missing locally, the Node Daemon dynamically downloads it from the Controller and caches it in the node-wide cache directory for future CAS or mount requests.

---

## Future Extensions
While SCM_RIGHTS is highly efficient and provides immediate benefits, other mechanisms could be explored in future phases:
- **FUSE-based virtual file views:** Dynamically presenting only requested blobs under a virtual directory.
- **Page table/memory mapping:** Directly mapping file pages into container address spaces.
