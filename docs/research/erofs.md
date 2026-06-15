# Research: Optimizing AgentFS Base Layer Population with EROFS

**Author:** Gemini CLI / codebot-robot  
**Date:** June 15, 2026  
**Status:** Proposal / Research Phase  
**Issue Reference:** Issue #8 (Try building EROFS filesystem)

---

## 1. Executive Summary

In the current implementation of AgentFS, when a volume snapshot is published to a node (`NodePublishVolume`), the base read-only layer is prepared in the `lower/` directory of the OverlayFS mount. If the hybrid lazy-loading protocol is enabled via `--lazy-load-threshold`, files exceeding the threshold are not immediately downloaded. Instead, the driver iterates over every file in the snapshot and manually creates an empty "stub" (sparse) file of the original size with matching metadata (timestamps, mode) to serve as a placeholder for the `fanotify` interceptor.

While this approach works, it scales poorly with the number of files ($N$) in a snapshot:
* For snapshots with tens or hundreds of thousands of files, creating these directories and sparse files sequentially requires hundreds of thousands of syscalls (`MkdirAll`, `OpenFile`, `Truncate`, `Chtimes`, `Chmod`).
* This $O(N)$ system call and disk I/O overhead introduces significant mount latency, negating some of the startup benefits of lazy-loading.

This research documents how **EROFS (Enhanced Read-Only File System)** can be leveraged to optimize base layer population. By packaging snapshot metadata into a single EROFS image at build/controller time, we can populate the entire directory structure and stub file metadata on the node with **exactly one system call** (`mount`).

---

## 2. Current Approach: Fanotify Stub Creation

Under the current `fanotify`-based hybrid lazy-loading flow, the `agentfs-node-daemon` executes the following sequence during `pullSnapshot`:

```
[For each file in Snapshot Metadata]
     |
     +---> 1. MkdirAll() for parent directories (multiple mkdir syscalls)
     +---> 2. os.Stat() to check for existing file
     +---> 3. OpenFile(targetFile, O_CREATE|O_WRONLY) -> Create fd
     +---> 4. Truncate(fileSize) -> Set sparse file size
     +---> 5. Close() -> Close fd
     +---> 6. os.Chtimes() -> Set modification and access times
     +---> 7. os.Chmod() -> Set file mode/permissions
```

### Bottlenecks and Overhead:
1. **Syscall Amplification:** A snapshot containing $N$ lazy-loaded files requires at least $5N$ system calls to create placeholders.
2. **Metadata Write Amplification:** Every stub creation generates write operations in the underlying filesystem journal and metadata blocks (inodes, directory blocks).
3. **Single-Threaded Bottleneck:** This loop runs sequentially on the node, which heavily bottlenecks pod startup on volumes containing complex directory layouts (e.g., deep node_modules, datasets, or model checkpoints).

---

## 3. The EROFS Solution: "One Syscall" Metadata Mounting

EROFS is a highly efficient, lightweight read-only filesystem natively supported in mainline Linux kernels (since 5.4, with active enhancements in 5.15+). A key feature of EROFS is its ability to separate filesystem **metadata** from actual **data blocks** (specifically designed for container image lazy-loading, as used in CNCF Nydus/Dragonfly).

### The EROFS "Bootstrap" (Metadata-Only) Concept
Using `mkfs.erofs`, we can compile a snapshot's directory hierarchy and file metadata into a single read-only filesystem image. 

We can configure EROFS in two primary ways:
1. **Metadata-Only (Index/Bootstrap Mode):** We build an EROFS image containing all directories, symlinks, file names, sizes, permissions, and timestamps, but with no physical file data.
2. **Chunk-based Image with Blob Device:** EROFS files point to logical chunks stored on an external block or blob device (`--blobdev`), which are pulled on demand by the kernel's `fscache` driver.

By adopting EROFS metadata images, we can replace the entire sequential loop of stub creation on the node:
* **The Controller** builds a small, pre-compiled EROFS image representing the snapshot's directory structure and metadata, and stores it as a blob.
* **The Node Daemon** downloads this single EROFS image (typically just a few kilobytes to megabytes, even for millions of files) during `NodePublishVolume`.
* **The Node Daemon** mounts the EROFS image directly onto the `lower/` directory using a single `mount(2)` system call:
  ```bash
  mount -t erofs -o loop /var/lib/agentfs/snapshots/volume-123.img /var/lib/agentfs/volumes/volume-123/lower
  ```
With this single system call, the kernel parses the EROFS superblock, and **the entire directory tree and all file stubs immediately become visible to the system** with zero file-system writes or sequential loop overhead.

---

## 4. How EROFS Integrates with the Hybrid Lazy-Loading Protocol

To retain our existing user-space `fanotify` lazy-loading mechanism (which is highly compatible and does not require complex `fscache`/kernel socket setups), EROFS can be integrated seamlessly as the `lower` layer in the OverlayFS mount.

### Architectural Workflow

```
+-----------------------------------------------------------------------------+
|                                BUILD TIME                                  |
|                                                                             |
|  1. Snapshot files are uploaded to Controller.                              |
|  2. Controller executes mkfs.erofs --tar=headerball to build a compact      |
|     metadata-only EROFS bootstrap image representing the snapshot.          |
+-----------------------------------------------------------------------------+
                                       |
                                       v
+-----------------------------------------------------------------------------+
|                            PUBLISH / MOUNT TIME                             |
|                                                                             |
|  1. Node Daemon fetches the compact EROFS metadata-only bootstrap image.     |
|  2. Node Daemon mounts EROFS to lower/:                                      |
|     mount -t erofs -o loop snapshot.img /var/lib/agentfs/volumes/vol/lower  |
|  3. Node Daemon configures fanotify on the target directory.                |
|  4. Node Daemon mounts OverlayFS:                                           |
|     mount -t overlay overlay -o lowerdir=lower/,upperdir=upper/,workdir=wd  |
|  5. Container starts instantly! (Entire layout is visible via lower/ mount)  |
+-----------------------------------------------------------------------------+
                                       |
                                       v
+-----------------------------------------------------------------------------+
|                             ACCESS / LAZY-LOAD                              |
|                                                                             |
|  1. Container opens a file (e.g. model.bin, which is in EROFS lower/).     |
|  2. fanotify catches FAN_OPEN_PERM event and blocks the container.          |
|  3. Node Daemon downloads model.bin blob from Controller.                    |
|  4. Node Daemon writes model.bin directly into upper/ target path.          |
|  5. Node Daemon responds with FAN_ALLOW.                                    |
|  6. Container reads model.bin directly from upper/ (OverlayFS copy-up logic)|
+-----------------------------------------------------------------------------+
```

### Key Advantages of This Hybrid Setup:
* **True $O(1)$ Setup:** The node setup time is completely decoupled from the snapshot's file count.
* **Seamless OverlayFS Copy-Up:** Writing the downloaded file directly into the `upper/` directory is standard OverlayFS copy-up behavior. The kernel automatically handles redirection so subsequent reads and writes go straight to the fast local `upper/` layer.
* **No Kernel Socket/fscache Complexity:** We avoid having to configure or mount `cachefilesd` or configure kernel-level `fscache` user-space sockets, which can be highly complex and fragile across different host distributions.

---

## 5. Detailed Comparison & Trade-off Analysis

| Metric / Dimension | Current Approach (Fanotify Stub Loop) | Proposed Approach (EROFS + Fanotify) |
| :--- | :--- | :--- |
| **Node Population Time** | $O(N)$ — Scales linearly with file count. Slow for large structures. | $O(1)$ — Instantaneous, independent of file count. |
| **System Calls (Node)** | $5N$ to $10N$ syscalls per mount. | **Exactly 1 system call** (`mount`). |
| **Disk I/O (Node)** | High disk metadata write amplification (creating inodes/directories). | Zero write amplification (read-only mount). |
| **Memory Footprint** | Low, but requires tracking all pending files in-memory in Go. | Lower; kernel manages EROFS metadata page cache efficiently. |
| **Kernel Requirements** | High portability (`fanotify` is widely available). | Requires `CONFIG_EROFS_FS` enabled (standard in Linux >= 5.4, AWS Bottlerocket, COS, Ubuntu, RedHat 9). |
| **Fallback Mechanism** | If `fanotify` fails, falls back to full download. | If EROFS mount or loop fails, falls back to the current stub-writing loop. |

---

## 6. Implementation Blueprint

To prototype and implement this change, we propose the following changes:

### Phase A: Controller EROFS Compilation
1. In the `agentfs-controller`, when a snapshot is uploaded:
   - Run `mkfs.erofs` inside the snapshot directory to produce an `index.img` (containing metadata-only).
   - Store the `index.img` file as a special system-defined blob associated with the snapshot ID.
2. Update the `GetLatestSnapshot` API to return a reference to the `index.img` blob.

### Phase B: Node Daemon Mount Integration
1. In `cmd/agentfs-node-daemon/main.go`, during `NodePublishVolume`:
   - Detect if EROFS is supported by the host kernel (e.g., checking `/proc/filesystems`).
   - If supported, download the `index.img` blob for the snapshot.
   - Set up a loop device and mount the image onto the `lower/` directory:
     ```go
     // Equivalent of mount -t erofs -o loop index.img target/lower
     err := unix.Mount(imagePath, lowerPath, "erofs", unix.MS_RDONLY, "")
     ```
   - If the mount succeeds, proceed to setup `fanotify` and mount OverlayFS.
   - If any step fails (e.g. host kernel has no EROFS), fall back to the existing `pullSnapshot` loop (backwards compatibility).

---

## 7. Conclusion & Recommendations

Using EROFS to build and mount the readonly base image is an exceptionally elegant way to solve the $O(N)$ scaling issue of metadata pre-population in AgentFS. It turns the entire node-side layout setup into a single `mount` system call, delivering:
1. **Near-zero volume startup latency** regardless of the number of files.
2. **Reduced disk write amplification and CPU usage** during mount phase.
3. **Seamless compatibility** with our existing, robust `fanotify` lazy-loading logic via OverlayFS.

We recommend proceeding with **Phase A and B prototyping** as the next logical milestone in the AgentFS roadmap.
