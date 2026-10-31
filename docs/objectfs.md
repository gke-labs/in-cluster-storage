# ObjectFS Architecture & Design

This document describes the design, architecture, metadata management, storage layout, caching mechanisms, and future roadmap for **ObjectFS** in the `in-cluster-storage` project.

---

## Overview

**ObjectFS** is a high-performance, POSIX-like distributed filesystem engineered to run natively on Kubernetes clusters. The ultimate source of truth and persistent backing store for ObjectFS is cloud object storage (such as Google Cloud Storage or Amazon S3).

ObjectFS is built around the fundamental architectural principle of **splitting metadata storage from object payload storage**:
1. **Metadata Layer:** Tracks the hierarchical directory tree, file inodes, attributes, permissions, and timestamps. Metadata updates are recorded in an append-only change-log using the **Streams** service ([`docs/streams.md`](streams.md)) for low-latency persistence, combined with periodic immutable **EROFS** snapshots to avoid long replay times during recovery.
2. **Object/Blob Layer:** Stores file contents as deduplicated, immutable content-addressable blobs identified by their SHA-256 digests. ObjectFS adopts a storage strategy inspired by **Git**: large files are written as standalone loose blobs, while smaller files are aggregated into indexed packfiles (`.pack`) to eliminate small-object metadata overhead and API throttling in cloud object storage.

Mounting on Kubernetes worker nodes is currently handled via a user-space **FUSE** driver (`go-fuse`) that communicates with a cluster controller service. ObjectFS provides near-instant volume readiness, intelligent local node caching, push-based invalidations, and high-throughput data access without requiring cloud storage credentials to be distributed to worker nodes.

For deployment and installation instructions with real object storage (GCS/S3) and persistent WAL/metadata storage, see the [Installation Guide](install.md).

---

## Architectural Design

```
+-----------------------------------------------------------------------------------------+
|                                    Kubernetes Node                                      |
|                                                                                         |
|  +--------------------+         POSIX Syscalls (read, write, lookup, mkdir, etc.)       |
|  | Application Pod    | -------------------------------------------------------------+  |
|  | (Standard Workload)|                                                              |  |
|  +--------------------+                                                              |  |
|                                                                                      |  |
|  +--------------------------------------------------------------------------------+  |  |
|  | Node CSI Driver & FUSE Client (objectfs-node-daemon)                           |  |  |
|  |                                                                                |  |  |
|  |  +-----------------------------+        +-----------------------------------+  |  |  |
|  |  | Inode / Path Mapper         |        | Local Node Cache (NodeCache)      |<-+  |  |
|  |  +-----------------------------+        | (In-Memory + Dirty Write Buffers) |     |  |
|  |                 |                       +-----------------------------------+     |  |
|  +-----------------|-----------------------------------------|-----------------------+  |
|                    | (gRPC: Lookup, GetAttr, Read, Write)     |                          |
|                    v                                         | (WatchVolume Stream)     |
|  +-----------------------------------------------------------v-----------------------+  |
|  | Enlightened App / CAS Socket (TODO): /.in-cluster-storage/api (SCM_RIGHTS FD)    |  |
+-----------------------------------------------------------------------------------------+
                                     |
                                     | Bidirectional gRPC
                                     v
+-----------------------------------------------------------------------------------------+
|                       Central Cluster Service (objectfs-controller)                     |
|                                                                                         |
|  +-----------------------------------------------------------------------------------+  |
|  | In-Memory Volume State & Metadata Engine                                          |  |
|  |  - Active Directory Tree & Inode Allocator                                        |  |
|  |  - EventBroadcaster (Push Invalidation to Node Watchers)                          |  |
|  +-----------------------------------------------------------------------------------+  |
|         |                                                       |                       |
|         | 1. Metadata Change-Log                                | 2. Blob Management    |
|         v                                                       v                       |
|  +------------------------------+             +--------------------------------------+  |
|  | Streams Layer (docs/streams) |             | Content-Addressable Blob Store       |  |
|  | (Append-Only WAL Engine)     |             | (Loose Blobs & Git-style Packfiles)  |  |
|  +------------------------------+             +--------------------------------------+  |
+-----------------|-----------------------------------------------|-----------------------+
                  |                                               |
                  | Flush / Commit                                | Put / Get / Range
                  v                                               v
+-----------------------------------------------------------------------------------------+
|                             Cloud Object Storage (GCS / S3)                             |
|                                                                                         |
|  Metadata (Snapshots & WAL):                                                            |
|    volumes/<volID>/meta/<stream_seq, 20 digits>.erofs (Immutable EROFS snapshot projection) |
|    streams/wal/segments/<first_pos>-<last_pos>.wal    (Streams WAL segment logs)             |
|                                                                                         |
|  Data Blobs:                                                                            |
|    blobs/<sha256>                                (Standalone loose blob for > 64MB)     |
|    blobs/<packID>-<count>.pack                   (Packed small/medium blobs + index)    |
+-----------------------------------------------------------------------------------------+
```

### Key Components

1. **Central Controller Service (`objectfs-controller`, `pkg/objectfs/controller`):**
   - Runs as a central service or `StatefulSet` in the Kubernetes cluster.
   - Maintains a metadata engine for sub-millisecond lookup and directory traversal (currently in-memory; TODO: index only "hot" entries in memory while keeping the full metadata structure on local disk replicated from S3/GCS snapshots).
   - Manages credentials to backend cloud object storage (GCS/S3), isolating sensitive storage secrets from worker nodes.
   - Manages periodic consolidation of active volume states into read-only EROFS snapshots and flushes dirty blobs to object storage.
   - Hosts an `EventBroadcaster` that transmits live filesystem events (`EVENT_CREATED`, `EVENT_MODIFIED`, `EVENT_DELETED`, `EVENT_RENAMED`) over `WatchVolume` gRPC streams to connected node daemons.

2. **Node CSI Driver & FUSE Client (`objectfs-node-daemon`, `pkg/objectfs/fuse`):**
   - Deployed as a `DaemonSet` on every Kubernetes node.
   - Implements the CSI node interface (`NodePublishVolume`, `NodeUnpublishVolume`) and mounts a POSIX filesystem at the pod volume path via `go-fuse`.
   - Embeds a fast `NodeCache` supporting LRU read caching, dirty write buffering, and write-through synchronization modes (TODO: cache blobs in allocated fixed-size local scratch storage on an LRU basis; similarly, the controller should cache blobs locally).
   - Subscribes to the controller's `WatchVolume` notification stream to proactively invalidate cached node attributes and file blocks upon remote concurrent writes.

3. **Metadata Change-Log & Streams Integration (`pkg/wal`, `docs/streams.md`):**
   - Serves as the write-ahead log for all filesystem mutations (file creations, writes, truncates, renames, unlinks).
   - Enables fast multi-tier acknowledgements (`Local`, `Witness`, `Permanent`) for write operations before large EROFS snapshot generation.

4. **Content-Addressable Blob Storage (`pkg/objectfs/blob`):**
   - Handles storage, compression, and retrieval of file contents addressed exclusively by their cryptographic hash (SHA-256).
   - Partitions blobs into standalone loose objects (for files > 64MB) and consolidated packfiles (for files <= 64MB).

5. **Cloud Object Storage Backend (`pkg/objectstore`):**
   - Pluggable storage abstraction supporting Google Cloud Storage (GCS), Amazon S3, local filesystem emulation, and in-memory test backends.

---

## Mount Modes: Standard FUSE vs. Exclusive-Local

ObjectFS supports two distinct mount modes, selected via the `mode` parameter in the volume context (StorageClass parameters or CSI volume attributes):

1. **Standard FUSE Mode (`mode: fuse`, default):**
   - Workloads interact with the volume through a user-space FUSE driver (`go-fuse`).
   - Supports multi-node concurrent access (`ReadWriteMany` / `ReadWriteOnce`).
   - Read and write operations issue per-operation RPCs to the controller or interact with the local in-memory/disk cache, with remote writes invalidated proactively via `WatchVolume` event streams.

2. **Exclusive-Local Mode (`mode: exclusive-local`):**
   - The node daemon eagerly materialises the volume from the controller into a local directory (`<cacheDir>/<volume>/lower`), mounts an **OverlayFS** filesystem into the pod with local `upper` and `work` directories, and serves I/O at local kernel filesystem speed with **no FUSE overhead and no per-operation RPCs**.
   - **Durability Contract:** Writes remain entirely local on the node until a **checkpoint**. Writes are durable **at unmount, at each checkpoint, and on request**:
     - **At unmount:** when the pod terminates, `NodeUnpublishVolume` unmounts the overlay, scans `upper/` for changes, and commits the upper layer back to the controller via ordinary RPCs (`Mkdir`, `CreateFile`, `WriteFile`, `TruncateFile`, `Unlink`, `Rmdir`, `Symlink`, `SetAttr`, and blocking `Fsync`).
     - **At each checkpoint (periodic interval):** configured via the `checkpointInterval` parameter on the `objectfs-exclusive-local` StorageClass or as a volume attribute (duration string, e.g. `checkpointInterval: "10s"`, default off). The daemon runs the upper-layer commit on a timer without unmounting; pieces written since the last checkpoint are committed, tracked via a local checkpoint manifest without mutating `lower/` or `upper/`, the overlay stays mounted, and the pod keeps running. Concurrency: the scan tolerates files changing underneath it (commits what it reads as a point-in-time prefix per file; the next checkpoint picks up the rest).
     - **On request (explicit checkpoint):** triggered via `objectfsctl checkpoint <volume>`. The controller records the request with a unique request ID, broadcasts `EVENT_CHECKPOINT_REQUEST` over the existing `WatchVolume` stream to the node daemon, the daemon commits the upper layer and acknowledges via `AckCheckpoint` with the stream position of the commit, unblocking the CLI.
     - Writes are lost only when a worker node dies before unmount can run, which is bounded by the periodic checkpoint interval; a force-deleted pod still has its volume unpublished and committed by kubelet.
     - **Explicit Hand-off via Controller Hold:** When a pod is force-deleted (`--force --grace-period=0`), kubelet on the old node continues tearing down the container and executing the final unmount checkpoint while the claim is freed. To prevent race conditions during hand-off, the controller coordinates volume possession through an exclusive-local hold with a bounded lease (default 30s):
       - **Acquire on publish:** `NodePublishVolume` calls `AcquireExclusive` before touching local state or materialising the volume. If another node holds a live lease, the controller returns `codes.Aborted`, which the node daemon surfaces as a retryable CSI error so kubelet retries publish with backoff while the pod waits in `ContainerCreating`.
       - **Renew while mounted:** The daemon periodically renews the hold well inside the lease period (every 10s) via `RenewExclusive`.
       - **Release after final checkpoint:** `NodeUnpublishVolume` calls `ReleaseExclusive` strictly after the final checkpoint commits to the controller, never before.
       - **Lease expiry on node death:** If a node crashes or the node daemon is terminated, its hold expires automatically upon lease TTL (30s), admitting the next node to materialise and mount the volume up to the last committed checkpoint.
       - **Operator visibility:** Active holds (holder node, mount ID, lease expiry) can be inspected via `objectfsctl volumes describe <volume>`.
       - **Controller Restart & Split Hand-off Window:** Holds are currently tracked in an in-memory lease table; if the controller restarts, holds are forgotten and re-established on the next periodic renew. Between the restart and the holder's next renew (up to 10s), another node's acquire could succeed if requested, causing the original holder's next renew to fail (logged at `Errorf` as a split hand-off) while it may still have uncommitted writes. Persisting holds in WAL/stream metadata is planned as the future fix.
       - **Degraded Volume Hold Safety:** If a volume enters a degraded state (#316, e.g. from a terminal WAL stream error), unmount replay fails repeatedly and the daemon continues renewing the hold indefinitely to prevent another node from mounting divergent state; the hold is not released until the underlying issue is resolved or the node is destroyed.
   - **Leftover Recovery & Checkpoint Failure Safety:** A failed checkpoint never discards or purges uncommitted local state. If scanning or replay fails during `NodeUnpublishVolume`, `upper/` is kept completely intact on disk, and an error is returned so kubelet retries unpublish until durability is achieved. Furthermore, if a new pod mounts the volume on the same node after an unmount failure, `publishExclusiveLocal` detects the non-empty `upper/`, replays and commits it to the controller first, and only then wipes the directory and materialises fresh. If recovery replay fails, publish fails with an error naming the leftover directory.
   - **Resumable Eager Materialisation & Kubelet Timeout:** Kubelet imposes a default 2-minute timeout on CSI `NodePublishVolume` calls. If a large volume exceeds 2 minutes to materialise, kubelet cancels and retries `NodePublishVolume`. Materialisation is designed to be resumable: files are written to temporary files and atomically renamed upon completion. Subsequent retries inspect existing files in `lower/` and skip downloading any file whose size and modification time already match the live volume. Active progress (files and bytes completed) is logged every 5 seconds.
   - **Access Mode & Exclusivity Requirement:** Because multiple concurrent writers to the same volume in exclusive-local mode would diverge without synchronization, exclusivity is enforced through CSI access modes. Drivers advertise `SINGLE_NODE_SINGLE_WRITER` (`NodeServiceCapability_RPC_SINGLE_NODE_MULTI_WRITER`). Volumes mounted in `exclusive-local` mode **must** be claimed with `ReadWriteOncePod` (RWOP). Mounting with any other access mode is rejected with `InvalidArgument`.
   - **Using Exclusive-Local via StorageClass:** The recommended and primary way to use exclusive-local mode in Kubernetes is via the `objectfs-exclusive-local` StorageClass (`volumeBindingMode: WaitForFirstConsumer`) paired with a `ReadWriteOncePod` PersistentVolumeClaim:
     ```yaml
     apiVersion: storage.k8s.io/v1
     kind: StorageClass
     metadata:
       name: objectfs-exclusive-local
     provisioner: objectfs.labs.gke.io
     volumeBindingMode: WaitForFirstConsumer
     reclaimPolicy: Delete
     parameters:
       mode: exclusive-local
       checkpointInterval: "10s" # optional: periodic checkpoint interval (e.g. "10s", "1m", default off)
     ---
     apiVersion: v1
     kind: PersistentVolumeClaim
     metadata:
       name: my-exclusive-pvc
     spec:
       accessModes:
         - ReadWriteOncePod
       storageClassName: objectfs-exclusive-local
       resources:
         requests:
           storage: 10Gi
     ```
     Static PersistentVolumes also remain supported by specifying `mode: exclusive-local` and optional `checkpointInterval: "10s"` in `spec.csi.volumeAttributes`.
   - **Local Disk Requirements:** The worker node must have sufficient local scratch disk capacity under the daemon's cache directory (`/var/cache/objectfs/<volume>`) to store the eagerly downloaded `lower/` layer, overlay `upper/`, and `work/` trees.
   - **Metadata Preservation Semantics:**
     - **Regular Files & Directories:** Permissions (mode) and modification times (mtime) are preserved.
     - **Ownership (UID / GID):** Preserved on materialisation and during checkpoint replay.
     - **Symlinks:** Preserved across mount and checkpoint.
     - **Hard Links:** Not preserved. Materialised as independent copies, and checkpoint replay commits them as separate files.
     - **Extended Attributes (xattr):** Preserved across checkpoints and materialisation (#378). All namespaces (`user.*`, `trusted.*`, `security.*`) are scanned and carried verbatim; only OverlayFS-internal names (`trusted.overlay.*` and `user.overlay.*`) are filtered. During materialisation, pairs are applied via `unix.Lsetxattr`; if the destination kernel or security policy rejects an attribute, materialise fails with an error naming the path and attribute.
       - *Caveat (reserved names):* OverlayFS reserves `*.overlay.*` attribute names, so user xattrs matching that pattern cannot be stored on an exclusive-local volume.
       - *Caveat (copy-up on setxattr):* The overlay is mounted without `metacopy`, so setting an xattr on a lower-only file copies the entire file into the upper layer and causes the next checkpoint to re-upload its data.
     - **Special Files (FIFOs, Sockets, Device Nodes):** Not supported; skipped with a warning during materialisation and checkpoint scan.
     - **Renames:** File renames performed in the upper layer appear natively to OverlayFS as an unlink of the old path plus a creation of the new path; these are replayed as `Unlink` followed by `CreateFile` during checkpoint commit.

---

## Metadata Architecture & Management

A core strength of ObjectFS is separating metadata operations from data payloads, eliminating the high latency and eventual-consistency hazards typical of naive object-storage-backed filesystems.

### 1. The Metadata Change-Log (Streams Layer)

Every filesystem operation that mutates state—such as `mkdir`, `create`, `write`, `truncate`, `rename`, or `unlink`—is modeled as a discrete record appended to the volume's metadata change-log.

```
+-----------------------------------------------------------------------------------+
|                            Metadata Mutation Stream                               |
|                                                                                   |
|  [Seq 101: MKDIR /models] ──> [Seq 102: CREATE /models/config.json (SHA: 0a1b...)]|
|                                                                                   |
|  ──> [Seq 103: WRITE /models/weights.bin (offset: 0, len: 4MB, SHA: 8f3e...)]     |
|                                                                                   |
|  ──> [Seq 104: RENAME /temp/out.tmp -> /models/out.final] ──> ...                 |
+-----------------------------------------------------------------------------------+
```

- **Low Latency & High Durability:** Mutations are immediately durable upon being written to the Streams buffer without waiting for full cloud object flush cycles.
- **Ordered Replay & Recovery:** If the controller crashes or restarts, the filesystem state is deterministically reconstructed by loading the last EROFS snapshot and replaying subsequent change-log records from the Streams layer.
- **Cross-Node Event Notification:** The change-log acts as the event source driving `WatchVolume` invalidation notifications across all cluster worker nodes.

> **Partial Writes on Large Files (TODO):** An important architectural trade-off is how to handle partial writes or in-place modifications to large files (e.g. updating a 4KB block in a 100MB file). Potential approaches include:
> - **Full Copy-on-Write Rewrite:** Simple and preserves single-blob integrity, but incurs write amplification on small edits to large files.
> - **Chunked Blobs / Extent Maps:** Splitting large files into fixed-size or Content-Defined Chunking (CDC) blocks, updating only dirty chunks and indexing chunk hashes in metadata.
> - **Append-Only Delta Log:** Recording write deltas in the Streams change-log and compacting them during periodic snapshot creation.

> **Note on Implementation Roadmap:** ObjectFS currently maintains volume state in memory and flushes snapshots periodically to object storage. Integrating the Streams layer ([`docs/streams.md`](streams.md)) as the live, authoritative change-log is the primary next step for ObjectFS metadata durability and multi-writer synchronization.

### 2. SQLite Metadata Storage, In-Memory Read Cache & Write-Behind Architecture

To provide bounded memory consumption, fast indexing, and ACID transaction safety when serving millions of files and directories, ObjectFS uses an embedded **SQLite metadata store** with an asynchronous write-behind applier and an in-memory LRU read cache:

1. **Relational Schema Architecture (Inodes, DirEntries & FileChunks):**
   - **`objectfs.v1alpha1.Inode`:** Stores inode metadata (`ino`, `mode`, `size`, `mtime`, `atime`, `ctime`, `uid`, `gid`, `is_dir`, `sha256`, `etag`, `content_sha256`, `chunk_size`, `manifest_sha256`).
   - **`objectfs.v1alpha1.DirEntry`:** Stores directory hierarchy (`parent_ino`, `name`, `ino`, `is_dir`, `mode`), enabling sub-millisecond single-entry lookups (`WHERE parent_ino = ? AND name = ?`) and sorted range scans for directory listings (`WHERE parent_ino = ?`).
   - **`objectfs.v1alpha1.FileChunk`:** Stores content block mappings (`ino`, `index`, `sha256`, `inline_data`) for chunked files and inline data.

2. **In-Memory Read Cache & Unapplied Overlay:**
   - **Read Cache:** Inodes and directory entries are cached in memory using a thread-safe LRU cache bounded by entry count and byte capacity (defaults to 64MB, configurable via `--metadata-cache-max-bytes`). Missing rows are loaded transparently from SQLite.
   - **Write-Behind Overlay & Async Applier:** Mutations append to the authoritative Structured Data Stream (SDS) / WAL log immediately and update an in-memory unapplied overlay map for zero-latency read-your-own-writes visibility. A background applier coalesces and batches committed transactions into SQLite (`WAL` journal mode with `NORMAL` synchronous settings) without blocking client operations. Backpressure is applied if the unapplied overlay exceeds configurable limits.

3. **Multi-Tier Volume Recovery:**
   When the controller starts or recovers, volume metadata is restored using a prioritized four-step recovery hierarchy:
   - **1. Local SQLite File (`metadata.sqlite`):** If a local SQLite database exists on disk, its position is loaded immediately for fast zero-download cold starts.
   - **2. Published SQLite Snapshot:** If local state is missing (e.g. after pod relocation), the latest published SQLite snapshot is downloaded from cloud storage and restored.
   - **3. EROFS Snapshot Import:** For legacy volumes that only have EROFS snapshots, the latest EROFS snapshot image is imported into SQLite once.
   - **4. Authoritative SDS Stream Replay:** Any stream changes logged beyond the snapshot position are replayed from the SDS stream, ensuring full point-in-time recovery of in-flight and post-snapshot mutations.

4. **Snapshot Publishing & Projections:**
   - **SQLite Snapshot Publishing:** Point-in-time SQLite database snapshots are uploaded to cloud object storage using SQLite backup APIs, and a `SnapshotPointer` with format `sqlite` is logged to the stream.
   - **EROFS Snapshot Projections:** ObjectFS compiles immutable EROFS filesystem snapshots serving as SDS projections for kernel-level image mounting and Composefs integration, accompanied by `SnapshotPointer` records.

### 3. Periodic Snapshots as Structured Data Stream Projections

ObjectFS writes filesystem metadata as a Structured Data Stream (SDS) using three registered relational tables: `Inode`, `DirEntry`, and `Content`. To prevent the change-log from growing indefinitely and to provide point-in-time recovery, ObjectFS periodically compiles the filesystem hierarchy into an immutable **EROFS (Enhanced Read-Only File System)** snapshot image serving as an SDS **projection**.

#### Why EROFS as a Projection?
- **Deterministic & Compact:** EROFS is a lightweight, read-only filesystem format supported in Linux kernels (>= 5.4). It organizes inodes, directory tables, and extended attributes with minimal space overhead.
- **Composefs Alignment:** ObjectFS follows the Composefs pattern: the EROFS snapshot contains directory hierarchies, file names, sizes, modes, and timestamps, but **contains no file data blocks**.
- **Digest Association via Extended Attributes:** Inode data associations are embedded directly in EROFS xattrs:
  - `user.digest`: The SHA-256 content hash of the blob.
  - `user.sha256`: Duplicate digest xattr for broad tool compatibility.
- **Embedded Schema Registry & Stream Position:** Snapshots embed SDS metadata in extended attributes on the root directory node:
  - `trusted.sds.position`: Stream sequence position of the snapshot.
  - `trusted.sds.registry`: Serialized `sds.v1.Registry` containing all table schemas and fingerprints.
  - `trusted.sds.snapshot_time`: RFC3339Nano creation timestamp.

#### Snapshot Storage Hierarchy & Discovery
Snapshots are written to cloud object storage under the volume metadata prefix:
- `volumes/<volumeID>/meta/<stream_seq, 20 digits>.erofs` (e.g. `volumes/vol-1/meta/00000000000000000042.erofs`)
- **No `meta/latest` pointer:** Snapshots are discovered by lexicographical listing where the highest stream sequence position wins, eliminating dual-object synchronization and skew.
- **Content Flushing:** Referenced `Content` rows are flushed to the blob store during snapshot compilation, while unreferenced rows are safely discarded. Following snapshot upload, a `SnapshotPointer` framework record is appended to the SDS log.
- **Crash Recovery:** Controller memory is reconstructed on startup by mounting the latest EROFS snapshot projection, extracting its position and registry from root xattrs, and replaying subsequent records from that position using `sds.StreamReader`.

---

## Content-Addressable Blob Storage & Packfiles

ObjectFS stores all file data as content-addressable blobs. Content addressing provides automatic cross-file and cross-volume **data deduplication**: identical files or identical chunks uploaded across multiple containers share the same underlying storage.

### Git-Inspired Storage Model: Loose Blobs vs. Packfiles

In cloud object storage, storing millions of individual small objects introduces severe performance degradation due to API call overhead, rate-limiting, and cost amplification. ObjectFS resolves this using a model inspired by Git:

```
                                  File Write Request
                                          │
                         Is Blob Size > 64 MB (Large Threshold)?
                                   /              \
                            YES   /                \   NO
                                 /                  \
                                v                    v
                    [ Standalone Loose Blob ]   [ Aggregate into Packfile ]
                    blobs/<sha256>              blobs/<packID>-<N>.pack
```

1. **Standalone Loose Blobs (Large Files > 64MB):**
   - Stored directly as individual objects keyed by `blobs/<sha256>`.
   - Allows direct streaming, HTTP range queries, and signed cloud redirect URLs for high-throughput parallel reads.
2. **Packfiles (Small & Medium Files <= 64MB):**
   - Batched into a consolidated `.pack` file containing multiple blobs.
   - Named `blobs/<packID>-<count>.pack` (where `count` indicates the number of blobs).
   - Contains a sorted index table enabling $O(\log N)$ binary search lookup without reading data payloads.

### Binary Wire & File Formats (`OBJB`)

All loose blob files and packfiles share the `OBJB` binary format (big-endian integer fields).

#### 1. File Base Header (8 Bytes)
```
+--------------------+-------------------+--------------------+--------------------+
|    Magic (4B)      |   Version (1B)    |   FileType (1B)    |   Reserved (2B)    |
|   0x4F424A42       |       0x01        |  1 (Blob) / 2 (Pack|       0x0000       |
+--------------------+-------------------+--------------------+--------------------+
```

#### 2. Standalone Loose Blob Format (64-Byte Header + Payload)
```
+-----------------------------------------------------------------------------------+
| Base Header (8B) | Encoding (4B) | FinalLength (8B) | SHA256 (32B) | Reserved (12B)|
+-----------------------------------------------------------------------------------+
|                                 Data Payload                                      |
+-----------------------------------------------------------------------------------+
```
- **Encoding:** `0 = Raw Uncompressed`, `1 = zlib Compressed`, `2 = Chunked`.

#### 3. Packfile Format (`.pack`)
```
+-----------------------------------------------------------------------------------+
| Base Header (8B) | Slots S (4B) | Used N (4B) | Flags (4B) | Reserved (12B)       |
+-----------------------------------------------------------------------------------+
| Table: S Fixed-Size Slots (S * 48 Bytes)                                          |
| First N sorted by SHA256, remaining S - N zeroed:                                 |
| [ SHA256 (32B) | Start (4B) | StoredLength (4B) | FinalLength (4B) | Encoding (4B)|
| ...                                                                               |
+-----------------------------------------------------------------------------------+
| Data Payload Section (Blob Items in arrival order)                                |
|   +-----------------------------------------------------------------------------+ |
|   | Item 0: Encoding (4B) | FinalLen (4B) | Reserved (8B) | Payload Data ...    | |
|   +-----------------------------------------------------------------------------+ |
|   | Item 1: Encoding (4B) | FinalLen (4B) | Reserved (8B) | Payload Data ...    | |
|   +-----------------------------------------------------------------------------+ |
+-----------------------------------------------------------------------------------+
```

- **Explicit Lengths & Reserved Slots (v2):** Packfile format version 2 decouples data payload order from table order. The pack reserves $S$ fixed-size slots up front (default $S = 1024$), with the first $N$ slots sorted by SHA-256 for $O(\log N)$ binary search and remaining slots zeroed. Each slot explicitly records `start`, `stored_length`, `final_length`, and `encoding`. Readers binary-search the first $N$ entries and range-read `[start, start+stored_length)` to retrieve the payload directly, sizing decode buffers and selecting the decoder without touching the item header. The 16-byte item header precedes each payload on disk so items remain self-describing for disaster recovery. Data items are written in arrival order rather than SHA-256 order, enabling packs to be built incrementally. Bit 0 of `Flags` marks a pack as sealed (`0x00000001`). Per-item compression is adaptive: payloads are stored compressed only when compression saves at least 10% in size; incompressible or already-compressed media (audio, video, images, archives) is stored as raw binary to eliminate decompression CPU.

Packfiles written in format version 1 (`OBJB` version byte `0x01`, with $N$ sorted hashes and $(N+1)$ offset deltas) remain supported and readable forever without requiring rewrites or migrations of existing packs; the version byte in the 8-byte base header selects the appropriate parser.

---

## Mounting, Client Architecture & Caching

### 1. FUSE Filesystem Driver (`pkg/objectfs/fuse`)
The node daemon mounts ObjectFS volumes into container pod paths using `go-fuse`. The driver maps POSIX operations to gRPC calls:
- `Lookup` / `GetAttr` -> Retrieves inode attributes from the controller or local cache.
- `ReadDir` / `ReadDirPlus` -> Enumerates directory contents with populated inode attributes.
- `Read` -> Fetches byte ranges from the local cache or requests content streams from the controller.
- `Write` / `Truncate` -> Buffers changes locally and transmits modifications to the controller.
- `Fsync` / `Flush` -> Forces synchronization of dirty buffers to the cluster service.

### 2. Local Caching & Scratch Storage (`NodeCache` & `DiskBlobCache`)
Each node daemon and controller maintains a local cache backed by memory and dedicated scratch disk storage:
- **Persistent On-Disk Blob Cache (`DiskBlobCache`):** Verified content-addressed blobs are stored in a node-local scratch disk directory (`--cache-dir`, default `/var/cache/objectfs`, sized via `--disk-cache-size-mb`, default 10 GiB) backed by a local SQLite index (`index.db`) in WAL mode. In Kubernetes deployments, `--cache-dir` must be mounted from a node-local `hostPath` volume so cached blobs persist across daemon restarts and pod reschedules. Eviction runs on an LRU basis to enforce the disk budget. On file `Open`, chunks are prefetched in parallel in offset order ahead of reads.
- **Read Caching:** Clean chunks are served directly from the persistent on-disk blob cache and the kernel page cache without duplicating memory.
- **Dirty Write Buffering (`NodeCache`):** The in-memory cache holds only uncommitted dirty write buffers (`--cache-size-mb`, default 128 MB) until acknowledged by an `Fsync` RPC (#222/#245 generation protocol).
- **Cache Invalidation & Kernel Notifications:** Subscribes to the controller's `WatchVolume` gRPC push notification stream. When another node modifies, renames, or deletes a file, the node daemon invalidates its local cache entries, file manifests, and pushes kernel invalidations via go-fuse (`InodeNotify`, `EntryNotify`, `DeleteNotify`). After a remote write, a process on another node that already has the file open sees the new size on its next `read(2)`, `stat`, or `open`; readers that use `sendfile`/`splice` on an existing descriptor keep the old EOF until one of those happens, because the kernel's splice path does not revalidate FUSE attributes (#274/#278). Reconnects invalidate all clean state, reset manifests, and sweep both `EntryNotify(parent, name)` for all tracked directory entries and `InodeNotify(ino, -1, 0)` over all kernel-held inodes. During a controller or watch-stream outage a node continues to serve cached data and names; it revalidates all of them (attributes, data, directory entries) as soon as the stream is re-established, so staleness is bounded by the outage length; a node resumes serving within a few seconds of the controller returning. Entry and attribute cache timeouts while healthy can be tuned via `--entry-timeout` and `--attr-timeout` (default 60s, e.g. in `objectfsctl mount`).

### 3. Security, TLS & Authentication

ObjectFS enforces network isolation, transport encryption, and role-based mutual TLS (mTLS) authentication:

1. **Network Isolation:**
   - Ingress to the controller's gRPC port (`50051`) is restricted by a Kubernetes `NetworkPolicy` strictly to pods labeled `app: objectfs-node-daemon` and `app: objectfs-admin`. All other cluster traffic to the controller is dropped at the network layer.

2. **Per-Directory Certificates & Transport Layer Security (mTLS):**
   - The controller serves gRPC over mutual TLS (`tls.RequireAndVerifyClientCert`), requiring and validating client certificates against its mounted trust bundle.
   - **Per-Directory Certificate Layout:** Every pod receives one directory per certificate it holds:
     ```
     <dir>/tls.key        private key (PEM)
     <dir>/tls.crt        certificate for that key (PEM, leaf first)
     <dir>/trust/*.crt    trust bundle: any number of PEM files; readers load all and dedupe
     ```
     - Server directory mounted at `/etc/objectfs/certs/server` (`--server-cert-dir`).
     - Client directory mounted at `/etc/objectfs/certs/client` (`--client-cert-dir`).
   - **Fail-Closed Security:** Plaintext gRPC is strictly disabled on both the controller and clients (`objectfs-node-daemon`, `objectfsctl`) unless explicitly opted into via `--insecure` (for local development). Missing or invalid certificate directories result in immediate client startup errors rather than falling back to plaintext.

3. **Client Certificate Identity & Role-Based Authorization:**
   - Client identity is derived directly from the client certificate's URI Subject Alternative Name (SAN), formatted as `objectfs://<role>/<namespace>/<serviceaccount>`.
   - The controller maps the role from the URI SAN and enforces namespace scoping:
     - **`node-daemon` Role (`objectfs://node-daemon/<namespace>/objectfs-node-daemon`):** Allowed to execute filesystem, volume, blob, and CSI operations (`GetAttr`, `ReadFile`, `WriteFile`, `Fsync`, `WatchVolume`, `AckCheckpoint`, `AcquireExclusive`, `GetBlob`, `ListBlobs`, etc.). Calls to administrative methods are rejected with `PermissionDenied`.
     - **`admin` Role (`objectfs://admin/<namespace>/objectfs-admin`):** Allowed full access to all RPCs, including `CreateSnapshot`, `ListSnapshots`, `ListVolumes`, and `Checkpoint`.
     - **Cross-tenant / Unprivileged Certificates:** Any certificate from another namespace or without a valid `objectfs://` URI SAN receives `RoleNone` and is rejected with `PermissionDenied` or `Unauthenticated`.

4. **`certsigner` Module & Secret Issuance:**
   - By default, `k8s/objectfs.yaml` runs a one-shot `objectfs-certsigner` Job under a dedicated `objectfs-certsigner` ServiceAccount (whose Role grants `create`/`get` strictly scoped to the three TLS Secrets).
   - `certsigner` generates an in-memory self-signed CA (10-year validity, private key never stored on disk or in Secrets) and 1-year leaf certificates.
   - It writes one Secret per directory (create-only, never updates existing Secrets):
     - `objectfs-controller-server` (mounted at `/etc/objectfs/certs/server` on controller)
     - `objectfs-node-daemon-client` (mounted at `/etc/objectfs/certs/client` on node daemon; never mounts server key)
     - `objectfs-admin-client` (mounted at `/etc/objectfs/certs/client` on admin pods; never mounted on the controller)
   - On initial deployment, node daemon pods may briefly log `secret "objectfs-node-daemon-client" not found` while the init Job completes; this is transient and resolves automatically once the Job creates the Secrets.
   - The controller logs a warning at startup and daily when within 30 days of leaf expiration.
   - **Rotation Procedure:** Re-running the `certsigner` Job updates the Secrets in place with new leaf certificates and a new CA, while carrying forward the previous CA certificate into the single `trust/ca.crt` trust bundle (carrying current and previous CA). This CA trust overlap allows controller and node daemons to verify both old and new certificates during the kubelet volume sync window without interruption or restart (no restart needed). Preserving this trust overlap is required for restart-free rotation:
     1. Delete the completed Job: `kubectl delete job objectfs-certsigner -n kube-objectfs-system`
     2. Re-apply the manifest (`kubectl apply -f k8s/objectfs.yaml`) to trigger a fresh Job run. Do not delete the Secrets, so `certsigner` can carry the previous CA forward into the trust bundle.
     Controller and node daemons dynamically reload renewed certificates and the overlapping CA trust pool without restart (no restart needed).
     3. Fallback restart path (if manual restart is desired or if Secrets were deleted, breaking CA overlap): `kubectl rollout restart statefulset objectfs-controller daemonset objectfs-node-daemon -n kube-objectfs-system`

5. **Administrative Path for `objectfsctl`:**
   - `objectfsctl` provides `--client-cert-dir` (also configurable via `OBJECTFS_CLIENT_CERT_DIR` environment variable, defaulting to `/etc/objectfs/certs/client`).
   - For administrative operations from an admin pod mounting `objectfs-admin-client` at `/etc/objectfs/certs/client`:
     ```bash
     objectfsctl --server objectfs-controller:50051 volumes list
     objectfsctl --server objectfs-controller:50051 checkpoint <volume_id>
     ```

### 4. Trying It Locally

`dev/tasks/fuse-start` runs the whole stack on one machine with no cloud credentials: it starts `objectfs-controller` against a `file://` object store in a temp directory and mounts a volume there with `objectfsctl mount`. On Linux it runs natively; on macOS it cross-compiles Linux binaries and runs them in a Docker container with `/dev/fuse`, opening a shell inside the container where the mount is visible. See the header of the script for the environment variables it accepts (volume ID, write mode, data directory, extra `objectfsctl mount` flags such as `--debug`).

### 4. Write Staging, Pack Consolidation & Durability Contract

To achieve both high write throughput and optimal packing in cloud object storage, ObjectFS employs a **stage-and-sync** architecture:

1. **Client as Write Buffer, Controller as Packer:**
   - The node daemon buffers writes in its local `NodeCache` and pushes runs of contiguous dirty chunks to the controller via `WriteFile`.
   - `WriteFile` stages chunk bytes in controller memory and acknowledges immediately without writing to cloud object storage or committing metadata.
   - Chunks remain **dirty** in the node daemon's cache until an `Fsync` RPC acknowledges durability.

2. **Sync and Pack Consolidation:**
   - A sync (`Fsync` RPC) takes **every staged chunk across all files on the volume** and uploads them as consolidated packfiles using `PutBlobs` (capped at 64 MiB per pack).
   - Only after `PutBlobs` returns successfully does the controller commit all `FileChunk` records and updated inode sizes/mtimes in a single atomic transaction and wait for WAL durability.
   - This ensures **no logged row ever references a blob that is not durable** in cloud storage, preventing dangling references and read I/O errors after crashes.

3. **Consistency Contract & Modes:**

   ObjectFS provides two consistency modes configured per StorageClass via `parameters: consistency`:

   - **Standard Mode (default, `consistency: standard`):**
     - **On the writer's own node:** Visibility is immediate through its local cache (`NodeCache`).
     - **Across remote nodes (what is served during concurrent writes):** Remote nodes do **not** serve a snapshot frozen at `open(2)` time. Instead, reads serve whatever state has already been synced and committed to the controller (its metadata index and object storage) as of the moment the read or cache fetch occurs. While a writer on Node 1 is actively writing, un-synced data remains buffered in Node 1's local cache or staged in controller memory; a reader on Node 2 reading the file observes the previously committed/synced version of those blocks (or pre-existing/zero data) until a sync occurs. If a sync completes on Node 1 while Node 2 has the file open, the controller commits the new chunks and broadcasts an invalidation event via `WatchVolume`, allowing subsequent reads on Node 2 to fetch the newly committed data.
     - **Close behavior across nodes:** When an application calls `close(2)`, the kernel first delivers FUSE `Flush`. The daemon pushes dirty chunks of that file from `NodeCache` to controller staging memory (`pushDirtyChunksToStaging`, one or more `WriteFile` RPCs) and waits for the controller to acknowledge them. Close blocks for a controller round trip proportional to the file's dirty bytes that periodic sync or dirty-budget overflow has not already moved; if that push fails, `close(2)` returns an error. Then `Release` records the inode as closed and pokes the daemon's background sync goroutine, which later sends a non-blocking `Fsync` with the CLOSE trigger. `close(2)` does not wait for packfile upload or metadata commit. On remote nodes, `open(2)` does not wait. If Node 2 opens the file before the background close sync finishes uploading packs and committing metadata, it may observe the pre-close state (and may continue serving cached pages from its kernel cache).

   - **Close-to-Open Mode (`consistency: close-to-open`):**
     - **Guaranteed visibility ordering:** If process $P_1$ on Node 1 writes to file $F$ and successfully completes `close(2)`, and process $P_2$ on Node 2 subsequently executes `open(2)` on $F$, $P_2$ is guaranteed to observe all modifications made by $P_1$ up to its close.
     - **How it works mechanically:**
       - **No pre-write registration or locks:** Nodes do not cooperatively record file locks, leases, or write intents with the controller before writing. Writes remain in the local daemon's `NodeCache` until `Flush` on close, the periodic sync (`--background-sync-interval`, default 30 s), or dirty-budget overflow (`WouldDirtyOverflow`, running a blocking volume sync including upload).
       - **Synchronous chunk push on `close(2)`:** When an application calls `close(2)`, the kernel first delivers FUSE `Flush`. The daemon pushes every dirty chunk of that file from `NodeCache` to controller staging memory (`pushDirtyChunksToStaging`, one or more `WriteFile` RPCs) and waits for the controller to acknowledge them. Close blocks for the controller round trip proportional to the file's dirty bytes that periodic sync or dirty-budget overflow has not already moved; if that push fails, `close(2)` returns an error. Then `Release` records the inode as closed and pokes the daemon's background sync goroutine, which later sends a non-blocking `Fsync` with the CLOSE trigger. `close(2)` does not wait for packfile upload or metadata commit.
       - **The close invariant:** Because a successful `close(2)` requires `Flush` to succeed, **by the time `close(2)` returns to the calling application, the controller holds all of the file's data not already committed by an earlier sync in its in-memory staging table** (`v.stagedChunks[inodeID]`).
       - **Controller Open-Side Barrier:** The open barrier checks `stagedChunks[ino]` and whether a sync is in flight. It can only work because a successful close implies the file's chunks are already in controller memory:
         - When process $P_2$ on Node 2 calls `open(2)` on file $F$, the FUSE driver issues an `Open` RPC to the controller.
         - If file $F$ is clean (no staged chunks in controller memory and no sync currently packaging $F$), `Open` completes without barrier wait.
         - If file $F$ has staged chunks in controller memory or an in-flight background sync is currently packaging $F$, the controller intercepts `Open`. The controller itself initiates an immediate per-inode sync (`syncInodesLocked`) for that inode, uploads the packfile to object storage, commits the metadata transaction, and waits for completion before replying to `Open`.
         - This wait is bounded by `--open-barrier-timeout` (default 5 s). If the timeout expires or the volume's WAL stream is degraded, `Open` returns `EIO`.
       - **Reader Cache Invalidation:** The `OpenResponse` RPC returns fresh `EntryAttr` and the committed `stream_position`. When the stream position moved, Node 2's daemon purges its local page cache (`InodeNotify(ino, 0, -1)`) and attribute cache (`InodeNotify(ino, -1, 0)`), invalidates clean cached chunks, and replies to FUSE without `FOPEN_KEEP_CACHE`, ensuring subsequent reads fetch the newly durable data.
     - **Trade-offs Relative to Standard Mode:**
       - The write and close paths are identical in both modes: writes buffer in `NodeCache`, and `close(2)` blocks on FUSE `Flush` pushing remaining dirty chunks to controller staging memory without waiting for cloud storage pack upload.
       - Close-to-open adds a bounded wait on `open(2)` for files with staged or in-flight data, can fail those opens with `EIO` after `--open-barrier-timeout` or when the volume is degraded, and requires keeping the per-open controller RPC.
       - Clean opens pay the controller `Open` RPC in both modes.
       - When the barrier waits on `open(2)`, the latency depends on pack upload and WAL commit. Measured barrier latency (Linux amd64, Go 1.27.3, in-memory backend over bufconn):
         - 4 KiB inline file: ~443 µs (inlined into transaction, 0 pack blobs)
         - 16 MiB multi-chunk file: ~106.6 ms
         Against a real cloud object store, the barrier wait additionally includes the packfile `PutBlobs` upload latency, scaling with the size of dirty data closed since the last sync.
     - **Visibility, NOT Crash Durability:** Close-to-open orders **visibility across nodes, NOT crash durability**. Until the sync that close triggers (or a reader's barrier forces) has uploaded packfiles and committed metadata, data exists only in controller staging memory and the writer daemon's cache. If the controller restarts during that window, staged data is dropped; if the writer daemon is also lost, the data is lost even though `close(2)` returned success. Durability still arrives only with a committed sync, and `fsync(2)` is the only way for applications to wait for it.
     - **Interaction with Exclusive-Local Mode:** Exclusive-local volumes (`mode: exclusive-local`) satisfy close-to-open by construction via unmount checkpoints. The `consistency` parameter is ignored on exclusive-local volumes.
     - **Interaction with #174:** Issue #174 explores avoiding the per-FUSE-open `Open` RPC. Close-to-open consistency strictly requires the `Open` RPC so the controller can execute the open barrier and the node daemon can invalidate stale reader caches. Implementations must retain the `Open` RPC for close-to-open volumes, and any changes under #174 must preserve this path.

4. **Sync Triggers:**

| Trigger | Who | Blocks Caller? | Default |
| :--- | :--- | :--- | :--- |
| `fsync` / `fdatasync` | Application via `Fsync` RPC (`blocking=true`) | Yes, until pack uploaded + rows logged at WAL level | Always |
| Close of written file (`Release`) | Node daemon triggers per-inode sync (`blocking=false, trigger=CLOSE`) | No (Flush stages chunks in controller RAM; close does not wait for cloud PUT) | On (`--sync-on-close=true`) |
| Background timer | Node daemon, per volume (`blocking=false, trigger=PERIODIC`) | No | On (`--background-sync-interval=30s`) |
| Client cache budget | Node daemon, when dirty bytes exceed cache | The write that overflows waits | Always (`--cache-size-mb=128`) |

5. **Controller Coalescing Mechanism:**
   - Non-blocking syncs are coalesced within a configurable window (`--sync-coalesce-window=1s`, default 1s). The controller gathers staged writes and non-blocking sync requests across the volume, then commits them in a single pack and transaction.
   - When triggered by close (`trigger=CLOSE`), the controller coalesces close triggers for a short fixed floor window (75ms) so bursts of closes share a pack, and commits only the requested closed file's staged chunks; other staged data on the volume follows on the coalesce window. Waiters registered for other inodes are not completed by a close sync.
   - A blocking `fsync` arriving during an active coalesce window preempts the window and flushes immediately, including all writes in the window.
   - Non-blocking sync failures are retained by the controller and surfaced on the next blocking `fsync` for any affected inode.
   - Controller staging memory is bounded via `--max-staging-bytes=64MB` per volume; writes that exceed this limit are rejected until an `Fsync` commits staged data.

6. **Crash Recovery & Client Re-Push:**
   - If the controller restarts before an `Fsync` completes, un-synced staged writes are dropped cleanly.
   - When the client next contacts the restarted controller for `Fsync`, it detects a staging epoch change (`ESTALE`) and automatically re-pushes its still-dirty chunks, ensuring no data loss.

7. **Durability Matrix (controller WAL permanent / witness / local):**

The durability of an `fsync` operation depends on the configured controller WAL stream replication level (both `--wal-dir` and `--wal-target` are required):

| Durability Mode | Configuration | Fsync Ack Guarantee | Failure Mode & Recovery Guarantee | Recommended Use Case |
| :--- | :--- | :--- | :--- | :--- |
| **Controller WAL `permanent`** | `--wal-durability=permanent` (requires `--wal-dir` and `--wal-target`) | Blocks until chunk packfiles are uploaded to object store AND controller WAL log records are sealed and committed to permanent cloud object storage (GCS/S3). | **Disaster Safe:** Survives total cluster loss; all metadata changes are durably persisted in cloud storage alongside chunk packs. | Archival, compliance, high-value data. |
| **Controller WAL `witness`** *(default in `k8s/objectfs.yaml`)* | `--wal-durability=witness` (requires `--wal-dir` and `--wal-target`) | Blocks until chunk packfiles are uploaded to object store AND metadata transaction is replicated and fsynced to the central `wal-buffer` witness disk. | **Crash Safe:** Survives controller pod restarts or node failovers; restarted controllers replay authoritative stream tail from `wal-buffer`. | Standard production workloads requiring low commit latency (~2–10 ms). |
| **Controller WAL `local`** | `--wal-durability=local` (requires `--wal-dir` and `--wal-target`) | Blocks until chunk packfiles are uploaded to object store AND metadata transaction is appended and fsynced to node-local WAL segment files on host disk. | **Node-bound:** Survives controller restarts on the same node with local WAL directory intact. If the node is lost, metadata since the last snapshot is lost. | Single-node testing and scratch workloads. |

**WAL Buffer Deployment Requirement for Witness Durability:**
When `--wal-durability=witness` is used, metadata transactions are acknowledged as soon as they are committed and fsynced to `wal-buffer` local disk. For this guarantee to survive `wal-buffer` pod restarts or rescheduling across nodes:
- The default deployment manifest (`k8s/wal.yaml`) configures a `PersistentVolumeClaim` via `volumeClaimTemplates` for `data-dir` (using the cluster's default `StorageClass`), ensuring the volume re-attaches across pod restarts and rescheduling.
- If using `hostPath` storage for local development instead of PVCs, `wal-buffer` **must be node-pinned** (via `nodeSelector: kubernetes.io/hostname: <node>`). Otherwise, rescheduling the pod to a different node mounts an empty directory, regressing the local witness watermark to the last object-store flush and violating witness durability.

### 5. Configurable Write-Through Modes
ObjectFS supports three distinct write synchronization modes:
1. `WRITE_THROUGH_FSYNC`: Default mode. Writes are staged on the controller and committed to durable pack storage upon `fsync(2)` or background coalesce window sync.
2. `LAZY_WRITE`: Writes are buffered asynchronously and flushed in background batches, maximizing write throughput for scratch and temporary workloads.
3. `EAGER_REPLICATION`: Writes are streamed immediately to the central controller memory and change-log, providing cross-node visibility and crash resilience without waiting for object store commits.

---

## Protocol Specification

The gRPC contract between `objectfs-node-daemon` and `objectfs-controller` is defined in [`proto/objectfs.proto`](../proto/objectfs.proto):

| Category | RPC Method | Description |
| :--- | :--- | :--- |
| **Metadata Operations** | `GetAttr` | Fetches inode attributes for a path. |
| | `Lookup` | Finds a named child entry inside a parent directory. |
| | `ReadDir` | Lists entries within a directory. |
| | `Mkdir` | Creates a new directory. |
| | `Unlink` | Removes a file entry. |
| | `Rmdir` | Removes an empty directory. |
| | `Rename` | Atomically renames or moves a file or directory. |
| | `WatchVolume` | Server-streaming RPC pushing real-time filesystem change events to nodes. |
| **Data Operations** | `CreateFile` | Creates a new regular file with optional initial content. |
| | `ReadFile` | Reads byte slices or obtains direct download redirect URLs. |
| | `WriteFile` | Writes byte slices at specified offsets with configurable write modes. |
| | `TruncateFile` | Resizes a file to a specified length. |
| | `Fsync` | Flushes uncommitted file writes to durable storage. |
| **Blob & Snapshot APIs**| `ListBlobs` | Enumerates available content-addressable blob SHAs. |
| | `GetBlob` | Server-streaming download of raw or packed blob payloads. |
| | `ListVolumes` | Discovers available volumes across the cluster. |
| | `ListSnapshots` | Lists point-in-time EROFS snapshots for a volume. |
| | `CreateSnapshot` | Manually triggers an immediate EROFS snapshot compilation. |
| | `Checkpoint` | Triggers an explicit checkpoint commit on an exclusive-local volume and waits for acknowledgement. |
| | `AckCheckpoint` | Acknowledges completion of an explicit checkpoint commit by a node daemon. |
| | `RepairVolume` | Moves volume's degraded segment files aside and reopens stream from witness watermark. |

---

## Degraded Volumes & Recovery Runbook

When a volume's underlying write-ahead log (WAL) stream encounters an unrecoverable local storage error (such as a read-only filesystem, out-of-space condition, or permission denial during stream divergence reconciliation), the stream marks itself as **degraded** (`DegradedError`) and stops replication.

### Operational Characteristics

1. **Read Availability:** The controller keeps the degraded volume fully readable. Workloads can continue reading existing cached and persistent metadata and object data.
2. **Write Safety:** To prevent uncommitted mutations from accumulating on a divergent local filesystem, all stream-mutating operations fail fast with `EIO` upon `fsync`.
3. **Data Durability Guarantee:** Records above the WAL buffer's witness watermark represent local unacknowledged records that have diverged from authoritative history; records at or below the witness watermark are already durable in the WAL buffer. Consequently, the local segment store holds no records that cannot be recovered from the buffer.

### Identifying Degraded Volumes

Operators can check volume health using `objectfsctl`:

```bash
# List volumes and inspect status (degraded volumes display the reason and since timestamp)
objectfsctl --server <controller-addr> volumes list

# Detailed volume stats and degraded information
objectfsctl --server <controller-addr> volumes describe <volume-id>
```

When degraded, `volumes list` outputs:
```
my-volume	(degraded: failed to truncate local segment store to 42: read-only file system, since: 2026-10-21T18:00:00Z)
```

### Automatic Recovery

The WAL client background loop automatically retries the failed truncate operation with exponential backoff (starting at 1 second, capped at 1 minute, continuing indefinitely). If the underlying cause is transient (for example, the filesystem is remounted read-write or disk space is freed), the truncate succeeds automatically.

Upon successful truncate:
- The stream clears its degraded state.
- A recovery log is emitted at info level noting the stream ID and duration of degradation.
- The stream re-enters the replication loop and re-resolves divergence during the buffer handshake.
- Blocked sync waiters are unblocked, and new mutating operations proceed normally.

### Controller Restart Behaviour

If the controller is restarted:
- **With the underlying fault cleared:** The controller detects stream divergence during volume open, successfully truncates obsolete local segments to the buffer's witness watermark, and resumes healthy operation automatically.
- **With the fault still present:** The controller starts up cleanly without crashlooping. The volume initializes in degraded state, remaining readable while failing fsyncs with `EIO` until repaired or until automatic retry succeeds.

### Manual Operator Repair (`objectfsctl volumes repair`)

If automatic retry cannot resolve the issue (e.g. persistent local disk corruption), an operator can repair the volume using `objectfsctl`:

```bash
objectfsctl --server <controller-addr> volumes repair <volume-id>
```

This is backed by the `RepairVolume` RPC:
1. Closes the volume's active WAL stream and segment store.
2. Moves the volume's local segment files aside into `<dir>/stream-<id>.<unix-ts>.degraded/` (never deletes files).
3. Reopens the stream, resuming sequence numbering from the buffer's authoritative witness watermark.
4. Clears the degraded state and restores full read-write availability.
5. Leaves all other volumes on the controller untouched.

If the volume is healthy, `repair` is refused unless `--force` is specified:
```bash
objectfsctl --server <controller-addr> volumes repair <volume-id> --force
```

---

## Compatibility

### Format Versioning Policy
Every durable artefact written to cloud object storage or local disk contains an explicit magic identifier and format version header:
- **Readers accept current and previous versions:** Readers for all durable artefacts accept both the current format version ($V_N$) and the previous format version ($V_{N-1}$). Readers reject any artefact with a format version newer than the current supported version with an informative error. Note that change-stream readers validate the stream header when reading from the beginning of a stream (`seq == 1`); replay operations resuming from a post-snapshot position (`seq > 1`) rely on the snapshot metadata for version validation and do not expect a stream header at the resume offset.
- **Writers write current version:** Writers always write the current format version ($V_N$). When resuming an existing stream after a controller restart, writers do not re-emit duplicate stream headers mid-stream.
- **Upgrade flag (`--accept-unversioned-segments`):** Pre-versioning legacy artefacts (files created prior to the introduction of magic and format version headers) are rejected by default. They are accepted only when the `--accept-unversioned-segments` flag is explicitly passed to controllers, buffer daemons, or CLI tools. When an unversioned artefact is encountered with this flag enabled, a warning is logged and the data is read. For existing unversioned change-log streams created prior to this format version, record 1 remains an unversioned record. Therefore, the `--accept-unversioned-segments` flag is required when replaying legacy streams from sequence 1 until a snapshot advances the base position past sequence 1.

### API Compatibility
The gRPC APIs (`objectfs.v1alpha1`, `wal.v1alpha1`, and `sds.v1`) are in **Alpha**. During the alpha phase, APIs are strictly additive: new fields, RPC methods, and enum values may be added, but existing field numbers, message semantics, and wire formats remain backward-compatible without breaking existing serialized payloads. The API surface will be stabilized and pinned upon reaching `v1`.

### Artefact Classification: Derived vs. Only Copy

Artefacts in ObjectFS are categorized as either **Derived** or **Only Copy**:

| Artefact | Category | Storage Location | Recovery / Resilience Guarantee |
|---|---|---|---|
| **Data Blobs** (Loose & Packfiles) | **Only Copy** | Cloud Object Storage (`blobs/`) | Authoritative content-addressable file payload data. Cannot be re-derived if deleted. Versioned with `OBJB` magic and format versions `1` and `2`. |
| **Streams** (Log segments & sealed files) | **Only Copy** | Cloud Object Storage & Local WAL (`streams/`) | Authoritative change-log of metadata mutations and transaction events. Cannot be re-derived if deleted. Versioned with `OFWL` magic, format version `1`, and `StreamHeader` record. |
| **Snapshots past trim point** | **Only Copy** | Cloud Object Storage (`snapshots/`) | When change-log stream history has been trimmed or garbage-collected past the snapshot position, the snapshot is the sole authoritative copy of the base filesystem state. Format-versioned (`_meta` table in SQLite, `\x00format_version` in table snapshots). |
| **Snapshots with stream retained** | **Derived** | Cloud Object Storage (`snapshots/`) | Point-in-time index images that can be regenerated from scratch by replaying the change-log stream from sequence 1 (or a prior retained snapshot). |
| **Local Indexes** | **Derived** | Local Node & Controller Disk (`metadata-dir`) | SQLite databases and Pebble table indexes maintained on fast local storage for low-latency queries; rebuilt on restart or corruption from cloud snapshots and WAL replay. |
| **Local Caches** | **Derived** | Local Node Disk (`cache-dir`) & Memory | Temporary read caches (`NodeCache`) and dirty write staging buffers. Can be discarded or repopulated from authoritative storage on demand. |

---

## Roadmap & Next Steps (TODOs)

The following items represent the planned roadmap and architectural evolution for ObjectFS:

### 1. Integration with the Streams Layer (Metadata Change-Log)
- [x] **Wire Streams as Authoritative Change-Log:** Replace the current in-memory mutation log with the Streams service ([`docs/streams.md`](streams.md)). Every `mkdir`, `create`, `write`, `rename`, and `unlink` will be logged as an append-only Streams record with configurable durability (`Local`, `Witness`, `Permanent`).
- [x] **Deterministic Crash Recovery:** Rebuild controller memory on startup by mounting the latest EROFS snapshot and replaying outstanding change-log records from the Streams log.
- [ ] **Multi-Writer Ordering:** Use the central Streams buffer sequence numbers to establish linearizable ordering across multiple concurrent node writers.
- [x] **Eliminate "latest" Snapshot Pointer Object:** Remove the `volumes/<volID>/meta/latest` pointer file; discover the most recent valid snapshot via lexicographical listing or timestamp markers to avoid single-object update contention.
- [x] **Tiered Metadata Caching (Hot in Memory, Cold on Disk):** Refactor the controller metadata engine so only "hot" active directories and inodes are kept in RAM, while the full metadata state resides on fast local disk (replicated from cloud EROFS snapshots) to scale to millions of files without unbounded memory usage.

### 2. Exclusive-Local Mount Mode (Kernel-Native OverlayFS)
- [x] **Bypass User-Space FUSE Overhead:** Instead of routing all node I/O through a user-space FUSE daemon, `mode: exclusive-local` materialises the volume into a local directory and mounts it into the pod with OverlayFS, running at local-disk speed with no FUSE overhead and no per-operation RPCs (#287). AgentFS is retired (#289).
- [ ] **Kernel-Level Lazy-Loading & EROFS Materialisation:** For very large volumes where eager download is measured to be too slow, evaluate on-demand lazy materialisation with an EROFS lower layer and chunk fetches (deferred until measured necessary).

### 3. Enlightened Applications & CAS Unix Domain Socket (UDS) API
- [ ] **In-Filesystem Control Socket:** Expose a well-known Unix Domain Socket path inside the mounted volume (e.g. `/.in-cluster-storage/api` or `/.objectfs/api`).
- [ ] **Zero-Copy `SCM_RIGHTS` Blob Retrieval:** Allow enlightened applications (such as AI model runtimes, build systems, or data processors) to query cached blobs directly by SHA-256 over the socket. The node daemon transfers an open file descriptor (`SCM_RIGHTS`) of the locally cached blob directly to the application, enabling zero-copy `mmap`/`read` access without data copying through FUSE or user-space memory buffers.

### 4. Blob Storage, Caching & Write Handling
- [ ] **Local Scratch Storage & LRU Blob Caching:** Allocate a fixed-size local scratch disk volume on worker nodes and controller pods to cache downloaded loose blobs and packfile slices on an LRU basis, avoiding repeated network transfers from cloud storage.
- [ ] **Partial Write Strategies for Large Files:** Evaluate and design trade-offs for handling partial writes to large files: evaluate whether to perform full copy-on-write rewrites, introduce chunked blob index indirection (fixed-size chunks or Content-Defined Chunking / Rabin fingerprints), or record write deltas in the change-log.
- [ ] **Multipart Blob Uploading:** Implement parallel multipart uploads to S3 and GCS for large files (> 64MB) to maximize network bandwidth.
- [ ] **Signed Direct-Download Redirects:** Enhance `ReadFile` and `GetBlob` with presigned URLs for client-side direct parallel streaming from cloud object storage, bypassing controller network bottlenecks.
- [ ] **Packfile Compaction & Garbage Collection:** Periodically scan object storage packfiles to identify and purge unreferenced blobs from deleted files or pruned snapshots, rewriting active blobs into consolidated new packfiles.

### 5. Distributed Leasing & File Locking
- [ ] **File Locking Support (Advisory vs Mandatory):** Evaluate whether ObjectFS should support POSIX file locking (`fcntl(F_SETLK)` / `flock`). Explore trade-offs of centralized lock management in the controller (with heartbeat leases and failover cleanup) vs. client-side or distributed locking mechanisms.
- [ ] **Distributed Read/Write Leases:** Introduce delegation leases allowing node daemons to cache files locally for writes and reads with optimistic concurrency until invalidated by conflicting leases.

### 6. Extended Attributes (xattr) & POSIX ACLs
- [x] **Extended Attributes (xattr):** Support full `getxattr`, `setxattr`, `listxattr`, and `removexattr` operations in the FUSE driver and controller, preserving application metadata across SDS snapshots and controller restarts (#129).
  - **Supported Namespaces & Limits:** Custom and standard namespaces (e.g. `user.*`, `trusted.*`, `security.*`) are supported. Attributes in `trusted.*` and `security.*` are stored and returned as given (the kernel enforces caller capabilities such as `CAP_SYS_ADMIN`, and the controller does not filter them). Attribute names are limited to 255 bytes and values to 64 KiB (`XATTR_SIZE_MAX`). Setting or removing an extended attribute updates the target inode's `ctime` and tracks `xattr_count` on the inode.
  - **POSIX ACL Exclusion:** POSIX ACLs (`system.posix_acl_access` and `system.posix_acl_default`), as well as all other `system.*` attributes, are unsupported and return `ENOTSUP`.
  - **RPC Overhead & Cross-Node Visibility:** Extended attribute queries and modifications are not cached on node daemons except for zero-count shortcuts (`xattr_count == 0` on cached inodes avoids remote RPCs). When extended attributes exist, `getxattr` and `listxattr` issue RPCs to the controller. Cross-node visibility follows standard metadata durability: updates committed by a writer (via fsync, close, or periodic sync) are immediately readable by other nodes.
  - **Exclusive-Local Support:** On `mode: exclusive-local` volumes, extended attributes across all namespaces are served locally by OverlayFS while mounted and round-trip across checkpoints and volume materialisation (#378). OverlayFS-internal bookkeeping names (`trusted.overlay.*` and `user.overlay.*`) are excluded from checkpoint replay. Materialisation fails loudly if a destination node cannot apply an attribute (e.g. unsupported namespace or SELinux policy rejection).
