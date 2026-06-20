# Research: Exploring Storing and Managing Volume Snapshots as EROFS Layers

**Author:** codebot-robot / Gemini CLI  
**Date:** June 19, 2026  
**Status:** Proposed & Implemented  
**Issue Reference:** Issue #13 (Explore storing EROFS layers)

---

## 1. Executive Summary

Historically, AgentFS managed snapshots by tracking individual file metadata in `SnapshotMetadata` and transferring files as individual blobs. While this approach enables incremental updates, it requires the controller or daemon to expand and reconstruct the file hierarchy on-disk, leading to high metadata overhead and $O(N)$ system call complexity during snapshot restoration.

To address these inefficiencies, this research introduces an **EROFS Layers Mode** (`--enable-erofs-layers`). In this mode, snapshots are stored and transmitted entirely as **raw, block-aligned EROFS images representing immutable filesystem layers**. 

Instead of expanding layers, the node daemon mounts EROFS layer images directly using loop devices and stacks them with OverlayFS. When a volume is unpublished, the daemon uses the Go-native EROFS compiler (`pkg/erofs`) to compile the modified files in the `upper/` overlay directory into a new EROFS layer. Furthermore, to avoid deep layering overhead, the daemon can dynamically combine/flatten layers on the client-side when compiling the snapshot. On the server side, the controller verifies all uploaded EROFS layers using our native `pkg/erofs.Fsck` tool.

---

## 2. Architectural Design

The EROFS Layers protocol integrates the controller, node daemon, and `pkg/erofs` into an end-to-end layer storage architecture:

```
[AgentFS Node Daemon]                               [AgentFS Controller]
         |                                                   |
         | --- 1. GetLatestSnapshot(WantErofs: true) ------> |
         | <--- 2. SnapshotMetadata (ErofsLayers list) ----- |
         |                                                   |
         | --- 3. Download EROFS layer blobs --------------> |
         |                                                   |
         | === 4. Loop mount each layer image ===            |
         | === 5. Mount OverlayFS using multiple lowerdirs = |
         |                                                   |
         | (Container reads and writes to merged mount)      |
         |                                                   |
         | === 6. NodeUnpublishVolume (before unmount) ===   |
         |        - If changes exist:                        |
         |          - If layers < threshold:                 |
         |            Compile "upper/" to new EROFS layer    |
         |          - If layers >= threshold:                |
         |            Compile merged mount to flattened EROFS|
         |                                                   |
         | --- 7. Upload EROFS layer blob -----------------> |
         | --- 8. UploadSnapshot (Updated layers list) ----> |
         |                                                   | === 9. Run pkg/erofs.Fsck to verify image
         | <--- 10. Upload Success ------------------------- |
```

### A. Layer Mounting and OverlayFS Stacking
During `NodePublishVolume` (via `pullSnapshot`), if EROFS layers mode is enabled:
1. The daemon requests the latest snapshot from the controller.
2. If `SnapshotMetadata.ErofsLayers` is populated (or has `ErofsSha256` for backward compatibility), the daemon downloads each EROFS layer image blob.
3. Each EROFS image is loop-mounted to a separate directory under `volumeDir/layer-<i>` (where index `0` represents the oldest/bottom layer and index `N-1` represents the newest/top layer).
4. The daemon constructs the OverlayFS `lowerdir` mount parameter by stacking them in reverse order (newest layer first, oldest last):
   ```
   lowerdir=volumeDir/layer-<N-1>:volumeDir/layer-<N-2>:...:volumeDir/layer-0
   ```
5. If no layers exist yet, it falls back to mounting with an empty `volumeDir/lower` directory.

### B. Delta Layer Generation with pkg/erofs
When the volume is unpublished, any modifications (new files, overrides) reside in `volumeDir/upper`. If changes are detected:
1. The daemon uses `pkg/erofs.NewFileSystemNode` to traverse `volumeDir/upper`.
2. The daemon compiles the directory tree into a new EROFS image file directly using `pkg/erofs.WriteImage`.
3. The image is uploaded as a single blob to the controller and appended to `SnapshotMetadata.ErofsLayers`.

### C. Client-Side Layer Optimization (Flattening/Combining)
To prevent performance degradation from deeply nested OverlayFS mount stacks (which can degrade file lookup times), we implement an optimization threshold on the client. 
* If the number of existing layers is `>= 2` (meaning the new layer would make it `>= 3` layers), the daemon **flattens** the stack.
* To perform flattening, the daemon compiles the **entire active merged `targetPath`** (which represents the exact union of all lower layers and the upper layer) into a single, comprehensive EROFS image.
* The daemon uploads this new unified EROFS blob and updates the snapshot's `ErofsLayers` list to contain **only this single flattened layer**, resetting the stacking depth.

### D. Server-Side Fsck Verification
To prevent storage of corrupted or malicious images, the controller intercepts `UploadSnapshot` requests. If the snapshot contains `ErofsLayers`:
1. The controller iterates through each layer hash.
2. It opens the corresponding EROFS blob file from disk.
3. It executes the Go-native `erofs.Fsck` utility over the file. If validation fails (e.g., due to loop detection, directory truncation, or path traversal), the upload is rejected.

---

## 3. Implementation Details

Our working implementation introduces:
1. **Protobuf Extension:** Added `repeated string erofs_layers = 3;` in `SnapshotMetadata` to manage layer sequence order.
2. **Flag Support:** Added `--enable-erofs-layers` flag to the node daemon.
3. **Mounting Orchestration:** Implemented logic inside the daemon's `pullSnapshot` and `NodePublishVolume` to automatically download, loop mount, and stack multiple EROFS directories.
4. **Unmounting Cleanup:** Extended `NodeUnpublishVolume` to automatically unmount all loop devices matching `volumeDir/layer-*` prior to purging the local volume directory, preventing dangling mount leaks.
5. **Native Compiler & Fsck Integration:** Plugged in `pkg/erofs.NewFileSystemNode`, `pkg/erofs.WriteImage`, and `pkg/erofs.Fsck` to compile directories and verify blobs natively in Go.

---

## 4. Key Advantages

| Feature / Metric | Classic File-Based Snapshotting | EROFS Layers Mode |
| :--- | :--- | :--- |
| **Storage Format** | Unpacked file blobs on server | Packed, block-aligned EROFS layers |
| **Mount Syscalls** | $O(N)$ directory/file creation | Exactly $1 + L$ mounts (where $L$ is number of layers) |
| **Transfer Size** | Large volume of individual files | Single compressed/packed EROFS image blob |
| **Server-Side Overhead** | High (unpacking & reconstructing files) | Extremely low (flat blob storage + fast native `fsck` check) |
| **Container Startup Latency**| High metadata creation delay | Near-instant loop mount |
| **Whiteout Handling** | Custom whiteout parsing / tracking | Native OverlayFS/kernel whiteout character device support |

---

## 5. Conclusion

Storing and managing snapshots as EROFS layers represents a major milestone in AgentFS efficiency. It minimizes startup latency from $O(N)$ to $O(1)$ while preserving incremental snapshot capabilities via OverlayFS stacking. By integrating client-side layer combining and native server-side `fsck` validation, we ensure high filesystem performance and rigorous security.
