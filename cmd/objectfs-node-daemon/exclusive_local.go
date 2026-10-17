/*
Copyright 2026 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/errutil"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
)

func isExclusiveLocalDir(volumeDir string) bool {
	if fi, err := os.Stat(filepath.Join(volumeDir, "upper")); err == nil && fi.IsDir() {
		return true
	}
	if b, err := os.ReadFile(filepath.Join(volumeDir, "mode")); err == nil && strings.TrimSpace(string(b)) == "exclusive-local" {
		return true
	}
	return false
}

func isUpperNonEmpty(upperPath string) (bool, error) {
	f, err := os.Open(upperPath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	defer errutil.CloseAndLog(f, "upper directory")

	names, err := f.Readdirnames(1)
	if err != nil && err.Error() != "EOF" {
		return false, err
	}
	return len(names) > 0, nil
}

func isNotMountPoint(path string) (bool, error) {
	stat, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return true, nil
		}
		return false, err
	}
	parentPath := filepath.Dir(path)
	parentStat, err := os.Stat(parentPath)
	if err != nil {
		return false, err
	}
	return stat.Sys().(*syscall.Stat_t).Dev == parentStat.Sys().(*syscall.Stat_t).Dev, nil
}

func unmountTarget(targetPath string) error {
	if err := syscall.Unmount(targetPath, 0); err != nil {
		if err == syscall.EBUSY {
			klog.Warningf("Target path %s busy, attempting lazy unmount (MNT_DETACH)", targetPath)
			if detachErr := syscall.Unmount(targetPath, syscall.MNT_DETACH); detachErr != nil && detachErr != syscall.EINVAL && detachErr != syscall.ENOENT {
				return fmt.Errorf("failed to lazy unmount %s: %w", targetPath, detachErr)
			}
		} else if err != syscall.EINVAL && err != syscall.ENOENT {
			return fmt.Errorf("failed to unmount %s: %w", targetPath, err)
		}
	}
	return nil
}

func (d *objectFSDriver) publishExclusiveLocal(ctx context.Context, req *csi.NodePublishVolumeRequest, volumeID string) (*csi.NodePublishVolumeResponse, error) {
	targetPath := req.GetTargetPath()
	mountID := targetPath
	klog.Infof("Publishing ObjectFS volume %s in exclusive-local mode to %s", volumeID, targetPath)

	volumeDir := filepath.Join(d.cacheDir, volumeID)
	lowerPath := filepath.Join(volumeDir, "lower")
	upperPath := filepath.Join(volumeDir, "upper")
	workPath := filepath.Join(volumeDir, "work")

	client, err := d.getClient()
	if err != nil {
		return nil, fmt.Errorf("failed to get controller client: %w", err)
	}

	// 1. Acquire exclusive hold from controller before touching local state or materialising.
	// If another node holds the lease, controller returns codes.Aborted, which we surface as
	// a retryable CSI error so kubelet retries publish with backoff.
	acqResp, err := client.AcquireExclusive(ctx, &pb.AcquireExclusiveRequest{
		VolumeId: volumeID,
		NodeId:   d.nodeID,
		MountId:  mountID,
	})
	if err != nil {
		st, ok := status.FromError(err)
		if ok && st.Code() == codes.Aborted {
			klog.Warningf("exclusive-local [%s]: volume is held by another node: %v; retrying publish", volumeID, st.Message())
			return nil, err
		}
		return nil, fmt.Errorf("failed to acquire exclusive hold for volume %s: %w", volumeID, err)
	}
	klog.Infof("exclusive-local [%s]: acquired exclusive hold on node %s (mount %s) lease expires %s",
		volumeID, d.nodeID, mountID, acqResp.GetHold().GetLeaseExpiry().AsTime().Format(time.RFC3339))

	var publishSuccess bool
	defer func() {
		if !publishSuccess {
			relCtx, relCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer relCancel()
			_, _ = client.ReleaseExclusive(relCtx, &pb.ReleaseExclusiveRequest{
				VolumeId: volumeID,
				NodeId:   d.nodeID,
				MountId:  mountID,
			})
		}
	}()

	// Check if a previous session left an uncommitted upper layer (e.g. from a failed checkpoint)
	if hasLeftover, err := isUpperNonEmpty(upperPath); err != nil {
		return nil, fmt.Errorf("failed to inspect upper directory at %s: %w", upperPath, err)
	} else if hasLeftover {
		klog.Warningf("Found non-empty upper directory at %s from previous uncommitted session; replaying checkpoint first", upperPath)
		opList, scanErr := ScanUpperLayer(lowerPath, upperPath)
		if scanErr != nil {
			return nil, fmt.Errorf("failed to scan leftover upper layer at %s: %w", upperPath, scanErr)
		}
		if _, replayErr := ReplayOperations(ctx, client, volumeID, opList); replayErr != nil {
			// On failure, leave upper intact and fail publish naming upperPath
			return nil, fmt.Errorf("failed to recover and commit leftover upper layer from previous session at %s: %w", upperPath, replayErr)
		}
		klog.Infof("Successfully recovered and committed leftover upper layer from previous session for volume %s", volumeID)
		// After successful recovery, wipe volumeDir clean to materialise fresh
		if err := os.RemoveAll(volumeDir); err != nil {
			return nil, fmt.Errorf("failed to cleanup volume directory %s after recovery: %w", volumeDir, err)
		}
	}

	if err := os.MkdirAll(lowerPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create lower path %s: %w", lowerPath, err)
	}
	if err := os.MkdirAll(upperPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create upper path %s: %w", upperPath, err)
	}
	if err := os.MkdirAll(workPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create work path %s: %w", workPath, err)
	}

	if err := os.WriteFile(filepath.Join(volumeDir, "mode"), []byte("exclusive-local"), 0644); err != nil {
		return nil, fmt.Errorf("failed to write mode file: %w", err)
	}
	if err := os.WriteFile(filepath.Join(volumeDir, "volume_id"), []byte(volumeID), 0644); err != nil {
		return nil, fmt.Errorf("failed to write volume_id file: %w", err)
	}

	if err := d.materialiseVolume(ctx, client, volumeID, lowerPath); err != nil {
		return nil, fmt.Errorf("failed to materialise volume %s: %w", volumeID, err)
	}

	if err := os.MkdirAll(targetPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create target path %s: %w", targetPath, err)
	}

	notMnt, err := isNotMountPoint(targetPath)
	if err != nil {
		return nil, fmt.Errorf("failed to check if %s is a mount point: %w", targetPath, err)
	}
	if !notMnt {
		klog.Infof("Volume %s already mounted at %s", volumeID, targetPath)
		publishSuccess = true
		return &csi.NodePublishVolumeResponse{}, nil
	}

	opts := fmt.Sprintf("lowerdir=%s,upperdir=%s,workdir=%s", lowerPath, upperPath, workPath)
	if err := syscall.Mount("overlay", targetPath, "overlay", 0, opts); err != nil {
		return nil, fmt.Errorf("failed to mount overlayfs at %s: %w", targetPath, err)
	}

	// Parse checkpointInterval from volume context (e.g. "10s", "1m"). Default off (0).
	var checkpointInterval time.Duration
	if intervalStr, ok := req.GetVolumeContext()["checkpointInterval"]; ok && intervalStr != "" {
		if dur, pErr := time.ParseDuration(intervalStr); pErr == nil && dur > 0 {
			checkpointInterval = dur
		} else if pErr != nil {
			klog.Warningf("exclusive-local: invalid checkpointInterval %q for volume %s: %v", intervalStr, volumeID, pErr)
		}
	}

	var syncWg sync.WaitGroup
	watchCtx, cancel := context.WithCancel(context.Background())
	d.startExclusiveLocalSync(watchCtx, &syncWg, client, volumeID, volumeDir, lowerPath, upperPath, mountID, checkpointInterval)

	d.mu.Lock()
	if old, ok := d.mounts[targetPath]; ok {
		if old.cancelFunc != nil {
			old.cancelFunc()
		}
		if old.syncWg != nil {
			old.syncWg.Wait()
		}
		if old.server != nil {
			if unmountErr := old.server.Unmount(); unmountErr != nil {
				klog.Warningf("failed to unmount previous mount at %s: %v", targetPath, unmountErr)
			}
		}
	}
	d.mounts[targetPath] = &activeMount{
		volumeID:   volumeID,
		mode:       "exclusive-local",
		volumeDir:  volumeDir,
		mountID:    mountID,
		cancelFunc: cancel,
		syncWg:     &syncWg,
	}
	publishSuccess = true
	d.mu.Unlock()

	klog.Infof("Successfully mounted ObjectFS exclusive-local volume %s at %s", volumeID, targetPath)
	return &csi.NodePublishVolumeResponse{}, nil
}

func (d *objectFSDriver) startExclusiveLocalSync(ctx context.Context, wg *sync.WaitGroup, client pb.ObjectFSControllerClient, volumeID, volumeDir, lowerPath, upperPath, mountID string, interval time.Duration) {
	if mountID == "" {
		mountID = filepath.Join(volumeDir, "mount")
	}
	var cpMu sync.Mutex
	manifestPath := filepath.Join(volumeDir, "checkpoint.json")

	doCheckpoint := func() (uint64, error) {
		cpMu.Lock()
		defer cpMu.Unlock()

		if ctx.Err() != nil {
			return 0, ctx.Err()
		}

		manifest, err := LoadCheckpointManifest(manifestPath)
		if err != nil {
			return 0, fmt.Errorf("load checkpoint manifest: %w", err)
		}

		opList, err := ScanUpperLayer(lowerPath, upperPath, manifest)
		if err != nil {
			return 0, fmt.Errorf("scan upper layer: %w", err)
		}

		if ctx.Err() != nil {
			return 0, ctx.Err()
		}

		commitSeq, err := ReplayOperations(ctx, client, volumeID, opList)
		if err != nil {
			return 0, fmt.Errorf("replay operations: %w", err)
		}
		UpdateCheckpointManifest(manifest, upperPath, opList)
		if err := SaveCheckpointManifest(manifestPath, manifest); err != nil {
			klog.Warningf("exclusive-local [%s]: failed to save checkpoint manifest: %v", volumeID, err)
		}
		return commitSeq, nil
	}

	// 1. Periodic interval checkpoint
	if interval > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			klog.Infof("exclusive-local [%s]: starting periodic checkpoint loop with interval %v", volumeID, interval)
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if ctx.Err() != nil {
						return
					}
					if _, err := doCheckpoint(); err != nil {
						if ctx.Err() != nil {
							return
						}
						klog.Warningf("exclusive-local [%s]: periodic checkpoint failed: %v", volumeID, err)
					} else {
						klog.V(4).Infof("exclusive-local [%s]: periodic checkpoint committed successfully", volumeID)
					}
				}
			}
		}()
	}

	// 2. Explicit checkpoint requests over WatchVolume stream
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			stream, err := client.WatchVolume(ctx, &pb.WatchVolumeRequest{
				VolumeId: volumeID,
				NodeId:   d.nodeID,
			})
			if err != nil {
				select {
				case <-ctx.Done():
					return
				case <-time.After(1 * time.Second):
					continue
				}
			}

			for {
				ev, err := stream.Recv()
				if err != nil {
					break // reconnect loop or stream canceled
				}
				if ev.GetEventType() == pb.WatchEventType_EVENT_CHECKPOINT_REQUEST {
					requestID := ev.GetCheckpointRequestId()
					klog.Infof("exclusive-local [%s]: received EVENT_CHECKPOINT_REQUEST (reqID=%s), executing checkpoint", volumeID, requestID)
					commitSeq, cpErr := doCheckpoint()
					if cpErr != nil {
						if ctx.Err() != nil {
							return
						}
						klog.Errorf("exclusive-local [%s]: checkpoint execution failed: %v", volumeID, cpErr)
						continue
					}
					// Acknowledge checkpoint commit with stream_position
					_, ackErr := client.AckCheckpoint(ctx, &pb.AckCheckpointRequest{
						VolumeId:       volumeID,
						RequestId:      requestID,
						StreamPosition: commitSeq,
					})
					if ackErr != nil {
						klog.Errorf("exclusive-local [%s]: failed to acknowledge checkpoint: %v", volumeID, ackErr)
					} else {
						klog.Infof("exclusive-local [%s]: checkpoint acknowledged at position %d", volumeID, commitSeq)
					}
				}
			}
		}
	}()

	// 3. Periodic lease renewal for exclusive-local hold (renew well inside 30s lease)
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				renewCtx, renewCancel := context.WithTimeout(context.Background(), 5*time.Second)
				resp, err := client.RenewExclusive(renewCtx, &pb.RenewExclusiveRequest{
					VolumeId: volumeID,
					NodeId:   d.nodeID,
					MountId:  mountID,
				})
				renewCancel()
				if err != nil {
					klog.Warningf("exclusive-local [%s]: failed to renew exclusive hold: %v", volumeID, err)
				} else if resp.GetHold() != nil {
					klog.V(4).Infof("exclusive-local [%s]: renewed exclusive hold until %s", volumeID, resp.GetHold().GetLeaseExpiry().AsTime().Format(time.RFC3339))
				}
			}
		}
	}()
}

func (d *objectFSDriver) unpublishExclusiveLocal(ctx context.Context, req *csi.NodeUnpublishVolumeRequest, volumeID, volumeDir string, mountInfo *activeMount) (*csi.NodeUnpublishVolumeResponse, error) {
	targetPath := req.GetTargetPath()
	klog.Infof("Unpublishing ObjectFS exclusive-local volume %s from %s", volumeID, targetPath)

	lowerPath := filepath.Join(volumeDir, "lower")
	upperPath := filepath.Join(volumeDir, "upper")
	manifestPath := filepath.Join(volumeDir, "checkpoint.json")

	// Unpublish ordering: cancel, wait, unmount, scan, replay, remove, release hold
	// 1. Cancel background watcher, ticker, and renew loops
	if mountInfo != nil && mountInfo.cancelFunc != nil {
		mountInfo.cancelFunc()
	}

	// 2. Wait for background goroutines to finish cleanly
	if mountInfo != nil && mountInfo.syncWg != nil {
		mountInfo.syncWg.Wait()
	}

	// 3. Unmount overlay from targetPath
	if err := unmountTarget(targetPath); err != nil {
		return nil, fmt.Errorf("failed to unmount overlay at %s: %w", targetPath, err)
	}

	// 4. Scan upper layer against checkpoint manifest
	manifest, err := LoadCheckpointManifest(manifestPath)
	if err != nil {
		klog.Warningf("Failed to load checkpoint manifest for volume %s: %v", volumeID, err)
		manifest = nil
	}

	opList, err := ScanUpperLayer(lowerPath, upperPath, manifest)
	if err != nil {
		klog.Errorf("Failed to scan upper layer for volume %s: %v. Leaving upper layer intact.", volumeID, err)
		return nil, fmt.Errorf("checkpoint scan failed: %w", err)
	}

	// 5. Connect to controller and replay operations (commits final unmount checkpoint)
	client, err := d.getClient()
	if err != nil {
		klog.Errorf("Failed to connect to controller for checkpoint on volume %s: %v. Leaving upper layer intact.", volumeID, err)
		return nil, fmt.Errorf("failed to get controller client: %w", err)
	}

	if _, err := ReplayOperations(ctx, client, volumeID, opList); err != nil {
		klog.Errorf("Failed to replay checkpoint operations for volume %s: %v. Leaving upper layer intact.", volumeID, err)
		return nil, fmt.Errorf("checkpoint replay failed: %w", err)
	}

	// 6. On SUCCESS only, clean up volumeDir
	if err := os.RemoveAll(volumeDir); err != nil {
		klog.Warningf("Failed to cleanup volume directory %s after checkpoint: %v", volumeDir, err)
	}

	// 7. Release exclusive hold AFTER the final checkpoint has committed, never before.
	mountID := targetPath
	if mountInfo != nil && mountInfo.mountID != "" {
		mountID = mountInfo.mountID
	}
	relCtx, relCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer relCancel()
	if _, relErr := client.ReleaseExclusive(relCtx, &pb.ReleaseExclusiveRequest{
		VolumeId: volumeID,
		NodeId:   d.nodeID,
		MountId:  mountID,
	}); relErr != nil {
		klog.Warningf("exclusive-local [%s]: failed to release exclusive hold after checkpoint: %v", volumeID, relErr)
	} else {
		klog.Infof("exclusive-local [%s]: released exclusive hold on node %s (mount %s)", volumeID, d.nodeID, mountID)
	}

	klog.Infof("Successfully unpublished and checkpointed ObjectFS volume %s", volumeID)
	return &csi.NodeUnpublishVolumeResponse{}, nil
}
