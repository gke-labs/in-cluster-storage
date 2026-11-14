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
	"github.com/gke-labs/in-cluster-storage/pkg/scoped"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

func unmountTarget(ctx context.Context, targetPath string) error {
	if err := syscall.Unmount(targetPath, 0); err != nil {
		if err == syscall.EBUSY {
			scoped.Log(ctx).Info("target path busy, attempting lazy unmount (MNT_DETACH)", "target", targetPath, "err", err)
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
	log := scoped.Log(ctx).WithValues("volume", volumeID)
	ctx = scoped.WithLog(ctx, log)
	targetPath := req.GetTargetPath()
	mountID := targetPath
	log.Info("publishing ObjectFS volume in exclusive-local mode", "target", targetPath)

	volumeDir := filepath.Join(d.cacheDir, volumeID)
	lowerPath := filepath.Join(volumeDir, "lower")
	upperPath := filepath.Join(volumeDir, "upper")
	workPath := filepath.Join(volumeDir, "work")

	client, err := d.getClient(ctx)
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
			log.Info("volume is held by another node, retrying publish", "err", err)
			return nil, err
		}
		return nil, fmt.Errorf("failed to acquire exclusive hold for volume %s: %w", volumeID, err)
	}
	log.Info("acquired exclusive hold",
		"node", d.nodeID, "mount", mountID, "leaseExpiry", acqResp.GetHold().GetLeaseExpiry().AsTime().Format(time.RFC3339))

	var publishSuccess bool
	defer func() {
		if !publishSuccess {
			relCtx, relCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer relCancel()
			if _, relErr := client.ReleaseExclusive(relCtx, &pb.ReleaseExclusiveRequest{
				VolumeId: volumeID,
				NodeId:   d.nodeID,
				MountId:  mountID,
			}); relErr != nil {
				log.Info("failed to release hold on publish failure", "mount", mountID, "err", relErr)
			}
		}
	}()

	// Check if a previous session left an uncommitted upper layer (e.g. from a failed checkpoint)
	if hasLeftover, err := isUpperNonEmpty(upperPath); err != nil {
		return nil, fmt.Errorf("failed to inspect upper directory at %s: %w", upperPath, err)
	} else if hasLeftover {
		log.Info("found non-empty upper directory from previous uncommitted session, replaying checkpoint first", "path", upperPath)
		opList, scanErr := ScanUpperLayer(ctx, lowerPath, upperPath)
		if scanErr != nil {
			return nil, fmt.Errorf("failed to scan leftover upper layer at %s: %w", upperPath, scanErr)
		}
		if _, replayErr := ReplayOperations(ctx, client, volumeID, opList); replayErr != nil {
			// On failure, leave upper intact and fail publish naming upperPath
			return nil, fmt.Errorf("failed to recover and commit leftover upper layer from previous session at %s: %w", upperPath, replayErr)
		}
		log.Info("successfully recovered and committed leftover upper layer from previous session")
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
		log.Info("volume already mounted", "target", targetPath)
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
			log.Info("invalid checkpointInterval", "checkpointInterval", intervalStr, "err", pErr)
		}
	}

	var syncWg sync.WaitGroup
	mountLogger := scoped.Log(context.Background()).WithValues("volume", volumeID, "target", targetPath)
	watchCtx, cancel := context.WithCancel(scoped.WithLog(context.Background(), mountLogger))
	var renewWg sync.WaitGroup
	renewCtx, renewCancel := context.WithCancel(scoped.WithLog(context.Background(), mountLogger))
	d.startExclusiveLocalSync(watchCtx, &syncWg, renewCtx, &renewWg, client, volumeID, volumeDir, lowerPath, upperPath, mountID, checkpointInterval)

	d.mu.Lock()
	if old, ok := d.mounts[targetPath]; ok {
		if old.cancelFunc != nil {
			old.cancelFunc()
		}
		if old.syncWg != nil {
			old.syncWg.Wait()
		}
		if old.renewCancel != nil {
			old.renewCancel()
		}
		if old.renewWg != nil {
			old.renewWg.Wait()
		}
		if old.server != nil {
			if unmountErr := old.server.Unmount(); unmountErr != nil {
				mountLogger.Info("failed to unmount previous mount", "err", unmountErr)
			}
		}
	}
	d.mounts[targetPath] = &activeMount{
		volumeID:    volumeID,
		mode:        "exclusive-local",
		volumeDir:   volumeDir,
		mountID:     mountID,
		cancelFunc:  cancel,
		syncWg:      &syncWg,
		renewCancel: renewCancel,
		renewWg:     &renewWg,
	}
	publishSuccess = true
	d.mu.Unlock()

	log.Info("successfully mounted ObjectFS exclusive-local volume", "target", targetPath)
	return &csi.NodePublishVolumeResponse{}, nil
}

func (d *objectFSDriver) startExclusiveLocalSync(
	ctx context.Context, wg *sync.WaitGroup,
	renewCtx context.Context, renewWg *sync.WaitGroup,
	client pb.ObjectFSControllerClient,
	volumeID, volumeDir, lowerPath, upperPath, mountID string,
	interval time.Duration,
) {
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

		opList, err := ScanUpperLayer(ctx, lowerPath, upperPath, manifest)
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
		if err := UpdateCheckpointManifest(manifest, upperPath, opList); err != nil {
			return 0, fmt.Errorf("update checkpoint manifest: %w", err)
		}
		if err := SaveCheckpointManifest(manifestPath, manifest); err != nil {
			scoped.Log(ctx).Info("failed to save checkpoint manifest", "err", err)
		}
		return commitSeq, nil
	}

	// 1. Periodic interval checkpoint
	if interval > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			scoped.Log(ctx).Info("starting periodic checkpoint loop", "interval", interval)
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
						scoped.Log(ctx).Info("periodic checkpoint failed", "err", err)
					} else {
						scoped.Log(ctx).V(4).Info("periodic checkpoint committed successfully")
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
					scoped.Log(ctx).Info("received EVENT_CHECKPOINT_REQUEST, executing checkpoint", "requestID", requestID)
					commitSeq, cpErr := doCheckpoint()
					if cpErr != nil {
						if ctx.Err() != nil {
							return
						}
						scoped.Log(ctx).Error(cpErr, "checkpoint execution failed")
						continue
					}
					// Acknowledge checkpoint commit with stream_position
					_, ackErr := client.AckCheckpoint(ctx, &pb.AckCheckpointRequest{
						VolumeId:       volumeID,
						RequestId:      requestID,
						StreamPosition: commitSeq,
					})
					if ackErr != nil {
						scoped.Log(ctx).Error(ackErr, "failed to acknowledge checkpoint")
					} else {
						scoped.Log(ctx).Info("checkpoint acknowledged", "position", commitSeq)
					}
				}
			}
		}
	}()

	// 3. Periodic lease renewal for exclusive-local hold (renew well inside 30s lease)
	if renewCtx != nil && renewWg != nil {
		renewWg.Add(1)
		go func() {
			defer renewWg.Done()
			ticker := time.NewTicker(10 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-renewCtx.Done():
					return
				case <-ticker.C:
					ctxTimeout, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					resp, err := client.RenewExclusive(ctxTimeout, &pb.RenewExclusiveRequest{
						VolumeId: volumeID,
						NodeId:   d.nodeID,
						MountId:  mountID,
					})
					cancel()
					if err != nil {
						st, ok := status.FromError(err)
						if ok && (st.Code() == codes.FailedPrecondition || st.Code() == codes.Aborted) {
							scoped.Log(renewCtx).Error(err, "lost exclusive hold to another node while still mounted (split hand-off)")
						} else {
							scoped.Log(renewCtx).Info("failed to renew exclusive hold", "err", err)
						}
					} else if resp.GetHold() != nil {
						scoped.Log(renewCtx).V(4).Info("renewed exclusive hold",
							"leaseExpiry", resp.GetHold().GetLeaseExpiry().AsTime().Format(time.RFC3339))
					}
				}
			}
		}()
	}
}

func (d *objectFSDriver) unpublishExclusiveLocal(ctx context.Context, req *csi.NodeUnpublishVolumeRequest, volumeID, volumeDir string, mountInfo *activeMount) (*csi.NodeUnpublishVolumeResponse, error) {
	log := scoped.Log(ctx).WithValues("volume", volumeID)
	ctx = scoped.WithLog(ctx, log)
	targetPath := req.GetTargetPath()
	log.Info("unpublishing ObjectFS exclusive-local volume", "target", targetPath)

	lowerPath := filepath.Join(volumeDir, "lower")
	upperPath := filepath.Join(volumeDir, "upper")
	manifestPath := filepath.Join(volumeDir, "checkpoint.json")

	mountID := targetPath
	if mountInfo != nil && mountInfo.mountID != "" {
		mountID = mountInfo.mountID
	}

	client, err := d.getClient(ctx)
	if err != nil {
		log.Error(err, "failed to connect to controller for checkpoint, leaving upper layer intact")
		return nil, fmt.Errorf("failed to get controller client: %w", err)
	}

	// Unpublish ordering:
	// The hold must be live for as long as there are uncommitted bytes in upper/;
	// it is released by a successful final checkpoint or by node death, never by time while the node is alive.
	//
	// Synchronously renew the hold at the top of unpublish so the lease is immediately fresh.
	renewCtx, renewCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if resp, renewErr := client.RenewExclusive(renewCtx, &pb.RenewExclusiveRequest{
		VolumeId: volumeID,
		NodeId:   d.nodeID,
		MountId:  mountID,
	}); renewErr != nil {
		st, ok := status.FromError(renewErr)
		if ok && (st.Code() == codes.FailedPrecondition || st.Code() == codes.Aborted) {
			log.Error(renewErr, "lost exclusive hold to another node while unpublishing (split hand-off)")
		} else {
			log.Info("failed to renew exclusive hold at start of unpublish", "err", renewErr)
		}
	} else if resp.GetHold() != nil {
		log.V(4).Info("renewed exclusive hold for unpublish",
			"leaseExpiry", resp.GetHold().GetLeaseExpiry().AsTime().Format(time.RFC3339))
	}
	renewCancel()

	// 1. Cancel background watcher and checkpoint ticker.
	// NOTE: We do NOT cancel the background renew ticker here. It must keep renewing the hold
	// while unmount, scan, and replay are in progress, and while retries occur if replay fails.
	if mountInfo != nil && mountInfo.cancelFunc != nil {
		mountInfo.cancelFunc()
	}

	// 2. Wait for background watcher and checkpoint goroutines to finish cleanly
	if mountInfo != nil && mountInfo.syncWg != nil {
		mountInfo.syncWg.Wait()
	}

	// 3. Unmount overlay from targetPath
	if err := unmountTarget(ctx, targetPath); err != nil {
		return nil, fmt.Errorf("failed to unmount overlay at %s: %w", targetPath, err)
	}

	// 4. Scan upper layer against checkpoint manifest
	manifest, err := LoadCheckpointManifest(manifestPath)
	if err != nil {
		log.Info("failed to load checkpoint manifest", "err", err)
		manifest = nil
	}

	opList, err := ScanUpperLayer(ctx, lowerPath, upperPath, manifest)
	if err != nil {
		log.Error(err, "failed to scan upper layer, leaving upper layer intact")
		return nil, fmt.Errorf("checkpoint scan failed: %w", err)
	}

	// 5. Connect to controller and replay operations (commits final unmount checkpoint)
	if _, err := ReplayOperations(ctx, client, volumeID, opList); err != nil {
		log.Error(err, "failed to replay checkpoint operations, leaving upper layer intact")
		return nil, fmt.Errorf("checkpoint replay failed: %w", err)
	}

	if err := UpdateCheckpointManifest(manifest, upperPath, opList); err != nil {
		log.Error(err, "failed to update checkpoint manifest, leaving upper layer intact")
		return nil, fmt.Errorf("checkpoint manifest update failed: %w", err)
	}

	// 6. On SUCCESS only, clean up volumeDir
	if err := os.RemoveAll(volumeDir); err != nil {
		log.Info("failed to cleanup volume directory after checkpoint", "path", volumeDir, "err", err)
	}

	// 7. Release exclusive hold AFTER the final checkpoint has committed, never before.
	relCtx, relCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer relCancel()
	if _, relErr := client.ReleaseExclusive(relCtx, &pb.ReleaseExclusiveRequest{
		VolumeId: volumeID,
		NodeId:   d.nodeID,
		MountId:  mountID,
	}); relErr != nil {
		log.Info("failed to release exclusive hold after checkpoint", "err", relErr)
	} else {
		log.Info("released exclusive hold", "node", d.nodeID, "mount", mountID)
	}

	// 8. Now that ReleaseExclusive has completed, stop the background renew ticker.
	// Note: If the daemon restarted between publish and unpublish, mountInfo is nil so only
	// the synchronous renew at the start of unpublish protects the hold during the final
	// checkpoint (giving it 30s). Full daemon restart recovery across unpublish is tracked in #132.
	if mountInfo != nil && mountInfo.renewCancel != nil {
		mountInfo.renewCancel()
		if mountInfo.renewWg != nil {
			mountInfo.renewWg.Wait()
		}
	}

	log.Info("successfully unpublished and checkpointed ObjectFS volume")
	return &csi.NodeUnpublishVolumeResponse{}, nil
}
