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
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/errutil"
	objectfuse "github.com/gke-labs/in-cluster-storage/pkg/objectfs/fuse"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/spf13/cobra"
)

type mountOptions struct {
	*options
	volumeID     string
	writeMode    string
	cacheSizeMB  int64
	nodeID       string
	allowOther   bool
	debug        bool
	entryTimeout time.Duration
	attrTimeout  time.Duration
}

func parseWriteMode(val string) (pb.WriteMode, error) {
	switch strings.ToLower(val) {
	case "", "fsync", "writethrough", "write_through_fsync":
		return pb.WriteMode_WRITE_THROUGH_FSYNC, nil
	case "lazy", "lazywrite", "lazy_write":
		return pb.WriteMode_LAZY_WRITE, nil
	case "eager", "eagerreplication", "eager_replication":
		return pb.WriteMode_EAGER_REPLICATION, nil
	default:
		return pb.WriteMode_WRITE_THROUGH_FSYNC, fmt.Errorf("unknown write mode %q (expected fsync, lazy or eager)", val)
	}
}

// newMountCommand mounts a volume via FUSE directly, without going through the
// CSI node daemon. It is intended for local development and debugging.
func newMountCommand(opts *options) *cobra.Command {
	mountOpts := &mountOptions{options: opts}
	cmd := &cobra.Command{
		Use:   "mount <mountpoint>",
		Short: "Mount a volume via FUSE (blocks until interrupted)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runMount(cmd.Context(), mountOpts, args[0])
		},
	}
	cmd.Flags().StringVar(&mountOpts.volumeID, "volume", "", "Volume ID to mount (required)")
	cmd.Flags().StringVar(&mountOpts.writeMode, "write-mode", "fsync", "Write mode: fsync, lazy or eager")
	cmd.Flags().Int64Var(&mountOpts.cacheSizeMB, "cache-size-mb", 128, "Local cache size in MB")
	cmd.Flags().StringVar(&mountOpts.nodeID, "node-id", "", "Node ID reported to the controller (defaults to the hostname)")
	cmd.Flags().BoolVar(&mountOpts.allowOther, "allow-other", false, "Allow other users to access the mount (requires user_allow_other in /etc/fuse.conf when not root)")
	cmd.Flags().BoolVar(&mountOpts.debug, "debug", false, "Log every FUSE request")
	cmd.Flags().DurationVar(&mountOpts.entryTimeout, "entry-timeout", 60*time.Second, "Kernel dentry cache timeout while watcher is healthy")
	cmd.Flags().DurationVar(&mountOpts.attrTimeout, "attr-timeout", 60*time.Second, "Kernel attribute cache timeout while watcher is healthy")
	return cmd
}

func runMount(ctx context.Context, opts *mountOptions, mountpoint string) error {
	if opts.serverAddr == "" {
		return fmt.Errorf("--server is required")
	}
	if opts.volumeID == "" {
		return fmt.Errorf("--volume is required")
	}
	writeMode, err := parseWriteMode(opts.writeMode)
	if err != nil {
		return err
	}
	nodeID := opts.nodeID
	if nodeID == "" {
		nodeID, err = os.Hostname()
		if err != nil {
			return fmt.Errorf("failed to determine hostname (set --node-id): %w", err)
		}
	}

	if err := os.MkdirAll(mountpoint, 0755); err != nil {
		return fmt.Errorf("failed to create mountpoint %s: %w", mountpoint, err)
	}

	client, conn, err := dialServer(opts.options)
	if err != nil {
		return err
	}
	defer errutil.CloseAndLog(conn, "server connection")

	// Fail early with a clear error if the controller is not reachable.
	if resp, err := client.GetAttr(ctx, &pb.GetAttrRequest{VolumeId: opts.volumeID, Inode: 1}); err != nil {
		return fmt.Errorf("failed to reach controller at %s for volume %s: %w", opts.serverAddr, opts.volumeID, err)
	} else if resp.GetError() != 0 {
		return fmt.Errorf("failed to get root directory from controller at %s for volume %s: %w", opts.serverAddr, opts.volumeID, syscall.Errno(resp.GetError()))
	}

	nodeCache := objectfuse.NewNodeCache(opts.cacheSizeMB * 1024 * 1024)
	rawFS := objectfuse.NewObjectFS(client, opts.volumeID, writeMode, nodeCache)
	rawFS.SetEntryTimeout(opts.entryTimeout)
	rawFS.SetAttrTimeout(opts.attrTimeout)

	fuseOpts := &gofuse.MountOptions{
		FsName:     "objectfs",
		Name:       "objectfs",
		AllowOther: true,
		Debug:      opts.debug,
		Options:    []string{"default_permissions", "allow_other"},
	}

	server, err := gofuse.NewServer(rawFS, mountpoint, fuseOpts)
	if err != nil {
		return fmt.Errorf("failed to mount FUSE filesystem at %s: %w", mountpoint, err)
	}
	go server.Serve()
	if err := server.WaitMount(); err != nil {
		if unmountErr := server.Unmount(); unmountErr != nil {
			log.Printf("failed to unmount %s after WaitMount failure: %v", mountpoint, unmountErr)
		}
		return fmt.Errorf("failed waiting for FUSE mount at %s: %w", mountpoint, err)
	}

	watchCtx, cancelWatch := context.WithCancel(ctx)
	defer cancelWatch()
	objectfuse.StartWatcher(watchCtx, client, opts.volumeID, nodeID, nodeCache, rawFS)

	fmt.Fprintf(os.Stderr, "Mounted volume %s at %s (writeMode=%v); press Ctrl-C to unmount\n", opts.volumeID, mountpoint, writeMode)

	// Exit when the mount is torn down externally (e.g. umount) or when interrupted.
	served := make(chan struct{})
	go func() {
		server.Wait()
		close(served)
	}()

	sigCtx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	select {
	case <-served:
		fmt.Fprintf(os.Stderr, "FUSE server for %s exited\n", mountpoint)
		return nil
	case <-sigCtx.Done():
	}

	fmt.Fprintf(os.Stderr, "Unmounting %s\n", mountpoint)
	if err := server.Unmount(); err != nil {
		return fmt.Errorf("failed to unmount %s: %w", mountpoint, err)
	}
	<-served
	return nil
}
