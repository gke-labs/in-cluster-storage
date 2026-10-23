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
	"io"
	"os"
	"strings"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/objectfs/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/errutil"
	"github.com/gke-labs/in-cluster-storage/pkg/grpcutil"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type options struct {
	serverAddr string
	caCert     string
	token      string
	insecure   bool
}

func dialServer(opts *options) (pb.ObjectFSControllerClient, io.Closer, error) {
	addr := opts.serverAddr
	if addr == "" {
		addr = os.Getenv("OBJECTFS_SERVER")
	}
	if addr == "" {
		return nil, nil, fmt.Errorf("--server is required")
	}

	target := addr
	if strings.HasPrefix(target, "tcp://") {
		target = strings.TrimPrefix(target, "tcp://")
	}

	var dialOpts []grpc.DialOption
	if opts.insecure {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		caFile := opts.caCert
		if caFile == "" {
			caFile = os.Getenv("OBJECTFS_CA")
		}
		if caFile == "" && grpcutil.FileExists("/etc/objectfs/tls/ca.crt") {
			caFile = "/etc/objectfs/tls/ca.crt"
		}
		if caFile == "" {
			return nil, nil, fmt.Errorf("CA certificate required (provide --ca or OBJECTFS_CA, or specify --insecure for plaintext)")
		}
		if !grpcutil.FileExists(caFile) {
			return nil, nil, fmt.Errorf("CA certificate file not found: %s", caFile)
		}

		tlsCreds, err := grpcutil.NewClientTLSFromFile(caFile)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to load TLS credentials: %w", err)
		}
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(tlsCreds))

		tok := opts.token
		if tok == "" {
			tok = os.Getenv("OBJECTFS_TOKEN")
		}
		if tok == "" && grpcutil.FileExists("/var/run/secrets/tokens/token") {
			tok = "/var/run/secrets/tokens/token"
		}
		if tok != "" {
			if grpcutil.FileExists(tok) {
				dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(grpcutil.NewTokenFileCredentials(tok)))
			} else if strings.HasPrefix(tok, "/") || strings.HasPrefix(tok, "./") {
				return nil, nil, fmt.Errorf("token file not found: %s", tok)
			} else {
				dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(grpcutil.NewTokenCredentials(tok)))
			}
		}
	}

	conn, err := grpc.NewClient(target, grpcutil.DialOptions(dialOpts...)...)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to server at %s: %w", addr, err)
	}

	client := pb.NewObjectFSControllerClient(conn)
	return client, conn, nil
}

func NewRootCommand() *cobra.Command {
	opts := &options{}

	rootCmd := &cobra.Command{
		Use:           "objectfsctl",
		Aliases:       []string{"objectfs"},
		Short:         "objectfs administrative CLI",
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	rootCmd.PersistentFlags().StringVar(&opts.serverAddr, "server", "", "ObjectFS server address (e.g. localhost:50051 or unix:///path/to/socket)")
	rootCmd.PersistentFlags().StringVar(&opts.caCert, "ca", "", "Path to CA certificate file for controller TLS verification")
	rootCmd.PersistentFlags().StringVar(&opts.token, "token", "", "Token string or path to token file for controller authentication")
	rootCmd.PersistentFlags().BoolVar(&opts.insecure, "insecure", false, "Connect to controller using plaintext gRPC without authentication")

	rootCmd.AddCommand(newBlobsCommand(opts))
	rootCmd.AddCommand(newVolumesCommand(opts))
	rootCmd.AddCommand(newSnapshotsCommand(opts))
	rootCmd.AddCommand(newMountCommand(opts))
	rootCmd.AddCommand(newCheckpointCommand(opts))

	return rootCmd
}

func newBlobsCommand(opts *options) *cobra.Command {
	blobsCmd := &cobra.Command{
		Use:     "blobs",
		Aliases: []string{"blob"},
		Short:   "Manage underlying objects/blobs in the store",
	}

	blobsCmd.AddCommand(newBlobsListCommand(opts))
	blobsCmd.AddCommand(newBlobsGetCommand(opts))

	return blobsCmd
}

type blobsListOptions struct {
	*options
	from   string
	limit  int32
	prefix string
}

func newBlobsListCommand(opts *options) *cobra.Command {
	listOpts := &blobsListOptions{options: opts}
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List all blobs in the store",
		RunE: func(cmd *cobra.Command, args []string) error {
			if listOpts.serverAddr == "" {
				return fmt.Errorf("--server is required")
			}

			client, conn, err := dialServer(listOpts.options)
			if err != nil {
				return err
			}
			defer errutil.CloseAndLog(conn, "server connection")

			fromSHA := listOpts.from
			remainingLimit := listOpts.limit
			out := cmd.OutOrStdout()

			for {
				reqLimit := remainingLimit
				resp, err := client.ListBlobs(cmd.Context(), &pb.ListBlobsRequest{
					FromSha:   fromSHA,
					Limit:     reqLimit,
					ShaPrefix: listOpts.prefix,
				})
				if err != nil {
					return fmt.Errorf("failed to list blobs: %w", err)
				}

				for _, sha := range resp.GetSha256() {
					if _, err := fmt.Fprintln(out, sha); err != nil {
						return fmt.Errorf("failed writing to output: %w", err)
					}
					fromSHA = sha
					if remainingLimit > 0 {
						remainingLimit--
						if remainingLimit == 0 {
							return nil
						}
					}
				}

				if resp.GetEndOfData() || len(resp.GetSha256()) == 0 {
					break
				}
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&listOpts.from, "from", "", "List blobs starting after this SHA256")
	cmd.Flags().Int32Var(&listOpts.limit, "limit", 0, "Maximum number of blobs to list (0 = no limit)")
	cmd.Flags().StringVar(&listOpts.prefix, "prefix", "", "Filter blobs starting with this SHA256 prefix")
	return cmd
}

type blobsGetOptions struct {
	*options
	offset int64
	limit  int64
}

func newBlobsGetCommand(opts *options) *cobra.Command {
	getOpts := &blobsGetOptions{options: opts}
	cmd := &cobra.Command{
		Use:   "get <sha256>",
		Short: "Print the contents of a blob to stdout",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if getOpts.serverAddr == "" {
				return fmt.Errorf("--server is required")
			}

			sha := args[0]
			client, conn, err := dialServer(getOpts.options)
			if err != nil {
				return err
			}
			defer errutil.CloseAndLog(conn, "server connection")

			stream, err := client.GetBlob(cmd.Context(), &pb.GetBlobRequest{
				Sha256: sha,
				Offset: getOpts.offset,
				Limit:  getOpts.limit,
			})
			if err != nil {
				return fmt.Errorf("failed to get blob: %w", err)
			}

			out := cmd.OutOrStdout()
			for {
				chunk, err := stream.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					return fmt.Errorf("failed receiving blob data: %w", err)
				}
				if len(chunk.GetData()) > 0 {
					if _, err := out.Write(chunk.GetData()); err != nil {
						return fmt.Errorf("failed writing to output: %w", err)
					}
				}
			}
			return nil
		},
	}
	cmd.Flags().Int64Var(&getOpts.offset, "offset", 0, "Offset in bytes to start reading from")
	cmd.Flags().Int64Var(&getOpts.limit, "limit", 0, "Maximum number of bytes to read (0 = no limit)")
	return cmd
}

func newVolumesCommand(opts *options) *cobra.Command {
	volumesCmd := &cobra.Command{
		Use:     "volumes",
		Aliases: []string{"volume", "vol"},
		Short:   "Manage volumes in the store",
	}

	volumesCmd.AddCommand(newVolumesListCommand(opts))
	volumesCmd.AddCommand(newVolumesDescribeCommand(opts))

	return volumesCmd
}

func newVolumesDescribeCommand(opts *options) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "describe <volume>",
		Aliases: []string{"desc", "show", "get"},
		Short:   "Describe a volume and its current exclusive-local hold",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if opts.serverAddr == "" {
				return fmt.Errorf("--server is required")
			}
			volumeID := args[0]
			if volumeID == "" {
				return fmt.Errorf("volume ID is required")
			}

			client, conn, err := dialServer(opts)
			if err != nil {
				return err
			}
			defer errutil.CloseAndLog(conn, "server connection")

			out := cmd.OutOrStdout()
			var printErr error
			printf := func(format string, a ...any) {
				if printErr != nil {
					return
				}
				if _, err := fmt.Fprintf(out, format, a...); err != nil {
					printErr = fmt.Errorf("failed writing to output: %w", err)
				}
			}
			println := func(a ...any) {
				if printErr != nil {
					return
				}
				if _, err := fmt.Fprintln(out, a...); err != nil {
					printErr = fmt.Errorf("failed writing to output: %w", err)
				}
			}

			printf("Volume: %s\n", volumeID)

			holdResp, err := client.GetExclusiveHold(cmd.Context(), &pb.GetExclusiveHoldRequest{
				VolumeId: volumeID,
			})
			if err != nil {
				return fmt.Errorf("failed to get exclusive hold for volume %s: %w", volumeID, err)
			}

			hold := holdResp.GetHold()
			if hold != nil && hold.GetNodeId() != "" {
				println("Exclusive Hold:")
				printf("  Holder Node:  %s\n", hold.GetNodeId())
				printf("  Mount ID:     %s\n", hold.GetMountId())
				if hold.GetAcquiredAt() != nil {
					printf("  Acquired:     %s\n", hold.GetAcquiredAt().AsTime().Format(time.RFC3339))
				}
				if hold.GetLeaseExpiry() != nil {
					expiry := hold.GetLeaseExpiry().AsTime()
					remaining := time.Until(expiry).Round(time.Second)
					if remaining > 0 {
						printf("  Lease Expiry: %s (%s remaining)\n", expiry.Format(time.RFC3339), remaining)
					} else {
						printf("  Lease Expiry: %s (expired)\n", expiry.Format(time.RFC3339))
					}
				}
			} else {
				println("Exclusive Hold: none")
			}

			// Also display stats if volume exists in store
			statsResp, err := client.GetVolumeStats(cmd.Context(), &pb.GetVolumeStatsRequest{
				VolumeId: volumeID,
			})
			if err != nil {
				printf("Stats: unavailable (%v)\n", err)
			} else if statsResp.GetStats() != nil {
				s := statsResp.GetStats()
				println("Stats:")
				printf("  Files:         %d\n", s.GetInodesFile())
				printf("  Directories:   %d\n", s.GetInodesDir())
				printf("  Logical Bytes: %d\n", s.GetLogicalBytes())
			}

			return printErr
		},
	}
	return cmd
}

type volumesListOptions struct {
	*options
	from  string
	limit int32
}

func newVolumesListCommand(opts *options) *cobra.Command {
	listOpts := &volumesListOptions{options: opts}
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List all volumes in the store",
		RunE: func(cmd *cobra.Command, args []string) error {
			if listOpts.serverAddr == "" {
				return fmt.Errorf("--server is required")
			}

			client, conn, err := dialServer(listOpts.options)
			if err != nil {
				return err
			}
			defer errutil.CloseAndLog(conn, "server connection")

			fromVol := listOpts.from
			remainingLimit := listOpts.limit
			out := cmd.OutOrStdout()

			for {
				reqLimit := remainingLimit
				resp, err := client.ListVolumes(cmd.Context(), &pb.ListVolumesRequest{
					FromVolumeId: fromVol,
					Limit:        reqLimit,
				})
				if err != nil {
					return fmt.Errorf("failed to list volumes: %w", err)
				}

				for _, vol := range resp.GetVolumes() {
					if _, err := fmt.Fprintln(out, vol.GetVolumeId()); err != nil {
						return fmt.Errorf("failed writing to output: %w", err)
					}
					fromVol = vol.GetVolumeId()
					if remainingLimit > 0 {
						remainingLimit--
						if remainingLimit == 0 {
							return nil
						}
					}
				}

				if resp.GetEndOfData() || len(resp.GetVolumes()) == 0 {
					break
				}
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&listOpts.from, "from", "", "List volumes starting after this volume ID")
	cmd.Flags().Int32Var(&listOpts.limit, "limit", 0, "Maximum number of volumes to list (0 = no limit)")
	return cmd
}

func newSnapshotsCommand(opts *options) *cobra.Command {
	snapshotsCmd := &cobra.Command{
		Use:     "snapshots",
		Aliases: []string{"snapshot", "snap"},
		Short:   "Manage snapshots for volumes",
	}

	snapshotsCmd.AddCommand(newSnapshotsListCommand(opts))
	snapshotsCmd.AddCommand(newSnapshotCreateCommand(opts))

	return snapshotsCmd
}

type snapshotsListOptions struct {
	*options
	volumeID string
	from     string
	fromTime string
	to       string
	toTime   string
	limit    int32
}

func parseTimeFlag(val string) (time.Time, error) {
	formats := []string{
		"2006-01-02T15:04:05.000000Z",
		"2006-01-02T15:04:05Z",
		"2006-01-02 15:04:05",
		"2006-01-02",
		"20060102T150405.000000Z",
		"20060102T150405Z",
	}
	var lastErr error
	for _, f := range formats {
		var t time.Time
		t, lastErr = time.Parse(f, val)
		if lastErr == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("cannot parse %q as timestamp: %w", val, lastErr)
}

func newSnapshotsListCommand(opts *options) *cobra.Command {
	listOpts := &snapshotsListOptions{options: opts}
	cmd := &cobra.Command{
		Use:     "list [volume_id]",
		Aliases: []string{"ls"},
		Short:   "List snapshots for a volume, paginated by time",
		Args:    cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if listOpts.serverAddr == "" {
				return fmt.Errorf("--server is required")
			}

			volID := listOpts.volumeID
			if len(args) > 0 && args[0] != "" {
				volID = args[0]
			}
			if volID == "" {
				return fmt.Errorf("volume_id is required")
			}

			client, conn, err := dialServer(listOpts.options)
			if err != nil {
				return err
			}
			defer errutil.CloseAndLog(conn, "server connection")

			var fromTimePb, toTimePb *timestamppb.Timestamp
			fromSnapshot := ""

			if listOpts.fromTime != "" {
				t, err := parseTimeFlag(listOpts.fromTime)
				if err != nil {
					return fmt.Errorf("invalid --from-time %q: %w", listOpts.fromTime, err)
				}
				fromTimePb = timestamppb.New(t)
			}

			if listOpts.from != "" {
				// --from has dual meaning (documented in flag help as timestamp or snapshot name):
				// if it parses as a timestamp and --from-time is unset, use it as a time bound;
				// otherwise a parse failure legitimately means it is a snapshot name to resume listing after.
				if t, err := parseTimeFlag(listOpts.from); err == nil && listOpts.fromTime == "" {
					fromTimePb = timestamppb.New(t)
				} else {
					fromSnapshot = listOpts.from
				}
			}

			toVal := listOpts.to
			toFlag := "--to"
			if listOpts.toTime != "" {
				toVal = listOpts.toTime
				toFlag = "--to-time"
			}
			if toVal != "" {
				t, err := parseTimeFlag(toVal)
				if err != nil {
					return fmt.Errorf("invalid %s %q: %w", toFlag, toVal, err)
				}
				toTimePb = timestamppb.New(t)
			}

			remainingLimit := listOpts.limit
			out := cmd.OutOrStdout()

			for {
				reqLimit := remainingLimit
				resp, err := client.ListSnapshots(cmd.Context(), &pb.ListSnapshotsRequest{
					VolumeId:     volID,
					FromTime:     fromTimePb,
					ToTime:       toTimePb,
					Limit:        reqLimit,
					FromSnapshot: fromSnapshot,
				})
				if err != nil {
					return fmt.Errorf("failed to list snapshots: %w", err)
				}

				for _, snap := range resp.GetSnapshots() {
					if _, err := fmt.Fprintln(out, snap.GetName()); err != nil {
						return fmt.Errorf("failed writing to output: %w", err)
					}
					fromSnapshot = snap.GetName()
					if remainingLimit > 0 {
						remainingLimit--
						if remainingLimit == 0 {
							return nil
						}
					}
				}

				if resp.GetEndOfData() || len(resp.GetSnapshots()) == 0 {
					break
				}
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&listOpts.volumeID, "volume", "", "Volume ID")
	cmd.Flags().StringVar(&listOpts.from, "from", "", "List snapshots created at or after this time, or after this snapshot name")
	cmd.Flags().StringVar(&listOpts.fromTime, "from-time", "", "List snapshots created at or after this time")
	cmd.Flags().StringVar(&listOpts.to, "to", "", "List snapshots created at or before this time")
	cmd.Flags().StringVar(&listOpts.toTime, "to-time", "", "List snapshots created at or before this time")
	cmd.Flags().Int32Var(&listOpts.limit, "limit", 0, "Maximum number of snapshots to list (0 = no limit)")
	return cmd
}

type snapshotCreateOptions struct {
	*options
	volumeID string
	name     string
}

func newSnapshotCreateCommand(opts *options) *cobra.Command {
	createOpts := &snapshotCreateOptions{options: opts}
	cmd := &cobra.Command{
		Use:     "create <volume_id> [name]",
		Aliases: []string{"new", "add"},
		Short:   "Create a new snapshot for a volume",
		Args:    cobra.RangeArgs(0, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			if createOpts.serverAddr == "" {
				return fmt.Errorf("--server is required")
			}

			volID := createOpts.volumeID
			if len(args) > 0 && args[0] != "" {
				volID = args[0]
			}
			if volID == "" {
				return fmt.Errorf("volume_id is required")
			}

			snapName := createOpts.name
			if len(args) > 1 && args[1] != "" {
				snapName = args[1]
			}

			client, conn, err := dialServer(createOpts.options)
			if err != nil {
				return err
			}
			defer errutil.CloseAndLog(conn, "server connection")

			resp, err := client.CreateSnapshot(cmd.Context(), &pb.CreateSnapshotRequest{
				VolumeId: volID,
				Name:     snapName,
			})
			if err != nil {
				return fmt.Errorf("failed to create snapshot: %w", err)
			}

			out := cmd.OutOrStdout()
			if _, err := fmt.Fprintln(out, resp.GetSnapshotName()); err != nil {
				return fmt.Errorf("failed writing to output: %w", err)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&createOpts.volumeID, "volume", "", "Volume ID")
	cmd.Flags().StringVar(&createOpts.name, "name", "", "Snapshot name (optional)")
	return cmd
}

func newCheckpointCommand(opts *options) *cobra.Command {
	var timeout time.Duration
	cmd := &cobra.Command{
		Use:     "checkpoint <volume>",
		Aliases: []string{"cp"},
		Short:   "Trigger an explicit checkpoint on an exclusive-local volume and wait for it",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			volumeID := args[0]
			if volumeID == "" {
				return fmt.Errorf("volume ID is required")
			}

			client, conn, err := dialServer(opts)
			if err != nil {
				return err
			}
			defer errutil.CloseAndLog(conn, "server connection")

			ctx := cmd.Context()
			if timeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, timeout)
				defer cancel()
			}

			resp, err := client.Checkpoint(ctx, &pb.CheckpointRequest{
				VolumeId: volumeID,
			})
			if err != nil {
				return fmt.Errorf("checkpoint failed for volume %s: %w", volumeID, err)
			}

			out := cmd.OutOrStdout()
			if _, err := fmt.Fprintf(out, "Checkpoint completed for volume %s at stream position %d\n", volumeID, resp.GetStreamPosition()); err != nil {
				return fmt.Errorf("failed writing output: %w", err)
			}
			return nil
		},
	}
	cmd.Flags().DurationVar(&timeout, "timeout", 0, "Timeout for waiting for the checkpoint to complete (default no timeout)")
	return cmd
}

func main() {
	rootCmd := NewRootCommand()
	if err := rootCmd.ExecuteContext(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
