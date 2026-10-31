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

package buffertest

import (
	"context"
	"errors"
	"net"
	"os"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/wal/v1alpha1"
	"github.com/gke-labs/in-cluster-storage/pkg/errutil"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore"
	"github.com/gke-labs/in-cluster-storage/pkg/objectstore/inmemorystorage"
	"github.com/gke-labs/in-cluster-storage/pkg/wal/buffer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"k8s.io/klog/v2"
)

// BufferTest wraps an in-process WAL buffer server running over bufconn.
type BufferTest struct {
	Server   *buffer.Server
	Target   string
	DialOpts []grpc.DialOption
	Client   pb.WalBufferClient
	Backend  objectstore.Backend
	DataDir  string
	conn     *grpc.ClientConn
	grpcSrv  *grpc.Server
	listener *bufconn.Listener
}

// Close stops the buffer server and closes connections and listeners.
func (bt *BufferTest) Close() {
	if bt.conn != nil {
		errutil.CloseAndLog(bt.conn, "buffertest client connection")
	}
	if bt.grpcSrv != nil {
		bt.grpcSrv.Stop()
	}
	if bt.Server != nil {
		errutil.CloseAndLog(bt.Server, "buffertest server")
	}
	if bt.listener != nil {
		errutil.CloseAndLog(bt.listener, "buffertest listener")
	}
}

// Option configures BufferTest options.
type Option func(*config)

type config struct {
	backend            objectstore.Backend
	dataDir            string
	flushInterval      time.Duration
	batchMaxDelay      time.Duration
	streamSealInterval time.Duration
	bufSize            int
	noFsync            bool
	acceptUnversioned  bool
}

// WithBackend configures the backend object storage for the buffer.
func WithBackend(backend objectstore.Backend) Option {
	return func(c *config) {
		c.backend = backend
	}
}

// WithDataDir configures the local data directory for the buffer segments.
func WithDataDir(dir string) Option {
	return func(c *config) {
		c.dataDir = dir
	}
}

// WithFlushInterval configures the buffer flush interval to object storage.
func WithFlushInterval(d time.Duration) Option {
	return func(c *config) {
		c.flushInterval = d
	}
}

// WithBatchMaxDelay configures the batch maximum delay before a commit is acknowledged.
func WithBatchMaxDelay(d time.Duration) Option {
	return func(c *config) {
		c.batchMaxDelay = d
	}
}

// WithStreamSealInterval configures the stream seal interval.
func WithStreamSealInterval(d time.Duration) Option {
	return func(c *config) {
		c.streamSealInterval = d
	}
}

// WithBufSize configures the in-memory bufconn buffer size.
func WithBufSize(size int) Option {
	return func(c *config) {
		c.bufSize = size
	}
}

// WithNoFsync configures whether the buffer skips fsync to disk (defaults to true for test speed).
func WithNoFsync(noFsync bool) Option {
	return func(c *config) {
		c.noFsync = noFsync
	}
}

// WithAcceptUnversioned configures whether unversioned segments are accepted.
func WithAcceptUnversioned(accept bool) Option {
	return func(c *config) {
		c.acceptUnversioned = accept
	}
}

// Start launches an in-process WAL buffer server over bufconn.
// If t is provided, the buffer's Close is automatically registered via t.Cleanup.
func Start(ctx context.Context, t testing.TB, opts ...Option) (*BufferTest, func()) {
	if t != nil {
		t.Helper()
	}
	cfg := config{
		flushInterval: 10 * time.Second,
		batchMaxDelay: 2 * time.Millisecond,
		bufSize:       4 * 1024 * 1024,
		noFsync:       true,
	}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.backend == nil {
		cfg.backend = inmemorystorage.New()
	}
	if cfg.dataDir == "" {
		if t != nil {
			cfg.dataDir = t.TempDir()
		} else {
			dir, err := os.MkdirTemp("", "buffertest-*")
			if err != nil {
				panic(err)
			}
			cfg.dataDir = dir
		}
	}

	srv, err := buffer.NewServer(ctx, buffer.ServerConfig{
		Backend:            cfg.backend,
		DataDir:            cfg.dataDir,
		FlushInterval:      cfg.flushInterval,
		BatchMaxDelay:      cfg.batchMaxDelay,
		StreamSealInterval: cfg.streamSealInterval,
		NoFsync:            cfg.noFsync,
		AcceptUnversioned:  cfg.acceptUnversioned,
	})
	if err != nil {
		if t != nil {
			t.Fatalf("buffertest: failed to create buffer server: %v", err)
		}
		panic(err)
	}

	lis := bufconn.Listen(cfg.bufSize)
	grpcServer := grpc.NewServer()
	pb.RegisterWalBufferServer(grpcServer, srv)

	go func() {
		if err := grpcServer.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			klog.V(4).Infof("buffertest: grpcServer.Serve exited: %v", err)
		}
	}()

	dialOpts := []grpc.DialOption{
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	conn, err := grpc.NewClient("passthrough://bufnet", dialOpts...)
	if err != nil {
		if t != nil {
			t.Fatalf("buffertest: failed to dial buffer: %v", err)
		}
		panic(err)
	}

	client := pb.NewWalBufferClient(conn)

	bt := &BufferTest{
		Server:   srv,
		Target:   "passthrough://bufnet",
		DialOpts: dialOpts,
		Client:   client,
		Backend:  cfg.backend,
		DataDir:  cfg.dataDir,
		conn:     conn,
		grpcSrv:  grpcServer,
		listener: lis,
	}

	cleanup := func() {
		bt.Close()
	}

	if t != nil {
		t.Cleanup(cleanup)
	}

	return bt, cleanup
}
