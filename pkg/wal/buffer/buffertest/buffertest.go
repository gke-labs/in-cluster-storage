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
	"sync"
	"testing"
	"time"

	pb "github.com/gke-labs/in-cluster-storage/pkg/api/wal/v1alpha1"
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
	Server    *buffer.Server
	Target    string
	DialOpts  []grpc.DialOption
	Client    pb.WalBufferClient
	Backend   objectstore.Backend
	DataDir   string
	conn      *grpc.ClientConn
	grpcSrv   *grpc.Server
	listener  *bufconn.Listener
	closeOnce sync.Once
	closeErr  error
}

// Close stops the buffer server and closes connections and listeners.
func (bt *BufferTest) Close() error {
	bt.closeOnce.Do(func() {
		var errs []error
		if bt.conn != nil {
			if err := bt.conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				errs = append(errs, err)
			}
		}
		if bt.grpcSrv != nil {
			bt.grpcSrv.Stop()
		}
		if bt.Server != nil {
			if err := bt.Server.Close(); err != nil {
				errs = append(errs, err)
			}
		}
		if bt.listener != nil {
			if err := bt.listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				errs = append(errs, err)
			}
		}
		bt.closeErr = errors.Join(errs...)
	})
	return bt.closeErr
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
	writerLockGrace    time.Duration
	clock              buffer.Clock
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

// FakeClock implements buffer.Clock with a manually controlled timestamp.
type FakeClock struct {
	mu  sync.Mutex
	now time.Time
}

// NewFakeClock returns a FakeClock fixed at start.
func NewFakeClock(start time.Time) *FakeClock {
	return &FakeClock{now: start}
}

// Now returns the current clock time.
func (clock *FakeClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

// Advance moves the clock forward by duration.
func (clock *FakeClock) Advance(duration time.Duration) {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	clock.now = clock.now.Add(duration)
}

// WithWriterLockGrace configures the writer lock grace period on the buffer server.
func WithWriterLockGrace(gracePeriod time.Duration) Option {
	return func(c *config) {
		c.writerLockGrace = gracePeriod
	}
}

// WithClock configures the clock used by the buffer server for writer lock grace periods.
func WithClock(clock buffer.Clock) Option {
	return func(c *config) {
		c.clock = clock
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
		WriterLockGrace:    cfg.writerLockGrace,
		Clock:              cfg.clock,
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
		if err := bt.Close(); err != nil {
			if t != nil {
				t.Fatalf("buffertest: close failed: %v", err)
			} else {
				klog.Warningf("buffertest: close failed: %v", err)
			}
		}
	}

	if t != nil {
		t.Cleanup(cleanup)
	}

	return bt, cleanup
}
