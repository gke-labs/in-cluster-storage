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

package grpcutil

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/gke-labs/in-cluster-storage/pkg/scoped"
	"google.golang.org/grpc/credentials"
)

// CertReloader dynamically monitors and reloads TLS certificates and trust pools from a directory.
type CertReloader struct {
	dir        string
	reloadMu   sync.Mutex
	mu         sync.RWMutex
	cert       *tls.Certificate
	trustPool  *x509.CertPool
	lastFailed bool

	watcher  *fsnotify.Watcher
	stopOnce sync.Once
	stopCh   chan struct{}
	doneCh   chan struct{}
}

func (r *CertReloader) reload() error {
	certFile := filepath.Join(r.dir, "tls.crt")
	keyFile := filepath.Join(r.dir, "tls.key")
	trustDir := filepath.Join(r.dir, "trust")

	if !FileExists(certFile) {
		return fmt.Errorf("missing certificate file %s", certFile)
	}
	if !FileExists(keyFile) {
		return fmt.Errorf("missing private key file %s", keyFile)
	}

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return fmt.Errorf("failed to load keypair from %s and %s: %w", certFile, keyFile, err)
	}

	trustPool, err := LoadTrustBundle(trustDir)
	if err != nil {
		return fmt.Errorf("failed to load trust bundle from %s: %w", trustDir, err)
	}

	r.mu.Lock()
	r.cert = &cert
	r.trustPool = trustPool
	r.mu.Unlock()

	return nil
}

// Reload forces a reload of the certificate and trust bundle from disk.
// If reload fails, the last valid material is retained and a warning is logged once.
func (r *CertReloader) Reload(ctx context.Context) error {
	r.reloadMu.Lock()
	defer r.reloadMu.Unlock()

	err := r.reload()
	r.mu.Lock()
	defer r.mu.Unlock()

	if err != nil {
		if !r.lastFailed {
			r.lastFailed = true
			scoped.Log(ctx).Info("failed to reload TLS material (keeping last good material)", "dir", r.dir, "err", err)
		}
		return err
	}

	if r.lastFailed {
		r.lastFailed = false
		scoped.Log(ctx).Info("successfully reloaded TLS material", "dir", r.dir)
	}
	return nil
}

func (r *CertReloader) run(ctx context.Context) {
	defer close(r.doneCh)

	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	trustDir := filepath.Join(r.dir, "trust")

	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			if err := r.Reload(ctx); err != nil {
				scoped.Log(ctx).V(4).Info("periodic reload failed", "dir", r.dir, "err", err)
			}
		case event, ok := <-r.watcher.Events:
			if !ok {
				return
			}
			if event.Op&fsnotify.Create != 0 && DirExists(trustDir) {
				if err := r.watcher.Add(trustDir); err != nil {
					scoped.Log(ctx).V(4).Info("watching trust directory", "dir", trustDir, "err", err)
				}
			}
			if err := r.Reload(ctx); err != nil {
				scoped.Log(ctx).V(4).Info("event-triggered reload failed", "dir", r.dir, "err", err)
			}
		case err, ok := <-r.watcher.Errors:
			if !ok {
				return
			}
			scoped.Log(ctx).Info("watcher error", "dir", r.dir, "err", err)
		}
	}
}

// NewCertReloader initializes a reloader that watches dir for certificate updates
// and periodically reloads trust material.
func NewCertReloader(ctx context.Context, dir string) (*CertReloader, error) {
	if dir == "" {
		return nil, fmt.Errorf("certificate directory path must not be empty")
	}

	r := &CertReloader{
		dir:    dir,
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}

	if err := r.reload(); err != nil {
		return nil, err
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("failed to create fsnotify watcher: %w", err)
	}
	r.watcher = watcher

	if err := watcher.Add(dir); err != nil {
		if closeErr := watcher.Close(); closeErr != nil {
			scoped.Log(ctx).Info("failed to close watcher", "err", closeErr)
		}
		return nil, fmt.Errorf("failed to watch directory %s: %w", dir, err)
	}

	trustDir := filepath.Join(dir, "trust")
	if DirExists(trustDir) {
		if err := watcher.Add(trustDir); err != nil {
			if closeErr := watcher.Close(); closeErr != nil {
				scoped.Log(ctx).Info("failed to close watcher", "err", closeErr)
			}
			return nil, fmt.Errorf("failed to watch trust directory %s: %w", trustDir, err)
		}
	}

	go r.run(ctx)

	return r, nil
}

// Close stops the background watcher and ticker loops.
func (r *CertReloader) Close() error {
	var closed bool
	r.stopOnce.Do(func() {
		closed = true
		close(r.stopCh)
	})
	if !closed {
		return nil
	}
	err := r.watcher.Close()
	<-r.doneCh
	if err != nil {
		return fmt.Errorf("failed to close watcher for %s: %w", r.dir, err)
	}
	return nil
}

// CloseReloader closes the underlying CertReloader if creds supports it.
func CloseReloader(creds credentials.TransportCredentials) error {
	if c, ok := creds.(interface{ Close() error }); ok {
		return c.Close()
	}
	return nil
}

// GetCertificate returns the current server certificate for TLS handshakes.
func (r *CertReloader) GetCertificate(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.cert, nil
}

// GetClientCertificate returns the current client certificate for TLS handshakes.
func (r *CertReloader) GetClientCertificate(cri *tls.CertificateRequestInfo) (*tls.Certificate, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.cert, nil
}

// GetConfigForClient returns a *tls.Config for incoming connections with current cert and ClientCAs.
func (r *CertReloader) GetConfigForClient(hello *tls.ClientHelloInfo) (*tls.Config, error) {
	r.mu.RLock()
	cert := r.cert
	trustPool := r.trustPool
	r.mu.RUnlock()

	var certs []tls.Certificate
	if cert != nil {
		certs = []tls.Certificate{*cert}
	}
	return &tls.Config{
		Certificates:   certs,
		GetCertificate: r.GetCertificate,
		ClientCAs:      trustPool,
		ClientAuth:     tls.RequireAndVerifyClientCert,
		MinVersion:     tls.VersionTLS13,
	}, nil
}

// ServerTLSConfig returns a *tls.Config configured for server mTLS backed by this reloader.
func (r *CertReloader) ServerTLSConfig() *tls.Config {
	r.mu.RLock()
	cert := r.cert
	trustPool := r.trustPool
	r.mu.RUnlock()

	var certs []tls.Certificate
	if cert != nil {
		certs = []tls.Certificate{*cert}
	}
	return &tls.Config{
		Certificates:       certs,
		GetCertificate:     r.GetCertificate,
		GetConfigForClient: r.GetConfigForClient,
		ClientCAs:          trustPool,
		ClientAuth:         tls.RequireAndVerifyClientCert,
		MinVersion:         tls.VersionTLS13,
	}
}

// ClientTLSConfig returns a *tls.Config configured for client mTLS backed by this reloader.
func (r *CertReloader) ClientTLSConfig() *tls.Config {
	r.mu.RLock()
	cert := r.cert
	trustPool := r.trustPool
	r.mu.RUnlock()

	var certs []tls.Certificate
	if cert != nil {
		certs = []tls.Certificate{*cert}
	}
	return &tls.Config{
		Certificates:         certs,
		GetClientCertificate: r.GetClientCertificate,
		RootCAs:              trustPool,
		MinVersion:           tls.VersionTLS13,
	}
}

type serverTransportCredentials struct {
	credentials.TransportCredentials
	reloader *CertReloader
}

func (s *serverTransportCredentials) Reloader() *CertReloader {
	return s.reloader
}

func (s *serverTransportCredentials) Close() error {
	return s.reloader.Close()
}

func (s *serverTransportCredentials) Clone() credentials.TransportCredentials {
	return &serverTransportCredentials{
		TransportCredentials: s.TransportCredentials.Clone(),
		reloader:             s.reloader,
	}
}

type clientTransportCredentials struct {
	reloader *CertReloader
}

// NewClientTransportCredentials creates credentials.TransportCredentials for clients backed by a CertReloader.
func NewClientTransportCredentials(reloader *CertReloader) credentials.TransportCredentials {
	return &clientTransportCredentials{reloader: reloader}
}

func (c *clientTransportCredentials) Reloader() *CertReloader {
	return c.reloader
}

func (c *clientTransportCredentials) Close() error {
	return c.reloader.Close()
}

func (c *clientTransportCredentials) ClientHandshake(ctx context.Context, authority string, rawConn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	cfg := c.reloader.ClientTLSConfig()
	inner := credentials.NewTLS(cfg)
	conn, authInfo, err := inner.ClientHandshake(ctx, authority, rawConn)
	if err != nil {
		if isVerificationError(err) {
			if reloadErr := c.reloader.Reload(ctx); reloadErr != nil {
				scoped.Log(ctx).V(4).Info("reload after handshake verification error", "err", reloadErr)
			}
		}
		return nil, nil, err
	}
	return conn, authInfo, nil
}

func (c *clientTransportCredentials) ServerHandshake(rawConn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	cfg := c.reloader.ServerTLSConfig()
	inner := credentials.NewTLS(cfg)
	return inner.ServerHandshake(rawConn)
}

func (c *clientTransportCredentials) Info() credentials.ProtocolInfo {
	return credentials.ProtocolInfo{
		SecurityProtocol: "tls",
		SecurityVersion:  "1.3",
	}
}

func (c *clientTransportCredentials) Clone() credentials.TransportCredentials {
	return &clientTransportCredentials{reloader: c.reloader}
}

func (c *clientTransportCredentials) OverrideServerName(name string) error {
	return nil
}

func isVerificationError(err error) bool {
	if err == nil {
		return false
	}
	var cve *tls.CertificateVerificationError
	if errors.As(err, &cve) {
		return true
	}
	var uae x509.UnknownAuthorityError
	if errors.As(err, &uae) {
		return true
	}
	var cie x509.CertificateInvalidError
	if errors.As(err, &cie) {
		return true
	}
	var hne x509.HostnameError
	if errors.As(err, &hne) {
		return true
	}
	return false
}
