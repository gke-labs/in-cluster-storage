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
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"google.golang.org/grpc/credentials"
)

// DirExists returns true if path exists and is a directory.
func DirExists(p string) bool {
	info, err := os.Stat(p)
	return err == nil && info.IsDir()
}

// FileExists returns true if path exists and is not a directory.
func FileExists(p string) bool {
	info, err := os.Stat(p)
	return err == nil && !info.IsDir()
}

// LoadTrustBundle loads all *.crt certificate files from trustDir,
// parses and deduplicates certificates, and returns an x509.CertPool.
// Returns an error naming trustDir if the directory is missing, cannot be read,
// or contains no valid PEM certificates.
func LoadTrustBundle(trustDir string) (*x509.CertPool, error) {
	entries, err := os.ReadDir(trustDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read trust directory %s: %w", trustDir, err)
	}

	pool := x509.NewCertPool()
	seen := make(map[[32]byte]bool)
	count := 0

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".crt") {
			continue
		}

		filePath := filepath.Join(trustDir, entry.Name())
		data, err := os.ReadFile(filePath)
		if err != nil {
			return nil, fmt.Errorf("failed to read trust certificate %s: %w", filePath, err)
		}

		rest := data
		for len(rest) > 0 {
			var block *pem.Block
			block, rest = pem.Decode(rest)
			if block == nil {
				break
			}
			if block.Type != "CERTIFICATE" {
				continue
			}

			cert, err := x509.ParseCertificate(block.Bytes)
			if err != nil {
				return nil, fmt.Errorf("failed to parse certificate in %s: %w", filePath, err)
			}

			hash := sha256.Sum256(cert.Raw)
			if !seen[hash] {
				seen[hash] = true
				pool.AddCert(cert)
				count++
			}
		}
	}

	if count == 0 {
		return nil, fmt.Errorf("no valid certificates found in trust directory %s", trustDir)
	}

	return pool, nil
}

// ServerTLSConfigFromDir constructs a *tls.Config for a server from a certificate directory.
// It loads the key pair from tls.crt and tls.key, loads the trust bundle from trust/*.crt,
// and enforces client certificate verification (RequireAndVerifyClientCert) with TLS 1.3+.
func ServerTLSConfigFromDir(dir string) (*tls.Config, error) {
	if dir == "" {
		return nil, fmt.Errorf("server certificate directory path must not be empty")
	}

	certFile := filepath.Join(dir, "tls.crt")
	keyFile := filepath.Join(dir, "tls.key")
	trustDir := filepath.Join(dir, "trust")

	if !FileExists(certFile) {
		return nil, fmt.Errorf("missing server certificate file %s", certFile)
	}
	if !FileExists(keyFile) {
		return nil, fmt.Errorf("missing server private key file %s", keyFile)
	}

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load server keypair from %s and %s: %w", certFile, keyFile, err)
	}

	trustPool, err := LoadTrustBundle(trustDir)
	if err != nil {
		return nil, fmt.Errorf("failed to load server trust bundle from %s: %w", trustDir, err)
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    trustPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS13,
	}, nil
}

// NewServerTLSFromDir creates gRPC server TransportCredentials requiring client certificates,
// backed by dynamic live reload of certificates and trust pools.
func NewServerTLSFromDir(ctx context.Context, dir string) (credentials.TransportCredentials, error) {
	reloader, err := NewCertReloader(ctx, dir)
	if err != nil {
		return nil, err
	}
	inner := credentials.NewTLS(reloader.ServerTLSConfig())
	return &serverTransportCredentials{
		TransportCredentials: inner,
		reloader:             reloader,
	}, nil
}

// ClientTLSConfigFromDir constructs a *tls.Config for a client from a certificate directory.
// It loads the key pair from tls.crt and tls.key, and loads root CAs from trust/*.crt with TLS 1.3+.
func ClientTLSConfigFromDir(dir string) (*tls.Config, error) {
	if dir == "" {
		return nil, fmt.Errorf("client certificate directory path must not be empty")
	}

	certFile := filepath.Join(dir, "tls.crt")
	keyFile := filepath.Join(dir, "tls.key")
	trustDir := filepath.Join(dir, "trust")

	if !FileExists(certFile) {
		return nil, fmt.Errorf("missing client certificate file %s", certFile)
	}
	if !FileExists(keyFile) {
		return nil, fmt.Errorf("missing client private key file %s", keyFile)
	}

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load client keypair from %s and %s: %w", certFile, keyFile, err)
	}

	trustPool, err := LoadTrustBundle(trustDir)
	if err != nil {
		return nil, fmt.Errorf("failed to load client trust bundle from %s: %w", trustDir, err)
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      trustPool,
		MinVersion:   tls.VersionTLS13,
	}, nil
}

// NewClientTLSFromDir creates gRPC client TransportCredentials presenting client certificates,
// backed by dynamic live reload of certificates and trust pools.
func NewClientTLSFromDir(ctx context.Context, dir string) (credentials.TransportCredentials, error) {
	reloader, err := NewCertReloader(ctx, dir)
	if err != nil {
		return nil, err
	}
	return NewClientTransportCredentials(reloader), nil
}
