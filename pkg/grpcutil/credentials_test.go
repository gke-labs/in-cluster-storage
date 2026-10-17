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
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestTokenCredentials_Static(t *testing.T) {
	creds := NewTokenCredentials("test-token-123")
	if !creds.RequireTransportSecurity() {
		t.Fatalf("expected RequireTransportSecurity() to be true")
	}

	meta, err := creds.GetRequestMetadata(t.Context())
	if err != nil {
		t.Fatalf("GetRequestMetadata failed: %v", err)
	}

	if meta["authorization"] != "Bearer test-token-123" {
		t.Errorf("expected Bearer test-token-123, got %q", meta["authorization"])
	}
}

func TestTokenCredentials_File(t *testing.T) {
	tmpDir := t.TempDir()
	tokenFile := filepath.Join(tmpDir, "token")

	if err := os.WriteFile(tokenFile, []byte("  initial-token  \n"), 0600); err != nil {
		t.Fatalf("failed to write token file: %v", err)
	}

	creds := NewTokenFileCredentials(tokenFile)

	meta, err := creds.GetRequestMetadata(t.Context())
	if err != nil {
		t.Fatalf("GetRequestMetadata failed: %v", err)
	}
	if meta["authorization"] != "Bearer initial-token" {
		t.Errorf("expected Bearer initial-token, got %q", meta["authorization"])
	}

	// Update token in file (e.g. rotation)
	if err := os.WriteFile(tokenFile, []byte("rotated-token-456\n"), 0600); err != nil {
		t.Fatalf("failed to rotate token file: %v", err)
	}

	meta, err = creds.GetRequestMetadata(t.Context())
	if err != nil {
		t.Fatalf("GetRequestMetadata failed after rotation: %v", err)
	}
	if meta["authorization"] != "Bearer rotated-token-456" {
		t.Errorf("expected Bearer rotated-token-456, got %q", meta["authorization"])
	}
}

func TestTokenCredentials_Empty(t *testing.T) {
	creds := NewTokenCredentials("")
	if _, err := creds.GetRequestMetadata(t.Context()); err == nil {
		t.Fatalf("expected error for empty token, got nil")
	}
}

func TestNewClientTLSFromFile(t *testing.T) {
	// Generate self-signed CA cert for testing
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "test-ca",
		},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("failed to create certificate: %v", err)
	}

	tmpDir := t.TempDir()
	caFile := filepath.Join(tmpDir, "ca.crt")
	caPem := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err := os.WriteFile(caFile, caPem, 0644); err != nil {
		t.Fatalf("failed to write CA file: %v", err)
	}

	creds, err := NewClientTLSFromFile(caFile)
	if err != nil {
		t.Fatalf("NewClientTLSFromFile failed: %v", err)
	}
	if creds.Info().SecurityProtocol != "tls" {
		t.Errorf("expected security protocol tls, got %s", creds.Info().SecurityProtocol)
	}

	// Test with system pool (empty string)
	sysCreds, err := NewClientTLSFromFile("")
	if err != nil {
		t.Fatalf("NewClientTLSFromFile with empty ca file failed: %v", err)
	}
	if sysCreds.Info().SecurityProtocol != "tls" {
		t.Errorf("expected security protocol tls, got %s", sysCreds.Info().SecurityProtocol)
	}
}
