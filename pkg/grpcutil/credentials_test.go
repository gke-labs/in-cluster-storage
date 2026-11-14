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
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	testpb "google.golang.org/grpc/interop/grpc_testing"
)

type testCerts struct {
	caCert     *x509.Certificate
	caKey      *rsa.PrivateKey
	caPEM      []byte
	serverCert *x509.Certificate
	serverKey  *rsa.PrivateKey
	serverPEM  []byte
	serverKeyM []byte
	clientCert *x509.Certificate
	clientKey  *rsa.PrivateKey
	clientPEM  []byte
	clientKeyM []byte
}

func generateTestCerts(t *testing.T) testCerts {
	t.Helper()

	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate CA key: %v", err)
	}

	caTemplate := &x509.Certificate{
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

	caDer, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("failed to create CA cert: %v", err)
	}
	caCert, _ := x509.ParseCertificate(caDer)
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDer})

	// Server cert
	serverKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate server key: %v", err)
	}
	serverTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			CommonName: "localhost",
		},
		DNSNames:    []string{"localhost"},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:   time.Now().Add(-1 * time.Hour),
		NotAfter:    time.Now().Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	serverDer, err := x509.CreateCertificate(rand.Reader, serverTemplate, caTemplate, &serverKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("failed to create server cert: %v", err)
	}
	serverCert, _ := x509.ParseCertificate(serverDer)
	serverPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverDer})
	serverKeyM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(serverKey)})

	// Client cert
	clientKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate client key: %v", err)
	}
	clientTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject: pkix.Name{
			CommonName: "test-client",
		},
		NotBefore:   time.Now().Add(-1 * time.Hour),
		NotAfter:    time.Now().Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	clientDer, err := x509.CreateCertificate(rand.Reader, clientTemplate, caTemplate, &clientKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("failed to create client cert: %v", err)
	}
	clientCert, _ := x509.ParseCertificate(clientDer)
	clientPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: clientDer})
	clientKeyM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(clientKey)})

	return testCerts{
		caCert:     caCert,
		caKey:      caKey,
		caPEM:      caPEM,
		serverCert: serverCert,
		serverKey:  serverKey,
		serverPEM:  serverPEM,
		serverKeyM: serverKeyM,
		clientCert: clientCert,
		clientKey:  clientKey,
		clientPEM:  clientPEM,
		clientKeyM: clientKeyM,
	}
}

func setupCertDir(t *testing.T, key, cert, ca []byte) string {
	t.Helper()
	dir := t.TempDir()

	if key != nil {
		if err := os.WriteFile(filepath.Join(dir, "tls.key"), key, 0600); err != nil {
			t.Fatalf("failed to write tls.key: %v", err)
		}
	}
	if cert != nil {
		if err := os.WriteFile(filepath.Join(dir, "tls.crt"), cert, 0644); err != nil {
			t.Fatalf("failed to write tls.crt: %v", err)
		}
	}
	if ca != nil {
		trustDir := filepath.Join(dir, "trust")
		if err := os.Mkdir(trustDir, 0755); err != nil {
			t.Fatalf("failed to create trust dir: %v", err)
		}
		if err := os.WriteFile(filepath.Join(trustDir, "ca.crt"), ca, 0644); err != nil {
			t.Fatalf("failed to write ca.crt: %v", err)
		}
	}
	return dir
}

func TestLoadTrustBundle(t *testing.T) {
	certs := generateTestCerts(t)
	dir := t.TempDir()
	trustDir := filepath.Join(dir, "trust")
	if err := os.Mkdir(trustDir, 0755); err != nil {
		t.Fatalf("failed to mkdir: %v", err)
	}

	// File 1 with CA cert
	if err := os.WriteFile(filepath.Join(trustDir, "ca1.crt"), certs.caPEM, 0644); err != nil {
		t.Fatalf("failed to write ca1.crt: %v", err)
	}
	// File 2 with duplicate of CA cert and extra bundle
	dupPEM := append(certs.caPEM, certs.serverPEM...)
	if err := os.WriteFile(filepath.Join(trustDir, "ca2.crt"), dupPEM, 0644); err != nil {
		t.Fatalf("failed to write ca2.crt: %v", err)
	}
	// Non-crt file ignored
	if err := os.WriteFile(filepath.Join(trustDir, "notes.txt"), []byte("ignore me"), 0644); err != nil {
		t.Fatalf("failed to write notes.txt: %v", err)
	}

	pool, err := LoadTrustBundle(trustDir)
	if err != nil {
		t.Fatalf("LoadTrustBundle failed: %v", err)
	}
	if pool == nil {
		t.Fatalf("expected non-nil cert pool")
	}

	// Missing directory
	if _, err := LoadTrustBundle(filepath.Join(dir, "nonexistent")); err == nil {
		t.Errorf("expected error for nonexistent trust directory")
	}

	// Empty directory
	emptyDir := t.TempDir()
	if _, err := LoadTrustBundle(emptyDir); err == nil {
		t.Errorf("expected error for empty trust directory")
	}
}

func TestServerAndClientTLSConfigFromDir(t *testing.T) {
	certs := generateTestCerts(t)

	// Missing dir argument
	if _, err := ServerTLSConfigFromDir(""); err == nil {
		t.Errorf("expected error for empty dir")
	}
	if _, err := ClientTLSConfigFromDir(""); err == nil {
		t.Errorf("expected error for empty dir")
	}

	// Missing tls.crt
	dirNoCert := setupCertDir(t, certs.serverKeyM, nil, certs.caPEM)
	if _, err := ServerTLSConfigFromDir(dirNoCert); err == nil || !strings.Contains(err.Error(), "tls.crt") {
		t.Errorf("expected error naming tls.crt, got %v", err)
	}

	// Missing tls.key
	dirNoKey := setupCertDir(t, nil, certs.serverPEM, certs.caPEM)
	if _, err := ServerTLSConfigFromDir(dirNoKey); err == nil || !strings.Contains(err.Error(), "tls.key") {
		t.Errorf("expected error naming tls.key, got %v", err)
	}

	// Missing trust directory
	dirNoTrust := setupCertDir(t, certs.serverKeyM, certs.serverPEM, nil)
	if _, err := ServerTLSConfigFromDir(dirNoTrust); err == nil || !strings.Contains(err.Error(), "trust") {
		t.Errorf("expected error naming trust, got %v", err)
	}

	// Valid directories
	serverDir := setupCertDir(t, certs.serverKeyM, certs.serverPEM, certs.caPEM)
	serverCfg, err := ServerTLSConfigFromDir(serverDir)
	if err != nil {
		t.Fatalf("ServerTLSConfigFromDir failed: %v", err)
	}
	if serverCfg.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Errorf("expected ClientAuth RequireAndVerifyClientCert, got %v", serverCfg.ClientAuth)
	}
	if len(serverCfg.Certificates) != 1 {
		t.Errorf("expected 1 server certificate, got %d", len(serverCfg.Certificates))
	}

	clientDir := setupCertDir(t, certs.clientKeyM, certs.clientPEM, certs.caPEM)
	clientCfg, err := ClientTLSConfigFromDir(clientDir)
	if err != nil {
		t.Fatalf("ClientTLSConfigFromDir failed: %v", err)
	}
	if clientCfg.RootCAs == nil {
		t.Errorf("expected RootCAs to be populated")
	}
	if len(clientCfg.Certificates) != 1 {
		t.Errorf("expected 1 client certificate, got %d", len(clientCfg.Certificates))
	}
}

type testServer struct {
	testpb.UnimplementedTestServiceServer
}

func (s *testServer) EmptyCall(ctx context.Context, in *testpb.Empty) (*testpb.Empty, error) {
	return &testpb.Empty{}, nil
}

func TestGRPCRoundTrip_MTLS(t *testing.T) {
	ctx := t.Context()
	certs := generateTestCerts(t)

	serverDir := setupCertDir(t, certs.serverKeyM, certs.serverPEM, certs.caPEM)
	clientDir := setupCertDir(t, certs.clientKeyM, certs.clientPEM, certs.caPEM)

	serverCreds, err := NewServerTLSFromDir(ctx, serverDir)
	if err != nil {
		t.Fatalf("NewServerTLSFromDir failed: %v", err)
	}
	defer func() {
		if cErr := CloseReloader(serverCreds); cErr != nil {
			t.Logf("CloseReloader: %v", cErr)
		}
	}()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()

	srv := grpc.NewServer(grpc.Creds(serverCreds))
	testpb.RegisterTestServiceServer(srv, &testServer{})
	go func() {
		_ = srv.Serve(lis)
	}()
	defer srv.Stop()

	// 1. Valid mTLS client succeeds
	clientCreds, err := NewClientTLSFromDir(ctx, clientDir)
	if err != nil {
		t.Fatalf("NewClientTLSFromDir failed: %v", err)
	}
	defer func() {
		if cErr := CloseReloader(clientCreds); cErr != nil {
			t.Logf("CloseReloader: %v", cErr)
		}
	}()

	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(clientCreds),
		DialOptions()[0],
	)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()

	client := testpb.NewTestServiceClient(conn)
	callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if _, err := client.EmptyCall(callCtx, &testpb.Empty{}); err != nil {
		t.Fatalf("mTLS call failed: %v", err)
	}

	// 2. Insecure plaintext client fails handshake
	insecureConn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to dial insecure: %v", err)
	}
	defer insecureConn.Close()

	insecureClient := testpb.NewTestServiceClient(insecureConn)
	insecureCtx, cancelInsecure := context.WithTimeout(t.Context(), 1*time.Second)
	defer cancelInsecure()

	if _, err := insecureClient.EmptyCall(insecureCtx, &testpb.Empty{}); err == nil {
		t.Fatalf("expected plaintext client to fail against mTLS server, but call succeeded")
	}
}
