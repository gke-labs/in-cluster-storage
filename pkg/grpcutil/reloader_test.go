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
	"errors"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	testpb "google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/peer"
)

type identityServer struct {
	testpb.UnimplementedTestServiceServer
	mu            sync.Mutex
	lastCallerCN  string
	lastCallerURI string
}

func (s *identityServer) EmptyCall(ctx context.Context, in *testpb.Empty) (*testpb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if p, ok := peer.FromContext(ctx); ok && p.AuthInfo != nil {
		if tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo); ok && len(tlsInfo.State.PeerCertificates) > 0 {
			cert := tlsInfo.State.PeerCertificates[0]
			s.lastCallerCN = cert.Subject.CommonName
			if len(cert.URIs) > 0 {
				s.lastCallerURI = cert.URIs[0].String()
			}
		}
	}
	return &testpb.Empty{}, nil
}

func (s *identityServer) LastCallerCN() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastCallerCN
}

type bundle struct {
	caPEM      []byte
	caKey      *rsa.PrivateKey
	caTemplate *x509.Certificate
	certPEM    []byte
	keyPEM     []byte
	certPool   *x509.CertPool
	serial     *big.Int
	cn         string
}

func generateCertBundle(t *testing.T, caCN, leafCN string, isServer bool, serial int64) bundle {
	t.Helper()

	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate CA key: %v", err)
	}

	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(serial * 10),
		Subject: pkix.Name{
			CommonName: caCN,
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
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDer})

	pool := x509.NewCertPool()
	parsedCA, err := x509.ParseCertificate(caDer)
	if err != nil {
		t.Fatalf("failed to parse CA cert: %v", err)
	}
	pool.AddCert(parsedCA)

	leafKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate leaf key: %v", err)
	}

	extUsages := []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	if isServer {
		extUsages = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	}

	leafTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject: pkix.Name{
			CommonName: leafCN,
		},
		DNSNames:    []string{leafCN, "localhost"},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:   time.Now().Add(-1 * time.Hour),
		NotAfter:    time.Now().Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: extUsages,
	}

	leafDer, err := x509.CreateCertificate(rand.Reader, leafTemplate, caTemplate, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("failed to create leaf cert: %v", err)
	}
	leafPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDer})
	leafKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(leafKey)})

	return bundle{
		caPEM:      caPEM,
		caKey:      caKey,
		caTemplate: caTemplate,
		certPEM:    leafPEM,
		keyPEM:     leafKeyPEM,
		certPool:   pool,
		serial:     big.NewInt(serial),
		cn:         leafCN,
	}
}

func generateLeafCert(t *testing.T, b bundle, leafCN string, isServer bool, serial int64) bundle {
	t.Helper()

	leafKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate leaf key: %v", err)
	}

	extUsages := []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	if isServer {
		extUsages = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	}

	leafTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject: pkix.Name{
			CommonName: leafCN,
		},
		DNSNames:    []string{leafCN, "localhost"},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:   time.Now().Add(-1 * time.Hour),
		NotAfter:    time.Now().Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: extUsages,
	}

	leafDer, err := x509.CreateCertificate(rand.Reader, leafTemplate, b.caTemplate, &leafKey.PublicKey, b.caKey)
	if err != nil {
		t.Fatalf("failed to create leaf cert: %v", err)
	}
	leafPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDer})
	leafKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(leafKey)})

	return bundle{
		caPEM:      b.caPEM,
		caKey:      b.caKey,
		caTemplate: b.caTemplate,
		certPEM:    leafPEM,
		keyPEM:     leafKeyPEM,
		certPool:   b.certPool,
		serial:     big.NewInt(serial),
		cn:         leafCN,
	}
}

func writeMaterial(t *testing.T, dir string, certPEM, keyPEM, caPEM []byte) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "tls.key"), keyPEM, 0600); err != nil {
		t.Fatalf("failed to write tls.key: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "tls.crt"), certPEM, 0644); err != nil {
		t.Fatalf("failed to write tls.crt: %v", err)
	}
	trustDir := filepath.Join(dir, "trust")
	if err := os.MkdirAll(trustDir, 0755); err != nil {
		t.Fatalf("failed to create trust dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(trustDir, "ca.crt"), caPEM, 0644); err != nil {
		t.Fatalf("failed to write ca.crt: %v", err)
	}
}

func TestServerCertReload_NoRestart(t *testing.T) {
	ctx := t.Context()
	// Setup material: initial server cert 1 and client cert 1
	b1 := generateCertBundle(t, "ca-1", "localhost", true, 101)
	bClient := generateCertBundle(t, "client-ca", "client-1", false, 201)

	serverDir := t.TempDir()
	writeMaterial(t, serverDir, b1.certPEM, b1.keyPEM, bClient.caPEM)

	clientDir := t.TempDir()
	writeMaterial(t, clientDir, bClient.certPEM, bClient.keyPEM, b1.caPEM)

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
	testpb.RegisterTestServiceServer(srv, &identityServer{})
	go func() {
		_ = srv.Serve(lis)
	}()
	defer srv.Stop()

	// 1. Initial client handshake receives ServerCert1
	clientCreds1, err := NewClientTLSFromDir(ctx, clientDir)
	if err != nil {
		t.Fatalf("NewClientTLSFromDir failed: %v", err)
	}
	defer func() {
		if cErr := CloseReloader(clientCreds1); cErr != nil {
			t.Logf("CloseReloader: %v", cErr)
		}
	}()

	conn1, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(clientCreds1),
		DialOptions()[0],
	)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn1.Close()

	client1 := testpb.NewTestServiceClient(conn1)
	var p1 peer.Peer
	callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if _, err := client1.EmptyCall(callCtx, &testpb.Empty{}, grpc.Peer(&p1)); err != nil {
		t.Fatalf("call with initial server cert failed: %v", err)
	}
	tlsInfo1 := p1.AuthInfo.(credentials.TLSInfo)
	if tlsInfo1.State.PeerCertificates[0].SerialNumber.Cmp(b1.serial) != 0 {
		t.Fatalf("expected server cert serial %v, got %v", b1.serial, tlsInfo1.State.PeerCertificates[0].SerialNumber)
	}

	// 2. Rotate server directory material to ServerCert2 (signed by CA2)
	b2 := generateCertBundle(t, "ca-2", "localhost", true, 102)

	// Update client trust directory with CA2 so new clients trust the new server cert
	trust2 := append(b1.caPEM, b2.caPEM...)
	if err := os.WriteFile(filepath.Join(clientDir, "trust", "ca.crt"), trust2, 0644); err != nil {
		t.Fatalf("failed to update client trust: %v", err)
	}

	// Swap server material without restarting server
	writeMaterial(t, serverDir, b2.certPEM, b2.keyPEM, bClient.caPEM)

	// Wait briefly for fsnotify or trigger reload
	time.Sleep(100 * time.Millisecond)

	// 3. New handshake without restarting server observes ServerCert2
	clientCreds2, err := NewClientTLSFromDir(ctx, clientDir)
	if err != nil {
		t.Fatalf("NewClientTLSFromDir failed: %v", err)
	}
	defer func() {
		if cErr := CloseReloader(clientCreds2); cErr != nil {
			t.Logf("CloseReloader: %v", cErr)
		}
	}()

	conn2, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(clientCreds2),
		DialOptions()[0],
	)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn2.Close()

	client2 := testpb.NewTestServiceClient(conn2)
	var p2 peer.Peer
	ctx2, cancel2 := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel2()

	if _, err := client2.EmptyCall(ctx2, &testpb.Empty{}, grpc.Peer(&p2)); err != nil {
		t.Fatalf("call after server rotation failed: %v", err)
	}
	tlsInfo2 := p2.AuthInfo.(credentials.TLSInfo)
	if tlsInfo2.State.PeerCertificates[0].SerialNumber.Cmp(b2.serial) != 0 {
		t.Fatalf("expected rotated server cert serial %v, got %v", b2.serial, tlsInfo2.State.PeerCertificates[0].SerialNumber)
	}
}

func TestClientCertReload_NoRestart(t *testing.T) {
	ctx := t.Context()
	// Setup material: server cert and two client certs signed by same client CA
	bServer := generateCertBundle(t, "server-ca", "localhost", true, 100)
	bClient1 := generateCertBundle(t, "client-ca", "client-identity-1", false, 301)
	bClient2 := generateLeafCert(t, bClient1, "client-identity-2", false, 302)

	serverDir := t.TempDir()
	writeMaterial(t, serverDir, bServer.certPEM, bServer.keyPEM, bClient1.caPEM)

	clientDir := t.TempDir()
	writeMaterial(t, clientDir, bClient1.certPEM, bClient1.keyPEM, bServer.caPEM)

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

	idServer := &identityServer{}
	srv := grpc.NewServer(grpc.Creds(serverCreds))
	testpb.RegisterTestServiceServer(srv, idServer)
	go func() {
		_ = srv.Serve(lis)
	}()
	defer srv.Stop()

	clientCreds, err := NewClientTLSFromDir(ctx, clientDir)
	if err != nil {
		t.Fatalf("NewClientTLSFromDir failed: %v", err)
	}
	defer func() {
		if cErr := CloseReloader(clientCreds); cErr != nil {
			t.Logf("CloseReloader: %v", cErr)
		}
	}()

	conn1, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(clientCreds),
		DialOptions()[0],
	)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn1.Close()

	client := testpb.NewTestServiceClient(conn1)
	ctx1, cancel1 := context.WithTimeout(ctx, 5*time.Second)
	defer cancel1()

	if _, err := client.EmptyCall(ctx1, &testpb.Empty{}); err != nil {
		t.Fatalf("call with initial client cert failed: %v", err)
	}
	if idServer.LastCallerCN() != "client-identity-1" {
		t.Fatalf("expected caller CN 'client-identity-1', got '%s'", idServer.LastCallerCN())
	}

	// Swap client certificate files to client-identity-2 without restarting client or server
	writeMaterial(t, clientDir, bClient2.certPEM, bClient2.keyPEM, bServer.caPEM)
	time.Sleep(100 * time.Millisecond)

	// Next handshake uses client-identity-2
	conn2, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(clientCreds),
		DialOptions()[0],
	)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn2.Close()

	clientNew := testpb.NewTestServiceClient(conn2)
	ctx2, cancel2 := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel2()

	if _, err := clientNew.EmptyCall(ctx2, &testpb.Empty{}); err != nil {
		t.Fatalf("call after client cert rotation failed: %v", err)
	}
	if idServer.LastCallerCN() != "client-identity-2" {
		t.Fatalf("expected caller CN 'client-identity-2', got '%s'", idServer.LastCallerCN())
	}
}

func TestClientCAReload_OnVerificationFailure(t *testing.T) {
	ctx := t.Context()
	bServer1 := generateCertBundle(t, "server-ca-1", "localhost", true, 501)
	bServer2 := generateCertBundle(t, "server-ca-2", "localhost", true, 502)
	bClient := generateCertBundle(t, "client-ca", "test-client", false, 601)

	serverDir := t.TempDir()
	writeMaterial(t, serverDir, bServer1.certPEM, bServer1.keyPEM, bClient.caPEM)

	clientDir := t.TempDir()
	// Client initially only trusts server-ca-1
	writeMaterial(t, clientDir, bClient.certPEM, bClient.keyPEM, bServer1.caPEM)

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
	testpb.RegisterTestServiceServer(srv, &identityServer{})
	go func() {
		_ = srv.Serve(lis)
	}()
	defer srv.Stop()

	clientCreds, err := NewClientTLSFromDir(ctx, clientDir)
	if err != nil {
		t.Fatalf("NewClientTLSFromDir failed: %v", err)
	}
	defer func() {
		if cErr := CloseReloader(clientCreds); cErr != nil {
			t.Logf("CloseReloader: %v", cErr)
		}
	}()

	// Rotate server to Server2 (signed by CA2)
	writeMaterial(t, serverDir, bServer2.certPEM, bServer2.keyPEM, bClient.caPEM)
	time.Sleep(100 * time.Millisecond)

	// Update client trust directory with CA2 file
	if err := os.WriteFile(filepath.Join(clientDir, "trust", "ca2.crt"), bServer2.caPEM, 0644); err != nil {
		t.Fatalf("failed to write ca2.crt: %v", err)
	}

	// Dialing server triggers handshake with Server2; client detects verification failure
	// against old CA pool and forces an immediate reload of trust/, succeeding on connection retry.
	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(clientCreds),
		DialOptions(grpc.WithDefaultCallOptions(grpc.WaitForReady(true)))[0],
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
	)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()

	client := testpb.NewTestServiceClient(conn)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	if _, err := client.EmptyCall(ctx, &testpb.Empty{}); err != nil {
		t.Fatalf("expected client with WaitForReady to reload CA and succeed on retry, but got: %v", err)
	}
}

func TestIsVerificationError(t *testing.T) {
	if !isVerificationError(&tls.CertificateVerificationError{}) {
		t.Errorf("expected CertificateVerificationError to be recognized as verification error")
	}
	if !isVerificationError(x509.UnknownAuthorityError{}) {
		t.Errorf("expected UnknownAuthorityError to be recognized as verification error")
	}
	if !isVerificationError(x509.CertificateInvalidError{}) {
		t.Errorf("expected CertificateInvalidError to be recognized as verification error")
	}
	if !isVerificationError(x509.HostnameError{}) {
		t.Errorf("expected HostnameError to be recognized as verification error")
	}
	if isVerificationError(nil) {
		t.Errorf("expected nil to not be recognized as verification error")
	}
	if isVerificationError(errors.New("io: EOF")) {
		t.Errorf("expected EOF to not be recognized as verification error")
	}
}

func TestConcurrentReloadAndHandshake(t *testing.T) {
	ctx := t.Context()
	bServerCA := generateCertBundle(t, "server-ca", "localhost", true, 901)
	bServer2 := generateLeafCert(t, bServerCA, "localhost", true, 902)
	bClient := generateCertBundle(t, "client-ca", "client", false, 903)

	serverDir := t.TempDir()
	writeMaterial(t, serverDir, bServerCA.certPEM, bServerCA.keyPEM, bClient.caPEM)

	clientDir := t.TempDir()
	writeMaterial(t, clientDir, bClient.certPEM, bClient.keyPEM, bServerCA.caPEM)

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
	testpb.RegisterTestServiceServer(srv, &identityServer{})
	go func() {
		_ = srv.Serve(lis)
	}()
	defer srv.Stop()

	clientCreds, err := NewClientTLSFromDir(ctx, clientDir)
	if err != nil {
		t.Fatalf("NewClientTLSFromDir failed: %v", err)
	}
	defer func() {
		if cErr := CloseReloader(clientCreds); cErr != nil {
			t.Logf("CloseReloader: %v", cErr)
		}
	}()

	stopReload := make(chan struct{})
	reloadDone := make(chan struct{})
	reloader := serverCreds.(interface{ Reloader() *CertReloader }).Reloader()

	// Concurrently reload server certificate in a tight loop while handshakes run
	go func() {
		defer close(reloadDone)
		toggle := false
		for {
			select {
			case <-stopReload:
				return
			default:
				if toggle {
					writeMaterial(t, serverDir, bServerCA.certPEM, bServerCA.keyPEM, bClient.caPEM)
				} else {
					writeMaterial(t, serverDir, bServer2.certPEM, bServer2.keyPEM, bClient.caPEM)
				}
				toggle = !toggle
				if rErr := reloader.Reload(ctx); rErr != nil {
					t.Logf("reloader.Reload: %v", rErr)
				}
			}
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				conn, dialErr := grpc.NewClient(lis.Addr().String(),
					grpc.WithTransportCredentials(clientCreds),
					DialOptions()[0],
				)
				if dialErr != nil {
					t.Errorf("failed to dial: %v", dialErr)
					return
				}
				client := testpb.NewTestServiceClient(conn)
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				if _, callErr := client.EmptyCall(ctx, &testpb.Empty{}); callErr != nil {
					t.Errorf("handshake call failed: %v", callErr)
				}
				cancel()
				_ = conn.Close()
			}
		}()
	}

	wg.Wait()
	close(stopReload)
	<-reloadDone
}

func TestReloader_FailedReloadKeepsLastGoodMaterial(t *testing.T) {
	ctx := t.Context()
	b := generateCertBundle(t, "ca-1", "localhost", true, 701)
	dir := t.TempDir()
	writeMaterial(t, dir, b.certPEM, b.keyPEM, b.caPEM)

	reloader, err := NewCertReloader(ctx, dir)
	if err != nil {
		t.Fatalf("NewCertReloader failed: %v", err)
	}
	defer func() {
		if cErr := reloader.Close(); cErr != nil {
			t.Logf("reloader close: %v", cErr)
		}
	}()

	origCert, err := reloader.GetCertificate(nil)
	if err != nil {
		t.Fatalf("GetCertificate failed: %v", err)
	}
	if len(origCert.Certificate) == 0 {
		t.Fatalf("expected valid certificate")
	}

	// Corrupt private key with invalid PEM
	if err := os.WriteFile(filepath.Join(dir, "tls.key"), []byte("invalid-key-data"), 0600); err != nil {
		t.Fatalf("failed to corrupt key: %v", err)
	}

	// Force reload; should return error and retain last good material
	if err := reloader.Reload(ctx); err == nil {
		t.Fatalf("expected Reload to fail for corrupted key, but succeeded")
	}

	retainedCert, err := reloader.GetCertificate(nil)
	if err != nil {
		t.Fatalf("GetCertificate failed after reload failure: %v", err)
	}
	if len(retainedCert.Certificate) == 0 || string(retainedCert.Certificate[0]) != string(origCert.Certificate[0]) {
		t.Fatalf("reloader did not retain last good certificate after failed reload")
	}

	// Restore valid key and verify successful reload
	if err := os.WriteFile(filepath.Join(dir, "tls.key"), b.keyPEM, 0600); err != nil {
		t.Fatalf("failed to restore key: %v", err)
	}
	if err := reloader.Reload(ctx); err != nil {
		t.Fatalf("expected reload to succeed after key restore, got: %v", err)
	}
}

func TestReloader_DeduplicateTrustBundle(t *testing.T) {
	b := generateCertBundle(t, "ca-1", "localhost", true, 801)
	dir := t.TempDir()
	writeMaterial(t, dir, b.certPEM, b.keyPEM, b.caPEM)

	// Add multiple duplicate CA files
	trustDir := filepath.Join(dir, "trust")
	if err := os.WriteFile(filepath.Join(trustDir, "ca_dup.crt"), b.caPEM, 0644); err != nil {
		t.Fatalf("failed to write duplicate CA: %v", err)
	}

	pool, err := LoadTrustBundle(trustDir)
	if err != nil {
		t.Fatalf("LoadTrustBundle failed: %v", err)
	}
	if pool == nil {
		t.Fatalf("expected non-nil cert pool")
	}
}
