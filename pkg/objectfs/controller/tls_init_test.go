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

package controller

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestGenerateTLS(t *testing.T) {
	certs, err := GenerateTLS("objectfs-controller", "kube-objectfs-system")
	if err != nil {
		t.Fatalf("GenerateTLS failed: %v", err)
	}

	// Verify CA
	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(certs.CACert) {
		t.Fatalf("failed to parse generated CA cert")
	}

	// Verify Leaf
	keyPair, err := tls.X509KeyPair(certs.LeafCert, certs.LeafKey)
	if err != nil {
		t.Fatalf("failed to parse leaf key pair: %v", err)
	}

	leaf, err := x509.ParseCertificate(keyPair.Certificate[0])
	if err != nil {
		t.Fatalf("failed to parse leaf x509: %v", err)
	}

	// Check validity duration ~1 year
	duration := leaf.NotAfter.Sub(leaf.NotBefore)
	if duration < 360*24*time.Hour || duration > 370*24*time.Hour {
		t.Errorf("expected ~1 year leaf duration, got %v", duration)
	}

	// Check SANs
	opts := x509.VerifyOptions{
		Roots:   caPool,
		DNSName: "objectfs-controller",
	}
	if _, err := leaf.Verify(opts); err != nil {
		t.Errorf("failed to verify leaf cert against objectfs-controller: %v", err)
	}

	opts.DNSName = "objectfs-controller.kube-objectfs-system.svc.cluster.local"
	if _, err := leaf.Verify(opts); err != nil {
		t.Errorf("failed to verify leaf cert against FQDN: %v", err)
	}
}

func TestEnsureTLSSecret(t *testing.T) {
	var createdSecret kubeSecret
	getCalls := 0
	postCalls := 0

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-controller-token" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		if r.Method == http.MethodGet {
			getCalls++
			if postCalls == 0 {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(createdSecret)
			return
		}

		if r.Method == http.MethodPost {
			postCalls++
			if err := json.NewDecoder(r.Body).Decode(&createdSecret); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusCreated)
			return
		}

		http.Error(w, "bad method", http.StatusMethodNotAllowed)
	}))
	defer ts.Close()

	tmpDir := t.TempDir()
	tokenPath := filepath.Join(tmpDir, "token")
	if err := os.WriteFile(tokenPath, []byte("test-controller-token"), 0600); err != nil {
		t.Fatalf("failed to write token: %v", err)
	}

	// 1. Initial call: secret does not exist -> creates it
	err := EnsureTLSSecret(t.Context(), ts.URL, tokenPath, "", "default", "objectfs-tls", "objectfs-controller")
	if err != nil {
		t.Fatalf("EnsureTLSSecret failed: %v", err)
	}
	if postCalls != 1 {
		t.Errorf("expected 1 POST call to create secret, got %d", postCalls)
	}
	if createdSecret.StringData["ca.crt"] == "" || createdSecret.StringData["tls.crt"] == "" || createdSecret.StringData["tls.key"] == "" {
		t.Errorf("created secret missing cert data: %v", createdSecret.StringData)
	}

	// 2. Second call: secret already exists -> skips generation
	err = EnsureTLSSecret(t.Context(), ts.URL, tokenPath, "", "default", "objectfs-tls", "objectfs-controller")
	if err != nil {
		t.Fatalf("second EnsureTLSSecret failed: %v", err)
	}
	if postCalls != 1 {
		t.Errorf("expected postCalls to still be 1 (skipped), got %d", postCalls)
	}
}
