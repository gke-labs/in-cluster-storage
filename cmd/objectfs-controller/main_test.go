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
	"crypto/x509"
	"flag"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/gke-labs/in-cluster-storage/pkg/sds"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/projection/table"
)

func TestParseEndpoint(t *testing.T) {
	tests := []struct {
		endpoint  string
		wantProto string
		wantAddr  string
		wantErr   bool
	}{
		{"unix:///tmp/csi.sock", "unix", "/tmp/csi.sock", false},
		{"tcp://127.0.0.1:50051", "tcp", "127.0.0.1:50051", false},
		{"http://127.0.0.1:50051", "", "", true},
		{"invalid", "", "", true},
	}

	for _, tt := range tests {
		proto, addr, err := parseEndpoint(tt.endpoint)
		if (err != nil) != tt.wantErr {
			t.Errorf("parseEndpoint(%q) error = %v, wantErr %v", tt.endpoint, err, tt.wantErr)
			continue
		}
		if !tt.wantErr {
			if proto != tt.wantProto || addr != tt.wantAddr {
				t.Errorf("parseEndpoint(%q) = (%q, %q), want (%q, %q)", tt.endpoint, proto, addr, tt.wantProto, tt.wantAddr)
			}
		}
	}
}

func TestMetadataIndexCacheMBFlag(t *testing.T) {
	f := flag.Lookup("metadata-index-cache-mb")
	if f == nil {
		t.Fatal("flag --metadata-index-cache-mb not registered")
	}
	if f.DefValue != "64" {
		t.Errorf("default value = %q, want 64", f.DefValue)
	}
}

func TestControllerTableRegistration(t *testing.T) {
	ctx := t.Context()
	cache := pebble.NewCache(64 * 1024 * 1024)
	defer cache.Unref()

	sds.RegisterIndexFactory(table.NewFactory(
		table.WithDisableWAL(true), // stream is the source of truth, position and rows move in one batch
		table.WithCache(cache),
	))

	factory, err := sds.GetIndexFactory("table")
	if err != nil {
		t.Fatalf("GetIndexFactory(table) failed: %v", err)
	}

	dir := t.TempDir()
	idx, err := factory.NewEmpty(ctx, "test-stream", dir)
	if err != nil {
		t.Fatalf("factory.NewEmpty failed: %v", err)
	}
	defer idx.Close()

	tableDB, ok := idx.(*table.DB)
	if !ok {
		t.Fatalf("expected *table.DB, got %T", idx)
	}
	if !tableDB.DisableWAL() {
		t.Errorf("tableDB.DisableWAL() = false, want true")
	}
}

func TestCheckLeafExpiry(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name      string
		notAfter  time.Time
		wantLevel expiryLevel
	}{
		{
			name:      "expired certificate",
			notAfter:  now.Add(-1 * time.Hour),
			wantLevel: expiryError,
		},
		{
			name:      "warning when within 30 days of expiry (10 days left)",
			notAfter:  now.Add(10 * 24 * time.Hour),
			wantLevel: expiryWarning,
		},
		{
			name:      "ok when well before 30-day window (200 days left)",
			notAfter:  now.Add(200 * 24 * time.Hour),
			wantLevel: expiryOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cert := &x509.Certificate{
				NotAfter: tt.notAfter,
			}
			got := evaluateLeafExpiry(cert.NotAfter, now)
			if got != tt.wantLevel {
				t.Errorf("evaluateLeafExpiry(%v) = %v, want %v", tt.notAfter, got, tt.wantLevel)
			}
			level := checkLeafExpiry(cert)
			if level != tt.wantLevel {
				t.Errorf("checkLeafExpiry(%v) = %v, want %v", tt.notAfter, level, tt.wantLevel)
			}
		})
	}
}
