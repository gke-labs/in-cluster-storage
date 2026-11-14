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
	"io"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/gke-labs/in-cluster-storage/pkg/sds"
	"github.com/gke-labs/in-cluster-storage/pkg/sds/projection/table"
	walclient "github.com/gke-labs/in-cluster-storage/pkg/wal/client"
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

func TestMetadataIndexFlag(t *testing.T) {
	options := NewOptions()
	flagSet := flag.NewFlagSet("test", flag.ContinueOnError)
	flagSet.SetOutput(io.Discard)
	options.AddFlags(flagSet)
	flagOption := flagSet.Lookup("metadata-index")
	if flagOption == nil {
		t.Fatal("flag --metadata-index not registered")
	}
	if flagOption.DefValue != "sqlite" {
		t.Errorf("default value = %q, want sqlite", flagOption.DefValue)
	}
	for _, accepted := range []string{"table", "sqlite"} {
		if err := flagOption.Value.Set(accepted); err != nil || options.MetadataIndex != accepted {
			t.Fatalf("Set(%q) error = %v, got %q", accepted, err, options.MetadataIndex)
		}
	}
	err := flagOption.Value.Set("memory")
	if err == nil || !strings.Contains(err.Error(), "sqlite") || !strings.Contains(err.Error(), "table") {
		t.Errorf("Set(memory) error = %v, want error naming accepted values sqlite and table", err)
	}
	if err := flagSet.Parse([]string{"--metadata-index=memory"}); err == nil {
		t.Fatal("expected flagSet.Parse to fail for --metadata-index=memory")
	}
}

func TestMetadataIndexCacheMBFlag(t *testing.T) {
	options := NewOptions()
	flagSet := flag.NewFlagSet("test", flag.ContinueOnError)
	options.AddFlags(flagSet)
	f := flagSet.Lookup("metadata-index-cache-mb")
	if f == nil {
		t.Fatal("flag --metadata-index-cache-mb not registered")
	}
	if f.DefValue != "64" {
		t.Errorf("default value = %q, want 64", f.DefValue)
	}
}

func TestWalDurabilityFlag(t *testing.T) {
	options := NewOptions()
	flagSet := flag.NewFlagSet("test", flag.ContinueOnError)
	options.AddFlags(flagSet)
	f := flagSet.Lookup("wal-durability")
	if f == nil {
		t.Fatal("flag --wal-durability not registered")
	}
	if f.DefValue != "local" {
		t.Errorf("default value = %q, want local", f.DefValue)
	}
	if err := f.Value.Set("witness"); err != nil {
		t.Fatalf("Set witness failed: %v", err)
	}
	if options.WALDurability != walclient.Witness {
		t.Errorf("options.WALDurability = %v, want %v", options.WALDurability, walclient.Witness)
	}
	if err := f.Value.Set("permanent"); err != nil {
		t.Fatalf("Set permanent failed: %v", err)
	}
	if options.WALDurability != walclient.Permanent {
		t.Errorf("options.WALDurability = %v, want %v", options.WALDurability, walclient.Permanent)
	}
	if err := f.Value.Set("invalid"); err == nil {
		t.Fatal("expected error for invalid durability")
	}
}

func TestOpenBarrierTimeoutFlag(t *testing.T) {
	options := NewOptions()
	flagSet := flag.NewFlagSet("test", flag.ContinueOnError)
	options.AddFlags(flagSet)
	f := flagSet.Lookup("open-barrier-timeout")
	if f == nil {
		t.Fatal("flag --open-barrier-timeout not registered")
	}
	if f.DefValue != "30s" {
		t.Errorf("default value = %q, want 30s", f.DefValue)
	}
	if options.OpenBarrierTimeout != 30*time.Second {
		t.Errorf("options.OpenBarrierTimeout = %v, want 30s", options.OpenBarrierTimeout)
	}
}

func TestShutdownTimeoutFlag(t *testing.T) {
	options := NewOptions()
	flagSet := flag.NewFlagSet("test", flag.ContinueOnError)
	options.AddFlags(flagSet)
	shutdownFlag := flagSet.Lookup("shutdown-timeout")
	if shutdownFlag == nil {
		t.Fatal("flag --shutdown-timeout not registered")
	}
	if shutdownFlag.DefValue != "20s" {
		t.Errorf("default value = %q, want 20s", shutdownFlag.DefValue)
	}
	if options.ShutdownTimeout != 20*time.Second {
		t.Errorf("options.ShutdownTimeout = %v, want 20s", options.ShutdownTimeout)
	}
}

func TestOptionsParseAndValidate(t *testing.T) {
	options := NewOptions()
	flagSet := flag.NewFlagSet("test", flag.ContinueOnError)
	options.AddFlags(flagSet)

	arguments := []string{
		"--backend=memory://",
		"--metadata-dir=/tmp/metadata",
		"--wal-dir=/tmp/wal",
		"--wal-target=127.0.0.1:50051",
		"--port=8080",
		"--wal-durability=permanent",
		"--csi-endpoint=unix:///tmp/csi.sock",
	}
	if err := flagSet.Parse(arguments); err != nil {
		t.Fatalf("flagSet.Parse failed: %v", err)
	}

	if options.Backend != "memory://" || options.MetadataDir != "/tmp/metadata" ||
		options.WALDir != "/tmp/wal" || options.WALTarget != "127.0.0.1:50051" ||
		options.Port != 8080 || options.WALDurability != walclient.Permanent ||
		options.CSIEndpoint != "unix:///tmp/csi.sock" {
		t.Fatalf("parsed options mismatch: %+v", options)
	}
	if err := options.Validate(); err != nil {
		t.Fatalf("options.Validate() unexpected error: %v", err)
	}

	tests := []struct {
		name    string
		mutate  func(o *Options)
		wantErr string
	}{
		{"missing backend", func(o *Options) { o.Backend = "" }, "--backend"},
		{"missing metadata dir", func(o *Options) { o.MetadataDir = "" }, "--metadata-dir"},
		{"missing wal dir", func(o *Options) { o.WALDir = "" }, "--wal-dir"},
		{"missing wal target", func(o *Options) { o.WALTarget = "" }, "--wal-target"},
		{"invalid metadata index", func(o *Options) { o.MetadataIndex = "memory" }, "accepted values"},
		{"invalid csi endpoint", func(o *Options) { o.CSIEndpoint = "http://invalid" }, "invalid endpoint"},
		{"non-positive shutdown timeout", func(o *Options) { o.ShutdownTimeout = 0 }, "--shutdown-timeout"},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			validOptions := *options
			testCase.mutate(&validOptions)
			err := validOptions.Validate()
			if err == nil || !strings.Contains(err.Error(), testCase.wantErr) {
				t.Errorf("Validate() = %v, want error containing %q", err, testCase.wantErr)
			}
		})
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
	ctx := t.Context()
	now := time.Now()

	t.Run("expired certificate", func(t *testing.T) {
		cert := &x509.Certificate{
			NotAfter: now.Add(-1 * time.Hour),
		}
		evaluateLeafExpiry(ctx, cert, now)
	})

	t.Run("warning when within 30 days of expiry (10 days left)", func(t *testing.T) {
		cert := &x509.Certificate{
			NotAfter: now.Add(10 * 24 * time.Hour),
		}
		evaluateLeafExpiry(ctx, cert, now)
	})

	t.Run("ok when well before 30-day window (200 days left)", func(t *testing.T) {
		cert := &x509.Certificate{
			NotAfter: now.Add(200 * 24 * time.Hour),
		}
		evaluateLeafExpiry(ctx, cert, now)
	})
}
