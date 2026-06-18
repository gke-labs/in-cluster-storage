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

package cas

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
)

type mockDownloader struct {
	content map[string]string
}

func (m *mockDownloader) DownloadBlob(ctx context.Context, sha string, destPath string) error {
	data, exists := m.content[sha]
	if !exists {
		return os.ErrNotExist
	}
	return os.WriteFile(destPath, []byte(data), 0644)
}

func TestCASServerAndClient(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "cas-test-")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	socketPath := filepath.Join(tempDir, "cas.sock")
	storageDir := filepath.Join(tempDir, "storage")

	testSHA := "a94a8fe5ccb19ba61c4c0873d391e987982fbbd3" // valid 40-char hex but regex expects 64-char
	_ = testSHA
	// Let's use a valid 64-char hex SHA256 string
	validSHA := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	blobContent := "Hello, this is content addressable storage!"

	downloader := &mockDownloader{
		content: map[string]string{
			validSHA: blobContent,
		},
	}

	srv, err := StartServer(socketPath, storageDir, downloader)
	if err != nil {
		t.Fatalf("failed to start CAS server: %v", err)
	}
	defer srv.Stop()

	client := NewClient(socketPath)

	// 1. Request existing blob
	f, size, err := client.RequestBlob(validSHA)
	if err != nil {
		t.Fatalf("failed to request valid blob: %v", err)
	}
	defer f.Close()

	if size != int64(len(blobContent)) {
		t.Errorf("expected size %d, got %d", len(blobContent), size)
	}

	contentBytes, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("failed to read from returned file descriptor: %v", err)
	}

	if string(contentBytes) != blobContent {
		t.Errorf("expected content %q, got %q", blobContent, string(contentBytes))
	}

	// 2. Request missing blob
	missingSHA := "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
	_, _, err = client.RequestBlob(missingSHA)
	if err == nil {
		t.Errorf("expected error for missing blob, but got none")
	} else if !os.IsNotExist(err) && !filepath.IsLocal(missingSHA) {
		t.Logf("got expected error for missing blob: %v", err)
	}

	// 3. Request invalid SHA format
	invalidSHA := "shortsha"
	_, _, err = client.RequestBlob(invalidSHA)
	if err == nil {
		t.Errorf("expected error for invalid sha, but got none")
	} else {
		t.Logf("got expected error for invalid sha: %v", err)
	}
}
