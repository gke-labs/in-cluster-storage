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
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"golang.org/x/sys/unix"
	"k8s.io/klog/v2"
)

// BlobDownloader interface abstracts the downloading of a blob from a remote/controller.
type BlobDownloader interface {
	DownloadBlob(ctx context.Context, sha string, destPath string) error
}

// Server implements the Unix Domain Socket CAS API.
type Server struct {
	socketPath string
	storageDir string
	downloader BlobDownloader

	listener net.Listener
	stopCh   chan struct{}
	wg       sync.WaitGroup

	connsMu sync.Mutex
	conns   map[net.Conn]struct{}
}

var chdirMu sync.Mutex

// StartServer starts listening on the specified socketPath.
func StartServer(socketPath, storageDir string, downloader BlobDownloader) (*Server, error) {
	// Ensure the parent directory of socket exists
	parentDir := filepath.Dir(socketPath)
	if err := os.MkdirAll(parentDir, 0777); err != nil {
		return nil, fmt.Errorf("failed to create directory for socket: %v", err)
	}

	chdirMu.Lock()
	defer chdirMu.Unlock()

	oldWD, err := os.Getwd()
	if err != nil {
		return nil, fmt.Errorf("failed to get current working directory: %v", err)
	}

	// Workaround for the 108-character sockaddr_un limit on Linux:
	// If the absolute path to the socket is longer than 108 characters (common in Kubelet mount paths),
	// net.Listen("unix", absolutePath) will fail with EINVAL (invalid argument).
	// To bypass this limit, we change the working directory (os.Chdir) to the socket's parent directory,
	// and call net.Listen on the relative filename. We protect this process-wide Chdir with a global mutex
	// and restore the working directory immediately afterward.
	if err := os.Chdir(parentDir); err != nil {
		return nil, fmt.Errorf("failed to change directory to %s: %v", parentDir, err)
	}

	// Listen on relative socket name to bypass the 108-character limit
	socketName := filepath.Base(socketPath)
	listener, err := net.Listen("unix", socketName)

	// Restore working directory immediately
	if chdirErr := os.Chdir(oldWD); chdirErr != nil {
		klog.Warningf("failed to restore working directory to %s: %v", oldWD, chdirErr)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to listen on unix socket %s (relative %s): %v", socketPath, socketName, err)
	}

	// Make the socket writeable for any user in the container
	if err := os.Chmod(socketPath, 0777); err != nil {
		klog.Warningf("failed to chmod socket %s: %v", socketPath, err)
	}

	srv := &Server{
		socketPath: socketPath,
		storageDir: storageDir,
		downloader: downloader,
		listener:   listener,
		stopCh:     make(chan struct{}),
		conns:      make(map[net.Conn]struct{}),
	}

	srv.wg.Add(1)
	go srv.serve()

	return srv, nil
}

func (s *Server) serve() {
	defer s.wg.Done()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.stopCh:
				return
			default:
				klog.Errorf("CAS server accept error: %v", err)
				return
			}
		}

		s.connsMu.Lock()
		s.conns[conn] = struct{}{}
		s.connsMu.Unlock()

		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer func() {
		conn.Close()
		s.connsMu.Lock()
		delete(s.conns, conn)
		s.connsMu.Unlock()
	}()

	unixConn, ok := conn.(*net.UnixConn)
	if !ok {
		klog.Errorf("CAS server: accepted connection is not a unix connection")
		return
	}

	reader := bufio.NewReader(unixConn)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if !errors.Is(err, io.EOF) {
				klog.Errorf("CAS server: read error: %v", err)
			}
			return
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		parts := strings.SplitN(line, " ", 2)
		if len(parts) != 2 || strings.ToUpper(parts[0]) != "GET" {
			s.sendError(unixConn, "invalid command: expected GET <sha256>")
			continue
		}

		shaStr := strings.TrimSpace(parts[1])
		blobID, err := ParseBlobID(shaStr)
		if err != nil {
			s.sendError(unixConn, fmt.Sprintf("invalid blob id: %v", err))
			continue
		}

		s.handleGet(unixConn, blobID)
	}
}

func (s *Server) handleGet(conn *net.UnixConn, blobID BlobID) {
	blobsDir := filepath.Join(s.storageDir, "blobs")
	if err := os.MkdirAll(blobsDir, 0755); err != nil {
		s.sendError(conn, fmt.Sprintf("failed to create blobs directory: %v", err))
		return
	}

	blobPath := filepath.Join(blobsDir, string(blobID))

	// Open the file first, which avoids TOCTOU (time-of-check to time-of-use) races.
	file, err := os.Open(blobPath)
	if err != nil {
		if os.IsNotExist(err) {
			klog.Infof("CAS server: blob %s not found locally, downloading...", blobID)
			ctx := context.Background()
			if dlErr := s.downloader.DownloadBlob(ctx, string(blobID), blobPath); dlErr != nil {
				klog.Errorf("CAS server: failed to download blob %s: %v", blobID, dlErr)
				s.sendError(conn, fmt.Sprintf("failed to download blob: %v", dlErr))
				return
			}
			// Try opening again after downloading
			file, err = os.Open(blobPath)
			if err != nil {
				klog.Errorf("CAS server: failed to open blob %s after download: %v", blobID, err)
				s.sendError(conn, fmt.Sprintf("failed to open blob after download: %v", err))
				return
			}
		} else {
			klog.Errorf("CAS server: failed to open blob %s: %v", blobID, err)
			s.sendError(conn, fmt.Sprintf("failed to open blob: %v", err))
			return
		}
	}
	defer file.Close()

	fi, err := file.Stat()
	if err != nil {
		klog.Errorf("CAS server: failed to stat blob %s: %v", blobID, err)
		s.sendError(conn, fmt.Sprintf("failed to stat blob: %v", err))
		return
	}

	// Send file descriptor using SCM_RIGHTS
	rights := unix.UnixRights(int(file.Fd()))
	resp := fmt.Sprintf("OK %d\n", fi.Size())
	_, _, err = conn.WriteMsgUnix([]byte(resp), rights, nil)
	if err != nil {
		klog.Errorf("CAS server: failed to send file descriptor: %v", err)
		return
	}
}

func (s *Server) sendError(conn *net.UnixConn, errMsg string) {
	resp := fmt.Sprintf("ERR %s\n", errMsg)
	_, _ = conn.Write([]byte(resp))
}

// Stop closes the listener, aborts active connections, and cleans up the socket file.
func (s *Server) Stop() {
	close(s.stopCh)
	s.listener.Close()

	s.connsMu.Lock()
	for conn := range s.conns {
		conn.Close()
	}
	s.connsMu.Unlock()

	s.wg.Wait()
	_ = os.Remove(s.socketPath)
}

// BlobID represents a validated, normalized (lowercase) 64-char hex SHA-256 blob identifier.
type BlobID string

// ParseBlobID validates s as a 64-char hex SHA-256 hash and returns it as a normalized BlobID.
func ParseBlobID(s string) (BlobID, error) {
	s = strings.TrimSpace(s)
	if len(s) != 64 {
		return "", fmt.Errorf("invalid blob ID length %d (expected 64 characters)", len(s))
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return "", fmt.Errorf("invalid character %q in blob ID", c)
		}
	}
	return BlobID(strings.ToLower(s)), nil
}
