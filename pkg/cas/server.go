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
	"regexp"
	"strings"
	"sync"

	"golang.org/x/sys/unix"
	"k8s.io/klog/v2"
)

var shaRegex = regexp.MustCompile(`^[a-fA-F0-9]{64}$`)

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

// StartServer starts listening on the specified socketPath.
func StartServer(socketPath, storageDir string, downloader BlobDownloader) (*Server, error) {
	// Ensure the parent directory of socket exists
	if err := os.MkdirAll(filepath.Dir(socketPath), 0777); err != nil {
		return nil, fmt.Errorf("failed to create directory for socket: %v", err)
	}

	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on unix socket %s: %v", socketPath, err)
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

		sha := strings.ToLower(strings.TrimSpace(parts[1]))
		if !shaRegex.MatchString(sha) {
			s.sendError(unixConn, "invalid sha256 format")
			continue
		}

		s.handleGet(unixConn, sha)
	}
}

func (s *Server) handleGet(conn *net.UnixConn, sha string) {
	// Check/Create the node-wide central blobs cache directory
	blobsDir := filepath.Join(s.storageDir, "blobs")
	if err := os.MkdirAll(blobsDir, 0755); err != nil {
		s.sendError(conn, fmt.Sprintf("failed to create blobs directory: %v", err))
		return
	}

	blobPath := filepath.Join(blobsDir, sha)

	// Check if the file exists
	if _, err := os.Stat(blobPath); os.IsNotExist(err) {
		klog.Infof("CAS server: blob %s not found locally, downloading...", sha)
		ctx := context.Background() // Or we can use a context with timeout
		if err := s.downloader.DownloadBlob(ctx, sha, blobPath); err != nil {
			klog.Errorf("CAS server: failed to download blob %s: %v", sha, err)
			s.sendError(conn, fmt.Sprintf("failed to download blob: %v", err))
			return
		}
	}

	// Open the file
	file, err := os.Open(blobPath)
	if err != nil {
		klog.Errorf("CAS server: failed to open blob %s: %v", sha, err)
		s.sendError(conn, fmt.Sprintf("failed to open blob: %v", err))
		return
	}
	defer file.Close()

	fi, err := file.Stat()
	if err != nil {
		klog.Errorf("CAS server: failed to stat blob %s: %v", sha, err)
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
