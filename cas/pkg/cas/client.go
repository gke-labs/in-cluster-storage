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
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

// Client represents a client for the CAS UDS server.
//
// Why can't we use gRPC for SCM_RIGHTS?
// While gRPC is our standard protocol, standard gRPC runs over HTTP/2, which multiplexes
// multiple logical streams concurrently over a single TCP/Unix connection. In contrast,
// SCM_RIGHTS (file descriptor passing) is a lower-level kernel capability tied strictly
// to the physical boundaries of a specific Unix Domain Socket connection and socket message.
// Multiplexing multiple independent gRPC requests/responses over a single connection
// makes it impossible to safely associate a returned file descriptor with the correct gRPC stream.
// Therefore, we utilize a dedicated single-use message exchange on a dedicated Unix Domain Socket
// connection to ensure flawless and secure SCM_RIGHTS descriptor passing.
type Client struct {
	socketPath string
	conn       *net.UnixConn
}

// NewClient creates a new Client pointing to the given UDS socketPath,
// establishing and maintaining a persistent connection to the server.
func NewClient(socketPath string) (*Client, error) {
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to CAS socket %s: %v", socketPath, err)
	}

	unixConn, ok := conn.(*net.UnixConn)
	if !ok {
		conn.Close()
		return nil, fmt.Errorf("connection to %s is not a unix connection", socketPath)
	}

	return &Client{
		socketPath: socketPath,
		conn:       unixConn,
	}, nil
}

// Close closes the underlying persistent connection.
func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// RequestBlob requests the blob with the given SHA256 from the CAS server,
// and returns the open *os.File pointing to that blob (using SCM_RIGHTS) along with the file size.
func (c *Client) RequestBlob(sha string) (*os.File, int64, error) {
	req := fmt.Sprintf("GET %s\n", sha)
	if _, err := c.conn.Write([]byte(req)); err != nil {
		return nil, 0, fmt.Errorf("failed to send request: %v", err)
	}

	buf := make([]byte, 1024)
	oob := make([]byte, unix.CmsgSpace(4)) // space for 1 fd (4 bytes)
	n, oobn, _, _, err := c.conn.ReadMsgUnix(buf, oob)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to read response: %v", err)
	}

	resp := strings.TrimSpace(string(buf[:n]))
	if strings.HasPrefix(resp, "ERR") {
		return nil, 0, fmt.Errorf("server error: %s", strings.TrimPrefix(resp, "ERR "))
	}

	if !strings.HasPrefix(resp, "OK") {
		return nil, 0, fmt.Errorf("unexpected server response: %s", resp)
	}

	sizeStr := strings.TrimSpace(strings.TrimPrefix(resp, "OK"))
	size, err := strconv.ParseInt(sizeStr, 10, 64)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to parse size from response: %v", err)
	}

	if oobn == 0 {
		return nil, 0, fmt.Errorf("server did not send file descriptor")
	}

	scms, err := syscall.ParseSocketControlMessage(oob[:oobn])
	if err != nil {
		return nil, 0, fmt.Errorf("failed to parse socket control message: %v", err)
	}

	if len(scms) == 0 {
		return nil, 0, fmt.Errorf("no socket control messages received")
	}

	fds, err := syscall.ParseUnixRights(&scms[0])
	if err != nil {
		return nil, 0, fmt.Errorf("failed to parse unix rights: %v", err)
	}

	if len(fds) == 0 {
		return nil, 0, fmt.Errorf("no file descriptor received in unix rights")
	}

	file := os.NewFile(uintptr(fds[0]), "cas-blob")
	return file, size, nil
}
