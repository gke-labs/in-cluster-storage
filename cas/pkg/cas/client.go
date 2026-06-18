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
type Client struct {
	socketPath string
}

// NewClient creates a new Client pointing to the given UDS socketPath.
func NewClient(socketPath string) *Client {
	return &Client{socketPath: socketPath}
}

// RequestBlob requests the blob with the given SHA256 from the CAS server,
// and returns the open *os.File pointing to that blob (using SCM_RIGHTS) along with the file size.
func (c *Client) RequestBlob(sha string) (*os.File, int64, error) {
	conn, err := net.Dial("unix", c.socketPath)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to connect to CAS socket %s: %v", c.socketPath, err)
	}
	defer conn.Close()

	unixConn, ok := conn.(*net.UnixConn)
	if !ok {
		return nil, 0, fmt.Errorf("connection is not a unix connection")
	}

	req := fmt.Sprintf("GET %s\n", sha)
	if _, err := unixConn.Write([]byte(req)); err != nil {
		return nil, 0, fmt.Errorf("failed to send request: %v", err)
	}

	buf := make([]byte, 1024)
	oob := make([]byte, unix.CmsgSpace(4)) // space for 1 fd (4 bytes)
	n, oobn, _, _, err := unixConn.ReadMsgUnix(buf, oob)
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
