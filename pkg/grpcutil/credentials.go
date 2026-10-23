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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"

	"google.golang.org/grpc/credentials"
)

// TokenCredentials implements credentials.PerRPCCredentials.
// It injects a Bearer authorization token into per-RPC metadata.
type TokenCredentials struct {
	token     string
	tokenFile string
}

// NewTokenCredentials returns a PerRPCCredentials sending a static bearer token.
func NewTokenCredentials(token string) *TokenCredentials {
	return &TokenCredentials{
		token: strings.TrimSpace(token),
	}
}

// NewTokenFileCredentials returns a PerRPCCredentials that dynamically reads the
// token from the specified file on each RPC (supporting projected token rotation).
func NewTokenFileCredentials(tokenFile string) *TokenCredentials {
	return &TokenCredentials{
		tokenFile: tokenFile,
	}
}

// GetRequestMetadata returns the authorization metadata for the RPC.
func (t *TokenCredentials) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	tok := t.token
	if t.tokenFile != "" {
		b, err := os.ReadFile(t.tokenFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read token file %s: %w", t.tokenFile, err)
		}
		tok = strings.TrimSpace(string(b))
	}
	if tok == "" {
		return nil, fmt.Errorf("authorization token is empty")
	}
	return map[string]string{
		"authorization": "Bearer " + tok,
	}, nil
}

// RequireTransportSecurity returns whether TLS is required.
// For security, bearer tokens must only be transmitted over secure channels.
func (t *TokenCredentials) RequireTransportSecurity() bool {
	return true
}

// NewClientTLSFromFile creates transport credentials from a CA certificate file.
// If caCertFile is empty, it uses the host's system certificate pool.
func NewClientTLSFromFile(caCertFile string) (credentials.TransportCredentials, error) {
	if caCertFile == "" {
		return credentials.NewTLS(&tls.Config{}), nil
	}
	caCert, err := os.ReadFile(caCertFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read CA certificate from %s: %w", caCertFile, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("failed to parse CA certificate from %s", caCertFile)
	}
	return credentials.NewTLS(&tls.Config{RootCAs: pool}), nil
}

// FileExists returns true if path exists and is not a directory.
func FileExists(p string) bool {
	info, err := os.Stat(p)
	return err == nil && !info.IsDir()
}
