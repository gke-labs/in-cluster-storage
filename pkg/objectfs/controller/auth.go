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
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gke-labs/in-cluster-storage/pkg/errutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
)

// Role defines the authorization role granted to an authenticated client.
type Role string

const (
	// RoleNone indicates an authenticated user with no granted privileges.
	RoleNone Role = "none"
	// RoleNode indicates the node daemon role (filesystem and volume operations).
	RoleNode Role = "node-daemon"
	// RoleAdmin indicates the administrator role (all operations, including snapshots and checkpoints).
	RoleAdmin Role = "admin"
)

// DefaultAudience is the default audience expected in projected ServiceAccount tokens.
const DefaultAudience = "objectfs-controller"

// nodeAllowedMethods is the set of gRPC methods permitted for the node-daemon role.
var nodeAllowedMethods = map[string]bool{
	"/objectfs.v1alpha1.ObjectFSController/GetAttr":        true,
	"/objectfs.v1alpha1.ObjectFSController/SetAttr":        true,
	"/objectfs.v1alpha1.ObjectFSController/Lookup":         true,
	"/objectfs.v1alpha1.ObjectFSController/ReadDir":        true,
	"/objectfs.v1alpha1.ObjectFSController/Mkdir":          true,
	"/objectfs.v1alpha1.ObjectFSController/CreateFile":     true,
	"/objectfs.v1alpha1.ObjectFSController/ReadFile":       true,
	"/objectfs.v1alpha1.ObjectFSController/WriteFile":      true,
	"/objectfs.v1alpha1.ObjectFSController/TruncateFile":   true,
	"/objectfs.v1alpha1.ObjectFSController/PunchHole":      true,
	"/objectfs.v1alpha1.ObjectFSController/Unlink":         true,
	"/objectfs.v1alpha1.ObjectFSController/Rmdir":          true,
	"/objectfs.v1alpha1.ObjectFSController/Rename":         true,
	"/objectfs.v1alpha1.ObjectFSController/Fsync":          true,
	"/objectfs.v1alpha1.ObjectFSController/WatchVolume":    true,
	"/objectfs.v1alpha1.ObjectFSController/Symlink":        true,
	"/objectfs.v1alpha1.ObjectFSController/Readlink":       true,
	"/objectfs.v1alpha1.ObjectFSController/Link":           true,
	"/objectfs.v1alpha1.ObjectFSController/Open":           true,
	"/objectfs.v1alpha1.ObjectFSController/Release":        true,
	"/objectfs.v1alpha1.ObjectFSController/GetManifest":    true,
	"/objectfs.v1alpha1.ObjectFSController/GetVolumeStats": true,
	"/objectfs.v1alpha1.ObjectFSController/AckCheckpoint":  true,
}

// TokenReviewResult contains the parsed result of a TokenReview API call.
type TokenReviewResult struct {
	Authenticated bool
	Username      string
	UID           string
	Groups        []string
	Audiences     []string
	Error         string
}

// TokenReviewer verifies a ServiceAccount token against an identity provider (Kubernetes).
type TokenReviewer interface {
	ReviewToken(ctx context.Context, token string, audiences []string) (*TokenReviewResult, error)
}

// AuthConfig defines the authentication and authorization policy for ObjectFSController.
type AuthConfig struct {
	Namespace            string
	Audiences            []string
	AdminServiceAccounts []string
	NodeServiceAccounts  []string
	Insecure             bool
}

// DefaultAuthConfig returns a standard AuthConfig for in-cluster deployment.
func DefaultAuthConfig() AuthConfig {
	return AuthConfig{
		Namespace:            "kube-objectfs-system",
		Audiences:            []string{DefaultAudience},
		AdminServiceAccounts: []string{"objectfs-admin", "objectfs-controller"},
		NodeServiceAccounts:  []string{"objectfs-node-daemon"},
		Insecure:             false,
	}
}

func matchesServiceAccount(username string, candidates []string, expectedNamespace string) bool {
	for _, c := range candidates {
		c = strings.TrimSpace(c)
		if c == "" {
			continue
		}
		// 1. If candidate is already a fully qualified username, match exactly.
		if strings.Contains(c, ":") {
			if username == c {
				return true
			}
			continue
		}
		// 2. Candidate is a bare service account name (e.g. "objectfs-admin" or "objectfs-node-daemon").
		// Restrict match strictly to expectedNamespace: system:serviceaccount:<expectedNamespace>:<name>
		if expectedNamespace != "" {
			qualified := fmt.Sprintf("system:serviceaccount:%s:%s", expectedNamespace, c)
			if username == qualified {
				return true
			}
		}
	}
	return false
}

// DetermineRole returns the role assigned to the given authenticated username.
func (c *AuthConfig) DetermineRole(username string) Role {
	if matchesServiceAccount(username, c.AdminServiceAccounts, c.Namespace) {
		return RoleAdmin
	}
	if matchesServiceAccount(username, c.NodeServiceAccounts, c.Namespace) {
		return RoleNode
	}
	return RoleNone
}

// Authorize verifies if a given role is allowed to call the specified method.
func (c *AuthConfig) Authorize(role Role, fullMethod string) error {
	switch role {
	case RoleAdmin:
		return nil
	case RoleNode:
		if nodeAllowedMethods[fullMethod] {
			return nil
		}
		return status.Errorf(codes.PermissionDenied, "role %s is not authorized to call %s", role, fullMethod)
	default:
		return status.Errorf(codes.PermissionDenied, "unauthorized caller (%s) cannot call %s", role, fullMethod)
	}
}

type cachedTokenEntry struct {
	result    *TokenReviewResult
	expiresAt time.Time
}

// CachedTokenReviewer wraps a TokenReviewer with an in-memory TTL cache to minimize
// TokenReview calls to the Kubernetes API server.
type CachedTokenReviewer struct {
	inner       TokenReviewer
	ttl         time.Duration
	negativeTTL time.Duration

	mu    sync.RWMutex
	cache map[[32]byte]*cachedTokenEntry
}

// NewCachedTokenReviewer creates a cached reviewer with the specified positive and negative TTLs.
func NewCachedTokenReviewer(inner TokenReviewer, ttl, negativeTTL time.Duration) *CachedTokenReviewer {
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	if negativeTTL <= 0 {
		negativeTTL = 10 * time.Second
	}
	return &CachedTokenReviewer{
		inner:       inner,
		ttl:         ttl,
		negativeTTL: negativeTTL,
		cache:       make(map[[32]byte]*cachedTokenEntry),
	}
}

// ReviewToken checks cache first, then delegates to inner reviewer.
func (c *CachedTokenReviewer) ReviewToken(ctx context.Context, token string, audiences []string) (*TokenReviewResult, error) {
	key := sha256.Sum256([]byte(token))
	now := time.Now()

	c.mu.RLock()
	entry, ok := c.cache[key]
	c.mu.RUnlock()

	if ok && now.Before(entry.expiresAt) {
		return entry.result, nil
	}

	result, err := c.inner.ReviewToken(ctx, token, audiences)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	var expire time.Time
	if result.Authenticated {
		expire = now.Add(c.ttl)
	} else {
		expire = now.Add(c.negativeTTL)
	}
	c.cache[key] = &cachedTokenEntry{
		result:    result,
		expiresAt: expire,
	}

	// Purge expired entries if cache grows
	if len(c.cache) > 1024 {
		for k, v := range c.cache {
			if now.After(v.expiresAt) {
				delete(c.cache, k)
			}
		}
	}

	return result, nil
}

// KubeTokenReviewer implements TokenReviewer by querying the Kubernetes TokenReview API.
type KubeTokenReviewer struct {
	apiServerURL string
	tokenPath    string
	httpClient   *http.Client
}

type tokenReviewReq struct {
	APIVersion string             `json:"apiVersion"`
	Kind       string             `json:"kind"`
	Spec       tokenReviewReqSpec `json:"spec"`
}

type tokenReviewReqSpec struct {
	Token     string   `json:"token"`
	Audiences []string `json:"audiences,omitempty"`
}

type tokenReviewResp struct {
	APIVersion string                `json:"apiVersion"`
	Kind       string                `json:"kind"`
	Status     tokenReviewRespStatus `json:"status"`
}

type tokenReviewRespStatus struct {
	Authenticated bool                `json:"authenticated"`
	User          tokenReviewRespUser `json:"user,omitempty"`
	Audiences     []string            `json:"audiences,omitempty"`
	Error         string              `json:"error,omitempty"`
}

type tokenReviewRespUser struct {
	Username string              `json:"username"`
	UID      string              `json:"uid"`
	Groups   []string            `json:"groups"`
	Extra    map[string][]string `json:"extra"`
}

// NewKubeTokenReviewer creates a TokenReviewer targeting the in-cluster or configured API server.
func NewKubeTokenReviewer(apiServerURL, tokenPath, caCertPath string) (*KubeTokenReviewer, error) {
	if apiServerURL == "" {
		host := os.Getenv("KUBERNETES_SERVICE_HOST")
		port := os.Getenv("KUBERNETES_SERVICE_PORT")
		if host != "" && port != "" {
			apiServerURL = fmt.Sprintf("https://%s:%s", host, port)
		} else {
			apiServerURL = "https://kubernetes.default.svc"
		}
	}
	if tokenPath == "" {
		tokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	}

	var rootCAs *x509.CertPool
	if caCertPath == "" {
		caCertPath = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
	}
	b, err := os.ReadFile(caCertPath)
	if err != nil {
		if !os.IsNotExist(err) {
			klog.V(4).Infof("could not read kubernetes cluster CA cert from %s: %v", caCertPath, err)
		}
	} else {
		pool := x509.NewCertPool()
		if pool.AppendCertsFromPEM(b) {
			rootCAs = pool
		}
	}

	tlsConfig := &tls.Config{
		RootCAs: rootCAs,
	}
	transport := &http.Transport{
		TLSClientConfig: tlsConfig,
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   10 * time.Second,
	}

	return &KubeTokenReviewer{
		apiServerURL: strings.TrimRight(apiServerURL, "/"),
		tokenPath:    tokenPath,
		httpClient:   client,
	}, nil
}

// ReviewToken invokes the Kubernetes TokenReview REST API.
func (k *KubeTokenReviewer) ReviewToken(ctx context.Context, token string, audiences []string) (*TokenReviewResult, error) {
	controllerToken, err := os.ReadFile(k.tokenPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read controller service account token from %s: %w", k.tokenPath, err)
	}

	reqBody := tokenReviewReq{
		APIVersion: "authentication.k8s.io/v1",
		Kind:       "TokenReview",
		Spec: tokenReviewReqSpec{
			Token:     token,
			Audiences: audiences,
		},
	}
	b, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal tokenreview request: %w", err)
	}

	endpoint := k.apiServerURL + "/apis/authentication.k8s.io/v1/tokenreviews"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(b))
	if err != nil {
		return nil, fmt.Errorf("failed to create http request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(controllerToken)))

	resp, err := k.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("tokenreview request failed: %w", err)
	}
	defer errutil.CloseAndLog(resp.Body, "tokenreview response body")

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read tokenreview response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("tokenreview returned HTTP status %d: %s", resp.StatusCode, strings.TrimSpace(string(respBytes)))
	}

	var trResp tokenReviewResp
	if err := json.Unmarshal(respBytes, &trResp); err != nil {
		return nil, fmt.Errorf("failed to decode tokenreview response: %w", err)
	}

	return &TokenReviewResult{
		Authenticated: trResp.Status.Authenticated,
		Username:      trResp.Status.User.Username,
		UID:           trResp.Status.User.UID,
		Groups:        trResp.Status.User.Groups,
		Audiences:     trResp.Status.Audiences,
		Error:         trResp.Status.Error,
	}, nil
}

// ServerAuthInterceptor enforces authentication and role-based authorization for ObjectFSController gRPC calls.
type ServerAuthInterceptor struct {
	reviewer TokenReviewer
	config   AuthConfig
}

// NewServerAuthInterceptor creates a new ServerAuthInterceptor.
func NewServerAuthInterceptor(reviewer TokenReviewer, config AuthConfig) *ServerAuthInterceptor {
	return &ServerAuthInterceptor{
		reviewer: reviewer,
		config:   config,
	}
}

type authIdentityKey struct{}

// AuthIdentity holds the validated caller identity and assigned role.
type AuthIdentity struct {
	Username string
	Role     Role
}

// CallerFromContext extracts the AuthIdentity from context if available.
func CallerFromContext(ctx context.Context) (*AuthIdentity, bool) {
	val, ok := ctx.Value(authIdentityKey{}).(*AuthIdentity)
	return val, ok
}

func (s *ServerAuthInterceptor) authenticateAndAuthorize(ctx context.Context, fullMethod string) (context.Context, error) {
	if s.config.Insecure {
		return ctx, nil
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx, status.Errorf(codes.Unauthenticated, "missing authorization metadata")
	}

	authHeaders := md.Get("authorization")
	if len(authHeaders) == 0 {
		return ctx, status.Errorf(codes.Unauthenticated, "missing authorization header")
	}

	authHeader := authHeaders[0]
	if !strings.HasPrefix(strings.ToLower(authHeader), "bearer ") {
		return ctx, status.Errorf(codes.Unauthenticated, "invalid authorization header: must be Bearer token")
	}

	token := strings.TrimSpace(authHeader[7:])
	if token == "" {
		return ctx, status.Errorf(codes.Unauthenticated, "authorization token is empty")
	}

	result, err := s.reviewer.ReviewToken(ctx, token, s.config.Audiences)
	if err != nil {
		klog.Errorf("TokenReview API failure: %v", err)
		return ctx, status.Errorf(codes.Internal, "token review failed: %v", err)
	}

	if !result.Authenticated {
		klog.Warningf("Token authentication failed for RPC %s: %s", fullMethod, result.Error)
		return ctx, status.Errorf(codes.Unauthenticated, "authentication failed: %s", result.Error)
	}

	// Verify audience overlap if audiences were returned
	if len(s.config.Audiences) > 0 && len(result.Audiences) > 0 {
		matched := false
		for _, exp := range s.config.Audiences {
			for _, aud := range result.Audiences {
				if aud == exp {
					matched = true
					break
				}
			}
		}
		if !matched {
			return ctx, status.Errorf(codes.Unauthenticated, "token audiences %v do not match required audience %v", result.Audiences, s.config.Audiences)
		}
	}

	role := s.config.DetermineRole(result.Username)
	if err := s.config.Authorize(role, fullMethod); err != nil {
		klog.Warningf("Authorization failed for user %q (role %s) calling %s: %v", result.Username, role, fullMethod, err)
		return ctx, err
	}

	newCtx := context.WithValue(ctx, authIdentityKey{}, &AuthIdentity{
		Username: result.Username,
		Role:     role,
	})
	return newCtx, nil
}

// UnaryServerInterceptor returns a grpc.UnaryServerInterceptor enforcing authentication and authorization.
func (s *ServerAuthInterceptor) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		newCtx, err := s.authenticateAndAuthorize(ctx, info.FullMethod)
		if err != nil {
			return nil, err
		}
		return handler(newCtx, req)
	}
}

type wrappedServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedServerStream) Context() context.Context {
	return w.ctx
}

// StreamServerInterceptor returns a grpc.StreamServerInterceptor enforcing authentication and authorization.
func (s *ServerAuthInterceptor) StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		newCtx, err := s.authenticateAndAuthorize(ss.Context(), info.FullMethod)
		if err != nil {
			return err
		}
		return handler(srv, &wrappedServerStream{ServerStream: ss, ctx: newCtx})
	}
}
