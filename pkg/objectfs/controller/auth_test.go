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
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type mockReviewer struct {
	reviews int64
	fn      func(ctx context.Context, token string, audiences []string) (*TokenReviewResult, error)
}

func (m *mockReviewer) ReviewToken(ctx context.Context, token string, audiences []string) (*TokenReviewResult, error) {
	atomic.AddInt64(&m.reviews, 1)
	return m.fn(ctx, token, audiences)
}

func TestCachedTokenReviewer(t *testing.T) {
	var reviewCalls int64
	mock := &mockReviewer{
		fn: func(ctx context.Context, token string, audiences []string) (*TokenReviewResult, error) {
			atomic.AddInt64(&reviewCalls, 1)
			if token == "valid-token" {
				return &TokenReviewResult{
					Authenticated: true,
					Username:      "system:serviceaccount:default:objectfs-node-daemon",
					Audiences:     audiences,
				}, nil
			}
			return &TokenReviewResult{
				Authenticated: false,
				Error:         "invalid token",
			}, nil
		},
	}

	cached := NewCachedTokenReviewer(mock, 100*time.Millisecond, 50*time.Millisecond)

	// Call 1: Miss
	res1, err := cached.ReviewToken(t.Context(), "valid-token", []string{"objectfs-controller"})
	if err != nil || !res1.Authenticated {
		t.Fatalf("unexpected call 1 result: res=%v, err=%v", res1, err)
	}
	if atomic.LoadInt64(&reviewCalls) != 1 {
		t.Errorf("expected 1 review call, got %d", reviewCalls)
	}

	// Call 2: Hit
	res2, err := cached.ReviewToken(t.Context(), "valid-token", []string{"objectfs-controller"})
	if err != nil || !res2.Authenticated {
		t.Fatalf("unexpected call 2 result: res=%v, err=%v", res2, err)
	}
	if atomic.LoadInt64(&reviewCalls) != 1 {
		t.Errorf("expected still 1 review call on cache hit, got %d", reviewCalls)
	}

	// Wait for TTL to expire
	time.Sleep(120 * time.Millisecond)

	// Call 3: Expired, re-review
	res3, err := cached.ReviewToken(t.Context(), "valid-token", []string{"objectfs-controller"})
	if err != nil || !res3.Authenticated {
		t.Fatalf("unexpected call 3 result: res=%v, err=%v", res3, err)
	}
	if atomic.LoadInt64(&reviewCalls) != 2 {
		t.Errorf("expected 2 review calls after expiration, got %d", reviewCalls)
	}
}

func TestKubeTokenReviewer_HTTP(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/apis/authentication.k8s.io/v1/tokenreviews" {
			http.NotFound(w, r)
			return
		}
		if r.Header.Get("Authorization") != "Bearer controller-secret-token" {
			http.Error(w, "unauthorized controller", http.StatusUnauthorized)
			return
		}

		var req tokenReviewReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		resp := tokenReviewResp{
			APIVersion: "authentication.k8s.io/v1",
			Kind:       "TokenReview",
		}

		if req.Spec.Token == "good-node-token" {
			resp.Status = tokenReviewRespStatus{
				Authenticated: true,
				User: tokenReviewRespUser{
					Username: "system:serviceaccount:kube-objectfs-system:objectfs-node-daemon",
					UID:      "node-daemon-uid",
				},
				Audiences: req.Spec.Audiences,
			}
		} else {
			resp.Status = tokenReviewRespStatus{
				Authenticated: false,
				Error:         "bad token",
			}
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer ts.Close()

	tmpDir := t.TempDir()
	tokenFile := filepath.Join(tmpDir, "token")
	if err := os.WriteFile(tokenFile, []byte("controller-secret-token"), 0600); err != nil {
		t.Fatalf("failed to write token file: %v", err)
	}

	reviewer, err := NewKubeTokenReviewer(ts.URL, tokenFile, "")
	if err != nil {
		t.Fatalf("failed to create KubeTokenReviewer: %v", err)
	}

	// Test good token
	res, err := reviewer.ReviewToken(t.Context(), "good-node-token", []string{"objectfs-controller"})
	if err != nil {
		t.Fatalf("ReviewToken failed: %v", err)
	}
	if !res.Authenticated {
		t.Fatalf("expected authenticated=true, got false")
	}
	if res.Username != "system:serviceaccount:kube-objectfs-system:objectfs-node-daemon" {
		t.Errorf("expected username objectfs-node-daemon, got %s", res.Username)
	}

	// Test bad token
	resBad, err := reviewer.ReviewToken(t.Context(), "bad-token", []string{"objectfs-controller"})
	if err != nil {
		t.Fatalf("ReviewToken failed for bad token: %v", err)
	}
	if resBad.Authenticated {
		t.Fatalf("expected authenticated=false for bad token")
	}
}

func TestServerAuthInterceptor(t *testing.T) {
	mock := &mockReviewer{
		fn: func(ctx context.Context, token string, audiences []string) (*TokenReviewResult, error) {
			switch token {
			case "node-token":
				return &TokenReviewResult{
					Authenticated: true,
					Username:      "system:serviceaccount:kube-objectfs-system:objectfs-node-daemon",
					Audiences:     audiences,
				}, nil
			case "admin-token":
				return &TokenReviewResult{
					Authenticated: true,
					Username:      "system:serviceaccount:kube-objectfs-system:objectfs-admin",
					Audiences:     audiences,
				}, nil
			case "unauthorized-token":
				return &TokenReviewResult{
					Authenticated: true,
					Username:      "system:serviceaccount:default:other-sa",
					Audiences:     audiences,
				}, nil
			default:
				return &TokenReviewResult{
					Authenticated: false,
					Error:         "invalid token",
				}, nil
			}
		},
	}

	cfg := DefaultAuthConfig()
	interceptor := NewServerAuthInterceptor(mock, cfg)
	unary := interceptor.UnaryServerInterceptor()

	dummyHandler := func(ctx context.Context, req any) (any, error) {
		caller, ok := CallerFromContext(ctx)
		if !ok {
			return nil, fmt.Errorf("no caller in context")
		}
		return caller, nil
	}

	// 1. Missing metadata
	_, err := unary(t.Context(), nil, &grpc.UnaryServerInfo{FullMethod: "/objectfs.v1alpha1.ObjectFSController/GetAttr"}, dummyHandler)
	if status.Code(err) != codes.Unauthenticated {
		t.Errorf("expected Unauthenticated for missing metadata, got %v", err)
	}

	// 2. Missing authorization header
	ctxWithEmptyMeta := metadata.NewIncomingContext(t.Context(), metadata.MD{})
	_, err = unary(ctxWithEmptyMeta, nil, &grpc.UnaryServerInfo{FullMethod: "/objectfs.v1alpha1.ObjectFSController/GetAttr"}, dummyHandler)
	if status.Code(err) != codes.Unauthenticated {
		t.Errorf("expected Unauthenticated for empty metadata, got %v", err)
	}

	// 3. Invalid token
	ctxWithBadToken := metadata.NewIncomingContext(t.Context(), metadata.Pairs("authorization", "Bearer bogus"))
	_, err = unary(ctxWithBadToken, nil, &grpc.UnaryServerInfo{FullMethod: "/objectfs.v1alpha1.ObjectFSController/GetAttr"}, dummyHandler)
	if status.Code(err) != codes.Unauthenticated {
		t.Errorf("expected Unauthenticated for bad token, got %v", err)
	}

	// 4. Node token on Node method (GetAttr) -> OK
	ctxWithNodeToken := metadata.NewIncomingContext(t.Context(), metadata.Pairs("authorization", "Bearer node-token"))
	resp, err := unary(ctxWithNodeToken, nil, &grpc.UnaryServerInfo{FullMethod: "/objectfs.v1alpha1.ObjectFSController/GetAttr"}, dummyHandler)
	if err != nil {
		t.Errorf("unexpected error for node token on GetAttr: %v", err)
	}
	identity, ok := resp.(*AuthIdentity)
	if !ok || identity.Role != RoleNode {
		t.Errorf("expected RoleNode identity, got %v", resp)
	}

	// 5. Node token on Admin method (CreateSnapshot) -> PermissionDenied
	_, err = unary(ctxWithNodeToken, nil, &grpc.UnaryServerInfo{FullMethod: "/objectfs.v1alpha1.ObjectFSController/CreateSnapshot"}, dummyHandler)
	if status.Code(err) != codes.PermissionDenied {
		t.Errorf("expected PermissionDenied for node token on CreateSnapshot, got %v", err)
	}

	// 6. Admin token on Admin method (CreateSnapshot) -> OK
	ctxWithAdminToken := metadata.NewIncomingContext(t.Context(), metadata.Pairs("authorization", "Bearer admin-token"))
	resp, err = unary(ctxWithAdminToken, nil, &grpc.UnaryServerInfo{FullMethod: "/objectfs.v1alpha1.ObjectFSController/CreateSnapshot"}, dummyHandler)
	if err != nil {
		t.Errorf("unexpected error for admin token on CreateSnapshot: %v", err)
	}
	identity, ok = resp.(*AuthIdentity)
	if !ok || identity.Role != RoleAdmin {
		t.Errorf("expected RoleAdmin identity, got %v", resp)
	}

	// 7. Admin token on Node method (GetAttr) -> OK
	resp, err = unary(ctxWithAdminToken, nil, &grpc.UnaryServerInfo{FullMethod: "/objectfs.v1alpha1.ObjectFSController/GetAttr"}, dummyHandler)
	if err != nil {
		t.Errorf("unexpected error for admin token on GetAttr: %v", err)
	}

	// 8. Unauthorized token -> PermissionDenied
	ctxWithOtherToken := metadata.NewIncomingContext(t.Context(), metadata.Pairs("authorization", "Bearer unauthorized-token"))
	_, err = unary(ctxWithOtherToken, nil, &grpc.UnaryServerInfo{FullMethod: "/objectfs.v1alpha1.ObjectFSController/GetAttr"}, dummyHandler)
	if status.Code(err) != codes.PermissionDenied {
		t.Errorf("expected PermissionDenied for unauthorized service account, got %v", err)
	}

	// 9. Insecure mode bypasses auth
	insecureCfg := DefaultAuthConfig()
	insecureCfg.Insecure = true
	insecureInterceptor := NewServerAuthInterceptor(mock, insecureCfg)
	_, err = insecureInterceptor.UnaryServerInterceptor()(t.Context(), nil, &grpc.UnaryServerInfo{FullMethod: "/objectfs.v1alpha1.ObjectFSController/GetAttr"}, func(ctx context.Context, req any) (any, error) {
		return "ok", nil
	})
	if err != nil {
		t.Errorf("expected success in insecure mode, got error: %v", err)
	}
}

func TestDetermineRole_NamespaceScoping(t *testing.T) {
	cfg := DefaultAuthConfig() // Namespace: "kube-objectfs-system"

	tests := []struct {
		username string
		wantRole Role
	}{
		// Valid in-namespace service accounts
		{"system:serviceaccount:kube-objectfs-system:objectfs-admin", RoleAdmin},
		{"system:serviceaccount:kube-objectfs-system:objectfs-controller", RoleAdmin},
		{"system:serviceaccount:kube-objectfs-system:objectfs-node-daemon", RoleNode},

		// Spoofed cross-tenant service accounts with identical bare names must be RoleNone
		{"system:serviceaccount:evil:objectfs-admin", RoleNone},
		{"system:serviceaccount:evil:objectfs-controller", RoleNone},
		{"system:serviceaccount:evil:objectfs-node-daemon", RoleNone},
		{"system:serviceaccount:default:objectfs-admin", RoleNone},
		{"system:serviceaccount:default:objectfs-node-daemon", RoleNone},

		// Other accounts
		{"system:serviceaccount:kube-objectfs-system:random-user", RoleNone},
		{"some-other-user", RoleNone},
	}

	for _, tt := range tests {
		got := cfg.DetermineRole(tt.username)
		if got != tt.wantRole {
			t.Errorf("DetermineRole(%q) = %q, want %q", tt.username, got, tt.wantRole)
		}
	}
}
