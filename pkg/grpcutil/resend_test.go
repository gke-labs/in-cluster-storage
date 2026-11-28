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
	"errors"
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestResendDelay(t *testing.T) {
	resendErr := ResendStatus("writer lock reserved", 75*time.Millisecond)
	if status.Code(resendErr) != codes.Unavailable {
		t.Fatalf("expected Unavailable code, got %v", status.Code(resendErr))
	}

	for _, testCase := range []struct {
		name      string
		err       error
		wantDelay time.Duration
		wantOK    bool
	}{
		{name: "resend status", err: resendErr, wantDelay: 75 * time.Millisecond, wantOK: true},
		{name: "wrapped resend status", err: fmt.Errorf("controller call failed: %w", resendErr), wantDelay: 75 * time.Millisecond, wantOK: true},
		{name: "plain unavailable", err: status.Error(codes.Unavailable, "transport down"), wantOK: false},
		{name: "deadline exceeded", err: status.Error(codes.DeadlineExceeded, "timed out"), wantOK: false},
		{name: "non-status error", err: errors.New("boom"), wantOK: false},
		{name: "nil error", err: nil, wantOK: false},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			delay, resend := ResendDelay(testCase.err)
			if resend != testCase.wantOK || delay != testCase.wantDelay {
				t.Fatalf("ResendDelay(%v) = (%v, %v), want (%v, %v)", testCase.err, delay, resend, testCase.wantDelay, testCase.wantOK)
			}
		})
	}
}

func TestClampResendDelay(t *testing.T) {
	for _, testCase := range []struct {
		input, want time.Duration
	}{
		{-10 * time.Millisecond, minimumResendDelay},
		{0, minimumResendDelay},
		{250 * time.Millisecond, 250 * time.Millisecond},
		{5 * time.Second, maximumResendDelay},
	} {
		if got := clampResendDelay(testCase.input); got != testCase.want {
			t.Errorf("clampResendDelay(%v) = %v, want %v", testCase.input, got, testCase.want)
		}
	}
}

func TestResendUnaryClientInterceptor_SucceedsAfterTwoResends(t *testing.T) {
	interceptor := ResendUnaryClientInterceptor()
	invocations := 0
	invoker := func(ctx context.Context, method string, request, reply any, connection *grpc.ClientConn, callOptions ...grpc.CallOption) error {
		invocations++
		if invocations <= 2 {
			return ResendStatus("try again", 5*time.Millisecond)
		}
		return nil
	}

	start := time.Now()
	if err := interceptor(t.Context(), "/test.Service/Method", nil, nil, nil, invoker); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if invocations != 3 {
		t.Fatalf("expected 3 invocations, got %d", invocations)
	}
	if elapsed := time.Since(start); elapsed < 2*minimumResendDelay {
		t.Fatalf("expected at least %v elapsed across 2 resends, got %v", 2*minimumResendDelay, elapsed)
	}
}

func TestResendUnaryClientInterceptor_StopsWhenContextEnds(t *testing.T) {
	interceptor := ResendUnaryClientInterceptor()
	resendErr := ResendStatus("still busy", minimumResendDelay)
	invocations := 0
	invoker := func(ctx context.Context, method string, request, reply any, connection *grpc.ClientConn, callOptions ...grpc.CallOption) error {
		invocations++
		return resendErr
	}

	start := time.Now()
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()

	err := interceptor(ctx, "/test.Service/Method", nil, nil, nil, invoker)
	elapsed := time.Since(start)
	if err != resendErr {
		t.Fatalf("expected last resend error %v, got %v", resendErr, err)
	}
	if invocations < 2 {
		t.Fatalf("expected multiple invocations before context deadline, got %d", invocations)
	}
	if elapsed < 190*time.Millisecond || elapsed > 450*time.Millisecond {
		t.Fatalf("expected return near 200ms context deadline, elapsed %v", elapsed)
	}
}

func TestResendUnaryClientInterceptor_NonResendErrorsReturnedOnce(t *testing.T) {
	interceptor := ResendUnaryClientInterceptor()
	for _, testCase := range []struct {
		name string
		err  error
	}{
		{name: "plain unavailable", err: status.Error(codes.Unavailable, "transport failure")},
		{name: "deadline exceeded", err: status.Error(codes.DeadlineExceeded, "deadline exceeded")},
		{name: "non-status error", err: errors.New("unexpected failure")},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			invocations := 0
			invoker := func(ctx context.Context, method string, request, reply any, connection *grpc.ClientConn, callOptions ...grpc.CallOption) error {
				invocations++
				return testCase.err
			}
			if err := interceptor(t.Context(), "/test.Service/Method", nil, nil, nil, invoker); err != testCase.err {
				t.Fatalf("expected error %v, got %v", testCase.err, err)
			}
			if invocations != 1 {
				t.Fatalf("expected 1 invocation, got %d", invocations)
			}
		})
	}
}
