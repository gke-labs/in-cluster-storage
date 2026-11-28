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
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	minimumResendDelay = 20 * time.Millisecond
	maximumResendDelay = 1 * time.Second
)

// ResendStatus returns a gRPC Unavailable status error carrying a
// google.rpc.RetryInfo detail with the given delay, telling the caller that
// the request was not applied and may be sent again after waiting.
func ResendStatus(message string, delay time.Duration) error {
	statusObject := status.New(codes.Unavailable, message)
	statusWithDetails, err := statusObject.WithDetails(&errdetails.RetryInfo{
		RetryDelay: durationpb.New(delay),
	})
	if err != nil {
		return status.Errorf(codes.Internal, "failed to attach RetryInfo detail to status: %v", err)
	}
	return statusWithDetails.Err()
}

// ResendDelay reports whether err carries a gRPC Unavailable status with a
// google.rpc.RetryInfo detail, including through wrapped errors, and returns
// the advertised delay. A plain Unavailable status returns false.
func ResendDelay(err error) (delay time.Duration, resend bool) {
	if err == nil {
		return 0, false
	}
	statusObject, ok := status.FromError(err)
	if !ok || statusObject == nil || statusObject.Code() != codes.Unavailable {
		return 0, false
	}
	for _, detail := range statusObject.Details() {
		retryInfo, ok := detail.(*errdetails.RetryInfo)
		if !ok {
			continue
		}
		if retryDelay := retryInfo.GetRetryDelay(); retryDelay != nil {
			return retryDelay.AsDuration(), true
		}
		return 0, true
	}
	return 0, false
}

func clampResendDelay(delay time.Duration) time.Duration {
	return min(max(delay, minimumResendDelay), maximumResendDelay)
}

// ResendUnaryClientInterceptor returns a grpc.UnaryClientInterceptor that
// re-sends unary RPCs answered with a ResendStatus after waiting the stated
// delay clamped to [20ms, 1s]. It repeats until the call returns any other
// result or the call context ends, in which case it returns the last error.
func ResendUnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, request, reply any, connection *grpc.ClientConn, invoker grpc.UnaryInvoker, callOptions ...grpc.CallOption) error {
		for {
			err := invoker(ctx, method, request, reply, connection, callOptions...)
			delay, resend := ResendDelay(err)
			if !resend || ctx.Err() != nil {
				return err
			}

			timer := time.NewTimer(clampResendDelay(delay))
			select {
			case <-ctx.Done():
				timer.Stop()
				return err
			case <-timer.C:
			}
		}
	}
}
