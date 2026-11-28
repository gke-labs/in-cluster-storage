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
	"time"

	"github.com/gke-labs/in-cluster-storage/pkg/grpcutil"
	"github.com/gke-labs/in-cluster-storage/pkg/scoped"
	"google.golang.org/grpc"
)

// writerLockLostResendDelay is the retry delay returned when a request fails due to writer lock loss.
const writerLockLostResendDelay = 20 * time.Millisecond

// volumeIDRequest is implemented by gRPC requests that carry a volume_id field.
type volumeIDRequest interface {
	GetVolumeId() string
}

// errnoResponse is implemented by gRPC responses that carry a POSIX errno in their error field.
type errnoResponse interface {
	GetError() int32
}

// lookupVolume returns the cached Volume for volumeID without creating or loading one.
func (s *Server) lookupVolume(volumeID string) *Volume {
	if volumeID == "" {
		return nil
	}
	s.mu.RLock()
	volume := s.volumes[volumeID]
	s.mu.RUnlock()
	return volume
}

// responseFailed reports whether a unary handler failed via a gRPC error or a non-zero response errno.
func responseFailed(response any, err error) bool {
	if err != nil {
		return true
	}
	if errno, ok := response.(errnoResponse); ok && errno.GetError() != 0 {
		return true
	}
	return false
}

// ResendUnaryServerInterceptor returns a grpc.UnaryServerInterceptor that
// answers a failed request with a resend status when the request's volume lost
// its WAL stream writer lock while the handler was running. If the volume had
// already lost its lock before the handler ran, the handler's own lookup
// reloads it and its result is returned unchanged.
func (s *Server) ResendUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		volumeRequest, ok := request.(volumeIDRequest)
		if !ok {
			return handler(ctx, request)
		}
		volume := s.lookupVolume(volumeRequest.GetVolumeId())
		if volume == nil || volume.writerLockLost() {
			return handler(ctx, request)
		}
		response, err := handler(ctx, request)
		if responseFailed(response, err) && volume.writerLockLost() {
			scoped.Log(ctx).Info("request invalidated by writer lock loss; asking caller to resend", "delay", writerLockLostResendDelay)
			return nil, grpcutil.ResendStatus("volume reloading after writer lock loss", writerLockLostResendDelay)
		}
		return response, err
	}
}
