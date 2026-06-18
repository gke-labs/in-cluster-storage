# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM golang:1.26.4 AS builder

WORKDIR /app
COPY . .
WORKDIR /app/cas
RUN go mod download
RUN CGO_ENABLED=0 go build -o cas-node-daemon ./cmd/cas-node-daemon

FROM debian:bookworm-slim

# Install mount utilities
RUN apt-get update && apt-get install -y mount ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/cas/cas-node-daemon /usr/local/bin/cas-node-daemon

ENTRYPOINT ["/usr/local/bin/cas-node-daemon"]
