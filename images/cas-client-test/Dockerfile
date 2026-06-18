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
RUN CGO_ENABLED=0 go build -o cas-client-test ./cmd/cas-client-test

FROM alpine:latest

COPY --from=builder /app/cas/cas-client-test /usr/local/bin/cas-client-test

ENTRYPOINT ["/usr/local/bin/cas-client-test"]
