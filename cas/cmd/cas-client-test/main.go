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

package main

import (
	"crypto/sha256"
	"fmt"
	"io"
	"os"

	"github.com/gke-labs/in-cluster-storage/cas/pkg/cas"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: cas-client-test <socket_path> <sha256>")
		os.Exit(1)
	}

	socketPath := os.Args[1]
	sha := os.Args[2]

	client, err := cas.NewClient(socketPath)
	if err != nil {
		fmt.Printf("Error creating CAS client: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()

	file, size, err := client.RequestBlob(sha)
	if err != nil {
		fmt.Printf("Error requesting blob: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	hasher := sha256.New()
	tee := io.TeeReader(file, hasher)

	content, err := io.ReadAll(tee)
	if err != nil {
		fmt.Printf("Error reading file: %v\n", err)
		os.Exit(1)
	}

	computedSha := fmt.Sprintf("%x", hasher.Sum(nil))
	fmt.Printf("SUCCESS size=%d sha256=%s\n", size, computedSha)
	fmt.Print(string(content))
}
