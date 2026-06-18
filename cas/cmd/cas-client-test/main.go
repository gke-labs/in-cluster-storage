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

	client := cas.NewClient(socketPath)
	file, size, err := client.RequestBlob(sha)
	if err != nil {
		fmt.Printf("Error requesting blob: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	fmt.Printf("SUCCESS size=%d\n", size)
	content, err := io.ReadAll(file)
	if err != nil {
		fmt.Printf("Error reading file: %v\n", err)
		os.Exit(1)
	}
	fmt.Print(string(content))
}
