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

package cas

import "testing"

func TestParseBlobID(t *testing.T) {
	tests := []struct {
		sha      string
		valid    bool
		expected string
	}{
		{"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", true, "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"},
		{"0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF", true, "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"},
		{"short", false, ""},
		{"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdeg", false, ""}, // non-hex character 'g'
	}

	for _, tc := range tests {
		blobID, err := ParseBlobID(tc.sha)
		if tc.valid {
			if err != nil {
				t.Errorf("ParseBlobID(%q) returned unexpected error: %v", tc.sha, err)
			}
			if string(blobID) != tc.expected {
				t.Errorf("ParseBlobID(%q) = %q; want %q", tc.sha, blobID, tc.expected)
			}
		} else {
			if err == nil {
				t.Errorf("ParseBlobID(%q) expected error, but got none", tc.sha)
			}
		}
	}
}
