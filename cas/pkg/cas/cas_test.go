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

func TestIsValidSHA256(t *testing.T) {
	tests := []struct {
		sha   string
		valid bool
	}{
		{"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", true},
		{"0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF", true},
		{"short", false},
		{"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdeg", false}, // non-hex character 'g'
	}

	for _, tc := range tests {
		got := isValidSHA256(tc.sha)
		if got != tc.valid {
			t.Errorf("isValidSHA256(%q) = %t; want %t", tc.sha, got, tc.valid)
		}
	}
}
