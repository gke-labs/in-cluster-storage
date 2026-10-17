// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package e2e

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

var (
	coreGroup = map[string]bool{
		"TestObjectFSE2E":            true,
		"TestObjectFSStatefulSetE2E": true,
		"TestObjectFSPersistentE2E":  true,
		"TestWALE2E":                 true,
		"TestWALS3E2E":               true,
	}

	multiNodeGroup = map[string]bool{
		"TestObjectFSMultiNodeFailoverE2E":      true,
		"TestObjectFSCrossNodeVisibilityE2E":    true,
		"TestObjectFSExclusiveLocalE2E":         true, // #290
		"TestObjectFSExclusiveLocalKillE2E":     true, // #288
		"TestObjectFSExclusiveLocalIntervalE2E": true, // #288
	}

	chaosGroup = map[string]bool{
		"TestObjectFSControllerCrashMidWriteE2E": true,
		"TestObjectFSControllerOutageE2E":        true,
		"TestObjectFSWalBufferRestartE2E":        true,
	}

	periodicGroup = map[string]bool{
		"TestObjectFSTortureE2E": true, // #300
	}
)

func findE2ETestFunctions(t *testing.T, dir string) (map[string]*ast.FuncDecl, *token.FileSet) {
	t.Helper()
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, dir, func(fi os.FileInfo) bool {
		// Only check Go test files, excluding this test itself
		name := fi.Name()
		return strings.HasSuffix(name, "_test.go") && name != "e2e_groups_test.go"
	}, parser.ParseComments)
	if err != nil {
		t.Fatalf("failed to parse directory %s: %v", dir, err)
	}

	tests := make(map[string]*ast.FuncDecl)
	for _, pkg := range pkgs {
		for _, file := range pkg.Files {
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok {
					continue
				}
				if strings.HasPrefix(fn.Name.Name, "Test") {
					tests[fn.Name.Name] = fn
				}
			}
		}
	}
	return tests, fset
}

func checkTestAssignment(tests map[string]*ast.FuncDecl, groups map[string]map[string]bool) (missing []string, duplicates map[string][]string) {
	duplicates = make(map[string][]string)
	for testName := range tests {
		var assigned []string
		for groupName, groupSet := range groups {
			if groupSet[testName] {
				assigned = append(assigned, groupName)
			}
		}
		sort.Strings(assigned)
		if len(assigned) == 0 {
			missing = append(missing, testName)
		} else if len(assigned) > 1 {
			duplicates[testName] = assigned
		}
	}
	sort.Strings(missing)
	return missing, duplicates
}

func TestE2EGroups_NoDrift(t *testing.T) {
	tests, _ := findE2ETestFunctions(t, ".")
	if len(tests) == 0 {
		t.Fatal("no E2E tests found in tests/e2e")
	}

	groups := map[string]map[string]bool{
		"core":      coreGroup,
		"multinode": multiNodeGroup,
		"chaos":     chaosGroup,
		"periodic":  periodicGroup,
	}

	missing, duplicates := checkTestAssignment(tests, groups)
	if len(missing) > 0 {
		t.Errorf("found %d E2E test(s) not assigned to any group (must be added to core, multinode, chaos, or periodic): %v", len(missing), missing)
	}
	if len(duplicates) > 0 {
		for testName, assigned := range duplicates {
			t.Errorf("test %s is assigned to multiple groups: %v", testName, assigned)
		}
	}

	// Periodic tests must never be listed in presubmit groups
	for testName := range periodicGroup {
		if coreGroup[testName] || multiNodeGroup[testName] || chaosGroup[testName] {
			t.Errorf("periodic test %s must not be in presubmit groups", testName)
		}
	}

	// Verify any present periodic tests are gated by OBJECTFS_TORTURE
	for testName, fn := range tests {
		if periodicGroup[testName] {
			hasGating := false
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if lit, ok := n.(*ast.BasicLit); ok && strings.Contains(lit.Value, "OBJECTFS_TORTURE") {
					hasGating = true
					return false
				}
				if id, ok := n.(*ast.Ident); ok && strings.Contains(id.Name, "OBJECTFS_TORTURE") {
					hasGating = true
					return false
				}
				return true
			})
			if !hasGating {
				t.Errorf("periodic test %s must be gated by OBJECTFS_TORTURE=1", testName)
			}
		}
	}
}

func extractRunPattern(t *testing.T, scriptPath string) string {
	t.Helper()
	content, err := os.ReadFile(scriptPath)
	if err != nil {
		t.Fatalf("failed to read script %s: %v", scriptPath, err)
	}
	re := regexp.MustCompile(`-run\s+'([^']+)'`)
	matches := re.FindStringSubmatch(string(content))
	if len(matches) < 2 {
		t.Fatalf("could not find -run pattern in script %s", scriptPath)
	}
	return matches[1]
}

func TestE2EPresubmitScripts_MatchGroups(t *testing.T) {
	repoRoot := filepath.Join("..", "..")

	scriptChecks := []struct {
		scriptPath string
		group      map[string]bool
		name       string
	}{
		{
			scriptPath: filepath.Join(repoRoot, "dev", "ci", "presubmits", "ap-e2e-core"),
			group:      coreGroup,
			name:       "core",
		},
		{
			scriptPath: filepath.Join(repoRoot, "dev", "ci", "presubmits", "ap-e2e-multinode"),
			group:      multiNodeGroup,
			name:       "multinode",
		},
		{
			scriptPath: filepath.Join(repoRoot, "dev", "ci", "presubmits", "ap-e2e-chaos"),
			group:      chaosGroup,
			name:       "chaos",
		},
		{
			scriptPath: filepath.Join(repoRoot, "dev", "ci", "periodics", "ap-torture"),
			group:      periodicGroup,
			name:       "periodic",
		},
	}

	for _, sc := range scriptChecks {
		pattern := extractRunPattern(t, sc.scriptPath)
		re, err := regexp.Compile(pattern)
		if err != nil {
			t.Fatalf("invalid regex %q in %s: %v", pattern, sc.scriptPath, err)
		}

		for testName := range sc.group {
			if !re.MatchString(testName) {
				t.Errorf("script %s (%s) does not match assigned test %s with pattern %s", sc.scriptPath, sc.name, testName, pattern)
			}
		}

		// Ensure presubmit scripts never match any periodic tests
		if sc.name != "periodic" {
			for pTest := range periodicGroup {
				if re.MatchString(pTest) {
					t.Errorf("presubmit script %s matches periodic test %s", sc.scriptPath, pTest)
				}
			}
		}
	}
}

func TestE2EGroups_DriftGuardDetection(t *testing.T) {
	// Verify that the assignment checker catches unassigned and duplicate tests
	mockTests := map[string]*ast.FuncDecl{
		"TestAssigned":   nil,
		"TestUnassigned": nil,
		"TestDuplicate":  nil,
	}
	mockGroups := map[string]map[string]bool{
		"groupA": {"TestAssigned": true, "TestDuplicate": true},
		"groupB": {"TestDuplicate": true},
	}

	missing, duplicates := checkTestAssignment(mockTests, mockGroups)
	if len(missing) != 1 || missing[0] != "TestUnassigned" {
		t.Errorf("expected missing [TestUnassigned], got %v", missing)
	}
	if len(duplicates) != 1 || len(duplicates["TestDuplicate"]) != 2 {
		t.Errorf("expected duplicate TestDuplicate in 2 groups, got %v", duplicates)
	}
}
