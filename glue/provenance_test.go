//go:build n1ql

//  Copyright (c) 2026 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package glue

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestPrepareLevelAchievable checks the graceful-degradation gate: with no `go`
// toolchain, the compiled ceilings fall back to the interpreter; with one, the
// desired ceiling is kept. The interpreter floor is never gated.
func TestPrepareLevelAchievable(t *testing.T) {
	cases := []struct {
		want     PrepareLevel
		goAvail  bool
		expected PrepareLevel
	}{
		{PrepareInterpreted, false, PrepareInterpreted}, // floor never needs a toolchain
		{PrepareInterpreted, true, PrepareInterpreted},
		{PrepareCompiledData, false, PrepareInterpreted}, // no go -> degrade
		{PrepareCompiledData, true, PrepareCompiledData},
		{PrepareCompiledFull, false, PrepareInterpreted}, // no go -> degrade
		{PrepareCompiledFull, true, PrepareCompiledFull},
	}
	for _, c := range cases {
		if got := prepareLevelAchievable(c.want, c.goAvail); got != c.expected {
			t.Errorf("prepareLevelAchievable(%v, goAvail=%v) = %v, want %v",
				c.want, c.goAvail, got, c.expected)
		}
	}
}

// TestGoToolchainDetect: in the test environment `go` is on PATH, so detection
// reports Available with a version string; the result is cached (stable).
func TestGoToolchainDetect(t *testing.T) {
	tc := GoToolchainDetect()
	if !tc.Available || tc.Path == "" {
		t.Fatalf("go should be detected in the test env; got %+v", tc)
	}
	if !strings.Contains(tc.Version, "go") {
		t.Errorf("Version = %q, want a `go version` string", tc.Version)
	}
	if GoToolchainDetect() != tc {
		t.Error("GoToolchainDetect should be cached/stable")
	}
}

// TestProvenanceFromGit captures git provenance from a throwaway repo: the HEAD
// SHA on a clean tree, then Dirty + a non-empty Diff after an uncommitted edit --
// the "dev progress without a commit" case codegen relies on.
func TestProvenanceFromGit(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}
	dir := t.TempDir()
	run := func(args ...string) {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
		cmd.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@t", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@t")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
	}
	run("init")
	if err := os.WriteFile(filepath.Join(dir, "f.go"), []byte("package p\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	run("add", "f.go")
	run("commit", "-m", "init")

	// Clean tree: a SHA, not dirty, no diff.
	p, ok := provenanceFromGit(dir)
	if !ok {
		t.Fatal("provenanceFromGit failed on a valid repo")
	}
	if len(p.SHA) < 7 {
		t.Errorf("SHA = %q, want a git hash", p.SHA)
	}
	if p.Dirty || p.Diff != "" {
		t.Errorf("clean tree: Dirty=%v Diff=%q, want clean", p.Dirty, p.Diff)
	}
	if !strings.HasPrefix(p.Source, "git:") {
		t.Errorf("Source = %q, want git:...", p.Source)
	}

	// Uncommitted edit: Dirty, with the edit visible in the Diff.
	if err := os.WriteFile(filepath.Join(dir, "f.go"), []byte("package p\n\nvar X = 1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	p2, _ := provenanceFromGit(dir)
	if !p2.Dirty {
		t.Error("after an edit, Dirty should be true")
	}
	if !strings.Contains(p2.Diff, "var X = 1") {
		t.Errorf("Diff should contain the uncommitted edit; got:\n%s", p2.Diff)
	}
	if s := p2.Stamp(); !strings.Contains(s, "+uncommitted") {
		t.Errorf("Stamp() = %q, want it to note +uncommitted", s)
	}

	// A non-repo dir falls through so ProvenanceCapture uses build info.
	if _, ok := provenanceFromGit(t.TempDir()); ok {
		t.Error("provenanceFromGit on a non-repo should report not-ok")
	}
}
