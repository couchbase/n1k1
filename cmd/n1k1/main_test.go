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

package main

import (
	"os"
	"path/filepath"
	"testing"
)

// TestResolveSessionExplicitBadPathErrors: an explicitly-named path that can't be
// opened is fatal (returns an error) rather than silently starting empty -- so a
// typo'd path in `-c`/piped/scripted use fails loudly instead of "succeeding".
func TestResolveSessionExplicitBadPathErrors(t *testing.T) {
	sess, effDir, cleanup, err := resolveSession("bad/path/does-not-exist", true)
	if cleanup != nil {
		cleanup()
	}
	if err == nil {
		t.Fatalf("explicit bad path should error, got sess=%v effDir=%q", sess, effDir)
	}
	if sess != nil {
		t.Errorf("expected nil session on error, got %v", sess)
	}
}

// TestResolveSessionExplicitGoodPath: an openable explicit path (an existing dir,
// even if it has no keyspaces yet) succeeds and reports that dir.
func TestResolveSessionExplicitGoodPath(t *testing.T) {
	dir := t.TempDir()
	sess, effDir, cleanup, err := resolveSession(dir, true)
	if cleanup != nil {
		defer cleanup()
	}
	if err != nil {
		t.Fatalf("existing dir should open, got err %v", err)
	}
	if sess == nil {
		t.Fatal("expected a session")
	}
	if effDir != dir {
		t.Errorf("effDir = %q, want %q", effDir, dir)
	}
}

// TestTidyMsg: doubled spaces (e.g. the fork's "file datastore  - cause") collapse
// to one, while ordinary single-spaced text is untouched.
func TestTidyMsg(t *testing.T) {
	cases := map[string]string{
		"Error in file datastore  - cause: x": "Error in file datastore - cause: x",
		"a   b    c":                          "a b c",
		"already single spaced":               "already single spaced",
		"":                                    "",
	}
	for in, want := range cases {
		if got := tidyMsg(in); got != want {
			t.Errorf("tidyMsg(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestResolveSessionFallbackWhenNotExplicit: with no path named (explicit=false),
// a failed open falls back to a fresh empty store (effDir == "") so a bare REPL
// still starts; cleanup is safe to call.
func TestResolveSessionFallbackWhenNotExplicit(t *testing.T) {
	// A path that cannot be opened, but "not explicit" (as if defaulted).
	bad := filepath.Join(t.TempDir(), "definitely-missing")
	sess, effDir, cleanup, err := resolveSession(bad, false)
	if cleanup != nil {
		defer cleanup()
	}
	if err != nil {
		t.Fatalf("non-explicit open failure should fall back, got err %v", err)
	}
	if sess == nil {
		t.Fatal("expected an empty-store session on fallback")
	}
	if effDir != "" {
		t.Errorf("effDir = %q, want \"\" (signals the empty-store fallback)", effDir)
	}
}

// TestResolveSessionNoPathNeverScansCwd: a bare invocation (no path -> explicit=false)
// must start a FRESH EMPTY store, NOT scan the current working directory -- even when
// cwd opens fine as a datastore (a dir with data files in it). Regression guard: a
// bare `n1k1` used to expose the whole cwd as keyspaces because it opened the default
// "." dir and only fell back to empty on an open *failure*.
func TestResolveSessionNoPathNeverScansCwd(t *testing.T) {
	// Run from a directory that is a perfectly openable datastore (holds a data file),
	// so OpenSession(".") would succeed and scan it if the fix regressed.
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(cwd)
	dataDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dataDir, "leak.jsonl"), []byte(`{"x":1}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dataDir); err != nil {
		t.Fatal(err)
	}

	sess, effDir, cleanup, err := resolveSession(".", false) // exactly what main() passes with no args
	if cleanup != nil {
		defer cleanup()
	}
	if err != nil {
		t.Fatalf("no-path resolve should succeed, got %v", err)
	}
	if sess == nil {
		t.Fatal("expected an empty-store session")
	}
	if effDir != "" {
		t.Errorf("effDir = %q, want \"\" -- a bare REPL must start empty, not scan cwd", effDir)
	}
	// And the empty store must expose NO keyspace from cwd.
	if rows, rerr := sess.Run(`SELECT COUNT(*) AS n FROM leak`); rerr == nil {
		t.Errorf("bare REPL saw a cwd keyspace %q; want a no-keyspace error, got rows %v", "leak", rows)
	}
}
