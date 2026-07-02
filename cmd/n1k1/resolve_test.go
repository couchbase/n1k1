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
	"path/filepath"
	"testing"
)

// TestResolveSessionExplicitBadPathErrors: an explicitly-named path that can't be
// opened is fatal (returns an error) rather than silently starting empty -- so a
// typo'd path in `-c`/piped/scripted use fails loudly instead of "succeeding".
func TestResolveSessionExplicitBadPathErrors(t *testing.T) {
	sess, effDir, cleanup, err := resolveSession("bad/path/does-not-exist", true, "default")
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
	sess, effDir, cleanup, err := resolveSession(dir, true, "default")
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
	sess, effDir, cleanup, err := resolveSession(bad, false, "default")
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
