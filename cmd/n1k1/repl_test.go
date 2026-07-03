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
	"strings"
	"testing"

	"github.com/couchbase/n1k1/glue"
)

// TestFeedCommentThenDot: a leading comment and blank line must not block a
// following dot-command (they're skipped while nothing is buffered). Uses .print
// as the dot-command so no engine session is needed.
func TestFeedCommentThenDot(t *testing.T) {
	var b strings.Builder
	c := &cli{stderr: &b}
	for _, ln := range []string{"-- a leading comment", "   ", ".print hello world"} {
		if c.feed(ln) {
			t.Fatalf("feed(%q) unexpectedly quit", ln)
		}
	}
	if !strings.Contains(b.String(), "hello world") {
		t.Errorf(".print after comment/blank not emitted; got %q", b.String())
	}
}

// TestFeedEcho: with .echo on, each non-blank input line is echoed as it's read,
// so a subsequent .print both echoes its input line and emits its text.
func TestFeedEcho(t *testing.T) {
	var b strings.Builder
	c := &cli{stderr: &b}
	c.feed(".echo on") // sets echo (no output)
	b.Reset()
	c.feed(".print marker")
	if got := b.String(); strings.Count(got, "marker") < 2 {
		t.Errorf("echo on should echo the input line AND emit it; got %q", got)
	}
	// .echo off silences the echo again.
	c.feed(".echo off")
	b.Reset()
	c.feed(".print quiet")
	if got := b.String(); strings.Count(got, "quiet") != 1 {
		t.Errorf("echo off should emit once (no echo); got %q", got)
	}
}

// TestBailStopsOnError: with .bail off (default) a script continues past an
// error, but with .bail on it stops after the first failing statement.
func TestBailStopsOnError(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "d.jsonl"), []byte(`{"a":1}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	sess, err := glue.OpenSession(dir, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}

	// run feeds good; bad; good and returns what reached stdout (the results).
	run := func(bail bool) string {
		var out, errb strings.Builder
		c := &cli{sess: sess, mode: "jsonlines", bail: bail, out: &out, stderr: &errb}
		for _, ln := range []string{"SELECT 1 AS x;", "SELECT bogus syntax here;", "SELECT 2 AS y;"} {
			if c.feed(ln) {
				break
			}
		}
		c.flush()
		return out.String()
	}

	if off := run(false); !strings.Contains(off, `"x":1`) || !strings.Contains(off, `"y":2`) {
		t.Errorf("bail off should run both good statements; got %q", off)
	}
	on := run(true)
	if !strings.Contains(on, `"x":1`) {
		t.Errorf("bail on should still run the statement before the error; got %q", on)
	}
	if strings.Contains(on, `"y":2`) {
		t.Errorf("bail on should stop before the post-error statement; got %q", on)
	}
}
