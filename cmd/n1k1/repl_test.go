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
	"strings"
	"testing"
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
