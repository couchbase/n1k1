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
	"bytes"
	"strings"
	"testing"
)

// TestHelpReservedWordsLiveCheck (IDEA-0028): `.help reserved-words <name>...` checks
// each name against cbq's live parser -- a reserved keyword is flagged, an ordinary
// field name is cleared -- so an author sees it up front.
func TestHelpReservedWordsLiveCheck(t *testing.T) {
	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", mode: "jsonlines", out: &out, stderr: &errb}
	c.cmdHelp("reserved-words level keys msg node")

	s := errb.String()
	lines := map[string]string{}
	for _, ln := range strings.Split(s, "\n") {
		f := strings.Fields(ln)
		if len(f) >= 2 {
			lines[f[0]] = ln
		}
	}
	for _, r := range []string{"level", "keys"} {
		if !strings.Contains(lines[r], "RESERVED") {
			t.Errorf("%q should be flagged RESERVED; got %q", r, lines[r])
		}
	}
	for _, ok := range []string{"msg", "node"} {
		if l := lines[ok]; !strings.Contains(l, "ok") || strings.Contains(l, "RESERVED") {
			t.Errorf("%q should be cleared (ok); got %q", ok, l)
		}
	}
}

// TestHelpReservedWordsList (IDEA-0028): `.help reserved-words` with no arg prints the
// FULL reserved-word list -- an author reads it once instead of playing whack-a-mole.
func TestHelpReservedWordsList(t *testing.T) {
	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", mode: "jsonlines", out: &out, stderr: &errb}
	c.cmdHelp("reserved-words")

	s := errb.String()
	if !strings.Contains(s, "reserved words —") {
		t.Errorf("missing the count header; got:\n%s", s)
	}
	// A spread of real reserved keywords must appear in the printed grid.
	for _, w := range []string{"select", "where", "level", "keys", "groups", "window"} {
		if !strings.Contains(s, w) {
			t.Errorf(".help reserved-words list missing %q", w)
		}
	}
	// It's the full list, not just a handful of examples.
	if got := len(strings.Fields(s)); got < 200 {
		t.Errorf(".help reserved-words printed only %d tokens; want the full list", got)
	}
}

// TestHelpTopics: each deep-dive topic prints (no panic) with recognizable content,
// and an unknown topic is reported.
func TestHelpTopics(t *testing.T) {
	cases := map[string]string{
		"reserved-words": "reserved words",
		"quoting":        "single-quote",
		"keyspaces":      "keyspace",
		"meta":           "_meta",
		"temp-keyspaces": "CREATE",
	}
	for topic, want := range cases {
		var out, errb bytes.Buffer
		c := &cli{prog: "n1k1", mode: "jsonlines", out: &out, stderr: &errb}
		c.cmdHelp(topic)
		if !strings.Contains(errb.String(), want) {
			t.Errorf(".help %s missing %q; got:\n%s", topic, want, errb.String())
		}
	}

	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", mode: "jsonlines", out: &out, stderr: &errb}
	c.cmdHelp("bogus-topic")
	if !strings.Contains(errb.String(), "unknown help topic") {
		t.Errorf("unknown topic not reported; got:\n%s", errb.String())
	}
}
