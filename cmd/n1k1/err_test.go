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

	"github.com/couchbase/n1k1/cmd"
)

func TestParseErrPos(t *testing.T) {
	cases := []struct {
		in     string
		wl, wc int
		ok     bool
	}{
		{"syntax error - line 1, column 10, near 'x', at: FRM", 1, 10, true},
		{"FROM expression term (near line 3, column 22) must have a name", 3, 22, true},
		{"... - line 12, column 8, near 'y'", 12, 8, true},
		{"syntax error - at end of input", 0, 0, false},
		{"Invalid function foo()", 0, 0, false},
	}
	for _, tc := range cases {
		l, c, ok := parseErrPos(tc.in)
		if ok != tc.ok || (ok && (l != tc.wl || c != tc.wc)) {
			t.Errorf("parseErrPos(%q) = %d,%d,%v want %d,%d,%v", tc.in, l, c, ok, tc.wl, tc.wc, tc.ok)
		}
	}
}

// caretIndex returns the 1-based rune column the caret points at (its offset
// within the target line, discounting the 2-space gutter), or -1 if absent.
func caretIndex(out string) int {
	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	if len(lines) < 2 {
		return -1
	}
	caret := lines[len(lines)-1]
	i := strings.IndexByte(caret, '^')
	if i < 0 {
		return -1
	}
	return i - len("  ") + 1 // strip the gutter, make 1-based
}

func TestErrorCaretColumn(t *testing.T) {
	plain := cmd.Style{On: false}
	cases := []struct {
		stmt, errText string
		wantCol       int
	}{
		{"SELECT * FRM t", "syntax error - line 1, column 10, near 'x', at: FRM", 10},
		{"SELECT COUNT(*) FROM 2026-01", "FROM expression term (near line 1, column 22) must have a name or alias", 22},
		{"SELECT 'abc FROM t", "syntax error: ... - line 1, column 8, near 'z'", 8},
	}
	for _, tc := range cases {
		out := errorCaret(tc.stmt, tc.errText, plain)
		if got := caretIndex(out); got != tc.wantCol {
			t.Errorf("stmt %q: caret at col %d, want %d\n%s", tc.stmt, got, tc.wantCol, out)
		}
		// The gutter'd statement line must be present verbatim (plain style).
		if !strings.Contains(out, "  "+tc.stmt) {
			t.Errorf("stmt %q: statement line missing from:\n%s", tc.stmt, out)
		}
	}
}

func TestErrorCaretEndOfInput(t *testing.T) {
	stmt := "SELECT * FROM t WHERE"
	out := errorCaret(stmt, "syntax error - at end of input", cmd.Style{})
	// Caret sits one past the last character (col == len+1).
	if got, want := caretIndex(out), len([]rune(stmt))+1; got != want {
		t.Errorf("end-of-input caret at %d, want %d\n%s", got, want, out)
	}
}

func TestErrorCaretMultiLineEndOfInput(t *testing.T) {
	stmt := "SELECT *\nFROM t\nWHERE a =="
	out := errorCaret(stmt, "syntax error - at end of input", cmd.Style{})
	// All statement lines are echoed, and the caret lands under the last one.
	for _, ln := range strings.Split(stmt, "\n") {
		if !strings.Contains(out, "  "+ln) {
			t.Errorf("missing line %q in:\n%s", ln, out)
		}
	}
	if got, want := caretIndex(out), len([]rune("WHERE a =="))+1; got != want {
		t.Errorf("multi-line caret at %d, want %d\n%s", got, want, out)
	}
}

func TestErrorCaretNoPosition(t *testing.T) {
	if s := errorCaret("SELECT 1+1", "some runtime failure, no position here", cmd.Style{}); s != "" {
		t.Errorf("expected no caret for a positionless error, got %q", s)
	}
}

// A tab before the error column is copied into the pad so the caret still aligns.
func TestErrorCaretTabAlignment(t *testing.T) {
	out := errorCaret("\tSELECT x", "syntax error - line 1, column 2, near 'z'", cmd.Style{})
	caret := strings.Split(strings.TrimRight(out, "\n"), "\n")[1]
	// gutter (2 spaces) + one tab copied through, then the caret.
	if !strings.HasPrefix(caret, "  \t^") {
		t.Errorf("tab not preserved in caret pad: %q", caret)
	}
}

func TestDottedKeyspaceHint(t *testing.T) {
	plain := cmd.Style{}
	names := []string{"events", "ns_server.error", "ns_server.debug"}

	// The unquoted dotted keyspace `ns_server.error` parses as a field path ->
	// "Ambiguous reference to field 'ns_server'"; a keyspace of that dotted name
	// exists, so hint to backtick it + note the shell quoting (IDEA-0010).
	got := dottedKeyspaceHint("Ambiguous reference to field 'ns_server' (near line 1, column 27).", names, plain)
	for _, want := range []string{"`ns_server.error`", "single quotes", "-f"} {
		if !strings.Contains(got, want) {
			t.Errorf("dottedKeyspaceHint missing %q; got %q", want, got)
		}
	}

	// No matching keyspace for the ambiguous field -> no hint (don't mislead).
	if h := dottedKeyspaceHint("Ambiguous reference to field 'foo' (near line 1, column 8).", names, plain); h != "" {
		t.Errorf("expected no hint for a non-keyspace field; got %q", h)
	}
	// A non-ambiguous error -> no hint.
	if h := dottedKeyspaceHint("syntax error - at end of input", names, plain); h != "" {
		t.Errorf("expected no hint for a non-ambiguous error; got %q", h)
	}
	// nil keyspace list -> no hint (nil-safe).
	if h := dottedKeyspaceHint("Ambiguous reference to field 'ns_server' (x).", nil, plain); h != "" {
		t.Errorf("expected no hint with nil names; got %q", h)
	}
}

func TestAmbiguousField(t *testing.T) {
	if f, ok := ambiguousField("Ambiguous reference to field 'ns_server' (near line 1, column 27)."); !ok || f != "ns_server" {
		t.Errorf("ambiguousField = (%q,%v), want (ns_server,true)", f, ok)
	}
	if _, ok := ambiguousField("some other error"); ok {
		t.Errorf("ambiguousField should not match a non-ambiguous error")
	}
}

func TestReservedWordHint(t *testing.T) {
	plain := cmd.Style{}
	cases := []struct {
		errText, wantSub string
	}{
		{"syntax error - line 1, column 21, near 'SELECT l.', at: level (reserved word)", "`level`"},
		{"syntax error - line 1, column 10, near 'x', at: FRM", ""}, // ordinary typo -> no hint
		{"syntax error - at end of input", ""},                      // no token
	}
	for _, c := range cases {
		got := reservedWordHint(c.errText, plain)
		if c.wantSub == "" {
			if got != "" {
				t.Errorf("reservedWordHint(%q) = %q, want \"\"", c.errText, got)
			}
			continue
		}
		if !strings.Contains(got, c.wantSub) || !strings.Contains(got, "reserved word") {
			t.Errorf("reservedWordHint(%q) = %q, want it to mention %q + \"reserved word\"", c.errText, got, c.wantSub)
		}
	}
}

// TestReservedWordDiscoverability covers ISSUE-10: the parse-error hint names the
// .help reserved-words topic, and a rejected-entry reason is augmented with the
// backtick fix + the per-word lookup (so lint/compose flag an `AS value` alias).
func TestReservedWordDiscoverability(t *testing.T) {
	const errText = "syntax error - line 1, column 13, near 'SELECT 1 AS ', at: value (reserved word)"

	if tok := reservedWordToken(errText); tok != "value" {
		t.Fatalf("reservedWordToken: got %q, want \"value\"", tok)
	}
	if tok := reservedWordToken("some other parse error"); tok != "" {
		t.Fatalf("reservedWordToken(non-reserved): got %q, want \"\"", tok)
	}

	hint := reservedWordHint(errText, cmd.Style{})
	for _, want := range []string{"reserved word", "`value`", ".help reserved-words value"} {
		if !strings.Contains(hint, want) {
			t.Fatalf("hint missing %q; got %q", want, hint)
		}
	}

	got := reservedWordReason(errText)
	for _, want := range []string{"backtick it", ".help reserved-words value"} {
		if !strings.Contains(got, want) {
			t.Fatalf("reason augment missing %q; got %q", want, got)
		}
	}
	// A non-reserved reason is unchanged.
	if r := reservedWordReason("plain reason"); r != "plain reason" {
		t.Fatalf("non-reserved reason changed: %q", r)
	}
}

// TestNoKeyspaceReason covers the unresolved-keyspace --bind pointer: a query over a
// LOGICAL keyspace run without --bind rejects with a bare "no keyspace sessions",
// which reads as "my query is wrong" rather than "the binding is missing" -- in a
// compose DAG this used to surface as an indistinguishable count:0.
func TestNoKeyspaceReason(t *testing.T) {
	const errText = "plan error: namespace default: no keyspace sessions"
	if ks := noKeyspaceToken(errText); ks != "sessions" {
		t.Fatalf("noKeyspaceToken: got %q, want \"sessions\"", ks)
	}
	if ks := noKeyspaceToken("some other plan error"); ks != "" {
		t.Fatalf("noKeyspaceToken(other): got %q, want \"\"", ks)
	}
	// The fork's file-datastore shape (a physical default/ exists, keyspace doesn't).
	if ks := noKeyspaceToken("plan error: Keyspace not found sessions"); ks != "sessions" {
		t.Fatalf("noKeyspaceToken(fork shape): got %q, want \"sessions\"", ks)
	}
	got := noKeyspaceReason(errText)
	for _, want := range []string{`"sessions"`, "--bind <manifest>", "sessions = <glob>", ".tables"} {
		if !strings.Contains(got, want) {
			t.Fatalf("reason augment missing %q; got %q", want, got)
		}
	}
	if r := noKeyspaceReason("plain reason"); r != "plain reason" {
		t.Fatalf("non-keyspace reason changed: %q", r)
	}
	// rejectReason chains both augmenters; either class of reason gets its hint.
	if r := rejectReason(errText); !strings.Contains(r, "--bind") {
		t.Fatalf("rejectReason missing bind hint: %q", r)
	}
}
