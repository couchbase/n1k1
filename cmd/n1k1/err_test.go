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

func TestReservedWordHint(t *testing.T) {
	plain := cmd.Style{}
	cases := []struct {
		errText, wantSub string
	}{
		{"syntax error - line 1, column 21, near 'SELECT l.', at: level (reserved word)", "`level`"},
		{"syntax error - line 1, column 10, near 'x', at: FRM", ""},        // ordinary typo -> no hint
		{"syntax error - at end of input", ""},                            // no token
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
