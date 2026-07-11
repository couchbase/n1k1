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
	"strconv"
	"strings"

	"github.com/couchbase/n1k1/cmd"
)

// The N1QL parser embeds a 1-based "line L, column C" position in its error text.
// Both the syntax-error form ("... - line 1, column 10, near 'SELECT * ', at:
// FRM") and the semantic form ("... (near line 1, column 22) ...") contain that
// exact substring, so parseErrPos below finds either.

// errorCaret renders the offending statement with a caret pointing at the column
// the parser flagged, e.g.
//
//	SELECT * FRM t
//	         ^
//
// It returns "" when the error carries no usable position (e.g. errors that only
// say "at end of input", or a position past the text), so the caller can just
// skip the decoration. Coloring follows st and is a no-op when st.On is false
// (non-TTY / NO_COLOR). The trailing newline is included when non-empty.
func errorCaret(stmt, errText string, st cmd.Style) string {
	lines := strings.Split(stmt, "\n")
	line, col, ok := parseErrPos(errText)
	if !ok {
		// The parser reports incomplete input as "at end of input" with no
		// position; point the caret just past the last non-blank line.
		if !strings.Contains(errText, "end of input") {
			return ""
		}
		line = len(lines)
		for line > 1 && strings.TrimSpace(lines[line-1]) == "" {
			line--
		}
		col = len([]rune(lines[line-1])) + 1
	}
	if line < 1 || line > len(lines) {
		return ""
	}
	target := []rune(lines[line-1])
	// Column is 1-based over runes; clamp to one-past-the-end so a caret can sit
	// just after the final character.
	if col < 1 {
		col = 1
	}
	if col > len(target)+1 {
		col = len(target) + 1
	}

	const indent = "  "
	var b strings.Builder
	for i, ln := range lines {
		b.WriteString(indent)
		if i == line-1 {
			b.WriteString(highlightRune(ln, col-1, st))
		} else {
			b.WriteString(st.Dim(ln))
		}
		b.WriteByte('\n')
	}
	b.WriteString(indent)
	b.WriteString(caretPad(target, col-1))
	b.WriteString(st.Red(st.Bold("^")))
	b.WriteByte('\n')
	return b.String()
}

// reservedWordHint returns a one-line hint when a syntax error is a bare use of a
// reserved word where an identifier was meant. The N1QL parser appends
// " (reserved word)" to the offending token (n1ql.go), e.g.
//
//	syntax error - line 1, column 21, near '...', at: level (reserved word)
//
// This bites naturally: n1k1's built-in log recipe emits a `level` field, but
// `level` is reserved (ISOLATION LEVEL), so `WHERE l.level = "error"` fails to
// parse -- the fix is to backtick it. Returns "" when the error isn't a
// reserved-word case. Coloring follows st (no-op when st.On is false).
func reservedWordHint(errText string, st cmd.Style) string {
	const marker = " (reserved word)"
	i := strings.Index(errText, marker)
	if i < 0 {
		return ""
	}
	// The reserved token is the last "at: <TOKEN>" segment before the marker.
	before := errText[:i]
	at := strings.LastIndex(before, "at: ")
	if at < 0 {
		return ""
	}
	tok := strings.TrimSpace(before[at+len("at: "):])
	if tok == "" {
		return ""
	}
	return "  " + st.Dim("hint: "+tok+" is a reserved word here — quote it as `"+tok+"`") + "\n"
}

// dottedKeyspaceHint returns a one-line hint when a statement referenced a dotted
// keyspace (e.g. `ns_server.error`, the norm in a bundle) WITHOUT backticks, so the
// parser read it as a field path -- "Ambiguous reference to field 'ns_server'" -- yet a
// keyspace of that dotted name exists. It suggests backticking it, and (IDEA-0010) the
// shell-safe way to keep the backticks (single-quote the -c arg, or use -f), since
// backticks are command-substitution inside "double quotes". Returns "" when it doesn't
// apply. names is the datastore's keyspace list (nil-safe).
func dottedKeyspaceHint(errText string, names []string, st cmd.Style) string {
	field, ok := ambiguousField(errText)
	if !ok || field == "" {
		return ""
	}
	// A dotted keyspace whose FIRST segment is the ambiguous field is the culprit.
	for _, n := range names {
		if strings.HasPrefix(n, field+".") {
			return "  " + st.Dim("hint: `"+n+"` is a dotted keyspace name — quote it: FROM `"+n+"`. "+
				"In a shell wrap the -c arg in 'single quotes' (backticks are command-substitution "+
				"in \"double quotes\"), or use -f <file>.") + "\n"
		}
	}
	return ""
}

// ambiguousField extracts X from a `Ambiguous reference to field 'X'` parser error
// (what an unquoted dotted keyspace `X.y` produces), or ("", false).
func ambiguousField(errText string) (string, bool) {
	const marker = "Ambiguous reference to field '"
	i := strings.Index(errText, marker)
	if i < 0 {
		return "", false
	}
	rest := errText[i+len(marker):]
	j := strings.IndexByte(rest, '\'')
	if j <= 0 {
		return "", false
	}
	return rest[:j], true
}

// parseErrPos extracts the 1-based (line, column) from a parser error message.
func parseErrPos(errText string) (line, col int, ok bool) {
	// Find "line <n>, column <n>" without a regexp: locate "line " that is
	// followed by "..., column ...".
	const lineKey, colKey = "line ", ", column "
	i := strings.Index(errText, lineKey)
	for i >= 0 {
		rest := errText[i+len(lineKey):]
		l, n := leadingInt(rest)
		if n > 0 && strings.HasPrefix(rest[n:], colKey) {
			c, m := leadingInt(rest[n+len(colKey):])
			if m > 0 {
				return l, c, true
			}
		}
		next := strings.Index(errText[i+1:], lineKey)
		if next < 0 {
			break
		}
		i = i + 1 + next
	}
	return 0, 0, false
}

// leadingInt reads a run of leading ASCII digits, returning the value and the
// number of bytes consumed (0 if s doesn't start with a digit).
func leadingInt(s string) (val, n int) {
	for n < len(s) && s[n] >= '0' && s[n] <= '9' {
		n++
	}
	if n == 0 {
		return 0, 0
	}
	v, _ := strconv.Atoi(s[:n])
	return v, n
}

// highlightRune returns line with the rune at idx (0-based) drawn red+bold and
// the rest dimmed; if idx is out of range the whole line is just dimmed.
func highlightRune(line string, idx int, st cmd.Style) string {
	runes := []rune(line)
	if idx < 0 || idx >= len(runes) {
		return st.Dim(line)
	}
	return st.Dim(string(runes[:idx])) +
		st.Red(st.Bold(string(runes[idx]))) +
		st.Dim(string(runes[idx+1:]))
}

// caretPad builds the whitespace that positions a caret under rune index idx of
// line, preserving tabs (copied through) so the caret aligns regardless of tab
// width; every other rune becomes a single space.
func caretPad(line []rune, idx int) string {
	if idx > len(line) {
		idx = len(line)
	}
	var b strings.Builder
	for i := 0; i < idx; i++ {
		if line[i] == '\t' {
			b.WriteByte('\t')
		} else {
			b.WriteByte(' ')
		}
	}
	return b.String()
}
