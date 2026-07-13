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

import "strings"

// PrettySQL re-whitespaces a SQL++ statement into an indented, multi-line form so a
// deeply-nested / gensym-heavy statement (e.g. a macro expansion, or a fused corpus
// query) reads as a plan instead of one long line. It is a VIEW, not a rewrite: it
// only inserts/removes WHITESPACE around a quote-aware token stream -- it never
// reorders, drops, or adds tokens -- so the result is the same statement (SQL++ is
// whitespace-insensitive outside string/identifier literals, which the tokenizer keeps
// whole). It breaks a new line before each top-level clause keyword (SELECT / FROM /
// WHERE / GROUP / ORDER / ...) and indents the body of a subquery `( SELECT ... )`;
// function-call and value-list parens stay inline. It makes no attempt to be a faithful
// formatter for every construct -- unknown tokens simply pass through space-joined.
func PrettySQL(sql string) string {
	toks := tokenizeSQL(sql)
	if len(toks) == 0 {
		return strings.TrimSpace(sql)
	}

	var b strings.Builder
	indent := 0
	atLineStart := true   // true right after a newline (or at the very start): no leading space
	var parenIsSub []bool // per open paren: true when it opened a subquery (line-broken)
	prev := ""            // previous emitted token, lower-cased (for join-chain / spacing)

	writeIndent := func() { b.WriteString(strings.Repeat("  ", indent)) }
	newline := func() {
		b.WriteByte('\n')
		writeIndent()
		atLineStart = true
	}

	for i := 0; i < len(toks); i++ {
		t := toks[i]
		lower := strings.ToLower(t)

		// At statement level (not inside an inline function/value-list paren) a clause
		// keyword starts a fresh line -- the primary readability win.
		if !atLineStart && atClauseLevel(parenIsSub) && isClauseBreak(lower, prev) {
			newline()
		}

		switch {
		case t == "(":
			// Subquery paren iff the next significant token opens a SELECT-family
			// statement; then its body is indented on its own lines (and the `(`
			// keeps a leading space, e.g. "FROM ("). A function-call / IN-list /
			// grouping paren stays inline and attaches TIGHTLY to the preceding token
			// (e.g. "COUNT(*)").
			sub := i+1 < len(toks) && isSubqueryStart(strings.ToLower(toks[i+1]))
			parenIsSub = append(parenIsSub, sub)
			if sub {
				writeSpaced(&b, "(", atLineStart, prev)
				indent++
				newline()
			} else {
				b.WriteString("(")
				atLineStart = false
			}
		case t == ")":
			sub := false
			if n := len(parenIsSub); n > 0 {
				sub = parenIsSub[n-1]
				parenIsSub = parenIsSub[:n-1]
			}
			if sub {
				indent--
				newline()
			}
			writeSpaced(&b, ")", atLineStart, prev)
			atLineStart = false
		default:
			writeSpaced(&b, t, atLineStart, prev)
			atLineStart = false
		}
		prev = lower
	}
	return b.String()
}

// atClauseLevel reports whether we are at statement level -- either the outermost scope
// or directly inside a subquery paren (NOT inside an inline function-call / value-list
// paren, where a "clause keyword" is really a function arg / CASE branch and must not
// break).
func atClauseLevel(parenIsSub []bool) bool {
	if n := len(parenIsSub); n > 0 {
		return parenIsSub[n-1]
	}
	return true
}

// clauseBreakKW is the set of keywords that begin a new top-level line. GROUP/ORDER
// break but the following BY does not (handled in isClauseBreak). The join family is
// handled by joinModifier so a multi-word join (LEFT OUTER JOIN) stays on one line.
var clauseBreakKW = map[string]bool{
	"select": true, "from": true, "where": true, "group": true, "order": true,
	"having": true, "limit": true, "offset": true, "let": true, "letting": true,
	"union": true, "intersect": true, "except": true, "with": true, "nest": true,
	"unnest": true, "on": true, "join": true, "left": true, "right": true,
	"inner": true, "full": true, "cross": true,
}

// joinModifier is the set of tokens that chain within a single join clause; a
// clause-break token whose predecessor is one of these is mid-clause, so it does NOT
// start a new line (keeps "LEFT OUTER JOIN" / "INNER JOIN" together).
var joinModifier = map[string]bool{
	"left": true, "right": true, "inner": true, "full": true, "cross": true, "outer": true,
}

// isClauseBreak reports whether token `lower` should start a new line, given the
// previous emitted token `prev`.
func isClauseBreak(lower, prev string) bool {
	if !clauseBreakKW[lower] {
		return false
	}
	// "GROUP BY" / "ORDER BY": break before GROUP/ORDER, never before BY (BY is not a
	// clause-break keyword, so it never reaches here) -- and never break a join word
	// that continues a join chain (its predecessor is a join modifier).
	if joinModifier[prev] {
		return false
	}
	// "UNION ALL": ALL is not a break keyword, so it stays with UNION. A bare SELECT
	// right after UNION (set-op arm) still breaks -- desired.
	return true
}

// isSubqueryStart reports whether a `(` immediately followed by this token opens a
// subquery whose body should be line-broken and indented.
func isSubqueryStart(lower string) bool {
	return lower == "select" || lower == "with"
}

// writeSpaced appends tok, inserting a single separating space UNLESS we are at a line
// start or a punctuation rule forbids it (no space before , ) . ; and no space after
// ( . -- so member access `a.b`, calls `f(x)`, and `(1,2)` render tightly).
func writeSpaced(b *strings.Builder, tok string, atLineStart bool, prev string) {
	if !atLineStart && needSpaceBetween(prev, tok) {
		b.WriteByte(' ')
	}
	b.WriteString(tok)
}

// needSpaceBetween decides whether a space goes between prev and tok. prev is the
// lower-cased previous token; tok is the raw next token. Empty prev (start) -> no space.
func needSpaceBetween(prev, tok string) bool {
	if prev == "" {
		return false
	}
	switch tok {
	case ",", ")", ".", ";":
		return false
	}
	switch prev {
	case "(", ".":
		return false
	}
	return true
}

// tokenizeSQL splits a SQL++ string into tokens, keeping each quoted literal ('...',
// "...", `...`, with doubled-quote escapes) WHOLE so a keyword or paren inside a string
// is never mistaken for structure. Words (identifiers/keywords/numbers) are maximal
// runs of word characters; a run of operator characters is one token (so `>=`, `||`
// stay intact); every other non-space rune ( ( ) , . ; ) is its own token. Whitespace
// separates and is dropped (re-inserted by the emitter).
func tokenizeSQL(s string) []string {
	var toks []string
	r := []rune(s)
	n := len(r)
	for i := 0; i < n; {
		c := r[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '\'' || c == '"' || c == '`':
			// A quoted literal: consume to the matching close, treating a doubled
			// quote (''/""/``) as an escaped quote that stays inside the literal.
			q := c
			j := i + 1
			for j < n {
				if r[j] == q {
					if j+1 < n && r[j+1] == q { // doubled -> escaped, skip both
						j += 2
						continue
					}
					j++ // closing quote
					break
				}
				j++
			}
			toks = append(toks, string(r[i:j]))
			i = j
		case isWordRune(c):
			j := i + 1
			for j < n && isWordRune(r[j]) {
				j++
			}
			toks = append(toks, string(r[i:j]))
			i = j
		case isOpRune(c):
			j := i + 1
			for j < n && isOpRune(r[j]) {
				j++
			}
			toks = append(toks, string(r[i:j]))
			i = j
		default:
			toks = append(toks, string(c))
			i++
		}
	}
	return toks
}

func isWordRune(c rune) bool {
	return c == '_' || c == '$' || c == '@' ||
		(c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

// isOpRune matches the characters that make up multi-char operators (so `>=`, `<=`,
// `!=`, `||`, `==`) stay as one token. `.` `,` `(` `)` `;` are structural and handled
// separately, so they are NOT operator runes.
func isOpRune(c rune) bool {
	switch c {
	case '+', '-', '*', '/', '%', '<', '>', '=', '!', '|', '&', '^', '~', ':':
		return true
	}
	return false
}
