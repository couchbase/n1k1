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
	"strings"
	"testing"
)

// TestPrettySQLPreservesTokens is the safety invariant: PrettySQL only re-whitespaces,
// so the TOKEN SEQUENCE (whitespace dropped) must be identical before and after -- no
// token is reordered, dropped, added, or split/glued. This is what makes the output the
// SAME statement, only re-laid-out. It holds even for keywords/parens inside a string
// literal (the tokenizer keeps a quoted literal whole).
func TestPrettySQLPreservesTokens(t *testing.T) {
	for _, in := range []string{
		`SELECT * FROM logs WHERE sev = "ERROR"`,
		`SELECT sev, COUNT(*) AS n FROM logs WHERE msg LIKE "%panic%" GROUP BY sev ORDER BY n DESC LIMIT 10`,
		`SELECT g.msg FROM (SELECT gc.*, MAX(CASE WHEN x=1 THEN 1 ELSE 0 END) OVER (PARTITION BY id) AS hit FROM (SELECT src.* FROM logs src) AS gc) AS g WHERE g.hit = 1`,
		`SELECT a FROM t1 LEFT OUTER JOIN t2 ON t1.k = t2.k INNER JOIN t3 ON t2.k = t3.k`,
		`SELECT x FROM t WHERE msg = "select * from where group by ("`, // keywords/parens INSIDE a string
		`SELECT n WHERE a >= 1 AND b != 2 OR c <= 3`,
		`SELECT RAW t.v FROM t UNION ALL SELECT RAW u.v FROM u`,
	} {
		got := PrettySQL(in)
		wantToks := tokenizeSQL(in)
		gotToks := tokenizeSQL(got)
		if !equalTokens(wantToks, gotToks) {
			t.Errorf("PrettySQL changed the token sequence (not just whitespace):\n in:  %q\n out: %q\n in-toks:  %v\n out-toks: %v",
				in, got, wantToks, gotToks)
		}
	}
}

func equalTokens(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestPrettySQLBreaksClauses checks the readability behavior: top-level clauses each
// start their own line, a subquery paren indents its body, and inline parens
// (function calls, value lists) do NOT break.
func TestPrettySQLBreaksClauses(t *testing.T) {
	got := PrettySQL(`SELECT sev, COUNT(*) AS n FROM logs WHERE sev = "ERROR" GROUP BY sev ORDER BY n`)
	lines := strings.Split(got, "\n")
	// Each of these clause keywords leads a line (after trimming indentation).
	leads := map[string]bool{}
	for _, ln := range lines {
		fields := strings.Fields(ln)
		if len(fields) > 0 {
			leads[strings.ToUpper(fields[0])] = true
		}
	}
	for _, kw := range []string{"SELECT", "FROM", "WHERE", "GROUP", "ORDER"} {
		if !leads[kw] {
			t.Errorf("expected %s to lead a line; got:\n%s", kw, got)
		}
	}
	// COUNT(*) stays inline (its paren did not break the line).
	if !strings.Contains(got, "COUNT(*)") {
		t.Errorf("function-call paren should stay inline; got:\n%s", got)
	}
	// "GROUP BY" stays together (BY does not break onto its own line).
	if !strings.Contains(got, "GROUP BY") {
		t.Errorf("GROUP BY should stay on one line; got:\n%s", got)
	}
}

// TestPrettySQLIndentsSubquery: a `( SELECT ... )` subquery has its body indented
// deeper than the enclosing SELECT, so nesting is visible (the macro-expansion win).
func TestPrettySQLIndentsSubquery(t *testing.T) {
	got := PrettySQL(`SELECT g.msg FROM (SELECT src.msg FROM logs src WHERE src.sev = "ERROR") AS g`)
	lines := strings.Split(got, "\n")
	var outerSelect, innerSelect int = -1, -1
	depth := func(s string) int { return len(s) - len(strings.TrimLeft(s, " ")) }
	seenSelect := 0
	for _, ln := range lines {
		if strings.HasPrefix(strings.TrimLeft(ln, " "), "SELECT") {
			if seenSelect == 0 {
				outerSelect = depth(ln)
			} else {
				innerSelect = depth(ln)
			}
			seenSelect++
		}
	}
	if seenSelect < 2 {
		t.Fatalf("expected two SELECTs on their own lines; got:\n%s", got)
	}
	if !(innerSelect > outerSelect) {
		t.Errorf("subquery SELECT (indent %d) should be deeper than outer SELECT (indent %d); got:\n%s",
			innerSelect, outerSelect, got)
	}
}
