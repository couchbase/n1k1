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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
)

// TestJoinBandPredicateNotDropped is a regression for two silent-wrong-result bugs
// (found via the grep -C "context lines" pattern -- a band self-join on a line
// ordinal) where a non-equi (band) join predicate was DROPPED, so the join
// over-returned every row instead of the ±k window:
//
//	(A) a HASH join (an equi key present) applied only its equi key and ignored the
//	    residual band term living in the ON clause (VisitHashJoin's inner fast path).
//	(B) a comma/cross join carried the band in the NLJoin Filter() (nil ON clause),
//	    which VisitNLJoin never read.
//
// This is invisible to the interpreter/compiler differential (both n1k1 lanes share
// the same conversion, so both drop it identically), so it needs a result-level
// oracle: over 8 ordered lines with a single "hit" at n=3, grep -C1 must yield
// exactly n = 2,3,4 -- regardless of how the planner shapes the join.
func TestJoinBandPredicateNotDropped(t *testing.T) {
	dir := t.TempDir()
	ks := filepath.Join(dir, "default", "lines")
	if err := os.MkdirAll(ks, 0o755); err != nil {
		t.Fatal(err)
	}
	var body []byte
	for i := 0; i < 8; i++ {
		body = append(body, []byte(fmt.Sprintf(`{"n":%d,"g":"x","hit":%t}`+"\n", i, i == 3))...)
	}
	if err := os.WriteFile(filepath.Join(ks, "l.jsonl"), body, 0o644); err != nil {
		t.Fatal(err)
	}

	sess, err := OpenSession(dir, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}

	posOf := func(q string) []int {
		res, err := sess.Run(q)
		if err != nil {
			t.Fatalf("Run %q: %v", q, err)
		}
		var out []int
		for _, row := range res.Rows {
			var m struct {
				Pos int `json:"pos"`
			}
			if err := json.Unmarshal(row, &m); err != nil {
				t.Fatalf("decode %q from %q: %v", row, q, err)
			}
			out = append(out, m.Pos)
		}
		sort.Ints(out)
		return out
	}

	want := []int{2, 3, 4} // grep -C1 around the single hit at n=3

	cases := []struct{ name, q string }{
		// Already-correct forms (NL join with the band as the ON clause) -- guard
		// against regressing them.
		{"inequalities-in-ON",
			`SELECT ctx.n AS pos FROM lines m JOIN lines ctx ` +
				`ON ctx.n >= m.n - 1 AND ctx.n <= m.n + 1 WHERE m.hit`},
		{"between-in-ON",
			`SELECT ctx.n AS pos FROM lines m JOIN lines ctx ` +
				`ON ctx.n BETWEEN m.n - 1 AND m.n + 1 WHERE m.hit`},
		// Bug A: an equi key (g = g) makes the planner hash-join; the residual band
		// term in the ON must still be applied.
		{"hash-join-residual",
			`SELECT ctx.n AS pos FROM lines m JOIN lines ctx ` +
				`ON ctx.n >= m.n - 1 AND ctx.n <= m.n + 1 AND ctx.g = m.g WHERE m.hit`},
		// Bug B: a comma/cross join with the band in WHERE (NLJoin Filter()).
		{"comma-where-band",
			`SELECT ctx.n AS pos FROM lines m, lines ctx ` +
				`WHERE m.hit AND ctx.n >= m.n - 1 AND ctx.n <= m.n + 1`},
	}
	for _, c := range cases {
		got := posOf(c.q)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s: got pos %v, want %v (band predicate dropped?)\n  %s", c.name, got, want, c.q)
		}
	}
}

// TestJoinLateral exercises LATERAL joins (and a comma-join whose right is a correlated
// subquery): the inner subquery references the outer row, so it runs per outer row with
// that row in scope via the glue join-lateral op (VisitNLJoin -> JoinLateralOp). Covers
// INNER, LEFT OUTER (NULL-extension), a multi-row inner + an outer ON filter, and a
// correlated aggregate. Before this, the planner produced a correlated plan.ExpressionScan
// that VisitExpressionScan NA'd ("unsupported"). See TODO.md / DESIGN-data.md.
func TestJoinLateral(t *testing.T) {
	dir := t.TempDir()
	write := func(ks string, docs ...string) {
		d := filepath.Join(dir, "default", ks)
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
		for i, doc := range docs {
			if err := os.WriteFile(filepath.Join(d, fmt.Sprintf("r%d.json", i)), []byte(doc), 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	write("a", `{"k":1}`, `{"k":2}`)
	write("b", `{"k":2,"bv":"b2"}`, `{"k":3,"bv":"b3"}`)

	sess, err := OpenSession(dir, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	defer sess.Close()

	// rowsOf runs q and returns each row as canonical JSON (sorted keys), sorted -- so
	// assertions don't depend on row or field order.
	rowsOf := func(q string) []string {
		res, err := sess.Run(q)
		if err != nil {
			t.Fatalf("Run %q: %v", q, err)
		}
		var out []string
		for _, row := range res.Rows {
			var v interface{}
			if err := json.Unmarshal(row, &v); err != nil {
				t.Fatalf("decode %q from %q: %v", row, q, err)
			}
			b, _ := json.Marshal(v)
			out = append(out, string(b))
		}
		sort.Strings(out)
		return out
	}

	cases := []struct {
		name string
		q    string
		want []string
	}{
		{"inner-comma",
			`SELECT x.k AS xk, y.bk AS ybk FROM a AS x, ` +
				`LATERAL (SELECT b.k AS bk FROM b WHERE b.k = x.k) AS y`,
			[]string{`{"xk":2,"ybk":2}`}},
		{"inner-join-on-true",
			`SELECT x.k AS xk, y.bk AS ybk FROM a AS x ` +
				`JOIN LATERAL (SELECT b.k AS bk FROM b WHERE b.k = x.k) AS y ON TRUE`,
			[]string{`{"xk":2,"ybk":2}`}},
		{"left-outer-null-extends",
			`SELECT x.k AS xk, y.bk AS ybk FROM a AS x ` +
				`LEFT JOIN LATERAL (SELECT b.k AS bk FROM b WHERE b.k = x.k) AS y ON TRUE`,
			[]string{`{"xk":1}`, `{"xk":2,"ybk":2}`}}, // x=1 has no match -> ybk MISSING (omitted).
		{"multi-row-plus-on-filter",
			`SELECT x.k AS xk, y.bk AS ybk FROM a AS x ` +
				`JOIN LATERAL (SELECT b.k AS bk FROM b WHERE b.k >= x.k) AS y ON y.bk = 2`,
			[]string{`{"xk":1,"ybk":2}`, `{"xk":2,"ybk":2}`}},
		{"correlated-aggregate",
			`SELECT x.k AS xk, y.n AS n FROM a AS x, ` +
				`LATERAL (SELECT COUNT(*) AS n FROM b WHERE b.k >= x.k) AS y`,
			[]string{`{"n":2,"xk":1}`, `{"n":2,"xk":2}`}},
	}
	for _, c := range cases {
		got := rowsOf(c.q)
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s:\n  got  %v\n  want %v\n  %s", c.name, got, c.want, c.q)
		}
	}
}

// TestSubqueryEmptyArray pins the N1QL rule that a subquery evaluates to an ARRAY, so
// an EMPTY subquery is the empty array [] -- NOT null (EvaluateSubquery). Regression for
// a CTE-as-datasource bug: `FROM <empty cte>` used to yield one spurious {} row because
// the empty subquery came back as NULL and a FROM expr-scan can't iterate a non-array,
// so it fell back to emitting the value as a single row. Also checks the array-valued
// contexts (ARRAY_LENGTH / IN) the same NULL-vs-[] bug affected.
func TestSubqueryEmptyArray(t *testing.T) {
	dir := t.TempDir()
	d := filepath.Join(dir, "default", "b")
	if err := os.MkdirAll(d, 0o755); err != nil {
		t.Fatal(err)
	}
	for i, doc := range []string{`{"k":2}`, `{"k":3}`} {
		if err := os.WriteFile(filepath.Join(d, fmt.Sprintf("r%d.json", i)), []byte(doc), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	sess, err := OpenSession(dir, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	defer sess.Close()

	rows := func(q string) []string {
		res, err := sess.Run(q)
		if err != nil {
			t.Fatalf("Run %q: %v", q, err)
		}
		var out []string // nil when there are no rows (matches a nil `want`).
		for _, r := range res.Rows {
			out = append(out, string(r))
		}
		sort.Strings(out)
		return out
	}
	eq := func(name string, got, want []string) {
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s:\n  got  %v\n  want %v", name, got, want)
		}
	}

	// The bug: an empty CTE used as a datasource must yield ZERO rows, not one {} row.
	eq("empty-cte-from",
		rows(`WITH c AS (SELECT b.k FROM b WHERE b.k = 99) SELECT c.k FROM c`),
		nil)
	// A non-empty CTE datasource is unaffected.
	eq("nonempty-cte-from",
		rows(`WITH c AS (SELECT b.k FROM b WHERE b.k = 2) SELECT c.k FROM c`),
		[]string{`{"k":2}`})
	// An empty subquery is [] (length 0), and a non-empty one its real length.
	eq("array-length-empty",
		rows(`SELECT ARRAY_LENGTH((SELECT b.k FROM b WHERE b.k = 99)) AS n`),
		[]string{`{"n":0}`})
	eq("array-length-nonempty",
		rows(`SELECT ARRAY_LENGTH((SELECT b.k FROM b)) AS n`),
		[]string{`{"n":2}`})
	// x IN (empty subquery) is false, so the WHERE keeps no rows.
	eq("in-empty-subquery",
		rows(`SELECT b.k FROM b WHERE b.k IN (SELECT RAW b2.k FROM b b2 WHERE b2.k = 99)`),
		nil)
}

// TestJoinNest exercises NEST (collect matching right rows into an array under the rhs
// alias). Covers the three op kinds -- ANSI nested-loop (nestNL, non-equi ON), HASH nest
// (nestNL fallback via VisitHashNest for an equi ON), and lookup ON KEYS (nestKeys) --
// each inner (drops a no-match left row) and LEFT OUTER (keeps it with an empty array
// []), matching cbq execution/nest_nl.go processAnsiNest. Before this, all NEST plan
// operators NA'd ("unsupported").
func TestJoinNest(t *testing.T) {
	dir := t.TempDir()
	write := func(ks string, docs ...string) {
		d := filepath.Join(dir, "default", ks)
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
		for i, doc := range docs {
			p := filepath.Join(d, fmt.Sprintf("r%d.json", i))
			// key-named files for the ON KEYS keyspace (doc = `key:body`).
			if k, body, ok := splitKeyDoc(doc); ok {
				p = filepath.Join(d, k+".json")
				doc = body
			}
			if err := os.WriteFile(p, []byte(doc), 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	write("a", `{"k":1}`, `{"k":2}`)
	write("b", `{"k":2,"bv":"b2"}`, `{"k":3,"bv":"b3"}`)
	// ON KEYS fixtures: `o` rows carry the rhs keys; `d` rows are keyed by file stem.
	write("o", `{"nm":"alice","ks":["d1","d2"]}`, `{"nm":"bob","ks":["d3"]}`, `{"nm":"carol","ks":[]}`)
	write("d", `d1:{"v":"D1"}`, `d2:{"v":"D2"}`, `d3:{"v":"D3"}`)

	sess, err := OpenSession(dir, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	defer sess.Close()

	rows := func(q string) []string {
		res, err := sess.Run(q)
		if err != nil {
			t.Fatalf("Run %q: %v", q, err)
		}
		var out []string
		for _, r := range res.Rows {
			var v interface{}
			if err := json.Unmarshal(r, &v); err != nil {
				t.Fatalf("decode %q: %v", r, err)
			}
			b, _ := json.Marshal(v) // canonical (sorted keys)
			out = append(out, string(b))
		}
		sort.Strings(out)
		return out
	}
	eq := func(name, q string, want []string) {
		if got := rows(q); !reflect.DeepEqual(got, want) {
			t.Errorf("%s:\n  got  %v\n  want %v\n  %s", name, got, want, q)
		}
	}

	// ANSI equi (planner -> HashNest -> NL fallback): inner drops a=1 (no b.k==1).
	eq("ansi-equi-inner", `SELECT a.k AS ak, ARRAY_LENGTH(b) AS n FROM a NEST b ON a.k = b.k`,
		[]string{`{"ak":2,"n":1}`})
	// ...LEFT OUTER keeps a=1 with an empty array.
	eq("ansi-equi-leftouter", `SELECT a.k AS ak, ARRAY_LENGTH(b) AS n FROM a LEFT OUTER NEST b ON a.k = b.k`,
		[]string{`{"ak":1,"n":0}`, `{"ak":2,"n":1}`})
	// ANSI non-equi (nestNL): each a nests both b rows (b.k >= a.k holds for k in {2,3}).
	eq("ansi-noneq", `SELECT a.k AS ak, ARRAY_LENGTH(b) AS n FROM a NEST b ON b.k >= a.k`,
		[]string{`{"ak":1,"n":2}`, `{"ak":2,"n":2}`})
	// The nested value is the full rhs doc.
	eq("ansi-doc", `SELECT a.k AS ak, b AS bs FROM a NEST b ON a.k = b.k`,
		[]string{`{"ak":2,"bs":[{"bv":"b2","k":2}]}`})
	// Lookup ON KEYS (nestKeys): inner drops carol (empty keys).
	eq("keys-inner", `SELECT o.nm AS nm, ARRAY_LENGTH(d) AS n FROM o NEST d ON KEYS o.ks`,
		[]string{`{"n":1,"nm":"bob"}`, `{"n":2,"nm":"alice"}`})
	// ...LEFT OUTER keeps carol with an empty array.
	eq("keys-leftouter", `SELECT o.nm AS nm, ARRAY_LENGTH(d) AS n FROM o LEFT OUTER NEST d ON KEYS o.ks`,
		[]string{`{"n":0,"nm":"carol"}`, `{"n":1,"nm":"bob"}`, `{"n":2,"nm":"alice"}`})
}

// splitKeyDoc splits a `key:body` test fixture (used to name a keyspace file by its
// document key) into (key, body, true); a plain doc returns ok=false.
func splitKeyDoc(s string) (key, body string, ok bool) {
	if len(s) > 0 && s[0] == '{' {
		return "", "", false
	}
	if i := indexByteASCII(s, ':'); i > 0 {
		return s[:i], s[i+1:], true
	}
	return "", "", false
}

func indexByteASCII(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}
