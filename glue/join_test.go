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
