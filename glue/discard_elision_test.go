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
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/couchbase/n1k1/base"
)

// countGroup builds a value-agnostic group op (empty GROUP BY, count(*) star
// operand projected as the ["json","true"] constant) over the given child.
func countGroup(child *base.Op) *base.Op {
	return &base.Op{
		Kind:   "group",
		Labels: base.Labels{"^aggregates|count(*)"},
		Params: []interface{}{
			[]interface{}{}, // groups: no GROUP BY
			[]interface{}{[]interface{}{"json", "true"}}, // aggExprs: count(*) constant
			[]interface{}{[]interface{}{"count"}},        // aggCalcs
		},
		Children: []*base.Op{child},
	}
}

func proj(child *base.Op) *base.Op {
	return &base.Op{Kind: "project", Labels: base.Labels{".[\"x\"]"},
		Params: []interface{}{[]interface{}{"exprTree", nil}}, Children: []*base.Op{child}}
}

// TestElideDiscardedPass checks the pass splices dead project chains under a
// value-agnostic group, and leaves everything else untouched.
func TestElideDiscardedPass(t *testing.T) {
	// group(count*) -> project -> project -> scan   ==> both projects spliced.
	scan := &base.Op{Kind: "datastore-scan-index"}
	g := countGroup(proj(proj(scan)))
	elideDiscarded(g)
	if len(g.Children) != 1 || g.Children[0] != scan {
		t.Fatalf("count(*) group: projects not spliced; child kind = %q", g.Children[0].Kind)
	}

	// A GROUP BY key makes the group read input -> do NOT splice.
	scan2 := &base.Op{Kind: "datastore-scan-index"}
	gk := countGroup(proj(scan2))
	gk.Params[0] = []interface{}{[]interface{}{"exprTree", nil}} // one group key
	elideDiscarded(gk)
	if gk.Children[0].Kind != "project" {
		t.Errorf("GROUP BY present: project should be kept, got %q", gk.Children[0].Kind)
	}

	// COUNT(x) (an exprTree operand reads the row) -> do NOT splice.
	scan3 := &base.Op{Kind: "datastore-scan-index"}
	gx := countGroup(proj(scan3))
	gx.Params[1] = []interface{}{[]interface{}{"exprTree", nil}} // count(x)
	elideDiscarded(gx)
	if gx.Children[0].Kind != "project" {
		t.Errorf("COUNT(x): project should be kept, got %q", gx.Children[0].Kind)
	}

	// A filter between the group and its projects is NOT spliced (it changes the
	// row count that count(*) counts); the walk stops at it.
	scan4 := &base.Op{Kind: "datastore-scan-index"}
	filt := &base.Op{Kind: "filter", Children: []*base.Op{scan4}}
	gf := countGroup(filt)
	elideDiscarded(gf)
	if gf.Children[0].Kind != "filter" {
		t.Errorf("filter child should be kept, got %q", gf.Children[0].Kind)
	}
}

// runRows runs a statement and returns its result rows as a sorted []string
// (set-equality, order-insensitive).
func runRows(t *testing.T, sess *Session, stmt string) []string {
	t.Helper()
	res, err := sess.Run(stmt)
	if err != nil {
		t.Fatalf("Run(%q): %v", stmt, err)
	}
	out := make([]string, 0, len(res.Rows))
	for _, r := range res.Rows {
		out = append(out, string(r))
	}
	sort.Strings(out)
	return out
}

// TestDiscardElisionDifferential is the confidence oracle: a battery of query
// shapes must return identical results with elision ON and OFF.
func TestDiscardElisionDifferential(t *testing.T) {
	dir := t.TempDir()
	ks := filepath.Join(dir, "default", "nums")
	if err := os.MkdirAll(ks, 0o755); err != nil {
		t.Fatal(err)
	}
	docs := []string{
		`{"a":1,"g":"x"}`, `{"a":2,"g":"x"}`, `{"a":3,"g":"y"}`,
		`{"a":4,"g":"y"}`, `{"g":"z"}`, // last doc: no "a" (tests COUNT(a) vs COUNT(*))
	}
	for i, d := range docs {
		if err := os.WriteFile(filepath.Join(ks, "n"+string(rune('0'+i))+".json"), []byte(d), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	sess, err := OpenSession(dir, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}

	queries := []string{
		"SELECT COUNT(*) AS c FROM (SELECT 1 FROM nums a, nums b) t",                 // the target: count over join-subquery
		"SELECT COUNT(*) AS c FROM nums",                                             // count over a plain scan
		"SELECT COUNT(*) AS c FROM (SELECT a FROM nums) t",                           // subquery projects a field
		"SELECT COUNT(a) AS c FROM nums",                                             // COUNT(x): reads a -> must not change (5 docs, 4 have a)
		"SELECT g, COUNT(*) AS c FROM nums GROUP BY g",                               // GROUP BY: key source must survive
		"SELECT COUNT(*) AS c FROM nums WHERE a > 1",                                 // filter under the count
		"SELECT * FROM nums",                                                         // plain SELECT *
		"SELECT a FROM nums WHERE a >= 2",                                            // filter + field projection
		"SELECT COUNT(DISTINCT g) AS c FROM nums",                                    // COUNT(DISTINCT): reads g
		"SELECT COUNT(*) AS c, SUM(a) AS s FROM nums",                                // mixed aggs: SUM reads a
		"SELECT COUNT(*) AS c FROM (SELECT COUNT(*) AS n FROM nums) t",               // nested count
		"SELECT COUNT(*) AS c FROM nums n WHERE n.a IN (SELECT RAW m.a FROM nums m)", // subquery under the count
		"SELECT g, COUNT(*) AS c FROM nums GROUP BY g HAVING COUNT(*) > 1",           // GROUP BY + HAVING
	}

	restore := DiscardElision
	defer func() { DiscardElision = restore }()

	for _, q := range queries {
		DiscardElision = true
		on := runRows(t, sess, q)
		DiscardElision = false
		off := runRows(t, sess, q)
		if len(on) != len(off) {
			t.Errorf("%q: %d rows with elision, %d without\n on=%v\n off=%v", q, len(on), len(off), on, off)
			continue
		}
		for i := range on {
			if on[i] != off[i] {
				t.Errorf("%q: elision changed results\n on=%v\n off=%v", q, on, off)
				break
			}
		}
	}
}
