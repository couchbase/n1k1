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
	"strings"
	"testing"
)

// TestExplainConvPlan: an EXPLAIN statement populates Result.Plan (n1k1's converted
// op tree) alongside the cbq plan row, and FormatConvPlan tags each expression's
// eval lane -- native for byte-path exprs, boxed for the SELECT * self-projection
// and a non-optimizable scalar function.
func TestExplainConvPlan(t *testing.T) {
	dir := t.TempDir()
	ks := filepath.Join(dir, "default", "orders")
	if err := os.MkdirAll(ks, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ks, "o1.json"), []byte(`{"custId":"c1","orderId":1}`), 0o644); err != nil {
		t.Fatal(err)
	}
	sess, err := OpenSession(dir, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}

	cases := []struct {
		q          string
		wantSub    []string // substrings the rendered n1k1 plan must contain
		wantAbsent []string // substrings it must NOT contain
	}{
		{
			// arithmetic + field refs stay native -> no boxed marker anywhere.
			q:          `EXPLAIN SELECT o.custId, o.orderId + 1 AS n FROM orders o WHERE o.orderId > 0`,
			wantSub:    []string{"project", "filter"},
			wantAbsent: []string{boxedMarker},
		},
		{
			// SELECT * is a self-projection n1k1 can't reduce -> boxed marker.
			q:       `EXPLAIN SELECT * FROM orders o`,
			wantSub: []string{"self " + boxedMarker},
		},
		{
			// A non-optimizable scalar fn boxes; the sibling arithmetic stays native
			// (unmarked). Exactly one boxed marker in the projection. (REPEAT is not
			// ported -- its RANGE_LIMIT/size-limit error paths need request context.)
			q:       `EXPLAIN SELECT REPEAT(o.custId,2) AS s, o.orderId*2 AS d FROM orders o`,
			wantSub: []string{"repeat", boxedMarker},
		},
	}
	for _, c := range cases {
		res, err := sess.Run(c.q)
		if err != nil {
			t.Fatalf("Run(%q): %v", c.q, err)
		}
		if res.Plan == nil {
			t.Errorf("%q: Result.Plan is nil (conv tree not populated for EXPLAIN)", c.q)
			continue
		}
		got := FormatConvPlan(res.Plan)
		for _, sub := range c.wantSub {
			if !strings.Contains(got, sub) {
				t.Errorf("%q: plan missing %q\n%s", c.q, sub, got)
			}
		}
		for _, sub := range c.wantAbsent {
			if strings.Contains(got, sub) {
				t.Errorf("%q: plan unexpectedly contains %q\n%s", c.q, sub, got)
			}
		}
		// EXPLAIN must still return the cbq plan row (compatibility unchanged).
		if len(res.Rows) != 1 || !strings.Contains(string(res.Rows[0]), `"#operator"`) {
			t.Errorf("%q: expected the cbq plan JSON row, got %v", c.q, res.Rows)
		}
	}
}

// TestExprCoverageAndBoxedEvals: static ExprCoverage classifies project/filter
// expressions, and Result.BoxedEvals counts per-row boxed evaluations at run time.
func TestExprCoverageAndBoxedEvals(t *testing.T) {
	dir := t.TempDir()
	ks := filepath.Join(dir, "default", "items")
	if err := os.MkdirAll(ks, 0o755); err != nil {
		t.Fatal(err)
	}
	const n = 5
	for i := 0; i < n; i++ {
		doc := `{"a":` + string(rune('0'+i)) + `,"s":"x"}`
		if err := os.WriteFile(filepath.Join(ks, "d"+string(rune('0'+i))+".json"), []byte(doc), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	sess, err := OpenSession(dir, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}

	// All-native projection: coverage 2/2, zero boxed evals over n rows.
	res, err := sess.Run(`SELECT i.a, i.a + 1 AS b FROM items i`)
	if err != nil {
		t.Fatalf("native run: %v", err)
	}
	if nat, box := ExprCoverage(res.Plan); nat != 2 || box != 0 {
		t.Errorf("native coverage = %d/%d, want 2 native / 0 boxed", nat, box)
	}
	if res.BoxedEvals != 0 {
		t.Errorf("native query BoxedEvals = %d, want 0", res.BoxedEvals)
	}

	// SELECT * self-projection: 1 boxed expr, boxed once per row.
	res, err = sess.Run(`SELECT * FROM items i`)
	if err != nil {
		t.Fatalf("star run: %v", err)
	}
	if nat, box := ExprCoverage(res.Plan); nat != 0 || box != 1 {
		t.Errorf("star coverage = %d/%d, want 0 native / 1 boxed", nat, box)
	}
	if res.BoxedEvals != n {
		t.Errorf("SELECT * BoxedEvals = %d, want %d (one per row)", res.BoxedEvals, n)
	}
}
