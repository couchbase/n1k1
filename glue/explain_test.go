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
// eval lane -- native for byte-path exprs (including the SELECT * self-
// projection), boxed for a non-optimizable scalar function.
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
			// SELECT * self-projection now runs on the native byte path
			// (engine.ExprSelf) -> rendered as "self", no boxed marker.
			q:          `EXPLAIN SELECT * FROM orders o`,
			wantSub:    []string{"self"},
			wantAbsent: []string{boxedMarker},
		},
		{
			// A grouped aggregate reads the group's precomputed
			// "^aggregates|count(*)" value natively (labelPath), instead of boxing
			// the grouped row to re-invoke cbq's Aggregate.Evaluate.
			q:          `EXPLAIN SELECT COUNT(*) AS c FROM orders o`,
			wantSub:    []string{"count(*)"},
			wantAbsent: []string{boxedMarker},
		},
		{
			// A non-optimizable scalar fn boxes; the sibling arithmetic stays native
			// (unmarked). Exactly one boxed marker in the projection. (MB_LENGTH is not
			// ported -- multi-byte string funcs remain delegated to cbq.)
			q:       `EXPLAIN SELECT MB_LENGTH(o.custId) AS s, o.orderId*2 AS d FROM orders o`,
			wantSub: []string{"mb_length", boxedMarker},
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

// TestConvPlanSourceAndRescanMarkers: FormatConvPlan flags the two costs a bare op
// tree hides -- a boxed row source and a nested-loop-rescanned inner subtree. For
// `WITH x AS (SELECT * FROM orders) SELECT ... FROM x o1, x o2` the CTE `expr-scan`
// leaves carry ⟨boxed source⟩ (served per-row via GlueContext.EvaluateSubquery),
// and each nested-loop join's inner (right) child carries ⟨re-scanned per outer
// row⟩. A native `datastore-scan-*` leaf must NOT get the boxed-source marker, and
// the existing project ⟨boxed⟩ expression marker is unaffected. ConvPlanLegendFor
// returns exactly the keys for the markers a given plan uses.
//
// This exercises the UN-materialized shape (the expr-scan leaves + per-row rescan
// the markers describe), so it disables EnableCTEMaterialize -- otherwise the
// multiply-referenced CTE is materialized into temp-capture/temp-yield (there are
// then no expr-scan CTE leaves to mark). The materialized display is covered by
// TestExplainDisplayMatchesExec.
func TestConvPlanSourceAndRescanMarkers(t *testing.T) {
	defer func(prev bool) { EnableCTEMaterialize = prev }(EnableCTEMaterialize)
	EnableCTEMaterialize = false
	dir := t.TempDir()
	ks := filepath.Join(dir, "default", "orders")
	if err := os.MkdirAll(ks, 0o755); err != nil {
		t.Fatal(err)
	}
	docs := map[string]string{
		"o1.json": `{"custId":"c1","total":5}`,
		"o2.json": `{"custId":"c2","total":7}`,
	}
	for name, d := range docs {
		if err := os.WriteFile(filepath.Join(ks, name), []byte(d), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	sess, err := OpenSession(dir, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}

	// (a) CTE expr-scan leaves are boxed sources; (c) NL join inners are re-scanned.
	res, err := sess.Run(`EXPLAIN WITH x AS (SELECT * FROM orders) ` +
		`SELECT count(1) AS c, sum(o1.total) AS s FROM x o1, x o2`)
	if err != nil {
		t.Fatalf("CTE-join run: %v", err)
	}
	got := FormatConvPlan(res.Plan)
	if strings.Count(got, "expr-scan") != 2 {
		t.Fatalf("expected two expr-scan CTE leaves\n%s", got)
	}
	// Both CTE sources are boxed; exactly one join, whose single inner child is
	// re-scanned. (The outer expr-scan is not re-scanned.)
	if n := strings.Count(got, boxedSourceMarker); n != 2 {
		t.Errorf("boxed-source marker count = %d, want 2 (both CTE expr-scans)\n%s", n, got)
	}
	if n := strings.Count(got, rescanMarker); n != 1 {
		t.Errorf("rescan marker count = %d, want 1 (the join's inner)\n%s", n, got)
	}
	// The re-scanned marker must sit on an expr-scan line (the inner CTE source),
	// and one expr-scan line must be boxed WITHOUT the rescan marker (the outer).
	sawOuterBoxed, sawInnerRescan := false, false
	for _, ln := range strings.Split(got, "\n") {
		if !strings.Contains(ln, "expr-scan") {
			continue
		}
		if !strings.Contains(ln, boxedSourceMarker) {
			t.Errorf("expr-scan line missing boxed-source marker: %q", ln)
		}
		if strings.Contains(ln, rescanMarker) {
			sawInnerRescan = true
		} else {
			sawOuterBoxed = true
		}
	}
	if !sawOuterBoxed || !sawInnerRescan {
		t.Errorf("want one boxed-only (outer) and one boxed+rescan (inner) expr-scan; got outer=%v inner=%v\n%s",
			sawOuterBoxed, sawInnerRescan, got)
	}
	// Legend lists exactly the two markers this plan uses (no ⟨boxed⟩ expr line:
	// this plan's projection is native).
	legend := ConvPlanLegendFor(got)
	if !strings.Contains(legend, boxedSourceMarker) || !strings.Contains(legend, rescanMarker) {
		t.Errorf("legend missing a used marker key:\n%s", legend)
	}
	if strings.Contains(legend, boxedMarker+" =") {
		t.Errorf("legend should omit the ⟨boxed⟩ expr key (no boxed expr here):\n%s", legend)
	}

	// (b) A native datastore-scan join: the inner is re-scanned, but no leaf is a
	// boxed source (datastore I/O is native).
	res, err = sess.Run(`EXPLAIN SELECT count(1) AS c FROM orders o1, orders o2`)
	if err != nil {
		t.Fatalf("datastore-join run: %v", err)
	}
	got = FormatConvPlan(res.Plan)
	if !strings.Contains(got, "datastore-scan") {
		t.Fatalf("expected a datastore-scan leaf\n%s", got)
	}
	if strings.Contains(got, boxedSourceMarker) {
		t.Errorf("native datastore-scan must NOT carry the boxed-source marker\n%s", got)
	}
	if n := strings.Count(got, rescanMarker); n != 1 {
		t.Errorf("datastore-join rescan marker count = %d, want 1\n%s", n, got)
	}
	// The rescan marker sits on the inner datastore-scan line.
	for _, ln := range strings.Split(got, "\n") {
		if strings.Contains(ln, rescanMarker) && !strings.Contains(ln, "datastore-scan") {
			t.Errorf("rescan marker not on the inner datastore-scan line: %q", ln)
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

	// SELECT * self-projection now runs on the native byte path
	// (engine.ExprSelf): 1 native expr, zero boxed evals.
	res, err = sess.Run(`SELECT * FROM items i`)
	if err != nil {
		t.Fatalf("star run: %v", err)
	}
	if nat, box := ExprCoverage(res.Plan); nat != 1 || box != 0 {
		t.Errorf("star coverage = %d/%d, want 1 native / 0 boxed", nat, box)
	}
	if res.BoxedEvals != 0 {
		t.Errorf("SELECT * BoxedEvals = %d, want 0 (native self)", res.BoxedEvals)
	}
}

// TestExplainDisplayMatchesExec is the regression guard for the EXPLAIN/exec drift:
// `EXPLAIN <stmt>` must display the SAME optimized op tree the statement actually
// executes. Both paths now run the shared post-conversion passes (session.go
// applyPostConvPasses), so convForDisplay (EXPLAIN) and PlanConvert (exec) produce
// the same tree. Concretely, for a multiply-referenced non-correlated WITH CTE the
// display must show the materialize rewrite (temp-capture + temp-yield), not the
// stale un-materialized expr-scan leaves it used to show.
func TestExplainDisplayMatchesExec(t *testing.T) {
	dir := t.TempDir()
	ks := filepath.Join(dir, "default", "orders")
	if err := os.MkdirAll(ks, 0o755); err != nil {
		t.Fatal(err)
	}
	docs := map[string]string{
		"o1.json": `{"custId":"c1","total":5}`,
		"o2.json": `{"custId":"c2","total":7}`,
	}
	for name, d := range docs {
		if err := os.WriteFile(filepath.Join(ks, name), []byte(d), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	sess, err := OpenSession(dir, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}

	// A CTE referenced 4x under a chain of nested-loop joins: materialize-once fires.
	const body = `WITH x AS (SELECT * FROM orders) ` +
		`SELECT count(1) AS c, sum(o1.total) AS s FROM x o1, x o2, x o3, x o4`

	// EXPLAIN: Result.Plan is convForDisplay's output.
	exRes, err := sess.Run("EXPLAIN " + body)
	if err != nil {
		t.Fatalf("EXPLAIN run: %v", err)
	}
	if exRes.Plan == nil {
		t.Fatalf("EXPLAIN: Result.Plan is nil (conv tree not populated)")
	}
	explainTree := FormatConvPlan(exRes.Plan)

	// The display must now show the materialized shape (the exec path's shape), not
	// the old un-materialized expr-scan-per-reference tree.
	if !strings.Contains(explainTree, "temp-capture") || !strings.Contains(explainTree, "temp-yield") {
		t.Errorf("EXPLAIN display missing materialize rewrite (temp-capture/temp-yield):\n%s", explainTree)
	}
	// The 4 FROM refs must be temp-yields, not re-evaluated expr-scans; only the ONE
	// captured CTE source stays an expr-scan (fed into the temp-capture).
	if n := strings.Count(explainTree, "temp-yield"); n != 4 {
		t.Errorf("EXPLAIN display temp-yield count = %d, want 4 (one per FROM ref)\n%s", n, explainTree)
	}
	if n := strings.Count(explainTree, "expr-scan"); n != 1 {
		t.Errorf("EXPLAIN display expr-scan count = %d, want 1 (the captured CTE source)\n%s", n, explainTree)
	}

	// Now run the SAME statement for real (non-EXPLAIN): Result.Plan is the exec
	// path's pp.topOp (PlanConvert). Its rendered shape must equal the EXPLAIN one.
	execRes, err := sess.Run(body)
	if err != nil {
		t.Fatalf("exec run: %v", err)
	}
	if execRes.Plan == nil {
		t.Fatalf("exec: Result.Plan is nil")
	}
	execTree := FormatConvPlan(execRes.Plan)
	if explainTree != execTree {
		t.Errorf("EXPLAIN display tree != exec tree\n--- EXPLAIN ---\n%s\n--- exec ---\n%s", explainTree, execTree)
	}
}
