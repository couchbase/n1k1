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
	"testing"
)

// writeOrdersTotals writes n orders docs, each {"id","total","custId"}, into a
// fresh file-datastore keyspace and returns its root dir.
func writeOrdersTotals(t testing.TB, n int) string {
	t.Helper()
	root := t.TempDir()
	d := filepath.Join(root, "default", "orders")
	if err := os.MkdirAll(d, 0o755); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		doc := fmt.Sprintf(`{"id":"o%03d","total":%d,"custId":"c%d"}`, i, (i+1)*10, i%3)
		if err := os.WriteFile(filepath.Join(d, fmt.Sprintf("o%03d.json", i)), []byte(doc), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

// runCTE runs stmt over the given datastore root with EnableCTEMaterialize forced
// to `enable`, returning the result rows and how many CTEs the materialize pass
// rewrote during this run. It saves/restores the global flag + counter so tests
// don't leak state into each other (these globals are process-wide).
func runCTE(t *testing.T, root, stmt string, enable bool) (rows []json.RawMessage, applied int64) {
	t.Helper()
	savedFlag, savedCount := EnableCTEMaterialize, CTEMaterializeApplied
	defer func() { EnableCTEMaterialize, CTEMaterializeApplied = savedFlag, savedCount }()
	EnableCTEMaterialize = enable
	CTEMaterializeApplied = 0

	s, err := OpenSession(root, "default")
	if err != nil {
		t.Fatal(err)
	}
	res, err := s.Run(stmt)
	if err != nil {
		t.Fatalf("run (enable=%v) %q: %v", enable, stmt, err)
	}
	return res.Rows, CTEMaterializeApplied
}

func sameRows(a, b []json.RawMessage) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if string(a[i]) != string(b[i]) {
			return false
		}
	}
	return true
}

// The gate: the exact multiply-referenced self-cross-join CTE must produce
// byte-identical rows with the optimization ON vs OFF, AND match the equivalent
// query that inlines the same subquery as derived tables (never a CTE, so never
// materialized) -- an independent oracle.
func TestCTEMaterializeMultiRefByteIdentical(t *testing.T) {
	root := writeOrdersTotals(t, 5)

	const cteQ = `WITH x AS (SELECT * FROM orders) ` +
		`SELECT count(1) AS c, sum(o1.orders.total) AS s FROM x o1, x o2, x o3, x o4`

	on, appliedOn := runCTE(t, root, cteQ, true)
	off, appliedOff := runCTE(t, root, cteQ, false)

	if appliedOn != 1 {
		t.Fatalf("materialize should fire exactly once with flag ON, got %d", appliedOn)
	}
	if appliedOff != 0 {
		t.Fatalf("materialize must not fire with flag OFF, got %d", appliedOff)
	}
	if !sameRows(on, off) {
		t.Fatalf("ON vs OFF differ:\n ON: %v\nOFF: %v", on, off)
	}

	// Independent oracle: derived tables (FROM (SELECT ...) oN) are the same
	// subquery inlined 4x, but as subqueries not CTEs -- never materialized.
	const derivedQ = `SELECT count(1) AS c, sum(o1.orders.total) AS s FROM ` +
		`(SELECT * FROM orders) o1, (SELECT * FROM orders) o2, ` +
		`(SELECT * FROM orders) o3, (SELECT * FROM orders) o4`
	oracle, appliedOracle := runCTE(t, root, derivedQ, true)
	if appliedOracle != 0 {
		t.Fatalf("derived-table oracle must not materialize, got %d", appliedOracle)
	}
	if !sameRows(on, oracle) {
		t.Fatalf("materialized CTE vs derived-table oracle differ:\n  CTE: %v\noracle: %v", on, oracle)
	}

	// Sanity on the value: 5 docs, totals 10..50 (sum 150), count = 5^4 = 625,
	// s = 150 * 5^3 = 18750.
	var got struct {
		C int64 `json:"c"`
		S int64 `json:"s"`
	}
	if err := json.Unmarshal(on[0], &got); err != nil {
		t.Fatal(err)
	}
	if got.C != 625 || got.S != 18750 {
		t.Fatalf("got c=%d s=%d, want c=625 s=18750", got.C, got.S)
	}
}

// The materialize pass must fire ONLY for the safe multi-ref shape: not for a
// single reference, and not for a recursive CTE (a with-recursive fixpoint).
func TestCTEMaterializeFiringConditions(t *testing.T) {
	root := writeOrdersTotals(t, 4)

	// Single reference: correct, and must NOT materialize (no re-eval blowup).
	singleRows, singleApplied := runCTE(t, root,
		`WITH x AS (SELECT * FROM orders) SELECT count(1) AS c FROM x o1`, true)
	if singleApplied != 0 {
		t.Fatalf("single-ref CTE must not materialize, got %d", singleApplied)
	}
	var sc struct {
		C int64 `json:"c"`
	}
	if err := json.Unmarshal(singleRows[0], &sc); err != nil || sc.C != 4 {
		t.Fatalf("single-ref count: got %s (err %v), want 4", singleRows[0], err)
	}

	// Recursive CTE: handled by the with-recursive op, never an expr-scan, so the
	// materialize pass must leave it alone -- and results stay correct (1..5).
	recRows, recApplied := runCTE(t, root,
		`WITH RECURSIVE r AS (SELECT 1 AS n UNION SELECT r.n + 1 AS n FROM r WHERE r.n < 5) `+
			`SELECT x.n AS n FROM r AS x ORDER BY x.n`, true)
	if recApplied != 0 {
		t.Fatalf("recursive CTE must not materialize, got %d", recApplied)
	}
	if len(recRows) != 5 {
		t.Fatalf("recursive CTE: got %d rows, want 5", len(recRows))
	}

	// A two-reference CTE fires exactly once.
	_, twoApplied := runCTE(t, root,
		`WITH x AS (SELECT * FROM orders) SELECT count(1) AS c FROM x o1, x o2`, true)
	if twoApplied != 1 {
		t.Fatalf("two-ref CTE should materialize once, got %d", twoApplied)
	}
}

// Two distinct multiply-referenced CTEs each materialize (once apiece), and the
// result is byte-identical ON vs OFF -- exercising the stacked-sequence path.
func TestCTEMaterializeTwoCTEs(t *testing.T) {
	root := writeOrdersTotals(t, 4)
	const q = `WITH x AS (SELECT * FROM orders), y AS (SELECT * FROM orders WHERE orders.total > 20) ` +
		`SELECT count(1) AS c FROM x a, x b, y p, y q`

	on, appliedOn := runCTE(t, root, q, true)
	off, _ := runCTE(t, root, q, false)
	if appliedOn != 2 {
		t.Fatalf("two multi-ref CTEs should materialize twice, got %d", appliedOn)
	}
	if !sameRows(on, off) {
		t.Fatalf("two-CTE ON vs OFF differ:\n ON: %v\nOFF: %v", on, off)
	}
}

// A correlated subquery (per-outer-row result) present in the query must not be
// disturbed: the materialize pass leaves it alone (counter 0) and results stay
// correct. Correlated CTEs only arise inside subqueries, which are compiled by a
// separate path the pass never runs -- this guards non-interference at the level
// the pass can actually reach.
func TestCTEMaterializeCorrelatedUntouched(t *testing.T) {
	root := writeOrdersTotals(t, 6) // custIds c0,c1,c2 repeating

	const q = `SELECT o.id AS id, ` +
		`(SELECT RAW o2.id FROM orders o2 WHERE o2.custId = o.custId) AS peers ` +
		`FROM orders o ORDER BY o.id`

	on, appliedOn := runCTE(t, root, q, true)
	off, _ := runCTE(t, root, q, false)
	if appliedOn != 0 {
		t.Fatalf("correlated subquery must not trigger materialize, got %d", appliedOn)
	}
	if !sameRows(on, off) {
		t.Fatalf("correlated ON vs OFF differ:\n ON: %v\nOFF: %v", on, off)
	}
	if len(on) != 6 {
		t.Fatalf("correlated query: got %d rows, want 6", len(on))
	}
}

// Two independent WITH scopes sharing an alias but with DIFFERENT definitions,
// combined under EXCEPT ALL, must NOT be conflated into one materialized temp
// (keying is by expression identity, not alias) -- AND, being under a union-
// family op, the pass bails out anyway. Either way the result must be correct:
// [1,2,3] EXCEPT ALL [3,4,5] = {1,2}.
func TestCTEMaterializeExceptAllSameAlias(t *testing.T) {
	root := writeOrdersTotals(t, 1) // unused by the query, but a datastore is needed
	const q = `(WITH cte AS (SELECT a FROM [1,2,3] AS a) SELECT cte.a AS a FROM cte) ` +
		`EXCEPT ALL ` +
		`(WITH cte AS (SELECT a FROM [3,4,5] AS a) SELECT cte.a AS a FROM cte)`

	on, appliedOn := runCTE(t, root, q, true)
	off, _ := runCTE(t, root, q, false)
	if appliedOn != 0 {
		t.Fatalf("EXCEPT-ALL same-alias CTEs must not materialize, got %d", appliedOn)
	}
	if !sameRows(on, off) {
		t.Fatalf("EXCEPT-ALL ON vs OFF differ:\n ON: %v\nOFF: %v", on, off)
	}
	got := map[int64]bool{}
	for _, r := range on {
		var v struct {
			A int64 `json:"a"`
		}
		if err := json.Unmarshal(r, &v); err != nil {
			t.Fatal(err)
		}
		got[v.A] = true
	}
	if len(got) != 2 || !got[1] || !got[2] {
		t.Fatalf("EXCEPT-ALL result = %v, want {1,2}", got)
	}
}

// BenchmarkCTEMaterialize measures the user's pattern (a 4-way self cross-join of
// a CTE) with the optimization ON vs OFF. Run with -benchmem:
//
//	go test -tags n1ql -run x -bench BenchmarkCTEMaterialize -benchmem ./glue/
//
// OFF re-evaluates (re-reads + re-boxes) the CTE subquery O(n^3) times (once per
// combination of the three inner references, per outer row); ON evaluates it once
// into a spillable temp and re-scans the captured heap -- so both time and
// allocations collapse.
func BenchmarkCTEMaterialize(b *testing.B) {
	root := writeOrdersTotals(b, 10)
	const q = `WITH x AS (SELECT * FROM orders) ` +
		`SELECT count(1) AS c, sum(o1.orders.total) AS s FROM x o1, x o2, x o3, x o4`

	run := func(b *testing.B, enable bool) {
		saved := EnableCTEMaterialize
		defer func() { EnableCTEMaterialize = saved }()
		EnableCTEMaterialize = enable
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			s, err := OpenSession(root, "default")
			if err != nil {
				b.Fatal(err)
			}
			if _, err := s.Run(q); err != nil {
				b.Fatal(err)
			}
		}
	}

	b.Run("On", func(b *testing.B) { run(b, true) })
	b.Run("Off", func(b *testing.B) { run(b, false) })
}
