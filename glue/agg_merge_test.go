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
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/couchbase/n1k1/base"
)

// vals turns ints into base.Val JSON bytes (a value's JSON encoding is its byte form).
func vals(ns ...int) []base.Val {
	out := make([]base.Val, len(ns))
	for i, n := range ns {
		out[i] = base.Val(strconv.Itoa(n))
	}
	return out
}

// TestCombineAggregateMonoid proves the merge() contract: for a mergeable aggregate,
// folding independent PARTIAL accumulators equals aggregating the concatenation of all
// batches -- combine(part(A), part(B)) == aggregate(A ∪ B) -- and a fold-only aggregate
// is rejected.
func TestCombineAggregateMonoid(t *testing.T) {
	// Mergeable: a plain additive sum with an explicit merge.
	mustReg(t, RegisterJSAggregate("mq_msum", `
		function mq_msum_init()       { return 0; }
		function mq_msum_update(s, v) { return s + v; }
		function mq_msum_final(s)     { return s; }
		function mq_msum_merge(a, b)  { return a + b; }`))

	// Fold-only: same, minus merge -- must NOT become mergeable.
	mustReg(t, RegisterJSAggregate("mq_fsum", `
		function mq_fsum_init()       { return 0; }
		function mq_fsum_update(s, v) { return s + v; }
		function mq_fsum_final(s)     { return s; }`))

	sess, err := OpenSession(t.TempDir(), "default")
	if err != nil {
		t.Fatal(err)
	}

	// Sum of two partials equals the sum over the union.
	got, err := sess.CombineAggregate("mq_msum", [][]base.Val{vals(1, 2, 3), vals(4, 5)})
	if err != nil {
		t.Fatalf("CombineAggregate: %v", err)
	}
	if string(got) != "15" {
		t.Fatalf("mq_msum combine: got %q, want 15", got)
	}

	// Order/partitioning must not matter (commutative + associative).
	g2, _ := sess.CombineAggregate("mq_msum", [][]base.Val{vals(5), vals(4, 3, 2, 1), {}})
	if string(g2) != "15" {
		t.Fatalf("mq_msum re-partitioned combine: got %q, want 15", g2)
	}

	// A fold-only aggregate is not mergeable -> a clear error, not a silent wrong answer.
	if _, err := sess.CombineAggregate("mq_fsum", [][]base.Val{vals(1), vals(2)}); err == nil {
		t.Fatal("mq_fsum should be rejected as non-mergeable")
	}

	// Unknown aggregate -> error.
	if _, err := sess.CombineAggregate("mq_nope", nil); err == nil {
		t.Fatal("unknown aggregate should error")
	}
}

// TestCombineAggregateHLL loads the shipped hll.agg.js and checks the monoid law on the
// real (approximate) aggregate: merging per-shard sketches equals sketching the union,
// and both land near the true distinct count.
func TestCombineAggregateHLL(t *testing.T) {
	src, err := os.ReadFile(filepath.Join("..", "extensions", "functions", "js", "hll.agg.js"))
	if err != nil {
		t.Skipf("hll.agg.js not found: %v", err)
	}
	mustReg(t, RegisterJSAggregate("hll", string(src)))

	sess, err := OpenSession(t.TempDir(), "default")
	if err != nil {
		t.Fatal(err)
	}

	// 1000 distinct ids split across 4 shards, with overlap between shards.
	const distinct = 1000
	shard := func(lo, hi int) []base.Val {
		out := []base.Val{}
		for i := lo; i < hi; i++ {
			out = append(out, base.Val(fmt.Sprintf("%d", i)))
		}
		return out
	}
	batches := [][]base.Val{
		shard(0, 400), shard(300, 600), shard(500, 900), shard(800, distinct),
	}

	merged, err := sess.CombineAggregate("hll", batches)
	if err != nil {
		t.Fatalf("CombineAggregate hll (sharded): %v", err)
	}
	// The same ids as one batch -> the identical sketch, hence the identical estimate.
	all := shard(0, distinct)
	whole, err := sess.CombineAggregate("hll", [][]base.Val{all})
	if err != nil {
		t.Fatalf("CombineAggregate hll (whole): %v", err)
	}
	if string(merged) != string(whole) {
		t.Fatalf("monoid law broken: sharded estimate %q != whole estimate %q", merged, whole)
	}

	est, err := strconv.Atoi(string(merged))
	if err != nil {
		t.Fatalf("hll estimate not an int: %q (%v)", merged, err)
	}
	// p=6 HLL is coarse (~13% std error); accept a generous band around the truth.
	if est < distinct*7/10 || est > distinct*13/10 {
		t.Fatalf("hll estimate %d too far from %d distinct", est, distinct)
	}
}

// TestJSAggMutableState guards the in-place-mutation contract for JS aggregate state.
//
// Regression: the bridge decoded the accumulator with rt.ToValue over a plain Go
// []interface{}, which goja wraps as a FIXED-LENGTH slice view -- so `state.rows.push(x)`
// silently did nothing and every collected row was lost, while object property writes
// (state.n++) kept working, hiding the bug. aggStateFromJSON now rewrites arrays to
// *[]interface{} (growable), so ordinary JS mutation works. See ext_jsvm_agg.go.
func TestJSAggMutableState(t *testing.T) {
	// Collect via push (the natural JS idiom), plus a nested array and a splice/pop,
	// so any regression to a fixed-length wrapper fails loudly here.
	mustReg(t, RegisterJSAggregate("mq_collect", `
		function mq_collect_init()       { return { rows: [], nested: { xs: [] }, n: 0 }; }
		function mq_collect_update(s, v) {
			s.rows.push(v);          // in-place array growth on the state
			s.nested.xs.push(v);     // ... nested one level down
			s.n++;                   // property write (always worked)
			return s;
		}
		function mq_collect_final(s)     {
			var popped = s.rows.slice(0).pop();
			return { len: s.rows.length, nested: s.nested.xs.length, n: s.n,
			         first: s.rows[0], last: popped };
		}
		function mq_collect_merge(a, b)  {
			return { rows: a.rows.concat(b.rows),
			         nested: { xs: a.nested.xs.concat(b.nested.xs) },
			         n: a.n + b.n };
		}`))

	sess, err := OpenSession(t.TempDir(), "default")
	if err != nil {
		t.Fatal(err)
	}
	got, err := sess.CombineAggregate("mq_collect", [][]base.Val{vals(1, 2, 3), vals(4, 5)})
	if err != nil {
		t.Fatal(err)
	}
	// All five rows must survive -- pre-fix this collapsed to len:0.
	want := `{"first":1,"last":5,"len":5,"n":5,"nested":5}`
	if s := string(got); s != want {
		t.Errorf("mq_collect over 5 rows = %s, want %s\n"+
			"(len:0 means JS array mutation on the aggregate state was dropped again)", s, want)
	}
}

// TestJSAggResidentHandleProtocol drives the base.Agg bridge directly with a
// GlueContext-pinned (stable) runtime and proves the JSVM-resident state
// contract: the accumulator blob is a FIXED 9-byte handle (8-byte length prefix
// + 'H' + uint64) that never grows however much state the JS holds; NAME_final
// is repeatable (pure) and does not disturb the live state; NAME_snapshot wires
// Agg.ResultLive as the cheaper mid-flight view; and aggregation continues
// correctly after mid-flight renders (the inflight scenario).
func TestJSAggResidentHandleProtocol(t *testing.T) {
	if err := RegisterJSAggregate("mq_res", `
		function mq_res_init()        { return {rows: []}; }
		function mq_res_update(s, v)  { s.rows.push(v); return s; }
		function mq_res_final(s)      { return s.rows.length; }
		function mq_res_snapshot(s)   { return {"n": s.rows.length}; }
	`); err != nil {
		t.Fatalf("RegisterJSAggregate: %v", err)
	}

	tmpDir, vars := MakeVars("", "n1k1jsres")
	defer os.RemoveAll(tmpDir)
	gctx := NewGlueContext(time.Now())
	vars.Temps = vars.Temps[:0]
	vars.Temps = append(vars.Temps, gctx)
	for i := 0; i < 16; i++ {
		vars.Temps = append(vars.Temps, nil)
	}
	vc := vars.Ctx.ValComparer

	agg := base.Aggs[base.AggCatalog["mq_res"]]
	if agg.ResultLive == nil {
		t.Fatalf("mq_res defines mq_res_snapshot but Agg.ResultLive is nil")
	}

	const handleBlobLen = 8 + 9 // length prefix + 'H' + uint64 handle
	st := agg.Init(vars, nil)
	if len(st) != handleBlobLen {
		t.Fatalf("Init blob = %d bytes, want %d (resident handle)", len(st), handleBlobLen)
	}

	var buf []byte
	for i := 0; i < 100; i++ {
		buf, _, _ = agg.Update(vars, base.Val(`{"y":1}`), buf[:0], st, vc)
		st, buf = buf, st
		if len(st) != handleBlobLen {
			t.Fatalf("after update %d: blob = %d bytes, want %d (state must stay resident)",
				i, len(st), handleBlobLen)
		}
	}

	// Mid-flight live view (snapshot), then the terminal result TWICE: final is
	// contractually pure, so both reads agree and the state is undisturbed.
	live, _, _ := agg.ResultLive(vars, st, nil)
	if string(live) != `{"n":100}` {
		t.Fatalf("ResultLive = %s, want {\"n\":100}", live)
	}
	r1, _, _ := agg.Result(vars, st, nil)
	r2, _, _ := agg.Result(vars, st, nil)
	if string(r1) != "100" || string(r2) != "100" {
		t.Fatalf("Result twice = %s, %s, want 100, 100 (final must be pure/repeatable)", r1, r2)
	}

	// The inflight scenario: aggregation continues after mid-flight renders.
	buf, _, _ = agg.Update(vars, base.Val(`{"y":2}`), buf[:0], st, vc)
	st = append(st[:0], buf...)
	r3, _, _ := agg.Result(vars, st, nil)
	if string(r3) != "101" {
		t.Fatalf("Result after post-render update = %s, want 101", r3)
	}
}

// TestJSAggMapStateResident: a state shape the v1 JSON threading could not carry
// at all -- a JS Map (json.Marshal of its export fails -> state collapsed to null
// between rows). Resident state holds ANY JS value, so it just works.
func TestJSAggMapStateResident(t *testing.T) {
	if err := RegisterJSAggregate("mq_mapdistinct", `
		function mq_mapdistinct_init()        { return new Map(); }
		function mq_mapdistinct_update(s, v)  { var k = JSON.stringify(v); s.set(k, (s.get(k)||0)+1); return s; }
		function mq_mapdistinct_final(s)      { return s.size; }
	`); err != nil {
		t.Fatalf("RegisterJSAggregate: %v", err)
	}

	tmpDir, vars := MakeVars("", "n1k1jsmap")
	defer os.RemoveAll(tmpDir)
	gctx := NewGlueContext(time.Now())
	vars.Temps = vars.Temps[:0]
	vars.Temps = append(vars.Temps, gctx)
	for i := 0; i < 16; i++ {
		vars.Temps = append(vars.Temps, nil)
	}
	vc := vars.Ctx.ValComparer

	agg := base.Aggs[base.AggCatalog["mq_mapdistinct"]]
	st := agg.Init(vars, nil)
	var buf []byte
	for _, v := range []string{`1`, `2`, `2`, `3`, `3`, `3`, `"a"`, `"a"`, `null`} {
		buf, _, _ = agg.Update(vars, base.Val(v), buf[:0], st, vc)
		st, buf = buf, st
	}
	r, _, _ := agg.Result(vars, st, nil)
	if string(r) != "5" {
		t.Fatalf("Map-state distinct count = %s, want 5 (1, 2, 3, \"a\", null)", r)
	}
}
