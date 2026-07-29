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
