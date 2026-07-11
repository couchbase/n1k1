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

package engine

import (
	"strconv"
	"strings"
	"testing"

	"github.com/couchbase/n1k1/base"
)

// mergeKeyChild builds a "scan" child that yields one row per key in keys, each
// row a two-slot Vals: slot 0 is the whole doc ("."), slot 1 is the int64 sort
// key register ("ts"). The docs are emitted in the order given (the merge op
// requires each child to already be sorted ascending on the key).
func mergeKeyChild(keys []int64) *base.Op {
	var sb strings.Builder
	for _, k := range keys {
		sb.WriteString(`{"ts":`)
		sb.WriteString(strconv.FormatInt(k, 10))
		sb.WriteString("}\n")
	}

	// A scan yields the whole line into slot 0 ("."). We add a project so slot
	// 1 ("ts") carries the bare int64 key that OpMergeScan compares on.
	return &base.Op{
		Kind:   "project",
		Labels: base.Labels{".", "ts"},
		Params: []interface{}{
			[]interface{}{"labelPath", "."},
			[]interface{}{"labelPath", ".", "ts"},
		},
		Children: []*base.Op{{
			Kind:   "scan",
			Labels: base.Labels{"."},
			Params: []interface{}{"jsonsData", sb.String()},
		}},
	}
}

// runMergeScan executes a merge-scan op and returns the sequence of sort keys
// it yielded (read back out of slot 1 of each yielded row).
func runMergeScan(t *testing.T, root *base.Op) []int64 {
	t.Helper()

	vars := &base.Vars{
		Temps: make([]interface{}, 16),
		Ctx: &base.Ctx{
			ExprCatalog: ExprCatalog,
			ValComparer: base.NewValComparer(),
		},
	}

	var got []int64
	yieldVals := func(vals base.Vals) {
		if len(vals) < 2 {
			t.Fatalf("row has %d slots, want >= 2", len(vals))
		}
		n, err := strconv.ParseInt(string(vals[1]), 10, 64)
		if err != nil {
			t.Fatalf("row key %q not an int64: %v", string(vals[1]), err)
		}
		got = append(got, n)
	}
	yieldErr := func(err error) {
		if err != nil {
			t.Fatalf("yieldErr: %v", err)
		}
	}

	ExecOp(root, vars, yieldVals, yieldErr, "", "")

	return got
}

func assertAscending(t *testing.T, got []int64) {
	t.Helper()
	for i := 1; i < len(got); i++ {
		if got[i] < got[i-1] {
			t.Fatalf("output not globally ascending at %d: %v", i, got)
		}
	}
}

func assertSameMultiset(t *testing.T, got, want []int64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d rows %v, want %d rows %v", len(got), got, len(want), want)
	}
	counts := map[int64]int{}
	for _, k := range want {
		counts[k]++
	}
	for _, k := range got {
		counts[k]--
	}
	for k, c := range counts {
		if c != 0 {
			t.Fatalf("multiset mismatch for key %d (residual %d): got %v want %v", k, c, got, want)
		}
	}
}

// mergeScanOp builds a merge-scan whose sort key is slot 1 ("ts").
func mergeScanOp(regime string, children []*base.Op, params ...interface{}) *base.Op {
	p := []interface{}{1, regime}
	p = append(p, params...)
	return &base.Op{
		Kind:     "merge-scan",
		Labels:   base.Labels{".", "ts"},
		Params:   p,
		Children: children,
	}
}

// TestMergeScanConcatenate covers regime (a): disjoint, ordered ranges stream
// back-to-back, with no heap. Also drives the "auto" regime with zone maps
// proving disjointness, so it must select concatenate and produce the same
// result.
func TestMergeScanConcatenate(t *testing.T) {
	children := []*base.Op{
		mergeKeyChild([]int64{10, 20, 30}),
		mergeKeyChild([]int64{40, 50}),
		mergeKeyChild([]int64{60, 70, 80}),
	}
	want := []int64{10, 20, 30, 40, 50, 60, 70, 80}

	// Explicit concatenate.
	got := runMergeScan(t, mergeScanOp("concatenate", children))
	assertSameMultiset(t, got, want)
	assertAscending(t, got)

	// auto regime with zone maps that prove disjointness -> concatenate.
	minKeys := []interface{}{int64(10), int64(40), int64(60)}
	maxKeys := []interface{}{int64(30), int64(50), int64(80)}
	got = runMergeScan(t, mergeScanOp("auto", children, nil, minKeys, maxKeys))
	assertSameMultiset(t, got, want)
	assertAscending(t, got)
}

// TestMergeScanHeap covers regime (b): overlapping strict sources merged via
// the K-way min-heap, including duplicate keys across children.
func TestMergeScanHeap(t *testing.T) {
	children := []*base.Op{
		mergeKeyChild([]int64{10, 30, 50, 70}),
		mergeKeyChild([]int64{20, 30, 40, 60}),
		mergeKeyChild([]int64{15, 25, 35, 90}),
	}
	want := []int64{10, 15, 20, 25, 30, 30, 35, 40, 50, 60, 70, 90}

	got := runMergeScan(t, mergeScanOp("heap", children))
	assertSameMultiset(t, got, want)
	assertAscending(t, got)

	// auto regime with OVERLAPPING zone maps must fall back to heap.
	minKeys := []interface{}{int64(10), int64(20), int64(15)}
	maxKeys := []interface{}{int64(70), int64(60), int64(90)}
	got = runMergeScan(t, mergeScanOp("auto", children, nil, minKeys, maxKeys))
	assertSameMultiset(t, got, want)
	assertAscending(t, got)
}

// TestMergeScanEmptyChild checks that an empty child contributes nothing and
// does not disturb ordering, in both regimes.
func TestMergeScanEmptyChild(t *testing.T) {
	children := []*base.Op{
		mergeKeyChild([]int64{10, 20}),
		mergeKeyChild(nil), // empty
		mergeKeyChild([]int64{5, 30}),
	}
	want := []int64{5, 10, 20, 30}

	got := runMergeScan(t, mergeScanOp("heap", children))
	assertSameMultiset(t, got, want)
	assertAscending(t, got)

	// Empty child at the end of a concatenation of disjoint ranges.
	concatChildren := []*base.Op{
		mergeKeyChild([]int64{1, 2}),
		mergeKeyChild([]int64{3, 4}),
		mergeKeyChild(nil),
	}
	got = runMergeScan(t, mergeScanOp("concatenate", concatChildren))
	assertSameMultiset(t, got, []int64{1, 2, 3, 4})
	assertAscending(t, got)
}

// TestMergeScanSingleChild checks the degenerate K=1 case in both regimes.
func TestMergeScanSingleChild(t *testing.T) {
	for _, regime := range []string{"heap", "concatenate", "auto"} {
		children := []*base.Op{mergeKeyChild([]int64{7, 14, 21, 28})}
		got := runMergeScan(t, mergeScanOp(regime, children))
		want := []int64{7, 14, 21, 28}
		assertSameMultiset(t, got, want)
		assertAscending(t, got)
	}
}

// TestMergeScanNoChildren checks that a childless merge yields nothing cleanly.
func TestMergeScanNoChildren(t *testing.T) {
	got := runMergeScan(t, mergeScanOp("heap", nil))
	if len(got) != 0 {
		t.Fatalf("expected no rows, got %v", got)
	}
}

// runMergeScanErr executes a merge-scan and returns (yielded keys, error, warns).
func runMergeScanErr(t *testing.T, root *base.Op) ([]int64, error, []string) {
	t.Helper()

	var warns []string
	vars := &base.Vars{
		Temps: make([]interface{}, 16),
		Ctx: &base.Ctx{
			ExprCatalog: ExprCatalog,
			ValComparer: base.NewValComparer(),
			Warn:        func(w string) { warns = append(warns, w) },
		},
	}

	var got []int64
	var gotErr error
	ExecOp(root, vars,
		func(vals base.Vals) {
			n, err := strconv.ParseInt(string(vals[1]), 10, 64)
			if err != nil {
				t.Fatalf("row key %q not an int64: %v", string(vals[1]), err)
			}
			got = append(got, n)
		},
		func(err error) {
			if err != nil {
				gotErr = err
			}
		}, "", "")

	return got, gotErr, warns
}

// TestMergeScanWatermarkedNear covers regime (c): near-sorted children whose
// within-bound reorders are corrected by the watermark/reorder buffer into a
// globally ordered output stream.
func TestMergeScanWatermarkedNear(t *testing.T) {
	// Two near-sorted streams. Each has a local inversion within a disorder bound
	// of 5: child 0 emits 30 before 28 (a 2-nanos lag); child 1 emits 45 before
	// 42 (a 3-nanos lag). Neither exceeds the declared bound of 5.
	children := []*base.Op{
		mergeKeyChild([]int64{10, 30, 28, 50}),
		mergeKeyChild([]int64{20, 45, 42, 60}),
	}
	want := []int64{10, 20, 28, 30, 42, 45, 50, 60}

	sortedness := []interface{}{"near", "near"}
	bounds := []interface{}{int64(5), int64(5)}
	op := mergeScanOp("heap", children, sortedness, nil, nil, bounds, "error")

	got, err, warns := runMergeScanErr(t, op)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(warns) != 0 {
		t.Fatalf("unexpected warns for within-bound input: %v", warns)
	}
	assertSameMultiset(t, got, want)
	assertAscending(t, got)
}

// TestMergeScanWatermarkedNearMixed mixes a strict child with a near child.
func TestMergeScanWatermarkedNearMixed(t *testing.T) {
	children := []*base.Op{
		mergeKeyChild([]int64{5, 15, 25, 35}),  // strict
		mergeKeyChild([]int64{10, 22, 18, 40}), // near: 22 before 18 (bound 4)
	}
	want := []int64{5, 10, 15, 18, 22, 25, 35, 40}

	sortedness := []interface{}{"strict", "near"}
	bounds := []interface{}{int64(0), int64(4)}
	op := mergeScanOp("heap", children, sortedness, nil, nil, bounds, "error")

	got, err, _ := runMergeScanErr(t, op)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertSameMultiset(t, got, want)
	assertAscending(t, got)
}

// TestMergeScanWatermarkedSingleStreams covers the single-child streaming path: one
// near-sorted source is reordered into a globally ascending stream WITHOUT materializing
// the whole child first (the memory fix for near-sorted ASOF build/probe). Multiple
// within-bound inversions must all be corrected.
func TestMergeScanWatermarkedSingleStreams(t *testing.T) {
	// One near source: 30-before-28 (2 lag), 55-before-52-before-50 (within bound 8).
	children := []*base.Op{
		mergeKeyChild([]int64{10, 20, 30, 28, 40, 55, 52, 50, 70}),
	}
	want := []int64{10, 20, 28, 30, 40, 50, 52, 55, 70}

	op := mergeScanOp("heap", children, []interface{}{"near"}, nil, nil, []interface{}{int64(8)}, "error")

	got, err, warns := runMergeScanErr(t, op)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(warns) != 0 {
		t.Fatalf("unexpected warns for within-bound input: %v", warns)
	}
	assertSameMultiset(t, got, want)
	assertAscending(t, got)
}

// TestMergeScanWatermarkedSingleBeyondBound: the streaming path's ingestion tripwire
// still fires when a single near source is disordered beyond its declared bound.
func TestMergeScanWatermarkedSingleBeyondBound(t *testing.T) {
	// Claims bound 3 but is wildly out of order: by the time 20 arrives, the running max
	// (200) has driven the watermark past 100, which was already emitted -- so 20 is a
	// true, unrecoverable violation the tripwire must catch.
	children := []*base.Op{
		mergeKeyChild([]int64{10, 100, 200, 20, 300}),
	}
	op := mergeScanOp("heap", children, []interface{}{"near"}, nil, nil, []interface{}{int64(3)}, "error")

	_, err, _ := runMergeScanErr(t, op)
	if err == nil {
		t.Fatal("expected a disorder_bound violation error, got nil")
	}
	if !strings.Contains(err.Error(), "disorder_bound") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestMergeScanBeyondBoundErrors verifies the bound-validation tripwire fires
// under the default "error" policy when a record arrives further out of order
// than the declared disorder_bound (the bound was too small).
func TestMergeScanBeyondBoundErrors(t *testing.T) {
	// Child 0 claims a bound of 3 but emits 40 then 20 -- a 20-nanos lag, far
	// beyond 3. By the time 20 arrives the watermark has advanced past it and
	// smaller keys from child 1 have been emitted, so 20 is a true violation.
	children := []*base.Op{
		mergeKeyChild([]int64{10, 40, 20}),
		mergeKeyChild([]int64{25, 30, 35}),
	}
	sortedness := []interface{}{"near", "near"}
	bounds := []interface{}{int64(3), int64(3)}
	op := mergeScanOp("heap", children, sortedness, nil, nil, bounds, "error")

	_, err, _ := runMergeScanErr(t, op)
	if err == nil {
		t.Fatal("expected a disorder_bound violation error, got nil")
	}
	if !strings.Contains(err.Error(), "disorder_bound") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestMergeScanBeyondBoundWiden verifies the "widen" late-record policy: a
// beyond-bound record does NOT fail the query -- the op widens, warns, and
// keeps producing output rather than silently mis-ordering.
func TestMergeScanBeyondBoundWiden(t *testing.T) {
	children := []*base.Op{
		mergeKeyChild([]int64{10, 40, 20}),
		mergeKeyChild([]int64{25, 30, 35}),
	}
	sortedness := []interface{}{"near", "near"}
	bounds := []interface{}{int64(3), int64(3)}
	op := mergeScanOp("heap", children, sortedness, nil, nil, bounds, "widen")

	got, err, warns := runMergeScanErr(t, op)
	if err != nil {
		t.Fatalf("widen policy should not error, got: %v", err)
	}
	if len(warns) == 0 {
		t.Fatal("widen policy must emit a Warn about the late record")
	}
	// All six rows are still delivered (best-effort, never silently dropped).
	assertSameMultiset(t, got, []int64{10, 20, 25, 30, 35, 40})
}

// TestMergeScanNearAllArrivesInOrder checks a degenerate near source that is in
// fact perfectly ordered: the watermark path must reproduce the strict output.
func TestMergeScanNearAllArrivesInOrder(t *testing.T) {
	children := []*base.Op{
		mergeKeyChild([]int64{10, 30, 50}),
		mergeKeyChild([]int64{20, 40, 60}),
	}
	want := []int64{10, 20, 30, 40, 50, 60}

	sortedness := []interface{}{"near", "near"}
	bounds := []interface{}{int64(100), int64(100)}
	op := mergeScanOp("heap", children, sortedness, nil, nil, bounds, "error")

	got, err, _ := runMergeScanErr(t, op)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertSameMultiset(t, got, want)
	assertAscending(t, got)
}

// TestMergeScanOutOfOrderTripwire verifies the monotonicity tripwire fires
// when a source lies about being sorted (concatenate regime, given children
// whose ranges are declared disjoint/ordered but actually are not).
func TestMergeScanOutOfOrderTripwire(t *testing.T) {
	// Second child starts BELOW the first child's max -> concatenation of these
	// (as if wrongly claimed disjoint) must trip the output-order check.
	children := []*base.Op{
		mergeKeyChild([]int64{10, 50}),
		mergeKeyChild([]int64{20, 60}),
	}
	op := mergeScanOp("concatenate", children)

	vars := &base.Vars{
		Temps: make([]interface{}, 16),
		Ctx: &base.Ctx{
			ExprCatalog: ExprCatalog,
			ValComparer: base.NewValComparer(),
		},
	}

	var gotErr error
	ExecOp(op, vars,
		func(vals base.Vals) {},
		func(err error) {
			if err != nil {
				gotErr = err
			}
		}, "", "")

	if gotErr == nil {
		t.Fatal("expected an out-of-order tripwire error, got nil")
	}
	if !strings.Contains(gotErr.Error(), "out-of-order") {
		t.Fatalf("unexpected error: %v", gotErr)
	}
}
