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

// TestMergeScanNearRejected checks the deferred watermarked-near regime raises
// a clear error rather than silently producing an out-of-order stream.
func TestMergeScanNearRejected(t *testing.T) {
	children := []*base.Op{
		mergeKeyChild([]int64{10, 30}),
		mergeKeyChild([]int64{20, 40}),
	}
	sortedness := []interface{}{"strict", "near"}
	op := mergeScanOp("heap", children, sortedness)

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
		t.Fatal("expected a not-implemented error for a near source, got nil")
	}
	if !strings.Contains(gotErr.Error(), "watermarked-near") {
		t.Fatalf("unexpected error: %v", gotErr)
	}
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
