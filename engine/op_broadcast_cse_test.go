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
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/couchbase/n1k1/base"
)

// makeLineJsons returns n one-per-line JSON docs shaped {"a":i,"line":"..."},
// so tests/benchmarks can exercise both a cheap numeric field (a) and a heavier
// string field (line) that a regexp predicate scans.
func makeLineJsons(n int) string {
	var sb strings.Builder
	for i := 0; i < n; i++ {
		sb.WriteString(`{"a":`)
		sb.WriteString(strconv.Itoa(i))
		sb.WriteString(`,"line":`)
		if i%3 == 0 {
			sb.WriteString(`"a panic happened at line ` + strconv.Itoa(i) + `"`)
		} else {
			sb.WriteString(`"routine log message ` + strconv.Itoa(i) + `"`)
		}
		sb.WriteString("}\n")
	}
	return sb.String()
}

func lineScanOp(n int) *base.Op {
	return &base.Op{
		Kind:   "scan",
		Labels: base.Labels{"."},
		Params: []interface{}{"jsonsData", makeLineJsons(n)},
	}
}

// plainBroadcast builds a flat (non-CSE) broadcast over scan for the SAME
// queries, so a test can diff its labelResults stream against the CSE'd plan.
func plainBroadcast(scan *base.Op, queries []BroadcastQuery, labelResultsLabels base.Labels) *base.Op {
	detParams := make([]interface{}, 0, len(queries))
	for _, d := range queries {
		detParams = append(detParams, []interface{}{d.Tag, d.Pred, d.Proj})
	}
	return &base.Op{
		Kind:     "broadcast",
		Labels:   append(base.Labels(nil), labelResultsLabels...),
		Params:   []interface{}{detParams},
		Children: []*base.Op{scan},
	}
}

// TestBroadcastCSEEquivalence is the CORE gate: a CSE'd plan must yield labelResults
// BYTE-IDENTICAL to the same queries run through a plain broadcast. Because a
// single broadcast fans queries out in order over one scan (no union-all
// concurrency), the whole tagged labelResults stream is order-stable, so the two
// [][]string streams compare exactly.
//
// The corpus deliberately exercises the four cases the transform must handle:
//
//	(a) a sub-predicate G = (a > 5) shared by several queries' predicates;
//	(b) an UNSHARED predicate (a < 3), which must NOT be hoisted;
//	(c) a sub-expr P = regexp_contains(line,"panic") shared between one
//	    query's PREDICATE and its PROJECTION;
//	(d) a NESTED shared subtree N = not(G), itself shared, that CONTAINS the
//	    also-shared G.
func TestBroadcastCSEEquivalence(t *testing.T) {
	const n = 30

	G := []interface{}{"gt", lp(".", "a"), []interface{}{"json", "5"}} // shared sub-predicate
	P := []interface{}{"regexp_contains", lp(".", "line"),             // shared pred<->proj
		[]interface{}{"json", "\"panic\""}}
	N := []interface{}{"not", G}                                           // nested shared subtree (contains G)
	small := []interface{}{"lt", lp(".", "a"), []interface{}{"json", "3"}} // unshared

	labelResultsLabels := base.Labels{"tag", "ev"}

	queries := []BroadcastQuery{
		// (a) G shared across three predicates.
		{Tag: "g1", Pred: []interface{}{"and", G, P}, Proj: []interface{}{lp(".", "a")}},
		{Tag: "g2", Pred: G, Proj: []interface{}{lp(".", "a")}},
		{Tag: "g3", Pred: []interface{}{"and", G, small}, Proj: []interface{}{lp(".", "a")}},
		// (b) unshared predicate.
		{Tag: "u1", Pred: small, Proj: []interface{}{lp(".", "a")}},
		// (c) P shared between this query's PRED and its PROJ.
		{Tag: "c1", Pred: P, Proj: []interface{}{P}},
		// (d) N = not(G) shared across two queries (and contains shared G).
		{Tag: "n1", Pred: N, Proj: []interface{}{lp(".", "a")}},
		{Tag: "n2", Pred: []interface{}{"and", N, P}, Proj: []interface{}{lp(".", "a")}},
	}

	scanA := lineScanOp(n)
	scanB := lineScanOp(n)

	// NOTE: BroadcastCSE applies per source group; a router (BroadcastRoute)
	// would split the corpus by TargetSource first, then wrap each group here.
	cse := BroadcastCSE(scanA, queries, labelResultsLabels)
	plain := plainBroadcast(scanB, queries, labelResultsLabels)

	gotCSE := collectRows(t, cse, broadcastVars())
	gotPlain := collectRows(t, plain, broadcastVars())

	if !reflect.DeepEqual(gotCSE, gotPlain) {
		t.Fatalf("CSE labelResults != plain labelResults\n CSE  =%v\n plain=%v", gotCSE, gotPlain)
	}
	if len(gotPlain) == 0 {
		t.Fatal("corpus produced no labelResults; the equivalence check would be vacuous")
	}
}

// TestBroadcastCSEStructural asserts the emitted plan shape: one "^cseN" column
// per shared NON-TRIVIAL candidate, trivial subtrees (bare labelPath / json) are
// NOT hoisted, and a zero-sharing corpus yields NO precompute project.
func TestBroadcastCSEStructural(t *testing.T) {
	labelResultsLabels := base.Labels{"tag", "ev"}

	G := []interface{}{"gt", lp(".", "a"), []interface{}{"json", "5"}}
	P := []interface{}{"regexp_contains", lp(".", "line"), []interface{}{"json", "\"panic\""}}

	// Corpus: G shared (2x), P shared (2x). The bare field read lp(".","a")
	// recurs many times and the constant ["json","5"] recurs too, but both are
	// TRIVIAL and must never be hoisted.
	queries := []BroadcastQuery{
		{Tag: "d1", Pred: G, Proj: []interface{}{lp(".", "a")}},
		{Tag: "d2", Pred: []interface{}{"and", G, P}, Proj: []interface{}{lp(".", "a")}},
		{Tag: "d3", Pred: P, Proj: []interface{}{lp(".", "a")}},
	}

	scan := lineScanOp(5)
	root := BroadcastCSE(scan, queries, labelResultsLabels)

	if root.Kind != "broadcast" {
		t.Fatalf("root Kind=%q, want broadcast", root.Kind)
	}
	proj := root.Children[0]
	if proj.Kind != "project" {
		t.Fatalf("broadcast child Kind=%q, want project (precompute)", proj.Kind)
	}
	if proj.Children[0] != scan {
		t.Fatal("precompute project's child is not the original scan")
	}

	// Labels: [".", "^cse0", "^cse1"] for exactly the two shared candidates.
	wantLabels := base.Labels{".", "^cse0", "^cse1"}
	if !reflect.DeepEqual(proj.Labels, wantLabels) {
		t.Fatalf("precompute labels=%v, want %v", proj.Labels, wantLabels)
	}

	// Every cse column's expr-tree must be NON-TRIVIAL (head is an operation).
	// Slot 0 is the whole-row passthrough; slots 1.. are the candidates.
	for i := 1; i < len(proj.Params); i++ {
		tree := proj.Params[i].([]interface{})
		if cseTrivial(tree) {
			t.Fatalf("precompute slot %d hoisted a TRIVIAL subtree %v", i, tree)
		}
	}
	// Passthrough must be the whole-row read.
	if got := fmt.Sprint(proj.Params[0]); got != fmt.Sprint([]interface{}{"labelPath", "."}) {
		t.Fatalf("precompute slot 0 = %v, want whole-row [labelPath .]", proj.Params[0])
	}

	// Zero-sharing corpus: no precompute project, plain broadcast over scan.
	lone := []BroadcastQuery{
		{Tag: "x1", Pred: G, Proj: []interface{}{lp(".", "a")}},
		{Tag: "x2", Pred: []interface{}{"lt", lp(".", "a"), []interface{}{"json", "3"}},
			Proj: []interface{}{lp(".", "line")}},
	}
	scan2 := lineScanOp(5)
	root2 := BroadcastCSE(scan2, lone, labelResultsLabels)
	if root2.Kind != "broadcast" {
		t.Fatalf("zero-sharing root Kind=%q, want broadcast", root2.Kind)
	}
	if root2.Children[0] != scan2 {
		t.Fatalf("zero-sharing corpus inserted a precompute (child Kind=%q); want plain broadcast over scan",
			root2.Children[0].Kind)
	}
}

// BenchmarkBroadcastCSE shows the CSE win: K queries that ALL share one
// EXPENSIVE sub-predicate -- regexp_contains(line,"panic") (native for a
// constant pattern) -- ANDed with a cheap per-query term (a > i). Under a
// plain broadcast the regexp runs K times per row; under CSE it runs ONCE per
// row (in the precompute project) and each query reads a cached slot. The gap
// grows with K.
func BenchmarkBroadcastCSE(b *testing.B) {
	const n = 1000
	labelResultsLabels := base.Labels{"tag", "ev"}

	buildQueries := func(k int) []BroadcastQuery {
		shared := []interface{}{"regexp_contains", lp(".", "line"),
			[]interface{}{"json", "\"panic\""}}
		dets := make([]BroadcastQuery, k)
		for i := 0; i < k; i++ {
			cheap := []interface{}{"gt", lp(".", "a"), []interface{}{"json", strconv.Itoa(i)}}
			dets[i] = BroadcastQuery{
				Tag:  "d" + strconv.Itoa(i),
				Pred: []interface{}{"and", shared, cheap},
				Proj: []interface{}{lp(".", "a")},
			}
		}
		return dets
	}

	sink := func(base.Vals) {}
	noErr := func(err error) {
		if err != nil {
			b.Fatalf("yieldErr: %v", err)
		}
	}

	for _, k := range []int{4, 32} {
		dets := buildQueries(k)

		// Build each plan ONCE (the CSE rewrite is a build-time pass, its garbage
		// off the hot path); the hot loop only re-runs the shared scan.
		cse := BroadcastCSE(lineScanOp(n), dets, labelResultsLabels)
		plain := plainBroadcast(lineScanOp(n), dets, labelResultsLabels)

		b.Run(fmt.Sprintf("cse/K=%d", k), func(b *testing.B) {
			vars := broadcastVars()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ExecOp(cse, vars, sink, noErr, "", "")
			}
		})

		b.Run(fmt.Sprintf("no-cse/K=%d", k), func(b *testing.B) {
			vars := broadcastVars()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ExecOp(plain, vars, sink, noErr, "", "")
			}
		})
	}
}
