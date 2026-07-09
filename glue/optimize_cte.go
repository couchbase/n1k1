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
	"github.com/couchbase/query/expression"

	"github.com/couchbase/n1k1/base"
)

// EnableCTEMaterialize opts in the "materialize-once" optimization for a WITH
// CTE that is non-recursive, non-correlated, and referenced from FROM more than
// once (e.g. a self cross-join `FROM x o1, x o2`). Such a CTE is evaluated ONCE
// into a spillable temp (temp-capture over the CTE's expr-scan), and each
// `FROM <cte>` reference becomes a temp-yield that re-iterates the captured rows
// -- instead of the default, where each reference is an independent expr-scan
// re-evaluated (re-read + re-boxed) per reference AND re-driven per outer row of
// any nested-loop join (an O(n^k) blowup of the CTE subquery).
//
// Defaulted ON: the rewrite fires only for the provably safe shape (see
// materializeMultiRefCTEs) and produces byte-identical rows to the inlined path;
// the full differential suite exercises WITH/CTE queries as the proof. Anything
// outside the safe shape falls back to today's inlining unchanged.
var EnableCTEMaterialize = true

// CTEMaterializeApplied counts how many multiply-referenced CTEs the materialize
// pass rewrote (test observability, mirroring MergeRewriteApplied). A counter,
// not a per-run flag: tests reset it around a Run to assert the rewrite fired
// (multi-ref) or did not (correlated / recursive / single-ref).
var CTEMaterializeApplied int64

// materializeMultiRefCTEs rewrites each qualifying FROM-used CTE so its subquery
// runs ONCE into a spillable temp-capture, with every `FROM <cte>` reference
// turned into a temp-yield that re-iterates the captured rows. The rewrite fires
// only when EnableCTEMaterialize is set AND the CTE is:
//
//   - non-recursive (a recursive CTE is a with-recursive fixpoint, untouched);
//   - a subquery binding (the file-reading case worth materializing; a constant
//     CTE array is cheap and left inline);
//   - non-correlated (a correlated subquery's result depends on the outer row and
//     MUST NOT be captured once -- verified via Subquery.IsCorrelated());
//   - referenced from FROM >= 2 times.
//
// Everything else falls back to the default inlining (leaves the expr-scans as
// they are). Correctness rests on: (a) the captured rows carry the CTE's own
// (single-Val) shape, so each reference only needs to relabel to its alias --
// which the temp-yield op does via its unchanged Labels; and (b) OpTempYield
// re-reads the heap from the start on every invocation, so a nested-loop join
// re-driving an inner reference per outer row still sees the full captured set.
func (c *Conv) materializeMultiRefCTEs() {
	if !EnableCTEMaterialize || c.TopOp == nil || len(c.cteFromRefs) == 0 {
		return
	}

	// Conservative concurrency guard: a UNION ALL (and the broadcast ops) runs its
	// branches as SEPARATE, possibly concurrent actors (see DESIGN-testing.md
	// "-race"). Materializing hoists a single temp-capture above the whole tree and
	// has each reference read the one shared heap; that is safe under a single-actor
	// nested-loop join (the target shape), but sharing a captured heap across
	// concurrent union branches is not. So if the tree contains any such op, skip
	// the whole pass and fall back to per-reference inlining. (The self cross-join
	// target has no union/broadcast, so it is unaffected.)
	if treeHasConcurrentOp(c.TopOp) {
		return
	}

	// Iterate in first-encounter order (map iteration is random) so nested
	// sequences and temp slot indices are stable across runs / between the interp
	// and compiled paths.
	for _, expr := range c.cteFromRefOrder {
		r := c.cteFromRefs[expr]
		if r == nil || len(r.ops) < 2 { // single-ref: leave inline (no re-eval blowup)
			continue
		}
		if r.with == nil || r.with.IsRecursive() { // recursive handled by with-recursive
			continue
		}
		sub, ok := r.with.Expression().(expression.Subquery)
		if !ok { // only materialize a subquery CTE (a constant CTE is cheap inline)
			continue
		}
		if sub.IsCorrelated() { // result depends on the outer row -- must NOT capture once
			continue
		}

		// A fresh temp slot holds the spillable heap the capture fills and each
		// yield re-reads. The capture's child is an expr-scan over the CTE's own
		// expression (reusing the slot the first reference already recorded it in),
		// so the CTE subquery is evaluated exactly once, through the same boxed
		// EvaluateSubquery path a lone reference would have used.
		heapIdx := c.AddTemp(nil)

		capture := &base.Op{
			Kind:   "temp-capture",
			Params: []interface{}{heapIdx},
			Children: []*base.Op{{
				Kind:   "expr-scan",
				Labels: base.Labels{"." + LabelSuffix(r.alias)},
				Params: []interface{}{r.exprSlot},
			}},
		}

		// Rewrite each reference in place: an expr-scan re-evaluating the CTE
		// becomes a temp-yield re-iterating the captured heap. The op's Labels
		// (already .["<alias-of-this-reference>"]) are preserved, so the captured
		// single-Val rows are relabeled to this reference's alias exactly as the
		// inlined expr-scan did -- downstream field paths (o1.orders.total, ...)
		// resolve unchanged.
		for _, op := range r.ops {
			op.Kind = "temp-yield"
			op.Params = []interface{}{heapIdx}
			op.Children = nil
		}

		// Sequence the capture before the (now temp-yield-driven) main query, so
		// the temp is populated before any reference iterates it. The sequence
		// yields only the main query's rows (temp-capture is silent to its parent),
		// and carries the main query's labels for NewConvertVals. Stacking one
		// sequence per materialized CTE keeps every capture ahead of the main query.
		c.TopOp = &base.Op{
			Kind:     "sequence",
			Labels:   c.TopOp.Labels,
			Children: []*base.Op{capture, c.TopOp},
		}

		CTEMaterializeApplied++
	}
}

// treeHasConcurrentOp reports whether the op tree contains an operator that runs
// its children as separate (possibly concurrent) actors -- union-all and the
// broadcast ops. The materialize rewrite shares one captured heap across all
// references, which is only safe within a single-actor pipeline; these ops break
// that assumption, so their presence disables the rewrite (a conservative,
// whole-tree bail-out).
func treeHasConcurrentOp(op *base.Op) bool {
	if op == nil {
		return false
	}
	switch op.Kind {
	case "union-all", "broadcast", "broadcast-indexed":
		return true
	}
	for _, child := range op.Children {
		if treeHasConcurrentOp(child) {
			return true
		}
	}
	return false
}
