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

// optimize_temporal.go is the Track B (merge & ASOF) grammar-free surfacing pass
// (DESIGN-merging.md §3). It is a READ-ONLY, POST-plan rewrite over the finished
// base.Op tree -- run downstream of the cbq fork's plan output, never touching
// the fork's grammar/planner (the unifying principle shared with DESIGN-data.md:
// "the fork produces plans; all recognition happens n1k1-side").
//
// It is invoked from ExecConv via a single call-site hook (rewriteTemporal),
// deliberately NOT by editing conv.go's VisitUnionAll -- the recognition is a
// structural property of the whole op subtree (an `order` sitting over a
// `union-all` of sorted branches), which reads more cleanly as one focused pass
// than smeared across per-op Visit methods.
//
// This first cut recognizes ONE idiom:
//
//	UNION ALL of sorted streams, wrapped by ORDER BY <key>  ->  merge-scan
//
// i.e. an `order-offset-limit` whose single child is `union-all` and whose ORDER
// BY key is a common column across the branches. Instead of materializing the
// whole concatenation and heap-sorting it (what order(union-all) does today), it
// emits the streaming K-way `merge-scan` op (engine/op_merge_scan.go): O(N log K)
// heap, or O(N) concatenate when the branches' key ranges are disjoint.
//
// The ASOF / argmax-subquery -> merge-join recognition (DESIGN-merging.md §3
// "argmax-subquery -> ASOF") is a LATER slice -- see the stub rewriteASOF below.
//
// GATING (first-cut precondition). The engine merge-scan compares a NORMALIZED
// int64 epoch-nanos sort key (the extract layer places it into a labeled
// register -- records.SortedSourceMeta / DESIGN-data.md "The normalized sort
// key"). That extract-normalization is Track A work not yet wired end-to-end, so
// a branch's key column is NOT guaranteed to be a bare int64 in the general case;
// firing unconditionally could turn a string-keyed UNION ALL...ORDER BY into a
// merge-scan that errors on the non-int key. So the pass is OPT-IN via
// EnableMergeRewrite (default off), exactly like DisableColumnarOptimize gates
// the columnar path. Once Track A wires SortedSourceMeta through glue, this gate
// becomes "fire when the branches carry a proven int64 sort key", and the
// per-branch sortedness/zone-map Params below are read from that meta rather than
// defaulted.

import (
	"encoding/json"
	"strings"

	"github.com/couchbase/query/expression"

	"github.com/couchbase/n1k1/base"
)

// EnableMergeRewrite opts the temporal merge rewrite (UNION ALL of sorted
// streams -> merge-scan) in. Default off: see the GATING note above -- the
// engine merge-scan requires a normalized int64 sort key that Track A's extract
// layer does not yet guarantee, so the rewrite stays opt-in until that lands.
// The differential test flips it on to prove interp == compiler over merge-scan.
var EnableMergeRewrite bool

// MergeRewriteApplied counts how many order(union-all) subtrees this pass
// rewrote into a merge-scan (test observability, mirroring AggColumnarApplied).
var MergeRewriteApplied int64

// rewriteTemporal is the single post-plan call-site hook (from ExecConv). It
// walks the finished op tree and rewrites every recognized order(union-all)-of-
// sorted-streams subtree into a streaming merge-scan, in place. A no-op unless
// EnableMergeRewrite is set. Safe fallback throughout: any subtree that does not
// match the exact recognized shape is left untouched (keeps order(union-all)).
func rewriteTemporal(op *base.Op) {
	if op == nil || !EnableMergeRewrite {
		return
	}
	rewriteTemporalWalk(op)
}

// rewriteTemporalWalk rewrites each child in place (so a matched child is
// replaced by its merge-scan), then recurses. Rewriting children (not the node
// itself) lets the pass replace an order(union-all) wherever it sits in the tree
// -- under a project, a subquery sequence, etc.
func rewriteTemporalWalk(op *base.Op) {
	for i, child := range op.Children {
		if ms := mergeScanFromOrderUnion(child); ms != nil {
			op.Children[i] = ms
			MergeRewriteApplied++
			child = ms
		}
		rewriteTemporalWalk(child)
	}
	// The root itself may be an order(union-all): rewriteTemporal's caller keeps
	// the same root pointer, so we cannot swap the root here. ExecConv handles the
	// root case via rewriteTemporalRoot below.
}

// rewriteTemporalRoot returns the merge-scan replacement for the tree ROOT when
// the root itself is a recognized order(union-all); otherwise it returns root
// unchanged. (The root pointer is owned by ExecConv, so only it can swap it.)
func rewriteTemporalRoot(root *base.Op) *base.Op {
	if root == nil || !EnableMergeRewrite {
		return root
	}
	if ms := mergeScanFromOrderUnion(root); ms != nil {
		MergeRewriteApplied++
		rewriteTemporalWalk(ms)
		return ms
	}
	rewriteTemporalWalk(root)
	return root
}

// mergeScanFromOrderUnion returns a merge-scan op that replaces the given op iff
// it is the recognized shape:
//
//   - op.Kind == "order-offset-limit" with NO offset/limit (Params == [exprs,
//     dirs]); paging would still need applying, so a paged order is left alone.
//   - a single ORDER BY term, ASCending, natural nulls (merge-scan emits one
//     ascending key stream).
//   - op's single child is "union-all" with >= 2 branches.
//   - the ORDER BY key resolves to a column position (keyIdx) present in the
//     union's output labels -- the slot the branches carry the sort key in.
//
// Otherwise it returns nil (no rewrite; the safe fallback keeps order(union-all)).
func mergeScanFromOrderUnion(op *base.Op) *base.Op {
	if op == nil || op.Kind != "order-offset-limit" {
		return nil
	}
	// [exprs, dirs] only -- a folded OFFSET/LIMIT (len > 2) still needs applying
	// after the merge, which this first cut does not add, so skip it.
	if len(op.Params) != 2 || len(op.Children) != 1 {
		return nil
	}
	union := op.Children[0]
	if union.Kind != "union-all" || len(union.Children) < 2 {
		return nil
	}

	exprs, ok := op.Params[0].([]interface{})
	if !ok || len(exprs) != 1 {
		return nil // first cut: a single sort key (not a multi-key prefix)
	}
	dirs, ok := op.Params[1].([]interface{})
	if !ok || len(dirs) != 1 {
		return nil
	}
	if d, _ := dirs[0].(string); d != "asc" {
		return nil // merge-scan produces an ascending stream
	}

	keyIdx := mergeResolveKeyIdx(exprs[0], union.Labels)
	if keyIdx < 0 {
		return nil // the sort key is not a plain column of the union output
	}

	// Per-branch sortedness. Conservative default (DESIGN-merging.md §3, this
	// task's spec): "strict" for every branch. When Track A wires
	// records.SortedSourceMeta through glue, this is where a branch declared
	// "near" contributes its sortedness + disorder-bound + zone-map Params
	// instead of the strict default.
	sortedness := make([]interface{}, len(union.Children))
	for i := range sortedness {
		sortedness[i] = "strict"
	}

	// Params layout matches engine/op_merge_scan.go MergeScanExec:
	//   [0] keyIdx, [1] regime, [2] sortedness, [3] minKeys, [4] maxKeys.
	// Regime "heap" is the conservative default (strict K-way min-heap); with no
	// zone maps supplied the "auto" regime could not prove concatenation anyway.
	return &base.Op{
		Kind:     "merge-scan",
		Labels:   append(base.Labels{}, union.Labels...),
		Params:   []interface{}{keyIdx, "heap", sortedness, nil, nil},
		Children: union.Children,
	}
}

// mergeResolveKeyIdx maps a single ORDER BY term (as converted by VisitOrder --
// either ["labelPath", L] or ["exprTree", <expr>]) to the position of its column
// within the union's output labels, or -1 if the key is not a plain column
// (a computed key can't be a merge sort key without the extract layer having
// materialized it into a register). Handles the plain-column cases the first cut
// supports: a labelPath, a bare identifier, or a field access whose leaf name is
// a union column.
//
// The union's output columns are LABEL-PATH encoded (e.g. `.["a"]` for column
// `a`, `.["x","a"]` for `x.a` -- see base.Labels), so an ORDER BY column named
// `a` is matched against a label whose leaf path element is "a".
func mergeResolveKeyIdx(term interface{}, labels base.Labels) int {
	tt, ok := term.([]interface{})
	if !ok || len(tt) != 2 {
		return -1
	}
	kind, _ := tt[0].(string)

	switch kind {
	case "labelPath":
		l, ok := tt[1].(string)
		if !ok {
			return -1
		}
		if i := labels.IndexOf(l); i >= 0 { // exact label match
			return i
		}
		if leaf, ok := mergeLabelLeaf(l); ok {
			return mergeLabelIdxByLeaf(labels, leaf)
		}
	case "exprTree":
		if e, ok := tt[1].(expression.Expression); ok {
			if name, ok := mergeExprLeafName(e); ok {
				return mergeLabelIdxByLeaf(labels, name)
			}
		}
	}
	return -1
}

// mergeLabelIdxByLeaf returns the index of the (unique) union label whose leaf
// field name equals name, or -1 if there is no match or the match is ambiguous
// (two branches expose different columns with the same leaf name -- a rewrite
// there could pick the wrong slot, so bail to the safe order(union-all)).
func mergeLabelIdxByLeaf(labels base.Labels, name string) int {
	found := -1
	for i, l := range labels {
		leaf, ok := mergeLabelLeaf(l)
		if !ok {
			continue
		}
		if leaf == name {
			if found >= 0 {
				return -1 // ambiguous
			}
			found = i
		}
	}
	return found
}

// mergeLabelLeaf returns the leaf (last) field name of a `.["..."]` label-path,
// e.g. `.["a"]` -> "a", `.["x","a"]` -> "a". The suffix after the leading "." is
// a JSON-encoded []string path (base.Labels). Returns ok=false for a non-path
// label (`.`, `^id`, an aggregate label, etc.).
func mergeLabelLeaf(label string) (string, bool) {
	if !strings.HasPrefix(label, ".[") {
		return "", false
	}
	var path []string
	if err := json.Unmarshal([]byte(label[1:]), &path); err != nil || len(path) == 0 {
		return "", false
	}
	return path[len(path)-1], true
}

// mergeExprLeafName returns the plain column name an ORDER BY expression refers
// to (a bare identifier `a`, or the leaf of a field access `x.a` -> "a"), so it
// can be matched against a union output label. Any other expression shape (a
// function call, arithmetic, an array index) is not a plain column and returns
// ok=false, so the pass leaves the order in place.
func mergeExprLeafName(e expression.Expression) (string, bool) {
	switch t := e.(type) {
	case *expression.Identifier:
		return t.Identifier(), true
	case *expression.Field:
		// The field's second operand is the field name as a string constant.
		if fn, ok := t.Second().(*expression.FieldName); ok {
			return fn.Alias(), true
		}
		if c, ok := t.Second().(*expression.Constant); ok {
			if s, ok := c.Value().Actual().(string); ok {
				return s, true
			}
		}
	}
	return "", false
}

// -----------------------------------------------------------------------------

// rewriteASOF is the DEFERRED argmax-subquery -> ASOF-join recognition
// (DESIGN-merging.md §3 "The argmax-subquery -> ASOF rewrite"): an outer row
// correlated with a `... WHERE ref.key <= outer.key ORDER BY ref.key DESC LIMIT
// 1` subquery is a nearest-preceding (ASOF) join, executable as one linear pass
// over two sorted streams instead of the O(n^2) correlated-subquery shape it
// plans as. That recognition is NON-LOCAL (it spans the outer query and the
// correlated subquery's WHERE/ORDER BY/LIMIT together), needs the merge-JOIN op
// (a separate engine slice), and is intentionally left as a stub here.
//
// TODO(track-b, asof): implement once the merge-join op lands. Recognize the
// argmax subquery shape on the plan tree, verify the comparison direction
// matches the ORDER BY direction (<= with DESC = nearest-preceding; a mismatch
// is NOT an argmax and must not be rewritten), confirm both sides are orderable
// by the key, and lower to a merge-join (nearest-preceding, optionally
// partitioned / tolerance-bounded for soft ASOF).
func rewriteASOF(op *base.Op) {
	// Intentionally a no-op: ASOF recognition is a later slice (see TODO above).
	_ = op
}
