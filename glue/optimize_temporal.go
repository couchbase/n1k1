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
// It also carries the ASOF / argmax-subquery recognizer (DESIGN-merging.md §3
// "argmax-subquery -> ASOF"): MatchArgmaxAsof / recognizeASOFRoot below classify
// the canonical nearest-preceding correlated subquery and extract an AsofMatch.
// That recognizer is the conservative, independently-testable analysis half; the
// merge-JOIN lowering it feeds is gated on the same normalized-key wiring (see the
// note above recognizeASOFRoot).
//
// GATING (first-cut precondition). The engine merge-scan compares a NORMALIZED
// int64 epoch-nanos sort key (the extract layer places it into a labeled
// register -- records.SortedSourceMeta / DESIGN-data.md "The normalized sort
// key"). That extract-normalization is Track A work not yet wired end-to-end, so
// a branch's key column is NOT guaranteed to be a bare int64 in the general case;
// firing unconditionally could turn a string-keyed UNION ALL...ORDER BY into a
// merge-scan that errors on the non-int key. So the pass is OPT-IN via
// EnableMergeRewrite (default off), exactly like DisableColumnarOptimize gates
// the columnar path.
//
// UPDATE (A->B wiring landed): that "fire when the branches carry a proven int64
// sort key" path now exists as WireTemporalMergeMeta (below), run from PlanConvert
// (the s.Run path) and ServiceRequestEx where Temps are in hand. It consults Track
// A's SortedSourceMetasForKeyspace per branch and fires -- with the branches' REAL
// sortedness/disorder/zone-map Params -- only when the ORDER BY key is a proven
// normalized sort key. EnableMergeRewrite remains the no-metadata opt-in fallback
// (e.g. the compiler differential) for when that metadata isn't available.

import (
	"encoding/json"
	"sort"
	"strings"
	"sync/atomic"

	"github.com/couchbase/query/algebra"
	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/plan"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/records"
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
//
// The argmax-subquery -> ASOF recognizer (DESIGN-merging.md §3 "The
// argmax-subquery -> ASOF rewrite").
//
// An outer row correlated with a `(SELECT r.<f> FROM <R> r WHERE r.<key> <=
// e.<key> [AND r.<eqk> = e.<eqk>]* [AND r.<key> >= e.<key> - <Δt>] ORDER BY
// r.<key> DESC LIMIT 1) AS y` subquery is a NEAREST-PRECEDING (ASOF) join: it
// computes, per outer row, the single right row with the greatest key <= the
// outer key. That is exactly what OpMergeJoin's ASOF mode does in ONE linear
// pass, replacing the O(N*M) correlated-subquery re-drive.
//
// This slice ships the RECOGNIZER -- the conservative, independently-testable
// analysis half the design calls out ("feed it plans, assert the match; feed
// near-misses, assert no match"). It walks the finished op tree, finds every
// projected correlated subquery, and decides whether it is the EXACT canonical
// argmax shape, extracting an AsofMatch descriptor (right alias, key field,
// direction, soft-tolerance, partition fields). It is deliberately paranoid: a
// false positive would SILENTLY change query semantics (the correctness risk the
// design flags), so anything that is not the precise shape is left as the
// correct correlated subquery.
//
// What is NOT in this slice -- and why it is gated off -- is the LOWERING of a
// matched subquery into an executable merge-join op subtree. That reconstruction
// (plan+convert the subquery's keyspace into the right/build input, splice the
// outer scan as the left/probe input, and re-project the y alias) additionally
// depends on the NORMALIZED int64 sort-key contract that Track A's extract layer
// has not yet wired end-to-end -- the same gap that keeps the UNION-ALL->merge
// rewrite opt-in (see the GATING note at the top of this file). Until that lands,
// a recognized subquery could only be lowered to a merge-join over a raw,
// possibly-non-int key, so the recognizer records the match (AsofRecognized, for
// tests / observability) but does not mutate the tree. TODO(track-b, asof):
// lower AsofMatch -> a "merge-join" base.Op once the sort-key register is wired.

// AsofRecognized counts argmax subqueries the recognizer matched to the ASOF
// canonical shape (test observability, mirroring MergeRewriteApplied). It counts
// recognition, not lowering (see the note above).
var AsofRecognized int64

// EnableASOFRecognize opts the argmax->ASOF recognition pass in (default off).
// The pass is read-only -- it only classifies and counts -- so enabling it is
// side-effect free today; the gate mirrors EnableMergeRewrite so the two Track B
// passes flip together and the recognizer stays dormant in the normal path until
// the lowering + sort-key wiring lands.
var EnableASOFRecognize bool

// AsofMatch is the descriptor a recognized argmax subquery lowers to (once the
// merge-join lowering is wired). All fields are scalar / name-level so it maps
// directly onto OpMergeJoin's Params (leftKeyIdx/rightKeyIdx/joinType/asof/
// tolerance/partition-idxs) after the left+right label resolution a later slice
// adds.
type AsofMatch struct {
	RightAlias     string   // the subquery's keyspace alias (the build/right side).
	KeyField       string   // the sort-key field on BOTH sides (ORDER BY == inequality).
	Direction      string   // "preceding" (<= + DESC) or "following" (>= + ASC).
	Soft           bool     // a look-back guard was present -> soft ASOF.
	ToleranceNanos int64    // Δt for soft ASOF (0 when !Soft).
	PartitionEq    []AsofEq // zero+ equality (partition) predicates.
	ProjField      string   // the projected right field (the y value).
	ProjAlias      string   // the y alias.
}

// AsofEq is one equality (partition) predicate: right.RightField = outer.OuterField.
type AsofEq struct {
	RightField string
	OuterField string
}

// recognizeASOFRoot walks the op tree (read-only) and counts every projected
// argmax subquery that matches the ASOF canonical shape. A no-op unless
// EnableASOFRecognize is set. It never mutates the tree in this slice (lowering
// is gated -- see the note above).
func recognizeASOFRoot(root *base.Op) {
	if root == nil || !EnableASOFRecognize {
		return
	}
	recognizeASOFWalk(root)
}

func recognizeASOFWalk(op *base.Op) {
	if op == nil {
		return
	}
	if op.Kind == "project" {
		for _, p := range op.Params {
			term, ok := p.([]interface{})
			if !ok || len(term) < 2 {
				continue
			}
			if kind, _ := term[0].(string); kind != "exprTree" {
				continue
			}
			e, ok := term[1].(expression.Expression)
			if !ok || e == nil {
				continue
			}
			for _, subq := range collectSubqueries(e) {
				if _, matched := MatchArgmaxAsof(subq); matched {
					AsofRecognized++
				}
			}
		}
	}
	for _, child := range op.Children {
		recognizeASOFWalk(child)
	}
}

// collectSubqueries returns every algebra.Subquery embedded in an expression
// tree (the projected value may BE a subquery, or wrap one -- e.g.
// `(SELECT ...)[0]`). Uses the generic expression.Expression.Children() walk so
// it is robust to the wrapper shapes cbq produces for a scalar subquery.
func collectSubqueries(e expression.Expression) []*algebra.Subquery {
	var out []*algebra.Subquery
	var walk func(expression.Expression)
	walk = func(x expression.Expression) {
		if x == nil {
			return
		}
		if sq, ok := x.(*algebra.Subquery); ok {
			out = append(out, sq)
		}
		for _, c := range x.Children() {
			walk(c)
		}
	}
	walk(e)
	return out
}

// MatchArgmaxAsof is the conservative recognizer: it returns an AsofMatch iff the
// subquery is EXACTLY the nearest-preceding (or -following) argmax canonical
// shape (DESIGN-merging.md §3 recognition predicate). Every deviation -> ok=false
// (leave the correct correlated subquery in place). Guards, in order:
//
//  1. correlated, single simple keyspace FROM <R> r (no joins/subqueries).
//  2. ORDER BY exactly one term = r.<key>, with a direction (DESC / ASC).
//  3. LIMIT is the constant 1; no OFFSET.
//  4. projection is exactly ONE scalar field r.<f> (not *, not an aggregate).
//  5. WHERE is a conjunction of: exactly one inequality r.<key> (<= | >=)
//     outer.<key> whose direction AGREES with the ORDER BY (<= wants DESC =
//     preceding; >= wants ASC = following) and whose key == the ORDER BY key;
//     zero+ equality partition preds r.<eqk> = outer.<eqk>; and at most one
//     look-back guard r.<key> >= outer.<key> - Δt (preceding) -> soft ASOF.
//     Anything else in the WHERE -> bail.
func MatchArgmaxAsof(subq *algebra.Subquery) (*AsofMatch, bool) {
	if subq == nil || !subq.IsCorrelated() {
		return nil, false
	}

	sel := subq.Select()
	if sel == nil || sel.Offset() != nil {
		return nil, false
	}
	if !isConstOne(sel.Limit()) {
		return nil, false
	}

	// (2) ORDER BY exactly one term, a plain r.<key> field, with a direction.
	ord := sel.Order()
	if ord == nil || len(ord.Terms()) != 1 {
		return nil, false
	}
	ot := ord.Terms()[0]
	oAlias, oField, ok := splitFieldRef(ot.Expression())
	if !ok {
		return nil, false
	}
	descending := ot.Descending(nil, nil)

	// (1) single simple keyspace FROM, whose alias must be the ORDER BY root.
	ss, ok := sel.Subresult().(*algebra.Subselect)
	if !ok {
		return nil, false
	}
	fromAlias, ok := fromKeyspaceAlias(ss.From())
	if !ok || fromAlias != oAlias {
		return nil, false
	}
	rightAlias := oAlias
	keyField := oField

	// (4) projection = exactly one scalar field of the right alias.
	proj := ss.Projection()
	if proj == nil || len(proj.Terms()) != 1 {
		return nil, false
	}
	pt := proj.Terms()[0]
	if pt.Star() {
		return nil, false
	}
	pAlias, pField, ok := splitFieldRef(pt.Expression())
	if !ok || pAlias != rightAlias {
		return nil, false
	}

	// (5) WHERE conjuncts.
	where := ss.Where()
	if where == nil {
		return nil, false
	}
	conjuncts := flattenAnd(where)

	m := &AsofMatch{
		RightAlias: rightAlias,
		KeyField:   keyField,
		ProjField:  pField,
		ProjAlias:  pt.Alias(),
	}

	mainSeen := false
	lookbackSeen := false

	for _, cj := range conjuncts {
		switch cmp := cj.(type) {
		case *expression.Eq:
			eq, ok := classifyEq(cmp, rightAlias)
			if !ok {
				return nil, false
			}
			m.PartitionEq = append(m.PartitionEq, eq)

		case *expression.LE:
			// Both `<=` and `>=` parse as LE (the parser normalizes `a >= b` to
			// LE(b, a) -- there is no GE type); classifyIneq recovers the effective
			// relation direction from which operand is the right-side field.
			rField, outer, dir, ok := classifyIneq(cj, rightAlias)
			if !ok || rField != keyField {
				return nil, false
			}
			// A plain outer key field => the MAIN nearest inequality.
			if oa, of, ok := splitFieldRef(outer); ok && oa != rightAlias {
				_ = of
				if mainSeen {
					return nil, false // two main inequalities: not the canonical shape.
				}
				mainSeen = true
				// Direction must agree with the ORDER BY: <= wants DESC (preceding),
				// >= wants ASC (following). A mismatch is NOT an argmax.
				if dir == "le" && descending {
					m.Direction = "preceding"
				} else if dir == "ge" && !descending {
					m.Direction = "following"
				} else {
					return nil, false
				}
				continue
			}
			// An outer (key - Δt) subtraction => the look-back guard (soft ASOF).
			if of, delta, ok := splitLookback(outer, rightAlias); ok && of == keyField {
				// Only nearest-preceding has a `>= outer.key - Δt` look-back.
				if dir != "ge" || lookbackSeen {
					return nil, false
				}
				lookbackSeen = true
				m.Soft = true
				m.ToleranceNanos = delta
				continue
			}
			return nil, false // an inequality we do not recognize.

		default:
			return nil, false // any other predicate shape -> bail.
		}
	}

	if !mainSeen {
		return nil, false
	}
	// A look-back only makes sense for nearest-preceding (the documented shape).
	if lookbackSeen && m.Direction != "preceding" {
		return nil, false
	}
	return m, true
}

// fromKeyspaceAlias returns the alias of a single plain-keyspace FROM term (no
// joins, no subquery/derived-table source), or ok=false. The parser wraps a bare
// `FROM keyspace r` as either a *KeyspaceTerm or an *ExpressionTerm that reports
// IsKeyspace() -- both are acceptable; a derived table / expression source is not.
func fromKeyspaceAlias(from algebra.FromTerm) (string, bool) {
	switch t := from.(type) {
	case *algebra.KeyspaceTerm:
		return t.Alias(), true
	case *algebra.ExpressionTerm:
		if t.IsKeyspace() {
			return t.Alias(), true
		}
	}
	return "", false
}

// flattenAnd returns the conjuncts of an AND tree (or the single expression when
// it is not an AND).
func flattenAnd(e expression.Expression) []expression.Expression {
	and, ok := e.(*expression.And)
	if !ok {
		return []expression.Expression{e}
	}
	return and.Operands()
}

// classifyIneq normalizes an LE comparison (which is what BOTH `<=` and `>=`
// parse to -- `a >= b` becomes LE(b, a)) to (rightField, outerExpr, dir) where
// exactly ONE operand is a plain field of rightAlias. dir is "le" when the
// relation reads right.field <= outerExpr, "ge" when right.field >= outerExpr
// (recovered from operand order: LE(right.field, x) is "le"; LE(x, right.field)
// is "ge").
func classifyIneq(e expression.Expression, rightAlias string) (rightField string, outer expression.Expression, dir string, ok bool) {
	cmp, isLE := e.(*expression.LE)
	if !isLE {
		return "", nil, "", false
	}
	ops := cmp.Operands()
	if len(ops) != 2 {
		return "", nil, "", false
	}
	a, b := ops[0], ops[1]

	// LE(right.field, x): right.field <= x.
	if al, af, okA := splitFieldRef(a); okA && al == rightAlias {
		return af, b, "le", true
	}
	// LE(x, right.field): x <= right.field, i.e. right.field >= x.
	if bl, bf, okB := splitFieldRef(b); okB && bl == rightAlias {
		return bf, a, "ge", true
	}
	return "", nil, "", false
}

// classifyEq matches an equality partition predicate right.<f> = outer.<g>,
// returning the (rightField, outerField) pair. Both sides must be plain field
// refs and exactly one must be the right alias.
func classifyEq(cmp *expression.Eq, rightAlias string) (AsofEq, bool) {
	ops := cmp.Operands()
	if len(ops) != 2 {
		return AsofEq{}, false
	}
	al, af, okA := splitFieldRef(ops[0])
	bl, bf, okB := splitFieldRef(ops[1])
	if !okA || !okB {
		return AsofEq{}, false
	}
	if al == rightAlias && bl != rightAlias {
		return AsofEq{RightField: af, OuterField: bf}, true
	}
	if bl == rightAlias && al != rightAlias {
		return AsofEq{RightField: bf, OuterField: af}, true
	}
	return AsofEq{}, false
}

// splitLookback matches an outer look-back expression `outer.<field> - <Δt>`
// (a Sub of a non-right field ref and a positive numeric constant), returning
// the outer field name and Δt. Anything else -> ok=false.
func splitLookback(e expression.Expression, rightAlias string) (field string, delta int64, ok bool) {
	sub, ok := e.(*expression.Sub)
	if !ok {
		return "", 0, false
	}
	ops := sub.Operands()
	if len(ops) != 2 {
		return "", 0, false
	}
	al, af, okA := splitFieldRef(ops[0])
	if !okA || al == rightAlias {
		return "", 0, false
	}
	d, ok := constInt64(ops[1])
	if !ok || d < 0 {
		return "", 0, false
	}
	return af, d, true
}

// splitFieldRef decomposes `alias.field` into (alias, field). It handles a Field
// whose object operand is a plain Identifier and whose name operand is a
// FieldName or a string Constant. Deeper paths / other shapes -> ok=false.
func splitFieldRef(e expression.Expression) (alias, field string, ok bool) {
	f, isField := e.(*expression.Field)
	if !isField {
		return "", "", false
	}
	ops := f.Operands()
	if len(ops) != 2 {
		return "", "", false
	}
	id, isID := ops[0].(*expression.Identifier)
	if !isID {
		return "", "", false
	}
	switch n := ops[1].(type) {
	case *expression.FieldName:
		return id.Identifier(), n.Alias(), true
	case *expression.Constant:
		if s, ok := n.Value().Actual().(string); ok {
			return id.Identifier(), s, true
		}
	}
	return "", "", false
}

// isConstOne reports whether e is the numeric constant 1 (the LIMIT 1 guard).
func isConstOne(e expression.Expression) bool {
	n, ok := constInt64(e)
	return ok && n == 1
}

// constInt64 reads a numeric constant expression as an int64 (tolerating the
// float64 the parser stores numbers as). Non-constant / non-integral -> false.
func constInt64(e expression.Expression) (int64, bool) {
	c, ok := e.(*expression.Constant)
	if !ok {
		return 0, false
	}
	switch v := c.Value().Actual().(type) {
	case float64:
		if v == float64(int64(v)) {
			return int64(v), true
		}
	case int64:
		return v, true
	case int:
		return int64(v), true
	}
	return 0, false
}

// -------------------------------------------------------------------
// A -> B wiring: fire UNION-ALL -> merge from Track A's SortedSourceMeta.
//
// rewriteTemporal (above) is the no-metadata, opt-in (EnableMergeRewrite) pass
// that runs in ExecConv where no datastore is reachable. WireTemporalMergeMeta is
// its metadata-aware counterpart: it runs at exec time (ServiceRequestEx), where
// the GlueContext + Temps are available, so it can consult Track A's memoized
// SortedSourceMeta (via each union branch's scan keyspace) and FIRE the rewrite --
// with REAL per-branch sortedness / disorder-bound / zone-map Params -- but ONLY
// when the ORDER BY key is a PROVEN normalized int64 sort key (SortKeyLabel) across
// every branch. Safe by construction: any branch whose key isn't a proven sorted
// source leaves order(union-all) untouched (the correct heap-sort path). The
// watermarked-near merge validates monotonicity (policy "error"), so even an
// optimistic sortedness aggregation fails loudly rather than mis-orders.

// MergeMetaRewriteApplied counts order(union-all) subtrees WireTemporalMergeMeta
// lowered to a metadata-driven merge-scan (test observability).
var MergeMetaRewriteApplied int64

// WireTemporalMergeMeta walks the op tree (root included) replacing each recognized
// order(union-all) whose sort key is a proven sorted source with a merge-scan whose
// Params come from Track A's SortedSourceMeta. Returns the (possibly new) root.
// gctx may be nil (SortedSourceMetasForKeyspace then walks fresh instead of using
// the per-request walk-file cache) -- so this is safe to call from PlanConvert,
// which runs before any GlueContext exists.
//
// conv supplies both the Temps the branch scans resolve their keyspaces from AND the
// AddTemp seam the per-file expansion registers each single-file keyspacer into (so a
// K-file branch becomes K per-file merge children -- see perFileScans). Callers that
// only have a bare temps slice (ServiceRequestEx) wrap it in a throwaway *Conv and
// read conv.Temps back afterwards.
func WireTemporalMergeMeta(root *base.Op, conv *Conv, gctx *GlueContext) *base.Op {
	if root == nil {
		return root
	}
	if ms := mergeScanWithMeta(root, conv, gctx); ms != nil {
		wireTemporalMetaWalk(ms, conv, gctx)
		return ms
	}
	wireTemporalMetaWalk(root, conv, gctx)
	return root
}

func wireTemporalMetaWalk(op *base.Op, conv *Conv, gctx *GlueContext) {
	for i, child := range op.Children {
		if ms := mergeScanWithMeta(child, conv, gctx); ms != nil {
			op.Children[i] = ms
			child = ms
		}
		wireTemporalMetaWalk(child, conv, gctx)
	}
}

// mergeScanWithMeta recognizes an order(union-all) (reusing mergeScanFromOrderUnion
// for the shape + keyIdx) and, iff the ORDER BY key is a proven normalized sort key
// across every branch, returns a merge-scan carrying the branches' real sortedness,
// disorder-bound and zone-map Params. Returns nil (no rewrite) otherwise.
//
// Per-file expansion (DESIGN-merging.md "Multi-bundle / cross-node clusters"): a
// branch that is a single project(scan) over a keyspace resolving to K recipe files
// is expanded into K per-file merge children -- the branch's projection cloned over
// each single-file scan -- so the merge sees each file as its own globally-ordered
// cursor instead of one CONCATENATED (and thus non-monotonic, tripwire-tripping)
// stream. A branch that is not so expandable (0/1 files, an unproven key, or a
// non-single-scan subtree) keeps the single whole-keyspace child.
func mergeScanWithMeta(op *base.Op, conv *Conv, gctx *GlueContext) *base.Op {
	ms := mergeScanFromOrderUnion(op)
	if ms == nil {
		return nil
	}
	keyName := mergeOrderKeyName(op)
	if keyName == "" {
		return nil
	}
	union := op.Children[0]

	var children []*base.Op
	var sortedness, minKeys, maxKeys, bounds []interface{}
	haveZone := true

	for _, branch := range union.Children {
		ks := branchScanKeyspace(branch, conv.Temps)
		if ks == nil {
			return nil // a branch we can't resolve to a keyspace: don't fire
		}
		metas, err := SortedSourceMetasForKeyspace(ks, gctx)
		if err != nil || len(metas) == 0 {
			return nil // no proven sorted source for this branch
		}

		// Try per-file expansion for a simple single-scan branch: clone the branch's
		// projection over each per-file scan, one merge child per file.
		if leaf, cnt := branchScanLeaf(branch); leaf != nil && cnt == 1 {
			if scans, sPer, minPer, maxPer, bndPer, ok :=
				perFileScans(metas, leaf.Labels, keyName, conv); ok {
				for _, sc := range scans {
					children = append(children, cloneBranchWithScan(branch, sc))
				}
				sortedness = append(sortedness, sPer...)
				bounds = append(bounds, bndPer...)
				if len(minPer) == len(sPer) {
					minKeys = append(minKeys, minPer...)
					maxKeys = append(maxKeys, maxPer...)
				} else {
					haveZone = false
				}
				continue
			}
		}

		// Single whole-keyspace child fallback for this branch.
		s, minK, maxK, boundNs, zoneOK, ok := aggregateBranchMeta(metas, keyName)
		if !ok {
			return nil // key isn't the proven normalized sort key for this branch
		}
		children = append(children, branch)
		sortedness = append(sortedness, s)
		bounds = append(bounds, boundNs)
		if zoneOK {
			minKeys = append(minKeys, minK)
			maxKeys = append(maxKeys, maxK)
		} else {
			haveZone = false
		}
	}

	if !haveZone {
		minKeys, maxKeys = nil, nil // "auto" can't prove disjointness without a full zone map
	}
	// Params layout: engine/op_merge_scan.go MergeScanExec
	// [0]keyIdx [1]regime [2]sortedness [3]minKeys [4]maxKeys [5]bounds [6]policy.
	ms.Children = children
	ms.Params = []interface{}{ms.Params[0], "auto", sortedness, minKeys, maxKeys, bounds, "error"}
	MergeMetaRewriteApplied++
	return ms
}

// mergeOrderKeyName returns the leaf column name of the single ORDER BY term (or "").
func mergeOrderKeyName(op *base.Op) string {
	if op == nil || len(op.Params) < 1 {
		return ""
	}
	exprs, ok := op.Params[0].([]interface{})
	if !ok || len(exprs) != 1 {
		return ""
	}
	term, ok := exprs[0].([]interface{})
	if !ok || len(term) != 2 {
		return ""
	}
	switch term[0].(string) {
	case "labelPath":
		if l, ok := term[1].(string); ok {
			if leaf, ok := mergeLabelLeaf(l); ok {
				return leaf
			}
		}
	case "exprTree":
		if e, ok := term[1].(expression.Expression); ok {
			if name, ok := mergeExprLeafName(e); ok {
				return name
			}
		}
	}
	return ""
}

// branchScanKeyspace finds the first datastore-scan op under branch and returns the
// keyspace it scans (the plan op sits in Temps[Params[0]], implementing keyspacer),
// or nil if the branch has no resolvable scan keyspace.
func branchScanKeyspace(op *base.Op, temps []interface{}) datastore.Keyspace {
	if op == nil {
		return nil
	}
	if strings.HasPrefix(op.Kind, "datastore-scan") && len(op.Params) > 0 {
		if idx, ok := op.Params[0].(int); ok && idx >= 0 && idx < len(temps) {
			if ks, ok := temps[idx].(keyspacer); ok {
				return ks.Keyspace()
			}
		}
	}
	for _, c := range op.Children {
		if ks := branchScanKeyspace(c, temps); ks != nil {
			return ks
		}
	}
	return nil
}

// aggregateBranchMeta collapses a branch's per-file SortedSourceMetas into the
// branch's single merge input: it requires every file's SortKeyLabel to equal
// keyName (the proven normalized key) and no file to be unsorted. sortedness is
// "near" if any file is near; boundNs is the max disorder window; the zone map is
// [min(minKey), max(maxKey)] over files that have one (zoneOK=false if any file
// lacks a record-count/zone, disabling the concatenate optimization for safety).
func aggregateBranchMeta(metas []FileSortedSourceMeta, keyName string) (
	sortedness string, minK, maxK, boundNs int64, zoneOK, ok bool) {
	sortedness = records.SortedStrict
	zoneOK = true
	haveZone := false
	for _, fm := range metas {
		m := fm.Meta
		if m.SortKeyLabel != keyName {
			return "", 0, 0, 0, false, false // not the normalized sort key
		}
		switch m.Sortedness {
		case records.SortedNone:
			return "", 0, 0, 0, false, false // unsorted: can't merge without a sort
		case records.SortedNear:
			sortedness = records.SortedNear
		}
		if m.Disorder.WindowNanos > boundNs {
			boundNs = m.Disorder.WindowNanos
		}
		if m.RecordCount > 0 {
			if !haveZone || m.MinKey < minK {
				minK = m.MinKey
			}
			if !haveZone || m.MaxKey > maxK {
				maxK = m.MaxKey
			}
			haveZone = true
		} else {
			zoneOK = false
		}
	}
	if !haveZone {
		zoneOK = false
	}
	return sortedness, minK, maxK, boundNs, zoneOK, true
}

// -------------------------------------------------------------------
// Per-file child scans (DESIGN-merging.md "Multi-bundle / cross-node clusters").
//
// A merge/ASOF input keyspace that resolves to MULTIPLE recipe files (a **/*.log
// glob, a classic keyspace of many files, or K per-node cbcollect bundles with
// OVERLAPPING time ranges) is scanned by default as ONE CONCATENATED stream (records
// walks + unions the files into a single cursor). Concatenated overlapping files are
// not globally key-ordered, so the merge's monotonicity tripwire rejects them. The
// fix here turns such a keyspace into K per-file ORDERED child inputs to the K-way
// merge -- each file its own cursor with its own SortedSourceMeta zone map -- so the
// engine's "auto" regime concatenates disjoint daily files and heap/watermarks
// overlapping cross-node files. This is the enabler for cross-node ASOF and the
// single-glob timeline.

// EnablePerFileMergeScans gates the per-file expansion (default on). Off, a multi-
// file merge/ASOF keyspace stays a single concatenated child (the pre-existing
// behavior) -- which trips the tripwire on overlapping files. The differential
// tests toggle it to prove the fix both fires and is required.
var EnablePerFileMergeScans = true

// PerFileMergeApplied counts how many multi-file keyspaces perFileScans expanded into
// per-file merge children (test observability, mirroring MergeMetaRewriteApplied).
var PerFileMergeApplied int64

// perFileScans expands a multi-file, recipe-matched keyspace into K per-file
// datastore-scan-records leaf ops (each backed by a single-file flatKeyspace) plus
// the per-child merge-scan Params (sortedness/minKeys/maxKeys/bounds) derived from
// each file's memoized SortedSourceMeta. metas is the keyspace's per-file metadata
// (from SortedSourceMetasForKeyspace); scanLabels is the label set each leaf scan
// advertises (the branch/asof leaf's own [".alias","^id"]); keyName is the proven
// normalized sort-key field every file must carry. conv.AddTemp registers each
// single-file keyspacer into the SAME Temps the execution reads.
//
// ok=false (the caller keeps its single whole-keyspace child) when: the feature is
// gated off, there are < 2 recipe files, or any file is unsorted / not keyed by
// keyName. When EVERY file carries a zone map (RecordCount > 0) the children are
// returned sorted by MinKey (aligned arrays) so the engine's "auto" regime can prove
// disjoint files concatenate; if any file lacks a zone the zone map is dropped
// (minKeys/maxKeys returned empty -> heap/watermark), matching aggregateBranchMeta.
//
// Cold path (plan time): the per-file ops + Temps entries are fine to allocate; the
// merge steady state stays zero-alloc, and each single-file scan lazy-opens its file.
func perFileScans(metas []FileSortedSourceMeta, scanLabels base.Labels, keyName string,
	conv *Conv) (scans []*base.Op, sortedness, minKeys, maxKeys, bounds []interface{}, ok bool) {
	if !EnablePerFileMergeScans || len(metas) < 2 || conv == nil {
		return nil, nil, nil, nil, nil, false
	}

	type perFile struct {
		scan              *base.Op
		sorted            string
		minK, maxK, bound int64
		hasZone           bool
	}
	files := make([]perFile, 0, len(metas))
	zoneOK := true

	for _, fm := range metas {
		m := fm.Meta
		if m.SortKeyLabel != keyName || m.Sortedness == records.SortedNone {
			return nil, nil, nil, nil, nil, false // not the proven sorted source
		}
		sorted := records.SortedStrict
		if m.Sortedness == records.SortedNear {
			sorted = records.SortedNear
		}
		idx := conv.AddTemp(asofKeyspacer{ks: &flatKeyspace{file: fm.Path}})
		scan := &base.Op{
			Kind:   "datastore-scan-records",
			Labels: append(base.Labels{}, scanLabels...),
			Params: []interface{}{idx},
		}
		pf := perFile{scan: scan, sorted: sorted, bound: m.Disorder.WindowNanos}
		if m.RecordCount > 0 {
			pf.minK, pf.maxK, pf.hasZone = m.MinKey, m.MaxKey, true
		} else {
			zoneOK = false
		}
		files = append(files, pf)
	}

	// A full zone map lets "auto" prove disjointness only if the children are ordered
	// by min key (the consecutive max<=min test); sort them so disjoint daily files
	// concatenate regardless of walk order. Overlapping files still fall to heap.
	if zoneOK {
		sort.SliceStable(files, func(i, j int) bool { return files[i].minK < files[j].minK })
	}

	for _, pf := range files {
		scans = append(scans, pf.scan)
		sortedness = append(sortedness, pf.sorted)
		bounds = append(bounds, pf.bound)
		if zoneOK {
			minKeys = append(minKeys, pf.minK)
			maxKeys = append(maxKeys, pf.maxK)
		}
	}
	atomic.AddInt64(&PerFileMergeApplied, 1)
	return scans, sortedness, minKeys, maxKeys, bounds, true
}

// branchScanLeaf returns the single datastore-scan leaf under op and the TOTAL number
// of scan leaves in the subtree. The count lets the caller expand only a branch with
// exactly one scan (a plain project(scan)); a branch with a join/two scans is left as
// a single concatenated child (cloneBranchWithScan would mis-replace both leaves).
func branchScanLeaf(op *base.Op) (leaf *base.Op, count int) {
	if op == nil {
		return nil, 0
	}
	if strings.HasPrefix(op.Kind, "datastore-scan") {
		return op, 1
	}
	for _, c := range op.Children {
		l, n := branchScanLeaf(c)
		count += n
		if l != nil && leaf == nil {
			leaf = l
		}
	}
	return leaf, count
}

// cloneBranchWithScan deep-clones a union branch's op subtree, substituting newScan
// for the (single) datastore-scan leaf -- so a branch's projection is reproduced K
// times, once per per-file scan. Labels/Params are copied at the slice level; their
// elements (label strings, ["exprTree", expr] terms, project-columns) are read-only
// at exec time, so sharing them across clones is safe.
func cloneBranchWithScan(branch, newScan *base.Op) *base.Op {
	if branch == nil {
		return nil
	}
	if strings.HasPrefix(branch.Kind, "datastore-scan") {
		return newScan
	}
	c := &base.Op{
		Kind:   branch.Kind,
		Labels: append(base.Labels{}, branch.Labels...),
		Params: append([]interface{}{}, branch.Params...),
	}
	for _, ch := range branch.Children {
		c.Children = append(c.Children, cloneBranchWithScan(ch, newScan))
	}
	return c
}

// mergeScanParamsMulti builds a K-child merge-scan's Params (engine layout
// [keyIdx, regime, sortedness, minKeys, maxKeys, bounds, policy]) from the aligned
// per-child arrays perFileScans returns. Regime "auto": concatenate when the zone
// maps prove disjoint, else heap (escalating to watermarked-near for a near child).
// Policy "error": a wrong zone-map / disorder-bound claim fails loudly, never mis-orders.
func mergeScanParamsMulti(keyIdx int, sortedness, minKeys, maxKeys, bounds []interface{}) []interface{} {
	return []interface{}{keyIdx, "auto", sortedness, minKeys, maxKeys, bounds, "error"}
}

// -------------------------------------------------------------------
// argmax -> ASOF merge-JOIN lowering (DESIGN-merging.md §3, "the argmax-subquery
// -> ASOF rewrite"; Track B round 4 piece 2).
//
// The RECOGNIZER above (MatchArgmaxAsof) classifies the canonical nearest-
// preceding correlated subquery. This section LOWERS a matched one -- when the
// normalized-int64-sort-key contract is PROVEN for both keyspaces -- into a
// streaming merge-join, replacing the O(N*M) per-outer-row correlated re-drive
// (EvaluateSubquery) with the single linear ASOF pass of engine/op_merge_join.go.
//
// It rewrites the INITIAL project that carries the boxed argmax subquery term
// (child = the outer keyspace E's records scan):
//
//	project[ .ts=e.ts, .state_at=<SUBQUERY>, .e ]        // child = scan(E)
//
// into:
//
//	project[ .ts=e.ts, .state_at=IFMISSING(asofresult, null), .e ]
//	  merge-join asof/soft [leftKey=E.key, rightKey=R.key, left, tol, partIdxs]
//	    merge-scan(E)   over  project[ .e, ^ekey=e.key, <ePart...> ] over scan(E)
//	    merge-scan(R)   over  project[ ^rkey=r.key,
//	                                   asofresult=[{ProjAlias: r.ProjField}],
//	                                   <rPart...> ] over a PLAIN scan(R)
//
// Why the right side carries `[{ProjAlias: r.ProjField}]` per R row: the argmax
// subquery is LIMIT 1, so its result for a matched outer row is exactly the array-
// wrap of the single nearest-preceding R row's projection. Precomputing that value
// per R row means the ASOF-selected right row's column IS the subquery result --
// byte-for-byte -- so the lowered output matches the correlated baseline. A no-
// preceding-row outer (left-outer -> MISSING right) maps to null via IFMISSING,
// matching the empty subquery's value.NewValue(nil) -> NULL.
//
// GATING (the safety net, identical in spirit to WireTemporalMergeMeta): fire ONLY
// when BOTH E and R are recipe-matched with SortedSourceMeta.SortKeyLabel ==
// AsofMatch.KeyField (a proven normalized int64 sort key). Otherwise the correlated
// subquery is left UNTOUCHED (the correct, if slower, fallback). Every merge-scan /
// merge-join also validates ascending order at runtime (policy "error"), so an
// optimistic sortedness aggregation fails loudly rather than mis-orders.

// EnableASOFRewrite gates the argmax->ASOF merge-join LOWERING (this pass MUTATES
// the tree, unlike the read-only EnableASOFRecognize). Default ON: the lowering is
// guarded by proven SortedSourceMeta (both keyspaces' int64 sort key == the argmax
// key) exactly like the UNION-ALL->merge rewrite, so it fires only where
// correctness is provable and is a safe no-op everywhere else.
var EnableASOFRewrite = true

// AsofRewriteApplied counts argmax subqueries this pass lowered into a merge-join
// (test observability, mirroring MergeMetaRewriteApplied / AsofRecognized).
var AsofRewriteApplied int64

// AsofRewriteSkipped counts recognized argmax subqueries this pass did NOT lower --
// each logs the gate that stopped it (see tryLowerASOFProject's skip()), so an author
// can tell WHY a temporal correlation stayed on the O(n^2) correlated path instead of
// silently running a slow query (IDEA-0014 observability).
var AsofRewriteSkipped int64

// asofKeyspacer is the minimal keyspacer (a datastore.Keyspace holder) that backs
// the fresh, PLAIN full ordered scan of the subquery's right keyspace R. The
// correlated argmax sub-plan carries a correlated span (r.key <= e.key); the ASOF
// join enforces that inequality itself, so the right input is rebuilt as an
// unfiltered records scan of R -- not the correlated sub-plan. DatastoreScanRecords
// needs only Keyspace() off its Temps entry (no Limit -> unbounded full scan).
type asofKeyspacer struct{ ks datastore.Keyspace }

func (a asofKeyspacer) Keyspace() datastore.Keyspace { return a.ks }

// WireASOFJoin lowers each proven correlated argmax subquery in the converted tree
// into a streaming merge-join. It is called from PlanConvert with the Conv (to
// register the fresh R scan's keyspace into the SAME Temps execution reads) and the
// QueryPlan (whose Subqueries() hold R's pre-planned sub-plan, from which R's
// keyspace is resolved). A no-op unless EnableASOFRewrite is set; each candidate is
// lowered only when the metadata gate proves both E and R int64 sort keys.
func WireASOFJoin(conv *Conv, qp *plan.QueryPlan) {
	if conv == nil || conv.TopOp == nil || !EnableASOFRewrite || qp == nil {
		return
	}
	// Re-key the pointer-keyed sub-plan map by canonical String() so a lookup by the
	// tree's subquery (same object, but be robust to a re-parsed pointer) hits.
	var byKey map[string]plan.Operator
	if subs := qp.Subqueries(); len(subs) > 0 {
		byKey = make(map[string]plan.Operator, len(subs))
		for sel, op := range subs {
			byKey[subqKey(sel)] = op
		}
	}
	asofWalk(conv.TopOp, conv, byKey)
}

// asofWalk visits every op, trying to lower a project that carries an argmax
// subquery term. It recurses AFTER a lowering (into the new merge-join subtree),
// which contains no further argmax terms, so no re-processing occurs.
func asofWalk(op *base.Op, conv *Conv, byKey map[string]plan.Operator) {
	if op == nil {
		return
	}
	if op.Kind == "project" {
		tryLowerASOFProject(op, conv, byKey)
	}
	for _, c := range op.Children {
		asofWalk(c, conv, byKey)
	}
}

// tryLowerASOFProject attempts the argmax->ASOF lowering on one project op. It is
// paranoid: any deviation from the exact expected shape, or an unproven sort key on
// either keyspace, leaves the project (and its correlated subquery) UNTOUCHED.
func tryLowerASOFProject(p *base.Op, conv *Conv, byKey map[string]plan.Operator) {
	// Safety net: this runs inside PlanConvert (inside Run's recover), so a panic
	// here would fail the WHOLE query -- worse than the correlated baseline. The
	// tree is mutated only at the very end (after all fallible work), so on any
	// panic the project is left untouched and the correct correlated path runs.
	defer func() { _ = recover() }()

	// Find the ONE projection term that is EXACTLY a bare argmax subquery matching the
	// canonical nearest-preceding/-following shape (MatchArgmaxAsof). Requiring the
	// bare subquery (not e.g. `(SELECT ...)[0]`) means replacing the term reproduces
	// the subquery's value precisely. No such term => this project has nothing to
	// lower; return silently.
	termIdx := -1
	var match *AsofMatch
	var sel *algebra.Select
	for i, prm := range p.Params {
		term, ok := prm.([]interface{})
		if !ok || len(term) < 2 {
			continue
		}
		if k, _ := term[0].(string); k != "exprTree" {
			continue
		}
		sq, ok := term[1].(*algebra.Subquery)
		if !ok {
			continue
		}
		if m, matched := MatchArgmaxAsof(sq); matched {
			termIdx, match, sel = i, m, sq.Select()
			break
		}
	}
	if termIdx < 0 || sel == nil {
		return
	}

	// From here a lowerable argmax subquery EXISTS, so every bail is a missed ASOF an
	// author would want to know about: skip() records the gate that stopped it (a -v
	// log line + the AsofRewriteSkipped counter). The correlated baseline still runs
	// correctly; this is the temporal rewrite's "report card" (IDEA-0014), not an error.
	skip := func(reason string) {
		AsofRewriteSkipped++
		base.Logf(1, "glue/asof", "argmax subquery NOT lowered to ASOF (runs as a correlated subquery): %s", reason)
	}

	// The project's child must be the outer keyspace E's records scan, optionally
	// behind ONE outer filter -- the common "correlate only a filtered subset" case
	// (e.g. WHERE e.level="error"). The filter is re-applied to the E probe stream so
	// exactly the same E rows flow through as the correlated baseline; it references
	// only outer (E) fields, resolved against the same scan labels, so it moves down
	// unchanged. Any OTHER intermediate op (a join, a second project, a group) would
	// change the correlated rows -> bail to the safe correlated path.
	if len(p.Children) != 1 {
		skip("the project has multiple children")
		return
	}
	scanE := p.Children[0]
	var outerFilter *base.Op
	if scanE.Kind == "filter" {
		if len(scanE.Children) != 1 {
			skip("outer filter has an unexpected shape")
			return
		}
		outerFilter = scanE
		scanE = scanE.Children[0]
	}
	if scanE.Kind != "datastore-scan-records" || len(scanE.Labels) == 0 {
		skip("an op other than a single WHERE filter sits between the SELECT and its scan")
		return
	}
	outerAlias, ok := mergeLabelLeaf(scanE.Labels[0])
	if !ok {
		return
	}

	// --- GATE: prove E's int64 sort key == the argmax key. -----------------------
	eKS := branchScanKeyspace(scanE, conv.Temps)
	if eKS == nil {
		skip("the outer keyspace could not be resolved")
		return
	}
	eMetas, err := SortedSourceMetasForKeyspace(eKS, nil)
	if err != nil || len(eMetas) == 0 {
		skip("the outer keyspace has no sorted-source metadata -- its recipe must frame a sorted time key")
		return
	}
	eSorted, eMin, eMax, eBound, eZone, eOK := aggregateBranchMeta(eMetas, match.KeyField)
	if !eOK {
		skip("the outer keyspace's sort key is not the argmax key `" + match.KeyField +
			"` (or it is unsorted) -- declare framing time/order on that field")
		return
	}

	// --- resolve R's keyspace from the subquery's pre-planned sub-plan. ----------
	subPlan := byKey[subqKey(sel)]
	if subPlan == nil {
		skip("the subquery's sub-plan was not found")
		return
	}
	subConv := &Conv{Temps: []interface{}{nil}}
	if _, cerr := subPlan.Accept(subConv); cerr != nil || subConv.TopOp == nil {
		skip("the subquery's sub-plan did not convert")
		return
	}
	rKS := branchScanKeyspace(subConv.TopOp, subConv.Temps)
	if rKS == nil {
		skip("the subquery keyspace could not be resolved")
		return
	}
	rMetas, rerr := SortedSourceMetasForKeyspace(rKS, nil)
	if rerr != nil || len(rMetas) == 0 {
		skip("the subquery keyspace has no sorted-source metadata -- its recipe must frame a sorted time key")
		return
	}
	rSorted, rMin, rMax, rBound, rZone, rOK := aggregateBranchMeta(rMetas, match.KeyField)
	if !rOK {
		skip("the subquery keyspace's sort key is not the argmax key `" + match.KeyField +
			"` (or it is unsorted) -- declare framing time/order on that field")
		return
	}

	// --- build the LEFT (probe) input: merge-scan(E) over a key-materializing ----
	// project over scan(E). Keeps the whole E doc (.e) so the outer project's other
	// terms (e.ts, .e) resolve unchanged; adds ^ekey = e.<KeyField> (int64) and one
	// column per partition-eq outer field. mkLeft wraps ONE E scan (the whole-keyspace
	// scanE, or a per-file scan when E resolves to multiple files -- see perFileScans).
	eKeyLabel := "^ekey"
	// withFilter re-applies the outer WHERE (if any) to an E scan before the key-
	// materializing project, so only the filtered E rows enter the merge-join -- the
	// same rows the correlated baseline saw. The filter passes labels through, so the
	// project's sc.Labels[0] reference is unchanged. A per-file E expansion clones the
	// filter over each file scan (sharing the read-only predicate Params is safe).
	withFilter := func(sc *base.Op) *base.Op {
		if outerFilter == nil {
			return sc
		}
		return &base.Op{
			Kind:     "filter",
			Labels:   append(base.Labels{}, sc.Labels...),
			Params:   outerFilter.Params,
			Children: []*base.Op{sc},
		}
	}
	mkLeft := func(sc *base.Op) *base.Op {
		lp := &base.Op{
			Kind:     "project",
			Labels:   base.Labels{sc.Labels[0], eKeyLabel},
			Params:   []interface{}{[]interface{}{"labelPath", sc.Labels[0]}, exprTerm(fieldRef(outerAlias, match.KeyField))},
			Children: []*base.Op{withFilter(sc)},
		}
		for i, eq := range match.PartitionEq {
			lp.Labels = append(lp.Labels, "^epart"+itoa(i))
			lp.Params = append(lp.Params, exprTerm(fieldRef(outerAlias, eq.OuterField)))
		}
		return lp
	}
	leftKeyIdx := 1 // position of ^ekey in the left project's Labels
	var leftParts []interface{}
	for i := range match.PartitionEq {
		leftParts = append(leftParts, 2+i) // ^epart_i sits after .e(0), ^ekey(1)
	}
	var msE *base.Op
	if scans, sPer, minPer, maxPer, bndPer, okE :=
		perFileScans(eMetas, scanE.Labels, match.KeyField, conv); okE {
		children := make([]*base.Op, len(scans))
		for i, sc := range scans {
			children[i] = mkLeft(sc)
		}
		msE = &base.Op{
			Kind:     "merge-scan",
			Labels:   append(base.Labels{}, children[0].Labels...),
			Params:   mergeScanParamsMulti(leftKeyIdx, sPer, minPer, maxPer, bndPer),
			Children: children,
		}
	} else {
		lp := mkLeft(scanE)
		msE = &base.Op{
			Kind:     "merge-scan",
			Labels:   append(base.Labels{}, lp.Labels...),
			Params:   mergeScanParams(leftKeyIdx, eSorted, eMin, eMax, eBound, eZone),
			Children: []*base.Op{lp},
		}
	}

	// --- build the RIGHT (build) input: merge-scan(R) over a project over a fresh --
	// PLAIN scan(R). Produces ^rkey = r.<KeyField> (int64) and asofresult =
	// [{ProjAlias: r.ProjField}] -- the array-wrapped single-row subquery projection.
	// mkRight wraps ONE R scan; R also per-file-expands when it resolves to K files
	// (the cross-node state keyspace case).
	rScanLabels := base.Labels{"." + LabelSuffix(match.RightAlias), "^id"}
	rKeyLabel := "^rkey"
	asofResultLabel := ".[\"" + asofResultField + "\"]"
	mkRight := func(sc *base.Op) *base.Op {
		rp := &base.Op{
			Kind:   "project",
			Labels: base.Labels{rKeyLabel, asofResultLabel},
			Params: []interface{}{
				exprTerm(fieldRef(match.RightAlias, match.KeyField)),
				exprTerm(asofResultExpr(match)),
			},
			Children: []*base.Op{sc},
		}
		for i, eq := range match.PartitionEq {
			rp.Labels = append(rp.Labels, "^rpart"+itoa(i))
			rp.Params = append(rp.Params, exprTerm(fieldRef(match.RightAlias, eq.RightField)))
		}
		return rp
	}
	rightKeyIdx := 0 // position of ^rkey in the right project's Labels
	var rightParts []interface{}
	for i := range match.PartitionEq {
		rightParts = append(rightParts, 2+i) // ^rpart_i sits after ^rkey(0), asofresult(1)
	}
	var msR *base.Op
	if scans, sPer, minPer, maxPer, bndPer, okR :=
		perFileScans(rMetas, rScanLabels, match.KeyField, conv); okR {
		children := make([]*base.Op, len(scans))
		for i, sc := range scans {
			children[i] = mkRight(sc)
		}
		msR = &base.Op{
			Kind:     "merge-scan",
			Labels:   append(base.Labels{}, children[0].Labels...),
			Params:   mergeScanParamsMulti(rightKeyIdx, sPer, minPer, maxPer, bndPer),
			Children: children,
		}
	} else {
		rScanIdx := conv.AddTemp(asofKeyspacer{ks: rKS})
		scanR := &base.Op{
			Kind:   "datastore-scan-records",
			Labels: append(base.Labels{}, rScanLabels...),
			Params: []interface{}{rScanIdx},
		}
		rp := mkRight(scanR)
		msR = &base.Op{
			Kind:     "merge-scan",
			Labels:   append(base.Labels{}, rp.Labels...),
			Params:   mergeScanParams(rightKeyIdx, rSorted, rMin, rMax, rBound, rZone),
			Children: []*base.Op{rp},
		}
	}

	// --- build the merge-join. Left-outer so a no-preceding outer row survives with
	// MISSING right cols (mapped to null by the outer term below), matching the empty
	// subquery. asof mode "soft" iff a look-back guard was recognized.
	asofMode := "asof"
	var tolerance interface{}
	if match.Soft {
		asofMode = "soft"
		tolerance = match.ToleranceNanos
	}
	mj := &base.Op{
		Kind:     "merge-join",
		Labels:   append(append(base.Labels{}, msE.Labels...), msR.Labels...),
		Params:   []interface{}{leftKeyIdx, rightKeyIdx, "left", asofMode, tolerance, leftParts, rightParts},
		Children: []*base.Op{msE, msR},
	}

	// --- rewire the project: child = merge-join; the argmax term now reads the joined
	// right asofresult column, mapping MISSING (no preceding row) -> null.
	p.Children[0] = mj
	p.Params[termIdx] = exprTerm(expression.NewIfMissing(
		fieldRef("", asofResultField), expression.NewConstant(nil)))

	AsofRewriteApplied++
}

// asofResultField is the synthetic field name the right side carries the array-
// wrapped subquery projection under; the outer project reads it back by this name.
const asofResultField = "asofresult"

// mergeScanParams builds a single-branch merge-scan's Params (engine layout
// [keyIdx, regime, sortedness, minKeys, maxKeys, bounds, policy]) from the branch's
// aggregated SortedSourceMeta. A "near" branch forces the "heap" regime so the op
// escalates to the watermarked-near reorder (which normalizes near -> strict for the
// downstream merge-join); a "strict" branch uses "concatenate" (pass-through with a
// monotonicity tripwire). Zone maps ride only the strict/concatenate case.
func mergeScanParams(keyIdx int, sorted string, minK, maxK, bound int64, zoneOK bool) []interface{} {
	regime := "concatenate"
	var minKeys, maxKeys []interface{}
	if sorted == records.SortedNear {
		regime = "heap" // -> watermarked-near (a single near source still reorders).
	} else if zoneOK {
		minKeys = []interface{}{minK}
		maxKeys = []interface{}{maxK}
	}
	return []interface{}{
		keyIdx, regime,
		[]interface{}{sorted},
		minKeys, maxKeys,
		[]interface{}{bound},
		"error",
	}
}

// exprTerm wraps a cbq expression as a project op's ["exprTree", expr] param term.
func exprTerm(e expression.Expression) []interface{} {
	return []interface{}{"exprTree", e}
}

// fieldRef builds a field-access expression alias.field, or a bare identifier when
// alias is "" (used for the synthetic asofresult column, which the outer project
// resolves as a plain field of the joined row).
func fieldRef(alias, field string) expression.Expression {
	if alias == "" {
		return expression.NewIdentifier(field)
	}
	return expression.NewField(expression.NewIdentifier(alias),
		expression.NewFieldName(field, false))
}

// asofResultExpr builds the per-R-row array-wrapped projection [{ProjAlias:
// r.ProjField}] -- exactly the value the LIMIT 1 argmax subquery yields for the
// outer row this R row is the nearest-preceding of. Built via cbq's object/array
// constructors so its serialization matches the correlated baseline byte-for-byte.
func asofResultExpr(m *AsofMatch) expression.Expression {
	obj := expression.NewObjectConstruct(map[expression.Expression]expression.Expression{
		expression.NewConstant(m.ProjAlias): fieldRef(m.RightAlias, m.ProjField),
	})
	return expression.NewArrayConstruct(obj)
}

// itoa is a tiny int->string for building distinct synthetic partition labels
// without pulling strconv into this file's cold path.
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	pos := len(b)
	neg := i < 0
	if neg {
		i = -i
	}
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		b[pos] = '-'
	}
	return string(b[pos:])
}
