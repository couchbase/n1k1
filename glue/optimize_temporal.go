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
// the columnar path. Once Track A wires SortedSourceMeta through glue, this gate
// becomes "fire when the branches carry a proven int64 sort key", and the
// per-branch sortedness/zone-map Params below are read from that meta rather than
// defaulted.

import (
	"encoding/json"
	"strings"

	"github.com/couchbase/query/algebra"
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
