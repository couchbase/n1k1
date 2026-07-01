//go:build n1ql

//  Copyright (c) 2019 Couchbase, Inc.
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
	"math"
	"sort"
	"strings"

	"github.com/couchbase/query/algebra"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/plan"

	"github.com/couchbase/n1k1/base"
)

// Conv implements the conversion of a couchbase/query/plan into a
// n1k1 base.Op tree. It implements the plan.Visitor interface.
type Conv struct {
	// Temps holds the slots of vars.Temps being built up.
	Temps []interface{}

	// TopPlan holds the top-most plan operator that was converted.
	TopPlan plan.Operator

	// TopOp holds top of the converted base.Op tree.
	TopOp *base.Op

	// withBindings maps a WITH CTE alias to its binding, so a later `FROM <alias>`
	// (an ExpressionScan over the identifier) can be handled: a non-recursive
	// binding is inlined to its Expression(); a recursive one becomes a
	// with-recursive fixpoint op. Populated by VisitWith.
	withBindings map[string]expression.With
}

// -------------------------------------------------------------------

// ExecConv converts a plan.Operator into a base.Op.
func ExecConv(p plan.Operator) (*base.Op, []interface{}, error) {
	// The 0'th temps slot is prealloc'ed for the execution context.
	c := &Conv{Temps: []interface{}{nil}}

	_, err := p.Accept(c)

	return c.TopOp, c.Temps, err
}

// -------------------------------------------------------------------

// AddTemp appends an object to the Temps, returning its slot index.
func (c *Conv) AddTemp(t interface{}) int {
	rv := len(c.Temps)
	c.Temps = append(c.Temps, t)
	return rv
}

// TopPush pushes an operator as the new top operator, chaining the
// previous top operator as a child.
func (c *Conv) TopPush(p plan.Operator, op *base.Op) (*base.Op, error) {
	if c.TopOp != nil {
		op.Children = append(op.Children, c.TopOp)
	}
	return c.TopSet(p, op)
}

// TopSet sets the operator as the top operator.
func (c *Conv) TopSet(p plan.Operator, op *base.Op) (*base.Op, error) {
	c.TopPlan, c.TopOp = p, op
	return op, nil
}

// WithBindings returns the WITH CTE bindings this conversion discovered (alias
// -> binding), for threading into subquery sub-conversions (see
// GlueContext.InitSubqueries). May be nil.
func (c *Conv) WithBindings() map[string]expression.With { return c.withBindings }

// -------------------------------------------------------------------

// LabelSuffix converts a string into label syntax.
func LabelSuffix(s string) string {
	if s != "" {
		return `["` + s + `"]`
	}
	return s
}

// projAllFieldPaths reports whether every label is a plain `.["..."]` field
// path (not "." whole-row, ".*" star-spread, or a "^" attachment). The ORDER-BY
// source-scope augmentation only applies to such projections.
func projAllFieldPaths(labels base.Labels) bool {
	for _, l := range labels {
		if !strings.HasPrefix(l, ".[") {
			return false
		}
	}
	return len(labels) > 0
}

// stripBindingNames wraps a SELECT-* star expression so that the given LET /
// WITH binding variable names are removed from the spread, via OBJECT_REMOVE.
// With no binding names it returns the expression unchanged (so non-LET star
// queries are unaffected). Names are sorted for deterministic codegen.
func stripBindingNames(e expression.Expression, names map[string]bool) expression.Expression {
	if e == nil || len(names) == 0 {
		return e
	}

	sorted := make([]string, 0, len(names))
	for name := range names {
		sorted = append(sorted, name)
	}
	sort.Strings(sorted)

	operands := make(expression.Expressions, 0, len(sorted)+1)
	operands = append(operands, e)
	for _, name := range sorted {
		operands = append(operands, expression.NewConstant(name))
	}

	return expression.NewObjectRemove(operands...)
}

// -------------------------------------------------------------------

// Scan

func (c *Conv) VisitPrimaryScan(o *plan.PrimaryScan) (interface{}, error) {
	if o.Term().Namespace() == "#system" {
		return NA(o)
	}

	return c.recordsScan(o, o.Term().Alias())
}

func (c *Conv) VisitPrimaryScan3(o *plan.PrimaryScan3) (interface{}, error) {
	if o.Term().Namespace() == "#system" {
		return NA(o)
	}

	return c.recordsScan(o, o.Term().Alias())
}

// recordsScan emits a datastore-scan-records op that reads the keyspace's
// directory n1k1-native (recordsource: union of files, recurse, decode,
// transparent gzip) and yields whole documents (".alias") plus "^id" directly,
// replacing cbq's scan-keys + fetch-docs round-trip for the file datastore (see
// DESIGN-data.md "Where this code lives" A2). A following plan.Fetch over the
// same keyspace becomes a no-op pass-through (see VisitFetch).
func (c *Conv) recordsScan(o plan.Operator, alias string) (interface{}, error) {
	return c.TopPush(o, &base.Op{
		Kind:   "datastore-scan-records",
		Labels: base.Labels{"." + LabelSuffix(alias), "^id"},
		Params: []interface{}{c.AddTemp(o)},
	})
}

func (c *Conv) VisitIndexScan(o *plan.IndexScan) (interface{}, error) {
	return c.TopPush(o, &base.Op{
		Kind:   "datastore-scan-index",
		Labels: base.Labels{"^id"},
		Params: []interface{}{c.AddTemp(o)},
	})
}

func (c *Conv) VisitIndexScan2(o *plan.IndexScan2) (interface{}, error) { return NA(o) }
func (c *Conv) VisitIndexScan3(o *plan.IndexScan3) (interface{}, error) { return NA(o) }

func (c *Conv) VisitKeyScan(o *plan.KeyScan) (interface{}, error) {
	return c.TopPush(o, &base.Op{
		Kind:   "datastore-scan-keys",
		Labels: base.Labels{"^id"},
		Params: []interface{}{c.AddTemp(o)},
	})
}

func (c *Conv) VisitValueScan(o *plan.ValueScan) (interface{}, error) { return NA(o) } // Used for mutations (VALUES clause).

func (c *Conv) VisitDummyScan(o *plan.DummyScan) (interface{}, error) {
	return c.TopPush(o, &base.Op{Kind: "nil"})
}

func (c *Conv) VisitCountScan(o *plan.CountScan) (interface{}, error)           { return NA(o) }
func (c *Conv) VisitIndexCountScan(o *plan.IndexCountScan) (interface{}, error) { return NA(o) }
func (c *Conv) VisitIndexCountScan2(o *plan.IndexCountScan2) (interface{}, error) {
	return NA(o)
}
func (c *Conv) VisitIndexCountDistinctScan2(o *plan.IndexCountDistinctScan2) (interface{}, error) {
	return NA(o)
}
func (c *Conv) VisitDistinctScan(o *plan.DistinctScan) (interface{}, error)   { return NA(o) }
func (c *Conv) VisitUnionScan(o *plan.UnionScan) (interface{}, error)         { return NA(o) }
func (c *Conv) VisitIntersectScan(o *plan.IntersectScan) (interface{}, error) { return NA(o) }
func (c *Conv) VisitOrderedIntersectScan(o *plan.OrderedIntersectScan) (interface{}, error) {
	return NA(o)
}

func (c *Conv) VisitExpressionScan(o *plan.ExpressionScan) (interface{}, error) {
	// A correlated FROM identifier is a CTE reference whose binding lives in an
	// outer scope -- e.g. a WITH RECURSIVE step's `FROM r`, where r is the latest
	// working set. expr-scan resolves it against GlueContext.corrParent at
	// runtime, so allow it. A correlated *subquery* FROM-expr (FROM (SELECT ...
	// outer.x)) isn't supported yet.
	if o.IsCorrelated() {
		if _, isID := o.FromExpr().(*expression.Identifier); !isID {
			return NA(o)
		}
	}

	// The FROM expression: a constant (FROM [1,2,3] AS x), a subquery
	// (FROM (SELECT ...) AS x), or a WITH CTE reference (FROM cte AS x, an
	// identifier).
	expr := o.FromExpr()
	if id, ok := expr.(*expression.Identifier); ok {
		if w, ok := c.withBindings[id.Identifier()]; ok {
			if w.IsRecursive() {
				// FROM <recursive-cte>: run the fixpoint (anchor + repeated step)
				// at runtime; the whole binding is passed to the with-recursive op.
				return c.TopPush(o, &base.Op{
					Kind:   "with-recursive",
					Labels: base.Labels{"." + LabelSuffix(o.Alias())},
					Params: []interface{}{c.AddTemp(w)},
				})
			}
			expr = w.Expression() // non-recursive CTE: inline the binding
		}
	}

	// Evaluate the expression at runtime (via the "expr-scan" glue op) rather
	// than at convert time: a subquery / CTE binding needs the engine + the
	// datastore, which only exist at runtime. The live expression is passed
	// through a vars.Temps slot (like datastore ops carry their plan objects).
	return c.TopPush(o, &base.Op{
		Kind:   "expr-scan",
		Labels: base.Labels{"." + LabelSuffix(o.Alias())},
		Params: []interface{}{c.AddTemp(expr)},
	})
}

// FTS Search

func (c *Conv) VisitIndexFtsSearch(o *plan.IndexFtsSearch) (interface{}, error) { return NA(o) }

// Fetch

func (c *Conv) VisitFetch(o *plan.Fetch) (interface{}, error) {
	if len(o.SubPaths()) > 0 {
		return NA(o) // TODO.
	}

	// A file-datastore primary scan (datastore-scan-records) already materialized
	// whole documents, so the fetch is a no-op pass-through. (Index scans still
	// yield only keys, so their fetch stays a real datastore-fetch below.)
	if c.TopOp != nil && c.TopOp.Kind == "datastore-scan-records" {
		return c.TopOp, nil
	}

	return c.TopPush(o, &base.Op{
		Kind:   "datastore-fetch",
		Labels: base.Labels{"." + LabelSuffix(o.Term().Alias()), "^id"},
		Params: []interface{}{c.AddTemp(o)},
	})
}

func (c *Conv) VisitDummyFetch(o *plan.DummyFetch) (interface{}, error) { return NA(o) } // Used for mutations.

// Join

func (c *Conv) VisitJoin(o *plan.Join) (interface{}, error) {
	// Allocate a vars.Temps slot to hold evaluated keys.
	varsTempsSlot := c.AddTemp(nil)

	// The output preserves the left input's labels (derived from the left child
	// rather than assuming the left plan is a keyspace Termer -- it isn't when
	// the left is an UNNEST or another join) and appends the joined keyspace's
	// doc+id. Matches OpJoinNestedLoop's runtime Children[0]+Children[1] labels.
	rv := &base.Op{
		Kind: "joinKeys-inner",
		Labels: append(append(base.Labels{}, c.TopOp.Labels...),
			"."+LabelSuffix(o.Term().Alias()), "^id"),
		Params: []interface{}{
			// The vars.Temps slot that holds evaluated keys.
			varsTempsSlot,
			// The expression that will evaluate to the keys.
			[]interface{}{"exprTree", o.Term().JoinKeys()},
		},
		Children: []*base.Op{
			c.TopOp,
			&base.Op{
				Kind:   "datastore-fetch",
				Labels: base.Labels{"." + LabelSuffix(o.Term().Alias()), "^id"},
				Params: []interface{}{c.AddTemp(o)},
				Children: []*base.Op{&base.Op{
					Kind:   "temp-yield-var",
					Labels: base.Labels{"^id"},
					Params: []interface{}{varsTempsSlot},
				}},
			}},
	}

	if o.Outer() {
		rv.Kind = "joinKeys-leftOuter"
	}

	return c.TopSet(o, rv)
}

func (c *Conv) VisitIndexJoin(o *plan.IndexJoin) (interface{}, error) { return NA(o) }
func (c *Conv) VisitNest(o *plan.Nest) (interface{}, error)           { return NA(o) }
func (c *Conv) VisitIndexNest(o *plan.IndexNest) (interface{}, error) { return NA(o) }

func (c *Conv) VisitUnnest(o *plan.Unnest) (interface{}, error) {
	// The output preserves the left input's labels (whatever they are -- a
	// keyspace doc+id, or, for chained UNNEST / UNNEST over a join, several
	// labels) and appends the unnested element under the UNNEST alias. Derive
	// them from the left child's labels rather than assuming the left plan is a
	// keyspace Termer (it isn't when the left is itself an UNNEST or a join).
	// This also matches how OpJoinNestedLoop assembles vals at runtime
	// (Children[0].Labels + Children[1].Labels).
	unnestLabel := "." + LabelSuffix(o.Term().Alias())

	rv := &base.Op{
		Kind:   "unnest-inner",
		Labels: append(append(base.Labels{}, c.TopOp.Labels...), unnestLabel),
		Params: []interface{}{
			// The expression to unnest.
			"exprTree", o.Term().Expression(),
		},
		Children: []*base.Op{
			c.TopOp,
			&base.Op{
				Kind:   "noop",
				Labels: base.Labels{unnestLabel},
			}},
	}

	if o.Term().Outer() {
		rv.Kind = "unnest-leftOuter"
	}

	c.TopSet(o, rv)

	// The planner pushes a post-UNNEST predicate (e.g. WHERE child.x = ...)
	// into the Unnest operator rather than emitting a separate Filter, so
	// apply it here as a filter on the unnested output.
	if f := o.Filter(); f != nil {
		return c.TopPush(o, &base.Op{
			Kind:   "filter",
			Labels: c.TopOp.Labels,
			Params: []interface{}{"exprTree", f},
		})
	}

	return c.TopOp, nil
}

func (c *Conv) VisitNLJoin(o *plan.NLJoin) (interface{}, error)     { return NA(o) }
func (c *Conv) VisitNLNest(o *plan.NLNest) (interface{}, error)     { return NA(o) }
func (c *Conv) VisitHashJoin(o *plan.HashJoin) (interface{}, error) { return NA(o) }
func (c *Conv) VisitHashNest(o *plan.HashNest) (interface{}, error) { return NA(o) }

// Let + Letting, With

// VisitLet converts a LET clause: each binding computes a named variable that
// later clauses (WHERE / ORDER BY / projection) can reference. Model it as a
// "project" that passes through every existing column unchanged and appends one
// computed column per binding, labeled .["<var>"], so a downstream expression
// referencing the variable resolves it as a field (matching query, which does
// item.SetField(variable, val); see execution/let.go). SELECT * must not spread
// these added columns -- VisitInitialProject strips the binding names from the
// star (see stripBindingNames).
func (c *Conv) VisitLet(o *plan.Let) (interface{}, error) {
	// Stack one pass-through "project" per binding (rather than one project for
	// all of them) so a later binding can reference an earlier one -- e.g.
	// LET x = 1, y = x + 1 -- matching query's left-to-right LET scoping, where
	// each binding is added to the item before the next is evaluated.
	for _, b := range o.Bindings() {
		src := c.TopOp

		op := &base.Op{
			Kind:   "project",
			Labels: append(base.Labels{}, src.Labels...),
			Params: make([]interface{}, 0, len(src.Labels)+1),
		}

		// Pass through every existing column unchanged.
		for _, lbl := range src.Labels {
			op.Params = append(op.Params, []interface{}{"labelPath", lbl})
		}

		// Append this binding's computed column.
		op.Labels = append(op.Labels, "."+LabelSuffix(b.Variable()))
		op.Params = append(op.Params, []interface{}{"exprTree", b.Expression()})

		c.TopPush(o, op)
	}

	return c.TopOp, nil
}

// VisitWith converts a WITH clause (CTE): record each binding's alias ->
// expression, then visit the wrapped child. A later `FROM <alias>` is an
// ExpressionScan over the identifier <alias>; VisitExpressionScan inlines it by
// evaluating the recorded binding expression (a constant, or a subquery run via
// EvaluateSubquery). Bindings that don't feed the row (unreferenced CTEs) are
// simply unused; SELECT * won't leak a WITH name (never added as a field, and
// VisitInitialProject strips project binding names from the star).
func (c *Conv) VisitWith(o *plan.With) (interface{}, error) {
	if c.withBindings == nil {
		c.withBindings = map[string]expression.With{}
	}
	for _, w := range o.Bindings().Bindings() {
		c.withBindings[w.Alias()] = w
	}
	return o.Child().Accept(c)
}

// Filter

func (c *Conv) VisitFilter(o *plan.Filter) (interface{}, error) {
	return c.TopPush(o, &base.Op{
		Kind:   "filter",
		Labels: c.TopOp.Labels,
		Params: []interface{}{"exprTree", o.Condition()},
	})
}

// Group

func (c *Conv) VisitInitialGroup(o *plan.InitialGroup) (interface{}, error) {
	return c.TopOp, nil // Skip as the final group will handle grouping.
}

func (c *Conv) VisitIntermediateGroup(o *plan.IntermediateGroup) (interface{}, error) {
	return c.TopOp, nil // Skip as the final group will handle grouping.
}

func (c *Conv) VisitFinalGroup(o *plan.FinalGroup) (interface{}, error) {
	var labels base.Labels
	var groups []interface{}

	for _, key := range o.Keys() {
		// TODO: Only works for simple GROUP BY expressions on field names,
		// not grouping on general expressions. The reason is the generated
		// label here is only on field names, and a later projection
		// is based on the full expression string.
		fieldPath, ok := ExprFieldPath(key)
		if !ok {
			return nil, fmt.Errorf("VisitFinalGroup, ExprFieldPath, key: %v", key)
		}

		labels = append(labels, "."+LabelSuffix(strings.Join(fieldPath, `","`)))
		groups = append(groups, []interface{}{"exprTree", key})
	}

	var aggExprs []interface{}
	var aggCalcs []interface{}

	for _, agg := range o.Aggregates() {
		// TODO: Optimize as one aggExpr can support >=1 aggCalc.
		operands := agg.Operands()
		if len(operands) == 0 || operands[0] == nil {
			// COUNT(*) has no operand expression; project a constant so the
			// aggregate sees a non-MISSING value for every input row.
			aggExprs = append(aggExprs, []interface{}{"json", "true"})
		} else {
			aggExprs = append(aggExprs, []interface{}{"exprTree", operands[0]})
		}
		aggName := strings.ToLower(agg.Name())
		if agg.Distinct() && (aggName == "count" || aggName == "array_agg") {
			aggName += "_distinct" // e.g. COUNT(DISTINCT x), ARRAY_AGG(DISTINCT x).
		}
		aggCalcs = append(aggCalcs, []interface{}{aggName})

		labels = append(labels, "^aggregates|"+agg.String())
	}

	return c.TopPush(o, &base.Op{
		Kind:   "group",
		Labels: labels,
		Params: []interface{}{groups, aggExprs, aggCalcs},
	})
}

// Window functions

var DefaultWindowFrame = algebra.NewWindowFrame(algebra.WINDOW_FRAME_RANGE,
	algebra.WindowFrameExtents{
		algebra.NewWindowFrameExtent(nil, algebra.WINDOW_FRAME_UNBOUNDED_PRECEDING),
		algebra.NewWindowFrameExtent(nil, algebra.WINDOW_FRAME_CURRENT_ROW),
	})

func (c *Conv) VisitWindowAggregate(o *plan.WindowAggregate) (interface{}, error) {
	if len(o.Aggregates()) <= 0 {
		return c.TopOp, nil
	}

	var rv *base.Op

	// All the o.Aggregates() have the same PARTITION BY.
	var partitionBys []interface{}
	for _, e := range o.Aggregates()[0].WindowTerm().PartitionBy() {
		partitionBys = append(partitionBys, []interface{}{"exprTree", e})
	}

	for _, agg := range o.Aggregates() {
		// TODO: Perhaps only need to append order-by's when we have
		// extra trackings, for rank, denseRank?
		partitionings := append([]interface{}(nil), partitionBys...) // Clone.
		if agg.WindowTerm().OrderBy() != nil {
			for _, e := range agg.WindowTerm().OrderBy().Expressions() {
				// The asc vs desc is ignored as equality of vals is all
				// that we need to check.
				partitionings = append(partitionings, []interface{}{"exprTree", e})
			}
		}

		partitionSlot := c.AddTemp(nil)

		partitionOp := &base.Op{
			Kind:   "window-partition",
			Labels: c.TopOp.Labels,
			Params: []interface{}{
				partitionSlot,
				partitionings,
				len(partitionBys), // # of the partitioning exprs for PARTITION-BY.
				"",                // TODO: Additional tracking ("rank,denseRank").
			},
			Children: []*base.Op{c.TopOp},
		}

		// Chain the window-partition to the previous round.
		if rv != nil {
			partitionOp.Children = append(partitionOp.Children, rv)
		}

		// TODO: Reuse of a window-partition by multiple
		// aggregates and window-frame instances?

		var wf *algebra.WindowFrame
		if agg.WindowTerm() != nil {
			wf = agg.WindowTerm().WindowFrame()
		}
		if wf == nil {
			wf = DefaultWindowFrame
		}

		frameType := "rows"
		if wf.HasModifier(algebra.WINDOW_FRAME_RANGE) {
			frameType = "range"
		} else if wf.HasModifier(algebra.WINDOW_FRAME_GROUPS) {
			frameType = "groups"
		}

		frameCfg := []interface{}{frameType}

		appendExtent := func(wfe *algebra.WindowFrameExtent) {
			if wfe.HasModifier(algebra.WINDOW_FRAME_CURRENT_ROW) {
				frameCfg = append(frameCfg, "num", 0)
			} else if wfe.HasModifier(algebra.WINDOW_FRAME_VALUE_PRECEDING) {
				// TODO: Handle non-int64 RANGE extent.
				n := EvalExprInt64(nil, wfe.ValueExpression(), nil, 0)
				frameCfg = append(frameCfg, "num", -n)
			} else if wfe.HasModifier(algebra.WINDOW_FRAME_VALUE_FOLLOWING) {
				// TODO: Handle non-int64 RANGE extent.
				n := EvalExprInt64(nil, wfe.ValueExpression(), nil, 0)
				frameCfg = append(frameCfg, "num", n)
			} else {
				// Unbounded preceding or following.
				frameCfg = append(frameCfg, "unbounded", 0)
			}
		}

		wfes := wf.WindowFrameExtents()

		appendExtent(wfes[0])

		if len(wfes) > 1 {
			appendExtent(wfes[1])
		} else {
			// Default to CURRENT ROW for end.
			frameCfg = append(frameCfg, "num", 0)
		}

		frameExclude := "no-others"
		if wf.HasModifier(algebra.WINDOW_FRAME_EXCLUDE_CURRENT_ROW) {
			frameExclude = "current-row"
		} else if wf.HasModifier(algebra.WINDOW_FRAME_EXCLUDE_GROUP) {
			frameExclude = "group"
		} else if wf.HasModifier(algebra.WINDOW_FRAME_EXCLUDE_TIES) {
			frameExclude = "ties"
		}

		frameCfg = append(frameCfg, frameExclude)

		// TODO: Specify the val to compare for RANGE type.
		valIdx := 0

		frameCfg = append(frameCfg, valIdx)

		framesSlot := c.AddTemp(nil)

		framesOp := &base.Op{
			Kind:   "window-frames",
			Labels: c.TopOp.Labels, // TODO: Handle extra trackings.
			Params: []interface{}{
				partitionSlot,
				framesSlot,
				[]interface{}{frameCfg},
			},
			Children: []*base.Op{partitionOp},
		}

		// TODO: agg modifiers are DISTINCT, RESPECT|IGNORE NULLS, FROM FIRST|LAST.

		rv = framesOp
	}

	return c.TopSet(o, rv)
}

// Project

func (c *Conv) VisitInitialProject(o *plan.InitialProject) (interface{}, error) {
	op := &base.Op{
		Kind:   "project",
		Params: make([]interface{}, 0, len(o.Terms())),
	}

	// SELECT RAW expr (a.k.a. SELECT ELEMENT) yields the projected value
	// itself as each result, not wrapped under an alias -- so label it "."
	// (the whole value) rather than .["alias"].
	raw := o.Projection() != nil && o.Projection().Raw()

	for _, term := range o.Terms() {
		rt := term.Result()

		// A star term (SELECT *, SELECT path.*) spreads the fields of its
		// (object) value into the result row -- and yields no fields when the
		// value is not an object (N1QL: path.* over a scalar => {}). The ".*"
		// label tells Convert to merge, which also lets multiple stars (and a
		// star mixed with plain terms) combine into one object.
		if rt.Star() {
			// SELECT * spreads the whole row; LET / WITH binding variables live
			// in the row as fields (see VisitLet) but must not appear in *, so
			// strip them -- matching query, which UnsetFields the binding names
			// from the star value (execution/project_initial.go).
			op.Labels = append(op.Labels, ".*")
			op.Params = append(op.Params,
				[]interface{}{"exprTree", stripBindingNames(rt.Expression(), o.BindingNames())})
			continue
		}

		label := "." + LabelSuffix(rt.Alias())
		if raw {
			label = "."
		}
		op.Labels = append(op.Labels, label)
		op.Params = append(op.Params,
			[]interface{}{"exprTree", rt.Expression()})
	}

	return c.TopPush(o, op)
}

func (c *Conv) VisitIndexCountProject(o *plan.IndexCountProject) (interface{}, error) {
	return NA(o)
}

// Distinct

func (c *Conv) VisitDistinct(o *plan.Distinct) (interface{}, error) {
	if c.TopOp.Kind == "distinct" {
		// N1QL planner produces multiple, nested distinct's, so
		// filter away the last one of them...
		// Sequence[Scan, Parallel[Sequence[InitialProject, Distinct, FinalProject]], Distinct].
		return c.TopOp, nil
	}

	return c.TopPush(o, &base.Op{
		Kind:   "distinct",
		Labels: c.TopOp.Labels,
		Params: []interface{}{
			[]interface{}{
				// TODO: This expression might not be enough for the DISTINCT?
				[]interface{}{"labelPath", c.TopOp.Labels[0]},
			},
		},
	})
}

// Set operators

func (c *Conv) VisitUnionAll(o *plan.UnionAll) (interface{}, error)         { return NA(o) }
func (c *Conv) VisitIntersectAll(o *plan.IntersectAll) (interface{}, error) { return NA(o) }
func (c *Conv) VisitExceptAll(o *plan.ExceptAll) (interface{}, error)       { return NA(o) }

// Order, Paging

func (c *Conv) VisitOrder(o *plan.Order) (interface{}, error) {
	exprs, dirs := []interface{}{}, []interface{}{}

	for _, term := range o.Terms() {
		exprs = append(exprs, []interface{}{"exprTree", term.Expression()})

		if term.Descending(nil, nil) { // nil item/context is fine for constant ASC/DESC.
			dirs = append(dirs, "desc")
		} else {
			dirs = append(dirs, "asc")
		}

		if term.NullsPosExpr() != nil {
			return NA(o) // TODO: One day handle non-natural nulls ordering.
		}
	}

	params := []interface{}{exprs, dirs}

	// Modern query folds OFFSET/LIMIT into the Order operator (rather than
	// emitting separate plan.Offset/plan.Limit operators after it), so read
	// them here. Param slots: [exprs, dirs, offset, limit].
	if o.Offset() != nil {
		offset := EvalExprInt64(nil, o.Offset().Expression(), nil, 0)
		params = append(params, int64(offset))
	}

	if o.Limit() != nil {
		for len(params) < 3 {
			params = append(params, int64(0)) // default offset slot
		}
		limit := EvalExprInt64(nil, o.Limit().Expression(), nil, int64(math.MaxInt64))
		params = append(params, int64(limit))
	}

	// When the order sits directly above a projection, the ORDER BY exprs may
	// reference source fields that projection dropped -- e.g. SELECT dimensions
	// ... ORDER BY dimensions.length, where the planner qualifies the key as
	// (catalog.dimensions).length. The projected row no longer carries the
	// "catalog" source doc, so such keys resolve to MISSING and don't sort.
	// Give the order op a row carrying BOTH the projected columns (so alias-
	// based ORDER BY still resolves) AND the source doc columns (so source-
	// qualified keys resolve), then strip back to just the projected columns.
	if proj := c.TopOp; proj != nil && proj.Kind == "project" &&
		len(proj.Children) == 1 && projAllFieldPaths(proj.Labels) {
		src := proj.Children[0]

		// ORDER BY an aggregate (e.g. count(*)) must sort by the already-
		// projected aggregate column, not re-evaluate the aggregate. The order
		// runs above the projection, whose rows no longer carry the
		// "^aggregates" attachment the group produced -- re-evaluating count(*)
		// there panics (it asserts an AnnotatedValue). The projection already
		// computed it, so rewrite an aggregate order term to a labelPath on the
		// matching projected column (matched by expression text).
		projExprLabel := map[string]string{}
		for i, p := range proj.Params {
			if pp, ok := p.([]interface{}); ok && len(pp) == 2 {
				if name, _ := pp[0].(string); name == "exprTree" {
					if e, ok := pp[1].(expression.Expression); ok {
						projExprLabel[e.String()] = proj.Labels[i]
					}
				}
			}
		}
		for i, term := range o.Terms() {
			if _, isAgg := term.Expression().(algebra.Aggregate); !isAgg {
				continue
			}
			if lbl, ok := projExprLabel[term.Expression().String()]; ok {
				exprs[i] = []interface{}{"labelPath", lbl}
			}
		}

		// Augmented projection: the projected terms, plus a pass-through of the
		// source's doc (`.`-path) labels so the order keys can resolve them.
		aug := &base.Op{
			Kind:     "project",
			Labels:   append(base.Labels{}, proj.Labels...),
			Params:   append([]interface{}{}, proj.Params...),
			Children: proj.Children,
		}
		for _, srcLabel := range src.Labels {
			if srcLabel[0] == '.' && aug.Labels.IndexOf(srcLabel) < 0 {
				aug.Labels = append(aug.Labels, srcLabel)
				aug.Params = append(aug.Params, []interface{}{"labelPath", srcLabel})
			}
		}

		orderOp := &base.Op{
			Kind:     "order-offset-limit",
			Labels:   aug.Labels,
			Params:   params,
			Children: []*base.Op{aug},
		}

		// Strip back to just the originally-projected columns.
		strip := &base.Op{
			Kind:     "project",
			Labels:   append(base.Labels{}, proj.Labels...),
			Children: []*base.Op{orderOp},
		}
		for _, lbl := range proj.Labels {
			strip.Params = append(strip.Params, []interface{}{"labelPath", lbl})
		}

		return c.TopSet(o, strip)
	}

	return c.TopPush(o, &base.Op{
		Kind:   "order-offset-limit",
		Labels: c.TopOp.Labels,
		Params: params,
	})
}

// orderFoldTarget returns the order-offset-limit op that a separate
// plan.Offset/plan.Limit should fold its paging into, or nil if none. It looks
// through the "strip" project that the ORDER-BY source-scope augmentation
// leaves directly above the inner order op (see VisitOrder), so paging folds
// into that order rather than spawning a redundant outer wrapper.
func orderFoldTarget(top *base.Op) *base.Op {
	if top == nil {
		return nil
	}
	if top.Kind == "order-offset-limit" {
		return top
	}
	if top.Kind == "project" && len(top.Children) == 1 &&
		top.Children[0].Kind == "order-offset-limit" {
		return top.Children[0]
	}
	return nil
}

func (c *Conv) VisitOffset(o *plan.Offset) (interface{}, error) {
	offset := EvalExprInt64(nil, o.Expression(), nil, 0)

	if t := orderFoldTarget(c.TopOp); t != nil {
		for len(t.Params) < 3 {
			t.Params = append(t.Params, nil)
		}

		t.Params[2] = int64(offset)

		return c.TopOp, nil
	}

	return c.TopPush(o, &base.Op{
		Kind:   "order-offset-limit",
		Labels: c.TopOp.Labels,
		Params: []interface{}{[]interface{}{}, []interface{}{}, int64(offset)},
	})
}

func (c *Conv) VisitLimit(o *plan.Limit) (interface{}, error) {
	limit := EvalExprInt64(nil, o.Expression(), nil, int64(math.MaxInt64))

	if t := orderFoldTarget(c.TopOp); t != nil {
		for len(t.Params) < 4 {
			t.Params = append(t.Params, nil)
		}

		t.Params[3] = int64(limit)

		return c.TopOp, nil
	}

	return c.TopPush(o, &base.Op{
		Kind:   "order-offset-limit",
		Labels: c.TopOp.Labels,
		Params: []interface{}{[]interface{}{}, []interface{}{}, int64(0), int64(limit)},
	})
}

// Mutations

func (c *Conv) VisitSendInsert(o *plan.SendInsert) (interface{}, error) { return NA(o) }
func (c *Conv) VisitSendUpsert(o *plan.SendUpsert) (interface{}, error) { return NA(o) }
func (c *Conv) VisitSendDelete(o *plan.SendDelete) (interface{}, error) { return NA(o) }
func (c *Conv) VisitClone(o *plan.Clone) (interface{}, error)           { return NA(o) }
func (c *Conv) VisitSet(o *plan.Set) (interface{}, error)               { return NA(o) }
func (c *Conv) VisitUnset(o *plan.Unset) (interface{}, error)           { return NA(o) }
func (c *Conv) VisitSendUpdate(o *plan.SendUpdate) (interface{}, error) { return NA(o) }
func (c *Conv) VisitMerge(o *plan.Merge) (interface{}, error)           { return NA(o) }

// Framework

// VisitAlias wraps each child row under the alias -- e.g. FROM (SELECT ...) AS x
// makes the subquery's rows become {"x": <row>}, so downstream `x.field`
// resolves. The planner emits Alias directly above a FROM-clause subquery's ops.
func (c *Conv) VisitAlias(o *plan.Alias) (interface{}, error) {
	return c.TopPush(o, &base.Op{
		Kind:   "project",
		Labels: base.Labels{"." + LabelSuffix(o.Alias())},
		Params: []interface{}{
			[]interface{}{"exprTree", expression.NewSelf()},
		},
	})
}

func (c *Conv) VisitAuthorize(o *plan.Authorize) (interface{}, error) {
	// TODO: Need a real authorize operation here one day?
	return o.Child().Accept(c)
}

func (c *Conv) VisitParallel(o *plan.Parallel) (interface{}, error) {
	// TODO: One day implement parallel correctly, but stay serial for now.
	return o.Child().Accept(c)
}

func (c *Conv) VisitSequence(o *plan.Sequence) (rv interface{}, err error) {
	// Convert plan.Sequence's children into a branch of descendants.
	for _, child := range o.Children() {
		_, err := child.Accept(c)
		if err != nil {
			return nil, err
		}
	}

	return c.TopOp, nil
}

func (c *Conv) VisitDiscard(o *plan.Discard) (interface{}, error) { return NA(o) }

func (c *Conv) VisitStream(o *plan.Stream) (interface{}, error) {
	return c.TopOp, nil
}

func (c *Conv) VisitCollect(o *plan.Collect) (interface{}, error) { return NA(o) }

// Index DDL

func (c *Conv) VisitCreatePrimaryIndex(o *plan.CreatePrimaryIndex) (interface{}, error) {
	return NA(o)
}

func (c *Conv) VisitCreateIndex(o *plan.CreateIndex) (interface{}, error)   { return NA(o) }
func (c *Conv) VisitDropIndex(o *plan.DropIndex) (interface{}, error)       { return NA(o) }
func (c *Conv) VisitAlterIndex(o *plan.AlterIndex) (interface{}, error)     { return NA(o) }
func (c *Conv) VisitBuildIndexes(o *plan.BuildIndexes) (interface{}, error) { return NA(o) }

// Roles

func (c *Conv) VisitGrantRole(o *plan.GrantRole) (interface{}, error)   { return NA(o) }
func (c *Conv) VisitRevokeRole(o *plan.RevokeRole) (interface{}, error) { return NA(o) }

// Explain

func (c *Conv) VisitExplain(o *plan.Explain) (interface{}, error) { return NA(o) }

// Prepare

func (c *Conv) VisitPrepare(o *plan.Prepare) (interface{}, error) { return NA(o) }

// Infer

func (c *Conv) VisitInferKeyspace(o *plan.InferKeyspace) (interface{}, error) { return NA(o) }

// Function statements

func (c *Conv) VisitCreateFunction(o *plan.CreateFunction) (interface{}, error) { return NA(o) }
func (c *Conv) VisitDropFunction(o *plan.DropFunction) (interface{}, error)     { return NA(o) }
func (c *Conv) VisitExecuteFunction(o *plan.ExecuteFunction) (interface{}, error) {
	return NA(o)
}

// Index Advisor

func (c *Conv) VisitIndexAdvice(o *plan.IndexAdvice) (interface{}, error) { return NA(o) }
func (c *Conv) VisitAdvise(o *plan.Advise) (interface{}, error)           { return NA(o) }

// Update Statistics

func (c *Conv) VisitUpdateStatistics(o *plan.UpdateStatistics) (interface{}, error) {
	return NA(o)
}

// -------------------------------------------------------------------

// New methods since CB 6.5 / 2019.

func (c *Conv) VisitAll(o *plan.All) (interface{}, error) { return NA(o) } // Related to DISTINCT?

func (c *Conv) VisitStartTransaction(o *plan.StartTransaction) (interface{}, error)   { return NA(o) }
func (c *Conv) VisitCommitTransaction(o *plan.CommitTransaction) (interface{}, error) { return NA(o) }
func (c *Conv) VisitRollbackTransaction(o *plan.RollbackTransaction) (interface{}, error) {
	return NA(o)
}
func (c *Conv) VisitTransactionIsolation(o *plan.TransactionIsolation) (interface{}, error) {
	return NA(o)
}
func (c *Conv) VisitSavepoint(o *plan.Savepoint) (interface{}, error) { return NA(o) }

func (c *Conv) VisitCreateScope(o *plan.CreateScope) (interface{}, error)           { return NA(o) }
func (c *Conv) VisitDropScope(o *plan.DropScope) (interface{}, error)               { return NA(o) }
func (c *Conv) VisitCreateCollection(o *plan.CreateCollection) (interface{}, error) { return NA(o) }
func (c *Conv) VisitAlterCollection(o *plan.AlterCollection) (interface{}, error)   { return NA(o) }
func (c *Conv) VisitDropCollection(o *plan.DropCollection) (interface{}, error)     { return NA(o) }
func (c *Conv) VisitFlushCollection(o *plan.FlushCollection) (interface{}, error)   { return NA(o) }

func (c *Conv) VisitInferExpression(o *plan.InferExpression) (interface{}, error) { return NA(o) }

func (c *Conv) VisitReceive(o *plan.Receive) (interface{}, error) { return NA(o) }

// -------------------------------------------------------------------

// New methods since CB 7 / 2026 -- DDL / admin / sequence / catalog /
// credential-store / user/group ops, plus external scan. n1k1 doesn't
// interpret these, so they're "not applicable".

func (c *Conv) VisitCreateBucket(o *plan.CreateBucket) (interface{}, error) { return NA(o) }
func (c *Conv) VisitAlterBucket(o *plan.AlterBucket) (interface{}, error)   { return NA(o) }
func (c *Conv) VisitDropBucket(o *plan.DropBucket) (interface{}, error)     { return NA(o) }

func (c *Conv) VisitCreateCatalog(o *plan.CreateCatalog) (interface{}, error) { return NA(o) }
func (c *Conv) VisitAlterCatalog(o *plan.AlterCatalog) (interface{}, error)   { return NA(o) }
func (c *Conv) VisitDropCatalog(o *plan.DropCatalog) (interface{}, error)     { return NA(o) }

func (c *Conv) VisitCreateSequence(o *plan.CreateSequence) (interface{}, error) { return NA(o) }
func (c *Conv) VisitAlterSequence(o *plan.AlterSequence) (interface{}, error)   { return NA(o) }
func (c *Conv) VisitDropSequence(o *plan.DropSequence) (interface{}, error)     { return NA(o) }

func (c *Conv) VisitCreateUser(o *plan.CreateUser) (interface{}, error) { return NA(o) }
func (c *Conv) VisitAlterUser(o *plan.AlterUser) (interface{}, error)   { return NA(o) }
func (c *Conv) VisitDropUser(o *plan.DropUser) (interface{}, error)     { return NA(o) }

func (c *Conv) VisitCreateGroup(o *plan.CreateGroup) (interface{}, error) { return NA(o) }
func (c *Conv) VisitAlterGroup(o *plan.AlterGroup) (interface{}, error)   { return NA(o) }
func (c *Conv) VisitDropGroup(o *plan.DropGroup) (interface{}, error)     { return NA(o) }

func (c *Conv) VisitCreateCredentialStore(o *plan.CreateCredentialStore) (interface{}, error) {
	return NA(o)
}
func (c *Conv) VisitAlterCredentialStore(o *plan.AlterCredentialStore) (interface{}, error) {
	return NA(o)
}
func (c *Conv) VisitDropCredentialStore(o *plan.DropCredentialStore) (interface{}, error) {
	return NA(o)
}

func (c *Conv) VisitExplainFunction(o *plan.ExplainFunction) (interface{}, error) { return NA(o) }
func (c *Conv) VisitExternalScan(o *plan.ExternalScan) (interface{}, error)       { return NA(o) }

// -------------------------------------------------------------------

func NA(o interface{}) (interface{}, error) { return nil, fmt.Errorf("NA: %#v", o) }

// -------------------------------------------------------------------

func ExprFieldPath(expr expression.Expression) (rv []string, ok bool) {
	var visit func(e expression.Expression) bool // Declare for recursion.

	visit = func(e expression.Expression) bool {
		if f, ok := e.(*expression.Field); ok {
			ok = visit(f.First())
			if !ok {
				return false
			}

			rv = append(rv, f.Second().Alias())
		} else if i, ok := e.(*expression.Identifier); ok {
			rv = append(rv, i.Identifier())
		} else {
			return false
		}

		return true
	}

	ok = visit(expr)

	return rv, ok
}
