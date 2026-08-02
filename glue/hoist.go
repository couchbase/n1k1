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

// Loop-invariant expression hoisting above fan-out ops. A fan-out op (UNNEST)
// multiplies its input rows -- one output row per array element -- while
// copying the pre-existing labels unchanged. A group op directly above it
// evaluates ALL its key / aggregate-operand expressions per OUTPUT row, so an
// expression that references only pre-fan-out labels (e.g. MIN(t.ts) under
// `... UNNEST t.paths AS pth GROUP BY t.rt, pth.pp`) is re-evaluated once per
// element -- fan-out x too many times -- for a value that is constant across a
// record's elements.
//
// maybeHoistInvariants is a post-conv rewrite pass (the columnar pass is the
// precedent): it matches group -> [filter...] -> unnest, classifies each group
// key / aggregate operand by the identifiers it references, and for each
// invariant expression (a) inserts one extend-project below the unnest that
// evaluates it once per input row under a synthetic `$hoist<N>` label
// (passthrough labelPath params carry the existing labels), and (b) rewrites
// the group's param to a plain ["labelPath", <synth>] lookup. No new op kinds:
// project / labelPath / exprTree are all existing dual-mode machinery, so the
// compiled lane needs nothing.
//
// Conservative by construction -- an expression is hoisted only when:
//   - it is an ["exprTree", expr] param (["json", ...] constants are skipped);
//   - it is not a bare identifier (that is already a passthrough);
//   - it contains no Self, no subquery, and no window/aggregate node;
//   - every identifier it references is a single-field label of the unnest's
//     LEFT child (so never the unnest alias, and never a correlated /
//     WITH-scope name that resolves outside the row's own labels).
//
// Correctness leans on the fan-out contract: unnest-inner emits >= 0 and
// unnest-leftOuter EXACTLY >= 1 copies of the input row extended with the
// element; pre-existing bindings are never rewritten, and SQL++ scalar
// expressions are side-effect-free and total, so evaluating one earlier (even
// for an inner-unnest row that ends up emitting zero elements) cannot change
// results.

import (
	"fmt"
	"os"

	"github.com/couchbase/query/algebra"
	"github.com/couchbase/query/expression"

	"github.com/couchbase/n1k1/base"
)

// DisableHoistOptimize disables the loop-invariant hoisting pass (also via the
// N1K1_HOIST_NOOPTIMIZE env), leaving the un-hoisted -- always correct, just
// slower -- plan. Useful for A/B timing and differential tests.
var DisableHoistOptimize bool

func init() {
	if os.Getenv(base.DefEnv("N1K1_HOIST_NOOPTIMIZE",
		"set to disable loop-invariant hoisting above UNNEST")) != "" {
		DisableHoistOptimize = true
	}
}

func maybeHoistInvariants(op *base.Op) {
	if op == nil || DisableHoistOptimize {
		return
	}
	hoistGroupOverUnnest(op)
	for _, c := range op.Children {
		maybeHoistInvariants(c)
	}
}

// hoistGroupOverUnnest rewrites one group -> [filter...] -> unnest site in
// place, per the file comment. Anything that doesn't match exactly is left
// untouched.
func hoistGroupOverUnnest(group *base.Op) {
	if group.Kind != "group" || len(group.Params) != 3 || len(group.Children) != 1 {
		return
	}

	// Walk through post-UNNEST predicate filters (conv applies o.Filter() as a
	// filter op above the unnest). They pass rows through label-unchanged, so
	// hoisted vals ride through -- but their Labels must be re-pointed after
	// the unnest's labels grow.
	var filters []*base.Op
	cur := group.Children[0]
	for cur != nil && cur.Kind == "filter" && len(cur.Children) == 1 {
		filters = append(filters, cur)
		cur = cur.Children[0]
	}
	if cur == nil || (cur.Kind != "unnest-inner" && cur.Kind != "unnest-leftOuter") ||
		len(cur.Children) != 2 || cur.Children[0] == nil || cur.Children[1] == nil ||
		len(cur.Children[1].Labels) != 1 {
		return
	}
	unnest, left := cur, cur.Children[0]
	aliasLabel := unnest.Children[1].Labels[0]
	alias, ok := singleFieldLabelName(aliasLabel)
	if !ok {
		return
	}

	// The identifier universe an invariant expression may reference: the left
	// child's single-field labels. Any other left label shape (whole-row ".",
	// multi-element path, ".*") makes identifier->label reasoning ambiguous;
	// bail on the whole site. "^..." attachments are metadata, not identifier
	// targets -- skipped, and carried by the passthrough params below.
	allowed := map[string]bool{}
	for _, l := range left.Labels {
		if len(l) > 0 && l[0] == '^' {
			continue
		}
		name, ok := singleFieldLabelName(l)
		if !ok {
			return
		}
		allowed[name] = true
	}
	if len(allowed) == 0 {
		return
	}

	groups, ok1 := group.Params[0].([]interface{})
	aggExprs, ok2 := group.Params[1].([]interface{})
	if !ok1 || !ok2 {
		return
	}

	// Collect the hoistable params (indices into groups / aggExprs), deduped
	// by expression string so a key and an operand sharing an expression share
	// one hoisted column.
	type site struct {
		list []interface{}
		idx  int
	}
	sites := map[string][]site{} // expr.String() -> where it's referenced
	exprs := map[string]expression.Expression{}
	var order []string // deterministic hoist-column order
	for _, list := range [][]interface{}{groups, aggExprs} {
		for i, p := range list {
			e := hoistableExpr(p, allowed, alias)
			if e == nil {
				continue
			}
			s := e.String()
			if _, seen := sites[s]; !seen {
				order = append(order, s)
				exprs[s] = e
			}
			sites[s] = append(sites[s], site{list, i})
		}
	}
	if len(order) == 0 {
		return
	}

	// Build the extend-project below the unnest: passthrough every left label,
	// then one exprTree column per hoisted expression under a synthetic label.
	labels := append(base.Labels{}, left.Labels...)
	params := make([]interface{}, 0, len(left.Labels)+len(order))
	for _, l := range left.Labels {
		params = append(params, []interface{}{"labelPath", string(l)})
	}
	next := 0
	for _, s := range order {
		var synth string
		for { // pick a label not already present (nested hoist sites below).
			synth = "." + LabelSuffix(fmt.Sprintf("$hoist%d", next))
			next++
			if labels.IndexOf(synth) < 0 {
				break
			}
		}
		labels = append(labels, synth)
		params = append(params, []interface{}{"exprTree", exprs[s]})
		for _, st := range sites[s] {
			st.list[st.idx] = []interface{}{"labelPath", synth}
		}
	}

	unnest.Children[0] = &base.Op{
		Kind:     "project",
		Labels:   labels,
		Params:   params,
		Children: []*base.Op{left},
	}
	unnest.Labels = append(append(base.Labels{}, labels...), aliasLabel)
	for _, f := range filters {
		f.Labels = unnest.Labels
	}
}

// hoistableExpr returns the expression of an ["exprTree", expr] param that
// qualifies for hoisting (see the file comment), or nil.
func hoistableExpr(p interface{}, allowed map[string]bool, alias string) expression.Expression {
	parts, ok := p.([]interface{})
	if !ok || len(parts) != 2 {
		return nil
	}
	if kind, ok := parts[0].(string); !ok || kind != "exprTree" {
		return nil
	}
	expr, ok := parts[1].(expression.Expression)
	if !ok || expr == nil {
		return nil
	}
	if _, bare := expr.(*expression.Identifier); bare {
		return nil // already a whole-val passthrough; nothing to save.
	}

	idents := 0
	unsafe := false
	r := &identRefFinder{}
	r.SetMapper(r)
	r.SetMapFunc(func(e expression.Expression) (expression.Expression, error) {
		switch x := e.(type) {
		case *expression.Identifier:
			idents++
			if x.Identifier() == alias || !allowed[x.Identifier()] {
				unsafe = true
			}
		case *expression.Self:
			unsafe = true // whole-row reference includes the element.
		}
		if _, isSubq := e.(expression.Subquery); isSubq {
			unsafe = true // evaluation context we can't see through; bail.
		}
		if _, isAgg := e.(algebra.Aggregate); isAgg {
			unsafe = true // window/aggregate node: not a per-row scalar.
		}
		if unsafe {
			return e, nil // short-circuit: no need to descend further.
		}
		return e, e.MapChildren(r)
	})
	_, _ = r.Map(expr)

	if unsafe || idents == 0 {
		return nil
	}
	return expr
}
