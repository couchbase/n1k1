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
	"encoding/json"
	"fmt"

	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/value"

	"github.com/couchbase/n1k1/base"
)

// Implicit safety caps for a recursive CTE with no explicit OPTIONS, matching
// query's execution/with.go (_MAX_RECUR_DEPTH / _MAX_IMPLICIT_DOCS) -- they stop
// an unbounded recursion (e.g. a missing termination predicate) from looping
// forever.
const (
	maxRecurDepth = 100
	maxRecurDocs  = 10000
)

// WithRecursiveOp runs a WITH RECURSIVE CTE's fixpoint and yields the accumulated
// result rows under the alias label. The binding (an expression.With) is in a
// vars.Temps slot: Expression() is the anchor, RecursiveExpression() the step.
//
// It mirrors query's execution/with.go: evaluate the anchor, then repeatedly
// evaluate the step with the CTE alias bound to the latest working set (via
// GlueContext.corrParent, which the step's `FROM <cte>` expr-scan reads), union-
// deduping and accumulating, until the step yields nothing (or a cap is hit).
//
// Scope: the common UNION / UNION ALL recursion. The CYCLE clause
// (w.CycleFields) and explicit OPTIONS limits (w.Config) aren't honored yet --
// the depth/doc caps below bound the loop regardless.
func WithRecursiveOp(o *base.Op, vars *base.Vars, yieldVals base.YieldVals,
	yieldErr base.YieldErr) {
	idx, ok := o.Params[0].(int)
	if !ok {
		yieldErr(fmt.Errorf("with-recursive: expected int Temps index, got %T", o.Params[0]))
		return
	}
	w, ok := vars.Temps[idx].(expression.With)
	if !ok {
		yieldErr(fmt.Errorf("with-recursive: no binding at Temps[%d]", idx))
		return
	}
	gctx, ok := vars.Temps[0].(*GlueContext)
	if !ok {
		yieldErr(fmt.Errorf("with-recursive: Temps[0] is not a *GlueContext"))
		return
	}

	alias := w.Alias()

	asArray := func(v value.Value) ([]interface{}, error) {
		if v == nil {
			return nil, nil
		}
		a, ok := v.Actual().([]interface{})
		if !ok {
			return nil, fmt.Errorf("with-recursive: %q did not evaluate to an array", alias)
		}
		return a, nil
	}

	// UNION dedups (by canonical JSON) across all rows ever produced; UNION ALL
	// keeps everything.
	union := w.IsUnion()
	seen := map[string]bool{}
	dedup := func(items []interface{}) []interface{} {
		if !union {
			return items
		}
		kept := make([]interface{}, 0, len(items))
		for _, it := range items {
			b, _ := json.Marshal(it)
			k := string(b)
			if seen[k] {
				continue
			}
			seen[k] = true
			kept = append(kept, it)
		}
		return kept
	}

	// Anchor.
	av, err := w.Expression().Evaluate(nil, gctx)
	if err != nil {
		yieldErr(err)
		return
	}
	workRes, err := asArray(av)
	if err != nil {
		yieldErr(err)
		return
	}
	workRes = dedup(workRes)

	// Fixpoint: accumulate the working set, then re-run the step with the alias
	// bound to it, until it produces nothing new.
	var final []interface{}
	for level := 0; len(workRes) > 0 && level < maxRecurDepth; level++ {
		if len(final)+len(workRes) > maxRecurDocs {
			workRes = workRes[:maxRecurDocs-len(final)]
		}
		final = append(final, workRes...)
		if len(final) >= maxRecurDocs {
			break
		}

		// Bind the CTE alias to the latest working set so the step's
		// `FROM <cte>` (an expr-scan) reads it via corrParent.
		scope := value.NewScopeValue(
			map[string]interface{}{alias: value.NewValue(workRes)}, nil)
		prev := gctx.corrParent
		gctx.corrParent = scope
		sv, serr := w.RecursiveExpression().Evaluate(scope, gctx)
		gctx.corrParent = prev
		if serr != nil {
			yieldErr(serr)
			return
		}

		next, aerr := asArray(sv)
		if aerr != nil {
			yieldErr(aerr)
			return
		}
		workRes = dedup(next)
	}

	for _, it := range final {
		b, err := json.Marshal(it)
		if err != nil {
			yieldErr(err)
			return
		}
		yieldVals(base.Vals{base.Val(b)})
	}
	yieldErr(nil)
}
