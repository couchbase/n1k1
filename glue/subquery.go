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
	"fmt"
	"os"

	"github.com/couchbase/query/algebra"
	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/value"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/engine"
)

// subqEvaluator executes expression subqueries (IN (SELECT ...), scalar
// subqueries, ...) by re-entering the n1k1 pipeline. A subquery SELECT is itself
// an algebra.Statement, so we plan it with the same planner (on demand, mirroring
// query's own execution/context.go which plans subqueries lazily rather than from
// QueryPlan.Subqueries()), conv it to a base.Op, run it on n1k1's engine, and
// collect the rows into an array value -- the shape N1QL expects for a subquery.
//
// Scope: uncorrelated subqueries only. The parent (outer) row is not yet
// threaded into the sub-op's scope, so a correlated subquery (one that
// references outer fields) won't resolve those fields. See EvaluateSubquery.
type subqEvaluator struct {
	store     *Store
	namespace string
	cache     map[*algebra.Select]*subCompiled
}

// subCompiled is one subquery's sub-plan converted to a base.Op, cached so
// repeated evaluations (e.g. once per outer row) don't re-plan/re-convert.
type subCompiled struct {
	topOp *base.Op
	temps []interface{}
	cv    *ConvertVals
}

// InitSubqueries wires this context to evaluate expression subqueries by
// planning them against the given store/namespace. Until it's called,
// EvaluateSubquery errors.
func (c *GlueContext) InitSubqueries(store *Store, namespace string) {
	c.subq = &subqEvaluator{
		store:     store,
		namespace: namespace,
		cache:     map[*algebra.Select]*subCompiled{},
	}
}

// --- algebra.Context (the methods beyond the embedded expression.Context) ---

func (c *GlueContext) Datastore() datastore.Datastore {
	if c.subq != nil && c.subq.store != nil {
		return c.subq.store.Datastore
	}
	return nil
}

func (c *GlueContext) NamedArg(name string) (value.Value, bool)       { return nil, false }
func (c *GlueContext) PositionalArg(position int) (value.Value, bool) { return nil, false }

// EvaluateSubquery runs a subquery SELECT and returns its rows as an array
// value (N1QL represents a subquery expression's result as an array). This
// satisfies algebra.Context so query's algebra.Subquery.Evaluate can call it.
//
// parent is the outer row for correlated subqueries; it is not yet threaded
// into the sub-op scope, so only uncorrelated subqueries produce correct
// results (a correlated one sees its outer references as MISSING).
func (c *GlueContext) EvaluateSubquery(query *algebra.Select, parent value.Value) (value.Value, error) {
	if c.subq == nil {
		return nil, fmt.Errorf("subquery evaluation not configured")
	}

	// Correlated subqueries need the outer row threaded into the sub-op scope,
	// which isn't wired yet -- error explicitly rather than silently returning
	// wrong (empty) results.
	if query.IsCorrelated() {
		return nil, fmt.Errorf("correlated subqueries are not yet supported")
	}

	sc, err := c.subq.compile(query)
	if err != nil {
		return nil, err
	}

	// Expr constructors are global on the engine; ensure they're wired (the
	// outer Session.Run sets these + ExecOpEx before it runs, which is when a
	// subquery-bearing expression gets evaluated).
	if engine.ExprCatalog["exprStr"] == nil {
		engine.ExprCatalog["exprStr"] = ExprStr
	}
	if engine.ExprCatalog["exprTree"] == nil {
		engine.ExprCatalog["exprTree"] = ExprTree
	}

	tmpDir, vars := MakeVars("", "n1k1subq")
	defer os.RemoveAll(tmpDir)

	vars.Temps = vars.Temps[:0]
	vars.Temps = append(vars.Temps, c) // this GlueContext is the expr context
	vars.Temps = append(vars.Temps, sc.temps[1:]...)
	for i := 0; i < 16; i++ {
		vars.Temps = append(vars.Temps, nil)
	}

	var out []interface{}
	var execErr error

	yieldVals := func(vals base.Vals) {
		v, e := sc.cv.Convert(vals)
		if e != nil {
			if execErr == nil {
				execErr = e
			}
			return
		}
		if v != nil {
			out = append(out, v.Actual())
		} else {
			out = append(out, nil)
		}
	}
	yieldErr := func(e error) {
		if e != nil && execErr == nil {
			execErr = e
		}
	}

	engine.ExecOp(sc.topOp, vars, yieldVals, yieldErr, "", "")
	if execErr != nil {
		return nil, execErr
	}

	return value.NewValue(out), nil
}

// compile plans + converts (once, cached) a subquery SELECT to a base.Op tree.
func (e *subqEvaluator) compile(query *algebra.Select) (*subCompiled, error) {
	if sc, ok := e.cache[query]; ok {
		return sc, nil
	}

	// A subquery SELECT is an algebra.Statement -- plan it like any other.
	qp, err := e.store.PlanStatementQP(query, e.namespace, nil, nil)
	if err != nil {
		return nil, err
	}

	conv := &Conv{Temps: []interface{}{nil}}
	if _, err := qp.PlanOp().Accept(conv); err != nil {
		return nil, err
	}
	if conv.TopOp == nil {
		return nil, fmt.Errorf("subquery: unconvertible sub-plan")
	}

	cv, err := NewConvertVals(conv.TopOp.Labels)
	if err != nil {
		return nil, err
	}

	sc := &subCompiled{topOp: conv.TopOp, temps: conv.Temps, cv: cv}
	e.cache[query] = sc
	return sc, nil
}
