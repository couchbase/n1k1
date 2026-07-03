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
	"strings"
	"sync"

	"github.com/couchbase/query/algebra"
	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/plan"
	"github.com/couchbase/query/value"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/engine"
)

// subqEvaluator executes expression subqueries (IN (SELECT ...), scalar
// subqueries, ...) by re-entering the n1k1 pipeline. It prefers the outer plan's
// in-context sub-plan (preplanned, from qp.Subqueries()); when a subquery isn't
// there it re-plans the sub-SELECT standalone with the same planner. Either way
// it convs the plan to a base.Op, runs it on n1k1's engine, and collects the rows
// into an array value -- the shape N1QL expects for a subquery.
//
// Correlated subqueries resolve outer references: the outer row is exposed as
// corrParent (see EvaluateSubquery), which ExprTree scopes each sub-row over and
// the scan ops read for a correlated USE KEYS / index span (see scanParent). The
// remaining gap is an AGGREGATE inside a correlated subquery (see the "subquery"
// group in suite_gsi_test.go).
type subqEvaluator struct {
	store     *Store
	namespace string

	// mu guards cache. Subqueries are normally evaluated on the single push
	// goroutine, but OpUnionAll runs its branches as concurrent actors that share
	// this evaluator (ChainExtend copies the Temps slice but not the *GlueContext
	// it points at), so two branch subqueries can compile() at once -- an
	// unsynchronized map write is a fatal "concurrent map writes". (corrParent is
	// untouched here: only correlated subqueries set it, and those are rarer
	// inside a concurrent union branch -- tracked separately if it bites.)
	mu    sync.Mutex
	cache map[*algebra.Select]*subCompiled

	// withBindings carries the outer query's WITH CTE bindings into sub-SELECT
	// conversions, so a sub-SELECT that references an outer CTE (WITH a AS (...),
	// b AS (SELECT .. FROM a) ... FROM b) can still resolve `FROM a`. Without it,
	// compile()'s fresh Conv wouldn't know the CTE.
	withBindings map[string]expression.With

	// preplanned holds the outer plan's in-context sub-plans (qp.Subqueries(),
	// flattened recursively by the planner). compile() prefers these over
	// re-planning standalone: they were planned WITH the outer keyspace scope, so a
	// correlated reference (an index span `META(d).id = t.to`, a USE KEYS expr)
	// formalizes correctly. Standalone re-planning loses that scope and degenerates
	// the span to null.
	//
	// Keyed by the subquery's canonical String(), NOT its *algebra.Select pointer:
	// the interpreter evaluates the very *algebra.Select object the outer plan
	// carries, but the COMPILED path serializes each subquery to a string and
	// re-parses it (glue.ExprStr) into a fresh *algebra.Select -- a different
	// pointer with the same String(). String-keying lets both paths hit.
	preplanned map[string]plan.Operator
}

// subCompiled is one subquery's sub-plan converted to a base.Op, cached so
// repeated evaluations (e.g. once per outer row) don't re-plan/re-convert.
type subCompiled struct {
	topOp *base.Op
	temps []interface{}
	cv    *ConvertVals

	// withScope holds the sub-conv's constant WITH bindings referenced as
	// variables (Conv.WithScopeBindings()) -- e.g. a nested derived-table's
	// `WITH p1 AS ('ABC')`. The main query builds its withScope in Session.Run;
	// a subquery has no such preamble, so EvaluateSubquery builds one from this
	// (see buildWithScope) so `p1` resolves inside the sub-op.
	withScope map[string]expression.With
}

// InitSubqueries wires this context to evaluate expression subqueries by
// planning them against the given store/namespace. withBindings is the outer
// query's WITH CTE bindings (Conv.WithBindings(), or nil) so sub-SELECTs can
// reference outer CTEs. Until this is called, EvaluateSubquery errors.
func (c *GlueContext) InitSubqueries(store *Store, namespace string,
	withBindings map[string]expression.With,
	subqueries map[*algebra.Select]plan.Operator) {
	// Re-key the planner's pointer-keyed sub-plan map by canonical String() so both
	// the interpreter (same *algebra.Select pointer) and the compiled path (a
	// re-parsed subquery, same string / different pointer) resolve it. See the
	// preplanned field.
	var preplanned map[string]plan.Operator
	if len(subqueries) > 0 {
		preplanned = make(map[string]plan.Operator, len(subqueries))
		for sel, op := range subqueries {
			preplanned[subqKey(sel)] = op
		}
	}
	c.subq = &subqEvaluator{
		store:        store,
		namespace:    namespace,
		cache:        map[*algebra.Select]*subCompiled{},
		withBindings: withBindings,
		preplanned:   preplanned,
	}
}

// --- algebra.Context (the methods beyond the embedded expression.Context) ---

func (c *GlueContext) Datastore() datastore.Datastore {
	if c.subq != nil && c.subq.store != nil {
		return c.subq.store.Datastore
	}
	return nil
}

func (c *GlueContext) NamedArg(name string) (value.Value, bool) {
	v, ok := c.namedArgs[name]
	return v, ok
}
func (c *GlueContext) PositionalArg(position int) (value.Value, bool) { return nil, false }

// EvaluateSubquery runs a subquery SELECT and returns its rows as an array
// value (N1QL represents a subquery expression's result as an array). This
// satisfies algebra.Context so query's algebra.Subquery.Evaluate can call it.
//
// parent is the outer row; for a correlated subquery it is exposed to the
// sub-op's expressions (see corrParent below) so outer references resolve.
//
// TODO(subquery-perf): a correlated subquery is evaluated once per outer row,
// and each call does MakeVars (a fresh temp dir) + a full ExecOp of the sub-op.
// The conv is cached (see compile), but the per-row MakeVars/spill-dir setup and
// re-execution are not amortized -- fine for correctness, worth revisiting for
// large correlated workloads (e.g. reuse vars, or a join-style rewrite).
func (c *GlueContext) EvaluateSubquery(query *algebra.Select, parent value.Value) (value.Value, error) {
	if c.subq == nil {
		return nil, fmt.Errorf("subquery evaluation not configured")
	}

	sc, err := c.subq.compile(query)
	if err != nil {
		return nil, err
	}

	// Correlated subquery: expose the outer row (parent) to the sub-op's
	// expressions for the duration of this execution, so they can resolve outer
	// identifiers (ExprTree wraps each sub-row as a scope over corrParent). Saved
	// and restored so nested subqueries chain their parents correctly. n1k1's
	// engine is single-goroutine (synchronous push), so this is safe.
	// Set up the subquery's scope: its WITH (CTE) variables plus, for a correlated
	// subquery, the outer row. Saved and restored so nested subqueries chain
	// correctly. n1k1's engine is single-goroutine (synchronous push), so safe.
	prevCorr, prevWith := c.corrParent, c.withScope
	defer func() { c.corrParent, c.withScope = prevCorr, prevWith }()
	{
		// Collect the subquery's WITH-variable values:
		//  - CONSTANT CTEs the sub-conv recorded (e.g. a nested derived-table's
		//    `WITH p1 AS ('ABC')`) -- buildWithScope evaluates these; the main query
		//    gets its equivalent in Session.Run, but a subquery has no such preamble.
		//  - CORRELATED CTEs (`WITH w1 AS (a)` where a is an outer field) -- can't be
		//    pre-evaluated at plan time; evaluate each against the outer row here.
		withVars := map[string]interface{}{}
		if cw := buildWithScope(sc.withScope, c); cw != nil {
			for k, v := range cw.Fields() {
				withVars[k] = v
			}
		}
		if query.IsCorrelated() {
			if wc := query.With(); wc != nil && !wc.IsRecursive() {
				for _, w := range wc.Bindings() {
					if w == nil || w.IsRecursive() {
						continue
					}
					if v, err := w.Expression().Evaluate(parent, c); err == nil && v != nil {
						withVars[w.Alias()] = v
					}
				}
			}
		}

		// Base of the scope chain: the outer row ONLY for a correlated subquery
		// (else nil -- a non-correlated subquery must not see outer fields). The CTE
		// vars layer on top, so both they and (when correlated) outer identifiers
		// resolve as the sub-op's rows scope over this.
		var base value.Value
		if query.IsCorrelated() {
			base = parent
		}
		if len(withVars) > 0 {
			c.corrParent = value.NewScopeValue(withVars, base)
		} else if query.IsCorrelated() {
			c.corrParent = base
		}
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

// subqKey is the preplanned-map key for a subquery SELECT: its canonical
// String() with any `cover (...)` planner annotations stripped. The pre-planned
// sub-plan's SELECT carries index-covering covers, but the runtime subquery
// expression (which the interpreter evaluates directly, and which the compiled
// path re-parses from a string) is the plain logical form -- so matching on the
// raw String() would miss. Stripping covers from the serialized form on both
// sides (build + lookup) normalizes them to the same logical key.
func subqKey(sel *algebra.Select) string {
	return stripCoverText(sel.String())
}

// stripCoverText removes N1QL `cover (<expr>)` planner annotations from a
// serialized expression/statement, keeping <expr>. Scans left-to-right, matching
// the balanced parens that open right after each "cover " token. Leaves the
// string unchanged past any unbalanced remainder (defensive; shouldn't happen).
func stripCoverText(s string) string {
	const tok = "cover ("
	for {
		i := strings.Index(s, tok)
		if i < 0 {
			return s
		}
		open := i + len(tok) - 1 // index of the '(' in "cover ("
		depth, j := 0, open
		for ; j < len(s); j++ {
			if s[j] == '(' {
				depth++
			} else if s[j] == ')' {
				depth--
				if depth == 0 {
					break
				}
			}
		}
		if j >= len(s) {
			return s // unbalanced -- give up rather than corrupt
		}
		s = s[:i] + s[open+1:j] + s[j+1:] // drop "cover (" and its matching ")"
	}
}

// compile plans + converts (once, cached) a subquery SELECT to a base.Op tree.
func (e *subqEvaluator) compile(query *algebra.Select) (*subCompiled, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if sc, ok := e.cache[query]; ok {
		return sc, nil
	}

	// Prefer the outer plan's in-context sub-plan (qp.Subqueries(), threaded in via
	// InitSubqueries). It was planned WITH the outer keyspace scope, so a correlated
	// reference formalizes correctly -- notably an index span like
	// `META(d).id = t.to`, which standalone re-planning degenerates to a `null`
	// span bound (t is out of scope), silently returning no rows. Fall back to
	// standalone re-planning when the subquery isn't in the map (e.g. a subquery
	// reached only via a nested evaluation the outer planner didn't pre-plan): a
	// plain correlated ref like `c.name` still resolves at runtime against the
	// corrParent scope.
	var planOp plan.Operator
	if op, ok := e.preplanned[subqKey(query)]; ok && op != nil {
		planOp = op
	} else {
		qp, err := e.store.PlanStatementQP(query, e.namespace, nil, nil)
		if err != nil {
			return nil, err
		}
		planOp = qp.PlanOp()
	}

	// Seed the sub-conv with a copy of the outer WITH bindings so a sub-SELECT
	// that references an outer CTE (e.g. `FROM a`) resolves it. Exclude RECURSIVE
	// bindings: inside a recursive CTE's step, `FROM r` must read the latest
	// working set (via corrParent, as a plain expr-scan) -- not re-enter the
	// fixpoint -- so r must fall through rather than route to with-recursive.
	// (A copy, so the sub-SELECT's own WITH bindings don't mutate the outer map.)
	var wb map[string]expression.With
	for k, v := range e.withBindings {
		if v.IsRecursive() {
			continue
		}
		if wb == nil {
			wb = map[string]expression.With{}
		}
		wb[k] = v
	}
	conv := &Conv{Temps: []interface{}{nil}, withBindings: wb}
	if _, err := planOp.Accept(conv); err != nil {
		return nil, err
	}
	if conv.TopOp == nil {
		return nil, fmt.Errorf("subquery: unconvertible sub-plan")
	}

	cv, err := NewConvertVals(conv.TopOp.Labels)
	if err != nil {
		return nil, err
	}

	// The subquery's OWN constant WITH bindings referenced as variables (the outer
	// ones seeded into wb are already covered by the outer query's withScope on the
	// shared context, so exclude them). EvaluateSubquery builds a withScope from
	// these so e.g. a nested derived-table's `WITH p1 AS ('ABC')` resolves.
	var subWith map[string]expression.With
	for alias, w := range conv.WithScopeBindings() {
		if _, seeded := wb[alias]; seeded {
			continue
		}
		if subWith == nil {
			subWith = map[string]expression.With{}
		}
		subWith[alias] = w
	}

	sc := &subCompiled{topOp: conv.TopOp, temps: conv.Temps, cv: cv, withScope: subWith}
	e.cache[query] = sc
	return sc, nil
}
