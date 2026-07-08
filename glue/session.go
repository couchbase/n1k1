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
	"os"
	"time"

	"github.com/couchbase/query/algebra"
	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/errors"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/plan"
	"github.com/couchbase/query/value"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/engine"
)

// Session is a reusable end-to-end driver: parse -> plan -> convert -> execute a
// SQL++ statement over a file datastore using n1k1's own operators. It is the
// single engine path shared by the conformance test harness and the cmd/n1k1
// CLI, so only one place knows the full pipeline.
type Session struct {
	Store     *Store
	Namespace string // e.g. "default"

	// NamedArgs are optional named query parameters ($name) supplied with the
	// request; they flow to the planner and to eval-time NamedParameter lookups
	// (see GlueContext.NamedArg). nil when the statement uses none.
	NamedArgs map[string]value.Value

	// PositionalArgs are optional positional query parameters ($1, $2, ...) supplied
	// with the request; they flow to the planner and to eval-time PositionalParameter
	// lookups (see GlueContext.PositionalArg). nil when the statement uses none. An
	// EXECUTE ... USING [...] clause supplies these instead (see ExecuteRun).
	PositionalArgs value.Values

	// prepareds is this session's prepared-statement store (PREPARE ... AS <stmt> /
	// EXECUTE <name>), keyed by name. Lazily created. Interpreter-only: PREPARE
	// parses + caches the inner statement; EXECUTE re-plans and runs it with the
	// bound args. See PrepareRun / ExecuteRun and DESIGN-prepare.md.
	prepareds map[string]*preparedStmt

	// CollectStats opts a run into the per-operator counter core: Run lays out a
	// base.Stats over the op tree (see base.LayoutStats), points Ctx.Stats at it,
	// and returns it as Result.Stats. Off by default (zero cost). See
	// DESIGN-stats.md.
	CollectStats bool

	// OnStats, if set (and CollectStats is on), is invoked at each engine stats
	// checkpoint with the live shared *Stats, for progress display. It runs on the
	// execution goroutine, so it must be fast and non-blocking (snapshot/render a
	// throttled view; don't do slow work). Its counters are monotonic and may show
	// per-field skew mid-run -- fine for progress.
	OnStats func(*base.Stats)

	// OnRow, if set, is invoked with each result row's JSON as it is
	// produced, instead of accumulating into Result.Rows (which stays nil).
	// (Object keys are in projection order, not sorted -- the boxing-free
	// encoder passes value bytes through; see ConvertVals.ConvertBytes.)
	// Result.Count still reports the total. This is the streaming path: it lets a
	// caller render/forward rows incrementally rather than waiting for -- and
	// holding -- the whole result set (the WASM demo's Web Worker streams batches
	// to the UI this way). It runs on the execution goroutine, so it must be fast
	// and must not retain the passed slice (copy if kept).
	OnRow func([]byte)
}

// OpenSession opens a file-datastore directory and prepares it for queries.
func OpenSession(datastoreDir, namespace string) (*Session, error) {
	store, err := FileStore(datastoreDir)
	if err != nil {
		return nil, err
	}
	if err := store.InitParser(); err != nil {
		return nil, err
	}
	if namespace == "" {
		namespace = "default"
	}
	return &Session{Store: store, Namespace: namespace}, nil
}

// Result is the outcome of one Session.Run. Each Row is a canonical-JSON value:
// a JSON object keyed by the projection's column aliases, or a bare JSON value
// for SELECT RAW. Warnings are advisories the engine recorded during evaluation
// (e.g. divide-by-zero), distinct from a hard error.
type Result struct {
	Labels   base.Labels       // the converted op tree's top labels (internal syntax)
	Rows     []json.RawMessage // one canonical-JSON value per result row
	Warnings []errors.Error    // non-fatal advisories recorded during execution
	Elapsed  time.Duration     // wall-clock of the ExecOp run
	Plan     *base.Op          // the converted n1k1 op tree (for .explain / -v)
	Stats    *base.Stats       // per-operator counters when CollectStats was on, else nil
	Count    int               // number of result rows (len(Rows), or the streamed count when OnRow was set)

	// BoxedEvals is the number of per-row expressions that fell back to the boxed
	// cbq lane during execution (the GC-heavy Convert->Evaluate->WriteJSON path);
	// 0 means every evaluated expression stayed native. See GlueContext.boxedEvals.
	BoxedEvals int64

	// Prepared is the name a PREPARE statement cached under (empty for any other
	// statement), so a caller can confirm the prepare without the result carrying
	// rows. See PrepareRun.
	Prepared string
}

// findExplain returns the *plan.Explain node in a plan tree (it sits under the
// top-level Authorize the planner adds), or nil if the statement isn't EXPLAIN.
func findExplain(op plan.Operator) *plan.Explain {
	switch o := op.(type) {
	case *plan.Explain:
		return o
	case *plan.Authorize:
		return findExplain(o.Child())
	}
	return nil
}

// convForDisplay best-effort converts a cbq plan sub-tree into n1k1's op tree for
// EXPLAIN display, mirroring Run's convert + discard-elision. It never fails the
// caller: EXPLAIN shows the cbq plan regardless, so an unconvertible plan or a
// convert panic just yields a nil tree (no n1k1 plan shown).
func convForDisplay(inner plan.Operator) (op *base.Op) {
	if inner == nil {
		return nil
	}
	defer func() {
		if recover() != nil {
			op = nil
		}
	}()
	c := &Conv{Temps: []interface{}{nil}}
	if _, err := inner.Accept(c); err != nil || c.TopOp == nil {
		return nil
	}
	if DiscardElision {
		elideDiscarded(c.TopOp)
	}
	// Apply the same Step-5 columnar rewrite the execution path does, so EXPLAIN
	// shows agg-columnar / agg-metadata exactly where a real run would fire them
	// (it peeks the keyspace footer, same as at run time, and honors
	// DisableVectorizedAgg). Best effort: a rewrite panic leaves the un-rewritten
	// tree rather than nilling the whole display plan.
	func() {
		defer func() { recover() }()
		maybeColumnarOptimize(c.TopOp, c.Temps)
	}()
	return c.TopOp
}

// ErrUnsupported means n1k1 can't (yet) run this statement -- the plan converted
// to no op tree, a convert step failed, or execution panicked -- as opposed to
// the statement being genuinely invalid (a parse/plan error, returned verbatim).
type ErrUnsupported struct{ Reason string }

func (e *ErrUnsupported) Error() string { return "unsupported: " + e.Reason }

// Run parses, plans, converts and executes a single statement, returning its
// rows (plus any warnings, timing and converted plan). A parse or plan error is
// returned verbatim (the statement is wrong); an unconvertible plan or a panic
// is returned as *ErrUnsupported (n1k1 can't run it yet).
func (s *Session) Run(stmt string) (res *Result, err error) {
	defer func() {
		if r := recover(); r != nil {
			res, err = nil, &ErrUnsupported{Reason: fmt.Sprintf("panic: %v", r)}
		}
	}()

	// A correlated / USE-KEYS subquery re-plans and re-evaluates through
	// couchbase/query's algebra, which resolves keyspaces via the process-global
	// datastore (datastore.GetDatastore) rather than the store we thread through
	// PlanStatementQP -- so without this, a subquery-bearing query errors with
	// "Datastore not set" (or worse, resolves against a stale global set by an
	// earlier Session). OpenSession points the global at this store via InitParser,
	// but Run is also invoked on directly-constructed Sessions (the test harness,
	// embedders), so ensure it here too. Cheap idempotent global assignment.
	if s.Store != nil && s.Store.Datastore != nil {
		datastore.SetDatastore(s.Store.Datastore)
	}

	parsed, err := ParseStatement(stmt, s.Namespace, true)
	if err != nil {
		return nil, err
	}

	// PREPARE / EXECUTE are handled in-session (a per-Session prepared-statement
	// store), not by the cbq planner -- which has no home for them in n1k1's CE
	// build (PlanStatementQP rejects PREPARE; EXECUTE would hit an unsupported
	// plan.Discard). See PrepareRun / ExecuteRun and DESIGN-prepare.md.
	switch st := parsed.(type) {
	case *algebra.Prepare:
		return s.PrepareRun(st)
	case *algebra.Execute:
		return s.ExecuteRun(st)
	}

	return s.StatementRun(parsed, s.NamedArgs, s.PositionalArgs)
}

// StatementRun plans, converts, and executes an already-parsed statement with the
// given query parameters. It is the shared core of Run (a top-level statement) and
// of EXECUTE (a prepared inner statement run with bound args). Its panics are
// recovered by Run's deferred handler -- StatementRun is only ever called within
// Run's dynamic extent (directly, or via ExecuteRun).
func (s *Session) StatementRun(parsed algebra.Statement,
	namedArgs map[string]value.Value, positionalArgs value.Values) (res *Result, err error) {
	qp, err := s.Store.PlanStatementQP(parsed, s.Namespace, namedArgs, positionalArgs)
	if err != nil {
		return nil, err
	}
	p := qp.PlanOp()

	// EXPLAIN: the couchbase/query planner already built the plan (it's what conv
	// would otherwise convert). Rather than convert+execute, emit that plan as a
	// single JSON row, matching N1QL's EXPLAIN output. The Explain op sits under
	// the top Authorize wrapper, so unwrap to find it.
	if ex := findExplain(p); ex != nil {
		// plan.Explain marshals to {"text": <stmt>, "plan": <op>} -- N1QL's
		// EXPLAIN result shape (unchanged, for compatibility).
		b, err := json.Marshal(ex)
		if err != nil {
			return nil, err
		}
		// Also convert the underlying plan into n1k1's own op tree, purely for
		// display (the CLI's EXPLAIN / .explain rendering shows what n1k1 would
		// actually run, and which expressions evaluate natively vs boxed). EXPLAIN
		// doesn't execute, so a convert failure/panic just leaves Plan nil.
		return &Result{Rows: []json.RawMessage{b}, Plan: convForDisplay(ex.Plan())}, nil
	}

	pp, err := PlanConvert(qp)
	if err != nil {
		return nil, err
	}
	return s.PlanExec(pp, namedArgs, positionalArgs)
}

// PreparedPlan is a converted, reusable query plan: the op tree plus the per-conv
// state execution needs, built once (with no args baked in, so $params stay
// deferred to eval) and re-run with fresh Vars per EXECUTE. Sharing topOp/temps
// across runs is safe -- the mutable per-run state lives in the fresh Vars, not
// here -- exactly as the subquery evaluator re-runs a cached sub-plan once per
// outer row (see subCompiled / EvaluateSubquery).
type PreparedPlan struct {
	topOp        *base.Op
	temps        []interface{}
	cv           *ConvertVals
	withBindings map[string]expression.With
	withScope    map[string]expression.With
	subqueries   map[*algebra.Select]plan.Operator
}

// PlanConvert converts a planned QueryPlan into a reusable PreparedPlan (op tree +
// conv state), applying the same optimizations the execution path relies on.
func PlanConvert(qp *plan.QueryPlan) (*PreparedPlan, error) {
	conv := &Conv{Temps: []interface{}{nil}}
	if _, err := qp.PlanOp().Accept(conv); err != nil {
		return nil, &ErrUnsupported{Reason: err.Error()}
	}
	if conv.TopOp == nil {
		return nil, &ErrUnsupported{Reason: "nil TopOp (unconverted plan)"}
	}
	if DiscardElision {
		elideDiscarded(conv.TopOp) // drop dead projections under count(*)-style groups
	}
	maybeColumnarOptimize(conv.TopOp, conv.Temps) // fuse ungrouped SUM over a Parquet column
	cv, err := NewConvertVals(conv.TopOp.Labels)
	if err != nil {
		return nil, &ErrUnsupported{Reason: err.Error()}
	}
	return &PreparedPlan{
		topOp:        conv.TopOp,
		temps:        conv.Temps,
		cv:           cv,
		withBindings: conv.WithBindings(),
		withScope:    conv.WithScopeBindings(),
		subqueries:   qp.Subqueries(),
	}, nil
}

// PlanExec runs a converted PreparedPlan with the given query args bound at eval
// time, collecting the result rows. Shared by StatementRun (a freshly converted
// plan) and EXECUTE (a cached one). Panics propagate to Run's deferred recover.
func (s *Session) PlanExec(pp *PreparedPlan,
	namedArgs map[string]value.Value, positionalArgs value.Values) (*Result, error) {
	if engine.ExprCatalog["exprStr"] == nil {
		engine.ExprCatalog["exprStr"] = ExprStr
	}
	if engine.ExprCatalog["exprTree"] == nil {
		engine.ExprCatalog["exprTree"] = ExprTree
	}

	tmpDir, vars := MakeVars("", "n1k1")
	defer os.RemoveAll(tmpDir)

	gctx := NewGlueContext(time.Now())
	gctx.InitSubqueries(s.Store, s.Namespace, pp.withBindings, pp.subqueries) // enable expression subqueries
	gctx.SetNamedArgs(namedArgs)                                              // resolve $name at eval time
	gctx.SetPositionalArgs(positionalArgs)                                    // resolve $1,$2 at eval time
	gctx.SetWithScopeFrom(pp.withScope)                                       // resolve `x IN cte` etc.

	// Route native-expression advisories (e.g. divide-by-zero) into the
	// request's warning collector; kept cbq-free on the engine side.
	vars.Ctx.Warn = func(w string) { gctx.Warning(errors.NewWarning(w)) }

	vars.Temps = vars.Temps[:0]
	vars.Temps = append(vars.Temps, gctx)
	vars.Temps = append(vars.Temps, pp.temps[1:]...)
	for i := 0; i < 16; i++ {
		vars.Temps = append(vars.Temps, nil)
	}

	// Opt-in per-operator counters: size a flat counter array over the op tree,
	// point Ctx.Stats at it, and (if a live callback was given) route the engine's
	// stats checkpoints to it. Off by default, so the hot path pays nothing.
	var stats *base.Stats
	if s.CollectStats {
		stats = base.LayoutStats(pp.topOp)
		vars.Ctx.Stats = stats
		if stats != nil {
			// Label each running-aggregate group op's partials with (alias, expr) so
			// the display path can show "alias (expr): value" -- for the live footer
			// (which sees only *base.Stats, not the plan) as well as the final block.
			stats.RunningAggLabels = RunningAggLabels(pp.topOp)
		}
		if stats != nil && s.OnStats != nil {
			onStats := s.OnStats
			vars.Ctx.YieldStats = func(st *base.Stats) error {
				// The live in-flight aggregate partials (COUNT/SUM/AVG/MIN/MAX
				// climbing) are refreshed by each actor at its own checkpoint
				// (Ctx.RefreshRunningAggs, called just before this YieldStats fires), so
				// they are already current here. onStats reads them via st.RangeRunningAggs
				// / StatsSnapshotJSON, which fences the read against a concurrently
				// refreshing actor. See DESIGN-stats.md "Live aggregates".
				onStats(st)
				return nil
			}
		}
	}

	origExecOpEx := engine.ExecOpEx
	defer func() { engine.ExecOpEx = origExecOpEx }()
	engine.ExecOpEx = DatastoreOp

	var rows []json.RawMessage
	var rowCount int
	var execErr error

	// rowBuf is reused across rows for the boxing-free ConvertBytes encoding;
	// the OnRow contract already forbids retaining the passed slice.
	var rowBuf []byte

	yieldVals := func(vals base.Vals) {
		// Fast path: render the row's JSON straight from the label bytes, with
		// no value.Value boxing (see ConvertVals.ConvertBytes). The label set is
		// fixed, so this either handles every row or none.
		if b, ok := pp.cv.ConvertBytes(vals, rowBuf[:0]); ok {
			rowBuf = b
			rowCount++
			if s.OnRow != nil {
				s.OnRow(rowBuf) // reused buffer; OnRow must copy to retain (see doc)
			} else {
				rows = append(rows, append(json.RawMessage(nil), rowBuf...)) // copy to keep
			}
			return
		}

		// Boxed fallback for shapes ConvertBytes can't encode natively.
		v, e := pp.cv.Convert(vals)
		if e != nil {
			if execErr == nil {
				execErr = e
			}
			return
		}
		var b []byte
		if v != nil {
			b, _ = json.Marshal(v.Actual())
		} else {
			b = []byte("null")
		}
		rowCount++
		if s.OnRow != nil {
			s.OnRow(b) // stream this row; don't accumulate (Result.Rows stays nil)
		} else {
			rows = append(rows, json.RawMessage(b))
		}
	}
	yieldErr := func(e error) {
		if e != nil && execErr == nil {
			execErr = e
		}
	}

	start := time.Now()
	engine.ExecOp(pp.topOp, vars, yieldVals, yieldErr, "", "")
	elapsed := time.Since(start)

	if execErr != nil {
		return nil, execErr
	}

	return &Result{
		Labels:     pp.topOp.Labels,
		Rows:       rows,
		Count:      rowCount,
		Warnings:   gctx.GetErrors(),
		Elapsed:    elapsed,
		Plan:       pp.topOp,
		Stats:      stats,
		BoxedEvals: gctx.BoxedEvals(),
	}, nil
}

// preparedStmt is one entry in a Session's prepared-statement store: the inner
// statement PREPARE parsed, its original text, a use counter, and (lazily, on the
// first EXECUTE) the converted op tree cached for reuse. Interpreter-only: EXECUTE
// runs the cached plan through the interpreter, binding its args at eval time. See
// DESIGN-prepare.md "Preparation is a level".
type preparedStmt struct {
	name     string
	text     string
	stmt     algebra.Statement
	uses     int64
	compiled *PreparedPlan // cached converted plan (params deferred to eval); nil until first EXECUTE
}

// PrepareRun handles `PREPARE [name] AS <stmt>` (cbq also spells it `PREPARE [name]
// FROM <stmt>`): it caches the inner statement in the session's prepared-statement
// store so a later EXECUTE can run it. An unnamed PREPARE keys on the inner
// statement's text. It does NOT execute the inner statement, and returns an empty
// result -- interpreter-only PREPARE is a cache, not a compile, so there is no
// encoded-plan row to hand back (cf. cbq).
func (s *Session) PrepareRun(p *algebra.Prepare) (*Result, error) {
	name := p.Name()
	if name == "" {
		name = p.Statement().String() // unnamed PREPARE: key on the inner statement text
	}
	if s.prepareds == nil {
		s.prepareds = map[string]*preparedStmt{}
	}
	s.prepareds[name] = &preparedStmt{name: name, text: p.Text(), stmt: p.Statement()}
	return &Result{Prepared: name}, nil
}

// PreparedInner returns the inner statement text of the prepared statement cached
// under name (what EXECUTE <name> would run), or ("", false) if there is no such
// prepared statement. The CLI uses it to analyze/emit a prepared statement's
// compile level at a -prepare ceiling. See reportPrepared.
func (s *Session) PreparedInner(name string) (string, bool) {
	ps := s.prepareds[name]
	if ps == nil {
		return "", false
	}
	return ps.stmt.String(), true
}

// ExecuteRun handles `EXECUTE <name> [USING <args>]`: it looks up the prepared
// statement, binds its query parameters, and runs it via StatementRun. Args come
// from EITHER the USING clause (a constant array -> positional, object -> named) OR
// the request's Session.NamedArgs / PositionalArgs -- never both (cbq rejects that
// combination). See case_prepare.json.
func (s *Session) ExecuteRun(e *algebra.Execute) (*Result, error) {
	ps := s.prepareds[e.Prepared()]
	if ps == nil {
		return nil, fmt.Errorf("No such prepared statement: %s", e.Prepared())
	}
	ps.uses++

	namedArgs, positionalArgs := s.NamedArgs, s.PositionalArgs

	if u := e.Using(); u != nil {
		// USING and request-level parameters are mutually exclusive.
		if len(s.NamedArgs) > 0 || len(s.PositionalArgs) > 0 {
			return nil, fmt.Errorf(
				"Execution parameter error: cannot have both USING clause and request parameters")
		}
		uv, err := u.Evaluate(nil, convEvalContext)
		if err != nil {
			return nil, err
		}
		namedArgs, positionalArgs = nil, nil
		switch uv.Type() {
		case value.ARRAY: // USING [ ... ]  -> positional $1, $2, ...
			act, _ := uv.Actual().([]interface{})
			positionalArgs = make(value.Values, len(act))
			for i, a := range act {
				positionalArgs[i] = value.NewValue(a)
			}
		case value.OBJECT: // USING { "k": ... } -> named $k
			act, _ := uv.Actual().(map[string]interface{})
			namedArgs = make(map[string]value.Value, len(act))
			for k, a := range act {
				namedArgs[k] = value.NewValue(a)
			}
		default:
			return nil, fmt.Errorf(
				"EXECUTE USING must be an array (positional) or object (named), got %s", uv.Type())
		}
	}

	// Build + cache the converted plan on the first EXECUTE, planned with NO args so
	// $params stay deferred to eval; every EXECUTE then reuses it and binds its own
	// args -- the PREPARE-once / EXECUTE-many win (skips cbq's planner.Build per run).
	if ps.compiled == nil {
		qp, err := s.Store.PlanStatementQP(ps.stmt, s.Namespace, nil, nil)
		if err != nil {
			return nil, err
		}
		pp, err := PlanConvert(qp)
		if err != nil {
			return nil, err
		}
		ps.compiled = pp
	}

	return s.PlanExec(ps.compiled, namedArgs, positionalArgs)
}
