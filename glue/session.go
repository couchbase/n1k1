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

	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/errors"
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

	// OnRow, if set, is invoked with each result row's canonical JSON as it is
	// produced, instead of accumulating into Result.Rows (which stays nil).
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
		vectorizeColumnarAggs(c.TopOp, c.Temps)
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

	qp, err := s.Store.PlanStatementQP(parsed, s.Namespace, s.NamedArgs, nil)
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

	conv := &Conv{Temps: []interface{}{nil}}
	if _, err = p.Accept(conv); err != nil {
		return nil, &ErrUnsupported{Reason: err.Error()}
	}
	if conv.TopOp == nil {
		return nil, &ErrUnsupported{Reason: "nil TopOp (unconverted plan)"}
	}

	if DiscardElision {
		elideDiscarded(conv.TopOp) // drop dead projections under count(*)-style groups
	}

	vectorizeColumnarAggs(conv.TopOp, conv.Temps) // fuse ungrouped SUM over a Parquet column

	cv, err := NewConvertVals(conv.TopOp.Labels)
	if err != nil {
		return nil, &ErrUnsupported{Reason: err.Error()}
	}

	if engine.ExprCatalog["exprStr"] == nil {
		engine.ExprCatalog["exprStr"] = ExprStr
	}
	if engine.ExprCatalog["exprTree"] == nil {
		engine.ExprCatalog["exprTree"] = ExprTree
	}

	tmpDir, vars := MakeVars("", "n1k1")
	defer os.RemoveAll(tmpDir)

	gctx := NewGlueContext(time.Now())
	gctx.InitSubqueries(s.Store, s.Namespace, conv.WithBindings(), qp.Subqueries()) // enable expression subqueries
	gctx.SetNamedArgs(s.NamedArgs)                                                  // resolve $name at eval time
	gctx.SetWithScopeFrom(conv.WithScopeBindings())                                 // resolve `x IN cte` etc.

	// Route native-expression advisories (e.g. divide-by-zero) into the
	// request's warning collector; kept cbq-free on the engine side.
	vars.Ctx.Warn = func(w string) { gctx.Warning(errors.NewWarning(w)) }

	vars.Temps = vars.Temps[:0]
	vars.Temps = append(vars.Temps, gctx)
	vars.Temps = append(vars.Temps, conv.Temps[1:]...)
	for i := 0; i < 16; i++ {
		vars.Temps = append(vars.Temps, nil)
	}

	// Opt-in per-operator counters: size a flat counter array over the op tree,
	// point Ctx.Stats at it, and (if a live callback was given) route the engine's
	// stats checkpoints to it. Off by default, so the hot path pays nothing.
	var stats *base.Stats
	if s.CollectStats {
		stats = base.LayoutStats(conv.TopOp)
		vars.Ctx.Stats = stats
		if stats != nil && s.OnStats != nil {
			onStats := s.OnStats
			vars.Ctx.YieldStats = func(st *base.Stats) error {
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

	yieldVals := func(vals base.Vals) {
		v, e := cv.Convert(vals)
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
	engine.ExecOp(conv.TopOp, vars, yieldVals, yieldErr, "", "")
	elapsed := time.Since(start)

	if execErr != nil {
		return nil, execErr
	}

	return &Result{
		Labels:     conv.TopOp.Labels,
		Rows:       rows,
		Count:      rowCount,
		Warnings:   gctx.GetErrors(),
		Elapsed:    elapsed,
		Plan:       conv.TopOp,
		Stats:      stats,
		BoxedEvals: gctx.BoxedEvals(),
	}, nil
}
