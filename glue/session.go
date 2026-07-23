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
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/query/algebra"
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

	// PrepareLevel is the MAX level EXECUTE will compile a prepared statement to (a
	// ceiling; default PrepareInterpreted). At PrepareCompiledFull, a standalone-
	// compilable (fully-native) prepared statement is compiled to a cbq-free program
	// and run as a child process (see executeCompiled), with the interpreter as the
	// fallback for anything that can't compile or when no toolchain/source is present.
	PrepareLevel PrepareLevel

	// Pipe, when set, serves this session's datastore leaf ops (scans/fetches) --
	// e.g. an engine.MemPipe reading inline in-memory data instead of the file
	// datastore. The plan is still built against Store (schema resolution); only the
	// data source is redirected at run time. nil = read files. See base.DatastorePipe.
	Pipe base.DatastorePipe

	// MergeStats, when set, is the shared race-safe sorted-merge counter set that every
	// Run of this session bumps (propagated to each Run's Ctx by PlanExec, like Pipe), so
	// a corpus run can aggregate merge/spill/skip stats across its detectors. nil = off.
	MergeStats *base.MergeStats

	// prepareds is this session's prepared-statement store (PREPARE ... AS <stmt> /
	// EXECUTE <name>), keyed by name. Lazily created. Interpreter-only: PREPARE
	// parses + caches the inner statement; EXECUTE re-plans and runs it with the
	// bound args. See PrepareRun / ExecuteRun and DESIGN-prepare.md.
	prepareds map[string]*preparedStmt

	// CollectStats opts a run into the per-operator counter core: Run lays out a
	// base.Stats over the op tree (see base.StatsLayout), points Ctx.Stats at it,
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

	// halt is the cooperative-cancel flag threaded into each Run's Ctx.Halt: Interrupt
	// sets it (from a signal goroutine, or when the output pipe closes); the datastore
	// scans + the op_scan checkpoint see it and unwind with base.ErrHalted. Reset to 0
	// at the start of every Run, so a stale interrupt never cancels the next query.
	// Accessed with sync/atomic (the interrupter runs on another goroutine).
	halt int32
}

// Interrupt requests the current Run halt as soon as it reaches its next cooperative
// checkpoint (a scan yield). Safe to call from any goroutine and when nothing is
// running (the next Run clears it). The Run returns base.ErrHalted. This is the engine
// side of the CLI's Ctrl-C and of stopping a query whose output pipe has closed.
func (s *Session) Interrupt() { atomic.StoreInt32(&s.halt, 1) }

// OpenSession opens a file-datastore directory and prepares it for queries.
func OpenSession(datastoreDir, namespace string) (*Session, error) {
	return OpenSessionBound(datastoreDir, namespace, nil)
}

// OpenSessionBound opens a file-datastore directory with a per-bundle late-binding
// manifest (binding.go): a detector corpus authored against a stable LOGICAL
// vocabulary (`FROM indexer_log`) runs against THIS bundle by resolving each logical
// name to the manifest's glob pattern at bind time. To run the same corpus against
// the NEXT bundle, OpenSessionBound its root with the same manifest and re-run (or
// re-CorpusCompile) -- no detector edits. A nil/empty manifest is exactly OpenSession.
func OpenSessionBound(datastoreDir, namespace string, b Binding) (*Session, error) {
	store, err := FileStoreBound(datastoreDir, b)
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
	Elapsed  time.Duration     // wall-clock of just the ExecOp run
	// RunElapsed is the wall-clock of the WHOLE Run: parse + plan + convert +
	// execute -- the honest end-to-end query time (what a caller experiences). For
	// tiny statements it's ~= Elapsed; for a large inline literal, parse dominates.
	RunElapsed time.Duration

	// CompiledChildElapsed is the standalone-compiled EXECUTE child's OWN report of
	// its compute wall -- from after it has read + parsed the parent's piped input
	// payload to when its Run() returns. It isolates the core compute (non-I/O) --
	// the specialized, Futamura-projected query code running over in-memory records --
	// from the
	// parent<->child IPC (file scan, JSON-serializing inputs to stdin, piping rows
	// back) that dominates RunElapsed for compiled EXECUTE. 0 for the interpreter and
	// for any query that didn't compile standalone. See compiledMain / runCompiledChild.
	CompiledChildElapsed time.Duration

	Plan  *base.Op    // the converted n1k1 op tree (for .explain / -v)
	Stats *base.Stats // per-operator counters when CollectStats was on, else nil
	Count int         // number of result rows (len(Rows), or the streamed count when OnRow was set)

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

// applyPostConvPasses runs the post-conversion optimization passes that shape the
// op tree the engine actually executes, so every consumer of the converted tree --
// execution AND EXPLAIN display -- sees the same optimized plan. It is the single
// home for this sequence: PlanConvert (the exec path) and convForDisplay (EXPLAIN
// display) both call it, so the two can no longer silently drift -- the drift that
// made `EXPLAIN <stmt>` show a stale, un-materialized tree while the query really
// ran the materialized one.
//
// qp may be nil. Every pass here is runnable from a Conv alone EXCEPT WireASOFJoin,
// which needs the *plan.QueryPlan (its Subqueries() hold the ASOF right sub-plan);
// when the caller can supply the qp it is threaded through so that pass runs too,
// and when qp is nil it is skipped while every Conv-only pass still runs (which is
// what fixes the observed CTE-materialize discrepancy). The passes mutate
// conv.TopOp in place -- materialize may swap the root into a sequence -- so callers
// re-read conv.TopOp afterward.
func applyPostConvPasses(conv *Conv, qp *plan.QueryPlan) {
	if conv == nil || conv.TopOp == nil {
		return
	}
	if DiscardElision {
		elideDiscarded(conv.TopOp) // drop dead projections under count(*)-style groups
	}
	maybeColumnarOptimize(conv.TopOp, conv.Temps) // fuse ungrouped SUM over a Parquet column
	// A -> B wiring (DESIGN-merging.md §3): lower order(union-all) -> merge-scan when
	// Track A's SortedSourceMeta proves the ORDER BY key is a normalized int64 sorted
	// source; no-op otherwise (and unless EnableMergeRewrite). gctx is nil here
	// (plan-time); SortedSourceMetasForKeyspace then walks fresh.
	conv.TopOp = WireTemporalMergeMeta(conv.TopOp, conv, nil)
	// A -> B wiring (DESIGN-merging.md §3, piece 2): lower a proven correlated argmax
	// subquery -> a streaming ASOF merge-join. It needs the QueryPlan (whose
	// Subqueries() hold the right sub-plan), so it is skipped when qp is nil -- the
	// only qp-gated pass. A no-op unless both keyspaces carry a proven normalized
	// int64 sort key.
	if qp != nil {
		WireASOFJoin(conv, qp)
	}
	// Materialize a multiply-referenced, non-recursive, non-correlated WITH CTE ONCE
	// into a spillable temp (opt-in via EnableCTEMaterialize); may wrap the root in a
	// sequence, so run it last. See optimize_cte.go.
	conv.materializeMultiRefCTEs()
}

// convForDisplay best-effort converts a cbq plan sub-tree into n1k1's op tree for
// EXPLAIN display, mirroring the execution path's convert + post-conversion
// optimization passes so the displayed plan is the one a real run would execute
// (CTE materialize, columnar-agg fusion, temporal merge-scan) rather than the raw
// pre-optimization tree. It never fails the caller: EXPLAIN shows the cbq plan
// regardless, so an unconvertible plan or a convert/pass panic just yields a nil (or
// partially optimized) tree, never a statement error. qp is the EXPLAIN statement's
// QueryPlan, threaded so the qp-gated WireASOFJoin runs here too (nil-safe).
func convForDisplay(inner plan.Operator, qp *plan.QueryPlan) (op *base.Op) {
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
	// Run the SAME post-conversion passes the execution path applies (PlanConvert ->
	// applyPostConvPasses), so EXPLAIN matches reality. Best effort: a pass panic
	// leaves the tree as of before it (rather than nilling the whole display plan),
	// preserving the never-fail contract -- an isolated inner recover, so the outer
	// recover above still guards the Accept convert itself.
	func() {
		defer func() { recover() }()
		applyPostConvPasses(c, qp)
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

	// RunElapsed is the whole-Run wall (parse+plan+convert+execute). Set via a defer
	// on the named return so every path (StatementRun, PrepareRun, EXPLAIN, ...) gets
	// it; PlanExec, which builds the Result, only knows the ExecOp slice (Elapsed).
	runStart := time.Now()
	defer func() {
		if res != nil {
			res.RunElapsed = time.Since(runStart)
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
	if s.Store != nil {
		ensureDatastore(s.Store.Datastore) // no-op (read-only) when already set -- see stmt.go.
	}

	// CREATE / DROP TEMP KEYSPACE (IDEA-0027) has no fork grammar, so recognize it at
	// the text level BEFORE the parser (which would reject it) -- one rung earlier
	// than the PREPARE/EXECUTE/INSERT statement-level intercepts below. Anything not
	// matching this strict recognizer falls through to the normal parse path.
	if t, ok := parseTempKeyspaceStmt(stmt); ok {
		return s.TempKeyspaceRun(t)
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
	case *algebra.Insert:
		// INSERT INTO is handled in-session (phase-1 materialize), NOT by the cbq
		// planner: the planner would require the target keyspace to already exist
		// (SendInsert resolves it), which defeats writing a brand-new keyspace file.
		// Intercepting the parsed statement before planning sidesteps that -- same
		// pattern as PREPARE/EXECUTE. See InsertRun and DESIGN-data.md.
		return s.InsertRun(st)
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
		return &Result{Rows: []json.RawMessage{b}, Plan: convForDisplay(ex.Plan(), qp)}, nil
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
	// Apply the shared post-conversion optimization passes (the SAME sequence
	// EXPLAIN display runs via convForDisplay, so the two can't drift). qp is
	// passed so the qp-gated WireASOFJoin fires here. See applyPostConvPasses.
	applyPostConvPasses(conv, qp)
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
	// exprStr/exprTree are registered once in this package's init() (not lazily here) so the
	// concurrent Run path never writes the shared engine.ExprCatalog map. See expr.go.

	tmpDir, vars := MakeVars("", "n1k1")
	defer os.RemoveAll(tmpDir)

	// A per-session DatastorePipe overrides where datastore leaves read from (e.g.
	// inline in-memory data instead of files); nil keeps the file datastore. See
	// base.DatastorePipe / DatastoreOp.
	vars.Ctx.Pipe = s.Pipe
	vars.Ctx.MergeStats = s.MergeStats // shared merge counters (nil = off).

	// Cooperative cancel: clear any stale interrupt, then thread this Run's flag into
	// Ctx so scans (and the op_scan checkpoint below) can halt on Interrupt / a closed
	// output pipe. Reset FIRST so a signal that arrived between Runs never cancels this one.
	atomic.StoreInt32(&s.halt, 0)
	vars.Ctx.Halt = &s.halt

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
		stats = base.StatsLayout(pp.topOp)
		vars.Ctx.Stats = stats
		if stats != nil {
			// Label each running-aggregate group op's partials with (alias, expr) so
			// the display path can show "alias (expr): value" -- for the live footer
			// (which sees only *base.Stats, not the plan) as well as the final block.
			stats.RunningAggLabels = RunningAggLabels(pp.topOp)
		}
		if stats != nil && s.OnStats != nil {
			onStats := s.OnStats
			// pace re-tunes the checkpoint interval so onStats fires at a display-
			// friendly rate: a scan racing past (checkpoints arriving faster than a UI
			// can absorb) backs the interval off (1024 -> 2048 -> ...), a slowing scan
			// eases it back down.
			pace := NewYieldPacer(engine.ScanYieldStatsEvery)
			// statsMu serializes the callback: UNION ALL (and other broadcast ops) run
			// their branches as CONCURRENT actors (base/stage.go), and every actor's
			// scan invokes this one shared YieldStats at its checkpoints -- so both the
			// stateful pace.Next (read-modify-write of the pacer) and onStats (the single
			// live-footer sink) would otherwise race across actor goroutines. Checkpoints
			// are infrequent (every >=1024 rows, growing), so the lock is uncontended.
			var statsMu sync.Mutex
			vars.Ctx.YieldStats = func(st *base.Stats) base.YieldStatsControl {
				if atomic.LoadInt32(&s.halt) != 0 {
					return base.YieldStatsControl{Stop: base.ErrHalted}
				}
				statsMu.Lock()
				defer statsMu.Unlock()
				// The live in-flight aggregate partials (COUNT/SUM/AVG/MIN/MAX
				// climbing) are refreshed by each actor at its own checkpoint
				// (Ctx.RunningAggsRefresh, called just before this YieldStats fires), so
				// they are already current here. onStats reads them via st.RangeRunningAggs
				// / StatsSnapshotJSON, which fences the read against a concurrently
				// refreshing actor. See DESIGN-stats.md "Live aggregates".
				onStats(st)
				return base.YieldStatsControl{NextEvery: pace.Next(time.Now())}
			}
		}
	}

	// Always-on cooperative-halt checkpoint when no live-stats callback set one above,
	// so a Ctrl-C / closed-pipe interrupt stops op_scan (which honors Stop) even for a
	// plain, stats-off query. Cheap: one atomic load per scan checkpoint (~every 1024
	// rows). The datastore-records scan checks Ctx.Halt directly (it has no YieldStats).
	if vars.Ctx.YieldStats == nil {
		vars.Ctx.YieldStats = func(*base.Stats) base.YieldStatsControl {
			if atomic.LoadInt32(&s.halt) != 0 {
				return base.YieldStatsControl{Stop: base.ErrHalted}
			}
			return base.YieldStatsControl{}
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

	// compiledBin is the built standalone binary for the compiled-full EXECUTE path
	// (cached across EXECUTEs -- PREPARE-once / run-many). compiledTried records that
	// a build was attempted so a non-compilable statement (or a missing toolchain/
	// source) doesn't retry every EXECUTE. See executeCompiled.
	compiledBin     string
	compiledTried   bool
	compiledCleanup func()
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

	// At the compiled-full ceiling, try running EXECUTE as a compiled standalone
	// program; fall back to the interpreter when the statement can't compile (a
	// boxed expr), the toolchain/source isn't available, or args are in play (the
	// compiled path doesn't bind params yet).
	if s.PrepareLevel >= PrepareCompiledFull &&
		len(namedArgs) == 0 && len(positionalArgs) == 0 {
		if res, ok, err := s.executeCompiled(ps); err != nil {
			return nil, err
		} else if ok {
			return res, nil
		}
	}

	return s.PlanExec(ps.compiled, namedArgs, positionalArgs)
}
