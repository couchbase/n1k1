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
	"sync"
	"time"

	"github.com/couchbase/query/encryption"
	"github.com/couchbase/query/errors"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/expression/parser"
	"github.com/couchbase/query/tenant"
	"github.com/couchbase/query/value"
)

// Parse parses an expression string, overriding the embedded IndexContext.Parse
// (a no-op that returns nil). expression.Context requires it; expression
// machinery uses it e.g. for a computed SELECT ... EXCLUDE to_string(...) that
// yields field names to parse. Returns the parsed *expression.Expression.
func (g *GlueContext) Parse(s string) (interface{}, error) {
	return parser.Parse(s)
}

// GlueContext is n1k1's own evaluation + datastore context. It replaces
// query/execution.Context, which n1k1 previously reused but which transitively
// pulls cgo dependencies (cbft/jemalloc, sigar). Avoiding it lets n1k1 build
// with CGO_ENABLED=0 into a single, cross-compilable, self-contained binary.
//
// GlueContext satisfies the two query interfaces n1k1's glue actually needs:
//   - expression.Context: for expression.Expression.Evaluate(). Provided by the
//     embedded query/expression.IndexContext (no-op/default impls), with Now()
//     overridden. Inheriting the interface keeps n1k1 compiling as query evolves.
//   - datastore.Context: for datastore.NewIndexConnection() + index scans against
//     the file datastore. Implemented below as no-ops, except Error/Warning/Fatal
//     which accumulate into errs.
type GlueContext struct {
	*expression.IndexContext

	now  time.Time
	errs []errors.Error

	// Results collects rows when GlueContext is used as a result sink
	// (e.g. by ServiceRequestEx). The test path instead drives n1k1.ExecOp
	// directly with its own yield callbacks and ignores this.
	Results []value.AnnotatedValue

	// subq drives expression-subquery evaluation (see subquery.go). nil until
	// InitSubqueries is called; when nil, EvaluateSubquery errors.
	subq *subqEvaluator

	// corrParent is the outer row during a correlated subquery's execution, so
	// the sub-op's expressions can resolve outer identifiers by falling through
	// to it (ExprTree wraps each sub-row as a scope over this parent). nil
	// outside a correlated subquery. See subquery.go / expr.go.
	corrParent value.Value

	// namedArgs holds the request's named query parameters ($name), resolved by
	// NamedArg at eval time (a `WHERE x IN $inlist` NamedParameter looks here).
	// nil when the statement uses none. See Session.NamedArgs.
	namedArgs map[string]value.Value

	// withScope holds the query's WITH (CTE) aliases as fields, so an expression
	// that references a CTE by name (e.g. `x IN cte`, `FIRST v FOR v IN cte`)
	// resolves it: Identifier.Evaluate reads item.Field(name), and ExprTree scopes
	// each non-star row over this. The star projection resets the scope (see the
	// resetScope marker in expr.go / VisitInitialProject) so WITH aliases don't leak
	// into SELECT *. nil when the query has no eagerly-bindable WITH bindings.
	// (`FROM <cte>` is handled separately by VisitExpressionScan.) See buildWithScope.
	withScope value.Value

	// fetchCache memoizes doc bytes read by the native fetch path within this one
	// request -- a nested-loop join re-fetches the same keys O(NxM) times, so this
	// turns all-but-the-first read of each doc into a map hit. Two-level, keyed by
	// keyspace dir then doc key, so a hit needs no per-fetch path-string building
	// (both keys already exist). Values are owned, immutable copies (safe to yield
	// repeatedly, and stable for the whole request -- more than the YieldVals borrow
	// contract requires). Bounded by DatastoreFetchCacheMaxBytes and guarded by
	// fetchCacheMu (UNION-ALL actors share the GlueContext). See datastore_fetch.go.
	fetchCache   map[string]map[string][]byte // dir -> key -> owned doc bytes
	fetchCacheN  int                          // total bytes currently cached (against the cap)
	fetchCacheMu sync.Mutex
}

// SetNamedArgs installs the request's named query parameters ($name), so
// NamedArg can resolve them during expression evaluation.
func (c *GlueContext) SetNamedArgs(args map[string]value.Value) { c.namedArgs = args }

// SetWithScope installs the WITH-alias scope (see the withScope field).
func (c *GlueContext) SetWithScope(v value.Value) { c.withScope = v }

// scanParent is the value against which a scan op should evaluate a correlated
// KEYS / index-span expression: the outer row during a correlated subquery
// (corrParent), else the WITH-alias scope. nil at top level. A correlated
// `USE KEYS <expr>` or an index span like `META(d).id = t.to` needs the outer
// row to resolve, so DatastoreScanKeys/EvalSpan pass this rather than nil.
func (c *GlueContext) scanParent() value.Value {
	if c.corrParent != nil {
		return c.corrParent
	}
	return c.withScope
}

// SetWithScopeFrom builds the WITH-alias scope from the given bindings (see
// buildWithScope) and installs it. Both the interpreter (Session.Run) and the
// compiled path (setupCompiled) call this with conv.WithScopeBindings(), so a
// `x IN cte` resolves identically in both.
func (c *GlueContext) SetWithScopeFrom(bindings map[string]expression.With) {
	c.withScope = buildWithScope(bindings, c)
}

// buildWithScope evaluates a query's non-recursive CONSTANT WITH bindings into
// one AnnotatedValue keyed by alias, for scoping expression evaluation (see the
// GlueContext.withScope field). Only constant bindings (arrays, objects) are
// bound: a subquery binding (cte AS (SELECT ...)) is skipped -- eagerly running
// it here risks a nil-item error or interfering with the main query's own
// subquery/GROUP-AS execution, and it still works as `FROM <cte>` via
// VisitExpressionScan. Recursive CTEs are skipped (the with-recursive op owns
// them). Returns nil when nothing bound. NOTE: map iteration order means
// dependent bindings (b AS (..a..)) bind reliably only for the common
// single-binding case; ordered binding is a TODO.
func buildWithScope(bindings map[string]expression.With, ctx *GlueContext) value.Value {
	if len(bindings) == 0 {
		return nil
	}
	wv := value.NewAnnotatedValue(map[string]interface{}{})
	bound := false
	for alias, w := range bindings {
		if w == nil || w.IsRecursive() {
			continue
		}
		if _, isSubq := w.Expression().(expression.Subquery); isSubq {
			continue
		}
		v, err := w.Expression().Evaluate(wv, ctx)
		if err != nil || v == nil {
			continue
		}
		wv.SetField(alias, v)
		bound = true
	}
	if !bound {
		return nil
	}
	return wv
}

// NewGlueContext returns a GlueContext stamped with the given "now".
func NewGlueContext(now time.Time) *GlueContext {
	return &GlueContext{IndexContext: &expression.IndexContext{}, now: now}
}

func (c *GlueContext) Now() time.Time { return c.now }

// Result collects a result row (stand-in for execution.Context.Result).
func (c *GlueContext) Result(item value.AnnotatedValue) bool {
	c.Results = append(c.Results, item)
	return true
}

// CloseResults is a no-op stand-in for execution.Context.CloseResults.
func (c *GlueContext) CloseResults() {}

// glueRequestId is the fixed request id used for n1k1's file-datastore index
// scans (n1k1 has no per-request id concept like the query server does).
const glueRequestId = "n1k1"

// --- datastore.Context ---

func (c *GlueContext) GetActiveEncryptionKey(dt encryption.KeyDataType) (*encryption.EaRKey, errors.Error) {
	return nil, nil
}

func (c *GlueContext) GetScanCap() int64   { return 0 }
func (c *GlueContext) MaxParallelism() int { return 1 }

func (c *GlueContext) Fatal(e errors.Error)   { c.errs = append(c.errs, e) }
func (c *GlueContext) Error(e errors.Error)   { c.errs = append(c.errs, e) }
func (c *GlueContext) Warning(e errors.Error) { c.errs = append(c.errs, e) }

func (c *GlueContext) GetErrors() []errors.Error { return c.errs }

func (c *GlueContext) GetReqDeadline() time.Time { return time.Time{} }

func (c *GlueContext) TenantCtx() tenant.Context { return nil }

func (c *GlueContext) SetFirstCreds(string)       {}
func (c *GlueContext) FirstCreds() (string, bool) { return "", false }

func (c *GlueContext) RecordFtsRU(ru tenant.Unit) {}
func (c *GlueContext) RecordGsiRU(ru tenant.Unit) {}
func (c *GlueContext) RecordKvRU(ru tenant.Unit)  {}
func (c *GlueContext) RecordKvWU(wu tenant.Unit)  {}

func (c *GlueContext) ScanReportWait() time.Duration { return 0 }
func (c *GlueContext) SkipKey(key string) bool       { return false }
