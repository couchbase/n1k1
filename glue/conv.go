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

	// withFromUsed records CTE aliases consumed as a FROM/JOIN source by
	// VisitExpressionScan. Those are handled by expr-scan, so they must NOT go into
	// the eval-time withScope (buildWithScope) -- scoping them would spread a
	// nested/derived-table CTE into an unrelated SELECT *. Only aliases referenced
	// as expression VARIABLES (`x IN cte`) belong in withScope. See WithScopeBindings.
	withFromUsed map[string]bool

	// groupKeyExprLabels maps a *computed* (non-field-path) GROUP BY key's
	// expression string to the synthetic label under which VisitFinalGroup
	// materialized its value. VisitInitialProject uses it to read such a key
	// back by label instead of re-evaluating the expression -- which can't be
	// reconstructed from the grouped row (e.g. `orderlines[1]`, `now_str()`).
	// Plain field-path keys are absent here (they re-materialize at their path
	// and are re-evaluated as before). Populated by VisitFinalGroup.
	groupKeyExprLabels map[string]string
	groupKeyCount      int

	// groupAs / groupAsFields carry a GROUP AS <name> binding from the
	// InitialGroup (which owns it) to VisitFinalGroup (which does the grouping,
	// the Initial/Intermediate groups being skipped). See VisitFinalGroup.
	groupAs       string
	groupAsFields []string

	// sawFTS is set once an FTS scan (VisitIndexFtsSearch) is emitted, so
	// VisitFilter knows to strip the (already-satisfied) SEARCH() term from the
	// residual filter -- n1k1 can't re-evaluate SEARCH() outside the index.
	sawFTS bool

	// cteFromRefs records the expr-scan ops emitted for each `FROM <cte>`
	// reference of a non-recursive CTE, keyed by the CTE's Expression() IDENTITY
	// (not its alias). A CTE referenced >=2 times can be materialized once (see
	// materializeMultiRefCTEs). Keying by expression identity -- not alias -- is
	// essential: two independent WITH scopes can share an alias but have different
	// definitions (e.g. `(WITH cte AS ([1,2,3]) ...) EXCEPT ALL (WITH cte AS
	// ([3,4,5]) ...)`), and must NOT be conflated into one materialized temp. The
	// self-join target (`FROM x o1, x o2, ...`) is one WITH clause, so all its refs
	// share one Expression pointer and DO group. cteFromRefOrder preserves
	// first-encounter order for deterministic rewriting. Populated by
	// VisitExpressionScan via recordCTEFromRef.
	cteFromRefs     map[expression.Expression]*cteFromRef
	cteFromRefOrder []expression.Expression
}

// cteFromRef accumulates the expr-scan ops for one FROM-used non-recursive CTE
// (identified by its Expression), plus the vars.Temps slot holding that
// expression, so materializeMultiRefCTEs can rewrite a multiply-referenced CTE
// into a single temp-capture + per-reference temp-yields.
type cteFromRef struct {
	alias    string // an alias the CTE was referenced under (for the capture label)
	with     expression.With
	exprSlot int        // vars.Temps slot holding the CTE's Expression()
	ops      []*base.Op // the "expr-scan" ops, one per `FROM <cte>` reference
}

// recordCTEFromRef notes one `FROM <cte>` expr-scan for the materialize pass,
// grouped by the CTE's Expression identity.
func (c *Conv) recordCTEFromRef(alias string, w expression.With, expr expression.Expression,
	exprSlot int, op *base.Op) {
	if c.cteFromRefs == nil {
		c.cteFromRefs = map[expression.Expression]*cteFromRef{}
	}
	r := c.cteFromRefs[expr]
	if r == nil {
		r = &cteFromRef{alias: alias, with: w, exprSlot: exprSlot}
		c.cteFromRefs[expr] = r
		c.cteFromRefOrder = append(c.cteFromRefOrder, expr)
	}
	r.ops = append(r.ops, op)
}

// -------------------------------------------------------------------

// ExecConv converts a plan.Operator into a base.Op.
func ExecConv(p plan.Operator) (*base.Op, []interface{}, error) {
	// The 0'th temps slot is prealloc'ed for the execution context.
	c := &Conv{Temps: []interface{}{nil}}

	_, err := p.Accept(c)

	if DiscardElision && err == nil && c.TopOp != nil {
		elideDiscarded(c.TopOp) // drop dead projections under count(*)-style groups
	}

	if err == nil && c.TopOp != nil {
		maybeColumnarOptimize(c.TopOp, c.Temps) // fuse ungrouped SUM over a Parquet column
	}

	if err == nil && c.TopOp != nil {
		// Track B (DESIGN-merging.md §3): recognize UNION ALL of sorted streams
		// wrapped by ORDER BY <key> and lower it to a streaming merge-scan. A
		// read-only post-plan pass (opt-in via EnableMergeRewrite); may replace
		// the root itself, so it returns the (possibly new) root.
		c.TopOp = rewriteTemporalRoot(c.TopOp)

		// Track B (DESIGN-merging.md §3): recognize the correlated argmax subquery
		// (nearest-preceding ASOF) shape. Read-only + opt-in (EnableASOFRecognize);
		// the merge-join LOWERING is gated on the normalized sort-key wiring, so
		// this pass only classifies/counts for now (see optimize_temporal.go).
		recognizeASOFRoot(c.TopOp)

		// Materialize a multiply-referenced, non-recursive, non-correlated WITH CTE
		// ONCE into a spillable temp (opt-in via EnableCTEMaterialize); may wrap the
		// root in a sequence, so run it last. See optimize_cte.go.
		c.materializeMultiRefCTEs()
	}

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

// inlineProjectedSubqueryCTE rewrites a projection term that is exactly a
// reference to a subquery-bound CTE (`SELECT w2` where `w2 AS (SELECT ...)`) into
// that subquery expression, so it projects as {w2: <subquery result array>} --
// the value semantics of selecting a materialized CTE. A CONSTANT CTE the
// subquery references (e.g. `... IN w1`) resolves via withScope at eval time. Only
// the top-level `SELECT <cte>` shape is handled (not `SELECT f(w2)`); non-CTE and
// FROM-used / recursive / constant CTEs pass through unchanged.
func (c *Conv) inlineProjectedSubqueryCTE(e expression.Expression) expression.Expression {
	id, ok := e.(*expression.Identifier)
	if !ok {
		return e
	}
	w, ok := c.withBindings[id.Identifier()]
	if !ok || w.IsRecursive() || c.withFromUsed[id.Identifier()] {
		return e
	}
	if _, isSubq := w.Expression().(expression.Subquery); !isSubq {
		return e // a constant CTE is handled by withScope, not inlined
	}
	return w.Expression()
}

// WithScopeBindings returns the WITH bindings eligible for the eval-time
// withScope (a CTE referenced as a variable, e.g. `x IN cte`): all bindings
// EXCEPT those consumed as a FROM/JOIN source (expr-scan handles those, and
// scoping a nested/derived-table CTE globally would leak it into unrelated
// projections -- see withFromUsed). May be nil.
func (c *Conv) WithScopeBindings() map[string]expression.With {
	if len(c.withBindings) == 0 {
		return nil
	}
	out := make(map[string]expression.With, len(c.withBindings))
	for alias, w := range c.withBindings {
		if c.withFromUsed[alias] {
			continue
		}
		out[alias] = w
	}
	return out
}

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
// directory n1k1-native (records: union of files, recurse, decode,
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
	// A covering index scan carries the projected/filtered fields as cover
	// expressions and the planner emits NO plan.Fetch (the index alone answers the
	// query). The covers lower (via stripCovers, expr.go) to plain doc-field
	// accesses (e.g. `.age`) that need a document to read from.
	//
	// True covering: when the index is coverable (every key is a plain field ref),
	// reconstruct that document straight from the decoded index-key values -- no
	// fetch. datastore-scan-index-cover emits a `.alias` doc + `^id` in the same
	// shape a fetch would, so the peeled field accesses (and META().id) resolve
	// against it identically. See DESIGN-indexing.md "true covering execution".
	if len(o.Covers()) > 0 && coverableIndexScan(o) {
		return c.TopPush(o, &base.Op{
			Kind:   "datastore-scan-index-cover",
			Labels: base.Labels{"." + LabelSuffix(o.Term().Alias()), "^id"},
			Params: []interface{}{c.AddTemp(o)},
		})
	}

	// A covering scan over the file datastore's #primary (a non-n1k1 index): the
	// "index" is just the keyspace's document listing, so a covering scan is really
	// a full document scan. Emit a records-scan (whole doc + ^id) rather than
	// scan-index + a synthesized fetch below -- so META().id and any covered field
	// accesses resolve against a real doc, AND a container record whose id carries
	// no fetchable byte offset (a YAML / JSON-array record) doesn't hit a fetch that
	// can't resolve it (which silently drops every row). No selectivity is lost: a
	// #primary covering scan is a full scan anyway. n1k1 secondary indexes keep the
	// scan-index path (their covering is answered above, or they stay selective).
	if len(o.Covers()) > 0 && o.Term().Namespace() != "#system" {
		if _, isSI := o.Index().(index); !isSI {
			return c.recordsScan(o, o.Term().Alias())
		}
	}

	c.TopPush(o, &base.Op{
		Kind:   "datastore-scan-index",
		Labels: base.Labels{"^id"},
		Params: []interface{}{c.AddTemp(o)},
	})

	// A non-coverable covering scan (e.g. an expression key like LOWER(name), a
	// partial index, or a non-n1k1 index): the index can't reconstruct the doc, so
	// de-optimize by materializing it -- synthesize a datastore-fetch over the
	// scanned keyspace (plan.IndexScan implements Keyspacer) so the peeled field
	// accesses resolve against a real ".". Non-covering scans skip this -- a real
	// plan.Fetch follows and VisitFetch adds the fetch.
	if len(o.Covers()) > 0 {
		return c.TopPush(o, &base.Op{
			Kind:   "datastore-fetch",
			Labels: base.Labels{"." + LabelSuffix(o.Term().Alias()), "^id"},
			Params: []interface{}{c.AddTemp(o)},
		})
	}

	return c.TopOp, nil
}

// coverableIndexScan reports whether a covering IndexScan can be answered
// straight from the index (no fetch): the index must be an n1k1 secondary index
// whose keys are all plain field refs (def.coverable), and the scan must carry no
// filter-covers (a partial index's condition-field covers a key-only
// reconstruction can't satisfy). Everything else falls back to scan+fetch.
func coverableIndexScan(o *plan.IndexScan) bool {
	si, ok := o.Index().(index)
	if !ok {
		return false
	}
	if len(o.FilterCovers()) > 0 {
		return false
	}
	return si.indexDefn().coverable()
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

// VisitCountScan converts the planner's whole-keyspace COUNT(*) pushdown. n1k1
// has no O(1) count (and, for JSONL/gzip keyspaces, cbq's file-count would be
// wrong anyway), so we de-optimize: yield the records (like a primary scan) and
// let the downstream count(*) group-aggregate count them -- correct for every
// format. (A true O(1) count-from-metadata is a later, indexing-doc item.)
func (c *Conv) VisitCountScan(o *plan.CountScan) (interface{}, error) {
	return c.recordsScan(o, o.Term().Alias())
}
func (c *Conv) VisitIndexCountScan(o *plan.IndexCountScan) (interface{}, error) { return NA(o) }
func (c *Conv) VisitIndexCountScan2(o *plan.IndexCountScan2) (interface{}, error) {
	return NA(o)
}
func (c *Conv) VisitIndexCountDistinctScan2(o *plan.IndexCountDistinctScan2) (interface{}, error) {
	return NA(o)
}

// VisitDistinctScan wraps a single index scan whose predicate produced several
// spans (e.g. a same-field OR: `age < 30 OR age > 50`), deduping docIDs across
// them. n1k1's datastore-scan-index already drains every span of the inner
// IndexScan, and the planner only emits these spans as disjoint ranges (an
// overlapping OR is simplified first), so no docID repeats -- we convert the
// inner scan directly. (A general dedup op is future work if overlapping spans
// ever arise.)
func (c *Conv) VisitDistinctScan(o *plan.DistinctScan) (interface{}, error) {
	return o.Scan().Accept(c)
}

// VisitUnionScan handles an OR of several sargable secondary indexes. cbq unions
// the per-index docID sets; n1k1 has no union-scan op, and unlike an intersect we
// can't drop to one child (that would miss the other OR branch's rows). So we
// fall back to a full records (primary) scan of the keyspace and let the residual
// Filter apply the OR predicate -- exactly the behavior when no index exists, so
// always correct. (Without this, advertising indexes would turn a previously
// working OR query into an Unsupported UnionScan.)
func (c *Conv) VisitUnionScan(o *plan.UnionScan) (interface{}, error) {
	scans := o.Scans()
	if len(scans) == 0 {
		return NA(o)
	}
	is, ok := scans[0].(*plan.IndexScan)
	if !ok {
		return NA(o)
	}
	return c.recordsScan(is, is.Term().Alias())
}

// VisitIntersectScan handles an AND of several sargable secondary indexes. cbq
// intersects the per-index docID sets; n1k1 has no set-intersection scan op, so
// we convert just the *first* child index scan and lean on the residual Filter
// the planner always places after the scan to enforce the full predicate. Using
// one index yields a superset of docIDs, which the Filter then narrows -- correct,
// just less selective than a true intersection. (v1 pragmatic reduction; a real
// intersect op is future work. Verified against the no-index path.)
func (c *Conv) VisitIntersectScan(o *plan.IntersectScan) (interface{}, error) {
	return c.firstIntersectScan(o.Scans())
}

func (c *Conv) VisitOrderedIntersectScan(o *plan.OrderedIntersectScan) (interface{}, error) {
	return c.firstIntersectScan(o.Scans())
}

func (c *Conv) firstIntersectScan(scans []plan.SecondaryScan) (interface{}, error) {
	if len(scans) == 0 {
		return NA(nil)
	}
	return scans[0].Accept(c)
}

func (c *Conv) VisitExpressionScan(o *plan.ExpressionScan) (interface{}, error) {
	// A correlated FROM-expr is resolved against the outer row (corrParent) by
	// expr-scan at runtime. That works for an identifier (a CTE ref -- e.g. a
	// WITH RECURSIVE step's `FROM r`) or a path into the outer row (a correlated
	// subquery's `FROM orders.orderlines`, which iterates the outer order's
	// lineitems). A correlated *subquery* FROM-expr (FROM (SELECT ... outer.x))
	// would need nested subquery evaluation and isn't supported yet.
	if o.IsCorrelated() {
		if _, isSubq := o.FromExpr().(expression.Subquery); isSubq {
			return NA(o)
		}
	}

	// The FROM expression: a constant (FROM [1,2,3] AS x), a subquery
	// (FROM (SELECT ...) AS x), or a WITH CTE reference (FROM cte AS x, an
	// identifier).
	expr := o.FromExpr()
	var cteAlias string         // set iff this scan resolves a non-recursive WITH CTE
	var cteWith expression.With // the resolved CTE binding (for the materialize pass)
	if id, ok := expr.(*expression.Identifier); ok {
		if w, ok := c.withBindings[id.Identifier()]; ok {
			if c.withFromUsed == nil {
				c.withFromUsed = map[string]bool{}
			}
			c.withFromUsed[id.Identifier()] = true // consumed as FROM -> not a withScope var
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
			cteAlias, cteWith = id.Identifier(), w
		}
	}

	// A streaming table-valued source (a JS *.stream.js source like FROM gen(...),
	// or FROM rule_matches(...)): any FROM expression implementing StreamSource is
	// routed to the generic stream-fn op, which drives its StreamRows with an emit
	// callback and yields rows as they're produced -- no materialization. Plain
	// (array-returning) functions and subqueries/CTEs fall through to expr-scan.
	if _, ok := expr.(StreamSource); ok {
		return c.TopPush(o, &base.Op{
			Kind:   "stream-fn",
			Labels: base.Labels{"." + LabelSuffix(o.Alias())},
			Params: []interface{}{c.AddTemp(expr)},
		})
	}

	// Evaluate the expression at runtime (via the "expr-scan" glue op) rather
	// than at convert time: a subquery / CTE binding needs the engine + the
	// datastore, which only exist at runtime. The live expression is passed
	// through a vars.Temps slot (like datastore ops carry their plan objects).
	exprSlot := c.AddTemp(expr)
	op := &base.Op{
		Kind:   "expr-scan",
		Labels: base.Labels{"." + LabelSuffix(o.Alias())},
		Params: []interface{}{exprSlot},
	}
	if _, err := c.TopPush(o, op); err != nil {
		return nil, err
	}

	// Record a FROM-used non-recursive CTE reference so a CTE referenced from
	// FROM more than once can be materialized ONCE into a spillable temp instead
	// of being re-evaluated (re-read + re-boxed) per reference and per nested-loop
	// rescan. See materializeMultiRefCTEs (optimize_cte.go).
	if cteAlias != "" {
		c.recordCTEFromRef(cteAlias, cteWith, expr, exprSlot, op)
	}

	return c.TopOp, nil
}

// FTS Search

// VisitIndexFtsSearch converts the FTS scan the planner emits for a SEARCH()
// predicate over a bleve-backed index (idx_fts.go). datastore-scan-fts runs
// bleve.Search, fetches the matching docs itself, and yields each as `.alias` +
// `^id` + `^smeta` (the search-meta attachment carrying the hit score). It fetches
// in the op rather than via a following plan.Fetch because the score is only
// available at the scan and would be lost across a separate fetch -- so we emit no
// synth fetch here, and VisitFetch passes through after an FTS scan. Because our
// Sargable returns exact=true the planner drops the residual SEARCH() filter, which
// n1k1 couldn't evaluate anyway (VisitFilter strips it, gated on sawFTS).
func (c *Conv) VisitIndexFtsSearch(o *plan.IndexFtsSearch) (interface{}, error) {
	c.sawFTS = true
	return c.TopPush(o, &base.Op{
		Kind:   "datastore-scan-fts",
		Labels: base.Labels{"." + LabelSuffix(o.Term().Alias()), "^id", "^smeta"},
		Params: []interface{}{c.AddTemp(o)},
	})
}

// Fetch

func (c *Conv) VisitFetch(o *plan.Fetch) (interface{}, error) {
	// A file-datastore primary scan (datastore-scan-records) already materialized
	// whole documents, so the fetch is a no-op pass-through. (Index scans still
	// yield only keys, so their fetch stays a real datastore-fetch below.)
	//
	// This precedes the SubPaths() guard on purpose: when the planner references
	// META() it attaches a metadata subpath (e.g. "$document.exptime") to the
	// Fetch, but the file datastore has no such per-doc metadata -- exptime etc.
	// don't exist -- and META().id already rides the scan's "^id" label. So for a
	// whole-doc scan the subpaths are simply irrelevant and safely ignored.
	if c.TopOp != nil && c.TopOp.Kind == "datastore-scan-records" {
		// Column-projection pushdown (DESIGN-col.md Step 4): the planner already
		// computed the complete set of top-level fields this query reads from the
		// keyspace (with a cover-check), exposed as Fetch.EarlyProjection(). Attach
		// it so DatastoreScanRecords can hand it to a records.ColumnsProjector (e.g.
		// the Parquet source) and decode only those columns. Empty => read all.
		if !DisableColumnProjection {
			if proj := o.EarlyProjection(); len(proj) > 0 {
				c.TopOp.Params = append(c.TopOp.Params, []interface{}{"project-columns", proj})
			}
		}
		return c.TopOp, nil
	}

	// An FTS scan (datastore-scan-fts) already fetched the docs itself (so it could
	// attach the hit score before the doc value was replaced), so the planner's
	// following plan.Fetch is a no-op pass-through. See VisitIndexFtsSearch.
	if c.TopOp != nil && c.TopOp.Kind == "datastore-scan-fts" {
		return c.TopOp, nil
	}

	// A covering-but-not-reconstructable IndexScan (e.g. #primary covering
	// META().id under a LIKE range) already SYNTHESIZED a datastore-fetch to
	// materialize the doc (see VisitIndexScan) -- so the planner's following
	// plan.Fetch is redundant. Adding a second fetch would read the already-
	// materialized doc (the `.alias` val at index 0) as if it were the ^id key,
	// find no such document, and drop every row. Pass through. (Two consecutive
	// datastore-fetches are never meaningful -- the first has the doc already.)
	if c.TopOp != nil && c.TopOp.Kind == "datastore-fetch" {
		return c.TopOp, nil
	}

	if len(o.SubPaths()) > 0 {
		return NA(o) // TODO: metadata subpaths on a real (index-scan) fetch.
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
	// A lookup JOIN needs an ON KEYS expression to fetch the right side. A
	// comma-join term (FROM a, b) has none -- JoinKeys() is nil -- so bail out
	// cleanly rather than emit an ["exprTree", nil] that panics at eval time.
	if o.Term().JoinKeys() == nil {
		return NA(o)
	}

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

// VisitNLJoin converts an ANSI nested-loop JOIN. The left input is the current
// TopOp; the right (inner) side is o.Child(), a self-contained sub-plan we
// convert as a fresh branch. OpJoinNestedLoop re-drives the right branch for
// each left row and applies the ON clause to the joined left+right vals.
func (c *Conv) VisitNLJoin(o *plan.NLJoin) (interface{}, error) {
	// A nil ON clause is a comma/cross join (FROM a, b): every left row pairs
	// with every right row. The nested-loop op needs a predicate to evaluate, so
	// stand in a constant TRUE -- a cross product is a nested-loop join whose
	// filter always holds. (A nil ["exprTree", nil] would panic at eval time.)
	onclause := o.Onclause()
	if onclause == nil {
		onclause = expression.TRUE_EXPR
	}
	right, err := c.convertBranch(o.Child())
	if err != nil {
		return nil, err
	}
	c.TopSet(o, c.ansiJoinOp("joinNL", o.Outer(), onclause, c.TopOp, right))

	// The planner may push an extra predicate into the join's Filter -- most
	// commonly a comma/cross join (FROM a, b WHERE p(a,b)) carries a nil ON clause
	// and puts p in Filter(). cbq applies Filter to EVERY emitted row (both matched
	// pairs and, for an outer join, null-extended left rows -- execution/join_nl.go's
	// checkSendItem), which is exactly a filter on the join's output. Replicate that
	// here; without it the predicate is silently dropped and a cross product leaks.
	if f := o.Filter(); f != nil {
		return c.TopPush(o, &base.Op{
			Kind:   "filter",
			Labels: c.TopOp.Labels,
			Params: []interface{}{"exprTree", f},
		})
	}
	return c.TopOp, nil
}

func (c *Conv) VisitNLNest(o *plan.NLNest) (interface{}, error)     { return NA(o) }
func (c *Conv) VisitHashNest(o *plan.HashNest) (interface{}, error) { return NA(o) }

// VisitHashJoin converts an ANSI HASH JOIN. n1k1 has no hash-join runtime wired
// through the glue layer yet, but the join is semantically a nested-loop join
// with the same ON clause, so we execute it as one (correct, if not as fast).
func (c *Conv) VisitHashJoin(o *plan.HashJoin) (interface{}, error) {
	right, err := c.convertBranch(o.Child())
	if err != nil {
		return nil, err
	}

	build, probe := o.BuildExprs(), o.ProbeExprs()

	// A comma-join the planner hashed has a nil ON clause: the equality lives in
	// build/probe (the same equality often repeated as a redundant Filter), e.g.
	// `FROM a, b WHERE a.x=b.y`. Build the single-key inner equijoin via OpJoinHash
	// and apply any Filter as a residual on top (a redundant filter is harmless; a
	// genuine residual is then still enforced). Composite-key / outer comma-join
	// hashes aren't handled -- bail cleanly (the NL fallback needs an ON clause).
	if o.Onclause() == nil {
		if o.Outer() || len(build) != 1 || len(probe) != 1 {
			return NA(o)
		}
		c.TopSet(o, &base.Op{
			Kind:   "joinHash-inner",
			Labels: append(append(base.Labels{}, right.Labels...), c.TopOp.Labels...),
			Params: []interface{}{
				[]interface{}{"exprTree", build[0]}, // build key, on right (Children[0])
				[]interface{}{"exprTree", probe[0]}, // probe key, on left (Children[1])
			},
			Children: []*base.Op{right, c.TopOp},
		})
		if f := o.Filter(); f != nil {
			return c.TopPush(o, &base.Op{
				Kind:   "filter",
				Labels: c.TopOp.Labels,
				Params: []interface{}{"exprTree", f},
			})
		}
		return c.TopOp, nil
	}

	// Use the real hash-join op (OpJoinHash: build a probe map from one side, probe
	// with the other) for a single build/probe key pair. build[0] is the child
	// (right) branch's key, probe[0] the outer (left/TopOp) branch's key. An INNER
	// join with a residual ON term applies the full ON as a filter on the matched
	// pairs; anything else the hash op can't express (composite keys, or a LEFT
	// OUTER with a residual) falls back to the nested-loop join, correct if slower.
	if o.Filter() == nil && len(build) == 1 && len(probe) == 1 {
		// OpJoinHash matches ONLY the single equi key (build[0] == probe[0]). If the
		// ON clause is an *expression.And it carries a RESIDUAL -- an extra ANDed term
		// beyond that key (a band / non-equi predicate, e.g. `... AND ctx.pos BETWEEN
		// m.pos-1 AND m.pos+1`) that the bare hash op cannot apply. For an INNER join
		// we hash-match the key and then apply the full ON clause as a residual filter
		// on the matched pairs (key-equality is a conjunct, so re-checking it is
		// redundant-safe). For a LEFT OUTER join the residual can't be pushed off the
		// join, so we fall through to the NL join (which evaluates the full Onclause).
		_, residual := o.Onclause().(*expression.And)

		if !o.Outer() {
			// INNER: fill the map from the right branch, probe with the left.
			c.TopSet(o, &base.Op{
				Kind:   "joinHash-inner",
				Labels: append(append(base.Labels{}, right.Labels...), c.TopOp.Labels...),
				Params: []interface{}{
					[]interface{}{"exprTree", build[0]}, // build key, on right (Children[0])
					[]interface{}{"exprTree", probe[0]}, // probe key, on left (Children[1])
				},
				Children: []*base.Op{right, c.TopOp},
			})
			if residual {
				return c.TopPush(o, &base.Op{
					Kind:   "filter",
					Labels: c.TopOp.Labels,
					Params: []interface{}{"exprTree", o.Onclause()},
				})
			}
			return c.TopOp, nil
		}

		// LEFT OUTER: OpJoinHash's leftOuter path applies ONLY the equijoin key and
		// preserves the *map/build* side -- correct only when the ON clause is exactly
		// that equijoin. With a residual (see above), fall through to the NL join.
		if !residual {
			// Preserved outer side = left (TopOp) must be the build side: fill the
			// map from the left using the left key (probe[0]), probe with the right
			// using the right key (build[0]). Unmatched left rows -- including ones
			// with a NULL/MISSING key, which OpJoinHash keeps -- come out with the
			// right side MISSING.
			return c.TopSet(o, &base.Op{
				Kind:   "joinHash-leftOuter",
				Labels: append(append(base.Labels{}, c.TopOp.Labels...), right.Labels...),
				Params: []interface{}{
					[]interface{}{"exprTree", probe[0]}, // build/map key = left key, on left (Children[0])
					[]interface{}{"exprTree", build[0]}, // probe key = right key, on right (Children[1])
				},
				Children: []*base.Op{c.TopOp, right},
			})
		}
	}

	// onclause != nil here (the nil case is fully handled above): the NL fallback
	// evaluates the full ON clause on the concatenated left+right vals.
	return c.TopSet(o, c.ansiJoinOp("joinNL", o.Outer(), o.Onclause(), c.TopOp, right))
}

// convertBranch converts a sub-plan into its own base.Op tree, independent of
// the current TopOp (saved and restored around the conversion), for use as a
// join's inner/right child.
func (c *Conv) convertBranch(child plan.Operator) (*base.Op, error) {
	saved := c.TopOp
	c.TopOp = nil
	if _, err := child.Accept(c); err != nil {
		c.TopOp = saved
		return nil, err
	}
	branch := c.TopOp
	c.TopOp = saved
	return branch, nil
}

// ansiJoinOp builds an ANSI join base.Op (kind "joinNL") over left+right with
// the ON clause as its filter, matching OpJoinNestedLoop's expectations: the
// filter is evaluated on the concatenated left+right labels/vals.
func (c *Conv) ansiJoinOp(kind string, outer bool, onclause expression.Expression,
	left, right *base.Op) *base.Op {
	if outer {
		kind += "-leftOuter"
	} else {
		kind += "-inner"
	}
	return &base.Op{
		Kind: kind,
		Labels: append(append(base.Labels{}, left.Labels...),
			right.Labels...),
		Params:   []interface{}{"exprTree", onclause},
		Children: []*base.Op{left, right},
	}
}

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

		// Append this binding's computed column. A GROUP BY <expr> AS <alias>
		// becomes a LETTING binding (alias = expr) after the group; when the
		// binding's expression is exactly a computed group key, read the value
		// the group already materialized (by synthetic label) rather than
		// re-evaluating it -- the grouped row has dropped the key's input fields
		// (e.g. DATE_PART_STR(dateAdded,...) can't recompute; dateAdded is gone).
		// Same treatment VisitInitialProject gives a projected group key.
		op.Labels = append(op.Labels, "."+LabelSuffix(b.Variable()))
		if lbl, ok := c.groupKeyExprLabels[b.Expression().String()]; ok {
			op.Params = append(op.Params, []interface{}{"labelPath", lbl})
		} else {
			op.Params = append(op.Params, []interface{}{"exprTree", b.Expression()})
		}

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
	cond := o.Condition()
	// When an FTS scan (plan.IndexFtsSearch) is in this plan, the bleve Search
	// already selected the matching docs, but the planner still leaves the
	// SEARCH() term in the residual Filter -- and n1k1 can't re-evaluate SEARCH()
	// (no live FTS verify; it would return false and drop every row). Strip the
	// covered SEARCH() term (-> true) so only the genuine residual predicate
	// remains. Gated on sawFTS so a SEARCH() with no FTS index still filters.
	if c.sawFTS {
		cond = stripSearch(cond)
	}
	return c.TopPush(o, &base.Op{
		Kind:   "filter",
		Labels: c.TopOp.Labels,
		Params: []interface{}{"exprTree", cond},
	})
}

// Group

func (c *Conv) VisitInitialGroup(o *plan.InitialGroup) (interface{}, error) {
	// The InitialGroup owns the GROUP AS binding; stash it for VisitFinalGroup,
	// which does the actual grouping (Initial/Intermediate are skipped).
	c.groupAs, c.groupAsFields = o.GroupAs(), o.GroupAsFields()
	return c.TopOp, nil // Skip as the final group will handle grouping.
}

func (c *Conv) VisitIntermediateGroup(o *plan.IntermediateGroup) (interface{}, error) {
	return c.TopOp, nil // Skip as the final group will handle grouping.
}

func (c *Conv) VisitFinalGroup(o *plan.FinalGroup) (interface{}, error) {
	var labels base.Labels
	var groups []interface{}

	for _, key := range o.Keys() {
		groups = append(groups, []interface{}{"exprTree", key})

		// A plain field-path key (e.g. `custId`, `a.b`) materializes its value
		// at that path in the grouped row, so the downstream projection can
		// re-evaluate the same expression against it. Label it by the path.
		if fieldPath, ok := ExprFieldPath(key); ok {
			labels = append(labels, "."+LabelSuffix(strings.Join(fieldPath, `","`)))
			continue
		}

		// A computed key (e.g. `orderlines[1]`, `now_str()`) has no field path
		// to re-materialize at, and can't be reconstructed from the grouped row.
		// Store its value under a synthetic label; VisitInitialProject reads it
		// back by that label when a projected term is exactly this key. (Terms
		// that merely *recompute* -- e.g. now_str() inside a larger expr -- fall
		// through to re-evaluation, which is fine for side-effect-free funcs.)
		if c.groupKeyExprLabels == nil {
			c.groupKeyExprLabels = map[string]string{}
		}
		synth := "." + LabelSuffix(fmt.Sprintf("$group%d", c.groupKeyCount))
		c.groupKeyCount++
		c.groupKeyExprLabels[key.String()] = synth
		labels = append(labels, synth)
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
		if _, ok := base.AggCatalog[aggName+"_distinct"]; ok && agg.Distinct() {
			aggName += "_distinct" // e.g. COUNT(DISTINCT x), MEDIAN(DISTINCT x).
		}
		aggCalcs = append(aggCalcs, []interface{}{aggName})

		labels = append(labels, "^aggregates|"+agg.String())
	}

	// GROUP AS <name>: bind <name> to an array of one object per grouped row,
	// {field: <that row's field value>} over the in-scope group-as fields --
	// exactly what query materializes (execution/groupby_initial.go). Model it as
	// ARRAY_AGG of an ObjectConstruct so it rides the existing aggregate path,
	// and label it as a plain field path (not a "^aggregates" attachment) so the
	// projection / HAVING / LETTING / ORDER BY can reference <name> as an ordinary
	// identifier (e.g. len(g), g[0].orders.id). An absent field is simply omitted
	// from the object (ObjectConstruct drops MISSING values), matching query.
	if groupAs := c.groupAs; groupAs != "" {
		mapping := map[expression.Expression]expression.Expression{}
		for _, f := range c.groupAsFields {
			mapping[expression.NewConstant(f)] = expression.NewIdentifier(f)
		}
		aggExprs = append(aggExprs, []interface{}{"exprTree", expression.NewObjectConstruct(mapping)})
		aggCalcs = append(aggCalcs, []interface{}{"array_agg"})
		labels = append(labels, "."+LabelSuffix(groupAs))
		c.groupAs, c.groupAsFields = "", nil // consume
	}

	return c.TopPush(o, &base.Op{
		Kind:   "group",
		Labels: labels,
		Params: []interface{}{groups, aggExprs, aggCalcs},
	})
}

// Window functions

// DefaultWindowFrame is the SQL default frame when a window has an ORDER BY but
// no explicit frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW.
var DefaultWindowFrame = algebra.NewWindowFrame(algebra.WINDOW_FRAME_RANGE,
	algebra.WindowFrameExtents{
		algebra.NewWindowFrameExtent(nil, algebra.WINDOW_FRAME_UNBOUNDED_PRECEDING),
		algebra.NewWindowFrameExtent(nil, algebra.WINDOW_FRAME_CURRENT_ROW),
	})

// DefaultWindowFrameNoOrderBy is the SQL default frame when a window has NO
// ORDER BY: the WHOLE partition (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED
// FOLLOWING). Using the CURRENT-ROW default here instead would (a) be wrong --
// e.g. SUM(...) OVER() must total the partition, not stop at the current row --
// and (b) drive CurrentUpdate into FindGroupEdge, which has no order-by column to
// walk and ran off the partition end (a slice-out-of-range crash).
var DefaultWindowFrameNoOrderBy = algebra.NewWindowFrame(algebra.WINDOW_FRAME_RANGE,
	algebra.WindowFrameExtents{
		algebra.NewWindowFrameExtent(nil, algebra.WINDOW_FRAME_UNBOUNDED_PRECEDING),
		algebra.NewWindowFrameExtent(nil, algebra.WINDOW_FRAME_UNBOUNDED_FOLLOWING),
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
		// PARTITION BY exprs are shared across the aggregates; append THIS aggregate's
		// ORDER BY exprs (partitioning only needs val equality, so asc/desc is ignored).
		partitionings := append([]interface{}(nil), partitionBys...) // Clone.
		var orderByExprs []interface{}
		if agg.WindowTerm().OrderBy() != nil {
			for _, e := range agg.WindowTerm().OrderBy().Expressions() {
				orderByExprs = append(orderByExprs, []interface{}{"exprTree", e})
			}
			partitionings = append(partitionings, orderByExprs...)
		}

		// Resolve the frame: explicit, else the ORDER BY-dependent default (with an
		// ORDER BY it's UNBOUNDED PRECEDING .. CURRENT ROW; without, the whole
		// partition -- DefaultWindowFrameNoOrderBy).
		var wf *algebra.WindowFrame
		if agg.WindowTerm() != nil {
			wf = agg.WindowTerm().WindowFrame()
		}
		if wf == nil {
			if agg.WindowTerm() != nil && agg.WindowTerm().OrderBy() != nil {
				wf = DefaultWindowFrame
			} else {
				wf = DefaultWindowFrameNoOrderBy
			}
		}

		frameType := "rows"
		if wf.HasModifier(algebra.WINDOW_FRAME_RANGE) {
			frameType = "range"
		} else if wf.HasModifier(algebra.WINDOW_FRAME_GROUPS) {
			frameType = "groups"
		}

		// Ranking window functions (computed from partition position + peer groups,
		// not a frame aggregate). ROW_NUMBER needs only the position; RANK/DENSE_RANK
		// need peer detection, i.e. the ORDER BY value column.
		winFuncName := strings.ToLower(agg.Name())
		isRankRowNumber := winFuncName == "row_number"
		isRankRank := winFuncName == "rank"
		isRankDense := winFuncName == "dense_rank"
		isRankPercent := winFuncName == "percent_rank"
		isRankCume := winFuncName == "cume_dist"
		isRankNtile := winFuncName == "ntile"
		// PERCENT_RANK/CUME_DIST need peer detection (like RANK/DENSE_RANK); NTILE and
		// ROW_NUMBER need only the row position.
		isRankPeer := isRankRank || isRankDense || isRankPercent || isRankCume

		// Offset / navigation window functions -> StepToOffset (initial, asc, num).
		// FIRST/LAST/NTH_VALUE respect the frame; LAG/LEAD are partition-relative, so
		// they force a whole-partition frame (below). num for NTH/LAG/LEAD is the 2nd
		// operand (a constant), default 1.
		isFirstValue := winFuncName == "first_value"
		isLastValue := winFuncName == "last_value"
		isNthValue := winFuncName == "nth_value"
		isLag := winFuncName == "lag"
		isLead := winFuncName == "lead"
		isOffset := isFirstValue || isLastValue || isNthValue || isLag || isLead

		offInitial, offNum := 0, 1
		offAsc := false
		if isOffset {
			ops := agg.Operands()
			nthArg := func() int {
				if len(ops) > 1 && ops[1] != nil {
					if n := int(EvalExprInt64(nil, ops[1], nil, 1)); n > 0 {
						return n
					}
				}
				return 1
			}
			switch {
			case isFirstValue:
				offInitial, offAsc, offNum = -1, true, 1
			case isLastValue:
				offInitial, offAsc, offNum = 1, false, 1
			case isNthValue:
				offInitial, offAsc, offNum = -1, true, nthArg()
			case isLag:
				offInitial, offAsc, offNum = 0, false, nthArg()
			case isLead:
				offInitial, offAsc, offNum = 0, true, nthArg()
			}
		}

		// The ORDER BY is stored as a trailing "^worderby" column (the frame's ValIdx)
		// for peer/value detection. Two modes:
		//   "value" -- the single numeric ORDER BY value, for a RANGE frame's arithmetic
		//              bounds (ParseFloat64). Single ORDER BY column only.
		//   "tuple" -- the ORDER BY tuple canonically encoded into one column, for peer
		//              detection by bytes.Equal -- GROUPS frames and RANK/DENSE_RANK/
		//              PERCENT_RANK/CUME_DIST. Works for any number of ORDER BY columns.
		// LAG/LEAD force a whole-partition frame, so they don't need it; ROWS aggregates,
		// ROW_NUMBER, and NTILE don't either.
		isRanking := isRankRowNumber || isRankNtile || isRankPeer
		orderByMode := ""
		if len(orderByExprs) >= 1 && !isLag && !isLead {
			if isRankPeer || frameType == "groups" {
				orderByMode = "tuple" // Composite-key peers OK (any # of ORDER BY columns).
			} else if frameType == "range" && len(orderByExprs) == 1 {
				orderByMode = "value" // Single numeric ORDER BY column.
			}
		}
		appendOrderBy := orderByMode != ""

		partitionSlot := c.AddTemp(nil)

		// Chain: the first aggregate's partition reads the window input; each
		// subsequent one reads the PREVIOUS aggregate's frames op, so appended
		// "^aggregates|..." columns accumulate down the chain and ALL reach the
		// projection. (OpWindowPartition drives only Children[0]; re-partitioning per
		// aggregate supports aggregates with differing PARTITION BY / ORDER BY.)
		windowChild := c.TopOp
		if rv != nil {
			windowChild = rv
		}

		partitionLabels := append(base.Labels(nil), windowChild.Labels...)
		if appendOrderBy {
			partitionLabels = append(partitionLabels, "^worderby")
		}

		partitionOp := &base.Op{
			Kind:   "window-partition",
			Labels: partitionLabels,
			Params: []interface{}{
				partitionSlot,
				partitionings,
				len(partitionBys), // # of the partitioning exprs for PARTITION-BY.
				"",                // TODO: Additional tracking ("rank,denseRank").
				orderByMode,       // "" | "value" | "tuple": how "^worderby" is stored.
			},
			Children: []*base.Op{windowChild},
		}

		frameCfg := []interface{}{frameType}

		appendExtent := func(wfe *algebra.WindowFrameExtent) {
			if wfe.HasModifier(algebra.WINDOW_FRAME_CURRENT_ROW) {
				frameCfg = append(frameCfg, "num", 0)
			} else if wfe.HasModifier(algebra.WINDOW_FRAME_VALUE_PRECEDING) {
				// TODO: Handle non-int64 RANGE extent. int() cast is required:
				// WindowFrame.Init does parts[N].(int), so an int64 (what
				// EvalExprInt64 returns) would silently fall back to a 0 extent.
				n := EvalExprInt64(nil, wfe.ValueExpression(), nil, 0)
				frameCfg = append(frameCfg, "num", int(-n))
			} else if wfe.HasModifier(algebra.WINDOW_FRAME_VALUE_FOLLOWING) {
				// TODO: Handle non-int64 RANGE extent.
				n := EvalExprInt64(nil, wfe.ValueExpression(), nil, 0)
				frameCfg = append(frameCfg, "num", int(n))
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

		// ValIdx points at the trailing "^worderby" column (the ORDER BY value) for
		// RANGE/GROUPS peer/value comparisons; unused (0) for ROWS frames.
		valIdx := 0
		if appendOrderBy {
			valIdx = len(windowChild.Labels)
		}

		frameCfg = append(frameCfg, valIdx)

		// LAG/LEAD are partition-relative (frame-independent), and ranking functions
		// (ROW_NUMBER/RANK/DENSE_RANK/PERCENT_RANK/CUME_DIST/NTILE) compute from position
		// + peers, not a frame. Both force a whole-partition ROWS frame: StepToOffset can
		// then reach any offset row, and -- crucially for the "tuple" ValIdx column --
		// CurrentUpdate never runs FindGroupEdge/ParseFloat64 (which a RANGE frame would,
		// failing on a canonical multi-column tuple).
		if isLag || isLead || isRanking {
			frameCfg = []interface{}{"rows", "unbounded", 0, "unbounded", 0, "no-others", valIdx}
		}

		framesSlot := c.AddTemp(nil)

		framesLabels := append(base.Labels(nil), windowChild.Labels...)
		if appendOrderBy {
			framesLabels = append(framesLabels, "^worderby")
		}
		framesParams := []interface{}{partitionSlot, framesSlot, []interface{}{frameCfg}}

		// Native frame aggregate: SUM/COUNT/AVG/MIN/MAX/... OVER (...). When the
		// window function maps to a base.AggCatalog aggregate, have the
		// window-frames op compute it over each row's frame and append it under
		// "^aggregates|"+agg.String(); the projection then reads it natively
		// (exprTreeOptimizeNative), exactly as GROUP BY aggregates do. Ranking and
		// offset functions take their own branches below, through the same label path.
		//
		// Wired: ROWS frames; all-unbounded frames (OVER () / OVER (PARTITION BY g));
		// RANGE frames whose single numeric ORDER BY value is stored ("value" mode); and
		// GROUPS frames whose canonical ORDER BY tuple is stored ("tuple" mode, any # of
		// ORDER BY columns). A RANGE frame with multi-column ORDER BY has no single
		// numeric value to bound on, so it stays boxed rather than mis-computing.
		begUnbounded := len(frameCfg) > 1 && frameCfg[1] == "unbounded"
		endUnbounded := len(frameCfg) > 3 && frameCfg[3] == "unbounded"
		frameNativeOK := frameType == "rows" || (begUnbounded && endUnbounded) || appendOrderBy

		wired := false // did this window function map to a native ^aggregates column?

		if isRankRowNumber || isRankNtile || (isRankPeer && appendOrderBy) {
			// Ranking / partition-level: the frames op computes it from position + peer
			// groups + partition count (no aggregate). ROW_NUMBER and NTILE need only the
			// position; RANK/DENSE_RANK/PERCENT_RANK/CUME_DIST need appendOrderBy (peer
			// detection) -- without a single ORDER BY they fall through and stay boxed.
			framesParams = append(framesParams, winFuncName)
			if isRankNtile {
				// NTILE(k): k is the first (constant) operand -> Params[4] (an int, so the
				// op tells it apart from an aggregate/offset operand slice). Default 1.
				k := 1
				if ops := agg.Operands(); len(ops) > 0 && ops[0] != nil {
					if n := int(EvalExprInt64(nil, ops[0], nil, 1)); n > 0 {
						k = n
					}
				}
				framesParams = append(framesParams, k)
			}
			framesLabels = append(framesLabels, "^aggregates|"+agg.String())
			wired = true
		} else if isOffset && frameNativeOK {
			// Offset/navigation: pass the operand + StepToOffset navigation (initial,
			// asc, num); the frames op evaluates the operand at the target row.
			var operand []interface{}
			if ops := agg.Operands(); len(ops) > 0 && ops[0] != nil {
				operand = []interface{}{"exprTree", ops[0]}
			} else {
				operand = []interface{}{"json", "null"}
			}
			framesParams = append(framesParams, winFuncName, operand, offInitial, offAsc, offNum)
			framesLabels = append(framesLabels, "^aggregates|"+agg.String())
			wired = true
		} else {
			aggName := winFuncName
			if _, ok := base.AggCatalog[aggName+"_distinct"]; ok && agg.Distinct() {
				aggName += "_distinct"
			}
			if _, ok := base.AggCatalog[aggName]; ok && frameNativeOK {
				var operand []interface{}
				if ops := agg.Operands(); len(ops) == 0 || ops[0] == nil {
					operand = []interface{}{"json", "true"} // COUNT(*): non-MISSING per row.
				} else {
					operand = []interface{}{"exprTree", ops[0]}
				}
				framesParams = append(framesParams, aggName, operand)
				framesLabels = append(framesLabels, "^aggregates|"+agg.String())
				wired = true
			}
		}

		if !wired {
			// A window function n1k1 doesn't compute natively: an unlisted aggregate
			// (RATIO_TO_REPORT / COUNTN / ...), a non-native offset or aggregate frame
			// (e.g. a multi-column RANGE), or a peer ranking without a single ORDER BY.
			// Leaving it boxed makes the projection invoke cbq's window Evaluate, which
			// panics on n1k1's plain (non-AnnotatedValue) rows -- so reject the whole
			// statement gracefully (NA) instead of emitting a landmine.
			return NA(o)
		}

		framesOp := &base.Op{
			Kind:     "window-frames",
			Labels:   framesLabels,
			Params:   framesParams,
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
			//
			// The star value is the fully-assembled row object (e.g.
			// {"alias": doc}). The native byte path (engine.ExprSelf, gated by
			// selfNativeSpecFor) assembles it straight from the input label
			// vals -- zero per-row garbage -- for the common shapes (bare
			// expression.Self over plain .["name"] field labels). It emits keys
			// in label order, whereas cbq's value.WriteJSON emits them SORTED
			// (sortedNames, recursively); that is a byte-level, not value-level,
			// difference, and n1k1's result comparison is key-order-insensitive
			// (see the test harness canonJSON / rowsMatch). Byte-identical
			// output would additionally need a verified canonical serializer
			// (blocked on encoder fidelity -- TODO(encoder-fidelity) in
			// base/compare.go; see DESIGN-exprs.md "self-projection byte path").
			//
			// Shapes ExprSelf can't reproduce (path stars like SELECT p.*,
			// whole-row ".", nested paths, ".*" spreads) fall back to the boxed
			// exprTree, which delegates to cbq's serializer (one Convert +
			// WriteJSON/row). The exprResetScope marker makes that box evaluate
			// the star row WITHOUT the corrParent/withScope wrap, so it spreads
			// only the row's own fields -- WITH aliases (withScope) and outer
			// correlated rows don't leak into * (stripBindingNames still removes
			// LET names, which live in the row itself). Mirrors query's
			// ResetParent(nil). ExprSelf gets the same effect for free: it
			// iterates only the row's own labels (which exclude withScope /
			// corrParent) and selfNativeSpec drops the binding names.
			op.Labels = append(op.Labels, ".*")
			if selfParam, ok := selfNativeSpecFor(c, o, rt.Expression()); ok {
				op.Params = append(op.Params, selfParam)
			} else {
				op.Params = append(op.Params,
					[]interface{}{"exprTree", stripBindingNames(rt.Expression(), o.BindingNames()), exprResetScope})
			}
			continue
		}

		label := "." + LabelSuffix(rt.Alias())
		if raw {
			label = "."
		}
		op.Labels = append(op.Labels, label)

		// If this term is exactly a computed GROUP BY key, read the value the
		// group already materialized (by synthetic label) rather than
		// re-evaluating -- the grouped row can't reconstruct e.g. orderlines[1].
		if lbl, ok := c.groupKeyExprLabels[rt.Expression().String()]; ok {
			op.Params = append(op.Params, []interface{}{"labelPath", lbl})
		} else {
			op.Params = append(op.Params,
				[]interface{}{"exprTree", c.inlineProjectedSubqueryCTE(rt.Expression())})
		}
	}

	if _, err := c.TopPush(o, op); err != nil {
		return nil, err
	}

	// SELECT ... EXCLUDE <path>...: remove the excluded paths from the projected
	// value. Only supported for a lone unprefixed star (SELECT * EXCLUDE ...) --
	// query resolves that via getExclusions(singleQualification=true) with the
	// row as self; other shapes (o.* EXCLUDE, star mixed with terms) use a
	// different resolution we don't handle yet. The project-exclude glue op reuses
	// query's expression.GetReferences + DeleteFromObject at runtime (see
	// datastore.go) since the exclude paths can be nested, array-indexed, or
	// computed per row -- not expressible as a static OBJECT_REMOVE.
	if excl := projectionExclude(o); len(excl) > 0 {
		if len(op.Labels) == 1 && op.Labels[0] == ".*" {
			// TopPush chains the current TopOp (the project op) as the child.
			return c.TopPush(o, &base.Op{
				Kind:   "project-exclude",
				Labels: append(base.Labels{}, op.Labels...),
				Params: []interface{}{c.AddTemp(excl)},
			})
		}
		return NA(o) // EXCLUDE on a non-lone-star projection: not yet supported.
	}

	return c.TopOp, nil
}

// projectionExclude returns the projection's EXCLUDE expressions, or nil.
func projectionExclude(o *plan.InitialProject) expression.Expressions {
	if o.Projection() == nil {
		return nil
	}
	return o.Projection().Exclude()
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

	// DISTINCT is backed by OpGroup, which dedups on -- and yields -- exactly the
	// group-key exprs. So the key must cover *every* projected column (not just
	// the first): SELECT DISTINCT a, b dedups on the (a, b) pair, and OpGroup then
	// re-emits both. Emitting only Labels[0] both deduped on the wrong key and
	// yielded a single val against N labels (a label/vals arity mismatch that
	// crashed multi-column DISTINCT, e.g. ON KEYS join projections).
	groupExprs := make([]interface{}, 0, len(c.TopOp.Labels))
	for _, label := range c.TopOp.Labels {
		groupExprs = append(groupExprs, []interface{}{"labelPath", label})
	}

	return c.TopPush(o, &base.Op{
		Kind:   "distinct",
		Labels: c.TopOp.Labels,
		Params: []interface{}{groupExprs},
	})
}

// Set operators

// VisitUnionAll converts UNION ALL: each child is a self-contained SELECT
// sub-plan converted as its own branch; OpUnionAll runs them concurrently and
// remaps each child's vals to the union's output labels by label name. (Plain
// UNION is this wrapped in a plan.Distinct, which VisitDistinct handles.)
func (c *Conv) VisitUnionAll(o *plan.UnionAll) (interface{}, error) {
	children := make([]*base.Op, 0, len(o.Children()))
	for _, ch := range o.Children() {
		branch, err := c.convertBranch(ch)
		if err != nil {
			return nil, err
		}
		children = append(children, branch)
	}
	if len(children) == 0 {
		return NA(o)
	}
	// The union's output columns are the UNION (by name) of every branch's labels,
	// not just the first branch's -- cbq matches columns by name across branches
	// and a row carries only its own columns (others MISSING). Using children[0]
	// alone dropped columns unique to a later branch (e.g. `SELECT a ... UNION ALL
	// SELECT b AS x ...` lost x). OpUnionAll remaps each branch's vals to these
	// labels by name (missing -> MISSING, which the projection omits from output).
	var labels base.Labels
	seen := map[string]bool{}
	for _, ch := range children {
		for _, l := range ch.Labels {
			if !seen[l] {
				seen[l] = true
				labels = append(labels, l)
			}
		}
	}
	return c.TopSet(o, &base.Op{
		Kind:     "union-all",
		Labels:   labels,
		Children: children,
	})
}

func (c *Conv) VisitIntersectAll(o *plan.IntersectAll) (interface{}, error) {
	return c.setOp(o, "intersect", o.First(), o.Second(), o.Distinct())
}

func (c *Conv) VisitExceptAll(o *plan.ExceptAll) (interface{}, error) {
	return c.setOp(o, "except", o.First(), o.Second(), o.Distinct())
}

// setOp converts INTERSECT / EXCEPT ([ALL]) to the engine's hash-based set-op
// (OpJoinHash, kinds intersect-*/except-*): the left (first) branch fills a probe
// map keyed by each row's canonical whole-row encoding and the right (second)
// probes it. The op canonicalizes the vals itself (valsEncodeCanonical), so no
// key Params are needed; result rows are the left branch's, so its labels carry
// through. distinct picks -distinct (set semantics) vs -all (multiset min/diff).
func (c *Conv) setOp(o plan.Operator, kind string, first, second plan.Operator, distinct bool) (interface{}, error) {
	left, err := c.convertBranch(first)
	if err != nil {
		return nil, err
	}
	right, err := c.convertBranch(second)
	if err != nil {
		return nil, err
	}
	if distinct {
		kind += "-distinct"
	} else {
		kind += "-all"
	}
	return c.TopSet(o, &base.Op{
		Kind:     kind,
		Labels:   append(base.Labels{}, left.Labels...),
		Children: []*base.Op{left, right},
	})
}

// Order, Paging

func (c *Conv) VisitOrder(o *plan.Order) (interface{}, error) {
	exprs, dirs := []interface{}{}, []interface{}{}

	for _, term := range o.Terms() {
		exprs = append(exprs, []interface{}{"exprTree", term.Expression()})

		// nil item/context is fine for constant ASC/DESC and NULLS FIRST/LAST literals.
		dir := "asc"
		if term.Descending(nil, nil) {
			dir = "desc"
		}

		// Explicit NULLS FIRST/LAST -> encode into the direction so the order op places
		// null/missing keys accordingly. Omitted when unspecified (the natural position
		// -- ASC nulls first, DESC nulls last -- which the op's plain asc/desc handles
		// via ValComparer's collation, preserving the exact prior behavior).
		if term.NullsPosExpr() != nil {
			if term.NullsLast(nil, nil) {
				dir += "-nulls-last"
			} else {
				dir += "-nulls-first"
			}
		}

		dirs = append(dirs, dir)
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

		if orderReEvalsAggregate(exprs) {
			return NA(o)
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

	// No projection to bind against; an order term that re-evaluates an aggregate
	// here would panic (see orderReEvalsAggregate / the proj-block above).
	if orderReEvalsAggregate(exprs) {
		return NA(o)
	}

	return c.TopPush(o, &base.Op{
		Kind:   "order-offset-limit",
		Labels: c.TopOp.Labels,
		Params: params,
	})
}

// orderReEvalsAggregate reports whether any ORDER BY term (as converted into
// exprs) is still an "exprTree" carrying an aggregate -- i.e. it wasn't bound to
// an already-projected column and would re-evaluate the aggregate above the
// group/projection, which panics (the row lacks the "^aggregates" attachment).
func orderReEvalsAggregate(exprs []interface{}) bool {
	for _, x := range exprs {
		xx, ok := x.([]interface{})
		if !ok || len(xx) != 2 {
			continue
		}
		if name, _ := xx[0].(string); name != "exprTree" {
			continue
		}
		if e, ok := xx[1].(expression.Expression); ok && containsAggregate(e) {
			return true
		}
	}
	return false
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

// containsAggregate reports whether e is, or contains anywhere in its subtree,
// an aggregate function (algebra.Aggregate).
func containsAggregate(e expression.Expression) bool {
	if e == nil {
		return false
	}
	if _, ok := e.(algebra.Aggregate); ok {
		return true
	}
	for _, ch := range e.Children() {
		if containsAggregate(ch) {
			return true
		}
	}
	return false
}

// -------------------------------------------------------------------

func ExprFieldPath(expr expression.Expression) (rv []string, ok bool) {
	var visit func(e expression.Expression) bool // Declare for recursion.

	visit = func(e expression.Expression) bool {
		if f, ok := e.(*expression.Field); ok {
			// Only a STATIC field step (`a.b`, where Second is a *FieldName) is a
			// path component. A DYNAMIC field (`a.[expr]` / `a.[$p]`, where Second is
			// an arbitrary expression) can't be a static path -- its name is computed
			// per row -- so bail: the caller then uses the general expr.Evaluate path,
			// which navigates the dynamic field correctly (cbq's Field.Evaluate).
			fn, isFieldName := f.Second().(*expression.FieldName)
			if !isFieldName {
				return false
			}
			if !visit(f.First()) {
				return false
			}

			rv = append(rv, fn.Alias())
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
