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
	si, ok := o.Index().(*secondaryIndex)
	if !ok {
		return false
	}
	if len(o.FilterCovers()) > 0 {
		return false
	}
	return si.def.coverable()
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

// VisitIndexFtsSearch converts the FTS scan the planner emits for a SEARCH()
// predicate over a bleve-backed index (fts.go). datastore-scan-fts runs
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
		return c.TopOp, nil
	}

	// An FTS scan (datastore-scan-fts) already fetched the docs itself (so it could
	// attach the hit score before the doc value was replaced), so the planner's
	// following plan.Fetch is a no-op pass-through. See VisitIndexFtsSearch.
	if c.TopOp != nil && c.TopOp.Kind == "datastore-scan-fts" {
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
	// A nil ON clause is a comma/cross join (FROM a, b) -- no join predicate to
	// feed the nested-loop filter; bail cleanly rather than emit an
	// ["exprTree", nil] that panics at eval time.
	if o.Onclause() == nil {
		return NA(o)
	}
	right, err := c.convertBranch(o.Child())
	if err != nil {
		return nil, err
	}
	return c.TopSet(o, c.ansiJoinOp("joinNL", o.Outer(), o.Onclause(), c.TopOp, right))
}

func (c *Conv) VisitNLNest(o *plan.NLNest) (interface{}, error)     { return NA(o) }
func (c *Conv) VisitHashNest(o *plan.HashNest) (interface{}, error) { return NA(o) }

// VisitHashJoin converts an ANSI HASH JOIN. n1k1 has no hash-join runtime wired
// through the glue layer yet, but the join is semantically a nested-loop join
// with the same ON clause, so we execute it as one (correct, if not as fast).
func (c *Conv) VisitHashJoin(o *plan.HashJoin) (interface{}, error) {
	if o.Onclause() == nil { // comma/cross join -- see VisitNLJoin.
		return NA(o)
	}
	right, err := c.convertBranch(o.Child())
	if err != nil {
		return nil, err
	}

	// Use the real hash-join op (OpJoinHash: build a probe map from one side,
	// probe with the other) for the common shape -- an equijoin on a single
	// build/probe key pair with no residual filter. build[0] is the child (right)
	// branch's key, probe[0] the outer (left/TopOp) branch's key. Anything else
	// (composite keys, or a residual non-equi ON filter OpJoinHash can't apply)
	// falls back to the nested-loop join, which is correct if slower.
	build, probe := o.BuildExprs(), o.ProbeExprs()
	if o.Filter() == nil && len(build) == 1 && len(probe) == 1 {
		if !o.Outer() {
			// INNER: fill the map from the right branch, probe with the left.
			return c.TopSet(o, &base.Op{
				Kind:   "joinHash-inner",
				Labels: append(append(base.Labels{}, right.Labels...), c.TopOp.Labels...),
				Params: []interface{}{
					[]interface{}{"exprTree", build[0]}, // build key, on right (Children[0])
					[]interface{}{"exprTree", probe[0]}, // probe key, on left (Children[1])
				},
				Children: []*base.Op{right, c.TopOp},
			})
		}
		// LEFT OUTER: OpJoinHash's leftOuter path applies ONLY the equijoin key and
		// preserves the *map/build* side. That's correct only when the ON clause is
		// exactly that equijoin: a residual predicate (an extra ANDed term) on the
		// non-preserved side can't be pushed off a LEFT JOIN and OpJoinHash won't
		// apply it, so anything but a bare equijoin must fall back to the NL join
		// (which evaluates the full Onclause). Unlike the inner case, we can't rely
		// on residuals being pushed to the probe scan (the planner doesn't always).
		if _, residual := o.Onclause().(*expression.And); !residual {
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

		// If this term is exactly a computed GROUP BY key, read the value the
		// group already materialized (by synthetic label) rather than
		// re-evaluating -- the grouped row can't reconstruct e.g. orderlines[1].
		if lbl, ok := c.groupKeyExprLabels[rt.Expression().String()]; ok {
			op.Params = append(op.Params, []interface{}{"labelPath", lbl})
		} else {
			op.Params = append(op.Params,
				[]interface{}{"exprTree", rt.Expression()})
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
	// The union's output labels are the first branch's; OpUnionAll matches the
	// other branches' columns to these by label name (missing -> MISSING).
	return c.TopSet(o, &base.Op{
		Kind:     "union-all",
		Labels:   append(base.Labels{}, children[0].Labels...),
		Children: children,
	})
}

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
