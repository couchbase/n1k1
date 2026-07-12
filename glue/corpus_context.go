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

// Context-detector recognition for the shared sorted-stream substrate (DESIGN-sorting.md
// step 3, glue slice). A grep -A/-B/-C result detector -- the windowed
// match-flag idiom
//
//	SELECT ... FROM (
//	  SELECT ..., MAX(CASE WHEN <pred> THEN 1 ELSE 0 END)
//	    OVER (PARTITION BY <P> ORDER BY <O> ROWS BETWEEN B PRECEDING AND A FOLLOWING) AS near
//	  FROM <ks>) sub
//	WHERE sub.near = 1
//
// -- is otherwise a STANDALONE detector (its own scan + sort + window). recognizeContext
// Detector matches this canonical shape in the converted plan and extracts its context
// parameters; CorpusCompile then groups all context detectors sharing the SAME
// (keyspace, PARTITION, ORDER) signature onto ONE scan + ONE sort feeding a single
// engine.OpBroadcastContext -- so K context detectors share the dominant cost (the sort).
//
// The windowed subquery may be wrapped in EXTRA star-passthrough derived tables -- the
// @grep_context macro expands to two (`SELECT g.* FROM (SELECT gc.* FROM (<window>) gc
// WHERE gc.near=1) g`) -- and the recognizer/projection compose through them (IDEA-0029):
// the outer SELECT references the outermost alias, and the result projection maps its
// columns through the star(s) to the scan row. A RENAMING middle wrapper can't compose,
// so it bails to standalone (never emits wrong or empty result).
//
// It is PARANOID (the ASOF playbook): any deviation from the exact shape returns ok=false
// and the detector stays standalone (correct, just unshared), so a mis-match can never
// produce wrong findings. The grouped fan-out's findings are differential-tested against
// each detector's own SQL (its standalone window result is the oracle).

import (
	"bytes"
	"fmt"

	"github.com/couchbase/query/algebra"
	"github.com/couchbase/query/expression"

	"github.com/couchbase/n1k1/base"
)

// contextGroupEntry is one detector's label + extracted info within a context group.
type contextGroupEntry struct {
	label string
	info  contextDetInfo
}

// analyzeContextDetector parses/plans/converts one detector's SQL and, iff it is the
// canonical context idiom, returns its extracted contextDetInfo. A parse/plan/convert
// failure (or any non-context shape) returns ok=false -- the detector then falls through
// to analyzeCorpusDetector (which surfaces a genuine reject or classifies it fusable /
// standalone). Convert is done here with a plain per-detector Conv; the double convert (a
// non-context detector converts again in analyzeCorpusDetector) is a one-time compile
// cost, not a per-bundle-run cost.
func (s *Session) analyzeContextDetector(stmt string) (contextDetInfo, bool) {
	parsed, err := ParseStatement(stmt, s.Namespace, true)
	if err != nil {
		return contextDetInfo{}, false
	}
	qp, err := s.Store.PlanStatementQP(parsed, s.Namespace, nil, nil)
	if err != nil {
		return contextDetInfo{}, false
	}
	conv := &Conv{Temps: []interface{}{nil}}
	var convErr error
	func() {
		defer func() {
			if r := recover(); r != nil {
				convErr = fmt.Errorf("panic: %v", r)
			}
		}()
		_, convErr = qp.PlanOp().Accept(conv)
	}()
	if convErr != nil || conv.TopOp == nil {
		return contextDetInfo{}, false
	}
	return recognizeContextDetector(conv.TopOp, conv.Temps)
}

// contextDetInfo is the extracted description of one recognized context detector. All
// exprs are still rooted at the detector's scan alias; the builder re-roots them to the
// shared "." row via renameAliasToSelf.
type contextDetInfo struct {
	keyspaceName string
	keyspacer    interface{}
	alias        string
	scanLabels   base.Labels // the detector scan's labels, for native predicate lowering.

	partExpr   expression.Expression   // single PARTITION BY column (MVP)
	orderExprs []expression.Expression // ORDER BY columns (all ascending)

	beforeMatch int // grep -B: rows emitted before a match  (= frame FOLLOWING count)
	afterMatch  int // grep -A: rows emitted after a match   (= frame PRECEDING count)

	matchPred expression.Expression // the CASE WHEN predicate

	// proj is the detector's SELECT projection shaped as fused result (engine
	// extractor det[4]), re-rooted to the shared "." scan row -- so a fused context
	// finding's shape matches the same SELECT run standalone (IDEA-0025), exactly as
	// corpusDetInfo.proj does for the plain broadcast path (IDEA-0004). nil => the
	// whole scan row (buildContextBroadcast's fallback).
	proj []interface{}

	// sig is the grouping key: keyspace + the canonical (partition, order) column texts.
	// Detectors with the same sig share one scan + sort + broadcast-context.
	sig string
}

// recognizeContextDetector matches the canonical windowed match-flag idiom on a converted
// plan and extracts a contextDetInfo, or ok=false. See the file header for the shape.
func recognizeContextDetector(top *base.Op, temps []interface{}) (contextDetInfo, bool) {
	// (1) descend from the top through the outer projection(s) and any PURE outer sort
	// (order-offset-limit with Params = [terms, dirs], no OFFSET/LIMIT) to the `near`
	// filter. Projections and a pure sort don't change the row SET (corpus findings are
	// an unordered set), so skipping them is sound; a sort WITH an offset/limit, or any
	// non-project/-sort op, stops the descent (and a non-filter there -> bail).
	node := top
	var outerProject *base.Op // the detector's SELECT (IDEA-0025), captured to shape result.
	for node != nil && len(node.Children) == 1 &&
		(node.Kind == "project" || (node.Kind == "order-offset-limit" && len(node.Params) == 2)) {
		if outerProject == nil && node.Kind == "project" {
			outerProject = node
		}
		node = node.Children[0]
	}
	if node == nil || node.Kind != "filter" || len(node.Children) != 1 {
		return contextDetInfo{}, false
	}
	filter := node
	if !contextFilterIsPositive(filter) {
		return contextDetInfo{}, false
	}

	// (2) descend through the derived-table projection wrapper(s) -- a `SELECT self`
	// passthrough and the window-column project -- to the window-frames op. Only linear
	// single-child projects are traversed (a branch is not the context shape). The LAST
	// project (its child is window-frames) is the derived table's own projection, whose
	// terms are rooted at the scan alias -- used to compose the outer SELECT into
	// scan-row result (IDEA-0025). Any project ABOVE it (the @grep_context macro's
	// `SELECT sub.*` middle, IDEA-0029) must be a pure star passthrough: only then are
	// column names preserved 1:1, so composing the outer terms straight onto the window-
	// column project's scan-rooted terms is faithful. A middle that RENAMES columns could
	// map result to the wrong scan field, so -- per this file's paranoid doctrine -- it
	// bails to standalone, where the full pipeline honors the SELECT.
	node = filter.Children[0]
	var innerProject *base.Op
	for node != nil && node.Kind == "project" && len(node.Children) == 1 {
		if innerProject != nil && !isStarPassthrough(innerProject) {
			return contextDetInfo{}, false // a renaming middle project -> can't fuse faithfully.
		}
		innerProject = node
		node = node.Children[0]
	}
	if node == nil || node.Kind != "window-frames" || len(node.Children) != 1 {
		return contextDetInfo{}, false
	}
	wf := node

	// (3) window-frames: func MAX, a ROWS/no-others frame, operand = CASE WHEN <pred>.
	before, after, pred, ok := contextFromWindowFrames(wf)
	if !ok {
		return contextDetInfo{}, false
	}

	// (4) window-partition (partition-column count) -> order-offset-limit (the (P,O) sort).
	wp := wf.Children[0]
	if wp.Kind != "window-partition" || len(wp.Children) != 1 {
		return contextDetInfo{}, false
	}
	numPart, ok := contextInt(wp.Params, 2)
	if !ok || numPart != 1 { // MVP: exactly one PARTITION BY column.
		return contextDetInfo{}, false
	}
	ord := wp.Children[0]
	if ord.Kind != "order-offset-limit" {
		return contextDetInfo{}, false
	}
	partExpr, orderExprs, ok := contextSortExprs(ord, numPart)
	if !ok {
		return contextDetInfo{}, false
	}

	// (5) resolve the scan keyspace + alias.
	scanLabels, alias, ks := contextScan(top, temps)
	if ks == nil || alias == "" {
		return contextDetInfo{}, false
	}

	// (6) shape the detector's SELECT projection into fused result over the shared "."
	// scan row (IDEA-0025), then re-root to SELF via the plain-path corpusFusedProjection
	// -- same consistency guarantee IDEA-0004 gives the broadcast path. The outer SELECT
	// references the OUTERMOST derived-table alias; with more than one wrapper (the
	// @grep_context macro expands to two, `... ) gc) g`) that differs from the innermost
	// filter's alias, so substitute the alias the outer terms actually use (IDEA-0029),
	// falling back to the filter's for the single-nested shape where the two coincide. A
	// projection the fused envelope can't reproduce (it selects the window flag, or a
	// column the wrappers don't carry through) returns ok=false -> the detector runs
	// STANDALONE, where the full pipeline honors its SELECT.
	derivedAlias := outerRefAlias(outerProject)
	if derivedAlias == "" && len(filter.Labels) > 0 {
		derivedAlias, _ = mergeLabelLeaf(filter.Labels[0])
	}
	proj, ok := contextFusedProjection(outerProject, innerProject, derivedAlias, alias)
	if !ok {
		return contextDetInfo{}, false
	}

	info := contextDetInfo{
		keyspaceName: ks.QualifiedName(),
		keyspacer:    contextKeyspacer(ord, temps),
		alias:        alias,
		scanLabels:   scanLabels,
		partExpr:     partExpr,
		orderExprs:   orderExprs,
		beforeMatch:  before,
		afterMatch:   after,
		matchPred:    pred,
		proj:         proj,
	}
	if info.keyspacer == nil {
		return contextDetInfo{}, false
	}

	// Grouping signature: keyspace + the SELF-rooted (partition, order) column texts, so
	// detectors that sort identically (regardless of their scan alias) group together.
	sig := info.keyspaceName + "\x00" + renameAliasToSelf(partExpr, alias).String()
	for _, oe := range orderExprs {
		sig += "\x00" + renameAliasToSelf(oe, alias).String()
	}
	info.sig = sig
	return info, true
}

// contextFilterIsPositive reports whether the outer WHERE selects "flag present" -- the
// near column equated to a positive constant (WHERE near = 1). Anything else (an absence
// test near = 0, a non-constant, an OR, ...) is NOT the context idiom -> bail. This
// polarity check is what keeps an ABSENCE detector from being mis-lowered.
func contextFilterIsPositive(filter *base.Op) bool {
	expr, ok := corpusFilterExpr(filter)
	if !ok {
		return false
	}
	eq, ok := expr.(*expression.Eq)
	if !ok {
		return false
	}
	ops := eq.Operands()
	if len(ops) != 2 {
		return false
	}
	// One side a field (the near column), the other a numeric constant >= 1.
	for _, side := range []expression.Expression{ops[0], ops[1]} {
		if c, ok := side.(*expression.Constant); ok {
			if f, ok := c.Value().Actual().(float64); ok && f >= 1 {
				return true
			}
		}
	}
	return false
}

// contextFromWindowFrames extracts (beforeMatch, afterMatch, matchPred) from the window-
// frames op, or ok=false. Requires window func MAX, a ROWS frame with numeric bounds and
// EXCLUDE no-others, and an operand of the form CASE WHEN <pred> THEN <nonzero> ELSE ...
//
// Frame->match mapping: on the flag, ROWS BETWEEN B PRECEDING AND A FOLLOWING means a
// match at m makes rows [m-A, m+B] pass, i.e. A rows BEFORE + B rows AFTER the match. So
// beforeMatch = A (the FOLLOWING count = endNum), afterMatch = B (the PRECEDING count =
// -begNum).
func contextFromWindowFrames(wf *base.Op) (before, after int, pred expression.Expression, ok bool) {
	if len(wf.Params) < 5 {
		return 0, 0, nil, false
	}
	if fn, _ := wf.Params[3].(string); fn != "max" {
		return 0, 0, nil, false
	}

	// Frame cfg: Params[2] == [[type, begB, begN, endB, endN, exclude, valIdx]].
	cfgs, ok := wf.Params[2].([]interface{})
	if !ok || len(cfgs) != 1 {
		return 0, 0, nil, false
	}
	cfg, ok := cfgs[0].([]interface{})
	if !ok || len(cfg) < 7 {
		return 0, 0, nil, false
	}
	if t, _ := cfg[0].(string); t != "rows" {
		return 0, 0, nil, false
	}
	if ex, _ := cfg[5].(string); ex != "no-others" {
		return 0, 0, nil, false
	}
	begB, _ := cfg[1].(string)
	endB, _ := cfg[3].(string)
	if begB != "num" || endB != "num" {
		return 0, 0, nil, false // UNBOUNDED bounds are a whole-partition aggregate, not grep context.
	}
	begN, okA := asInt(cfg[2])
	endN, okB := asInt(cfg[4])
	if !okA || !okB {
		return 0, 0, nil, false
	}
	before = endN // FOLLOWING count
	after = -begN // PRECEDING count (begN is negative for "N PRECEDING")
	if before < 0 || after < 0 {
		return 0, 0, nil, false
	}

	// Operand: ["exprTree", CASE WHEN <pred> THEN ... ELSE ...]. The first child of a
	// SearchedCase is its first WHEN predicate (children = when1,then1,...,else).
	operand, ok := projTermExpr(wf.Params[4])
	if !ok {
		return 0, 0, nil, false
	}
	cse, ok := operand.(*expression.SearchedCase)
	if !ok {
		return 0, 0, nil, false
	}
	kids := cse.Children()
	if len(kids) < 2 {
		return 0, 0, nil, false
	}
	return before, after, kids[0], true
}

// contextSortExprs reads the order-offset-limit's sort columns, requiring all ASCending,
// and splits the first numPart as the partition column(s) (MVP: exactly one) from the
// remaining ORDER BY columns.
func contextSortExprs(ord *base.Op, numPart int) (partExpr expression.Expression, orderExprs []expression.Expression, ok bool) {
	if len(ord.Params) < 2 {
		return nil, nil, false
	}
	terms, ok := ord.Params[0].([]interface{})
	if !ok || len(terms) <= numPart {
		return nil, nil, false
	}
	dirs, ok := ord.Params[1].([]interface{})
	if !ok || len(dirs) != len(terms) {
		return nil, nil, false
	}
	for _, d := range dirs {
		if s, _ := d.(string); s != "asc" {
			return nil, nil, false // context/merge assume an ascending stream.
		}
	}
	exprs := make([]expression.Expression, 0, len(terms))
	for _, t := range terms {
		e, ok := projTermExpr(t)
		if !ok {
			return nil, nil, false
		}
		exprs = append(exprs, e)
	}
	return exprs[0], exprs[numPart:], true
}

// contextScan finds the detector's scan leaf and returns its labels, alias, and keyspace.
func contextScan(top *base.Op, temps []interface{}) (base.Labels, string, contextKeyspaceRef) {
	scan := contextFindScan(top)
	if scan == nil || len(scan.Labels) == 0 {
		return nil, "", nil
	}
	alias, ok := mergeLabelLeaf(scan.Labels[0])
	if !ok {
		return nil, "", nil
	}
	ks := branchScanKeyspace(top, temps)
	if ks == nil {
		return nil, "", nil
	}
	return scan.Labels, alias, ks
}

// contextKeyspaceRef is the subset of the keyspace interface we use (QualifiedName).
type contextKeyspaceRef interface{ QualifiedName() string }

// contextFindScan walks to the single datastore-scan-records leaf, or nil.
func contextFindScan(op *base.Op) *base.Op {
	if op == nil {
		return nil
	}
	if op.Kind == "datastore-scan-records" {
		return op
	}
	if len(op.Children) != 1 {
		return nil // a branch (join/union) is not the linear context shape.
	}
	return contextFindScan(op.Children[0])
}

// contextKeyspacer returns the scan op's keyspacer (the Temps entry the scan reads) so
// the shared scan can be rebuilt against it.
func contextKeyspacer(op *base.Op, temps []interface{}) interface{} {
	scan := contextFindScan(op)
	if scan == nil || len(scan.Params) == 0 {
		return nil
	}
	idx, ok := scan.Params[0].(int)
	if !ok || idx < 0 || idx >= len(temps) {
		return nil
	}
	return temps[idx]
}

// asInt reads an int/int64/float64 as an int.
func asInt(v interface{}) (int, bool) {
	switch n := v.(type) {
	case int:
		return n, true
	case int64:
		return int(n), true
	case float64:
		return int(n), true
	}
	return 0, false
}

// contextInt reads params[i] as an int (int/int64/float64), ok=false if absent/other.
func contextInt(params []interface{}, i int) (int, bool) {
	if i >= len(params) {
		return 0, false
	}
	return asInt(params[i])
}

// buildContextBroadcast builds the shared plan for one context group (all detectors with
// the same sig): a fresh scan -> order-offset-limit (sort by the SELF-rooted (partition,
// order) columns, ascending) -> a single engine broadcast-context fanning to K context
// extractors. Field refs are re-rooted to the shared "." row via renameAliasToSelf. The
// result (MVP) is the whole matched/context row.
func buildContextBroadcast(group []contextDetInfo, tags []string, unified *Conv) *base.Op {
	first := group[0]

	// Decide sort-elision on the PRISTINE alias-rooted exprs -- this must happen BEFORE
	// any renameAliasToSelf call below, which mutates an expr in place (replacing its root
	// identifier with SELF), after which isDotMetaField's root-Identifier check would fail.
	// See SORT ELISION below.
	elide := isDotMetaField(first.partExpr, "path") &&
		len(first.orderExprs) == 1 && isDotMetaField(first.orderExprs[0], "pos")

	// Shared scan into the unified Temps, labeled "." + "^id" (the whole-row convention).
	scan := &base.Op{
		Kind:   "datastore-scan-records",
		Labels: base.Labels{".", "^id"},
		Params: []interface{}{unified.AddTemp(first.keyspacer)},
	}

	// The shared (partition, order) sort, self-rooted so it resolves against ".".
	var terms []interface{}
	var dirs []interface{}
	selfExpr := func(e expression.Expression, alias string) []interface{} {
		return []interface{}{"exprTree", renameAliasToSelf(e, alias)}
	}
	terms = append(terms, selfExpr(first.partExpr, first.alias))
	dirs = append(dirs, "asc")
	for _, oe := range first.orderExprs {
		terms = append(terms, selfExpr(oe, first.alias))
		dirs = append(dirs, "asc")
	}
	// SORT ELISION: the file scan already yields records grouped per file (each file's
	// records contiguous -- filepath.Walk + sort.Strings, one file fully before the next)
	// and, within a file, in ascending _meta.pos (the record's in-file ordinal). So for a
	// group partitioned by _meta.path and ordered by _meta.pos the raw scan IS already in
	// (partition, order) form: the context op needs only per-partition contiguity + in-
	// partition order (findings are an unordered set, so the order of partitions is
	// irrelevant), both of which hold by construction. Drop the O(N log N) sort + full
	// buffer for an O(N) streaming pass. Any other (partition, order) keeps the explicit
	// sort. (Guarded narrowly to these exact _meta keys, whose order the file datastore
	// guarantees; a general "source advertises its order" contract is future work.)
	child := (*base.Op)(nil)
	if elide {
		child = scan
	} else {
		child = &base.Op{
			Kind:     "order-offset-limit",
			Labels:   base.Labels{".", "^id"},
			Params:   []interface{}{terms, dirs},
			Children: []*base.Op{scan},
		}
	}

	// K context extractors. The match predicate is lowered to its NATIVE tree where
	// possible (contextPredTree), so the engine op's Aho-Corasick index can extract a
	// necessary literal and skip the predicate eval on rows that lack it (sparse-match).
	// Result is each detector's own SELECT projection (IDEA-0025), shaped over the
	// shared "." scan row by recognizeContextDetector; a detector with no captured
	// projection (e.g. a directly-constructed contextDetInfo) falls back to the whole row.
	wholeRow := []interface{}{[]interface{}{"labelPath", "."}}
	exts := make([]interface{}, 0, len(group))
	for i, d := range group {
		proj := d.proj
		if proj == nil {
			proj = wholeRow
		}
		exts = append(exts, []interface{}{
			tags[i],
			d.beforeMatch,
			d.afterMatch,
			contextPredTree(d.matchPred, d.scanLabels, d.alias),
			proj,
		})
	}

	return &base.Op{
		Kind:   "broadcast-context",
		Labels: corpusFindingsLabels(),
		Params: []interface{}{
			exts,
			selfExpr(first.partExpr, first.alias), // partition key for boundary reset
		},
		Children: []*base.Op{child},
	}
}

// contextFusedProjection composes a context detector's outer SELECT (which references the
// OUTERMOST derived-table alias, e.g. `g.pos`) into an result projection over the shared
// "." scan row (IDEA-0025): each `<derivedAlias>.<col>` is resolved to its scan-rooted
// expression by derivedColumnResolver, then the rewritten terms are handed to the plain-
// path corpusFusedProjection, which re-roots the scan alias to SELF and builds the star /
// RAW / OBJECT_CONSTRUCT result exactly as the broadcast path does.
//
// Returns (nil, true) when there is no outer projection to shape (whole-row fallback), and
// (nil, false) when the projection can't be faithfully reproduced -- an outer term still
// references the derived alias after substitution (it selects the window flag, or a column
// the wrappers don't carry through), or corpusFusedProjection rejects the shape -- so the
// detector runs STANDALONE, where the full pipeline honors its SELECT.
func contextFusedProjection(outer, inner *base.Op, derivedAlias, scanAlias string) ([]interface{}, bool) {
	if outer == nil || outer.Kind != "project" || len(outer.Labels) == 0 {
		return nil, true // no captured SELECT -> whole-row result (prior behavior).
	}

	// A lone star SELECT * over the DERIVED table is standalone `{<derivedAlias>:{...,
	// near}}` -- the derived row (including the window flag), NOT the scan row -- so the
	// fused whole-scan-row envelope can't reproduce it. Route to standalone rather than
	// emit a shape that disagrees with the detector's own SQL (the IDEA-0025 contract).
	// (This differs from the plain broadcast path, where SELECT * IS the scanned row.)
	if len(outer.Labels) == 1 && outer.Labels[0] == ".*" {
		return nil, false
	}

	resolve := derivedColumnResolver(inner, scanAlias)
	rewritten := make([]interface{}, len(outer.Params))
	for i, p := range outer.Params {
		e, ok := projTermExpr(p)
		if !ok {
			rewritten[i] = p // a star's Self term (or non-expr): corpusFusedProjection handles it.
			continue
		}
		sub := substituteDerivedFields(e, derivedAlias, resolve)
		if referencesIdentifier(sub, derivedAlias) {
			return nil, false // unresolved `<derivedAlias>.X` -> can't reproduce -> standalone.
		}
		rewritten[i] = []interface{}{"exprTree", sub}
	}

	synthetic := &base.Op{Kind: "project", Labels: outer.Labels, Params: rewritten}
	return corpusFusedProjection(synthetic, scanAlias)
}

// derivedColumnResolver returns the one function that maps a derived column name to its
// scan-rooted expression -- the single seam between the detector's derived-view names and
// the scan row. It reads the window-column project (`inner`, whose terms are rooted at the
// scan alias): an explicit `<expr> AS <col>` term resolves to that expr; a `<scan>.*` star
// (or whole-row Self) resolves any OTHER name to the identity scan column `<scanAlias>.<col>`
// (the @grep_context macro's `SELECT src.*, MAX(...) AS hit`, IDEA-0029). The window
// aggregate (`near`/`hit`) and any name the star doesn't carry resolve to ok=false -- not
// reproducible per scan row -- so the caller bails to standalone. Star-passthrough wrappers
// ABOVE this project preserve names 1:1 (recognizeContextDetector rejects a renaming one),
// so resolving straight against `inner` is faithful.
func derivedColumnResolver(inner *base.Op, scanAlias string) func(string) (expression.Expression, bool) {
	m := map[string]expression.Expression{}
	aggCols := map[string]bool{}
	hasStar := false
	if inner != nil {
		for i := range inner.Labels {
			if i >= len(inner.Params) {
				break
			}
			if inner.Labels[i] == ".*" { // a star-spread column (`SELECT src.*`).
				hasStar = true
				continue
			}
			e, ok := projTermExpr(inner.Params[i])
			if !ok {
				continue
			}
			if _, isSelf := e.(*expression.Self); isSelf { // whole-row self.
				hasStar = true
				continue
			}
			name, ok := mergeLabelLeaf(inner.Labels[i])
			if !ok {
				continue
			}
			if _, isAgg := e.(algebra.Aggregate); isAgg {
				aggCols[name] = true // the window flag -- NOT reproducible per scan row.
				continue
			}
			m[name] = e
		}
	}
	return func(col string) (expression.Expression, bool) {
		if e, has := m[col]; has {
			return e, true // an explicit derived term (rooted at the scan alias).
		}
		if hasStar && scanAlias != "" && !aggCols[col] {
			// carried through `<scanAlias>.*` -> the identity scan column, same rooting as
			// m's terms (corpusFusedProjection re-roots the scan alias -> SELF).
			return expression.NewField(expression.NewIdentifier(scanAlias),
				expression.NewFieldName(col, false)), true
		}
		return nil, false // window flag / unknown -> the caller bails to standalone.
	}
}

// substituteDerivedFields returns a copy of expr with every `<alias>.<col>` field access
// replaced by resolve(col), the derived table's own scan-rooted term for that column. A
// column resolve can't reproduce (ok=false) is left as `<alias>.<col>`, so the caller
// detects it (referencesIdentifier) and routes the detector to standalone.
func substituteDerivedFields(expr expression.Expression, alias string,
	resolve func(string) (expression.Expression, bool)) expression.Expression {
	if expr == nil || alias == "" {
		return expr
	}
	s := &derivedFieldSubst{}
	s.SetMapper(s)
	s.SetMapFunc(func(e expression.Expression) (expression.Expression, error) {
		if f, ok := e.(*expression.Field); ok {
			ops := f.Operands()
			if len(ops) == 2 {
				if id, ok := ops[0].(*expression.Identifier); ok && id.Identifier() == alias {
					if repl, ok := resolve(fieldNameOf(ops[1])); ok {
						return repl, nil // substitute (don't recurse into the replacement).
					}
				}
			}
		}
		return e, e.MapChildren(s)
	})
	out, err := s.Map(expr)
	if err != nil {
		return expr
	}
	return out
}

type derivedFieldSubst struct {
	expression.MapperBase
}

// isStarPassthrough reports whether op is a `SELECT <alias>.*` project -- one term that
// forwards its child's rows verbatim (column names preserved), encoded either as a whole-
// row expression.Self (label `.["alias"]`) or as a star-spread (label ".*"). Such a
// middle wrapper composes transparently in the fused projection; anything else (a rename
// or computed column) does not, and the caller bails to standalone.
func isStarPassthrough(op *base.Op) bool {
	if op == nil || op.Kind != "project" || len(op.Params) != 1 {
		return false
	}
	if len(op.Labels) == 1 && op.Labels[0] == ".*" {
		return true
	}
	e, ok := projTermExpr(op.Params[0])
	if !ok {
		return false
	}
	_, isSelf := e.(*expression.Self)
	return isSelf
}

// outerRefAlias returns the single table alias a context detector's outer SELECT
// references in its field terms (e.g. "g" in SELECT g.file, g.pos). The @grep_context
// macro expands to TWO derived-table wrappers, so this outermost alias (g) differs from
// the innermost filter's alias (sub) -- and it, not filter.Labels, is what the fused
// projection must substitute (IDEA-0029). Returns "" when the terms reference no single
// alias (a bare star, or mixed aliases), leaving the caller's filter.Labels fallback in
// charge (the single-nested case, where the two aliases coincide).
func outerRefAlias(outer *base.Op) string {
	if outer == nil {
		return ""
	}
	seen := map[string]bool{}
	for _, p := range outer.Params {
		e, ok := projTermExpr(p)
		if !ok {
			continue
		}
		r := &identRefFinder{}
		r.SetMapper(r)
		r.SetMapFunc(func(x expression.Expression) (expression.Expression, error) {
			if f, ok := x.(*expression.Field); ok {
				ops := f.Operands()
				if len(ops) == 2 {
					if id, ok := ops[0].(*expression.Identifier); ok {
						seen[id.Identifier()] = true
					}
				}
			}
			return x, x.MapChildren(r)
		})
		_, _ = r.Map(e)
	}
	if len(seen) == 1 {
		for k := range seen {
			return k
		}
	}
	return ""
}

// referencesIdentifier reports whether expr contains an identifier named alias.
func referencesIdentifier(expr expression.Expression, alias string) bool {
	if expr == nil || alias == "" {
		return false
	}
	found := false
	r := &identRefFinder{}
	r.SetMapper(r)
	r.SetMapFunc(func(e expression.Expression) (expression.Expression, error) {
		if id, ok := e.(*expression.Identifier); ok && id.Identifier() == alias {
			found = true
		}
		return e, e.MapChildren(r)
	})
	_, _ = r.Map(expr)
	return found
}

type identRefFinder struct {
	expression.MapperBase
}

// contextPredTree lowers a context detector's match predicate to a native expr-tree
// rooted at the shared "." row (so the engine op's Aho-Corasick index can extract a
// necessary literal and prune), falling back to a boxed ["exprTree", ...] (always-wake)
// when it doesn't lower natively. Mirrors normalizeCorpusPred's native/boxed split.
func contextPredTree(pred expression.Expression, scanLabels base.Labels, alias string) []interface{} {
	aliasLabel := "." + LabelSuffix(alias)
	var buf bytes.Buffer
	if out, ok := ExprTreeOptimize(scanLabels, pred, &buf, false); ok {
		if rooted, ok := rewriteLabelRoot(out, aliasLabel, ".").([]interface{}); ok {
			return rooted
		}
	}
	return []interface{}{"exprTree", renameAliasToSelf(pred, alias)}
}

// isDotMetaField reports whether e is exactly `<ident>._meta.<leaf>` (e.g.
// alias._meta.path / alias._meta.pos) -- the file-metadata columns whose scan order the
// file datastore guarantees, enabling sort elision.
func isDotMetaField(e expression.Expression, leaf string) bool {
	outer, ok := e.(*expression.Field)
	if !ok {
		return false
	}
	oo := outer.Operands()
	if len(oo) != 2 || fieldNameOf(oo[1]) != leaf {
		return false
	}
	inner, ok := oo[0].(*expression.Field)
	if !ok {
		return false
	}
	io := inner.Operands()
	if len(io) != 2 || fieldNameOf(io[1]) != "_meta" {
		return false
	}
	_, ok = io[0].(*expression.Identifier)
	return ok
}

// fieldNameOf returns the field-name string of a Field's name operand, handling both the
// FieldName and string-Constant forms (as splitFieldRef does), else "".
func fieldNameOf(op expression.Expression) string {
	switch n := op.(type) {
	case *expression.FieldName:
		return n.Alias()
	case *expression.Constant:
		if s, ok := n.Value().Actual().(string); ok {
			return s
		}
	}
	return ""
}
