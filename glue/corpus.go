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

// corpus.go is the PREPARE++ corpus compiler (DESIGN-prepare.md phase 6): it
// turns a repo of stock SQL++ "detector" queries into ONE fused multi-query-
// optimization (MQO) plan over shared scans. It is the glue-level FEEDER for the
// engine's MQO substrate (engine/op_broadcast*.go): where those hand-built
// helpers (BroadcastCSE / BroadcastIndexed / BroadcastRoute) consume already-
// bound engine.Detector structs, CorpusCompile derives those structs from real
// SQL++ -- parse -> plan -> convert -> recognize -> normalize -> group -> compose.
//
// A "detector" is a stock SELECT over a single keyspace: `SELECT ... FROM <ks>
// <alias> [WHERE <pred>]`. Its ESSENCE is the PREDICATE; a "finding" is the
// matching evidence row, tagged with the detector's id. Fusing K detectors on one
// keyspace means: scan the keyspace ONCE, and per row evaluate the K predicates
// (with corpus-CSE hoisting shared sub-terms and an Aho-Corasick predicate index
// waking only detectors whose necessary literal is present) instead of K separate
// scan+decode passes.
//
// THE PIPELINE (CorpusCompile):
//
//  1. RECOGNIZE. Each detector is parsed/planned/converted (the normal glue path)
//     and matched against the canonical fusable shape
//     project(filter(datastore-scan-records)) -- or a bare
//     project(datastore-scan-records) for a no-WHERE detector (predicate =
//     always-true). Anything else (joins, group/order/distinct, subqueries,
//     multiple sources, or a non-records leaf such as an index scan) is appended to
//     Unfused with a reason and skipped -- surfaced, never silently dropped.
//
//  2. EXTRACT. Per fused detector: its keyspace (branchScanKeyspace), its predicate
//     (the filter's expr, or always-true), and its alias (mergeLabelLeaf of the
//     scan label).
//
//  3. NORMALIZE (the core challenge -- alias unification). To share ONE scan across
//     detectors on the same keyspace, every detector's predicate must resolve
//     against a single canonical row. The shared scan labels its row under "." (the
//     whole-row convention the broadcast/CSE machinery expects; see
//     engine/op_broadcast_cse.go). So each detector's predicate is rewritten to be
//     rooted at "." instead of its own alias:
//       - Native path: the predicate is lowered to the native expr-tree
//         (ExprTreeOptimize) whose field refs are ["labelPath", `.["<alias>"]`,
//         ...]; rewriteLabelRoot swaps the `.["<alias>"]` root for ".". A native
//         predicate additionally exposes a required literal to the Aho-Corasick
//         index (extra sparsity).
//       - Boxed fallback: a predicate that does not lower natively stays a boxed
//         ["exprTree", <cbq-expr>] (MakeExprFunc/ExprTree still evaluate it -- just
//         no extractable literal, so the index marks it always-wake). Its cbq
//         identifier <alias> is remapped to SELF (renameAliasToSelf) so it resolves
//         against the "."-labeled shared row. Boxed predicates are thus fully
//         supported; they simply don't get indexed.
//
//  4. FINDINGS. MVP evidence = the WHOLE matched row (a uniform findings schema
//     across all detectors, so union-all funnels cleanly). So each detector's Proj
//     is a single whole-row passthrough ["labelPath","."] and FindingsLabels =
//     [.["tag"], .["evidence"]]. Per-detector SELECT projection into a uniform
//     envelope object is a deliberate FUTURE refinement (the detector's SELECT list
//     is intentionally ignored here) -- the predicate is what makes a detector.
//
//  5. COMPOSE. Fused detectors are grouped by keyspace (routing == per-keyspace
//     grouping). Per keyspace: BroadcastCSE hoists shared sub-predicates over the
//     shared scan, and the result is turned into a broadcast-INDEXED fan-out (CSE
//     precompute + Aho-Corasick predicate index) by re-kinding the CSE broadcast
//     op -- its child/Params layout is byte-identical to what BroadcastIndexed
//     builds. The per-keyspace plans combine under a single union-all.
//
//  6. RUN. CompiledCorpus.Run mirrors Session.PlanExec's vars/GlueContext/ExecOpEx
//     setup and drives engine.ExecOp, collecting the tagged findings.
//
// DELIBERATELY DEFERRED (noted, not built): a SHA-keyed / content-addressed build
// cache; the embed-source analyzer binary; per-detector SELECT projection into an
// envelope (MVP = whole-row evidence); a logical-keyspace vocabulary + late-binding
// manifest (detectors FROM the real keyspace name for now); and recipe metadata
// (severity / ticket beyond the Tag).
//
// RECOGNIZER NARROWNESS (known): the fusable shape is exactly
// project([filter,]datastore-scan-records). A detector that the planner answers
// with an INDEX scan (a secondary index exists and is sargable) converts to a
// datastore-scan-index leaf, not datastore-scan-records, and is reported Unfused --
// even though it is semantically a single-keyspace filter. Fusing indexed leaves
// (and per-detector projection, and META().id predicates under CSE, whose ^id slot
// the CSE precompute currently drops) are future refinements.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/errors"
	"github.com/couchbase/query/expression"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/engine"
)

// CorpusDetector is one SQL++ detector query plus its stable id (Tag). The Tag is
// emitted in slot 0 of every finding this detector produces, so a consumer can
// demultiplex the interleaved findings stream.
type CorpusDetector struct {
	Tag  string
	Stmt string
}

// UnfusedDetector reports a detector that could NOT be folded into the shared-scan
// plan, with a human-readable Reason. Surfaced (never silently dropped) so a
// caller can decide whether an unfusable detector is a hard error or a fall-back-
// to-standalone-run.
type UnfusedDetector struct {
	Tag    string
	Reason string
}

// Finding is one tagged evidence row produced by running the compiled corpus:
// the detector's Tag plus the whole matched row as canonical JSON (the MVP
// whole-row evidence -- see the file header).
type Finding struct {
	Tag      string
	Evidence json.RawMessage
}

// CompiledCorpus is the output of CorpusCompile: the fused MQO plan (Plan) plus the
// Temps it resolves its shared-scan keyspaces from, the detectors that could not be
// fused (Unfused), the uniform findings schema (FindingsLabels), and enough of the
// originating session to Run it. Plan is nil when no detector fused (empty corpus,
// or all unfusable) -- an honestly empty plan, not one that "reads as clean".
type CompiledCorpus struct {
	Plan           *base.Op
	Temps          []interface{}
	Unfused        []UnfusedDetector
	FindingsLabels base.Labels

	session *Session
}

// corpusFindingsLabels is the uniform two-column findings schema every per-keyspace
// broadcast (and the union-all) shares: slot 0 the detector tag, slot 1 the whole
// evidence row.
func corpusFindingsLabels() base.Labels {
	return base.Labels{"." + LabelSuffix("tag"), "." + LabelSuffix("evidence")}
}

// CorpusCompile turns a set of stock SQL++ detectors into ONE fused shared-scan
// plan (see the file header for the full pipeline). It never returns a hard error
// for an individual unfusable detector -- those land in the returned
// CompiledCorpus.Unfused; err is non-nil only for a setup-level failure.
func (s *Session) CorpusCompile(dets []CorpusDetector) (*CompiledCorpus, error) {
	// Correlated/keyspace resolution in the planner reads the process-global
	// datastore; point it at this session's store (idempotent), exactly as
	// Session.Run does before planning.
	if s.Store != nil && s.Store.Datastore != nil {
		datastore.SetDatastore(s.Store.Datastore)
	}

	findingsLabels := corpusFindingsLabels()

	// One unified Conv/Temps for the WHOLE fused plan: the per-detector Convs' Temps
	// indices belong to their own conversions, so we CANNOT reuse their scan ops.
	// Instead we register one fresh shared scan per keyspace into this unified Conv
	// (its keyspacer plan-op borrowed from a detector's Temps -- only Keyspace() is
	// read off it at scan time).
	unified := &Conv{Temps: []interface{}{nil}}

	type fusedDet struct {
		tag  string
		pred []interface{} // normalized: field refs rooted at "."
	}

	byKeyspace := map[string][]fusedDet{}
	keyspacerFor := map[string]interface{}{}
	var unfused []UnfusedDetector

	for _, d := range dets {
		info, reason := s.analyzeCorpusDetector(d.Stmt)
		if reason != "" {
			unfused = append(unfused, UnfusedDetector{Tag: d.Tag, Reason: reason})
			continue
		}
		if _, seen := keyspacerFor[info.keyspaceName]; !seen {
			keyspacerFor[info.keyspaceName] = info.keyspacer
		}
		byKeyspace[info.keyspaceName] = append(byKeyspace[info.keyspaceName],
			fusedDet{tag: d.Tag, pred: info.pred})
	}

	// Deterministic keyspace order so the emitted plan is stable regardless of map
	// iteration order.
	ksNames := make([]string, 0, len(byKeyspace))
	for ks := range byKeyspace {
		ksNames = append(ksNames, ks)
	}
	sort.Strings(ksNames)

	var perKeyspace []*base.Op
	for _, ks := range ksNames {
		// The shared scan: one datastore-scan-records over the keyspace, labeling its
		// row under "." (the whole-row convention the broadcast/CSE machinery resolves
		// against) plus "^id". Built fresh into the unified Temps -- NOT reused from a
		// detector's Conv.
		scan := &base.Op{
			Kind:   "datastore-scan-records",
			Labels: base.Labels{".", "^id"},
			Params: []interface{}{unified.AddTemp(keyspacerFor[ks])},
		}

		dets := byKeyspace[ks]
		edets := make([]engine.Detector, 0, len(dets))
		for _, fd := range dets {
			edets = append(edets, engine.Detector{
				Tag:  fd.tag,
				Pred: fd.pred,
				// MVP evidence: the whole matched row (uniform across detectors).
				Proj: []interface{}{[]interface{}{"labelPath", "."}},
			})
		}

		// CSE-hoist shared sub-predicates over the shared scan, then turn the CSE
		// broadcast into an INDEXED fan-out: BroadcastCSE's returned op (a "broadcast"
		// over the CSE precompute project, or over the bare scan when nothing is
		// shared) has the EXACT child/Params layout OpBroadcastIndexed expects, so
		// re-kinding it composes "CSE precompute + Aho-Corasick predicate index" with
		// no new engine op. See broadcastCSEIndexed.
		perKeyspace = append(perKeyspace, broadcastCSEIndexed(scan, edets, findingsLabels))
	}

	var planOp *base.Op
	switch len(perKeyspace) {
	case 0:
		planOp = nil // empty corpus or all unfusable.
	case 1:
		planOp = perKeyspace[0] // a single keyspace needs no union-all wrapper.
	default:
		planOp = &base.Op{
			Kind:     "union-all",
			Labels:   append(base.Labels(nil), findingsLabels...),
			Children: perKeyspace,
		}
	}

	return &CompiledCorpus{
		Plan:           planOp,
		Temps:          unified.Temps,
		Unfused:        unfused,
		FindingsLabels: findingsLabels,
		session:        s,
	}, nil
}

// corpusDetInfo is the extracted, normalized description of one fusable detector.
type corpusDetInfo struct {
	keyspaceName string        // keyspace.QualifiedName() -- the grouping key.
	keyspacer    interface{}   // the plan scan op (a keyspacer) for the shared scan.
	pred         []interface{} // predicate expr-tree, field refs rooted at ".".
}

// analyzeCorpusDetector parses/plans/converts one detector's SQL++ and, if it is
// the canonical fusable shape, extracts its keyspace + normalized predicate. A
// non-empty reason means the detector is unfusable (the caller records it in
// Unfused); info is then zero.
func (s *Session) analyzeCorpusDetector(stmt string) (info corpusDetInfo, reason string) {
	parsed, err := ParseStatement(stmt, s.Namespace, true)
	if err != nil {
		return corpusDetInfo{}, "parse error: " + err.Error()
	}
	qp, err := s.Store.PlanStatementQP(parsed, s.Namespace, nil, nil)
	if err != nil {
		return corpusDetInfo{}, "plan error: " + err.Error()
	}

	// Convert with a per-detector Conv (a plain Accept -- no post-plan rewrites; we
	// only need the raw project/filter/scan shape). A convert panic (an unsupported
	// op) is caught and reported as unfusable, never crashing the compile.
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
	if convErr != nil {
		return corpusDetInfo{}, "convert error: " + convErr.Error()
	}
	if conv.TopOp == nil {
		return corpusDetInfo{}, "unconverted plan (nil TopOp)"
	}

	scan, filter, ok := recognizeCorpusDetector(conv.TopOp)
	if !ok {
		return corpusDetInfo{}, "not a fusable project([filter,]datastore-scan-records) shape"
	}

	// The alias the detector's field refs are rooted at (from the scan's `.["alias"]`
	// row label).
	if len(scan.Labels) == 0 {
		return corpusDetInfo{}, "scan has no row label"
	}
	alias, ok := mergeLabelLeaf(scan.Labels[0])
	if !ok {
		return corpusDetInfo{}, "scan alias not resolvable from label " + scan.Labels[0]
	}
	aliasLabel := "." + LabelSuffix(alias)

	ks := branchScanKeyspace(conv.TopOp, conv.Temps)
	if ks == nil {
		return corpusDetInfo{}, "keyspace not resolvable"
	}

	scanTempIdx, ok := scan.Params[0].(int)
	if !ok || scanTempIdx < 0 || scanTempIdx >= len(conv.Temps) {
		return corpusDetInfo{}, "scan temp index not resolvable"
	}

	pred := normalizeCorpusPred(filter, scan.Labels, alias, aliasLabel)

	return corpusDetInfo{
		keyspaceName: ks.QualifiedName(),
		keyspacer:    conv.Temps[scanTempIdx],
		pred:         pred,
	}, ""
}

// recognizeCorpusDetector matches the canonical fusable detector shape and returns
// its scan leaf + (optional) filter. Accepts:
//
//	project -> filter -> datastore-scan-records     (SELECT ... WHERE ...)
//	project -> datastore-scan-records               (SELECT ... with no WHERE)
//
// Everything else (a join/group/order/distinct/subquery op in the chain, multiple
// sources, or a non-records leaf) returns ok=false. The projection is intentionally
// ignored (MVP whole-row evidence), so only the shape below the project matters.
func recognizeCorpusDetector(top *base.Op) (scan, filter *base.Op, ok bool) {
	if top == nil || top.Kind != "project" || len(top.Children) != 1 {
		return nil, nil, false
	}
	child := top.Children[0]
	if child.Kind == "filter" {
		if len(child.Children) != 1 {
			return nil, nil, false
		}
		filter = child
		scan = child.Children[0]
	} else {
		scan = child
	}
	if scan == nil || scan.Kind != "datastore-scan-records" || len(scan.Children) != 0 {
		return nil, nil, false
	}
	return scan, filter, true
}

// normalizeCorpusPred returns the detector's predicate as an expr-tree rooted at
// the shared "." row. With no filter the predicate is the always-true constant. The
// filter carries a boxed ["exprTree", <cbq-expr>] (VisitFilter's output); we prefer
// to LOWER it to a native tree (so field refs become ["labelPath", `.["alias"]`,
// ...] we can re-root to "." -- and so the Aho-Corasick index can extract a
// required literal), and fall back to keeping it boxed with the alias remapped to
// SELF when it does not lower.
func normalizeCorpusPred(filter *base.Op, scanLabels base.Labels, alias, aliasLabel string) []interface{} {
	if filter == nil {
		// No WHERE: always-true (matches every scanned row).
		return []interface{}{"json", "true"}
	}

	expr, ok := corpusFilterExpr(filter)
	if !ok {
		// Unexpected (VisitFilter always boxes an ["exprTree", expr]); defensively
		// re-root whatever tree is in the filter's Params.
		if rooted, ok := rewriteLabelRoot(append([]interface{}(nil), filter.Params...),
			aliasLabel, ".").([]interface{}); ok {
			return rooted
		}
		return []interface{}{"json", "true"}
	}

	// Native path: lower against the detector's own scan labels (`.["alias"]`, ^id),
	// then re-root every `.["alias"]` labelPath to the shared "." row.
	var buf bytes.Buffer
	if out, ok := ExprTreeOptimize(scanLabels, expr, &buf, false); ok {
		if rooted, ok := rewriteLabelRoot(out, aliasLabel, ".").([]interface{}); ok {
			return rooted
		}
	}

	// Boxed fallback: keep the cbq expression, but remap its keyspace identifier to
	// SELF so it evaluates against the "."-labeled shared row (the alias no longer
	// exists as a field key there). Still fully evaluated by ExprTree; just not
	// indexed (no extractable literal), so the predicate index always-wakes it.
	return []interface{}{"exprTree", renameAliasToSelf(expr, alias)}
}

// corpusFilterExpr extracts the cbq expression from a filter op's boxed
// ["exprTree", <expr>] Params (VisitFilter's encoding).
func corpusFilterExpr(filter *base.Op) (expression.Expression, bool) {
	if filter == nil || len(filter.Params) < 2 {
		return nil, false
	}
	if head, _ := filter.Params[0].(string); head != "exprTree" {
		return nil, false
	}
	e, ok := filter.Params[1].(expression.Expression)
	return e, ok
}

// rewriteLabelRoot returns node with every ["labelPath", <from>, ...path] whose
// ROOT label element equals from re-rooted to `to`, preserving the trailing path
// elements. Non-matching labelPaths and all other tree nodes are structurally
// copied unchanged. Used to swap a detector's `.["alias"]` row root for the shared
// "." row root, so K detectors resolve against one scan. Returns the same dynamic
// type it was given (a []interface{} tree, or a passed-through leaf).
func rewriteLabelRoot(node interface{}, from, to string) interface{} {
	t, ok := node.([]interface{})
	if !ok {
		return node // leaf (string / number): unchanged.
	}
	if len(t) >= 2 {
		if head, _ := t[0].(string); head == "labelPath" {
			if root, _ := t[1].(string); root == from {
				out := make([]interface{}, len(t))
				copy(out, t) // path elements after [1] are plain field-name strings.
				out[1] = to  // re-root.
				return out
			}
		}
	}
	out := make([]interface{}, len(t))
	for i, e := range t {
		out[i] = rewriteLabelRoot(e, from, to)
	}
	return out
}

// renameAliasToSelf returns a copy of expr with every identifier named alias
// replaced by SELF -- so a field access `alias.f` becomes `self.f`, resolving
// against the current ("."-labeled) row instead of a now-absent `alias` field key.
// Mirrors the stripCovers / stripSearch Mapper idiom (expr.go). On any mapper error
// it returns expr unchanged.
func renameAliasToSelf(expr expression.Expression, alias string) expression.Expression {
	if expr == nil {
		return nil
	}
	ar := &aliasToSelf{alias: alias}
	ar.SetMapper(ar)
	ar.SetMapFunc(func(e expression.Expression) (expression.Expression, error) {
		if id, ok := e.(*expression.Identifier); ok && id.Identifier() == alias {
			return expression.NewSelf(), nil
		}
		return e, e.MapChildren(ar)
	})
	out, err := ar.Map(expr)
	if err != nil {
		return expr
	}
	return out
}

type aliasToSelf struct {
	expression.MapperBase
	alias string
}

// broadcastCSEIndexed composes corpus CSE with the predicate index over one shared
// scan: engine.BroadcastCSE builds a "broadcast" op over a CSE precompute project
// (or the bare scan when nothing is shared) whose child/Params layout is exactly
// what OpBroadcastIndexed consumes, so re-kinding it to "broadcast-indexed" yields
// "CSE precompute + Aho-Corasick predicate index" with no new engine op. See
// engine/op_broadcast_cse.go and engine/op_broadcast_indexed.go.
func broadcastCSEIndexed(scan *base.Op, dets []engine.Detector,
	findingsLabels base.Labels) *base.Op {
	op := engine.BroadcastCSE(scan, dets, findingsLabels)
	op.Kind = "broadcast-indexed"
	return op
}

// Run executes the compiled corpus plan and returns the tagged findings. It mirrors
// Session.PlanExec's vars / GlueContext / ExecOpEx setup, but reads each finding's
// [tag, evidence] straight from the yielded row slots (no ConvertVals). Findings
// order across the union-all / interleaved fan-out is NOT guaranteed (compare as a
// set). A nil Plan (no detector fused) yields no findings.
func (cc *CompiledCorpus) Run() ([]Finding, error) {
	if cc.Plan == nil {
		return nil, nil
	}

	s := cc.session
	if s.Store != nil && s.Store.Datastore != nil {
		datastore.SetDatastore(s.Store.Datastore)
	}

	// Wire the boxed expr lanes into the engine's expr catalog (as PlanExec does),
	// so a boxed ["exprTree", ...] predicate resolves.
	if engine.ExprCatalog["exprStr"] == nil {
		engine.ExprCatalog["exprStr"] = ExprStr
	}
	if engine.ExprCatalog["exprTree"] == nil {
		engine.ExprCatalog["exprTree"] = ExprTree
	}

	tmpDir, vars := MakeVars("", "n1k1corpus")
	defer os.RemoveAll(tmpDir)

	vars.Ctx.Pipe = s.Pipe

	gctx := NewGlueContext(time.Now())
	gctx.InitSubqueries(s.Store, s.Namespace, nil, nil) // no subqueries in fusable detectors
	vars.Ctx.Warn = func(w string) { gctx.Warning(errors.NewWarning(w)) }

	vars.Temps = vars.Temps[:0]
	vars.Temps = append(vars.Temps, gctx)
	vars.Temps = append(vars.Temps, cc.Temps[1:]...)
	for i := 0; i < 16; i++ {
		vars.Temps = append(vars.Temps, nil)
	}

	origExecOpEx := engine.ExecOpEx
	defer func() { engine.ExecOpEx = origExecOpEx }()
	engine.ExecOpEx = DatastoreOp

	var findings []Finding
	var execErr error

	yieldVals := func(vals base.Vals) {
		if len(vals) < 2 {
			return
		}
		// Slot 0 = the tag (a JSON-quoted string placed by the broadcast op); slot 1
		// = the whole evidence row (the doc's JSON bytes).
		var tag string
		if err := json.Unmarshal([]byte(vals[0]), &tag); err != nil {
			tag = string(vals[0]) // fall back to the raw bytes if not a JSON string.
		}
		findings = append(findings, Finding{
			Tag:      tag,
			Evidence: append(json.RawMessage(nil), vals[1]...),
		})
	}
	yieldErr := func(e error) {
		if e != nil && execErr == nil {
			execErr = e
		}
	}

	engine.ExecOp(cc.Plan, vars, yieldVals, yieldErr, "", "")

	if execErr != nil {
		return nil, execErr
	}
	return findings, nil
}
