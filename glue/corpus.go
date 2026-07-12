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
// THE PIPELINE (CorpusCompile) classifies every detector into one of THREE classes:
//
//   - FUSABLE  -- the canonical single-source shape below; folded into the shared-
//                 scan broadcast/CSE/index plan (Plan) and run by that plan.
//   - STANDALONE -- parse+plan+convert SUCCEEDED but the shape is not fusable
//                 (an ASOF/argmax correlated subquery, a window, GROUP BY, a join,
//                 a datastore-scan-index leaf, multiple sources, ...). These are
//                 VALID queries n1k1 runs end-to-end; CorpusCompile keeps the
//                 detector's Tag+Stmt and, at Run() time, executes each through the
//                 FULL normal pipeline (s.Run) -- so WireASOFJoin / window / group
//                 all fire and each detector is individually optimized -- then tags
//                 its result rows into the uniform Finding shape, UNION'd with the
//                 fused findings. (Standalone detectors do NOT share a scan among
//                 themselves -- each is an independent run; sharing standalone scans
//                 is a future step. Their evidence is the detector's SELECT
//                 projection -- as is the fused path's, so the two shapes agree.)
//   - REJECTED -- parse/plan/convert FAILED (a genuinely broken detector). Surfaced
//                 with a reason and NOT run; it never aborts the corpus.
//
//  1. RECOGNIZE. Each detector is parsed/planned/converted (the normal glue path)
//     and matched against the canonical fusable shape
//     project(filter(datastore-scan-records)) -- or a bare
//     project(datastore-scan-records) for a no-WHERE detector (predicate =
//     always-true). A detector whose parse/plan/convert FAILS is Rejected. A
//     detector that converts fine but is NOT the fusable shape (joins,
//     group/order/distinct, subqueries, multiple sources, or a non-records leaf such
//     as an index scan) is Standalone -- run individually at Run() time, never
//     silently dropped.
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
//  4. FINDINGS. A uniform findings schema across all detectors (FindingsLabels =
//     [.["tag"], .["evidence"]]), so union-all funnels cleanly: slot 0 the tag, slot
//     1 the single evidence value. Evidence is SHAPED to the detector's SELECT
//     projection (corpusFusedProjection) so a fused detector's evidence matches the
//     SAME SELECT run standalone: SELECT * keeps the whole-row passthrough
//     ["labelPath","."]; SELECT RAW yields the single value; SELECT a,b assembles a
//     boxed OBJECT_CONSTRUCT {"a":..,"b":..}. A projection that can't be faithfully
//     reproduced in that single-column envelope routes the detector to standalone.
//
//  5. COMPOSE. Fused detectors are grouped by keyspace (routing == per-keyspace
//     grouping). Per keyspace: BroadcastCSE hoists shared sub-predicates over the
//     shared scan, and the result is turned into a broadcast-INDEXED fan-out (CSE
//     precompute + Aho-Corasick predicate index) by re-kinding the CSE broadcast
//     op -- its child/Params layout is byte-identical to what BroadcastIndexed
//     builds. The per-keyspace plans combine under a single union-all.
//
//  6. RUN. CompiledCorpus.Run first runs each STANDALONE detector through the full
//     s.Run pipeline (so ASOF/window/group lowerings fire) and tags its rows, then
//     mirrors Session.PlanExec's vars/GlueContext/ExecOpEx setup and drives
//     engine.ExecOp over the fused Plan -- the UNION of both is the findings set.
//
// DELIBERATELY DEFERRED (noted, not built): a SHA-keyed / content-addressed build
// cache; the embed-source analyzer binary; a logical-keyspace vocabulary +
// late-binding manifest (detectors FROM the real keyspace name for now); and recipe
// metadata (severity / ticket beyond the Tag).
//
// RECOGNIZER NARROWNESS (known): the fusable shape is exactly
// project([filter,]datastore-scan-records). A detector that the planner answers
// with an INDEX scan (a secondary index exists and is sargable) converts to a
// datastore-scan-index leaf, not datastore-scan-records, and is classified Standalone
// (run individually) even though it is semantically a single-keyspace filter. It
// still produces its findings; it just does not share the fused scan. Fusing indexed
// leaves (and META().id predicates under CSE, whose ^id slot the CSE precompute
// currently drops) are future refinements.

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
	"github.com/couchbase/query/value"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/engine"
)

// CorpusDetector is one SQL++ detector query plus its stable id (Tag). The Tag is
// emitted in slot 0 of every finding this detector produces, so a consumer can
// demultiplex the interleaved findings stream.
type CorpusDetector struct {
	Tag  string
	Stmt string

	// Source + Gate drive index-gating of a STANDALONE detector (see CompiledCorpus.Run
	// / gateAllows). Source is the detector's logical keyspace; Gate is a cheap NECESSARY
	// precondition (boolean SQL++ over Source). When both are set and Source holds no row
	// satisfying Gate, the standalone detector is SKIPPED (its expensive sort/window never
	// runs). Empty Gate = never gated (always run). Populated from recipe front-matter by
	// Recipe.AsDetector; a hand-built CorpusDetector leaves them empty (ungated).
	Source string
	Gate   string
}

// RejectedDetector reports a detector whose parse/plan/convert FAILED (a genuinely
// broken query), with a human-readable Reason. Surfaced (never silently dropped) and
// NOT run. Distinct from a Standalone detector, which converts fine but is not the
// fusable shape -- that one still runs and produces findings.
type RejectedDetector struct {
	Tag    string
	Reason string
}

// Finding is one tagged evidence row produced by running the compiled corpus: the
// detector's Tag plus its evidence as canonical JSON. Evidence is the detector's
// SELECT projection whether it ran FUSED (shaped by corpusFusedProjection) or
// STANDALONE (whatever s.Run of its statement yields) -- the two paths agree on shape.
// Both are the same Finding{Tag, Evidence} envelope, so they union cleanly.
type Finding struct {
	Tag      string
	Evidence json.RawMessage
}

// CompiledCorpus is the output of CorpusCompile: the fused MQO plan (Plan) plus the
// Temps it resolves its shared-scan keyspaces from, the STANDALONE detectors (valid
// but non-fusable -- run individually at Run() time), the REJECTED detectors (parse/
// plan/convert failed -- surfaced, not run), the uniform findings schema
// (FindingsLabels), and enough of the originating session to Run it. Plan is nil when
// no detector fused (empty corpus, or all standalone/rejected) -- an honestly empty
// fused plan; Run() still produces the standalone detectors' findings.
type CompiledCorpus struct {
	Plan           *base.Op
	Temps          []interface{}
	Standalone     []CorpusDetector
	Rejected       []RejectedDetector
	FindingsLabels base.Labels

	// DetKeyspace maps a FUSED detector's Tag to the keyspace it scans (its qualified
	// name), so a run report can attribute per-keyspace scanned-row counts to each
	// detector (IDEA-0015 hit stats). Standalone/rejected detectors are absent.
	DetKeyspace map[string]string

	// wokenByTag holds one live *int64 per FUSED detector (by Tag), wired into the
	// broadcast op's engine.Detector.Woken so it counts the rows that woke each
	// detector; RunReport reads them back (IDEA-0015-followup). Standalone absent.
	wokenByTag map[string]*int64

	// GatedSkipped lists the Tags of STANDALONE detectors that Run skipped because their
	// `gate:` precondition matched no row in their Source keyspace (index-gating). Reset
	// and populated on each Run; surfaced by the caller so a skip is visible, not silent.
	GatedSkipped []string

	// CorrelationGroups maps a temporal-correlation signature (left keyspace, right
	// keyspace, key, direction) to the Tags of the correlation detectors that share it
	// (DESIGN-sorting.md Part B). A group of >1 could share ONE sorted scan of each
	// keyspace; today each still runs standalone -- this surfaces the opportunity.
	CorrelationGroups map[string][]string

	session   *Session
	scanCache *corpusScanCache // the last run's shared-scan cache (test observability), or nil

	// MergeStats holds the last run's sorted-merge counters (merge/spill/skip/stream),
	// race-safe across the streaming merge's actor goroutines. Set per run; nil before.
	MergeStats *base.MergeStats
}

// CorpusRunReport accompanies a RunReport run with the diagnostics an author needs to
// debug a 0-findings detector (IDEA-0015): how many rows each fused keyspace scan
// fanned (ScannedByKeyspace, the shared-scan RowsIn) alongside the per-detector match
// count the caller tallies from the findings. It distinguishes "the keyspace scanned
// ~0 rows" (a whole-file blob / empty scan -- the IDEA-0001 trap) from "the predicate
// matched nothing of N scanned".
type CorpusRunReport struct {
	// ScannedByKeyspace maps a keyspace's qualified name to the rows its fused shared
	// scan fanned in (== the broadcast-indexed op's RowsIn). Fused keyspaces only.
	ScannedByKeyspace map[string]int64

	// WokenByDetector maps a FUSED detector's Tag to how many rows woke it (its
	// predicate was evaluated) -- the predicate index's effect per detector
	// (IDEA-0015-followup): woken<<scanned means the literal is rare/absent, woken
	// with 0 matched means the predicate ran but never held. Fused detectors only.
	WokenByDetector map[string]int64
}

// corpusFindingsLabels is the uniform two-column findings schema every per-keyspace
// broadcast (and the union-all) shares: slot 0 the detector tag, slot 1 the whole
// evidence row.
func corpusFindingsLabels() base.Labels {
	return base.Labels{"." + LabelSuffix("tag"), "." + LabelSuffix("evidence")}
}

// CorpusCompile turns a set of stock SQL++ detectors into ONE fused shared-scan
// plan (see the file header for the full pipeline) plus a list of Standalone
// detectors (valid but non-fusable, run individually at Run() time) and Rejected
// detectors (parse/plan/convert failed). It never returns a hard error for an
// individual detector -- those are classified into the returned CompiledCorpus; err
// is non-nil only for a setup-level failure.
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
		proj []interface{} // evidence projection, field refs rooted at "."
	}

	byKeyspace := map[string][]fusedDet{}
	keyspacerFor := map[string]interface{}{}
	detKeyspace := map[string]string{}
	wokenByTag := map[string]*int64{}
	var standalone []CorpusDetector
	var rejected []RejectedDetector
	var correlationGroups map[string][]string // Part B: correlation sig -> tags (foundation)

	// Context detectors (grep -A/-B/-C windowed match-flag idiom) grouped by their
	// (keyspace, partition, order) signature -- each group shares one scan + sort + a
	// broadcast-context fan-out (corpus_context.go).
	contextGroups := map[string][]contextGroupEntry{}
	var contextSigOrder []string

	for _, d := range dets {
		// Try the context idiom first: a recognized context detector joins its shared-sort
		// group instead of running standalone.
		if ci, ok := s.analyzeContextDetector(d.Stmt); ok {
			if _, seen := contextGroups[ci.sig]; !seen {
				contextSigOrder = append(contextSigOrder, ci.sig)
			}
			contextGroups[ci.sig] = append(contextGroups[ci.sig], contextGroupEntry{tag: d.Tag, info: ci})
			detKeyspace[d.Tag] = ci.keyspaceName
			continue
		}

		info, fusable, rejectReason := s.analyzeCorpusDetector(d.Stmt)
		switch {
		case rejectReason != "":
			// Parse/plan/convert failed -- a broken detector. Surfaced, not run.
			rejected = append(rejected, RejectedDetector{Tag: d.Tag, Reason: rejectReason})
		case !fusable:
			// Valid but not the fusable shape (ASOF/window/group/join/index-scan/...).
			// Keep it verbatim (incl. its Source/Gate, so Run can index-gate it); Run()
			// executes it through the full s.Run pipeline.
			standalone = append(standalone, CorpusDetector{
				Tag: d.Tag, Stmt: d.Stmt, Source: d.Source, Gate: d.Gate})
			// A temporal-correlation detector additionally records its shared-scan
			// signature (Part B foundation) -- still standalone for now.
			if sig, isCorr := s.analyzeCorrelationDetector(d.Stmt); isCorr {
				if correlationGroups == nil {
					correlationGroups = map[string][]string{}
				}
				correlationGroups[sig] = append(correlationGroups[sig], d.Tag)
			}
		default:
			if _, seen := keyspacerFor[info.keyspaceName]; !seen {
				keyspacerFor[info.keyspaceName] = info.keyspacer
			}
			byKeyspace[info.keyspaceName] = append(byKeyspace[info.keyspaceName],
				fusedDet{tag: d.Tag, pred: info.pred, proj: info.proj})
			detKeyspace[d.Tag] = info.keyspaceName
		}
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
			// A live per-detector woken counter, wired into the broadcast op and read
			// back by RunReport (IDEA-0015-followup).
			w := new(int64)
			wokenByTag[fd.tag] = w
			edets = append(edets, engine.Detector{
				Tag:  fd.tag,
				Pred: fd.pred,
				// Evidence shaped to the detector's SELECT projection (IDEA-0004):
				// whole row for SELECT *, else the RAW value / assembled object.
				Proj:  fd.proj,
				Woken: w,
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

	// Context groups: one shared scan + sort + broadcast-context per (keyspace, P, O)
	// signature (in recognition order for a stable plan). Findings share the same
	// [tag, evidence] schema, so they union with the fused broadcasts.
	for _, sig := range contextSigOrder {
		entries := contextGroups[sig]
		group := make([]contextDetInfo, len(entries))
		tags := make([]string, len(entries))
		for i, e := range entries {
			group[i] = e.info
			tags[i] = e.tag
		}
		perKeyspace = append(perKeyspace, buildContextBroadcast(group, tags, unified))
	}

	var planOp *base.Op
	switch len(perKeyspace) {
	case 0:
		planOp = nil // no fusable detector (empty corpus, or all standalone/rejected).
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
		Plan:              planOp,
		Temps:             unified.Temps,
		Standalone:        standalone,
		Rejected:          rejected,
		FindingsLabels:    findingsLabels,
		DetKeyspace:       detKeyspace,
		CorrelationGroups: correlationGroups,
		wokenByTag:        wokenByTag,
		session:           s,
	}, nil
}

// corpusDetInfo is the extracted, normalized description of one fusable detector.
type corpusDetInfo struct {
	keyspaceName string        // keyspace.QualifiedName() -- the grouping key.
	keyspacer    interface{}   // the plan scan op (a keyspacer) for the shared scan.
	pred         []interface{} // predicate expr-tree, field refs rooted at ".".
	proj         []interface{} // evidence projection (engine.Detector.Proj), rooted at ".".
}

// analyzeCorpusDetector parses/plans/converts one detector's SQL++ and classifies it
// (see the file header's three classes). It returns:
//
//   - rejectReason != ""            -> REJECTED (parse/plan/convert failed).
//   - rejectReason == "", fusable   -> FUSABLE; info carries keyspace + normalized pred.
//   - rejectReason == "", !fusable  -> STANDALONE (converts fine, not the fusable
//     shape -- or a post-convert extraction step declined); the caller runs it
//     individually via s.Run. info is then zero.
//
// The crucial split: only a genuine parse/plan/convert FAILURE is a reject. Once the
// statement converts, an unrecognized shape (or a normalize step that can't extract
// the shared-scan bits) is Standalone -- still a valid, runnable query, never dropped.
func (s *Session) analyzeCorpusDetector(stmt string) (info corpusDetInfo, fusable bool, rejectReason string) {
	parsed, err := ParseStatement(stmt, s.Namespace, true)
	if err != nil {
		return corpusDetInfo{}, false, "parse error: " + err.Error()
	}
	qp, err := s.Store.PlanStatementQP(parsed, s.Namespace, nil, nil)
	if err != nil {
		return corpusDetInfo{}, false, "plan error: " + err.Error()
	}

	// Convert with a per-detector Conv (a plain Accept -- no post-plan rewrites; we
	// only need the raw project/filter/scan shape). A convert panic (an unsupported
	// op) is caught and reported as REJECTED, never crashing the compile.
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
		return corpusDetInfo{}, false, "convert error: " + convErr.Error()
	}
	if conv.TopOp == nil {
		return corpusDetInfo{}, false, "unconverted plan (nil TopOp)"
	}

	// From here parse/plan/convert has SUCCEEDED: any shape/extraction miss below is
	// STANDALONE (fusable=false, no reject reason), so the detector still runs.
	scan, filter, ok := recognizeCorpusDetector(conv.TopOp)
	if !ok {
		return corpusDetInfo{}, false, ""
	}

	// The recognizer matches only the shape BELOW the project; the projection itself
	// is shaped into evidence by corpusFusedProjection below. But a projection carrying
	// a (correlated) SUBQUERY -- e.g. the ASOF nearest-preceding argmax -- has an OUTER
	// shape (project -> scan) that LOOKS fusable while its real value comes from the
	// subquery per row. Fusing it would silently drop that subquery. Such a detector
	// must run STANDALONE (so WireASOFJoin / EvaluateSubquery fire); route it there.
	if projectionHasSubquery(conv.TopOp) {
		return corpusDetInfo{}, false, ""
	}

	// The alias the detector's field refs are rooted at (from the scan's `.["alias"]`
	// row label).
	if len(scan.Labels) == 0 {
		return corpusDetInfo{}, false, ""
	}
	alias, ok := mergeLabelLeaf(scan.Labels[0])
	if !ok {
		return corpusDetInfo{}, false, ""
	}
	aliasLabel := "." + LabelSuffix(alias)

	ks := branchScanKeyspace(conv.TopOp, conv.Temps)
	if ks == nil {
		return corpusDetInfo{}, false, ""
	}

	scanTempIdx, ok := scan.Params[0].(int)
	if !ok || scanTempIdx < 0 || scanTempIdx >= len(conv.Temps) {
		return corpusDetInfo{}, false, ""
	}

	pred := normalizeCorpusPred(filter, scan.Labels, alias, aliasLabel)

	// Shape the fused evidence to match the detector's SELECT projection (IDEA-0004):
	// a projection the fused envelope can't faithfully reproduce routes the detector
	// to standalone, where the full pipeline runs its real projection -- so evidence
	// shape is consistent whether a detector fuses or not.
	proj, ok := corpusFusedProjection(conv.TopOp, alias)
	if !ok {
		return corpusDetInfo{}, false, ""
	}

	return corpusDetInfo{
		keyspaceName: ks.QualifiedName(),
		keyspacer:    conv.Temps[scanTempIdx],
		pred:         pred,
		proj:         proj,
	}, true, ""
}

// recognizeCorpusDetector matches the canonical fusable detector shape and returns
// its scan leaf + (optional) filter. Accepts:
//
//	project -> filter -> datastore-scan-records     (SELECT ... WHERE ...)
//	project -> datastore-scan-records               (SELECT ... with no WHERE)
//
// Everything else (a join/group/order/distinct/subquery op in the chain, multiple
// sources, or a non-records leaf) returns ok=false. Only the shape below the project
// matters here; the project op's own terms are shaped into evidence separately by
// corpusFusedProjection (called from analyzeCorpusDetector).
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

// projectionHasSubquery reports whether the project op's boxed ["exprTree", expr]
// projection terms embed any correlated/scalar subquery. Mirrors the temporal
// recognizer's projection walk (recognizeASOFWalk in optimize_temporal.go): a fusable
// detector's outer shape can hide a subquery in its SELECT list, which the whole-row
// fused path would drop -- so such a detector is routed to a standalone run instead.
func projectionHasSubquery(project *base.Op) bool {
	if project == nil {
		return false
	}
	for _, p := range project.Params {
		term, ok := p.([]interface{})
		if !ok || len(term) < 2 {
			continue
		}
		if kind, _ := term[0].(string); kind != "exprTree" {
			continue
		}
		e, ok := term[1].(expression.Expression)
		if !ok || e == nil {
			continue
		}
		if len(collectSubqueries(e)) > 0 {
			return true
		}
	}
	return false
}

// corpusFusedProjection derives the fused-evidence projection (engine.Detector.Proj)
// from a detector's converted `project` op, so a FUSED detector's evidence shape
// matches the same SELECT run STANDALONE (IDEA-0004). The engine's projFunc emits one
// output column per Proj term after the tag; the findings schema carries a single
// evidence column, so every case here yields exactly ONE Proj term. Returns ok=false
// when the projection can't be faithfully reproduced in that single-column envelope,
// so analyzeCorpusDetector routes the detector to standalone (which honors it via the
// full pipeline). Field refs are re-rooted from the detector alias to SELF, resolving
// against the shared "." row exactly as normalizeCorpusPred's boxed fallback does.
//
//	SELECT *            (lone ".*" label)  -> whole matched row (the established MVP).
//	SELECT RAW expr     (lone "." label)   -> the single value itself.
//	SELECT a, b AS c    (all named terms)  -> a boxed OBJECT_CONSTRUCT {"a":..,"c":..}.
//	anything else (mixed star+terms, path.*, EXCLUDE, a group-key labelPath) -> false.
func corpusFusedProjection(project *base.Op, alias string) ([]interface{}, bool) {
	if project == nil || project.Kind != "project" || len(project.Labels) == 0 {
		return nil, false
	}

	// SELECT * -> the whole matched row (unchanged whole-row evidence). A lone star
	// label may also be SELECT p.*, whose value is NOT the whole row; but the "." row
	// IS the scanned doc and a bare `SELECT *` is overwhelmingly the case, so treat a
	// lone star as whole-row and leave p.* refinement to a future step.
	if len(project.Labels) == 1 && project.Labels[0] == ".*" {
		return []interface{}{[]interface{}{"labelPath", "."}}, true
	}

	// SELECT RAW expr -> the projected value itself (label "." from VisitInitialProject).
	if len(project.Labels) == 1 && project.Labels[0] == "." {
		e, ok := projTermExpr(project.Params[0])
		if !ok {
			return nil, false
		}
		return []interface{}{[]interface{}{"exprTree", renameAliasToSelf(e, alias)}}, true
	}

	// All plain named terms -> assemble a {alias: value, ...} object so the evidence
	// matches the standalone SELECT's row. A star mixed in, or a non-exprTree term (a
	// group-key labelPath), makes the leaf/expr unavailable -> route to standalone.
	mapping := make(map[expression.Expression]expression.Expression, len(project.Labels))
	for i, lbl := range project.Labels {
		name, ok := mergeLabelLeaf(lbl) // `.["c"]` -> "c"
		if !ok {
			return nil, false
		}
		e, ok := projTermExpr(project.Params[i])
		if !ok {
			return nil, false
		}
		mapping[expression.NewConstant(value.NewValue(name))] = renameAliasToSelf(e, alias)
	}
	obj := expression.NewObjectConstruct(mapping)
	return []interface{}{[]interface{}{"exprTree", obj}}, true
}

// projTermExpr returns the cbq expression of a project op's ["exprTree", expr] term,
// or ok=false for any other term shape (a native ["labelPath", ...] group-key term,
// or a star's self param).
func projTermExpr(param interface{}) (expression.Expression, bool) {
	t, ok := param.([]interface{})
	if !ok || len(t) < 2 {
		return nil, false
	}
	if head, _ := t[0].(string); head != "exprTree" {
		return nil, false
	}
	e, ok := t[1].(expression.Expression)
	return e, ok && e != nil
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

// Run executes the compiled corpus and returns the UNION of its tagged findings.
// It is the buffering wrapper over RunStream: collect every streamed finding into
// a slice. Callers that want bounded memory should use RunStream directly.
func (cc *CompiledCorpus) Run() ([]Finding, error) {
	var findings []Finding
	err := cc.RunStream(func(f Finding) error {
		findings = append(findings, f)
		return nil
	})
	return findings, err
}

// RunReport runs the corpus like Run and additionally returns per-keyspace scanned-row
// counts (IDEA-0015): it lays a stats overlay over the fused Plan so each shared scan's
// RowsIn is captured, letting a caller tell a 0-findings detector whose keyspace
// scanned ~0 rows (a whole-file blob / empty scan) apart from one whose predicate
// matched nothing. Findings are buffered like Run.
func (cc *CompiledCorpus) RunReport() ([]Finding, *CorpusRunReport, error) {
	var stats *base.Stats
	if cc.Plan != nil {
		stats = base.StatsLayout(cc.Plan)
	}
	// Reset the per-detector woken counters so a repeated RunReport doesn't accumulate
	// (the broadcast op bumps them live via the wired pointers during the run).
	for _, w := range cc.wokenByTag {
		*w = 0
	}
	var findings []Finding
	err := cc.runStream(func(f Finding) error {
		findings = append(findings, f)
		return nil
	}, stats)
	woken := make(map[string]int64, len(cc.wokenByTag))
	for tag, w := range cc.wokenByTag {
		woken[tag] = *w
	}
	return findings, &CorpusRunReport{
		ScannedByKeyspace: cc.scannedByKeyspace(stats),
		WokenByDetector:   woken,
	}, err
}

// scannedByKeyspace reads each fused broadcast-indexed op's RowsIn (its shared scan's
// fanned-row count) out of a post-run stats overlay, keyed by the op's keyspace. Empty
// when stats weren't collected. The op pointers walked here are the SAME ones
// StatsLayout stamped with StatsBase and the engine bumped, so the read is exact.
func (cc *CompiledCorpus) scannedByKeyspace(stats *base.Stats) map[string]int64 {
	out := map[string]int64{}
	if stats == nil || cc.Plan == nil {
		return out
	}
	var walk func(op *base.Op)
	walk = func(op *base.Op) {
		if op == nil {
			return
		}
		if op.Kind == "broadcast-indexed" && op.StatsBase >= 0 {
			slot := op.StatsBase + engine.StatBroadcastIndexedRowsIn
			if slot >= 0 && slot < len(stats.Counters) {
				if ks := branchScanKeyspace(op, cc.Temps); ks != nil {
					out[ks.QualifiedName()] = stats.Counters[slot]
				}
			}
		}
		for _, c := range op.Children {
			walk(c)
		}
	}
	walk(cc.Plan)
	return out
}

// RunStream executes the compiled corpus and calls onFinding for EACH tagged
// finding as it is produced -- so a consumer can stream at bounded memory instead
// of materializing the whole result set. It runs first each STANDALONE detector via
// the full s.Run pipeline (so ASOF/window/group lowerings fire and each is
// individually optimized), then the fused Plan -- mirroring Session.PlanExec's
// vars / GlueContext / ExecOpEx setup and reading each finding's [tag, evidence]
// straight from the yielded row slots (no ConvertVals). Finding order (across
// standalone runs, the union-all, and the interleaved fan-out) is NOT guaranteed.
// An onFinding error aborts the run and is returned. A nil Plan with no standalone
// detectors produces nothing; standalone-only corpora still produce their findings.
//
// NOTE: standalone detectors run via s.Run, which buffers that ONE detector's rows
// before they stream out; the FUSED majority streams row-by-row from the shared
// scan. So peak memory is bounded by the largest single standalone result, not the
// whole corpus.
func (cc *CompiledCorpus) RunStream(onFinding func(Finding) error) error {
	return cc.runStream(onFinding, nil)
}

// runStream is RunStream's body, optionally wiring a stats overlay (non-nil only from
// RunReport) so the fused Plan's per-op counters (e.g. each shared scan's RowsIn) are
// captured. stats == nil is the zero-overhead default path.
func (cc *CompiledCorpus) runStream(onFinding func(Finding) error, stats *base.Stats) error {
	s := cc.session

	cc.GatedSkipped = nil // repopulated per run by streamStandalone's index-gating.

	// Memory-behavior knobs + evidence. A fresh shared, race-safe merge-counter set is
	// installed for this run (propagated to each detector Run's Ctx by PlanExec + the
	// fused plan below), so a streaming merge's per-actor goroutines bump it without a
	// data race. N1K1_MEM_STATS prints a summary; the counts are also on cc.MergeStats
	// for a caller (e.g. RunReport) to surface.
	applyMemEnv()
	cc.MergeStats = &base.MergeStats{}
	s.MergeStats = cc.MergeStats
	defer func() { s.MergeStats = nil }()
	if os.Getenv("N1K1_MEM_STATS") != "" {
		defer cc.printMemStats()
	}

	// Part B execution sharing: install a shared-scan cache over the correlation
	// keyspaces for this run (DESIGN-sorting.md). It reaches the standalone detectors'
	// own s.Run scans (PlanExec propagates s.Pipe) and the fused plan below, serving each
	// keyspace's full scan+decode once. Transparent to everything else; removed after.
	if qns := correlationKeyspaceQNs(cc.CorrelationGroups); len(qns) > 0 {
		if dir, err := os.MkdirTemp("", "n1k1scancache"); err == nil {
			orig := s.Pipe
			cache := newCorpusScanCache(qns, dir, orig)
			cc.scanCache = cache
			s.Pipe = cache
			defer func() { s.Pipe = orig; os.RemoveAll(dir) }()
		}
	}

	// (A) Standalone detectors: run each verbatim through the full normal pipeline and
	// tag its SELECT-projection rows as evidence. s.Run is self-contained (it does its
	// own datastore + ExecOpEx setup), so this must happen BEFORE the fused block's
	// global ExecOpEx swap below.
	if err := cc.streamStandalone(onFinding); err != nil {
		return err
	}

	if cc.Plan == nil {
		return nil // standalone-only (or empty) corpus.
	}

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
	vars.Ctx.MergeStats = s.MergeStats
	if stats != nil {
		vars.Ctx.Stats = stats // RunReport: capture per-keyspace scanned (RowsIn) etc.
	}

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

	var execErr error

	yieldVals := func(vals base.Vals) {
		if len(vals) < 2 || execErr != nil {
			return
		}
		// Slot 0 = the tag (a JSON-quoted string placed by the broadcast op); slot 1
		// = the evidence value shaped by the detector's projection (JSON bytes).
		var tag string
		if err := json.Unmarshal([]byte(vals[0]), &tag); err != nil {
			tag = string(vals[0]) // fall back to the raw bytes if not a JSON string.
		}
		if e := onFinding(Finding{
			Tag:      tag,
			Evidence: append(json.RawMessage(nil), vals[1]...),
		}); e != nil {
			execErr = e
		}
	}
	yieldErr := func(e error) {
		if e != nil && execErr == nil {
			execErr = e
		}
	}

	engine.ExecOp(cc.Plan, vars, yieldVals, yieldErr, "", "")

	return execErr
}

// streamStandalone runs each Standalone detector through the FULL normal pipeline
// (Session.Run -- so its ASOF/window/group/join lowerings all fire and each is
// individually optimized) and calls onFinding for every result row, tagged with the
// detector's id. Evidence is the detector's SELECT projection (the fused path shapes
// the same projection into evidence via corpusFusedProjection). A run-time error
// from any standalone detector (or from onFinding) is returned. Note s.Run buffers a
// single detector's rows before they stream out (see RunStream's NOTE).
func (cc *CompiledCorpus) streamStandalone(onFinding func(Finding) error) error {
	// gateCache dedups the presence probe per (Source, Gate) so N detectors sharing a
	// gate over one keyspace probe it once.
	gateCache := map[string]bool{}

	for _, d := range cc.Standalone {
		if d.Gate != "" && d.Source != "" {
			allow, err := cc.gateAllows(d.Source, d.Gate, gateCache)
			if err != nil {
				// A broken/erroring gate must not silently drop findings: fall through
				// and RUN the detector (safe -- gating only ever SKIPS). The error rides
				// out as a warning-shaped skip note so the author can fix the gate.
				cc.GatedSkipped = append(cc.GatedSkipped,
					fmt.Sprintf("%s (gate error, ran anyway: %v)", d.Tag, err))
			} else if !allow {
				// The precondition matched no row in the keyspace: the detector cannot
				// produce a finding, so skip its expensive standalone sort/window.
				cc.GatedSkipped = append(cc.GatedSkipped, d.Tag)
				continue
			}
		}

		res, err := cc.session.Run(d.Stmt)
		if err != nil {
			return fmt.Errorf("standalone query %q: %w", d.Tag, err)
		}
		for _, row := range res.Rows {
			if err := onFinding(Finding{
				Tag:      d.Tag,
				Evidence: append(json.RawMessage(nil), row...),
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

// gateAllows reports whether the Source keyspace holds at least one row satisfying the
// gate precondition -- a cheap `SELECT 1 FROM <source> WHERE <gate> LIMIT 1` probe that
// stops at the first match (and whose scan itself benefits from literal pushdown). false
// => the standalone detector can be skipped. Results are cached per (source|gate) key.
func (cc *CompiledCorpus) gateAllows(source, gate string, cache map[string]bool) (bool, error) {
	key := source + "\x00" + gate
	if v, ok := cache[key]; ok {
		return v, nil
	}
	// The probe is a plain SELECT over the (possibly bound) logical keyspace, so it
	// resolves exactly as the detector's own FROM does under this session's binding.
	res, err := cc.session.Run("SELECT 1 FROM " + source + " WHERE " + gate + " LIMIT 1")
	if err != nil {
		return false, err
	}
	allow := len(res.Rows) > 0
	cache[key] = allow
	return allow, nil
}
