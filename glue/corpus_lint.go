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

// corpus_lint.go is the "report card" companion to corpus.go's CorpusCompile
// (DESIGN-prepare.md phase 7, "Detector authoring & ops"). Where CorpusCompile
// FUSES a corpus into one runnable plan, CorpusLint COMPILES each detector and
// SURFACES the signals n1k1 already computes -- fuse/standalone/reject, the eval
// lane (native vs boxed, via ExprCoverage), and the predicate index verdict
// (index-pruned by a necessary literal vs always-wake, via engine.PrefilterLiteral)
// -- plus mechanical, deterministic authoring advice. The load-bearing point of the
// design: "most of this is reporting what exists, not new machinery." So CorpusLint
// reuses corpus.go's exact classifier (parse/plan/convert + recognizeCorpusDetector +
// normalizeCorpusPred) so a detector's lint verdict matches how CorpusCompile would
// actually treat it.
//
// DEFERRED (noted, not built): CSE participation per detector (which shared terms it
// contributes -- BroadcastCSE knows, but plumbing it per-detector is future work); a
// cost class / est. predicate-evals per row; and golden-fixture presence (phase 7's
// recipe front-matter). The score line below covers % fused / native / index-pruned.

import (
	"fmt"

	"github.com/couchbase/query/datastore"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/engine"
)

// Detector lint classes.
const (
	LintFused      = "fused"      // the canonical single-source shape; folded into the shared-scan plan.
	LintStandalone = "standalone" // valid but not fusable (join/group/window/subquery/index-scan); runs individually.
	LintRejected   = "rejected"   // parse/plan/convert failed; surfaced, not run.
)

// DetectorLint is one detector's authoring report card: how CorpusCompile would
// classify it, the keyspace it targets, whether its expressions evaluate on the
// native byte lane or box to cbq, whether its predicate is index-pruned by a
// necessary literal (or always-wakes), and mechanical advice for the author.
type DetectorLint struct {
	Label    string   // the detector id (its filename stem in the CLI).
	Class    string   // LintFused | LintStandalone | LintRejected.
	Reason   string   // why standalone / why rejected; "" for a clean fused detector.
	Keyspace string   // the target keyspace (a fused detector's grouping key; a standalone's FROM); "" if none.
	Lane     string   // "native" | "boxed" | "" (rejected/unconverted). "boxed" iff any expr falls back to cbq.
	Native   int      // count of natively-evaluated project/filter expressions.
	Boxed    int      // count of boxed (cbq value.Value) project/filter expressions.
	Literal  string   // the predicate index's required substring, when index-pruned.
	Indexed  bool     // true when a necessary literal was extracted (index prunes wake-ups).
	Advice   []string // mechanical, deterministic authoring advice (may be empty).
}

// CorpusScore is the corpus-level roll-up printed under a lint report: the fraction
// of detectors that fuse, evaluate natively, and are index-pruned, plus the raw
// counts behind each fraction. It is the guardrail that stops an AI-authored corpus
// from silently bloating (all always-wake) or lying (rejected -> no findings).
type CorpusScore struct {
	Total         int // all detectors linted.
	Fused         int // classified fused.
	Standalone    int // classified standalone.
	Rejected      int // classified rejected.
	Native        int // converted detectors whose every expression stays native.
	Converted     int // fused + standalone (the denominator for Native).
	IndexPruned   int // fused detectors with a necessary literal.
	FusedForIndex int // fused detectors (the denominator for IndexPruned).
}

// PctFused, PctNative, PctIndexPruned are the three headline percentages (0..100),
// guarding against a zero denominator (an empty / all-rejected corpus reports 0).
func (s CorpusScore) PctFused() int       { return pct(s.Fused, s.Total) }
func (s CorpusScore) PctNative() int      { return pct(s.Native, s.Converted) }
func (s CorpusScore) PctIndexPruned() int { return pct(s.IndexPruned, s.FusedForIndex) }

func pct(n, d int) int {
	if d <= 0 {
		return 0
	}
	return n * 100 / d
}

// CorpusLint compiles (does NOT run) each detector and returns its report card, in
// the given order. It mirrors CorpusCompile's classifier exactly so the verdict is
// faithful: a detector CorpusLint calls "fused" is one CorpusCompile would fuse. It
// resolves keyspaces against this session's store (as CorpusCompile / Run do), so a
// bound (late-binding) session lints the corpus the same way it would run it. A
// per-detector failure never fails the call -- it becomes a LintRejected row; err is
// non-nil only for a setup-level problem (currently none, reserved).
func (s *Session) CorpusLint(dets []CorpusDetector) ([]DetectorLint, CorpusScore, error) {
	if s.Store != nil && s.Store.Datastore != nil {
		datastore.SetDatastore(s.Store.Datastore)
	}

	out := make([]DetectorLint, 0, len(dets))
	var score CorpusScore
	for _, d := range dets {
		dl := s.lintOne(d)
		out = append(out, dl)

		score.Total++
		switch dl.Class {
		case LintFused:
			score.Fused++
			score.Converted++
			score.FusedForIndex++
			if dl.Boxed == 0 {
				score.Native++
			}
			if dl.Indexed {
				score.IndexPruned++
			}
		case LintStandalone:
			score.Standalone++
			score.Converted++
			if dl.Boxed == 0 {
				score.Native++
			}
		case LintRejected:
			score.Rejected++
		}
	}
	return out, score, nil
}

// lintOne is the per-detector analysis: parse/plan/convert (a failure -> rejected),
// then classify the converted shape exactly as analyzeCorpusDetector does, layering
// on the eval-lane (ExprCoverage) and predicate-index (engine.PrefilterLiteral)
// signals plus mechanical advice.
func (s *Session) lintOne(d CorpusDetector) DetectorLint {
	dl := DetectorLint{Label: d.Label}

	conv, rejectReason := s.convertCorpusDetector(d.Stmt)
	if rejectReason != "" {
		dl.Class = LintRejected
		dl.Reason = rejectReason
		dl.Advice = []string{"rejected: " + rejectReason + " -- fix the statement (it never runs, so it can never fire)"}
		return dl
	}
	top := conv.TopOp

	// The eval lane is a static ExprCoverage over the detector's OWN converted plan
	// (its project + filter expressions), uniform across fused and standalone.
	dl.Native, dl.Boxed = ExprCoverage(top)
	if dl.Boxed > 0 {
		dl.Lane = "boxed"
	} else {
		dl.Lane = "native"
	}

	// Best-effort target keyspace (the FROM leaf). For a fused detector this is the
	// shared-scan grouping key; for a standalone one it's informational.
	if ks := branchScanKeyspace(top, conv.Temps); ks != nil {
		dl.Keyspace = ks.QualifiedName()
	}

	scan, filter, ok := recognizeCorpusDetector(top)
	if !ok || projectionHasSubquery(top) {
		dl.Class = LintStandalone
		dl.Reason = standaloneReason(top)
		dl.Advice = adviceFor(dl)
		return dl
	}
	if len(scan.Labels) == 0 {
		dl.Class = LintStandalone
		dl.Reason = "scan has no row label"
		dl.Advice = adviceFor(dl)
		return dl
	}
	alias, aok := mergeLabelLeaf(scan.Labels[0])
	if !aok {
		dl.Class = LintStandalone
		dl.Reason = "unrecognized scan alias"
		dl.Advice = adviceFor(dl)
		return dl
	}

	// Fusable: normalize the predicate the same way CorpusCompile does, then ask the
	// predicate index whether a necessary literal is extractable.
	dl.Class = LintFused
	pred := normalizeCorpusPred(filter, scan.Labels, alias, "."+LabelSuffix(alias))
	if lit, has := engine.PrefilterLiteral(pred); has {
		dl.Indexed = true
		dl.Literal = lit
	}
	dl.Advice = adviceFor(dl)
	return dl
}

// convertCorpusDetector is the parse/plan/convert prefix of analyzeCorpusDetector,
// returning the converted top op (nil + a reason on any parse/plan/convert failure).
// Kept separate so the lint can inspect the converted tree without re-deriving the
// fused-scan bits.
func (s *Session) convertCorpusDetector(stmt string) (conv *Conv, rejectReason string) {
	parsed, err := ParseStatement(stmt, s.Namespace, true)
	if err != nil {
		return nil, "parse error: " + err.Error()
	}
	qp, err := s.Store.PlanStatementQP(parsed, s.Namespace, nil, nil)
	if err != nil {
		return nil, "plan error: " + err.Error()
	}
	conv = &Conv{Temps: []interface{}{nil}}
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
		return nil, "convert error: " + convErr.Error()
	}
	if conv.TopOp == nil {
		return nil, "unconverted plan (nil TopOp)"
	}
	return conv, ""
}

// standaloneReason describes WHY a converted-but-non-fusable detector runs standalone,
// by naming the first non-fusable feature in its op tree (a correlated subquery,
// join, group, window, order/limit, distinct, or an index/FTS/keys scan leaf). It is
// advisory -- CorpusCompile's authority is recognizeCorpusDetector; this just puts a
// human-readable label on the miss.
func standaloneReason(top *base.Op) string {
	if projectionHasSubquery(top) {
		return "correlated/scalar subquery in projection (ASOF/argmax) -- runs standalone so the merge/subquery fires"
	}
	kinds := map[string]bool{}
	var walk func(op *base.Op)
	walk = func(op *base.Op) {
		if op == nil {
			return
		}
		kinds[op.Kind] = true
		for _, ch := range op.Children {
			walk(ch)
		}
	}
	walk(top)

	switch {
	case kinds["merge-join"] || kinds["unnest-inner"]:
		return "join / unnest (multiple sources) -- runs standalone"
	case kinds["group"]:
		return "GROUP BY / aggregation -- runs standalone"
	case kinds["window-frames"] || kinds["window-partition"]:
		return "window function (OVER ...) -- runs standalone"
	case kinds["with-recursive"] || kinds["expr-scan"]:
		return "CTE / expression scan -- runs standalone"
	case kinds["distinct"]:
		return "DISTINCT -- runs standalone"
	case kinds["order-offset-limit"]:
		return "ORDER BY / LIMIT / OFFSET -- runs standalone"
	case kinds["datastore-scan-index"] || kinds["datastore-scan-index-cover"]:
		return "planner chose an index scan (sargable) -- single-keyspace filter, not yet fused"
	case kinds["datastore-scan-fts"]:
		return "full-text (SEARCH) scan -- runs standalone"
	case kinds["datastore-scan-keys"]:
		return "USE KEYS / key scan -- runs standalone"
	case kinds["union-all"]:
		return "set operation (UNION/INTERSECT/EXCEPT) -- runs standalone"
	}
	return "non-fusable shape (not project([filter,]datastore-scan-records)) -- runs standalone"
}

// adviceFor produces the mechanical, deterministic authoring nudges the report card
// shows (DESIGN-prepare.md: "advice ... is mechanical"). A boxed predicate caps the
// compile level and needs cbq; an always-wake fused detector wastes the predicate
// index; a standalone detector doesn't share the fused scan.
func adviceFor(dl DetectorLint) []string {
	var adv []string
	if dl.Lane == "boxed" {
		adv = append(adv, "predicate/projection boxes (needs cbq) -> caps compile level; prefer a native form "+
			"(e.g. a plain literal contains/=, or regexp_contains without a case wrapper)")
	}
	if dl.Class == LintFused && !dl.Indexed {
		adv = append(adv, "always-wake: no necessary literal -- add a discriminating literal as a top-level AND "+
			"conjunct so the index can prune (the query wakes on every row otherwise)")
	}
	if dl.Class == LintStandalone {
		adv = append(adv, "standalone: "+dl.Reason+" -- runs individually (does not share the fused scan)")
	}
	return adv
}
