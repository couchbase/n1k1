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

package engine

import (
	"strconv"

	"github.com/couchbase/n1k1/base"
)

// OpBroadcast is the shared-scan fan-out (broadcast / tee) op -- the MVP of the
// PREPARE++ multi-query optimization (DESIGN-prepare.md phase 3, "Shared scan /
// multi-query optimization").
//
// It scans a keyspace ONCE and fans each yielded row to K "detector" pipelines,
// so K detectors run over one scan + one decode instead of K separate
// scan+decode passes. This is the core capability that lets thousands of
// detectors be applied to a support bundle without rescanning it thousands of
// times: "scan once, push each row to K detector predicate pipelines ... Beats N
// separate runs (one scan, one decode per row)".
//
// Shape:
//
//   - Exactly ONE child (o.Children[0]): the shared scan whose rows are fanned.
//   - o.Params[0] encodes the K detectors. Each detector = { tag, predicate
//     expr-tree, projection expr-trees } -- i.e. a "filter + project", the MVP
//     granularity the design calls "N predicates". General fed sub-pipelines are
//     future work and deliberately NOT attempted here.
//
// Per matching row a detector yields its projected row TAGGED with the
// detector's id in output slot 0, so a consumer can demultiplex the interleaved
// findings stream by tag. All detectors project into a uniform findings schema,
// so the op's output Labels stay stable: [<tag>, <evidence...>].
//
// Zero per-row garbage: the predicate/projection base.ExprFuncs are built ONCE
// at setup (via MakeExprFunc / MakeProjectFunc, exactly as OpFilter / OpProject
// do), the input row is shared across all detectors (native, no re-decode / no
// boxing), and each detector reuses its own output buffer across rows. The only
// allocations happen in setup.
//
// Like OpMergeScan (DESIGN-merging.md), this op is interpreter-oriented for now:
// OpBroadcast delegates via a single "// !lz" line to BroadcastExec, whose body
// is deliberately free of any "lz" tokens so the gen-compiler copies it VERBATIM
// into the intermed package (it compiles cleanly there, but is never dispatched,
// since the SQL/plan recognition that would emit a "broadcast" op is a later
// slice). Compiled-path fusion of the fan-out is future work.
func OpBroadcast(o *base.Op, lzVars *base.Vars, lzYieldVals base.YieldVals,
	lzYieldErr base.YieldErr, path, pathNext string) {
	BroadcastExec(o, lzVars, lzYieldVals, lzYieldErr, path, pathNext) // !lz
}

// broadcastDetector holds one detector's setup-built, reusable machinery: its
// tag value (a precomputed JSON string placed in output slot 0), its predicate
// ExprFunc, its projection ProjectFunc, and a per-detector output buffer reused
// across rows. Nothing here is allocated per row.
type broadcastDetector struct {
	tagVal   base.Val
	predFunc base.ExprFunc
	projFunc base.ProjectFunc
	outReuse base.Vals
}

// BroadcastExec holds the actual (non-lazy) fan-out logic. Its params are
// intentionally named WITHOUT the "lz" prefix so every line is copied verbatim
// by the gen-compiler (see OpMergeScan / MergeScanExec for the same pattern).
//
// Params layout:
//
//	Params[0] []interface{}  -- the K detectors. Each element is itself a
//	                            []interface{}{ tag, predExpr, projExprs }:
//	                              tag       string        -- detector id (output slot 0)
//	                              predExpr  []interface{} -- predicate expr-tree
//	                                                         (an ExprCatalog tree,
//	                                                         e.g. ["gt", ["labelPath",
//	                                                         ".","a"], ["json","5"]]).
//	                              projExprs []interface{} -- list of projection
//	                                                         expr-trees, same shape
//	                                                         as an OpProject Params.
//
// The child scan's Labels drive predicate / projection field resolution. The
// op's own o.Labels are the stable findings schema ([<tag>, <evidence...>]); the
// caller supplies them.
func BroadcastExec(o *base.Op, vars *base.Vars, yieldVals base.YieldVals,
	yieldErr base.YieldErr, path, pathNext string) {
	childLabels := base.Labels(nil)
	if len(o.Children) > 0 {
		childLabels = o.Children[0].Labels
	}

	// Setup (once, before any row): build each detector's predicate ExprFunc and
	// projection ProjectFunc. Garbage here is fine; the hot loop below is not.
	specs := broadcastDetectorSpecs(o.Params)

	detectors := make([]broadcastDetector, 0, len(specs))
	for i, spec := range specs {
		predFunc := MakeExprFunc(vars, childLabels, spec.pred, pathNext, "pred"+strconv.Itoa(i))

		projFunc := MakeProjectFunc(vars, childLabels, spec.proj, pathNext, "proj"+strconv.Itoa(i))

		detectors = append(detectors, broadcastDetector{
			tagVal:   base.Val(strconv.Quote(spec.tag)),
			predFunc: predFunc,
			projFunc: projFunc,
		})
	}

	// Stats counters update live (per row) into the shared array so the CLI
	// display animates; they are genCompiler:hide'd -> interpreter-only (the
	// compiled path collects no stats -- see DESIGN-stats.md's KNOWN LIMITATION).
	stats := StatsFromVars(vars)                                // <== genCompiler:hide
	statsBase := o.StatsBase                                    // <== genCompiler:hide
	StatsCounterZero(stats, statsBase+StatBroadcastRowsIn)      // <== genCompiler:hide
	StatsCounterZero(stats, statsBase+StatBroadcastFindingsOut) // <== genCompiler:hide

	// Per row (in the shared scan's yield callback): evaluate every detector's
	// predicate against the SAME native row; on a truthy predicate, project into
	// the detector's REUSED buffer -- tag first -- and yield the tagged finding.
	// MISSING / NULL / false predicates are non-truthy (base.ValTruthy), so those
	// rows are dropped exactly as OpFilter drops them.
	childYield := func(vals base.Vals) {
		StatsCounterBump(stats, statsBase+StatBroadcastRowsIn) // stats: live // <== genCompiler:hide

		for i := range detectors {
			d := &detectors[i]

			predVal := d.predFunc(vals, yieldErr)

			if base.ValTruthy(predVal) {
				out := d.outReuse[:0]
				out = append(out, d.tagVal)
				out = d.projFunc(vals, out, yieldErr)
				d.outReuse = out

				StatsCounterBump(stats, statsBase+StatBroadcastFindingsOut) // stats: live // <== genCompiler:hide

				yieldVals(out)
			}
		}
	}

	ExecOp(o.Children[0], vars, childYield, yieldErr, pathNext, "0")
}

// broadcastDetectorSpec is the parsed, pre-setup description of one detector.
type broadcastDetectorSpec struct {
	tag  string
	pred []interface{}
	proj []interface{}
}

// broadcastDetectorSpecs parses o.Params[0] into per-detector specs. A malformed
// entry is a programming error at this engine layer (glue builds these), so it is
// simply skipped rather than half-decoded.
func broadcastDetectorSpecs(params []interface{}) []broadcastDetectorSpec {
	if len(params) == 0 || params[0] == nil {
		return nil
	}

	raw, ok := params[0].([]interface{})
	if !ok {
		return nil
	}

	specs := make([]broadcastDetectorSpec, 0, len(raw))
	for _, r := range raw {
		det, ok := r.([]interface{})
		if !ok || len(det) < 3 {
			continue
		}

		tag, _ := det[0].(string)
		pred, _ := det[1].([]interface{})

		var proj []interface{}
		if p, ok := det[2].([]interface{}); ok {
			proj = p
		}

		specs = append(specs, broadcastDetectorSpec{tag: tag, pred: pred, proj: proj})
	}

	return specs
}
