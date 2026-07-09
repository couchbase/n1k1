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
	"sort"

	"github.com/couchbase/n1k1/base"
)

// Detector describes one filter+project unit of the PREPARE++ detector corpus,
// already BOUND to the single source it targets. It is the routing-builder's
// input granularity -- the same "filter + project" MVP that OpBroadcast fans out
// (see op_broadcast.go), plus one extra field: TargetSource.
//
// The caller DECLARES TargetSource. Inferring it from a detector's SQL++ FROM
// clause needs cbq parsing and is the future corpus-compiler's job (DESIGN-
// prepare.md phase 6) that will FEED this builder; BroadcastRoute deliberately
// does no SQL parsing.
type Detector struct {
	// Tag is the detector id, emitted in output slot 0 of every finding so a
	// consumer can demultiplex the interleaved findings stream.
	Tag string

	// TargetSource is the id of the ONE source (a key of BroadcastRoute's
	// sources map) this detector runs against. A detector whose TargetSource is
	// absent from sources is an orphan: pruned and RETURNED, never silently run
	// against nothing (which "would yield an empty findings table that reads as
	// clean").
	TargetSource string

	// Pred is the predicate expr-tree (an ExprCatalog tree), same shape as an
	// OpFilter Params -- e.g. ["gt", ["labelPath",".","a"], ["json","5"]].
	Pred []interface{}

	// Proj is the LIST of projection expr-trees (same shape as an OpProject
	// Params); it becomes det[2] of a broadcast detector spec.
	Proj []interface{}
}

// BroadcastRoute is the source-routing plan builder: the cheapest, highest-
// leverage lever of PREPARE++ multi-query optimization (DESIGN-prepare.md,
// "Source routing (cheap, big)"). It takes a detector corpus + the available
// sources and produces a routed plan where each detector runs ONLY against its
// TargetSource -- "a file only fans out to detectors that target it. Prune
// before any evaluation."
//
// Where a flat broadcast over all sources is O(K x rows) in predicate work
// (BenchmarkBroadcastScaling), routing drops the per-row predicate work ~M-fold
// for K detectors spread across M sources: a source's rows are only ever
// evaluated by the detectors that target that source.
//
// The routed plan reuses the EXISTING ops -- no new engine op, no intermed /
// codegen change. It is a "union-all" over one "broadcast" per source-that-has-
// detectors. Each broadcast shares the uniform findings schema (findingsLabels,
// [tag, evidence...]), so union-all funnels the per-source findings into one
// stream.
//
//   - sources: available sources, keyed by id; the value is that source's scan
//     *base.Op (the broadcast's shared-scan child).
//   - detectors: the corpus, each already bound to a TargetSource.
//   - findingsLabels: the uniform findings schema shared by every broadcast and
//     the union-all.
//
// Returns:
//
//   - routed: the routed plan. nil if NO detector targets a present source
//     (empty corpus, or all orphaned) -- an honestly empty plan, not a plan that
//     "reads as clean". A single contributing source returns that source's
//     broadcast directly (no needless union-all wrapper).
//   - orphans: detectors whose TargetSource is absent from sources, in input
//     order. Surfaced, never hidden -- a binding "must fail loudly, not
//     silently". The CALLER decides whether an orphan is a hard error.
func BroadcastRoute(sources map[string]*base.Op, detectors []Detector,
	findingsLabels base.Labels) (routed *base.Op, orphans []Detector) {
	// Group detectors by TargetSource, preserving input order within a source.
	// Orphans (unknown TargetSource) are collected out, in input order.
	bySource := map[string][]Detector{}
	for _, d := range detectors {
		if _, ok := sources[d.TargetSource]; !ok {
			orphans = append(orphans, d)
			continue
		}
		bySource[d.TargetSource] = append(bySource[d.TargetSource], d)
	}

	// One broadcast per source-that-has-detectors, in sorted-id order so the
	// plan is deterministic regardless of Go map iteration order.
	ids := make([]string, 0, len(bySource))
	for id := range bySource {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	broadcasts := make([]*base.Op, 0, len(ids))
	for _, id := range ids {
		dets := bySource[id]

		detParams := make([]interface{}, 0, len(dets))
		for _, d := range dets {
			// The existing broadcast detector spec: []interface{}{tag, pred, proj}
			// (see broadcastDetectorSpecs / benchDetectorParams).
			detParams = append(detParams, []interface{}{d.Tag, d.Pred, d.Proj})
		}

		broadcasts = append(broadcasts, &base.Op{
			Kind:     "broadcast",
			Labels:   append(base.Labels(nil), findingsLabels...),
			Params:   []interface{}{detParams},
			Children: []*base.Op{sources[id]},
		})
	}

	switch len(broadcasts) {
	case 0:
		// No present source has a detector: empty corpus or all orphaned.
		return nil, orphans
	case 1:
		// A single contributing source needs no union-all wrapper.
		return broadcasts[0], orphans
	default:
		return &base.Op{
			Kind:     "union-all",
			Labels:   append(base.Labels(nil), findingsLabels...),
			Children: broadcasts,
		}, orphans
	}
}
