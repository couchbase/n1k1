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
	"github.com/couchbase/n1k1/base"
)

// Every operator counter is declared below with base.DefStat, one per line. That
// single line is the source of truth for both the counter's registered name (so
// LayoutStats can size the flat array -- see base.StatsDescs) and its offset
// constant (used by the op's hot path). To list every counter straight from the
// source (no doc drift):
//
//	git grep '= base.DefStat'   # the counter catalog: name, about-string, op Kind(s)
//	git grep 'DefStat("RowsOut' # every op that has a RowsOut counter
//
// See DESIGN-stats.md "Stat naming" for the naming rules (Noun-first; monotonic
// is the unmarked default; a gauge takes a "Cur" suffix, a high-water "Peak").
// All counters below are monotonic.
var (
	StatScanRowsOut = base.DefStat("RowsOut", "rows emitted to the parent", "scan")

	StatFilterRowsIn  = base.DefStat("RowsIn", "input rows the operator consumed", "filter")
	StatFilterRowsOut = base.DefStat("RowsOut", "rows emitted to the parent", "filter")

	// broadcast (shared-scan fan-out): RowsIn counts the SHARED rows fanned in
	// (scanned + decoded ONCE); FindingsOut counts tagged findings emitted across
	// all K detectors. RowsIn stays at N no matter how many detectors run -- that
	// flat decode cost is the whole point of the op.
	StatBroadcastRowsIn      = base.DefStat("RowsIn", "shared rows fanned in (scanned+decoded once)", "broadcast")
	StatBroadcastFindingsOut = base.DefStat("FindingsOut", "tagged findings emitted across all detectors", "broadcast")

	StatGroupRowsIn    = base.DefStat("RowsIn", "input rows the operator consumed", "group", "distinct")
	StatGroupGroupsOut = base.DefStat("GroupsOut", "distinct groups (or DISTINCT rows) emitted", "group", "distinct")

	StatOrderRowsIn  = base.DefStat("RowsIn", "input rows the operator consumed", "order-offset-limit")
	StatOrderRowsOut = base.DefStat("RowsOut", "rows emitted to the parent", "order-offset-limit")

	// joinNL family: Probes counts inner-loop row visits (|left| x
	// |right-per-left|) -- the "exploding join" work signal that spins even when
	// RowsOut stays small. See DESIGN-stats.md.
	StatJoinNLRowsLeft = base.DefStat("RowsLeft", "outer (left) rows driving a nested-loop join", StatsJoinNLKinds...)
	StatJoinNLProbes   = base.DefStat("Probes", "inner-loop row visits -- join work (|left|x|right|)", StatsJoinNLKinds...)
)

// StatsJoinNLKinds are the op Kinds handled by OpJoinNestedLoop; they share a stats layout.
var StatsJoinNLKinds = []string{
	"joinNL-inner", "joinNL-leftOuter",
	"joinKeys-inner", "joinKeys-leftOuter",
	"nestNL-inner", "nestNL-leftOuter",
	"nestKeys-inner", "nestKeys-leftOuter",
	"unnest-inner", "unnest-leftOuter",
}

// StatsFromVars returns the request's shared *Stats for live, per-row counter updates,
// or nil when stats are off. Ops bump their counters as rows flow (so the CLI's
// live display animates instead of only showing final totals). These live updates
// are interpreter-only for now: their call sites are marked genCompiler:hide, so
// the compiled path is unaffected (see DESIGN-stats.md's KNOWN LIMITATION).
func StatsFromVars(lzVars *base.Vars) *base.Stats {
	if lzVars != nil && lzVars.Ctx != nil {
		return lzVars.Ctx.Stats
	}
	return nil
}

// StatsCounterBump increments counter i (a no-op when stats are off). StatsCounterZero
// resets it to 0 -- called at an op's setup so a re-run op (e.g. a nested-loop join's
// inner subtree, re-executed per outer row) restarts its counters each invocation,
// matching the datastore scan's per-invocation semantics and giving a live bar
// that resets. Single-writer per op instance, so no atomics.
func StatsCounterBump(s *base.Stats, i int) {
	if s != nil {
		s.Counters[i]++
	}
}

func StatsCounterZero(s *base.Stats, i int) {
	if s != nil {
		s.Counters[i] = 0
	}
}

// StatsGroupAggNames flattens an OpGroup's aggCalcs (a list of lists of aggregate
// handler names, in the same order the group value bytes are laid out) into a flat
// []string -- but ONLY if every aggregate is cheaply runningCapable (base.AggRunningCapable);
// otherwise it returns nil so OpGroup registers no live-aggregate running-aggregate (the
// group stays progress-only). Kept store-free so it can be copied verbatim into the
// intermed compiled-builder package (which does not import rhmap/store); the actual
// map walk lives in base.RunningAggsGroup. See DESIGN-stats.md "Live aggregates".
func StatsGroupAggNames(aggCalcs []interface{}) []string {
	if len(aggCalcs) == 0 {
		return nil
	}

	var names []string
	for _, aggCalc := range aggCalcs {
		for _, aggName := range aggCalc.([]interface{}) {
			name := aggName.(string)
			if !base.IsRunningAggCapable(name) {
				return nil // A non-runningCapable agg -> progress-only for the whole group.
			}
			names = append(names, name)
		}
	}

	return names
}
