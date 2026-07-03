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

	StatGroupRowsIn    = base.DefStat("RowsIn", "input rows the operator consumed", "group", "distinct")
	StatGroupGroupsOut = base.DefStat("GroupsOut", "distinct groups (or DISTINCT rows) emitted", "group", "distinct")

	StatOrderRowsIn  = base.DefStat("RowsIn", "input rows the operator consumed", "order-offset-limit")
	StatOrderRowsOut = base.DefStat("RowsOut", "rows emitted to the parent", "order-offset-limit")

	// joinNL family: Probes counts inner-loop row visits (|left| x
	// |right-per-left|) -- the "exploding join" work signal that spins even when
	// RowsOut stays small. See DESIGN-stats.md.
	StatJoinNLRowsLeft = base.DefStat("RowsLeft", "outer (left) rows driving a nested-loop join", joinNLKinds...)
	StatJoinNLProbes   = base.DefStat("Probes", "inner-loop row visits -- join work (|left|x|right|)", joinNLKinds...)
)

// joinNLKinds are the op Kinds handled by OpJoinNestedLoop; they share a layout.
var joinNLKinds = []string{
	"joinNL-inner", "joinNL-leftOuter",
	"joinKeys-inner", "joinKeys-leftOuter",
	"nestNL-inner", "nestNL-leftOuter",
	"nestKeys-inner", "nestKeys-leftOuter",
	"unnest-inner", "unnest-leftOuter",
}
