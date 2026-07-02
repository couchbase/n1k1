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
//	git grep '= base.DefStat'   # the counter catalog across engine/ and glue/
//	git grep 'DefStat("RowsOut' # every op that has a RowsOut counter
//
// See DESIGN-stats.md "Stat naming" for the naming rules (Noun-first; monotonic
// is the unmarked default; a gauge takes a "Cur" suffix, a high-water "Peak").
// All counters below are monotonic.
var (
	StatScanRowsOut = base.DefStat("RowsOut", "scan")

	StatFilterRowsIn  = base.DefStat("RowsIn", "filter")  // rows examined
	StatFilterRowsOut = base.DefStat("RowsOut", "filter") // rows passing the predicate

	StatGroupRowsIn    = base.DefStat("RowsIn", "group", "distinct")    // rows aggregated
	StatGroupGroupsOut = base.DefStat("GroupsOut", "group", "distinct") // distinct groups

	StatOrderRowsIn  = base.DefStat("RowsIn", "order-offset-limit")  // rows into sort/limit
	StatOrderRowsOut = base.DefStat("RowsOut", "order-offset-limit") // rows after offset/limit

	// joinNL family: Probes counts inner-loop row visits (|left| x
	// |right-per-left|) -- the "exploding join" work signal that spins even when
	// RowsOut stays small. See DESIGN-stats.md.
	StatJoinNLRowsLeft = base.DefStat("RowsLeft", joinNLKinds...) // left-driver (outer) rows
	StatJoinNLProbes   = base.DefStat("Probes", joinNLKinds...)   // inner-loop row visits
)

// joinNLKinds are the op Kinds handled by OpJoinNestedLoop; they share a layout.
var joinNLKinds = []string{
	"joinNL-inner", "joinNL-leftOuter",
	"joinKeys-inner", "joinKeys-leftOuter",
	"nestNL-inner", "nestNL-leftOuter",
	"nestKeys-inner", "nestKeys-leftOuter",
	"unnest-inner", "unnest-leftOuter",
}
