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

// Stat counter offsets, one block of constants per op Kind. Each constant is the
// index of a stat within that op's counter section, and MUST match the order the
// names are registered in base.StatsDescs (init below). The hot path uses these
// as compile-time constant offsets, e.g. counters[o.StatsBase + StatScanRowsOut].
//
// Naming convention (see DESIGN-stats.md "Stat naming"): Noun-first
// ("NounAdjective") so a subsystem's stats sort/cluster together (RowsIn,
// RowsOut, ...). Counters are monotonically increasing by default (unmarked); a
// current-level gauge would take a "Cur" suffix and a high-water mark a "Peak"
// suffix. All counters below are monotonic.
const (
	// scan -> RowsOut.
	StatScanRowsOut = 0

	// filter -> RowsIn, RowsOut (RowsOut/RowsIn is the selectivity).
	StatFilterRowsIn  = 0
	StatFilterRowsOut = 1

	// group / distinct -> RowsIn, GroupsOut (RowsIn/GroupsOut is the fan-in).
	StatGroupRowsIn    = 0
	StatGroupGroupsOut = 1

	// order-offset-limit -> RowsIn, RowsOut (RowsIn is the sort/heap pressure).
	StatOrderRowsIn  = 0
	StatOrderRowsOut = 1

	// joinNL family -> RowsLeft, Probes. Probes counts inner-loop row visits
	// (|left| x |right-per-left|): the "exploding join" work signal -- it spins
	// wildly even when RowsOut stays small. See DESIGN-stats.md.
	StatJoinNLRowsLeft = 0
	StatJoinNLProbes   = 1
)

// joinNLKinds are the op Kinds handled by OpJoinNestedLoop; they all share the
// same counter layout.
var joinNLKinds = []string{
	"joinNL-inner", "joinNL-leftOuter",
	"joinKeys-inner", "joinKeys-leftOuter",
	"nestNL-inner", "nestNL-leftOuter",
	"nestKeys-inner", "nestKeys-leftOuter",
	"unnest-inner", "unnest-leftOuter",
}

func init() {
	// Register the counters each op Kind contributes, so LayoutStats can size
	// the flat counter array. Order defines the offsets above.
	base.StatsDescs["scan"] = []string{"RowsOut"}
	base.StatsDescs["filter"] = []string{"RowsIn", "RowsOut"}
	base.StatsDescs["group"] = []string{"RowsIn", "GroupsOut"}
	base.StatsDescs["distinct"] = []string{"RowsIn", "GroupsOut"}
	base.StatsDescs["order-offset-limit"] = []string{"RowsIn", "RowsOut"}

	for _, k := range joinNLKinds {
		base.StatsDescs[k] = []string{"RowsLeft", "Probes"}
	}
}
