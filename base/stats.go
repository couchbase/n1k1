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

package base

import (
	"strconv"
)

// This file implements the counter core described in DESIGN-stats.md
// ("A concrete counter core: one flat []int64 keyed by op id"): every op
// contributes a known set of int64 counters, all counters live in one big,
// pre-sized []int64, and an index maps a human-readable "opId:statName" to a
// slot. Each op owns a contiguous section (at Op.StatsBase); the hot path only
// bumps counters[base+offset] into its own single-writer section, so no atomics
// are needed today (see DESIGN-stats.md for the per-(op,actor) extension for
// when same-op parallelism lands).

// StatsDescs declares, per op Kind, the ordered names of the int64 counters that
// kind contributes. Engine ops register their names here at init time; the
// layout pass (LayoutStats) consults it to size the flat counter array. The
// ordering is the contract: an op's Nth registered stat lives at
// Counters[op.StatsBase+N], which is the constant offset the hot path uses.
var StatsDescs = map[string][]string{}

// StatsOpInfo describes one operator's section of the flat counter array, for
// human-readable attribution at report time (e.g. rendering counters next to a
// plan-tree node).
type StatsOpInfo struct {
	Id    string   // Synthetic, stable op id (tree path), e.g. "0/1/0".
	Kind  string   // The op's Kind.
	Base  int      // Offset of this op's first counter within Counters.
	Names []string // Ordered stat names; Names[i] lives at Counters[Base+i].
}

// Stats is a request's counter core: one flat, pre-sized []int64 shared by all
// operators (each owns a contiguous section starting at its Op.StatsBase), plus
// an index for attributing a counter back to an "opId:statName". It is sized
// once by LayoutStats at request setup; the hot path only bumps
// Counters[base+offset]. A nil *Stats means stats are off, and ops skip all
// counter work (the default, zero-cost path).
//
// Stats lives on the per-request Ctx (Ctx.Stats), which is shared across every
// actor's cloned Vars (Ctx.Clone copies the pointer), so all actors bump into
// one backing array. Reading Counters concurrently with hot-path writers yields
// per-field skew only; the counters are monotonic, which is fine for progress
// (see DESIGN-stats.md "Open questions").
type Stats struct {
	Counters []int64
	Index    map[string]int // "opId:statName" -> index into Counters.
	Ops      []StatsOpInfo  // Per-op sections, in layout (pre-order) order.
}

// LayoutStats walks the op tree once (pre-order), assigns each op the base
// offset of its counter section (stored in Op.StatsBase), sizes the flat counter
// array, and builds the attribution index. It runs at request setup, off the hot
// path. Ops that contribute no counters get a StatsBase of -1. Returns nil if no
// op in the tree contributes any counter (stats then stay off).
func LayoutStats(root *Op) *Stats {
	s := &Stats{Index: map[string]int{}}

	var walk func(o *Op, id string)

	walk = func(o *Op, id string) {
		if o == nil {
			return
		}

		names := StatsDescs[o.Kind]
		if len(names) > 0 {
			o.StatsBase = len(s.Counters)

			for i, name := range names {
				s.Index[id+":"+name] = o.StatsBase + i
			}

			s.Ops = append(s.Ops, StatsOpInfo{
				Id: id, Kind: o.Kind, Base: o.StatsBase, Names: names,
			})

			s.Counters = append(s.Counters, make([]int64, len(names))...)
		} else {
			o.StatsBase = -1 // Contributes no counters.
		}

		for ci, child := range o.Children {
			walk(child, id+"/"+strconv.Itoa(ci))
		}
	}

	walk(root, "0")

	if len(s.Counters) == 0 {
		return nil
	}

	return s
}

// Get returns the counter value for the given "opId:statName" key, or (0, false)
// if the key is unknown. For report-side attribution, not the hot path.
func (s *Stats) Get(key string) (int64, bool) {
	if s == nil {
		return 0, false
	}

	i, ok := s.Index[key]
	if !ok {
		return 0, false
	}

	return s.Counters[i], true
}
