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

import (
	"time"

	"github.com/couchbase/n1k1/base"
)

// Checkpoint-cadence pacing bounds (see base.YieldStatsControl.NextEvery). A
// YieldStats callback firing faster than YieldPaceFast apart backs its checkpoint
// interval off; slower than YieldPaceSlow eases it back toward the floor; YieldPaceMax
// caps it. Tuned for a live progress display -- a few updates per second is plenty.
const (
	YieldPaceFast = 40 * time.Millisecond
	YieldPaceSlow = 400 * time.Millisecond
	YieldPaceMax  = 1 << 20 // ~1M rows between checkpoints
)

// YieldPacer adapts a YieldStats checkpoint interval to hold the callback at a
// display-friendly rate: a source racing past (checkpoints arriving faster than a UI
// can absorb) has its interval backed off (1024 -> 2048 -> ...), a slowing source
// eases it back toward the floor. It is exported so an embedder wiring its own
// Ctx.YieldStats can reuse the policy: construct one with NewYieldPacer, and on each
// checkpoint return base.YieldStatsControl{NextEvery: pacer.Next(time.Now())}. Not
// safe for concurrent use -- drive it from the single goroutine that sinks the
// live-stats callback (as glue does). For a stateless rule, call YieldPace directly.
type YieldPacer struct {
	floor int       // the source's initial interval (the lower bound)
	every int       // current suggested interval
	last  time.Time // wall-clock of the previous checkpoint (zero until the first)
}

// NewYieldPacer returns a YieldPacer whose interval starts at (and never drops below)
// floor -- typically the source's initial engine.ScanYieldStatsEvery.
func NewYieldPacer(floor int) *YieldPacer {
	if floor < 1 {
		floor = 1
	}
	return &YieldPacer{floor: floor, every: floor}
}

// Next records a checkpoint at time now and returns the interval the source should
// adopt next (rows until the following checkpoint). The first call only seeds the
// clock, returning the unchanged interval.
func (p *YieldPacer) Next(now time.Time) int {
	if !p.last.IsZero() {
		p.every = YieldPace(p.every, p.floor, now.Sub(p.last))
	}
	p.last = now
	return p.every
}

// YieldPace is the pure re-pacing rule: double cur (capped at YieldPaceMax) when the
// gap dt since the last checkpoint is shorter than YieldPaceFast, halve it toward
// floor when longer than YieldPaceSlow, else keep it. Split out so it unit-tests
// without a clock and an embedder can apply the rule statelessly.
func YieldPace(cur, floor int, dt time.Duration) int {
	switch {
	case dt < YieldPaceFast:
		if n := cur * 2; n <= YieldPaceMax {
			return n
		}
		return YieldPaceMax
	case dt > YieldPaceSlow:
		if n := cur / 2; n >= floor {
			return n
		}
		return floor
	}
	return cur
}

// datastoreScanKinds are the DatastoreOp kinds that read rows from the datastore
// and so contribute a RowsOut counter. (Non-scan kinds handled by DatastoreOp --
// datastore-fetch, expr-scan, project-exclude, with-recursive -- are not counted;
// they get no section and countingYield leaves their yield untouched.) These scan
// ops live in glue (they read the file datastore), so unlike the engine's own ops
// they register here rather than in engine/stats.go -- but via the same
// base.DefStat convention, so `git grep DefStat` still lists them.
var datastoreScanKinds = []string{
	"datastore-scan-records",
	"datastore-scan-primary",
	"datastore-scan-index",
	"datastore-scan-index-cover",
	"datastore-scan-fts",
	"datastore-scan-keys",
}

// StatDatastoreScanRowsOut is the offset of the RowsOut counter within a
// datastore-scan op's section. See DESIGN-stats.md.
var StatDatastoreScanRowsOut = base.DefStat("RowsOut", "rows emitted to the parent", datastoreScanKinds...)

// DatastoreScanYieldStatsEvery is how many rows a datastore scan yields between
// live YieldStats checkpoints. Unlike the engine's OpScan, these scans have no
// built-in per-row checkpoint, so countingYield drives one here to feed live
// progress display. Mirrors engine.ScanYieldStatsEvery.
var DatastoreScanYieldStatsEvery = 1024

// countingYield wraps yieldVals to (1) count rows out of a datastore scan into the
// op's RowsOut slot and (2) fire the live YieldStats checkpoint every
// DatastoreScanYieldStatsEvery rows -- these scans lack the engine's per-row
// checkpoint, so without this a file-datastore query would show no live progress.
// It returns yieldVals unchanged when stats are off (Ctx.Stats == nil) or the op
// isn't a counter-contributing scan (StatsBase < 0), so those paths pay nothing.
// The scan's drain runs on the single query goroutine, so the slot has one writer
// and needs no atomics. YieldStats's early-exit error is ignored here (LIMIT is
// already enforced by the scan's own limit); the checkpoint is display-only.
func countingYield(o *base.Op, vars *base.Vars, yieldVals base.YieldVals) base.YieldVals {
	if vars == nil || vars.Ctx == nil || vars.Ctx.Stats == nil || o.StatsBase < 0 {
		return yieldVals
	}

	stats := vars.Ctx.Stats
	counters := stats.Counters
	totals := stats.Totals
	slot := o.StatsBase + StatDatastoreScanRowsOut
	yieldStats := vars.Ctx.YieldStats
	every := int64(DatastoreScanYieldStatsEvery)

	// n is per-invocation: DatastoreOp (hence this closure) is re-entered for each
	// scan invocation, so for a nested-loop join's inner scan n resets to 0 every
	// outer row -- RowsOut naturally counts the *current* inner pass, a bar that
	// resets each iteration. The denominator (Totals) is the peak pass size seen
	// so far, which lives in the shared array and thus persists across invocations,
	// giving that resetting bar a stable 0..peak scale. A scan run once (a plain
	// top-level scan) simply ends at RowsOut == peak == its row count.
	var n int64

	return func(vals base.Vals) {
		n++
		counters[slot] = n

		if totals != nil && n > totals[slot] {
			totals[slot] = n
		}

		if yieldStats != nil && n%every == 0 {
			vars.Ctx.RunningAggsRefresh() // refresh THIS actor's live aggregate partials

			ctl := yieldStats(stats)
			if ctl.NextEvery > 0 { // re-pace the checkpoint (dynamic cadence)
				every = int64(ctl.NextEvery)
			}
			// ctl.Stop is ignored here: LIMIT is enforced by the scan's own limit, and
			// a cooperative halt (closed output pipe) is future work.
		}

		yieldVals(vals)
	}
}
