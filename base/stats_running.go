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
	"github.com/couchbase/rhmap/store"
)

// This file implements the "Live aggregates: partials that climb" design in
// DESIGN-stats.md: exposing an in-flight, partial view of a GROUP BY / ungrouped
// aggregate's accumulators (COUNT/SUM/AVG/MIN/MAX climbing toward their finals)
// while a query runs -- with NO per-row cost and NO per-snapshot allocation in
// steady state.
//
// The hot path is untouched: aggregates keep folding bytes into their fixed-width
// accumulators exactly as before (base/agg.go, engine/op_group.go). A blocking op
// (OpGroup) registers a refresher on its actor's Ctx once at setup
// (Ctx.RegisterRunningAgg); at the existing synchronous YieldStats checkpoint
// (op_scan.go / glue countingYield, on the exec goroutine, between row yields so no
// Agg.Update is mid-flight) that actor calls Ctx.RefreshRunningAggs, walking a bounded
// sample of ITS OWN live group map and decoding each partial via the SAME
// base.Agg.Result byte-path that produces the finalized "^aggregates|..." value --
// into ONE reused buffer per row, in this op's own fixed Stats.RunningAggs slot (so
// parallel UNION ALL branches never collide). So the last live partial is bit-identical to the
// final result (they are the same Result call), and after warm-up a snapshot of
// fixed-width aggregates allocates nothing.

// RunningAggMaxGroups bounds how many groups a single checkpoint snapshots (first-N),
// keeping the ~10 Hz reader O(N) rather than O(groups). Ungrouped aggregates have
// exactly one group, so they are always fully covered.
var RunningAggMaxGroups = 64

// RunningAggsCapable is the set of aggregate handler names whose partial value is
// cheap to decode live: fixed-width (or trivially self-describing) accumulators
// whose Agg.Result is O(1)/O(value-len) and allocation-free. DISTINCT-based aggs,
// ARRAY_AGG, MEDIAN, VARIANCE/STDDEV are deliberately excluded -- their Result
// re-walks or allocates a []float64, so they stay progress-only (see
// DESIGN-stats.md's per-aggregate table). The vectorized _v forms reuse the
// scalar Result verbatim, so they are runningCapable too.
var RunningAggsCapable = map[string]bool{
	"count": true, "countn": true, "count_v": true,
	"sum": true, "sum_v_float64": true, "sum_v_int64": true,
	"avg": true, "avg_v_float64": true, "avg_v_int64": true,
	"min": true, "max": true,
}

// IsRunningAggCapable reports whether the named aggregate's partial value can be shown
// live cheaply (fixed-width, no per-snapshot alloc). OpGroup only registers a
// running-aggregate refresher when every aggregate in the group is runningCapable -- a clean
// carve-out that also avoids having to walk past a costly agg's variable-width
// bytes just to reach a later cheap one.
func IsRunningAggCapable(name string) bool { return RunningAggsCapable[name] }

// A blocking op's live partial-aggregate state is snapshotted by a refresher
// (Ctx.runningAggJob.fill, a func(*RunningAggs)) registered at op setup via
// Ctx.RegisterRunningAgg and invoked synchronously at the op's actor's YieldStats
// checkpoint (Ctx.RefreshRunningAggs), so it reads coherent (non-mutating)
// accumulator bytes. It copies out / decodes a bounded sample into its per-op
// RunningAggs buffer (reused across checkpoints), appending rows via dst.Next(), and
// retains no reference to the live group map after it returns (copy-out for
// RecycleMap safety). GroupRunningAggs below is the OpGroup refresher.

// RunningAggRow is one group's in-flight partial aggregate values, captured at a
// checkpoint: the decoded group-by key vals (empty for an ungrouped aggregate)
// and each projected aggregate's current partial rendered by Agg.Result -- the
// same bytes the finalized result row yields under its "^aggregates|..." label.
//
// The unexported buffers (keyBuf/aggBuf/aggOff) are reused across checkpoints so
// re-decoding a fixed-width row allocates nothing once warmed up. Key aliases
// keyBuf and each Aggs entry aliases aggBuf; both are stable until the row's next
// begin(), so a synchronous reader (or a JSON serializer) may read them in place.
type RunningAggRow struct {
	Op    string   // op id (codegen path) that produced this row
	Key   Vals     // decoded group-by key vals (nil/empty for ungrouped)
	Names []string // aggregate handler names aligned with Aggs (e.g. "sum","avg")
	Aggs  []Val    // partial value per projected aggregate, in projection order

	keyBuf []byte
	aggBuf []byte
	aggOff []int
}

func (r *RunningAggRow) begin(op string) {
	r.Op = op
	r.keyBuf = r.keyBuf[:0]
	r.Key = r.Key[:0]
	r.aggBuf = r.aggBuf[:0]
	r.aggOff = r.aggOff[:0]
	r.Names = r.Names[:0]
	r.Aggs = r.Aggs[:0]
}

// SetKey copies the raw group-key bytes out of the live map and decodes them, so
// the row retains no pointer into the map (RecycleMap safe).
func (r *RunningAggRow) SetKey(keyBytes []byte) {
	r.keyBuf = append(r.keyBuf, keyBytes...)
	r.Key = ValsDecode(r.keyBuf, r.Key[:0])
}

// AggBufTail hands the caller the unused tail of this row's reused aggregate
// buffer, to pass as Agg.Result's scratch -- so Result appends into aggBuf
// itself when it fits (no alloc), mirroring OpGroup's lzValBuf discipline.
func (r *RunningAggRow) AggBufTail() []byte { return r.aggBuf[len(r.aggBuf):] }

// AddAgg records one decoded partial (name + value). v may alias the tail from
// AggBufTail (the common fit case, a same-to-same copy) or a freshly grown slice
// (first snapshots); either way the value is copied into aggBuf and referenced by
// a stable offset, so a later append can't dangle an earlier value.
func (r *RunningAggRow) AddAgg(name string, v Val) {
	r.aggOff = append(r.aggOff, len(r.aggBuf))
	r.aggBuf = append(r.aggBuf, v...)
	r.Names = append(r.Names, name)
}

func (r *RunningAggRow) finish() {
	r.aggOff = append(r.aggOff, len(r.aggBuf)) // end sentinel
	r.Aggs = r.Aggs[:0]
	for i := 0; i+1 < len(r.aggOff); i++ {
		r.Aggs = append(r.Aggs, Val(r.aggBuf[r.aggOff[i]:r.aggOff[i+1]]))
	}
}

// RunningAggs is the per-request, reused buffer for the latest live-aggregate
// snapshot. It owns a growable pool of RunningAggRow (rows), each with its own
// reused byte buffers; a snapshot resets the live count to zero and re-fills via
// Next(), so steady-state re-decoding allocates nothing. Held on Stats.RunningAggs.
type RunningAggs struct {
	rows []RunningAggRow // backing pool, reused across checkpoints
	n    int             // number of live rows filled this snapshot
}

// Next returns the next reusable RunningAggRow for a source to fill (its buffers
// reset but their capacity retained). Grows the pool only until it reaches the
// steady-state row count.
func (p *RunningAggs) Next(op string) *RunningAggRow {
	if p.n == len(p.rows) {
		p.rows = append(p.rows, RunningAggRow{})
	}
	r := &p.rows[p.n]
	p.n++
	r.begin(op)
	return r
}

// FinishRow is called by a source after it has added all of a row's aggregates,
// materializing the row's Aggs slices from the recorded offsets.
func (p *RunningAggs) FinishRow(r *RunningAggRow) { r.finish() }

// Full reports whether the snapshot has reached RunningAggMaxGroups live rows, so a
// source can stop walking the group map early.
func (p *RunningAggs) Full() bool { return p.n >= RunningAggMaxGroups }

func (p *RunningAggs) reset() { p.n = 0 }

// Rows returns the live rows filled by the last RefreshRunningAggs. Valid until the
// next refresh; a retaining consumer must copy.
func (p *RunningAggs) Rows() []RunningAggRow {
	if p == nil {
		return nil
	}
	return p.rows[:p.n]
}

// RunningAggsGroup copies out a bounded, coherent sample of an OpGroup's live
// accumulators into dst, decoding each partial via the same Agg.Result byte-path
// the finalized result uses. It runs synchronously at the YieldStats checkpoint
// (the map is not mutating), and retains no pointer into the map -- every key and
// aggregate byte is copied into dst's reused per-row buffers -- so it is safe
// against a later RecycleMap. aggNames is the flat, in-layout-order list of
// runningCapable aggregate handler names (from engine.StatsGroupAggNames); vars is
// passed through to Agg.Result (the runningCapable aggs ignore it). Allocation-free
// in steady state: dst and its rows reuse their buffers across checkpoints.
//
// This lives in base (not engine) because it references *store.RHStore, and base
// -- unlike the engine intermed builder -- already imports rhmap/store.
func RunningAggsGroup(dst *RunningAggs, op string, set *store.RHStore, aggNames []string, vars *Vars) {
	if dst == nil || set == nil || len(aggNames) == 0 {
		return
	}

	set.Visit(func(key store.Key, val store.Val) bool {
		if dst.Full() {
			return false // Bounded sample reached; stop walking.
		}

		r := dst.Next(op)
		r.SetKey([]byte(key)) // Copy-out + decode the group key.

		agg := []byte(val)
		for _, name := range aggNames {
			a := Aggs[AggCatalog[name]]

			// Result appends the partial into the row's own reused aggBuf tail
			// (no alloc once warm) and returns the unread remainder so we can walk
			// to the next aggregate's bytes.
			v, aggRest, _ := a.Result(vars, agg, r.AggBufTail())
			agg = aggRest
			r.AddAgg(name, v)
		}

		dst.FinishRow(r)

		return true
	})
}
