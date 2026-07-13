//  Copyright (c) 2019 Couchbase, Inc.
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
	"github.com/couchbase/rhmap/store" // <== genCompiler:hide

	"github.com/couchbase/n1k1/base"
)

// OpGroup implements GROUP BY and DISTINCT.
//
// Ex: SELECT SUM(sales), MIN(sales), COUNT(*) ... GROUP BY state, city.
func OpGroup(o *base.Op, lzVars *base.Vars, lzYieldVals base.YieldVals,
	lzYieldErr base.YieldErr, path, pathNext string) {
	// GROUP BY exprs.
	// Ex: ["labelPath",".","state"], ["labelPath",".","city"].
	// The len(groupExprs) >= 1.
	groupExprs := o.Params[0].([]interface{})

	var aggExprs, aggCalcs []interface{}

	if len(o.Params) > 1 {
		// Aggregation exprs.
		// Ex: ["labelPath",".","sales"], ["labelPath","."].
		// The len(aggExprs) >= 0.
		aggExprs = o.Params[1].([]interface{})
		_ = aggExprs

		// Aggregation calcs.
		// Ex: ["sum","min"], ["count"].
		// The len(aggExprs) == len(aggFuncs).
		aggCalcs = o.Params[2].([]interface{})
		_ = aggCalcs
	}

	if LzScope {
		pathNextG := EmitPush(lzVars, pathNext, "G") // !lz

		var groupProjectFunc, aggProjectFunc base.ProjectFunc // !lz

		groupProjectFunc =
			MakeProjectFunc(lzVars, o.Children[0].Labels, groupExprs, pathNextG, "GP") // !lz

		if len(aggExprs) > 0 { // !lz
			aggProjectFunc =
				MakeProjectFunc(lzVars, o.Children[0].Labels, aggExprs, pathNextG, "AP") // !lz
		}

		_, _ = groupProjectFunc, aggProjectFunc

		// TODO: Configurable initial size for rhstore, and reusable rhstore.
		// TODO: Reuse backing bytes for lzSet.
		lzSet, lzErr := lzVars.Ctx.AllocMap()
		if lzErr != nil {
			lzYieldErr(lzErr)
		}

		var lzValOut base.Val

		var lzValsOut base.Vals

		var lzGroupKey, lzGroupVal, lzGroupValNew, lzGroupValReuse, lzGroupValPrev []byte

		var lzGroupKeyFound bool

		var lzAgg *base.Agg

		_, _, _, _, _ = lzValOut, lzGroupValNew, lzGroupValReuse, lzAgg, lzGroupValPrev

		// Pre-resolve each aggregate to its base.Aggs index once. aggCalcs is
		// fixed for the query, so the base.AggCatalog[name] map lookup + the
		// name.(string) type-assert the Init/Update loops below used to do PER
		// INPUT ROW (here x an N-way cross-join) is pure overhead -- in the
		// interp lane it ran on every one of the millions of rows. lzAggIdxs
		// mirrors aggCalcs' [calc][name] shape and is consulted with a plain
		// slice index instead. (!lz: gen-time in the compiled lane, which bakes
		// the index in as a constant regardless, so the emitted per-row code is
		// unchanged -- this only removes the interp lane's per-row map lookup.)
		var lzAggIdxs [][]int              // !lz
		for _, aggCalc := range aggCalcs { // !lz
			var lzAggIdxRow []int                             // !lz
			for _, aggName := range aggCalc.([]interface{}) { // !lz
				lzAggIdxRow = append(lzAggIdxRow, base.AggCatalog[aggName.(string)]) // !lz
			}
			lzAggIdxs = append(lzAggIdxs, lzAggIdxRow) // !lz
		}
		_ = lzAggIdxs // !lz

		lzStats := StatsFromVars(lzVars)                          // stats (live) // <== genCompiler:hide
		lzStatsBase := o.StatsBase                                // <== genCompiler:hide
		StatsCounterZero(lzStats, lzStatsBase+StatGroupRowsIn)    // <== genCompiler:hide
		StatsCounterZero(lzStats, lzStatsBase+StatGroupGroupsOut) // <== genCompiler:hide

		// Live-aggregate running-aggregate (DESIGN-stats.md "Live aggregates"): register a
		// refresher that, at each synchronous YieldStats checkpoint, snapshots a
		// bounded sample of the in-flight group map -- decoding each partial via
		// the same base.Agg.Result byte-path the final result uses -- so the CLI /
		// a library reader can watch COUNT/SUM/AVG/MIN/MAX climb. Interpreter-only
		// (genCompiler:hide), zero hot-path cost (the per-row loop is untouched),
		// and only when EVERY projected aggregate is cheaply runningCapable (else
		// StatsGroupAggNames returns nil -> progress-only). Registered on THIS
		// actor's own Ctx, keyed to this op's fixed Stats.RunningAggs slot
		// (o.RunningAggSlot), so a GROUP BY inside each parallel UNION ALL branch is a
		// single writer of its own slot -- no cross-actor race. lzRunningAggLive is
		// flipped off before RecycleMap so a later checkpoint can't walk a reclaimed
		// map. (base.GroupRunningAggs does the store-walk; base imports store,
		// engine intermed does not, so no store symbol leaks into the compiled build.)
		lzRunningAggNames := StatsGroupAggNames(aggCalcs)                        // <== genCompiler:hide
		lzRunningAggLive := false                                                // <== genCompiler:hide
		if lzStats != nil && lzRunningAggNames != nil && o.RunningAggSlot >= 0 { // <== genCompiler:hide
			lzRunningAggLive = true                                                         // <== genCompiler:hide
			lzVars.Ctx.RunningAggRegister(o.RunningAggSlot, func(lzDst *base.RunningAggs) { // <== genCompiler:hide
				if lzRunningAggLive { // <== genCompiler:hide
					base.RunningAggsGroup(lzDst, path, lzSet, lzRunningAggNames, lzVars) // <== genCompiler:hide
				} // <== genCompiler:hide
			}) // <== genCompiler:hide
		} // <== genCompiler:hide

		lzYieldValsOrig := lzYieldVals

		lzYieldVals = func(lzVals base.Vals) {
			StatsCounterBump(lzStats, lzStatsBase+StatGroupRowsIn) // stats: live // <== genCompiler:hide

			// Compute the group key. With no GROUP BY (empty groupExprs), this
			// is the canonical encoding of zero vals -- one constant key -- so
			// all input rows fold into a single group, i.e. one output row of
			// the aggregates.
			lzValsOut = lzValsOut[:0]

			lzValsOut = groupProjectFunc(lzVals, lzValsOut, lzYieldErr) // <== emitCaptured: pathNextG "GP"

			lzGroupKey, lzErr = base.ValsEncodeCanonical(lzValsOut,
				lzGroupKey[:0], lzVars.Ctx.ValComparer)

			if lzErr == nil {
				// Check if we've seen the group key before or not.
				lzGroupVal, lzGroupKeyFound = lzSet.Get(lzGroupKey)

				// Remember the stored value slice (a mutable view into the map's
				// backing bytes) before the Update loop below reassigns lzGroupVal to
				// each agg's post-consume remainder -- so a same-size update can
				// overwrite in place instead of re-Set'ing (append into the map's
				// never-reclaimed value heap) on every row.
				lzGroupValPrev = lzGroupVal

				if len(aggExprs) > 0 { // !lz
					if !lzGroupKeyFound {
						// We have aggregate exprs on a newly seen
						// group key, so initialize the agg data
						// structures for this new group key.
						lzGroupVal = lzGroupValReuse[:0]

						for aggCalcI := range aggCalcs { // !lz
							for aggNameI := range aggCalcs[aggCalcI].([]interface{}) { // !lz
								aggIdx := lzAggIdxs[aggCalcI][aggNameI] // !lz
								lzAgg = base.Aggs[aggIdx]

								lzGroupVal = lzAgg.Init(lzVars, lzGroupVal)
							}
						}

						lzGroupValReuse = lzGroupVal[:0]
					}

					// Project the aggregate exprs from the tuple.
					lzValsOut = lzValsOut[:0]

					lzValsOut = aggProjectFunc(lzVals, lzValsOut, lzYieldErr) // <== emitCaptured: pathNextG "AP"

					lzGroupValNew = lzGroupValNew[:0]

					var lzGroupValChanged, lzChanged bool

					// Use the projected aggregate exprs to update
					// the agg data structures.
					for aggCalcI := range aggCalcs { // !lz
						for aggNameI := range aggCalcs[aggCalcI].([]interface{}) { // !lz
							aggIdx := lzAggIdxs[aggCalcI][aggNameI] // !lz
							lzAgg = base.Aggs[aggIdx]

							lzGroupValNew, lzGroupVal, lzChanged = lzAgg.Update(lzVars,
								lzValsOut[aggCalcI], lzGroupValNew, lzGroupVal, lzVars.Ctx.ValComparer)

							lzGroupValChanged = lzGroupValChanged || lzChanged
						}
					}

					if lzGroupKeyFound {
						if lzGroupValChanged {
							// With a previously seen group key, overwrite the stored
							// agg bytes in place when the new agg data is exactly the
							// same size (always true for fixed-width aggs like count /
							// sum / avg) -- avoiding a re-Set that appends a fresh copy
							// into the map's append-only value heap on every row. A
							// different size (a growing variable-width agg) falls back
							// to Set, which records the new length; an in-place copy of
							// a shorter value would leave a stale tail the reader can't
							// distinguish, since the map tracks the value size itself.
							if len(lzGroupValPrev) == len(lzGroupValNew) {
								copy(lzGroupValPrev, lzGroupValNew)
							} else {
								lzSet.Set(lzGroupKey, lzGroupValNew)
							}
						}
					} else {
						// We fall thru to the below lzSet.Set().
						lzGroupVal = lzGroupValNew
					}
				}

				if !lzGroupKeyFound {
					lzSet.Set(lzGroupKey, lzGroupVal)
				}
			}
		}

		lzYieldErrOrig := lzYieldErr

		lzYieldErr = func(lzErrIn error) {
			if lzErrIn == nil { // If no error, yield our group items.
				lzSetVisitor := func(lzGroupKey store.Key, lzGroupVal store.Val) bool {
					lzValsOut = base.ValsDecode(lzGroupKey, lzValsOut[:0])

					if len(aggExprs) > 0 { // !lz
						// If we have aggregate exprs, append their
						// accummulated results to the yielded vals.
						lzValBuf := lzValOut[:cap(lzValOut)]

						lzValBytes := 0

						var lzVal base.Val

						for _, aggCalc := range aggCalcs { // !lz
							for _, aggName := range aggCalc.([]interface{}) { // !lz
								aggIdx := base.AggCatalog[aggName.(string)] // !lz
								lzAgg = base.Aggs[aggIdx]

								lzVal, lzGroupVal, lzValBuf = lzAgg.Result(lzVars, lzGroupVal, lzValBuf)

								lzValsOut = append(lzValsOut, lzVal)

								lzValBytes += len(lzVal)
							}
						}

						if cap(lzValOut) < lzValBytes {
							lzValOut = make(base.Val, lzValBytes)
						}
					}

					StatsCounterBump(lzStats, lzStatsBase+StatGroupGroupsOut) // stats: live // <== genCompiler:hide

					lzYieldValsOrig(lzValsOut)

					return true
				}

				lzSet.Visit(lzSetVisitor)
			}

			lzYieldErrOrig(lzErrIn)
		}

		EmitPop(pathNext, "G") // !lz

		if lzErr == nil {
			ExecOp(o.Children[0], lzVars, lzYieldVals, lzYieldErr, pathNext, "GO") // !lz
		}

		lzRunningAggLive = false // stats: disable running-aggregate before the map is reclaimed // <== genCompiler:hide

		lzVars.Ctx.RecycleMap(lzSet)
	}
}
