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
	"container/heap" // <== genCompiler:hide
	"math"

	"github.com/couchbase/n1k1/base"
)

// OpOrderOffsetLimit implements ORDER BY and OFFSET and LIMIT.
func OpOrderOffsetLimit(o *base.Op, lzVars *base.Vars, lzYieldVals base.YieldVals,
	lzYieldErr base.YieldErr, path, pathNext string) {
	orders := o.Params[0].([]interface{}) // ORDER BY expressions.

	// The directions has same len as orders, ex: ["asc", "desc", "asc"].
	directions := o.Params[1].([]interface{})

	offset := int64(0)

	limit := int64(math.MaxInt64)

	if len(o.Params) >= 3 {
		if o.Params[2] != nil {
			offset = o.Params[2].(int64)
		}

		if len(o.Params) >= 4 && o.Params[3] != nil {
			limit = o.Params[3].(int64)
		}
	}

	offsetPlusLimit := offset + limit
	if offsetPlusLimit < 0 { // Overflow.
		offsetPlusLimit = math.MaxInt64
	}

	if LzScope {
		pathNextOOL := EmitPush(lzVars, pathNext, "OOL") // !lz

		var lzProjectFunc base.ProjectFunc

		var lzValsLessFunc base.ValsLessFunc

		var lzHeap *base.HeapValsProjected

		if len(orders) > 0 { // !lz
			// The ORDER BY exprs are treated as a projection.
			lzProjectFunc =
				MakeProjectFunc(lzVars, o.Children[0].Labels, orders, pathNextOOL, "PF") // !lz

			lzValsLessFunc =
				MakeValsLessFunc(lzVars, directions, pathNextOOL) // !lz

			lzHeap = base.CreateHeapValsProjected(lzVars.Ctx, lzValsLessFunc)
		}

		_, _, _ = lzProjectFunc, lzValsLessFunc, lzHeap

		var lzEncoded []byte
		var lzExamined int64
		var lzValsPre, lzValsMax base.Vals

		_, _, _, _ = lzEncoded, lzExamined, lzValsPre, lzValsMax

		lzStats := StatsFromVars(lzVars)                        // stats (live) // <== genCompiler:hide
		lzStatsBase := o.StatsBase                              // <== genCompiler:hide
		StatsCounterZero(lzStats, lzStatsBase+StatOrderRowsIn)  // <== genCompiler:hide
		StatsCounterZero(lzStats, lzStatsBase+StatOrderRowsOut) // <== genCompiler:hide
		if lzStats != nil && limit < math.MaxInt64 {            // A LIMIT is a real output-row denominator. // <== genCompiler:hide
			lzStats.Totals[lzStatsBase+StatOrderRowsOut] = limit // <== genCompiler:hide
		} // <== genCompiler:hide

		lzYieldValsOrig := lzYieldVals

		lzYieldVals = func(lzVals base.Vals) {
			StatsCounterBump(lzStats, lzStatsBase+StatOrderRowsIn) // stats: live // <== genCompiler:hide

			if len(orders) > 0 { // !lz
				// If there were ORDER BY exprs, we use the lzHeap.
				lzValsOut := lzValsPre[:0]

				lzValsOut = lzProjectFunc(lzVals, lzValsOut, lzYieldErr) // <== emitCaptured: pathNextOOL "PF"

				lzValsPre = lzValsOut

				// Push onto heap if heap is small or heap is empty or
				// item < max-heap-item.
				lzHeapLen := lzHeap.CurItems

				var lzErr error

				lzNeedPush := lzHeapLen < offsetPlusLimit || lzHeapLen == 0
				if !lzNeedPush {
					var lzMax []byte

					lzMax, lzErr = lzHeap.Get(0)
					if lzErr != nil {
						lzYieldErr(lzErr)
					} else {
						lzValsMax = base.ValsProjectedDecodeProjected(lzMax, lzValsMax[:0])

						lzNeedPush = lzValsLessFunc(lzValsOut, lzValsMax)
					}
				}

				if lzErr == nil && lzNeedPush {
					lzEncoded, lzErr = base.ValsProjectedEncode(lzVals, lzValsOut,
						lzEncoded[:0], lzVars.Ctx.ValComparer)
					if lzErr != nil {
						lzYieldErr(lzErr)
					} else {
						heap.Push(lzHeap, lzEncoded)

						// If heap is too big, pop max-heap-item.
						if lzHeapLen+1 > offsetPlusLimit {
							heap.Pop(lzHeap)
						}

						if lzHeap.Err != nil {
							lzYieldErr(lzHeap.Err)
						}
					}
				}
			} else { // !lz
				if lzExamined >= offset && lzExamined < offsetPlusLimit {
					StatsCounterBump(lzStats, lzStatsBase+StatOrderRowsOut) // stats: live // <== genCompiler:hide

					lzYieldValsOrig(lzVals)
				}

				lzExamined++

				// TODO: No ORDER-BY, but OFFSET+LIMIT reached, so
				// need to early exit via lzVars.Ctx.YieldStats?
			}
		}

		lzYieldErrOrig := lzYieldErr

		lzYieldErr = func(lzErrIn error) {
			if lzErrIn == nil { // If no error, yield our sorted items.
				var lzN int64
				_ = lzN

				if len(orders) > 0 { // !lz
					// Pop off items from the heap, placing them in
					// reverse at end of the heap slots, which results
					// in the end of the heap slots having the needed
					// items correctly sorted in-place.
					lzHeapLen := lzHeap.CurItems

					lzErr := lzHeap.Sort(offset)
					if lzErr != nil {
						lzYieldErrOrig(lzErr)
					}

					for lzI := int64(offset); lzI < lzHeapLen; lzI++ {
						if limit < math.MaxInt64 { // !lz
							if lzN >= limit {
								break
							}
							lzN++
						}

						lzItem, lzErr := lzHeap.Get(lzI)
						if lzErr != nil {
							lzYieldErrOrig(lzErr)
							break
						}

						lzValsPre = base.ValsProjectedDecodeVals(lzItem, lzValsPre[:0])

						StatsCounterBump(lzStats, lzStatsBase+StatOrderRowsOut) // stats: live // <== genCompiler:hide

						lzYieldValsOrig(lzValsPre)
					}

					// TODO: Recycle lzHeap into lzVars.Ctx?
				}
			}

			lzYieldErrOrig(lzErrIn)
		}

		EmitPop(pathNext, "OOL") // !lz

		if LzScope {
			ExecOp(o.Children[0], lzVars, lzYieldVals, lzYieldErr, pathNext, "OOLO") // !lz

			if lzHeap != nil {
				lzHeap.Close()
			}
		}
	}
}

// -----------------------------------------------------

// MakeValsLessFunc returns a ValsLessFunc that compares based on the given
// directions. Each direction is one of "asc"/"desc" (natural nulls collation, via
// ValComparer) or a nulls-qualified form "asc-nulls-first" / "asc-nulls-last" /
// "desc-nulls-first" / "desc-nulls-last" (explicit NULLS FIRST/LAST placement of
// null/missing keys, independent of the sort direction).
func MakeValsLessFunc(lzVars *base.Vars, directions []interface{},
	path string) (lzValsLessFunc base.ValsLessFunc) {
	// TODO: One day use eagerly discovered types to optimize?

	ndirections := len(directions)
	if ndirections > 0 {
		lzAscs := make([]bool, ndirections) // <== varLift: lzAscs by path
		// lzNullsPos[i]: 0 = natural (let ValComparer place null/missing by collation),
		// 1 = NULLS FIRST, 2 = NULLS LAST. Explicit only when the query asked for it, so
		// natural ORDER BY keeps its exact prior behavior (incl. missing < null).
		lzNullsPos := make([]int, ndirections) // <== varLift: lzNullsPos by path
		for directioni, direction := range directions {
			switch direction.(string) {
			case "asc":
				lzAscs[directioni] = true
			case "desc":
			case "asc-nulls-first":
				lzAscs[directioni] = true
				lzNullsPos[directioni] = 1
			case "asc-nulls-last":
				lzAscs[directioni] = true
				lzNullsPos[directioni] = 2
			case "desc-nulls-first":
				lzNullsPos[directioni] = 1
			case "desc-nulls-last":
				lzNullsPos[directioni] = 2
			}
		}

		lzValsLessFunc = func(lzValsA, lzValsB base.Vals) bool {
			var lzCmp int
			var lzANull, lzBNull bool

			_, _ = lzANull, lzBNull

			for idx := range directions { // !lz
				if lzNullsPos[idx] != 0 {
					// Explicit NULLS FIRST/LAST moves ONLY the null-ish group to one side
					// (independent of asc/desc). When exactly one key is null-ish, place it
					// per request. Otherwise (both null-ish, or both non-null) fall through
					// to the collation compare below -- so missing<null holds within the
					// null group, exactly as it does in the natural (no-NULLS) branch.
					lzANull = base.IsNullOrMissing(lzValsA[idx])
					lzBNull = base.IsNullOrMissing(lzValsB[idx])
					if lzANull != lzBNull {
						if lzANull {
							return lzNullsPos[idx] == 1
						}
						return lzNullsPos[idx] == 2
					}
				}

				lzCmp = lzVars.Ctx.ValComparer.Compare(lzValsA[idx], lzValsB[idx])
				if lzCmp < 0 {
					return lzAscs[idx]
				}
				if lzCmp > 0 {
					return !lzAscs[idx]
				}
			}

			return false
		}
	}

	return lzValsLessFunc
}
