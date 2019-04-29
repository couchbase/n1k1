package n1k1

import (
	"container/heap" // <== genCompiler:hide
	"math"

	"github.com/couchbase/n1k1/base"
)

var InitPreallocVals = 16
var InitPreallocVal = 4096

func OpOrderByOffsetLimit(o *base.Op, lzYieldVals base.YieldVals,
	lzYieldStats base.YieldStats, lzYieldErr base.YieldErr, path, pathNext string) {
	projections := o.Params[0].([]interface{}) // ORDER BY expressions.

	// The directions has same len as projections, ex: ["asc", "desc", "asc"].
	directions := o.Params[1].([]interface{})

	offset := 0

	limit := math.MaxInt64

	if len(o.Params) >= 3 {
		offset = o.Params[2].(int)

		if len(o.Params) >= 4 {
			limit = o.Params[3].(int)
		}
	}

	offsetPlusLimit := offset + limit
	if offsetPlusLimit < 0 { // Overflow.
		offsetPlusLimit = math.MaxInt64
	}

	if LzScope {
		pathNextOOL := EmitPush(pathNext, "OOL") // !lz

		var lzProjectFunc base.ProjectFunc
		var lzLessFunc base.LessFunc

		if len(projections) > 0 { // !lz
			lzProjectFunc =
				MakeProjectFunc(o.ParentA.Fields, nil, projections, pathNextOOL, "PF") // !lz

			lzLessFunc =
				MakeLessFunc(nil, directions) // !lz
		} // !lz

		// Used when there are ORDER-BY exprs.
		lzHeap := &base.HeapValsProjected{nil, lzLessFunc}

		// Used when there are no ORDER-BY exprs.
		var lzItems []base.Vals

		_, _, _, _ = lzProjectFunc, lzLessFunc, lzHeap, lzItems

		var lzPreallocVals base.Vals
		var lzPreallocVal base.Val
		var lzPreallocProjected base.Vals

		_, _, _ = lzPreallocVals, lzPreallocVal, lzPreallocProjected

		lzYieldValsOrig := lzYieldVals

		lzYieldVals = func(lzVals base.Vals) {
			var lzValsCopy base.Vals

			lzValsCopy, lzPreallocVals, lzPreallocVal = base.ValsDeepCopy(lzVals, lzPreallocVals, lzPreallocVal)

			if len(projections) > 0 { // !lz
				lzValsOut := lzPreallocProjected

				lzPreallocProjected = nil

				lzVals = lzValsCopy

				lzValsOut = lzProjectFunc(lzVals, lzValsOut) // <== emitCaptured: pathNextOOL "PF"

				heap.Push(lzHeap, base.ValsProjected{lzValsCopy, lzValsOut})

				if lzHeap.Len() > offsetPlusLimit {
					lzOld := heap.Pop(lzHeap).(base.ValsProjected)

					lzPreallocVals = lzOld.Vals[0:cap(lzOld.Vals)]

					lzPreallocVal = lzOld.Vals[0]
					lzPreallocVal = lzPreallocVal[0:cap(lzPreallocVal)]

					lzPreallocProjected = lzOld.Projected[:0]
				}
			} else { // !lz
				lzItems = append(lzItems, lzValsCopy)

				// TODO: No ORDER-BY, but OFFSET+LIMIT reached, early exit via lzYieldStats?
			} // !lz
		}

		lzYieldErrOrig := lzYieldErr

		lzYieldErr = func(lzErrIn error) {
			if lzErrIn == nil { // If no error, yield our sorted items.
				var lzN int
				_ = lzN

				if len(projections) > 0 { // !lz
					lzHeapLen := lzHeap.Len()

					lzValsProjected := lzHeap.ValsProjected

					for lzJ := lzHeapLen - 1; lzJ >= offset; lzJ-- {
						lzValsProjected[lzJ] = heap.Pop(lzHeap).(base.ValsProjected)
					}

					lzHeap.ValsProjected = lzValsProjected

					for lzI := offset; lzI < lzHeapLen; lzI++ {
						if limit < math.MaxInt64 { // !lz
							if lzN >= limit {
								break
							}
							lzN++
						} // !lz

						lzYieldValsOrig(lzHeap.GetVals(lzI))
					}
				} else { // !lz
					lzItemsLen := len(lzItems)

					for lzI := offset; lzI < lzItemsLen; lzI++ {
						if limit < math.MaxInt64 { // !lz
							if lzN >= limit {
								break
							}
							lzN++
						} // !lz

						lzYieldValsOrig(lzItems[lzI])
					}
				} // !lz
			}

			lzYieldErrOrig(lzErrIn)
		}

		EmitPop(pathNext, "OOL") // !lz

		if LzScope {
			ExecOp(o.ParentA, lzYieldVals, lzYieldStats, lzYieldErr, path, pathNext) // !lz
		}
	}
}

func MakeLessFunc(types base.Types, directions []interface{}) (
	lzLessFunc base.LessFunc) {
	// TODO: One day use types to optimize.

	if len(directions) > 0 {
		lzValComparer := base.NewValComparer()

		lzLessFunc = func(lzValsA, lzValsB base.Vals) bool {
			var lzCmp int

			for idx := range directions { // !lz
				direction := directions[idx] // !lz

				lt, gt := true, false                               // !lz
				if s, ok := direction.(string); ok && s == "desc" { // !lz
					lt, gt = false, true // !lz
				} // !lz

				lzCmp = lzValComparer.Compare(lzValsA[idx], lzValsB[idx])
				if lzCmp < 0 {
					return lt
				}

				if lzCmp > 0 {
					return gt
				}
			} // !lz

			return false
		}
	}

	return lzLessFunc
}
