package n1k1

import (
	"container/heap" // <== genCompiler:hide
	"math"

	"github.com/couchbase/n1k1/base"
)

func OpOrderOffsetLimit(o *base.Op, lzVars *base.Vars, lzYieldVals base.YieldVals,
	lzYieldErr base.YieldErr, path, pathNext string) {
	orders := o.Params[0].([]interface{}) // ORDER BY expressions.

	// The directions has same len as orders, ex: ["asc", "desc", "asc"].
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

		if len(orders) > 0 { // !lz
			// The ORDER BY exprs are treated as a projection.
			lzProjectFunc =
				MakeProjectFunc(lzVars, o.Children[0].Labels, orders, pathNextOOL, "PF") // !lz

			lzLessFunc =
				MakeLessFunc(lzVars, directions) // !lz
		} // !lz

		// Used when there are ORDER-BY exprs.
		lzHeap := &base.HeapValsProjected{nil, lzLessFunc}

		// Used when there are no ORDER-BY exprs.
		var lzItems []base.Vals

		var lzPreallocVals base.Vals
		var lzPreallocVal base.Val
		var lzPreallocProjected base.Vals

		_, _, _, _ = lzProjectFunc, lzHeap, lzItems, lzPreallocProjected

		lzYieldValsOrig := lzYieldVals

		lzYieldVals = func(lzVals base.Vals) {
			// Deep copy the incoming lzVals, because unlike other
			// "stateless" operators, we hold onto the vals in the
			// lzHeap/lzItems for sorting.
			var lzValsCopy base.Vals

			lzValsCopy, lzPreallocVals, lzPreallocVal = base.ValsDeepCopy(lzVals, lzPreallocVals, lzPreallocVal)

			if len(orders) > 0 { // !lz
				// If there were ORDER BY exprs, we use the lzHeap.
				lzValsOut := lzPreallocProjected[:0]

				lzPreallocProjected = nil

				lzVals = lzValsCopy

				lzValsOut = lzProjectFunc(lzVals, lzValsOut, lzYieldErr) // <== emitCaptured: pathNextOOL "PF"

				lzHeapLen := lzHeap.Len()
				if lzHeapLen < offsetPlusLimit || lzHeapLen == 0 || lzLessFunc(lzValsOut, lzHeap.GetProjected(0)) {
					// Push onto heap if heap is small or heap is empty or item < max-heap-item.
					heap.Push(lzHeap, base.ValsProjected{lzValsCopy, lzValsOut})

					// If heap is too big (> offset+limit), then recycle max-heap-item.
					if lzHeapLen+1 > offsetPlusLimit {
						lzFormerMax := heap.Pop(lzHeap).(base.ValsProjected)

						lzPreallocVals = lzFormerMax.Vals[0:cap(lzFormerMax.Vals)]

						lzPreallocVal = lzFormerMax.Vals[0]
						lzPreallocVal = lzPreallocVal[0:cap(lzPreallocVal)]

						// TODO: Recycle each val in lzFormerMax.Projected into lzVars.Ctx?

						lzPreallocProjected = lzFormerMax.Projected[:0]
					}
				}
			} else { // !lz
				lzItems = append(lzItems, lzValsCopy)

				// TODO: No ORDER-BY, but OFFSET+LIMIT reached, so
				// need to early exit via lzVars.Ctx.YieldStats?
			} // !lz
		}

		lzYieldErrOrig := lzYieldErr

		lzYieldErr = func(lzErrIn error) {
			if lzErrIn == nil { // If no error, yield our sorted items.
				var lzN int
				_ = lzN

				if len(orders) > 0 { // !lz
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

					// TODO: Recycle lzHeap into lzVars.Ctx?
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

					// TODO: Recycle lzItems into lzVars.Ctx?
				} // !lz
			}

			lzYieldErrOrig(lzErrIn)
		}

		EmitPop(pathNext, "OOL") // !lz

		if LzScope {
			ExecOp(o.Children[0], lzVars, lzYieldVals, lzYieldErr, pathNext, "OOLO") // !lz
		}
	}
}

// -----------------------------------------------------

func MakeLessFunc(lzVars *base.Vars, directions []interface{}) (
	lzLessFunc base.LessFunc) {
	// TODO: One day use eagerly discovered types to optimize?

	if len(directions) > 0 {
		lzLessFunc = func(lzValsA, lzValsB base.Vals) bool {
			var lzCmp int

			for idx := range directions { // !lz
				direction := directions[idx] // !lz

				lt, gt := true, false             // !lz
				if direction.(string) == "desc" { // !lz
					lt, gt = false, true // !lz
				} // !lz

				lzCmp = lzVars.Ctx.ValComparer.Compare(lzValsA[idx], lzValsB[idx])
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
