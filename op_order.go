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

		var lzValsLessFunc base.ValsLessFunc

		var lzHeap *base.HeapValsProjected

		if len(orders) > 0 { // !lz
			// The ORDER BY exprs are treated as a projection.
			lzProjectFunc =
				MakeProjectFunc(lzVars, o.Children[0].Labels, orders, pathNextOOL, "PF") // !lz

			lzValsLessFunc =
				MakeValsLessFunc(lzVars, directions) // !lz

			lzHeap = base.CreateHeapValsProjected(lzVars.Ctx, lzValsLessFunc)
		} // !lz

		_, _, _ = lzProjectFunc, lzValsLessFunc, lzHeap

		var lzEncoded []byte
		var lzExamined int
		var lzValsPre, lzValsMax base.Vals

		_, _, _, _ = lzEncoded, lzExamined, lzValsPre, lzValsMax

		lzYieldValsOrig := lzYieldVals

		lzYieldVals = func(lzVals base.Vals) {
			if len(orders) > 0 { // !lz
				// If there were ORDER BY exprs, we use the lzHeap.
				lzValsOut := lzValsPre[:0]

				lzValsOut = lzProjectFunc(lzVals, lzValsOut, lzYieldErr) // <== emitCaptured: pathNextOOL "PF"

				lzValsPre = lzValsOut

				// Push onto heap if heap is small or heap is empty or
				// item < max-heap-item.
				lzHeapLen := lzHeap.Len()

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
					lzEncoded, lzErr = base.ValsProjectedEncode(lzVals, lzValsOut, lzEncoded[:0], lzVars.Ctx.ValComparer)
					if lzErr != nil {
						lzYieldErr(lzErr)
					} else {
						heap.Push(lzHeap, lzEncoded)

						// If heap too big, pop max-heap-item.
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
					lzYieldValsOrig(lzVals)
				}

				lzExamined++

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

					// Pop off items from the heap, placing them in
					// reverse at end of the heap slots, which leads
					// to the end of the heap slots having the needed
					// items items correctly sorted in-place.
					lzHeapLen := lzHeap.Len()

					lzErr := lzHeap.Sort(offset)
					if lzErr != nil {
						lzYieldErrOrig(lzErr)
					}

					for lzI := offset; lzI < lzHeapLen; lzI++ {
						if limit < math.MaxInt64 { // !lz
							if lzN >= limit {
								break
							}
							lzN++
						} // !lz

						lzItem, lzErr := lzHeap.Get(lzI)
						if lzErr != nil {
							lzYieldErrOrig(lzErr)
							break
						}

						lzValsPre = base.ValsProjectedDecodeVals(lzItem, lzValsPre[:0])

						lzYieldValsOrig(lzValsPre)
					}

					// TODO: Recycle lzHeap into lzVars.Ctx?
				} // !lz
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

func MakeValsLessFunc(lzVars *base.Vars, directions []interface{}) (
	lzValsLessFunc base.ValsLessFunc) {
	// TODO: One day use eagerly discovered types to optimize?

	if len(directions) > 0 {
		lzValsLessFunc = func(lzValsA, lzValsB base.Vals) bool {
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

	return lzValsLessFunc
}
