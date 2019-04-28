package n1k1

import (
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

	if LzScope {
		pathNextOOL := EmitPush(pathNext, "OOL") // !lz

		var lzProjectFunc base.ProjectFunc
		_ = lzProjectFunc

		if len(projections) > 0 { // !lz
			lzProjectFunc =
				MakeProjectFunc(o.ParentA.Fields, nil, projections, pathNextOOL, "PF") // !lz
		} // !lz

		var lzLessFunc base.LessFunc
		_ = lzLessFunc

		lzLessFunc =
			MakeLessFunc(nil, directions) // !lz

		var lzPreallocVals base.Vals
		var lzPreallocVal base.Val

		var lzItems []base.Vals // Items collected to be sorted.

		lzYieldValsOrig := lzYieldVals

		lzYieldVals = func(lzVals base.Vals) {
			var lzItem base.Vals

			lzItem, lzPreallocVals, lzPreallocVal = base.ValsDeepCopy(lzVals, lzPreallocVals, lzPreallocVal, InitPreallocVals, InitPreallocVal)

			lzItems = append(lzItems, lzItem)

			// TODO: If no order-by, but OFFSET+LIMIT reached, early exit?
		}

		lzYieldErrOrig := lzYieldErr

		lzYieldErr = func(lzErrIn error) {
			if lzErrIn == nil { // If no error, yield our sorted items.
				nProjections := len(projections) // !lz
				if nProjections > 0 {            // !lz
					lzProjected := make([]base.Vals, 0, len(lzItems))

					for _, lzVals := range lzItems {
						var lzValsOut base.Vals

						lzValsOut = lzProjectFunc(lzVals, lzValsOut) // <== emitCaptured: pathNextOOL "PF"

						lzProjected = append(lzProjected, lzValsOut)
					}

					base.OrderByItems(lzItems, lzProjected, lzLessFunc)
				} // !lz

				lzI := offset
				lzN := 0

				for lzI < len(lzItems) && lzN < limit {
					lzVals := lzItems[lzI]

					lzYieldValsOrig(lzVals)

					lzI++
					lzN++
				}
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
		lzValComparer := &base.ValComparer{}

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
