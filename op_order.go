package n1k1

import (
	"math"
	"sort" // <== genCompiler:hide

	"github.com/couchbase/n1k1/base"
)

func OpOrderByOffsetLimit(o *base.Op, lzYieldVals base.YieldVals,
	lzYieldStats base.YieldStats, lzYieldErr base.YieldErr, path, pathNext string) {
	projections := o.Params[0].([]interface{}) // ORDER BY expressions.

	// Then directions has same len as projections, ex: ["asc", "desc", "asc"].
	directions := o.Params[1].([]interface{})

	offset := int64(0)

	limit := int64(math.MaxInt64)

	if len(o.Params) >= 3 {
		offset = o.Params[2].(int64)

		if len(o.Params) >= 4 {
			limit = o.Params[3].(int64)
		}
	}

	if LzScope {
		pathNextOOL := EmitPush(pathNext, "OOL") // !lz

		lzProjectFunc :=
			MakeProjectFunc(o.ParentA.Fields, nil, projections, pathNextOOL, "PF") // !lz

		lzLessFunc :=
			MakeLessFunc(nil, directions) // !lz

		var lzItems []base.Vals // Items collected to be sorted.

		lzYieldValsOrig := lzYieldVals

		lzYieldVals = func(lzVals base.Vals) {
			var lzItem base.Vals // Deep copy.
			for _, lzVal := range lzVals {
				lzItem = append(lzItem, append(base.Val(nil), lzVal...))
			}

			lzItems = append(lzItems, lzItem)
		}

		lzYieldErrOrig := lzYieldErr

		lzYieldErr = func(lzErrIn error) {
			if lzErrIn == nil { // If no error, yield our sorted items.
				var lzProjected = make([]base.Vals, 0, len(lzItems))

				for _, lzVals := range lzItems {
					var lzValsOut base.Vals

					lzValsOut = lzProjectFunc(lzVals, lzValsOut)

					lzProjected = append(lzProjected, lzValsOut)
				}

				sort.Sort(&base.OrderBySorter{lzItems, lzProjected, lzLessFunc})

				lzI := offset
				lzN := int64(0)

				for lzN < limit {
					lzVals := lzItems[lzI]

					lzYieldValsOrig(lzVals)

					lzI++
					lzN++
				}
			}

			lzYieldErrOrig(lzErrIn)
		}

		EmitPop(pathNext, "OOL") // !lz

		ExecOp(o.ParentA, lzYieldVals, lzYieldStats, lzYieldErr, pathNextOOL, "") // !lz
	}
}

func MakeLessFunc(types base.Types, directions []interface{}) (
	lessFunc func(base.Vals, base.Vals) bool) {
	// TODO: One day use types to optimize.

	lzValComparer := &base.ValComparer{}

	lessFunc = func(lzValsA, lzValsB base.Vals) bool {
		for i := range directions { // !lz
			direction := directions[i] // !lz

			lt, gt := true, false                               // !lz
			if s, ok := direction.(string); ok && s == "desc" { // !lz
				lt, gt = false, true // !lz
			} // !lz

			lzCmp := lzValComparer.Compare(lzValsA[i], lzValsB[i])
			if lzCmp < 0 {
				return lt
			}

			if lzCmp > 0 {
				return gt
			}
		} // !lz

		return false
	}

	return lessFunc
}
