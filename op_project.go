package n1k1

import (
	"strconv"

	"github.com/couchbase/n1k1/base"
)

func OpProject(o *base.Op, lzYieldVals base.YieldVals,
	lzYieldStats base.YieldStats, lzYieldErr base.YieldErr, path, pathNext string) {
	if LzScope {
		pathNextP := EmitPush(pathNext, "P") // !lz

		var lzValsReuse base.Vals // <== varLift: lzValsReuse by path

		projectFunc :=
			MakeProjectFunc(o.ParentA.Fields, nil, o.Params, pathNextP, "PF") // !lz

		lzYieldValsOrig := lzYieldVals

		_ = lzYieldValsOrig

		lzYieldVals = func(lzVals base.Vals) {
			lzValsOut := lzValsReuse[:0]

			lzValsOut = projectFunc(lzVals, lzValsOut) // <== emitCaptured: pathNextP "PF"

			lzValsReuse = lzValsOut

			lzYieldValsOrig(lzValsOut)
		}

		EmitPop(pathNext, "P") // !lz

		ExecOp(o.ParentA, lzYieldVals, lzYieldStats, lzYieldErr, pathNextP, "") // !lz
	}
}

func MakeProjectFunc(fields base.Fields, types base.Types,
	projections []interface{}, path, pathItem string) (
	lzProjectFunc base.ProjectFunc) {
	pathNext := EmitPush(path, pathItem)

	defer EmitPop(path, pathItem)

	var exprFuncs []base.ExprFunc

	var lzExprFunc base.ExprFunc // !lz

	for i, projection := range projections {
		expr := projection.([]interface{})

		if LzScope {
			lzExprFunc =
				MakeExprFunc(fields, types, expr, pathNext, strconv.Itoa(i)) // !lz

			exprFuncs = append(exprFuncs, lzExprFunc) // !lz
		}
	}

	lzProjectFunc = func(lzVals, lzValsPre base.Vals) (lzValsOut base.Vals) {
		for i := range exprFuncs { // !lz
			if LzScope {
				var lzVal base.Val

				lzVal = exprFuncs[i](lzVals) // <== emitCaptured: pathNext strconv.Itoa(i)

				// NOTE: lzVals are stable while we are building up
				// lzValsOut, so no need to deep copy lzVal yet.
				lzValsOut = append(lzValsOut, lzVal)
			}
		} // !lz

		return lzValsOut
	}

	return lzProjectFunc
}