package n1k1

import (
	"strconv"

	"github.com/couchbase/n1k1/base"
)

func MakeProjectFunc(fields base.Fields, types base.Types,
	projections []interface{}, outTypes base.Types, path, pathItem string) (
	lzProjectFunc base.ProjectFunc) {
	pathNext := EmitPush(path, pathItem)

	defer EmitPop(path, pathItem)

	var exprFuncs []base.ExprFunc

	var lzExprFunc base.ExprFunc // !lz

	for i, projection := range projections {
		iStr := strconv.Itoa(i)

		outTypes = append(outTypes, "") // TODO: projected out type.

		expr := projection.([]interface{})

		if LzScope {
			lzExprFunc =
				MakeExprFunc(fields, types, expr, outTypes, pathNext, iStr) // !lz

			exprFuncs = append(exprFuncs, lzExprFunc) // !lz
		}
	}

	lzProjectFunc = func(lzVals, lzValsPre base.Vals) (lzValsOut base.Vals) {
		for i := range exprFuncs { // !lz
			iStr := strconv.Itoa(i) // !lz
			_ = iStr                // !lz

			if LzScope {
				var lzVal base.Val

				lzVal = exprFuncs[i](lzVals) // <== emitCaptured: pathNext iStr

				// NOTE: lzVals are stable while we are building up
				// lzValsOut, so no need to deep copy lzVal yet.
				lzValsOut = append(lzValsOut, lzVal)
			}
		} // !lz

		return lzValsOut
	}

	return lzProjectFunc
}
