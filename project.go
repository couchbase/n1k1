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

	var lzExprFunc base.ExprFunc // <== notLz
	_ = lzExprFunc               // <== notLz

	for i, projection := range projections {
		iStr := strconv.Itoa(i)

		outTypes = append(outTypes, "") // TODO: projected out type.

		expr := projection.([]interface{})

		if LzScope {
			var lzExprFunc base.ExprFunc
			_ = lzExprFunc

			lzExprFunc =
				MakeExprFunc(fields, types, expr, outTypes, pathNext, iStr) // <== notLz

			exprFuncs = append(exprFuncs, lzExprFunc) // <== notLz
		}
	}

	lzProjectFunc = func(lzVals, lzValsPre base.Vals) (lzValsOut base.Vals) {
		for i, lzExprFunc := range exprFuncs { // <== notLz
			_ = lzExprFunc // <== notLz

			iStr := strconv.Itoa(i) // <== notLz
			_ = iStr                // <== notLz

			if LzScope {
				var lzVal base.Val

				lzVal = lzExprFunc(lzVals) // <== emitCaptured: pathNext iStr

				// NOTE: lzVals are stable while we are building up
				// lzValsOut, so no need to deep copy lzVal yet.
				lzValsOut = append(lzValsOut, lzVal)
			}
		} // <== notLz

		return lzValsOut
	}

	return lzProjectFunc
}
