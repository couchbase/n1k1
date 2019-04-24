package n1k1

import (
	"strconv"

	"github.com/couchbase/n1k1/base"
)

func MakeProjectFunc(fields base.Fields, types base.Types,
	projections []interface{}, outTypes base.Types, path, pathItem string) (
	lazyProjectFunc base.ProjectFunc) {
	pathNext := EmitPush(path, pathItem)

	defer EmitPop(path, pathItem)

	var exprFuncs []base.ExprFunc

	var lazyExprFunc base.ExprFunc // <== notLazy
	_ = lazyExprFunc               // <== notLazy

	for i, projection := range projections {
		iStr := strconv.Itoa(i)

		outTypes = append(outTypes, "") // TODO: projected out type.

		expr := projection.([]interface{})

		if LazyScope {
			var lazyExprFunc base.ExprFunc
			_ = lazyExprFunc

			lazyExprFunc =
				MakeExprFunc(fields, types, expr, outTypes, pathNext, iStr) // <== notLazy

			exprFuncs = append(exprFuncs, lazyExprFunc) // <== notLazy
		}
	}

	lazyProjectFunc = func(lazyVals, lazyValsPre base.Vals) (lazyValsOut base.Vals) {
		for i, lazyExprFunc := range exprFuncs { // <== notLazy
			_ = lazyExprFunc // <== notLazy

			iStr := strconv.Itoa(i) // <== notLazy
			_ = iStr                // <== notLazy

			if LazyScope {
				var lazyVal base.Val

				lazyVal = lazyExprFunc(lazyVals) // <== emitCaptured: pathNext iStr

				// NOTE: lazyVals are stable while we are building up
				// lazyValsOut, so no need to deep copy lazyVal yet.
				lazyValsOut = append(lazyValsOut, lazyVal)
			}
		} // <== notLazy

		return lazyValsOut
	}

	return lazyProjectFunc
}
