package n1k1

import (
	"github.com/couchbase/n1k1/base"
)

func MakeProjectFunc(fields base.Fields, types base.Types,
	projections []interface{}, outTypes base.Types) (
	lazyProjectFunc base.ProjectFunc) {
	var lazyExprFuncs []base.ExprFunc

	for _, projection := range projections {
		expr := projection.([]interface{})

		outTypes = append(outTypes, "")

		if LazyScope {
			var lazyExprFunc base.ExprFunc

			lazyExprFunc =
				MakeExprFunc(fields, types, expr, outTypes, "", "") // <== notLazy

			lazyExprFuncs = append(lazyExprFuncs, lazyExprFunc)
		}
	}

	lazyProjectFunc = func(lazyVals, lazyValsPre base.Vals) (
		lazyValsOut base.Vals) {
		lazyValsOut = lazyValsPre // Optional pre-alloc'ed slice.

		for _, lazyExprFunc := range lazyExprFuncs {
			var lazyValProjected base.Val

			lazyValProjected = lazyExprFunc(lazyVals)

			// NOTE: The lazyVals is stable while we are building up
			// the lazyValsOut, so no need to deep copy lazyVal yet.
			lazyValsOut = append(lazyValsOut, lazyValProjected)
		}

		return lazyValsOut
	}

	return lazyProjectFunc
}
