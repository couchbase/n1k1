package n1k1

import (
	"github.com/couchbase/n1k1/base"
)

type LazyProjectFunc func(lazyVals, lazyValsPre base.LazyVals) base.LazyVals

func MakeProjectFunc(fields base.Fields, types base.Types,
	projections []interface{}, outTypes base.Types) (
	lazyProjectFunc LazyProjectFunc) {
	var lazyExprFuncs []base.LazyExprFunc

	for _, projection := range projections {
		expr := projection.([]interface{})

		outTypes = append(outTypes, "")

		if base.LazyScope {
			var lazyExprFunc base.LazyExprFunc

			lazyExprFunc =
				MakeExprFunc(fields, types, expr, outTypes, "") // <== inlineOk

			lazyExprFuncs = append(lazyExprFuncs, lazyExprFunc)
		}
	}

	lazyProjectFunc = func(lazyVals, lazyValsPre base.LazyVals) (
		lazyValsOut base.LazyVals) {
		lazyValsOut = lazyValsPre // Optional pre-alloc'ed slice.

		for _, lazyExprFunc := range lazyExprFuncs {
			var lazyValProjected base.LazyVal

			lazyValProjected = lazyExprFunc(lazyVals)

			// NOTE: The lazyVals is stable while we are building up
			// the lazyValsOut, so no need to deep copy lazyVal yet.
			lazyValsOut = append(lazyValsOut, lazyValProjected)
		}

		return lazyValsOut
	}

	return lazyProjectFunc
}
