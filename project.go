package n1k1

import (
	"github.com/couchbase/n1k1/base"
)

func MakeProjectFunc(fields base.Fields, types base.Types,
	projections []interface{}, outTypes base.Types) (
	lazyProjectFunc base.ProjectFunc) {
	for range projections {
		outTypes = append(outTypes, "") // TODO: projected out type.
	}

	lazyProjectFunc = func(lazyVals, lazyValsPre base.Vals) (
		lazyValsOut base.Vals) {
		lazyValsOut = lazyValsPre // Optional pre-alloc'ed slice.

		for _, projection := range projections { // <== notLazy
			expr := projection.([]interface{}) // <== notLazy

			if LazyScope {
				var lazyExprFunc base.ExprFunc
				_ = lazyExprFunc

				lazyExprFunc =
					MakeExprFunc(fields, types, expr, outTypes, "project", "PF") // <== notLazy

				var lazyVal base.Val

				lazyVal = lazyExprFunc(lazyVals) // <== emitCaptured: "project" PF

				// NOTE: lazyVals are stable while we are building up
				// lazyValsOut, so no need to deep copy lazyVal yet.
				lazyValsOut = append(lazyValsOut, lazyVal)
			}
		} // <== notLazy

		return lazyValsOut
	}

	return lazyProjectFunc
}
