package n1k1

type LazyProjectFunc func(lazyVals, lazyValsPre LazyVals) LazyVals

func MakeProjectFunc(fields Fields, types Types,
	projections []interface{}, outTypes Types) (
	lazyProjectFunc LazyProjectFunc) {
	var lazyExprFuncs []LazyExprFunc

	for _, projection := range projections {
		expr := projection.([]interface{})

		outTypes = append(outTypes, "")

		var lazyExprFunc LazyExprFunc

		lazyExprFunc =
			MakeExprFunc(fields, types, expr, outTypes) // <== inline-ok.

		lazyExprFuncs = append(lazyExprFuncs, lazyExprFunc)
	}

	lazyProjectFunc = func(lazyVals, lazyValsPre LazyVals) (
		lazyValsOut LazyVals) {
		lazyValsOut = lazyValsPre // Optional pre-alloc'ed slice.

		for _, lazyExprFunc := range lazyExprFuncs {
			var lazyVal LazyVal

			lazyVal =
				lazyExprFunc(lazyVals) // <== inline-ok.

			// NOTE: The lazyVals is stable while we are building up
			// the lazyValsOut, so no need to deep copy items yet.
			lazyValsOut = append(lazyValsOut, lazyVal)
		}

		return lazyValsOut
	}

	return lazyProjectFunc
}
