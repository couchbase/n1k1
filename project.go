package n1k1

type LazyProjectFunc func(lazyVals, lazyValsPre LazyVals) LazyVals

func MakeProjectFunc(fields Fields, types Types, projections []interface{},
	outTypes Types) (lazyProjectFunc LazyProjectFunc) {
	var lazyExprFuncs []LazyExprFunc

	for _, projection := range projections {
		expr := projection.([]interface{})

		outTypes = append(outTypes, "")

		if LazyScope {
			var lazyExprFunc LazyExprFunc

			lazyExprFunc =
				MakeExprFunc(fields, types, expr, outTypes, "") // <== inlineOk

			lazyExprFuncs = append(lazyExprFuncs, lazyExprFunc)
		}
	}

	lazyProjectFunc = func(lazyVals, lazyValsPre LazyVals) (
		lazyValsOut LazyVals) {
		lazyValsOut = lazyValsPre // Optional pre-alloc'ed slice.

		for _, lazyExprFunc := range lazyExprFuncs {
			var lazyValProjected LazyVal

			lazyValProjected = lazyExprFunc(lazyVals)

			// NOTE: The lazyVals is stable while we are building up
			// the lazyValsOut, so no need to deep copy lazyVal yet.
			lazyValsOut = append(lazyValsOut, lazyValProjected)
		}

		return lazyValsOut
	}

	return lazyProjectFunc
}
