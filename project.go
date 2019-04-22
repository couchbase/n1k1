package n1k1

import (
	"strconv"

	"github.com/couchbase/n1k1/base"
)

func MakeProjectFunc(fields base.Fields, types base.Types,
	projections []interface{}, outTypes base.Types, path, pathItem string) (
	lazyProjectFunc base.ProjectFunc) {
	EmitPush(path, pathItem)

	defer EmitPop(path, pathItem)

	if len(pathItem) > 0 {
		path = path + "_" + pathItem
	}

	for range projections {
		outTypes = append(outTypes, "") // TODO: projected out type.
	}

	lazyProjectFunc = func(lazyVals, lazyValsOut base.Vals) base.Vals {
		for i, projection := range projections { // <== notLazy
			iStr := strconv.Itoa(i) // <== notLazy

			expr := projection.([]interface{}) // <== notLazy

			if LazyScope {
				var lazyExprFunc base.ExprFunc
				_ = lazyExprFunc

				lazyExprFunc =
					MakeExprFunc(fields, types, expr, outTypes, path, iStr) // <== notLazy

				var lazyVal base.Val

				lazyVal = lazyExprFunc(lazyVals) // <== emitCaptured: path iStr

				// NOTE: lazyVals are stable while we are building up
				// lazyValsOut, so no need to deep copy lazyVal yet.
				lazyValsOut = append(lazyValsOut, lazyVal)
			}
		} // <== notLazy

		return lazyValsOut
	}

	return lazyProjectFunc
}
