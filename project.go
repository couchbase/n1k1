package n1k1

import (
	"encoding/json"
	"fmt"
)

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
			var lazyAny interface{}

			lazyAny =
				lazyExprFunc(lazyVals) // <== inline-ok.

			lazyJsonBytes, lazyErr := json.Marshal(lazyAny)
			if lazyErr != nil { // TODO TODO TODO TODO TODO TODO.
				lazyJsonBytes, _ = json.Marshal(fmt.Sprintf("%v", lazyErr))
			}

			lazyJson := string(lazyJsonBytes)

			lazyValsOut = append(lazyValsOut, LazyVal(lazyJson))
		}

		return lazyValsOut
	}

	return lazyProjectFunc
}
