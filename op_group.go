package n1k1

import (
	"bytes" // <== genCompiler:hide

	"github.com/couchbase/rhmap" // <== genCompiler:hide

	"github.com/couchbase/n1k1/base"
)

// OpGroup implements GROUP BY and DISTINCT.
//
// Ex: SELECT SUM(sales), MIN(sales), COUNT(*) ... GROUP BY state, city.
func OpGroup(o *base.Op, lzVars *base.Vars, lzYieldVals base.YieldVals,
	lzYieldErr base.YieldErr, path, pathNext string) {
	// GROUP BY exprs.
	// Ex: ["labelPath",".","state"], ["labelPath",".","city"].
	// The len(groupExprs) >= 1.
	groupExprs := o.Params[0].([]interface{})

	var aggExprs, aggCalcs []interface{}

	if len(o.Params) > 1 {
		// Aggregation exprs.
		// Ex: ["labelPath",".","sales"], ["labelPath","."].
		// The len(aggExprs) >= 0.
		aggExprs = o.Params[1].([]interface{})
		_ = aggExprs

		// Aggregation calcs.
		// Ex: ["sum","min"], ["count"].
		// The len(aggExprs) == len(aggFuncs).
		aggCalcs = o.Params[2].([]interface{})
		_ = aggCalcs
	}

	if LzScope {
		pathNextG := EmitPush(pathNext, "G") // !lz

		var lzGroupProjectFunc base.ProjectFunc

		var lzAggProjectFunc base.ProjectFunc

		var lzProjectFunc base.ProjectFunc

		lzProjectFunc =
			MakeProjectFunc(lzVars, o.Children[0].Labels, groupExprs, pathNextG, "GP") // !lz

		lzGroupProjectFunc = lzProjectFunc

		if len(aggExprs) > 0 { // !lz
			lzProjectFunc =
				MakeProjectFunc(lzVars, o.Children[0].Labels, aggExprs, pathNextG, "AP") // !lz

			lzAggProjectFunc = lzProjectFunc
		} // !lz

		lzSet := rhmap.NewRHMap(97) // TODO: Initial size.

		// TODO: Reuse backing bytes for lzSet.
		// TODO: Allow spill out to disk.
		var lzSetBytes []byte

		_, _, _, _ = lzGroupProjectFunc, lzAggProjectFunc, lzSet, lzSetBytes

		var lzValsOut base.Vals

		var lzValOut base.Val

		var lzGroupKey []byte

		_, _, _ = lzValsOut, lzValOut, lzGroupKey

		lzYieldValsOrig := lzYieldVals

		lzYieldVals = func(lzVals base.Vals) {
			if len(groupExprs) > 0 { // !lz
				lzValsOut = lzValsOut[:0]

				lzValsOut = lzGroupProjectFunc(lzVals, lzValsOut, lzYieldErr) // <== emitCaptured: pathNextG "GP"

				lzGroupKey = lzGroupKey[:0] // The group key is newline delimited.

				var lzErr error

				for lzI, lzVal := range lzValsOut {
					lzValOut, lzErr = lzVars.Ctx.ValComparer.CanonicalJSON(lzVal, lzValOut[:0])
					if lzErr != nil {
						break
					}

					if lzI > 0 {
						lzGroupKey = append(lzGroupKey, '\n')
					}

					lzGroupKey = append(lzGroupKey, lzValOut...)
				}

				if lzErr == nil {
					_, lzGroupKeyFound := lzSet.Get(lzGroupKey)
					if !lzGroupKeyFound {
						// Copy lzGroupKey into lzSetBytes.
						lzSetBytesLen := len(lzSetBytes)
						lzSetBytes = append(lzSetBytes, lzGroupKey...)
						lzGroupKeyCopy := lzSetBytes[lzSetBytesLen:]

						lzSet.Set(lzGroupKeyCopy, nil)
					}
				}
			} // !lz
		}

		lzYieldErrOrig := lzYieldErr

		lzYieldErr = func(lzErrIn error) {
			if lzErrIn == nil { // If no error, yield our group items.
				lzSetVisitor := func(lzGroupKey rhmap.Key, lzGroupVal rhmap.Val) bool {
					lzValsOut = lzValsOut[:0]

					for {
						lzIdx := bytes.IndexByte(lzGroupKey, '\n')
						if lzIdx < 0 {
							lzIdx = len(lzGroupKey)
						}

						lzValsOut = append(lzValsOut, base.Val(lzGroupKey[:lzIdx]))

						if lzIdx >= len(lzGroupKey) {
							break
						}

						lzGroupKey = lzGroupKey[lzIdx+1:]
					}

					lzYieldValsOrig(lzValsOut)

					return true
				}

				lzSet.Visit(lzSetVisitor)
			}

			lzYieldErrOrig(lzErrIn)
		}

		EmitPop(pathNext, "G") // !lz

		if LzScope {
			ExecOp(o.Children[0], lzVars, lzYieldVals, lzYieldErr, pathNext, "GO") // !lz
		}
	}
}
