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

		lzAggCalcs := aggCalcs

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

		// TODO: Configurable initial size for rhmap, and reusable rhmap.
		lzSet := rhmap.NewRHMap(97)

		// TODO: Reuse backing bytes for lzSet.
		// TODO: Allow spill out to disk.
		var lzSetBytes []byte

		_, _, _, _, _ = lzAggCalcs, lzGroupProjectFunc, lzAggProjectFunc, lzSet, lzSetBytes

		var lzValsOut base.Vals

		var lzValOut base.Val

		var lzGroupKey []byte

		var lzGroupVal []byte

		var lzGroupValNew []byte

		var lzGroupValReuse []byte

		var lzGroupKeyFound bool

		_, _, _, _, _, _, _ = lzValsOut, lzValOut, lzGroupKey, lzGroupVal, lzGroupValNew, lzGroupValReuse, lzGroupKeyFound

		lzYieldValsOrig := lzYieldVals

		lzYieldVals = func(lzVals base.Vals) {
			if len(groupExprs) > 0 { // !lz
				lzValsOut = lzValsOut[:0]

				lzValsOut = lzGroupProjectFunc(lzVals, lzValsOut, lzYieldErr) // <== emitCaptured: pathNextG "GP"

				// Construct the newline-delimited group key.
				lzGroupKey = lzGroupKey[:0]

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
					lzGroupVal, lzGroupKeyFound = lzSet.Get(lzGroupKey)

					if len(aggExprs) > 0 { // !lz
						if !lzGroupKeyFound {
							lzGroupVal = lzGroupValReuse[:0]

							for _, lzAggCalc := range lzAggCalcs {
								for _, lzAggName := range lzAggCalc.([]interface{}) {
									lzAgg := base.AggCatalog[lzAggName.(string)]
									lzGroupVal = lzAgg.Init(lzGroupVal)
								}
							}

							lzGroupValReuse = lzGroupVal[:0]
						}

						lzValsOut = lzValsOut[:0]

						lzValsOut = lzAggProjectFunc(lzVals, lzValsOut, lzYieldErr) // <== emitCaptured: pathNextG "AP"

						lzGroupValNew = lzGroupValNew[:0]

						for lzAggCalcI, lzAggCalc := range lzAggCalcs {
							for _, lzAggName := range lzAggCalc.([]interface{}) {
								lzAgg := base.AggCatalog[lzAggName.(string)]
								lzGroupValNew, lzGroupVal = lzAgg.Update(lzValsOut[lzAggCalcI], lzGroupValNew, lzGroupVal)
							}
						}

						if lzGroupKeyFound {
							if len(lzGroupVal) >= len(lzGroupValNew) {
								copy(lzGroupVal, lzGroupValNew)
							} else {
								// Copy lzGroupValNew into lzSetBytes.
								lzSetBytesLen := len(lzSetBytes)
								lzSetBytes = append(lzSetBytes, lzGroupValNew...)
								lzGroupValNewCopy := lzSetBytes[lzSetBytesLen:]

								lzSet.Set(lzGroupKey, lzGroupValNewCopy)
							}
						} else {
							lzGroupVal = lzGroupValNew
						}
					} // !lz

					if !lzGroupKeyFound {
						// Copy lzGroupKey into lzSetBytes.
						lzSetBytesLen := len(lzSetBytes)
						lzSetBytes = append(lzSetBytes, lzGroupKey...)
						lzGroupKeyCopy := lzSetBytes[lzSetBytesLen:]

						// Copy lzGroupVal into lzSetBytes.
						lzSetBytesLen = len(lzSetBytes)
						lzSetBytes = append(lzSetBytes, lzGroupVal...)
						lzGroupValCopy := lzSetBytes[lzSetBytesLen:]

						lzSet.Set(lzGroupKeyCopy, lzGroupValCopy)
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

					if len(aggExprs) > 0 { // !lz
						lzValBuf := lzValOut[:cap(lzValOut)]

						lzValBytes := 0

						for _, lzAggCalc := range lzAggCalcs {
							for _, lzAggName := range lzAggCalc.([]interface{}) {
								var lzVal base.Val

								lzAgg := base.AggCatalog[lzAggName.(string)]
								lzVal, lzGroupVal, lzValBuf = lzAgg.Result(lzGroupVal, lzValBuf)

								lzValsOut = append(lzValsOut, lzVal)

								lzValBytes += len(lzVal)
							}
						}

						if cap(lzValOut) < lzValBytes {
							lzValOut = make(base.Val, lzValBytes)
						}
					} // !lz

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
