package n1k1

import (
	"github.com/couchbase/rhmap/store" // <== genCompiler:hide

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

		var groupProjectFunc, aggProjectFunc base.ProjectFunc // !lz

		groupProjectFunc =
			MakeProjectFunc(lzVars, o.Children[0].Labels, groupExprs, pathNextG, "GP") // !lz

		if len(aggExprs) > 0 { // !lz
			aggProjectFunc =
				MakeProjectFunc(lzVars, o.Children[0].Labels, aggExprs, pathNextG, "AP") // !lz
		} // !lz

		_, _ = groupProjectFunc, aggProjectFunc

		// TODO: Configurable initial size for rhstore, and reusable rhstore.
		// TODO: Reuse backing bytes for lzSet.
		lzSet, lzErr := lzVars.Ctx.AllocMap()
		if lzErr != nil {
			lzYieldErr(lzErr)
		}

		var lzValOut base.Val

		var lzValsOut base.Vals

		var lzGroupKey, lzGroupVal, lzGroupValNew, lzGroupValReuse []byte

		var lzGroupKeyFound bool

		var lzAgg *base.Agg

		_, _, _ = lzGroupValNew, lzGroupValReuse, lzAgg

		lzYieldValsOrig := lzYieldVals

		lzYieldVals = func(lzVals base.Vals) {
			if len(groupExprs) > 0 { // !lz
				lzValsOut = lzValsOut[:0]

				lzValsOut = groupProjectFunc(lzVals, lzValsOut, lzYieldErr) // <== emitCaptured: pathNextG "GP"

				// Construct the newline-delimited group key.
				//
				// Ex: `GROUP BY state, city` would lead to a group
				// key such as '"CA"\n"San Francisco"'.
				lzGroupKey = lzGroupKey[:0]

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
					// Check if we've seen the group key before or not.
					lzGroupVal, lzGroupKeyFound = lzSet.Get(lzGroupKey)

					if len(aggExprs) > 0 { // !lz
						if !lzGroupKeyFound {
							// We have aggregate exprs on a newly seen
							// group key, so initialize the agg data
							// structures for this new group key.
							lzGroupVal = lzGroupValReuse[:0]

							for _, aggCalc := range aggCalcs { // !lz
								for _, aggName := range aggCalc.([]interface{}) { // !lz
									aggIdx := base.AggCatalog[aggName.(string)] // !lz
									lzAgg = base.Aggs[aggIdx]

									lzGroupVal = lzAgg.Init(lzGroupVal)
								} // !lz
							} // !lz

							lzGroupValReuse = lzGroupVal[:0]
						}

						// Project the aggregate exprs from the tuple.
						lzValsOut = lzValsOut[:0]

						lzValsOut = aggProjectFunc(lzVals, lzValsOut, lzYieldErr) // <== emitCaptured: pathNextG "AP"

						lzGroupValNew = lzGroupValNew[:0]

						// Use the projected aggregate exprs to update
						// the agg data structures.
						for aggCalcI, aggCalc := range aggCalcs { // !lz
							for _, aggName := range aggCalc.([]interface{}) { // !lz
								aggIdx := base.AggCatalog[aggName.(string)] // !lz
								lzAgg = base.Aggs[aggIdx]

								lzGroupValNew, lzGroupVal = lzAgg.Update(lzValsOut[aggCalcI], lzGroupValNew, lzGroupVal)
							} // !lz
						} // !lz

						if lzGroupKeyFound {
							// With a previously seen group key, the
							// previous agg data structure might be
							// in-place overwritable if its size is >=
							// the new agg data structure's size.
							if len(lzGroupVal) >= len(lzGroupValNew) {
								copy(lzGroupVal, lzGroupValNew)
							} else {
								lzSet.Set(lzGroupKey, lzGroupValNew)
							}
						} else {
							// We fall thru to the below lzSet.Set().
							lzGroupVal = lzGroupValNew
						}
					} // !lz

					if !lzGroupKeyFound {
						lzSet.Set(lzGroupKey, lzGroupVal)
					}
				}
			} // !lz
		}

		lzYieldErrOrig := lzYieldErr

		lzYieldErr = func(lzErrIn error) {
			if lzErrIn == nil { // If no error, yield our group items.
				lzSetVisitor := func(lzGroupKey store.Key, lzGroupVal store.Val) bool {
					lzValsOut = base.ValsSplit(lzGroupKey, lzValsOut[:0])

					if len(aggExprs) > 0 { // !lz
						// If we have aggregate exprs, append their
						// accummulated results to the yielded vals.
						lzValBuf := lzValOut[:cap(lzValOut)]

						lzValBytes := 0

						var lzVal base.Val

						for _, aggCalc := range aggCalcs { // !lz
							for _, aggName := range aggCalc.([]interface{}) { // !lz
								aggIdx := base.AggCatalog[aggName.(string)] // !lz
								lzAgg = base.Aggs[aggIdx]

								lzVal, lzGroupVal, lzValBuf = lzAgg.Result(lzGroupVal, lzValBuf)

								lzValsOut = append(lzValsOut, lzVal)

								lzValBytes += len(lzVal)
							} // !lz
						} // !lz

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

		if lzErr == nil {
			ExecOp(o.Children[0], lzVars, lzYieldVals, lzYieldErr, pathNext, "GO") // !lz
		}

		if lzSet != nil {
			lzSet.Close()
		}
	}
}
