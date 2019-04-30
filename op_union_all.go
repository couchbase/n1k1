package n1k1

import (
	"github.com/couchbase/n1k1/base"
)

func OpUnionAll(o *base.Op, lzVars *base.Vars, lzYieldVals base.YieldVals,
	lzYieldStats base.YieldStats, lzYieldErr base.YieldErr,
	path, pathNext string) {
	numFields := len(o.Fields)

	// Implemented via data-staging concurrent actors, with one actor
	// per union contributor.
	//
	var lzStage *base.Stage        // !lz
	var lzActorFunc base.ActorFunc // !lz
	var lzActorData interface{}    // !lz

	_, _, _ = lzStage, lzActorFunc, lzActorData // !lz

	if LzScope {
		lzStage := base.NewStage(0, lzVars, lzYieldVals, lzYieldStats, lzYieldErr)

		for _, child := range o.Children { // !lz
			if LzScope {
				lzActorData = child // !lz

				var lzActorData interface{} = child

				lzActorFunc := func(lzVars *base.Vars, lzYieldVals base.YieldVals, lzYieldStats base.YieldStats, lzYieldErr base.YieldErr, lzActorData interface{}) {
					child := lzActorData.(*base.Op) // !lz

					lzValsUnion := make(base.Vals, numFields)

					lzYieldValsOrig := lzYieldVals

					lzYieldVals = func(lzVals base.Vals) {
						// Remap incoming vals to the union's field positions.
						for unionIdx, unionField := range o.Fields { // !lz
							found := false // !lz

							for childIdx, childField := range child.Fields { // !lz
								if childField == unionField { // !lz
									lzValsUnion[unionIdx] = lzVals[childIdx]
									found = true // !lz
									break        // !lz
								} // !lz
							} // !lz

							if !found { // !lz
								lzValsUnion[unionIdx] = base.ValMissing
							} // !lz
						} // !lz

						lzYieldValsOrig(lzValsUnion)
					}

					ExecOp(child, lzVars, lzYieldVals, lzYieldStats, lzYieldErr, pathNext, "U") // !lz
				}

				StageStartActor(lzStage, lzActorFunc, lzActorData, 0) // !lz
			}
		} // !lz

		StageWaitForActors(lzStage) // !lz
	}
}
