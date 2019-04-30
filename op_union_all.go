package n1k1

import (
	"github.com/couchbase/n1k1/base"
)

func OpUnionAll(o *base.Op, lzVars *base.Vars, lzYieldVals base.YieldVals,
	lzYieldStats base.YieldStats, lzYieldErr base.YieldErr,
	path, pathNext string) {
	numUnionFields := len(o.Fields)

	// UNION via concurrent stage actors.
	stage := &base.Stage{lzVars, lzYieldVals, lzYieldStats, lzYieldErr, len(o.Children)} // !lz

	for _, child := range o.Children {
		actorFunc := func(lzVars *base.Vars, lzYieldVals base.YieldVals, lzYieldStats base.YieldStats, lzYieldErr base.YieldErr, data interface{}) { // !lz
			child := data.(*base.Op)

			if LzScope {
				lzValsUnion := make(base.Vals, numUnionFields)

				lzYieldValsOrig := lzYieldVals

				lzYieldVals := func(lzVals base.Vals) {
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
		} // !lz

		StageStartActor(stage, actorFunc, child)
	}

	StageWaitForActors(stage)
}
