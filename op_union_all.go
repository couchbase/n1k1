package n1k1

import (
	"strconv"

	"github.com/couchbase/n1k1/base"
)

func OpUnionAll(o *base.Op, lzVars *base.Vars, lzYieldVals base.YieldVals,
	lzYieldErr base.YieldErr, path, pathNext string) {
	pathNextU := EmitPush(pathNext, "U") // !lz

	EmitPop(pathNext, "U") // !lz

	numChildren := len(o.Children)
	numFields := len(o.Fields)

	// Implemented via data-staging concurrent actors, with one actor
	// per union contributor.
	//
	var lzStage *base.Stage        // !lz
	var lzActorFunc base.ActorFunc // !lz
	var lzActorData interface{}    // !lz

	_, _, _ = lzStage, lzActorFunc, lzActorData // !lz

	if LzScope {
		lzStage := base.NewStage(numChildren, 0, lzVars, lzYieldVals, lzYieldErr)

		for childi := range o.Children { // !lz
			pathNextU := EmitPush(pathNextU, strconv.Itoa(childi)) // !lz

			if LzScope {
				lzActorData = childi // !lz

				var lzActorData interface{} = childi

				lzActorFunc := func(lzVars *base.Vars, lzYieldVals base.YieldVals, lzYieldErr base.YieldErr, lzActorData interface{}) {
					childi := lzActorData.(int) // !lz
					child := o.Children[childi] // !lz

					lzVars = lzVars.PushForConcurrency()

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

					ExecOp(child, lzVars, lzYieldVals, lzYieldErr, pathNextU, "UO") // !lz
				}

				lzStage.StartActor(lzActorFunc, lzActorData, 0)
			}

			EmitPop(pathNextU, strconv.Itoa(childi)) // !lz
		} // !lz

		lzStage.WaitForActors()

		// TODO: Recycle children's lzVars.Ctx into my lzVars.Ctx?
	}
}
