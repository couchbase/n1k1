//  Copyright (c) 2019 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package n1k1

import (
	"strconv"

	"github.com/couchbase/n1k1/base"
)

// OpUnionAll implements UNION ALL.
func OpUnionAll(o *base.Op, lzVars *base.Vars, lzYieldVals base.YieldVals,
	lzYieldErr base.YieldErr, path, pathNext string) {
	pathNextU := EmitPush(pathNext, "U") // !lz

	numChildren := len(o.Children)
	numLabels := len(o.Labels)

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
			pathNextUC := EmitPush(pathNextU, strconv.Itoa(childi)) // !lz

			if LzScope {
				lzActorData = childi // !lz

				var lzActorData interface{} = childi

				lzActorFunc := func(lzVars *base.Vars, lzYieldVals base.YieldVals, lzYieldErr base.YieldErr, lzActorData interface{}) {
					childi := lzActorData.(int) // !lz
					child := o.Children[childi] // !lz

					lzVars = lzVars.ChainExtend() // For concurrent usage.

					lzValsUnion := make(base.Vals, numLabels)

					lzYieldValsOrig := lzYieldVals

					lzYieldVals = func(lzVals base.Vals) {
						// Remap incoming vals to the union's label positions.
						for unionIdx, unionLabel := range o.Labels { // !lz
							found := false // !lz

							for childIdx, childLabel := range child.Labels { // !lz
								if childLabel == unionLabel { // !lz
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

					ExecOp(child, lzVars, lzYieldVals, lzYieldErr, pathNextUC, "UO") // !lz
				}

				// TODO: Configure actor batch size.
				lzStage.StartActor(lzActorFunc, lzActorData, 0)
			}

			EmitPop(pathNextU, strconv.Itoa(childi)) // !lz
		} // !lz

		lzStage.YieldResultsFromActors()

		// TODO: Recycle children's lzVars.Ctx into my lzVars.Ctx?

		// TODO: Recycle stage.
	}

	EmitPop(pathNext, "U") // !lz
}
