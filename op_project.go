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

func OpProject(o *base.Op, lzVars *base.Vars, lzYieldVals base.YieldVals,
	lzYieldErr base.YieldErr, path, pathNext string) {
	if LzScope {
		pathNextP := EmitPush(pathNext, "P") // !lz

		var lzValsReuse base.Vals // <== varLift: lzValsReuse by path

		projectFunc :=
			MakeProjectFunc(lzVars, o.Children[0].Labels, o.Params, pathNextP, "PF") // !lz

		lzYieldValsOrig := lzYieldVals

		lzYieldVals = func(lzVals base.Vals) {
			lzValsOut := lzValsReuse[:0]

			lzValsOut = projectFunc(lzVals, lzValsOut, lzYieldErr) // <== emitCaptured: pathNextP "PF"

			lzValsReuse = lzValsOut

			lzYieldValsOrig(lzValsOut)
		}

		EmitPop(pathNext, "P") // !lz

		ExecOp(o.Children[0], lzVars, lzYieldVals, lzYieldErr, pathNextP, "") // !lz
	}
}

// -----------------------------------------------------

func MakeProjectFunc(lzVars *base.Vars, labels base.Labels,
	projections []interface{}, path, pathItem string) (
	lzProjectFunc base.ProjectFunc) {
	pathNext := EmitPush(path, pathItem)

	defer EmitPop(path, pathItem)

	var exprFuncs []base.ExprFunc

	var lzExprFunc base.ExprFunc // !lz

	for i, projection := range projections {
		expr := projection.([]interface{})

		lzExprFunc =
			MakeExprFunc(lzVars, labels, expr, pathNext, strconv.Itoa(i)) // !lz

		exprFuncs = append(exprFuncs, lzExprFunc) // !lz
	}

	lzProjectFunc = func(lzVals, lzValsOut base.Vals, lzYieldErr base.YieldErr) base.Vals {
		for i := range exprFuncs { // !lz
			if LzScope {
				var lzVal base.Val

				lzVal = exprFuncs[i](lzVals, lzYieldErr) // <== emitCaptured: pathNext strconv.Itoa(i)

				// NOTE: lzVals are stable while we are building up
				// lzValsOut, so no need to deep copy lzVal yet.
				lzValsOut = append(lzValsOut, lzVal)
			}
		} // !lz

		return lzValsOut
	}

	return lzProjectFunc
}
