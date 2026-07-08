//  Copyright (c) 2026 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package engine

import (
	"github.com/couchbase/n1k1/base"
)

// GREATEST(...) / LEAST(...): the max / min operand by N1QL collation, skipping
// MISSING/NULL operands; NULL if all are MISSING/NULL. N-ary; logic in
// base.GreatestLeast.

func init() {
	ExprCatalog["greatest"] = ExprGreatest
	ExprCatalog["least"] = ExprLeast
}

func ExprGreatest(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	var lzValsReduce base.Vals // <== varLift: lzValsReduce by path

	lzChildren := CaptureNaryChildren(lzVars, labels, params, path) // !lz

	if LzScope {
		lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
			lzValsReduce = lzValsReduce[:0]

			for lzI := range lzChildren { // !lz
				lzVal =
					lzChildren[lzI](lzVals, lzYieldErr) // <== emitCaptured: path strconv.Itoa(lzI)

				lzValsReduce = append(lzValsReduce, lzVal)
			} // !lz

			lzVal = base.GreatestLeastVals(lzVars.Ctx.ValComparer, lzValsReduce, true)

			return lzVal
		}
	}

	return lzExprFunc
}

func ExprLeast(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	var lzValsReduce base.Vals // <== varLift: lzValsReduce by path

	lzChildren := CaptureNaryChildren(lzVars, labels, params, path) // !lz

	if LzScope {
		lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
			lzValsReduce = lzValsReduce[:0]

			for lzI := range lzChildren { // !lz
				lzVal =
					lzChildren[lzI](lzVals, lzYieldErr) // <== emitCaptured: path strconv.Itoa(lzI)

				lzValsReduce = append(lzValsReduce, lzVal)
			} // !lz

			lzVal = base.GreatestLeastVals(lzVars.Ctx.ValComparer, lzValsReduce, false)

			return lzVal
		}
	}

	return lzExprFunc
}
