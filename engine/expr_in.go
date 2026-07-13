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

func init() {
	ExprCatalog["in"] = ExprIn
}

// ExprIn handles `x IN arr` (membership in an array). The three-valued logic
// lives in base.ValIn (mirrors cbq expression/coll_in.go).
func ExprIn(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	biExprFunc := func(lzA, lzB base.ExprFunc, lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) { // !lz
		if LzScope {
			// Capture each operand FROM the shared lzVal register: emitCaptured
			// replaces the marked line with the child's code (which writes lzVal),
			// then a plain lzValX := lzVal copies it out. A direct lzValX := lzA(...)
			// bind is DROPPED in the compiled path (only lzVal survives inlining), so
			// its var ends up undefined -- the bug that kept `in` denylisted. Mirrors
			// ExprBetween / ExprElement / ExprArithBi.
			lzVal = lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"
			lzValX := lzVal

			lzVal = lzB(lzVals, lzYieldErr) // <== emitCaptured: path "B"
			lzValArr := lzVal

			lzVal = base.ValIn(lzVars.Ctx.ValComparer, lzValX, lzValArr)
		}

		return lzVal
	} // !lz

	lzExprFunc = MakeBiExprFunc(lzVars, labels, params, path, biExprFunc) // !lzRHS

	return lzExprFunc
}
