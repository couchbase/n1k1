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

// ExprConcat handles the `||` string concatenation operator (n-ary). MISSING if
// any operand is MISSING; NULL if any is a non-string value; else the strings
// concatenated. Logic in base.NaryConcat; result built into a reused buffer.

func init() {
	ExprCatalog["concat"] = ExprConcat
}

func ExprConcat(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	var lzBufPre []byte        // <== varLift: lzBufPre by path
	var lzValsReduce base.Vals // <== varLift: lzValsReduce by path

	lzChildren := CaptureNaryChildren(lzVars, labels, params, path) // !lz

	if LzScope {
		lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
			// Evaluate every operand into lzValsReduce (eager -- concat needs all
			// operands anyway), then reduce over the evaluated values. See
			// CaptureNaryChildren for why this is inlined per-expr.
			lzValsReduce = lzValsReduce[:0]

			for lzI := range lzChildren { // !lz
				lzVal =
					lzChildren[lzI](lzVals, lzYieldErr) // <== emitCaptured: path strconv.Itoa(lzI)

				lzValsReduce = append(lzValsReduce, lzVal)
			} // !lz

			lzVal, lzBufPre = base.NaryConcatVals(lzValsReduce, lzBufPre)

			return lzVal
		}
	}

	return lzExprFunc
}
