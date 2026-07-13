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

// ExprCase handles searched and simple CASE. The glue optimizer lowers both to a
// flat [cond, then, cond, then, ..., else?] param list (simple CASE's conds are
// eq(searchTerm, when)); mirrors base.CaseReduce.
//
// CASE is LAZY -- only the matched branch runs -- so it can't use the eager-Vals
// nary harness. Instead each operand is inlined (emitCaptured) guarded by an
// lzMatched flag: evaluate a WHEN condition, and only if nothing has matched yet
// AND it is truthy, evaluate that THEN and set lzMatched. Each (cond,then) pair
// is a flat sibling block, so once matched every later pair's condition is
// skipped (short-circuit) -- and the compiler emits straight-line if/else with no
// nested func literal for a parent expr's emitCaptured to trip over. The trailing
// ELSE (odd operand count) runs only if still unmatched, else the result is NULL.
// lzMatched is varLift'd so sibling/nested CASEs get distinct flags.

func init() {
	ExprCatalog["case"] = ExprCase
}

func ExprCase(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	var lzMatched bool // <== varLift: lzMatched by path

	lzChildren := NaryCaptureChildren(lzVars, labels, params, path) // !lz

	if LzScope {
		lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
			lzMatched = false

			for lzI := 0; lzI+1 < len(lzChildren); lzI += 2 { // !lz
				if !lzMatched {
					// The condition evaluates INTO lzVal (the emitCaptured target),
					// which is only the CASE result if it matches -- so on no match
					// lzVal is left as scratch and reset to NULL at the end below.
					lzVal =
						lzChildren[lzI](lzVals, lzYieldErr) // <== emitCaptured: path strconv.Itoa(lzI)

					if base.ValTruthy(lzVal) {
						lzVal =
							lzChildren[lzI+1](lzVals, lzYieldErr) // <== emitCaptured: path strconv.Itoa(lzI+1)

						lzMatched = true
					}
				}
			}

			if len(lzChildren)%2 == 1 { // !lz
				if !lzMatched {
					lzVal =
						lzChildren[len(lzChildren)-1](lzVals, lzYieldErr) // <== emitCaptured: path strconv.Itoa(len(lzChildren)-1)

					lzMatched = true
				}
			}

			if !lzMatched {
				lzVal = base.ValNull
			}

			return lzVal
		}
	}

	return lzExprFunc
}
