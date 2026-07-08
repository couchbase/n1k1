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
	ExprCatalog["between"] = ExprBetween
}

// MakeTriExprFunc constructs handlers for three-argument ("ternary") expressions,
// mirroring MakeBiExprFunc.
func MakeTriExprFunc(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string, triExprFunc base.TriExprFunc) (
	lzExprFunc base.ExprFunc) {
	exprA := params[0].([]interface{})
	exprB := params[1].([]interface{})
	exprC := params[2].([]interface{})

	var lzA base.ExprFunc        // !lz
	var lzB base.ExprFunc        // !lz
	var lzC base.ExprFunc        // !lz
	var lzVals base.Vals         // !lz
	var lzYieldErr base.YieldErr // !lz

	_, _, _, _, _ = lzA, lzB, lzC, lzVals, lzYieldErr // !lz

	if LzScope {
		lzExprFunc =
			MakeExprFunc(lzVars, labels, exprA, path, "A") // !lz
		lzA := lzExprFunc

		lzExprFunc =
			MakeExprFunc(lzVars, labels, exprB, path, "B") // !lz
		lzB := lzExprFunc

		lzExprFunc =
			MakeExprFunc(lzVars, labels, exprC, path, "C") // !lz
		lzC := lzExprFunc

		lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
			lzVal =
				triExprFunc(lzA, lzB, lzC, lzVals, lzYieldErr) // !lz

			return lzVal
		}
	}

	return lzExprFunc
}

// -----------------------------------------------------

// ExprBetween handles `item BETWEEN low AND high`. Semantics mirror cbq
// expression/comp_between.go: MISSING if any operand is MISSING; NULL if any is
// NULL; else the boolean (low <= item <= high) by N1QL collation order.
func ExprBetween(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	triExprFunc := func(lzA, lzB, lzC base.ExprFunc, lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) { // !lz
		if LzScope {
			// Capture each operand FROM the shared lzVal register (emitCaptured
			// replaces the marked line with the child's code, which writes lzVal);
			// a direct lzValX := lzX(...) bind is DROPPED in the compiled path.
			// Mirrors ExprArithBi.
			lzVal = lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"
			lzValItem := lzVal

			lzVal = lzB(lzVals, lzYieldErr) // <== emitCaptured: path "B"
			lzValLow := lzVal

			lzVal = lzC(lzVals, lzYieldErr) // <== emitCaptured: path "C"
			lzValHigh := lzVal

			if base.ValKind(lzValItem) == base.ValKindMissing ||
				base.ValKind(lzValLow) == base.ValKindMissing ||
				base.ValKind(lzValHigh) == base.ValKindMissing {
				lzVal = base.ValMissing
			} else if base.ValKind(lzValItem) == base.ValKindNull ||
				base.ValKind(lzValLow) == base.ValKindNull ||
				base.ValKind(lzValHigh) == base.ValKindNull {
				lzVal = base.ValNull
			} else {
				lzCmpLow := lzVars.Ctx.ValComparer.Compare(lzValItem, lzValLow)
				lzCmpHigh := lzVars.Ctx.ValComparer.Compare(lzValItem, lzValHigh)
				if lzCmpLow >= 0 && lzCmpHigh <= 0 {
					lzVal = base.ValTrue
				} else {
					lzVal = base.ValFalse
				}
			}
		}

		return lzVal
	} // !lz

	lzExprFunc =
		MakeTriExprFunc(lzVars, labels, params, path, triExprFunc) // !lz

	return lzExprFunc
}
