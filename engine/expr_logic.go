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

// Logical AND / OR. Both are binary here (two operands); the optimizer folds
// cbq's n-ary And/Or into right-nested binary applications (see
// ExprTreeOptimize), which is exact under three-valued logic. Each evaluates
// both operands and combines them with N1QL semantics in base.LogicAnd2 /
// base.LogicOr2 -- one leaf call, like ExprNullIf, so the lz codegen inlines the
// two operands (emitCaptured A/B) and emits the reduce as a plain runtime call.
// (Evaluating both operands rather than short-circuiting on a false/true is
// result-identical: SQL scalar operands yield values, not errors, for degenerate
// cases like divide-by-zero.)

func init() {
	ExprCatalog["and"] = ExprAnd
	ExprCatalog["or"] = ExprOr
}

func ExprAnd(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	biExprFunc := func(lzA, lzB base.ExprFunc, lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) { // !lz
		if LzScope {
			lzValA := lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"
			lzValB := lzB(lzVals, lzYieldErr) // <== emitCaptured: path "B"

			lzVal = base.LogicAnd2(lzValA, lzValB)
		}

		return lzVal
	} // !lz

	lzExprFunc =
		MakeBiExprFunc(lzVars, labels, params, path, biExprFunc) // !lz

	return lzExprFunc
}

func ExprOr(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	biExprFunc := func(lzA, lzB base.ExprFunc, lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) { // !lz
		if LzScope {
			lzValA := lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"
			lzValB := lzB(lzVals, lzYieldErr) // <== emitCaptured: path "B"

			lzVal = base.LogicOr2(lzValA, lzValB)
		}

		return lzVal
	} // !lz

	lzExprFunc =
		MakeBiExprFunc(lzVars, labels, params, path, biExprFunc) // !lz

	return lzExprFunc
}
