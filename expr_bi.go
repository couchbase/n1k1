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
	"github.com/couchbase/n1k1/base"
)

func init() {
	ExprCatalog["or"] = ExprOr
	ExprCatalog["and"] = ExprAnd
}

// MakeBiExprFunc is for constructing handlers for two-argument or
// "binary" expressions.
func MakeBiExprFunc(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string, biExprFunc base.BiExprFunc) (
	lzExprFunc base.ExprFunc) {
	exprA := params[0].([]interface{})
	exprB := params[1].([]interface{})

	var lzA base.ExprFunc        // !lz
	var lzB base.ExprFunc        // !lz
	var lzVals base.Vals         // !lz
	var lzYieldErr base.YieldErr // !lz

	_, _, _, _ = lzA, lzB, lzVals, lzYieldErr // !lz

	if LzScope {
		lzExprFunc =
			MakeExprFunc(lzVars, labels, exprA, path, "A") // !lz
		lzA := lzExprFunc

		lzExprFunc =
			MakeExprFunc(lzVars, labels, exprB, path, "B") // !lz
		lzB := lzExprFunc

		lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
			lzVal =
				biExprFunc(lzA, lzB, lzVals, lzYieldErr) // !lz

			return lzVal
		}
	}

	return lzExprFunc
}

// -----------------------------------------------------

func ExprOr(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	biExprFunc := func(lzA, lzB base.ExprFunc, lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) { // !lz
		// TODO: This might not match N1QL logical OR semantics.
		lzVal = lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"
		if !base.ValEqualTrue(lzVal) {
			lzVal = lzB(lzVals, lzYieldErr) // <== emitCaptured: path "B"
		}

		return lzVal
	} // !lz

	lzExprFunc =
		MakeBiExprFunc(lzVars, labels, params, path, biExprFunc) // !lz

	return lzExprFunc
}

// -----------------------------------------------------

func ExprAnd(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	biExprFunc := func(lzA, lzB base.ExprFunc, lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) { // !lz
		// TODO: This might not match N1QL logical AND semantics.
		lzVal = lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"
		if base.ValEqualTrue(lzVal) {
			lzVal = lzB(lzVals, lzYieldErr) // <== emitCaptured: path "B"
		}

		return lzVal
	} // !lz

	lzExprFunc =
		MakeBiExprFunc(lzVars, labels, params, path, biExprFunc) // !lz

	return lzExprFunc
}
