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

package engine

import (
	"github.com/couchbase/n1k1/base"
)

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
		lzA := MakeExprFunc(lzVars, labels, exprA, path, "A") // !lzRHS, via: lzExprFunc

		lzB := MakeExprFunc(lzVars, labels, exprB, path, "B") // !lzRHS, via: lzExprFunc

		lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
			lzVal =
				biExprFunc(lzA, lzB, lzVals, lzYieldErr) // !lz

			return lzVal
		}
	}

	return lzExprFunc
}
