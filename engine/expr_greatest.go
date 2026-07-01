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
	naryExprFunc := func(lzChildren []base.ExprFunc, lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) { // !lz
		lzVal = base.GreatestLeast(lzVars.Ctx.ValComparer, lzChildren, lzVals, lzYieldErr, true)

		return lzVal
	} // !lz

	lzExprFunc =
		MakeNaryExprFunc(lzVars, labels, params, path, naryExprFunc) // !lz

	return lzExprFunc
}

func ExprLeast(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	naryExprFunc := func(lzChildren []base.ExprFunc, lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) { // !lz
		lzVal = base.GreatestLeast(lzVars.Ctx.ValComparer, lzChildren, lzVals, lzYieldErr, false)

		return lzVal
	} // !lz

	lzExprFunc =
		MakeNaryExprFunc(lzVars, labels, params, path, naryExprFunc) // !lz

	return lzExprFunc
}
