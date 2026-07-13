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

// IS [NOT] DISTINCT FROM: binary null-safe (in)equality. Both operands are read as
// bytes and reduced by the shared exprDistinct harness, which passes the matching
// base leaf (base.ValDistinctFrom / ValNotDistinctFrom) -- emitted by name via
// LzExprFmt. No buffers, no boxing. Only 2 ops, so registered directly (a table
// wouldn't pay for itself).

func init() {
	ExprCatalog["is_distinct_from"] = ExprIsDistinctFrom
	ExprCatalog["is_not_distinct_from"] = ExprIsNotDistinctFrom
}

func ExprIsDistinctFrom(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return ExprDistinct(lzVars, labels, params, path, base.ValDistinctFrom)
}

func ExprIsNotDistinctFrom(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return ExprDistinct(lzVars, labels, params, path, base.ValNotDistinctFrom)
}

func ExprDistinct(lzVars *base.Vars, labels base.Labels, params []interface{},
	path string, cmp func(vc *base.ValComparer, a, b base.Val) base.Val) (lzExprFunc base.ExprFunc) {
	biExprFunc := func(lzA, lzB base.ExprFunc, lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) { // !lz
		if LzScope {
			lzValA := lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A", via: lzVal

			lzValB := lzB(lzVals, lzYieldErr) // <== emitCaptured: path "B", via: lzVal

			lzVal = cmp(lzVars.Ctx.ValComparer, lzValA, lzValB)
		}

		return lzVal
	} // !lz

	lzExprFunc = MakeBiExprFunc(lzVars, labels, params, path, biExprFunc) // !lzRHS

	return lzExprFunc
}
