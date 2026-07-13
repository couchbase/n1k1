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

// NULLIF(a, b) and MISSINGIF(a, b): MISSING if either operand is MISSING; NULL if
// either is NULL; else NULL (nullif) / MISSING (missingif) when a equals b, else
// the first operand a. Logic in base.NullMissingIf.

func init() {
	ExprCatalog["nullif"] = ExprNullIf
	ExprCatalog["missingif"] = ExprMissingIf
}

func ExprNullIf(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	return ExprNullMissingIf(lzVars, labels, params, path, base.ValNull)
}

func ExprMissingIf(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	return ExprNullMissingIf(lzVars, labels, params, path, base.ValMissing)
}

func ExprNullMissingIf(lzVars *base.Vars, labels base.Labels, params []interface{},
	path string, whenEqual base.Val) (lzExprFunc base.ExprFunc) {
	var lzWhenEqual base.Val = whenEqual // <== varLift: lzWhenEqual by path

	biExprFunc := func(lzA, lzB base.ExprFunc, lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) { // !lz
		if LzScope {
			// Capture each operand FROM the shared lzVal register (emitCaptured
			// child code writes lzVal); a direct lzValX := lzX(...) bind is lost
			// in the compiled path. Mirrors ExprCmp.
			lzValA := lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A", via: lzVal

			lzValB := lzB(lzVals, lzYieldErr) // <== emitCaptured: path "B", via: lzVal

			lzVal = base.NullMissingIf(lzVars.Ctx.ValComparer, lzValA, lzValB, lzWhenEqual)
		}

		return lzVal
	} // !lz

	lzExprFunc = MakeBiExprFunc(lzVars, labels, params, path, biExprFunc) // !lzRHS

	return lzExprFunc
}
