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

// The conditional-unknown selectors: IFNULL / IFMISSING / IFMISSINGORNULL (and
// NVL == 2-arg IFNULL). Semantics: return the first operand that is "kept" under
// the mode, else NULL; the result is the chosen operand's bytes verbatim
// (zero-copy). Mirrors cbq expression/func_cond_unknown.go.
//
// cbq's forms are variadic; n1k1 handles the two-operand case natively (the
// common one, and all of NVL) and lets >2-operand forms fall back to cbq -- the
// intermed compiler harness is fixed-arity (see glue/expr_optimize.go's guard).

func init() {
	ExprCatalog["ifnull"] = ExprIfNull
	ExprCatalog["ifmissing"] = ExprIfMissing
	ExprCatalog["ifmissingornull"] = ExprIfMissingOrNull
	ExprCatalog["nvl"] = ExprIfNull // NVL(a, b) == IFNULL(a, b).
}

func ExprIfNull(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	return ExprCondUnknown(lzVars, labels, params, path, base.CondIfNull)
}

func ExprIfMissing(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	return ExprCondUnknown(lzVars, labels, params, path, base.CondIfMissing)
}

func ExprIfMissingOrNull(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	return ExprCondUnknown(lzVars, labels, params, path, base.CondIfMissingOrNull)
}

// -----------------------------------------------------

// ExprCondUnknown is the shared two-operand harness. Both operands are evaluated
// (matching cbq's loop, incl. error behavior); the result is the first kept
// operand's value, or NULL if neither is kept.
func ExprCondUnknown(lzVars *base.Vars, labels base.Labels, params []interface{},
	path string, mode int) (lzExprFunc base.ExprFunc) {
	biExprFunc := func(lzA, lzB base.ExprFunc, lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) { // !lz
		if LzScope {
			lzValA := lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"
			lzValB := lzB(lzVals, lzYieldErr) // <== emitCaptured: path "B"

			if base.CondUnknownKeep(mode, lzValA) {
				lzVal = lzValA
			} else if base.CondUnknownKeep(mode, lzValB) {
				lzVal = lzValB
			} else {
				lzVal = base.ValNull
			}
		}

		return lzVal
	} // !lz

	lzExprFunc =
		MakeBiExprFunc(lzVars, labels, params, path, biExprFunc) // !lz

	return lzExprFunc
}
