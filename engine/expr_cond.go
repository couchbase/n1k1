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
// COALESCE == IFMISSINGORNULL). All variadic: return the first operand "kept"
// under the mode, else NULL; the result is the chosen operand's bytes verbatim
// (zero-copy). Mirrors cbq expression/func_cond_unknown.go. N-ary (any operand
// count) via the eager-Vals harness (see ExprCondUnknown).

// ExprCondFuncs maps each conditional-unknown selector to its base.CondIf* mode
// (passed to base.NaryFirstKeptVals). NVL(a, b) returns b when a is NULL *or* MISSING
// (cbq NVL.Evaluate: `first.Type() > NULL ? first : second`), i.e. it's 2-arg
// IFMISSINGORNULL -- not IFNULL, which keeps a MISSING first operand.
var ExprCondFuncs = map[string]int{
	"ifnull": base.CondIfNull, "ifmissing": base.CondIfMissing,
	"ifmissingornull": base.CondIfMissingOrNull, "nvl": base.CondIfMissingOrNull,
}

func init() {
	for name, mode := range ExprCondFuncs {
		ExprCatalog[name] = ExprCondOp(mode)
	}
}

// ExprCondOp closes over a conditional-unknown mode and dispatches to
// ExprCondUnknown. Plain (non-lz) Go.
func ExprCondOp(mode int) base.ExprCatalogFunc {
	return func(lzVars *base.Vars, labels base.Labels, params []interface{}, path string) base.ExprFunc {
		return ExprCondUnknown(lzVars, labels, params, path, mode)
	}
}

// ExprCondUnknown is the eager-Vals handler for the conditional-unknown selectors
// (see CaptureNaryChildren). Every operand IS evaluated (base.NaryFirstKept does
// too), so pre-evaluating them all into lzValsReduce and reducing over the values
// is semantics-preserving and compiles natively. The mode selects the base
// reducer's keep-test; a build-time `if mode == ...` chain (`// !lz`, taken once
// at codegen) picks it so the compiled reduce line carries a literal
// base.CondIf* constant rather than an undefined runtime `mode`.
func ExprCondUnknown(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string, mode int) (lzExprFunc base.ExprFunc) {
	var lzValsReduce base.Vals // <== varLift: lzValsReduce by path

	lzChildren := CaptureNaryChildren(lzVars, labels, params, path) // !lz

	if LzScope {
		lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
			lzValsReduce = lzValsReduce[:0]

			for lzI := range lzChildren { // !lz
				lzVal =
					lzChildren[lzI](lzVals, lzYieldErr) // <== emitCaptured: path strconv.Itoa(lzI)

				lzValsReduce = append(lzValsReduce, lzVal)
			} // !lz

			if mode == base.CondIfNull { // !lz
				lzVal = base.NaryFirstKeptVals(lzValsReduce, base.CondIfNull)
			} else if mode == base.CondIfMissing { // !lz
				lzVal = base.NaryFirstKeptVals(lzValsReduce, base.CondIfMissing)
			} else { // !lz
				lzVal = base.NaryFirstKeptVals(lzValsReduce, base.CondIfMissingOrNull)
			} // !lz

			return lzVal
		}
	}

	return lzExprFunc
}
