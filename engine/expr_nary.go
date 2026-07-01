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
	"strconv"

	"github.com/couchbase/n1k1/base"
)

// MakeNaryExprFunc constructs handlers for variadic expressions (any number of
// operands), the n-ary analog of MakeBiExprFunc / MakeTriExprFunc.
//
// The child ExprFuncs are built in a `// !lz` loop over the operand params, so
// intermed_build emits that loop verbatim (like op_union's per-child loop) --
// the earlier failure was iterating a slice inside the *codegen'd* eval body.
// The per-row reduce stays a single `// !lz` call to naryExprFunc, keeping the
// actual variadic logic in a plain base helper (base.NaryFirstKept, etc.) that
// intermed does not try to fuse.
func MakeNaryExprFunc(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string, naryExprFunc base.NaryExprFunc) (
	lzExprFunc base.ExprFunc) {
	var lzChildren []base.ExprFunc // !lz
	var lzVals base.Vals           // !lz
	var lzYieldErr base.YieldErr   // !lz

	_, _, _ = lzChildren, lzVals, lzYieldErr // !lz

	for lzI := range params { // !lz
		lzExprFunc =
			MakeExprFunc(lzVars, labels, params[lzI].([]interface{}), path, strconv.Itoa(lzI)) // !lz

		lzChildren = append(lzChildren, lzExprFunc) // !lz
	} // !lz

	if LzScope {
		lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
			lzVal =
				naryExprFunc(lzChildren, lzVals, lzYieldErr) // !lz

			return lzVal
		}
	}

	return lzExprFunc
}
