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

// Native array navigation. ExprElement is `arr[idx]` (cbq expression Element,
// nav_element.go) -- binary, so it rides the shared MakeBiExprFunc harness; all
// the three-valued / index semantics live in base.ValElement. (Slice navigation
// `arr[start:end]` is not yet native -- see DESIGN-exprs.md for the blocker.)

func init() {
	ExprCatalog["element"] = ExprElement
}

func ExprElement(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	var lzBufPre []byte // <== varLift: lzBufPre by path

	biExprFunc := func(lzA, lzB base.ExprFunc, lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) { // !lz
		if LzScope {
			lzValArr := lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"
			lzValIdx := lzB(lzVals, lzYieldErr) // <== emitCaptured: path "B"

			lzVal, lzBufPre = base.ValElement(lzValArr, lzValIdx, lzBufPre)
		}

		return lzVal
	} // !lz

	lzExprFunc =
		MakeBiExprFunc(lzVars, labels, params, path, biExprFunc) // !lz

	return lzExprFunc
}
