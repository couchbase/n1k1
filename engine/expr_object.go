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

// Object / collection reader functions (OBJECT_LENGTH, POLY_LENGTH): unary,
// count the operand's bytes and format a JSON int (or a MISSING/NULL sentinel)
// into the reused buffer -- no boxing. Mirrors the array reader ops
// (expr_array.go); base.LengthReader returns the count via an int op-code (kept
// off the reused-buffer line, so op-code %#v and the buffer varLift %s never
// share a codegen line).

func init() {
	ExprCatalog["object_length"] = ExprObjectLength
	ExprCatalog["poly_length"] = ExprPolyLength
}

func ExprObjectLength(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return exprLenReader(lzVars, labels, params, path, base.LenObject)
}

func ExprPolyLength(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return exprLenReader(lzVars, labels, params, path, base.LenPoly)
}

func exprLenReader(lzVars *base.Vars, labels base.Labels, params []interface{},
	path string, op int) (lzExprFunc base.ExprFunc) {
	exprA := params[0].([]interface{})

	var lzBufPre []byte // <== varLift: lzBufPre by path

	lzExprFunc =
		MakeExprFunc(lzVars, labels, exprA, path, "A") // !lz
	lzA := lzExprFunc

	lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
		lzVal = lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"

		lzN, lzSentinel, lzOk := base.LengthReader(op, lzVal)
		if !lzOk {
			lzVal = lzSentinel
		} else {
			lzOut := strconv.AppendInt(lzBufPre[:0], int64(lzN), 10)
			lzBufPre = lzOut
			lzVal = base.Val(lzOut)
		}

		return lzVal
	}

	return lzExprFunc
}
