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

// Array reader functions (ARRAY_LENGTH / ARRAY_COUNT / ARRAY_SUM / ARRAY_AVG):
// unary, iterate the array's element bytes and reduce to a scalar number without
// materializing -- no boxing. All share exprArrayReduce, selected by an int
// op-code; base.ArrayReduce returns the Num (or a MISSING/NULL sentinel), which
// the harness formats into the reused lzBufPre on a separate line (op-code %#v and
// buffer varLift %s never share a line).

func init() {
	ExprCatalog["array_length"] = ExprArrayLength
	ExprCatalog["array_count"] = ExprArrayCount
	ExprCatalog["array_sum"] = ExprArraySum
	ExprCatalog["array_avg"] = ExprArrayAvg
}

func ExprArrayLength(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return exprArrayReduce(lzVars, labels, params, path, base.ArrayOpLength)
}

func ExprArrayCount(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return exprArrayReduce(lzVars, labels, params, path, base.ArrayOpCount)
}

func ExprArraySum(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return exprArrayReduce(lzVars, labels, params, path, base.ArrayOpSum)
}

func ExprArrayAvg(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return exprArrayReduce(lzVars, labels, params, path, base.ArrayOpAvg)
}

func exprArrayReduce(lzVars *base.Vars, labels base.Labels, params []interface{},
	path string, op int) (lzExprFunc base.ExprFunc) {
	exprA := params[0].([]interface{})

	var lzBufPre []byte // <== varLift: lzBufPre by path

	lzExprFunc =
		MakeExprFunc(lzVars, labels, exprA, path, "A") // !lz
	lzA := lzExprFunc

	lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
		lzVal = lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"

		lzNum, lzSentinel, lzOk := base.ArrayReduce(op, lzVal)
		if !lzOk {
			lzVal = lzSentinel
		} else {
			lzOut := base.AppendNum(lzBufPre[:0], lzNum)
			lzBufPre = lzOut
			lzVal = base.Val(lzOut)
		}

		return lzVal
	}

	return lzExprFunc
}
