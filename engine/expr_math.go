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

// Unary numeric math functions (ABS/CEIL/FLOOR/SQRT/EXP/LN/LOG/SIGN/DEGREES/
// RADIANS), evaluated on JSON-number bytes into a reused buffer -- no boxing.
// cbq's func_num.go skeleton is uniform (MISSING -> MISSING, non-number -> NULL,
// else math result), so all share exprMathUnary; the per-op math lives in
// base.MathUnary, selected by an int op-code (NOT a func value, which the lz
// codegen can't emit).

func init() {
	ExprCatalog["abs"] = ExprAbs
	ExprCatalog["ceil"] = ExprCeil
	ExprCatalog["floor"] = ExprFloor
	ExprCatalog["sqrt"] = ExprSqrt
	ExprCatalog["exp"] = ExprExp
	ExprCatalog["ln"] = ExprLn
	ExprCatalog["log"] = ExprLog
	ExprCatalog["sign"] = ExprSign
	ExprCatalog["degrees"] = ExprDegrees
	ExprCatalog["radians"] = ExprRadians
}

func ExprAbs(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return exprMathUnary(lzVars, labels, params, path, base.MathAbs)
}

func ExprCeil(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return exprMathUnary(lzVars, labels, params, path, base.MathCeil)
}

func ExprFloor(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return exprMathUnary(lzVars, labels, params, path, base.MathFloor)
}

func ExprSqrt(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return exprMathUnary(lzVars, labels, params, path, base.MathSqrt)
}

func ExprExp(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return exprMathUnary(lzVars, labels, params, path, base.MathExp)
}

func ExprLn(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return exprMathUnary(lzVars, labels, params, path, base.MathLn)
}

func ExprLog(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return exprMathUnary(lzVars, labels, params, path, base.MathLog)
}

func ExprSign(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return exprMathUnary(lzVars, labels, params, path, base.MathSign)
}

func ExprDegrees(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return exprMathUnary(lzVars, labels, params, path, base.MathDegrees)
}

func ExprRadians(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return exprMathUnary(lzVars, labels, params, path, base.MathRadians)
}

// exprMathUnary is the shared single-child harness for the unary math funcs.
// cbq's skeleton: MISSING passes through, a non-number operand -> NULL, else the
// op-code result formatted into the reused lzBufPre. NULL also passes through (it
// isn't a ValKindValue). The op-code (%#v) and the buffer (varLift %s) are kept
// on separate lines -- the codegen mis-orders args if both share one line.
func exprMathUnary(lzVars *base.Vars, labels base.Labels, params []interface{},
	path string, op int) (lzExprFunc base.ExprFunc) {
	exprA := params[0].([]interface{})

	var lzBufPre []byte // <== varLift: lzBufPre by path

	lzExprFunc =
		MakeExprFunc(lzVars, labels, exprA, path, "A") // !lz
	lzA := lzExprFunc

	lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
		lzVal = lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"

		if base.ValKind(lzVal) == base.ValKindValue {
			lzNum, lzOk := base.ParseNum(lzVal)
			if !lzOk {
				lzVal = base.ValNull // non-number operand
			} else {
				lzNumR := base.MathApply(op, lzNum)
				lzOut := base.AppendNum(lzBufPre[:0], lzNumR)
				lzBufPre = lzOut
				lzVal = base.Val(lzOut)
			}
		}

		return lzVal
	}

	return lzExprFunc
}
