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

// Native arithmetic expressions (+ - * / % DIV MOD and unary -), evaluated on
// JSON-number bytes into a reused buffer -- no value.Value boxing. The numeric
// core lives in base/arith.go (ported to mirror cbq's value.NumberValue); this
// file adds the N1QL three-valued propagation harness (MISSING dominant, then
// NULL for any non-number operand), matching cbq's arith_*.go Evaluate() exactly.

func init() {
	ExprCatalog["add"] = ExprAdd
	ExprCatalog["sub"] = ExprSub
	ExprCatalog["mult"] = ExprMult
	ExprCatalog["div"] = ExprDiv
	ExprCatalog["mod"] = ExprMod
	ExprCatalog["idiv"] = ExprIDiv
	ExprCatalog["imod"] = ExprIMod
	ExprCatalog["neg"] = ExprNeg
}

// -----------------------------------------------------

func ExprAdd(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	return ExprArithBi(lzVars, labels, params, path, base.ArithAdd)
}

func ExprSub(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	return ExprArithBi(lzVars, labels, params, path, base.ArithSub)
}

func ExprMult(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	return ExprArithBi(lzVars, labels, params, path, base.ArithMult)
}

func ExprDiv(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	return ExprArithBi(lzVars, labels, params, path, base.ArithDiv)
}

func ExprMod(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	return ExprArithBi(lzVars, labels, params, path, base.ArithMod)
}

func ExprIDiv(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	return ExprArithBi(lzVars, labels, params, path, base.ArithIDiv)
}

func ExprIMod(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	return ExprArithBi(lzVars, labels, params, path, base.ArithIMod)
}

// -----------------------------------------------------

// ExprArithBi handles the binary arithmetic operators. op is a base.Arith*
// op-code (early-bound). The N1QL rule: if either operand is MISSING the result
// is MISSING; else if either operand is not a number the result is NULL; else
// compute (divide/mod-by-zero also yields NULL).
func ExprArithBi(lzVars *base.Vars, labels base.Labels, params []interface{},
	path string, op int) (lzExprFunc base.ExprFunc) {
	var lzBufPre []byte // <== varLift: lzBufPre by path

	// Only '/' (Div) and 'DIV' (IDiv) emit a divide-by-zero warning in cbq;
	// '%'/'MOD' return NULL silently.
	warnZero := op == base.ArithDiv || op == base.ArithIDiv

	biExprFunc := func(lzA, lzB base.ExprFunc, lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) { // !lz
		if LzScope {
			lzValA := lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"
			lzValB := lzB(lzVals, lzYieldErr) // <== emitCaptured: path "B"

			if len(lzValA) == 0 || len(lzValB) == 0 {
				lzVal = base.ValMissing // MISSING dominant.
			} else {
				lzNumA, lzOkA := base.ParseNum(lzValA)
				lzNumB, lzOkB := base.ParseNum(lzValB)
				if !lzOkA || !lzOkB {
					lzVal = base.ValNull // Non-number operand.
				} else {
					lzNumR, lzOkR := base.ArithApply(op, lzNumA, lzNumB)
					if !lzOkR {
						lzVal = base.ValNull // Divide/mod by zero.
						if warnZero && lzVars.Ctx.Warn != nil {
							lzVars.Ctx.Warn(base.WarnDivideByZero)
						}
					} else {
						lzBufPre = base.AppendNum(lzBufPre[:0], lzNumR)
						lzVal = base.Val(lzBufPre)
					}
				}
			}
		}

		return lzVal
	} // !lz

	lzExprFunc =
		MakeBiExprFunc(lzVars, labels, params, path, biExprFunc) // !lz

	return lzExprFunc
}

// -----------------------------------------------------

// ExprNeg handles unary negation. MISSING -> MISSING, non-number -> NULL, else
// -operand (matching expression/arith_neg.go).
func ExprNeg(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	exprA := params[0].([]interface{})

	var lzBufPre []byte // <== varLift: lzBufPre by path

	lzExprFunc =
		MakeExprFunc(lzVars, labels, exprA, path, "A") // !lz
	lzA := lzExprFunc

	lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
		lzVal = lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"

		if len(lzVal) == 0 {
			lzVal = base.ValMissing
		} else {
			lzNum, lzOk := base.ParseNum(lzVal)
			if !lzOk {
				lzVal = base.ValNull
			} else {
				lzBufPre = base.AppendNum(lzBufPre[:0], lzNum.Neg())
				lzVal = base.Val(lzBufPre)
			}
		}

		return lzVal
	}

	return lzExprFunc
}
