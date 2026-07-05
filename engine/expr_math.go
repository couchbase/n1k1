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
	"math"

	"github.com/couchbase/n1k1/base"
)

// Unary numeric math functions (ABS/CEIL/FLOOR/SQRT/EXP/LN/LOG/SIGN/DEGREES/
// RADIANS/SIN/COS/TAN/ASIN/ACOS/ATAN), evaluated on JSON-number bytes into a
// reused buffer -- no boxing. cbq's func_num.go skeleton is uniform (MISSING ->
// MISSING, non-number -> NULL, else math result), so all share exprMathUnary; the
// per-op math is a real func value (a stdlib math.Abs/... or a base.Math* named
// func) passed in and emitted by name (see base/lzfmt.go).

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
	ExprCatalog["sin"] = ExprSin
	ExprCatalog["cos"] = ExprCos
	ExprCatalog["tan"] = ExprTan
	ExprCatalog["asin"] = ExprAsin
	ExprCatalog["acos"] = ExprAcos
	ExprCatalog["atan"] = ExprAtan
	ExprCatalog["power"] = ExprPower
	ExprCatalog["atan2"] = ExprAtan2
	// ROUND/TRUNC: 1-arg (prec 0) / 2-arg (explicit prec), arity-dispatched.
	ExprCatalog["round_1"] = ExprRound1
	ExprCatalog["round_2"] = ExprRound2
	ExprCatalog["trunc_1"] = ExprTrunc1
	ExprCatalog["trunc_2"] = ExprTrunc2
}

func ExprAbs(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return exprMathUnary(lzVars, labels, params, path, math.Abs)
}

func ExprCeil(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return exprMathUnary(lzVars, labels, params, path, math.Ceil)
}

func ExprFloor(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return exprMathUnary(lzVars, labels, params, path, math.Floor)
}

func ExprSqrt(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return exprMathUnary(lzVars, labels, params, path, math.Sqrt)
}

func ExprExp(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return exprMathUnary(lzVars, labels, params, path, math.Exp)
}

func ExprLn(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return exprMathUnary(lzVars, labels, params, path, math.Log)
}

func ExprLog(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return exprMathUnary(lzVars, labels, params, path, math.Log10)
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

func ExprSin(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return exprMathUnary(lzVars, labels, params, path, math.Sin)
}

func ExprCos(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return exprMathUnary(lzVars, labels, params, path, math.Cos)
}

func ExprTan(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return exprMathUnary(lzVars, labels, params, path, math.Tan)
}

func ExprAsin(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return exprMathUnary(lzVars, labels, params, path, math.Asin)
}

func ExprAcos(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return exprMathUnary(lzVars, labels, params, path, math.Acos)
}

func ExprAtan(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return exprMathUnary(lzVars, labels, params, path, math.Atan)
}

func ExprPower(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return exprMathBi(lzVars, labels, params, path, math.Pow)
}

func ExprAtan2(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return exprMathBi(lzVars, labels, params, path, math.Atan2)
}

// ROUND / TRUNC: numeric, into the reused buffer. 1-arg uses precision 0; the
// 2-arg form takes an integral precision (any sign). cbq skeleton: MISSING if any
// operand MISSING; NULL if the value is non-number or the precision non-integral;
// else base.RoundFloat/TruncFloat (passed by name) formatted via AppendNum. round
// is round-half-to-even (cbq ROUND / ROUND_EVEN).

func ExprRound1(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return exprRoundTrunc1(lzVars, labels, params, path, base.RoundFloat)
}

func ExprTrunc1(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return exprRoundTrunc1(lzVars, labels, params, path, base.TruncFloat)
}

func ExprRound2(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return exprRoundTrunc2(lzVars, labels, params, path, base.RoundFloat)
}

func ExprTrunc2(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return exprRoundTrunc2(lzVars, labels, params, path, base.TruncFloat)
}

// exprRoundTrunc1 is the 1-arg ROUND/TRUNC harness (precision 0).
func exprRoundTrunc1(lzVars *base.Vars, labels base.Labels, params []interface{},
	path string, roundFn func(x float64, prec int) float64) (lzExprFunc base.ExprFunc) {
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
				lzBufPre = base.AppendNum(lzBufPre[:0], base.FloatNum(roundFn(lzNum.Float64(), 0)))
				lzVal = base.Val(lzBufPre)
			}
		}

		return lzVal
	}

	return lzExprFunc
}

// exprRoundTrunc2 is the 2-arg ROUND/TRUNC harness (value, precision).
func exprRoundTrunc2(lzVars *base.Vars, labels base.Labels, params []interface{},
	path string, roundFn func(x float64, prec int) float64) (lzExprFunc base.ExprFunc) {
	var lzBufPre []byte // <== varLift: lzBufPre by path

	biExprFunc := func(lzA, lzB base.ExprFunc, lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) { // !lz
		if LzScope {
			lzVal = lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"
			lzValNum := lzVal

			lzVal = lzB(lzVals, lzYieldErr) // <== emitCaptured: path "B"
			lzValPrec := lzVal

			if base.ValKind(lzValNum) == base.ValKindMissing ||
				base.ValKind(lzValPrec) == base.ValKindMissing {
				lzVal = base.ValMissing
			} else {
				lzNum, lzNumOk := base.ParseNum(lzValNum)
				lzPrec, lzPrecOk := base.IntOperand(lzValPrec)
				if !lzNumOk || !lzPrecOk {
					lzVal = base.ValNull // non-number value / non-integral precision
				} else {
					lzBufPre = base.AppendNum(lzBufPre[:0], base.FloatNum(roundFn(lzNum.Float64(), lzPrec)))
					lzVal = base.Val(lzBufPre)
				}
			}
		}

		return lzVal
	} // !lz

	lzExprFunc =
		MakeBiExprFunc(lzVars, labels, params, path, biExprFunc) // !lz

	return lzExprFunc
}

// exprMathBi is the shared two-operand harness for binary math funcs
// (POWER/ATAN2). cbq skeleton: either operand MISSING -> MISSING, either
// non-number -> NULL, else the func result formatted into the reused lzBufPre.
// Mirrors ExprArithBi. Each operand is captured FROM lzVal (emitCaptured writes
// lzVal).
func exprMathBi(lzVars *base.Vars, labels base.Labels, params []interface{},
	path string, fn func(a, b float64) float64) (lzExprFunc base.ExprFunc) {
	var lzBufPre []byte // <== varLift: lzBufPre by path

	biExprFunc := func(lzA, lzB base.ExprFunc, lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) { // !lz
		if LzScope {
			lzVal = lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"
			lzValA := lzVal

			lzVal = lzB(lzVals, lzYieldErr) // <== emitCaptured: path "B"
			lzValB := lzVal

			if len(lzValA) == 0 || len(lzValB) == 0 {
				lzVal = base.ValMissing // MISSING dominant.
			} else {
				lzNumA, lzOkA := base.ParseNum(lzValA)
				lzNumB, lzOkB := base.ParseNum(lzValB)
				if !lzOkA || !lzOkB {
					lzVal = base.ValNull // non-number operand
				} else {
					lzNumR := base.MathBinApply(fn, lzNumA, lzNumB)
					lzOut := base.AppendNum(lzBufPre[:0], lzNumR)
					lzBufPre = lzOut
					lzVal = base.Val(lzOut)
				}
			}
		}

		return lzVal
	} // !lz

	lzExprFunc =
		MakeBiExprFunc(lzVars, labels, params, path, biExprFunc) // !lz

	return lzExprFunc
}

// exprMathUnary is the shared single-child harness for the unary math funcs.
// cbq's skeleton: MISSING passes through, a non-number operand -> NULL, else the
// func result formatted into the reused lzBufPre. NULL also passes through (it
// isn't a ValKindValue).
func exprMathUnary(lzVars *base.Vars, labels base.Labels, params []interface{},
	path string, fn func(f float64) float64) (lzExprFunc base.ExprFunc) {
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
				lzNumR := base.MathApply(fn, lzNum)
				lzOut := base.AppendNum(lzBufPre[:0], lzNumR)
				lzBufPre = lzOut
				lzVal = base.Val(lzOut)
			}
		}

		return lzVal
	}

	return lzExprFunc
}
