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
// func) passed in and emitted by name (see base.LzExprFmt).

// The per-op leaf for each math family is a real func value emitted by name (see
// LzExprFmt), so each op is just a (name -> leaf) table row rather than a
// hand-written constructor. init() registers each row via the matching xxxOp
// adapter (below), which closes over the leaf and defers to the shared harness.
// Because the adapter + the table + init are all plain (non-lz) Go, intermed_build
// passes them through untouched and the leaf still reaches the harness's LzExprFmt
// emission site -- so both the interpreter and the compiled path stay identical to
// the old per-op funcs. See DESIGN-exprs.md "Codegen ergonomics".

// mathUnaryFuncs: ABS/CEIL/... -- a stdlib math.Abs/... or a base.Math* named func.
var mathUnaryFuncs = map[string]func(float64) float64{
	"abs": math.Abs, "ceil": math.Ceil, "floor": math.Floor, "sqrt": math.Sqrt,
	"exp": math.Exp, "ln": math.Log, "log": math.Log10, "sign": base.MathSign,
	"degrees": base.MathDegrees, "radians": base.MathRadians,
	"sin": math.Sin, "cos": math.Cos, "tan": math.Tan,
	"asin": math.Asin, "acos": math.Acos, "atan": math.Atan,
}

// mathBiFuncs: the binary math funcs, as always-ok Num leaves (POWER/ATAN2).
var mathBiFuncs = map[string]func(a, b base.Num) (base.Num, bool){
	"power": base.MathPow, "atan2": base.MathAtan2,
}

// ROUND/TRUNC share one (name -> rounder) table; init registers both arities per
// rounder (_1 = 1-arg, precision 0; _2 = 2-arg, explicit precision -- the conv
// layer arity-dispatches). round is round-half-to-even.
var roundTruncFuncs = map[string]func(x float64, prec int) float64{
	"round": base.RoundFloat, "trunc": base.TruncFloat,
}

func init() {
	for name, fn := range mathUnaryFuncs {
		ExprCatalog[name] = exprMathUnaryOp(fn)
	}
	for name, fn := range mathBiFuncs {
		ExprCatalog[name] = exprMathBiOp(fn)
	}
	for name, fn := range roundTruncFuncs {
		ExprCatalog[name+"_1"] = exprRoundTrunc1Op(fn)
		ExprCatalog[name+"_2"] = exprRoundTrunc2Op(fn)
	}
}

// The xxxOp adapters turn a per-op leaf into an ExprCatalogFunc by closing over
// the leaf and deferring to the shared harness. Plain Go (no lz), so intermed_build
// emits them verbatim and the leaf flows unchanged to the harness emission site.

func exprMathUnaryOp(fn func(float64) float64) base.ExprCatalogFunc {
	return func(lzVars *base.Vars, labels base.Labels, params []interface{}, path string) base.ExprFunc {
		return exprMathUnary(lzVars, labels, params, path, fn)
	}
}

func exprMathBiOp(fn func(a, b base.Num) (base.Num, bool)) base.ExprCatalogFunc {
	return func(lzVars *base.Vars, labels base.Labels, params []interface{}, path string) base.ExprFunc {
		return exprMathBi(lzVars, labels, params, path, fn)
	}
}

func exprRoundTrunc1Op(roundFn func(x float64, prec int) float64) base.ExprCatalogFunc {
	return func(lzVars *base.Vars, labels base.Labels, params []interface{}, path string) base.ExprFunc {
		return exprRoundTrunc1(lzVars, labels, params, path, roundFn)
	}
}

func exprRoundTrunc2Op(roundFn func(x float64, prec int) float64) base.ExprCatalogFunc {
	return func(lzVars *base.Vars, labels base.Labels, params []interface{}, path string) base.ExprFunc {
		return exprRoundTrunc2(lzVars, labels, params, path, roundFn)
	}
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

		lzVal, lzBufPre = base.MathUnknownPassthroughRound1(lzVal, lzBufPre, roundFn)

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

			lzVal, lzBufPre = base.MathMissingDominantRound2(lzValNum, lzValPrec, lzBufPre, roundFn)
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
	path string, fn func(a, b base.Num) (base.Num, bool)) (lzExprFunc base.ExprFunc) {
	var lzBufPre []byte // <== varLift: lzBufPre by path

	biExprFunc := func(lzA, lzB base.ExprFunc, lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) { // !lz
		if LzScope {
			lzVal = lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"
			lzValA := lzVal

			lzVal = lzB(lzVals, lzYieldErr) // <== emitCaptured: path "B"
			lzValB := lzVal

			lzVal, lzBufPre = base.ArithBiMissingDominant(lzValA, lzValB, lzBufPre, fn, false, lzVars)
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

		lzVal, lzBufPre = base.MathUnknownPassthroughUnary(lzVal, lzBufPre, fn)

		return lzVal
	}

	return lzExprFunc
}
