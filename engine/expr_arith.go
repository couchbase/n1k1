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

// ExprArithOps is the (name -> base.Arith* leaf + divide-by-zero-warning flag) table
// for the binary arithmetic operators. Each leaf has the uniform (a, b Num) (Num,
// bool) shape, emitted by name in the compiled path (see LzExprFmt). '/' and DIV
// request the divide-by-zero advisory (warnZero); '%'/MOD stay silent. neg is a
// unary op with its own body, registered directly.
var ExprArithOps = map[string]struct {
	op       func(a, b base.Num) (base.Num, bool)
	warnZero bool
}{
	"add":  {base.ArithAdd, false},
	"sub":  {base.ArithSub, false},
	"mult": {base.ArithMult, false},
	"div":  {base.ArithDiv, true},
	"mod":  {base.ArithMod, false},
	"idiv": {base.ArithIDiv, true},
	"imod": {base.ArithIMod, false},
}

func init() {
	for name, a := range ExprArithOps {
		ExprCatalog[name] = ExprArithOp(a.op, a.warnZero)
	}
	ExprCatalog["neg"] = ExprNeg
}

// -----------------------------------------------------

// ExprArithOp adapts an arith leaf + warnZero into an ExprCatalogFunc by closing
// over them and deferring to the shared ExprArithBi harness. Plain Go (no lz), so
// intermed_build emits it verbatim and the leaf flows unchanged to the emission
// site.
func ExprArithOp(op func(a, b base.Num) (base.Num, bool), warnZero bool) base.ExprCatalogFunc {
	return func(lzVars *base.Vars, labels base.Labels, params []interface{}, path string) base.ExprFunc {
		return ExprArithBi(lzVars, labels, params, path, op, warnZero)
	}
}

// -----------------------------------------------------

// ExprArithBi handles the binary arithmetic operators. arith is the numeric
// operation (returning ok=false only on divide/mod-by-zero); warnZero requests
// the divide-by-zero advisory. The N1QL rule: if either operand is MISSING the
// result is MISSING; else if either operand is not a number the result is NULL;
// else compute (divide/mod-by-zero also yields NULL).
func ExprArithBi(lzVars *base.Vars, labels base.Labels, params []interface{},
	path string, arith func(a, b base.Num) (base.Num, bool), warnZero bool) (
	lzExprFunc base.ExprFunc) {
	var lzBufPre []byte // <== varLift: lzBufPre by path

	biExprFunc := func(lzA, lzB base.ExprFunc, lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) { // !lz
		if LzScope {
			// Capture each operand FROM the shared lzVal register (emitCaptured
			// replaces the marked line with the child's code, which writes lzVal);
			// a direct lzValX := lzX(...) bind is dropped in the compiled path.
			// Mirrors ExprCmp.
			lzVal = lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"
			lzValA := lzVal

			lzVal = lzB(lzVals, lzYieldErr) // <== emitCaptured: path "B"
			lzValB := lzVal

			lzVal, lzBufPre = base.ArithBiMissingDominant(lzValA, lzValB, lzBufPre, arith, warnZero, lzVars)
		}

		return lzVal
	} // !lz

	lzExprFunc = MakeBiExprFunc(lzVars, labels, params, path, biExprFunc) // !lzRHS

	return lzExprFunc
}

// -----------------------------------------------------

// ExprNeg handles unary negation. MISSING -> MISSING, non-number -> NULL, else
// -operand (matching expression/arith_neg.go).
func ExprNeg(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	exprA := params[0].([]interface{})

	var lzBufPre []byte // <== varLift: lzBufPre by path

	lzA := MakeExprFunc(lzVars, labels, exprA, path, "A") // !lzRHS, via: lzExprFunc

	lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
		lzVal = lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"

		lzVal, lzBufPre = base.ArithUnaryUnknownPassthrough(lzVal, lzBufPre, base.Num.Neg)

		return lzVal
	}

	return lzExprFunc
}
