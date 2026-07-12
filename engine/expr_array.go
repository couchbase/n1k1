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

// ExprArrayReduceFuncs maps each unary array reader to its base.ArrayOp* op-code,
// passed to the shared exprArrayReduce harness (one ArrayEach pass -> a scalar).
var ExprArrayReduceFuncs = map[string]int{
	"array_length": base.ArrayOpLength, "array_count": base.ArrayOpCount,
	"array_sum": base.ArrayOpSum, "array_avg": base.ArrayOpAvg,
}

func init() {
	for name, op := range ExprArrayReduceFuncs {
		ExprCatalog[name] = ExprArrayReduceOp(op)
	}
	ExprCatalog["array_min"] = ExprArrayMin
	ExprCatalog["array_max"] = ExprArrayMax
	ExprCatalog["array_contains"] = ExprArrayContains
	ExprCatalog["array_position"] = ExprArrayPosition
	// Array structure builders (2-arg forms): splice element bytes into the reused
	// buffer -- no boxing, no element re-encode. See base.Array{Append,Prepend,Concat}.
	ExprCatalog["array_append"] = ExprArrayAppend
	ExprCatalog["array_prepend"] = ExprArrayPrepend
	ExprCatalog["array_concat"] = ExprArrayConcat
	// Array reshaping builders: unary sort/reverse (share the unary-array-to-buffer
	// harness) and the 2-arg flatten. See base.Array{Sort,Reverse,Flatten}.
	ExprCatalog["array_sort"] = ExprArraySort
	ExprCatalog["array_reverse"] = ExprArrayReverse
	ExprCatalog["array_flatten"] = ExprArrayFlatten
}

// ExprArrayReduceOp closes over an array reader's op-code and defers to the
// shared harness -- plain (non-lz) Go, codegen-transparent.
func ExprArrayReduceOp(op int) base.ExprCatalogFunc {
	return func(lzVars *base.Vars, labels base.Labels, params []interface{}, path string) base.ExprFunc {
		return ExprArrayReduce(lzVars, labels, params, path, op)
	}
}

func ExprArrayReduce(lzVars *base.Vars, labels base.Labels, params []interface{},
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

// ARRAY_MIN / ARRAY_MAX / ARRAY_SORT / ARRAY_REVERSE: unary array ops that read one
// array and emit a value (an element, or a reshaped array) into the reused buffer.
// read is the base func (emitted by name in the compiled path); it shares an emitted
// line with the reused lzBufPre (the codegen preserves fmt-arg order across a func
// placeholder and a varLift buffer -- see cmd/intermed_build).
func ExprArrayMin(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return ExprArrayUnaryBuf(lzVars, labels, params, path, base.ArrayMin)
}

func ExprArrayMax(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return ExprArrayUnaryBuf(lzVars, labels, params, path, base.ArrayMax)
}

// ExprArraySort is ARRAY_SORT(arr): unary, the array's elements in collation order
// into the reused buffer (or a MISSING/NULL sentinel). See base.ArraySort.
func ExprArraySort(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return ExprArrayUnaryBuf(lzVars, labels, params, path, base.ArraySort)
}

// ExprArrayReverse is ARRAY_REVERSE(arr): unary, the array's elements in reverse
// order into the reused buffer (or a MISSING/NULL sentinel). See base.ArrayReverse.
func ExprArrayReverse(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return ExprArrayUnaryBuf(lzVars, labels, params, path, base.ArrayReverse)
}

func ExprArrayUnaryBuf(lzVars *base.Vars, labels base.Labels, params []interface{},
	path string, read func(vc *base.ValComparer, v base.Val, bufPre []byte) (base.Val, []byte)) (lzExprFunc base.ExprFunc) {
	exprA := params[0].([]interface{})

	var lzBufPre []byte // <== varLift: lzBufPre by path

	lzExprFunc =
		MakeExprFunc(lzVars, labels, exprA, path, "A") // !lz
	lzA := lzExprFunc

	lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
		lzVal = lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"

		lzVal, lzBufPre = read(lzVars.Ctx.ValComparer, lzVal, lzBufPre)

		return lzVal
	}

	return lzExprFunc
}

// ARRAY_CONTAINS(arr, v): binary -> bool (no buffer). Operands captured FROM lzVal.
func ExprArrayContains(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	biExprFunc := func(lzA, lzB base.ExprFunc, lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) { // !lz
		if LzScope {
			lzVal = lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"
			lzValA := lzVal

			lzVal = lzB(lzVals, lzYieldErr) // <== emitCaptured: path "B"
			lzValB := lzVal

			lzVal = base.ArrayContains(lzVars.Ctx.ValComparer, lzValA, lzValB)
		}

		return lzVal
	} // !lz

	lzExprFunc =
		MakeBiExprFunc(lzVars, labels, params, path, biExprFunc) // !lz

	return lzExprFunc
}

// ARRAY_POSITION(arr, v): binary -> index (or -1) into the reused buffer. The
// index (from base.ArrayPositionIndex, no buffer) is formatted separately.
func ExprArrayPosition(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	var lzBufPre []byte // <== varLift: lzBufPre by path

	biExprFunc := func(lzA, lzB base.ExprFunc, lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) { // !lz
		if LzScope {
			lzVal = lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"
			lzValA := lzVal

			lzVal = lzB(lzVals, lzYieldErr) // <== emitCaptured: path "B"
			lzValB := lzVal

			lzIdx, lzSentinel, lzOk := base.ArrayPositionIndex(lzVars.Ctx.ValComparer, lzValA, lzValB)
			if !lzOk {
				lzVal = lzSentinel
			} else {
				lzOut := base.AppendNum(lzBufPre[:0], base.IntNum(int64(lzIdx)))
				lzBufPre = lzOut
				lzVal = base.Val(lzOut)
			}
		}

		return lzVal
	} // !lz

	lzExprFunc =
		MakeBiExprFunc(lzVars, labels, params, path, biExprFunc) // !lz

	return lzExprFunc
}

// ExprArrayAppend is ARRAY_APPEND(arr, val) (2-arg): binary, splices val onto the
// end of arr into the reused buffer (or a MISSING/NULL sentinel) -- no boxing. See
// base.ArrayAppend. (cbq's variadic >2-arg form falls back to boxed.)
func ExprArrayAppend(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	var lzBufPre []byte // <== varLift: lzBufPre by path

	biExprFunc := func(lzA, lzB base.ExprFunc, lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) { // !lz
		if LzScope {
			lzVal = lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"
			lzValArr := lzVal

			lzVal = lzB(lzVals, lzYieldErr) // <== emitCaptured: path "B"
			lzValElem := lzVal

			lzOut, lzSentinel, lzOk := base.ArrayAppend(lzValArr, lzValElem, lzBufPre)
			if !lzOk {
				lzVal = lzSentinel
			} else {
				lzBufPre = lzOut
				lzVal = base.Val(lzOut)
			}
		}

		return lzVal
	} // !lz

	lzExprFunc =
		MakeBiExprFunc(lzVars, labels, params, path, biExprFunc) // !lz

	return lzExprFunc
}

// ExprArrayPrepend is ARRAY_PREPEND(val, arr) (2-arg): binary, splices val onto the
// FRONT of arr (val is the FIRST operand, arr the second) into the reused buffer (or
// a MISSING/NULL sentinel) -- no boxing. See base.ArrayPrepend.
func ExprArrayPrepend(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	var lzBufPre []byte // <== varLift: lzBufPre by path

	biExprFunc := func(lzA, lzB base.ExprFunc, lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) { // !lz
		if LzScope {
			lzVal = lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"
			lzValElem := lzVal

			lzVal = lzB(lzVals, lzYieldErr) // <== emitCaptured: path "B"
			lzValArr := lzVal

			lzOut, lzSentinel, lzOk := base.ArrayPrepend(lzValElem, lzValArr, lzBufPre)
			if !lzOk {
				lzVal = lzSentinel
			} else {
				lzBufPre = lzOut
				lzVal = base.Val(lzOut)
			}
		}

		return lzVal
	} // !lz

	lzExprFunc =
		MakeBiExprFunc(lzVars, labels, params, path, biExprFunc) // !lz

	return lzExprFunc
}

// ExprArrayConcat is ARRAY_CONCAT(arr1, arr2) (2-arg): binary, joins the two arrays'
// elements into the reused buffer (or a MISSING/NULL sentinel) -- no boxing. See
// base.ArrayConcat.
func ExprArrayConcat(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	var lzBufPre []byte // <== varLift: lzBufPre by path

	biExprFunc := func(lzA, lzB base.ExprFunc, lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) { // !lz
		if LzScope {
			lzVal = lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"
			lzValArr1 := lzVal

			lzVal = lzB(lzVals, lzYieldErr) // <== emitCaptured: path "B"
			lzValArr2 := lzVal

			lzOut, lzSentinel, lzOk := base.ArrayConcat(lzValArr1, lzValArr2, lzBufPre)
			if !lzOk {
				lzVal = lzSentinel
			} else {
				lzBufPre = lzOut
				lzVal = base.Val(lzOut)
			}
		}

		return lzVal
	} // !lz

	lzExprFunc =
		MakeBiExprFunc(lzVars, labels, params, path, biExprFunc) // !lz

	return lzExprFunc
}

// ExprArrayFlatten is ARRAY_FLATTEN(arr, depth) (2-arg): binary, splices nested
// arrays into arr up to depth levels deep into the reused buffer (or a MISSING/NULL
// sentinel) -- no boxing. See base.ArrayFlatten.
func ExprArrayFlatten(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	var lzBufPre []byte // <== varLift: lzBufPre by path

	biExprFunc := func(lzA, lzB base.ExprFunc, lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) { // !lz
		if LzScope {
			lzVal = lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"
			lzValArr := lzVal

			lzVal = lzB(lzVals, lzYieldErr) // <== emitCaptured: path "B"
			lzValDepth := lzVal

			lzOut, lzSentinel, lzOk := base.ArrayFlatten(lzValArr, lzValDepth, lzBufPre)
			if !lzOk {
				lzVal = lzSentinel
			} else {
				lzBufPre = lzOut
				lzVal = base.Val(lzOut)
			}
		}

		return lzVal
	} // !lz

	lzExprFunc =
		MakeBiExprFunc(lzVars, labels, params, path, biExprFunc) // !lz

	return lzExprFunc
}
