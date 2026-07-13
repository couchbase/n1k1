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
	// OBJECT_NAMES/OBJECT_VALUES/OBJECT_PAIRS: name-sorted structure builders over
	// the operand object, each emitting into the reused buffer (see base.Object*).
	ExprCatalog["object_names"] = ExprObjectNames
	ExprCatalog["object_values"] = ExprObjectValues
	ExprCatalog["object_pairs"] = ExprObjectPairs
	// Object mutating builders: key-sorted re-emit with one field added/set/removed
	// or two objects merged. ADD/PUT are ternary (obj,key,val); REMOVE/CONCAT are the
	// 2-arg forms (variadic >2 falls back). See base.Object{Add,Put,Remove,Concat}.
	ExprCatalog["object_add"] = ExprObjectAdd
	ExprCatalog["object_put"] = ExprObjectPut
	ExprCatalog["object_remove"] = ExprObjectRemove
	ExprCatalog["object_concat"] = ExprObjectConcat
}

// ExprObjectAdd is OBJECT_ADD(obj, key, val): ternary, adds a NEW field (never
// overwrites) and re-emits key-sorted into the reused buffer -- no boxing. See
// base.ObjectAdd.
func ExprObjectAdd(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	var lzBufPre []byte // <== varLift: lzBufPre by path

	triExprFunc := func(lzA, lzB, lzC base.ExprFunc, lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) { // !lz
		if LzScope {
			lzVal = lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"
			lzValObj := lzVal

			lzVal = lzB(lzVals, lzYieldErr) // <== emitCaptured: path "B"
			lzValKey := lzVal

			lzVal = lzC(lzVals, lzYieldErr) // <== emitCaptured: path "C"
			lzValElem := lzVal

			lzOut, lzSentinel, lzOk := base.ObjectAdd(lzVars.Ctx.ValComparer, lzValObj, lzValKey, lzValElem, lzBufPre)
			if !lzOk {
				lzVal = lzSentinel
			} else {
				lzBufPre = lzOut
				lzVal = base.Val(lzOut)
			}
		}

		return lzVal
	} // !lz

	lzExprFunc = MakeTriExprFunc(lzVars, labels, params, path, triExprFunc) // !lzRHS

	return lzExprFunc
}

// ExprObjectPut is OBJECT_PUT(obj, key, val): ternary, sets field key to val (a
// MISSING val removes the field) and re-emits key-sorted into the reused buffer --
// no boxing. See base.ObjectPut.
func ExprObjectPut(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	var lzBufPre []byte // <== varLift: lzBufPre by path

	triExprFunc := func(lzA, lzB, lzC base.ExprFunc, lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) { // !lz
		if LzScope {
			lzVal = lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"
			lzValObj := lzVal

			lzVal = lzB(lzVals, lzYieldErr) // <== emitCaptured: path "B"
			lzValKey := lzVal

			lzVal = lzC(lzVals, lzYieldErr) // <== emitCaptured: path "C"
			lzValElem := lzVal

			lzOut, lzSentinel, lzOk := base.ObjectPut(lzVars.Ctx.ValComparer, lzValObj, lzValKey, lzValElem, lzBufPre)
			if !lzOk {
				lzVal = lzSentinel
			} else {
				lzBufPre = lzOut
				lzVal = base.Val(lzOut)
			}
		}

		return lzVal
	} // !lz

	lzExprFunc = MakeTriExprFunc(lzVars, labels, params, path, triExprFunc) // !lzRHS

	return lzExprFunc
}

// ExprObjectRemove is OBJECT_REMOVE(obj, key) (2-arg): binary, drops field key and
// re-emits key-sorted into the reused buffer -- no boxing. See base.ObjectRemove.
func ExprObjectRemove(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	var lzBufPre []byte // <== varLift: lzBufPre by path

	biExprFunc := func(lzA, lzB base.ExprFunc, lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) { // !lz
		if LzScope {
			lzVal = lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"
			lzValObj := lzVal

			lzVal = lzB(lzVals, lzYieldErr) // <== emitCaptured: path "B"
			lzValKey := lzVal

			lzOut, lzSentinel, lzOk := base.ObjectRemove(lzVars.Ctx.ValComparer, lzValObj, lzValKey, lzBufPre)
			if !lzOk {
				lzVal = lzSentinel
			} else {
				lzBufPre = lzOut
				lzVal = base.Val(lzOut)
			}
		}

		return lzVal
	} // !lz

	lzExprFunc = MakeBiExprFunc(lzVars, labels, params, path, biExprFunc) // !lzRHS

	return lzExprFunc
}

// ExprObjectConcat is OBJECT_CONCAT(obj1, obj2) (2-arg): binary, merges the two
// objects (obj2 wins) and re-emits key-sorted into the reused buffer -- no boxing.
// See base.ObjectConcat.
func ExprObjectConcat(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	var lzBufPre []byte // <== varLift: lzBufPre by path

	biExprFunc := func(lzA, lzB base.ExprFunc, lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) { // !lz
		if LzScope {
			lzVal = lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"
			lzValObj1 := lzVal

			lzVal = lzB(lzVals, lzYieldErr) // <== emitCaptured: path "B"
			lzValObj2 := lzVal

			lzOut, lzSentinel, lzOk := base.ObjectConcat(lzVars.Ctx.ValComparer, lzValObj1, lzValObj2, lzBufPre)
			if !lzOk {
				lzVal = lzSentinel
			} else {
				lzBufPre = lzOut
				lzVal = base.Val(lzOut)
			}
		}

		return lzVal
	} // !lz

	lzExprFunc = MakeBiExprFunc(lzVars, labels, params, path, biExprFunc) // !lzRHS

	return lzExprFunc
}

func ExprObjectLength(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return ExprLenReader(lzVars, labels, params, path, base.LenObject)
}

func ExprPolyLength(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return ExprLenReader(lzVars, labels, params, path, base.LenPoly)
}

func ExprLenReader(lzVars *base.Vars, labels base.Labels, params []interface{},
	path string, op int) (lzExprFunc base.ExprFunc) {
	exprA := params[0].([]interface{})

	var lzBufPre []byte // <== varLift: lzBufPre by path

	lzA := MakeExprFunc(lzVars, labels, exprA, path, "A") // !lzRHS, via: lzExprFunc

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

// ExprObjectNames is OBJECT_NAMES(obj): unary, builds the sorted JSON string-array
// of the operand object's field names into the reused buffer (or a MISSING/NULL
// sentinel for a MISSING / non-object operand) -- no boxing. Mirrors the SPLIT
// structure-builders (expr_str.go): the whole build lives in one base call so the
// reused buffer varLift placeholder stays on its own emitted line.
func ExprObjectNames(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	exprA := params[0].([]interface{})

	var lzBufPre []byte // <== varLift: lzBufPre by path

	lzA := MakeExprFunc(lzVars, labels, exprA, path, "A") // !lzRHS, via: lzExprFunc

	lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
		lzVal = lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"

		lzOut, lzSentinel, lzOk := base.ObjectNames(lzVars.Ctx.ValComparer, lzVal, lzBufPre)
		if !lzOk {
			lzVal = lzSentinel
		} else {
			lzBufPre = lzOut
			lzVal = base.Val(lzOut)
		}

		return lzVal
	}

	return lzExprFunc
}

// ExprObjectValues is OBJECT_VALUES(obj): unary, builds the JSON array of the
// operand object's values ordered by field name into the reused buffer (or a
// MISSING/NULL sentinel for a MISSING / non-object operand) -- no boxing. Sibling
// of ExprObjectNames; see base.ObjectValues.
func ExprObjectValues(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	exprA := params[0].([]interface{})

	var lzBufPre []byte // <== varLift: lzBufPre by path

	lzA := MakeExprFunc(lzVars, labels, exprA, path, "A") // !lzRHS, via: lzExprFunc

	lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
		lzVal = lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"

		lzOut, lzSentinel, lzOk := base.ObjectValues(lzVars.Ctx.ValComparer, lzVal, lzBufPre)
		if !lzOk {
			lzVal = lzSentinel
		} else {
			lzBufPre = lzOut
			lzVal = base.Val(lzOut)
		}

		return lzVal
	}

	return lzExprFunc
}

// ExprObjectPairs is OBJECT_PAIRS(obj): unary, builds the JSON array of
// {"name":k,"val":v} objects ordered by field name into the reused buffer (or a
// MISSING/NULL sentinel for a MISSING / non-object operand) -- no boxing. Sibling
// of ExprObjectNames; see base.ObjectPairs. (The 2-arg `types` option form stays
// boxed -- the optimizer only lowers the 1-arg form.)
func ExprObjectPairs(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	exprA := params[0].([]interface{})

	var lzBufPre []byte // <== varLift: lzBufPre by path

	lzA := MakeExprFunc(lzVars, labels, exprA, path, "A") // !lzRHS, via: lzExprFunc

	lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
		lzVal = lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"

		lzOut, lzSentinel, lzOk := base.ObjectPairs(lzVars.Ctx.ValComparer, lzVal, lzBufPre)
		if !lzOk {
			lzVal = lzSentinel
		} else {
			lzBufPre = lzOut
			lzVal = base.Val(lzOut)
		}

		return lzVal
	}

	return lzExprFunc
}
