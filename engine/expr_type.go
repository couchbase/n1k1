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

// Native type-check predicates: IS_ARRAY / IS_NUMBER / IS_STRING / IS_BOOLEAN /
// IS_OBJECT / IS_ATOM. All unary: MISSING and NULL pass through unchanged; any
// other value yields the boolean result of a type test. Mirrors cbq
// expression/func_type_check.go. (IS_BINARY and the TYPE() function are left to
// the cbq fallback.)
//
// Also the scalar type-CONVERSION functions TO_BOOLEAN / TO_STRING / TO_NUMBER
// (see the second half of this file); logic in base.ToBoolean/ToString/ToNumber.

func init() {
	ExprCatalog["is_array"] = ExprIsArray
	ExprCatalog["is_number"] = ExprIsNumber
	ExprCatalog["is_string"] = ExprIsString
	ExprCatalog["is_boolean"] = ExprIsBoolean
	ExprCatalog["is_object"] = ExprIsObject
	ExprCatalog["is_atom"] = ExprIsAtom

	ExprCatalog["to_boolean"] = ExprToBoolean
	ExprCatalog["to_string"] = ExprToString
	ExprCatalog["to_number"] = ExprToNumber
}

// Each predicate passes its type test as a real func value (base.TypeIs*) into
// the shared harness -- the codegen emits it by name via LzExprFmt, so no int
// op-code + switch is needed. This is the func-passing spike; extend the pattern
// as the codegen supports it (see DESIGN-exprs.md).
func ExprIsArray(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	return ExprIsType(lzVars, labels, params, path, base.TypeIsArray)
}

func ExprIsNumber(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	return ExprIsType(lzVars, labels, params, path, base.TypeIsNumber)
}

func ExprIsString(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	return ExprIsType(lzVars, labels, params, path, base.TypeIsString)
}

func ExprIsBoolean(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	return ExprIsType(lzVars, labels, params, path, base.TypeIsBoolean)
}

func ExprIsObject(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	return ExprIsType(lzVars, labels, params, path, base.TypeIsObject)
}

func ExprIsAtom(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	return ExprIsType(lzVars, labels, params, path, base.TypeIsAtom)
}

// -----------------------------------------------------

// ExprIsType is the shared unary type-check harness. match reports whether a
// value's base.ValType satisfies the predicate. MISSING/NULL pass through.
func ExprIsType(lzVars *base.Vars, labels base.Labels, params []interface{},
	path string, match func(valType int) bool) (lzExprFunc base.ExprFunc) {
	exprA := params[0].([]interface{})

	lzExprFunc =
		MakeExprFunc(lzVars, labels, exprA, path, "A") // !lz
	lzA := lzExprFunc

	lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
		lzVal = lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"

		if base.ValKind(lzVal) != base.ValKindValue {
			// MISSING and NULL pass through unchanged.
		} else {
			_, lzType := base.Parse(lzVal)
			if match(base.ParseTypeToValType[lzType]) {
				lzVal = base.ValTrue
			} else {
				lzVal = base.ValFalse
			}
		}

		return lzVal
	}

	return lzExprFunc
}

// -----------------------------------------------------
// Scalar type conversions (TO_BOOLEAN / TO_STRING / TO_NUMBER). TO_BOOLEAN
// yields a constant Val (no buffer); the others build into the reused lzBufPre.
// TO_NUMBER's 2-arg strip form is variadic -> falls back per the optimizer guard.

func ExprToBoolean(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	exprA := params[0].([]interface{})

	lzExprFunc =
		MakeExprFunc(lzVars, labels, exprA, path, "A") // !lz
	lzA := lzExprFunc

	lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
		lzVal = lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"

		lzVal = base.ToBoolean(lzVal)

		return lzVal
	}

	return lzExprFunc
}

func ExprToString(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	exprA := params[0].([]interface{})

	var lzBufPre []byte // <== varLift: lzBufPre by path

	lzExprFunc =
		MakeExprFunc(lzVars, labels, exprA, path, "A") // !lz
	lzA := lzExprFunc

	lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
		lzVal = lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"

		lzVal, lzBufPre = base.ToString(lzVal, lzBufPre)

		return lzVal
	}

	return lzExprFunc
}

func ExprToNumber(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	exprA := params[0].([]interface{})

	var lzBufPre []byte // <== varLift: lzBufPre by path

	lzExprFunc =
		MakeExprFunc(lzVars, labels, exprA, path, "A") // !lz
	lzA := lzExprFunc

	lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
		lzVal = lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"

		lzVal, lzBufPre = base.ToNumber(lzVal, lzBufPre)

		return lzVal
	}

	return lzExprFunc
}
