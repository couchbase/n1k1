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

func init() {
	ExprCatalog["is_array"] = ExprIsArray
	ExprCatalog["is_number"] = ExprIsNumber
	ExprCatalog["is_string"] = ExprIsString
	ExprCatalog["is_boolean"] = ExprIsBoolean
	ExprCatalog["is_object"] = ExprIsObject
	ExprCatalog["is_atom"] = ExprIsAtom
}

// Each predicate passes its type test directly into the shared harness.
func typeIsArray(t int) bool   { return t == base.ValTypeArray }
func typeIsNumber(t int) bool  { return t == base.ValTypeNumber }
func typeIsString(t int) bool  { return t == base.ValTypeString }
func typeIsBoolean(t int) bool { return t == base.ValTypeBoolean }
func typeIsObject(t int) bool  { return t == base.ValTypeObject }

// IS_ATOM: a scalar -- boolean, number, or string (not array/object).
func typeIsAtom(t int) bool {
	return t == base.ValTypeBoolean || t == base.ValTypeNumber || t == base.ValTypeString
}

func ExprIsArray(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	return ExprIsType(lzVars, labels, params, path, typeIsArray)
}

func ExprIsNumber(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	return ExprIsType(lzVars, labels, params, path, typeIsNumber)
}

func ExprIsString(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	return ExprIsType(lzVars, labels, params, path, typeIsString)
}

func ExprIsBoolean(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	return ExprIsType(lzVars, labels, params, path, typeIsBoolean)
}

func ExprIsObject(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	return ExprIsType(lzVars, labels, params, path, typeIsObject)
}

func ExprIsAtom(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	return ExprIsType(lzVars, labels, params, path, typeIsAtom)
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
