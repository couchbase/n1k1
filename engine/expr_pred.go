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

// Native unary predicates: logical NOT and the IS [NOT] NULL / MISSING / VALUED
// tests. All operate on the operand's byte-kind (MISSING = empty, NULL = "null",
// else a value) and return a constant Val -- no buffers, no boxing. Semantics
// mirror cbq's logic_not.go / comp_null.go / comp_missing.go / comp_valued.go
// exactly.

func init() {
	// IS predicates, keyed by result for (missing, null, value) operand kinds.
	ExprCatalog["is_null"] = ExprIsNull
	ExprCatalog["is_not_null"] = ExprIsNotNull
	ExprCatalog["is_missing"] = ExprIsMissing
	ExprCatalog["is_not_missing"] = ExprIsNotMissing
	ExprCatalog["is_valued"] = ExprIsValued
	ExprCatalog["is_not_valued"] = ExprIsNotValued

	ExprCatalog["not"] = ExprNot
}

// -----------------------------------------------------

// IS NULL:        NULL -> true,  MISSING -> MISSING, else false.
func ExprIsNull(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	return ExprIsPredicate(lzVars, labels, params, path,
		base.ValMissing, base.ValTrue, base.ValFalse)
}

// IS NOT NULL:    NULL -> false, MISSING -> MISSING, else true.
func ExprIsNotNull(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	return ExprIsPredicate(lzVars, labels, params, path,
		base.ValMissing, base.ValFalse, base.ValTrue)
}

// IS MISSING:     MISSING -> true, else false.
func ExprIsMissing(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	return ExprIsPredicate(lzVars, labels, params, path,
		base.ValTrue, base.ValFalse, base.ValFalse)
}

// IS NOT MISSING: MISSING -> false, else true.
func ExprIsNotMissing(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	return ExprIsPredicate(lzVars, labels, params, path,
		base.ValFalse, base.ValTrue, base.ValTrue)
}

// IS VALUED:      NULL/MISSING -> false, else true.
func ExprIsValued(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	return ExprIsPredicate(lzVars, labels, params, path,
		base.ValFalse, base.ValFalse, base.ValTrue)
}

// IS NOT VALUED:  NULL/MISSING -> true, else false.
func ExprIsNotValued(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	return ExprIsPredicate(lzVars, labels, params, path,
		base.ValTrue, base.ValTrue, base.ValFalse)
}

// -----------------------------------------------------

// ExprIsPredicate is the shared unary harness for the IS [NOT] NULL/MISSING/
// VALUED tests. It classifies the operand into one of three byte-kinds and
// returns the matching constant Val: whenMissing (operand is empty / MISSING),
// whenNull (operand is "null"), or whenValue (any other value).
func ExprIsPredicate(lzVars *base.Vars, labels base.Labels, params []interface{},
	path string, whenMissing, whenNull, whenValue base.Val) (lzExprFunc base.ExprFunc) {
	exprA := params[0].([]interface{})

	var lzWhenMissing base.Val = whenMissing // <== varLift: lzWhenMissing by path
	var lzWhenNull base.Val = whenNull       // <== varLift: lzWhenNull by path
	var lzWhenValue base.Val = whenValue     // <== varLift: lzWhenValue by path

	lzExprFunc =
		MakeExprFunc(lzVars, labels, exprA, path, "A") // !lz
	lzA := lzExprFunc

	lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
		_, _, _ = lzWhenMissing, lzWhenNull, lzWhenValue

		lzVal = lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"

		if len(lzVal) == 0 {
			lzVal = lzWhenMissing
		} else if base.ValEqualNull(lzVal) {
			lzVal = lzWhenNull
		} else {
			lzVal = lzWhenValue
		}

		return lzVal
	}

	return lzExprFunc
}

// -----------------------------------------------------

// ExprNot is logical NOT: MISSING -> MISSING, NULL -> NULL, else negate the
// operand's N1QL truthiness (matching expression/logic_not.go).
func ExprNot(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	exprA := params[0].([]interface{})

	lzExprFunc =
		MakeExprFunc(lzVars, labels, exprA, path, "A") // !lz
	lzA := lzExprFunc

	lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
		lzVal = lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"

		if len(lzVal) == 0 {
			lzVal = base.ValMissing
		} else if base.ValEqualNull(lzVal) {
			lzVal = base.ValNull
		} else if base.ValTruthy(lzVal) {
			lzVal = base.ValFalse
		} else {
			lzVal = base.ValTrue
		}

		return lzVal
	}

	return lzExprFunc
}
