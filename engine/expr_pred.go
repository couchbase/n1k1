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

// Each IS predicate is a 3-element result table indexed by base.ValKind
// ({value, null, missing}); the shared ExprIsPredicate harness just looks up.

// IS NULL:        NULL -> true,  MISSING -> MISSING, else false.
func ExprIsNull(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	return ExprIsPredicate(lzVars, labels, params, path,
		[]base.Val{base.ValFalse, base.ValTrue, base.ValMissing})
}

// IS NOT NULL:    NULL -> false, MISSING -> MISSING, else true.
func ExprIsNotNull(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	return ExprIsPredicate(lzVars, labels, params, path,
		[]base.Val{base.ValTrue, base.ValFalse, base.ValMissing})
}

// IS MISSING:     MISSING -> true, else false.
func ExprIsMissing(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	return ExprIsPredicate(lzVars, labels, params, path,
		[]base.Val{base.ValFalse, base.ValFalse, base.ValTrue})
}

// IS NOT MISSING: MISSING -> false, else true.
func ExprIsNotMissing(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	return ExprIsPredicate(lzVars, labels, params, path,
		[]base.Val{base.ValTrue, base.ValTrue, base.ValFalse})
}

// IS VALUED:      NULL/MISSING -> false, else true.
func ExprIsValued(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	return ExprIsPredicate(lzVars, labels, params, path,
		[]base.Val{base.ValTrue, base.ValFalse, base.ValFalse})
}

// IS NOT VALUED:  NULL/MISSING -> true, else false.
func ExprIsNotValued(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	return ExprIsPredicate(lzVars, labels, params, path,
		[]base.Val{base.ValFalse, base.ValTrue, base.ValTrue})
}

// -----------------------------------------------------

// ExprIsPredicate is the shared unary harness for the IS [NOT] NULL/MISSING/
// VALUED tests. byKind is a 3-element result table indexed by base.ValKind
// (value / null / missing); the harness classifies the operand and returns the
// matching constant Val.
func ExprIsPredicate(lzVars *base.Vars, labels base.Labels, params []interface{},
	path string, byKind []base.Val) (lzExprFunc base.ExprFunc) {
	exprA := params[0].([]interface{})

	var lzByKind []base.Val = byKind // <== varLift: lzByKind by path

	lzExprFunc =
		MakeExprFunc(lzVars, labels, exprA, path, "A") // !lz
	lzA := lzExprFunc

	lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
		lzVal = lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"

		lzVal = lzByKind[base.ValKind(lzVal)]

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

		if base.ValKind(lzVal) != base.ValKindValue {
			// MISSING and NULL pass through unchanged.
		} else if base.ValTruthy(lzVal) {
			lzVal = base.ValFalse
		} else {
			lzVal = base.ValTrue
		}

		return lzVal
	}

	return lzExprFunc
}
