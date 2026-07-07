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

// isPredicateFuncs maps each IS predicate to its 3-element result table, indexed
// by base.ValKind {value, null, missing} -- the shared ExprIsPredicate harness
// classifies the operand and returns the matching constant Val.
var isPredicateFuncs = map[string][]base.Val{
	"is_null":        {base.ValFalse, base.ValTrue, base.ValMissing}, // NULL->true, MISSING->MISSING, else false
	"is_not_null":    {base.ValTrue, base.ValFalse, base.ValMissing}, // NULL->false, MISSING->MISSING, else true
	"is_missing":     {base.ValFalse, base.ValFalse, base.ValTrue},   // MISSING->true, else false
	"is_not_missing": {base.ValTrue, base.ValTrue, base.ValFalse},    // MISSING->false, else true
	"is_valued":      {base.ValTrue, base.ValFalse, base.ValFalse},   // NULL/MISSING->false, else true
	"is_not_valued":  {base.ValFalse, base.ValTrue, base.ValTrue},    // NULL/MISSING->true, else false
}

func init() {
	for name, byKind := range isPredicateFuncs {
		ExprCatalog[name] = exprIsPredicateOp(byKind)
	}
	ExprCatalog["not"] = ExprNot
}

// exprIsPredicateOp closes over an IS predicate's result table and defers to the
// shared harness. Plain (non-lz) Go, so intermed_build passes it through and the
// table still reaches the harness's emission site -- both paths stay identical to
// the old per-op funcs (see DESIGN-exprs.md "Codegen ergonomics").
func exprIsPredicateOp(byKind []base.Val) base.ExprCatalogFunc {
	return func(lzVars *base.Vars, labels base.Labels, params []interface{}, path string) base.ExprFunc {
		return ExprIsPredicate(lzVars, labels, params, path, byKind)
	}
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
