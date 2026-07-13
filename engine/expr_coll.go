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

func init() {
	ExprCatalog["any"] = ExprAny
}

// ExprAny implements the ANY collection predicate (single-binding, IN form):
//
//	ANY <var> IN <arr> SATISFIES <pred> END
//
// It binds <var> to each element of <arr> in an APPENDED register slot -- labeled
// like a LET variable, `.["<var>"]` -- and evaluates <pred> in that extended scope;
// it returns TRUE on the first element whose predicate is truthy (skipping further
// predicate evaluation), else FALSE. cbq skeleton (collEval + Any.Evaluate): <arr>
// MISSING -> MISSING; a non-array (incl. NULL and the plain-IN-over-object case) ->
// NULL; an empty array -> FALSE.
//
// params: [ bindingLabel(string), arrExpr, satisfiesExpr ]. The optimizer lowers
// satisfiesExpr against the parent labels + bindingLabel, so a reference to <var>
// (as `x` or `x.field`) resolves to the appended slot via the normal labelPath
// matcher -- exactly how LET-bound variables resolve (see glue/expr_optimize.go and
// VisitLet). Correlated predicates work for free: the child sees the parent
// registers plus the element slot.
//
// The runtime shape mirrors OpJoinNestedLoop's UNNEST path (base.ArrayYield + a
// shadowed lzVals right before the captured child), which is the established
// codegen-safe way to iterate array elements and invoke a captured child.
func ExprAny(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	bindingLabel := params[0].(string)
	arrParam := params[1].([]interface{})
	satParam := params[2].([]interface{})

	// The child predicate sees the parent registers plus one appended slot for the
	// bound variable. The optimizer lowered satParam against these SAME labels, so
	// the slot index it baked matches len(labels).
	childLabels := append(append(base.Labels{}, labels...), bindingLabel)

	var lzValsChild base.Vals // <== varLift: lzValsChild by path
	var lzValsPre base.Vals   // <== varLift: lzValsPre by path

	lzExprFunc =
		MakeExprFunc(lzVars, labels, arrParam, path, "arr") // !lz
	lzArr := lzExprFunc

	lzExprFunc =
		MakeExprFunc(lzVars, childLabels, satParam, path, "sat") // !lz
	lzSat := lzExprFunc

	lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
		lzValArr := lzArr(lzVals, lzYieldErr) // <== emitCaptured: path "arr"

		if base.ValKind(lzValArr) == base.ValKindMissing {
			lzVal = base.ValMissing
		} else {
			lzFound := false

			lzYieldElem := func(lzElemVals base.Vals) {
				if !lzFound {
					// Extend the row with the bound element in the appended slot.
					lzValsChild = append(lzValsChild[:0], lzVals...)
					lzValsChild = append(lzValsChild, lzElemVals[0])

					lzVals := lzValsChild // shadow: the captured child reads the appended slot

					lzValSat := lzSat(lzVals, lzYieldErr) // <== emitCaptured: path "sat"

					if base.ValTruthy(lzValSat) {
						lzFound = true
					}
				}
			}

			var lzIsArr bool
			lzValsPre, lzIsArr = base.ArrayYield(lzValArr, lzYieldElem, lzValsPre[:0])

			if !lzIsArr {
				lzVal = base.ValNull
			} else if lzFound {
				lzVal = base.ValTrue
			} else {
				lzVal = base.ValFalse
			}
		}

		return lzVal
	}

	return lzExprFunc
}
