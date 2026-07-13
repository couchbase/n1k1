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
	ExprCatalog["every"] = ExprEvery
	ExprCatalog["first"] = ExprFirst
	ExprCatalog["array"] = ExprArray
	ExprCatalog["object"] = ExprObject
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

// ExprEvery implements the EVERY collection predicate (single-binding, IN form):
//
//	EVERY <var> IN <arr> SATISFIES <pred> END
//
// Same binding/skeleton as ExprAny, but returns FALSE on the first element whose
// predicate is NOT truthy (skipping the rest), else TRUE. cbq: <arr> MISSING ->
// MISSING; non-array -> NULL; an EMPTY array -> TRUE (vacuous). params match "any":
// [ bindingLabel, arrExpr, satisfiesExpr ].
func ExprEvery(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	bindingLabel := params[0].(string)
	arrParam := params[1].([]interface{})
	satParam := params[2].([]interface{})

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
			lzFailed := false

			lzYieldElem := func(lzElemVals base.Vals) {
				if !lzFailed {
					lzValsChild = append(lzValsChild[:0], lzVals...)
					lzValsChild = append(lzValsChild, lzElemVals[0])

					lzVals := lzValsChild // shadow: the captured child reads the appended slot

					lzValSat := lzSat(lzVals, lzYieldErr) // <== emitCaptured: path "sat"

					if !base.ValTruthy(lzValSat) {
						lzFailed = true
					}
				}
			}

			var lzIsArr bool
			lzValsPre, lzIsArr = base.ArrayYield(lzValArr, lzYieldElem, lzValsPre[:0])

			if !lzIsArr {
				lzVal = base.ValNull
			} else if lzFailed {
				lzVal = base.ValFalse
			} else {
				lzVal = base.ValTrue
			}
		}

		return lzVal
	}

	return lzExprFunc
}

// ExprFirst implements the FIRST array comprehension (single-binding, IN form):
//
//	FIRST <map> FOR <var> IN <arr> [WHEN <cond>] END
//
// It returns <map> evaluated on the FIRST element for which <cond> is truthy
// (skipping the rest). cbq: <arr> MISSING -> MISSING; non-array -> NULL; no
// matching element -> MISSING. params: [ bindingLabel, arrExpr, mapExpr, whenExpr ]
// -- the optimizer always supplies whenExpr (a constant `true` when the source has
// no WHEN), so there is no per-element branch.
func ExprFirst(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	bindingLabel := params[0].(string)
	arrParam := params[1].([]interface{})
	mapParam := params[2].([]interface{})
	whenParam := params[3].([]interface{})

	childLabels := append(append(base.Labels{}, labels...), bindingLabel)

	var lzValsChild base.Vals // <== varLift: lzValsChild by path
	var lzValsPre base.Vals   // <== varLift: lzValsPre by path

	lzExprFunc =
		MakeExprFunc(lzVars, labels, arrParam, path, "arr") // !lz
	lzArr := lzExprFunc

	lzExprFunc =
		MakeExprFunc(lzVars, childLabels, whenParam, path, "when") // !lz
	lzWhen := lzExprFunc

	lzExprFunc =
		MakeExprFunc(lzVars, childLabels, mapParam, path, "map") // !lz
	lzMap := lzExprFunc

	lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
		lzValArr := lzArr(lzVals, lzYieldErr) // <== emitCaptured: path "arr"

		if base.ValKind(lzValArr) == base.ValKindMissing {
			lzVal = base.ValMissing
		} else {
			lzDone := false
			var lzResult base.Val = base.ValMissing

			lzYieldElem := func(lzElemVals base.Vals) {
				if !lzDone {
					lzValsChild = append(lzValsChild[:0], lzVals...)
					lzValsChild = append(lzValsChild, lzElemVals[0])

					lzVals := lzValsChild // shadow: the captured children read the appended slot

					lzValWhen := lzWhen(lzVals, lzYieldErr) // <== emitCaptured: path "when"

					if base.ValTruthy(lzValWhen) {
						lzResult = lzMap(lzVals, lzYieldErr) // <== emitCaptured: path "map"
						lzDone = true
					}
				}
			}

			var lzIsArr bool
			lzValsPre, lzIsArr = base.ArrayYield(lzValArr, lzYieldElem, lzValsPre[:0])

			if !lzIsArr {
				lzVal = base.ValNull
			} else {
				lzVal = lzResult
			}
		}

		return lzVal
	}

	return lzExprFunc
}

// ExprArray implements the ARRAY array comprehension (single-binding, IN form):
//
//	ARRAY <map> FOR <var> IN <arr> [WHEN <cond>] END
//
// It builds a JSON array of <map> evaluated on each element for which <cond> is
// truthy; a MISSING mapping value is skipped (cbq). <arr> MISSING -> MISSING;
// non-array -> NULL; empty (or all-filtered) -> []. params: [ bindingLabel,
// arrExpr, mapExpr, whenExpr ] (whenExpr always supplied, see ExprFirst).
func ExprArray(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	bindingLabel := params[0].(string)
	arrParam := params[1].([]interface{})
	mapParam := params[2].([]interface{})
	whenParam := params[3].([]interface{})

	childLabels := append(append(base.Labels{}, labels...), bindingLabel)

	var lzValsChild base.Vals // <== varLift: lzValsChild by path
	var lzValsPre base.Vals   // <== varLift: lzValsPre by path
	var lzBufPre []byte       // <== varLift: lzBufPre by path

	lzExprFunc =
		MakeExprFunc(lzVars, labels, arrParam, path, "arr") // !lz
	lzArr := lzExprFunc

	lzExprFunc =
		MakeExprFunc(lzVars, childLabels, whenParam, path, "when") // !lz
	lzWhen := lzExprFunc

	lzExprFunc =
		MakeExprFunc(lzVars, childLabels, mapParam, path, "map") // !lz
	lzMap := lzExprFunc

	lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
		lzValArr := lzArr(lzVals, lzYieldErr) // <== emitCaptured: path "arr"

		if base.ValKind(lzValArr) == base.ValKindMissing {
			lzVal = base.ValMissing
		} else {
			lzOut := append(lzBufPre[:0], '[')
			lzWrote := false

			lzYieldElem := func(lzElemVals base.Vals) {
				lzValsChild = append(lzValsChild[:0], lzVals...)
				lzValsChild = append(lzValsChild, lzElemVals[0])

				lzVals := lzValsChild // shadow: the captured children read the appended slot

				lzValWhen := lzWhen(lzVals, lzYieldErr) // <== emitCaptured: path "when"

				if base.ValTruthy(lzValWhen) {
					lzValMap := lzMap(lzVals, lzYieldErr) // <== emitCaptured: path "map"

					if base.ValKind(lzValMap) != base.ValKindMissing {
						if lzWrote {
							lzOut = append(lzOut, ',')
						}
						lzOut = append(lzOut, lzValMap...)
						lzWrote = true
					}
				}
			}

			var lzIsArr bool
			lzValsPre, lzIsArr = base.ArrayYield(lzValArr, lzYieldElem, lzValsPre[:0])

			if !lzIsArr {
				lzVal = base.ValNull
			} else {
				lzOut = append(lzOut, ']')
				lzBufPre = lzOut
				lzVal = base.Val(lzOut)
			}
		}

		return lzVal
	}

	return lzExprFunc
}

// ExprObject implements the OBJECT comprehension (single-binding, IN form):
//
//	OBJECT <name> : <value> FOR <var> IN <arr> [WHEN <cond>] END
//
// It builds a JSON object from each WHEN-matching element's <name>:<value>. cbq:
// <arr> MISSING -> MISSING; non-array -> NULL; a <name> that evaluates to MISSING
// aborts the whole result to MISSING; a non-string <name> aborts to NULL; a MISSING
// <value> is skipped; duplicate names are last-wins; the object is key-sorted.
// params: [ bindingLabel, arrExpr, nameExpr, valueExpr, whenExpr ] (whenExpr always
// supplied, a constant `true` when the source has no WHEN).
func ExprObject(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	bindingLabel := params[0].(string)
	arrParam := params[1].([]interface{})
	nameParam := params[2].([]interface{})
	valueParam := params[3].([]interface{})
	whenParam := params[4].([]interface{})

	childLabels := append(append(base.Labels{}, labels...), bindingLabel)

	var lzValsChild base.Vals // <== varLift: lzValsChild by path
	var lzValsPre base.Vals   // <== varLift: lzValsPre by path
	var lzBufPre []byte       // <== varLift: lzBufPre by path
	var lzKVs base.KeyVals    // <== varLift: lzKVs by path

	lzExprFunc =
		MakeExprFunc(lzVars, labels, arrParam, path, "arr") // !lz
	lzArr := lzExprFunc

	lzExprFunc =
		MakeExprFunc(lzVars, childLabels, whenParam, path, "when") // !lz
	lzWhen := lzExprFunc

	lzExprFunc =
		MakeExprFunc(lzVars, childLabels, nameParam, path, "name") // !lz
	lzName := lzExprFunc

	lzExprFunc =
		MakeExprFunc(lzVars, childLabels, valueParam, path, "value") // !lz
	lzValue := lzExprFunc

	lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
		lzValArr := lzArr(lzVals, lzYieldErr) // <== emitCaptured: path "arr"

		if base.ValKind(lzValArr) == base.ValKindMissing {
			lzVal = base.ValMissing
		} else {
			lzKVs = lzKVs[:0]
			lzBad := 0 // 0 ok, 1 name MISSING (-> MISSING), 2 name non-string (-> NULL)

			lzYieldElem := func(lzElemVals base.Vals) {
				if lzBad == 0 {
					lzValsChild = append(lzValsChild[:0], lzVals...)
					lzValsChild = append(lzValsChild, lzElemVals[0])

					lzVals := lzValsChild // shadow: the captured children read the appended slot

					lzValWhen := lzWhen(lzVals, lzYieldErr) // <== emitCaptured: path "when"

					if base.ValTruthy(lzValWhen) {
						lzValName := lzName(lzVals, lzYieldErr) // <== emitCaptured: path "name"

						if base.ValKind(lzValName) == base.ValKindMissing {
							lzBad = 1
						} else if !base.ValIsString(lzValName) {
							lzBad = 2
						} else {
							lzValValue := lzValue(lzVals, lzYieldErr) // <== emitCaptured: path "value"

							if base.ValKind(lzValValue) != base.ValKindMissing {
								lzKVs = base.CollObjectPut(lzKVs, lzValName, lzValValue)
							}
						}
					}
				}
			}

			var lzIsArr bool
			lzValsPre, lzIsArr = base.ArrayYield(lzValArr, lzYieldElem, lzValsPre[:0])

			if !lzIsArr {
				lzVal = base.ValNull
			} else if lzBad == 1 {
				lzVal = base.ValMissing
			} else if lzBad == 2 {
				lzVal = base.ValNull
			} else {
				lzOut := base.CollObjectEmit(lzKVs, lzBufPre)
				lzBufPre = lzOut
				lzVal = base.Val(lzOut)
			}
		}

		return lzVal
	}

	return lzExprFunc
}
