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
	ExprCatalog["descendants"] = ExprDescendants
}

// collBind derives the per-element child scope labels (parent labels + one slot
// per bound variable) and whether this is the named (k:v) form, from a
// comprehension op's params[0] -- a list of binding-label strings: one label for a
// value-only `x IN` binding, or two ([nameLabel, valueLabel]) for the named `k:v IN`
// form. named is a plain (non-lz) build-time value, so it bakes into the compiled
// path as a constant handed to base.CollElems.
func collBind(labels base.Labels, param0 interface{}) (childLabels base.Labels, named bool) {
	bindLabels := param0.([]interface{})
	childLabels = append(base.Labels{}, labels...)
	for _, b := range bindLabels {
		childLabels = append(childLabels, b.(string))
	}
	return childLabels, len(bindLabels) == 2
}

// ExprDescendants is the WITHIN operand transform: it flattens an array/object to
// its descendants (base.Descendants / cbq value.Descendants order) as a JSON array,
// so a comprehension binding `x WITHIN v` reuses the ordinary element-iterating
// comprehension ops over that array. Unary; rides the shared array harness.
func ExprDescendants(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return ExprArrayUnaryBuf(lzVars, labels, params, path, base.Descendants)
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
// params: [ bindLabels, arrExpr, satisfiesExpr ]. bindLabels is a label list -- one
// [valueLabel] for `x IN`, two [nameLabel, valueLabel] for the named `k:v IN` form.
// The optimizer lowers satisfiesExpr against the parent labels + bindLabels, so <var>
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
	arrParam := params[1].([]interface{})
	satParam := params[2].([]interface{})

	// The child predicate sees the parent registers plus one appended slot for the
	// bound variable. The optimizer lowered satParam against these SAME labels, so
	// the slot index it baked matches len(labels).
	childLabels, named := collBind(labels, params[0])

	var lzValsChild base.Vals // <== varLift: lzValsChild by path
	var lzElemsPre base.Vals  // <== varLift: lzElemsPre by path

	lzArr := MakeExprFunc(lzVars, labels, arrParam, path, "arr") // !lzRHS, via: lzExprFunc

	lzSat := MakeExprFunc(lzVars, childLabels, satParam, path, "sat") // !lzRHS, via: lzExprFunc

	lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
		lzValArr := lzArr(lzVals, lzYieldErr) // <== emitCaptured: path "arr", via: lzVal

		if base.ValKind(lzValArr) == base.ValKindMissing {
			lzVal = base.ValMissing
		} else {
			// Materialize the binding's members (stride 1 value-only, 2 for k:v) and
			// iterate with a plain for-loop -- no per-element callback closure, which
			// the expr codegen can't emit; the captured child inlines in the loop body.
			var lzStride int
			var lzIsColl bool
			lzElemsPre, lzStride, lzIsColl = base.CollElems(lzVars.Ctx.ValComparer, lzValArr, named, lzElemsPre[:0])

			if !lzIsColl {
				lzVal = base.ValNull
			} else {
				lzFound := false

				for lzI := 0; lzI < len(lzElemsPre); lzI = lzI + lzStride {
					if !lzFound {
						lzValsChild = append(lzValsChild[:0], lzVals...)
						lzValsChild = append(lzValsChild, lzElemsPre[lzI:lzI+lzStride]...)

						lzVals := lzValsChild // shadow: the captured child reads the appended slot(s)

						lzValSat := lzSat(lzVals, lzYieldErr) // <== emitCaptured: path "sat", via: lzVal

						if base.ValTruthy(lzValSat) {
							lzFound = true
						}
					}
				}

				if lzFound {
					lzVal = base.ValTrue
				} else {
					lzVal = base.ValFalse
				}
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
// [ bindLabels, arrExpr, satisfiesExpr ].
func ExprEvery(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	arrParam := params[1].([]interface{})
	satParam := params[2].([]interface{})

	childLabels, named := collBind(labels, params[0])

	var lzValsChild base.Vals // <== varLift: lzValsChild by path
	var lzElemsPre base.Vals  // <== varLift: lzElemsPre by path

	lzArr := MakeExprFunc(lzVars, labels, arrParam, path, "arr") // !lzRHS, via: lzExprFunc

	lzSat := MakeExprFunc(lzVars, childLabels, satParam, path, "sat") // !lzRHS, via: lzExprFunc

	lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
		lzValArr := lzArr(lzVals, lzYieldErr) // <== emitCaptured: path "arr", via: lzVal

		if base.ValKind(lzValArr) == base.ValKindMissing {
			lzVal = base.ValMissing
		} else {
			var lzStride int
			var lzIsColl bool
			lzElemsPre, lzStride, lzIsColl = base.CollElems(lzVars.Ctx.ValComparer, lzValArr, named, lzElemsPre[:0])

			if !lzIsColl {
				lzVal = base.ValNull
			} else {
				lzFailed := false

				for lzI := 0; lzI < len(lzElemsPre); lzI = lzI + lzStride {
					if !lzFailed {
						lzValsChild = append(lzValsChild[:0], lzVals...)
						lzValsChild = append(lzValsChild, lzElemsPre[lzI:lzI+lzStride]...)

						lzVals := lzValsChild // shadow: the captured child reads the appended slot(s)

						lzValSat := lzSat(lzVals, lzYieldErr) // <== emitCaptured: path "sat", via: lzVal

						if !base.ValTruthy(lzValSat) {
							lzFailed = true
						}
					}
				}

				if lzFailed {
					lzVal = base.ValFalse
				} else {
					lzVal = base.ValTrue
				}
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
// matching element -> MISSING. params: [ bindLabels, arrExpr, mapExpr, whenExpr ]
// -- the optimizer always supplies whenExpr (a constant `true` when the source has
// no WHEN), so there is no per-element branch.
func ExprFirst(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	arrParam := params[1].([]interface{})
	mapParam := params[2].([]interface{})
	whenParam := params[3].([]interface{})

	childLabels, named := collBind(labels, params[0])

	var lzValsChild base.Vals // <== varLift: lzValsChild by path
	var lzElemsPre base.Vals  // <== varLift: lzElemsPre by path

	lzArr := MakeExprFunc(lzVars, labels, arrParam, path, "arr") // !lzRHS, via: lzExprFunc

	lzWhen := MakeExprFunc(lzVars, childLabels, whenParam, path, "when") // !lzRHS, via: lzExprFunc

	lzMap := MakeExprFunc(lzVars, childLabels, mapParam, path, "map") // !lzRHS, via: lzExprFunc

	lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
		lzValArr := lzArr(lzVals, lzYieldErr) // <== emitCaptured: path "arr", via: lzVal

		if base.ValKind(lzValArr) == base.ValKindMissing {
			lzVal = base.ValMissing
		} else {
			var lzStride int
			var lzIsColl bool
			lzElemsPre, lzStride, lzIsColl = base.CollElems(lzVars.Ctx.ValComparer, lzValArr, named, lzElemsPre[:0])

			if !lzIsColl {
				lzVal = base.ValNull
			} else {
				lzDone := false
				var lzResult base.Val = base.ValMissing

				for lzI := 0; lzI < len(lzElemsPre); lzI = lzI + lzStride {
					if !lzDone {
						lzValsChild = append(lzValsChild[:0], lzVals...)
						lzValsChild = append(lzValsChild, lzElemsPre[lzI:lzI+lzStride]...)

						lzVals := lzValsChild // shadow: the captured children read the appended slot(s)

						lzValWhen := lzWhen(lzVals, lzYieldErr) // <== emitCaptured: path "when", via: lzVal

						if base.ValTruthy(lzValWhen) {
							lzResult = lzMap(lzVals, lzYieldErr) // <== emitCaptured: path "map", via: lzVal
							lzDone = true
						}
					}
				}

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
// non-array -> NULL; empty (or all-filtered) -> []. params: [ bindLabels,
// arrExpr, mapExpr, whenExpr ] (whenExpr always supplied, see ExprFirst).
func ExprArray(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	arrParam := params[1].([]interface{})
	mapParam := params[2].([]interface{})
	whenParam := params[3].([]interface{})

	childLabels, named := collBind(labels, params[0])

	var lzValsChild base.Vals // <== varLift: lzValsChild by path
	var lzElemsPre base.Vals  // <== varLift: lzElemsPre by path
	var lzBufPre []byte       // <== varLift: lzBufPre by path

	lzArr := MakeExprFunc(lzVars, labels, arrParam, path, "arr") // !lzRHS, via: lzExprFunc

	lzWhen := MakeExprFunc(lzVars, childLabels, whenParam, path, "when") // !lzRHS, via: lzExprFunc

	lzMap := MakeExprFunc(lzVars, childLabels, mapParam, path, "map") // !lzRHS, via: lzExprFunc

	lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
		lzValArr := lzArr(lzVals, lzYieldErr) // <== emitCaptured: path "arr", via: lzVal

		if base.ValKind(lzValArr) == base.ValKindMissing {
			lzVal = base.ValMissing
		} else {
			var lzStride int
			var lzIsColl bool
			lzElemsPre, lzStride, lzIsColl = base.CollElems(lzVars.Ctx.ValComparer, lzValArr, named, lzElemsPre[:0])

			if !lzIsColl {
				lzVal = base.ValNull
			} else {
				lzOut := append(lzBufPre[:0], '[')
				lzWrote := false

				for lzI := 0; lzI < len(lzElemsPre); lzI = lzI + lzStride {
					lzValsChild = append(lzValsChild[:0], lzVals...)
					lzValsChild = append(lzValsChild, lzElemsPre[lzI:lzI+lzStride]...)

					lzVals := lzValsChild // shadow: the captured children read the appended slot(s)

					lzValWhen := lzWhen(lzVals, lzYieldErr) // <== emitCaptured: path "when", via: lzVal

					if base.ValTruthy(lzValWhen) {
						lzValMap := lzMap(lzVals, lzYieldErr) // <== emitCaptured: path "map", via: lzVal

						if base.ValKind(lzValMap) != base.ValKindMissing {
							if lzWrote {
								lzOut = append(lzOut, ',')
							}
							lzOut = append(lzOut, lzValMap...)
							lzWrote = true
						}
					}
				}

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
// params: [ bindLabels, arrExpr, nameExpr, valueExpr, whenExpr ] (whenExpr always
// supplied, a constant `true` when the source has no WHEN).
func ExprObject(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	arrParam := params[1].([]interface{})
	nameParam := params[2].([]interface{})
	valueParam := params[3].([]interface{})
	whenParam := params[4].([]interface{})

	childLabels, named := collBind(labels, params[0])

	var lzValsChild base.Vals // <== varLift: lzValsChild by path
	var lzElemsPre base.Vals  // <== varLift: lzElemsPre by path
	var lzBufPre []byte       // <== varLift: lzBufPre by path
	var lzKVs base.KeyVals    // <== varLift: lzKVs by path

	lzArr := MakeExprFunc(lzVars, labels, arrParam, path, "arr") // !lzRHS, via: lzExprFunc

	lzWhen := MakeExprFunc(lzVars, childLabels, whenParam, path, "when") // !lzRHS, via: lzExprFunc

	lzName := MakeExprFunc(lzVars, childLabels, nameParam, path, "name") // !lzRHS, via: lzExprFunc

	lzValue := MakeExprFunc(lzVars, childLabels, valueParam, path, "value") // !lzRHS, via: lzExprFunc

	lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
		lzValArr := lzArr(lzVals, lzYieldErr) // <== emitCaptured: path "arr", via: lzVal

		if base.ValKind(lzValArr) == base.ValKindMissing {
			lzVal = base.ValMissing
		} else {
			var lzStride int
			var lzIsColl bool
			lzElemsPre, lzStride, lzIsColl = base.CollElems(lzVars.Ctx.ValComparer, lzValArr, named, lzElemsPre[:0])

			if !lzIsColl {
				lzVal = base.ValNull
			} else {
				lzKVs = lzKVs[:0]
				lzBad := 0 // 0 ok, 1 name MISSING (-> MISSING), 2 name non-string (-> NULL)

				for lzI := 0; lzI < len(lzElemsPre); lzI = lzI + lzStride {
					if lzBad == 0 {
						lzValsChild = append(lzValsChild[:0], lzVals...)
						lzValsChild = append(lzValsChild, lzElemsPre[lzI:lzI+lzStride]...)

						lzVals := lzValsChild // shadow: the captured children read the appended slot(s)

						lzValWhen := lzWhen(lzVals, lzYieldErr) // <== emitCaptured: path "when", via: lzVal

						if base.ValTruthy(lzValWhen) {
							lzValName := lzName(lzVals, lzYieldErr) // <== emitCaptured: path "name", via: lzVal

							if base.ValKind(lzValName) == base.ValKindMissing {
								lzBad = 1
							} else if !base.ValIsString(lzValName) {
								lzBad = 2
							} else {
								lzValValue := lzValue(lzVals, lzYieldErr) // <== emitCaptured: path "value", via: lzVal

								if base.ValKind(lzValValue) != base.ValKindMissing {
									lzKVs = base.CollObjectPut(lzKVs, lzValName, lzValValue)
								}
							}
						}
					}
				}

				if lzBad == 1 {
					lzVal = base.ValMissing
				} else if lzBad == 2 {
					lzVal = base.ValNull
				} else {
					lzOut := base.CollObjectEmit(lzKVs, lzBufPre)
					lzBufPre = lzOut
					lzVal = base.Val(lzOut)
				}
			}
		}

		return lzVal
	}

	return lzExprFunc
}
