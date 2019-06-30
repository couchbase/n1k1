//  Copyright (c) 2019 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package n1k1

import (
	"strings"

	"github.com/couchbase/n1k1/base"
)

// OpJoinNestedLoop implements...
//  o.Kind:             via flags that control if-else codepaths:
//   joinNL-inner
//   joinNL-leftOuter                           isLeftOuter
//   joinKeys-inner                      isKeys
//   joinKeys-leftOuter                  isKeys isLeftOuter
//   nestNL-inner        isNest
//   nestNL-leftOuter    isNest                 isLeftOuter
//   nestKeys-inner      isNest          isKeys
//   nestKeys-leftOuter  isNest          isKeys isLeftOuter
//   unnest-inner               isUnnest
//   unnest-leftOuter           isUnnest        isLeftOuter
func OpJoinNestedLoop(o *base.Op, lzVars *base.Vars, lzYieldVals base.YieldVals,
	lzYieldErr base.YieldErr, path, pathNext string) {
	var lzErr error

	lzYieldErrOrig := lzYieldErr

	lzYieldErr = func(lzErrIn error) {
		if lzErr == nil {
			lzErr = lzErrIn // Capture the incoming error.
		}
	}

	joinKind := strings.Split(o.Kind, "-")

	isNest := strings.HasPrefix(joinKind[0], "nest") // Ex: "nestNL", "nestKeys".
	isKeys := strings.HasSuffix(joinKind[0], "Keys") // Ex: "joinKeys", "nestKeys".

	isUnnest := joinKind[0] == "unnest"
	isLeftOuter := joinKind[1] == "leftOuter"

	lenLabelsA := len(o.Children[0].Labels) // "A" means left side.
	lenLabelsB := len(o.Children[1].Labels) // "B" means right side.
	lenLabelsAB := lenLabelsA + lenLabelsB  // "AB" means joined.

	labelsAB := make(base.Labels, 0, lenLabelsAB)
	labelsAB = append(labelsAB, o.Children[0].Labels...)
	labelsAB = append(labelsAB, o.Children[1].Labels...)

	exprParams := o.Params
	if isKeys {
		exprParams = o.Params[1].([]interface{})
	}

	var exprFunc base.ExprFunc

	if isUnnest || isKeys {
		// UNNEST and ON KEYS evaluate the expr on the left-side vals only.
		exprFunc =
			MakeExprFunc(lzVars, o.Children[0].Labels, exprParams, pathNext, "JF") // !lz
	} else {
		// Other modes evaluate the expr on the fully joined left+right vals.
		exprFunc =
			MakeExprFunc(lzVars, labelsAB, exprParams, pathNext, "JF") // !lz
	}

	var lzHadInner bool // Used only when isLeftOuter is true.

	var lzValsPre base.Vals

	var lzNestBytes []byte // Used only when isNest is true.

	_, _, _, _ = exprFunc, lzHadInner, lzValsPre, lzNestBytes

	lzValsJoin := make(base.Vals, lenLabelsAB)

	lzYieldValsOrig := lzYieldVals

	lzYieldVals = func(lzValsA base.Vals) {
		if lzErr != nil {
			return
		}

		lzValsJoin = lzValsJoin[:0]
		lzValsJoin = append(lzValsJoin, lzValsA...)

		if isNest { // !lz
			lzNestBytes = lzNestBytes[:0]
			lzNestBytes = append(lzNestBytes, '[')
		} // !lz

		if isLeftOuter { // !lz
			lzHadInner = false
		} // !lz

		var lzVal base.Val

		lzYieldVals := func(lzValsB base.Vals) {
			lzValsJoin = lzValsJoin[0:lenLabelsA]
			lzValsJoin = append(lzValsJoin, lzValsB...)

			lzVals := lzValsJoin

			if isUnnest || isKeys { // !lz
				// UNNEST is a self-join, so the join condition is always true.
				//
				// ON KEYS has the right-driver only providing items with keys
				// that came from the left side, so the join condition is
				// also always true.
				lzVal = base.ValTrue
			} else { // !lz
				lzVal = exprFunc(lzVals, lzYieldErr) // <== emitCaptured: pathNext, "JF"
			} // !lz

			if base.ValEqualTrue(lzVal) {
				if isLeftOuter { // !lz
					lzHadInner = true
				} // !lz

				if isNest { // !lz
					// Append right-side val into lzNestBytes, comma separated.
					//
					// NOTE: Assume right-side nest val will be in lzValsB[-1].
					//
					// TODO: Double check that lzValsB[-1] assumption.
					if len(lzValsB) > 0 && len(lzValsB[len(lzValsB)-1]) > 0 {
						if len(lzNestBytes) > 1 {
							lzNestBytes = append(lzNestBytes, ',')
						}

						lzNestBytes = append(lzNestBytes, lzValsB[len(lzValsB)-1]...)
					}
				} else { // !lz
					lzYieldValsOrig(lzVals) // <== emitCaptured: path ""
				} // !lz
			}
		}

		// The right driver.
		if isUnnest || isKeys { // !lz
			// In UNNEST and ON KEYS case, we evaluate the expr only
			// on the left-side vals.
			lzVals := lzValsA

			lzVal = exprFunc(lzVals, lzYieldErr) // <== emitCaptured: pathNext, "JF"

			if isUnnest { // !lz
				// Case of UNNEST, the evaluated expr's val is an
				// array that we yield  as the right-side's items.
				lzValsPre, _ = base.ArrayYield(lzVal, lzYieldVals, lzValsPre[:0])
			} else { // !lz
				// Case of ON KEYS, the evaluated expr's val is
				// treated as key(s) which we place into a
				// vars.Temps[]. The right driver should fetch and
				// yield based on those keys.
				lzVars.Temps[o.Params[0].(int)] = lzVal

				ExecOp(o.Children[1], lzVars, lzYieldVals, lzYieldErr, pathNext, "JNLI") // !lz

				lzVars.Temps[o.Params[0].(int)] = nil
			} // !lz
		} else { // !lz
			ExecOp(o.Children[1], lzVars, lzYieldVals, lzYieldErr, pathNext, "JNLI") // !lz
		} // !lz

		// Case of NEST, we've been collecting a JSON encoded array of
		// right-side values, which we finally join to the left-side
		// and yield onwards.
		if isNest { // !lz
			if len(lzNestBytes) > 1 && lzErr == nil {
				lzNestBytes = append(lzNestBytes, ']')

				lzValsJoin = lzValsJoin[0:lenLabelsA]
				lzValsJoin = append(lzValsJoin, base.Val(lzNestBytes))

				lzYieldValsOrig(lzValsJoin)
			}
		} // !lz

		// Case of leftOuter join when the right driver was empty.
		if isLeftOuter { // !lz
			if !lzHadInner && lzErr == nil {
				lzValsJoin = lzValsJoin[0:lenLabelsA]

				if isNest { // !lz
					lzValsJoin = append(lzValsJoin, base.ValArrayEmpty)
				} else { // !lz
					for i := 0; i < lenLabelsB; i++ { // !lz
						lzValsJoin = append(lzValsJoin, base.ValMissing)
					} // !lz
				} // !lz

				lzYieldValsOrig(lzValsJoin)
			}
		} // !lz
	}

	// The left driver.
	ExecOp(o.Children[0], lzVars, lzYieldVals, lzYieldErr, pathNext, "JNLO") // !lz

	lzYieldErrOrig(lzErr)
}
