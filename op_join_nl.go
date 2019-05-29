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

func OpJoinNestedLoop(o *base.Op, lzVars *base.Vars, lzYieldVals base.YieldVals,
	lzYieldErr base.YieldErr, path, pathNext string) {
	var lzErr error

	lzYieldErrOrig := lzYieldErr

	lzYieldErr = func(lzErrIn error) {
		if lzErr == nil {
			lzErr = lzErrIn // Capture the incoming error.
		}
	}

	lenLabelsA := len(o.Children[0].Labels)
	lenLabelsB := len(o.Children[1].Labels)
	lenLabelsAB := lenLabelsA + lenLabelsB

	labelsAB := make(base.Labels, 0, lenLabelsAB)
	labelsAB = append(labelsAB, o.Children[0].Labels...)
	labelsAB = append(labelsAB, o.Children[1].Labels...)

	joinKind := strings.Split(o.Kind, "-")

	isNest := strings.HasPrefix(joinKind[0], "nest") // Ex: "nestNL", "nestKeys".

	isUnnest := joinKind[0] == "unnest"

	isLeftOuter := joinKind[1] == "leftOuter"

	exprParams := o.Params

	isKeys := strings.HasSuffix(joinKind[0], "Keys") // Ex: "joinKeys", "nestKeys".
	if isKeys {
		exprParams = o.Params[1].([]interface{})
	}

	var exprFunc base.ExprFunc

	if isUnnest || isKeys {
		exprFunc =
			MakeExprFunc(lzVars, o.Children[0].Labels, exprParams, pathNext, "JF") // !lz
	} else {
		exprFunc =
			MakeExprFunc(lzVars, labelsAB, exprParams, pathNext, "JF") // !lz
	}

	var lzHadInner bool

	var lzValsPre base.Vals

	var lzNestBytes []byte

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

		// Inner (right) driver.
		if isUnnest || isKeys { // !lz
			lzVals := lzValsA

			lzVal = exprFunc(lzVals, lzYieldErr) // <== emitCaptured: pathNext, "JF"

			if isUnnest { // !lz
				lzValsPre, _ = base.ArrayYield(lzVal, lzYieldVals, lzValsPre[:0])
			} else { // !lz
				// Case isKeys, where the right driver should yield
				// fetched items based on the key(s) that we're
				// placing into the vars.Temps[].
				lzVars.Temps[o.Params[0].(int)] = lzVal

				ExecOp(o.Children[1], lzVars, lzYieldVals, lzYieldErr, pathNext, "JNLI") // !lz

				lzVars.Temps[o.Params[0].(int)] = nil
			} // !lz
		} else { // !lz
			ExecOp(o.Children[1], lzVars, lzYieldVals, lzYieldErr, pathNext, "JNLI") // !lz
		} // !lz

		// Case of NEST, we've been collecting a JSON encoded array of
		// right-side values.
		if isNest { // !lz
			if len(lzNestBytes) > 1 && lzErr == nil {
				lzNestBytes = append(lzNestBytes, ']')

				lzValsJoin = lzValsJoin[0:lenLabelsA]
				lzValsJoin = append(lzValsJoin, base.Val(lzNestBytes))

				lzYieldValsOrig(lzValsJoin)
			}
		} // !lz

		// Case of leftOuter join when inner (right) was empty.
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

	// Outer (left) driver.
	ExecOp(o.Children[0], lzVars, lzYieldVals, lzYieldErr, pathNext, "JNLO") // !lz

	lzYieldErrOrig(lzErr)
}
