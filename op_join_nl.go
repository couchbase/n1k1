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

	isNest := joinKind[0] == "nestNL"
	isUnnest := joinKind[0] == "unnest"
	isLeftOuter := joinKind[1] == "leftOuter"

	joinClauseFunc :=
		MakeExprFunc(lzVars, labelsAB, o.Params, pathNext, "JF") // !lz

	var lzHadInner bool

	var lzValsPre base.Vals

	var lzNestBytes []byte

	_, _, _, _ = joinClauseFunc, lzHadInner, lzValsPre, lzNestBytes

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

			if isUnnest { // !lz
				lzVal = base.ValTrue
			} else { // !lz
				lzVal = joinClauseFunc(lzVals, lzYieldErr) // <== emitCaptured: pathNext, "JF"
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
		if isUnnest { // !lz
			lzVals := lzValsA

			lzVal = joinClauseFunc(lzVals, lzYieldErr) // <== emitCaptured: pathNext, "JF"

			lzValsPre = base.ArrayYield(lzVal, lzYieldVals, lzValsPre[:0])
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
