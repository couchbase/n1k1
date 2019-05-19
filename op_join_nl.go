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

	isUnnest := joinKind[0] == "unnest"

	isOuterLeft := joinKind[1] == "outerLeft"

	joinClauseFunc :=
		MakeExprFunc(lzVars, labelsAB, o.Params, pathNext, "JF") // !lz

	var lzHadInner bool

	var lzValsPre base.Vals

	_, _, _ = joinClauseFunc, lzHadInner, lzValsPre

	lzValsJoin := make(base.Vals, lenLabelsAB)

	lzYieldValsOrig := lzYieldVals

	lzYieldVals = func(lzValsA base.Vals) {
		if lzErr != nil {
			return
		}

		lzValsJoin = lzValsJoin[:0]
		lzValsJoin = append(lzValsJoin, lzValsA...)

		if isOuterLeft { // !lz
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
				if isOuterLeft { // !lz
					lzHadInner = true
				} // !lz

				lzYieldValsOrig(lzVals) // <== emitCaptured: path ""
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

		// Case of outerLeft join when inner (right) was empty.
		if isOuterLeft { // !lz
			if !lzHadInner && lzErr == nil {
				lzValsJoin = lzValsJoin[0:lenLabelsA]
				for i := 0; i < lenLabelsB; i++ { // !lz
					lzValsJoin = append(lzValsJoin, base.ValMissing)
				} // !lz

				lzYieldValsOrig(lzValsJoin)
			}
		} // !lz
	}

	// Outer (left) driver.
	ExecOp(o.Children[0], lzVars, lzYieldVals, lzYieldErr, pathNext, "JNLO") // !lz

	lzYieldErrOrig(lzErr)
}
