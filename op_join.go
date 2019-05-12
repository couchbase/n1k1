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

	joinKind := strings.Split(o.Kind, "-")[1] // Ex: "inner", "outerLeft".

	isOuterLeft := joinKind == "outerLeft"

	joinClauseFunc :=
		MakeExprFunc(lzVars, labelsAB, o.Params, pathNext, "JF") // !lz

	var lzHadInner bool

	_, _ = joinClauseFunc, lzHadInner

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

		lzYieldVals := func(lzValsB base.Vals) {
			lzValsJoin = lzValsJoin[0:lenLabelsA]
			lzValsJoin = append(lzValsJoin, lzValsB...)

			lzVals := lzValsJoin

			var lzVal base.Val

			lzVal = joinClauseFunc(lzVals, lzYieldErr) // <== emitCaptured: pathNext, "JF"

			if base.ValEqualTrue(lzVal) {
				if isOuterLeft { // !lz
					lzHadInner = true
				} // !lz

				lzYieldValsOrig(lzVals) // <== emitCaptured: path ""
			}
		}

		// Inner (right) driver.
		ExecOp(o.Children[1], lzVars, lzYieldVals, lzYieldErr, pathNext, "JNLI") // !lz

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
