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

	lenFieldsA := len(o.Children[0].Fields)
	lenFieldsB := len(o.Children[1].Fields)
	lenFieldsAB := lenFieldsA + lenFieldsB

	fieldsAB := make(base.Fields, 0, lenFieldsAB)
	fieldsAB = append(fieldsAB, o.Children[0].Fields...)
	fieldsAB = append(fieldsAB, o.Children[1].Fields...)

	joinKind := strings.Split(o.Kind, "-")[2] // Ex: "inner", "outerLeft".

	isOuterLeft := joinKind == "outerLeft"

	joinClauseFunc :=
		MakeExprFunc(lzVars, fieldsAB, nil, o.Params, pathNext, "JF") // !lz

	var lzHadInner bool

	_, _ = joinClauseFunc, lzHadInner

	lzValsJoin := make(base.Vals, lenFieldsAB)

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
			if isOuterLeft { // !lz
				lzHadInner = true
			} // !lz

			lzValsJoin = lzValsJoin[0:lenFieldsA]
			lzValsJoin = append(lzValsJoin, lzValsB...)

			lzVals := lzValsJoin

			var lzVal base.Val

			lzVal = joinClauseFunc(lzVals) // <== emitCaptured: pathNext, "JF"

			if base.ValEqualTrue(lzVal) {
				lzYieldValsOrig(lzVals) // <== emitCaptured: path ""
			} else {
				if isOuterLeft { // !lz
					lzValsJoin = lzValsJoin[0:lenFieldsA]
					for i := 0; i < lenFieldsB; i++ { // !lz
						lzValsJoin = append(lzValsJoin, base.ValMissing)
					} // !lz

					lzYieldValsOrig(lzValsJoin)
				} // !lz
			}
		}

		// Inner (right) driver.
		ExecOp(o.Children[1], lzVars, lzYieldVals, lzYieldErr, pathNext, "JNLI") // !lz

		// Case of outerLeft join when inner (right) was empty.
		if isOuterLeft { // !lz
			if !lzHadInner && lzErr == nil {
				lzValsJoin = lzValsJoin[0:lenFieldsA]
				for i := 0; i < lenFieldsB; i++ { // !lz
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
