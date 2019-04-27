package n1k1

import (
	"strings"

	"github.com/couchbase/n1k1/base"
)

func ExecOp(o *base.Op, lzYieldVals base.YieldVals,
	lzYieldStats base.YieldStats, lzYieldErr base.YieldErr,
	path, pathItem string) {
	pathNext := EmitPush(path, pathItem)

	defer EmitPop(path, pathItem)

	if o == nil {
		return
	}

	switch o.Kind {
	case "scan":
		OpScan(o.Params, o.Fields, lzYieldVals, lzYieldStats, lzYieldErr) // !lz

	case "filter":
		OpFilter(o, lzYieldVals, lzYieldStats, lzYieldErr, path, pathNext) // !lz

	case "project":
		OpProject(o, lzYieldVals, lzYieldStats, lzYieldErr, path, pathNext) // !lz

	case "join-nl-inner":
		OpJoinNestedLoop(o, lzYieldVals, lzYieldStats, lzYieldErr, path, pathNext) // !lz

	case "join-nl-outerLeft":
		OpJoinNestedLoop(o, lzYieldVals, lzYieldStats, lzYieldErr, path, pathNext) // !lz

	case "order-by-offset-limit":
		OpOrderByOffsetLimit(o, lzYieldVals, lzYieldStats, lzYieldErr, path, pathNext) // !lz
	}
}

// 	---------------------------------------------------------

func OpJoinNestedLoop(o *base.Op, lzYieldVals base.YieldVals,
	lzYieldStats base.YieldStats, lzYieldErr base.YieldErr,
	path, pathNext string) {
	var lzErr error

	lzYieldErrOrig := lzYieldErr

	lzYieldErr = func(lzErrIn error) {
		if lzErr == nil {
			lzErr = lzErrIn // Capture the incoming error.
		}
	}

	joinKind := strings.Split(o.Kind, "-")[2] // Ex: "inner", "outerLeft".

	lenFieldsA := len(o.ParentA.Fields)
	lenFieldsB := len(o.ParentB.Fields)
	lenFieldsAB := lenFieldsA + lenFieldsB

	fieldsAB := make(base.Fields, 0, lenFieldsAB)
	fieldsAB = append(fieldsAB, o.ParentA.Fields...)
	fieldsAB = append(fieldsAB, o.ParentB.Fields...)

	joinClauseFunc :=
		MakeExprFunc(fieldsAB, nil, o.Params, pathNext, "JF") // !lz

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

		if joinKind == "outerLeft" { // !lz
			lzHadInner = false
		} // !lz

		lzYieldVals := func(lzValsB base.Vals) {
			if joinKind == "outerLeft" { // !lz
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
				if joinKind == "outerLeft" { // !lz
					lzValsJoin = lzValsJoin[0:lenFieldsA]
					for i := 0; i < lenFieldsB; i++ { // !lz
						lzValsJoin = append(lzValsJoin, base.ValMissing)
					} // !lz

					lzYieldValsOrig(lzValsJoin)
				} // !lz
			}
		}

		// Inner (right) driver.
		ExecOp(o.ParentB, lzYieldVals, lzYieldStats, lzYieldErr, pathNext, "JNLI") // !lz

		// Case of outerLeft join when inner (right) was empty.
		if joinKind == "outerLeft" { // !lz
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
	ExecOp(o.ParentA, lzYieldVals, lzYieldStats, lzYieldErr, pathNext, "JNLO") // !lz

	lzYieldErrOrig(lzErr)
}
