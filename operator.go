package n1k1

import (
	"strings"

	"github.com/couchbase/n1k1/base"
)

func ExecOperator(o *base.Operator,
	lazyYieldVals base.YieldVals, lazyYieldErr base.YieldErr) {
	if o == nil {
		return
	}

	switch o.Kind {
	case "scan":
		Scan(o.Params, o.Fields, lazyYieldVals, lazyYieldErr) // <== inlineOk

	case "filter":
		types := make(base.Types, len(o.ParentA.Fields)) // TODO.

		if LazyScope {
			var lazyExprFunc base.ExprFunc

			lazyExprFunc =
				MakeExprFunc(o.ParentA.Fields, types, o.Params, nil, "") // <== inlineOk

			lazyYieldValsOrig := lazyYieldVals

			lazyYieldVals = func(lazyVals base.Vals) {
				lazyVal := lazyExprFunc(lazyVals)
				if base.ValEqualTrue(lazyVal) {
					lazyYieldValsOrig(lazyVals)
				}
			}

			ExecOperator(o.ParentA, lazyYieldVals, lazyYieldErr) // <== inlineOk
		}

	case "project":
		types := make(base.Types, len(o.ParentA.Fields)) // TODO.
		outTypes := base.Types{""}                       // TODO.

		if LazyScope {
			var lazyProjectFunc base.ProjectFunc

			lazyProjectFunc =
				MakeProjectFunc(o.ParentA.Fields, types, o.Params, outTypes) // <== inlineOk

			var lazyValsProjected base.Vals

			lazyYieldValsOrig := lazyYieldVals

			lazyYieldVals = func(lazyValsIn base.Vals) {
				lazyValsProjected = lazyValsProjected[:0]

				lazyValsProjected = lazyProjectFunc(lazyValsIn, lazyValsProjected)

				lazyYieldValsOrig(lazyValsProjected)
			}

			ExecOperator(o.ParentA, lazyYieldVals, lazyYieldErr) // <== inlineOk
		}

	case "join-inner-nl":
		ExecJoinNestedLoop(o, lazyYieldVals, lazyYieldErr) // <== inlineOk

	case "join-outerLeft-nl":
		ExecJoinNestedLoop(o, lazyYieldVals, lazyYieldErr) // <== inlineOk

	case "join-outerRight-nl":
		ExecJoinNestedLoop(o, lazyYieldVals, lazyYieldErr) // <== inlineOk

	case "join-outerFull-nl":
		ExecJoinNestedLoop(o, lazyYieldVals, lazyYieldErr) // <== inlineOk
	}
}

func ExecJoinNestedLoop(o *base.Operator,
	lazyYieldVals base.YieldVals, lazyYieldErr base.YieldErr) {
	joinKind := strings.Split(o.Kind, "-")[1] // Ex: "inner", "outerLeft".

	lenFieldsA := len(o.ParentA.Fields)
	lenFieldsB := len(o.ParentB.Fields)
	lenFieldsAB := lenFieldsA + lenFieldsB

	fieldsAB := make(base.Fields, 0, lenFieldsAB)
	fieldsAB = append(fieldsAB, o.ParentA.Fields...)
	fieldsAB = append(fieldsAB, o.ParentB.Fields...)

	typesAB := make(base.Types, lenFieldsAB) // TODO.

	if LazyScope {
		var lazyExprFunc base.ExprFunc

		lazyExprFunc =
			MakeExprFunc(fieldsAB, typesAB, o.Params, nil, "") // <== inlineOk

		var lazyValsJoinOuterRight base.Vals
		_ = lazyValsJoinOuterRight

		if joinKind == "outerRight" { // <== inlineOk
			lazyValsJoinOuterRight = make(base.Vals, lenFieldsAB)
		} // <== inlineOk

		lazyValsJoin := make(base.Vals, lenFieldsAB)

		lazyYieldValsOrig := lazyYieldVals

		lazyYieldVals = func(lazyValsA base.Vals) {
			lazyValsJoin = lazyValsJoin[:0]
			lazyValsJoin = append(lazyValsJoin, lazyValsA...)

			if LazyScope {
				lazyYieldVals := func(lazyValsB base.Vals) {
					lazyValsJoin = lazyValsJoin[0:lenFieldsA]
					lazyValsJoin = append(lazyValsJoin, lazyValsB...)

					if joinKind == "outerFull" { // <== inlineOk
						lazyYieldValsOrig(lazyValsJoin)
					} else { // <== inlineOk
						lazyVal := lazyExprFunc(lazyValsJoin)
						if base.ValEqualTrue(lazyVal) {
							lazyYieldValsOrig(lazyValsJoin)
						} else {
							if joinKind == "outerLeft" { // <== inlineOk
								lazyValsJoin = lazyValsJoin[0:lenFieldsA]
								for i := 0; i < lenFieldsB; i++ { // <== inlineOk
									lazyValsJoin = append(lazyValsJoin, base.ValMissing)
								} // <== inlineOk

								lazyYieldValsOrig(lazyValsJoin)
							} // <== inlineOk

							if joinKind == "outerRight" { // <== inlineOk
								lazyValsJoinOuterRight = lazyValsJoinOuterRight[0:lenFieldsA]
								lazyValsJoinOuterRight = append(lazyValsJoinOuterRight, lazyValsB...)

								lazyYieldValsOrig(lazyValsJoinOuterRight)
							} // <== inlineOk
						}
					} // <== inlineOk
				}

				// Inner...
				ExecOperator(o.ParentB, lazyYieldVals, lazyYieldErr) // <== inlineOk
			}
		}

		// Outer...
		ExecOperator(o.ParentA, lazyYieldVals, lazyYieldErr) // <== inlineOk
	}
}
