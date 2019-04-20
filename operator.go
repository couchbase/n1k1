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
		Scan(o.Params, o.Fields, lazyYieldVals, lazyYieldErr) // <== notLazy

	case "filter":
		types := make(base.Types, len(o.ParentA.Fields)) // TODO.

		if LazyScope {
			var lazyExprFunc base.ExprFunc
			_ = lazyExprFunc

			lazyExprFunc =
				MakeExprFunc(o.ParentA.Fields, types, o.Params, nil, o.Kind, "FF") // <== notLazy

			lazyYieldValsOrig := lazyYieldVals

			lazyYieldVals = func(lazyVals base.Vals) {
				var lazyVal base.Val

				lazyVal = lazyExprFunc(lazyVals) // <== emitCaptured: o.Kind FF

				if base.ValEqualTrue(lazyVal) {
					lazyYieldValsOrig(lazyVals)
				}
			}

			ExecOperator(o.ParentA, lazyYieldVals, lazyYieldErr) // <== notLazy
		}

	case "project":
		types := make(base.Types, len(o.ParentA.Fields)) // TODO.
		outTypes := base.Types{""}                       // TODO.

		if LazyScope {
			var lazyProjectFunc base.ProjectFunc

			lazyProjectFunc =
				MakeProjectFunc(o.ParentA.Fields, types, o.Params, outTypes) // <== notLazy

			var lazyValsProjected base.Vals

			lazyYieldValsOrig := lazyYieldVals

			lazyYieldVals = func(lazyValsIn base.Vals) {
				lazyValsProjected = lazyValsProjected[:0]

				lazyValsProjected = lazyProjectFunc(lazyValsIn, lazyValsProjected)

				lazyYieldValsOrig(lazyValsProjected)
			}

			ExecOperator(o.ParentA, lazyYieldVals, lazyYieldErr) // <== notLazy
		}

	case "join-inner-nl":
		ExecJoinNestedLoop(o, lazyYieldVals, lazyYieldErr) // <== notLazy

	case "join-outerLeft-nl":
		ExecJoinNestedLoop(o, lazyYieldVals, lazyYieldErr) // <== notLazy

	case "join-outerRight-nl":
		ExecJoinNestedLoop(o, lazyYieldVals, lazyYieldErr) // <== notLazy

	case "join-outerFull-nl":
		ExecJoinNestedLoop(o, lazyYieldVals, lazyYieldErr) // <== notLazy
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
			MakeExprFunc(fieldsAB, typesAB, o.Params, nil, "", "") // <== notLazy

		var lazyValsJoinOuterRight base.Vals
		_ = lazyValsJoinOuterRight

		if joinKind == "outerRight" { // <== notLazy
			lazyValsJoinOuterRight = make(base.Vals, lenFieldsAB)
		} // <== notLazy

		lazyValsJoin := make(base.Vals, lenFieldsAB)

		lazyYieldValsOrig := lazyYieldVals

		lazyYieldVals = func(lazyValsA base.Vals) {
			lazyValsJoin = lazyValsJoin[:0]
			lazyValsJoin = append(lazyValsJoin, lazyValsA...)

			if LazyScope {
				lazyYieldVals := func(lazyValsB base.Vals) {
					lazyValsJoin = lazyValsJoin[0:lenFieldsA]
					lazyValsJoin = append(lazyValsJoin, lazyValsB...)

					if joinKind == "outerFull" { // <== notLazy
						lazyYieldValsOrig(lazyValsJoin)
					} else { // <== notLazy
						lazyVal := lazyExprFunc(lazyValsJoin)
						if base.ValEqualTrue(lazyVal) {
							lazyYieldValsOrig(lazyValsJoin)
						} else {
							if joinKind == "outerLeft" { // <== notLazy
								lazyValsJoin = lazyValsJoin[0:lenFieldsA]
								for i := 0; i < lenFieldsB; i++ { // <== notLazy
									lazyValsJoin = append(lazyValsJoin, base.ValMissing)
								} // <== notLazy

								lazyYieldValsOrig(lazyValsJoin)
							} // <== notLazy

							if joinKind == "outerRight" { // <== notLazy
								lazyValsJoinOuterRight = lazyValsJoinOuterRight[0:lenFieldsA]
								lazyValsJoinOuterRight = append(lazyValsJoinOuterRight, lazyValsB...)

								lazyYieldValsOrig(lazyValsJoinOuterRight)
							} // <== notLazy
						}
					} // <== notLazy
				}

				// Inner...
				ExecOperator(o.ParentB, lazyYieldVals, lazyYieldErr) // <== notLazy
			}
		}

		// Outer...
		ExecOperator(o.ParentA, lazyYieldVals, lazyYieldErr) // <== notLazy
	}
}
