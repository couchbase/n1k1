package n1k1

import (
	"strings"

	"github.com/couchbase/n1k1/base"
)

func ExecOperator(o *base.Operator,
	lzYieldVals base.YieldVals, lzYieldErr base.YieldErr,
	path, pathItem string) {
	pathNext := EmitPush(path, pathItem)

	defer EmitPop(path, pathItem)

	if o == nil {
		return
	}

	switch o.Kind {
	case "scan":
		Scan(o.Params, o.Fields, lzYieldVals, lzYieldErr) // !lz

	case "filter":
		types := make(base.Types, len(o.ParentA.Fields)) // TODO.

		if LzScope {
			pathNextF := EmitPush(pathNext, "F") // !lz

			var lzExprFunc base.ExprFunc
			_ = lzExprFunc

			lzExprFunc =
				MakeExprFunc(o.ParentA.Fields, types, o.Params, nil, pathNextF, "FF") // !lz

			lzYieldValsOrig := lzYieldVals
			_ = lzYieldValsOrig

			lzYieldVals = func(lzVals base.Vals) {
				var lzVal base.Val

				lzVal = lzExprFunc(lzVals) // <== emitCaptured: pathNextF "FF"

				if base.ValEqualTrue(lzVal) {
					lzYieldValsOrig(lzVals) // <== emitCaptured: path ""
				}
			}

			EmitPop(pathNext, "F") // !lz

			ExecOperator(o.ParentA, lzYieldVals, lzYieldErr, pathNextF, "") // !lz
		}

	case "project":
		types := make(base.Types, len(o.ParentA.Fields)) // TODO.
		outTypes := base.Types{""}                       // TODO.

		if LzScope {
			pathNextP := EmitPush(pathNext, "P") // !lz

			var lzProjectFunc base.ProjectFunc
			_ = lzProjectFunc

			lzProjectFunc =
				MakeProjectFunc(o.ParentA.Fields, types, o.Params, outTypes, pathNextP, "PF") // !lz

			var lzValsReuse base.Vals // <== varLift: lzValsReuse by path
			_ = lzValsReuse           // <== varLift: lzValsReuse by path

			lzYieldValsOrig := lzYieldVals
			_ = lzYieldValsOrig

			lzYieldVals = func(lzVals base.Vals) {
				lzValsOut := lzValsReuse[:0] // <== varLift: lzValsReuse by path

				lzValsOut = lzProjectFunc(lzVals, lzValsOut) // <== emitCaptured: pathNextP "PF"

				lzValsReuse = lzValsOut // <== varLift: lzValsReuse by path

				lzYieldValsOrig(lzValsOut)
			}

			EmitPop(pathNext, "P") // !lz

			ExecOperator(o.ParentA, lzYieldVals, lzYieldErr, pathNextP, "") // !lz
		}

	case "join-inner-nl":
		ExecJoinNestedLoop(o, lzYieldVals, lzYieldErr, path, pathNext) // !lz

	case "join-outerLeft-nl":
		ExecJoinNestedLoop(o, lzYieldVals, lzYieldErr, path, pathNext) // !lz

	case "join-outerRight-nl":
		ExecJoinNestedLoop(o, lzYieldVals, lzYieldErr, path, pathNext) // !lz

	case "join-outerFull-nl":
		ExecJoinNestedLoop(o, lzYieldVals, lzYieldErr, path, pathNext) // !lz
	}
}

func ExecJoinNestedLoop(o *base.Operator,
	lzYieldVals base.YieldVals, lzYieldErr base.YieldErr,
	path, pathNext string) {
	joinKind := strings.Split(o.Kind, "-")[1] // Ex: "inner", "outerLeft".

	lenFieldsA := len(o.ParentA.Fields)
	lenFieldsB := len(o.ParentB.Fields)
	lenFieldsAB := lenFieldsA + lenFieldsB

	fieldsAB := make(base.Fields, 0, lenFieldsAB)
	fieldsAB = append(fieldsAB, o.ParentA.Fields...)
	fieldsAB = append(fieldsAB, o.ParentB.Fields...)

	typesAB := make(base.Types, lenFieldsAB) // TODO.

	if LzScope {
		var lzExprFunc base.ExprFunc
		_ = lzExprFunc

		lzExprFunc =
			MakeExprFunc(fieldsAB, typesAB, o.Params, nil, pathNext, "JF") // !lz

		var lzValsJoinOuterRight base.Vals
		_ = lzValsJoinOuterRight

		if joinKind == "outerRight" { // !lz
			lzValsJoinOuterRight = make(base.Vals, lenFieldsAB)
		} // !lz

		lzValsJoin := make(base.Vals, lenFieldsAB)

		lzYieldValsOrig := lzYieldVals

		lzYieldVals = func(lzValsA base.Vals) {
			lzValsJoin = lzValsJoin[:0]
			lzValsJoin = append(lzValsJoin, lzValsA...)

			if LzScope {
				lzYieldVals := func(lzValsB base.Vals) {
					lzValsJoin = lzValsJoin[0:lenFieldsA]
					lzValsJoin = append(lzValsJoin, lzValsB...)

					if joinKind == "outerFull" { // !lz
						lzYieldValsOrig(lzValsJoin)
					} else { // !lz
						lzVals := lzValsJoin
						_ = lzVals

						var lzVal base.Val

						lzVal = lzExprFunc(lzVals) // <== emitCaptured: pathNext, "JF"
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

							if joinKind == "outerRight" { // !lz
								lzValsJoinOuterRight = lzValsJoinOuterRight[0:lenFieldsA]
								lzValsJoinOuterRight = append(lzValsJoinOuterRight, lzValsB...)

								lzYieldValsOrig(lzValsJoinOuterRight)
							} // !lz
						}
					} // !lz
				}

				// Inner...
				ExecOperator(o.ParentB, lzYieldVals, lzYieldErr, pathNext, "JNLI") // !lz
			}
		}

		// Outer...
		ExecOperator(o.ParentA, lzYieldVals, lzYieldErr, pathNext, "JNLO") // !lz
	}
}
