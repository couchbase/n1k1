package n1k1

import (
	"strings"

	"github.com/couchbase/n1k1/base"
)

func ExecOperator(o *base.Operator, lzYieldVals base.YieldVals,
	lzYieldStats base.YieldStats, lzYieldErr base.YieldErr,
	path, pathItem string) {
	pathNext := EmitPush(path, pathItem)

	defer EmitPop(path, pathItem)

	if o == nil {
		return
	}

	switch o.Kind {
	case "scan":
		Scan(o.Params, o.Fields, lzYieldVals, lzYieldStats, lzYieldErr) // !lz

	case "filter":
		types := make(base.Types, len(o.ParentA.Fields)) // TODO.

		if LzScope {
			pathNextF := EmitPush(pathNext, "F") // !lz

			var lzExprFunc base.ExprFunc

			lzExprFunc =
				MakeExprFunc(o.ParentA.Fields, types, o.Params, nil, pathNextF, "FF") // !lz

			lzYieldValsOrig := lzYieldVals

			_, _ = lzExprFunc, lzYieldValsOrig

			lzYieldVals = func(lzVals base.Vals) {
				var lzVal base.Val

				lzVal = lzExprFunc(lzVals) // <== emitCaptured: pathNextF "FF"

				if base.ValEqualTrue(lzVal) {
					lzYieldValsOrig(lzVals) // <== emitCaptured: path ""
				}
			}

			EmitPop(pathNext, "F") // !lz

			ExecOperator(o.ParentA, lzYieldVals, lzYieldStats, lzYieldErr, pathNextF, "") // !lz
		}

	case "project":
		types := make(base.Types, len(o.ParentA.Fields)) // TODO.
		outTypes := base.Types{""}                       // TODO.

		if LzScope {
			pathNextP := EmitPush(pathNext, "P") // !lz

			var lzValsReuse base.Vals // <== varLift: lzValsReuse by path

			var lzProjectFunc base.ProjectFunc

			lzProjectFunc =
				MakeProjectFunc(o.ParentA.Fields, types, o.Params, outTypes, pathNextP, "PF") // !lz

			lzYieldValsOrig := lzYieldVals

			_, _ = lzProjectFunc, lzYieldValsOrig

			lzYieldVals = func(lzVals base.Vals) {
				lzValsOut := lzValsReuse[:0]

				lzValsOut = lzProjectFunc(lzVals, lzValsOut) // <== emitCaptured: pathNextP "PF"

				lzValsReuse = lzValsOut

				lzYieldValsOrig(lzValsOut)
			}

			EmitPop(pathNext, "P") // !lz

			ExecOperator(o.ParentA, lzYieldVals, lzYieldStats, lzYieldErr, pathNextP, "") // !lz
		}

	case "join-inner-nl":
		ExecJoinNestedLoop(o, lzYieldVals, lzYieldStats, lzYieldErr, path, pathNext) // !lz

	case "join-outerLeft-nl":
		ExecJoinNestedLoop(o, lzYieldVals, lzYieldStats, lzYieldErr, path, pathNext) // !lz

	case "join-outerRight-nl":
		ExecJoinNestedLoop(o, lzYieldVals, lzYieldStats, lzYieldErr, path, pathNext) // !lz

	case "join-outerFull-nl":
		ExecJoinNestedLoop(o, lzYieldVals, lzYieldStats, lzYieldErr, path, pathNext) // !lz
	}
}

func ExecJoinNestedLoop(o *base.Operator, lzYieldVals base.YieldVals,
	lzYieldStats base.YieldStats, lzYieldErr base.YieldErr,
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

		lzExprFunc =
			MakeExprFunc(fieldsAB, typesAB, o.Params, nil, pathNext, "JF") // !lz

		var lzValsJoinOuterRight base.Vals

		if joinKind == "outerFull" || joinKind == "outerRight" { // !lz
			lzValsJoinOuterRight = make(base.Vals, lenFieldsAB)
		} // !lz

		var lzHadOuter bool
		var lzHadInner bool

		_, _, _, _ = lzExprFunc, lzValsJoinOuterRight, lzHadOuter, lzHadInner

		lzValsJoin := make(base.Vals, lenFieldsAB)

		lzYieldValsOrig := lzYieldVals

		lzYieldVals = func(lzValsA base.Vals) {
			lzValsJoin = lzValsJoin[:0]
			lzValsJoin = append(lzValsJoin, lzValsA...)

			if joinKind == "outerFull" || joinKind == "outerRight" { // !lz
				lzHadOuter = true
			} // !lz

			if joinKind == "outerFull" || joinKind == "outerLeft" { // !lz
				lzHadInner = false
			} // !lz

			if LzScope {
				lzYieldVals := func(lzValsB base.Vals) {
					lzValsJoin = lzValsJoin[0:lenFieldsA]
					lzValsJoin = append(lzValsJoin, lzValsB...)

					if joinKind == "outerFull" || joinKind == "outerLeft" { // !lz
						lzHadInner = true
					} // !lz

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

				// Inner (right)...
				ExecOperator(o.ParentB, lzYieldVals, lzYieldStats, lzYieldErr, pathNext, "JNLI") // !lz

				// Case of outer join when inner (right) was empty.
				if joinKind == "outerFull" || joinKind == "outerLeft" { // !lz
					if !lzHadInner {
						lzValsJoin = lzValsJoin[0:lenFieldsA]
						for i := 0; i < lenFieldsB; i++ { // !lz
							lzValsJoin = append(lzValsJoin, base.ValMissing)
						} // !lz

						lzYieldValsOrig(lzValsJoin)
					}
				} // !lz
			}
		}

		// Outer (left)...
		ExecOperator(o.ParentA, lzYieldVals, lzYieldStats, lzYieldErr, pathNext, "JNLO") // !lz

		// Case of outer join when outer (left) was empty.
		if joinKind == "outerFull" || joinKind == "outerRight" { // !lz
			if !lzHadOuter {
				lzYieldVals := func(lzValsB base.Vals) {
					lzValsJoinOuterRight = lzValsJoinOuterRight[0:lenFieldsA]
					lzValsJoinOuterRight = append(lzValsJoinOuterRight, lzValsB...)

					lzYieldValsOrig(lzValsJoinOuterRight)
				}

				ExecOperator(o.ParentB, lzYieldVals, lzYieldStats, lzYieldErr, pathNext, "JNLIO") // !lz
			}
		} // !lz
	}
}
