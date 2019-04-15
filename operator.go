package n1k1

import (
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

	case "join-nl": // Nested loop join.
		var fieldsAB base.Fields

		fieldsAB = append(fieldsAB, o.ParentA.Fields...)
		fieldsAB = append(fieldsAB, o.ParentB.Fields...)

		typesAB := make(base.Types, len(fieldsAB)) // TODO.

		if LazyScope {
			var lazyExprFunc base.ExprFunc

			lazyExprFunc =
				MakeExprFunc(fieldsAB, typesAB, o.Params, nil, "") // <== inlineOk

			var lazyValsJoin base.Vals

			lazyYieldValsOrig := lazyYieldVals

			lazyYieldVals = func(lazyValsA base.Vals) {
				lazyValsJoin = lazyValsJoin[:0]
				lazyValsJoin = append(lazyValsJoin, lazyValsA...)

				if LazyScope {
					lazyYieldVals := func(lazyValsB base.Vals) {
						lazyValsJoin = lazyValsJoin[0:len(lazyValsA)]
						lazyValsJoin = append(lazyValsJoin, lazyValsB...)

						lazyVal := lazyExprFunc(lazyValsJoin)
						if base.ValEqualTrue(lazyVal) {
							lazyYieldValsOrig(lazyValsJoin)
						}
					}

					// Inner...
					ExecOperator(o.ParentB, lazyYieldVals, lazyYieldErr) // <== inlineOk
				}
			}

			// Outer...
			ExecOperator(o.ParentA, lazyYieldVals, lazyYieldErr) // <== inlineOk
		}
	}
}
