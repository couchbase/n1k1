package n1k1

import (
	"github.com/couchbase/n1k1/base"
)

func ExecOperator(o *base.Operator,
	lazyYield base.YieldVals, lazyYieldErr base.YieldErr) {
	if o == nil {
		return
	}

	switch o.Kind {
	case "scan":
		Scan(o.Params, o.Fields, lazyYield, lazyYieldErr) // <== inlineOk

	case "filter":
		types := make(base.Types, len(o.ParentA.Fields)) // TODO.

		if LazyScope {
			var lazyExprFunc base.ExprFunc

			lazyExprFunc =
				MakeExprFunc(o.ParentA.Fields, types, o.Params, nil, "") // <== inlineOk

			lazyYieldOrig := lazyYield

			lazyYield = func(lazyVals base.Vals) {
				lazyVal := lazyExprFunc(lazyVals)
				if base.ValEqualTrue(lazyVal) {
					lazyYieldOrig(lazyVals)
				}
			}

			ExecOperator(o.ParentA, lazyYield, lazyYieldErr) // <== inlineOk
		}

	case "project":
		types := make(base.Types, len(o.ParentA.Fields)) // TODO.
		outTypes := base.Types{""}                       // TODO.

		if LazyScope {
			var lazyProjectFunc base.ProjectFunc

			lazyProjectFunc =
				MakeProjectFunc(o.ParentA.Fields, types, o.Params, outTypes) // <== inlineOk

			var lazyValsProjected base.Vals

			lazyYieldOrig := lazyYield

			lazyYield = func(lazyValsIn base.Vals) {
				lazyValsProjected = lazyValsProjected[:0]

				lazyValsProjected = lazyProjectFunc(lazyValsIn, lazyValsProjected)

				lazyYieldOrig(lazyValsProjected)
			}

			ExecOperator(o.ParentA, lazyYield, lazyYieldErr) // <== inlineOk
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

			lazyYieldOrig := lazyYield

			lazyYield = func(lazyValsA base.Vals) {
				lazyValsJoin = lazyValsJoin[:0]
				lazyValsJoin = append(lazyValsJoin, lazyValsA...)

				lazyYield = func(lazyValsB base.Vals) {
					lazyValsJoin = lazyValsJoin[0:len(lazyValsA)]
					lazyValsJoin = append(lazyValsJoin, lazyValsB...)

					lazyVal := lazyExprFunc(lazyValsJoin)
					if base.ValEqualTrue(lazyVal) {
						lazyYieldOrig(lazyValsJoin)
					}
				}

				// Inner...
				ExecOperator(o.ParentB, lazyYield, lazyYieldErr) // <== inlineOk
			}

			// Outer...
			ExecOperator(o.ParentA, lazyYield, lazyYieldErr) // <== inlineOk
		}
	}
}
