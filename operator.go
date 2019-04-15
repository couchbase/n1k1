package n1k1

import (
	"github.com/couchbase/n1k1/base"
)

func ExecOperator(o *base.Operator,
	lazyYield base.LazyYield, lazyYieldErr base.LazyYieldErr) {
	if o == nil {
		return
	}

	switch o.Kind {
	case "scan":
		Scan(o.Params, o.Fields, lazyYield, lazyYieldErr) // <== inlineOk

	case "filter":
		types := make(base.Types, len(o.ParentA.Fields)) // TODO.

		if base.LazyScope {
			var lazyExprFunc base.LazyExprFunc

			lazyExprFunc =
				MakeExprFunc(o.ParentA.Fields, types, o.Params, nil, "") // <== inlineOk

			lazyYieldOrig := lazyYield

			lazyYield = func(lazyVals base.LazyVals) {
				lazyVal := lazyExprFunc(lazyVals)
				if base.LazyValEqualTrue(lazyVal) {
					lazyYieldOrig(lazyVals)
				}
			}

			ExecOperator(o.ParentA, lazyYield, lazyYieldErr) // <== inlineOk
		}

	case "project":
		types := make(base.Types, len(o.ParentA.Fields)) // TODO.
		outTypes := base.Types{""}                       // TODO.

		if base.LazyScope {
			var lazyProjectFunc base.LazyProjectFunc

			lazyProjectFunc =
				MakeProjectFunc(o.ParentA.Fields, types, o.Params, outTypes) // <== inlineOk

			var lazyValsProjected base.LazyVals

			lazyYieldOrig := lazyYield

			lazyYield = func(lazyValsIn base.LazyVals) {
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

		if base.LazyScope {
			var lazyExprFunc base.LazyExprFunc

			lazyExprFunc =
				MakeExprFunc(fieldsAB, typesAB, o.Params, nil, "") // <== inlineOk

			var lazyValsJoin base.LazyVals

			lazyYieldOrig := lazyYield

			lazyYield = func(lazyValsA base.LazyVals) {
				lazyValsJoin = lazyValsJoin[:0]
				lazyValsJoin = append(lazyValsJoin, lazyValsA...)

				lazyYield = func(lazyValsB base.LazyVals) {
					lazyValsJoin = lazyValsJoin[0:len(lazyValsA)]
					lazyValsJoin = append(lazyValsJoin, lazyValsB...)

					lazyVal := lazyExprFunc(lazyValsJoin)
					if base.LazyValEqualTrue(lazyVal) {
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
