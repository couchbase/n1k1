package n1k1

const LazyScope = true // For marking varible scopes (ex: IF statement).

// The LazyYield memory ownership rule: the receiver func should copy
// any inputs that it wants to keep, because the provided slices might
// be reused by future invocations.
type LazyYield func(LazyVals)

type LazyYieldErr func(error)

type Operator struct {
	Kind   string        // Ex: "scan", "filter", "project", etc.
	Fields Fields        // Output fields of this operator.
	Params []interface{} // Params based on the kind.

	ParentA *Operator
	ParentB *Operator
}

func ExecOperator(o *Operator,
	lazyYield LazyYield, lazyYieldErr LazyYieldErr) {
	if o == nil {
		return
	}

	switch o.Kind {
	case "scan":
		Scan(o.Params, o.Fields, lazyYield, lazyYieldErr) // <== inlineOk

	case "filter":
		types := make(Types, len(o.ParentA.Fields)) // TODO.

		if LazyScope {
			var lazyExprFunc LazyExprFunc

			lazyExprFunc =
				MakeExprFunc(o.ParentA.Fields, types, o.Params, nil, "") // <== inlineOk

			lazyYieldOrig := lazyYield

			lazyYield = func(lazyVals LazyVals) {
				lazyVal := lazyExprFunc(lazyVals)
				if LazyValEqualTrue(lazyVal) {
					lazyYieldOrig(lazyVals)
				}
			}

			ExecOperator(o.ParentA, lazyYield, lazyYieldErr) // <== inlineOk
		}

	case "project":
		types := make(Types, len(o.ParentA.Fields)) // TODO.
		outTypes := Types{""}                       // TODO.

		if LazyScope {
			var lazyProjectFunc LazyProjectFunc

			lazyProjectFunc =
				MakeProjectFunc(o.ParentA.Fields, types, o.Params, outTypes) // <== inlineOk

			var lazyValsProjected LazyVals

			lazyYieldOrig := lazyYield

			lazyYield = func(lazyValsIn LazyVals) {
				lazyValsProjected = lazyValsProjected[:0]

				lazyValsProjected = lazyProjectFunc(lazyValsIn, lazyValsProjected)

				lazyYieldOrig(lazyValsProjected)
			}

			ExecOperator(o.ParentA, lazyYield, lazyYieldErr) // <== inlineOk
		}

	case "join-nl": // Nested loop join.
		var fieldsAB Fields

		fieldsAB = append(fieldsAB, o.ParentA.Fields...)
		fieldsAB = append(fieldsAB, o.ParentB.Fields...)

		typesAB := make(Types, len(fieldsAB)) // TODO.

		if LazyScope {
			var lazyExprFunc LazyExprFunc

			lazyExprFunc =
				MakeExprFunc(fieldsAB, typesAB, o.Params, nil, "") // <== inlineOk

			var lazyValsJoin LazyVals

			lazyYieldOrig := lazyYield

			lazyYield = func(lazyValsA LazyVals) {
				lazyValsJoin = lazyValsJoin[:0]
				lazyValsJoin = append(lazyValsJoin, lazyValsA...)

				lazyYield = func(lazyValsB LazyVals) {
					lazyValsJoin = lazyValsJoin[0:len(lazyValsA)]
					lazyValsJoin = append(lazyValsJoin, lazyValsB...)

					lazyVal := lazyExprFunc(lazyValsJoin)
					if LazyValEqualTrue(lazyVal) {
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
