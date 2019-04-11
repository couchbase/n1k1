package n1ko

// The LazyYield memory ownership rule: the receiver func should copy
// any inputs that it wants to keep, because the provided slices might
// be reused by future invocations.
type LazyYield func(LazyVals)

type LazyYieldErr func(error)

type Operator struct {
	Kind   string   // Ex: "scan", "filter", "project", etc.
	Fields Fields   // Resulting fields of this operator.
	Params []string // Params specific to this operator.

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
		Scan(o.Params, lazyYield, lazyYieldErr) // <== inline-ok.

	case "filter":
		var lazyPredicateFunc LazyPredicateFunc

		lazyPredicateFunc =
			MakePredicateFunc(o.ParentA.Fields, o.Params) // <== inline-ok.

		lazyYieldOrig := lazyYield

		lazyYield = func(lazyVals LazyVals) {
			if lazyPredicateFunc(lazyVals) {
				lazyYieldOrig(lazyVals)
			}
		}

		ExecOperator(o.ParentA, lazyYield, lazyYieldErr) // <== inlineOk

	case "project":
		var lazyProjectFunc LazyProjectFunc

		lazyProjectFunc =
			MakeProjectFunc(o.ParentA.Fields, o.Params) // <== inlineOk

		var lazyVals LazyVals

		lazyYieldOrig := lazyYield

		lazyYield = func(lazyValsIn LazyVals) {
			lazyVals = lazyProjectFunc(lazyValsIn, lazyVals[:0])

			lazyYieldOrig(lazyVals)
		}

		ExecOperator(o.ParentA, lazyYield, lazyYieldErr) // <== inlineOk

	case "join-nl": // Nested loop join.
		var fieldsAB Fields

		fieldsAB = append(fieldsAB, o.ParentA.Fields...)
		fieldsAB = append(fieldsAB, o.ParentB.Fields...)

		var lazyPredicateFunc LazyPredicateFunc

		lazyPredicateFunc =
			MakePredicateFunc(fieldsAB, o.Params) // <== inline-ok.

		var lazyVals LazyVals

		lazyYieldOrig := lazyYield

		lazyYield = func(lazyValsA LazyVals) {
			lazyVals = lazyVals[:0]
			lazyVals = append(lazyVals, lazyValsA...)

			lazyYield = func(lazyValsB LazyVals) {
				lazyVals = lazyVals[0:len(lazyValsA)]
				lazyVals = append(lazyVals, lazyValsB...)

				if lazyPredicateFunc(lazyVals) {
					lazyYieldOrig(lazyVals)
				}
			}

			// Inner...
			ExecOperator(o.ParentB, lazyYield, lazyYieldErr) // <== inlineOk
		}

		// Outer...
		ExecOperator(o.ParentA, lazyYield, lazyYieldErr) // <== inlineOk
	}
}
