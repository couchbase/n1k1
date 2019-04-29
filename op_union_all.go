package n1k1

import (
	"github.com/couchbase/n1k1/base"
)

func OpUnionAll(o *base.Op, lzYieldVals base.YieldVals,
	lzYieldStats base.YieldStats, lzYieldErr base.YieldErr,
	path, pathNext string) {
	var lzErr error

	lzYieldErrOrig := lzYieldErr

	lzYieldErr = func(lzErrIn error) {
		if lzErr == nil {
			lzErr = lzErrIn // Capture the incoming error.
		}
	}

	numUnionFields := len(o.Fields)

	lzValsUnion := make(base.Vals, numUnionFields)

	lzYieldValsOrig := lzYieldVals

	for _, child := range o.Children {
		if lzErr == nil {
			lzYieldVals = func(lzVals base.Vals) {
				if lzErr != nil {
					return
				}

				// Remap incoming vals to the union's field positions.
				for unionIdx, unionField := range o.Fields { // !lz
					found := false // !lz

					for childIdx, childField := range child.Fields { // !lz
						if childField == unionField { // !lz
							lzValsUnion[unionIdx] = lzVals[childIdx]
							found = true // !lz
							break        // !lz
						} // !lz
					} // !lz

					if !found { // !lz
						lzValsUnion[unionIdx] = base.ValMissing
					} // !lz
				} // !lz

				lzYieldValsOrig(lzValsUnion)
			}

			ExecOp(child, lzYieldVals, lzYieldStats, lzYieldErr, pathNext, "U") // !lz
		}
	}

	lzYieldErrOrig(lzErr)
}
