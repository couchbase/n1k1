package n1k1

import (
	"github.com/couchbase/n1k1/base"
)

// OpTempCapture runs the child op, and appends any yielded vals as an
// entry into a vars.Temps slot.
func OpTempCapture(o *base.Op, lzVars *base.Vars, lzYieldVals base.YieldVals,
	lzYieldErr base.YieldErr, path, pathNext string) {
	// A heap data structure is allocated but is used without keeping
	// the heap invariant -- we're not using the container/heap's
	// Push(). Instead, we're using the data structure as an
	// appendable sequence of []byte entries.
	tempIdx := o.Params[0].(int)

	lzHeap, lzErr := lzVars.Ctx.AllocHeap()
	if lzErr != nil {
		lzYieldErr(lzErr)
	} else {
		var lzBytes []byte

		lzYieldVals := func(lzVals base.Vals) {
			lzErr = lzHeap.PushBytes(base.ValsEncode(lzVals, lzBytes[:0]))
			if lzErr != nil {
				lzYieldErr(lzErr)
			}
		}

		ExecOp(o.Children[0], lzVars, lzYieldVals, lzYieldErr, pathNext, "TC") // !lz

		if lzErr == nil {
			lzVars.TempSet(tempIdx, lzHeap)
		}
	}
}

// -----------------------------------------------------

// OpTempYield yields vals previously captured by OpTempCapture.
func OpTempYield(o *base.Op, lzVars *base.Vars, lzYieldVals base.YieldVals,
	lzYieldErr base.YieldErr, path, pathNext string) {
	tempIdx := o.Params[0].(int)

	var lzErr error

	lzHeap := lzVars.TempGetHeap(tempIdx)
	if lzHeap != nil {
		var lzBytes []byte
		var lzVals base.Vals

		for lzI := 0; lzI < lzHeap.Len() && lzErr == nil; lzI++ {
			lzBytes, lzErr = lzHeap.Get(lzI)
			if lzErr != nil {
				lzYieldErr(lzErr)
			} else {
				lzYieldVals(base.ValsDecode(lzBytes, lzVals[:0]))
			}
		}
	}

	lzYieldErr(lzErr)
}