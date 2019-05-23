package n1k1

import (
	"bytes" // <== genCompiler:hide

	"github.com/couchbase/n1k1/base"

	"github.com/couchbase/rhmap/store" // <== genCompiler:hide
)

// OpWindowPartition maintains a current window partition in a vars
// temp slot as it processes incoming vals. This operator depends on
// its child operator to produce vals that are sorted by the same
// major sorting expressions as this operator's partitioning
// expressions. When a vals from the next partition appears, all the
// collected vals from the current partition are yielded before
// reseting the current partition to reuse it as the next partition.
func OpWindowPartition(o *base.Op, lzVars *base.Vars, lzYieldVals base.YieldVals,
	lzYieldErr base.YieldErr, path, pathNext string) {
	windowPartitionSlot := o.Params[0].(int) // Vars.Temps slot number.

	// Partitioning expressions.
	partitionings := o.Params[1].([]interface{}) // Can be 0 length.

	// A heap data structure is allocated but is used merely as an
	// appendable sequence of []byte items, not as an actual heap.
	lzHeap, lzErr := lzVars.Ctx.AllocHeap()
	if lzErr != nil {
		lzYieldErr(lzErr)
	} else {
		// Incremented whenever we start a new partition.
		var lzPartitionId uint64

		lzHeap.Extra = lzPartitionId

		lzVars.TempSet(windowPartitionSlot, lzHeap)

		pathNextWP := EmitPush(pathNext, "WP") // !lz

		// The partitioning exprs are treated as a projection.
		var partitioningsFunc base.ProjectFunc // !lz

		if len(partitionings) > 0 { // !lz
			partitioningsFunc =
				MakeProjectFunc(lzVars, o.Children[0].Labels, partitionings, pathNextWP, "PF") // !lz
		} // !lz

		_ = partitioningsFunc // !lz

		var lzValsOut base.Vals

		var lzPartitionNext, lzPartitionCurr, lzHeapBytes, lzBytes []byte

		lzYieldValsOrig := lzYieldVals

		lzYieldVals = func(lzVals base.Vals) {
			if len(partitionings) > 0 { // !lz
				lzValsOut = lzValsOut[:0]

				lzValsOut = partitioningsFunc(lzVals, lzValsOut, lzYieldErr) // <== emitCaptured: pathNextWP "PF"

				lzPartitionNext, lzErr = base.ValsEncodeCanonical(lzValsOut, lzPartitionNext[:0], lzVars.Ctx.ValComparer)
			} // !lz

			if lzErr == nil {
				if !bytes.Equal(lzPartitionCurr, lzPartitionNext) {
					// The incoming lzVals represents a new partition,
					// so emit the current partition and reset the
					// current partition before the next PushBytes().
					for lzI := 0; lzI < lzHeap.Len() && lzErr == nil; lzI++ {
						lzHeapBytes, lzErr = lzHeap.Get(lzI)
						if lzErr != nil {
							lzYieldErr(lzErr)
						} else {
							lzValsOut = base.ValsDecode(lzHeapBytes, lzValsOut[:0])

							lzYieldValsOrig(lzValsOut)
						}
					}

					lzPartitionCurr = append(lzPartitionCurr[:0], lzPartitionNext...)

					lzHeap.Reset()

					lzPartitionId++

					lzHeap.Extra = lzPartitionId
				}

				lzBytes = base.ValsEncode(lzVals, lzBytes[:0])

				lzErr = lzHeap.PushBytes(lzBytes)
				if lzErr != nil {
					lzYieldErr(lzErr)
				}
			}
		}

		EmitPop(pathNext, "WP") // !lz

		ExecOp(o.Children[0], lzVars, lzYieldVals, lzYieldErr, pathNext, "WPC") // !lz

		for lzI := 0; lzI < lzHeap.Len() && lzErr == nil; lzI++ {
			lzHeapBytes, lzErr = lzHeap.Get(lzI)
			if lzErr != nil {
				lzYieldErr(lzErr)
			} else {
				lzValsOut = base.ValsDecode(lzHeapBytes, lzValsOut[:0])

				lzYieldValsOrig(lzValsOut)
			}
		}
	}
}

// -------------------------------------------------------------------

// OpWindowFrames maintains a slice of window frames in a vars temp
// slot as it processes incoming vals. This operator depends on its
// child (or some descendent operator) to be an OpWindowPartition.
func OpWindowFrames(o *base.Op, lzVars *base.Vars, lzYieldVals base.YieldVals,
	lzYieldErr base.YieldErr, path, pathNext string) {
	windowPartitionSlot := o.Params[0].(int) // Vars.Temps slot number.
	windowFramesSlot := o.Params[1].(int)    // Vars.Temps slot number.
	windowFramesCfg := o.Params[2].([]interface{})
	windowFramesLen := len(windowFramesCfg)

	if LzScope {
		var lzHeap *store.Heap

		var lzWindowFrames []base.WindowFrame

		var lzPartitionId, lzCurrentPos uint64

		lzYieldValsOrig := lzYieldVals

		lzYieldVals = func(lzVals base.Vals) {
			if lzHeap == nil {
				lzHeap = lzVars.TempGetHeap(windowPartitionSlot)

				lzWindowFrames = make([]base.WindowFrame, windowFramesLen)
				for lzI := range lzWindowFrames {
					lzWindowFrame := &lzWindowFrames[lzI]
					lzWindowFrame.Init(windowFramesCfg[lzI], lzHeap)
				}

				lzVars.TempSet(windowFramesSlot, lzWindowFrames)
			}

			if lzPartitionId != lzHeap.Extra.(uint64) {
				// We've encountered a new partition.
				lzPartitionId = lzHeap.Extra.(uint64)

				for lzI := range lzWindowFrames {
					lzWindowFrame := &lzWindowFrames[lzI]
					lzWindowFrame.PartitionStart()
				}

				lzCurrentPos = 0
			} else {
				lzCurrentPos++
			}

			for lzI := range lzWindowFrames {
				lzWindowFrame := &lzWindowFrames[lzI]
				lzWindowFrame.CurrentUpdate(lzCurrentPos)
			}

			lzYieldValsOrig(lzVals)
		}
	}

	ExecOp(o.Children[0], lzVars, lzYieldVals, lzYieldErr, pathNext, "WFC") // !lz
}
