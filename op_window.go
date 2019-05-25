//  Copyright (c) 2019 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package n1k1

import (
	"encoding/binary" // <== genCompiler:hide

	"bytes" // <== genCompiler:hide

	"strings"

	"github.com/couchbase/n1k1/base"

	"github.com/couchbase/rhmap/store" // <== genCompiler:hide
)

// OpWindowPartition maintains a current window partition in a vars
// temp slot as it processes incoming vals. This operator depends on
// its child operator to produce vals that are sorted by the same
// sorting expressions as this operator's PARTITION-BY and ORDER-BY
// expressions. When a vals from the next partition appears, all the
// collected vals from the current partition are yielded before
// reseting the current partition to reuse it as the next partition.
// This operator can optionally track rank / numbering related info.
func OpWindowPartition(o *base.Op, lzVars *base.Vars, lzYieldVals base.YieldVals,
	lzYieldErr base.YieldErr, path, pathNext string) {
	partitionSlot := o.Params[0].(int) // Vars.Temps slot number.

	// PARTITION-BY & ORDER-BY expressions.
	partitionExprs := o.Params[1].([]interface{}) // Can be 0 length.

	// The subset of the partitionExprs that are for PARTITION-BY.
	// partitionExprs[:partitionPrefix] is the PARTITION-BY.
	// partitionExprs[partitionPrefix:] is the ORDER-BY.
	partitionPrefix := o.Params[2].(int)

	// The track config is a comma-separated list of additional
	// information to track for each partition entry, such as
	// different kinds of ranks and numberings.
	track := o.Params[3].(string)

	trackOn := len(track) > 0

	trackRank := strings.Index(track, "rank") >= 0

	trackDenseRank := strings.Index(track, "denseRank") >= 0

	// A heap data structure is allocated but is used merely as an
	// appendable sequence of []byte items, not as an actual heap.
	lzHeap, lzErr := lzVars.Ctx.AllocHeap()
	if lzErr != nil {
		lzYieldErr(lzErr)
	} else {
		// Incremented whenever we start a new partition.
		var lzPartitionId uint64

		lzHeap.Extra = lzPartitionId

		lzVars.TempSet(partitionSlot, lzHeap)

		pathNextWP := EmitPush(pathNext, "WP") // !lz

		// The partitioning exprs are treated as a projection.
		var partitionExprsFunc base.ProjectFunc // !lz

		if len(partitionExprs) > 0 { // !lz
			partitionExprsFunc =
				MakeProjectFunc(lzVars, o.Children[0].Labels, partitionExprs, pathNextWP, "PF") // !lz
		} // !lz

		_ = partitionExprsFunc // !lz

		var lzValsOut base.Vals

		var lzPartitionNext, lzPartitionCurr, lzOrderNext, lzOrderCurr, lzHeapBytes, lzBytes []byte

		var lzRank, lzDenseRank uint64

		var lzBuf8Rank, lzBuf8DenseRank [8]byte

		_, _ = lzBuf8Rank, lzBuf8DenseRank

		lzYieldValsOrig := lzYieldVals

		lzYieldVals = func(lzVals base.Vals) {
			lzPartitionNext = lzPartitionNext[:0]
			lzOrderNext = lzOrderNext[:0]

			if len(partitionExprs) > 0 { // !lz
				lzValsOut = lzValsOut[:0]

				lzValsOut = partitionExprsFunc(lzVals, lzValsOut, lzYieldErr) // <== emitCaptured: pathNextWP "PF"

				lzPartitionNext, lzErr = base.ValsEncodeCanonical(lzValsOut[:partitionPrefix], lzPartitionNext[:0], lzVars.Ctx.ValComparer)
				if lzErr == nil {
					lzOrderNext, lzErr = base.ValsEncodeCanonical(lzValsOut[partitionPrefix:], lzOrderNext[:0], lzVars.Ctx.ValComparer)
				}
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

					lzHeap.Reset()

					lzPartitionId++

					lzHeap.Extra = lzPartitionId

					lzPartitionCurr = append(lzPartitionCurr[:0], lzPartitionNext...)

					// Also, when there's a new partition, reset the
					// rank-related tracking info.
					lzOrderCurr, lzRank, lzDenseRank = lzOrderCurr[:0], 0, 0
				}

				if trackOn { // !lz
					if !bytes.Equal(lzOrderCurr, lzOrderNext) {
						lzOrderCurr = append(lzOrderCurr[:0], lzOrderNext...)

						if trackRank { // !lz
							lzRank = uint64(lzHeap.Len()) + 1
						} // !lz

						if trackDenseRank { // !lz
							lzDenseRank++
						} // !lz
					}

					lzValsOut = append(lzValsOut[:0], lzVals...)

					if trackRank { // !lz
						binary.LittleEndian.PutUint64(lzBuf8Rank[:], lzRank)
						lzValsOut = append(lzValsOut, base.Val(lzBuf8Rank[:]))
					} // !lz

					if trackDenseRank { // !lz
						binary.LittleEndian.PutUint64(lzBuf8DenseRank[:], lzDenseRank)
						lzValsOut = append(lzValsOut, base.Val(lzBuf8DenseRank[:]))
					} // !lz

					lzVals = lzValsOut
				} // !lz

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
	partitionSlot := o.Params[0].(int) // Vars.Temps slot number.
	framesSlot := o.Params[1].(int)    // Vars.Temps slot number.
	framesCfg := o.Params[2].([]interface{})
	framesLen := len(framesCfg)

	if LzScope {
		var lzHeap *store.Heap

		var lzFrames []base.WindowFrame

		var lzPartitionId, lzCurrentPos uint64

		lzYieldValsOrig := lzYieldVals

		lzYieldVals = func(lzVals base.Vals) {
			if lzHeap == nil {
				lzHeap = lzVars.TempGetHeap(partitionSlot)

				lzFrames = make([]base.WindowFrame, framesLen)
				for lzI := range lzFrames {
					lzFrame := &lzFrames[lzI]
					lzFrame.Init(framesCfg[lzI], lzHeap)
				}

				lzVars.TempSet(framesSlot, lzFrames)
			}

			if lzPartitionId != lzHeap.Extra.(uint64) {
				// We've encountered a new partition.
				lzPartitionId = lzHeap.Extra.(uint64)

				for lzI := range lzFrames {
					lzFrame := &lzFrames[lzI]
					lzFrame.PartitionStart()
				}

				lzCurrentPos = 0
			} else {
				lzCurrentPos++
			}

			for lzI := range lzFrames {
				lzFrame := &lzFrames[lzI]

				lzErr := lzFrame.CurrentUpdate(lzCurrentPos)
				if lzErr != nil {
					lzYieldErr(lzErr)
				}
			}

			lzYieldValsOrig(lzVals)
		}
	}

	ExecOp(o.Children[0], lzVars, lzYieldVals, lzYieldErr, pathNext, "WFC") // !lz
}
