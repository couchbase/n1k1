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

package engine

import (
	"encoding/binary" // <== genCompiler:hide

	"bytes" // <== genCompiler:hide

	"strconv" // <== genCompiler:hide

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

	// appendOrderBy: store the ORDER BY value(s) as trailing column(s), so a
	// downstream RANGE/GROUPS frame can compare peers/values via WindowFrame.ValIdx.
	appendOrderBy := false
	if len(o.Params) > 4 {
		appendOrderBy, _ = o.Params[4].(bool)
	}

	// A heap data structure is allocated but is used merely as an
	// appendable sequence of []byte items, without keeping the actual
	// heap invariant.
	lzHeap, lzErr := lzVars.Ctx.AllocHeap()
	if lzErr != nil {
		lzYieldErr(lzErr)
	} else {
		// Incremented whenever we start a new partition.
		var lzPartitionId uint64

		lzHeap.Extra = lzPartitionId

		lzVars.TempSet(partitionSlot, lzHeap)

		pathNextWP := EmitPush(lzVars, pathNext, "WP") // !lz

		// The partitioning exprs are treated as a projection.
		var partitionExprsFunc base.ProjectFunc // !lz

		if len(partitionExprs) > 0 { // !lz
			partitionExprsFunc =
				MakeProjectFunc(lzVars, o.Children[0].Labels, partitionExprs, pathNextWP, "PF") // !lz
		} // !lz

		_ = partitionExprsFunc // !lz

		// The ORDER BY exprs (partitionExprs after the PARTITION-BY prefix), projected
		// and appended to the stored row when appendOrderBy, so a RANGE/GROUPS frame
		// reads the ORDER BY value by index (WindowFrame.ValIdx).
		var orderByProjectFunc base.ProjectFunc                     // !lz
		if appendOrderBy && len(partitionExprs) > partitionPrefix { // !lz
			orderByProjectFunc =
				MakeProjectFunc(lzVars, o.Children[0].Labels, partitionExprs[partitionPrefix:], pathNextWP, "OF") // !lz
		} // !lz

		_ = orderByProjectFunc // !lz

		var lzValsOut base.Vals

		var lzPartitionNext, lzPartitionCurr, lzOrderNext, lzOrderCurr, lzHeapBytes, lzBytes []byte

		var lzRank, lzDenseRank uint64

		_, _ = lzRank, lzDenseRank // TODO: go vet complains about unused vars.

		var lzBuf8Rank, lzBuf8DenseRank [8]byte

		_, _ = lzBuf8Rank, lzBuf8DenseRank

		lzYieldValsOrig := lzYieldVals

		lzYieldVals = func(lzVals base.Vals) {
			lzPartitionNext = lzPartitionNext[:0]
			lzOrderNext = lzOrderNext[:0]

			if len(partitionExprs) > 0 { // !lz
				lzValsOut = lzValsOut[:0]

				lzValsOut = partitionExprsFunc(lzVals, lzValsOut, lzYieldErr) // <== emitCaptured: pathNextWP "PF"

				lzPartitionNext, lzErr = base.ValsEncodeCanonical(
					lzValsOut[:partitionPrefix], lzPartitionNext[:0], lzVars.Ctx.ValComparer)
				if lzErr == nil {
					lzOrderNext, lzErr = base.ValsEncodeCanonical(
						lzValsOut[partitionPrefix:], lzOrderNext[:0], lzVars.Ctx.ValComparer)
				}
			} // !lz

			if lzErr == nil {
				if !bytes.Equal(lzPartitionCurr, lzPartitionNext) {
					// The incoming lzVals represents a new partition,
					// so emit the current partition and reset the
					// current partition before the next PushBytes().
					for lzI := int64(0); lzI < lzHeap.CurItems && lzErr == nil; lzI++ {
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
				} // !lz

				// Augment the stored row with trailing columns: rank/denseRank (when
				// tracking) and/or the ORDER BY value (when appendOrderBy). Rebuild from
				// lzVals (the current row) since the partition-change drain above reuses
				// lzValsOut. The ORDER BY value goes LAST -- conv's ValIdx assumes it at
				// len(inputLabels) (no ranking is combined with a native frame aggregate).
				if trackOn || appendOrderBy { // !lz
					lzValsOut = append(lzValsOut[:0], lzVals...)

					if trackRank { // !lz
						binary.LittleEndian.PutUint64(lzBuf8Rank[:], lzRank)
						lzValsOut = append(lzValsOut, base.Val(lzBuf8Rank[:]))
					} // !lz

					if trackDenseRank { // !lz
						binary.LittleEndian.PutUint64(lzBuf8DenseRank[:], lzDenseRank)
						lzValsOut = append(lzValsOut, base.Val(lzBuf8DenseRank[:]))
					} // !lz

					if appendOrderBy { // !lz
						lzValsOut = orderByProjectFunc(lzVals, lzValsOut, lzYieldErr) // <== emitCaptured: pathNextWP "OF"
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

		// Drain the final (or only) partition on end-of-stream, BEFORE propagating
		// yieldErr(nil): a downstream collect-then-emit op (ORDER BY / GROUP BY) emits
		// its result when it sees nil, so the buffered last partition must reach it
		// first -- otherwise it emits before our deferred rows arrive (empty output).
		// Mirrors OpGroup's yieldErr(nil) drain. Mid-stream partitions already drained
		// inside the yieldVals wrapper on each partition change.
		lzYieldErrOrig := lzYieldErr

		lzYieldErr = func(lzErrIn error) {
			if lzErrIn == nil {
				for lzI := int64(0); lzI < lzHeap.CurItems && lzErr == nil; lzI++ {
					lzHeapBytes, lzErr = lzHeap.Get(lzI)
					if lzErr != nil {
						lzYieldErrOrig(lzErr)
					} else {
						lzValsOut = base.ValsDecode(lzHeapBytes, lzValsOut[:0])

						lzYieldValsOrig(lzValsOut)
					}
				}
			}

			lzYieldErrOrig(lzErrIn)
		}

		EmitPop(pathNext, "WP") // !lz

		ExecOp(o.Children[0], lzVars, lzYieldVals, lzYieldErr, pathNext, "WPC") // !lz
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

	// Params[3] optionally names the window function this op materializes as an extra
	// "^aggregates|<agg.String()>" column, read natively by the projection via
	// exprTreeOptimizeNative (the same path GROUP BY uses). It is either a
	// base.AggCatalog aggregate (sum/count/avg/min/max/...; Params[4] is its operand
	// expr, folded over each row's frame) OR a ranking function ("row_number" /
	// "rank" / "dense_rank", computed from partition position + peer groups, no
	// operand). Absent means boundary-tracking only.
	winFunc := ""
	var aggOperand []interface{}
	if len(o.Params) > 3 {
		winFunc, _ = o.Params[3].(string)
	}
	if len(o.Params) > 4 {
		aggOperand, _ = o.Params[4].([]interface{})
	}
	isRankRowNumber := winFunc == "row_number"
	isRankRank := winFunc == "rank"
	isRankDense := winFunc == "dense_rank"
	isRanking := isRankRowNumber || isRankRank || isRankDense
	aggName := ""
	if winFunc != "" && !isRanking {
		aggName = winFunc
	}

	if LzScope {
		var lzHeap *store.Heap

		var lzFrames []base.WindowFrame

		var lzAgg *base.Agg                // !lz
		var lzAggOperandFunc base.ExprFunc // !lz
		if aggName != "" {                 // !lz
			lzAgg = base.Aggs[base.AggCatalog[aggName]]                                                // !lz
			lzAggOperandFunc = MakeExprFunc(lzVars, o.Children[0].Labels, aggOperand, pathNext, "WFA") // !lz
		} // !lz
		_, _ = lzAgg, lzAggOperandFunc // !lz

		// Reused accumulator ping-pong buffers + Result scratch + frame-row decode
		// buffer, so a per-row frame aggregate allocates nothing steady-state.
		var lzAccA, lzAccB, lzResBuf []byte
		var lzFrameVals base.Vals

		// Only the (interpreter-only) agg block below uses these; the gen-compiler
		// strips that block via "// !lz", so keep them "used" in the compiled lane --
		// same guard the rank buffers use above.
		_, _, _, _ = lzAccA, lzAccB, lzResBuf, lzFrameVals

		// Ranking output buffer (row_number / rank / dense_rank). The peer-group state
		// lives in base.WindowFrame (StepRanking), so the op only calls a method +
		// formats -- no field access that the gen-compiler would lift to gen-time.
		var lzRankBuf []byte
		_ = lzRankBuf

		// lzPartitionId starts at a sentinel that no real partition id equals, so
		// the FIRST row always takes the new-partition branch (lzCurrentPos = 0).
		// Without this, an OVER() with no PARTITION BY keeps heap.Extra at 0 --
		// matching a zero-valued lzPartitionId -- so the first row would instead
		// take the else branch (lzCurrentPos = 1), off-by-one, and CurrentUpdate
		// would walk FindGroupEdge one past the partition end (a crash).
		lzPartitionId := ^uint64(0)

		var lzCurrentPos uint64

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

			if isRanking { // !lz
				// StepRanking (a base.WindowFrame method -- runtime, no gen-time field
				// lifting) maintains the peer-group state and returns all three; pick
				// the one this op materializes and append it as JSON. No inner "// !lz":
				// the whole block strips as one unit, like the agg block below. Bind
				// lzFrame first (a plain var) -- a selector on lzFrames[0] (an indexed
				// expr) would be lifted to gen-time by the compiler.
				lzFrame := &lzFrames[0]
				lzRowNum, lzRankV, lzDenseRankV := lzFrame.StepRanking()

				lzRankOut := lzRowNum
				if isRankDense {
					lzRankOut = lzDenseRankV
				} else if isRankRank {
					lzRankOut = lzRankV
				}

				lzRankBuf = strconv.AppendUint(lzRankBuf[:0], lzRankOut, 10)
				lzVals = append(lzVals, base.Val(lzRankBuf))
			} // !lz

			if aggName != "" { // !lz
				// Fold the native aggregate over frame 0's rows: Init a fresh
				// accumulator, Update once per frame row (operand evaluated against
				// that row), then Result. Ping-pong lzAcc/lzAccOther so Update reads
				// the prior state and writes the next without allocating.
				lzFrame := &lzFrames[0]

				lzAcc := lzAgg.Init(lzVars, lzAccA[:0])
				lzAccOther := lzAccB

				lzStepPos := int64(-1)
				lzStepOk := true
				var lzStepErr error
				for {
					lzFrameVals, lzStepPos, lzStepOk, lzStepErr =
						lzFrame.StepVals(true, lzStepPos, lzFrameVals[:0])
					if !lzStepOk || lzStepErr != nil {
						break
					}

					lzOperandVal := lzAggOperandFunc(lzFrameVals, lzYieldErr) // <== emitCaptured: pathNext "WFA"

					lzAccOther, lzAcc, _ = lzAgg.Update(lzVars, lzOperandVal,
						lzAccOther[:0], lzAcc, lzVars.Ctx.ValComparer)

					lzAcc, lzAccOther = lzAccOther, lzAcc
				}
				if lzStepErr != nil {
					lzYieldErr(lzStepErr)
				}

				var lzResVal base.Val
				lzResVal, _, lzResBuf = lzAgg.Result(lzVars, lzAcc, lzResBuf[:0])

				lzAccA, lzAccB = lzAcc, lzAccOther

				lzVals = append(lzVals, lzResVal)
			} // !lz

			lzYieldValsOrig(lzVals)
		}
	}

	ExecOp(o.Children[0], lzVars, lzYieldVals, lzYieldErr, pathNext, "WFC") // !lz
}
