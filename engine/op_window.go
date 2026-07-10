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

	// appendOrderBy: store the ORDER BY as a trailing "^worderby" column, so a
	// downstream RANGE/GROUPS frame or ranking function reads peers/values via
	// WindowFrame.ValIdx. Two modes (Params[4]):
	//   "value" -- the raw single ORDER BY value, for a RANGE frame's numeric bounds
	//              (ParseFloat64). Single ORDER BY column.
	//   "tuple" -- the ORDER BY tuple canonically encoded into ONE column, for peer
	//              detection by bytes.Equal (GROUPS frames + RANK/DENSE_RANK/
	//              PERCENT_RANK/CUME_DIST); works for any number of ORDER BY columns.
	// (A plain bool is also accepted as "value" for back-compat.)
	appendOrderBy := false
	appendOrderByTuple := false
	if len(o.Params) > 4 {
		switch m := o.Params[4].(type) {
		case string:
			appendOrderBy = m != ""
			appendOrderByTuple = m == "tuple"
		case bool:
			appendOrderBy = m
		}
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
		// Only "value" mode projects the raw ORDER BY value(s); "tuple" mode reuses the
		// already-computed canonical ORDER BY tuple (lzOrderNext) instead.
		var orderByProjectFunc base.ProjectFunc                                            // !lz
		if appendOrderBy && !appendOrderByTuple && len(partitionExprs) > partitionPrefix { // !lz
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

					if appendOrderBy && !appendOrderByTuple { // !lz
						lzValsOut = orderByProjectFunc(lzVals, lzValsOut, lzYieldErr) // <== emitCaptured: pathNextWP "OF"
					} // !lz

					if appendOrderByTuple { // !lz
						// The canonical ORDER BY tuple (any number of columns) as ONE
						// column -- peer detection is bytes.Equal on it. ValsEncode below
						// copies, so referencing lzOrderNext is safe.
						lzValsOut = append(lzValsOut, base.Val(lzOrderNext))
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

	// The value an offset function yields when the target lands outside the partition
	// (evaluated at the CURRENT row): LAG/LEAD's 3rd arg, else a "null" operand. Always
	// present for an offset op (Params[8]).
	var offDefaultOperand []interface{}
	if len(o.Params) > 8 {
		offDefaultOperand, _ = o.Params[8].([]interface{})
	}
	isRankRowNumber := winFunc == "row_number"
	isRankRank := winFunc == "rank"
	isRankDense := winFunc == "dense_rank"
	isRankPercent := winFunc == "percent_rank"
	isRankCume := winFunc == "cume_dist"
	isRankNtile := winFunc == "ntile"
	isRanking := isRankRowNumber || isRankRank || isRankDense ||
		isRankPercent || isRankCume || isRankNtile
	rankNtileN := int64(1) // NTILE(k): k in Params[4] (an int, not the operand slice).
	if isRankNtile && len(o.Params) > 4 {
		if nt, ok := o.Params[4].(int); ok {
			rankNtileN = int64(nt)
		}
	}

	// Offset / navigation functions (FIRST_VALUE / LAST_VALUE / NTH_VALUE / LAG /
	// LEAD): Params[5..7] carry the StepToOffset navigation (initial, asc, num);
	// Params[4] (aggOperand) is the operand evaluated at the target row.
	isOffset := winFunc == "first_value" || winFunc == "last_value" ||
		winFunc == "nth_value" || winFunc == "lag" || winFunc == "lead"
	offsetInitial := 0
	offsetAsc := false
	offsetNum := uint64(1)
	if isOffset && len(o.Params) > 7 {
		offsetInitial, _ = o.Params[5].(int)
		offsetAsc, _ = o.Params[6].(bool)
		if n, ok := o.Params[7].(int); ok && n > 0 {
			offsetNum = uint64(n)
		}
	}

	// RATIO_TO_REPORT(x) = x / SUM(x over the frame): folds SUM (like an aggregate) but
	// divides the current row's operand by that sum. Handled by the agg block below with
	// lzAgg = SUM; NOT an aggName (there's no "ratio_to_report" in AggCatalog).
	isRatioToReport := winFunc == "ratio_to_report"

	aggName := ""
	if winFunc != "" && !isRanking && !isOffset && !isRatioToReport {
		aggName = winFunc
	}

	// isCount gates the invertible sliding-COUNT fast path. A baked bool, not a literal
	// `aggName == "count"` inside the emitted agg block -- a string literal there is
	// mangled into a "%s" placeholder by the gen-compiler (the base.Val("null") landmine).
	isCount := aggName == "count"

	if LzScope {
		var lzHeap *store.Heap

		var lzFrames []base.WindowFrame

		var lzAgg *base.Agg                // !lz
		var lzOperandFunc base.ExprFunc    // !lz
		var lzOffDefaultFunc base.ExprFunc // !lz
		if aggName != "" {                 // !lz
			lzAgg = base.Aggs[base.AggCatalog[aggName]]                                             // !lz
			lzOperandFunc = MakeExprFunc(lzVars, o.Children[0].Labels, aggOperand, pathNext, "WFA") // !lz
		} else if isOffset { // !lz  -- the offset block shares the operand func
			lzOperandFunc = MakeExprFunc(lzVars, o.Children[0].Labels, aggOperand, pathNext, "WFA")           // !lz
			lzOffDefaultFunc = MakeExprFunc(lzVars, o.Children[0].Labels, offDefaultOperand, pathNext, "WFD") // !lz
		} else if isRatioToReport { // !lz  -- fold SUM over the frame, divide the current row
			lzAgg = base.Aggs[base.AggCatalog["sum"]]                                               // !lz
			lzOperandFunc = MakeExprFunc(lzVars, o.Children[0].Labels, aggOperand, pathNext, "WFA") // !lz
		} // !lz
		_, _, _ = lzAgg, lzOperandFunc, lzOffDefaultFunc // !lz

		// Reused accumulator ping-pong buffers + Result scratch + frame-row decode
		// buffer, so a per-row frame aggregate allocates nothing steady-state.
		var lzAccA, lzAccB, lzResBuf []byte
		var lzFrameVals base.Vals

		// Carried accumulator for the left-anchored incremental fold (the whole-partition
		// / running-total fast path -- see the agg block). lzGrowFolded is how many
		// leading partition rows have already been folded into lzGrowAcc; both reset at
		// each partition start.
		var lzGrowAcc, lzGrowAccOther []byte
		var lzGrowFolded int64

		// Invertible sliding-window COUNT state (see the agg block): the running count
		// and the previous frame edges, so a forward-sliding frame adjusts by the rows
		// that entered/left rather than re-counting. Reset per partition.
		var lzSlideCount, lzSlidePrevBeg, lzSlidePrevEnd int64

		// Only the (interpreter-only) agg block below uses these; the gen-compiler
		// strips that block via "// !lz", so keep them "used" in the compiled lane --
		// same guard the rank buffers use above.
		_, _, _, _ = lzAccA, lzAccB, lzResBuf, lzFrameVals
		_, _, _ = lzGrowAcc, lzGrowAccOther, lzGrowFolded
		_, _, _ = lzSlideCount, lzSlidePrevBeg, lzSlidePrevEnd

		// Partition-level output buffer (row_number / rank / dense_rank / percent_rank /
		// cume_dist / ntile). The peer-group state lives in base.WindowFrame
		// (WindowRankValue), which also formats -- so the op only calls a method, no
		// field access that the gen-compiler would lift to gen-time.
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
				// WindowRankValue (a base.WindowFrame method -- runtime, no gen-time
				// field lifting) computes + formats the partition-level function. No
				// inner "// !lz": the whole block strips as one unit, like the agg block
				// below. Bind lzFrame first (a plain var) -- a selector on lzFrames[0]
				// (an indexed expr) would be lifted to gen-time by the compiler.
				lzFrame := &lzFrames[0]

				lzRankKind := base.WRankRowNumber
				if isRankRank {
					lzRankKind = base.WRankRank
				} else if isRankDense {
					lzRankKind = base.WRankDenseRank
				} else if isRankPercent {
					lzRankKind = base.WRankPercentRank
				} else if isRankCume {
					lzRankKind = base.WRankCumeDist
				} else if isRankNtile {
					lzRankKind = base.WRankNtile
				}

				var lzRankVal base.Val
				lzRankVal, lzRankBuf = lzFrame.WindowRankValue(lzRankKind, rankNtileN, lzRankBuf[:0])
				lzVals = append(lzVals, lzRankVal)
			} // !lz

			if isOffset { // !lz
				// Offset / navigation: StepToOffset (a base.WindowFrame method --
				// runtime, no gen-time field lifting) walks to the target row; evaluate
				// the operand there and append it. A target outside the frame (e.g. LAG
				// at the partition start) yields NULL. No inner "// !lz" -- strips as one
				// unit by brace depth. Bind lzFrame first (a plain var), like the agg
				// block; the operand func (built above) is inlined at the emitCaptured
				// call site "WFA".
				lzFrame := &lzFrames[0]

				var lzOffOk bool
				var lzOffErr error
				lzFrameVals, lzOffOk, lzOffErr = lzFrame.StepToOffset(offsetInitial, offsetAsc, offsetNum, lzFrameVals[:0])
				if lzOffErr != nil {
					lzYieldErr(lzOffErr)
				}

				if lzOffOk {
					lzOffVal := lzOperandFunc(lzFrameVals, lzYieldErr) // <== emitCaptured: pathNext "WFA"
					lzVals = append(lzVals, lzOffVal)
				} else {
					// Target outside the partition: yield the default, evaluated at the
					// CURRENT row (lzVals). It is a "null" operand except for LAG/LEAD's
					// 3rd arg, so this covers both NULL and an explicit default value.
					lzOffDefVal := lzOffDefaultFunc(lzVals, lzYieldErr) // <== emitCaptured: pathNext "WFD"
					lzVals = append(lzVals, lzOffDefVal)
				}
			} // !lz

			if aggName != "" || isRatioToReport { // !lz
				// Fold the native aggregate over frame 0's rows and append the Result
				// (RATIO_TO_REPORT folds SUM, then divides the current row's operand by
				// it below). This whole block is interpreter-only and strips as one unit,
				// so the inner branches are plain Go (no nested "// !lz").
				lzFrame := &lzFrames[0]

				var lzResVal base.Val

				// Left-anchored, no-EXCLUDE frame (UNBOUNDED PRECEDING AND <anything>): as
				// Pos advances the frame only GROWS (Include.Beg stays 0; Include.End is
				// monotone non-decreasing), so carry the accumulator across rows and fold
				// only the rows that newly entered -- O(N) over the partition instead of
				// re-folding [0,End) every row (O(N^2)). This is the common case: whole
				// partition (OVER (PARTITION BY x)), running total (OVER (ORDER BY x)), and
				// UNBOUNDED PRECEDING AND n FOLLOWING/PRECEDING. Every base aggregate is
				// add-only over a growing frame, so the carried fold is exact.
				//   lzGrowFolded = # leading rows already folded into lzGrowAcc (reset per
				//   partition). StepVals(true, lzGrowFolded-1, ...) resumes at lzGrowFolded
				//   and stops at Include.End, so each partition row is folded exactly once.
				if lzFrame.BegBoundary != base.WTokNum && lzFrame.Exclude == base.WTokNoOthers {
					if lzCurrentPos == 0 {
						lzGrowAcc = lzAgg.Init(lzVars, lzGrowAcc[:0])
						lzGrowFolded = 0
					}

					lzStepPos := lzGrowFolded - 1
					for {
						var lzOk bool
						var lzErr2 error
						lzFrameVals, lzStepPos, lzOk, lzErr2 =
							lzFrame.StepVals(true, lzStepPos, lzFrameVals[:0])
						if lzErr2 != nil {
							lzYieldErr(lzErr2)
							break
						}
						if !lzOk {
							break
						}

						lzOperandVal := lzOperandFunc(lzFrameVals, lzYieldErr) // <== emitCaptured: pathNext "WFA"

						lzGrowAccOther, lzGrowAcc, _ = lzAgg.Update(lzVars, lzOperandVal,
							lzGrowAccOther[:0], lzGrowAcc, lzVars.Ctx.ValComparer)
						lzGrowAcc, lzGrowAccOther = lzGrowAccOther, lzGrowAcc

						lzGrowFolded = lzStepPos + 1
					}

					lzResVal, _, lzResBuf = lzAgg.Result(lzVars, lzGrowAcc, lzResBuf[:0])
				} else if isCount && lzFrame.Exclude == base.WTokNoOthers {
					// Invertible sliding COUNT (Include.Beg finite -- not left-anchored --
					// so the frame slides forward: Include.Beg and Include.End are both
					// monotone non-decreasing). Instead of re-counting the whole frame each
					// row, adjust a running count by the rows that entered/left. COUNT is
					// an integer and exactly invertible (unlike a float SUM, which would
					// drift), so add-entering/remove-leaving matches the brute-force fold
					// bit-for-bit -- O(N) over the partition regardless of window size (the
					// grep -A/-B/-C shape, ROWS BETWEEN N PRECEDING AND N FOLLOWING).
					if lzCurrentPos == 0 {
						lzSlideCount, lzSlidePrevBeg, lzSlidePrevEnd = 0, 0, 0
					}

					var lzSlideErr error

					// Rows that ENTERED [prevEnd, End): +1 each if they count (a non-NULL,
					// non-MISSING operand -- matching AggCount). Enter before leave so the
					// running count never dips below zero.
					for lzI := lzSlidePrevEnd; lzI < lzFrame.Include.End && lzSlideErr == nil; lzI++ {
						lzFrameVals, lzSlideErr = lzFrame.RowVals(lzI, lzFrameVals[:0])
						lzEnterVal := lzOperandFunc(lzFrameVals, lzYieldErr) // <== emitCaptured: pathNext "WFA"
						if lzSlideErr == nil && base.ValHasValue(lzEnterVal) {
							lzSlideCount++
						}
					}
					// Rows that LEFT [prevBeg, Beg): -1 each if they had counted.
					for lzI := lzSlidePrevBeg; lzI < lzFrame.Include.Beg && lzSlideErr == nil; lzI++ {
						lzFrameVals, lzSlideErr = lzFrame.RowVals(lzI, lzFrameVals[:0])
						lzLeaveVal := lzOperandFunc(lzFrameVals, lzYieldErr) // <== emitCaptured: pathNext "WFA"
						if lzSlideErr == nil && base.ValHasValue(lzLeaveVal) {
							lzSlideCount--
						}
					}
					if lzSlideErr != nil {
						lzYieldErr(lzSlideErr)
					}

					lzSlidePrevBeg = lzFrame.Include.Beg
					lzSlidePrevEnd = lzFrame.Include.End

					lzResVal, lzResBuf = base.WindowCountResult(lzSlideCount, lzResBuf[:0])
				} else {
					// General frame (fixed slide / arbitrary bounds / EXCLUDE): re-fold
					// the whole frame from scratch each row. Ping-pong lzAcc/lzAccOther so
					// Update reads the prior state and writes the next without allocating.
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

						lzOperandVal := lzOperandFunc(lzFrameVals, lzYieldErr) // <== emitCaptured: pathNext "WFA"

						lzAccOther, lzAcc, _ = lzAgg.Update(lzVars, lzOperandVal,
							lzAccOther[:0], lzAcc, lzVars.Ctx.ValComparer)

						lzAcc, lzAccOther = lzAccOther, lzAcc
					}
					if lzStepErr != nil {
						lzYieldErr(lzStepErr)
					}

					lzResVal, _, lzResBuf = lzAgg.Result(lzVars, lzAcc, lzResBuf[:0])

					lzAccA, lzAccB = lzAcc, lzAccOther
				}

				// RATIO_TO_REPORT: divide the CURRENT row's operand by the frame SUM
				// (lzResVal). ParseFloat64 reads lzResVal before WindowRatioValue
				// overwrites lzResBuf, so the alias is safe.
				if isRatioToReport {
					lzCurrVal := lzOperandFunc(lzVals, lzYieldErr) // <== emitCaptured: pathNext "WFA"
					lzResVal, lzResBuf = base.WindowRatioValue(lzCurrVal, lzResVal, lzResBuf[:0])
				}

				lzVals = append(lzVals, lzResVal)
			} // !lz

			lzYieldValsOrig(lzVals)
		}
	}

	ExecOp(o.Children[0], lzVars, lzYieldVals, lzYieldErr, pathNext, "WFC") // !lz
}
