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
	"math"
	"strconv"

	"github.com/couchbase/n1k1/base"
)

func init() {
	ExprCatalog["window-partition-row-number"] = ExprWindowPartitionRowNumber
	ExprCatalog["window-frame-count"] = ExprWindowFrameCount
	ExprCatalog["window-frame-step-value"] = ExprWindowFrameStepValue
}

/*
NOTES on window functions...

navigation functions
  Computes an expr over a different row in the window frame than the current row.
  Usage rules...
                Partition  Order By     Window        Scope
                                        Frame Clause
  pctile-cont   optional   disallowed   disallowed    window-frame
  pctile-disc   optional   disallowed   disallowed    window-frame
  first-value   optional   required     optional      window-frame's first row
  last-value    optional   required     optional      window-frame's last row
  nth-value     optional   required     optional      window-frame's nth row
  lead          optional   required     disallowed    window-frame row after current-row
  lag           optional   required     disallowed    window-frame row before current-row

numbering functions
  Assigns integer to each row based on their position within the window-partition.
  Usage rules...
                Partition  Order By     Window        Scope
                                        Frame Clause
  row-number    optional   optional     disallowed    window-partition

  rank          optional   required     disallowed    window-partition
  dense-rank    optional   required     disallowed    window-partition
  percent-rank  optional   required     disallowed    window-partition
  cume-dist     optional   required     disallowed    window-partition
  ntile         optional   required     disallowed    window-partition

aggregate functions
  Computes aggregate over the relevant window frame for each row.
  Usage rules...
                Partition  Order By     Window        Scope
                                        Frame Clause
  count         optional   optional'    optional'     window-frame
  sum           optional   optional'    optional'     window-frame
  min           optional   optional'    optional'     window-frame
  max           optional   optional'    optional'     window-frame
  avg           optional   optional'    optional'     window-frame

  optional' -- disallowed if DISTINCT is present.
*/

// -----------------------------------------------------

func ExprWindowPartitionRowNumber(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	framesSlot, frameIdx := params[0].(int), params[1].(int)

	var lzBufPre []byte // <== varLift: lzBufPre by path

	lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
		lzFrames := lzVars.Temps[framesSlot].([]base.WindowFrame)
		lzFrame := &lzFrames[frameIdx]

		lzBuf := strconv.AppendUint(lzBufPre[:0], uint64(lzFrame.Pos+1), 10)

		lzVal = base.Val(lzBuf)

		lzBufPre = lzBuf

		return lzVal
	}

	return lzExprFunc
}

// -----------------------------------------------------

func ExprWindowFrameCount(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	framesSlot, frameIdx := params[0].(int), params[1].(int)

	var lzBufPre []byte // <== varLift: lzBufPre by path

	lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
		lzFrames := lzVars.Temps[framesSlot].([]base.WindowFrame)
		lzFrame := &lzFrames[frameIdx]

		c := lzFrame.Count()

		lzBuf := strconv.AppendUint(lzBufPre[:0], uint64(c), 10)

		lzVal = base.Val(lzBuf)

		lzBufPre = lzBuf

		return lzVal
	}

	return lzExprFunc
}

// -----------------------------------------------------

// ExprWindowFrameStepValue implements the navigation window
// functions of FIRST_VALUE, LAST_VALUE, NTH_VALUE, LEAD and LAG for
// window partitions of type ROWS.
func ExprWindowFrameStepValue(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	framesSlot, frameIdx := params[0].(int), params[1].(int)

	// The initial config defines the starting step position, with
	// allowed values of...
	// == -1. Ex: a related config of an ascending "num" of 1 means a
	// single step will be taken, which results in the FIRST_VALUE
	// ==  0. This means the starting step position is the current
	// position, which is used for row-based LEAD & LAG.
	// ==  1. This means the starting step position is MaxInt64,
	// with a related descending "num" of 1 means the LAST_VALUE.
	initial := params[2].(int)

	// The asc defines whether the step should ascend or descend.
	asc := params[3].(bool)

	// The num defines the number of steps to take.
	num := params[4].(uint64)

	// The expr to evaluate at the final position.
	expr := params[5].([]interface{})

	if LzScope {
		lzExprFunc =
			MakeExprFunc(lzVars, labels, expr, path, "E") // !lz
		lzExprValFunc := lzExprFunc

		var lzValsPre base.Vals // <== varLift: lzValsPre by path

		lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
			lzFrames := lzVars.Temps[framesSlot].([]base.WindowFrame)
			lzFrame := &lzFrames[frameIdx]

			lzOk := true

			var lzErr error

			lzPos := int64(-1)
			if initial == 0 { // !lz
				lzPos = lzFrame.Pos
			} else if initial == 1 { // !lz
				lzPos = math.MaxInt64
			} // !lz

			for lzI := uint64(0); lzI < num && lzOk && lzErr == nil; lzI++ {
				lzValsStep := lzValsPre[:0]

				lzVals, lzPos, lzOk, lzErr = lzFrame.StepVals(asc, lzPos, lzValsStep)
			}

			if lzOk && lzErr == nil {
				lzValsPre = lzVals

				lzVal = lzExprValFunc(lzVals, lzYieldErr) // <== emitCaptured: path "E"
			}

			return lzVal
		}
	}

	return lzExprFunc
}
