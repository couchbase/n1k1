package n1k1

import (
	"strconv"

	"github.com/couchbase/n1k1/base"
)

func init() {
	ExprCatalog["window-frame-count"] = ExprWindowFrameCount
	ExprCatalog["window-frame-first-value"] = ExprWindowFrameFirstValue
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

func ExprWindowFrameCount(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	windowFramesSlot, windowFrameIdx := params[0].(int), params[1].(int)

	var lzBufPre []byte // <== varLift: lzBufPre by path

	lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
		lzWindowFrames := lzVars.Temps[windowFramesSlot].([]base.WindowFrame)
		lzWindowFrame := &lzWindowFrames[windowFrameIdx]

		c := lzWindowFrame.Count()

		lzBuf := strconv.AppendUint(lzBufPre[:0], uint64(c), 10)

		lzVal = base.Val(lzBuf)

		lzBufPre = lzBuf

		return lzVal
	}

	return lzExprFunc
}

// -----------------------------------------------------

func ExprWindowFrameFirstValue(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	windowFramesSlot, windowFrameIdx := params[0].(int), params[1].(int)

	expr := params[2].([]interface{})

	if LzScope {
		lzExprFunc =
			MakeExprFunc(lzVars, labels, expr, path, "E") // !lz
		lzExprFirstValue := lzExprFunc

		var lzValsPre base.Vals // <== varLift: lzValsPre by path

		lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
			lzWindowFrames := lzVars.Temps[windowFramesSlot].([]base.WindowFrame)
			lzWindowFrame := &lzWindowFrames[windowFrameIdx]

			lzValsStep := lzValsPre[:0]

			lzVals, _, lzOk, lzErr := lzWindowFrame.StepVals(true, int64(-1), lzValsStep)
			if lzOk && lzErr == nil {
				lzValsPre = lzVals

				lzVal = lzExprFirstValue(lzVals, lzYieldErr) // <== emitCaptured: path "E"
			}

			return lzVal
		}
	}

	return lzExprFunc
}
