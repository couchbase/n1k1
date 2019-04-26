package n1k1

import (
	"github.com/couchbase/n1k1/base"
)

func OpFilter(o *base.Op, lzYieldVals base.YieldVals,
	lzYieldStats base.YieldStats, lzYieldErr base.YieldErr, path, pathNext string) {
	if LzScope {
		pathNextF := EmitPush(pathNext, "F") // !lz

		var lzExprFunc base.ExprFunc

		lzExprFunc =
			MakeExprFunc(o.ParentA.Fields, nil, o.Params, pathNextF, "FF") // !lz

		lzYieldValsOrig := lzYieldVals

		_, _ = lzExprFunc, lzYieldValsOrig

		lzYieldVals = func(lzVals base.Vals) {
			var lzVal base.Val

			lzVal = lzExprFunc(lzVals) // <== emitCaptured: pathNextF "FF"

			if base.ValEqualTrue(lzVal) {
				lzYieldValsOrig(lzVals) // <== emitCaptured: path ""
			}
		}

		EmitPop(pathNext, "F") // !lz

		ExecOp(o.ParentA, lzYieldVals, lzYieldStats, lzYieldErr, pathNextF, "") // !lz
	}
}
