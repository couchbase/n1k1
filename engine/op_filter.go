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
	"github.com/couchbase/n1k1/base"
)

func OpFilter(o *base.Op, lzVars *base.Vars, lzYieldVals base.YieldVals,
	lzYieldErr base.YieldErr, path, pathNext string) {
	if LzScope {
		pathNextF := EmitPush(pathNext, "F") // !lz

		exprFunc :=
			MakeExprFunc(lzVars, o.Children[0].Labels, o.Params, pathNextF, "FF") // !lz

		// Stats counters are genCompiler:hide'd -> interpreter-only for now; the
		// compiled (intermed) path collects no stats yet. See DESIGN-stats.md
		// "KNOWN LIMITATION -- compiled path currently has NO stats (TODO)".
		lzStatRowsIn := 0          // stats: rows examined. // <== genCompiler:hide
		lzStatRowsOut := 0         // stats: rows passing the predicate. // <== genCompiler:hide
		lzStatsBase := o.StatsBase // stats: baked as a literal in the compiled path. // <== genCompiler:hide

		lzYieldValsOrig := lzYieldVals

		lzYieldVals = func(lzVals base.Vals) {
			lzStatRowsIn++ // stats: local counter, flushed when the scan completes. // <== genCompiler:hide

			var lzVal base.Val

			lzVal = exprFunc(lzVals, lzYieldErr) // <== emitCaptured: pathNextF "FF"

			if base.ValTruthy(lzVal) {
				lzStatRowsOut++ // stats // <== genCompiler:hide

				lzYieldValsOrig(lzVals) // <== emitCaptured: path ""
			}
		}

		EmitPop(pathNext, "F") // !lz

		ExecOp(o.Children[0], lzVars, lzYieldVals, lzYieldErr, pathNextF, "") // !lz

		// stats: flush final counts once the child has drained.
		if lzVars != nil && lzVars.Ctx != nil && lzVars.Ctx.Stats != nil {
			lzVars.Ctx.Stats.Counters[lzStatsBase+StatFilterRowsIn] = int64(lzStatRowsIn) // <== genCompiler:hide
			lzVars.Ctx.Stats.Counters[lzStatsBase+StatFilterRowsOut] = int64(lzStatRowsOut) // <== genCompiler:hide
		}
	}
}
