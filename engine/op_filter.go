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
		pathNextF := EmitPush(lzVars, pathNext, "F") // !lz

		exprFunc :=
			MakeExprFunc(lzVars, o.Children[0].Labels, o.Params, pathNextF, "FF") // !lz

		// Stats counters update live (per row) into the shared array so the CLI
		// display animates; they are genCompiler:hide'd -> interpreter-only for now
		// (the compiled path collects no stats -- see DESIGN-stats.md's KNOWN
		// LIMITATION). statZero resets at setup so a re-run filter restarts.
		lzStats := StatsFromVars(lzVars)                         // <== genCompiler:hide
		lzStatsBase := o.StatsBase                               // <== genCompiler:hide
		StatsCounterZero(lzStats, lzStatsBase+StatFilterRowsIn)  // <== genCompiler:hide
		StatsCounterZero(lzStats, lzStatsBase+StatFilterRowsOut) // <== genCompiler:hide

		lzYieldValsOrig := lzYieldVals

		lzYieldVals = func(lzVals base.Vals) {
			StatsCounterBump(lzStats, lzStatsBase+StatFilterRowsIn) // stats: live // <== genCompiler:hide

			var lzVal base.Val

			lzVal = exprFunc(lzVals, lzYieldErr) // <== emitCaptured: pathNextF "FF"

			if base.ValTruthy(lzVal) {
				StatsCounterBump(lzStats, lzStatsBase+StatFilterRowsOut) // stats: live // <== genCompiler:hide

				lzYieldValsOrig(lzVals) // <== emitCaptured: path ""
			}
		}

		EmitPop(pathNext, "F") // !lz

		ExecOp(o.Children[0], lzVars, lzYieldVals, lzYieldErr, pathNextF, "") // !lz
	}
}
