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
	"strconv"

	"github.com/couchbase/n1k1/base"
)

// ExecOp recursively executes a base.Op tree or plan.
func ExecOp(o *base.Op, lzVars *base.Vars, lzYieldVals base.YieldVals,
	lzYieldErr base.YieldErr, path, pathItem string) {
	if o == nil {
		return
	}

	pathNext := EmitPush(path, pathItem)

	switch o.Kind {
	case "scan":
		OpScan(o, lzVars, lzYieldVals, lzYieldErr) // !lz

	case "filter":
		OpFilter(o, lzVars, lzYieldVals, lzYieldErr, path, pathNext) // !lz

	case "project":
		OpProject(o, lzVars, lzYieldVals, lzYieldErr, path, pathNext) // !lz

	case "order-offset-limit":
		OpOrderOffsetLimit(o, lzVars, lzYieldVals, lzYieldErr, path, pathNext) // !lz

	case "joinNL-inner", "joinNL-leftOuter", "joinKeys-inner", "joinKeys-leftOuter",
		"nestNL-inner", "nestNL-leftOuter", "nestKeys-inner", "nestKeys-leftOuter",
		"unnest-inner", "unnest-leftOuter":
		OpJoinNestedLoop(o, lzVars, lzYieldVals, lzYieldErr, path, pathNext) // !lz

	case "joinHash-inner", "joinHash-leftOuter",
		"intersect-distinct", "intersect-all", "except-distinct", "except-all":
		OpJoinHash(o, lzVars, lzYieldVals, lzYieldErr, path, pathNext) // !lz

	case "union-all":
		OpUnionAll(o, lzVars, lzYieldVals, lzYieldErr, path, pathNext) // !lz

	case "group", "distinct":
		OpGroup(o, lzVars, lzYieldVals, lzYieldErr, path, pathNext) // !lz

	case "sequence":
		OpSequence(o, lzVars, lzYieldVals, lzYieldErr, path, pathNext) // !lz

	case "temp-capture":
		OpTempCapture(o, lzVars, lzYieldVals, lzYieldErr, path, pathNext) // !lz

	case "temp-yield":
		OpTempYield(o, lzVars, lzYieldVals, lzYieldErr, path, pathNext) // !lz

	case "temp-yield-var":
		OpTempYieldVar(o, lzVars, lzYieldVals, lzYieldErr, path, pathNext) // !lz

	case "window-partition":
		OpWindowPartition(o, lzVars, lzYieldVals, lzYieldErr, path, pathNext) // !lz

	case "window-frames":
		OpWindowFrames(o, lzVars, lzYieldVals, lzYieldErr, path, pathNext) // !lz

	case "nil":
		lzYieldVals(nil)
		lzYieldErr(nil)

	default:
		ExecOpEx(o, lzVars, lzYieldVals, lzYieldErr, path, pathNext) // !lz
	}

	EmitPop(path, pathItem)
}

// -----------------------------------------------------

// ExecOpEx is overridable by apps and is invoked by ExecOp() to
// handle additional or extra operator kinds.
var ExecOpEx = func(o *base.Op, lzVars *base.Vars, lzYieldVals base.YieldVals,
	lzYieldErr base.YieldErr, path, pathItem string) {
}

// -----------------------------------------------------

// OpSequence executes its children in sequence.
func OpSequence(o *base.Op, lzVars *base.Vars, lzYieldVals base.YieldVals,
	lzYieldErr base.YieldErr, path, pathNext string) {
	var lzErr error

	lzYieldErrOrig := lzYieldErr

	lzYieldErr = func(lzErrIn error) {
		if lzErr == nil {
			lzErr = lzErrIn // Capture the incoming error.
		}
	}

	for childi, child := range o.Children {
		if lzErr == nil {
			ExecOp(child, lzVars, lzYieldVals, lzYieldErr, pathNext, strconv.Itoa(childi)) // !lz
		}
	}

	lzYieldErrOrig(lzErr)
}

// -----------------------------------------------------

// LzScope is used to mark block scope (ex: IF block) as lazy.
const LzScope = true

// -----------------------------------------------------

// Marks the start of a nested "emit capture" area.
var EmitPush = func(path, pathItem string) string {
	return path + "_" + pathItem // Placeholder for compiler.
}

// Marks the end of a nested "emit capture" area.
var EmitPop = func(path, pathItem string) {} // Placeholder for compiler.
