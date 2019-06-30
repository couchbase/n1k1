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
	"github.com/couchbase/n1k1/base"
)

// OpTempCapture implements a "temp table" by running the child op and
// by appending any yielded vals as a sequence of entries associated
// with a vars.Temps slot. See the associated OpTempYield().
func OpTempCapture(o *base.Op, lzVars *base.Vars, lzYieldVals base.YieldVals,
	lzYieldErr base.YieldErr, path, pathNext string) {
	// A heap data structure is allocated but is used without keeping
	// the heap invariant -- we're not using the container/heap's
	// Push(). Instead, we're using the data structure as an
	// appendable sequence of []byte entries.
	tempIdx := o.Params[0].(int)

	lzHeap, lzErr := lzVars.Ctx.AllocHeap()
	if lzErr != nil {
		lzYieldErr(lzErr)
	} else {
		var lzBytes []byte

		lzYieldVals := func(lzVals base.Vals) {
			lzErr = lzHeap.PushBytes(base.ValsEncode(lzVals, lzBytes[:0]))
			if lzErr != nil {
				lzYieldErr(lzErr)
			}
		}

		ExecOp(o.Children[0], lzVars, lzYieldVals, lzYieldErr, pathNext, "TC") // !lz

		if lzErr == nil {
			lzVars.TempSet(tempIdx, lzHeap)
		}
	}
}

// -----------------------------------------------------

// OpTempYield yields vals previously captured by OpTempCapture.
func OpTempYield(o *base.Op, lzVars *base.Vars, lzYieldVals base.YieldVals,
	lzYieldErr base.YieldErr, path, pathNext string) {
	tempIdx := o.Params[0].(int)

	var lzErr error

	lzHeap := lzVars.TempGetHeap(tempIdx)
	if lzHeap != nil {
		var lzBytes []byte
		var lzVals base.Vals

		for lzI := int64(0); lzI < lzHeap.CurItems && lzErr == nil; lzI++ {
			lzBytes, lzErr = lzHeap.Get(lzI)
			if lzErr != nil {
				lzYieldErr(lzErr)
			} else {
				lzYieldVals(base.ValsDecode(lzBytes, lzVals[:0]))
			}
		}
	}

	lzYieldErr(lzErr)
}

// -----------------------------------------------------

// OpTempYieldVar yields an item or an array's items from a vars slot,
// and is useful for passing data "out of band" amongst operators.
// For example, a parent op might update a vars slot every time
// through a loop, and on each loop invoke ExecOp() on a child op tree
// which is driven by the latest data from a OpTempYieldVar().
func OpTempYieldVar(o *base.Op, lzVars *base.Vars, lzYieldVals base.YieldVals,
	lzYieldErr base.YieldErr, path, pathNext string) {
	tempIdx := o.Params[0].(int)

	lzVal := lzVars.Temps[tempIdx].(base.Val)

	_, lzOk := base.ArrayYield(lzVal, lzYieldVals, nil)
	if !lzOk {
		lzVals := base.Vals{lzVal}

		lzYieldVals(lzVals)
	}

	lzYieldErr(nil)
}
