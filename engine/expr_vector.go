//  Copyright (c) 2026 Couchbase, Inc.
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

func init() {
	ExprCatalog["vector_distance"] = ExprVectorDistance
}

// ExprVectorDistance is the native, unboxed VECTOR_DISTANCE(vecA, vecB, metric)
// (DESIGN-vectors.md): it evaluates the three operands into reused Vals and folds the
// distance on the byte lane via base.VectorDistanceVals -- NO per-row value.Value array
// boxing (the measured jsonl+boxed bottleneck: ~1000 allocs/row). Rides the eager-Vals
// N-ary harness like ExprArrayConcat; the two float scratch slices are lifted so they
// grow once and are reused across rows. Semantics match cbq's boxed vectorDistance
// bit-for-bit (see base.VectorDistanceVals), so it is a drop-in replacement.
func ExprVectorDistance(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	var lzBufOut []byte        // <== varLift: lzBufOut by path
	var lzValsReduce base.Vals // <== varLift: lzValsReduce by path

	lzChildren := NaryCaptureChildren(lzVars, labels, params, path) // !lz

	if LzScope {
		lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
			lzValsReduce = lzValsReduce[:0]

			for lzI := range lzChildren { // !lz
				lzVal =
					lzChildren[lzI](lzVals, lzYieldErr) // <== emitCaptured: path strconv.Itoa(lzI)

				lzValsReduce = append(lzValsReduce, lzVal)
			}

			lzVal, lzBufOut = base.VectorDistanceVals(lzValsReduce, lzBufOut)

			return lzVal
		}
	}

	return lzExprFunc
}
