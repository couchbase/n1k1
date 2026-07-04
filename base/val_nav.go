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

package base

import (
	"math"

	"github.com/buger/jsonparser"
)

// ValElement implements array element navigation `arr[idx]`, mirroring cbq
// expression/nav_element.go: if idx is MISSING -> MISSING; if idx is a
// non-integral / non-number -> MISSING when arr is MISSING else NULL; if idx is
// an integer -> arr's element at that position (negative counts from the end),
// which is MISSING when arr isn't an array or the index is out of range. String
// elements are re-quoted into buf (jsonparser unquotes them); other elements are
// returned as raw slices into arr (zero-copy). Returns the result and the
// (possibly-grown) buf.
//
// TODO(perf): like ValIn, the jsonparser.ArrayEach walk allocates one iteration
// closure per call (two for a negative index -- one to count). A cached path key
// "[i]" for a constant non-negative index would let the labelPath machinery
// handle it allocation-free.
func ValElement(arr, idxVal Val, buf []byte) (Val, []byte) {
	if ValKind(idxVal) == ValKindMissing {
		return ValMissing, buf
	}

	n, ok := ParseNum(idxVal)
	if !ok || n.f64() != math.Trunc(n.f64()) { // non-number or non-integral index
		if ValKind(arr) == ValKindMissing {
			return ValMissing, buf
		}
		return ValNull, buf
	}
	idx := int(n.f64())

	// Non-array (incl. MISSING/NULL) source -> MISSING (cbq Index() semantics).
	if _, t := Parse(arr); ParseTypeToValType[t] != ValTypeArray {
		return ValMissing, buf
	}

	if idx < 0 { // negative counts from the end; needs the length first
		count := 0
		jsonparser.ArrayEach(arr, func([]byte, jsonparser.ValueType, int, error) {
			count++
		})
		idx = count + idx
		if idx < 0 {
			return ValMissing, buf
		}
	}

	result := ValMissing
	i := 0
	jsonparser.ArrayEach(arr, func(v []byte, dt jsonparser.ValueType, offset int, err error) {
		if i == idx {
			switch dt {
			case jsonparser.String:
				buf = append(append(append(buf[:0], '"'), v...), '"')
				result = Val(buf)
			case jsonparser.Null:
				result = ValNull
			default:
				result = Val(v) // raw slice into arr (zero-copy)
			}
		}
		i++
	})

	return result, buf
}
