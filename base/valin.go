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
	"github.com/buger/jsonparser"
)

// ValIn evaluates `x IN arr` on JSON bytes, mirroring cbq expression/coll_in.go:
//   - MISSING if x or arr is MISSING;
//   - NULL if arr is not an array;
//   - if x is NULL: FALSE for an empty array, else NULL;
//   - otherwise TRUE if any array element equals x (N1QL collation); if no match,
//     NULL when the array contains a NULL element, else FALSE.
//
// Element comparison reuses ValComparer.CompareWithType with the jsonparser
// element type, so strings (unquoted by jsonparser) compare consistently with x.
//
// TODO(perf): for a static array operand, precompute the elements once (cbq
// builds a hash table); today each call re-scans and allocates one iteration
// closure -- still far cheaper than cbq's per-element value.Value boxing.
func ValIn(vc *ValComparer, x, arr Val) Val {
	if ValKind(x) == ValKindMissing || ValKind(arr) == ValKindMissing {
		return ValMissing
	}

	_, arrType := Parse(arr)
	if ParseTypeToValType[arrType] != ValTypeArray {
		return ValNull
	}

	xIsNull := ValKind(x) == ValKindNull
	xVal, xType := Parse(x)

	found := false
	sawNull := false
	count := 0

	jsonparser.ArrayEach(arr, func(elem []byte, dt jsonparser.ValueType, offset int, err error) {
		count++
		if dt == jsonparser.Null {
			sawNull = true
		} else if !xIsNull && !found {
			if vc.CompareWithType(xVal, elem, xType, int(dt), 0) == 0 {
				found = true
			}
		}
	})

	if xIsNull {
		if count == 0 {
			return ValFalse
		}
		return ValNull
	}
	if found {
		return ValTrue
	}
	if sawNull {
		return ValNull
	}
	return ValFalse
}
