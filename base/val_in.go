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
// On early exit: jsonparser.ArrayEach has no stop/early-exit API (its callback
// returns nothing), so we scan the whole array. That's only wasteful in the
// match case -- and even then only for large lists -- because a *no-match* must
// scan fully anyway to detect a NULL element (which makes the result NULL, not
// FALSE). cbq itself returns TRUE on the first match but likewise scans fully on
// no-match.
//
// TODO(perf): the real win (and it yields early-exit for free) is to precompute
// a *constant* array operand once at setup -- parse its elements into a lifted
// slice, then per-eval do a plain loop with `break` on match. That removes both
// the per-eval re-scan and the one iteration-closure alloc ArrayEach forces
// here, mirroring cbq's static-list hash table. Dynamic arrays keep this path.
// (A hand-rolled early-exit ArrayEach was considered and rejected: jsonparser
// exposes no public element-cursor -- getType is unexported -- so it would mean
// re-implementing JSON scanning, exactly the fragile thing we avoid.)
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
