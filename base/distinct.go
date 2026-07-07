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

// ValDistinctFrom implements IS DISTINCT FROM (null-safe inequality): two operands
// are distinct unless they are the same unknown kind (both MISSING or both NULL) or
// both real values that compare equal. MISSING and NULL are distinct from each other
// and from any value. Always returns ValTrue/ValFalse (never MISSING/NULL). Mirrors
// cbq expression/func_distinct.go.
func ValDistinctFrom(vc *ValComparer, a, b Val) Val {
	ka, kb := ValKind(a), ValKind(b)

	var distinct bool
	if ka != ValKindValue || kb != ValKindValue {
		// At least one is NULL/MISSING: distinct unless the same unknown kind
		// (both MISSING, or both NULL).
		distinct = ka != kb
	} else {
		distinct = vc.Compare(a, b) != 0
	}

	if distinct {
		return ValTrue
	}
	return ValFalse
}

// ValNotDistinctFrom implements IS NOT DISTINCT FROM (null-safe equality) -- the
// negation of ValDistinctFrom.
func ValNotDistinctFrom(vc *ValComparer, a, b Val) Val {
	if ValEqualTrue(ValDistinctFrom(vc, a, b)) {
		return ValFalse
	}
	return ValTrue
}
