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

import "testing"

// TestArrayAggEmptyIsNull guards ARRAY_AGG's empty-group semantics: a group with
// no non-MISSING values aggregates to NULL, not [] (matches cbq -- aggregate over
// an empty set is NULL). Regression for the gsi array_functions cases where
// array_agg of an all-MISSING field fed array_append(NULL,...) = NULL. The fix
// lives in AggArrayAgg / AggArrayAggDistinct Result (agg.go); this is the fast
// unit-level guard for it.
func TestArrayAggEmptyIsNull(t *testing.T) {
	vc := &ValComparer{}

	for _, agg := range []*Agg{AggArrayAgg, AggArrayAggDistinct} {
		// Empty group (only a MISSING update, which is ignored) -> NULL.
		st := agg.Init(nil, nil)
		st, _, _ = agg.Update(nil, ValMissing, nil, st, vc)
		if v, _, _ := agg.Result(nil, st, nil); string(v) != "null" {
			t.Errorf("empty ARRAY_AGG = %q, want null", string(v))
		}

		// A real value still aggregates to a one-element array.
		st = agg.Init(nil, nil)
		st, _, _ = agg.Update(nil, Val(`"x"`), nil, st, vc)
		if v, _, _ := agg.Result(nil, st, nil); string(v) != `["x"]` {
			t.Errorf("ARRAY_AGG([\"x\"]) = %q, want [\"x\"]", string(v))
		}
	}
}
