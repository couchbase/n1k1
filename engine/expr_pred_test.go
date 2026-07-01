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
	"testing"
)

// Uses the j() / evalArith() helpers from expr_arith_test.go (same package).
// Result "" means MISSING; "true"/"false"/"null" are the JSON literals.

func TestExprIsPredicates(t *testing.T) {
	miss := j("")     // MISSING
	null := j("null") // NULL
	num := j("3")     // a value
	str := j(`"x"`)   // a value

	tests := []struct {
		name string
		tree []interface{}
		want string
	}{
		// IS NULL: NULL->true, MISSING->MISSING, value->false
		{"isnull-null", []interface{}{"is_null", null}, "true"},
		{"isnull-missing", []interface{}{"is_null", miss}, ""},
		{"isnull-value", []interface{}{"is_null", num}, "false"},
		// IS NOT NULL: NULL->false, MISSING->MISSING, value->true
		{"isnotnull-null", []interface{}{"is_not_null", null}, "false"},
		{"isnotnull-missing", []interface{}{"is_not_null", miss}, ""},
		{"isnotnull-value", []interface{}{"is_not_null", str}, "true"},
		// IS MISSING: MISSING->true, else false
		{"ismissing-missing", []interface{}{"is_missing", miss}, "true"},
		{"ismissing-null", []interface{}{"is_missing", null}, "false"},
		{"ismissing-value", []interface{}{"is_missing", num}, "false"},
		// IS NOT MISSING: MISSING->false, else true
		{"isnotmissing-missing", []interface{}{"is_not_missing", miss}, "false"},
		{"isnotmissing-null", []interface{}{"is_not_missing", null}, "true"},
		// IS VALUED: NULL/MISSING->false, else true
		{"isvalued-value", []interface{}{"is_valued", num}, "true"},
		{"isvalued-null", []interface{}{"is_valued", null}, "false"},
		{"isvalued-missing", []interface{}{"is_valued", miss}, "false"},
		// IS NOT VALUED: NULL/MISSING->true, else false
		{"isnotvalued-value", []interface{}{"is_not_valued", num}, "false"},
		{"isnotvalued-null", []interface{}{"is_not_valued", null}, "true"},
		{"isnotvalued-missing", []interface{}{"is_not_valued", miss}, "true"},
	}
	for _, tc := range tests {
		if got := evalArith(t, tc.tree); got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.name, got, tc.want)
		}
	}
}

func TestExprNot(t *testing.T) {
	tests := []struct {
		name string
		tree []interface{}
		want string
	}{
		{"not-true", []interface{}{"not", j("true")}, "false"},
		{"not-false", []interface{}{"not", j("false")}, "true"},
		{"not-null", []interface{}{"not", j("null")}, "null"},     // passthrough
		{"not-missing", []interface{}{"not", j("")}, ""},          // passthrough
		{"not-num-truthy", []interface{}{"not", j("5")}, "false"}, // 5 is truthy
		{"not-zero-falsy", []interface{}{"not", j("0")}, "true"},  // 0 is falsy
	}
	for _, tc := range tests {
		if got := evalArith(t, tc.tree); got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.name, got, tc.want)
		}
	}
}
