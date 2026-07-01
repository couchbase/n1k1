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

// Uses j() / evalArith() from expr_arith_test.go. "" means MISSING.
func between(item, low, high []interface{}) []interface{} {
	return []interface{}{"between", item, low, high}
}

func TestExprBetween(t *testing.T) {
	miss := j("")
	null := j("null")

	tests := []struct {
		name string
		tree []interface{}
		want string
	}{
		{"in-range", between(j("3"), j("1"), j("5")), "true"},
		{"below", between(j("0"), j("1"), j("5")), "false"},
		{"above", between(j("9"), j("1"), j("5")), "false"},
		{"eq-low", between(j("1"), j("1"), j("5")), "true"},  // >= low
		{"eq-high", between(j("5"), j("1"), j("5")), "true"}, // <= high
		{"float", between(j("2.5"), j("2"), j("3")), "true"},
		{"string-collation", between(j(`"b"`), j(`"a"`), j(`"c"`)), "true"},
		// MISSING dominates: any MISSING operand -> MISSING.
		{"item-missing", between(miss, j("1"), j("5")), ""},
		{"low-missing", between(j("3"), miss, j("5")), ""},
		{"high-missing", between(j("3"), j("1"), miss), ""},
		// NULL (no MISSING) -> NULL.
		{"item-null", between(null, j("1"), j("5")), "null"},
		{"low-null", between(j("3"), null, j("5")), "null"},
		{"high-null", between(j("3"), j("1"), null), "null"},
	}
	for _, tc := range tests {
		if got := evalArith(t, tc.tree); got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.name, got, tc.want)
		}
	}
}
