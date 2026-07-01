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
func in(x, arr []interface{}) []interface{} {
	return []interface{}{"in", x, arr}
}

func TestExprIn(t *testing.T) {
	tests := []struct {
		name string
		tree []interface{}
		want string
	}{
		{"member", in(j("2"), j("[1,2,3]")), "true"},
		{"not-member", in(j("5"), j("[1,2,3]")), "false"},
		{"empty", in(j("2"), j("[]")), "false"},
		{"int-eq-float", in(j("2"), j("[1.0,2.0]")), "true"}, // 2 == 2.0
		{"string-member", in(j(`"b"`), j(`["a","b"]`)), "true"},
		{"string-not", in(j(`"z"`), j(`["a","b"]`)), "false"},
		// Non-array second operand -> NULL.
		{"not-array", in(j("2"), j("5")), "null"},
		{"arr-null-lit", in(j("2"), j("null")), "null"},
		// MISSING dominant.
		{"x-missing", in(j(""), j("[1,2,3]")), ""},
		{"arr-missing", in(j("2"), j("")), ""},
		// x is NULL: empty -> false; non-empty -> null.
		{"null-x-empty", in(j("null"), j("[]")), "false"},
		{"null-x-nonempty", in(j("null"), j("[1,2]")), "null"},
		// no match, array contains null -> NULL; match still wins over null.
		{"nomatch-with-null", in(j("5"), j("[1,null,3]")), "null"},
		{"match-with-null", in(j("3"), j("[1,null,3]")), "true"},
		{"nomatch-no-null", in(j("5"), j("[1,2,3]")), "false"},
	}
	for _, tc := range tests {
		if got := evalArith(t, tc.tree); got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.name, got, tc.want)
		}
	}
}
