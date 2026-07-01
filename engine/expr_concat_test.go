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
func TestExprConcat(t *testing.T) {
	tests := []struct {
		name string
		tree []interface{}
		want string
	}{
		{"two", []interface{}{"concat", j(`"a"`), j(`"b"`)}, `"ab"`},
		{"three", []interface{}{"concat", j(`"a"`), j(`"b"`), j(`"c"`)}, `"abc"`},
		{"empty-strings", []interface{}{"concat", j(`""`), j(`"x"`)}, `"x"`},
		{"one", []interface{}{"concat", j(`"solo"`)}, `"solo"`},
		{"with-escape", []interface{}{"concat", j(`"a\nb"`), j(`"c"`)}, `"a\nbc"`},
		// MISSING dominant.
		{"missing", []interface{}{"concat", j(`"a"`), j(""), j(`"b"`)}, ""},
		// Non-string operand -> NULL (but MISSING still dominates).
		{"num-null", []interface{}{"concat", j(`"a"`), j("5")}, "null"},
		{"null-null", []interface{}{"concat", j(`"a"`), j("null")}, "null"},
		{"num-then-missing", []interface{}{"concat", j("5"), j("")}, ""}, // MISSING beats non-string
	}
	for _, tc := range tests {
		if got := evalArith(t, tc.tree); got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.name, got, tc.want)
		}
	}
}
