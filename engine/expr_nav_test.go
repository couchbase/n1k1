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
func TestExprElement(t *testing.T) {
	tests := []struct {
		name string
		tree []interface{}
		want string
	}{
		{"first", []interface{}{"element", j("[10,20,30]"), j("0")}, "10"},
		{"last-positive", []interface{}{"element", j("[10,20,30]"), j("2")}, "30"},
		{"negative-last", []interface{}{"element", j("[10,20,30]"), j("-1")}, "30"},
		{"negative-mid", []interface{}{"element", j("[10,20,30]"), j("-2")}, "20"},
		{"negative-oob", []interface{}{"element", j("[10,20,30]"), j("-4")}, ""},
		{"oob", []interface{}{"element", j("[10,20,30]"), j("5")}, ""},
		{"string-elem-requoted", []interface{}{"element", j(`["a","b"]`), j("1")}, `"b"`},
		{"null-elem", []interface{}{"element", j("[null,2]"), j("0")}, "null"},
		{"nested-elem", []interface{}{"element", j(`[[1,2],{"x":3}]`), j("1")}, `{"x":3}`},
		{"integral-float-index", []interface{}{"element", j("[10,20,30]"), j("1.0")}, "20"},
		{"fractional-index-null", []interface{}{"element", j("[10,20,30]"), j("1.5")}, "null"},
		{"nonnumber-index-null", []interface{}{"element", j("[10,20,30]"), j(`"x"`)}, "null"},
		{"missing-index", []interface{}{"element", j("[10,20,30]"), j("")}, ""},
		{"missing-arr", []interface{}{"element", j(""), j("0")}, ""},
		{"nonarray-arr", []interface{}{"element", j("5"), j("0")}, ""},
		{"null-arr", []interface{}{"element", j("null"), j("0")}, ""},
	}
	for _, tc := range tests {
		if got := evalArith(t, tc.tree); got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.name, got, tc.want)
		}
	}
}
