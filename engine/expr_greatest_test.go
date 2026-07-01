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
func TestExprGreatestLeast(t *testing.T) {
	tests := []struct {
		name string
		tree []interface{}
		want string
	}{
		{"greatest-nums", []interface{}{"greatest", j("3"), j("7"), j("5")}, "7"},
		{"greatest-skip-null", []interface{}{"greatest", j("3"), j("null"), j("9")}, "9"},
		{"greatest-skip-missing", []interface{}{"greatest", j(""), j("4"), j("2")}, "4"},
		{"greatest-all-unknown", []interface{}{"greatest", j("null"), j("")}, "null"},
		{"greatest-strings", []interface{}{"greatest", j(`"a"`), j(`"c"`), j(`"b"`)}, `"c"`},
		{"greatest-mixed-collation", []interface{}{"greatest", j("5"), j(`"a"`)}, `"a"`}, // string > number
		{"least-nums", []interface{}{"least", j("3"), j("7"), j("5")}, "3"},
		{"least-skip-missing", []interface{}{"least", j(""), j("7"), j("2")}, "2"},
		{"least-mixed-collation", []interface{}{"least", j("5"), j(`"a"`)}, "5"}, // number < string
		{"least-all-unknown", []interface{}{"least", j(""), j("null")}, "null"},
	}
	for _, tc := range tests {
		if got := evalArith(t, tc.tree); got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.name, got, tc.want)
		}
	}
}
