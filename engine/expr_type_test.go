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
func TestExprIsType(t *testing.T) {
	tests := []struct {
		name string
		tree []interface{}
		want string
	}{
		{"array-yes", []interface{}{"is_array", j("[1,2]")}, "true"},
		{"array-no", []interface{}{"is_array", j("5")}, "false"},
		{"number-yes", []interface{}{"is_number", j("5")}, "true"},
		{"number-yes-float", []interface{}{"is_number", j("5.5")}, "true"},
		{"number-no", []interface{}{"is_number", j(`"x"`)}, "false"},
		{"string-yes", []interface{}{"is_string", j(`"x"`)}, "true"},
		{"string-no", []interface{}{"is_string", j("5")}, "false"},
		{"boolean-yes", []interface{}{"is_boolean", j("true")}, "true"},
		{"boolean-no", []interface{}{"is_boolean", j("5")}, "false"},
		{"object-yes", []interface{}{"is_object", j(`{"a":1}`)}, "true"},
		{"object-no", []interface{}{"is_object", j("[1]")}, "false"},
		// IS_ATOM: bool/number/string true; array/object false.
		{"atom-number", []interface{}{"is_atom", j("5")}, "true"},
		{"atom-string", []interface{}{"is_atom", j(`"x"`)}, "true"},
		{"atom-boolean", []interface{}{"is_atom", j("false")}, "true"},
		{"atom-array", []interface{}{"is_atom", j("[1]")}, "false"},
		{"atom-object", []interface{}{"is_atom", j(`{"a":1}`)}, "false"},
		// MISSING / NULL pass through for every type check.
		{"array-missing", []interface{}{"is_array", j("")}, ""},
		{"number-null", []interface{}{"is_number", j("null")}, "null"},
		{"atom-missing", []interface{}{"is_atom", j("")}, ""},
		{"atom-null", []interface{}{"is_atom", j("null")}, "null"},
	}
	for _, tc := range tests {
		if got := evalArith(t, tc.tree); got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.name, got, tc.want)
		}
	}
}
