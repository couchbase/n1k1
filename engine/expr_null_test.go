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
func TestExprNullMissingIf(t *testing.T) {
	tests := []struct {
		name string
		tree []interface{}
		want string
	}{
		// NULLIF: equal -> NULL, else first operand.
		{"nullif-eq", []interface{}{"nullif", j("5"), j("5")}, "null"},
		{"nullif-ne", []interface{}{"nullif", j("5"), j("6")}, "5"},
		{"nullif-eq-canon", []interface{}{"nullif", j("5"), j("5.0")}, "null"}, // 5 == 5.0
		{"nullif-a-missing", []interface{}{"nullif", j(""), j("5")}, ""},
		{"nullif-a-null", []interface{}{"nullif", j("null"), j("5")}, "null"},
		{"nullif-b-null", []interface{}{"nullif", j("5"), j("null")}, "null"},
		{"nullif-str-ne", []interface{}{"nullif", j(`"a"`), j(`"b"`)}, `"a"`},
		// MISSINGIF: equal -> MISSING, else first operand.
		{"missingif-eq", []interface{}{"missingif", j("5"), j("5")}, ""},
		{"missingif-ne", []interface{}{"missingif", j("5"), j("6")}, "5"},
		{"missingif-a-missing", []interface{}{"missingif", j(""), j("5")}, ""},
		{"missingif-b-null", []interface{}{"missingif", j("5"), j("null")}, "null"},
	}
	for _, tc := range tests {
		if got := evalArith(t, tc.tree); got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.name, got, tc.want)
		}
	}
}
