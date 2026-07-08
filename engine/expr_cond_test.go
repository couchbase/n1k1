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
// "" means MISSING.

func TestExprCondUnknown(t *testing.T) {
	miss := j("")
	null := j("null")
	num := j("5")
	num3 := j("3")

	tests := []struct {
		name string
		tree []interface{}
		want string
	}{
		// IFNULL: first non-NULL (MISSING is non-NULL, so it wins).
		{"ifnull-null-num", []interface{}{"ifnull", null, num}, "5"},
		{"ifnull-num-num", []interface{}{"ifnull", num3, num}, "3"},
		{"ifnull-missing-num", []interface{}{"ifnull", miss, num}, ""},
		{"ifnull-null-null", []interface{}{"ifnull", null, null}, "null"},
		// IFMISSING: first non-MISSING (NULL is non-MISSING, so it wins).
		{"ifmissing-missing-num", []interface{}{"ifmissing", miss, num}, "5"},
		{"ifmissing-null-num", []interface{}{"ifmissing", null, num}, "null"},
		{"ifmissing-num-num", []interface{}{"ifmissing", num3, num}, "3"},
		{"ifmissing-miss-miss", []interface{}{"ifmissing", miss, miss}, "null"},
		// IFMISSINGORNULL: first valued (two-operand native form).
		{"ifmon-miss-num", []interface{}{"ifmissingornull", miss, num}, "5"},
		{"ifmon-null-num", []interface{}{"ifmissingornull", null, num3}, "3"},
		{"ifmon-miss-null", []interface{}{"ifmissingornull", miss, null}, "null"},
		{"ifmon-num", []interface{}{"ifmissingornull", num3, num}, "3"},
		// NVL == IFNULL.
		{"nvl-null-num", []interface{}{"nvl", null, num}, "5"},
		// N-ary (3+ operands) via the eager-Vals harness (ExprCondUnknown).
		{"ifnull-3", []interface{}{"ifnull", null, null, num}, "5"},
		{"ifmissing-3", []interface{}{"ifmissing", miss, null, num}, "null"}, // null is first non-MISSING
		{"ifmon-3", []interface{}{"ifmissingornull", miss, null, num}, "5"},
		{"ifmon-4-none", []interface{}{"ifmissingornull", miss, null, null, null}, "null"},
	}
	for _, tc := range tests {
		if got := evalArith(t, tc.tree); got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.name, got, tc.want)
		}
	}
}
