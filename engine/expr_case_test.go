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

// Uses j() / evalArith() from expr_arith_test.go. "" means MISSING. CASE params
// are the flat searched form: [cond, then, cond, then, ..., else?].
func TestExprCase(t *testing.T) {
	tru := j("true")
	fls := j("false")
	a := j(`"a"`)
	b := j(`"b"`)

	tests := []struct {
		name string
		tree []interface{}
		want string
	}{
		{"first", []interface{}{"case", tru, a, fls, b}, `"a"`},
		{"second", []interface{}{"case", fls, a, tru, b}, `"b"`},
		{"else", []interface{}{"case", fls, a, j(`"e"`)}, `"e"`},
		{"no-match-no-else", []interface{}{"case", fls, a}, "null"},
		{"null-cond-skipped", []interface{}{"case", j("null"), a, tru, b}, `"b"`},
		{"missing-cond-skipped", []interface{}{"case", j(""), a, tru, b}, `"b"`},
		{"number-cond-truthy", []interface{}{"case", j("5"), a}, `"a"`},
		{"zero-cond-falsy", []interface{}{"case", j("0"), a, j(`"z"`)}, `"z"`}, // 0 falsy -> else
		{"then-missing", []interface{}{"case", tru, j(""), b}, ""},             // matched then is MISSING
		{"empty-str-cond-falsy", []interface{}{"case", j(`""`), a, j(`"z"`)}, `"z"`},
	}
	for _, tc := range tests {
		if got := evalArith(t, tc.tree); got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.name, got, tc.want)
		}
	}
}
