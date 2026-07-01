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

	"github.com/couchbase/n1k1/base"
)

// j builds a "json" constant operand tree. An empty string yields MISSING (a
// zero-length Val); "null" yields NULL; other strings are the JSON literal.
func j(s string) []interface{} { return []interface{}{"json", s} }

// evalArith builds the expression tree via the ExprCatalog and evaluates it with
// no input row. The result is returned as a string; MISSING is "" (empty).
func evalArith(t *testing.T, tree []interface{}) string {
	t.Helper()
	vars := &base.Vars{Ctx: &base.Ctx{
		ExprCatalog: ExprCatalog,
		ValComparer: base.NewValComparer(),
	}}
	var gotErr error
	fn := MakeExprFunc(vars, nil, tree, "", "")
	out := fn(nil, func(e error) {
		if e != nil {
			gotErr = e
		}
	})
	if gotErr != nil {
		t.Fatalf("tree %v yielded err: %v", tree, gotErr)
	}
	return string(out)
}

func TestExprArithValues(t *testing.T) {
	tests := []struct {
		name string
		tree []interface{}
		want string
	}{
		{"add-int", []interface{}{"add", j("2"), j("3")}, "5"},
		{"add-float", []interface{}{"add", j("2"), j("3.5")}, "5.5"},
		{"sub-int", []interface{}{"sub", j("5"), j("3")}, "2"},
		{"mult-int", []interface{}{"mult", j("3"), j("4")}, "12"},
		{"div-frac", []interface{}{"div", j("1"), j("2")}, "0.5"},
		{"div-whole", []interface{}{"div", j("4"), j("2")}, "2"},
		{"mod", []interface{}{"mod", j("5"), j("3")}, "2"},
		{"idiv", []interface{}{"idiv", j("7"), j("2")}, "3"},
		{"imod", []interface{}{"imod", j("7"), j("3")}, "1"},
		{"neg", []interface{}{"neg", j("5")}, "-5"},
		{"neg-neg", []interface{}{"neg", j("-5")}, "5"},
		// Nested: (2 + 3) * 4 = 20
		{"nested", []interface{}{"mult", []interface{}{"add", j("2"), j("3")}, j("4")}, "20"},
	}
	for _, tc := range tests {
		if got := evalArith(t, tc.tree); got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.name, got, tc.want)
		}
	}
}

// TestExprArithPropagation checks the N1QL three-valued logic: MISSING dominates,
// then any non-number operand yields NULL, and divide/mod-by-zero yields NULL.
func TestExprArithPropagation(t *testing.T) {
	miss := j("")     // MISSING
	null := j("null") // NULL
	str := j(`"abc"`) // non-number
	bln := j("true")  // non-number
	num := j("3")

	tests := []struct {
		name string
		tree []interface{}
		want string // "" means MISSING
	}{
		{"add-missing-left", []interface{}{"add", miss, num}, ""},
		{"add-missing-right", []interface{}{"add", num, miss}, ""},
		{"add-null", []interface{}{"add", null, num}, "null"},
		{"add-missing-dominates-null", []interface{}{"add", null, miss}, ""},
		{"add-string", []interface{}{"add", str, num}, "null"},
		{"add-bool", []interface{}{"add", bln, num}, "null"},
		{"div-by-zero", []interface{}{"div", num, j("0")}, "null"},
		{"mod-by-zero", []interface{}{"mod", num, j("0")}, "null"},
		{"idiv-by-zero", []interface{}{"idiv", num, j("0")}, "null"},
		{"idiv-by-frac-zero", []interface{}{"idiv", num, j("0.4")}, "null"}, // Int64()==0
		{"sub-missing", []interface{}{"sub", miss, num}, ""},
		{"mult-null", []interface{}{"mult", null, num}, "null"},
		{"neg-missing", []interface{}{"neg", miss}, ""},
		{"neg-null", []interface{}{"neg", null}, "null"},
		{"neg-string", []interface{}{"neg", str}, "null"},
	}
	for _, tc := range tests {
		if got := evalArith(t, tc.tree); got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.name, got, tc.want)
		}
	}
}
