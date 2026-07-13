//go:build n1ql

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

package glue

// Differential test: the native n1k1 arithmetic path (ExprTreeOptimize ->
// engine.MakeExprFunc, on JSON bytes) must produce byte-identical results to
// cbq's expr.Evaluate() -- the oracle. This is what guarantees we inherited
// cbq's exact semantics (int/float, overflow, divide-by-zero, MISSING/NULL).

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/engine"

	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/expression/parser"
	"github.com/couchbase/query/value"
)

// cbqEval evaluates an expression via cbq and returns the JSON bytes, with
// MISSING normalized to "" (matching how glue's ExprTree fallback omits MISSING).
func cbqEval(t *testing.T, e expression.Expression) string {
	t.Helper()
	ctx := NewExprGlueContext(time.Now())
	r, err := e.Evaluate(value.NULL_VALUE, ctx)
	if err != nil {
		t.Fatalf("cbq Evaluate error: %v", err)
	}
	if r == nil || r.Type() == value.MISSING {
		return ""
	}
	var buf bytes.Buffer
	if err := r.WriteJSON(nil, &buf, "", "", true); err != nil {
		t.Fatalf("cbq WriteJSON error: %v", err)
	}
	return buf.String()
}

// nativeEval optimizes the cbq expression into a native n1k1 expr tree and
// evaluates it, returning the JSON bytes ("" for MISSING).
func nativeEval(t *testing.T, e expression.Expression) (string, bool) {
	t.Helper()
	var buf bytes.Buffer
	params, ok := ExprTreeOptimize(nil, e, &buf, false)
	if !ok {
		return "", false
	}
	vars := &base.Vars{Ctx: &base.Ctx{
		ExprCatalog: engine.ExprCatalog,
		ValComparer: base.NewValComparer(),
	}}
	fn := engine.MakeExprFunc(vars, nil, params, "", "")
	var gotErr error
	out := fn(nil, func(err error) {
		if err != nil {
			gotErr = err
		}
	})
	if gotErr != nil {
		t.Fatalf("native eval error: %v", gotErr)
	}
	return string(out), true
}

func TestArithDifferentialVsCBQ(t *testing.T) {
	c := func(v interface{}) expression.Expression { return expression.NewConstant(v) }

	cases := []struct {
		name string
		expr expression.Expression
	}{
		{"add-int", expression.NewAdd(c(2), c(3))},
		{"add-float", expression.NewAdd(c(2), c(3.5))},
		{"add-float-whole", expression.NewAdd(c(2.0), c(3.0))},
		{"add-neg", expression.NewAdd(c(-10), c(3))},
		// NOTE: MISSING is not a representable JSON constant (it arises from
		// missing fields, i.e. empty bytes); MISSING propagation is covered
		// directly in engine/expr_arith_test.go. Here we test the representable
		// unknown, NULL.
		{"add-null", expression.NewAdd(c(value.NULL_VALUE), c(3))},
		{"add-string", expression.NewAdd(c("abc"), c(3))},
		{"add-bool", expression.NewAdd(c(true), c(3))},
		{"add-overflow", expression.NewAdd(c(int64(9223372036854775807)), c(1))},
		{"sub-int", expression.NewSub(c(5), c(3))},
		{"sub-float", expression.NewSub(c(5), c(3.5))},
		{"mult-int", expression.NewMult(c(3), c(4))},
		{"mult-float", expression.NewMult(c(3), c(0.5))},
		{"mult-overflow", expression.NewMult(c(int64(9223372036854775807)), c(2))},
		{"mult-null", expression.NewMult(c(value.NULL_VALUE), c(4))},
		{"div-frac", expression.NewDiv(c(1), c(2))},
		{"div-whole", expression.NewDiv(c(4), c(2))},
		{"div-zero", expression.NewDiv(c(1), c(0))},
		{"mod-int", expression.NewMod(c(5), c(3))},
		{"mod-float", expression.NewMod(c(5.5), c(2))},
		{"mod-zero", expression.NewMod(c(5), c(0))},
		{"idiv-int", expression.NewIDiv(c(7), c(2))},
		{"idiv-neg", expression.NewIDiv(c(-7), c(2))},
		{"idiv-zero", expression.NewIDiv(c(7), c(0))},
		{"imod-int", expression.NewIMod(c(7), c(3))},
		{"neg-int", expression.NewNeg(c(5))},
		{"neg-neg", expression.NewNeg(c(-5))},
		{"neg-float", expression.NewNeg(c(2.5))},
		{"neg-string", expression.NewNeg(c("abc"))},
		// Nested: (a + b) * c
		{"nested", expression.NewMult(expression.NewAdd(c(2), c(3)), c(4))},
	}

	for _, tc := range cases {
		want := cbqEval(t, tc.expr)
		got, ok := nativeEval(t, tc.expr)
		if !ok {
			t.Errorf("%s: expression did not optimize to native path", tc.name)
			continue
		}
		if got != want {
			t.Errorf("%s: native=%q, cbq=%q", tc.name, got, want)
		}
	}
}

func TestCondUnknownDifferentialVsCBQ(t *testing.T) {
	c := func(v interface{}) expression.Expression { return expression.NewConstant(v) }
	num := c(3)
	num5 := c(5)
	str := c("x")
	null := c(value.NULL_VALUE)

	cases := []struct {
		name string
		expr expression.Expression
	}{
		// MISSING isn't a representable constant; covered in engine/expr_cond_test.go.
		{"ifnull-null-num", expression.NewIfNull(null, num)},
		{"ifnull-num-num", expression.NewIfNull(num, num5)},
		{"ifnull-null-null", expression.NewIfNull(null, null)},
		{"ifmissing-null-num", expression.NewIfMissing(null, num)},
		{"ifmissing-num", expression.NewIfMissing(num, num5)},
		{"ifmon-null-num", expression.NewIfMissingOrNull(null, num)},
		{"ifmon-null-null", expression.NewIfMissingOrNull(null, null)},
		{"ifmon-null-str", expression.NewIfMissingOrNull(null, str)},
		{"nvl-null-num", expression.NewNVL(null, num)},
		// N-ary (>2 operands) now optimizes natively via the eager-Vals harness.
		{"ifnull-3", expression.NewIfNull(null, null, num)},
		{"ifmissing-3", expression.NewIfMissing(null, num, num5)},
		{"ifmon-3", expression.NewIfMissingOrNull(null, null, str)},
		{"ifmon-4-allnull", expression.NewIfMissingOrNull(null, null, null, null)},
		// (COALESCE parses to IFMISSINGORNULL -- same struct/Name -- so it's
		// covered by the ifmon cases above; there's no NewCoalesce constructor.)
	}

	for _, tc := range cases {
		want := cbqEval(t, tc.expr)
		got, ok := nativeEval(t, tc.expr)
		if !ok {
			t.Errorf("%s: expression did not optimize to native path", tc.name)
			continue
		}
		if got != want {
			t.Errorf("%s: native=%q, cbq=%q", tc.name, got, want)
		}
	}
}

func TestIsTypeDifferentialVsCBQ(t *testing.T) {
	c := func(v interface{}) expression.Expression { return expression.NewConstant(v) }
	null := c(value.NULL_VALUE)
	num := c(5)
	str := c("x")
	bln := c(true)
	arrv := expression.NewConstant([]interface{}{1, 2})
	objv := expression.NewConstant(map[string]interface{}{"a": 1})

	mk := map[string]func(expression.Expression) expression.Expression{
		"is_array":   func(e expression.Expression) expression.Expression { return expression.NewIsArray(e) },
		"is_number":  func(e expression.Expression) expression.Expression { return expression.NewIsNumber(e) },
		"is_string":  func(e expression.Expression) expression.Expression { return expression.NewIsString(e) },
		"is_boolean": func(e expression.Expression) expression.Expression { return expression.NewIsBoolean(e) },
		"is_object":  func(e expression.Expression) expression.Expression { return expression.NewIsObject(e) },
		"is_atom":    func(e expression.Expression) expression.Expression { return expression.NewIsAtom(e) },
	}
	operands := map[string]expression.Expression{
		"num": num, "str": str, "bln": bln, "arr": arrv, "obj": objv, "null": null,
		// MISSING constant now round-trips (optimizer emits an empty json ->
		// MISSING, not "null"); IS_<type>(missing) must be MISSING like cbq.
		"missing": c(value.MISSING_VALUE),
	}

	for fn, ctor := range mk {
		for on, operand := range operands {
			expr := ctor(operand)
			want := cbqEval(t, expr)
			got, ok := nativeEval(t, expr)
			if !ok {
				t.Errorf("%s(%s): did not optimize", fn, on)
				continue
			}
			if got != want {
				t.Errorf("%s(%s): native=%q, cbq=%q", fn, on, got, want)
			}
		}
	}
}

func TestNullMissingIfDifferentialVsCBQ(t *testing.T) {
	c := func(v interface{}) expression.Expression { return expression.NewConstant(v) }
	miss := c(value.MISSING_VALUE)
	null := c(value.NULL_VALUE)

	cases := []struct {
		name string
		expr expression.Expression
	}{
		{"nullif-eq", expression.NewNullIf(c(5), c(5))},
		{"nullif-ne", expression.NewNullIf(c(5), c(6))},
		{"nullif-canon", expression.NewNullIf(c(5), c(5.0))},
		{"nullif-a-missing", expression.NewNullIf(miss, c(5))},
		{"nullif-a-null", expression.NewNullIf(null, c(5))},
		{"nullif-b-null", expression.NewNullIf(c(5), null)},
		{"nullif-str-ne", expression.NewNullIf(c("a"), c("b"))},
		{"missingif-eq", expression.NewMissingIf(c(5), c(5))},
		{"missingif-ne", expression.NewMissingIf(c(5), c(6))},
		{"missingif-a-missing", expression.NewMissingIf(miss, c(5))},
		{"missingif-b-null", expression.NewMissingIf(c(5), null)},
	}
	for _, tc := range cases {
		want := cbqEval(t, tc.expr)
		got, ok := nativeEval(t, tc.expr)
		if !ok {
			t.Errorf("%s: did not optimize", tc.name)
			continue
		}
		if got != want {
			t.Errorf("%s: native=%q, cbq=%q", tc.name, got, want)
		}
	}
}

// TestExprConstFoldVsCBQ covers const-folding (glue.exprConstFold): a
// runtime-constant expression that has NO native handler (REPEAT, LIKE,
// DATE_FORMAT_STR) must still optimize to a native ["json", value] leaf
// (nativeEval ok=true, i.e. not boxed) AND match cbq's runtime Evaluate. It also
// pins two subtleties: the fold VALUE comes from Evaluate() not the static
// Value() (SIGN(NaN())==0, not Value()'s answer), and a non-finite NaN() subtree
// stays boxed so the enclosing native op isn't corrupted -- yet SIGN(NaN()) as a
// whole still folds to a finite 0.
func TestExprConstFoldVsCBQ(t *testing.T) {
	cases := []struct{ name, expr string }{
		{"repeat", `REPEAT("x", 3)`},
		{"like-true", `"beer-1" LIKE "beer%"`},
		{"like-false", `"a" LIKE "%zz%"`},
		{"date", `DATE_FORMAT_STR("2020-01-01", "1111")`},
		{"upper-const", `UPPER("hello")`},
		{"sign-nan-folds-finite", `SIGN(NaN())`},
		{"nested-const", `SUBSTR(REPEAT("ab", 3), 2)`},
	}
	for _, tc := range cases {
		e, err := parser.Parse(tc.expr)
		if err != nil {
			t.Fatalf("%s: parse %q: %v", tc.name, tc.expr, err)
		}
		want := cbqEval(t, e)
		got, ok := nativeEval(t, e)
		if !ok {
			t.Errorf("%s: %q did not optimize (still boxed); const-fold should make it native", tc.name, tc.expr)
			continue
		}
		if got != want {
			t.Errorf("%s: native=%q, cbq=%q", tc.name, got, want)
		}
	}
}

func TestGreatestLeastDifferentialVsCBQ(t *testing.T) {
	c := func(v interface{}) expression.Expression { return expression.NewConstant(v) }
	miss := c(value.MISSING_VALUE)
	null := c(value.NULL_VALUE)

	cases := []struct {
		name string
		expr expression.Expression
	}{
		{"greatest-nums", expression.NewGreatest(c(3), c(7), c(5))},
		{"greatest-skip-null", expression.NewGreatest(c(3), null, c(9))},
		{"greatest-skip-missing", expression.NewGreatest(miss, c(4), c(2))},
		{"greatest-all-unknown", expression.NewGreatest(null, miss)},
		{"greatest-strings", expression.NewGreatest(c("a"), c("c"), c("b"))},
		{"greatest-mixed", expression.NewGreatest(c(5), c("a"))},
		{"greatest-4", expression.NewGreatest(c(1), c(9), c(3), c(7))},
		{"least-nums", expression.NewLeast(c(3), c(7), c(5))},
		{"least-skip-missing", expression.NewLeast(miss, c(7), c(2))},
		{"least-mixed", expression.NewLeast(c(5), c("a"))},
		{"least-all-unknown", expression.NewLeast(miss, null)},
	}
	for _, tc := range cases {
		want := cbqEval(t, tc.expr)
		got, ok := nativeEval(t, tc.expr)
		if !ok {
			t.Errorf("%s: did not optimize", tc.name)
			continue
		}
		if got != want {
			t.Errorf("%s: native=%q, cbq=%q", tc.name, got, want)
		}
	}
}

func TestElementDifferentialVsCBQ(t *testing.T) {
	c := func(v interface{}) expression.Expression { return expression.NewConstant(v) }
	arr := c([]interface{}{10, 20, 30})
	strs := c([]interface{}{"a", "b", "c"})
	nested := c([]interface{}{[]interface{}{1, 2}, map[string]interface{}{"x": 3}})
	withNull := c([]interface{}{nil, 2})

	cases := []struct {
		name string
		expr expression.Expression
	}{
		{"first", expression.NewElement(arr, c(0))},
		{"last-positive", expression.NewElement(arr, c(2))},
		{"negative-last", expression.NewElement(arr, c(-1))},
		{"negative-mid", expression.NewElement(arr, c(-2))},
		{"negative-oob", expression.NewElement(arr, c(-4))},
		{"oob", expression.NewElement(arr, c(5))},
		{"string-elem", expression.NewElement(strs, c(1))},
		{"nested-elem", expression.NewElement(nested, c(1))},
		{"null-elem", expression.NewElement(withNull, c(0))},
		{"integral-float-index", expression.NewElement(arr, c(1.0))},
		{"fractional-index", expression.NewElement(arr, c(1.5))},
		{"nonnumber-index", expression.NewElement(arr, c("x"))},
		{"missing-index", expression.NewElement(arr, c(value.MISSING_VALUE))},
		{"null-index", expression.NewElement(arr, c(value.NULL_VALUE))},
		{"missing-arr", expression.NewElement(c(value.MISSING_VALUE), c(0))},
		{"null-arr", expression.NewElement(c(value.NULL_VALUE), c(0))},
		{"nonarray-arr", expression.NewElement(c(5), c(0))},
	}
	for _, tc := range cases {
		want := cbqEval(t, tc.expr)
		got, ok := nativeEval(t, tc.expr)
		if !ok {
			t.Errorf("%s: did not optimize", tc.name)
			continue
		}
		if got != want {
			t.Errorf("%s: native=%q, cbq=%q", tc.name, got, want)
		}
	}
}

func TestCaseDifferentialVsCBQ(t *testing.T) {
	c := func(v interface{}) expression.Expression { return expression.NewConstant(v) }
	wt := func(when, then expression.Expression) *expression.WhenTerm {
		return &expression.WhenTerm{When: when, Then: then}
	}

	cases := []struct {
		name string
		expr expression.Expression
	}{
		// Searched CASE.
		{"searched-first", expression.NewSearchedCase(
			expression.WhenTerms{wt(c(true), c("a")), wt(c(false), c("b"))}, nil)},
		{"searched-second", expression.NewSearchedCase(
			expression.WhenTerms{wt(c(false), c("a")), wt(c(true), c("b"))}, nil)},
		{"searched-else", expression.NewSearchedCase(
			expression.WhenTerms{wt(c(false), c("a"))}, c("e"))},
		{"searched-no-match-no-else", expression.NewSearchedCase(
			expression.WhenTerms{wt(c(false), c("a"))}, nil)},
		{"searched-null-cond", expression.NewSearchedCase(
			expression.WhenTerms{wt(c(value.NULL_VALUE), c("a")), wt(c(true), c("b"))}, nil)},
		{"searched-num-cond", expression.NewSearchedCase(
			expression.WhenTerms{wt(c(5), c("a"))}, c("z"))},
		// Simple CASE (desugars to eq conditions).
		{"simple-match", expression.NewSimpleCase(c(5),
			expression.WhenTerms{wt(c(5), c("five")), wt(c(6), c("six"))}, c("other"))},
		{"simple-second", expression.NewSimpleCase(c(6),
			expression.WhenTerms{wt(c(5), c("five")), wt(c(6), c("six"))}, c("other"))},
		{"simple-else", expression.NewSimpleCase(c(9),
			expression.WhenTerms{wt(c(5), c("five"))}, c("other"))},
		{"simple-no-else", expression.NewSimpleCase(c(9),
			expression.WhenTerms{wt(c(5), c("five"))}, nil)},
		{"simple-str", expression.NewSimpleCase(c("x"),
			expression.WhenTerms{wt(c("x"), c(1)), wt(c("y"), c(2))}, c(0))},
	}

	for _, tc := range cases {
		want := cbqEval(t, tc.expr)
		got, ok := nativeEval(t, tc.expr)
		if !ok {
			t.Errorf("%s: did not optimize", tc.name)
			continue
		}
		if got != want {
			t.Errorf("%s: native=%q, cbq=%q", tc.name, got, want)
		}
	}
}

func TestConcatDifferentialVsCBQ(t *testing.T) {
	c := func(v interface{}) expression.Expression { return expression.NewConstant(v) }
	null := c(value.NULL_VALUE)

	cases := []struct {
		name string
		expr expression.Expression
	}{
		{"two", expression.NewConcat(c("a"), c("b"))},
		{"three", expression.NewConcat(c("a"), c("b"), c("c"))},
		{"empty", expression.NewConcat(c(""), c("x"))},
		{"escape-nl", expression.NewConcat(c("a\nb"), c("c"))},
		{"escape-quote", expression.NewConcat(c(`x"y`), c("z"))},
		{"num-null", expression.NewConcat(c("a"), c(5))},
		{"null-null", expression.NewConcat(c("a"), null)},
	}

	for _, tc := range cases {
		want := cbqEval(t, tc.expr)
		got, ok := nativeEval(t, tc.expr)
		if !ok {
			t.Errorf("%s: did not optimize", tc.name)
			continue
		}
		if got != want {
			t.Errorf("%s: native=%q, cbq=%q", tc.name, got, want)
		}
	}
}

func TestInDifferentialVsCBQ(t *testing.T) {
	c := func(v interface{}) expression.Expression { return expression.NewConstant(v) }
	arr := func(xs ...interface{}) expression.Expression {
		if xs == nil { // a zero-arg variadic is a nil slice, which becomes NULL
			xs = []interface{}{}
		}
		return expression.NewConstant(xs)
	}
	null := c(value.NULL_VALUE)

	cases := []struct {
		name string
		expr expression.Expression
	}{
		{"member", expression.NewIn(c(2), arr(1, 2, 3))},
		{"not-member", expression.NewIn(c(5), arr(1, 2, 3))},
		{"empty", expression.NewIn(c(2), arr())},
		{"int-eq-float", expression.NewIn(c(2), arr(1.0, 2.0))},
		{"string-member", expression.NewIn(c("b"), arr("a", "b"))},
		{"not-array", expression.NewIn(c(2), c(5))},
		{"null-x-empty", expression.NewIn(null, arr())},
		{"null-x-nonempty", expression.NewIn(null, arr(1, 2))},
		{"nomatch-with-null", expression.NewIn(c(5), arr(1, value.NULL_VALUE, 3))},
		{"match-with-null", expression.NewIn(c(3), arr(1, value.NULL_VALUE, 3))},
		{"nomatch-no-null", expression.NewIn(c(5), arr(1, 2, 3))},
	}

	for _, tc := range cases {
		want := cbqEval(t, tc.expr)
		got, ok := nativeEval(t, tc.expr)
		if !ok {
			t.Errorf("%s: expression did not optimize to native path", tc.name)
			continue
		}
		if got != want {
			t.Errorf("%s: native=%q, cbq=%q", tc.name, got, want)
		}
	}
}

func TestBetweenDifferentialVsCBQ(t *testing.T) {
	c := func(v interface{}) expression.Expression { return expression.NewConstant(v) }
	null := c(value.NULL_VALUE)

	cases := []struct {
		name string
		expr expression.Expression
	}{
		{"in-range", expression.NewBetween(c(3), c(1), c(5))},
		{"below", expression.NewBetween(c(0), c(1), c(5))},
		{"above", expression.NewBetween(c(9), c(1), c(5))},
		{"eq-low", expression.NewBetween(c(1), c(1), c(5))},
		{"eq-high", expression.NewBetween(c(5), c(1), c(5))},
		{"float", expression.NewBetween(c(2.5), c(2), c(3))},
		{"string", expression.NewBetween(c("b"), c("a"), c("c"))},
		{"item-null", expression.NewBetween(null, c(1), c(5))},
		{"low-null", expression.NewBetween(c(3), null, c(5))},
		{"high-null", expression.NewBetween(c(3), c(1), null)},
	}

	for _, tc := range cases {
		want := cbqEval(t, tc.expr)
		got, ok := nativeEval(t, tc.expr)
		if !ok {
			t.Errorf("%s: expression did not optimize to native path", tc.name)
			continue
		}
		if got != want {
			t.Errorf("%s: native=%q, cbq=%q", tc.name, got, want)
		}
	}
}

func TestLogicAndOrDifferentialVsCBQ(t *testing.T) {
	c := func(v interface{}) expression.Expression { return expression.NewConstant(v) }
	tru := c(true)
	fls := c(false)
	null := c(value.NULL_VALUE)
	miss := c(value.MISSING_VALUE)
	num := c(5)   // a real, truthy non-boolean
	zero := c(0)  // a real, non-truthy non-boolean
	str := c("x") // truthy string
	estr := c("") // non-truthy (empty) string

	cases := []struct {
		name string
		expr expression.Expression
	}{
		// AND: any real non-truthy -> false; else MISSING dominates NULL.
		{"and-tt", expression.NewAnd(tru, tru)},
		{"and-tf", expression.NewAnd(tru, fls)},
		{"and-ft", expression.NewAnd(fls, tru)},
		{"and-t-null", expression.NewAnd(tru, null)},
		{"and-t-miss", expression.NewAnd(tru, miss)},
		{"and-null-miss", expression.NewAnd(null, miss)}, // -> MISSING (asymmetry)
		{"and-miss-null", expression.NewAnd(miss, null)}, // -> MISSING
		{"and-f-null", expression.NewAnd(fls, null)},     // -> FALSE (short-circuit)
		{"and-f-miss", expression.NewAnd(fls, miss)},
		{"and-null-null", expression.NewAnd(null, null)},
		{"and-num-truthy", expression.NewAnd(num, tru)}, // 5 is truthy
		{"and-zero", expression.NewAnd(zero, tru)},      // 0 is falsy -> FALSE
		{"and-estr", expression.NewAnd(estr, tru)},      // "" is falsy -> FALSE
		{"and-str", expression.NewAnd(str, tru)},        // "x" is truthy
		{"and-3-tf", expression.NewAnd(tru, tru, fls)},
		{"and-3-tnt", expression.NewAnd(tru, null, tru)},
		{"and-4-tnmt", expression.NewAnd(tru, null, miss, tru)},

		// OR: any real truthy -> true; else NULL dominates MISSING.
		{"or-ff", expression.NewOr(fls, fls)},
		{"or-ft", expression.NewOr(fls, tru)},
		{"or-f-null", expression.NewOr(fls, null)},
		{"or-f-miss", expression.NewOr(fls, miss)},
		{"or-null-miss", expression.NewOr(null, miss)}, // -> NULL (asymmetry)
		{"or-miss-null", expression.NewOr(miss, null)}, // -> NULL
		{"or-t-null", expression.NewOr(tru, null)},     // -> TRUE (short-circuit)
		{"or-null-null", expression.NewOr(null, null)},
		{"or-miss-miss", expression.NewOr(miss, miss)},
		{"or-zero-num", expression.NewOr(zero, num)}, // 5 truthy -> TRUE
		{"or-zero-zero", expression.NewOr(zero, zero)},
		{"or-estr-estr", expression.NewOr(estr, estr)},
		{"or-3-fft", expression.NewOr(fls, fls, tru)},
		{"or-3-fnm", expression.NewOr(fls, null, miss)},
	}

	for _, tc := range cases {
		want := cbqEval(t, tc.expr)
		got, ok := nativeEval(t, tc.expr)
		if !ok {
			t.Errorf("%s: expression did not optimize to native path", tc.name)
			continue
		}
		if got != want {
			t.Errorf("%s: native=%q, cbq=%q", tc.name, got, want)
		}
	}
}

func TestMathUnaryDifferentialVsCBQ(t *testing.T) {
	c := func(v interface{}) expression.Expression { return expression.NewConstant(v) }
	null := c(value.NULL_VALUE)

	mk := map[string]func(expression.Expression) expression.Expression{
		"abs":     func(e expression.Expression) expression.Expression { return expression.NewAbs(e) },
		"ceil":    func(e expression.Expression) expression.Expression { return expression.NewCeil(e) },
		"floor":   func(e expression.Expression) expression.Expression { return expression.NewFloor(e) },
		"sqrt":    func(e expression.Expression) expression.Expression { return expression.NewSqrt(e) },
		"exp":     func(e expression.Expression) expression.Expression { return expression.NewExp(e) },
		"ln":      func(e expression.Expression) expression.Expression { return expression.NewLn(e) },
		"log":     func(e expression.Expression) expression.Expression { return expression.NewLog(e) },
		"sign":    func(e expression.Expression) expression.Expression { return expression.NewSign(e) },
		"degrees": func(e expression.Expression) expression.Expression { return expression.NewDegrees(e) },
		"radians": func(e expression.Expression) expression.Expression { return expression.NewRadians(e) },
		"sin":     func(e expression.Expression) expression.Expression { return expression.NewSin(e) },
		"cos":     func(e expression.Expression) expression.Expression { return expression.NewCos(e) },
		"tan":     func(e expression.Expression) expression.Expression { return expression.NewTan(e) },
		"asin":    func(e expression.Expression) expression.Expression { return expression.NewAsin(e) },
		"acos":    func(e expression.Expression) expression.Expression { return expression.NewAcos(e) },
		"atan":    func(e expression.Expression) expression.Expression { return expression.NewAtan(e) },
	}
	// Operands span sign, magnitude, int-vs-float, and the domain edges that
	// produce NaN/Inf (sqrt(-1), ln(0), ln(-1), log(0)) -- all must agree with cbq.
	operands := map[string]expression.Expression{
		"pos-int": c(9), "neg-int": c(-7), "zero": c(0), "pos-float": c(2.5),
		"neg-float": c(-2.5), "big": c(1000000), "frac": c(0.5), "one": c(1),
		"null": null, "str": c("x"), "bool": c(true),
	}

	for fn, ctor := range mk {
		for on, operand := range operands {
			expr := ctor(operand)
			want := cbqEval(t, expr)
			got, ok := nativeEval(t, expr)
			if !ok {
				t.Errorf("%s(%s): did not optimize", fn, on)
				continue
			}
			if got != want {
				t.Errorf("%s(%s): native=%q, cbq=%q", fn, on, got, want)
			}
		}
	}
}

func TestMathBinaryDifferentialVsCBQ(t *testing.T) {
	c := func(v interface{}) expression.Expression { return expression.NewConstant(v) }

	mk := map[string]func(a, b expression.Expression) expression.Expression{
		"power": func(a, b expression.Expression) expression.Expression { return expression.NewPower(a, b) },
		"atan2": func(a, b expression.Expression) expression.Expression { return expression.NewAtan2(a, b) },
	}
	// Operand pairs spanning sign/magnitude/int-vs-float and NaN/Inf edges
	// (power(-1,0.5), power(0,-1)=+Inf), plus non-number operands -> NULL and NULL.
	pairs := []struct{ a, b expression.Expression }{
		{c(2), c(10)}, {c(2.0), c(0.5)}, {c(-1), c(0.5)}, {c(0), c(-1)},
		{c(9), c(0)}, {c(1), c(1)}, {c(3), c(-2)}, {c(1), c(0)},
		{c(value.NULL_VALUE), c(2)}, {c(2), c("x")}, {c("a"), c("b")},
	}

	for fn, ctor := range mk {
		for i, p := range pairs {
			expr := ctor(p.a, p.b)
			want := cbqEval(t, expr)
			got, ok := nativeEval(t, expr)
			if !ok {
				t.Errorf("%s pair[%d]: did not optimize", fn, i)
				continue
			}
			if got != want {
				t.Errorf("%s pair[%d]: native=%q, cbq=%q", fn, i, got, want)
			}
		}
	}
}

func TestArrayReduceDifferentialVsCBQ(t *testing.T) {
	c := func(v interface{}) expression.Expression { return expression.NewConstant(v) }
	arr := func(xs ...interface{}) expression.Expression {
		if xs == nil { // a zero-arg variadic is a nil slice, which becomes NULL
			xs = []interface{}{}
		}
		return expression.NewConstant(xs)
	}

	mk := map[string]func(expression.Expression) expression.Expression{
		"array_length": func(e expression.Expression) expression.Expression { return expression.NewArrayLength(e) },
		"array_count":  func(e expression.Expression) expression.Expression { return expression.NewArrayCount(e) },
		"array_sum":    func(e expression.Expression) expression.Expression { return expression.NewArraySum(e) },
		"array_avg":    func(e expression.Expression) expression.Expression { return expression.NewArrayAvg(e) },
	}
	// Arrays mixing numbers/strings/nulls/nested, ints vs floats, empty; plus
	// non-array and MISSING operands.
	operands := map[string]expression.Expression{
		"nums":     arr(1, 2, 3, 4),
		"mixed":    arr(1, "x", 2.5, nil, 3),
		"floats":   arr(1.5, 2.5),
		"withnull": arr(1, nil, 2),
		"strs":     arr("a", "b"),
		"nested":   arr(1, []interface{}{2, 3}, 4),
		"empty":    arr(),
		"nonarr":   c(5),
		"null":     c(value.NULL_VALUE),
		"missing":  c(value.MISSING_VALUE),
	}

	for fn, ctor := range mk {
		for on, operand := range operands {
			expr := ctor(operand)
			want := cbqEval(t, expr)
			got, ok := nativeEval(t, expr)
			if !ok {
				t.Errorf("%s(%s): did not optimize", fn, on)
				continue
			}
			if got != want {
				t.Errorf("%s(%s): native=%q, cbq=%q", fn, on, got, want)
			}
		}
	}
}

func TestDistinctFromDifferentialVsCBQ(t *testing.T) {
	c := func(v interface{}) expression.Expression { return expression.NewConstant(v) }

	mk := map[string]func(a, b expression.Expression) expression.Expression{
		"is_distinct_from":     func(a, b expression.Expression) expression.Expression { return expression.NewIsDistinctFrom(a, b) },
		"is_not_distinct_from": func(a, b expression.Expression) expression.Expression { return expression.NewIsNotDistinctFrom(a, b) },
	}
	// Every pair over {equal/unequal numbers, strings, arrays, objects, bools,
	// NULL, MISSING} -- covers the null-safe branches (both-MISSING, one-MISSING,
	// both-NULL, one-NULL, MISSING-vs-NULL) and value equality across types.
	vals := map[string]expression.Expression{
		"i1": c(1.0), "i1b": c(1.0), "i2": c(2.0), "f15": c(1.5),
		"sa": c("a"), "sb": c("b"),
		"arr": c([]interface{}{1.0, 2.0}), "arr2": c([]interface{}{1.0, 2.0}),
		"obj": c(map[string]interface{}{"x": 1.0}), "tt": c(true), "ff": c(false),
		"null": c(value.NULL_VALUE), "missing": c(value.MISSING_VALUE),
	}

	for fn, ctor := range mk {
		for an, a := range vals {
			for bn, b := range vals {
				expr := ctor(a, b)
				want := cbqEval(t, expr)
				got, ok := nativeEval(t, expr)
				if !ok {
					t.Errorf("%s(%s,%s): did not optimize", fn, an, bn)
					continue
				}
				if got != want {
					t.Errorf("%s(%s,%s): native=%q, cbq=%q", fn, an, bn, got, want)
				}
			}
		}
	}
}

func TestObjectPolyLengthDifferentialVsCBQ(t *testing.T) {
	c := func(v interface{}) expression.Expression { return expression.NewConstant(v) }

	mk := map[string]func(expression.Expression) expression.Expression{
		"object_length": func(e expression.Expression) expression.Expression { return expression.NewObjectLength(e) },
		"poly_length":   func(e expression.Expression) expression.Expression { return expression.NewPolyLength(e) },
	}
	// Objects, arrays, strings (incl. escapes / multi-byte), plus non-matching
	// types and MISSING/NULL. OBJECT_LENGTH -> NULL on non-objects; POLY_LENGTH
	// -> decoded byte length of a string / element count / pair count.
	operands := map[string]expression.Expression{
		"obj3":     c(map[string]interface{}{"a": 1, "b": 2, "c": 3}),
		"obj0":     c(map[string]interface{}{}),
		"objnest":  c(map[string]interface{}{"a": map[string]interface{}{"x": 1}, "b": []interface{}{1, 2}}),
		"arr4":     c([]interface{}{1, 2, 3, 4}),
		"arr0":     c([]interface{}{}),
		"str":      c("hello"),
		"stresc":   c("a\tb\nc"), // escapes: decoded length (5), not the escaped bytes
		"struni":   c("café"),    // multi-byte: cbq's len(ToString()) is the BYTE length (5)
		"strempty": c(""),
		"num":      c(5),
		"boolt":    c(true),
		"null":     c(value.NULL_VALUE),
		"missing":  c(value.MISSING_VALUE),
	}

	for fn, ctor := range mk {
		for on, operand := range operands {
			expr := ctor(operand)
			want := cbqEval(t, expr)
			got, ok := nativeEval(t, expr)
			if !ok {
				t.Errorf("%s(%s): did not optimize", fn, on)
				continue
			}
			if got != want {
				t.Errorf("%s(%s): native=%q, cbq=%q", fn, on, got, want)
			}
		}
	}
}

// TestObjectNamesDifferentialVsCBQ proves OBJECT_NAMES native lowering: the sorted
// JSON string-array of an object's field names is byte-identical to cbq, MISSING ->
// MISSING, and any non-object -> NULL. The sort must match cbq's byte-order name
// sort exactly (base.ObjectNames reuses the ValComparer KeyVals sort), so the
// operands mix unsorted / reverse-sorted / escaped / unicode / numeric keys.
func TestObjectNamesDifferentialVsCBQ(t *testing.T) {
	c := func(v interface{}) expression.Expression { return expression.NewConstant(v) }
	ctor := func(e expression.Expression) expression.Expression { return expression.NewObjectNames(e) }

	operands := map[string]expression.Expression{
		"sorted":    c(map[string]interface{}{"a": 1, "b": 2, "c": 3}),
		"reverse":   c(map[string]interface{}{"c": 1, "b": 2, "a": 3}),
		"mixedlen":  c(map[string]interface{}{"ab": 1, "a": 2, "abc": 3}),
		"caps":      c(map[string]interface{}{"B": 1, "a": 2, "A": 3, "b": 4}), // uppercase sorts before lowercase (byte order)
		"digits":    c(map[string]interface{}{"1": 1, "10": 2, "2": 3}),        // lexical, not numeric
		"nested":    c(map[string]interface{}{"z": map[string]interface{}{"x": 1}, "a": []interface{}{1, 2}}),
		"unicode":   c(map[string]interface{}{"café": 1, "abc": 2}),
		"one":       c(map[string]interface{}{"only": 1}),
		"empty":     c(map[string]interface{}{}),
		"valuekind": c(map[string]interface{}{"n": nil, "s": "x", "b": true, "num": 3}),
		"arr":       c([]interface{}{1, 2}),
		"str":       c("hello"),
		"num":       c(5),
		"boolt":     c(true),
		"null":      c(value.NULL_VALUE),
		"missing":   c(value.MISSING_VALUE),
	}

	for on, operand := range operands {
		expr := ctor(operand)
		want := cbqEval(t, expr)
		got, ok := nativeEval(t, expr)
		if !ok {
			t.Errorf("object_names(%s): did not optimize to native", on)
			continue
		}
		if got != want {
			t.Errorf("object_names(%s): native=%q, cbq=%q", on, got, want)
		}
	}
}

// TestObjectNamesZeroAlloc asserts the per-row native OBJECT_NAMES build allocates
// nothing after warmup: the ValComparer's KeyVals pool reuses the key backing and
// the output goes into the reused bufPre buffer (the GARBAGE MANDATE).
func TestObjectNamesZeroAlloc(t *testing.T) {
	skipZeroAllocUnderRace(t)
	cmp := base.NewValComparer()
	val := base.Val([]byte(`{"gamma":1,"alpha":2,"beta":3,"delta":4}`))

	var buf []byte
	buf, _, _ = base.ObjectNames(cmp, val, buf) // warm up the KeyVals pool + buffer

	n := testing.AllocsPerRun(2000, func() {
		buf, _, _ = base.ObjectNames(cmp, val, buf)
	})
	if n != 0 {
		t.Errorf("base.ObjectNames: %v allocs/row after warmup; want 0", n)
	}
}

// TestObjectValuesPairsDifferentialVsCBQ proves OBJECT_VALUES / OBJECT_PAIRS native
// lowering is byte-identical to cbq: the by-name-sorted value array (and the
// {"name","val"} pair array) must match cbq's serialization exactly. This is the
// real test of the value re-emit fidelity (base.appendJSONElem): each value --
// string (re-quoted), null, boolean, number, nested array/object -- is copied
// verbatim, so the operands deliberately mix every kind, plus escaped / unicode /
// caps keys to exercise both the name sort and (for pairs) name re-encoding.
func TestObjectValuesPairsDifferentialVsCBQ(t *testing.T) {
	c := func(v interface{}) expression.Expression { return expression.NewConstant(v) }

	operands := map[string]expression.Expression{
		"sorted":    c(map[string]interface{}{"a": 1, "b": 2, "c": 3}),
		"reverse":   c(map[string]interface{}{"c": 1, "b": 2, "a": 3}),
		"mixedlen":  c(map[string]interface{}{"ab": 1, "a": 2, "abc": 3}),
		"caps":      c(map[string]interface{}{"B": 1, "a": 2, "A": 3, "b": 4}),
		"digits":    c(map[string]interface{}{"1": 1, "10": 2, "2": 3}),
		"nested":    c(map[string]interface{}{"z": map[string]interface{}{"x": 1}, "a": []interface{}{1, 2}}),
		"unicode":   c(map[string]interface{}{"café": 1, "abc": 2}),
		"one":       c(map[string]interface{}{"only": 1}),
		"empty":     c(map[string]interface{}{}),
		"valuekind": c(map[string]interface{}{"n": nil, "s": "x", "b": true, "num": 3, "f": 2.5}),
		"stresc":    c(map[string]interface{}{"a": "he\"llo\n", "b": "tab\tend"}),
		"arr":       c([]interface{}{1, 2}),
		"str":       c("hello"),
		"num":       c(5),
		"boolt":     c(true),
		"null":      c(value.NULL_VALUE),
		"missing":   c(value.MISSING_VALUE),
	}

	ctors := map[string]func(expression.Expression) expression.Expression{
		"object_values": func(e expression.Expression) expression.Expression { return expression.NewObjectValues(e) },
		"object_pairs":  func(e expression.Expression) expression.Expression { return expression.NewObjectPairs(e) },
	}

	for fn, ctor := range ctors {
		for on, operand := range operands {
			expr := ctor(operand)
			want := cbqEval(t, expr)
			got, ok := nativeEval(t, expr)
			if !ok {
				t.Errorf("%s(%s): did not optimize to native", fn, on)
				continue
			}
			if got != want {
				t.Errorf("%s(%s): native=%q, cbq=%q", fn, on, got, want)
			}
		}
	}
}

// TestObjectValuesPairsZeroAlloc asserts the per-row native OBJECT_VALUES /
// OBJECT_PAIRS builds allocate nothing after warmup: the KeyVals pool reuses the
// key backing, value slices point into the operand, and output goes into the
// reused bufPre (the GARBAGE MANDATE).
func TestObjectValuesPairsZeroAlloc(t *testing.T) {
	skipZeroAllocUnderRace(t)
	cmp := base.NewValComparer()
	val := base.Val([]byte(`{"gamma":1,"alpha":"x","beta":[1,2],"delta":null}`))

	for _, tc := range []struct {
		name string
		fn   func(*base.ValComparer, base.Val, []byte) ([]byte, base.Val, bool)
	}{
		{"ObjectValues", base.ObjectValues},
		{"ObjectPairs", base.ObjectPairs},
	} {
		var buf []byte
		buf, _, _ = tc.fn(cmp, val, buf) // warm up the pool + buffer
		n := testing.AllocsPerRun(2000, func() {
			buf, _, _ = tc.fn(cmp, val, buf)
		})
		if n != 0 {
			t.Errorf("base.%s: %v allocs/row after warmup; want 0", tc.name, n)
		}
	}
}

// TestArrayBuildersDifferentialVsCBQ proves ARRAY_APPEND / ARRAY_PREPEND /
// ARRAY_CONCAT (2-arg forms) native lowering is byte-identical to cbq: the spliced
// result, MISSING short-circuit (a MISSING operand -> MISSING, precedence over
// NULL), and non-array -> NULL all match. Operands mix element kinds (string/null/
// nested), empty arrays, and non-array / MISSING operands.
func TestArrayBuildersDifferentialVsCBQ(t *testing.T) {
	c := func(v interface{}) expression.Expression { return expression.NewConstant(v) }
	arr := func(xs ...interface{}) expression.Expression {
		if xs == nil {
			xs = []interface{}{}
		}
		return expression.NewConstant(xs)
	}
	miss := c(value.MISSING_VALUE)
	null := c(value.NULL_VALUE)

	// Each case is a (left, right) operand pair fed to all three builders. cbq's
	// operand ORDER differs per builder (append: arr,val; prepend: val,arr; concat:
	// arr,arr), but a differential just needs SAME order into cbq and native, so we
	// pass (left,right) verbatim to each constructor and compare.
	pairs := map[string][2]expression.Expression{
		"arr+num":     {arr(1, 2), c(3)},
		"arr+str":     {arr(1), c("x")},
		"arr+null":    {arr(1), null},
		"arr+arr":     {arr(1, 2), arr(3, 4)},
		"arr+nested":  {arr(1), arr(9, 8)},
		"empty+num":   {arr(), c(5)},
		"empty+arr":   {arr(), arr(3)},
		"arr+empty":   {arr(1), arr()},
		"empty+empty": {arr(), arr()},
		"nonarr+num":  {c(5), c(3)},
		"num+arr":     {c(5), arr(1)},
		"miss+num":    {miss, c(3)},
		"arr+miss":    {arr(1), miss},
		"nonarr+miss": {c(5), miss}, // missing must win over null
		"null-l+arr":  {null, arr(1)},
	}

	ctors := map[string]func(a, b expression.Expression) expression.Expression{
		"array_append":  func(a, b expression.Expression) expression.Expression { return expression.NewArrayAppend(a, b) },
		"array_prepend": func(a, b expression.Expression) expression.Expression { return expression.NewArrayPrepend(a, b) },
		"array_concat":  func(a, b expression.Expression) expression.Expression { return expression.NewArrayConcat(a, b) },
	}

	for fn, ctor := range ctors {
		for on, pr := range pairs {
			expr := ctor(pr[0], pr[1])
			want := cbqEval(t, expr)
			got, ok := nativeEval(t, expr)
			if !ok {
				t.Errorf("%s(%s): did not optimize to native", fn, on)
				continue
			}
			if got != want {
				t.Errorf("%s(%s): native=%q, cbq=%q", fn, on, got, want)
			}
		}
	}
}

// TestArrayReshapersDifferentialVsCBQ proves ARRAY_SORT / ARRAY_REVERSE (unary) and
// ARRAY_FLATTEN (2-arg) native lowering is byte-identical to cbq: collation-ordered
// sort (mixed types, NULLs, nesting), reversal, depth-limited flatten, and every
// MISSING/NULL/non-array (and non-integer depth) edge. Inputs are canonical so the
// verbatim element re-emit matches cbq's serialization.
func TestArrayReshapersDifferentialVsCBQ(t *testing.T) {
	c := func(v interface{}) expression.Expression { return expression.NewConstant(v) }
	arr := func(xs ...interface{}) expression.Expression {
		if xs == nil {
			xs = []interface{}{}
		}
		return expression.NewConstant(xs)
	}
	miss := c(value.MISSING_VALUE)
	null := c(value.NULL_VALUE)

	// --- unary SORT / REVERSE ---
	unaryCtors := map[string]func(a expression.Expression) expression.Expression{
		"array_sort":    func(a expression.Expression) expression.Expression { return expression.NewArraySort(a) },
		"array_reverse": func(a expression.Expression) expression.Expression { return expression.NewArrayReverse(a) },
	}
	unaryCases := map[string]expression.Expression{
		"nums":         arr(3, 1, 2),
		"nums-dup":     arr(2, 2, 1, 3, 1),
		"strs":         arr("banana", "apple", "cherry"),
		"mixed-type":   arr(2, "a", true, 1), // cross-type collation order
		"with-null":    arr(3, nil, 1),       // NULL sorts below numbers
		"nested":       arr([]interface{}{2, 1}, []interface{}{1, 0}),
		"single":       arr(7),
		"empty":        arr(),
		"nonarr":       c(5),
		"missing":      miss,
		"null-operand": null,
	}
	for fn, ctor := range unaryCtors {
		for on, in := range unaryCases {
			expr := ctor(in)
			want := cbqEval(t, expr)
			got, ok := nativeEval(t, expr)
			if !ok {
				t.Errorf("%s(%s): did not optimize to native", fn, on)
				continue
			}
			if got != want {
				t.Errorf("%s(%s): native=%q, cbq=%q", fn, on, got, want)
			}
		}
	}

	// --- 2-arg FLATTEN: (arr, depth) ---
	flatCases := map[string][2]expression.Expression{
		"depth0":        {arr(1, []interface{}{2, 3}, 4), c(0)},                     // shallow copy
		"depth1":        {arr(1, []interface{}{2, 3}, 4), c(1)},                     // one level
		"depth2":        {arr(1, []interface{}{2, []interface{}{3, 4}}), c(2)},      // two levels
		"neg-full":      {arr(1, []interface{}{2, []interface{}{3, 4}}), c(-1)},     // negative -> flatten fully
		"deep-shallow":  {arr([]interface{}{[]interface{}{[]interface{}{1}}}), c(1)}, // only outer level
		"no-nesting":    {arr(1, 2, 3), c(2)},                                       // depth exceeds nesting
		"empty":         {arr(), c(1)},                                              //
		"mixed":         {arr("a", []interface{}{"b", "c"}, nil), c(1)},             // strings + null
		"missing-arr":   {miss, c(1)},                                               // -> MISSING
		"arr-missing-d": {arr(1), miss},                                             // -> MISSING
		"nonarr":        {c(5), c(1)},                                               // -> NULL
		"nonnum-depth":  {arr(1), c("x")},                                           // -> NULL
		"frac-depth":    {arr(1, []interface{}{2}), c(1.5)},                         // non-integer -> NULL
		"float-int-d":   {arr(1, []interface{}{2}), c(2.0)},                         // integral float OK
	}
	for on, pr := range flatCases {
		expr := expression.NewArrayFlatten(pr[0], pr[1])
		want := cbqEval(t, expr)
		got, ok := nativeEval(t, expr)
		if !ok {
			t.Errorf("array_flatten(%s): did not optimize to native", on)
			continue
		}
		if got != want {
			t.Errorf("array_flatten(%s): native=%q, cbq=%q", on, got, want)
		}
	}
}

// TestAnyComprehensionDifferentialVsCBQ proves the native ANY predicate
// (single-binding, IN form) is byte-identical to cbq over constant arrays: the
// element predicate, the collation-based comparison, and every MISSING (-> "") /
// NULL / non-array / empty edge.
func TestAnyComprehensionDifferentialVsCBQ(t *testing.T) {
	c := func(v interface{}) expression.Expression { return expression.NewConstant(v) }
	arr := func(xs ...interface{}) expression.Expression {
		if xs == nil {
			xs = []interface{}{}
		}
		return expression.NewConstant(xs)
	}
	// ANY x IN <arrE> SATISFIES x > <k> END
	anyGT := func(arrE expression.Expression, k interface{}) expression.Expression {
		b := expression.NewSimpleBinding("x", arrE)
		sat := expression.NewGT(expression.NewIdentifier("x"), c(k))
		return expression.NewAny(expression.Bindings{b}, sat)
	}

	cases := map[string]expression.Expression{
		"has-gt":         anyGT(arr(1, 2, 9), 5),                // true
		"none-gt":        anyGT(arr(1, 2, 3), 5),                // false
		"boundary":       anyGT(arr(5, 5), 5),                   // false (strict >)
		"empty":          anyGT(arr(), 5),                       // false
		"nonarr-num":     anyGT(c(42), 5),                       // null
		"nonarr-str":     anyGT(c("hi"), 5),                     // null
		"null-arr":       anyGT(c(value.NULL_VALUE), 5),         // null
		"missing-arr":    anyGT(c(value.MISSING_VALUE), 5),      // missing -> ""
		"null-elem-hit":  anyGT(arr(nil, 9), 5),                 // true (9>5; null skipped)
		"null-elem-miss": anyGT(arr(nil, 1), 5),                 // false
		"strings":        anyGT(arr("a", "zz"), "m"),            // true ("zz" > "m")
		"strings-none":   anyGT(arr("a", "b"), "m"),             // false
		"mixed-type":     anyGT(arr("a", 9), 5),                 // true (9>5; "a" vs num by collation)
	}
	for name, e := range cases {
		want := cbqEval(t, e)
		got, ok := nativeEval(t, e)
		if !ok {
			t.Errorf("ANY(%s): did not optimize to native", name)
			continue
		}
		if got != want {
			t.Errorf("ANY(%s): native=%q, cbq=%q", name, got, want)
		}
	}
}

// TestEveryFirstArrayDifferentialVsCBQ proves native EVERY (predicate),
// FIRST/ARRAY (mapping + optional WHEN) are byte-identical to cbq over constant
// arrays, including the vacuous-empty, no-match, and MISSING/NULL/non-array edges.
func TestEveryFirstArrayDifferentialVsCBQ(t *testing.T) {
	c := func(v interface{}) expression.Expression { return expression.NewConstant(v) }
	arr := func(xs ...interface{}) expression.Expression {
		if xs == nil {
			xs = []interface{}{}
		}
		return expression.NewConstant(xs)
	}
	x := expression.NewIdentifier("x")
	bind := func(arrE expression.Expression) expression.Bindings {
		return expression.Bindings{expression.NewSimpleBinding("x", arrE)}
	}

	cases := map[string]expression.Expression{
		// EVERY x IN arr SATISFIES x < 5
		"every-all":     expression.NewEvery(bind(arr(1, 2, 3)), expression.NewLT(x, c(5))),   // true
		"every-not-all": expression.NewEvery(bind(arr(1, 9)), expression.NewLT(x, c(5))),      // false
		"every-empty":   expression.NewEvery(bind(arr()), expression.NewLT(x, c(5))),          // true (vacuous)
		"every-nonarr":  expression.NewEvery(bind(c(7)), expression.NewLT(x, c(5))),           // null
		"every-missing": expression.NewEvery(bind(c(value.MISSING_VALUE)), expression.NewLT(x, c(5))), // ""
		// FIRST x FOR x IN arr WHEN x > 2
		"first-hit":     expression.NewFirst(x, bind(arr(1, 2, 9, 4)), expression.NewGT(x, c(2))),  // 9
		"first-nomatch": expression.NewFirst(x, bind(arr(1, 2)), expression.NewGT(x, c(2))),        // "" (MISSING)
		"first-nowhen":  expression.NewFirst(x, bind(arr(7, 8)), nil),                              // 7
		"first-empty":   expression.NewFirst(x, bind(arr()), nil),                                  // "" (MISSING)
		"first-nonarr":  expression.NewFirst(x, bind(c(3)), nil),                                   // null
		// ARRAY x*10 FOR x IN arr WHEN x > 1
		"array-map":     expression.NewArray(expression.NewMult(x, c(10)), bind(arr(1, 2, 3)), expression.NewGT(x, c(1))), // [20,30]
		"array-nowhen":  expression.NewArray(x, bind(arr(1, 2)), nil),                              // [1,2]
		"array-empty":   expression.NewArray(x, bind(arr()), nil),                                  // []
		"array-allfilt": expression.NewArray(x, bind(arr(1, 2)), expression.NewGT(x, c(9))),        // []
		"array-nonarr":  expression.NewArray(x, bind(c(3)), nil),                                   // null
		"array-missing": expression.NewArray(x, bind(c(value.MISSING_VALUE)), nil),                 // ""
	}
	for name, e := range cases {
		want := cbqEval(t, e)
		got, ok := nativeEval(t, e)
		if !ok {
			t.Errorf("%s: did not optimize to native", name)
			continue
		}
		if got != want {
			t.Errorf("%s: native=%q, cbq=%q", name, got, want)
		}
	}
}

// TestObjectComprehensionDifferentialVsCBQ proves the native OBJECT comprehension
// is byte-identical to cbq over constant arrays: key-sorted output, last-wins
// dedup, WHEN filter, MISSING-value skip, and the MISSING/NULL/non-array/non-string-
// name/empty edges.
func TestObjectComprehensionDifferentialVsCBQ(t *testing.T) {
	c := func(v interface{}) expression.Expression { return expression.NewConstant(v) }
	arr := func(xs ...interface{}) expression.Expression {
		if xs == nil {
			xs = []interface{}{}
		}
		return expression.NewConstant(xs)
	}
	x := expression.NewIdentifier("x")
	bind := func(arrE expression.Expression) expression.Bindings {
		return expression.Bindings{expression.NewSimpleBinding("x", arrE)}
	}
	// OBJECT TO_STRING(x) : <val> FOR x IN <arr> [WHEN <when>] END
	objTS := func(arrE, val, when expression.Expression) expression.Expression {
		return expression.NewObject(expression.NewToString(x), val, bind(arrE), when)
	}

	cases := map[string]expression.Expression{
		"basic":       objTS(arr(3, 1, 2), x, nil),                                  // {"1":1,"2":2,"3":3} (key-sorted)
		"when":        objTS(arr(1, 2, 3), x, expression.NewGT(x, c(1))),            // {"2":2,"3":3}
		"dup-lastwin": objTS(arr(1, 1), expression.NewMult(x, c(10)), nil),          // {"1":10} (last wins, same here)
		"empty":       objTS(arr(), x, nil),                                         // {}
		"all-filt":    objTS(arr(1, 2), x, expression.NewGT(x, c(9))),               // {}
		"nonarr":      objTS(c(5), x, nil),                                          // null
		"missing":     objTS(c(value.MISSING_VALUE), x, nil),                        // "" (MISSING)
		"nullarr":     objTS(c(value.NULL_VALUE), x, nil),                           // null
		// non-string NAME -> NULL: OBJECT x:x (name is the number x, not a string)
		"nonstr-name": expression.NewObject(x, x, bind(arr(1, 2)), nil),             // null
	}
	for name, e := range cases {
		want := cbqEval(t, e)
		got, ok := nativeEval(t, e)
		if !ok {
			t.Errorf("OBJECT(%s): did not optimize to native", name)
			continue
		}
		if got != want {
			t.Errorf("OBJECT(%s): native=%q, cbq=%q", name, got, want)
		}
	}
}

// TestAnyComprehensionRowContext exercises the row-context cases the constant
// differential can't reach: object-field navigation on the bound var, correlation
// (predicate references the outer row), and nested ANY-in-ANY -- run through a real
// session over JSONL data.
func TestAnyComprehensionRowContext(t *testing.T) {
	dir := t.TempDir()
	d := filepath.Join(dir, "default", "t")
	if err := os.MkdirAll(d, 0o755); err != nil {
		t.Fatal(err)
	}
	body := `{"id":1,"nums":[1,2,9],"objs":[{"p":3},{"p":8}],"lim":5,"m":[[1,2],[8,9]]}` + "\n" +
		`{"id":2,"nums":[1,2,3],"objs":[{"p":1}],"lim":5,"m":[[1],[2,3]]}` + "\n" +
		`{"id":3,"nums":[],"lim":5}` + "\n" +
		`{"id":4,"nums":"x","lim":5}` + "\n"
	if err := os.WriteFile(filepath.Join(d, "d.jsonl"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	sess, err := OpenSession(dir, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}

	ids := func(t *testing.T, where string) []int {
		t.Helper()
		r, err := sess.Run("SELECT t.id FROM t WHERE " + where + " ORDER BY t.id")
		if err != nil {
			t.Fatalf("query %q: %v", where, err)
		}
		var got []int
		for _, row := range r.Rows {
			var m struct {
				ID int `json:"id"`
			}
			if e := json.Unmarshal(row, &m); e != nil {
				t.Fatalf("decode: %v", e)
			}
			got = append(got, m.ID)
		}
		return got
	}
	eq := func(t *testing.T, name string, got, want []int) {
		t.Helper()
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("%s: got %v, want %v", name, got, want)
		}
	}

	// object-field on the bound var (o.p), correlation (x > t.lim), nesting.
	eq(t, "field-nav", ids(t, `ANY o IN t.objs SATISFIES o.p > 5 END`), []int{1})
	eq(t, "correlated", ids(t, `ANY x IN t.nums SATISFIES x > t.lim END`), []int{1})
	eq(t, "nested", ids(t, `ANY r IN t.m SATISFIES (ANY v IN r SATISFIES v > 5 END) END`), []int{1})
	// edges: empty array (id 3) and non-array (id 4) are never TRUE.
	eq(t, "plain", ids(t, `ANY x IN t.nums SATISFIES x > 2 END`), []int{1, 2})
}

// TestArrayBuildersZeroAlloc asserts the per-row native array builds allocate
// nothing after warmup (splice into the reused bufPre; the GARBAGE MANDATE).
func TestArrayBuildersZeroAlloc(t *testing.T) {
	skipZeroAllocUnderRace(t)
	arr := base.Val([]byte(`[1,"two",{"a":1},[9]]`))
	elem := base.Val([]byte(`"x"`))
	vc := base.NewValComparer()
	sortArr := base.Val([]byte(`[3,1,"b","a",2]`))
	nestArr := base.Val([]byte(`[1,[2,[3,4]],5]`))
	depth2 := base.Val([]byte(`2`))

	for _, tc := range []struct {
		name string
		run  func(buf []byte) []byte
	}{
		{"ArrayAppend", func(buf []byte) []byte { o, _, _ := base.ArrayAppend(arr, elem, buf); return o }},
		{"ArrayPrepend", func(buf []byte) []byte { o, _, _ := base.ArrayPrepend(elem, arr, buf); return o }},
		{"ArrayConcat", func(buf []byte) []byte { o, _, _ := base.ArrayConcat(arr, arr, buf); return o }},
		{"ArraySort", func(buf []byte) []byte { o, _ := base.ArraySort(vc, sortArr, buf); return o }},
		{"ArrayReverse", func(buf []byte) []byte { o, _ := base.ArrayReverse(vc, arr, buf); return o }},
		{"ArrayFlatten", func(buf []byte) []byte { o, _, _ := base.ArrayFlatten(nestArr, depth2, buf); return o }},
	} {
		buf := tc.run(nil) // warm up the buffer
		n := testing.AllocsPerRun(2000, func() { buf = tc.run(buf) })
		if n != 0 {
			t.Errorf("base.%s: %v allocs/row after warmup; want 0", tc.name, n)
		}
	}
}

// TestObjectMutatorsDifferentialVsCBQ proves OBJECT_ADD / OBJECT_PUT (ternary) and
// OBJECT_REMOVE / OBJECT_CONCAT (2-arg) native lowering is byte-identical to cbq:
// the KEY-SORTED re-emit, ADD's no-overwrite, PUT's set (and MISSING-val removal),
// REMOVE's drop, CONCAT's obj2-wins merge, and every MISSING/NULL/non-object edge.
func TestObjectMutatorsDifferentialVsCBQ(t *testing.T) {
	c := func(v interface{}) expression.Expression { return expression.NewConstant(v) }
	obj := func(kvs map[string]interface{}) expression.Expression { return expression.NewConstant(kvs) }
	miss := c(value.MISSING_VALUE)
	null := c(value.NULL_VALUE)

	// --- ternary ADD / PUT: (obj, key, val) ---
	triCtors := map[string]func(a, b, d expression.Expression) expression.Expression{
		"object_add": func(a, b, d expression.Expression) expression.Expression { return expression.NewObjectAdd(a, b, d) },
		"object_put": func(a, b, d expression.Expression) expression.Expression { return expression.NewObjectPut(a, b, d) },
	}
	triCases := map[string][3]expression.Expression{
		"add-new":       {obj(map[string]interface{}{"a": 1}), c("b"), c(2)},
		"add-existing":  {obj(map[string]interface{}{"a": 1, "z": 9}), c("a"), c(3)},
		"into-empty":    {obj(map[string]interface{}{}), c("k"), c("x")},
		"str-val":       {obj(map[string]interface{}{"a": 1}), c("b"), c("hi")},
		"null-val":      {obj(map[string]interface{}{"a": 1}), c("b"), null},
		"nested-val":    {obj(map[string]interface{}{"a": 1}), c("b"), obj(map[string]interface{}{"n": 2})},
		"unsorted-keys": {obj(map[string]interface{}{"z": 1, "a": 2, "m": 3}), c("b"), c(4)},
		"missing-val":   {obj(map[string]interface{}{"a": 1}), c("a"), miss}, // PUT removes; ADD no-ops
		"missing-obj":   {miss, c("b"), c(2)},
		"missing-key":   {obj(map[string]interface{}{"a": 1}), miss, c(2)},
		"nonobj":        {c(5), c("b"), c(2)},
		"nonstr-key":    {obj(map[string]interface{}{"a": 1}), c(7), c(2)},
		"null-obj":      {null, c("b"), c(2)},
	}
	for fn, ctor := range triCtors {
		for on, args := range triCases {
			expr := ctor(args[0], args[1], args[2])
			want := cbqEval(t, expr)
			got, ok := nativeEval(t, expr)
			if !ok {
				t.Errorf("%s(%s): did not optimize to native", fn, on)
				continue
			}
			if got != want {
				t.Errorf("%s(%s): native=%q, cbq=%q", fn, on, got, want)
			}
		}
	}

	// --- 2-arg REMOVE (obj, key) / CONCAT (obj1, obj2) ---
	remCases := map[string][2]expression.Expression{
		"present":     {obj(map[string]interface{}{"a": 1, "b": 2}), c("a")},
		"absent":      {obj(map[string]interface{}{"a": 1}), c("z")},
		"last":        {obj(map[string]interface{}{"a": 1}), c("a")},
		"unsorted":    {obj(map[string]interface{}{"z": 1, "a": 2}), c("z")},
		"missing-obj": {miss, c("a")},
		"missing-key": {obj(map[string]interface{}{"a": 1}), miss},
		"nonobj":      {c(5), c("a")},
		"null-key":    {obj(map[string]interface{}{"a": 1}), null},
		"nonstr-key":  {obj(map[string]interface{}{"a": 1}), c(7)},
	}
	for on, args := range remCases {
		expr := expression.NewObjectRemove(args[0], args[1])
		want := cbqEval(t, expr)
		got, ok := nativeEval(t, expr)
		if !ok {
			t.Errorf("object_remove(%s): did not optimize to native", on)
			continue
		}
		if got != want {
			t.Errorf("object_remove(%s): native=%q, cbq=%q", on, got, want)
		}
	}

	concatCases := map[string][2]expression.Expression{
		"disjoint":   {obj(map[string]interface{}{"a": 1}), obj(map[string]interface{}{"b": 2})},
		"overlap":    {obj(map[string]interface{}{"a": 1, "b": 2}), obj(map[string]interface{}{"b": 9, "c": 3})},
		"l-empty":    {obj(map[string]interface{}{}), obj(map[string]interface{}{"a": 1})},
		"r-empty":    {obj(map[string]interface{}{"a": 1}), obj(map[string]interface{}{})},
		"both-empty": {obj(map[string]interface{}{}), obj(map[string]interface{}{})},
		"missing":    {obj(map[string]interface{}{"a": 1}), miss},
		"nonobj":     {obj(map[string]interface{}{"a": 1}), c(5)},
		"null-r":     {obj(map[string]interface{}{"a": 1}), null},
	}
	for on, args := range concatCases {
		expr := expression.NewObjectConcat(args[0], args[1])
		want := cbqEval(t, expr)
		got, ok := nativeEval(t, expr)
		if !ok {
			t.Errorf("object_concat(%s): did not optimize to native", on)
			continue
		}
		if got != want {
			t.Errorf("object_concat(%s): native=%q, cbq=%q", on, got, want)
		}
	}
}

// TestObjectMutatorsZeroAlloc asserts the per-row native OBJECT mutating builds
// allocate nothing after warmup (pooled KeyVals + reused bufPre; GARBAGE MANDATE).
func TestObjectMutatorsZeroAlloc(t *testing.T) {
	skipZeroAllocUnderRace(t)
	cmp := base.NewValComparer()
	o1 := base.Val([]byte(`{"gamma":1,"alpha":"x","beta":[1,2]}`))
	o2 := base.Val([]byte(`{"beta":9,"delta":{"k":1}}`))
	key := base.Val([]byte(`"gamma"`))
	nkey := base.Val([]byte(`"epsilon"`))
	val := base.Val([]byte(`42`))

	for _, tc := range []struct {
		name string
		run  func(buf []byte) []byte
	}{
		{"ObjectPut-overwrite", func(buf []byte) []byte { o, _, _ := base.ObjectPut(cmp, o1, key, val, buf); return o }},
		{"ObjectPut-new", func(buf []byte) []byte { o, _, _ := base.ObjectPut(cmp, o1, nkey, val, buf); return o }},
		{"ObjectAdd", func(buf []byte) []byte { o, _, _ := base.ObjectAdd(cmp, o1, nkey, val, buf); return o }},
		{"ObjectRemove", func(buf []byte) []byte { o, _, _ := base.ObjectRemove(cmp, o1, key, buf); return o }},
		{"ObjectConcat", func(buf []byte) []byte { o, _, _ := base.ObjectConcat(cmp, o1, o2, buf); return o }},
	} {
		buf := tc.run(nil) // warm up pool + buffer
		n := testing.AllocsPerRun(2000, func() { buf = tc.run(buf) })
		if n != 0 {
			t.Errorf("base.%s: %v allocs/row after warmup; want 0", tc.name, n)
		}
	}
}

func TestArrayMinMaxContainsDifferentialVsCBQ(t *testing.T) {
	c := func(v interface{}) expression.Expression { return expression.NewConstant(v) }
	arr := func(xs ...interface{}) expression.Expression {
		if xs == nil {
			xs = []interface{}{}
		}
		return expression.NewConstant(xs)
	}

	// Unary: array_min / array_max over collation-mixed arrays.
	mkU := map[string]func(expression.Expression) expression.Expression{
		"array_min": func(e expression.Expression) expression.Expression { return expression.NewArrayMin(e) },
		"array_max": func(e expression.Expression) expression.Expression { return expression.NewArrayMax(e) },
	}
	uOps := map[string]expression.Expression{
		"nums": arr(3, 1, 2), "floats": arr(2.5, 1.5), "strs": arr("b", "a", "c"),
		"mixed": arr(2, "a", true, 1), "withnull": arr(nil, 3, nil, 1),
		"allnull": arr(nil, nil), "empty": arr(), "one": arr(5),
		"nonarr": c(5), "null": c(value.NULL_VALUE), "missing": c(value.MISSING_VALUE),
	}
	for fn, ctor := range mkU {
		for on, op := range uOps {
			expr := ctor(op)
			want := cbqEval(t, expr)
			got, ok := nativeEval(t, expr)
			if !ok {
				t.Errorf("%s(%s): did not optimize", fn, on)
			} else if got != want {
				t.Errorf("%s(%s): native=%q, cbq=%q", fn, on, got, want)
			}
		}
	}

	// Binary: array_contains / array_position (arr, v).
	mkB := map[string]func(a, b expression.Expression) expression.Expression{
		"array_contains": func(a, b expression.Expression) expression.Expression { return expression.NewArrayContains(a, b) },
		"array_position": func(a, b expression.Expression) expression.Expression { return expression.NewArrayPosition(a, b) },
	}
	bPairs := []struct{ a, b expression.Expression }{
		{arr(1, 2, 3), c(2)}, {arr(1, 2, 3), c(9)}, {arr("a", "b"), c("b")},
		{arr(1, 2.0, 3), c(2)}, {arr(), c(1)}, {arr(1, nil, 2), c(2)},
		{c(5), c(1)}, {arr(1, 2), c(value.NULL_VALUE)}, {c(value.MISSING_VALUE), c(1)},
		{arr(1), c(value.MISSING_VALUE)},
	}
	for fn, ctor := range mkB {
		for i, p := range bPairs {
			expr := ctor(p.a, p.b)
			want := cbqEval(t, expr)
			got, ok := nativeEval(t, expr)
			if !ok {
				t.Errorf("%s pair[%d]: did not optimize", fn, i)
			} else if got != want {
				t.Errorf("%s pair[%d]: native=%q, cbq=%q", fn, i, got, want)
			}
		}
	}
}

func TestTypeConvDifferentialVsCBQ(t *testing.T) {
	c := func(v interface{}) expression.Expression { return expression.NewConstant(v) }

	mk := map[string]func(expression.Expression) expression.Expression{
		"to_boolean": func(e expression.Expression) expression.Expression { return expression.NewToBoolean(e) },
		"to_string":  func(e expression.Expression) expression.Expression { return expression.NewToString(e) },
		"to_number":  func(e expression.Expression) expression.Expression { return expression.NewToNumber(e) },
	}
	// Every value kind + edge strings (parseable int/float, leading zeros, junk,
	// empty), so to_* branches all fire and must agree with cbq.
	operands := map[string]expression.Expression{
		"missing": c(value.MISSING_VALUE), "null": c(value.NULL_VALUE),
		"true": c(true), "false": c(false),
		"int": c(42), "negint": c(-7), "float": c(2.5), "wholefloat": c(5.0), "zero": c(0),
		"str": c("hi"), "emptystr": c(""), "numstr": c("42"), "floatstr": c("2.5"),
		"leadzero": c("007"), "junkstr": c("abc"),
		"arr":      expression.NewConstant([]interface{}{1, 2}),
		"emptyarr": expression.NewConstant([]interface{}{}),
		"obj":      expression.NewConstant(map[string]interface{}{"a": 1}),
	}

	for fn, ctor := range mk {
		for on, operand := range operands {
			expr := ctor(operand)
			want := cbqEval(t, expr)
			got, ok := nativeEval(t, expr)
			if !ok {
				t.Errorf("%s(%s): did not optimize", fn, on)
				continue
			}
			if got != want {
				t.Errorf("%s(%s): native=%q, cbq=%q", fn, on, got, want)
			}
		}
	}
}

func TestStrBinaryDifferentialVsCBQ(t *testing.T) {
	c := func(v interface{}) expression.Expression { return expression.NewConstant(v) }

	mk := map[string]func(a, b expression.Expression) expression.Expression{
		"contains":  func(a, b expression.Expression) expression.Expression { return expression.NewContains(a, b) },
		"position0": func(a, b expression.Expression) expression.Expression { return expression.NewPosition0(a, b) },
		"position1": func(a, b expression.Expression) expression.Expression { return expression.NewPosition1(a, b) },
	}
	// Substring present/absent/at-start/empty, unicode, escapes; plus non-string
	// and MISSING operands (-> NULL / MISSING).
	pairs := []struct{ a, b expression.Expression }{
		{c("hello world"), c("o w")}, {c("hello"), c("z")}, {c("hello"), c("he")},
		{c("hello"), c("")}, {c("café x"), c("x")}, {c("a\"b\nc"), c("b\nc")},
		{c("abc"), c("abc")}, {c(""), c("x")},
		{c(value.NULL_VALUE), c("x")}, {c("x"), c(5)}, {c(5), c("x")},
		{c(value.MISSING_VALUE), c("x")},
	}

	for fn, ctor := range mk {
		for i, p := range pairs {
			expr := ctor(p.a, p.b)
			want := cbqEval(t, expr)
			got, ok := nativeEval(t, expr)
			if !ok {
				t.Errorf("%s pair[%d]: did not optimize", fn, i)
				continue
			}
			if got != want {
				t.Errorf("%s pair[%d]: native=%q, cbq=%q", fn, i, got, want)
			}
		}
	}
}

func TestStrUnaryDifferentialVsCBQ(t *testing.T) {
	c := func(v interface{}) expression.Expression { return expression.NewConstant(v) }

	mk := map[string]func(expression.Expression) expression.Expression{
		"upper":   func(e expression.Expression) expression.Expression { return expression.NewUpper(e) },
		"lower":   func(e expression.Expression) expression.Expression { return expression.NewLower(e) },
		"length":  func(e expression.Expression) expression.Expression { return expression.NewLength(e) },
		"title":   func(e expression.Expression) expression.Expression { return expression.NewTitle(e) },
		"trim":    func(e expression.Expression) expression.Expression { return expression.NewTrim(e) },
		"ltrim":   func(e expression.Expression) expression.Expression { return expression.NewLTrim(e) },
		"rtrim":   func(e expression.Expression) expression.Expression { return expression.NewRTrim(e) },
		"reverse": func(e expression.Expression) expression.Expression { return expression.NewReverse(e) },
	}
	// Strings spanning case, unicode, escapes, empty; whitespace at each end
	// (space/tab/newline/carriage-return -- see the \f note below); plus
	// non-string operands (NULL/number/bool/array) which must yield NULL, and
	// MISSING via a c(MISSING).
	//
	// NB: the operands deliberately avoid a SURVIVING formfeed (\f) / backspace
	// (\b) in trimmed output: n1k1's EncodeStr (stdlib encoding/json) escapes them
	// as the two-char \f / \b, whereas cbq's value encoder emits the six-char
	// \u000c / \u0008 form -- a
	// pre-existing, cosmetic encoder difference (both are valid JSON), unrelated to
	// trim. \f IS still in the cbq cutset (base.strWhitespace) and is trimmed
	// correctly; we just can't byte-compare a surviving one here.
	operands := map[string]expression.Expression{
		"mixed": c("Hello World"), "upperstr": c("ABC"), "lowerstr": c("abc"),
		"empty": c(""), "unicode": c("café ☃"), "escapes": c("a\"b\n\tc"),
		"digits": c("12ab"), "null": c(value.NULL_VALUE), "num": c(5),
		"bool": c(true), "arr": expression.NewConstant([]interface{}{1}),
		"missing": c(value.MISSING_VALUE),
		"padded":  c("  hi  "), "tabnl": c("\t\n hi \r"),
		"innerkeep": c("  a b  "), "allws": c(" \t\r\n"),
	}

	for fn, ctor := range mk {
		for on, operand := range operands {
			expr := ctor(operand)
			want := cbqEval(t, expr)
			got, ok := nativeEval(t, expr)
			if !ok {
				t.Errorf("%s(%s): did not optimize", fn, on)
				continue
			}
			if got != want {
				t.Errorf("%s(%s): native=%q, cbq=%q", fn, on, got, want)
			}
		}
	}
}

func TestStrReplaceDifferentialVsCBQ(t *testing.T) {
	c := func(v interface{}) expression.Expression { return expression.NewConstant(v) }
	null := c(value.NULL_VALUE)
	miss := c(value.MISSING_VALUE)

	cases := []struct {
		name         string
		s, old, repl expression.Expression
	}{
		{"all", c("banana"), c("a"), c("o")},       // "bonono"
		{"none", c("abc"), c("x"), c("y")},         // unchanged
		{"del", c("a-b-c"), c("-"), c("")},         // remove sep -> "abc"
		{"empty-old", c("ab"), c(""), c("-")},      // insert between every char (Go semantics)
		{"multichar", c("aXbXc"), c("X"), c("__")}, // "a__b__c"
		{"unicode", c("café"), c("é"), c("e")},     // "cafe"
		{"empty-str", c(""), c("a"), c("b")},       // "" stays ""
		{"miss-s", miss, c("a"), c("b")},           // MISSING
		{"miss-old", c("ab"), miss, c("b")},        // MISSING dominant
		{"null-s", null, c("a"), c("b")},           // NULL
		{"null-repl", c("ab"), c("a"), null},       // NULL
		{"num-old", c("ab"), c(5), c("b")},         // non-string -> NULL
		{"miss-over-null", miss, null, c("b")},     // MISSING dominates NULL
	}

	for _, tc := range cases {
		expr := expression.NewReplace(tc.s, tc.old, tc.repl)
		want := cbqEval(t, expr)
		got, ok := nativeEval(t, expr)
		if !ok {
			t.Errorf("replace(%s): did not optimize", tc.name)
			continue
		}
		if got != want {
			t.Errorf("replace(%s): native=%q, cbq=%q", tc.name, got, want)
		}
	}
}

func TestStrSubstrDifferentialVsCBQ(t *testing.T) {
	c := func(v interface{}) expression.Expression { return expression.NewConstant(v) }
	null := c(value.NULL_VALUE)
	miss := c(value.MISSING_VALUE)
	s := c("hello") // len 5

	cases := []struct {
		name string
		expr expression.Expression
	}{
		// SUBSTR0 (0-based), 2-arg.
		{"s0-2-mid", expression.NewSubstr0(s, c(1))},       // "ello"
		{"s0-2-zero", expression.NewSubstr0(s, c(0))},      // "hello"
		{"s0-2-neg", expression.NewSubstr0(s, c(-2))},      // "lo"
		{"s0-2-oor", expression.NewSubstr0(s, c(5))},       // NULL (pos==len)
		{"s0-2-negoor", expression.NewSubstr0(s, c(-9))},   // NULL
		{"s0-2-frac", expression.NewSubstr0(s, c(1.5))},    // NULL (non-integral)
		{"s0-2-null", expression.NewSubstr0(null, c(1))},   // NULL (non-string)
		{"s0-2-posnull", expression.NewSubstr0(s, null)},   // NULL
		{"s0-2-miss", expression.NewSubstr0(miss, c(1))},   // MISSING
		{"s0-2-posmiss", expression.NewSubstr0(s, miss)},   // MISSING
		{"s0-2-strnum", expression.NewSubstr0(c(5), c(1))}, // NULL (non-string)
		// SUBSTR0 (0-based), 3-arg.
		{"s0-3-mid", expression.NewSubstr0(s, c(1), c(3))},       // "ell"
		{"s0-3-clamp", expression.NewSubstr0(s, c(3), c(9))},     // "lo" (len clamped)
		{"s0-3-zerolen", expression.NewSubstr0(s, c(1), c(0))},   // ""
		{"s0-3-neglen", expression.NewSubstr0(s, c(1), c(-1))},   // NULL
		{"s0-3-negpos", expression.NewSubstr0(s, c(-3), c(2))},   // "ll"
		{"s0-3-lenmiss", expression.NewSubstr0(s, c(1), miss)},   // MISSING
		{"s0-3-lennull", expression.NewSubstr0(s, c(1), null)},   // NULL
		{"s0-3-lenfrac", expression.NewSubstr0(s, c(1), c(2.5))}, // NULL
		// SUBSTR1 (1-based).
		{"s1-2-first", expression.NewSubstr1(s, c(1))},       // "hello"
		{"s1-2-mid", expression.NewSubstr1(s, c(2))},         // "ello"
		{"s1-2-zero", expression.NewSubstr1(s, c(0))},        // cbq: pos stays 0 -> "hello"
		{"s1-2-neg", expression.NewSubstr1(s, c(-1))},        // "o"
		{"s1-3-mid", expression.NewSubstr1(s, c(2), c(2))},   // "el"
		{"s1-3-first", expression.NewSubstr1(s, c(1), c(3))}, // "hel"
	}

	for _, tc := range cases {
		want := cbqEval(t, tc.expr)
		got, ok := nativeEval(t, tc.expr)
		if !ok {
			t.Errorf("%s: did not optimize", tc.name)
			continue
		}
		if got != want {
			t.Errorf("%s: native=%q, cbq=%q", tc.name, got, want)
		}
	}
}

func TestStrSplitDifferentialVsCBQ(t *testing.T) {
	c := func(v interface{}) expression.Expression { return expression.NewConstant(v) }
	null := c(value.NULL_VALUE)
	miss := c(value.MISSING_VALUE)

	cases := []struct {
		name string
		expr expression.Expression
	}{
		// 1-arg: split on whitespace (strings.Fields).
		{"f-words", expression.NewSplit(c("a b  c"))}, // ["a","b","c"] (runs collapse)
		{"f-lead", expression.NewSplit(c("  x y "))},  // ["x","y"]
		{"f-one", expression.NewSplit(c("hello"))},    // ["hello"]
		{"f-empty", expression.NewSplit(c(""))},       // []
		{"f-allws", expression.NewSplit(c("  \t "))},  // []
		{"f-null", expression.NewSplit(null)},         // NULL
		{"f-miss", expression.NewSplit(miss)},         // MISSING
		{"f-num", expression.NewSplit(c(5))},          // NULL (non-string)
		// 2-arg: split on explicit sep (strings.Split).
		{"s-comma", expression.NewSplit(c("a,b,c"), c(","))},        // ["a","b","c"]
		{"s-adjacent", expression.NewSplit(c("a,,b"), c(","))},      // ["a","","b"]
		{"s-nosep", expression.NewSplit(c("abc"), c("-"))},          // ["abc"]
		{"s-empty-str", expression.NewSplit(c(""), c(","))},         // [""]
		{"s-empty-sep", expression.NewSplit(c("abc"), c(""))},       // ["a","b","c"] (per-rune)
		{"s-multichar", expression.NewSplit(c("aXXbXXc"), c("XX"))}, // ["a","b","c"]
		{"s-miss", expression.NewSplit(miss, c(","))},               // MISSING
		{"s-sepmiss", expression.NewSplit(c("a,b"), miss)},          // MISSING
		{"s-null", expression.NewSplit(null, c(","))},               // NULL
		{"s-sepnull", expression.NewSplit(c("a,b"), null)},          // NULL
		{"s-sepnum", expression.NewSplit(c("a,b"), c(5))},           // NULL (non-string)
		{"s-miss-over-null", expression.NewSplit(null, miss)},       // MISSING dominates
	}

	for _, tc := range cases {
		want := cbqEval(t, tc.expr)
		got, ok := nativeEval(t, tc.expr)
		if !ok {
			t.Errorf("%s: did not optimize", tc.name)
			continue
		}
		if got != want {
			t.Errorf("%s: native=%q, cbq=%q", tc.name, got, want)
		}
	}
}

func TestStrPadDifferentialVsCBQ(t *testing.T) {
	c := func(v interface{}) expression.Expression { return expression.NewConstant(v) }
	null := c(value.NULL_VALUE)
	miss := c(value.MISSING_VALUE)
	s := c("ab")

	cases := []struct {
		name string
		expr expression.Expression
	}{
		// LPAD 2-arg (default space pad).
		{"l2-grow", expression.NewLPad(s, c(5))},  // "   ab"
		{"l2-same", expression.NewLPad(s, c(2))},  // "ab"
		{"l2-trunc", expression.NewLPad(s, c(1))}, // "a" (first l bytes)
		{"l2-zero", expression.NewLPad(s, c(0))},  // ""
		// RPAD 2-arg.
		{"r2-grow", expression.NewRPad(s, c(5))},  // "ab   "
		{"r2-trunc", expression.NewRPad(s, c(1))}, // "a"
		// LPAD 3-arg (explicit pad).
		{"l3-star", expression.NewLPad(s, c(5), c("*"))},     // "***ab"
		{"l3-multi", expression.NewLPad(s, c(7), c("xy"))},   // "xyxyxab"
		{"l3-partial", expression.NewLPad(s, c(6), c("xy"))}, // "xyxyab"
		// RPAD 3-arg.
		{"r3-star", expression.NewRPad(s, c(5), c("*"))},   // "ab***"
		{"r3-multi", expression.NewRPad(s, c(7), c("xy"))}, // "abxyxyx"
		// Guards.
		{"neg-len", expression.NewLPad(s, c(-1))},          // NULL
		{"frac-len", expression.NewLPad(s, c(2.5))},        // NULL
		{"empty-pad", expression.NewLPad(s, c(5), c(""))},  // NULL
		{"str-num", expression.NewLPad(c(5), c(3))},        // NULL (non-string)
		{"pad-num", expression.NewLPad(s, c(5), c(9))},     // NULL (non-string pad)
		{"len-str", expression.NewLPad(s, c("x"))},         // NULL (non-number len)
		{"miss-s", expression.NewLPad(miss, c(5))},         // MISSING
		{"miss-len", expression.NewLPad(s, miss)},          // MISSING
		{"miss-pad", expression.NewLPad(s, c(5), miss)},    // MISSING
		{"null-s", expression.NewLPad(null, c(5))},         // NULL
		{"miss-over-null", expression.NewLPad(null, miss)}, // MISSING dominates
	}

	for _, tc := range cases {
		want := cbqEval(t, tc.expr)
		got, ok := nativeEval(t, tc.expr)
		if !ok {
			t.Errorf("%s: did not optimize", tc.name)
			continue
		}
		if got != want {
			t.Errorf("%s: native=%q, cbq=%q", tc.name, got, want)
		}
	}
}

func TestRoundTruncDifferentialVsCBQ(t *testing.T) {
	c := func(v interface{}) expression.Expression { return expression.NewConstant(v) }
	null := c(value.NULL_VALUE)
	miss := c(value.MISSING_VALUE)

	cases := []struct {
		name string
		expr expression.Expression
	}{
		// ROUND 1-arg (prec 0), round-half-to-even.
		{"r1-up", expression.NewRound(c(2.7))},          // 3
		{"r1-down", expression.NewRound(c(2.3))},        // 2
		{"r1-half-even-2", expression.NewRound(c(2.5))}, // 2 (half to even)
		{"r1-half-even-4", expression.NewRound(c(3.5))}, // 4
		{"r1-neg", expression.NewRound(c(-2.5))},        // -2
		{"r1-int", expression.NewRound(c(5))},           // 5
		// ROUND 2-arg (precision).
		{"r2-2dp", expression.NewRound(c(3.14159), c(2))},  // 3.14
		{"r2-1dp", expression.NewRound(c(2.345), c(2))},    // 2.34 / 2.35 (float)
		{"r2-negp", expression.NewRound(c(1234.5), c(-2))}, // 1200
		{"r2-zero", expression.NewRound(c(2.7), c(0))},     // 3
		// TRUNC.
		{"t1-pos", expression.NewTrunc(c(2.9))},            // 2
		{"t1-neg", expression.NewTrunc(c(-2.9))},           // -2
		{"t2-2dp", expression.NewTrunc(c(3.14959), c(2))},  // 3.14
		{"t2-negp", expression.NewTrunc(c(1567.0), c(-2))}, // 1500
		// Guards.
		{"r-str", expression.NewRound(c("x"))},                 // NULL
		{"r-null", expression.NewRound(null)},                  // NULL
		{"r-miss", expression.NewRound(miss)},                  // MISSING
		{"r2-precstr", expression.NewRound(c(2.5), c("x"))},    // NULL
		{"r2-precfrac", expression.NewRound(c(2.5), c(1.5))},   // NULL
		{"r2-precmiss", expression.NewRound(c(2.5), miss)},     // MISSING
		{"r2-precnull", expression.NewRound(c(2.5), null)},     // NULL
		{"r2-miss-over-null", expression.NewRound(null, miss)}, // MISSING dominates
	}

	for _, tc := range cases {
		want := cbqEval(t, tc.expr)
		got, ok := nativeEval(t, tc.expr)
		if !ok {
			t.Errorf("%s: did not optimize", tc.name)
			continue
		}
		if got != want {
			t.Errorf("%s: native=%q, cbq=%q", tc.name, got, want)
		}
	}
}

func TestDatePartMillisDifferentialVsCBQ(t *testing.T) {
	c := func(v interface{}) expression.Expression { return expression.NewConstant(v) }
	null := c(value.NULL_VALUE)
	miss := c(value.MISSING_VALUE)
	// A fixed epoch-millis with a non-zero millisecond component. Native and cbq
	// both read time.Unix in the SAME process-local zone, so results match
	// regardless of what that zone is -- cbq is the oracle.
	ms := c(1500000000123)

	// All known components (case-insensitive) + an unknown one (-> NULL).
	parts := []string{
		"millennium", "century", "decade", "year", "quarter", "month",
		"calendar_month", "day", "hour", "minute", "second", "millisecond",
		"week", "day_of_year", "doy", "day_of_week", "dow", "iso_week",
		"iso_year", "iso_dow", "timezone", "timezone_hour", "timezone_minute",
		"YEAR", "Day", "bogus_part",
	}
	for _, p := range parts {
		expr := expression.NewDatePartMillis(ms, c(p))
		want := cbqEval(t, expr)
		got, ok := nativeEval(t, expr)
		if !ok {
			t.Errorf("date_part_millis(_, %q): did not optimize", p)
			continue
		}
		if got != want {
			t.Errorf("date_part_millis(_, %q): native=%q, cbq=%q", p, got, want)
		}
	}

	// Edge operands.
	edges := []struct {
		name string
		expr expression.Expression
	}{
		{"neg-millis", expression.NewDatePartMillis(c(-1000), c("year"))},
		{"frac-ms", expression.NewDatePartMillis(c(1234), c("millisecond"))},
		{"oor-hi", expression.NewDatePartMillis(c(999999999999999.0), c("year"))}, // > max -> NULL
		{"millis-str", expression.NewDatePartMillis(c("x"), c("year"))},           // NULL
		{"millis-null", expression.NewDatePartMillis(null, c("year"))},            // NULL
		{"part-num", expression.NewDatePartMillis(ms, c(5))},                      // NULL
		{"millis-miss", expression.NewDatePartMillis(miss, c("year"))},            // MISSING
		{"part-miss", expression.NewDatePartMillis(ms, miss)},                     // MISSING
	}
	for _, tc := range edges {
		want := cbqEval(t, tc.expr)
		got, ok := nativeEval(t, tc.expr)
		if !ok {
			t.Errorf("%s: did not optimize", tc.name)
			continue
		}
		if got != want {
			t.Errorf("%s: native=%q, cbq=%q", tc.name, got, want)
		}
	}
}

func TestDateAddMillisDifferentialVsCBQ(t *testing.T) {
	c := func(v interface{}) expression.Expression { return expression.NewConstant(v) }
	null := c(value.NULL_VALUE)
	miss := c(value.MISSING_VALUE)
	// A fixed epoch-millis with a non-zero millisecond component. Native and cbq
	// both read time.Unix in the SAME process-local zone -- cbq is the oracle.
	ms := c(1500000000123)

	// All known components (case-insensitive) crossed with a few interval counts
	// (incl. 0, negative, and a large n) + an unknown component (-> NULL).
	parts := []string{
		"millennium", "century", "decade", "year", "quarter", "month",
		"calendar_month", "week", "day", "hour", "minute", "second",
		"millisecond", "YEAR", "Calendar_Month", "bogus_part",
	}
	ns := []interface{}{0, 1, -1, 3, -13, 400, 1000000}
	for _, p := range parts {
		for _, n := range ns {
			expr := expression.NewDateAddMillis(ms, c(n), c(p))
			want := cbqEval(t, expr)
			got, ok := nativeEval(t, expr)
			if !ok {
				t.Errorf("date_add_millis(_, %v, %q): did not optimize", n, p)
				continue
			}
			if got != want {
				t.Errorf("date_add_millis(_, %v, %q): native=%q, cbq=%q", n, p, got, want)
			}
		}
	}

	// calendar_month last-day-of-month rounding: a date on the 31st, +/- months.
	// (1612051200000 ~ 2021-01-31 UTC; the local-zone day may differ but cbq is
	// still the oracle so parity holds regardless.)
	jan31 := c(1612051200000)
	for _, n := range []interface{}{1, 2, -1, 12, 13} {
		expr := expression.NewDateAddMillis(jan31, c(n), c("calendar_month"))
		want := cbqEval(t, expr)
		got, ok := nativeEval(t, expr)
		if !ok {
			t.Errorf("date_add_millis(jan31, %v, calendar_month): did not optimize", n)
			continue
		}
		if got != want {
			t.Errorf("date_add_millis(jan31, %v, calendar_month): native=%q, cbq=%q", n, got, want)
		}
	}

	// Edge operands.
	edges := []struct {
		name string
		expr expression.Expression
	}{
		{"neg-millis", expression.NewDateAddMillis(c(-1000), c(1), c("year"))},
		{"frac-n", expression.NewDateAddMillis(ms, c(1.5), c("year"))},                     // non-integral -> NULL
		{"oor-millis", expression.NewDateAddMillis(c(999999999999999.0), c(1), c("year"))}, // > max -> NULL
		{"overflow", expression.NewDateAddMillis(ms, c(1000000000), c("year"))},            // result overflow -> NULL
		{"millis-str", expression.NewDateAddMillis(c("x"), c(1), c("year"))},               // NULL
		{"n-str", expression.NewDateAddMillis(ms, c("x"), c("year"))},                      // NULL
		{"part-num", expression.NewDateAddMillis(ms, c(1), c(5))},                          // NULL
		{"millis-null", expression.NewDateAddMillis(null, c(1), c("year"))},                // NULL
		{"n-null", expression.NewDateAddMillis(ms, null, c("year"))},                       // NULL
		{"part-null", expression.NewDateAddMillis(ms, c(1), null)},                         // NULL
		{"millis-miss", expression.NewDateAddMillis(miss, c(1), c("year"))},                // MISSING
		{"n-miss", expression.NewDateAddMillis(ms, miss, c("year"))},                       // MISSING
		{"part-miss", expression.NewDateAddMillis(ms, c(1), miss)},                         // MISSING
	}
	for _, tc := range edges {
		want := cbqEval(t, tc.expr)
		got, ok := nativeEval(t, tc.expr)
		if !ok {
			t.Errorf("%s: did not optimize", tc.name)
			continue
		}
		if got != want {
			t.Errorf("%s: native=%q, cbq=%q", tc.name, got, want)
		}
	}
}

func TestPredicateDifferentialVsCBQ(t *testing.T) {
	c := func(v interface{}) expression.Expression { return expression.NewConstant(v) }

	// Representable operands (MISSING isn't a JSON constant -- covered in
	// engine/expr_pred_test.go).
	num := c(3)
	str := c("x")
	null := c(value.NULL_VALUE)
	tru := c(true)
	fls := c(false)
	zero := c(0)

	cases := []struct {
		name string
		expr expression.Expression
	}{
		{"isnull-value", expression.NewIsNull(num)},
		{"isnull-null", expression.NewIsNull(null)},
		{"isnotnull-value", expression.NewIsNotNull(str)},
		{"isnotnull-null", expression.NewIsNotNull(null)},
		{"ismissing-value", expression.NewIsMissing(num)},
		{"ismissing-null", expression.NewIsMissing(null)},
		{"isnotmissing-value", expression.NewIsNotMissing(num)},
		{"isvalued-value", expression.NewIsValued(num)},
		{"isvalued-null", expression.NewIsValued(null)},
		{"isnotvalued-value", expression.NewIsNotValued(str)},
		{"isnotvalued-null", expression.NewIsNotValued(null)},
		{"not-true", expression.NewNot(tru)},
		{"not-false", expression.NewNot(fls)},
		{"not-null", expression.NewNot(null)},
		{"not-num", expression.NewNot(num)},
		{"not-zero", expression.NewNot(zero)},
	}

	for _, tc := range cases {
		want := cbqEval(t, tc.expr)
		got, ok := nativeEval(t, tc.expr)
		if !ok {
			t.Errorf("%s: expression did not optimize to native path", tc.name)
			continue
		}
		if got != want {
			t.Errorf("%s: native=%q, cbq=%q", tc.name, got, want)
		}
	}
}

// TestRegexpDifferentialVsCBQ proves REGEXP_CONTAINS / REGEXP_LIKE native
// lowering: a CONSTANT, compilable pattern lowers to the native regexp head
// (base.RegexpMatchStr), matching cbq byte-for-byte; a DYNAMIC or INVALID-constant
// pattern must stay BOXED (not lowered), so cbq's per-row-recompile / runtime-error
// behavior is preserved. Both operands here are constants -- native lowering is
// tried before const-fold, so these hit the regexp handler (asserted via the tree
// head), not cbq's static fold.
func TestRegexpDifferentialVsCBQ(t *testing.T) {
	parse := func(s string) expression.Expression {
		e, err := parser.Parse(s)
		if err != nil {
			t.Fatalf("parse %q: %v", s, err)
		}
		return e
	}

	native := []struct{ name, head, expr string }{
		{"contains-digits", "regexp_contains", `REGEXP_CONTAINS("hello123", "[0-9]+")`},
		{"contains-no", "regexp_contains", `REGEXP_CONTAINS("hello", "[0-9]+")`},
		{"contains-anchor", "regexp_contains", `REGEXP_CONTAINS("abcxyz", "^abc")`},
		{"contains-alt", "regexp_contains", `REGEXP_CONTAINS("warn: disk", "error|warn")`},
		{"contains-backslash", "regexp_contains", `REGEXP_CONTAINS("a.b", "a\\.b")`},
		{"contains-backslash-no", "regexp_contains", `REGEXP_CONTAINS("axb", "a\\.b")`},
		{"contains-empty-pat", "regexp_contains", `REGEXP_CONTAINS("anything", "")`},
		{"contains-null-src", "regexp_contains", `REGEXP_CONTAINS(null, "x")`},
		{"contains-num-src", "regexp_contains", `REGEXP_CONTAINS(5, "[0-9]")`},
		{"like-full-yes", "regexp_like", `REGEXP_LIKE("12345", "[0-9]+")`},
		{"like-full-no", "regexp_like", `REGEXP_LIKE("a12345", "[0-9]+")`},
		{"like-literal", "regexp_like", `REGEXP_LIKE("abc", "abc")`},
		{"like-partial-no", "regexp_like", `REGEXP_LIKE("xabcx", "abc")`},
		{"like-null-src", "regexp_like", `REGEXP_LIKE(null, ".*")`},
		{"like-num-src", "regexp_like", `REGEXP_LIKE(5, ".*")`},
	}
	for _, tc := range native {
		e := parse(tc.expr)
		var buf bytes.Buffer
		params, ok := ExprTreeOptimize(nil, e, &buf, false)
		if !ok {
			t.Errorf("%s: %q did not optimize to native", tc.name, tc.expr)
			continue
		}
		if head, _ := params[0].(string); head != tc.head {
			t.Errorf("%s: expected native head %q, got %v (const-folded/boxed, not the regexp handler)",
				tc.name, tc.head, params[0])
			continue
		}
		want := cbqEval(t, e)
		got, gotOk := nativeEval(t, e)
		if !gotOk {
			t.Errorf("%s: nativeEval boxed unexpectedly", tc.name)
			continue
		}
		if got != want {
			t.Errorf("%s: native=%q, cbq=%q", tc.name, got, want)
		}
	}

	// A dynamic pattern (row-dependent identifier) or an invalid constant pattern
	// must NOT lower -- ExprTreeOptimize returns ok=false (boxed to cbq).
	boxed := []struct{ name, expr string }{
		{"dynamic-pattern", `REGEXP_CONTAINS("hello", p)`},
		{"dynamic-pattern-like", `REGEXP_LIKE("hello", p)`},
		{"invalid-const-pattern", `REGEXP_CONTAINS("hello", "[")`},
		{"invalid-const-like", `REGEXP_LIKE("hello", "(")`},
		{"nonstring-const-pattern", `REGEXP_CONTAINS("hello", 5)`},
	}
	for _, tc := range boxed {
		e := parse(tc.expr)
		var buf bytes.Buffer
		if params, ok := ExprTreeOptimize(nil, e, &buf, false); ok {
			if head, _ := params[0].(string); head == "regexp_contains" || head == "regexp_like" {
				t.Errorf("%s: %q should stay boxed, but lowered to native %q", tc.name, tc.expr, head)
			}
		}
	}
}

// TestRegexpMatchZeroAlloc asserts the per-row native regexp eval allocates
// nothing after the one-time (first-row) pattern compile -- the GARBAGE MANDATE.
func TestRegexpMatchZeroAlloc(t *testing.T) {
	skipZeroAllocUnderRace(t)
	val := base.Val([]byte(`"2026-07-08T10:34:39 error: disk full"`))
	src := "error|warn|fatal"

	var re base.Regexp
	_ = base.RegexpMatchStr(val, src, &re) // warm up: compiles once

	n := testing.AllocsPerRun(2000, func() {
		_ = base.RegexpMatchStr(val, src, &re)
	})
	if n != 0 {
		t.Errorf("RegexpMatchStr: %v allocs/row after warmup; want 0", n)
	}
}

// TestLikeContainsRewrite proves IDEA-0003: `x LIKE '%lit%'` with a constant
// pattern lowers to the native CONTAINS head (off the boxed lane) and matches cbq
// byte-for-byte, INCLUDING the MISSING/NULL/non-string rows; the rewritten needle
// is also extractable by engine.PrefilterLiteral so the predicate index can prune.
// Prefix/suffix/exact/interior-wildcard/underscore/empty/custom-escape/dynamic
// forms must stay BOXED (no regression -- LIKE was never natively lowered before).
func TestLikeContainsRewrite(t *testing.T) {
	parse := func(s string) expression.Expression {
		e, err := parser.Parse(s)
		if err != nil {
			t.Fatalf("parse %q: %v", s, err)
		}
		return e
	}

	// Lowers to native contains AND agrees with cbq. like-special proves the
	// QuoteMeta equivalence: `.` `*` in the needle are literal, not regex metas.
	native := []struct{ name, expr string }{
		{"basic-yes", `"hello" LIKE "%ell%"`},
		{"basic-no", `"hello" LIKE "%xyz%"`},
		{"word", `"error: disk full" LIKE "%disk%"`},
		{"edge-match", `"hello" LIKE "%hello%"`},
		{"special-literal", `"a.b*c" LIKE "%.b*%"`},
		{"null-src", `null LIKE "%x%"`},
		{"num-src", `5 LIKE "%x%"`},
		{"bool-src", `true LIKE "%x%"`},
	}
	for _, tc := range native {
		e := parse(tc.expr)
		var buf bytes.Buffer
		params, ok := ExprTreeOptimize(nil, e, &buf, false)
		if !ok {
			t.Errorf("%s: %q did not optimize to native", tc.name, tc.expr)
			continue
		}
		if head, _ := params[0].(string); head != "contains" {
			t.Errorf("%s: expected native head %q, got %v", tc.name, "contains", params[0])
			continue
		}
		want := cbqEval(t, e)
		got, gotOk := nativeEval(t, e)
		if !gotOk {
			t.Errorf("%s: native eval failed", tc.name)
			continue
		}
		if got != want {
			t.Errorf("%s: native=%q, cbq=%q", tc.name, got, want)
		}
	}

	// NOT LIKE = NewNot(NewLike(...)); rides the same rewrite under a native "not".
	notLike := []struct{ name, expr string }{
		{"not-basic-true", `"hello" NOT LIKE "%xyz%"`},
		{"not-basic-false", `"hello" NOT LIKE "%ell%"`},
		{"not-null", `null NOT LIKE "%x%"`},
	}
	for _, tc := range notLike {
		e := parse(tc.expr)
		want := cbqEval(t, e)
		got, gotOk := nativeEval(t, e)
		if !gotOk {
			t.Errorf("%s: %q did not optimize to native", tc.name, tc.expr)
			continue
		}
		if got != want {
			t.Errorf("%s: native=%q, cbq=%q", tc.name, got, want)
		}
	}

	// The rewritten needle is extractable for predicate-index pruning.
	{
		var buf bytes.Buffer
		params, ok := ExprTreeOptimize(nil, parse(`l.msg LIKE "%ell%"`), &buf, false)
		if !ok {
			t.Fatal("index: field contains-pattern LIKE did not lower")
		}
		if lit, has := engine.PrefilterLiteral(params); !has || lit != "ell" {
			t.Errorf("index: PrefilterLiteral = (%q,%v); want (%q,true)", lit, has, "ell")
		}

		// UPPER(msg) LIKE "%ELL%" lowers to contains(upper(field),"ELL") -- correct
		// to evaluate, but "ELL" is NOT in the raw row bytes, so it must NOT be
		// extracted as a required literal (soundness: always-wake instead).
		params2, ok := ExprTreeOptimize(nil, parse(`UPPER(l.msg) LIKE "%ELL%"`), &buf, false)
		if !ok {
			t.Fatal("index: UPPER(field) LIKE did not lower")
		}
		if lit, has := engine.PrefilterLiteral(params2); has {
			t.Errorf("index: transformed operand yielded literal %q; want no extraction", lit)
		}
	}

	// These forms are NOT a plain substring test -> must stay boxed.
	boxed := []struct{ name, expr string }{
		{"prefix", `"hello" LIKE "abc%"`},
		{"suffix", `"hello" LIKE "%abc"`},
		{"exact", `"hello" LIKE "abc"`},
		{"interior-wildcard", `"hello" LIKE "%a%b%"`},
		{"underscore", `"hello" LIKE "%a_b%"`},
		{"empty-middle", `"hello" LIKE "%%"`},
		{"single-pct", `"hello" LIKE "%"`},
		{"escaped-pct", `"hello" LIKE "%a\\%b%"`},
		{"dynamic-pattern", `"hello" LIKE foo`},
	}
	for _, tc := range boxed {
		e := parse(tc.expr)
		var buf bytes.Buffer
		if params, ok := ExprTreeOptimize(nil, e, &buf, false); ok {
			if head, _ := params[0].(string); head == "contains" {
				t.Errorf("%s: %q should stay boxed, but lowered to native contains", tc.name, tc.expr)
			}
		}
	}

	// Custom ESCAPE clause (built directly -- pattern parsing changes) stays boxed.
	{
		c := func(v interface{}) expression.Expression { return expression.NewConstant(value.NewValue(v)) }
		e := expression.NewLike(c("hello"), c("%a%"), c("!"))
		var buf bytes.Buffer
		if params, ok := ExprTreeOptimize(nil, e, &buf, false); ok {
			if head, _ := params[0].(string); head == "contains" {
				t.Error("custom-escape: LIKE with ESCAPE should stay boxed, but lowered to native contains")
			}
		}
	}
}
