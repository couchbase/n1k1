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
	"testing"
	"time"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/engine"

	"github.com/couchbase/query/expression"
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
		// N-ary (>2 operands) now optimizes natively via MakeNaryExprFunc.
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
