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
	params, ok := ExprTreeOptimize(nil, e, &buf)
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
