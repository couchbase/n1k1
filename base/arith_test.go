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

package base

import (
	"math"
	"testing"
)

func numStr(n Num) string { return string(AppendNum(nil, n)) }

func TestParseNumAndAppend(t *testing.T) {
	tests := []struct {
		in    string
		ok    bool
		isInt bool
		out   string // AppendNum round-trip
	}{
		{"123", true, true, "123"},
		{"-7", true, true, "-7"},
		{"0", true, true, "0"},
		{"-0", true, true, "0"}, // integral literal -> int 0
		{"1.5", true, false, "1.5"},
		{"0.0", true, false, "0"}, // -0 normalized, 0.0 -> "0"
		{"1e3", true, false, "1000"},
		{"2.5e-1", true, false, "0.25"},
		{"abc", false, false, ""},
		{"", false, false, ""},
		{"12x", false, false, ""},
	}
	for _, tc := range tests {
		n, ok := ParseNum([]byte(tc.in))
		if ok != tc.ok {
			t.Fatalf("ParseNum(%q) ok=%v, want %v", tc.in, ok, tc.ok)
		}
		if !ok {
			continue
		}
		if n.IsInt != tc.isInt {
			t.Errorf("ParseNum(%q) isInt=%v, want %v", tc.in, n.IsInt, tc.isInt)
		}
		if got := numStr(n); got != tc.out {
			t.Errorf("AppendNum(ParseNum(%q))=%q, want %q", tc.in, got, tc.out)
		}
	}

	// Integral literal too large for int64 -> float64 (not int).
	big, ok := ParseNum([]byte("99999999999999999999999"))
	if !ok || big.IsInt {
		t.Errorf("huge integral literal: ok=%v isInt=%v, want ok=true isInt=false", ok, big.IsInt)
	}
}

// mustNum parses or fails the test.
func mustNum(t *testing.T, s string) Num {
	t.Helper()
	n, ok := ParseNum([]byte(s))
	if !ok {
		t.Fatalf("ParseNum(%q) failed", s)
	}
	return n
}

func TestArithAddSubMultNeg(t *testing.T) {
	tests := []struct {
		op   string
		a, b string
		want string
	}{
		{"add", "2", "3", "5"},      // int+int -> int
		{"add", "2", "3.5", "5.5"},  // int+float -> float
		{"add", "2.0", "3.0", "5"},  // float+float, 5.0 -> "5"
		{"sub", "5", "3", "2"},      // int
		{"sub", "5", "3.5", "1.5"},  // float
		{"mult", "3", "4", "12"},    // int
		{"mult", "3", "0.5", "1.5"}, // float
		{"mult", "0", "9", "0"},     // int 0 short path
	}
	for _, tc := range tests {
		a, b := mustNum(t, tc.a), mustNum(t, tc.b)
		var got string
		switch tc.op {
		case "add":
			got = numStr(a.Add(b))
		case "sub":
			got = numStr(a.Sub(b))
		case "mult":
			got = numStr(a.Mult(b))
		}
		if got != tc.want {
			t.Errorf("%s(%s,%s)=%q, want %q", tc.op, tc.a, tc.b, got, tc.want)
		}
	}

	if got := numStr(mustNum(t, "5").Neg()); got != "-5" {
		t.Errorf("Neg(5)=%q, want -5", got)
	}
	if got := numStr(mustNum(t, "-5").Neg()); got != "5" {
		t.Errorf("Neg(-5)=%q, want 5", got)
	}
}

func TestArithOverflowPromotesToFloat(t *testing.T) {
	max := IntNum(math.MaxInt64)
	if r := max.Add(IntNum(1)); r.IsInt {
		t.Errorf("MaxInt64+1 should promote to float, got int %v", r.I)
	}
	if r := max.Mult(IntNum(2)); r.IsInt {
		t.Errorf("MaxInt64*2 should promote to float, got int %v", r.I)
	}
	if r := IntNum(math.MinInt64).Neg(); r.IsInt {
		t.Errorf("Neg(MinInt64) should promote to float, got int %v", r.I)
	}
	// Sub that underflows also promotes.
	if r := IntNum(math.MinInt64).Sub(IntNum(1)); r.IsInt {
		t.Errorf("MinInt64-1 should promote to float, got int %v", r.I)
	}
}

func TestArithDivModByZero(t *testing.T) {
	one := IntNum(1)
	zero := IntNum(0)
	if _, ok := one.Div(zero); ok {
		t.Error("Div by zero should be ok=false (NULL)")
	}
	if _, ok := one.Mod(zero); ok {
		t.Error("Mod by zero should be ok=false (NULL)")
	}
	if _, ok := one.IDiv(zero); ok {
		t.Error("IDiv by zero should be ok=false (NULL)")
	}
	if _, ok := one.IMod(zero); ok {
		t.Error("IMod by zero should be ok=false (NULL)")
	}
	// Float zero divisor too.
	if _, ok := one.Div(FloatNum(0.0)); ok {
		t.Error("Div by 0.0 should be ok=false (NULL)")
	}
}

func TestArithDivModIDivIMod(t *testing.T) {
	tests := []struct {
		op   string
		a, b string
		want string
	}{
		{"div", "1", "2", "0.5"}, // '/' always float
		{"div", "4", "2", "2"},   // 2.0 -> "2"
		{"mod", "5", "3", "2"},   // math.Mod -> 2.0 -> "2"
		{"mod", "5.5", "2", "1.5"},
		{"idiv", "7", "2", "3"},   // integer division
		{"idiv", "7.9", "2", "3"}, // operands truncate to int
		{"idiv", "-7", "2", "-3"}, // Go trunc toward zero
		{"imod", "7", "3", "1"},
	}
	for _, tc := range tests {
		a, b := mustNum(t, tc.a), mustNum(t, tc.b)
		var r Num
		var ok bool
		switch tc.op {
		case "div":
			r, ok = a.Div(b)
		case "mod":
			r, ok = a.Mod(b)
		case "idiv":
			r, ok = a.IDiv(b)
		case "imod":
			r, ok = a.IMod(b)
		}
		if !ok {
			t.Fatalf("%s(%s,%s) unexpected ok=false", tc.op, tc.a, tc.b)
		}
		if got := numStr(r); got != tc.want {
			t.Errorf("%s(%s,%s)=%q, want %q", tc.op, tc.a, tc.b, got, tc.want)
		}
	}
}

func TestAppendNumSpecials(t *testing.T) {
	tests := []struct {
		n    Num
		want string
	}{
		{FloatNum(math.NaN()), `"NaN"`},
		{FloatNum(math.Inf(1)), `"+Infinity"`},
		{FloatNum(math.Inf(-1)), `"-Infinity"`},
		{FloatNum(math.Copysign(0, -1)), "0"}, // -0 -> 0
		{IntNum(-42), "-42"},
	}
	for _, tc := range tests {
		if got := numStr(tc.n); got != tc.want {
			t.Errorf("AppendNum(%v)=%q, want %q", tc.n, got, tc.want)
		}
	}
}
