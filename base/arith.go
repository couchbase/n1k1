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

// Byte-level numeric arithmetic primitives, ported to faithfully mirror the
// couchbase/query value package's NumberValue semantics (value/integer.go,
// value/float.go) and its arithmetic expressions (expression/arith_*.go). The
// goal is that a native n1k1 arithmetic expression built on these produces
// byte-identical results to cbq's expr.Evaluate() -- but without boxing into
// value.Value objects: operands are JSON number bytes and the result is appended
// into a caller-reused []byte buffer (see DESIGN-exprs.md "primitives" phase).
//
// The MISSING/NULL and non-number type propagation (three-valued logic) is NOT
// done here -- it belongs to the per-expression Layer-1 harness. These functions
// are the pure numeric core.

import (
	"math"
	"strconv"

	"github.com/buger/jsonparser"
)

// Num mirrors cbq's NumberValue tagged union (value.intValue vs value.floatValue):
// a JSON number is kept as an int64 when the literal is integral (and fits),
// else as a float64. Preserving that int-vs-float distinction is what makes
// arithmetic and serialization match cbq exactly (e.g. int-preservation with
// promotion to float only on overflow; "4" vs "4.0" output).
type Num struct {
	I     int64
	F     float64
	IsInt bool
}

// IntNum and FloatNum construct a Num of each kind.
func IntNum(i int64) Num     { return Num{I: i, IsInt: true} }
func FloatNum(f float64) Num { return Num{F: f} }

// f64 is the float64 view. Matches value.intValue.Actual(), which returns
// float64(this) -- i.e. the '/' and '%' expressions read every operand as float64.
func (n Num) f64() float64 {
	if n.IsInt {
		return float64(n.I)
	}
	return n.F
}

// i64 truncates toward zero to int64. Matches cbq's intValue(float64) coercion
// used by IDiv/IMod (value/integer.go, value/float.go).
func (n Num) i64() int64 {
	if n.IsInt {
		return n.I
	}
	return int64(n.F)
}

// Float64 returns the number as a float64 -- what cbq's numberValue.Actual()
// yields, and the domain the math functions compute over.
func (n Num) Float64() float64 {
	if n.IsInt {
		return float64(n.I)
	}
	return n.F
}

// ParseNum parses JSON number bytes into a Num, choosing int64 when the literal
// is integral and fits, else float64 -- mirroring how cbq boxes JSON numbers.
// ok is false if b is not a valid JSON number. Allocation-free (jsonparser
// parses directly out of the byte slice).
func ParseNum(b []byte) (num Num, ok bool) {
	if len(b) == 0 {
		return Num{}, false
	}

	// Integral unless it carries a fraction or exponent.
	isFloat := false
	for _, c := range b {
		if c == '.' || c == 'e' || c == 'E' {
			isFloat = true
			break
		}
	}

	if !isFloat {
		if i, err := jsonparser.ParseInt(b); err == nil {
			return IntNum(i), true
		}
		// Integral but too large for int64: fall through to float64.
	}

	if f, err := jsonparser.ParseFloat(b); err == nil {
		return FloatNum(f), true
	}

	return Num{}, false
}

// Add mirrors value.intValue.Add / value.floatValue.Add: int+int stays int
// unless it overflows, in which case it promotes to float.
func (a Num) Add(b Num) Num {
	if a.IsInt && b.IsInt {
		rv := int64(uint64(a.I) + uint64(b.I))
		overflow := (a.I < 0 && b.I < 0 && rv >= 0) || (a.I >= 0 && b.I >= 0 && rv < 0)
		if !overflow {
			return IntNum(rv)
		}
	}
	return FloatNum(a.f64() + b.f64())
}

// Sub mirrors value.intValue.Sub (this.Add(-b), guarding -MinInt64 overflow).
func (a Num) Sub(b Num) Num {
	if a.IsInt && b.IsInt && b.I > math.MinInt64 {
		return a.Add(IntNum(-b.I))
	}
	return FloatNum(a.f64() - b.f64())
}

// Mult mirrors value.intValue.Mult: int*int stays int unless it overflows
// (detected via rv/a == b), in which case it promotes to float.
func (a Num) Mult(b Num) Num {
	if a.IsInt && b.IsInt {
		rv := a.I * b.I
		if a.I == 0 || rv/a.I == b.I {
			return IntNum(rv)
		}
	}
	return FloatNum(a.f64() * b.f64())
}

// Neg mirrors value.intValue.Neg (MinInt64 promotes to float since -MinInt64
// overflows int64).
func (a Num) Neg() Num {
	if a.IsInt {
		if a.I == math.MinInt64 {
			return FloatNum(-float64(a.I))
		}
		return IntNum(-a.I)
	}
	return FloatNum(-a.F)
}

// Div is float division, the '/' operator (expression/arith_div.go). Both
// operands are read as float64 and the result is always float. ok is false on
// divide-by-zero, which cbq maps to N1QL NULL (and emits a warning -- warning
// left to the Layer-1 harness).
func (a Num) Div(b Num) (Num, bool) {
	d := b.f64()
	if d == 0.0 {
		return Num{}, false
	}
	return FloatNum(a.f64() / d), true
}

// Mod is float modulo, the '%' operator (expression/arith_mod.go), via math.Mod.
// ok is false on mod-by-zero (=> N1QL NULL).
func (a Num) Mod(b Num) (Num, bool) {
	d := b.f64()
	if d == 0.0 {
		return Num{}, false
	}
	return FloatNum(math.Mod(a.f64(), d)), true
}

// IDiv is integer division, the 'DIV' operator: both operands truncate to int64
// and the result is int (value.intValue.IDiv / value.floatValue.IDiv). ok is
// false on divide-by-zero (=> N1QL NULL).
func (a Num) IDiv(b Num) (Num, bool) {
	d := b.i64()
	if d == 0 {
		return Num{}, false
	}
	return IntNum(a.i64() / d), true
}

// IMod is integer modulo, the 'MOD' operator (value.intValue.IMod /
// value.floatValue.IMod). ok is false on mod-by-zero (=> N1QL NULL).
func (a Num) IMod(b Num) (Num, bool) {
	d := b.i64()
	if d == 0 {
		return Num{}, false
	}
	return IntNum(a.i64() % d), true
}

// The arithmetic operators are passed to the expr harness as real func values
// (emitted by name, see LzExprFmt) with a uniform (a, b Num) (Num, bool)
// signature -- ok=false only for a divide/mod-by-zero (Div/Mod/IDiv/IMod); the
// always-ok ops (Add/Sub/Mult) return ok=true. These thin funcs adapt the Num
// methods (whose Add/Sub/Mult return a bare Num) to that one signature.
func ArithAdd(a, b Num) (Num, bool)  { return a.Add(b), true }
func ArithSub(a, b Num) (Num, bool)  { return a.Sub(b), true }
func ArithMult(a, b Num) (Num, bool) { return a.Mult(b), true }
func ArithDiv(a, b Num) (Num, bool)  { return a.Div(b) }
func ArithMod(a, b Num) (Num, bool)  { return a.Mod(b) }
func ArithIDiv(a, b Num) (Num, bool) { return a.IDiv(b) }
func ArithIMod(a, b Num) (Num, bool) { return a.IMod(b) }

// WarnDivideByZero is the advisory emitted (via Ctx.Warn) when '/' or DIV
// divides by zero, matching cbq's message. Kept as a named constant so the lz
// codegen emits an identifier reference rather than an inline string literal.
const WarnDivideByZero = "Division by 0."

// -----------------------------------------------------

// Propagation-class combinators. These fold the N1QL three-valued skeleton (the
// MISSING-dominant / unknown-passthrough branch that used to be re-expanded
// inline in every numeric expr's lz leaf) into one named base func that takes the
// already-captured operand value(s), the reused buffer, and a NAMED leaf func
// emitted by name (see LzExprFmt). Because the whole skeleton now lives here and
// the per-op leaf is a single named-func arg, the lz leaf collapses to one line
// and the compiled path emits one real call -- codegen-safe (see DESIGN-exprs.md
// "Codegen ergonomics", idea 2). Each returns the (possibly re-grown) buffer so
// the caller can keep reusing it.

// MissingDominantBiNum applies the binary-numeric three-valued rule: if either
// operand is MISSING (empty) the result is MISSING; else if either operand is not
// a JSON number the result is NULL; else op(a, b) formatted into buf[:0]. op
// returns ok=false only for a divide/mod-by-zero, which yields NULL and -- when
// warnZero is set and a warner is installed -- emits the divide-by-zero advisory.
// Mirrors cbq's arith_*.go Evaluate(). Used by ExprArithBi (+ - * / % DIV MOD)
// and the binary math funcs (POWER/ATAN2, via always-ok Num leaves).
func MissingDominantBiNum(a, b Val, buf []byte,
	op func(a, b Num) (Num, bool), warnZero bool, vars *Vars) (Val, []byte) {
	if len(a) == 0 || len(b) == 0 {
		return ValMissing, buf // MISSING dominant.
	}
	numA, okA := ParseNum(a)
	numB, okB := ParseNum(b)
	if !okA || !okB {
		return ValNull, buf // Non-number operand.
	}
	numR, okR := op(numA, numB)
	if !okR {
		if warnZero && vars.Ctx.Warn != nil {
			vars.Ctx.Warn(WarnDivideByZero)
		}
		return ValNull, buf // Divide/mod by zero.
	}
	out := AppendNum(buf[:0], numR)
	return Val(out), out
}

// UnknownPassthroughUnNum applies the unary-numeric rule with a Num->Num leaf:
// MISSING and NULL (any non-value) pass through unchanged; a non-number value
// becomes NULL; else op(num) formatted into buf[:0]. Used by ExprNeg, whose leaf
// (base.Num.Neg) must stay Num-based to preserve the int64/float64 distinction.
func UnknownPassthroughUnNum(v Val, buf []byte, op func(Num) Num) (Val, []byte) {
	if ValKind(v) != ValKindValue {
		return v, buf // MISSING/NULL pass through.
	}
	num, ok := ParseNum(v)
	if !ok {
		return ValNull, buf // Non-number operand.
	}
	out := AppendNum(buf[:0], op(num))
	return Val(out), out
}

// AppendNum formats a Num as JSON into out, matching cbq's value serialization
// (value/integer.go, value/float.go MarshalJSON): FormatInt for ints; for floats
// FormatFloat('f', -1, 64) with -0 normalized to 0, and NaN/+Inf/-Inf emitted as
// the quoted N1QL sentinel strings.
func AppendNum(out []byte, n Num) []byte {
	if n.IsInt {
		return strconv.AppendInt(out, n.I, 10)
	}

	f := n.F
	switch {
	case math.IsNaN(f):
		return append(out, `"NaN"`...)
	case math.IsInf(f, 1):
		return append(out, `"+Infinity"`...)
	case math.IsInf(f, -1):
		return append(out, `"-Infinity"`...)
	}
	if f == 0 { // normalize -0 to 0 (matches value.floatValue.MarshalJSON)
		f = 0
	}
	return strconv.AppendFloat(out, f, 'f', -1, 64)
}
