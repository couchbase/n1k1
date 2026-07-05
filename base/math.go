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

import "math"

// The per-op math for the unary/binary math funcs is a real func value passed
// into the engine harness and emitted by name (see LzExprFmt): a stdlib
// math.Abs/Ceil/... (a func(float64) float64) directly, or one of the three
// named funcs below for the ops that aren't a bare stdlib call. POWER/ATAN2 use
// math.Pow/math.Atan2 (func(float64, float64) float64) via MathBinApply.

// MathSign returns -1, 0, or +1 for a negative, zero, or positive f (SIGN()).
func MathSign(f float64) float64 {
	if f < 0 {
		return -1
	}
	if f > 0 {
		return 1
	}
	return 0
}

// MathDegrees / MathRadians convert between radians and degrees (DEGREES/RADIANS).
func MathDegrees(f float64) float64 { return f * 180.0 / math.Pi }
func MathRadians(f float64) float64 { return f * math.Pi / 180.0 }

// MathBinApply applies a binary math func to (a, b), returning the result as a
// Num. Same skeleton as MathApply -- the engine harness handles MISSING ->
// MISSING / non-number -> NULL around it.
func MathBinApply(fn func(a, b float64) float64, a, b Num) Num {
	return FloatNum(fn(a.Float64(), b.Float64()))
}

// RoundFloat rounds x to prec decimal places, round-half-to-even -- a
// byte-for-byte port of cbq's roundFloat(x, prec, to_even=true)
// (expression/func_num.go), so ROUND() matches exactly. NaN/Inf pass through.
// Signature func(float64, int) float64 so it can be passed to the shared
// ROUND/TRUNC harness and emitted by name (see base/lzfmt.go).
func RoundFloat(x float64, prec int) float64 {
	if math.IsNaN(x) || math.IsInf(x, 0) {
		return x
	}
	sign := 1.0
	if x < 0 {
		sign = -1.0
		x = -x
	}
	pow := math.Pow(10, float64(prec))
	intermed := (x * pow) + 0.5
	rounder := math.Floor(intermed)
	if rounder == intermed && math.Mod(rounder, 2) != 0 {
		rounder-- // round half to even
	}
	return sign * rounder / pow
}

// TruncFloat truncates x toward zero to prec decimal places -- a port of cbq's
// truncateFloat(x, prec). Same func(float64, int) float64 shape as RoundFloat.
func TruncFloat(x float64, prec int) float64 {
	pow := math.Pow(10, float64(prec))
	return math.Trunc(x*pow) / pow
}

// MathApply applies a unary math func to n, returning the result as a Num (a
// float64). The caller (engine/expr_math.go) handles the cbq skeleton around it
// -- MISSING -> MISSING, non-number -> NULL -- and formats via AppendNum, which
// serializes a NaN / +Inf / -Inf result as the quoted "NaN" / "+Infinity" /
// "-Infinity" sentinels (matching cbq), so e.g. sqrt(-1) and ln(0) agree.
func MathApply(fn func(f float64) float64, n Num) Num {
	return FloatNum(fn(n.Float64()))
}
