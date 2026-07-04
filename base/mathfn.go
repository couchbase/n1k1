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

// Unary math op-codes for MathUnary. An int op-code (not a func value) is what
// the expr harness hands the lz codegen -- a func value renders under %#v as an
// invalid pointer literal (see Num.Arith / DESIGN-exprs.md).
const (
	MathAbs = iota
	MathCeil
	MathFloor
	MathSqrt
	MathExp
	MathLn
	MathLog
	MathSign
	MathDegrees
	MathRadians
)

func mathApply(op int, f float64) float64 {
	switch op {
	case MathAbs:
		return math.Abs(f)
	case MathCeil:
		return math.Ceil(f)
	case MathFloor:
		return math.Floor(f)
	case MathSqrt:
		return math.Sqrt(f)
	case MathExp:
		return math.Exp(f)
	case MathLn:
		return math.Log(f)
	case MathLog:
		return math.Log10(f)
	case MathSign:
		if f < 0 {
			return -1
		}
		if f > 0 {
			return 1
		}
		return 0
	case MathDegrees:
		return f * 180.0 / math.Pi
	case MathRadians:
		return f * math.Pi / 180.0
	}
	return f
}

// MathApply applies a unary math op-code to n, returning the result as a Num
// (a float64). The caller (engine/expr_math.go) handles the cbq skeleton around
// it -- MISSING -> MISSING, non-number -> NULL -- and formats via AppendNum, which
// serializes a NaN / +Inf / -Inf result as the quoted "NaN" / "+Infinity" /
// "-Infinity" sentinels (matching cbq), so e.g. sqrt(-1) and ln(0) agree.
//
// Kept as a Num->Num compute (not a Val->Val one taking the output buffer) so the
// harness can keep the op-code (%#v) and the reused buffer (varLift %s) on
// SEPARATE emitted lines -- the codegen mis-orders args when both appear on one.
func MathApply(op int, n Num) Num {
	return FloatNum(mathApply(op, n.Float64()))
}
