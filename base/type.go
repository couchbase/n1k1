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
	"strconv"

	"github.com/buger/jsonparser"
)

var numOne = Val("1")
var numZero = Val("0")

// ToBoolean mirrors cbq TO_BOOLEAN: MISSING/NULL pass through; otherwise the
// value's truthiness (bool -> itself; non-zero number; non-empty string / array /
// object). Zero-alloc (returns a constant Val).
func ToBoolean(v Val) Val {
	switch ValKind(v) {
	case ValKindMissing:
		return ValMissing
	case ValKindNull:
		return ValNull
	}
	if ValTruthy(v) {
		return ValTrue
	}
	return ValFalse
}

// ToString mirrors cbq TO_STRING: MISSING/NULL pass through; STRING unchanged;
// BOOLEAN/NUMBER -> their JSON-string form; ARRAY/OBJECT -> NULL. Built into the
// reused bufPre; zero-alloc, since a canonical number or bool literal has no
// characters needing JSON escaping, so we just quote-and-append.
func ToString(v Val, bufPre []byte) (Val, []byte) {
	if len(v) == 0 {
		return ValMissing, bufPre
	}
	switch ParseTypeToValType[parseType(v)] {
	case ValTypeNull:
		return ValNull, bufPre
	case ValTypeString:
		return v, bufPre // already a string
	case ValTypeBoolean:
		out := append(bufPre[:0], '"')
		out = append(out, v...) // true / false
		out = append(out, '"')
		return Val(out), out
	case ValTypeNumber:
		n, ok := ParseNum(v)
		if !ok {
			return ValNull, bufPre
		}
		out := append(bufPre[:0], '"')
		out = AppendNum(out, n) // canonical decimal (matches cbq FormatInt/FormatFloat)
		out = append(out, '"')
		return Val(out), out
	default: // array, object
		return ValNull, bufPre
	}
}

// ToNumber mirrors cbq TO_NUMBER (single-operand form): MISSING/NULL/NUMBER pass
// through; BOOLEAN -> 1/0; STRING -> the parsed number (cbq's ParseInt-then-
// ParseFloat) or NULL; ARRAY/OBJECT -> NULL. Built into the reused bufPre.
func ToNumber(v Val, bufPre []byte) (Val, []byte) {
	if len(v) == 0 {
		return ValMissing, bufPre
	}
	inner, pt := Parse(v)
	switch ParseTypeToValType[pt] {
	case ValTypeNull:
		return ValNull, bufPre
	case ValTypeNumber:
		return v, bufPre // already a number
	case ValTypeBoolean:
		if ValTruthy(v) {
			return numOne, bufPre
		}
		return numZero, bufPre
	case ValTypeString:
		d, err := jsonparser.Unescape(inner, nil)
		if err != nil {
			return ValNull, bufPre
		}
		s := string(d)
		if i, e := strconv.ParseInt(s, 10, 64); e == nil &&
			((i > math.MinInt64 && i < math.MaxInt64) || strconv.FormatInt(i, 10) == s) {
			out := AppendNum(bufPre[:0], IntNum(i))
			return Val(out), out
		}
		if f, e := strconv.ParseFloat(s, 64); e == nil {
			out := AppendNum(bufPre[:0], FloatNum(f))
			return Val(out), out
		}
		return ValNull, bufPre
	default: // array, object
		return ValNull, bufPre
	}
}

// parseType returns just the jsonparser type of v (companion to Parse).
func parseType(v Val) int {
	_, pt := Parse(v)
	return pt
}

// Type-test predicates over a ValType, passed as real func values to the engine's
// shared is-type harness (ExprIsType). Exported + in base so LzExprFmt can emit
// them by name (base.TypeIsArray) into the compiled path.
func TypeIsArray(t int) bool   { return t == ValTypeArray }
func TypeIsNumber(t int) bool  { return t == ValTypeNumber }
func TypeIsString(t int) bool  { return t == ValTypeString }
func TypeIsBoolean(t int) bool { return t == ValTypeBoolean }
func TypeIsObject(t int) bool  { return t == ValTypeObject }

// TypeIsAtom: a scalar -- boolean, number, or string (not array/object).
func TypeIsAtom(t int) bool {
	return t == ValTypeBoolean || t == ValTypeNumber || t == ValTypeString
}
