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
	"bytes"
	"strconv"

	"github.com/buger/jsonparser"
)

// Unary string functions on JSON-string bytes. cbq's func_str.go skeleton is
// uniform (MISSING -> MISSING, a non-string operand -> NULL, else compute on the
// DECODED string), so the pieces below are split so ONE engine harness can serve
// the whole case-transform family: StrDecode (guard + decode) and StrCaseApply
// (the per-op-code transform, kept off the reused-buffer line) then EncodeStr.
// The int op-code and the reused buffer never share an emitted line -- see
// engine/expr_str.go and DESIGN-exprs.md.
//
// The operand's inner (still-escaped) content is decoded once via
// jsonparser.Unescape -- which returns the input unchanged when there are no
// escapes (no allocation) -- matching cbq's arg.ToString().

// Case-transform op-codes for StrCaseApply.
const (
	StrUpper = iota
	StrLower
	StrTitle
)

// StrDecode returns the operand's decoded string bytes and ok=true for a JSON
// string; for MISSING/non-string it returns the sentinel Val to yield and
// ok=false.
func StrDecode(v Val) (decoded []byte, sentinel Val, ok bool) {
	if len(v) == 0 {
		return nil, ValMissing, false
	}
	inner, pt := Parse(v)
	if ParseTypeToValType[pt] != ValTypeString {
		return nil, ValNull, false
	}
	d, err := jsonparser.Unescape(inner, nil)
	if err != nil {
		return nil, ValNull, false
	}
	return d, nil, true
}

// StrCaseApply case-transforms the decoded string bytes per op-code, returning
// the raw (not yet JSON-encoded) result. StrTitle mirrors cbq exactly:
// strings.Title(strings.ToLower).
func StrCaseApply(op int, decoded []byte) []byte {
	// bytes.* (not strings.*(string(decoded))) avoids the []byte<->string round-trip
	// allocations; one allocation remains for the transformed result (inherent --
	// the case-folded content differs from the input), which EncodeStr then copies
	// into the reused output buffer.
	switch op {
	case StrUpper:
		return bytes.ToUpper(decoded)
	case StrLower:
		return bytes.ToLower(decoded)
	case StrTitle:
		return bytes.Title(bytes.ToLower(decoded)) //nolint:staticcheck // match cbq strings.Title
	}
	return decoded
}

// EncodeStr re-encodes raw bytes as a JSON string into the reused bufPre.
func EncodeStr(c *ValComparer, raw []byte, bufPre []byte) []byte {
	c.PrepareEncoder() // lazily wires c.Encoder to c.Buffer (idempotent)
	out, _ := c.EncodeAsString(raw, bufPre[:0])
	return out
}

// StrLength: the byte length of the decoded string as a JSON int; MISSING ->
// MISSING, non-string -> NULL. Mirrors cbq len(arg.ToString()).
func StrLength(v Val, bufPre []byte) (Val, []byte) {
	decoded, sentinel, ok := StrDecode(v)
	if !ok {
		return sentinel, bufPre
	}
	out := strconv.AppendInt(bufPre[:0], int64(len(decoded)), 10)
	return Val(out), out
}

// StrBinDecode classifies two operands for the binary string funcs: both decoded
// strings, ok=true only when both are JSON strings; else the sentinel Val to
// yield -- MISSING if either operand is MISSING (dominant), otherwise NULL.
func StrBinDecode(a, b Val) (da, db []byte, sentinel Val, ok bool) {
	if len(a) == 0 || len(b) == 0 {
		return nil, nil, ValMissing, false
	}
	ia, ta := Parse(a)
	ib, tb := Parse(b)
	if ParseTypeToValType[ta] != ValTypeString || ParseTypeToValType[tb] != ValTypeString {
		return nil, nil, ValNull, false
	}
	xa, ea := jsonparser.Unescape(ia, nil)
	xb, eb := jsonparser.Unescape(ib, nil)
	if ea != nil || eb != nil {
		return nil, nil, ValNull, false
	}
	return xa, xb, nil, true
}

// StrContains reports whether a contains substring b (cbq CONTAINS); both string
// -> bool, MISSING -> MISSING, non-string -> NULL.
func StrContains(a, b Val) Val {
	da, db, sentinel, ok := StrBinDecode(a, b)
	if !ok {
		return sentinel
	}
	if bytes.Contains(da, db) { // byte-level: no []byte->string alloc
		return ValTrue
	}
	return ValFalse
}

// StrPositionIndex is the byte position of b in a, plus startPos (0 for
// POSITION0, 1 for POSITION1); strings.Index returns -1 when absent. ok=false
// (with a sentinel) for the MISSING/non-string guard. Mirrors cbq
// strPositionApply (byte offsets, not runes). Kept off the reused-buffer line --
// the engine harness formats the returned int separately.
func StrPositionIndex(a, b Val, startPos int) (idx int, sentinel Val, ok bool) {
	da, db, s, o := StrBinDecode(a, b)
	if !o {
		return 0, s, false
	}
	return bytes.Index(da, db) + startPos, nil, true // byte offset, no alloc
}
