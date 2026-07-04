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
	"strconv"
	"strings"

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
	s := string(decoded)
	switch op {
	case StrUpper:
		return []byte(strings.ToUpper(s))
	case StrLower:
		return []byte(strings.ToLower(s))
	case StrTitle:
		return []byte(strings.Title(strings.ToLower(s))) //nolint:staticcheck // match cbq
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
