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
	"unicode"
	"unicode/utf8"

	"github.com/buger/jsonparser"
)

// Unary string functions on JSON-string bytes. cbq's func_str.go skeleton is
// uniform (MISSING -> MISSING, a non-string operand -> NULL, else compute on the
// DECODED string), so ONE engine harness serves the whole case-transform family:
// StrDecode (guard + decode), a case-transform func (StrCaseUpper/Lower/Title),
// then EncodeStr into the reused buffer. The case func is passed as a real func
// value and emitted by name; it can now share an emitted line with the reused
// buffer, so transform + encode collapse into one line -- see engine/expr_str.go,
// base/lzfmt.go, and DESIGN-exprs.md.
//
// The operand's inner (still-escaped) content is decoded once via
// jsonparser.Unescape -- which returns the input unchanged when there are no
// escapes (no allocation) -- matching cbq's arg.ToString().

// Case-transform funcs on decoded string bytes, returning the raw (not yet
// JSON-encoded) result. bytes.* (not strings.*(string(decoded))) avoids the
// []byte<->string round-trip allocations; one allocation remains for the
// transformed result (inherent -- the case-folded content differs from the
// input), which EncodeStr then copies into the reused output buffer. Passed as
// real func values into the shared harness and emitted by name (see LzExprFmt).
func StrCaseUpper(decoded []byte) []byte { return bytes.ToUpper(decoded) }
func StrCaseLower(decoded []byte) []byte { return bytes.ToLower(decoded) }

// StrCaseTitle mirrors cbq exactly: strings.Title(strings.ToLower).
func StrCaseTitle(decoded []byte) []byte {
	return bytes.Title(bytes.ToLower(decoded)) //nolint:staticcheck // match cbq strings.Title
}

// strWhitespace is cbq's default TRIM/LTRIM/RTRIM cutset (query
// expression/func_str.go _WHITESPACE = " \t\n\f\r"). The 2-arg forms (explicit
// cutset) are variadic and fall back to cbq per the optimizer arity guard.
const strWhitespace = " \t\n\f\r"

// StrTrim / StrTrimLeft / StrTrimRight strip cbq's default whitespace cutset from
// both / the left / the right of the decoded bytes. bytes.Trim* return a subslice
// (no allocation); EncodeStr then copies into the reused output buffer. Same
// func([]byte) []byte shape as the case funcs, so they share the harness.
func StrTrim(decoded []byte) []byte      { return bytes.Trim(decoded, strWhitespace) }
func StrTrimLeft(decoded []byte) []byte  { return bytes.TrimLeft(decoded, strWhitespace) }
func StrTrimRight(decoded []byte) []byte { return bytes.TrimRight(decoded, strWhitespace) }

// StrReverse reverses the decoded string, preserving combining-character
// sequences (a base rune keeps its following Mn/Me/Mc marks in order) and quietly
// skipping invalid UTF-8 -- a byte-for-byte port of cbq's
// util.ReversePreservingCombiningCharacters, so REVERSE() matches exactly. Same
// func([]byte) []byte shape as the case funcs, so it shares the harness.
func StrReverse(decoded []byte) []byte {
	if len(decoded) == 0 {
		return decoded
	}
	p := []rune(string(decoded))
	r := make([]rune, len(p))
	start := len(r)
	for i := 0; i < len(p); {
		if p[i] == utf8.RuneError { // quietly skip invalid UTF-8
			i++
			continue
		}
		j := i + 1
		for j < len(p) && (unicode.Is(unicode.Mn, p[j]) ||
			unicode.Is(unicode.Me, p[j]) || unicode.Is(unicode.Mc, p[j])) {
			j++
		}
		for k := j - 1; k >= i; k-- {
			start--
			r[start] = p[k]
		}
		i = j
	}
	return []byte(string(r[start:]))
}

// StrReplaceAll replaces every occurrence of old in s with repl (REPLACE()'s
// 3-arg form; cbq strings.Replace(s, old, repl, -1)). The 4-arg count form is
// variadic and falls back to cbq per the optimizer arity guard.
func StrReplaceAll(s, old, repl []byte) []byte { return bytes.Replace(s, old, repl, -1) }

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
