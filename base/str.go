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
	"math"
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

// StrSplitFields builds the SPLIT(str) 1-arg result -- a JSON array of the
// whitespace-delimited fields (cbq strings.Fields), encoded into bufPre.
func StrSplitFields(c *ValComparer, s, bufPre []byte) []byte {
	return strEncodeStrArray(c, bytes.Fields(s), bufPre)
}

// StrSplitSep builds the SPLIT(str, sep) 2-arg result -- a JSON array of the
// sep-delimited parts (cbq strings.Split; an empty sep splits into UTF-8
// sequences), encoded into bufPre.
func StrSplitSep(c *ValComparer, s, sep, bufPre []byte) []byte {
	return strEncodeStrArray(c, bytes.Split(s, sep), bufPre)
}

// strEncodeStrArray serializes parts as a JSON array of strings into bufPre[:0].
// EncodeAsString APPENDS the encoded string to its out arg (unlike EncodeStr,
// which resets to bufPre[:0]), so elements accumulate. The parts alias s (no
// content copy); the [][]byte header from bytes.Split/Fields is the only alloc.
func strEncodeStrArray(c *ValComparer, parts [][]byte, bufPre []byte) []byte {
	c.PrepareEncoder()
	out := append(bufPre[:0], '[')
	for i, p := range parts {
		if i > 0 {
			out = append(out, ',')
		}
		out, _ = c.EncodeAsString(p, out)
	}
	return append(out, ']')
}

// StrPadLen reads an LPAD/RPAD length operand: ok=false (-> NULL) if not a number,
// not integral, or negative (cbq `num < 0.0 || num != math.Trunc(num)`).
func StrPadLen(v Val) (int, bool) {
	n, ok := ParseNum(v)
	if !ok {
		return 0, false
	}
	f := n.Float64()
	if f < 0 || f != math.Trunc(f) {
		return 0, false
	}
	return int(f), true
}

// StrPad pads (or truncates) s to l bytes with pad, for LPAD (right=false) /
// RPAD (right=true) -- byte-based, cbq padString with inRunes=false. If l <= len(s)
// it returns the first l bytes (a subslice; both L and R truncate the prefix).
// Otherwise it fills l-len(s) bytes with repeated pad (last chunk truncated),
// before (LPAD) or after (RPAD) s. The pad case allocates its result (content
// differs from input, like the case transforms); EncodeStr copies it into the
// reused buffer. The caller guarantees len(pad) >= 1.
func StrPad(s []byte, l int, pad []byte, right bool) []byte {
	if l <= len(s) {
		return s[:l]
	}
	out := make([]byte, 0, l)
	if right {
		out = append(out, s...)
	}
	d := l - len(s)
	for d > 0 {
		if len(pad) < d {
			out = append(out, pad...)
		} else {
			out = append(out, pad[:d]...)
		}
		d -= len(pad)
	}
	if !right {
		out = append(out, s...)
	}
	return out
}

// strPadSpace is the default LPAD/RPAD pad (a single space) for the 2-arg form.
var strPadSpace = []byte(" ")

// StrPadSpace is StrPad with the default single-space pad (LPAD/RPAD 2-arg form).
func StrPadSpace(s []byte, l int, right bool) []byte {
	return StrPad(s, l, strPadSpace, right)
}

// StrSubstr slices the decoded string per SUBSTR (byte-based, cbq
// strSubstrApply with inRunes=false). startPos is 0 (SUBSTR0) or 1 (SUBSTR1, the
// 1-based bias applied only to a positive pos). A negative pos counts from the
// end. hasLen selects the 2-arg (to end) vs 3-arg (fixed length) form. Returns a
// subslice of decoded (no allocation) and ok=false (-> NULL) for an out-of-range
// pos or a negative length. EncodeStr re-quotes the result into the buffer.
func StrSubstr(decoded []byte, pos, startPos int, hasLen bool, length int) ([]byte, bool) {
	l := len(decoded)
	if pos < 0 {
		pos = l + pos
	} else if pos > 0 && startPos > 0 {
		pos = pos - startPos
	}
	if pos < 0 || pos >= l {
		return nil, false
	}
	if !hasLen {
		return decoded[pos:], true
	}
	if length < 0 {
		return nil, false
	}
	if pos+length > l {
		length = l - pos
	}
	return decoded[pos : pos+length], true
}

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

// StrEncode re-encodes raw bytes as a JSON string into the reused bufPre.
func StrEncode(c *ValComparer, raw []byte, bufPre []byte) []byte {
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

// --------------------------------

// IntOperand reads a numeric operand as an integer: ok=false (the caller yields
// NULL) if v is not a number or not integral -- mirroring cbq's
// `int(v.Actual().(float64))` guarded by `vf != math.Trunc(vf)`. Used for a
// SUBSTR position/length and a ROUND/TRUNC precision (any sign).
func IntOperand(v Val) (int, bool) {
	n, ok := ParseNum(v)
	if !ok {
		return 0, false
	}
	f := n.Float64()
	if f != math.Trunc(f) {
		return 0, false
	}
	return int(f), true
}
