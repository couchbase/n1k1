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
// DECODED string), so each shares the same guard. Named per-op (not an int
// op-code) so the harness never puts a live-expr placeholder on the same line as
// the reused output buffer -- see engine/expr_str.go and DESIGN-exprs.md.
//
// The operand's inner (still-escaped) content is decoded once via
// jsonparser.Unescape -- which returns the input unchanged when there are no
// escapes (no allocation) -- matching cbq's arg.ToString().

// strDecode returns the operand's decoded string bytes and ok=true for a JSON
// string; for MISSING/non-string it returns the sentinel Val to yield and
// ok=false.
func strDecode(v Val) (decoded []byte, sentinel Val, ok bool) {
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

// StrUpper / StrLower: uppercase / lowercase a string; MISSING -> MISSING,
// non-string -> NULL. The result is re-encoded as a JSON string into bufPre.
func StrUpper(v Val, c *ValComparer, bufPre []byte) (Val, []byte) {
	return strCase(v, c, bufPre, true)
}

func StrLower(v Val, c *ValComparer, bufPre []byte) (Val, []byte) {
	return strCase(v, c, bufPre, false)
}

func strCase(v Val, c *ValComparer, bufPre []byte, upper bool) (Val, []byte) {
	decoded, sentinel, ok := strDecode(v)
	if !ok {
		return sentinel, bufPre
	}
	var s string
	if upper {
		s = strings.ToUpper(string(decoded))
	} else {
		s = strings.ToLower(string(decoded))
	}
	c.PrepareEncoder() // lazily wires c.Encoder to c.Buffer (idempotent)
	out, _ := c.EncodeAsString([]byte(s), bufPre[:0])
	return Val(out), out
}

// StrLength: the byte length of the decoded string as a JSON int; MISSING ->
// MISSING, non-string -> NULL. Mirrors cbq len(arg.ToString()).
func StrLength(v Val, bufPre []byte) (Val, []byte) {
	decoded, sentinel, ok := strDecode(v)
	if !ok {
		return sentinel, bufPre
	}
	out := strconv.AppendInt(bufPre[:0], int64(len(decoded)), 10)
	return Val(out), out
}
