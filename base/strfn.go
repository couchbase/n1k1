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

// StrUpper / StrLower / StrTitle: case transforms; MISSING -> MISSING,
// non-string -> NULL. The result is re-encoded as a JSON string into bufPre.
// (The transform is a plain func arg to a base helper -- fine, since base code is
// not lz-codegen'd; only the engine-level call is, and it names the base func.)
func StrUpper(v Val, c *ValComparer, bufPre []byte) (Val, []byte) {
	return strTransform(v, c, bufPre, strings.ToUpper)
}

func StrLower(v Val, c *ValComparer, bufPre []byte) (Val, []byte) {
	return strTransform(v, c, bufPre, strings.ToLower)
}

// StrTitle title-cases the string, mirroring cbq: strings.Title(strings.ToLower).
func StrTitle(v Val, c *ValComparer, bufPre []byte) (Val, []byte) {
	return strTransform(v, c, bufPre, strTitleCase)
}

func strTitleCase(s string) string { return strings.Title(strings.ToLower(s)) } //nolint:staticcheck // match cbq

func strTransform(v Val, c *ValComparer, bufPre []byte, fn func(string) string) (Val, []byte) {
	decoded, sentinel, ok := strDecode(v)
	if !ok {
		return sentinel, bufPre
	}
	c.PrepareEncoder() // lazily wires c.Encoder to c.Buffer (idempotent)
	out, _ := c.EncodeAsString([]byte(fn(string(decoded))), bufPre[:0])
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
