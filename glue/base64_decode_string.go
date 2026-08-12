//go:build n1ql

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

package glue

// BASE64_DECODE_STRING(s): base64-decode s and return the bytes as a UTF-8 STRING --
// always a string, never re-parsed as JSON (unlike the stock BASE64_DECODE, which runs
// the decoded bytes back through value.NewValue and so yields the object for base64 of
// `{"a":1}` but an unusable binary for a base64'd plain string like an etcd key path).
// Name follows Snowflake's BASE64_DECODE_STRING. See DESIGN-data.md §9.
//
// n1k1 favors its NATIVE []byte lane: the ONE implementation is base.StrBase64DecodeInto
// (byte-in, byte-out, no boxing), and engine/expr_str.go's ExprBase64DecodeString lowers
// a convertible call straight to it via glue/expr_optimize.go's optSelf (zero-boxing).
// This file is NOT a second (boxed) implementation -- it exists only because cbq's parser
// resolves a function name against its registry (like node()/multi_matches()), and its
// Evaluate is a thin bridge that DELEGATES to the same native helper for the rare case an
// operand isn't natively convertible (bridging value->bytes->helper->value there).

import (
	"bytes"

	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/value"

	"github.com/couchbase/n1k1/base"
)

// Base64DecodeStringFuncName is the registered SQL++ function name (lowercase, like the
// other n1k1-registered builtins).
const Base64DecodeStringFuncName = "base64_decode_string"

func init() {
	expression.RegisterFunction(Base64DecodeStringFuncName, newBase64DecodeStringFunc())
}

type base64DecodeStringFunc struct {
	expression.FunctionBase
}

func newBase64DecodeStringFunc(operands ...expression.Expression) expression.Function {
	rv := &base64DecodeStringFunc{}
	rv.Init(Base64DecodeStringFuncName, operands...)
	rv.SetExpr(rv)
	return rv
}

func (this *base64DecodeStringFunc) Accept(visitor expression.Visitor) (interface{}, error) {
	return visitor.VisitFunction(this)
}
func (this *base64DecodeStringFunc) Type() value.Type { return value.STRING }
func (this *base64DecodeStringFunc) MinArgs() int     { return 1 }
func (this *base64DecodeStringFunc) MaxArgs() int     { return 1 }
func (this *base64DecodeStringFunc) Constructor() expression.FunctionConstructor {
	return func(operands ...expression.Expression) expression.Function {
		return newBase64DecodeStringFunc(operands...)
	}
}

// Evaluate bridges the boxed operand value to the native byte-lane implementation
// (base.StrBase64DecodeInto) -- the single source of truth. Only reached when the
// operand isn't natively convertible (a convertible call lowers straight to the native
// op); MISSING/non-string/invalid-base64 propagate exactly as the native lane does.
func (this *base64DecodeStringFunc) Evaluate(item value.Value, context expression.Context) (value.Value, error) {
	arg, err := this.Operands()[0].Evaluate(item, context)
	if err != nil {
		return nil, err
	}
	if arg.Type() == value.MISSING {
		return value.MISSING_VALUE, nil
	}
	var buf bytes.Buffer // the operand's JSON bytes -> a base.Val for the native helper
	if err := arg.WriteJSON(nil, &buf, "", "", true); err != nil {
		return nil, err
	}
	out, _, _ := base.StrBase64DecodeInto(base.Val(buf.Bytes()), base.NewValComparer(), nil, nil)
	switch base.ValKind(out) {
	case base.ValKindMissing:
		return value.MISSING_VALUE, nil
	case base.ValKindNull:
		return value.NULL_VALUE, nil
	default:
		return value.NewValue([]byte(out)), nil // out is a JSON string -> stringValue
	}
}
