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

// BASE64_DECODE_STRING(s) base64-decodes s and returns the bytes reinterpreted as a
// UTF-8 STRING -- always a string, never binary. It fills a gap the stock BASE64_DECODE
// leaves: BASE64_DECODE runs the decoded bytes back through value.NewValue, which PARSES
// them as JSON -- so base64 of `{"a":1}` conveniently yields the object {"a":1}, but
// base64 of a plain string (an etcd key like `/config/app`, or a non-JSON value) yields
// an unusable BINARY value with no binary->text coercion in SQL++. This function forces
// the string interpretation, so a base64'd key/plaintext round-trips to a queryable
// string. Name follows Snowflake's BASE64_DECODE_STRING (vs its BASE64_DECODE_BINARY).
//
// LANE: this is a boxed (cbq value.Value) scalar function, registered like node() /
// multi_matches() -- n1k1's Convert has no native []byte lowering, so it NA()s the
// projection to the boxed lane while the scan stays native. That's right for a
// once-per-row ingest projection (BASE64_DECODE is boxed too); not a hot-path primitive.
//
// Semantics mirror the stock Base64Decode: MISSING -> MISSING, a non-STRING arg -> NULL,
// invalid base64 -> NULL. See DESIGN-data.md §9 (the etcd recipe drove this).

import (
	"encoding/base64"

	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/value"
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

func (this *base64DecodeStringFunc) Evaluate(item value.Value, context expression.Context) (value.Value, error) {
	arg, err := this.Operands()[0].Evaluate(item, context)
	if err != nil {
		return nil, err
	}
	if arg.Type() == value.MISSING {
		return value.MISSING_VALUE, nil
	}
	if arg.Type() != value.STRING {
		return value.NULL_VALUE, nil
	}
	b, derr := base64.StdEncoding.DecodeString(arg.ToString())
	if derr != nil {
		return value.NULL_VALUE, nil
	}
	return value.NewValue(string(b)), nil // string(): force text, never re-parse as JSON
}
