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

package engine

import (
	"github.com/couchbase/n1k1/base"
)

// Unary string functions on JSON-string bytes into a reused buffer -- no boxing.
// The case-transform family (UPPER/LOWER/TITLE) shares exprStrCase, selected by
// an int op-code; the op-code (base.StrCaseApply, %#v) and the reused lzBufPre
// buffer (varLift %s) stay on SEPARATE emitted lines, because the generator
// mis-orders args when a %#v placeholder sits between %s placeholders on one line
// (see base/strfn.go, DESIGN-exprs.md). LENGTH yields a number, not a re-encoded
// string, so it has its own harness.

func init() {
	ExprCatalog["upper"] = ExprUpper
	ExprCatalog["lower"] = ExprLower
	ExprCatalog["title"] = ExprTitle
	ExprCatalog["length"] = ExprLength
}

func ExprUpper(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return exprStrCase(lzVars, labels, params, path, base.StrUpper)
}

func ExprLower(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return exprStrCase(lzVars, labels, params, path, base.StrLower)
}

func ExprTitle(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return exprStrCase(lzVars, labels, params, path, base.StrTitle)
}

// exprStrCase is the shared case-transform harness. base.StrDecode guards +
// decodes; base.StrCaseApply transforms (op-code line, no buffer); base.EncodeStr
// re-encodes into lzBufPre (buffer line, no op-code).
func exprStrCase(lzVars *base.Vars, labels base.Labels, params []interface{},
	path string, op int) (lzExprFunc base.ExprFunc) {
	exprA := params[0].([]interface{})

	var lzBufPre []byte // <== varLift: lzBufPre by path

	lzExprFunc =
		MakeExprFunc(lzVars, labels, exprA, path, "A") // !lz
	lzA := lzExprFunc

	lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
		lzVal = lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"

		lzDecoded, lzSentinel, lzOk := base.StrDecode(lzVal)
		if !lzOk {
			lzVal = lzSentinel
		} else {
			lzRaw := base.StrCaseApply(op, lzDecoded)
			lzBufPre = base.EncodeStr(lzVars.Ctx.ValComparer, lzRaw, lzBufPre)
			lzVal = base.Val(lzBufPre)
		}

		return lzVal
	}

	return lzExprFunc
}

func ExprLength(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	exprA := params[0].([]interface{})

	var lzBufPre []byte // <== varLift: lzBufPre by path

	lzExprFunc =
		MakeExprFunc(lzVars, labels, exprA, path, "A") // !lz
	lzA := lzExprFunc

	lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
		lzVal = lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"

		lzVal, lzBufPre = base.StrLength(lzVal, lzBufPre)

		return lzVal
	}

	return lzExprFunc
}
