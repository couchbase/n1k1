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
// The transform family (UPPER/LOWER/TITLE/TRIM/LTRIM/RTRIM) shares
// exprStrTransform, selected by a transform FUNC (base.StrCase*/StrTrim*) emitted
// by name (see base/lzfmt.go). The func and the reused lzBufPre buffer co-exist on
// one emitted line -- the generator preserves fmt-arg order across a func
// placeholder and a varLift buffer placeholder (see cmd/intermed_build,
// DESIGN-exprs.md). LENGTH yields a number, not a re-encoded string, so it has its
// own harness. TRIM/LTRIM/RTRIM's 2-arg (explicit cutset) forms are variadic and
// fall back to cbq per the optimizer arity guard.

func init() {
	ExprCatalog["upper"] = ExprUpper
	ExprCatalog["lower"] = ExprLower
	ExprCatalog["title"] = ExprTitle
	ExprCatalog["trim"] = ExprTrim
	ExprCatalog["ltrim"] = ExprLTrim
	ExprCatalog["rtrim"] = ExprRTrim
	ExprCatalog["length"] = ExprLength
	ExprCatalog["contains"] = ExprContains
	ExprCatalog["position0"] = ExprPosition0
	ExprCatalog["position1"] = ExprPosition1
}

func ExprUpper(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return exprStrTransform(lzVars, labels, params, path, base.StrCaseUpper)
}

func ExprLower(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return exprStrTransform(lzVars, labels, params, path, base.StrCaseLower)
}

func ExprTitle(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return exprStrTransform(lzVars, labels, params, path, base.StrCaseTitle)
}

func ExprTrim(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return exprStrTransform(lzVars, labels, params, path, base.StrTrim)
}

func ExprLTrim(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return exprStrTransform(lzVars, labels, params, path, base.StrTrimLeft)
}

func ExprRTrim(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return exprStrTransform(lzVars, labels, params, path, base.StrTrimRight)
}

// exprStrTransform is the shared unary string-transform harness. base.StrDecode
// guards + decodes; transform maps the decoded bytes and base.EncodeStr re-encodes
// into lzBufPre on a single line -- the transform func value and the varLift buffer
// co-exist.
func exprStrTransform(lzVars *base.Vars, labels base.Labels, params []interface{},
	path string, transform func([]byte) []byte) (lzExprFunc base.ExprFunc) {
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
			lzBufPre = base.EncodeStr(lzVars.Ctx.ValComparer, transform(lzDecoded), lzBufPre)
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

// ExprContains: two string operands -> bool (no output buffer). Captures each
// operand FROM lzVal, mirrors the binary harness (ExprArithBi).
func ExprContains(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	biExprFunc := func(lzA, lzB base.ExprFunc, lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) { // !lz
		if LzScope {
			lzVal = lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"
			lzValA := lzVal

			lzVal = lzB(lzVals, lzYieldErr) // <== emitCaptured: path "B"
			lzValB := lzVal

			lzVal = base.StrContains(lzValA, lzValB)
		}

		return lzVal
	} // !lz

	lzExprFunc =
		MakeBiExprFunc(lzVars, labels, params, path, biExprFunc) // !lz

	return lzExprFunc
}

func ExprPosition0(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return exprStrPosition(lzVars, labels, params, path, 0)
}

func ExprPosition1(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return exprStrPosition(lzVars, labels, params, path, 1)
}

// exprStrPosition is the shared POSITION0/POSITION1 harness. base.StrPositionIndex
// computes the int (startPos %#v, no buffer); the int is then formatted into the
// reused lzBufPre (buffer %s, no startPos) -- kept on separate lines.
func exprStrPosition(lzVars *base.Vars, labels base.Labels, params []interface{},
	path string, startPos int) (lzExprFunc base.ExprFunc) {
	var lzBufPre []byte // <== varLift: lzBufPre by path

	biExprFunc := func(lzA, lzB base.ExprFunc, lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) { // !lz
		if LzScope {
			lzVal = lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"
			lzValA := lzVal

			lzVal = lzB(lzVals, lzYieldErr) // <== emitCaptured: path "B"
			lzValB := lzVal

			lzIdx, lzSentinel, lzOk := base.StrPositionIndex(lzValA, lzValB, startPos)
			if !lzOk {
				lzVal = lzSentinel
			} else {
				lzOut := base.AppendNum(lzBufPre[:0], base.IntNum(int64(lzIdx)))
				lzBufPre = lzOut
				lzVal = base.Val(lzOut)
			}
		}

		return lzVal
	} // !lz

	lzExprFunc =
		MakeBiExprFunc(lzVars, labels, params, path, biExprFunc) // !lz

	return lzExprFunc
}
