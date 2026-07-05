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
	ExprCatalog["reverse"] = ExprReverse
	ExprCatalog["replace"] = ExprReplace
	// SUBSTR0/SUBSTR1, 2-arg and 3-arg -- the optimizer dispatches to an
	// arity-specific name so each rides a fixed-arity harness (no variadic).
	ExprCatalog["substr0_2"] = ExprSubstr02
	ExprCatalog["substr0_3"] = ExprSubstr03
	ExprCatalog["substr1_2"] = ExprSubstr12
	ExprCatalog["substr1_3"] = ExprSubstr13
	// SPLIT: 1-arg (whitespace) / 2-arg (explicit sep), arity-dispatched.
	ExprCatalog["split_1"] = ExprSplit1
	ExprCatalog["split_2"] = ExprSplit2
	// LPAD/RPAD: 2-arg (default space pad) / 3-arg (explicit pad), arity-dispatched.
	ExprCatalog["lpad_2"] = ExprLPad2
	ExprCatalog["lpad_3"] = ExprLPad3
	ExprCatalog["rpad_2"] = ExprRPad2
	ExprCatalog["rpad_3"] = ExprRPad3
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

func ExprReverse(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return exprStrTransform(lzVars, labels, params, path, base.StrReverse)
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

// ExprReplace handles REPLACE(str, old, repl) -- replace all occurrences. cbq
// skeleton (expression/func_str.go): MISSING if any operand is MISSING; else NULL
// if any is non-string; else base.StrReplaceAll into the reused buffer. The 4-arg
// count form is variadic and falls back to cbq per the optimizer arity guard. Each
// operand is captured FROM lzVal (emitCaptured writes lzVal; a direct
// lzValX := lzX(...) bind is dropped in the compiled path -- mirrors ExprArithBi).
func ExprReplace(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	var lzBufPre []byte // <== varLift: lzBufPre by path

	triExprFunc := func(lzA, lzB, lzC base.ExprFunc, lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) { // !lz
		if LzScope {
			lzVal = lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"
			lzValA := lzVal

			lzVal = lzB(lzVals, lzYieldErr) // <== emitCaptured: path "B"
			lzValB := lzVal

			lzVal = lzC(lzVals, lzYieldErr) // <== emitCaptured: path "C"
			lzValC := lzVal

			if base.ValKind(lzValA) == base.ValKindMissing ||
				base.ValKind(lzValB) == base.ValKindMissing ||
				base.ValKind(lzValC) == base.ValKindMissing {
				lzVal = base.ValMissing
			} else {
				lzStrA, _, lzOkA := base.StrDecode(lzValA)
				lzStrB, _, lzOkB := base.StrDecode(lzValB)
				lzStrC, _, lzOkC := base.StrDecode(lzValC)
				if !lzOkA || !lzOkB || !lzOkC {
					lzVal = base.ValNull // a non-string operand
				} else {
					lzRaw := base.StrReplaceAll(lzStrA, lzStrB, lzStrC)
					lzBufPre = base.EncodeStr(lzVars.Ctx.ValComparer, lzRaw, lzBufPre)
					lzVal = base.Val(lzBufPre)
				}
			}
		}

		return lzVal
	} // !lz

	lzExprFunc =
		MakeTriExprFunc(lzVars, labels, params, path, triExprFunc) // !lz

	return lzExprFunc
}

// SUBSTR0/SUBSTR1 (byte-based, cbq strSubstrApply inRunes=false). Two arities,
// each a fixed-arity harness: the 2-arg form (str, pos) is binary, the 3-arg form
// (str, pos, len) is ternary. startPos is 0 (SUBSTR0) or 1 (SUBSTR1). MISSING if
// any operand is MISSING; else NULL if str is non-string / pos|len non-integral /
// out-of-range; else the substring re-encoded into the reused buffer.

func ExprSubstr02(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return exprSubstr2(lzVars, labels, params, path, 0)
}

func ExprSubstr03(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return exprSubstr3(lzVars, labels, params, path, 0)
}

func ExprSubstr12(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return exprSubstr2(lzVars, labels, params, path, 1)
}

func ExprSubstr13(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return exprSubstr3(lzVars, labels, params, path, 1)
}

// exprSubstr2 is the 2-arg SUBSTR harness (str, pos) -> str[pos:].
func exprSubstr2(lzVars *base.Vars, labels base.Labels, params []interface{},
	path string, startPos int) (lzExprFunc base.ExprFunc) {
	var lzBufPre []byte // <== varLift: lzBufPre by path

	biExprFunc := func(lzA, lzB base.ExprFunc, lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) { // !lz
		if LzScope {
			lzVal = lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"
			lzValStr := lzVal

			lzVal = lzB(lzVals, lzYieldErr) // <== emitCaptured: path "B"
			lzValPos := lzVal

			if base.ValKind(lzValStr) == base.ValKindMissing ||
				base.ValKind(lzValPos) == base.ValKindMissing {
				lzVal = base.ValMissing
			} else {
				lzStr, _, lzStrOk := base.StrDecode(lzValStr)
				lzPos, lzPosOk := base.SubstrNum(lzValPos)
				if !lzStrOk || !lzPosOk {
					lzVal = base.ValNull
				} else {
					lzSub, lzInRange := base.StrSubstr(lzStr, lzPos, startPos, false, 0)
					if !lzInRange {
						lzVal = base.ValNull
					} else {
						lzBufPre = base.EncodeStr(lzVars.Ctx.ValComparer, lzSub, lzBufPre)
						lzVal = base.Val(lzBufPre)
					}
				}
			}
		}

		return lzVal
	} // !lz

	lzExprFunc =
		MakeBiExprFunc(lzVars, labels, params, path, biExprFunc) // !lz

	return lzExprFunc
}

// exprSubstr3 is the 3-arg SUBSTR harness (str, pos, len) -> str[pos:pos+len].
func exprSubstr3(lzVars *base.Vars, labels base.Labels, params []interface{},
	path string, startPos int) (lzExprFunc base.ExprFunc) {
	var lzBufPre []byte // <== varLift: lzBufPre by path

	triExprFunc := func(lzA, lzB, lzC base.ExprFunc, lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) { // !lz
		if LzScope {
			lzVal = lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"
			lzValStr := lzVal

			lzVal = lzB(lzVals, lzYieldErr) // <== emitCaptured: path "B"
			lzValPos := lzVal

			lzVal = lzC(lzVals, lzYieldErr) // <== emitCaptured: path "C"
			lzValLen := lzVal

			if base.ValKind(lzValStr) == base.ValKindMissing ||
				base.ValKind(lzValPos) == base.ValKindMissing ||
				base.ValKind(lzValLen) == base.ValKindMissing {
				lzVal = base.ValMissing
			} else {
				lzStr, _, lzStrOk := base.StrDecode(lzValStr)
				lzPos, lzPosOk := base.SubstrNum(lzValPos)
				lzLen, lzLenOk := base.SubstrNum(lzValLen)
				if !lzStrOk || !lzPosOk || !lzLenOk {
					lzVal = base.ValNull
				} else {
					lzSub, lzInRange := base.StrSubstr(lzStr, lzPos, startPos, true, lzLen)
					if !lzInRange {
						lzVal = base.ValNull
					} else {
						lzBufPre = base.EncodeStr(lzVars.Ctx.ValComparer, lzSub, lzBufPre)
						lzVal = base.Val(lzBufPre)
					}
				}
			}
		}

		return lzVal
	} // !lz

	lzExprFunc =
		MakeTriExprFunc(lzVars, labels, params, path, triExprFunc) // !lz

	return lzExprFunc
}

// SPLIT(str [, sep]) -> array of substrings. cbq skeleton: MISSING if any
// operand MISSING; NULL if any non-string; else a JSON array built into the
// reused buffer (the first structure-building native expr). Two arities, each a
// fixed-arity harness: 1-arg splits on whitespace (strings.Fields), 2-arg on sep.

func ExprSplit1(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
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
			lzBufPre = base.StrSplitFields(lzVars.Ctx.ValComparer, lzDecoded, lzBufPre)
			lzVal = base.Val(lzBufPre)
		}

		return lzVal
	}

	return lzExprFunc
}

func ExprSplit2(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	var lzBufPre []byte // <== varLift: lzBufPre by path

	biExprFunc := func(lzA, lzB base.ExprFunc, lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) { // !lz
		if LzScope {
			lzVal = lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"
			lzValStr := lzVal

			lzVal = lzB(lzVals, lzYieldErr) // <== emitCaptured: path "B"
			lzValSep := lzVal

			if base.ValKind(lzValStr) == base.ValKindMissing ||
				base.ValKind(lzValSep) == base.ValKindMissing {
				lzVal = base.ValMissing
			} else {
				lzStr, _, lzStrOk := base.StrDecode(lzValStr)
				lzSep, _, lzSepOk := base.StrDecode(lzValSep)
				if !lzStrOk || !lzSepOk {
					lzVal = base.ValNull
				} else {
					lzBufPre = base.StrSplitSep(lzVars.Ctx.ValComparer, lzStr, lzSep, lzBufPre)
					lzVal = base.Val(lzBufPre)
				}
			}
		}

		return lzVal
	} // !lz

	lzExprFunc =
		MakeBiExprFunc(lzVars, labels, params, path, biExprFunc) // !lz

	return lzExprFunc
}

// LPAD/RPAD (byte-based, cbq padString inRunes=false). MISSING if any operand
// MISSING; NULL if str/pad non-string, len non-integral or negative, or pad empty;
// else base.StrPad(Space) into the reused buffer. Two arities: 2-arg (default
// single-space pad) is binary, 3-arg (explicit pad) is ternary. right selects
// RPAD (append) vs LPAD (prepend).

func ExprLPad2(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return exprPad2(lzVars, labels, params, path, false)
}

func ExprRPad2(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return exprPad2(lzVars, labels, params, path, true)
}

func ExprLPad3(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return exprPad3(lzVars, labels, params, path, false)
}

func ExprRPad3(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return exprPad3(lzVars, labels, params, path, true)
}

// exprPad2 is the 2-arg LPAD/RPAD harness (str, len) with the default space pad.
func exprPad2(lzVars *base.Vars, labels base.Labels, params []interface{},
	path string, right bool) (lzExprFunc base.ExprFunc) {
	var lzBufPre []byte // <== varLift: lzBufPre by path

	biExprFunc := func(lzA, lzB base.ExprFunc, lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) { // !lz
		if LzScope {
			lzVal = lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"
			lzValStr := lzVal

			lzVal = lzB(lzVals, lzYieldErr) // <== emitCaptured: path "B"
			lzValLen := lzVal

			if base.ValKind(lzValStr) == base.ValKindMissing ||
				base.ValKind(lzValLen) == base.ValKindMissing {
				lzVal = base.ValMissing
			} else {
				lzStr, _, lzStrOk := base.StrDecode(lzValStr)
				lzL, lzLOk := base.PadLen(lzValLen)
				if !lzStrOk || !lzLOk {
					lzVal = base.ValNull
				} else {
					lzBufPre = base.EncodeStr(lzVars.Ctx.ValComparer, base.StrPadSpace(lzStr, lzL, right), lzBufPre)
					lzVal = base.Val(lzBufPre)
				}
			}
		}

		return lzVal
	} // !lz

	lzExprFunc =
		MakeBiExprFunc(lzVars, labels, params, path, biExprFunc) // !lz

	return lzExprFunc
}

// exprPad3 is the 3-arg LPAD/RPAD harness (str, len, pad).
func exprPad3(lzVars *base.Vars, labels base.Labels, params []interface{},
	path string, right bool) (lzExprFunc base.ExprFunc) {
	var lzBufPre []byte // <== varLift: lzBufPre by path

	triExprFunc := func(lzA, lzB, lzC base.ExprFunc, lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) { // !lz
		if LzScope {
			lzVal = lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"
			lzValStr := lzVal

			lzVal = lzB(lzVals, lzYieldErr) // <== emitCaptured: path "B"
			lzValLen := lzVal

			lzVal = lzC(lzVals, lzYieldErr) // <== emitCaptured: path "C"
			lzValPad := lzVal

			if base.ValKind(lzValStr) == base.ValKindMissing ||
				base.ValKind(lzValLen) == base.ValKindMissing ||
				base.ValKind(lzValPad) == base.ValKindMissing {
				lzVal = base.ValMissing
			} else {
				lzStr, _, lzStrOk := base.StrDecode(lzValStr)
				lzL, lzLOk := base.PadLen(lzValLen)
				lzPad, _, lzPadOk := base.StrDecode(lzValPad)
				if !lzStrOk || !lzLOk || !lzPadOk || len(lzPad) < 1 {
					lzVal = base.ValNull // non-string / bad len / empty pad
				} else {
					lzBufPre = base.EncodeStr(lzVars.Ctx.ValComparer, base.StrPad(lzStr, lzL, lzPad, right), lzBufPre)
					lzVal = base.Val(lzBufPre)
				}
			}
		}

		return lzVal
	} // !lz

	lzExprFunc =
		MakeTriExprFunc(lzVars, labels, params, path, triExprFunc) // !lz

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
