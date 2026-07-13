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
// The transform family (UPPER/LOWER/TITLE/TRIM/LTRIM/RTRIM/REVERSE) shares
// exprStrTransform via the strTransformFuncs table, selected by a transform FUNC
// (base.StrCase*/StrTrim*) emitted by name (see base.LzExprFmt). The func and the
// reused lzBufPre buffer co-exist on
// one emitted line -- the generator preserves fmt-arg order across a func
// placeholder and a varLift buffer placeholder (see cmd/intermed_build,
// DESIGN-exprs.md). LENGTH yields a number, not a re-encoded string, so it has its
// own harness. TRIM/LTRIM/RTRIM's 2-arg (explicit cutset) forms are variadic and
// fall back to cbq per the optimizer arity guard.

// ExprStrTransformFuncs is the (name -> []byte->[]byte transform leaf) table for the
// unary string-transform funcs, emitted by name in the compiled path (see
// LzExprFmt). All share exprStrTransform via the exprStrTransformOp adapter.
var ExprStrTransformFuncs = map[string]func([]byte) []byte{
	"upper": base.StrCaseUpper, "lower": base.StrCaseLower, "title": base.StrCaseTitle,
	"trim": base.StrTrim, "ltrim": base.StrTrimLeft, "rtrim": base.StrTrimRight,
	"reverse": base.StrReverse,
}

func init() {
	for name, transform := range ExprStrTransformFuncs {
		ExprCatalog[name] = ExprStrTransformOp(transform)
	}
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
	// REGEXP_CONTAINS (partial match) / REGEXP_LIKE (full ^...$ match). Both are
	// boolean predicates over a CONSTANT pattern (the optimizer keeps a dynamic or
	// invalid pattern boxed); see exprRegexpMatch and base.StrRegexpMatch.
	ExprCatalog["regexp_contains"] = ExprRegexpContains
	ExprCatalog["regexp_like"] = ExprRegexpLike
}

// ExprStrTransformOp adapts a []byte->[]byte transform into an ExprCatalogFunc by
// closing over it and deferring to the shared exprStrTransform harness. Plain Go
// (no lz), so intermed_build emits it verbatim and the transform flows unchanged
// to the emission site.
func ExprStrTransformOp(transform func([]byte) []byte) base.ExprCatalogFunc {
	return func(lzVars *base.Vars, labels base.Labels, params []interface{}, path string) base.ExprFunc {
		return ExprStrTransform(lzVars, labels, params, path, transform)
	}
}

// ExprStrTransform is the shared unary string-transform harness. base.StrDecode
// guards + decodes; transform maps the decoded bytes and base.EncodeStr re-encodes
// into lzBufPre on a single line -- the transform func value and the varLift buffer
// co-exist.
func ExprStrTransform(lzVars *base.Vars, labels base.Labels, params []interface{},
	path string, transform func([]byte) []byte) (lzExprFunc base.ExprFunc) {
	exprA := params[0].([]interface{})

	var lzBufPre []byte // <== varLift: lzBufPre by path

	lzA := MakeExprFunc(lzVars, labels, exprA, path, "A") // !lzRHS, via: lzExprFunc

	lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
		lzVal = lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"

		lzVal, lzBufPre = base.StrTransformInto(lzVal, lzVars.Ctx.ValComparer, lzBufPre, transform)

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
			lzValA := lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A", via: lzVal

			lzValB := lzB(lzVals, lzYieldErr) // <== emitCaptured: path "B", via: lzVal

			lzValC := lzC(lzVals, lzYieldErr) // <== emitCaptured: path "C", via: lzVal

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
					lzBufPre = base.StrEncode(lzVars.Ctx.ValComparer, lzRaw, lzBufPre)
					lzVal = base.Val(lzBufPre)
				}
			}
		}

		return lzVal
	} // !lz

	lzExprFunc = MakeTriExprFunc(lzVars, labels, params, path, triExprFunc) // !lzRHS

	return lzExprFunc
}

// SUBSTR0/SUBSTR1 (byte-based, cbq strSubstrApply inRunes=false). Two arities,
// each a fixed-arity harness: the 2-arg form (str, pos) is binary, the 3-arg form
// (str, pos, len) is ternary. startPos is 0 (SUBSTR0) or 1 (SUBSTR1). MISSING if
// any operand is MISSING; else NULL if str is non-string / pos|len non-integral /
// out-of-range; else the substring re-encoded into the reused buffer.

func ExprSubstr02(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return ExprSubstr2(lzVars, labels, params, path, 0)
}

func ExprSubstr03(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return ExprSubstr3(lzVars, labels, params, path, 0)
}

func ExprSubstr12(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return ExprSubstr2(lzVars, labels, params, path, 1)
}

func ExprSubstr13(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return ExprSubstr3(lzVars, labels, params, path, 1)
}

// ExprSubstr2 is the 2-arg SUBSTR harness (str, pos) -> str[pos:].
func ExprSubstr2(lzVars *base.Vars, labels base.Labels, params []interface{},
	path string, startPos int) (lzExprFunc base.ExprFunc) {
	var lzBufPre []byte // <== varLift: lzBufPre by path

	biExprFunc := func(lzA, lzB base.ExprFunc, lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) { // !lz
		if LzScope {
			lzValStr := lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A", via: lzVal

			lzValPos := lzB(lzVals, lzYieldErr) // <== emitCaptured: path "B", via: lzVal

			if base.ValKind(lzValStr) == base.ValKindMissing ||
				base.ValKind(lzValPos) == base.ValKindMissing {
				lzVal = base.ValMissing
			} else {
				lzStr, _, lzStrOk := base.StrDecode(lzValStr)
				lzPos, lzPosOk := base.ParseInt(lzValPos)
				if !lzStrOk || !lzPosOk {
					lzVal = base.ValNull
				} else {
					lzSub, lzInRange := base.StrSubstr(lzStr, lzPos, startPos, false, 0)
					if !lzInRange {
						lzVal = base.ValNull
					} else {
						lzBufPre = base.StrEncode(lzVars.Ctx.ValComparer, lzSub, lzBufPre)
						lzVal = base.Val(lzBufPre)
					}
				}
			}
		}

		return lzVal
	} // !lz

	lzExprFunc = MakeBiExprFunc(lzVars, labels, params, path, biExprFunc) // !lzRHS

	return lzExprFunc
}

// ExprSubstr3 is the 3-arg SUBSTR harness (str, pos, len) -> str[pos:pos+len].
func ExprSubstr3(lzVars *base.Vars, labels base.Labels, params []interface{},
	path string, startPos int) (lzExprFunc base.ExprFunc) {
	var lzBufPre []byte // <== varLift: lzBufPre by path

	triExprFunc := func(lzA, lzB, lzC base.ExprFunc, lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) { // !lz
		if LzScope {
			lzValStr := lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A", via: lzVal

			lzValPos := lzB(lzVals, lzYieldErr) // <== emitCaptured: path "B", via: lzVal

			lzValLen := lzC(lzVals, lzYieldErr) // <== emitCaptured: path "C", via: lzVal

			if base.ValKind(lzValStr) == base.ValKindMissing ||
				base.ValKind(lzValPos) == base.ValKindMissing ||
				base.ValKind(lzValLen) == base.ValKindMissing {
				lzVal = base.ValMissing
			} else {
				lzStr, _, lzStrOk := base.StrDecode(lzValStr)
				lzPos, lzPosOk := base.ParseInt(lzValPos)
				lzLen, lzLenOk := base.ParseInt(lzValLen)
				if !lzStrOk || !lzPosOk || !lzLenOk {
					lzVal = base.ValNull
				} else {
					lzSub, lzInRange := base.StrSubstr(lzStr, lzPos, startPos, true, lzLen)
					if !lzInRange {
						lzVal = base.ValNull
					} else {
						lzBufPre = base.StrEncode(lzVars.Ctx.ValComparer, lzSub, lzBufPre)
						lzVal = base.Val(lzBufPre)
					}
				}
			}
		}

		return lzVal
	} // !lz

	lzExprFunc = MakeTriExprFunc(lzVars, labels, params, path, triExprFunc) // !lzRHS

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

	lzA := MakeExprFunc(lzVars, labels, exprA, path, "A") // !lzRHS, via: lzExprFunc

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
			lzValStr := lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A", via: lzVal

			lzValSep := lzB(lzVals, lzYieldErr) // <== emitCaptured: path "B", via: lzVal

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

	lzExprFunc = MakeBiExprFunc(lzVars, labels, params, path, biExprFunc) // !lzRHS

	return lzExprFunc
}

// LPAD/RPAD (byte-based, cbq padString inRunes=false). MISSING if any operand
// MISSING; NULL if str/pad non-string, len non-integral or negative, or pad empty;
// else base.StrPad(Space) into the reused buffer. Two arities: 2-arg (default
// single-space pad) is binary, 3-arg (explicit pad) is ternary. right selects
// RPAD (append) vs LPAD (prepend).

func ExprLPad2(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return ExprPad2(lzVars, labels, params, path, false)
}

func ExprRPad2(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return ExprPad2(lzVars, labels, params, path, true)
}

func ExprLPad3(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return ExprPad3(lzVars, labels, params, path, false)
}

func ExprRPad3(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return ExprPad3(lzVars, labels, params, path, true)
}

// ExprPad2 is the 2-arg LPAD/RPAD harness (str, len) with the default space pad.
func ExprPad2(lzVars *base.Vars, labels base.Labels, params []interface{},
	path string, right bool) (lzExprFunc base.ExprFunc) {
	var lzBufPre []byte // <== varLift: lzBufPre by path

	biExprFunc := func(lzA, lzB base.ExprFunc, lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) { // !lz
		if LzScope {
			lzValStr := lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A", via: lzVal

			lzValLen := lzB(lzVals, lzYieldErr) // <== emitCaptured: path "B", via: lzVal

			if base.ValKind(lzValStr) == base.ValKindMissing ||
				base.ValKind(lzValLen) == base.ValKindMissing {
				lzVal = base.ValMissing
			} else {
				lzStr, _, lzStrOk := base.StrDecode(lzValStr)
				lzL, lzLOk := base.StrPadLen(lzValLen)
				if !lzStrOk || !lzLOk {
					lzVal = base.ValNull
				} else {
					lzBufPre = base.StrEncode(lzVars.Ctx.ValComparer, base.StrPadSpace(lzStr, lzL, right), lzBufPre)
					lzVal = base.Val(lzBufPre)
				}
			}
		}

		return lzVal
	} // !lz

	lzExprFunc = MakeBiExprFunc(lzVars, labels, params, path, biExprFunc) // !lzRHS

	return lzExprFunc
}

// ExprPad3 is the 3-arg LPAD/RPAD harness (str, len, pad).
func ExprPad3(lzVars *base.Vars, labels base.Labels, params []interface{},
	path string, right bool) (lzExprFunc base.ExprFunc) {
	var lzBufPre []byte // <== varLift: lzBufPre by path

	triExprFunc := func(lzA, lzB, lzC base.ExprFunc, lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) { // !lz
		if LzScope {
			lzValStr := lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A", via: lzVal

			lzValLen := lzB(lzVals, lzYieldErr) // <== emitCaptured: path "B", via: lzVal

			lzValPad := lzC(lzVals, lzYieldErr) // <== emitCaptured: path "C", via: lzVal

			if base.ValKind(lzValStr) == base.ValKindMissing ||
				base.ValKind(lzValLen) == base.ValKindMissing ||
				base.ValKind(lzValPad) == base.ValKindMissing {
				lzVal = base.ValMissing
			} else {
				lzStr, _, lzStrOk := base.StrDecode(lzValStr)
				lzL, lzLOk := base.StrPadLen(lzValLen)
				lzPad, _, lzPadOk := base.StrDecode(lzValPad)
				if !lzStrOk || !lzLOk || !lzPadOk || len(lzPad) < 1 {
					lzVal = base.ValNull // non-string / bad len / empty pad
				} else {
					lzBufPre = base.StrEncode(lzVars.Ctx.ValComparer, base.StrPad(lzStr, lzL, lzPad, right), lzBufPre)
					lzVal = base.Val(lzBufPre)
				}
			}
		}

		return lzVal
	} // !lz

	lzExprFunc = MakeTriExprFunc(lzVars, labels, params, path, triExprFunc) // !lzRHS

	return lzExprFunc
}

func ExprLength(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	exprA := params[0].([]interface{})

	var lzBufPre []byte // <== varLift: lzBufPre by path

	lzA := MakeExprFunc(lzVars, labels, exprA, path, "A") // !lzRHS, via: lzExprFunc

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
			lzValA := lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A", via: lzVal

			lzValB := lzB(lzVals, lzYieldErr) // <== emitCaptured: path "B", via: lzVal

			lzVal = base.StrContains(lzValA, lzValB)
		}

		return lzVal
	} // !lz

	lzExprFunc = MakeBiExprFunc(lzVars, labels, params, path, biExprFunc) // !lzRHS

	return lzExprFunc
}

func ExprPosition0(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return ExprStrPosition(lzVars, labels, params, path, 0)
}

func ExprPosition1(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return ExprStrPosition(lzVars, labels, params, path, 1)
}

// ExprStrPosition is the shared POSITION0/POSITION1 harness. base.StrPositionIndex
// computes the int (startPos %#v, no buffer); the int is then formatted into the
// reused lzBufPre (buffer %s, no startPos) -- kept on separate lines.
func ExprStrPosition(lzVars *base.Vars, labels base.Labels, params []interface{},
	path string, startPos int) (lzExprFunc base.ExprFunc) {
	var lzBufPre []byte // <== varLift: lzBufPre by path

	biExprFunc := func(lzA, lzB base.ExprFunc, lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) { // !lz
		if LzScope {
			lzValA := lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A", via: lzVal

			lzValB := lzB(lzVals, lzYieldErr) // <== emitCaptured: path "B", via: lzVal

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

	lzExprFunc = MakeBiExprFunc(lzVars, labels, params, path, biExprFunc) // !lzRHS

	return lzExprFunc
}

// REGEXP_CONTAINS(str, pattern) / REGEXP_LIKE(str, pattern) -> bool. Both take a
// CONSTANT pattern: the glue optimizer only lowers these when the 2nd operand is a
// compile-time-constant string that COMPILES, keeping a dynamic or invalid pattern
// BOXED so cbq's per-row-recompile / runtime-error behavior is preserved. So the
// pattern operand is NOT a per-row child ExprFunc -- it's decoded from the params
// tree at CONSTRUCTION time (codegen / query setup), baked as a varLift'd string,
// and compiled ONCE (lazily, on the first row) into the varLift'd base.Regexp
// cache. Only the source operand (params[0]) rides a per-row leaf, so this is a
// UNARY harness (like LENGTH). full selects REGEXP_LIKE's ^...$ anchoring.

func ExprRegexpContains(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return ExprRegexpMatch(lzVars, labels, params, path, false)
}

func ExprRegexpLike(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) base.ExprFunc {
	return ExprRegexpMatch(lzVars, labels, params, path, true)
}

// ExprRegexpMatch is the shared REGEXP_CONTAINS/REGEXP_LIKE harness. exprA is the
// source operand's per-row leaf; the constant pattern (params[1]) is decoded and
// (for REGEXP_LIKE) anchored at construction, baked into lzPat, and compiled once
// per row-stream into lzRe by base.StrRegexpMatch.
func ExprRegexpMatch(lzVars *base.Vars, labels base.Labels, params []interface{},
	path string, full bool) (lzExprFunc base.ExprFunc) {
	exprA := params[0].([]interface{})

	pat := regexpPatternFrom(params[1].([]interface{}), full)

	var lzPat string = pat // <== varLift: lzPat by path

	var lzRe base.Regexp // <== varLift: lzRe by path

	lzA := MakeExprFunc(lzVars, labels, exprA, path, "A") // !lzRHS, via: lzExprFunc

	lzExprFunc = func(lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) {
		lzVal = lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"

		lzVal = base.RegexpMatchStr(lzVal, lzPat, &lzRe)

		return lzVal
	}

	return lzExprFunc
}

// regexpPatternFrom decodes the constant pattern operand -- a ["json", <text>]
// leaf holding a JSON string -- into its raw regexp source, anchored as "^...$"
// for REGEXP_LIKE (full). Runs at construction only (never per row), so its
// allocations don't count against the eval path. The optimizer guarantees a
// json-string pattern, so StrDecode succeeds.
func regexpPatternFrom(patParam []interface{}, full bool) string {
	decoded, _, ok := base.StrDecode(base.Val(patParam[1].(string)))
	if !ok {
		return ""
	}
	s := string(decoded)
	if full {
		s = "^" + s + "$"
	}
	return s
}
