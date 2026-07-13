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

// Native date/time functions. Only the millis-based, non-volatile pieces are
// ported (base/datetime.go); string parsing and named-timezone forms stay in the
// cbq fallback. See DESIGN-exprs.md.

func init() {
	ExprCatalog["date_part_millis"] = ExprDatePartMillis
	ExprCatalog["date_add_millis"] = ExprDateAddMillis
}

// ExprDatePartMillis handles DATE_PART_MILLIS(millis, part) -- the 2-arg form
// (the 3-arg named-timezone form falls back per the optimizer arity guard). cbq
// skeleton: MISSING if either operand MISSING; NULL if millis non-number, part
// non-string, millis out of range, or part an unknown component; else the integer
// component into the reused buffer. Each operand is captured FROM lzVal.
func ExprDatePartMillis(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	var lzBufPre []byte // <== varLift: lzBufPre by path

	biExprFunc := func(lzA, lzB base.ExprFunc, lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) { // !lz
		if LzScope {
			lzValMillis := lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A", via: lzVal

			lzValPart := lzB(lzVals, lzYieldErr) // <== emitCaptured: path "B", via: lzVal

			if base.ValKind(lzValMillis) == base.ValKindMissing ||
				base.ValKind(lzValPart) == base.ValKindMissing {
				lzVal = base.ValMissing
			} else {
				lzNum, lzNumOk := base.ParseNum(lzValMillis)
				lzPart, _, lzPartOk := base.StrDecode(lzValPart)
				if !lzNumOk || !lzPartOk {
					lzVal = base.ValNull // non-number millis / non-string part
				} else {
					lzP, lzPOk := base.DatePartMillis(lzNum.Float64(), lzPart)
					if !lzPOk {
						lzVal = base.ValNull // out-of-range millis / unknown component
					} else {
						lzBufPre = base.AppendNum(lzBufPre[:0], base.IntNum(int64(lzP)))
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

// ExprDateAddMillis handles DATE_ADD_MILLIS(millis, n, part) -- always 3-arg
// (TernaryFunctionBase). cbq skeleton (expression/func_date.go): MISSING if any
// operand MISSING; NULL if millis non-number, n non-number, part non-string,
// millis out of range, n non-integral, part an unknown component, or the result
// overflows; else the resulting epoch millis (integer) into the reused buffer.
// Each operand is captured FROM lzVal (emitCaptured writes lzVal; read out on the
// next line -- mirrors ExprReplace).
func ExprDateAddMillis(lzVars *base.Vars, labels base.Labels,
	params []interface{}, path string) (lzExprFunc base.ExprFunc) {
	var lzBufPre []byte // <== varLift: lzBufPre by path

	triExprFunc := func(lzA, lzB, lzC base.ExprFunc, lzVals base.Vals, lzYieldErr base.YieldErr) (lzVal base.Val) { // !lz
		if LzScope {
			lzValMillis := lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A", via: lzVal

			lzValN := lzB(lzVals, lzYieldErr) // <== emitCaptured: path "B", via: lzVal

			lzValPart := lzC(lzVals, lzYieldErr) // <== emitCaptured: path "C", via: lzVal

			if base.ValKind(lzValMillis) == base.ValKindMissing ||
				base.ValKind(lzValN) == base.ValKindMissing ||
				base.ValKind(lzValPart) == base.ValKindMissing {
				lzVal = base.ValMissing
			} else {
				lzMillis, lzMillisOk := base.ParseNum(lzValMillis)
				lzN, lzNOk := base.ParseNum(lzValN)
				lzPart, _, lzPartOk := base.StrDecode(lzValPart)
				if !lzMillisOk || !lzNOk || !lzPartOk {
					lzVal = base.ValNull // non-number millis|n / non-string part
				} else {
					lzR, lzROk := base.DateAddMillis(lzMillis.Float64(), lzN.Float64(), lzPart)
					if !lzROk {
						lzVal = base.ValNull // out-of-range / non-integral n / unknown component / overflow
					} else {
						lzBufPre = base.AppendNum(lzBufPre[:0], base.IntNum(int64(lzR)))
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
