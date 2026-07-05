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
			lzVal = lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"
			lzValMillis := lzVal

			lzVal = lzB(lzVals, lzYieldErr) // <== emitCaptured: path "B"
			lzValPart := lzVal

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

	lzExprFunc =
		MakeBiExprFunc(lzVars, labels, params, path, biExprFunc) // !lz

	return lzExprFunc
}
