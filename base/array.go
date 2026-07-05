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
	"github.com/buger/jsonparser"
)

// Array reader op-codes for ArrayReduce -- functions that iterate an array and
// produce a scalar Num without materializing the elements. An int op-code (not a
// func value) keeps the compiled path clean; ArrayReduce is dispatched once per
// row, the reduce loop reading element bytes via jsonparser (no allocation).
const (
	ArrayOpLength = iota // count of all elements (ARRAY_LENGTH)
	ArrayOpCount         // count of non-NULL elements (ARRAY_COUNT)
	ArrayOpSum           // sum of NUMBER elements (ARRAY_SUM)
	ArrayOpAvg           // mean of NUMBER elements (ARRAY_AVG)
)

// ArrayReduce applies a reader op-code to v, returning the scalar as a Num. cbq
// skeleton: MISSING -> MISSING, non-array -> NULL (both via sentinel/ok=false).
// ARRAY_AVG over zero numbers is NULL. One ArrayEach pass feeds every op. The int
// result is kept off the reused-buffer line -- the engine harness formats the Num
// via AppendNum separately.
func ArrayReduce(op int, v Val) (result Num, sentinel Val, ok bool) {
	if len(v) == 0 {
		return Num{}, ValMissing, false
	}
	pv, pt := Parse(v)
	if ParseTypeToValType[pt] != ValTypeArray {
		return Num{}, ValNull, false
	}

	var total, nonNull, numCount int64
	sum := IntNum(0)
	jsonparser.ArrayEach(pv, func(e []byte, dt jsonparser.ValueType, _ int, _ error) {
		total++
		if dt != jsonparser.Null {
			nonNull++
		}
		if dt == jsonparser.Number {
			if n, okn := ParseNum(e); okn {
				sum = sum.Add(n)
				numCount++
			}
		}
	})

	switch op {
	case ArrayOpLength:
		return IntNum(total), nil, true
	case ArrayOpCount:
		return IntNum(nonNull), nil, true
	case ArrayOpSum:
		return sum, nil, true
	case ArrayOpAvg:
		if numCount == 0 {
			return Num{}, ValNull, false
		}
		return FloatNum(sum.Float64() / float64(numCount)), nil, true
	}
	return Num{}, ValNull, false
}

// ArrayMin / ArrayMax return the collation-min / -max non-NULL element; the
// winning element is copied into bufPre (a string element is re-quoted, as
// jsonparser strips the quotes). MISSING -> MISSING, non-array -> NULL, and an
// empty / all-NULL array -> NULL. Mirrors cbq ARRAY_MIN/ARRAY_MAX (v.Collate over
// non-null elements; NULL sorts below everything, so it never wins).
func ArrayMin(vc *ValComparer, v Val, bufPre []byte) (Val, []byte) {
	return ArrayMinMax(vc, v, false, bufPre)
}

func ArrayMax(vc *ValComparer, v Val, bufPre []byte) (Val, []byte) {
	return ArrayMinMax(vc, v, true, bufPre)
}

func ArrayMinMax(vc *ValComparer, v Val, wantMax bool, bufPre []byte) (Val, []byte) {
	if len(v) == 0 {
		return ValMissing, bufPre
	}
	pv, pt := Parse(v)
	if ParseTypeToValType[pt] != ValTypeArray {
		return ValNull, bufPre
	}

	var best []byte
	var bestType jsonparser.ValueType
	has := false
	jsonparser.ArrayEach(pv, func(e []byte, dt jsonparser.ValueType, _ int, _ error) {
		if dt == jsonparser.Null {
			return
		}
		if !has {
			best, bestType, has = e, dt, true
			return
		}
		cmp := vc.CompareWithType(e, best, int(dt), int(bestType), 0)
		if (wantMax && cmp > 0) || (!wantMax && cmp < 0) {
			best, bestType = e, dt
		}
	})
	if !has {
		return ValNull, bufPre
	}
	out := bufPre[:0]
	if bestType == jsonparser.String {
		out = append(out, '"')
		out = append(out, best...)
		out = append(out, '"')
	} else {
		out = append(out, best...) // number/bool/array/object already valid JSON
	}
	return Val(out), out
}

// ArrayContains reports membership of x in array arr (cbq ARRAY_CONTAINS).
// MISSING (either) -> MISSING; non-array arr OR NULL x -> NULL; else true iff some
// element equals x by value (CompareWithType == 0, matching cbq Equals).
func ArrayContains(vc *ValComparer, arr, x Val) Val {
	idx, sentinel, ok := ArrayPositionIndex(vc, arr, x)
	if !ok {
		return sentinel
	}
	if idx >= 0 {
		return ValTrue
	}
	return ValFalse
}

// ArrayPositionIndex is the 0-based index of x in arr, or -1 if absent; the
// MISSING/non-array/NULL-x guard is the same as ArrayContains (sentinel/ok).
func ArrayPositionIndex(vc *ValComparer, arr, x Val) (idx int, sentinel Val, ok bool) {
	if len(arr) == 0 || len(x) == 0 {
		return 0, ValMissing, false
	}
	pv, at := Parse(arr)
	if ParseTypeToValType[at] != ValTypeArray {
		return 0, ValNull, false
	}
	if ValKind(x) == ValKindNull {
		return 0, ValNull, false
	}
	xVal, xType := Parse(x)
	found := -1
	i := 0
	jsonparser.ArrayEach(pv, func(e []byte, dt jsonparser.ValueType, _ int, _ error) {
		if found < 0 && vc.CompareWithType(xVal, e, xType, int(dt), 0) == 0 {
			found = i
		}
		i++
	})
	return found, nil, true
}
