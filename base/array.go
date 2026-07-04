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
