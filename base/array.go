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
	"bytes"
	"math"

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

// ArrayTrimSpace returns the element-list bytes of a JSON array (the content between
// the outer '[' and ']', outer whitespace trimmed) -- e.g. `[1, 2]` -> `1, 2`, and
// an empty array -> empty. arr must be array bytes (a `[...]` token from Parse; callers guard the
// type first). The returned slice points into arr (no copy). Note this preserves any
// INNER element formatting; the builders assume canonical-ish JSON input (the same
// assumption as ArrayMinMax's element re-emit), so the spliced result matches cbq's
// canonical serialization for canonical inputs.
func ArrayTrimSpace(arr []byte) []byte {
	return bytes.TrimSpace(arr[1 : len(arr)-1])
}

// ArrayAppend builds ARRAY_APPEND(arr, val) -- arr with val appended as its last
// ArrayAppendVals builds ARRAY_APPEND(arr, val1, val2, ...) -- arr with each valN
// appended in order -- into the reused buffer bufPre. vals[0] is the array; vals[1:]
// are the elements. cbq ARRAY_APPEND (variadic, MinArgs 2): a MISSING operand ->
// MISSING (precedence over NULL); a non-array vals[0] -> NULL; else arr + the values.
// Each value is a complete Val (already valid JSON, e.g. `"x"` keeps its quotes) and
// is spliced verbatim -- a NULL value IS appended (only MISSING short-circuits). See
// the arrayElems canonical-input note.
func ArrayAppendVals(vals Vals, bufPre []byte) (out []byte, sentinel Val, ok bool) {
	for _, v := range vals { // MISSING anywhere dominates.
		if len(v) == 0 {
			return nil, ValMissing, false
		}
	}
	pv, pt := Parse(vals[0])
	if ParseTypeToValType[pt] != ValTypeArray {
		return nil, ValNull, false
	}
	elems := ArrayTrimSpace(pv)

	out = append(bufPre[:0], '[')
	out = append(out, elems...)
	wrote := len(elems) > 0
	for _, v := range vals[1:] {
		if wrote {
			out = append(out, ',')
		}
		out = append(out, v...)
		wrote = true
	}
	out = append(out, ']')
	return out, nil, true
}

// ArrayPrependVals builds ARRAY_PREPEND(val1, val2, ..., arr) -- arr with each valN
// inserted (in order) before its elements -- into bufPre. cbq operand order puts the
// array LAST (vals[len-1]); vals[:len-1] are the prepended values. cbq ARRAY_PREPEND
// (variadic, MinArgs 2): a MISSING operand -> MISSING; a non-array last operand ->
// NULL; else the values ++ arr. Values are spliced verbatim (a NULL value IS prepended).
func ArrayPrependVals(vals Vals, bufPre []byte) (out []byte, sentinel Val, ok bool) {
	for _, v := range vals {
		if len(v) == 0 {
			return nil, ValMissing, false
		}
	}
	n := len(vals) - 1 // the array is the LAST operand.
	pv, pt := Parse(vals[n])
	if ParseTypeToValType[pt] != ValTypeArray {
		return nil, ValNull, false
	}
	elems := ArrayTrimSpace(pv)

	out = append(bufPre[:0], '[')
	wrote := false
	for _, v := range vals[:n] {
		if wrote {
			out = append(out, ',')
		}
		out = append(out, v...)
		wrote = true
	}
	if wrote && len(elems) > 0 {
		out = append(out, ',')
	}
	out = append(out, elems...)
	out = append(out, ']')
	return out, nil, true
}

// ArrayConcatVals builds ARRAY_CONCAT(arr1, arr2, ...) -- all the arrays' elements
// joined in order -- into bufPre. cbq ARRAY_CONCAT (variadic, MinArgs 2): a MISSING
// operand -> MISSING; ANY non-array operand -> NULL (missing takes precedence); else
// the concatenation.
func ArrayConcatVals(vals Vals, bufPre []byte) (out []byte, sentinel Val, ok bool) {
	for _, v := range vals {
		if len(v) == 0 {
			return nil, ValMissing, false
		}
	}
	for _, v := range vals { // ANY non-array -> NULL.
		if _, pt := Parse(v); ParseTypeToValType[pt] != ValTypeArray {
			return nil, ValNull, false
		}
	}

	out = append(bufPre[:0], '[')
	wrote := false
	for _, v := range vals {
		pv, _ := Parse(v)
		elems := ArrayTrimSpace(pv)
		if len(elems) > 0 {
			if wrote {
				out = append(out, ',')
			}
			out = append(out, elems...)
			wrote = true
		}
	}
	out = append(out, ']')
	return out, nil, true
}

// arrayCollectElems appends each element of the array-token bytes inner into the
// ValComparer's pooled KeyVals at depth 0 (Val = element bytes as jsonparser yields
// them -- strings arrive unquoted -- ValType = its jsonparser type). The element
// slices alias inner (valid for the row), so there is no per-element copy. The
// caller MUST KeyValsRelease(0, kvs) the returned slice.
func arrayCollectElems(c *ValComparer, inner []byte) KeyVals {
	kvs := c.KeyValsAcquire(0)
	jsonparser.ArrayEach(inner, func(e []byte, dt jsonparser.ValueType, _ int, _ error) {
		kvs = append(kvs, KeyVal{Val: e, ValType: int(dt)})
	})
	return kvs
}

// arrayEmitKVs serializes kvs (in their current order) as a JSON array `[...]` into
// bufPre; each element is re-emitted verbatim, re-quoting strings (JSONElementAppend).
func arrayEmitKVs(kvs KeyVals, bufPre []byte) []byte {
	out := append(bufPre[:0], '[')
	for i := range kvs {
		if i > 0 {
			out = append(out, ',')
		}
		out = JSONElementAppend(out, kvs[i].Val, jsonparser.ValueType(kvs[i].ValType))
	}
	return append(out, ']')
}

// arraySortByCollation insertion-sorts kvs ascending by N1QL collation, matching
// cbq ARRAY_SORT (value.Sorter over Collate). It compares at pool depth 1 so the
// nested string/object compares (which acquire the pool) don't reuse the depth-0
// slice that holds these very elements. Insertion sort is allocation-free (no
// sort.Interface boxing); for canonical inputs the result is byte-identical to cbq's
// (collation-equal elements are byte-equal, so tie order is immaterial).
func arraySortByCollation(c *ValComparer, kvs KeyVals) {
	for i := 1; i < len(kvs); i++ {
		kv := kvs[i]
		j := i - 1
		for j >= 0 && c.CompareWithType(kvs[j].Val, kv.Val, kvs[j].ValType, kv.ValType, 1) > 0 {
			kvs[j+1] = kvs[j]
			j--
		}
		kvs[j+1] = kv
	}
}

// ArraySort builds ARRAY_SORT(arr) -- arr's elements in N1QL collation order -- into
// bufPre. cbq skeleton: MISSING -> MISSING, non-array -> NULL; else the sorted array
// (NULL elements included, sorting below everything, as in cbq). Element bytes are
// re-emitted verbatim (see the canonical-input note on ArrayAppend/ArrayTrimSpace).
func ArraySort(c *ValComparer, v Val, bufPre []byte) (Val, []byte) {
	if len(v) == 0 {
		return ValMissing, bufPre
	}
	pv, pt := Parse(v)
	if ParseTypeToValType[pt] != ValTypeArray {
		return ValNull, bufPre
	}
	kvs := arrayCollectElems(c, pv)
	arraySortByCollation(c, kvs)
	out := arrayEmitKVs(kvs, bufPre)
	c.KeyValsRelease(0, kvs)
	return Val(out), out
}

// ArrayReverse builds ARRAY_REVERSE(arr) -- arr's elements in reverse order -- into
// bufPre. cbq skeleton: MISSING -> MISSING, non-array -> NULL. Element bytes are
// re-emitted verbatim (canonical-input note as above). The ValComparer is used only
// for its pooled element scratch.
func ArrayReverse(c *ValComparer, v Val, bufPre []byte) (Val, []byte) {
	if len(v) == 0 {
		return ValMissing, bufPre
	}
	pv, pt := Parse(v)
	if ParseTypeToValType[pt] != ValTypeArray {
		return ValNull, bufPre
	}
	kvs := arrayCollectElems(c, pv)
	out := append(bufPre[:0], '[')
	for i := len(kvs) - 1; i >= 0; i-- {
		if i < len(kvs)-1 {
			out = append(out, ',')
		}
		out = JSONElementAppend(out, kvs[i].Val, jsonparser.ValueType(kvs[i].ValType))
	}
	out = append(out, ']')
	c.KeyValsRelease(0, kvs)
	return Val(out), out
}

// ArrayFlatten builds ARRAY_FLATTEN(arr, depth) -- arr with nested arrays spliced in
// up to depth levels deep -- into bufPre. cbq skeleton: MISSING arr OR depth ->
// MISSING; a non-array arr OR non-number depth -> NULL; a non-integer depth -> NULL;
// else the flattened array. depth 0 is a shallow copy; a NEGATIVE depth flattens
// fully (cbq recurses whenever depth != 0, decrementing, so it never reaches the 0
// stop). Element bytes are re-emitted verbatim (canonical-input note as above).
func ArrayFlatten(arr, depth Val, bufPre []byte) (out []byte, sentinel Val, ok bool) {
	if len(arr) == 0 || len(depth) == 0 {
		return nil, ValMissing, false
	}
	pv, pt := Parse(arr)
	if ParseTypeToValType[pt] != ValTypeArray {
		return nil, ValNull, false
	}
	dv, dt := Parse(depth)
	if ParseTypeToValType[dt] != ValTypeNumber {
		return nil, ValNull, false
	}
	df, err := ParseFloat64(dv)
	if err != nil || math.Trunc(df) != df { // depth must be an integer
		return nil, ValNull, false
	}

	out = append(bufPre[:0], '[')
	out, _ = arrayFlattenInto(out, pv, int(df), false)
	out = append(out, ']')
	return out, nil, true
}

// arrayFlattenInto appends the elements of the array-token bytes arrInner to out
// (without the enclosing brackets), recursing into array elements while depth > 0
// (mirroring cbq's arrayFlattenInto). wrote tracks whether a comma is needed; the
// updated (out, wrote) are returned.
func arrayFlattenInto(out, arrInner []byte, depth int, wrote bool) ([]byte, bool) {
	jsonparser.ArrayEach(arrInner, func(e []byte, dt jsonparser.ValueType, _ int, _ error) {
		if dt == jsonparser.Array && depth != 0 { // depth==0 stops; negative flattens fully (cbq)
			out, wrote = arrayFlattenInto(out, e, depth-1, wrote)
			return
		}
		if wrote {
			out = append(out, ',')
		}
		out = JSONElementAppend(out, e, dt)
		wrote = true
	})
	return out, wrote
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
