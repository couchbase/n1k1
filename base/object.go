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

	"github.com/buger/jsonparser"
)

// Length reader op-codes for LengthReader.
const (
	LenObject = iota // OBJECT_LENGTH: number of object name/value pairs.
	LenPoly          // POLY_LENGTH: string bytes / array elements / object pairs.
)

// LengthReader counts v per op-code and returns the count as an int, or ok=false
// with the MISSING/NULL sentinel Val the caller should yield. It iterates the
// bytes (no materializing / boxing), mirroring cbq:
//
//   - OBJECT_LENGTH (LenObject): MISSING -> MISSING, non-object -> NULL, else the
//     number of name/value pairs.
//   - POLY_LENGTH (LenPoly): MISSING -> MISSING; a string -> its DECODED byte
//     length (cbq's len(arg.ToString())); an array -> element count; an object ->
//     pair count; number/boolean/null/other -> NULL.
func LengthReader(op int, v Val) (n int, sentinel Val, ok bool) {
	if len(v) == 0 {
		return 0, ValMissing, false
	}

	inner, pt := Parse(v)
	vt := ParseTypeToValType[pt]

	if op == LenObject {
		if vt != ValTypeObject {
			return 0, ValNull, false
		}
		return objectPairCount(inner), nil, true
	}

	// LenPoly.
	switch vt {
	case ValTypeString:
		decoded, err := jsonparser.Unescape(inner, nil)
		if err != nil {
			return 0, ValNull, false
		}
		return len(decoded), nil, true
	case ValTypeArray:
		return arrayElemCount(inner), nil, true
	case ValTypeObject:
		return objectPairCount(inner), nil, true
	}

	return 0, ValNull, false
}

// objectPairCount returns the number of top-level name/value pairs in the object
// bytes inner (a `{...}` from Parse).
func objectPairCount(inner []byte) int {
	n := 0
	jsonparser.ObjectEach(inner, func(_, _ []byte, _ jsonparser.ValueType, _ int) error {
		n++
		return nil
	})
	return n
}

// arrayElemCount returns the number of top-level elements in the array bytes
// inner (a `[...]` from Parse).
func arrayElemCount(inner []byte) int {
	n := 0
	jsonparser.ArrayEach(inner, func(_ []byte, _ jsonparser.ValueType, _ int, _ error) {
		n++
	})
	return n
}

// ObjectNames builds the OBJECT_NAMES(obj) result -- a JSON array of the object's
// field names, SORTED ascending by byte order -- into the reused buffer bufPre,
// returning (out, nil, true). It mirrors cbq's ObjectNames.Evaluate: MISSING ->
// MISSING, a non-object -> NULL (returned as the sentinel with ok=false), else the
// sorted name array. cbq sorts the DECODED key strings with Go's `a < b` (byte
// order); this reuses the ValComparer's pooled KeyVals machinery (as
// CanonicalJSON does) -- ObjectEach hands back each name already unescaped, the
// names are copied into the pool's reused key backing (ReuseNextKey, so no
// per-row key allocation after warmup), sorted by bytes.Compare (identical
// ordering), and re-encoded as JSON strings via EncodeAsString. Only the names
// are read, so the values are ignored (no value-serialization fidelity concern).
//
// Note the EncodeStr formfeed/backspace encoder caveat (see EncodeAsString /
// DESIGN-exprs.md) applies to any name containing a literal 0x0C / 0x08.
func ObjectNames(c *ValComparer, v Val, bufPre []byte) (out []byte, sentinel Val, ok bool) {
	if len(v) == 0 {
		return nil, ValMissing, false
	}

	inner, pt := Parse(v)
	if ParseTypeToValType[pt] != ValTypeObject {
		return nil, ValNull, false
	}

	kvs := c.KeyValsAcquire(0)

	iterErr := jsonparser.ObjectEach(inner,
		func(k []byte, _ []byte, _ jsonparser.ValueType, _ int) error {
			kCopy := append(ReuseNextKey(kvs), k...)
			kvs = append(kvs, KeyVal{Key: kCopy})
			return nil
		})
	if iterErr != nil {
		c.KeyValsRelease(0, kvs)
		return nil, ValNull, false
	}

	c.PrepareEncoder()

	// Insertion sort the names ascending by byte order (matching cbq's `a < b`
	// name sort). A concrete in-place sort avoids the sort.Interface boxing that
	// sort.Sort(kvs) would heap-allocate every row -- object field counts are
	// small, so insertion sort is both allocation-free and fast here.
	for i := 1; i < len(kvs); i++ {
		kv := kvs[i]
		j := i - 1
		for j >= 0 && bytes.Compare(kvs[j].Key, kv.Key) > 0 {
			kvs[j+1] = kvs[j]
			j--
		}
		kvs[j+1] = kv
	}

	out = append(bufPre[:0], '[')

	for i := 0; i < len(kvs); i++ {
		if i > 0 {
			out = append(out, ',')
		}
		out, _ = c.EncodeAsString(kvs[i].Key, out)
	}

	out = append(out, ']')

	c.KeyValsRelease(0, kvs)

	return out, nil, true
}
