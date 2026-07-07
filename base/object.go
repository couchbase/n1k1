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

import "github.com/buger/jsonparser"

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
