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

// appendJSONElem appends one jsonparser-yielded value (val, its dataType dt) to
// out as valid JSON. jsonparser strips a string's surrounding quotes but leaves
// its escapes intact, so a string is re-quoted (the escaped content is copied
// verbatim -- no re-escape); every other type (number/boolean/null/array/object)
// is already valid JSON and is copied as-is. Mirrors the re-quote branch in
// ArrayMinMax (array.go); shared by the OBJECT_VALUES / OBJECT_PAIRS builders.
func appendJSONElem(out, val []byte, dt jsonparser.ValueType) []byte {
	if dt == jsonparser.String {
		out = append(out, '"')
		out = append(out, val...)
		return append(out, '"')
	}
	return append(out, val...)
}

// objectSortedKVs parses an object Val, collects each (name, value, type) into the
// ValComparer's pooled KeyVals, and insertion-sorts them ascending by name (byte
// order) -- the shared front half of OBJECT_VALUES / OBJECT_PAIRS. It returns the
// sorted kvs (still owned by the pool: the caller MUST KeyValsRelease it) plus the
// MISSING/NULL sentinel + ok=false for a MISSING / non-object / malformed operand.
// Names are copied into the pool's reused key backing (ReuseNextKey, no per-row
// key alloc after warmup); value slices point into v and stay valid for the
// caller's single-pass emit before it returns.
func objectSortedKVs(c *ValComparer, v Val) (kvs KeyVals, sentinel Val, ok bool) {
	if len(v) == 0 {
		return nil, ValMissing, false
	}

	inner, pt := Parse(v)
	if ParseTypeToValType[pt] != ValTypeObject {
		return nil, ValNull, false
	}

	kvs = c.KeyValsAcquire(0)

	var iterErr error
	kvs, iterErr = objectPairsInto(kvs, inner)
	if iterErr != nil {
		c.KeyValsRelease(0, kvs)
		return nil, ValNull, false
	}

	kvsSortByName(kvs)

	return kvs, nil, true
}

// objectPairsInto appends v's object (name, value, type) pairs to kvs via one
// ObjectEach pass -- names copied into the pool's reused key backing (ReuseNextKey,
// no per-row key alloc after warmup), value slices pointing into inner (valid for
// the caller's single-pass emit). Does NOT sort. inner must be object bytes. Shared
// by objectSortedKVs (OBJECT_VALUES/PAIRS) and the mutating builders.
func objectPairsInto(kvs KeyVals, inner []byte) (KeyVals, error) {
	err := jsonparser.ObjectEach(inner,
		func(k []byte, val []byte, dt jsonparser.ValueType, _ int) error {
			kCopy := append(ReuseNextKey(kvs), k...)
			kvs = append(kvs, KeyVal{Key: kCopy, Val: val, ValType: int(dt)})
			return nil
		})
	return kvs, err
}

// kvsSortByName insertion-sorts kvs ascending by name (byte order), matching cbq's
// `a < b` name sort (nameList/mapList Less) and the key-sorted JSON serialization
// cbq emits for every object. Object field counts are small, so insertion sort is
// both allocation-free (no sort.Interface boxing) and fast.
func kvsSortByName(kvs KeyVals) {
	for i := 1; i < len(kvs); i++ {
		kv := kvs[i]
		j := i - 1
		for j >= 0 && bytes.Compare(kvs[j].Key, kv.Key) > 0 {
			kvs[j+1] = kvs[j]
			j--
		}
		kvs[j+1] = kv
	}
}

// objectEmit serializes kvs (name/value pairs) as a JSON object `{"k":v,...}` into
// bufPre, sorted by name first (so the output is key-sorted exactly like cbq's
// object serialization). The name is re-encoded via EncodeAsString (kvs names are
// decoded); each value is re-emitted verbatim via appendJSONElem. The shared back
// half of the OBJECT mutating builders.
func objectEmit(c *ValComparer, kvs KeyVals, bufPre []byte) []byte {
	kvsSortByName(kvs)
	c.PrepareEncoder()

	out := append(bufPre[:0], '{')
	for i := 0; i < len(kvs); i++ {
		if i > 0 {
			out = append(out, ',')
		}
		out, _ = c.EncodeAsString(kvs[i].Key, out)
		out = append(out, ':')
		out = appendJSONElem(out, kvs[i].Val, jsonparser.ValueType(kvs[i].ValType))
	}
	out = append(out, '}')
	return out
}

// kvsFind returns the index of the pair named key (decoded) in kvs, or -1.
func kvsFind(kvs KeyVals, key []byte) int {
	for i := range kvs {
		if bytes.Equal(kvs[i].Key, key) {
			return i
		}
	}
	return -1
}

// ObjectValues builds OBJECT_VALUES(obj) -- a JSON array of the object's values,
// ordered by their field name ascending (so the result is deterministic) -- into
// the reused buffer bufPre. Mirrors cbq's ObjectValues.Evaluate: MISSING ->
// MISSING, a non-object -> NULL (the sentinel with ok=false), else the by-name
// sorted values. Each value is re-emitted verbatim (appendJSONElem); only the
// names drive the sort, so no value re-serialization happens.
func ObjectValues(c *ValComparer, v Val, bufPre []byte) (out []byte, sentinel Val, ok bool) {
	kvs, sentinel, ok := objectSortedKVs(c, v)
	if !ok {
		return nil, sentinel, false
	}

	out = append(bufPre[:0], '[')
	for i := 0; i < len(kvs); i++ {
		if i > 0 {
			out = append(out, ',')
		}
		out = appendJSONElem(out, kvs[i].Val, jsonparser.ValueType(kvs[i].ValType))
	}
	out = append(out, ']')

	c.KeyValsRelease(0, kvs)

	return out, nil, true
}

// ObjectPairs builds OBJECT_PAIRS(obj) -- a JSON array of {"name":<k>,"val":<v>}
// objects, one per field, ordered by name ascending -- into the reused buffer
// bufPre. Mirrors cbq's ObjectPairs.Evaluate (1-arg form): MISSING -> MISSING, a
// non-object -> NULL, else the by-name sorted pairs. The "name" key is re-encoded
// via EncodeAsString (the name arrives decoded from ObjectEach, exactly as in
// ObjectNames); the "val" is re-emitted verbatim (appendJSONElem). The object keys
// are emitted "name" then "val" -- already the sorted order cbq serializes.
func ObjectPairs(c *ValComparer, v Val, bufPre []byte) (out []byte, sentinel Val, ok bool) {
	kvs, sentinel, ok := objectSortedKVs(c, v)
	if !ok {
		return nil, sentinel, false
	}

	c.PrepareEncoder()

	out = append(bufPre[:0], '[')
	for i := 0; i < len(kvs); i++ {
		if i > 0 {
			out = append(out, ',')
		}
		out = append(out, `{"name":`...)
		out, _ = c.EncodeAsString(kvs[i].Key, out)
		out = append(out, `,"val":`...)
		out = appendJSONElem(out, kvs[i].Val, jsonparser.ValueType(kvs[i].ValType))
		out = append(out, '}')
	}
	out = append(out, ']')

	c.KeyValsRelease(0, kvs)

	return out, nil, true
}

// objectMutateStart is the shared front half of the mutating builders: it validates
// obj (MISSING -> MISSING sentinel, non-object -> NULL) and collects its pairs into
// a freshly-acquired pooled KeyVals. On ok=false the pool is already released; on
// ok=true the caller MUST objectEmit + KeyValsRelease(0, kvs).
func objectMutateStart(c *ValComparer, obj Val) (kvs KeyVals, sentinel Val, ok bool) {
	if len(obj) == 0 {
		return nil, ValMissing, false
	}
	inner, pt := Parse(obj)
	if ParseTypeToValType[pt] != ValTypeObject {
		return nil, ValNull, false
	}
	kvs = c.KeyValsAcquire(0)
	var err error
	kvs, err = objectPairsInto(kvs, inner)
	if err != nil {
		c.KeyValsRelease(0, kvs)
		return nil, ValNull, false
	}
	return kvs, nil, true
}

// ObjectPut builds OBJECT_PUT(obj, key, val) -- obj with field key set to val --
// into the reused buffer bufPre, key-sorted. Mirrors cbq's ObjectPut: obj OR key
// MISSING -> MISSING; a non-object obj OR non-string key -> NULL; else obj with key
// set. A MISSING val REMOVES the field (cbq's SetField-removes-on-missing); an
// existing key is overwritten in place, a new key is appended.
func ObjectPut(c *ValComparer, obj, key, val Val, bufPre []byte) (out []byte, sentinel Val, ok bool) {
	if len(obj) == 0 || len(key) == 0 {
		return nil, ValMissing, false
	}
	keyDec, keySentinel, keyOk := StrDecode(key)
	if !keyOk {
		return nil, keySentinel, false // non-string key -> NULL (MISSING already handled above).
	}
	kvs, sentinel, ok := objectMutateStart(c, obj)
	if !ok {
		return nil, sentinel, false
	}

	i := kvsFind(kvs, keyDec)
	if len(val) == 0 { // MISSING val -> remove the field (SetField semantics).
		if i >= 0 {
			copy(kvs[i:], kvs[i+1:])
			kvs = kvs[:len(kvs)-1]
		}
	} else {
		pv, pt := Parse(val)
		if i >= 0 {
			kvs[i].Val, kvs[i].ValType = pv, pt // overwrite in place.
		} else {
			kCopy := append(ReuseNextKey(kvs), keyDec...)
			kvs = append(kvs, KeyVal{Key: kCopy, Val: pv, ValType: pt})
		}
	}

	out = objectEmit(c, kvs, bufPre)
	c.KeyValsRelease(0, kvs)
	return out, nil, true
}

// ObjectAdd builds OBJECT_ADD(obj, key, val) -- obj with a NEW field key=val --
// into bufPre, key-sorted. Mirrors cbq's ObjectAdd: obj OR key MISSING -> MISSING;
// a non-object obj OR non-string key -> NULL. Unlike PUT it does NOT overwrite: if
// key already exists (or val is MISSING) obj is returned unchanged (re-emitted
// key-sorted, matching cbq's re-serialization).
func ObjectAdd(c *ValComparer, obj, key, val Val, bufPre []byte) (out []byte, sentinel Val, ok bool) {
	if len(obj) == 0 || len(key) == 0 {
		return nil, ValMissing, false
	}
	keyDec, keySentinel, keyOk := StrDecode(key)
	if !keyOk {
		return nil, keySentinel, false
	}
	kvs, sentinel, ok := objectMutateStart(c, obj)
	if !ok {
		return nil, sentinel, false
	}

	// Add only when the key is absent AND a (non-MISSING) value was supplied; else
	// leave obj unchanged (cbq never overwrites and skips a MISSING value).
	if len(val) > 0 && kvsFind(kvs, keyDec) < 0 {
		pv, pt := Parse(val)
		kCopy := append(ReuseNextKey(kvs), keyDec...)
		kvs = append(kvs, KeyVal{Key: kCopy, Val: pv, ValType: pt})
	}

	out = objectEmit(c, kvs, bufPre)
	c.KeyValsRelease(0, kvs)
	return out, nil, true
}

// ObjectRemove builds OBJECT_REMOVE(obj, key) (2-arg form) -- obj without field key
// -- into bufPre, key-sorted. Mirrors cbq's ObjectRemove: obj OR key MISSING ->
// MISSING; a NULL / non-object obj OR NULL / non-string key -> NULL; else obj with
// key removed (a no-op when key is absent).
func ObjectRemove(c *ValComparer, obj, key Val, bufPre []byte) (out []byte, sentinel Val, ok bool) {
	if len(obj) == 0 || len(key) == 0 {
		return nil, ValMissing, false
	}
	keyDec, keySentinel, keyOk := StrDecode(key)
	if !keyOk {
		return nil, keySentinel, false
	}
	kvs, sentinel, ok := objectMutateStart(c, obj)
	if !ok {
		return nil, sentinel, false
	}

	if i := kvsFind(kvs, keyDec); i >= 0 {
		copy(kvs[i:], kvs[i+1:])
		kvs = kvs[:len(kvs)-1]
	}

	out = objectEmit(c, kvs, bufPre)
	c.KeyValsRelease(0, kvs)
	return out, nil, true
}

// ObjectConcat builds OBJECT_CONCAT(obj1, obj2) (2-arg form) -- the two objects
// merged, obj2's fields winning on a name clash -- into bufPre, key-sorted. Mirrors
// cbq's ObjectConcat: a MISSING operand -> MISSING; a non-object operand -> NULL;
// else the union with obj2 overwriting obj1.
func ObjectConcat(c *ValComparer, obj1, obj2 Val, bufPre []byte) (out []byte, sentinel Val, ok bool) {
	if len(obj1) == 0 || len(obj2) == 0 {
		return nil, ValMissing, false
	}
	inner2, pt2 := Parse(obj2)
	if ParseTypeToValType[pt2] != ValTypeObject {
		return nil, ValNull, false
	}
	kvs, sentinel, ok := objectMutateStart(c, obj1)
	if !ok {
		return nil, sentinel, false
	}

	// Merge obj2: overwrite a matching name in place, else append (obj2 wins).
	mergeErr := jsonparser.ObjectEach(inner2,
		func(k []byte, val []byte, dt jsonparser.ValueType, _ int) error {
			if i := kvsFind(kvs, k); i >= 0 {
				kvs[i].Val, kvs[i].ValType = val, int(dt)
			} else {
				kCopy := append(ReuseNextKey(kvs), k...)
				kvs = append(kvs, KeyVal{Key: kCopy, Val: val, ValType: int(dt)})
			}
			return nil
		})
	if mergeErr != nil {
		c.KeyValsRelease(0, kvs)
		return nil, ValNull, false
	}

	out = objectEmit(c, kvs, bufPre)
	c.KeyValsRelease(0, kvs)
	return out, nil, true
}
