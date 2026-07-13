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
	"strconv"

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
		return ObjectPairCount(inner), nil, true
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
		return ArrayElemCount(inner), nil, true
	case ValTypeObject:
		return ObjectPairCount(inner), nil, true
	}

	return 0, ValNull, false
}

// ObjectPairCount returns the number of top-level name/value pairs in the object
// bytes inner (a `{...}` from Parse).
func ObjectPairCount(inner []byte) int {
	n := 0
	jsonparser.ObjectEach(inner, func(_, _ []byte, _ jsonparser.ValueType, _ int) error {
		n++
		return nil
	})
	return n
}

// ArrayElemCount returns the number of top-level elements in the array bytes
// inner (a `[...]` from Parse).
func ArrayElemCount(inner []byte) int {
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
			kCopy := append(KeyValsReuseNextKey(kvs), k...)
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

// JSONElementAppend appends one jsonparser-yielded value (val, its dataType dt) to
// out as valid JSON. jsonparser strips a string's surrounding quotes but leaves
// its escapes intact, so a string is re-quoted (the escaped content is copied
// verbatim -- no re-escape); every other type (number/boolean/null/array/object)
// is already valid JSON and is copied as-is. Mirrors the re-quote branch in
// ArrayMinMax (array.go); shared by the OBJECT_VALUES / OBJECT_PAIRS builders.
func JSONElementAppend(out, val []byte, dt jsonparser.ValueType) []byte {
	if dt == jsonparser.String {
		out = append(out, '"')
		out = append(out, val...)
		return append(out, '"')
	}
	return append(out, val...)
}

// ObjectSortedKVs parses an object Val, collects each (name, value, type) into the
// ValComparer's pooled KeyVals, and insertion-sorts them ascending by name (byte
// order) -- the shared front half of OBJECT_VALUES / OBJECT_PAIRS. It returns the
// sorted kvs (still owned by the pool: the caller MUST KeyValsRelease it) plus the
// MISSING/NULL sentinel + ok=false for a MISSING / non-object / malformed operand.
// Names are copied into the pool's reused key backing (ReuseNextKey, no per-row
// key alloc after warmup); value slices point into v and stay valid for the
// caller's single-pass emit before it returns.
func ObjectSortedKVs(c *ValComparer, v Val) (kvs KeyVals, sentinel Val, ok bool) {
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

	KeyValsSortByName(kvs)

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
			kCopy := append(KeyValsReuseNextKey(kvs), k...)
			kvs = append(kvs, KeyVal{Key: kCopy, Val: val, ValType: int(dt)})
			return nil
		})
	return kvs, err
}

// KeyValsSortByName insertion-sorts kvs ascending by name (byte order), matching cbq's
// `a < b` name sort (nameList/mapList Less) and the key-sorted JSON serialization
// cbq emits for every object. Object field counts are small, so insertion sort is
// both allocation-free (no sort.Interface boxing) and fast.
func KeyValsSortByName(kvs KeyVals) {
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

// ObjectEmit serializes kvs (name/value pairs) as a JSON object `{"k":v,...}` into
// bufPre, sorted by name first (so the output is key-sorted exactly like cbq's
// object serialization). The name is re-encoded via EncodeAsString (kvs names are
// decoded); each value is re-emitted verbatim via appendJSONElem. The shared back
// half of the OBJECT mutating builders.
func ObjectEmit(c *ValComparer, kvs KeyVals, bufPre []byte) []byte {
	KeyValsSortByName(kvs)
	c.PrepareEncoder()

	out := append(bufPre[:0], '{')
	for i := 0; i < len(kvs); i++ {
		if i > 0 {
			out = append(out, ',')
		}
		out, _ = c.EncodeAsString(kvs[i].Key, out)
		out = append(out, ':')
		out = JSONElementAppend(out, kvs[i].Val, jsonparser.ValueType(kvs[i].ValType))
	}
	out = append(out, '}')
	return out
}

// CollObjectPut accumulates one (name, value) pair for an OBJECT comprehension into
// kvs with LAST-WINS dedup (matching cbq's map[key]=val overwrite). name is a raw
// JSON string Val (WITH quotes) stored verbatim as the sort/emit key; value is any
// JSON Val stored verbatim. Both are copied (they alias per-element reused buffers);
// backing is reused across rows when kvs (reset via kvs[:0]) has spare cap.
func CollObjectPut(kvs KeyVals, name, value Val) KeyVals {
	for i := range kvs {
		if bytes.Equal(kvs[i].Key, name) {
			kvs[i].Val = append(kvs[i].Val[:0], value...)
			return kvs
		}
	}
	n := len(kvs)
	if cap(kvs) > n { // reuse the next slot's backing (kvs was reset with kvs[:0])
		kvs = kvs[:n+1]
		kvs[n].Key = append(kvs[n].Key[:0], name...)
		kvs[n].Val = append(kvs[n].Val[:0], value...)
		return kvs
	}
	return append(kvs, KeyVal{
		Key: append([]byte(nil), name...),
		Val: append([]byte(nil), value...),
	})
}

// CollObjectEmit serializes kvs as a key-sorted JSON object `{"name":value,...}`
// into bufPre -- the back half of the OBJECT comprehension. Keys (raw JSON strings)
// and values are emitted VERBATIM (both already valid JSON), sorted by key so the
// output matches cbq's canonical key-sorted serialization (for canonical inputs).
func CollObjectEmit(kvs KeyVals, bufPre []byte) []byte {
	KeyValsSortByName(kvs) // sorts ascending by Key bytes (the raw "name")
	out := append(bufPre[:0], '{')
	for i := range kvs {
		if i > 0 {
			out = append(out, ',')
		}
		out = append(out, kvs[i].Key...)
		out = append(out, ':')
		out = append(out, kvs[i].Val...)
	}
	return append(out, '}')
}

// DescendantsAppend emits v's descendants as a JSON array into bufPre, in cbq
// value.Descendants order: a pre-order DFS where each child is emitted, then its own
// descendants -- array elements in order, object field VALUES in sorted-name order,
// and container children (arrays/objects) are included as descendants themselves.
// Used by WITHIN comprehensions (the binding's Descend flag). Strings are re-quoted;
// everything else is emitted verbatim. depth indexes the pooled KeyVals so nested
// object sorts don't clobber an enclosing one.
func DescendantsAppend(c *ValComparer, v Val, bufPre []byte) []byte {
	out := append(bufPre[:0], '[')
	_, out = descendChildren(c, v, out, false, 0)
	return append(out, ']')
}

// CollElems materializes a comprehension binding's members into dst as a flat Vals
// with a stride, so a comprehension op can iterate with a plain indexed for-loop
// (no per-element callback -- keeps the expr codegen able to emit it). Value-only
// (named=false): each array element, stride 1. Named: [name, value] per member,
// stride 2 -- object fields in sorted-name order (name = JSON string), array with
// the integer index. Returns (dst, stride, ok); ok=false for a non-iterable (value-
// only: non-array; named: neither object nor array) which the caller maps to NULL.
// Element/value bytes alias coll (stable for the row); strings/names are re-quoted.
func CollElems(c *ValComparer, coll Val, named bool, dst Vals) (Vals, int, bool) {
	pv, pt := Parse(coll)
	vt := ParseTypeToValType[pt]
	if !named {
		if vt != ValTypeArray {
			return dst, 1, false
		}
		jsonparser.ArrayEach(pv, func(e []byte, dt jsonparser.ValueType, _ int, _ error) {
			if dt == jsonparser.String {
				e = collQuote(e)
			}
			dst = append(dst, Val(e))
		})
		return dst, 1, true
	}
	switch vt {
	case ValTypeObject:
		kvs := c.KeyValsAcquire(0)
		jsonparser.ObjectEach(pv, func(key []byte, e []byte, dt jsonparser.ValueType, _ int) error {
			kvs = append(kvs, KeyVal{Key: key, Val: e, ValType: int(dt)})
			return nil
		})
		KeyValsSortByName(kvs)
		for i := range kvs {
			val := kvs[i].Val
			if kvs[i].ValType == int(jsonparser.String) {
				val = collQuote(kvs[i].Val)
			}
			dst = append(dst, collQuote(kvs[i].Key), Val(val))
		}
		c.KeyValsRelease(0, kvs)
		return dst, 2, true
	case ValTypeArray:
		idx := 0
		jsonparser.ArrayEach(pv, func(e []byte, dt jsonparser.ValueType, _ int, _ error) {
			if dt == jsonparser.String {
				e = collQuote(e)
			}
			dst = append(dst, Val(strconv.AppendInt(nil, int64(idx), 10)), Val(e))
			idx++
		})
		return dst, 2, true
	}
	return dst, 2, false
}

// collQuote returns a fresh JSON string Val `"s"` (s is the unquoted content).
func collQuote(s []byte) Val {
	q := make([]byte, 0, len(s)+2)
	q = append(q, '"')
	q = append(q, s...)
	q = append(q, '"')
	return Val(q)
}

// Descendants is the WITHIN operand transform: MISSING -> MISSING; an array or
// object -> its DescendantsAppend array; anything else (NULL / scalar) -> NULL --
// so a comprehension binding `x WITHIN v` iterates v's descendants and inherits the
// same MISSING/NULL/non-collection guards it would have for `x IN v`. Signature
// matches the unary-array harness (engine.ExprArrayUnaryBuf).
func Descendants(c *ValComparer, v Val, bufPre []byte) (Val, []byte) {
	if len(v) == 0 {
		return ValMissing, bufPre
	}
	_, pt := Parse(v)
	vt := ParseTypeToValType[pt]
	if vt != ValTypeArray && vt != ValTypeObject {
		return ValNull, bufPre
	}
	out := DescendantsAppend(c, v, bufPre)
	return Val(out), out
}

func descendChildren(c *ValComparer, v Val, out []byte, wrote bool, depth int) (bool, []byte) {
	pv, pt := Parse(v)
	switch ParseTypeToValType[pt] {
	case ValTypeArray:
		jsonparser.ArrayEach(pv, func(e []byte, dt jsonparser.ValueType, _ int, _ error) {
			wrote, out = descendEmit(c, e, dt, out, wrote, depth)
		})
	case ValTypeObject:
		kvs := c.KeyValsAcquire(depth)
		jsonparser.ObjectEach(pv, func(key []byte, e []byte, dt jsonparser.ValueType, _ int) error {
			kvs = append(kvs, KeyVal{Key: key, Val: e, ValType: int(dt)})
			return nil
		})
		KeyValsSortByName(kvs)
		for i := range kvs { // field VALUES, sorted by name; recurse a level deeper
			wrote, out = descendEmit(c, kvs[i].Val, jsonparser.ValueType(kvs[i].ValType), out, wrote, depth+1)
		}
		c.KeyValsRelease(depth, kvs)
	}
	return wrote, out
}

// descendEmit appends one descendant e (re-quoting a string) then, if e is a
// container, recurses into ITS descendants (the pre-order "child, then subtree").
func descendEmit(c *ValComparer, e []byte, dt jsonparser.ValueType, out []byte, wrote bool, depth int) (bool, []byte) {
	if wrote {
		out = append(out, ',')
	}
	out = JSONElementAppend(out, e, dt)
	wrote = true
	if dt == jsonparser.Array || dt == jsonparser.Object {
		wrote, out = descendChildren(c, e, out, wrote, depth)
	}
	return wrote, out
}

// KeyValsFindKey returns the index of the pair named key (decoded) in kvs, or -1.
func KeyValsFindKey(kvs KeyVals, key []byte) int {
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
	kvs, sentinel, ok := ObjectSortedKVs(c, v)
	if !ok {
		return nil, sentinel, false
	}

	out = append(bufPre[:0], '[')
	for i := 0; i < len(kvs); i++ {
		if i > 0 {
			out = append(out, ',')
		}
		out = JSONElementAppend(out, kvs[i].Val, jsonparser.ValueType(kvs[i].ValType))
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
	kvs, sentinel, ok := ObjectSortedKVs(c, v)
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
		out = JSONElementAppend(out, kvs[i].Val, jsonparser.ValueType(kvs[i].ValType))
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

	i := KeyValsFindKey(kvs, keyDec)
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
			kCopy := append(KeyValsReuseNextKey(kvs), keyDec...)
			kvs = append(kvs, KeyVal{Key: kCopy, Val: pv, ValType: pt})
		}
	}

	out = ObjectEmit(c, kvs, bufPre)
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
	if len(val) > 0 && KeyValsFindKey(kvs, keyDec) < 0 {
		pv, pt := Parse(val)
		kCopy := append(KeyValsReuseNextKey(kvs), keyDec...)
		kvs = append(kvs, KeyVal{Key: kCopy, Val: pv, ValType: pt})
	}

	out = ObjectEmit(c, kvs, bufPre)
	c.KeyValsRelease(0, kvs)
	return out, nil, true
}

// ObjectRemoveVals builds OBJECT_REMOVE(obj, key1, key2, ...) -- obj without each
// named field -- into bufPre, key-sorted. vals[0] is the object; vals[1:] are the
// string keys. Mirrors cbq's ObjectRemove (variadic, MinArgs 2): a MISSING operand ->
// MISSING; a NULL / non-object obj OR any NULL / non-string key -> NULL; else obj with
// those keys removed (a no-op for an absent key).
func ObjectRemoveVals(c *ValComparer, vals Vals, bufPre []byte) (Val, []byte) {
	for _, v := range vals { // MISSING anywhere dominates.
		if len(v) == 0 {
			return ValMissing, bufPre
		}
	}
	kvs, sentinel, ok := objectMutateStart(c, vals[0])
	if !ok {
		return sentinel, bufPre
	}

	for _, key := range vals[1:] {
		keyDec, keySentinel, keyOk := StrDecode(key)
		if !keyOk {
			c.KeyValsRelease(0, kvs)
			return keySentinel, bufPre
		}
		if i := KeyValsFindKey(kvs, keyDec); i >= 0 {
			copy(kvs[i:], kvs[i+1:])
			kvs = kvs[:len(kvs)-1]
		}
	}

	out := ObjectEmit(c, kvs, bufPre)
	c.KeyValsRelease(0, kvs)
	return Val(out), out
}

// ObjectConcatVals builds OBJECT_CONCAT(obj1, obj2, ...) -- all the objects merged,
// a later object's fields winning on a name clash -- into bufPre, key-sorted. Mirrors
// cbq's ObjectConcat (variadic, MinArgs 2): a MISSING operand -> MISSING; any
// non-object operand -> NULL; else the union with later objects overwriting earlier.
func ObjectConcatVals(c *ValComparer, vals Vals, bufPre []byte) (Val, []byte) {
	for _, v := range vals {
		if len(v) == 0 {
			return ValMissing, bufPre
		}
	}
	kvs, sentinel, ok := objectMutateStart(c, vals[0])
	if !ok {
		return sentinel, bufPre
	}

	for _, obj := range vals[1:] {
		inner, pt := Parse(obj)
		if ParseTypeToValType[pt] != ValTypeObject {
			c.KeyValsRelease(0, kvs)
			return ValNull, bufPre
		}
		// Merge: overwrite a matching name in place, else append (this obj wins).
		mergeErr := jsonparser.ObjectEach(inner,
			func(k []byte, val []byte, dt jsonparser.ValueType, _ int) error {
				if i := KeyValsFindKey(kvs, k); i >= 0 {
					kvs[i].Val, kvs[i].ValType = val, int(dt)
				} else {
					kCopy := append(KeyValsReuseNextKey(kvs), k...)
					kvs = append(kvs, KeyVal{Key: kCopy, Val: val, ValType: int(dt)})
				}
				return nil
			})
		if mergeErr != nil {
			c.KeyValsRelease(0, kvs)
			return ValNull, bufPre
		}
	}

	out := ObjectEmit(c, kvs, bufPre)
	c.KeyValsRelease(0, kvs)
	return Val(out), out
}

// ObjectConstructVals builds an OBJECT literal `{"k0":v0, ...}` from the evaluated
// name/value operands into bufPre, key-sorted. vals is a FLAT alternating slice
// [name0, val0, name1, val1, ...]. Mirrors cbq's ObjectConstruct: a MISSING or NULL
// name SKIPS its pair; a non-string name -> the whole result NULL; a MISSING value
// SKIPS its pair; a later duplicate name overwrites (last wins, via CollObjectPut).
// The name Val keeps its JSON quotes and is emitted as the key. Pool-backed (zero
// steady-state garbage), like ObjectConcatVals.
func ObjectConstructVals(c *ValComparer, vals Vals, bufPre []byte) (Val, []byte) {
	kvs := c.KeyValsAcquire(0)

	for i := 0; i+1 < len(vals); i += 2 {
		name, val := vals[i], vals[i+1]

		switch ValKind(name) {
		case ValKindMissing, ValKindNull: // MISSING/NULL name -> skip this pair.
			continue
		}
		if !ValIsString(name) { // non-string name -> whole result NULL.
			c.KeyValsRelease(0, kvs)
			return ValNull, bufPre
		}
		if len(val) == 0 { // MISSING value -> skip this pair.
			continue
		}

		kvs = CollObjectPut(kvs, name, val)
	}

	out := CollObjectEmit(kvs, bufPre)
	c.KeyValsRelease(0, kvs)
	return Val(out), out
}
