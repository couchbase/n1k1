//  Copyright (c) 2019 Couchbase, Inc.
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
	"encoding/json"
	"sort"

	"github.com/buger/jsonparser"
)

// The ordering of ValType's here is intended to match N1QL's ordering
// of value types.
const (
	ValTypeMissing = iota
	ValTypeNull
	ValTypeBoolean
	ValTypeNumber
	ValTypeString
	ValTypeArray
	ValTypeObject
	ValTypeUnknown // Ex: BINARY.
)

// ParseTypeToValType provides a mapping from jsonparser's type
// numbers to ValType's numbers.
var ParseTypeToValType = []int{
	jsonparser.NotExist: ValTypeMissing,
	jsonparser.Null:     ValTypeNull,
	jsonparser.Boolean:  ValTypeBoolean,
	jsonparser.Number:   ValTypeNumber,
	jsonparser.String:   ValTypeString,
	jsonparser.Array:    ValTypeArray,
	jsonparser.Object:   ValTypeObject,
	jsonparser.Unknown:  ValTypeUnknown, // Ex: BINARY.
}

// ---------------------------------------------

// Parse is a wrapper around jsonparser.Get().
func Parse(b []byte) (parseVal []byte, parseType int) {
	if len(b) == 0 {
		return nil, int(jsonparser.NotExist) // ValTypeMissing.
	}

	v, vt, _, err := jsonparser.Get(b)
	if err != nil {
		return b, int(jsonparser.Unknown)
	}

	return v, int(vt)
}

// ParseTypeHasValue returns true if the parseType is not missing and
// is not null.
func ParseTypeHasValue(parseType int) bool {
	return ParseTypeToValType[parseType] > ValTypeNull
}

// ParseFloat64 is a wrapper around jsonparser.ParseFloat().
func ParseFloat64(v []byte) (float64, error) {
	return jsonparser.ParseFloat(v)
}

// ---------------------------------------------

// ValComparer holds data structures needed to compare JSON, so that a
// single, reused ValComparer can avoid repeated memory
// allocations. An instance of ValComparer is not concurrent safe.
type ValComparer struct {
	KeyVals []KeyVals // Indexed by depth.

	Buffer bytes.Buffer

	Bytes []byte

	Encoder *json.Encoder
}

// NewValComparer returns a ready-to-use ValComparer.
func NewValComparer() *ValComparer { return &ValComparer{} }

// PrepareEncoder initializes the json encoder of a ValComparer if not
// already initialized.
func (c *ValComparer) PrepareEncoder() {
	if c.Encoder == nil {
		c.Encoder = json.NewEncoder(&c.Buffer)
	}
}

// ---------------------------------------------

// Compare returns < 0 if a < b, 0 if a == b, and > 0 if a > b.
func (c *ValComparer) Compare(a, b Val) int {
	aValue, aValueType, _, aErr := jsonparser.Get(a)
	bValue, bValueType, _, bErr := jsonparser.Get(b)

	if aErr != nil || bErr != nil {
		return CompareErr(aErr, bErr)
	}

	return c.CompareWithType(aValue, bValue, int(aValueType), int(bValueType), 0)
}

func (c *ValComparer) CompareWithType(aValue, bValue []byte,
	aValueType, bValueType int, depth int) int {
	if aValueType != bValueType {
		return ParseTypeToValType[aValueType] - ParseTypeToValType[bValueType]
	}

	// Both types are the same, so need type-based cases...
	switch jsonparser.ValueType(aValueType) {
	case jsonparser.String:
		kvs := c.KeyValsAcquire(depth)

		aBuf := KeyValsReuseNextKey(kvs)
		kvs = append(kvs, KeyVal{Key: aBuf})

		bBuf := KeyValsReuseNextKey(kvs)
		kvs = append(kvs, KeyVal{Key: bBuf})

		av, aErr := jsonparser.Unescape(aValue, aBuf[:cap(aBuf)])
		bv, bErr := jsonparser.Unescape(bValue, bBuf[:cap(bBuf)])

		// NOTE: do NOT store av/bv back into the pooled kvs[].Key. When the input
		// has no escapes, jsonparser.Unescape returns the *input* slice unchanged
		// -- i.e. av/bv can alias the caller's memory (e.g. a static constant or a
		// doc field). Recording those pointers as reusable pool buffers would let
		// a later ReuseNextKey (e.g. the Object branch copying a field name) write
		// into that caller memory and corrupt it. The pool keeps its own aBuf/bBuf
		// buffers; that's all that's safe to reuse.
		c.KeyValsRelease(depth, kvs)

		if aErr != nil || bErr != nil {
			return CompareErr(aErr, bErr)
		}

		return bytes.Compare(av, bv)

	case jsonparser.Number:
		av, aErr := jsonparser.ParseFloat(aValue)
		bv, bErr := jsonparser.ParseFloat(bValue)

		if aErr != nil || bErr != nil {
			return CompareErr(aErr, bErr)
		}

		if av == bv {
			return 0
		}

		if av < bv {
			return -1
		}

		return 1

	case jsonparser.Boolean:
		return int(aValue[0]) - int(bValue[0]) // Ex: 't' - 'f'.

	case jsonparser.Array:
		kvs := c.KeyValsAcquire(depth)

		_, bErr := jsonparser.ArrayEach(bValue,
			func(v []byte, vT jsonparser.ValueType, o int, vErr error) {
				kvs = append(kvs, KeyVal{KeyValsReuseNextKey(kvs), v, int(vT), 0})
			})

		bLen := len(kvs)

		depthPlus1 := depth + 1

		var i int
		var cmp int

		_, aErr := jsonparser.ArrayEach(aValue,
			func(v []byte, vT jsonparser.ValueType, o int, vErr error) {
				if cmp != 0 {
					return
				}

				if i >= bLen {
					cmp = 1
					return
				}

				cmp = c.CompareWithType(
					v, kvs[i].Val, int(vT), kvs[i].ValType, depthPlus1)

				i++
			})

		c.KeyValsRelease(depth, kvs)

		if aErr != nil || bErr != nil {
			return CompareErr(aErr, bErr)
		}

		// A non-zero cmp from the first differing element decides the
		// result. Only when all compared elements were equal (cmp == 0)
		// does a shorter aValue (i < bLen) make aValue the lesser. NOTE:
		// the loop stops advancing i once cmp != 0, so i < bLen can be
		// true here even though cmp already decided -- don't let the
		// shorter-array rule override a real element comparison.
		if cmp != 0 {
			return cmp
		}

		if i < bLen {
			return -1
		}

		return cmp

	case jsonparser.Object:
		kvs := c.KeyValsAcquire(depth)

		var aLen int
		aErr := jsonparser.ObjectEach(aValue,
			func(k []byte, v []byte, vT jsonparser.ValueType, o int) error {
				kCopy := append(KeyValsReuseNextKey(kvs), k...)
				kvs = append(kvs, KeyVal{kCopy, v, int(vT), 1})
				aLen++
				return nil
			})

		var bLen int
		bErr := jsonparser.ObjectEach(bValue,
			func(k []byte, v []byte, vT jsonparser.ValueType, o int) error {
				kCopy := append(KeyValsReuseNextKey(kvs), k...)
				kvs = append(kvs, KeyVal{kCopy, v, int(vT), -1})
				bLen++
				return nil
			})

		// Compute the result into rv, then release the pooled kvs exactly once
		// after all use (sort + loop). Releasing before use -- as this branch did
		// -- hands the depth-pool slice back while we still sort/read it, which
		// corrupts later same-depth Compare/canonical calls that reuse this
		// ValComparer. (See the matching fix in canonical.go's object branch.)
		rv := 0
		switch {
		case aErr != nil || bErr != nil:
			rv = CompareErr(aErr, bErr)
		case aLen != bLen:
			rv = aLen - bLen // Larger object wins.
		default:
			sort.Sort(kvs)

			// With closely matching objects, the sorted kvs should look like a
			// sequence of pairs, like:
			//   [{"city","sf",1}, {"city","sf",-1}, {"state",...} ...]
			// A KeyVal from aValue has Pos 1; from bValue has Pos -1. The loop
			// looks for a non-matching pair, kvX & kvY.
			depthPlus1 := depth + 1

			for i := 0; i < len(kvs); {
				kvX := kvs[i]
				i++

				if i >= len(kvs) {
					rv = kvX.Pos
					break
				}

				kvY := kvs[i]
				i++

				if kvX.Pos == kvY.Pos {
					rv = kvX.Pos
					break
				}

				if !bytes.Equal(kvX.Key, kvY.Key) {
					rv = kvX.Pos
					break
				}

				cmp := c.CompareWithType(kvX.Val, kvY.Val,
					int(kvX.ValType), int(kvY.ValType), depthPlus1)
				if cmp != 0 {
					rv = cmp
					break
				}
			}
		}

		c.KeyValsRelease(depth, kvs)

		return rv

	default: // Null, NotExist, Unknown.
		return 0
	}
}

// ---------------------------------------------

// EncodeAsString appends the JSON encoded string to the optional out
// slice and returns the append()'ed out.
//
// TODO(encoder-fidelity): this uses stdlib encoding/json, which escapes formfeed
// (0x0C) and backspace (0x08) as the two-char \f / \b, whereas cbq's value
// encoder emits the six-char \u000c / \u0008. Both are valid JSON for the same
// character, but the bytes differ -- so a native string function whose output
// contains a literal formfeed/backspace is NOT byte-identical to the cbq
// fallback. Cosmetic (re-parsing yields the same value) but it breaks the
// byte-level differential vs cbq. A faithful fix routes this through cbq's
// encoder (or post-escapes \f/\b -> \u000c/\u0008). See DESIGN-exprs.md.
func (c *ValComparer) EncodeAsString(s []byte, out []byte) ([]byte, error) {
	c.Buffer.Reset()

	c.Bytes = s

	c.Encoder.Encode(c)

	written := c.Buffer.Len() - 1 // Strip off newline from encoder.

	lenOld := len(out)
	needed := lenOld + written

	if cap(out) >= needed {
		out = out[:needed]
	} else {
		out = append(make([]byte, 0, needed), out...)[:needed]
	}

	c.Buffer.Read(out[lenOld:])

	return out, nil
}

// MarshalText() allows a ValComparer instance to implement the
// encoding.TextMarshaler interface with no extra allocations.
func (c *ValComparer) MarshalText() ([]byte, error) { return c.Bytes, nil }

// ---------------------------------------------

func (c *ValComparer) KeyValsAcquire(depth int) KeyVals {
	for len(c.KeyVals) < depth+1 {
		c.KeyVals = append(c.KeyVals, nil)
	}

	return c.KeyVals[depth]
}

func (c *ValComparer) KeyValsRelease(depth int, s KeyVals) {
	c.KeyVals[depth] = s[:0]
}

// ---------------------------------------------

// KeyVal is used while sorting multiple keys (and their associated
// vals), such as when comparing objects when field name sorting is
// needed.
type KeyVal struct {
	Key     []byte
	Val     []byte
	ValType int
	Pos     int
}

type KeyVals []KeyVal

func (a KeyVals) Len() int { return len(a) }

func (a KeyVals) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

func (a KeyVals) Less(i, j int) bool {
	cmp := bytes.Compare(a[i].Key, a[j].Key)
	if cmp < 0 {
		return true
	}

	if cmp > 0 {
		return false
	}

	return a[i].Pos > a[j].Pos // Reverse ordering on Pos.
}

// ---------------------------------------------

// When append()'ing to the kvs, the entry that we're going to
// overwrite might have a Key []byte that we can reuse.
func KeyValsReuseNextKey(kvs KeyVals) []byte {
	if cap(kvs) > len(kvs) {
		return kvs[0 : len(kvs)+1][len(kvs)].Key[:0]
	}

	return nil
}

// ---------------------------------------------

// CompareErr should be invoked with a or b or both as an error.
func CompareErr(aErr, bErr error) int {
	if aErr != nil && bErr != nil {
		return 0
	}

	if aErr != nil {
		return -1
	}

	return 1
}
