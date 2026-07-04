//go:build n1ql

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

package glue

// Order-preserving key encoding for the bbolt-backed secondary index (Phase 1
// GSI; see DESIGN-indexing.md). bbolt orders entries by raw byte comparison
// (memcmp), which is NOT N1QL collation order in general -- so we encode each
// index-key value into bytes chosen so that bytes.Compare(encode(a), encode(b))
// has the same sign as a.Collate(b) *for scalar values*, which is what
// WHERE-sargable predicates range over. Then a bbolt Cursor.Seek/Next walk
// visits entries in N1QL order and range scans can prune with a real seek.
//
// The leading type tag reproduces N1QL's cross-type collation order
// (MISSING < NULL < FALSE < TRUE < number < string < array < object), so even a
// keyspace mixing types keeps *scalar* bounds correctly positioned: a scalar
// high bound stops the walk before ever reaching the array/object tags. Arrays
// and objects are stored (so the index is complete) but their intra-type byte
// order is not collation order -- a v1 limitation, acceptable because index
// keys are essentially always scalars. A fully order-preserving composite/
// container encoder is a v2 item.
//
// The full bbolt key is encodeValue(secondaryKey) followed by the raw docID
// bytes. encodeValue is self-delimiting (prefix-free: singletons are 1 byte,
// numbers a fixed 9, strings 0x00-escaped with a 0x00 0x00 terminator), so the
// docID suffix is recoverable without a separator. The stored value is empty.

import (
	"encoding/binary"
	"math"

	"github.com/couchbase/query/value"
)

// Type tags, ordered to match N1QL collation across types. Booleans split into
// FALSE/TRUE so false < true. Keep these contiguous and ordered -- their byte
// value IS the cross-type ordering.
const (
	tagMissing = byte(0x00)
	tagNull    = byte(0x01)
	tagFalse   = byte(0x02)
	tagTrue    = byte(0x03)
	tagNumber  = byte(0x04)
	tagString  = byte(0x05)
	tagArray   = byte(0x06)
	tagObject  = byte(0x07)
)

// encodeSeq encodes a sequence of bound values (one per sarged key) into a
// single comparable byte prefix. Shared by the bbolt and in-memory scan paths.
func encodeSeq(vals value.Values) []byte {
	if len(vals) == 0 {
		return nil
	}
	var out []byte
	for _, v := range vals {
		out = encodeValue(out, v)
	}
	return out
}

// decodeKeyComponents recovers the index-key values from a stored key, given the
// per-component byte offsets returned by splitKey. Used by a covering scan to
// reconstruct the projected doc straight from the index (no fetch). A component
// that fails to decode leaves a nil (MISSING) slot rather than dropping the whole
// entry. Shared by the bbolt and in-memory scan paths.
func decodeKeyComponents(key []byte, compEnds []int) value.Values {
	keys := make(value.Values, len(compEnds))
	start := 0
	for i, end := range compEnds {
		if v, _, ok := decodeValue(key[start:end]); ok {
			keys[i] = v
		}
		start = end
	}
	return keys
}

// encodeValue appends the order-preserving encoding of v to dst and returns it.
func encodeValue(dst []byte, v value.Value) []byte {
	if v == nil {
		return append(dst, tagMissing)
	}
	switch v.Type() {
	case value.MISSING:
		return append(dst, tagMissing)
	case value.NULL:
		return append(dst, tagNull)
	case value.BOOLEAN:
		if b, _ := v.Actual().(bool); b {
			return append(dst, tagTrue)
		}
		return append(dst, tagFalse)
	case value.NUMBER:
		dst = append(dst, tagNumber)
		return appendOrderedFloat(dst, toFloat64(v))
	case value.STRING:
		dst = append(dst, tagString)
		s, _ := v.Actual().(string)
		return appendOrderedString(dst, s)
	case value.ARRAY:
		return appendOrderedString(append(dst, tagArray), string(canonicalBytes(v)))
	default: // OBJECT, JSON, BINARY -- store canonically, sorts last.
		return appendOrderedString(append(dst, tagObject), string(canonicalBytes(v)))
	}
}

// decodeValue reads one encodeValue-encoded value from the front of b, returning
// the value, the number of bytes it consumed (so the caller can slice the docID
// suffix), and ok=false on a malformed prefix.
func decodeValue(b []byte) (v value.Value, n int, ok bool) {
	if len(b) == 0 {
		return nil, 0, false
	}
	switch b[0] {
	case tagMissing:
		return value.MISSING_VALUE, 1, true
	case tagNull:
		return value.NULL_VALUE, 1, true
	case tagFalse:
		return value.FALSE_VALUE, 1, true
	case tagTrue:
		return value.TRUE_VALUE, 1, true
	case tagNumber:
		if len(b) < 9 {
			return nil, 0, false
		}
		return value.NewValue(decodeOrderedFloat(b[1:9])), 9, true
	case tagString:
		s, m, ok := decodeOrderedString(b[1:])
		if !ok {
			return nil, 0, false
		}
		return value.NewValue(s), 1 + m, true
	case tagArray, tagObject:
		// Containers use the same self-delimiting escaped form as strings, so a
		// composite key can still recover the docID suffix after one. Their
		// intra-type byte order is not collation order (a v1 caveat), but that
		// only affects range *bounds* on a container, which predicates never use.
		s, m, ok := decodeOrderedString(b[1:])
		if !ok {
			return nil, 0, false
		}
		return value.NewValue([]byte(s)), 1 + m, true
	default:
		return nil, 0, false
	}
}

// appendOrderedFloat writes f as 8 big-endian bytes whose unsigned order matches
// numeric order: flip the sign bit for non-negatives, flip all bits for
// negatives (the standard IEEE-754 order-preserving transform).
func appendOrderedFloat(dst []byte, f float64) []byte {
	bits := math.Float64bits(f)
	if bits&(1<<63) != 0 { // negative
		bits = ^bits
	} else {
		bits |= 1 << 63
	}
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], bits)
	return append(dst, b[:]...)
}

func decodeOrderedFloat(b []byte) float64 {
	bits := binary.BigEndian.Uint64(b)
	if bits&(1<<63) != 0 { // was non-negative
		bits &^= 1 << 63
	} else {
		bits = ^bits
	}
	return math.Float64frombits(bits)
}

// appendOrderedString writes s so byte order matches string order and the
// encoding is self-delimiting: 0x00 is escaped to 0x00 0xFF and the whole thing
// is terminated with 0x00 0x00 (which sorts before any escaped 0x00 and before
// any real byte, so a prefix sorts before a longer string sharing it). Raw
// UTF-8 byte order matches N1QL's default string collation.
func appendOrderedString(dst []byte, s string) []byte {
	for i := 0; i < len(s); i++ {
		c := s[i]
		dst = append(dst, c)
		if c == 0x00 {
			dst = append(dst, 0xFF)
		}
	}
	return append(dst, 0x00, 0x00)
}

func decodeOrderedString(b []byte) (s string, n int, ok bool) {
	var out []byte
	for i := 0; i < len(b); i++ {
		if b[i] == 0x00 {
			if i+1 >= len(b) {
				return "", 0, false
			}
			switch b[i+1] {
			case 0x00: // terminator
				return string(out), i + 2, true
			case 0xFF: // escaped 0x00
				out = append(out, 0x00)
				i++
			default:
				return "", 0, false
			}
		} else {
			out = append(out, b[i])
		}
	}
	return "", 0, false
}

// toFloat64 extracts a number's float64. It must agree between the build path
// (doc field values) and the scan path (predicate bound values): cbq represents
// a JSON number as either a float or an int value, and .Actual() unwraps any
// annotated/scope wrapper, so we switch on the concrete Go type it yields (an
// integer literal in a predicate arrives as int64, a fraction as float64).
func toFloat64(v value.Value) float64 {
	switch a := v.Actual().(type) {
	case float64:
		return a
	case float32:
		return float64(a)
	case int64:
		return float64(a)
	case int:
		return float64(a)
	case int32:
		return float64(a)
	case uint64:
		return float64(a)
	}
	if nv, ok := v.(value.NumberValue); ok {
		return nv.Float64()
	}
	return 0
}

// canonicalBytes returns a stable byte form of a container value for storage.
func canonicalBytes(v value.Value) []byte {
	if raw, ok := v.Actual().([]byte); ok {
		return raw
	}
	b, _ := v.MarshalJSON()
	return b
}

// splitKey walks n encodeValue-encoded components off the front of a stored
// bbolt key, returning the byte offset after each component (compEnds) and the
// trailing docID bytes. ok is false on a malformed key.
func splitKey(key []byte, n int) (compEnds []int, docID []byte, ok bool) {
	compEnds = make([]int, 0, n)
	off := 0
	for i := 0; i < n; i++ {
		_, m, ok := decodeValue(key[off:])
		if !ok {
			return nil, nil, false
		}
		off += m
		compEnds = append(compEnds, off)
	}
	return compEnds, key[off:], true
}
