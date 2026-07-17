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

// Package variant holds small, self-contained helpers for the Apache Parquet/Iceberg
// VARIANT type, built on github.com/apache/arrow-go/v18/parquet/variant. It has no
// dependencies on the rest of n1k1, so it can be reused as a standalone library.
//
// The headline helper is AppendJSON: a recursive, []byte-appending Variant→JSON
// projector that reads straight from the Variant value/metadata bytes and appends into
// a caller-supplied (reusable) buffer — the zero-garbage alternative to
// variant.Value.MarshalJSON, which returns a fresh slice each call and internally
// boxes + reflect-marshals + builds intermediate maps/slices. AppendJSON allocates
// nothing for the JSON-native types (null/bool/int/string/object/array), for decimals
// (Decimal4/8/16, via AppendDecimal128), and for the typed scalars date/timestamp/time
// (via time.Time.AppendFormat), uuid, and binary (via base64.AppendEncode). The
// MarshalJSON fallback now only guards genuinely unrecognized primitive tags.
package variant

import (
	"encoding/base64"
	"encoding/binary"
	"math"
	"math/bits"
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	av "github.com/apache/arrow-go/v18/parquet/variant"
)

// Timestamp/time layouts — byte-identical to what av.Value.MarshalJSON formats each
// type with (arrow-go parquet/variant/variant.go). Microsecond vs nanosecond precision
// distinguishes the two timestamp widths; the "Z0700" element prints "Z" for UTC and
// ±hhmm otherwise (so NTZ values, rendered in time.Local, carry the local offset).
const (
	tsMicrosLayout = "2006-01-02 15:04:05.999999Z0700"
	tsNanosLayout  = "2006-01-02 15:04:05.999999999Z0700"
	timeLayout     = "15:04:05.999999Z0700"
	dateLayout     = "2006-01-02"
)

// AppendJSON appends v's canonical JSON projection to dst and returns the extended
// slice. See the package doc for the allocation contract.
func AppendJSON(dst []byte, v av.Value) []byte {
	return appendValue(dst, v.Metadata().Bytes(), v.Bytes())
}

// AppendJSONBytes appends the JSON of a Variant whose metadata and value bytes are given
// directly (as carried by n1k1's V-envelope). Unlike AppendJSON it never constructs an
// av.Value / av.Metadata, so it does not pay av.NewMetadata's make([][]byte, dictSize)
// dictionary-build cost -- object keys resolve through metaKeyAt, a zero-alloc byte walk.
func AppendJSONBytes(dst []byte, meta, val []byte) []byte {
	return appendValue(dst, meta, val)
}

// appendValue appends the JSON of the Variant value bytes `val` (whose object field
// names resolve through the raw metadata bytes `meta`) to dst.
func appendValue(dst []byte, meta []byte, val []byte) []byte {
	switch av.BasicType(val[0] & 0x03) {
	case av.BasicShortString:
		n := int(val[0] >> 2)
		return appendJSONBytes(dst, val[1:1+n])
	case av.BasicObject:
		return appendObject(dst, meta, val)
	case av.BasicArray:
		return appendArray(dst, meta, val)
	default:
		return appendPrimitive(dst, meta, val)
	}
}

func appendPrimitive(dst []byte, meta []byte, val []byte) []byte {
	switch av.PrimitiveType((val[0] >> 2) & 0x3F) {
	case av.PrimitiveNull:
		return append(dst, "null"...)
	case av.PrimitiveBoolTrue:
		return append(dst, "true"...)
	case av.PrimitiveBoolFalse:
		return append(dst, "false"...)
	case av.PrimitiveInt8:
		return strconv.AppendInt(dst, int64(int8(val[1])), 10)
	case av.PrimitiveInt16:
		return strconv.AppendInt(dst, int64(int16(binary.LittleEndian.Uint16(val[1:3]))), 10)
	case av.PrimitiveInt32:
		return strconv.AppendInt(dst, int64(int32(binary.LittleEndian.Uint32(val[1:5]))), 10)
	case av.PrimitiveInt64:
		return strconv.AppendInt(dst, int64(binary.LittleEndian.Uint64(val[1:9])), 10)
	case av.PrimitiveFloat:
		return strconv.AppendFloat(dst, float64(math.Float32frombits(binary.LittleEndian.Uint32(val[1:5]))), 'g', -1, 32)
	case av.PrimitiveDouble:
		return strconv.AppendFloat(dst, math.Float64frombits(binary.LittleEndian.Uint64(val[1:9])), 'g', -1, 64)
	case av.PrimitiveString:
		n := binary.LittleEndian.Uint32(val[1:5])
		return appendJSONBytes(dst, val[5:5+n])
	case av.PrimitiveDecimal4:
		return AppendDecimal(dst, int64(int32(binary.LittleEndian.Uint32(val[2:6]))), int(val[1]))
	case av.PrimitiveDecimal8:
		return AppendDecimal(dst, int64(binary.LittleEndian.Uint64(val[2:10])), int(val[1]))
	case av.PrimitiveDecimal16:
		lo := binary.LittleEndian.Uint64(val[2:10])
		hi := int64(binary.LittleEndian.Uint64(val[10:18]))
		return AppendDecimal128(dst, hi, lo, int(val[1]))
	case av.PrimitiveDate:
		d := arrow.Date32(int32(binary.LittleEndian.Uint32(val[1:5])))
		return appendTimeStr(dst, d.ToTime(), dateLayout)
	case av.PrimitiveTimestampMicros: // has tz → UTC
		ts := arrow.Timestamp(binary.LittleEndian.Uint64(val[1:9]))
		return appendTimeStr(dst, ts.ToTime(arrow.Microsecond), tsMicrosLayout)
	case av.PrimitiveTimestampMicrosNTZ: // no tz → local
		ts := arrow.Timestamp(binary.LittleEndian.Uint64(val[1:9]))
		return appendTimeStr(dst, ts.ToTime(arrow.Microsecond).In(time.Local), tsMicrosLayout)
	case av.PrimitiveTimestampNanos: // has tz → UTC
		ts := arrow.Timestamp(binary.LittleEndian.Uint64(val[1:9]))
		return appendTimeStr(dst, ts.ToTime(arrow.Nanosecond), tsNanosLayout)
	case av.PrimitiveTimestampNanosNTZ: // no tz → local
		ts := arrow.Timestamp(binary.LittleEndian.Uint64(val[1:9]))
		return appendTimeStr(dst, ts.ToTime(arrow.Nanosecond).In(time.Local), tsNanosLayout)
	case av.PrimitiveTimeMicrosNTZ:
		tm := arrow.Time64(binary.LittleEndian.Uint64(val[1:9]))
		return appendTimeStr(dst, tm.ToTime(arrow.Microsecond).In(time.Local), timeLayout)
	case av.PrimitiveUUID:
		return appendUUID(dst, val[1:17])
	case av.PrimitiveBinary:
		n := binary.LittleEndian.Uint32(val[1:5])
		dst = append(dst, '"')
		dst = base64.StdEncoding.AppendEncode(dst, val[5:5+n])
		return append(dst, '"')
	default:
		// Any genuinely unhandled primitive: fall back to MarshalJSON (allocates a fresh
		// slice + parses meta, but correct). Only exotic scalar types reach here.
		if sub, err := av.New(meta, val); err == nil {
			if j, err := sub.MarshalJSON(); err == nil {
				return append(dst, j...)
			}
		}
		return append(dst, "null"...)
	}
}

// appendTimeStr appends t formatted with layout as a JSON string. The layouts used here
// (date/timestamp/time) only ever produce characters that need no JSON escaping, so the
// value is quote-wrapped directly. AppendFormat writes into dst without allocating.
func appendTimeStr(dst []byte, t time.Time, layout string) []byte {
	dst = append(dst, '"')
	dst = t.AppendFormat(dst, layout)
	return append(dst, '"')
}

// appendUUID appends the 16-byte b as a JSON-quoted canonical UUID string
// (8-4-4-4-12 lowercase hex) — byte-identical to json.Marshal(uuid.UUID{...}), which
// goes through uuid.UUID.MarshalText.
func appendUUID(dst, b []byte) []byte {
	dst = append(dst, '"')
	for i := 0; i < 16; i++ {
		if i == 4 || i == 6 || i == 8 || i == 10 {
			dst = append(dst, '-')
		}
		dst = append(dst, hexDigits[b[i]>>4], hexDigits[b[i]&0xf])
	}
	return append(dst, '"')
}

func appendObject(dst []byte, meta []byte, val []byte) []byte {
	vh := val[0] >> 2
	offSz := int((vh & 0b11) + 1)
	idSz := int(((vh >> 2) & 0b11) + 1)
	nSz := 1
	if (vh>>4)&0b1 == 1 { // isLarge
		nSz = 4
	}
	n := int(readLE(val[1 : 1+nSz]))
	idStart := 1 + nSz
	offStart := idStart + n*idSz
	dataStart := offStart + (n+1)*offSz

	dst = append(dst, '{')
	for i := 0; i < n; i++ {
		if i > 0 {
			dst = append(dst, ',')
		}
		id := readLE(val[idStart+i*idSz : idStart+i*idSz+idSz])
		off := int(readLE(val[offStart+i*offSz : offStart+i*offSz+offSz]))
		dst = appendJSONBytes(dst, metaKeyAt(meta, id))
		dst = append(dst, ':')
		dst = appendValue(dst, meta, val[dataStart+off:])
	}
	return append(dst, '}')
}

func appendArray(dst []byte, meta []byte, val []byte) []byte {
	vh := val[0] >> 2
	offSz := int((vh & 0b11) + 1)
	var n, offStart int
	if vh&0b1 == 1 { // isLarge (matches arrow-go's array decoder)
		n, offStart = int(binary.LittleEndian.Uint32(val[1:5])), 5
	} else {
		n, offStart = int(val[1]), 2
	}
	dataStart := offStart + (n+1)*offSz

	dst = append(dst, '[')
	for i := 0; i < n; i++ {
		if i > 0 {
			dst = append(dst, ',')
		}
		off := int(readLE(val[offStart+i*offSz : offStart+i*offSz+offSz]))
		dst = appendValue(dst, meta, val[dataStart+off:])
	}
	return append(dst, ']')
}

// AppendDecimal appends an int64-coefficient decimal (Variant Decimal4/8) as a JSON
// number, allocation-free. Equivalent to AppendDecimal128 with the coefficient
// sign-extended to 128 bits.
func AppendDecimal(dst []byte, coeff int64, scale int) []byte {
	var hi int64
	if coeff < 0 {
		hi = -1
	}
	return AppendDecimal128(dst, hi, uint64(coeff), scale)
}

// AppendDecimal128 appends a 128-bit fixed-point decimal — signed coefficient
// (hi<<64 | lo, two's complement) divided by 10^scale — as a JSON number, appending
// into dst with no heap allocation. Byte-identical to
// decimal128.New(hi, lo).ToString(scale). This is the piece that lets a Variant
// Decimal16 (what ParseJSON stores every fractional JSON number as) project to JSON
// without the big.Int / fresh-string cost.
func AppendDecimal128(dst []byte, hi int64, lo uint64, scale int) []byte {
	uhi := uint64(hi)
	neg := hi < 0
	if neg { // two's-complement negate (uhi:lo)
		lo = ^lo + 1
		uhi = ^uhi
		if lo == 0 {
			uhi++
		}
	}

	// Extract base-10 digits least-significant-first via 128-bit /10.
	var digs [40]byte // 2^128 < 10^39
	n := 0
	for {
		q := uhi / 10
		r := uhi % 10 // r < 10, so bits.Div64 below won't overflow
		ql, rem := bits.Div64(r, lo, 10)
		uhi, lo = q, ql
		digs[n] = byte('0' + rem)
		n++
		if uhi == 0 && lo == 0 {
			break
		}
	}

	if neg {
		dst = append(dst, '-')
	}
	if scale <= 0 {
		return appendRevDigits(dst, digs[:n], n-1, 0)
	}
	if n <= scale { // value < 1: "0." + leading zeros + digits
		dst = append(dst, '0', '.')
		for i := 0; i < scale-n; i++ {
			dst = append(dst, '0')
		}
		return appendRevDigits(dst, digs[:n], n-1, 0)
	}
	dst = appendRevDigits(dst, digs[:n], n-1, scale) // integer part
	dst = append(dst, '.')
	return appendRevDigits(dst, digs[:n], scale-1, 0) // fraction
}

// appendRevDigits appends digs[from], digs[from-1], … digs[to] (digs is stored
// least-significant-first, so this walks most-significant-first over the range).
func appendRevDigits(dst, digs []byte, from, to int) []byte {
	for i := from; i >= to; i-- {
		dst = append(dst, digs[i])
	}
	return dst
}

// readLE reads a little-endian unsigned integer from 1..4 bytes.
func readLE(b []byte) uint32 {
	var v uint32
	for i := 0; i < len(b); i++ {
		v |= uint32(b[i]) << (8 * i)
	}
	return v
}

// metaKeyAt returns the id-th dictionary key of the Variant metadata `meta` as a
// zero-copy sub-slice, replicating av.Metadata.KeyAt without av.NewMetadata's
// make([][]byte, dictSize) dictionary build. The metadata layout (arrow-go builder.go):
//
//	meta[0]  = version | ((offsetSize-1) << 6)
//	meta[1 : 1+offSz]                       = dictSize (LE)
//	offsets: (dictSize+1) x offSz starting at 1+offSz
//	keys:    concatenated bytes starting at 1 + (dictSize+2)*offSz
func metaKeyAt(meta []byte, id uint32) []byte {
	offSz := int((meta[0]>>6)&0b11) + 1
	dictSize := int(readLE(meta[1 : 1+offSz]))
	offStart := 1 + offSz
	keyStart := 1 + (dictSize+2)*offSz
	i := int(id)
	o0 := int(readLE(meta[offStart+i*offSz : offStart+i*offSz+offSz]))
	o1 := int(readLE(meta[offStart+(i+1)*offSz : offStart+(i+1)*offSz+offSz]))
	return meta[keyStart+o0 : keyStart+o1]
}

// PathGet navigates the Variant value bytes `val` (metadata `meta`) down `path` and
// returns the reached value's bytes as a zero-copy sub-slice of `val` (a valid Variant
// value under the same `meta`), or ok=false if any step is missing / not navigable.
// It never boxes a node (no av.Value) nor builds the metadata dictionary (no av.New) --
// object steps compare against metaKeyAt, array steps index the offset table directly.
func PathGet(meta, val []byte, path []string) ([]byte, bool) {
	for _, p := range path {
		if len(val) == 0 {
			return nil, false
		}
		switch av.BasicType(val[0] & 0x03) {
		case av.BasicObject:
			child, ok := objectField(meta, val, p)
			if !ok {
				return nil, false
			}
			val = child
		case av.BasicArray:
			idx, err := strconv.Atoi(p)
			if err != nil {
				return nil, false
			}
			child, ok := arrayElem(val, idx)
			if !ok {
				return nil, false
			}
			val = child
		default:
			return nil, false // A scalar leaf: nothing left to descend into.
		}
	}
	return val, true
}

// objectField returns the value bytes of the field named `key` in the Variant object
// `val` (mirrors appendObject's header walk), or ok=false if absent. The returned slice
// starts at the child value and runs to the end of `val`; the child is self-delimiting.
func objectField(meta, val []byte, key string) ([]byte, bool) {
	vh := val[0] >> 2
	offSz := int((vh & 0b11) + 1)
	idSz := int(((vh >> 2) & 0b11) + 1)
	nSz := 1
	if (vh>>4)&0b1 == 1 { // isLarge
		nSz = 4
	}
	n := int(readLE(val[1 : 1+nSz]))
	idStart := 1 + nSz
	offStart := idStart + n*idSz
	dataStart := offStart + (n+1)*offSz

	for i := 0; i < n; i++ {
		id := readLE(val[idStart+i*idSz : idStart+i*idSz+idSz])
		// string(metaKeyAt(...)) == key is compiled to an alloc-free byte compare.
		if string(metaKeyAt(meta, id)) == key {
			off := int(readLE(val[offStart+i*offSz : offStart+i*offSz+offSz]))
			return val[dataStart+off:], true
		}
	}
	return nil, false
}

// arrayElem returns the idx-th element's value bytes of the Variant array `val`
// (mirrors appendArray's header walk), or ok=false if idx is out of range.
func arrayElem(val []byte, idx int) ([]byte, bool) {
	vh := val[0] >> 2
	offSz := int((vh & 0b11) + 1)
	var n, offStart int
	if vh&0b1 == 1 { // isLarge
		n, offStart = int(binary.LittleEndian.Uint32(val[1:5])), 5
	} else {
		n, offStart = int(val[1]), 2
	}
	if idx < 0 || idx >= n {
		return nil, false
	}
	dataStart := offStart + (n+1)*offSz
	off := int(readLE(val[offStart+idx*offSz : offStart+idx*offSz+offSz]))
	return val[dataStart+off:], true
}

const hexDigits = "0123456789abcdef"

// appendEscByte appends the JSON escape for a byte that must be escaped.
func appendEscByte(dst []byte, c byte) []byte {
	switch c {
	case '"':
		return append(dst, '\\', '"')
	case '\\':
		return append(dst, '\\', '\\')
	case '\n':
		return append(dst, '\\', 'n')
	case '\r':
		return append(dst, '\\', 'r')
	case '\t':
		return append(dst, '\\', 't')
	default: // other control char < 0x20
		return append(dst, '\\', 'u', '0', '0', hexDigits[c>>4], hexDigits[c&0xf])
	}
}

func mustEscape(c byte) bool { return c < 0x20 || c == '"' || c == '\\' }

// appendJSONBytes appends raw (a UTF-8 string's content) as a quoted, JSON-escaped
// string. Valid UTF-8 bytes ≥ 0x20 pass through verbatim (JSON permits raw UTF-8).
func appendJSONBytes(dst, raw []byte) []byte {
	dst = append(dst, '"')
	start := 0
	for i := 0; i < len(raw); i++ {
		if mustEscape(raw[i]) {
			dst = append(dst, raw[start:i]...)
			dst = appendEscByte(dst, raw[i])
			start = i + 1
		}
	}
	dst = append(dst, raw[start:]...)
	return append(dst, '"')
}

// appendJSONStr is appendJSONBytes for a string (object keys) — no []byte conversion,
// so it stays allocation-free.
func appendJSONStr(dst []byte, s string) []byte {
	dst = append(dst, '"')
	start := 0
	for i := 0; i < len(s); i++ {
		if mustEscape(s[i]) {
			dst = append(dst, s[start:i]...)
			dst = appendEscByte(dst, s[i])
			start = i + 1
		}
	}
	dst = append(dst, s[start:]...)
	return append(dst, '"')
}
