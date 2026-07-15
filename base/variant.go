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

// VARIANT fidelity carrier (DESIGN-variant.md §4, Phase 1). A base.Val is normally
// JSON, but its first byte may instead be SigilVariant ('V'), marking the bytes as an
// Apache Parquet/Iceberg VARIANT value carried losslessly through the engine (so a
// VARIANT date/decimal/uuid round-trips out as the same typed scalar, not its JSON
// projection). This file is the arrow-go-free half: the byte framing and a cheap
// tag->ValType classification, so base keeps building under GOOS=js/wasm. The actual
// decode/projection (which needs arrow-go) is delegated to a registered hook
// (VariantAppendJSON), installed by an n1k1 package that may import the ./variant lib
// (e.g. records). See DESIGN-variant.md §4.1.
//
// This file is ADDITIVE: it introduces the carrier primitives but does not yet wire
// them into Parse/ValKind/ValPathGet/output — that is Phase-1 step 2, behind an opt-in
// scan mode and a differential test. Nothing here changes existing query behavior.

import "encoding/binary"

// SigilVariant is the first byte of a Val carrying a VARIANT envelope. 'V' (0x56) sits
// outside JSON's value-start alphabet (" { [ - digits t f n + whitespace), so
// v[0]==SigilVariant is an unambiguous "not JSON" signal (DESIGN-variant.md §4.4).
const SigilVariant = 'V'

// A VARIANT envelope is a self-contained single []byte:
//
//	'V' <uvarint len(metadata)> <metadata bytes> <value bytes>
//
// metadata is the Apache Variant field-name dictionary; value is the tagged value
// stream (its first byte carries VARIANT's own basic_type/type_info). Carrying both
// makes a row's VARIANT value independent of any shared column dictionary.

// IsVariant reports whether v is a VARIANT-carrier Val (starts with SigilVariant).
func IsVariant(v Val) bool { return len(v) > 0 && v[0] == SigilVariant }

// AppendVariantEnvelope appends a VARIANT envelope framing meta+value to dst and
// returns the extended slice.
func AppendVariantEnvelope(dst, meta, value []byte) []byte {
	dst = append(dst, SigilVariant)
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(len(meta)))
	dst = append(dst, tmp[:n]...)
	dst = append(dst, meta...)
	dst = append(dst, value...)
	return dst
}

// SplitVariantEnvelope returns the metadata and value byte slices of a VARIANT
// envelope as zero-copy subslices of v, or ok=false if v is not a well-formed
// envelope.
func SplitVariantEnvelope(v Val) (meta, value []byte, ok bool) {
	if len(v) < 2 || v[0] != SigilVariant {
		return nil, nil, false
	}
	mlen, n := binary.Uvarint(v[1:])
	if n <= 0 {
		return nil, nil, false
	}
	start := 1 + n
	if uint64(len(v)-start) < mlen {
		return nil, nil, false
	}
	end := start + int(mlen)
	return v[start:end], v[end:], true
}

// Apache Variant value-stream tags. The low 2 bits of value[0] are basic_type; for a
// primitive the high 6 bits are type_info, selecting the primitive kind below. These
// mirror github.com/apache/arrow-go/v18/parquet/variant (spec-stable); records'
// TestVariantValTypeMatchesArrowGo cross-checks them against arrow-go so they can't
// drift silently.
const (
	variantBasicPrimitive   = 0
	variantBasicShortString = 1
	variantBasicObject      = 2
	variantBasicArray       = 3
)

const ( // primitive type_info values
	vpNull       = 0
	vpBoolTrue   = 1
	vpBoolFalse  = 2
	vpInt8       = 3
	vpInt16      = 4
	vpInt32      = 5
	vpInt64      = 6
	vpDouble     = 7
	vpDecimal4   = 8
	vpDecimal8   = 9
	vpDecimal16  = 10
	vpDate       = 11
	vpTsMicros   = 12
	vpTsMicrosNZ = 13
	vpFloat      = 14
	vpBinary     = 15
	vpString     = 16
	vpTimeNZ     = 17
	vpTsNanos    = 18
	vpTsNanosNZ  = 19
	vpUUID       = 20
)

// VariantValType classifies a VARIANT-carrier Val by the ValType of its JSON
// PROJECTION (date/timestamp/time/uuid/binary -> String, the integer/decimal/float
// widths -> Number, object -> Object, ...). Collation and the is_*/TYPE() family must
// see a VARIANT value as what it projects to, so this must agree with how the ./variant
// projector renders it. Alloc-free, no decode — it reads a single tag byte.
func VariantValType(v Val) int {
	_, value, ok := SplitVariantEnvelope(v)
	if !ok || len(value) == 0 {
		return ValTypeUnknown
	}
	return variantTagValType(value[0])
}

func variantTagValType(tag byte) int {
	switch tag & 0x03 {
	case variantBasicShortString:
		return ValTypeString
	case variantBasicObject:
		return ValTypeObject
	case variantBasicArray:
		return ValTypeArray
	default: // primitive
		switch (tag >> 2) & 0x3F {
		case vpNull:
			return ValTypeNull
		case vpBoolTrue, vpBoolFalse:
			return ValTypeBoolean
		case vpInt8, vpInt16, vpInt32, vpInt64, vpDouble, vpFloat,
			vpDecimal4, vpDecimal8, vpDecimal16:
			return ValTypeNumber
		case vpString, vpDate, vpTsMicros, vpTsMicrosNZ, vpTimeNZ,
			vpTsNanos, vpTsNanosNZ, vpUUID, vpBinary:
			// date/timestamp/time/uuid project to a JSON string; binary -> base64 string.
			return ValTypeString
		default:
			return ValTypeUnknown
		}
	}
}

// VariantAppendJSON is the registered hook that appends the JSON projection of a
// VARIANT (metadata, value) pair to dst. It is installed by an n1k1 package that may
// import the arrow-go-backed ./variant library (keeping base itself arrow-go-free /
// wasm-safe). nil until registered.
var VariantAppendJSON func(dst, meta, value []byte) []byte

// VariantProjectJSON appends the JSON projection of the VARIANT-carrier Val v to dst.
// Falls back to JSON null if v is malformed or no projector hook is registered.
func VariantProjectJSON(dst []byte, v Val) []byte {
	meta, value, ok := SplitVariantEnvelope(v)
	if !ok || VariantAppendJSON == nil {
		return append(dst, "null"...)
	}
	return VariantAppendJSON(dst, meta, value)
}
