//go:build !js

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

package records

// Prototype for DESIGN-variant.md §2: a recursive, []byte-appending Variant→JSON
// emitter, as the zero-garbage alternative to variant.Value.MarshalJSON() (which
// returns a fresh slice + boxes + reflect-marshals + builds intermediate maps/slices).
//
// It proves two things the design leans on:
//   1. SCALAR projection is ZERO-ALLOC -- read the primitive straight out of
//      v.Bytes() and append into a reused dst. This is the filter hot path (ValPathGet
//      projects a scalar leaf), so it's the allocation that matters most.
//   2. The recursive whole-value emit is FEASIBLE and correct (semantically equal to
//      MarshalJSON), and allocates dramatically less than MarshalJSON. Container nodes
//      here still cost one small box each (arrow-go's ObjectValue/ArrayValue are only
//      reachable via v.Value() any); true-zero for whole-object emit is a bounded
//      further step (read the container offset tables from v.Bytes() directly, the code
//      arrow-go already has, or an upstream non-boxing accessor).

import (
	"encoding/binary"
	"encoding/json"
	"math"
	"reflect"
	"strconv"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/variant"
)

// appendVariantScalar appends v's JSON form straight from its bytes, zero-alloc, for
// the primitive/short-string types this prototype handles; ok=false for containers or
// typed scalars left to the caller. (Production would also format date→ISO,
// decimal→digits, etc. into dst, and JSON-escape strings via base's encoder.)
func appendVariantScalar(dst []byte, v variant.Value) ([]byte, bool) {
	b := v.Bytes()
	switch variant.BasicType(b[0] & 0x03) {
	case variant.BasicShortString:
		n := int(b[0] >> 2)
		return appendJSONRawString(dst, b[1:1+n]), true
	case variant.BasicPrimitive:
		switch variant.PrimitiveType((b[0] >> 2) & 0x3F) {
		case variant.PrimitiveNull:
			return append(dst, "null"...), true
		case variant.PrimitiveBoolTrue:
			return append(dst, "true"...), true
		case variant.PrimitiveBoolFalse:
			return append(dst, "false"...), true
		case variant.PrimitiveInt8:
			return strconv.AppendInt(dst, int64(int8(b[1])), 10), true
		case variant.PrimitiveInt16:
			return strconv.AppendInt(dst, int64(int16(binary.LittleEndian.Uint16(b[1:3]))), 10), true
		case variant.PrimitiveInt32:
			return strconv.AppendInt(dst, int64(int32(binary.LittleEndian.Uint32(b[1:5]))), 10), true
		case variant.PrimitiveInt64:
			return strconv.AppendInt(dst, int64(binary.LittleEndian.Uint64(b[1:9])), 10), true
		case variant.PrimitiveDouble:
			return strconv.AppendFloat(dst, math.Float64frombits(binary.LittleEndian.Uint64(b[1:9])), 'g', -1, 64), true
		case variant.PrimitiveFloat:
			return strconv.AppendFloat(dst, float64(math.Float32frombits(binary.LittleEndian.Uint32(b[1:5]))), 'g', -1, 32), true
		case variant.PrimitiveString:
			n := binary.LittleEndian.Uint32(b[1:5])
			return appendJSONRawString(dst, b[5:5+n]), true
		case variant.PrimitiveDecimal4:
			return appendDecimalInt64(dst, int64(int32(binary.LittleEndian.Uint32(b[2:6]))), b[1]), true
		case variant.PrimitiveDecimal8:
			return appendDecimalInt64(dst, int64(binary.LittleEndian.Uint64(b[2:10])), b[1]), true
			// PrimitiveDecimal16 (128-bit) needs a bigint-free 128-bit formatter — the one
			// remaining piece; falls through to the MarshalJSON fallback for now. Likewise
			// date/timestamp/time/uuid/binary (each a small dst-formatter — see doc §7).
		}
	}
	return dst, false // container or unhandled typed scalar
}

// appendDecimalInt64 appends an int64-coefficient decimal (Variant Decimal4/8) as a
// JSON number, zero-alloc: format the coefficient's digits into a stack buffer, then
// splice a '.' `scale` digits from the right. (Decimal16 needs the 128-bit analogue.)
func appendDecimalInt64(dst []byte, coeff int64, scale byte) []byte {
	if scale == 0 {
		return strconv.AppendInt(dst, coeff, 10)
	}
	if coeff < 0 {
		dst = append(dst, '-')
		coeff = -coeff
	}
	var tmp [24]byte
	d := strconv.AppendInt(tmp[:0], coeff, 10) // digits, stack buffer, no alloc
	s := int(scale)
	if len(d) <= s { // 0.00…digits
		dst = append(dst, '0', '.')
		for i := 0; i < s-len(d); i++ {
			dst = append(dst, '0')
		}
		return append(dst, d...)
	}
	dst = append(dst, d[:len(d)-s]...)
	dst = append(dst, '.')
	return append(dst, d[len(d)-s:]...)
}

// appendJSONRawString appends "raw" as a JSON string. Prototype: assumes raw needs no
// escaping (true for the test data); production escapes via base's string encoder.
func appendJSONRawString(dst, raw []byte) []byte {
	dst = append(dst, '"')
	dst = append(dst, raw...)
	return append(dst, '"')
}

// appendVariantJSON recursively appends v's JSON projection into dst. Scalars go
// through the zero-alloc byte reader; objects/arrays navigate via arrow-go's view API
// (which boxes the container node once per node) and recurse.
func appendVariantJSON(dst []byte, v variant.Value) []byte {
	if d, ok := appendVariantScalar(dst, v); ok {
		return d
	}
	switch v.BasicType() {
	case variant.BasicObject:
		ov := v.Value().(variant.ObjectValue) // boxes (one alloc/node) — see file header
		dst = append(dst, '{')
		for i := uint32(0); i < ov.NumElements(); i++ {
			f, err := ov.FieldAt(i)
			if err != nil {
				return dst
			}
			if i > 0 {
				dst = append(dst, ',')
			}
			dst = append(dst, '"')
			dst = append(dst, f.Key...) // string appends into []byte, no alloc
			dst = append(dst, '"', ':')
			dst = appendVariantJSON(dst, f.Value)
		}
		return append(dst, '}')
	case variant.BasicArray:
		av := v.Value().(variant.ArrayValue)
		dst = append(dst, '[')
		for i := uint32(0); i < av.Len(); i++ {
			e, err := av.Value(i)
			if err != nil {
				return dst
			}
			if i > 0 {
				dst = append(dst, ',')
			}
			dst = appendVariantJSON(dst, e)
		}
		return append(dst, ']')
	default: // typed scalar not handled directly — fall back (allocates; rare)
		j, _ := v.MarshalJSON()
		return append(dst, j...)
	}
}

func TestVariantAppendJSONPrototype(t *testing.T) {
	mem := memory.DefaultAllocator

	mk := func(s string) variant.Value {
		v, err := variant.ParseJSON(s, false)
		if err != nil {
			t.Fatalf("ParseJSON(%q): %v", s, err)
		}
		return v
	}
	// JSON-native deep order (matches the deep-nesting fixture, minus typed scalars so
	// the whole thing goes through the direct byte reader / view navigation).
	order := mk(`{"customer":{"address":{"city":"London","geo":{"lat":51.5,"lon":-0.12}},` +
		`"name":"Ada"},"id":"order-1001","orderlines":[{"price":9.99,"qty":2,"sku":"A1"},` +
		`{"qty":1,"sku":"B2","tags":["x","y"]}]}`)
	num := mk(`4200000`)        // large int (Int32): boxing (v.Value()→any) WOULD allocate
	str := mk(`"hello, world"`) // string: boxing allocates the string header
	// A Variant Decimal8 (12.34) built directly — ParseJSON encodes *fractional* JSON
	// numbers as exact Decimal16 (128-bit); Decimal4/8 have an int-fitting coefficient,
	// so appendDecimalInt64 renders them zero-alloc. (See the deep-object note below.)
	var db variant.Builder
	if err := db.AppendDecimal8(2, decimal.Decimal64(1234)); err != nil {
		t.Fatal(err)
	}
	dec8, err := db.Build()
	if err != nil {
		t.Fatal(err)
	}

	va := roundTripVariants(t, mem, order, num, str, dec8)
	ov, _ := va.Value(0)
	sn, _ := va.Value(1)
	ss, _ := va.Value(2)
	sd, _ := va.Value(3)

	// (1) correctness: appendVariantJSON is semantically equal to MarshalJSON.
	for i, v := range []variant.Value{ov, sn, ss, sd} {
		got := appendVariantJSON(nil, v)
		want, _ := v.MarshalJSON()
		var ga, wa any
		if err := json.Unmarshal(got, &ga); err != nil {
			t.Fatalf("row %d: appendVariantJSON emitted invalid JSON %q: %v", i, got, err)
		}
		if err := json.Unmarshal(want, &wa); err != nil {
			t.Fatalf("row %d: MarshalJSON %q: %v", i, want, err)
		}
		if !reflect.DeepEqual(ga, wa) {
			t.Errorf("row %d: append %s != marshal %s", i, got, want)
		}
	}

	// (2) SCALAR projection is zero-alloc (the filter hot path), vs MarshalJSON.
	buf := make([]byte, 0, 256)
	for _, tc := range []struct {
		name string
		v    variant.Value
	}{{"int", sn}, {"string", ss}, {"decimal8", sd}} {
		buf = appendVariantJSON(buf[:0], tc.v) // warm the buffer
		app := testing.AllocsPerRun(2000, func() { buf = appendVariantJSON(buf[:0], tc.v) })
		mj := testing.AllocsPerRun(2000, func() { _, _ = tc.v.MarshalJSON() })
		t.Logf("scalar %-6s: appendVariantJSON=%.0f allocs/op   MarshalJSON=%.0f allocs/op", tc.name, app, mj)
		if app != 0 {
			t.Errorf("scalar %s: appendVariantJSON did %v allocs/op, want 0", tc.name, app)
		}
	}

	// (3) whole deep object: append vs marshal allocation contrast. NOTE the deep order
	// has 3 fractional numbers (lat/lon/price), which ParseJSON stored as exact
	// Decimal16 (128-bit) -- appendVariantScalar doesn't yet format those, so they take
	// the MarshalJSON *fallback*, which is what dominates append's alloc count here (NOT
	// container boxing, which the probe showed is ~1/node). With a 128-bit decimal
	// formatter this drops to ~one box per container node; reading container headers from
	// v.Bytes() directly would take it to zero.
	buf = appendVariantJSON(buf[:0], ov)
	appObj := testing.AllocsPerRun(2000, func() { buf = appendVariantJSON(buf[:0], ov) })
	mjObj := testing.AllocsPerRun(2000, func() { _, _ = ov.MarshalJSON() })
	t.Logf("deep object : appendVariantJSON=%.0f allocs/op   MarshalJSON=%.0f allocs/op (append residual = Decimal16 fallback)", appObj, mjObj)
	if appObj >= mjObj {
		t.Errorf("appendVariantJSON (%.0f) should allocate less than MarshalJSON (%.0f)", appObj, mjObj)
	}
}
