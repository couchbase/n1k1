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

package variant

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/decimal"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	av "github.com/apache/arrow-go/v18/parquet/variant"
	"github.com/google/uuid"
)

// typedScalars builds one variant.Value per VARIANT-only typed scalar (the types that
// used to take the MarshalJSON fallback), including all four timestamp variants.
func typedScalars(t *testing.T) map[string]av.Value {
	t.Helper()
	build := func(name string, fn func(b *av.Builder) error) av.Value {
		var b av.Builder
		if err := fn(&b); err != nil {
			t.Fatalf("build %s: %v", name, err)
		}
		v, err := b.Build()
		if err != nil {
			t.Fatalf("Build %s: %v", name, err)
		}
		return v
	}
	// AppendTimestamp(v, useMicros, useUTC): useUTC=true → tz-aware (rendered UTC),
	// useUTC=false → NTZ (rendered in time.Local).
	ts := arrow.Timestamp(1713270896789123)
	return map[string]av.Value{
		"date":          build("date", func(b *av.Builder) error { return b.AppendDate(20194) }),
		"time":          build("time", func(b *av.Builder) error { return b.AppendTimeMicro(arrow.Time64(45296789123)) }),
		"ts-micros-utc": build("ts-micros-utc", func(b *av.Builder) error { return b.AppendTimestamp(ts, true, true) }),
		"ts-micros-ntz": build("ts-micros-ntz", func(b *av.Builder) error { return b.AppendTimestamp(ts, true, false) }),
		"ts-nanos-utc":  build("ts-nanos-utc", func(b *av.Builder) error { return b.AppendTimestamp(ts, false, true) }),
		"ts-nanos-ntz":  build("ts-nanos-ntz", func(b *av.Builder) error { return b.AppendTimestamp(ts, false, false) }),
		"uuid":          build("uuid", func(b *av.Builder) error { return b.AppendUUID(uuid.MustParse("12345678-90ab-cdef-1234-567890abcdef")) }),
		"binary":        build("binary", func(b *av.Builder) error { return b.AppendBinary([]byte("hello\x00\xff world")) }),
		"binary-empty":  build("binary-empty", func(b *av.Builder) error { return b.AppendBinary(nil) }),
		"uuid-zero":     build("uuid-zero", func(b *av.Builder) error { return b.AppendUUID(uuid.UUID{}) }),
	}
}

// TestAppendJSONTypedScalars asserts AppendJSON is BYTE-identical to MarshalJSON for the
// VARIANT-only typed scalars (date/timestamp/time/uuid/binary) — the newly-added
// dst-formatters must reproduce arrow-go's rendering exactly, not merely a semantic
// equivalent.
func TestAppendJSONTypedScalars(t *testing.T) {
	for name, v := range typedScalars(t) {
		want, err := v.MarshalJSON()
		if err != nil {
			t.Fatalf("%s: MarshalJSON: %v", name, err)
		}
		if got := AppendJSON(nil, v); string(got) != string(want) {
			t.Errorf("%s: AppendJSON=%s, want (byte-identical) %s", name, got, want)
		}
	}
}

// TestAppendDecimal128 checks the 128-bit decimal formatter is byte-identical to
// arrow-go's decimal128.Num.ToString(scale) (which is what variant's DecimalValue
// MarshalJSON emits) across sign / magnitude / scale edges.
func TestAppendDecimal128(t *testing.T) {
	nums := []decimal128.Num{
		decimal128.FromI64(0),
		decimal128.FromI64(1),
		decimal128.FromI64(-1),
		decimal128.FromI64(12),
		decimal128.FromI64(-12),
		decimal128.FromI64(999),
		decimal128.FromI64(515),
		decimal128.FromI64(-51500),
		decimal128.FromU64(1234567891234567890),
		decimal128.New(669260594276348691, 15112505930532393806), // a big 128-bit value
		decimal128.New(-1, 0),                                    // -2^64
	}
	scales := []int{0, 1, 2, 5, 20, 38}
	for _, n := range nums {
		for _, s := range scales {
			want := n.ToString(int32(s))
			got := string(AppendDecimal128(nil, n.HighBits(), n.LowBits(), s))
			if got != want {
				t.Errorf("AppendDecimal128(hi=%d, lo=%d, scale=%d) = %q, want %q",
					n.HighBits(), n.LowBits(), s, got, want)
			}
		}
	}
}

// TestAppendJSON checks AppendJSON is semantically equal to variant.Value.MarshalJSON
// across scalars, nested objects/arrays, and the VARIANT-only typed scalars — including
// the deep "order" shape (objects 4-deep + array-of-subobjects + nested array) and the
// fractional numbers that ParseJSON stores as exact Decimal16.
func TestAppendJSON(t *testing.T) {
	mkJSON := func(s string) av.Value {
		v, err := av.ParseJSON(s, false)
		if err != nil {
			t.Fatalf("ParseJSON(%q): %v", s, err)
		}
		return v
	}
	built := func(name string, fn func(b *av.Builder) error) av.Value {
		var b av.Builder
		if err := fn(&b); err != nil {
			t.Fatalf("build %s: %v", name, err)
		}
		v, err := b.Build()
		if err != nil {
			t.Fatalf("Build %s: %v", name, err)
		}
		return v
	}

	cases := map[string]av.Value{
		"null":      mkJSON(`null`),
		"true":      mkJSON(`true`),
		"int":       mkJSON(`42`),
		"negint":    mkJSON(`-17`),
		"bigint":    mkJSON(`10000000000`),
		"frac":      mkJSON(`3.14159`), // Decimal16
		"small-dec": mkJSON(`0.001`),   // Decimal16, value < 1
		"neg-dec":   mkJSON(`-0.12`),   // Decimal16, negative < 1
		"str":       mkJSON(`"plain"`),
		"str-esc":   mkJSON(`"a\"b\n\tc\\d"`), // escaping
		"str-uni":   mkJSON(`"café ☕ 🐢"`),     // multi-byte UTF-8 passes through
		"empty-obj": mkJSON(`{}`),
		"arr":       mkJSON(`[1,"two",3.5,true,null]`),
		"deep": mkJSON(`{"customer":{"address":{"city":"London","geo":{"lat":51.5,"lon":-0.12}},` +
			`"name":"Ada"},"id":"order-1001","orderlines":[{"price":9.99,"qty":2,"sku":"A1"},` +
			`{"qty":1,"sku":"B2","tags":["x","y"]}]}`),
		"dec8": built("dec8", func(b *av.Builder) error { return b.AppendDecimal8(2, decimal.Decimal64(1234)) }),
		"dec4": built("dec4", func(b *av.Builder) error { return b.AppendDecimal4(3, decimal.Decimal32(-45)) }),
		"date": built("date", func(b *av.Builder) error { return b.AppendDate(20194) }),
	}

	for name, v := range cases {
		got := AppendJSON(nil, v)
		want, err := v.MarshalJSON()
		if err != nil {
			t.Fatalf("%s: MarshalJSON: %v", name, err)
		}
		var ga, wa any
		if err := json.Unmarshal(got, &ga); err != nil {
			t.Errorf("%s: AppendJSON emitted invalid JSON %q: %v", name, got, err)
			continue
		}
		if err := json.Unmarshal(want, &wa); err != nil {
			t.Fatalf("%s: MarshalJSON produced invalid JSON %q: %v", name, want, err)
		}
		if !reflect.DeepEqual(ga, wa) {
			t.Errorf("%s: AppendJSON=%s  !=  MarshalJSON=%s", name, got, want)
		}
	}

	// An empty array is a case where AppendJSON is MORE correct than MarshalJSON:
	// arrow-go's ArrayValue.MarshalJSON does slices.Collect (→ nil slice) → json.Marshal
	// → "null", whereas AppendJSON emits "[]". Assert the correct output directly.
	if got := string(AppendJSON(nil, mkJSON(`[]`))); got != `[]` {
		t.Errorf("empty array: AppendJSON=%q, want %q", got, `[]`)
	}
}

// TestAppendJSONZeroAlloc asserts AppendJSON allocates nothing per call (after warmup)
// for the JSON-native + decimal core — scalars and a deep object whose fractional
// fields are Decimal16 — appending into a reused buffer.
func TestAppendJSONZeroAlloc(t *testing.T) {
	mk := func(s string) av.Value {
		v, _ := av.ParseJSON(s, false)
		return v
	}
	cases := map[string]av.Value{
		"int":     mk(`4200000`),
		"string":  mk(`"hello, world"`),
		"decimal": mk(`3.14159`), // Decimal16 → AppendDecimal128, zero-alloc
		"deep": mk(`{"customer":{"address":{"city":"London","geo":{"lat":51.5,"lon":-0.12}},` +
			`"name":"Ada"},"id":"order-1001","orderlines":[{"price":9.99,"qty":2,"sku":"A1"},` +
			`{"qty":1,"sku":"B2","tags":["x","y"]}]}`),
	}
	// The typed scalars must be zero-alloc too (AppendFormat / AppendEncode / hand-rolled
	// UUID all append into the reused buffer).
	for name, v := range typedScalars(t) {
		cases[name] = v
	}
	buf := make([]byte, 0, 512)
	for name, v := range cases {
		buf = AppendJSON(buf[:0], v) // warm the buffer
		n := testing.AllocsPerRun(2000, func() { buf = AppendJSON(buf[:0], v) })
		if n != 0 {
			t.Errorf("AppendJSON(%s): %v allocs/op, want 0 (out=%s)", name, n, buf)
		}
	}
}
