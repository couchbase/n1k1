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
	"testing"
)

// tag builds a VARIANT value-stream first byte from a basic_type + type_info.
func tag(basic, info int) byte { return byte(basic&0x03) | byte(info&0x3F)<<2 }

func TestVariantEnvelopeRoundTrip(t *testing.T) {
	meta := []byte{0x01, 0x02, 0x03}
	value := []byte{tag(variantBasicObject, 0), 9, 9, 9}

	env := AppendVariantEnvelope(nil, meta, value)
	if !IsVariant(env) {
		t.Fatalf("IsVariant(env) = false, want true (env=%v)", env)
	}
	gotMeta, gotValue, ok := SplitVariantEnvelope(env)
	if !ok {
		t.Fatalf("SplitVariantEnvelope: ok=false")
	}
	if !bytes.Equal(gotMeta, meta) {
		t.Errorf("meta = %v, want %v", gotMeta, meta)
	}
	if !bytes.Equal(gotValue, value) {
		t.Errorf("value = %v, want %v", gotValue, value)
	}

	// Empty metadata is valid (a scalar variant with no field names).
	env2 := AppendVariantEnvelope(nil, nil, []byte{tag(variantBasicPrimitive, vpInt64)})
	m2, v2, ok := SplitVariantEnvelope(env2)
	if !ok || len(m2) != 0 || len(v2) != 1 {
		t.Errorf("empty-meta envelope: ok=%v meta=%v value=%v", ok, m2, v2)
	}
}

func TestSplitVariantEnvelopeMalformed(t *testing.T) {
	cases := map[string]Val{
		"nil":           nil,
		"empty":         {},
		"json-object":   Val(`{"a":1}`),
		"json-string":   Val(`"V is fine inside a string"`),
		"sigil-only":    {SigilVariant},
		"meta-overruns": append([]byte{SigilVariant}, 0x7f), // uvarint says 127 bytes of meta, none present
	}
	for name, v := range cases {
		if _, _, ok := SplitVariantEnvelope(v); ok {
			t.Errorf("%s: SplitVariantEnvelope ok=true, want false", name)
		}
		if name == "json-object" || name == "json-string" {
			if IsVariant(v) {
				t.Errorf("%s: IsVariant=true, want false", name)
			}
		}
	}
}

func TestVariantValType(t *testing.T) {
	cases := []struct {
		name  string
		value []byte
		want  int
	}{
		{"object", []byte{tag(variantBasicObject, 0)}, ValTypeObject},
		{"array", []byte{tag(variantBasicArray, 0)}, ValTypeArray},
		{"short-string", []byte{tag(variantBasicShortString, 5)}, ValTypeString},
		{"null", []byte{tag(variantBasicPrimitive, vpNull)}, ValTypeNull},
		{"true", []byte{tag(variantBasicPrimitive, vpBoolTrue)}, ValTypeBoolean},
		{"false", []byte{tag(variantBasicPrimitive, vpBoolFalse)}, ValTypeBoolean},
		{"int64", []byte{tag(variantBasicPrimitive, vpInt64)}, ValTypeNumber},
		{"float", []byte{tag(variantBasicPrimitive, vpFloat)}, ValTypeNumber},
		{"decimal16", []byte{tag(variantBasicPrimitive, vpDecimal16)}, ValTypeNumber},
		{"long-string", []byte{tag(variantBasicPrimitive, vpString)}, ValTypeString},
		{"date", []byte{tag(variantBasicPrimitive, vpDate)}, ValTypeString},
		{"ts-micros", []byte{tag(variantBasicPrimitive, vpTsMicros)}, ValTypeString},
		{"time", []byte{tag(variantBasicPrimitive, vpTimeNZ)}, ValTypeString},
		{"uuid", []byte{tag(variantBasicPrimitive, vpUUID)}, ValTypeString},
		{"binary", []byte{tag(variantBasicPrimitive, vpBinary)}, ValTypeString},
	}
	for _, c := range cases {
		env := AppendVariantEnvelope(nil, nil, c.value)
		if got := VariantValType(env); got != c.want {
			t.Errorf("%s: VariantValType = %d, want %d", c.name, got, c.want)
		}
	}
	// A malformed / empty-value envelope classifies as Unknown, not a panic.
	if got := VariantValType(Val{SigilVariant, 0x00}); got != ValTypeUnknown {
		t.Errorf("empty-value envelope: VariantValType = %d, want %d (Unknown)", got, ValTypeUnknown)
	}
}

// BenchmarkVariantValType and BenchmarkSplitVariantEnvelope characterize the
// arrow-go-free carrier primitives base runs on the hot path: classification and the
// zero-copy split must be cheap and alloc-free.
func BenchmarkVariantValType(b *testing.B) {
	env := AppendVariantEnvelope(nil, []byte{0x01, 0x02, 0x03}, []byte{tag(variantBasicObject, 0), 9, 9})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = VariantValType(env)
	}
}

func BenchmarkSplitVariantEnvelope(b *testing.B) {
	env := AppendVariantEnvelope(nil, []byte{0x01, 0x02, 0x03}, []byte{tag(variantBasicObject, 0), 9, 9})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = SplitVariantEnvelope(env)
	}
}

func TestVariantProjectJSONHook(t *testing.T) {
	// No hook registered -> null (and never a panic).
	saved := VariantAppendJSON
	VariantAppendJSON = nil
	defer func() { VariantAppendJSON = saved }()

	env := AppendVariantEnvelope(nil, []byte{0xAB}, []byte{tag(variantBasicPrimitive, vpInt64), 0})
	if got := string(VariantProjectJSON(nil, env)); got != "null" {
		t.Errorf("no hook: VariantProjectJSON = %q, want \"null\"", got)
	}

	// A registered hook receives the split (meta, value) and its output is returned.
	VariantAppendJSON = func(dst, meta, value []byte) []byte {
		dst = append(dst, "META="...)
		dst = append(dst, meta...)
		dst = append(dst, "/VAL="...)
		return append(dst, value...)
	}
	got := string(VariantProjectJSON([]byte("pre:"), env))
	want := "pre:META=\xab/VAL=" + string([]byte{tag(variantBasicPrimitive, vpInt64), 0})
	if got != want {
		t.Errorf("hook: VariantProjectJSON = %q, want %q", got, want)
	}

	// Malformed envelope -> null even with a hook installed.
	if got := string(VariantProjectJSON(nil, Val(`{"not":"variant"}`))); got != "null" {
		t.Errorf("malformed: VariantProjectJSON = %q, want \"null\"", got)
	}
}
