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

import (
	"testing"

	av "github.com/apache/arrow-go/v18/parquet/variant"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/variant"
)

// TestVariantCarrierProjectsBackToJSON is the end-to-end foundation check: encode a real
// Apache VARIANT value into base's carrier envelope, then project it back through the
// registered hook — it must match the ./variant projector exactly. This exercises the
// whole "carry a V value, read it back" vertical (base framing + av.New reconstruction +
// variant.AppendJSON) even though the scan doesn't emit V yet (Phase-1 step 2).
func TestVariantCarrierProjectsBackToJSON(t *testing.T) {
	for _, js := range []string{
		`{"a":1,"b":"x","c":[1,2,3]}`,
		`{"customer":{"name":"Ada","address":{"city":"London"}},"total":9.99}`,
		`42`,
		`"hello"`,
		`[true,null,3.5]`,
	} {
		v, err := av.ParseJSON(js, false)
		if err != nil {
			t.Fatalf("ParseJSON(%q): %v", js, err)
		}
		env := base.AppendVariantEnvelope(nil, v.Metadata().Bytes(), v.Bytes())
		if !base.IsVariant(env) {
			t.Fatalf("%s: IsVariant(env)=false", js)
		}
		got := string(base.VariantProjectJSON(nil, env))
		want := string(variant.AppendJSON(nil, v))
		if got != want {
			t.Errorf("%s: carrier projection = %s, want %s", js, got, want)
		}
	}
}

// TestVariantValTypeMatchesArrowGo pins base's hardcoded VARIANT tag IDs (base/variant.go)
// against arrow-go's actual enum: it builds the value-stream tag byte from the arrow-go
// constant, so if arrow-go ever renumbers a primitive, base's now-stale numeric ID
// mis-classifies and this test fails.
func TestVariantValTypeMatchesArrowGo(t *testing.T) {
	prim := func(pt av.PrimitiveType) byte { return byte(av.BasicPrimitive) | byte(pt)<<2 }

	cases := []struct {
		name string
		tag  byte
		want int
	}{
		{"null", prim(av.PrimitiveNull), base.ValTypeNull},
		{"bool-true", prim(av.PrimitiveBoolTrue), base.ValTypeBoolean},
		{"bool-false", prim(av.PrimitiveBoolFalse), base.ValTypeBoolean},
		{"int8", prim(av.PrimitiveInt8), base.ValTypeNumber},
		{"int16", prim(av.PrimitiveInt16), base.ValTypeNumber},
		{"int32", prim(av.PrimitiveInt32), base.ValTypeNumber},
		{"int64", prim(av.PrimitiveInt64), base.ValTypeNumber},
		{"double", prim(av.PrimitiveDouble), base.ValTypeNumber},
		{"float", prim(av.PrimitiveFloat), base.ValTypeNumber},
		{"decimal4", prim(av.PrimitiveDecimal4), base.ValTypeNumber},
		{"decimal8", prim(av.PrimitiveDecimal8), base.ValTypeNumber},
		{"decimal16", prim(av.PrimitiveDecimal16), base.ValTypeNumber},
		{"string", prim(av.PrimitiveString), base.ValTypeString},
		{"date", prim(av.PrimitiveDate), base.ValTypeString},
		{"ts-micros", prim(av.PrimitiveTimestampMicros), base.ValTypeString},
		{"ts-micros-ntz", prim(av.PrimitiveTimestampMicrosNTZ), base.ValTypeString},
		{"time-micros-ntz", prim(av.PrimitiveTimeMicrosNTZ), base.ValTypeString},
		{"ts-nanos", prim(av.PrimitiveTimestampNanos), base.ValTypeString},
		{"ts-nanos-ntz", prim(av.PrimitiveTimestampNanosNTZ), base.ValTypeString},
		{"uuid", prim(av.PrimitiveUUID), base.ValTypeString},
		{"binary", prim(av.PrimitiveBinary), base.ValTypeString},
		{"short-string", byte(av.BasicShortString), base.ValTypeString},
		{"object", byte(av.BasicObject), base.ValTypeObject},
		{"array", byte(av.BasicArray), base.ValTypeArray},
	}
	for _, c := range cases {
		env := base.AppendVariantEnvelope(nil, nil, []byte{c.tag})
		if got := base.VariantValType(env); got != c.want {
			t.Errorf("%s (tag=0x%02x): base.VariantValType = %d, want %d", c.name, c.tag, got, c.want)
		}
	}
}
