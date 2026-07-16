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

// Integration check for DESIGN-variant.md §2/§7: the reusable variant.AppendJSON
// (github.com/couchbase/n1k1/variant) — the zero-garbage Variant→JSON projector — works
// on VARIANT values read back from a real Parquet file (real metadata dictionaries,
// Decimal16 fractional numbers), matching MarshalJSON and allocating nothing. The
// emitter's own unit tests live in the variant package; this pins the records↔variant
// seam (a `case *extensions.VariantArray` in appendArrowValueJSON would call this).

import (
	"encoding/json"
	"reflect"
	"testing"

	av "github.com/apache/arrow-go/v18/parquet/variant"

	"github.com/apache/arrow-go/v18/arrow/memory"
	nvariant "github.com/couchbase/n1k1/variant"
)

func TestVariantAppendJSONFromParquet(t *testing.T) {
	mem := memory.DefaultAllocator

	mk := func(s string) av.Value {
		v, err := av.ParseJSON(s, false)
		if err != nil {
			t.Fatalf("ParseJSON(%q): %v", s, err)
		}
		return v
	}
	// A deep order (fractional fields are Decimal16) + a couple scalars.
	order := mk(`{"customer":{"address":{"city":"London","geo":{"lat":51.5,"lon":-0.12}},` +
		`"name":"Ada"},"id":"order-1001","orderlines":[{"price":9.99,"qty":2,"sku":"A1"},` +
		`{"qty":1,"sku":"B2","tags":["x","y"]}]}`)

	va := roundTripVariants(t, mem, order, mk(`4200000`), mk(`"hi"`))

	// (1) variant.AppendJSON matches MarshalJSON (semantically) on Parquet-read values.
	for i := 0; i < va.Len(); i++ {
		v, err := va.Value(i)
		if err != nil {
			t.Fatalf("Value(%d): %v", i, err)
		}
		got := nvariant.AppendJSON(nil, v)
		want, _ := v.MarshalJSON()
		var ga, wa any
		if err := json.Unmarshal(got, &ga); err != nil {
			t.Fatalf("row %d: AppendJSON invalid JSON %q: %v", i, got, err)
		}
		_ = json.Unmarshal(want, &wa)
		if !reflect.DeepEqual(ga, wa) {
			t.Errorf("row %d: AppendJSON=%s != MarshalJSON=%s", i, got, want)
		}
	}

	// (2) zero-alloc on the deep Parquet-read order (Decimal16 fields included).
	ov, _ := va.Value(0)
	buf := make([]byte, 0, 512)
	buf = nvariant.AppendJSON(buf[:0], ov)
	if n := testing.AllocsPerRun(2000, func() { buf = nvariant.AppendJSON(buf[:0], ov) }); n != 0 {
		t.Errorf("AppendJSON(deep order): %v allocs/op, want 0", n)
	}
	// t.Logf("Parquet-read order via variant.AppendJSON: %s", buf)
}
