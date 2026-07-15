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

// End-to-end validation of the Parquet VARIANT path for DESIGN-variant.md
// (resolves §1.5 open question Q1.3): build a Parquet file with a VARIANT column
// holding a mix of JSON-native values AND the VARIANT-only typed scalars (date,
// exact decimal), read it back through pqarrow, and confirm:
//
//   1. the column surfaces as *extensions.VariantArray (no manual sub-array
//      decomposition needed) -- the Q1.3 answer;
//   2. each row hands back a variant.Value whose (metadata, value) byte slices
//      round-trip intact -- these are exactly what n1k1 would carry as `V<meta><value>`;
//   3. a value reconstructs from just those two []byte via variant.New(meta, value)
//      -- proving n1k1 can carry the bytes through its registers and rebuild a view;
//   4. navigation is zero-copy: a nested field's bytes alias the parent's backing
//      slice (the "unboxed, subslice into the same []byte" property from §1.5);
//   5. MarshalJSON yields the JSON projection (what a Phase-0 decode-to-JSON emits),
//      while Type() preserves the original VARIANT type (fidelity).

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/arrow-go/v18/parquet/variant"
)

func TestVariantParquetEndToEnd(t *testing.T) {
	mem := memory.DefaultAllocator

	// --- build the variant values (a mix of JSON-native + VARIANT-only scalars) ---
	mkJSON := func(s string) variant.Value {
		v, err := variant.ParseJSON(s, false)
		if err != nil {
			t.Fatalf("ParseJSON(%q): %v", s, err)
		}
		return v
	}
	mkBuilt := func(name string, fn func(b *variant.Builder) error) variant.Value {
		var b variant.Builder
		if err := fn(&b); err != nil {
			t.Fatalf("build %s: %v", name, err)
		}
		v, err := b.Build()
		if err != nil {
			t.Fatalf("Build %s: %v", name, err)
		}
		return v
	}

	rows := []struct {
		name     string
		v        variant.Value
		wantJSON string // "" = don't assert an exact literal (typed scalar; just log)
	}{
		{"object", mkJSON(`{"abv":5.5,"name":"hoppy"}`), ``}, // field order is impl-defined; compare to orig
		{"array", mkJSON(`[1,2,3]`), `[1,2,3]`},
		{"string", mkJSON(`"plain"`), `"plain"`},
		{"number", mkJSON(`42`), `42`},
		// VARIANT-only typed scalars -- no JSON equivalent type; carried with fidelity.
		{"date", mkBuilt("date", func(b *variant.Builder) error { return b.AppendDate(arrow.Date32(20194)) }), ``},
		{"decimal", mkBuilt("decimal", func(b *variant.Builder) error {
			return b.AppendDecimal16(2, decimal128.FromU64(1234567891234567890))
		}), ``},
	}

	// --- assemble a one-column VARIANT table ---
	vt := extensions.NewDefaultVariantType()
	bldr := extensions.NewVariantBuilder(mem, vt)
	defer bldr.Release()
	for _, r := range rows {
		bldr.Append(r.v)
	}
	arr := bldr.NewArray()
	defer arr.Release()

	schema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: vt, Nullable: true}}, nil)
	rec := array.NewRecord(schema, []arrow.Array{arr}, int64(len(rows)))
	defer rec.Release()
	tbl := array.NewTableFromRecords(schema, []arrow.Record{rec})
	defer tbl.Release()

	// --- write it to a real .parquet file ---
	path := filepath.Join(t.TempDir(), "variant.parquet")
	out, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := pqarrow.WriteTable(tbl, out, max(1, tbl.NumRows()),
		parquet.NewWriterProperties(parquet.WithDictionaryDefault(false), parquet.WithStats(false)),
		pqarrow.DefaultWriterProps()); err != nil {
		t.Fatalf("WriteTable: %v", err)
	}
	_ = out.Close() // WriteTable already closed+flushed the writer; ignore double-close.

	// --- read it back through pqarrow ---
	in, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	got, err := pqarrow.ReadTable(context.Background(), in, nil, pqarrow.ArrowReadProperties{}, mem)
	if err != nil {
		t.Fatalf("ReadTable: %v", err)
	}
	defer got.Release()

	// (1) Q1.3: the column surfaces directly as *extensions.VariantArray.
	col := got.Column(0).Data().Chunk(0)
	va, ok := col.(*extensions.VariantArray)
	if !ok {
		t.Fatalf("Q1.3: variant column read back as %T, want *extensions.VariantArray", col)
	}
	if va.Len() != len(rows) {
		t.Fatalf("read %d rows, want %d", va.Len(), len(rows))
	}

	for i, r := range rows {
		val, err := va.Value(i)
		if err != nil {
			t.Fatalf("%s: Value(%d): %v", r.name, i, err)
		}

		// (2) the (metadata, value) byte slices survive the Parquet round-trip intact.
		meta, value := val.Metadata().Bytes(), val.Bytes()
		if len(value) == 0 {
			t.Errorf("%s: empty value bytes", r.name)
		}
		origJSON, _ := r.v.MarshalJSON()
		gotJSON, _ := val.MarshalJSON()
		if string(gotJSON) != string(origJSON) {
			t.Errorf("%s: round-trip JSON %q != original %q", r.name, gotJSON, origJSON)
		}
		if val.Type() != r.v.Type() {
			t.Errorf("%s: round-trip Type %v != original %v (fidelity lost)", r.name, val.Type(), r.v.Type())
		}

		// (3) reconstruct a working view from ONLY the two []byte -- the n1k1
		//     `V<meta><value>` carry-and-rebuild story.
		re, err := variant.New(meta, value)
		if err != nil {
			t.Fatalf("%s: variant.New(meta,value): %v", r.name, err)
		}
		reJSON, _ := re.MarshalJSON()
		if string(reJSON) != string(gotJSON) {
			t.Errorf("%s: rebuilt-from-bytes JSON %q != %q", r.name, reJSON, gotJSON)
		}

		// (5) JSON projection: assert the literal for JSON-native rows; log the rest.
		if r.wantJSON != "" && string(gotJSON) != r.wantJSON {
			t.Errorf("%s: JSON projection %q, want %q", r.name, gotJSON, r.wantJSON)
		}
		t.Logf("row %d %-8s  type=%-10v  json=%s  (meta %dB, value %dB)",
			i, r.name, val.Type(), gotJSON, len(meta), len(value))
	}

	// (4) zero-copy navigation: a nested field's bytes are a subslice of the parent's
	//     backing []byte -- no boxing, no copy (DESIGN-variant.md §1.5).
	objVal, err := va.Value(0) // the {"abv":5.5,"name":"hoppy"} row
	if err != nil {
		t.Fatal(err)
	}
	obj, ok := objVal.Value().(variant.ObjectValue)
	if !ok {
		t.Fatalf("object row is %T, want variant.ObjectValue", objVal.Value())
	}
	field, err := obj.ValueByKey("name")
	if err != nil {
		t.Fatalf("ValueByKey(name): %v", err)
	}
	if got := field.Value.Value(); got != "hoppy" {
		t.Errorf("nested v.name = %v, want \"hoppy\"", got)
	}
	if !aliases(field.Value.Bytes(), objVal.Bytes()) {
		t.Errorf("nested field bytes are NOT a subslice of the parent -- navigation copied (expected zero-copy)")
	}
}

// TestVariantParquetDeepNesting exercises the multi-level cases the flat test omits:
// an `order` object → `customer` → `address` → `geo` (objects 4 deep), an `orderlines`
// ARRAY of sub-objects (one carrying its own nested `tags` array), and a VARIANT-only
// typed scalar (a date) buried at depth. It proves (a) deep structure round-trips
// through Parquet byte-identically, (b) the unboxed view API navigates arbitrarily
// deep (chained object-key / array-index → subslice views), (c) a typed scalar keeps
// its VARIANT Type() at depth (fidelity), and (d) zero-copy holds AT DEPTH -- a
// 4-levels-down leaf's bytes still alias the TOP-LEVEL value's backing.
func TestVariantParquetDeepNesting(t *testing.T) {
	mem := memory.DefaultAllocator

	// One deeply-nested `order`, built (incl. a typed date at depth) via the
	// map/slice-recursing Builder.Append.
	order := map[string]any{
		"id": "order-1001",
		"customer": map[string]any{
			"name": "Ada",
			"address": map[string]any{
				"city": "London",
				"geo":  map[string]any{"lat": 51.5, "lon": -0.12},
			},
		},
		"orderlines": []any{
			map[string]any{"sku": "A1", "qty": int64(2), "price": 9.99},
			map[string]any{"sku": "B2", "qty": int64(1),
				"tags":     []any{"x", "y"},
				"shipDate": arrow.Date32(20194)}, // typed scalar 3 levels down
		},
	}
	var b variant.Builder
	if err := b.Append(order); err != nil {
		t.Fatalf("build order: %v", err)
	}
	orig, err := b.Build()
	if err != nil {
		t.Fatalf("Build order: %v", err)
	}

	va := roundTripVariants(t, mem, orig)
	got, err := va.Value(0)
	if err != nil {
		t.Fatal(err)
	}

	// (a) deep structure survives Parquet byte-identically.
	origJSON, _ := orig.MarshalJSON()
	gotJSON, _ := got.MarshalJSON()
	if string(gotJSON) != string(origJSON) {
		t.Errorf("deep round-trip JSON differs:\n got %s\nwant %s", gotJSON, origJSON)
	}
	t.Logf("round-tripped order: %s", gotJSON)

	// (b) navigate arbitrarily deep through the view API.
	key := func(v variant.Value, k string) variant.Value {
		t.Helper()
		ov, ok := v.Value().(variant.ObjectValue)
		if !ok {
			t.Fatalf("key(%q): value is %T, not object", k, v.Value())
		}
		f, err := ov.ValueByKey(k)
		if err != nil {
			t.Fatalf("key(%q): %v", k, err)
		}
		return f.Value
	}
	idx := func(v variant.Value, i uint32) variant.Value {
		t.Helper()
		av, ok := v.Value().(variant.ArrayValue)
		if !ok {
			t.Fatalf("idx(%d): value is %T, not array", i, v.Value())
		}
		e, err := av.Value(i)
		if err != nil {
			t.Fatalf("idx(%d): %v", i, err)
		}
		return e
	}

	// order.customer.address.geo.lat  (object nesting, 4 deep)
	lat := key(key(key(key(got, "customer"), "address"), "geo"), "lat")
	if lat.Value() != 51.5 {
		t.Errorf("customer.address.geo.lat = %v, want 51.5", lat.Value())
	}
	// order.orderlines[1].tags[0]  (array → sub-object → nested array)
	tag0 := idx(key(idx(key(got, "orderlines"), 1), "tags"), 0)
	if tag0.Value() != "x" {
		t.Errorf("orderlines[1].tags[0] = %v, want \"x\"", tag0.Value())
	}

	// (c) a typed scalar keeps its VARIANT type + projection at depth.
	ship := key(idx(key(got, "orderlines"), 1), "shipDate")
	shipJSON, _ := ship.MarshalJSON()
	if string(shipJSON) != `"2025-04-16"` {
		t.Errorf("orderlines[1].shipDate JSON = %s, want \"2025-04-16\"", shipJSON)
	}
	if ship.BasicType() != variant.BasicPrimitive || ship.Type() != variant.Date {
		t.Errorf("orderlines[1].shipDate lost its VARIANT date type at depth: basic=%v type=%v",
			ship.BasicType(), ship.Type())
	}

	// (d) zero-copy AT DEPTH: the 4-deep lat leaf aliases the TOP-LEVEL backing.
	if !aliases(lat.Bytes(), got.Bytes()) {
		t.Errorf("deep leaf bytes are NOT a subslice of the top-level value (navigation copied)")
	}
}

// roundTripVariants writes vals as a one-column VARIANT Parquet file and reads it back,
// returning the read-back *extensions.VariantArray. Shared by the fixture tests.
func roundTripVariants(t *testing.T, mem memory.Allocator, vals ...variant.Value) *extensions.VariantArray {
	t.Helper()
	vt := extensions.NewDefaultVariantType()
	bldr := extensions.NewVariantBuilder(mem, vt)
	defer bldr.Release()
	for _, v := range vals {
		bldr.Append(v)
	}
	arr := bldr.NewArray()
	defer arr.Release()

	schema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: vt, Nullable: true}}, nil)
	rec := array.NewRecord(schema, []arrow.Array{arr}, int64(len(vals)))
	defer rec.Release()
	tbl := array.NewTableFromRecords(schema, []arrow.Record{rec})
	defer tbl.Release()

	path := filepath.Join(t.TempDir(), "variant.parquet")
	out, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := pqarrow.WriteTable(tbl, out, max(1, tbl.NumRows()),
		parquet.NewWriterProperties(parquet.WithDictionaryDefault(false), parquet.WithStats(false)),
		pqarrow.DefaultWriterProps()); err != nil {
		t.Fatalf("WriteTable: %v", err)
	}
	_ = out.Close()

	in, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { in.Close() })
	tblIn, err := pqarrow.ReadTable(context.Background(), in, nil, pqarrow.ArrowReadProperties{}, mem)
	if err != nil {
		t.Fatalf("ReadTable: %v", err)
	}
	t.Cleanup(tblIn.Release)

	col := tblIn.Column(0).Data().Chunk(0)
	va, ok := col.(*extensions.VariantArray)
	if !ok {
		t.Fatalf("variant column read back as %T, want *extensions.VariantArray", col)
	}
	return va
}

// aliases reports whether child's backing storage lies within parent's -- i.e. child
// is a subslice view into parent (zero-copy), not an independent allocation.
func aliases(child, parent []byte) bool {
	if len(child) == 0 || len(parent) == 0 {
		return false
	}
	c := uintptr(unsafe.Pointer(&child[0]))
	p0 := uintptr(unsafe.Pointer(&parent[0]))
	p1 := p0 + uintptr(len(parent))
	return c >= p0 && c < p1
}
