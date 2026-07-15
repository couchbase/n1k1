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
