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
	"encoding/json"
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	av "github.com/apache/arrow-go/v18/parquet/variant"

	"github.com/couchbase/n1k1/base"
)

// TestVariantRowAssemble builds an {id string, order VARIANT} batch and assembles each
// row into a whole-row VARIANT object, asserting: (1) the object projects to the same
// JSON as the Phase-0 read would (correctness), (2) a typed VARIANT date inside `order`
// survives assembly as av.Date, not merely its ISO-string projection (fidelity — the
// whole point of the carrier), and (3) a null VARIANT cell round-trips as JSON null. One
// assembler is reused across all rows to exercise builder/buffer reuse.
func TestVariantRowAssemble(t *testing.T) {
	mem := memory.DefaultAllocator
	vt := extensions.NewDefaultVariantType()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "order", Type: vt, Nullable: true},
	}, nil)
	rb := array.NewRecordBuilder(mem, schema)
	defer rb.Release()
	idB := rb.Field(0).(*array.StringBuilder)
	orderB := rb.Field(1).(*extensions.VariantBuilder)

	// Row 0: order = {"d": <DATE 2025-04-16>, "name":"Ada"} — d is a *typed* date.
	var vb av.Builder
	start := vb.Offset()
	var f []av.FieldEntry
	f = append(f, vb.NextField(start, "d"))
	if err := vb.AppendDate(20194); err != nil { // 20194 days -> 2025-04-16
		t.Fatal(err)
	}
	f = append(f, vb.NextField(start, "name"))
	if err := vb.AppendString("Ada"); err != nil {
		t.Fatal(err)
	}
	if err := vb.FinishObject(start, f); err != nil {
		t.Fatal(err)
	}
	orderDate, err := vb.Build()
	if err != nil {
		t.Fatal(err)
	}
	idB.Append("o1")
	orderB.Append(orderDate)

	// Row 1: order is NULL.
	idB.Append("o2")
	orderB.AppendNull()

	// Row 2: order = a nested JSON object (number/array/bool/sub-object) — pass-through.
	orderNested, err := av.ParseJSON(`{"a":1,"b":[1,2],"c":{"x":true},"total":9.99}`, false)
	if err != nil {
		t.Fatal(err)
	}
	idB.Append("o3")
	orderB.Append(orderNested)

	rec := rb.NewRecord()
	defer rec.Release()

	var a variantRowAssembler

	want := []string{
		`{"id":"o1","order":{"d":"2025-04-16","name":"Ada"}}`,
		`{"id":"o2","order":null}`,
		`{"id":"o3","order":{"a":1,"b":[1,2],"c":{"x":true},"total":9.99}}`,
	}
	var assembled [][]byte
	for row := 0; row < int(rec.NumRows()); row++ {
		env, err := a.appendRow(nil, rec, row)
		if err != nil {
			t.Fatalf("row %d: appendRow: %v", row, err)
		}
		if !base.IsVariant(env) {
			t.Fatalf("row %d: assembled value is not a V-carrier", row)
		}
		assembled = append(assembled, env)

		got := base.VariantProjectJSON(nil, env)
		if !jsonEqual(t, got, []byte(want[row])) {
			t.Errorf("row %d: projection = %s, want %s", row, got, want[row])
		}
	}

	// Fidelity: row 0's order.d must still be a typed VARIANT date after assembly, not a
	// string — that's what distinguishes the carrier from the Phase-0 JSON projection.
	meta, value, ok := base.SplitVariantEnvelope(assembled[0])
	if !ok {
		t.Fatal("row 0: SplitVariantEnvelope failed")
	}
	rowV, err := av.New(meta, value)
	if err != nil {
		t.Fatalf("row 0: av.New: %v", err)
	}
	order, err := rowV.Value().(av.ObjectValue).ValueByKey("order")
	if err != nil {
		t.Fatalf("row 0: nav order: %v", err)
	}
	d, err := order.Value.Value().(av.ObjectValue).ValueByKey("d")
	if err != nil {
		t.Fatalf("row 0: nav order.d: %v", err)
	}
	if got := d.Value.Type(); got != av.Date {
		t.Errorf("row 0: order.d type = %v, want Date (typed-scalar fidelity lost)", got)
	}
}

// TestVariantRowAssembleMultiVariantUnsupported: a batch with two VARIANT columns is a
// shape the splice assembler can't currently handle (each cell wants dict ids 0..); it
// must error rather than silently corrupt, so the caller can fall back to Phase-0 JSON.
func TestVariantRowAssembleMultiVariantUnsupported(t *testing.T) {
	mem := memory.DefaultAllocator
	vt := extensions.NewDefaultVariantType()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: vt, Nullable: true},
		{Name: "b", Type: vt, Nullable: true},
	}, nil)
	rb := array.NewRecordBuilder(mem, schema)
	defer rb.Release()
	va, _ := av.ParseJSON(`{"x":1}`, false)
	vbv, _ := av.ParseJSON(`{"y":2}`, false)
	rb.Field(0).(*extensions.VariantBuilder).Append(va)
	rb.Field(1).(*extensions.VariantBuilder).Append(vbv)
	rec := rb.NewRecord()
	defer rec.Release()

	var a variantRowAssembler
	if _, err := a.appendRow(nil, rec, 0); err == nil {
		t.Fatal("expected an error for a multi-VARIANT-column batch, got nil")
	}
}

func jsonEqual(t *testing.T, a, b []byte) bool {
	t.Helper()
	var x, y interface{}
	if err := json.Unmarshal(a, &x); err != nil {
		t.Fatalf("invalid JSON %q: %v", a, err)
	}
	if err := json.Unmarshal(b, &y); err != nil {
		t.Fatalf("invalid JSON %q: %v", b, err)
	}
	return reflect.DeepEqual(x, y)
}
