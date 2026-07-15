//go:build n1ql

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

package glue

// Full-stack check for DESIGN-variant.md §7: a Parquet keyspace whose rows carry a
// VARIANT column is queryable through ordinary SQL++. Phase-0 renders the VARIANT to
// JSON at the scan boundary (records/parquet.go → variant.AppendJSON), so nested-field
// projection, string filters, and numeric filters over the variant "just work" via the
// existing JSON path machinery — no VARIANT-awareness in the engine/exprs.

import (
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	av "github.com/apache/arrow-go/v18/parquet/variant"
)

// writeVariantParquet writes a Parquet file at path with an `id` string column and an
// `order` VARIANT column (built from the ordersJSON strings), using a non-shredded
// (default) variant layout.
func writeVariantParquet(t *testing.T, path string, ids, ordersJSON []string) {
	t.Helper()
	writeVariantParquetVT(t, path, ids, ordersJSON, extensions.NewDefaultVariantType(), false)
}

// writeVariantParquetVT is the general writer: it builds the `order` VARIANT column with
// the given variant type `vt` (default or shredded). When wantShredded is true it asserts
// the built array is genuinely shredded, so a "shredded" fixture can't silently degrade
// to the plain layout.
func writeVariantParquetVT(t *testing.T, path string, ids, ordersJSON []string, vt *extensions.VariantType, wantShredded bool) {
	t.Helper()
	mem := memory.DefaultAllocator

	idB := array.NewStringBuilder(mem)
	defer idB.Release()
	for _, s := range ids {
		idB.Append(s)
	}
	idArr := idB.NewArray()
	defer idArr.Release()

	vB := extensions.NewVariantBuilder(mem, vt)
	defer vB.Release()
	for _, s := range ordersJSON {
		v, err := av.ParseJSON(s, false)
		if err != nil {
			t.Fatalf("ParseJSON(%q): %v", s, err)
		}
		vB.Append(v)
	}
	vArr := vB.NewArray()
	defer vArr.Release()

	if got := vArr.(*extensions.VariantArray).IsShredded(); got != wantShredded {
		t.Fatalf("IsShredded() = %v, want %v", got, wantShredded)
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "order", Type: vt, Nullable: true},
	}, nil)
	rec := array.NewRecord(schema, []arrow.Array{idArr, vArr}, int64(len(ids)))
	defer rec.Release()
	tbl := array.NewTableFromRecords(schema, []arrow.Record{rec})
	defer tbl.Release()

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
}

func TestVariantParquetKeyspaceQuery(t *testing.T) {
	dir := t.TempDir()
	ksDir := filepath.Join(dir, "default", "orders")
	if err := os.MkdirAll(ksDir, 0o755); err != nil {
		t.Fatal(err)
	}
	writeVariantParquet(t, filepath.Join(ksDir, "orders.parquet"),
		[]string{"o1", "o2"},
		[]string{
			`{"customer":{"name":"Ada","address":{"city":"London"}},"orderlines":[{"sku":"A1","qty":2}],"total":9.99}`,
			`{"customer":{"name":"Bo","address":{"city":"Paris"}},"orderlines":[{"sku":"B2","qty":1}],"total":19.5}`,
		})

	sess, err := OpenSession(dir, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}

	cases := []struct {
		name string
		stmt string
		want []string
	}{
		{
			// Nested-field projection through the VARIANT.
			"project-nested",
			`SELECT o.id, o.order.customer.name AS name FROM orders o`,
			[]string{`{"id":"o1","name":"Ada"}`, `{"id":"o2","name":"Bo"}`},
		},
		{
			// String filter on a 3-deep VARIANT field.
			"filter-string",
			`SELECT o.id FROM orders o WHERE o.order.customer.address.city = "London"`,
			[]string{`{"id":"o1"}`},
		},
		{
			// Numeric filter on a field that was an exact Decimal16 in the VARIANT,
			// projected to a JSON number by Phase-0.
			"filter-decimal",
			`SELECT o.order.total AS total FROM orders o WHERE o.order.total > 10`,
			[]string{`{"total":19.5}`},
		},
		{
			// Array element navigation inside the VARIANT.
			"array-elem",
			`SELECT o.order.orderlines[0].sku AS sku FROM orders o WHERE o.id = "o2"`,
			[]string{`{"sku":"B2"}`},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assertVariantRows(t, sess, c.stmt, c.want)
		})
	}
}

// assertVariantRows runs stmt and asserts the (order-normalized, sorted) result rows
// equal want.
func assertVariantRows(t *testing.T, sess *Session, stmt string, want []string) {
	t.Helper()
	got := runRowsCanon(t, sess, stmt)
	w := make([]string, len(want))
	for i := range want {
		w[i] = canonRow(want[i])
	}
	sort.Strings(w)
	if len(got) != len(w) {
		t.Fatalf("%s: got %d rows %v, want %d %v", stmt, len(got), got, len(w), w)
	}
	for i := range got {
		if got[i] != w[i] {
			t.Errorf("%s: row %d = %s, want %s", stmt, i, got[i], w[i])
		}
	}
}

// TestVariantParquetShreddedKeyspaceQuery is the same full-stack check as
// TestVariantParquetKeyspaceQuery, but the VARIANT column is *shredded*: the
// customer.name/customer.tier subfields are promoted to typed Parquet sub-columns
// (`typed_value`), while everything else (customer.address, total, orderlines) stays in
// the residual `value` column. pqarrow reads the shredded file back as a single
// *extensions.VariantArray and `.Value(i)` coalesces the typed sub-columns + residual
// bytes into one variant.Value, so Phase-0's variant.AppendJSON — and therefore the
// whole SQL++ path — is oblivious to the shredded physical layout. This test proves
// queries hit both the shredded fields and the residual fields correctly.
func TestVariantParquetShreddedKeyspaceQuery(t *testing.T) {
	dir := t.TempDir()
	ksDir := filepath.Join(dir, "default", "orders")
	if err := os.MkdirAll(ksDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Shred customer.name and customer.tier into typed sub-columns. customer.address
	// (not in the schema) and the other top-level fields fall through to residual.
	shredType := extensions.NewShreddedVariantType(arrow.StructOf(
		arrow.Field{Name: "customer", Type: arrow.StructOf(
			arrow.Field{Name: "name", Type: arrow.BinaryTypes.String},
			arrow.Field{Name: "tier", Type: arrow.BinaryTypes.String},
		)},
	))

	writeVariantParquetVT(t, filepath.Join(ksDir, "orders.parquet"),
		[]string{"o1", "o2"},
		[]string{
			`{"customer":{"name":"Ada","tier":"gold","address":{"city":"London"}},"total":9.99,"orderlines":[{"sku":"A1","qty":2}]}`,
			`{"customer":{"name":"Bo","tier":"silver","address":{"city":"Paris"}},"total":19.5,"orderlines":[{"sku":"B2","qty":1}]}`,
		}, shredType, true)

	sess, err := OpenSession(dir, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}

	cases := []struct {
		name string
		stmt string
		want []string
	}{
		{
			// Projection of a SHREDDED subfield.
			"shredded-name",
			`SELECT o.id, o.order.customer.name AS name FROM orders o`,
			[]string{`{"id":"o1","name":"Ada"}`, `{"id":"o2","name":"Bo"}`},
		},
		{
			// Filter on a SHREDDED subfield.
			"shredded-tier-filter",
			`SELECT o.id FROM orders o WHERE o.order.customer.tier = "gold"`,
			[]string{`{"id":"o1"}`},
		},
		{
			// Filter on a RESIDUAL nested subfield (customer.address was not shredded)
			// — proves the residual value bytes are coalesced back in.
			"residual-address-filter",
			`SELECT o.id FROM orders o WHERE o.order.customer.address.city = "Paris"`,
			[]string{`{"id":"o2"}`},
		},
		{
			// RESIDUAL top-level Decimal16-derived number.
			"residual-total",
			`SELECT o.order.total AS total FROM orders o WHERE o.order.total > 10`,
			[]string{`{"total":19.5}`},
		},
		{
			// RESIDUAL array element.
			"residual-array-elem",
			`SELECT o.order.orderlines[0].sku AS sku FROM orders o WHERE o.id = "o2"`,
			[]string{`{"sku":"B2"}`},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assertVariantRows(t, sess, c.stmt, c.want)
		})
	}
}
