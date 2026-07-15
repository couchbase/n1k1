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
// `order` VARIANT column (built from the ordersJSON strings).
func writeVariantParquet(t *testing.T, path string, ids, ordersJSON []string) {
	t.Helper()
	mem := memory.DefaultAllocator
	vt := extensions.NewDefaultVariantType()

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
			got := runRowsCanon(t, sess, c.stmt)
			want := make([]string, len(c.want))
			for i, w := range c.want {
				want[i] = canonRow(w)
			}
			sort.Strings(want)
			if len(got) != len(want) {
				t.Fatalf("%s: got %d rows %v, want %d %v", c.stmt, len(got), got, len(want), want)
			}
			for i := range got {
				if got[i] != want[i] {
					t.Errorf("%s: row %d = %s, want %s", c.stmt, i, got[i], want[i])
				}
			}
		})
	}
}
