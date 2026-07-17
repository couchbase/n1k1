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

// Differential test for the Phase-1 VARIANT fidelity scan mode (DESIGN-variant.md §4.1
// / §4.2 substep 2c): a VARIANT-bearing query must return IDENTICAL results whether the
// scan emits Phase-0 JSON rows or whole-row `V`-carrier objects. This pins "query
// behavior provably unchanged" while the carrier flows through navigation
// (ValPathGet -> variant nav) and output (Convert -> project V to JSON). The engagement
// (that the scan really emits V, so this can't false-pass on a silent fallback) is
// proven by records.TestParquetReaderEmitsVariantCarrier.

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	av "github.com/apache/arrow-go/v18/parquet/variant"

	"github.com/couchbase/n1k1/records"
)

func TestVariantFidelityDifferential(t *testing.T) {
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

	queries := []string{
		`SELECT * FROM orders o`,                                                   // whole doc
		`SELECT o.id, o.order.customer.name AS name FROM orders o`,                 // nested scalar leaf
		`SELECT o.id FROM orders o WHERE o.order.customer.address.city = "London"`, // 3-deep filter
		`SELECT o.order.total AS total FROM orders o WHERE o.order.total > 10`,     // decimal-derived filter
		`SELECT o.order.orderlines[0].sku AS sku FROM orders o WHERE o.id = "o2"`,  // array-element nav
		`SELECT o.order AS ord FROM orders o`,                                      // whole VARIANT container leaf
		`SELECT o.id FROM orders o WHERE o.order IS VALUED ORDER BY o.id`,          // presence + order
	}

	// Always leave the global knob off for other tests.
	defer func() { records.VariantFidelity = false }()

	for _, q := range queries {
		records.VariantFidelity = false
		sessOff, err := OpenSession(dir, "default")
		if err != nil {
			t.Fatalf("OpenSession (off): %v", err)
		}
		off := runRowsCanon(t, sessOff, q)

		records.VariantFidelity = true
		sessOn, err := OpenSession(dir, "default")
		if err != nil {
			t.Fatalf("OpenSession (on): %v", err)
		}
		on := runRowsCanon(t, sessOn, q)

		records.VariantFidelity = false

		if len(off) != len(on) {
			t.Errorf("%s: row count differs — Phase-0=%d %v, fidelity=%d %v", q, len(off), off, len(on), on)
			continue
		}
		for i := range off {
			if off[i] != on[i] {
				t.Errorf("%s: row %d differs\n Phase-0 : %s\n fidelity: %s", q, i, off[i], on[i])
			}
		}
	}
}

// writeLoneVariantParquet writes a Parquet file whose ONLY column is a VARIANT ("doc"):
// the variant value is the whole document, so n1k1 reads it UNWRAPPED (rows are the
// variant itself, navigated as o.<field>, not o.doc.<field>). See records.loneVariantCol.
func writeLoneVariantParquet(t *testing.T, path string, docsJSON []string) {
	t.Helper()
	mem := memory.DefaultAllocator
	vt := extensions.NewDefaultVariantType()
	vB := extensions.NewVariantBuilder(mem, vt)
	for _, s := range docsJSON {
		v, err := av.ParseJSON(s, false)
		if err != nil {
			t.Fatalf("ParseJSON(%q): %v", s, err)
		}
		vB.Append(v)
	}
	schema := arrow.NewSchema([]arrow.Field{{Name: "doc", Type: vt, Nullable: true}}, nil)
	rec := array.NewRecord(schema, []arrow.Array{vB.NewArray()}, int64(len(docsJSON)))
	tbl := array.NewTableFromRecords(schema, []arrow.Record{rec})
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

// TestVariantLoneColumnUnwrap covers the borrow landing: a lone-VARIANT-column keyspace
// is read UNWRAPPED (the variant IS the document) via the BORROW fast path in fidelity
// mode and a direct JSON render in Phase-0 -- both must agree, and o.<field> navigation
// must work (proving the row really is the variant, not {doc: ...}).
func TestVariantLoneColumnUnwrap(t *testing.T) {
	dir := t.TempDir()
	ksDir := filepath.Join(dir, "default", "docs")
	if err := os.MkdirAll(ksDir, 0o755); err != nil {
		t.Fatal(err)
	}
	writeLoneVariantParquet(t, filepath.Join(ksDir, "docs.parquet"), []string{
		`{"customer":{"name":"Ada","address":{"city":"London"}},"orderlines":[{"sku":"A1","qty":2}],"total":9.99}`,
		`{"customer":{"name":"Bo","address":{"city":"Paris"}},"orderlines":[{"sku":"B2","qty":1}],"total":19.5}`,
	})

	// UNWRAPPED: the variant IS the document -> fields are o.<field>, not o.doc.<field>.
	queries := []string{
		`SELECT o.customer.name AS name FROM docs o ORDER BY o.customer.name`,
		`SELECT o.total AS total FROM docs o WHERE o.total > 10`,          // decimal filter
		`SELECT o.customer.name AS n FROM docs o WHERE o.customer.address.city = "Paris"`,
		`SELECT o.orderlines[0].sku AS sku FROM docs o WHERE o.total > 10`, // array-element nav
		`SELECT COUNT(*) c FROM docs o WHERE o.total > 0`,
	}
	defer func() { records.VariantFidelity = false }()
	for _, q := range queries {
		records.VariantFidelity = false
		sOff, err := OpenSession(dir, "default")
		if err != nil {
			t.Fatal(err)
		}
		off := runRowsCanon(t, sOff, q)

		records.VariantFidelity = true
		sOn, err := OpenSession(dir, "default")
		if err != nil {
			t.Fatal(err)
		}
		on := runRowsCanon(t, sOn, q)
		records.VariantFidelity = false

		if len(off) == 0 {
			t.Errorf("%s: Phase-0 returned 0 rows -- unwrapped o.<field> navigation failed?", q)
			continue
		}
		if len(off) != len(on) {
			t.Errorf("%s: count differs Phase-0=%v fidelity=%v", q, off, on)
			continue
		}
		for i := range off {
			if off[i] != on[i] {
				t.Errorf("%s: row %d differs\n Phase-0 : %s\n fidelity: %s", q, i, off[i], on[i])
			}
		}
	}

	// Concrete value: the decimal filter really navigates into the unwrapped variant.
	records.VariantFidelity = false
	s, err := OpenSession(dir, "default")
	if err != nil {
		t.Fatal(err)
	}
	got := runRowsCanon(t, s, `SELECT o.total AS total FROM docs o WHERE o.total > 10`)
	if len(got) != 1 || got[0] != `{"total":19.5}` {
		t.Errorf(`decimal unwrap nav: got %v, want [{"total":19.5}]`, got)
	}
}
