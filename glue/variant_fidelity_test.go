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
