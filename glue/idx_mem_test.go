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

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeMemFixture lays out a classic <root>/default/beers/<k>.json datastore plus
// a .n1k1/catalog.json declaring a gsi index on `abv`, and returns the root.
func writeMemFixture(t *testing.T, indexKeys string) string {
	t.Helper()
	root := t.TempDir()
	ksDir := filepath.Join(root, "default", "beers")
	if err := os.MkdirAll(ksDir, 0o755); err != nil {
		t.Fatal(err)
	}
	docs := []string{
		`{"name":"low","abv":4.0,"style":"Lager"}`,
		`{"name":"mid","abv":5.5,"style":"Pale"}`,
		`{"name":"ipa1","abv":7.0,"style":"IPA"}`,
		`{"name":"ipa2","abv":8.2,"style":"IPA"}`,
		`{"name":"big","abv":10.0,"style":"Imperial"}`,
	}
	for i, d := range docs {
		if err := os.WriteFile(filepath.Join(ksDir, fmt.Sprintf("b%d.json", i)), []byte(d), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	catDir := filepath.Join(root, sidecarDir)
	if err := os.MkdirAll(catDir, 0o755); err != nil {
		t.Fatal(err)
	}
	cat := fmt.Sprintf(`{"indexes":[{"name":"beers_by_abv","keyspace":"beers","keys":[%s]}]}`, indexKeys)
	if err := os.WriteFile(filepath.Join(catDir, "catalog.json"), []byte(cat), 0o644); err != nil {
		t.Fatal(err)
	}
	return root
}

// withMemMode flips SecondaryIndexMode to "mem" for the duration of the test.
func withMemMode(t *testing.T) {
	t.Helper()
	prev := SecondaryIndexMode
	SecondaryIndexMode = "mem"
	t.Cleanup(func() { SecondaryIndexMode = prev })
}

func TestMemIndexPlansIndexScan(t *testing.T) {
	withMemMode(t)
	root := writeMemFixture(t, `"abv"`)

	sess, err := OpenSession(root, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}

	// EXPLAIN should pick the mem index, not a primary scan.
	res, err := sess.Run("EXPLAIN SELECT b.name FROM beers b WHERE b.abv >= 7")
	if err != nil {
		t.Fatalf("EXPLAIN: %v", err)
	}
	plan := string(res.Rows[0])
	if !strings.Contains(plan, "beers_by_abv") {
		t.Fatalf("mem index not chosen; plan did not mention beers_by_abv:\n%s", plan)
	}
	if !strings.Contains(plan, "IndexScan") {
		t.Fatalf("expected an IndexScan operator in plan:\n%s", plan)
	}
}

func TestMemIndexRangeScanResults(t *testing.T) {
	withMemMode(t)
	root := writeMemFixture(t, `"abv"`)
	sess, err := OpenSession(root, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}

	cases := []struct {
		sql  string
		want int
	}{
		{"SELECT b.name FROM beers b WHERE b.abv >= 7", 3},   // 7.0, 8.2, 10.0
		{"SELECT b.name FROM beers b WHERE b.abv > 7", 2},    // exclusive low
		{"SELECT b.name FROM beers b WHERE b.abv < 6", 2},    // 4.0, 5.5
		{"SELECT b.name FROM beers b WHERE b.abv = 8.2", 1},  // point lookup
		{"SELECT b.name FROM beers b WHERE b.abv BETWEEN 5 AND 8", 2}, // 5.5, 7.0
	}
	for _, c := range cases {
		res, err := sess.Run(c.sql)
		if err != nil {
			t.Fatalf("%s: %v", c.sql, err)
		}
		if len(res.Rows) != c.want {
			t.Errorf("%s: got %d rows, want %d", c.sql, len(res.Rows), c.want)
		}
	}
}

// The mem index must return the SAME rows a plain (no-index) primary scan would,
// so an index is a pure optimization. Compares row counts across modes.
func TestMemIndexMatchesPrimaryScan(t *testing.T) {
	root := writeMemFixture(t, `"abv"`)
	const q = "SELECT b.name FROM beers b WHERE b.abv >= 7 ORDER BY b.name"

	// Primary-scan baseline (no secondary indexes).
	prev := SecondaryIndexMode
	SecondaryIndexMode = "off"
	baseSess, err := OpenSession(root, "default")
	if err != nil {
		t.Fatal(err)
	}
	base, err := baseSess.Run(q)
	if err != nil {
		t.Fatal(err)
	}

	// Mem index.
	SecondaryIndexMode = "mem"
	t.Cleanup(func() { SecondaryIndexMode = prev })
	memSess, err := OpenSession(root, "default")
	if err != nil {
		t.Fatal(err)
	}
	got, err := memSess.Run(q)
	if err != nil {
		t.Fatal(err)
	}

	if len(got.Rows) != len(base.Rows) {
		t.Fatalf("mem rows %d != primary rows %d", len(got.Rows), len(base.Rows))
	}
	for i := range base.Rows {
		if string(got.Rows[i]) != string(base.Rows[i]) {
			t.Errorf("row %d differs: mem=%s primary=%s", i, got.Rows[i], base.Rows[i])
		}
	}
}
