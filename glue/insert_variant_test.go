//go:build n1ql && !js

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

// Write-back for DESIGN-variant.md §7 Phase 2: `INSERT INTO <x>.parquet SELECT ...` where
// a projected column holds nested objects writes that column as a Parquet VARIANT logical
// type (glue/insert_writer.go). This closes the loop with the Phase-0 read path: a VARIANT
// column read as JSON, re-inserted, comes back queryable — nested navigation and all.

import (
	"os"
	"path/filepath"
	"testing"
)

// insertVariantSrcDir seeds a `src` keyspace whose docs carry an object-valued `order`
// field (plus a scalar id), so a SELECT projecting `order` produces object values — the
// shape that now infers a VARIANT column on INSERT INTO a .parquet target.
func insertVariantSrcDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	d := filepath.Join(dir, "default", "src")
	if err := os.MkdirAll(d, 0o755); err != nil {
		t.Fatal(err)
	}
	body := `{"id":"o1","order":{"customer":{"name":"Ada","address":{"city":"London"}},"total":9.99,"lines":[{"sku":"A1","qty":2}]}}` + "\n" +
		`{"id":"o2","order":{"customer":{"name":"Bo","address":{"city":"Paris"}},"total":19.5,"lines":[{"sku":"B2","qty":1}]}}` + "\n" +
		`{"id":"o3"}` + "\n" // o3 has NO order field -> the VARIANT column cell is NULL
	if err := os.WriteFile(filepath.Join(d, "src.jsonl"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

// TestInsertVariantColumnRoundTrip drives the full write→read loop: INSERT a nested-object
// column into a Parquet keyspace (inferring a VARIANT column), then a fresh session reads
// the file back and queries through the VARIANT — proving the write path emits a genuine,
// readable Parquet VARIANT column.
func TestInsertVariantColumnRoundTrip(t *testing.T) {
	dir := insertVariantSrcDir(t)
	sess, err := OpenSession(dir, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}

	// Project id (string col) + order (object -> VARIANT col) into a new Parquet file.
	// VALUE self = the whole projected row; ORDER BY id guarantees the object-bearing
	// row (o1) is first so the schema infers the `order` VARIANT column. `order` is a
	// reserved word, hence the backticks.
	res, err := sess.Run("INSERT INTO `out/orders.parquet` (KEY UUID(), VALUE self) " +
		"SELECT s.id, s.`order` FROM src s ORDER BY s.id")
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if res.Count != 3 {
		t.Fatalf("inserted %d rows, want 3", res.Count)
	}

	// The file exists on disk.
	if _, err := os.Stat(filepath.Join(dir, "default", "out", "orders.parquet")); err != nil {
		t.Fatalf("expected orders.parquet on disk: %v", err)
	}

	// A fresh session reads the `out/orders` Parquet keyspace back and navigates the
	// VARIANT column just like any JSON object (Phase-0 read).
	sess2, err := OpenSession(dir, "default")
	if err != nil {
		t.Fatalf("OpenSession 2: %v", err)
	}

	cases := []struct {
		name string
		stmt string
		want []string
	}{
		{
			// Nested navigation through the written-back VARIANT column.
			"nested-name",
			"SELECT o.id, o.`order`.customer.name AS name FROM `out` o WHERE o.`order` IS VALUED",
			[]string{`{"id":"o1","name":"Ada"}`, `{"id":"o2","name":"Bo"}`},
		},
		{
			// A 3-deep residual filter + a Decimal16-derived numeric field survive the round-trip.
			"deep-filter",
			"SELECT o.`order`.total AS total FROM `out` o WHERE o.`order`.customer.address.city = \"Paris\"",
			[]string{`{"total":19.5}`},
		},
		{
			// Array element inside the VARIANT column.
			"array-elem",
			"SELECT o.`order`.lines[0].sku AS sku FROM `out` o WHERE o.id = \"o1\"",
			[]string{`{"sku":"A1"}`},
		},
		{
			// The row with no `order` field wrote a NULL VARIANT cell -> reads back MISSING/null.
			"null-row",
			"SELECT o.id FROM `out` o WHERE o.`order` IS MISSING OR o.`order` IS NULL",
			[]string{`{"id":"o3"}`},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assertVariantRows(t, sess2, c.stmt, c.want)
		})
	}
}
