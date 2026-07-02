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

package test

import (
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/couchbase/n1k1/base"
)

// writeIndexedKeyspace builds a classic <root>/<ns>/<keyspace>/<key>.json layout
// (one JSON doc per file) plus a .n1k1/catalog.json declaring secondary indexes,
// and returns the root dir. It's the scenario the Phase-1 GSI-like secondary
// index targets (see DESIGN-indexing.md).
func writeIndexedKeyspace(t *testing.T, docs map[string]string, catalog string) string {
	t.Helper()
	root := t.TempDir()
	ksDir := filepath.Join(root, "default", "customer")
	if err := os.MkdirAll(ksDir, 0o755); err != nil {
		t.Fatal(err)
	}
	for key, body := range docs {
		if err := os.WriteFile(filepath.Join(ksDir, key+".json"), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	sc := filepath.Join(root, ".n1k1")
	if err := os.MkdirAll(sc, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sc, "catalog.json"), []byte(catalog), 0o644); err != nil {
		t.Fatal(err)
	}
	return root
}

var siDocs = map[string]string{
	"c1": `{"id":"c1","name":"Alice","country":"US","age":30}`,
	"c2": `{"id":"c2","name":"Bob","country":"UK","age":45}`,
	"c3": `{"id":"c3","name":"Carol","country":"US","age":25}`,
	"c4": `{"id":"c4","name":"Dave","country":"FR","age":52}`,
	"c5": `{"id":"c5","name":"Eve","country":"US","age":38}`,
}

const siCatalog = `{
  "indexes": [
    { "name": "ix_country", "keyspace": "customer", "keys": ["country"] },
    { "name": "ix_age", "keyspace": "customer", "keys": ["age"] }
  ]
}`

// opKinds collects the Kind of every op in a converted tree (depth-first).
func opKinds(op *base.Op) []string {
	if op == nil {
		return nil
	}
	kinds := []string{op.Kind}
	for _, ch := range op.Children {
		kinds = append(kinds, opKinds(ch)...)
	}
	return kinds
}

func hasKind(op *base.Op, kind string) bool {
	for _, k := range opKinds(op) {
		if k == kind {
			return true
		}
	}
	return false
}

// idJSONs renders each result row (from a `SELECT c.id AS id ...` query) as JSON
// and returns them sorted, so a test can compare against expected `{"id":"cN"}`
// objects without unwrapping value.Value field types.
func idJSONs(rows []interface{}) []string {
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		out = append(out, jsonOf(r))
	}
	sort.Strings(out)
	return out
}

// wantIDJSONs turns id strings into the sorted `{"id":"cN"}` JSON forms.
func wantIDJSONs(ids []string) []string {
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		out = append(out, `{"id":"`+id+`"}`)
	}
	sort.Strings(out)
	return out
}

// TestSecondaryIndexUsedAndCorrect: a WHERE on an indexed field plans an
// IndexScan (not a full records scan), and returns the right rows.
func TestSecondaryIndexUsedAndCorrect(t *testing.T) {
	root := writeIndexedKeyspace(t, siDocs, siCatalog)

	// Project the doc's own `id` field (each doc has one equal to its key) rather
	// than META().id -- the lower-level flatRootRows harness doesn't materialize
	// META().id, but the doc field exercises the same scan+fetch path.
	cases := []struct {
		stmt    string
		wantIDs []string
	}{
		{`SELECT c.id AS id FROM default:customer c WHERE c.country = "US"`, []string{"c1", "c3", "c5"}},
		{`SELECT c.id AS id FROM default:customer c WHERE c.age = 45`, []string{"c2"}},
		{`SELECT c.id AS id FROM default:customer c WHERE c.age >= 38`, []string{"c2", "c4", "c5"}},
		{`SELECT c.id AS id FROM default:customer c WHERE c.age > 30 AND c.age < 52`, []string{"c2", "c5"}},
		{`SELECT c.id AS id FROM default:customer c WHERE c.age IN [25, 45, 52]`, []string{"c2", "c3", "c4"}},
		{`SELECT c.id AS id FROM default:customer c WHERE c.country IN ["UK", "FR"]`, []string{"c2", "c4"}},
	}

	for _, tc := range cases {
		store, conv := flatRootConv(t, root, tc.stmt)
		if !hasKind(conv.TopOp, "datastore-scan-index") {
			t.Errorf("%q: expected an IndexScan (datastore-scan-index), got kinds %v",
				tc.stmt, opKinds(conv.TopOp))
		}
		if hasKind(conv.TopOp, "datastore-scan-records") {
			t.Errorf("%q: unexpected full records scan in plan %v", tc.stmt, opKinds(conv.TopOp))
		}
		rows := flatRootRows(t, conv, testGlueExec(t, false, store, conv))
		got := idJSONs(rows)
		if !equalStrs(got, wantIDJSONs(tc.wantIDs)) {
			t.Errorf("%q: want ids %v, got %v", tc.stmt, tc.wantIDs, got)
		}
	}
}

// TestSecondaryIndexPartialWhere: a partial-index `where` only indexes matching
// docs; a query the condition covers still returns correct rows.
func TestSecondaryIndexPartialWhere(t *testing.T) {
	catalog := `{ "indexes": [
	  { "name": "ix_us_age", "keyspace": "customer", "keys": ["age"], "where": "country = \"US\"" }
	] }`
	root := writeIndexedKeyspace(t, siDocs, catalog)
	stmt := `SELECT c.id AS id FROM default:customer c WHERE c.country = "US" AND c.age >= 30`
	store, conv := flatRootConv(t, root, stmt)
	if !hasKind(conv.TopOp, "datastore-scan-index") {
		t.Fatalf("expected IndexScan, got %v", opKinds(conv.TopOp))
	}
	rows := flatRootRows(t, conv, testGlueExec(t, false, store, conv))
	if got, want := idJSONs(rows), wantIDJSONs([]string{"c1", "c5"}); !equalStrs(got, want) {
		t.Fatalf("partial-index query: want %v, got %v", want, got)
	}
}

// TestNoCatalogNoIndex: without a .n1k1/catalog.json, nothing changes -- the same
// query does a full records scan (no IndexScan), still correct.
func TestNoCatalogNoIndex(t *testing.T) {
	root := t.TempDir()
	ksDir := filepath.Join(root, "default", "customer")
	if err := os.MkdirAll(ksDir, 0o755); err != nil {
		t.Fatal(err)
	}
	for key, body := range siDocs {
		if err := os.WriteFile(filepath.Join(ksDir, key+".json"), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	stmt := `SELECT c.id AS id FROM default:customer c WHERE c.country = "US"`
	store, conv := flatRootConv(t, root, stmt)
	if hasKind(conv.TopOp, "datastore-scan-index") {
		t.Fatalf("no catalog: should not plan an IndexScan, got %v", opKinds(conv.TopOp))
	}
	rows := flatRootRows(t, conv, testGlueExec(t, false, store, conv))
	if got, want := idJSONs(rows), wantIDJSONs([]string{"c1", "c3", "c5"}); !equalStrs(got, want) {
		t.Fatalf("no-catalog query: want %v, got %v", want, got)
	}
}

func equalStrs(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
