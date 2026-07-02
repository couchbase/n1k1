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
	"strconv"
	"sync"
	"testing"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/glue"
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

// TestSecondaryIndexComposite: a two-key index (region, product) serves leading-
// key-only, full-key, leading+range, and IN predicates -- each using the index and
// matching the no-index result. The self-delimiting composite key encoding makes
// prefix matching work; the residual filter enforces any part the span prefix
// doesn't pin precisely.
func TestSecondaryIndexComposite(t *testing.T) {
	docs := map[string]string{}
	// regions US(3),EU(2),AS(1) x products a,b,c
	i := 0
	for _, region := range []string{"US", "US", "US", "EU", "EU", "AS"} {
		for _, prod := range []string{"a", "b", "c"} {
			docs[fmtID(i)] = `{"id":"` + fmtID(i) + `","region":"` + region + `","product":"` + prod + `"}`
			i++
		}
	}
	catalog := `{ "indexes": [
	  { "name": "ix_region_product", "keyspace": "sales", "keys": ["region","product"] }
	] }`
	root := writeKeyspaceDocs(t, "sales", docs, catalog)

	cases := []struct {
		stmt string
		want func(id, region, prod string) bool
	}{
		{`SELECT s.id AS id FROM sales s WHERE s.region = "US"`,
			func(_, r, _ string) bool { return r == "US" }},
		{`SELECT s.id AS id FROM sales s WHERE s.region = "US" AND s.product = "b"`,
			func(_, r, p string) bool { return r == "US" && p == "b" }},
		{`SELECT s.id AS id FROM sales s WHERE s.region = "EU" AND s.product >= "b"`,
			func(_, r, p string) bool { return r == "EU" && p >= "b" }},
		{`SELECT s.id AS id FROM sales s WHERE s.region IN ["EU","AS"]`,
			func(_, r, _ string) bool { return r == "EU" || r == "AS" }},
	}
	for _, tc := range cases {
		store, conv := flatRootConv(t, root, tc.stmt)
		if !hasKind(conv.TopOp, "datastore-scan-index") {
			t.Errorf("%q: expected IndexScan, got %v", tc.stmt, opKinds(conv.TopOp))
		}
		var want []string
		i := 0
		for _, region := range []string{"US", "US", "US", "EU", "EU", "AS"} {
			for _, prod := range []string{"a", "b", "c"} {
				id := fmtID(i)
				if tc.want(id, region, prod) {
					want = append(want, `{"id":"`+id+`"}`)
				}
				i++
			}
		}
		sort.Strings(want)
		got := idJSONs(flatRootRows(t, conv, testGlueExec(t, false, store, conv)))
		if !equalStrs(got, want) {
			t.Errorf("%q: want %v, got %v", tc.stmt, want, got)
		}
	}
}

func fmtID(i int) string { return "s" + strconv.Itoa(i) }

// writeKeyspaceDocs builds <root>/default/<keyspace>/<key>.json for each doc plus
// a .n1k1/catalog.json, returning the root.
func writeKeyspaceDocs(t *testing.T, keyspace string, docs map[string]string, catalog string) string {
	t.Helper()
	root := t.TempDir()
	ksDir := filepath.Join(root, "default", keyspace)
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

// TestSecondaryIndexModeOff: with glue.SecondaryIndexMode == "off" the catalog is
// ignored -- no IndexScan is planned -- but results are still correct (primary scan).
func TestSecondaryIndexModeOff(t *testing.T) {
	defer func(prev string) { glue.SecondaryIndexMode = prev }(glue.SecondaryIndexMode)
	glue.SecondaryIndexMode = "off"

	root := writeIndexedKeyspace(t, siDocs, siCatalog)
	stmt := `SELECT c.id AS id FROM default:customer c WHERE c.country = "US"`
	store, conv := flatRootConv(t, root, stmt)
	if hasKind(conv.TopOp, "datastore-scan-index") {
		t.Fatalf("-index=off: should not plan an IndexScan, got %v", opKinds(conv.TopOp))
	}
	rows := flatRootRows(t, conv, testGlueExec(t, false, store, conv))
	if got, want := idJSONs(rows), wantIDJSONs([]string{"c1", "c3", "c5"}); !equalStrs(got, want) {
		t.Fatalf("-index=off query: want %v, got %v", want, got)
	}
	if infos := glue.SecondaryIndexInfos(store.Datastore); infos != nil {
		t.Fatalf("-index=off: expected no index infos, got %v", infos)
	}
}

// TestSecondaryIndexInfos: .indexes introspection reports each declared index,
// built, with the right key count.
func TestSecondaryIndexInfos(t *testing.T) {
	root := writeIndexedKeyspace(t, siDocs, siCatalog)
	store, _ := flatRootConv(t, root, `SELECT 1`) // opens the (wrapped) store
	infos := glue.SecondaryIndexInfos(store.Datastore)
	if len(infos) != 2 {
		t.Fatalf("want 2 index infos, got %d (%+v)", len(infos), infos)
	}
	byName := map[string]glue.IndexInfo{}
	for _, ix := range infos {
		byName[ix.Name] = ix
	}
	for _, name := range []string{"ix_country", "ix_age"} {
		ix, ok := byName[name]
		if !ok {
			t.Fatalf("missing index %q in %+v", name, infos)
		}
		if !ix.Built || ix.Err != "" {
			t.Errorf("%s: want built, got Built=%v Err=%q", name, ix.Built, ix.Err)
		}
		if ix.Entries != len(siDocs) {
			t.Errorf("%s: want %d entries, got %d", name, len(siDocs), ix.Entries)
		}
	}
}

// TestSecondaryIndexEagerConcurrent: EagerBuildSecondaryIndexes builds several
// indexes concurrently, emits a start+done event per index, and the built indexes
// are then usable. Run under -race to check the concurrent open/build path.
func TestSecondaryIndexEagerConcurrent(t *testing.T) {
	catalog := `{ "indexes": [
	  { "name": "ix_country", "keyspace": "customer", "keys": ["country"] },
	  { "name": "ix_age", "keyspace": "customer", "keys": ["age"] }
	] }`
	root := writeIndexedKeyspace(t, siDocs, catalog)
	store, _ := flatRootConv(t, root, `SELECT 1`)

	var mu sync.Mutex
	starts, dones := map[string]bool{}, map[string]bool{}
	err := glue.EagerBuildSecondaryIndexes(store.Datastore, func(ev glue.IndexBuildEvent) {
		mu.Lock()
		defer mu.Unlock()
		switch ev.Phase {
		case "start":
			starts[ev.Name] = true
		case "done":
			dones[ev.Name] = true
		case "error":
			t.Errorf("index %s build error: %v", ev.Name, ev.Err)
		}
	})
	if err != nil {
		t.Fatalf("eager build: %v", err)
	}
	for _, name := range []string{"ix_country", "ix_age"} {
		if !starts[name] || !dones[name] {
			t.Errorf("%s: start=%v done=%v (want both)", name, starts[name], dones[name])
		}
	}
	// Both usable afterward.
	infos := glue.SecondaryIndexInfos(store.Datastore)
	if len(infos) != 2 {
		t.Fatalf("want 2 infos, got %d", len(infos))
	}
	for _, ix := range infos {
		if !ix.Built || ix.Entries != len(siDocs) {
			t.Errorf("%s: Built=%v Entries=%d", ix.Name, ix.Built, ix.Entries)
		}
	}
}

// TestSecondaryIndexReindex: RebuildSecondaryIndexes force-rebuilds (all, or one by
// name) and the index stays usable/correct afterward -- the .reindex escape hatch.
func TestSecondaryIndexReindex(t *testing.T) {
	root := writeIndexedKeyspace(t, siDocs, siCatalog)
	store, _ := flatRootConv(t, root, `SELECT 1`)

	for _, only := range []string{"", "ix_age"} {
		var dones int
		err := glue.RebuildSecondaryIndexes(store.Datastore, only, func(ev glue.IndexBuildEvent) {
			if ev.Phase == "error" {
				t.Errorf("reindex %q error: %v", ev.Name, ev.Err)
			}
			if ev.Phase == "done" {
				dones++
			}
		})
		if err != nil {
			t.Fatalf("reindex only=%q: %v", only, err)
		}
		want := 2 // all
		if only != "" {
			want = 1
		}
		if dones != want {
			t.Errorf("reindex only=%q: want %d done events, got %d", only, want, dones)
		}
	}
	// Still correct after the rebuilds.
	stmt := `SELECT c.id AS id FROM default:customer c WHERE c.age = 45`
	store2, conv := flatRootConv(t, root, stmt)
	rows := flatRootRows(t, conv, testGlueExec(t, false, store2, conv))
	if got := idJSONs(rows); !equalStrs(got, wantIDJSONs([]string{"c2"})) {
		t.Fatalf("after reindex: want [c2], got %v", got)
	}
}

// TestSecondaryIndexSuggest: the .index suggest advisor proposes selective scalar
// / nested-no-array fields and skips low-cardinality, array, and object fields.
func TestSecondaryIndexSuggest(t *testing.T) {
	docs := map[string]string{}
	for i := 0; i < 12; i++ {
		id := fmtID(i)
		status := "a"
		if i%2 == 0 {
			status = "b"
		}
		// id: unique (high card) -> suggest; status: 2 values (low) -> skip;
		// tags: array -> skip; profile: object -> skip; profile.city: nested unique -> suggest.
		docs[id] = `{"id":"` + id + `","status":"` + status + `","tags":["x","y"],` +
			`"profile":{"city":"city` + strconv.Itoa(i) + `"}}`
	}
	root := writeKeyspaceDocs(t, "ks", docs, `{"indexes":[]}`)
	store, _ := flatRootConv(t, root, `SELECT 1`)

	sugg, _, err := glue.SuggestIndexes(store, "default", "ks", 0)
	if err != nil {
		t.Fatalf("SuggestIndexes: %v", err)
	}
	got := map[string]bool{}
	for _, s := range sugg {
		got[s.Field] = true
	}
	for _, want := range []string{"id", "profile.city"} {
		if !got[want] {
			t.Errorf("expected suggestion for %q; got fields %v", want, keysOf(got))
		}
	}
	for _, no := range []string{"status", "tags", "profile"} {
		if got[no] {
			t.Errorf("did not expect suggestion for %q (low-card/array/object)", no)
		}
	}
}

func keysOf(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// TestSecondaryIndexCreate: glue.CatalogAddIndexes (the .index create backend)
// writes defs into catalog.json (array + single-def forms), rejects duplicate
// names, and the created index is usable after re-open.
func TestSecondaryIndexCreate(t *testing.T) {
	root := writeIndexedKeyspace(t, siDocs, `{"indexes":[]}`) // start with no indexes

	added, err := glue.CatalogAddIndexes(root,
		[]byte(`{"indexes":[{"name":"ix_country","keyspace":"customer","keys":["country"]}]}`))
	if err != nil {
		t.Fatalf("add ix_country: %v", err)
	}
	if len(added) != 1 || added[0] != "ix_country" {
		t.Fatalf("added = %v, want [ix_country]", added)
	}

	// Duplicate name is rejected (no clobber).
	if _, err := glue.CatalogAddIndexes(root,
		[]byte(`{"name":"ix_country","keyspace":"customer","keys":["country"]}`)); err == nil {
		t.Fatalf("expected duplicate-name error")
	}

	// Single-def form adds another.
	if _, err := glue.CatalogAddIndexes(root,
		[]byte(`{"name":"ix_age","keyspace":"customer","keys":["age"]}`)); err != nil {
		t.Fatalf("add ix_age: %v", err)
	}

	// The created index is usable after (re-)opening the datastore.
	store, conv := flatRootConv(t, root, `SELECT c.id AS id FROM default:customer c WHERE c.country = "US"`)
	if !hasKind(conv.TopOp, "datastore-scan-index") {
		t.Errorf("created index not used: %v", opKinds(conv.TopOp))
	}
	rows := flatRootRows(t, conv, testGlueExec(t, false, store, conv))
	if got, want := idJSONs(rows), wantIDJSONs([]string{"c1", "c3", "c5"}); !equalStrs(got, want) {
		t.Fatalf("after create: want %v, got %v", want, got)
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
