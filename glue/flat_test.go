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
	"os"
	"path/filepath"
	"testing"
)

func keyspaceSet(t *testing.T, store *Store) map[string]bool {
	t.Helper()
	ns, err := store.Datastore.NamespaceByName("default")
	if err != nil {
		t.Fatalf("default namespace: %v", err)
	}
	names, err := ns.KeyspaceNames()
	if err != nil {
		t.Fatalf("KeyspaceNames: %v", err)
	}
	got := map[string]bool{}
	for _, n := range names {
		got[n] = true
	}
	return got
}

// TestMaybeFlatGrabBag: a directory with subdirs AND loose structured files (the
// ~/Desktop case) exposes one keyspace per structured file, by stem. Extracted
// documents (PDF/DOCX/XLSX) are not auto-exposed.
func TestMaybeFlatGrabBag(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	write := func(name, body string) {
		if err := os.WriteFile(filepath.Join(root, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("people.csv", "id,name\n1,al\n")
	write("orders.jsonl", `{"o":1}`+"\n")
	write("notes.pdf", "%PDF-1.4 not really") // extract doc -> must NOT be a keyspace
	if err := os.WriteFile(filepath.Join(root, "sub", "x.json"), []byte(`{"a":1}`), 0o644); err != nil {
		t.Fatal(err)
	}

	store, err := FileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	got := keyspaceSet(t, store)
	if !got["people"] || !got["orders"] {
		t.Errorf("expected per-file keyspaces people+orders, got %v", got)
	}
	if got["notes"] {
		t.Errorf("extract doc notes.pdf must not become a keyspace, got %v", got)
	}
	ns, _ := store.Datastore.NamespaceByName("default")
	if _, kerr := ns.KeyspaceByName("people"); kerr != nil {
		t.Errorf("KeyspaceByName people: %v", kerr)
	}
}

// TestCatalogFormats: formats round-trip through catalog.json, preserving any
// existing index defs; a blank removes the field.
func TestCatalogFormats(t *testing.T) {
	root := t.TempDir()
	if f, err := CatalogFormats(root); err != nil || f != "" {
		t.Fatalf("no catalog yet: got %q err %v", f, err)
	}
	// Pre-seed a catalog with an index -- CatalogSetFormats must not clobber it.
	sc := filepath.Join(root, sidecarDir)
	if err := os.MkdirAll(sc, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sc, "catalog.json"),
		[]byte(`{"indexes":[{"name":"ix","keyspace":"k","keys":["a"]}]}`), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := CatalogSetFormats(root, "json,csv"); err != nil {
		t.Fatal(err)
	}
	if f, _ := CatalogFormats(root); f != "json,csv" {
		t.Errorf("formats = %q, want json,csv", f)
	}
	if cat, err := loadCatalog(root); err != nil || cat == nil || len(cat.Indexes) != 1 {
		t.Errorf("index def not preserved after set: %+v err %v", cat, err)
	}

	// A blank removes the field (but keeps the catalog/indexes).
	if err := CatalogSetFormats(root, ""); err != nil {
		t.Fatal(err)
	}
	if f, _ := CatalogFormats(root); f != "" {
		t.Errorf("blank should remove formats, got %q", f)
	}
	if cat, _ := loadCatalog(root); cat == nil || len(cat.Indexes) != 1 {
		t.Errorf("index def lost when clearing formats: %+v", cat)
	}
}

func TestSidecarName(t *testing.T) {
	if SidecarName() != ".n1k1" {
		t.Errorf("default SidecarName() = %q, want .n1k1", SidecarName())
	}
	orig := sidecarDir
	defer func() { sidecarDir = orig }()

	SetSidecarName(".foo")
	if SidecarName() != ".foo" {
		t.Errorf("after SetSidecarName(.foo) = %q", SidecarName())
	}
	SetSidecarName("") // blank is ignored (keeps current)
	if SidecarName() != ".foo" {
		t.Errorf("blank should be ignored, got %q", SidecarName())
	}
}

// TestIsFlatDatastore: grab-bag/flat layouts report true; a classic
// <ns>/<keyspace> directory (where secondary indexes are supported) reports false.
func TestIsFlatDatastore(t *testing.T) {
	// grab-bag: a subdir + a loose data file -> flat.
	gb := t.TempDir()
	if err := os.MkdirAll(filepath.Join(gb, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(gb, "orgs.csv"), []byte("id\n1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if st, err := FileStore(gb); err != nil || !IsFlatDatastore(st.Datastore) {
		t.Errorf("grab-bag should be a flat datastore (err %v)", err)
	}

	// classic <ns>/<keyspace> (no catalog) -> not flat.
	cl := t.TempDir()
	if err := os.MkdirAll(filepath.Join(cl, "default", "orders"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cl, "default", "orders", "o.json"), []byte(`{"id":1}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if st, err := FileStore(cl); err != nil || IsFlatDatastore(st.Datastore) {
		t.Errorf("classic layout should not be flat (err %v)", err)
	}
}

// TestMaybeFlatMergesRealDefault: loose root files coexist with a real
// <root>/default/<keyspace> layout -- the synthetic per-file keyspace is added
// without hiding the real keyspaces.
func TestMaybeFlatMergesRealDefault(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "default", "orders"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "default", "orders", "o1.json"), []byte(`{"id":"o1"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "extra.csv"), []byte("a,b\n1,2\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	store, err := FileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	got := keyspaceSet(t, store)
	if !got["orders"] || !got["extra"] {
		t.Errorf("expected merged real 'orders' + synthetic 'extra', got %v", got)
	}
}
