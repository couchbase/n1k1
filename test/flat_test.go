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
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/glue"
)

// jsonOf marshals a row object to canonical JSON (map keys sorted), so numeric
// type differences (int vs float64) don't matter for comparisons.
func jsonOf(v interface{}) string { b, _ := json.Marshal(v); return string(b) }

// writeFlatRoot creates a flat root -- a directory holding record files directly
// (no namespace/keyspace subdirs) -- and returns its path + base name. The dir is
// given a stable, alphabetic base name so `FROM <base>` parses as an identifier
// (a numeric t.TempDir() base like "001" would need backticks).
func writeFlatRoot(t *testing.T, files map[string]string) (dir, base string) {
	t.Helper()
	base = "flatds"
	dir = filepath.Join(t.TempDir(), base)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir, base
}

// flatRootConv opens a FileStore at dir (which should trigger the flat-root
// "fake it" wrapper) and parses/plans/converts stmt.
func flatRootConv(t *testing.T, dir, stmt string) (*glue.Store, *glue.Conv) {
	t.Helper()
	store, err := glue.FileStore(dir)
	if err != nil {
		t.Fatalf("FileStore: %v", err)
	}
	if err := store.InitParser(); err != nil {
		t.Fatalf("InitParser: %v", err)
	}
	s, err := glue.ParseStatement(stmt, "", true)
	if err != nil {
		t.Fatalf("parse %q: %v", stmt, err)
	}
	p, err := store.PlanStatement(s, "", nil, nil)
	if err != nil {
		t.Fatalf("plan %q: %v", stmt, err)
	}
	conv := &glue.Conv{Temps: []interface{}{nil}}
	if _, err := p.Accept(conv); err != nil {
		t.Fatalf("accept %q: %v", stmt, err)
	}
	return store, conv
}

// TestFlatRootFakesMetadata: a flat root is advertised as default:<basename>.
func TestFlatRootFakesMetadata(t *testing.T) {
	dir, base := writeFlatRoot(t, map[string]string{
		"a.json": `{"n":1}`, "b.json": `{"n":2}`,
	})
	store, err := glue.FileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	ds := store.Datastore
	if names, _ := ds.NamespaceNames(); len(names) != 1 || names[0] != "default" {
		t.Fatalf("flat root should fake one 'default' namespace, got %v", names)
	}
	ns, nerr := ds.NamespaceByName("default")
	if nerr != nil {
		t.Fatalf("NamespaceByName default: %v", nerr)
	}
	ks, kerr := ns.KeyspaceByName(base)
	if kerr != nil {
		t.Fatalf("KeyspaceByName %q: %v", base, kerr)
	}
	// The synthetic keyspace resolves its data to the root dir itself.
	rd, ok := ks.(interface{ RecordsDir() string })
	if !ok || rd.RecordsDir() != dir {
		t.Fatalf("RecordsDir want %q, got ok=%v val=%q", dir, ok, func() string {
			if ok {
				return rd.RecordsDir()
			}
			return ""
		}())
	}
	// A primary index must be advertised so the planner can scan.
	ixer, ierr := ks.Indexer("gsi")
	if ierr != nil {
		t.Fatalf("Indexer: %v", ierr)
	}
	if prims, perr := ixer.PrimaryIndexes(); perr != nil || len(prims) != 1 {
		t.Fatalf("want 1 primary index, got %v err %v", prims, perr)
	}
}

// flatRootRows assembles the raw engine rows into result objects via the plan's
// projection labels (the same conversion the CLI applies).
func flatRootRows(t *testing.T, conv *glue.Conv, results []base.Vals) []interface{} {
	t.Helper()
	cv, err := glue.NewConvertVals(conv.TopOp.Labels)
	if err != nil {
		t.Fatalf("NewConvertVals: %v", err)
	}
	out := make([]interface{}, 0, len(results))
	for _, r := range results {
		v, e := cv.Convert(r)
		if e != nil {
			t.Fatalf("Convert: %v", e)
		}
		out = append(out, v.Actual())
	}
	return out
}

// TestFlatRootQuery: FROM <basename> reads the flat dir's records.
func TestFlatRootQuery(t *testing.T) {
	dir, base := writeFlatRoot(t, map[string]string{
		"r1.json": `{"n":1,"kind":"m"}`,
		"r2.json": `{"n":2,"kind":"m"}`,
		"r3.json": `{"n":3,"kind":"m"}`,
	})
	for _, stmt := range []string{
		"SELECT x.n AS n FROM default:" + base + " x",
		"SELECT x.n AS n FROM " + base + " x", // unqualified -> default ns
	} {
		store, conv := flatRootConv(t, dir, stmt)
		rows := flatRootRows(t, conv, testGlueExec(t, false, store, conv))
		if len(rows) != 3 {
			t.Fatalf("%q: want 3 rows, got %d", stmt, len(rows))
		}
		got := map[string]bool{}
		for _, row := range rows {
			got[jsonOf(row)] = true
		}
		for _, want := range []string{`{"n":1}`, `{"n":2}`, `{"n":3}`} {
			if !got[want] {
				t.Fatalf("%q: missing row %s; got %v", stmt, want, rows)
			}
		}
	}
}

// TestFlatRootAggregate: aggregates flow through the flat-root records scan.
func TestFlatRootAggregate(t *testing.T) {
	dir, base := writeFlatRoot(t, map[string]string{
		"r1.json": `{"amt":10}`, "r2.json": `{"amt":20}`, "r3.json": `{"amt":30}`,
	})
	store, conv := flatRootConv(t, dir,
		"SELECT SUM(x.amt) AS tot, COUNT(*) AS n FROM "+base+" x")
	rows := flatRootRows(t, conv, testGlueExec(t, false, store, conv))
	if len(rows) != 1 {
		t.Fatalf("want 1 aggregate row, got %d (%v)", len(rows), rows)
	}
	if got := jsonOf(rows[0]); got != `{"n":3,"tot":60}` {
		t.Fatalf("want {\"n\":3,\"tot\":60}, got %s", got)
	}
}

// TestFlatRootNotTriggeredForNormalLayout: a root WITH namespace subdirs is the
// normal layout and must NOT get a synthetic basename keyspace.
func TestFlatRootNotTriggeredForNormalLayout(t *testing.T) {
	root := t.TempDir()
	ksDir := filepath.Join(root, "default", "widgets")
	if err := os.MkdirAll(ksDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ksDir, "w1.json"), []byte(`{"id":"w1"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	store, err := glue.FileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	ns, nerr := store.Datastore.NamespaceByName("default")
	if nerr != nil {
		t.Fatalf("NamespaceByName default: %v", nerr)
	}
	// The real keyspace resolves...
	if _, kerr := ns.KeyspaceByName("widgets"); kerr != nil {
		t.Fatalf("normal keyspace widgets should resolve: %v", kerr)
	}
	// ...but the root basename must NOT be a synthesized keyspace.
	if ks, _ := ns.KeyspaceByName(filepath.Base(root)); ks != nil {
		if _, ok := ks.(interface{ RecordsDir() string }); ok {
			t.Fatalf("normal layout must not synthesize a flat-root keyspace")
		}
	}
}
