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
	"bytes"
	"compress/gzip"
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

// writeFlatFile creates a single record file (no directory tree) and returns its
// path. The name's stem becomes the keyspace, so callers pick alphabetic stems so
// `FROM <stem>` parses as a bare identifier. See DESIGN-data.md scenario B2.
func writeFlatFile(t *testing.T, name, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// writeFlatFileGz gzip-compresses body to <TempDir>/<name> (name should end .gz).
func writeFlatFileGz(t *testing.T, name, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write([]byte(body)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestFlatFileFakesMetadata: a single file arg is advertised as default:<stem>,
// with the synthetic keyspace pointing RecordsFile at that one file.
func TestFlatFileFakesMetadata(t *testing.T) {
	path := writeFlatFile(t, "events.jsonl", `{"n":1}`+"\n"+`{"n":2}`+"\n")
	store, err := glue.FileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	ds := store.Datastore
	if names, _ := ds.NamespaceNames(); len(names) != 1 || names[0] != "default" {
		t.Fatalf("single file should fake one 'default' namespace, got %v", names)
	}
	ns, nerr := ds.NamespaceByName("default")
	if nerr != nil {
		t.Fatalf("NamespaceByName default: %v", nerr)
	}
	ks, kerr := ns.KeyspaceByName("events") // stem of events.jsonl
	if kerr != nil {
		t.Fatalf("KeyspaceByName events: %v", kerr)
	}
	// The synthetic keyspace resolves its data to the one file (not a dir walk).
	rf, ok := ks.(interface{ RecordsFile() string })
	if !ok || rf.RecordsFile() != path {
		got := ""
		if ok {
			got = rf.RecordsFile()
		}
		t.Fatalf("RecordsFile want %q, got ok=%v val=%q", path, ok, got)
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

// TestFlatFileQuery: FROM <stem> reads every record in the single JSONL file.
func TestFlatFileQuery(t *testing.T) {
	path := writeFlatFile(t, "events.jsonl",
		`{"n":1,"kind":"m"}`+"\n"+`{"n":2,"kind":"m"}`+"\n"+`{"n":3,"kind":"m"}`+"\n")
	for _, stmt := range []string{
		"SELECT x.n AS n FROM default:events x",
		"SELECT x.n AS n FROM events x", // unqualified -> default ns
	} {
		store, conv := flatRootConv(t, path, stmt)
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

// TestFlatFileAggregate: aggregates flow through the single-file records scan.
func TestFlatFileAggregate(t *testing.T) {
	path := writeFlatFile(t, "sales.jsonl",
		`{"amt":10}`+"\n"+`{"amt":20}`+"\n"+`{"amt":30}`+"\n")
	store, conv := flatRootConv(t, path,
		"SELECT SUM(x.amt) AS tot, COUNT(*) AS n FROM sales x")
	rows := flatRootRows(t, conv, testGlueExec(t, false, store, conv))
	if len(rows) != 1 {
		t.Fatalf("want 1 aggregate row, got %d (%v)", len(rows), rows)
	}
	if got := jsonOf(rows[0]); got != `{"n":3,"tot":60}` {
		t.Fatalf("want {\"n\":3,\"tot\":60}, got %s", got)
	}
}

// TODO(covering-meta-scan-container): a COVERING primary scan over a keyspace
// whose directory holds container files (a *.jsonl) returns ZERO rows -- e.g.
// `SELECT meta().id FROM events` (projecting only meta().id, so the primary index
// covers it) or an aliased `SELECT meta(e).id FROM events e`. The covering /
// primary-key path lists the keyspace directory as if it were one-doc-per-file
// (documentPathToId -> file stems), which don't match the container records'
// ids, so nothing resolves. A NON-covering scan (meta().id plus a doc field) is
// fine -- it takes the whole-doc records scan, which assigns correct
// <relpath>#<line>@<offset> ids. Empty, not a hang (the flat-keyspace #primary
// scan that used to *hang* is fixed -- see DatastoreScanIndex/scanContainerKeys).
// Fix likely belongs in the covering/primary path: list container record ids via
// the records source (as scanContainerKeys does) instead of readdir stems.

// TestFlatFileJSONLUseKeysFetch: a key-based fetch (USE KEYS) into a .jsonl
// container resolves each record via the byte offset baked into its META().id
// (<relpath>#<line>@<offset>). Before offset ids, fetch had no way to map a
// positional key back to a doc in a multi-doc file and returned nothing. The
// ids/offsets are fixed by the exact bytes below: each `{...}` line is 15 bytes +
// a newline = 16, so the records start at 0, 16, 32.
func TestFlatFileJSONLUseKeysFetch(t *testing.T) {
	path := writeFlatFile(t, "events.jsonl",
		`{"n":1,"u":"a"}`+"\n"+`{"n":2,"u":"b"}`+"\n"+`{"n":3,"u":"c"}`+"\n")

	// Fetch the 1st and 3rd records by id; the projected fields prove the fetch
	// returned the correct doc for each offset (not a neighbor).
	stmt := `SELECT x.n AS n, x.u AS u FROM events x ` +
		`USE KEYS ["events.jsonl#0@0", "events.jsonl#2@32"]`
	store, conv := flatRootConv(t, path, stmt)
	rows := flatRootRows(t, conv, testGlueExec(t, false, store, conv))
	if len(rows) != 2 {
		t.Fatalf("want 2 fetched rows, got %d (%v)", len(rows), rows)
	}
	got := map[string]bool{}
	for _, row := range rows {
		got[jsonOf(row)] = true
	}
	for _, want := range []string{`{"n":1,"u":"a"}`, `{"n":3,"u":"c"}`} {
		if !got[want] {
			t.Fatalf("missing fetched row %s; got %v", want, rows)
		}
	}
}

// TestFlatFileJSONLOnKeysJoinRoundTrip: an ON KEYS self-join over a .jsonl
// container -- the inner side is a full #primary IndexScan (yielding record ids)
// feeding a Fetch. Both used to hang: the flat keyspace's virtual primary index
// can't be scanned by cbq's IndexConnection (its Scan never feeds the sender, so
// the drain deadlocks). DatastoreScanIndex now yields the ids from the records
// source (scanContainerKeys), and the Fetch resolves each via its byte offset --
// so every record round-trips to itself.
func TestFlatFileJSONLOnKeysJoinRoundTrip(t *testing.T) {
	path := writeFlatFile(t, "events.jsonl",
		`{"n":1,"u":"a"}`+"\n"+`{"n":2,"u":"b"}`+"\n"+`{"n":3,"u":"c"}`+"\n")
	stmt := "SELECT e2.n AS n, e2.u AS u FROM events e1 JOIN events e2 ON KEYS META(e1).id"
	store, conv := flatRootConv(t, path, stmt)
	rows := flatRootRows(t, conv, testGlueExec(t, false, store, conv))
	if len(rows) != 3 {
		t.Fatalf("want 3 round-tripped rows, got %d (%v)", len(rows), rows)
	}
	got := map[string]bool{}
	for _, row := range rows {
		got[jsonOf(row)] = true
	}
	for _, want := range []string{`{"n":1,"u":"a"}`, `{"n":2,"u":"b"}`, `{"n":3,"u":"c"}`} {
		if !got[want] {
			t.Fatalf("missing round-tripped row %s; got %v", want, rows)
		}
	}
}

// TestFlatFileGzipStem: a single foo.jsonl.gz is keyspace "foo" (both the .gz and
// .jsonl extensions are stripped) and transparently decompressed.
func TestFlatFileGzipStem(t *testing.T) {
	path := writeFlatFileGz(t, "orders.jsonl.gz",
		`{"amt":5}`+"\n"+`{"amt":7}`+"\n")
	store, conv := flatRootConv(t, path,
		"SELECT SUM(x.amt) AS tot, COUNT(*) AS n FROM orders x")
	rows := flatRootRows(t, conv, testGlueExec(t, false, store, conv))
	if len(rows) != 1 {
		t.Fatalf("want 1 aggregate row, got %d (%v)", len(rows), rows)
	}
	if got := jsonOf(rows[0]); got != `{"n":2,"tot":12}` {
		t.Fatalf("want {\"n\":2,\"tot\":12}, got %s", got)
	}
}

// TestFlatFileNotTriggeredForNonRecordFile: pointing at a non-record file must not
// synthesize a keyspace (it falls through to the plain file datastore, which
// errors because the arg isn't a directory). Uses a ".dat" file: since the
// extract feature landed, .txt/.log/.md/.pdf/... are record files (their text is
// extracted), so the non-record case needs an extension that is neither a
// structured-data format nor an extractable document.
func TestFlatFileNotTriggeredForNonRecordFile(t *testing.T) {
	path := writeFlatFile(t, "notes.dat", "hello")
	if _, err := glue.FileStore(path); err == nil {
		t.Fatalf("non-record file %q should not yield a usable store", path)
	}
}
