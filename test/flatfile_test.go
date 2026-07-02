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
	"os"
	"path/filepath"
	"testing"

	"github.com/couchbase/n1k1/glue"
)

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
