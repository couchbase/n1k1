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
)

// writeKeyspace builds a normal <root>/default/<ks>/ layout and writes files
// (whose relative paths may include subdirs), returning the root. This exercises
// the n1k1-native record scan over multi-file / recursive / gzip'd keyspaces --
// the C/E/H scenarios in DESIGN-data.md -- through the standard FROM path.
func writeKeyspace(t *testing.T, ks string, files map[string]string) (root string) {
	t.Helper()
	root = t.TempDir()
	for rel, body := range files {
		p := filepath.Join(root, "default", ks, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		var data []byte = []byte(body)
		if filepath.Ext(p) == ".gz" {
			data = gzipBytes(t, body)
		}
		if err := os.WriteFile(p, data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func gzipBytes(t *testing.T, s string) []byte {
	t.Helper()
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	if _, err := gw.Write([]byte(s)); err != nil {
		t.Fatal(err)
	}
	if err := gw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// TestRecordScanMultiFileJSONL: scenario C -- a keyspace is the union of many
// JSONL files, each with many records.
func TestRecordScanMultiFileJSONL(t *testing.T) {
	root := writeKeyspace(t, "events", map[string]string{
		"day1.jsonl": "{\"u\":\"a\",\"act\":\"login\"}\n{\"u\":\"a\",\"act\":\"view\"}\n",
		"day2.jsonl": "{\"u\":\"b\",\"act\":\"login\"}\n{\"u\":\"b\",\"act\":\"buy\"}\n{\"u\":\"c\",\"act\":\"login\"}\n",
	})
	store, conv := flatRootConv(t, root,
		"SELECT e.act AS act FROM default:events e")
	rows := flatRootRows(t, conv, testGlueExec(t, false, store, conv))
	if len(rows) != 5 {
		t.Fatalf("want 5 records across 2 JSONL files, got %d (%v)", len(rows), rows)
	}
	logins := 0
	for _, r := range rows {
		if jsonOf(r) == `{"act":"login"}` {
			logins++
		}
	}
	if logins != 3 {
		t.Fatalf("want 3 logins, got %d (%v)", logins, rows)
	}
}

// TestRecordScanRecursive: scenario E -- a keyspace unions files across nested
// subdirectories.
func TestRecordScanRecursive(t *testing.T) {
	root := writeKeyspace(t, "cpu", map[string]string{
		"hostA/2026/01/a.jsonl": "{\"host\":\"A\",\"v\":1}\n{\"host\":\"A\",\"v\":2}\n",
		"hostB/2026/02/b.jsonl": "{\"host\":\"B\",\"v\":3}\n",
	})
	store, conv := flatRootConv(t, root,
		"SELECT COUNT(*) AS n, SUM(c.v) AS tot FROM default:cpu c")
	rows := flatRootRows(t, conv, testGlueExec(t, false, store, conv))
	if len(rows) != 1 || jsonOf(rows[0]) != `{"n":3,"tot":6}` {
		t.Fatalf("recursive union: want {n:3,tot:6}, got %v", rows)
	}
}

// TestRecordScanGzip: scenario H -- transparent gzip decompression.
func TestRecordScanGzip(t *testing.T) {
	root := writeKeyspace(t, "orders", map[string]string{
		"2025.jsonl.gz": "{\"id\":\"o1\",\"amt\":10}\n{\"id\":\"o2\",\"amt\":20}\n",
		"2026.jsonl.gz": "{\"id\":\"o3\",\"amt\":30}\n",
	})
	store, conv := flatRootConv(t, root,
		"SELECT COUNT(*) AS n, SUM(o.amt) AS tot FROM default:orders o")
	rows := flatRootRows(t, conv, testGlueExec(t, false, store, conv))
	if len(rows) != 1 || jsonOf(rows[0]) != `{"n":3,"tot":60}` {
		t.Fatalf("gzip: want {n:3,tot:60}, got %v", rows)
	}
}

// TestCountStar: COUNT(*) works (VisitCountScan de-optimized to scan+count) and
// counts RECORDS -- for multi-record JSONL it must be 5, not the 2-file count
// cbq's keyspace.Count() would have returned.
func TestCountStar(t *testing.T) {
	t.Run("jsonl-records-not-files", func(t *testing.T) {
		root := writeKeyspace(t, "events", map[string]string{
			"a.jsonl": "{\"x\":1}\n{\"x\":2}\n{\"x\":3}\n",
			"b.jsonl": "{\"x\":4}\n{\"x\":5}\n",
		})
		store, conv := flatRootConv(t, root, "SELECT COUNT(*) AS n FROM default:events")
		rows := flatRootRows(t, conv, testGlueExec(t, false, store, conv))
		if len(rows) != 1 || jsonOf(rows[0]) != `{"n":5}` {
			t.Fatalf("COUNT(*) should count 5 records across 2 files, got %v", rows)
		}
	})
	t.Run("one-doc-per-file", func(t *testing.T) {
		root := writeKeyspace(t, "docs", map[string]string{
			"d1.json": `{"a":1}`, "d2.json": `{"a":2}`, "d3.json": `{"a":3}`, "d4.json": `{"a":4}`,
		})
		store, conv := flatRootConv(t, root, "SELECT COUNT(*) AS n FROM default:docs")
		rows := flatRootRows(t, conv, testGlueExec(t, false, store, conv))
		if len(rows) != 1 || jsonOf(rows[0]) != `{"n":4}` {
			t.Fatalf("COUNT(*) want {n:4}, got %v", rows)
		}
	})
}

// TestRecordScanCSV: scenario J -- CSV rows decode to JSON objects (header keys),
// queryable by column with type inference, through the standard FROM path.
func TestRecordScanCSV(t *testing.T) {
	root := writeKeyspace(t, "txns", map[string]string{
		"q1.csv": "id,amount,currency\n" +
			"t1,10.5,USD\n" +
			"t2,20,USD\n" +
			"t3,5.25,EUR\n",
	})
	// Filter + aggregate over CSV-derived columns (amount inferred numeric).
	store, conv := flatRootConv(t, root,
		"SELECT x.currency AS cur, SUM(x.amount) AS tot FROM default:txns x "+
			"WHERE x.currency = 'USD' GROUP BY x.currency")
	rows := flatRootRows(t, conv, testGlueExec(t, false, store, conv))
	if len(rows) != 1 || jsonOf(rows[0]) != `{"cur":"USD","tot":30.5}` {
		t.Fatalf("CSV query: want {cur:USD,tot:30.5}, got %v", rows)
	}
}

// TestRecordScanMixedFormats: a keyspace mixing one-doc JSON + JSONL unions both.
func TestRecordScanMixedFormats(t *testing.T) {
	root := writeKeyspace(t, "mix", map[string]string{
		"single.json": `{"src":"json"}`,
		"many.jsonl":  "{\"src\":\"jsonl\"}\n{\"src\":\"jsonl\"}\n",
	})
	// (COUNT(*) alone would hit the pre-existing CountScan-NA gap; project a
	// field so the records path runs and count the rows.)
	store, conv := flatRootConv(t, root,
		"SELECT m.src AS src FROM default:mix m")
	rows := flatRootRows(t, conv, testGlueExec(t, false, store, conv))
	if len(rows) != 3 {
		t.Fatalf("mixed formats: want 3 rows (1 json + 2 jsonl), got %d (%v)", len(rows), rows)
	}
}
