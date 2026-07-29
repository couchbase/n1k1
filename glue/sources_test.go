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
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// TestOpenSessionSources: several independent local roots become sibling keyspaces
// under one namespace, joinable in one query -- the DESIGN-data.md §2 Phase 1 story.
func TestOpenSessionSources(t *testing.T) {
	root := t.TempDir()
	// Two directory sources + one single-file source, laid out as independent trees.
	drive := filepath.Join(root, "drive")
	docs := filepath.Join(root, "docs")
	for _, d := range []string{drive, filepath.Join(docs, "sub")} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	write := func(p, body string) {
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write(filepath.Join(drive, "a.json"), `{"id":1,"who":"drive"}`)
	write(filepath.Join(docs, "b.json"), `{"id":2,"who":"docs-top"}`)
	write(filepath.Join(docs, "sub", "c.json"), `{"id":3,"who":"docs-sub"}`) // must recurse
	notesFile := filepath.Join(root, "notes.jsonl")
	write(notesFile, `{"id":1,"tag":"x"}`+"\n"+`{"id":2,"tag":"y"}`+"\n")

	sess, err := OpenSessionSources([]Source{
		{Path: drive},                   // bare dir -> derived name "drive", recursive
		{Name: "documents", Path: docs}, // explicit name
		{Path: notesFile},               // single file -> derived name "notes"
	}, "default")
	if err != nil {
		t.Fatalf("OpenSessionSources: %v", err)
	}
	defer sess.Close()

	// .tables (Keyspaces) advertises exactly the three sources.
	infos, err := sess.Keyspaces()
	if err != nil {
		t.Fatalf("Keyspaces: %v", err)
	}
	var names []string
	for _, ki := range infos {
		names = append(names, ki.Name)
	}
	sort.Strings(names)
	if got, want := names, []string{"documents", "drive", "notes"}; !equalStrings(got, want) {
		t.Fatalf("keyspaces = %v, want %v", got, want)
	}

	// The bare-dir source recursed (docs-top + docs-sub) -> 2 rows.
	if res, err := sess.Run(`SELECT COUNT(*) AS n FROM documents`); err != nil {
		t.Fatalf("count documents: %v", err)
	} else if s := string(res.Rows[0]); s != `{"n":2}` {
		t.Errorf("documents count = %s, want {\"n\":2}", s)
	}

	// A cross-source join in a single query -- the whole point.
	res, err := sess.Run(`SELECT d.who AS who FROM drive d JOIN notes n ON d.id = n.id`)
	if err != nil {
		t.Fatalf("cross-source join: %v", err)
	}
	if len(res.Rows) != 1 || string(res.Rows[0]) != `{"who":"drive"}` {
		t.Errorf("join rows = %v, want one {\"who\":\"drive\"}", stringsOf(res.Rows))
	}
}

// TestOpenSessionSourcesErrors covers the guard rails: a name collision and an
// underivable name (a rootless glob).
func TestOpenSessionSourcesErrors(t *testing.T) {
	root := t.TempDir()
	a := filepath.Join(root, "x", "data")
	b := filepath.Join(root, "y", "data") // same basename "data" -> collision
	for _, d := range []string{a, b} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(d, "f.json"), []byte(`{"k":1}`), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := OpenSessionSources([]Source{{Path: a}, {Path: b}}, "default"); err == nil {
		t.Error("expected a name-collision error for two 'data' basenames")
	}
	if _, err := OpenSessionSources([]Source{{Path: "**/*.json"}}, "default"); err == nil {
		t.Error("expected an error deriving a name from a rootless glob")
	}
	// Explicit names sidestep the collision.
	sess, err := OpenSessionSources([]Source{{Name: "ax", Path: a}, {Name: "bx", Path: b}}, "default")
	if err != nil {
		t.Fatalf("explicit-name sources: %v", err)
	}
	sess.Close()
}

// TestDeriveSourceName spot-checks the name-derivation rules.
func TestDeriveSourceName(t *testing.T) {
	cases := map[string]string{
		"/data/orders":        "orders", // dir
		"/data/events.jsonl":  "events", // file stem
		"/data/logs.jsonl.gz": "logs",   // one compression + one format ext stripped
		"/data/sales/*.csv":   "sales",  // glob base
		"/data/tree/**":       "tree",   // recursive glob base
	}
	for in, want := range cases {
		got, err := deriveSourceName(in)
		if err != nil {
			t.Errorf("deriveSourceName(%q) error: %v", in, err)
			continue
		}
		if got != want {
			t.Errorf("deriveSourceName(%q) = %q, want %q", in, got, want)
		}
	}
	if _, err := deriveSourceName("*"); err == nil {
		t.Error("deriveSourceName(\"*\") should error (no usable base)")
	}
}

// TestSourceFlatKeyspaceKinds: the per-source classifier maps each KIND to the right
// flatKeyspace fields (what KeyspaceRecordsOpen routes on) -- the heart of Phase 2
// federation. Object-store cases that need no network are checked offline.
func TestSourceFlatKeyspaceKinds(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a.json"), []byte(`{"k":1}`), 0o644); err != nil {
		t.Fatal(err)
	}

	// Local directory -> a recursive glob keyspace.
	if ks, err := sourceFlatKeyspace(root, ""); err != nil {
		t.Errorf("local dir: %v", err)
	} else if ks.glob == "" || ks.iceberg != "" || ks.parquetURL != "" {
		t.Errorf("local dir -> %+v, want glob set", ks)
	}
	// Remote Parquet object -> parquetURL (offline: just records the URL).
	if ks, err := sourceFlatKeyspace("s3://bucket/data/t.parquet", ""); err != nil {
		t.Errorf("remote parquet: %v", err)
	} else if ks.parquetURL != "s3://bucket/data/t.parquet" {
		t.Errorf("remote parquet -> %+v, want parquetURL set", ks)
	}
	// Remote Iceberg via an explicit metadata JSON -> iceberg + table dir (offline).
	loc := "s3://bucket/warehouse/db/orders/metadata/00003-abc.metadata.json"
	if ks, err := sourceFlatKeyspace(loc, ""); err != nil {
		t.Errorf("remote iceberg: %v", err)
	} else if ks.iceberg != loc || ks.dir != "s3://bucket/warehouse/db/orders" {
		t.Errorf("remote iceberg -> %+v, want iceberg+dir set", ks)
	}
	// A malformed object-store Iceberg location (no .../metadata/…metadata.json) errors
	// offline, not silently.
	if _, err := sourceFlatKeyspace("s3://bucket/x.metadata.json", ""); err == nil {
		t.Error("expected a malformed object-store Iceberg location to error")
	}
}

// TestOpenSessionSourcesHeterogeneous federates DIFFERENT kinds in one session -- a
// local JSON directory and a local Apache Iceberg table -- and queries across both.
func TestOpenSessionSourcesHeterogeneous(t *testing.T) {
	root := t.TempDir()
	jsonDir := filepath.Join(root, "logs")
	if err := os.MkdirAll(jsonDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(jsonDir, "l.jsonl"),
		[]byte(`{"id":0,"sev":"info"}`+"\n"+`{"id":1,"sev":"error"}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	writeIcebergTable(t, root, "events", []string{"disk full", "oom killed"}) // {id,msg}, local Iceberg

	sess, err := OpenSessionSources([]Source{
		{Name: "logs", Path: jsonDir},
		{Name: "events", Path: filepath.Join(root, "events")}, // a local Iceberg table dir
	}, "default")
	if err != nil {
		t.Fatalf("OpenSessionSources (heterogeneous): %v", err)
	}
	defer sess.Close()

	// Each kind scans on its own: the JSON dir and the Iceberg table.
	if res, err := sess.Run(`SELECT COUNT(*) AS n FROM logs`); err != nil || string(res.Rows[0]) != `{"n":2}` {
		t.Errorf("count logs (json dir): rows=%v err=%v", stringsOf(res.Rows), err)
	}
	if res, err := sess.Run(`SELECT COUNT(*) AS n FROM events`); err != nil || string(res.Rows[0]) != `{"n":2}` {
		t.Errorf("count events (iceberg): rows=%v err=%v", stringsOf(res.Rows), err)
	}
	// A join ACROSS the two kinds in one query -- the whole point of federation.
	res, err := sess.Run(`SELECT e.msg AS msg FROM logs l JOIN events e ON l.id = e.id WHERE l.sev = "error"`)
	if err != nil {
		t.Fatalf("cross-kind join: %v", err)
	}
	if len(res.Rows) != 1 || string(res.Rows[0]) != `{"msg":"oom killed"}` {
		t.Errorf("cross-kind join rows = %v, want one {\"msg\":\"oom killed\"}", stringsOf(res.Rows))
	}
}

// TestLoadSources: a -sources config parses from JSON, YAML, and TOML (via n1k1's own
// decoders), accepts both the string shorthand and the object form, anchors a relative
// path at the config file's directory, and opens end-to-end.
func TestLoadSources(t *testing.T) {
	root := t.TempDir()
	// A source tree beside the config, referenced by a RELATIVE path (config-dir anchored).
	if err := os.MkdirAll(filepath.Join(root, "drive"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "drive", "a.json"), []byte(`{"id":1}`), 0o644); err != nil {
		t.Fatal(err)
	}
	docsAbs := filepath.Join(root, "docs")
	if err := os.MkdirAll(docsAbs, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(docsAbs, "b.json"), []byte(`{"id":2}`), 0o644); err != nil {
		t.Fatal(err)
	}

	configs := map[string]string{
		"sources.json": `{"sources":{"drive":"drive/**","docs":{"path":"` + docsAbs + `"}}}`,
		"sources.yaml": "sources:\n  drive: \"drive/**\"\n  docs: { path: \"" + docsAbs + "\" }\n",
		"sources.toml": "[sources]\ndrive = \"drive/**\"\n[sources.docs]\npath = \"" + docsAbs + "\"\n",
	}
	for file, body := range configs {
		path := filepath.Join(root, file)
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		srcs, err := LoadSources(path)
		if err != nil {
			t.Fatalf("%s: LoadSources: %v", file, err)
		}
		if len(srcs) != 2 || srcs[0].Name != "docs" || srcs[1].Name != "drive" { // name-sorted
			t.Fatalf("%s: sources = %+v, want name-sorted docs,drive", file, srcs)
		}
		// The relative "drive/**" anchored at the config's dir (not CWD).
		if want := filepath.Join(root, "drive", "**"); srcs[1].Path != want {
			t.Errorf("%s: drive path = %q, want config-dir-anchored %q", file, srcs[1].Path, want)
		}
		// Opens and queries end-to-end.
		sess, err := OpenSessionSourcesFile(path, "default")
		if err != nil {
			t.Fatalf("%s: OpenSessionSourcesFile: %v", file, err)
		}
		if res, err := sess.Run(`SELECT COUNT(*) AS n FROM drive`); err != nil || string(res.Rows[0]) != `{"n":1}` {
			t.Errorf("%s: query drive: rows=%v err=%v", file, stringsOf(res.Rows), err)
		}
		sess.Close()
	}
}

// TestOpenSessionSourcesPerSourceFormats: a per-source `-formats` restricts THAT source
// only -- two keyspaces over the same mixed dir, one all-formats and one json-only, see
// different file sets (no leakage of the override to the sibling).
func TestOpenSessionSourcesPerSourceFormats(t *testing.T) {
	mixed := t.TempDir()
	if err := os.WriteFile(filepath.Join(mixed, "a.json"), []byte(`{"k":1}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(mixed, "b.csv"), []byte("x,y\n1,2\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	sess, err := OpenSessionSources([]Source{
		{Name: "full", Path: mixed},                      // all formats: json + csv
		{Name: "jsononly", Path: mixed, Formats: "json"}, // json only
	}, "default")
	if err != nil {
		t.Fatalf("OpenSessionSources (per-source formats): %v", err)
	}
	defer sess.Close()

	count := func(ks string) string {
		t.Helper()
		res, err := sess.Run(`SELECT COUNT(*) AS n FROM ` + ks)
		if err != nil {
			t.Fatalf("count %s: %v", ks, err)
		}
		return string(res.Rows[0])
	}
	if got := count("full"); got != `{"n":2}` { // a.json + b.csv
		t.Errorf(`COUNT(full) = %s, want {"n":2} (json + csv)`, got)
	}
	if got := count("jsononly"); got != `{"n":1}` { // a.json only
		t.Errorf(`COUNT(jsononly) = %s, want {"n":1} (json only; csv filtered out)`, got)
	}

	// A per-source formats on an Iceberg/Parquet-kind source is rejected (single-format).
	if _, err := OpenSessionSources([]Source{
		{Name: "t", Path: "s3://bucket/db/t.parquet", Formats: "csv"},
	}, "default"); err == nil {
		t.Error("expected per-source formats on a remote Parquet source to be rejected")
	}
}

// TestLoadSourcesPerSourceOptions: a config `formats` is honored end-to-end; the
// reserved `namespace`/`sorted` fields are still rejected (no silent no-op).
func TestLoadSourcesPerSourceOptions(t *testing.T) {
	root := t.TempDir()
	// Data in a subdir so the config files (also *.json) in root aren't scanned.
	data := filepath.Join(root, "data")
	if err := os.MkdirAll(data, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(data, "a.json"), []byte(`{"k":1}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(data, "b.csv"), []byte("x\n1\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	// formats accepted + applied: a json-only source over the mixed dir sees 1 file.
	okCfg := filepath.Join(root, "ok.json")
	if err := os.WriteFile(okCfg,
		[]byte(`{"sources":{"docs":{"path":"data","formats":"json"}}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	srcs, err := LoadSources(okCfg)
	if err != nil {
		t.Fatalf("LoadSources (formats): %v", err)
	}
	if len(srcs) != 1 || srcs[0].Formats != "json" {
		t.Fatalf("sources = %+v, want one with Formats=json", srcs)
	}
	sess, err := OpenSessionSourcesFile(okCfg, "default")
	if err != nil {
		t.Fatalf("OpenSessionSourcesFile: %v", err)
	}
	res, _ := sess.Run(`SELECT COUNT(*) AS n FROM docs`)
	if string(res.Rows[0]) != `{"n":1}` {
		t.Errorf(`COUNT(docs) with formats=json = %s, want {"n":1}`, string(res.Rows[0]))
	}
	sess.Close()

	// `sorted` is still reserved -> rejected (namespace is supported; see below).
	badCfg := filepath.Join(root, "bad.json")
	if err := os.WriteFile(badCfg,
		[]byte(`{"sources":{"t":{"path":".","sorted":"ts"}}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadSources(badCfg); err == nil {
		t.Error("expected a per-source `sorted` to be rejected (reserved)")
	}
}

// TestOpenSessionSourcesNamespace: a per-source namespace places its keyspace under a
// non-default namespace, reachable as `FROM <ns>:<keyspace>`, while default-namespace
// sources stay prefix-free -- and both are joinable in one query.
func TestOpenSessionSourcesNamespace(t *testing.T) {
	root := t.TempDir()
	mk := func(sub, body string) string {
		d := filepath.Join(root, sub)
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(d, "a.json"), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		return d
	}
	logs := mk("logs", `{"id":1,"sev":"error"}`)
	orders := mk("orders", `{"id":1,"amt":10}`)

	sess, err := OpenSessionSources([]Source{
		{Name: "logs", Path: logs},                             // session-default namespace
		{Name: "orders", Path: orders, Namespace: "analytics"}, // a different namespace
	}, "default")
	if err != nil {
		t.Fatalf("OpenSessionSources (namespace): %v", err)
	}
	defer sess.Close()

	// Unqualified name resolves in the default namespace.
	if res, err := sess.Run(`SELECT COUNT(*) AS n FROM logs`); err != nil || string(res.Rows[0]) != `{"n":1}` {
		t.Errorf("FROM logs (default ns): rows=%v err=%v", stringsOf(res.Rows), err)
	}
	// The namespaced source needs its prefix.
	if res, err := sess.Run(`SELECT COUNT(*) AS n FROM analytics:orders`); err != nil || string(res.Rows[0]) != `{"n":1}` {
		t.Errorf("FROM analytics:orders: rows=%v err=%v", stringsOf(res.Rows), err)
	}
	// Cross-namespace join in one query.
	res, err := sess.Run(`SELECT o.amt AS amt FROM logs l JOIN analytics:orders o ON l.id = o.id`)
	if err != nil {
		t.Fatalf("cross-namespace join: %v", err)
	}
	if len(res.Rows) != 1 || string(res.Rows[0]) != `{"amt":10}` {
		t.Errorf("cross-namespace join = %v, want one {\"amt\":10}", stringsOf(res.Rows))
	}
}

func equalStrings(a, b []string) bool {
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

func stringsOf(rows []json.RawMessage) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = string(r)
	}
	return out
}
