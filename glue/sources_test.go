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

// TestOpenSessionSourcesErrors covers the guard rails: name collision, an
// object-store URI (Phase 2), and an undial-able derived name.
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
	if _, err := OpenSessionSources([]Source{{Path: "s3://bucket/t/metadata/x.json"}}, "default"); err == nil {
		t.Error("expected object-store sources to be rejected in Phase 1")
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
