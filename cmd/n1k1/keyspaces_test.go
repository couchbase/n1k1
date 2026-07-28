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

package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/couchbase/n1k1/glue"
)

// TestDotFormatsPersists: `.formats <set>` on a directory datastore updates the
// live scan options and persists the set into the catalog.
func TestDotFormatsPersists(t *testing.T) {
	saved := glue.ScanWalkOptions
	defer func() { glue.ScanWalkOptions = saved }()

	dir := t.TempDir()
	var buf bytes.Buffer
	c := &cli{prog: "n1k1", dir: dir, out: &buf, stderr: &buf}
	if quit := c.dot(".formats json,csv"); quit {
		t.Fatal("dot returned quit")
	}
	if f, err := glue.CatalogFormats(dir); err != nil || f != "json,csv" {
		t.Errorf("persisted formats = %q err %v, want json,csv", f, err)
	}
	if !strings.Contains(glue.ScanWalkOptions.Spec, "csv") {
		t.Errorf("live formats not updated: %s", glue.ScanWalkOptions.Spec)
	}
}

// TestKeyspacesBacktickHint (IDEA-0010): a listing that includes a keyspace needing
// backticks (a dotted name, the norm in a bundle) appends the shell-quoting note; a
// listing of only bare identifiers does not.
func TestKeyspacesBacktickHint(t *testing.T) {
	mkKS := func(t *testing.T, name string) *cli {
		t.Helper()
		root := t.TempDir()
		ks := filepath.Join(root, "default", name)
		if err := os.MkdirAll(ks, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(ks, "l.jsonl"), []byte(`{"a":1}`+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		sess, err := glue.OpenSession(root, "default")
		if err != nil {
			t.Fatalf("OpenSession: %v", err)
		}
		return &cli{prog: "n1k1", dir: root, sess: sess}
	}

	// A dotted keyspace -> the note appears (and names the shell-safe options).
	var dotted bytes.Buffer
	mkKS(t, "ns_server.error").printKeyspaces(&dotted)
	out := dotted.String()
	if !strings.Contains(out, "single quotes") || !strings.Contains(out, "-f") {
		t.Errorf("dotted-keyspace listing missing the backtick/shell note:\n%s", out)
	}

	// A bare identifier -> no note.
	var bare bytes.Buffer
	mkKS(t, "events").printKeyspaces(&bare)
	if strings.Contains(bare.String(), "single quotes") {
		t.Errorf("bare-keyspace listing should not carry the backtick note:\n%s", bare.String())
	}
}

// TestQuotePath: dotted field paths are backticked per-segment (only where SQL++
// needs it), so a nested path stays a path expression.
func TestQuotePath(t *testing.T) {
	cases := map[string]string{
		"sku":                "sku",
		"profile.city":       "profile.city",
		"first name":         "`first name`",
		"profile.first name": "profile.`first name`",
		"a.b c.d":            "a.`b c`.d",
		"2026-01":            "`2026-01`", // leading-digit/hyphen segment
	}
	for in, want := range cases {
		if got := quotePath(in); got != want {
			t.Errorf("quotePath(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestCatalogPath(t *testing.T) {
	if got := (&cli{dir: "/data/shop"}).catalogPath(); got != "/data/shop/.n1k1/catalog.json" {
		t.Errorf("catalogPath(dir) = %q", got)
	}
	if got := (&cli{}).catalogPath(); !strings.Contains(got, "<dataRoot>") {
		t.Errorf("catalogPath(empty) = %q, want a placeholder", got)
	}
}

func TestDataLoc(t *testing.T) {
	if got := (&cli{dir: "/data/shop"}).dataLoc(); got != "/data/shop" {
		t.Errorf("dataLoc(dir) = %q", got)
	}
	if got := (&cli{}).dataLoc(); !strings.Contains(got, "none") {
		t.Errorf("dataLoc(empty) = %q, want a 'none' hint", got)
	}
}

// TestJsonType covers the type-name mapping .schema uses to describe fields
// (the value shapes come from encoding/json's decode of a JSON document).
func TestSchemaSamplesMultiRecordSingleFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.jsonl") // single file, many records
	body := `{"type":"a","n":1,"tags":["x"]}` + "\n" + `{"type":"b","n":2}` + "\n"
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}

	sess, err := glue.OpenSession(path, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	c := &cli{sess: sess}

	ss, err := c.sess.SampleSchema("events", 50)
	if err != nil {
		t.Fatalf("SampleSchema: %v", err)
	}
	if ss.Rows != 2 {
		t.Fatalf("sampled %d docs, want 2 (the multi-record file's rows)", ss.Rows)
	}
	for _, k := range []string{"type", "n", "tags"} {
		if ss.Fields[k] == nil {
			t.Errorf("missing field %q in fields %v", k, ss.Fields)
		}
	}
	if fs := ss.Fields["n"]; fs == nil || len(fs.Types) != 1 || fs.Types[0] != "number" {
		t.Errorf("n types = %v, want [number]", ss.Fields["n"])
	}
	if fs := ss.Fields["tags"]; fs == nil || !hasType(fs, "array") || !fs.NonScalar {
		t.Errorf("tags should be a non-scalar array field, got %v", ss.Fields["tags"])
	}
}

// hasType reports whether a sampled field observed the given JSON type.
func hasType(fs *glue.FieldStat, want string) bool {
	for _, t := range fs.Types {
		if t == want {
			return true
		}
	}
	return false
}

// TestKeyspacesFramingTags: .tables tags each keyspace with how its files become
// rows (IDEA-0007) -- a structured format, a whole-file blob, or a recipe -- and
// prints the whole-file hint when a blob is present.
func TestKeyspacesFramingTags(t *testing.T) {
	root := t.TempDir()
	write := func(ks, name, body string) {
		d := filepath.Join(root, "default", ks)
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(d, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("events", "e.jsonl", `{"a":1}`+"\n"+`{"a":2}`+"\n") // structured (jsonl)
	write("notes", "readme.log", "line one\nline two\n")      // whole-file blob
	// A .log the built-in ns_server_log recipe claims -> recipe-framed.
	write("nsl", "ns_server.error.log",
		"[ns_server:error,2026-07-10T12:00:01.000Z,n1@h:<0.2>] boom\n")

	sess, err := glue.OpenSession(root, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	var buf bytes.Buffer
	c := &cli{sess: sess, dir: root, out: &buf}
	c.printKeyspaces(&buf)
	out := buf.String()

	for _, want := range []string{
		"events", "jsonl",
		"notes", "whole-file",
		"nsl", "recipe=ns_server_log",
		"whole-file = one row per file", // the blob hint
	} {
		if !strings.Contains(out, want) {
			t.Errorf(".tables output missing %q; got:\n%s", want, out)
		}
	}
}

// TestKeyspacesUnexposedFilesHint: a bundle-style dir (top-level files + a subdir)
// hints the unframed top-level logs that aren't keyspaces (IDEA-0012), and never
// exposes or mentions files inside a dot-dir (.git/.n1k1).
func TestKeyspacesUnexposedFilesHint(t *testing.T) {
	root := t.TempDir()
	write := func(rel, body string) {
		p := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("data.jsonl", `{"a":1}`+"\n")      // structured -> keyspace
	write("memcached.log", "raw log\n")      // unframed -> hinted
	write("couchbase.log", "more raw\n")     // unframed -> hinted
	write("stats/x.txt", "x\n")              // a subdir -> bundle (B3) layout
	write(".git/junk.jsonl", `{"j":1}`+"\n") // dot-dir -> never exposed nor mentioned

	sess, err := glue.OpenSession(root, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	var buf bytes.Buffer
	c := &cli{sess: sess, dir: root, out: &buf}
	c.printKeyspaces(&buf)
	out := buf.String()

	for _, want := range []string{
		"data",             // the structured keyspace is listed
		"aren't keyspaces", // the unframed-files hint fired
		"memcached.log", "couchbase.log",
		"add a *.extract.js recipe",
	} {
		if !strings.Contains(out, want) {
			t.Errorf(".tables output missing %q; got:\n%s", want, out)
		}
	}
	for _, bad := range []string{"junk", ".git"} {
		if strings.Contains(out, bad) {
			t.Errorf(".tables output leaked hidden-dir content %q; got:\n%s", bad, out)
		}
	}
}

// TestSchemaFlatRootUnion: a flat-root dir samples the union of fields across its
// files (the b.jsonl-only "z" field must appear).
// TestSchemaAcceptsBacktickedKeyspace: a dot-command keyspace arg may be backticked
// exactly like the SQL spelling (IDEA-0009) -- `.schema `ns_server.error“ resolves
// the same keyspace as the bare `.schema ns_server.error`, with no error.
func TestSchemaAcceptsBacktickedKeyspace(t *testing.T) {
	root := t.TempDir()
	// Flat single-file keyspace by stem: ns_server.error.log -> "ns_server.error".
	if err := os.WriteFile(filepath.Join(root, "ns_server.error.log"),
		[]byte("[ns_server:info,2026-05-17T15:36:11.100+02:00,n1:x]hello\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "certs"), 0o755); err != nil { // subdir -> B3
		t.Fatal(err)
	}
	sess, err := glue.OpenSession(root, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}

	run := func(arg string) (string, string) {
		var out, errb bytes.Buffer
		c := &cli{prog: "n1k1", dir: root, sess: sess, out: &out, stderr: &errb}
		c.cmdSchema(arg)
		return out.String(), errb.String()
	}
	bareOut, bareErr := run("ns_server.error")
	tickOut, tickErr := run("`ns_server.error`")

	for _, tc := range []struct{ name, out, err string }{
		{"bare", bareOut, bareErr}, {"backticked", tickOut, tickErr},
	} {
		if strings.Contains(tc.err, "Error") || strings.Contains(tc.err, "no keyspace") {
			t.Errorf("%s: .schema errored: %s", tc.name, tc.err)
		}
		if !strings.Contains(tc.out, "ns_server.error") || !strings.Contains(tc.out, "sampled") {
			t.Errorf("%s: .schema output unexpected:\n%s", tc.name, tc.out)
		}
	}
}

func TestSchemaFlatRootUnion(t *testing.T) {
	base := "sd"
	dir := filepath.Join(t.TempDir(), base)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "a.jsonl"), []byte(`{"m":1}`+"\n"+`{"m":2}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "b.jsonl"), []byte(`{"m":3,"z":true}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	sess, err := glue.OpenSession(dir, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	c := &cli{sess: sess}

	ss, err := c.sess.SampleSchema(base, 50)
	if err != nil {
		t.Fatalf("SampleSchema: %v", err)
	}
	if ss.Rows != 3 {
		t.Fatalf("sampled %d docs, want 3", ss.Rows)
	}
	if ss.Fields["m"] == nil || ss.Fields["z"] == nil {
		t.Errorf("expected union of m+z across files, got %v", ss.Fields)
	}
}

// TestSchemaDistinctAndExample: distinct scalar values are tracked (deduped,
// first-seen order) and drive the generated SQL++: a single value -> `=`, a few
// -> `IN`, and a non-scalar field -> `IS NOT MISSING`.
func TestSchemaDistinctAndExample(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "txns.jsonl")
	body := `{"cur":"USD","kind":"sale","tags":["a"]}` + "\n" +
		`{"cur":"EUR","kind":"sale","tags":["b","c"]}` + "\n" +
		`{"cur":"USD","kind":"sale"}` + "\n"
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	sess, err := glue.OpenSession(path, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	c := &cli{sess: sess}

	ss, err := c.sess.SampleSchema("txns", 50)
	if err != nil {
		t.Fatalf("SampleSchema: %v", err)
	}

	// cur: two distinct values (deduped), first-seen order -> IN.
	if got := schemaExample("txns", "cur", ss.Fields["cur"]); got != `SELECT * FROM txns WHERE cur IN ["USD", "EUR"];` {
		t.Errorf("cur example = %q", got)
	}
	// kind: one distinct value -> =.
	if got := schemaExample("txns", "kind", ss.Fields["kind"]); got != `SELECT * FROM txns WHERE kind = "sale";` {
		t.Errorf("kind example = %q", got)
	}
	// tags: array-valued -> no scalar literal, IS NOT MISSING.
	if got := schemaExample("txns", "tags", ss.Fields["tags"]); got != `SELECT * FROM txns WHERE tags IS NOT MISSING;` {
		t.Errorf("tags example = %q", got)
	}
}

// TestExampleQuery: the .help/.schema example uses a real keyspace, omits the
// optional "default:" prefix, and is empty when there are no keyspaces.
func TestExampleQuery(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.jsonl")
	if err := os.WriteFile(path, []byte(`{"n":1}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	sess, err := glue.OpenSession(path, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	c := &cli{sess: sess}
	if got := c.exampleQuery(); got != "SELECT * FROM events LIMIT 5;" {
		t.Errorf("exampleQuery = %q", got)
	}

	// No datastore / no keyspaces -> no example.
	empty := &cli{}
	empty.sess, err = glue.OpenSession(t.TempDir(), "default")
	if err != nil {
		t.Fatalf("OpenSession(empty): %v", err)
	}
	if got := empty.exampleQuery(); got != "" {
		t.Errorf("exampleQuery(no keyspaces) = %q, want empty", got)
	}
}

// TestSchemaExample unit-tests the SQL++ generation directly across the branches.
func TestSchemaExample(t *testing.T) {
	raws := func(ss ...string) []json.RawMessage {
		out := make([]json.RawMessage, len(ss))
		for i, s := range ss {
			out[i] = json.RawMessage(s)
		}
		return out
	}
	cases := []struct {
		name  string
		ks    string
		field string
		fs    *glue.FieldStat
		want  string
	}{
		{"single", "orders", "status", &glue.FieldStat{Values: raws(`"open"`)},
			`SELECT * FROM orders WHERE status = "open";`},
		{"few-in", "orders", "cur", &glue.FieldStat{Values: raws(`"USD"`, `"EUR"`, `"GBP"`)},
			`SELECT * FROM orders WHERE cur IN ["USD", "EUR", "GBP"];`},
		{"numbers-in", "orders", "qty", &glue.FieldStat{Values: raws(`1`, `2`)},
			`SELECT * FROM orders WHERE qty IN [1, 2];`},
		{"none", "orders", "meta", &glue.FieldStat{NonScalar: true},
			`SELECT * FROM orders WHERE meta IS NOT MISSING;`},
		{"capped-falls-back-to-eq", "orders", "id",
			&glue.FieldStat{Values: raws(`"a"`, `"b"`), Capped: true},
			`SELECT * FROM orders WHERE id = "a";`},
		{"backticked", "2026-01", "my-field", &glue.FieldStat{Values: raws(`"x"`)},
			"SELECT * FROM `2026-01` WHERE `my-field` = \"x\";"},
	}
	for _, tc := range cases {
		if got := schemaExample(tc.ks, tc.field, tc.fs); got != tc.want {
			t.Errorf("%s: schemaExample = %q, want %q", tc.name, got, tc.want)
		}
	}
}
