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
func TestJsonType(t *testing.T) {
	cases := []struct {
		v    interface{}
		want string
	}{
		{nil, "null"},
		{true, "bool"},
		{float64(3), "number"},
		{"s", "string"},
		{[]interface{}{1, 2}, "array"},
		{map[string]interface{}{"a": 1}, "object"},
	}
	for _, tc := range cases {
		if got := jsonType(tc.v); got != tc.want {
			t.Errorf("jsonType(%T) = %q, want %q", tc.v, got, tc.want)
		}
	}
}

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

	stats, n, err := c.sampleSchema("events", 50)
	if err != nil {
		t.Fatalf("sampleSchema: %v", err)
	}
	if n != 2 {
		t.Fatalf("sampled %d docs, want 2 (the multi-record file's rows)", n)
	}
	for _, k := range []string{"type", "n", "tags"} {
		if stats[k] == nil {
			t.Errorf("missing field %q in stats %v", k, stats)
		}
	}
	if fs := stats["n"]; fs == nil || len(fs.types) != 1 || !fs.types["number"] {
		t.Errorf("n types = %v, want {number}", stats["n"])
	}
	if fs := stats["tags"]; fs == nil || !fs.types["array"] || !fs.nonScalar {
		t.Errorf("tags should be a non-scalar array field, got %v", stats["tags"])
	}
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
	write("notes", "readme.log", "line one\nline two\n")       // whole-file blob
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

// TestSchemaFlatRootUnion: a flat-root dir samples the union of fields across its
// files (the b.jsonl-only "z" field must appear).
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

	stats, n, err := c.sampleSchema(base, 50)
	if err != nil {
		t.Fatalf("sampleSchema: %v", err)
	}
	if n != 3 {
		t.Fatalf("sampled %d docs, want 3", n)
	}
	if stats["m"] == nil || stats["z"] == nil {
		t.Errorf("expected union of m+z across files, got %v", stats)
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

	stats, _, err := c.sampleSchema("txns", 50)
	if err != nil {
		t.Fatalf("sampleSchema: %v", err)
	}

	// cur: two distinct values (deduped), first-seen order -> IN.
	if got := schemaExample("txns", "cur", stats["cur"]); got != `SELECT * FROM txns WHERE cur IN ["USD", "EUR"];` {
		t.Errorf("cur example = %q", got)
	}
	// kind: one distinct value -> =.
	if got := schemaExample("txns", "kind", stats["kind"]); got != `SELECT * FROM txns WHERE kind = "sale";` {
		t.Errorf("kind example = %q", got)
	}
	// tags: array-valued -> no scalar literal, IS NOT MISSING.
	if got := schemaExample("txns", "tags", stats["tags"]); got != `SELECT * FROM txns WHERE tags IS NOT MISSING;` {
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
		fs    *fieldStat
		want  string
	}{
		{"single", "orders", "status", &fieldStat{values: raws(`"open"`)},
			`SELECT * FROM orders WHERE status = "open";`},
		{"few-in", "orders", "cur", &fieldStat{values: raws(`"USD"`, `"EUR"`, `"GBP"`)},
			`SELECT * FROM orders WHERE cur IN ["USD", "EUR", "GBP"];`},
		{"numbers-in", "orders", "qty", &fieldStat{values: raws(`1`, `2`)},
			`SELECT * FROM orders WHERE qty IN [1, 2];`},
		{"none", "orders", "meta", &fieldStat{nonScalar: true},
			`SELECT * FROM orders WHERE meta IS NOT MISSING;`},
		{"capped-falls-back-to-eq", "orders", "id",
			&fieldStat{values: raws(`"a"`, `"b"`), capped: true},
			`SELECT * FROM orders WHERE id = "a";`},
		{"backticked", "2026-01", "my-field", &fieldStat{values: raws(`"x"`)},
			"SELECT * FROM `2026-01` WHERE `my-field` = \"x\";"},
	}
	for _, tc := range cases {
		if got := schemaExample(tc.ks, tc.field, tc.fs); got != tc.want {
			t.Errorf("%s: schemaExample = %q, want %q", tc.name, got, tc.want)
		}
	}
}

// TestFieldStatObserveCaps: distinct scalar values are capped and marked; null is
// not collected as a value.
func TestFieldStatObserveCaps(t *testing.T) {
	fs := &fieldStat{}
	for i := 0; i < maxSchemaValues+5; i++ {
		raw, _ := json.Marshal(i)
		fs.observe(float64(i), raw)
	}
	if len(fs.values) != maxSchemaValues || !fs.capped {
		t.Errorf("expected %d values + capped, got %d capped=%v", maxSchemaValues, len(fs.values), fs.capped)
	}
	// A duplicate isn't re-added; null is never a value.
	fs2 := &fieldStat{}
	fs2.observe("x", json.RawMessage(`"x"`))
	fs2.observe("x", json.RawMessage(`"x"`))
	fs2.observe(nil, json.RawMessage(`null`))
	if len(fs2.values) != 1 || !fs2.types["null"] {
		t.Errorf("dedup/null handling off: values=%v types=%v", fs2.values, fs2.types)
	}
}
