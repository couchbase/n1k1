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
	"os"
	"path/filepath"
	"testing"

	"github.com/couchbase/n1k1/glue"
)

// TestSchemaSamplesMultiRecordSingleFile guards the fix for the ".schema reports
// 0 docs while SELECT COUNT(*) > 0" bug: the old filesystem walk only read
// one-doc-per-file *.json, so single-file / multi-record / non-JSON keyspaces
// sampled nothing. .schema now samples via a real SELECT through the session.
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
	c := &cli{sess: sess, ns: "default"}

	shape, n, err := c.sampleSchema("events", 50)
	if err != nil {
		t.Fatalf("sampleSchema: %v", err)
	}
	if n != 2 {
		t.Fatalf("sampled %d docs, want 2 (the multi-record file's rows)", n)
	}
	for _, k := range []string{"type", "n", "tags"} {
		if shape[k] == nil {
			t.Errorf("missing key %q in shape %v", k, shape)
		}
	}
	if got := shape["n"]; len(got) != 1 || got[0] != "number" {
		t.Errorf("n type = %v, want [number]", got)
	}
	if got := shape["tags"]; len(got) != 1 || got[0] != "array" {
		t.Errorf("tags type = %v, want [array]", got)
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
	c := &cli{sess: sess, ns: "default"}

	shape, n, err := c.sampleSchema(base, 50)
	if err != nil {
		t.Fatalf("sampleSchema: %v", err)
	}
	if n != 3 {
		t.Fatalf("sampled %d docs, want 3", n)
	}
	if shape["m"] == nil || shape["z"] == nil {
		t.Errorf("expected union of m+z across files, got %v", shape)
	}
}
