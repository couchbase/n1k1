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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/couchbase/n1k1/glue"
)

// TestIndexSuggestEmitsCreateCommands: .index suggest prints both a catalog.json
// fragment (stdout) and the equivalent copy-pasteable `.index create` command
// (stderr), and the emitted command is valid create-DSL (parseCreateDSL accepts
// it, round-tripping to the same keyspace/keys).
func TestIndexSuggestEmitsCreateCommands(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "default", "customer")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	// A high-cardinality "sku" (selective -> suggested) and a low-card "kind".
	for i := 0; i < 12; i++ {
		doc := fmt.Sprintf(`{"sku":"SKU-%04d","kind":"x"}`, i)
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("c%02d.json", i)), []byte(doc), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	sess, err := glue.OpenSession(root, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", sess: sess, ns: "default", out: &out, stderr: &errb}

	c.cmdIndexSuggest("customer")

	// stdout: the catalog.json fragment for "sku".
	if !strings.Contains(out.String(), `"indexes"`) || !strings.Contains(out.String(), `"sku"`) {
		t.Errorf("stdout should be a catalog fragment for sku, got:\n%s", out.String())
	}
	// The fragment carries "why" and must be usable as-is (the catalog loader
	// ignores unknown keys) -- so the header no longer says to drop it.
	if !strings.Contains(out.String(), `"why"`) {
		t.Errorf("fragment should include the why rationale, got:\n%s", out.String())
	}
	if added, aerr := glue.CatalogAddIndexes(root, []byte(out.String())); aerr != nil || len(added) == 0 {
		t.Errorf("catalog fragment (with \"why\") should be accepted as-is: added=%v err=%v", added, aerr)
	}
	// stderr: a `.index create ... on customer (sku)` command.
	var createLine string
	for _, ln := range strings.Split(errb.String(), "\n") {
		if strings.Contains(ln, ".index create") && strings.Contains(ln, "(sku)") {
			createLine = strings.TrimSpace(ln)
		}
	}
	if createLine == "" {
		t.Fatalf(".index create command for sku not emitted; stderr:\n%s", errb.String())
	}

	// The emitted command must be valid create-DSL: strip ".index create " and
	// parse the rest, checking it round-trips to keyspace customer / key sku.
	dsl := strings.TrimPrefix(createLine, ".index create ")
	b, perr := parseCreateDSL(dsl)
	if perr != nil {
		t.Fatalf("emitted command isn't valid create-DSL (%q): %v", dsl, perr)
	}
	if !strings.Contains(string(b), `"keyspace":"customer"`) || !strings.Contains(string(b), `"sku"`) {
		t.Errorf("round-tripped DSL = %s", b)
	}
}

// TestIndexCreateRefusesFlatDatastore: .index create on a flat/grab-bag datastore
// refuses honestly (secondary indexes need a <ns>/<keyspace> layout) rather than
// claiming success and writing an orphan catalog.
func TestIndexCreateRefusesFlatDatastore(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "orgs.csv"), []byte("id\n1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	sess, err := glue.OpenSession(root, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", sess: sess, ns: "default", dir: root, out: &out, stderr: &errb}

	c.cmdIndexCreate("ix on orgs (id)")

	if !strings.Contains(errb.String(), "aren't supported") {
		t.Errorf("expected an honest refusal, got: %q", errb.String())
	}
	if strings.Contains(errb.String(), "created") {
		t.Errorf("must not claim success: %q", errb.String())
	}
	if _, serr := os.Stat(filepath.Join(root, ".n1k1", "catalog.json")); !os.IsNotExist(serr) {
		t.Errorf("no catalog.json should be written on refusal (stat err: %v)", serr)
	}
}

// TestIndexSuggestQuotesSpacedField: a field whose name has a space is backticked
// in both the catalog fragment (a key expression) and the .index create command,
// and the emitted command round-trips through parseCreateDSL.
func TestIndexSuggestQuotesSpacedField(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "default", "people")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 12; i++ {
		doc := fmt.Sprintf(`{"full name":"Person-%04d","kind":"x"}`, i)
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("p%02d.json", i)), []byte(doc), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	sess, err := glue.OpenSession(root, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", sess: sess, ns: "default", out: &out, stderr: &errb}
	c.cmdIndexSuggest("people")

	// Catalog fragment: the key must be the backticked expression.
	if !strings.Contains(out.String(), "`full name`") {
		t.Errorf("catalog fragment should backtick the spaced key; got:\n%s", out.String())
	}
	// .index create command: keyspace + key backticked, and it round-trips.
	var line string
	for _, ln := range strings.Split(errb.String(), "\n") {
		if strings.Contains(ln, ".index create") {
			line = strings.TrimSpace(ln)
		}
	}
	if !strings.Contains(line, "(`full name`)") {
		t.Fatalf("create command should backtick the spaced key; got %q", line)
	}
	b, perr := parseCreateDSL(strings.TrimPrefix(line, ".index create "))
	if perr != nil {
		t.Fatalf("emitted command not valid create-DSL (%q): %v", line, perr)
	}
	if !strings.Contains(string(b), `"keyspace":"people"`) || !strings.Contains(string(b), "`full name`") {
		t.Errorf("round-trip = %s", b)
	}
}
