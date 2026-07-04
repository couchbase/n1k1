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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/couchbase/n1k1/glue"
)

func TestParseCreateDSL(t *testing.T) {
	type def struct {
		Name     string
		Keyspace string
		Keys     []string
		Where    string
	}
	type cat struct{ Indexes []def }
	parse := func(s string) (def, error) {
		b, err := parseCreateDSL(s)
		if err != nil {
			return def{}, err
		}
		var c cat
		if e := json.Unmarshal(b, &c); e != nil || len(c.Indexes) != 1 {
			t.Fatalf("parseCreateDSL(%q) bad json %s: %v", s, b, e)
		}
		return c.Indexes[0], nil
	}

	if d, err := parse("byTotal on orders (total)"); err != nil ||
		d.Name != "byTotal" || d.Keyspace != "orders" ||
		len(d.Keys) != 1 || d.Keys[0] != "total" || d.Where != "" {
		t.Errorf("simple = %+v err %v", d, err)
	}
	// Multi-key (top-level commas only; brackets/parens protect inner commas) + WHERE.
	if d, err := parse("x on ks (a, b[0], c(1,2)) where amount > 1"); err != nil ||
		len(d.Keys) != 3 || d.Keys[0] != "a" || d.Keys[1] != "b[0]" || d.Keys[2] != "c(1,2)" ||
		d.Where != "amount > 1" {
		t.Errorf("multi+where = %+v err %v", d, err)
	}
	// Backticked keyspace (spaces) unquotes to the plain name; a backticked key
	// (spaces) stays a key expression.
	if d, err := parse("ix on `Sales Transaction` (`full name`)"); err != nil ||
		d.Keyspace != "Sales Transaction" || len(d.Keys) != 1 || d.Keys[0] != "`full name`" {
		t.Errorf("backticked = %+v err %v", d, err)
	}

	bad := []string{
		"noparens",            // no '('
		"x on ks",             // no '('
		"x ks (a)",            // head not "<name> on <keyspace>"
		"x on ks (a) junk",    // trailing text isn't WHERE
		"x on ks (a",          // unbalanced parens
		"x on ks ()",          // no key expressions
		"x on ks (a) where  ", // WHERE with empty expr
	}
	for _, s := range bad {
		if _, err := parseCreateDSL(s); err == nil {
			t.Errorf("parseCreateDSL(%q) = nil error, want error", s)
		}
	}
}

func TestMatchParen(t *testing.T) {
	cases := []struct {
		s    string
		open int
		want int
	}{
		{"(a)", 0, 2},
		{"a(b(c)d)e", 1, 7},
		{"(", 0, -1},   // unbalanced
		{"()", 0, 1},   // empty
		{"(())", 0, 3}, // nested
	}
	for _, tc := range cases {
		if got := matchParen(tc.s, tc.open); got != tc.want {
			t.Errorf("matchParen(%q,%d) = %d, want %d", tc.s, tc.open, got, tc.want)
		}
	}
}

func TestSplitTopLevelCommas(t *testing.T) {
	cases := map[string][]string{
		"a, b, c":           {"a", "b", "c"},
		"a, b[0,1], c(x,y)": {"a", "b[0,1]", "c(x,y)"}, // nested commas protected
		"a,,b":              {"a", "b"},                // empties dropped
		"  solo  ":          {"solo"},
		"":                  nil,
	}
	for in, want := range cases {
		got := splitTopLevelCommas(in)
		if len(got) != len(want) {
			t.Errorf("splitTopLevelCommas(%q) = %v, want %v", in, got, want)
			continue
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("splitTopLevelCommas(%q)[%d] = %q, want %q", in, i, got[i], want[i])
			}
		}
	}
}

func TestFieldsBacktickAwareAndUnquote(t *testing.T) {
	toks := fieldsBacktickAware("ix on `Sales Transaction v.4a`")
	if len(toks) != 3 || toks[0] != "ix" || toks[1] != "on" || toks[2] != "`Sales Transaction v.4a`" {
		t.Fatalf("fieldsBacktickAware = %q", toks)
	}
	// A doubled backtick inside quotes is a literal ` and doesn't end the span.
	toks = fieldsBacktickAware("n on `a``b c`")
	if len(toks) != 3 || toks[2] != "`a``b c`" {
		t.Fatalf("escaped backtick tokens = %q", toks)
	}
	cases := map[string]string{
		"`Sales Transaction`": "Sales Transaction",
		"`a``b`":              "a`b",
		"plain":               "plain",
		"`x`":                 "x",
	}
	for in, want := range cases {
		if got := unquoteIdent(in); got != want {
			t.Errorf("unquoteIdent(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestHumanBytes(t *testing.T) {
	cases := map[int64]string{
		0:       "0B",
		512:     "512B",
		1024:    "1.0KB",
		1536:    "1.5KB",
		1048576: "1.0MB",
	}
	for n, want := range cases {
		if got := humanBytes(n); got != want {
			t.Errorf("humanBytes(%d) = %q, want %q", n, got, want)
		}
	}
}

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
	c := &cli{prog: "n1k1", sess: sess, out: &out, stderr: &errb}

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
	c := &cli{prog: "n1k1", sess: sess, dir: root, out: &out, stderr: &errb}

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
	c := &cli{prog: "n1k1", sess: sess, out: &out, stderr: &errb}
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
