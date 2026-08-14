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
		Name      string
		Namespace string
		Keyspace  string
		Keys      []string
		Where     string
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

	// Namespace-qualified keyspaces (ISSUE-done-12: `ns`:`ks` used to be swallowed
	// whole into the keyspace name, backticks included, under namespace "default").
	if d, err := parse("ix on proj:sess (type)"); err != nil ||
		d.Namespace != "proj" || d.Keyspace != "sess" {
		t.Errorf("ns:ks = %+v err %v", d, err)
	}
	if d, err := parse("ix_t on `-Users-x-com`:`0c62f774-ab` (type)"); err != nil ||
		d.Namespace != "-Users-x-com" || d.Keyspace != "0c62f774-ab" {
		t.Errorf("backticked ns:ks = %+v err %v", d, err)
	}
	// A colon INSIDE backticks is not a qualifier.
	if d, err := parse("ix on `a:b` (x)"); err != nil ||
		d.Namespace != "" || d.Keyspace != "a:b" {
		t.Errorf("colon-in-ticks = %+v err %v", d, err)
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
// TestIndexCreateFlatDatastore: a flat/grab-bag layout is INDEXABLE (ISSUE-27 --
// the index side reads the keyspace's own records source), and without an
// -index-store the CLI notes the sidecar will land inside the data dir.
func TestIndexCreateFlatDatastore(t *testing.T) {
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

	if !strings.Contains(errb.String(), "created ix") {
		t.Errorf("flat layouts are indexable now; got: %q", errb.String())
	}
	if !strings.Contains(errb.String(), "-index-store") {
		t.Errorf("without -index-store the sidecar lands in the data dir -- expected the advisory note; got: %q", errb.String())
	}
	if _, serr := os.Stat(filepath.Join(root, ".n1k1", "catalog.json")); serr != nil {
		t.Errorf("catalog.json should be written: %v", serr)
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

// TestIndexCreateNamespacedAndFailLoud replays ISSUE-done-12's transcript: the DDL
// form addresses a <ns>:<ks> layout (project-slug/session style), and a keyspace
// that does not resolve REFUSES at create — the way --bind refuses a logical
// keyspace matching zero files — instead of reporting "created" for an index that
// can never build (and writing the broken def into the catalog).
func TestIndexCreateNamespacedAndFailLoud(t *testing.T) {
	root := t.TempDir()
	ksDir := filepath.Join(root, "proj-slug", "sess-uuid")
	if err := os.MkdirAll(ksDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ksDir, "e.jsonl"),
		[]byte(`{"type":"a"}`+"\n"+`{"type":"b"}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	sess, err := glue.OpenSession(root, "default")
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()

	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", mode: "jsonlines", out: &out, stderr: &errb, dir: root, sess: sess}

	// A keyspace that resolves nowhere: refused, nothing written.
	c.cmdIndexCreate("ix_bogus on `proj-slug`:`nope` (type)")
	if !c.failed || !strings.Contains(errb.String(), "not found") {
		t.Fatalf("unresolvable keyspace must refuse: %q", errb.String())
	}
	if _, err := os.Stat(filepath.Join(root, ".n1k1", "catalog.json")); err == nil {
		t.Fatalf("refused create must write nothing")
	}

	// The real ns:ks: created, built, and the def carries the namespace.
	c.failed = false
	errb.Reset()
	c.cmdIndexCreate("ix_type on `proj-slug`:`sess-uuid` (type)")
	if c.failed || !strings.Contains(errb.String(), "created ix_type") {
		t.Fatalf("namespaced create failed: %q", errb.String())
	}
	b, err := os.ReadFile(filepath.Join(root, ".n1k1", "catalog.json"))
	if err != nil {
		t.Fatal(err)
	}
	var cat struct {
		Indexes []struct {
			Name      string `json:"name"`
			Namespace string `json:"namespace"`
			Keyspace  string `json:"keyspace"`
		} `json:"indexes"`
	}
	if err := json.Unmarshal(b, &cat); err != nil || len(cat.Indexes) != 1 {
		t.Fatalf("catalog: %s (%v)", b, err)
	}
	if d := cat.Indexes[0]; d.Namespace != "proj-slug" || d.Keyspace != "sess-uuid" || d.Name != "ix_type" {
		t.Fatalf("def mis-filed (the ISSUE-done-12 shape): %+v", d)
	}
	// And it actually builds (the old defect's defs could never build).
	if !strings.Contains(errb.String(), "created ix_type") || strings.Contains(errb.String(), "no keyspace") {
		t.Fatalf("build after create: %q", errb.String())
	}
}
