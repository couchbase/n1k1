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
	"reflect"
	"strings"
	"testing"
)

// TestParseRecipeFull: a recipe with front-matter + fixture + expect parses into all
// the expected fields; the label becomes the Label, the SQL body is stripped of front-
// matter and both sections, and the fixture rows + golden findings round-trip.
func TestParseRecipeFull(t *testing.T) {
	text := `-- label: ET-12345
-- description: disk-full errors
-- source: logs
-- gate: l.sev = "ERROR"
-- tags: ["disk","io"]
SELECT l.msg, l.ts FROM logs l WHERE l.sev = "ERROR"
-- @fixture
{"sev":"ERROR","msg":"disk full","ts":3}
{"sev":"WARN","msg":"ok","ts":5}
{"sev":"ERROR","msg":"oom","ts":9}
-- @expect
{"label":"ET-12345","result":{"sev":"ERROR","msg":"disk full","ts":3}}
{"label":"ET-12345","result":{"sev":"ERROR","msg":"oom","ts":9}}
`
	r, err := ParseRecipe("recipes/disk_full.sql++", text)
	if err != nil {
		t.Fatalf("ParseRecipe: %v", err)
	}
	if r.Label != "ET-12345" {
		t.Errorf("Label = %q, want ET-12345 (from label)", r.Label)
	}
	if r.Source != "logs" || r.Description != "disk-full errors" {
		t.Errorf("Source/Description = %q/%q, want logs/'disk-full errors'", r.Source, r.Description)
	}
	if r.Gate != `l.sev = "ERROR"` {
		t.Errorf("Gate = %q, want l.sev = \"ERROR\"", r.Gate)
	}
	if d := r.AsDetector(); d.Gate != r.Gate || d.Source != r.Source {
		t.Errorf("AsDetector() dropped gate/source: %+v", d)
	}
	if !reflect.DeepEqual(r.Tags, []string{"disk", "io"}) {
		t.Errorf("Tags = %v, want [disk io]", r.Tags)
	}
	if r.Stmt != `SELECT l.msg, l.ts FROM logs l WHERE l.sev = "ERROR"` {
		t.Errorf("Stmt = %q (front-matter/sections not stripped cleanly)", r.Stmt)
	}
	if !r.HasFixture || !r.HasExpect {
		t.Errorf("HasFixture/HasExpect = %v/%v, want true/true", r.HasFixture, r.HasExpect)
	}
	if len(r.Fixture.Rows) != 3 {
		t.Errorf("fixture rows = %d, want 3", len(r.Fixture.Rows))
	}
	if len(r.Fixture.Expect) != 2 || r.Fixture.Expect[0].Label != "ET-12345" {
		t.Errorf("expect findings = %+v, want 2 tagged ET-12345", r.Fixture.Expect)
	}
	if canonicalJSON(r.Fixture.Expect[0].Result) != `{"msg":"disk full","sev":"ERROR","ts":3}` {
		t.Errorf("expect[0].Result = %s", r.Fixture.Expect[0].Result)
	}
}

// TestParseRecipeNoTicketAlias: `label` is the only key that sets the Label. The old
// `ticket` back-compat alias was dropped (n1k1 is pre-1.0, no compatibility promise), so
// `ticket` is now just an ordinary front-matter key stashed in Meta -- and the Label
// falls back to the filename stem.
func TestParseRecipeNoTicketAlias(t *testing.T) {
	r, err := ParseRecipe("recipes/old.sql++", "-- ticket: LEGACY-1\nSELECT * FROM logs")
	if err != nil {
		t.Fatalf("ParseRecipe: %v", err)
	}
	if r.Label != "old" {
		t.Errorf("Label = %q, want the filename stem 'old' (`ticket` is no longer an alias)", r.Label)
	}
	if r.Meta["ticket"] != "LEGACY-1" {
		t.Errorf("`ticket` should be a plain key stashed in Meta, got %v", r.Meta)
	}
}

// TestParseRecipePlain: a plain *.sql++ with NO front-matter and NO sections loads as a
// detector whose Label is the filename stem and whose Stmt is the whole body -- the
// backward-compatible path. A leading non-key:value comment is NOT front-matter (it
// stays in the body).
func TestParseRecipePlain(t *testing.T) {
	text := "-- just a note about this detector\nSELECT * FROM logs WHERE sev = \"ERROR\"\n"
	r, err := ParseRecipe("errors.sql++", text)
	if err != nil {
		t.Fatalf("ParseRecipe: %v", err)
	}
	if r.Label != "errors" {
		t.Errorf("Label = %q, want errors (filename stem)", r.Label)
	}
	if r.HasFixture || r.HasExpect {
		t.Errorf("plain recipe should have no fixture/expect; got %v/%v", r.HasFixture, r.HasExpect)
	}
	if r.Source != "" {
		t.Errorf("Source = %q, want empty", r.Source)
	}
	// The whole body (including the leading prose comment) is the statement.
	if !strings.Contains(r.Stmt, "just a note") || !strings.Contains(r.Stmt, "SELECT * FROM logs") {
		t.Errorf("Stmt = %q, want the whole body (comment + SQL)", r.Stmt)
	}
}

// TestLoadCorpus: a dir of *.sql++ recipes loads sorted, mixing a full recipe and a
// plain one; an empty dir errors (a silent no-op corpus would read as clean).
func TestLoadCorpus(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("a_full.sql++", "-- label: ET-1\n-- source: logs\nSELECT * FROM logs\n-- @fixture\n{\"x\":1}\n")
	write("b_plain.sql++", "SELECT * FROM events\n")

	recipes, err := LoadCorpus(dir)
	if err != nil {
		t.Fatalf("LoadCorpus: %v", err)
	}
	if len(recipes) != 2 {
		t.Fatalf("loaded %d recipes, want 2", len(recipes))
	}
	if recipes[0].Label != "ET-1" || !recipes[0].HasFixture {
		t.Errorf("recipe[0] = %+v, want Label ET-1 with a fixture", recipes[0])
	}
	if recipes[1].Label != "b_plain" || recipes[1].HasFixture {
		t.Errorf("recipe[1] = %+v, want Label b_plain with no fixture", recipes[1])
	}

	if _, err := LoadCorpus(t.TempDir()); err == nil {
		t.Error("empty corpus dir should error (a silent no-op corpus reads as clean)")
	}
}

// TestRewriteExpectRoundTrip: RewriteExpect records a golden into a fixture-only recipe
// (leaving everything before @expect byte-identical), and re-parsing yields exactly
// those findings. A second RewriteExpect over an existing @expect replaces it in place.
func TestRewriteExpectRoundTrip(t *testing.T) {
	head := "-- label: ET-9\n-- source: logs\nSELECT * FROM logs l WHERE l.sev = \"ERROR\"\n-- @fixture\n{\"sev\":\"ERROR\"}\n"

	findings := []Finding{
		{Label: "ET-9", Result: json.RawMessage(`{"sev":"ERROR","n":1}`)},
	}
	out := RewriteExpect(head, findings)

	// Everything before the appended @expect block is byte-identical.
	if !strings.HasPrefix(out, head) {
		t.Fatalf("RewriteExpect changed the head:\n%q", out)
	}
	if !strings.Contains(out, markerExpect) {
		t.Fatal("RewriteExpect did not add an @expect marker")
	}

	r, err := ParseRecipe("x.sql++", out)
	if err != nil {
		t.Fatalf("re-parse: %v", err)
	}
	if !r.HasExpect || len(r.Fixture.Expect) != 1 || r.Fixture.Expect[0].Label != "ET-9" {
		t.Fatalf("recorded golden did not round-trip: %+v", r.Fixture.Expect)
	}

	// Replacing an existing @expect keeps the head and swaps the block.
	out2 := RewriteExpect(out, []Finding{{Label: "ET-9", Result: json.RawMessage(`{"sev":"ERROR","n":2}`)}})
	if !strings.HasPrefix(out2, head) {
		t.Fatalf("second RewriteExpect changed the head:\n%q", out2)
	}
	r2, _ := ParseRecipe("x.sql++", out2)
	if canonicalJSON(r2.Fixture.Expect[0].Result) != `{"n":2,"sev":"ERROR"}` {
		t.Fatalf("in-place replace failed: %s", r2.Fixture.Expect[0].Result)
	}
}

// TestDiffFindings: equal sets diff clean regardless of order and result key order;
// a missing and an unexpected finding are reported on their respective sides.
func TestDiffFindings(t *testing.T) {
	expected := []Finding{
		{Label: "T", Result: json.RawMessage(`{"a":1,"b":2}`)},
		{Label: "T", Result: json.RawMessage(`{"a":3}`)},
	}
	// Same set, reordered + different key order in result -> no diff.
	actual := []Finding{
		{Label: "T", Result: json.RawMessage(`{"a":3}`)},
		{Label: "T", Result: json.RawMessage(`{"b":2,"a":1}`)},
	}
	if m, u := DiffFindings(expected, actual); len(m) != 0 || len(u) != 0 {
		t.Fatalf("equal sets diffed: missing=%v unexpected=%v", m, u)
	}

	// Drop one expected, add one unexpected.
	actual2 := []Finding{
		{Label: "T", Result: json.RawMessage(`{"a":1,"b":2}`)},
		{Label: "T", Result: json.RawMessage(`{"a":99}`)},
	}
	m, u := DiffFindings(expected, actual2)
	if len(m) != 1 || canonicalJSON(m[0].Result) != `{"a":3}` {
		t.Fatalf("missing = %v, want the {a:3} finding", m)
	}
	if len(u) != 1 || canonicalJSON(u[0].Result) != `{"a":99}` {
		t.Fatalf("unexpected = %v, want the {a:99} finding", u)
	}
}

// TestRunFixture: a recipe's fixture runs end-to-end -- RunFixture builds a temp
// keyspace named after `source`, runs the detector, and returns whole-row findings for
// the matching rows. A rejected (broken-SQL) detector surfaces as an error (not a silent
// zero-findings pass). A fixture without `source` errors.
func TestRunFixture(t *testing.T) {
	r, err := ParseRecipe("d.sql++", `-- label: ET-1
-- source: logs
SELECT * FROM logs l WHERE l.sev = "ERROR"
-- @fixture
{"sev":"ERROR","msg":"a"}
{"sev":"INFO","msg":"b"}
{"sev":"ERROR","msg":"c"}
`)
	if err != nil {
		t.Fatal(err)
	}
	findings, err := r.RunFixture()
	if err != nil {
		t.Fatalf("RunFixture: %v", err)
	}
	if len(findings) != 2 {
		t.Fatalf("findings = %d, want 2 (the ERROR rows)", len(findings))
	}
	for _, f := range findings {
		if f.Label != "ET-1" {
			t.Errorf("finding label = %q, want ET-1", f.Label)
		}
	}

	// A broken detector must FAIL loudly, not run to a false clean 0 findings.
	broken, _ := ParseRecipe("b.sql++", "-- source: logs\nSELECT FROM WHERE (((\n-- @fixture\n{\"x\":1}\n")
	if _, err := broken.RunFixture(); err == nil {
		t.Error("a rejected detector's fixture must error, not pass with 0 findings")
	}

	// A fixture with no source can't be placed into a keyspace.
	noSrc, _ := ParseRecipe("n.sql++", "SELECT * FROM logs\n-- @fixture\n{\"x\":1}\n")
	if _, err := noSrc.RunFixture(); err == nil {
		t.Error("a fixture without `source` must error")
	}
}
