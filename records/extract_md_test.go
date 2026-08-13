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

package records

import (
	"path/filepath"
	"strings"
	"testing"
)

// TestExtractMarkdownFrontmatter covers mdExtract's YAML frontmatter split -- the
// Jekyll / Hugo / Obsidian / Google-OKF `---` convention. The contract is ADDITIVE:
// `text` stays the whole file so existing LIKE/FTS queries are unaffected, plus
// `front` (the parsed mapping) and `body` (markdown after the closing fence); a
// fenced block that will not parse reports `front_error` rather than failing the scan.
func TestExtractMarkdownFrontmatter(t *testing.T) {
	dir := t.TempDir()

	// 1) The happy path: scalars, a list, a nested map, and a list of maps.
	okf := "---\ntype: table\ntitle: Orders\ntags: [sales, core]\n" +
		"generated:\n  by: dbt\n  at: 2026-07-01T10:00:00Z\n" +
		"sources:\n  - resource: bq://p/d/t\n    author: data-eng\n---\n\n# Schema\n\nprose\n"
	m := extractOne(t, writeFile(t, filepath.Join(dir, "orders.md"), []byte(okf)))

	if m["text"] != strings.TrimSpace(okf) {
		t.Errorf("text must stay the WHOLE file (backward compatible); got %q", m["text"])
	}
	if got, want := m["body"], "# Schema\n\nprose"; got != want {
		t.Errorf("body = %q, want %q", got, want)
	}
	front, ok := m["front"].(map[string]interface{})
	if !ok {
		t.Fatalf("front = %#v, want a map", m["front"])
	}
	if front["type"] != "table" || front["title"] != "Orders" {
		t.Errorf("front scalars = %#v", front)
	}
	if tags, _ := front["tags"].([]interface{}); len(tags) != 2 || tags[0] != "sales" {
		t.Errorf("front.tags = %#v, want [sales core]", front["tags"])
	}
	if gen, _ := front["generated"].(map[string]interface{}); gen == nil || gen["by"] != "dbt" {
		t.Errorf("front.generated = %#v, want a nested map with by=dbt", front["generated"])
	}
	if src, _ := front["sources"].([]interface{}); len(src) != 1 {
		t.Errorf("front.sources = %#v, want one entry", front["sources"])
	} else if e, _ := src[0].(map[string]interface{}); e == nil || e["resource"] != "bq://p/d/t" {
		t.Errorf("front.sources[0] = %#v", src[0])
	}
	if _, bad := m["front_error"]; bad {
		t.Errorf("unexpected front_error: %v", m["front_error"])
	}

	// 2) Shapes that must NOT be treated as frontmatter, the error paths, and the
	//    spellings that must be tolerated.
	bom := "\ufeff"
	for _, tc := range []struct {
		name, body    string
		wantFront     bool // a parsed `front` mapping
		wantBody      bool // a `body` field, i.e. a complete fence pair was seen
		wantErrSubstr string
	}{
		{name: "plain.md", body: "# Title\n\nno frontmatter\n"},
		// A leading `---` with no closing fence is a horizontal rule, not frontmatter.
		{name: "hrule.md", body: "---\n\njust an hr\n"},
		// Fenced but invalid YAML -> reported, not silently dropped; body still split.
		{name: "broken.md", body: "---\ntags: [a, b\n---\n\nprose\n",
			wantBody: true, wantErrSubstr: "yaml:"},
		// Fenced but not a mapping (a sequence) -> same treatment.
		{name: "seq.markdown", body: "---\n- a\n- b\n---\n\nprose\n",
			wantBody: true, wantErrSubstr: "not a YAML mapping"},
		// Tolerated: a UTF-8 BOM, CRLF line ends, and YAML's own `...` end marker.
		{name: "bom.md", body: bom + "---\ntype: t\n---\n\nprose\n", wantFront: true, wantBody: true},
		{name: "crlf.md", body: "---\r\ntype: t\r\n---\r\n\r\nprose\r\n", wantFront: true, wantBody: true},
		{name: "dots.md", body: "---\ntype: t\n...\n\nprose\n", wantFront: true, wantBody: true},
	} {
		got := extractOne(t, writeFile(t, filepath.Join(dir, tc.name), []byte(tc.body)))
		if _, has := got["front"]; has != tc.wantFront {
			t.Errorf("%s: front present = %v, want %v (front=%#v)", tc.name, has, tc.wantFront, got["front"])
		}
		if _, has := got["body"]; has != tc.wantBody {
			t.Errorf("%s: body present = %v, want %v", tc.name, has, tc.wantBody)
		}
		errStr, _ := got["front_error"].(string)
		if tc.wantErrSubstr == "" && errStr != "" {
			t.Errorf("%s: unexpected front_error %q", tc.name, errStr)
		}
		if tc.wantErrSubstr != "" && !strings.Contains(errStr, tc.wantErrSubstr) {
			t.Errorf("%s: front_error = %q, want it to contain %q", tc.name, errStr, tc.wantErrSubstr)
		}
		// text is always the verbatim file (whitespace-trimmed) -- never lossy. A BOM
		// is part of the file, so it stays in `text`, exactly as textExtract leaves it
		// for .txt/.log; only the frontmatter split looks past it, so `front`/`body`
		// are BOM-free.
		if want := strings.TrimSpace(tc.body); got["text"] != want {
			t.Errorf("%s: text = %q, want %q", tc.name, got["text"], want)
		}
	}
}
