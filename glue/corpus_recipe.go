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

// corpus_recipe.go is PREPARE++ phase 7's TESTABLE detector recipe format
// (DESIGN-prepare.md "AI-authored recipes need a test harness first" +
// "Shaping SQL++ for fusion + authoring"). A recipe is a SINGLE file (best for git
// provenance) that carries three things at once:
//
//   1. FRONT-MATTER metadata as leading `-- key: value` SQL comments (so it parses
//      with no YAML/heavy dep and a plain reader still sees valid SQL comments):
//        -- label: ET-12345       (becomes the query Label)
//        -- description: disk-full errors   (a free-form summary)
//        -- source: logs          (the LOGICAL keyspace this query targets)
//        -- tags: ["disk","io"]   (scalar or inline JSON)
//   2. The SQL++ QUERY statement itself (everything after the front-matter, up to
//      the first section marker).
//   3. A GOLDEN FIXTURE, inline, behind two markers. The DATA lines are themselves
//      SQL comments (`-- {...}`), so the whole file stays valid SQL++ -- a plain SQL
//      reader/highlighter sees only comments plus the one SELECT:
//        -- @fixture   -> JSONL input rows for the query's `source` keyspace
//        -- @expect    -> the golden findings, one {"label":...,"result":...} per line
//
// Example (round-trips through ParseRecipe / RewriteExpect):
//
//	-- label: ET-12345
//	-- description: disk-full errors
//	-- source: logs
//	-- tags: ["disk","io"]
//	SELECT l.msg, l.ts FROM logs l WHERE l.sev = "ERROR"
//	-- @fixture
//	-- {"sev":"ERROR","msg":"disk full","ts":3}
//	-- {"sev":"WARN","msg":"ok","ts":5}
//	-- {"sev":"ERROR","msg":"oom","ts":9}
//	-- @expect
//	-- {"label":"ET-12345","result":{"msg":"disk full","ts":3}}
//	-- {"label":"ET-12345","result":{"msg":"oom","ts":9}}
//
// (Result is the query's SELECT projection -- here {msg, ts}, not the whole matched
// row; a `SELECT *` query's result is the whole matched doc. The parser also still
// accepts bare, un-commented JSON data lines.)
//
// Front-matter and both sections are OPTIONAL. A plain `.sql++` file with none of them
// loads as a query whose Label is the filename stem and whose Stmt is the whole body.
//
// SCOPE (MVP, deferred -- noted here and honored by RunFixture / .rules test):
//   - MULTI-KEYSPACE fixtures: a fixture feeds the query's single `source` keyspace
//     only. A query that joins/correlates a second keyspace can't be fixtured yet.
//   - SHA-keyed cache / re-run delta: unrelated build-economics concerns.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Recipe is one parsed query recipe: the SQL++ statement plus its front-matter
// metadata and (optional) golden fixture. Label + Stmt are all CorpusCompile / CorpusLint
// need (see AsDetector); the rest drives routing (Source), reporting (Description, Tags,
// Meta), and the golden-fixture test harness (Fixture / HasFixture / HasExpect).
type Recipe struct {
	Label       string   // query id: the `label` front-matter, else the filename stem.
	Stmt        string   // the SQL++ query statement (front-matter + fixture/expect stripped).
	Source      string   // `source` front-matter: the LOGICAL keyspace this query targets.
	Description string   // `description` front-matter: a free-form summary (advisory, reported).
	Tags        []string // `tags` front-matter: freeform labels (a JSON array or comma-separated).
	Gate        string   // `gate` front-matter: a cheap NECESSARY precondition (a boolean SQL++
	// expression over the Source keyspace). A STANDALONE detector (window / GROUP BY / join --
	// one that gets its own scan, not the fused shared scan) is SKIPPED when its Source has no
	// row satisfying Gate, so an expensive sort/window never runs over a keyspace that cannot
	// possibly match. The author asserts necessity (no finding is possible unless some row
	// satisfies Gate) -- it is the standalone analog of the fused predicate index. Needs Source.

	Fixture    Fixture // the golden fixture: input rows + expected findings (empty if none).
	HasFixture bool    // the `-- @fixture` marker was present.
	HasExpect  bool    // the `-- @expect` marker was present.

	Path string            // the file this recipe was read from (provenance).
	Meta map[string]string // any front-matter key not promoted to a field above (raw string value).
}

// Fixture is a recipe's golden test data: JSONL input Rows fed into the detector's
// `source` keyspace, and the Expect golden findings that running the detector over
// those rows must reproduce (compared as a set -- see DiffFindings).
type Fixture struct {
	Rows   [][]byte  // input rows (one raw-JSON document per fixture line).
	Expect []Finding // golden findings ({label, result}); empty when @expect is absent.
}

// AsDetector projects a Recipe onto the CorpusDetector{Label,Stmt} that CorpusCompile /
// CorpusLint consume -- the bridge that lets the richer recipe format feed the existing
// corpus machinery unchanged.
func (r *Recipe) AsDetector() CorpusDetector {
	return CorpusDetector{Label: r.Label, Stmt: r.Stmt, Source: r.Source, Gate: r.Gate}
}

// LoadCorpus reads every *.sql++ file in dir as one Recipe (see ParseRecipe for the
// format), returned sorted by Path for deterministic output. An empty corpus (no
// *.sql++ files) is an error -- a silent no-op corpus would falsely read as a clean
// bundle. This is the single reusable loader shared by the CLI (.rules run/lint/test),
// the tests, and any future CI driver.
func LoadCorpus(dir string) ([]Recipe, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "*.sql++"))
	if err != nil {
		return nil, fmt.Errorf("scanning corpus %q: %v", dir, err)
	}
	sort.Strings(paths)
	var recipes []Recipe
	for _, p := range paths {
		body, rerr := os.ReadFile(p)
		if rerr != nil {
			return nil, fmt.Errorf("reading %q: %v", p, rerr)
		}
		r, perr := ParseRecipe(p, string(body))
		if perr != nil {
			return nil, fmt.Errorf("parsing recipe %q: %v", p, perr)
		}
		recipes = append(recipes, r)
	}
	if len(recipes) == 0 {
		return nil, fmt.Errorf("no *.sql++ queries in %q", dir)
	}
	return recipes, nil
}

// LoadCorpusDirs loads several corpus dirs and concatenates their recipes, so multiple
// query tiers (e.g. an index-free `detectors/` and a `detectors-indexing/`) compile into
// ONE multi-query pack that fuses over a shared scan -- the point of "multi" (IDEA-0034).
// Each dir must contain at least one *.sql++ (LoadCorpus fails loudly on an empty/typo'd
// dir); a single dir behaves exactly like LoadCorpus.
func LoadCorpusDirs(dirs []string) ([]Recipe, error) {
	if len(dirs) == 0 {
		return nil, fmt.Errorf("no query directory given")
	}
	var all []Recipe
	for _, dir := range dirs {
		recipes, err := LoadCorpus(dir)
		if err != nil {
			return nil, err
		}
		all = append(all, recipes...)
	}
	return all, nil
}

// section markers -- recognized only as their own (trimmed) line.
const (
	markerFixture = "-- @fixture"
	markerExpect  = "-- @expect"
)

// ParseRecipe parses one recipe file's text (see the file header for the format). path
// supplies the fallback Label (filename stem) and the recorded provenance. It never fails
// on a plain `.sql++` (no front-matter / no fixture); a parse error is returned only for
// a malformed @expect finding (bad JSON) so a broken golden is loud, not silently empty.
func ParseRecipe(path, text string) (Recipe, error) {
	r := Recipe{
		Path:  path,
		Label: strings.TrimSuffix(filepath.Base(path), ".sql++"),
		Meta:  map[string]string{},
	}

	lines := strings.Split(text, "\n")

	// Phase 1: front-matter -- the run of leading blank / `-- key: value` comment
	// lines. The first line that is neither ends the front-matter and starts the SQL
	// body (that line included). A leading SQL comment that is NOT key:value-shaped
	// (`-- just a note`) simply isn't front-matter: it ends the block and rides along
	// in the SQL body (harmless -- the SQL parser ignores it).
	i := 0
	for ; i < len(lines); i++ {
		ln := strings.TrimSpace(lines[i])
		if ln == "" {
			continue
		}
		key, val, ok := frontMatterKV(ln)
		if !ok {
			break
		}
		r.applyFrontMatter(key, val)
	}

	// Phases 2-4: SQL body, then optional @fixture / @expect sections.
	var stmt []string
	sawFixture, sawExpect := false, false
	var fixtureRaw, expectRaw []string
	section := "stmt"
	for ; i < len(lines); i++ {
		trimmed := strings.TrimSpace(lines[i])
		switch {
		case trimmed == markerFixture:
			section, sawFixture = "fixture", true
			continue
		case trimmed == markerExpect:
			section, sawExpect = "expect", true
			continue
		}
		switch section {
		case "stmt":
			stmt = append(stmt, lines[i])
		case "fixture":
			fixtureRaw = append(fixtureRaw, trimmed)
		case "expect":
			expectRaw = append(expectRaw, trimmed)
		}
	}

	r.Stmt = strings.TrimSpace(strings.Join(stmt, "\n"))
	r.HasFixture = sawFixture
	r.HasExpect = sawExpect

	// Fixture rows: each data line is a JSON document written as an SQL comment
	// (`-- {...}`), so the whole recipe file stays valid SQL++. uncommentFixtureJSON
	// strips the comment prefix; blank lines and prose (non-JSON) comments are skipped.
	for _, ln := range fixtureRaw {
		if j, ok := uncommentFixtureJSON(ln); ok {
			r.Fixture.Rows = append(r.Fixture.Rows, []byte(j))
		}
	}

	// Expected findings: each is one {"label","result"} JSON object, likewise written
	// as an SQL comment line.
	for _, ln := range expectRaw {
		j, ok := uncommentFixtureJSON(ln)
		if !ok {
			continue
		}
		var f findingJSON
		if err := json.Unmarshal([]byte(j), &f); err != nil {
			return r, fmt.Errorf("@expect: bad finding %q: %v", j, err)
		}
		r.Fixture.Expect = append(r.Fixture.Expect, Finding{Label: f.Label, Result: f.Result})
	}

	return r, nil
}

// uncommentFixtureJSON reads one @fixture / @expect line: it strips an optional leading
// SQL comment prefix (`--`) plus surrounding space, and returns the remainder if it looks
// like a JSON value (starts with `{` or `[`). This lets the fixture/expect DATA live in
// SQL comments so the whole *.sql++ file is valid SQL++, while blank lines and prose
// comments (`-- a note`) are skipped. A bare (un-commented) JSON line is still accepted.
func uncommentFixtureJSON(ln string) (string, bool) {
	s := strings.TrimSpace(ln)
	s = strings.TrimSpace(strings.TrimPrefix(s, "--"))
	if s == "" {
		return "", false
	}
	if s[0] == '{' || s[0] == '[' {
		return s, true
	}
	return "", false
}

// findingJSON is the on-disk shape of an @expect / .rules run finding row.
type findingJSON struct {
	Label  string          `json:"label"`
	Result json.RawMessage `json:"result"`
}

// frontMatterKV recognizes a `-- key: value` front-matter comment line, returning the
// key and the (trimmed) value. The key must be a bare identifier (letters/digits/_/-)
// so an ordinary prose comment with a colon (`-- see: the ticket`) is NOT mistaken for
// front-matter (it fails the identifier test and ends the block).
func frontMatterKV(line string) (key, val string, ok bool) {
	if !strings.HasPrefix(line, "--") {
		return "", "", false
	}
	rest := strings.TrimSpace(strings.TrimPrefix(line, "--"))
	colon := strings.IndexByte(rest, ':')
	if colon <= 0 {
		return "", "", false
	}
	key = strings.TrimSpace(rest[:colon])
	if !isIdent(key) {
		return "", "", false
	}
	return key, strings.TrimSpace(rest[colon+1:]), true
}

func isIdent(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-') {
			return false
		}
	}
	return true
}

// applyFrontMatter promotes a recognized front-matter key to its Recipe field; any
// other key is stashed in Meta (reported, not interpreted). `label` becomes the Label;
// `tags` accepts either a JSON array (["disk","io"]) or a comma-separated scalar.
func (r *Recipe) applyFrontMatter(key, val string) {
	switch strings.ToLower(key) {
	case "label":
		if val != "" {
			r.Label = val
		}
	case "source":
		r.Source = val
	case "gate":
		r.Gate = val
	case "description":
		r.Description = val
	case "tags":
		r.Tags = parseListValue(val)
	default:
		r.Meta[key] = val
	}
}

// parseListValue reads a front-matter list value as either inline JSON (a JSON array,
// or a single JSON-quoted string) or a bare comma-separated list. Empties are dropped.
func parseListValue(val string) []string {
	val = strings.TrimSpace(val)
	if val == "" {
		return nil
	}
	if strings.HasPrefix(val, "[") {
		var arr []string
		if err := json.Unmarshal([]byte(val), &arr); err == nil {
			return arr
		}
		// Fall through to comma-splitting on malformed JSON (best effort).
	}
	if strings.HasPrefix(val, "\"") {
		var s string
		if err := json.Unmarshal([]byte(val), &s); err == nil {
			return []string{s}
		}
	}
	var out []string
	for _, part := range strings.Split(val, ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// RewriteExpect returns raw (a recipe file's full text) with its `-- @expect` section
// replaced by the given findings, serialized one canonical {"label","result"} per line
// (sorted for a stable, review-friendly diff). Everything before the expect block is
// left BYTE-IDENTICAL. If the file has a `-- @fixture` but no `-- @expect`, the expect
// block is appended after the fixture (the golden-master capture case). This is the
// --update writer; it is a pure string transform so it is trivially testable.
func RewriteExpect(raw string, findings []Finding) string {
	block := markerExpect + "\n" + serializeFindings(findings)

	lines := strings.Split(raw, "\n")
	for i, ln := range lines {
		if strings.TrimSpace(ln) == markerExpect {
			// Replace from the @expect marker to EOF with the fresh block.
			head := strings.Join(lines[:i], "\n")
			if head != "" {
				head += "\n"
			}
			return head + block
		}
	}

	// No existing @expect: append the block, ensuring a single separating newline.
	out := raw
	if out != "" && !strings.HasSuffix(out, "\n") {
		out += "\n"
	}
	return out + block
}

// serializeFindings renders findings as one canonical {"label","result"} JSON object
// per line, sorted by (label, result) so the recorded golden is deterministic (findings
// order is not guaranteed at run time -- see CompiledCorpus.Run).
func serializeFindings(findings []Finding) string {
	sorted := make([]Finding, len(findings))
	copy(sorted, findings)
	sort.Slice(sorted, func(a, b int) bool { return findingKey(sorted[a]) < findingKey(sorted[b]) })

	var b strings.Builder
	for _, f := range sorted {
		label, _ := json.Marshal(f.Label)
		b.WriteString(`-- {"label":`) // commented so the recipe file stays valid SQL++.
		b.Write(label)
		b.WriteString(`,"result":`)
		b.WriteString(canonicalJSON(f.Result))
		b.WriteString("}\n")
	}
	return b.String()
}

// DiffFindings compares expected vs actual findings as SORTED SETS (findings order is
// not guaranteed -- see CompiledCorpus.Run), returning the missing (expected but not
// produced) and unexpected (produced but not expected) findings. Result is compared
// canonically (JSON re-serialized with sorted keys), so object key order / whitespace
// differences never cause a spurious diff. A recipe PASSES iff both slices are empty.
func DiffFindings(expected, actual []Finding) (missing, unexpected []Finding) {
	exp := map[string]int{}
	for _, f := range expected {
		exp[findingKey(f)]++
	}
	act := map[string]int{}
	for _, f := range actual {
		act[findingKey(f)]++
	}
	for _, f := range actual {
		k := findingKey(f)
		if exp[k] > 0 {
			exp[k]--
		} else {
			unexpected = append(unexpected, f)
		}
	}
	for _, f := range expected {
		k := findingKey(f)
		if act[k] > 0 {
			act[k]--
		} else {
			missing = append(missing, f)
		}
	}
	sort.Slice(missing, func(a, b int) bool { return findingKey(missing[a]) < findingKey(missing[b]) })
	sort.Slice(unexpected, func(a, b int) bool { return findingKey(unexpected[a]) < findingKey(unexpected[b]) })
	return missing, unexpected
}

// findingKey is a finding's canonical set-membership key: its label joined to its
// canonicalized result (sorted JSON keys), so semantically-equal findings collapse.
func findingKey(f Finding) string {
	return f.Label + "\x00" + canonicalJSON(f.Result)
}

// canonicalJSON re-serializes raw JSON into a canonical form (object keys sorted by
// Go's encoding/json), so two JSON values that differ only in key order or whitespace
// compare equal. Non-parseable bytes are returned trimmed, unchanged (best effort).
func canonicalJSON(raw json.RawMessage) string {
	if len(raw) == 0 {
		return "null"
	}
	var v interface{}
	if err := json.Unmarshal(raw, &v); err != nil {
		return strings.TrimSpace(string(raw))
	}
	b, err := json.Marshal(v)
	if err != nil {
		return strings.TrimSpace(string(raw))
	}
	return string(b)
}
