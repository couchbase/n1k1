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
//        -- label: ET-12345       (becomes the detector Tag)
//        -- severity: high
//        -- source: logs          (the LOGICAL keyspace this detector targets)
//        -- versions: ["7.2","7.6"]   (scalar or inline JSON)
//        -- tags: ["disk","io"]
//   2. The SQL++ DETECTOR statement itself (everything after the front-matter, up to
//      the first section marker).
//   3. A GOLDEN FIXTURE, inline, behind two markers:
//        -- @fixture   -> JSONL input rows for the detector's `source` keyspace
//        -- @expect    -> the golden findings, one {"tag":...,"evidence":...} per line
//
// Example (round-trips through ParseRecipe / RewriteExpect):
//
//	-- label: ET-12345
//	-- severity: high
//	-- source: logs
//	-- versions: ["7.2","7.6"]
//	SELECT l.msg, l.ts FROM logs l WHERE l.sev = "ERROR"
//	-- @fixture
//	{"sev":"ERROR","msg":"disk full","ts":3}
//	{"sev":"WARN","msg":"ok","ts":5}
//	{"sev":"ERROR","msg":"oom","ts":9}
//	-- @expect
//	{"tag":"ET-12345","evidence":{"msg":"disk full","ts":3}}
//	{"tag":"ET-12345","evidence":{"msg":"oom","ts":9}}
//
// (Evidence is the detector's SELECT projection -- here {msg, ts}, not the whole
// matched row; a `SELECT *` detector's evidence is the whole matched doc.)
//
// BACKWARD COMPATIBLE: front-matter and both sections are OPTIONAL. A plain `.sql++`
// file with none of them loads as a detector whose Tag is the filename stem and whose
// Stmt is the whole body -- exactly the pre-phase-7 corpus loader's behavior.
//
// SCOPE (MVP, deferred -- noted here and honored by RunFixture / .rules test):
//   - MULTI-KEYSPACE fixtures: a fixture feeds the detector's single `source` keyspace
//     only. A detector that joins/correlates a second keyspace can't be fixtured yet.
//   - VERSION-specific selection: `versions` is parsed + reported but not yet used to
//     filter which detector/fixture applies to a bundle's software version.
//   - SHA-keyed cache / re-run delta: unrelated build-economics concerns.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Recipe is one parsed detector recipe: the SQL++ statement plus its front-matter
// metadata and (optional) golden fixture. Tag + Stmt are all CorpusCompile / CorpusLint
// need (see AsDetector); the rest drives routing (Source), reporting (Severity,
// Versions, Tags, Meta), and the golden-fixture test harness (Fixture / HasFixture /
// HasExpect).
type Recipe struct {
	Tag      string   // detector id: the `label` front-matter (`ticket` alias), else the filename stem.
	Stmt     string   // the SQL++ detector statement (front-matter + fixture/expect stripped).
	Source   string   // `source` front-matter: the LOGICAL keyspace this detector targets.
	Severity string   // `severity` front-matter (advisory, reported).
	Versions []string // `versions` front-matter (software versions; parsed, reporting-only for now).
	Tags     []string // `tags` front-matter (freeform labels).
	Gate     string   // `gate` front-matter: a cheap NECESSARY precondition (a boolean SQL++
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
	Expect []Finding // golden findings ({tag, evidence}); empty when @expect is absent.
}

// AsDetector projects a Recipe onto the CorpusDetector{Tag,Stmt} that CorpusCompile /
// CorpusLint consume -- the bridge that lets the richer recipe format feed the existing
// corpus machinery unchanged.
func (r *Recipe) AsDetector() CorpusDetector {
	return CorpusDetector{Tag: r.Tag, Stmt: r.Stmt, Source: r.Source, Gate: r.Gate}
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

// section markers -- recognized only as their own (trimmed) line.
const (
	markerFixture = "-- @fixture"
	markerExpect  = "-- @expect"
)

// ParseRecipe parses one recipe file's text (see the file header for the format). path
// supplies the fallback Tag (filename stem) and the recorded provenance. It never fails
// on a plain `.sql++` (no front-matter / no fixture); a parse error is returned only for
// a malformed @expect finding (bad JSON) so a broken golden is loud, not silently empty.
func ParseRecipe(path, text string) (Recipe, error) {
	r := Recipe{
		Path: path,
		Tag:  strings.TrimSuffix(filepath.Base(path), ".sql++"),
		Meta: map[string]string{},
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

	// Fixture rows: each non-blank, non-comment line is one JSON document.
	for _, ln := range fixtureRaw {
		if ln == "" || strings.HasPrefix(ln, "--") {
			continue
		}
		r.Fixture.Rows = append(r.Fixture.Rows, []byte(ln))
	}

	// Expected findings: each non-blank, non-comment line is one {"tag","evidence"}.
	for _, ln := range expectRaw {
		if ln == "" || strings.HasPrefix(ln, "--") {
			continue
		}
		var f findingJSON
		if err := json.Unmarshal([]byte(ln), &f); err != nil {
			return r, fmt.Errorf("@expect: bad finding %q: %v", ln, err)
		}
		r.Fixture.Expect = append(r.Fixture.Expect, Finding{Tag: f.Tag, Evidence: f.Evidence})
	}

	return r, nil
}

// findingJSON is the on-disk shape of an @expect / .rules run finding row.
type findingJSON struct {
	Tag      string          `json:"tag"`
	Evidence json.RawMessage `json:"evidence"`
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
// other key is stashed in Meta (reported, not interpreted). `label` becomes the Tag
// (`ticket` is still accepted as a back-compat alias); `versions` / `tags` accept
// either a JSON array (["7.2","7.6"]) or a comma-separated scalar (7.2, 7.6).
func (r *Recipe) applyFrontMatter(key, val string) {
	switch strings.ToLower(key) {
	case "label", "ticket": // `ticket` is the pre-rename alias for `label`.
		if val != "" {
			r.Tag = val
		}
	case "source":
		r.Source = val
	case "gate":
		r.Gate = val
	case "severity":
		r.Severity = val
	case "versions":
		r.Versions = parseListValue(val)
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
// replaced by the given findings, serialized one canonical {"tag","evidence"} per line
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

// serializeFindings renders findings as one canonical {"tag","evidence"} JSON object
// per line, sorted by (tag, evidence) so the recorded golden is deterministic (findings
// order is not guaranteed at run time -- see CompiledCorpus.Run).
func serializeFindings(findings []Finding) string {
	sorted := make([]Finding, len(findings))
	copy(sorted, findings)
	sort.Slice(sorted, func(a, b int) bool { return findingKey(sorted[a]) < findingKey(sorted[b]) })

	var b strings.Builder
	for _, f := range sorted {
		tag, _ := json.Marshal(f.Tag)
		b.WriteString(`{"tag":`)
		b.Write(tag)
		b.WriteString(`,"evidence":`)
		b.WriteString(canonicalJSON(f.Evidence))
		b.WriteString("}\n")
	}
	return b.String()
}

// DiffFindings compares expected vs actual findings as SORTED SETS (findings order is
// not guaranteed -- see CompiledCorpus.Run), returning the missing (expected but not
// produced) and unexpected (produced but not expected) findings. Evidence is compared
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

// findingKey is a finding's canonical set-membership key: its tag joined to its
// canonicalized evidence (sorted JSON keys), so semantically-equal findings collapse.
func findingKey(f Finding) string {
	return f.Tag + "\x00" + canonicalJSON(f.Evidence)
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
