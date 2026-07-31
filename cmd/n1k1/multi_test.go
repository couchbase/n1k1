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
	"reflect"
	"strings"
	"testing"

	"github.com/couchbase/n1k1/glue"
)

// writeCorpus writes each name->body entry as <dir>/<name>.sql++ and returns dir.
func writeMultiQueryEntries(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(dir, name+".sql++"), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// TestMultiQueriesTagsSelector guards ISSUE-16: --queries-tags / --queries-not-tags
// select a subset of a pack by front-matter tag, report the selection, and fail LOUD
// (never silently run zero) when a tag matches nothing.
func TestMultiQueriesTagsSelector(t *testing.T) {
	root := newLogsBundle(t)
	pack := writeMultiQueryEntries(t, map[string]string{
		"a": "-- label: a\n-- tags: [\"tier1\",\"base\"]\nSELECT * FROM logs l WHERE l.sev = \"ERROR\"",
		"b": "-- label: b\n-- tags: [\"tier1\"]\nSELECT * FROM logs l WHERE l.sev = \"WARN\"",
		"c": "-- label: c\n-- tags: [\"roll\"]\nSELECT COUNT(*) AS n FROM logs l",
	})
	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", mode: "jsonlines", out: &out, stderr: &errb, dir: root}

	errb.Reset()
	c.failed = false
	c.cmdMulti("lint --queries " + pack + " --queries-tags tier1")
	if c.failed || !strings.Contains(errb.String(), "selected 2 of 3") {
		t.Errorf("--queries-tags tier1: want 'selected 2 of 3', failed=%v stderr=%s", c.failed, errb.String())
	}

	errb.Reset()
	c.failed = false
	c.cmdMulti("lint --queries " + pack + " --queries-not-tags roll")
	if !strings.Contains(errb.String(), "selected 2 of 3") {
		t.Errorf("--queries-not-tags roll: want 'selected 2 of 3', stderr=%s", errb.String())
	}

	// a tag that matches nothing must fail loudly and list the available tags.
	errb.Reset()
	c.failed = false
	c.cmdMulti("lint --queries " + pack + " --queries-tags nope")
	if !c.failed {
		t.Errorf("--queries-tags nope should fail (not silently run zero)")
	}
	if !strings.Contains(errb.String(), "available tags") {
		t.Errorf("zero-match should list available tags, stderr=%s", errb.String())
	}
}

// TestMultiShow guards `.multi show`: it prints each *.sql++ query's SQL++ (a viewer +
// an existence/parse check), shows the SQL++ a builtin generates, and notes the native
// builtin has none.
func TestMultiShow(t *testing.T) {
	root := newLogsBundle(t)
	pack := writeMultiQueryEntries(t, map[string]string{
		"a": "-- label: a\n-- tags: [\"t1\"]\nSELECT l.sev FROM logs l",
	})
	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", mode: "jsonlines", out: &out, stderr: &errb, dir: root}
	// -mode jsonlines emits one JSON object per line (NDJSON); parse line-by-line.
	arr := func(cmd string) []map[string]interface{} {
		out.Reset()
		errb.Reset()
		c.failed = false
		c.cmdMulti(cmd)
		var rows []map[string]interface{}
		for _, ln := range strings.Split(strings.TrimSpace(out.String()), "\n") {
			if ln = strings.TrimSpace(ln); ln == "" {
				continue
			}
			var m map[string]interface{}
			if json.Unmarshal([]byte(ln), &m) == nil {
				rows = append(rows, m)
			}
		}
		return rows
	}

	// a dir of *.sql++: each entry's label + SQL++ source.
	rows := arr("show --queries " + pack)
	if c.failed || len(rows) != 1 || rows[0]["label"] != "a" || rows[0]["sql"] == nil {
		t.Fatalf("show dir: %v (stderr %s)", rows, errb.String())
	}

	// builtin:census.sql++: the generated SQL++ (>=1 statement, non-empty sql).
	rows = arr(`show --queries "builtin:census.sql++?keyspace=sessions"`)
	if c.failed || len(rows) == 0 || rows[0]["sql"] == nil || !strings.Contains(rows[0]["sql"].(string), "OBJECT_PAIRS") {
		t.Fatalf("show census.sql++: %v (stderr %s)", rows, errb.String())
	}

	// native builtin:census: a note, no sql.
	rows = arr(`show --queries "builtin:census?keyspace=sessions"`)
	if c.failed || len(rows) != 1 || rows[0]["sql"] != nil || rows[0]["note"] == nil {
		t.Fatalf("show census: want a note and no sql, got %v", rows)
	}
}

// newLogsBundle builds a <root>/default/logs datastore of a few log docs and returns
// the root (the bundle dir a .multi command opens as c.dir).
func newLogsBundle(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	dir := filepath.Join(root, "default", "logs")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	docs := []string{
		`{"sev":"ERROR","msg":"disk full","ts":3}`,
		`{"sev":"INFO","msg":"started","ts":1}`,
		`{"sev":"ERROR","msg":"timeout","ts":5}`,
		`{"sev":"WARN","msg":"slow","ts":2}`,
	}
	for i, d := range docs {
		if err := os.WriteFile(filepath.Join(dir, "l"+string(rune('0'+i))+".json"), []byte(d), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

// TestMultiList: the metadata-only inventory shows one row per entry with its
// label / source / description / tags and fixture?/golden? flags -- WITHOUT opening a
// bundle (c.dir is empty) and without compiling.
func TestMultiList(t *testing.T) {
	entries := writeMultiQueryEntries(t, map[string]string{
		"a_full": `-- label: ET-1
-- source: logs
-- description: disk errors
-- tags: ["disk","io"]
SELECT * FROM logs l WHERE l.sev = "ERROR"
-- @fixture
{"sev":"ERROR","msg":"boom"}
-- @expect
{"label":"ET-1","result":{"sev":"ERROR","msg":"boom"}}`,
		"b_bare": `SELECT * FROM logs`,
	})

	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", mode: "jsonlines", out: &out, stderr: &errb} // no c.dir: no bundle opened
	c.cmdMulti("list --queries " + entries)

	stdout := out.String()
	// The rich entry: label/source/description/tags + both flags "yes".
	for _, want := range []string{
		`"label":"ET-1"`, `"source":"logs"`, `"description":"disk errors"`, `"tags":"disk,io"`,
		`"fixture?":"yes"`, `"golden?":"yes"`,
	} {
		if !strings.Contains(stdout, want) {
			t.Errorf("list inventory missing %s; stdout:\n%s", want, stdout)
		}
	}
	// The bare entry: label is the filename stem, no source, both flags "no".
	if !strings.Contains(stdout, `"label":"b_bare"`) || !strings.Contains(stdout, `"fixture?":"no"`) {
		t.Errorf("bare entry row wrong; stdout:\n%s", stdout)
	}
	if !strings.Contains(errb.String(), "2 query/queries") {
		t.Errorf("inventory summary count wrong; stderr:\n%s", errb.String())
	}
	if c.failed {
		t.Errorf("list must not fail (no bundle needed); stderr:\n%s", errb.String())
	}
}

// TestMultiHelp: the embedded guide prints the key sections to stdout -- the entry
// format markers, an example score line, and the authoring tips.
func TestMultiHelp(t *testing.T) {
	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", mode: "jsonlines", out: &out, stderr: &errb}
	c.cmdMulti("help")

	help := out.String()
	for _, want := range []string{
		"-- @fixture", "-- @expect", // the golden-fixture format
		"ANNOTATED ENTRY", "QUERIES DIRECTORY LAYOUT", // the doc structure
		"score:", "% fused", // an example score line shape
		"TIPS", "regexp_contains", // the tips (native-over-boxed nudge)
		"--bind", "--update", // the flag one-liners
		"RESERVED WORDS", "`level`", // DOC-3: the reserved-word note
		"TEMPORAL (ASOF)", "ORDER BY", "NOT lowered to ASOF", // DOC-2: the ASOF requirements
		"CONTEXT (grep -A/-B/-C)", "PARTITION BY _meta.`path`", // the grep-context idiom + multi-file gotcha
		"GATE (index-gate a standalone query)", "gate:", // the standalone index-gate
	} {
		if !strings.Contains(help, want) {
			t.Errorf(".multi help missing %q; stdout:\n%s", want, help)
		}
	}
}

// TestMultiQueriesFlag: the directory flag is --queries. It is REPEATABLE and accepts a
// comma-list, so several tiers fuse into one pack (IDEA-0034). The former --pack alias
// was removed (hard cut) and now fails loudly as an unknown flag.
func TestMultiQueriesFlag(t *testing.T) {
	for _, tc := range []struct {
		arg  string
		want []string
	}{
		{"--queries ./x", []string{"./x"}},
		{"--queries=./x", []string{"./x"}},
		{"--queries ./a --queries ./b", []string{"./a", "./b"}}, // repeatable
		{"--queries ./a,./b", []string{"./a", "./b"}},           // comma-list
	} {
		a, err := parseMultiArgs(tc.arg)
		if err != nil || !reflect.DeepEqual(a.queries, tc.want) {
			t.Errorf("parseMultiArgs(%q) = {queries:%v} err %v; want queries=%v", tc.arg, a.queries, err, tc.want)
		}
	}
	// The removed --pack alias is now an unknown flag, not silently accepted.
	if _, err := parseMultiArgs("--pack ./x"); err == nil || !strings.Contains(err.Error(), "unknown flag") {
		t.Errorf("--pack should now be an unknown flag; got %v", err)
	}
	// The error message names the new flag, not the old one.
	if _, err := parseMultiArgs("--bind m"); err == nil || !strings.Contains(err.Error(), "--queries") {
		t.Errorf("missing-dir error should mention --queries; got %v", err)
	}
}

// TestExtractHelp: .extract help is a self-contained *.extract.js authoring reference
// (DOC-1) -- it names the file object, every framing kind, the timestamp layouts, and
// the match claim shape, so writing an entry doesn't require reading records/spec.go.
func TestExtractHelp(t *testing.T) {
	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", mode: "jsonlines", out: &out, stderr: &errb}
	c.cmdExtract("help")

	help := out.String()
	for _, want := range []string{
		"describe(file)", "{ path, name, ext, head }", // the file object
		"var match =", "exts", "names", "priority", // the match claim
		"FRAMING", "multiline", "section", "whole", "json", // framing kinds
		"TIME", "RFC3339", "epoch_ms", "epoch-NANOS", // timestamp layouts
		"FIELDS", "(?P<", // named-capture fields
		"ANNOTATED EXAMPLE", // a full entry
	} {
		if !strings.Contains(help, want) {
			t.Errorf(".extract help missing %q; stdout:\n%s", want, help)
		}
	}
}

// TestExtractList: .extract list inventories the loaded entries with what each claims.
func TestExtractList(t *testing.T) {
	repo, err := filepath.Abs("../..") // from cmd/n1k1 up to the repo root
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(repo, "extensions", "extract_plugins", "couchbase_log.extract.js")
	if _, err := os.Stat(path); err != nil {
		t.Skipf("example entry not present: %v", err)
	}
	if _, err := glue.RegisterExtensionFile(path); err != nil {
		t.Fatalf("RegisterExtensionFile: %v", err)
	}
	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", mode: "jsonlines", out: &out, stderr: &errb}
	c.cmdExtract("list")
	got := errb.String()
	for _, want := range []string{"couchbase_log", "couchbase", "priority="} {
		if !strings.Contains(got, want) {
			t.Errorf(".extract list missing %q; stderr:\n%s", want, got)
		}
	}
}

// TestMultiFixSnippets: every author-facing status carries its fix snippet. A boxed
// entry, an always-wake entry, and a rejected one surface their snippets in the
// lint advice column and (rejected) in the run health block; a fixture with no @expect
// surfaces the "capture the golden" snippet in test output.
// TestMultiRunHitStats: .multi run prints per-entry hit stats (IDEA-0015) so a
// 0-labelResults entry is debuggable -- a matched=0 over a scanned-many keyspace is a
// predicate miss, while matched=0 over a scanned=1 whole-file blob is an upstream
// framing problem, and the two carry different hints.
func TestMultiRunHitStats(t *testing.T) {
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
	// A framed keyspace (3 rows, 2 ERROR) and a whole-file blob keyspace (1 row).
	write("default/logs/l.jsonl", `{"sev":"ERROR","msg":"a"}`+"\n"+`{"sev":"ERROR","msg":"b"}`+"\n"+`{"sev":"INFO","msg":"c"}`+"\n")
	write("default/blob/dump.log", "just raw text\nnothing structured\n")

	entries := writeMultiQueryEntries(t, map[string]string{
		"hit":        `SELECT * FROM logs l WHERE l.sev = "ERROR"`,                // matches 2 of 3
		"absent_lit": `SELECT * FROM logs l WHERE l.msg = "zzz_never"`,            // literal absent -> 0 woken
		"woke_miss":  `SELECT * FROM logs l WHERE l.msg = "a" AND l.sev = "INFO"`, // "a" in 1 row, pred false -> woken>0, matched 0
		"miss_blob":  `SELECT * FROM blob b WHERE b.text LIKE "%zzz%"`,            // 0 of 1 -> blob
	})

	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", dir: root, mode: "jsonlines", out: &out, stderr: &errb}
	c.cmdMulti("run --queries " + entries)
	got := errb.String() // the per-entry block goes to stderr

	for _, want := range []string{
		"per-query hits",
		"hit", "matched=2", "woken=", // the woken column is present
		"absent_lit", "0 woken", "never appears", // absent-literal hint
		"woke_miss", "never held", // predicate woke but never held
		"miss_blob", "scanned 1 row", "whole-file blob", // blob hint
	} {
		if !strings.Contains(got, want) {
			t.Errorf(".multi run hit-stats missing %q; stderr:\n%s", want, got)
		}
	}
}

func TestMultiFixSnippets(t *testing.T) {
	root := newLogsBundle(t)
	entries := writeMultiQueryEntries(t, map[string]string{
		"boxed":   `SELECT * FROM logs l WHERE l.msg LIKE "%a%b%"`,                    // boxed PREDICATE (interior wildcards) + always-wake: genuinely unprunable
		"boxproj": `SELECT l.msg LIKE "%a%b%" AS x FROM logs l WHERE l.sev = "ERROR"`, // boxed PROJECTION over a native literal predicate: still fused + index-pruned
		"wake":    `SELECT * FROM logs l WHERE l.ts > 5`,                              // fused, always-wake (no literal)
		"broken":  `SELECT * FROM logs l WHERE`,                                       // rejected
	})

	// lint: the advice column carries the boxed, always-wake, and rejected snippets.
	var lout, lerr bytes.Buffer
	c := &cli{prog: "n1k1", dir: root, mode: "jsonlines", out: &lout, stderr: &lerr}
	c.cmdMulti("lint --queries " + entries)
	lintOut := lout.String()
	for _, want := range []string{
		"a boxed expression falls back to cbq", // boxed-predicate advice (unprunable)
		"can't be index-pruned",                // ...and explains the pruning loss (boxed WHERE)
		"no discriminating literal",            // always-wake advice
		"not a runnable query",                 // rejected advice
		"msg LIKE '%a%b%'", "regexp_contains",  // the boxed native-form example
		// Regression guard (ISSUE-06 dogfooding): a boxed PROJECTION over a native
		// literal predicate must NOT be told it lost index pruning -- the scan still
		// fuses and prunes; boxing is only a per-row projection cost.
		"per-row projection cost on woken rows, not a scan cost",
	} {
		if !strings.Contains(lintOut, want) {
			t.Errorf("lint advice missing fix snippet %q; stdout:\n%s", want, lintOut)
		}
	}

	// run: the rejected entry's fix snippet appears in the health block on stderr.
	var rout, rerr bytes.Buffer
	c2 := &cli{prog: "n1k1", dir: root, mode: "jsonlines", out: &rout, stderr: &rerr}
	c2.cmdMulti("run --queries " + entries)
	if !strings.Contains(rerr.String(), "not a runnable query") {
		t.Errorf("run health block missing the rejected fix snippet; stderr:\n%s", rerr.String())
	}

	// test: a fixture with no @expect surfaces the "capture the golden" snippet.
	tc := writeMultiQueryEntries(t, map[string]string{
		"nogold": `-- label: G
-- source: logs
SELECT * FROM logs l WHERE l.sev = "ERROR"
-- @fixture
{"sev":"ERROR","msg":"boom"}`,
	})
	var tout, terr bytes.Buffer
	c3 := &cli{prog: "n1k1", mode: "jsonlines", out: &tout, stderr: &terr}
	c3.cmdMulti("test --queries " + tc)
	if !strings.Contains(terr.String(), "fixture has no expected labelResults recorded") {
		t.Errorf("test missing the no-golden fix snippet; stderr:\n%s", terr.String())
	}
	if !strings.Contains(terr.String(), ".multi test --update") {
		t.Errorf("no-golden snippet must point at --update; stderr:\n%s", terr.String())
	}
}

// TestMultiRun: a pack of one fusable filter, one correlated (standalone), and one
// broken (rejected) entry. The fusable + standalone produce tagged labelResults; the
// coverage summary reports 1 fused / 1 standalone / 1 rejected (with the reason); the
// broken entry does not abort the run.
func TestMultiRun(t *testing.T) {
	root := newLogsBundle(t)
	entries := writeMultiQueryEntries(t, map[string]string{
		"errors":   `SELECT * FROM logs WHERE sev = "ERROR"`,
		"prev_ts":  `SELECT e.msg, (SELECT RAW r.ts FROM logs r WHERE r.ts <= e.ts ORDER BY r.ts DESC LIMIT 1)[0] AS prior_ts FROM logs e`,
		"broken_x": `SELECT * FROM logs WHERE`,
	})

	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", dir: root, mode: "jsonlines", out: &out, stderr: &errb}
	c.cmdMulti("run --queries " + entries)

	stderr := errb.String()
	if !strings.Contains(stderr, "1 fused, 1 standalone, 1 rejected") {
		t.Errorf("coverage summary wrong; stderr:\n%s", stderr)
	}
	// The rejected entry is surfaced with its label + a reason, and did not abort.
	if !strings.Contains(stderr, "broken_x") {
		t.Errorf("rejected entry broken_x not surfaced; stderr:\n%s", stderr)
	}
	// LabelResults for the fusable (errors) and standalone (prev_ts) entries appear,
	// tagged. (2 ERROR rows fused + 4 standalone projection rows.)
	stdout := out.String()
	if !strings.Contains(stdout, `"label":"errors"`) {
		t.Errorf("no fusable labelResults tagged errors; stdout:\n%s", stdout)
	}
	if !strings.Contains(stdout, `"label":"prev_ts"`) {
		t.Errorf("no standalone labelResults tagged prev_ts; stdout:\n%s", stdout)
	}
	if c.failed {
		t.Errorf("a broken entry must not abort the run (c.failed=true); stderr:\n%s", stderr)
	}
}

// TestMultiLint: the report card shows the three classes, an always-wake fused
// entry gets the always-wake advice, a boxed one names its native alternative, and
// the pack score line is present.
func TestMultiLint(t *testing.T) {
	root := newLogsBundle(t)
	entries := writeMultiQueryEntries(t, map[string]string{
		"errors":     `SELECT * FROM logs WHERE sev = "ERROR"`,           // fused, native, indexed
		"everything": `SELECT * FROM logs`,                               // fused, always-wake (no literal)
		"grouped":    `SELECT sev, COUNT(*) AS n FROM logs GROUP BY sev`, // standalone
		"broken_x":   `SELECT * FROM logs WHERE`,                         // rejected
	})

	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", dir: root, mode: "jsonlines", out: &out, stderr: &errb}
	c.cmdMulti("lint --queries " + entries)

	stdout := out.String()
	for _, want := range []string{`"class":"fused"`, `"class":"standalone"`, `"class":"rejected"`} {
		if !strings.Contains(stdout, want) {
			t.Errorf("lint report missing %s; stdout:\n%s", want, stdout)
		}
	}
	// The no-WHERE fused entry always-wakes -> the discriminating-literal advice.
	if !strings.Contains(stdout, "always-wake") {
		t.Errorf("expected always-wake advice for the no-literal entry; stdout:\n%s", stdout)
	}
	// A native+indexed entry reports its required literal.
	if !strings.Contains(stdout, "ERROR") {
		t.Errorf("expected the ERROR literal for the indexed entry; stdout:\n%s", stdout)
	}
	// The pack score line (on stderr) is present.
	if !strings.Contains(errb.String(), "score:") || !strings.Contains(errb.String(), "% fused") {
		t.Errorf("pack score line missing; stderr:\n%s", errb.String())
	}
}

// TestMultiExplain: `.multi explain` surfaces the fused shared-scan PLAN (the op tree
// MULTI_MATCHES's stream-fn node hides) plus the fusion map -- which queries share the
// scan and the index literal each is keyed on -- and lists the standalone/rejected ones
// (IDEA-0036). It compiles but does NOT run (no labelResults printed).
func TestMultiExplain(t *testing.T) {
	root := newLogsBundle(t)
	// A second keyspace, so the fused plan is a union-all over TWO shared scans.
	evDir := filepath.Join(root, "default", "events")
	if err := os.MkdirAll(evDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(evDir, "e0.json"), []byte(`{"act":"login","who":"ann"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	entries := writeMultiQueryEntries(t, map[string]string{
		"errors":     `SELECT * FROM logs WHERE sev = "ERROR"`,           // fused, native, indexed on "ERROR"
		"everything": `SELECT * FROM logs`,                               // fused, always-wake (no literal)
		"logins":     `SELECT * FROM events WHERE act = "login"`,         // fused, a SECOND keyspace -> union-all
		"grouped":    `SELECT sev, COUNT(*) AS n FROM logs GROUP BY sev`, // standalone
		"broken_x":   `SELECT * FROM logs WHERE`,                         // rejected
	})

	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", dir: root, mode: "jsonlines", out: &out, stderr: &errb}
	c.cmdMulti("explain --queries " + entries)

	got := out.String()
	for _, want := range []string{
		"fused across",           // the roll-up header
		"FUSED PLAN",             // the op-tree section
		"union-all",              // two keyspaces fused -> a union-all over both shared scans
		"broadcast-indexed",      // the fused MQO shape (was hidden behind stream-fn)
		"datastore-scan-records", // the shared scan leaf
		"FUSION MAP",             // which queries share the scan
		"shared scan:",           // per-keyspace group header
		`literal "ERROR"`,        // the indexed entry's necessary literal
		"always-wake",            // the no-literal entry
		"STANDALONE",             // the grouped entry
		"grouped",                //   ... named
		"REJECTED",               // the broken entry
		"broken_x",               //   ... named
	} {
		if !strings.Contains(got, want) {
			t.Errorf(".multi explain missing %q; stdout:\n%s", want, got)
		}
	}
	// explain compiles, it does NOT run -- no labelResults rows (a labelResult row is
	// `{"label":"errors",...}`; the bare label token appears in the plan's schema).
	if strings.Contains(got, `"label":"`) {
		t.Errorf(".multi explain must not run the pack (no labelResults); stdout:\n%s", got)
	}
	if c.failed {
		t.Errorf("a rejected entry must not fail explain (c.failed=true); stderr:\n%s", errb.String())
	}
}

// TestMultiExplainSQL: `.multi explain --sql` renders each query as PRETTY SQL++ with a
// provenance header comment (fused/standalone/rejected + keyspace + lane + index literal)
// and inline `-- hint:` advice (IDEA-0037). The SQL body is re-laid-out multi-line.
func TestMultiExplainSQL(t *testing.T) {
	root := newLogsBundle(t)
	entries := writeMultiQueryEntries(t, map[string]string{
		"errors":     `SELECT * FROM logs WHERE sev = "ERROR"`,           // fused, indexed on "ERROR"
		"everything": `SELECT * FROM logs`,                               // fused, always-wake -> a hint
		"grouped":    `SELECT sev, COUNT(*) AS n FROM logs GROUP BY sev`, // standalone
		"broken_x":   `SELECT * FROM logs WHERE`,                         // rejected
	})

	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", dir: root, mode: "jsonlines", out: &out, stderr: &errb}
	c.cmdMulti("explain --sql --queries " + entries)

	got := out.String()
	for _, want := range []string{
		"-- errors",       // a provenance header comment
		"fused",           //   ... classified fused
		`literal "ERROR"`, //   ... keyed on its literal
		"-- everything",   // the always-wake query
		"-- hint:",        //   ... gets an inline authoring hint
		"-- grouped",      // the standalone query
		"standalone",      //
		"-- broken_x",     // the rejected query
		"REJECTED",        //
	} {
		if !strings.Contains(got, want) {
			t.Errorf(".multi explain --sql missing %q; stdout:\n%s", want, got)
		}
	}
	// The SQL body is pretty-printed multi-line: "SELECT *" and "FROM logs" each lead
	// a line (not one wall).
	if !strings.Contains(got, "SELECT *\nFROM logs") {
		t.Errorf(".multi explain --sql should pretty-print the body multi-line; stdout:\n%s", got)
	}
	if c.failed {
		t.Errorf("a rejected entry must not fail explain --sql; stderr:\n%s", errb.String())
	}
}

// TestMultiTest: the golden-fixture runner in check mode over a pack of a PASSING
// entry (fixture + correct expect), a FAILING entry (fixture + deliberately wrong
// expect -> reported with a diff), a NO-FIXTURE entry (counted, not a hard fail), and a
// FIXTURE-WITHOUT-EXPECT entry (a hard fail -- "no golden recorded"). The summary counts
// are asserted and failure is signaled via c.failed (so a CI caller exits non-zero).
// It needs no open bundle -- .multi test builds its own temp fixture keyspaces.
func TestMultiTest(t *testing.T) {
	entries := writeMultiQueryEntries(t, map[string]string{
		"pass": `-- label: P
-- source: logs
SELECT * FROM logs l WHERE l.sev = "ERROR"
-- @fixture
{"sev":"ERROR","msg":"boom"}
{"sev":"INFO","msg":"fine"}
-- @expect
{"label":"P","result":{"sev":"ERROR","msg":"boom"}}`,
		"fail": `-- label: F
-- source: logs
SELECT * FROM logs l WHERE l.sev = "ERROR"
-- @fixture
{"sev":"ERROR","msg":"boom"}
-- @expect
{"label":"F","result":{"sev":"ERROR","msg":"NOT-THE-ROW"}}`,
		"nofix": `-- label: N
-- source: logs
SELECT * FROM logs l WHERE l.sev = "WARN"`,
		"nogold": `-- label: G
-- source: logs
SELECT * FROM logs l WHERE l.sev = "ERROR"
-- @fixture
{"sev":"ERROR","msg":"boom"}`,
	})

	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", mode: "jsonlines", out: &out, stderr: &errb}
	c.cmdMulti("test --queries " + entries)

	stderr := errb.String()
	// pass PASS, fail FAIL (with a diff), nogold FAIL (no golden), nofix counted.
	if !strings.Contains(stderr, "1 passed / 2 failed / 1 no-fixture") {
		t.Errorf("summary counts wrong; stderr:\n%s", stderr)
	}
	if !strings.Contains(stderr, "P: PASS") {
		t.Errorf("passing entry not reported PASS; stderr:\n%s", stderr)
	}
	if !strings.Contains(stderr, "F: FAIL") || !strings.Contains(stderr, "missing:") {
		t.Errorf("failing entry not reported FAIL with a diff; stderr:\n%s", stderr)
	}
	if !strings.Contains(stderr, "no expected labelResults recorded") {
		t.Errorf("fixture-without-expect not reported as no-golden FAIL; stderr:\n%s", stderr)
	}
	if !c.failed {
		t.Errorf("any FAIL must set c.failed (CI exit signal); stderr:\n%s", stderr)
	}
}

// TestMultiTestContextProjection (IDEA-0025): a CONTEXT (grep -C) entry's golden is
// its SELECT projection {pos,msg}, and `.multi test` check-PASSES against it -- proving
// the fused broadcast-context path honors the projection (not the whole framed row) and
// that the golden shape matches what a real run emits. The golden would MISMATCH the old
// whole-row result ({_meta,...,msg}), so a passing check locks in the fix.
func TestMultiTestContextProjection(t *testing.T) {
	entries := writeMultiQueryEntries(t, map[string]string{
		"ctx": `-- label: CTX
-- source: logs
SELECT sub.pos AS pos, sub.msg AS msg
FROM (
  SELECT pos, msg,
         MAX(CASE WHEN regexp_contains(msg, "boom") THEN 1 ELSE 0 END)
           OVER (PARTITION BY file ORDER BY pos ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS near
  FROM logs) sub
WHERE sub.near = 1
-- @fixture
{"file":"p","pos":0,"msg":"before the boom"}
{"file":"p","pos":1,"msg":"boom happened"}
{"file":"p","pos":2,"msg":"after"}
{"file":"p","pos":3,"msg":"far away"}
-- @expect
{"label":"CTX","result":{"msg":"before the boom","pos":0}}
{"label":"CTX","result":{"msg":"boom happened","pos":1}}
{"label":"CTX","result":{"msg":"after","pos":2}}`,
	})

	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", mode: "jsonlines", out: &out, stderr: &errb}
	c.cmdMulti("test --queries " + entries)

	stderr := errb.String()
	if c.failed || !strings.Contains(stderr, "CTX: PASS") {
		t.Errorf("context entry golden (projected shape) should PASS; stderr:\n%s", stderr)
	}
	if !strings.Contains(stderr, "1 passed / 0 failed") {
		t.Errorf("summary wrong; stderr:\n%s", stderr)
	}
}

// TestMultiTestUpdate: an entry with a fixture and NO @expect -> --update records the
// golden; re-running in check mode then PASSES; and everything before the @expect block
// is left byte-identical.
func TestMultiTestUpdate(t *testing.T) {
	head := `-- label: U
-- source: logs
SELECT * FROM logs l WHERE l.sev = "ERROR"
-- @fixture
{"sev":"ERROR","msg":"boom"}
{"sev":"INFO","msg":"fine"}
`
	dir := t.TempDir()
	path := filepath.Join(dir, "u.sql++")
	if err := os.WriteFile(path, []byte(head), 0o644); err != nil {
		t.Fatal(err)
	}

	// (1) --update records the golden; no failure.
	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", mode: "jsonlines", out: &out, stderr: &errb}
	c.cmdMulti("test --queries " + dir + " --update")
	if c.failed {
		t.Fatalf("--update must not fail on a runnable fixture; stderr:\n%s", errb.String())
	}
	if !strings.Contains(errb.String(), "U: recorded 1 labelResult") {
		t.Errorf("--update did not record the golden; stderr:\n%s", errb.String())
	}

	// The head (front-matter + SQL + fixture) is byte-identical; an @expect was appended.
	rewritten, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(rewritten), head) {
		t.Errorf("--update altered the entry head:\n%s", string(rewritten))
	}
	if !strings.Contains(string(rewritten), "-- @expect") {
		t.Errorf("--update did not append an @expect block:\n%s", string(rewritten))
	}

	// (2) Re-run in check mode -> PASS now.
	var out2, errb2 bytes.Buffer
	c2 := &cli{prog: "n1k1", mode: "jsonlines", out: &out2, stderr: &errb2}
	c2.cmdMulti("test --queries " + dir)
	if c2.failed {
		t.Errorf("recorded golden should PASS on re-check; stderr:\n%s", errb2.String())
	}
	if !strings.Contains(errb2.String(), "1 passed / 0 failed") {
		t.Errorf("re-check summary wrong; stderr:\n%s", errb2.String())
	}
}

// TestMultiRunBind: a pack written against a LOGICAL keyspace resolves via a
// manifest and runs; an unresolved logical keyspace fails loud (coverage surfaces the
// gap) rather than reporting a silently clean bundle.
func TestMultiRunBind(t *testing.T) {
	// A flat bundle of *.json at the root (the manifest globs them directly).
	root := t.TempDir()
	for i, d := range []string{
		`{"sev":"ERROR","msg":"oom"}`,
		`{"sev":"INFO","msg":"ok"}`,
	} {
		if err := os.WriteFile(filepath.Join(root, "app"+string(rune('0'+i))+".json"), []byte(d), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	entries := writeMultiQueryEntries(t, map[string]string{
		"oom": `SELECT * FROM indexer_log WHERE sev = "ERROR"`,
	})

	// (1) A manifest that resolves -> the run works.
	good := filepath.Join(t.TempDir(), "manifest")
	if err := os.WriteFile(good, []byte("indexer_log = *.json\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", dir: root, mode: "jsonlines", out: &out, stderr: &errb}
	c.cmdMulti("run --queries " + entries + " --bind " + good)
	if !strings.Contains(out.String(), `"label":"oom"`) {
		t.Errorf("bound run produced no labelResults; stdout:\n%s\nstderr:\n%s", out.String(), errb.String())
	}
	if !strings.Contains(errb.String(), "resolved") {
		t.Errorf("binding coverage should report the resolved keyspace; stderr:\n%s", errb.String())
	}
	if c.failed {
		t.Errorf("a resolving bind must not fail; stderr:\n%s", errb.String())
	}

	// (2) A manifest that resolves to NO files -> fail loud (a gap), not clean.
	bad := filepath.Join(t.TempDir(), "manifest")
	if err := os.WriteFile(bad, []byte("indexer_log = nowhere/*.json\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	var out2, errb2 bytes.Buffer
	c2 := &cli{prog: "n1k1", dir: root, mode: "jsonlines", out: &out2, stderr: &errb2}
	c2.cmdMulti("run --queries " + entries + " --bind " + bad)
	if !strings.Contains(errb2.String(), "UNRESOLVED") {
		t.Errorf("an unresolved logical keyspace must fail loud; stderr:\n%s", errb2.String())
	}
	if !c2.failed {
		t.Errorf("an unresolved binding must set c.failed (fail-loud), stderr:\n%s", errb2.String())
	}
	if strings.TrimSpace(out2.String()) != "" {
		t.Errorf("must NOT render a (falsely clean) labelResults table on a gap; stdout:\n%s", out2.String())
	}
}

// TestMultiRunParams covers --param on the run/show surface: defaults apply, --param
// overrides, an unknown --param errors naming the declared set, and show renders a
// missing REQUIRED param as a <placeholder> instead of refusing.
func TestMultiRunParams(t *testing.T) {
	root := t.TempDir()
	ks := filepath.Join(root, "default", "events")
	if err := os.MkdirAll(ks, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ks, "e.jsonl"),
		[]byte(`{"n":1}`+"\n"+`{"n":5}`+"\n"+`{"n":9}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	pack := writeMultiQueryEntries(t, map[string]string{
		"p": "-- label: P\n-- param: threshold int = 4\nSELECT e.n FROM events e WHERE e.n > $threshold",
	})

	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", mode: "jsonlines", out: &out, stderr: &errb, dir: root}
	rows := func() int { return strings.Count(strings.TrimSpace(out.String()), "\n") + 1 }

	// Default threshold=4 -> n=5, n=9.
	out.Reset()
	c.cmdMulti("run --queries " + pack)
	if c.failed || rows() != 2 {
		t.Fatalf("run under defaults: want 2 rows, got %q (stderr %s)", out.String(), errb.String())
	}

	// --param threshold=8 -> only n=9.
	out.Reset()
	c.failed = false
	c.cmdMulti("run --queries " + pack + " --param threshold=8")
	if c.failed || rows() != 1 || !strings.Contains(out.String(), `"n":9`) {
		t.Fatalf("run with --param: want just n=9, got %q", out.String())
	}

	// Unknown --param errors naming the declared set.
	out.Reset()
	errb.Reset()
	c.failed = false
	c.cmdMulti("run --queries " + pack + " --param treshold=8")
	if !c.failed || !strings.Contains(errb.String(), "treshold") || !strings.Contains(errb.String(), "threshold") {
		t.Fatalf("unknown --param must error naming the declared set: %s", errb.String())
	}

	// show renders under defaults (threshold=4 appears as the literal).
	out.Reset()
	c.failed = false
	c.cmdMulti("show --queries " + pack)
	if c.failed || !strings.Contains(out.String(), `e.n \u003e 4`) {
		t.Fatalf("show should render the default literal: %q", out.String())
	}

	// A REQUIRED param (no default) shows as a <placeholder> instead of refusing.
	pack2 := writeMultiQueryEntries(t, map[string]string{
		"q": "-- label: Q\n-- param: field ident\nSELECT e.$field FROM events e",
	})
	out.Reset()
	c.failed = false
	c.cmdMulti("show --queries " + pack2)
	if c.failed || !strings.Contains(out.String(), `\u003cfield\u003e`) {
		t.Fatalf("show should placeholder a required param: %q (stderr %s)", out.String(), errb.String())
	}
}
