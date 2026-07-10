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
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeCorpus writes each name->body entry as <dir>/<name>.sql++ and returns dir.
func writeCorpus(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(dir, name+".sql++"), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// newLogsBundle builds a <root>/default/logs datastore of a few log docs and returns
// the root (the bundle dir a .rules command opens as c.dir).
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

// TestRulesList: the metadata-only inventory shows one row per recipe with its
// tag / source / severity / versions and fixture?/golden? flags -- WITHOUT opening a
// bundle (c.dir is empty) and without compiling.
func TestRulesList(t *testing.T) {
	corpus := writeCorpus(t, map[string]string{
		"a_full": `-- ticket: ET-1
-- source: logs
-- severity: high
-- versions: ["7.2","7.6"]
SELECT * FROM logs l WHERE l.sev = "ERROR"
-- @fixture
{"sev":"ERROR","msg":"boom"}
-- @expect
{"tag":"ET-1","evidence":{"sev":"ERROR","msg":"boom"}}`,
		"b_bare": `SELECT * FROM logs`,
	})

	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", mode: "jsonlines", out: &out, stderr: &errb} // no c.dir: no bundle opened
	c.cmdRules("list --corpus " + corpus)

	stdout := out.String()
	// The rich recipe: tag/source/severity/versions + both flags "yes".
	for _, want := range []string{
		`"tag":"ET-1"`, `"source":"logs"`, `"severity":"high"`, `"versions":"7.2,7.6"`,
		`"fixture?":"yes"`, `"golden?":"yes"`,
	} {
		if !strings.Contains(stdout, want) {
			t.Errorf("list inventory missing %s; stdout:\n%s", want, stdout)
		}
	}
	// The bare recipe: tag is the filename stem, no source, both flags "no".
	if !strings.Contains(stdout, `"tag":"b_bare"`) || !strings.Contains(stdout, `"fixture?":"no"`) {
		t.Errorf("bare recipe row wrong; stdout:\n%s", stdout)
	}
	if !strings.Contains(errb.String(), "2 detector(s)") {
		t.Errorf("inventory summary count wrong; stderr:\n%s", errb.String())
	}
	if c.failed {
		t.Errorf("list must not fail (no bundle needed); stderr:\n%s", errb.String())
	}
}

// TestRulesHelp: the embedded guide prints the key sections to stdout -- the recipe
// format markers, an example score line, and the authoring tips.
func TestRulesHelp(t *testing.T) {
	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", mode: "jsonlines", out: &out, stderr: &errb}
	c.cmdRules("help")

	help := out.String()
	for _, want := range []string{
		"-- @fixture", "-- @expect", // the golden-fixture format
		"ANNOTATED RECIPE", "CORPUS LAYOUT", // the doc structure
		"score:", "% fused", // an example score line shape
		"TIPS", "regexp_contains", // the tips (native-over-boxed nudge)
		"--bind", "--update", // the flag one-liners
	} {
		if !strings.Contains(help, want) {
			t.Errorf(".rules help missing %q; stdout:\n%s", want, help)
		}
	}
}

// TestRulesFixSnippets: every author-facing status carries its fix snippet. A boxed
// detector, an always-wake detector, and a rejected one surface their snippets in the
// lint advice column and (rejected) in the run health block; a fixture with no @expect
// surfaces the "capture the golden" snippet in test output.
func TestRulesFixSnippets(t *testing.T) {
	root := newLogsBundle(t)
	corpus := writeCorpus(t, map[string]string{
		"boxed":  `SELECT * FROM logs l WHERE UPPER(l.msg) LIKE "%X%"`, // boxed + always-wake
		"wake":   `SELECT * FROM logs l WHERE l.ts > 5`,                // fused, always-wake (no literal)
		"broken": `SELECT * FROM logs l WHERE`,                         // rejected
	})

	// lint: the advice column carries the boxed, always-wake, and rejected snippets.
	var lout, lerr bytes.Buffer
	c := &cli{prog: "n1k1", dir: root, mode: "jsonlines", out: &lout, stderr: &lerr}
	c.cmdRules("lint --corpus " + corpus)
	lintOut := lout.String()
	for _, want := range []string{
		"predicate boxes (falls back to cbq)", // boxed advice
		"no discriminating literal",           // always-wake advice
		"not a runnable detector",             // rejected advice
		"msg LIKE '%X%'", "regexp_contains",   // the boxed native-form example
	} {
		if !strings.Contains(lintOut, want) {
			t.Errorf("lint advice missing fix snippet %q; stdout:\n%s", want, lintOut)
		}
	}

	// run: the rejected detector's fix snippet appears in the health block on stderr.
	var rout, rerr bytes.Buffer
	c2 := &cli{prog: "n1k1", dir: root, mode: "jsonlines", out: &rout, stderr: &rerr}
	c2.cmdRules("run --corpus " + corpus)
	if !strings.Contains(rerr.String(), "not a runnable detector") {
		t.Errorf("run health block missing the rejected fix snippet; stderr:\n%s", rerr.String())
	}

	// test: a fixture with no @expect surfaces the "capture the golden" snippet.
	tc := writeCorpus(t, map[string]string{
		"nogold": `-- ticket: G
-- source: logs
SELECT * FROM logs l WHERE l.sev = "ERROR"
-- @fixture
{"sev":"ERROR","msg":"boom"}`,
	})
	var tout, terr bytes.Buffer
	c3 := &cli{prog: "n1k1", mode: "jsonlines", out: &tout, stderr: &terr}
	c3.cmdRules("test --corpus " + tc)
	if !strings.Contains(terr.String(), "fixture has no expected findings recorded") {
		t.Errorf("test missing the no-golden fix snippet; stderr:\n%s", terr.String())
	}
	if !strings.Contains(terr.String(), ".rules test --update") {
		t.Errorf("no-golden snippet must point at --update; stderr:\n%s", terr.String())
	}
}

// TestRulesRun: a corpus of one fusable filter, one correlated (standalone), and one
// broken (rejected) detector. The fusable + standalone produce tagged findings; the
// coverage summary reports 1 fused / 1 standalone / 1 rejected (with the reason); the
// broken detector does not abort the run.
func TestRulesRun(t *testing.T) {
	root := newLogsBundle(t)
	corpus := writeCorpus(t, map[string]string{
		"errors":   `SELECT * FROM logs WHERE sev = "ERROR"`,
		"prev_ts":  `SELECT e.msg, (SELECT RAW r.ts FROM logs r WHERE r.ts <= e.ts ORDER BY r.ts DESC LIMIT 1)[0] AS prior_ts FROM logs e`,
		"broken_x": `SELECT * FROM logs WHERE`,
	})

	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", dir: root, mode: "jsonlines", out: &out, stderr: &errb}
	c.cmdRules("run --corpus " + corpus)

	stderr := errb.String()
	if !strings.Contains(stderr, "1 fused, 1 standalone, 1 rejected") {
		t.Errorf("coverage summary wrong; stderr:\n%s", stderr)
	}
	// The rejected detector is surfaced with its tag + a reason, and did not abort.
	if !strings.Contains(stderr, "broken_x") {
		t.Errorf("rejected detector broken_x not surfaced; stderr:\n%s", stderr)
	}
	// Findings for the fusable (errors) and standalone (prev_ts) detectors appear,
	// tagged. (2 ERROR rows fused + 4 standalone projection rows.)
	stdout := out.String()
	if !strings.Contains(stdout, `"tag":"errors"`) {
		t.Errorf("no fusable findings tagged errors; stdout:\n%s", stdout)
	}
	if !strings.Contains(stdout, `"tag":"prev_ts"`) {
		t.Errorf("no standalone findings tagged prev_ts; stdout:\n%s", stdout)
	}
	if c.failed {
		t.Errorf("a broken detector must not abort the run (c.failed=true); stderr:\n%s", stderr)
	}
}

// TestRulesLint: the report card shows the three classes, an always-wake fused
// detector gets the always-wake advice, a boxed one names its native alternative, and
// the corpus score line is present.
func TestRulesLint(t *testing.T) {
	root := newLogsBundle(t)
	corpus := writeCorpus(t, map[string]string{
		"errors":     `SELECT * FROM logs WHERE sev = "ERROR"`,           // fused, native, indexed
		"everything": `SELECT * FROM logs`,                               // fused, always-wake (no literal)
		"grouped":    `SELECT sev, COUNT(*) AS n FROM logs GROUP BY sev`, // standalone
		"broken_x":   `SELECT * FROM logs WHERE`,                         // rejected
	})

	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", dir: root, mode: "jsonlines", out: &out, stderr: &errb}
	c.cmdRules("lint --corpus " + corpus)

	stdout := out.String()
	for _, want := range []string{`"class":"fused"`, `"class":"standalone"`, `"class":"rejected"`} {
		if !strings.Contains(stdout, want) {
			t.Errorf("lint report missing %s; stdout:\n%s", want, stdout)
		}
	}
	// The no-WHERE fused detector always-wakes -> the discriminating-literal advice.
	if !strings.Contains(stdout, "always-wake") {
		t.Errorf("expected always-wake advice for the no-literal detector; stdout:\n%s", stdout)
	}
	// A native+indexed detector reports its required literal.
	if !strings.Contains(stdout, "ERROR") {
		t.Errorf("expected the ERROR literal for the indexed detector; stdout:\n%s", stdout)
	}
	// The corpus score line (on stderr) is present.
	if !strings.Contains(errb.String(), "score:") || !strings.Contains(errb.String(), "% fused") {
		t.Errorf("corpus score line missing; stderr:\n%s", errb.String())
	}
}

// TestRulesTest: the golden-fixture runner in check mode over a corpus of a PASSING
// recipe (fixture + correct expect), a FAILING recipe (fixture + deliberately wrong
// expect -> reported with a diff), a NO-FIXTURE recipe (counted, not a hard fail), and a
// FIXTURE-WITHOUT-EXPECT recipe (a hard fail -- "no golden recorded"). The summary counts
// are asserted and failure is signaled via c.failed (so a CI caller exits non-zero).
// It needs no open bundle -- .rules test builds its own temp fixture keyspaces.
func TestRulesTest(t *testing.T) {
	corpus := writeCorpus(t, map[string]string{
		"pass": `-- ticket: P
-- source: logs
SELECT * FROM logs l WHERE l.sev = "ERROR"
-- @fixture
{"sev":"ERROR","msg":"boom"}
{"sev":"INFO","msg":"fine"}
-- @expect
{"tag":"P","evidence":{"sev":"ERROR","msg":"boom"}}`,
		"fail": `-- ticket: F
-- source: logs
SELECT * FROM logs l WHERE l.sev = "ERROR"
-- @fixture
{"sev":"ERROR","msg":"boom"}
-- @expect
{"tag":"F","evidence":{"sev":"ERROR","msg":"NOT-THE-ROW"}}`,
		"nofix": `-- ticket: N
-- source: logs
SELECT * FROM logs l WHERE l.sev = "WARN"`,
		"nogold": `-- ticket: G
-- source: logs
SELECT * FROM logs l WHERE l.sev = "ERROR"
-- @fixture
{"sev":"ERROR","msg":"boom"}`,
	})

	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", mode: "jsonlines", out: &out, stderr: &errb}
	c.cmdRules("test --corpus " + corpus)

	stderr := errb.String()
	// pass PASS, fail FAIL (with a diff), nogold FAIL (no golden), nofix counted.
	if !strings.Contains(stderr, "1 passed / 2 failed / 1 no-fixture") {
		t.Errorf("summary counts wrong; stderr:\n%s", stderr)
	}
	if !strings.Contains(stderr, "P: PASS") {
		t.Errorf("passing recipe not reported PASS; stderr:\n%s", stderr)
	}
	if !strings.Contains(stderr, "F: FAIL") || !strings.Contains(stderr, "missing:") {
		t.Errorf("failing recipe not reported FAIL with a diff; stderr:\n%s", stderr)
	}
	if !strings.Contains(stderr, "no expected findings recorded") {
		t.Errorf("fixture-without-expect not reported as no-golden FAIL; stderr:\n%s", stderr)
	}
	if !c.failed {
		t.Errorf("any FAIL must set c.failed (CI exit signal); stderr:\n%s", stderr)
	}
}

// TestRulesTestUpdate: a recipe with a fixture and NO @expect -> --update records the
// golden; re-running in check mode then PASSES; and everything before the @expect block
// is left byte-identical.
func TestRulesTestUpdate(t *testing.T) {
	head := `-- ticket: U
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
	c.cmdRules("test --corpus " + dir + " --update")
	if c.failed {
		t.Fatalf("--update must not fail on a runnable fixture; stderr:\n%s", errb.String())
	}
	if !strings.Contains(errb.String(), "U: recorded 1 finding") {
		t.Errorf("--update did not record the golden; stderr:\n%s", errb.String())
	}

	// The head (front-matter + SQL + fixture) is byte-identical; an @expect was appended.
	rewritten, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(rewritten), head) {
		t.Errorf("--update altered the recipe head:\n%s", string(rewritten))
	}
	if !strings.Contains(string(rewritten), "-- @expect") {
		t.Errorf("--update did not append an @expect block:\n%s", string(rewritten))
	}

	// (2) Re-run in check mode -> PASS now.
	var out2, errb2 bytes.Buffer
	c2 := &cli{prog: "n1k1", mode: "jsonlines", out: &out2, stderr: &errb2}
	c2.cmdRules("test --corpus " + dir)
	if c2.failed {
		t.Errorf("recorded golden should PASS on re-check; stderr:\n%s", errb2.String())
	}
	if !strings.Contains(errb2.String(), "1 passed / 0 failed") {
		t.Errorf("re-check summary wrong; stderr:\n%s", errb2.String())
	}
}

// TestRulesRunBind: a corpus written against a LOGICAL keyspace resolves via a
// manifest and runs; an unresolved logical keyspace fails loud (coverage surfaces the
// gap) rather than reporting a silently clean bundle.
func TestRulesRunBind(t *testing.T) {
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
	corpus := writeCorpus(t, map[string]string{
		"oom": `SELECT * FROM indexer_log WHERE sev = "ERROR"`,
	})

	// (1) A manifest that resolves -> the run works.
	good := filepath.Join(t.TempDir(), "manifest")
	if err := os.WriteFile(good, []byte("indexer_log = *.json\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	var out, errb bytes.Buffer
	c := &cli{prog: "n1k1", dir: root, mode: "jsonlines", out: &out, stderr: &errb}
	c.cmdRules("run --corpus " + corpus + " --bind " + good)
	if !strings.Contains(out.String(), `"tag":"oom"`) {
		t.Errorf("bound run produced no findings; stdout:\n%s\nstderr:\n%s", out.String(), errb.String())
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
	c2.cmdRules("run --corpus " + corpus + " --bind " + bad)
	if !strings.Contains(errb2.String(), "UNRESOLVED") {
		t.Errorf("an unresolved logical keyspace must fail loud; stderr:\n%s", errb2.String())
	}
	if !c2.failed {
		t.Errorf("an unresolved binding must set c.failed (fail-loud), stderr:\n%s", errb2.String())
	}
	if strings.TrimSpace(out2.String()) != "" {
		t.Errorf("must NOT render a (falsely clean) findings table on a gap; stdout:\n%s", out2.String())
	}
}
