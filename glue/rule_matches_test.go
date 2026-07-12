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
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/couchbase/query/value"
)

// writeRuleMatchesCorpus writes a small detector corpus (recipe .sql++ files) into
// a FRESH temp dir (a sibling of the data dir, so it is never scanned as data) and
// returns that dir. The detectors target the corpusTestSession logs/events
// keyspaces so they resolve against the current session's datastore at eval time.
func writeRuleMatchesCorpus(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	recipes := map[string]string{
		"error.sql++": "-- label: T1_error\n" +
			`SELECT * FROM logs l WHERE l.sev = "ERROR"`,
		"rare.sql++": "-- label: T3_rare\n" +
			`SELECT * FROM logs l WHERE l.msg = "rare_token_xyz"`,
		"login.sql++": "-- label: T5_login\n" +
			`SELECT * FROM events e WHERE e.act = "login"`,
	}
	for name, body := range recipes {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// findingsFromRows turns `SELECT f.tag, f.evidence` result rows into the Finding
// set they represent (so it compares against CompiledCorpus.Run's findings).
func findingsFromRows(t *testing.T, rows []json.RawMessage) []Finding {
	t.Helper()
	var out []Finding
	for _, row := range rows {
		var m struct {
			Tag      string          `json:"tag"`
			Evidence json.RawMessage `json:"evidence"`
		}
		if err := json.Unmarshal(row, &m); err != nil {
			t.Fatalf("decoding matches row %q: %v", row, err)
		}
		out = append(out, Finding{Tag: m.Tag, Evidence: m.Evidence})
	}
	return out
}

func findingSetKeys(t *testing.T, fs []Finding) []string {
	t.Helper()
	keys := make([]string, 0, len(fs))
	for _, f := range fs {
		keys = append(keys, f.Tag+"\t"+canonJSON(t, f.Evidence))
	}
	sort.Strings(keys)
	return keys
}

// TestRuleMatchesFromSource is the headline: `SELECT f.tag, f.evidence FROM
// rule_matches('<corpus>') f` returns EXACTLY the same tagged matches as running
// the corpus directly via CorpusCompile().Run() (compared as sorted sets).
func TestRuleMatchesFromSource(t *testing.T) {
	sess := corpusTestSession(t)
	corpus := writeRuleMatchesCorpus(t)

	// Baseline: run the corpus directly.
	recipes, err := LoadCorpus(corpus)
	if err != nil {
		t.Fatalf("LoadCorpus: %v", err)
	}
	dets := make([]CorpusDetector, 0, len(recipes))
	for i := range recipes {
		dets = append(dets, recipes[i].AsDetector())
	}
	cc, err := sess.CorpusCompile(dets)
	if err != nil {
		t.Fatalf("CorpusCompile: %v", err)
	}
	baseline, err := cc.Run()
	if err != nil {
		t.Fatalf("cc.Run: %v", err)
	}
	if len(baseline) == 0 {
		t.Fatal("baseline produced no findings -- fixture invalid")
	}

	// Via the RULE_MATCHES FROM-source.
	q := fmt.Sprintf("SELECT f.tag, f.evidence FROM rule_matches(%q) AS f", corpus)
	res, err := sess.Run(q)
	if err != nil {
		t.Fatalf("Run %q: %v", q, err)
	}
	got := findingsFromRows(t, res.Rows)

	gotKeys := findingSetKeys(t, got)
	wantKeys := findingSetKeys(t, baseline)
	if len(gotKeys) != len(wantKeys) {
		t.Fatalf("match count via FROM rule_matches(): got %d want %d\n got=%v\n want=%v",
			len(gotKeys), len(wantKeys), gotKeys, wantKeys)
	}
	for i := range gotKeys {
		if gotKeys[i] != wantKeys[i] {
			t.Fatalf("match[%d] mismatch:\n got=%q\n want=%q", i, gotKeys[i], wantKeys[i])
		}
	}
	t.Logf("FROM rule_matches() matched %d findings", len(gotKeys))
}

// TestRuleMatchesStreamsViaStreamFnOp proves FROM rule_matches(...) converts to the
// generic STREAMING stream-fn op (op_stream_fn.go), NOT the materializing expr-scan
// -- so findings flow into the pipeline at bounded memory. It also checks LIMIT
// composes over the streaming source.
func TestRuleMatchesStreamsViaStreamFnOp(t *testing.T) {
	sess := corpusTestSession(t)
	corpus := writeRuleMatchesCorpus(t)

	q := fmt.Sprintf(`SELECT f.tag FROM rule_matches(%q) AS f`, corpus)
	res, err := sess.Run(q)
	if err != nil {
		t.Fatalf("Run %q: %v", q, err)
	}
	tree := FormatConvPlan(res.Plan)
	if !strings.Contains(tree, "stream-fn") {
		t.Fatalf("FROM rule_matches() should convert to a stream-fn op (streaming); plan:\n%s", tree)
	}
	if strings.Contains(tree, "expr-scan") {
		t.Fatalf("FROM rule_matches() must NOT materialize via expr-scan; plan:\n%s", tree)
	}

	// LIMIT composes with the streaming source (yields exactly the limited rows).
	q = fmt.Sprintf(`SELECT f.tag FROM rule_matches(%q) AS f LIMIT 1`, corpus)
	res, err = sess.Run(q)
	if err != nil {
		t.Fatalf("Run LIMIT %q: %v", q, err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("LIMIT 1 over rule_matches(): got %d rows, want 1", len(res.Rows))
	}
}

// TestRuleMatchesComposable: RULE_MATCHES composes with WHERE (filter) and GROUP BY
// (aggregate) like any FROM source.
func TestRuleMatchesComposable(t *testing.T) {
	sess := corpusTestSession(t)
	corpus := writeRuleMatchesCorpus(t)

	// WHERE filter on the tag: only the login detector's matches survive.
	q := fmt.Sprintf(`SELECT f.tag FROM rule_matches(%q) AS f WHERE f.tag = "T5_login"`, corpus)
	res, err := sess.Run(q)
	if err != nil {
		t.Fatalf("Run %q: %v", q, err)
	}
	if len(res.Rows) == 0 {
		t.Fatal("WHERE f.tag=T5_login returned no rows")
	}
	for _, row := range res.Rows {
		var m struct {
			Tag string `json:"tag"`
		}
		if err := json.Unmarshal(row, &m); err != nil {
			t.Fatalf("decode %q: %v", row, err)
		}
		if m.Tag != "T5_login" {
			t.Fatalf("WHERE filter leaked tag %q", m.Tag)
		}
	}

	// GROUP BY tag with COUNT(*): one row per detector that produced matches.
	q = fmt.Sprintf(`SELECT f.tag, COUNT(*) AS hits FROM rule_matches(%q) AS f `+
		`GROUP BY f.tag ORDER BY f.tag`, corpus)
	res, err = sess.Run(q)
	if err != nil {
		t.Fatalf("Run %q: %v", q, err)
	}
	counts := map[string]int{}
	for _, row := range res.Rows {
		var m struct {
			Tag  string `json:"tag"`
			Hits int    `json:"hits"`
		}
		if err := json.Unmarshal(row, &m); err != nil {
			t.Fatalf("decode group row %q: %v", row, err)
		}
		counts[m.Tag] = m.Hits
	}
	// The logs fixture has 2 ERROR rows and 1 with msg=rare_token_xyz; events has 2
	// logins. (rare_token_xyz row is also sev=ERROR, so T1_error sees 2.)
	if counts["T1_error"] != 2 {
		t.Errorf("T1_error hits = %d, want 2 (counts=%v)", counts["T1_error"], counts)
	}
	if counts["T3_rare"] != 1 {
		t.Errorf("T3_rare hits = %d, want 1 (counts=%v)", counts["T3_rare"], counts)
	}
	if counts["T5_login"] != 2 {
		t.Errorf("T5_login hits = %d, want 2 (counts=%v)", counts["T5_login"], counts)
	}
}

// TestRuleMatchesPrepareExecute: because FROM rule_matches(...) is a plain SELECT,
// it PREPAREs and EXECUTEs for free, returning the same rows.
func TestRuleMatchesPrepareExecute(t *testing.T) {
	sess := corpusTestSession(t)
	corpus := writeRuleMatchesCorpus(t)

	direct := fmt.Sprintf(`SELECT f.tag, f.evidence FROM rule_matches(%q) AS f`, corpus)
	dres, err := sess.Run(direct)
	if err != nil {
		t.Fatalf("direct Run: %v", err)
	}
	wantKeys := findingSetKeys(t, findingsFromRows(t, dres.Rows))

	prep := fmt.Sprintf(`PREPARE fp AS SELECT f.tag, f.evidence FROM rule_matches(%q) AS f`, corpus)
	if _, err := sess.Run(prep); err != nil {
		t.Fatalf("PREPARE: %v", err)
	}
	eres, err := sess.Run("EXECUTE fp")
	if err != nil {
		t.Fatalf("EXECUTE: %v", err)
	}
	gotKeys := findingSetKeys(t, findingsFromRows(t, eres.Rows))

	if len(gotKeys) != len(wantKeys) {
		t.Fatalf("EXECUTE match count: got %d want %d", len(gotKeys), len(wantKeys))
	}
	for i := range gotKeys {
		if gotKeys[i] != wantKeys[i] {
			t.Fatalf("EXECUTE match[%d]: got %q want %q", i, gotKeys[i], wantKeys[i])
		}
	}
}

// TestRuleMatchesBindOpt: the opts object's `bind` resolves a logical-keyspace
// corpus against this data source via a manifest (OpenSessionBound). The detector
// says `FROM app_logs` (logical); the manifest maps app_logs -> the logs glob.
func TestRuleMatchesBindOpt(t *testing.T) {
	sess := corpusTestSession(t)
	root := dataRootOfSession(t, sess)

	// A corpus authored against a LOGICAL keyspace name.
	corpus := t.TempDir()
	if err := os.WriteFile(filepath.Join(corpus, "logical.sql++"),
		[]byte("-- label: B1_error\n"+`SELECT * FROM app_logs a WHERE a.sev = "ERROR"`),
		0o644); err != nil {
		t.Fatal(err)
	}

	// A manifest binding the logical name to the physical logs files.
	manifest := filepath.Join(t.TempDir(), "manifest.json")
	if err := os.WriteFile(manifest,
		[]byte(fmt.Sprintf(`{"app_logs": %q}`, filepath.Join(root, "default", "logs", "*.jsonl"))),
		0o644); err != nil {
		t.Fatal(err)
	}

	q := fmt.Sprintf(`SELECT f.tag, f.evidence FROM rule_matches(%q, {"bind": %q}) AS f`, corpus, manifest)
	res, err := sess.Run(q)
	if err != nil {
		t.Fatalf("Run bound %q: %v", q, err)
	}
	got := findingsFromRows(t, res.Rows)
	if len(got) != 2 { // 2 ERROR rows in the logs fixture
		t.Fatalf("bound RULE_MATCHES: got %d matches, want 2 (rows=%v)", len(got), res.Rows)
	}
	for _, f := range got {
		if f.Tag != "B1_error" {
			t.Fatalf("bound RULE_MATCHES: unexpected tag %q", f.Tag)
		}
	}
}

// TestRuleMatchesEmptyCorpusErrors: an empty/missing corpus dir is a HARD error
// (not a silent empty result), consistent with fail-loud.
func TestRuleMatchesEmptyCorpusErrors(t *testing.T) {
	sess := corpusTestSession(t)

	// A dir with no *.sql++ files.
	empty := t.TempDir()
	q := fmt.Sprintf(`SELECT f.tag FROM rule_matches(%q) AS f`, empty)
	if _, err := sess.Run(q); err == nil {
		t.Fatal("empty corpus dir: expected an error, got nil")
	}

	// A missing dir.
	missing := filepath.Join(t.TempDir(), "does-not-exist")
	q = fmt.Sprintf(`SELECT f.tag FROM rule_matches(%q) AS f`, missing)
	if _, err := sess.Run(q); err == nil {
		t.Fatal("missing corpus dir: expected an error, got nil")
	}
}

// TestRuleMatchesAllRejectedErrors is the IDEA-0017 gate: a corpus whose detectors
// ALL fail to compile (here: an unresolvable keyspace, the "logical name without a
// bind" case) must ERROR loudly from RULE_MATCHES, not return a silent empty array
// that reads as a clean bundle.
func TestRuleMatchesAllRejectedErrors(t *testing.T) {
	sess := corpusTestSession(t)
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "bad.sql++"),
		[]byte("-- label: T_BAD\nSELECT * FROM nosuch_ks x WHERE x.a = 1"), 0o644); err != nil {
		t.Fatal(err)
	}
	q := fmt.Sprintf(`SELECT f.tag FROM rule_matches(%q) AS f`, dir)
	_, err := sess.Run(q)
	if err == nil {
		t.Fatal("all-rejected corpus: expected an error, got nil (the silent-empty bug)")
	}
	msg := err.Error()
	for _, want := range []string{"no query", "rejected", "bind"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error should mention %q; got: %v", want, err)
		}
	}
}

// TestRuleMatchesPartialRejectWarns: when only SOME detectors reject, RULE_MATCHES
// still streams the runnable rest AND records a warning naming the skipped ones.
func TestRuleMatchesPartialRejectWarns(t *testing.T) {
	sess := corpusTestSession(t)
	dir := t.TempDir()
	write := func(name, body string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("good.sql++", "-- label: T_GOOD\n"+`SELECT * FROM logs l WHERE l.sev = "ERROR"`)
	write("bad.sql++", "-- label: T_BAD\n"+`SELECT * FROM nosuch_ks x WHERE x.a = 1`)

	q := fmt.Sprintf(`SELECT f.tag FROM rule_matches(%q) AS f`, dir)
	res, err := sess.Run(q)
	if err != nil {
		t.Fatalf("partial-reject corpus should still run the good detector: %v", err)
	}
	if len(res.Rows) == 0 {
		t.Fatal("expected findings from the runnable detector")
	}
	warned := false
	for _, w := range res.Warnings {
		if strings.Contains(w.Error(), "T_BAD") && strings.Contains(w.Error(), "skipped") {
			warned = true
		}
	}
	if !warned {
		t.Errorf("expected a warning naming the skipped T_BAD detector; warnings=%v", res.Warnings)
	}
}

// dataRootOfSession recovers the on-disk root of a session's file datastore (the
// same file:// URL trick ruleMatchesSession uses for the bind path).
func dataRootOfSession(t *testing.T, sess *Session) string {
	t.Helper()
	url := sess.Store.Datastore.URL()
	const p = "file://"
	if len(url) <= len(p) || url[:len(p)] != p {
		t.Fatalf("unexpected datastore URL %q", url)
	}
	return url[len(p):]
}

// --- RULE_MATCHES manifest binding + option parsing (rule_matches.go) ---
// These pure/file-based helpers were the thinnest part of the TVF (loadRuleMatchesBinding
// 32%, parseRuleMatchesOpts 43%): the happy paths run via the corpus tests above, but the
// manifest formats and the many rejection branches were unexercised. Test them directly.

// TestLoadRuleMatchesBinding covers both manifest formats (JSON object and
// `logical = glob` lines with comments/blanks) plus every error branch.
func TestLoadRuleMatchesBinding(t *testing.T) {
	write := func(name, body string) string {
		p := filepath.Join(t.TempDir(), name)
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}

	// JSON object form.
	if b, err := loadRuleMatchesBinding(write("m.json", `{"logs":"logs/*.json","ev":"events/*"}`)); err != nil ||
		b["logs"] != "logs/*.json" || b["ev"] != "events/*" {
		t.Fatalf("json manifest: b=%v err=%v", b, err)
	}
	// `logical = glob` line form, with a comment and a blank line and stray whitespace.
	if b, err := loadRuleMatchesBinding(write("m.txt", "# manifest\n\nlogs = logs/*.json\n  ev =  events/*  \n")); err != nil ||
		b["logs"] != "logs/*.json" || b["ev"] != "events/*" {
		t.Fatalf("line manifest: b=%v err=%v", b, err)
	}

	for _, c := range []struct {
		name, body, wantErr string
		missing             bool
	}{
		{name: "missing-file", missing: true, wantErr: "reading manifest"},
		{name: "bad-json", body: `{ not valid`, wantErr: "(JSON)"},
		{name: "no-equals", body: "logs logs/*.json", wantErr: "want `logical = glob`"},
		{name: "empty-glob", body: "logs =", wantErr: "empty logical or glob"},
		{name: "empty-logical", body: " = logs/*", wantErr: "empty logical or glob"},
		{name: "no-bindings", body: "# only a comment\n\n", wantErr: "no bindings"},
	} {
		path := filepath.Join(t.TempDir(), "nope")
		if !c.missing {
			path = write(c.name+".txt", c.body)
		}
		_, err := loadRuleMatchesBinding(path)
		if err == nil || !strings.Contains(err.Error(), c.wantErr) {
			t.Errorf("%s: err = %v, want containing %q", c.name, err, c.wantErr)
		}
	}
}

// TestParseRuleMatchesOpts covers the lenient arg-1 object reader: bind + versions,
// non-object input, wrong-typed fields (ignored), and unknown keys (forward-compat).
func TestParseRuleMatchesOpts(t *testing.T) {
	o := parseRuleMatchesOpts(value.NewValue(map[string]interface{}{
		"bind":     "manifest.txt",
		"versions": []interface{}{"v1", "v2"},
	}))
	if o.bind != "manifest.txt" || len(o.versions) != 2 || o.versions[0] != "v1" || o.versions[1] != "v2" {
		t.Errorf("full opts = %+v", o)
	}

	// Non-object / nil -> zero opts.
	if got := parseRuleMatchesOpts(value.NewValue("a string")); got.bind != "" || got.versions != nil {
		t.Errorf("non-object -> %+v, want zero", got)
	}
	if got := parseRuleMatchesOpts(nil); got.bind != "" || got.versions != nil {
		t.Errorf("nil -> %+v, want zero", got)
	}

	// Wrong-typed fields are ignored; non-string versions entries are skipped.
	o = parseRuleMatchesOpts(value.NewValue(map[string]interface{}{
		"bind":     123,
		"versions": []interface{}{"keep", 5, "also"},
	}))
	if o.bind != "" {
		t.Errorf("non-string bind should be ignored, got %q", o.bind)
	}
	if len(o.versions) != 2 || o.versions[0] != "keep" || o.versions[1] != "also" {
		t.Errorf("versions filter = %v, want [keep also]", o.versions)
	}

	// Unknown key ignored (forward-compatible opts).
	if got := parseRuleMatchesOpts(value.NewValue(map[string]interface{}{"future": "x"})); got.bind != "" || got.versions != nil {
		t.Errorf("unknown key -> %+v, want zero", got)
	}
}

// TestRejectsMentionKeyspace / warnSink: the bind-hint heuristic and the no-op warn
// callback when the eval context isn't a *GlueContext.
func TestRejectsMentionKeyspaceAndWarnSink(t *testing.T) {
	if !rejectsMentionKeyspace([]RejectedDetector{{Tag: "d1", Reason: "no such KEYSPACE `logs`"}}) {
		t.Error("a keyspace-resolution reason should trigger the bind hint")
	}
	if rejectsMentionKeyspace([]RejectedDetector{{Tag: "d1", Reason: "syntax error near FROM"}}) {
		t.Error("a non-keyspace reason should not trigger the hint")
	}
	if rejectsMentionKeyspace(nil) {
		t.Error("no rejects -> false")
	}

	// warnSink with a non-*GlueContext context returns a no-op that must not panic.
	warnSink(nil)("this warning is dropped")

	// warnSink with a real *GlueContext routes the message to its warning collector.
	gc := &GlueContext{}
	warnSink(gc)("a real warning")
	if got := len(gc.GetErrors()); got != 1 {
		t.Errorf("warnSink(*GlueContext) collected %d warnings, want 1", got)
	}
}
