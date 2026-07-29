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
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/couchbase/query/value"
)

// writeMultiMatchesEntries writes a small entry pack (entry .sql++ files) into
// a FRESH temp dir (a sibling of the data dir, so it is never scanned as data) and
// returns that dir. The entries target the corpusTestSession logs/events
// keyspaces so they resolve against the current session's datastore at eval time.
func writeMultiMatchesEntries(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	entries := map[string]string{
		"error.sql++": "-- label: T1_error\n" +
			`SELECT * FROM logs l WHERE l.sev = "ERROR"`,
		"rare.sql++": "-- label: T3_rare\n" +
			`SELECT * FROM logs l WHERE l.msg = "rare_token_xyz"`,
		"login.sql++": "-- label: T5_login\n" +
			`SELECT * FROM events e WHERE e.act = "login"`,
	}
	for name, body := range entries {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// labelResultsFromRows turns `SELECT f.label, f.result` result rows into the LabelResult
// set they represent (so it compares against CompiledMultiQueryEntries.Run's labelResults).
func labelResultsFromRows(t *testing.T, rows []json.RawMessage) []LabelResult {
	t.Helper()
	var out []LabelResult
	for _, row := range rows {
		var m struct {
			Label  string          `json:"label"`
			Result json.RawMessage `json:"result"`
		}
		if err := json.Unmarshal(row, &m); err != nil {
			t.Fatalf("decoding matches row %q: %v", row, err)
		}
		out = append(out, LabelResult{Label: m.Label, Result: m.Result})
	}
	return out
}

func labelResultSetKeys(t *testing.T, fs []LabelResult) []string {
	t.Helper()
	keys := make([]string, 0, len(fs))
	for _, f := range fs {
		keys = append(keys, f.Label+"\t"+canonJSON(t, f.Result))
	}
	sort.Strings(keys)
	return keys
}

// TestMultiMatchesFromSource is the headline: `SELECT f.label, f.result FROM
// multi_matches('<pack>') f` returns EXACTLY the same tagged matches as running
// the pack directly via MultiQueryCompile().Run() (compared as sorted sets).
func TestMultiMatchesFromSource(t *testing.T) {
	sess := multiQueryTestSession(t)
	dir := writeMultiMatchesEntries(t)

	// Baseline: run the pack directly.
	entries, err := LoadMultiQueryEntries(dir)
	if err != nil {
		t.Fatalf("LoadMultiQueryEntries: %v", err)
	}
	dets := make([]MultiQueryEntry, 0, len(entries))
	for i := range entries {
		dets = append(dets, entries[i])
	}
	cc, err := sess.MultiQueryCompile(dets)
	if err != nil {
		t.Fatalf("MultiQueryCompile: %v", err)
	}
	baseline, err := cc.Run()
	if err != nil {
		t.Fatalf("cc.Run: %v", err)
	}
	if len(baseline) == 0 {
		t.Fatal("baseline produced no labelResults -- fixture invalid")
	}

	// Via the MULTI_MATCHES FROM-source.
	q := fmt.Sprintf("SELECT f.label, f.result FROM multi_matches(%q) AS f", dir)
	res, err := sess.Run(q)
	if err != nil {
		t.Fatalf("Run %q: %v", q, err)
	}
	got := labelResultsFromRows(t, res.Rows)

	gotKeys := labelResultSetKeys(t, got)
	wantKeys := labelResultSetKeys(t, baseline)
	if len(gotKeys) != len(wantKeys) {
		t.Fatalf("match count via FROM multi_matches(): got %d want %d\n got=%v\n want=%v",
			len(gotKeys), len(wantKeys), gotKeys, wantKeys)
	}
	for i := range gotKeys {
		if gotKeys[i] != wantKeys[i] {
			t.Fatalf("match[%d] mismatch:\n got=%q\n want=%q", i, gotKeys[i], wantKeys[i])
		}
	}
	// t.Logf("FROM multi_matches() matched %d labelResults", len(gotKeys))
}

// TestMultiMatchesMultipleDirs: multi_matches accepts several query dirs -- as an ARRAY
// arg or a comma-list string -- and fuses their entries into one shared-scan pack, so
// tiers stay organized on disk yet run as one entry set (IDEA-0034). Both spellings
// must match exactly the same labelResults as running the tiers' union directly.
func TestMultiMatchesMultipleDirs(t *testing.T) {
	mkTier := func(name, body string) string {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, name+".sql++"), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		return dir
	}
	tierA := mkTier("error", "-- label: T1_error\n"+`SELECT * FROM logs l WHERE l.sev = "ERROR"`)
	tierB := mkTier("login", "-- label: T5_login\n"+`SELECT * FROM events e WHERE e.act = "login"`)

	// Baseline: both tiers loaded together.
	baseKeys := func() []string {
		entries, err := LoadMultiQueryEntriesDirs([]string{tierA, tierB})
		if err != nil {
			t.Fatalf("LoadMultiQueryEntriesDirs: %v", err)
		}
		dets := make([]MultiQueryEntry, 0, len(entries))
		for i := range entries {
			dets = append(dets, entries[i])
		}
		cc, err := multiQueryTestSession(t).MultiQueryCompile(dets)
		if err != nil {
			t.Fatalf("MultiQueryCompile: %v", err)
		}
		found, err := cc.Run()
		if err != nil {
			t.Fatalf("cc.Run: %v", err)
		}
		return labelResultSetKeys(t, found)
	}()
	if len(baseKeys) == 0 {
		t.Fatal("baseline produced no labelResults -- fixtures invalid")
	}

	for _, tc := range []struct {
		name string
		arg  string
	}{
		{"array", fmt.Sprintf(`[%q, %q]`, tierA, tierB)},
		{"comma", fmt.Sprintf(`%q`, tierA+","+tierB)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := fmt.Sprintf("SELECT f.label, f.result FROM multi_matches(%s) AS f", tc.arg)
			res, err := multiQueryTestSession(t).Run(q)
			if err != nil {
				t.Fatalf("Run %q: %v", q, err)
			}
			gotKeys := labelResultSetKeys(t, labelResultsFromRows(t, res.Rows))
			if !reflect.DeepEqual(gotKeys, baseKeys) {
				t.Fatalf("%s-arg fused labelResults mismatch:\n got=%v\n want=%v", tc.name, gotKeys, baseKeys)
			}
		})
	}
}

// TestMultiMatchesStreamsViaStreamFnOp proves FROM multi_matches(...) converts to the
// generic STREAMING stream-fn op (op_stream_fn.go), NOT the materializing expr-scan
// -- so labelResults flow into the pipeline at bounded memory. It also checks LIMIT
// composes over the streaming source.
func TestMultiMatchesStreamsViaStreamFnOp(t *testing.T) {
	sess := multiQueryTestSession(t)
	dir := writeMultiMatchesEntries(t)

	q := fmt.Sprintf(`SELECT f.label FROM multi_matches(%q) AS f`, dir)
	res, err := sess.Run(q)
	if err != nil {
		t.Fatalf("Run %q: %v", q, err)
	}
	tree := FormatConvPlan(res.Plan)
	if !strings.Contains(tree, "stream-fn") {
		t.Fatalf("FROM multi_matches() should convert to a stream-fn op (streaming); plan:\n%s", tree)
	}
	if strings.Contains(tree, "expr-scan") {
		t.Fatalf("FROM multi_matches() must NOT materialize via expr-scan; plan:\n%s", tree)
	}

	// LIMIT composes with the streaming source (yields exactly the limited rows).
	q = fmt.Sprintf(`SELECT f.label FROM multi_matches(%q) AS f LIMIT 1`, dir)
	res, err = sess.Run(q)
	if err != nil {
		t.Fatalf("Run LIMIT %q: %v", q, err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("LIMIT 1 over multi_matches(): got %d rows, want 1", len(res.Rows))
	}
}

// TestMultiMatchesComposable: MULTI_MATCHES composes with WHERE (filter) and GROUP BY
// (aggregate) like any FROM source.
func TestMultiMatchesComposable(t *testing.T) {
	sess := multiQueryTestSession(t)
	dir := writeMultiMatchesEntries(t)

	// WHERE filter on the label: only the login entry's matches survive.
	q := fmt.Sprintf(`SELECT f.label FROM multi_matches(%q) AS f WHERE f.label = "T5_login"`, dir)
	res, err := sess.Run(q)
	if err != nil {
		t.Fatalf("Run %q: %v", q, err)
	}
	if len(res.Rows) == 0 {
		t.Fatal("WHERE f.label=T5_login returned no rows")
	}
	for _, row := range res.Rows {
		var m struct {
			Label string `json:"label"`
		}
		if err := json.Unmarshal(row, &m); err != nil {
			t.Fatalf("decode %q: %v", row, err)
		}
		if m.Label != "T5_login" {
			t.Fatalf("WHERE filter leaked label %q", m.Label)
		}
	}

	// GROUP BY label with COUNT(*): one row per entry that produced matches.
	q = fmt.Sprintf(`SELECT f.label, COUNT(*) AS hits FROM multi_matches(%q) AS f `+
		`GROUP BY f.label ORDER BY f.label`, dir)
	res, err = sess.Run(q)
	if err != nil {
		t.Fatalf("Run %q: %v", q, err)
	}
	counts := map[string]int{}
	for _, row := range res.Rows {
		var m struct {
			Label string `json:"label"`
			Hits  int    `json:"hits"`
		}
		if err := json.Unmarshal(row, &m); err != nil {
			t.Fatalf("decode group row %q: %v", row, err)
		}
		counts[m.Label] = m.Hits
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

// TestMultiMatchesPrepareExecute: because FROM multi_matches(...) is a plain SELECT,
// it PREPAREs and EXECUTEs for free, returning the same rows.
func TestMultiMatchesPrepareExecute(t *testing.T) {
	sess := multiQueryTestSession(t)
	dir := writeMultiMatchesEntries(t)

	direct := fmt.Sprintf(`SELECT f.label, f.result FROM multi_matches(%q) AS f`, dir)
	dres, err := sess.Run(direct)
	if err != nil {
		t.Fatalf("direct Run: %v", err)
	}
	wantKeys := labelResultSetKeys(t, labelResultsFromRows(t, dres.Rows))

	prep := fmt.Sprintf(`PREPARE fp AS SELECT f.label, f.result FROM multi_matches(%q) AS f`, dir)
	if _, err := sess.Run(prep); err != nil {
		t.Fatalf("PREPARE: %v", err)
	}
	eres, err := sess.Run("EXECUTE fp")
	if err != nil {
		t.Fatalf("EXECUTE: %v", err)
	}
	gotKeys := labelResultSetKeys(t, labelResultsFromRows(t, eres.Rows))

	if len(gotKeys) != len(wantKeys) {
		t.Fatalf("EXECUTE match count: got %d want %d", len(gotKeys), len(wantKeys))
	}
	for i := range gotKeys {
		if gotKeys[i] != wantKeys[i] {
			t.Fatalf("EXECUTE match[%d]: got %q want %q", i, gotKeys[i], wantKeys[i])
		}
	}
}

// TestMultiMatchesBindOpt: the opts object's `bind` resolves a logical-keyspace
// pack against this data source via a manifest (OpenSessionBound). The entry
// says `FROM app_logs` (logical); the manifest maps app_logs -> the logs glob.
func TestMultiMatchesBindOpt(t *testing.T) {
	sess := multiQueryTestSession(t)
	root := dataRootOfSession(t, sess)

	// A pack authored against a LOGICAL keyspace name.
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "logical.sql++"),
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

	q := fmt.Sprintf(`SELECT f.label, f.result FROM multi_matches(%q, {"bind": %q}) AS f`, dir, manifest)
	res, err := sess.Run(q)
	if err != nil {
		t.Fatalf("Run bound %q: %v", q, err)
	}
	got := labelResultsFromRows(t, res.Rows)
	if len(got) != 2 { // 2 ERROR rows in the logs fixture
		t.Fatalf("bound MULTI_MATCHES: got %d matches, want 2 (rows=%v)", len(got), res.Rows)
	}
	for _, f := range got {
		if f.Label != "B1_error" {
			t.Fatalf("bound MULTI_MATCHES: unexpected label %q", f.Label)
		}
	}
}

// TestMultiMatchesEmptyCorpusErrors: an empty/missing pack dir is a HARD error
// (not a silent empty result), consistent with fail-loud.
func TestMultiMatchesEmptyCorpusErrors(t *testing.T) {
	sess := multiQueryTestSession(t)

	// A dir with no *.sql++ files.
	empty := t.TempDir()
	q := fmt.Sprintf(`SELECT f.label FROM multi_matches(%q) AS f`, empty)
	if _, err := sess.Run(q); err == nil {
		t.Fatal("empty pack dir: expected an error, got nil")
	}

	// A missing dir.
	missing := filepath.Join(t.TempDir(), "does-not-exist")
	q = fmt.Sprintf(`SELECT f.label FROM multi_matches(%q) AS f`, missing)
	if _, err := sess.Run(q); err == nil {
		t.Fatal("missing pack dir: expected an error, got nil")
	}
}

// TestMultiMatchesAllRejectedErrors is the IDEA-0017 gate: a pack whose entries
// ALL fail to compile (here: an unresolvable keyspace, the "logical name without a
// bind" case) must ERROR loudly from MULTI_MATCHES, not return a silent empty array
// that reads as a clean bundle.
func TestMultiMatchesAllRejectedErrors(t *testing.T) {
	sess := multiQueryTestSession(t)
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "bad.sql++"),
		[]byte("-- label: T_BAD\nSELECT * FROM nosuch_ks x WHERE x.a = 1"), 0o644); err != nil {
		t.Fatal(err)
	}
	q := fmt.Sprintf(`SELECT f.label FROM multi_matches(%q) AS f`, dir)
	_, err := sess.Run(q)
	if err == nil {
		t.Fatal("all-rejected pack: expected an error, got nil (the silent-empty bug)")
	}
	msg := err.Error()
	for _, want := range []string{"no query", "rejected", "bind"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error should mention %q; got: %v", want, err)
		}
	}
}

// TestMultiMatchesPartialRejectWarns: when only SOME entries reject, MULTI_MATCHES
// still streams the runnable rest AND records a warning naming the skipped ones.
func TestMultiMatchesPartialRejectWarns(t *testing.T) {
	sess := multiQueryTestSession(t)
	dir := t.TempDir()
	write := func(name, body string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("good.sql++", "-- label: T_GOOD\n"+`SELECT * FROM logs l WHERE l.sev = "ERROR"`)
	write("bad.sql++", "-- label: T_BAD\n"+`SELECT * FROM nosuch_ks x WHERE x.a = 1`)

	q := fmt.Sprintf(`SELECT f.label FROM multi_matches(%q) AS f`, dir)
	res, err := sess.Run(q)
	if err != nil {
		t.Fatalf("partial-reject pack should still run the good entry: %v", err)
	}
	if len(res.Rows) == 0 {
		t.Fatal("expected labelResults from the runnable entry")
	}
	warned := false
	for _, w := range res.Warnings {
		if strings.Contains(w.Error(), "T_BAD") && strings.Contains(w.Error(), "skipped") {
			warned = true
		}
	}
	if !warned {
		t.Errorf("expected a warning naming the skipped T_BAD entry; warnings=%v", res.Warnings)
	}
}

// dataRootOfSession recovers the on-disk root of a session's file datastore (the
// same file:// URL trick multiMatchesSession uses for the bind path).
func dataRootOfSession(t *testing.T, sess *Session) string {
	t.Helper()
	url := sess.Store.Datastore.URL()
	const p = "file://"
	if len(url) <= len(p) || url[:len(p)] != p {
		t.Fatalf("unexpected datastore URL %q", url)
	}
	return url[len(p):]
}

// --- MULTI_MATCHES manifest binding + option parsing (multi_matches.go) ---
// These pure/file-based helpers were the thinnest part of the TVF (loadMultiMatchesBinding
// 32%, parseMultiMatchesOpts 43%): the happy paths run via the pack tests above, but the
// manifest formats and the many rejection branches were unexercised. Test them directly.

// TestLoadMultiMatchesBinding covers both manifest formats (JSON object and
// `logical = glob` lines with comments/blanks) plus every error branch.
func TestLoadMultiMatchesBinding(t *testing.T) {
	write := func(name, body string) string {
		p := filepath.Join(t.TempDir(), name)
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}

	// JSON object form.
	if b, err := loadMultiMatchesBinding(write("m.json", `{"logs":"logs/*.json","ev":"events/*"}`)); err != nil ||
		b["logs"] != "logs/*.json" || b["ev"] != "events/*" {
		t.Fatalf("json manifest: b=%v err=%v", b, err)
	}
	// `logical = glob` line form, with a comment and a blank line and stray whitespace.
	if b, err := loadMultiMatchesBinding(write("m.txt", "# manifest\n\nlogs = logs/*.json\n  ev =  events/*  \n")); err != nil ||
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
		_, err := loadMultiMatchesBinding(path)
		if err == nil || !strings.Contains(err.Error(), c.wantErr) {
			t.Errorf("%s: err = %v, want containing %q", c.name, err, c.wantErr)
		}
	}
}

// TestParseMultiMatchesOpts covers the lenient arg-1 object reader: bind, non-object
// input, a wrong-typed bind (ignored), and unknown keys (forward-compat).
func TestParseMultiMatchesOpts(t *testing.T) {
	o := parseMultiMatchesOpts(value.NewValue(map[string]interface{}{
		"bind": "manifest.txt",
	}))
	if o.bind != "manifest.txt" {
		t.Errorf("full opts = %+v", o)
	}

	// Non-object / nil -> zero opts.
	if got := parseMultiMatchesOpts(value.NewValue("a string")); got.bind != "" {
		t.Errorf("non-object -> %+v, want zero", got)
	}
	if got := parseMultiMatchesOpts(nil); got.bind != "" {
		t.Errorf("nil -> %+v, want zero", got)
	}

	// Wrong-typed bind is ignored.
	o = parseMultiMatchesOpts(value.NewValue(map[string]interface{}{"bind": 123}))
	if o.bind != "" {
		t.Errorf("non-string bind should be ignored, got %q", o.bind)
	}

	// Unknown key ignored (forward-compatible opts).
	if got := parseMultiMatchesOpts(value.NewValue(map[string]interface{}{"future": "x"})); got.bind != "" {
		t.Errorf("unknown key -> %+v, want zero", got)
	}
}

// TestRejectsMentionKeyspace / warnSink: the bind-hint heuristic and the no-op warn
// callback when the eval context isn't a *GlueContext.
func TestRejectsMentionKeyspaceAndWarnSink(t *testing.T) {
	if !rejectsMentionKeyspace([]RejectedEntry{{Label: "d1", Reason: "no such KEYSPACE `logs`"}}) {
		t.Error("a keyspace-resolution reason should trigger the bind hint")
	}
	if rejectsMentionKeyspace([]RejectedEntry{{Label: "d1", Reason: "syntax error near FROM"}}) {
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
