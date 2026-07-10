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
	"sort"
	"testing"

	"github.com/couchbase/n1k1/base"
)

// corpusTestSession writes a two-keyspace file datastore (logs + events) and opens
// a Session over it.
func corpusTestSession(t *testing.T) *Session {
	t.Helper()
	dir := t.TempDir()

	write := func(ks, name, body string) {
		d := filepath.Join(dir, "default", ks)
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(d, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	write("logs", "l.jsonl",
		`{"id":"a","sev":"ERROR","code":5,"msg":"disk full"}`+"\n"+
			`{"id":"b","sev":"ERROR","code":1,"msg":"rare_token_xyz"}`+"\n"+
			`{"id":"c","sev":"INFO","code":2,"msg":"ok"}`+"\n"+
			`{"id":"d","sev":"WARN","code":9,"msg":"high load"}`+"\n")
	write("events", "e.jsonl",
		`{"id":"e1","act":"login","u":"x"}`+"\n"+
			`{"id":"e2","act":"logout","u":"y"}`+"\n"+
			`{"id":"e3","act":"login","u":"z"}`+"\n")

	sess, err := OpenSession(dir, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	return sess
}

// canonJSON canonicalizes a JSON value (sorted keys via json.Marshal of the
// unmarshaled value) so evidence rows compare independent of field order.
func canonJSON(t *testing.T, raw []byte) string {
	t.Helper()
	var v interface{}
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("canonJSON %q: %v", raw, err)
	}
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("canonJSON marshal: %v", err)
	}
	return string(b)
}

// TestCorpusCompileDifferential is the correctness gate (DESIGN-prepare.md phase
// 6): CorpusCompile over a small corpus must yield findings EQUIVALENT to running
// each fused detector's ORIGINAL SQL separately and tagging its matched rows. The
// corpus deliberately exercises every lever:
//
//   - T1/T2 share the sub-predicate `l.sev = "ERROR"` -> corpus CSE hoists it.
//   - T3 keys on a distinct rare string literal -> the Aho-Corasick index.
//   - T4 has no WHERE -> the always-true predicate.
//   - T5 targets a SECOND keyspace -> the per-keyspace union-all.
//   - T6 is a GROUP BY -> must land in Standalone (valid but non-fusable) and RUN,
//     producing its findings via the full pipeline -- not silently dropped.
//
// Findings are compared as SORTED SETS (order across the standalone runs, the
// union-all, and the interleaved fan-out is not guaranteed).
func TestCorpusCompileDifferential(t *testing.T) {
	sess := corpusTestSession(t)

	// Each fusable detector plus the equivalent standalone baseline that yields the
	// raw matched row (SELECT RAW <alias>), i.e. the same whole-row evidence.
	type det struct {
		tag      string
		stmt     string
		baseline string
	}
	fused := []det{
		{"T1_error", `SELECT * FROM logs l WHERE l.sev = "ERROR"`,
			`SELECT RAW l FROM logs l WHERE l.sev = "ERROR"`},
		{"T2_error_hot", `SELECT * FROM logs l WHERE l.sev = "ERROR" AND l.code > 3`,
			`SELECT RAW l FROM logs l WHERE l.sev = "ERROR" AND l.code > 3`},
		{"T3_rare", `SELECT * FROM logs l WHERE l.msg = "rare_token_xyz"`,
			`SELECT RAW l FROM logs l WHERE l.msg = "rare_token_xyz"`},
		{"T4_all", `SELECT * FROM logs l`,
			`SELECT RAW l FROM logs l`},
		{"T5_login", `SELECT * FROM events e WHERE e.act = "login"`,
			`SELECT RAW e FROM events e WHERE e.act = "login"`},
	}
	standaloneTag := "T6_group"
	standaloneStmt := `SELECT sev, count(*) AS n FROM logs GROUP BY sev`

	// Assemble the corpus (fusable + the one non-canonical detector).
	corpus := make([]CorpusDetector, 0, len(fused)+1)
	for _, d := range fused {
		corpus = append(corpus, CorpusDetector{Tag: d.tag, Stmt: d.stmt})
	}
	corpus = append(corpus, CorpusDetector{Tag: standaloneTag, Stmt: standaloneStmt})

	cc, err := sess.CorpusCompile(corpus)
	if err != nil {
		t.Fatalf("CorpusCompile: %v", err)
	}

	// (1) The GROUP BY detector must be classified STANDALONE (valid but non-fusable),
	// not Rejected and not silently dropped -- it will RUN and produce findings.
	if len(cc.Standalone) != 1 || cc.Standalone[0].Tag != standaloneTag {
		t.Fatalf("expected exactly 1 standalone (%s), got %+v", standaloneTag, cc.Standalone)
	}
	if len(cc.Rejected) != 0 {
		t.Fatalf("expected no rejected detectors, got %+v", cc.Rejected)
	}
	// t.Logf("standalone: %+v", cc.Standalone)

	// (2) Structural sanity: two keyspaces -> a union-all of two broadcast-indexed
	// fan-outs, and the logs group carries a CSE precompute project (T1/T2 share).
	if cc.Plan == nil {
		t.Fatal("nil plan")
	}
	if cc.Plan.Kind != "union-all" || len(cc.Plan.Children) != 2 {
		t.Fatalf("plan top = %q children=%d, want union-all of 2", cc.Plan.Kind, len(cc.Plan.Children))
	}
	sawCSE := false
	for _, bc := range cc.Plan.Children {
		if bc.Kind != "broadcast-indexed" {
			t.Fatalf("per-keyspace op = %q, want broadcast-indexed", bc.Kind)
		}
		if len(bc.Children) == 1 && bc.Children[0].Kind == "project" {
			for _, l := range bc.Children[0].Labels {
				if len(l) >= 4 && l[:4] == "^cse" {
					sawCSE = true
				}
			}
		}
	}
	if !sawCSE {
		t.Errorf("expected a CSE precompute (^cse column) in the logs broadcast; plan=%s", dumpPlan(cc.Plan))
	}

	// (3) Equivalence: the corpus findings set == the union of each fused detector's
	// baseline rows PLUS the standalone detector's own SELECT rows, each tagged with
	// its id.
	gotFindings, err := cc.Run()
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	got := make([]string, 0, len(gotFindings))
	for _, f := range gotFindings {
		got = append(got, f.Tag+"\t"+canonJSON(t, f.Evidence))
	}
	sort.Strings(got)

	var want []string
	for _, d := range fused {
		res, err := sess.Run(d.baseline)
		if err != nil {
			t.Fatalf("baseline %q: %v", d.baseline, err)
		}
		for _, row := range res.Rows {
			want = append(want, d.tag+"\t"+canonJSON(t, row))
		}
	}
	// The standalone GROUP BY detector runs its own SQL; its evidence is that SELECT's
	// REAL projection (the evidence asymmetry vs. the fused whole-row path), so its
	// expected rows come straight from running standaloneStmt.
	saRes, err := sess.Run(standaloneStmt)
	if err != nil {
		t.Fatalf("standalone baseline %q: %v", standaloneStmt, err)
	}
	if len(saRes.Rows) == 0 {
		t.Fatal("standalone GROUP BY produced no rows -- fixture invalid")
	}
	for _, row := range saRes.Rows {
		want = append(want, standaloneTag+"\t"+canonJSON(t, row))
	}
	sort.Strings(want)

	if len(got) != len(want) {
		t.Fatalf("finding count: got %d, want %d\n got=%v\n want=%v", len(got), len(want), got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("finding[%d] mismatch:\n got=%q\n want=%q\n --- full got=%v\n --- full want=%v",
				i, got[i], want[i], got, want)
		}
	}
	// t.Logf("matched %d findings across %d fused + 1 standalone detector", len(got), len(fused))
}

// TestCorpusCompileSingleKeyspace: a corpus confined to one keyspace returns the
// per-keyspace broadcast directly (no union-all wrapper), and an empty / all-
// unfusable corpus yields a nil plan (Run -> no findings).
func TestCorpusCompileSingleKeyspace(t *testing.T) {
	sess := corpusTestSession(t)

	cc, err := sess.CorpusCompile([]CorpusDetector{
		{Tag: "a", Stmt: `SELECT * FROM logs l WHERE l.sev = "ERROR"`},
		{Tag: "b", Stmt: `SELECT * FROM logs l WHERE l.code > 3`},
	})
	if err != nil {
		t.Fatalf("CorpusCompile: %v", err)
	}
	if cc.Plan == nil || cc.Plan.Kind != "broadcast-indexed" {
		t.Fatalf("single-keyspace plan = %v, want a bare broadcast-indexed", cc.Plan)
	}
	if len(cc.Standalone) != 0 || len(cc.Rejected) != 0 {
		t.Fatalf("unexpected non-fused: standalone=%+v rejected=%+v", cc.Standalone, cc.Rejected)
	}
	findings, err := cc.Run()
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(findings) == 0 {
		t.Fatal("expected findings from single-keyspace corpus")
	}

	// Empty corpus -> nil plan, no findings.
	empty, err := sess.CorpusCompile(nil)
	if err != nil {
		t.Fatalf("CorpusCompile(nil): %v", err)
	}
	if empty.Plan != nil {
		t.Fatalf("empty corpus plan = %v, want nil", empty.Plan)
	}
	fs, err := empty.Run()
	if err != nil || len(fs) != 0 {
		t.Fatalf("empty Run: findings=%v err=%v", fs, err)
	}
}

// TestCorpusCompileBoxedPredicate: a predicate that does not lower to a native
// tree stays boxed (alias remapped to SELF) and STILL evaluates against the shared
// scan. Uses a function-heavy predicate to force the boxed lane, and checks the
// fused findings match the standalone baseline.
func TestCorpusCompileBoxedPredicate(t *testing.T) {
	sess := corpusTestSession(t)

	// l.msg LIKE '%high%load%' -- an interior-wildcard LIKE the native lowering
	// declines (only the plain %lit% form lowers to CONTAINS), so the predicate
	// stays boxed; alias `l` is remapped to SELF for the shared row.
	stmt := `SELECT * FROM logs l WHERE l.msg LIKE "%high%load%"`
	baseline := `SELECT RAW l FROM logs l WHERE l.msg LIKE "%high%load%"`

	cc, err := sess.CorpusCompile([]CorpusDetector{{Tag: "boxed", Stmt: stmt}})
	if err != nil {
		t.Fatalf("CorpusCompile: %v", err)
	}
	if len(cc.Standalone) != 0 || len(cc.Rejected) != 0 {
		t.Fatalf("boxed detector unexpectedly non-fused: standalone=%+v rejected=%+v", cc.Standalone, cc.Rejected)
	}
	findings, err := cc.Run()
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	res, err := sess.Run(baseline)
	if err != nil {
		t.Fatalf("baseline: %v", err)
	}
	if len(findings) != len(res.Rows) {
		t.Fatalf("boxed findings=%d baseline rows=%d", len(findings), len(res.Rows))
	}
	if len(findings) == 0 {
		t.Fatal("expected at least one boxed finding (row d: 'high load')")
	}
	for i := range findings {
		if canonJSON(t, findings[i].Evidence) != canonJSON(t, res.Rows[i]) {
			t.Fatalf("boxed evidence mismatch: %s vs %s", findings[i].Evidence, res.Rows[i])
		}
	}
}

// TestCorpusCompileASOFStandalone is the headline for the standalone class: a corpus
// that mixes fusable single-source detectors with a NON-fusable ASOF/argmax
// correlated-subquery detector. The ASOF detector must be classified STANDALONE (not
// fused, not rejected) and, at Run() time, execute through the FULL pipeline so its
// nearest-preceding merge-join lowering FIRES -- producing findings identical to
// running that SQL alone, unioned with the fused findings.
func TestCorpusCompileASOFStandalone(t *testing.T) {
	root := t.TempDir()

	// A plain 'logs' keyspace for the fusable detectors (proven to fuse -- see the
	// differential test), PLUS recipe-matched elog/rlog ns_server_log keyspaces (with
	// a normalized int64 `ts`) for the ASOF correlated subquery.
	logsDir := filepath.Join(root, "default", "logs")
	if err := os.MkdirAll(logsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(logsDir, "l.jsonl"),
		[]byte(`{"id":"a","sev":"ERROR","code":5}`+"\n"+
			`{"id":"b","sev":"ERROR","code":1}`+"\n"+
			`{"id":"c","sev":"INFO","code":2}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	asofWriteKS(t, root, "elog", "ns_server.error.log",
		nsLine("2026-05-17T15:36:11.100+02:00", "n1", "e-100")+
			nsLine("2026-05-17T15:36:13.300+02:00", "n1", "e-300")+
			nsLine("2026-05-17T15:36:15.500+02:00", "n1", "e-500"))
	asofWriteKS(t, root, "rlog", "ns_server.rebalance.log",
		nsLine("2026-05-17T15:36:12.200+02:00", "n1", "r-200")+
			nsLine("2026-05-17T15:36:14.400+02:00", "n1", "r-400"))

	sess, err := OpenSession(root, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	defer sess.Close()

	// Keep the ASOF lowering enabled (default on) -- isolated so we can assert it fired.
	prev := EnableASOFRewrite
	EnableASOFRewrite = true
	defer func() { EnableASOFRewrite = prev }()

	asofStmt := "SELECT e.ts AS ts, " +
		"(SELECT r.msg FROM default:rlog r WHERE r.ts <= e.ts ORDER BY r.ts DESC LIMIT 1) AS state " +
		"FROM default:elog e"
	corpus := []CorpusDetector{
		{Tag: "F_err", Stmt: `SELECT * FROM logs l WHERE l.sev = "ERROR"`},
		{Tag: "F_hot", Stmt: `SELECT * FROM logs l WHERE l.sev = "ERROR" AND l.code > 3`},
		{Tag: "ASOF", Stmt: asofStmt},
	}

	cc, err := sess.CorpusCompile(corpus)
	if err != nil {
		t.Fatalf("CorpusCompile: %v", err)
	}

	// (a) The ASOF detector is STANDALONE -- not fused, not rejected.
	if len(cc.Rejected) != 0 {
		t.Fatalf("unexpected rejected: %+v", cc.Rejected)
	}
	if len(cc.Standalone) != 1 || cc.Standalone[0].Tag != "ASOF" {
		t.Fatalf("expected ASOF as the sole standalone detector, got %+v", cc.Standalone)
	}
	// The two fusable logs detectors folded into a single-keyspace broadcast plan.
	if cc.Plan == nil || cc.Plan.Kind != "broadcast-indexed" {
		t.Fatalf("expected a fused broadcast-indexed plan for the 2 fusable detectors, got %v", cc.Plan)
	}

	// (c) The ASOF lowering must FIRE during the corpus Run (proving the standalone
	// detector ran the merge-join, not nothing).
	before := AsofRewriteApplied
	got, err := cc.Run()
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if AsofRewriteApplied <= before {
		t.Fatalf("ASOF lowering did not fire during the corpus Run (AsofRewriteApplied did not advance from %d)", before)
	}

	// (b) The corpus's ASOF findings == running the ASOF SQL alone (its real
	// projection as evidence), compared as sorted sets.
	var asofGot []string
	fusedCount := 0
	for _, f := range got {
		switch f.Tag {
		case "ASOF":
			asofGot = append(asofGot, canonJSON(t, f.Evidence))
		case "F_err", "F_hot":
			fusedCount++
		}
	}
	sort.Strings(asofGot)

	res, err := sess.Run(asofStmt)
	if err != nil {
		t.Fatalf("standalone ASOF Run: %v", err)
	}
	var asofWant []string
	for _, row := range res.Rows {
		asofWant = append(asofWant, canonJSON(t, row))
	}
	sort.Strings(asofWant)

	if len(asofGot) == 0 {
		t.Fatal("no ASOF findings produced by the corpus")
	}
	if len(asofGot) != len(asofWant) {
		t.Fatalf("ASOF findings count: got %d want %d\n got=%v\n want=%v", len(asofGot), len(asofWant), asofGot, asofWant)
	}
	for i := range asofGot {
		if asofGot[i] != asofWant[i] {
			t.Fatalf("ASOF finding[%d]: got %s want %s", i, asofGot[i], asofWant[i])
		}
	}
	// Spot-check the nearest-preceding semantics reached the findings: e-300 (@13.3)
	// should carry r-200 as its state.
	sawNearest := false
	for _, e := range asofGot {
		if e == `{"state":[{"msg":"r-200"}],"ts":1779024973300000000}` {
			sawNearest = true
		}
	}
	if !sawNearest {
		t.Fatalf("expected a nearest-preceding ASOF finding (e@13.3 -> r-200); got=%v", asofGot)
	}

	// And the fusable detectors still produce their findings.
	if fusedCount == 0 {
		t.Fatal("expected the fusable logs detectors to also produce findings")
	}
	// t.Logf("ASOF-in-corpus: %d ASOF findings + %d fused findings", len(asofGot), fusedCount)
}

// TestCorpusCompileStandaloneOnly: a corpus of ONLY non-fusable detectors (a GROUP BY)
// has a nil fused Plan yet STILL produces findings -- run individually via the full
// pipeline -- matching the detector's own SELECT rows.
func TestCorpusCompileStandaloneOnly(t *testing.T) {
	sess := corpusTestSession(t)

	stmt := `SELECT sev, count(*) AS n FROM logs GROUP BY sev`
	cc, err := sess.CorpusCompile([]CorpusDetector{{Tag: "grp", Stmt: stmt}})
	if err != nil {
		t.Fatalf("CorpusCompile: %v", err)
	}
	if cc.Plan != nil {
		t.Fatalf("standalone-only corpus should have a nil fused Plan, got %v", cc.Plan)
	}
	if len(cc.Rejected) != 0 || len(cc.Standalone) != 1 || cc.Standalone[0].Tag != "grp" {
		t.Fatalf("expected 1 standalone (grp), 0 rejected; got standalone=%+v rejected=%+v", cc.Standalone, cc.Rejected)
	}

	got, err := cc.Run()
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	var gotRows []string
	for _, f := range got {
		if f.Tag != "grp" {
			t.Fatalf("unexpected finding tag %q", f.Tag)
		}
		gotRows = append(gotRows, canonJSON(t, f.Evidence))
	}
	sort.Strings(gotRows)

	res, err := sess.Run(stmt)
	if err != nil {
		t.Fatalf("baseline: %v", err)
	}
	var wantRows []string
	for _, row := range res.Rows {
		wantRows = append(wantRows, canonJSON(t, row))
	}
	sort.Strings(wantRows)

	if len(gotRows) == 0 {
		t.Fatal("standalone-only corpus produced no findings")
	}
	if len(gotRows) != len(wantRows) {
		t.Fatalf("count: got %d want %d\n got=%v\n want=%v", len(gotRows), len(wantRows), gotRows, wantRows)
	}
	for i := range gotRows {
		if gotRows[i] != wantRows[i] {
			t.Fatalf("row[%d]: got %s want %s", i, gotRows[i], wantRows[i])
		}
	}
}

// TestCorpusCompileRejected: a genuinely broken detector (a parse error) is classified
// REJECTED with a reason and NOT run -- and it does NOT abort the corpus: the other
// (fusable) detector still compiles and produces its findings.
func TestCorpusCompileRejected(t *testing.T) {
	sess := corpusTestSession(t)

	cc, err := sess.CorpusCompile([]CorpusDetector{
		{Tag: "good", Stmt: `SELECT * FROM logs l WHERE l.sev = "ERROR"`},
		{Tag: "broken", Stmt: `SELECT FROM WHERE GROUP nonsense (((`},
	})
	if err != nil {
		t.Fatalf("CorpusCompile: %v", err)
	}

	// The broken detector is rejected (with a reason), not standalone, not fused.
	if len(cc.Rejected) != 1 || cc.Rejected[0].Tag != "broken" {
		t.Fatalf("expected 'broken' rejected, got %+v", cc.Rejected)
	}
	if cc.Rejected[0].Reason == "" {
		t.Fatal("rejected detector must carry a reason")
	}
	// t.Logf("rejected: %+v", cc.Rejected)
	if len(cc.Standalone) != 0 {
		t.Fatalf("unexpected standalone: %+v", cc.Standalone)
	}

	// The good detector still fused and still produces findings (corpus not aborted).
	if cc.Plan == nil {
		t.Fatal("expected a fused plan for the good detector")
	}
	got, err := cc.Run()
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(got) == 0 {
		t.Fatal("expected the good detector's findings despite the broken sibling")
	}
	for _, f := range got {
		if f.Tag != "good" {
			t.Fatalf("unexpected finding tag %q (broken detector must not run)", f.Tag)
		}
	}
}

func dumpPlan(op *base.Op) string {
	b, _ := json.Marshal(op)
	return string(b)
}
