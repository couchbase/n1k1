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
//   - T6 is a GROUP BY -> must land in Unfused, not silently dropped.
//
// Findings are compared as SORTED SETS (order across the union-all / interleaved
// fan-out is not guaranteed).
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
	unfusableTag := "T6_group"
	unfusableStmt := `SELECT sev, count(*) AS n FROM logs GROUP BY sev`

	// Assemble the corpus (fusable + the one non-canonical detector).
	corpus := make([]CorpusDetector, 0, len(fused)+1)
	for _, d := range fused {
		corpus = append(corpus, CorpusDetector{Tag: d.tag, Stmt: d.stmt})
	}
	corpus = append(corpus, CorpusDetector{Tag: unfusableTag, Stmt: unfusableStmt})

	cc, err := sess.CorpusCompile(corpus)
	if err != nil {
		t.Fatalf("CorpusCompile: %v", err)
	}

	// (1) The non-canonical detector must be reported unfused, not dropped.
	if len(cc.Unfused) != 1 || cc.Unfused[0].Tag != unfusableTag {
		t.Fatalf("expected exactly 1 unfused (%s), got %+v", unfusableTag, cc.Unfused)
	}
	t.Logf("unfused: %+v", cc.Unfused)

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

	// (3) Equivalence: the fused findings set == the union of each fused detector's
	// baseline rows tagged with its id.
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
	t.Logf("matched %d fused findings across %d detectors", len(got), len(fused))
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
	if len(cc.Unfused) != 0 {
		t.Fatalf("unexpected unfused: %+v", cc.Unfused)
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

	// UPPER(l.msg) LIKE '%LOAD%' -- a function + LIKE the native lowering declines,
	// so the predicate stays boxed; alias `l` is remapped to SELF for the shared row.
	stmt := `SELECT * FROM logs l WHERE UPPER(l.msg) LIKE "%LOAD%"`
	baseline := `SELECT RAW l FROM logs l WHERE UPPER(l.msg) LIKE "%LOAD%"`

	cc, err := sess.CorpusCompile([]CorpusDetector{{Tag: "boxed", Stmt: stmt}})
	if err != nil {
		t.Fatalf("CorpusCompile: %v", err)
	}
	if len(cc.Unfused) != 0 {
		t.Fatalf("boxed detector unexpectedly unfused: %+v", cc.Unfused)
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

func dumpPlan(op *base.Op) string {
	b, _ := json.Marshal(op)
	return string(b)
}
