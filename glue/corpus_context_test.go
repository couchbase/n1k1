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
	"testing"

	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/expression/parser"

	"github.com/couchbase/n1k1/base"
)

// ctxCorpusSession writes a <root>/default/logs keyspace of {file,pos,sev,line} docs
// across two files (f1: pos 0..5 ERROR@2; f2: pos 0..3 ERROR@0) and opens it.
func ctxCorpusSession(t *testing.T) *Session {
	t.Helper()
	dir := t.TempDir()
	ks := filepath.Join(dir, "default", "logs")
	if err := os.MkdirAll(ks, 0o755); err != nil {
		t.Fatal(err)
	}
	var sb []byte
	line := func(file string, pos int, sev string) {
		sb = append(sb, []byte(fmt.Sprintf(`{"file":%q,"pos":%d,"sev":%q,"line":"L%d"}`+"\n", file, pos, sev, pos))...)
	}
	for i := 0; i < 6; i++ {
		s := "info"
		if i == 2 {
			s = "ERROR"
		}
		line("f1", i, s)
	}
	for i := 0; i < 4; i++ {
		s := "info"
		if i == 0 {
			s = "ERROR"
		}
		line("f2", i, s)
	}
	if err := os.WriteFile(filepath.Join(ks, "l.jsonl"), sb, 0o644); err != nil {
		t.Fatal(err)
	}
	sess, err := OpenSession(dir, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	return sess
}

// ctxStmt is the canonical windowed match-flag context detector: grep -C<before/after>
// for a severity, partitioned by file, ordered by pos.
func ctxStmt(sev string, before, after int) string {
	return fmt.Sprintf(`SELECT file, pos, line FROM (`+
		`SELECT file, pos, line, MAX(CASE WHEN sev = %q THEN 1 ELSE 0 END) `+
		`OVER (PARTITION BY file ORDER BY pos ROWS BETWEEN %d PRECEDING AND %d FOLLOWING) AS near `+
		`FROM logs) sub WHERE sub.near = 1`, sev, before, after)
}

// filePosKeys extracts a sorted list of "file:pos" keys from evidence rows (each a JSON
// object with file + pos), so context findings (whole-row evidence) and the standalone
// SQL (projected {file,pos,line}) compare on the ROWS SELECTED regardless of shape.
func filePosKeys(t *testing.T, raws []json.RawMessage) []string {
	t.Helper()
	var keys []string
	for _, r := range raws {
		var m map[string]interface{}
		if err := json.Unmarshal(r, &m); err != nil {
			t.Fatalf("decode %q: %v", r, err)
		}
		keys = append(keys, fmt.Sprintf("%v:%v", m["file"], m["pos"]))
	}
	sort.Strings(keys)
	return keys
}

// countOpKind walks a plan counting ops of a kind.
func countOpKind(op *base.Op, kind string) int {
	if op == nil {
		return 0
	}
	n := 0
	if op.Kind == kind {
		n++
	}
	for _, c := range op.Children {
		n += countOpKind(c, kind)
	}
	return n
}

// TestCorpusContextRecognitionDifferential: two context detectors sharing the same
// (keyspace, partition, order) signature FUSE into ONE shared scan + sort + broadcast-
// context (one scan, one order op, one broadcast-context with two extractors), and their
// findings equal -- per detector, by selected rows -- running each detector's own SQL
// standalone (its window result is the oracle).
func TestCorpusContextRecognitionDifferential(t *testing.T) {
	sess := ctxCorpusSession(t)

	// grep -C1 for ERROR, and grep -B2/-A0 for ERROR: same (file, pos) signature.
	dets := []CorpusDetector{
		{Tag: "ctxC1", Stmt: ctxStmt("ERROR", 1, 1)},
		{Tag: "ctxB2", Stmt: ctxStmt("ERROR", 2, 0)},
	}
	cc, err := sess.CorpusCompile(dets)
	if err != nil {
		t.Fatalf("CorpusCompile: %v", err)
	}

	// Both recognized as context detectors -> not standalone, not rejected.
	if len(cc.Standalone) != 0 || len(cc.Rejected) != 0 {
		t.Fatalf("expected both context detectors fused; standalone=%v rejected=%v", cc.Standalone, cc.Rejected)
	}
	// Shared: exactly ONE scan, ONE order-offset-limit, ONE broadcast-context (2 detectors,
	// one signature -> one group -> one shared scan+sort).
	if n := countOpKind(cc.Plan, "datastore-scan-records"); n != 1 {
		t.Errorf("shared scan: got %d scans, want 1", n)
	}
	if n := countOpKind(cc.Plan, "order-offset-limit"); n != 1 {
		t.Errorf("shared sort: got %d order ops, want 1", n)
	}
	if n := countOpKind(cc.Plan, "broadcast-context"); n != 1 {
		t.Errorf("one context group: got %d broadcast-context ops, want 1", n)
	}

	// Findings, grouped by tag.
	findings, err := cc.Run()
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	byTag := map[string][]json.RawMessage{}
	for _, f := range findings {
		byTag[f.Tag] = append(byTag[f.Tag], f.Evidence)
	}

	// Oracle: each detector's own SQL standalone.
	for _, d := range dets {
		res, err := sess.Run(d.Stmt)
		if err != nil {
			t.Fatalf("oracle Run(%s): %v", d.Tag, err)
		}
		want := filePosKeys(t, res.Rows)
		got := filePosKeys(t, byTag[d.Tag])
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("%s: context findings rows %v != standalone SQL rows %v", d.Tag, got, want)
		}
		if len(want) == 0 {
			t.Errorf("%s: oracle produced no rows -- test fixture too weak", d.Tag)
		}
	}
}

// TestCorpusContextSeparateSignatures: context detectors with DIFFERENT (partition, order)
// signatures do NOT share -- they land in separate groups (two broadcast-context ops),
// while a same-signature pair shares one.
func TestCorpusContextSeparateSignatures(t *testing.T) {
	sess := ctxCorpusSession(t)

	sameA := CorpusDetector{Tag: "a", Stmt: ctxStmt("ERROR", 1, 1)}
	sameB := CorpusDetector{Tag: "b", Stmt: ctxStmt("info", 1, 1)} // same (file,pos) sig
	// Different ORDER key (pos vs line) -> a different signature.
	diff := CorpusDetector{Tag: "c", Stmt: `SELECT file, pos, line FROM (` +
		`SELECT file, pos, line, MAX(CASE WHEN sev = "ERROR" THEN 1 ELSE 0 END) ` +
		`OVER (PARTITION BY file ORDER BY line ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS near ` +
		`FROM logs) sub WHERE sub.near = 1`}

	cc, err := sess.CorpusCompile([]CorpusDetector{sameA, sameB, diff})
	if err != nil {
		t.Fatalf("CorpusCompile: %v", err)
	}
	if len(cc.Standalone) != 0 {
		t.Fatalf("expected all three recognized as context; standalone=%v", cc.Standalone)
	}
	// Two groups (sig {file,pos} shared by a+b; sig {file,line} for c) -> 2 broadcast-context,
	// 2 scans, 2 sorts.
	if n := countOpKind(cc.Plan, "broadcast-context"); n != 2 {
		t.Errorf("got %d broadcast-context ops, want 2 (two signatures)", n)
	}
	if n := countOpKind(cc.Plan, "datastore-scan-records"); n != 2 {
		t.Errorf("got %d scans, want 2", n)
	}
}

// TestCorpusContextAbsenceStaysStandalone: an ABSENCE detector (WHERE near = 0) must NOT
// be recognized as a context detector (its polarity is inverted) -- it stays standalone,
// so it is never mis-lowered to the "present" fan-out.
func TestCorpusContextAbsenceStaysStandalone(t *testing.T) {
	sess := ctxCorpusSession(t)
	absence := CorpusDetector{Tag: "absent", Stmt: `SELECT file, pos FROM (` +
		`SELECT file, pos, MAX(CASE WHEN sev = "ERROR" THEN 1 ELSE 0 END) ` +
		`OVER (PARTITION BY file ORDER BY pos ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS near ` +
		`FROM logs) sub WHERE sub.near = 0`}
	cc, err := sess.CorpusCompile([]CorpusDetector{absence})
	if err != nil {
		t.Fatalf("CorpusCompile: %v", err)
	}
	if countOpKind(cc.Plan, "broadcast-context") != 0 {
		t.Errorf("absence detector must NOT lower to broadcast-context")
	}
	if len(cc.Standalone) != 1 || cc.Standalone[0].Tag != "absent" {
		t.Errorf("absence detector should be standalone; got standalone=%v", cc.Standalone)
	}
}

// TestCorpusContextSortElision: a group ordered by (_meta.path, _meta.pos) needs NO sort
// -- the file scan already yields those (the flagship rotated-log grep shape), so the
// built plan is scan -> broadcast-context (no order-offset-limit). Any other (partition,
// order) keeps the explicit sort.
func TestCorpusContextSortElision(t *testing.T) {
	mustParse := func(s string) expression.Expression {
		e, err := parser.Parse(s)
		if err != nil {
			t.Fatalf("parse %q: %v", s, err)
		}
		return e
	}
	build := func(part, order string) *base.Op {
		info := contextDetInfo{
			keyspaceName: "default:logs",
			keyspacer:    nil,
			alias:        "x",
			partExpr:     mustParse(part),
			orderExprs:   []expression.Expression{mustParse(order)},
			beforeMatch:  1, afterMatch: 1,
			matchPred: mustParse(`x.sev = "ERROR"`),
		}
		return buildContextBroadcast([]contextDetInfo{info}, []string{"t"}, &Conv{Temps: []interface{}{nil}})
	}

	// _meta.path / _meta.pos -> sort ELIDED (scan feeds broadcast-context directly).
	bt := "`" // "path" is a reserved word -> backtick it
	elided := build("x._meta."+bt+"path"+bt, "x._meta.pos")
	if n := countOpKind(elided, "order-offset-limit"); n != 0 {
		t.Errorf("(_meta.path,_meta.pos): expected sort ELIDED, got %d order ops", n)
	}
	if n := countOpKind(elided, "datastore-scan-records"); n != 1 {
		t.Errorf("elided plan: want 1 scan, got %d", n)
	}
	if n := countOpKind(elided, "broadcast-context"); n != 1 {
		t.Errorf("elided plan: want 1 broadcast-context, got %d", n)
	}

	// A non-_meta partition/order -> sort RETAINED (the scan isn't grouped/ordered by it).
	kept := build("x.file", "x.pos")
	if n := countOpKind(kept, "order-offset-limit"); n != 1 {
		t.Errorf("(file,pos): expected sort RETAINED, got %d order ops", n)
	}
}

// TestCorpusContextPredNativized: the recognized detector's match predicate is lowered
// to its NATIVE tree (e.g. ["eq", ...] for sev="ERROR"), not left boxed ["exprTree",...],
// so the engine op's Aho-Corasick index can extract a necessary literal and prune. A
// boxed pred would head with "exprTree" (always-wake, no pruning).
func TestCorpusContextPredNativized(t *testing.T) {
	sess := ctxCorpusSession(t)
	cc, err := sess.CorpusCompile([]CorpusDetector{{Tag: "c", Stmt: ctxStmt("ERROR", 1, 1)}})
	if err != nil {
		t.Fatal(err)
	}
	// walk to the broadcast-context op.
	var bc *base.Op
	var walk func(*base.Op)
	walk = func(op *base.Op) {
		if op == nil {
			return
		}
		if op.Kind == "broadcast-context" {
			bc = op
		}
		for _, c := range op.Children {
			walk(c)
		}
	}
	walk(cc.Plan)
	if bc == nil {
		t.Fatal("no broadcast-context op in plan")
	}
	exts := bc.Params[0].([]interface{})
	ext0 := exts[0].([]interface{})
	pred := ext0[3].([]interface{}) // {tag, before, after, pred, proj}
	if head, _ := pred[0].(string); head == "exprTree" {
		t.Errorf("match pred left BOXED (%v) -- expected a native tree so the AC index can prune", pred[0])
	}
	if head, _ := pred[0].(string); head != "eq" {
		t.Logf("note: pred head = %q (native, ok as long as PrefilterLiteral can read it)", head)
	}
}
