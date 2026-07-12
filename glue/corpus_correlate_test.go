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
	"github.com/couchbase/n1k1/base"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// correlateSession writes two keyspaces (errors, state) with a ts field and opens it.
func correlateSession(t *testing.T) *Session {
	t.Helper()
	dir := t.TempDir()
	for _, ks := range []string{"errors", "state"} {
		d := filepath.Join(dir, "default", ks)
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(d, "a.jsonl"),
			[]byte(`{"ts":1,"msg":"a"}`+"\n"+`{"ts":2,"msg":"b"}`+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	sess, err := OpenSession(dir, "default")
	if err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	return sess
}

// TestCorpusCorrelationGrouping (Part B foundation): temporal-correlation detectors are
// recognized and grouped by their (left, right, key, direction) signature -- two
// nearest-PRECEDING errors->state-by-ts detectors share a group; a nearest-FOLLOWING one
// is a separate group. They still run standalone (this slice only surfaces the grouping).
func TestCorpusCorrelationGrouping(t *testing.T) {
	sess := correlateSession(t)

	preceding := func() string {
		return "SELECT e.ts AS ts, (SELECT r.msg FROM default:state r WHERE r.ts <= e.ts " +
			"ORDER BY r.ts DESC LIMIT 1) AS state_at FROM default:errors e"
	}
	following := "SELECT e.ts AS ts, (SELECT r.msg FROM default:state r WHERE r.ts >= e.ts " +
		"ORDER BY r.ts ASC LIMIT 1) AS next_state FROM default:errors e"

	dets := []CorpusDetector{
		{Label: "p1", Stmt: preceding()},
		{Label: "p2", Stmt: preceding() + " WHERE e.msg = \"x\""}, // same sig (outer WHERE doesn't change it)
		{Label: "f1", Stmt: following},                            // different direction -> different sig
	}
	cc, err := sess.CorpusCompile(dets)
	if err != nil {
		t.Fatalf("CorpusCompile: %v", err)
	}

	// All three are correlation detectors -> still standalone (no execution change here).
	if len(cc.Standalone) != 3 {
		t.Fatalf("expected 3 standalone correlation detectors, got %d", len(cc.Standalone))
	}

	// Two groups: {p1,p2} (preceding) and {f1} (following).
	if len(cc.CorrelationGroups) != 2 {
		t.Fatalf("expected 2 correlation groups, got %d: %v", len(cc.CorrelationGroups), cc.CorrelationGroups)
	}
	var shared []string
	for _, tags := range cc.CorrelationGroups {
		if len(tags) == 2 {
			shared = append(shared, tags...)
			sort.Strings(shared)
		}
	}
	if want := []string{"p1", "p2"}; len(shared) != 2 || shared[0] != want[0] || shared[1] != want[1] {
		t.Errorf("shared preceding group = %v, want [p1 p2]", shared)
	}
}

// TestCorpusCorrelationScanSharing (Part B execution): two ASOF-lowered correlation
// detectors over the same two keyspaces share the scan+decode of each via the corpus scan
// cache (both merge-scan sides are n1k1 full scans). The findings are byte-identical to
// running each detector standalone (the oracle), and the shared keyspace is CAPTURED once
// then REPLAYED for the second detector -- proving the sharing without changing results.
// (Uses recipe-matched ns_server_log keyspaces + the ASOF lowering, because an UN-lowered
// correlated subquery evaluates its inner scan via boxed cbq, which no n1k1 scan cache --
// nor temp-capture/temp-yield -- can intercept.)
func TestCorpusCorrelationScanSharing(t *testing.T) {
	prev := EnableASOFRewrite
	EnableASOFRewrite = true
	defer func() { EnableASOFRewrite = prev }()

	root := t.TempDir()
	asofWriteKS(t, root, "elog", "ns_server.error.log",
		nsLine("2026-05-17T15:36:11.100+02:00", "n1", "e-100")+
			nsLine("2026-05-17T15:36:13.300+02:00", "n1", "e-300"))
	asofWriteKS(t, root, "rlog", "ns_server.rebalance.log",
		nsLine("2026-05-17T15:36:12.200+02:00", "n1", "r-200")+
			nsLine("2026-05-17T15:36:14.400+02:00", "n1", "r-400"))
	sess, err := OpenSession(root, "default")
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()

	d1 := "SELECT e.ts AS ts, (SELECT r.msg FROM default:rlog r WHERE r.ts <= e.ts " +
		"ORDER BY r.ts DESC LIMIT 1) AS state_at FROM default:elog e ORDER BY e.ts"
	d2 := "SELECT e.node AS node, (SELECT r.msg FROM default:rlog r WHERE r.ts <= e.ts " +
		"ORDER BY r.ts DESC LIMIT 1) AS state_at FROM default:elog e ORDER BY e.ts"

	// Oracle: each detector standalone (no corpus / no cache) FIRST.
	oracle := map[string][]string{}
	for label, stmt := range map[string]string{"c1": d1, "c2": d2} {
		res, rerr := sess.Run(stmt)
		if rerr != nil {
			t.Fatalf("oracle Run(%s): %v", label, rerr)
		}
		for _, r := range res.Rows {
			oracle[label] = append(oracle[label], string(r))
		}
		sort.Strings(oracle[label])
	}

	cc, err := sess.CorpusCompile([]CorpusDetector{{Label: "c1", Stmt: d1}, {Label: "c2", Stmt: d2}})
	if err != nil {
		t.Fatalf("CorpusCompile: %v", err)
	}
	if len(cc.CorrelationGroups) != 1 {
		t.Fatalf("expected 1 correlation group (both share the sig), got %v", cc.CorrelationGroups)
	}

	findings, err := cc.Run()
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	got := map[string][]string{}
	for _, f := range findings {
		got[f.Label] = append(got[f.Label], string(f.Result))
	}
	for label := range oracle {
		sort.Strings(got[label])
		if len(got[label]) != len(oracle[label]) {
			t.Fatalf("%s: %d findings, oracle %d\n got=%v\n oracle=%v", label, len(got[label]), len(oracle[label]), got[label], oracle[label])
		}
		for i := range got[label] {
			if got[label][i] != oracle[label][i] {
				t.Errorf("%s row %d: cached %s != oracle %s", label, i, got[label][i], oracle[label][i])
			}
		}
	}

	// The win: a correlation keyspace is captured once then replayed for the other
	// detector's full scan (the merge build side is a full n1k1 scan). At least one
	// capture + one replay proves the sharing fired.
	if cc.scanCache == nil {
		t.Fatal("no scan cache installed (correlation keyspaces not recognized?)")
	}
	if cc.scanCache.captures == 0 {
		t.Errorf("captures = 0 -- no correlation keyspace scan was cached")
	}
	if cc.scanCache.replays == 0 {
		t.Errorf("replays = 0 -- the cache never served a shared scan (no sharing happened)")
	}
	t.Logf("scan cache: %d captured, %d replayed", cc.scanCache.captures, cc.scanCache.replays)
}

// TestCorpusCorrelationSharesBothSides: when two detectors share the sig AND project the
// DRIVING (left) keyspace identically (here they differ only by a constant projection
// term, so both scans are byte-identical), the cache shares BOTH sides -- the build-side
// (rlog) full scan and the driving-side (elog) projected scan are each captured once and
// replayed for the second detector. captures == 2, replays == 2.
func TestCorpusCorrelationSharesBothSides(t *testing.T) {
	prev := EnableASOFRewrite
	EnableASOFRewrite = true
	defer func() { EnableASOFRewrite = prev }()

	root := t.TempDir()
	asofWriteKS(t, root, "elog", "ns_server.error.log",
		nsLine("2026-05-17T15:36:11.100+02:00", "n1", "e-100")+
			nsLine("2026-05-17T15:36:13.300+02:00", "n1", "e-300"))
	asofWriteKS(t, root, "rlog", "ns_server.rebalance.log",
		nsLine("2026-05-17T15:36:12.200+02:00", "n1", "r-200")+
			nsLine("2026-05-17T15:36:14.400+02:00", "n1", "r-400"))
	sess, err := OpenSession(root, "default")
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()

	sub := "(SELECT r.msg FROM default:rlog r WHERE r.ts <= e.ts ORDER BY r.ts DESC LIMIT 1)"
	d1 := "SELECT \"d1\" AS label, e.ts AS ts, " + sub + " AS state_at FROM default:elog e ORDER BY e.ts"
	d2 := "SELECT \"d2\" AS label, e.ts AS ts, " + sub + " AS state_at FROM default:elog e ORDER BY e.ts"

	cc, err := sess.CorpusCompile([]CorpusDetector{{Label: "c1", Stmt: d1}, {Label: "c2", Stmt: d2}})
	if err != nil {
		t.Fatalf("CorpusCompile: %v", err)
	}
	if _, err := cc.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if cc.scanCache == nil {
		t.Fatal("no scan cache installed")
	}
	if cc.scanCache.captures != 2 {
		t.Errorf("captures = %d, want 2 (elog + rlog each captured once)", cc.scanCache.captures)
	}
	if cc.scanCache.replays != 2 {
		t.Errorf("replays = %d, want 2 (each side replayed for the 2nd detector)", cc.scanCache.replays)
	}
	t.Logf("both-side sharing: %d captured, %d replayed", cc.scanCache.captures, cc.scanCache.replays)
}

// TestCorpusCorrelationScanBudget: a tiny capture budget makes the cache ABANDON a
// keyspace mid-capture (free the partial heap, re-scan thereafter) instead of spilling it
// in full -- and the findings are STILL byte-identical to standalone (abandoning caching
// never changes results).
func TestCorpusCorrelationScanBudget(t *testing.T) {
	prev := EnableASOFRewrite
	EnableASOFRewrite = true
	defer func() { EnableASOFRewrite = prev }()
	prevB := CorpusScanCacheBudgetBytes
	CorpusScanCacheBudgetBytes = 1 // 1 byte: nothing fits.
	defer func() { CorpusScanCacheBudgetBytes = prevB }()

	root := t.TempDir()
	asofWriteKS(t, root, "elog", "ns_server.error.log",
		nsLine("2026-05-17T15:36:11.100+02:00", "n1", "e-100"))
	asofWriteKS(t, root, "rlog", "ns_server.rebalance.log",
		nsLine("2026-05-17T15:36:10.000+02:00", "n1", "r-000"))
	sess, err := OpenSession(root, "default")
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()

	d := "SELECT e.ts AS ts, (SELECT r.msg FROM default:rlog r WHERE r.ts <= e.ts " +
		"ORDER BY r.ts DESC LIMIT 1) AS state_at FROM default:elog e ORDER BY e.ts"

	oracle, err := sess.Run(d)
	if err != nil {
		t.Fatalf("oracle: %v", err)
	}

	cc, err := sess.CorpusCompile([]CorpusDetector{{Label: "c1", Stmt: d}, {Label: "c2", Stmt: d}})
	if err != nil {
		t.Fatalf("CorpusCompile: %v", err)
	}
	findings, err := cc.Run()
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	// Findings unchanged despite no caching.
	byTag := map[string]int{}
	for _, f := range findings {
		byTag[f.Label]++
	}
	for _, label := range []string{"c1", "c2"} {
		if byTag[label] != len(oracle.Rows) {
			t.Errorf("%s: %d findings, oracle %d (skipping the cache changed results!)", label, byTag[label], len(oracle.Rows))
		}
	}
	if cc.scanCache.captures != 0 {
		t.Errorf("captures = %d, want 0 (budget too small to cache anything)", cc.scanCache.captures)
	}
	// These are standard default/<ks>/ keyspaces, so keyspaceRawBytes can't size them (-1)
	// and the size gate doesn't fire -- the tiny budget is caught by the mid-capture ABANDON
	// backstop instead. (The size gate, which needs flat-layout file sizes, is covered by
	// TestKeyspaceRawBytes + verified on real flat bundles.)
	if cc.scanCache.abandoned == 0 {
		t.Errorf("abandoned = 0, want > 0 (tiny budget should abandon; size unknown -> backstop)")
	}
	t.Logf("budget: captured=%d skipped-big=%d abandoned=%d", cc.scanCache.captures, cc.scanCache.skippedBig, cc.scanCache.abandoned)
}

// TestKeyspaceRawBytes covers the size gate's estimate source: for a flat-layout keyspace
// (the cbcollect-bundle case) it returns the backing file's byte size (a cheap os.Stat),
// which the gate scales by CorpusScanCacheSizeFactor to skip an over-budget keyspace up
// front instead of spilling and abandoning it. A non-file keyspace returns -1 (no gate).
func TestKeyspaceRawBytes(t *testing.T) {
	dir := t.TempDir()
	f := filepath.Join(dir, "data.jsonl")
	if err := os.WriteFile(f, make([]byte, 5000), 0o644); err != nil {
		t.Fatal(err)
	}
	vars := &base.Vars{Temps: []interface{}{nil, asofKeyspacer{ks: &flatKeyspace{file: f}}}}
	op := &base.Op{Params: []interface{}{1}} // Temps[1] is the keyspacer.

	if got := keyspaceRawBytes(op, vars); got != 5000 {
		t.Errorf("keyspaceRawBytes(single file) = %d, want 5000", got)
	}
	// The gate decision it feeds: raw*factor vs budget.
	if !(float64(5000)*CorpusScanCacheSizeFactor > float64(4000)) {
		t.Errorf("gate math wrong: 5000*%v should exceed a 4000 budget", CorpusScanCacheSizeFactor)
	}
}

// TestCorrelationKeyspaceQNsSharedOnly: only keyspaces read by 2+ detectors are cached.
// Two detectors with DIFFERENT probes but the SAME build keyspace -> only the build is
// shared (caching a single-use probe would spill a heap that's never replayed).
func TestCorrelationKeyspaceQNsSharedOnly(t *testing.T) {
	groups := map[string][]string{
		"default:master_events\x00default:memcached\x00ts\x00preceding":  {"c1"},
		"default:cbcollect_info\x00default:memcached\x00ts\x00preceding": {"c2"},
	}
	qns := correlationKeyspaceQNs(groups)
	if !qns["default:memcached"] {
		t.Errorf("memcached (used by both detectors) should be shared; got %v", qns)
	}
	if qns["default:master_events"] || qns["default:cbcollect_info"] {
		t.Errorf("single-use probes should NOT be cached; got %v", qns)
	}
	if len(qns) != 1 {
		t.Errorf("expected exactly 1 shared keyspace (memcached), got %v", qns)
	}
}
