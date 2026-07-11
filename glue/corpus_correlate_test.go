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
		{Tag: "p1", Stmt: preceding()},
		{Tag: "p2", Stmt: preceding() + " WHERE e.msg = \"x\""}, // same sig (outer WHERE doesn't change it)
		{Tag: "f1", Stmt: following},                            // different direction -> different sig
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
	for tag, stmt := range map[string]string{"c1": d1, "c2": d2} {
		res, rerr := sess.Run(stmt)
		if rerr != nil {
			t.Fatalf("oracle Run(%s): %v", tag, rerr)
		}
		for _, r := range res.Rows {
			oracle[tag] = append(oracle[tag], string(r))
		}
		sort.Strings(oracle[tag])
	}

	cc, err := sess.CorpusCompile([]CorpusDetector{{Tag: "c1", Stmt: d1}, {Tag: "c2", Stmt: d2}})
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
		got[f.Tag] = append(got[f.Tag], string(f.Evidence))
	}
	for tag := range oracle {
		sort.Strings(got[tag])
		if len(got[tag]) != len(oracle[tag]) {
			t.Fatalf("%s: %d findings, oracle %d\n got=%v\n oracle=%v", tag, len(got[tag]), len(oracle[tag]), got[tag], oracle[tag])
		}
		for i := range got[tag] {
			if got[tag][i] != oracle[tag][i] {
				t.Errorf("%s row %d: cached %s != oracle %s", tag, i, got[tag][i], oracle[tag][i])
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
