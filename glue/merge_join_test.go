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
	"sync/atomic"
	"testing"

	"github.com/couchbase/n1k1/records"
)

// The argmax -> ASOF merge-join lowering's correctness net (DESIGN-merging.md §3;
// Track B round 4 piece 2). Each test runs the EXACT same argmax-subquery query
// TWICE -- once with the lowering OFF (the correlated-subquery baseline) and once
// ON -- and asserts BYTE-IDENTICAL result rows. A wrong lowering fails here.

// asofWriteKS writes one file into <root>/default/<ks>/<name>.
func asofWriteKS(t *testing.T, root, ks, name, body string) {
	t.Helper()
	dir := filepath.Join(root, "default", ks)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

// nsLine formats one ns_server_log lead line at the given RFC3339 timestamp, node,
// and message -- the format the built-in "ns_server_log" recipe recognizes (so the
// file is a recipe-matched, normalized-int64-ts sorted source).
func nsLine(ts, node, msg string) string {
	return "[ns_server:info," + ts + "," + node + ":x]" + msg + "\n"
}

// runBoth runs stmt with the ASOF lowering OFF then ON, returning both result-row
// slices (as strings) plus whether the lowering fired on the ON run. It asserts the
// two runs are byte-identical -- the differential correctness gate.
func runBoth(t *testing.T, s *Session, stmt string) (off, on []string, fired bool) {
	t.Helper()

	prev := EnableASOFRewrite
	defer func() { EnableASOFRewrite = prev }()

	EnableASOFRewrite = false
	resOff, err := s.Run(stmt)
	if err != nil {
		t.Fatalf("baseline (OFF) Run(%q): %v", stmt, err)
	}

	before := AsofRewriteApplied
	EnableASOFRewrite = true
	resOn, err := s.Run(stmt)
	if err != nil {
		t.Fatalf("lowered (ON) Run(%q): %v", stmt, err)
	}
	fired = AsofRewriteApplied > before

	off = rowsAsStrings(resOff.Rows)
	on = rowsAsStrings(resOn.Rows)

	if len(off) != len(on) {
		t.Fatalf("row count differs OFF=%d ON=%d\n OFF=%v\n ON=%v", len(off), len(on), off, on)
	}
	for i := range off {
		if off[i] != on[i] {
			t.Fatalf("row[%d] NOT byte-identical:\n OFF=%s\n ON =%s", i, off[i], on[i])
		}
	}
	return off, on, fired
}

func rowsAsStrings[T ~[]byte](rows []T) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = string(r)
	}
	return out
}

// TestASOFLoweringDifferential is the core differential: a nearest-preceding
// argmax subquery over two recipe-matched keyspaces lowers to a merge-join whose
// output is byte-identical to the correlated-subquery baseline.
func TestASOFLoweringDifferential(t *testing.T) {
	root := t.TempDir()
	// E = errors log (outer); R = rebalance/state log (subquery keyspace). Both are
	// recipe-matched ns_server_log files with a normalized int64 `ts` sort key.
	asofWriteKS(t, root, "elog", "ns_server.error.log",
		nsLine("2026-05-17T15:36:11.100+02:00", "n1", "e-100")+
			nsLine("2026-05-17T15:36:13.300+02:00", "n1", "e-300")+
			nsLine("2026-05-17T15:36:15.500+02:00", "n1", "e-500"))
	asofWriteKS(t, root, "rlog", "ns_server.rebalance.log",
		nsLine("2026-05-17T15:36:12.200+02:00", "n1", "r-200")+
			nsLine("2026-05-17T15:36:14.400+02:00", "n1", "r-400"))

	s, err := OpenSession(root, "default")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	stmt := "SELECT e.ts AS ts, " +
		"(SELECT r.msg FROM default:rlog r WHERE r.ts <= e.ts ORDER BY r.ts DESC LIMIT 1) AS state_at " +
		"FROM default:elog e ORDER BY e.ts"

	off, _, fired := runBoth(t, s, stmt)
	if !fired {
		t.Fatalf("expected the ASOF lowering to FIRE (proven keyspaces); it did not")
	}
	// Spot-check the shape: 3 rows, first has no preceding R row (null), later rows
	// carry the array-wrapped nearest-preceding projection.
	if len(off) != 3 {
		t.Fatalf("want 3 rows, got %d: %v", len(off), off)
	}
	if off[0] != `{"ts":1779024971100000000,"state_at":null}` {
		t.Fatalf("row0 (no preceding -> null) unexpected: %s", off[0])
	}
	if off[1] != `{"ts":1779024973300000000,"state_at":[{"msg":"r-200"}]}` {
		t.Fatalf("row1 (nearest preceding) unexpected: %s", off[1])
	}
	if off[2] != `{"ts":1779024975500000000,"state_at":[{"msg":"r-400"}]}` {
		t.Fatalf("row2 (nearest preceding) unexpected: %s", off[2])
	}
}

// TestASOFLoweringSoftDifferential covers SOFT ASOF (a `r.ts >= e.ts - Δt`
// look-back guard): ON == OFF byte-identical, AND the guard actually drops a match
// whose nearest-preceding row is farther back than Δt (so the soft path is really
// exercised, not silently equal to plain ASOF).
func TestASOFLoweringSoftDifferential(t *testing.T) {
	root := t.TempDir()
	asofWriteKS(t, root, "elog", "ns_server.error.log",
		nsLine("2026-05-17T15:36:11.100+02:00", "n1", "e-100")+ // no preceding R
			nsLine("2026-05-17T15:36:13.300+02:00", "n1", "e-300")+ // R@12.200 ~1.1s back: in Δt
			nsLine("2026-05-17T15:36:20.000+02:00", "n1", "e-20s")) // R@14.400 ~5.6s back: OUT of Δt
	asofWriteKS(t, root, "rlog", "ns_server.rebalance.log",
		nsLine("2026-05-17T15:36:12.200+02:00", "n1", "r-200")+
			nsLine("2026-05-17T15:36:14.400+02:00", "n1", "r-400"))

	s, err := OpenSession(root, "default")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Δt = 2s = 2_000_000_000 ns.
	softStmt := "SELECT e.ts AS ts, " +
		"(SELECT r.msg FROM default:rlog r WHERE r.ts <= e.ts AND r.ts >= e.ts - 2000000000 " +
		"ORDER BY r.ts DESC LIMIT 1) AS state_at " +
		"FROM default:elog e ORDER BY e.ts"

	off, _, fired := runBoth(t, s, softStmt)
	if !fired {
		t.Fatalf("expected the SOFT ASOF lowering to FIRE; it did not")
	}
	// The last row's nearest-preceding R (@14.400) is ~5.6s back, beyond Δt=2s, so
	// the soft guard drops it -> null (both baseline and lowered must agree here).
	if off[2] != `{"ts":1779024980000000000,"state_at":null}` {
		t.Fatalf("soft row2 should be null (out of tolerance), got: %s", off[2])
	}

	// Sanity: PLAIN ASOF (no look-back) over the same data would NOT drop that far
	// match -- proving the soft guard is what changed the result.
	plainStmt := "SELECT e.ts AS ts, " +
		"(SELECT r.msg FROM default:rlog r WHERE r.ts <= e.ts ORDER BY r.ts DESC LIMIT 1) AS state_at " +
		"FROM default:elog e ORDER BY e.ts"
	plainOff, _, _ := runBoth(t, s, plainStmt)
	if plainOff[2] != `{"ts":1779024980000000000,"state_at":[{"msg":"r-400"}]}` {
		t.Fatalf("plain row2 should match the far R (no tolerance), got: %s", plainOff[2])
	}
}

// TestASOFLoweringPartitionedDifferential covers a partition-equality argmax
// (`r.node = e.node`): the nearest-preceding row must be within the outer row's
// partition. ON == OFF byte-identical, with two interleaved nodes.
func TestASOFLoweringPartitionedDifferential(t *testing.T) {
	root := t.TempDir()
	// Two nodes interleaved by ts. Each E row must match the nearest-preceding R row
	// OF THE SAME NODE, not merely the nearest by ts.
	asofWriteKS(t, root, "elog", "ns_server.error.log",
		nsLine("2026-05-17T15:36:13.000+02:00", "n1", "e1-13")+
			nsLine("2026-05-17T15:36:13.500+02:00", "n2", "e2-135")+
			nsLine("2026-05-17T15:36:16.000+02:00", "n1", "e1-16")+
			nsLine("2026-05-17T15:36:16.500+02:00", "n2", "e2-165"))
	asofWriteKS(t, root, "rlog", "ns_server.rebalance.log",
		nsLine("2026-05-17T15:36:12.000+02:00", "n1", "r1-12")+
			nsLine("2026-05-17T15:36:14.000+02:00", "n2", "r2-14")+
			nsLine("2026-05-17T15:36:15.000+02:00", "n1", "r1-15")+
			nsLine("2026-05-17T15:36:17.000+02:00", "n2", "r2-17"))

	s, err := OpenSession(root, "default")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	stmt := "SELECT e.ts AS ts, e.node AS node, " +
		"(SELECT r.msg FROM default:rlog r WHERE r.ts <= e.ts AND r.node = e.node " +
		"ORDER BY r.ts DESC LIMIT 1) AS state_at " +
		"FROM default:elog e ORDER BY e.ts"

	_, on, fired := runBoth(t, s, stmt)
	if !fired {
		t.Fatalf("expected the partitioned ASOF lowering to FIRE; it did not")
	}
	// e2@13.5 (node n2): nearest preceding R of node n2 with ts<=13.5 is NONE
	// (r2-14 is later) -> null, even though r1-12 is nearer by ts. Verifies the
	// partition is respected.
	if on[1] != `{"ts":1779024973500000000,"node":"n2","state_at":null}` {
		t.Fatalf("partitioned row1 should be null (no same-node preceding), got: %s", on[1])
	}
}

// TestASOFLoweringCrossNodeDifferential is the cross-node ASOF net (DESIGN-merging.md
// "Multi-bundle / cross-node clusters"): the argmax state keyspace R resolves to TWO
// recipe files (two nodes) whose ts ranges INTERLEAVE. The default single concatenated
// R scan is not globally ts-ordered, so the merge-join's build side would trip its
// monotonicity tripwire; per-file expansion turns R into two ordered cursors the ASOF
// merge consumes. ON (lowered, per-file R) must be BYTE-IDENTICAL to the correlated
// baseline (OFF), and the per-file expansion must actually fire.
func TestASOFLoweringCrossNodeDifferential(t *testing.T) {
	root := t.TempDir()
	// E (outer) = a single-file errors log.
	asofWriteKS(t, root, "elog", "ns_server.error.log",
		nsLine("2026-05-17T15:36:11.100+02:00", "n1", "e-100")+ // no preceding R
			nsLine("2026-05-17T15:36:13.300+02:00", "n1", "e-300")+
			nsLine("2026-05-17T15:36:15.500+02:00", "n1", "e-500"))
	// R (state) = TWO files whose ts ranges INTERLEAVE across files: file r1 spans
	// 12.2..15.0, file r2 spans 13.0..14.4, so the concatenated (r1 then r2) stream
	// is out of order (15.0 then 13.0) -> requires per-file cursors to merge.
	asofWriteKS(t, root, "rlog", "ns_server.r1.log",
		nsLine("2026-05-17T15:36:12.200+02:00", "nA", "r-a")+
			nsLine("2026-05-17T15:36:15.000+02:00", "nA", "r-d"))
	asofWriteKS(t, root, "rlog", "ns_server.r2.log",
		nsLine("2026-05-17T15:36:13.000+02:00", "nB", "r-b")+
			nsLine("2026-05-17T15:36:14.400+02:00", "nB", "r-c"))

	s, err := OpenSession(root, "default")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	stmt := "SELECT e.ts AS ts, " +
		"(SELECT r.msg FROM default:rlog r WHERE r.ts <= e.ts ORDER BY r.ts DESC LIMIT 1) AS state_at " +
		"FROM default:elog e ORDER BY e.ts"

	beforePF := atomic.LoadInt64(&PerFileMergeApplied)
	off, _, fired := runBoth(t, s, stmt)
	if !fired {
		t.Fatalf("expected the cross-node ASOF lowering to FIRE; it did not")
	}
	if atomic.LoadInt64(&PerFileMergeApplied) == beforePF {
		t.Fatalf("expected per-file expansion of the 2-file R keyspace to fire")
	}
	// e@11.1 -> no preceding R (null); e@13.3 -> nearest preceding is R@13.0 (r-b);
	// e@15.5 -> nearest preceding is R@15.0 (r-d). runBoth already asserted ON==OFF.
	want := []string{
		`{"ts":1779024971100000000,"state_at":null}`,
		`{"ts":1779024973300000000,"state_at":[{"msg":"r-b"}]}`,
		`{"ts":1779024975500000000,"state_at":[{"msg":"r-d"}]}`,
	}
	if len(off) != len(want) {
		t.Fatalf("want %d rows, got %d: %v", len(want), len(off), off)
	}
	for i := range want {
		if off[i] != want[i] {
			t.Fatalf("row[%d]: want %s got %s", i, want[i], off[i])
		}
	}
}

// TestASOFLoweringNearSortedR is the near-sorted build-side net. The state keyspace
// R is a genuinely NEAR-sorted file (a later line carries an earlier ts, the shape
// real logs exhibit). The lowering wraps R in a watermarked-near merge-scan that must
// REORDER it to strictly-ascending BEFORE the merge-join's build side consumes it --
// otherwise the raw out-of-order R would trip the merge-join's monotonicity tripwire.
// The test is arranged so the nearest-preceding match for one E row is precisely the
// OUT-OF-ORDER R record (r-400 below), so a broken reorder can't accidentally pass.
// ON (lowered) must be byte-identical to the correlated baseline (OFF).
func TestASOFLoweringNearSortedR(t *testing.T) {
	root := t.TempDir()
	asofWriteKS(t, root, "elog", "ns_server.error.log",
		nsLine("2026-05-17T15:36:11.000+02:00", "n1", "e-1100")+ // no preceding R -> null
			nsLine("2026-05-17T15:36:12.500+02:00", "n1", "e-2500")+ // nearest <=12.5 is R@12.4 (r-400)
			nsLine("2026-05-17T15:36:12.700+02:00", "n1", "e-2700")+ // nearest is R@12.6 (r-600)
			nsLine("2026-05-17T15:36:13.500+02:00", "n1", "e-3500")) // nearest is R@13.0 (r-1000)
	// R is NEAR-sorted: r-400 (@12.4) arrives AFTER r-600 (@12.6) -- a 200ms
	// out-of-order displacement. The raw stream 200,600,400,1000 is not ascending.
	asofWriteKS(t, root, "rlog", "ns_server.rebalance.log",
		nsLine("2026-05-17T15:36:12.200+02:00", "n1", "r-200")+
			nsLine("2026-05-17T15:36:12.600+02:00", "n1", "r-600")+
			nsLine("2026-05-17T15:36:12.400+02:00", "n1", "r-400")+ // 200ms late
			nsLine("2026-05-17T15:36:13.000+02:00", "n1", "r-1000"))

	s, err := OpenSession(root, "default")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Sanity: R really is near-sorted (else the watermark path isn't exercised).
	rMetas, merr := SortedSourceMetasForKeyspace(mustKeyspace(t, s, "rlog"), nil)
	if merr != nil || len(rMetas) != 1 || rMetas[0].Meta.Sortedness != records.SortedNear {
		t.Fatalf("rlog not near-sorted (err=%v metas=%+v) -- test data invalid", merr, rMetas)
	}

	stmt := "SELECT e.ts AS ts, " +
		"(SELECT r.msg FROM default:rlog r WHERE r.ts <= e.ts ORDER BY r.ts DESC LIMIT 1) AS state_at " +
		"FROM default:elog e ORDER BY e.ts"

	off, _, fired := runBoth(t, s, stmt)
	if !fired {
		t.Fatalf("expected the near-sorted-R ASOF lowering to FIRE; it did not")
	}
	// The e@12.5 row's nearest preceding is the OUT-OF-ORDER r-400 -- proving the
	// watermark reordered R correctly (a raw near stream would either trip the
	// build-side tripwire or mismatch here). runBoth already asserted ON == OFF.
	want := []string{
		`{"ts":1779024971000000000,"state_at":null}`,
		`{"ts":1779024972500000000,"state_at":[{"msg":"r-400"}]}`,
		`{"ts":1779024972700000000,"state_at":[{"msg":"r-600"}]}`,
		`{"ts":1779024973500000000,"state_at":[{"msg":"r-1000"}]}`,
	}
	if len(off) != len(want) {
		t.Fatalf("want %d rows, got %d: %v", len(want), len(off), off)
	}
	for i := range want {
		if off[i] != want[i] {
			t.Fatalf("row[%d]: want %s got %s", i, want[i], off[i])
		}
	}
}

// TestASOFLoweringNonProvenDoesNotFire proves the safety gate: over a plain
// (non-recipe) keyspace with no sorted-source contract, the lowering must NOT fire,
// and the correlated subquery still returns correct rows.
func TestASOFLoweringNonProvenDoesNotFire(t *testing.T) {
	root := t.TempDir()
	// Plain JSON doc keyspaces (a directory of <key>.json files): no extractor
	// recipe claims them, so SortedSourceMeta is absent and the gate must block.
	writeDoc := func(ks, key, body string) {
		dir := filepath.Join(root, "default", ks)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, key+".json"), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	writeDoc("epv", "e1", `{"ts":100,"msg":"e-100"}`)
	writeDoc("epv", "e2", `{"ts":300,"msg":"e-300"}`)
	writeDoc("rpv", "r1", `{"ts":200,"msg":"r-200"}`)

	s, err := OpenSession(root, "default")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	stmt := "SELECT e.ts AS ts, " +
		"(SELECT r.msg FROM default:rpv r WHERE r.ts <= e.ts ORDER BY r.ts DESC LIMIT 1) AS state_at " +
		"FROM default:epv e ORDER BY e.ts"

	before := AsofRewriteApplied
	prev := EnableASOFRewrite
	EnableASOFRewrite = true
	res, err := s.Run(stmt)
	EnableASOFRewrite = prev
	if err != nil {
		t.Fatalf("Run over plain keyspace: %v", err)
	}
	if AsofRewriteApplied != before {
		t.Fatalf("lowering must NOT fire over a non-proven keyspace, but it did")
	}
	got := rowsAsStrings(res.Rows)
	want := []string{
		`{"ts":100,"state_at":null}`,
		`{"ts":300,"state_at":[{"msg":"r-200"}]}`,
	}
	if len(got) != len(want) {
		t.Fatalf("want %d rows, got %d: %v", len(want), len(got), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("row[%d]: want %s got %s", i, want[i], got[i])
		}
	}
}
