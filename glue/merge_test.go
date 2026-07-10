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

// TestASOFLoweringOuterFilterDifferential is the IDEA-0014 gate: a nearest-preceding
// argmax with an OUTER WHERE (correlate only a filtered subset of E -- the common
// real detector, e.g. only error rows) must STILL lower. Before the fix an outer
// WHERE put a `filter` between the project and the scan, so the recognizer bailed to
// the O(n^2) correlated path. The filter is now re-applied to the E probe stream, so
// the lowering fires AND the output stays byte-identical to the correlated baseline.
func TestASOFLoweringOuterFilterDifferential(t *testing.T) {
	root := t.TempDir()
	// E carries a mix of msgs; the outer WHERE keeps only the "boom" rows (mirrors a
	// detector's `regexp_contains(e.msg, ...)` correlate-only-matching-rows filter).
	asofWriteKS(t, root, "elog", "ns_server.error.log",
		nsLine("2026-05-17T15:36:11.100+02:00", "n1", "boom-100")+ // kept; no preceding R -> null
			nsLine("2026-05-17T15:36:12.500+02:00", "n1", "noise-250")+ // dropped by filter
			nsLine("2026-05-17T15:36:13.300+02:00", "n1", "boom-300")) // kept; nearest R@12.200
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
		"FROM default:elog e WHERE regexp_contains(e.msg, \"boom\") ORDER BY e.ts"

	off, _, fired := runBoth(t, s, stmt)
	if !fired {
		t.Fatalf("expected the ASOF lowering to FIRE with an outer WHERE (IDEA-0014); it did not")
	}
	// The filter keeps exactly the two "boom" rows, correlated to the nearest R.
	if len(off) != 2 {
		t.Fatalf("want 2 filtered rows, got %d: %v", len(off), off)
	}
	if off[0] != `{"ts":1779024971100000000,"state_at":null}` {
		t.Fatalf("row0 (kept, no preceding -> null) unexpected: %s", off[0])
	}
	if off[1] != `{"ts":1779024973300000000,"state_at":[{"msg":"r-200"}]}` {
		t.Fatalf("row1 (kept, nearest preceding R@12.200) unexpected: %s", off[1])
	}
}

// TestASOFLoweringRightResidualDifferential: a CONTENT predicate on the inner (build)
// stream -- `r.msg LIKE "%r-4%"`, referencing only r -- used to make the recognizer BAIL
// to the O(n^2) correlated path (its WHERE loop's `default: return nil,false`). It is now
// recognized as a right-only residual and pushed onto the build scan, so the merge finds
// the nearest PRECEDING R row that ALSO matches -- byte-identical to the correlated
// baseline. This is what unlocks "the nearest <content-matching> row of another stream".
func TestASOFLoweringRightResidualDifferential(t *testing.T) {
	root := t.TempDir()
	asofWriteKS(t, root, "elog", "ns_server.error.log",
		nsLine("2026-05-17T15:36:11.100+02:00", "n1", "e-100")+
			nsLine("2026-05-17T15:36:13.300+02:00", "n1", "e-300")+
			nsLine("2026-05-17T15:36:15.500+02:00", "n1", "e-500"))
	asofWriteKS(t, root, "rlog", "ns_server.rebalance.log",
		nsLine("2026-05-17T15:36:12.200+02:00", "n1", "r-200")+ // excluded by the residual
			nsLine("2026-05-17T15:36:14.400+02:00", "n1", "r-400")) // the only candidate
	s, err := OpenSession(root, "default")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	stmt := "SELECT e.ts AS ts, " +
		"(SELECT r.msg FROM default:rlog r WHERE r.ts <= e.ts AND r.msg LIKE \"%r-4%\" ORDER BY r.ts DESC LIMIT 1) AS state_at " +
		"FROM default:elog e ORDER BY e.ts"

	off, _, fired := runBoth(t, s, stmt)
	if !fired {
		t.Fatalf("expected the ASOF lowering to FIRE with a right-only content residual; it did not")
	}
	if len(off) != 3 {
		t.Fatalf("want 3 rows, got %d: %v", len(off), off)
	}
	// Only r-400 (@14.400) survives the residual. e@11.100 and e@13.300 have no PRECEDING
	// r-4* (r-400 is at 14.400) -> null; e@15.500 -> r-400.
	if off[0] != `{"ts":1779024971100000000,"state_at":null}` {
		t.Fatalf("row0 (no preceding r-4*) unexpected: %s", off[0])
	}
	if off[1] != `{"ts":1779024973300000000,"state_at":null}` {
		t.Fatalf("row1 (r-400 is after, not preceding) unexpected: %s", off[1])
	}
	if off[2] != `{"ts":1779024975500000000,"state_at":[{"msg":"r-400"}]}` {
		t.Fatalf("row2 (nearest preceding r-4* = r-400) unexpected: %s", off[2])
	}
}

// TestASOFLoweringFollowingDifferential: nearest-FOLLOWING (ASC + `r.ts >= e.ts`) lowers
// to a forward merge-join, byte-identical to the correlated baseline. This is the mirror
// of the preceding case -- for each E row, the first R row at/after it.
func TestASOFLoweringFollowingDifferential(t *testing.T) {
	root := t.TempDir()
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
		"(SELECT r.msg FROM default:rlog r WHERE r.ts >= e.ts ORDER BY r.ts ASC LIMIT 1) AS nx " +
		"FROM default:elog e ORDER BY e.ts"

	off, _, fired := runBoth(t, s, stmt)
	if !fired {
		t.Fatalf("expected nearest-following to lower to a merge; it did not")
	}
	// e@11.100 -> first r at/after = r-200@12.200; e@13.300 -> r-400@14.400;
	// e@15.500 -> no following r -> null.
	if off[0] != `{"ts":1779024971100000000,"nx":[{"msg":"r-200"}]}` {
		t.Fatalf("row0 unexpected: %s", off[0])
	}
	if off[1] != `{"ts":1779024973300000000,"nx":[{"msg":"r-400"}]}` {
		t.Fatalf("row1 unexpected: %s", off[1])
	}
	if off[2] != `{"ts":1779024975500000000,"nx":null}` {
		t.Fatalf("row2 unexpected: %s", off[2])
	}
}

// TestASOFLoweringFollowingResidualDifferential is the flagship "XYZ in log1, then ABC
// soon after in log2": nearest FOLLOWING inner row that ALSO matches a content residual
// (`r.msg LIKE "%r-4%"`), pushed onto the build scan. Byte-identical to the correlated
// baseline.
func TestASOFLoweringFollowingResidualDifferential(t *testing.T) {
	root := t.TempDir()
	asofWriteKS(t, root, "elog", "ns_server.error.log",
		nsLine("2026-05-17T15:36:11.100+02:00", "n1", "e-100")+
			nsLine("2026-05-17T15:36:13.300+02:00", "n1", "e-300")+
			nsLine("2026-05-17T15:36:15.500+02:00", "n1", "e-500"))
	asofWriteKS(t, root, "rlog", "ns_server.rebalance.log",
		nsLine("2026-05-17T15:36:12.200+02:00", "n1", "r-200")+ // excluded by the residual
			nsLine("2026-05-17T15:36:14.400+02:00", "n1", "r-400")) // the only candidate
	s, err := OpenSession(root, "default")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	stmt := "SELECT e.ts AS ts, " +
		"(SELECT r.msg FROM default:rlog r WHERE r.ts >= e.ts AND r.msg LIKE \"%r-4%\" ORDER BY r.ts ASC LIMIT 1) AS next_state " +
		"FROM default:elog e ORDER BY e.ts"

	off, _, fired := runBoth(t, s, stmt)
	if !fired {
		t.Fatalf("expected following + residual to lower; it did not")
	}
	// Only r-400 survives the residual. e@11.100 & e@13.300 -> r-400 (following);
	// e@15.500 -> none.
	if off[0] != `{"ts":1779024971100000000,"next_state":[{"msg":"r-400"}]}` {
		t.Fatalf("row0 unexpected: %s", off[0])
	}
	if off[1] != `{"ts":1779024973300000000,"next_state":[{"msg":"r-400"}]}` {
		t.Fatalf("row1 unexpected: %s", off[1])
	}
	if off[2] != `{"ts":1779024975500000000,"next_state":null}` {
		t.Fatalf("row2 unexpected: %s", off[2])
	}
}

// TestASOFLoweringFollowingSoftDifferential: soft FOLLOWING -- "the nearest R within Δt
// AFTER each E" (`r.ts >= e.ts AND r.ts <= e.ts + Δt`), the look-AHEAD guard. The merge
// gates the nearest-following candidate by held.key - left.key <= Δt. Δt = 1.5s here, so
// e@13.300 (nearest following r-400@14.400, 1.1s away) matches, but e@11.100 (nearest
// r-200@12.200 is 1.1s -> matches) ... tuned below.
func TestASOFLoweringFollowingSoftDifferential(t *testing.T) {
	root := t.TempDir()
	asofWriteKS(t, root, "elog", "ns_server.error.log",
		nsLine("2026-05-17T15:36:11.100+02:00", "n1", "e-100")+ // nearest r-200@12.200 (1.1s) <= 1.5s -> match
			nsLine("2026-05-17T15:36:12.500+02:00", "n1", "e-250")+ // nearest r-400@14.400 (1.9s) > 1.5s -> null
			nsLine("2026-05-17T15:36:15.500+02:00", "n1", "e-500")) // no following r -> null
	asofWriteKS(t, root, "rlog", "ns_server.rebalance.log",
		nsLine("2026-05-17T15:36:12.200+02:00", "n1", "r-200")+
			nsLine("2026-05-17T15:36:14.400+02:00", "n1", "r-400"))
	s, err := OpenSession(root, "default")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Δt = 1.5s = 1_500_000_000 ns.
	stmt := "SELECT e.ts AS ts, " +
		"(SELECT r.msg FROM default:rlog r WHERE r.ts >= e.ts AND r.ts <= e.ts + 1500000000 " +
		"ORDER BY r.ts ASC LIMIT 1) AS nx FROM default:elog e ORDER BY e.ts"

	off, _, fired := runBoth(t, s, stmt)
	if !fired {
		t.Fatalf("expected soft-following (look-ahead) to lower; it did not")
	}
	if off[0] != `{"ts":1779024971100000000,"nx":[{"msg":"r-200"}]}` {
		t.Fatalf("row0 (within 1.5s) unexpected: %s", off[0])
	}
	if off[1] != `{"ts":1779024972500000000,"nx":null}` {
		t.Fatalf("row1 (nearest following beyond 1.5s -> null) unexpected: %s", off[1])
	}
	if off[2] != `{"ts":1779024975500000000,"nx":null}` {
		t.Fatalf("row2 (no following) unexpected: %s", off[2])
	}
}

// TestASOFLoweringFollowingPartitionedDifferential: following + a partition equi-key
// (r.node = e.node) -- each E row correlated to the nearest following R row OF THE SAME
// NODE. Exercises the per-partition forward cursor.
func TestASOFLoweringFollowingPartitionedDifferential(t *testing.T) {
	root := t.TempDir()
	// Two nodes interleaved by time; the following R must respect the node partition.
	asofWriteKS(t, root, "elog", "ns_server.error.log",
		nsLine("2026-05-17T15:36:11.000+02:00", "n1", "e-n1")+
			nsLine("2026-05-17T15:36:11.500+02:00", "n2", "e-n2")+
			nsLine("2026-05-17T15:36:16.000+02:00", "n1", "e-n1-late"))
	asofWriteKS(t, root, "rlog", "ns_server.rebalance.log",
		nsLine("2026-05-17T15:36:12.000+02:00", "n2", "r-n2")+ // only following for n2 rows
			nsLine("2026-05-17T15:36:13.000+02:00", "n1", "r-n1")) // only following for early n1
	s, err := OpenSession(root, "default")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	stmt := "SELECT e.node AS node, e.ts AS ts, " +
		"(SELECT r.msg FROM default:rlog r WHERE r.ts >= e.ts AND r.node = e.node ORDER BY r.ts ASC LIMIT 1) AS nx " +
		"FROM default:elog e ORDER BY e.ts, e.node"

	off, _, fired := runBoth(t, s, stmt)
	if !fired {
		t.Fatalf("expected following + partition to lower; it did not")
	}
	// e-n1@11.0 -> nearest following n1 = r-n1@13.0; e-n2@11.5 -> nearest following n2 =
	// r-n2@12.0; e-n1-late@16.0 -> no following n1 -> null.
	if len(off) != 3 {
		t.Fatalf("want 3 rows, got %d: %v", len(off), off)
	}
	if off[0] != `{"node":"n1","ts":1779024971000000000,"nx":[{"msg":"r-n1"}]}` {
		t.Fatalf("row0 (n1 -> r-n1) unexpected: %s", off[0])
	}
	if off[1] != `{"node":"n2","ts":1779024971500000000,"nx":[{"msg":"r-n2"}]}` {
		t.Fatalf("row1 (n2 -> r-n2) unexpected: %s", off[1])
	}
	if off[2] != `{"node":"n1","ts":1779024976000000000,"nx":null}` {
		t.Fatalf("row2 (n1 late -> no following) unexpected: %s", off[2])
	}
}

// TestASOFLoweringSingleFileKeyspace is the IDEA-0016 gate: ASOF must lower over
// flat SINGLE-FILE recipe keyspaces (a cbcollect bundle exposes ns_server.error /
// cbcollect_info as one top-level file each, not a <ns>/<keyspace>/ dir). The sort-key
// gate resolves metadata via KeyspaceDir, which has no dir for a single-file keyspace
// -> it saw "no sorted-source metadata" and bailed to the O(n^2) correlated path, even
// though describe measures the file as sorted. keyspaceFiles now resolves the file
// directly, so the gate sees the metadata and the rewrite fires.
func TestASOFLoweringSingleFileKeyspace(t *testing.T) {
	root := t.TempDir()
	// Flat bundle layout: recipe log files at the TOP LEVEL (each its own single-file
	// keyspace by stem) plus a subdir, which is what makes it the per-file (B3) layout.
	writeTop := func(name, body string) {
		if err := os.WriteFile(filepath.Join(root, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	writeTop("ns_server.error.log",
		nsLine("2026-05-17T15:36:11.100+02:00", "n1", "e-100")+ // no preceding R -> null
			nsLine("2026-05-17T15:36:13.300+02:00", "n1", "e-300")) // nearest R@12.200
	writeTop("ns_server.rebalance.log",
		nsLine("2026-05-17T15:36:12.200+02:00", "n1", "r-200"))
	if err := os.MkdirAll(filepath.Join(root, "certs"), 0o755); err != nil { // subdir -> B3
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "certs", "c.txt"), []byte("x\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	s, err := OpenSession(root, "default")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	stmt := "SELECT e.ts AS ts, " +
		"(SELECT r.msg FROM `ns_server.rebalance` r WHERE r.ts <= e.ts ORDER BY r.ts DESC LIMIT 1) AS state_at " +
		"FROM `ns_server.error` e ORDER BY e.ts"

	off, _, fired := runBoth(t, s, stmt)
	if !fired {
		t.Fatalf("expected ASOF to FIRE over single-file recipe keyspaces (IDEA-0016); it did not")
	}
	if len(off) != 2 {
		t.Fatalf("want 2 rows, got %d: %v", len(off), off)
	}
	if off[0] != `{"ts":1779024971100000000,"state_at":null}` {
		t.Fatalf("row0 (no preceding -> null) unexpected: %s", off[0])
	}
	if off[1] != `{"ts":1779024973300000000,"state_at":[{"msg":"r-200"}]}` {
		t.Fatalf("row1 (nearest preceding R@12.200) unexpected: %s", off[1])
	}
}

// TestASOFLoweringScalarForms is the IDEA-0014-followup gate: the argmax lowering
// accepts the scalar-extraction subquery forms -- (SELECT ...)[0] and SELECT RAW (and
// their combination, the natural "give me the scalar" idiom) -- not just the bare
// subquery. Each form must FIRE and stay byte-identical to its correlated baseline
// (runBoth), so the reconstructed result reproduces the subquery's value exactly.
func TestASOFLoweringScalarForms(t *testing.T) {
	root := t.TempDir()
	asofWriteKS(t, root, "elog", "ns_server.error.log",
		nsLine("2026-05-17T15:36:11.100+02:00", "n1", "e-100")+ // no preceding R -> null
			nsLine("2026-05-17T15:36:13.300+02:00", "n1", "e-300")) // nearest R@12.200 = r-200
	asofWriteKS(t, root, "rlog", "ns_server.rebalance.log",
		nsLine("2026-05-17T15:36:12.200+02:00", "n1", "r-200"))

	s, err := OpenSession(root, "default")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	const tail = " FROM default:rlog r WHERE r.ts <= e.ts ORDER BY r.ts DESC LIMIT 1)"
	cases := []struct {
		name string
		proj string // the AS state_at projection term
	}{
		{"element0", "(SELECT r.msg" + tail + "[0]"},         // {"msg":"r-200"} / null
		{"raw", "(SELECT RAW r.msg" + tail},                  // ["r-200"] / null
		{"raw_element0", "(SELECT RAW r.msg" + tail + "[0]"}, // "r-200" (scalar) / null
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stmt := "SELECT e.ts AS ts, " + tc.proj + " AS state_at FROM default:elog e ORDER BY e.ts"
			off, _, fired := runBoth(t, s, stmt)
			if !fired {
				t.Fatalf("%s: expected the ASOF lowering to FIRE; it did not", tc.name)
			}
			if len(off) != 2 {
				t.Fatalf("%s: want 2 rows, got %d: %v", tc.name, len(off), off)
			}
		})
	}

	// Spot-check the scalar idiom yields a bare scalar (not an array/object): the
	// (SELECT RAW ...)[0] form's second row is the string "r-200".
	stmt := "SELECT e.ts AS ts, (SELECT RAW r.msg FROM default:rlog r WHERE r.ts <= e.ts " +
		"ORDER BY r.ts DESC LIMIT 1)[0] AS state_at FROM default:elog e ORDER BY e.ts"
	off, _, _ := runBoth(t, s, stmt)
	if off[1] != `{"ts":1779024973300000000,"state_at":"r-200"}` {
		t.Fatalf("(SELECT RAW ...)[0] should give a bare scalar; got: %s", off[1])
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
	beforeSkip := AsofRewriteSkipped
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
	// IDEA-0014 observability: a recognized argmax that could not lower records WHY
	// (here: the plain keyspace has no sorted-source metadata), rather than silently
	// falling back -- so it's not an invisible O(n^2).
	if AsofRewriteSkipped <= beforeSkip {
		t.Fatalf("expected a skip to be recorded for the recognized-but-unlowered argmax")
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
