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
	"sync/atomic"
	"testing"
)

// TestWireTemporalMergeMetaE2E proves the A->B integration end-to-end: a
// `... UNION ALL ... ORDER BY ts` over two recipe-matched (ns_server_log)
// keyspaces fires the metadata-driven merge rewrite (WireTemporalMergeMeta reads
// Track A's SortedSourceMeta, sees `ts` is a proven normalized int64 sort key,
// and lowers order(union-all) -> merge-scan) and returns globally time-ordered
// rows. A plain (non-recipe) keyspace must NOT fire (no sorted-source contract).
func TestWireTemporalMergeMetaE2E(t *testing.T) {
	root := t.TempDir()
	writeKS := func(ks, name, body string) {
		dir := filepath.Join(root, "default", ks)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// ns_server_log lead lines: [module:level,<RFC3339 ts>,node:...]msg. Each
	// keyspace is internally time-ordered; their ranges overlap so the merge must
	// interleave (heap regime): a=100ms,300ms  b=200ms,400ms -> 100,200,300,400.
	writeKS("alog", "ns_server.info.log",
		"[ns_server:info,2026-05-17T15:36:11.100+02:00,n1:x]a1\n"+
			"[ns_server:info,2026-05-17T15:36:13.300+02:00,n1:x]a3\n")
	writeKS("blog", "ns_server.info.log",
		"[ns_server:info,2026-05-17T15:36:12.200+02:00,n1:x]b2\n"+
			"[ns_server:info,2026-05-17T15:36:14.400+02:00,n1:x]b4\n")

	s, err := OpenSession(root, "default")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	tsOf := func(stmt string) []int64 {
		t.Helper()
		res, err := s.Run(stmt)
		if err != nil {
			t.Fatalf("Run(%q): %v", stmt, err)
		}
		out := make([]int64, 0, len(res.Rows))
		for _, r := range res.Rows {
			var row struct {
				Ts int64 `json:"ts"`
			}
			if err := json.Unmarshal(r, &row); err != nil {
				t.Fatalf("row %s: %v", r, err)
			}
			out = append(out, row.Ts)
		}
		return out
	}

	before := atomic.LoadInt64(&MergeMetaRewriteApplied)
	got := tsOf("SELECT e.ts AS ts, e.msg AS msg FROM default:alog e " +
		"UNION ALL SELECT w.ts AS ts, w.msg AS msg FROM default:blog w ORDER BY ts")

	if atomic.LoadInt64(&MergeMetaRewriteApplied) == before {
		t.Fatalf("expected the metadata-driven merge rewrite to FIRE; got rows %v", got)
	}
	if len(got) != 4 {
		t.Fatalf("want 4 merged rows, got %d: %v", len(got), got)
	}
	for i := 1; i < len(got); i++ {
		if got[i] < got[i-1] {
			t.Fatalf("merge output not ascending by ts: %v", got)
		}
	}
}

// mustRows runs stmt, failing the test on error, and returns its raw result rows.
func mustRows(t *testing.T, s *Session, stmt string) []json.RawMessage {
	t.Helper()
	res, err := s.Run(stmt)
	if err != nil {
		t.Fatalf("Run(%q): %v", stmt, err)
	}
	return res.Rows
}

// TestPerFileMergeOverlapping is the per-file correctness net for the UNION-ALL
// merge (DESIGN-merging.md "Multi-bundle / cross-node clusters"). A union branch
// keyspace resolves to TWO recipe files (two nodes) whose ts ranges OVERLAP, so the
// default single concatenated scan is NOT globally ts-ordered. Per-file expansion
// turns that branch into two ordered cursors the K-way merge interleaves.
//
//   - per-file ON: the merge fires, and its output is byte-identical to the oracle
//     (all rows in one keyspace, plain ORDER BY -- no merge involved).
//   - per-file OFF: the concatenated overlapping files trip the monotonicity
//     tripwire (the fix is REQUIRED, not merely present) -- never a silent mis-order.
func TestPerFileMergeOverlapping(t *testing.T) {
	root := t.TempDir()
	// clus = one keyspace, two files (two nodes), OVERLAPPING ts ranges.
	asofWriteKS(t, root, "clus", "ns_server.a.log",
		nsLine("2026-05-17T15:36:10.100+02:00", "n1", "a-100")+
			nsLine("2026-05-17T15:36:10.300+02:00", "n1", "a-300"))
	asofWriteKS(t, root, "clus", "ns_server.b.log",
		nsLine("2026-05-17T15:36:10.200+02:00", "n2", "b-200")+
			nsLine("2026-05-17T15:36:10.400+02:00", "n2", "b-400"))
	// other = a single-file second branch (so the union has >= 2 branches).
	asofWriteKS(t, root, "other", "ns_server.info.log",
		nsLine("2026-05-17T15:36:10.150+02:00", "n3", "o-150")+
			nsLine("2026-05-17T15:36:10.350+02:00", "n3", "o-350"))
	// allrows = the oracle: all six rows in one file, sorted by a plain ORDER BY.
	asofWriteKS(t, root, "allrows", "ns_server.all.log",
		nsLine("2026-05-17T15:36:10.100+02:00", "n1", "a-100")+
			nsLine("2026-05-17T15:36:10.150+02:00", "n3", "o-150")+
			nsLine("2026-05-17T15:36:10.200+02:00", "n2", "b-200")+
			nsLine("2026-05-17T15:36:10.300+02:00", "n1", "a-300")+
			nsLine("2026-05-17T15:36:10.350+02:00", "n3", "o-350")+
			nsLine("2026-05-17T15:36:10.400+02:00", "n2", "b-400"))

	s, err := OpenSession(root, "default")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	mergeStmt := "SELECT e.ts AS ts, e.msg AS msg FROM default:clus e " +
		"UNION ALL SELECT w.ts AS ts, w.msg AS msg FROM default:other w ORDER BY ts"
	oracleStmt := "SELECT a.ts AS ts, a.msg AS msg FROM default:allrows a ORDER BY a.ts"

	want := rowsAsStrings(mustRows(t, s, oracleStmt))

	beforeMerge := atomic.LoadInt64(&MergeMetaRewriteApplied)
	beforePF := atomic.LoadInt64(&PerFileMergeApplied)
	got := rowsAsStrings(mustRows(t, s, mergeStmt))
	if atomic.LoadInt64(&MergeMetaRewriteApplied) == beforeMerge {
		t.Fatalf("the UNION-ALL merge rewrite did not fire")
	}
	if atomic.LoadInt64(&PerFileMergeApplied) == beforePF {
		t.Fatalf("per-file expansion did not fire for the 2-file branch")
	}
	if len(got) != len(want) {
		t.Fatalf("row count merged=%d oracle=%d\n merged=%v\n oracle=%v", len(got), len(want), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("row[%d] merged=%s oracle=%s", i, got[i], want[i])
		}
	}

	// per-file OFF: the overlapping files concatenate into one non-monotonic stream,
	// which the merge's tripwire must reject rather than silently mis-order.
	prev := EnablePerFileMergeScans
	EnablePerFileMergeScans = false
	_, offErr := s.Run(mergeStmt)
	EnablePerFileMergeScans = prev
	if offErr == nil {
		t.Fatalf("without per-file expansion the concatenated overlapping files must" +
			" trip the monotonicity tripwire, but the query succeeded")
	}
}

// TestPerFileMergeDisjoint proves the concatenate path: a union branch keyspace of
// two files with NON-OVERLAPPING ranges still merges correctly (the engine's "auto"
// regime concatenates disjoint children). Output equals the plain-ORDER-BY oracle.
func TestPerFileMergeDisjoint(t *testing.T) {
	root := t.TempDir()
	// clus2 = two files, DISJOINT ts ranges (100-200 then 300-400).
	asofWriteKS(t, root, "clus2", "ns_server.a.log",
		nsLine("2026-05-17T15:36:10.100+02:00", "n1", "a-100")+
			nsLine("2026-05-17T15:36:10.200+02:00", "n1", "a-200"))
	asofWriteKS(t, root, "clus2", "ns_server.b.log",
		nsLine("2026-05-17T15:36:10.300+02:00", "n2", "b-300")+
			nsLine("2026-05-17T15:36:10.400+02:00", "n2", "b-400"))
	asofWriteKS(t, root, "other2", "ns_server.info.log",
		nsLine("2026-05-17T15:36:10.500+02:00", "n3", "o-500")+
			nsLine("2026-05-17T15:36:10.600+02:00", "n3", "o-600"))
	asofWriteKS(t, root, "allrows2", "ns_server.all.log",
		nsLine("2026-05-17T15:36:10.100+02:00", "n1", "a-100")+
			nsLine("2026-05-17T15:36:10.200+02:00", "n1", "a-200")+
			nsLine("2026-05-17T15:36:10.300+02:00", "n2", "b-300")+
			nsLine("2026-05-17T15:36:10.400+02:00", "n2", "b-400")+
			nsLine("2026-05-17T15:36:10.500+02:00", "n3", "o-500")+
			nsLine("2026-05-17T15:36:10.600+02:00", "n3", "o-600"))

	s, err := OpenSession(root, "default")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	mergeStmt := "SELECT e.ts AS ts, e.msg AS msg FROM default:clus2 e " +
		"UNION ALL SELECT w.ts AS ts, w.msg AS msg FROM default:other2 w ORDER BY ts"
	oracleStmt := "SELECT a.ts AS ts, a.msg AS msg FROM default:allrows2 a ORDER BY a.ts"

	want := rowsAsStrings(mustRows(t, s, oracleStmt))
	beforePF := atomic.LoadInt64(&PerFileMergeApplied)
	got := rowsAsStrings(mustRows(t, s, mergeStmt))
	if atomic.LoadInt64(&PerFileMergeApplied) == beforePF {
		t.Fatalf("per-file expansion did not fire for the disjoint 2-file branch")
	}
	if len(got) != len(want) {
		t.Fatalf("row count merged=%d oracle=%d\n merged=%v\n oracle=%v", len(got), len(want), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("row[%d] merged=%s oracle=%s", i, got[i], want[i])
		}
	}
}
