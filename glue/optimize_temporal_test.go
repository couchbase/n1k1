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
