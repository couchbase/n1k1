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
	"strconv"
	"testing"
)

// benchPeerSession writes n docs whose ORDER BY key k is CONSTANT (one big peer group),
// so a RANGE/GROUPS CURRENT ROW frame's edge is the whole group -- the shape whose
// per-row FindGroupEdge outward re-scan is O(N^2) in CurrentUpdate.
func benchPeerSession(b *testing.B, n int) *Session {
	b.Helper()
	dir := b.TempDir()
	ks := filepath.Join(dir, "default", "peers")
	if err := os.MkdirAll(ks, 0o755); err != nil {
		b.Fatal(err)
	}
	var sb []byte
	for i := 0; i < n; i++ {
		sb = append(sb, []byte(`{"k":1,"x":`+strconv.Itoa(i)+"}\n")...)
	}
	if err := os.WriteFile(filepath.Join(ks, "p.jsonl"), sb, 0o644); err != nil {
		b.Fatal(err)
	}
	sess, err := OpenSession(dir, "default")
	if err != nil {
		b.Fatalf("OpenSession: %v", err)
	}
	return sess
}

func benchRangePeer(b *testing.B, n int, stmt string) {
	sess := benchPeerSession(b, n)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := sess.Run(stmt); err != nil {
			b.Fatalf("Run: %v", err)
		}
	}
}

// RANGE CURRENT ROW (the default running-total frame) over one big peer group: End =
// FindGroupEdge(Pos,+1) walks to the group end every row.
func BenchmarkWindowRangePeer2000(b *testing.B) {
	benchRangePeer(b, 2000, `SELECT SUM(x) OVER (ORDER BY k) AS s FROM peers`)
}
func BenchmarkWindowRangePeer4000(b *testing.B) {
	benchRangePeer(b, 4000, `SELECT SUM(x) OVER (ORDER BY k) AS s FROM peers`)
}

// GROUPS CURRENT ROW over one big peer group.
func BenchmarkWindowGroupsPeer2000(b *testing.B) {
	benchRangePeer(b, 2000, `SELECT SUM(x) OVER (ORDER BY k GROUPS CURRENT ROW) AS s FROM peers`)
}
func BenchmarkWindowGroupsPeer4000(b *testing.B) {
	benchRangePeer(b, 4000, `SELECT SUM(x) OVER (ORDER BY k GROUPS CURRENT ROW) AS s FROM peers`)
}

// EXCLUDE GROUP over one big peer group: both peer edges recomputed each row.
func BenchmarkWindowExcludeGroupPeer4000(b *testing.B) {
	benchRangePeer(b, 4000, `SELECT COUNT(x) OVER (ORDER BY k ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP) AS s FROM peers`)
}
