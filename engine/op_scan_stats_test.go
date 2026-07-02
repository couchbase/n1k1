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

package engine

import (
	"strconv"
	"strings"
	"testing"

	"github.com/couchbase/n1k1/base"
)

// makeJsons returns n one-per-line JSON docs.
func makeJsons(n int) string {
	var sb strings.Builder
	for i := 0; i < n; i++ {
		sb.WriteString(`{"a":`)
		sb.WriteString(strconv.Itoa(i))
		sb.WriteString("}\n")
	}
	return sb.String()
}

// runScanWithStats builds a scan op over the given jsons, lays out stats, runs
// the op, and returns the resulting *Stats plus how many rows were yielded.
func runScanWithStats(t *testing.T, o *base.Op, jsons string) (*base.Stats, int) {
	t.Helper()

	stats := base.LayoutStats(o)
	if stats == nil {
		t.Fatal("LayoutStats returned nil; expected scan to contribute a counter")
	}

	var yieldStatsCalls int

	vars := &base.Vars{
		Ctx: &base.Ctx{
			Stats:      stats,
			YieldStats: func(s *base.Stats) error { yieldStatsCalls++; return nil },
		},
	}

	var rowsYielded int
	yieldVals := func(vals base.Vals) { rowsYielded++ }
	yieldErr := func(err error) {
		if err != nil {
			t.Fatalf("yieldErr: %v", err)
		}
	}

	ExecOp(o, vars, yieldVals, yieldErr, "", "")

	return stats, rowsYielded
}

// TestScanStatsRowsOut checks the flat-counter core end-to-end on a scan: the
// RowsOut counter must equal the rows actually yielded, both under a single
// final flush (< 1024 rows) and across many YieldStats checkpoints (> 1024).
func TestScanStatsRowsOut(t *testing.T) {
	for _, n := range []int{0, 1, 3, 1024, 2500} {
		o := &base.Op{
			Kind:   "scan",
			Labels: base.Labels{"."},
			Params: []interface{}{"jsonsData", makeJsons(n)},
		}

		stats, rowsYielded := runScanWithStats(t, o, "")

		if rowsYielded != n {
			t.Fatalf("n=%d: yielded %d rows, want %d", n, rowsYielded, n)
		}

		// Counter reachable both by baked offset and by "opId:statName".
		if got := stats.Counters[o.StatsBase+StatScanRowsOut]; got != int64(n) {
			t.Fatalf("n=%d: Counters[base+RowsOut]=%d, want %d", n, got, n)
		}

		if got, ok := stats.Get("0:RowsOut"); !ok || got != int64(n) {
			t.Fatalf("n=%d: Get(\"0:RowsOut\")=(%d,%v), want (%d,true)", n, got, ok, n)
		}
	}
}

// TestLayoutStatsTree checks base offsets and the attribution index for a small
// tree: only ops registered in StatsDescs get a section, keyed by tree path.
func TestLayoutStatsTree(t *testing.T) {
	scan := &base.Op{
		Kind:   "scan",
		Labels: base.Labels{"."},
		Params: []interface{}{"jsonsData", makeJsons(2)},
	}
	filter := &base.Op{
		Kind:     "filter",
		Labels:   base.Labels{"."},
		Children: []*base.Op{scan},
	}

	stats := base.LayoutStats(filter)
	if stats == nil {
		t.Fatal("LayoutStats returned nil")
	}

	// filter contributes no counters (not registered) -> StatsBase -1.
	if filter.StatsBase != -1 {
		t.Fatalf("filter.StatsBase=%d, want -1", filter.StatsBase)
	}
	// scan is the only counter-contributing op -> base 0, one slot.
	if scan.StatsBase != 0 {
		t.Fatalf("scan.StatsBase=%d, want 0", scan.StatsBase)
	}
	if len(stats.Counters) != 1 {
		t.Fatalf("len(Counters)=%d, want 1", len(stats.Counters))
	}

	// The scan sits at tree path "0/0" (child 0 of the root filter "0").
	if _, ok := stats.Index["0/0:RowsOut"]; !ok {
		t.Fatalf("Index missing key 0/0:RowsOut; have %v", stats.Index)
	}
	if len(stats.Ops) != 1 || stats.Ops[0].Id != "0/0" || stats.Ops[0].Kind != "scan" {
		t.Fatalf("Ops=%+v, want one scan at id 0/0", stats.Ops)
	}
}

// TestScanStatsOff verifies the zero-cost off path: with no Stats on the Ctx, a
// scan runs normally and never touches the counter machinery.
func TestScanStatsOff(t *testing.T) {
	o := &base.Op{
		Kind:   "scan",
		Labels: base.Labels{"."},
		Params: []interface{}{"jsonsData", makeJsons(5)},
	}

	vars := &base.Vars{Ctx: &base.Ctx{}} // Stats nil, YieldStats nil.

	var rowsYielded int
	ExecOp(o, vars,
		func(vals base.Vals) { rowsYielded++ },
		func(err error) {
			if err != nil {
				t.Fatalf("yieldErr: %v", err)
			}
		}, "", "")

	if rowsYielded != 5 {
		t.Fatalf("yielded %d rows, want 5", rowsYielded)
	}
}
