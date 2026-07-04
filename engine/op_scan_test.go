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

func scanOp(n int) *base.Op {
	return &base.Op{
		Kind:   "scan",
		Labels: base.Labels{"."},
		Params: []interface{}{"jsonsData", makeJsons(n)},
	}
}

// runOpWithStats lays out stats for the op tree, runs it with a full-enough Ctx
// (expr catalog + comparer, so filters evaluate), and returns the resulting
// *Stats plus the number of rows yielded at the top.
func runOpWithStats(t *testing.T, root *base.Op) (*base.Stats, int) {
	t.Helper()

	stats := base.LayoutStats(root)
	if stats == nil {
		t.Fatal("LayoutStats returned nil; expected a counter-contributing op")
	}

	vars := &base.Vars{
		Temps: make([]interface{}, 16),
		Ctx: &base.Ctx{
			Stats:       stats,
			ExprCatalog: ExprCatalog,
			ValComparer: base.NewValComparer(),
			YieldStats:  func(s *base.Stats) error { return nil },
		},
	}

	var rowsYielded int
	yieldVals := func(vals base.Vals) { rowsYielded++ }
	yieldErr := func(err error) {
		if err != nil {
			t.Fatalf("yieldErr: %v", err)
		}
	}

	ExecOp(root, vars, yieldVals, yieldErr, "", "")

	return stats, rowsYielded
}

// TestScanStatsRowsOut checks the flat-counter core end-to-end on a scan: the
// RowsOut counter must equal the rows actually yielded, both under a single
// final flush (< 1024 rows) and across many YieldStats checkpoints (> 1024).
func TestScanStatsRowsOut(t *testing.T) {
	for _, n := range []int{0, 1, 3, 1024, 2500} {
		o := scanOp(n)

		stats, rowsYielded := runOpWithStats(t, o)

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

// TestFilterStats checks RowsIn/RowsOut on a filter over a scan, using constant
// predicates so no field access is needed: "true" passes every row, "false"
// passes none. RowsIn must equal the scan's output either way.
func TestFilterStats(t *testing.T) {
	const n = 7

	cases := []struct {
		pred    string
		wantOut int64
	}{
		{"true", n},
		{"false", 0},
	}

	for _, c := range cases {
		filter := &base.Op{
			Kind:     "filter",
			Labels:   base.Labels{"."},
			Params:   []interface{}{"json", c.pred},
			Children: []*base.Op{scanOp(n)},
		}

		stats, rowsYielded := runOpWithStats(t, filter)

		if rowsYielded != int(c.wantOut) {
			t.Fatalf("pred=%q: yielded %d rows, want %d", c.pred, rowsYielded, c.wantOut)
		}

		// filter is the root op "0"; scan is child "0/0".
		if got, ok := stats.Get("0:RowsIn"); !ok || got != n {
			t.Fatalf("pred=%q: filter RowsIn=(%d,%v), want %d", c.pred, got, ok, n)
		}
		if got, ok := stats.Get("0:RowsOut"); !ok || got != c.wantOut {
			t.Fatalf("pred=%q: filter RowsOut=(%d,%v), want %d", c.pred, got, ok, c.wantOut)
		}
		if got, ok := stats.Get("0/0:RowsOut"); !ok || got != n {
			t.Fatalf("pred=%q: scan RowsOut=(%d,%v), want %d", c.pred, got, ok, n)
		}
	}
}

// TestLayoutStatsTree checks base offsets and the attribution index for a small
// tree. filter (registered) and scan (registered) each get a section, keyed by
// tree path; sections are laid out pre-order (filter root, then its scan child).
func TestLayoutStatsTree(t *testing.T) {
	scan := scanOp(2)
	filter := &base.Op{
		Kind:     "filter",
		Labels:   base.Labels{"."},
		Params:   []interface{}{"json", "true"},
		Children: []*base.Op{scan},
	}

	stats := base.LayoutStats(filter)
	if stats == nil {
		t.Fatal("LayoutStats returned nil")
	}

	// Pre-order: filter (root "0") gets base 0 with 2 slots (RowsIn, RowsOut);
	// scan ("0/0") follows at base 2 with 1 slot (RowsOut). Total 3.
	if filter.StatsBase != 0 {
		t.Fatalf("filter.StatsBase=%d, want 0", filter.StatsBase)
	}
	if scan.StatsBase != 2 {
		t.Fatalf("scan.StatsBase=%d, want 2", scan.StatsBase)
	}
	if len(stats.Counters) != 3 {
		t.Fatalf("len(Counters)=%d, want 3", len(stats.Counters))
	}

	for _, key := range []string{"0:RowsIn", "0:RowsOut", "0/0:RowsOut"} {
		if _, ok := stats.Index[key]; !ok {
			t.Fatalf("Index missing key %q; have %v", key, stats.Index)
		}
	}
	if len(stats.Ops) != 2 || stats.Ops[0].Kind != "filter" || stats.Ops[1].Kind != "scan" {
		t.Fatalf("Ops=%+v, want filter then scan", stats.Ops)
	}
}

// TestScanStatsOff verifies the zero-cost off path: with no Stats on the Ctx, a
// scan runs normally and never touches the counter machinery.
func TestScanStatsOff(t *testing.T) {
	o := scanOp(5)

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
