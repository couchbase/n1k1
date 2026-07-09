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
	"reflect"
	"strconv"
	"testing"

	"github.com/couchbase/n1k1/base"
)

// broadcastVars builds a Vars with a full-enough Ctx (expr catalog + comparer)
// so predicate / projection expr-trees evaluate.
func broadcastVars() *base.Vars {
	return &base.Vars{
		Temps: make([]interface{}, 16),
		Ctx: &base.Ctx{
			ExprCatalog: ExprCatalog,
			ValComparer: base.NewValComparer(),
		},
	}
}

// collectRows runs an op tree and returns each yielded row as a []string (one
// per Val), deep-copying so reused output buffers don't alias across rows.
func collectRows(t *testing.T, root *base.Op, vars *base.Vars) [][]string {
	t.Helper()

	var out [][]string
	ExecOp(root, vars,
		func(vals base.Vals) {
			row := make([]string, len(vals))
			for i, v := range vals {
				row[i] = string(v)
			}
			out = append(out, row)
		},
		func(err error) {
			if err != nil {
				t.Fatalf("yieldErr: %v", err)
			}
		}, "", "")
	return out
}

// broadcastTestDetector describes one detector for the test: a tag, a predicate
// expr-tree, and a single projection expr-tree (uniform findings schema).
type broadcastTestDetector struct {
	tag  string
	pred []interface{}
	proj []interface{} // one expr-tree
}

// lp builds a ["labelPath", "<label>", "<path>..."] operand tree.
func lp(label string, path ...string) []interface{} {
	t := []interface{}{"labelPath", label}
	for _, p := range path {
		t = append(t, p)
	}
	return t
}

// TestOpBroadcastEquivalence proves that running K detectors through ONE
// broadcast over a single scan yields, per tag, EXACTLY the rows that running
// each detector as a separate scan -> filter -> project pipeline yields. It
// covers a detector matching ALL rows, one matching NONE, one matching SOME
// (a > threshold), and one whose predicate reads a MISSING field (which must be
// non-truthy and match nothing, exactly like OpFilter drops MISSING).
func TestOpBroadcastEquivalence(t *testing.T) {
	const n = 20

	dets := []broadcastTestDetector{
		{tag: "all", pred: []interface{}{"json", "true"}, proj: lp(".", "a")},
		{tag: "none", pred: []interface{}{"json", "false"}, proj: lp(".", "a")},
		{tag: "some", pred: []interface{}{"gt", lp(".", "a"), []interface{}{"json", "12"}}, proj: lp(".", "a")},
		{tag: "missing", pred: lp(".", "nope"), proj: lp(".", "a")},
	}

	// Build the broadcast op: Params[0] = []detector, each = {tag, pred, [proj]}.
	var detParams []interface{}
	for _, d := range dets {
		detParams = append(detParams, []interface{}{
			d.tag, d.pred, []interface{}{d.proj},
		})
	}

	broadcast := &base.Op{
		Kind:     "broadcast",
		Labels:   base.Labels{"tag", "a"},
		Params:   []interface{}{detParams},
		Children: []*base.Op{scanOp(n)},
	}

	// Run the broadcast once; group the interleaved tagged findings by tag. Slot 0
	// is the tag (a JSON string, e.g. "all"); slot 1 is the projected evidence.
	got := map[string][]string{}
	for _, row := range collectRows(t, broadcast, broadcastVars()) {
		if len(row) != 2 {
			t.Fatalf("broadcast finding has %d slots, want 2: %v", len(row), row)
		}
		tag, err := strconv.Unquote(row[0])
		if err != nil {
			t.Fatalf("slot-0 tag %q not a JSON string: %v", row[0], err)
		}
		got[tag] = append(got[tag], row[1])
	}

	// Oracle: each detector as its own scan -> filter -> project pipeline.
	for _, d := range dets {
		oracle := &base.Op{
			Kind:   "project",
			Labels: base.Labels{"a"},
			Params: []interface{}{d.proj},
			Children: []*base.Op{{
				Kind:     "filter",
				Labels:   base.Labels{"."},
				Params:   d.pred,
				Children: []*base.Op{scanOp(n)},
			}},
		}

		var want []string
		for _, row := range collectRows(t, oracle, broadcastVars()) {
			want = append(want, row[0])
		}

		if !reflect.DeepEqual(got[d.tag], want) {
			t.Fatalf("detector %q: broadcast findings %v != separate-pipeline %v",
				d.tag, got[d.tag], want)
		}
	}

	// Spot-check the coverage the cases are meant to exercise.
	if len(got["all"]) != n {
		t.Fatalf("tag all: got %d findings, want %d", len(got["all"]), n)
	}
	if len(got["none"]) != 0 {
		t.Fatalf("tag none: got %d findings, want 0", len(got["none"]))
	}
	if len(got["some"]) != n-13 { // a in 13..19
		t.Fatalf("tag some: got %d findings, want %d", len(got["some"]), n-13)
	}
	if len(got["missing"]) != 0 {
		t.Fatalf("tag missing: got %d findings, want 0 (MISSING is non-truthy)", len(got["missing"]))
	}
}

// TestOpBroadcastScansOnce proves the whole point of the op: broadcasting K
// detectors decodes each row ONCE (the shared scan's RowsOut == N and the
// broadcast's RowsIn == N), whereas K separate pipelines decode K*N rows.
func TestOpBroadcastScansOnce(t *testing.T) {
	const n = 50
	const k = 8

	var detParams []interface{}
	for i := 0; i < k; i++ {
		detParams = append(detParams, []interface{}{
			"d" + strconv.Itoa(i),
			[]interface{}{"gt", lp(".", "a"), []interface{}{"json", strconv.Itoa(i)}},
			[]interface{}{lp(".", "a")},
		})
	}

	broadcast := &base.Op{
		Kind:     "broadcast",
		Labels:   base.Labels{"tag", "a"},
		Params:   []interface{}{detParams},
		Children: []*base.Op{scanOp(n)},
	}

	stats := base.LayoutStats(broadcast)
	if stats == nil {
		t.Fatal("LayoutStats returned nil")
	}
	vars := broadcastVars()
	vars.Ctx.Stats = stats

	collectRows(t, broadcast, vars)

	// broadcast is root "0"; its shared scan is child "0/0".
	if got, ok := stats.Get("0:RowsIn"); !ok || got != n {
		t.Fatalf("broadcast RowsIn=(%d,%v), want %d (one decode per row)", got, ok, n)
	}
	if got, ok := stats.Get("0/0:RowsOut"); !ok || got != n {
		t.Fatalf("shared scan RowsOut=(%d,%v), want %d (scanned once)", got, ok, n)
	}
}

// -----------------------------------------------------

// benchDetectorParams builds k detectors (a > i, projecting a) for benchmarks.
func benchDetectorParams(k int) []interface{} {
	var detParams []interface{}
	for i := 0; i < k; i++ {
		detParams = append(detParams, []interface{}{
			"d" + strconv.Itoa(i),
			[]interface{}{"gt", lp(".", "a"), []interface{}{"json", strconv.Itoa(i)}},
			[]interface{}{lp(".", "a")},
		})
	}
	return detParams
}

// BenchmarkBroadcastVsSeparate contrasts K detectors fanned out over ONE scan
// (broadcast) with the same K detectors run as K independent scan+filter+project
// pipelines. The broadcast decodes each of the N rows once; the separate runs
// decode K*N rows -- the win the op exists to capture.
func BenchmarkBroadcastVsSeparate(b *testing.B) {
	const n = 1000
	const k = 16

	data := makeJsons(n)
	scan := func() *base.Op {
		return &base.Op{
			Kind:   "scan",
			Labels: base.Labels{"."},
			Params: []interface{}{"jsonsData", data},
		}
	}

	detParams := benchDetectorParams(k)

	sink := func(base.Vals) {}
	noErr := func(err error) {
		if err != nil {
			b.Fatalf("yieldErr: %v", err)
		}
	}

	b.Run("broadcast", func(b *testing.B) {
		broadcast := &base.Op{
			Kind:     "broadcast",
			Labels:   base.Labels{"tag", "a"},
			Params:   []interface{}{detParams},
			Children: []*base.Op{scan()},
		}
		vars := broadcastVars()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ExecOp(broadcast, vars, sink, noErr, "", "")
		}
	})

	b.Run("separate", func(b *testing.B) {
		// Build the K pipelines once; each shares nothing (its own scan).
		pipelines := make([]*base.Op, k)
		for j := 0; j < k; j++ {
			det := detParams[j].([]interface{})
			pipelines[j] = &base.Op{
				Kind:   "project",
				Labels: base.Labels{"a"},
				Params: det[2].([]interface{}),
				Children: []*base.Op{{
					Kind:     "filter",
					Labels:   base.Labels{"."},
					Params:   det[1].([]interface{}),
					Children: []*base.Op{scan()},
				}},
			}
		}
		vars := broadcastVars()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for _, p := range pipelines {
				ExecOp(p, vars, sink, noErr, "", "")
			}
		}
	})
}
