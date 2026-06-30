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

package benchmark

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/engine"
	"github.com/couchbase/n1k1/glue"
)

// newVars returns a fully-configured *base.Vars (spill allocators + the engine
// expr catalog) and a cleanup. Created once per benchmark and reused across the
// b.N iterations, so the loop measures execution -- not setup.
func newVars() (*base.Vars, func()) {
	tmpDir, vars := glue.MakeVars("", "n1k1bench")
	return vars, func() { os.RemoveAll(tmpDir) }
}

// --- base.Op tree builders (built once, executed many) -------------------

// opScan yields the jsonsData docs, optionally replicated `reps` times (cheap
// way to amplify row count without growing the source string).
func opScan(jsons string, reps int) *base.Op {
	return &base.Op{
		Kind:   "scan",
		Labels: base.Labels{"."},
		Params: []interface{}{"jsonsData", jsons, reps},
	}
}

// opFilter applies a built-in (static-param) expression, e.g.
// ["eq", ["labelPath",".","type"], ["json",`"contact"`]].
func opFilter(child *base.Op, expr []interface{}) *base.Op {
	return &base.Op{
		Kind:     "filter",
		Labels:   base.Labels{"."},
		Params:   expr,
		Children: []*base.Op{child},
	}
}

// opProject emits the given labels from the given per-label expressions.
func opProject(child *base.Op, labels base.Labels, exprs []interface{}) *base.Op {
	return &base.Op{
		Kind:     "project",
		Labels:   labels,
		Params:   exprs,
		Children: []*base.Op{child},
	}
}

// opGroupCountG is GROUP BY g, COUNT(*) -- the cardinality knob for the spill
// study. Distinct "g" values drive the rhmap's distinct-key count.
func opGroupCountG(child *base.Op) *base.Op {
	return &base.Op{
		Kind:   "group",
		Labels: base.Labels{`.["g"]`, "count"},
		Params: []interface{}{
			[]interface{}{[]interface{}{"labelPath", ".", "g"}}, // group keys
			[]interface{}{[]interface{}{"labelPath", "."}},      // agg input
			[]interface{}{[]interface{}{"count"}},               // agg calc
		},
		Children: []*base.Op{child},
	}
}

// --- run helpers ----------------------------------------------------------

// runRows executes op with a no-op yield (count rows, discard bytes) so the
// measurement is the engine, not output formatting. Fails on any yieldErr.
func runRows(tb testing.TB, op *base.Op, vars *base.Vars) int {
	n := 0
	yieldVals := func(base.Vals) { n++ }
	yieldErr := func(err error) {
		if err != nil {
			tb.Fatalf("yieldErr: %v", err)
		}
	}
	engine.ExecOp(op, vars, yieldVals, yieldErr, "", "")
	return n
}

// benchRows runs op b.N times and reports rows/s alongside ns/op + allocs.
// rowsPerOp is the number of input rows the op processes per execution.
func benchRows(b *testing.B, op *base.Op, vars *base.Vars, rowsPerOp int) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runRows(b, op, vars)
	}
	b.StopTimer()
	if secs := b.Elapsed().Seconds(); secs > 0 {
		b.ReportMetric(float64(rowsPerOp)*float64(b.N)/secs, "rows/s")
	}
}

// --- spill detection ------------------------------------------------------

// tmpDirSpill walks an rhmap/store temp dir and reports whether the hashmap's
// metadata slots grew onto disk (a "*_slots_*" file => Generation > 0, i.e. it
// spilled past the in-memory StartSize) and the total bytes of all temp files.
func tmpDirSpill(dir string) (slotsFiles int, totalBytes int64) {
	filepath.Walk(dir, func(p string, fi os.FileInfo, err error) error {
		if err != nil || fi.IsDir() {
			return nil
		}
		if strings.Contains(filepath.Base(p), "_slots_") {
			slotsFiles++
		}
		totalBytes += fi.Size()
		return nil
	})
	return slotsFiles, totalBytes
}
