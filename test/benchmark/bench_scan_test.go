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
	"testing"

	"github.com/couchbase/n1k1/base"
)

// scales: a fixed 1000-distinct-doc source amplified via scan `reps` to hit the
// effective row count. allocs/op should stay ~flat across scales (the garbage-
// avoidance claim: recycle []byte, no per-row boxing); rows/s shows throughput.
var scanScales = []struct {
	name string
	reps int
}{
	{"1K", 1}, {"100K", 100}, {"1M", 1000},
}

const scanDistinct = 1000 // source docs; reps multiplies to the effective rows.

// BenchmarkScan -- raw scan throughput (parse one doc per row, yield).
func BenchmarkScan(b *testing.B) {
	jsons := GenJSONs(scanDistinct, scanDistinct)
	for _, s := range scanScales {
		b.Run(s.name, func(b *testing.B) {
			vars, cleanup := newVars()
			defer cleanup()
			op := opScan(jsons, s.reps)
			benchRows(b, op, vars, scanDistinct*s.reps)
		})
	}
}

// BenchmarkScanFilter -- scan -> filter(type == "contact"). Exercises the
// static-param eq expression on every row (push-based, no per-row alloc).
func BenchmarkScanFilter(b *testing.B) {
	jsons := GenJSONs(scanDistinct, scanDistinct)
	eq := []interface{}{"eq",
		[]interface{}{"labelPath", ".", "type"},
		[]interface{}{"json", `"contact"`}}
	for _, s := range scanScales {
		b.Run(s.name, func(b *testing.B) {
			vars, cleanup := newVars()
			defer cleanup()
			op := opFilter(opScan(jsons, s.reps), eq)
			benchRows(b, op, vars, scanDistinct*s.reps)
		})
	}
}

// BenchmarkScanFilterProject -- the full scan -> filter -> project pipeline,
// the canonical push-based-fusion shape. project emits g + age as new registers.
func BenchmarkScanFilterProject(b *testing.B) {
	jsons := GenJSONs(scanDistinct, scanDistinct)
	eq := []interface{}{"eq",
		[]interface{}{"labelPath", ".", "type"},
		[]interface{}{"json", `"contact"`}}
	projLabels := base.Labels{`.["g"]`, `.["age"]`}
	projExprs := []interface{}{
		[]interface{}{"labelPath", ".", "g"},
		[]interface{}{"labelPath", ".", "age"},
	}
	for _, s := range scanScales {
		b.Run(s.name, func(b *testing.B) {
			vars, cleanup := newVars()
			defer cleanup()
			op := opProject(opFilter(opScan(jsons, s.reps), eq), projLabels, projExprs)
			benchRows(b, op, vars, scanDistinct*s.reps)
		})
	}
}

// confirm the pipeline yields the rows we expect (all pass the type filter),
// so a benchmark regression that silently drops rows is caught.
func TestPipelineSanity(t *testing.T) {
	jsons := GenJSONs(100, 100)
	vars, cleanup := newVars()
	defer cleanup()
	op := opProject(
		opFilter(opScan(jsons, 1), []interface{}{"eq",
			[]interface{}{"labelPath", ".", "type"},
			[]interface{}{"json", `"contact"`}}),
		base.Labels{`.["g"]`}, []interface{}{[]interface{}{"labelPath", ".", "g"}})
	if got := runRows(t, op, vars); got != 100 {
		t.Fatalf("expected 100 rows, got %d", got)
	}
}
