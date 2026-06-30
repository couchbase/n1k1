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
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/couchbase/n1k1/glue"
)

// TestSpillPoint pins where GROUP BY's rhmap/store grows its metadata slots
// onto disk. It sweeps distinct group-key cardinality and reports, per C:
// whether a "*_slots_*" file appeared (Generation > 0, i.e. spilled past the
// in-memory StartSize), the temp-file bytes, the bytes allocated on the Go heap
// during the run (TotalAlloc delta -- the spill should keep this sub-linear),
// and the wall time. Run: go test -tags n1ql -run TestSpillPoint -v ./test/benchmark
func TestSpillPoint(t *testing.T) {
	cards := []int{1000, 2000, 4000, 5000, 5303, 6000, 8000, 16000, 64000, 256000}

	t.Logf("%-8s %-8s %-7s %-5s %-12s %-12s %s",
		"distinct", "groups", "spill", "slots", "tmpBytes", "allocBytes", "time")

	firstSpill := -1
	for _, c := range cards {
		jsons := GenJSONs(c, c) // c distinct docs -> c distinct groups
		tmpDir, vars := glue.MakeVars("", "n1k1spill")
		op := opGroupCountG(opScan(jsons, 1))

		var m0, m1 runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&m0)
		t0 := time.Now()
		groups := runRows(t, op, vars)
		dt := time.Since(t0)
		runtime.ReadMemStats(&m1)

		slots, tmpBytes := tmpDirSpill(tmpDir)
		if slots > 0 && firstSpill < 0 {
			firstSpill = c
		}
		t.Logf("%-8d %-8d %-7v %-5d %-12d %-12d %v",
			c, groups, slots > 0, slots, tmpBytes, m1.TotalAlloc-m0.TotalAlloc,
			dt.Round(time.Millisecond))

		os.RemoveAll(tmpDir)
	}

	if firstSpill < 0 {
		t.Logf("no slots spill observed up to %d distinct keys", cards[len(cards)-1])
	} else {
		t.Logf("slots spill (Generation>0) first observed at ~%d distinct keys "+
			"(rhmap/store StartSize=5303)", firstSpill)
	}
}

// BenchmarkGroupBy measures GROUP BY g, COUNT(*) throughput at cardinalities
// below and above the spill point, to show that crossing it degrades gracefully
// (rhmap/store mmap spill) rather than catastrophically.
func BenchmarkGroupBy(b *testing.B) {
	for _, c := range []int{1000, 4000, 16000, 64000} {
		b.Run(fmt.Sprintf("distinct=%d", c), func(b *testing.B) {
			jsons := GenJSONs(c, c)
			vars, cleanup := newVars()
			defer cleanup()
			op := opGroupCountG(opScan(jsons, 1))
			benchRows(b, op, vars, c)
		})
	}
}
