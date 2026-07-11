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

package base

import "sync/atomic"

// MergeStats is a request's shared, race-safe counter set for the sorted-merge ops
// (engine/op_merge_scan.go + op_merge_join.go). It replaces what used to be process-global
// counters: a streaming merge spins up per-actor goroutines, each on a Ctx CLONE that
// shares this one struct by pointer (Ctx.Clone does a shallow struct copy, exactly like
// Stats/Pipe), so they can bump it concurrently without a data race (`go test -race`).
// Every field is atomic; read them only after the request has finished. A nil *MergeStats
// means "stats off" -- all the recorder methods are nil-receiver-safe, so call sites need
// no guard.
type MergeStats struct {
	JoinCount        atomic.Int64 // merge-joins executed.
	JoinSpillCount   atomic.Int64 // merge-joins whose build spilled past the budget.
	BuildRows        atomic.Int64 // total build (right-side) rows materialized.
	BuildBytes       atomic.Int64 // total build row-payload bytes seen.
	BuildBytesPeak   atomic.Int64 // largest single build's payload bytes.
	NoKeySkipped     atomic.Int64 // rows dropped for a missing/non-int64 sort key.
	ScanStreamed     atomic.Int64 // merge-scans run via the streaming (bounded) path.
	ScanMaterialized atomic.Int64 // merge-scans that materialized their children (K-way).
}

// RecordBuild folds in one merge-join build's size, tracking the running peak.
func (m *MergeStats) RecordBuild(rows, bytes int64, spilled bool) {
	if m == nil {
		return
	}
	m.JoinCount.Add(1)
	m.BuildRows.Add(rows)
	m.BuildBytes.Add(bytes)
	if spilled {
		m.JoinSpillCount.Add(1)
	}
	for { // atomic max.
		cur := m.BuildBytesPeak.Load()
		if bytes <= cur || m.BuildBytesPeak.CompareAndSwap(cur, bytes) {
			break
		}
	}
}

// AddNoKeySkipped counts a merge row dropped for lacking a sort key.
func (m *MergeStats) AddNoKeySkipped(n int64) {
	if m != nil {
		m.NoKeySkipped.Add(n)
	}
}

// RecordScan counts one merge-scan by whether it streamed or materialized its input.
func (m *MergeStats) RecordScan(streamed bool) {
	if m == nil {
		return
	}
	if streamed {
		m.ScanStreamed.Add(1)
	} else {
		m.ScanMaterialized.Add(1)
	}
}
