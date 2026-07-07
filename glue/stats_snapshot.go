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

	"github.com/couchbase/n1k1/base"
)

// StatsSnapshotJSON renders a live per-operator counter snapshot as JSON, for
// progress display. The WASM demo's Web Worker calls this from the Session
// OnStats callback (which the engine invokes periodically during execution --
// every ScanYieldStatsEvery rows) and posts the result to the UI thread, so a
// long query shows live progress while the worker is busy. See web/wasm/worker.js.
//
// Shape: {"ops":[{"id","kind","stats":{<name>:<count>,...}}],"aggs":[{"op","key",
// "vals":{<aggName>:<partialJSON>,...}}]}. Counters are monotonic per field but
// may show mild cross-field skew mid-run (a re-run inner op resets) -- fine for
// progress. The optional "aggs" array carries in-flight partial aggregate values
// (COUNT/SUM/AVG/MIN/MAX climbing toward their finals; see DESIGN-stats.md "Live
// aggregates"), present only when an OpGroup registered a preview and a checkpoint
// filled it. Returns "{}" for a nil Stats.
func StatsSnapshotJSON(s *base.Stats) string {
	if s == nil {
		return "{}"
	}
	type opView struct {
		Id    string           `json:"id"`
		Kind  string           `json:"kind"`
		Stats map[string]int64 `json:"stats"`
	}
	type aggView struct {
		Op   string            `json:"op"`
		Key  []string          `json:"key,omitempty"`
		Vals map[string]string `json:"vals"`
	}
	ops := make([]opView, 0, len(s.Ops))
	for _, op := range s.Ops {
		m := make(map[string]int64, len(op.Names))
		for i, name := range op.Names {
			idx := op.Base + i
			if idx >= 0 && idx < len(s.Counters) {
				m[name] = s.Counters[idx]
			}
		}
		ops = append(ops, opView{Id: op.Id, Kind: op.Kind, Stats: m})
	}

	// RangePreview holds the checkpoint lock while we read, so a concurrently
	// refreshing actor (a GROUP BY in another UNION ALL branch) can't tear the read.
	var aggs []aggView
	s.RangePreview(func(r *base.PreviewRow) {
		vals := make(map[string]string, len(r.Names))
		for i, name := range r.Names {
			if i < len(r.Aggs) {
				vals[name] = string(r.Aggs[i]) // partial as JSON text (Agg.Result bytes)
			}
		}
		var key []string
		for _, k := range r.Key {
			key = append(key, string(k))
		}
		aggs = append(aggs, aggView{Op: r.Op, Key: key, Vals: vals})
	})

	b, _ := json.Marshal(struct {
		Ops  []opView  `json:"ops"`
		Aggs []aggView `json:"aggs,omitempty"`
	}{ops, aggs})
	return string(b)
}
