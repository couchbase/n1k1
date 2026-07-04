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
	"testing"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/engine"
)

// The Session live-stats path (CollectStats + OnStats) must fire during a query
// and produce a JSON snapshot with non-zero counters -- the data the Web Worker
// streams to the UI. Force frequent checkpoints so the tiny fixture triggers
// several OnStats calls.
func TestStatsSnapshotLiveDuringQuery(t *testing.T) {
	prev := engine.ScanYieldStatsEvery
	engine.ScanYieldStatsEvery = 1
	t.Cleanup(func() { engine.ScanYieldStatsEvery = prev })

	root := writeMemFixture(t, `"abv"`) // 5 beers docs
	sess, err := OpenSession(root, "default")
	if err != nil {
		t.Fatal(err)
	}

	var snapshots []string
	var fires int
	sess.CollectStats = true
	sess.OnStats = func(s *base.Stats) {
		fires++
		snapshots = append(snapshots, StatsSnapshotJSON(s))
	}

	if _, err := sess.Run("SELECT b.name FROM beers b"); err != nil {
		t.Fatal(err)
	}
	if fires == 0 {
		t.Fatal("OnStats never fired; live stats would show nothing")
	}

	// The final snapshot must be valid JSON with at least one op carrying a
	// positive counter (rows flowed through the scan).
	last := snapshots[len(snapshots)-1]
	var parsed struct {
		Ops []struct {
			Id    string           `json:"id"`
			Kind  string           `json:"kind"`
			Stats map[string]int64 `json:"stats"`
		} `json:"ops"`
	}
	if err := json.Unmarshal([]byte(last), &parsed); err != nil {
		t.Fatalf("snapshot is not valid JSON: %v\n%s", err, last)
	}
	if len(parsed.Ops) == 0 {
		t.Fatalf("snapshot has no ops: %s", last)
	}
	var maxCount int64
	for _, op := range parsed.Ops {
		for _, v := range op.Stats {
			if v > maxCount {
				maxCount = v
			}
		}
	}
	if maxCount <= 0 {
		t.Fatalf("no positive counter in snapshot: %s", last)
	}
}

func TestStatsSnapshotNil(t *testing.T) {
	if StatsSnapshotJSON(nil) != "{}" {
		t.Error("nil Stats should snapshot to {}")
	}
}
