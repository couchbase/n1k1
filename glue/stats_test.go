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
	"sync/atomic"
	"testing"

	"github.com/couchbase/n1k1/base"
)

// TestStatsRunningAggsUnionRace exercises the concurrency the earlier single-goroutine
// tests missed: a parallel UNION ALL whose EACH branch has a GROUP BY with cheap
// aggregates, with live stats ON and snapshots taken at every checkpoint. Each
// branch runs on its OWN actor goroutine sharing ONE *base.Stats, so the live
// running-aggregate registration + refresh + read must be goroutine-safe. Run under
// `go test -race` -- it must be clean.
func TestStatsRunningAggsUnionRace(t *testing.T) {
	forceLiveCheckpoints(t)

	root := writeMemFixture(t, `"abv"`)
	sess, err := OpenSession(root, "default")
	if err != nil {
		t.Fatal(err)
	}

	var fires int64
	sess.CollectStats = true
	// Read the live RUNNING-AGGREGATE partials the way a consumer does -- via the fenced
	// RunningAggsRange -- while the two branch actors concurrently register and refresh
	// their own running-aggregate slots. The reader runs on whichever actor's checkpoint
	// fired it, so it reads slots owned by the OTHER actor too; RunningAggsRange's lock
	// fences that against the owner's concurrent refresh. (We deliberately read only
	// running aggregates here, not the full StatsSnapshotJSON: the counter core it also reads
	// carries a SEPARATE, pre-existing, design-accepted advisory read race --
	// lock-free monotonic counters, DESIGN-stats.md Open Questions -- orthogonal to
	// the running-aggregate races this fix targets and unchanged by it.)
	sess.OnStats = func(s *base.Stats) {
		atomic.AddInt64(&fires, 1)
		s.RunningAggsRange(func(r *base.RunningAggRow) {
			// Touch the row's fields to force real reads the detector can catch.
			_ = r.Op
			for _, a := range r.Aggs {
				_ = len(a)
			}
		})
	}

	// Two GROUP BY branches, run concurrently by the UNION ALL Stage.
	q := `SELECT style, count(*) AS c, sum(abv) AS s, avg(abv) AS a FROM beers WHERE abv < 9 GROUP BY style
	      UNION ALL
	      SELECT style, count(*) AS c, sum(abv) AS s, avg(abv) AS a FROM beers WHERE abv >= 5 GROUP BY style`

	// Repeat to widen the race window (data races are probabilistic).
	for i := 0; i < 50; i++ {
		res, err := sess.Run(q)
		if err != nil {
			t.Fatal(err)
		}
		// UNION ALL of the two per-style groupings: 4 styles (<9) + 4 styles (>=5,
		// where "big"/Imperial abv 10 is its own style) -> the concrete fixture
		// yields a stable non-empty row set; just assert it produced rows.
		if len(res.Rows) == 0 {
			t.Fatalf("iter %d: union produced no rows", i)
		}
	}
	if fires == 0 {
		t.Fatal("OnStats never fired; the concurrent running-aggregate path was not exercised")
	}
}
