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
	"strconv"
	"testing"

	"github.com/couchbase/n1k1/base"
	"github.com/couchbase/n1k1/engine"

	"github.com/couchbase/rhmap/store"
)

// snapAgg is the JSON shape of one live-aggregate running-aggregate row (see
// StatsSnapshotJSON's "aggs" array).
type snapAgg struct {
	Op   string            `json:"op"`
	Key  []string          `json:"key"`
	Vals map[string]string `json:"vals"`
}

type snapParsed struct {
	Ops []struct {
		Id    string           `json:"id"`
		Kind  string           `json:"kind"`
		Stats map[string]int64 `json:"stats"`
	} `json:"ops"`
	Aggs []snapAgg `json:"aggs"`
}

func parseSnap(t *testing.T, s string) snapParsed {
	t.Helper()
	var p snapParsed
	if err := json.Unmarshal([]byte(s), &p); err != nil {
		t.Fatalf("snapshot is not valid JSON: %v\n%s", err, s)
	}
	return p
}

// forceLiveCheckpoints makes both the engine and datastore scans checkpoint every
// row, so a tiny fixture yields many live snapshots.
func forceLiveCheckpoints(t *testing.T) {
	t.Helper()
	pe := engine.ScanYieldStatsEvery
	pd := DatastoreScanYieldStatsEvery
	engine.ScanYieldStatsEvery = 1
	DatastoreScanYieldStatsEvery = 1
	t.Cleanup(func() {
		engine.ScanYieldStatsEvery = pe
		DatastoreScanYieldStatsEvery = pd
	})
}

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
	// A Stats with no running-aggregate slots must snapshot cleanly (no "aggs"), and
	// RangeRunningAggs on it is a safe no-op.
	s := &base.Stats{}
	s.RangeRunningAggs(func(*base.RunningAggRow) { t.Error("no rows expected") })
	if js := StatsSnapshotJSON(s); js != `{"ops":[]}` {
		t.Errorf("empty Stats snapshot = %q, want no aggs", js)
	}
}

// TestStatsSnapshotLiveAggregates proves the "Live aggregates" design end to end
// for an ungrouped aggregate: the partials (COUNT/SUM/AVG/MIN/MAX) climb
// monotonically toward their finals, and the LAST live snapshot is byte-identical
// to the finalized result -- because the running-aggregate decode and the final result use
// the same base.Agg.Result call.
func TestStatsSnapshotLiveAggregates(t *testing.T) {
	forceLiveCheckpoints(t)

	root := writeMemFixture(t, `"abv"`) // 5 beers, abv 4.0/5.5/7.0/8.2/10.0
	sess, err := OpenSession(root, "default")
	if err != nil {
		t.Fatal(err)
	}

	var snaps []snapParsed
	sess.CollectStats = true
	sess.OnStats = func(s *base.Stats) { snaps = append(snaps, parseSnap(t, StatsSnapshotJSON(s))) }

	res, err := sess.Run(`SELECT count(*) AS count, sum(abv) AS sum, avg(abv) AS avg,
		min(abv) AS min, max(abv) AS max FROM beers`)
	if err != nil {
		t.Fatal(err)
	}

	// Gather the running-aggregate rows that actually carried aggregates (early checkpoints,
	// before the first row is folded, have an empty group -> no aggs).
	var withAggs []map[string]string
	for _, sp := range snaps {
		if len(sp.Aggs) == 1 {
			withAggs = append(withAggs, sp.Aggs[0].Vals)
		}
	}
	if len(withAggs) < 3 {
		t.Fatalf("expected several live aggregate snapshots, got %d", len(withAggs))
	}

	// Monotonicity: COUNT and SUM (abv all positive) never decrease, MIN never
	// increases, MAX never decreases -- a partial climbing toward its final.
	num := func(m map[string]string, k string) float64 {
		f, err := strconv.ParseFloat(m[k], 64)
		if err != nil {
			t.Fatalf("bad %s=%q: %v", k, m[k], err)
		}
		return f
	}
	for i := 1; i < len(withAggs); i++ {
		prev, cur := withAggs[i-1], withAggs[i]
		if num(cur, "count") < num(prev, "count") {
			t.Errorf("count went backwards: %v -> %v", prev["count"], cur["count"])
		}
		if num(cur, "sum") < num(prev, "sum") {
			t.Errorf("sum went backwards: %v -> %v", prev["sum"], cur["sum"])
		}
		if num(cur, "min") > num(prev, "min") {
			t.Errorf("min rose: %v -> %v", prev["min"], cur["min"])
		}
		if num(cur, "max") < num(prev, "max") {
			t.Errorf("max fell: %v -> %v", prev["max"], cur["max"])
		}
	}

	// Convergence: the LAST live partial equals the finalized result byte-for-byte.
	// The final result row is JSON like {"count":5,"sum":34.7,...}; the running-aggregate
	// carries each value as the same Agg.Result text ("5","34.7",...).
	last := withAggs[len(withAggs)-1]
	var final map[string]json.RawMessage
	if err := json.Unmarshal(res.Rows[0], &final); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"count", "sum", "avg", "min", "max"} {
		if got, want := last[k], string(final[k]); got != want {
			t.Errorf("convergence: last live %s=%q != final %q", k, got, want)
		}
	}
}

// TestStatsSnapshotLiveAggregatesGrouped checks the bounded per-group sample: the
// last live snapshot's per-group partials equal the finalized GROUP BY result.
func TestStatsSnapshotLiveAggregatesGrouped(t *testing.T) {
	forceLiveCheckpoints(t)

	root := writeMemFixture(t, `"abv"`)
	sess, err := OpenSession(root, "default")
	if err != nil {
		t.Fatal(err)
	}

	var last snapParsed
	var fired int
	sess.CollectStats = true
	sess.OnStats = func(s *base.Stats) { fired++; last = parseSnap(t, StatsSnapshotJSON(s)) }

	res, err := sess.Run(`SELECT style, count(*) AS count, sum(abv) AS sum,
		min(abv) AS min, max(abv) AS max FROM beers GROUP BY style`)
	if err != nil {
		t.Fatal(err)
	}
	if fired == 0 {
		t.Fatal("OnStats never fired")
	}

	// Index the final rows by style.
	finalByStyle := map[string]map[string]json.RawMessage{}
	for _, raw := range res.Rows {
		var m map[string]json.RawMessage
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatal(err)
		}
		var style string
		if err := json.Unmarshal(m["style"], &style); err != nil {
			t.Fatal(err)
		}
		finalByStyle[style] = m
	}

	if len(last.Aggs) != len(finalByStyle) {
		t.Fatalf("last snapshot has %d groups, final has %d", len(last.Aggs), len(finalByStyle))
	}
	for _, ag := range last.Aggs {
		if len(ag.Key) != 1 {
			t.Fatalf("expected 1 group key, got %v", ag.Key)
		}
		var style string
		if err := json.Unmarshal([]byte(ag.Key[0]), &style); err != nil {
			t.Fatalf("bad key %q: %v", ag.Key[0], err)
		}
		fm, ok := finalByStyle[style]
		if !ok {
			t.Fatalf("running-aggregate group %q not in final result", style)
		}
		for _, k := range []string{"count", "sum", "min", "max"} {
			if got, want := ag.Vals[k], string(fm[k]); got != want {
				t.Errorf("group %q: live %s=%q != final %q", style, k, got, want)
			}
		}
	}
}

// TestStatsSnapshotNoRunningAggsForCostlyAgg checks the carve-out: a group containing a
// non-cheap aggregate (ARRAY_AGG / COUNT(DISTINCT)) registers no live-value running-aggregate
// (progress-only), so snapshots carry counters but no "aggs" payload.
func TestStatsSnapshotNoRunningAggsForCostlyAgg(t *testing.T) {
	forceLiveCheckpoints(t)

	root := writeMemFixture(t, `"abv"`)
	sess, err := OpenSession(root, "default")
	if err != nil {
		t.Fatal(err)
	}

	for _, q := range []string{
		`SELECT array_agg(style) AS a FROM beers`,
		`SELECT count(DISTINCT style) AS d FROM beers`,
		`SELECT sum(abv) AS s, array_agg(style) AS a FROM beers`, // mixed cheap+costly -> still off
	} {
		var sawAggs bool
		sess.CollectStats = true
		sess.OnStats = func(s *base.Stats) {
			if len(parseSnap(t, StatsSnapshotJSON(s)).Aggs) > 0 {
				sawAggs = true
			}
		}
		if _, err := sess.Run(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		if sawAggs {
			t.Errorf("%s: expected progress-only (no live aggs), but a running-aggregate appeared", q)
		}
	}
}

// buildGroupVal builds one group's accumulator bytes for the given aggregates over
// the given numeric rows, mirroring OpGroup's Init-then-Update thread.
func buildGroupVal(t testing.TB, vars *base.Vars, names []string, rows []string) []byte {
	vc := vars.Ctx.ValComparer
	var val []byte
	for _, n := range names {
		val = base.Aggs[base.AggCatalog[n]].Init(vars, val)
	}
	for _, row := range rows {
		var valNew []byte
		rest := val
		for _, n := range names {
			valNew, rest, _ = base.Aggs[base.AggCatalog[n]].Update(vars, base.Val(row), valNew, rest, vc)
		}
		val = valNew
	}
	return val
}

// BenchmarkLiveAggSnapshot is the zero-allocation proof for the live-aggregate
// checkpoint decode: repeatedly snapshotting a group's fixed-width accumulators
// (COUNT/SUM/AVG/MIN/MAX) through base.GroupRunningAggs into a REUSED
// base.RunningAggs must be 0 allocs/op once warmed up. (The per-row accumulate hot path
// is untouched by this feature, so this checkpoint decode is the only new work that
// could allocate.)
func BenchmarkLiveAggSnapshot(b *testing.B) {
	vars := &base.Vars{Ctx: &base.Ctx{ValComparer: base.NewValComparer()}}
	names := []string{"count", "sum", "avg", "min", "max"}

	sf, err := store.CreateRHStoreFile(b.TempDir()+"/m", store.DefaultRHStoreFileOptions)
	if err != nil {
		b.Fatal(err)
	}
	set := &sf.RHStore

	val := buildGroupVal(b, vars, names, []string{"4.0", "5.5", "7.0", "8.2", "10.0"})
	key, err := base.ValsEncodeCanonical(nil, nil, vars.Ctx.ValComparer) // ungrouped: one constant key
	if err != nil {
		b.Fatal(err)
	}
	if _, err := set.Set(store.Key(key), store.Val(val)); err != nil {
		b.Fatal(err)
	}

	// Drive the exact production path: a Stats with one per-op running-aggregate slot, an
	// actor Ctx with one registered refresher, re-snapshotted via
	// Ctx.RefreshRunningAggs (which takes the checkpoint lock, resets, and re-fills the
	// reused per-op buffer) at each checkpoint.
	stats := &base.Stats{RunningAggs: make([]base.RunningAggs, 1)}
	vars.Ctx.Stats = stats
	vars.Ctx.RegisterRunningAgg(0, func(dst *base.RunningAggs) {
		base.GroupRunningAggs(dst, "0", set, names, vars)
	})
	vars.Ctx.RefreshRunningAggs() // warm up the reused buffers

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vars.Ctx.RefreshRunningAggs()
	}
	b.StopTimer()

	// Sanity: the reused snapshot still decodes correctly (one group, count == 5).
	rows := stats.RunningAggs[0].Rows()
	if len(rows) != 1 || string(rows[0].Aggs[0]) != "5" {
		b.Fatalf("unexpected snapshot: %+v", rows)
	}
}

// BenchmarkAggQueryStatsOnVsOff reports allocs/op for the same aggregate query with
// live stats+running-aggregate OFF vs ON, forcing a checkpoint every row. The per-row hot
// path (the accumulate loop) is unchanged by this feature and the checkpoint
// decode is allocation-free (see BenchmarkLiveAggSnapshot), so enabling stats must
// not materially inflate allocations. The ON callback reads the live running-aggregate but
// does NOT serialize it -- StatsSnapshotJSON's map/JSON building is a separate,
// off-hot-path rendering concern (throttled to ~10 Hz in production), so leaving it
// out isolates the cost of counting + RefreshRunningAggs itself. Compare with -benchmem.
func BenchmarkAggQueryStatsOnVsOff(b *testing.B) {
	root := writeMemFixture(b, `"abv"`)
	stmt := `SELECT count(*) AS c, sum(abv) AS s, avg(abv) AS a, min(abv) AS mn, max(abv) AS mx FROM beers`

	run := func(b *testing.B, collect bool) {
		pe, pd := engine.ScanYieldStatsEvery, DatastoreScanYieldStatsEvery
		engine.ScanYieldStatsEvery, DatastoreScanYieldStatsEvery = 1, 1
		defer func() { engine.ScanYieldStatsEvery, DatastoreScanYieldStatsEvery = pe, pd }()

		sess, err := OpenSession(root, "default")
		if err != nil {
			b.Fatal(err)
		}
		if collect {
			sess.CollectStats = true
			// Read the live partials (as a real consumer would), without the JSON
			// rendering, so the measurement reflects counting + RefreshRunningAggs only.
			sess.OnStats = func(s *base.Stats) {
				s.RangeRunningAggs(func(r *base.RunningAggRow) { _ = r.Aggs })
			}
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := sess.Run(stmt); err != nil {
				b.Fatal(err)
			}
		}
	}

	b.Run("off", func(b *testing.B) { run(b, false) })
	b.Run("on", func(b *testing.B) { run(b, true) })
}
