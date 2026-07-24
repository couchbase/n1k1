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

// PREPARE/EXECUTE variants of the concurrency ramp. The ad-hoc benchmark
// (bench_concurrency_test.go) is plan-bound: its ceiling is contention on the cbq planner's
// process-global object pools (blocker 4a). PREPARE builds + caches the converted plan ONCE;
// EXECUTE reuses it and skips planner.Build -- so a prepared workload should sidestep that
// contention and scale much better. These benchmarks quantify that, two ways:
//
//   Shared    -- ONE *glue.PreparedPlan (built once via PlanConvert) reused by every goroutine
//                via its own Session's PlanExec. The plan's per-run state lives in fresh Vars,
//                so the immutable plan is shared; nothing calls the planner in the timed loop.
//   PerClient -- each goroutine runs `PREPARE p AS <stmt>` once then loops `EXECUTE p` (the
//                realistic per-connection model: a client prepares once, executes many). The
//                first EXECUTE per Session builds+caches that Session's plan; the rest reuse it.
//
// A SINGLE prepared statement driven through ONE shared Session is NOT concurrent-safe (a
// Session is single-query-at-a-time: preparedStmt.uses++, lazy compiled-plan init, and the
// per-Run halt/args are all shared mutable state). So both variants keep a Session per
// goroutine and share only the immutable plan (or nothing). Run without -race (the planner
// warm-up in PerClient still touches blocker 4a); the Shared variant is verified race-clean
// by TestConcurrentSharedPreparedPlanRace below.

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/couchbase/n1k1/glue"
)

// constUnnestStmt is a SQL++ query that touches NO files: a literal array of one object holding
// an n-element nested `items` array, UNNEST'd and aggregated. cbq folds the literal to a value
// scan, so the concurrency curve here reflects the PURE ENGINE (unnest + aggregate), with zero
// datastore-scan syscalls -- the control for "is the file-per-doc scan I/O the ceiling?".
func constUnnestStmt(n int) string {
	var b strings.Builder
	b.WriteString(`SELECT COUNT(*) AS c, SUM(i) AS s FROM [{"items":[`)
	for k := 0; k < n; k++ {
		if k > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.Itoa(k))
	}
	b.WriteString(`]}] AS d UNNEST d.items AS i WHERE i >= 0`)
	return b.String()
}

// BenchmarkConcurrentUnnestConst -- ad-hoc, file-less (literal UNNEST). Per-query cost is
// parse+plan+exec with no scan syscalls. Compare its ramp to BenchmarkConcurrentQueries (which
// is syscall-bound): if THIS scales better, file I/O was the ceiling.
func BenchmarkConcurrentUnnestConst(b *testing.B) {
	store, cleanup := benchConcStore(b, 1) // store for the planner; the query reads no file.
	defer cleanup()
	stmt := constUnnestStmt(500)
	for _, g := range concLevels {
		b.Run(fmt.Sprintf("g%02d", g), func(b *testing.B) {
			runConcurrent(b, store, []string{stmt}, g)
		})
	}
}

// BenchmarkConcurrentUnnestConstPrepared -- the same file-less query, PREPARE'd (a shared plan):
// no parse, no plan, no files in the loop -- the purest measure of engine EXECUTE concurrency.
func BenchmarkConcurrentUnnestConstPrepared(b *testing.B) {
	store, cleanup := benchConcStore(b, 1)
	defer cleanup()
	pps := buildSharedPlans(b, store, []string{constUnnestStmt(500)})
	for _, g := range concLevels {
		b.Run(fmt.Sprintf("g%02d", g), func(b *testing.B) {
			runConcurrentShared(b, store, pps, g)
		})
	}
}

// buildSharedPlans converts each stmt to a reusable *glue.PreparedPlan ONCE (no args baked
// in), the public PREPARE-once reuse path: parse -> PlanStatementQP -> PlanConvert.
func buildSharedPlans(tb testing.TB, store *glue.Store, stmts []string) []*glue.PreparedPlan {
	tb.Helper()
	pps := make([]*glue.PreparedPlan, len(stmts))
	for i, s := range stmts {
		parsed, err := glue.ParseStatement(s, "default", true)
		if err != nil {
			tb.Fatalf("parse %q: %v", s, err)
		}
		qp, err := store.PlanStatementQP(parsed, "default", nil, nil)
		if err != nil {
			tb.Fatalf("plan %q: %v", s, err)
		}
		pp, err := glue.PlanConvert(qp)
		if err != nil {
			tb.Fatalf("convert %q: %v", s, err)
		}
		pps[i] = pp
	}
	return pps
}

// runConcurrentShared executes b.N PlanExec calls (over the shared plans) across g goroutines.
func runConcurrentShared(b *testing.B, store *glue.Store, pps []*glue.PreparedPlan, g int) {
	b.ReportAllocs()
	var seq int64
	b.ResetTimer()
	var wg sync.WaitGroup
	for gi := 0; gi < g; gi++ {
		n := b.N / g
		if gi < b.N%g {
			n++
		}
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			sess := &glue.Session{Store: store, Namespace: "default"}
			for i := 0; i < n; i++ {
				k := atomic.AddInt64(&seq, 1)
				if _, err := sess.PlanExec(pps[int(k)%len(pps)], nil, nil); err != nil {
					b.Errorf("PlanExec: %v", err)
					return
				}
			}
		}(n)
	}
	wg.Wait()
	b.StopTimer()
	if secs := b.Elapsed().Seconds(); secs > 0 {
		b.ReportMetric(float64(b.N)/secs, "queries/s")
	}
}

// runConcurrentPerClient has each goroutine PREPARE the stmts once (as p0..pN) then loop
// EXECUTE over them -- the per-connection prepare-once/execute-many model.
func runConcurrentPerClient(b *testing.B, store *glue.Store, stmts []string, g int) {
	b.ReportAllocs()
	var seq int64
	b.ResetTimer()
	var wg sync.WaitGroup
	for gi := 0; gi < g; gi++ {
		n := b.N / g
		if gi < b.N%g {
			n++
		}
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			sess := &glue.Session{Store: store, Namespace: "default"}
			for i, s := range stmts { // PREPARE once per client.
				if _, err := sess.Run(fmt.Sprintf("PREPARE p%d AS %s", i, s)); err != nil {
					b.Errorf("PREPARE: %v", err)
					return
				}
			}
			for i := 0; i < n; i++ {
				k := atomic.AddInt64(&seq, 1)
				if _, err := sess.Run(fmt.Sprintf("EXECUTE p%d", int(k)%len(stmts))); err != nil {
					b.Errorf("EXECUTE: %v", err)
					return
				}
			}
		}(n)
	}
	wg.Wait()
	b.StopTimer()
	if secs := b.Elapsed().Seconds(); secs > 0 {
		b.ReportMetric(float64(b.N)/secs, "queries/s")
	}
}

// BenchmarkConcurrentPreparedShared -- one shared plan set reused by all clients (PlanExec,
// no planner in the loop). Compare its queries/s ramp to BenchmarkConcurrentQueries (ad-hoc).
func BenchmarkConcurrentPreparedShared(b *testing.B) {
	store, cleanup := benchConcStore(b, concDocs)
	defer cleanup()
	pps := buildSharedPlans(b, store, concStmts)
	for _, g := range concLevels {
		b.Run(fmt.Sprintf("g%02d", g), func(b *testing.B) {
			runConcurrentShared(b, store, pps, g)
		})
	}
}

// BenchmarkConcurrentPreparedPerClient -- each client PREPAREs once, then EXECUTEs.
func BenchmarkConcurrentPreparedPerClient(b *testing.B) {
	store, cleanup := benchConcStore(b, concDocs)
	defer cleanup()
	for _, g := range concLevels {
		b.Run(fmt.Sprintf("g%02d", g), func(b *testing.B) {
			runConcurrentPerClient(b, store, concStmts, g)
		})
	}
}

// TestConcurrentSharedPreparedPlanRace verifies that ONE immutable *glue.PreparedPlan can be
// executed by many goroutines at once with no data race (the claim that lets the Shared
// benchmark share a plan). Meaningful under `go test -race`.
func TestConcurrentSharedPreparedPlanRace(t *testing.T) {
	store, cleanup := benchConcStore(t, 40)
	defer cleanup()
	pps := buildSharedPlans(t, store, concStmts)
	var wg sync.WaitGroup
	var seq int64
	for gi := 0; gi < 16; gi++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sess := &glue.Session{Store: store, Namespace: "default"}
			for i := 0; i < 25; i++ {
				k := atomic.AddInt64(&seq, 1)
				if _, err := sess.PlanExec(pps[int(k)%len(pps)], nil, nil); err != nil {
					t.Errorf("PlanExec: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
}
