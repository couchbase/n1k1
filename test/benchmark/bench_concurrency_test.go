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

// Concurrency characterization: how does n1k1 throughput scale as concurrent clients ramp
// up? This drives the FULL request path -- Session.Run (parse -> plan -> convert -> execute)
// -- the way a listen-port server's goroutine-per-connection would, over ONE shared Store
// (the cbq datastore is a process-global singleton; a server serves one data root). Each of
// G goroutines gets its OWN Session; the concurrency is ACROSS sessions.
//
// Read the "queries/s" metric across the gNN sub-benchmarks. FINDING (see DESIGN-concurrency.md):
// n1k1 does NOT scale near-linearly with cores -- throughput peaks early (roughly 2-2.5x around
// G=4 on a 12-core box) and then PLATEAUS/DECLINES as G grows. That holds even with a tiny
// keyspace (so it isn't scan I/O): it's contention on shared parse/plan state -- the cbq
// planner's process-global object pools (mutex-serialized), the global n1ql parser namespace,
// and GC pressure from the boxed plan path. n1k1's OWN per-query globals are already fixed
// (blockers 1-3); this benchmark tracks the remaining concurrency ceiling and will show the
// win when the cbq fork planner pools are made per-request (blocker 4a).
//
// NOTE: run WITHOUT -race. n1k1's own per-query globals are race-clean (see
// glue/concurrency_test.go + DESIGN-concurrency.md), but the cbq FORK planner still has a
// global object-pool race under concurrent plan-building (blocker 4a). It's benign enough in
// practice to characterize throughput, but -race will flag it (a fork patch is pending).

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/couchbase/n1k1/glue"
)

// concLevels is the client ramp: goroutine (== concurrent-client) counts.
var concLevels = []int{1, 2, 4, 8, 16, 32}

// concDocs is the shared keyspace size: small enough that parse+plan dominates per-query cost
// (so the curve reflects engine/plan concurrency, not file I/O), yet a real multi-doc keyspace.
const concDocs = 100

// concStmts is a mix of query shapes a client might send: a filtered scan, a grouped count,
// an ungrouped aggregate, and an order+limit. Parse+plan dominates per-query cost at this
// data size, so this mostly characterizes concurrent PLANNING + light execution.
var concStmts = []string{
	"SELECT e.id FROM events AS e WHERE e.n >= 100",
	"SELECT e.cat AS c, COUNT(*) AS k FROM events AS e GROUP BY e.cat",
	"SELECT SUM(e.n) AS s FROM events AS e",
	"SELECT e.id FROM events AS e ORDER BY e.n DESC LIMIT 10",
}

// benchConcStore writes nDocs JSON docs to a classic <root>/default/events/ layout and
// returns a shared, InitParser'd Store plus a cleanup. The docs are small so per-query cost
// is parse+plan+light-exec, not I/O.
func benchConcStore(tb testing.TB, nDocs int) (*glue.Store, func()) {
	tb.Helper()
	root, err := os.MkdirTemp("", "n1k1conc")
	if err != nil {
		tb.Fatal(err)
	}
	dir := filepath.Join(root, "default", "events")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		tb.Fatal(err)
	}
	for i := 0; i < nDocs; i++ {
		cat := []string{"a", "b", "c"}[i%3]
		doc := fmt.Sprintf(`{"id":%d,"cat":%q,"n":%d}`, i, cat, i%200)
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("d%05d.json", i)), []byte(doc), 0o644); err != nil {
			tb.Fatal(err)
		}
	}
	store, err := glue.FileStore(root)
	if err != nil {
		tb.Fatal(err)
	}
	if err := store.InitParser(); err != nil {
		tb.Fatal(err)
	}
	return store, func() { os.RemoveAll(root) }
}

// benchConcStoreSingleFile is the syscall-LIGHT counterpart to benchConcStore: the same nDocs
// live in ONE events.jsonl file, so each scan is a single file open (no per-doc opens, no dir
// walk) -- isolating how much of the concurrency ceiling was the file-per-doc layout.
func benchConcStoreSingleFile(tb testing.TB, nDocs int) (*glue.Store, func()) {
	tb.Helper()
	root, err := os.MkdirTemp("", "n1k1sf")
	if err != nil {
		tb.Fatal(err)
	}
	var b strings.Builder
	for i := 0; i < nDocs; i++ {
		cat := []string{"a", "b", "c"}[i%3]
		fmt.Fprintf(&b, `{"id":%d,"cat":%q,"n":%d}`+"\n", i, cat, i%200)
	}
	f := filepath.Join(root, "events.jsonl")
	if err := os.WriteFile(f, []byte(b.String()), 0o644); err != nil {
		tb.Fatal(err)
	}
	store, err := glue.FileStore(f) // single-file arg -> keyspace "events" (one open per scan).
	if err != nil {
		tb.Fatal(err)
	}
	if err := store.InitParser(); err != nil {
		tb.Fatal(err)
	}
	return store, func() { os.RemoveAll(root) }
}

// runConcurrent spreads b.N Session.Run calls across g goroutines (each its own Session) and
// reports throughput. queries/s = total queries / wall-clock; ns/op is the wall-clock per
// query amortized across all goroutines (so it drops as G scales, if it scales).
func runConcurrent(b *testing.B, store *glue.Store, stmts []string, g int) {
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
				if _, err := sess.Run(stmts[int(k)%len(stmts)]); err != nil {
					b.Errorf("Run: %v", err)
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

// BenchmarkConcurrentQueries ramps the concurrent-client count and reports queries/s at each
// level, over a shared store -- the goroutine-per-client scaling curve.
func BenchmarkConcurrentQueries(b *testing.B) {
	store, cleanup := benchConcStore(b, concDocs)
	defer cleanup()
	for _, g := range concLevels {
		b.Run(fmt.Sprintf("g%02d", g), func(b *testing.B) {
			runConcurrent(b, store, concStmts, g)
		})
	}
}

// BenchmarkConcurrentSingleFile is the syscall-light variant of BenchmarkConcurrentQueries: the
// SAME query mix over a single-file keyspace (one open per scan). Compare its queries/s ramp to
// the file-per-doc one to quantify how much headroom the data layout costs under concurrency.
func BenchmarkConcurrentSingleFile(b *testing.B) {
	store, cleanup := benchConcStoreSingleFile(b, concDocs)
	defer cleanup()
	for _, g := range concLevels {
		b.Run(fmt.Sprintf("g%02d", g), func(b *testing.B) {
			runConcurrent(b, store, concStmts, g)
		})
	}
}

// BenchmarkConcurrentByShape isolates each query shape's scaling (some are plan-heavy, some
// execution-heavy), at a fixed mid-range concurrency, so a regression can be attributed.
func BenchmarkConcurrentByShape(b *testing.B) {
	store, cleanup := benchConcStore(b, concDocs)
	defer cleanup()
	const g = 8
	for _, stmt := range concStmts {
		name := stmt[len("SELECT "):]
		if len(name) > 24 {
			name = name[:24]
		}
		b.Run(name, func(b *testing.B) {
			runConcurrent(b, store, []string{stmt}, g)
		})
	}
}

// BenchmarkConcurrentRunParallel is the idiomatic peak-parallel-throughput number: b.N
// queries spread across GOMAXPROCS goroutines (each its own Session), one query mix. ns/op is
// the amortized per-query latency at full parallelism.
func BenchmarkConcurrentRunParallel(b *testing.B) {
	store, cleanup := benchConcStore(b, concDocs)
	defer cleanup()
	b.ReportAllocs()
	var seq int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		sess := &glue.Session{Store: store, Namespace: "default"}
		for pb.Next() {
			k := atomic.AddInt64(&seq, 1)
			if _, err := sess.Run(concStmts[int(k)%len(concStmts)]); err != nil {
				b.Errorf("Run: %v", err)
				return
			}
		}
	})
}
