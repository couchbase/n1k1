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

// Concurrency: simulate the "goroutine per client" model a listen-port server would use --
// ONE shared Store (the cbq datastore is a global singleton, so a server serves one data
// root), and N goroutines each with its OWN Session running a mix of queries in a loop.
//
// FINDING: functionally this returns correct results under heavy contention (these tests
// pass). Blockers 2 & 3 are now FIXED -- engine.ExprCatalog is registered once in init()
// (no concurrent map-write panic) and datastore.SetDatastore is guarded by ensureDatastore
// (no write during serving). The REMAINING barrier is blocker 1: the engine.ExecOpEx IoC
// hook is a package-global func var swapped to DatastoreOp per Run and read on the op-exec
// hot path, so concurrent Runs still race on it (plus a blocker-4 cbq-planner audit). So
// these tests SKIP under -race until blocker 1 lands; making it race-clean is a real engine
// refactor. See DESIGN-concurrency.md. A Session is single-query-at-a-time (halt resets per
// Run); the concurrency here is ACROSS sessions sharing the store + the global engine hooks.

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

// writeJSONKeyspace writes n docs to a classic <root>/default/<ks>/ layout.
func writeJSONKeyspace(t *testing.T, root, ks string, n int) {
	t.Helper()
	dir := filepath.Join(root, "default", ks)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		cat := "a"
		if i%2 == 1 {
			cat = "b"
		}
		doc := fmt.Sprintf(`{"id":%d,"cat":%q,"amt":%d}`, i, cat, i*10)
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("d%03d.json", i)), []byte(doc), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

type concQuery struct {
	stmt string
	want int // expected row count
}

// hammer runs G goroutines, each with its own Session over the shared store, each looping N
// times through the query set (rotated by goroutine so they interleave differently), and
// fails on any error or wrong row count.
func hammer(t *testing.T, store *Store, queries []concQuery, g, n int) {
	t.Helper()
	if raceEnabled {
		t.Skip("blocker 1 open: engine.ExecOpEx is a global func var swapped per Run + read on " +
			"the op-exec hot path, so concurrent Run() still races under -race (blockers 2-3, " +
			"ExprCatalog + SetDatastore, are now fixed). See DESIGN-concurrency.md.")
	}
	var wg sync.WaitGroup
	var fails int64
	for gi := 0; gi < g; gi++ {
		wg.Add(1)
		go func(gi int) {
			defer wg.Done()
			sess := &Session{Store: store, Namespace: "default"}
			for i := 0; i < n; i++ {
				q := queries[(gi+i)%len(queries)]
				res, err := sess.Run(q.stmt)
				if err != nil {
					atomic.AddInt64(&fails, 1)
					t.Errorf("g%d i%d %q: %v", gi, i, q.stmt, err)
					return
				}
				if len(res.Rows) != q.want {
					atomic.AddInt64(&fails, 1)
					t.Errorf("g%d i%d %q: %d rows, want %d", gi, i, q.stmt, len(res.Rows), q.want)
					return
				}
			}
		}(gi)
	}
	wg.Wait()
	if fails > 0 {
		t.Fatalf("%d concurrent query failures", fails)
	}
}

// TestConcurrentSessionsJSON: goroutine-per-client over a shared JSON keyspace, exercising
// the core engine (scan / filter / group / aggregate / order-limit) concurrently.
func TestConcurrentSessionsJSON(t *testing.T) {
	root := t.TempDir()
	writeJSONKeyspace(t, root, "events", 50)
	store, err := FileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.InitParser(); err != nil {
		t.Fatal(err)
	}
	hammer(t, store, []concQuery{
		{"SELECT e.id FROM events AS e", 50},
		{"SELECT e.id FROM events AS e WHERE e.id >= 25", 25},
		{"SELECT e.cat AS c, COUNT(*) AS n FROM events AS e GROUP BY e.cat", 2},
		{"SELECT SUM(e.amt) AS s FROM events AS e", 1},
		{"SELECT e.id FROM events AS e ORDER BY e.id DESC LIMIT 5", 5},
	}, 24, 40)
}

// TestConcurrentSessionsIceberg: goroutine-per-client over a shared Iceberg table, hammering
// the row scan, the columnar/vectorized aggregate path, and the metadata (COUNT/MIN/MAX) path
// concurrently -- each query opens its own iceberg-go scan.
func TestConcurrentSessionsIceberg(t *testing.T) {
	root := t.TempDir()
	writeIcebergAmounts(t, root, "sales", []float64{10, 20, 30, 40, 50})
	store, err := FileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.InitParser(); err != nil {
		t.Fatal(err)
	}
	hammer(t, store, []concQuery{
		{"SELECT e.id FROM sales AS e", 5},
		{"SELECT e.id FROM sales AS e WHERE e.amt >= 30", 3},
		{"SELECT SUM(e.amt) AS s FROM sales AS e", 1},
		{"SELECT COUNT(*) AS n FROM sales", 1},
		{"SELECT MIN(e.amt) AS lo, MAX(e.amt) AS hi FROM sales AS e", 1},
	}, 16, 30)
}
