# n1k1 benchmarks — design

DESIGN.md claims a pile of performance techniques (garbage avoidance, `[]byte`
registers, push-based fusion, static-param expr optimization, rhmap spilling,
max-heap ORDER BY, canonical-JSON reuse, …). **Do they actually pay off?** This
is the plan to measure that, on local data, across latency / throughput / memory
— and, later, for *compiled* n1k1 queries and against couchbase/query.

------------------------------------------------------------------------
## 1. What we're validating (the DESIGN.md claims)

Grouped by where they live, so each maps to a benchmark:

- **Per-row engine cost** — push-based codepaths, register (positional `Vals`)
  access vs map lookups, `YieldErr` push error handling.
- **Garbage avoidance** — `[]byte`/`[][]byte` recycling, no boxing, no
  `map[string]interface{}`, jsonparser-not-Unmarshal. The headline metric is
  **allocs/op that stays ~flat as the row count grows** (steady-state reuse).
- **Static-param expr optimization** — `sales < 1000` evaluates `1000` once,
  picks a typed codepath. (Already visible: `ExprEq` 13 allocs/op vs `ExprStr`
  4042 over 1000 docs.)
- **Scale / spilling** — rhmap + `rhmap/store` spill to temp files for GROUP BY
  / DISTINCT / hash-join / INTERSECT / EXCEPT; max-heap spills for ORDER BY.
  Claim: process datasets bigger than RAM without OOM, at graceful throughput.
- **Canonical JSON** — `ValComparer.CanonicalJSON` into reused buffers (group/
  distinct keys, number/object normalization).
- **Compilation (Futamura / operator fusion)** — generated Go fuses operators
  and lifts vars. The in-process head-to-head: **interpreted vs compiled**.

------------------------------------------------------------------------
## 2. Strategy & phasing

A true in-process race against couchbase/query's executor is **not possible** in
this module: the pure-Go decouple dropped `query/execution` (it pulls cgo/cbft),
and it's not in n1k1's dependency graph. So:

- **Phase 1 (now) — intrinsic validation, in-process, pure-Go.** Measure each
  claim on its own terms (allocs/op flat, ns/row, throughput at scale, spill
  behavior). This answers "do the techniques work?" without an external
  yardstick. *This is the bulk of the value and the only part with no
  feasibility risk.*
- **Phase 2 (deferred) — n1k1 interpreted vs compiled.** The novel head-to-head
  that validates fusion/lifting. In-process, pure-Go, builds on Phase 1 by
  running the same query both ways (see §7).
- **Phase 3 (BLOCKED in a stock env) — vs couchbase/query, as an absolute
  baseline.** Investigated 2026-06; *not* runnable here without a full Couchbase
  Server build/runtime. Three mechanisms, all blocked (see §10). Phase 1/2 stand
  alone as the perf story; Phase 3 is recorded for a future run on an equipped
  box.

------------------------------------------------------------------------
## 3. Metrics & dimensions

Standard Go `testing.B`, so results compose with `benchstat`.

- **Latency** — `ns/op` (per full query run) and a derived **ns/row**.
- **Throughput** — `b.ReportMetric(rows/sec, "rows/s")`, rows = `nDocs * b.N`.
- **Memory** — `b.ReportAllocs()` → `allocs/op` + `B/op` (the garbage-avoidance
  signal). For macro runs, sample `runtime.MemStats` (HeapAlloc peak, NumGC,
  PauseTotalNs) around the run, and report temp-file bytes when spilling.
- **Scale** — sweep `nDocs` = 1, 1K, 100K, 1M. The interesting plot is *how each
  metric grows with N* (e.g. allocs/op flat = recycling works; throughput holds
  until the spill threshold, then degrades gracefully not catastrophically).

Each benchmark isolates **execution**: parse+plan once, outside the `b.N` loop;
only `engine.ExecOp` runs inside it, with a **no-op yield** (count rows, discard
bytes) so we measure the engine, not output formatting.

------------------------------------------------------------------------
## 4. Claim → benchmark mapping

| Claim | Primary metric | Benchmark |
|---|---|---|
| garbage avoidance / `[]byte` / no boxing | allocs/op flat vs N | `BenchmarkScan*`, `BenchmarkFilterProject*` across scales |
| push-based / register access | ns/row | scan → filter → project pipeline |
| static-param expr optimization | ns/op, allocs/op | `ExprEq` vs `ExprStr` (have; formalize) |
| canonical JSON reuse | allocs/op | `base.BenchmarkCanonicalJSON` (have) |
| jsonparser vs Unmarshal | ns/op, allocs | `base.BenchmarkParse` (have) + field-access micro |
| GROUP BY / DISTINCT + spill | rows/s, MemStats, temp bytes | `BenchmarkGroupBy*`, `BenchmarkDistinct*` at rising cardinality |
| hash-join + spill | rows/s, mem | `BenchmarkJoinHash*` |
| max-heap ORDER BY (no final sort) | ns/op, allocs | `BenchmarkOrderLimit*` |
| INTERSECT/EXCEPT reuse hash-join | rows/s | `BenchmarkSetOps*` |
| compilation / fusion | interp vs compiled ns/op | Phase 2 (§7) |

------------------------------------------------------------------------
## 5. Harness & data

- **Synthetic generator** — produce N docs of a configurable shape (scalars,
  nested object, an array) for clean scaling to 1M. The cheapest scan path is
  `jsonsData` (one doc replicated N times, in-memory) — isolates engine cost
  with no datastore I/O; the existing `BenchmarkInterp*` already use it.
- **Realistic shapes** — also run a few benches via `glue` over the vendored
  suite corpus (`test/suite/json`) using `FileStore` + `DatastoreOp`, so joins/
  fetches exercise the real datastore path. Lower N, representative shapes.
- **Plan once, execute many** — for glue-driven benches, build the `base.Op`
  tree once; reuse it across `b.N`, resetting `Vars` / recycling spill state per
  iteration.
- **No-op yield** — `yieldVals` increments a counter and returns; `yieldErr`
  fails the bench. Confirms row count without measuring marshalling.

------------------------------------------------------------------------
## 6. Layout

```
test/benchmark/            (//go:build n1ql)
  gen.go                   synthetic doc generator (shape + count knobs)
  harness.go               plan-once/execute-many helpers, no-op yield, rows/s metric
  bench_scan_test.go       scan, filter, project (per-row cost, garbage)
  bench_expr_test.go       static-param vs interpreted expr (migrate ExprEq/ExprStr)
  bench_group_test.go      GROUP BY / DISTINCT + spill at scale
  bench_join_test.go       nested-loop + hash join + spill
  bench_order_test.go      ORDER BY / OFFSET / LIMIT max-heap
  bench_setops_test.go     INTERSECT / EXCEPT
  README.md                claim→bench map + how to run + benchstat tips
```

Make targets: `make bench` (run all, `-benchmem`), `make bench-mem` (alloc
focus), reuse the existing `benchmark-expr-eq` flow. Keep the per-claim
micro-benches in `base/` where they already live.

------------------------------------------------------------------------
## 7. Compiled-query benchmarking (Phase 2 detail)

The compiler emits Go for a query (today into `test/tmp` via the differential
harness). To benchmark compiled execution: generate the Go for a fixed set of
benchmark queries, build it into the `test/benchmark` package (or a generated
sibling), and run the *same* query interpreted vs compiled under identical data/
scale. Expected wins: fewer function calls (fusion), fewer allocs (lifted reuse
buffers). This reuses the Phase 1 generator, scales, and metrics — only the
"run" step differs (compiled func vs `engine.ExecOp`).

------------------------------------------------------------------------
## 8. What already exists (build on, don't duplicate)

- `test/n1k1_interp_test.go`: `BenchmarkInterpExprEq/Str_{1,1000,100000}Docs`,
  `BenchmarkInterpGroupBy_{1,100,10000}Docs` — the seed for bench_expr/bench_group.
- `base/`: `BenchmarkCanonicalJSON`, `BenchmarkValCompare`, `BenchmarkParse`,
  `BenchmarkEncodeAsString`; `test/BenchmarkBoxing`, `test/BenchmarkValCompare*`.
- `make benchmark-expr-eq` already wires `-tags n1ql` + `CGO_ENABLED=0` + benchmem.

------------------------------------------------------------------------
## 9. Open decisions

1. **Generator shape** — one canonical "contact"-like doc (mirror the corpus) vs
   a few shapes (wide/narrow/deeply-nested) to stress different field-access
   patterns. Lean: start with one corpus-like shape, add variants as needed.
2. **Scale ceiling** — 1M docs for in-memory scan benches is fine; spilling
   benches need a cardinality high enough to cross the rhmap/store threshold —
   find and document that threshold as its own result.
3. **Phase 3 trigger** — defer cbq-engine until Phase 1/2 give a clear internal
   picture; revisit whether HTTP-over-the-wire or a fork-patched in-process
   timing hook is the cleaner apples-to-apples.

------------------------------------------------------------------------
## 10. Phase 3 feasibility — findings (2026-06)

Goal: time the **same** queries over the **same** data through couchbase/query's
executor, as an absolute baseline. We probed every mechanism; none runs in a
stock dev environment (Couchbase Server.app installed, but no server source tree
checked out). Recorded here so a future run on an equipped box is turnkey.

**(a) In-process, in n1k1's module — impossible.** `query/execution` imports
`n1fty/verify → cbft`, which is cgo and was deliberately pruned by the pure-Go
decouple. It's not in n1k1's dependency graph, and re-adding it (n1fty, cbft,
indexing, …) would undo the pure-Go property the whole project rests on. Any
package that transitively touches `query/execution` hits this same wall, so even
a minimal in-process harness is out.

**(b) Prebuilt `cbq-engine` (ships in Couchbase Server.app) — can't run
standalone.** It *does* support `-datastore "dir:PATH"` (the same `datastore/file`
package `glue.FileStore` uses) and `test/suite/json/default/contacts/...` is
already a valid `dir:` datastore. But the shipped 7.6.x binary is a *server*
build: `server/cbq-engine/main.go` calls `waitForInitialSettings()`
*unconditionally* at the top of `main()`, which does a `wg.Wait()` that only
completes when the **metakv settings notifier** fires. Without a cluster
(`CBAUTH_REVRPC_URL` unset) the notifier retries `MAX_METAKV_RETRIES`=100 times
with backoff, then `Fatalf`s. No flag bypasses it (`-configstore` already
defaults to `stub:`; the wait is unconditional). So the binary never binds :8093.

**(c) Patched build from source — needs the full server manifest.** The fix to
(b) is a one-line guard: skip `waitForInitialSettings()` when `-configstore`
starts with `stub:` (run with default settings). Verified the patch site. But
building `cbq-engine` needs the whole Couchbase Server module graph — `n1fty`,
`cbauth`, `indexing`, `cbgt`, `cbft`, `gomemcached`, `go-couchbase`, `goutils`,
`go_json`, … — wired via the query go.mod's `replace => ../<sibling>` directives,
plus cgo (sigar/jemalloc). Local sibling checkouts exist but are stale GOPATH-era
trees with no `go.mod` at coherent versions, so they don't resolve as modules.
Producing a buildable set is a `repo sync`-against-a-manifest exercise — a real
server build, out of scope for this sandbox. (cgo itself is fine here: cc/clang
present, and Couchbase Server.app carries the sigar native libs.)

### To run Phase 3 later (recipe)

On a machine with a buildable couchbase/query (a synced server source tree, or
sibling modules at coherent versions), the **closest analog to n1k1** (file-based,
no KV/GSI/network) is the standalone `dir:` datastore:

1. Patch `server/cbq-engine/main.go` — guard the settings wait:
   ```go
   var initialCfg queryMetakv.Config
   var num_cpus int
   if strings.HasPrefix(*CONFIGSTORE, "stub:") {
       // standalone: no cbauth/metakv, run with default settings
   } else {
       initialCfg, num_cpus = waitForInitialSettings()
   }
   ```
2. `CGO_ENABLED=1 go build -o cbq-engine ./server/cbq-engine`
3. `./cbq-engine -datastore "dir:$PWD/test/suite/json" -configstore stub:`
4. Warm up, then POST the benchmark queries to `http://localhost:8093/query/service`
   (`--data-urlencode statement=...`), time N runs, compare to n1k1's numbers for
   the same query shapes (Phase 1/2 harness). Mind that HTTP/server overhead
   dominates micro-comparisons — prefer large per-query row counts.

A heavier alternative giving real *product* numbers (but a different architecture
— KV + GSI + network, not a file scan): start the full Couchbase Server locally
(`couchbase-server`, `couchbase-cli cluster-init`, `bucket-create`, `cbimport`
the corpus, `CREATE PRIMARY INDEX`), then query the running service. This
initializes/modifies the local Couchbase install, so it's a deliberate, opt-in
step.
