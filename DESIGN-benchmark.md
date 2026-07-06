# n1k1 benchmarks — design

## Overview

DESIGN.md claims many performance techniques (garbage avoidance, push-based
fusion, static-param expr optimization, rhmap spilling, max-heap ORDER BY,
canonical-JSON reuse). This doc plans how to measure whether they pay off, across
latency / throughput / memory. Phase 1 validates them intrinsically (in-process,
pure-Go); Phase 2 races interpreted vs compiled n1k1; Phase 3 (blocked in a
stock env) would compare against couchbase/query as a baseline.

## Contents

1. [Claims → benchmarks](#claims--benchmarks)
2. [Strategy & phasing](#strategy--phasing)
3. [Metrics & dimensions](#metrics--dimensions)
4. [Harness, data & layout](#harness-data--layout)
5. [Phase 2 — compiled-query benchmarking](#phase-2--compiled-query-benchmarking)
6. [What already exists](#what-already-exists)
7. [Open decisions](#open-decisions)
8. [Phase 3 feasibility — findings (2026-06)](#phase-3-feasibility--findings-2026-06)

## Claims → benchmarks

| Claim | Metric | Benchmark |
|---|---|---|
| garbage avoidance — `[]byte`/`[][]byte` recycling, no boxing, no `map[string]interface{}` | allocs/op flat vs N | `BenchmarkScan*`, `BenchmarkFilterProject*` across scales |
| per-row engine cost — push-based, register (positional `Vals`) vs map lookup, `YieldErr` | ns/row | scan → filter → project pipeline |
| static-param expr — evaluate const once, typed codepath (`ExprEq` 13 allocs/op vs `ExprStr` 4042 / 1000 docs) | ns/op, allocs/op | `ExprEq` vs `ExprStr` (have; formalize) |
| canonical JSON — `ValComparer.CanonicalJSON` into reused buffers | allocs/op | `base.BenchmarkCanonicalJSON` (have) |
| jsonparser vs Unmarshal | ns/op, allocs | `base.BenchmarkParse` (have) + field-access micro |
| GROUP BY / DISTINCT + spill — rhmap + `rhmap/store` to temp files, >RAM without OOM | rows/s, MemStats, temp bytes | `BenchmarkGroupBy*`, `BenchmarkDistinct*` at rising cardinality |
| hash-join + spill | rows/s, mem | `BenchmarkJoinHash*` |
| max-heap ORDER BY (spills, no final sort) | ns/op, allocs | `BenchmarkOrderLimit*` |
| INTERSECT/EXCEPT reuse hash-join | rows/s | `BenchmarkSetOps*` |
| compilation / fusion — generated Go fuses operators, lifts vars | interp vs compiled ns/op | Phase 2 |

## Strategy & phasing

An in-process race against couchbase/query's executor is **not possible** here
(the pure-Go decouple dropped `query/execution`, which pulls cgo/cbft). Hence:

- **Phase 1 (now) — intrinsic validation, in-process, pure-Go.** Measure each
  claim on its own terms (allocs/op flat, ns/row, throughput, spill). No
  external yardstick, no feasibility risk; the bulk of the value.
- **Phase 2 (deferred) — n1k1 interpreted vs compiled.** Validates
  fusion/lifting; in-process, builds on Phase 1 (see
  [Phase 2](#phase-2--compiled-query-benchmarking)).
- **Phase 3 (BLOCKED in a stock env) — vs couchbase/query, absolute baseline.**
  Needs a full Couchbase Server build/runtime; three mechanisms all blocked (see
  [Phase 3 feasibility](#phase-3-feasibility--findings-2026-06)).

## Metrics & dimensions

Standard Go `testing.B`, so results compose with `benchstat`.

- **Latency** — `ns/op` and derived **ns/row**.
- **Throughput** — `b.ReportMetric(rows/sec, "rows/s")`, rows = `nDocs * b.N`.
- **Memory** — `b.ReportAllocs()` → `allocs/op` + `B/op`. For macro runs, sample
  `runtime.MemStats` (HeapAlloc peak, NumGC, PauseTotalNs) and temp-file bytes
  when spilling.
- **Scale** — sweep `nDocs` = 1, 1K, 100K, 1M; plot *how each metric grows with
  N* (allocs/op flat = recycling works; throughput holds until spill, then
  degrades gracefully).

Each benchmark isolates **execution**: parse+plan once outside the `b.N` loop;
only `engine.ExecOp` runs inside, with a **no-op yield**.

## Harness, data & layout

### Data sources

- **Synthetic generator** — N docs of a configurable shape (scalars, nested
  object, array), scaling to 1M. Cheapest scan path is `jsonsData` (one doc
  replicated N times, in-memory), no I/O; existing `BenchmarkInterp*` use it.
- **Realistic shapes** — a few benches via `glue` over the vendored corpus
  (`test/suite/json`) using `FileStore` + `DatastoreOp`, exercising the real
  datastore path. Lower N.

### Harness conventions

- **Plan once, execute many** — build the `base.Op` tree once, reuse across
  `b.N`, resetting `Vars` / recycling spill state per iteration.
- **No-op yield** — `yieldVals` increments a counter; `yieldErr` fails the bench.

### Layout

```
test/benchmark/            (//go:build n1ql)
  gen.go                   synthetic doc generator (shape + count knobs)
  harness.go               plan-once/execute-many, no-op yield, rows/s metric
  bench_scan_test.go       scan, filter, project (per-row cost, garbage)
  bench_expr_test.go       static-param vs interp expr (migrate ExprEq/ExprStr)
  bench_group_test.go      GROUP BY / DISTINCT + spill
  bench_join_test.go       nested-loop + hash join + spill
  bench_order_test.go      ORDER BY / OFFSET / LIMIT max-heap
  bench_setops_test.go     INTERSECT / EXCEPT
  README.md                claim→bench map, how to run, benchstat tips
```

Make targets: `make bench` (all, `-benchmem`), `make bench-mem` (alloc focus);
reuse the `benchmark-expr-eq` flow. Keep per-claim micro-benches in `base/`.

## Phase 2 — compiled-query benchmarking

The compiler emits Go for a query (today into `test/tmp` via the differential
harness). Plan: generate Go for a fixed set of benchmark queries, build into
`test/benchmark`, run the *same* query interpreted vs compiled at identical
data/scale. Expected wins: fewer calls (fusion), fewer allocs (lifted reuse
buffers). Reuses the Phase 1 generator/scales/metrics — only the "run" step
differs (compiled func vs `engine.ExecOp`).

## What already exists

Build on these, don't duplicate:

- `test/n1k1_interp_test.go`: `BenchmarkInterpExprEq/Str_{1,1000,100000}Docs`,
  `BenchmarkInterpGroupBy_{1,100,10000}Docs` — seed for bench_expr/bench_group.
- `base/`: `BenchmarkCanonicalJSON`, `BenchmarkValCompare`, `BenchmarkParse`,
  `BenchmarkEncodeAsString`; `test/BenchmarkBoxing`, `test/BenchmarkValCompare*`.
- `make benchmark-expr-eq` wires `-tags n1ql` + `CGO_ENABLED=0` + benchmem.

## Open decisions

1. **Generator shape** — one "contact"-like doc vs several (wide/narrow/nested).
   Lean: start with one corpus-like shape.
2. **Scale ceiling** — 1M docs fine for in-memory scans; spilling benches need
   cardinality high enough to cross the rhmap/store threshold — document that
   threshold as its own result.
3. **Phase 3 trigger** — defer cbq-engine until Phase 1/2 give a clear picture;
   revisit HTTP-over-the-wire vs a fork-patched in-process timing hook.

## Phase 3 feasibility — findings (2026-06)

Goal: time the **same** queries over the **same** data through
couchbase/query's executor. None of three probed mechanisms runs in a stock dev
env (Couchbase Server.app installed, no server source tree).

### Mechanisms probed (all blocked)

**(a) In-process, in n1k1's module — impossible.** `query/execution` imports
`n1fty/verify → cbft` (cgo), pruned by the pure-Go decouple. Re-adding it would
undo the pure-Go property; any package transitively touching it hits this wall.

**(b) Prebuilt `cbq-engine` (Couchbase Server.app) — can't run standalone.** It
supports `-datastore "dir:PATH"` (same `datastore/file` `glue.FileStore` uses),
and `test/suite/json/default/contacts/...` is a valid `dir:` datastore. But the
7.6.x *server* build's `server/cbq-engine/main.go` calls
`waitForInitialSettings()` unconditionally; its `wg.Wait()` returns only when the
**metakv settings notifier** fires. Without a cluster (`CBAUTH_REVRPC_URL` unset)
it retries `MAX_METAKV_RETRIES`=100 then `Fatalf`s. No flag bypasses it; never
binds :8093.

**(c) Patched build from source — needs the full server manifest.** Fix to (b):
one-line guard skipping `waitForInitialSettings()` when `-configstore` starts
with `stub:` (verified). But building `cbq-engine` needs the whole Couchbase
Server module graph — `n1fty`, `cbauth`, `indexing`, `cbgt`, `cbft`,
`gomemcached`, `go-couchbase`, `goutils`, `go_json`, … — via query go.mod's
`replace => ../<sibling>` directives, plus cgo (sigar/jemalloc). Local sibling
checkouts are stale GOPATH-era trees with no coherent `go.mod` — a `repo
sync`-against-a-manifest exercise, out of scope. (cgo is fine: cc/clang present,
Server.app carries the sigar libs.)

### To run Phase 3 later (recipe)

On a buildable couchbase/query, the closest analog to n1k1 (file-based, no
KV/GSI/network) is the standalone `dir:` datastore:

1. Patch `server/cbq-engine/main.go` — guard the settings wait:
   ```go
   if strings.HasPrefix(*CONFIGSTORE, "stub:") {
       // standalone: no cbauth/metakv, default settings
   } else {
       initialCfg, num_cpus = waitForInitialSettings()
   }
   ```
2. `CGO_ENABLED=1 go build -o cbq-engine ./server/cbq-engine`
3. `./cbq-engine -datastore "dir:$PWD/test/suite/json" -configstore stub:`
4. Warm up, POST queries to `http://localhost:8093/query/service`
   (`--data-urlencode statement=...`), time N runs, compare to n1k1. HTTP/server
   overhead dominates micro-comparisons — prefer large per-query row counts.

### Heavier alternative (real product numbers, different architecture)

Real *product* numbers, but KV + GSI + network (not a file scan): start full
Couchbase Server (`couchbase-server`, `couchbase-cli cluster-init`,
`bucket-create`, `cbimport` corpus, `CREATE PRIMARY INDEX`), then query it.
Modifies the local install — opt-in.
