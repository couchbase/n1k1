# n1k1 benchmarks — design

## Status & remaining TODOs

_Last reviewed: 2026-07-11._

**Done:** Phase 1 (intrinsic pure-Go microbenchmarks) and Phase 2 (interpreted vs
compiled) are built and run under `make bench` / `bench-spill` / `bench-compiler`
over `test/benchmark/`, with recorded findings (flat allocs/op at 1M rows,
~4000-key GROUP BY spill onset with graceful degradation, fusion cutting ~40% of
allocs). Phase 3 (absolute baseline vs couchbase/query) stays blocked in a stock
dev env; the blockers and a future-run recipe are recorded below.

**Remaining (headline TODOs):**
- [ ] Phase 3 (vs couchbase/query, absolute baseline) — needs a buildable server
  source tree; the one-line patch and run recipe are recorded, but no stock env
  can build it.
- [ ] Fold the newer perf levers into this harness — streaming merge-scan,
  fixed-width columnar, window incremental-fold — currently measured ad hoc
  (`glue/window_bench_test.go`, `test/col_test.go`), not under `make bench`.
- [ ] Attack boxed-value / JSON alloc churn: the scan/filter/project
  path is parse-bound (Phase 2), so a native-lane ASOF/subquery projection is the
  next perf lever.

## Overview

DESIGN.md claims many performance techniques (garbage avoidance, push-based
fusion, static-param expr optimization, rhmap spilling, max-heap ORDER BY,
canonical-JSON reuse). This doc records how they are measured, across
latency / throughput / memory. Phase 1 validates them intrinsically (in-process,
pure-Go); Phase 2 races interpreted vs compiled n1k1; Phase 3 (blocked in a
stock env) would compare against couchbase/query as a baseline.

## Contents

1. [Claims → benchmarks](#claims--benchmarks)
2. [Strategy & phasing](#strategy--phasing)
3. [Metrics & dimensions](#metrics--dimensions)
4. [Harness, data & layout](#harness-data--layout)
5. [Phase 1 findings (current)](#phase-1-findings-current)
6. [Phase 2 — compiled-query benchmarking + findings](#phase-2--compiled-query-benchmarking--findings)
7. [What already exists](#what-already-exists)
8. [Open decisions](#open-decisions)
9. [Phase 3 feasibility — findings (2026-06)](#phase-3-feasibility--findings-2026-06)

## Claims → benchmarks

| Claim | Metric | Benchmark |
|---|---|---|
| garbage avoidance — `[]byte`/`[][]byte` recycling, no boxing, no `map[string]interface{}` | allocs/op flat vs N | `BenchmarkScan*`, `BenchmarkFilterProject*` across scales |
| per-row engine cost — push-based, register (positional `Vals`) vs map lookup, `YieldErr` | ns/row | scan → filter → project pipeline |
| static-param expr — evaluate const once, typed codepath (`ExprEq` 13 allocs/op vs `ExprStr` 4042 / 1000 docs) | ns/op, allocs/op | `ExprEq` vs `ExprStr` (`test/BenchmarkInterpExpr*`) |
| canonical JSON — `ValComparer.CanonicalJSON` into reused buffers | allocs/op | `base.BenchmarkCanonicalJSON` |
| jsonparser vs Unmarshal | ns/op, allocs | `base.BenchmarkParse` + field-access micro |
| GROUP BY / DISTINCT + spill — rhmap + `rhmap/store` to temp files, >RAM without OOM | rows/s, MemStats, temp bytes | `BenchmarkGroupBy*`, `TestSpillPoint` at rising cardinality |
| hash-join + spill | rows/s, mem | `BenchmarkJoinHash*` |
| max-heap ORDER BY (spills, no final sort) | ns/op, allocs | `BenchmarkOrderLimit*` |
| INTERSECT/EXCEPT reuse hash-join | rows/s | `BenchmarkSetOps*` |
| compilation / fusion — generated Go fuses operators, lifts vars | interp vs compiled ns/op | Phase 2 |

## Strategy & phasing

An in-process race against couchbase/query's executor is **not possible** here
(the pure-Go decouple dropped `query/execution`, which pulls cgo/cbft). Hence:

- **Phase 1 (done) — intrinsic validation, in-process, pure-Go.** Measures each
  claim on its own terms (allocs/op flat, ns/row, throughput, spill). No
  external yardstick, no feasibility risk; the bulk of the value.
- **Phase 2 (done) — n1k1 interpreted vs compiled.** Validates
  fusion/lifting; in-process, builds on Phase 1 (see
  [Phase 2](#phase-2--compiled-query-benchmarking--findings)).
- **Phase 3 (BLOCKED in a stock env) — vs couchbase/query, absolute baseline.**
  Needs a full Couchbase Server build/runtime; three mechanisms all blocked (see
  [Phase 3 feasibility](#phase-3-feasibility--findings-2026-06)).

## Metrics & dimensions

Standard Go `testing.B`, so results compose with `benchstat`.

- **Latency** — `ns/op` and derived **ns/row**.
- **Throughput** — `b.ReportMetric(rows/sec, "rows/s")`, rows = `nDocs * b.N`.
- **Memory** — `b.ReportAllocs()` → `allocs/op` + `B/op`. For macro runs, sample
  `runtime.MemStats` (HeapAlloc peak, NumGC, PauseTotalNs) and temp-file bytes
  when spilling. For per-object alloc attribution, `-memprofilerate=1` +
  `pprof -alloc_objects`.
- **Scale** — sweep `nDocs` = 1, 1K, 100K, 1M; plot *how each metric grows with
  N* (allocs/op flat = recycling works; throughput holds until spill, then
  degrades gracefully).

Each benchmark isolates **execution**: parse+plan once outside the `b.N` loop;
only `engine.ExecOp` runs inside, with a **no-op yield**.

## Harness, data & layout

### Data sources

- **Synthetic generator** (`gen.go`) — one corpus-like "contact" doc shape, with a
  tunable-cardinality grouping key `g`. Cheapest scan path replicates the source
  string via `reps` (no I/O), amplifying row count without growing the source or
  the distinct-key count.
- **Realistic shapes** — `glue` over the vendored corpus (`test/suite/json`) via
  `FileStore` + `DatastoreOp`, exercising the real datastore path. Lower N.

### Harness conventions

- **Plan once, execute many** — build the `base.Op` tree once, reuse across
  `b.N`, resetting `Vars` / recycling spill state per iteration.
- **No-op yield** — `yieldVals` increments a counter; `yieldErr` fails the bench.

### Layout (`test/benchmark/`, `//go:build n1ql`)

```
gen.go                    synthetic doc generator (shape + cardinality knobs)
harness.go                plan-once/execute-many, no-op yield, rows/s metric
bench_scan_test.go        scan, filter, project (per-row cost, garbage)
bench_expr_arith_test.go  static-param vs interp expr
bench_spill_test.go       GROUP BY spill onset (TestSpillPoint)
bench_self_test.go        self-timed engine micro-runs
bench_compiler_test.go    Phase 2 interp-vs-compiled generator
compare_test.go           value-compare micro
boxing_test.go            interface-boxing / alloc micro
README.md                 claim→bench map, how to run, findings, benchstat tips
```

Make targets: `make bench` (all, `-benchmem`), `make bench-spill` (spill onset),
`make bench-compiler` (Phase 2, `-benchtime=30s`), `make benchmark-expr-eq`
(static-param expr eq). Canonical-JSON / parse micro-benchmarks stay in `base/`.

## Phase 1 findings (current)

Indicative apple-silicon numbers (`-benchtime=30x`; trends matter, not exact ns).
Full detail in `test/benchmark/README.md`.

- **Garbage avoidance — holds.** allocs/op is constant as row count scales 1000×:
  scan **6**, scan+filter **18**, scan+filter+project **37** allocs/op at 1K *and*
  1M rows. The fixed count is pipeline setup; per-row allocation is ~zero.
- **Throughput:** raw scan ~500M rows/s, +filter ~12M, +project ~5M rows/s.
- **Spill point ≈ 4000–5000 distinct keys.** GROUP BY's rhmap/store keeps
  metadata slots in memory until ~4000 distinct keys (`StartSize=5303`, `Grow`
  fires a touch earlier on load factor / MaxDistance), then grows to an mmap'd
  `*_slots_*` file. Above it, temp bytes grow ~linearly (~80 B/key: ~4MB at 64K
  keys, ~20MB at 256K).
- **Graceful degradation — holds.** GROUP BY throughput barely moves across the
  spill boundary: ~4.5M rows/s at 1000 distinct (in-memory) vs ~4.2M at 64000
  (spilled) — ~6% slower while paging to disk, not a cliff.

## Phase 2 — compiled-query benchmarking + findings

`make bench-compiler` generates, for a fixed query set, paired `BenchmarkInterp_X`
(engine.ExecOp over a baked Op tree) and `BenchmarkCompiled_X` (operators fused
inline as compiler-generated Go) into `test/tmp`, run side by side. Reuses the
Phase 1 generator/scales/metrics — only the "run" step differs. (Generator:
`bench_compiler_test.go`'s `TestGenerateBenchmarks`; codegen helpers shared with
the `test/emit` differential generators.)

Findings, stable across repeats (30M rows/op, `-benchtime=30s`):

| query (30M rows/op) | interpreted | compiled | |
|---|---|---|---|
| ScanFilterProject | ~5.60s, 35 allocs/op | ~5.77s, 21 allocs/op | ~3% slower, **40% fewer allocs** |
| GroupBy | ~4.28s, 174 allocs/op | ~3.74s, ~150 allocs/op | **~13% faster**, ~15% fewer allocs |

- **Fusion + lifted-var reuse cut allocations** (fewer closures / reused buffers)
  on both — the DESIGN.md claim holds on the allocation axis. (allocs/op held
  ~flat from 100K → 30M rows: 35 vs 34 for the pipeline — garbage avoidance
  again, at 300× scale.)
- **Wall-time is shape-dependent.** GROUP BY (function-call-heavy aggregation per
  row) gets a clear ~13% from fusion. The scan→filter→project pipeline is
  parse-bound (jsonparser field extraction dominates), so eliminating per-op call
  overhead is marginal there (even slightly slower). Signal: for
  scan/filter/project, optimize parsing; fusion pays most for call-heavy ops.

## What already exists

Build on these, don't duplicate:

- `test/n1k1_interp_test.go`: `BenchmarkInterpExprEq/Str_{1,1000,100000}Docs`,
  `BenchmarkInterpGroupBy_{1,100,10000}Docs`.
- `base/`: `BenchmarkCanonicalJSON`, `BenchmarkValCompare`, `BenchmarkParse`,
  `BenchmarkEncodeAsString`; `test/BenchmarkBoxing`, `test/BenchmarkValCompare*`.
- `make benchmark-expr-eq` wires `-tags n1ql` + `CGO_ENABLED=0` + benchmem.

## Open decisions

1. **Generator shape** — one "contact"-like doc (in use) vs several
   (wide/narrow/nested).
2. **Scale ceiling** — 1M docs fine for in-memory scans; spilling benches need
   cardinality high enough to cross the rhmap/store threshold (documented above
   as its own result).
3. **Phase 3 trigger** — deferred; revisit HTTP-over-the-wire vs a fork-patched
   in-process timing hook when a buildable server tree exists.

## Phase 3 feasibility — findings (2026-06)

Goal: time the **same** queries over the **same** data through
couchbase/query's executor. **Blocked in a stock dev env** (Couchbase Server.app
installed, no server source tree): all three probed mechanisms fail. (a) An
in-process race is impossible — `query/execution` imports `n1fty/verify → cbft`
(cgo), pruned by the pure-Go decouple. (b) The prebuilt `cbq-engine` from
Server.app can't run standalone — `main.go` calls `waitForInitialSettings()`
unconditionally, which blocks forever on the metakv settings notifier without a
cluster (no flag bypasses it; never binds :8093). (c) A patched from-source build
would work (one-line guard, below) but needs the whole Couchbase Server module
graph (`n1fty`, `cbauth`, `indexing`, `cbgt`, `cbft`, … via query go.mod
`replace => ../<sibling>`), a `repo sync`-against-a-manifest exercise out of
scope here. Phase 1/2 stand alone as the perf story.

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

**Heavier alternative (real product numbers, different architecture):** start
full Couchbase Server (`couchbase-cli cluster-init`, `bucket-create`, `cbimport`
corpus, `CREATE PRIMARY INDEX`), then query it — but that is KV + GSI + network,
not a file scan, and modifies the local install (opt-in).
