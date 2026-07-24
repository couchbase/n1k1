# test/benchmark/ — do DESIGN.md's perf techniques work?

Phase 1 of `../../DESIGN-benchmark.md`: in-process, pure-Go benchmarks over
synthetic local data that measure the claims in `../../DESIGN.md`. Each runs the
engine with a **no-op yield** (count rows, discard bytes), so the number is the
engine, not output formatting.

## Run

    make bench          # all engine benchmarks, with -benchmem
    make bench-spill    # pin where GROUP BY spills to disk (verbose)

    # directly:
    CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' \
      go test -tags n1ql -run=xxx -bench=. -benchmem ./test/benchmark

Compare runs with `benchstat` (save `-bench` output to files, then
`benchstat old.txt new.txt`).

## Claim → benchmark

| DESIGN.md claim | Benchmark | Signal |
|---|---|---|
| garbage avoidance / `[]byte` / no boxing | `BenchmarkScan`, `ScanFilter`, `ScanFilterProject` | allocs/op **flat** as rows grow 1K→1M |
| push-based per-row cost | same (rows/s) | throughput holds at scale |
| static-param expr optimization | `ScanFilter` (eq) + `test/BenchmarkInterpExprEq` vs `ExprStr` | low, flat allocs |
| rhmap spill-to-disk + graceful degradation | `TestSpillPoint`, `BenchmarkGroupBy` | spill onset; throughput holds across it |

Lower-level micro-benchmarks also live here: `compare_test.go` (value compare)
and `boxing_test.go` (interface boxing / allocs). The interpreter-vs-compiled
generator is `bench_compiler_test.go` (Phase 2, below). The compiler-codegen
helpers it shares with the `test/` differential generators are in the
`test/emit` package. (Canonical-JSON / parse micro-benchmarks still live in
`base/`; the static-param expr-eq benchmarks are `test/`'s `BenchmarkInterpExpr*`,
run via `make benchmark-expr-eq`.)

## Findings so far

Indicative numbers (apple-silicon, `-benchtime=30x`; trends matter, not exact ns):

- **Garbage avoidance — holds.** allocs/op is constant as row count scales 1000×:
  scan **6**, scan+filter **18**, scan+filter+project **37** allocs/op at 1K *and*
  1M rows. The fixed count is pipeline setup; per-row allocation is ~zero.
- **Throughput:** raw scan ~500M rows/s, +filter ~12M, +project ~5M rows/s.
- **Spill point ≈ 4000–5000 distinct keys.** GROUP BY's rhmap/store keeps its
  metadata slots in memory until ~4000 distinct keys (rhmap `StartSize=5303`,
  but `Grow` fires a touch earlier on load factor / MaxDistance), then grows to
  an mmap'd `*_slots_*` file. Above it, temp bytes grow ~linearly (~80 B/key:
  ~4MB at 64K keys, ~20MB at 256K).
- **Graceful degradation — holds.** GROUP BY throughput barely moves across the
  spill boundary: ~4.5M rows/s at 1000 distinct (in-memory) vs ~4.2M at 64000
  (spilled) — ~6% slower while paging to disk, not a cliff.

## Phase 2: interpreted vs compiled

`make bench-compiler` generates, for a fixed query set, paired
`BenchmarkInterp_X` (engine.ExecOp over a baked Op tree) and
`BenchmarkCompiled_X` (the operators fused inline as compiler-generated Go) into
`test/tmp`, run side by side. (Generator: `test/bench_compiler_test.go`'s
`TestGenerateBenchmarks`.)

Each op runs ~5s (30M rows; `bench-compiler` uses `-benchtime=30s`, so per-op
fixed setup is negligible and the delta is the per-row codepath). Findings,
stable across repeats:

| query (30M rows/op) | interpreted | compiled | |
|---|---|---|---|
| ScanFilterProject | ~5.60s, 35 allocs/op | ~5.77s, 21 allocs/op | ~3% slower, **40% fewer allocs** |
| GroupBy | ~4.28s, 174 allocs/op | ~3.74s, ~150 allocs/op | **~13% faster**, ~15% fewer allocs |

- **Fusion + lifted-var reuse cut allocations** (fewer closures / reused
  buffers) on both — the DESIGN.md claim holds on the allocation axis. (allocs/op
  also held ~flat from 100K → 30M rows: 35 vs 34 for the pipeline — garbage
  avoidance again, at 300x scale.)
- **Wall-time is shape-dependent.** GROUP BY (aggregation is function-call-heavy
  per row) gets a clear ~13% from fusion. The scan→filter→project pipeline is
  parse-bound (jsonparser field extraction dominates), so eliminating per-op
  call overhead is marginal there (even slightly slower). Useful signal: for
  scan/filter/project, optimize parsing; fusion pays most for call-heavy ops.

## Concurrency: how does throughput scale with clients?

`bench_concurrency_test.go` characterizes n1k1 under a "goroutine per client" server model:
ONE shared `Store` (the cbq datastore is a process-global singleton, so a server serves one
data root), N goroutines each with its OWN `Session`, all driving the FULL request path
(`Session.Run`: parse → plan → convert → execute). Run **without `-race`** (see the caveat
below).

    # the client ramp (queries/s at G = 1,2,4,8,16,32):
    go test -tags n1ql -run=xxx -bench=BenchmarkConcurrentQueries -benchtime=500ms ./test/benchmark
    go test -tags n1ql -run=xxx -bench=BenchmarkConcurrent -benchtime=500ms ./test/benchmark

- `BenchmarkConcurrentQueries/gNN` — a query mix at each concurrency level; read the
  **queries/s** metric across the ramp.
- `BenchmarkConcurrentByShape` — each query shape at a fixed G=8, to attribute a change.
- `BenchmarkConcurrentRunParallel` — the idiomatic peak-parallel-throughput number.

**Finding (Apple M2 Pro, 12 cores, 100-doc keyspace; the SHAPE is the point — absolute q/s
swings ±25% run-to-run at `-benchtime=500ms`).** Throughput does **not** scale near-linearly
with cores. The curve is consistently: rise from G=1, **peak around G=4–8 at only ~2–2.5×
single-threaded**, then plateau and erode as G grows past the peak — e.g. one run
`252 → 348 → 443 → 444 → 440 → 410 q/s` for G = 1,2,4,8,16,32 (≈1.8× peak); another
`295 → 446 → 624 → 660 → 485 → 550` (≈2.5× peak). Either way it's far short of the ~12×
a contention-free CPU-bound workload would give on 12 cores, and it *declines* past the peak.

### Where the ceiling is (PREPARE/EXECUTE locates it)

`bench_concurrency_prepared_test.go` re-runs the ramp with prepared queries, which skip
parse+plan:

- `BenchmarkConcurrentPreparedShared` — ONE immutable `*glue.PreparedPlan` (built once via
  `PlanConvert`) reused by every goroutine through its own Session's `PlanExec`. A single shared
  prepared plan **is** concurrent-safe (per-run state is in fresh Vars; verified race-clean by
  `TestConcurrentSharedPreparedPlanRace`). Nothing calls the planner in the loop.
- `BenchmarkConcurrentPreparedPerClient` — each client runs `PREPARE p AS …` once, then loops
  `EXECUTE p` (the realistic per-connection model; first EXECUTE caches that Session's plan).

Result: prepared runs faster in ABSOLUTE terms (~½ the allocs, ~10–30% higher throughput) but its
scaling curve is the **same** — peak ~2× at G=4, then decline. So the ceiling is **not** the
planner or the parser (removing them via PREPARE doesn't lift it).

**pprof pins it to SYSCALLS, not GC** (`-cpuprofile`/`-memprofile`/`-mutexprofile` on the g16
sub-benchmark):

- CPU is **~94–97% `syscall.syscall`** in both ad-hoc and prepared, all in the scan path
  (`DatastoreScanRecords → walkSource.Next → OpenFile`): the file-per-doc keyspace opens/reads/
  closes/`lstat`s a file per document + walks the dir on EVERY query.
- The planner is ~4% of ad-hoc CPU, 0% of prepared. **GC is negligible** (no `mallocgc`/GC worker
  in the top). Top *alloc* is `rhmap/store.CreateRHStoreFile` (~38%) — GROUP BY/ORDER build a
  per-query mmap temp store; the mutex profile is ~98% `runtime.unlock` (kernel/runtime locks
  around the syscall + mmap flood), which is what caps scaling at ~G=4 and erodes past it.
- Shrinking to 4 docs raises absolute q/s (~3600 vs ~250) but keeps the 94% syscall share and the
  same curve — it's the per-query syscall PATTERN, not data volume.

**Control — `BenchmarkConcurrentUnnestConst` (file-less):** a literal `[{"items":[…]}]` array
UNNEST'd + aggregated, which cbq folds to a value scan (zero datastore syscalls). It scales
*strongly* — ad-hoc **~6.6× at G=32**, prepared **~2.9× at G=8** — vs the file-backed ~1.6–2×
plateau, and its pprof drops syscalls ~97%→~38% (residue: per-query `MakeVars` temp-dir + GC). So
the **engine is not the concurrency bottleneck — the file-per-doc scan is.**

**Lever:** cut per-query syscalls — a syscall-light layout (single JSONL / columnar Parquet = one
open, no per-doc walk; cf. `parallel-scan-experiment`, ~245× from packing file-per-doc into one
file), an in-memory temp store for small GROUP BY/ORDER, and skipping the `MakeVars` temp dir for
non-spilling queries. blocker 4a (the cbq fork planner-pool race, `DESIGN-concurrency.md`) stays a
`-race` correctness item + a small constant-factor cost, not the throughput ceiling. n1k1's own
per-query globals are already fixed (blockers 1–3).

**Caveat — no `-race`.** Functionally correct under contention (guardrails: `glue/concurrency_test.go`
+ `TestConcurrentSharedPreparedPlanRace`), but the cbq fork planner's global pools still race under
`-race` (blocker 4a, a pending fork patch), so these throughput benchmarks run without it.

## Not here yet

- **vs couchbase/query** — Phase 3 is **blocked in a stock dev env** (no
  buildable server source tree). The prebuilt `cbq-engine` from Couchbase
  Server.app can't run standalone (it blocks forever on the cbauth/metakv
  settings notifier), an in-process race is impossible (the cgo `query/execution`
  deps were pruned by the pure-Go decouple), and a from-source patched build
  needs the full server module manifest. The blockers, the one-line patch that
  makes a from-source `cbq-engine` run standalone, and a turnkey run recipe are
  documented in `../../DESIGN-benchmark.md` §10. Phase 1/2 stand alone as the
  perf story.

## Notes

- `gen.go` makes one corpus-like "contact" doc shape; the `g` field is a
  tunable-cardinality grouping key.
- Scan `reps` cheaply amplifies row count without growing the source string
  (used for throughput/garbage scaling); distinct cardinality (for spill) comes
  from distinct `g` values, which `reps` does not multiply.
