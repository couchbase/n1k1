# DESIGN-concurrency: n1k1 under concurrent workloads

**Question.** If n1k1 grows a listen port and serves clients goroutine-per-connection, does it
fall over — and how does throughput scale?

**Short answer.** n1k1's own engine is now goroutine-per-client race-clean, and it *scales* — the
throughput ceiling we measured is not the engine but the **file-per-doc keyspace layout** (a scan
opens one file per document). Over a syscall-light layout (single file), concurrent throughput is
~20× higher. One `-race` failure remains, and it's in the cbq **fork's** planner, not n1k1.

## The model: one data root, one Store, N sessions

The cbq planner resolves keyspaces through a process-global datastore singleton
(`datastore.GetDatastore`), so a server serves ONE data root:

```
FileStore(dir) once  ->  store.InitParser() once   (sets the global datastore)
   -> per connection:  sess := &Session{Store: store, Namespace: "default"}
   -> goroutine per connection calls sess.Run(stmt)  (or PlanExec of a shared *PreparedPlan)
```

A `Session` is single-query-at-a-time (`halt`/args/prepareds are per-Session mutable state);
concurrency is *across* sessions sharing the one store. Multiple stores concurrently isn't
supportable (they'd fight over the global datastore) and isn't the model.

## Race-safety: what was fixed, what remains

Already safe: per-request state is per-request (`GlueContext`, `base.Vars`/`Ctx` are built per
`Run`; `Session.halt` is atomic); the records read path is per-scan; the flat namespace's Iceberg
snapshot cache is mutex-guarded; observability counters are atomic; and the prior
`corrParent`/`withScope` and cbq `LocklessPool` races were already patched.

`Session.Run` used to mutate three process-globals per query — all now FIXED (`glue/concurrency_test.go`
is the guardrail; its JSON variant is `-race`-clean):

| global | was | fix |
|---|---|---|
| `engine.ExprCatalog` | lazy check-then-set of a shared map per `Run` → concurrent-map-write PANIC | registered once in `glue` `init()` (`expr.go`); read-only during serving |
| `datastore.SetDatastore` | written every `Run` → write-write race | `ensureDatastore` (`stmt.go`) writes only when it differs; in the one-store model (set at `InitParser`) every `Run` just reads it |
| `engine.ExecOpEx` (IoC hook, read on every datastore op) | swapped to `DatastoreOp` + `defer`-restored per `Run` | it's ALWAYS `DatastoreOp` (per-request source variation rides `Ctx.Pipe` *inside* it), so set once in `init()`; no per-run swap. No engine op-dispatch refactor needed |

Remaining `-race` failures — NOT in n1k1:

- **4a (open, fork).** The cbq fork planner shares process-global object pools across concurrent
  builds — `planner._COVERING_ENTRY_POOL` via `util.FastPool`/`poolList`, hit in `buildCoveringScan`
  — and their lockless fast-path isn't goroutine-safe (same class as the patched `LocklessPool`).
  Intermittent under concurrent planning. Needs a FORK patch (mutex / per-request pool → republish →
  go.mod re-pin), not an n1k1 change. It is **orthogonal to throughput** (see below).
- **4b (audit).** The n1k1 `idx_si`/`datastore_scan` process-global caches
  (`glue/idx_si.go`, `datastore_scan.go`, already commented "fine for the single-process CLI") aren't
  stress-tested (no secondary indexes in the harness); each needs a read-only-during-serving audit.

## Throughput scaling (measured)

`test/benchmark/bench_concurrency*_test.go` (`make bench-concurrency`) ramps concurrent clients over
one shared Store and reports queries/s. On a 12-core M2 Pro (trends matter, ±25% run-to-run):

> **Notation.** `G` is the number of concurrent client goroutines, each with its own `Session`
> hammering the one shared Store. It appears in Go sub-benchmark names as a zero-padded `gNN`
> suffix — so `g01` = one client (single-threaded baseline), `g04` = four, `g08` = eight, `g16`,
> `g32`. "Peak" below is the best queries/s across the `G` sweep, and "N×" is that peak over the
> `g01` baseline (the concurrency speedup).

| workload | G=1 | peak | shape |
|---|---|---|---|
| file-per-doc keyspace (100 one-doc files) | ~250 q/s | ~450 (~1.9×) | plateau by G=4 |
| **single-file keyspace** (same 100 docs, one `.jsonl`) | ~2500 q/s | **~8900 (~3.5×)** | peak ~G=16 |
| file-less (literal-array `UNNEST`, ad-hoc) | ~290 | ~1930 (~6.6×) | rises to G=32 |

The file-per-doc curve is not the engine — pprof of it (both ad-hoc and PREPARE/EXECUTE) is **~94–97%
`syscall.syscall`**, all in the scan path (`DatastoreScanRecords → walkSource.Next → OpenFile`): it
opens+reads+closes+`lstat`s a file **per document** plus walks the dir on **every** query. The
planner is ~4% of ad-hoc CPU (0% of prepared), and **GC is negligible**. Two controls confirm the
engine itself scales: a **single-file** keyspace (one open per scan) is ~10× faster single-threaded,
scales ~3.5×, and delivers ~20× the peak concurrent throughput; a **file-less** UNNEST (cbq folds the
literal to a value scan — zero datastore syscalls) scales ~6.6× and its pprof syscall share drops to
~38%.

PREPARE/EXECUTE is a constant-factor win, not a scaling one: a single immutable `*PreparedPlan`
(built once via `PlanConvert`) is safely shared across goroutines' `PlanExec` (race-clean —
`TestConcurrentSharedPreparedPlanRace`; a shared *Session* is not, being single-query-at-a-time). It
skips parse+plan (~½ the allocs), but its scaling curve matches ad-hoc — because the ceiling is the
scan, not the plan.

**Secondary per-query costs, now optimized.** Two eager per-`PlanExec` costs showed up once the
file-scan was removed, and both are fixed:

- `MakeVars` used to create a temp dir (`mkdir`/`rmdir`) on every `PlanExec` even for a query that
  can't spill. Now the dir is **lazy** — `rt.SpillState.ensureDir` creates it only when an
  allocator (GROUP/ORDER/hash-join) actually needs to spill a file, so every scan/filter/project
  pays zero mkdir.
- GROUP BY / ORDER / hash-join / window eagerly allocate the rhmap store's `StartSize`(=5303)-slot
  buffer at op init (an in-memory ~tens-of-KB heap alloc; the mmap'd *file* is already lazy — only
  on grow past ~4000 keys). A **Session now holds one `rt.SpillState`** (allocator pools + temp
  dir) reused across all its `PlanExec`s, so that buffer + the batch buffers recycle across a
  connection's queries and the temp dir is created at most once (freed by `Session.Close`). Only
  the pools are shared; the `Ctx` is fresh per query (so `RunningAggJobs`/`Stats` never leak), and
  the pooled pieces are cleared not just parked — `RHStore.Reset` zeroes every hash slot,
  `Heap.Reset` truncates, batches are `AcquireBatch()[:0]`'d — so there's no cross-query data leak
  (guarded by `TestSessionSpillReuseNoLeak`).

Measured effect (12-core M2 Pro): the single-file mix at **g08** (8 concurrent clients) rose ~8585 →
~12336 (lazy dir) → ~19715 queries/s (+ Session reuse) — **~2.3× the pre-optimization baseline**;
group-query bytes/query −20–40%. These wins concentrate exactly where the fixed overhead dominates:
*small, repeated* queries over a warm Session (tiny keyspaces), i.e. the concurrent-server regime.

**Do these optimizations touch the older / versus benchmarks? No regressions; near-zero effect.**

- **Older non-concurrent engine benchmarks — unchanged.** `BenchmarkScan` (6 allocs/op, ~510M
  rows/s), `BenchmarkGroupBy` (42 allocs/op, ~4.6M rows/s), `ArithAddNative` (30 ns, 0 alloc), etc.
  are byte-identical before/after. They call `engine.ExecOp` with a `vars` built once and reused
  across `b.N`, so they never touch the per-query `Session.Run → PlanExec → MakeVars` path the
  optimizations live on.
- **`versus/` n1k1 side — flat allocations, small ms win on light queries.** An A/B of the same
  warm-REPL versus run (baseline vs optimized binary, NDOCS=2000 / BULK_ITEMS=20000 / REPS=5) shows
  the heavy *bulk* queries (17–90 MB working sets from 20 000-item UNNESTs) unchanged in MB — the
  reused ~tens-of-KB `StartSize` buffer is a rounding error against that working set. The small
  *packed* queries got ~10–20% faster in ms (e.g. filter+project 2.05 → 1.65, expr-heavy 2.82 →
  2.43), which is the lazy-dir win: dropping a per-query `mkdir`+`rmdir` is a measurable fraction of
  a 1–2 ms in-memory query. Net: a strict Pareto improvement — small wins on light queries, neutral
  on heavy ones, the big ~2.3× win on the concurrent-small-query path — with no regressions, so the
  committed versus numbers don't need a refresh.

**Remaining lever** (none is the planner): a syscall-light data layout — single-file / columnar
Parquet, one open per scan (cf. the `parallel-scan-experiment` memo: file-per-doc packed to one
file was ~245× faster). That's the big one; the two above are done.

### The trivial-query floor (`SELECT 0`) — where the non-scan ceiling actually is

To crank on *everything but* the scan, `BenchmarkConcurrentTrivial` / `…TrivialPrepared` run the
most minimal query there is — `SELECT 0` (no `FROM`, no scan, no fetch) — raw and PREPARE-shared.
It exposes two things the scan-bound curves hid (12-core M2 Pro, queries/s):

| workload | g01 | peak | shape | per-query |
|---|---|---|---|---|
| `SELECT 0` raw (parse+plan+convert+exec) | 35.8K | ~113K (~3.2×) | plateau by g04 | 27.9 µs · 177 alloc · 58 KB |
| `SELECT 0` prepared (exec + Session only) | 607K | ~1.55M (~2.5×) | peak g08, declines | 1.6 µs · 49 alloc · 2.9 KB |

1. **Parse+plan is ~94% of a trivial query's cost.** Raw `SELECT 0` is 27.9 µs; the *same* query
   prepared once is 1.6 µs — so cbq's n1ql parse + planner + n1k1 convert is ~26 µs and **128 of the
   177 allocs / 55 of the 58 KB**, for literally `SELECT 0`. That boxed front-end is the dominant
   per-query *latency*, dwarfing execution of a small query.
2. **A ~2.5–3× concurrency ceiling remains with zero scan AND zero planner.** Prepared `SELECT 0`
   touches no file and no planner (immutable shared plan) — pure `Session`/`PlanExec`/convert
   plumbing — yet it still peaks ~2.5× at g08 and *declines* past it. So a second ceiling lives in
   the per-query execute+Session path, independent of the known scan and planner-globals limits.
3. **That ceiling is allocation-rate-bound, not pure GC.** A `GOGC=off` A/B on prepared `SELECT 0`
   *raised* mid-range throughput (g04 1.34M → 1.84M, +37%) but *collapsed* at high concurrency
   (g32 1.43M → 519K — heap bloat hits the memory-bandwidth/allocator wall). So GC is a real
   mid-range tax, but the wall is the allocation *rate* itself; the lever is **fewer allocations per
   query** (even prepared `SELECT 0` does 49), not GC tuning. The one workload that scaled well
   (file-less `UNNEST`, ~6.6×) is the one whose genuine per-query *compute* dwarfs this shared
   per-query overhead — confirming the engine's compute path parallelizes; the fixed per-query
   overhead (parse/plan + allocation) is what doesn't.

Reproduce: `go test -tags n1ql -run=^$ -bench BenchmarkConcurrent -benchtime=500ms ./test/benchmark`
(add `-cpuprofile`/`-memprofile`/`-mutexprofile` for the profiles; run WITHOUT `-race` — the fork
planner pool (4a) still trips it).
