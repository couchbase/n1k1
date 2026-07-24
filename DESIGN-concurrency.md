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

**Secondary per-query costs** (visible once the file-scan is removed): `MakeVars` eagerly creates a
temp dir (`mkdir`/`rmdir`) on every `PlanExec` even for a query that can't spill; and GROUP BY /
ORDER / hash-join / window eagerly allocate the rhmap store's `StartSize`(=5303)-slot slots buffer at
op init, before any row (an in-memory ~tens-of-KB heap alloc — a real mmap'd *file* appears only on
grow/spill past ~4000 keys, so the mmap is lazy; the eager cost is the buffer + GC).

**Levers to lift the ceiling** (none is the planner): a syscall-light layout — single-file / columnar
Parquet, one open per scan (cf. the `parallel-scan-experiment` memo: file-per-doc packed to one file
was ~245× faster); a lazily-created `MakeVars` temp dir (only when a query actually spills); a
smaller or lazily-grown group/order store for small aggregations.

Reproduce: `go test -tags n1ql -run=^$ -bench BenchmarkConcurrent -benchtime=500ms ./test/benchmark`
(add `-cpuprofile`/`-memprofile`/`-mutexprofile` for the profiles; run WITHOUT `-race` — the fork
planner pool (4a) still trips it).
