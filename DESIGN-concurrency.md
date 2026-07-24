# DESIGN-concurrency: n1k1 under concurrent workloads

**Question.** If n1k1 one day grows a listen port and serves clients goroutine-per-connection,
does it fall over?

**Short answer (updated).** n1k1 was architected as a single-process, one-query-at-a-time CLI
engine, and originally `Session.Run` mutated three process-globals per query that raced under
concurrency. Those are now all FIXED (`ExprCatalog`, `SetDatastore`, `ExecOpEx` — below), and
n1k1's own engine path is race-clean under `-race` (the stress test's JSON variant passes). The
ONE remaining `-race` failure is not in n1k1 but in the cbq FORK's planner, whose global object
pools race during concurrent plan-building — a fork patch. So: n1k1-side, ready; fork planner
pool, one patch away. Reproducer/guardrail: `glue/concurrency_test.go`.

## The only viable model: one data root, one Store, N sessions

The cbq planner resolves keyspaces through a **process-global datastore singleton**
(`datastore.GetDatastore`), so a server serves ONE data root. The shape is:

```
FileStore(dir) once  ->  store.InitParser() once (sets the global datastore)
   -> per connection: sess := &Session{Store: store, Namespace: "default"}
   -> goroutine per connection calls sess.Run(stmt) in a loop
```

A `Session` is single-query-at-a-time by design (`halt` resets each `Run`); concurrency is
*across* sessions that share the one store + the global planner/engine hooks. Multiple *stores*
concurrently is not supportable (they'd fight over the global datastore) and isn't the model.

## What is already concurrency-safe (the good news)

- **Per-request state is per-request.** `GlueContext` is built fresh each `Run`
  (`NewGlueContext`), `base.Vars`/`Ctx` are per-run, the scan key/file caches live on the
  per-run `GlueContext`, and `Session.halt` is accessed with `sync/atomic`.
- **The records read path is per-scan.** Each scan opens its own `records.Source`; Iceberg
  `OpenIcebergTable`/`PlanFiles`/`ToArrowRecords` read immutable snapshot files. The flat
  namespace's Iceberg time-travel cache is mutex-guarded (`flatNamespace.mu`). Observability
  counters (`IcebergProjectionApplied`, `ColumnProjectionApplied`, …) are atomic.
- **Prior races already fixed** (see the race-fix history): the glue `corrParent`/`withScope`
  shared-context race (per-actor `base.ChainCloner`), and cbq's `LocklessPool` non-atomic
  cursors (forked patch). So the *executor* internals are in reasonable shape.

## The blockers: process-global mutation per query

All in the hot `Session.Run` -> `PlanExec` path. Each is a real `-race` finding.

| # | Global | Where | Severity | Consequence under load |
|---|--------|-------|----------|------------------------|
| 1 | `engine.ExecOpEx` (IoC hook) | was swapped `glue/session.go`, read `engine/op.go:102` | ✅ **FIXED** | The engine reads this global on EVERY datastore op; `Run` used to swap it to `DatastoreOp` + restore via `defer` (a write-write race). But it is ALWAYS `DatastoreOp` -- per-request source variation rides `Ctx.Pipe` *inside* `DatastoreOp`, so one global handler serves every request. Now set ONCE in `glue` `init()` (`expr.go`); no per-run swap. The engine-level `Ctx.OpEx` refactor turned out unnecessary. |
| 2 | `engine.ExprCatalog` (map) | was lazy init in `Run`/`PlanExec`/corpus | ✅ **FIXED** | Was a check-then-set on a shared map from many goroutines -> concurrent map write (a runtime PANIC). Now registered ONCE in `glue` `init()` (`expr.go`); read-only during serving. |
| 3 | `datastore.SetDatastore` | was every `Run` (`session.go`) + corpus | ✅ **FIXED** | Was a write-write race on the global datastore. Now `ensureDatastore` (`stmt.go`) writes only when the global isn't already this store's datastore -- so in the one-store model (set once at `InitParser`) every concurrent `Run` just READS it. Race-free confirmed. |
| 4a | cbq FORK planner global object pools | `planner._COVERING_ENTRY_POOL` (a package `var`) via `util.FastPool`/`poolList` (`util/sync.go`), hit in `buildCoveringScan` | **OPEN — fork** | With 1-3 fixed, this is the race the Iceberg stress variant hits under `-race`: concurrent plan builds share the planner's global covering-entry pool, and its lockless fast-path isn't goroutine-safe (same class as the previously-patched cbq `LocklessPool`). Intermittent (can hit ANY concurrent `buildCoveringScan`). Needs a FORK patch (patch -> republish -> go.mod re-pin), not an n1k1 change. |
| 4b | n1k1 process-global caches | `glue/idx_si.go:75,734`, `glue/datastore_scan.go` (`ScanWalkOptions`, scan caches) | AUDIT | Comments already flag these "process-global … fine for the single-process CLI". Not exercised by the current stress test (no secondary indexes); each needs a read-only-during-serving audit or a mutex/per-Store move. |

The design *knows* this: `datastore_scan.go:301` and `idx_si.go:75` literally say "process-global
… fine for the single-process CLI; a [server would need more]".

**Status:** blockers 1, 2 & 3 are FIXED (n1k1-level; the goroutine-per-client stress test's JSON
variant is race-clean). The remaining `-race` failure is blocker 4a -- a race in the cbq FORK's
global planner pools, which needs a fork patch.

## Path to a concurrent server

Roughly in increasing effort:

1. ✅ **`ExprCatalog` (blocker 2) — DONE:** `exprStr`/`exprTree` are registered once in `glue`
   `init()` (`expr.go`) instead of lazily per `Run`; the shared map is read-only during serving.
2. ✅ **`SetDatastore` (blocker 3) — DONE:** `ensureDatastore` (`stmt.go`) sets the global only
   when it isn't already this store's datastore — in the shared-store model (set once at
   `InitParser`) no write happens during serving, so concurrent reads are race-free.
   (Cross-store concurrency stays unsupported by construction.)
3. ✅ **`ExecOpEx` (blocker 1) — DONE:** it turned out NOT to need the per-`Ctx` engine refactor:
   the global is always `DatastoreOp`, and per-request source variation already rides `Ctx.Pipe`
   *inside* `DatastoreOp`. So it is set ONCE in `init()` and the per-run swap+restore is deleted
   (`session.go`/`corpus.go`/`compiled_exec.go`). No engine op-dispatch change.
4. **Blocker 4a — cbq fork planner pools (OPEN, the remaining `-race` failure):** `_COVERING_ENTRY_POOL`
   (and peers) are global `util.FastPool`s the planner shares across concurrent builds; their
   lockless fast-path races. Fix in the FORK (a mutex / per-request pool, like the prior
   `LocklessPool` patch-03), then republish + re-pin go.mod. **Blocker 4b:** audit the n1k1
   `idx_si`/`datastore_scan` globals (not yet stress-tested).

**Cheaper interim options** if a server is wanted before the fork planner-pool fix: (a) a single
lock serializing just the PLAN step (execution is already race-clean); or (b) process-per-request,
which n1k1 *already does* for standalone-compiled `EXECUTE` (a child process per query) — a natural
fit for its Futamura-projection compile path.

## Verdict

n1k1's OWN engine is now goroutine-per-client race-clean: the three per-query process-global
mutations (`ExprCatalog`, `SetDatastore`, `ExecOpEx`) are all fixed, and the stress test's JSON
variant passes under `-race`. The remaining barrier is NOT in n1k1 but in the cbq FORK's planner:
global object pools (`_COVERING_ENTRY_POOL` via `util.FastPool`) race during concurrent
plan-building (blocker 4a) — a fork patch. Until then a server could serialize just the plan step
(execution is safe) or go process-per-request. Reproducer + guardrail: `glue/concurrency_test.go`
(passes functionally under contention; skips under `-race` until the fork planner pools are fixed).

## Throughput scaling (measured)

`test/benchmark/bench_concurrency_test.go` (`make bench-concurrency`) ramps concurrent clients
over one shared Store and reports queries/s. Throughput rises from G=1 but **peaks around G=4 at
only ~2–2.5× single-threaded, then plateaus and erodes** — nowhere near the ~12× a contention-free
CPU-bound workload would give on 12 cores.

The PREPARE/EXECUTE variants (`bench_concurrency_prepared_test.go`) locate the ceiling. A prepared
workload skips parse+plan entirely (a single immutable `*PreparedPlan`, built once, is safely
shared across goroutines via `PlanExec` — verified race-clean by `TestConcurrentSharedPreparedPlanRace`).
It runs faster in absolute terms (~½ the allocs, ~10–30% higher throughput) — but its scaling curve
is the SAME: peak ~2× at G=4, then decline. So the ceiling is NOT the planner (blocker 4a) or the
parser: removing them doesn't lift it. It's the shared EXECUTION substrate — GC pressure from
per-query allocations and per-query datastore-scan file I/O (syscalls), both shared across
goroutines. blocker 4a stays a `-race` correctness item + a constant-factor cost, NOT the main
throughput ceiling. Lifting the ceiling means cutting per-query allocations (GC) and the
per-request file-walk/open cost (e.g. a cached dir listing or a columnar single-file source).
Run without `-race` (throughput-only; the prepared warm-up still touches blocker 4a).
