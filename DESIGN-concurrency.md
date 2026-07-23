# DESIGN-concurrency: n1k1 under concurrent workloads

**Question.** If n1k1 one day grows a listen port and serves clients goroutine-per-connection,
does it fall over?

**Short answer.** Not gracefully — *today*. Functionally it returns correct results even under
heavy contention (see `glue/concurrency_test.go`), but that's luck, not safety: `go test -race`
shows genuine data races on **process-global state that `Session.Run` mutates on every query**.
n1k1 is deliberately architected as a single-process, one-query-at-a-time CLI engine. Serving
concurrent clients needs a bounded, well-understood set of changes — none of them huge, one of
them (the `ExecOpEx` hook) a real refactor.

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
| 1 | `engine.ExecOpEx` (IoC hook) | swapped `glue/session.go`, read `engine/op.go:102` | **HARD — open** | The engine reads this global on EVERY datastore op. `Run` swaps it to `DatastoreOp` and restores via `defer`. Concurrent runs can restore a stale/nil value mid-flight in another goroutine -> ops routed to the wrong handler or a nil call. Confirmed still the sole `PlanExec`/`ExecOp` race after fixing 2-3. |
| 2 | `engine.ExprCatalog` (map) | was lazy init in `Run`/`PlanExec`/corpus | ✅ **FIXED** | Was a check-then-set on a shared map from many goroutines -> concurrent map write (a runtime PANIC). Now registered ONCE in `glue` `init()` (`expr.go`); read-only during serving. |
| 3 | `datastore.SetDatastore` | was every `Run` (`session.go`) + corpus | ✅ **FIXED** | Was a write-write race on the global datastore. Now `ensureDatastore` (`stmt.go`) writes only when the global isn't already this store's datastore -- so in the one-store model (set once at `InitParser`) every concurrent `Run` just READS it. Race-free confirmed. |
| 4 | assorted process-global caches | `glue/idx_si.go:75,734`, `glue/datastore_scan.go` (`ScanWalkOptions`, scan caches); cbq planner internals seen in `PlanStatementQP`/`planner.Build` | AUDIT | Comments already flag these "process-global … fine for the single-process CLI"; each needs a read-only-during-serving audit or a mutex/per-Store move. A concurrent-plan-build race in the cbq fork may also live here. |

The design *knows* this: `datastore_scan.go:301` and `idx_si.go:75` literally say "process-global
… fine for the single-process CLI; a [server would need more]".

**Status:** blockers 2 & 3 are FIXED (commit trailer below); the goroutine-per-client stress test
now shows only blocker 1 (`ExecOpEx`) and the blocker-4 planner globals under `-race`.

## Path to a concurrent server

Roughly in increasing effort:

1. ✅ **`ExprCatalog` (blocker 2) — DONE:** `exprStr`/`exprTree` are registered once in `glue`
   `init()` (`expr.go`) instead of lazily per `Run`; the shared map is read-only during serving.
2. ✅ **`SetDatastore` (blocker 3) — DONE:** `ensureDatastore` (`stmt.go`) sets the global only
   when it isn't already this store's datastore — in the shared-store model (set once at
   `InitParser`) no write happens during serving, so concurrent reads are race-free.
   (Cross-store concurrency stays unsupported by construction.)
3. **`ExecOpEx` (blocker 1) — the real work, OPEN:** stop swapping a global. The per-`Ctx` seam already
   half-exists: `base.DatastorePipe` + `vars.Ctx.Pipe` route the data *source* per query. Extend
   that so `engine.ExecOp` dispatches the "extra op kinds" through a per-`Vars`/`Ctx` handler
   rather than the `engine.ExecOpEx` package var — then each run carries its own `DatastoreOp`
   binding and nothing global is swapped. Touches the engine's op-dispatch core, so it needs care
   + the differential/compiler tests.
4. **Global caches (blocker 4):** audit `idx_si` + `datastore_scan` globals; make them per-`Store`
   or guard them.

**Cheaper interim options** if a server is wanted before the `ExecOpEx` refactor: (a) a single
global lock serializing the plan+exec window (correct, but serializes — throughput = 1 core); or
(b) process-per-request, which n1k1 *already does* for standalone-compiled `EXECUTE` (a child
process per query) — a natural fit for its Futamura-projection compile path.

## Verdict

n1k1 is **not yet** goroutine-per-client safe, by deliberate single-process design — it won't
silently corrupt in the CLI (one query at a time). The gap is a *small, enumerated* set of
process globals, not a pervasive thread-unsafety. Blockers 2 & 3 are now FIXED (`ExprCatalog`
panic risk and the `SetDatastore` race are gone); the remaining barrier to a concurrent server
is blocker 1 (`ExecOpEx`, the one architectural change) plus the blocker-4 audit (incl. any
cbq-planner concurrent-build race). Reproducer + guardrail: `glue/concurrency_test.go` (passes
functionally under contention; skips under `-race` until blocker 1 lands).
