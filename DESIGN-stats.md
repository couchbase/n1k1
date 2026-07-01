# Design: Live Stats & Progress Reporting

Status: proposal / for review

Many good CLI/data tools show live-ish stats and progress while they work —
rows/bytes processed, throughput, % complete, ETA — during queries, ingest,
indexing, and data transfer. This document designs how n1k1 supports that:
**optional**, **library-user-first**, and **lightweight / high-performance** (the
hot path must stay nearly free).

The easy part is aggregating everything and reporting once at the end (progress =
100% by definition). The hard part — and the focus here — is delivering
occasional, cheap updates to whoever asked, *without* slowing the pipeline.

## What n1k1 already has (a skeleton wired for exactly this)

This is not greenfield. The plumbing is scaffolded:
- **`base.Stats struct{}`** (`base/base.go:310`) — currently empty (`// TODO`).
- **`type YieldStats func(*Stats) error`** (`base/base.go:308`) — the callback an
  op invokes "occasionally … to yield stats and progress information," documented
  as concurrent-safe, and whose **error return doubles as early-exit / abort**
  (e.g. `LIMIT`, cancellation).
- **`Ctx.YieldStats`** (`base/vars.go:82`) — set per request; "may be invoked
  concurrently by multiple goroutines."
- **Throttling already present:** `engine/op_scan.go` calls
  `lzVars.Ctx.YieldStats(&lzStats)` **every `ScanYieldStatsEvery` (=1024) rows**;
  `op_order.go` / `op_window.go` have TODO notes for the same checkpoint.
- **A no-op sink is wired** in `glue/exec.go` (`YieldStats: func(*Stats) error {
  return nil }`).

So the model is already a **throttled, per-operator push** with a
cancellation-carrying return. The work is: (1) define `Stats`, (2) make collection
cheap, (3) choose how the client is notified, (4) give library users a clean API.

## Core principle: two decoupled frequencies

The single most important idea: **separate measurement cadence from reporting
cadence.**
- **Measurement** happens on the per-row hot path and must be ~free (a local
  counter increment, no atomics, no allocs, no `time.Now()`).
- **Reporting** happens at a coarse cadence — **~10×/second** (ClickHouse caps
  progress at ≤10/s; DuckDB only shows a bar after a query exceeds ~2s). Delivery,
  rate math, and rendering all live here, off the hot path.

n1k1's existing per-1024-row throttle is the seam between the two. Never call a
user callback or compute a rate per row.

## What to measure — the `Stats` struct

Keep it **flat, fixed-size, copyable** (counters, not maps, on the hot path):
- **Universal:** `RowsIn` / `RowsOut`, `Bytes`, `Elapsed`, and derived
  `RowsPerSec` / `BytesPerSec`, `Percent`, `ETA` (computed at report time from
  deltas, not stored per row).
- **Progress needs a denominator.** A `%`/ETA requires a known total: source file
  sizes, or the manifest `doc_count` / partition counts from `DESIGN-data.md §5`
  when available; otherwise progress is **indeterminate** (spinner, not bar) — cf.
  ClickHouse's `total_rows_approx`.
- **Phase-tagged** (`Phase` enum): `query` (rows scanned/filtered/grouped, spill
  bytes), `ingest` (files, bytes, docs), `index` (docs indexed, index bytes,
  spill), `transfer` (bytes in/out).
- **Per-operator vs rolled-up:** support both — per-op counters that **roll up**
  to a pipeline total (DuckDB exposes `GetProgress()` per source; ClickHouse shows
  one rolled-up number). A stable op id/label keys each slot.

## Collection: counting cheaply on the hot path

- **Per-op / per-actor local `uint64` counters** incremented in the yield path.
  Because each `base.Stage` actor runs in its own goroutine, per-actor counters
  need **no atomics and have no contention**.
- **Roll up lazily**, only at the coarse cadence: either the reporter reads each
  actor's counters, or actors flush their local deltas into a shared aggregate at
  the checkpoint. Use atomics only for the shared aggregate, touched ~10×/s, never
  per row.
- **Avoid** per-row `time.Now()`, per-row allocations, and per-row atomics. Sample
  the clock once per checkpoint. (Also fix the skeleton's per-checkpoint
  `var lzStats base.Stats` allocation — pass a pointer to the op's persistent
  counters instead of allocating.)

## Delivery: getting stats to the client — the approaches

These differ only in **how the client is notified**; all share one counter core.

### (a) Pull / polling snapshot — *recommended default*
The engine keeps a shared `*Stats` of atomic counters; the library user calls
`Snapshot()` from **their own goroutine on their own ticker** (e.g. 100 ms). The
hot path only bumps counters.
- **Pros:** zero coupling, no backpressure, client owns the cadence, dead simple,
  cheapest. Snapshot = per-field atomic loads (slightly skewed but monotonic →
  fine for progress). This is how `pv`/`rsync`/most tools work (shared counters +
  a reader ticker).
- **Cons:** the client must run a ticker (trivial).

### (b) Throttled push callback — *the existing `YieldStats`, refined*
The user registers `Progress func(*Stats)`; the engine calls it, but **only at
coarse boundaries** (every N rows / per batch / and ≥T since last call).
- **Pros:** user gets updates without managing a goroutine; natural for "print a
  status line occasionally."
- **Cons:** it runs **in the execution goroutine**, so the callback **must be fast
  and non-blocking** (copy the snapshot or do a non-blocking send) — a slow
  callback stalls the pipeline. Today each op calls the user hook directly;
  **refine** to (i) throttle by *both* count and wall-time, and (ii) route through
  one central lightweight sink rather than every op calling the user's render
  function. Keep the error-return = cancellation contract.

### (c) Channel / latest-wins — *clean push↔pull bridge*
The engine does a **non-blocking send** of a snapshot onto a `chan *Stats` of
**capacity 1** (coalescing: if full, drop and keep newest). The consumer reads at
its own pace and renders.
- **Pros:** decouples producer/consumer, no backpressure, auto-throttles to
  consumer speed, ideal for a CLI render goroutine.
- **Cons:** one more concept; snapshots must be immutable once sent.

### (d) expvar / metrics exporter — *for servers/observability*
Publish counters via stdlib **`expvar`** or **Prometheus** (already a dep, but
pull-scrape and heavier). Good for long-running/server contexts, overkill for an
interactive CLI. Offer as an optional add-on over the same core.

**Recommendation:** make **(a) pull-snapshot the primary library API**, backed by
one atomic counter core. Provide a small internal **reporter goroutine** that (for
the CLI) reads snapshots on a ~10 Hz ticker and renders. Offer **(b) throttled
callback** and **(c) latest-wins channel** as thin opt-in wrappers over the same
core. Keep the per-op `YieldStats` checkpoint as the **cancellation + flush**
point, with its default sink being a cheap counter merge (not the user's renderer).

## In-flight / partial results (the "spinning numbers" effect)

Beyond progress *metrics*, it's compelling to show partial **result values** as
they build — running `COUNT`/`SUM`/`MIN`/`MAX` per group ticking up toward their
finals, an `ORDER BY … LIMIT` leaderboard reshuffling. The goal is *perception*
("wow, it's working"), not completeness — a bounded sample at ~10 Hz, allowed to
be approximate and eventually-correct, never mistaken for the real answer.

### Which operators can preview, and what
- **Ungrouped aggregate** (`SELECT COUNT(*), SUM(x)`): the accumulator is a couple
  of scalars — trivially cheap to publish every checkpoint via atomics. This is
  the best/cheapest demo case.
- **`GROUP BY`** (blocking, but its accumulators are inspectable): publish a
  **bounded sample** of the current group map — first-N or top-N-by-current-value
  (N ~ 10–50) — not all groups (which could be millions). `COUNT`/`SUM`(≥0)/`MAX`
  rise monotonically and `MIN` falls, so they animate nicely toward the final.
- **`ORDER BY … LIMIT`** (top-K heap): the current top-K *is* a meaningful evolving
  preview — a satisfying "live leaderboard."
- **Not previewable as an answer:** full `ORDER BY`, `DISTINCT`, final
  projections — their intermediate buffer isn't a valid partial result. Expose
  only cardinality/progress for these, and **flag the payload so the UI never
  renders it as data**.
- **Streaming ops** (filter/project) already yield rows to the client
  incrementally via the normal `YieldVals` path — the preview concept matters
  mainly for **blocking** ops that otherwise show nothing until the end.

### How to read partial state safely and cheaply
The aggregation lives in the rhmap group map, mutated by the exec goroutine(s); a
naive concurrent read during a resize is a data race. The safe, free trick:
**snapshot at the existing per-checkpoint `YieldStats` call, which already runs
synchronously on the exec goroutine** — at that instant the map isn't being
mutated, so copying a bounded N-row sample into an **immutable snapshot** is
race-free and O(N). No locks on the hot path. For parallel `GROUP BY` (per-actor
maps merged at the end), each actor previews its own shard; the reporter shows one
shard or a merged sample — perception-level accuracy is fine.

### Reuse the same delivery + payload
A partial-result preview rides the **same** pull-snapshot / throttled-callback /
latest-wins-channel machinery as progress stats — it's just a richer payload:
`Stats` (or a sibling `Preview`) carries an optional bounded `[]PartialRow` marked
`Partial: true`. Bounded sample × ~10 Hz = negligible cost, and it's opt-in.

### Animation is render-side
The core emits *real* partial numbers at ~10 Hz; the "spinning" tween between
frames (easing from the last value to the new one) is a pure TUI concern
(pterm/mpb), keeping the engine dumb. Solidify/undim at 100 %.

## Cooperative cancellation piggybacks on the same checkpoint
`YieldStats` already returns an error used for early exit (LIMIT). Keep this: the
coarse checkpoint that flushes stats is also where we check `ctx.Done()` / an abort
flag and return an error to unwind. **One mechanism serves both progress and
cancellation** — a nice property to preserve as the codegen/`intermed` path is
regenerated (the `lz` checkpoints must survive codegen).

## Library-user API (sketch)
Opt-in, zero-cost when off (nil-check the hook exactly as `op_scan.go` already
does; gate counters so an unobserved run pays ~nothing):
```go
// Pull (default):
res, stats := sess.RunObserved(ctx, stmt)          // stats is a live handle
go func() {
  for range time.Tick(100 * time.Millisecond) { render(stats.Snapshot()) }
}()

// Push (throttled callback):
sess.Run(ctx, stmt, glue.WithProgress(func(s *base.Stats) { printLine(s) },
                                      glue.Every(100*time.Millisecond)))

// Channel (latest-wins):
ch := make(chan *base.Stats, 1)
sess.Run(ctx, stmt, glue.WithProgressChan(ch))
```
- Expose both a **rolled-up** snapshot and, optionally, the **per-op tree**.
- `Phase` lets ingest/index/transfer/query share one API.

## Rendering stays out of the core
The core only emits `*Stats` snapshots; rendering is a separate concern. For the
CLI use **`pterm`** (already a dep) or **`mpb`** for bars/spinners/ETA. Adopt the
good UX defaults: DuckDB's "only show a bar after ~2 s" and ClickHouse's "≤10
updates/s, skip entirely for quick queries."

## Multi-phase pipelines (ingest / index / transfer)
These long-running operations benefit most (queries are often sub-second). Same
core; denominators come from source file sizes and the `DESIGN-data.md §5`
manifest (`doc_count`, byte totals). Report the current item (e.g. the file being
indexed) plus overall %.

## Dependency licensing (permissive only)
- `pterm` — MIT (already a dep); `vbauerster/mpb` — Unlicense (public domain);
  `expvar` / `sync/atomic` — stdlib (BSD); Prometheus client — Apache-2.0 (already
  a dep). All fit the no-GPL/AGPL policy (see `DESIGN-data.md`).

## Open questions
- **Per-op tree vs single rolled-up number** as the default surface — cost vs
  usefulness.
- **Snapshot consistency:** accept per-field skew (monotonic counters) vs a
  seqlock/double-buffer for a coherent snapshot.
- **Where the reporter goroutine lives** — engine, glue, or CLI only.
- **Denominators:** how eagerly to compute totals (file sizes now; manifest
  `doc_count` when the `DESIGN-data.md` work lands) vs indeterminate progress.
- **Codegen path:** ensure the `intermed`/`lz` compiled path preserves the
  `YieldStats` checkpoints and their cheap-counter semantics.
- **Partial-result sampling policy:** first-N vs top-N-by-value vs a fixed watched
  set; and how firmly to guard previews from being consumed as final results.

## Prior art
- DuckDB progress bar (`enable_progress_bar`, ~2 s threshold, per-source
  `GetProgress()`, C API `duckdb_query_progress`):
  https://duckdb.org/docs/current/configuration/overview ,
  https://github.com/duckdb/duckdb/pull/1432
- ClickHouse client live progress (≤10/s, rows/bytes/time, `total_rows_approx`,
  `execute_with_progress`, `system.processes`):
  https://clickhouse.com/docs/interfaces/client
- Go rendering: `pterm` https://pterm.sh/ , `vbauerster/mpb`
  https://github.com/vbauerster/mpb
- Go metrics primitives: `expvar` (stdlib), `sync/atomic`
  https://gobyexample.com/atomic-counters , Prometheus client
  https://pkg.go.dev/github.com/prometheus/client_golang/prometheus
- Online Aggregation (Hellerstein, Haas & Wang, SIGMOD 1997) — running aggregate
  estimates that refine over time; the academic root of "watch the numbers
  converge" (we do the perception-level version, not statistical confidence
  intervals): https://dl.acm.org/doi/10.1145/253260.253291
