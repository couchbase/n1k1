# Design: Live Stats & Progress Reporting

## Status & remaining TODOs

_Last reviewed: 2026-07-23._

An **opt-in, lightweight, library-user-first** per-operator counter core with a live CLI
footer. Core idea: **decouple measurement cadence from reporting cadence** — count on the
per-row hot path for ~free, deliver/rate-math/render only at a coarse (~10 Hz) cadence off
the hot path.

**Done** (interpreter-only — the compiled path is `genCompiler:hide`, see KNOWN LIMITATION):
the counter core (`base/stats.go`: a flat `[]int64` keyed by op id, `StatsDescs`/`DefStat`/
`LayoutStats`/`Op.StatsBase`/`Ctx.Stats`), instrumented across scan/filter/group/distinct/
order-offset-limit/join-nest-unnest + the glue datastore scans; three CLI modes
(`off`/`on`/`final`) with a throttled ~10 Hz columnar footer + a process `runtime:` line +
a `YieldPacer` (`glue/stats.go`); live running-aggregate partials (COUNT/SUM/AVG/MIN/MAX +
`_v` forms) that climb race-safe across concurrent `UNION ALL` actors with zero per-row/
per-snapshot alloc (`base/stats_running.go`, `glue/stats_running.go`, `glue/stats_snapshot.go`);
and the corpus `RunReport` (per-keyspace scanned rows + per-detector woken counts).

**Remaining (headline TODOs):**
- [ ] Re-enable counters on the **compiled** path (`genCompiler:hide` today — see KNOWN
  LIMITATION).
- [ ] Instrument `project`/`union`/`window` + hash-join counters.
- [ ] `BytesIn` + pruning counters (`FilesOpened`/`FilesPruned`, bytes skipped) — needs the
  `DESIGN-data.md §5` manifest / zone maps.
- [ ] Richer denominators (planner cardinality, manifest doc/byte counts).
- [ ] `StatKind` (Counter/Gauge/Peak) descriptor marker.
- [ ] Heavy running aggregates (MEDIAN/VARIANCE/STDDEV/ARRAY_AGG/DISTINCT) — progress-only,
  coarse-cadence alloc, or approximate (HLL / streaming quantile).
- [ ] The richer UI surface (detail dial `off|auto|min|rich|debug`, view selector, plan-flow
  viz, DVR/replay, racing bars, `EXPLAIN PRICE`/`COST`) — all future; see [Future](#future).

## Core principle: two decoupled cadences

- **Measurement** is on the per-row hot path and must be ~free: a **local** counter `++` —
  no atomics, no allocs, no `time.Now()`.
- **Reporting** is coarse — **~10 Hz** (ClickHouse caps progress at ≤10/s; DuckDB shows a bar
  only after ~2 s). Delivery, rate math, rendering all live here.

n1k1's existing per-1024-row `YieldStats` throttle (`engine/op_scan.go`,
`ScanYieldStatsEvery=1024`) is the seam between the two. ⚠ **Never call a user callback or
compute a rate per row.** The `YieldStats` **error return doubles as early-exit / cancellation**
(`LIMIT`, ctx-cancel) — one mechanism serves both progress and abort.

### Concurrency: single-writer, no atomics

Each `base.Stage` actor runs in its own goroutine, and counters are **single-writer**: a
scan→filter→group pipeline is one goroutine, and only `Stage`/`UNION` actors split goroutines
— onto *distinct* subtrees → distinct counter sections. So plain `=`/`++` is safe and **no
atomics are needed today** (atomics-per-row + false sharing would erase the parallelism they
measure). ⚠ **If same-op parallelism ever lands** (parallel scans / GROUP BY shards), add a
per-`(op, actor)` dimension (array size `= Σ (numStats × numActors)`, roll up at report time)
**before relying on the counts** — `NumActors` is known at stage setup, so allocation stays
one-shot and the hot path is unchanged. The only concurrent reader is the ~10 Hz reporter, for
which monotonic per-field skew is acceptable (no seqlock).

## The counter core

Every op contributes a known set of `int64` counters in one pre-sized `[]int64`; a
`map[string]int` maps `"opId:statName"` → index. Each op owns a contiguous sub-slice.

- **The op id already exists** — `ExecOp` threads a unique `path` (`_0_1_2`) via `EmitPush`;
  it survives into the compiled path (codegen mints unique var names from it), so one key
  works in both builds.
- **Layout computed once, at convert time** (`LayoutStats`): a plan walk sums each op kind's
  `StatsDesc` into total `N`, allocs one `make([]int64, N)`, builds the index map, assigns
  `Op.StatsBase`. Non-`lz`, never on the hot path.
- **The array lives on `base.Ctx`** (per-request, concurrent-safe, shared across every actor's
  cloned `Vars`).
- **Per-row is a local `++`; flush at the checkpoint.** The 1024-row `YieldStats` checkpoint
  runs synchronously on the actor's own goroutine, where the actor flushes its local deltas
  into its shared section.
- ⚠ **The index is compile-time, never a per-row map lookup.** Resolve `"opId:statName" →
  base offset` at convert time, bake the integer `base` into the op closure; the hot path is
  `Counters[base+RowsOut]++`. This is what lets the scheme survive codegen: `base` is an
  ordinary int in the non-`lz` setup block, only the increment + checkpoint flush cross into `lz`.

**Single source of truth:** every counter is one `base.DefStat(name, about, kinds…)` line
(`engine/stats.go`, `glue/stats.go`) defining the offset constant + registered name +
one-line description, so they can't drift. List all: `git grep '= base.DefStat'`. `DefStat`
is idempotent (the compiled path's re-run of initializers doesn't double-register); the full
glossary is populated before `main` (`.stats about`).

**Naming convention:** noun-first (`RowsIn`/`RowsOut`/`BytesIn`, so a subsystem's stats
cluster, not `InRows`); **monotonic is the unmarked default**; a current level takes a `Cur`
suffix, a high-water mark `Peak` (suffix, not prefix, keeps the noun leading). A future
`StatKind` (Counter/Gauge/Peak) descriptor lets tooling pick rendering without parsing the
suffix.

## Codegen safety (dev notes)

n1k1 ops are **dual-mode source** (interpreter + `cmd/intermed_build`). To keep a counter
working in both:
- **`lz`-prefix drives emission.** A line assigning an `lz` var from a non-`lz` expression is
  emitted with the value **baked as a literal** — how `base` becomes a compile-time constant.
  Write `lzStatsBase := o.StatsBase`, index `Counters[lzStatsBase+StatFooBar]`. **Never** index
  by a non-`lz` var in emitted code; **never** do a per-row map lookup.
- **Per-row increments go on a local `lz` counter** (`lzStatRowsOut++`), flushed into the
  shared slot only at a coarse point, guarded by `if lzVars != nil && lzVars.Ctx != nil &&
  lzVars.Ctx.Stats != nil { … }`.
- **Signature changes ripple through regeneration**, not by hand (`// !lz` call sites re-emit
  on re-run); the generated file is gitignored. Always regenerate + compile + run the suite in
  both modes — the suite is the only check that exercises the *compiled* counters.

> ⚠ **KNOWN LIMITATION — compiled path currently has NO stats (TODO).** The counter lines are
> `// <== genCompiler:hide`, so `cmd/intermed_build` omits them. Root cause: `test/emit.OpToLines`
> inlines the whole op tree into one function, and a per-op local counter (`lzStatRowsOut`)
> incremented inside the yield closure gets **cleared** when that closure is inlined at a
> child's call site (`clearFuncLines` keeps only lifted `var X = Y` idioms) → `undefined:
> lzStatRowsOut`. Naive fixes surface two more gaps: `var X = 0` collides across sibling ops,
> and the flush line carries *two* lifted vars which `varLift` mis-aligns. So stats are
> interpreter-only for now (the CLI's live progress runs on the interpreter → no user-visible
> loss). TO RE-ENABLE: drop the markers, give each counter a path-unique name via `// <==
> varLift: lzStat… by path`, and teach `varLift` to align format args when several lifted vars
> share one line. **Scans are exempt** — they compile to a `glue.DatastoreOp` island and
> `countingYield` tracks their rows out in both modes.

## What's instrumented

All monotonic: `scan`→`RowsOut` (live, flushed at the checkpoint; `YieldStats` now receives
the shared `Ctx.Stats`, no per-checkpoint alloc); `filter`→`RowsIn`/`RowsOut`; `group`/
`distinct`→`RowsIn`/`GroupsOut`; `order-offset-limit`→`RowsIn`/`RowsOut`; the NL join/nest/
unnest family→`RowsLeft`/`Probes` (the exploding-join signal). The **glue datastore scans**
(where the CLI's real file reads go, not `OpScan`) get `RowsOut` via `countingYield`
(`glue/stats.go`), driving a live checkpoint every 1024 rows.

The render trigger fires at each **scan-invocation boundary** (a pulse in `DatastoreOp`), not
only every N rows — essential because a NL join's inner scan yields a handful of rows per
pass, under the checkpoint interval. Counters reset per op invocation (`statZero`), so a
re-run inner subtree restarts while a single-run op climbs cumulatively.

The three CLI modes (`-stats`/`.stats`): `off` (default, zero cost, nil `Ctx.Stats`); `on`
(collect + live footer, throttled ~10 Hz in place on stderr, + final totals); `final`
(collect, grand totals once at the end, no live footer — isolates animation cost from counter
cost for measurement). Any non-`off` implies `.timer`; the runtime baseline is sampled at
statement start, the end sample **pinned the moment execution returns** (before result
rendering, which itself allocates). The footer is a columnar table (tree-indented `op` column,
one right-aligned column per stat shared by ≥2 ops, a trailing `misc` column, a compact
glossary):
```
op                          RowsIn  RowsOut  misc
order-offset-limit               1      1/5
  group                          5           GroupsOut=1
    filter                       6        5
      datastore-scan-records              6/6
```
(Profiling confirmed `-stats` is not an alloc driver: on a 3-way join ~79% of allocs are
`glue.DatastoreFetch` re-fetching docs; the stats subsystem doesn't appear.)

**Process `runtime:` line** — `allocated`/`allocs`/`GCs` as **deltas from a per-statement
baseline**, `heap`/`goroutines` as current gauges. ⚠ Read via `runtime/metrics` (not
`runtime.ReadMemStats`, which **stops the world**), sampled only on redraw (~10 Hz). Entirely
render-side (`cmd/n1k1`), so `base`/`engine`/`glue` take no `runtime` dep.

## Estimates & progress bars

A bar needs a **denominator**: `Stats.Totals` is a parallel `[]int64` (same indexing as
`Counters`), `Totals[i]==0` meaning indeterminate (spinner), else a bar is
`Counters[i]/Totals[i]` (`cur/total`). Bars need not be monotonic — a re-run op's inner-loop
bar fills 0→N, snaps to 0, refills per outer row. Sources: **self-observed peak** pass size
(implemented, self-calibrating, no planner input) and query **`LIMIT`** (implemented, a hard
exact denominator for the top op); planner cardinality (often 0 on the file datastore) and
manifest doc/byte counts are future.

## Live running aggregates (zero per-row alloc)

The highest-value partial: `COUNT`/`SUM`/`AVG`/`MIN`/`MAX` (+ `_v` forms) *ticking upward*
while the query runs. A running aggregate is an **advisory partial** (payload flagged
`Partial: true`, undims at 100%), the perception-level version of Online Aggregation — not a
statistical estimator.

The hot path does **not change**: grouped aggregates already accumulate as **encoded bytes
inside the group map's value** (`OpGroup`, laid out by `base.Agg.Init/Update/Result`), and the
fixed-width aggs are already alloc-free per row (the in-place `copy(lzGroupValPrev,
lzGroupValNew)` when sizes match) with **AVG's divide already deferred to `Result`-time, never
per row**. Everything is read-side, at the checkpoint:

- **Decode partials at the synchronous checkpoint via the same `Agg.Result`.** A finalized
  aggregate is `Agg.Result(vars, aggBytes, buf)` decoded into `^aggregates|<agg.String()>`
  (DESIGN-exprs.md lever #6); a live partial is the same `Result` against the *current*
  accumulator bytes, run early. ⚠ Because `YieldStats` fires on the exec goroutine *between*
  row yields, no `Agg.Update` is mid-flight → the group value bytes are coherent, **no
  hot-path lock**, O(sampled groups × aggs) at ~10 Hz.
- **`Result`'s `buf` arg is the zero-alloc seam** — a reader keeps one lifted `previewBuf` +
  `[]RunningAggRow` reused across checkpoints (`BenchmarkLiveAggSnapshot`: 0 allocs/op).
- **Bounded sample** — `RunningAggMaxGroups`(=64) caps a first-N walk; ungrouped (one group)
  is always fully covered.

`OpGroup` registers a refresher once at setup (`Ctx.RunningAggRegister`, non-`lz`, off the hot
path) capturing the live map + layout; the checkpoint calls `Ctx.RunningAggsRefresh`; the
refresher is torn down before `RecycleMap`. Interpreter-only (`genCompiler:hide`).

⚠ **Reader coherence.** Checkpoint-sourced reads (in the `OnStats` callback, on the exec
goroutine) are coherent by construction. A **pull reader** on a separate goroutine can tear on
a *variable-width* `min`/`max` `Set` (which relinks the value to a new heap offset — a stale/
half-relinked read = garbage), so the checkpoint must first **copy the sampled raw agg bytes
into an immutable reused snapshot buffer**; field-skew stays advisory-acceptable, garbage does
not.

**Cheap vs not (honest tally):** truly zero-alloc live — COUNT/COUNTN/SUM/AVG/MIN/MAX (+ `_v`)
and COUNT(DISTINCT) *cardinality*. **Not cheaply live** — ARRAY_AGG/MEDIAN/VARIANCE/STDDEV and
DISTINCT-materializing variants: their `Result` re-walks or `make([]float64,n)`+sorts, so a
live value costs a coarse-cadence alloc or an approximation → ship progress-only by default
(opt-in value in `.stats rich`). (DISTINCT-family sets grow unbounded in memory regardless — a
pre-existing property, not caused by live stats.) Window aggregates are frame-relative and out
of scope here.

## Future

All of the following is designed but **unbuilt**; the counter core is the substrate each wears
differently.
- **Delivery models** — the shipped path is a throttled push callback (`OnStats`/`YieldStats`,
  runs in the exec goroutine so it must be fast/non-blocking). Planned: a **pull-snapshot**
  primary library API (`Snapshot()` from the user's own ticker, per-field atomic loads,
  monotonic-skewed but fine) + thin channel/expvar wrappers. Keep the per-op checkpoint as the
  cancellation + flush point.
- **CLI dial** — today only `off|on|final`; planned a detail level (`off|auto|min|rich|debug`,
  `auto`=DuckDB's silent-until-~2s default) × a view selector (`line|bars|plan|pruning|preview`,
  tabbed panels over one live trace).
- **In-flight result preview** beyond the aggregate scalars — a `GROUP BY` bounded sample, an
  `ORDER BY … LIMIT` live leaderboard (safe to read at the same synchronous checkpoint).
- **Plan-flow visualization** (Sankey edges = child `RowsOut`; exploding-join flash on
  `RowsOut/RowsIn`; a `Probes` "work" counter; spill/pruning states) + a **pruning view**
  (`scanned 88/9,500 files, pruned 99%`) driven by the future `FilesOpened`/`FilesPruned`
  counters; render targets ASCII/SVG/web over one JSON **query trace**.
- **DVR / replay** (ring-buffer the last N traces, transport controls, live-rewind), **racing
  bars** (per-actor/per-file lanes, straggler highlight), **multi-phase** (ingest/index/transfer
  denominators).
- **`EXPLAIN PRICE`** (a-priori estimate from cardinality × a cloud pricing table, pruning-aware)
  / **`EXPLAIN COST`** (a-posteriori, price the measured counters). ⚠ Wire **without a grammar
  fork**: dot commands (`.price`/`.cost`) or a CLI pre-parse intercept stripping a leading
  `EXPLAIN PRICE`/`COST` — both reuse the existing plan + stats counters.

Dependency policy for the render libs: permissive only (pterm MIT, mpb Unlicense, expvar/atomic
stdlib) — no GPL/AGPL.

## Open questions

- Per-op tree vs single rolled-up number as the default surface.
- Snapshot consistency: accept per-field monotonic skew vs a seqlock/double-buffer.
- Denominators: how eagerly to compute totals (file sizes now; manifest `doc_count` later).
- Grouped running-agg sample policy: first-N (O(N)) vs top-N-by-value (O(groups)/checkpoint).
- Heavy aggregates: value-with-coarse-alloc vs progress-only vs approximate (HLL / streaming
  quantile).
- Per-op "work" counters (probes, hash inserts): worth the hot-path cost? Gate behind an
  explain-analyze mode.
