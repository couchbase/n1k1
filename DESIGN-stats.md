# Design: Live Stats & Progress Reporting

Status: proposal / for review (counter core now implemented — see [Implementation status](#implementation-status)).

## Overview

Good CLI/data tools show live-ish stats and progress while they work — rows/bytes
processed, throughput, % complete, ETA. This doc designs how n1k1 supports that:
**optional**, **library-user-first**, **lightweight** (the hot path stays nearly
free). Key idea: **decouple measurement cadence from reporting cadence** — count on
the per-row hot path for ~free, but deliver/rate-math/render only at a coarse
(~10 Hz) cadence off the hot path. The doc also designs the user-facing surface
(`-progress`/`.stats`), pruning visibility, partial-result previews, live plan
visualization, DVR/replay, and `EXPLAIN PRICE`/`EXPLAIN COST`.

## Contents

- [Existing skeleton](#existing-skeleton)
- [Core principle: two decoupled cadences](#core-principle-two-decoupled-cadences)
- [What to measure — the `Stats` struct](#what-to-measure--the-stats-struct)
- [Counter core: one flat `[]int64` keyed by op id](#counter-core-one-flat-int64-keyed-by-op-id)
- [Stat naming convention](#stat-naming-convention)
- [Codegen safety (dev notes)](#codegen-safety-dev-notes)
- [Implementation status](#implementation-status)
- [Estimates & progress bars](#estimates--progress-bars)
- [Process / runtime stats (memory, GC, goroutines)](#process--runtime-stats-memory-gc-goroutines)
- [Delivery: getting stats to the client](#delivery-getting-stats-to-the-client)
- [Cooperative cancellation](#cooperative-cancellation)
- [Library-user API (sketch)](#library-user-api-sketch)
- [Rendering stays out of the core](#rendering-stays-out-of-the-core)
- [CLI control surface: `-progress` & `.stats`](#cli-control-surface--progress--stats)
- [In-flight / partial results (the "spinning numbers")](#in-flight--partial-results-the-spinning-numbers)
- [Visualizing the plan with live data-flow](#visualizing-the-plan-with-live-data-flow)
- [Record & playback: a DVR / TiVo for queries](#record--playback-a-dvr--tivo-for-queries)
- [Parallel progress: racing bars](#parallel-progress-racing-bars)
- [Multi-phase pipelines (ingest / index / transfer)](#multi-phase-pipelines-ingest--index--transfer)
- [`EXPLAIN PRICE` & `EXPLAIN COST`](#explain-price--explain-cost)
- [Dependency licensing (permissive only)](#dependency-licensing-permissive-only)
- [Open questions](#open-questions)
- [Prior art](#prior-art)

## Existing skeleton

Not greenfield — the plumbing was scaffolded and the counter core is now
implemented (see [Implementation status](#implementation-status)):

- **`base.Stats`** (`base/stats.go`) — the counter core (`Counters []int64`,
  `Index`, `Ops`); originally an empty `struct{} // TODO`.
- **`type YieldStats func(*Stats) error`** (`base/base.go`) — the callback an op
  invokes occasionally to yield stats; documented concurrent-safe, and its
  **error return doubles as early-exit / abort** (`LIMIT`, cancellation).
- **`Ctx.YieldStats`** (`base/vars.go:82`) — set per request; may be invoked
  concurrently by multiple goroutines.
- **Throttling present:** `engine/op_scan.go` calls `YieldStats` **every
  `ScanYieldStatsEvery` (=1024) rows**; `op_order.go`/`op_window.go` have TODOs
  for the same checkpoint.
- **No-op sink** wired in `glue/exec.go`.

So the model is a **throttled, per-operator push** with a cancellation-carrying
return. Work: (1) define `Stats`, (2) make collection cheap, (3) choose client
notification, (4) give library users a clean API.

## Core principle: two decoupled cadences

**Separate measurement cadence from reporting cadence.**

- **Measurement** is on the per-row hot path and must be ~free: a local counter
  increment — no atomics, no allocs, no `time.Now()`.
- **Reporting** is coarse — **~10×/second** (ClickHouse caps progress at ≤10/s;
  DuckDB only shows a bar after a query exceeds ~2s). Delivery, rate math, and
  rendering all live here, off the hot path.

n1k1's existing per-1024-row throttle is the seam between the two. **Never call a
user callback or compute a rate per row.**

### Concurrency (single-writer, no atomics)

Each `base.Stage` actor runs in its own goroutine. Counters are **single-writer**:
each op instance's section is written by one goroutine (a scan→filter→group
pipeline runs in one goroutine; only `Stage`/`UNION` actors split goroutines, and
those are *distinct* subtrees → distinct sections). So plain `=`/`++` is safe **and
no atomics are needed today**. Avoid atomics-per-row anyway — a locked op every row
plus **false sharing** (cores ping-ponging a shared cache line) would erase the
parallelism it measures. If same-op parallelism ever lands (parallel scans /
`GROUP BY` shards), add a per-`(op, actor)` dimension *before* relying on the
counts (see [Counter core](#counter-core-one-flat-int64-keyed-by-op-id)).

## What to measure — the `Stats` struct

Keep it **flat, fixed-size, copyable** (counters, not maps):

- **Universal:** `RowsIn`/`RowsOut`, `Bytes`, `Elapsed`; derived
  `RowsPerSec`/`BytesPerSec`, `Percent`, `ETA` computed at report time from
  deltas.
- **Progress needs a denominator.** `%`/ETA requires a known total (source file
  sizes, or manifest `doc_count`/partition counts from `DESIGN-data.md §5`);
  otherwise indeterminate (spinner, not bar) — cf. ClickHouse's
  `total_rows_approx`.
- **Pruning / skip counters** (often the single most useful stat): files /
  partitions / row-groups **considered** vs **opened**, plus bytes skipped — so
  the UI can say *"read 88 of 9,500 files (skipped 99% via zone maps)"*. From the
  scan layer's manifest / zone-map checks (`DESIGN-data.md §5`) and index
  sargability (`DESIGN-indexing.md`), incremented **once per file/partition
  decision** (not per row). Feed `EXPLAIN COST` (bytes not scanned = money not
  spent). See [Pruning visibility](#pruning-visibility-the-what-did-we-skip-view).
- **Phase-tagged** (`Phase` enum): `query`, `ingest`, `index`, `transfer`.
- **Per-operator + rolled-up:** support both — per-op counters that **roll up** to
  a pipeline total. A stable op id/label keys each slot.

## Counter core: one flat `[]int64` keyed by op id

Concrete structure: **every op contributes a known set of `int64` counters, all
live in one pre-sized `[]int64`, and a `map[string]int` maps `"opId:statName"` to
that counter's index.** Each op owns a contiguous sub-slice; `YieldStats` hands out
the whole array; the receiver uses the map to attribute counters back to operators.
It reuses n1k1's existing seams:

- **The op id already exists.** `ExecOp` threads `path`/`pathItem` through every op
  via `EmitPush` (`engine/op.go:27,126`), producing a unique string like `_0_1_2`.
  That **is** the `opId`, and it survives into the compiled path (codegen mints
  unique variable names from it), so one key works in both builds.
- **Layout computed once, at convert time.** Each op kind declares its stat names
  (a static `StatsDesc`, e.g. scan → `{RowsOut, BytesIn, FilesOpened, FilesPruned}`,
  joinNL → `{Probes, RowsOut}`). One plan walk sums contributions into total `N`,
  allocates one `make([]int64, N)`, and builds the `"path:statName" → index` map.
  Non-`lz` setup work, never on the hot path.
- **The shared array lives on `Ctx`.** `base.Ctx` is per-request, immutable for the
  request lifetime, concurrent-safe, already shared across every actor's cloned
  `Vars` (`ChainExtend` → `Ctx.Clone`, `base/vars.go:41,124`) — the natural home
  for the `[]int64` and index map.

### Keep the per-row path free — flush at the checkpoint

Per-row work is a **local** `++`. At the **already-present 1024-row `YieldStats`
checkpoint** — which runs synchronously on the actor's own goroutine
(`engine/op_scan.go:139`) — the actor **flushes its local deltas** into its section
of the shared `[]int64`. The only concurrent reader is the ~10 Hz reporter, for
which monotonic per-field skew is acceptable (no seqlock — see
[Open questions](#open-questions)).

### The index is compile-time, never a per-row map lookup

The `map[string]int` is a **planning + reporting** artifact only. Resolve
`"path:statName" → base offset` at convert time, bake the integer `base` into the
generated op closure, and the hot path is `counters[base+RowsOut]++`. This lets the
scheme survive codegen unchanged: `base` is an ordinary int in the non-`lz` setup
block; only the increment and checkpoint flush cross into `lz`.

### Concurrency dimension: per-op today, per-(op, actor) when needed

- **Today it's contention-free.** The only fan-out is `op_union.go`, which spawns
  **one actor per child** (`base.NewStage(numChildren, …)`), each running a distinct
  child subtree with a distinct `path` → distinct sections → no shared slot. "One
  section per `opId`" just works.
- **When same-op parallelism lands** (N actors on the same op/path), add an actor
  dimension: array size `= Σ over ops of (numStats × numActors)`, each `(op, actor)`
  gets private slots, roll-up sums an op's actor rows at report time. `NumActors` is
  known at stage setup (before the hot path), so allocation stays one-shot; the hot
  path is unchanged.

Blocking ops (`GROUP BY`, `ORDER BY`) are a *display* nuance ("inhale, then a final
burst"), not a counting one — each still runs in a goroutine that owns its counters.
The flat array only cares which goroutine owns which slots.

## Stat naming convention

Counter names are public-ish (they key the index and label the plan tree):

- **Noun first ("NounAdjective"), not "AdjectiveNoun".** Lead with the *thing*
  counted so a subsystem's stats cluster together: `RowsIn`, `RowsOut`, `RowsLeft`;
  `BytesIn`, `BytesOut`; `GroupsOut`. Not `InRows`/`OutRows` (which scatter `Rows`
  across the alphabet).
- **Monotonic is the default, and unmarked.** Most counters only increase
  (cumulative totals); leave them unadorned (`RowsOut`, `Probes`, `GroupsOut`).
- **A current level takes `Cur`; a high-water mark takes `Peak`.** A value that
  rises *and* falls (current memory, in-flight batches) is a gauge:
  `MemCur`, `BatchesCur`; its high-water mark is `MemPeak`. Suffix (not prefix)
  keeps the noun leading, so `Mem*` clusters.
- **vs cbft/cbgt's `Tot`/`Cur` prefixes:** adopt the distinction but flip it to a
  **suffix** (a prefix sorts all `Tot*`/`Cur*` together, fighting noun-clustering),
  and make monotonic the *unmarked* default — shorter names, and the rarer gauges
  stand out (they need bar/needle rendering vs a sparkline).
- **Future: `StatKind`** (Counter / Gauge / Peak) in the descriptor, so tooling can
  pick rendering without string-parsing the suffix. Not needed yet (every counter
  today is monotonic); the naming lines up with that enum when it lands.

## Codegen safety (dev notes)

n1k1 ops are **dual-mode source**: the same `op_*.go` runs in the interpreter *and*
is read by `cmd/intermed_build` to emit the compiled path
(`intermed/generated_by_intermed_build.go`). Rules to keep a counter working in
**both**:

- **`lz`-prefix drives emission.** A line assigning an `lz`-prefixed var from a
  non-`lz` expression is emitted with the value **baked** as a literal — that's how
  the base offset becomes a compile-time constant. Write `lzStatsBase := o.StatsBase`
  and index `Counters[lzStatsBase+StatFooBar]`. **Never** index by a non-`lz` var in
  emitted code, and **never** do a per-row map lookup.
- **Per-row increments go on a local `lz` counter** (`lzStatRowsOut++`), emitted
  into the runtime loop, ~free even when stats are off. **Flush** into the shared
  slot only at a coarse point, guarded by
  `if lzVars != nil && lzVars.Ctx != nil && lzVars.Ctx.Stats != nil { ... }`. Scans
  flush at each 1024-row checkpoint (live); other ops flush once, after their child
  drains (final value correct; live intermediate cadence is a follow-up).
- **Signature changes ripple through regeneration**, not by hand: `// !lz` call
  sites are re-emitted on re-run. The generated file is **gitignored**; do not
  commit it.
- **Always regenerate + compile + run the suite in both modes:**
  `go build ./cmd/intermed_build/ && ./intermed_build`, then `go build ./intermed/`,
  then the suite with `-tags n1ql` (see the worktree bootstrap in
  `DESIGN-testing.md`). The suite is the only check that exercises the *compiled*
  counters; `engine` unit tests cover only the interpreter.

> **KNOWN LIMITATION — compiled path currently has NO stats (TODO).** The counter
> lines are marked `// <== genCompiler:hide`, so `cmd/intermed_build` omits them and
> the compiled path collects nothing. Root cause: the compiler-test codegen
> (`test/emit.OpToLines`) inlines the whole op tree into one function, and a per-op
> local counter (`lzStatRowsOut`) incremented inside the yield closure gets
> **cleared** when that closure is inlined at a child's call site (`clearFuncLines`
> keeps only lifted `var X = Y` idioms) → `undefined: lzStatRowsOut`. Naive fixes
> surface two more gaps: `var X = 0` collides across sibling ops, and the flush line
> carries *two* lifted vars which `varLift` mis-aligns. So stats are
> interpreter-only for now (the CLI's live progress runs on the interpreter, so no
> user-visible loss). TO RE-ENABLE: drop the markers, give each counter a
> path-unique name via `// <== varLift: lzStat… by path` (as `lzValsReuse` does),
> and teach `varLift` to align format args when several lifted vars share one line.
> Scans are exempt: they compile to a `glue.DatastoreOp` island and `countingYield`
> tracks their rows out in both modes.

## Implementation status

Implemented (interpreter + compiled path, verified by the `n1ql` suite in both
modes):

- **`base/stats.go`** — `Stats{Counters, Index, Ops}`, the `StatsDescs` registry
  (op Kind → ordered stat names), `LayoutStats(root)` (pre-order walk sizing the
  array, assigning `Op.StatsBase`, building the `"opId:statName"` index),
  `Stats.Get(key)`. `Op.StatsBase` (`json:"-"`) and `Ctx.Stats` added; the empty
  `Stats struct{}` placeholder removed.
- **Instrumented ops** (all monotonic): `scan` → `RowsOut` (live, flushed at the
  1024-row checkpoint; also removed the per-checkpoint throwaway `Stats` alloc —
  `YieldStats` now receives the shared `Ctx.Stats`); `filter` → `RowsIn`/`RowsOut`;
  `group`/`distinct` → `RowsIn`/`GroupsOut`; `order-offset-limit` →
  `RowsIn`/`RowsOut`; the nested-loop `join`/`nest`/`unnest` family →
  `RowsLeft`/`Probes` (the exploding-join signal). The **glue datastore scans**
  (`datastore-scan-records`/`-primary`/`-index`/`-index-cover`/`-fts`/`-keys`) also
  get `RowsOut`, instrumented in `glue` (`countingYield` in `glue/stats.go`) since
  the CLI's real file reads go through those (not `OpScan`); that wrapper drives a
  live checkpoint every 1024 rows.
- **Single source of truth, greppable, self-documenting.** Every counter is one
  `base.DefStat(name, about, kinds…)` line (`engine/stats.go`, `glue/stats.go`)
  defining the offset constant, registered name, **and one-line description** — so
  they can't drift. List all: **`git grep '= base.DefStat'`**. `DefStat` is
  idempotent, so the compiled path's re-run of initializers doesn't double-register.
  It runs in package-var initializers, so the full catalog (names, `base.StatAbout`,
  kinds) is populated before `main`: `.stats about` prints the whole glossary; the
  `-stats` footer appends a compact glossary of just the stats shown.
- **`glue`/CLI opt-in, three modes.** `Session.CollectStats` lays out the counters
  and returns them as `Result.Stats`; `Session.OnStats` receives the live `*Stats`
  at each checkpoint. The CLI exposes `-stats`/`.stats`:
  - `off` (default) — collect nothing; zero cost.
  - `on` — collect + on a TTY redraw the footer live (throttled ~10 Hz, in place, on
    stderr) + print final totals.
  - `final` (aliases `end`/`summary`) — collect, show only the **grand totals once
    at the end**, no live footer. For **measurement**: `on` vs `final` isolates the
    animation's cost, `final` vs `off` isolates the counters' cost. (On the 3-way
    join: the live animation added ~0.3 MB over ~65 frames against ~932 MB total —
    not the memory driver.)

  Any mode but `off` implies `.timer` (elapsed is the denominator for rows/s and
  alloc/s). The runtime baseline is sampled at statement start; the end sample is
  **pinned the moment execution returns** — before result rendering, which itself
  allocates — so `allocated`/`GCs` reflect the statement, not the display.
- **Profiled to confirm the source (`-profile-cpu`/`-profile-mem`).** On the 3-way
  `orders` join (~931 MB *allocated*, ~3 MB *in-use* at exit — pure churn, no leak),
  the alloc profile is **~79% `glue.DatastoreFetch`** (the NL join re-fetches docs
  hundreds of thousands of times; the file datastore re-opens/`stat`/`readdir`/
  re-parses each time). The **stats subsystem does not appear at all** — confirming
  `-stats` is not the driver. (A fetch cache would cut it — out of scope.)
- **Everything animates, not just scans.** Two things move the whole tree: (1) every
  op writes its counters per row (`statBump`), reset at each op invocation
  (`statZero`) — so a re-run inner subtree restarts while a single-run op climbs
  cumulatively; (2) the render trigger fires at each **scan-invocation boundary** (a
  pulse in `DatastoreOp`), not only every N rows — essential because a NL join's
  inner scan yields only a handful of rows per pass, under the checkpoint interval.
  Single-writer, so no atomics; render throttles to ~10 Hz. (Interpreter-only via
  `genCompiler:hide`.)
- **Aligned columnar display.** The footer is a table: a tree-indented `op` column,
  one **right-aligned numeric column per stat name shared by ≥2 operators** (so a
  value repeated down the plan lines up), then a trailing free-form `misc` column
  for one-off stats. A counter with a known estimate renders `cur/total`. Example:
  ```
  op                          RowsIn  RowsOut  misc
  order-offset-limit               1      1/5
    group                          5           GroupsOut=1
      filter                       6        5
        datastore-scan-records              6/6
  glossary: GroupsOut: distinct groups (or DISTINCT rows) emitted
            RowsIn: input rows the operator consumed · RowsOut: rows emitted to the parent
  ```
- Nil `Ctx.Stats` ⇒ the zero-cost off path (unchanged default).

**Not yet wired (follow-ups):** `project`/`union`/`window` and hash-join counters;
`BytesIn` and pruning/`FilesOpened`/`FilesPruned` counters (need the
`DESIGN-data.md §5` manifest); the richer views (`.stats view plan`, racing bars,
DVR) and `EXPLAIN PRICE`/`COST`; the `StatKind` gauge/peak marker; and re-enabling
counters on the **compiled** path (they are `genCompiler:hide` today).

## Estimates & progress bars

Progress bars need a **denominator**. `Stats.Totals` is a parallel `[]int64` (same
length/indexing as `Counters`) where `Totals[i]` is the estimate for counter `i`
and `0` means *indeterminate* (spinner). A bar is `Counters[i] / Totals[i]`; the
CLI shows `cur/total`.

**Bars need not be monotonic.** A re-run operator resets its counter each
invocation — exactly the UX for an NL join's inner scan: fills 0→N, snaps to 0,
fills again per outer row. Falls out for free (`DatastoreOp`/`countingYield` is
re-entered per invocation). The denominator is the **self-observed peak** pass size,
kept in `Totals` so it persists across invocations; it self-calibrates after the
first pass.

Where estimates come from:

- **Self-observed peak** (implemented) — largest pass seen so far; drives the
  resetting inner-loop bar, needs no planner input.
- **Query `LIMIT`** (implemented) — a hard denominator for the top op's output rows
  (`order-offset-limit`'s `RowsOut` total = the `LIMIT`). Cheap and exact.
- **Planner cardinality** — cbq's `Cardinality()`; often `0` on the file datastore
  (no `UPDATE STATISTICS`) → use when `> 0`, else indeterminate.
- **Manifest doc/byte counts** (`DESIGN-data.md §5`) — a scan's total rows/bytes;
  lands with that work.
- **Propagation** — a parent borrows a child's estimate (filter output ≤ input;
  project output = input), so a scan estimate flows upward.

The bar is render-side; richer front-ends consume the same `Counters`/`Totals` pair.

## Process / runtime stats (memory, GC, goroutines)

Per-op counters are about *rows*; a complementary readout is *how hard the process
is working*. Per-operator memory attribution is out of reach (cbq heap-allocs
opaquely), but a **process-wide** readout is cheap and honest. The CLI shows one
`runtime:` line:

```
runtime: 932.4MB allocated · 8.0M allocs · heap 5.9MB · 239 GCs · 5 goroutines
```

- **What.** *allocated* (cumulative bytes) and *allocs* (count) are **deltas from a
  per-statement baseline** — the churn this statement generated; *GCs* is the cycle
  delta. Headline: a 3-way NL join shows ~932 MB allocated / 8 M allocs / 239 GCs
  though live *heap* is only ~6 MB. *heap* and *goroutines* are current gauges.
- **How, cheaply.** Read via `runtime/metrics` (`/gc/heap/allocs:*`,
  `/memory/classes/heap/objects:bytes`, `/gc/cycles/total:gc-cycles`) plus
  `runtime.NumGoroutine()`. `runtime/metrics` does **not** stop the world (unlike
  `runtime.ReadMemStats`), so it's safe to sample periodically.
- **When.** Sampled only when the display **redraws** — piggybacking the ~10 Hz
  throttle. Baseline captured at statement start; same view reused for the final
  footer so deltas span the statement.
- **Where.** Entirely render-side (`cmd/n1k1`), so `base`/`engine`/`glue` take no
  `runtime`-package dep; a library user samples on their own cadence.

Later candidates (process-wide, cheap, same cadence): **alloc-rate** (bytes/s),
**rows/s** throughput, **GC pause** (`/gc/pauses:seconds`), **CPU** time
(getrusage), **peak heap**.

## Delivery: getting stats to the client

These differ only in **how the client is notified**; all share one counter core.

- **(a) Pull / polling snapshot** — engine keeps a shared `*Stats`; the user calls
  `Snapshot()` from their own goroutine/ticker (e.g. 100 ms). Zero coupling, no
  backpressure, client owns cadence, cheapest. Snapshot = per-field atomic loads
  (skewed but monotonic → fine). How `pv`/`rsync` work.
- **(b) Throttled push callback** — the existing `YieldStats`, refined. User
  registers `Progress func(*Stats)`; engine calls it only at coarse boundaries.
  Runs **in the execution goroutine**, so the callback **must be fast and
  non-blocking**. Refine to throttle by *both* count and wall-time, and route
  through one central sink rather than every op calling the user's render fn. Keep
  error-return = cancellation.
- **(c) Channel / latest-wins** — non-blocking send onto a `chan *Stats` of
  **capacity 1** (coalesce: if full, newest-wins). Decouples producer/consumer,
  auto-throttles to consumer speed. Snapshots must be immutable once sent.
- **(d) expvar / metrics exporter** — publish via stdlib `expvar` or Prometheus
  (dep, heavier). Good for servers, overkill for an interactive CLI; optional
  add-on over the same core.

**Recommendation:** make **(a) pull-snapshot the primary library API**, backed by
one counter core, with a small internal reporter goroutine reading snapshots on a
~10 Hz ticker for the CLI. Offer (b) and (c) as thin opt-in wrappers. Keep the per-op
`YieldStats` checkpoint as the **cancellation + flush** point, its default sink a
cheap counter merge (not the user's renderer).

## Cooperative cancellation

`YieldStats` already returns an error for early exit (LIMIT). The coarse checkpoint
that flushes stats is also where we check `ctx.Done()`/an abort flag and return an
error to unwind. **One mechanism serves both progress and cancellation** — preserve
as the `intermed` path is regenerated (the `lz` checkpoints must survive codegen).

## Library-user API (sketch)

Opt-in, zero-cost when off (nil-check the hook as `op_scan.go` does):

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

The core only emits `*Stats` snapshots; rendering is separate. For the CLI use
**`pterm`** (dep) or **`mpb`** for bars/spinners/ETA. Adopt DuckDB's "only show a
bar after ~2 s" and ClickHouse's "≤10 updates/s, skip for quick queries."

## CLI control surface: `-progress` & `.stats`

One obvious dial for **how much** to show and **which view**, modeled on shipped
controls (`-meta`/`.meta`, `-scan`, `.timer`, `.explain` — see `DESIGN-cli.md`).
**Two orthogonal axes**, set by a startup flag and adjustable live in the REPL:

- **Detail level:** `off | auto | min | rich | debug`.
  - `off` — never collect/animate (counter gates stay nil).
  - `auto` (**default**) — DuckDB-style: silent until a query crosses ~2 s.
  - `min` — a single throttled status line (rows, rows/s, %, ETA).
  - `rich` — bars/spinners + partial-result previews + the pruning panel.
  - `debug` — also per-op "work" counters (join probes, hash inserts) + the live
    plan-flow diagram; the only level paying the hot-path work-counter cost (see
    [Open questions](#open-questions)), so it's explicitly gated.
- **View:** `line | bars | plan | pruning | preview`, each a front-end over the
  *same* snapshot stream. A non-TTY / `NO_COLOR` / piped run forces `min` plaintext.

**Flip between screens during a long query.** In the TUI the views are **tabbed
panels** over one live trace — a hotkey (Tab, or `1`–`5`) swaps between racing bars,
plan-flow, leaderboard, and pruning panel *without* interrupting the query. Cheap
because switching is purely render-side. (bubbletea's model/update/view loop fits;
pterm's multi-area printer a simpler one.)

```
.stats                 # show current level + view
.stats rich            # set detail level
.stats view plan       # switch the active screen (also Tab/1–5 while running)
.stats off             # disable
```

`-progress=<level>[:<view>]` is the batch equivalent. Keep the surface small — one
dial, one view selector, `auto` default.

## In-flight / partial results (the "spinning numbers")

Beyond metrics, show partial **result values** as they build — running
`COUNT`/`SUM`/`MIN`/`MAX` per group ticking toward finals, an `ORDER BY … LIMIT`
leaderboard reshuffling. Goal is *perception*, not completeness — a bounded sample
at ~10 Hz, approximate and eventually-correct, never mistaken for the real answer.

**Which operators can preview:**

- **Ungrouped aggregate** (`COUNT(*)`, `SUM`): a couple of scalars — trivially
  cheap to publish every checkpoint. Best demo case.
- **`GROUP BY`** (blocking but inspectable): publish a **bounded sample** of the
  group map — first-N or top-N-by-value (N ~ 10–50). `COUNT`/`SUM`(≥0)/`MAX` rise,
  `MIN` falls, so they animate toward final.
- **`ORDER BY … LIMIT`** (top-K heap): the current top-K *is* a meaningful evolving
  preview — a "live leaderboard."
- **Not previewable:** full `ORDER BY`, `DISTINCT`, final projections — flag the
  payload so the UI never renders it as data; expose only cardinality/progress.
- **Streaming ops** (filter/project) already yield rows incrementally; preview
  matters mainly for **blocking** ops.

**Reading partial state safely:** the group map is mutated by the exec goroutine(s).
The free trick: **snapshot at the existing per-checkpoint `YieldStats` call, which
runs synchronously on the exec goroutine** — at that instant the map isn't mutating,
so copying a bounded N-row **immutable snapshot** is race-free and O(N), no hot-path
locks. For parallel `GROUP BY`, each actor previews its own shard.

**Same delivery + payload:** rides the same pull/callback/channel machinery — just a
richer payload: `Stats` (or a sibling `Preview`) carries an optional bounded
`[]PartialRow` marked `Partial: true`. Animation is **render-side**: the core emits
*real* numbers at ~10 Hz; the "spinning" tween (easing) is a pure TUI concern
(pterm/mpb). Solidify/undim at 100 %.

## Visualizing the plan with live data-flow

Render the *executing* plan as a diagram and **animate rows flowing edge→edge**, so
expensive shapes (NL joins, big sorts, spills) become obvious. The visualizer is a
**consumer** of two things we already produce:

- **Plan graph** (nodes + edges) extracted once from the `base.Op` tree
  (`Kind`/`Children`/`Labels`); `EXPLAIN` already emits it.
- **Per-op snapshot stream** at ~10 Hz keyed by op id (`RowsIn`, `RowsOut`, rows/s,
  bytes, spill, wall-time). Each **edge's flow** = the child's `RowsOut` (= the
  parent's `RowsIn`).

**Visceral signals:**

- **Edge flow → animation speed / particle density / line thickness** (Sankey ∝
  rows).
- **Row amplification** (`RowsOut/RowsIn`): the Snowflake **"exploding join"** tell
  — flash red when the ratio blows up.
- **Work, not just rows:** count inner **probes/comparisons** (a `Probes` slot), so
  an NL join over L×R glows hot even when output is small, while a hash join stays
  calm.
- **Node heat = time share** → a live flame-graph-on-the-plan.
- **Pipeline breakers** (`GROUP BY`, `ORDER BY`) visibly *inhale* then burst.
- **Spill:** node flips to a red "disk" state when rhmap/store spills.
- **Pruning:** eliminated files render as dimmed/struck tiles that never light up.

### Pruning visibility: the "what did we skip?" view

A first-class screen (the `pruning` view), driven by the pruning counters:

- **One-liner:** `scanned 88 / 9,500 files · 12 MB / 47 GB · pruned 99.3% (zone
  maps: 8,900, partition filter: 512)` — attributing *why* each swath skipped, from
  `DESIGN-data.md §5` (manifest + zone maps) and `DESIGN-indexing.md` (`RangeKey`
  sargability).
- **Visual:** a grid/treemap where **opened** tiles fill with throughput color and
  **pruned** tiles stay dark — a Hive `year=/month=` tree lighting up only matching
  partitions.
- **Anti-signal:** poor pruning (non-sargable predicate, `SELECT *`) is a wall of
  lit tiles — "you're reading everything; add an index / WHERE / partition column."
  Ties to `EXPLAIN COST`.
- **Cheap:** per-file/partition decisions (thousands, not billions), coarse cadence.

### Render targets

1. **ASCII / TUI (default, SSH-friendly):** box-drawing nodes with live counters,
   marching-glyph (`▸▸▸`) edges, hot nodes glow. **bubbletea + lipgloss** or
   **pterm** (dep); ~10 Hz.
2. **SVG (share / report):** a **self-contained** SVG (à la PEV2's `pev2.html`) —
   static heat map or a CSS/SMIL "query movie." No external refs.
3. **Canvas / web (rich):** an HTML page — or a claude.ai **Artifact** — with
   SVG/canvas + JS particles flowing at real throughput; live via SSE/WebSocket or
   replayed from a trace.

### Query-trace format

Record `(plan graph + snapshot stream)` as a self-contained JSON **query trace**;
render it **live** (subscribe) or **replay/scrub** later. Same visualizer, two
sources; shareable, and the substrate for the DVR controls in
[Record & playback](#record--playback-a-dvr--tivo-for-queries).

**Layout:** Reingold–Tilford for plan trees (trivial pure-Go); layered Sugiyama
(dagre) for DAGs; ASCII uses indented boxes. The engine stays render-agnostic —
ASCII / SVG / web are interchangeable front-ends over one trace.

**Libraries (permissive):** bubbletea / lipgloss / bubbles (MIT), pterm (MIT, dep),
tview (MIT), termdash (Apache-2.0); gonum/graph (BSD) or hand-rolled
Reingold–Tilford; dagre (MIT) in a web artifact. **Avoid bundling** Graphviz
(Eclipse Public License — outside the MIT/Apache-2 policy); use only as an optional
external `dot` binary. **Prior art:** PostgreSQL **PEV2** / explain.dalibo.com;
**Snowflake Query Profile** (the canonical exploding-join view); **Spark/Flink UI**
(live DAG); **Sankey diagrams**.

## Record & playback: a DVR / TiVo for queries

The query-trace turns progress into something you can **rewind, pause, scrub,
re-watch** — high-value because the interesting moment (exploding join, spill,
straggler) is often *over* before you focused on it.

- **Always-on ring buffer.** Keep the last N traces in a bounded in-memory ring
  (bytes-capped, oldest evicted) so any just-finished query can be replayed without
  asking first. Traces are small (a few MB covers deep history). `.rec on` promotes
  to persistent (`.n1k1/traces/<ts>.json`); `rich`/`debug` auto-record to the ring.
- **Transport controls.** Over a recorded/paused trace: `space` pause/resume, `←/→`
  step a frame, `,`/`.` slow/fast (0.25×–8×), `home`/`end`, a scrubber. Seeking is
  just indexing into the immutable frame slice.
- **Live rewind.** Pausing a running query freezes the display while the engine
  keeps going; resuming snaps to live. The engine never blocks on the viewer
  (latest-wins channel, delivery (c)).
- **REPL:** `.rec [on|off]`, `.play [<trace>|last]` — sibling to `.stats`.

**Pop it open in a browser:** write the trace into a **self-contained HTML page**
(no external refs, CSP-safe, à la PEV2) with the trace inlined as JSON + a small
player (SVG/canvas, particles, scrubber), launched via the OS opener with **zero new
deps** — `open`/`xdg-open`/`rundll32`, behind `.play --web`. Or publish as a
claude.ai **Artifact**. The same trace still replays in ASCII for headless. Prior
art: **asciinema**, **rr** / time-travel debuggers, DVR/TiVo.

## Parallel progress: racing bars

**Many progress bars racing rightward in parallel** (think `docker pull` per-layer
bars, npm/pip/cargo). The *unevenness* is diagnostic — bars at different rates
expose **data skew and stragglers**.

- **What becomes a "lane":** `base.Stage` actors (`NumActors`); parallel scans (one
  bar per file/partition); parallel `GROUP BY` shards; ingest/index/transfer (one
  bar per file). Each needs its own counters + a **denominator** (file size or
  manifest per-partition `doc_count`); no-total lanes render as spinners.
- **Same core, keyed by lane.** Reuses per-actor local counters rolled up at the
  ~10 Hz checkpoint, keyed by lane/task id. Snapshot becomes a
  `[]LaneStat{ id, label, current, total, rate, state }`. Diagram shows operator
  *relationships*; bars show concurrent *tasks* — two lenses over one stream.
- **Bound the visible lanes.** Cap like `docker pull`: an **overall aggregate bar**
  plus **top-K active/slowest lanes**, rest collapsed into "…and M more." Highlight
  the straggler.
- **Libraries:** `vbauerster/mpb` (Unlicense, purpose-built for concurrent multi-bar
  CLIs), `pterm` (MIT, dep), or bubbletea.

## Multi-phase pipelines (ingest / index / transfer)

These long-running ops benefit most (queries are often sub-second). Same core;
denominators from source file sizes and the `DESIGN-data.md §5` manifest
(`doc_count`, byte totals). Report the current item (e.g. the file being indexed)
plus overall %.

## `EXPLAIN PRICE` & `EXPLAIN COST`

`EXPLAIN` shows the *plan*; two cost-flavored siblings answer **before** ("what will
this cost me?") and **after** ("what did it cost?"). Both are the stats core wearing
a price tag.

- **`EXPLAIN PRICE` — a-priori estimate.** From the plan's cardinality/byte
  estimates (bytes to scan, egress, GET/LIST counts, compute-seconds) **×** a cloud
  pricing table → a **$ (or credit) range + predicted wall-time**. Prior art:
  **BigQuery dry-run** (bytes × $/TB) and **Athena** (per-TB billing), generalized.
  Must be **pruning-aware** — run *after* partition/zone-map pruning so
  `WHERE year=2026` quotes pruned bytes: *"$0.02 — pruning saved ~$6.40."* Present
  as a range, not false precision.
- **`EXPLAIN COST` — a-posteriori actual.** Price the **measured** counters we
  already collect (bytes scanned, egress, request counts, wall/compute time) against
  the same table: *"$0.018 (2.1 GB scanned, 140 GET, 3.2 s)."* Nearly free; a
  `.cost` toggle appends it next to `.timer`.

**Prices:** a small **pricing table** (per provider/region: $/GB scanned, egress,
$/1k GET, $/compute-second), cached locally (`.n1k1/pricing.json`) and refreshable
from public sources — **AWS Price List API**, **GCP Cloud Billing Catalog API**,
Azure retail. Ship a **bundled offline default** + a `--pricing <file>` override
(air-gapped, auditable). **Local files cost ≈ $0** — the $ story becomes real with
the **object-store backend** (`DESIGN-data.md` S3/gocloud); frame PRICE/COST as
*"what this would cost against `s3://…`"* even reading a local mirror. Optionally a
labeled local energy/time estimate.

**Wiring without a grammar fork.** True keywords would need a goyacc patch (the
merge-hostile fork `DESIGN-data.md` rejects). Prefer: (1) **dot commands**
`.price <stmt>` / `.cost [on|off]`; (2) a **CLI pre-parse intercept** stripping a
leading `EXPLAIN PRICE`/`COST` to the inner statement. Both reuse the existing plan
(PRICE) and stats counters (COST); neither touches the fork.

## Dependency licensing (permissive only)

`pterm` — MIT (dep); `vbauerster/mpb` — Unlicense; `expvar`/`sync/atomic` — stdlib
(BSD); Prometheus client — Apache-2.0 (dep). All fit the no-GPL/AGPL policy (see
`DESIGN-data.md`).

## Open questions

- **Per-op tree vs single rolled-up number** as the default surface.
- **Snapshot consistency:** accept per-field skew (monotonic) vs a
  seqlock/double-buffer for a coherent snapshot.
- **Where the reporter goroutine lives** — engine, glue, or CLI only.
- **Denominators:** how eagerly to compute totals (file sizes now; manifest
  `doc_count` later) vs indeterminate progress.
- **Codegen path** *(resolved for counters landed so far — see
  [Codegen safety](#codegen-safety-dev-notes))*: the base offset rides through as a
  baked literal, verified by the suite; keep this invariant.
- **Partial-result sampling:** first-N vs top-N-by-value vs a fixed watched set; how
  firmly to guard previews from being consumed as final.
- **Per-op "work" counters** (probes, hash inserts): worth the hot-path cost? Gate
  behind an explain-analyze / viz mode.
- **Visualization transport:** live streaming vs record-then-replay as default;
  ASCII fidelity vs the web canvas.
- **DVR ring bounds:** how many traces / MB always-recorded; is `rich`/`debug`
  auto-record cost acceptable when the user never replays?
- **Screen-flip UX:** tabbed panels vs split dashboard; how many views before
  clutter; persist view state across queries?
- **Cost model fidelity:** $ vs credits; PRICE error bars; pricing-table staleness;
  provider/region defaults.
- **PRICE without a real remote:** is a "what this *would* cost on s3://…" number
  illuminating or confusing? Is the local energy/time estimate worth it?

## Prior art

- DuckDB progress bar (`enable_progress_bar`, ~2 s threshold, per-source
  `GetProgress()`, `duckdb_query_progress`):
  https://duckdb.org/docs/current/configuration/overview ,
  https://github.com/duckdb/duckdb/pull/1432
- ClickHouse client live progress (≤10/s, `total_rows_approx`, `system.processes`):
  https://clickhouse.com/docs/interfaces/client
- Go rendering: `pterm` https://pterm.sh/ , `vbauerster/mpb`
  https://github.com/vbauerster/mpb
- Go metrics: `expvar`, `sync/atomic` https://gobyexample.com/atomic-counters ,
  Prometheus client
  https://pkg.go.dev/github.com/prometheus/client_golang/prometheus
- Online Aggregation (Hellerstein, Haas & Wang, SIGMOD 1997) — running estimates
  that refine over time (we do the perception-level version):
  https://dl.acm.org/doi/10.1145/253260.253291
- Record/replay & DVR: **asciinema** https://asciinema.org/ ; **rr**
  https://rr-project.org/ .
- Cost/pricing: **BigQuery dry-run**
  https://cloud.google.com/bigquery/docs/estimate-costs ; **Athena** per-TB billing
  https://docs.aws.amazon.com/athena/latest/ug/performance-tuning-data-optimization-techniques.html ;
  **AWS Price List API**
  https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/price-changes.html ;
  **GCP Cloud Billing Catalog API**
  https://cloud.google.com/billing/docs/reference/rest/v1/services.skus/list
- Partition/zone-map pruning as a headline: Snowflake Query Profile; Spark "files
  pruned"; Iceberg/Parquet row-group skipping.
