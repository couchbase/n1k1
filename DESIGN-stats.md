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

## Visualizing the plan with live data-flow

The payoff of everything above: render the *executing* plan as a diagram and
**animate rows flowing edge→edge in real time**, so expensive shapes — nested-loop
joins, big sorts, spills — become viscerally obvious. The visualizer is just a
**consumer** of two things we already produce: the **plan graph** and the **per-op
snapshot stream**.

### What drives it
- **Plan graph** (nodes + edges) extracted once at start from the `base.Op` tree
  (`Kind` / `Children` / `Labels`). `EXPLAIN` already emits the plan, so this is
  largely in hand.
- **Per-op snapshot stream** at ~10 Hz (the stats core above), keyed by op id:
  `RowsIn`, `RowsOut`, rows/s, bytes, spill, wall-time.
- Each **edge's flow** = the child's `RowsOut` (= the parent's `RowsIn` on that
  input).

### The visceral signals (how NL joins get exposed)
- **Edge flow → animation speed / particle density / line thickness** (Sankey-style
  width ∝ rows). Watch a firehose pour *into* a node and a trickle come *out*.
- **Row amplification** (`RowsOut/RowsIn`): the Snowflake **"exploding join"** tell
  — a join emitting *more* rows than its inputs. Flash the edge/node red when the
  ratio blows up.
- **Work, not just rows** — the key to making NL joins look as bad as they are:
  instrument the join to count **inner probes / comparisons**, not only output
  rows. A nested-loop join over L×R does |L|×|R| comparisons — show that counter
  spinning wildly and the node glowing hot even while output is small, while a hash
  join (build once, probe once) stays calm. This needs a per-op **"work" counter**
  (probes / comparisons / hash inserts) in the `Stats` slot, beyond rows in/out.
- **Node heat = time share** → a live flame-graph-on-the-plan (which op is eating
  wall-clock right now).
- **Pipeline breakers:** blocking ops (`GROUP BY`, `ORDER BY`) visibly *inhale* all
  input and emit nothing until a final burst.
- **Spill:** when rhmap/store spills to mmap/disk, the node flips to a red "disk"
  state — "this fell out of memory."

### Render targets (ASCII / SVG / canvas)
1. **ASCII / TUI (default, works over SSH):** box-drawing nodes with live counters;
   edges animated with marching glyphs (`▸▸▸`) / color intensity by throughput; hot
   nodes glow. Build with charmbracelet **bubbletea + lipgloss** or **pterm**
   (dep); refresh ~10 Hz from snapshots.
2. **SVG (share / report):** emit a **self-contained** SVG of the plan (à la PEV2's
   single `pev2.html`) — a static heat map, or a recorded timeline replayed via
   inlined CSS/SMIL: a "query movie." No external refs.
3. **Canvas / web (rich, interactive):** an HTML page — or a claude.ai **Artifact**
   (self-contained, CSP-safe) — with SVG/canvas + JS particles flowing along edges
   at real throughput; live via SSE/WebSocket, or replayed from a trace. This is
   where "watch particles flood the NL join" really lands.

### Live vs replay + the query-trace format
Record `(plan graph + snapshot stream)` as a self-contained JSON **query trace**;
render it **live** (subscribe) or **replay/scrub** later for post-mortems ("why was
this slow?"). Same visualizer, two sources; the trace is shareable and can back an
Artifact.

### Layout & separation
- **Layout:** Reingold–Tilford for plan trees (parents centered over children —
  trivial pure-Go, no dep); layered Sugiyama for DAGs with shared subplans (dagre
  in the web target). ASCII can use a simple bottom-up indented box layout.
- **Separation:** the engine core stays render-agnostic — it already emits the
  snapshot stream and the plan, so ASCII / SVG / web are interchangeable front-ends
  over one trace. Reuses the "spinning numbers" partial previews for node-local
  aggregate values.

### Libraries (permissive) & prior art
- **Libraries:** bubbletea / lipgloss / bubbles (MIT), pterm (MIT, dep), tview
  (MIT), termdash (Apache-2.0); gonum/graph (BSD) or a hand-rolled Reingold–Tilford
  for layout; dagre (MIT) in a web artifact. **Avoid as a bundled dep:** Graphviz
  is **Eclipse Public License** (weak/file-level copyleft) — not GPL/AGPL, but
  outside the MIT/Apache-2 policy; use only as an *optional external `dot` binary*,
  never linked/vendored. Pure-Go layout sidesteps this.
- **Prior art:** PostgreSQL **PEV2** / explain.dalibo.com (plan tree, per-node
  time/rows/cost/buffers, self-contained `pev2.html`), explain.depesz.com;
  **Snowflake Query Profile** (the canonical row-explosion / exploding-join view);
  **Spark / Flink UI** (live DAG with per-stage input/output/shuffle rows+bytes);
  **Sankey diagrams** (edge width ∝ flow).
  - https://github.com/dalibo/pev2 ,
    https://medium.com/snowflake/understanding-the-exploding-joins-problem-in-snowflake-6b4f89f006c7 ,
    https://spark.apache.org/docs/latest/web-ui.html ,
    https://github.com/charmbracelet/bubbletea

## Parallel progress: racing bars for concurrent work

The other satisfying UI (think `docker pull`'s per-layer bars, or npm/pip/cargo
downloading many packages at once): **many progress bars racing rightward in
parallel.** It reads as "it's working hard" — and the *unevenness* is genuinely
diagnostic, not just eye-candy: bars advancing at different rates expose **data
skew and stragglers** (the one partition/lane that lags is your bottleneck).

### What becomes a "lane" (bar) in a query engine
Each independent unit of concurrent work gets its own bar:
- **`base.Stage` actors** (`NumActors`) — the built-in parallelism unit.
- **Parallel scans** over multiple files / partitions — one bar per file/partition.
- **Parallel `GROUP BY` shards** (per-actor maps merged at the end) — one bar each.
- **Ingest / index / transfer** — one bar per file being read/indexed/sent (this is
  the most natural fit, see below).
Each lane needs its own counters + a **denominator** (its total): file size, or the
manifest's per-partition `doc_count` (`DESIGN-data.md §5`). Lanes with no known
total render as spinners, not bars.

### It's the same core, keyed by lane
This reuses everything above — per-actor **local counters** (no atomics), rolled up
at the ~10 Hz checkpoint — just keyed by **lane/task id** instead of by operator.
The snapshot becomes a small `[]LaneStat{ id, label, current, total, rate, state }`.
The plan-flow diagram and the racing bars are **two lenses over the same stream**:
the diagram shows operator *relationships*; the bars show concurrent *tasks*.

### Scaling: bound the visible lanes
The trap is thousands of partitions → thousands of bars. Cap it like `docker pull`
does: show an **overall aggregate bar** plus the **top-K active/slowest lanes**, and
collapse the rest into a "…and M more" line. Lanes **appear/disappear** as tasks
start/finish (stable ordering, finished bars flip green/✓ then retire). Highlight
the straggler lane — that's the diagnostic payoff.

### Libraries
`vbauerster/mpb` (Unlicense) is purpose-built for concurrent multi-bar CLIs
(decorators, ETA, add/remove bars); `pterm` (MIT, dep) has a multi-area/progress
printer; bubbletea can do it with more control. All permissive. Prior art: `docker
pull` (per-layer bars), npm/pip/cargo parallel downloads, `mpb`'s own examples.

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
- **Per-op "work" counters** (join probes/comparisons, hash inserts) that power the
  data-flow visualization: worth the hot-path cost? Gate behind an
  explain-analyze / viz mode so normal runs pay nothing.
- **Visualization transport:** live streaming (SSE/WebSocket) vs record-then-replay
  as the default; and ASCII animation fidelity vs needing the web canvas.

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
