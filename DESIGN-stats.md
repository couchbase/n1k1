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

Beyond the raw plumbing, this doc also designs the **user-facing surface** that
governs it: a `-progress` flag + a `.stats` dot command (a verbosity dial + a
view/screen selector, in the family of the shipped `.timer`/`.explain`/`.meta`
controls — see `DESIGN-cli.md`); making **pruning** ("we skipped 99% of the
files") a headline signal; **record & playback** of a query's progress (a DVR /
TiVo, optionally popped open in a browser); and a **cost lens** — `EXPLAIN PRICE`
(what will this query likely cost, in $ / credits + wall-time) and `EXPLAIN COST`
(what did it actually cost), both riding the same measured counters.

## What n1k1 already has (a skeleton wired for exactly this)

This is not greenfield. The plumbing is scaffolded (and the counter core below is
now implemented — see "Implementation status"; `base.Stats` is no longer the empty
`struct{}` this section originally described):
- **`base.Stats`** (`base/stats.go`) — the counter core (`Counters []int64`,
  `Index`, `Ops`); originally an empty `struct{} // TODO` in `base/base.go`.
- **`type YieldStats func(*Stats) error`** (`base/base.go`) — the callback an
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
- **Pruning / skip counters (often the single most useful stat):** files /
  partitions / row-groups **considered** vs **actually opened**, plus bytes
  skipped — so the UI can shout *"read 88 of 9,500 files (skipped 99% via zone
  maps)"*. These come from the scan layer's manifest / zone-map checks
  (`DESIGN-data.md §5`) and index sargability (`DESIGN-indexing.md`), incremented
  **once per file/partition decision** (not per row), so they're nearly free. A
  query that pruned well vs one about to read the world is the difference this
  number exposes; it also feeds `EXPLAIN COST` (bytes not scanned = money not
  spent). See "Pruning visibility" below.
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

## A concrete counter core: one flat `[]int64` keyed by op id

The section above states the *policy* (local increments, lazy roll-up); this one
proposes a concrete data structure for it that fits n1k1's existing seams almost
exactly. The shape: **every op contributes a known set of `int64` counters, all
counters live in one big pre-sized `[]int64`, and a `map[string]int` maps a
human-readable `"opId:statName"` to that counter's index.** Each op owns a
contiguous sub-slice; `YieldStats` hands out the whole array; the receiver uses
the map to attribute counters back to operators (numbers, sparklines, or moving
bars next to each node in an `EXPLAIN`-style plan tree).

Most of this already exists in the tree — the point is to *reuse* it, not invent:

- **The op id already exists.** `ExecOp` threads `path`/`pathItem` through every
  op via `EmitPush(path, pathItem)` (`engine/op.go:27,126`), producing a unique
  string like `_0_1_2` as it descends the tree. That **is** the `opId` — and it
  already survives into the compiled path (codegen uses it to mint unique variable
  names), so the same key works in both the interpreted and `intermed` builds.
- **The layout is computed once, at convert time.** Each op kind declares its stat
  names (a small static `StatsDesc`, e.g. scan → `{RowsOut, BytesIn, FilesOpened,
  FilesPruned}`, joinNL → `{Probes, RowsOut}`). Walking the plan once sums every
  op's contribution into a total `N`, allocates one `make([]int64, N)`, and builds
  the `map[string]int` of `"path:statName" → index`. This is non-`lz` setup work —
  it runs at preparation time, never on the hot path.
- **The shared array lives on `Ctx`.** `base.Ctx` is per-request, immutable for the
  request's lifetime, concurrent-safe, and already shared across every actor's
  cloned `Vars` (`ChainExtend` → `Ctx.Clone`, `base/vars.go:41,124`). It is the
  natural home for both the `[]int64` and the index map. `YieldStats` then no
  longer needs to allocate a throwaway `Stats` per checkpoint (the bug flagged
  above) — it signals "the counters moved; come read them."

### Keep the per-row path free — flush at the checkpoint, don't atomic-add per row
The tempting version — `atomic.AddInt64(&arr[i], 1)` per row — is *correct* on
64-bit (`[]int64` elements are 8-byte aligned) but violates the core principle:
- An atomic add is a locked memory op on **every row**.
- Worse, if an op ever runs in **N actor goroutines that share its slots** (or even
  land on the same 64-byte cache line), those cores ping-pong that line on every
  increment — **false sharing** that erases the parallelism it's meant to measure.

So split it exactly as "Collection" prescribes: the per-row work is a **local**
`++` (a stack/struct counter, no atomics); at the **already-present 1024-row
`YieldStats` checkpoint** — which runs synchronously on the actor's own goroutine
(`engine/op_scan.go:139`) — the actor **flushes its local deltas** into its section
of the shared `[]int64`. Atomics (if any) touch the array ~once per 1024 rows, and
the only true concurrent reader is the ~10 Hz reporter, for which monotonic
per-field skew is acceptable (no seqlock needed — see Open questions).

### The index must be a compile-time constant, never a per-row map lookup
The `map[string]int` is a **planning + reporting** artifact only. Resolve
`"path:statName" → base offset` at convert time, bake the integer `base` into the
generated op closure, and the hot path is just `counters[base+RowsOut]++`. A map
lookup per row would dwarf the work being measured. This is what lets the scheme
survive codegen unchanged: `base` is an ordinary int computed in the non-`lz`
setup block; only the increment (and the checkpoint flush) crosses into `lz`.

### The concurrency dimension: per-op today, per-(op, actor) when needed
What the plan-time layout keys on determines whether flushes ever contend:
- **Today it's already contention-free.** The only fan-out is `op_union.go`, which
  spawns **one actor per child** (`base.NewStage(numChildren, …)`), and each actor
  runs a *distinct* child subtree with a *distinct* `path` → distinct sections → no
  two goroutines ever touch the same slot. "One section per `opId`" just works.
- **When same-op parallelism lands** (the doc's anticipated parallel scans /
  parallel `GROUP BY` shards — N actors running the *same* op/path), add an actor
  dimension: array size `= Σ over ops of (numStats × numActors)`, each `(op, actor)`
  gets private slots, and roll-up sums an op's actor rows at report time.
  `NumActors` is known at stage setup — before the hot path — so allocation stays
  one-shot and up-front; the hot path is unchanged.

### Why blocking ops don't complicate it
Pipeline breakers (`GROUP BY`, `ORDER BY`) are a *display* nuance ("inhale, then a
final burst"), not a counting one: each still runs in a goroutine that owns its
counters and increments them normally. The flat array is agnostic to streaming vs.
blocking — it only cares about "which goroutine owns which slots."

### Stat naming

Counter names are the public-ish surface (they key the index and label the plan
tree), so pin a convention:

- **Noun first ("NounAdjective"), not "AdjectiveNoun".** Lead with the *thing*
  counted so a subsystem's stats sort/cluster together: `RowsIn`, `RowsOut`,
  `RowsLeft`; `BytesIn`, `BytesOut`; `GroupsOut`. Not `InRows` / `OutRows` (which
  scatter `Rows` across the alphabet). When the counters are listed, everything
  about `Rows` is adjacent, everything about `Bytes` is adjacent, etc.
- **Monotonic is the default, and unmarked.** The overwhelming majority of
  counters only ever increase (cumulative totals); leave them unadorned so the
  common case stays terse (`RowsOut`, `Probes`, `GroupsOut`).
- **A current level takes a `Cur` suffix; a high-water mark takes `Peak`.** A value
  that can rise *and* fall — current memory, in-flight batches, live group-map
  entries — is a gauge, not a total: `MemCur`, `BatchesCur`; its high-water mark is
  `MemPeak`. The suffix (not a prefix) keeps the noun leading, so `Mem*` still
  clusters (`MemCur`, `MemPeak` adjacent).
- **On cbft/cbgt's `Tot`/`Cur` *prefixes*:** adopt the *distinction* (total vs.
  current) but flip it to a **suffix**, because a prefix sorts all `Tot*` together
  and all `Cur*` together — which fights the "group a noun's stats" goal above. And
  since monotonic dominates, make it the *unmarked* default rather than prefixing
  everything with `Tot`: shorter names, and the rarer gauges (`Cur`/`Peak`) are the
  ones that visually stand out — which is the right emphasis, since a gauge needs
  different rendering (a bar/needle) than a monotonic counter (a rising sparkline).
- **Future: a machine-readable `StatKind`.** If tooling should pick sparkline
  (monotonic) vs. gauge (level) rendering without string-parsing the suffix, carry
  a `StatKind` (Counter / Gauge / Peak) in the descriptor. Not needed yet — every
  registered counter today is monotonic — but the naming above is chosen to line up
  with that enum when it lands.

### Codegen safety (dev notes for adding counters)

n1k1 ops are **dual-mode source**: the same `op_*.go` runs directly in the
interpreter *and* is read by `cmd/intermed_build` to emit the compiled path
(`intermed/generated_by_intermed_build.go`). A few rules keep an added counter
working in **both**:

- **`lz`-prefix drives what gets emitted.** A source line assigning an
  `lz`-prefixed var from a non-`lz` expression is emitted with the value **baked**
  as a literal (`lzStatsBase := statsBase` → `Emit("lzStatsBase := %#v", statsBase)`).
  That is exactly how the op's base offset becomes a compile-time constant in the
  compiled path — so write `lzStatsBase := o.StatsBase` (or a passed-in
  `statsBase`) and index with `Counters[lzStatsBase+StatFooBar]`. **Never** index
  by a non-`lz` var directly in emitted code (it won't exist at runtime), and
  **never** do a per-row map lookup.
- **Per-row increments go on a local `lz` counter** (`lzStatRowsOut++`), which is
  emitted into the runtime loop and costs ~nothing even when stats are off. **Flush**
  that local into the shared slot only at a coarse point, guarded by
  `if lzVars != nil && lzVars.Ctx != nil && lzVars.Ctx.Stats != nil { ... }` — the
  same nil-guard idiom the scan checkpoint already uses. Scans flush at each
  1024-row `YieldStats` checkpoint (live); other ops currently flush once, after
  their child drains (final value correct; live intermediate cadence is a
  follow-up).
- **Signature changes ripple through regeneration, not by hand.** Adding a param
  (e.g. `statsBase int` to `ScanReaderAsCsv`) is fine: the `// !lz` call sites are
  gen-time and are re-emitted when you re-run `intermed_build`. The generated file
  is **gitignored** and must be regenerated — do not commit it.
- **Always regenerate + compile + run the suite in both modes.** After touching an
  op, run `go build ./cmd/intermed_build/ && ./intermed_build`, then
  `go build ./intermed/`, then the suite with `-tags n1ql` (see the worktree
  bootstrap in `DESIGN-testing.md` — the local-path `replace` stubs for the EE
  placeholder modules are what let a worktree build the `n1ql` path at all). The
  suite is the only check that exercises the *compiled* counters at runtime; the
  `engine` unit tests only cover the interpreter.

> **KNOWN LIMITATION — compiled path currently has NO stats (TODO).** The counter
> lines above are all marked `// <== genCompiler:hide`, so `cmd/intermed_build`
> omits them from `intermed/` and the compiled path collects nothing. This is a
> stopgap: the *compiler-test* codegen (`test/emit.OpToLines`) inlines the whole
> op tree into one function, and a per-op local counter (`lzStatRowsOut`) declared
> at op-entry but incremented inside the yield closure gets **cleared** when that
> closure is inlined at a child's call site (`clearFuncLines` keeps only lifted
> `var X = Y` idioms) → `undefined: lzStatRowsOut`. Naively fixing it surfaces two
> more codegen gaps: a plain `var X = 0` collides across sibling ops (redeclared),
> and the flush line carries *two* lifted vars (`lzStatsBase` + a counter) which
> the `varLift` format-arg path mis-aligns (`lzStatRowsIn%!s(int=0)`). So stats are
> interpreter-only for now (the CLI's live progress runs on the interpreter, so no
> user-visible loss). TO RE-ENABLE in the compiled path: drop the
> `genCompiler:hide` markers and make the counters survive inlining — give each a
> path-unique name via `// <== varLift: lzStat… by path` (as `lzValsReuse` does) AND
> teach `varLift` to align format args when several lifted vars share one line.
> Scans are exempt: they compile to a `glue.DatastoreOp` island, and `countingYield`
> already tracks their rows out in both modes.
- **Single-writer slots need no atomics — today.** Each op instance's section is
  written by one goroutine (a scan→filter→group pipeline all runs in one
  goroutine; only `Stage`/`UNION` actors split goroutines, and those are distinct
  subtrees → distinct sections). Plain `=`/`++` is safe. If same-op parallelism
  ever lands (parallel scan, parallel `GROUP BY` shards writing one op's slots),
  add the per-`(op, actor)` dimension described above *before* relying on the
  counts.

### Implementation status

Implemented (interpreter + compiled path, verified by the `n1ql` suite in both
modes):

- **`base/stats.go`** — `Stats{Counters, Index, Ops}`, the `StatsDescs` registry
  (op Kind → ordered stat names), `LayoutStats(root)` (pre-order walk sizing the
  array, assigning `Op.StatsBase`, building the `"opId:statName"` index), and
  `Stats.Get(key)`. `Op.StatsBase` (`json:"-"`) and `Ctx.Stats` added; the empty
  `Stats struct{}` placeholder removed.
- **Instrumented ops** (all monotonic counters): `scan` → `RowsOut` (live, flushed
  at the 1024-row checkpoint; also removed the per-checkpoint throwaway `Stats`
  alloc — `YieldStats` now receives the shared `Ctx.Stats`); `filter` →
  `RowsIn`/`RowsOut` (selectivity); `group`/`distinct` → `RowsIn`/`GroupsOut`
  (fan-in); `order-offset-limit` → `RowsIn`/`RowsOut`; the nested-loop `join`/`nest`/
  `unnest` family → `RowsLeft`/`Probes` (the exploding-join work signal). The
  **glue datastore scans** (`datastore-scan-records`/`-primary`/`-index`/
  `-index-cover`/`-fts`/`-keys`) also get `RowsOut` — instrumented in `glue`
  (`countingYield` in `glue/stats.go`), since the CLI's real file reads go through
  those, not the engine's `OpScan`; that wrapper also drives a live `YieldStats`
  checkpoint every 1024 rows (these scans have no built-in per-row checkpoint).
- **Single source of truth + greppable.** Every counter is one `base.DefStat(name,
  kinds…)` line (in `engine/stats.go` and `glue/stats.go`) that defines both the
  offset constant and the registered name together, so they can't drift. To list
  every counter straight from source (no doc drift): **`git grep '= base.DefStat'`**
  — one line per counter, across engine and glue, each showing its name and op
  Kind(s) (`git grep 'DefStat("RowsOut'` finds every op with a RowsOut). `DefStat` is
  idempotent, so the compiled path's re-run of an engine package's initializers
  doesn't double-register.
- **`glue`/CLI opt-in, live.** `Session.CollectStats` lays out the counters and
  returns them as `Result.Stats`; `Session.OnStats` receives the live `*Stats` at
  each checkpoint. The CLI exposes `-stats` and `.stats [on|off]`: it prints a
  per-operator footer (an indented op tree with each op's counters), and on a TTY
  redraws it live (throttled ~10 Hz, in place, on stderr) while the query runs.
  Off by default → zero cost.
- Nil `Ctx.Stats` ⇒ the zero-cost off path (unchanged default).

Not yet wired (follow-ups): `project`/`union`/`window` and hash-join counters;
`BytesIn` and pruning/`FilesOpened`/`FilesPruned` counters (need the
`DESIGN-data.md §5` manifest); per-op **live** flush cadence for non-scan ops (they
flush once at completion today, so mid-query they read 0 while the scan ticks); the
richer views/screens (`.stats view plan`, racing bars, DVR) and `EXPLAIN
PRICE`/`COST` over this core; and the `StatKind` gauge/peak marker.

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

## CLI control surface: the `-progress` flag & `.stats` dot command

All of the above is opt-in machinery; the user needs one obvious dial for **how
much** to show and **which view**. Model it on the controls already shipped
(`-meta`/`.meta`, `-scan`, `.timer`, `.explain`, `.version` — see
`DESIGN-cli.md`), so it feels native and stays zero-cost when off.

**Two orthogonal axes** — a *detail level* and a *view* — set by a startup flag
and adjustable live in the REPL:

- **Detail level (how many stats):** `off | auto | min | rich | debug`.
  - `off` — never collect/animate (fast path pays nothing; the counter gates stay
    nil, exactly as `op_scan.go` already nil-checks its hook).
  - `auto` (**default**) — DuckDB-style: stay silent, and only reveal a live
    display once a query crosses ~2 s; sub-second queries print nothing extra.
  - `min` — a single throttled status line (rows, rows/s, %, ETA).
  - `rich` — bars/spinners + partial-result previews + the pruning panel.
  - `debug` — also the per-op "work" counters (join probes, hash inserts) and the
    live plan-flow diagram; the only level that pays the hot-path work-counter cost
    (§ open questions), so it's explicitly gated here.
- **View (what visualization):** `line | bars | plan | pruning | preview`, each a
  front-end over the *same* snapshot stream (§ "Layout & separation"). `-progress`
  picks the initial level+view; a non-TTY / `NO_COLOR` / piped run forces `min`
  plaintext regardless (same discipline as the caret/colored-error code).

**Flip between screens during a long query.** In the interactive TUI the views are
**tabbed panels** over one live trace — press a hotkey (Tab, or `1`–`5`) to swap
between racing bars, the plan-flow diagram, the top-N partial-result leaderboard,
and the pruning panel *without* interrupting the running query. This is cheap
because switching views is purely render-side: the engine emits one trace; each
screen is a different projection of it. (bubbletea's model/update/view loop is a
natural fit; pterm's multi-area printer a simpler one.)

**Dot command (REPL), mirroring `.meta`/`.timer` idioms:**
```
.stats                 # show current level + view
.stats rich            # set detail level
.stats view plan       # switch the active screen (also Tab/1–5 while running)
.stats off             # disable
```
`-progress=<level>[:<view>]` is the batch/`-c` equivalent. Keep the surface small:
one dial, one view selector, sensible `auto` default — not a config forest.

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
- **Pruning:** files/partitions eliminated before opening render as **dimmed,
  struck-through, or greyed tiles** that never light up — you can *see* the query
  skip the world (see below).

### Pruning visibility: the "what did we skip?" view
The user's instinct is right: *if indexes/zone-maps skip huge swaths of files,
that's a headline, not a footnote.* Make it a first-class screen (and the
`pruning` view of § CLI control surface), driven by the pruning counters in
"What to measure":
- **The one-liner:** `scanned 88 / 9,500 files · 12 MB / 47 GB · pruned 99.3% (zone
  maps: 8,900, partition filter: 512)` — attributing *why* each swath was skipped
  (zone-map min/max miss, Hive/partition predicate, index sargability), sourced
  from `DESIGN-data.md §5` (manifest + zone maps) and `DESIGN-indexing.md` (index
  `RangeKey` sargability).
- **The visual:** a grid/treemap of files or partitions where **opened** tiles
  fill with throughput color and **pruned** tiles stay dark — a Hive-partitioned
  `year=/month=` tree literally lighting up only the matching partitions. This is
  the payoff shot for partition pruning.
- **Anti-signal too:** when pruning is *poor* (predicate not sargable, no zone
  map, `SELECT *` over everything), the panel is a wall of lit tiles — a visceral
  "you're reading everything; add an index / a WHERE / a partition column." Ties
  directly to `EXPLAIN COST` ($ saved by pruning vs $ spent scanning).
- **Cheap:** these are per-file/partition decisions (thousands, not billions), so
  the panel updates at the coarse cadence with no hot-path cost.

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
Artifact. This trace is the substrate for the DVR/TiVo controls and the
browser-open target described in "Record & playback" below.

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

## Record & playback: a DVR / TiVo for queries

The query-trace (§ "Live vs replay") turns progress into something you can
**rewind, pause, scrub, and re-watch** — a DVR for query execution. This is
high-value precisely because the interesting moment (the exploding join, the
spill, the straggler lane) is often *over* before you focused on it.

- **Always-on ring buffer (the DVR part).** Keep the last N query traces in a
  bounded in-memory ring (bytes-capped, oldest evicted) so *any* just-finished
  query can be replayed without having asked first — "wait, what just happened?"
  Traces are small (plan graph + a few hundred ~10 Hz snapshot frames), so a few
  MB covers a deep history. `.rec on` promotes recording to persistent
  (`.n1k1/traces/<ts>.json`); `-progress` at `rich`/`debug` auto-records to the
  ring.
- **Transport controls (the TiVo part).** Over a recorded (or paused-live) trace:
  `space` pause/resume, `←/→` step one snapshot frame, `,`/`.` slow/fast (0.25×–8×),
  `home`/`end` jump to start/finish, and a scrubber bar. Because a frame is an
  immutable snapshot, seeking is just indexing into the frame slice — the
  visualizer already renders one frame; replay only changes *which* frame and the
  clock driving it.
- **Live rewind.** Pausing a *running* query freezes the display (a rolling window
  of recent frames stays in the ring) while the engine keeps going; resuming
  snaps back to live. The engine never blocks on the viewer (latest-wins channel,
  § delivery (c)).
- **REPL surface:** `.rec [on|off]`, `.play [<trace>|last]`, `.play last` replaying
  the previous query from the ring — sibling to `.stats`.

### Pop it open in a browser
For anything richer than ASCII, **write the trace into a self-contained HTML page
and open it in the default browser** — the "explore this playback" gesture:
- **What:** one file, no external refs (CSP-safe, à la PEV2's `pev2.html`), with
  the trace inlined as JSON + a small player (SVG/canvas, particles flowing at
  recorded throughput, a scrubber). Static heat-map, animated **SMIL/CSS** "query
  movie," or interactive canvas — the § "Render targets" tiers, fed by a trace
  instead of a live stream.
- **How to launch:** the OS opener with **zero new deps** — `open` (macOS),
  `xdg-open` (Linux), `rundll32 …FileProtocolHandler` (Windows), behind a
  `.play --web` / `-progress=...:web` gesture. Or, in a Claude context, publish the
  page as an **Artifact** (self-contained, already CSP-constrained).
- **Why a browser:** scrubbing a timeline, hovering nodes for exact counters, and
  particle animation are things a terminal can only approximate; the same trace
  still replays in ASCII for SSH/headless. One trace, many players.

Prior art worth stealing from: **asciinema** (record/replay a terminal session as
a tiny self-contained cast + web player — the exact shape of "record once, scrub
in a browser later"); **rr** / time-travel debuggers (record-then-replay
execution); DVR/TiVo (the always-buffering-so-you-can-rewind mental model).

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

## `EXPLAIN PRICE` & `EXPLAIN COST`: dollars, not just rows

`EXPLAIN` shows the *plan*; two cost-flavored siblings answer the questions users
actually lose sleep over — **before**: "what will this query cost me?" and
**after**: "what did it just cost me?" Both are the stats core wearing a price tag.

- **`EXPLAIN PRICE` — a-priori estimate.** From the plan's cardinality/byte
  estimates (bytes to scan, egress bytes, object-store GET/LIST request counts,
  estimated compute-seconds) **×** a cloud pricing table, produce a **$ (or credit)
  range + a predicted wall-time**, with the assumptions shown. The canonical prior
  art is **BigQuery's dry-run** (`--dry_run` returns bytes-to-be-scanned, which ×
  on-demand $/TB = a price) and **Athena** (billed per TB scanned) — this is that,
  generalized. Crucially it's **pruning-aware**: the estimate must run *after*
  partition/zone-map pruning (`DESIGN-data.md §5`), so `WHERE year=2026` quotes the
  pruned bytes, and the panel can show *"$0.02 — pruning saved an estimated
  $6.40"*. Present as a range, not false precision (estimate error compounds).
- **`EXPLAIN COST` — a-posteriori actual.** After a run, price the **measured**
  counters we already collect — bytes actually scanned, egress, request counts,
  wall/compute time — against the same table: *"this query cost $0.018 (2.1 GB
  scanned, 140 GET, 3.2 s)"*. This is nearly free: it's the pruning/byte/time
  counters from "What to measure," multiplied by unit prices. A `.cost` toggle can
  append it as a footer next to the `.timer` line.

**Where the prices come from (and staying honest).**
- A small **pricing table** (per provider/region: $/GB scanned, $/GB egress,
  $/1k GET/LIST, $/compute-second or credit rates), cached locally (e.g.
  `.n1k1/pricing.json` or the user cache dir) and **refreshable** from public
  sources — **AWS Price List API**, **GCP Cloud Billing Catalog API**, Azure retail
  prices (all public JSON). Prices are facts, so no licensing landmine; ship a
  **bundled offline default** and a `--pricing <file>` / `.pricing` override so it
  works air-gapped and is auditable.
- **Local files cost ≈ $0** — n1k1 reads local disk today, so the honest answer for
  a local query is "$0 (local); ~3.2 s wall". The $ story becomes real with the
  **object-store backend** (`DESIGN-data.md` S3/gocloud): egress + GET + scanned
  bytes are what a lakehouse actually bills. Frame PRICE/COST as *"what this would
  cost against `s3://…` at current <provider/region> prices"* even when reading a
  local mirror — a genuinely useful pre-flight before pointing the same query at
  the cloud. Optionally, a fun-but-clearly-labeled local **energy/time** estimate
  (compute-seconds × a wattage guess) rather than pretending disk reads are free.

**How to wire it without a grammar fork.** True `EXPLAIN PRICE`/`EXPLAIN COST`
keywords would need patching the goyacc grammar — the same merge-hostile fork
change `DESIGN-data.md` rejects for inline table functions. Prefer, in order:
(1) **dot commands** `.price <stmt>` / `.cost [on|off]` (no parser change, matches
`.explain`); (2) a **CLI pre-parse intercept** that recognizes a leading `EXPLAIN
PRICE`/`EXPLAIN COST` and strips it to the inner statement before handing the rest
to the parser (a cheap string check, like the single-file arg detection). Both
reuse the existing plan (for PRICE) and the stats counters (for COST); neither
touches the fork.

## Open questions
- **Per-op tree vs single rolled-up number** as the default surface — cost vs
  usefulness.
- **Snapshot consistency:** accept per-field skew (monotonic counters) vs a
  seqlock/double-buffer for a coherent snapshot.
- **Where the reporter goroutine lives** — engine, glue, or CLI only.
- **Denominators:** how eagerly to compute totals (file sizes now; manifest
  `doc_count` when the `DESIGN-data.md` work lands) vs indeterminate progress.
- **Codegen path:** *(resolved for the counters landed so far — see "Codegen
  safety")* the base offset rides through as a baked literal and the suite verifies
  the compiled path at runtime; keep this invariant as more ops are instrumented.
- **Partial-result sampling policy:** first-N vs top-N-by-value vs a fixed watched
  set; and how firmly to guard previews from being consumed as final results.
- **Per-op "work" counters** (join probes/comparisons, hash inserts) that power the
  data-flow visualization: worth the hot-path cost? Gate behind an
  explain-analyze / viz mode so normal runs pay nothing.
- **Visualization transport:** live streaming (SSE/WebSocket) vs record-then-replay
  as the default; and ASCII animation fidelity vs needing the web canvas.
- **DVR ring bounds:** how many traces / how many MB to keep always-recorded, and
  whether `rich`/`debug` auto-record cost is acceptable when the user never replays.
- **Screen-flip UX:** tabbed panels vs a split dashboard; how many views before it's
  clutter; whether view state should persist across queries in a session.
- **Cost model fidelity:** $ vs credits (Snowflake/BigQuery-flat-rate don't map to
  per-byte $); PRICE estimate error bars; how stale a cached pricing table may get
  before it's misleading; provider/region defaults when unspecified.
- **PRICE without a real remote:** is a "what this *would* cost on s3://…" number
  for a local query illuminating or confusing? And is the local energy/time
  estimate worth including at all?

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
- Record/replay & DVR: **asciinema** (self-contained terminal cast + web player)
  https://asciinema.org/ ; **rr** time-travel debugger https://rr-project.org/ .
- Cost/pricing prior art: **BigQuery dry-run** byte estimate + on-demand pricing
  https://cloud.google.com/bigquery/docs/estimate-costs ; **Athena** per-TB-scanned
  billing (pruning = savings) https://docs.aws.amazon.com/athena/latest/ug/performance-tuning-data-optimization-techniques.html ;
  **AWS Price List API** https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/price-changes.html
  and **GCP Cloud Billing Catalog API**
  https://cloud.google.com/billing/docs/reference/rest/v1/services.skus/list
  (public pricing sources for the cached table).
- Partition/zone-map pruning as a headline stat: Snowflake pruning stats in Query
  Profile; Spark "files pruned"; Iceberg/Parquet row-group skipping.
