# Design: K-way sorted merge & merge joins (ASOF) for n1k1

Status: proposal

These operators are **generic over any time-ordered records** — log lines,
trades/quotes, sensor/IoT streams, telescope observations, financial transactions,
event streams. n1k1's core knows no specific file family; a "sorted source" is just
records carrying a comparable key. When many such sources share one comparable key,
a **K-way sorted merge** turns them into a single globally ordered stream *cheaply* —
O(N log K), often O(N) — which in turn makes **temporal correlation** (ASOF joins:
"what was the reference stream's value at the moment this event happened?"),
**windowed detection** (rate / burst / streak / gap over a `PARTITION BY … ORDER BY
ts`), and **de-duplicated global timelines** all fall out of one ordered pass instead
of the O(n²) correlated-subquery shape they parse as today.

The **driving (not exclusive) use case** is **PREPARE++** (`DESIGN-prepare.md`):
thousands of SQL++ "detector" queries run over unzipped `cbcollect_info` support
bundles, whose bulk is date/time-sorted (or near-sorted) log files — `ns_server.*.log`,
`diag.log`, `indexer.log`, `master_events.log`, per-node rebalance traces. That is the
recurring concrete example throughout this doc (its shapes are vivid and real), but it
stands in for the general class; read "rebalance state at the time of an error" as a
stand-in for "quote at the time of a trade," "calibration frame at the time of an
observation," and so on.

The hard constraint that shapes the whole design: **the cbq grammar/parser is
off-limits.** n1k1 parses and plans through the private `couchbase/query` fork
(`n1k1-query`); a new `ASOF` keyword or `merge(...)` table function would mean
grammar/lexer edits and perpetual fork divergence. So every capability here must
land as a **plan-time / operator optimization over stock SQL++ idioms** the parser
already accepts — recognized at conversion or plan-rewrite time and lowered to new
`base.Op`s. New runtime, no new syntax. This mirrors the position already taken in
`DESIGN-prepare.md` ("Detectors stay in stock SQL++") and `DESIGN-data.md`
("everything shipped needed zero fork changes").

## Contents

- [Background: what we ride on](#background)
- [The sorted-source contract (recap of `DESIGN-data.md`)](#contract)
- [§1 The K-way sorted merge SCAN op](#merge-scan)
  - [Three regimes: concatenate, min-heap, watermarked-near](#regimes)
  - [Cursor management & seek-by-time via sync points](#cursors)
  - [Backpressure in the push model](#backpressure)
  - [Spill](#merge-spill)
  - [Soft options: disorder tolerance, late-record policy, bound validation](#soft-options)
- [§2 The sorted merge JOIN op](#merge-join)
  - [Equi merge-join over sorted keys](#equi-merge)
  - [ASOF — nearest-preceding](#asof)
  - [Soft ASOF — tolerance window / max look-back](#soft-asof)
  - [Push mechanics, re-entrancy, spill; feeding from the K-way merge](#join-mechanics)
- [§3 Grammar-free surfacing](#surfacing)
  - [The argmax-subquery → ASOF rewrite](#argmax-rewrite)
  - [UNION ALL of sorted streams → merge](#unionall-merge)
  - [Window functions ride the ordered stream](#window-streams)
  - [Table-valued functions in FROM — a verdict](#tvf-verdict)
  - [Where recognition lives](#recognition-home)
- [§4 NEST — is it interesting here? (a verdict)](#nest)
- [Coherence with the other docs](#coherence)
- [Phasing](#phasing)
- [Open questions](#open-questions)
- [Sources](#sources)

## Background: what we ride on <a name="background"></a>

Four pieces of existing machinery make this cheap to build; a fifth is the one real
prerequisite gap.

- **Push-based execution over `[]byte`** (`DESIGN.md`, "Performance approaches"). A
  scan *pushes* each row into a `base.YieldVals` closure; operators chain by function
  call, not by `HasNext()` pull. `base.Val = []byte`, `base.Vals = [][]byte`, fields
  addressed by positional `base.Labels` "registers", parsed allocation-free with
  `jsonparser`. A merge op is therefore *a scan-shaped source*: it owns K child
  cursors and calls one downstream `yield` per emitted row — it slots into the same
  pipeline as `datastore-scan`.
- **The spillable max-heap** (`base/heap.go`, `HeapValsProjected` over
  `couchbase/rhmap/store.Heap`; `CreateHeapValsProjected`). ORDER BY / OFFSET / LIMIT
  already build a heap that **spills to temp files** when it outgrows memory
  (`DESIGN.md`: "max-heap that becomes too large will spill"). The K-way merge's
  small K-entry frontier heap, and the watermarked-near reorder buffer, reuse this
  spill substrate rather than inventing one.
- **Actor-per-branch fan-in over data-staging** (`base.Stage`, `Stage.BatchCh`;
  `StartActor` / `ProcessBatchesFromActors` in `base/stage.go`; used end-to-end by
  `OpUnionAll`, `engine/op_union.go`). `UNION ALL` today runs **one actor goroutine
  per branch**, each deep-copying rows into recycled batches sent over a
  credit-bounded `BatchCh`, drained by a consumer. **This is exactly the substrate a
  K-way merge needs** — the merge is `OpUnionAll`'s fan-in with an *ordering
  discipline* on the consumer: instead of draining batches in arrival order, the
  consumer peeks each actor's head row and pops the minimum. Actor-per-cursor also
  solves the stepping problem ([§1 cursors](#cursors)): push ops run to completion, so
  you cannot "call `yield` once and pause" — but a cursor running in its own actor
  *naturally* parks at the `BatchCh` send when the consumer stops crediting it.
- **Dedup patterns for timelines.** `trackSet` (`glue/recursive.go`,
  `recurDedupCycleRestrict`) is a canonical-JSON-keyed `map[string]bool` the
  `WITH RECURSIVE` fixpoint uses to drop duplicates; the hash-join/set ops do the same
  idea **zero-copy** via `RHStore` keyed on `ValsEncodeCanonical`. A merge that emits a
  de-duplicated global timeline reuses whichever fits (the zero-copy `RHStore` on the
  hot path).
- **UNION ALL has since landed** (`glue/conv.go` `VisitUnionAll`, kind `union-all`;
  by-name label union across branches). `DESIGN-data.md` still lists
  `plan.UnionAll` as `NA()` — **that note is now stale**; the conversion exists.
  This matters because "UNION ALL of sorted streams" is our most natural merge
  trigger ([below](#unionall-merge)); the *plumbing* to accept the plan shape is
  already there, and the merge is an **execution refinement** of an operator that
  already runs correctly (if unordered).

What we lower to today (all `glue/conv.go`, all executing as n1k1 `base.Op`s, fork
untouched):

| Plan op | Lowered to | Note |
|---|---|---|
| `PrimaryScan` / `IndexScan` | `datastore-scan-*` | push rows as `[]byte` |
| `Join` (ON KEYS) | `joinKeys-inner`/`-leftOuter` | lookup fetch by evaluated keys |
| `NLJoin` (ANSI) | `joinNL` (`OpJoinNestedLoop`) | **re-drives the right branch per left row** |
| `HashJoin` (ANSI, 1-key equi) | `joinHash-inner` (`OpJoinHash`) | build probe map one side |
| `Order` | `order-offset-limit` (max-heap, spill) | folds OFFSET/LIMIT |
| `UnionAll` | `union-all` | by-name label union |
| `IntersectAll`/`ExceptAll` | reuse `OpJoinHash` | probe-map set ops |
| `WindowAggregate` | window op (`engine/op_window.go`) | `OVER (PARTITION BY … ORDER BY …)` |
| `Nest` / `NLNest` / `HashNest` / `IndexNest` | **`NA()`** | none convert (see [§4](#nest)) |

The load-bearing observation: today an ANSI join **re-drives the inner branch per
outer row** (`joinNL` = O(n·m)), and the ASOF idiom parses as a **correlated
subquery** (a nested-loop with an inner ORDER BY … LIMIT 1 per outer row) — the
worst case this doc exists to replace. When both inputs are already time-ordered,
the merge op replaces that quadratic re-drive with a single co-advancing pass.

## The sorted-source contract (recap of `DESIGN-data.md`) <a name="contract"></a>

This doc **builds on** the "Sorted & near-sorted sources: the merge-join contract"
section of `DESIGN-data.md` (§4 extract provider + §5 manifest). Treat that as
authoritative; the merge ops here are *consumers* of the metadata it produces. The
contract, in brief:

- **Normalized sort key.** The extract layer (`describe(file) → ExtractSpec`, an
  `*.extract.js` function per `DESIGN-extensions.md`) normalizes each source's own
  timestamp format / timezone / precision into ONE comparable **int64 epoch-nanos**
  key. This is what makes cross-source ordering meaningful at all: a merge compares
  int64s, never re-parses `"2026-07-08T14:22:01.123Z"` vs `"Jul 08 14:22:01"` vs an
  exchange's microsecond epoch.
- **The metadata is computed once and cached.** `describe(file) → ExtractSpec` is
  **memoized in the `.n1k1/` sidecar — once per file, ever** (`DESIGN-data.md` §4,
  `DESIGN-extensions.md`); the per-record `extract(file, meta, emit)` is then handed
  that cached spec. **The merge consumes the *cached* result, never re-deriving it:**
  the merge planner consults the memoized `describe` output **at plan time** (to pick
  the regime, seed the `disorder_bound`, and read the `min_key`/`max_key` zone maps for
  disjointness/pruning), and each merge cursor reads the cached `sort_key` /
  `sortedness` / `disorder_bound` / sync-point offsets from the sidecar at run time.
  So classification and key normalization are *not* on the merge's hot path — they were
  paid once when the file was first described.
- **Sortedness classification** per source: **`strict`** (key non-decreasing),
  **`near`** (bounded disorder), **`none`** (must spill-sort before merging).
- **`disorder_bound`** for `near` sources — either `{window: Δt}` (bounded
  lateness, the Flink/Dataflow watermark model) or `{span: N}` (max positional
  displacement). Declared by the format author or measured by sampling. **It is a
  claim, and a wrong claim silently corrupts a merge** — so the merge op MUST
  validate it at runtime.
- **Manifest zone maps** — `min_key` / `max_key` per file (`DESIGN-data.md` §5) —
  and periodic **key→offset sync points** that let a cursor seek to a start time and
  double as seekable doc-IDs (`<relpath>#<line>@<offset>`, already shipped for
  JSONL/YAML).
- **Sketched merge behaviors** (this doc deepens them into a real op): disjoint
  ranges → concatenate; strict → min-heap; near → watermarked buffer; validate the
  bound.

---

# §1 The K-way sorted merge SCAN op <a name="merge-scan"></a>

The merge-scan is a **source op** that presents K sorted files (or K sorted
sub-streams) as one stream ordered by the normalized int64 key. Its inputs are K
child scans plus the per-source metadata from the contract; its output is a single
`yield`-driven ordered stream carrying the same opaque-document rows the underlying
scans produce, with the normalized key available as a labeled register (so
downstream ops sort/compare on the int64, never re-parse).

Kind sketch: `merge-sorted` with `Params` = `{keyExpr, keyKind:int64, regime,
disorder_bound, late_policy, validate}` and `Children` = the K child scan ops.
Crucially it is a **new op kind added n1k1-side**, not a new plan op or scan kind —
so it stays compiler-safe exactly like the data-source work (`DESIGN-data.md`,
"Compiler compatibility"): the op carries only static choices in `Params` and live
handles in `Temps`.

## Three regimes: concatenate, min-heap, watermarked-near <a name="regimes"></a>

The op picks a regime from the manifest metadata; the cheapest legal one wins. All
three emit into the *same* downstream `yield`, so downstream operators never know
which ran.

### (a) Concatenate — no heap, no comparisons <a name="regime-concat"></a>

When the files' key ranges are **disjoint and ordered** —
`max_key(fᵢ) ≤ min_key(fᵢ₊₁)` for the manifest's sort of files — the merged order is
just the files read back-to-back. This is the common case for **dated log rotations**
(`ns_server.info.2026-07-06.log`, `…07-07.log`, `…07-08.log`): each day's file wholly
precedes the next. Cost: O(N), zero key comparisons, zero buffering, one open cursor
at a time. The op reads file 1 to exhaustion, then file 2, … The manifest's
`min_key`/`max_key` zone maps are exactly the inputs that let us *prove* disjointness
without opening anything.

> A near-source can still concatenate at file granularity if its `disorder_bound` is
> smaller than the inter-file gap — a file may be internally jittery yet wholly
> precede the next file. The regime is chosen per boundary, not globally.

### (b) Min-heap merge — strict sources, overlapping ranges <a name="regime-heap"></a>

When ranges overlap and every source is **`strict`**, run the classic K-way merge:
a K-entry min-heap keyed on the frontier (current head key) of each cursor. Pop the
minimum, `yield` it, advance that cursor, re-push its new head. Cost O(N log K); K is
the number of *live* cursors (files whose ranges overlap the current key), typically
tiny (one file per node, a handful of nodes). The heap is `base/heap.go` machinery
inverted to a min-heap over `{key:int64, cursorIdx}` — small (K entries), never
spills. Only the row *bytes* are large, and they stay in the child cursor's reused
buffer until popped (borrow contract: copy on `yield` per `YieldVals`).

This is the sort-merge classic, and the workhorse for **concurrent per-node logs**
of the same subsystem across a cluster (`node1/indexer.log`, `node2/indexer.log`, …)
where events genuinely interleave in time.

### (c) Watermarked-near — bounded disorder <a name="regime-near"></a>

A `near` source violates the heap invariant: its head key is *not* a lower bound on
its remaining keys — a record up to `disorder_bound` out of order may still arrive.
Emitting on head-key-min alone could emit a row before a smaller-keyed row that
hasn't surfaced yet. The fix is the **watermark / reorder buffer** (the
Flink/Dataflow model, applied to a *bounded* offline stream):

- Maintain a **frontier** = `min over live cursors(head_key)`.
- Compute a **watermark** = `frontier − max(disorder_bound over live near cursors)`
  (for a `{window: Δt}` bound; for a `{span: N}` bound the watermark is instead "the
  key N positions back on the laggard cursor").
- Buffer incoming rows in a small min-heap (again `base/heap.go`); a row is **safe to
  emit** only once its key `≤ watermark` — nothing smaller can still arrive. Pop-emit
  all safe rows, advance, recompute.
- The buffer is bounded by `disorder_bound × arrival_rate` (time bound) or `N` rows
  (span bound). If it exceeds the memory budget, it **spills** via the same
  `store.Heap` path ORDER BY uses — degrading to disk, never to wrong answers.

The buffer is what converts "near-sorted, cheaply" into "strictly ordered output" at
O(N log B) where B is the (small, bounded) buffer size — vastly cheaper than a full
spill-sort (`none` regime), which is the fallback when a source declares no usable
bound.

## Cursor management & seek-by-time via sync points <a name="cursors"></a>

- **A cursor** is a child scan plus its current head row + head key. Push ops run to
  completion — you cannot "call `yield` once and pause" a scan in-line. The idiomatic
  n1k1 stepping mechanism is therefore the **actor-per-cursor** shape `OpUnionAll`
  already uses: each child scan runs in its own `Stage` actor goroutine pushing
  batches over a credit-bounded `BatchCh`; the merge consumer holds the head row of
  each actor's current batch, pops the minimum, and *credits that actor for one more*
  — which is what unblocks its next push. A cursor with no outstanding credit simply
  parks at its `BatchCh` send. This gives one-row-at-a-time stepping across K cursors
  with the engine's existing flow control and **no new concurrency primitive** — it is
  `OpUnionAll` with an ordering discipline on the drain. (This resumable-cursor
  requirement is the crux; see [§2 mechanics](#join-mechanics).)
- **Lazy cursor opening.** With concatenate, only one cursor is open at a time. With
  heap/near, a file whose `min_key` is beyond the current frontier need not be opened
  until the frontier reaches it — the zone map gates cursor creation, bounding open
  FDs and buffers to the *overlapping* set, not all K files.
- **Seek-by-time.** The manifest's key→offset sync points let a merge that is
  bounded by a query predicate (`WHERE ts >= '2026-07-08T00:00'`) **skip directly**
  to the first relevant record in each file — `os.Seek` to the sync-point offset at or
  before the start key, then scan forward. This is the temporal analog of a sargable
  range scan, and it reuses the seekable doc-ID machinery (`<relpath>#…@<offset>`)
  already built for fetch. A merge over a 30-day bundle answering a 1-hour question
  opens a handful of blocks, not 30 files end-to-end. **Predicate pushdown to the
  merge is therefore the single highest-leverage optimization** — and it depends on
  the same "predicate must reach the scan" work `DESIGN-data.md` §5 calls out as a
  prerequisite for zone-map pruning.

## Backpressure in the push model <a name="backpressure"></a>

The merge is a **pipeline breaker only to the degree the regime forces**:
concatenate and heap are streaming (bounded state: K frontier entries); watermarked-
near holds a bounded buffer; full spill-sort (`none`) is a true breaker. Downstream
backpressure works the natural push way — the merge calls `yield` and only advances
when `yield` returns; a slow consumer simply slows the pop loop, no unbounded
buffering. For a *parallel* merge (producers per file group feeding one frontier-
merging consumer over `Stage.BatchCh`), the existing credit/window flow-control on
`BatchCh` bounds in-flight batches per producer — the merge inherits the engine's
data-staging backpressure rather than adding its own.

## Spill <a name="merge-spill"></a>

Nothing in concatenate or strict-heap needs spill (state is K frontier entries). The
watermarked-near buffer and the `none` full-sort fallback both spill through
`store.Heap` (`base/heap.go`) — the identical temp-file path ORDER BY uses, so
memory pressure degrades gracefully to disk. A merge should **never** hold more than
`bound × rate` (near) live rows; if a source lies about its bound, the buffer grows
and either spills (safe, slow) or trips the validation policy ([below](#soft-options)).

## Soft options: disorder tolerance, late-record policy, bound validation <a name="soft-options"></a>

These knobs are what make the merge *robust* over messy real logs, and they are the
correctness heart of the design. All are Params on the op, defaulted from the
manifest / `ExtractSpec` and overridable per query (via a catalog / detector
metadata field — never new SQL syntax).

- **Disorder tolerance** = the effective `disorder_bound` the op enforces. Wider =
  more buffering (bigger/slower) but tolerates jitterier sources; narrower = cheaper
  but risks late records. Default = the source's declared/measured bound.
- **Late-record policy** — what to do with a record that arrives *below the
  watermark* (older than we've already emitted past), i.e. the bound was too small:
  - **`widen`** (default for exploratory use) — widen the effective bound, re-buffer,
    and emit a `Warn` (the same warning stream divide-by-zero uses). Self-healing but
    silently changes memory behavior.
  - **`error`** — fail the query with a clear "source X violated disorder_bound at key
    K" (the safe default for a *correctness-critical* detector).
  - **`drop`** — drop the late record and count it in stats (Flink's default; fine for
    approximate rate/burst detection where a stray old line doesn't change the
    verdict).
  - **`resort`** — fall back to a full spill-sort of the offending source (correct,
    expensive) — the escape hatch that trades the merge's speed for guaranteed order.
- **Bound-validation strictness** — even under `widen`/`drop`, the op **always checks
  monotonicity of its own output** (each emitted key ≥ the last). This is cheap
  (one int64 compare per row) and is the tripwire that catches a wrong `disorder_bound`
  before it corrupts a downstream ASOF join. A validation failure routes to the
  late-record policy. **The doc-level warning is blunt: a wrong bound is a silent
  data-corruption bug — the merge must be paranoid about it, because nothing
  downstream can detect an out-of-order stream that was promised to be ordered.**

Worked shape — merge every node's `ns_server` log into one cluster timeline:

```sql
-- Stock SQL++: UNION ALL of per-node keyspaces, ORDER BY the normalized key.
-- Recognized (see §3) and executed as a watermarked-near K-way merge, NOT a
-- materialize-then-sort, because each source is near-sorted with a declared bound.
SELECT l.node, l.ts, l.level, l.msg
  FROM `n0/ns_server.info.log` l
  UNION ALL SELECT l.node, l.ts, l.level, l.msg FROM `n1/ns_server.info.log` l
  UNION ALL SELECT l.node, l.ts, l.level, l.msg FROM `n2/ns_server.info.log` l
 ORDER BY l.ts
```

---

# §2 The sorted merge JOIN op <a name="merge-join"></a>

Given ordered inputs (from a merge-scan, or any already-sorted source), a **merge
join** co-advances two cursors instead of re-driving the inner branch per outer row.
Three variants: equi merge-join, ASOF (nearest-preceding), and soft ASOF.

## Equi merge-join over sorted keys <a name="equi-merge"></a>

Standard sort-merge equijoin: both inputs sorted by the join key; advance the
lagging cursor; on key equality emit the cross-product of the equal-key groups
(buffer the right group when the left has duplicates). This is the ordered analog of
`joinHash-inner`. For the support-bundle case it is *less* central than ASOF (logs
rarely join on an exact shared timestamp), but it is the correct lowering when the
planner sees an equijoin on keys both sides are already sorted on — and it is the
foundation the ASOF variant specializes. Cost O(N+M) vs the hash join's O(N+M)
with a build-side map; the merge wins when inputs are *already* sorted (no build
phase, no probe map, streaming, spill-free) and loses when they aren't (a sort is a
pipeline breaker — so only choose merge-join when sortedness is free).

## ASOF — nearest-preceding <a name="asof"></a>

ASOF is the temporal star of this doc. **Definition:** for each left (probe) row,
find the single right (build) row with the **greatest key that is ≤ the left key**
(nearest-preceding; DuckDB/ClickHouse/kdb+ "backward" ASOF), optionally partitioned
by equality keys. Semantically it answers *"what was the most recent state of Y at
the moment X happened?"* — the canonical support question: *what rebalance state was
in effect when this error was logged?*

**Mechanics (nearest-preceding, both inputs ascending by key):**

- Keep a single **`held`** row = the latest right row seen whose key ≤ the current
  left key. Initialize empty.
- Advance the right cursor while `right.key ≤ left.key`, updating `held` to the last
  such right row (this is the "argmax" — the right group's max key not exceeding the
  left key). Stop when `right.key > left.key`.
- Emit `left ⋈ held` (or `left ⋈ NULL` for the outer/no-preceding case). Advance the
  left cursor and repeat — never rewinding the right cursor.

This is **one linear pass**, O(N+M), holding exactly one right row (plus the
equality-partition bookkeeping) — the direct replacement for the O(n·m) correlated-
argmax subquery ([§3](#argmax-rewrite)). With **partition keys** (equality
conditions besides the inequality), maintain one `held` per active partition; over a
merge-scan the partitions arrive interleaved in key order, so a small map of
`partition → held row` suffices, evicted as partitions fall out of the frontier.

DuckDB's planner materializes this as a specialized operator with a per-partition
sorted lookup ("Planning AsOf Joins"); we get the same asymptotics for free when the
inputs already flow ordered out of a merge-scan — which, in the bundle case, they do.

## Soft ASOF — tolerance window / max look-back <a name="soft-asof"></a>

Plain ASOF matches the nearest preceding row *however old*. For logs that is often
wrong: if the last rebalance was 3 days before this error, calling that error
"during rebalance R" is nonsense. **Soft ASOF bounds the staleness** with a maximum
look-back Δt (or forward tolerance). Two precise meanings — a query/detector picks
one via metadata:

- **Within-tolerance-or-null.** Match nearest-preceding **only if**
  `left.key − held.key ≤ Δt`; otherwise emit NULL (like an outer ASOF that expired).
  Use when "no recent state" is itself meaningful ("error with no rebalance in the
  prior 5 minutes").
- **Bounded-staleness (drop).** Same, but drop the left row entirely when no match is
  within Δt — use when you only care about correlated events.
- **Nearest (either side).** Match the closest right row within ±Δt regardless of
  direction (pandas `merge_asof(direction='nearest', tolerance=…)`) — for "the
  measurement taken closest to this event." Costs a one-row look-ahead on the right
  (hold both the last-preceding and first-following, pick the closer).

Mechanically soft ASOF is plain ASOF plus a single subtraction-and-compare against Δt
at emit time (and, for `nearest`, one buffered look-ahead row). It is *cheaper* than
plain ASOF in practice because the Δt bound lets the right cursor **discard held rows
that fall out of the window**, capping per-partition state. "Soft" = the join is a
*fuzzy* temporal lookup with an explicit tolerance, which is exactly DuckDB's framing
("AsOf Joins: Fuzzy Temporal Lookups").

Worked shape — annotate each error with the rebalance state in effect, but only if a
rebalance touched the last 10 minutes:

```sql
-- Stock SQL++ argmax subquery with a look-back guard in the WHERE.
-- Recognized (§3) as SOFT ASOF (within-tolerance-or-null, Δt = 10m) over the
-- merge of errors and master_events, both time-ordered.
SELECT e.ts, e.node, e.msg,
       (SELECT r.stage FROM `master_events.log` r
         WHERE r.type = "rebalance"
           AND r.ts <= e.ts
           AND r.ts >= e.ts - 600000        -- 10-minute look-back → the tolerance
         ORDER BY r.ts DESC LIMIT 1) AS rebalance_stage
  FROM `diag.log` e
 WHERE e.level = "ERROR"
```

## Push mechanics, re-entrancy, spill; feeding from the K-way merge <a name="join-mechanics"></a>

- **Push mechanics.** The merge-join is driven by the left stream's `yield`: each
  left row triggers the "advance right until `> left.key`, update `held`, emit"
  step. State between left rows is just `held` (+ the partition map + at most one
  look-ahead) — tiny and bounded, so the join is **streaming, not a pipeline
  breaker**, unlike hash join's build phase or a sort.
- **Re-entrancy is the crux.** A merge join must consume the right stream
  *incrementally* interleaved with the left — advance right a bit, emit, advance right
  a bit more. Push-based ops run to completion, so the right side must be a
  **resumable cursor** we can step one row at a time. Note this is *not* what the
  nested-loop join does — it re-executes the whole inner branch from scratch per left
  row (`ExecOp` on `Children[1]` per left row in `OpJoinNestedLoop`), the O(n·m) cost
  we're replacing. The merge instead uses the **actor-per-cursor** stepping from
  [§1](#cursors): the right stream runs in a `Stage` actor, and the merge-join
  credits it forward as the left key advances. Same mechanism as the merge-scan, no
  new primitive; a single-input merge-join over two already-sorted actors is a
  two-cursor case of the same consumer loop.
- **Spill.** Nearest-preceding ASOF holds one row per active partition — bounded,
  no spill in the common case. Two cases can grow: (1) equi merge-join with large
  equal-key duplicate groups buffers the right group (spill via `store.Heap` if
  huge); (2) a partition map with many long-lived partitions — cap it and spill cold
  partitions. Both reuse the ORDER BY spill path.
- **Feeding from the K-way merge.** The ideal shape is
  `merge-join( merge-scan(left files…), merge-scan(right files…) )` — the merge-scans
  produce globally ordered streams per side, the merge-join co-advances them. Because
  both are push sources with parked cursors, they compose without materialization:
  one ordered pass over the whole bundle answers "every error joined to its
  concurrent rebalance state," across all nodes, with bounded memory. This is the
  end-to-end win the doc is for.

---

# §3 Grammar-free surfacing <a name="surfacing"></a>

None of §1–§2 adds SQL++ syntax. Each capability is triggered by recognizing a
**stock idiom** and lowering it to the merge ops. The recognition must be *robust*
(no false positives that silently change semantics) and its **canonical form** is
what detector authors — human or agent — are told to write.

## The argmax-subquery → ASOF rewrite <a name="argmax-rewrite"></a>

**Canonical form** (the shape detector authors should write, and the shape the
recognizer matches):

```sql
SELECT e.*,
       (SELECT r.<field> FROM <right> r
         WHERE r.<key> <=  e.<key>            -- (A) one inequality vs an outer key
           [ AND r.<eqk> = e.<eqk> ]*         -- (B) zero+ equality (partition) preds
           [ AND r.<key> >= e.<key> - <Δt> ]  -- (C) optional look-back → SOFT ASOF
         ORDER BY r.<key> DESC LIMIT 1) AS <alias>   -- (D) argmax by the same key
  FROM <left> e
```

**Recognition predicate** — rewrite to ASOF **only if all hold** (else leave as the
correlated subquery, which is always correct):

1. The subquery is **correlated** to the outer via exactly the `<key>` inequality (A)
   and zero-or-more equality predicates (B); no other correlation.
2. It is `ORDER BY r.<key>  <dir>  LIMIT 1` where `<dir>` and the inequality agree —
   `<= … ORDER BY DESC LIMIT 1` = nearest-**preceding**; `>= … ORDER BY ASC LIMIT 1`
   = nearest-**following**. A mismatch is *not* an argmax and must not be rewritten.
3. The projected value is a plain field (or a small whitelist of exprs) of `r` — a
   scalar per outer row (LIMIT 1).
4. Both `<left>` and `<right>` are **orderable by `<key>`** — a sorted/near-sorted
   source per the contract, or cheaply sortable. Absent that, the rewrite still
   *works* but must pay a sort; the planner may decline if the sort dominates.
5. Optional predicate (C) `r.<key> >= e.<key> − <Δt>` (constant Δt) ⇒ **soft ASOF**,
   within-tolerance, with that Δt. Anything more complex in the WHERE ⇒ don't rewrite.

**Robustness.** The rewrite is **semantics-preserving by construction** — ASOF
computes exactly the argmax the subquery specifies — so a *false negative* (missing a
rewritable subquery) only costs speed, never correctness. The danger is a *false
positive*: matching a subquery that isn't really argmax (e.g. `LIMIT 2`, an
additional non-equality correlation, an aggregate projection, a `<key>` that differs
between the ORDER BY and the inequality). The recognizer must be **conservative** —
require the exact shape, bail on anything else. This is the same "recognize a narrow
canonical form, fall back safely otherwise" discipline as const-folding boxed exprs
in `DESIGN-prepare.md`.

**Multi-row (JOIN-shaped) variant.** The same idiom also appears as a correlated
subquery in a `FROM`/`JOIN` position or as a `LEFT JOIN … ON r.key <= e.key` that a
later `ORDER BY … LIMIT 1 per group` collapses. Phase 1 targets the scalar-subquery
form (the one detector authors naturally write); the JOIN-shaped form is a Phase-2
generalization.

## UNION ALL of sorted streams → merge <a name="unionall-merge"></a>

`UNION ALL` of several time-ordered keyspaces, wrapped by `ORDER BY <key>`, is the
natural "one global timeline" idiom — and, since `VisitUnionAll` landed
([Background](#background)), it already *runs* (as `union-all` feeding a heap
`order`). The optimization: when the recognizer sees `ORDER BY <key>` directly over a
`union-all` whose branches are all **sorted/near-sorted on `<key>`** (per the
contract), replace `order(union-all(…))` — which materializes and heap-sorts the
whole concatenation — with a **`merge-sorted`** over the same branches. Same output,
but O(N log K) streaming (or O(N) concatenate) instead of a full spill-sort of the
entire bundle.

**Recognition predicate:** an `order` op whose single child is `union-all`, whose ORDER
BY keys are a prefix of / equal to the branches' declared sort keys, and whose
branches are all orderable sources. The `union-all` op's by-name label reconciliation
already handles heterogeneous branch shapes; the merge just adds ordering. Safe
fallback: if any branch isn't sorted on the key, keep `order(union-all(…))`.

## Window functions ride the ordered stream <a name="window-streams"></a>

`… OVER (PARTITION BY <p> ORDER BY <key> …)` is **already stock SQL++** and already
lowered (`VisitWindowAggregate` → `engine/op_window.go`). No recognition needed — the
win is *sharing the ordered stream*: rate / burst / streak / gap detectors
(`COUNT(*) OVER (… ROWS BETWEEN …)`, `LAG(ts) OVER (…)` for inter-arrival gaps,
running streak lengths) all need input ordered by `<key>` within `<p>`, which is
exactly what a merge-scan produces. When a window op sits over a sorted/near-sorted
source (or a merge-scan), the planner can **skip the window op's own sort** and feed
it the merge's output directly — the merge and the window share one ordered pass.
This is a pure execution optimization, invisible to the SQL, and it composes with the
UNION-ALL→merge rewrite (window over a merged cluster-wide timeline). Worked shape —
burst detection over a merged per-node error stream:

```sql
SELECT node, ts, msg,
       COUNT(*) OVER (PARTITION BY node ORDER BY ts
                      RANGE BETWEEN 10000 PRECEDING AND CURRENT ROW) AS errs_last_10s
  FROM (SELECT node, ts, msg FROM `n0/diag.log` WHERE level="ERROR"
        UNION ALL SELECT node, ts, msg FROM `n1/diag.log` WHERE level="ERROR") t
```

## Table-valued functions in FROM — a verdict <a name="tvf-verdict"></a>

**Verdict: NO — do not fork the grammar for a `FROM merge(...)` TVF.** A ClickHouse-
style `FROM merge('logs.*')` or `FROM asof_join(...)` is attractive and explicit, but
TVF-in-`FROM` needs parser + `algebra` + planner support (`DESIGN-data.md` §2 mode 2
confirms the fork rejects both `FROM read_csv('foo.csv')` and bare `FROM 'foo.csv'` —
"Invalid function", "must have a name or alias"). That is precisely the merge-hostile
fork divergence the whole approach avoids, and the payoff — explicitness — is
achievable without it. Two grammar-free substitutes:

- **Catalog VIEWs (the power path, `DESIGN-data.md` §2 "query-defined virtual
  datasources").** A catalog entry whose definition is the canonical `UNION ALL … ORDER
  BY <key>` (or the argmax subquery) *is* the merge, expanded as an implicit WITH
  binding before planning — pure glue layer, no fork change. `FROM cluster_timeline`
  then reads as one merged keyspace. This is the recommended surface: the merge lives
  in the catalog, authored once, and the recognizer turns its stored SQL into the
  merge op. It also rides the same `VisitUnionAll` that just landed.
- **A backtick-quoted keyspace-name convention** (`DESIGN-data.md`,
  `DESIGN-extensions.md`) — e.g. `` FROM `merge:ns_server.*` `` — parsed as an
  ordinary keyspace name and resolved n1k1-side to a merge-scan over the matched
  files. Cheaper than a VIEW for ad-hoc use, no catalog needed, still zero grammar.

So the merge capability surfaces as **(a) recognized idioms** (argmax → ASOF, UNION
ALL → merge), **(b) catalog VIEWs** carrying those idioms, and **(c) optionally a
keyspace-name convention** — never a TVF. If a compelling need for explicit inline
TVFs ever emerges, it is a *shared* grammar-fork decision with `DESIGN-data.md` mode 2
(one fork buys both `read_csv` and `merge`), not a merge-specific one — and the
verdict there is likewise "defer."

## Where recognition lives <a name="recognition-home"></a>

Three candidate homes; the recommendation is a mix:

- **`glue/conv.go` (`Visit*` lowering)** — for the *local, structural* rewrites that
  are a property of the plan-op shape: `order` directly over `union-all` of sorted
  branches → `merge-sorted`; a `WindowAggregate` over a sorted source skipping its
  sort. These are pattern matches on the `plan.Operator` tree as it is lowered, and
  conv is already where all lowering decisions live. **Recommended home for the
  UNION-ALL→merge and window-stream cases.**
- **A dedicated post-plan rewrite pass (n1k1-side, over the `plan.Operator` tree,
  before conv)** — for the *non-local* ASOF recognition, which spans an outer query
  and a correlated subquery and must inspect the subquery's WHERE/ORDER BY/LIMIT
  together. This is cleaner than smearing the match across conv's per-op visits, is
  independently testable (feed it plans, assert the rewritten shape), and keeps conv
  simple. **Recommended home for the argmax → ASOF rewrite.** It operates on the
  fork's plan output *without modifying the fork* — a read-only tree rewrite,
  exactly like the boxed-expr stringify pass in `DESIGN-prepare.md`.
- **Catalog-expansion time (before planning)** — for VIEW-carried merges: the stored
  SQL is expanded as a WITH binding, then planned and lowered normally, so its merge
  is recognized by the two homes above. No separate recognizer needed.

The unifying principle (shared with `DESIGN-data.md`): **the fork produces plans; all
recognition and rewriting happen n1k1-side, downstream of the fork, so the fork stays
untouched.** Recognition is a read-only analysis of cbq's plan output plus a
lowering choice — never a grammar or planner edit.

---

# §4 NEST — is it interesting here? (a verdict) <a name="nest"></a>

SQL++ `NEST` groups correlated child rows *under* a parent row (the inverse of
UNNEST): `FROM rebalance r NEST errors e ON …` would attach, to each rebalance event,
an array of the error rows that correlate to it. For the support-bundle framing the
tempting shape is *"nest all log lines within ±Δt of each rebalance event under that
event"* — a temporal group-under-parent.

**State today:** every NEST *lowering* is `NA()` in `glue/conv.go` — `VisitNest`,
`VisitNLNest`, `VisitHashNest`, `VisitIndexNest` all bail — so NEST does not run. But
the *runtime* mostly exists: `OpJoinNestedLoop` (`engine/op_join_nl.go`) already
handles nest via an `isNest` flag (kinds `nestNL-*` / `nestKeys-*`), accumulating a
JSON array of matched right rows per left row (`lzNestBytes`) and emitting once per
parent, with `ValArrayEmpty` for the outer-no-match case. The gap is only the glue
lowering, not the engine — so wiring NEST is cheaper than "net-new." That does not
change the verdict; it changes the reason from "expensive" to "still not the right
shape."

**Verdict: not worth it for this use case — recommend UNNEST / window / soft-ASOF
instead.** Reasons:

1. **It's a windowed-ASOF in disguise, which we're already building.** "All log lines
   within ±Δt of each rebalance" is a **range/band join** (many children per parent
   within a tolerance) — a generalization of soft ASOF from "the one nearest" to "all
   within Δt." The merge substrate handles it: co-advance the two ordered streams,
   and for each parent buffer the children whose key ∈ [parent.key−Δt, parent.key+Δt].
   That is a *band merge-join*, not NEST semantics — and it produces flat rows the
   rest of stock SQL++ (GROUP BY, ARRAY_AGG) can nest *if wanted*, without a NEST op.
2. **Nesting is a projection choice, not a join operator.** If a detector genuinely
   wants the children as a nested array, `ARRAY_AGG(…) GROUP BY parent` over the
   band-join output expresses it in stock SQL++ that already runs — no NEST op needed.
3. **NEST is confusing and rarely written.** It's one of SQL++'s least-used, most-
   surprising constructs; asking detector authors (and agents) to write `NEST` — then
   maintaining a from-scratch nested-loop/hash NEST runtime — buys little over the
   `UNNEST` / subquery / window / `ARRAY_AGG` idioms they already know and that
   already execute.
4. **Cost/benefit.** Even though the nest *runtime* largely exists, the temporal
   semantics we actually want ("children within ±Δt of a parent") is a **band join**,
   not NEST's equijoin/ON-KEYS grouping — the existing `nestNL-*` machinery groups by
   the ON predicate, not by a key-distance window, so it would not give the band
   result anyway. The band-merge-join + `ARRAY_AGG` path reuses the merge investment
   (ASOF, windows, timelines); wiring `VisitNest` would buy a construct that is both
   confusing and semantically wrong for the temporal need.

**Recommendation:** leave NEST `NA()`. If the "children under a parent within Δt"
pattern proves common, add it as a **band variant of the merge-join** (all-within-Δt
rather than nearest-within-Δt) feeding `ARRAY_AGG`/`GROUP BY` for the nesting — not as
a NEST operator. Revisit only if a real detector corpus shows NEST-shaped needs the
band-join + `ARRAY_AGG` idiom can't serve.

---

# Coherence with the other docs <a name="coherence"></a>

- **`DESIGN-prepare.md`** — this doc *is* "Temporal as optimizations" (PREPARE++
  phasing step 4) made concrete: the ASOF/merge/window recognitions it sketches, and
  the "no grammar changes" constraint it sets, are realized here. The merge ops are
  detectors' substrate for temporal correlation; the MQO / shared-scan fan-out
  (`#mqo`) and this merge compose — a merged ordered stream can fan out to many
  temporal detectors. The "recognizing the ASOF idiom" open question there is
  answered by [§3](#argmax-rewrite)'s canonical form + conservative recognizer; the
  "log time model" open question is answered by the normalized int64 key from the
  extract layer.
- **`DESIGN-data.md`** — the [sorted-source contract](#contract) (§4 extract provider,
  §5 manifest zone maps / sync points, sortedness classification, `disorder_bound`) is
  the input this doc consumes; the merge ops are its downstream consumer. Two
  cross-doc corrections/links: **`plan.UnionAll` is no longer `NA()`** (its note is
  stale — `VisitUnionAll` landed), which un-blocks the UNION-ALL→merge trigger; and
  the **"predicate must reach the scan"** prerequisite (§5 caveat) is the same one
  gating seek-by-time merge pruning ([§1 cursors](#cursors)). The TVF verdict here
  matches §2 mode 2's "defer the grammar fork."
- **`DESIGN-extensions.md`** — the `*.extract.js` extract functions produce the
  per-source metadata (normalized key, sortedness, `disorder_bound`) the merge reads.
  Their `describe(file) → ExtractSpec` result is **memoized in `.n1k1/` once per file**
  and `extract(file, meta, emit)` is handed the cached spec; the merge planner reads
  that same cached `describe` output at plan time and the cursors read it at run time.
  This doc adds no new extension mechanism — it consumes theirs, and depends on the
  memoization for the "metadata off the hot path" property.
- **`DESIGN.md`** — the merge honors every "Performance approaches" tenet: `[]byte`
  rows, buffer reuse, no boxing, push-based `yield`, `Stage.BatchCh` data-staging,
  `store.Heap` spill, max-heap reuse. It adds no channels/locks on the hot path
  (concatenate and strict-heap are lock-free single-consumer loops).

---

# Phasing <a name="phasing"></a>

Each step is independently useful and evidence-gated (build the cheap high-value
regime first, measure, then earn the next). Status legend: ⬜ not built · ◐ partial ·
✅ done.

1. **⬜ Merge-scan, concatenate regime.** The disjoint-ranges case
   (`max_key(fᵢ) ≤ min_key(fᵢ₊₁)`) — dated log rotations. Zero heap, zero buffering,
   O(N). Needs only the manifest's `min_key`/`max_key` zone maps and a new
   `merge-sorted` op. Delivers "read a month of rotated logs as one ordered stream"
   immediately. *Prerequisite: manifest zone maps (`DESIGN-data.md` §5).*
2. **⬜ Merge-scan, strict min-heap regime.** Overlapping strict sources (concurrent
   per-node logs). K-entry min-heap over `base/heap.go`, lazy cursor opening. The
   general ordered union.
3. **⬜ UNION-ALL→merge recognition** (`conv`): `order(union-all(sorted…))` →
   `merge-sorted`. Rides the already-landed `VisitUnionAll`. Makes the stock timeline
   idiom fast with no new SQL.
4. **⬜ Watermarked-near regime + soft options.** The reorder buffer, `disorder_bound`
   enforcement, late-record policy, and monotonicity validation. The correctness-
   critical step — the one that makes real (jittery) logs safe. Reuses `store.Heap`
   spill.
5. **⬜ Seek-by-time via sync points + predicate pushdown to the merge.** Skip to a
   start key per file. Shares the "predicate reaches the scan" work with
   `DESIGN-data.md` §5 zone-map pruning — the highest-leverage perf step for
   time-bounded detectors.
6. **⬜ ASOF merge-join** (nearest-preceding) + the **argmax-subquery → ASOF rewrite**
   (post-plan pass). Turns the O(n²) correlated argmax into O(N+M). The temporal
   headline.
7. **⬜ Soft ASOF** — tolerance / max-look-back (within-tolerance-or-null, drop,
   nearest). A small delta on step 6; recognized from the optional `>= e.key − Δt`
   predicate.
8. **⬜ Equi merge-join** for the already-sorted equijoin case (lower priority — less
   common in bundles than ASOF).
9. **⬜ Window-stream sharing** — feed a `WindowAggregate` over a sorted source /
   merge-scan directly, skipping its own sort. Composes with steps 2–5.
10. **⬜ Catalog-VIEW carried merges + optional `merge:` keyspace-name convention** —
    the grammar-free explicit surface (steps depend on catalog work in
    `DESIGN-data.md`).

Steps 1–3 already let a human (or the detector corpus) read merged timelines cheaply;
4–5 make it safe and fast over messy time-bounded data; 6–7 add temporal correlation;
8–10 round it out. *(Band merge-join for the NEST-shaped need — [§4](#nest) — is a
later variant of step 6, added only on demonstrated demand.)*

Testing rides the existing discipline (`DESIGN-testing.md`): every merge op gets an
**interpreter/compiler differential** case (the new op kind must compile-match), and
the recognizer gets **golden plan-rewrite tests** (feed a canonical argmax subquery,
assert the ASOF op; feed near-misses — `LIMIT 2`, mismatched ORDER BY dir, extra
correlation — assert *no* rewrite). A **disorder-bound-violation fixture** (a source
that lies about its bound) must exercise each late-record policy — the correctness
tripwire deserves a dedicated test, as it is the subtlest failure mode.

---

# Open questions <a name="open-questions"></a>

- **Measuring `disorder_bound`.** Declared-by-author vs measured-by-sampling — how
  large a sample, and how conservatively to pad the measured bound (a measured max is
  a lower bound on the true max)? A too-tight measured bound is the silent-corruption
  risk; a too-loose one wastes buffer. What default padding factor?
- **Late-record policy default.** Is `widen`+`warn` (self-healing, exploratory) or
  `error` (safe, strict) the right *global* default, given detectors span exploratory
  and correctness-critical? Likely per-detector metadata, but the engine default
  matters for ad-hoc use.
- **Recognizer scope — how much argmax variation to match.** Only the exact scalar-
  subquery canonical form (Phase 1), or also the JOIN-shaped and `GROUP BY … having
  max` variants? Each added shape widens coverage but raises false-positive risk.
  Where's the line?
- **Actor-per-cursor overhead vs a single stepping goroutine.** The design steps K
  cursors as K `Stage` actors (reusing `OpUnionAll`'s fan-in). For large K (many small
  files) the goroutine + `BatchCh` + deep-copy-per-row overhead may dominate the actual
  merge work; a single goroutine holding K lightweight file readers (no channels,
  no per-row copy) could be cheaper. Which wins likely depends on K and row size —
  measure. Lazy cursor opening bounds K to the *overlapping* set regardless.
- **Key materialization on the hot path.** The `describe` spec (format, timezone,
  which field) is cached once per file — but the int64 key still has to be *produced
  per record* at scan time from each row's raw timestamp. Is it materialized into a
  labeled register once (fast int64 compares downstream, some memory) or recomputed at
  each comparison? For a merge over billions of records the per-row key *production*
  (parse the timestamp field, apply the cached spec) may dominate — can it be fused
  into the scan/extract that already touches the bytes, so the key falls out for free?
- **Partition-map eviction in ASOF.** With many equality partitions (e.g. ASOF
  partitioned by node × bucket × index), the `partition → held row` map can grow —
  what eviction policy is correct (a partition can always receive a later left row)?
  Frontier-based eviction assumes partitions don't reappear far apart; is that safe?
- **Band merge-join (the NEST alternative).** If demand appears, is all-within-Δt best
  expressed as a merge-join variant feeding `ARRAY_AGG`, or does it want its own op?
  ([§4](#nest) recommends the former — revisit with real detectors.)
- **Interaction with the compiler / PREPARE++.** The merge ops must compile
  (Futamura path) like every other op — new op kinds carrying live cursors/actors in
  `Temps`. `OpUnionAll`'s actor fan-in already runs under the compiler, so the fan-in
  shape should codegen; does the *ordered-drain* consumer (frontier heap + per-actor
  crediting) codegen as cleanly, or does the merge cap a query at some prepare level?

---

# Sources <a name="sources"></a>

- DuckDB — AsOf Join (syntax, inequality + equality conditions, OUTER):
  https://duckdb.org/docs/guides/sql_features/asof_join
- DuckDB — "AsOf Joins: Fuzzy Temporal Lookups" (the fuzzy/tolerance framing):
  https://duckdb.org/2023/09/15/asof-joins-fuzzy-temporal-lookups
- DuckDB — "Planning AsOf Joins" (the specialized operator / per-partition lookup):
  https://duckdb.org/2025/02/19/asof-plans
- ClickHouse — JOIN clause (ASOF: one closest-match inequality + N equalities;
  supported only by hash and full_sorting_merge algorithms):
  https://clickhouse.com/docs/sql-reference/statements/select/join
- ClickHouse — "Supporting ASOF for full sorting merge" (sort-merge ASOF design):
  https://github.com/ClickHouse/ClickHouse/issues/54493
- ClickHouse — `merge()` table function (regex-matched multi-table union, the
  ad-hoc merge-in-FROM this doc declines to fork the grammar for):
  https://clickhouse.com/docs/sql-reference/table-functions/merge
- pandas — `merge_asof` (backward/forward/nearest direction, `tolerance` = the soft
  bound; both frames sorted on the key):
  https://pandas.pydata.org/docs/reference/api/pandas.merge_asof.html
- kdb+/q — `aj`, `aj0`, `ajf` as-of join (the original time-series ASOF):
  https://code.kx.com/q/ref/aj/
- Apache Flink — Builtin watermark generators / bounded out-of-orderness (the
  watermark + `maxOutOfOrderness` model behind the near regime):
  https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/event-time/built_in/
- Apache Flink — allowed lateness / late events (drop vs side-output vs re-fire —
  the late-record policy design space):
  https://nightlies.apache.org/flink/flink-docs-stable/docs/learn-flink/streaming_analytics/
- Classic external sort-merge join & K-way merge (Graefe, "Query Evaluation
  Techniques for Large Databases", ACM Computing Surveys 1993) — the O(N log K)
  merge and sort-merge join foundations.
</content>
</invoke>
