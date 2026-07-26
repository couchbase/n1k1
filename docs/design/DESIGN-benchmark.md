# n1k1 benchmarks — design

_Last reviewed: 2026-07-25._

## Status

- **Phase 1 (intrinsic pure-Go microbenchmarks) — DONE.** `make bench` over
  `test/benchmark/`: flat allocs/op at 1M rows, ~4000-key GROUP BY spill onset with graceful
  degradation.
- **Phase 2 (interpreted vs compiled) — DONE.** `make bench-compiler`: fusion cuts ~40% of
  allocs; wall-time win is shape-dependent.
- **Phase 3 (n1k1 vs a real cbq executor) — REALIZED** via `test/benchmark/versus` (not the
  blocked from-source cbq-engine route): races both engines over the *same* local files, cbq
  driven through the `n1k1-query` fork's `local-benchmark` branch.
- **Per-query fixed-cost levers — DONE:** a Session-scoped `rt.SpillState` recycles the
  GROUP/ORDER store buffer across a connection (no per-query alloc, no leak — `RHStore.Reset`
  zeroes slots, guard `TestSessionSpillReuseNoLeak`) + a lazy spill temp dir (no `mkdir`/`rmdir`
  unless a query spills). Together → single-file query mix ~2.3× baseline throughput. See
  `DESIGN-concurrency.md`.

**Remaining:** the Phase-3 *product-numbers* variant (from-source cbq-engine over `dir:` — needs
a buildable server tree; recipe below); folding the newer levers (streaming merge-scan,
fixed-width columnar, window incremental-fold — measured ad hoc today) under `make bench`;
attacking boxed/JSON alloc churn in the parse-bound scan/filter/project path; and columnar
pushdown into *shredded* VARIANT sub-columns (the unshipped columnar-VARIANT win).

## Claims → benchmarks

| Claim | Metric | Benchmark |
|---|---|---|
| garbage avoidance — `[]byte`/`[][]byte` recycling, no boxing, no `map[string]interface{}` | allocs/op flat vs N | `BenchmarkScan*`, `BenchmarkFilterProject*` |
| per-row engine cost — push-based, register (positional `Vals`) vs map lookup | ns/row | scan→filter→project pipeline |
| static-param expr — evaluate const once (`ExprEq` 13 allocs/op vs `ExprStr` 4042 / 1000 docs) | ns/op, allocs/op | `test/BenchmarkInterpExpr*` |
| canonical JSON into reused buffers | allocs/op | `base.BenchmarkCanonicalJSON` |
| GROUP BY / DISTINCT + spill (rhmap + `rhmap/store` to temp files, >RAM without OOM) | rows/s, temp bytes | `BenchmarkGroupBy*`, `TestSpillPoint` |
| hash-join + spill; max-heap ORDER BY; INTERSECT/EXCEPT reuse hash-join | rows/s, allocs | `BenchmarkJoinHash*`, `BenchmarkOrderLimit*`, `BenchmarkSetOps*` |
| compilation / fusion — generated Go fuses operators, lifts vars | interp vs compiled ns/op | Phase 2 |

An in-process race against couchbase/query's executor is **not possible** here (the pure-Go
decouple dropped `query/execution`, which pulls cgo/cbft) — hence Phase 3 goes through the
`versus` harness or the blocked from-source recipe.

## Harness (`test/benchmark/`, `//go:build n1ql`)

Standard Go `testing.B` (composes with `benchstat`). Metrics: `ns/op`→ns/row, throughput via
`b.ReportMetric(rows/s)`, `allocs/op`+`B/op`; macro runs sample `runtime.MemStats` +
temp-file bytes. Scale sweep `nDocs` = 1/1K/100K/1M — *how each metric grows with N* (flat
allocs/op = recycling works). Each bench isolates **execution**: parse+plan once outside `b.N`,
only `engine.ExecOp` inside with a **no-op yield** (`yieldVals` counts, `yieldErr` fails).

Data: a synthetic generator (`gen.go`, one "contact" doc shape + tunable-cardinality key `g`;
`reps` amplifies row count with no I/O) and realistic shapes via `glue` over the vendored
corpus. Key files: `bench_scan_test.go`, `bench_expr_arith_test.go`, `bench_spill_test.go`,
`bench_compiler_test.go` (Phase-2 generator `TestGenerateBenchmarks`),
`bench_concurrency{,_prepared}_test.go` (see `DESIGN-concurrency.md`). Targets: `make bench`,
`bench-spill`, `bench-compiler` (`-benchtime=30s`), `bench-concurrency`, `benchmark-expr-eq`.

## Phase 1 findings

Indicative apple-silicon (trends matter, not exact ns; full detail in `README.md`):

- **Garbage avoidance holds** — allocs/op constant as rows scale 1000×: scan **6**,
  scan+filter **18**, +project **37** at 1K *and* 1M rows (fixed = pipeline setup; per-row ~0).
- **Throughput:** raw scan ~500M rows/s, +filter ~12M, +project ~5M rows/s.
- **Spill point ≈ 4000–5000 distinct keys** (`StartSize=5303`, `Grow` fires a touch earlier on
  load factor / MaxDistance), then grows to an mmap'd `*_slots_*` file (~80 B/key: ~4MB at 64K,
  ~20MB at 256K).
- **Graceful degradation holds** — GROUP BY ~4.5M rows/s at 1000 distinct (in-mem) vs ~4.2M at
  64000 (spilled): ~6% slower paging to disk, not a cliff.

## Phase 2 — compiled-query benchmarking

`make bench-compiler` generates paired `BenchmarkInterp_X` / `BenchmarkCompiled_X` (operators
fused inline as compiler-generated Go) into `test/tmp`, reusing the Phase-1 generator/scales.
Findings (30M rows/op, `-benchtime=30s`):

| query | interpreted | compiled | |
|---|---|---|---|
| ScanFilterProject | ~5.60s, 35 allocs/op | ~5.77s, 21 allocs/op | ~3% slower, **40% fewer allocs** |
| GroupBy | ~4.28s, 174 allocs/op | ~3.74s, ~150 allocs/op | **~13% faster**, ~15% fewer allocs |

**Fusion + lifted-var reuse cut allocations on both** (allocs/op held ~flat 100K→30M rows).
**Wall-time is shape-dependent:** GROUP BY (function-call-heavy per-row aggregation) gets a clear
~13% from fusion; the scan→filter→project pipeline is **parse-bound** (jsonparser field
extraction dominates), so eliminating per-op call overhead is marginal (even slightly slower).
Signal: for scan/filter/project, optimize *parsing*; fusion pays most for call-heavy ops.

## Phase 3 realized — n1k1 vs cbq over local files

`test/benchmark/versus` times the **same** queries over the **same** data through cbq's real
executor. Both engines read the classic cbq file-datastore layout
`<root>/<namespace>/<keyspace>/<key>.json`, both use cbq's parser+planner (identical plan); what
differs is the **execution engine** — n1k1's `[]byte` byte-engine vs cbq's boxed
`value.AnnotatedValue` executor. The cbq side runs via the fork's `local-benchmark` branch
(`cmd/localbench` over `test/filestore`): build `CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*'
go build -o /tmp/localbench ./cmd/localbench`, then `CBQ_LOCALBENCH=/tmp/localbench`. Run:
`[COMPILED=1] [CBQ_LOCALBENCH=...] python3 test/benchmark/versus/bench.py`.

⚠ **Both columns are the FULL `parse→plan→execute`**, warm (median of REPS, first few dropped).
The n1k1 side reports `Result.RunElapsed` (whole `Session.Run`), **not** just `ExecOp` — an
ExecOp-only measurement once showed a bogus ~40× "win" (see gotchas). Two scenarios separate I/O
from execution: **files** (one doc per file — realistic but I/O-bound: both pay `os.Open` per
doc) and **bulk** (a few docs holding a large `items[]`, driven by UNNEST — I/O trivial,
per-row execution dominates).

| scenario | time (n1k1 vs cbq) | memory |
|---|---|---|
| files (I/O-bound) | ~1.0–1.1× (tie) | **2–6× less** |
| bulk (compute-bound) | **~6–9× faster** | **~6–26× less** |

Representative bulk (20K-elem arrays): `unnest+group` 59ms/17.8MB vs cbq 531ms/469MB (~9×/~26×).
The bulk gap is the thesis in one number: cbq boxes every unnested element into a `value.Value`;
n1k1 evaluates on raw `[]byte`. The files tie is an artifact of the one-doc-per-file layout (see
the layout lesson), not the engines. `EXPLAIN` shows **zero `⟨boxed⟩` markers** on every
`versus` query — the wins are genuine native-vs-boxed.

### Compiled-codegen column (`COMPILED=1`) — the Futamura payoff, isolated

Adds n1k1's `-prepare=full` **standalone-compiled EXECUTE**: each query is `PREPARE`d once
(emitting cbq-free Go, `go build`ing a child binary — needs the toolchain + `N1K1_SRC`), then
`EXECUTE`d warm. The lane is a **thin child**: the parent scans + JSON-pipes each input record
to the child, the child runs only the compiled compute and pipes rows back. The table splits it:
**`comp`** = whole round-trip; **`core`** = the child's own compute wall (`N1K1_CORE_NS`, IPC
excluded); **`core:i`** = core / interp.

**Two opposing truths:** end-to-end, `comp` is ~1.2–3.0× *slower* than the interpreter (thin-child
IPC costs more than the compute it accelerates), **but the specialization itself pays** — `core`
runs ~1.3–1.6× faster than interp on compute-bound bulk rows (`core:i` ≈ 0.64–0.77×). The
Futamura projection is a real win, buried under IPC in *this* thin-child deployment (targets the
standalone/MQO scenario, not single-`EXECUTE`-over-a-pipe; see `DESIGN-prepare.md`). No compiled
MB column (compute runs in a child, invisible to the parent heap counter). `n/a` = didn't compile
standalone (any `JOIN ... ON KEYS` — the thin child can't do a per-row datastore fetch). Two
codegen bugs had to be fixed to make aggregates + two-field arithmetic even compile
(`glue.TestExecuteCompiledAggAndArith`).

### Container-format scenarios: packed `.jsonl` and parquet+VARIANT

Two more `versus` scenarios put the *same* docs in a single container file, removing the
per-file-`open` cost:

- **packed `.jsonl`** (`orders_jsonl`, `NDOCS_JSONL` knob). cbq gets a real column via the fork's
  `jsonl:` in-memory datastore (`datastore/jsonl`, a `datastore/mock` adaptation; `SITE=jsonl:`;
  `ON KEYS` joins stay n1k1-only since `cust` has no `.jsonl`). **Result (200K docs): n1k1 ~5–12×
  faster and ~50–3500× less memory** (group+agg 108ms/0.18MB vs 1143ms/627MB) — the clean
  byte-vs-boxed gap the I/O-bound `files` scenario masks.
- **parquet+VARIANT** (`VARIANT=1`, `orders_variant`). arrow-go v18 has a native Parquet VARIANT
  type; `records/parquet.go` reads it (Phase-0 projects VARIANT→JSON at the scan boundary by
  default; `-variant-fidelity` opts into the Phase-1 `V`-carrier for typed-scalar fidelity our
  plain-JSON orders lack). cbq n/a (iceberg-go v0.4.0 has no VARIANT). **Result (n1k1, 200K):
  whole-doc VARIANT is ~1.5–2.4× SLOWER and far more memory-hungry than the same docs as `.jsonl`**
  (count+filter 167ms/120MB vs 67ms/0.21MB). An *unshredded* VARIANT is one column read+decoded
  whole per row, with none of the columnar sub-field projection that would justify the format.
  **VARIANT's payoff needs shredding (typed sub-columns) or plain typed columns for
  column-selective queries.** (Generators `gen_variant.go`, `gen.py`.)

  **Where the memory goes (pprof) + Phase-1/shredded A/B.** The alloc is ~93% *arrow-go*, not
  n1k1: ~60% copies the VARIANT byte-array column into `BinaryBuilder` buffers (no zero-copy
  chunk reference via the `pqarrow` table API), ~18% `variant.Metadata.loadDictionary` re-parses
  each row's embedded dict per `VariantArray.Value(i)`, ~16% per-chunk read streams; n1k1's own
  `variant.AppendJSON` + record buffer is ~3–4%. So "zero-copy/no-boxing" holds for n1k1's engine
  lane — the cost is the arrow-go read boundary, which jsonl sidesteps. A/B (50K docs, in-process,
  `records.VariantFidelity` toggle): unshredded Phase-0 37ms/53MB; **Phase-1 fidelity 59ms/110MB**
  (~2× — assembles a whole-row `V`-carrier per row; buys fidelity, not speed); **shredded Phase-0
  103ms/154MB** (~2.9× — the OPPOSITE of the hoped win: no projection/predicate *pushdown into
  shredded sub-columns* yet, so arrow reads all sub-columns + residual and *coalesces* them back
  into a full variant per row). So the columnar-VARIANT win is **blocked on shredded-column
  pushdown** (DESIGN-variant §6, unshipped); until then unshredded Phase-0 is cheapest, still far
  above jsonl.

  ⚠ **This is a layout limit, not an arrow-API misuse.** n1k1 *does* have a zero-copy columnar
  lane — `records/parquet.go` `NextColumns()` borrows each column's raw little-endian buffer, and
  `glue/columnar.go` adds a metadata-only path (COUNT/MIN/MAX from footer stats, zero data reads)
  + a vectorized `agg-columnar` op. It just can't apply to a *whole-doc VARIANT*: the queried
  fields live *inside* the VARIANT binary, so they aren't parquet columns to borrow. Proven with a
  TYPED-column parquet vs the SAME docs as one VARIANT column (50K): `SUM(amount)` TYPED
  1.2ms/2.44MB vs VARIANT 29ms/37MB (~24×/~15×). So the lever is **layout** (typed columns, or
  shredded VARIANT once pushdown lands), not a different arrow API. (Even typed-column parquet
  allocs ~2.4MB for column materialization vs jsonl's ~0-alloc reader: jsonl wins memory,
  typed-parquet wins *time* on vectorizable numeric aggregates.)

## I/O-bound scan & the file-layout lesson (2026-07)

The `versus` **files** scenario prompted "is n1k1 just waiting on I/O, would concurrency help?"
The answer reframed the problem. *(All exploration below is env-gated throwaway in an experimental
worktree, not landed — `N1K1_PSCAN`, `N1K1_STAGE`, `Stage.NoCopy`; the findings are the
deliverable.)*

**It IS I/O-blocked** — a filtered scan over 20 000 one-doc `.json` files is ~78% off-CPU (66.8s
wall vs 14.7s on-CPU): serial per-file `open/stat/read/close` syscalls, one goroutine.

⚠ **Parallel scan helps ~4× but is a band-aid, and `auto`=NumCPU is a trap.** Fanning the file
list across N `base.Stage` supplier goroutines gives ~4× — **but only at ~128 actors** (they're
I/O-*blocked*, so you need ~10× oversubscription vs cores). Measured (count+filter, 20 000 files):
serial 3.27s, `auto`(=12) **~3.2s = NO gain**, 48→0.95s (3.3×), 128→0.73s (4.3×), 128+NOCOPY
0.85s (no gain). At only NumCPU actors the speedup is ~0, so a naïve "use NumCPU" default makes
parallel scan *look dead*. Profiling at 128 actors: 97.6% in `syscall.syscall` — the ceiling is
the OS/FS capping concurrent read syscalls to ~3.7 effective cores (containerized/overlay FS,
~160µs kernel/file); the consumer + dir walk are ~37ms each, **negligible**. So batch size and
in-flight depth are noise, zero-copy handoff is unneeded, and parallel *compute* would be pointless.

**The punchline — packing beats parallelizing opens by two orders of magnitude:**

| layout (20 000 docs, filter+group) | time |
|---|---|
| 20 000 `.json` files, serial | 3194 ms |
| 20 000 `.json` files, parallel (128 actors) | 703 ms (4.5×) |
| **1 `.jsonl` file, serial** | **13 ms** |

The actual compute is ~13 ms — free. The whole 3.2s was per-file syscall overhead × 20 000.
Packing into one `.jsonl` is **~245× faster than serial, ~54× faster than the parallel hack**. So
the one-doc-per-file layout is pathologically syscall-heavy; the real fix is a **container format**
(`.jsonl`/parquet), not parallel scanning. Parallel scan is a consolation prize only when you're
*stuck* with a directory of many files (Couchbase exports, log/cbcollect dirs).

**Read-ahead *decoupling* (vs parallelism) was a dead end** — a single-supplier `Stage` overlapping
I/O with compute gave no win (one supplier can't parallelize serial opens; consumer too small to
overlap) and was **3× WORSE** on a re-executed nested-loop inner (a fresh Stage goroutine+channel
per outer row — ~80 000 spawns on `bulk unnest+join`).

**Caveats:** warm page cache; containerized FS (~3.7× concurrency cap). On bare metal cheaper opens
shrink the packing win toward ~10–15×; cold-cache disk may let parallelism hide more. The
*direction* is universal: fewer syscalls always wins; parallelism only hides them.

## Attacking the arrow record-batch read floor (2026-07)

After the zero-alloc VARIANT navigator (DESIGN-variant.md) pushed the query hot path to zero
allocs, a lone-VARIANT parquet scan (`glue.BenchmarkVariantLoneScan`, 50K docs, `COUNT(*)` +
nested-field filter) still sat at ~36 MB/op — a mem-profile showed ~93% is **arrow-go's own
record-batch read machinery**, not n1k1. Two safe, independent levers took it down ~⅓ with no time
cost (fidelity-borrow 36.2MB/18.9ms → **24.7MB/17.3ms**; Phase-0 34.8MB → **23.4MB**):

- **Pool the pqarrow *output* arrays** (`records/parquet_alloc.go`, `poolAllocator`): a
  size-classed `sync.Pool` `memory.Allocator` recycles the buffers arrow frees on
  `batch.Release()` into the next `Read()` (`GoAllocator` just `make()`s + GCs). Saves ~5MB/op;
  GC-cooperative → retention bounded (~one batch's live buffers).
- **`BufferedStreamEnabled` on the local reader**: `ReaderProperties.GetStream` otherwise
  `make([]byte, nbytes)`-slurps the entire column chunk (the single largest scan alloc); buffered
  streaming reads page-by-page through an `io.SectionReader`. Saves ~6MB/op, no slowdown (file in
  page cache). The remote object-store reader already did this.

⚠ **Two gotchas the pooling exposed (both load-bearing):**
- **arrow relies on `Allocate` returning zeroed memory** — validity bitmaps only write set bits,
  assuming the rest are 0. Recycled buffers carry stale bytes → **`clear()` on reuse** (a memclr,
  still far cheaper than `make`+GC). Symptom without it: `order:null`, garbage floats, zero-byte
  string columns.
- **Only pool the *output* allocator, NEVER the parquet *decode* allocator.** pqarrow's
  string/binary output arrays alias the decode buffers zero-copy, and arrow frees+reuses decode
  scratch *within* one batch build → recycling it hands back a buffer a live output column still
  points at. Symptom: corrupted/zeroed string columns *mid-scan*. The decode side stays on
  `GoAllocator` — the genuine remaining floor.

The residual ~24MB is arrow's unavoidable decode scratch + n1k1's V-carrier framing copy
(`base.AppendVariantEnvelope`, inherent to a single-`[]byte` `base.Val`); going below needs a
lower-level zero-copy page reader (deferred). **Confirmed in the `versus` VARIANT scenario** (the
levers are below the VARIANT layer, so apply to *every* parquet read): whole-doc `orders_variant`
count+filter at 200K dropped **~181 → 120 MB**. Still deferred: a zero-copy parquet *page* reader
+ shredded-VARIANT pushdown — the unshipped columnar-VARIANT win.

## Measurement gotchas (hard-won)

Mistakes that produced confidently-wrong numbers — encode into any new harness:

- ⚠ **Time the FULL request, not just `ExecOp`.** The CLI footer originally timed only `ExecOp`
  (`Result.Elapsed`); against cbq's full request that showed a bogus ~40× "win". Fixed via
  `Result.RunElapsed` (parse+plan+convert+execute). For a large inline literal, parse dominates —
  always measure end-to-end.
- ⚠ **Compiled memory isn't visible to the parent** — a standalone-compiled EXECUTE runs in a
  *child process*, so the parent's heap counter can't see it; the compiled column is time-only.
- ⚠ **Validate row COUNTS when hacking the scan, not just speedup.** The parallel-scan prototype
  had two silent bugs: (1) an over-conservative dispatch guard
  (`scanProjectColumns==nil && !hasFilter`) kept every filtered/projected query on the serial path
  → a false "payload queries don't parallelize" result; (2) actors didn't send the done-signal
  (`yieldErr(nil)`), so `base.Stage` never flushed each actor's final partial batch → **~18% of
  rows silently dropped** (20 000→16 384 = 256×64). Both invisible unless you check counts.
- ⚠ **Float SUM is non-deterministic under reordering** — parallel actors sum in a different order
  → `SUM` differs in the last 1–2 ULPs (float non-associativity, not a bug, but trips an exact-diff
  oracle).
- ⚠ **The cbq `jsonl:` packed column is unreliable at large `NDOCS_JSONL`.** At 200K docs the mock
  reported 0.27–0.97 ms/query (~1000× faster than its own README, and faster than the 20K run —
  impossible); it almost certainly fails to load the big container and counts ~0 docs. Trust the
  cbq packed column only at modest sizes (sanity-check row counts); n1k1's packed numbers are solid.
- ⚠ **`bench.py` regenerates (and can shrink) the shared data dir.** `NDOCS_JSONL` defaults to
  `NDOCS`, so a run omitting it rewrites `orders_jsonl`/`orders_variant` at the smaller size,
  clobbering a prior big dataset. Comparing two runs: pin `NDOCS_JSONL` in both or use separate
  `DATA` dirs; `packed`/`VARIANT` tables are only comparable at the same `NDOCS_JSONL`.
- ⚠ **`N1K1_PSCAN=auto` (=NumCPU) shows no speedup — don't conclude "parallel scan is dead."** The
  scan is syscall-*latency*-bound, needing ~10× more blocked goroutines than cores (above). Sweep
  the actor count explicitly (48/128) before judging an I/O-parallelism experiment.

## Existing benchmarks (build on, don't duplicate)

`test/n1k1_interp_test.go` (`BenchmarkInterpExprEq/Str_*`, `BenchmarkInterpGroupBy_*`); `base/`
(`BenchmarkCanonicalJSON`, `BenchmarkValCompare`, `BenchmarkParse`, `BenchmarkEncodeAsString`);
`test/BenchmarkBoxing`. `make benchmark-expr-eq` wires `-tags n1ql` + `CGO_ENABLED=0` + benchmem.

## Phase 3 feasibility — product-numbers variant (2026-06)

_Superseded for the executor-vs-executor comparison by the `versus` harness above. This remains
the recipe for the heavier **product-numbers** run — a full from-source cbq-engine over a `dir:`
datastore — still blocked in a stock env._

**Blocked** (Server.app installed, no source tree): all three mechanisms fail. (a) an in-process
race is impossible (`query/execution` imports `n1fty/verify → cbft` cgo, pruned by the decouple);
(b) the prebuilt `cbq-engine` calls `waitForInitialSettings()` unconditionally → blocks forever on
metakv without a cluster; (c) a patched from-source build would work (one-line guard below) but
needs the whole Server module graph (`n1fty`, `cbauth`, `indexing`, `cbgt`, `cbft` via
`replace => ../<sibling>`), a `repo sync` exercise out of scope. Phase 1/2 stand alone.

**Recipe (on a buildable couchbase/query):** the closest analog is the standalone `dir:` datastore.
(1) Patch `server/cbq-engine/main.go` to guard the settings wait on a `stub:` configstore prefix;
(2) `CGO_ENABLED=1 go build -o cbq-engine ./server/cbq-engine`;
(3) `./cbq-engine -datastore "dir:$PWD/test/suite/json" -configstore stub:`; (4) warm up, POST to
`http://localhost:8093/query/service`, time N runs. HTTP/server overhead dominates micro-comparisons
— prefer large per-query row counts. (Heavier real-product-numbers alternative: full Couchbase
Server via `cluster-init`/`bucket-create`/`cbimport`/`CREATE PRIMARY INDEX` — but that is KV+GSI+
network, not a file scan, and modifies the local install.)
