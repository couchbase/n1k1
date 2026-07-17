# n1k1 benchmarks — design

## Status & remaining TODOs

_Last reviewed: 2026-07-16._

**Done:** Phase 1 (intrinsic pure-Go microbenchmarks) and Phase 2 (interpreted vs
compiled) are built and run under `make bench` / `bench-spill` / `bench-compiler`
over `test/benchmark/`, with recorded findings (flat allocs/op at 1M rows,
~4000-key GROUP BY spill onset with graceful degradation, fusion cutting ~40% of
allocs). **Phase 3 is now REALIZED** — not the from-source cbq-engine route (still
blocked, below), but a leaner one: `test/benchmark/versus` races n1k1 vs a real cbq
executor over the *same* local `*.json` dir, via the `n1k1-query` fork's
`local-benchmark` branch (`cmd/localbench` over `test/filestore`). See
[Phase 3 realized](#phase-3-realized--n1k1-vs-cbq-over-local-files).

**Remaining (headline TODOs):**
- [ ] Phase 3 *product-numbers* variant (from-source cbq-engine over `dir:`) — still
  needs a buildable server source tree; the one-line patch and recipe are recorded
  below. The `versus` harness covers the executor-vs-executor comparison already.
- [ ] Fold the newer perf levers into this harness — streaming merge-scan,
  fixed-width columnar, window incremental-fold — currently measured ad hoc
  (`glue/window_bench_test.go`, `test/col_test.go`), not under `make bench`.
- [ ] Attack boxed-value / JSON alloc churn: the scan/filter/project
  path is parse-bound (Phase 2), so a native-lane ASOF/subquery projection is the
  next perf lever.
- [ ] Consider a **packed-layout column** in `versus` (same data as one `.jsonl`):
  the I/O-bound file scenario is dominated by per-file-open syscall overhead, and
  packing beats it ~245× — the layout effect dwarfs the engine gap. See
  [I/O-bound scan & the file-layout lesson](#io-bound-scan--the-file-layout-lesson-2026-07).

## Overview

DESIGN.md claims many performance techniques (garbage avoidance, push-based
fusion, static-param expr optimization, rhmap spilling, max-heap ORDER BY,
canonical-JSON reuse). This doc records how they are measured, across
latency / throughput / memory. Phase 1 validates them intrinsically (in-process,
pure-Go); Phase 2 races interpreted vs compiled n1k1; Phase 3 (blocked in a
stock env) would compare against couchbase/query as a baseline.

## Contents

1. [Claims → benchmarks](#claims--benchmarks)
2. [Strategy & phasing](#strategy--phasing)
3. [Metrics & dimensions](#metrics--dimensions)
4. [Harness, data & layout](#harness-data--layout)
5. [Phase 1 findings (current)](#phase-1-findings-current)
6. [Phase 2 — compiled-query benchmarking + findings](#phase-2--compiled-query-benchmarking--findings)
7. [Phase 3 realized — n1k1 vs cbq over local files](#phase-3-realized--n1k1-vs-cbq-over-local-files)
8. [I/O-bound scan & the file-layout lesson](#io-bound-scan--the-file-layout-lesson-2026-07)
9. [Measurement gotchas (hard-won)](#measurement-gotchas-hard-won)
10. [What already exists](#what-already-exists)
11. [Open decisions](#open-decisions)
12. [Phase 3 feasibility — product-numbers variant (2026-06)](#phase-3-feasibility--product-numbers-variant-2026-06)

## Claims → benchmarks

| Claim | Metric | Benchmark |
|---|---|---|
| garbage avoidance — `[]byte`/`[][]byte` recycling, no boxing, no `map[string]interface{}` | allocs/op flat vs N | `BenchmarkScan*`, `BenchmarkFilterProject*` across scales |
| per-row engine cost — push-based, register (positional `Vals`) vs map lookup, `YieldErr` | ns/row | scan → filter → project pipeline |
| static-param expr — evaluate const once, typed codepath (`ExprEq` 13 allocs/op vs `ExprStr` 4042 / 1000 docs) | ns/op, allocs/op | `ExprEq` vs `ExprStr` (`test/BenchmarkInterpExpr*`) |
| canonical JSON — `ValComparer.CanonicalJSON` into reused buffers | allocs/op | `base.BenchmarkCanonicalJSON` |
| jsonparser vs Unmarshal | ns/op, allocs | `base.BenchmarkParse` + field-access micro |
| GROUP BY / DISTINCT + spill — rhmap + `rhmap/store` to temp files, >RAM without OOM | rows/s, MemStats, temp bytes | `BenchmarkGroupBy*`, `TestSpillPoint` at rising cardinality |
| hash-join + spill | rows/s, mem | `BenchmarkJoinHash*` |
| max-heap ORDER BY (spills, no final sort) | ns/op, allocs | `BenchmarkOrderLimit*` |
| INTERSECT/EXCEPT reuse hash-join | rows/s | `BenchmarkSetOps*` |
| compilation / fusion — generated Go fuses operators, lifts vars | interp vs compiled ns/op | Phase 2 |

## Strategy & phasing

An in-process race against couchbase/query's executor is **not possible** here
(the pure-Go decouple dropped `query/execution`, which pulls cgo/cbft). Hence:

- **Phase 1 (done) — intrinsic validation, in-process, pure-Go.** Measures each
  claim on its own terms (allocs/op flat, ns/row, throughput, spill). No
  external yardstick, no feasibility risk; the bulk of the value.
- **Phase 2 (done) — n1k1 interpreted vs compiled.** Validates
  fusion/lifting; in-process, builds on Phase 1 (see
  [Phase 2](#phase-2--compiled-query-benchmarking--findings)).
- **Phase 3 (BLOCKED in a stock env) — vs couchbase/query, absolute baseline.**
  Needs a full Couchbase Server build/runtime; three mechanisms all blocked (see
  [Phase 3 feasibility](#phase-3-feasibility--findings-2026-06)).

## Metrics & dimensions

Standard Go `testing.B`, so results compose with `benchstat`.

- **Latency** — `ns/op` and derived **ns/row**.
- **Throughput** — `b.ReportMetric(rows/sec, "rows/s")`, rows = `nDocs * b.N`.
- **Memory** — `b.ReportAllocs()` → `allocs/op` + `B/op`. For macro runs, sample
  `runtime.MemStats` (HeapAlloc peak, NumGC, PauseTotalNs) and temp-file bytes
  when spilling. For per-object alloc attribution, `-memprofilerate=1` +
  `pprof -alloc_objects`.
- **Scale** — sweep `nDocs` = 1, 1K, 100K, 1M; plot *how each metric grows with
  N* (allocs/op flat = recycling works; throughput holds until spill, then
  degrades gracefully).

Each benchmark isolates **execution**: parse+plan once outside the `b.N` loop;
only `engine.ExecOp` runs inside, with a **no-op yield**.

## Harness, data & layout

### Data sources

- **Synthetic generator** (`gen.go`) — one corpus-like "contact" doc shape, with a
  tunable-cardinality grouping key `g`. Cheapest scan path replicates the source
  string via `reps` (no I/O), amplifying row count without growing the source or
  the distinct-key count.
- **Realistic shapes** — `glue` over the vendored corpus (`test/suite/json`) via
  `FileStore` + `DatastoreOp`, exercising the real datastore path. Lower N.

### Harness conventions

- **Plan once, execute many** — build the `base.Op` tree once, reuse across
  `b.N`, resetting `Vars` / recycling spill state per iteration.
- **No-op yield** — `yieldVals` increments a counter; `yieldErr` fails the bench.

### Layout (`test/benchmark/`, `//go:build n1ql`)

```
gen.go                    synthetic doc generator (shape + cardinality knobs)
harness.go                plan-once/execute-many, no-op yield, rows/s metric
bench_scan_test.go        scan, filter, project (per-row cost, garbage)
bench_expr_arith_test.go  static-param vs interp expr
bench_spill_test.go       GROUP BY spill onset (TestSpillPoint)
bench_self_test.go        self-timed engine micro-runs
bench_compiler_test.go    Phase 2 interp-vs-compiled generator
compare_test.go           value-compare micro
boxing_test.go            interface-boxing / alloc micro
README.md                 claim→bench map, how to run, findings, benchstat tips
```

Make targets: `make bench` (all, `-benchmem`), `make bench-spill` (spill onset),
`make bench-compiler` (Phase 2, `-benchtime=30s`), `make benchmark-expr-eq`
(static-param expr eq). Canonical-JSON / parse micro-benchmarks stay in `base/`.

## Phase 1 findings (current)

Indicative apple-silicon numbers (`-benchtime=30x`; trends matter, not exact ns).
Full detail in `test/benchmark/README.md`.

- **Garbage avoidance — holds.** allocs/op is constant as row count scales 1000×:
  scan **6**, scan+filter **18**, scan+filter+project **37** allocs/op at 1K *and*
  1M rows. The fixed count is pipeline setup; per-row allocation is ~zero.
- **Throughput:** raw scan ~500M rows/s, +filter ~12M, +project ~5M rows/s.
- **Spill point ≈ 4000–5000 distinct keys.** GROUP BY's rhmap/store keeps
  metadata slots in memory until ~4000 distinct keys (`StartSize=5303`, `Grow`
  fires a touch earlier on load factor / MaxDistance), then grows to an mmap'd
  `*_slots_*` file. Above it, temp bytes grow ~linearly (~80 B/key: ~4MB at 64K
  keys, ~20MB at 256K).
- **Graceful degradation — holds.** GROUP BY throughput barely moves across the
  spill boundary: ~4.5M rows/s at 1000 distinct (in-memory) vs ~4.2M at 64000
  (spilled) — ~6% slower while paging to disk, not a cliff.

## Phase 2 — compiled-query benchmarking + findings

`make bench-compiler` generates, for a fixed query set, paired `BenchmarkInterp_X`
(engine.ExecOp over a baked Op tree) and `BenchmarkCompiled_X` (operators fused
inline as compiler-generated Go) into `test/tmp`, run side by side. Reuses the
Phase 1 generator/scales/metrics — only the "run" step differs. (Generator:
`bench_compiler_test.go`'s `TestGenerateBenchmarks`; codegen helpers shared with
the `test/emit` differential generators.)

Findings, stable across repeats (30M rows/op, `-benchtime=30s`):

| query (30M rows/op) | interpreted | compiled | |
|---|---|---|---|
| ScanFilterProject | ~5.60s, 35 allocs/op | ~5.77s, 21 allocs/op | ~3% slower, **40% fewer allocs** |
| GroupBy | ~4.28s, 174 allocs/op | ~3.74s, ~150 allocs/op | **~13% faster**, ~15% fewer allocs |

- **Fusion + lifted-var reuse cut allocations** (fewer closures / reused buffers)
  on both — the DESIGN.md claim holds on the allocation axis. (allocs/op held
  ~flat from 100K → 30M rows: 35 vs 34 for the pipeline — garbage avoidance
  again, at 300× scale.)
- **Wall-time is shape-dependent.** GROUP BY (function-call-heavy aggregation per
  row) gets a clear ~13% from fusion. The scan→filter→project pipeline is
  parse-bound (jsonparser field extraction dominates), so eliminating per-op call
  overhead is marginal there (even slightly slower). Signal: for
  scan/filter/project, optimize parsing; fusion pays most for call-heavy ops.

## Phase 3 realized — n1k1 vs cbq over local files

The Phase 3 goal — time the **same** queries over the **same** data through
couchbase/query's real executor — is achieved by `test/benchmark/versus`, sidestepping
the blocked from-source cbq-engine route ([below](#phase-3-feasibility--product-numbers-variant-2026-06)).

**How it's apples-to-apples.** Both engines read the *classic cbq file-datastore
layout* `<root>/<namespace>/<keyspace>/<key>.json` (one JSON doc per file). Both use
cbq's parser+planner (identical plan); what differs is the **execution engine** —
n1k1's `[]byte` byte-engine vs cbq's boxed `value.AnnotatedValue` executor. The cbq
side runs via the `n1k1-query` fork's **`local-benchmark`** branch: `cmd/localbench`
drives `test/filestore` over the same `dir:` datastore, timing `filestore.Run` +
`runtime.MemStats`. Build once: `CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go
build -o /tmp/localbench ./cmd/localbench`, then `CBQ_LOCALBENCH=/tmp/localbench`.

**Fairness — both columns are the FULL `parse→plan→execute`**, warm (median of REPS,
first few dropped), reporting median ms + median allocated MB. On the n1k1 side the CLI
reports `Result.RunElapsed` (the whole `Session.Run`), *not* just `ExecOp` — see the
[gotchas](#measurement-gotchas-hard-won); measuring ExecOp-only once showed a bogus
~40× "win". Run: `[COMPILED=1] [CBQ_LOCALBENCH=...] python3 test/benchmark/versus/bench.py`.

**Two scenarios, chosen to separate I/O from execution:**
- **files** — `orders`/`cust`, one doc per file. Realistic but **I/O-bound**: a scan
  opens *every* file, a cost both engines pay, so wall time is close.
- **bulk** — a few docs each holding a large `items[]` array, driven by **UNNEST**.
  Volume lives *inside* documents → file I/O is trivial and per-row execution dominates.

**Findings (indicative, apple-silicon, warm):**

| scenario | time (n1k1 vs cbq) | memory (n1k1 vs cbq) |
|---|---|---|
| files (I/O-bound) | ~1.0–1.1× (tie) | **2–6× less** |
| bulk (compute-bound) | **5–8× faster** | **5–26× less** |

The bulk gap is the thesis in one number: cbq boxes every unnested array element into a
`value.Value` (`SimpleUnmarshal` + map); n1k1 UNNESTs and evaluates on raw `[]byte`.
The files tie is expected (both pay `os.Open` per doc) — but see the layout lesson: that
tie is an artifact of the one-doc-per-file layout, not the engines.

**n1k1 stays native (no boxed fallback).** `EXPLAIN` on every `versus` query shows zero
`⟨boxed⟩` markers — all project/filter/join/UNNEST run on the byte path, so the wins are
genuine native-vs-boxed, not measurement artifacts.

### Compiled-codegen column (`COMPILED=1`) — the Futamura payoff, isolated

`COMPILED=1` adds n1k1's `-prepare=full` **standalone-compiled EXECUTE**: each query is
`PREPARE`d once (emitting cbq-free Go, `go build`ing a child binary — needs the `go`
toolchain + `N1K1_SRC`), then `EXECUTE`d warm (build cost dropped). The lane is a **thin
child**: the *parent* scans + JSON-pipes each input record to the child; the *child*
runs only the compiled compute and pipes rows back. So the table splits it:

- **`comp`** — whole round-trip (parent scan + pipe in + child compute + pipe out).
- **`core`** — the child's *own* reported compute wall (`N1K1_CORE_NS`), i.e. the
  specialized, Futamura-projected query code over in-memory records, IPC excluded.
- **`core:i` = core / interp** — on the bulk rows (interp ≈ all compute) `<1.0×` means
  the compiled code is genuinely faster.

**Finding — two opposing truths:** end-to-end, `comp` is ~1.2–3.0× *slower* than the
interpreter (the thin-child IPC — JSON-marshalling inputs, piping rows — costs more than
the compute it accelerates). **But the specialization itself pays off**: `core` runs
**~1.3–1.6× faster than the interpreter** on the compute-bound bulk rows (`core:i`
≈ 0.64–0.77×). The Futamura projection is a real win, just buried under IPC in this
thin-child deployment (which targets the standalone/MQO scenario, not single-`EXECUTE`
over a pipe; see `DESIGN-prepare.md`). No compiled MB column — the compute runs in a
child process, invisible to the parent's heap-alloc counter. `n/a` = didn't compile
standalone (today any `JOIN ... ON KEYS`: the thin child can't do a per-row datastore
fetch). Two codegen bugs had to be fixed to make aggregates + two-field arithmetic even
compile — see `glue.TestExecuteCompiledAggAndArith`.

### Container-format scenarios: packed `.jsonl` and parquet+VARIANT

Two more `versus` scenarios put the *same* order docs in a single container file, to
compare storage formats with the per-file-`open` cost removed:

- **packed `.jsonl`** (`orders_jsonl`, own `NDOCS_JSONL` knob). cbq gets a real column
  here via the fork's new **`jsonl:` in-memory datastore** (`local-benchmark` branch,
  `datastore/jsonl` — a thin `datastore/mock` adaptation loading `<root>/default/<ks>/*.jsonl`;
  `bench.py` sets `SITE=jsonl:`; `ON KEYS` joins stay n1k1-only since `cust` has no
  `.jsonl`). **Result (200K-doc `.jsonl`, engine-vs-engine): n1k1 ~5–12× faster and
  ~50–3500× less memory than cbq** (e.g. group+agg 108 ms / 0.18 MB vs 1143 ms / 627 MB) —
  the clean byte-engine-vs-boxed gap the I/O-bound `files` scenario masked.
- **parquet+VARIANT** (`VARIANT=1`, `orders_variant`; the jsonl re-encoded so docs are
  identical). arrow-go v18 has a native Parquet VARIANT extension type; n1k1's
  `records/parquet.go` reads it (Phase-0 projects the VARIANT to JSON at the scan
  boundary — no CLI flag; `VariantFidelity` Phase-1 is a package global with no CLI
  setter, only needed for typed-scalar fidelity our plain-JSON orders lack). cbq is n/a
  (iceberg-go v0.4.0 has no VARIANT). **Result (n1k1, 200K docs): whole-doc VARIANT is
  ~1.5–2.4× SLOWER and far more memory-hungry than the same docs as `.jsonl`** (count+filter
  154 ms / 181 MB vs 65 ms / 0.21 MB). An *unshredded* VARIANT is one column read + decoded
  whole per row (parquet/arrow batch materialization + VARIANT→JSON), with none of the
  columnar sub-field projection that would justify the format — n1k1's `.jsonl` path is
  near-alloc-free by contrast. **VARIANT's payoff needs shredding (typed sub-columns) or
  plain typed columns for column-selective queries**; whole-doc-as-one-VARIANT just adds
  format overhead. (Generators: `test/benchmark/versus/gen_variant.go`, `gen.py`.)

## I/O-bound scan & the file-layout lesson (2026-07)

The `versus` **files** scenario prompted the question "is n1k1 just waiting on I/O, and
would concurrency help?" The answer reframed the whole problem, so it's recorded here.
*(All exploration below lives as env-gated throwaway hacks in an experimental worktree,
not landed — `N1K1_PSCAN`, `N1K1_STAGE`, `Stage.NoCopy`. The findings are the deliverable.)*

**It IS I/O-blocked.** A filtered scan over 20 000 one-doc `.json` files is **~78%
off-CPU** (20 reps: 66.8s wall vs 14.7s on-CPU) — serial per-file `open/stat/read/close`
syscalls, one at a time in one goroutine.

**Parallel scan helps ~3.9×, but it's a band-aid.** Fanning the file list across N
`base.Stage` supplier goroutines (each `WalkPrelisted` over a partition) gives ~3.9× on
real filter/group/project queries — **but only at ~128 actors** (the actors are
I/O-*blocked*, so you need ~10× oversubscription vs cores; ≤16 does nothing, 32→1.8×,
64→3.3×, 128→3.9× peak, 256 regresses). Profiling at 128 actors: ~359% on-CPU, 97.6% in
`syscall.syscall` under the actors — the ceiling is the **OS/FS capping concurrent read
syscalls to ~3.7 effective cores** (a containerized/overlay FS, ~160µs kernel per file);
the consumer (parse/filter/group) + dir walk are ~37ms each, **negligible**. So batch
size and in-flight depth are **noise**, a zero-copy handoff is **unneeded** (the per-row
`ValsDeepCopy` isn't the bottleneck), and parallel *compute* would be **pointless**.

**The punchline — packing beats parallelizing opens by two orders of magnitude:**

| layout (20 000 docs, filter+group) | time |
|---|---|
| 20 000 `.json` files, serial | 3194 ms |
| 20 000 `.json` files, parallel (128 actors) | 703 ms (4.5×) |
| **1 `.jsonl` file, serial** | **13 ms** |

The actual compute (parse+filter+group over 20 000 docs) is **~13 ms** — free. The whole
3.2s was per-file open/read/close syscall overhead × 20 000, nothing to do with the data.
Packing the same docs into one `.jsonl` (which n1k1 already reads) is **~245× faster than
serial, ~54× faster than the parallel hack**. So: the one-doc-per-file layout (the classic
cbq file-datastore shape) is pathologically syscall-heavy; the real fix is a **container
format** (`.jsonl`/parquet), not parallel scanning. Parallel scan is a genuine
consolation prize only when you're *stuck* with a directory of many files (Couchbase
exports, log dirs, cbcollect bundles).

**Read-ahead *decoupling* (as opposed to parallelism) was a dead end.** A single-supplier
`base.Stage` that overlaps the child's I/O with the consumer's compute (built as a
one-child `OpStage` + a flag-gated plan-rewrite) gave **no** win (one supplier can't
parallelize the serial opens; the consumer is too small to overlap) and was **3× WORSE**
on the hot, re-executed inner of a nested-loop join (a fresh `Stage` goroutine+channel per
outer row — ~80 000 spawns on `bulk unnest+join`).

**Caveats.** Warm page cache; a containerized FS (expensive per-file syscalls, ~3.7×
concurrency cap). On bare metal / a real disk the ratios shift (cheaper opens shrink the
packing win toward ~10–15×; cold-cache disk latency may let parallelism hide more). The
*direction* is universal: fewer syscalls always wins; parallelism only hides them.

## Measurement gotchas (hard-won)

Mistakes that produced confidently-wrong numbers before being caught — encode these into
any new harness:

- **Time the FULL request, not just `ExecOp`.** The n1k1 CLI footer originally timed only
  `ExecOp` (`Result.Elapsed`); against cbq's full request that showed a bogus ~40× "win".
  Fixed by `Result.RunElapsed` (parse+plan+convert+execute). For tiny SQL it ≈ `Elapsed`;
  for a large inline literal, parse dominates — so always measure end-to-end.
- **Compiled memory isn't visible to the parent.** A standalone-compiled EXECUTE runs in a
  *child process*, so the parent's `.stats`/heap-alloc counter can't see its allocations —
  the compiled column is **time-only** (an apples-to-apples MB would need child RSS).
- **Validate row COUNTS when hacking the scan, not just speedup.** The parallel-scan
  prototype had two silent bugs: (1) an over-conservative dispatch guard
  (`scanProjectColumns==nil && !hasFilter`) quietly kept every *filtered/projected* query
  on the serial path → a false "payload queries don't parallelize" result; those pushdown
  hints are no-ops on JSON dirs anyway (the filter/project ops above the scan do the real
  work). (2) actors didn't send the done-signal (`yieldErr(nil)`), so `base.Stage` never
  flushed each actor's final partial batch → **~18% of rows silently dropped**
  (20 000→16 384 = 256×64). Both invisible unless you check counts.
- **Float SUM is non-deterministic under reordering.** Parallel actors (or partial
  aggregation) sum in a different order, so `SUM` differs in the last 1–2 ULPs — expected
  float non-associativity, not a correctness bug (but it will trip an exact-diff oracle).

## What already exists

Build on these, don't duplicate:

- `test/n1k1_interp_test.go`: `BenchmarkInterpExprEq/Str_{1,1000,100000}Docs`,
  `BenchmarkInterpGroupBy_{1,100,10000}Docs`.
- `base/`: `BenchmarkCanonicalJSON`, `BenchmarkValCompare`, `BenchmarkParse`,
  `BenchmarkEncodeAsString`; `test/BenchmarkBoxing`, `test/BenchmarkValCompare*`.
- `make benchmark-expr-eq` wires `-tags n1ql` + `CGO_ENABLED=0` + benchmem.

## Open decisions

1. **Generator shape** — one "contact"-like doc (in use) vs several
   (wide/narrow/nested).
2. **Scale ceiling** — 1M docs fine for in-memory scans; spilling benches need
   cardinality high enough to cross the rhmap/store threshold (documented above
   as its own result).
3. **Phase 3 trigger** — deferred; revisit HTTP-over-the-wire vs a fork-patched
   in-process timing hook when a buildable server tree exists.

## Phase 3 feasibility — product-numbers variant (2026-06)

_Superseded for the executor-vs-executor comparison by
[Phase 3 realized](#phase-3-realized--n1k1-vs-cbq-over-local-files) (the `versus`
harness + `n1k1-query` `local-benchmark` fork). This section remains the recipe for the
heavier **product-numbers** run — a full from-source cbq-engine over a `dir:` datastore —
which is still blocked in a stock env._

Goal: time the **same** queries over the **same** data through
couchbase/query's executor. **Blocked in a stock dev env** (Couchbase Server.app
installed, no server source tree): all three probed mechanisms fail. (a) An
in-process race is impossible — `query/execution` imports `n1fty/verify → cbft`
(cgo), pruned by the pure-Go decouple. (b) The prebuilt `cbq-engine` from
Server.app can't run standalone — `main.go` calls `waitForInitialSettings()`
unconditionally, which blocks forever on the metakv settings notifier without a
cluster (no flag bypasses it; never binds :8093). (c) A patched from-source build
would work (one-line guard, below) but needs the whole Couchbase Server module
graph (`n1fty`, `cbauth`, `indexing`, `cbgt`, `cbft`, … via query go.mod
`replace => ../<sibling>`), a `repo sync`-against-a-manifest exercise out of
scope here. Phase 1/2 stand alone as the perf story.

### To run Phase 3 later (recipe)

On a buildable couchbase/query, the closest analog to n1k1 (file-based, no
KV/GSI/network) is the standalone `dir:` datastore:

1. Patch `server/cbq-engine/main.go` — guard the settings wait:
   ```go
   if strings.HasPrefix(*CONFIGSTORE, "stub:") {
       // standalone: no cbauth/metakv, default settings
   } else {
       initialCfg, num_cpus = waitForInitialSettings()
   }
   ```
2. `CGO_ENABLED=1 go build -o cbq-engine ./server/cbq-engine`
3. `./cbq-engine -datastore "dir:$PWD/test/suite/json" -configstore stub:`
4. Warm up, POST queries to `http://localhost:8093/query/service`
   (`--data-urlencode statement=...`), time N runs, compare to n1k1. HTTP/server
   overhead dominates micro-comparisons — prefer large per-query row counts.

**Heavier alternative (real product numbers, different architecture):** start
full Couchbase Server (`couchbase-cli cluster-init`, `bucket-create`, `cbimport`
corpus, `CREATE PRIMARY INDEX`), then query it — but that is KV + GSI + network,
not a file scan, and modifies the local install (opt-in).
