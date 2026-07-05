# n1k1 — columnar & SIMD design notes

Design analysis and running record for **columnar (vectorized) execution** and
**SIMD** in n1k1, organized around a **six-step plan**. Longer measured
details, the SIMD deep-dive, and prior art live in the appendices.

See also: `DESIGN.md` (row engine + the lz compiler), `DESIGN-data.md`
(§ Parquet/Arrow scan sources), `DESIGN-exprs.md` (the no-boxing expression
model), `DESIGN-stats.md`.

**Thesis.** `base.Val` is `[]byte` and axis-agnostic — nothing says a `Val` holds
*one* value, so it can hold a *packed column*. n1k1 is row-at-a-time today, but it
is a **query compiler** (`intermed_build` projects the lz interpreter into
specialized Go), which is the unusual leverage for hosting a vectorized lane.
Measured (arm64, no SIMD): fixed-width columnar beats the row-JSON path **40–730×**,
and the win is **not-parsing + touching one column stripe**, *not* SIMD. So the
plan is source-first and evidence-gated, with SIMD as a last, optional leaf.

-------------------------------------------------------
## The core idea

- **Today: strictly row-at-a-time.** `type Val []byte` (JSON), `type Vals []Val`
  (one row; slots positionally aligned with `Labels` — labeled "registers"),
  pushed upward via `YieldVals(Vals)`. Expressions are per-row
  (`ExprFunc(vals) Val`); aggregates fold row-by-row (`base/agg.go`); the only
  batches that exist (`base/stage.go`'s `[]Vals`) are **row-major copies** for
  concurrency, not vectorized compute. No vectorized kernel exists anywhere.

- **Transpose the axes.** A *column batch* reuses the same two-level `[][]byte`
  shape but reads it column-wise — each slot is a packed vector of M values, not
  one scalar:

  ```
  row batch:  Vals = [ "alice",       42,       {"x":1}   ]   <- ONE row, 3 cols
  col batch:  Vals = [ ["alice",      [42,      [{"x":1},  
                        "bob","cara"]  43,44]    ...]     ]   <- MANY rows
  ```
  The container, push plumbing, recycling, and Labels alignment all survive; the
  row count M is the new hidden dimension.

- **The compiler provides leverage.** `intermed_build` already projects lz
  `engine/*.go` into specialized Go (Futamura/LMS). The same type inference
  `TODO.md:250` wants ("`sales < 1000` is numeric") is the precondition for a
  fixed-width column encoding — so columnar and static-typing are one project.
  Precedent that a slot's bytes can carry a non-JSON interpretation:
  `engine/expr.go:ExprLabelUint64` already reads a slot as a packed LE `uint64`.

- **The tension: schemaless JSON.** A JSON column is variable-width, untyped, and
  three-state (`MISSING` ≠ `NULL` ≠ value; `base/base.go`). So the fixed-width fast
  path is an **opt-in specialization**; the row/JSON path is the correctness fallback.

-------------------------------------------------------
## The plan: Steps 1–6

Evidence-gated, source-first, kill-early: prove the ceiling *and* the workload fit
before building, and let columnar bytes enter from a columnar **source** rather
than synthesizing them from rows. The row engine is already push-based, compilable,
and garbage-free — a fast baseline the columnar lane must beat.

- **Step 1 — Characterize the workload. ✅** Target: local directories of mixed
  files incl. Parquet/Iceberg (a DuckDB-style "query your files in place" niche) —
  a real analytical-scan segment, so columnar is justified.

- **Step 2 — Spike the ceiling. ✅** arm64, pure Go, no SIMD: fixed-width SUM/filter
  beats the row-JSON path **40× (narrow docs) to 730× (50-field docs)**, sitting at
  the native-`[]float64` ceiling. The win is *not-parsing* + *touching one stripe*;
  SIMD would be additive, not load-bearing. The ≥3–5× gate is cleared by 10–150×.
  Numbers: Appendix A. (`test/col_test.go`.)

- **Step 3 — Ship a Parquet source (transpose-to-rows). ✅** `records/parquet.go`
  (arrow-go) decodes Parquet → JSON rows, wired into `records.OpenFile`; a
  `.parquet` file is now a queryable keyspace (`TestParquetQueryEndToEnd`;
  `examples/warehouse/`). `!js`-guarded so arrow-go stays out of wasm builds. A
  correctness feature with no engine change. Numbers: Appendix A.

- **Step 4 — Projection pushdown via sidecar interfaces. ✅** Optional
  `records.ColumnsProjector` / `ColumnsSource` (+ `ColumnMeta`) the scan
  type-asserts (the `SubPathser` idiom) — the core `Source` stays `{Next, Close}`
  and non-implementers fall back to the full transpose. The wanted-column set is
  **reused from cbq's planner** (`plan.Fetch.EarlyProjection()`, computed via
  `expr.FieldNames` + a full `expression.IsCovered` check). `walkSource` forwards
  the projection to each per-file source. The transpose was made **zero-alloc**
  (`appendRecordsNDJSON`: 526K→2.1K allocs/op, 2.9× faster, replacing arrow-go's
  `array.RecordToJSON`'s `interface{}`/`encoding/json` boxing). See § Key decisions +
  Appendix A.

- **Step 5 — First vectorized op: aggregation, no transpose. ◀ NEXT.** A fused
  Parquet-scan→agg over a proven-typed, non-null column, reusing `AggSum`'s
  accumulator via `AggCatalog["sum_v_float64"]`, feeding the borrowed Arrow buffer
  as a `base.Val`. **Aggregation is first because its output is one row** — it rejoins
  the row engine transpose-free. Full design (decision flow, scalability, prereqs)
  below in § Step 5.

- **Step 6 — Expand on measured wins.** More aggregates, expressions
  (vectorized arithmetic kernels), selection-vector filter, dictionary GROUP BY —
  each gated on a benchmark that beats row-at-a-time on a real workload. **SIMD
  lives here: last and optional** — an amd64-only accelerator with a mandatory
  scalar-Go path everywhere else; batching alone already carries the arm64/WASM
  win. Why SIMD is deferred: Appendix B.

-------------------------------------------------------
## Step 5 in detail

Borrowed Arrow column → vectorized aggregation, no transpose. Land-small order:
**5.1** `SUM(x)` fused scan→agg (null_count=0 numeric) → **5.2** multi-agg
`SUM(x),SUM(y)` (an N-tuple over one scan pass, nearly free) → **5.3** `SUM(x+y)`
(vectorized arithmetic kernel, inline fold) → **5.4** chained vectorized ops +
type-vector + selection vectors → GROUP BY (dictionary keys) → codegen north-star.
**Prereqs before 5.1:** `walkSource.Columns()` (multi-file keyspace schema) and a
`ColumnBatchSource` on `parquetSource` yielding borrowed Arrow columns.

**How the system decides to vectorize (`sum` → `sum_v_float64`).** A *two-input*
decision:
1. *Plan shape* (base.Op / cbq plan): ungrouped, agg is SUM/etc. of a **bare
   field**, single Parquet-capable keyspace.
2. *Column type + `null_count`* — from `ColumnsSource` (the Parquet footer). The
   plan is **schemaless**; cbq has no type for `x`, only the Parquet footer
   does.

The decision lives in a **post-conv rewrite pass** (like `addColumnProjections`):
gate on plan-shape; consult `ColumnsSource`; if the column is a supported fixed-width
type with `null_count==0`, swap the `group` op's `aggCalcs[i][0]` from `"sum"` to
`"sum_v_"+kernelType` and mark the columnar feed; **else leave the row path**
(conservative fallback — the same empty→read-all discipline as projection
pushdown). Multi-agg `SUM(x),SUM(y)` is an N-tuple in one scan pass; `SUM(x+y)`
needs a vectorized arithmetic kernel first (5.3).

**Reuse `AggSum`, don't duplicate.** SUM's whole state is one float64 (8 bytes)
and `Result` formats it. So `sum_v_float64` = `&Agg{Init: AggSum.Init, Result:
AggSum.Result, Update: <vectorized fold>}` — the type lives in the **catalog key**,
`Init`/`Result` reused verbatim ⇒ **byte-identical output** ⇒ the differential test
is exact string equality. Not a widened `Agg` interface (no `UpdateColumnXYZ`
method explosion). Generalizes: MIN/MAX reuse their accumulators, AVG the 16-byte
sum+count, COUNT the counter.

**Zero-copy from Arrow.** arrow's `array.Float64.Float64Values()` is an unsafe reinterpret
of the underlying buffer (no parse/copy), and `arr.Data().Buffers()[1].Bytes()` is
that same packed little-endian buffer — **already a `base.Val`**, borrowed. So the
column flows through the standard `Update(val Val, …)` signature with zero re-encode;
`sum_v_float64.Update` reinterprets via `binary.LittleEndian` + `math.Float64frombits`
(keeping `base` arrow-free), summing float64 **in scan order** ⇒ bit-exact vs the
row fold. `int64` columns → `float64(v)` per slot, matching the row path's
`ParseFloat64`. Borrow lifetime: valid until `batch.Release()`; Update-then-Release,
one call per batch.

*No label sigils at first.** Columnar shape is carried by a **parallel `[]ColKind`** aligned
with `Labels` (StatsBase-style; not a `@col.f64:` label prefix, which would force
every `IndexOf` to strip it). Deferred label sigils until 5.4, when columns flow
*between* ops; fused 5.1–5.3 ops know their own inputs and need none.

**Traps to pre-empt.** `COUNT(col)` ≠ `COUNT(*)` on nulls (null_count=0 sidesteps
v1); multi-file partial-aggregate combine (Σ / min / max associative, AVG carries
its count); and the fused path bypasses the row engine's `Stage` / stats /
`YieldStats` — preserve scan stats, `LIMIT`, and cancellation.

### Does Step 5 scale, or is it whack-a-mole?

Done naively it **is** whack-a-mole — a detector per query shape × a kernel per
(operation × type). Three levers plus a universal fallback keep it bounded:

1. **A general "columnarizable?" predicate, not per-shape matching.** A recursive
   bottom-up question over the op/expr tree: each node answers "can I run columnar
   given my children's shapes and the source column types?" Query shapes fall out
   of *composition* — you never enumerate them. It's the StatsBase approach applied to
   shape: one inference pass, not N cases.
2. **Generics kill the type combinatorics** — `sumV[T Numeric]`, one kernel per
   operation, the compiler instantiates per type (Go 1.25).
3. **Pointwise lifting** — a *typed* scalar `f(a,b)` becomes `for i {
   out[i]=f(a[i],b[i]) }` mechanically, vectorizing the whole pointwise
   arithmetic/comparison surface with no per-function work.

**Honest boundary (what doesn't fall out for free):** reductions (a fixed ~5) and
reshaping relational ops (filter+selection, group-by, join, sort — the bounded
~dozen) are hand-authored once; the untyped/string/date long tail **defers to
the row engine** as a graceful fallback. So coverage = *pointwise
(≈free via lifting)* + *fixed reductions* + *fixed relational ops* + *everything
else → row engine*. Any composition of vectorized primitives over typed columns
works; that is not whack-a-mole.

**The codegen way out (the north-star).** Rather than hand-maintain a second tower,
teach `intermed_build` to project a **column-batch target** from the *same* lz
source — the generated program loops over column batches (inner element loop, exprs
inlined) instead of over rows. Write each kernel once (scalar, lz); the compiler
emits *both* lanes, chosen per query by type inference. **Pointwise lifting is its
own LegoBase/LMS source-transform pass** (`// <== pointwise` over a typed scalar
kernel) whose lz-style output feeds *both* the interpreter (so vectorized kernels
can be differential-tested vector-vs-scalar *before* the compiler is involved) and
`intermed_build`. Prerequisite: compile-time type inference (`TODO.md:250`) — you
can only project the typed column loop once you know the column types. See
Appendix C for the LMS/LegoBase lineage.

-------------------------------------------------------
### Beyond `null_count == 0`: null handling & footer-stat shortcuts

v1 gates on `null_count == 0`. Lifting that — and going faster still — draws on the
footer stats the rewrite already reads (`ColumnMeta.{Min,Max,NullCount}` + the file
row count). A menu, fastest first:

- **Aggregates from stats — *zero scan*.** `COUNT(*)` = `num_rows`; `COUNT(x)` =
  `num_rows − null_count` (correct for *any* null_count → supersedes `count_v`,
  reads no data pages); `MIN(x)`/`MAX(x)` = the footer min/max. Multi-file
  aggregates associatively (Σ counts, min-of-mins, max-of-maxs). A `metadata-agg`
  op the rewrite emits when every agg is COUNT/MIN/MAX (mixed with SUM/AVG → scan,
  or a hybrid). Caveats: stats may be absent → fall back; **float MIN/MAX has a
  NaN/signed-zero subtlety** (Parquet excludes NaN by convention, which matches our
  transpose's NaN→null but is writer-dependent) → COUNT and integer MIN/MAX are safe
  first, float MIN/MAX gated. The purest "operate on metadata, not data"
  (Appendix C).
- **Sentinel-for-null (materialization-time), for SUM/AVG.** A borrowed Arrow buffer
  keeps nulls in a *separate* validity bitmap (null slots hold undefined bytes), so
  a branchless reduction can't read them directly. But when we materialize our own
  fixed-width column, fill null positions with the reduction's identity /
  out-of-range sentinel — SUM→0, MIN→(> max), MAX→(< min), the sentinel taken from
  the footer min/max (so the two shortcuts compose). The hot loop stays branch-free:
  the "pad with identity" tail-trick (Appendix B) applied to nulls, moving null
  handling to a one-time bitmap pass instead of a per-element check.
- **Masked kernel (validity bitmap) — the general case.** For SUM/AVG over a borrowed
  buffer we can't rewrite, read the validity bitmap and skip null lanes — the same
  masking AVX-512/SVE use for the tail. Always applicable, slower than the two above.

-------------------------------------------------------
## Key design decisions (settled)

- **Columnar source = optional sidecar interfaces**, not a widened `Source` (the
  `SubPathser` idiom). `ColumnsProjector{ ProjectColumns([]string) error }` and
  `ColumnsSource{ Columns() []ColumnMeta }`; non-implementers fall back to the row
  transpose. Reuse cbq's `EarlyProjection()` for the field set.
- **Column encoding = the Arrow value buffer itself.** Its raw `[]byte` *is* the
  packed fixed-width column (`base.Val`, zero-copy) — no re-encode. The JSON-array
  encoding barely helps (1.3×, Appendix A) — skip it. Arrow offsets+payload (strings)
  and dictionary codes (GROUP BY) come later.
- **Shape carried as a parallel `[]ColKind`** (StatsBase-style), not label sigils,
  which might be introduced only at 5.4 when columns flow between ops.
- **`null_count == 0` fast path first** — no validity bitmap ⇒ unmasked kernel;
  nulls/selection bitmaps come with the relational ops.
- **Reuse existing accumulators** (`AggSum` etc.) via typed catalog keys
  (`sum_v_float64`); do not widen the `Agg` interface.
- **Differential testing from the start** — the row lane is the oracle; scalar-Go
  kernels sum in scan order ⇒ *exact* equality (SIMD would force epsilon compares,
  another reason SIMD is last).
- **Reuse cbq's plan analysis, don't hand-roll** — the recurring Step-4 lesson
  (EarlyProjection), reapplied to Step 5's vectorizability detection.

-------------------------------------------------------
## Still-open questions

- **Compile-time-only lane forever?** (interpreter stays row, compiler emits
  row-or-column). Leaning yes; the lifting pass would hand the interpreter
  vectorized kernels anyway if we ever want them for its own testing.
- **String/dictionary encoding layout.** Numeric is settled (use Arrow's LE buffer
  directly). Variable-width strings: Arrow-compatible offsets+payload vs n1k1-native?
  Lean Arrow-compatible (near-free Parquet interop).
- **How much columnar to ship to WASM at all?** No `archsimd`/`simdjson`/asm there;
  the in-browser win is *batching* (fewer JS-boundary crossings), not vector width.
- **Operate on encoded data?** Computing on Parquet's RLE/dict pages *without*
  decoding (Appendix C) is the real "stop transposing" — a frontier beyond Step 5.
- **Predicate/row pushdown.** cbq's `iceberg_row_filter.go` skips rows at the Arrow
  level before transposing; n1k1 filters after. A future `RowGroupPruner`/predicate
  sidecar, pairing with 5.4 selection vectors.

-------------------------------------------------------
## Appendix A — measured results

### Step 2 spike (arm64, pure Go, NO SIMD) — the ceiling

`test/col_test.go`. SUM/filter over N float64 values; row path is faithful to
n1k1 today (whole JSON doc, `jsonparser.GetFloat` per row). All paths zero-alloc.

**SUM, ns/value, narrow 1-field doc:**

| N | row-JSON | JSON-array (enc 1) | fixed-width (enc 2) | native `[]float64` |
|---|---|---|---|---|
| 64  | 36.8 | 28.1 | 0.68 | 0.44 |
| 1K  | 38.8 | 30.2 | 0.87 | 0.83 |
| 64K | 38.5 | 29.8 | 0.88 | 0.87 |
| 1M  | 39.8 | 30.0 | 0.91 | 0.87 |

Fixed-width is ~44× the row path and sits at the native-`[]float64` ceiling (the
LE decode is nearly free); the row cost is JSON number parsing. **No tipping point
in N** — fixed-width wins from N=64. JSON-array (enc 1) barely helps (still parses
text) → prioritize fixed-width (enc 2).

**Doc-width sweep (N=1M) — where the "vertical stripe" shines:**

| doc width | row-JSON ns/value | fixed-width | speedup |
|---|---|---|---|
| 1  | 38.4 | ~0.9 | 42× |
| 5  | 83.6 | ~0.9 | 93× |
| 20 | 294  | ~0.9 | 327× |
| 50 | 660  | ~0.9 | 730× |

The row path scales linearly with doc width (a left-to-right parser skips past
every unwanted field); the fixed-width column is constant. So the win grows with
exactly what hurts row-at-a-time: **wide records, few projected fields.**
(Filter-count `price>500`, N=1M, narrow: row 39.5 vs fixed 0.69 → 57×.)

**Economic break-even, JSON source** (you pay to *build* the column, ~38 ns/value
once, then ~0.9/op): row = 38·K, columnar = 38 + 0.9·K → columnar wins once **K >
~1** (any query touching a column more than once). From a **Parquet source (no
parse to build), it wins unconditionally.**

### Step 3/4 Parquet prototype (arrow-go, arm64)

`test/parquet_test.go`. Wide file `{id, price, f0..fN}`, query wants one column.

**Projection pushdown** — read only `price` vs all 14 columns: 0.6 ms / ~0.02 MB
vs 50 ms / ~10.3 MB → **0.2% of the bytes, ~80× (137× at 1M rows)**, done by the
file format.

**Free footer metadata** (no data pages read): `price type=DOUBLE null_count=0
min=0.5 max=999.5`, etc. → type picks the kernel; `null_count=0` ⇒ no validity
bitmap; min/max are zone-map inputs.

**Parse-free column SUM:**

| SUM path | ns/value | vs row-JSON | allocs |
|---|---|---|---|
| Arrow column, kernel only | 0.93 | ~56× | 0 |
| full open+project+decode+sum | 3.0 | ~17× | ~2800 / 18 MB |
| row-JSON baseline | 52 | 1× | 0 |

The kernel hits the fixed-width ceiling with *no parse to build the column*. The
full path's allocs are Arrow's decode/materialization (per batch); the zero-copy
end-to-end path (0.93, not 3.0) is Step 5.

### Step 4's zero-alloc transpose

`appendRecordsNDJSON` (type-switch per Arrow column into a reused buffer; RFC-8259
escaping; NaN/Inf→null; zero-copy `String.Value`) replaced `array.RecordToJSON`
(which boxed each value to `interface{}` + `encoding/json`, ~8 allocs/row).
Measured (65K rows, 6 cols): **526K → 2.1K allocs/op (~248×), 2.9× faster**;
residual allocs are per-batch Arrow decode. `fastRenderable` gates it, with a
`RecordToJSON` fallback for exotic types (timestamp/decimal/list/struct). Proven
equivalent by `TestParquetFastTransposeEquivalence`.

-------------------------------------------------------
## Appendix B — SIMD reality (why it's last and optional)

**Go SIMD is toolchain-limited.** No compiler autovectorization of general loops
(the fast stdlib bits are hand-written Plan9 asm). Four ways to reach it: (1) raw
`.s` asm per-arch; (2) `avo` (asm-generator; what klauspost/minio use); (3) cgo
(defeats the point); (4) `GOEXPERIMENT=simd` → `simd/archsimd` (Go 1.26) — but
**amd64-only, no ARM/NEON, no WASM, unstable API**. Realistically reachable today
(portably): fixed-width int/float compare, arithmetic, min/max/sum, bitmap
AND/OR/POPCOUNT — exactly what the columnar encodings produce.

**Batch first, then SIMD.** golang/go#77647 documents the Go↔asm call-boundary
cost; on small chunks it eats the SIMD win. So per-`Val` SIMD is a guaranteed loss
— SIMD only pays amortized across a whole column batch. Combined with the Step-2
finding (the win is not-parsing + one-stripe, not vector width), SIMD is a **leaf
optimization on top of the columnar batch**, mandatory scalar-Go fallback, amd64-only.

**The tail/remainder.** A column of N rarely divides by the lane count L (128/256/512
bits ÷ elem size = 2–8 lanes). Handled by: a scalar remainder loop (default);
masked ops (AVX-512/SVE); padding to a lane multiple with identity values; or an
overlapping last block (idempotent ops only). Columnar engines fix a batch size
that's a multiple of L (DuckDB's 2048) so tails vanish except on the last batch.
n1k1 gets techniques for free: the validity/selection bitmap *is* the mask, and the
mandatory scalar fallback *is* the remainder loop.

**SIMD-json (`minio/simdjson-go`)** is a *different* use — accelerating JSON
*parsing* (SIMD structural scan → a "tape"), not compute. But it's **AVX2+CLMUL,
no fallback, amd64-only, no WASM/arm64**, and produces a whole-document tape rather
than jsonparser's lazy zero-copy sub-slices — so it fights n1k1's hot row path. It
fits only as an *ingest* front-end for the columnar path (parse once, scatter to
column buffers), gated on `SupportedCPU()`. SIMD-parse and SIMD-compute are
separate, independently-justified bets.

-------------------------------------------------------
## Appendix C — prior art & inspiration

Techniques from leading engines worth stealing, each with the n1k1 tie-in:

- **DuckDB** — the closest reference: embedded, vectorized push-based, reads Parquet
  directly, selection vectors + dictionary + late materialization + morsel
  parallelism. The target this path walks toward.
- **Compiled vs vectorized — "both"** (Kersten/Leis et al., VLDB 2018): neither
  dominates; n1k1 is unusually placed to be a **hybrid** (interpreter + Futamura
  compiler), with the pointwise-lifting pass as the bridge. HyPer/Umbra = compiled;
  MonetDB/X100 → VectorWise = vectorized.
- **LegoBase / DBLAB + LMS** (Klonatos & Koch, VLDB 2014; Rompf/Odersky): build the
  engine in a high-level language, apply optimizations as **source-to-source
  transforms** — the home of the lifting idea and of n1k1's `intermed_build`.
  Lineage: LegoBase → DBLAB/SC (SIGMOD 2016) → LB2 (SIGMOD 2018) & **Flare** (OSDI
  2018, LMS accelerating Spark). Reality check: the LMS-*Scala* engines stayed
  academic; the *idea* shipped via other codegen — **HyPer/Umbra** (LLVM →
  Tableau/CedarDB), **Spark Tungsten** (JVM bytecode), **Hekaton** (C→DLL), Impala
  (LLVM). Conceptual cousin: **GraalVM/Truffle** *automatically* does the first
  Futamura projection (interpreter → compiler) — the automatic version of
  `intermed_build`. So n1k1 is a pragmatic Go-source-gen member of this family, and
  the family's results (LB2/Flare matched hand-tuned engines) say it works.
- **Late materialization** (C-Store/Vertica; Abadi, ICDE 2007): carry column
  positions/IDs as long as possible, materialize values last or never (aggregates).
  The deep generalization of "aggregation output is tiny" — the frontier beyond
  Step 5.
- **Operate on compressed/encoded data** (Abadi et al., SIGMOD 2006): SUM over RLE =
  value×runlength; GROUP BY on dict codes; predicates on bit-packed data — *without
  decoding*. Parquet pages are RLE/dict/bit-packed and we fully decode via Arrow;
  computing on encoded pages would skip the decode — the real "stop transposing."
- **Micro-adaptivity** (Răducanu/Boncz/Zukowski, SIGMOD 2013): keep several kernel
  flavors, profile per batch, pick the fastest — a runtime extension of our
  compile-time kernel dispatch.
- **Morsel-driven parallelism** (Leis et al., SIGMOD 2014): the mature form of
  n1k1's `Stage`/actor batching for scaling across cores.
- **Arrow-native kernel libraries** — Arrow Acero/Compute (arrow-go has a `compute`
  package), DataFusion, Velox, Polars: build-vs-borrow per op.

The two to internalize most: **late materialization + operate-on-encoded-data** (the
real answer to "stop transposing") and **LegoBase-style source-transform generation**
(the home of the lifter).

-------------------------------------------------------
## Appendix D — reference: encodings & standing tensions

**Column encodings**, cheap-to-adopt → fast-to-compute: (1) JSON-array text —
lowest friction, but still parses (1.3× only); (2) **fixed-width native** (LE-packed
`int64`/`float64`) — the SIMD-friendly one, needs known types (this is what Arrow
buffers give us); (3) offset/length + payload (Arrow string/binary — borrow-friendly);
(4) dictionary codes (low-cardinality strings → integer GROUP BY/joins); (5) validity
+ selection bitmaps (orthogonal; needed for nulls and vectorized filter).

**Standing tensions** (still live):
- **Schemaless JSON is the point of SQL++** — heterogeneous/missing/nested values
  have no fixed width or single type. Fixed-width is opt-in-when-proven; the JSON
  path is the always-available fallback. (This is why the workload-fit question,
  Step 1, gates everything: columnar wins on flat/typed/large-scan data, the
  *opposite* of selective/nested/point JSON — so the plan aims at the Parquet-files
  segment specifically.)
- **MISSING ≠ NULL ≠ value** is three-state, unlike Arrow's two-state validity — any
  bitmap scheme must preserve the distinction.
- **Garbage-avoidance cuts both ways** — column batches are *more* recyclable (one
  buffer/column), but `stage.go` deep-copies rows on hand-off while a columnar batch
  wants to *borrow* Arrow buffers; the borrow/lifetime contract must be explicit
  (cf. the `-race` history).
- **Batch width** — row-at-a-time favors latency, vectorized favors throughput;
  reuse `stage.go`'s `batchSize`/`batchChSize` knobs, sized to a SIMD-lane multiple.

-------------------------------------------------------
## One-line summary

`base.Val` is axis-agnostic, so a `Val` can hold a column; n1k1's compiler +
batching machinery are unusually suited to a vectorized lane. Measured (arm64, no
SIMD): fixed-width beats row-JSON 40–730×, from *not-parsing* + *one-vertical-stripe*,
not SIMD. Steps 1–4 shipped the columnar **source** (Parquet, projection pushdown
reusing cbq's `EarlyProjection`, zero-alloc transpose). Step 5 is vectorized
execution — kept bounded (not whack-a-mole) by a general columnarizable predicate +
generics + pointwise lifting, with the untyped tail falling back to the row engine
and **codegen from the lz source as the north-star.**
