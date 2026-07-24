# n1k1 — columnar & SIMD design notes

## Status & remaining TODOs

_Last reviewed: 2026-07-23._

**Done:** Steps 1–5 have landed — the workload characterization, the ceiling spike
(40–730× measured), the Parquet source, projection pushdown, and the first vectorized
op (ungrouped `SUM`/`AVG`/`COUNT`/`MIN`/`MAX` aggregation over Parquet columns, fused
with a selection-vector `WHERE` and `+`/`-`/`*` arithmetic operands, with null-masked
kernels and zero-scan footer-stat shortcuts). What remains is Step 6: dictionary
GROUP BY, more/vectorized kernels, and the optional SIMD leaf.

Step roadmap:
- [x] Step 1 — Characterize the workload (Parquet/Iceberg local-file scan niche).
- [x] Step 2 — Spike the ceiling (fixed-width beats row-JSON 40–730×, no SIMD).
- [x] Step 3 — Parquet source, transpose-to-rows (`records/parquet.go`, `!js`-guarded).
- [x] Step 4 — Projection pushdown via `ColumnsProjector`/`ColumnsSource` sidecars,
  reusing cbq's `plan.Fetch.EarlyProjection()`.
- [x] Step 5 — Vectorized aggregation, no transpose (`agg-columnar` + zero-scan
  `agg-metadata` ops; masked nulls; 5.4 selection-vector `WHERE`; 5.5 arithmetic operands).
- [ ] Step 6 — Expand on measured wins: dictionary GROUP BY, vectorized arithmetic
  kernels beyond agg operands, more aggregates, index-list selection, and the optional
  amd64-only SIMD leaf.

**Remaining (headline TODOs):**
- [ ] Dictionary GROUP BY over low-cardinality string columns (integer key codes).
- [~] String / dictionary column encoding layout — string *scalar* (doc-id) columns
      now flow the columnar fast path (Arrow STRING borrowed via `Value`, rendered as
      a JSON string); dictionary-code encoding for GROUP BY is still open.
- [ ] Division / richer aggregate-operand expressions (nested, unary, n-ary).
- [ ] Index-list selection vectors for very low selectivity.
- [ ] Row/predicate pushdown at the Arrow level (`RowGroupPruner` sidecar, cf. cbq's
      `iceberg_row_filter.go`) and operate-on-encoded-data (SUM over RLE, dict predicates).
- [ ] The codegen north-star: project a column-batch target from the same lz source
      (pointwise-lifting pass), gated on type inference (`TODO.md:250`).
- [ ] Optional SIMD leaf on the columnar batch (amd64-only, mandatory scalar fallback).

## Overview

Design record for **columnar (vectorized) execution** and **SIMD** in n1k1, a
six-step plan. Key idea: `base.Val` is `[]byte` and axis-agnostic — a `Val` can
hold a *packed column*. n1k1 is row-at-a-time today but is a **query compiler**
(`intermed_build` projects the lz interpreter into specialized Go) — the leverage
for a vectorized lane. Measured (arm64, no SIMD), fixed-width columnar beats
row-JSON **40–730×**; the win is *not-parsing* + *touching one column stripe*,
**not** SIMD. Plan is source-first, evidence-gated, SIMD a last optional leaf; the
schemaless-JSON row path is always the correctness fallback.

See also: `DESIGN.md`, `DESIGN-data.md` (§ Parquet/Arrow), `DESIGN-exprs.md`,
`DESIGN-stats.md`.

## Contents

- [The core idea](#the-core-idea)
- [The plan: Steps 1–6](#the-plan-steps-16)
- [Step 5 — vectorized aggregation (DONE)](#step-5--vectorized-aggregation)
- [Null handling & footer-stat shortcuts](#null-handling--footer-stat-shortcuts)
- [Step 5.4 — selection-vector `WHERE` (DONE)](#step-54--selection-vector-where-done)
- [Step 5.5 — arithmetic-expression operands (DONE)](#step-55--arithmetic-expression-operands-done)
- [Key design decisions (settled)](#key-design-decisions-settled)
- [Still-open questions](#still-open-questions)
- [Appendix A — measured results](#appendix-a--measured-results)
- [Appendix B — SIMD reality](#appendix-b--simd-reality)
- [Appendix C — prior art & inspiration](#appendix-c--prior-art--inspiration)
- [Appendix D — reference: encodings & standing tensions](#appendix-d--reference-encodings--standing-tensions)

-------------------------------------------------------
## The core idea

- **Today: strictly row-at-a-time.** `type Val []byte` (JSON), `type Vals []Val`
  (one row; slots aligned with `Labels`), pushed via `YieldVals(Vals)`. Exprs
  per-row (`ExprFunc(vals) Val`); aggregates fold row-by-row (`base/agg.go`); the
  only batches (`base/stage.go` `[]Vals`) are row-major copies for concurrency. No
  vectorized kernel exists.

- **Transpose the axes.** A *column batch* reuses the same `[][]byte` shape read
  column-wise — each slot is a packed vector of M values:

  ```
  row batch:  Vals = [ "alice", 42, {"x":1} ]              <- ONE row, 3 cols
  col batch:  Vals = [ ["alice","bob"], [42,43], [...] ]   <- MANY rows
  ```
  Container, push plumbing, recycling, and Labels alignment survive; row count M is
  the new hidden dimension.

- **The compiler provides leverage.** `intermed_build` projects lz into specialized
  Go (Futamura/LMS). The type inference `TODO.md:250` wants (`sales < 1000` is
  numeric) is the precondition for fixed-width encoding — columnar and static-typing
  are one project. Precedent: `engine/expr.go:ExprLabelUint64` already reads a slot
  as a packed LE `uint64`.

- **The tension: schemaless JSON.** A JSON column is variable-width, untyped,
  three-state (`MISSING` ≠ `NULL` ≠ value; `base/base.go`).

-------------------------------------------------------
## The plan: Steps 1–6

Evidence-gated, source-first, kill-early: prove the ceiling and the workload fit
before building; let columnar bytes enter from a columnar **source**, not
synthesized from rows. The row engine (push-based, compilable, garbage-free) is the
baseline to beat.

- **Step 1 — Characterize the workload. ✅** Target: local dirs of mixed files incl.
  Parquet/Iceberg (a DuckDB-style "query files in place" niche) — a real
  analytical-scan segment.

- **Step 2 — Spike the ceiling. ✅** arm64, pure Go, no SIMD: fixed-width SUM/filter
  beats row-JSON **40× (narrow) to 730× (50-field docs)**, at the native-`[]float64`
  ceiling. ≥3–5× gate cleared by 10–150×. (`test/col_test.go`; Appendix A.)

- **Step 3 — Ship a Parquet source (transpose-to-rows). ✅** `records/parquet.go`
  (arrow-go) decodes Parquet → JSON rows, wired into `records.OpenFile`
  (`TestParquetQueryEndToEnd`; `examples/warehouse/`). `!js`-guarded so arrow-go stays
  out of wasm. Correctness feature, no engine change.

- **Step 4 — Projection pushdown via sidecar interfaces. ✅** Optional
  `records.ColumnsProjector` / `ColumnsSource` (+ `ColumnMeta`) type-asserted by the
  scan (the `SubPathser` idiom); core `Source` stays `{Next, Close}`, non-implementers
  fall back to full transpose. Wanted-column set **reused from cbq's planner**
  (`plan.Fetch.EarlyProjection()`, via `expr.FieldNames` + `expression.IsCovered`).
  `walkSource` forwards the projection per file. Transpose made zero-alloc
  (`appendRecordsNDJSON`: **526K→2.1K allocs/op, 2.9× faster**, replacing
  `array.RecordToJSON` boxing).

- **Step 5 — First vectorized op: aggregation, no transpose. ✅** A fused
  Parquet-scan→agg lane over a typed column, reusing `AggSum`'s accumulator via
  `AggCatalog["sum_v_float64"]`, feeds the borrowed Arrow buffer as a `base.Val`.
  **Aggregation is first because its output is one row** — it rejoins the row engine
  transpose-free. Ungrouped `SUM`/`AVG`/`COUNT`/`MIN`/`MAX` land, with masked nulls, a
  zero-scan `agg-metadata` footer-stat path, selection-vector `WHERE` (5.4), and
  `+`/`-`/`*` arithmetic operands (5.5). GROUP BY (dict keys) and the codegen north-star
  remain. See § Step 5.

- **Step 6 — Expand on measured wins. ◀ NEXT.** More aggregates, vectorized arithmetic kernels,
  selection-vector filter, dictionary GROUP BY — each gated on a benchmark beating
  row-at-a-time. **SIMD lives here: last and optional** (amd64-only accelerator,
  mandatory scalar-Go path; batching alone carries the arm64/WASM win). Appendix B.

-------------------------------------------------------
## Step 5 — vectorized aggregation

Borrowed Arrow column → vectorized aggregation, no transpose. Landed in small
increments: **5.1** `SUM(x)` fused scan→agg (null_count=0) → **5.2** multi-agg
`SUM(x),SUM(y)` (N-tuple over one scan pass) → **5.3** `SUM(x+y)` (vectorized arithmetic
kernel) → **5.4** chained ops + type-vector + selection vectors → **5.5** arithmetic
operands. GROUP BY (dict keys) and the codegen north-star remain. **Prereqs met:**
`walkSource.Columns()` (multi-file schema) and a `ColumnBatchSource` on `parquetSource`
yielding borrowed Arrow columns.

### The vectorize decision (`sum` → `sum_v_float64`)

Two inputs: (1) *plan shape* — ungrouped, agg = SUM/etc. of a **bare field**, single
Parquet-capable keyspace; (2) *column type + `null_count`* from `ColumnsSource`
(Parquet footer — the plan is schemaless, only the footer knows `x`'s type). Lives in a
**post-conv rewrite pass** (like `addColumnProjections`): gate on plan-shape; consult
`ColumnsSource`; if the column is a supported fixed-width type with `null_count==0`,
swap the `group` op's `aggCalcs[i][0]` from `"sum"` to `"sum_v_"+kernelType` and mark
the columnar feed; else leave the row path.

### Reuse `AggSum`, don't duplicate

SUM's state is one float64 (8 bytes); `Result` formats it. So `sum_v_float64` =
`&Agg{Init: AggSum.Init, Result: AggSum.Result, Update: <vectorized fold>}` — type in
the **catalog key**, `Init`/`Result` reused verbatim ⇒ byte-identical output ⇒
differential test is exact string equality. Not a widened `Agg` interface. MIN/MAX
reuse their accumulators, AVG the 16-byte sum+count, COUNT the counter.

### Zero-copy from Arrow

`array.Float64.Float64Values()` is an unsafe reinterpret (no parse/copy);
`arr.Data().Buffers()[1].Bytes()` is that packed LE buffer — **already a `base.Val`**,
borrowed. Flows through `Update(val Val, …)` with zero re-encode; `sum_v_float64.Update`
reinterprets via `binary.LittleEndian` + `math.Float64frombits` (keeping `base`
arrow-free), summing **in scan order** ⇒ bit-exact vs the row fold. `int64` →
`float64(v)`, matching the row path's `ParseFloat64`. Borrow valid until
`batch.Release()`; Update-then-Release, one call per batch.

### Shape carried by `ColumnMeta` + a typed catalog key, no label sigils

The type does NOT ride a `@col.f64:` label prefix (which would force every `IndexOf`
to strip it). It also did not need the once-envisioned parallel `[]ColKind` slice
aligned with `Labels`: the fused `agg-columnar` / `agg-metadata` op is a SINGLE op
(the group + its Parquet/Iceberg source collapse into one), so no column batch flows
*between* ops and there is no inter-op shape to carry. Instead the post-conv rewrite
(`glue/columnar.go`) consults the source's `records.ColumnMeta` (`Columns()`) at plan
time and bakes the physical type into the agg **catalog key** — `sum` over a float64
column becomes `sum_v_float64` (`aggCatalogKeyForColumnar` / `parseAggOperandSpec`).
A future multi-op columnar lane (Step 6) that DID pass batches between ops would
revisit an explicit `[]ColKind`-style shape; today it is unbuilt, not settled.

### Traps to pre-empt

- `COUNT(col)` ≠ `COUNT(*)` on nulls (null_count=0 sidesteps v1).
- Multi-file partial-aggregate combine (Σ/min/max associative; AVG carries its count).
- Fused path bypasses `Stage`/stats/`YieldStats` — preserve scan stats, `LIMIT`,
  cancellation.

### Does Step 5 scale, or is it whack-a-mole?

Naively it is (a detector per shape × a kernel per operation×type). Three levers plus
a universal fallback keep it bounded:

1. **A general "columnarizable?" predicate**, not per-shape matching — a recursive
   bottom-up question over the op/expr tree; query shapes fall out of *composition*.
   One inference pass, not N cases.
2. **Generics kill the type combinatorics** — `sumV[T Numeric]`, one kernel per
   operation, compiler instantiates per type (Go 1.25).
3. **Pointwise lifting** — a typed scalar `f(a,b)` becomes `for i { out[i]=f(a[i],b[i]) }`
   mechanically, vectorizing the whole pointwise surface with no per-function work.

**Honest boundary:** reductions (~5) and reshaping relational ops (filter+selection,
group-by, join, sort — ~a dozen) are hand-authored once; the untyped/string/date long
tail **defers to the row engine**. Coverage = pointwise + fixed reductions + fixed
relational ops + everything-else→row-engine.

### The codegen way out (north-star)

Teach `intermed_build` to project a **column-batch target** from the *same* lz source
— the generated program loops over column batches (inner element loop, exprs inlined)
instead of rows. Write each kernel once (scalar, lz); the compiler emits *both* lanes,
chosen per query by type inference. **Pointwise lifting is its own LegoBase/LMS
source-transform pass** (`// <== pointwise`) whose lz output feeds *both* the
interpreter (kernels differential-tested vector-vs-scalar *before* the compiler) and
`intermed_build`. Prerequisite: type inference (`TODO.md:250`). Lineage: Appendix C.

-------------------------------------------------------
## Null handling & footer-stat shortcuts

v1 gates on `null_count == 0`. Lifting that draws on footer stats the rewrite already
reads (`ColumnMeta.{Min,Max,NullCount}` + file row count). Menu, fastest first:

- **Aggregates from stats — *zero scan*.** `COUNT(*)` = `num_rows`; `COUNT(x)` =
  `num_rows − null_count` (any null_count → supersedes `count_v`, reads no data pages);
  `MIN`/`MAX` = footer min/max. Multi-file combines associatively. An `agg-metadata`
  op the rewrite emits when every agg is COUNT/MIN/MAX (mixed with SUM/AVG → scan or
  hybrid). Caveats: stats may be absent → fall back; **float MIN/MAX has a
  NaN/signed-zero subtlety** (Parquet excludes NaN by convention, matching our
  NaN→null, but writer-dependent) → COUNT and integer MIN/MAX safe first, float gated.
- **Sentinel-for-null (materialization-time), for SUM/AVG.** When we materialize our
  *own* column, fill null positions with the reduction identity/out-of-range sentinel —
  SUM→0, MIN→(> max), MAX→(< min), sentinel from footer min/max (shortcuts compose). Hot
  loop stays branch-free; null handling moves to a one-time bitmap pass.
- **Masked kernel (Arrow validity bitmap) — general case. DONE.** `Data().Buffers()[0]`
  is the validity bitmap (1 bit/elem, 1=valid), separate from values `Buffers()[1]`.
  The masked kernel reads both (borrowed, zero-copy) and skips null lanes — `for i {
  if valid(i) { s += v[i] } }`. Necessary because Arrow leaves null slots undefined.
  Bit-exact vs the row engine. **Key semantic:** `COUNT(x)` and `AVG`'s denominator
  count only **non-NULL, non-MISSING** values (matching cbq and the scalar
  `AggCount`), so COUNT, SUM, and the AVG count/sum all fold over
  **selection∧validity** — a null lane is skipped by every kernel. (`COUNT(*)` is a
  separate op: it counts every selected row via a constant-true operand, not this
  column path.) The masked kernels still take the selection and selection∧validity
  masks separately, but for `COUNT(x)`/`AVG(x)` the count uses selection∧validity.

### How validity threads to the kernel

`NextColumns` returns validity as a parallel `[][]byte` (nil when `null_count==0`;
byte-aligned Arrow buffer borrowed zero-copy; a rare unaligned offset normalized once).
When present the executor ANDs it into the selection and calls a **masked reducer**
(`base.MaskedSum*`/`MaskedCount`/`MaskedAvg*`) writing `AggSum`/`AggAvg`'s accumulator,
*instead of* `Agg.Update` (left untouched — masking lives outside it). The non-null,
no-WHERE lane takes plain `Agg.Update`.

**Alternatives rejected:** *sentinel-fill* (AVG's count still needs the validity
popcount); a *companion validity slot* threaded via a generic inter-op column shape
(the once-envisioned `[]ColKind`; unbuilt — the single-op fused lane needs no such
plumbing).

-------------------------------------------------------
## Step 5.4 — selection-vector `WHERE` (DONE)

A `WHERE` used to force the row path. Predicated aggregation introduces the
**selection vector** — the primitive the vectorized model leans on. Now landed: a flat
AND/OR of numeric field-vs-constant comparisons fuses into the agg-columnar lane (each
clause → bitmap, combined byte-wise; nullable predicate columns via three-valued logic
— a null clause row is 0, the right identity for AND and OR). Anything else (nested
boolean, field-vs-field, non-numeric) takes the row path.

### Mechanism

- **Selection = dense bitmap** (1 bit/row, LSB-first) — *same layout as Arrow's
  validity bitmap*, so a null lane and an unselected lane combine by byte-wise `AND`
  (`effective = predicate AND validity`), and one **masked-reduce** kernel serves both
  null-masking and `WHERE`. (Index-list selection, better at low selectivity, is a
  later TODO.)
- **Predicate → selection:** a vectorized compare kernel (`gt_v`/`lt_v`/`eq_v`/… over a
  borrowed column vs constant) emits the bitmap; `AND`/`OR` are byte-wise. Null lanes
  aren't selected (`null > k` isn't true) — AND the column's validity.
- **Fused scan→filter→agg:** rewrite extends `group→scan` to `group(vectorizable aggs)
  → filter(vectorizable predicate) → records-scan`, projecting predicate ∪ agg columns;
  per batch evaluate predicate → mask, fold aggs (masked reduce) over the mask. Gate:
  bare-column-vs-constant combined with AND/OR; else row path. Bit-exact.

### Bitmap library — roll our own

A dense `[]byte` bitmap (Arrow-validity-compatible LSB-first) + helpers over
`math/bits`: zero-dep, zero-alloc, wasm-safe, right shape for a small per-batch
selection. *Not* roaring (built for large/sparse/persistent posting lists; overhead
wasted on a dense ≤batch selection; build-tag-guarded out of wasm).
`bits-and-blooms/bitset` is an unnecessary dep for ~20 lines; and ours must be
byte-compatible with Arrow's validity anyway.

### Build order (all landed)

- **5.4a** dense bitmap + masked reduce kernels (`base/agg_masked.go`).
- **5.4b** compare kernels (`base/filter.go`: `FilterFloat64/Int64`,
  `AndBitmap`/`OrBitmap`).
- **5.4c** fused scan→filter→agg op + rewrite (`colPredicateExtract` pulls (field, op,
  const) from the cbq filter; cbq normalizes `>`/`>=` to `LT`/`LE` with swapped
  operands, so only `LT`/`LE`/`Eq` matched, reading operand order for direction).
  Differential-tested: 9 WHERE variants fire the fused lane bit-exact; 4 bail cases stay
  on the row path.
- **5.4d** flat AND/OR: `colPredicateExtract` recursively flattens a same-mode boolean
  tree (`And(And(a,b),c)`) into clauses; per batch each clause → bitmap (`Filter*`, then
  AND its validity), combined with `AndBitmap`/`OrBitmap`. Bails on nested mixed
  boolean, field-vs-field, non-numeric.

-------------------------------------------------------
## Step 5.5 — arithmetic-expression operands (DONE)

`SUM(price * qty)`, `SUM(price * 1.08)`, `AVG(a + b)` — the canonical analytics shape.
An aggregate operand is now a bare column *or* a binary `+`/`-`/`*` of two numeric
column/constant terms. `parseAggOperandSpec` recognizes cbq's `Add`/`Mult`
(commutative, 2 operands) and `Sub`; per batch the executor materializes into a reused
float64 scratch column (`base.ArithFloat64` / `ScaleFloat64`; int64 term widened via
`LoadFloat64FromInt64`), then the SUM/AVG masked reducers fold it. All float64
(matching the row engine's JSON-number arithmetic → bit-exact); the materialized
column's validity is the **AND of the term columns' validities**. Bails: `/`
(x/0→NULL), unary `-`, >2 operands, nested arithmetic, non-numeric. Composes with WHERE
and EXPLAIN (shows `agg-columnar`).

**Deferred:** division / richer expressions (nested, unary, n-ary); index-list
selection for very low selectivity; non-fixed-width (string/decimal) columns; the
long-term validity-as-companion-slot generic row-plumbing.

-------------------------------------------------------
## Key design decisions (settled)

- **Columnar source = optional sidecar interfaces**, not a widened `Source` (the
  `SubPathser` idiom): `ColumnsProjector{ ProjectColumns([]string) error }` and
  `ColumnsSource{ Columns() []ColumnMeta }`; non-implementers fall back. Field set from
  cbq's `EarlyProjection()`.
- **Column encoding = the Arrow value buffer itself** — its raw `[]byte` *is* the
  packed fixed-width column (`base.Val`, zero-copy), no re-encode. JSON-array encoding
  skipped (1.3× only, Appendix A). Strings (offsets+payload) and dictionary codes
  (GROUP BY) come later.
- **Shape = parallel `[]ColKind`** (StatsBase-style), not label sigils.
- **`null_count == 0` fast path first** — unmasked kernel; nulls/selection bitmaps come
  with the relational ops.
- **Reuse existing accumulators** (`AggSum` etc.) via typed catalog keys; don't widen
  `Agg`.
- **Differential testing from the start** — row lane is the oracle; scalar-Go kernels
  sum in scan order ⇒ *exact* equality (SIMD would force epsilon compares — another
  reason it's last).
- **Reuse cbq's plan analysis, don't hand-roll** — the Step-4 lesson (EarlyProjection)
  reapplied to Step 5's vectorizability detection.
- **EXPLAIN shows the rewrite.** The columnar rewrite is a post-plan pass on n1k1's op
  tree (invisible to cbq's `EXPLAIN` JSON). `convForDisplay` (the EXPLAIN/`-v` path)
  runs the same `maybeColumnarOptimize` the executor does, so `EXPLAIN SELECT SUM(x)`
  shows an `agg-columnar`/`agg-metadata` node and honors `DisableColumnarOptimize`. The
  op-tree renderer (`FormatConvPlan`) is generic (prints each op's `Kind` + `Labels`),
  so future columnar op-kinds surface with no per-kind code.

-------------------------------------------------------
## Still-open questions

- **Dictionary encoding layout (for GROUP BY).** Numeric settled (Arrow's LE buffer);
  string *scalar* columns already borrow Arrow STRING via `Value` (shipped). Still
  open: low-cardinality dictionary-code encoding for a vectorized GROUP BY — Arrow
  dictionary codes vs n1k1-native? Lean Arrow-compatible (near-free interop).
- **How much columnar to ship to WASM?** No `archsimd`/`simdjson`/asm there; the
  in-browser win is *batching* (fewer JS-boundary crossings), not vector width.
- **Operate on encoded data?** Computing on Parquet's RLE/dict pages *without*
  decoding (Appendix C) — the real "stop transposing," beyond Step 5.
- **Predicate/row pushdown.** cbq's `iceberg_row_filter.go` skips rows at the Arrow
  level before transposing; n1k1 filters after. A future `RowGroupPruner`/predicate
  sidecar pairing with 5.4 selection vectors.

-------------------------------------------------------
## Appendix A — measured results

### Step 2 spike (arm64, pure Go, NO SIMD) — the ceiling

`test/col_test.go`. SUM/filter over N float64; row path faithful to n1k1 (whole JSON
doc, `jsonparser.GetFloat` per row). All paths zero-alloc.

**SUM, ns/value, narrow 1-field doc:**

| N | row-JSON | JSON-array (enc 1) | fixed-width (enc 2) | native `[]float64` |
|---|---|---|---|---|
| 64  | 36.8 | 28.1 | 0.68 | 0.44 |
| 1K  | 38.8 | 30.2 | 0.87 | 0.83 |
| 64K | 38.5 | 29.8 | 0.88 | 0.87 |
| 1M  | 39.8 | 30.0 | 0.91 | 0.87 |

Fixed-width ~44× the row path, at the native ceiling (LE decode nearly free); row cost
is JSON number parsing. **No tipping point in N** — fixed-width wins from N=64.
JSON-array barely helps → prioritize fixed-width.

**Doc-width sweep (N=1M) — where the "vertical stripe" shines:**

| doc width | row-JSON ns/value | fixed-width | speedup |
|---|---|---|---|
| 1  | 38.4 | ~0.9 | 42× |
| 5  | 83.6 | ~0.9 | 93× |
| 20 | 294  | ~0.9 | 327× |
| 50 | 660  | ~0.9 | 730× |

Row path scales linearly with doc width; fixed-width is constant. The win grows with
what hurts row-at-a-time: **wide records, few projected fields.** (Filter-count
`price>500`, N=1M, narrow: row 39.5 vs fixed 0.69 → 57×.)

**Economic break-even, JSON source** (build the column ~38 ns/value once, then ~0.9/op):
row = 38·K, columnar = 38 + 0.9·K → columnar wins once **K > ~1**. From a **Parquet
source (no parse to build), it wins unconditionally.**

### Step 3/4 Parquet prototype (arrow-go, arm64)

`test/parquet_test.go`. Wide file `{id, price, f0..fN}`, query wants one column.

**Projection pushdown** — read only `price` vs all 14 columns: 0.6 ms / ~0.02 MB vs
50 ms / ~10.3 MB → **0.2% of the bytes, ~80× (137× at 1M rows)**, done by the format.

**Free footer metadata** (no data pages): `price type=DOUBLE null_count=0 min=0.5
max=999.5` → type picks kernel; `null_count=0` ⇒ no validity bitmap; min/max are
zone-map inputs.

**Parse-free column SUM** (`test/parquet_test.go`):

| SUM path | ns/value | vs row-JSON | allocs |
|---|---|---|---|
| Arrow column, kernel only | 0.93 | ~56× | 0 |
| full open+project+decode+sum | 3.0 | ~17× | ~2800 / 18 MB |
| row-JSON baseline | 52 | 1× | 0 |

Kernel hits the fixed-width ceiling with no parse to build the column. Full-path allocs
are Arrow's per-batch decode; the zero-copy end-to-end path (0.93, not 3.0) is Step 5.

### Step 4's zero-alloc transpose

`appendRecordsNDJSON` (type-switch per Arrow column into a reused buffer; RFC-8259
escaping; NaN/Inf→null; zero-copy `String.Value`) replaced `array.RecordToJSON` (boxed
each value to `interface{}` + `encoding/json`, ~8 allocs/row). Measured (65K rows, 6
cols): **526K → 2.1K allocs/op (~248×), 2.9× faster**. `fastRenderable` gates it, with
a `RecordToJSON` fallback for exotic types (timestamp/decimal/list/struct). Proven by
`TestParquetFastTransposeEquivalence`.

-------------------------------------------------------
## Appendix B — SIMD reality

**Go SIMD is toolchain-limited.** No compiler autovectorization (fast stdlib bits are
hand-written Plan9 asm). Four routes: (1) raw `.s` asm per-arch; (2) `avo`
(asm-generator; klauspost/minio); (3) cgo (defeats the point); (4) `GOEXPERIMENT=simd`
→ `simd/archsimd` (Go 1.26) — but **amd64-only, no ARM/NEON, no WASM, unstable API**.
Portably reachable today: fixed-width int/float compare, arithmetic, min/max/sum,
bitmap AND/OR/POPCOUNT — exactly what columnar encodings produce.

**Batch first, then SIMD.** golang/go#77647 documents the Go↔asm call-boundary cost; on
small chunks it eats the SIMD win, so per-`Val` SIMD is a guaranteed loss — SIMD only
pays amortized across a batch. With the Step-2 finding, SIMD is a **leaf on the columnar
batch**, mandatory scalar-Go fallback, amd64-only.

**The tail/remainder.** A column of N rarely divides by lane count L (2–8 lanes).
Handled by: scalar remainder loop (default); masked ops (AVX-512/SVE); padding to a lane
multiple with identity; or an overlapping last block (idempotent ops only). n1k1 gets
these free: the validity/selection bitmap *is* the mask; the scalar fallback *is* the
remainder loop.

**SIMD-json (`minio/simdjson-go`)** is a *different* use — accelerating JSON *parsing*
(SIMD scan → a "tape"), not compute. But **AVX2+CLMUL, no fallback, amd64-only, no
WASM/arm64**, and produces a whole-document tape rather than jsonparser's lazy zero-copy
sub-slices — so it fights n1k1's hot row path. Fits only as an *ingest* front-end for
the columnar path (parse once, scatter to column buffers), gated on `SupportedCPU()`.
SIMD-parse and SIMD-compute are separate bets.

-------------------------------------------------------
## Appendix C — prior art & inspiration

Techniques worth stealing, with the n1k1 tie-in:

- **DuckDB** — closest reference: embedded, vectorized push-based, reads Parquet
  directly, selection vectors + dictionary + late materialization + morsel parallelism.
- **Compiled vs vectorized — "both"** (Kersten/Leis, VLDB 2018): neither dominates;
  n1k1 is placed to be a **hybrid** (interpreter + Futamura compiler), with
  pointwise-lifting the bridge. HyPer/Umbra = compiled; MonetDB/X100 → VectorWise =
  vectorized.
- **LegoBase / DBLAB + LMS** (Klonatos & Koch, VLDB 2014; Rompf/Odersky): build the
  engine high-level, optimize as **source-to-source transforms** — the home of lifting
  and of `intermed_build`. Lineage: LegoBase → DBLAB/SC (2016) → LB2 (2018) & **Flare**
  (OSDI 2018). The LMS-*Scala* engines stayed academic; the *idea* shipped via other
  codegen — **HyPer/Umbra** (LLVM), **Spark Tungsten** (JVM bytecode), **Hekaton**
  (C→DLL), Impala (LLVM). Cousin: **GraalVM/Truffle** automatically does the first
  Futamura projection — the automatic `intermed_build`. LB2/Flare matched hand-tuned
  engines, so it works.
- **Late materialization** (C-Store/Vertica; Abadi, ICDE 2007): carry column
  positions/IDs, materialize last or never (aggregates) — beyond Step 5.
- **Operate on compressed/encoded data** (Abadi, SIGMOD 2006): SUM over RLE =
  value×runlength; GROUP BY on dict codes; predicates on bit-packed data — *without
  decoding*. Parquet pages are RLE/dict/bit-packed and we fully decode via Arrow;
  computing on encoded pages is the real "stop transposing."
- **Micro-adaptivity** (Răducanu/Boncz/Zukowski, SIGMOD 2013): several kernel flavors,
  profile per batch, pick fastest — runtime extension of our compile-time dispatch.
- **Morsel-driven parallelism** (Leis, SIGMOD 2014): mature form of n1k1's `Stage`/actor
  batching.
- **Arrow-native kernel libraries** — Arrow Acero/Compute, DataFusion, Velox, Polars:
  build-vs-borrow per op.

The two to internalize: **late materialization + operate-on-encoded-data** and
**LegoBase-style source-transform generation**.

-------------------------------------------------------
## Appendix D — reference: encodings & standing tensions

**Column encodings**, cheap-to-adopt → fast-to-compute:
1. JSON-array text — lowest friction, still parses (1.3× only).
2. **Fixed-width native** (LE-packed `int64`/`float64`) — SIMD-friendly, needs known
   types (what Arrow buffers give us).
3. Offset/length + payload (Arrow string/binary — borrow-friendly).
4. Dictionary codes (low-cardinality strings → integer GROUP BY/joins).
5. Validity + selection bitmaps (orthogonal; nulls and vectorized filter).

**Standing tensions** (still live):
- **Schemaless JSON is the point of SQL++** — heterogeneous/missing/nested values have
  no fixed width or type; why Step 1's workload-fit gates everything (columnar wins on
  flat/typed/large-scan data, the *opposite* of selective/nested/point JSON → aim at
  the Parquet-files segment).
- **MISSING ≠ NULL ≠ value** is three-state, unlike Arrow's two-state validity — any
  bitmap scheme must preserve the distinction.
- **Garbage-avoidance cuts both ways** — column batches are *more* recyclable, but
  `stage.go` deep-copies rows on hand-off while a columnar batch wants to *borrow* Arrow
  buffers; the borrow/lifetime contract must be explicit (cf. `-race` history).
- **Batch width** — row-at-a-time favors latency, vectorized throughput; reuse
  `stage.go`'s `batchSize`/`batchChSize`, sized to a SIMD-lane multiple.
