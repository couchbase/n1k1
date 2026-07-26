# n1k1 — columnar & SIMD design notes

## Status & remaining TODOs

_Last reviewed: 2026-07-23._

Columnar (vectorized) execution over Parquet/Iceberg columns. Steps 1–5 have
landed; Step 6 is the frontier.

- [x] **1** — Characterize the workload: local dirs of Parquet/Iceberg files
  (a DuckDB-style "query files in place" niche).
- [x] **2** — Spike the ceiling: fixed-width columnar beats row-JSON **40–730×**
  (arm64, no SIMD; `test/col_test.go`, Appendix A).
- [x] **3** — Parquet source, transpose-to-rows (`records/parquet.go`, `!js`-guarded).
- [x] **4** — Projection pushdown via `ColumnsProjector`/`ColumnsSource` sidecars,
  reusing cbq's `plan.Fetch.EarlyProjection()`.
- [x] **5** — Vectorized ungrouped aggregation, no transpose (`agg-columnar` +
  zero-scan `agg-metadata` ops): `SUM`/`AVG`/`COUNT`/`MIN`/`MAX`, null-masked, fused
  with a selection-vector `WHERE` (5.4) and `+`/`-`/`*` operand arithmetic (5.5),
  over Parquet **and** Iceberg columnar sources.
- [ ] **6** — Dictionary GROUP BY (low-cardinality string → integer key codes);
  richer aggregate operands (division, unary, nested, n-ary); more/vectorized
  kernels; index-list selection for low selectivity; Arrow-level row/predicate
  pushdown (`RowGroupPruner` sidecar, cf. cbq's `iceberg_row_filter.go`) and
  operate-on-encoded-data (SUM over RLE, dict predicates); the codegen north-star
  (project a column-batch lane from the same lz source, gated on type inference,
  `TODO.md:250`); the optional amd64-only SIMD leaf.

String *scalar* (doc-id) columns already flow the columnar path (Arrow STRING
borrowed via `Value`); dictionary-code encoding for GROUP BY is what's still open.

## The core idea

`type Val []byte` is axis-agnostic — a `Val` can hold one JSON value OR a *packed
column* of M values. The engine is row-at-a-time today (`Vals` = one row, slots
aligned with `Labels`, pushed via `YieldVals`), but transposing the axis reuses the
same `[][]byte` container, push plumbing, recycling, and `Labels` alignment — row
count M becomes a hidden dimension:

```
row batch:  Vals = [ "alice", 42, {"x":1} ]            <- ONE row, 3 cols
col batch:  Vals = [ ["alice","bob"], [42,43], [...] ] <- MANY rows
```

Two enablers: (1) n1k1 is a **query compiler** (`intermed_build` projects the lz
interpreter into specialized Go), so a vectorized lane can be a codegen target; and
(2) the win is *not-parsing* + *touching one column stripe*, **not** SIMD — so it
pays on arm64/WASM with no vector instructions. The standing tension is that
schemaless JSON is variable-width, untyped, and three-state (`MISSING` ≠ `NULL` ≠
value), so columnar bytes must enter from a typed **source** (Parquet/Iceberg), and
the row path is always the correctness fallback for the untyped/nested long tail.

See also `DESIGN-data.md` (§ Parquet/Arrow), `DESIGN-exprs.md`, `DESIGN-stats.md`.

## The shipped columnar lane

A **post-conv rewrite pass** (`glue/columnar.go` `maybeColumnarOptimize`) rewrites a
qualifying `group` op in place; nothing else in the engine changes. It fires when
the plan shape is ungrouped aggregation of a supported operand over a single
Parquet/Iceberg keyspace, and the source's `ColumnMeta` (Parquet footer — the plan
itself is schemaless) reports a supported fixed-width type. Otherwise the row path
runs unchanged. `EXPLAIN`/`-v` runs the same pass (`convForDisplay`), so it shows an
`agg-columnar`/`agg-metadata` node and honors `DisableColumnarOptimize`.

⚠ **The fused op bypasses the normal `Stage` machinery**, so anything extending it
must re-preserve what `Stage` gives for free — scan stats, `LIMIT`, and cancellation.

**Zero-copy from Arrow.** `array.Float64.Float64Values()` reinterprets the packed LE
buffer with no parse/copy; `arr.Data().Buffers()[1].Bytes()` *is* a borrowed
`base.Val`. The kernel reinterprets via `binary.LittleEndian` + `math.Float64frombits`
(keeping `base` arrow-free) and sums **in scan order** ⇒ bit-exact vs the row fold.
`int64`→`float64(v)` matches the row path's `ParseFloat64`. The borrow is valid until
`batch.Release()`: Update-then-Release, one call per batch.

**Reuse accumulators, don't widen `Agg`.** `sum_v_float64` = `AggSum`'s `Init`/`Result`
with a vectorized `Update` — the type rides the **catalog key**
(`aggCatalogKeyForColumnar`/`parseAggOperandSpec`), not a label sigil and not an
inter-op shape slice (the fused op is single-op — group + source collapse into one —
so no column batch flows between ops). `Init`/`Result` reused verbatim ⇒ byte-identical
output ⇒ the differential test is exact string equality against the row oracle.

**Nulls.** v1 required `null_count == 0`; the general case is now a **masked kernel**
(`base/agg_masked.go`). Arrow's validity bitmap (`Buffers()[0]`, 1 bit/elem) is
borrowed alongside the values; the reducer skips null lanes. `COUNT(x)` and `AVG`'s
denominator count only **non-NULL, non-MISSING** values (matching `AggCount`), so they
fold over selection∧validity. When every agg is COUNT/MIN/MAX the pass emits an
`agg-metadata` op that answers from footer stats with **zero data-page scan**
(`COUNT(*)`=`num_rows`, `MIN`/`MAX`=footer min/max); float MIN/MAX is gated on the
NaN/signed-zero convention.

**`WHERE` (5.4).** A flat AND/OR of numeric field-vs-constant comparisons fuses in as a
**selection vector** — a dense LSB-first `[]byte` bitmap, layout-compatible with Arrow's
validity so `effective = predicate AND validity` is a byte-wise AND and one masked
reducer serves both (our own ~20-line bitmap over `math/bits`, not roaring/bitset).
Compare kernels (`base/filter.go` `FilterFloat64/Int64`, `AndBitmap`/`OrBitmap`) emit
the bitmap; `colPredicateExtract` pulls `(field, op, const)` from the cbq filter
(cbq normalizes `>`/`>=` to `LT`/`LE` with swapped operands). Anything else (nested
mixed boolean, field-vs-field, non-numeric) takes the row path.

**Arithmetic operands (5.5).** An aggregate operand can be a bare column or a binary
`+`/`-`/`*` of two numeric column/constant terms (`SUM(price*qty)`, `AVG(a+b)`). Per
batch the executor materializes into a reused float64 scratch column
(`base.ArithFloat64`/`ScaleFloat64`; int64 widened via `LoadFloat64FromInt64`) whose
validity is the AND of the term validities, then the masked reducer folds it. All
float64 (matching the row engine's JSON-number arithmetic ⇒ bit-exact). Bails: `/`,
unary `-`, >2 operands, nested arithmetic, non-numeric.

**Bounding the kernel combinatorics.** Rather than a detector per query shape, the
long-term plan is a recursive "columnarizable?" predicate (shapes fall out of
composition), generics for the type axis (`sumV[T Numeric]`), and pointwise lifting
(`f(a,b)` → `for i { out[i]=f(a[i],b[i]) }`). Hand-authored surface = the ~5 reductions
+ the dozen reshaping relational ops; everything else defers to the row engine.

## Key design decisions

- **Columnar source = optional sidecar interfaces**, not a widened `Source` (the
  `SubPathser` idiom): `ColumnsProjector{ProjectColumns([]string) error}` and
  `ColumnsSource{Columns() []ColumnMeta}`; non-implementers fall back to full
  transpose. Wanted-column set reused from cbq's `EarlyProjection()`.
- **Encoding = the Arrow value buffer itself** — zero-copy, no re-encode. (JSON-array
  encoding was measured at only 1.3× and skipped.)
- **Reuse existing accumulators** via typed catalog keys; never widen `Agg`.
- **Differential testing from day one** — the row lane is the oracle; scalar-Go kernels
  summing in scan order give *exact* equality (SIMD would force epsilon compares —
  another reason it's last).
- **Reuse cbq's plan analysis, don't hand-roll** (the Step-4 `EarlyProjection` lesson,
  reapplied to Step-5 vectorizability detection).

## Appendix — evidence & references

**Measured (arm64, pure Go, no SIMD).** Fixed-width columnar SUM/filter beats row-JSON
**44× at N≥64 with no tipping point**, growing with doc width (1 field 42× → 50 fields
730×) — the win is wide records with few projected fields. From a Parquet source
(no parse to build the column) the kernel-only SUM is ~56× the row baseline at 0.93
ns/value, 0 allocs; projection pushdown reads ~0.2% of the bytes (~80×). The Step-4
zero-alloc transpose (`appendRecordsNDJSON`, replacing `array.RecordToJSON`) cut
526K→2.1K allocs/op (~248×), 2.9× faster. Full numbers: `test/col_test.go`,
`test/parquet_test.go`.

**SIMD reality.** Go has no autovectorization; `GOEXPERIMENT=simd`/`simd/archsimd`
(Go 1.26) is amd64-only, no ARM/NEON/WASM, unstable. The call-boundary cost
(golang/go#77647) means per-`Val` SIMD is a guaranteed loss — it only pays amortized
across a batch. So SIMD is a **last, optional leaf** on the columnar batch, amd64-only
with a mandatory scalar-Go path; batching alone carries the arm64/WASM win. n1k1 gets
the awkward bits free: the validity/selection bitmap *is* the SIMD mask, and the scalar
fallback *is* the tail-remainder loop. (SIMD-JSON parsing is a separate, unrelated bet —
amd64-only, and its whole-document "tape" fights n1k1's lazy zero-copy row path.)

**Prior art.** DuckDB is the closest reference (embedded, vectorized push-based, reads
Parquet directly, selection vectors + dictionary + late materialization). The
LegoBase/LMS line (Klonatos & Koch VLDB 2014; Rompf/Odersky) — build high-level,
optimize as source-to-source transforms — is the lineage of `intermed_build` and of
pointwise lifting. The two ideas most worth internalizing next: **late materialization**
(C-Store/Vertica) and **operate-on-encoded-data** (Abadi SIGMOD 2006 — SUM over RLE,
predicates on bit-packed pages, without decoding), which is the real "stop transposing"
beyond Step 5.

**Encoding ladder** (cheap-to-adopt → fast-to-compute): JSON-array text (skipped, 1.3×);
fixed-width LE-packed int/float (shipped — what Arrow buffers give us); offset+payload
(Arrow string/binary, borrow-friendly); dictionary codes (low-cardinality → integer
GROUP BY, next); validity/selection bitmaps (shipped).
