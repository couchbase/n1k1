# n1k1 — columnar & SIMD design notes

Analysis and pathway proposals for **columnar (vectorized) execution** and
**SIMD** in n1k1. This is a *thinking* document, not a spec: it collects the
scattered hints in `TODO.md` (`col versus row optimizations`, `SIMD
optimizations possible?`) and `DESIGN-data.md` (the "columnar source, row
engine" caveat) into one place, weighs them against n1k1's actual internals, and
proposes an incremental path where each step earns its keep on its own.

See also: `DESIGN.md` (row engine + the lz compiler), `DESIGN-data.md`
(§ Parquet/Arrow scan sources), `DESIGN-exprs.md` (the no-boxing expression
model), `DESIGN-stats.md`.

-------------------------------------------------------
## The premise the user noticed

n1k1 already passes `[]byte` everywhere. The fundamental currency is:

```go
type Val  []byte   // one JSON-encoded value, usually immutable
type Vals []Val    // one row: positional slots aligned with Labels
type YieldVals func(Vals)   // push one row upward to the parent op
```

An operator receives rows one at a time through a `YieldVals` closure, does its
work, and calls the parent's `YieldVals`. A `Vals` is **one row wide**: slot `i`
is column `i` (a labeled "register", see `DESIGN.md` § registers).

The observation: **`[]byte` is axis-agnostic.** Nothing about the byte slice
says "I am one scalar." A `Val` could equally hold *a whole column* — M
sequential values of one field, packed contiguously. `TODO.md:243` already
sketches exactly this:

> col versus row optimizations — if columns are fixed size or fixed width, then
> a Val in the Vals can be interpreted as having multiple values in contiguous
> sequence. e.g. `prices := vals[7]; numPrices := len(prices) / sizeOfUint64`.

So the question is really: **can we reinterpret the two axes n1k1 already has,
rather than invent a new data structure?** Largely yes — and that framing is the
most n1k1-idiomatic way in. But the leverage and the friction both live in
places worth being precise about.

-------------------------------------------------------
## Where n1k1 stands today (the honest baseline)

- **Strictly row-at-a-time.** Every op in `engine/op_*.go` is written as a
  `lzYieldVals func(base.Vals)` closure invoked once per row (see
  `op_filter.go`, `op_project.go`). One row in, zero-or-one row out.
- **Expressions are scalar and per-row.** `base.ExprFunc = func(vals Vals,
  yieldErr YieldErr) Val` — given one row, produce one value. The whole
  no-boxing discipline (`DESIGN-exprs.md`) is built on "read JSON bytes for
  *this row*, append the result into a reused buffer."
- **Aggregates fold row-by-row.** `base/agg.go` update funcs have the shape
  `update(vars, v Val, aggNew, agg []byte, vc) ...` — one incoming `Val` folded
  into an accumulator `[]byte`. No notion of "apply SUM to a column at once."
- **The one place batches already exist:** `base/stage.go`. The data-staging /
  pipeline-breaker path exchanges `BatchCh chan []Vals` between actor
  goroutines. But note what that batch *is*: `[]Vals` — a slice **of rows**,
  row-major, each row deep-copied via `ValsDeepCopy`. It exists for
  **concurrency and cache-friendly hand-off**, *not* for vectorized compute. It
  is the closest structural neighbor to a column batch, but it is transposed the
  wrong way (rows, not columns) and its bytes are copies.

Two consequences fall out immediately:

1. There is no vectorized kernel anywhere. "Columnar support" in n1k1 does not
   mean "read Parquet" — you can already read columnar files and transpose to
   rows (`DESIGN-data.md` § iceberg_reader). It means **an op path that computes
   over many values per call.** That path does not exist today.
2. The schemaless, JSON-`[]byte` value model is the source of both n1k1's
   flexibility *and* the reason naïve columnar doesn't drop in: a column of
   arbitrary JSON values is *variable-width and untyped*, which is exactly the
   case SIMD and fixed-stride columnar hate.

-------------------------------------------------------
## The central idea: transpose the Val/Vals axes

Today:

```
Vals  = [ col0        col1        col2      ]      <- ONE row
          "alice"     42          {"x":1}
```

A **column batch** reinterprets the same two-level `[][]byte` shape:

```
ColBatch = [ col0-vector   col1-vector   col2-vector ]   <- MANY rows
             ["alice",       [42,            [{"x":1},
              "bob",          43,             {"x":2},
              "cara"]         44]             {"x":3}]
```

i.e. `Vals` becomes "one entry per column, each `Val` encodes M sequential
values." The outer slice length goes from *#columns* to *#columns* (same!), and
the inner `Val` goes from *one value* to *a packed vector*. The row count M is
the new hidden dimension.

This is attractive because it **reuses `base.Vals` as the container type** and
keeps the `YieldVals(Vals)` signature — a column batch is "just" a `Vals` whose
slots the receiver agrees to interpret column-wise. The push-based plumbing, the
recycling discipline, the Stage batching, the Labels alignment — all survive
structurally.

The catch is that "each `Val` encodes M sequential values" hides a real
decision: **how is a column encoded inside one `[]byte`?**

-------------------------------------------------------
## Encoding a column inside one `Val []byte`

Options, roughly in order of "cheap to adopt" → "fast to compute":

1. **JSON-array reuse (zero new format).** A column of M values is literally the
   JSON text `[v0,v1,...,vM-1]`. n1k1 *already* has the machinery to walk this:
   `base.ArrayYield` / `jsonparser.ArrayEach`, and the recently-added native
   array readers (`array_length/count/sum/avg`, commit 1adc8c3). This is the
   lowest-friction encoding: a column batch is a `Vals` of JSON arrays, and
   existing array exprs are the first "vectorized" kernels. **But** it is still
   variable-width text — you re-parse to find element boundaries, so it buys
   batching (fewer yield calls) without buying SIMD.

2. **Fixed-width native columns** (the `TODO.md:243` case). When a column's type
   is known and fixed — `int64`, `float64`, a fixed char[N] — pack the raw
   little-endian values: `len(val)/8` gives the count, `binary.Uint64` indexes
   element i. This is the encoding SIMD wants (see below). It requires **type
   knowledge the plan mostly doesn't have yet** (see § the compiler is the
   leverage).

3. **Offset/length + payload (Arrow-ish, variable-width).** For strings/blobs:
   one `Val` holds a contiguous byte payload, a sibling `Val` holds M+1 uint32
   offsets. This is exactly Arrow's `String`/`Binary` layout, and it means an
   Arrow/Parquet scan source (`DESIGN-data.md`) could hand its **borrowed**
   column buffers in *without a per-value transpose* — the columnar caveat's
   copy cost drops to near zero for the columns you pass through unchanged.

4. **Dictionary-encoded columns.** Low-cardinality string columns → an int32
   code vector + a dictionary `Val`. GROUP BY / DISTINCT / joins on such columns
   become integer ops (and integer ops are SIMD-friendly). This is where
   columnar pays for analytics workloads, not just scans.

5. **Validity + selection vectors** (orthogonal, needed by all of the above):
   - a **validity bitmap** `Val` marks MISSING/NULL positions (JSON's
     missing/null distinction has to survive — `base.ValMissing` is `nil`,
     `ValNull` is `"null"`; a bitmap needs *two* states or a pair of bitmaps).
   - a **selection vector** (list of surviving row indices, or a bitmap) is how
     a vectorized `filter` avoids compacting: it yields the *same* column batch
     plus "rows 3,7,12 survive," and downstream ops honor the selection. This is
     the single most important vectorized-engine primitive and n1k1 has no
     analog today (today's `op_filter.go` simply doesn't call the parent yield).

Realistic read: **(1) is a free warm-up, (3)+(5) unlock the Parquet/Arrow win,
(2)+(4) unlock SIMD and analytic GROUP BY.** They are independent; you don't
need all five.

-------------------------------------------------------
## SIMD — where it actually helps (and where it can't)

SIMD is only worth wiring where data is **fixed-stride, typed, and contiguous**.
Mapping that onto n1k1:

- **Yes, big win:**
  - Fixed-width numeric columns (encoding 2/4): predicate scans
    (`price < 1000` over an `int64`/`float64` column → a vector compare
    producing a bitmap), SUM/MIN/MAX/COUNT aggregates, arithmetic projections.
    These are textbook SIMD and Go can reach them via `math/bits`-style tricks,
    assembler (`.s`), or a lib. Note the WASM target (see `web/DESIGN.md`,
    `wasm-browser-demo`): browsers expose **WASM SIMD (128-bit)**, so a
    fixed-width kernel can even vectorize in the browser build.
  - Bitmap operations on validity/selection vectors (AND/OR/POPCOUNT across a
    filter chain) — word-at-a-time already, SIMD widens it.
  - Dictionary code comparisons (encoding 4).

- **SIMD-json specifically** (`TODO.md:241` "see SIMD-json articles"): this is a
  *different* use of SIMD — accelerating the **parse** of JSON text, not the
  compute. It could speed `op_scan`'s JSON path and `jsonparser` boundary-finding
  regardless of whether the engine goes columnar. Worth separating in the mind:
  *SIMD-parse* helps the row engine today; *SIMD-compute* needs the columnar
  encoding first.

- **No / not worth it:**
  - Variable-width JSON text columns (encoding 1) — element boundaries are
    data-dependent; you're back to scalar parsing. SIMD can help *find* the
    boundaries (SIMD-json) but not *compute* over them.
  - Anything with the boxed cbq fallback (`glue/expr.go`) — it allocates per
    row; vectorization is meaningless there. Vectorization only ever applies on
    the **native** expression lane (`DESIGN-exprs.md`).

The honest SIMD conclusion: **SIMD is a leaf optimization that only lights up
after types are known and a fixed-width columnar encoding exists.** It is the
*last* payoff of the columnar path, not the entry point. Don't lead with it.

-------------------------------------------------------
## The compiler is the leverage (why n1k1 is unusually well-placed)

Most engines bolt vectorization on as a runtime interpreter mode. n1k1 has
something better: **it already generates specialized Go** (`intermed_build` →
`intermed/` → emitted `*.go`, via the Futamura/LMS approach in `DESIGN.md`). Two
things follow:

- **Type specialization is already the plan.** `TODO.md:250` wants "types
  learned during expression compilation" so `sales < 1000` can commit to a
  numeric-only codepath. That *same* type inference is the precondition for
  choosing a fixed-width column encoding. Columnar and the existing
  static-typing TODO are the same project viewed from two angles: once the
  compiler knows a column is `int64`-only, it can (a) skip the JSON parse and
  (b) emit a vectorized kernel over a packed `int64` `Val`.
- **Operator fusion + var lifting already produce tight loops.** A vectorized op
  is "the same fused loop, but the innermost step is a SIMD kernel over a lifted,
  reused column buffer." The lifted-buffer discipline (`varLift`, see
  `op_project.go`'s `lzValsReuse`) is exactly the "reusable column buffer"
  vectorization needs.

So the compiler can host **two emitted lanes**: the current scalar row lane, and
a vectorized column lane chosen per-op when the plan proves fixed-width types.
This is far less disruptive than it sounds because the *interpreter* (`engine/`)
can stay row-only at first — vectorization can be introduced as a
**compile-time-only** specialization, mirroring how stats are interpreter-only
today (`genCompiler:hide`) but inverted.

-------------------------------------------------------
## Where columnar naturally enters n1k1 (highest ROI first)

1. **Aggregation / GROUP BY** — the classic columnar win. SUM/COUNT/AVG/MIN/MAX
   over a typed column is a tight vectorizable loop; `base/agg.go`'s per-row fold
   is the thing to specialize. Grouping keys as dictionary codes (encoding 4)
   makes the hash probe integer-keyed.
2. **Filter with selection vectors** — cheap to prototype, compounding benefit
   (every downstream op processes fewer rows without compaction). Needs the
   selection-vector primitive (§ encoding 5).
3. **Columnar scan sources (Parquet/Arrow)** — `DESIGN-data.md` already flags
   that the perf win is gated on "a vectorized/column-batch op path the engine
   doesn't have today." Encodings (3)+(5) are precisely that path; they let
   `iceberg_reader.go`'s borrowed Arrow buffers flow in without the per-value
   transpose.
4. **ORDER BY / TOP-K** — sort keys as fixed-width columns; the max-heap
   (`base/heap.go`) can compare packed keys. Lower priority.
5. **Projection arithmetic** — `a + b * c` over three numeric columns → three
   vector ops. Nice, but only matters once (1)–(3) are proving the encoding.

-------------------------------------------------------
## Pathway proposal (incremental, each step stands alone)

Each phase is independently shippable and independently *reversible* — none
forces the next.

- **Phase 0 — SIMD-parse, no engine change.** Evaluate SIMD-json-style parsing
  for `op_scan`'s JSON path / `jsonparser` boundaries. Pure row engine, pure
  win, decouples the "SIMD" question from the "columnar" question. *(Measure
  first; may not beat `jsonparser` on real docs.)*

- **Phase 1 — column-as-JSON-array batches (encoding 1).** Introduce a
  convention where a `Vals` slot *may* hold a JSON array representing a column,
  reusing `ArrayYield` + the native array readers. No new type, no type
  inference. Proves the transpose plumbing and the recycling story with zero
  format risk. Wire it first into a single op (e.g. a vectorized COUNT/SUM).

- **Phase 2 — selection vectors (encoding 5).** Give `filter` (and the ops below
  it) an optional "these row indices survive" channel. This is the primitive the
  whole vectorized model leans on; get its ownership/lifetime contract right
  early (it must obey the same "copy if you keep it" rule as `YieldVals`, see
  `base.YieldVals` doc-comment and `DESIGN-data.md` § borrow contract).

- **Phase 3 — typed fixed-width columns (encoding 2), compile-time only.** Ride
  the `TODO.md:250` type-inference work: when the compiler proves a column is
  numeric-only, emit a fixed-width packed `Val` and a vectorized kernel for one
  op (aggregation is the best first target). Interpreter stays row-only.

- **Phase 4 — SIMD kernels.** Now, and only now, drop SIMD into the fixed-width
  kernels from Phase 3 (predicate scan, SUM/MIN/MAX, bitmap AND). Include a
  scalar fallback and a WASM-SIMD variant for the browser build.

- **Phase 5 — Arrow/Parquet zero-transpose (encoding 3).** Feed borrowed Arrow
  column buffers straight into the Phase 2/3 column path, closing the
  `DESIGN-data.md` columnar caveat.

- **Phase 6 — dictionary encoding (encoding 4).** Low-cardinality strings →
  integer codes for GROUP BY / joins. Highest complexity, do last.

-------------------------------------------------------
## Tensions & hard parts (don't paper over these)

- **Schemaless JSON is the whole point of SQL++.** A column of heterogeneous /
  missing / nested values has no fixed width and no single type. The fixed-width
  fast path is an *opt-in specialization the compiler proves*, never the default;
  the JSON-array/opaque path must always remain as the correctness fallback.
  Same shape as `DESIGN-exprs.md`'s native-vs-boxed split.
- **MISSING vs NULL vs value is a three-state per slot**, not the two-state
  valid/invalid of Arrow. Any bitmap scheme has to preserve n1k1's
  `ValMissing (nil)` ≠ `ValNull ("null")` distinction or it breaks SQL++
  semantics (`ValEqualMissing`/`ValEqualNull` in `base/base.go`).
- **The garbage-avoidance ethos cuts both ways.** Column batches are *more*
  recyclable than rows (one big buffer per column vs many small `Val`s) — a real
  win. But `stage.go` today **deep-copies** rows into batches; a columnar batch
  wants to *borrow* source buffers where possible, which collides with the
  "materialize on hand-off" safety currently in `ValsDeepCopy`. The borrow
  contract must be explicit (cf. the `-race` fixes in `race-fixes-union-all`:
  shared buffers across actors are a landmine).
- **Expression model is per-row.** `ExprFunc(vals, yieldErr) Val` can't express
  "compute over a column" without either (a) a parallel vectorized ExprFunc
  signature, or (b) the compiler synthesizing the vector loop around the scalar
  kernel. (b) is more n1k1-idiomatic (fusion already does this) but is compiler
  work, not a library add.
- **Correctness bar.** n1k1's test discipline is heavy (`DESIGN-testing.md`, the
  gsi corpus). A vectorized lane doubles the codepaths under test; it needs
  differential testing (row lane vs column lane must agree) from day one, not as
  an afterthought.
- **When is a batch a batch?** Row-at-a-time has latency benefits (first row out
  fast); vectorized wants big batches for throughput. n1k1 already has the
  `batchSize` / `batchChSize` knobs in `stage.go` and flags them as
  "dynamic/computable" TODOs (`TODO.md:77`) — the columnar batch width is the
  same knob and should reuse that machinery.

-------------------------------------------------------
## Open questions

- Is the biggest near-term prize **SIMD-parse of JSON** (Phase 0, helps the
  engine that exists) or **vectorized aggregation** (Phase 1–3, needs new
  plumbing)? Benchmark both before committing (`DESIGN-benchmark.md`).
- Should the column encoding be **n1k1-native** (fixed-width LE, our own
  offsets) or **Arrow-compatible** from the start? Arrow-compat costs some
  design freedom but makes Phase 5 (Parquet zero-transpose) nearly free and opens
  interop. Leaning Arrow-compatible for the *variable-width/string* layout,
  n1k1-native for the *numeric* layout.
- Can the vectorized lane stay **compile-time-only** indefinitely (interpreter
  = row, compiler = row-or-column), keeping the interpreter simple? That mirrors
  the current stats split and seems desirable.
- Does the WASM build (`web/`) change the cost/benefit? WASM SIMD is 128-bit
  only and JS-boundary costs dominate there (`js-udf-perf`), so the columnar win
  in-browser may come more from *fewer boundary crossings* (batching) than from
  SIMD width.

-------------------------------------------------------
## One-line summary

The user's instinct is right: **`[]byte` is axis-agnostic, so a `Val` can encode
a column, and n1k1's compiler + lifted-buffer + batching machinery are unusually
well-suited to host a vectorized lane.** The work is not "add a column type" —
it's *type inference in the compiler* + *a selection-vector primitive* + *an
opt-in fixed-width encoding*, with SIMD as the final leaf payoff and the
schemaless JSON path always kept as the correctness fallback.
