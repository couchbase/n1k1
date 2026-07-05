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
## Marking a slot as columnar via the label/register system

An attractive idea: rather than change the container type, let a **`Labels`
entry carry a marker prefix** signaling that its positionally-aligned `Val` is a
*column* (a packed vector), not a scalar. This is more idiomatic than it first
sounds — **n1k1 already does the scalar version of exactly this.**

**Existing precedent.** `engine/expr.go:ExprLabelUint64` reads a slot whose `Val`
is *not JSON* but a little-endian `uint64` (`binary.LittleEndian.Uint64(
lzVals[idx])`), converting to JSON only on demand. So "a labeled register whose
bytes carry a special, non-JSON interpretation, dispatched at compile time" is
already in the codebase. A columnar marker is the natural generalization: from
*"this slot is a packed scalar uint64"* to *"this slot is a packed vector of
uint64."* The label→reader dispatch happens via the expr catalog
(`base.ExprCatalogFunc`) at setup, so it costs **nothing per row** — which is the
whole point.

**Why labels are the right carrier.** They are **early-bound and positional**
(`Labels.IndexOf` runs at plan setup, expr.go:66/108, never per row), so a
vectorized kernel can be selected at codegen with zero runtime dispatch — exactly
the "compile-time-only vectorized lane" this doc argues for (§ *The compiler is
the leverage*). And the marker can encode *which* encoding a slot uses, turning
the § encodings list into one propagatable vocabulary:

| Marker | Meaning | Encoding |
|---|---|---|
| `@col.json` | column as a JSON array | § enc 1 |
| `@col.i64` / `@col.f64` | fixed-width packed numeric column | § enc 2 |
| `@col.str` | offsets + contiguous payload | § enc 3 |
| `@col.dict` | dictionary codes + dict slot | § enc 4 |
| `@valid` / `@sel` | validity / selection bitmap slot | § enc 5 |
| `@const` | one scalar broadcast across the batch | (constant vector) |

**Two ways to carry it (a real tradeoff):**
- **(a) Sigil in the label string** (`@col.f64:.["price"]`). Reuses
  `Labels []string`, no struct change, and the marker **rides along automatically**
  wherever labels are copied/derived through the op tree. Cost: `IndexOf` is
  exact-match, so every consumer matching the *logical* name must strip the prefix
  first — pervasive marker-awareness.
- **(b) A parallel shape vector** — a `[]ColKind` positionally aligned with
  `Labels`, the way `Op.StatsBase` / `Ctx.Stats.Counters` are aligned (see
  `DESIGN-stats.md`). Keeps `IndexOf` pure; costs one slice; still free at runtime
  since it's early-bound. More n1k1-idiomatic (positional parallel arrays are the
  established pattern). A **hybrid** also works: the sigil is the source of truth,
  parsed once into the `ColKind` enum at setup.

**What the marker does *not* solve (it's a signal, not the whole model):**
1. **Where the row-count M lives.** Every columnar slot in one `Vals` must share
   M. That count needs a home — a batch-header field, or derivation from a
   designated column's `len / elemWidth`. The marker says "column," not "how many."
2. **Nulls / selection.** A marker can *name* a companion `@valid`/`@sel` slot,
   but the bitmap must still exist as its own slot (§ enc 5).
3. **Coherence.** Mixed scalar+column in one batch is only sound if the scalars
   are `@const` (broadcast); otherwise M is ambiguous. That invariant must be
   enforced at plan-build.
4. **Propagation & explode rules.** Output labels are derived from input labels
   per op, so the marker must flow through label-derivation — and any op that
   *can't* vectorize a marked column must **explode** it back to rows and drop the
   marker. Those rules are the operator-fusion / lane-selection logic.

**Correctness boundary.** A marked slot must never reach an op that assumes
scalar JSON (it would misread packed bytes). The safe stance — consistent with
how `ExprLabelUint64` is *plan-selected*, not automatic — is that markers appear
only in **compiler-generated plans** where the compiler has proved every consumer
will vectorize-or-explode. Markers are an internal compiler artifact, not
something an interpreter-mode plan ever hand-writes.

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
  encoding first. (Deep dive on the actual library + its constraints in
  § *Go's SIMD reality* below — the short version is it's more limited and more
  x86-specific than it first sounds.)

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
## Go's SIMD reality (as of 2026) — what's actually reachable

The "kinds of operations are limited" instinct is correct, and it's a *toolchain*
limit, not just a design choice. Before committing to any SIMD kernel, know the
four ways to reach SIMD from Go and their costs:

1. **Compiler autovectorization — basically absent.** Go's SSA backend does *not*
   autovectorize general loops. A few runtime primitives (`bytes.IndexByte`,
   `bytes.Equal`, `crypto/*`, `hash/crc32`, `math/bits` popcount) are fast because
   they're **hand-written Plan9 assembly** in the stdlib, not because the compiler
   vectorized Go. You cannot expect a plain Go `for` loop over a column to become
   SIMD by itself.
2. **Hand-written Plan9 assembly (`.s` files).** The real, portable-today option.
   Per-arch (amd64 `.s`, arm64 `.s`, …), each with a Go fallback for other arches.
   High maintenance; easy to get wrong; but it's what actually ships.
3. **`avo` (`mmcloughlin/avo`) — generate that assembly from Go.** This is how
   `klauspost/*` and `minio/simdjson-go` are built. Still per-arch and still
   producing asm, but far more maintainable than raw `.s`. If n1k1 hand-rolls
   kernels, this is the tool to use.
4. **`GOEXPERIMENT=simd` → the `simd/archsimd` package (NEW).** Landed in **Go
   1.26** (RC1 Dec 2025, released ~Feb 2026); development continues on `dev.simd`
   for 1.27. Exposes real intrinsics — vector types like `Int8x16`, `Float64x8`
   (128/256/512-bit), each intrinsic ≈ one machine instruction — and the roadmap
   has a future *portable* high-level vector API layered on top. **But the caveats
   are severe for n1k1's purposes:**
   - **AMD64-only.** No ARM64/NEON. Many dev machines here are Apple Silicon
     (this repo is developed on `darwin`/arm64), so archsimd kernels wouldn't even
     run natively during development.
   - **No WASM.** The `web/` browser build (`wasm-browser-demo`) can't use
     archsimd at all; WASM SIMD is a *separate* 128-bit instruction set reached
     through entirely different codegen.
   - **Experimental + build-flag gated + unstable API.** Code only compiles under
     `GOEXPERIMENT=simd`; the surface can still change. Betting n1k1 code on it in
     2026 means dual codepaths and a moving target.

**The boundary-overhead trap (and why it reinforces "batch first").** A live Go
issue (golang/go#77647) documents that hand-written-asm SIMD carries a **Go↔asm
call-boundary cost** on every invocation; for *small* data chunks that fixed
overhead eats the SIMD win entirely. The intrinsics work (option 4) exists partly
to erase that boundary. For n1k1 the lesson is direct: **per-`Val` (per-scalar)
SIMD is a guaranteed loss** — the call overhead dwarfs the work — **so SIMD only
pays when amortized across a whole column batch of M values in one call.** This is
independent confirmation of this doc's central thesis: get the columnar *batch*
first; SIMD is only worthwhile on top of it.

**Practical takeaway.** The set of SIMD ops n1k1 could realistically reach *today*
(via avo/`.s`, portably) is narrow: fixed-width integer/float compare, arithmetic,
min/max/sum reductions, and bitmap AND/OR/POPCOUNT. That's *exactly* the set the
columnar encodings (§2, §4, §5) produce — which is fine, because those are also
the only ops worth vectorizing. Anything variable-width or type-polymorphic (i.e.
most raw JSON) is out of reach until decoded into one of those fixed layouts.

-------------------------------------------------------
## Fixed lane counts and the "tail" (remainder) problem

A SIMD register is fixed in **bits**, and holds only a *handful* of lanes — not
hundreds. Lane count = width ÷ element size:

| Register width | uint64 | uint32 | float64 | uint8 |
|---|---|---|---|---|
| 128-bit (SSE, ARM NEON, **WASM SIMD**) | 2 | 4 | 2 | 16 |
| 256-bit (AVX2) | 4 | 8 | 4 | 32 |
| 512-bit (AVX-512) | 8 | 16 | 8 | 64 |

So the widest common register does **8** uint64s at a time. (`archsimd`'s type
names spell this out: `Int8x16` = 16×int8 = 128-bit; `Float64x8` = 8×float64 =
512-bit.)

**The tail.** A column of N values almost never divides evenly by the lane count
L. 1003 uint64s at L=8 → 125 full vectors + **3 leftover**. Handling that
remainder is universal SIMD bookkeeping; the standard techniques, most common
first:

1. **Scalar remainder loop.** SIMD over `N − (N mod L)`, then a plain loop for the
   last `N mod L`. Always correct; one extra branchy loop. This is the default.
2. **Masked / predicated ops.** AVX-512 (`k`-mask registers) and ARM **SVE**
   (length-agnostic by design) compute only the active lanes via a bitmask, so the
   tail is one masked op — no scalar epilogue. But needs AVX-512/SVE (not baseline
   SSE/AVX2/NEON/WASM).
3. **Pad to a lane multiple with identity values.** If you *own the buffer*,
   over-allocate to a multiple of L and fill the pad with the op's identity (`0`
   for SUM/OR, `+∞` for MIN, `−∞` for MAX). Kernels then run only whole vectors and
   the pad is inert — no tail branch. Cost moves to allocation time.
4. **Overlapping last block.** Place the final vector to *end* exactly at N,
   re-touching a few done elements. Fine for idempotent ops (min/max, memcpy),
   **wrong for reductions** (SUM double-counts) unless the overlap is masked.

**How columnar engines dodge it:** fix a logical batch size that's a multiple of
every lane count — DuckDB's `STANDARD_VECTOR_SIZE = 2048` (divisible by 2/4/8/16)
— so the tail *disappears inside every batch but the last*. "A tail per kernel
call" becomes "a tail once per column."

**Why this favors n1k1.** Because n1k1 would own the column encoding, techniques
(2)/(3) come nearly free:
- Pick the columnar batch width as a multiple of L (DuckDB-style) → tails only on
  the final batch.
- Pad fixed-width column buffers to a lane multiple with identity values → most
  kernels never branch for a tail.
- The **validity/selection bitmap** the design already needs (§ encoding 5) *is*
  the mask for technique (2): a masked reduction that skips MISSING/NULL lanes is
  the same machinery that skips tail lanes.
- The mandatory scalar-Go fallback (Phase 4) doubles as the remainder loop of
  technique (1) — so it's never wasted work; it's the arm64/WASM path *and* the
  tail handler.

-------------------------------------------------------
## SIMD-json parse — what it is, and why it's not a drop-in

`simdjson` (Daniel Lemire et al.) and its Go port **`minio/simdjson-go`** parse
JSON with a **two-stage** design:

- **Stage 1 (the SIMD part):** scan the raw bytes 32/64 at a time to classify
  *structural characters* (`{ } [ ] : ,`, quotes, whitespace) and emit their byte
  offsets. This is the vectorized step — it finds *where* the structure is without
  interpreting values.
- **Stage 2:** consume those offsets to build a **"tape"** — a flat array encoding
  the document's structure (element types + offsets/inline values). In
  `simdjson-go` the two stages run as concurrent goroutines over a channel, and it
  emits *incremental* uint32 offsets so arbitrarily large files parse (limit: no
  single string element > 4 GB).

Why it does **not** slot cleanly into n1k1's hot path:

- **Hard CPU requirement, no fallback.** Needs **AVX2 + CLMUL** (Intel Haswell
  2013+, AMD Ryzen/EPYC 2017+). There is **no scalar fallback** — you call
  `SupportedCPU()` and route around it yourself. `gccgo` and **WASM** are
  unsupported, and it's **amd64-only** (no arm64) — so it can't run on Apple
  Silicon dev machines *or* in the `web/` browser build. That's two of n1k1's
  actual environments excluded outright.
- **It produces a tape, not borrowed sub-slices.** n1k1's whole value model is
  `jsonparser.Get()` — *lazy, on-demand, zero-copy `[]byte` sub-slices* into the
  original document, touching only the 1–2 fields a query actually projects out of
  a wide doc (`DESIGN-exprs.md`, `DESIGN-data.md` § borrow contract). simdjson-go
  instead parses the **whole document** into a tape up front. For n1k1's common
  case (project a couple of fields from a wide record) that's *more* work, not
  less — you materialize structure you'll never read. (It can point string
  payloads back into the message buffer via `WithCopyStrings(false)`, but the tape
  itself is still whole-document.)
- **It wins on the opposite workload:** parsing *gigabytes* where you consume most
  of each large document, with enough per-doc bytes to amortize the two-goroutine
  setup. On small docs the fixed overhead loses to `jsonparser`.

**So where does it actually fit?** Not as a `jsonparser` replacement on the row
path — but as an **ingest engine for the columnar path**. Stage-1's structural
scan + a tape is a natural front-end for *transposing a JSON file into column
batches* (§ encoding 1/3): parse once, scatter values into per-column buffers.
That places it around **Phase 5** (columnar ingest), gated behind
`SupportedCPU()` with the plain `jsonparser` row path as the mandatory fallback
for arm64 / WASM / pre-Haswell / small-doc cases. Treat *SIMD-parse* and
*SIMD-compute* as two separate, independently-justified investments.

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
## Pushdown: does n1k1 have the wiring? (projection down, metadata up)

Borrowing columnar `[]byte`s from Parquet / a columnar store pays off only if two
signals can flow across the scan boundary: **projection down** ("I only want
fields X, Y") and **metadata up** ("field XYZ is int64, `null_count == 0`"). Where
n1k1 stands on each, honestly:

**Projection pushdown (down) — a tale of two scan paths:**
- **cbq / GSI index path: yes, partially — inherited from the couchbase/query
  planner.** Three real channels already work:
  - **Index spans** — range predicates are pushed into `secondaryIndex.Scan(span
    …)` (`glue/idx_si.go`), so the index returns only the matching key range.
  - **Covering indexes** — `plan.IndexScan.Covers()` (`glue/conv.go`,
    `coverableIndexScan`) carries the *exact set of fields the query needs*; when
    the index covers them, the answer comes from the index with **no document
    fetch**. That is projection pushdown, just expressed in covering-index terms.
  - **Subpath projection on fetch** — `datastore_fetch.go` passes `subPaths` to
    cbq's `keyspace.Fetch(…, subPaths, projection, …)` (the fuller `projection`
    arg is still passed `nil`, an unused slot).
- **Native file / `records.Source` path: no.** The interface is a narrow whole-doc
  pipe (`records/records.go`):
  ```go
  type Record struct { ID []byte; Doc []byte }   // whole JSON doc, borrowed
  type Source interface { Next(rec *Record) (bool, error); Close() error }
  ```
  `Next` hands back the **entire document**; there is no input to request a
  subset, and CSV is decoded into whole JSON objects. This is precisely the path a
  Parquet/columnar reader plugs into — and it has **zero projection channel
  today.** (`DESIGN-data.md` § "Range pushdown for big/columnar files" via
  `ReadAt` flags this as future work.)

  **The information already exists** — the wanted-field set lives in the `project`
  op's expressions, in `Covers()`, and in the label vector at plan time. It is
  simply **not threaded into `Source`.** Wiring it is "surface an already-known
  field set to the source," not "invent field tracking."

**Schema / stats (up) — mostly a conceived hook, not built:**
- **Today `Source` exposes nothing** — no types, no nullability. The engine is
  schemaless; labels carry no type (the `@col.i64` marker of § *Marking a slot as
  columnar* is proposed, not present). Even CSV's sniffed types are dropped on the
  way to JSON objects.
- **Two hooks are designed in `DESIGN-data.md`:** (1) a typed source's inferred
  schema is meant to *become the label vector* — a channel to declare columns
  exists conceptually, it just carries no types/nullability yet; (2) the
  **catalog / zone-map** design already tracks per-column **min/max, null-count,
  distinct-estimate, `schema_fingerprint`** — but for *file/partition pruning*,
  not as a per-scan codegen signal.
- **Parquet hands you exactly this for free.** Its footer carries the Arrow schema
  (types) plus per-column-chunk statistics including `null_count` and min/max. So
  the *storage library provides* "XYZ is int64, `null_count == 0`" — n1k1 just has
  **no interface to receive and act on it.**

**Synthesis — the two ends of one unbuilt wire.** The needed-field set is known at
the *plan* end (projection-down), and the types + null-counts are known at the
*Parquet-footer* end (metadata-up), but `records.Source` carries **neither**.
The missing piece is *not* a wider `records.Source` — it's a pair of **optional
sidecar interfaces the caller type-asserts against**, which is already the n1k1
idiom (`glue`'s `SubPathser` = `interface{ SubPaths() []string }`, used as
`if sp, ok := plan.(SubPathser); ok {…}`; likewise `interface{ RecordsDir()
string }`). The core `Source` stays `{Next, Close}` — schemaless jsonl/csv/yaml
sources are untouched (no forced no-op methods), and a source that doesn't
implement a capability simply falls back to the full transpose. Two capabilities:

- **projection down** — `ColumnProjector interface { ProjectColumns(names
  []string) error }` (call before the first `Next`), so a columnar reader
  materializes only wanted columns — which is *also* what makes borrowing Arrow
  column buffers zero-copy (§ enc 3);
- **schema/stats up** — `SchemaSource interface { Columns() []ColumnMeta }` where
  `ColumnMeta` carries `{Name, Type, NullCount, Min, Max}`. This **populates the
  columnar labels/markers** of § *Marking a slot as columnar*: a Parquet footer is
  the natural *source* of a `@col.i64` / no-nulls marker, the marker its
  *destination*.

New capabilities (zone-map `RowGroupPruner`, predicate pushdown) then drop in as
further sidecars without touching the core interface or any existing source.
Caveat: the interface is the easy part — the *caller* (the glue scan layer) must
mine the wanted-column set from the plan (`project` exprs / `Covers()` / the label
vector, which already know it) and push it down; and the richest consumer of
types/stats is the **planner** (pruning + type specialization) running *before* a
scan-time `Source` exists, so `SchemaSource` serves the scan-time need while
plan-time stats stay a catalog/zone-map concern (`DESIGN-data.md`/`DESIGN-stats.md`).

The payoff is concrete and compounding: **`null_count == 0` means the column needs
no validity bitmap, so its SIMD kernels need no masking** (§ *the tail problem*) —
the fastest path — while a known fixed type lets the compiler skip JSON parsing
entirely and emit the vectorized kernel directly. Note the encouraging half: the
cbq/GSI path already *proves the engine can consume rich pushdown* — the
architecture isn't hostile to it; the gap is specifically the native
`records.Source` pipe.

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
  kernels from Phase 3 (predicate scan, SUM/MIN/MAX, bitmap AND). Reach it via
  `avo`-generated asm (portable-today) or `GOEXPERIMENT=simd`/`archsimd`
  (amd64-only, unstable) — see § *Go's SIMD reality*. **A plain-Go scalar kernel
  is mandatory, not optional**, since arm64 (Apple Silicon dev machines) and WASM
  can't run the amd64 asm; add a WASM-SIMD (128-bit) variant for the browser build
  separately. Every SIMD kernel is one impl among ≥2 that must agree under
  differential test.

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
  SIMD width. Neither `archsimd` nor `simdjson-go` runs in WASM at all.
- **How many SIMD backends can we stomach?** Portable SIMD across amd64 + arm64 +
  WASM means ≥3 asm/intrinsic backends *plus* a scalar reference — a real
  maintenance and testing multiplier. Is the win worth it beyond amd64 servers,
  or should SIMD stay an amd64-only accelerator with scalar-Go everywhere else
  (and columnar *batching alone* — fewer calls, better cache behavior — carry the
  arm64/WASM benefit)? Batching helps every arch; SIMD helps one. That argues for
  landing the columnar batch value even where no SIMD kernel ever ships.

-------------------------------------------------------
## The critical questions this doc has been dodging

Everything above asks *how* to do columnar. The harder questions are *whether* and
*how much*, and they're about **workload fit**, not mechanism. These gate the
whole effort:

1. **Does columnar even match n1k1's workload?** n1k1 runs inside couchbase/query
   over **schemaless, nested, heterogeneous JSON**, frequently accessed
   *selectively* (index-driven, point lookups, small results, `LIMIT`).
   Columnar/SIMD win on the **opposite** profile: large low-selectivity scans,
   flat uniformly-typed columns, aggregation over millions of rows. This is *the*
   question; everything else is downstream of it. If selective/nested/point
   queries dominate, columnar is a solution seeking a problem — unless there's a
   real analytical-scan segment (Parquet lakes, big GROUP BY) to aim at.
2. **Break-even vs. the existing row engine.** The row engine is already
   push-based, compiled, and garbage-free — *not* a slow baseline. Columnar's
   fixed setup (build/transpose batches) loses on small/selective/early-`LIMIT`
   plans. Need a measured crossover and a rule for when the compiler even picks
   the column lane.
3. **How much of a real plan can stay in-lane?** Only a sliver of SQL++'s function
   surface (`DESIGN-exprs.md`) vectorizes; every other op forces an
   **explode-to-rows** (and re-transpose to resume). If real plans transition
   lanes several times, transpose overhead can eat the kernel win. Measure the
   vectorizable fraction of real plans and the per-transition cost.
4. **The schema-flexibility tax — mixed-type columns.** A JSON "column" may hold
   int64 *and* string *and* missing across documents; fixed-width needs one type.
   Policy? Fall back entirely to JSON-array encoding, or typed-majority + an
   exception list? How common is a *cleanly* typed column in real Couchbase data?
   (Parquet sources are pre-typed and sidestep this — another vote for
   source-first.)
5. **Spilling & concurrency.** n1k1 spills hash-join/group-by/order-by to disk
   (rhmap/store) and hands row batches across actor goroutines with deep-copy (the
   `-race` history). How do column batches + borrowed Arrow buffers survive spill
   and cross-actor handoff without breaking the borrow contract?
6. **Who builds the first column batch?** Without a columnar *source*, columns
   must be synthesized by transposing row JSON — a cost that may never repay. The
   win is only "free" when the bytes arrive columnar (Parquet/Arrow). Strongly
   implies **source-first, not engine-first.**
7. **Dual-lane maintenance appetite.** Every vectorized op doubles the code under
   test and needs differential testing forever. A standing tax that should be a
   deliberate choice, not drift.

-------------------------------------------------------
## Proposed approach — evidence-gated, source-first, kill-early

Given the above, do *not* start by building columnar ops. Sequence to buy the most
information for the least code, and abandon early if the numbers aren't there.
(This refines the earlier § *Pathway proposal* with a strategy for *whether* to
walk it.)

- **Step 1 — Characterize the workload (no code).** Answer Q1: is there a real
  analytical-scan / Parquet-lake / big-aggregation segment for n1k1? If not, scope
  columnar to just "*query* Parquet at all" (correctness via transpose) and skip
  vectorization entirely.
- **Step 2 — Spike the ceiling (throwaway, ~1–2 days).** Bound the upside:
  micro-benchmark SUM and a predicate-filter over ~1M values, comparing (a)
  today's row-JSON path, (b) a hand-built fixed-width `[]byte` column + scalar Go,
  (c) + SIMD via `avo` — on **both amd64 and arm64**. If best-case isn't ≥3–5×,
  the *realistic* win (after transpose/explode overhead) won't justify a dual-lane
  engine. Stop here. **✅ Done on arm64 (pure Go, no SIMD) — the gate is cleared
  by 40×–730×; see § *Spike results* below.**
- **Step 3 — Ship a columnar *source*, transpose-to-rows (real feature, no engine
  change).** Wire a pure-Go Parquet/Arrow `RecordSource` yielding borrowed column
  buffers, initially transposed to JSON rows (the `DESIGN-data.md` correctness
  path). Delivers "query Parquet at all" — user value independent of
  vectorization — and creates the substrate. **✅ Shipped — `records/parquet.go`
  (arrow-go, transpose-to-rows via `array.RecordToJSON`) wired into
  `records.OpenFile`; a `.parquet` file is now a queryable keyspace end-to-end
  (`SELECT … FROM orders` over `orders.parquet`, proven by
  `test/parquet_test.go:TestParquetQueryEndToEnd`). Guarded `!js` (the wasm/browser
  build gets a stub — arrow-go stays out of it, verified by `go list -deps`). The
  *feasibility* measurements (projection pushdown 80–137× / 0.2% bytes, free footer
  types+null_count+min/max, parse-free Arrow `[]float64` at the 0.9 ns/value
  fixed-width ceiling) are in § *Parquet prototype results*.**
- **Step 4 — Optional sidecar interfaces on `Source` (the missing wire).** *Not*
  a wider `Source` — add capability interfaces the scan layer type-asserts, the
  `SubPathser` idiom (§ *Pushdown*): `ColumnProjector{ ProjectColumns([]string) }`
  and `SchemaSource{ Columns() []ColumnMeta }`. The Parquet source implements both;
  jsonl/csv/yaml stay `{Next, Close}` and fall back to the full transpose. The
  Parquet reader then reads only wanted columns and declares type + `null_count`;
  still row-transposed downstream, but pushdown cuts I/O and the schema populates
  the label markers. Real work is caller-side (mine wanted columns from the plan).
- **Step 5 — First true vectorized op, compile-time-only.** Aggregation over a
  proven-typed, non-null column straight from Parquet — end-to-end, no transpose.
  Introduce column batches + the `@col` markers (§ *Marking a slot*) here.
  Differential-test against the row lane from line one. Measure on real data.
- **Step 6 — Expand only on measured wins.** Selection vectors for filter, more
  aggregates, dictionary encoding for GROUP BY — each gated on a benchmark that
  beats row-at-a-time on a real workload.
- **Step 7 — SIMD last, amd64-only, scalar fallback mandatory** (which is also the
  arm64/WASM path and the tail/remainder loop).

The throughline: **the row engine is already good and the dual-lane cost is real,
so prove the ceiling *and* the workload fit before committing — and let columnar
bytes enter from a columnar source rather than synthesizing them from rows.**

-------------------------------------------------------
## Spike results — measured ceiling (Apple Silicon, pure Go, NO SIMD)

Step 2's ceiling spike, run on an M-series MacBook (arm64) with **scalar Go only —
no SIMD, no amd64 asm** — so these numbers are the **floor** of the columnar win,
not the ceiling. SUM and filter-count over N float64 `price` values, comparing
n1k1's faithful row path (whole JSON doc per record, `jsonparser.GetFloat` per
row) against the columnar encodings. All paths zero-alloc. Reproduce (benchmark
lives in `test/col_test.go`; needs the `DESIGN-testing.md` worktree bootstrap):
`CGO_ENABLED=0 GOPRIVATE='github.com/couchbase/*' go test -tags n1ql ./test/
-run='^$' -bench=BenchmarkCol -benchmem`.

**SUM, per-value cost (ns/value), narrow 1-field doc:**

| N | row-JSON (n1k1 today) | JSON-array (enc 1) | fixed-width (enc 2) | native `[]float64` |
|---|---|---|---|---|
| 64  | 36.8 | 28.1 | 0.68 | 0.44 |
| 1K  | 38.8 | 30.2 | 0.87 | 0.83 |
| 64K | 38.5 | 29.8 | 0.88 | 0.87 |
| 1M  | 39.8 | 30.0 | 0.91 | 0.87 |

1. **Fixed-width is ~44× the row path and sits AT the native-`[]float64` ceiling**
   (0.91 vs 0.87 — the little-endian decode is nearly free). The row path's whole
   cost is **JSON number parsing**, which columnar skips.
2. **No tipping point in N** at the kernel level — per-value cost is flat for every
   approach; fixed-width wins from N=64. "You need big data for columnar" is false
   for the *compute*. (Fixed-width drifts 0.68→0.91 as the 8 MB column outgrows
   cache — memory-bandwidth bound; the row path is parse-bound, hence flat.)
3. **JSON-array encoding (enc 1) barely helps** (1.3× over row) — it still parses
   text per value. The real jump is fixed-width (enc 2). Prioritize enc 2 over
   enc 1.

**The tipping point IS document width — where the "vertical stripe" shines
(N=1M):**

| doc width (fields) | row-JSON ns/value | fixed-width | speedup |
|---|---|---|---|
| 1  | 38.4 | ~0.9 | 42× |
| 5  | 83.6 | ~0.9 | 93× |
| 20 | 294  | ~0.9 | 327× |
| 50 | 660  | ~0.9 | 730× |

The row path scales **linearly with doc width** — a left-to-right JSON parser must
scan past every unwanted field to reach `price`. The fixed-width column is
**constant** — it touches only its stripe. So the columnar win grows with exactly
what hurts row-at-a-time: **wide records where you project few fields.** That is
the vertical-stripe payoff, quantified: ~40× at 1 field, ~730× at 50 fields.
(Filter-count `price > 500`, N=1M, narrow: row 39.5 vs fixed 0.69 → **57×**.)

**Economic break-even when the source is JSON** (you must pay to *build* the
column). Building a fixed-width column from JSONL costs one parse pass
(~38 ns/value); each op over it then costs ~0.9. For K ops touching the column:
row = 38·K, columnar = 38 + 0.9·K → columnar wins once **K > ~1**. So **any query
that touches a column more than once** (`WHERE price>x … SUM(price)`, GROUP BY +
agg) already repays a transient transpose. From a **Parquet/Arrow source (no parse
to build), columnar wins unconditionally.**

**What the spike settles for the approach:**
- The Step-2 gate (≥3–5×) is **cleared by 40×–730× on arm64 with zero SIMD.** On
  this hardware the win is *not-parsing* + *touching one stripe*; **SIMD would be
  additive, not load-bearing.** (This is *why* an arm64-only, no-SIMD target is
  still very much worth pursuing — the dominant lever isn't vector width.)
- Confirms **source-first**: unconditional win from a columnar source, and even
  JSON-sourced columns pay off after a single reuse — so the transpose is *not*
  the blocker the `DESIGN-data.md` caveat feared, provided the column is reused or
  the doc is wide (both true for this "directories full of fat files" workload).
- Confirms **fixed-width (enc 2) ≫ JSON-array (enc 1)** — 44× vs 1.3×.
- **Caveat — this is the kernel ceiling.** It excludes explode-to-rows transitions
  (§ critical Q3) and assumes a cleanly-typed column (§ Q4). A single SUM straight
  over JSONL with no reuse is a wash (you paid the parse to build). The measured
  win is real precisely for the reuse / wide-doc / columnar-source cases — which
  is the target workload, not a coincidence.

-------------------------------------------------------
## Parquet prototype results — a columnar source, measured (arrow-go, arm64)

Step 3's prototype (`test/parquet_test.go`, `apache/arrow-go/v18` — already an
n1k1 indirect dep, the same library `glue`'s `iceberg_reader` builds on). It
writes a Parquet file of `{id int64, price float64, f0..fN string}` — a *wide*
record where a query wants one numeric column — then exercises the three things a
columnar source is supposed to buy us. All on the same arm64 MacBook, pure Go.

**(1) Projection pushdown — read only the column you want.** Materializing just
`price` vs the whole 14-column file:

| read | time | column-chunk bytes |
|---|---|---|
| `price` only (1 of 14 cols) | 0.6 ms | ~0.02 MB |
| all 14 columns | 50 ms | ~10.3 MB |

→ the projection touches **0.2% of the bytes and is ~80× faster to materialize**
(~137× at 1M rows). This is the "only these fields wanted" wire from § *Pushdown*,
working at the storage layer — and it's the file format doing it, not n1k1.

**(2) Schema + stats from the footer, free.** With **no data pages read**, the
footer yields physical types and per-column statistics:

```
col[0] id     type=INT64      null_count=0
col[1] price  type=DOUBLE     null_count=0 min=0.5 max=999.5
col[2] f0     type=BYTE_ARRAY null_count=0
```

→ "metadata up" (§ *Pushdown*) is real and free: the type picks the fixed-width
kernel, and **`null_count=0` means no validity bitmap → the unmasked kernel**
(§ *the tail problem*). `min`/`max` are exactly the zone-map inputs for later
partition/row-group pruning.

**(3) A parse-free fixed-width column.** Arrow hands back the price column as a
borrowed contiguous `[]float64`; the SUM kernel runs straight over it:

| SUM path | ns/value | vs row-JSON | allocs |
|---|---|---|---|
| Arrow column, kernel only | 0.93 | ~56× | 0 |
| Arrow column, full open+project+decode+sum | 3.0 | ~17× | 2800 / ~18 MB |
| row-JSON baseline (n1k1 today) | 52 | 1× | 0 |

→ the kernel sits at the **same ~0.9 ns/value fixed-width ceiling the § Spike
results measured — but now with NO JSON parse to build the column**, because the
bytes arrived columnar. Even the *full* path (re-open + Snappy-decode + Arrow
materialize + sum, every iteration — worst case) is ~17× the row path.

**Honest caveats:**
- The full path allocates (~18 MB / 2800 allocs/op) — that's Arrow's
  decode/materialization, i.e. the *transpose/copy* the `DESIGN-data.md` columnar
  caveat named. A real integration reuses the `memory.Allocator` and `Release()`s
  each batch to amortize it; the prototype doesn't bother.
- This reads Arrow's *materialized* column — it does **not** yet carry the borrowed
  buffer all the way into a n1k1 op with `@col` markers. That zero-copy,
  end-to-end path is Step 5, and it's where the 0.93 (not 3.0) becomes the number
  that matters.
- Requires the `DESIGN-testing.md` worktree bootstrap (arrow-go pulls the EE
  module graph like everything else under `-tags n1ql`).

**Net:** the columnar-source half of the plan is de-risked. Projection pushdown
and free typed/null metadata work today through arrow-go; the parse-free column
hits the fixed-width ceiling. The remaining work is *integration* (Source
interface widening, then the `@col` in-op path), not *feasibility*.

-------------------------------------------------------
## One-line summary

The user's instinct is right: **`[]byte` is axis-agnostic, so a `Val` can encode
a column, and n1k1's compiler + lifted-buffer + batching machinery are unusually
well-suited to host a vectorized lane.** The work is not "add a column type" —
it's *type inference in the compiler* + *a selection-vector primitive* + *an
opt-in fixed-width encoding*, with SIMD as the final leaf payoff and the
schemaless JSON path always kept as the correctness fallback.
