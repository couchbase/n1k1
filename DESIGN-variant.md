# Design: supporting the VARIANT type

**Status: Phase 0 (read) shipped; write-back started; typed-scalar fidelity next.** The
read-as-JSON MVP is done and validated three ways — the Parquet reader (`records`), full
SQL++ over a Parquet *keyspace* (`glue`), and *shredded* columns. **Q5.2 is resolved: the
users do want write-back**, so we are building past Phase 0. First write-back increment
shipped: `INSERT INTO <x>.parquet SELECT …` now emits a Parquet **VARIANT column** for
object-valued projections (`glue/insert_writer.go`), round-tripping with the read path.
That write-back is *JSON-projection* fidelity (a VARIANT date/decimal read in comes back
out as its JSON type); *typed-scalar* fidelity needs the **`V` sigil carrier** (§4, the
"3D" approach) — Phase 1, in design. Phase-1 shape is decided (**Idea A**, §4.1): the scan
emits a fidelity row as one whole-row **VARIANT object** in the `.`-body slot (register
model), `base` owns cheap `V`→type classification with a registered decode/nav hook (so
`base` stays arrow-go-free / wasm-safe), gated behind an opt-in scan mode and pinned by a
differential test. A critical review of the full pathway (§4.2) found the load-bearing
work is *beyond* the read seams — construction-propagation, the display-vs-write-back
boundary, and canonicalization for compare/hash. Everything shipped so far is still **no
change to the query engine** (code in `records/`, `glue/`, and `./variant/`).

Companion to `DESIGN-data.md` §7 (Iceberg read support) and `DESIGN-exprs.md` (the
byte-oriented `base.Val = []byte` model). VARIANT enters n1k1 through the Iceberg /
Parquet reader — it is really "phase N of the Iceberg story."

---

## 0. Thesis

VARIANT is **JSON's value model plus (a) extra *typed scalars* and (b) a compact
self-describing *binary* encoding.** The extra scalars: `date`, `timestamp` (µs/ns,
±tz), `time`, exact `decimal` (≤38 digits), `binary`, `uuid`, and
width/precision-distinguished numerics (`int8/16/32/64`, `float` vs `double`).
Everything else (null, bool, string, object, array) is just JSON.

Two observations shape everything:

1. **SQL++ / N1QL has no first-class date/decimal/binary/uuid type** — in N1QL those
   are JSON strings/numbers. So a SQL++ *query* over a VARIANT value naturally
   "behaves as the JSON projection of that value" (date → ISO string, decimal →
   number, binary → base64 string), which is what cbq already does. **⇒ most of the
   engine needs no change to *query* VARIANT** — hand it the JSON projection.
2. The only *deltas* over "just decode to JSON" are **type fidelity** (round-trip /
   write-back / VARIANT-native semantics) and **precision** (VARIANT `decimal` is
   exact; JSON number is float64).

**The chosen approach (§3):** carry a VARIANT value as `V<metadata><value>` — a `V`
sigil then the raw Apache Variant bytes — reusing VARIANT's own type tagging instead
of inventing a parallel one, and lazily projecting to JSON only when a query actually
inspects the value. It keeps n1k1's "one `[]byte`, no boxing" model, makes
round-trip/write-back free (copy the bytes), and — crucially — the decode/navigate
machinery is a **ready-made, zero-copy library** already in the dep tree (§2).

---

## 1. What VARIANT actually is (grounding)

- **Two byte streams**: `metadata` + `value`. In Parquet the Variant logical type is a
  group of two binary columns. `metadata` is a **dictionary of field-name strings**
  (deduped, optionally sorted) shared across the value; `value` is the tagged tree.
- **`value` tagging** — first byte: `basic_type = b & 0x03`, `type_info = b >> 2`.
  - `0` primitive — `type_info` selects: null, bool(t/f), int8/16/32/64, float, double,
    decimal4/8/16, date, timestamp(µs, tz/ntz), timestamp(ns, tz/ntz), time, binary,
    (long) string, uuid.
  - `1` short string — `type_info` is the length (0..63); inline UTF-8 follows.
  - `2` object — count + field-id→offset table into `metadata` + child values.
  - `3` array — count + offset table + child values.
- **Superset of JSON**: JSON null/bool/number/string/object/array all map in; the extra
  scalars are the delta.
- **Shredding** (Parquet): a Variant column may be *partially shredded* into typed
  sub-columns (`typed_value` + residual `value`) so engines push predicates/projections
  down to a physical column — the columnar performance story (§6).

*(The exact primitive type-id numbering is enumerated in `arrow-go/v18 parquet/variant`
— `primitiveTypeFromHeader` / `Value.Value()`.)*

---

## 2. The decode library — `arrow-go/v18 parquet/variant`  *(researched, validated)*

n1k1 **already transitively depends on** `github.com/apache/arrow-go/v18 v18.4.1`,
which ships a first-party, cgo-free `parquet/variant` package built on the **same
"views into a backing `[]byte`, no boxing" discipline as n1k1 itself.** (`iceberg-go
v0.4.0` has no Variant yet — the read path is through `pqarrow`/`arrow-go`.) This is
the linchpin: n1k1 does **not** need to fork jsonparser or hand-roll a decoder.

**Unboxed / zero-copy navigation (the path we use):**
- `variant.Value` is a `{value []byte, meta Metadata}` **window**, not a materialized
  tree; `New(meta, value)` / `NewWithMetadata` construct it from bytes.
- `BasicType()` / `Type()` read the header byte; `Bytes()` / `Metadata().Bytes()` hand
  back the raw slices.
- Array/object *iteration* returns **child `Value`s that are subslices into the same
  backing buffer** (`ArrayValue.Value(i)`, `ObjectValue.ValueByKey`/`FieldAt`/`Values`)
  — zero alloc, zero copy, exactly like `base.Parse`/jsonparser subslicing. (Caveat:
  *obtaining* the `ObjectValue`/`ArrayValue` from a `Value` is only exposed via
  `v.Value() any`, which boxes the container struct — one small alloc per container
  *node* descended. Iterating that node's fields/elements is then free. See the
  emitter caveat below.)
- Primitives read directly (`readExact[T]`); strings/dict-keys are `unsafe.String`
  views; binary is a subslice. The **only** allocation in navigation is a one-time
  `make([][]byte, dictSize)` when a `Metadata` is built (and those entries are
  subslices into the metadata bytes — no key copies).
- Opt-in materialization: `Value.Value() any` and `Value.MarshalJSON() []byte`. These
  are the **allocating** paths (see the caveat below); `.Value()` boxing stays
  restrained though — timestamps are `arrow.Timestamp` (int64), *not* `time.Time`;
  dates `Date32`; small decimals as `DecimalValue` structs; only UUID truly allocates.

**Caveat — `MarshalJSON` is NOT a zero-alloc emitter,** so the scan boundary can't use
it per row. Its `json.Marshaler` signature returns a **fresh slice each call**, and
internally it's the naive path (box via `v.Value()`, build an intermediate map/slice,
reflect-`json.Marshal` — its own comment says "simplest, not the most efficient"). No
`AppendJSON(dst)` / `io.Writer` variant exists.

**So the projector lives in a small, dependency-free `./variant/` package** (arrow-go +
stdlib only — reusable as a standalone library): `variant.AppendJSON(dst, v) []byte`
walks objects/arrays via the offset tables in `v.Bytes()` directly (no `v.Value()`
per-node box), resolves keys via the zero-copy `Metadata.KeyAt`, and formats scalars
from bytes — including a **128-bit `Decimal16`** formatter (`variant.AppendDecimal128`,
`big.Int`-free /10 loop, byte-identical to `decimal128.Num.ToString`; needed because
`variant.ParseJSON` encodes *every* fractional number — `3.14159`, `51.5`, `0.1` — as an
exact `Decimal16`, not a `double`). Measured **0 allocs/op** for scalars *and* deep
nested objects with `Decimal16` fields, vs `MarshalJSON`'s 2–147 (`variant/json_test.go`,
`records/variant_append_test.go`); `MarshalJSON` is kept only as a test oracle (and
`AppendJSON` is even *more* correct on one edge — empty array → `[]`, not arrow-go's
`null`). **All typed scalars are now native dst-formatters** —
`date`/`timestamp`(µs/ns, tz/ntz)/`time` via `time.Time.AppendFormat`, `uuid` via a
hand-rolled hex walk, `binary` via `base64.AppendEncode` — each **byte-identical** to
`MarshalJSON` and **0 allocs/op** (`variant.TestAppendJSONTypedScalars`). The
`MarshalJSON` fallback now only guards a genuinely unknown primitive tag.

**Validated end-to-end** by `records/variant_parquet_test.go` (two tests):
- A VARIANT-column Parquet file, read back via `pqarrow.ReadTable`, surfaces the
  column **directly as `*extensions.VariantArray`** — `col :=
  tbl.Column(0).Data().Chunk(0)`, `va.Value(i)` → a `variant.Value` view. No manual
  sub-array decomposition.
- The `(metadata, value)` slices round-trip **intact**, and `variant.New(meta, value)`
  rebuilds a working view from just those two `[]byte` — the `V<meta><value>`
  carry-and-rebuild story, proven.
- Navigation is **zero-copy, and holds at depth**: a 4-level
  `order → customer → address → geo → lat` chain plus an `orderlines`
  array-of-subobjects with a nested `tags` array round-trips byte-identically, and a
  4-deep leaf's bytes still alias the top-level backing.
- `Type()` is preserved through the round-trip (fidelity) while `MarshalJSON()` gives
  the JSON projection. Observed: `date Date32(20194)` → `"2025-04-16"`; `decimal16`
  scale 2 → `12345678912345678.90` (exact). A date buried 3 levels deep keeps
  `Type()==Date` and projects to `"2025-04-16"` — **fidelity survives nesting.**

---

## 3. Where VARIANT enters n1k1 (the boundary)

Precisely one ingestion point, already mapped by the Iceberg work:

- **`records/parquet.go` → `appendArrowValueJSON(dst, arr, i)`** — a type switch over
  Arrow array types appending each cell's bytes into `dst` (bool/int/uint/float/string
  today; `fastRenderable` gates it, others fall to a `json.Marshal` slow path). The
  VARIANT column arrives here as `*extensions.VariantArray` (§2); a
  **`case *extensions.VariantArray`** decodes `(metadata, value)` → n1k1 bytes. **This
  is the entire ingestion surface.** What it emits — JSON (MVP) or `V<...>` — is the
  §4 choice.

Downstream, values flow as `base.Val = []byte`:
- **`base.Parse` (base/compare.go)** is the *single* wrapper over `buger/jsonparser` +
  first-byte dispatch — the one seam that would learn the `V` sigil.
- **`base.ValKind` / `ParseTypeToValType`** classify a value; `ValType` already
  reserves `ValTypeUnknown // Ex: BINARY` — a non-JSON tag precedent.
- **Output**: `ValComparer.CanonicalJSON` / `WriteJSON` (base/canonical.go) is the JSON
  emitter; a VARIANT *writer* would be a sibling.

---

## 4. The chosen representation: a single `V` sigil

Carry a VARIANT value as **`V<metadata><value>`** — the `V` sigil, then the raw Apache
Variant `metadata` + `value` bytes (with a length delimiter). First-byte dispatch sees
`V` and reads *VARIANT's own* first byte (`basic_type`/`type_info`) to classify —
**reuse the type tagging "many smart folks already figured out"; don't build a parallel
table.** Pair it with **lazy** projection: carry the bytes through untouched, and
JSON-ify only when a query actually inspects the value.

**Why this is the pick:**
- **One `[]byte`, no boxing** — the founding constraint survives; a value is still a
  single reused byte slice.
- **Round-trip / write-back is free and lossless** — a pass-through
  (`SELECT v FROM t WHERE <non-v-pred>` → write VARIANT) copies the bytes verbatim,
  zero decode/encode. Fidelity is automatic (the value *is* the source bytes).
- **The navigator already exists and is zero-copy** — §2's `variant.Value` view API
  walks a `V` tail with the same subslice-into-backing discipline as jsonparser walks
  JSON. This is what makes `V` viable rather than a "parallel machinery" burden.
- **The projection logic is ready-made** — `variant.Value.MarshalJSON()` defines the
  JSON form (and is a handy correctness oracle), but it allocates a fresh slice per
  call (§2 caveat), so the zero-garbage hot path is a small hand-rolled
  `appendVariantJSON(dst, v)` over the unboxed view API, appending into the reused
  buffer — not `MarshalJSON` itself.

**The two catches, and how they resolve:**

1. **Carry the metadata dictionary.** A Variant `value` references field names by ID
   into a separate `metadata` dict (shared per column for compression). n1k1 rows are
   independent `[]byte`, so a self-contained value must carry **both** streams —
   `V<len><metadata><value>`. That re-inlines a row's field names (small, but real).
   The library models exactly this (`NewWithMetadata(meta, value)`), and §2 proved the
   two slices round-trip and rebuild. *Accepted cost.*
2. **Inspection needs VARIANT navigation, not JSON navigation** — a `V` tail can't be
   walked by jsonparser/`ValPathGet`/`ValComparer` directly. Resolved by
   **lazy-decoding at the `base.Parse` seam**: teach `base.Parse`/`ValKind` to detect
   `V` and hand back the projected JSON (via `MarshalJSON`, or a typed-JSON form). Most
   exprs then "just work." Two residual costs to design around: (a) direct `v[0]` peek
   sites (`ValIsString`, first-byte checks) must learn `V` (Q5.5); (b) `Parse` runs in
   hot loops, so a value touched N times re-decodes N times — mitigated by decoding
   eagerly at the scan boundary for inspection-heavy queries (the read-as-JSON MVP
   already does exactly this).

**Consequence to keep in mind:** `V` tags *every* VARIANT-sourced value opaque —
including a plain string/number/object that JSON represents perfectly — until it's
inspected. That's the price of "VARIANT-native transit"; it's paid back by free
pass-through and trivial write-back.

### Alternatives considered (condensed)

- **Read-as-JSON (the MVP, retained as Phase 0).** Decode VARIANT → plain JSON right
  in `appendArrowValueJSON` (`MarshalJSON`), lose type identity + decimal precision,
  query as JSON. ~Zero engine change, read-only. Not an *alternative* to `V` so much
  as its first step — ship this first; it may be enough for a long while (Q5.2).
- **Per-subtype typed-JSON sigils** (`d"…"`, `x"…"`, …). Annotate only the non-JSON
  scalars over their JSON form; JSON-native content stays unprefixed, so fewer peek
  sites break, and it's best for *inspection-heavy* queries (decode once at scan).
  **Subsumed:** its whole benefit is the JSON projection, which `V` + lazy-decode also
  produces — without a parallel per-subtype table or a deeper jsonparser fork for
  sigils-inside-nested-structures. If profiling later shows inspection-heavy VARIANT
  workloads dominate, this is the natural specialization of the lazy step (decode `V`
  → typed-JSON instead of plain JSON).
- **Opaque binary + a parallel expr set.** Rejected — duplicates the whole expr
  library and splits every code path in two.

### Worked example — where the VARIANT-awareness actually lives

Trace `WHERE order.customer.rating > 10`, with `order` a VARIANT value carried in its
register as `V<meta><value>`. The question that motivates this: does every
`ValIsNumber`/`ValIsString`/… need a `V` branch, or does something smarter absorb it?

The happy answer is that n1k1's value inspection **funnels through a tiny set of
`jsonparser`-wrapping seams**, so VARIANT-awareness plugs into *those*, not the hundreds
of exprs:

| seam | today | `V`-aware behavior |
|---|---|---|
| `base.ValPathGet(val, path, out)` | `jsonparser.Get(val, path…)` | if `val[0]=='V'`, walk the path via the zero-copy `variant.Value` view API; project the reached **scalar** leaf into `out` (the reused buffer) |
| `base.Parse(v)` / `ValKind` | `jsonparser.Get(v)` + first-byte | if `v[0]=='V'`, classify from VARIANT's tag byte (`basic_type`→`ValType`) — **alloc-free, no decode** |
| `ValComparer.Compare(a,b)` | `jsonparser.Get(a)`,`(b)` | if an operand is `V`, project it (rare: whole-value compare) |

Step by step:

1. **`ExprLabelPath`** reads `order` (`V<…>`) from its slot and calls
   `base.ValPathGet(order, ["customer","rating"], lzValPre)`. `lzValPre` is already a
   `// <== varLift` reused buffer, so any projection is **alloc-free after warmup**.
2. **ValPathGet (the one hot-path seam)** sees the `V`, navigates `customer → rating`
   through the view API (offset-table walk, **zero-copy subslices** — no dict-wide
   decode), and projects just the `rating` **scalar** into `lzValPre`
   (`strconv.AppendInt(lzValPre[:0], n, 10)` → `10`). It returns **plain JSON**.
3. **`ExprCmp`** now compares plain JSON `10` to the constant `10` — ordinary number
   compare, **no VARIANT code**. The `order` register value is **still `V<…>`,
   untouched**, so a sibling `SELECT order` round-trips it verbatim.

So the whole filter touches VARIANT in **exactly one place** (ValPathGet), and the
result is unboxed, efficient, and round-trip-safe. Generalizing:

- **`ValIsNumber(order.customer.rating)` needs no change** — the path leaf is already
  projected to JSON by ValPathGet, so `ValIsNumber` sees `10`. And
  `ValIsNumber(order)` on a *whole* `V` value works too, because it's built on
  `base.Parse`, which classifies `V` from its tag byte. **Fix `Parse`, get the whole
  Parse-based `is_*` family for free** (`ExprIsType` → `ValKind`+`Parse`+`ParseType`).
- The exceptions are the few functions that peek `v[0]` **directly** instead of via
  `Parse` — e.g. `ValIsString` (`v[0]=='"'`). Those are a small, enumerable set
  (Q5.5): reroute through the classifier, or give them a one-line `V` branch.

**The one honest wrinkle — the metadata dict.** Object navigation must resolve field
names through the Variant `metadata` dictionary, and `variant.New` parses it with a
`make([][]byte, dictSize)` — one small alloc per navigation, i.e. per row on a filter.
Mitigation: in Parquet the dict is typically **shared per column batch**, so the scan
can parse the `Metadata` **once per batch** and navigate each row's `value` against it,
only materializing a self-contained `V<meta><value>` when a value *escapes* the batch
(spill, join build-side, output). That keeps the steady-state filter path allocation-free
at the cost of a shared-dict lifetime to manage.

### 4.1 Phase-1 chosen shape (Idea A): whole-row VARIANT object, opt-in

The scan renders a *fidelity* row as **one `V`-object slot**, not a JSON-object slot.
`records/parquet.go` `appendRecordsNDJSON` → **`appendRecordsVals`**: it emits the row
body as JSON `{…}` for a JSON-only batch (the fast path, unchanged) or as
`V<metadata><value>` for a batch carrying VARIANT columns. **One row = one `.`-body slot**,
so n1k1's "a document is a single navigable value" contract is preserved — no
per-column-slot explosion. (That explosion is *alternative B*: one slot per column, which
makes a projected VARIANT column zero-copy / zero-re-encode but detonates the doc-is-one-value
contract — kept only as a perf escape hatch if the per-row `V`-object build proves too
costly.) `base` owns cheap `V` classification (a byte-tag → `ValType` table, **no
arrow-go**); decode / navigate / project is a **registered hook** `variant/` installs,
keeping `base` arrow-go-free and wasm-safe. `V`-emission is **opt-in** (a scan/session
mode); default stays Phase-0 read-as-JSON. A **differential test** asserts a VARIANT query
returns byte-identical result rows with the mode on vs off — pinning "query behavior
provably unchanged."

### 4.2 Critical review — does the whole pathway hold together?

The read worked-example above implies VARIANT-awareness lives in ~3 seams. Tracing the
headline *write-back* — `INSERT INTO out.parquet (VALUE self) SELECT s.order FROM src s`
with fidelity on — shows that is the **easy half**; the full seam list is larger:

| seam | `V` handling | free? |
|---|---|---|
| **transit** — register moves, JOIN, UNION, spill (`ValsEncode`/`Decode` is length-prefixed per slot, format-agnostic) | bytes ride through untouched | ✅ free (verify no path assumes JSON) |
| **classify** — `base.Parse`/`ValKind` + direct `v[0]` peek sites (`ValTruthy`, `ValIsString`, kind checks, Q5.5) | byte-tag → `ValType` of the **JSON projection** (date/ts/uuid/binary→String; int/decimal/float→Number; object→Object…) so classify agrees with output & collation | needs a small table |
| **navigate** — `ValPathGet`, array `ValElement` | variant view walk (**unboxed offset-table**, not `v.Value()`); scalar leaf → project into reused `valOut`; container leaf → reframe as self-contained `V<meta><subvalue>` | 1 hook |
| **compare / hash** — `ValComparer.Compare`, GROUP/DISTINCT/JOIN hashing | **must project to CANONICAL JSON**, *not* hash raw `V` bytes | ⚠ correctness |
| **construct** — object/array construction with a `V` member | `SELECT s.order` builds `{"order":<order>}` — a construction; must **propagate `V`** into a `V`-object or write-back goes lossy | ⚠ **the gap** |
| **serialize** — row→bytes for output / INSERT `OnRow(rowBytes)` | must emit `V`-objects, else the writer never sees `V` | needs code |
| **output vs write-back** — `ConvertVals.Convert` → `value.Value`; `WriteJSON` | same result row must **render JSON for a SELECT** yet **preserve `V` for the writer** | ⚠ tension |
| **write** — parquet VARIANT-column appender | detect `V` → `variant.New(meta,value)` → `VariantBuilder.Append` (lossless), *not* Phase-2a `WriteJSON`→`ParseJSON` (lossy) | needs code |

Three findings the original worked example missed:

1. **Construction must propagate `V`.** A `SELECT` always wraps projections in a result
   object, so even the "pure pass-through" write-back goes through object construction. If
   construction projects `V`→JSON, write-back is lossy — so the object/array constructors
   must build a `V`-object when a member is `V`. This is the biggest surface beyond reads.
   (Corollary scope: an expr that *transforms* content — string ops, arithmetic,
   `OBJECT_*` mutators — legitimately projects to JSON and is lossy; the value changed, so
   typed-scalar identity is moot. Fidelity = pass-through **and** structural re-assembly,
   not transformation.)
2. **The display-vs-write-back tension.** The one `value.Value` boundary feeds *both* the
   SELECT result (wants JSON) and the INSERT writer (wants raw `V`). So `Convert` can't
   just project. Two resolutions to pick between before coding: **(a)** a boundary
   `VariantValue` whose `MarshalJSON` projects but which the writer type-asserts to recover
   raw bytes; **(b)** the INSERT path consumes engine `V`-slots *before* `value.Value`
   conversion. (b) avoids a new boxed value type but reworks `InsertRun`'s source.
3. **Compare/hash must canonicalize.** Two variants that project to equal JSON can differ
   in bytes (metadata order, encoding choices). Hashing raw `V` bytes would split equal
   GROUP BY / DISTINCT groups and misorder — a correctness bug. Compare/hash must run on
   the canonical JSON projection. (Decimal compares on the float64 projection, matching
   cbq — Q5.3.)

**Verdict:** the pathway holds, but Phase 1 is materially bigger than "teach
`Parse`/`ValPathGet` about `V`." Load-bearing additions: construction-propagation, the
`value.Value` boundary decision, and canonicalization. The read-only inspection story is
the small part.

### 4.3 Design-principle check (hot-path allocs, no boxing)

- **No boxing in the byte lane.** Navigation/projection use the offset-table byte walk
  (same as `variant.AppendJSON`), never `v.Value()` (which boxes a container per node).
  The only box is the `value.Value` at the `Convert` boundary — already boxed today, not
  new hot-lane boxing. ✅
- **Transit is alloc-free** (bytes copy through the reused slot/`valOut` buffers) and
  **scalar-leaf projection is alloc-free after warmup** (into the reused `lzValPre`), as
  today. ✅
**Measured (benchmarks, `benchmem`, arm64):**

| what | ns/op | B/op | allocs/op |
|---|--:|--:|--:|
| `base.SplitVariantEnvelope` (split) | 3.0 | 0 | 0 |
| `base.VariantValType` (classify) | 3.8 | 0 | 0 |
| `variant.AppendJSON` (deep obj) | 263 | 0 | 0 |
| — vs arrow-go `MarshalJSON` | 11858 | 10259 | 131 |
| `VariantPathGet` scalar leaf (3-deep) | 380 | 608 | 4 |
| `VariantPathGet` container leaf (reframe) | 245 | 416 | 2 |
| scan render, **Phase-0 JSON** (per 256-row batch) | 125µs | 74KB | 257 |
| scan render, **fidelity `V`** (per 256-row batch) | 362µs | 651KB | 4708 |

So the arrow-go-free carrier primitives are **free and zero-alloc**, the projector is
**~45× faster than `MarshalJSON` and zero-alloc**, and the opt-in fidelity render costs
**~2.9× time / ~9× memory** over Phase-0 (~1.4µs, ~2.5KB, ~18 allocs per row vs ~0.5µs,
~289B, ~1 alloc). Reasonable for opt-in; not yet fit to be the always-on default.

**A pathological O(N²) was found and fixed here:** reusing one `variant.Builder` across
rows via `Reset()` blew up to **221MB / 256-row batch** because arrow-go's `Builder.Reset`
clears the buffer/dictionary but *not* its internal `totalDictSize` accumulator, so each
row's `Build()` sized metadata by the running SUM of every row's dictionary. A **fresh
builder per row** makes it linear (the numbers above). (Lesson: don't reuse an arrow-go
variant `Builder` across `Build()`s.)

**Remaining alloc sources on the fidelity path** (the ~18 allocs/row, all opt-in — plain
queries pay nothing; optimize only if fidelity goes default):
1. *Per-row row-object build* — a fresh `variant.Builder` + metadata build per row.
2. *Per-scalar-column re-encode* — `appendArrowValueJSON` → `av.ParseJSONBytes` allocates
   a `Builder` + a `json.Decoder` per scalar column per row.
3. *Metadata parse per navigation* — `variant.New`/`va.Value` does `make([][]byte,
   dictSize)` per call, i.e. per row on a filter.
   Candidate mitigations (deferred): share the batch's column-name `Metadata` across rows;
   a scalar re-encoder that avoids the JSON round-trip; a reusable decoder. Nested-variant
   cells complicate dict sharing (their field names vary per row).

Net: transit + classification + scalar-leaf projection honor "no hot-path allocs"; the
scan-build and per-navigation meta-parse are the measured cost centres, each with a
mitigation to pursue *if* the fidelity mode becomes the default.

### 4.4 First-byte safety (Q5.6, confirmed)

`V` (0x56) is outside JSON's value-start alphabet (`" { [ - digits t f n` + whitespace),
so `v[0]=='V'` is an unambiguous non-JSON signal; a JSON string starting with "V" begins
with `"`, no collision. The `metadata`/`value` split needs a length delimiter
(`V<uvarint len(meta)><meta><value>`); the bytes survive `append` / `[:0]` reuse (they're
just bytes). Non-VARIANT typed-scalar Parquet columns (a bare `date`/`decimal` column, no
VARIANT) stay Phase-0 lossy for now; whole-row-`V` could later cover them too, since it
captures every typed scalar.

---

## 5. Open questions

- **Q5.1 Decode timing / eager-vs-lazy.** `V` favors lazy (free pass-through); but
  inspection-heavy queries want a single eager decode at the scan boundary. Likely
  both: `V` on the wire, and the scan `case` decodes eagerly to JSON for the MVP, lazily
  to `V` once fidelity/write-back lands. Sketch the expected VARIANT workload
  (pass-through/write-back vs inspection) before finalizing.
- **Q5.2 Is fidelity even needed soon?** §7 Iceberg is **read-only**. If n1k1 never
  *writes* VARIANT, the read-as-JSON MVP may suffice indefinitely; the `V` carrier
  earns its cost only with write-back OR VARIANT-native predicates. **Gates whether we
  go past Phase 0 at all.**
- **Q5.3 Decimal precision.** Exact 128-bit vs float64. Arithmetic degrades to float64
  (matches cbq/N1QL — document it); the `V` bytes preserve the exact value for
  round-trip. "Lossy-in-arithmetic, lossless-in-transit" — the same bargain cbq makes.
- **Q5.4 Collation.** N1QL order is null < bool < number < string < array < object.
  A `V` value collates on its JSON projection (date=string, decimal=number — matches
  cbq); VARIANT-native ordering only if we ever expose VARIANT-native compares.
  `ValComparer.CompareWithType` is the seam.
- **Q5.5 Direct `v[0]` peek sites.** The Parse-based `is_*` family works for free once
  `Parse` classifies `V` (see the §4 worked example); the residue is the few functions
  that read the first byte *directly* (`ValIsString`, kind checks) — enumerate them,
  reroute through the classifier or add a one-line `V` branch. How many, how hot?
- **Q5.6 `V` framing & safety.** `V` must sit outside JSON's first-byte alphabet
  (`" { [ - 0-9 t f n`); a length delimiter separates `metadata` from `value`; ensure
  the bytes survive `append` / `bufPre[:0]` reuse (they're just bytes).
- **Q5.7 Type predicates / builders / VARIANT-native accessors.** Does SQL++ grow
  `IS_DATE` / `TO_VARIANT` / typed accessors, or is VARIANT fully transparent? cbq has
  none — n1k1-native surface to design or deliberately omit. **Broader direction (and a
  reason the carrier is worthwhile independent of write-back):** once a `V` value flows
  through the layers with its *typed* bytes intact (Phase-1 steps 1–2, shipped), new
  builtins can operate on that fidelity that JSON simply can't express — e.g. an
  exact 128-bit `DECIMAL_COMPARE(a,b)` / `DECIMAL_ADD` that avoids the float64 collapse
  (Q5.3), true `DATE`/`TIMESTAMP` accessors, `IS_DATE`/`IS_DECIMAL`, or unit-preserving
  temporal math. These would dispatch on the `V` tag (`base.VariantValType`) and read the
  typed leaf via the nav hook — the same seams already in place. So the read-side carrier
  is a foundation for VARIANT-native computation, not only for lossless write-back.
- **Q5.8 Schema advertising.** How does a VARIANT column show itself in `.keyspaces` /
  column metadata so users know a field is VARIANT vs plain JSON?

---

## 6. Shredded VARIANT & pushdown (reads DONE; pushdown later)

Parquet shredding splits a Variant into typed sub-columns: the storage struct grows a
`typed_value` field (a mirror of the shred schema) alongside the residual `value`, so a
row's data is split across the shredded columns and the leftover binary.

**Reading shredded VARIANT needs no n1k1 code — it already works.** arrow-go's pqarrow
reader reconstructs a shredded Parquet file back into a single `*extensions.VariantArray`
(verified against arrow-go's own `pqarrow/variant_test.go`), and `VariantArray.Value(i)`
routes through a `shreddedVariantReader` that *coalesces* the `typed_value` sub-columns
and the residual `value` bytes into one complete `variant.Value`. Phase-0's scan `case`
(`a.Value(i)` → `variant.AppendJSON`) is therefore oblivious to the physical layout: a
shredded column projects to exactly the same JSON as a non-shredded one, so the whole
SQL++ path is unchanged. (The only lost property is zero-alloc pass-through — coalescing
a shredded row reconstructs a fresh `variant.Value`; non-shredded rows stay zero-alloc.)
Proven end-to-end by `glue.TestVariantParquetShreddedKeyspaceQuery`: a genuinely-shredded
keyspace (asserted `IsShredded()`) answers queries that touch both the shredded subfields
(`customer.name`, `customer.tier`) and the residual ones (`customer.address`, `total`,
`orderlines`).

**Pushdown is the remaining work (perf, not correctness).** n1k1's Iceberg path already
does **projection + predicate + partition pushdown** into the scan (DESIGN-data §7 /
the `records.ScanPredicate` sidecar). A predicate on a shredded subfield
(`WHERE v.customer.tier = 'gold'`) could push to the shredded physical column exactly
like a top-level column does today — reusing that machinery, not new machinery. The
real performance payoff, and a reason to keep VARIANT *inside* the Iceberg pushdown
framework rather than beside it.

---

## 7. Phasing

- **Phase 0 — read as JSON (MVP). DONE + validated.** `records/parquet.go`
  `appendArrowValueJSON` has a `case *extensions.VariantArray` emitting the JSON
  projection into the reused `dst` via `variant.AppendJSON` (§2), and
  `*extensions.VariantArray` is in `fastRenderable` so a VARIANT batch stays on the
  zero-garbage fast path (importing `extensions` also registers `parquet.variant`, so
  pqarrow surfaces the column as `*extensions.VariantArray`). Read-only, lossy-to-JSON,
  ~zero engine change. Validated at three levels: the reader
  (`records.TestParquetReaderRendersVariantColumn`), full SQL++ over a keyspace
  (`glue.TestVariantParquetKeyspaceQuery`), and shredded columns (§6). All typed
  scalars (`date`/`timestamp`/`time`/`uuid`/`binary`) now project via native,
  byte-identical, zero-alloc dst-formatters — no `MarshalJSON` in the read path (§2).
- **Phase 2a — JSON-projection write-back. DONE.** `INSERT INTO <x>.parquet SELECT …`
  emits a Parquet VARIANT column when a projected field is an object
  (`glue/insert_writer.go`: `inferParquetKind` OBJECT → `extensions.NewDefaultVariantType`,
  a per-row appender that `WriteJSON`s the value into a reused buffer then
  `av.ParseJSONBytes` → `VariantBuilder.Append`; a NULL/MISSING value → a Parquet-NULL
  cell). arrow's `RecordBuilder` yields the `*extensions.VariantBuilder` for a
  `VariantType` field automatically (`CustomExtensionBuilder`), and the streaming
  `pqarrow.FileWriter` writes the extension column. Validated by
  `glue.TestInsertVariantColumnRoundTrip` (INSERT objects → read back → nested nav /
  deep filter / array elem / null row). It's an encode **boundary**, so not zero-alloc
  (reuses the JSON buffer; per-row decode+build is inherent). Fidelity is the JSON
  projection only — see Phase 1.
- **Phase 1 — the `V` carrier (typed-scalar fidelity). Read side SHIPPED (Idea A, §4.1).**
  So a VARIANT date/decimal/uuid read in keeps its typed identity through the engine
  (Phase-2a loses that — it re-encodes the JSON projection). Progress:
  - **(1) DONE** — `base` carrier framing (`SigilVariant`, envelope), `V`→ValType
    classification (arrow-go-free), and the projection/nav hooks; the `variant`-backed
    hook bridge in `records`. Additive, zero behavioral change.
  - **(2) DONE** — opt-in scan mode (`records.VariantFidelity`) emits whole-row `V`
    objects; `ValPathGet`/`ValKind`/`ValsSelfObject` + `Convert` are `V`-aware; pinned by
    a differential test (identical results on vs off) + a scan-emits-`V` engagement test.
    Benchmarked (§4.3): opt-in fidelity render ≈ 2.9× time / 9× memory vs Phase-0 —
    reasonable for opt-in, not yet default-worthy.
  - **(3) NOT DONE — lossless write-back.** Analysis (§4.2) shows this is a large rework
    (lossy projection at three pipeline points; `base` can't build `V`-objects), not a
    clean increment — deferred pending a scope decision. Also deferred: compare/hash
    canonicalization for GROUP/ORDER on whole-`V` values, and whole-`V` `Parse`-based type
    predicates (`IS OBJECT` etc.; `ValKind`-based `IS VALUED`/`NULL`/`MISSING` work).
  Note the read-side carrier already enables **VARIANT-native accessors** (exact-decimal
  compare, typed date math) independent of write-back — see Q5.7.
- **Phase 2b — shredded pushdown.** Predicate/projection pushdown into shredded
  subfields (§6), reusing the Iceberg `ScanPredicate` sidecar. (Reading shredded VARIANT
  already works with zero n1k1 code — §6; this is the pushdown perf win.)

---

## 8. Table of contents

- [0. Thesis](#0-thesis)
- [1. What VARIANT actually is](#1-what-variant-actually-is-grounding)
- [2. The decode library (arrow-go parquet/variant)](#2-the-decode-library--arrow-gov18-parquetvariant--researched-validated)
- [3. Where VARIANT enters n1k1](#3-where-variant-enters-n1k1-the-boundary)
- [4. The chosen representation: a single `V` sigil](#4-the-chosen-representation-a-single-v-sigil)
  - [Alternatives considered](#alternatives-considered-condensed)
  - [Worked example — where the VARIANT-awareness lives](#worked-example--where-the-variant-awareness-actually-lives)
  - [4.1 Phase-1 chosen shape (Idea A)](#41-phase-1-chosen-shape-idea-a-whole-row-variant-object-opt-in)
  - [4.2 Critical review — does the whole pathway hold together?](#42-critical-review--does-the-whole-pathway-hold-together)
  - [4.3 Design-principle check (hot-path allocs, no boxing)](#43-design-principle-check-hot-path-allocs-no-boxing)
  - [4.4 First-byte safety](#44-first-byte-safety-q56-confirmed)
- [5. Open questions](#5-open-questions)
- [6. Shredded VARIANT & pushdown](#6-shredded-variant--pushdown-reads-done-pushdown-later)
- [7. Phasing](#7-phasing)
- [9. Research backlog](#9-research-backlog)

## 9. Research backlog

- [x] Variant spec primitive type-ids + physical encodings — enumerated in
      `arrow-go/v18 parquet/variant` (§1/§2).
- [x] Survey Go decoders — `arrow-go/v18 parquet/variant` is a ready-made, cgo-free,
      **unboxed/zero-copy** decoder, already a transitive dep (§2).
- [x] End-to-end read: `records/variant_parquet_test.go` — column surfaces as
      `*extensions.VariantArray`, zero-copy navigation (incl. deeply nested),
      byte-intact round-trip + `variant.New` rebuild, concrete JSON projections (§2).
- [x] Build the zero-alloc emitter as a reusable package — `./variant/`
      (`variant.AppendJSON` + `variant.AppendDecimal128`, arrow-go+stdlib only). **0
      allocs/op** for scalars AND deep nested objects incl. `Decimal16` (byte-level
      object/array walk, no `v.Value()` box; 128-bit decimal formatter byte-identical to
      `decimal128.Num.ToString`). Unit-tested in `variant/json_test.go`; integration-
      tested on Parquet-read values in `records/variant_append_test.go`.
- [x] **Phase 0**: `variant.AppendJSON` wired into `records/parquet.go`
      `appendArrowValueJSON` (+ `fastRenderable`); end-to-end test
      `records.TestParquetReaderRendersVariantColumn` (Parquet VARIANT column →
      `newParquetSource` → JSON records).
- [x] Exercise a VARIANT column through a full SQL++ query over a Parquet *keyspace*
      (glue-level), not just the `records` reader — `glue.TestVariantParquetKeyspaceQuery`
      (nested projection, string/decimal filters, array-element nav).
- [x] Handle the shredded `typed_value` column shape — turns out to need **zero n1k1
      code**: pqarrow reads a shredded file back as one `*extensions.VariantArray` and
      `.Value(i)` coalesces `typed_value` + residual `value`, so Phase-0 is oblivious
      (§6). Proven by `glue.TestVariantParquetShreddedKeyspaceQuery` (genuinely shredded,
      `IsShredded()` asserted; queries hit both shredded and residual subfields).
- [x] Finish the fallback types: `date`/`timestamp`/`time`/`uuid`/`binary` dst-formatters
      in `./variant/` — native, **byte-identical** to `MarshalJSON`, **0 allocs/op**
      (`date`/`timestamp`/`time` via `time.Time.AppendFormat`; `uuid` hand-rolled hex;
      `binary` via `base64.AppendEncode`). `variant.TestAppendJSONTypedScalars` +
      zero-alloc coverage. Read path no longer calls `MarshalJSON` for any known type.
- [x] Decide whether write-back is a real requirement (Q5.2) — **YES, users confirmed.**
      Building past Phase 0.
- [x] **Phase 2a**: JSON-projection write-back — `INSERT INTO <x>.parquet SELECT …`
      emits a Parquet VARIANT column for object-valued projections
      (`glue/insert_writer.go`); `glue.TestInsertVariantColumnRoundTrip` proves the
      write→read loop (nested nav, deep filter, array elem, null row).
- [ ] **Phase 1 (NEXT)**: the `V` carrier for typed-scalar fidelity — emit `V<meta><value>`
      at the scan; classify/project `V` at `base.Parse`/`ValKind` via a REGISTERED hook so
      `base` stays arrow-go-free (wasm-safe); writer copies bytes for pass-through `V`.
      Enumerate the direct `v[0]`-peek sites (Q5.5); measure the pure-JSON hot path is
      unaffected (Q5.6).
