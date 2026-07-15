# Design: supporting the VARIANT type

**Status: design research, converging.** Chosen direction: a **single `V` sigil**
carrying the value's tail as raw Apache VARIANT binary (the "3D" approach below), with
a **read-as-JSON MVP** as the first shippable step. No engine code yet; the ingestion
boundary and the library situation are validated end-to-end (see §2, §3).

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
- Array/object navigation returns **child `Value`s that are subslices into the same
  backing buffer** (`ArrayValue.Value(i)`, `ObjectValue.ValueByKey`/`FieldAt`/`Values`)
  — zero alloc, zero copy, exactly like `base.Parse`/jsonparser subslicing.
- Primitives read directly (`readExact[T]`); strings/dict-keys are `unsafe.String`
  views; binary is a subslice. The **only** allocation in navigation is a one-time
  `make([][]byte, dictSize)` when a `Metadata` is built (and those entries are
  subslices into the metadata bytes — no key copies).
- Opt-in materialization: `Value.Value() any` and `Value.MarshalJSON() []byte`. These
  are the **allocating** paths (see the caveat below); `.Value()` boxing stays
  restrained though — timestamps are `arrow.Timestamp` (int64), *not* `time.Time`;
  dates `Date32`; small decimals as `DecimalValue` structs; only UUID truly allocates.

**Caveat — `MarshalJSON` is NOT a zero-alloc / append-style emitter.** It has the
`json.Marshaler` signature `() ([]byte, error)`, so it returns a **fresh slice every
call** — it cannot append into a caller-preallocated `[]byte`. Worse, internally it is
the naive path: `Value.MarshalJSON` boxes via `v.Value()` then calls reflection-based
`json.Marshal`; `ObjectValue.MarshalJSON` literally `make(map[string]Value)` +
`json.Marshal(mapping)` (its own comment: "naive… not the most efficient… simplest");
`ArrayValue.MarshalJSON` `slices.Collect` + `json.Marshal`. So a nested value is
boxing + intermediate maps/slices + reflect-marshal, per row. There is **no**
`AppendJSON(dst)` / `io.Writer` variant in the package.
⇒ For n1k1's zero-garbage scan boundary, **do not** call `MarshalJSON` per row.
Instead hand-roll a small `appendVariantJSON(dst []byte, v variant.Value) []byte` over
the **unboxed view API** (`BasicType`/`Type` switch → `strconv.Append*` for scalars →
recursive object/array walk, each subslice appended into `dst`) — the same
append-into-reused-buffer pattern `appendArrowValueJSON` and base's canonical emitters
already use. `MarshalJSON` is fine as a *correctness oracle* / first cut, not the hot path.

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
- **Q5.5 Direct `v[0]` peek sites.** Enumerate the ops that read the first byte
  directly (`ValIsString`, kind checks) rather than through `base.Parse` — each must
  learn the `V` sigil. How many, how hot?
- **Q5.6 `V` framing & safety.** `V` must sit outside JSON's first-byte alphabet
  (`" { [ - 0-9 t f n`); a length delimiter separates `metadata` from `value`; ensure
  the bytes survive `append` / `bufPre[:0]` reuse (they're just bytes).
- **Q5.7 Type predicates / builders.** Does SQL++ grow `IS_DATE` / `TO_VARIANT` /
  typed accessors, or is VARIANT fully transparent? cbq has none — n1k1-native surface
  to design or deliberately omit.
- **Q5.8 Schema advertising.** How does a VARIANT column show itself in `.keyspaces` /
  column metadata so users know a field is VARIANT vs plain JSON?

---

## 6. Shredded VARIANT & pushdown (later; note the synergy)

Parquet shredding splits a Variant into typed sub-columns. n1k1's Iceberg path already
does **projection + predicate + partition pushdown** into the scan (DESIGN-data §7 /
the `records.ScanPredicate` sidecar). A predicate on a shredded subfield
(`WHERE v.customer.tier = 'gold'`) could push to the shredded physical column exactly
like a top-level column does today — reusing that machinery, not new machinery. The
real performance payoff, and a reason to keep VARIANT *inside* the Iceberg pushdown
framework rather than beside it.

---

## 7. Phasing

- **Phase 0 — read as JSON (MVP).** A `case *extensions.VariantArray` in
  `appendArrowValueJSON` that emits the JSON projection into the reused `dst` via a
  hand-rolled `appendVariantJSON` over the view API (using `MarshalJSON` only as the
  correctness oracle in tests — §2 caveat). Read-only, lossy-to-JSON, ~zero engine
  change downstream, and zero-garbage. Unblocks querying Iceberg VARIANT columns.
  *Do this first; it may be enough (Q5.2).*
- **Phase 1 — the `V` carrier (fidelity).** Only if write-back or type-fidelity is
  wanted. Emit `V<metadata><value>` from the scan `case`; teach `base.Parse`/`ValKind`
  to detect `V` and lazily project (and the peek sites of Q5.5); JSON output decodes
  `V` → JSON. Differential-test the projection against cbq so query behavior is
  provably unchanged.
- **Phase 2 — VARIANT writer + shredded pushdown.** A Variant-binary encoder for
  write-back (trivial for pass-through `V` values — copy the bytes), and
  predicate/projection pushdown into shredded subfields (§6).

---

## 8. Table of contents

- [0. Thesis](#0-thesis)
- [1. What VARIANT actually is](#1-what-variant-actually-is-grounding)
- [2. The decode library (arrow-go parquet/variant)](#2-the-decode-library--arrow-gov18-parquetvariant--researched-validated)
- [3. Where VARIANT enters n1k1](#3-where-variant-enters-n1k1-the-boundary)
- [4. The chosen representation: a single `V` sigil](#4-the-chosen-representation-a-single-v-sigil)
  - [Alternatives considered](#alternatives-considered-condensed)
- [5. Open questions](#5-open-questions)
- [6. Shredded VARIANT & pushdown](#6-shredded-variant--pushdown-later-note-the-synergy)
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
- [ ] **Phase 0**: implement the `case *extensions.VariantArray` in
      `appendArrowValueJSON` via a hand-rolled zero-alloc `appendVariantJSON(dst, v)`
      over the view API (NOT `MarshalJSON` per row — §2 caveat; use it as the test
      oracle); handle the shredded `typed_value` column shape. Add a fixture:
      Variant-Parquet keyspace → SQL++ query → expected JSON.
- [ ] Decide whether write-back / VARIANT-native semantics is a real requirement
      (Q5.2) — gates everything past Phase 0.
- [ ] For Phase 1: enumerate the direct `v[0]`-peek sites (Q5.5); prototype the `V`
      detection in `base.Parse` and measure the pure-JSON hot-path is unaffected (Q5.6).
