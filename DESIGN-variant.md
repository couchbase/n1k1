# Design (research): supporting the VARIANT type

**Status: research / thinking-out-loud. Not a spec, not a commitment.** This is a
starting outline + open questions for how n1k1 might support the Apache
Parquet/Iceberg/Spark **VARIANT** type. It exists to collect ideas, map them onto
n1k1's existing machinery, and flag what needs research before any code.

Companion to `DESIGN-data.md` §7 (Iceberg read support) and `DESIGN-exprs.md` (the
byte-oriented `base.Val = []byte` model). VARIANT enters n1k1 through the Iceberg /
Parquet reader, so it is really "phase N of the Iceberg story."

---

## 0. TL;DR / thesis

VARIANT is **JSON's value model plus (a) extra *typed scalars* and (b) a compact
self-describing *binary* encoding.** The extra scalars are: `date`, `timestamp`
(µs/ns, with/without tz), `time`, exact `decimal` (up to 38 digits), `binary`,
`uuid`, and *width/precision-distinguished* numerics (`int8/16/32/64`, `float` vs
`double`). Everything else (null, bool, string, object, array) is just JSON.

Two observations shape the whole design:

1. **SQL++ / N1QL has no first-class date/decimal/binary/uuid type.** In N1QL those
   are represented as JSON strings/numbers. So when a SQL++ *query* touches a VARIANT
   value, the natural semantics is "behave as the JSON projection of that value"
   (date → ISO string, decimal → number, binary → base64 string, uuid → string). That
   is exactly what cbq does with such values today. **⇒ most of the engine needs no
   changes to *query* VARIANT data** — if we hand it the JSON projection.

2. The only real *deltas* over "just decode to JSON" are:
   - **Type fidelity** — preserving the original VARIANT type so we can round-trip
     (read VARIANT → query → **write** VARIANT) or expose VARIANT-native semantics.
   - **Precision** — VARIANT `decimal` is exact; JSON number is float64 (lossy).

n1k1 already carries values as **one `[]byte` of JSON, dispatched by first byte**
(`base.Parse`/`ValKind`), and it already has a non-JSON slot: `base/compare.go` has
`ValTypeUnknown // Ex: BINARY`. The central idea explored here is to **extend that
tag space** with a leading *type sigil* so a value stays a single `[]byte`, most
exprs stay untouched, and the type survives for output — rather than boxing VARIANT
into a struct or duplicating the expr library.

---

## 1. What VARIANT actually is (grounding)

_Research item: pin every claim here against the current Parquet/Iceberg Variant
spec — the primitive type-id numbering especially. Treat this section as
"shape, roughly right," not authoritative._

- **Two byte streams**: `metadata` + `value`. In Parquet the Variant logical type is
  (today) a group of two binary columns. `metadata` is a **dictionary of field-name
  strings** (deduplicated, optionally sorted) shared by all objects in the value;
  `value` is the tagged payload tree.
- **`value` tagging** — first byte: `basic_type = b & 0x03`, `type_info = b >> 2`.
  - `0` primitive — `type_info` selects the primitive: null, bool(t/f), int8/16/32/64,
    float, double, decimal4/8/16, date, timestamp(µs, tz/ntz), timestamp(ns, tz/ntz),
    time, binary, (long) string, uuid.
  - `1` short string — `type_info` is the length (0..63); inline UTF-8 bytes follow.
  - `2` object — count + sorted field-id → offset table into `metadata` + child values.
  - `3` array — count + offset table + child values.
- **Superset-of-JSON**: JSON null/bool/number/string/object/array all map in; the
  extra scalars are the delta.
- **Shredding** (Parquet): a Variant column may be *partially shredded* into typed
  sub-columns (`typed_value` + residual `value`) so engines can push predicates /
  projections down to a physical column. This is the columnar performance story and
  ties directly into n1k1's existing Iceberg projection/predicate pushdown.

**Open questions (spec):**
- Q1.1 Exact primitive type-id table + physical layouts (offset-size encoding, decimal
  scale/precision, timestamp unit/tz flags).
- Q1.2 Is there a maintained Go decoder (in `apache/arrow-go` or `apache/iceberg-go`),
  or do we hand-roll? (n1k1's Iceberg path is `iceberg-go`, cgo-free — see §7.)
- Q1.3 How does `iceberg-go` / `pqarrow` currently *surface* a Variant column to us —
  as two binary Arrow arrays? an extension type? nothing yet (unsupported)?

---

## 2. Where VARIANT enters n1k1 (the boundary)

Precisely one place, already mapped by the Iceberg work:

- **`records/parquet.go` → `appendArrowValueJSON(dst, arr, i)`** — a type switch over
  Arrow array types that appends each cell's JSON bytes into `dst`. Today it handles
  bool / int{8..64} / uint / float{32,64} / string. `fastRenderable` gates the fast
  path; other types fall to a `json.Marshal` slow path.
- A VARIANT column is where a **`case` for the Variant (binary/struct) type** decodes
  `(metadata, value)` → n1k1 bytes. **This is the entire ingestion surface.** What it
  emits (plain JSON vs typed-JSON sigils) is the design choice in §3.

Downstream, values flow as `base.Val = []byte`:
- **`base.Parse` (base/compare.go)** is the *single* wrapper over `buger/jsonparser`
  (`jsonparser.Get`) and the first-byte dispatch. One function to teach about sigils.
- **`base.ValKind` / `ParseTypeToValType`** classify a value.
- **Output**: `ValComparer.CanonicalJSON` / `CanonicalJSONWithType` / `WriteJSON`
  (base/canonical.go) is the JSON emitter; a VARIANT *writer* would be a sibling.

---

## 3. Three candidate representations

### 3A. Decode VARIANT → plain JSON at the scan boundary  *(Phase 0)*

Emit the JSON projection (date→ISO string, decimal→number, binary→base64 string,
uuid→string, typed ints→number) directly in `appendArrowValueJSON`.

- **Cost**: a Variant→JSON decoder; **zero** engine change past the boundary.
- **Semantics**: query as JSON, matches cbq's treatment of such scalars.
- **Loss**: type identity + decimal precision; **cannot write VARIANT back** with
  fidelity, cannot answer "IS this value a date vs a string".
- **Verdict**: the obvious MVP. Delivers "query Iceberg VARIANT columns" immediately.
  Everything else is fidelity on top.

### 3B. Typed-JSON *sigils* — extend the tag space  *(the interesting idea; Phase 1+)*

Represent a non-JSON-native scalar as **`<sigil><json-form>`**, a lossless annotation
over its JSON projection. Sketch:

| VARIANT type | internal sigil form | JSON projection (output) |
|---|---|---|
| date | `d"2026-01-30"` | `"2026-01-30"` |
| timestamp | `t"2026-01-30T12:00:00Z"` | `"2026-01-30T12:00:00Z"` |
| decimal (exact) | `x"123.4500"` | `123.45` (or the string, TBD) |
| binary | `b"<base64>"` | `"<base64>"` |
| uuid | `g"6f9…"` | `"6f9…"` |
| int64-with-width | *(probably just a JSON number; carry width only if round-trip needs it)* | number |

The sigil is a single leading byte outside JSON's first-byte alphabet
(`" { [ - 0-9 t f n`), chosen so first-byte dispatch stays O(1). The rest of the
value is its ordinary JSON form, so **exprs that copy/compare bytes work on the
json-form untouched**; only three places learn the sigils:

1. **`base.Parse` / `ValKind`** — strip/recognize the sigil, then dispatch on the
   json-form (a date reports as string, a decimal as number — N1QL semantics), OR map
   the sigil to a dedicated `ValType`. One wrapper function; or a small
   `buger/jsonparser` fork (n1k1 already runs a patched cbq fork — same playbook).
2. **JSON output** (`CanonicalJSON`/`WriteJSON`) — strip the sigil (per §0 obs. 1).
3. **VARIANT output** (new writer) — use sigil + json-form to re-encode Variant binary.

- **Cost**: the sigil grammar, the `Parse`/`ValKind` seam, two output paths. Reuses
  the **entire** expr library for the common case.
- **Win**: single `[]byte`, no boxing, full round-trip fidelity, VARIANT-native
  semantics available where wanted.
- **The catch**: sigils *inside* nested structures (`{"born": d"…"}`) are not valid
  JSON, so anything that *walks* a nested value (jsonparser object/array iteration,
  `ValPathGet`) must tolerate sigils mid-structure — a deeper fork. See Q5.1: is the
  sigil top-level-scalar-only (nested VARIANT stays lazy/opaque until navigated), or
  understood everywhere?

### 3C. Carry VARIANT binary opaque + a parallel expr set  *(rejected)*

Keep the Variant binary as-is and add VARIANT-aware exprs beside the JSON ones.
Duplicates the whole expr library and splits every code path in two. The user's
instinct ("ugh, that doesn't sound great") is right. Only worth it if VARIANT-native
performance (no decode) ever dominates — unlikely before shredding (§6) matters more.

### 3D. A single `V` sigil — the value's tail *is* VARIANT binary  *(the sharp simplification)*

Instead of a rich per-subtype sigil table (3B), use **one** sigil `V`: the byte after
it is VARIANT's *own* first byte (its `basic_type`/`type_info`), so the tail is just a
raw Variant `value` (plus its metadata dict — see catch #1). Don't reinvent a type
table; reuse the one "many smart folks already figured out." Dispatch: n1k1 sees `V`,
then reads byte[1] through VARIANT's rules and maps `basic_type` → n1k1 `ValType`.

This is the elegant unification of 3B and 3C, and it separates **two orthogonal axes**
that 3B/3C had conflated:

- **Tag granularity** — rich per-subtype sigils (3B) *vs.* one opaque `V` (3D).
- **When to decode** — eagerly at the scan boundary *vs.* lazily on first inspection.

`V` pairs most naturally with **lazy** decode: carry the Variant bytes through
untouched, and JSON-ify only when an expr actually inspects the value.

- **Wins:**
  - No parallel type table — the decoder *is* the type machinery, invoked once.
  - **Round-trip is trivial and free** — a pass-through / write-back
    (`SELECT v FROM t WHERE <non-v-pred>` → write VARIANT) copies the bytes verbatim,
    zero decode/encode. This is where `V` decisively beats eager 3A/3B.
  - Lossless by construction (the value *is* the source bytes).

- **Catch #1 — the metadata dictionary.** A Variant `value` references object field
  names by ID into a *separate* `metadata` dictionary (shared per column for
  compression). n1k1 rows are independent `[]byte`, so a self-contained value must
  carry **both** streams: `V<len><metadata><value>`. That re-inlines the dict per row
  (small — just that row's field names — but real) and the decoder parses two streams.
  3B sidesteps this entirely (field names inline as ordinary JSON keys).

- **Catch #2 — inspection needs VARIANT navigation, not JSON navigation.** The moment
  an expr must *compare / compute / navigate / output* a `V` value, it can't use
  jsonparser / `ValPathGet` / `ValComparer` on the tail — those speak JSON, and the
  tail is Variant binary. Two ways out:
  - **Lazy-decode at the `base.Parse` seam** — teach `base.Parse`/`ValKind` to detect
    `V` and return the decoded (typed-)JSON. If *all* inspection funneled through
    `Parse`, most exprs "just work." But (a) some ops peek `v[0]` directly
    (`ValIsString`, first-byte checks) and would need a `V` branch anyway, and (b)
    Parse is called in hot loops, so a value touched N times **re-decodes N times**
    (a filtered-then-projected value decodes twice; a sort key, many times). Eager
    decode-at-scan pays once.
  - **VARIANT-native navigation** — a second navigator beside jsonparser. That is the
    "parallel machinery" cost 3C was rejected for, relocated into the value layer.

- **The subtle asymmetry vs 3B.** 3D `V`-tags *every* VARIANT-sourced value — even a
  plain string/number/object that is perfectly representable as JSON — so *every* such
  value is opaque until decoded, and *every* direct `v[0]` peek site must learn `V`.
  3B only prefixes the genuinely-non-JSON scalars (date/decimal/uuid/…); a VARIANT
  string/number/object decodes to plain **unprefixed** JSON, so it flows through the
  existing machinery with zero new branches. 3B = "JSON-native, annotate the
  exceptions"; 3D = "VARIANT-native, decode on demand."

**Where each wins:** `V` (3D) for **pass-through- and write-back-heavy** VARIANT
workloads (copy bytes, never decode). Typed-JSON (3B) for **inspection-heavy** queries
(decode once at scan, then ride the whole JSON engine). A hybrid is plausible:
carry `V` as the lazy transit form, and `base.Parse` decodes to 3B's typed-JSON on
first touch — VARIANT for transit, JSON for compute, each in its strong zone.

---

## 4. Why 3B maps cleanly onto n1k1 (the appeal)

- **One `[]byte`, no boxing** — the founding constraint survives intact.
- **First-byte dispatch already exists** — sigils just extend `base.Parse`'s table.
- **Precedent**: `ValType` already reserves `ValTypeUnknown // Ex: BINARY` and the
  `ValType` ordering is "intended to match N1QL's ordering" — a sigil'd type slots
  into that scheme rather than inventing a parallel one.
- **Exprs are byte-movers** — projection, comprehensions, array/object builders copy
  element bytes verbatim; a sigil'd scalar rides through untouched (it only *matters*
  at compare / arithmetic / output).
- **Output already centralized** — one canonical emitter to teach "strip sigil"; the
  VARIANT writer is its sibling.
- **Consistent with house style** — n1k1 already forks/patches cbq (`n1k1-query`) and
  keeps values as reused byte buffers; a small jsonparser fork + a sigil convention is
  the same kind of move, not a new paradigm.

---

## 5. The hard parts / open questions

- **Q5.1 Nested sigils.** Top-level-scalar-only (simplest; nested VARIANT stays opaque
  or is lazily JSON-ified on navigation) vs understood-everywhere (deep jsonparser
  fork; `ValPathGet`, object/array iteration, `CollElems` all tolerate sigils). This is
  the biggest fork in the road. Can most workloads live with "scalars keep type,
  nested access degrades to JSON"?
- **Q5.2 Collation.** N1QL order is null < bool < number < string < array < object.
  Do sigil'd values collate on their **json-form** (date=string, decimal=number —
  minimal change, matches cbq), or on a **VARIANT-native** ordering? Likely: json-form
  for SQL++ queries; VARIANT ordering only if/when we expose VARIANT-native compares.
  `ValComparer.CompareWithType` is the seam.
- **Q5.3 Decimal precision.** Exact 128-bit vs float64. Proposal: arithmetic degrades
  to float64 (matches cbq / N1QL — document it), but the sigil'd string form preserves
  the exact value for **round-trip output**. Is "lossy-in-arithmetic, lossless-in-
  transit" acceptable? (It's the same bargain cbq already makes.)
- **Q5.4 Is fidelity even needed soon?** §7 Iceberg is **read-only**. If n1k1 never
  *writes* VARIANT/Iceberg, Phase 0 (3A) may be all that's required for a long time.
  Fidelity (3B) earns its cost only with write-back OR VARIANT-native predicates. →
  gates whether Phase 1 happens at all.
- **Q5.5 The decoder.** Library vs hand-roll (Q1.2). Metadata-dictionary handling
  (shared field-name table) is the fiddly bit; a hand-rolled decoder is ~a few hundred
  lines but must track the spec.
- **Q5.6 Sigil grammar & safety.** Reserve a byte range disjoint from JSON's first
  bytes; ensure sigil'd bytes survive `append`/copy/`bufPre[:0]` reuse unharmed
  (they're just bytes — should be fine); define escaping so a sigil never appears
  ambiguously inside a following string.
- **Q5.7 Type predicates / builders.** Does SQL++ grow `IS_DATE` / `TO_VARIANT` /
  typed accessors, or is VARIANT fully transparent? cbq has no VARIANT, so this is
  n1k1-native surface to design (or deliberately omit).
- **Q5.8 `_meta` / schema.** How does a VARIANT column advertise itself in
  `.keyspaces` / column metadata so users know a field is VARIANT vs plain JSON?

---

## 6. Shredded VARIANT & pushdown (later, but note the synergy)

Parquet shredding splits a Variant into typed sub-columns. n1k1's Iceberg path already
does **projection + predicate + partition pushdown** into the scan (DESIGN-data §7 /
the `records.ScanPredicate` sidecar). A predicate on a shredded VARIANT subfield
(`WHERE v.customer.tier = 'gold'`) could push to the shredded physical column exactly
like a top-level column does today — reusing that machinery, not new machinery. This
is the real performance payoff and argues for keeping the VARIANT design *inside* the
Iceberg pushdown framework rather than bolting on beside it.

---

## 7. Rough phasing

- **Phase 0 — read as JSON.** Variant→JSON decoder in `appendArrowValueJSON`
  (+ `fastRenderable`/slow-path). Read-only, lossy-to-JSON, ~zero engine change.
  Unblocks querying Iceberg VARIANT columns. *Do this first; it may be enough.*
- **Phase 1 — fidelity.** Only if write-back or type-fidelity is wanted. Choose along
  the two axes (§3D): typed-JSON sigils (3B, best for inspection-heavy) vs the single
  `V` opaque carrier (3D, best for pass-through/write-back), or the hybrid (`V` transit
  + lazy decode-to-typed-JSON at `base.Parse`). Whichever: a `base.Parse`/`ValKind`
  seam + strip/decode-on-JSON-output. Differential-test the json-form semantics against
  cbq so "query behavior" is provably unchanged.
- **Phase 2 — VARIANT writer + nested sigils + shredded pushdown.** Variant binary
  encoder (round-trip), deep sigil-aware navigation, predicate/projection pushdown into
  shredded subfields (§6).

---

## 8. Table of contents (living)

- [0. TL;DR / thesis](#0-tldr--thesis)
- [1. What VARIANT actually is](#1-what-variant-actually-is-grounding)
- [2. Where VARIANT enters n1k1 (the boundary)](#2-where-variant-enters-n1k1-the-boundary)
- [3. Three candidate representations](#3-three-candidate-representations)
  - [3A. Decode → plain JSON](#3a-decode-variant--plain-json-at-the-scan-boundary--phase-0)
  - [3B. Typed-JSON sigils](#3b-typed-json-sigils--extend-the-tag-space--the-interesting-idea-phase-1)
  - [3C. Opaque binary + parallel exprs (rejected)](#3c-carry-variant-binary-opaque--a-parallel-expr-set--rejected)
  - [3D. Single `V` sigil — tail is VARIANT binary](#3d-a-single-v-sigil--the-values-tail-is-variant-binary--the-sharp-simplification)
- [4. Why 3B maps cleanly onto n1k1](#4-why-3b-maps-cleanly-onto-n1k1-the-appeal)
- [5. Hard parts / open questions](#5-the-hard-parts--open-questions)
- [6. Shredded VARIANT & pushdown](#6-shredded-variant--pushdown-later-but-note-the-synergy)
- [7. Rough phasing](#7-rough-phasing)

## 9. Research backlog (pull from §1/§5)

- [ ] Confirm the Variant spec's primitive type-id table + physical encodings (Q1.1).
- [ ] Survey Go decoders: `arrow-go`, `iceberg-go` Variant status; how a Variant column
      surfaces through `pqarrow` today (Q1.2, Q1.3).
- [ ] Decide Phase-0 JSON projection rules per type (match cbq exactly; write a
      differential fixture: a small Variant Parquet file → expected JSON).
- [ ] Prototype the sigil grammar; measure whether `base.Parse` can absorb it with no
      measurable hit on the pure-JSON hot path (Q5.6).
- [ ] Nested-sigil decision (Q5.1) — the fork that most shapes the effort.
- [ ] The two axes (§3D): tag granularity (per-subtype sigils vs single `V`) ×
      decode timing (eager-at-scan vs lazy-at-`Parse`). Sketch the expected VARIANT
      workload — pass-through/write-back-heavy (favors `V`) vs inspection-heavy
      (favors eager typed-JSON) — before choosing.
- [ ] Enumerate the direct `v[0]`-peek sites (`ValIsString`, first-byte checks) that a
      leading prefix (any of 3B/3D) would have to learn — how many, how hot?
- [ ] Decide whether write-back / VARIANT-native semantics is a real requirement
      (Q5.4) — it gates Phase 1 entirely.
