# Design: supporting the VARIANT type

_Last reviewed: 2026-07-25._

**Status:** Phase-0 read (as JSON) + typed-scalar fidelity, Phase-2a JSON-projection
write-back, and the Phase-1 read-side `V`-carrier all **ship**; Phase-1 *write-back* and
Phase-2b shredded-pushdown are next. Everything is `records/` + `glue/` + `./variant/` — **no
query-engine change**. Companion to `DESIGN-data.md §7` (Iceberg read — VARIANT enters through
the Iceberg/Parquet reader, it's "phase N of the Iceberg story") and `DESIGN-exprs.md` (the
`base.Val = []byte` model).

## Thesis

VARIANT is **JSON's value model plus (a) extra typed scalars** (`date`, `timestamp` µs/ns±tz,
`time`, exact `decimal` ≤38 digits, `binary`, `uuid`, width-distinguished numerics) **and (b) a
compact self-describing binary encoding** (two byte streams: a `metadata` field-name dictionary
+ a tagged `value` tree; the first `value` byte carries `basic_type = b&0x03` /
`type_info = b>>2`). Two observations drive the whole design:

1. **SQL++/N1QL has no first-class date/decimal/binary/uuid type** — those are JSON
   strings/numbers. So a SQL++ *query* over a VARIANT naturally "behaves as the JSON projection"
   (date→ISO string, decimal→number, binary→base64), which cbq already does — **⇒ most of the
   engine needs no change to *query* VARIANT; hand it the JSON projection.**
2. The only *deltas* over "just decode to JSON" are **type fidelity** (round-trip / write-back /
   VARIANT-native semantics) and **precision** (VARIANT `decimal` is exact; JSON number is
   float64).

**Chosen approach:** carry a VARIANT value as `V<len><metadata><value>` — a `V` sigil then the
raw Apache Variant bytes — reusing VARIANT's own type tagging instead of a parallel one, and
lazily projecting to JSON only when a query inspects the value. Keeps "one `[]byte`, no boxing";
makes round-trip/write-back free (copy the bytes); the decode/navigate machinery is a ready-made
zero-copy library already in the dep tree.

## The decode library + the `./variant/` projector

n1k1 already transitively depends on `apache/arrow-go/v18`, which ships a first-party, cgo-free
`parquet/variant` package built on the same "views into a backing `[]byte`, no boxing"
discipline. `variant.Value` is a `{value []byte, meta Metadata}` **window** (not a materialized
tree); array/object iteration returns child `Value`s that are **subslices into the same backing
buffer** (zero-copy, like jsonparser). The read path surfaces a VARIANT Parquet column
**directly as `*extensions.VariantArray`** (via `pqarrow`; importing `extensions` registers
`parquet.variant`), `va.Value(i)` → a `variant.Value` view.

⚠ **`MarshalJSON` is NOT a zero-alloc emitter** — its `json.Marshaler` signature returns a fresh
slice per call, and internally it boxes via `v.Value()` + reflect-`json.Marshal` ("simplest, not
most efficient"). There is no `AppendJSON(dst)` variant. **So the projector lives in a small,
dependency-free `./variant/` package** (arrow-go + stdlib only, reusable standalone):
`variant.AppendJSON(dst, v)` walks objects/arrays via the offset tables in `v.Bytes()` directly
(no per-node `v.Value()` box), resolves keys via the zero-copy `Metadata.KeyAt`, and formats
scalars from bytes — including a 128-bit `variant.AppendDecimal128` (`big.Int`-free /10 loop,
byte-identical to `decimal128.Num.ToString`; needed because `variant.ParseJSON` encodes *every*
fractional number as an exact `Decimal16`, not a `double`). **All typed scalars are native
dst-formatters** (`date`/`timestamp`/`time` via `time.Time.AppendFormat`; `uuid` hand-rolled
hex; `binary` via `base64.AppendEncode`), each byte-identical to `MarshalJSON` and **0
allocs/op**; `MarshalJSON` is kept only as a test oracle. Measured ~45× faster than
`MarshalJSON`, 0 allocs even for deep nested objects with `Decimal16` fields. Fidelity survives
nesting: a `date` buried 3 levels deep keeps `Type()==Date` and projects to `"2025-04-16"`.

## Where VARIANT enters + the `V` carrier

**Exactly one ingestion point:** `records/parquet.go → appendArrowValueJSON(dst, arr, i)` — a
type switch over Arrow arrays. A `case *extensions.VariantArray` decodes `(metadata, value)` →
n1k1 bytes; what it emits (JSON for Phase-0, `V<...>` for fidelity) is the carrier choice. `V`
(0x56) is **outside JSON's value-start alphabet** (`" { [ - digits t f n` + whitespace), so
`v[0]=='V'` is an unambiguous non-JSON signal (a JSON string starting with "V" begins with `"`);
the `metadata`/`value` split needs a `<uvarint len(meta)>` delimiter; the bytes survive
`append`/`[:0]` reuse.

Value inspection **funnels through a tiny set of `jsonparser`-wrapping seams**, so
VARIANT-awareness plugs into *those*, not the hundreds of exprs:

| seam | `V`-aware behavior |
|---|---|
| `base.Parse(v)` / `ValKind` | if `v[0]=='V'`, classify from VARIANT's tag byte (`basic_type`→`ValType` of the JSON projection) — **alloc-free, no decode**. Fixing `Parse` gets the whole `Parse`-based `is_*` family for free |
| `base.ValPathGet(val, path, out)` | if `val[0]=='V'`, walk the path via the zero-copy view API (offset-table subslices), project the reached **scalar** leaf into the reused `out` buffer as plain JSON; a **container** leaf reframes as a self-contained `V<meta><subvalue>` |
| `ValComparer.Compare(a,b)` | if an operand is `V`, project it (rare whole-value compare) |

So `WHERE order.customer.rating > 10` touches VARIANT in exactly one place (`ValPathGet`
projects the `rating` scalar to `10`); `ExprCmp` then does an ordinary number compare with no
VARIANT code, and the `order` register value stays `V<…>` untouched (a sibling `SELECT order`
round-trips verbatim). The residue is the few functions that peek `v[0]` **directly** (e.g.
`ValIsString` checks `v[0]=='"'`) — a small enumerable set to reroute through the classifier
(Q5.5). ⚠ **Metadata-dict alloc:** `variant.New` parses metadata with a `make([][]byte,
dictSize)` — one small alloc *per navigation* (per row on a filter). Mitigation: in Parquet the
dict is shared per column batch, so parse the `Metadata` **once per batch** and navigate each
row's `value` against it, materializing a self-contained `V<meta><value>` only when a value
*escapes* the batch (spill, join build, output).

**Phase-1 read shape (shipped, "Idea A"):** the scan renders a fidelity row as **one whole-row
`V`-object slot** in the `.`-body — `appendRecordsVals` emits JSON `{…}` for a JSON-only batch
(fast path) or `V<meta><value>` for a VARIANT batch, **one row = one slot** (preserving the
"a document is one navigable value" contract; the per-column-slot alternative "B" is a perf
escape hatch that detonates that contract). `base` owns cheap `V`→`ValType` classification (a
byte-tag table, **no arrow-go** — keeps `base` wasm-safe); decode/navigate/project is a
**registered hook** `variant/` installs. `V`-emission is opt-in (`records.VariantFidelity`);
default stays Phase-0 read-as-JSON. A **differential test** pins that a VARIANT query returns
byte-identical rows with the mode on vs off.

## The write-back pathway (Phase-1 write-back — NOT done)

Tracing `INSERT INTO out.parquet SELECT s.order FROM src s` with fidelity on shows the read
seams are the *easy half*; the full seam list for lossless write-back:

| seam | `V` handling | status |
|---|---|---|
| **transit** — register moves, JOIN, UNION, spill (`ValsEncode`/`Decode` is length-prefixed, format-agnostic) | bytes ride through untouched | ✅ free (verify no path assumes JSON) |
| **classify** — `Parse`/`ValKind` + direct `v[0]` peeks | byte-tag → `ValType` of the JSON projection (date/ts/uuid/binary→String; int/decimal/float→Number; object→Object) so classify agrees with output & collation | ✅ small table |
| **navigate** — `ValPathGet`, `ValElement` | unboxed offset-table walk; scalar leaf → project into reused `valOut`; container leaf → reframe as self-contained `V` | ✅ 1 hook |
| ⚠ **compare / hash** — `ValComparer.Compare`, GROUP/DISTINCT/JOIN hashing | **must project to CANONICAL JSON, not hash raw `V` bytes** — two variants that project equal can differ in bytes (metadata order, encoding choices); hashing raw `V` would split equal GROUP/DISTINCT groups + misorder (a correctness bug) | needed |
| ⚠ **construct** — object/array construction with a `V` member | `SELECT s.order` builds `{"order":<order>}` — a construction; constructors must **propagate `V`** into a `V`-object or write-back goes lossy. **The biggest surface beyond reads.** (An expr that *transforms* content — string ops, arithmetic, `OBJECT_*` — legitimately projects to JSON and is lossy; fidelity = pass-through + structural re-assembly, not transformation) | the gap |
| ⚠ **output vs write-back** — `ConvertVals.Convert` → `value.Value` → `WriteJSON` | the same result row must render JSON for a SELECT yet preserve `V` for the writer. Two resolutions: (a) a boundary `VariantValue` whose `MarshalJSON` projects but the writer type-asserts to recover raw bytes; (b) the INSERT path consumes engine `V`-slots *before* `value.Value` conversion (reworks `InsertRun`'s source) | tension |
| **write** — parquet VARIANT-column appender | detect `V` → `variant.New(meta,value)` → `VariantBuilder.Append` (lossless), *not* Phase-2a `WriteJSON`→`ParseJSON` (lossy) | needed |

**Verdict:** Phase-1 write-back is materially bigger than "teach `Parse`/`ValPathGet` about `V`"
— the load-bearing additions are construction-propagation, the `value.Value` boundary decision,
and compare/hash canonicalization. Deferred pending a scope decision. (The read-side carrier
already enables **VARIANT-native accessors** — exact-decimal compare, typed date math —
independent of write-back; see Q5.7.)

## Performance

Measured (arm64, `benchmem`): the arrow-go-free carrier primitives are free and zero-alloc
(`SplitVariantEnvelope` 3 ns / `VariantValType` 3.8 ns, both 0 allocs); `variant.AppendJSON`
(deep obj) 263 ns / 0 allocs vs `MarshalJSON` 11858 ns / 131 allocs; scalar-leaf `ValPathGet`
projects into the reused buffer (alloc-free after warmup, transit alloc-free). The **opt-in
fidelity render costs ~2.9× time / ~9× memory** over Phase-0 (~1.4 µs, ~2.5 KB, ~18 allocs/row
vs ~0.5 µs, ~289 B, ~1 alloc) — reasonable for opt-in, **not yet fit to be the always-on
default**.

⚠ **An arrow-go O(N²) bug found + worked around:** reusing one `variant.Builder` across rows via
`Reset()` blew up to **221 MB / 256-row batch** — `Builder.Reset` clears the buffer/dictionary
but *not* its internal `totalDictSize` accumulator, so each row's `Build()` sized metadata by
the running SUM of every row's dictionary. **Use a fresh builder per row** (linear). The
remaining ~18 fidelity allocs/row (all opt-in — plain queries pay nothing) are: the per-row
`variant.Builder`+metadata build; per-scalar-column `av.ParseJSONBytes` (a `Builder`+decoder per
column); and the per-navigation `make([][]byte, dictSize)` metadata parse. Mitigations (share
the batch's `Metadata`, a JSON-round-trip-free scalar re-encoder) are deferred until/unless
fidelity becomes the default; nested-variant cells complicate dict sharing (field names vary
per row).

## Shredded VARIANT & pushdown (reads DONE zero-code; pushdown later)

Parquet shredding splits a Variant into typed sub-columns (`typed_value` + residual `value`).
**Reading needs no n1k1 code** — pqarrow reconstructs a shredded file back into one
`*extensions.VariantArray`, and `.Value(i)` coalesces the sub-columns + residual into a complete
`variant.Value`, so Phase-0's `AppendJSON` case is oblivious (a shredded column projects to
exactly the same JSON). The only lost property is zero-alloc pass-through (coalescing rebuilds a
fresh `variant.Value`; non-shredded rows stay zero-alloc). Proven by
`glue.TestVariantParquetShreddedKeyspaceQuery` (`IsShredded()` asserted; queries hit both
shredded `customer.name`/`tier` and residual `customer.address`/`total`/`orderlines`).
**Pushdown is the remaining perf work** — a predicate on a shredded subfield
(`WHERE v.customer.tier='gold'`) could push to the shredded physical column exactly like a
top-level column does today, reusing the Iceberg `records.ScanPredicate` sidecar (DESIGN-data
§7), not new machinery — a reason to keep VARIANT inside the Iceberg pushdown framework.

## Phasing

- **Phase 0 — read as JSON. DONE + validated** at three levels (reader
  `TestParquetReaderRendersVariantColumn`, full SQL++ keyspace `TestVariantParquetKeyspaceQuery`,
  shredded `TestVariantParquetShreddedKeyspaceQuery`). All typed scalars project via native,
  byte-identical, zero-alloc dst-formatters — no `MarshalJSON` in the read path.
- **Phase 2a — JSON-projection write-back. DONE.** `INSERT INTO <x>.parquet SELECT …` emits a
  Parquet VARIANT column for object-valued projections (`glue/insert_writer.go`:
  `inferParquetKind` OBJECT → `NewDefaultVariantType`, a per-row appender `WriteJSON`→
  `av.ParseJSONBytes` → `VariantBuilder.Append`; NULL → Parquet-NULL cell). An encode
  *boundary*, so not zero-alloc; fidelity is the JSON projection only.
  (`TestInsertVariantColumnRoundTrip`.)
- **Phase 1 — the `V` carrier (typed-scalar fidelity). Read side SHIPPED:** (1) `base` carrier
  framing (`SigilVariant`, envelope) + `V`→ValType classification (arrow-go-free) + the
  projection/nav hooks + the `variant`-backed hook bridge in `records`; (2) the opt-in
  `records.VariantFidelity` scan mode emitting whole-row `V` objects, with `V`-aware
  `ValPathGet`/`ValKind`/`ValsSelfObject`/`Convert`, pinned by a differential test. **(3) NOT
  DONE — lossless write-back** (the large rework above), plus compare/hash canonicalization for
  GROUP/ORDER on whole-`V` values and whole-`V` `Parse`-based type predicates.
- **Phase 2b — shredded pushdown** (perf; reuse the Iceberg `ScanPredicate` sidecar).

## Open questions (condensed)

- **Q5.2 Is fidelity needed soon?** Iceberg read is read-only; if n1k1 never *writes* VARIANT,
  read-as-JSON may suffice — the `V` carrier earns its cost only with write-back OR
  VARIANT-native predicates. **Resolved: users confirmed write-back is wanted.**
- **Q5.3 Decimal precision.** Arithmetic degrades to float64 (matches cbq/N1QL); the `V` bytes
  preserve the exact value for round-trip — "lossy-in-arithmetic, lossless-in-transit."
- **Q5.4 Collation.** A `V` value collates on its JSON projection (date=string, decimal=number —
  matches cbq); `ValComparer.CompareWithType` is the seam for any future VARIANT-native order.
- **Q5.5 Direct `v[0]` peek sites** — enumerate the few functions reading the first byte directly
  and reroute through the classifier.
- **Q5.7 Type predicates / VARIANT-native accessors.** Once a `V` value flows with its typed
  bytes intact (shipped), builtins can compute what fidelity JSON can't express (exact 128-bit
  decimal math, real date/timestamp math), dispatching on `base.VariantValType` + the nav hook.
  Naming: reuse N1QL conventions (`ISDECIMAL`/`ISDATE` predicates, `TO_VARIANT` + `TODECIMAL`/
  `TODATE`, `DECIMAL_ADD`/`_SUB`/`_MUL`/`_DIV`/`_CMP` type-prefix families that auto-hoist float/
  string args to exact decimal and return a `V`, `TYPE(x)` extended to report the subtype) —
  every piece has a cbq or Snowflake precedent.
- **Q5.8 Schema advertising** — how a VARIANT column shows itself in `.keyspaces` so users know
  a field is VARIANT vs plain JSON.
- **Q5.9 VARIANT in JS (goja) extensions.** ⚠ JS can't natively hold the exact types (its
  `Number` is float64, `Date` is ms), so "VARIANT in JS" can't mean native JS values (that
  silently re-loses fidelity *inside* JS). The fix is **MongoDB Extended JSON (EJSON)** — a
  typed scalar as a tagged JSON object carrying the exact value as a string
  (`{"$numberDecimal":"9.99"}`) that JS passes through untouched or processes with a bigdecimal
  lib. This is the `marshal: "variant"` mode; ⚠ **don't encode marshaling in the filename** —
  `marshal` (`json`|`variant`|`raw`) is an axis orthogonal to `kind` (scalar/aggregate/stream),
  so it's a per-export field of a multi-export module manifest (`exports.functions`), not a
  combinatorial second filename suffix. (This shipped — see `DESIGN-extensions.md`.)
