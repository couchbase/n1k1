# Design: Native Expression Coverage

Status: proposal / for review

n1k1 evaluates a *small* set of SQL++/N1QL expressions natively — fast, byte-
oriented, allocation-avoiding, compiler-friendly — and **delegates everything else
to the cbq-query (`n1k1-query`) expression engine**, whose `Evaluate()` boxes into
`value.Value` objects and produces transient garbage on every row. This document
inventories what's native today, catalogs the full universe of what remains, and
— per expression family — assesses whether and how each can be ported while
honoring n1k1's performance principles (see `DESIGN.md`).

The goal: an incrementally growing library of native expressions, with the cbq
fallback kept forever as a correctness backstop.

## Why this matters

The fallback path (`glue/expr.go:ExprTree`) does three allocating things **per
row**:
1. **Box** `base.Vals` (`[]byte` per column) → a single `value.Value` document
   (`ConvertVals.Convert`).
2. **Evaluate** via cbq (`expr.Evaluate(v, context)`) — every sub-expression
   allocates intermediate `value.Value` objects.
3. **Unbox** the result `value.Value` → JSON bytes (`vResult.WriteJSON(...)`).

For a predicate or projection evaluated over millions of rows, that's a lot of
future GC work. A native expression instead reads JSON bytes with `jsonparser`,
computes, and appends the result into a **reused** `[]byte` buffer — zero steady-
state garbage.

## n1k1's expression principles (from `DESIGN.md`)

- Values are **`base.Val` = `[]byte`** holding JSON — never `interface{}` /
  `map[string]interface{}` / `value.Value`.
- **No boxing:** compute on bytes; emit results as JSON text into a **lifted,
  reused** buffer (`buf[:0]`), not by building Go structures.
- **`jsonparser`** for navigation/parsing — returns slices pointing into the input,
  no unmarshal garbage.
- **Positional "registers":** fields are pre-resolved to `vals[idx]` slots, not map
  lookups.
- **`lz` / lazy codegen:** expression code is written in the careful golang subset
  so `intermed_build` can emit both an interpreter and a compiled path
  (`varLift`, `// !lz`, `LzScope`). A native expr = a setup function returning an
  `ExprFunc` closure; static work happens once, per-row work stays minimal.
- **Early-constant folding:** `sales < 1000` parses/types `1000` once at setup (see
  `ExprCmp` static path), not per row.

### The byte-level toolkit already available
New native exprs build on: `base.Val`/`Vals`, `base.Parse` (type+bytes via
jsonparser), `base.ParseFloat64`, `base.ValPathGet`, `base.ValTruthy`,
`base.ValEqual*`, and especially **`base.ValComparer`** (`CompareWithType`,
`Collate`, `CanonicalJSON[WithType]`, `EncodeAsString`) — all of which operate into
caller-supplied buffers with no allocation and already encode N1QL type/collation
semantics.

## How it works today

- **Catalog:** `engine.ExprCatalog map[string]base.ExprCatalogFunc`
  (`base/vars.go`), signature
  `func(vars, labels, params, path) ExprFunc`, where
  `ExprFunc = func(Vals, YieldErr) Val`.
- **Conversion:** the cbq `expression.Expression` tree from the planner is walked
  by `glue/expr_optimize.go:ExprTreeOptimize`, which recognizes an **allowlist**
  and rewrites those to native catalog params; anything else → `ExprTree` fallback.
- **The native allowlist is tiny** (`OptimizableFuncs`): `eq`, `lt`, `le`, `gt`,
  `ge`, plus `Constant` → `json` and `Field` → `labelPath`. A single unsupported
  operand anywhere makes the **whole** expression fall back (recursive).

### Native inventory today (~15 entries)

| Name | File | Role |
|---|---|---|
| `json` | `engine/expr.go` | pre-parsed constant |
| `labelPath` | `engine/expr.go` | field/path access via `jsonparser` |
| `labelUint64` | `engine/expr.go` | binary uint64 → JSON int |
| `valsEncode` / `valsEncodeCanonical` | `engine/expr.go` | key encoding for maps |
| `and` / `or` | `engine/expr_bi.go` | short-circuit logical |
| `eq` `lt` `le` `gt` `ge` | `engine/expr_cmp.go` | comparisons (numeric fast path + `ValComparer` fallback) |
| `window-partition-row-number`, `window-frame-count`, `window-frame-step-value` | `engine/expr_window.go` | window helpers (FIRST/LAST/NTH/LEAD/LAG) |
| `exprStr` / `exprTree` | `glue/expr.go` | **the fallback** (parse / delegate to cbq) |

Notably **absent and therefore delegated:** `not`, arithmetic (`+ - * / %`),
`between`, `like`, `in`, `is null/missing/valued`, `||`, `CASE`, `NVL/IFNULL/
COALESCE`, and *all* ~350 scalar functions.

## The universe & the gap

The cbq `expression/` package defines **~357 distinct scalar expression types
(~410 registry entries incl. aliases)** across ~95 files. Counts by family, with
allocation profile:

| Family | ~Count | Profile |
|---|---|---|
| Arithmetic (`+ - * / div % imod neg`) | 8 | scalar |
| Comparison (`eq…ge`, between, like, is-null/missing/valued/distinct) | ~15 | scalar |
| Logical (and/or/**not**) | 3 | scalar |
| Concatenation | 3 | builds string |
| Conditional (CASE ×2, NVL/IFNULL/IFMISSING/COALESCE/NULLIF/GREATEST/LEAST/…) | ~14 | scalar |
| Navigation (field/element/slice) | ~5 | scalar (slice builds) |
| Collection (ANY/EVERY/ARRAY/MAP/OBJECT/FIRST/IN/WITHIN/EXISTS) | ~14 | **structure-building** |
| Construction (array/object literals) | ~7 | **structure-building** |
| String funcs | 32 | mostly scalar |
| Numeric/math funcs | 27 | scalar |
| Date/time funcs | 33 | scalar (some volatile) |
| Array funcs (`array_*`) | 34 | **mostly structure-building** |
| Object funcs (`object_*`) | 25 | **mostly structure-building** |
| Type check (`is_*`) | 8 | scalar |
| Type conv (`to_*`, decode) | 6+ | scalar/structure |
| JSON (encode/decode/poly_length/encoded_size/pairs) | 5 | mixed |
| Bitwise | 8 | scalar |
| Regexp / LIKE | 13 | scalar + some arrays |
| Token | 4 | some arrays |
| Meta/admin (meta, uuid, version, current_user…) | 10 | mostly scalar, side-effecting |
| Vector (distance/encode/normalize) | 7 | scalar + binary |
| Specialized (crypto, curl, control, fusion, timeseries, RCTE, UDF, natural/AI, advisor, distributed) | ~30 | varies, side-effecting |

## Supportability, per n1k1's principles

A key clarification: **"no boxing" ≠ "no output structure."** Even array/object
results can honor the principles by **serializing JSON text into a lifted `[]byte`
buffer** (exactly what `ValComparer.CanonicalJSONWithType` already does) rather than
allocating `[]interface{}` / `map[string]interface{}` / `value.Value`. So the real
axis is: *how much transient work per row, and does it fit the byte/register/lz
model.* Four tiers:

### Tier A — port first (scalar, byte-friendly, high per-row frequency)
Read operand bytes, compute, append result. These dominate `WHERE`/`JOIN`/
projection cost and are the highest ROI.
- **Logical `not`; arithmetic `+ - * / % div idiv neg`** — parse number(s) via
  `base.ParseFloat64`, compute, append; reuse the `ExprCmp` early-constant/typed
  fast-path pattern.
- **`between`, `in`** (scalar list), **`is null/missing/valued`**, **`is [not]
  distinct from`** — direct byte/type checks (`base.Parse`, `ValComparer`).
- **Type checks `is_array/object/string/number/boolean/atom`** — `base.Parse`
  returns the type; trivial.
- **`||` concat, `CASE` (both), `NVL/IFNULL/IFMISSING/IFMISSINGORNULL/COALESCE/
  NULLIF/MISSINGIF/GREATEST/LEAST`** — control-flow over already-native operands;
  mostly select-a-buffer, minimal work.
- **`element`/`slice` navigation** — extends `labelPath` via `jsonparser`.

### Tier B — port next (scalar but needs parse+format into a reused buffer)
A bounded amount of transient work, still zero steady-state garbage with buffer
reuse.
- **String funcs** (upper/lower/trim/substr/length/position/replace/split/contains/
  repeat/pad…) — most map to Go `strings.*` writing into a lifted buffer; watch
  multi-byte variants.
- **Numeric/math** (abs/ceil/floor/round/trunc/sqrt/pow/exp/ln/log/trig/sign/…) —
  `math.*` + `strconv.AppendFloat` into a buffer.
- **Date/time (non-volatile)** — parse/format millis↔string into a buffer.
- **Bitwise, type conversions `to_*`, JSON `encode/poly_length/encoded_size`,
  regexp/LIKE** — LIKE/regexp compile the pattern **once at setup** (early-constant),
  match per row (classic n1k1 static-arg win).

### Tier C — port with care / partial (structure-building, but doable in bytes)
Split by whether they *return* a structure:
- **Predicate/reader array & object ops that DON'T build output** —
  `array_length/contains/contains_all/position/binary_search`, `array_min/max/sum/
  avg/count`, `object_length/names`, `poly_length` — can iterate with
  `jsonparser.ArrayEach`/`ObjectEach` and compute a scalar **without materializing**
  the array. Good ROI, Tier-B-like in practice.
- **Ops that DO build output** — `array_append/concat/distinct/sort/flatten/
  reverse/union/…`, `object_put/remove/concat`, array/object **construction** —
  emit JSON text into a lifted buffer (sorting/dedup may need a scratch index via
  `ValComparer`). No `value.Value`, but real per-row serialization cost; port the
  common ones (`array_append`, `array_concat`, object construct) by frequency.
- **Comprehensions `ANY/EVERY/ARRAY/MAP/OBJECT/FIRST/IN/WITHIN`** — bind a variable
  and evaluate a **sub-expression per element**. Feasible in n1k1's model: iterate
  element byte-slices into a temp **register/label slot** and invoke the child
  `ExprFunc`; `ANY`/`EVERY` short-circuit (cheap), `ARRAY`/`MAP`/`OBJECT` build
  output (buffer). Highest-complexity of the "portable" set — needs the sub-expr
  binding plumbing; do after Tiers A/B prove the pattern.

### Tier D — delegate to cbq indefinitely (low ROI / side-effecting / rare)
Keep these on the fallback; they're one-shot, rare, non-deterministic, or external:
- **Volatile / non-deterministic:** `now_*`, `clock_*`, `random`, `uuid` (evaluate
  once where possible; correctness > garbage).
- **Side-effecting / environmental:** `curl`, `meta`, `current_user(s)`, `version`,
  `node_*`, `abort`, `hashbytes` (crypto), `advisor`.
- **Heavy/niche subsystems:** `func_fusion` (BM25/RRF search ranking), timeseries,
  `recursive_cte`, distributed, user-defined functions, natural/AI providers,
  vector distance (route to the FTS/vector path in `DESIGN-indexing.md` instead).
These are infrequent and their allocation cost is negligible against a whole-query
budget — porting them isn't worth the semantic-fidelity risk.

## Why porting is largely mechanical: cbq's two-layer structure

cbq's scalar expressions follow a rigid, uniform pattern, so porting can be
near line-for-line — and copying it faithfully is exactly what minimizes
edge-case misses.

**Layer 1 — a thin, uniform `Evaluate` skeleton.** Each expression only: (1)
evaluates operands recursively, (2) propagates errors, (3) applies a standard
MISSING/NULL branch, (4) delegates the real work to a `value` primitive. Verbatim
shapes from the source:
- **Comparison** delegates entirely — `Eq.Evaluate` → `first.Equals(second)` (the
  value method returns MISSING/NULL/bool per N1QL rules). `Between` → `op.Compare
  (low/high)`, MISSING propagates, non-comparable → NULL.
- **Arithmetic** is MISSING-dominant — `Add` loops operands: any MISSING →
  MISSING; any non-number → NULL; else `sum.Add(value.AsNumberValue(arg))`.
- **Unary unknown-passthrough** — `Not` / `IsString`: MISSING and NULL pass
  through unchanged; else compute (`arg.Truth()` / `arg.Type()==STRING`).
- **Conditional** — `IfNull`: return the first operand whose type isn't NULL.

These collapse into a **handful of propagation classes**:

| Class | Rule | Members |
|---|---|---|
| delegate-to-value | the value primitive encodes the 3-valued result | eq/ne/lt/le/gt/ge, between |
| MISSING-dominant → NULL | any MISSING → MISSING; else any non-typed → NULL; else compute | arithmetic, most scalar funcs |
| unknown-passthrough | MISSING → MISSING, NULL → NULL; else compute | not, `is_*` type checks, most string/num/date funcs |
| short-circuit truth-table | special 3-valued tables | and, or, coalesce/ifnull/ifmissing |

Members of a class share the *identical* skeleton, so n1k1 should encode each
class as **one reusable harness** (exactly as `engine/expr_cmp.go:ExprCmp` already
generalizes `eq..ge`) and plug in only the leaf op. A new expr = pick a class +
supply the leaf.

**Layer 2 — the semantics live in a tiny `value` primitive set.** All the
subtlety (three-valued logic, type collation order, numeric canonicalization
`0`/`0.0`/`-0`, int-vs-float, coercion) is concentrated in ~6 `value.Value`
methods: `Equals`, `Compare`, `Collate`, `Truth`, `Type`/`Actual`, and
`NumberValue` arithmetic (`Add/Sub/Mult/Div/Mod`). Every expression is built on
these.

**The strategy that minimizes misses:**
1. **Port the primitives first** as byte-level `base` functions that match the cbq
   `value` methods exactly. n1k1 already has the comparison ones —
   `ValComparer.CompareWithType`/`Collate` mirror `value.Compare`/`Collate`, and
   `ValTruthy` mirrors `Truth()`. The main gap is **numeric arithmetic** (mirror
   `value.NumberValue`, incl. int64/float64 paths and div/mod-by-zero) plus
   confirming `ValTruthy` and the type mapping match cbq bit-for-bit.
2. **Port each skeleton** by copying cbq's `Evaluate` branch-for-branch into the
   class harness — same operand order, same MISSING/NULL branches.
3. **Differential-test** against the cbq fallback (the oracle): identical
   primitives + identical skeleton ⇒ identical results, unknown-value edges
   included.

Caveat — the parts that are *not* obvious and must be copied, not reinvented:
`AND`/`OR` and the conditional-unknown family have subtle 3-valued truth tables
(`NULL AND FALSE = FALSE`, `NULL AND TRUE = NULL`, …). Port `logic_and.go`,
`logic_or.go`, and `func_cond_unknown.go` exactly — and audit n1k1's *existing*
`and`/`or` against them.

## The porting recipe (per expression)

1. **Register** a name in `ExprCatalog` + add it (and its cbq function name) to
   `OptimizableFuncs` in `glue/expr_optimize.go` so the planner's tree rewrites to
   it.
2. **Setup vs per-row:** in the `ExprCatalogFunc`, do all constant/type work once
   (fold constant args like `ExprCmp` does; compile regex/LIKE patterns; resolve
   label indices), **`varLift`** the reused buffers.
3. **Per-row closure:** read operands (already native `ExprFunc`s), compute on
   bytes via the toolkit, **append into the lifted buffer**, return the `base.Val`.
4. **Semantics fidelity (non-negotiable):** match cbq's **three-valued logic**
   (MISSING vs NULL vs value propagation) and **collation/type ordering** exactly —
   use `ValComparer` for ordering; implement the MISSING/NULL short-circuit (a
   `DESIGN.md`/TODO idea: first MISSING/NULL can `goto` an outer handler).
5. **lz discipline:** follow `varLift` / `// !lz` so both interpreter and compiled
   paths stay valid; verify `intermed_build` still generates.
6. **Differential test:** run the same expression through the native path **and**
   the cbq fallback over a corpus and assert byte-identical results (see
   Correctness).

## Prioritization

Rank candidates by **per-row frequency × allocation-avoided × ease**:
- **Per-row, predicate-side operators win** (Tier A): they run in `WHERE`/`JOIN`/
  `ON` for every tuple. Arithmetic, `not`, `between`, `in`, `is-null/missing`,
  `like` (constant pattern) are the fat part of the curve.
- **One-shot / constant sub-exprs barely matter** — already folded once at setup.
- **Measure, don't guess:** use the allocation-profiling method from the benchmark
  work (`-memprofilerate=1` + `pprof -alloc_objects`, per `DESIGN-benchmark.md`) to
  see which `ExprTree`/`Convert`/`Evaluate`/`WriteJSON` sites dominate a real
  workload, and port those first.

## Correctness: the cbq fallback is the oracle

- **Keep the fallback forever** as the default for anything unported — it
  guarantees full SQL++ coverage; native impls are pure optimizations layered
  underneath.
- **Differential testing** is the safety mechanism: for each ported expr, generate
  inputs (incl. MISSING/NULL/mixed-type/edge values) and assert the native result
  equals the cbq result byte-for-byte. This slots into the existing compiler/
  conformance suite (a natural `DESIGN-testing.md` topic).
- The subtle bugs live in **N1QL semantics**: MISSING vs NULL propagation, type
  collation order (number < string < array < object …), numeric canonicalization
  (`0` vs `0.0` vs `-0`), and multi-byte string handling. `ValComparer` /
  `CanonicalJSON` already encode much of this — reuse them rather than re-deriving.

## Open questions
- **Sub-expression binding for comprehensions:** what's the cleanest register/temp-
  slot mechanism to feed per-element bytes into a child `ExprFunc` (Tier C)?
- **Structure-building output:** standardize a JSON-array/object *builder* over a
  lifted buffer (with `ValComparer` scratch for sort/dedup) so `array_*`/`object_*`
  share one allocation-free emitter.
- **MISSING/NULL short-circuit:** interpreter can early-return; can the compiled
  path `goto` an outer handler cleanly (per TODO)?
- **How far to auto-generate:** many string/num/date funcs are thin wrappers over
  Go stdlib — could a small codegen emit the boilerplate native `ExprFunc` from a
  spec table?
- **Coverage metric:** track "% of a workload's per-row expression evaluations
  served natively" as the north-star, not raw function count.

## Sources / references
- Principles: `DESIGN.md`. Prior notes: `TODO.md` (compiled-expr support,
  MISSING/NULL short-circuit, early-constant precompute à la `ExprCmp`,
  `ARRAY_POSITION` constant-arg example).
- Native impls: `engine/expr.go`, `engine/expr_bi.go`, `engine/expr_cmp.go`,
  `engine/expr_window.go`; toolkit in `base/base.go`, `base/compare.go`,
  `base/canonical.go`.
- Fallback + allowlist: `glue/expr.go`, `glue/expr_optimize.go`.
- Universe: `n1k1-query/expression/` (~357 types across ~95 files;
  `func_registry.go` ~410 entries).
