# Design: Native Expression Coverage

Status: proposal / for review

n1k1 evaluates a *small but growing* set of SQL++/N1QL expressions natively —
fast, byte-oriented, allocation-avoiding, compiler-friendly — and **delegates
everything else to the cbq-query (`n1k1-query`) expression engine**, whose
`Evaluate()` boxes into `value.Value` objects and produces transient garbage on
every row. This doc inventories what's native today, catalogs the universe that
remains, lays out a per-family porting roadmap, and records the lessons learned.

The goal: an incrementally growing library of native expressions, with the cbq
fallback kept forever as a correctness backstop.

## Status at a glance

Native coverage is listed authoritatively in the **inventory table** below.
Summary: arithmetic; comparisons; logical + `IS [NOT] NULL/MISSING/VALUED`; the
conditional-unknown selectors (`IFNULL`/`IFMISSING`/`IFMISSINGORNULL`/`NVL`/
`COALESCE`); `BETWEEN`; `IN`; the `IS_*` type checks; `||` concatenation; and
`CASE` (searched + simple) — plus the reusable `MakeBiExprFunc` /
`MakeTriExprFunc` / `MakeNaryExprFunc` harness family. Every op is validated by a
**differential test against cbq** (`glue/expr_arith_diff_test.go`) plus cold
interpreter unit tests.

**Measured win** (Apple M2 Pro, `test/benchmark/bench_expr_arith_test.go`): native
`a+b` is `0 B/op, 0 allocs/op` (31 ns) vs cbq's `Evaluate()` fallback at
`384 B/op, 8 allocs/op` (190 ns) — ~6× faster, zero per-eval garbage.

**Next:** `slice` navigation (blocked — see Lessons); `is [not] distinct from`
(binary, low priority); then Tier B (string/numeric/date functions). `LIKE`/
`REGEXP_*` are deliberately deferred — see Lessons.

## Why this matters

The fallback path (`glue/expr.go:ExprTree`) does three allocating things **per
row**:
1. **Box** `base.Vals` (`[]byte` per column) → a single `value.Value` document
   (`ConvertVals.Convert`).
2. **Evaluate** via cbq (`expr.Evaluate(v, context)`) — every sub-expression
   allocates intermediate `value.Value` objects.
3. **Unbox** the result `value.Value` → JSON bytes (`vResult.WriteJSON(...)`).

Over millions of rows that's a lot of GC. A native expression instead reads JSON
bytes with `jsonparser`, computes, and appends the result into a **reused**
`[]byte` buffer — zero steady-state garbage.

## n1k1's expression principles (from `DESIGN.md`)

- Values are **`base.Val` = `[]byte`** holding JSON — never `interface{}` /
  `map[string]interface{}` / `value.Value`.
- **No boxing:** compute on bytes; emit results as JSON text into a **lifted,
  reused** buffer (`buf[:0]`), not by building Go structures. ("No boxing" ≠ "no
  output structure" — even array/object results can be serialized into a reused
  buffer, as `ValComparer.CanonicalJSONWithType` already does.)
- **`jsonparser`** for navigation/parsing — returns slices into the input, no
  unmarshal garbage.
- **Positional "registers":** fields are pre-resolved to `vals[idx]` slots.
- **`lz` / lazy codegen:** expression code is written in the careful golang subset
  so `intermed_build` emits both an interpreter and a compiled path (`varLift`,
  `// !lz`, `LzScope`). A native expr = a setup function returning an `ExprFunc`
  closure; static work happens once, per-row work stays minimal.
- **Early-constant folding:** `sales < 1000` parses/types `1000` once at setup
  (see `ExprCmp`'s static path), not per row.

### The byte-level toolkit
New native exprs build on: `base.Val`/`Vals`, `base.Parse` (type+bytes via
jsonparser), `base.ParseFloat64`, `base.ParseNum`, `base.ValKind`, `base.ValPathGet`,
`base.ValTruthy`, `base.ValEqual*`, and especially **`base.ValComparer`**
(`CompareWithType`, `Collate`, `CanonicalJSON[WithType]`, `EncodeAsString`) — all
operate into caller-supplied buffers with no allocation and already encode N1QL
type/collation semantics.

## How it works today

- **Catalog:** `engine.ExprCatalog map[string]base.ExprCatalogFunc`
  (`base/vars.go`); an `ExprCatalogFunc(vars, labels, params, path)` returns an
  `ExprFunc = func(Vals, YieldErr) Val`.
- **Conversion:** `glue/expr_optimize.go:ExprTreeOptimize` walks the cbq
  `expression.Expression` tree from the planner and rewrites recognized nodes into
  native catalog params; anything else → the `ExprTree` cbq fallback. A single
  unsupported operand anywhere makes the **whole** expression fall back.
- **Recognition** is keyed off the cbq `Function.Name()` allowlist
  (`OptimizableFuncs`) plus special-cased node types (`Constant` → `json`, `Field`
  → `labelPath`, `SearchedCase`/`SimpleCase` → `case`).

### Native inventory (the authoritative "done" list)

| Name | File | Role |
|---|---|---|
| `json` | `engine/expr.go` | pre-parsed constant |
| `labelPath` | `engine/expr.go` | field/path access via `jsonparser` |
| `labelUint64` | `engine/expr.go` | binary uint64 → JSON int |
| `valsEncode` / `valsEncodeCanonical` | `engine/expr.go` | key encoding for maps |
| `and` / `or` | `engine/expr_bi.go` | short-circuit logical |
| `eq` `lt` `le` `gt` `ge` | `engine/expr_cmp.go` | comparisons (numeric fast path + `ValComparer`) |
| `add` `sub` `mult` `div` `mod` `idiv` `imod` `neg` | `engine/expr_arith.go` + `base/arith.go` | arithmetic (mirrors cbq `value.NumberValue`) |
| `not` `is_null` `is_not_null` `is_missing` `is_not_missing` `is_valued` `is_not_valued` | `engine/expr_pred.go` | unary predicates (byte-kind classified) |
| `is_array` `is_number` `is_string` `is_boolean` `is_object` `is_atom` | `engine/expr_type.go` | type checks (unary; MISSING/NULL passthrough) |
| `ifnull` `ifmissing` `ifmissingornull` `nvl` (`coalesce`) | `engine/expr_cond.go` | conditional-unknown selectors (n-ary) |
| `case` | `engine/expr_case.go` + `base.CaseReduce` | searched + simple CASE (n-ary; simple desugars to eq conds) |
| `nullif` `missingif` | `engine/expr_nullif.go` + `base.NullMissingIf` | NULLIF / MISSINGIF (binary) |
| `greatest` `least` | `engine/expr_greatest.go` + `base.GreatestLeast` | GREATEST / LEAST (n-ary; collation max/min) |
| `element` | `engine/expr_nav.go` + `base.ValElement` | array element nav `arr[idx]` (binary; negative index, requoted strings) |
| `concat` (`\|\|`) | `engine/expr_concat.go` + `base.NaryConcat` | string concatenation (n-ary) |
| `between` | `engine/expr_between.go` | BETWEEN (ternary; collation-order bounds) |
| `in` | `engine/expr_in.go` + `base.ValIn` | IN (array membership; 2-operand) |
| `window-partition-row-number`, `window-frame-*` | `engine/expr_window.go` | window helpers (FIRST/LAST/NTH/LEAD/LAG) |
| `exprStr` / `exprTree` | `glue/expr.go` | **the fallback** (parse / delegate to cbq) |

Reusable harnesses: `MakeBiExprFunc` (binary), `MakeTriExprFunc` (ternary),
`MakeNaryExprFunc` (variadic). Shared byte helpers in `base`: `ArithApply`/`Num`,
`ValKind` (VALUE/NULL/MISSING classification — the one place encoding
"empty==MISSING, leading-n==null"), `CondUnknownKeep`/`NaryFirstKept`,
`NaryConcat`, `CaseReduce`, `ValIn`.

Still **delegated:** `LIKE`/`REGEXP_*`, `is [not] distinct from`, `slice`
navigation, `TYPE()`/`IS_BINARY`, and the ~320 remaining scalar functions
(string/numeric/date/array/object/…).

## The universe & the gap

The cbq `expression/` package defines **~357 distinct scalar expression types
(~410 registry entries incl. aliases)** across ~95 files. By family, with
allocation profile:

| Family | ~Count | Profile |
|---|---|---|
| Arithmetic | 8 | scalar |
| Comparison (`eq…ge`, between, like, is-null/missing/valued/distinct) | ~15 | scalar |
| Logical (and/or/not) | 3 | scalar |
| Concatenation | 3 | builds string |
| Conditional (CASE ×2, IF*/NVL/COALESCE/NULLIF/GREATEST/LEAST/…) | ~14 | scalar |
| Navigation (field/element/slice) | ~5 | scalar (slice builds) |
| Collection (ANY/EVERY/ARRAY/MAP/OBJECT/FIRST/IN/WITHIN/EXISTS) | ~14 | **structure-building** |
| Construction (array/object literals) | ~7 | **structure-building** |
| String funcs | 32 | mostly scalar |
| Numeric/math funcs | 27 | scalar |
| Date/time funcs | 33 | scalar (some volatile) |
| Array funcs (`array_*`) | 34 | **mostly structure-building** |
| Object funcs (`object_*`) | 25 | **mostly structure-building** |
| Type check / conversion (`is_*`, `to_*`) | ~14 | scalar/structure |
| JSON / bitwise | ~13 | mixed / scalar |
| Regexp / LIKE / token | ~17 | regexp-based |
| Meta/admin, vector, specialized (crypto/curl/fusion/timeseries/RCTE/UDF/…) | ~47 | side-effecting / niche |

## Roadmap: supportability tiers (remaining work)

The done items are in the inventory table above; below is what's *left*, tiered
by how they fit the byte/register/lz model.

### Tier A — remaining scalar, byte-friendly, high per-row frequency
- **`slice` navigation `arr[start:end]`** — blocked on cbq internals, see Lessons.
- **`is [not] distinct from`** (binary, low priority) — null-safe equality via
  `ValComparer`.

### Tier B — scalar but needs parse+format into a reused buffer
- **String funcs** (upper/lower/trim/substr/length/position/replace/split/…) — map
  to Go `strings.*` into a lifted buffer; watch multi-byte variants.
- **Numeric/math** (abs/ceil/floor/round/sqrt/pow/exp/ln/trig/…) — `math.*` +
  `strconv.AppendFloat`.
- **Date/time (non-volatile)** — parse/format millis↔string into a buffer.
- **Bitwise, `to_*` conversions, JSON `encode/poly_length/encoded_size`** — scalar.
- **`LIKE`/`REGEXP_*` do NOT fit here** — they compile to a `regexp` and even the
  per-row match is outside the byte-reuse model (see Lessons). Delegated until a
  bespoke zero-alloc glob matcher is worth building.

### Tier C — structure-building (doable in bytes, higher cost)
- **Reader array/object ops that DON'T build output** — `array_length/contains/
  position`, `array_min/max/sum/avg`, `object_length/names`, `poly_length` — iterate
  with `jsonparser.ArrayEach`/`ObjectEach`, compute a scalar without materializing.
  Good ROI (Tier-B-like in practice).
- **Ops that DO build output** — `array_append/concat/sort/…`, `object_put/…`,
  array/object construction — emit JSON text into a lifted buffer (sort/dedup may
  need `ValComparer` scratch). Port common ones by frequency.
- **Comprehensions `ANY/EVERY/ARRAY/MAP/OBJECT/FIRST/WITHIN`** — bind a variable
  and evaluate a sub-expression per element (feed element bytes into a temp
  register slot, invoke the child `ExprFunc`). Highest-complexity of the portable
  set — needs the sub-expr binding plumbing.

### Tier D — delegate to cbq indefinitely
- **Volatile / non-deterministic:** `now_*`, `clock_*`, `random`, `uuid`.
- **Side-effecting / environmental:** `curl`, `meta`, `current_user(s)`, `version`,
  `node_*`, `abort`, `hashbytes`, `advisor`.
- **Heavy/niche:** fusion (BM25/RRF), timeseries, `recursive_cte`, distributed,
  UDFs, natural/AI, vector distance (route to the FTS/vector path in
  `DESIGN-indexing.md`).

These are infrequent and their allocation cost is negligible per query — not worth
the semantic-fidelity risk.

## How porting works — cbq's two-layer structure

cbq's scalar expressions follow a rigid, uniform pattern, so porting is near
line-for-line, and copying it faithfully is what minimizes edge-case misses.

**Layer 1 — a thin `Evaluate` skeleton.** Each expression (1) evaluates operands,
(2) propagates errors, (3) applies a standard MISSING/NULL branch, (4) delegates
the real work to a `value` primitive. The skeletons collapse into a few
**propagation classes**, each portable as one reusable harness:

| Class | Rule | Members |
|---|---|---|
| delegate-to-value | the value primitive encodes the 3-valued result | eq/ne/lt/le/gt/ge, between |
| MISSING-dominant → NULL | any MISSING → MISSING; else any non-typed → NULL; else compute | arithmetic, most scalar funcs |
| unknown-passthrough | MISSING→MISSING, NULL→NULL; else compute | not, `is_*`, most string/num/date |
| short-circuit / truth-table | special 3-valued handling | and, or, ifnull/coalesce, case |

**Layer 2 — semantics live in a tiny `value` primitive set.** All the subtlety
(three-valued logic, collation order, numeric canonicalization `0`/`0.0`/`-0`,
int-vs-float, coercion) is concentrated in ~6 `value.Value` methods: `Equals`,
`Compare`, `Collate`, `Truth`, `Type`/`Actual`, and `NumberValue` arithmetic. n1k1
mirrors these in `base` (`ValComparer.CompareWithType`/`Collate`, `ValTruthy`,
`Num`), so each new expr is: pick a class harness + supply the leaf op on bytes.

### The porting recipe (per expression)
1. **Register** the name in `ExprCatalog`, and add its **cbq `Function.Name()`** to
   `OptimizableFuncs` (or special-case the node type in `ExprTreeOptimize`).
2. **Setup vs per-row:** fold constant args once (like `ExprCmp`), resolve label
   indices, **`varLift`** reused buffers.
3. **Per-row:** read operand bytes, compute via the toolkit, append into the lifted
   buffer, return the `base.Val`.
4. **Semantics fidelity (non-negotiable):** match cbq's three-valued logic and
   collation/type ordering exactly — reuse `ValComparer`.
5. **lz discipline:** follow `varLift` / `// !lz`; verify `intermed_build`
   regenerates and `./intermed` builds.
6. **Differential test:** assert the native path is byte-identical to the cbq
   fallback over a corpus (incl. MISSING/NULL/mixed-type/edge values).

**Correctness — the cbq fallback is the oracle.** Keep it forever as the default;
native impls are optimizations layered underneath. The differential test is the
safety net (it caught the `Function.Name()` and MISSING-constant bugs below).

## Lessons learned

- **`Function.Name()`, not the registry alias.** `OptimizableFuncs` keys must match
  the canonical name each constructor's `Init()` sets — *no-underscore* for the
  unknown predicates (`isnull`), but *underscore* for the type checks (`is_array`).
- **A MISSING constant has no JSON form.** `value.WriteJSON` emits `"null"`, so
  `ExprTreeOptimize` must emit an *empty* json constant (→ MISSING) for a MISSING
  `Constant`; otherwise any native op given a `missing` literal wrongly sees NULL.
- **intermed codegen is fixed-arity, with an n-ary escape hatch.** Binary
  (`MakeBiExprFunc`) and unary (single-child) codegen cleanly. For **n-ary**
  (`MakeNaryExprFunc`): (a) build child ExprFuncs in a `// !lz` loop over the params
  (emitted verbatim, like `op_union`); (b) pre-declare `lzVals`/`lzYieldErr` with
  `// !lz` dummy decls so the executed reduce-call has them in the generator's
  scope; (c) keep the reduce in a plain `base` helper called in one `// !lz` line.
  A runtime loop over `[]ExprFunc` *inside the codegen'd eval body* fails.
  Also: an inline string literal in codegen'd code desyncs the tokenizer — use a
  named `base` const (e.g. `WarnDivideByZero`).
- **Func-value params are intermed-safe.** The harness can take an op as a `func`
  (method expression like `base.Num.Div`, or an adapter) instead of an int + switch
  — cleaner, and codegen handles it. (Dropped the `ArithApply` op-code switch.)
- **Regex/pattern exprs don't fit the zero-alloc model.** `LIKE`/`REGEXP_*` compile
  to a `regexp`; cbq caches the compiled regex per static pattern so its per-tuple
  cost is `re.Match`, but a native port has no good story — a dynamic pattern
  recompiles per tuple, and even a constant pattern's `regexp.Match` is outside the
  byte-reuse design. The principled fit is a hand-rolled allocation-free byte glob
  matcher (tractable but a bespoke reimplementation); until then, delegate.
- **Non-`Function` nodes need special optimizer handling.** `CASE`
  (`SearchedCase`/`SimpleCase`) isn't an `expression.Function` and has unexported
  fields; `ExprTreeOptimize` reaches its parts via `Children()` and lowers both to
  a flat `case` param list (simple → searched with `eq` conds).
- **`element` navigation is a `Function`; `slice` is blocked.** `arr[idx]` is
  `expression.Element` (a `BinaryFunctionBase`, `Name()=="element"`), so it rides
  the generic 2-operand optimizer path + `MakeBiExprFunc`; `base.ValElement` does
  the index math (negative-from-end, integral-only, MISSING/NULL propagation) and
  re-quotes string elements that `jsonparser` unquotes. `arr[start:end]`
  (`expression.Slice`) can't be lowered: its presence-of-bound state lives in
  *unexported* `start`/`end` bools with no accessor, so `Operands()` alone can't
  tell `arr[X:]` from `arr[:X]` (both are 2 operands); and `jsonparser` has no
  slice primitive. Unblocking needs an exported `HasStart()`/`HasEnd()` on the
  fork's `Slice` plus a real `base.ValSlice` byte helper — deferred.
- **DRY via shared `base` reducers + one classifier.** `ValKind` centralizes
  kind detection; `NaryFirstKept`/`NaryConcat`/`CaseReduce` are plain `base` helpers
  the lz harness calls in one line (so intermed doesn't try to fuse them).

## Prioritization

Rank by **per-row frequency × allocation-avoided × ease**. Predicate-side operators
(WHERE/JOIN/ON) win — arithmetic, comparisons, `not`, `between`, `in`, is-checks —
they run per tuple. One-shot/constant sub-exprs are folded once and barely matter.
**Measure, don't guess:** use `-memprofilerate=1` + `pprof -alloc_objects` (per
`DESIGN-benchmark.md`) to see which `ExprTree`/`Convert`/`Evaluate`/`WriteJSON`
sites dominate a real workload, and port those first.

## Open questions
- **Sub-expression binding for comprehensions** (Tier C): cleanest register/temp-
  slot mechanism to feed per-element bytes into a child `ExprFunc`?
- **A shared JSON array/object builder** over a lifted buffer (with `ValComparer`
  scratch for sort/dedup) so `array_*`/`object_*` share one allocation-free emitter.
- **Auto-generation:** many string/num/date funcs are thin stdlib wrappers — could
  a small codegen emit their boilerplate native `ExprFunc` from a spec table?
- **Coverage metric:** track "% of a workload's per-row expression evaluations
  served natively", not raw function count.

## Sources / references
- Principles: `DESIGN.md`; prior notes in `TODO.md`.
- Native impls: `engine/expr*.go`; byte toolkit in `base/` (`base.go`, `arith.go`,
  `compare.go`, `canonical.go`, `valkind.go`, `valin.go`).
- Fallback + optimizer: `glue/expr.go`, `glue/expr_optimize.go`.
- Differential + unit tests: `glue/expr_arith_diff_test.go`, `engine/expr_*_test.go`,
  `base/arith_test.go`; benchmark in `test/benchmark/bench_expr_arith_test.go`.
- Universe: `n1k1-query/expression/` (~357 types; `func_registry.go` ~410 entries).
