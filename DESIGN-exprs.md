# Design: Native Expression Coverage

Status: proposal / for review

n1k1 evaluates a growing set of SQL++/N1QL expressions **natively** (byte-oriented,
allocation-avoiding, compiler-friendly) and **delegates the rest to the cbq-query
(`n1k1-query`) engine**, whose `Evaluate()` boxes into `value.Value` and garbages per
row. The native library grows incrementally; the cbq fallback stays **forever** as a
correctness backstop.

## Contents

- [Status at a glance](#status-at-a-glance)
- [Why native matters (the fallback's cost)](#why-native-matters-the-fallbacks-cost)
- [Design principles & the byte-level toolkit](#design-principles--the-byte-level-toolkit)
- [How it works today](#how-it-works-today)
  - [Catalog, conversion, recognition](#catalog-conversion-recognition)
  - [Native inventory (the authoritative "done" list)](#native-inventory-the-authoritative-done-list)
  - [Known-broken & caveats](#known-broken--caveats)
- [Profiling the fallback (2026-07)](#profiling-the-fallback-2026-07)
  - [Cost attribution](#cost-attribution)
  - [Where the Converts come from: the `self` projection](#where-the-converts-come-from-the-self-projection)
  - [Levers tried that did NOT help](#levers-tried-that-did-not-help)
  - [Levers that help (ranked)](#levers-that-help-ranked)
- [The universe & the gap](#the-universe--the-gap)
- [Roadmap: supportability tiers](#roadmap-supportability-tiers)
- [How porting works — cbq's two-layer structure](#how-porting-works--cbqs-two-layer-structure)
- [Lessons learned](#lessons-learned)
- [Codegen ergonomics — reducing lz boilerplate](#codegen-ergonomics--reducing-lz-boilerplate)
- [Prioritization](#prioritization)
- [Open questions](#open-questions)
- [Sources / references](#sources--references)

## Status at a glance

Native coverage is authoritative in the **inventory table** below; everything not
there is delegated. Every op is validated by a **differential test against cbq**
(`glue/expr_test.go`) plus cold interpreter unit tests — these drive the
**interpreter** path only; the **compiled** codegen is covered by the compiler suite
(see compiled-path caveat and `DESIGN-testing.md`).

**Measured win** (Apple M2 Pro, `test/benchmark/bench_expr_arith_test.go`): native
`a+b` is `0 B/op, 0 allocs/op` (31 ns) vs cbq fallback `384 B/op, 8 allocs/op`
(190 ns) — ~6× faster, zero per-eval garbage.

**Done recently:** (a) native exprs run under an active scope (correlated subqueries /
WITH / recursive CTEs) when every field ref is provably local — `strict` optimize
(lever #4); (b) logical `and`/`or` optimizer-wired with three-valued semantics (lever
#5), so `WHERE`/`JOIN`/`ON` conjunctions avoid boxing; (c) the whole-row `self` /
`SELECT *` projection now assembles JSON from label bytes instead of boxing (lever #2);
(d) grouped aggregates (`count(*)`, `sum(x)`, incl. `count(*)+1`) read the group's
`^aggregates|…` value natively (lever #6); (e) result-row output (`OnRow` / `Result.Rows`)
encodes boxing-free via `ConvertBytes` (lever #7).

**Next:** `slice` navigation (blocked); `is [not] distinct from` (binary, low
priority); Tier B (string/numeric/date). `LIKE`/`REGEXP_*` deferred.

## Why native matters (the fallback's cost)

The fallback (`glue/expr.go:ExprTree`) does three allocating things **per row**:
1. **Box** `base.Vals` (`[]byte` per column) → one `value.Value` (`ConvertVals.Convert`).
2. **Evaluate** via cbq (`expr.Evaluate(v, context)`) — sub-expressions allocate.
3. **Unbox** the result → JSON bytes (`vResult.WriteJSON(...)`).

A native expr instead reads JSON bytes with `jsonparser`, computes, and appends into
a **reused** `[]byte` buffer — zero steady-state garbage.

## Design principles & the byte-level toolkit

Principles (from `DESIGN.md`):

- Values are **`base.Val` = `[]byte`** holding JSON — never `interface{}` /
  `map[string]interface{}` / `value.Value`.
- **No boxing:** compute on bytes; emit JSON into a **lifted, reused** buffer
  (`buf[:0]`). Even array/object results serialize into that buffer (as
  `ValComparer.CanonicalJSONWithType` does) — "no boxing" ≠ "no output structure".
- **`jsonparser`** for navigation — returns slices into the input, no unmarshal garbage.
- **Positional "registers":** fields pre-resolved to `vals[idx]` slots.
- **`lz` / lazy codegen:** exprs in the golang subset so `intermed_build` emits both
  interpreter and compiled paths (`varLift`, `// !lz`, `LzScope`). A native expr = a
  setup func returning an `ExprFunc` closure; static work runs once.
- **Early-constant folding:** `sales < 1000` types `1000` once at setup (see
  `ExprCmp`'s static path), not per row.

**Toolkit.** Build on `base.Val`/`Vals`, `base.Parse`, `base.ParseFloat64`,
`base.ParseNum`, `base.ValKind`, `base.ValPathGet`, `base.ValTruthy`,
`base.ValEqual*`, and especially **`base.ValComparer`** (`CompareWithType`, `Collate`,
`CanonicalJSON[WithType]`, `EncodeAsString`) — all operate into caller-supplied
buffers with no allocation and encode N1QL type/collation semantics.

## How it works today

### Catalog, conversion, recognition

- **Catalog:** `engine.ExprCatalog map[string]base.ExprCatalogFunc` (`base/vars.go`);
  `ExprCatalogFunc(vars, labels, params, path)` returns `ExprFunc = func(Vals, YieldErr) Val`.
- **Conversion:** `glue/expr_optimize.go:ExprTreeOptimize` walks the cbq
  `expression.Expression` tree and rewrites recognized nodes into native catalog
  params; anything else → the `ExprTree` cbq fallback. A single unsupported operand
  anywhere makes the **whole** expression fall back.
- **Recognition** keys off the cbq `Function.Name()` allowlist (`OptimizableFuncs`) +
  special-cased nodes (`Constant`→`json`, `Field`→`labelPath`,
  `SearchedCase`/`SimpleCase`→`case`).

**Harnesses:** `MakeBiExprFunc`, `MakeTriExprFunc`, `MakeNaryExprFunc`. Shared `base`
helpers: `ArithApply`/`Num`, `ValKind` (VALUE/NULL/MISSING classification — the one
place encoding "empty==MISSING, leading-n==null"), `CondUnknownKeep`/`NaryFirstKept`,
`NaryConcat`, `CaseReduce`, `ValIn`.

### Native inventory (the authoritative "done" list)

| Name | File | Role |
|---|---|---|
| `json` | `engine/expr.go` | pre-parsed constant |
| `labelPath` | `engine/expr.go` | field/path access via `jsonparser` |
| `labelUint64` | `engine/expr.go` | binary uint64 → JSON int |
| `valsEncode` / `valsEncodeCanonical` | `engine/expr.go` | key encoding for maps |
| `and` / `or` | `engine/expr_logic.go` + `base/logic.go` | three-valued logical (binary harness; optimizer folds cbq's n-ary And/Or into right-nested binary) |
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
| `upper` `lower` `title` `trim` `ltrim` `rtrim` `reverse` `length` `contains` `position0` `position1` | `engine/expr_str.go` + `base/str.go` | unary string transforms (shared `exprStrTransform`: decode → `func([]byte)[]byte` → re-encode). TRIM/LTRIM/RTRIM 2-arg (cutset) fall back |
| `replace` | `engine/expr_str.go` + `base.StrReplaceAll` | REPLACE 3-arg (ternary → buffer); 4-arg count form falls back |
| `substr` (`substr0` `substr1`) | `engine/expr_str.go` + `base.StrSubstr` | SUBSTR (byte-based, `inRunes=false`), 2/3-arg, arity-dispatched to `substr{0,1}_{2,3}`. Rune-based `mb_substr*` fall back |
| `split` | `engine/expr_str.go` + `base.StrSplit*` | SPLIT (1-arg whitespace / 2-arg sep), arity-dispatched. **First structure-building native expr** — emits a JSON array via `EncodeAsString` (appends, unlike `EncodeStr`) |
| `lpad` `rpad` | `engine/expr_str.go` + `base.StrPad*` | LPAD/RPAD (byte-based), 2/3-arg, arity-dispatched. `l <= len(s)` truncates; else pad-fill. Rune-based `mb_*pad` fall back |
| `abs` `ceil` `floor` `sqrt` `exp` `ln` `log` `sign` `degrees` `radians` `sin` `cos` `tan` `asin` `acos` `atan` `power` `atan2` | `engine/expr_math.go` + `base/math.go` | numeric math (func-passing: stdlib `math.*` / `base.Math*`) |
| `round` `trunc` | `engine/expr_math.go` + `base.RoundFloat`/`TruncFloat` | ROUND (half-to-even) / TRUNC, 1/2-arg, arity-dispatched. `round_nearest` falls back |
| `date_part_millis` | `engine/expr_date.go` + `base.DatePartMillis` | DATE_PART_MILLIS 2-arg — component from epoch millis in process-local zone (port of cbq `millisToTime`+`datePart`). 3-arg named-TZ and other date funcs fall back |
| `to_boolean` `to_string` `to_number` | `engine/expr_type.go` + `base/type.go` | scalar type conversions |
| `array_length` `array_count` `array_sum` `array_avg` `array_min` `array_max` `array_contains` `array_position` | `engine/expr_array.go` + `base/array.go` | reader array ops (no materialization) |
| `object_length` `poly_length` | `engine/expr_object.go` + `base/object.go` | object/collection reader ops (unary; op-code dispatch; count via `jsonparser.ObjectEach`/`ArrayEach`, no materialization) |
| `window-partition-row-number`, `window-frame-*` | `engine/expr_window.go` | window helpers (FIRST/LAST/NTH/LEAD/LAG) |
| `exprStr` / `exprTree` | `glue/expr.go` | **the fallback** (parse / delegate to cbq) |

Still **delegated:** `LIKE`/`REGEXP_*`, `is [not] distinct from`, `slice`
navigation, `TYPE()`/`IS_BINARY`, and the ~320 remaining scalar functions.

### Known-broken & caveats

- **⚠️ Compiled-path BROKEN for n-ary ops** (`ifnull`/`ifmissing`/`ifmissingornull`/
  `nvl`, `greatest`/`least`, `concat`, `case`): correct in the **interpreter**, but
  the **compiled** (`intermed`) path is broken — `MakeNaryExprFunc` can't split a
  variable-arity `lzChildren` setup out of the `emitCaptured` inline eval. Dormant (no
  convertible compiled case reaches them) but optimizer-wired, so a future one fails at
  `go test ./test/tmp`. `and`/`or` sidestep via binary+fold. Detail in Lessons.
- **Encoder caveat (formfeed / backspace).** `base.EncodeStr` (stdlib `encoding/json`)
  escapes formfeed/backspace as two-char `\f`/`\b`; cbq's encoder emits six-char
  ``/``. Both valid JSON, but bytes differ — a native string func whose
  OUTPUT holds a literal formfeed/backspace won't be byte-identical. Cosmetic;
  differential tests avoid these chars. Fix routes `EncodeStr` through cbq's encoder
  (touches `ValComparer`) — deferred.

## Profiling the fallback (2026-07)

For **every row** the `exprTree` closure calls `ConvertVals.Convert` to rebuild the
row's `base.Vals` into a cbq `value.Value` (`objectValue`, each label via
`value.NewParsedValue` + `SetField`), then `expr.Evaluate(v)` and `WriteJSON`. This
`Convert` round-trip (byte-model ↔ value-model bridge) is the real cost.

**Invocation path:** `Session.Run` → cbq plan → `glue.Conv` emits a `base.Op` with
`Params` `["exprTree", <expression.Expression>]` → `op_project.go:MakeProjectFunc` →
`engine.MakeExprFunc` → `ExprCatalog["exprTree"]` → `glue.ExprTree`. It first tries
`ExprTreeOptimize` (**no Convert**); only if the whole tree isn't recognized (or the
row is `scoped`) does it fall to the per-row Convert closure.

**Profiled query** (`-profile-cpu`/`-profile-mem`):
`SELECT count(*) FROM (SELECT 1 FROM orders o1,o2,o3,o4) c` over 64-doc `orders` →
64⁴ = 16.8M join rows. Baseline: **10.4 GB, 121.7 M allocs, ~1600 GCs, ~14.5 s.**
CPU profile is **~86% Go scheduler/GC** (`pthread_cond_signal` from goroutine
wake-ups) — a symptom of the allocation rate, not real work.

### Cost attribution

Env-gated probe in the `ExprTree` closure: `HACK_EXPR=skip` returns a constant with
no work; `=nowrite` does Convert+Evaluate but skips WriteJSON.

| probe | alloc | allocs | GCs | time |
|---|---|---|---|---|
| baseline (Convert+Evaluate+WriteJSON) | 10.4 GB | 121.7 M | 1617 | 14.6 s |
| `nowrite` (Convert+Evaluate only) | 10.3 GB | 113.3 M | 1599 | 12.2 s |
| `skip` (none of the fallback) | **2.8 GB** | **63.0 M** | **433** | **8.9 s** |

- **`Convert`+`Evaluate` = 7.5 GB / 50 M allocs** — the bulk (`objectValue.setField`
  43.6%, `go_json.SimpleUnmarshal` 20.9%).
- **`WriteJSON` ≈ 0.1 GB** (writes into a reused `buf`) but ~2.4 s CPU.
- **Skeleton** (join fan-out, fetch, group, `ValsDeepCopy`) = **2.8 GB / 8.9 s** —
  irreducible for 16.8M cross-join rows.

So the fallback is **73% of the bytes (7.6 GB)** and 39% of the time — and the
allocation is **`Convert` building the object, not `WriteJSON`.** (Disproven myth:
`WriteJSON` doesn't force materialization, so a lazy value can serialize straight from
retained label bytes — lever 3 is viable.)

**Already optimized (don't re-do):**
- `SELECT 1` → `Constant` → native `json`; **no Convert** (verified: bare `SELECT 1
  FROM orders` never enters Convert).
- `COUNT(*)` → star operand `operands[0] == nil`, so `VisitGroup` (`glue/conv.go`)
  projects the constant `["json","true"]` as aggregate input; **no per-row Convert**
  (verified: Convert reached once, the final result projection, not 16.8M times).

### Where the Converts come from: the `self` projection

**The 16.8M Converts came from a whole-row `self` projection.** `expression.Self` =
the entire current row as one value. It has no label path to reduce to, so a `self`
projection *fell* to Convert and rebuilt the full object per row — now assembled
natively from label bytes for the common case (lever #2 below). Emitted by:
- **`SELECT *`** — projects `self`.
- **`FROM (subquery) AS x`** — the derived-table row-wrap (`VisitAlias`, the only
  source of `expression.NewSelf()`) packages each subquery row under its alias via a
  `self` projection. So the profiled query builds/serializes 16.8M full rows only for
  `count(*)` to discard them.

(A plain identifier `SELECT o1 FROM orders o1` is a field access, not self;
`SELECT 1` is a constant.)

### Levers tried that did NOT help

- **`ValsDeepCopy` prealloc reuse** (`base/stage.go`): a `:=` shadowed the outer
  `preallocVals`. Fixed (a real latent bug) — but inert: with `batchChSize=0` the
  producer re-`make`s before the consumer recycles.
- **Enlarging `BatchCh`** (`batchChSize` 0→256, `glue/datastore_fetch.go`): alloc
  count stayed flat; bigger buffers only keep more batches in flight (10.4→12.0 GB).

### Levers that help (ranked)

Ranked for this count-over-join shape:

**1. Discard-elision (dead-value elimination) — DONE (v1, `glue/discard_elision.go`).**
A post-conversion pass over the `base.Op` tree: under a *value-agnostic group* (no
GROUP BY keys + every aggregate operand a constant `["json",…]` term — the
`COUNT(*)`/`COUNT(<const>)` family), splice out the `project` chain below it (their
`self`-projected values are dead). Safe because `project` is 1:1 (row count
preserved), a value-agnostic group reads no value label, and the tree is
single-parent; the walk stops at filter/order/limit (they change row count). Measured:
**10.4→2.8 GB, 121.7→63.0 M allocs, 1617→433 GCs, 14.6→8.5 s** (the `HACK_EXPR=skip`
floor), result unchanged. Toggle `glue.DiscardElision`; a differential test asserts
on/off parity. v1 is narrow; a general field-pruning liveness pass is future work.
(These `self` projections come from cbq's planner — a further fix could be upstream.)

**2. A `self`-projection byte path — DONE (`engine/expr_self.go`, `base/self.go`,
`glue/self.go`).** When a projection is exactly `expression.Self` (unscoped) over
plain `.["name"]` field labels, `engine.ExprSelf` assembles the row object's JSON
straight from the input label bytes into a reused buffer (`base.ValsSelfObject`),
skipping Convert+Evaluate+WriteJSON — zero steady-state garbage, in both the
interpreter and compiled paths. Gated by `selfNativeSpec` / `glue.SelfProjectNative`;
path stars (`SELECT p.*`), whole-row `.`, nested paths and `.*` stay boxed.

*Why the old blocker dissolved:* the star value is the assembled row object
(`SELECT * FROM sales` → `{"sales":{doc}}`), and cbq's `objectValue.WriteJSON` emits
keys **sorted** (recursively). ExprSelf emits them in **label order** — a byte-level,
not value-level, difference. That was thought to require reproducing cbq's sorted-key
serialization byte-identically, but n1k1's result comparison is key-order-insensitive
(the test harness `canonJSON` / `rowsMatch` re-normalize both sides), so the suite +
compiler differential pass unchanged. Byte-identical output (to match Couchbase's exact
wire bytes) would still need a verified canonical serializer — the formfeed/backspace
encoder gap, `TODO(encoder-fidelity)` in `base/compare.go`, plus unaudited float/escape
cases — tracked separately. Bench (`test/benchmark/bench_self_test.go`): native
6.6 ns/op, 0 allocs vs boxed 784 ns/op, 25 allocs/op.

**3. Lazy/on-demand `Convert`** — return a `value.Value` that materializes a field
only on access and serializes JSON straight from retained label bytes. Most general
(helps field-selective queries like `WHERE a.x > 5`), but needs a lazy multi-label
`value.Value` impl (`Field`/`Fields`/`WriteJSON`/`Type`/`Actual`/annotations + the
correlated-subquery scope-wrap that calls `Actual()`) — correctness-sensitive, future
work.

**4. Native exprs under an active scope (`strict` optimize) — DONE
(`glue/expr_optimize.go` + `glue/expr.go`).** Previously, when a scope was active
(`corrParent`/`withScope` — every correlated subquery, WITH query, recursive CTE
step), `ExprTree` skipped the native optimizer wholesale, since `labelPath` can't see
the parent scope. But most scoped exprs reference **only local fields** (e.g. a
recursive CTE step's `z→z²+c` arithmetic over its own row); the scope is needed by a
*sibling* term. `ExprTree` now passes `strict = scoped`: in strict mode a `Field`
matching no local label prefix (the tell-tale of a parent reference) is a hard
failure, so the optimizer accepts a scoped expr only when **every** field ref resolves
locally. Purely additive; `strict=false` unchanged. Measured on
`examples/mandelbrot.sql++` (`WITH RECURSIVE`, 60 renders): **~500→~208 MB
alloc/render (−58%), ~52.3→~27.7 s CPU (−47%), ~573→~360 ms wall (−37%)**,
byte-identical; the scoped-expr Convert closure alone dropped **−81%** (12.2→2.3 GB).
Wiring `and`/`or` compounded to **~500→~129 MB alloc/render (−74%), ~573→~285 ms wall
(−50%)**. Remaining cost is subquery *output* boxing (`EvaluateSubquery.func2`) —
lever #3 territory.

**5. Wire logical `and`/`or` into the native optimizer — DONE
(`engine/expr_logic.go`, `base/logic.go`, `glue/expr_optimize.go`).** The handlers
existed but were unwired dead code (2-operand, `ValEqualTrue` not three-valued, absent
from `OptimizableFuncs`), so every `WHERE`/`JOIN`/`ON` conjunction fell to cbq.
Reimplemented as correct three-valued binary AND/OR on bytes (`base.LogicAnd2`/
`LogicOr2`, cbq's asymmetric MISSING/NULL precedence) via `MakeBiExprFunc`;
`ExprTreeOptimize` right-nests cbq's n-ary And/Or into binary. Predicate-side ops are
highest per-row frequency, so this is broad. Differential-tested over the full truth
table incl. MISSING/NULL (`TestLogicAndOrDifferentialVsCBQ`). Binary route because the
n-ary harness's compiled path is broken — see Lessons.

**6. Read grouped aggregates natively — DONE (`glue/expr_optimize.go`).** A projected
aggregate (`SELECT count(*)`, `sum(x)`, …) used to box once per group: the term stayed
an `["exprTree", <agg>]` param, so cbq's `Aggregate.Evaluate` ran against the grouped
`AnnotatedValue` just to fetch the value it already holds. But the group op appends each
finalized `Agg.Result` as JSON bytes under the label `^aggregates|<agg.String()>`
(`op_group.go`). `ExprTreeOptimize` now recognizes an `algebra.Aggregate` leaf and emits
`["labelPath", "^aggregates|…"]` when that label is present — a native byte read, no box
(mirrors the ORDER-BY aggregate→labelPath rewrite in `conv.go`). Because the optimizer
recurses, aggregate-containing expressions go native too: `count(*)+1` →
`add(labelPath, json)`, `sum(a)/count(*)` → `div(labelPath, labelPath)`. These cases
also become compiler-bakeable once they no longer box. Falls back to the box when the
aggregate isn't materialized (no matching label).

**7. Boxing-free output encoding — DONE (`glue/self.go` `ConvertVals.ConvertBytes`).**
The *output* side had the same waste: `session.yieldVals` rendered each result row via
`cv.Convert(vals)` → `v.Actual()` → `json.Marshal` (box, unbox to maps, re-serialize).
`ConvertBytes` renders the row's JSON straight from the label bytes into a reused buffer
(reusing `base.ValsSelfObject`), for the common shapes — all-`.["name"]` fields, a lone
`.` (RAW), a lone `.*` — falling back to the box otherwise. The plan is computed once
from the labels (fixed per result). Feeds `OnRow` (reused buffer; the contract already
forbids retaining) and `Result.Rows`. Keys in projection order, not sorted (value-equal;
`TestConvertBytesParity`). Bench (`SELECT a,b,c` row): native 12.7 ns/op, 0 allocs vs
boxed 776 ns/op, 22 allocs/op. This is the row-lifecycle counterpart to lever #2 (the
projection side) — the remaining `Convert` call sites (`exec.go`, `subquery.go`,
`expr.go`) genuinely need a `value.Value` for cbq interop, so they stay boxed.

## The universe & the gap

The cbq `expression/` package defines **~357 distinct scalar types (~410 registry
entries incl. aliases)** across ~95 files:

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

## Roadmap: supportability tiers

Done items are in the inventory table; below is what's *left*, tiered by fit to the
byte/register/lz model.

### Tier A — remaining scalar, byte-friendly, high per-row frequency
- **`slice` navigation `arr[start:end]`** — blocked on cbq internals (see Lessons).
- **`is [not] distinct from`** (binary, low priority) — null-safe equality via `ValComparer`.

### Tier B — scalar but needs parse+format into a reused buffer
- **String funcs** (upper/lower/trim/substr/…) — Go `strings.*` into a lifted buffer;
  watch multi-byte variants.
- **Numeric/math** (abs/ceil/floor/round/sqrt/pow/…) — `math.*` + `strconv.AppendFloat`.
- **Date/time (non-volatile)** — millis↔string into a buffer. `date_part_millis`
  (2-arg) **done** (no string parsing). The rest hinge on cbq's date-STRING parser
  (`strToTime`'s large `_DATE_FORMATS` list) and named-TZ loading (`loadLocation`) — a
  fiddly port where any mismatch breaks the differential, so delegated:
  `str_to_millis`/`millis_to_str`/`date_part_str`/`date_add_*`/`date_diff_*`/
  `date_trunc_*`/`weekday_*`. `now_*`/`clock_*` are volatile (Tier D). Next tractable:
  the millis-only funcs (`date_add_millis`/etc.) — no string parsing.
- **Bitwise, `to_*` conversions, JSON `encode/poly_length/encoded_size`** — scalar.
- **`LIKE`/`REGEXP_*` do NOT fit here** — compile to `regexp`, outside byte-reuse
  (see Lessons). Delegated until a bespoke zero-alloc glob matcher is worthwhile.

### Tier C — structure-building (doable in bytes, higher cost)
- **Reader ops (no output build)** — `array_length/contains/position`,
  `array_min/max/sum/avg`, `object_length`, `poly_length` **done** (iterate via
  `jsonparser.ArrayEach`/`ObjectEach`, compute a scalar without materializing — good
  ROI). Remaining readers: `object_names` (builds a sorted array — structure), the
  bare-identifier / whole-row operand case (e.g. `OBJECT_LENGTH(o)` still boxes because
  `o` isn't recognized as a native labelPath).
- **Ops that DO build output** — `array_append/concat/sort/…`, `object_put/…`,
  literals — emit JSON into a lifted buffer (sort/dedup may need `ValComparer`
  scratch). Port common ones by frequency.
- **Comprehensions `ANY/EVERY/ARRAY/MAP/OBJECT/FIRST/WITHIN`** — bind a variable,
  evaluate a sub-expr per element (feed element bytes into a temp register, invoke the
  child `ExprFunc`). Highest-complexity portable set — needs sub-expr binding plumbing.

### Tier D — delegate to cbq indefinitely
- **Volatile / non-deterministic:** `now_*`, `clock_*`, `random`, `uuid`.
- **Side-effecting / environmental:** `curl`, `meta`, `current_user(s)`, `version`,
  `node_*`, `abort`, `hashbytes`, `advisor`.
- **Heavy/niche:** fusion (BM25/RRF), timeseries, `recursive_cte`, distributed, UDFs,
  natural/AI, vector distance (route to the FTS/vector path in `DESIGN-indexing.md`).

Infrequent, negligible per-query alloc cost — not worth the semantic-fidelity risk.

## How porting works — cbq's two-layer structure

cbq's scalar expressions follow a rigid, uniform pattern, so porting is near
line-for-line; copying it faithfully minimizes edge-case misses.

**Layer 1 — a thin `Evaluate` skeleton:** (1) evaluate operands, (2) propagate errors,
(3) standard MISSING/NULL branch, (4) delegate real work to a `value` primitive. These
collapse into a few **propagation classes**, each a reusable harness:

| Class | Rule | Members |
|---|---|---|
| delegate-to-value | the value primitive encodes the 3-valued result | eq/ne/lt/le/gt/ge, between |
| MISSING-dominant → NULL | any MISSING → MISSING; else any non-typed → NULL; else compute | arithmetic, most scalar funcs |
| unknown-passthrough | MISSING→MISSING, NULL→NULL; else compute | not, `is_*`, most string/num/date |
| short-circuit / truth-table | special 3-valued handling | and, or, ifnull/coalesce, case |

**Layer 2 — semantics in a tiny `value` primitive set.** All subtlety (three-valued
logic, collation order, numeric canonicalization `0`/`0.0`/`-0`, int-vs-float,
coercion) lives in ~6 `value.Value` methods: `Equals`, `Compare`, `Collate`, `Truth`,
`Type`/`Actual`, `NumberValue` arithmetic. n1k1 mirrors these in `base`
(`ValComparer.CompareWithType`/`Collate`, `ValTruthy`, `Num`), so each new expr = pick
a class harness + supply the leaf op on bytes.

### The porting recipe (per expression)
1. **Register** in `ExprCatalog`; add its cbq `Function.Name()` to `OptimizableFuncs`
   (or special-case the node in `ExprTreeOptimize`).
2. **Setup vs per-row:** fold constant args once (like `ExprCmp`), resolve label
   indices, **`varLift`** reused buffers.
3. **Per-row:** read operand bytes, compute via the toolkit, append into the lifted
   buffer, return the `base.Val`.
4. **Semantics fidelity (non-negotiable):** match cbq's three-valued logic and
   collation/type ordering exactly — reuse `ValComparer`.
5. **lz discipline:** follow `varLift` / `// !lz`; verify `intermed_build` regenerates
   and `./intermed` builds.
6. **Differential test:** byte-identical to the cbq fallback over a corpus (incl.
   MISSING/NULL/mixed-type/edge).

**The cbq fallback is the oracle** — keep it forever as the default; native impls are
optimizations underneath. The differential test caught the `Function.Name()` and
MISSING-constant bugs below.

## Lessons learned

### Optimizer & recognition
- **`Function.Name()`, not the registry alias.** `OptimizableFuncs` keys must match
  the canonical name `Init()` sets — no-underscore for unknown predicates (`isnull`),
  underscore for type checks (`is_array`).
- **A MISSING constant has no JSON form.** `value.WriteJSON` emits `"null"`, so
  `ExprTreeOptimize` must emit an *empty* json constant (→ MISSING) for a MISSING
  `Constant`; else a native op given a `missing` literal wrongly sees NULL.
- **Non-`Function` nodes need special handling.** `CASE`
  (`SearchedCase`/`SimpleCase`) isn't an `expression.Function` and has unexported
  fields; the optimizer reaches its parts via `Children()` and lowers both to a flat
  `case` param list (simple → searched with `eq` conds).

### Navigation
- **`element` is a `Function`; `slice` is blocked.** `arr[idx]` is
  `expression.Element` (`Name()=="element"`), rides the 2-operand path +
  `MakeBiExprFunc`; `base.ValElement` does the index math (negative-from-end,
  integral-only, MISSING/NULL propagation) and re-quotes strings `jsonparser` unquotes.
  `arr[start:end]` (`expression.Slice`) can't be lowered: presence-of-bound state is in
  *unexported* `start`/`end` bools (no accessor), so `Operands()` can't tell `arr[X:]`
  from `arr[:X]`; and `jsonparser` has no slice primitive. Unblocking needs exported
  `HasStart()`/`HasEnd()` on the fork plus a `base.ValSlice` byte helper.
- **Regex/pattern exprs don't fit the zero-alloc model.** `LIKE`/`REGEXP_*` compile to
  a `regexp`; even a constant pattern's `regexp.Match` (cbq caches it per static
  pattern) is outside byte-reuse, and a dynamic pattern recompiles per tuple. The
  principled fit is a hand-rolled allocation-free byte glob matcher — until then,
  delegate.

### Compiled (intermed) codegen
- **`emitCaptured` captures FROM the shared `lzVal` register, not into a fresh var.** A
  binary op needing both operands (`and`/`or`, `nullif`) must write each child into
  `lzVal` and read it out on the *next* line, mirroring `ExprCmp`:

  ```go
  lzVal = lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"
  lzValA := lzVal
  lzVal = lzB(lzVals, lzYieldErr) // <== emitCaptured: path "B"
  lzValB := lzVal
  ```

  `emitCaptured` *replaces the whole marked line* with the child's emitted code, so a
  direct `lzValA := lzA(...)` bind is silently dropped and `lzValA` is undefined in the
  compiled path (the interpreter runs the source line literally, so it works there —
  how this shipped broken in `and`/`or` and older `nullif`/`missingif`; see
  `DESIGN-testing.md`). Also: an inline string literal in codegen'd code desyncs the
  tokenizer — use a named `base` const.
- **`MakeNaryExprFunc`'s COMPILED path is broken and not trivially fixable.**
  `op_filter`/`op_project` inline each per-row eval via `emitCaptured`. A binary op
  fits (FIXED `"A"`/`"B"` capture); an n-ary op needs a runtime-sized `lzChildren`
  slice built once (setup) + a reduce loop (per-row), and there's no way to split that
  variable-arity setup out of the single inlined eval. So compiled n-ary emits an
  undefined `lzChildren` (or runs the reducer at generation time and panics on nil
  children). Pre-existing/unexercised — hidden until wiring `and`/`or` put a native
  combining-op into a compiled filter. Two ways forward, neither done: (a) fold the
  foldable ops (`ifnull`/`greatest`/`least`/`concat`) to right-nested binary (as
  `and`/`or` do), giving them the proven `MakeBiExprFunc` compiled path; (b) a
  capture-stack rework for `CASE` (not foldable). Interpreter n-ary unaffected.
  **Risk:** these stay in `OptimizableFuncs`, so a future *convertible* compiled query
  using one fails at `go test ./test/tmp`; the safety valve is to de-wire them until
  (a)/(b) lands.
- **The n-ary→binary fold is exact under three-valued logic.** `and`/`or` prove it:
  `ExprTreeOptimize` right-nests cbq's n-ary And/Or into `MakeBiExprFunc` with
  `base.LogicAnd2`/`LogicOr2` (incl. AND-MISSING-over-NULL / OR-NULL-over-MISSING
  asymmetry). Byte-identical (`TestLogicAndOrDifferentialVsCBQ`); compiled path
  exercised by `naryProjectCase` in `test/cases.go`.
- **Func-value params are intermed-safe (via `LzExprFmt` + positional tokens).** A
  harness can take an op as a real `func` value (`base.StrCaseUpper`, method expr
  `base.Num.Div`) instead of an int op-code + `switch`. Two codegen fixes:
  - **Fix A — `base.LzExprFmt`.** `%#v` renders a func as an un-compilable pointer
    literal; `LzExprFmt` renders it by qualified Go name so the compiled path emits a
    real call; non-func stays `%#v`-identical. Only NAMED exported funcs in a package
    `tmp` imports work — not closures nor unexported funcs. Put leaf logic in `base`.
    Nil/unresolvable → `%#v`.
  - **Fix B — positional arg tokens.** The `varLift` and `SimpleExprRE` passes appended
    fmt args in pass order, mis-ordering them when a func and `varLift` placeholder
    shared a line. Each pass now plants a positional `\x00<n>\x00` token; a final
    left-to-right scan emits `%s` and collects args in on-line order, so transform +
    encode collapse to one line: `lzBufPre = base.EncodeStr(c, caseFn(lzDecoded), lzBufPre)`.

  Applied: `is_*` → `base.TypeIs*`; `upper`/… → `base.StrCase*`; unary+binary math →
  stdlib `math.Abs`/`math.Pow` (`base.MathSign`/`Degrees`/`Radians` for non-stdlib;
  `test/tmp` auto-imports `math`); arithmetic `add`/… → `base.ArithAdd`/… (uniform
  `(a, b Num) (Num, bool)` sig). Deleted the `MathAbs…`/`ArithAdd…` int-op-code enums
  and switches; regen byte-identical elsewhere. Remaining int-op-code: `array_reduce`
  (`length`/`count`/`sum`/`avg`), kept — its four ops share one iteration pass and the
  switch is a trivial selector.

### DRY
- **DRY via shared `base` reducers + one classifier.** `ValKind` centralizes kind
  detection; `NaryFirstKept`/`NaryConcat`/`CaseReduce` are plain `base` helpers the lz
  harness calls in one line (so intermed doesn't fuse them).

## Codegen ergonomics — reducing lz boilerplate

Native exprs are written twice-over: the same lz source runs in the interpreter
*and* is scraped line-by-line by `intermed_build` to emit the compiled path. That
double duty forces the verbose `lz*` shape and blocks the obvious fix — HOFs /
closures that factor the repeated MISSING→parse→NULL skeleton. Two moves cut most
of the boilerplate anyway, **without touching the codegen**: base propagation-class
combinators, and name→leaf tables.

### The core constraint

`intermed_build` is a **line-oriented text translator**, not a compiler
(`cmd/intermed_build/build.go`). Each lz line becomes an `Emit`/`EmitLift` printf of
its text (`EmitBlock`, build.go:234); a setup-time value is rendered by
`base.LzExprFmt` (`%#v`, but a *named* func by its qualified Go name). Operand
evaluation is spliced by the `// <== emitCaptured:` marker, which replaces the whole
marked line with the child's already-emitted code (so an operand must be written
`lzVal = lzX(...) // <== emitCaptured: path "X"` then read out on the next line).

The translator only ever sees **text** — it cannot look *inside* a runtime `func`
value. A HOF that takes `leaf func(...)Val` and calls it in a per-row loop works in
the interpreter, but the compiled path emits a *call to the closure value*, not its
body — so the leaf reaches compiled output only if it is a **named** func emitted on
one `LzExprFmt` line. (And `emitCaptured` has no loop form, which is why the compiled
n-ary path stays broken — see Known-broken & caveats.)

### The chosen approach

Keep everything the translator must inline as a **fixed-shape shared harness with a
named leaf**; let only the leaf vary per op, emitted by name. Two layers exploit that:

- **Propagation-class combinators (`base`).** The 3-valued skeleton (`MISSING →
  MISSING; else non-value → NULL; else compute`) used to be re-expanded inline in
  every op's leaf. It now lives in a few `base` combinators — `MissingDominantBiNum`,
  `UnknownPassthroughUnNum`, `UnknownPassthroughMathUn`, `UnknownPassthroughRound1` /
  `MissingDominantRound2`, `StrTransformInto` — each taking the captured operand
  *values* plus a **named** leaf (`base.ArithAdd`, `math.Abs`, `base.StrCaseUpper`,
  …). The lz leaf collapses to one line, so ~40 numeric/string ops shed their
  ~12-line branch.

- **Name→leaf tables + one adapter per family.** A per-op constructor that only
  passed a leaf to a shared harness (`ExprAbs`/`ExprAdd`/`ExprIsNull`/…) collapses
  into a `map[string]<leaf>` table plus one `xxxOp` adapter that closes over the leaf
  and defers to the harness; `init()` registers each row in a `for`-range loop.
  Shipped tables: math (`mathUnaryFuncs`, `mathBiFuncs`, `roundTruncFuncs`),
  `arithOps`, `strTransformFuncs`, IS predicates (`isPredicateFuncs`), IS type-checks
  (`isTypeFuncs`), array readers (`arrayReduceFuncs`), conditional-unknown
  (`condFuncs`), and comparisons (`cmpFuncs`, with a `swap` flag so GT/GE reuse
  LT/LE).

**Why it needs no `intermed_build` change.** The tables, `for`-range `init()` loops,
and adapters are all plain (non-lz) Go, so the translator copies them through
verbatim; the leaf value still rides the closure into the shared harness, where the
existing `LzExprFmt`-by-name emission fires. Verified: the generated compiled queries
emit the exact named leaf (`math.Abs`, `base.RoundFloat`, `base.ArithDiv`,
`base.TypeIsArray`, `base.CondIfNull`, …), and interpreter + compiled + cbq agree
across `test-suite` / `test-compiler` / `test-suite-all`.

**When to table.** Only for a family of *several* single-line-body constructors —
the table + adapter carries a fixed cost, so a 1–2-entry table isn't worth it (kept
`nullif`/`missingif`, `to_*`, `array_min`/`max`/`contains`/`position`, and the
arity-split `substr`/`pad` families as direct funcs). The multi-operand string
builders (REPLACE/SUBSTR/LPAD/RPAD/SPLIT) also stay hand-written: their leaves thread
`ValComparer` and aren't a single named leaf.

### Considered, not pursued

- **Fold the foldable n-ary ops (`ifnull`/`greatest`/`least`/`concat`) to right-nested
  binary** (as `and`/`or` already do) to give them the working compiled path — viable
  and codegen-free, not yet done. `case` can't fold (ordered when/then capture).
- **A capture-stack rework so `emitCaptured` supports variable arity** — the only way
  to compile `case`; large, touches the codegen, deferred.
- **A spec-table *generator*** that emits the harness source from a declarative
  `{name, arity, class, leaf}` table — a superset of the runtime tables above; worth
  it only once the hand-written families grow further.
- **Teaching the translator to trace closures** — rejected; it would turn the
  line-oriented translator into a real compiler for benefit the named-leaf route
  already gives.


## Prioritization

Rank by **per-row frequency × allocation-avoided × ease**. Predicate-side operators
(WHERE/JOIN/ON) win — arithmetic, comparisons, `not`, `between`, `in`, is-checks run
per tuple. One-shot/constant sub-exprs fold once and barely matter. **Measure, don't
guess:** `-memprofilerate=1` + `pprof -alloc_objects` (per `DESIGN-benchmark.md`) to
find the dominant `ExprTree`/`Convert`/`Evaluate`/`WriteJSON` sites; port those first.

## Open questions
- **Sub-expression binding for comprehensions** (Tier C): cleanest register/temp-slot
  mechanism to feed per-element bytes into a child `ExprFunc`?
- **A shared JSON array/object builder** over a lifted buffer (with `ValComparer`
  scratch for sort/dedup) so `array_*`/`object_*` share one allocation-free emitter.
- **Auto-generation:** could codegen emit boilerplate native `ExprFunc`s for the thin
  stdlib-wrapper string/num/date funcs from a spec table? (Partly answered — the thin
  wrappers are now runtime `name → leaf` tables, no generator needed; see
  [Codegen ergonomics](#codegen-ergonomics--reducing-lz-boilerplate). A full generator
  would additionally emit the shared-harness source itself.)
- **Coverage metric:** track "% of a workload's per-row expression evaluations served
  natively", not raw function count.

## Sources / references
- Principles: `DESIGN.md`; prior notes in `TODO.md`.
- Native impls: `engine/expr*.go`; byte toolkit in `base/` (`base.go`, `arith.go`,
  `compare.go`, `canonical.go`, `val_kind.go`, `val_in.go`).
- Fallback + optimizer: `glue/expr.go`, `glue/expr_optimize.go`.
- Differential + unit tests: `glue/expr_test.go`, `engine/expr_*_test.go`,
  `base/arith_test.go`; benchmark in `test/benchmark/bench_expr_arith_test.go`.
- Universe: `n1k1-query/expression/` (~357 types; `func_registry.go` ~410 entries).
