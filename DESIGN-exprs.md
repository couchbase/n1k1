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
**differential test against cbq** (`glue/expr_test.go`) plus cold interpreter
unit tests — but note these drive the **interpreter** native path only; the
**compiled** codegen is a separate concern covered by the compiler suite (see the
compiled-path caveat below and `DESIGN-testing.md`).

**Measured win** (Apple M2 Pro, `test/benchmark/bench_expr_arith_test.go`): native
`a+b` is `0 B/op, 0 allocs/op` (31 ns) vs cbq's `Evaluate()` fallback at
`384 B/op, 8 allocs/op` (190 ns) — ~6× faster, zero per-eval garbage.

**Next:** `slice` navigation (blocked — see Lessons); `is [not] distinct from`
(binary, low priority); then Tier B (string/numeric/date functions). `LIKE`/
`REGEXP_*` are deliberately deferred — see Lessons. **Done recently:** (a) native
exprs now run under an active scope (correlated subqueries / WITH / recursive CTEs)
when every field ref is provably local — the `strict` optimize path, lever #4; and
(b) logical `and`/`or` are now optimizer-wired with three-valued semantics — lever
#5 — so `WHERE`/`JOIN`/`ON` conjunctions avoid the boxed lane.

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

> ⚠️ **Compiled-path caveat for the n-ary ops** (`ifnull`/`ifmissing`/
> `ifmissingornull`/`nvl`, `greatest`/`least`, `concat`, `case`): these are
> correct in the **interpreter** but their **compiled** (`intermed`) path is
> broken — `MakeNaryExprFunc` can't split a variable-arity `lzChildren` setup out
> of the `emitCaptured` inline eval. It's dormant because no convertible compiled
> case reaches them, but they remain optimizer-wired, so a future one would fail
> at `go test ./test/tmp`. `and`/`or` sidestep this via the binary+fold route. See
> Lessons.

## The `exprTree` fallback's per-row cost (profiling, 2026-07)

The `exprTree` fallback (`glue/expr.go:ExprTree`) is the expensive path. For **every
row** its closure calls `ConvertVals.Convert` to rebuild the row's `base.Vals` (JSON
byte slices, one per label) into a cbq `value.Value` — an `objectValue`, each label
wrapped via `value.NewParsedValue` + `SetField` — then `expr.Evaluate(v)` and
`vResult.WriteJSON(&buf)` back to bytes. This byte-model ↔ value-model bridge — the
`Convert` round-trip — is the real cost of a delegated expression.

**Invocation path:** `Session.Run` → cbq plan → `glue.Conv` emits a `base.Op` whose
`Params` hold `["exprTree", <expression.Expression>]` for delegated exprs →
`op_project.go:MakeProjectFunc` → `engine.MakeExprFunc` dispatches
`ExprCatalog["exprTree"]` → `glue.ExprTree`. `ExprTree` first tries
`ExprTreeOptimize` (compiles `Constant`→`json`, `Field`→`labelPath`,
arithmetic/compare/CASE→native, **no Convert**); only if the whole tree isn't
recognized (or the row is `scoped`, i.e. a correlated subquery) does it fall to the
per-row Convert closure.

**Profiled query** (via `-profile-cpu`/`-profile-mem`):
`SELECT count(*) FROM (SELECT 1 FROM orders o1,o2,o3,o4) c` over 64-doc `orders`
→ 64⁴ = 16.8M join rows. Baseline: **10.4 GB allocated, 121.7 M allocs, ~1600 GCs,
~14.5 s.** The CPU profile is **~86% Go scheduler/GC** (`pthread_cond_signal` via
goroutine wake-ups) — a *symptom* of the allocation rate, not real work.

**Cost attribution** — an env-gated probe in the `ExprTree` closure
(`HACK_EXPR=skip` returns a preallocated constant with no work; `=nowrite` does
Convert+Evaluate but skips WriteJSON) isolates each stage:

| probe | alloc | allocs | GCs | time |
|---|---|---|---|---|
| baseline (Convert+Evaluate+WriteJSON) | 10.4 GB | 121.7 M | 1617 | 14.6 s |
| `nowrite` (Convert+Evaluate only) | 10.3 GB | 113.3 M | 1599 | 12.2 s |
| `skip` (none of the fallback) | **2.8 GB** | **63.0 M** | **433** | **8.9 s** |

- **`Convert`+`Evaluate` (building the objectValue) = 7.5 GB / 50 M allocs** — the
  allocation bulk (`objectValue.setField` 43.6%, `go_json.SimpleUnmarshal` 20.9%).
- **`WriteJSON` ≈ 0.1 GB** (it writes into a reused `buf`) but ~2.4 s of CPU.
- **Skeleton** (join fan-out, fetch, group, `ValsDeepCopy`) = **2.8 GB / 8.9 s** —
  irreducible for 16.8M cross-join rows.

So the fallback is **73% of the bytes (7.6 GB)** and 39% of the time — and the
allocation is **`Convert` building the object, not `WriteJSON`.** (Myth, disproven:
we first assumed `WriteJSON` would force materialization, making a lazy value moot.
It doesn't — `WriteJSON` barely allocates, and a lazy value can serialize straight
from the retained label bytes, so lazy Convert *is* viable — lever 3.)

**What is already optimized (don't re-do these):**
- `SELECT 1` (a constant projection) → a `Constant` node → `ExprTreeOptimize`
  compiles it to the native `json` func; **no Convert**. (Verified: a bare
  `SELECT 1 FROM orders` never enters the Convert closure.)
- `COUNT(*)` → its star operand has `operands[0] == nil`, so `VisitGroup`
  (`glue/conv.go`) already projects the constant `["json","true"]` as the aggregate
  input and the aggregate just counts. **No per-row Convert for `COUNT(*)`.**
  (Verified: `count(*)` reached Convert exactly once — the final result projection —
  not 16.8M times. A "special-case COUNT(\*)" optimization is unnecessary; it exists.)

**Where the 16.8M Converts actually came from: a whole-row `self` projection.**
`expression.Self` = "the entire current item/row as one value" (not a specific
field). `ExprTreeOptimize` can't reduce it to a label path, so a `self` projection
always falls to Convert and rebuilds the full multi-label object per row. It's
emitted by:
- **`SELECT *`** — e.g. `SELECT * FROM orders o1` projects `self`.
- **`FROM (subquery) AS x`** — the derived-table row-wrap (`VisitAlias`, the only
  source of `expression.NewSelf()`) packages each subquery row under its alias via a
  `self` projection. So `SELECT count(*) FROM (SELECT 1 FROM a,b,c,d) c` builds &
  serializes 16.8M full rows via `self`, only for `count(*)` to discard the values.
  (A plain identifier like `SELECT o1 FROM orders o1` is a field access `` `o1` ``,
  *not* self; `SELECT 1` is a constant. Both differ from `self`.)

**Levers tried that did NOT help (measured), and why:**
- *`ValsDeepCopy` prealloc reuse* (`base/stage.go`): a `:=` shadowed the outer
  `preallocVals`, so the recycled slice never reached `ValsDeepCopy` (it always
  `make`d). Fixed (a real latent bug) — but **inert here**: the recycled-buffer
  reuse depends on the consumer recycling a batch before the producer re-acquires,
  and with `batchChSize=0` the producer wins that race, so both buffers are
  re-`make`d ~per row regardless of the shadow.
- *Enlarging the `BatchCh` buffer* (`batchChSize` 0→4→…→256 in
  `glue/datastore_fetch.go`): allocation **count stayed flat**; larger buffers only
  keep more batches in flight (10.4 → 12.0 GB, more GCs). Reuse still never engages.

**Levers that would help (ranked for this count-over-join shape):**
1. *Discard-elision (dead-value elimination)* — **DONE (v1, `glue/discard_elision.go`).**
   A post-conversion pass over the `base.Op` tree: under a *value-agnostic group*
   (no GROUP BY keys + every aggregate operand is a constant `["json",…]` term, i.e.
   the `COUNT(*)`/`COUNT(<const>)` family), splice out the chain of `project` ops
   directly below it — their `self`-projected values are dead. Safe because `project`
   is strictly 1:1 (row count preserved → `COUNT(*)` unchanged), a value-agnostic
   group reads no input value label, and the tree is single-parent; only `project`
   is spliced (the walk stops at filter/order/limit, which change the row count).
   Measured on the profiled query: **10.4 → 2.8 GB, 121.7 → 63.0 M allocs, 1617 → 433
   GCs, 14.6 → 8.5 s** (the `HACK_EXPR=skip` floor), result unchanged. Toggle
   `glue.DiscardElision`; a differential test asserts on/off parity across query
   shapes (`COUNT(x)`, GROUP BY, DISTINCT, mixed aggs, subqueries all disqualify or
   are untouched). v1 is deliberately narrow; a general field-pruning liveness pass
   (materialize only referenced fields) is future work. NB: these `self` projections
   come from cbq's planner, so a further fix could be upstream.
2. *A `self`-projection byte path* — when a projection expr is exactly
   `expression.Self` (and not scoped), assemble the output JSON object directly from
   the input label bytes, skipping Convert+Evaluate+WriteJSON. Bounded; analogous to
   the existing `labelPath`/`json` fast paths.
3. *Lazy/on-demand `Convert`* — return a `value.Value` that materializes a field
   only on access **and serializes JSON straight from the retained label bytes**
   (the probe shows WriteJSON needn't build the map). Most general (helps
   field-selective queries like `WHERE a.x > 5` too), but needs a lazy multi-label
   `value.Value` impl (`Field`/`Fields`/`WriteJSON`/`Type`/`Actual`/annotations,
   plus the correlated-subquery scope-wrap which calls `Actual()`) —
   correctness-sensitive, tracked as future work.
4. *Native exprs under an active scope (`strict` optimize)* — **DONE
   (`glue/expr_optimize.go` + `glue/expr.go`).** Historically, whenever a scope was
   active (`corrParent`/`withScope` set — every correlated subquery, WITH-scoped
   query, and **recursive CTE step**), `ExprTree` skipped the native optimizer
   *wholesale* and sent every expression to the per-row Convert+Evaluate fallback —
   because the native `labelPath` can't see the parent scope, so an identifier that
   lives in the parent would be silently mis-navigated against the local row. But
   most scoped expressions reference **only local fields** (e.g. a recursive CTE
   step's `z→z²+c` arithmetic over its own `FROM <cte>` row); the scope is needed by
   a *sibling* term (the `FROM <cte>` self-scan), not by the arithmetic. `ExprTree`
   now passes `strict = scoped` to `ExprTreeOptimize`: in strict mode a `Field` that
   matches no real label prefix (i.e. would fall back to the whole-row `.`/`.*`
   default — the tell-tale of a possible parent reference) is a hard failure, so the
   optimizer accepts a scoped expression only when **every** field ref provably
   resolves to a local label. Fully-local scoped exprs then take the native byte
   path; anything touching the parent still falls through to the (parent-aware) cbq
   fallback. Purely additive: `strict=false` (unscoped) is unchanged.
   Measured on `examples/mandelbrot.sql++` (a `WITH RECURSIVE` fixpoint, 60 renders,
   profiled via `-profile-cpu`/`-profile-mem`): **~500 → ~208 MB alloc/render (−58%),
   ~52.3 → ~27.7 s CPU (−47%), ~573 → ~360 ms wall (−37%)**, output byte-identical;
   the scoped-expr Convert closure (`ExprTree.func2`) alone dropped **−81%** (12.2 →
   2.3 GB) as the 7 projection exprs went native. Then wiring `and`/`or` (below)
   took the step's `WHERE` predicate (`(…<4) and (…<45)`) native too, compounding to
   **~500 → ~129 MB alloc/render (−74% vs original), ~573 → ~285 ms wall (−50%)**.
   The remaining cost is the subquery *output* boxing (`EvaluateSubquery.func2`) —
   lever #3 territory.
5. *Wire logical `and`/`or` into the native optimizer* — **DONE (`engine/expr_logic.go`,
   `base/logic.go`, `glue/expr_optimize.go`).** The native `and`/`or` handlers existed
   but were unwired dead code (2-operand, `ValEqualTrue` instead of three-valued,
   never in `OptimizableFuncs`), so every `WHERE`/`JOIN`/`ON` conjunction fell to the
   cbq fallback. Reimplemented as correct three-valued binary AND/OR on bytes
   (`base.LogicAnd2`/`LogicOr2`, matching cbq's asymmetric MISSING/NULL precedence)
   via the compiled-safe `MakeBiExprFunc`; `ExprTreeOptimize` right-nests cbq's n-ary
   And/Or into binary applications and registers `and`/`or`. Predicate-side operators
   are the highest per-row frequency (§ Prioritization), so this is broad: any filter
   with `AND`/`OR` now avoids the boxed lane. Differential-tested vs cbq over the full
   truth table incl. MISSING/NULL and non-boolean truthiness
   (`TestLogicAndOrDifferentialVsCBQ`). NB: this took the binary route because the
   n-ary harness's *compiled* path is broken — see Lessons.

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
- **`emitCaptured` captures FROM the shared `lzVal` register, not into a fresh
  var.** A binary op that needs both operand values (`and`/`or`, `nullif`) must
  write each child into `lzVal` and read it out on the *next* line, mirroring
  `ExprCmp`:

  ```go
  lzVal = lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"
  lzValA := lzVal
  lzVal = lzB(lzVals, lzYieldErr) // <== emitCaptured: path "B"
  lzValB := lzVal
  ```

  `emitCaptured` *replaces the whole marked line* with the child's emitted code
  (which assigns `lzVal`), so a direct `lzValA := lzA(...)` bind is silently
  dropped and `lzValA` is undefined in the **compiled** path. The interpreter runs
  the source line literally, so it works there — which is exactly how this shipped
  broken in both `and`/`or` and the older `nullif`/`missingif` (see the
  compiled-path testing lesson in `DESIGN-testing.md`). Also: an inline string
  literal in codegen'd code desyncs the tokenizer — use a named `base` const
  (e.g. `WarnDivideByZero`).
- **`MakeNaryExprFunc`'s COMPILED path is broken and not trivially fixable.** Both
  `op_filter` and `op_project` inline each expression's per-row eval via
  `emitCaptured`. A binary op fits because it captures a FIXED set of child slots
  (`"A"`/`"B"`) inlined straight into the reduce expression. An n-ary op instead
  needs a runtime-sized `lzChildren` slice built once (setup) plus a reduce that
  loops over it (per-row eval) — and there is no way to split that variable-arity
  setup out of the single inlined eval `emitCaptured` gives you. So compiled n-ary
  emits an undefined `lzChildren` (or, with a plain-func reducer, executes the
  reducer at generation time and panics on nil children). This is **pre-existing
  and unexercised** — no compiled suite case reaches `ifnull`/`greatest`/`least`/
  `concat`/`case` natively, so it stayed hidden until wiring `and`/`or` first put
  a native combining-op into a compiled filter. Two ways forward, neither done:
  (a) fold the foldable ops (`ifnull`/`greatest`/`least`/`concat`) to right-nested
  binary in the optimizer — as `and`/`or` already do (exact under their
  associative three-valued semantics), giving them the proven `MakeBiExprFunc`
  compiled path; (b) a capture-stack rework for `CASE`, which is not foldable.
  Interpreter n-ary is correct and unaffected. **Risk:** these ops remain in
  `OptimizableFuncs`, so a future *convertible* compiled query using one would
  fail at `go test ./test/tmp`; the safety valve is to de-wire them (fall back to
  the working `exprTree` path) until (a)/(b) lands.
- **The n-ary→binary fold is exact under three-valued logic.** `and`/`or` prove
  the pattern: `ExprTreeOptimize` right-nests cbq's n-ary And/Or into
  `MakeBiExprFunc` applications, with `base.LogicAnd2`/`LogicOr2` holding the
  two-operand semantics (incl. the AND-MISSING-over-NULL / OR-NULL-over-MISSING
  asymmetry). Verified byte-identical to cbq in `TestLogicAndOrDifferentialVsCBQ`
  (interpreter) and exercised in the compiled path by the `naryProjectCase` cases
  in `test/cases.go`.
- **Func-value params are intermed-safe (via `LzExprFmt` + positional tokens).**
  A shared harness can take an op as a real `func` value (a package-level func like
  `base.StrCaseUpper`, or a method expression like `base.Num.Div`) instead of an int
  op-code + `switch`. Two codegen pieces make this work:
  - **Fix A — `base.LzExprFmt`.** The generator rendered a compile-time-known "live"
    expr with `%#v`, which for a func yields an un-compilable pointer literal
    `(func(int)bool)(0x…)`. `LzExprFmt` renders a func by its qualified Go name
    (`base.StrCaseUpper`, `math.Abs`) so the compiled path emits a genuine call;
    everything non-func stays byte-identical to `%#v`. Only NAMED, exported funcs
    in a package the generated `tmp` imports work — not closures (uncallable runtime
    name) nor unexported engine-local funcs. Put the leaf logic in `base`, pass
    `base.Foo`. A nil/unresolvable func falls back to `%#v` (valid `(T)(nil)`).
  - **Fix B — positional arg tokens.** The `varLift` (reused-buffer) pass and the
    `SimpleExprRE` (live-expr/func) pass used to append fmt args in pass order (all
    buffer args, then all live-expr args), which mis-ordered them when a func
    placeholder and a `varLift` placeholder shared one emitted line. Each pass now
    plants a positional `\x00<n>\x00` token; a final left-to-right scan emits `%s`
    and collects args in on-line order. So transform + encode can collapse to one
    line, e.g. `lzBufPre = base.EncodeStr(c, caseFn(lzDecoded), lzBufPre)`.
  Applied so far: the type-check predicates (`is_*` → `base.TypeIs*`) and the
  case-transform family (`upper`/`lower`/`title` → `base.StrCase*`). The token merge
  is behavior-preserving — regen produced byte-identical output for every other op.
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
  `compare.go`, `canonical.go`, `val_kind.go`, `val_in.go`).
- Fallback + optimizer: `glue/expr.go`, `glue/expr_optimize.go`.
- Differential + unit tests: `glue/expr_test.go`, `engine/expr_*_test.go`,
  `base/arith_test.go`; benchmark in `test/benchmark/bench_expr_arith_test.go`.
- Universe: `n1k1-query/expression/` (~357 types; `func_registry.go` ~410 entries).
