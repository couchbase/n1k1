# Design: Native Expression Coverage

n1k1 evaluates a growing set of SQL++/N1QL expressions **natively** (byte-oriented,
allocation-avoiding, compiler-friendly) and **delegates the rest to cbq-query
(`n1k1-query`)**, whose `Evaluate()` boxes into `value.Value` and garbages per row. The
native library grows incrementally; **the cbq fallback stays forever as the correctness
oracle** (every native op is validated by a differential test against it,
`glue/expr_test.go`).

## Status & remaining TODOs

_Last reviewed: 2026-07-23._

**Done:** a large native byte-lane (zero steady-state garbage) covering arithmetic,
comparisons, three-valued logical AND/OR, predicates/type-checks, CASE and
conditional-unknown selectors, string/numeric/math funcs, constant-pattern REGEXP,
array/object readers + builders + literals, comprehensions (ANY/EVERY/FIRST/ARRAY/OBJECT,
single-binding), the `self` / `SELECT *` projection, native grouped-aggregate reads, and
boxing-free result output. Window functions are a broadly-complete op subsystem (see
below). Measured (`bench_expr_arith_test.go`): native `a+b` is 0 B/0 allocs (31 ns) vs
cbq fallback 384 B/8 allocs (190 ns).

**Remaining (headline TODOs):**
- [ ] **Native-lane projection for ASOF / subquery results** — carry the value on the
  byte lane, skip the `Convert`→`value.Value` round-trip; boxed-value/JSON alloc churn
  still dominates some workloads and this cuts the bulk of it.
- [ ] **Lazy/on-demand `Convert`** — a `value.Value` that materializes fields only on
  access, serializing from label bytes; helps field-selective queries (`WHERE a.x > 5`).
- [ ] **Decompose boxed CTE / derived-table rows into native columns at capture** + a
  typed/parsed temp materialization (columnar territory, `DESIGN-col.md`).
- [ ] **Port more boxed funcs:** the date-STRING family (`str_to_millis`/`millis_to_str`/
  `date_diff_*`/`date_trunc_*`), multi-binding + `WITHIN k:v` comprehensions, `slice`
  navigation (blocked on a cbq-fork accessor).
- [ ] **`LIKE` / dynamic-pattern `REGEXP_*`** — need a hand-rolled zero-alloc byte glob
  matcher (they don't fit byte-reuse: `regexp` compiles).
- [ ] **Window perf tail:** sliding SUM/AVG over non-integer data re-folds; arbitrary
  frames want O(N log N) segment trees; decode operand once per partition row.

## Why native matters

The fallback (`glue/expr.go:ExprTree`) does three allocating things **per row**: (1)
**box** `base.Vals` → one `value.Value` (`ConvertVals.Convert`), (2) **evaluate** via cbq
(sub-exprs allocate), (3) **unbox** the result → JSON bytes (`WriteJSON`). A native expr
instead reads bytes with `jsonparser`, computes, and appends into a **reused** `[]byte`
buffer — zero steady-state garbage.

## Design principles & the byte-level toolkit

- Values are **`base.Val` = `[]byte`** holding JSON — never `interface{}` /
  `map[string]interface{}` / `value.Value`.
- **No boxing:** compute on bytes; emit JSON into a **lifted, reused** buffer (`buf[:0]`).
  Array/object results serialize into that buffer too ("no boxing" ≠ "no output structure").
- **`jsonparser`** for navigation (returns slices into the input, no unmarshal garbage).
- **Positional "registers":** fields pre-resolved to `vals[idx]` slots.
- **Early-constant folding:** `sales < 1000` types `1000` once at setup, not per row.
- **`lz` / lazy codegen:** exprs live in the golang subset so `intermed_build` emits both
  the interpreter and compiled paths. A native expr = a setup func returning an `ExprFunc`
  closure; static work runs once. ⚠ **lz rule:** a build-time control-flow block opens
  with `// !lz` (`if X { // !lz`, `for … { // !lz`); its **closing `}` needs no marker**
  — `intermed_build` auto-classifies a bare `}` at the same indent as an open `// !lz`
  brace as build-time (gofmt guarantees the alignment). `} // !lz` still works; `} else {
  // !lz` (close-and-reopen) still needs the marker.

**Toolkit:** `base.Val`/`Vals`, `base.Parse`, `base.ParseFloat64`, `base.ParseNum`,
`base.ValKind`, `base.ValPathGet`, `base.ValTruthy`, `base.ValEqual*`, and especially
**`base.ValComparer`** (`CompareWithType`, `Collate`, `CanonicalJSON[WithType]`,
`EncodeAsString`) — all operate into caller buffers with no allocation and encode N1QL
type/collation semantics.

## How it works today

- **Catalog:** `engine.ExprCatalog map[string]base.ExprCatalogFunc` (`base/vars.go`);
  `ExprCatalogFunc(vars, labels, params, path)` returns `ExprFunc = func(Vals, YieldErr) Val`.
- **Conversion:** `glue/expr_optimize.go:ExprTreeOptimize` walks the cbq expression tree
  and rewrites recognized nodes into native catalog params; anything else → the `ExprTree`
  cbq fallback. **A single unsupported operand anywhere makes the whole expression fall
  back.**
- **Recognition** keys off cbq `Function.Name()` (`OptimizableFuncs` allowlist) + special
  nodes (`Constant`→`json`, `Field`→`labelPath`, `SearchedCase`/`SimpleCase`→`case`).
- **Harnesses:** `MakeBiExprFunc`, `MakeTriExprFunc`, `MakeNaryExprFunc` (n-ary compiled
  path is broken — see Lessons).

### Native inventory (the authoritative "done" list)

| Name | File | Role |
|---|---|---|
| `json` / `labelPath` / `labelUint64` | `engine/expr.go` | constant / field access / binary uint64 |
| `valsEncode` / `valsEncodeCanonical` | `engine/expr.go` | key encoding for maps |
| `and` / `or` | `engine/expr_logic.go` + `base/logic.go` | three-valued logical (binary harness; optimizer right-nests cbq's n-ary And/Or) |
| `eq` `lt` `le` `gt` `ge` | `engine/expr_cmp.go` | comparisons (numeric fast path + `ValComparer`) |
| `add` `sub` `mult` `div` `mod` `idiv` `imod` `neg` | `engine/expr_arith.go` + `base/arith.go` | arithmetic (mirrors cbq `NumberValue`) |
| `not` `is_null`/`is_not_null`/`is_missing`/`is_not_missing`/`is_valued`/`is_not_valued` | `engine/expr_pred.go` | unary predicates |
| `is_array`/`is_number`/`is_string`/`is_boolean`/`is_object`/`is_atom` | `engine/expr_type.go` | type checks |
| `ifnull` `ifmissing` `ifmissingornull` `nvl`/`coalesce` | `engine/expr_cond.go` | conditional-unknown selectors (n-ary) |
| `case` | `engine/expr_case.go` + `base.CaseReduce` | searched + simple CASE (simple desugars to eq) |
| `nullif` `missingif` | `engine/expr_nullif.go` | NULLIF / MISSINGIF |
| `greatest` `least` | `engine/expr_greatest.go` | collation max/min (n-ary) |
| `element` | `engine/expr_nav.go` + `base.ValElement` | array element `arr[idx]` (neg index, requotes strings) |
| `concat` (`\|\|`) | `engine/expr_concat.go` | string concatenation (n-ary) |
| `between` `in` `is_distinct_from`/`is_not_distinct_from` | `engine/expr_between.go`/`_in.go`/`_distinct.go` | BETWEEN / IN / null-safe (in)equality |
| string transforms: `upper` `lower` `title` `trim`(2-arg cutset) `ltrim` `rtrim` `reverse` `length` `contains` `position0/1` `repeat` `replace`(3-arg) `substr` `lpad` `rpad` `split` | `engine/expr_str.go` + `base/str.go` | decode→transform→re-encode into a lifted buffer; arity-dispatched. `repeat` overflow → runtime error (`base.ErrStrRepeat`). `mb_*`/4-arg `replace`/`round_nearest` fall back |
| `regexp_contains` `regexp_like` | `engine/expr_str.go` | **constant** pattern only (compiled once); dynamic → fallback |
| math: `abs` `ceil` `floor` `round`(1/2-arg) `trunc` `sqrt` `exp` `ln` `log` `sign` `degrees` `radians` `sin` `cos` `tan` `asin` `acos` `atan` `power` `atan2` | `engine/expr_math.go` + `base/math.go` | func-passing to stdlib `math.*` / `base.Math*` |
| `date_part_millis` `date_add_millis` | `engine/expr_date.go` + `base/datetime.go` | millis math in the process zone; named-TZ + date-STRING funcs fall back |
| `to_boolean` `to_string` `to_number` | `engine/expr_type.go` | scalar conversions |
| array readers: `array_length`/`count`/`sum`/`avg`/`min`/`max`/`contains`/`position` | `engine/expr_array.go` + `base/array.go` | iterate, no materialization |
| array builders: `array_append`/`prepend`/`concat` (variadic ≥2), `array_sort`/`reverse`/`flatten` | `engine/expr_array.go` | splice/reshape into a lifted buffer via eager-Vals reducers / pooled `KeyVals`. `array_distinct` skipped (nondeterministic) |
| object readers/builders: `object_length` `poly_length`; `object_names`/`values`/`pairs`; `object_add`/`put`/`remove`(≥2)/`concat`(≥2) | `engine/expr_object.go` + `base/object.go` | key-sorted re-emit; eager-Vals reducers for variadic |
| `array_construct` `object_construct` | `engine/expr_array.go`/`_object.go` | `[...]` / `{...}` literals (key-sorted, last-wins, MISSING/NULL rules) |
| comprehensions (`ANY`/`EVERY`/`FIRST`/`ARRAY`/`OBJECT`) | `engine/expr_coll.go` | single-binding, `IN`/`WITHIN` + named `k:v IN`; bound var → appended register slot; native in BOTH lanes |
| `exprStr` / `exprTree` | `glue/expr.go` | **the fallback** (parse / delegate to cbq) |

Still **delegated:** `LIKE`, dynamic `REGEXP_*`, `slice`, `TYPE()`/`IS_BINARY`, the
date-STRING family, multi-binding/`WITHIN k:v` comprehensions, and the Tier-D niche/
volatile funcs (see roadmap).

### Known-broken & caveats (footguns)

- **⚠️ Compiled-path BROKEN for n-ary ops** (`ifnull`/`ifmissing`/`ifmissingornull`/`nvl`,
  `greatest`/`least`, `concat`, `case`): correct in the interpreter, but the compiled
  (`intermed`) path is broken — `MakeNaryExprFunc` can't split a variable-arity
  `lzChildren` setup out of the `emitCaptured` inline eval. Dormant (no convertible
  compiled case reaches them) but they stay in `OptimizableFuncs`, so a future convertible
  compiled query using one **fails at `go test ./test/tmp`**. `and`/`or` sidestep via
  binary+fold. See Lessons for the two ways out.
- **⚠️ Encoder formfeed/backspace.** `base.EncodeStr` (stdlib) escapes `\f`/`\b` as two
  chars; cbq emits the six-char `\uxxxx`. Both valid JSON, bytes differ — a native string
  func whose OUTPUT holds a literal formfeed/backspace won't be byte-identical. Cosmetic;
  differential tests avoid these chars. `TODO(encoder-fidelity)` in `base/compare.go`.

## Window functions

SQL++ window functions (`… OVER (PARTITION BY … ORDER BY … <frame>)`) are **ops**
(`engine/op_window.go`), not `ExprCatalog` entries; their values reach the projection via
the same `^aggregates|<agg.String()>` native label path GROUP BY uses. (The old
`engine/expr_window.go` `ExprCatalog` stubs are superseded/dead.) The subsystem was fully
non-functional before this arc (the boxed cbq window `Evaluate` panics on n1k1's plain,
non-`AnnotatedValue` rows) and is now broadly complete.

### Architecture

Two chained ops per window function (`glue/conv.go:VisitWindowAggregate` builds one chain
per function so each `^aggregates|…` column accumulates):

- **`OpWindowPartition`** — requires its child sorted by PARTITION BY + ORDER BY (cbq's
  planner inserts the sort). Buffers **one partition at a time** into a `store.Heap`;
  drains + resets on a partition boundary (or end-of-stream via a `yieldErr(nil)` drain).
  Memory O(largest partition) — necessary, since FOLLOWING frames need look-ahead.
  Optionally appends a trailing **`^worderby`** column (Params[4]: `"value"` = single
  numeric ORDER BY value for RANGE arithmetic; `"tuple"` = canonical ORDER BY tuple for
  GROUPS/peer/rank `bytes.Equal`).
- **`OpWindowFrames`** — per row: `CurrentUpdate` sets the frame `[Include.Beg, End)`,
  then computes the value and appends it under `^aggregates|…`. Four value kinds by param:
  **aggregate** (fold a `base.Agg`), **ranking** (`WindowRankValue`), **offset**
  (`StepToOffset`), **ratio** (`RATIO_TO_REPORT` = operand / frame SUM). Boundary math is
  in `base/agg_window.go` (`base.WindowFrame`). A RANGE frame with only CURRENT ROW /
  UNBOUNDED bounds is rewritten to **GROUPS** (peer semantics) so multi-column ORDER BY
  works; a numeric-offset RANGE stays single-column.

### What works

Validated by the ORDER-sensitive oracle `glue/window_test.go` + `glue/op_order_test.go`
(the default suite had zero `OVER (` cases — how the whole subsystem stayed broken
unnoticed) and the data-backed gsi window corpus.

| Capability | Notes |
|---|---|
| Frame **aggregates** | SUM/COUNT/AVG/MIN/MAX + any `base.AggCatalog` agg, over ROWS/RANGE/GROUPS, composite-key multi-column ORDER BY, empty/inverted frames, PARTITION BY, multiple funcs/query |
| **Ranking** | ROW_NUMBER, RANK, DENSE_RANK, PERCENT_RANK, CUME_DIST, NTILE(k) (`WindowRankValue`) |
| **Offset** | LAG, LEAD (incl. default-value arg), FIRST_VALUE, LAST_VALUE, NTH_VALUE (incl. `FROM LAST`) |
| **RATIO_TO_REPORT**, **DISTINCT-in-window** | needs `sum_distinct`/`avg_distinct` in `AggCatalog` |
| **ORDER BY … NULLS FIRST/LAST**; **ORDER BY / OVER an aggregate** | e.g. `SUM(COUNT(x)) OVER (… ORDER BY MAX(y))` |
| **Named WINDOW clause** | `REWRITE_PHASE1` runs before the semantics check (`glue/stmt.go`), matching cbq's server order |
| **RANGE over mixed-type ORDER BY** | non-numeric values fall to peer semantics instead of erroring on `ParseFloat64` |

### Performance model

The frame aggregate was originally brute-force — re-`Init` + re-fold `[Beg,End)` per row,
O(N·F), i.e. **O(N²)** for the dominant shapes. Four fast paths now cover the common
frames (in the `OpWindowFrames` agg block; a row no fast path can serve exactly sets
`lzResDone = false` and takes the general re-fold):

- **Left-anchored incremental fold** (whole-partition + running-total): UNBOUNDED
  PRECEDING, no EXCLUDE ⇒ `Beg` stays 0, `End` monotone, so carry the accumulator
  (`lzGrowAcc`, reset per partition) and fold only newly-entered rows. Every `base.Agg` is
  add-only over a growing frame ⇒ exact. **O(N)** (~29× running total, ~497× whole
  partition on 4000 rows).
- **Invertible sliding COUNT** (grep `-A/-B/-C`): fixed sliding frame, adjust a running
  count by rows entered(+1)/left(−1). **COUNT only** — integer-exact; a float SUM would
  drift vs cbq's re-fold. **O(N)** regardless of window size.
- **Invertible sliding SUM/AVG**: add-on-enter/subtract-on-leave (`SlideSum*`). Bit-exact
  **only while every operand + partial is an integer < 2⁵³** (float64 add/sub exact +
  associative); a non-integer latches `SlideSumExact()` false and that row + rest of
  partition re-folds (mirrors Postgres: inverse transitions for int sums, not float).
- **Sliding MIN/MAX monotonic deque** (`SlideMinMax*`): compares raw `Val`s via
  `ValComparer` (matching `AggMin/AggMax`, which do NOT skip NULL/MISSING). Exact except
  when a MISSING enters — `AggMin/AggMax` use the running value's byte length as their
  have-a-value flag, so a stored MISSING (length 0) overwrites unconditionally, an
  order-dependent quirk a deque can't reproduce ⇒ `SlideMinMaxExact()` false → re-fold.
  **O(N)** amortized (~208× MIN/MAX on a 401-wide window over 4000 rows).

Separately, **frame-edge discovery** was itself O(N²): a RANGE/GROUPS `CurrentUpdate`
called `FindGroupEdge`, walking outward each row — O(N·group). The edges are monotone as
Pos advances, so they now advance via **forward cursors** (`edgeBeg`/`edgeEnd`;
`currentPeerGroup` for EXCLUDE GROUP/TIES and GROUPS stepping) → **O(N)**, bit-equivalent.
(`FindGroupEdge` remains for `StepGroups`' n-group stepping, which isn't Pos-anchored.)
Ranking is O(1)/row; ROWS boundary math is O(1).

### What remains

Perf (priority): (1) sliding SUM/AVG over non-integer data still re-folds (Kahan wouldn't
obviously match the oracle's index-order fold — deferred until a real float-window
workload); (2) general arbitrary-frame O(N log N) segment trees (Leis et al., VLDB 2015)
— only if a workload hits large arbitrary/EXCLUDE frames (an **EXCLUDE frame still re-folds
per row, O(N²)**); (3) decode operand once per partition row (`StepVals`/`RowVals`
re-`Get`+`ValsDecode`+re-eval every visit; a typed/columnar temp would decode once —
overlaps `DESIGN-col.md`).

Correctness gaps (small, several deliberate non-matches of cbq quirks): cbq VAR_SAMP-of-1
= 0 vs standard NULL (n1k1 keeps NULL); AVG over rows with no NUMERIC value returns 0 not
NULL (a general AggCount gap); non-int64 RANGE extents int-truncated; ORDER BY an
aggregate over a `.*`-spread projection (`order-agg` group); frame-position over ties is
implementation-defined (n1k1 matches cbq's stored order on the corpus but it isn't
guaranteed — `window-nondeterministic` group).

**gsi corpus:** the fork's `test/gsi/test_cases/window` (31 cases, cbq's own results) is
imported (`test/suite_gsi_test.go`); ~19/31 reliably pass, the rest tracked in
`gsiExpectedNonPass` (`window-nondeterministic` + `window-results-differ`, the quirks
above). Window cases are **not** emitted to the compiler differential (the agg block is
`// !lz`), so the interpreter is the only lane; correctness rests on the gsi oracle + the
glue window tests.

### ⚠ Codegen landmines specific to op_window

`op_window.go` **is** scraped by `intermed_build` (the agg/ranking/offset blocks are
`// !lz`, stripped in the compiled lane, but the file must still translate cleanly):

- **Bind an indexed frame before a method/field.** `lzFrames[0].Method()` lifts to
  gen-time ("undefined"); write `lzFrame := &lzFrames[0]` then `lzFrame.X`.
- **No inner `// !lz` inside an `if X { // !lz … } // !lz` block** — the strip matches by
  brace depth as one unit; a nested `} // !lz` mis-closes it. Use plain-Go inner branches.
- **No string literal in an emitted comparison.** `aggName == "count"` mangles to a `%s`
  placeholder; gate on a baked bool (`isCount`) computed at param-parse.
- **`emitCaptured` operand calls must be lone statements.** `lzOperandFunc(...)` in a
  condition breaks the capture; assign to a var first.
- **Register new `base` aggregates LAST in `agg.go` `init()`.** `op_group`'s compiled path
  bakes `base.AggCatalog[name]` as a **literal index** (`// !lz`), so inserting an
  aggregate mid-`init()` shifts every later index and breaks the compiler differential.

## The fallback's cost (profiling, 2026-07)

Per row the `ExprTree` closure `Convert`s `base.Vals` → a cbq `value.Value`, `Evaluate`s,
and `WriteJSON`s. **`Convert`+`Evaluate` is the cost** (~73% of bytes on a profiled
16.8M-row cross-join: `objectValue.setField` 43.6% + `go_json.SimpleUnmarshal` 20.9%);
`WriteJSON` writes into a reused buffer and is not the allocator (so a *lazy* value can
serialize straight from retained label bytes — lever "lazy Convert" is viable). Already
optimized so they don't box: `SELECT 1` (`Constant`→`json`), `COUNT(*)` (star operand →
constant agg input).

The big source of `Convert`s was the whole-row **`self` projection** (`SELECT *`, and the
`FROM (subquery) AS x` derived-table row-wrap via `VisitAlias`) — it has no label path so
it rebuilt the full object per row. Landed levers (all differential-tested), each cutting
a class of boxing:
- **Discard-elision** (`glue/optimize.go`): under a value-agnostic group (`COUNT(*)`
  family) splice out the dead `project` chain below it. Toggle `glue.DiscardElision`.
- **`self`-projection byte path** (`engine/expr_self.go`, `base.ValsSelfObject`): assemble
  the row object's JSON from label bytes, both lanes. (Keys emit in label order, not
  cbq's sorted order — value-equal, and n1k1's result compare is key-order-insensitive;
  byte-identical wire output would need the `encoder-fidelity` fix.)
- **Scoped native exprs** (`strict` optimize): a scoped expr (correlated subquery / WITH /
  RCTE step) goes native when **every** field ref resolves locally (a `Field` matching no
  local label prefix = a parent reference = hard failure).
- **Logical `and`/`or`** wired into the optimizer (three-valued, right-nested binary) — the
  highest-frequency predicate-side ops.
- **Grouped aggregates read natively**: `ExprTreeOptimize` emits `labelPath` for the
  group's `^aggregates|…` column (so `count(*)+1` → `add(labelPath, json)`), no box.
- **Boxing-free output** (`ConvertVals.ConvertBytes`): render result rows from label bytes
  for the common shapes (all-`.["name"]`, lone `.` RAW, lone `.*`).

**Remaining:** lazy `Convert` (field-selective queries); native-lane ASOF/subquery
projection.

### Materialized CTE / derived-table re-scan

A different shape — a multiply-referenced CTE cross-joined with itself
(`WITH x AS (SELECT total FROM orders) SELECT … FROM x o1, x o2, …`) — where the cost is
**re-parsing an already-materialized boxed row per access**, not `Convert`. Already
handled: materialize-once (`glue/optimize_cte.go` — one `temp-capture` feeds the
`temp-yield`s, not re-executed per ref); temp-yield buffer reuse (`engine/op_temp.go`);
the `⟨boxed source⟩`/`⟨re-scanned per outer row⟩` explain markers. **Why the CTE body is
boxed:** a `FROM`-clause CTE is converted to an `expr-scan` op (`glue/conv.go`) evaluated
via cbq's boxed subquery evaluator (`ExprScanOp` → `EvaluateSubquery`), `json.Marshal`ed
back, then `OpTempCapture` stores each element as the JSON text of a whole opaque object
`Val` (e.g. `{"total":129.50}`), not a decomposed column. So per cross-join tuple,
`o.total` does `jsonparser.searchKeys` (~25%) + `ParseFloat` (~15%) over only ~20 distinct
values. Two not-yet-done levers: decompose boxed rows into native label columns at
capture (~25%, contained); typed/parsed temp (~15%, columnar — `DESIGN-col.md`). (The
interp hot path here was tightened separately: `EmitPush` per-actor memo + `OpGroup`
per-row agg-index hoist, ~25% fewer allocs / ~26% wall — engine ops, not exprs.)

## How porting works — cbq's two-layer structure

cbq's scalar exprs follow a rigid pattern, so porting is near line-for-line; copying it
faithfully minimizes edge-case misses.

**Layer 1 — a thin `Evaluate` skeleton** collapses into a few **propagation classes**,
each a reusable harness:

| Class | Rule | Members |
|---|---|---|
| delegate-to-value | the value primitive encodes the 3-valued result | eq…ge, between |
| MISSING-dominant → NULL | any MISSING → MISSING; else any non-typed → NULL; else compute | arithmetic, most scalar funcs |
| unknown-passthrough | MISSING→MISSING, NULL→NULL; else compute | not, `is_*`, most string/num/date |
| short-circuit / truth-table | special 3-valued handling | and, or, ifnull/coalesce, case |

**Layer 2 — semantics in ~6 `value.Value` methods** (`Equals`, `Compare`, `Collate`,
`Truth`, `Type`/`Actual`, `NumberValue`), mirrored in `base` (`ValComparer`, `ValTruthy`,
`Num`). Each new expr = pick a class harness + supply the leaf op on bytes.

**Recipe per expr:** (1) register in `ExprCatalog` + add the cbq `Function.Name()` to
`OptimizableFuncs`; (2) fold constants + resolve label indices + `varLift` buffers at
setup; (3) per-row read bytes, compute, append into the lifted buffer; (4) **match cbq's
three-valued logic + collation/type ordering exactly** (reuse `ValComparer`); (5) verify
`intermed_build` regenerates and `./intermed` builds; (6) differential-test byte-identical
to the fallback (incl. MISSING/NULL/mixed-type edges). The differential test caught the
`Function.Name()` and MISSING-constant bugs below.

## Lessons learned (footguns)

**Optimizer & recognition:**
- **`Function.Name()`, not the registry alias** — `OptimizableFuncs` keys must match the
  canonical name `Init()` sets (no-underscore `isnull`, underscore `is_array`).
- **A MISSING constant has no JSON form** — `WriteJSON` emits `"null"`, so
  `ExprTreeOptimize` must emit an *empty* json constant (→ MISSING) for a MISSING
  `Constant`, else a native op wrongly sees NULL.
- **Non-`Function` nodes need special handling** — `CASE` isn't an `expression.Function`
  (unexported fields); reach its parts via `Children()`.

**Navigation:**
- **`element` is a `Function`; `slice` is blocked.** `arr[start:end]` (`expression.Slice`)
  keeps presence-of-bound in *unexported* bools (no accessor), so `Operands()` can't tell
  `arr[X:]` from `arr[:X]`; and `jsonparser` has no slice primitive. Unblocking needs
  exported `HasStart()`/`HasEnd()` on the fork + a `base.ValSlice` helper.
- **Regex/pattern exprs don't fit zero-alloc** — `LIKE`/`REGEXP_*` compile to `regexp`;
  even a constant pattern's match is outside byte-reuse. Delegate until a hand-rolled byte
  glob matcher exists.

**Compiled (intermed) codegen — the load-bearing footguns:**
- **`emitCaptured` captures FROM the shared `lzVal` register, not into a fresh var.** A
  binary op needing both operands must write each child into `lzVal` and read it out on the
  *next* line (mirroring `ExprCmp`):
  ```go
  lzVal = lzA(lzVals, lzYieldErr) // <== emitCaptured: path "A"
  lzValA := lzVal
  lzVal = lzB(lzVals, lzYieldErr) // <== emitCaptured: path "B"
  lzValB := lzVal
  ```
  `emitCaptured` *replaces the whole marked line* with the child's emitted code, so a
  direct `lzValA := lzA(...)` bind is silently dropped and undefined in the compiled path
  (the interpreter runs the source line literally, so it *works there* — how this shipped
  broken in `and`/`or`/`nullif`). Also: an inline string literal in codegen'd code desyncs
  the tokenizer — use a named `base` const.
- **`MakeNaryExprFunc`'s compiled path is broken and not trivially fixable.** `op_filter`/
  `op_project` inline each per-row eval via `emitCaptured` (FIXED `"A"`/`"B"` capture); an
  n-ary op needs a runtime-sized `lzChildren` slice built at setup + a per-row reduce loop,
  and there's no way to split that variable-arity setup out of the single inlined eval.
  Two ways out (neither done): (a) fold the foldable ops (`ifnull`/`greatest`/`least`/
  `concat`) to right-nested binary (as `and`/`or` do), reusing the proven `MakeBiExprFunc`
  compiled path; (b) a capture-stack rework for `CASE`. Safety valve: de-wire them from
  `OptimizableFuncs` until (a)/(b) lands.
- **Func-value params are intermed-safe** via `base.LzExprFmt` (renders a func by its
  qualified Go name — only NAMED exported funcs in a `tmp`-imported package, not closures)
  + positional `\x00n\x00` arg tokens (so a func and a `varLift` placeholder on one line
  emit in order). Used for `is_*`→`base.TypeIs*`, `upper`→`base.StrCase*`, math→`math.*`,
  arith→`base.ArithAdd`. Put leaf logic in `base`.

## Codegen ergonomics — reducing lz boilerplate

`intermed_build` is a **line-oriented text translator** (`cmd/intermed_build/build.go`),
not a compiler: each lz line becomes an `Emit` printf of its text; a setup value is
rendered by `LzExprFmt`; operand eval is spliced by `// <== emitCaptured:`. **It only sees
text — it cannot look inside a runtime `func` value**, so a HOF taking `leaf func(...)Val`
works in the interpreter but the compiled path emits a *call to the closure*, not its body
(the leaf reaches compiled output only if it's a **named** func on one `LzExprFmt` line).
And `emitCaptured` has no loop form — why the compiled n-ary path stays broken.

Two moves cut most boilerplate without touching codegen: **propagation-class combinators**
in `base` (`MissingDominantBiNum`, `UnknownPassthroughUnNum`, `StrTransformInto`, …) that
take captured operand values + a named leaf, collapsing an op's leaf to one line; and
**name→leaf tables + one adapter per family** (`mathUnaryFuncs`, `arithOps`,
`strTransformFuncs`, `isPredicateFuncs`, `cmpFuncs` with a `swap` flag, …), all plain
(non-lz) Go the translator copies verbatim while the named leaf still rides `LzExprFmt`.
Table only a family of *several* single-line constructors — the fixed cost isn't worth a
1–2-entry table.

Source sugar `// !lzRHS` (a pre-pass, `expandLzRHS`) lets a build-time call
(`MakeBiExprFunc`) that must sit on its own `// !lz` line stay a single source line; it's a
plain string split on ` = `/` := ` (no AST), validated by diffing generated intermed with
and without the sugar (byte-identical).

## Sources / references
- Native impls: `engine/expr*.go`; byte toolkit in `base/` (`base.go`, `arith.go`,
  `compare.go`, `canonical.go`, `val_kind.go`, `val_in.go`).
- Fallback + optimizer: `glue/expr.go`, `glue/expr_optimize.go`.
- Differential + unit tests: `glue/expr_test.go`, `engine/expr_*_test.go`; benchmark
  `test/benchmark/bench_expr_arith_test.go`.
- Universe: `n1k1-query/expression/` (~357 scalar types, ~410 registry entries across ~95
  files; the structure-building `array_*`/`object_*`/collection families are the long tail).
