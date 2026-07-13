# Design: Native Expression Coverage

n1k1 evaluates a growing set of SQL++/N1QL expressions **natively** (byte-oriented,
allocation-avoiding, compiler-friendly) and **delegates the rest to the cbq-query
(`n1k1-query`) engine**, whose `Evaluate()` boxes into `value.Value` and garbages per
row. The native library grows incrementally; the cbq fallback stays **forever** as a
correctness backstop.

## Status & remaining TODOs

_Last reviewed: 2026-07-11._

**Done:** A large native byte-lane (zero steady-state garbage) covers arithmetic,
comparisons, three-valued logical AND/OR, predicates/type-checks, CASE and
conditional-unknown selectors, string/numeric/math funcs, constant-pattern REGEXP,
array/object readers and builders, the `self` / `SELECT *` projection, grouped-aggregate
reads, and boxing-free result output; window functions are a broadly-complete op
subsystem (ROWS/RANGE/GROUPS frames, ranking, LAG/LEAD/FIRST/LAST/NTH_VALUE,
RATIO_TO_REPORT, named WINDOW, NULLS ordering) with O(N) fast paths for the common frame
shapes. The cbq boxed fallback remains as the correctness oracle for everything else.

**Remaining (headline TODOs):**
- [ ] **Native-lane projection for ASOF / subquery results** — carry the value on the
  byte lane and skip the `Convert`→`value.Value` round-trip; boxed-value / JSON alloc
  churn still dominates some workloads, and this cuts the bulk of it.
- [ ] **Lazy/on-demand `Convert`** (profiling lever #3) — a `value.Value` that
  materializes fields only on access, serializing straight from label bytes; helps
  field-selective queries (`WHERE a.x > 5`).
- [ ] **Decompose boxed CTE / derived-table rows into native label columns at capture**,
  and a typed/parsed temp materialization (columnar territory, `DESIGN-col.md`).
- [ ] **Port more boxed funcs off the fallback:** the date-STRING family
  (`str_to_millis`/`millis_to_str`/`date_diff_*`/`date_trunc_*`), bare-identifier
  object/array operands (`OBJECT_LENGTH(o)`), variadic >2-arg array/object builders,
  array/object literals (`array_sort/reverse/flatten` done; `array_distinct` skipped —
  nondeterministic), comprehensions (ANY/EVERY/FIRST/ARRAY/OBJECT **done** single-binding;
  WITHIN/descend/multi-binding/name-variable remain),
  and `slice` navigation (blocked on a cbq-fork accessor).
- [ ] **`LIKE` / dynamic-pattern `REGEXP_*`** — need a hand-rolled zero-alloc byte glob
  matcher; they don't fit the byte-reuse model as `regexp` compiles.
- [ ] **Compiled-path n-ary ops broken** (`ifnull`/`greatest`/`least`/`concat`/`case`) —
  fold the foldable ones to right-nested binary; a capture-stack rework for CASE.
- [ ] **Window perf tail:** sliding SUM/AVG over non-integer data still re-folds;
  general arbitrary-frame O(N log N) segment trees; decode operand once per partition row.

## Contents

- [Status & remaining TODOs](#status--remaining-todos)
- [Status at a glance](#status-at-a-glance)
- [Why native matters (the fallback's cost)](#why-native-matters-the-fallbacks-cost)
- [Design principles & the byte-level toolkit](#design-principles--the-byte-level-toolkit)
- [How it works today](#how-it-works-today)
  - [Catalog, conversion, recognition](#catalog-conversion-recognition)
  - [Native inventory (the authoritative "done" list)](#native-inventory-the-authoritative-done-list)
  - [Known-broken & caveats](#known-broken--caveats)
- [Window functions](#window-functions)
  - [Architecture](#window-architecture)
  - [What works](#window-what-works)
  - [Performance model](#window-performance-model)
  - [What remains / what next](#window-what-remains--what-next)
  - [gsi window corpus status](#gsi-window-corpus-status)
  - [Codegen landmines specific to op_window](#codegen-landmines-specific-to-op_window)
- [Profiling the fallback (2026-07)](#profiling-the-fallback-2026-07)
  - [Cost attribution](#cost-attribution)
  - [Where the Converts come from: the `self` projection](#where-the-converts-come-from-the-self-projection)
  - [Levers tried that did NOT help](#levers-tried-that-did-not-help)
  - [Levers that help (ranked)](#levers-that-help-ranked)
  - [Materialized CTE / derived-table re-scan: the read-side re-parse](#materialized-cte--derived-table-re-scan-the-read-side-re-parse-2026-07)
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

**Done recently — perf levers:** (a) native exprs run under an active scope (correlated
subqueries / WITH / recursive CTEs) when every field ref is provably local — `strict`
optimize (lever #4); (b) logical `and`/`or` optimizer-wired with three-valued semantics
(lever #5), so `WHERE`/`JOIN`/`ON` conjunctions avoid boxing; (c) the whole-row `self` /
`SELECT *` projection now assembles JSON from label bytes instead of boxing (lever #2);
(d) grouped aggregates (`count(*)`, `sum(x)`, incl. `count(*)+1`) read the group's
`^aggregates|…` value natively (lever #6); (e) result-row output (`OnRow` / `Result.Rows`)
encodes boxing-free via `ConvertBytes` (lever #7).

**Done recently — new families** (PREPARE++ log-extraction workloads lean on string /
object / array funcs): constant-pattern `regexp_contains`/`regexp_like`; `date_add_millis`;
the object structure builders `object_names`/`object_values`/`object_pairs`; the object
mutators `object_add`/`object_put`/`object_remove`/`object_concat`; the array builders
`array_append`/`array_prepend`/`array_concat`; and the array reshapers
`array_sort`/`array_reverse`/`array_flatten`. All are in the inventory table.

**Next:** `slice` navigation (blocked); the remaining Tier C builders (comprehensions,
more `array_*`/`object_*`); Tier B (more string/numeric; the date-STRING funcs; bitwise;
JSON `encoded_size`). `LIKE` and dynamic-pattern `REGEXP_*` deferred.

**Window functions** (`… OVER (…)`) are a separate op-based subsystem — now broadly
complete (aggregates, ranking, offset, RATIO_TO_REPORT, DISTINCT-in-window, named
WINDOW clause, NULLS ordering) and O(N) for the common frame shapes. See the dedicated
[Window functions](#window-functions) section for status and what's next.

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
| `is_distinct_from` `is_not_distinct_from` | `engine/expr_distinct.go` + `base/distinct.go` | IS [NOT] DISTINCT FROM (binary null-safe (in)equality; MISSING/NULL-aware, then `ValComparer.Compare`) |
| `upper` `lower` `title` `trim` `ltrim` `rtrim` `reverse` `length` `contains` `position0` `position1` | `engine/expr_str.go` + `base/str.go` | unary string transforms (shared `exprStrTransform`: decode → `func([]byte)[]byte` → re-encode). TRIM/LTRIM/RTRIM 2-arg (cutset) fall back |
| `replace` | `engine/expr_str.go` + `base.StrReplaceAll` | REPLACE 3-arg (ternary → buffer); 4-arg count form falls back |
| `substr` (`substr0` `substr1`) | `engine/expr_str.go` + `base.StrSubstr` | SUBSTR (byte-based, `inRunes=false`), 2/3-arg, arity-dispatched to `substr{0,1}_{2,3}`. Rune-based `mb_substr*` fall back |
| `split` | `engine/expr_str.go` + `base.StrSplit*` | SPLIT (1-arg whitespace / 2-arg sep), arity-dispatched. **First structure-building native expr** — emits a JSON array via `EncodeAsString` (appends, unlike `EncodeStr`) |
| `lpad` `rpad` | `engine/expr_str.go` + `base.StrPad*` | LPAD/RPAD (byte-based), 2/3-arg, arity-dispatched. `l <= len(s)` truncates; else pad-fill. Rune-based `mb_*pad` fall back |
| `regexp_contains` `regexp_like` | `engine/expr_str.go` + `base.StrRegexpMatch` | regexp predicates over a **constant** pattern (compiled once at setup); a dynamic or invalid-constant pattern falls back |
| `abs` `ceil` `floor` `sqrt` `exp` `ln` `log` `sign` `degrees` `radians` `sin` `cos` `tan` `asin` `acos` `atan` `power` `atan2` | `engine/expr_math.go` + `base/math.go` | numeric math (func-passing: stdlib `math.*` / `base.Math*`) |
| `round` `trunc` | `engine/expr_math.go` + `base.RoundFloat`/`TruncFloat` | ROUND (half-to-even) / TRUNC, 1/2-arg, arity-dispatched. `round_nearest` falls back |
| `date_part_millis` `date_add_millis` | `engine/expr_date.go` + `base/datetime.go` | DATE_PART_MILLIS (2-arg component) / DATE_ADD_MILLIS (3-arg) — millis math in the process-local zone (ports of cbq `millisToTime`/`datePart`/`dateAdd`). 3-arg named-TZ and the date-STRING funcs fall back |
| `to_boolean` `to_string` `to_number` | `engine/expr_type.go` + `base/type.go` | scalar type conversions |
| `array_length` `array_count` `array_sum` `array_avg` `array_min` `array_max` `array_contains` `array_position` | `engine/expr_array.go` + `base/array.go` | reader array ops (no materialization) |
| `array_append` `array_prepend` `array_concat` | `engine/expr_array.go` + `base/array.go` | array builders (2-arg): splice element bytes into a lifted buffer — the value operand is a complete Val, spliced verbatim. Variadic >2-arg falls back |
| `array_sort` `array_reverse` `array_flatten` | `engine/expr_array.go` + `base/array.go` | array reshaping builders. SORT/REVERSE (unary) reuse the pooled `KeyVals` to collect elements (no per-elem copy), then reshape into a lifted buffer — SORT is an allocation-free insertion sort by N1QL collation (`CompareWithType` at pool depth 1); byte-identical to cbq for canonical inputs. FLATTEN (2-arg) recursively splices nested arrays; depth 0 = shallow copy, negative depth = flatten fully, non-integer depth → NULL |
| `object_length` `poly_length` | `engine/expr_object.go` + `base/object.go` | object/collection reader ops (unary; op-code dispatch; count via `jsonparser.ObjectEach`/`ArrayEach`, no materialization) |
| `object_names` `object_values` `object_pairs` | `engine/expr_object.go` + `base/object.go` | name-sorted structure builders (field names / values / `{name,val}` pairs; pooled `KeyVals` + reused buffer). OBJECT_PAIRS 1-arg only (2-arg `types` option falls back) |
| `object_add` `object_put` `object_remove` `object_concat` | `engine/expr_object.go` + `base/object.go` | object mutating builders — key-sorted re-emit (add-new / set (a MISSING value removes) / remove / merge). ADD/PUT ternary; REMOVE/CONCAT 2-arg (variadic >2 falls back) |
| `exprStr` / `exprTree` | `glue/expr.go` | **the fallback** (parse / delegate to cbq) |

Still **delegated:** `LIKE`, dynamic-pattern `REGEXP_*`, `slice` navigation,
`TYPE()`/`IS_BINARY`, and the bulk of the remaining scalar functions (see the roadmap
tiers).

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

## Window functions

SQL++ window functions (`… OVER (PARTITION BY … ORDER BY … <frame>)`) are a distinct
subsystem from the scalar exprs above: they are **ops** (`engine/op_window.go`), not
`ExprCatalog` entries, and their values reach the projection through the **same
`^aggregates|<agg.String()>` native label path** that GROUP BY aggregates use (lever
#6). They were fully non-functional before this arc (the boxed cbq window `Evaluate`
panics on n1k1's plain, non-`AnnotatedValue` rows); the subsystem is now broadly
complete. (`engine/expr_window.go` — the old `window-partition-row-number` /
`window-frame-*` `ExprCatalog` stubs — is **superseded and effectively dead**; the op
computes everything.)

<a name="window-architecture"></a>
### Architecture

Two chained ops per window function (`glue/conv.go:VisitWindowAggregate` builds one
chain per function; chaining lets each `^aggregates|…` column accumulate):

- **`OpWindowPartition`** (`window-partition`) — requires its child to deliver rows
  sorted by PARTITION BY + ORDER BY (cbq's planner inserts the sort). Buffers **one
  partition at a time** into a `store.Heap`; on a partition-boundary change (or
  end-of-stream, via a `yieldErr(nil)` drain) it drains + resets. Memory is O(largest
  partition) — necessary, since FOLLOWING / UNBOUNDED FOLLOWING frames need the whole
  partition buffered for look-ahead. Optionally appends a trailing **`^worderby`**
  column (Params[4] mode: `"value"` = the single numeric ORDER BY value for RANGE
  arithmetic; `"tuple"` = the canonical ORDER BY tuple for GROUPS / peer / rank
  bytes.Equal detection, any number of columns).
- **`OpWindowFrames`** (`window-frames`) — per row: `CurrentUpdate` sets the frame
  `[Include.Beg, Include.End)` (via `base.WindowFrame`), then computes the window value
  and appends it under `^aggregates|<agg.String()>`. Four value kinds, dispatched by
  the `winFunc`/`aggName` params: **aggregate** (fold a `base.Agg` over the frame),
  **ranking** (`base.WindowFrame.WindowRankValue` from position + peer + partition
  count), **offset** (`base.WindowFrame.StepToOffset` walks to the target row),
  **ratio** (`RATIO_TO_REPORT` = current operand / frame SUM).

Frame boundary math lives in `base/agg_window.go` (`base.WindowFrame`): `CurrentUpdate`,
`StepGroups`, `FindGroupEdge`, `StepVals`, `RowVals`. A RANGE frame with only
CURRENT ROW / UNBOUNDED bounds (no numeric offset) is rewritten by conv to **GROUPS**
(peer semantics), so multi-column ORDER BY works; a numeric-offset RANGE stays single
numeric column.

<a name="window-what-works"></a>
### What works

Validated by the ORDER-sensitive oracle `glue/window_test.go` + `glue/order_nulls_test.go`
(the default suite had zero `OVER (` cases, which is how the whole subsystem stayed
broken unnoticed), and by the data-backed gsi window corpus (below).

| Capability | Notes |
|---|---|
| Frame **aggregates** | SUM/COUNT/AVG/MIN/MAX + any `base.AggCatalog` agg, over ROWS / RANGE / GROUPS frames, incl. composite-key multi-column ORDER BY, empty/inverted frames (→ the agg's empty result), PARTITION BY, and multiple window funcs per query |
| **Ranking** | ROW_NUMBER, RANK, DENSE_RANK, PERCENT_RANK, CUME_DIST, NTILE(k) — unified in `base.WindowFrame.WindowRankValue` |
| **Offset / navigation** | LAG, LEAD (incl. the 3rd default-value arg, evaluated at the current row), FIRST_VALUE, LAST_VALUE, NTH_VALUE (incl. `FROM LAST`) |
| **RATIO_TO_REPORT** | current operand / frame SUM (`base.WindowRatioValue`) |
| **DISTINCT-in-window** | SUM/AVG/COUNT/COUNTN(DISTINCT), MEAN(DISTINCT) — needs `sum_distinct`/`avg_distinct` in `base.AggCatalog` |
| **ORDER BY … NULLS FIRST/LAST** | per-term nulls-position in `order-offset-limit`; natural terms keep the collation path (missing < null) |
| **ORDER BY / PARTITION BY / OVER an aggregate** | e.g. `ORDER BY COUNT(x)`, `SUM(COUNT(x)) OVER (… ORDER BY MAX(y))` — resolved via the group's `^aggregates|` columns passed through the ORDER-BY augmentation |
| **Named WINDOW clause** | `… OVER w …` and `OVER (w <frame>)` (adds a frame to a named window) — `REWRITE_PHASE1` runs **before** the semantics check (`glue/stmt.go`), matching cbq's server order |
| **RANGE over a mixed-type ORDER BY** | non-numeric (null/boolean) ORDER BY values fall back to peer semantics in `FindGroupEdge` instead of erroring on `ParseFloat64` |

<a name="window-performance-model"></a>
### Performance model

The frame aggregate was originally brute-force — re-`Init` + re-fold the whole frame
`[Beg,End)` per row, O(N·F), i.e. **O(N²)** for the dominant shapes. Four fast paths
now cover the common frames (all in the `OpWindowFrames` agg block; a row that no fast
path can serve exactly sets `lzResDone = false` and falls to the general per-row
re-fold):

- **Left-anchored incremental fold (whole-partition + running-total).** When the frame
  begins at UNBOUNDED PRECEDING with no EXCLUDE, `Include.Beg` stays 0 and `Include.End`
  is monotone, so the frame only grows: carry the accumulator (`lzGrowAcc`, reset per
  partition) and fold only the newly-entered rows. Every `base.Agg` is add-only over a
  growing frame, so exact. Whole-partition is the degenerate case (fold all at row 0,
  reuse). **O(N).** Measured (4000-row partition): running total 630→22 ms (~29×),
  whole partition 1169→2.4 ms (~497×).
- **Invertible sliding COUNT (grep `-A/-B/-C`).** A fixed sliding frame
  (`ROWS BETWEEN N PRECEDING AND M FOLLOWING`) slides forward (Beg, End both monotone),
  so adjust a running count by rows that entered (+1) / left (−1). **COUNT only** — it
  is integer-exact; a float SUM would drift under repeated `+=`/`−=` and diverge from
  cbq's exact re-fold. Serves the "is this row near a match" idiom
  `COUNT(CASE WHEN <pred> THEN 1 END) OVER (… ROWS BETWEEN B PRECEDING AND A FOLLOWING) > 0`.
  **O(N)** regardless of window size (k=200/4000 rows: 90→16 ms).
- **Invertible sliding SUM/AVG.** Same forward-slide shape: add-on-enter / subtract-on-
  leave a running float64 sum + numeric-count + has-value-count (`base.WindowFrame.SlideSum*`).
  Bit-exact vs a fresh fold **only while every operand and partial sum is an integer
  < 2^53** — where float64 add/subtract is exact and associative (the common integer/
  count shape). A non-integer or out-of-range numeric latches `SlideSumExact()` false
  and that row (and the rest of the partition) re-folds — mirroring Postgres offering
  inverse transitions for integer sums but not float. AVG's denominator is the has-value
  count (incl. non-numerics), matching `AggAvg`. **O(N)** on the integer path.
- **Sliding MIN/MAX monotonic deque.** A deque of candidate positions (front = current
  MIN/MAX): pop dominated tail entries on enter, expire the front past `Include.Beg`
  (`base.WindowFrame.SlideMinMax*`). Compares raw `Val`s via `ValComparer`, matching
  `AggMin/AggMax` (which do **not** skip NULL/MISSING). Exact **except** when a MISSING
  operand enters — `AggMin/AggMax` store the running value's byte length as their
  have-a-value count, so a stored MISSING (length 0) makes the next value overwrite
  unconditionally: an order-dependent, non-associative quirk a deque can't reproduce, so
  `SlideMinMaxExact()` latches false and the op re-folds. NULL (length 4) is a normal
  comparable, unaffected. **O(N)** amortized, allocation-free steady-state (recycled
  value buffers). Measured (4000 rows, 401-wide window): MIN/MAX 133→0.64 ms (~208×),
  SUM 77.7→0.48 ms (~163×) — both O(N), so the gap widens with window width.

Separately from the aggregate fold, **frame-edge discovery** was itself O(N²): for a
RANGE/GROUPS frame `CurrentUpdate` called `FindGroupEdge`, which walks outward from Pos
to the group/range boundary each row — O(N·group) over a big peer group. The edges are
monotone non-decreasing as Pos advances (values are sorted; thresholds only grow), so
they now advance via **forward cursors** (`base.WindowFrame.edgeBeg/edgeEnd` for a RANGE
frame's value/peer edges; `currentPeerGroup`, a monotone peer-group cache, for
EXCLUDE GROUP/TIES and the GROUPS-stepping anchor), each visiting a row at most once per
partition → **O(N)**, bit-equivalent to the `FindGroupEdge` result. Measured (4000 rows,
one big peer group): RANGE running total 122→4.5 ms (~27×), GROUPS CURRENT ROW 235→4.6 ms
(~51×), both now linear in N. (`FindGroupEdge` remains for `StepGroups`' n-group stepping,
which isn't Pos-anchored.)

Ranking is already O(1)/row (peer state maintained as rows stream); ROWS boundary math
is O(1).

<a name="window-what-remains--what-next"></a>
### What remains / what next

Perf (in rough priority):
1. **Sliding SUM/AVG over non-integer data** — the incremental path is exact only for
   integers (see the performance model); float operands still re-fold. Kahan/Neumaier
   compensation would narrow the drift but not obviously to a bit-exact match of the
   oracle's index-order fold — deferred until a real float-window workload appears.
2. **General arbitrary-frame** O(N log N) — segment/aggregate trees (Leis et al., VLDB
   2015, *Efficient Processing of Window Functions in Analytical SQL Queries*). Only
   worth it if a workload hits large arbitrary frames that no fast path covers (e.g. an
   EXCLUDE frame, or a non-invertible aggregate over a genuinely sliding frame). Note an
   EXCLUDE frame still re-folds the whole (minus-excluded) frame per row — O(N²) over a
   big partition — since none of the incremental paths apply to it.
3. **Decode operand once per partition row** — `StepVals`/`RowVals` re-`Partition.Get` +
   `ValsDecode` + re-eval the operand on every frame-row visit; a typed/columnar temp
   would decode once (overlaps `DESIGN-col.md`).

(Done: the sliding MIN/MAX deque and the invertible sliding SUM/AVG; and the
monotone frame-edge cursors — see the performance model above.)

Correctness gaps (small, several are deliberate non-matches of cbq quirks):
- **cbq VAR_SAMP-of-a-single-value = 0** (window path) vs standard/GROUP-BY NULL — n1k1
  keeps the standard NULL; matching it would need a window-specific variance that
  contradicts the GROUP BY path. (gsi window `results-differ`, with the RANGE-over-
  non-numeric membership difference.)
- **AVG over rows with no NUMERIC value** returns 0, not NULL (AggCount counts
  non-numerics) — a general aggregate gap, not window-specific.
- **Non-int64 RANGE numeric extents** are int-truncated (`conv.appendExtent` TODO);
  multi-column *numeric* RANGE has no scalar to bound on (graceful NA).
- **ORDER BY an aggregate over a `.*`-spread projection** (default-suite
  `aggregate[1,2]`, the `order-agg` group) — a star projection's `.` label can't take a
  sidecar `^aggregates|` column + strip; would need ordering below the star projection.
- **Frame-position over ties is implementation-defined.** FIRST_VALUE/NTH_VALUE/ROW_NUMBER
  over an ORDER BY that ties pick a specific row by scan order; n1k1 matches cbq's stored
  order on the corpus but it isn't guaranteed (see the `window-nondeterministic` group).

<a name="gsi-window-corpus-status"></a>
### gsi window corpus status

The fork's `test/gsi/test_cases/window` (31 cases, recording cbq's own results) is
imported into the isolated gsi suite (`test/suite_gsi_test.go`; see the window block +
group whys there). ~19 of 31 reliably pass; the rest are tracked in `gsiExpectedNonPass`:
- **`window-nondeterministic`** — produce cbq-matching results but have a frame-position
  function over tie-able keys (a pass shows as a stale-entry note, never a flaky fail).
- **`window-results-differ`** — the cbq VAR_SAMP / RANGE-over-non-numeric quirks above.
- The `window-unsupported` and `window-named-frame` groups are now **empty**.

`gsiPassFloor` was raised step-by-step as cases flipped (see git history). Window cases
are **not** emitted to the compiler differential (the agg block is `// !lz`), so the
interpreter is the only lane that runs them; correctness rests on the gsi oracle + the
`glue` window tests.

<a name="codegen-landmines-specific-to-op_window"></a>
### Codegen landmines specific to op_window

`op_window.go` **is** scraped by `intermed_build` (the agg/ranking/offset blocks are
`// !lz`, so stripped in the compiled lane — but the file must still translate cleanly),
so the general lz rules apply, plus:
- **Bind an indexed frame before calling a method/field.** `lzFrames[0].Method()` /
  `.Field` is lifted to gen-time (`LzExprFmt`, "undefined"); write `lzFrame := &lzFrames[0]`
  then `lzFrame.X`.
- **No inner `// !lz` inside an `if X { // !lz … } // !lz` block** — the strip matches by
  brace depth as one unit; a nested `} // !lz` mis-closes it. Use plain-Go inner branches
  (the incremental-fold and sliding-COUNT branches are plain `if`/`for`).
- **No string literal in an emitted comparison.** `aggName == "count"` mangles to a
  `%s` placeholder (`undefined: count`); gate on a baked bool (`isCount`) computed at
  param-parse. (Same class as `base.Val("null")` → named const.)
- **`emitCaptured` operand calls must be lone statements.** `lzOperandFunc(...)` embedded
  in a condition breaks the capture; assign to a var first (`lzV := lzOperandFunc(...); if base.ValHasValue(lzV)`).
- **Register new `base` aggregates LAST in `agg.go` init().** `op_group`'s compiled path
  bakes `base.AggCatalog[name]` as a **literal index** (`// !lz`), so inserting an
  aggregate mid-`init()` shifts every later index and breaks the compiler differential
  ("compiled rows mismatch" on unrelated aggregate cases).

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

**1. Discard-elision (dead-value elimination) — DONE (v1, `glue/optimize.go`).**
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

### Materialized CTE / derived-table re-scan: the read-side re-parse (2026-07)

A different profiled shape — a multiply-referenced CTE cross-joined with itself:
`WITH x AS (SELECT total FROM orders) SELECT count(1), sum(o1.total), sum(o2.total)
FROM x o1, x o2, x o3, x o4, x o5` over 20-doc `orders` → 20⁵ = 3.2M cross-join rows.
This is the flip side of the levers above: the cost is **not** `Convert` on projection
or output, but **re-parsing an already-materialized boxed row on every access**.

**What is already handled (don't re-do):**
- **Materialize-once** (`319f599`, `glue/optimize_cte.go`): a non-recursive,
  non-correlated CTE referenced N times is captured *once* into a temp heap, not
  re-executed per reference. The plan is a single `temp-capture` feeding the
  `temp-yield`s (one per `FROM x` ref); the 5 refs re-scan the one materialized temp.
- **Temp-yield buffer reuse** (`7612f56`, `engine/op_temp.go`): `OpTempYield` reuses
  one `base.Vals` backing array across decoded rows.
- The `⟨boxed source⟩` / `⟨re-scanned per outer row⟩` explain markers (`f9d2be4`)
  flag both properties in the converted plan.

**Why the CTE body is boxed:** a `WITH` CTE used as a `FROM` source is *not* compiled
to native ops — `glue/conv.go` converts it to an **`expr-scan`** op, and `ExprScanOp`
(`glue/datastore.go`) evaluates it via `expr.Evaluate(item, ctx)`, i.e. through cbq's
boxed subquery evaluator (`GlueContext.EvaluateSubquery`). The result is a cbq
`value.Value` array, `json.Marshal`ed back to bytes; `OpTempCapture` (`op_temp.go`)
`ValsEncode`s each element and appends it. So each stored temp entry is the **JSON text
of a whole one-field object** — e.g. `{"total":129.50}` (verified: the raw source text
`129.50`, not a reparsed `129.5`, survives — it is stored as JSON bytes, as one opaque
`Val`, not decomposed into a native `total` column).

**The residual cost (read side):** on `OpTempYield`, `ValsDecode` hands back that object
`Val`; downstream `o1.total`/`o2.total` then, **per cross-join tuple** (millions of
times, over only 20 distinct values): `jsonparser.searchKeys("total")` to navigate into
the object (~25% CPU) + `strconv.ParseFloat` on its value (~15%). Two independent,
not-yet-done levers:

1. **Decompose boxed CTE/derived-table rows into native label columns at capture
   (~25%, contained).** `expr-scan` yields each element as a single opaque object `Val`;
   splitting it into the alias's `.["name"]` columns (like a native scan row) makes a
   later `o.total` a column *index* rather than a `searchKeys` navigation. Attacks the
   `⟨boxed source⟩` shape directly; the parse cost (below) remains.
2. **Typed / parsed temp materialization (~15%, columnar territory).** Even a native
   `total` column stores JSON-text bytes (`129.50`), so `SUM` re-`ParseFloat`s per
   access. Killing this means storing the parsed value once (20 values, reused
   millions of times) — i.e. a typed/columnar temp. See DESIGN-col.md (columnar temp /
   `@col` vectorized agg); not an expression-layer change.

The interp-lane hot path for this shape was also tightened in the same pass, but those
two wins are engine ops, not expressions: `EmitPush` op-path memoized per-actor
(`3266236`) and `OpGroup`'s per-row aggregate resolution hoisted to a pre-resolved
index slice (`50754bd`) — together ~25% fewer allocs, ~26% wall. Recorded here only as
the provenance of this profiling pass.

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
  (The rest of this tier — arithmetic, comparisons, `between`, `in`, `is-*`,
  `is [not] distinct from` — is done; see the inventory.)

### Tier B — scalar but needs parse+format into a reused buffer
- **String funcs** — upper/lower/title/trim/reverse/length/contains/position, plus
  replace/substr/split/lpad/rpad and constant-pattern `regexp_contains`/`regexp_like`,
  **done** (Go `strings.*`/`regexp` into a lifted buffer). Remaining: multi-byte (`mb_*`)
  variants, the cutset/4-arg forms, `repeat`, `mask`, tokenizers.
- **Numeric/math** (abs/ceil/floor/round/sqrt/pow/…) — **done** (`math.*` +
  `strconv.AppendFloat`).
- **Date/time (non-volatile)** — the millis-only funcs `date_part_millis` /
  `date_add_millis` are **done** (no string parsing). The rest hinge on cbq's date-STRING
  parser (`strToTime`'s large `_DATE_FORMATS` list) and named-TZ loading (`loadLocation`)
  — a fiddly port where any mismatch breaks the differential, so delegated:
  `str_to_millis`/`millis_to_str`/`date_part_str`/`date_add_str`/`date_diff_*`/
  `date_trunc_*`/`weekday_*`. `now_*`/`clock_*` are volatile (Tier D). Next tractable
  millis-only: `date_diff_millis`, `date_trunc_millis`.
- **Bitwise, JSON `encoded_size`** — scalar; `to_boolean`/`to_string`/`to_number` done.
- **`LIKE` / dynamic-pattern `REGEXP_*` do NOT fit here** — compile to `regexp` per
  tuple, outside byte-reuse (see Lessons). A *constant* `regexp_*` pattern is native
  (compiled once); the rest wait on a bespoke zero-alloc glob matcher.

### Tier C — structure-building (doable in bytes, higher cost)
- **Reader ops (no output build)** — `array_length/contains/position`,
  `array_min/max/sum/avg`, `object_length`, `poly_length` **done** (iterate via
  `jsonparser.ArrayEach`/`ObjectEach`, compute a scalar without materializing — good
  ROI). Remaining: the bare-identifier / whole-row operand case (e.g. `OBJECT_LENGTH(o)`
  still boxes because `o` isn't recognized as a native labelPath).
- **Ops that DO build output** — `array_append/prepend/concat`, `array_sort/reverse/flatten`,
  the object structure builders `object_names/values/pairs`, and the object mutators
  `object_add/put/remove/concat` are **done** (emit JSON into a lifted buffer; mutators
  and array sort/reverse re-emit via a pooled `KeyVals`; the splicing builders copy
  element bytes verbatim). Remaining: `array_distinct` (**skip** — cbq's set order is
  nondeterministic), array/object literals, and the variadic >2-arg forms of the
  builders above (currently 2-arg only). `ValComparer`
  scratch covers sort/dedup. Skip `array_distinct` — cbq's set-based order is
  nondeterministic (no stable differential).
- **Comprehensions** — `ANY`/`EVERY` (predicate), `FIRST`/`ARRAY` (mapping + optional
  `WHEN`), and `OBJECT` (name:value mapping + optional `WHEN`), single-binding plain-IN
  form, are **done** (`engine/expr_coll.go`): the bound variable is an APPENDED register
  slot labeled like a LET var (`.["<var>"]`, resolved by the normal Field/Identifier
  matcher), fed per element via `base.ArrayYield` + a shadowed `lzVals` right before the
  captured child (the same codegen-safe shape as UNNEST) — so they work in BOTH lanes
  (verified byte-identical interpreter vs `-prepare=full` for flat, field-nav,
  correlated, and multi-level NESTED forms). `ANY`/`EVERY`/`FIRST` early-exit; `WHEN` is
  synthesized to a constant `true` when absent (no per-element branch); `OBJECT`
  accumulates last-wins key-sorted (string key required: MISSING-name → MISSING,
  non-string → NULL). `WITHIN` (descend) is supported for every form by wrapping the
  operand in a `descendants` transform (pre-order DFS, object fields sorted, containers
  included; cbq `value.Descendants` order) and reusing the element-iterating op.
  Remaining: multi-binding (`FOR x IN a, y IN b`) and name-variable object iteration
  (`FOR k:v IN obj`), both fall back.

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
  (Partial: the object builders share `objectPairsInto`/`kvsSortByName`/`objectEmit`/
  `appendJSONElem`, and array builders share `arrayElems`, but there is no single
  cross-family emitter yet.)
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
