# Extending n1k1's function library — builtins, drop-in UDFs, and table-valued/streaming functions

## Why

n1k1 runs SQL++ by reusing cbq's parser + planner and evaluating expressions
(natively where ported, otherwise via the embedded cbq evaluator). A natural next
question: can we grow the *function* surface — add builtins, let users drop in
their own functions, and support functions that return whole tables (e.g. shred a
PDF/PPT/DOC/XLSX into many JSON rows) — ideally **streaming** so a huge result
doesn't have to be materialized in memory?

Short answer: yes to all, in tiers with different effort/trade-offs. The
enabling plumbing (the UDF resolution seam, `FROM <expr>` scans, a push-based
streaming engine, spill-to-disk) already exists; the work is wiring and one small
new "streaming source function" protocol.

## How cbq resolves a function name (the seam)

At parse time `NAME(args)` resolves in this order
(`parser/n1ql/n1ql.y:5740`):

1. `expression.GetFunction(name)` — the static **builtin** registry
   (`expression/func_registry.go`, an unexported `_FUNCTIONS` map).
2. `search.GetSearchFunction(name)` — FTS.
3. `algebra.GetAggregate(name, …)` — aggregates.
4. `expression.GetUserDefinedFunction(name, …)` → `functions.PreLoad(name)` — the
   **UDF** subsystem (pluggable storage + language runtimes).
5. else → `FatalError("Invalid function …")`.

So there are two extension points: the builtin registry (step 1) and the UDF
resolver (step 4). Since **n1k1 owns the fork**, both are open to us.

**Current n1k1 state:** the UDF bridge is *not* wired — `glue/conv.go`'s
`VisitExecuteFunction` returns `NA()` and no functions storage/language is
initialized, so unknown/UDF names error at parse today. Wiring that bridge is the
one-time prerequisite for the drop-in tiers below.

## Tier 1 — Native Go builtins (best for heavy/binary work like shredding)

Builtins are `expression.Function` implementations in the static map. There's no
public `RegisterFunction`, so today adding one is a fork edit. Cleanest change:
add `expression.RegisterFunction(name, fn)` to the fork, then **register from
n1k1's own `glue` package at init** — keeping the implementations in n1k1 (the
cbq-aware bridge layer) so `base`/`engine` stay cbq-free, consistent with the
project's layering.

- Good for: functions needing Go libraries or real I/O (file loaders, parsers).
- Runs in the interpreted/boxed lane (via cbq `Evaluate`, or a native
  `ExprCatalog` handler) — not the zero-alloc byte fast-path or the compiler
  codegen path. Fine for I/O-bound enrichment; not for tight numeric loops.

## Tier 2 — "A bunch of JS in a directory / git repo" (drop-in UDFs)

No formal `CREATE FUNCTION` DDL required. n1k1 supplies its *own* implementation
of the UDF resolver instead of cbq's metakv/Enterprise machinery:

- **Registry = the filesystem.** Scan a directory or cloned git repo
  (e.g. `.n1k1/functions/*.js`); each exported function → a resolvable UDF name.
  `git pull` to update. The directory *is* the catalog.
- **Runtime = embedded pure-Go JS.** Use **goja** (MIT) — no V8, no cgo, no
  Enterprise dependency. The bridge's `Execute()` marshals args → runs the JS →
  returns JSON. (cbq's own golang/JS UDF paths are Enterprise-only:
  `functions/golang/golang.go:78` uses `plugin.Open` on `.so` files —
  toolchain-locked and Linux-mostly — and the Community build is a stub returning
  "not supported". So n1k1's goja approach is *lighter* than what cbq ships.)
- **Optionally streaming.** A JS UDF used in FROM can stream its rows via an
  `emit(row)` callback instead of returning one big array — see "Streaming
  JS/goja functions" below.

## Tier 3 — Inline N1QL UDFs (`CREATE FUNCTION … { expr }`)

Pure SQL++, trivial to wire (it's just an expression bound to a name), but limited
to expressions — can't touch a PDF. A nice-to-have that composes with Tiers 1–2.

## Table-valued (set-returning) functions in FROM

"Table-valued function" is the right term (a.k.a. *set-returning function*). The
N1QL-native idiom: a function that returns a **JSON array** used in the FROM
clause — `SELECT x.* FROM my_func(…) AS x` — where each array element becomes a
row. (The sibling construct is `UNNEST` over an array field.)

**This already works in n1k1.** `FROM <expr>` is `plan.ExpressionScan`, handled by
`glue/conv.go:VisitExpressionScan` → the `expr-scan` op
(`glue/datastore.go:ExprScanOp`). If the expression yields an array, `ArrayYield`
(`base/base.go:324`) streams each element as a row into the pipeline; a non-array
value becomes a single row.

### How the plan gets there (cbq planner → n1k1)

The node is produced by **cbq's own planner**, not by n1k1 — n1k1 only *converts*
what the planner emits. `planner/build_select_from.go` creates a
`plan.ExpressionScan` in two cases:

- `FROM <expr> AS x` — an `algebra.ExpressionTerm` (covers `FROM my_func(...)`,
  `FROM [array]`, `FROM cte`): `plan.NewExpressionScan(node.ExpressionTerm(), …)`
  (~line 765).
- `FROM (SELECT …) AS x` — a subquery term:
  `plan.NewExpressionScan(algebra.NewSubquery(subquery), …)` (~line 677), and it
  **also builds the subquery's full sub-plan** and attaches it via
  `exprScan.SetSubqueryPlan(selOp)`.

So the chain is: SQL FROM term → cbq planner → `plan.ExpressionScan` →
`VisitExpressionScan` → `expr-scan` → `ExprScanOp`. Two takeaways:

1. To make a specific function a *streaming* table-valued source, n1k1 branches at
   the **converter** (`VisitExpressionScan`): recognize the function and route it
   to a streaming source op instead of `expr-scan`. No grammar/planner change —
   the planner already hands us the call as the FROM expression.
2. For subqueries/CTEs, `SetSubqueryPlan(selOp)` means **the planner already
   handed us a ready-to-run child operator tree** for the subquery. n1k1 currently
   ignores it and re-evaluates the subquery *expression* via `Evaluate` (which
   materializes). Converting `selOp` into a child op and piping it is the concrete
   hook for streaming subqueries/CTEs (see below).

### The materialization problem (and the fix)

Downstream is streamed and spillable, but the **source is fully materialized
first**. `ExprScanOp` today does:

```
v, _  := expr.Evaluate(item, ctx)   // whole result built as one value.Value (in memory)
jv, _ := json.Marshal(v)            // whole result serialized again (in memory)
base.ArrayYield(jv, yieldVals, …)   // only now streamed row-by-row
```

So a table-valued function that produces a huge array is built (and marshaled)
in full before a single row flows. For big outputs (shredding a 500-page PDF, a
large XLSX) that's the memory blow-up you're worried about.

The fix is a **streaming source-function protocol** distinct from the scalar
expression contract. The cbq `expression.Expression` contract is fundamentally
"evaluate → one value", which is why it materializes. Instead:

- Add a **source op** (like the existing `scan`/`csvData`/`datastore-scan`
  yielders in `engine/op_scan.go`) that calls a Go *generator* — a function with
  signature roughly `func(args, yield func(base.Vals) bool) error` — pushing rows
  as it produces them. The engine is already **push-based with backpressure**
  (the consumer drives the drain via `YieldVals`), so a generator yielding into
  it streams with bounded memory automatically.
- The planner still sees `FROM func(...)`; the converter routes *known streaming
  source functions* to this op instead of `expr-scan`. Unknown/scalar ones keep
  the materializing path.

### Streaming JS/goja functions (callback fashion, mirroring the engine)

The same generator shape extends cleanly to Tier-2 JS functions, so a drop-in JS
function can *also* be a streaming table-valued source rather than returning one
giant array. Give the JS an **`emit(row)` callback** (or let it `return` a JS
generator / async iterator) that the goja host bridges straight to the source
op's `yield`:

```js
// docs/functions/shred_lines.js  — streams one row per line, never builds an array
function shred_lines(path) {
  for (const line of read_lines(path)) {   // host-provided lazy iterator
    emit({ line });                          // -> engine yield (backpressure applies)
  }
}
```

The goja host wires `emit` to the op's `func(base.Vals) bool` yield: each `emit`
marshals the JS value to a `base.Val` row and pushes it downstream, and the
boolean return propagates consumer backpressure/early-stop (e.g. a `LIMIT`) back
into the JS loop. This mirrors exactly how n1k1's native operators yield, so a JS
UDF and a Go generator source behave identically to the rest of the pipeline —
bounded memory, spillable consumers, early termination. A JS function that simply
`return`s a value keeps the materializing `expr-scan` path; only ones that call
`emit` (or return an iterator) take the streaming source path.

This is exactly why **document shredding belongs at the source/scan layer, not as
a scalar expression** (see DESIGN-data.md): shredding is one-to-many, I/O- and
memory-heavy, and streams naturally. `SELECT … FROM shred("docs/*.pdf") AS d`
then composes with WHERE/GROUP BY/indexing like any other scan, and spills like
any other operator — instead of materializing a giant array inside one
expression call.

## Streaming CTEs / subqueries (avoiding materialization)

Same materialization shape appears with CTEs. `VisitWith` records each WITH
binding; a non-recursive `FROM cte` is **inlined** as its subquery expression and
run through `expr-scan` — i.e. evaluated to a full value, then streamed. Recursive
CTEs (`with-recursive`) materialize each working set per fixpoint iteration.
(cbq itself materializes subqueries/CTEs too, so this matches upstream.)

Two improvements, in increasing ambition:

1. **Single-use CTE → pure pipe.** If a CTE is referenced exactly once, run its
   SELECT as a **child operator feeding the consumer directly** rather than
   inlining-and-evaluating to a full value. No materialization at all — pure
   streaming. The planner already gives us the raw material: the subquery's
   `plan.ExpressionScan` carries its sub-plan via `SetSubqueryPlan(selOp)` (see
   above), so the converter can convert `selOp` into a child op and wire it in,
   instead of routing to the materializing `expr-scan` temp.
2. **Multi-use CTE → materialize/spill once, re-scan.** When referenced N>1
   times, evaluate once into a **spill-backed buffer** (the same temp-file
   machinery `ORDER BY`/join/group already use — `base/heap.go` auto-spills when a
   buffer grows too large) and re-scan it per reference. Bounded memory, computed
   once. This goes *beyond* cbq's always-materialize-in-RAM behavior.

Both rely on the engine's existing yield + spill primitives; the new work is in
the converter/planner-bridge (choosing pipe vs spill) rather than the runtime.

## Licensing shortlist (document parsers)

Permissive only (MIT/BSD/Apache) — **avoid** UniDoc/UniPDF/unioffice
(AGPL/commercial):

| Format | Candidate | License |
|---|---|---|
| PDF text | `ledongthuc/pdf` / `dslipak/pdf` | BSD-3 |
| XLSX | `xuri/excelize` | BSD-3 |
| DOCX | `nguyenthenguyen/docx` | MIT |
| JS runtime | `dop251/goja` | MIT |

Verify each at adoption time (transitive deps included).

## Caveats

- **Security / sandboxing.** File-reading and JS-executing functions are a real
  attack surface for an embeddable engine. Gate behind a capability/flag, restrict
  accessible paths, and cap goja's reach (it's in-process).
- **Determinism.** Streaming sources and user JS can be non-deterministic and
  can't be cheaply re-read; a re-scan means re-run or spill. Keep the suite's
  determinism rules (see DESIGN-testing.md) in mind if these ever appear in tests.
- **Fast-path exclusion.** All of these run in the interpreted/boxed lane, not the
  byte-native fast path or the compiler codegen path.

## Roadmap (suggested phasing)

1. **Wire the UDF bridge** (init functions subsystem; implement
   `VisitExecuteFunction`; provide n1k1's resolver + storage). Unlocks Tiers 2–3.
2. **Tier 2 goja + directory registry** — the "JS in a repo" feature.
3. **Streaming source-function op** + route `FROM shred(...)`/loaders to it;
   pair with the DESIGN-data.md file-source work.
4. **Native Go builtins** via `expression.RegisterFunction` (fork) for the
   document parsers, or expose them as sources per step 3.
5. **Streaming CTEs** — single-use pipe first, then multi-use spill-and-rescan.
