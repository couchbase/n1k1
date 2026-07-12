# Extending n1k1 — functions, drop-in extensions, dynamic loading, and table-valued/streaming

## Status & remaining TODOs

_Last reviewed: 2026-07-11._

**Done:** The first extension slice is live and tested (interpreter + compiler): native zero-garbage aggregates `sparkline()`/`histogram()`, goja JavaScript scalar UDFs (opt-in dir/file/inline registry), JS aggregate UDFs (3-callback) and streaming table-valued sources (`emit` protocol, on one generic `stream-fn` op that `RULE_MATCHES` also rides), plus `*.extract.js` recipes whose `describe()` returns a declarative `ExtractSpec` applied on the native byte lane — all unlocked by two fork setters (`expression.RegisterFunction` / `algebra.RegisterAggregate`) that open the parser's builtin + aggregate registries without a grammar change. And `*.macro.js` **pre-parse SQL++ macros** (`@name(...)` → generated SQL++ before cbq's parser; gensym hygiene, `.macro expand`) — `grep_context` ships as the first, turning grep `-A`/`-B`/`-C` into a one-liner.

**Remaining (headline TODOs):**
- [ ] Extract recipes are `describe()`-only; the imperative `extract(file, meta, emit)` escape hatch for irregular formats is not yet wired.
- [ ] Streaming sources don't early-terminate on `LIMIT` (the `YieldStats` LIMIT hook is inert) — unbounded sources hang under `LIMIT`; needs engine-wide producer early-exit.
- [ ] JS aggregate/streaming UDFs are v1: state round-trips through JSON per Update (not zero-garbage) and callbacks have no error channel (throw/NaN → null).
- [ ] Full cbq UDF bridge unwired — `VisitExecuteFunction` returns `NA()`; no `CREATE FUNCTION` DDL / metadata catalog (Tier 3, roadmap step 1).
- [ ] More native `base.Agg` aggregates beyond `sparkline`/`histogram`.
- [ ] Streaming CTEs / subqueries: single-use pipe + multi-use spill-and-rescan (roadmap step 5) — not started.
- [ ] wazero (Wasm) sandboxed extensions (roadmap step 6) — not started.
- [ ] Hygienic JS reuse (`require()`/modules) + power-tier host functions (HTTP/S3, allowlisted `exec`); a complete extension-authoring guide.
- [x] **Macros (`*.macro.js`)** — DONE: pre-parse, text→text SQL++ generators expanded before cbq's parser (`@name(...)` sigil, `=>` named args, applicative-order + body-reemission nesting, gensym hygiene, `.macro list`/`help`/`expand`, parse-error annotation). `glue/macro.go` (scanner + registry), `glue/ext_macro_jsvm.go` (JS binding), hooked at the top of `ParseStatement`. `extensions/macros/grep_context.macro.js` ships. Later: AST-hygiene tier; `require()`/modules; more starter macros (top-per-group, sessionize).

## Overview

n1k1 runs SQL++ via cbq's parser + planner, evaluating expressions natively where
ported (else via the embedded cbq evaluator). This doc grows the engine's *surface*:
builtins, drop-in user functions (JS or Go), dynamic loading, and table-valued
functions that return whole tables (e.g. shred a PDF/PPT/DOC/XLSX into many JSON
rows) — ideally **streaming** so a huge result never materializes. Yes to all, in
tiers with different trade-offs. Most plumbing exists (the UDF-resolution seam,
`FROM <expr>` scans, a push-based streaming engine, spill-to-disk); the work is
wiring plus one new streaming source-function protocol. One hard constraint frames
everything: n1k1 builds `CGO_ENABLED=0`.

## Contents

- [Status & remaining TODOs](#status--remaining-todos)
- [Implemented details (2026-07)](#implemented-details-2026-07)
- [The function-name resolution seam](#the-function-name-resolution-seam)
- [Hard constraint: CGO_ENABLED=0](#hard-constraint-cgo_enabled0)
- [Extensibility tiers](#extensibility-tiers)
- [JS UDF runtime & state (the goja execution model)](#js-udf-runtime--state-the-goja-execution-model)
- [Extension aggregates](#extension-aggregates)
- [Table-valued / streaming sources in FROM](#table-valued--streaming-sources-in-from)
- [Extract functions (`*.extract.js`) — file-matched, scan-layer extensions](#extract-functions)
- [Macros (`*.macro.js`) — pre-parse SQL++ generators](#macros)
- [Dynamic loading in Go](#dynamic-loading-in-go)
- [JS extension power tiers](#js-extension-power-tiers)
- [Streaming CTEs / subqueries](#streaming-ctes--subqueries)
- [Scanning a corpus (a directory of documents)](#scanning-a-corpus-a-directory-of-documents)
- [Namespacing & versioning of extensions](#namespacing--versioning-of-extensions)
- [Licensing shortlist (document parsers)](#licensing-shortlist-document-parsers)
- [Caveats](#caveats)
- [Vision: a sandboxed extension registry](#vision-a-sandboxed-extension-registry)
- [Roadmap (suggested phasing)](#roadmap-suggested-phasing)

## Implemented details (2026-07)

First slice is **live and tested** end-to-end in the interpreter (`test/ext_test.go`);
full suite (interpreter + compiler) shows no regressions. Compiler mode: extension
aggregates dispatch through the same `base.AggCatalog[name]` runtime lookup group-op
codegen already emits, so they compile by construction; a compiled JS UDF needs its
`Register*` call to have run in the executing process (the name must re-resolve when the
baked `exprTree`/`exprStr` is re-parsed).

### Parser-resolution setters

Two tiny fork setters unlock parser resolution of new names without a grammar change:
`expression.RegisterFunction(name, fn)` (patch-05) and
`algebra.RegisterAggregate(name, property, agg)` (patch-06, `glue/patches/README.md`).
They expose the package-private `_FUNCTIONS`/`_AGGREGATES` maps.

### Tier-2 JavaScript scalar UDFs

Extension-agnostic loader dispatching **by file extension** (today `.js`; `.wasm`/etc.
slot into `extensionLoaders` later):

- `glue.RegisterExtensionDir(dir)` scans a directory; `glue.RegisterExtensionFile(path)`
  loads one file; `glue.RegisterJSFunc(name, source)` registers inline JS. The
  directory *is* the catalog.
- A `.js` file becomes an `expression.Function` (`glue/ext_jsvm.go`) resolved as
  `NAME(args)`, evaluated through the interpreted/boxed lane (ExprTree →
  `Expression.Evaluate`).
- Runtime is pure-Go/MIT goja, preserving `CGO_ENABLED=0`. Object/array args
  deep-copied in (no source mutation); a runaway script is interrupted
  (`glue.JSCallTimeout`); a name shadowing a builtin/aggregate is refused.
- Loading is **opt-in** (in-process user code is an attack surface): embedder,
  CLI `-ext`/`-extensions` (repeatable, comma-friendly), or `.extensions` REPL command
  (`list`/`load`/`unload`). `glue.ListExtensions()` / `glue.UnloadExtension(name)`
  back a registry (unload installs a stub that errors on call, since cbq's registry has
  no delete; reload re-enables). Examples in `extensions/functions/js/`.

### Extension aggregates

- **Native `sparkline()` / `histogram()`** — **zero-garbage**, against the `base.Agg`
  byte-slice Init/Update/Result protocol (`base/agg_ext.go`): Update only appends bytes
  (reusing MEDIAN/VARIANCE numeric-list state); Result renders a unicode inline chart
  (▁▂▃▄▅▆▇█) by walking the byte state into the reusable buffer — no intermediate
  `[]float64`. A parse/plan-only `algebra.Aggregate` shim (`glue/ext_agg.go`) makes the
  parser accept them; conv.go routes computation to `base.AggCatalog[name]`, so cbq's
  Cumulate*/ComputeFinal never runs. Auto-registered at glue init.
- **JS aggregates (3-callback protocol)** — `NAME_init()` / `NAME_update(state, value)`
  / `NAME_final(state)` (`glue/ext_jsvm_agg.go`, `glue.RegisterJSAggregate`, or a
  `NAME.agg.js` file). A `base.Agg` bridge threads `state` as JSON bytes in the group's
  spillable buffer, driving the callbacks on the same per-query/per-actor runtime as
  scalar UDFs. Same `algebra.Aggregate` shim. Trade-off vs native: state round-trips
  through JSON per Update (not zero-garbage) and callbacks have no error channel
  (throw/NaN → null). Ships `extensions/functions/js/geomean.agg.js`.

Both reuse the same shim, so `NAME(expr)` works in GROUP BY and as a bare aggregate.

### Table-valued JS functions in FROM

1. A JS UDF that *returns an array* is a set-returning source via the existing
   `plan.ExpressionScan` → `expr-scan` → `base.ArrayYield` path (materializes first).
2. **Streaming sources (`*.stream.js` / `glue.RegisterJSStream`)** — a
   `function NAME(emit, ...args)` pushes rows via `emit(row)` (one row per argument = a
   batch form). The producer lives in `glue/ext_jsvm_stream.go` and implements the
   generic `StreamSource` interface; `VisitExpressionScan` recognizes any such FROM
   expression and routes it to one shared `stream-fn` op (`glue/op_stream_fn.go`,
   `StreamFnOp`) — the same op `RULE_MATCHES` rides — yielding as rows are produced,
   **no materialization, bounded memory**, composing with WHERE/GROUP BY/LIMIT. Ships
   `extensions/functions/js/series.stream.js`. Caveat (matches every n1k1 source): no
   per-producer early-exit yet, so `LIMIT k` drops extras downstream while the source
   runs to completion — fine for a huge *finite* source, but an *unbounded* one hangs
   under `LIMIT`.

Everything below Status is the fuller forward-looking design/roadmap (WASM, streaming
sources, document shredding, a registry).

## The function-name resolution seam

At parse time `NAME(args)` resolves in this order (`parser/n1ql/n1ql.y:5740`):

1. `expression.GetFunction(name)` — static **builtin** registry
   (`expression/func_registry.go`, unexported `_FUNCTIONS`).
2. `search.GetSearchFunction(name)` — FTS.
3. `algebra.GetAggregate(name, …)` — aggregates.
4. `expression.GetUserDefinedFunction(name, …)` → `functions.PreLoad(name)` — the
   **UDF** subsystem (pluggable storage + language runtimes).
5. else → `FatalError("Invalid function …")`.

Two extension points: the builtin registry (step 1) and the UDF resolver (step 4).
n1k1 owns the fork, so both are open.

**Current state:** the UDF bridge is *not* wired — `glue/conv.go`'s
`VisitExecuteFunction` returns `NA()` and no functions storage/language is initialized,
so unknown/UDF names error at parse today. Wiring that bridge is the one-time
prerequisite for the drop-in tiers below (and for `CREATE FUNCTION` DDL).

**Off this seam entirely: [extract functions](#extract-functions).** They are matched
to *files* (by extension/regexp), not invoked by *name* in SQL, so they never go
through steps 1–4 and need no parser resolution — they register into a separate
scan-layer extract registry (`DESIGN-data.md` §4). Keep the two dispatch axes distinct:
name→function (this seam) vs file→extractor (the extract registry).

## Hard constraint: CGO_ENABLED=0

n1k1 builds **`CGO_ENABLED=0`** — a pure-Go static binary (the Makefile and every
build/test enforce it). That rules out anything needing cgo, notably Go's `plugin`
package (see [Dynamic loading in Go](#dynamic-loading-in-go)). Everything here stays
cgo-free.

## Extensibility tiers

### Tier 1 — Native Go builtins (best for heavy/binary work like shredding)

`expression.Function` implementations in the static map. Register via
`expression.RegisterFunction(name, fn)` (patch-05) from n1k1's own `glue` package at
init — keeping implementations in n1k1 so `base`/`engine` stay cbq-free.

- Good for: functions needing Go libraries or real I/O (file loaders, parsers).
- Runs in the interpreted/boxed lane (cbq `Evaluate` or a native `ExprCatalog`
  handler) — not the zero-alloc byte fast-path or compiler codegen. Fine for I/O-bound
  enrichment; not tight numeric loops.

### Tier 2 — "A bunch of JS in a directory / git repo" (drop-in UDFs)

No `CREATE FUNCTION` DDL required. n1k1 supplies its *own* UDF resolver instead of
cbq's metakv/Enterprise machinery:

- **Registry = the filesystem.** Scan a directory or cloned git repo
  (`.n1k1/functions/*.js`); each exported function → a resolvable UDF name. `git pull`
  to update.
- **Runtime = goja** (MIT, pure-Go, no V8/cgo/Enterprise dep). The bridge's `Execute()`
  marshals args → runs JS → returns JSON. (cbq's own golang/JS UDF paths are
  Enterprise-only: `functions/golang/golang.go:78` uses `plugin.Open` on `.so` files —
  toolchain-locked, Linux-mostly — and Community is a stub. goja is *lighter*.)
- **Optionally streaming** in FROM via an `emit(row)` callback (see streaming sources).

### Tier 3 — Inline N1QL UDFs (`CREATE FUNCTION … { expr }`)

Pure SQL++, trivial to wire (an expression bound to a name), but expression-only —
can't touch a PDF. Composes with Tiers 1–2.

## JS UDF runtime & state (the goja execution model)

The live `goja.Runtime` is scoped **per query, per actor** (`glue/ext_jsvm.go`):
programs compile once at registration, but the runtime builds lazily on the eval context
(`GlueContext.jsRT`) — a fresh `GlueContext` per `Session.Run`, and `ChainExtend`'s
per-actor context clone gives each concurrent UNION ALL branch (and future parallel
scan/GROUP-BY shard) its own. One `goja.Runtime` isn't goroutine-safe, so this keeps
each single-threaded with **no pool and no lock** (per DESIGN.md). Each runtime defines
*all* loaded UDFs in one shared JS scope and installs a `console`. Consequences (covered
by `test/ext_test.go`):

- **Module-scope globals persist across calls within a query, RESET on the next query**
  — good for per-query caches (hoisted regexes, memo tables); a "global counter" resets
  per query, so use SQL aggregates (or a `base.Agg` extension) for cross-row
  accumulation. No cross-query leakage.
- **A UDF can call another loaded UDF** — shared global scope.
- **`console.log`/`.error`/`.warn`/`.info`/`.debug`** write to `glue.JSConsoleWriter`
  (default `os.Stderr`).
- Cost: one `goja.New()` + re-running all UDF programs per query/actor, amortized over
  the query's rows (fine for n1k1's scan-heavy profile — measured JS boundary ≈
  **1 µs/row**, dominated by the boxed `ConvertVals`, not goja).
- **The whole runtime dies with the query.** `jsRT` hangs off the per-`Session.Run`
  `GlueContext`, referenced nowhere else, so at query return the runtime and every JS
  object it holds becomes unreachable and GC'd. A UDF can pile up heap *within* one query
  (bounded by the query's lifetime, reclaimed at its end); **no process-lifetime
  accumulation or leak across queries.** A panic/timeout drops the runtime mid-query;
  each UNION ALL actor's runtime frees independently.
- **`async`/`await`/Promises are rejected** (no event loop); a Promise return fails with
  a clear message rather than hanging. See
  [Sync vs async](#sync-vs-async--do-we-need-asyncawait).

## Extension aggregates

Two styles ship — native zero-garbage `base.Agg` (`sparkline()`/`histogram()`) and JS
(`NAME_init`/`NAME_update`/`NAME_final`), both reusing the same `algebra.Aggregate` shim.
Full detail in [Status](#extension-aggregates).

## Table-valued / streaming sources in FROM

A **table-valued** (set-returning) function returns a JSON array used in FROM —
`SELECT x.* FROM my_func(…) AS x` — each element a row. (Sibling: `UNNEST` over an
array field.)

**Already works.** `FROM <expr>` is `plan.ExpressionScan`, handled by
`glue/conv.go:VisitExpressionScan` → the `expr-scan` op (`glue/datastore.go:ExprScanOp`).
An array yields each element as a row via `ArrayYield` (`base/base.go:324`); a non-array
becomes one row.

### How the plan gets there (cbq planner → n1k1)

The node is produced by **cbq's own planner**; n1k1 only converts it.
`planner/build_select_from.go` creates a `plan.ExpressionScan` in two cases:

- `FROM <expr> AS x` — an `algebra.ExpressionTerm` (covers `FROM my_func(...)`,
  `FROM [array]`, `FROM cte`): `plan.NewExpressionScan(node.ExpressionTerm(), …)`
  (~line 765).
- `FROM (SELECT …) AS x` — a subquery term
  (`plan.NewExpressionScan(algebra.NewSubquery(subquery), …)`, ~line 677), which **also
  builds the subquery's full sub-plan** and attaches it via
  `exprScan.SetSubqueryPlan(selOp)`.

Chain: SQL FROM term → cbq planner → `plan.ExpressionScan` → `VisitExpressionScan` →
`expr-scan` → `ExprScanOp`. Two takeaways:

1. To make a function *streaming*, n1k1 branches at the **converter**
   (`VisitExpressionScan`): recognize it, route to a streaming source op. No
   grammar/planner change.
2. For subqueries/CTEs, `SetSubqueryPlan(selOp)` means **the planner already handed us a
   ready-to-run child operator tree**. n1k1 currently ignores it and re-evaluates the
   subquery expression via `Evaluate` (materializes). Converting `selOp` into a piped
   child op is the hook for streaming subqueries/CTEs (see
   [Streaming CTEs / subqueries](#streaming-ctes--subqueries)).

### The materialization problem (and the fix)

Downstream is streamed and spillable, but the **source is fully materialized first**.
`ExprScanOp` today:

```
v, _  := expr.Evaluate(item, ctx)   // whole result built as one value.Value (in memory)
jv, _ := json.Marshal(v)            // whole result serialized again (in memory)
base.ArrayYield(jv, yieldVals, …)   // only now streamed row-by-row
```

So a huge array is built and marshaled in full before a single row flows — the memory
blow-up (shredding a 500-page PDF, a large XLSX).

The fix is a **streaming source-function protocol** distinct from the scalar contract
(which is "evaluate → one value", hence it materializes):

- Add a **source op** (like `scan`/`csvData`/`datastore-scan` in `engine/op_scan.go`)
  calling a Go *generator*, signature ~`func(args, yield func(base.Vals) bool) error`,
  pushing rows as produced. The engine is already **push-based with backpressure**, so a
  generator yielding into it streams with bounded memory automatically.
- The converter routes *known streaming source functions* here; unknown/scalar ones keep
  the materializing path.

The same generator shape extends to Tier-2 JS: give it an **`emit(row)` callback** (first
argument) instead of returning a giant array. The goja host wires `emit` to the op's
`func(base.Vals) bool` yield — each `emit` marshals to a `base.Val` row and pushes
downstream; the boolean return propagates backpressure/early-stop (e.g. `LIMIT`) into the
JS loop. A JS function that just `return`s a value keeps the `expr-scan` path; only ones
calling `emit` (or returning an iterator) take the streaming path.

```js
// shred_lines.js — streams one row per line, never builds an array
function shred_lines(path) {
  for (const line of read_lines(path)) emit({ line });   // host-provided lazy iterator
}
```

### Emitting a *batch* of rows per crossing

The engine is **batch-oriented one layer down**: the per-op yield is per-row
(`base.YieldVals func(base.Vals)`), but cross-actor transport is a channel of batches —
`base.Stage.BatchCh chan []base.Vals`, filled by `StartActor(…, batchSize)`. So
`[]base.Vals` is native currency.

For **JS** this is the throughput lever: since the goja↔Go boundary costs ~1 µs/row, a
source that `emit`s an **array of rows per call** (`emit_batch([r1, r2, …])`, or a
generator yielding chunks) amortizes one crossing over many rows. The host hands the
decoded chunk straight through as one `[]base.Vals` to the Stage exchange, matching
`BatchCh`; backpressure applies at batch granularity. So the protocol admits both:
`emit(row)` (ergonomic default) and `emit_batch([rows])` (performance variant).

### What the JS looks like (worked example)

Three ways to author the same source — `n` rows `{i, sq}`.

**(a) Materializing — works today.** A scalar UDF `return`ing an array; `FROM gen(n)`
streams elements via `expr-scan`/`ArrayYield`. But the whole array is built and marshaled
before any row flows, so `gen(1000000)` allocates a million-element array even under
`LIMIT 5`.

```js
function gen(n) {
  var rows = [];
  for (var i = 1; i <= n; i++) rows.push({ i: i, sq: i * i });
  return rows;
}
```

**(b) Streaming, one row — the `emit` protocol.** `emit` returns `false` when the
consumer wants no more (e.g. `LIMIT` satisfied), so the loop stops early. The idiomatic
ES form is a **generator** (host drives `.next()`, stops when full — no explicit
backpressure check).

```js
function gen_stream(emit, n) {
  for (var i = 1; i <= n; i++)
    if (!emit({ i: i, sq: i * i })) return;   // consumer done (e.g. LIMIT hit)
}
function* gen_gen(n) { for (var i = 1; i <= n; i++) yield { i: i, sq: i * i }; }
```

**(c) Streaming in batches — amortize the boundary.** `emit` a chunk per call; the host
hands each to the Stage exchange as one `[]base.Vals`. Same backpressure.

```js
function gen_batch(emit, n) {
  var BATCH = 256, chunk = [];
  for (var i = 1; i <= n; i++) {
    chunk.push({ i: i, sq: i * i });
    if (chunk.length === BATCH) { if (!emit(chunk)) return; chunk = []; }
  }
  if (chunk.length) emit(chunk);
}
```

All three are used identically in SQL — the converter routes a streaming source to the
streaming op, an array-returner to `expr-scan`:

```sql
SELECT x.i, x.sq
FROM gen_stream(1000000) AS x     -- never materializes a 1e6 array (bounded memory)
WHERE x.sq > 10
LIMIT 5
```

(A real I/O source — `shred_lines(path)` over `read_lines(path)` — is the same shape with
a host-provided lazy iterator instead of a counter.)

### v1 shipped, and the LIMIT early-exit caveat

**Implemented (v1, `*.stream.js`).** The per-row `emit(row)` form ships:
`glue.RegisterJSStream` / a `NAME.stream.js` file registers a `jsStreamFunc` (its
producer in `glue/ext_jsvm_stream.go` implements `StreamSource`), the converter routes
`FROM NAME(...)` to the generic `stream-fn` op (`glue/op_stream_fn.go`), and rows flow
one at a time — bounded memory — composing with WHERE/GROUP BY/aggregates.
`emit(a, b, …)` yields one row per argument.

v1 does *not* early-terminate the source on `LIMIT` (the `YieldStats` LIMIT hook is
inert — `op_order.go`): `LIMIT k` returns the right rows by dropping extras downstream
while the JS loop runs to completion. Upshot: a huge *finite* source is fine; an
**unbounded** source (`for(;;) emit(...)`) hangs under `LIMIT`. Bound your source until
engine-wide producer early-exit lands (then `emit` returns `false` and the loop stops,
for free).

### Advanced: can JS participate in the reusable-slice discipline?

n1k1 recycles byte buffers (`varLift`, `[]byte` per row). An expert author can hook in
via `ArrayBuffer` — with limits:

- **`SharedArrayBuffer` isn't relevant** — it's for cross-agent (Web Worker) memory; a
  goja `Runtime` is single-threaded. Plain **`ArrayBuffer`** + typed-array views is what
  matters.
- **Near-zero-copy IN.** goja backs an `ArrayBuffer` with a Go `[]byte`
  (`Runtime.NewArrayBuffer([]byte)`), handing JS a `Uint8Array` *view* over n1k1's row
  buffer.
- **Near-zero-copy OUT.** JS writes into a preallocated `ArrayBuffer` n1k1 reads back
  (`ArrayBuffer.Bytes()`), instead of returning JS objects goja must marshal.
- **Limits.** (1) *Lifetime*: the row buffer recycles next iteration, so a view is valid
  only *within* the callback — consume/copy out first; the push model makes that window
  well-defined. (2) goja GC-manages ordinary values; only the `ArrayBuffer` backing store
  is manual, and typed arrays made inside JS still allocate. (3) Expert-only — most
  functions marshal a value and pay the copy.

This is why **document shredding belongs at the source/scan layer, not a scalar
expression** (DESIGN-data.md): one-to-many, I/O- and memory-heavy, streams naturally.
`SELECT … FROM shred("docs/*.pdf") AS d` composes with WHERE/GROUP BY and spills like any
operator.

## Extract functions (`*.extract.js`) — file-matched, scan-layer extensions <a name="extract-functions"></a>

Everything above is **name-invoked**: `NAME(args)` in SQL, resolved through the
[function-name seam](#the-function-name-resolution-seam). **Extract functions are a
different class**, on a different dispatch axis: they are *implicit*, **matched to
files** by extension/regexp and run by the **scan/extract layer** (`DESIGN-data.md`
§4) to turn messy inputs into typed rows **plus** the file-level metadata the engine
prunes and merges by. They never appear in SQL and never touch the parser seam; they
register into a separate **extract registry** (`DESIGN-data.md` §4 owns the seam &
metadata schema — this section owns the *JS authoring surface* that plugs into it).

**n1k1 core is domain-agnostic — all file knowledge lives in the recipes.** A user
brings a git-cloned repo of JS extract recipes that understand *their* files: log
formats, financial-filing dumps, astronomical catalogs, sensor streams — whatever they
have. The engine only provides the generic match→describe→extract seam. The recurring
example here is `DESIGN-prepare.md`'s **PREPARE++** (recipes cracking a `cbcollect_info`
support bundle into clean, time-sortable keyspaces a detector corpus queries) because it
drove the design — but nothing about bundles, or Couchbase, is baked in; swap the recipe
repo and the same machinery serves SEC filings or FITS files. The extract repo is the
*sibling* of the detector repo: both versioned in git, both content-addressed, both
consumed by the corpus compiler.

### The contract: `match`, `describe`, `extract`

A `*.extract.js` file (new suffix, slotting into the same `extensionLoaders` dispatch
as `*.agg.js`/`*.stream.js`) exports up to three things:

```js
// cb_ns_server.extract.js — ns_server-style Couchbase logs

// (1) match — which files this recipe claims. Extension list and/or regexps over the
//     bundle-relative path; higher `priority` wins on overlap (a specific pattern
//     beats a generic `\.log$`). This is ALSO DESIGN-prepare.md's source-routing key.
export const match = {
  exts:     [".log"],
  names:    [/ns_server\..*\.log$/, /^diag\.log$/, /^info\.log$/],
  priority: 10,
};

// (2) describe(file) -> ExtractSpec (+ measured metadata). CHEAP: runs ONCE per file,
//     may only sample (file.head/tail/slice), memoized into the .n1k1 sidecar keyed by
//     the file fingerprint. Returns a DECLARATIVE spec n1k1 executes natively, so no
//     JS runs on the per-record hot path. This is where the hard, format-specific
//     knowledge lives.
export function describe(file) {
  const banner = file.head(4096);                    // parse the ==== cbcollect banner
  return {
    format:  "ns_server_log",
    framing: { kind: "multiline", continuation: /^\s|^\[/ }, // Erlang dumps span lines
    fields:  { pattern: /\[(?<module>\w+):(?<level>\w+),(?<ts>[^,]+),(?<node>[^:]+):/ },
    time:    { field: "ts", layout: "RFC3339", tz_default: "+02:00" }, // -> int64 ns
    order:   { sorted: "near", disorder_bound: { window: "2s" } },
    provenance: { command: bannerCommand(banner), node: bannerNode(banner) },
  };
}

// (3) extract(file, meta, emit) -> records. OPTIONAL imperative escape hatch for
//     formats too irregular for a spec (crack a binary blob, stateful assembly).
//     `meta` is THIS FILE'S earlier describe() result (spec + measured metadata),
//     handed in from the .n1k1 sidecar cache — so extract never re-sniffs: it reads
//     meta.framing/meta.time/meta.provenance and gets to work. Streams via emit()
//     (bounded memory, backpressure) — but pays the per-record JS boundary (~1 µs/row),
//     so prefer a declarative describe wherever the format is regular.
export function extract(file, meta, emit) {
  for (const rec of file.records(meta.framing)) emit({ ts: rec.ts, msg: rec.text });
}
```

**Preferred path: `describe` returns a spec, n1k1 executes it natively.** Because GB
of log lines can't afford the ~1 µs/row goja boundary, the *default* is that JS runs
**only in `describe`** (once per file), returning a declarative `ExtractSpec`
(`framing`/`fields`/`time`/`order`/`provenance` — `DESIGN-data.md` §4). n1k1 applies
that spec on the **byte-native fast lane** (regex field capture, timestamp
normalization), so the hot per-record path never enters JS. **v1 ships exactly this
`describe()`-only path** (`glue/ext_extract_jsvm.go` registers a `records.Recipe` with a
`Describe` closure and `Extract` nil; native `records.SpecApply` runs the returned spec).
`extract(file, meta, emit)` — the imperative fallback for the irregular tail, riding the
`stream-fn` op — is **designed but not yet wired** (a headline TODO).

### The `file` host object (read-only, single-file capability)

`describe`/`extract` get a `file` handle whose authority is **exactly one file** — no
network, no other paths, no exec (unlike the power-tier sources' `-ext-allow-net/exec`).
Members:

- `file.path`, `file.size`, `file.name`, `file.ext` — identity (bundle-relative path).
- `file.head(n)` / `file.tail(n)` / `file.slice(off, len)` — sampling reads for a
  cheap `describe` (don't read a 400 MB log to sniff its format).
- `file.lines()` / `file.records(framing?)` — lazy iterators for `extract` (bounded
  memory; the host frames per `framing` or per line).

Keeping the capability to one read-only file makes an extract recipe the *ideal Wasm
shape* later ("bytes in → transform → rows/spec out, no ambient authority" — the
[registry vision](#vision-a-sandboxed-extension-registry)); the JS tier is the pure-Go
`CGO_ENABLED=0` starting point.

### The registry = a git repo, matched by file (not by name)

Reuse the Tier-2 loader machinery, one axis changed: instead of *name → function*, the
extract registry is *file-matcher → describe/extract*. `RegisterExtractDir(dir)` scans
`.n1k1/extractors/*.extract.js` (or a `-extractors` flag / cloned repo); each file's
`match` block indexes it. Overlap resolves by `priority` then load order. `git pull`
adds formats. n1k1 ships **built-in** extractors for the common cbcollect formats and
the office/PDF baseline (`records/extract.go`, re-expressed as `{framing: whole}`
specs — `DESIGN-data.md` §4); the repo *extends*, never having to touch Go.

This repo is one leg of `DESIGN-prepare.md`'s **late binding** triad: per *logical*
keyspace it versions the **detectors** (the corpus), the **adapter** (this recipe's
`describe`/`extract` — what normalizes a bundle's raw records into the logical
keyspace's canonical schema), and the per-bundle **binding manifest** (logical → the
files in *this* bundle, resolved by the `match` glob/regex, exactly the "convention /
content-sniffing" rungs of that section's ladder). Because a recipe is *data* the
datastore loads at run time, rebinding a prepared corpus to a new, differently-named
bundle needs **no recompilation** (`DESIGN-data.md` §4 "This *is* late binding").

### Why this shape (recap of the split)

- **`describe` is pluggable & cheap; `extract` is native & hot.** The format-specific
  intelligence (which regex, how near-sorted, what a header means) is JS that runs
  once; the GB-scale row production is native from the returned spec. Best of both.
- **Describe once, reuse forever.** The describe result is memoized in `.n1k1/` keyed
  by file fingerprint (`DESIGN-data.md` §5), and handed to every later `extract` as its
  `meta` argument — across this query and all future ones. An unchanged file never
  re-describes; a changed file re-describes only itself. The expensive, format-specific
  pass is paid once per file, not once per scan.
- **The metadata is first-class, not a side effect.** `describe`'s `time`+`order` is
  precisely the [sorted-source contract](DESIGN-data.md) the **K-way near-sorted merge
  join** and **ASOF** temporal correlation consume — the reason support engineers can
  ask "what was the rebalance state when this error fired?" over unsorted-looking logs.
- **One matcher, two consumers.** The `match` regexp that selects an extractor is the
  same signal `DESIGN-prepare.md`'s MQO uses to route a detector's `FROM` to the files
  it targets — so building extract recipes also builds the source-routing index.

### Testing (golden fixtures, mirrors the detector corpus)

Each recipe ships a golden fixture: a **tiny file fragment → expected `ExtractSpec` +
expected first-N records** (and, for sorted sources, the expected `min_key`/`max_key`/
`disorder_bound`). CI runs every recipe against the fixture library on change — the
same differential/golden discipline as detectors (`DESIGN-testing.md`,
`DESIGN-prepare.md`), so an AI agent can propose a recipe for a newly-seen log format
with confidence, and a wrong `disorder_bound` (a silent merge-corruption risk) is
caught before it ships.

## Macros (`*.macro.js`) — pre-parse SQL++ generators <a name="macros"></a>

### The problem: WINDOW syntax is a wall

Grep's `-A`/`-B`/`-C` (print N lines of context around each match) is the single most
natural log question — and expressing it in SQL++ means a windowed subquery most people
(and, honestly, most AIs on the first try) can't write cold:

```sql
SELECT p, pos, line FROM (
  SELECT _meta.`path` AS p, _meta.pos AS pos, line,
         MAX(CASE WHEN sev = "ERROR" THEN 1 ELSE 0 END)
           OVER (PARTITION BY _meta.`path` ORDER BY _meta.pos
                 ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS near
  FROM logs) sub
WHERE sub.near = 1;
```

A **JS UDF can't help here.** Scalar UDFs return one value per row; table-valued
`*.stream.js` sources return rows — neither can *emit a `WINDOW` clause*, because a
frame is **syntax**, not a value or a table. The hard thing the user wants sugar for is
precisely the syntax they can't factor into a function.

The answer is a **macro**: user-authored JS that takes a compact invocation and returns
**SQL++ source text**, expanded *before* cbq's parser sees the statement. The detector
author writes:

```sql
SELECT p, pos, line
  FROM @grep_context(logs, when => sev = "ERROR", before => 2, after => 2);
```

and a `grep_context.macro.js` in the `-extensions` dir expands it into the windowed
subquery above. This generalizes: `@top_per_group`, `@sessionize`, `@rate`,
`@pivot` — any recurring shape becomes a one-liner, and the ugly SQL++ lives once, in a
reviewed, golden-tested macro, instead of copy-pasted (and mis-edited) across detectors.

### Why pre-parse text, not an AST rewrite

Three layers could host this; only one fits a *user-authored, `WINDOW`-emitting* macro:

1. **JS UDF (name seam).** Rejected above — can't produce syntax.
2. **cbq AST rewrite** (like the `REWRITE_PHASE1` pass n1k1 already runs for named
   `WINDOW` clauses, `glue/stmt.go:67`). Works only if the invocation is *already valid
   SQL++* that parses to a node we then rewrite — but `@grep_context(...)` in `FROM`
   position is not a table term cbq's grammar accepts, and authoring the transform means
   writing Go AST visitors, not user JS. AST rewrites are the right tool for n1k1's *own*
   built-in desugaring; they are the wrong tool for a drop-in extension surface.
3. **Pre-parse text→text** (this design). The macro is a source-to-source generator that
   runs *before* `n1ql.ParseStatement2`. The invocation syntax is ours to define (it need
   not be valid SQL++), and the transform is ordinary JS returning a string. This is the
   layer the user named ("expanded before cbq's parser is applied"), and it is correct.

**The whole feature is invisible downstream.** Expansion happens at the top of
`ParseStatement` — the single choke point every `.rules` detector *and* every ad-hoc
query flows through (`glue/stmt.go:52`, right before `n1ql.ParseStatement2`). After
expansion the statement is ordinary SQL++, so the planner, CSE/fusion, MQO, ASOF
lowering, and the standalone-analyzer codegen (`IDEA-0018`) all see hand-written-shaped
SQL and neither know nor care a macro produced it. Macros add **zero** complexity to the
B/C engine: pure front-end sugar. (When no `@` appears in the statement — and when no
macros are loaded — expansion is a single `strings.IndexByte` and returns the input
untouched: no cost on the common path.)

### The `@name(...)` invocation sigil

Macro calls are marked with a leading `@`:

```
@macro_name(pos_arg, named => value, other = value)
```

`@` is chosen because n1ql's grammar does **not** use it — named/positional parameters
are `$name` and `?`, so `@` is lexically free and cannot collide with a real query.
(One thing to verify against the fork's lexer before shipping: that a bare `@` is a clean
tokenizer error today, not silently accepted somewhere — the design assumes `@` is
unambiguously "macro, not SQL".) The sigil also makes macros **greppable** and obvious to
a reader: "this line is generated."

The expander scans the statement for `@ident(`, reads a **balanced-paren** argument list
— respecting `'...'` / `"..."` / `` `...` `` string literals and `--` / `/* */` comments
so a paren or `@` inside a string is not miscounted — looks `ident` up in the macro
registry, calls its `expand`, and **substitutes the returned text wrapped in parens**
(safe in both expression and `FROM`-subquery position). An unknown `@name` is a clear
"no such macro" error listing the loaded ones.

**Composition — both directions work, and the evaluation order is nailed down:**

- **A macro call as an argument** to another — `@outer(@inner(x), y)` — expands
  **innermost-first (applicative order)**: `@inner` is fully expanded *before* `@outer`'s
  `expand` runs, so `@outer` receives its arguments as already-expanded SQL++ text (its
  arg inspection / `$lit` coercion sees real SQL, not the string `"@inner(x)"`). This
  matches the function-call mental model.
- **A macro call in a macro's body/output** — `expand()` returns text containing
  `@another(...)` — is picked up by a **re-scan** of that output, so macros can be built
  on top of macros.

Both fall out of one rule: **repeatedly expand the leftmost `@name(...)` whose argument
list contains no further `@`** (always an innermost call), substitute, re-scan, repeat.
Argument nesting strictly *shrinks* the `@` count so it can't loop; only body-emission can
grow it, so a **depth/rounds cap** (e.g. 16) bounds a macro that recursively emits itself,
turning a runaway into a clean error rather than a hang. The **gensym counter is global to
the whole expansion pass**, so inner, outer, and body-introduced expansions all draw
disjoint names — no alias collision however they nest. (A "pass this argument
*un*expanded" escape — an opaque-label macro — is a deliberate v1 omission; applicative
order is the default.)

### The JS contract: `expand(args, ctx) → string`

Mirrors the `*.extract.js` shape (module-scope declaration + one function), and reuses
the same goja lifetime/timeout/`console` plumbing (`glue/ext_jsvm.go`,
`glue/ext_extract_jsvm.go`). `expand` is a **cold-path, once-per-parse** call returning a
string — garbage is fine, exactly like `describe()` returning an `ExtractSpec`.

```js
// grep_context.macro.js
var macro = {
  name: "grep_context",
  // Optional signature — enables arity/keyword checks + `.macro help`, like *.extract.js's match.
  params: [ { name: "src",    required: true },
            { name: "when",   required: true },
            { name: "before", default: 2 },
            { name: "after",  default: 2 } ],
};

function expand(args, ctx) {
  // Hygiene: gensym every macro-introduced binding so two uses (or nesting) never collide.
  var sub  = ctx.gensym("ctx");
  var near = ctx.gensym("near");
  return `(SELECT * FROM (
            SELECT _meta.\`path\` AS p, _meta.pos AS pos, line,
                   MAX(CASE WHEN (${args.when}) THEN 1 ELSE 0 END)
                     OVER (PARTITION BY _meta.\`path\` ORDER BY _meta.pos
                           ROWS BETWEEN ${args.before} PRECEDING
                                    AND ${args.after}  FOLLOWING) AS ${near}
            FROM ${args.src}) ${sub}
          WHERE ${sub}.${near} = 1)`;
}
```

- **`args`** — positional args in `args[0..n]`, named args as `args.<key>`. Each value
  arrives as the **raw SQL++ source substring** of the argument (a macro manipulates
  syntax, so `src` is the identifier text `logs`, and `when` is the *unparsed predicate*
  `sev = "ERROR"` — spliced verbatim). A `literal` best-effort coercion is also offered
  (`args.$lit.before === 2` as a JS number) for the common case of a numeric/quoted
  arg used where a bare literal is wanted. Defaults from `macro.params` fill absent
  keywords.
- **`ctx`** — `{ gensym(prefix) → unique name, error(msg) → throw a mapped error,
  version }`. `gensym` is the hygiene primitive (below).
- **returns** a SQL++ string. A thrown JS error (or `ctx.error`) becomes a parse-time
  diagnostic naming the macro.

### Hygiene: gensym, honestly scoped

Full Scheme-style hygiene needs a binding resolver over an AST; a *text* macro can't have
that. What it **can** guarantee, and what solves the real bugs, is **gensym hygiene**:
`ctx.gensym("ctx")` returns a name unique to this expansion (`ctx__m7`), so a macro's
internal aliases never clash with the user's identifiers, nor with a second use of the
same macro, nor with an outer macro when nested. This is the same discipline as C's
`__COUNTER__` or Rust's pre-hygiene `paste!` — the 90% that matters.

Be honest about the 10% it does **not** buy: because there is no scope tracking at the
text level, a macro *could* reference a user column it never meant to (free-variable
capture in the other direction). The authoring rule that keeps macros safe: **introduce
every internal name via `gensym`, and only reference columns that were passed in as
explicit args.** A future **AST-hygiene tier** — expand into cbq AST nodes and run a
rename pass, reusing the `REWRITE_PHASE` visitor machinery — is the path to true hygiene
if text macros prove too sharp; noted, not built.

### Debuggability is not optional for a code generator

A macro that expands to broken SQL++ must **not** dump a cbq parse error about code the
user never wrote. So:

- **`.macro expand <statement>`** (and a `-explain-macros` flag) prints the
  fully-expanded SQL++ — the primary debugging tool. `.macro list` / `.macro help
  <name>` show loaded macros and their `params` signature, mirroring `.extract help`.
- On a post-expansion parse error, if expansion occurred, the error is **annotated**
  with the responsible macro name and the offending expanded snippet, so the message
  points at the generator, not its output.

### Determinism (keep PREPARE++ plan-caching sound)

`expand` should be **pure**: same args → same SQL++. It runs at parse time, upstream of
plan caching and corpus fusion, so a non-deterministic expansion would poison a cached
plan. Purity is documented as a contract (not enforced); v1 skips memoizing expansions
(parse is cheap next to execution). Note the goja no-`Date.now()` discipline that guards
the *codegen verbatim lane* does **not** apply here — macros run in the ordinary
parse-time runtime, not the gen-compiler copy lane.

### Trust

Same posture as recipes and UDFs: JS runs in sandboxed goja, **opt-in** via `-ext`, no
FS/network unless power-tier host functions are explicitly granted. The one thing to state
plainly: a macro is a **code generator** — its output runs with full query authority. But
this is not a *new* trust boundary, because the `.rules` detectors themselves are already
trusted SQL++ authored by the same person who drops the macro in the extensions dir. A
macro is exactly as trusted as the detector that calls it.

### Registry & loading

Slots straight into the existing seam: `RegisterExtensionFile`/`RegisterExtensionDir`
(`glue/ext.go`) dispatch by suffix — add `.macro.js` next to `.extract.js` / `.stream.js`
/ `.agg.js`. Registration mirrors `RegisterJSExtractRecipe`: compile once with
`goja.Compile`, run once at registration to surface top-level errors and read the
module-scope `macro` object, then store `{name, params, prog, sourceHash}` in a
**load-only** macro registry (like extract recipes; no unload). The expander builds a
throwaway `goja.Runtime` per `expand` call (goja runtimes aren't goroutine-safe;
same pattern as `runJSDescribe`).

### Testing (golden fixtures, like everything else)

Each macro ships a golden fixture: **invocation → expected expanded SQL++** (checked
literally, gensym counters seeded deterministically), plus at least one **end-to-end**
case — invocation → expanded → parsed → run against a tiny keyspace → expected rows — so a
macro that expands to *parseable-but-wrong* SQL is caught, not just a syntax slip. Same
differential/golden discipline as detectors and recipes (`DESIGN-testing.md`), giving an
AI proposing a new macro the same confidence loop.

### Scope: v1 vs later

- **v1 (shipped)** — `@name(...)` scanner (string/comment-aware, balanced parens,
  leftmost-innermost re-scan with a runaway cap); positional + named args (raw text +
  `$lit` coercion) with `params` defaults; `ctx.gensym`/`ctx.error`; `.macro
  list`/`help`/`expand`; parse-error annotation; load-only registry. Starter library in
  `extensions/macros/`: **`grep_context`** (grep `-A`/`-B`/`-C`), **`sessionize`**
  (gap-based episode grouping), **`top_per_group`** (top-N per partition), **`transitions`**
  (field-change edge detection). Each hides a real WINDOW/subquery wall and was validated
  end-to-end, not just by expansion.
- **Later** — AST-hygiene tier (true hygiene via a rename visitor); richer `params`
  typing; `require()`/shared modules once that lands for JS extensions generally; more
  starter macros (`rate`, `time_bucket`, `error_burst`, `collapse_repeats`).

**Engine bug surfaced while building the library (worked around, not yet fixed):**
`SELECT *`/whole-row projection combined with a *no-operand* window function
(`ROW_NUMBER()`/`RANK()`/`COUNT(*) OVER …`) panics in `glue/expr.go` `Convert` (a plain
`^aggregates` binary attachment collides with `^aggregates|key`). Operand functions
(`LAG(x)`, `COUNT(1)`) are fine, so `top_per_group` ranks via `COUNT(1) OVER (… ROWS
UNBOUNDED PRECEDING)` (position-based == `ROW_NUMBER`). The underlying star+no-arg-window
panic is a separate fix.

## Dynamic loading in Go

Can extensions load dynamically — DLLs, `.so`, or pure-Go modules — and what does cgo
cost?

### Go's `plugin` package (`.so`) — a non-starter

`plugin.Open()` loads a `-buildmode=plugin` shared object and `Lookup`s symbols.
Disqualified:

- **Requires cgo.** Built on `dlopen`, so the host must be `CGO_ENABLED=1`; under
  `CGO_ENABLED=0` `plugin.Open` isn't implemented. Enabling cgo forfeits the static
  binary.
- **No Windows.** Linux/FreeBSD/macOS only; no Go equivalent of loading a DLL for *Go*
  code.
- **Brittle.** Plugin and host must match Go toolchain version, all shared dependency
  versions, and build flags exactly — any drift is a load error. Can't unload.

*Once loaded*, calling a plugin symbol is an ordinary Go call — **no per-call cgo cost**
(cgo cost is only at `dlopen`/link time). The problem is incompatibility with a cgo-free,
cross-platform, version-independent binary, not speed.

### cgo cost, in general

- **Pure Go, or the interpreted/Wasm runtimes below:** *zero* cgo.
- **Actual C via cgo:** ~tens of ns/call plus pointer-passing rules. Only if an
  extension is C — which we avoid.
- **Go `plugin`:** forces host cgo (loses the static binary); per-call free.

### Pure-Go, cgo-free ways to load/run extensions (recommended)

| Mechanism | What it is | Cost | Fit |
|---|---|---|---|
| **Compile-time registry** | Go packages built in via `init()`-registration (or build tags). | Native, zero overhead. | Best for a curated set (the doc shredders). Adding one needs a rebuild. |
| **wazero (Wasm)** | WebAssembly via `tetratelabs/wazero` (Apache-2, **pure Go, no cgo**, cross-platform incl. Windows). Guests compiled from Go (`GOOS=wasip1`), Rust, C, AssemblyScript. | Boundary marshaling + slower-than-native; sandboxed. Linear memory *is* an ArrayBuffer → pass bytes with minimal copy. | The modern "load an untrusted binary extension at runtime" answer; true sandbox. |
| **yaegi (Go interpreter)** | `traefik/yaegi` (Apache-2, pure Go, no cgo) interprets Go *source* at runtime. | Interpreted; large Go subset. | "Go in a directory/repo" analog of the JS idea — no build step. |
| **goja (JS)** | Tier-2 above. | Interpreted JS. | Drop-in scripts from a directory/repo. |
| **subprocess / gRPC** | `hashicorp/go-plugin`: extension is a separate process. | IPC serialization per call — heavy for per-row. | Strong isolation / any language; coarse-grained, not hot loops. |

Net: native `.so`/DLL loading is **out** (cgo + platform + version lock-in). Viable
spectrum: compile-time registration (fastest) → yaegi/goja (drop-in source, no build) →
wazero (sandboxed binary) → subprocess (isolation). All cgo-free, all keep the static
binary.

### WASM memory: zero-copy reach and bounded pools

**Zero-copy is nuanced — ~1 copy in, 0 out.** A module has one **linear memory** (a
contiguous byte array = the guest's whole address space). The guest can't hold a pointer
into n1k1's Go heap, so you can't hand it an arbitrary `[]byte`. But the host side is
zero-copy: wazero exposes the guest's linear memory as an aliased Go `[]byte` view
(`api.Memory.Read`). So:

- **Input** lives in linear memory → n1k1 writes row bytes in (one write).
- **Output** read back as an aliased view → no copy.
- **Reuse trick:** make n1k1's reusable row buffer a *fixed window of the guest's linear
  memory* — the per-row marshal becomes the "copy in"; guest reads/writes in place. No
  *extra* copies. Far cheaper than gRPC/subprocess.
- **Caveat:** `memory.grow` can move the backing array, invalidating views — hold only
  across a grow-free call, or re-fetch. Bounded (non-growable) memory keeps them stable.

(Browser `SharedArrayBuffer` shares linear memory across Worker *threads* — a different
axis, N/A to a single-threaded host embedding.)

**Bounded memory is first-class.** A `memory` declares initial + optional **max** pages
(64 KiB each); the runtime refuses `memory.grow` past max, and wazero also caps at
runtime level (`RuntimeConfig.WithMemoryLimitPages`). Set min=max → growth fails. Core
Wasm has **no `malloc`** (allocation is the guest's toolchain), so a guest that allocates
does so only *within* the fixed pool — exhaustion **traps and is contained**. The call
stack is boundable too, so deep recursion traps rather than corrupts. This isolation is
the key advantage over goja (shares the Go heap + GC) and Go plugins (share everything).

## JS extension power tiers

How far the JS surface can grow. Through-line: **goja has no ambient authority** — a
bare runtime can't touch network, filesystem, clock-beyond-Now, or spawn processes
unless *n1k1 injects a host function*. So every capability below is opt-in; the
file-suffix marker + `-ext` flags are where the operator says "yes, this dir may do
that."

### Marking a table-valued / streaming source (`*.source.js`)

We already dispatch by suffix: `*.js` = scalar UDF, `*.agg.js` = aggregate. A streaming
source wants its own marker so the converter routes `FROM name(...)` to the streaming op
rather than guessing from arity. Candidates: **`*.source.js`** (recommended — names the
FROM-clause role), `*.stream.js`, `*.table.js`/`*.tvf.js`. A plain array-returning
`*.js` still works via `expr-scan`. Slots into the same
`extensionLoaders`/`RegisterExtensionFile` dispatch as `.agg.js`.

### Reuse across files: shared scope now, `require()` later

Basic reuse **already works**: every loaded UDF sits in one shared per-query runtime, so
a `_lib.js` of helpers is visible to every file (that's how a UDF calls another today).
Catch: one flat global namespace (two files defining `helper` collide) — but
deterministic and author-controllable: `RegisterExtensionDir` loads in **sorted filename
order** and JS "last top-level definition wins", so `zz_overrides.js` reliably shadows
`base.js` (verified by `TestExtJSDirLoadOrder`). Name files to encode precedence.

For hygienic reuse, add a host-provided `require("./util")` resolving *within the `-ext`
dirs*, evaluating once and returning memoized exports — goja has no built-in loader, but
a CommonJS registry (à la `goja_nodejs`) or goja's ESM `import` both fit. Dir-scoped
`require` also doubles as a safety boundary (can't `require` arbitrary host paths).

### Sync vs async — do we need `async`/`await`?

Mostly no, and that's a feature. n1k1 runs each operator on its own goroutine and is
happy for a producer to **block** (a file scan blocks on `read()`; the pipeline applies
backpressure). So the model is **synchronous host functions**: `http_get(url)` /
`s3_get(bucket,key)` / `run(...)` block the source goroutine, return bytes, and the JS
parses and `emit`s — no event loop.

goja accepts `async`/`await` syntax, but resolving a Promise needs an event loop n1k1
would pump and bridge to synchronous `yield` — real machinery for ergonomic gain.
**Rejected** for now; ship blocking host calls first, offer an `await` lane later only
if demanded. (Fan-out concurrency is better served by a host `http_get_all([urls])`
than an in-goja event loop.) **Today** a Promise return fails cleanly with an
"async/await … not supported … return a plain value" error — a fast, legible failure,
door open to a real `await` lane later.

### The "full-power" operator API (emit + stats + cancel)

A power-tier source is essentially *authoring a native operator in JS*, so give it the
operator's context object:

```js
// metrics.source.js
function metrics(ctx, args) {
  for (const row of ctx.rows(args)) {          // host-provided lazy input
    ctx.stats.inc("rows_in");                   // -> n1k1's -stats footer
    if (ctx.cancelled()) return;                // downstream LIMIT / timeout
    ctx.emit(transform(row));                   // or ctx.emitBatch([...])
  }
}
```

`ctx` bundles `emit`/`emitBatch` (→ the op's yield and Stage batch), `stats` (per-op
counters — DESIGN-stats.md), `cancelled()` (backpressure/timeout/`LIMIT` as a poll), and
`log`. The "I know what I'm doing" tier: more surface, same push/backpressure/spill
contract as native ops.

### Reaching outside: HTTP/S3, and `system()` to allowlisted programs

Since the sandbox grants nothing, "drag data off S3 or HTTPS" is a host function gated by
a capability flag (`-ext-allow-net`, ideally with a host/bucket allowlist to blunt
SSRF/exfiltration). A `shred("s3://…")` / `fetch_ndjson(url)` streaming source pulls bytes
and `emit`s rows as they arrive — bounded memory, backpressure throttling the fetch.

Shelling out is the most powerful/dangerous grant: a host `run(cmd, args, stdin?)`
executing **only programs found in the `-ext` dirs** — the extension dir doubles as the
sanctioned `bin/`. The JS reads the child's stdout as a lazy iterator, transforms, and
`emit`s — wrapping any external tool (`pdftotext`, a Python munger) as a streaming source.
Distinct opt-in (`-ext-allow-exec`): a subprocess runs with the engine's privileges, so
allowlist-by-directory is the containment, not a true sandbox. Streaming from the pipe
keeps memory bounded and lets `LIMIT` early-terminate (close pipe → SIGPIPE child).

### Turtles all the way down: `system()`-ing n1k1 itself

A special case: one allowlisted program is `n1k1` itself. A JS source running
`n1k1 -c "SELECT …" other/dataRoot` and ingesting its JSONL becomes a **federation /
fan-out** primitive — query a second datastore/host from inside the first, map-reduce
across shards, each child a *separate process* (crash- and memory-isolated). Requirements:
a **recursion guard** (depth counter via env, refuse past N — else a query that shells to
n1k1 running itself is a fork bomb), and it composes with streaming (a parent consumes a
child's rows before the child finishes).

## Streaming CTEs / subqueries

Same materialization shape as subqueries. `VisitWith` records each WITH binding; a
non-recursive `FROM cte` is **inlined** as its subquery expression, run through
`expr-scan` — evaluated to a full value, then streamed. Recursive CTEs materialize each
working set per fixpoint iteration. (cbq materializes these too.)

Two improvements, relying on existing yield + spill primitives (new work is in the
converter/planner-bridge, not the runtime):

1. **Single-use CTE → pure pipe.** If referenced once, run its SELECT as a **child
   operator feeding the consumer directly** — no materialization. The subquery's
   `plan.ExpressionScan` carries its sub-plan via `SetSubqueryPlan(selOp)`, so the
   converter converts `selOp` into a child op instead of routing to `expr-scan`.
2. **Multi-use CTE → materialize/spill once, re-scan.** When referenced N>1 times,
   evaluate once into a **spill-backed buffer** (the temp-file machinery ORDER
   BY/join/group use — `base/heap.go` auto-spills) and re-scan per reference. Bounded
   memory, computed once — beyond cbq's always-materialize-in-RAM.

## Scanning a corpus (a directory of documents)

Single-file `shred("report.pdf", …)` doesn't scale to a Drive/Box/SharePoint tree of
thousands of docs, where the *directory* drives and `shred` runs per file. Three
spellings, increasingly n1k1-native:

**A — Composable: a `files()` crawler + per-file `UNNEST shred()` (recommended)**

```sql
SELECT f.path, f.size, d.page, d.text
FROM files("/mnt/drive", {"glob": "**/*.{pdf,docx,xlsx}"}) AS f
UNNEST shred(f, {"want": ["text", "tables"], "pages": "1-20"}) AS d
WHERE f.modified > "2024-01-01"
  AND d.text LIKE "%invoice%"
```

`files()` streams one row per document (path, size, mtime, mime, bytes/handle);
`UNNEST shred(f, …)` runs the extension per file and flattens output into rows. Powerful
because it **separates crawling from parsing**, so cheap file-level predicates filter
*before* the expensive shred. n1k1's `UNNEST` is implemented (`conv.go:VisitUnnest` →
`unnest-inner`, a nested-loop streaming each element correlated to the left), so
`UNNEST <array-returning-fn>` composes today — extension resolution is the only new
piece.

**B — Convenience: a combined directory shredder**

```sql
SELECT d.path, d.page, d.text
FROM shred_dir("/mnt/drive", {"glob": "**/*.pdf", "want": ["text"]}) AS d
```

One function walks *and* shreds, streaming rows tagged with source path. Less
composable, a nice shorthand.

**C — Directory-as-keyspace (most n1k1-native)**

```sql
SELECT meta(f).id AS path, d.page, d.text
FROM `drive` AS f                      -- a keyspace = the directory of docs
UNNEST shred(f, {"want": ["text"]}) AS d
```

n1k1 already treats a subdirectory as a keyspace; here "documents" are file entries
(metadata + bytes accessor) and `META().id` is the path. Slots into the file-source
direction in DESIGN-data.md.

### Why it executes well

- **Parallel, one file per Wasm instance.** A directory is embarrassingly parallel;
  parallel scan fans out, each worker grabbing a pooled bounded Wasm instance for a
  *different* file.
- **Corpus-level streaming, bounded memory.** `files()` streams entries (millions of
  files); only *one document per worker* is resident, and `UNNEST` materializes just that
  file's shred array. Downstream GROUP BY/ORDER BY spill.
- **Predicate/column pushdown.** Form A skips parsing for files failing a cheap
  predicate, and passes `{"want":…, "pages":…}` in so the extension does less work.
- **Metadata-only queries for free.** `SELECT mime, count(*), sum(size) FROM
  files("/mnt/drive") GROUP BY mime` — zero shredding.
- **Incremental / caching.** With per-file content hashes (DESIGN-data.md), a re-run
  skips unchanged files, caching shred output keyed on `hash + config`.

## Namespacing & versioning of extensions

cbq's function names are already **path-structured**, and since n1k1 owns the resolver it
can layer its own scheme.

**Native cbq model (path-based).** The grammar builds names via
`functionsBridge.NewFunctionName([]parts, …)`: 1-part (`pdf_shred`), 2-part global
(`namespace:func`), or 4-part scoped (`namespace:bucket.scope.func`) — `:` separates
namespace, `.` the scope path. (3-part names have an explicit guard.) So `:` already
means *namespace*; a bare `name:version` would clash.

**The flexible path (recommended for a registry).** Since n1k1 supplies its own
resolver, **backtick-quote the name** to hand the parser one literal identifier and let
n1k1's registry interpret it:

```sql
FROM `pdf_shred:v2`(f, {...}) AS d           -- (name=pdf_shred, version=v2)
SELECT `lean:mathlib:bozeman`(42)            -- (org=lean, pkg=mathlib, fn=bozeman)
```

Backticks stop N1QL splitting on `:`/`.`, so the resolver receives the exact string.
Without them, `a:b:c` collides with the namespace/scope grammar.

**Versioning — three levels, in preference order:**

1. **Content-hash pinning (best).** Registry maps `pdf_shred:v2` → a specific Wasm
   module *by hash*; the name is an alias, the hash is truth. Makes pipelines
   reproducible.
2. **Version in the (backtick-quoted) name** — simple, human-readable.
3. **Version in config/args** — `shred(f, {"impl":"pdf_shred","version":"v2"})` — keeps
   the SQL name stable.

So `` `lean:mathlib:bozeman`(42) `` is plausible: backtick-quote → resolver reads
`org:pkg:fn`, fetches the hash-pinned Wasm module, instantiates (bounded, pooled), runs
— namespaced *and* versioned *and* sandboxed.

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

- **Security / sandboxing.** File-reading and JS-executing functions are a real attack
  surface. Gate behind a capability/flag, restrict paths, cap goja's reach (it's
  in-process).
- **Determinism.** Streaming sources and user JS can be non-deterministic and can't be
  cheaply re-read; a re-scan means re-run or spill. Keep DESIGN-testing.md's determinism
  rules in mind if these appear in tests.
- **Fast-path exclusion.** All of these run in the interpreted/boxed lane, not the
  byte-native fast path or compiler codegen.

## Vision: a sandboxed extension registry

The end state the Wasm path enables: an **online registry** (e.g. a PDF/PPTX/DOCX/XLSX
shredder compiled from Go/Rust to Wasm) that a query pulls in and runs *safely* —
isolation enforced by Wasm, not by trusting the code:

```sql
SELECT d.page, d.text
  FROM shred("report.pdf", {"want": ["text", "tables"], "pages": "1-20"}) AS d
```

resolves `shred` to a registry Wasm module, feeds it the raw PDF bytes + config, and
streams back whatever JSON it emits.

**Why it's the sweet spot.** "Raw bytes in → pure transform → stream chunks out, *no
ambient authority*" is the ideal Wasm shape: a shredder needs **zero I/O**, so "safe by
design" is literally true. Precedent: Extism, Shopify Functions, Redpanda Data
Transforms, Envoy/Istio proxy-wasm, Fastly Compute. This is "Extism for a SQL++ engine."

**Architecture (maps onto n1k1's shapes).**

- **Compile once, instantiate per worker.** wazero compiles once (`CompiledModule`,
  shareable across goroutines); each worker gets its **own instance** with its own linear
  memory — mandatory (single-threaded instance) and how n1k1's parallel scans/joins fan
  out.
- **Config prepared once.** A guest `init(config)` export parses "what do we want from
  this PDF" into linear memory once per instance, then many docs flow through.
- **Stream out with free backpressure.** The guest calls a host `emit(ptr,len)` per
  chunk; the host reads it as a zero-copy view and yields into the pipeline. The guest
  runs synchronously, so `emit` blocks until downstream is ready — a 500-page PDF shreds
  without materializing the whole output.
- **Reproducibility bonus.** A no-capabilities transform is deterministic; content-hash
  pinning makes the pipeline reproducible and outputs cacheable.

**Constraints / gotchas (carry forward as requirements).**

1. **DoS ≠ memory safety.** Cap *both* memory (bounded pages) *and* CPU per call
   (`WithCloseOnContextDone` interrupts a runaway guest on context timeout).
2. **Large-input residency.** PDF parsing needs random access (xref is at the end), so
   the whole input is usually resident — size the pool per document or add a host
   `read_range(off,len)` for incremental pull. MB-scale docs are a non-issue.
3. **ETL lane, not a hot loop.** Wasm parsing + marshaling is slower than native; an
   I/O-bound enrichment path, never an inner numeric loop.
4. **Guest toolchain heft.** Standard Go→`wasip1` modules are large and GC-heavy; prefer
   TinyGo or Rust.
5. **Registry hygiene.** An online registry runs third-party code — sign, record
   provenance, pin by version/hash. Payoff: the sandbox caps blast radius at "bad output
   or throttled DoS," never exfiltration or shell.

**Sequencing** (registry last): (1) a local single Wasm source function; (2) instance
pool + `init(config)` + memory/CPU limits; (3) online registry with signing and
hash-pinning. Valuable core is step 2 — `FROM shred('x.pdf', {...})` backed by a pooled,
bounded, capability-free Wasm instance that streams JSON.

## Roadmap (suggested phasing)

0. **DONE — parser-resolution setters + first extensions.** Two tiny fork setters
   (`expression.RegisterFunction`, `algebra.RegisterAggregate` — patch-05/06) open the
   builtin + aggregate registries directly, instead of wiring the full cbq `functions`
   subsystem (storage + metadata + `ParkableContext` + `Language` runner). On top:
   **Tier-2 JS scalar UDFs** (step 2) and the **`sparkline()`/`histogram()` aggregates**
   (native `base.Agg`s). The heavier bridge (step 1) is still the path for
   `CREATE FUNCTION` DDL / metakv catalogs if wanted.
1. **Wire the UDF bridge** (init functions subsystem; implement `VisitExecuteFunction`;
   provide n1k1's resolver + storage). Adds `CREATE FUNCTION` DDL (Tier 3) + a metadata
   catalog; NOT required for the Tier-2 UDFs already shipped via step 0.
2. **DONE — Tier 2 JavaScript + a general extension loader** — "code in a repo":
   `glue.RegisterExtensionDir`/`RegisterExtensionFile` dispatch by extension (`.js`
   today), CLI `-ext`/`.ext`; impl in `glue/ext_jsvm.go`.
3. **DONE — Streaming source-function op** — a generic `stream-fn` op
   (`glue/op_stream_fn.go`, `StreamFnOp`) driven by any `StreamSource`; `*.stream.js`
   JS sources and `RULE_MATCHES` both ride it. `FROM shred(...)`/loaders slot in via the
   same interface; pairs with DESIGN-data.md file-source work.
   - **3b. Extract functions (`*.extract.js`) — the PREPARE++ enabler.** A new
     extension class on the *file→extractor* axis (not the name seam): `match` +
     `describe(file) → ExtractSpec` **DONE** (native execution via `records.SpecApply`,
     JS off the hot path); the imperative `extract(file, meta, emit)` fallback (would
     reuse the step-3 streaming op) is **not yet wired**. A
     read-only single-file `file` host object; a git-repo registry matched by
     ext/regexp; golden-fixture CI. Produces the timestamp-normalization + sortedness
     metadata the **K-way merge join / ASOF** consume (`DESIGN-data.md`
     [sorted sources](DESIGN-data.md), §4/§5). Built-in office/PDF extractors become
     `{framing: whole}` specs under the same seam.
   - **3c. DONE — Macros (`*.macro.js`) — pre-parse SQL++ generators.** A fourth JS
     extension class, on yet another axis: not the name seam, not the file→extractor
     seam, but a **source-to-source** pass at the top of `ParseStatement` (before
     `n1ql.ParseStatement2`). `@name(...)` invocations expand to SQL++ text via a JS
     `expand(args, ctx)`, so users (and AIs) express `WINDOW`-heavy shapes — grep
     `-A`/`-B`/`-C` context, top-per-group, sessionize — as one-liners. gensym hygiene,
     leftmost-innermost (applicative) + body-reemission nesting with a runaway cap,
     `.macro list`/`help`/`expand` + parse-error annotation. **Invisible downstream**
     (planner/CSE/MQO/ASOF/analyzer codegen see only ordinary SQL++), so it adds zero
     B/C-engine complexity. Impl `glue/macro.go` + `glue/ext_macro_jsvm.go`; shipped macro
     `extensions/macros/grep_context.macro.js`. Design [above](#macros).
4. **Native Go builtins** via `expression.RegisterFunction` (patch-05) for doc parsers,
   or expose them as sources per step 3. (`sparkline`/`histogram` already exercise the
   aggregate side via patch-06.)
5. **Streaming CTEs** — single-use pipe first, then multi-use spill-and-rescan.
6. **wazero (Wasm) sandboxed extensions** — for untrusted/binary extensions where goja
   and native builtins don't fit. Add `tetratelabs/wazero` (Apache-2, pure Go, cgo-free,
   keeps `CGO_ENABLED=0`): resolve a Wasm module (from the same directory/repo registry
   as Tier 2), instantiate with a **bounded linear memory** (min=max /
   `WithMemoryLimitPages`) so a runaway guest traps in its own pool. Reuse the step-3
   streaming source-op protocol: place n1k1's recycled row buffer in a fixed window of the
   guest's linear memory (copy-in doubles as the marshal), read outputs back as zero-copy
   `[]byte` views (re-fetch after `memory.grow`). A guest ABI —
   `alloc(n)`/`process(ptr,len)`/an `emit`-style host callback — lets Wasm functions be
   scalar and streaming sources. Compile from Go (`GOOS=wasip1`), Rust, C. Highest
   isolation, at the cost of boundary marshaling + slower-than-native.
