# Extending n1k1 — functions, drop-in extensions, dynamic loading, and table-valued/streaming

## Why

n1k1 runs SQL++ by reusing cbq's parser + planner and evaluating expressions
(natively where ported, otherwise via the embedded cbq evaluator). A natural next
question: how do we grow the engine's *surface* — add builtins, let users drop in
their own functions/extensions (JS or Go), load extensions dynamically, and
support functions that return whole tables (e.g. shred a PDF/PPT/DOC/XLSX into
many JSON rows) — ideally **streaming** so a huge result doesn't have to be
materialized in memory?

Short answer: yes to all, in tiers with different effort/trade-offs. The
enabling plumbing (the UDF resolution seam, `FROM <expr>` scans, a push-based
streaming engine, spill-to-disk) already exists; the work is wiring and one small
new "streaming source function" protocol.

**One hard constraint frames every option below:** n1k1 builds **`CGO_ENABLED=0`**
— a pure-Go static binary (Makefile makes this explicit and every build/test uses
it). That rules out anything needing cgo, most notably Go's own `plugin` package
(see "Dynamic loading in Go"). Everything recommended here stays cgo-free.

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

## Dynamic loading in Go — what's viable (and the cgo question)

Can extensions be loaded dynamically — old-school DLLs, `.so` files, or pure-Go
modules — and what does cgo cost?

### Go's `plugin` package (`.so`) — a non-starter for n1k1

`plugin.Open()` loads a `-buildmode=plugin` shared object and `Lookup`s Go symbols.
It sounds ideal but is disqualified here:

- **Requires cgo.** The `plugin` package is built on `dlopen`, so the *host*
  binary must be `CGO_ENABLED=1`. n1k1 is `CGO_ENABLED=0` by design — enabling cgo
  would forfeit the pure-Go static binary (the whole point). Under
  `CGO_ENABLED=0`, `plugin.Open` isn't even implemented.
- **No Windows.** Supported only on Linux/FreeBSD/macOS — there is no Go
  equivalent of loading a DLL for *Go* code. (You can FFI into a C DLL via
  `golang.org/x/sys/windows` `LoadLibrary`/`GetProcAddress`, but that's the C ABI —
  cgo-style marshaling, not pure-Go extensions.)
- **Brittle even where it works.** Plugin and host must be built with the *exact*
  same Go toolchain version, the same versions of every shared dependency, and
  matching build flags — any drift is a runtime load error. Plugins can't be
  unloaded.

Worth noting on cost: *once loaded*, calling a Go plugin symbol is an ordinary Go
call — there is **no per-call cgo cost** (the cgo cost is only at `dlopen`/link
time, and in the host having cgo enabled at all). So the problem isn't call
speed; it's that the mechanism is fundamentally incompatible with a cgo-free,
cross-platform, version-independent binary.

### cgo cost, in general

- **Pure Go compiled normally, or interpreted/Wasm runtimes below:** *zero* cgo —
  no boundary, native or near-native calls.
- **Calling actual C via cgo:** ~tens of ns of overhead per call, plus the
  pointer-passing rules (can't hand Go-managed pointers to C freely). Only
  relevant if an extension is C — which we're avoiding.
- **Go `plugin`:** forces host cgo (loses the static binary); per-call is free.

### Pure-Go, cgo-free ways to load/run extensions (recommended)

| Mechanism | What it is | Cost | Fit |
|---|---|---|---|
| **Compile-time registry** | Extensions are Go packages built into the binary via an `init()`-registration map (or build tags). | Native speed, zero overhead. | Best for a curated set (e.g. the document shredders). Adding one needs a rebuild. |
| **wazero (Wasm)** | Embed WebAssembly modules via `tetratelabs/wazero` (Apache-2, **pure Go, no cgo**, cross-platform incl. Windows). Extensions compiled to Wasm from Go (`GOOS=wasip1`), Rust, C, AssemblyScript, … | Boundary marshaling + slower-than-native execution; sandboxed. Linear memory *is* an ArrayBuffer → can pass bytes with minimal copying. | The modern "load an untrusted binary extension at runtime" answer; true sandbox. |
| **yaegi (Go interpreter)** | `traefik/yaegi` (Apache-2, pure Go, no cgo) interprets Go *source* at runtime. | Interpreted (slower than native); supports a large Go subset. | The "Go in a directory/repo" analog of the JS-in-a-directory idea — no build step, cross-platform. |
| **goja (JS)** | Tier-2 above. | Interpreted JS. | Drop-in scripts from a directory/repo. |
| **subprocess / gRPC** | e.g. `hashicorp/go-plugin`: extension runs as a separate process. | IPC serialization per call — heavy for per-row work. | Strong isolation / any language; good for coarse-grained, not hot loops. |

Net: for n1k1, **dynamic native `.so`/DLL loading is out** (cgo + platform +
version lock-in). The viable spectrum is compile-time registration (fastest) →
yaegi/goja (drop-in source, no build) → wazero (sandboxed binary extensions) →
subprocess (isolation). All are cgo-free and keep the static-binary property.

### WASM memory: zero-copy reach and bounded pools

Two properties make Wasm especially interesting for n1k1's zero-garbage,
bounded-memory design.

**Zero-copy is nuanced — ~1 copy in, 0 copies out.** A Wasm module has exactly one
**linear memory**: a contiguous byte array that *is* the guest's whole address
space. The guest can only address *its* linear memory — it can't hold a pointer
into n1k1's Go heap, so you cannot hand an extension a pointer to an arbitrary
n1k1 `[]byte` (that's the isolation the "no shared buffer" warnings refer to).
*But* the host side is zero-copy: wazero exposes the guest's linear memory to n1k1
as a Go `[]byte` **view** (`api.Memory.Read` aliases the backing array, not a
copy). So:

- **Input** must live in linear memory → n1k1 writes the row bytes in (one write).
- **Output** is read back as an aliased view → no copy.
- **The reuse trick:** make n1k1's reusable row buffer a *fixed window of the
  guest's linear memory*. The per-row marshal n1k1 does anyway becomes the "copy
  in"; the guest reads in place and writes results into another window that n1k1
  reads in place — no *extra* copies beyond materializing the row somewhere. Far
  cheaper than gRPC/subprocess (which serialize both ways).
- **Caveat:** `memory.grow` can move the backing array, invalidating aliased
  views — hold them only across a grow-free call, or re-fetch. Bounded
  (non-growable) memory keeps the array stable, so views stay valid.

The browser `SharedArrayBuffer` story people recall is a *different axis*: it
shares Wasm linear memory across **Worker threads** (the threads proposal), not
host↔guest copying, and doesn't apply to a single-threaded host embedding.

**Bounded memory is first-class.** A Wasm `memory` declares initial + optional
**max** pages (64 KiB each); the runtime refuses `memory.grow` past the max, and
wazero also caps at the runtime level (`RuntimeConfig.WithMemoryLimitPages`). So
"here is a fixed pool — that's it" is native: set min=max (or the limit) and
growth fails. Core Wasm has **no `malloc`** — allocation is entirely the guest's
toolchain, so a guest compiled with a bump/arena allocator (or none) does no
dynamic allocation at all; a TinyGo/Rust/C guest that *does* allocate does so only
*within* the fixed pool, and exhausting it **traps and is contained** (it can't
touch n1k1's heap). The call/operand stack is runtime-managed and also boundable,
so deep recursion traps rather than corrupts. This isolation — a runaway guest
OOMs/overflows inside its own pool — is the key advantage over goja (in-process,
shares the Go heap + GC) and Go plugins (share everything).

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

### Advanced: can JS participate in n1k1's reusable-slice discipline?

n1k1's zero-garbage design reuses byte buffers (`varLift`, `[]byte` recycled per
row). Could an expert JS author avoid copies and hook into that discipline via
`ArrayBuffer`? Partly — with real limits:

- **`SharedArrayBuffer` isn't the relevant primitive.** It exists for *cross-agent*
  (Web Worker) shared memory. A goja `Runtime` is single-threaded — one instance
  per goroutine, not goroutine-safe — so there's no second agent to share with.
  The primitive that matters is plain **`ArrayBuffer`** + typed-array views
  (`Uint8Array`, `DataView`).
- **Near-zero-copy IN.** goja can back an `ArrayBuffer` with a Go `[]byte`
  (`Runtime.NewArrayBuffer([]byte)`) and hand the JS a `Uint8Array` *view* over
  n1k1's current row buffer — no copy. The JS reads/parses through typed arrays.
- **Near-zero-copy OUT.** The JS writes results into a **preallocated**
  `ArrayBuffer` whose bytes n1k1 reads back (`ArrayBuffer.Bytes()`), instead of
  returning JS objects that goja would marshal (allocate) on the way out.
- **The hard limits.** (1) *Lifetime*: n1k1 recycles the row buffer on the next
  iteration, so a view into it is valid only *within* the callback — the JS must
  consume (or copy out) before the pipeline advances. The push/callback model
  makes that window well-defined. (2) goja still GC-manages all ordinary JS
  values; only the `ArrayBuffer` backing store is under manual control, and typed
  arrays created *inside* JS still allocate. So the discipline holds for the
  buffers you explicitly thread through, not for arbitrary JS. (3) It's an
  expert-only path — most JS functions will just take the ordinary
  marshal-a-value route and pay the copy.

So: yes, a sophisticated author can operate on `ArrayBuffer`-backed views to stay
allocation-light and honor the reuse contract *for the byte buffers they manage*,
valid within the callback window — but it's an opt-in fast lane, not the default,
and `SharedArrayBuffer` specifically doesn't apply to the single-threaded runtime.

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
