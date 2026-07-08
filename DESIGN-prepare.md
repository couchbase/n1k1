# Design: PREPARE — SQL++ → Go, and running the prepared program

Status: proposal (Phase 1 — `.prepare` emit + gate + interpreter fallback — implemented)

n1k1 **compiles** a SQL++ query plan into Go source (the `intermed/` compiler +
`glue/emit.OpToLines`). `PREPARE` exposes that as a **Go-based preparation** of a
statement — the right word, mirroring SQL/N1QL `PREPARE` (and a future `EXECUTE`) —
with an **interpreter fallback** whenever a query needs cbq. The surface is cbq-compatible
`PREPARE [name] AS <stmt>` / `EXECUTE <name> [USING args]`, governed by a `-prepare=<level>`
**ceiling** (default `interpreted` — cache Op trees, no codegen) and inspected via an
`EXPLAIN`-like `.prepare` dot-command. Phase 1 (`.prepare` emit + compilability gate + else
interpret) is implemented (`glue.Prepare`, `cmd/n1k1`). This doc also explores the harder half: how
to *run* the prepared program — the process-separated ("FastCGI-inspired") models where
a prepared child either **asks the parent for data** over a pipe, or **carries the
datastore source itself** and only takes connectivity + auth over the pipe.

For the motivating large-scale use case — compiling a **corpus** of thousands of SQL++
detectors and applying them to support bundles with a single shared scan (multi-query
optimization) — see [DESIGN-detect.md](DESIGN-detect.md) ("PREPARE++").

## Contents

- [Background: what the compiler already does, and the one boundary](#background)
- [The surface: `PREPARE` / `EXECUTE`, and a max-level knob](#the-surface)
- [Preparation is a level, not a yes/no](#levels)
- [What gets emitted](#what-gets-emitted)
- [The one thing the generated code can't do alone: data access](#data-access)
- [Design principle: abstract the datastore leaves behind one interface](#design-principle)
- [Pathways (a ladder)](#pathways)
- [The pipe protocol (concrete)](#the-pipe-protocol)
- [What the parent provides — the "data server"](#the-data-server)
- [Self-contained prepared programs — embed the datastore source](#embed-source)
- [Boxed expressions — the cbq fallback across the boundary](#boxed-exprs)
- [When this pays off — and when not](#motivation)
- [Is codegen worth it? — the crossover](#worth-it)
- [Recommendation / phasing](#phasing)
- [Open questions](#open-questions)

## Background: what the compiler already does, and the one boundary <a name="background"></a>

- **`intermed_build`** generates the `intermed/` package from `engine/*.go` (the
  line-oriented `lz` codegen; see `DESIGN-exprs.md` "Codegen ergonomics").
- The **query compiler** + **`test/emit.OpToLines`** turn a converted `base.Op` tree
  into Go source that builds the operator closures and runs the plan (emitted into
  gitignored `test/tmp` today).
- **Fact 1 — the emitted code's deps are tiny and public.** `engine/` and `base/` are
  decoupled from `couchbase/query` (only *comments* mention it; the runtime code is
  cbq-free, CGO-free). So generated Go imports `engine` + `base` only — no cbq fork,
  no bleve/bbolt/cloud SDKs.
- **Fact 2 — the datastore *leaves* are the one unbaked boundary.** The compiler tests
  skip any query whose tree has a datastore op (`allDatastoreOpsBakeable`,
  `suite_compiler_test.go`; "datastore op not bakeable"). At runtime those leaves
  dispatch through the **overridable `engine.ExecOpEx` hook** (`engine/op.go`), which
  the CLI wires to `glue.DatastoreOp`. The op kinds it serves are exactly the data
  surface: `datastore-scan-records` / `-primary` / `-index` / `-index-cover` / `-fts`
  / `-keys`, `datastore-fetch`, plus `expr-scan`, `js-stream`, `with-recursive`,
  `agg-metadata`, `agg-columnar`. So **the generated Go is "the whole query minus its
  data-access leaves."**
- **Fact 3 — parse + plan already happened in the parent.** Turning SQL++ into the
  `base.Op` tree needs cbq's parser/planner (the private `n1k1-query` fork). By codegen
  time the plan is fixed and baked into the emitted code; the generated program only
  needs **runtime data** (scan rows, fetched docs, index hits), never the planner.

These three facts shape everything below: the compiled query is small + public +
CGO-free, and the *only* thing it can't do by itself is reach a datastore.

## The surface: `PREPARE` / `EXECUTE`, and a max-level knob <a name="the-surface"></a>

Three things, kept distinct: the **statements** (cbq-compatible), the **ceiling knob**
(how far PREPARE may go), and an **inspection** dot-command (show the emitted Go).

**Statements — cbq-compatible `PREPARE` / `EXECUTE`.** Follow N1QL/cbq so the surface is
familiar and existing scripts port:

- **`PREPARE [<name>] AS <stmt>`** runs `parse → plan → convert`, gates on compilability
  ([levels](#levels)), and **caches** the prepared artifact under `<name>` (or a
  plan-derived hash when unnamed). The artifact is whatever level was reached — a cached
  Op tree at the interpreter floor, or a compiled program at a compiled level.
- **`EXECUTE <name> [USING <args>]`** runs a previously-prepared statement, binding `args`
  as positional/named params — the cached Op tree through the interpreter, or the compiled
  program via a run model (below).

Semantics track cbq (naming, `USING` params, re-prepare on cache miss); the only
*difference* is that n1k1's prepared artifact **may be compiled Go**, not just a cached
plan.

**The knob — `-prepare=<level>` is a ceiling, not a switch.** It caps *how far* PREPARE
may go and **defaults to interpreter-only** (`interpreted`): PREPARE caches the converted
Op tree and never invokes codegen. Raising the ceiling opts into compilation up to that
level:

- **`-prepare=interpreted`** (default) — cache Op trees only; EXECUTE always interprets.
  Zero toolchain, zero surprise.
- **`-prepare=data`** — allow up to `PrepareCompiledData` (native exprs; datastore leaves
  served by a runtime data provider).
- **`-prepare=full`** — allow `PrepareCompiledFull` (self-contained emitted Go).

PREPARE produces **the best artifact at or below the ceiling** the query supports, and —
because [preparation is a level](#levels) — silently settles *lower* when a query can't
reach the ceiling (a boxed expr caps it at `interpreted` regardless of the flag). The knob
sets the *maximum* n1k1 will attempt; the query's own compilability sets the *actual*.

**Inspection — `.prepare <stmt>` is `EXPLAIN`-like, orthogonal to the ceiling.** To *see*
the generated Go without a run — for learning or debugging — `.prepare <stmt>` emits the
`*.go` when the query reaches `full`, else prints the reason and runs it interpreted. This
is the Phase-1 surface (implemented); like `EXPLAIN` shows a plan, `.prepare` just *shows*.
Emitting needs **no Go toolchain**; only a compiled `EXECUTE` (ceiling at `data`/`full`)
shells out to `go build` (opt-in, permission-gated).

**No separate `n1k1-prepare` binary is needed to emit, and no import cycle results.** The
CLI already links the whole compiler (glue's `conv`, `glue/emit.OpToLines`, `intermed/`).
The dependency graph is acyclic: `intermed` imports only `base`, `glue/emit` imports
`base`+`intermed`, and `cmd/intermed_build` imports neither — so a clean checkout
bootstraps `intermed_build → intermed → glue/emit → base` with no back-edge. `glue/emit`
does **not** need to move to a top-level package.

## Preparation is a level, not a yes/no <a name="levels"></a>

PREPARE never fails: every statement prepares to at least the **interpreter**, and the
compiled tiers are ordered by *how much runtime support the compiled program still needs*.
So `glue.Preparable(op)` returns a **`PrepareLevel`**, not a bool:

- **`PrepareInterpreted`** — a per-row expression is **boxed** (needs cbq's `Evaluate`;
  see [Boxed expressions](#boxed-exprs)) or the plan didn't convert. PREPARE keeps the
  converted Op tree ready; EXECUTE runs it through the in-process interpreter.
  **All-interpreter always works** — the universal floor, never a failure.
- **`PrepareCompiledData`** — every expression is native, but the plan reads a datastore
  whose op can't be baked into a Go literal. The compiled program needs a **runtime data
  provider**. This is the *widest* compiled level: only native exprs are required, so the
  datastore leaves — and the heavy record providers behind them — can stay parent-side
  (see below).
- **`PrepareCompiledFull`** — native exprs AND every datastore op bakes into the
  emitted Go. A self-contained program (a datastore-free query needs only `engine`+`base`;
  a datastore one links the datastore runtime). Phase-1 `.prepare` emit requires this.

PREPARE = "produce the best executable artifact, and always keep the interpreter Op tree
as the fallback"; EXECUTE runs whatever level was reached. The Phase-1 CLI already reports
it — emit at Full, else print the reason (distinguishing "needs cbq" from "needs a
data provider") and interpret.

**PREPARE runs in the parent — it is inherently cbq-having.** parse → plan → convert use
cbq's parser/planner/`value`; and const-folding a boxed *constant* sub-expr (a cheap way
to lift a query's level — see [Boxed expressions](#boxed-exprs)) uses cbq's
`Evaluate`/`Value`. That's fine — the parent always has cbq. Only the *prepared program*
may or may not, by level.

## What gets emitted <a name="what-gets-emitted"></a>

A Go file exposing roughly:

```go
func Run(vars *base.Vars, yield base.YieldVals, yieldErr base.YieldErr) { /* fused plan */ }
```

— essentially what `OpToLines` produces today, wrapped as a callable. Imports are
`engine` + `base` (+ the datastore-provider interface, below). The projection labels
travel alongside so a caller can interpret the `base.Vals` rows. Datastore leaves
appear as calls to an abstract provider rather than inline file/index I/O.

## The one thing the generated code can't do alone: data access <a name="data-access"></a>

`Run` calls `engine.ExecOp`; a datastore leaf dispatches to `engine.ExecOpEx`.
In-process that's `glue.DatastoreOp` — which pulls glue + records + cbq + bleve +
bbolt + cloud SDKs. So a `*.go` that touches files/indexes needs *something* to serve
those leaves. Who provides it is the whole design space.

## Design principle: abstract the datastore leaves behind one interface <a name="design-principle"></a>

Generalize the process-global `engine.ExecOpEx` into a small **`DatastorePipe`**
interface the generated query calls, e.g. `Scan(spec) → batches`, `Fetch(keys) →
docs`, `IndexScan(spec) → ids`, `Meta(...)`. The *linkage* picks the implementation;
the generated query is identical across them. The codebase already hints this is the
seam — `datastore_scan.go:301` calls the process-global hook "fine for the
single-process CLI; a per-store field is the cleaner future form." An interface makes
the emitted code portable across in-process, child-process, and WASM providers.

## Pathways (a ladder) <a name="pathways"></a>

### 1. Emit-only — the `.go` as artifact / library (cheapest, useful now)

Just write the file. Two immediate uses: (a) **inspection/learning** — see the
compiled plan; (b) **library** — a dev imports `engine`+`base`, supplies a
`DatastorePipe` (e.g. an in-memory one that yields their own `base.Vals`), and calls
`Run`. For inline / in-memory data this needs **zero datastore dependencies**. No Go
toolchain beyond the dev's own build.

### 2. Fat standalone — link glue + records (self-contained, direct datastore access)

`go build` the `.go` + a `main` + glue → a binary that reaches files/indexes directly,
**no parent needed for data**. The naive form pulls the **private `n1k1-query` fork**
(the SAML/SSO build-auth pain) + bleve/cloud SDKs at build time. But *that blocker
dissolves* if n1k1 **embeds its own datastore source** and builds the prepared program
from it, offline — which turns this from "the wrong trade" into a first-class run model.
See [Self-contained prepared programs](#embed-source).

### 3. Thin child + pipe — parent as data server (the recommended novel path)

**Why a child process is idiomatic, not a fallback.** Go has no runtime `eval`, and
`plugin`/`.so` loading is fragile and platform-bound (already rejected in
`DESIGN-extensions.md`). So freshly-compiled query code is *naturally a separate
binary*, and the clean way for the running parent to use it is to spawn it and talk
over pipes. Process separation is how you run generated Go — and it also buys the
isolation/safety the request mentions.

- **The child** = `engine` + `base` + a pipe `DatastorePipe`. Small, **public** deps,
  CGO-free, `go build`-able anywhere — crucially **without the private fork** (parse
  and plan already happened in the parent).
- **The parent** (n1k1) becomes a **data server**: it keeps cbq/glue/records/datastore/
  index/auth and answers Scan/Fetch/Index/Meta requests over the pipe.
- **Flow:** parent spawns child, sends `Run`; the child executes its baked plan and, at
  each datastore leaf, requests data from the parent; the parent streams batches back;
  the child streams result rows up. The **push-based engine maps naturally** — the
  child *drives* the plan, the parent *serves* data, like a compute engine over a
  storage service.

### 4. WASM + wazero — in-process sandbox (alternative separation)

Go compiles to WASM; run the query module in a **wazero** sandbox *inside* the parent,
with datastore leaves as WASM **host-function imports** the parent implements. No
separate process; sandboxed; portable; CGO-free (wazero is pure Go). *Cons:* WASM-Go
is ~2× slower, larger modules, GC quirks. Complements #3 (same "abstract the leaves"
principle, host imports instead of a pipe); `DESIGN-extensions.md`'s wazero note
already points this way.

## The pipe protocol (concrete) <a name="the-pipe-protocol"></a>

**Frame envelope.** Every message is a length-prefixed binary frame with a small
header — `{type: u8, cursor/req id: u32, flags: u8, len: u32}` then payload — over the
child's **stdin/stdout** (stderr = logs). Dependency-free; no gRPC/protobuf (which
would re-add the deps the thin child avoids). A leading **`Hello{protoVer, engineVer}`**
both ways guards parent↔child version skew before anything runs.

**Row framing reuses `base.ValsEncode`/`ValsDecode`.** `base.Val` is already `[]byte`,
so a batch payload is `count` + length-prefixed blobs — the exact encoding the engine
uses for map keys and spill files. Near-zero bespoke serialization for row data.

The message set has explicit room for the four things any real data pipe needs —
**batching, errors, warnings, and stats** — as first-class frame types, not
afterthoughts:

- **Control (parent → child):** `Run{params, namedArgs}`, `Cancel`, and flow-control
  `Credit{cursor, n}` (see backpressure).
- **Data requests (child → parent):** `OpenScan{kind, keyspace, spans, filter,
  projection, limit}`, `IndexScan{index, spans}`, `Fetch{keyspace, keys}`,
  `Meta{keyspace|index}`, `CloseCursor{cursor}`, and — only when a query keeps a boxed
  expression — `EvalExpr{exprText, Vals…}` (see
  [Boxed expressions](#boxed-exprs); a reluctant, batched compute-delegation).
- **Data responses (parent → child):** `Batch{cursor, count, Vals…}`,
  `CursorDone{cursor}`, `MetaResp{…}` — a scan/fetch is a *stream of `Batch` frames*
  terminated by `CursorDone`.
- **Results (child → parent):** `ResultBatch{count, Vals…}`, `ResultDone` — the query's
  output, batched the same way.
- **Errors (either direction):** `Err{origin: parent|child, cursor?, code, severity:
  fatal|retryable, msg}`. A per-`cursor` error fails just that scan; a cursor-less fatal
  ends the query. Kept distinct from warnings so the caller can react to each.
- **Warnings (either direction):** `Warn{cursor?, msg}` — non-fatal advisories (e.g.
  divide-by-zero → null), the same stream the request's warning collector gathers today.
- **Stats (either direction, throttled):** `Stats{snapshot}` — a periodic push of the
  `DESIGN-stats.md` counters (rows in/out, bytes, files pruned, per-op) so the **parent
  can render live progress** for a child-run query. Both sides contribute: the parent
  reports scan/fetch-side I/O, the child reports compute-side rows/ops. Coarse cadence
  (piggyback on batch boundaries or ~10 Hz), off the hot path — matching the two-cadence
  model in `DESIGN-stats.md`.

**Batching + backpressure.** Everything that carries rows is batched (`Batch`,
`ResultBatch`) at a negotiated max batch size; a **credit/window** scheme
(`Credit{cursor, n}`) bounds in-flight batches per cursor so a fast producer can't
flood a slow consumer. This is the engine's data-staging `batchCh` flow-control
projected onto the pipe.

**Re-entrancy is the crux.** A nested-loop join holds an outer cursor open and opens an
inner cursor per outer row; correlated subqueries the same. So frames are **multiplexed
by cursor id** — several scan streams and a fetch may be in flight at once — not a
strict ping-pong.

**Pushdowns must cross the boundary.** The plan's scan op already carries spans /
residual filter / projection / limit; send them so the parent does sargable/covered
index scans instead of streaming whole keyspaces — this is what keeps the split from
being a full-table-shipping disaster.

**Cancel/teardown.** Parent→child `Cancel` (or per-cursor `CloseCursor`); child exit or
either side closing the pipe ends the query and reaps.

## What the parent provides — the "data server" <a name="the-data-server"></a>

Mostly **existing code**. `glue.DatastoreOp` is already a dispatch keyed by `op.Kind`
that yields `base.Vals`; the data server is that same dispatch wrapped in a
request/response loop over the pipe instead of an in-process `ExecOpEx` call. The new
surface is the framing + cursor bookkeeping, not the datastore logic. (This is exactly
the "single-process is a simplification" seam the code already calls out.)

## Self-contained prepared programs — embed the datastore source <a name="embed-source"></a>

The thin-child + pipe model keeps the child minimal by shipping *every* scan/fetch
batch over the pipe — the hardest, deadlock-prone part of the protocol, and pure
per-batch serialization overhead. A better shape for the datastore case: **give the
prepared program the datastore source itself.**

By the time the n1k1 CLI is built, all the datastore code (glue + records + the cbq
runtime it uses) was on the machine — it had to be, to build the CLI. So the CLI can
**`//go:embed` a gzip'd snapshot of that source** as a static blob. When `PREPARE`
finds a query Go-friendly, it extracts the embedded source + a generated `main` +
scaffolding and `go build`s a **self-contained prepared program** that reads datastores
*directly* — the same code as the parent.

**What this buys:**
- **No data-hopping.** The prepared program does its own scans/fetches/index reads; the
  pipe carries only **connectivity + auth at startup** (file paths; Couchbase connstrings
  + creds) and **results / stats / errors** back. The whole multiplexed scan/fetch cursor
  protocol — and its re-entrancy / deadlock risk — disappears.
- **Offline / hermetic build — sidesteps the private fork.** The build-auth pain
  (fetching the private `n1k1-query` fork over SSH/SSO) is a *module-resolution* problem
  at build time. Embedding the source means `PREPARE` compiles against a **local,
  self-contained** tree with local `replace`s — no network, no SSO, no private-fork
  fetch. The source that built the CLI *is* the source the child builds from, so it's
  always in sync.
- **Same engine, compiled.** The prepared program is "n1k1 for this one query," with the
  plan baked as fused Go instead of interpreted.

**Costs / caveats (honest):**
- **Compile time balloons.** Compiling glue + records + the cbq runtime closure per
  prepared query is far slower than the thin child (engine+base only) — tens of seconds
  the first time. Mitigated by the **Go build cache** (the datastore packages compile
  once; only the query `.go` changes run-to-run) and by **PREPARE-once / EXECUTE-many**.
- **The dependency closure is large — and grows with record providers.** `glue.DatastoreOp`
  pulls cbq's `value`, `datastore`, `expression` packages (+ bleve/bbolt for indexes). And
  the **record providers** balloon it further: Parquet drags in Arrow, PDF/office
  extraction pulls its own libraries, etc. — embedding *all* of them into *every* prepared
  program is a lot of blob + compile. The **"tighter, more reusable" refactor** — carving a
  minimal runtime datastore library out of glue/records/cbq (no parser/planner; no
  `expression` at all for Go-friendly queries; ideally only the record providers a given
  query touches) — is the real enabling work, and bounds it. How small it can get is an
  open question.
- **Trust shifts.** The prepared program holds credentials and does I/O — it is *not* the
  sandboxable thin child. It's the parent's own code (not "untrusted"), but the
  process-isolation/safety story is weaker.

**So the thin+pipe model earns its keep after all.** It isn't just the sandboxing
alternative — when a query scans **Parquet** or extracts **PDFs/office docs**, keeping
those heavy providers *parent-side* and shipping only rows over the pipe avoids compiling
(and embedding) the whole provider stack into the child. Two ends of the `DatastorePipe`
abstraction, picked per query:
- **thin child + data-over-pipe** — minimal + fixed child deps regardless of provider
  weight, sandboxable, slower per-batch IPC. Favored for heavy/varied record providers.
- **fat child + embedded source** — direct datastore access, faster, config/auth-only
  pipe, heavier build. Favored for light providers (plain JSON) + throughput.

`PREPARE` picks — or the user flags — based on provider weight, isolation, and throughput.
(This is the run-model choice *within* `PrepareCompiledData`.)

**Fits PREPARE/EXECUTE.** `PREPARE` = compile (+ cache the binary keyed by plan) +
optionally spawn + open datastore connections; `EXECUTE` = run with params, reusing a
**warm** prepared process so the compile *and* the connection setup amortize across many
EXECUTEs.

## Boxed expressions — the cbq fallback across the boundary <a name="boxed-exprs"></a>

n1k1 evaluates a **growing native set** of expressions on bytes; everything else
**delegates to cbq's `Evaluate()`** via the `exprTree` / `exprStr` fallback
(`glue/expr.go`; `DESIGN-exprs.md`). `exprStr` is "parse a cbq expression's *text* →
cbq `Evaluate`." The thin child has **no cbq**, so it cannot evaluate a boxed
expression by itself — the sharpest limit on what can be codegen'd.

**Subtlety: serialize vs evaluate.** The compiler already rewrites a live
`["exprTree", <expression>]` into `["exprStr", "<text>"]` (`stringifyExprTrees`,
`suite_compiler_test.go`) so the plan *serializes* into Go — the boxed expr can be
**carried** in generated code as a string; what it can't do without cbq is **evaluate**
it. (A few exprs don't even stringify → "exprTree not serializable" → not compilable at
all.) So a boxed expr is a **runtime dependency, not a serialization blocker**.

Options, best first:

1. **Gate: the thin child runs fully-native queries only (default).** If any per-row
   expr is boxed, don't target the thin child — run it interpreted in the parent (or use
   the fat child). **Codegen coverage then rides native-expr coverage**: every port in
   `DESIGN-exprs.md` widens the set of compilable queries. Simple and honest, and it's
   already how the compiler tests behave.
2. **Const-fold boxed sub-exprs at codegen time (free).** Many boxed exprs are constant
   / early-bound (e.g. `REPEAT('x', 2)`). The parent evaluates them **once** during
   codegen and bakes the result as a `["json", …]` constant — no cbq in the child, no
   pipe traffic. Removes a real slice of the tail; only *per-row* boxed exprs remain.
3. **`EvalExpr` over the pipe (opt-in, reluctant).** The child carries the boxed
   `exprStr` text, batches the operand `Vals`, and sends `EvalExpr{exprText, Vals…}`; the
   parent evaluates via its existing `ExprStr` (cbq) and returns result `Vals`. So **yes,
   cbq eval can traverse the pipe** — but mind the asymmetry: **datastore requests are
   coarse and batched (many rows per scan), whereas expression eval is per-tuple — the
   hot path.** Putting per-row boxing on a pipe is the *opposite* of what native exprs
   are for — it's the existing slow boxed lane *plus* serialization. Only worth it when
   the boxed expr is a small fraction on an already-filtered stream, and always
   **batched**, never a round-trip per row. Not a general answer.
4. **Fat / embed-source child** — the [self-contained](#embed-source) program links the
   datastore runtime, so cbq is present and boxed exprs evaluate in-child — no pipe eval.
   (But if the goal was a *thin* child, embedding cbq to serve a rare boxed expr defeats
   it — prefer options 1–2.)
5. **Plan partitioning (future).** Rather than ship a boxed *expression* per row, keep
   the boxed *operator(s)* in the parent's interpreter and exchange **batched row
   streams** over the same pipe: the native subtree compiles into the child, the boxed
   subtree stays parent-side. Moves whole operators, not per-row exprs, so it avoids
   hot-path pipe traffic — the cost is choosing the cut (and a projection mixing one
   boxed + several native terms can't split cleanly without duplicating it).

**Bottom line.** The right investment is native-expr coverage (shrink the boxed set);
const-folding mops up the constant tail for free; `EvalExpr`-over-pipe is a reluctant
escape for mostly-native queries; the fat child or plan-partitioning cover the rest. The
pipe is great for *data* (batched, coarse) and poor for *per-row expressions* — which is
exactly why the native lane exists in the first place.

## When this pays off — and when not <a name="motivation"></a>

**Wins:**
- **Performance for hot/repeated queries** — compiled (operator fusion, no interpreter
  dispatch) beats interpreted; amortize the ~seconds of `go build` over many runs.
- **Distribution / embedding** — ship a query as a small artifact (thin child) or a
  library.
- **Safety / isolation** — the thin child or WASM module is sandboxable; the parent
  holds credentials and datastore access.
- **Edge / UDF-like** — push compiled compute toward the data or into a constrained
  runtime.

**Costs / when not:**
- **Compile latency** (~seconds) dwarfs interpreting a one-shot ad-hoc query — keep the
  interpreter as the default; codegen-to-run only wins on **reuse**.
- **Data crosses the boundary** (pipe or WASM) — serialization cost, mitigated by
  batching and Vals-already-bytes, but the **columnar/Arrow** path ships borrowed
  buffers that need copies.
- **Parent↔child protocol/version coupling** to manage.
- **Fat standalone** drags in the private fork + heavy deps — prefer thin+pipe.

## Is codegen worth it? — the crossover <a name="worth-it"></a>

Codegen trades a **fixed compile cost** for a **per-row speedup**, so it only pays past a
break-even. Measured on this machine:

- **Compile cost (warm):** ~0.1 s to `go build` one generated package with deps cached.
  (The *first* build of a runtime — engine+base, or the fat glue+cbq closure — is the
  tens-of-seconds cold tax, paid once and cached.)
- **Per-row speedup:** the compiler benchmark (`make bench-compiler`) shows compiled
  beating interpreted by only **~1.07× (scan/filter/project) to ~1.22× (group-by)** — a
  **~9–11 ns/row** saving. n1k1's interpreter is *already* fast (byte-oriented, native
  exprs, push-based closures), so codegen mostly just inlines the closure calls; the
  boxing/allocation wins were already banked by the native lane, which both paths share.

Crossover for a **one-shot** query ≈ `0.1 s / 10 ns ≈ 10 million rows` (and ~100M–1B if it
triggers the cold first build). So `SELECT 1+1` (one row) is ~7 orders of magnitude short
— compiling it is pure overhead. Where codegen *does* pay:

- **Prepared statements (PREPARE-once / EXECUTE-many)** — amortize the compile over K
  executions: break-even ≈ `10M / K` rows *per execution* (≈10k rows at K=1000). This is
  codegen's real value: hot, repeated queries.
- **Very large one-shot scans** — tens of millions of rows and up.

**So "can compile" ≠ "should compile."** `Preparable` answers *can* (a level); a separate
**worth-it heuristic** answers *should*: `est_rows × executions × ~10 ns/row > compile_cost`.
Absent cardinality, crude signals suffice — datastore-free/constant → never; explicit
`EXECUTE`/reuse → yes; a big keyspace scan → maybe (cardinality would come from
`DESIGN-stats.md`). And note the surface split: **emitting** the `.go` (Phase 1) is cheap
and always fine on an explicit `.prepare` (inspection, like `EXPLAIN`); it's **compiling**
— the seconds — that the worth-it gate guards.

## Recommendation / phasing <a name="phasing"></a>

1. **`.prepare` inspection emit + gate + interpreter fallback — DONE.** The `EXPLAIN`-like
   dot-command emits the `*.go`, no toolchain; a boxed-expr query runs interpreted. Reuses
   `glue/emit.OpToLines` + the existing bakeability/native gate (`glue.Preparable`).
2. **Make datastore scans bakeable + a `DatastorePipe` interface + an in-memory
   provider** — so an emitted query runs standalone over inline `base.Vals` (zero
   datastore deps) and real `FROM` queries stop falling back on the datastore-op gate.
3. **cbq `PREPARE ... AS` / `EXECUTE ... USING` + the `-prepare=<level>` ceiling + a run
   model.** Land the SQL statements (named artifact cache) and the ceiling knob (default
   `interpreted`; `data`/`full` opt into `go build`). Pick a run model per goal:
   - **embed-source (fat child, direct datastore)** — the headline for throughput:
     `//go:embed` a tightened datastore-runtime library, `go build` a self-contained
     prepared program, pipe carries only config/auth + results. Amortize compile +
     connections across `EXECUTE`s.
   - **thin child + data-over-pipe** — for sandboxing / minimal deps, with the parent's
     existing `glue.DatastoreOp` as the data server.
4. *(optional)* **WASM/wazero** — an in-process sandboxed alternative to the thin child.

## Open questions <a name="open-questions"></a>

- **Embed-runtime size** — how small can the "tighter, more reusable" datastore-runtime
  library be carved from glue/records/cbq (drop parser/planner; drop `expression`
  entirely for Go-friendly queries)? That bounds both the embedded blob and the
  per-prepare compile time, and decides whether embed-source is practical.
- **Multiplex vs nested request/response** for cursors — which fits the push engine
  with the least deadlock risk under nested-loop joins and correlated subqueries?
- **Framing:** reuse `ValsEncode` verbatim, or a versioned envelope carrying types/
  labels/warnings/stats too?
- **Columnar across the boundary** — how to ship Arrow buffers (copy vs shared memory /
  mmap) without losing the vectorized win.
- **What bakes into the child vs stays parent-side** — `with-recursive` drives
  sub-scans, `expr-scan`/CTE, `agg-metadata`/`agg-columnar` read footers; some are
  compute (bake) and some are data (serve). Draw the line.
- **Toolchain policy** — when is `go build` permitted (sandbox/permission), and how to
  pin the `engine`/`base` module version the child builds against (a prebuilt "thin
  runtime" module would make child builds fast + hermetic)?
- **Stats aggregation** — the `Stats` frame carries `DESIGN-stats.md` counters both
  ways; open question is how to *merge* parent-side (scan/fetch I/O) and child-side
  (compute) snapshots into one coherent progress view without double-counting.
