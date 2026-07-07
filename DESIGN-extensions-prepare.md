# Design: SQL++ → Go codegen, and running the generated code

Status: proposal / for discussion

n1k1 already **compiles** a SQL++ query plan into Go source (the `intermed/`
compiler + `test/emit.OpToLines`); today that only feeds the differential compiler
tests. This doc explores *exposing* that — a `CODEGEN` statement prefix / `.codegen`
dot-command / `-codegen` flag, mirroring `EXPLAIN` — to emit a `*.go` file for a
query, and then the harder question: how to *run* that generated code, including the
process-separated ("FastCGI-inspired") model where the compiled query is a thin
compute child that asks the parent n1k1 for data.

## Contents

- [Background: what the compiler already does, and the one boundary](#background)
- [The surface: `CODEGEN` / `.codegen` / `-codegen`](#the-surface)
- [What gets emitted](#what-gets-emitted)
- [The one thing the generated code can't do alone: data access](#data-access)
- [Design principle: abstract the datastore leaves behind one interface](#design-principle)
- [Pathways (a ladder)](#pathways)
- [The pipe protocol (concrete)](#the-pipe-protocol)
- [What the parent provides — the "data server"](#the-data-server)
- [Boxed expressions — the cbq fallback across the boundary](#boxed-exprs)
- [When this pays off — and when not](#motivation)
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

## The surface: `CODEGEN` / `.codegen` / `-codegen` <a name="the-surface"></a>

Model it on `EXPLAIN`. `CODEGEN <stmt>` (or `.codegen`, or a `-codegen` flag) runs
`parse → plan → convert → emit` and writes/prints a `*.go` for the statement.

**No separate `n1k1-codegen` binary is needed to *emit*.** The CLI already links the
whole compiler (glue's `conv`, `emit.OpToLines`, `intermed/`). A standalone
`n1k1-codegen` tool would just be the same code with a thinner main — worth having
only as a headless/CI build step, not as a dependency the CLI must "find." Emitting a
`*.go` needs **no Go toolchain**; only *compiling* it does (external `go`, opt-in and
permission-gated).

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

### 2. Fat standalone — link glue + records (self-contained, heavy)

`go build` the `.go` + a `main` + glue → a binary that reaches files/indexes directly.
*Pro:* no parent. *Cons:* it pulls the **private `n1k1-query` fork** (the SAML/SSO
build-auth pain), plus bleve/cloud SDKs — a large binary per query, CGO-free but
heavy. Usually the wrong trade; listed for completeness.

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
4. **Fat child** — link glue → cbq present → no pipe eval. Self-contained but heavy +
   private-fork (see the ladder).
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

## Recommendation / phasing <a name="phasing"></a>

1. **`CODEGEN` / `.codegen` emit-only** — write the `*.go`, no toolchain. Immediately
   useful (inspection; library over in-memory data). Reuses `OpToLines`.
2. **A `DatastorePipe` interface + an in-memory provider** — so an emitted query runs
   standalone over inline `base.Vals` with zero datastore deps; enables `-codegen -run`
   for datastore-free queries.
3. **Thin child + pipe protocol**, with the parent's existing `glue.DatastoreOp` as the
   data server. The headline architecture.
4. *(optional)* **WASM/wazero** as an in-process sandboxed alternative to #3.
- **Defer/avoid:** the fat standalone, unless a concrete need appears.

## Open questions <a name="open-questions"></a>

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
