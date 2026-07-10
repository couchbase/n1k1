# Design: PREPARE — SQL++ → Go, and running the prepared program

Status: proposal.
**PREPARE (first half)** — phases 1–3 implemented: `.prepare` emit + gate + interpreter
fallback; datastore `DatastorePipe`/MemPipe; cbq `PREPARE`/`EXECUTE` statements + `-prepare`
ceiling + compiled `EXECUTE` end-to-end via the thin-child + data-over-pipe run model.
Remaining: embed-source fat child, the full cursor pipe protocol, WASM.
**PREPARE++ (second half)** — the temporal optimizations (phase 5: ASOF / K-way merge) and the
multi-query-optimization engine substrate (phases 3–4: shared-scan `broadcast`, source routing,
corpus CSE, predicate index) are **implemented and benchmarked**, and the **corpus compiler
(phase 6) that feeds them from SQL++ detectors is built (MVP)** — `Session.CorpusCompile` fuses
single-source detectors into the shared-scan plan, runs correlated/complex ones (ASOF, window,
GROUP BY, join) standalone via their own optimized plans, and rejects only the broken. What
remains: the phase-6 tail (SHA-keyed build cache, embed-source analyzer binary, per-detector
projection envelope), the logical-keyspace / late-binding resolver (phases 1–2, partly enabled by
the extract-recipe data layer in `DESIGN-data.md`), and the recipe-format + golden-fixture CI
(phase 7). Native-expr coverage (the codegen lever that lets a detector compile to
`PrepareCompiledFull`) is ongoing (`DESIGN-exprs.md`).

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

And it has a **driving use case** beyond one-off prepared queries: compiling a **corpus** of
thousands of SQL++ detectors and applying them to support bundles with a single shared scan
(multi-query optimization) — **PREPARE++**, the second half of this doc
([the use case](#prepare-plus-plus)).

## Key design decisions (build log, 2026-07)

Non-obvious calls made while building the PREPARE++ substrate (the *what's built* is in the
status header + [phasing](#detect-phasing); this records *why*):

- **MQO is interpreter-first; codegen is optional on top.** The `broadcast`/route/CSE/index
  stack runs over expr-trees natively (zero-boxing), so MQO needs no `go build`; the compiled
  tier only adds fusion. Corollary: "weaving detectors into one program" is just **inlining**
  ([compile-corpus note](#compile-corpus)) — separate compilation *units* matter for build-cache
  economics + CSE, not for combining.
- **Predicate index = Aho-Corasick over raw row bytes, not `regexp`.** Each detector's cheapest
  *necessary* literal is extracted; one AC pass wakes only detectors whose literal is present.
  Chosen over a single RE2 alternation because we need per-pattern *presence incl. overlaps/
  nesting* (RE2's non-overlapping leftmost matches would UNDER-report → under-wake → missed
  detections), plus zero-alloc/row and K-independence over a pure-literal set. Invariant:
  over-wake is safe, under-wake is a bug.
- **ASOF / temporal / complex detectors run standalone, not fused.** The single-source broadcast
  fans filter+project detectors; a correlated ASOF (or window / group / join) detector runs via
  its OWN already-optimized plan (the merge-join `WireASOFJoin` lowers) with findings unioned in.
  The corpus compiler classifies each detector **fuse / standalone / reject** (reject = surfaced,
  never silently dropped).
- **Fusion is keyed by LOGICAL keyspace identity.** A bound keyspace reports its logical name, so
  binding (file-drift resolution) and shared-scan fusion compose: all `FROM indexer_log` detectors
  share one scan regardless of the physical files behind it this bundle.
- **Field-shape drift is a CORPUS concern, not a normalization adapter** ([why](#late-binding)):
  a bundle carries several co-deployed software versions, so version-aware / version-tolerant
  detectors (evolving in the git corpus) beat a per-record normalizer.
- **MVP evidence = the whole matched row** (per-detector SELECT-projection envelope deferred); a
  detector's essence is its predicate, the finding is the matching evidence row.

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
- [Public deps vs. the fork — native queries keep the child cbq-free](#fork-free)
- [Boxed expressions — the cbq fallback across the boundary](#boxed-exprs)
- [When this pays off — and when not](#motivation)
- [Is codegen worth it? — the crossover](#worth-it)
- [Recommendation / phasing](#phasing)
- **PREPARE++ — the detector-corpus use case:**
  - [The driving use case: a detector corpus over support bundles](#prepare-plus-plus)
  - [Shared scan / multi-query optimization](#mqo)
  - [Detectors stay in stock SQL++ — no grammar changes](#stock-sqlpp)
  - [Compiling & running the corpus](#compile-corpus)
  - [Late binding: a prepared corpus over a new, differently-named bundle](#late-binding)
  - [Detector authoring & ops — the AI-agent affordances](#authoring-ops)
  - [PREPARE++ phasing](#detect-phasing)
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

## Public deps vs. the fork — native queries keep the child cbq-free <a name="fork-free"></a>

Every run model turns on ONE dependency question: does the prepared program need cbq (the
`n1k1-query` fork) at all? The design stance is to make the answer *no* wherever possible —
not because the fork is unreachable (it may be a public repo), but because a child that
avoids it stays **small and hermetic**: no version-pinning against a large build graph, no
`go get` of cbq's transitive deps per prepared program, and a smaller embed blob. The line:

- **Parse + plan already happened in the parent.** The child never needs the
  parser/planner — only *runtime* support — so the fork's larger half is never a child
  dependency regardless (Fact 3, [Background](#background)).
- **A fully-native query needs NO cbq in the child.** Its exprs compile to inline
  `engine`+`base` code; its datastore leaves reach a record provider. Both `engine`/`base`
  and the record providers (file, and the format libraries — Parquet/Arrow, PDF/office) are
  **public** Go packages, so such a child links public deps only.
- **Only a boxed expression pulls cbq in** (per-row `Evaluate`; [Boxed
  expressions](#boxed-exprs)). So **native-expr coverage is the same investment as a
  fork-free child**: every expression moved to the native byte lane keeps the compiled
  program off the fork. Box-avoidance and compiled-throughput are one effort, not two.

**Data-locality and cbq-presence are separable axes.** Avoiding data-shipping does NOT
force embedding cbq. A fat child can read its data directly (no shipping) AND still delegate
the occasional boxed expression back to the parent over a thin control pipe (`EvalExpr` —
the parent always has cbq): the GBs stay local, only the rare per-row boxed batch crosses.
Embed cbq into the child only when boxed exprs are common AND full child autonomy is wanted
— the exception, not the default.

**Provider selection.** Don't compile every record provider into every program:
Arrow/Parquet/PDF are heavy. Include only the providers a query's `FROM` touches — which
needs the record layer factored so providers are separable (the "minimal runtime library"
carve-out; [open questions](#open-questions)). This bounds both blob size and compile time.

**Embed vs. fetch, and the one ask.** For parts we control (the runtime library, and cbq if
ever truly needed), prefer `//go:embed` — hermetic (no network, no auth, always in-sync with
the CLI that built it). For the public format libraries, `go get` is fine. Either way a
**local Go toolchain is the single ask** — reasonable for our own support engineers ("have
Go installed"), and best surfaced as an env-doctor / prep step that detects the toolchain,
warms the build cache (the heavy providers compile once), and prints the exact fix for
anything missing, rather than a wall of copy-paste.

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
2. **Const-fold boxed sub-exprs at codegen time (free) — IMPLEMENTED.** Many boxed exprs
   are constant / early-bound (e.g. `REPEAT('x', 2)`, `"a" LIKE "%b%"`, `DATE_FORMAT_STR`
   over literals). `glue.exprConstFold` (the fallback in `ExprTreeOptimize`) evaluates any
   row-independent, non-volatile expression **once** during codegen and bakes the result as
   a `["json", …]` constant — no cbq in the child, no pipe traffic. Because the optimizer
   recurses, a constant *subtree* folds wherever it appears, lifting an otherwise-boxed
   enclosing expr to native (e.g. the `LENGTH(SUFFIXES("abc"))` in `b.i + LENGTH(…)`).
   Native handlers are tried first (a constant `GREATEST(1,2)` keeps its tested handler),
   and the fold value comes from `Evaluate()` — not cbq's static `Value()`, which disagrees
   with runtime eval for some functions (`GREATEST(9,null).Value()==null` vs `Evaluate()==9`)
   — with non-finite `NaN`/`Inf` results left boxed (JSON can't represent them). Removes a
   real slice of the tail; only *per-row* boxed exprs remain.
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
   model — statements, ceiling, and the thin-child run model DONE (end-to-end).** The SQL
   statements (named artifact cache) and the ceiling knob (default `interpreted`;
   `data`/`full` opt into `go build`) are landed, and compiled `EXECUTE` runs end-to-end via
   the thin child + data-over-pipe model (below): `glue.executeCompiled` lowers exprs
   natively (`ExprTreesOptimize`, so field-access / arithmetic / nary / const-folded
   projections compile, not just `SELECT *`), `go build`s a cbq-free child (engine+base),
   ships the scanned records over its stdin, and the child streams **positional `base.Vals`**
   back (`ValsEncode` frames); the parent assembles each into the row JSON with its existing
   `ConvertVals`, so multi-column / nested projections come back correctly. A genuinely
   per-row boxed expr degrades to the interpreter. (`TestExecuteCompiledFull`; CLI:
   `n1k1 -prepare=full -c 'PREPARE p AS ...; EXECUTE p'`.) Remaining run-model work:
   - **embed-source (fat child, direct datastore)** — the headline for throughput:
     `//go:embed` a tightened datastore-runtime library, `go build` a self-contained
     prepared program, pipe carries only config/auth + results. Amortize compile +
     connections across `EXECUTE`s. (The thin child ships every record over the pipe; this
     avoids that for light providers — not yet built.)
   - **thin child + data-over-pipe — DONE (above).** For sandboxing / minimal deps, with
     the parent's existing `glue.DatastoreOp` as the data server. (Records are shipped over
     stdin as a simplified frame today; the full multiplexed cursor protocol with pushdowns
     — §"The pipe protocol" — is future work.)
4. *(optional)* **WASM/wazero** — an in-process sandboxed alternative to the thin child.

# PREPARE++ — the detector-corpus use case

## The driving use case: a detector corpus over support bundles <a name="prepare-plus-plus"></a>

Support engineers receive **support bundles**: big `*.zip`s a cluster-management tool
gathers on a customer's site — subtrees of log files, JSON, config, stat dumps, mixed
formats. Years of tickets (e.g. `ET-12345`) recur across customers and clusters. The vision:
a **growing, git-maintained repository of SQL++ "detectors"** — filters / scans /
correlations that report *"this bundle shows evidence of ET-12345 and ET-111222"* — and an
engine that applies **thousands** of them to an incoming bundle **without** scanning it
thousands of times.

This is why PREPARE is worth building: it hits **both** of codegen's payoff regimes at once
([Is codegen worth it?](#worth-it)). The corpus is compiled **once** and run against **every**
incoming bundle (PREPARE-once / run-many), and each bundle is a **large scan** (GBs of logs).
The example that looked absurd for the compiler — `SELECT 1+1 → emit Go` — inverts here: a
detector corpus is the compiler's reason to exist.

- **Input:** a `*.zip` = a tree of heterogeneous files. n1k1's record providers already read
  these formats (`DESIGN-data.md`); a **zip datastore** presents the archive as keyspaces
  (`<subdir>/<file>` → keyspace), decompressed on the fly like the existing `.gz` path.
- **Corpus:** thousands of **detectors**, each = a SQL++ query + metadata (target ticket,
  target sources, severity) + a golden fixture (below). Maintained in git.
- **Output:** per bundle, a ranked **findings** table — which tickets the bundle shows
  evidence for, with the evidence: `{ticket, confidence, source_file, line_range,
  evidence_rows, summary}`. `UNION ALL` across detectors → one table, ordinary SELECT
  projection over matched rows; de-dup / rank is `GROUP BY` / `ORDER BY`.

## Shared scan / multi-query optimization <a name="mqo"></a>

Push-based execution is the right substrate. A scan already *pushes* each row (a `base.Val`
= `[]byte`) into a yield function; multi-query = make that yield a **fan-out (tee)** into K
detector pipelines. Native exprs read the shared bytes with **zero boxing**, so a row is
decoded once and every detector evaluates against the same buffer.

The four levers below are **implemented** as engine primitives + pure build-helpers over
hand-built detectors (a `Detector` = `{tag, predicate expr-tree, projection expr-trees}`),
each with a committed benchmark; they are orthogonal and compose (route partitions by source →
CSE hoists shared terms → the index wakes a handful of the survivors per row). The corpus
compiler ([below](#compile-corpus), `glue.CorpusCompile`) now produces `Detector`s from SQL++
statements and wires the levers together, so these helpers can be driven from a real detector set
(or from hand-built params, as the benchmarks do).

- **MVP — broadcast op — DONE (`engine.OpBroadcast`, kind `broadcast`).** One `broadcast`/`tee`
  operator: scan once, fan each shared-byte row to K detectors (each an inlined filter+project),
  yielding tag-stamped findings up one stream. Beats N separate runs by decoding each row once
  (`TestOpBroadcastScansOnce`). Measured vs K separate scan+filter+project runs: modest time win
  on cheap in-memory rows (~1.1–1.18×, larger as the per-row scan gets heavier — gzip/multiline
  extract) but up to **6.3× fewer allocations** at K=256. `BenchmarkBroadcastScaling` confirmed
  the design's premise directly: broadcast removes the redundant scans but is still **O(K × rows)**
  in predicate work — the bottleneck is per-row predicate work, not I/O — which is what the next
  three levers attack.
  - **Source routing (cheap, big) — DONE (`engine.BroadcastRoute`).** A detector's target source
    is declared (inferred from its `FROM` by the future corpus compiler); a source's scan only
    fans out to detectors that target it (pure composition: one `broadcast` per source under a
    `union-all`; orphan detectors whose source is absent are pruned and RETURNED, never silently
    dropped). ~**M× less** per-row predicate work for M sources (measured ~3.4× at M=4).
  - **Corpus CSE — DONE (`engine.BroadcastCSE`).** Sub-predicates shared across detectors
    (`level="ERROR"`, a `regexp_contains(line,"panic")`) are computed **once per row** via a
    precompute `project` inserted below the broadcast (whole-row passthrough + one `^cseN` column
    per shared term; detectors rewritten to read the slot). Pure composition, byte-identical
    findings. Measured **~2.5×** at K=32 sharing one regexp; the win grows with K and the shared
    term's cost. (Expr-identity via canonical `json.Marshal` of the sub-tree — the same
    stringify the [boxed-expr path](#boxed-exprs) relies on.)
  - **Predicate index (the scale trick) — DONE (`engine.OpBroadcastIndexed`, kind
    `broadcast-indexed`; `base.AhoCorasick`).** Detectors are indexed by a **necessary**
    discriminating literal extracted from their predicate (`contains`/`eq`/plain-literal
    `regexp_*`/first `and` conjunct → a required substring; anything unprovable →
    "always-wake"). One **Aho-Corasick** pass over the raw row bytes per row wakes only the
    detectors whose literal is present; only those full predicates run. The correctness
    invariant: the literal must be NECESSARY (absent ⇒ predicate false), so over-waking is safe
    and under-waking never happens — guarded by a byte-identical differential test. Turns
    O(K × rows) into ~**O(hits × rows)**: measured evals/row = `(matches + always-wake)`, not K,
    and **~60× faster at K=1000** (roughly flat in K while broadcast is linear). An equality/range
    index over parsed fields (vs whole-row substring) is a future refinement.

**Shared scans are keyed by the LOGICAL keyspace.** The fan-out is "one scan per logical
keyspace → the K detectors that target it," decided at prepare time over a stable keyspace
vocabulary — so it is [bind-invariant](#late-binding). A new bundle resolves each logical
keyspace to physical file(s) *underneath* that key (one logical keyspace may bind to several
rotated files, concatenated, or to a glob); the fan-out, source routing, CSE and predicate
index are all logical and untouched. **Compile the MQO structure once; rebind the leaves per
bundle.**

**Growing the corpus without recompiling the world.** As detectors accrue, the cost is
per-row predicate work (K × rows), not I/O — so the predicate index is the load-bearing
piece: adding a detector is *insert one rule into the index + compile one predicate*, not
rebuild the corpus. Shard by source/subsystem so a new rule recompiles only its shard (with
the index local to it), and content-address each compiled shard by the recipe repo's git
tree SHA so unchanged shards are cache hits ([Compiling & running](#compile-corpus)). One
fused program maximizes CSE; sharded programs bound recompile scope — the
[granularity tradeoff](#open-questions) stays open.

## Detectors stay in stock SQL++ — no grammar changes <a name="stock-sqlpp"></a>

**Hard constraint: don't touch the dialect.** n1k1 parses via the private `n1k1-query`
fork; adding SQL++ syntax (a new `ASOF` keyword, a bespoke clause) means editing the fork's
grammar/lexer — fork divergence and perpetual maintenance. So **detectors use only stock
SQL++ the existing parser already accepts**, and AI agents author in a dialect any SQL++
tooling understands. New capability lands **not** as syntax but as three grammar-free forms:

1. **Engine/planner optimizations over stock idioms.** The temporal join support engineers
   want — **ASOF** (join each row to the nearest-preceding row of another stream by time) —
   is already expressible in stock SQL++ as a correlated "argmax" subquery:

   ```sql
   SELECT e.*, (SELECT r.state FROM rebalance r
                WHERE r.ts <= e.ts ORDER BY r.ts DESC LIMIT 1) AS state_at
   FROM errors e
   ```

   That parses today (correlated subquery + `ORDER BY` + `LIMIT 1`). ASOF is then an
   **execution optimization**, not a language feature: the planner recognizes the
   nearest-preceding pattern and runs it as an `O(n)` **merge** over time-sorted streams
   (which fit push-based execution, and logs are usually near-sorted / cheaply spill-sorted)
   instead of the `O(n²)` naive correlate. The user writes stock SQL; the speedup is
   transparent.
2. **Window functions — already stock.** Rate / burst / streak / gap detection ride on
   standard `... OVER (PARTITION BY … ORDER BY ts …)` (`DESIGN.md`): "N errors within 10s",
   inter-arrival gaps, streak length. No new syntax.
3. **Scalar extensions / UDFs.** Detector-specific parsing/matching (grok-style log-line
   extraction, a normalizer) lands as **scalar functions** — a native expr (widening
   `DESIGN-exprs.md` coverage) or a **JS UDF via `-ext`** (`DESIGN-cli.md`) — invoked with
   ordinary `func(args)` call syntax the parser already accepts.

The one gap: n1k1's extension mechanism today gives **scalar** UDFs, not **table-valued**
(set-returning) functions, and a TVF-in-`FROM` *would* need parser support — so prefer the
subquery / `UNNEST` / self-join idioms above over inventing a `FROM asof_join(…)` form. See
[open questions](#open-questions).

**Payoff for codegen coverage.** Regex / complex-string / time exprs currently **box** (fall
back to cbq — [boxed expressions](#boxed-exprs)), which caps a detector at `interpreted` and
blocks fusing it into the corpus. So this use case is the sharpest motivation for the
[`DESIGN-exprs.md`](DESIGN-exprs.md) native-coverage work: every **string / regex / time**
expr ported to the native byte lane widens what the detector corpus can compile to
`PrepareCompiledFull`. That coupling — not grammar — is the real lever.

## Compiling & running the corpus <a name="compile-corpus"></a>

PREPARE++ is [PREPARE](#the-surface) applied to a **repository**, not a statement: compile
the corpus into one (or a few) fused programs with the shared-scan fan-out, corpus CSE, and
predicate index baked in. The MVP `glue.CorpusCompile` does this at the **plan** level today
(fuse single-source detectors, run correlated/complex ones standalone, reject the broken — see
[phase 6](#detect-phasing)); the SHA-keyed build cache and the embed-source *binary* are the
codegen/packaging tail on top of that.

**On emitting "parts" — weaving is inlining; packages are for build economics.** A natural
question is whether the codegen must emit each detector (or its filter/projection) as a
separable library/package that a larger MQO program weaves together. Two layers of answer:
(1) *For correctness/weaving, no.* The MQO substrate above already runs interpreted (the
`broadcast`/index ops take detector expr-trees and evaluate them natively) — MQO needs no
codegen at all; the decode-once + zero-boxing win is the shared native lane's, which interp
and compiler share. And *if* compiled, `emit.OpToLines` already fuses a whole op tree by
inlining, so a compiled broadcast is just one scan loop with K inlined `if pred_k { emit
proj_k }` blocks — siblings in one function; you don't need separate packages to *combine*
them. (2) *For corpus scale, yes — but driven by build-cache economics, not composition.* Go
has no runtime dynamic linking (`plugin` is rejected, `DESIGN-extensions.md`), so "parts" means
separate **compilation units**: emit each detector (or shard) behind a uniform sink signature —
essentially `func(vars, row, emit)` (the push-based dual of a scan; the MVP's `(predicate,
projection)` pair is its inlined degenerate case) — so `go build`'s package cache recompiles
only the changed unit (SHA-keyed, below) and CSE fragments are emitted once and imported by
many. So the compiler's shape is *a shared scan/broadcast harness + one cached unit per detector
or shard + shared CSE fragments*, not a "can we combine them" problem.

- **Where it runs — embed-source, and that's now the easy target.** The
  [self-contained prepared program](#embed-source) ships a **support-bundle analyzer binary**
  with **no `n1k1-query` fork** (parse/plan happened at corpus-build time), runnable in the
  support pipeline or on-site; the zip is a datastore behind the
  [`DatastorePipe`](#design-principle). Its one cost — needing a **local Go toolchain to
  build** — is an acceptable ask for our own engineers as users ("have Go installed"), which
  de-risks the whole embed-source path.
- **Git-awareness.** Detectors are versioned artifacts; lean in.
  - **Provenance:** a finding cites the exact rule — `ET-12345 detected by
    recipes/indexer/panic.sql++@<sha>`.
  - **Build cache keyed by tree SHA:** the compiled corpus is content-addressed by the recipe
    repo's git tree SHA; only changed detectors recompile — the same content-addressed cache
    sketched for [embed-source](#embed-source), now bounding the "thousands of detectors"
    compile cost.
  - **Reproducibility:** re-run an old ticket's analysis with the recipe versions current then.
- **AI-authored recipes need a test harness first.** If agents write thousands of detectors,
  the **recipe format must be testable**: each recipe = SQL++ + metadata + a **golden fixture**
  (a tiny sample bundle fragment + the expected finding). **CI runs the whole corpus** against
  a labelled bundle library on every change — what keeps false-positives bounded and lets an
  agent propose a recipe from a freshly-solved ticket with confidence (mirrors the
  differential-test discipline in `DESIGN-testing.md`).

## Late binding: a prepared corpus over a new, differently-named bundle <a name="late-binding"></a>

The whole payoff is compile the corpus **once** (MQO + shared scans baked in) and run it
against **every** incoming bundle. But parse+plan bakes the `FROM` keyspace into the plan at
prepare time, and the next bundle rarely matches: a new customer's tree is laid out
differently, logs are rotated/suffixed (`indexer.log.3`, `indexer_2024.log`), a quarterly
dump is `2024Q4_results.parquet` not `2024Q3`. The prepared program refers to names that
aren't there verbatim — and recompiling per bundle throws away the amortization that
justified compiling at all.

The fix is ordinary prepared-statement **late binding**, applied to files: *compile against
LOGICAL keyspaces; resolve LOGICAL → PHYSICAL per bundle at EXECUTE time.* Two drift axes,
two layers:

- **Keyspace / file drift — a per-bundle binding manifest (resolver).** Detectors `FROM` a
  stable *logical* vocabulary (`indexer_log`, `orders`, `quarterly_results`), never a
  filename. At EXECUTE a small manifest maps each logical keyspace to how to find it in THIS
  bundle, on a robustness ladder:
  - **Explicit** — `indexer_log → glob("**/indexer*.log")`; copy-pasteable, versioned.
  - **Convention** — a logical name resolves through a glob/regex tolerant of version
    suffixes and layout drift.
  - **Content / schema sniffing** — resolve by WHAT a file is, not its name: a structured
    keyspace by column schema (Parquet), a log keyspace by a line-shape / grok signature.
    This is auto-cataloging the bundle — the robust-but-harder end.
  Because the compiled program reaches data through the [`DatastorePipe` / the datastore it
  opens at startup](#design-principle), the binding is exactly the "connectivity + config at
  startup" the pipe already carries. It is **data, not code** — so rebinding needs **no
  recompilation**. That is the property that makes recompile-once / rebind-per-bundle work.

- **Field / schema drift — a CORPUS concern, not a normalization adapter (design decision,
  2026-07).** When a new release of the log-generating software renames or reshapes fields
  (`level` → `severity`, a new nesting), the answer is that **the detector corpus changes too** —
  new/updated detectors that look for the new field names, or **version-tolerant** detectors that
  accept both (stock SQL++: `COALESCE(l.level, l.severity)`, an `OR` over old+new shapes — no
  adapter, no grammar change). The corpus is naturally *versioned* (a git repo of detectors), so
  it evolves alongside the software: think **detectors-per-software-version**, plus tolerance for
  the common case where a single support bundle carries logs from **several co-deployed versions
  at once** (a cluster mid-upgrade, mixed-version nodes). That heterogeneity is exactly why a
  per-keyspace *normalization adapter* is the wrong primary tool here: it would need to detect
  each record's originating version to normalize it, which is fragile — whereas version-aware /
  version-tolerant detectors handle the mix directly, and findings can be provenance-tagged by
  the detector's target version. A thin rename-adapter may still be a convenience for a trivial
  one-field difference, but it is NOT a required layer; the load-bearing binding is the *file*
  resolver above. (Timestamp normalization — many log time formats/zones → one sortable key for
  the ASOF merge — is a separate, real concern, but it belongs to the per-source *extract recipe*
  / parse-spec layer, `DESIGN-data.md`, not a field adapter.)

So the recipe repo versions, per logical keyspace: the **detectors** (evolving with the software
they target) and optionally a **source spec** (expected filename patterns + schema signature)
that drives content-based resolution — with a bundle-specific manifest overriding only where
file names genuinely differ. Binding is logical-keyspace-scoped, so the baked
[shared-scan / MQO structure](#mqo) is **bind-invariant**: only the leaf resolution changes.
The same machinery serves the non-support case — a fresh quarterly financial drop is just a
new bundle bound to the same logical keyspaces.

A binding must **fail loudly**, not silently: a logical keyspace that resolves to nothing (a
renamed file the manifest missed) should error at EXECUTE, not quietly yield an empty
findings table that reads as "clean."

## Detector authoring & ops — the AI-agent affordances <a name="authoring-ops"></a>

The corpus is meant to be authored and maintained by an **AI support agent** (from tickets / KB
articles / bug fixes) and run by tech-support teams. That makes *feedback and reporting* first-
class, not an afterthought. The load-bearing insight: **n1k1 already computes almost every signal
an author needs — it just needs surfacing.** `CorpusCompile` knows fuse/standalone/reject; the
native optimizer + `ExprCoverage` know native-vs-boxed per expression; the predicate index knows
literal-keyed-vs-always-wake; CSE knows the shared terms; the stats core (`DESIGN-stats.md`) counts rows in/out
per op (broadcast already exposes `RowsIn`/`FindingsOut`). So most of the below is *reporting what
exists*, not new machinery.

**The detector / corpus "report card" (a `detect lint`).** Per detector, surface: does it FUSE or
run STANDALONE (and why — "has a window function", "correlated subquery → ASOF merge") or get
REJECTED (with the parse/plan reason); does its predicate lower NATIVE or BOX (which caps its
compile level); is it INDEX-pruned by a necessary literal or ALWAYS-WAKE; which shared terms it
contributes to CSE; a rough cost class; and whether its golden fixture is present. Plus **advice**,
which is mechanical: a boxed sub-expr names its native alternative (`UPPER(msg) LIKE …` → boxes,
suggest a case-tolerant literal / `regexp_contains`); an always-wake detector is told "no necessary
literal — add a discriminating one as a top-level AND conjunct so the index can prune." And a
corpus-level **score**: `% fused / native / index-pruned`, CSE terms folded, est. predicate-evals
per row. This turns "write SQL and hope" into a tight loop AND is the guardrail that stops an
AI-authored corpus from silently bloating (all-always-wake) or lying (rejected → no findings).

**Debugging.** Extend `EXPLAIN`/`.explain` to the corpus plan (the `union-all(broadcast-indexed(
cse(scan)))` shape, the index literals, per-expr lane). A **per-detector hit report** —
`scanned / woken / matched` (the stats core already counts) — localizes a dud fast: `0 woken` = my
literal never appears (wrong field/bundle); `woken≫0, matched 0` = predicate-logic bug; `matched`
huge = too broad. **Evidence sampling** (`--sample N`) surfaces the rows behind a suspected false
positive. A **golden-fixture diff** (expected vs actual on the fixture) is the detector's unit
test. **Deterministic replay** via content-addressing (bundle fingerprint + corpus git SHA) lets
me reproduce and diff a past ticket's run after editing a detector.

**Per-bundle reports — and especially on RE-RUN.** The findings table
(`{ticket, confidence, source_file, line_range, evidence, summary, detector@sha}`, GROUP BY tag to
de-dup/rank) in two renderings: **jsonlines** for the agent/pipeline (streaming, already built) and
a **markdown/box** summary for humans. Bundles get re-run as the corpus grows or more logs arrive,
so a **delta report** is the killer feature: keyed by (bundle-fingerprint, corpus-SHA), "*since the
last run: ET-999 now fires (detector@abc added); ET-12345 evidence grew 2→9 lines.*" A
**coverage/health** block that is **fail-loud**: which logical keyspaces resolved vs. errored
(an unresolved `indexer_log` is a *gap*, never a clean bundle — see [binding](#late-binding)),
which detectors ran/errored, the corpus version, and which software-version detectors applied
(the [version-aware corpus](#late-binding)).

**Shaping SQL++ for fusion + authoring.** A **recipe format** = SQL++ + front-matter (ticket,
severity, `source:` logical keyspace, `versions:` for the version-aware corpus, tags) + a **golden
fixture** (sample rows + expected findings) — the front-matter is exactly what the corpus compiler
needs (Tag, routing target, version) plus what makes CI possible. And **fusion-friendly idioms**
the authoring guide standardizes (and the report card nudges toward): lead a predicate with a
discriminating literal as a top-level `AND` conjunct (so the index prunes); version-tolerance via
stock `COALESCE(l.level, l.severity)`/`OR` (no adapter — [field-drift decision](#late-binding));
ASOF as the canonical argmax subquery; rate/burst via stock `OVER (…)`; a uniform findings
projection (`… AS evidence`) once the projection envelope lands.

**Dev/ops / CI.** The `.rules` dot-command family is built: `run` (corpus→findings + coverage),
`lint` (the report card), `test` (golden fixtures, `--update` to record). Being added for the
low-cognitive-load surface: **`.rules list`** (a metadata-only inventory — tag/source/severity/
versions/has-fixture — that needs neither a compile nor a bundle, distinct from `lint`'s compiled
health report), **`.rules help`** (embedded docs: a sample corpus directory layout, an annotated
recipe showing the front-matter + fixture, example `run`/`lint`/`test` output, and short "get the
best out of it" tips), and **fix-carrying messages** — every reject/standalone/always-wake/boxed/
unresolved-keyspace status ships a mini snippet of the fix (e.g. always-wake → "add a literal:
`… WHERE msg LIKE '%panic%' AND …`"), so an author or agent doesn't have to reason it out.
**Golden-fixture CI** runs `.rules test` over the corpus on every commit (`make rules-test`;
non-zero exit on FAIL) — mirroring n1k1's own differential-test discipline to bound false
positives. Still ahead: a **corpus lint gate** (fail CI on `rejected` / missing fixture) and the
[SHA-keyed build cache + provenance](#compile-corpus) so findings cite `detector@sha`.

## PREPARE++ phasing <a name="detect-phasing"></a>

1. **Zip datastore + LOGICAL keyspaces + source routing** — **mostly DONE.** The extract-recipe
   data layer (`DESIGN-data.md`) presents a bundle's files as keyspaces (recipe-matched files
   auto-expose; glob keyspaces; a symlinked/unzipped tree scans today); a stable *logical*
   vocabulary is built (phase 2's binding, `glue.Binding`); source *routing* is done
   (`BroadcastRoute`); and the corpus compiler infers a detector's target keyspace from its `FROM`
   (`branchScanKeyspace`) to group + fuse. A true zip-as-datastore is optional (engineers unzip).
2. **Late binding — manifest DONE; adapters reframed.** The logical→physical *resolver* is built
   (`glue.Binding` / `OpenSessionBound`, `glue/binding.go`): a per-bundle manifest maps each
   logical keyspace to a glob, resolved at bind time, so the same corpus runs against the *next*,
   differently-named bundle by pointing at its root — no detector edits; a bound keyspace reports
   its LOGICAL name so same-logical detectors fuse into one shared scan; an unresolved/empty-glob
   keyspace **fails loudly** (`TestBindingTwoBundles` etc.). The convention/content-sniffing rungs
   of the resolver ladder remain future work. Per the field-drift [design decision](#late-binding),
   schema/field *adapters* are NOT a required layer — field-shape drift is handled by evolving the
   (version-aware) corpus, not by normalizing records.
3. **Shared-scan fan-out op (MVP MQO)** — **DONE** (`engine.OpBroadcast`; scans once, N native
   predicates; measured vs N separate runs — see [Shared scan / MQO](#mqo)).
4. **Predicate index + corpus CSE** — **DONE** (`engine.OpBroadcastIndexed` + `base.AhoCorasick`;
   `engine.BroadcastCSE`). The scale win: a row wakes ~O(hits) detectors, and shared sub-exprs
   compute once/row. ~60× at K=1000 / ~2.5× CSE — see [Shared scan / MQO](#mqo).
5. **Temporal as optimizations** — **DONE** (`DESIGN-merging.md`): the nearest-preceding argmax
   idiom is recognized and lowered to a K-way merge-join (ASOF, incl. soft / partitioned /
   cross-node / near-sorted), all differential-tested; windowed rate/burst/streak ride stock
   `OVER (…)`. Both grammar-free.
6. **PREPARE++ corpus compiler** — **DONE (MVP)** (`glue.CorpusCompile` / `CompiledCorpus`).
   Turns a set of SQL++ detectors into one runnable plan by classifying each: canonical
   single-source filter+project detectors **fuse** into the shared-scan broadcast → CSE → index
   (grouped per keyspace under `union-all`, field-refs alias-normalized to one canonical `.` row
   against a single unified `Conv`/`Temps`); non-fusable-but-valid detectors (ASOF/argmax
   subquery — routed via a projection-subquery check — plus window, GROUP BY, join, index-scan)
   run **standalone** through the full pipeline (so `WireASOFJoin` etc. fire, each individually
   optimized) with findings unioned in; parse/plan failures are **rejected** (surfaced with a
   reason, never silently dropped). A differential test gates it (fused ∪ standalone findings ==
   running each detector's own SQL) and proves `AsofRewriteApplied` fires for an ASOF detector in
   a corpus. *Remaining tail:* SHA-keyed build cache, embed-source analyzer binary, per-detector
   projection envelope (fused evidence is the whole matched row today), and standalone detectors
   not yet sharing scans among themselves.
7. **Recipe format + golden-fixture CI + agent ops** — **DONE (MVP)** + polish. The AI-authoring
   flywheel and tech-support surface. Built: the **`.rules` dot-command** family (`cmd/n1k1/
   rules.go`) — `run` (corpus → coverage + tagged findings), `lint` (the report card: fuse/native/
   index/advice + corpus score), `test` (golden fixtures); the **recipe format** (single file:
   `-- key: value` front-matter + SQL++ + inline `-- @fixture` / `-- @expect`, backward-compatible
   with a bare `.sql++`; `glue.LoadCorpus`/`Recipe`); **golden-fixture CI** (`.rules test`
   [`--update` records the golden], non-zero exit on FAIL, `make rules-test`). The report-card
   signals are *surfaced* from what `CorpusCompile` / `ExprCoverage` / `engine.PrefilterLiteral`
   already compute. Polish being added: a metadata-only **`.rules list`** inventory (no compile/
   bundle needed), a **`.rules help`** with a sample layout + annotated recipe + example outputs +
   tips, and **fix-snippet-carrying error/advice messages**. Remaining tail: per-detector hit
   stats, the **re-run delta** report, multi-keyspace fixtures, version-selection from the parsed
   `versions:` metadata, and `.rules bind` dry-run.

Each phase is independently useful, and the **core pipeline is now end-to-end**: logical-keyspace
binding (phases 1–2 core) → corpus compiler (phase 6 MVP) → the MQO + temporal engine substrate
(phases 3–5) → findings, all proven by benchmarks + a differential corpus test. The remaining
work is the *authoring / caching / packaging* layer that turns these primitives into a maintained
detector product: phase 7 (recipe format + golden-fixture CI), phase-6's tail (SHA-keyed build
cache, embed-source binary, projection envelope, standalone-scan sharing), the upper rungs of the
binding resolver ladder (convention / content-sniffing), and organizing the version-aware corpus
(the field-drift decision above).

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

PREPARE++ (detector corpus):

- **Binding robustness & authorship** — the explicit (glob) rung is built + fail-loud; how far up
  the → convention → content-sniffing ladder is worth building, and who authors the manifest: a
  human per bundle, an auto-cataloger, or a source spec in the recipe repo? (Pre-run validation
  is partly answered: an empty-glob logical keyspace already errors at bind, before any findings.)
- **Version-aware corpus (supersedes "field adapters").** DECIDED (see the field-drift
  [design decision](#late-binding)): field-shape drift is handled by evolving the corpus
  (version-specific and/or `COALESCE`/`OR`-tolerant detectors), not a normalization adapter —
  because one bundle can carry several co-deployed software versions, which a per-record
  normalizer can't cleanly disambiguate. Open: how best to ORGANIZE a version-aware corpus —
  version tags / directories per software release, a tolerance idiom the authoring guide
  standardizes, and provenance-tagging a finding by the detector's target version — and how the
  [shared-scan MQO](#mqo) folds many near-identical per-version detectors (corpus CSE should
  factor their shared terms; the predicate index should keep per-version literals cheap).
- **Corpus granularity — genuinely unsettled.** One giant fused program per bundle, or
  **sharded** by source/subsystem (indexer detectors, query detectors, …) compiled + cached
  independently? Sharding bounds compile time and lets an agent ship one rule without
  rebuilding the world (with the predicate index *within* each shard); one program maximizes
  cross-detector CSE. Tradeoffs not yet clear.
- **Set-returning extensions without grammar changes** — scalar UDFs exist today; a
  table-valued function in `FROM` would need parser support (which we're avoiding). Can the
  temporal/expansion needs stay inside stock subquery / `UNNEST` / self-join idioms, or is a
  grammar-free TVF hook worth it?
- **Recognizing the ASOF idiom** — *RESOLVED* (`DESIGN-merging.md`). A paranoid recognizer
  (`glue.WireASOFJoin` / `MatchArgmaxAsof`, golden near-miss tests) detects the canonical
  correlated argmax — `(SELECT r.<f> FROM R r WHERE r.<key> <= e.<key> [AND r.<eq>=e.<eq>]*
  [AND r.<key> >= e.<key> - Δt] ORDER BY r.<key> DESC LIMIT 1)` — and lowers it to a streaming
  K-way merge-join (soft / partitioned / cross-node / near-sorted all covered, differential-
  tested). Authors write that stock correlated subquery; the merge is transparent. Remaining
  nuance: the log time model below (normalizing timestamps into one sortable key).
- **Log time model** — how to normalize wildly different log timestamp formats / timezones
  into one sortable key for the merge-based ASOF: a per-source parse spec, or inferred?
- **Native-coverage ordering** — which string / regex / time exprs (`DESIGN-exprs.md`) to
  port first to unblock the most common detector shapes for full codegen?
- **Predicate-index structure** — *ANSWERED for the MVP* (`engine.OpBroadcastIndexed` +
  `base.AhoCorasick`): a single **Aho-Corasick** pass over the raw row bytes, keyed by each
  detector's necessary discriminating substring (a sound over-approximation — presence in a
  wrong field only over-wakes; never under-wakes), giving ~O(hits × rows). An **equality/range
  index over parsed fields** (for structured `field = const` / range predicates, avoiding the
  substring over-wake) and a hybrid BE-tree remain the natural refinement for the
  structured-heavy case — still open which blend is worth it.
