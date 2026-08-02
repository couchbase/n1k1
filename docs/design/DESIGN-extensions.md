# Extending n1k1 — functions, aggregates, streaming sources, extract plugins, macros

## Status & remaining TODOs

_Last reviewed: 2026-07-23._

n1k1 runs SQL++ via cbq's parser+planner; this doc grows the engine's *surface* —
builtins, drop-in user functions (JS or Go), table-valued/streaming sources, file-matched
extract plugins, and pre-parse macros. One hard constraint frames everything: **n1k1 builds
`CGO_ENABLED=0`** (pure-Go static binary), which rules out anything needing cgo (notably
Go's `plugin`/`.so`).

**Done** (live + tested, interpreter + compiler; `test/ext_test.go`), all unlocked by two
tiny fork setters — `expression.RegisterFunction` (patch-05) and `algebra.RegisterAggregate`
(patch-06) — that open the parser's builtin + aggregate registries without a grammar change:
- **goja JS scalar UDFs** (opt-in dir/file/inline registry) + **JS modules** (multi-export
  `exports.functions`, per-entry `kind`/`marshal`; embedded `builtin_decimal`/`builtin_ejson`
  auto-registered).
- **Extension aggregates**: native zero-garbage `sparkline()`/`histogram()` (`base.Agg`) +
  JS 3-callback (`init`/`update`/`final`).
- **Streaming table-valued sources** (`*.stream.js`, `emit` protocol) on one generic
  `stream-fn` op (which `MULTI_MATCHES` also rides).
- **`*.extract.js` plugins** — `describe()` returns a declarative `ExtractSpec` applied on
  the native byte lane (JS off the per-record hot path).
- **`*.macro.js` pre-parse SQL++ macros** — `@name(...)` → generated SQL++ before cbq's
  parser (gensym hygiene, `.macro expand`); starter library `grep_context`/`sessionize`/
  `top_per_group`/`transitions`.
- **Inline golden examples** (`glue/ext_examples.go`): every JS file may declare an
  `examples` array, run through the real per-kind protocol by `.extensions test` (non-zero
  exit on failure for CI).

**Remaining (headline TODOs):**
- [x] ~~Extract plugins are `describe()`-only; the imperative `extract(file, meta, emit)`
  escape hatch for irregular formats is not wired.~~ **DONE** — both a BUFFERED
  `extract(file, emit)` (whole-file, `toml2.extract.js` matches native `.toml` byte-for-byte)
  and a STREAMING `extractStream(file, emit)` (incremental `readLine`, backpressured per-row
  emit at bounded memory, race-clean, `stanza.extract.js`) are wired to the
  `records.ExtractPlugin.Extract` seam. A plugin's claim now makes even a brand-new extension a
  record file (`records.IsRecordFile` honors the registry).
- [ ] Streaming sources don't early-terminate on `LIMIT` (the `YieldStats` LIMIT hook is
  inert) — an **unbounded source hangs under `LIMIT`**; needs engine-wide producer early-exit.
- [x] ~~JS aggregate state round-trips through JSON per Update — O(state) per row, so a
  LIST-accumulating aggregate is O(n²) over the group (measured ~0.9ms/17K allocs per
  Update at 1000 held rows).~~ **DONE — JSVM-resident state (see "JS aggregate state v2"
  below): the accumulator stays a live goja value behind a fixed 9-byte handle; the same
  list shape through the real bridge is ~0.9µs/Update FLAT regardless of held state
  (`BenchmarkJSAggListStateResident`, ~940× at 1000 held), and non-JSON state (Map/Set)
  now works.** Still v1: callbacks have no error channel (throw/NaN → null).
- [ ] Full cbq UDF bridge unwired — `VisitExecuteFunction` returns `NA()`; no `CREATE
  FUNCTION` DDL / metadata catalog.
- [ ] More native `base.Agg` aggregates; streaming CTEs (single-use pipe + multi-use
  spill-rescan); wazero (Wasm) sandboxed extensions; `require()`/modules; power-tier host
  functions (HTTP/S3, allowlisted `exec`).

## The function-name resolution seam

At parse time `NAME(args)` resolves in order (`parser/n1ql/n1ql.y`): (1)
`expression.GetFunction` (builtin registry, `_FUNCTIONS`); (2) FTS; (3) `GetAggregate`;
(4) `GetUserDefinedFunction` → the UDF subsystem; else fatal. n1k1 owns the fork, so the two
extension points — the builtin registry (1) and the UDF resolver (4) — are open. **The full
UDF bridge is not wired** (`glue/conv.go`'s `VisitExecuteFunction` returns `NA()`), so
unknown/UDF names error at parse today; the two patch-05/06 setters sidestep it by
registering into (1)/(3) directly. **Extract plugins are off this seam entirely**: matched
to *files* (by extension/regexp), not invoked by name, they register into a separate
scan-layer extract registry (`DESIGN-data.md` §4). Keep the two axes distinct: name→function
vs file→extractor.

## Extensibility tiers

- **Tier 1 — native Go builtins.** `expression.Function` impls registered via
  `expression.RegisterFunction` from n1k1's `glue` (so `base`/`engine` stay cbq-free). For
  functions needing Go libraries or I/O; runs the interpreted/boxed lane, not the zero-alloc
  fast path.
- **Tier 2 — "a bunch of JS in a directory/git repo" (drop-in UDFs).** Registry = the
  filesystem (`.n1k1/functions/*.js`, `git pull` to update); runtime = goja (MIT, pure-Go,
  no cgo/V8/Enterprise). Opt-in only (in-process user code is an attack surface): embedder,
  CLI `-ext`/`-extensions`, or the `.extensions` REPL command (`list`/`load`/`unload`).
  (cbq's own golang/JS UDF paths are Enterprise-only and use `plugin.Open` — toolchain-locked.)
- **Tier 3 — inline N1QL UDFs (`CREATE FUNCTION … { expr }`).** Expression-only, trivial to
  wire — but needs the UDF bridge above.

## JS UDF runtime & state (the goja execution model)

⚠ The live `goja.Runtime` is scoped **per query, per actor** (`glue/ext_jsvm.go`): programs
compile once at registration, but the runtime builds lazily on the eval context
(`GlueContext.jsRT`, fresh per `Session.Run`); `ChainExtend`'s per-actor clone gives each
concurrent UNION ALL branch its own. One `goja.Runtime` isn't goroutine-safe, so this keeps
each single-threaded with **no pool and no lock** (per DESIGN.md). Consequences:

- **Module-scope globals persist across calls within a query, RESET on the next query** —
  good for per-query caches (hoisted regexes); a "global counter" resets per query, so use
  SQL aggregates for cross-row accumulation. No cross-query leakage.
- **A UDF can call another loaded UDF** (shared global scope; `RegisterExtensionDir` loads in
  sorted filename order, last definition wins, so `zz_overrides.js` shadows `base.js`).
- **The whole runtime dies with the query** — no process-lifetime accumulation; a
  panic/timeout drops it mid-query; each actor's runtime frees independently.
- **`async`/`await`/Promises are rejected** (no event loop) — a Promise return fails with a
  clear message, not a hang.
- **Cost ≈ 1 µs/row at the JS boundary**, dominated by the boxed `ConvertVals`, not goja —
  fine for scan-heavy work, not tight numeric loops. `console.*` → `glue.JSConsoleWriter`.

**JS modules & `marshal`.** A `.js` setting `exports.functions` loads as a module (a whole
family in one namespace), each entry self-describing `name` / `kind` (scalar/aggregate/
stream) / `marshal` (`json` default; `variant` = EJSON-tagged JSON like `{"$numberDecimal":
"9.99"}`, since JS can't hold exact decimals — a true `V`-carrier through cbq's `value.Value`
is the write-back follow-up; `raw` = opaque `Val` bytes). Every runtime gets a host `ejson`
helper; `builtin_decimal.js`/`builtin_ejson.js` are `go:embed`'d and auto-registered (so
`DECIMAL_*`/`EJSON_*` need no `-ext`). Filename is just the bundle/namespace — kind/marshal
are per-entry, not filename suffixes (which would be combinatorial; `DESIGN-variant.md` §5.2).

## Extension aggregates

- **Native `sparkline()`/`histogram()`** — zero-garbage against the `base.Agg` byte-slice
  Init/Update/Result protocol (`base/agg_ext.go`, reusing MEDIAN/VARIANCE numeric-list
  state, rendering the unicode chart into the reused buffer). A parse/plan-only
  `algebra.Aggregate` shim (`glue/ext_agg.go`) makes the parser accept them; conv routes
  computation to `base.AggCatalog[name]`, so cbq's Cumulate* never runs.
- **JS 3-callback** (`NAME_init`/`_update`/`_final`, `glue/ext_jsvm_agg.go`): a `base.Agg`
  bridge with JSVM-RESIDENT state — the group buffer holds a 9-byte handle, the live
  accumulator stays on the actor's runtime (see "JS aggregate state v2" below). No
  per-row state marshal; no error channel (unchanged).

**Aggregate state is normally mutable.** `state.rows.push(x)`, nested arrays, `splice`
and property writes all behave as a JS author expects. That is not free by construction:
the accumulator is decoded per Update, and `rt.ToValue` over a Go `[]interface{}` yields
a FIXED-LENGTH slice view whose `push` silently no-ops — so an early version dropped every
collected row while `state.n++` kept working, which hid the bug behind partial state.
`aggStateFromJSON` therefore rewrites decoded arrays to `*[]interface{}` (goja wraps a
slice POINTER as growable). Measured cost: none — identical allocs/op to the buggy path.
Using the runtime's own `JSON.parse` instead also fixes it but was **~2.5× slower with
~3× the allocations** (it eagerly materializes the object graph; `ToValue` wraps lazily),
so don't "simplify" it that way. Guard: `TestJSAggMutableState`.

Both reuse the same shim, so `NAME(expr)` works in GROUP BY. Compiler mode: they dispatch
through `base.AggCatalog[name]` (the runtime lookup group-op codegen already emits), so they
compile by construction — but a compiled JS UDF needs its `Register*` to have run in the
executing process (the baked `exprStr` must re-resolve the name).

### JS aggregate state v2 — JSVM-resident state (SHIPPED, except serialize/deserialize)

**Status:** resident-by-default state, the `NAME_snapshot` live view (`Agg.ResultLive`),
runningCapable-by-construction, and the JSON-adoption fallback are SHIPPED
(`glue/ext_jsvm_agg.go`; guards `TestJSAggResidentHandleProtocol`,
`TestJSAggMapStateResident`, `TestExtJSAggregateResidentGroupBy`,
`BenchmarkJSAggListStateResident`). `serialize`/`deserialize` remain design-only: no
boundary consumes them yet (spill carries the handle and keeps the object resident;
cross-actor merge doesn't exist) — wire them when real evict-to-disk or
group-partial/group-merge lands. The rest of this section is the design rationale.

The v1 trade-off above (state as JSON bytes through the group map, O(state) per Update) is
the wrong default for the advanced tier: an aggregate holding a `Map` of thousands of cells
pays a full state decode+encode **per input row**. v2's answer: **state never leaves the
JSVM.** Whatever `init()`/`update()` return is retained as a live `goja.Value` on the
actor's pinned runtime; the `base.Agg` state in the (spillable) group map becomes an
**8-byte handle** into a side table on the actor's context. Per-row cost drops to one
boundary call with the operand — state is O(1) at the border regardless of its size.
Fixed-width state is also the friendliest shape for the group map (never grows in place).

**Why pinning is the architecture (and already exists).** goja values CANNOT travel between
runtimes — a `goja.Value` references its runtime's internals (prototypes, interning), goja
is not goroutine-safe, and there is no serialization of live objects. So resident state must
pin to one runtime for the aggregation's lifetime — and n1k1's execution model already
provides exactly that: the runtime is **per query, per actor** (`GlueContext.jsRT` +
`ChainExtend`'s per-actor clone, single-threaded, no locks). The side table lives and dies
with the runtime (query end frees everything; `final` frees its handle eagerly), so the
leak window is bounded by the query. Any state that must CROSS a runtime (another actor, a
spill file, a client) crosses as bytes — that single fact drives the whole boundary design
below.

**The contract.** Required callbacks (v1-compatible — existing `*.agg.js` run unchanged,
just faster; mutate-in-place and return-new-state styles both work, the returned value
re-targets the handle):

- `init() → state` — any JS value; stays in the JSVM.
- `update(state, v) → state` — per input row; `state` is the LIVE object (no decode).
- `final(state) → result` — ⚠ **MUST be pure and repeatable.** This is the one new rule,
  and it buys the inflight story below: `final` may be called many times mid-flight (as the
  snapshot) before the terminal call. Result must be JSON-able (it crosses to the client).

Optional callbacks, all boundary-only (never per-row):

- `snapshot(state) → view` — a CHEAPER progressive view than `final` (counts-only, top-K
  cells) for the inflight channel; defaults to `final`.
- `serialize(state) → string` / `deserialize(string) → state` — boundary transport;
  default `JSON.stringify`/`JSON.parse`. ⚠ **Silent-loss trap:** `JSON.stringify(new Map())`
  is `"{}"` — precisely the developers this tier targets (who reach for `Map`/`Set`/
  `TypedArray`/`BigInt`) get silently-empty state at a boundary unless they provide the
  pair. Not cheaply detectable; documented contract, and `.extensions test` golden examples
  should include a serialize round-trip case.
- `merge(state, other) → state` — fold a `deserialize`d partial in. Absent ⇒ the aggregate
  is single-actor-only (today's group op is single-actor anyway, so nothing is lost yet).

**The three boundaries where bytes (not handles) travel:**

1. **Spill.** v1 policy: the 8-byte handle spills; the JS object stays resident — so the
   group map spilling **no longer relieves memory** for JS-resident aggregates (documented,
   acceptable for the advanced tier). Real evict-to-disk (serialize on evict, deserialize +
   re-handle on reload) is the later step `serialize` reserves the door for.
2. **Cross-actor merge** — parallel UNION branches now, group-partial/group-merge later
   (the partial-state serialization contract is the SAME one that feature needs: version it
   per aggregate — `state_version` in front-matter — not per query).
3. **Inflight snapshots** — rides the EXISTING live running-aggregates machinery
   (DESIGN-stats.md: `RunningAggsRefresh` at YieldStats checkpoints, ~10 Hz throttle,
   bounded sample of the in-flight group map). A JS aggregate is `runningCapable` by
   construction since `final` is pure; `snapshot` substitutes when the full result is too
   big for 10 Hz. This is the "work-in-progress results" feature: an AI client watches the
   aggregate-so-far and can cancel / course-correct (`Session.Interrupt`) without waiting
   for the full pass.

**The whole-record pattern (collapsing UNNEST + GROUP BY in the aggregate).** An advanced
JS aggregate does not need engine fusion to avoid the fan-out: `SELECT js_census(r) FROM
events r` — no UNNEST, no GROUP BY — walks each record IN JS and keeps its own
`Map<(type,path,valtype), cell>`, exactly the (since-retired, now test-only-oracle) native
`builtin:census`'s one-pass in-flight
shape. Boundary crossings drop from once-per-element to once-per-record (17.5× on the
census corpus). Honest perf expectation: the ~1 µs/row boundary is noise; goja's
`JSON.parse` of each record plus the interpreted walk dominate (goja ≈ 10–50× native Go for
compute), so this lands near the SQL++ census lane, NOT near native Go. The sell is
**power per unit of forkability** — histogram buckets, example-value sampling, custom type
lattices, top-K per cell — in a git-cloneable `.js` file with no Go toolchain, at
acceptable speed. Same philosophy rung as `extractStream`'s raw `emitBuffer`: opt-in
complexity buying control.

**Unchanged / open.** Compiled lane unchanged (dispatch stays `base.AggCatalog[name]`;
register-LAST rule applies). Error channel still missing (v1 TODO stands). Operand shape:
default = parsed value (host does the parse — `rt.ToValue` lazy-wrap, per the v1 lesson
that runtime-side `JSON.parse` was 2.5× slower); a raw-string opt-in for byte-level authors
is plausible later. Window/Slide lane: JS aggregates stay excluded from window frames in
v2 (no Slide protocol). DISTINCT: engine dedups upstream on value bytes — composes free.

## Table-valued / streaming sources in FROM

`FROM <expr>` is `plan.ExpressionScan` → `expr-scan` (`ExprScanOp`); an array yields one row
per element via `ArrayYield` — but the **source materializes fully first** (`Evaluate` builds
the whole `value.Value`, `json.Marshal`s it, only then streams), the memory blow-up for a
500-page PDF shred.

**Streaming fix (v1 shipped, `*.stream.js` / `glue.RegisterJSStream`).** A `function
NAME(emit, ...args)` pushes rows via `emit(row)`; the producer (`glue/ext_jsvm_stream.go`)
implements `StreamSource`, and `VisitExpressionScan` routes it to the generic `stream-fn` op
(`glue/stream.go`) — no materialization, bounded memory, composing with WHERE/GROUP BY. The
push-based engine makes a generator that yields into it stream automatically; `emit_batch([rows])`
amortizes the ~1µs boundary over a chunk (handed straight through as one `[]base.Vals` to the
Stage exchange). ⚠ **v1 does NOT early-terminate on `LIMIT`** (the `YieldStats` LIMIT hook is
inert): `LIMIT k` drops extras downstream while the JS loop runs to completion — fine for a
huge *finite* source, but an **unbounded** source (`for(;;) emit(...)`) hangs. Bound your
source until engine-wide producer early-exit lands (then `emit` returning `false` stops the
loop, for free). (An expert author can reach near-zero-copy via `ArrayBuffer` views over the
row buffer, valid only within the callback — most functions just marshal and pay the copy.)

## Extract functions (`*.extract.js`) — file-matched, scan-layer

A *different class* on a different axis: **implicit, matched to files** and run by the
scan/extract layer (`DESIGN-data.md` §4) to turn messy inputs into typed rows + the
file-level metadata the engine prunes/merges by. n1k1 core is domain-agnostic — all file
knowledge lives in a git-cloned plugin repo. A `*.extract.js` exports up to three things:

- **`match`** — which files it claims (`exts`/`names` regexps, `priority`); also the
  source-routing key for `DESIGN-prepare.md`'s MQO.
- **`describe(file) → ExtractSpec`** — CHEAP, runs **once per file** (may sample via
  `file.head/tail/slice`), memoized in the `.n1k1` sidecar by file fingerprint. Returns a
  DECLARATIVE spec (`framing`/`fields`/`time`/`order`/`provenance`) that n1k1 executes
  **natively** (`records.SpecApply`), so no JS runs on the GB-scale per-record path. This
  is the preferred path for line/multiline/section-framed text.
- **`extract(file, emit, emitBuffer)`** — the imperative escape hatch for self-contained or
  irregular formats a declarative spec can't frame. JS receives the WHOLE decompressed file
  (`file.text`, plus `path`/`name`/`ext`/`stem`) and calls `emit(doc[, id])` per record, so
  it owns framing AND parsing; records are buffered into a `records.Source`, paying the JS
  boundary once per file (not per row). **WIRED** (`glue/ext_extract_jsvm.go` →
  `records.ExtractPlugin.Extract`). The flagship demo, `extensions/extracts/toml2.extract.js`,
  parses TOML in JS under `.toml2` and reproduces the native Go `.toml` reader's records
  exactly.
- **`extractStream(file, emit, emitBuffer)`** — the STREAMING sibling, for a large/irregular
  MULTI-record file that shouldn't be buffered. JS reads incrementally — `file.readLine()` /
  `file.readAll()` for text, **`file.readBytes(n)`** (up to `n` raw bytes as an `ArrayBuffer`,
  `null` at EOF) as the GENERAL primitive for binary / length-prefixed / fixed-width / custom
  framing, or **`file.readInto(view)`** its zero-alloc BYOB form (fill a REUSED `Uint8Array`,
  returns bytes read — cf. Web Streams `reader.read(view)` / Node `fs.read(buffer)`; goja
  exports a `Uint8Array` as its live backing `[]byte`, so Go reads straight into it) — and
  emits: `emit(doc[, id])` marshals a JS value, or **`emitBuffer(bytes[, id])`** passes RAW
  JSON bytes straight through with NO marshal (n1k1 records are JSON `[]byte`; validated but
  not re-serialized — pair it with `readBytes`/`readInto` for a zero-hop binary/JSON plugin).
  Records flow out **one at a time with backpressure**, so memory stays bounded however large
  the file or record count. Mechanics:
  goja is single-threaded and calls `emit` synchronously, but `records.Source.Next` is
  pull-based — so the JS runs on its own goroutine and `emit` hands each record across an
  UNBOUNDED-loop-safe **unbuffered channel** (JS blocks until `Next` consumes). `emit`
  returns `false` once the consumer stops (`Source.Close` — a satisfied LIMIT, a cancel), so
  the JS loop can break (the same stop protocol as a `*.stream.js` source); `Close` also
  interrupts the runtime as a backstop and waits for the goroutine before releasing the file
  (no leak, no early-Close race). **WIRED + race-clean.** Demo:
  `extensions/extracts/stanza.extract.js` (blank-line-delimited stanzas). A plugin
  defines `describe`, `extract`, or `extractStream` (extract/extractStream are mutually
  exclusive); any of them may `match` a brand-new extension (`records.IsRecordFile` honors
  the registry).

The `file` host object has authority over **exactly one read-only file** (no network/exec) —
the ideal Wasm shape later. The registry is a git repo matched by file (`RegisterExtractDir`,
`priority` then load-order on overlap); built-in office/PDF extractors are `{framing: whole}`
specs under the same seam. ⚠ The `describe` `time`+`order` metadata is precisely the
[sorted-source contract](DESIGN-data.md) the K-way near-sorted merge-join and ASOF consume —
a wrong `disorder_bound` is a silent merge-corruption risk, so each plugin ships a golden
fixture (tiny fragment → expected spec + first-N records) run in CI.

## Macros (`*.macro.js`) — pre-parse SQL++ generators

**The problem:** the most natural log question — grep `-A`/`-B`/`-C` context — needs a
`WINDOW` subquery few can write cold, and **a JS UDF can't help** (scalar UDFs return a
value, stream sources return rows — neither can emit a `WINDOW` clause, which is *syntax*).
The answer is a **macro**: user JS that takes a compact `@name(...)` invocation and returns
**SQL++ source text**, expanded *before* cbq's parser. Chosen over a cbq AST rewrite (right
for n1k1's *own* desugaring, wrong for a drop-in surface — `@grep_context(...)` in `FROM`
isn't a grammar-accepted term) — this is pure front-end text→text.

⚠ **The whole feature is invisible downstream** — expansion is at the top of `ParseStatement`
(`glue/stmt.go`, the single chokepoint every `.multi` detector + ad-hoc query flows through),
before `n1ql.ParseStatement2`. After it the statement is ordinary SQL++, so planner/CSE/MQO/
ASOF/codegen see hand-written-shaped SQL and add **zero** B/C-engine complexity. No `@` in the
statement (or no macros loaded) → a single `strings.IndexByte`, input untouched.

**The `@name(...)` sigil.** `@` is chosen because n1ql's grammar doesn't use it (params are
`$name`/`?`), so it's lexically free. (⚠ Verify against the fork's lexer that a bare `@` is a
clean tokenizer error, not silently accepted.) The expander scans for `@ident(`, reads a
**balanced-paren** arg list (respecting string literals + `--`/`/* */` comments so a paren/`@`
inside a string isn't miscounted), calls `expand`, and substitutes the returned text wrapped
in parens. **Composition:** repeatedly expand the leftmost `@name(...)` whose args contain no
further `@` (innermost-first / applicative order), then re-scan (so a macro body can emit
`@another(...)`); arg nesting shrinks the `@` count (can't loop), body-emission is bounded by
a **depth/rounds cap** (~16).

**The JS contract: `expand(args, ctx) → string`.** `args` = positional `args[0..n]` + named
`args.<key>`, each the **raw SQL++ source substring** of the argument (a macro manipulates
syntax; `when` arrives as the unparsed predicate `sev = "ERROR"`, spliced verbatim; `args.$lit`
best-effort-coerces a literal). `ctx` = `{ gensym(prefix), error(msg), version }`.

⚠ **gensym hygiene.** `ctx.gensym("ctx")` returns a name unique to this expansion (global
gensym counter across the whole pass, so inner/outer/body expansions never collide) — the
same discipline as C's `__COUNTER__`. What it does NOT buy (no scope tracking at the text
level): a macro could reference a user column it never meant to. **Authoring rule:** introduce
every internal name via `gensym`, and only reference columns passed in as explicit args. (A
true AST-hygiene tier via a rename visitor is noted, not built.)

**Determinism + debuggability.** ⚠ `expand` must be **pure** (same args → same SQL++): it runs
upstream of plan caching + corpus fusion, so a non-deterministic expansion poisons a cached
plan (contract, not enforced). `.macro expand <stmt>` prints the fully-expanded SQL++ (the
primary debug tool); a post-expansion parse error is annotated with the responsible macro name
+ offending snippet, so it points at the generator, not its output. Trust: a macro is a code
generator running with full query authority, but it's exactly as trusted as the detector that
calls it (same author, same opt-in `-ext`).

⚠ **Engine bug surfaced while building macros — FIXED.** `SELECT *` + a *no-operand* window
function (`ROW_NUMBER()`/`RANK()`/`COUNT(*) OVER …`) panicked in `glue/expr.go` `Convert`:
those emit an internal `^worderby` ordering column, and `attKey`'s catch-all folded `worderby`
onto `ATT_AGGREGATES`, clobbering the real `^aggregates|…` map (`binaryValue vs map`). Fixed by
having `Convert` **skip the `^worderby` label** (it's consumed positionally by `op_window`,
never a boxed attachment). Regression guard: `TestNoPanicRegress` (`star-row-number`/etc.).

## Future directions

- **No `.so`/`plugin`** (needs cgo, loses the static binary, no Windows, version-brittle). The
  cgo-free spectrum: compile-time `init()`-registration (fastest, needs a rebuild) → goja /
  yaegi (drop-in source, no build) → **wazero (Wasm)** (Apache-2, pure Go, sandboxed) →
  subprocess/`go-plugin` (isolation, coarse). wazero is the modern "load an untrusted binary
  extension" answer: a guest's **bounded linear memory** (min=max pages / `WithMemoryLimitPages`)
  means a runaway guest traps in its own pool (isolation goja/plugins can't give); place n1k1's
  recycled row buffer in a fixed window of that memory so copy-in doubles as the marshal, read
  outputs back as zero-copy views (re-fetch after `memory.grow`). The end-state vision is a
  **sandboxed registry** (a PDF/XLSX shredder compiled to Wasm, pulled by hash) — "raw bytes in
  → pure transform → rows out, no ambient authority" is the ideal capability-free Wasm shape.
- **Power-tier JS (opt-in host functions).** goja has no ambient authority, so each capability
  is a host function behind a flag: a full operator `ctx` (`emit`/`emitBatch`/`stats`/
  `cancelled()`/`log`); `require()` for hygienic reuse (dir-scoped = a safety boundary);
  **synchronous** blocking host calls (`http_get`/`s3_get`/`run(...)` — an event loop was
  rejected) — `-ext-allow-net` (host/bucket allowlist vs SSRF), `-ext-allow-exec` (runs only
  programs in the `-ext` dir; allowlist-by-directory is the containment, not a sandbox).
  `system()`-ing n1k1 itself is a federation/fan-out primitive (needs a recursion guard vs a
  fork bomb).
- **Corpus scanning.** A `files()` crawler + `UNNEST shred(f, …)` composes *today* (UNNEST is
  implemented) and separates crawling from parsing so cheap file predicates filter before the
  expensive shred; embarrassingly parallel (one doc per worker/Wasm instance).
- **Streaming CTEs.** Single-use CTE → a child op feeding the consumer directly (the planner
  already hands us the sub-plan via `SetSubqueryPlan`); multi-use → spill-once + re-scan (the
  `base/heap.go` machinery). New work is in the converter, not the runtime.
- **Extension namespacing/versioning** — backtick-quote the name (`` `pdf_shred:v2` ``) so the
  parser hands the resolver one literal (bare `a:b:c` collides with cbq's namespace grammar);
  version by content-hash pinning (best), name, or config arg.

## Caveats

- **Security/sandboxing** — file-reading and JS-executing functions are a real attack surface;
  gated behind capability flags, in-process goja reach capped.
- **Determinism** — streaming sources + user JS can be non-deterministic and can't be cheaply
  re-read (a re-scan means re-run or spill); mind `DESIGN-testing.md`'s determinism rules.
- **Fast-path exclusion** — all of these run the interpreted/boxed lane, not the byte-native
  fast path or compiler codegen.
- **Licensing** (document parsers) — permissive only (MIT/BSD/Apache); **avoid** UniPDF/
  unioffice (AGPL/commercial). Candidates: `ledongthuc/pdf` (BSD), `xuri/excelize` (BSD),
  `nguyenthenguyen/docx` (MIT), `dop251/goja` (MIT) — verify transitive deps at adoption.
