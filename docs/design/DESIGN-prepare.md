# Design: PREPARE — SQL++ → Go, and running the prepared program

## Status & remaining TODOs

_Last reviewed: 2026-07-23._

n1k1 **compiles** a SQL++ plan into Go (the `intermed/` compiler + `glue/emit.OpToLines`).
`PREPARE`/`EXECUTE` expose that, cbq-compatibly, under a `-prepare=<level>` ceiling, with
the interpreter always the fallback. The second half of this doc is the **driving use
case**: PREPARE++ — compiling a *corpus* of thousands of SQL++ "detectors" and applying
them to support bundles with one shared scan (multi-query optimization).

**Done:** PREPARE/EXECUTE work in-session (parse+cache, re-plan+run with bound args) at a
ceiling of `interpreted` | `data` | `full`; at `full` a fully-native statement compiles to a
**cbq-free child binary** (engine+base) run over stdin/stdout (`executeCompiled`),
interpreter as fallback. PREPARE++ is built and differential-tested, running interpreted:
`CorpusCompile` classifies detectors fuse/standalone/reject and wires the MQO substrate
(broadcast / source-route / corpus CSE / Aho-Corasick predicate index); ASOF/temporal
lowers to a K-way merge; late binding (`OpenSessionBound` / `binding.go`) resolves logical
keyspace names to per-bundle globs (fail-loud); the datastore-pipe seam (`base.DatastorePipe`
/ `engine.MemPipe`) lets an emitted plan read inline data; the `.multi` dot-command family
(`run`/`lint`/`test`/`list`/`help`) drives authoring + golden-fixture CI.

> ⚠ **Naming:** the external surface is `.multi` (CLI) / `MULTI_MATCHES()` (in-SQL++ TVF) —
> renamed from `.rules` / `RULE_MATCHES` (2026), a **hard cut** (old names gone, no
> aliases). n1k1 *internals* keep their names (corpus, `CorpusCompile`, detector,
> `RejectedDetector`) — the rename is surface-only.

**Remaining (headline TODOs):**
- [ ] Fully standalone **fork-free analyzer binary** (`.multi build … -o analyzer` embedding
  a minimal datastore runtime + baked plan + recipes). MULTI_MATCHES codegens at `full` but
  still imports glue (`glue.DatastoreOp` islands), so it is NOT yet fork-free.
- [ ] embed-source **fat child** (direct datastore access, config/auth-only pipe) + the full
  multiplexed cursor pipe protocol with pushdowns; the WASM/wazero in-process alternative.
- [ ] Phase-6 tail: SHA-keyed build cache, per-detector projection envelope (fused result is
  the whole matched row today), standalone-scan sharing.
- [ ] Upper rungs of the binding resolver ladder (convention / content-sniffing).
- [ ] Native-expr coverage widening (`DESIGN-exprs.md`) — the lever that lets more detectors
  compile to `PrepareCompiledFull`.

## Background: the compiler, and the one boundary

Three facts shape everything:

1. **The emitted code's deps are tiny and public.** `engine/` + `base/` are cbq-free and
   CGO-free, so generated Go imports only them — no fork, no bleve/bbolt/cloud SDKs.
2. **The datastore *leaves* are the one unbaked boundary.** At runtime they dispatch through
   the overridable **`engine.ExecOpEx` hook** (`engine/op.go`), which the CLI wires to
   `glue.DatastoreOp`. Op kinds: `datastore-scan-{records,primary,index,index-cover,fts,keys}`,
   `datastore-fetch`, `expr-scan`, `js-stream`, `with-recursive`, `agg-metadata`,
   `agg-columnar`. So the generated Go is "the whole query minus its data-access leaves."
3. **Parse + plan already happened in the parent** (needs cbq's parser/planner). By codegen
   time the plan is baked in; the child needs only *runtime data*, never the planner.

So the compiled query is small + public + CGO-free, and the only thing it can't do alone is
reach a datastore.

## The surface & levels

**Statements** track cbq: `PREPARE [name] AS <stmt>` (parse→plan→convert, gate on
compilability, cache under `name` or a plan hash) and `EXECUTE <name> [USING args]`. The only
difference: n1k1's prepared artifact **may be compiled Go**, not just a cached plan.

**`-prepare=<level>` is a ceiling, not a switch** (default `interpreted`). `PREPARE` produces
the best artifact at-or-below the ceiling the query supports, and silently settles lower when
it can't reach it (a boxed expr caps it at `interpreted`). `glue.Preparable(op)` returns a
**`PrepareLevel`**, not a bool:

- **`PrepareInterpreted`** (default, universal floor) — a per-row expr is boxed (needs cbq
  `Evaluate`) or the plan didn't convert; EXECUTE runs the cached Op tree interpreted.
- **`PrepareCompiledData`** — every expr is native but a datastore op can't bake into a Go
  literal, so the program needs a runtime data provider. The *widest* compiled level.
- **`PrepareCompiledFull`** — native exprs AND every datastore op bakes in; self-contained.

**Inspection:** `.prepare <stmt>` is `EXPLAIN`-like — emits the `*.go` when the query reaches
`full` (no toolchain needed), else prints the reason and interprets. Only a compiled
`EXECUTE` shells out to `go build` (opt-in, permission-gated). PREPARE itself runs in the
parent, which always has cbq.

## Run models

The emitted code reaches data through a small **`DatastorePipe`** interface (`Scan`/`Fetch`/
`IndexScan`/`Meta`); the *linkage* picks the implementation, the generated query is identical
across them. (The code already flags this seam: `datastore_scan.go` calls the process-global
`ExecOpEx` "fine for the single-process CLI; a per-store field is the cleaner future form.")

- **Emit-only (library)** — a dev imports engine+base, supplies a `DatastorePipe` (e.g.
  in-memory `base.Vals`), calls `Run`. Zero datastore deps for inline data.
- **Thin child + data-over-pipe — DONE.** `glue.executeCompiled` lowers exprs natively
  (`ExprTreesOptimize` — field-access / arithmetic / nary / aggregates / const-folded
  projections compile, not just `SELECT *`), `go build`s a cbq-free child (engine+base), ships
  scanned records over its stdin, and the child streams **positional `base.Vals`** back
  (`ValsEncode` frames) which the parent reassembles via `ConvertVals`. A per-row boxed expr
  degrades to the interpreter. Why a child at all: Go has no runtime `eval` and `plugin` is
  fragile/rejected, so freshly-compiled query code is naturally a separate binary; the
  push-based engine maps cleanly (child drives the plan, parent serves data). The parent's
  data server is mostly existing code — `glue.DatastoreOp` wrapped in a request/response loop.
  (`TestExecuteCompiledFull`; `n1k1 -prepare=full -c 'PREPARE p AS …; EXECUTE p'`. Today
  records are shipped over stdin as a simplified frame; the full multiplexed cursor protocol
  with pushdowns is future.)
- **Fat child + embedded source — future, the throughput headline.** `//go:embed` a tightened
  datastore-runtime library, `go build` a self-contained program that reads datastores
  *directly*; the pipe carries only config/auth at startup + results back — no scan/fetch
  hopping, and an offline/hermetic build (local `replace`s, no private-fork fetch). Costs:
  compile time balloons (mitigated by the Go build cache + PREPARE-once/EXECUTE-many); the dep
  closure is large and grows with record providers (Arrow/Parquet, PDF/office), so the real
  enabling work is carving a **minimal runtime datastore library** (no parser/planner; only
  the providers a query touches); trust shifts (it holds credentials — not the sandboxable
  thin child).
- **WASM + wazero — future.** In-process sandbox; datastore leaves as host-function imports.
  ~2× slower, larger modules. Same "abstract the leaves" principle as the thin child.

**Thin vs fat is a per-query choice within `PrepareCompiledData`:** thin child (fixed minimal
deps, sandboxable, slower per-batch IPC) favored for heavy/varied record providers; fat child
(direct access, faster, config/auth-only pipe, heavier build) favored for light providers
(plain JSON) + throughput. Data-locality and cbq-presence are **separable axes**: a fat child
can read data directly AND still delegate a rare boxed expr back over a thin control pipe
(`EvalExpr` — the parent always has cbq).

## Boxed expressions across the boundary

The thin child has no cbq, so it can't evaluate a **boxed** expression (`exprTree`/`exprStr`
fallback — the sharpest limit on codegen). Note a boxed expr is a *runtime* dependency, not a
serialization blocker: the compiler rewrites `["exprTree", <expr>]` → `["exprStr", "<text>"]`
(`stringifyExprTrees`), so the plan serializes; what it can't do without cbq is *evaluate*.
Handling, best first:

1. **Gate: the thin child runs fully-native queries only (default).** Any boxed per-row expr
   → interpret in the parent. So **codegen coverage rides native-expr coverage** (every port
   in `DESIGN-exprs.md` widens the compilable set).
2. **Const-fold boxed sub-exprs at codegen time — IMPLEMENTED.** `glue.exprConstFold`
   evaluates any row-independent, non-volatile expr once during codegen and bakes a `["json",
   …]` constant; recursing, a constant *subtree* lifts an otherwise-boxed enclosing expr to
   native. The fold value comes from `Evaluate()` (not cbq's static `Value()`, which disagrees
   for some funcs — `GREATEST(9,null).Value()==null` vs `Evaluate()==9`); `NaN`/`Inf` stay boxed.
3. **`EvalExpr` over the pipe (reluctant).** Ship the boxed operand `Vals` to the parent,
   batched. But expression eval is *per-tuple* (the hot path) whereas datastore requests are
   coarse/batched — per-row boxing on a pipe is the slow boxed lane *plus* serialization. Only
   for a small boxed fraction on an already-filtered stream, always batched.
4. **Fat/embed child** — links the datastore runtime, so cbq is present in-child.
5. **Plan partitioning (future)** — keep the boxed *operator* parent-side and exchange batched
   row streams; native subtree compiles into the child. Moves whole ops, not per-row exprs.

Bottom line: native-expr coverage shrinks the boxed set; const-folding mops up the constant
tail free; the rest are reluctant escapes. The pipe is great for *data* (coarse), poor for
*per-row exprs* — which is why the native lane exists.

## Is codegen worth it? — the crossover

Codegen trades a fixed compile cost for a per-row speedup, so it pays only past a break-even.
Measured here: **compile (warm) ~0.1 s**; **per-row speedup only ~1.07× (scan/filter/project)
to ~1.22× (group-by)** — ~9–11 ns/row. n1k1's interpreter is already fast (byte-oriented,
native exprs, push-based closures), so codegen mostly inlines closure calls; the
boxing/allocation wins were already banked by the native lane, which both paths share. (The
standalone `EXECUTE` figure is read net of IPC via the child's self-reported compute wall:
env `N1K1_CORE_NS` → `Result.CompiledChildElapsed` → the CLI's "compiled child compute:" line.)

Crossover for a **one-shot** query ≈ `0.1 s / 10 ns ≈ 10M rows` (~100M–1B if it triggers the
cold first build). So `SELECT 1+1` is pure overhead. Where codegen pays: **prepared statements
(PREPARE-once / EXECUTE-many)** — break-even ≈ `10M / K` rows per execution (~10k rows at
K=1000) — and **very large one-shot scans**. So "can compile" (`Preparable`, a level) ≠
"should compile" (a worth-it heuristic: `est_rows × executions × ~10 ns/row > compile_cost`;
datastore-free/constant → never; explicit `EXECUTE`/reuse → yes; big scan → maybe).

## Phasing (first half)

1. `.prepare` inspection emit + gate + interpreter fallback — **DONE**.
2. Bakeable datastore scans + `DatastorePipe` + in-memory provider — **DONE** (drives the
   emit-only + thin-child paths).
3. `PREPARE … AS` / `EXECUTE … USING` + the `-prepare` ceiling + a run model — **DONE
   end-to-end** via the thin child + data-over-pipe. Remaining run-model work: embed-source
   fat child; the full multiplexed cursor protocol with pushdowns.
4. *(optional)* WASM/wazero in-process sandbox.

---

# PREPARE++ — the detector-corpus use case

Support engineers receive **support bundles** (big `*.zip`s of mixed log/JSON/config files).
The vision: a **git-maintained corpus of SQL++ "detectors"** — filters/scans/correlations that
report *"this bundle shows evidence of ET-12345"* — applied by the thousands to a bundle
**without scanning it thousands of times**. This hits both of codegen's payoff regimes at once:
compile the corpus **once**, run it against **every** bundle (each a GB-scale scan). Output per
bundle: a ranked findings table (`{ticket, confidence, source_file, line_range, result, summary,
detector@sha}`), `UNION ALL` across detectors, GROUP BY/ORDER BY to de-dup/rank.

### Key design decisions

- **MQO is interpreter-first; codegen is optional on top.** The broadcast/route/CSE/index
  stack runs over expr-trees natively (zero-boxing), so MQO needs no `go build`; compilation
  only adds fusion. "Weaving detectors into one program" is just **inlining** —
  `emit.OpToLines` fuses an op tree into one scan loop with K inlined `if pred_k { emit proj_k
  }` blocks; separate compilation *units* matter only for build-cache economics + CSE.
- **Fusion is keyed by LOGICAL keyspace identity**, so binding (file-drift) and shared-scan
  fusion compose: all `FROM indexer_log` detectors share one scan regardless of the physical
  files behind it. **Compile the MQO structure once; rebind the leaves per bundle.**
- **ASOF/temporal/window/group/join detectors run standalone, not fused** (each via its own
  already-optimized plan, findings unioned in); the compiler classifies each **fuse / standalone
  / reject** (reject = surfaced, never silently dropped).
- **Field-shape drift is a CORPUS concern, not a normalization adapter** (see Late binding).
- **MVP result = the whole matched row** (per-detector projection envelope deferred).

### Shared scan / MQO — the four levers (all DONE, benchmarked)

Push-based execution is the substrate: a scan pushes each row (`base.Val` = `[]byte`) into a
yield; multi-query makes that yield a **fan-out (tee)** into K detector pipelines, each reading
the shared bytes with zero boxing (decode once). The levers are orthogonal and compose:

- **`engine.OpBroadcast` (kind `broadcast`)** — scan once, fan each row to K inlined
  filter+project detectors, yielding tag-stamped findings. Removes redundant scans (up to
  ~6.3× fewer allocs at K=256) but is still **O(K × rows)** in predicate work — which the next
  three attack.
- **Source routing (`BroadcastRoute`)** — a source's scan fans out only to detectors targeting
  it (inferred from `FROM` by `branchScanKeyspace`; one broadcast per source under `union-all`;
  orphan detectors pruned + RETURNED). ~M× less predicate work for M sources.
- **Corpus CSE (`BroadcastCSE`)** — sub-predicates shared across detectors compute once/row via
  a precompute `project` below the broadcast (one `^cseN` column per shared term). ~2.5× at
  K=32 sharing one regexp; grows with K. (Expr-identity via canonical `json.Marshal` of the
  sub-tree.)
- **Predicate index (`engine.OpBroadcastIndexed`, `base.AhoCorasick`)** — the scale trick. Each
  detector is indexed by a **necessary** discriminating literal (from `contains`/`eq`/plain
  `regexp_*`/first `and` conjunct; unprovable → "always-wake"). One Aho-Corasick pass over the
  raw row bytes wakes only detectors whose literal is present. ⚠ **Correctness invariant: the
  literal must be NECESSARY (absent ⇒ predicate false), so over-wake is safe and under-wake
  never happens** — guarded by a byte-identical differential test. Turns O(K × rows) into
  ~O(hits × rows): ~60× faster at K=1000 (roughly flat in K). (An equality/range index over
  parsed fields is a future refinement for the structured-heavy case.)

Growing the corpus: adding a detector = insert one literal into the index + compile one
predicate, not rebuild the corpus. Shard by source so a new rule recompiles only its shard;
content-address each shard by the recipe repo's git tree SHA (cache hits on unchanged shards).

### Detectors stay in stock SQL++ — no grammar changes

Hard constraint: don't touch the dialect (adding syntax means editing the fork's grammar =
perpetual divergence). New capability lands as three grammar-free forms:
1. **Engine optimizations over stock idioms** — **ASOF** (nearest-preceding join) is the stock
   correlated-argmax subquery `(SELECT r.f FROM R r WHERE r.ts <= e.ts ORDER BY r.ts DESC LIMIT
   1)`, recognized and lowered to an O(n) merge (`DESIGN-merging.md`); the user writes stock SQL.
2. **Window functions** — rate/burst/streak/gap via stock `OVER (…)`.
3. **Scalar UDFs** — native exprs or JS UDFs via `-ext`, ordinary `func(args)` syntax.

The one gap: n1k1's extensions are **scalar**, not table-valued; a TVF-in-`FROM` would need
parser support — so prefer subquery/`UNNEST`/self-join idioms over a `FROM asof_join(…)` form.
Payoff coupling: regex/string/time exprs that **box** cap a detector at `interpreted` and block
fusing it, so widening `DESIGN-exprs.md` native coverage is what lets the corpus compile.

### Late binding: one corpus over differently-named bundles

Compile against **logical** keyspaces; resolve logical → physical **per bundle at EXECUTE**
(ordinary prepared-statement late binding, applied to files). Detectors `FROM` a stable logical
vocabulary (`indexer_log`, never a filename); a per-bundle manifest maps each logical keyspace
to how to find it, on a robustness ladder: **explicit** (`indexer_log → glob("**/indexer*.log")`,
built) → **convention** (version-suffix-tolerant globs) → **content/schema sniffing** (future).
Because the compiled program reaches data through the datastore it opens at startup, the binding
is **data, not code** — rebinding needs **no recompilation**, and the baked MQO structure is
bind-invariant. ⚠ **A binding must fail loudly**: a logical keyspace resolving to nothing errors
at EXECUTE, never a silently-empty findings table that reads as "clean."

**Field/schema drift is a corpus concern, not an adapter** (decision): when a release renames a
field, the *corpus* changes — version-specific detectors, or version-tolerant stock SQL++
(`COALESCE(l.level, l.severity)`). A per-record normalizer is the wrong tool because one bundle
carries **several co-deployed versions at once** (a cluster mid-upgrade), which a normalizer
can't cleanly disambiguate. (Timestamp normalization for the ASOF merge is separate — it belongs
to the per-source extract recipe, `DESIGN-data.md`.)

### Authoring & ops (the `.multi` surface + report card)

The corpus is authored by an AI agent and run by support teams, so feedback/reporting is
first-class — and **n1k1 already computes almost every signal an author needs; it just surfaces
it** (`CorpusCompile` knows fuse/standalone/reject; the optimizer knows native-vs-boxed; the
index knows literal-keyed-vs-always-wake; stats count rows in/out per op).

Built (`cmd/n1k1/rules.go`, `glue.LoadCorpus`/`Recipe`): the **`.multi`** family — `run` (corpus
→ coverage + tagged findings, jsonlines + box), `lint` (the **report card**: per detector
fuse/standalone/reject + native/boxed + index-pruned/always-wake + fix advice, plus a corpus
score), `test` (golden fixtures, `--update` records), `list` (metadata-only inventory, no
compile/bundle), `help` (embedded docs). Recipe = single file: `-- key: value` front-matter +
SQL++ + inline `-- @fixture`/`-- @expect`. **Golden-fixture CI** (`make rules-test`, non-zero on
FAIL) mirrors n1k1's differential-test discipline. Fix-carrying messages: every
reject/always-wake/boxed/unresolved status ships a mini snippet of the fix.

⚠ **Context (grep `-A/-B/-C`) gotcha.** The idiom is a sliding-window match flag `MAX(CASE WHEN
<pred> THEN 1 END) OVER (PARTITION BY _meta.`path` ORDER BY _meta.pos ROWS BETWEEN B PRECEDING
AND A FOLLOWING)` in a derived table filtered on the flag. **The `PARTITION BY _meta.`path`` is
load-bearing** on a multi-file (rotated-log) keyspace: `_meta.pos` restarts per file, so without
it context leaks across files and pulls unrelated lines into a match. For a timeline spanning
rotated files, order by an extract-recipe `time:` key instead.

**`gate:` for standalone detectors.** A fused detector is index-pruned per row; a standalone one
(window/group/join — its own scan) is not, so a `gate:` front-matter line declares a cheap
**necessary precondition** (a boolean over the detector's `source:`), probed as `SELECT 1 FROM
<source> WHERE <gate> LIMIT 1` before the expensive detector — skip when no row matches (the
standalone analog of the predicate index). Soundness is the author's assertion (an *absence*
detector must not gate on the thing it counts); a skipped detector is reported, never silent
(`CompiledCorpus.GatedSkipped`).

### PREPARE++ phasing

Phases 1–7 are **DONE (MVP)**: zip/logical-keyspaces + source routing (1); late-binding manifest
resolver, fail-loud, `glue.Binding`/`OpenSessionBound` (2); `OpBroadcast` (3); predicate index +
CSE (4); temporal-as-optimization / ASOF merge (5, `DESIGN-merging.md`); `CorpusCompile` /
`CompiledCorpus` classifying fuse/standalone/reject with a differential test gating
fused∪standalone == per-detector SQL (6); recipe format + golden-fixture CI + `.multi` family
(7). The core pipeline is end-to-end. Remaining is the *authoring/caching/packaging* layer:
SHA-keyed build cache, the embed-source analyzer binary, the per-detector projection envelope
(fused result is the whole matched row today), standalone-scan sharing (`DESIGN-sorting.md`
proposes fusing window/context + temporal detectors onto a shared sorted-stream substrate), the
upper binding-resolver rungs, and the re-run **delta report** (keyed by bundle-fingerprint +
corpus-SHA — the killer feature for re-runs).

## Open questions

- **Embed-runtime size** — how small can the minimal datastore-runtime library be carved from
  glue/records/cbq (drop parser/planner; drop `expression` for Go-friendly queries)? Decides
  whether embed-source is practical.
- **The pipe protocol** — multiplex vs nested request/response for cursors under nested-loop
  joins / correlated subqueries (deadlock risk); framing (reuse `ValsEncode` vs a versioned
  envelope with types/labels/warnings/stats); columnar/Arrow across the boundary (copy vs
  shared memory); what bakes into the child vs stays parent-side (`with-recursive`, `expr-scan`,
  `agg-*`); merging parent-side (I/O) + child-side (compute) stats snapshots without
  double-counting.
- **Toolchain policy** — when is `go build` permitted (sandbox/permission)? A prebuilt "thin
  runtime" module would make child builds fast + hermetic.
- **Corpus granularity — genuinely unsettled.** One giant fused program per bundle (max CSE) vs
  sharded by source (bounds recompile, index within each shard)? Tradeoffs unclear.
- **Version-aware corpus organization** — version tags/dirs, a tolerance idiom the authoring
  guide standardizes, provenance-tagging findings by target version, and how MQO CSE folds many
  near-identical per-version detectors.
- **Log time model** — normalize wildly different log timestamp formats/zones into one sortable
  key for the ASOF merge: per-source parse spec, or inferred?

**See also** [DESIGN-cep.md](DESIGN-cep.md): the MQO/`.multi` shared-scan stack described here is
the *hard half* of a complex-event-processing engine (many standing rules over one flow); that note
sketches the other half (unbounded source + continuous emit + incremental state), the agentic
"corpus authored by an AI agent" angle, and the market landscape.
