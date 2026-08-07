# Design: Integrating Indexes into n1k1

## Status & remaining TODOs

_Last reviewed: 2026-07-25._

GSI-like **secondary indexes** (bbolt-backed + an in-memory backend for WASM/opt-in),
**covering scans**, and **FTS via embedded bleve** (`SEARCH()`, score/meta, declared
mappings, flex path) all ship — declared in `.n1k1/catalog.json`, advertised to cbq's
planner by **wrapping** the file datastore (zero fork edits), controlled by
`-index=lazy|eager|off|mem` and the `.index` dot-command family. No dependency on cbft,
cbgt, n1fty, or cbauth. Companion: `DESIGN-data.md`.

**Remaining (headline TODOs):**
- [ ] Incremental index maintenance (insert/update/delete) — v1 is rebuild-on-open, gated by
  a coarse (file-count + newest-mtime) freshness signature (`.index rebuild` forces it).
- [ ] Predicated `COUNT(*)` / `CountIndex` pushdown — **blocked on exact-spans** (`Index2`
  proved necessary but not sufficient; two prototypes reverted — see §COUNT).
- [ ] Array/object index shapes (`ARRAY … FOR … END`, `ANY`/`UNNEST`) — unsupported; the
  advisor skips any path crossing an array.
- [ ] Secondary indexes require the classic `<ns>/<keyspace>` directory layout; flat/
  single-file/glob layouts advertise none (`.index create` refuses a flat datastore).
- [ ] Eager wildcard GSI + adaptive auto-index (`.index auto`) — need fork-side planner work /
  workload logging (research, §"index everything").
- [ ] `CREATE`/`DROP INDEX` DDL unwired (`VisitCreateIndex` = `NA()`) — define via `catalog.json`.
- [ ] Fingerprint / zone-map manifest + O(1) `COUNT` from `doc_count` metadata.
- [x] Per-field FTS analyzers/types — a `kind:fts` def can carry a raw bleve index-mapping JSON
  (`"mapping"`); the `"keys"` shorthand (map these fields as text) stays for the common case.
  *Remaining:* a `.index create` DSL for mappings (JSON only) + analyzer-aware flex translation.

## Core mechanism

n1k1 takes cbq's **plan**, not its runtime: `PlanStatement` calls the real `planner.Build()`,
so index selection is decided entirely by cbq's planner from **what the datastore advertises**
(`Keyspace → Indexer → Index`/`FTSIndex`). n1k1 advertises n1k1-built index objects by
**wrapping** the file datastore's keyspaces (appending an extra indexer) and runs their scans
over its own `[]byte` engine — **zero fork edits**. (A `datastore/file/file.go` `var`-hook seam
was the original design; it's superseded by wrapping and kept only as a fallback.) Pipeline:
```
SQL++ → ParseStatement → PlanStatement (cbq planner.Build) → plan.Operator tree
      → conv.go (plan.Visitor) → base.Op tree → engine.ExecOp → glue scan/fetch ops → rows
```
Plan-op selection is by interface assertion (`planner/spans_term.go:CreateScan`): an index
implementing only base `datastore.Index` forces `plan.IndexScan`, which `conv.go:VisitIndexScan`
already converts (`IndexApiVersion` is irrelevant — the interface gates the choice). GSI
sargability is core-planner (`RangeKey()` expressions); FTS sargability is externalized in
`datastore.FTSIndex` (a small in-process shim replaces n1fty's remote-cbft executor with
`bleve.Index.Search()`).

## Secondary index (`si`)

A **local** secondary index (`glue/idx_si*.go`, type `secondaryIndex`), advertising
`Type()==datastore.GSI` (cbq's ordered-range enum that drives sargability) — distinct from the
GSI *service*.

- **Key encoding** (`idx_si_encode.go`) — order-preserving, self-delimiting: each bbolt key is
  `encode(keyValue) + 0x00 + docID` (numbers via the IEEE-754 order-preserving transform;
  strings/containers `0x00`-escaped). **bbolt byte order == N1QL collation order, so a real
  `Cursor.Seek` prunes range scans.** ⚠ **Collation correctness is the highest-risk area** — the
  always-present residual `Filter` enforces the exact predicate, so boundary imprecision is a
  slightly-wider walk, never a wrong answer.
- **Catalog** (`idx_si_catalog.go`) — `.n1k1/catalog.json` `{indexes:[{name, keyspace, keys[],
  where?}]}`, parsed via `n1ql.ParseExpression`; `defHash` = short hash of the normalized def.
  Missing sidecar ⇒ no indexes.
- **Storage: bbolt** (`go.etcd.io/bbolt`, MIT) — B+tree, persistent, `Cursor.Seek`/`Next` give
  the ordered iteration `Scan()` needs, one file per index. (rhmap rejected — `Visit` is
  hash-bucket order, no range scans. Columnar formats are for *coarse* pruning [zone maps,
  `DESIGN-data.md §5`], not an ordered secondary index.)
- **In-memory backend** (`idx_mem.go`) — bbolt-free: reuses the catalog/encoding/span-scan/
  dispatch, swaps the B+tree for a sorted `[]entry` binary-searched per span. The **only** path
  in the WASM build (`idx_wasm.go`) and opt-in natively (`SecondaryIndexMode="mem"`).
- **Build = rebuild-on-open (v1):** a full `records.Walk` scan evals key/where exprs per doc and
  inserts `encode(keyValues)+docID`, validated by a **source signature** (file count + newest
  mtime). ⚠ Freshness is coarse — a change keeping both identical won't rebuild; `.index
  rebuild` forces it. A **process-global cache keyed by bbolt path** opens/builds each index
  once (bbolt takes an exclusive file lock, so re-opening per Store would deadlock).
- **`Scan()` contract:** `defer conn.Sender().Close()`; `Seek` to `span.Range.Low`; iterate
  ascending; decode key, stop at `High`, honor `Inclusion` via `Collate`; `SendEntry(&IndexEntry{
  PrimaryKey: docID})`; respect `limit`. `EntryKey` is filled only for a covering scan.
- **Composite (multi-key) indexes work** — the self-delimiting encoding makes prefix matching
  correct, so leading-key-only / full-key / leading+range / IN predicates all use the index.

⚠ **Hard-won footguns (learnings that changed the plan):**
- **Covering scans (the biggest surprise).** cbq turns a query whose projected/filtered fields
  are all index keys + `META().id` into a **covering `IndexScan` with no Fetch**, rewriting
  field refs into `expression.Cover` nodes that read a per-value cover slot n1k1 never fills →
  **every field came back MISSING**. Covering is on by default, undisableable without a fork
  edit. Fixes, n1k1-side: `glue/expr.go:stripCovers` peels every `Cover` to its underlying
  expr before eval; and **true covering execution ships** — when the index is *coverable*
  (`indexDef.coverable` — every key a plain field ref, no filter-covers), `VisitIndexScan` emits
  a **`datastore-scan-index-cover`** op that reconstructs the projected doc from the decoded
  index-key values (`reconstructCoverDoc`), **no fetch**. A non-coverable covering scan
  (expression key like `LOWER(name)`, partial index, non-n1k1 index) falls back to scan+fetch
  (`VisitIndexScan` synthesizes a `datastore-fetch` when `len(Covers())>0`).
- **Multi-span sender close.** `DatastoreScanIndex` used to run a goroutine per span, each
  `Close`-ing the shared sender → an IN-list / same-field-OR / `DistinctScan` had the first span
  truncate the drain. Now all spans run in **one goroutine** sharing the sender, closed once,
  deduping docIDs (`secondaryIndex.scanSpan`).
- **Build/scan number-encoding must agree.** A JSON number reaches build vs predicate-bound
  paths as different Go types (`float64` vs `int64`); `toFloat64` must handle both, or numeric
  scans return nothing.
- **Intersect/Union/Distinct scans.** A predicate over two indexed fields makes the planner emit
  `IntersectScan` (AND) / `UnionScan` (OR) / `DistinctScan` (same-field OR/IN). In `conv.go`:
  Intersect → convert the **first** child, residual `Filter` enforces the rest (a superset the
  Filter narrows); Union → fall back to a full records scan + Filter (can't drop an OR branch);
  Distinct → convert the inner scan (spans disjoint).

## FTS via embedded bleve (`idx_fts.go`)

`SELECT … WHERE SEARCH(ks, "q")` runs against an embedded `bleve.Index` (built from a full scan
on open, same freshness check) — no cbft/n1fty, zero fork edits (set `useFts=true` in `stmt.go`,
provide the `datastore.FTSIndex` shim). `Sargable` returns `exact=true` so the planner drops the
residual predicate; `Search` pushes `IndexEntry{PrimaryKey: hit.ID, MetaData: hit.Score}`.
`VisitIndexFtsSearch` emits one `datastore-scan-fts`.

⚠ **`DatastoreScanFTS` fetches the matching docs ITSELF** (not via a following `plan.Fetch`),
for two reasons, each a shipped fix:
- **The hit score is only available at the scan** and would be lost across a separate fetch — so
  `VisitFetch` passes through after an FTS scan. Score/meta ride the `^smeta` label (bound under
  `value.ATT_SMETA` by `ConvertVals`) so `SEARCH_SCORE`/`SEARCH_META` read it.
- **An FTS hit id is a framing RECORD id** — for a multi-record file a container id
  (`<relpath>#<line>@<offset>`) that cbq's `Keyspace.Fetch` **can't resolve**. Fetching via
  `Keyspace.Fetch` silently returned zero rows on every multi-record keyspace (and made an FTS
  index turn flex-served equality predicates into empty results) — **IDEA-0030**; the fix uses
  n1k1's byte-path reader (same container/native dispatch as `datastore-fetch`).

Two more shipped gotchas: **`SEARCH(<keyspace-name>, "q")`** (naming the keyspace, not its FROM
alias) is handed to us as `field=<keyspace-name>`, a field no doc has → row-emitting projections
silently returned 0 while COUNT worked ("counts but can't fetch", **IDEA-0033**); `bleveQuery`
now treats `field == <keyspace name>` as the whole-keyspace search it means. And **field-scoped
wildcard/prefix/fuzzy** (`SEARCH(ks.field, "x*")`, **IDEA-0035**): the dynamic mapping's default
(standard) analyzer makes each string one whole lowercased token (case-insensitive but not
substring); whole-doc `x*` already routed through bleve's query-string parser, but field-scoped
used a match query (no wildcards) → matched nothing; `fieldStringQuery` now detects the markers
and builds the Prefix/Wildcard/Fuzzy term query, lowercased to match.

**Mappings:** empty `keys` = dynamic (index every field); listed `keys` build a non-dynamic
mapping (nested dotted paths → sub-documents). A def may instead carry a **`"mapping"`** field
= raw bleve index-mapping JSON (full analyzer/per-field-type control; mutually exclusive with
`keys`, fts-only). The catalog file stays bleve-free (WASM-included) — it holds the raw bytes and
validates them through the `ftsMappingAnalyze` hook `idx_fts.go` registers (which also derives
the mapped-field set + dynamic flag `fieldIndexed` needs for flex; conservative — a false
positive drops valid rows). Mapping bytes fold into `defHash` (editing triggers a rebuild).

**Flex / implicit predicates:** `SargableFlex` lets a plain `WHERE` (no explicit `SEARCH()`) be
served by bleve — it translates the sargable part (`Eq`/`LT`/`LE`, `AND`/`OR`; `>`/`>=`→swapped)
into a bleve query DSL, wrapped as synthetic `SEARCH(ks, <query>)`. ⚠ **Correctness is
independent of translation precision:** we never set `FTS_FLEXINDEX_EXACT`, so the planner keeps
the original predicate in the residual `Filter` (which n1k1 re-evaluates) — the bleve query need
only be a *superset*. An `AND` may drop an untranslatable conjunct (still a superset); an `OR`
bails if any disjunct is untranslatable; a wholly untranslatable predicate → records scan.

## CLI control

`-index=lazy|eager|off|mem` (process-global `glue.SecondaryIndexMode`, re-read per
`maybeSecondaryIndexes`; all give identical *results*, differing in build *timing*): **`lazy`**
(default — build each on first use); **`eager`** (build every catalog index up front, concurrent
one-worker-per-CPU via a per-path `indexSlot` — a `once` opens the OS-lock-contended bbolt file,
a per-slot mutex serializes rebuilds; streams `IndexBuildEvent`s to a live per-index TTY bar);
**`off`** (unwrap the datastore — the A/B baseline + escape hatch); **`mem`** (the in-memory
backend; not on the flag, set programmatically / forced by WASM).

`.index` family: `list` (one line per index, builds any not-yet-built for live stats), `show
<name>`, `rebuild [<name>]` (force past the freshness signature), `suggest [<keyspace>]` (the
advisor — samples docs, prints an editable `catalog.json` fragment; a `gsi` def for selective
scalar fields, a `kind:fts` def for text fields, each tagged with a `why` the loader ignores),
`create …` (DSL or JSON fragment — appends to `catalog.json` and builds; explicit user intent,
so writing the catalog is fine — the single-writer rule bars only *background* rewriting),
`help`. Per-index knobs (collation, value-size cap, `defer`, CBO stats) belong in `catalog.json`
(properties of a def), not flags.

## Sidecar layout (`.n1k1/`)

A dataset accumulates many independent derived artifacts (GSI + FTS indexes, zone-maps, count
caches) that must coexist, build/drop/GC independently, swap atomically, and match back to the
exact def + source state.
```
<dataRoot>/.n1k1/
  catalog.json                       # source of truth: all defs + config fingerprint
  <ns>/<keyspace>/
    manifest.json                    # source fingerprints + zone-maps (DESIGN-data §5)
    idx/<name>__<kind>__<defhash>/    # one dir per built instance
      meta.json                      # def, kind, key exprs, format_version, built_from, state, stats
      data.bolt | bleve/ | zonemap.cbor | count.json   # payload per kind
    tmp/…<gen>/                       # in-progress build; atomically renamed in
    trash/                           # dropped/orphaned, lazily deleted
```
`kind` = `gsi|fts|zonemap|bloom|wildcard|count` (lets schemes coexist on the same keyspace/key).
**`defhash`** (short hash of the normalized def — key exprs + WHERE + options + collation/format
version) is the workhorse: a changed def ⇒ new dir (old orphaned + GC'd, no in-place corruption);
"is there a built index for this def?" is a dir-existence check; `catalog.json` is
reconstructable by scanning `idx/`. Build into `tmp/…<gen>/` then **atomic rename** so readers
never see a half-built index. ⚠ **Single-writer rule:** `catalog.json` comingles source mappings
+ index defs safely *only because it stays single-writer* — declared intent (human/generator-
authored). Everything machine-managed (build-state, stats, adaptive/auto indexes) lives in
self-describing per-instance `meta.json`, **never written back into `catalog.json`** (an adaptive
index that rewrote it would break the property). With encryption-at-rest (`DESIGN-data §6`),
artifact payloads are encrypted with the dataset DEK; `meta.json` records the wrapping key id.

## Sidecar location: `--index-store` (read-only datastores)

The sidecar (`catalog.json` + built indexes + freshness state) defaults to `<dataRoot>/<sidecar>`,
but **`--index-store <dir>`** (`glue.SetIndexStore` → `sidecarRootFor`) relocates the whole sidecar
to a writable directory — so a **read-only** datastore (a mounted snapshot, an archived bundle, live
state owned by another process) can still be indexed. Only the sidecar moves; source records are
always read from the data root. Same rule as the cursor store: *n1k1 never writes inside a bundle it
doesn't own*. Default (empty) keeps the sidecar under the data root, backward-compatible with
existing in-bundle catalogs. Guard: `TestIndexStoreRelocatesSidecar`. (ISSUE-12 §3.)

## Freshness & incremental maintenance (future)

Today freshness is a coarse **`(file-count, newest-mtime)` signature** (`sourceSignature`) and any
staleness triggers `buildIndex`'s **full rebuild** ("v1 rebuilds the whole index in one
transaction"). On an **append-only** corpus (e.g. `~/.claude/**/*.jsonl`, appended by every live
session) this makes an index a net loss — a single appended line re-indexes everything, measured
~19× slower than the plain scan, and the bbolt file grows because freed pages aren't reclaimed
(ISSUE-12 §1).

The differentiator (n1k1 is **stateful across runs**): maintain indexes **incrementally from per-file
byte watermarks**, the same machinery the census/cursors already use (`records` position
`path#line@offset`). Append-only is the easy case — existing entries never change, so it's
**insert-only**: store per-file offsets in the index's own bolt db instead of the mtime sig; on an
append, scan only the tail past each watermark, evaluate the key exprs on the new records, insert,
advance. Design notes:

- **Truncation/rewrite guard** (required): if a file shrinks or its prefix changes, watermarks are
  invalid → full-rebuild just that file (the same check a cursor needs).
- **Cover FTS too** — `idx_fts.go` has the identical coarse-freshness rebuild.
- **`-index stale-ok`** interim mode: serve the existing index and report how far behind it is
  (`indexed as of Ns ago`). Makes indexes usable on a live corpus before the incremental path lands.
- **Cursor-store unification** (follow-on): an index as a cursor consumer, sharing one watermark
  implementation + atomic-commit story with the census. Start with the index owning its own
  watermarks; unify only if it proves cheaper.

Tracked in TODO.md. **UPDATE: the incremental path SHIPPED** for both SI (`glue/idx_si.go`
`catchUpIndex` — water+fp in the index's own bolt meta, tail-only fold, one-tx commit,
anomalies → full rebuild) and FTS (`glue/idx_fts.go` `catchUpBleve` on STOCK bleve —
`Batch.SetInternal` carries the water in the delta batch). What follows is the next rung.

## Newest-first / partial index builds (design sketch, NOT built)

**The ask:** an impatient user starting a fresh index build wants the LATEST data queryable
soonest — so build "backwards": index the newest page first and step back in time, while
appends keep arriving. Prior art says both halves are respectable: CouchDB's `stale=ok`
serves a partially built view as an explicit per-query contract; ClickHouse `MATERIALIZE
INDEX/PROJECTION` serves the union of indexed parts + brute-force over unindexed parts
(always correct, monotonically faster); Lucene/ES/bleve are order-indifferent and the ops
practice around reindexing is exactly "live tail first, backfill history behind"; Loki
permanently serves scanned-unindexed-recent ∪ indexed-old. Postgres/Couchbase-GSI are the
opposite pole (index invalid until complete) — the conservative fallback.

**What already composes:** "more data shows up during the build" is SOLVED — that is the
shipped catch-up path, which extends the TOP of the indexed region. Backwards building adds
the symmetric bottom: a **floor watermark**, making index state a per-container WINDOW
`[floor, water]` (floor lives beside water in the same bolt meta / bleve SetInternal).
Catch-up moves `water` up (unchanged code); backfill pages move `floor` down; `floor == 0`
everywhere ⇒ today's complete index, drop the floor state. Same mergeable-fold discipline as
everything else, which is WHY it composes.

**The crux — a partial index must NEVER answer as if complete.** bleve is naturally safe-ish
(missing docs = missing hits; idf/scoring drifts while partial — the accepted ES-reindex
trade). But a partial SI consumed by the planner as authoritative silently DROPS every match
below the floor — a confident wrong subset, ISSUE-25's shape wearing an index costume. Only
two serving contracts are acceptable:

1. **Hybrid (ClickHouse-style, preferred):** index answers `[floor, water]`, the existing
   no-index scan path answers `[0, floor)`, union. Always correct; cost of the scan half
   shrinks as backfill proceeds. Planner change = "use both, split at floor".
2. **Disclosed window:** index-only serving with `index_coverage` (floor/water, fraction)
   stamped in the envelope, and refuse/warn when the query's range exceeds the window. For
   the actual impatience case — time-descending queries over the recent window — a
   newest-first partial index is EXACTLY complete for the range asked.

The silent third option (partial-as-complete) is the only bad variant; ruled out.

**Implementation wrinkles (why this is a design pass, not an afternoon):**
- *Cross-container newest-first ordering* needs time knowledge (sorted-source metadata /
  mtime); WITHIN a jsonl container, backwards byte-offset pages are cheap (seek, scan to the
  next newline).
- *Record ids embed line numbers* (`path#line@offset`) — unknowable when starting mid-file
  without counting from the top. Solvable (lazy line count on first full-file touch, or
  offset-anchored ids for backfilled entries), but it touches the id contract.

**Sequencing — the cheap first rung captures most of the value:** a **TIME-SCOPED index** —
"index everything newer than T, catch up forward, never backfill" (T aligned to container
boundaries: whole recent files ⇒ no mid-file wrinkles at all). Declared window + the
disclosure contract; the newest data is queryable almost immediately, and agent-exhaust
corpora rarely need deep history indexed. Paged backwards backfill (`.index backfill`
walking the floor down, resumable via the same watermark discipline, hybrid serving arriving
with it) is the optional second rung, not the entry price.

- **Whole-keyspace `COUNT(*)` done** — `VisitCountScan` de-optimizes to a records scan (like a
  primary scan); the `count(*)` group-aggregate rides the surrounding plan ops. `keyspace.Count()`
  (`len(ReadDir)`, `file.go:467`) and `Size()` (`:475`) exist. (O(1) count from a `doc_count`
  manifest is the future item below.)
- ⚠ **Predicated `COUNT(*)` via secondary index — BLOCKED on exact-spans (both prototypes
  reverted).** Count pushdown lives in the *covering* path (`build_scan_covering.go` →
  `build_scan_pushdowns.go`), gated on **`_PUSHDOWN_EXACTSPANS`**; a base (API1) index's spans
  are never marked exact (also why every base-`IndexScan` carries a residual `Filter`), so
  `plan.IndexCountScan` is never emitted. A second prototype implementing `datastore.Index2`
  (`RangeKey2`/`Scan2` + `VisitIndexScan2`) *did* make the planner emit `plan.IndexScan2` but
  still didn't drop the residual `Filter` or mark spans exact — so **`Index2` is necessary but
  not sufficient**. The predicated count visitors return `NA()` but that's currently
  **unreachable** (the planner won't emit them without exact spans), so it's not a live gap.
  Filter-elimination + count pushdown remain open pending a deeper trace (likely probes:
  `useCBO=true`, the `filterCovers`/`coveringScan` retention path, `Index3`/`IndexScan3`).
- **Manifest synergy** (`DESIGN-data.md §5`): once the manifest tracks per-file/partition
  `doc_count`, whole-keyspace/partition `COUNT(*)` is O(1) from metadata; predicated counts sum
  precomputed counts for fully-covered partitions and scan only boundary partitions.

## "Index everything": dynamic / wildcard / automatic (mostly research)

n1k1's full "index everything" posture = **bleve dynamic (text, free)** + **zone-maps/bloom
(cheap scalar pruning)** + **adaptive auto-index (hot scalar fields)** — no giant always-on
wildcard structure. Three tiers:
- **Tier 1 — always-on zone maps + optional per-file bloom** at the scan layer. Cheap,
  needs **no cbq planner changes** (pruning is a datastore concern). The manifest zone maps in
  `DESIGN-data.md §5`; the pragmatic default.
- **Tier 2 — adaptive auto-index** (RavenDB/Oracle-style): log the predicates the planner
  produces, auto-create an ordered index for hot field(s), GC unused. The created index is a
  **normal `RangeKey` index the planner already understands** (Phase-1 machinery, no
  wildcard-planner work) — the realistic medium-term path. ⚠ Signal: the b-tree auto-index win is
  **HIGH cardinality (÷ doc count) ∧ queried**, not low — a low-card field (`status`) matches a
  large fraction and barely beats a primary scan (it's for zone-maps/composite-leading-key
  instead). Eligibility: **scalar leaf, no array crossed** (a path crossing an array needs cbq's
  array index — a separate, harder class, flagged not auto-created). Sampling proposes
  candidates; the workload confirms which are queried. These estimates are what a future
  `Index.Statistics()` should return to feed cbq's CBO instead of `nil`. CLI ladder: `.index
  suggest` (advisor, shipped) → `.index create` (explicit, shipped) → `.index auto` (autonomous,
  later — writes a **separate machine-managed auto-catalog**, never the human one).
- **Tier 3 — eager wildcard GSI** (Cosmos/Mongo-style): a bbolt store keyed
  `encode(pathPrefix)+encode(value)+docID` (a dictionary maps field-path → short fixed-width
  prefix; the encoder must cap oversized values with a "truncated" marker since bbolt keys are
  bounded ~32 KB). Feasible to build; the hard part is **planner integration** — cbq's
  `sargableIndexes` matches a *fixed* `RangeKey()`, with no concept of a wildcard over arbitrary
  paths → needs fork-side planner work + inherits Mongo's caveats. Research item.

(Prior art surveyed: Cosmos DB auto-indexing, Mongo `$**` wildcard, ES/Lucene type-routed dynamic
mapping, Postgres GIN/BRIN, Parquet bloom, Oracle/Azure/RavenDB adaptive, SQLite transient
indexes, database cracking. Lesson: "index everything" should **route by inferred type** — bbolt
for scalars, bleve for text/geo.)

## Reference

**Affected files** (all real logic in n1k1; the fork carries only the tiny superseded seam):
`glue/idx_si*.go` + `idx_mem.go` + `idx_wasm.go` + `idx_fts.go` (index types, backends, catalog,
build, wrapping); `glue/datastore_scan.go` (`DatastoreScanIndex`, `reconstructCoverDoc`,
`DatastoreScanFTS`); `glue/conv.go` (`VisitIndexScan` + `-index-cover`, Intersect/Union/Distinct,
`VisitIndexFtsSearch`, `VisitCreateIndex` future); `glue/expr.go` (`stripCovers`, `stripSearch`);
`glue/stmt.go` (`IndexApiVersion`, `useFts=true`, wrapping registration); `cmd/n1k1` (`-index` +
`.index`, `indexprogress.go`); `go.mod` (direct `go.etcd.io/bbolt` MIT + `blevesearch/bleve/v2`
Apache-2.0). **Interface-drift now lands in n1k1** (a feature): a cbq rebase changing
`Index`/`Indexer`/`FTSIndex` signatures is a compile error in n1k1 (its natural owner), not
silent fork drift.

**Verification:** Phase 1 — confirm via `Result.Plan` an `IndexScan` (not `PrimaryScan`) and
results match the no-index run (`TestSecondaryIndex*`); covering — plan is
`datastore-scan-index-cover` with no `datastore-fetch`, results still match
(`TestSecondaryIndexCovering`); FTS — plan uses `datastore-scan-fts`, whole-doc/field/score/meta/
declared-mapping/flex all match with no cbft (`TestFTS*`).
