# Design: Data Sources for n1k1

_Last reviewed: 2026-07-29._

How n1k1 ingests source data — file formats (JSONL, multi-doc JSON, CSV/TSV, YAML,
Parquet/Iceberg, extracted office/PDF), directory layouts, compression/containers, how a `FROM`
term resolves to a set of files, synthetic `META().id`s, `_meta` injection, and the `.n1k1/`
sidecar that keeps derived artifacts in sync. Companion to `DESIGN-indexing.md` (read together —
see "Relationship"). Inspired by DuckDB, Spark, Athena/Glue, ClickHouse, Iceberg.

**The load-bearing decision:** new datastore behavior lives entirely **n1k1-side in thin glue
seams over `[]byte`, not in the forked `couchbase/query` (cbq) runtime** — and everything shipped
so far needed **zero fork changes**. n1k1 reuses cbq for its parser + *planner output* (the
`plan.Operator` tree), not its execution runtime (which boxes a `value.AnnotatedValue` per
tuple/field — the opposite of n1k1's `base.Val = []byte` buffer-reuse engine). Two organizing
axes: *layered concerns* (decoder / layout / compression / derived-artifacts, each independently
pluggable) and *allocation discipline* (no per-value boxing).

**n1k1 core stays domain-agnostic** — it knows nothing about any file family; all format/domain
knowledge lives in pluggable recipes (§4). The recurring worked example is a Couchbase
`cbcollect_info` support bundle (the case that drove `DESIGN-prepare.md`'s PREPARE++ corpus), but
the same mechanism serves any messy tree (SEC filings, sensor streams, genomics, access logs) —
the bundle is an *example*, not a built-in.

## Status

**Done (all `records/` + `glue/`, `//go:build n1ql`, zero fork changes):**
- **Discovery/layout** — multi-file / flat-root / single-file / grab-bag keyspaces + backtick
  inline globs (`` FROM `./data/**/*.json` ``); **multiple data sources on one command line** (or a
  `-sources` JSON/YAML/TOML config file) → one keyspace per source, joinable in a single query,
  **federating heterogeneous kinds** (local dir/glob/file + local/remote Iceberg table + remote Parquet)
  (§2, `glue.OpenSessionSources`/`LoadSources`).
- **Decoders** — JSONL, multi-doc JSON, CSV/TSV, YAML, office/PDF/media (opaque-document path);
  Parquet as a queryable keyspace with column-projection pushdown + footer-stats vectorized aggs
  (`DESIGN-col.md`); transparent gzip.
- **Fetch/scan** — native byte-path fetch + per-request doc & scan-key caches; `_meta` injection
  (incl. `byte_offset`/`line_start`/…); `COUNT(*)` pushdown.
- **Extract** — the two-phase `describe`/`extract` recipe provider (`*.extract.js`, matched by
  ext+regexp), framing `line`/`multiline`/`json`/`section`/`whole`/`opaque`, typed capture fields,
  sidecar-memoized describe; the sorted-source contract (`SortedSourceMeta`) driving the K-way
  near-sorted merge + ASOF (`DESIGN-merging.md`/`DESIGN-sorting.md`).
- **Materialization** — `CREATE [OR REPLACE] TEMP KEYSPACE … AS <select>` + `DROP` (spilling via
  rhmap `store.Heap`); `INSERT INTO … SELECT` file-materialize (with `OPTIONS` write modes +
  `RETURNING`; `.parquet` target incl. VARIANT write-back).
- **Table formats + object stores (§7/§8)** — Iceberg read-only (cgo-free via `iceberg-go`) with
  projection/predicate/partition pushdown + time-travel; remote Parquet & Iceberg over
  `s3://`/`gs://`/`abfs://`.

**Remaining (proposal / not built):** the catalog/sidecar (`.n1k1/catalog.json`) +
change-detection manifest with Merkle rollup, zone maps, append-only tail offsets (§5);
predicate/partition pushdown reaching the scan layer (lights up hive/date pruning F/G + pushdown
through a VIEW); catalog-defined query VIEWs (expansion + branch pushdown; `UNION ALL` already
landed) + materialized views; zstd decode (walker recognizes `.zst`; decode is a stub) + `.zip`
container; encryption-at-rest; full column-batch Parquet execution (`DESIGN-col.md`); catalogs
beyond a filesystem/object-store metadata path (REST/Glue); a `[]byte`-oriented zero-copy CSV
reader + the allocs/op benchmark gate; per-source **sortedness** for multi-source (§2 — per-source
`-formats`/`namespace` + the federation of kinds + the `-sources` config file have all shipped).

## Relationship to `DESIGN-indexing.md`

One design split in two; must stay coherent. **This doc (data):** source formats/layouts, `FROM`
resolution, compression, extraction, synthetic `META().id`s, the change-detection manifest
(fingerprints + zone-map *data*). **Indexing doc:** how the cbq planner comes to *use* an index
(GSI sargability, FTS), COUNT(*) pushdown, index tiers, and the canonical `.n1k1/` sidecar layout.
Touchpoints: (1) fork = plan-time metadata only, execution in n1k1 — both keep the fork thin via
`engine.ExecOpEx` IoC; a keyspace's existence is faked by **wrapping** the fork's datastore with
`datastore/virtual` (`glue/flat.go`), not a `DiscoverKeyspaces` seam. (2) `catalog.json` holds
source mappings **and** declared index defs — safe only because it stays **single-writer**;
adaptive index state lives in per-instance dirs. (3) ⚠ **Zone maps are the load-bearing shared
artifact, but pruning is only "no-planner-change" once the predicate REACHES the scan** (§5
caveat). (4) Doc-IDs match: columnar `file#row_position` = `<relpath>#<offset|line>`.

## Architecture: where the code lives

`FROM default:orders` is resolved by cbq's planner, which asks the `datastore.Datastore`
interface for keyspace metadata. Two needs hide behind "the datastore" — only one touches the
fork:

- **(A1) Plan-time keyspace metadata — n1k1 fakes it, NO fork change.** The planner must believe
  the keyspace exists with a primary index. n1k1 **wraps the datastore** to advertise a *synthetic*
  namespace + keyspace, reusing the fork's importable `datastore/virtual`
  (`virtual.NewVirtualKeyspace` + `NewVirtualIndex(isPrimary)`) so it emits a `PrimaryScan`
  (`glue/flat.go`; the synthetic keyspace's `RecordsDir()` points records-scan at the root).
- **(A2) Execution-time scan/fetch — already n1k1's, no fork seam.** `conv` lowers
  `PrimaryScan`/`Fetch` to n1k1 `datastore-scan`/`datastore-fetch` `base.Op`s run by
  `glue.DatastoreOp`, which read the directory and decode records directly to `base.Val = []byte`,
  bypassing cbq's boxing. All decoder/layout/doc-ID/compression code is ordinary n1k1 (the
  `records` package), registered via IoC (`engine.ExecOpEx = glue.DatastoreOp`, **set once in
  `init`** — per-request variation rides `Ctx.Pipe`, commit 19c2fbfa).

Get right: the synthetic keyspace is *minimal* (primary index only, `Count()` may be lazy/0 while
`useCBO=false`); prefer hanging the hook off the store/namespace instance.

**Compiler compatibility (don't break the Futamura path).** n1k1 is an interpreter **and** a
compiler; if FROM-file scans keep flowing through the existing `datastore-scan`/`datastore-fetch`
op path they compile for free (ops carry only int `Temps`-indices as Params; the live datastore
arrives at runtime via `SetupCompiled*` re-planning). Consequences:
- ⚠ **Do NOT introduce new engine scan *kinds* for new formats.** A `parquetData` op would fork
  the interpreter/compiler paths again. Decode the format **inside the existing glue ops** so the
  op *kind* is unchanged and the differential keeps passing.
- ⚠ **Anything that can't be a Go literal arrives via `Temps`** (a live `RecordSource`/decoder
  isn't bakeable). Format/layout *choices* (static strings/ints) go in Params, live handles in
  `Temps`. Test hook: the queryCases differential harness (`test/query_compiler_test.go`).

## Design principle: separate the concerns into layers

Decouple four things n1k1 originally fused into "a keyspace is a directory of json files":
(1) **record format / decoder** (bytes → rows); (2) **layout / discovery** (a FROM term → the set
of files, optionally deriving path-based partition columns); (3) **compression / container**
(transparently un-gzip/un-zst, or enumerate a `.zip`, beneath the decoder); (4) **derived
artifacts** (indexes/caches/extracted text + change-detection metadata). Each independently
pluggable.

## §1 Formats, the allocation model & the read/fetch path

`RecordSource` is shaped **for buffer reuse**: `Next(rec *Record)` where `rec` holds `[][]byte`
field slices borrowed from a reused read buffer, valid only until the next call — *not* a
`Read() (value.Value, error)` that allocates per row. ⚠ **State the borrow/lifetime contract
explicitly — "copy to persist"** — retaining a borrowed slice past the next read corrupts data
(what `base.Val`'s "usually immutable" already assumes). Allocation behavior is a **selection
criterion on par with correctness**; treat allocs/op (`-benchmem`) as an acceptance metric per
decoder ("allocs per row" near-constant regardless of file size).

- **JSONL / JSON → `buger/jsonparser`** (direct dep): `[]byte` sub-slices, no map materialization,
  near-zero-alloc end to end.
- **CSV/TSV** — on Go's `encoding/csv` (correct quoting/escaping) but it allocates field strings
  per row; the zero-copy `[]byte`-borrow reader is a later optimization. Each row decodes to **one
  JSON object keyed by the header** (light int/float/bool inference), so CSV rides the
  opaque-document path (§2).
- **Parquet: correctness shipped, full vectorization ongoing.** n1k1's engine is row-at-a-time, so
  feeding Arrow *columnar* RecordBatches means transposing to rows. We treated Parquet as a
  *correctness* feature first (`records/parquet.go`: transpose-to-rows), then added the partial
  wins that don't need row-lane surgery — **column-projection pushdown** (read only referenced
  columns via `ColumnsProjector`/`ColumnsSource`, reusing cbq's `EarlyProjection`) and
  **footer-stats vectorized aggregates** (COUNT/MIN/MAX + SUM/AVG over nullable columns from
  metadata, zero data-page reads). Full column-batch execution is the remaining perf win
  (`DESIGN-col.md`). VARIANT columns are supported end to end (`DESIGN-variant.md`).

**The fetch path is where allocation broke (measured 2026-07).** A 3-way nested-loop self-join
(262K rows) allocated ~931 MB (only ~3 MB live) — pure GC churn: ⚠ **~71% was
`glue.DatastoreFetch` → the fork's file `Fetch`, which materialized `value.AnnotatedValue`s and
re-parsed with `encoding/json.Unmarshal` (eager boxing)**, amplified O(|L|×|R|) by the join
re-fetching. Fixed entirely inside n1k1:
- **Native byte-path fetch (`glue.DatastoreFetch`)** — reads each doc into a reused growable buffer
  via `io.ReaderAt.ReadAt`, yields raw JSON as `base.Val`, no boxing / no standard-JSON parse.
  Dispatch by key form: a container id `<relpath>#<line>@<offset>` seeks into the multi-doc file; a
  plain key reads `<dir>/<key>.json`. Keyspace→dir resolution memoized (`N1K1_FETCH_CBQ=1` forces
  the old path). ~2.0 GB → ~917 MB.
- **Per-request doc cache** (`fetchCache`, two-level dir→key→immutable copy, bounded 64 MiB,
  `N1K1_FETCH_NOCACHE=1` off) → ~541 MB.
- **Per-request scan key-listing cache** (`scanKeyCache`; `DatastoreScanIndex` serves a full
  `#primary` scan natively — list+cache once, bypassing cbq's per-invocation `readdir`; ranged
  spans + secondary indexes keep the cbq path). End to end **~2.0 GB → ~152 MB (~92%), GCs
  420 → 31.**
- **Fetch-by-key into containers** — the byte offset is baked into the id at scan time (JSONL
  `<relpath>#<line>@<offset>`, `---` YAML `<relpath>#<i>@<offset>`), so `USE KEYS` / `ON KEYS` /
  non-covering fetches are O(1) seeks. ⚠ A `.gz` offset is into the *decompressed* stream, so
  compressed / non-seekable containers (`.gz`, CSV rows, JSON-array elements) omit `@<offset>` and
  aren't key-fetchable yet.

⚠ **mmap vs read-into:** we chose **read-into a reused buffer** (`io.ReaderAt.ReadAt`) over mmap —
amortized zero-alloc without mmap's lifetime hazard (**a retained mmap sub-slice dangles into
unmapped memory → delayed SIGBUS**), works for large files (read only the needed range), skips 4 KB
page waste on tiny files, and doesn't help compressed/extracted inputs anyway. Rule of thumb held
in reserve: mmap only for a packed segment of many docs.

**Push down what the query needs** — the cheapest read is the one you skip: `_meta`-only queries
(and bare `COUNT(*)`) answer from `readdir`/`stat` with no file read; jsonparser's `Get(path…)`
partial-decodes only referenced paths (thread the path set from `conv`); `ReadAt` fetches only the
byte ranges needed (a Parquet column chunk, a manifest-known offset).

## §2 Directory layouts & FROM-term resolution

Three resolution modes, increasing power:

**Mode 1 — Convention (zero-config), backward-compatible.** Keep
`<dir>/<namespace>/<keyspace>/…`, relaxed so a keyspace directory is the **union of all its
supported files across many files**, recursing subdirs. Shipped extensions: a **flat root** (a
bare data dir is one keyspace named by basename — ex. B); a **single file** (`n1k1 … events.jsonl`
→ a one-file keyspace named by stem, extensions stripped — B2); a **grab-bag directory** (one
keyspace per top-level structured file, by stem — B3).

**Mode 2 — Explicit table functions / globs in FROM (`read_csv('sales/*.csv')`) — blocked on a
grammar fork; deferred.** The fork's parser rejects it and there's no table-valued-function
machinery in `algebra/`; patching goyacc + a `FromTerm` node + planner is the merge-hostile change
we avoid.

**Mode 2b — Backtick-quoted glob as a keyspace name (✅ SHIPPED — the fork-free inline glob).**
`` FROM `./data/**/*.json` `` — backticks make it a single quoted identifier, stopping the parser
splitting on `:`/`.`, so `.`/`/`/`*` pass through as a literal string; n1k1 recognizes a
glob-shaped name and expands it in a `maybeGlob` datastore wrapper (`glue/flat.go`) backing a
`virtual.NewVirtualKeyspace` whose records-scan unions the matches (`records.Walk`: `**` = recursive
walk, `*.json` = format filter). Still a `PrimaryScan` → compiles like any FROM. Decisions: (a) a
name is a glob only if it contains glob metacharacters (`*`/`?`/`[`) — a plain `` `orders` `` stays
ordinary; (b) base dir by **prefix convention** (no `$ROOT` sigil): `./…`/`../…` → CWD-relative,
`/…` → absolute, bare `foo/bar/**` → datastore-root-relative. ⚠ (c) absolute/`../` globs can read
outside the root — for a local CLI that's the user's own files (noted, not blocked). `**` needs a
doublestar matcher (Go's `filepath.Glob` lacks it).

**Mode 3 — Catalog / sidecar mapping (`.n1k1/catalog.json`) — the realistic power path
(proposal).** Maps a keyspace name to a root glob, format, partition columns (hive **or**
projected date templates à la Athena), compression. Handles the invisible-date-container case:
declare `ecommerce/{date:YYYYMMDD}/*.log.gz` with `date` a projected partition column so
`WHERE date >= …` prunes by *computing* directory names. Hive `key=value` auto-detects within any
mode; bare date partitions need a projection template (they're ambiguous).

**Lockdown flag (`-formats`)** (`records.ParseModes`) restricts discovery/decoding to an explicit
set (`-formats=json,jsonl`; no `recurse` ⇒ don't descend; no `gzip` ⇒ ignore `.gz`). The REPL's
`.formats` shows/sets it and persists into `catalog.json`. Precedence: flag > persisted > flexible
default.

**Integration gap: schemaless docs vs positional labels.** n1k1's engine identifies fields by
**positional `base.Labels`**, but a multi-file keyspace with differently-shaped files has no single
fixed vector. The **opaque-document scan (default)** yields each record as a single self-value
(projections pull fields by name at expr-eval time), so heterogeneous shapes "just work" — why the
cheap cases shipped fast. Key insight that shipped: **CSV and office both ride the opaque path**
(emit a JSON object per row/doc), so `union_by_name` is trivial there. **Typed-label
reconciliation is forced only by columnar Parquet** (columns without a per-row JSON object) and by
hive/projected partition virtual-columns.

### Multiple data sources on one command line → multiple keyspaces (Phase 1 ✅ shipped)

Originally the CLI took **one** `<dataRoot>` (`main.go` used `flag.Args()[0]`; extra positional args
were ignored) and that root flattened into keyspaces via the modes above. Now n1k1 can be pointed at
**several independent roots at once** — one keyspace per source, so a single SQL++ query joins across
them — e.g. a local Google Drive mirror, `~/Documents`, and a local SharePoint mirror:

```
n1k1 'drive=~/Google Drive/**' docs=~/Documents/** 'sp=~/SharePoint/**/*.json'
# then:  SELECT d.title, s.owner FROM docs d JOIN sp s ON d.id = s.doc_id ...
```

**Key realization — this is mostly the existing `Binding` seam (`binding.go`), not new machinery.**
A `Binding` is already `map[logical-name → path/glob]`, already anchors bare patterns at the root
while honoring `./`, `../`, and **absolute** patterns (`globAbsPattern`), already treats a single
file as a degenerate glob, already **fails loudly** when a source matches zero files, and is already
enumerated by `.tables` (`bindingNamespace.KeyspaceNames`). `OpenSessionBound(dir, ns, Binding)`
threads it in. So a source list *is* a `Binding`; the work was turning CLI args into one — which is
exactly what `glue.OpenSessionSources`/`Source` (`glue/sources.go`) + `cmd/n1k1`'s arg parser
(`cmd_sources.go`: `parseSourceArgs`/`splitSourceArg`) now do.

- **CLI surface.** Each positional arg is a source, in either form:
  - `name=path` — explicit keyspace name (needed for globs, dotted names, or to avoid a collision).
  - bare `path` — name **derived** from the path's **deepest literal segment** (case preserved): a
    file's stem, a dir's basename, or a glob's base basename (`records.GlobBase`, the part before the
    first `*`/`?`/`[`). So a shared parent is never the name — `~/Drive/reports/**` + `~/Drive/ecommerce`
    → `reports` + `ecommerce` (not two `Drive`s), and `~/Drive/**/*.json` → `Drive` (the `*.json` suffix
    is irrelevant). Two sources deriving the **same** name is a **hard error** ("pass `name=` to
    disambiguate") — never a silent union of unrelated trees; a metacharacter-only glob (`**/*.json`,
    no literal base) has nothing to derive from and also requires `name=`.
  - `~`/`~user` expansion: the shell expands unquoted `~`, but a quoted glob (`'~/x/**'`) reaches us
    literally, so n1k1 must expand a leading `~` itself (small addition to `globAbsPattern`).
- **Root & namespace model.** With ≥2 sources (or any `name=`), there is no meaningful single root, so
  the datastore is built over a **synthetic empty root** and every source is a binding; bare *relative*
  sources anchor to **CWD** (not the empty root). All sources become **sibling keyspaces under the one
  `default` namespace**, so cross-source joins need no namespace prefix — the natural SQL++ ergonomics.
  (Rejected: source-per-namespace — it forces `FROM drive:docs` and muddies joins.) The one-bare-dir
  case stays byte-for-byte today's behavior (that dir is the root; conventional discovery). (A
  `-sources` config anchors its *relative* paths at the config file's dir instead of CWD — see below.)
- **Library API (✅ shipped).** `glue.Source{Name, Path}` + `OpenSessionSources([]Source, ns)` do the
  name-derivation + collision check + synthetic-root selection over the existing `Binding` seam;
  `LoadSources`/`OpenSessionSourcesFile` add the config-file path. Embedders get the same one-call power
  the CLI uses (parallels `OpenSession`/`OpenSessionBound`).
- **Provenance — which source did a row come from?** The **keyspace name is the source** (that is the
  point), and `_meta.path`/`_meta.name` (`.meta on`) pin the exact file. A `UNION ALL` across sources
  should carry a source tag — select a literal (`'drive' AS _src`) or project `_meta` — noted so results
  stay attributable.

**Two phases, both shipped (per-source options are the one deferral):**

- **Phase 1 — local multi-root (✅ shipped).** Drive-File-Stream / OneDrive / SharePoint sync clients all
  surface as **local filesystem mounts**, so the motivating example was nearly free.
  `glue.OpenSessionSources([]Source, ns)` classifies each source and builds one keyspace for it — `~`
  expansion (`expandTilde`), CWD-anchored abs-resolution, a bare dir expanded to a recursive `dir/**`
  union, basename/stem/glob-base name derivation (case preserved), collision-is-a-hard-error, and
  **fail-loud** on a zero-match local source. The CLI (`cmd_sources.go`) parses `name=path`/bare
  positional args (one bare path stays the classic single root).
- **Phase 2 — heterogeneous federation (✅ shipped; per-source options deferred).** Mixing source
  *kinds* — a local dir/glob/file, a **local or remote (`s3://`/`gs://`/`abfs://`) Iceberg table**, a
  **remote Parquet object** — works in one session. The realization that made it small: `flatKeyspace`
  is already a *unified* keyspace carrying the fields every kind needs (`{dir,glob}` local, `{dir,iceberg}`
  table, `{parquetURL}` remote), and `wrapFlatKeyspaces` already federates a **map** of them under one
  `default` namespace, and `KeyspaceRecordsOpen` already routes the scan on those fields. So
  `OpenSessionSources` just builds a `map[name]*flatKeyspace` (a per-source `sourceFlatKeyspace`
  classifier, reusing `records.IcebergTableMetadata` / `ResolveObjectStoreIcebergMetadata` /
  `SplitIcebergMetadataLocation`) over an **inert base** datastore (`inertBaseDatastore`, shared with the
  object-store builders) — **no new scan code**. **Per-source `-formats`** shipped too (a config source's
  `formats` restricts *that* keyspace's file eligibility): the scan reads a process-global
  `ScanWalkOptions`, so a `flatKeyspace` carries an optional override and `applyKeyspaceFormats` overlays
  it (keeping the live `.meta` + path prefix) at the **single `KeyspaceRecordsOpen` choke point** every
  scan/agg/vector path funnels through — plus `keyspaceFiles` (the one path that re-walks) so `.tables`
  counts match. It applies to file/dir/glob sources only (an Iceberg/Parquet source is single-format —
  rejected). **Per-source `namespace`** shipped too: a source can be placed under a namespace other
  than the session default, reachable as `FROM <ns>:<keyspace>` (which the cbq fork already parses +
  resolves -- no fork change). `OpenSessionSources` groups sources by namespace and CHAINS a
  `flatDatastore` per namespace (each serves its own, delegates the rest down); `.tables` lists every
  namespace's keyspaces (session default first, others namespace-qualified). Still reserved (parsed,
  rejected): per-source **sortedness**.

**REPL (Phase 1 ✅).** `.open <dir>` still opens one root; `.open <src>…` (2+ whitespace-separated paths,
or any `name=path`) opens multiple sources as keyspaces (`cmd_open.go`, same parser as the argv path),
and `.tables` lists them. A `.source add/list/rm` command family — each a new `Cmd` in the registry
(`cmd_registry.go`) — to attach/detach sources live is a natural follow-up.

**A `-sources` config file (✅ shipped for local sources).** The command line is the wrong surface for
richer source lists: the REPL splits `.open` on **whitespace**, so a source path with spaces
(`~/Google Drive`) can't be given interactively at all, and argv can't carry *per-source* options (this
source is `-formats=parquet` + sorted-by-`ts`; that one is an `s3://` Iceberg table with credentials).
The fix is a declarative config file — and n1k1 already **reads JSON/YAML/TOML natively**, so it parses
its own config with its own decoders (`records.DecodeConfigFile` → canonical JSON → the struct):

```yaml
# sources.yaml  —  n1k1 -sources sources.yaml   (or  .open @sources.yaml)
sources:
  drive:  { path: "~/Google Drive/**" }   # object form (space-in-path: fine, it's a quoted string)
  docs:   "~/Documents"                   # string shorthand (just a path)
  events: { path: "s3://…/events" }              # a remote Iceberg table source
  logs:   { path: "~/logs", formats: "jsonl,gzip" }   # per-source -formats lockdown (this source only)
```

Shipped: `-sources <file>` and `.open @<file>` load a `SourcesConfig` (`glue.LoadSources` /
`OpenSessionSourcesFile`) → name-sorted `[]Source` → `OpenSessionSources`. Each map key is the keyspace
name; the value is a path string or `{path: …}` object (local, `~`, glob, or an object-store URI); a
relative path anchors at the **config file's own directory** (portable regardless of CWD; `~`/absolute/
object-store pass through); it is mutually exclusive with positional sources. Per-source **`formats`** is
honored (file/dir/glob sources) and **`namespace`** places a source under `FROM <ns>:<keyspace>`; only
**`sorted`** is parsed but still reserved (rejected, so no
silent no-op). This is the **declarative twin** of the imperative CLI list and of the Mode
3 catalog: a durable `"sources"` map in `.n1k1/catalog.json` (persisted like `.formats`) would let a
workspace remember its sources across runs. All three (argv, `-sources` file, catalog) build the same
`[]Source` → federated `flatKeyspace` map.

### Query-defined VIEWs (proposal — expansion + pushdown remain)

A morphed-over-time source (early flat JSON → renamed/nested → Parquet under `year=/month=`) should
look like one coherent keyspace. **A catalog VIEW = an implicit, always-available WITH binding:**
the expansion machinery already exists (the WITH/CTE stack in glue; `Conv.withBindings`), so
seeding `Conv.withBindings` from the catalog makes `FROM events` plan exactly as
`WITH events AS (<stored SELECT>) SELECT … FROM events` — pure glue-layer (no fork change), views
compose via CTE-ref threading, recursive views ride `WITH RECURSIVE`, compiler-safe (expansion
before `conv`). The normalizing view is a union of per-era projections; **`VisitUnionAll` has
landed** (`glue/conv.go`, kind `union-all`, each child a self-contained SELECT sub-plan run by
`OpUnionAll` with vals remapped to a by-name union of labels); `INTERSECT`/`EXCEPT` too. ⚠ **The
hard part remaining is predicate pushdown through the view** — a `WHERE ts >= …` on `events` must
reach the sub-source scans to prune whole eras/partitions, or the view reads all history every
query. After expansion the planner sees a union of subqueries; whether it pushes the outer
predicate depends on cbq's rewrite rules + the §5 predicate-to-scan work. Correct either way;
materialization is the fallback. **Rejected: the fork's `datastore/virtual` for views** — it's a
metadata-only planner shim that refuses `Fetch` and yields no data; macro/WITH expansion is better
(the view name is rewritten away before planning).

### `CREATE TEMP KEYSPACE` — session-scoped materialization (landed)

`CREATE [OR REPLACE] TEMP KEYSPACE <name> AS <select>` runs a query once and holds its rows as a
queryable keyspace for the session; `DROP TEMP KEYSPACE` releases it. No fork grammar — `Session.Run`
recognizes it at the statement level (`parseTempKeyspaceStmt → TempKeyspaceRun`, like
PREPARE/EXECUTE). Rows live in an rhmap `store.Heap` that **spills to disk when large** (temp dir
created lazily only if a query spills, 82750a7f; the spill buffer reused across a Session's queries,
c8684bfb — `RHStore.Reset` zeroes slots so no data leaks). `DatastoreScanRecords` serves its rows
straight from the heap (no backing files).

### `INSERT INTO` — user-driven materialization (landed)

Run a query now and write its rows to a keyspace file for later slicing (drove by the PREPARE++ /
`MULTI_MATCHES()` flow). `` INSERT INTO `analysis/errors.jsonl` (KEY UUID(), VALUE self) SELECT … ``
→ `<root>/<ns>/analysis/errors.jsonl`; the queryable keyspace is the *directory* `analysis` (dated
files accumulate into one keyspace).

- **Where** — `glue/insert.go`, intercepted at the statement level *before* the cbq planner (which
  sidesteps cbq's `plan.SendInsert` requiring a pre-existing target — the default `"new"` mode
  writes a brand-new file). Zero fork changes.
- ⚠ **cbq INSERT-SELECT semantics:** the `VALUE` expression is evaluated against each SELECT
  **output** row (the projection), NOT the `FROM` alias. `VALUE self` writes the whole projected
  row; a `VALUE` referencing a `FROM` alias resolves to MISSING — faithful to cbq, not a bug.
- **Streaming + stage breaker** — rows are never materialized in memory: the producer hands each
  doc (a **copy** of the reused `OnRow` buffer) to a dedicated writer goroutine over a bounded
  channel (`insertWriterQueue`), so JSON encode + file I/O overlap with compute. Error state split
  across the two goroutines (verified under `-race`).
- **`RETURNING`** — makes the statement return a row per inserted doc; since INSERT runs outside the
  planner, `insertReturner` evaluates the `*algebra.Projection` directly against the inserted doc
  (`RETURNING code` → `(alias.code)` against `{alias: doc}`; `RETURNING *` → the whole doc;
  `RETURNING RAW` → the bare value). ⚠ `META().id` in RETURNING isn't meaningful (ids are
  positional).
- **`.parquet` target** — routes to `parquetWriter` (Arrow row-group builder) instead of the JSONL
  writer, so `` INSERT INTO `vecs.parquet` `` materializes a columnar file the read path queries
  back (drives the vector write story + VARIANT write-back).
- **Write mode via `OPTIONS`** (`INSERT INTO ks (KEY …, VALUE …, OPTIONS <objExpr>) …`, no fork
  change; `insertWriteMode` constant-folds it): `{"mode": "new"}` (default, errors if the file
  exists), `"append"`, `"overwrite"` (`"replace"` synonym). ⚠ **Atomicity per mode:** `new`/
  `overwrite` write a temp then rename; **`append` uses copy-then-rename** (`jsonlWriter.seed`
  pre-fills the temp with existing bytes, then appends, then renames — crash-safe at O(existing),
  chosen over a bare `O_APPEND` that would leave a partly-appended file on failure; the seeder
  forces a trailing newline so an appended row can't run onto a seeded line). Appending keeps
  positional record-ids stable (existing lines don't move). Not `UPSERT INTO` (our keys are
  positional).

## §3 Compression & containers

Treat **single-stream compression** (`.gz`, `.zst`) as a transparent decode layer *under* the
decoder, keyed off the inner extension (`foo.jsonl.gz` → gzip → JSONL). gzip shipped
(`compress/gzip`); zstd is a stub (`records: .zst not yet supported` — wire `klauspost/compress`).
Treat **`.zip`** as a *container/layout* concern, not a codec: enumerate entries (`archive/zip`),
feed each through format detection as if a directory (also covers office docx/xlsx/pptx, §4). ⚠
gzip/zstd streams aren't seekable, so columnar formats lose random-access/pushdown when gzipped —
fine for row formats.

## §4 The `extract` provider — unstructured & semi-structured sources

Crack files that aren't clean rows (office/PDF **and** the messy semi-structured bulk — logs,
command dumps, config concatenations, opaque blobs) into queryable rows **plus** the file-level
metadata (pruning, `_meta`, doc-IDs, and — the load-bearing new consumer — the sorted-source
merge). Domain knowledge lives in recipes, never in n1k1 core: the engine provides the generic
seam (match a file → describe it → extract rows); a user brings a git repo of recipes.

**Status: LANDED (E1–E5).** Two-phase, pluggable (`*.extract.js` matched by ext **or** name-regexp
with priority), streaming, metadata-rich. Only auto-cloning the recipe repo from git remains.

**Two outputs on two cadences:**
1. **`describe(file) → ExtractSpec`** — a *cheap, once-per-file* pass that may only **sample**,
   returning what the planner/manifest need before a full scan: format, framing, the timestamp/sort
   key contract, sortedness + zone maps, provenance, record count. ⚠ **Memoized in `.n1k1/` keyed by
   file fingerprint, so the expensive format-specific work happens once per file across all queries**
   (an unchanged file doesn't even re-describe; a changed one re-describes only itself). This is
   where the hard, format-specific knowledge lives.
2. **`extract(file, meta) → records`** — the *streaming, per-record* pass at scan time, **handed the
   earlier `describe` result** (no re-sniffing). Streams with bounded memory so a 400 MB log never
   materializes.

**Declarative spec (fast) vs imperative extract (flexible).** Most log formats are regular, so the
preferred contract is **`describe` returns a declarative `ExtractSpec` and n1k1 applies it natively**
(byte-oriented, zero JS on the hot path). Only formats too irregular (crack a binary blob, stateful
multiline, a self-contained document like TOML) fall back to imperative `extract(file, emit)` in the
runtime (JS today), paying the boundary cost. **This imperative path is now wired**, in two
forms: a BUFFERED `extract(file, emit)` (JS gets the whole file text and `emit`s records — demo
`toml2.extract.js` re-parses TOML under `.toml2` matching the native Go `.toml` reader
byte-for-byte) and a STREAMING `extractStream(file, emit)` (JS reads incrementally via
`file.readLine()` and `emit`s records that flow out one at a time with backpressure, at bounded
memory — demo `stanza.extract.js`). A recipe may define `describe`, `extract`, or `extractStream`,
and its `match` may claim a brand-new extension. `ExtractSpec`:
- **`framing`** — `line` / `multiline` (a lead line + continuation regex) / `json` (JSONL) /
  `section` (one record per `====`-banner block — `couchbase.log` is 302 concatenated command
  outputs) / `whole` (the office/PDF baseline).
- **`fields`** — named regex captures / grok pattern (`ts`, `level`, `node`, `module`, `msg`),
  native byte-regex on the fast lane.
- **`time`** — the sort-key contract: which field, its `layout` (RFC3339 / epoch_s/ms / strftime),
  default tz → normalized to one sortable **int64 epoch-nanos** key. The single field the merge
  join requires.
- **`order`** — `sorted: strict|near|none`, and for `near` a `disorder_bound` (sorted sources).
- **`provenance`** — constants lifted from the file once (the banner `command`, the `node` id) that
  ride every record's `_meta`.

**Matching: extension AND regexp AND priority.** The bundle breaks extension-only keying (nearly
everything is `.log`, yet `ns_server.info.log`/`diag.log`/`memcached.log` are different formats, and
`master_events.log` is JSONL). An extractor declares `{exts, names (regexps), priority}`; the
highest-priority match wins (`records.ExtractMatch`). **This is the same matcher `DESIGN-prepare.md`'s
source-routing uses** — the binding resolver's robustness ladder (explicit glob → convention regex →
content sniffing) is the extractor's `{names, exts}` + what `describe` learns; the "thin adapter" is
the `ExtractSpec` `describe` returns. Both the binding manifest and the memoized describe result are
*data* the datastore reads at run time, so rebinding a compiled corpus to a new bundle needs no
recompilation. The recipe repo versions three coupled things per logical keyspace: the detectors
(`DESIGN-prepare.md`), the adapter/extract recipe (this §4), and the per-bundle binding manifest.

The pure-Go office/PDF/media extractors (`records/extract.go`, ex. L) remain the built-in baseline,
re-expressed as `{whole}` specs; the registry is now open to git-cloned JS recipes.
**Document-extraction libraries (permissive only):** pure-Go default `xuri/excelize` (XLSX, BSD-3) +
`ledongthuc/pdf`/`pdfcpu` (PDF); optional Tika/`extractous`+Tesseract build-tag backend for
breadth+OCR (cgo). ⚠ **Avoid viral:** `go-fitz`/MuPDF (AGPLv3), UniDoc (AGPL/commercial),
`sajari/docconv` (shells out to GPL tools).

## Sorted & near-sorted sources: the merge-join contract

The payoff of describe metadata: temporal correlation over time-ordered records. Such records are
sorted or near-sorted by time; a K-way **merge** across many files is O(N log K) and streams —
vastly cheaper than sorting the whole corpus and than the O(n²) naive correlated subquery. But a
merge is correct only if extract hands it a trustworthy **sort key + sortedness contract**
(`ExtractSpec`'s `time` + `order`).

- **Normalized sort key** — `describe`'s `time` spec normalizes each source's wildly different
  timestamps into **one comparable int64 epoch-nanos** key, timezone-normalized. Without this the
  merge cannot order across sources at all; it is the single most important extract output.
- **Sortedness classified** — `strict` (every key ≥ predecessor → plain K-way min-heap); `near`
  (mostly sorted, **bounded** disorder — real logs: threads flush slightly out of order → buffer a
  small reordering window, gate emission on a **watermark**); `none` (spill-sort first).
- **The `disorder_bound`** (for `near`): `{window: Δt}` (a key never more than Δt behind an
  already-seen key — the Flink/Dataflow watermark model) or `{span: N}` (never more than N
  positions from sorted place). **Declared** by the format author or **measured** by describe from
  its sample.
- **The merge operator** (a K-way merge source op, one cursor per file): **disjoint ranges →
  concatenate, no heap** (if zone maps show `max_key(fᵢ) ≤ min_key(fᵢ₊₁)` — common for dated
  partitions); **strict → min-heap**; **near → watermarked buffer** (hold a record until the
  watermark `min(frontier_key) − max disorder_bound` passes its key; buffer bounded by
  `disorder_bound × arrival rate`, spill if not). ⚠ **Validate the claim: a wrong `disorder_bound`
  silently corrupts a merge — a correctness bug, not a perf one.** If a record arrives older than
  the current watermark, the merge must NOT silently emit out of order — it widens the buffer and
  warns, or errors, or falls back to a full spill-sort per a strictness knob.
- **ASOF rides the merge** — the stock-SQL++ correlated "argmax" subquery the planner recognizes
  runs as this O(n) merge instead of O(n²) (`DESIGN-merging.md`/`DESIGN-sorting.md`). Windowed
  rate/burst/streak detectors ride the same ordered stream.

## Worked examples: layout → resolution (status)

CLI `n1k1 [-c "<stmt>"] <dataRoot>` (one root today; **multiple sources proposed** — see §2);
`FROM default:orders` reads `<dataRoot>/default/orders/`.

| Ex | Layout | Resolves as | Status |
|---|---|---|---|
| A | one JSON doc per file (`orders/order-001.json`) | `META().id` = stem | ✅ (the original convention) |
| B | flat root (bare dir of files) | one keyspace named by basename (`glue/flat.go`) | ✅ |
| B2 | single file (`events.jsonl`) | one-file keyspace by stem; `META().id` = `events.jsonl#57@4210` | ✅ |
| B3 | grab-bag dir (loose files + subdirs) | one keyspace per top-level structured file, by stem | ✅ |
| C/D | multi-file, many records/file (`.jsonl`); mixed CSV+JSONL+JSON | union of all records; opaque-doc path (CSV `qty` int-inferred, JSON keeps its type) | ✅ |
| E | deep recursive tree | recurse + union; path segments invisible (not columns) | ✅ |
| F | hive `year=2026/…parquet` | `year`/`month` virtual columns, predicate prunes 2025 file | 🟡 needs scan-layer pushdown |
| G | bare date dirs + gzip (`20260101/*.log.gz`) | catalog projection computes candidate dirs from `WHERE date>=…` | 🟣 needs catalog |
| H | compressed single file | decompressed by inner ext | ✅ gzip / 🟡 zstd |
| J | CSV/TSV + header | header names columns, one JSON object per row | ✅ |
| K | Parquet | transpose-to-rows + projection + footer-stats aggs | ✅ (+partial vectorization) |
| L | office/PDF/media | one `{filename,kind,text,…}` record/file | ✅ (pure-Go; OCR later) |
| M | multiple sources on the CLI / a `-sources` config (`drive=~/Drive/** events=s3://…/tbl`) | one keyspace per source, joinable in one query; heterogeneous kinds federate; per-source `-formats` + `namespace` | ✅ (§2; sortedness reserved) |

**O — query-defined VIEW over a morphed source** 🟣 (`.n1k1/catalog.json` defines each era as a
keyspace + a normalizing `UNION ALL` view). The `UNION ALL` converts (`VisitUnionAll`); remaining is
catalog-view expansion + predicate pushdown so `WHERE ts>=… GROUP BY _era` prunes whole eras.

**P — support bundle (`cbcollect_info`)** 🟡, the driving PREPARE++ case: four `.log` formats + a
JSONL + a 302-section `couchbase.log` + JSON dumps + opaque blobs, each described by a
regexp-matched recipe (`ns_server\..*\.log` → `{multiline, fields:{ts,level,node,module,msg},
time:{RFC3339,+02:00}, order:{near,{window:2s}}}`; `master_events.log` → `{json, epoch_s}`;
`couchbase.log` → `{section}`; blobs → imperative or skipped). Then a detector reads clean,
time-ordered rows across nodes — `WHERE ts BETWEEN` prunes files by the time zone map; the union
scans as a **watermarked near-sorted merge** (globally ordered, bounded memory); `level`/`node` came
from declarative `fields`/`provenance` (no per-row JS). One detector; PREPARE++ runs thousands over
the same single merged scan (MQO).

**What the examples reveal:** the cheap cases (A–E, H-gzip, J, L) all ride the **opaque-document
path** — why they shipped fast, and it stretched further than expected (CSV and office fit by
emitting a JSON object per row/doc). Typed-label reconciliation is forced only by columnar Parquet
(K) and partition virtual-columns (F/G). Partition pruning (F/G) is the first feature needing the
predicate pushed to the scan layer. The bundle (P) is a *different* hard: format heterogeneity +
irregular framing + per-source timestamp normalization — why extract splits into cheap-`describe` +
fast-`extract` and why the sortedness/time metadata is first-class.

## §5 Indexes & derived artifacts: storage + change detection (proposal)

> **Scope: everything from here down (and §6) is post-MVP / aspirational** — written at design
> fidelity, not near-term effort. §6 doc-ID synthesis is needed only once multi-record files land,
> and the minimal `<relpath>#<line>` with rescan-`Fetch` suffices; encryption-at-rest is a
> much-later enterprise feature.

- **Where:** a single sidecar root per dataset (`<dir>/.n1k1/`, hidden, easy to gitignore/delete)
  holding the catalog, index files (bbolt/bleve), extracted caches, and a **manifest** describing
  the source state each artifact was built from. Canonical layout lives in `DESIGN-indexing.md`;
  this doc owns `catalog.json`'s source/layout half and the per-keyspace `manifest.json`.
- ⚠ **Comingling in `catalog.json` — separate by writer & lifecycle.** It carries both source
  mappings and *declared* index intents — safe because both are human/generator-authored,
  slow-changing, **single-writer**. **Machine-managed output must NOT live in `catalog.json`** —
  everything fast-changing/per-indexer/auto-rebuilt lives in self-describing per-instance dirs
  (`idx/<name>__<kind>__<defhash>/`) + `manifest.json`. **Adaptive/auto-created indexes must not
  rewrite `catalog.json`, else the single-writer property is lost.** Declared-vs-adaptive maps to
  file-vs-dir.
- **When:** default **lazy check-on-query** (`stat` the tree, Merkle-pruned, rebuild only stale
  artifacts), optional TTL + `--no-revalidate` for static trees; a background `fsnotify` watcher is
  a later nicety (a short-lived CLI shouldn't need a daemon). ⚠ **Files changing mid-scan:** snapshot
  the manifest fingerprints at query start; if a file's `(size,mtime)` changed since open, error or
  re-read — don't silently mix old and new (no MVCC). Concurrency on `.n1k1/` needs a
  lockfile/atomic-rename for manifest writes.
- **How: a manifest with per-file fingerprints, Merkle-rolled.** Per-file `(relpath, size, mtime,
  content_hash?)` — `(size,mtime)` alone is the cheap Spark/Hive-class check (default); an optional
  content hash (xxhash/blake3) computed only when identity says it might have changed. **Merkle
  rollup** (hash each dir node from its children, git's tree model): one root-hash compare answers
  "did anything change?"; descend only into changed subtrees. **Append-only optimization** (logs):
  store `(known_offset, hash_up_to_offset)` — if the prefix hash matches and size grew, index only
  the tail (assumes `known_offset` on a record boundary).
- **Manifest contents (three levels):** *per file* — identity + hashes/offsets, `format`/
  `compression`, `doc_count`, **zone map** (min/max per column, prune without reading),
  `schema_fingerprint`, `partition_values`, per-index build-state, `status`+`error`. Plus the §4
  **sorted-source contract** (`sort_key`, `sortedness`, `disorder_bound`, min/max time zone map,
  key→offset sync points that double as the §6 seekable index). *Per partition* — `merkle_hash`,
  rolled aggregates + min/max, `sealed?` (immutable past-partition hint). *Per root* —
  `manifest_schema_version` (bump ⇒ rebuild), `root_merkle_hash`, `config_fingerprint` (hash of
  catalog+defs, so changed *definitions* invalidate derived data). Rule of thumb: stat-level fields
  serve change-detection, stats fields serve pruning+planning, build-state serves incremental
  indexing — start minimal, add zone maps/counts when the planner can exploit them.
- ⚠ **The load-bearing coherence caveat with `DESIGN-indexing.md`:** its tier-1 pitches always-on
  zone maps as needing no planner change (file-skipping is a scan-layer concern) — true, **but the
  prerequisite it glosses is that the predicate has to REACH the scan.** Today a primary scan
  doesn't get the `WHERE`; the planner emits a residual `Filter` op *above* the scan, so the
  datastore never sees what to prune by. Zone-map pruning needs either (a) filter **pushdown into
  the primary scan** (a conv + fork datastore-interface change — modest, recommended) or (b) a
  datastore-side predicate hook. Sequencing: first manifest carries only change-detection +
  build-state; add zone maps with the predicate-pushdown work; defer cardinality until CBO. Head
  start: the fork already runs `SargableFor` against a throwaway `partitionVirtualIndex` — the
  sargability test exists; what's missing is delivering the verdict to the scan.
- **Libraries:** don't hand-roll a table format — `apache/iceberg-go` (a dep, read+write, per-file
  stats + snapshots + time-travel) is the mature expression; start with a thin custom manifest in
  `.n1k1/` (per-file `size+mtime+xxhash`, Merkle-rolled, append-only offsets) and keep iceberg-go as
  the upgrade path.

## §6 Primary keys / document IDs (`META().id`) & `_meta` (mostly proposal)

**Implemented: file metadata via a `_meta` doc field (not `META()`).** The fork's `META()` exposes
only a fixed bitmask, so per-file metadata rides a reserved **`_meta`** sub-object — `` `path` ``
(dir-relative), `name`, `ext`, `size`, `mtime`, `pos` (0-based ordinal within a container file) —
controlled by `-meta` `off`/`on`/`auto` (default; each provider decides — office/PDF include it,
structured JSON/CSV don't, keeping the conformance suite unchanged). `META().id` stays the stable
key (stem / `relpath#i`), since `USE KEYS`/`JOIN ON KEYS` depend on its format. (`path` is a
reserved word → `` _meta.`path` ``.)

A synthesized ID must be **deterministic, unique within a keyspace, self-describing/addressable**
(for O(1) `Fetch`), and **stable under the mutation pattern**. Strategies (configurable per source):
filename stem (today, one-doc-per-file); user-designated natural key (catalog names a key column);
ordinal/line number; **byte offset in the logical stream** (O(1) `Fetch` given a seekable substrate,
preferred for large files); content hash (stable across reorder, not addressable). **Recommended
default** for multi-record: composite `<source-relpath>#<logical-offset>`, falling back to
`#L<lineno>` when not seekable (shipped form `<relpath>#<line>@<offset>`, §1). ⚠ **Positional IDs
are durable only if content above them is immutable** — the append-only log case; for mutable files
prefer a natural key or content-hash.

Tweaks: compressed containers aren't randomly seekable, so use a seekable format (BGZF / seekable-
zstd, doc-ID stores the logical offset) or ordinal IDs + periodic sync points bounding `Fetch`
re-scan to one span. `.zip`: ID includes the entry name. **Encryption-at-rest** — design as another
transparent layer (raw → decrypt → decompress → decode); random access needs segmented encryption
(Tink `streamingaead` / age STREAM's `DecryptReaderAt`), so seekable-compression and
seekable-encryption share one mechanism; envelope keys via `gocloud.dev/secrets`. ⚠ **Critical
coupling — derived artifacts leak plaintext:** indexes, extracted text, and the manifest are built
from *decrypted* content, so the `.n1k1` sidecar must itself be encrypted at rest (same DEK/KEK) or
kept only in memory. A hard requirement.

## §7 Table formats: Apache Iceberg (read-only) — SHIPPED

A table format sits ABOVE §1 file formats: Iceberg is a metadata layer over a pile of Parquet files
(catalog → table metadata → snapshot → manifest list → manifests → data files + optional delete
files), adding schema evolution (columns by stable **field-ID**), hidden partitioning, per-file
column stats for pruning, atomic snapshots (ACID + **time-travel**), and merge-on-read (MoR)
deletes.

**The big enabler:** `github.com/apache/iceberg-go v0.4.0` is already a dep and **builds cgo-free**
(pure-Go: arrow-go, `hamba/avro`, aws-sdk-go-v2). Its `table.Scan(filter, columns, snapshot)` →
`ToArrowRecords()` yields Arrow batches with file pruning, MoR deletes, and field-ID schema
evolution already resolved — so n1k1 does NOT reimplement any of it, and an Iceberg source slots
into the **same** transpose/columnar machinery as Parquet (`records/parquet.go`'s
`appendRecordsNDJSON` / `NextColumns`); the only new code is "drive the scan, feed batches to the
existing renderer" (`records/iceberg.go` `OpenIcebergTable`, ~90 lines).

Shipped, feature by feature:
- **Keyspace wiring** — point the CLI at a table dir (or dir of tables); `records.IcebergTableMetadata`
  detects a `metadata/` dir and resolves current metadata (`version-hint.text`, else the
  lexicographically-greatest `*.metadata.json`); `glue/flat.go` `maybeIcebergTable` exposes each as a
  synthetic `default:<basename>` keyspace, tagged `iceberg` in `.tables`/`.schema`. ⚠ A dir mixing
  Iceberg tables AND loose files resolves the tables only (flat discovery skipped once a table found).
- **Projection + predicate pushdown** — the source builds its scan LAZILY, so a projection feeds
  `WithSelectedFields` and a WHERE feeds `WithRowFilter` (prunes whole data files by manifest column
  stats). A neutral `records.ScanPredicate` + `records.RowFilterer` sidecar carries the WHERE
  (`glue/scan_pushdown.go` extracts it in `VisitFilter`). ⚠ **Pushdown is a pure HINT: the `filter`
  op is kept, so an absent/partial/loose push is always correct** (unconvertible clauses dropped; a
  partial OR drops the whole predicate; the expression is pre-`BindExpr`-validated).
- **Partition pushdown (falls out of predicate pushdown)** — iceberg-go's `WithRowFilter` prunes
  partitions via `newInclusiveProjection` even through a HIDDEN transform (`day`/`month`/`bucket`/
  `truncate`). The one gap was temporal columns (SQL++ has no date literal): `clauseToIceberg` now
  builds a string-literal predicate for `date`/`time`/`timestamp`/`uuid` and lets iceberg-go coerce
  via `StringLiteral.To(type)` on Bind (timestamps render ISO-8601, so the residual string compare is
  chronological too).
- **Richer predicates + nested boolean** — `IN`/`NOT IN`/`!=`/`IS [NOT] NULL` (each also as `NOT(…)`);
  `ScanPredicate` is a negation-normal-form TREE (NOT pushed into leaves via De Morgan), so
  `(a AND b) OR c` pushes. Monotonicity keeps partial pushdown sound: an AND drops unpushable
  children (widen), an OR is all-or-nothing.
- ⚠ **LIKE-prefix as a string RANGE, NOT StartsWith** — `field LIKE 'prefix%'` rewrites to
  `field >= 'prefix' AND field < successor(prefix)` (`glue.likeToRange`). Deliberate: **iceberg-go
  v0.4.0 can PRUNE with `StartsWith` but CANNOT READ with it** — the residual row filter during
  `ToArrowRecords` runs `starts_with` through arrow-compute, which doesn't implement it, so a pushed
  `StartsWith` errors the scan. The range prunes identically via string zone-maps and reads fine.
- **Time-travel via a keyspace-name suffix** — cbq's native `AT SNAPSHOT` is welded to its
  `ExternalScan` path, which n1k1 bypasses, so time-travel rides `` `events@<snapshot-id>` ``
  (all-digits) or `` `events@<rfc3339>` `` (`glue/flat.go` `KeyspaceByName` clones the base keyspace
  with a `records.ScanSnapshot`, cached, not listed in `.tables`).
- **Columnar `NextColumns` + metadata-only COUNT/MIN/MAX** — the source implements
  `ColumnBatchSource`/`ColumnsSource`, so an ungrouped aggregate takes the same vectorized path as
  Parquet (only INT64/DOUBLE); and `Columns()` aggregates per-column stats from `PlanFiles`
  (manifest reads only, 8-byte-LE bounds) so COUNT/MIN/MAX answer from metadata. ⚠ **If the snapshot
  has ANY delete files, `addManifestStats` BAILS** — MoR deletes reduce effective rows, so raw
  counts/bounds would overcount; the query falls through to a real scan. ⚠ Fixing this exposed a
  latent bug: `count-star` read `cols[0].Count` unchecked → an Iceberg COUNT(*) returned −1 (Iceberg
  reports −1 when unknown); now guarded to require a known row count.

Open: catalogs beyond a filesystem/object-store path (REST/Glue — need config+creds); snapshot-
history discovery (list ids/timestamps); the `list<float32>` VECTOR_DISTANCE columnar path
(documented-future — only pays off if embeddings are stored IN Iceberg tables; DESIGN-vectors.md);
a MoR-delete correctness suite. **Non-goal: Iceberg *writes*** (needs a catalog + concurrency
control). ⚠ **Test note:** iceberg-go v0.4.0's `partitionedFanoutWriter` has an internal data race
in a PARTITIONED `AppendTable`, so partitioned-fixture builders skip under `-race`; n1k1's read path
is race-clean.

## §8 Object-store scans (S3 / GCS / Azure) — SHIPPED (reads)

Reads land over S3, GCS, and Azure via iceberg-go's IO + a gocloud bucket: Iceberg tables (by an
explicit metadata JSON, or a bare table dir with current-metadata listing) AND standalone
`FROM {s3,gs,abfs}://…/x.parquet` are FROM-able keyspaces. Mostly plumbing, not new machinery: the
dependency weight was paid long ago (iceberg-go bundles the S3/GCS/Azure FileIO backends, arrow-go's
Parquet reader is built on `io.ReaderAt`), so the only thing missing was a URI/scheme seam + a
credential source.
- **Iceberg** — `iceio.LoadFSFunc(props, location)` already dispatches by scheme (`s3`/`gs`/`abfs*`,
  else LocalFS); `OpenIcebergTable` detects an object-store URI and threads a props map. Its `File`
  is `io.ReaderAt` backed by ranged `GetObject`.
- **Standalone Parquet** — `records.OpenParquetSourceRemote`: `file.NewParquetReader(f,…)` where `f`
  is iceberg-go's own object-store `File`, so **S3/GCS/Azure all work through one path**
  (`objectStoreFSFunc`), reusing every credential/endpoint/addressing/anonymous rule.

⚠ **Streaming, not whole-file download (the load-bearing property).** Parquet's footer (schema +
per-chunk offsets + stats) is at the END; a ranged reader fetches (1) a tail read → footer, (2) only
*projected* columns' chunks, (3) only row groups surviving stats/predicate pruning. The whole object
is never downloaded — exactly why projection+predicate pushdown pays off over a network, and why
Iceberg's manifest-level file pruning is doubly valuable (a pruned file is never `GET`-ed). ⚠ **Two
arrow-go settings are load-bearing over the network** or an innocuous `SELECT * … LIMIT 10` becomes
pathological:
- **Bounded batch size** (`parquetBatchRows`, all sources) — arrow's default decodes the ENTIRE row
  group as one batch, so a LIMIT over a big row group decodes millions of rows across every projected
  column (measured on a 227 MB file: 12.9 GB allocated / 4.1 GB heap for `SELECT * LIMIT 10`). Bounded
  → 256 MB, and LIMIT stops after ~one batch.
- **Buffered streaming** (`ReaderProperties.BufferedStreamEnabled`, remote only) — off, arrow reads
  each column chunk in FULL before decoding (over the network, the whole chunk); on, it reads pages
  through an `io.SectionReader` so a LIMIT fetches only needed pages. Measured: `SELECT * LIMIT 10`
  57 s → 4.5 s; `COUNT(*)` stays footer-only (~1 s). Kept **off locally** (whole-chunk reads are
  cheap on a local file and preserve the tuned columnar path).

**Credentials & addressing.** v1 sources creds from the environment (`records.ObjectStoreProps`
maps `AWS_ENDPOINT_URL`/`AWS_REGION`/`AWS_ACCESS_KEY_ID`/… to iceberg-go's `s3.*` keys), else the
AWS default chain. `AWS_NO_SIGN_REQUEST=1` uses anonymous credentials for public buckets (wired for
both n1k1's own client and iceberg-go's data-file reads via `objectStoreFSFunc` injecting an
anonymous `aws.Config`). ⚠ **Addressing:** path-style for a custom endpoint (MinIO requires it),
virtual-hosted for real AWS; **the `AWS_REGION` must match the bucket's or AWS 301-redirects** (a
region-scoped SDK client doesn't auto-redirect). Verified end-to-end on real public data (Ookla Open
Data, ~227 MB, `AWS_NO_SIGN_REQUEST`): `COUNT(*)` in ~1 s (footer only), projected `LIMIT` fetching
only referenced chunks. GCS/Azure share the code paths but weren't exercised against a live bucket
(no creds in the test env).

⚠ **Test blocker (full mock-S3 read of an Iceberg table):** Iceberg bakes ABSOLUTE data-file
locations into its manifests, so a table built for local disk can't be copied to S3 — the fixture
must be *built* with `s3://` locations, needing a write-capable mock (`gofakes3`, BSD-3, in-process
`httptest`). But this standalone worktree's `go.mod` pins couchbase siblings at an unresolvable zero
pseudo-version, so `go get`/`go mod tidy` can't load the build list to add the dep; it must happen in
the repo-sync build (or by vendoring). Remote Parquet already reads real bytes end-to-end via a
range-honoring `httptest` endpoint (no mock-S3 needed).

## Dependency licensing (permissive only)

Policy: **MIT / Apache-2.0 / BSD** only — no GPL/AGPL/copyleft/viral. In use / planned:
`go.etcd.io/bbolt` (MIT), `blevesearch/bleve/v2` (Apache-2.0), `couchbase/rhmap` (Apache-2.0),
`apache/iceberg-go` + `apache/arrow-go/v18` (Apache-2.0), `scritchley/orc` + `hamba/avro` (MIT),
`klauspost/compress` (BSD-3), `buger/jsonparser` (MIT), Go stdlib (`encoding/csv`, `compress/gzip`,
`archive/zip`), `cespare/xxhash` (MIT); for the aspirational tracks `SaveTheRbtz/zstd-seekable`
(MIT), `google/tink-go` + `gocloud.dev/secrets` (Apache-2.0), `FiloSottile/age` (BSD-3),
`xuri/excelize` + `ledongthuc/pdf` (BSD-3), `pdfcpu` (Apache-2.0), Tika/extractous/Tesseract
(Apache-2.0). ⚠ **Excluded (viral — do NOT use):** `go-fitz`/MuPDF (AGPLv3), UniDoc/unipdf
(AGPL/commercial), `sajari/docconv` (shells out to GPL `wv`/`poppler-utils`/`unrtf`/`antiword`).
DuckDB (MIT) is design inspiration, not a dep.

## Testing strategy

- **Interpreter/compiler differential** — every new format/layout needs a case in the queryCases
  harness (`test/cases.go` + `test/query_compiler_test.go`) so the compiled path matches the
  interpreted. Done: flat-root + each decoder + a data-backed GSI suite (results-pass floor 833).
- **Golden fixtures for decoders** (`records/records_test.go`, table-driven); the existing
  one-doc-per-file JSON corpus validates the convention path unchanged.
- **Change-detection tests** — manifest logic (mtime skip, merkle subtree skip, append-only tail,
  concurrent-writer race) is pure logic over a temp dir; the part most likely to be subtly wrong.
- ⚠ **Allocation benchmarks (a gate)** — assert allocs/op stays ~flat as row count grows; a rising
  curve means a per-value allocation leaked in.

## Open questions (still genuinely open)

- **Predicate pushdown through a VIEW / to the scan (the gating perf question).** Does a
  `WHERE`/partition predicate reach the sub-source scans so whole eras/partitions prune, or does the
  view/scan read all history? Depends on cbq's rewrite rules + the §5 predicate-to-scan work.
  Correctness is fine either way; materialization is the fallback.
- **Bespoke manifest vs iceberg-go** — adopt Iceberg's proven metadata, or keep a minimal custom
  `.n1k1` manifest (interop/robustness vs simplicity)?
- **CSV typing** — how aggressively to infer types vs treat cells as strings; how to expose
  overrides. And the `[]byte`-oriented zero-copy CSV reader + the allocs/op gate.
- **`disorder_bound`: declared, measured, or both — and what when it's wrong.** A sampled bound can
  under-estimate; the merge must validate (widen+warn / error / spill-sort). How conservative the
  default, and is a per-source strictness knob the right surface?
- **Log time model** — normalizing wildly different timestamp formats/timezones/precision into one
  int64 epoch-nanos key; ⚠ the merge compares keys *across* nodes, so **clock skew is a correctness
  risk** (mirrors `DESIGN-prepare.md`).
- **`ExtractSpec` expressiveness vs the imperative escape hatch** — grok-completeness, stateful
  multiline, nested framing (a `section` whose body is itself `multiline`).
- **Extract-recipe repo governance** — a trusted-code surface (like the detector corpus):
  signing/pinning, golden-fixture CI per extractor, and how describe-spec invalidation interacts
  with the content-addressed sidecar cache.
- **Native vs cgo extractors/OCR** — accept the extractous/Tika native dependency for breadth+OCR,
  or stay pure-Go and narrower?
- **Default doc-ID scheme & encryption seekability** — positional (addressable, shifts on edit) vs
  content-hash vs requiring a natural key; which segmented-encryption format (Tink vs age).
- **Remaining multi-source niceties (§2).** Multi-source shipped: federating heterogeneous *kinds*
  (local + local/remote Iceberg + remote Parquet), the `-sources` config, per-source `-formats` (a
  `flatKeyspace` override overlaid at the `KeyspaceRecordsOpen` choke point), and per-source `namespace`
  (a `flatDatastore` chain, one per namespace). Still open: per-source **sortedness** — declaring a source
  is (near-)sorted on a key so it feeds the watermarked K-way merge / ASOF. Unlike the extract path
  (which *measures* `SortedSourceMeta` per file: normalized int64 key, min/max zone map, disorder bound),
  a plain multi-source declaration has none of that, so it needs either eager measurement at open or a
  reduced "declared, heap-merge, no pruning" contract — scope TBD. Also open: a durable catalog
  `"sources"` map; a `.source add/list/rm` live-attach command; cross-source `_meta` provenance under
  `UNION ALL`.
