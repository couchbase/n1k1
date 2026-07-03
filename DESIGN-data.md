# Design: Data Sources for n1k1

Status: **partially implemented** (MVP + several post-MVP items shipped — see
"Implementation status" below); rest is proposal / for review. Companion:
`DESIGN-indexing.md` (read together — see "Relationship" below). Revision
changelog lives in git history, not here.

---

## Implementation status (what has actually landed)

The single biggest confirmed learning: **the data-source work needed ZERO changes
to the `n1k1-query` fork (cbq).** The design predicted this ("fake it" for
plan-time metadata; execution in n1k1 glue ops) and it held — the fork's only
n1k1-specific commits are build-plumbing, none touching datasources:
`semantics/semchecker_ce.go` (enable EE SQL++ semantics in the community build),
`system/systemStats.go` (pure-Go stub dropping the sigar cgo dep), and a
committed goyacc-generated `parser/n1ql/y.go`. **No `datastore/file`,
`datastore/virtual`, `algebra/`, or planner edits — and no `DiscoverKeyspaces`
seam was ever added.** The hedged "a thin `DiscoverKeyspaces` seam, only for
catalog-defined keyspaces" language that still appears below is now **superseded**:
flat-root discovery was done entirely n1k1-side by *wrapping* the fork's datastore
with `datastore/virtual` building blocks (`glue/flatroot.go`), so even the
catalog case is expected to need no seam.

Landed n1k1-side (all in `records/` + `glue/`, `//go:build n1ql`), well past the
originally-drawn MVP line:
- **Flat-root keyspaces** (scenario B) — `glue/flat.go` wraps the datastore to
  advertise a synthetic `default:<basename>` keyspace with a primary index; the
  records-scan reads the root dir via `RecordsDir()`. Never calls into the fork.
- **Single file as a keyspace** (scenario B2) — the CLI arg may be a lone record
  file (`events.jsonl`, `dump.ndjson`, `orders.jsonl.gz`), not just a directory.
  Same `glue/flat.go` wrapper, one level flatter: `maybeFlatFile` fakes a
  `default:<stem>` keyspace whose `RecordsFile()` points the records-scan
  (`records.File`) at the one file. Covered by `test/flatfile_test.go`.
- **Grab-bag directory** (scenario B3) — a dir with loose data files *and*
  unrelated subdirs (e.g. `~/Desktop`) exposes one keyspace per top-level
  *structured* file, by stem (`maybeFlat`), merging any real `default` namespace.
  Fixes the prior "no keyspaces" for such dirs. Covered by `glue/flat_test.go`.
- **Multi-file keyspace = union of files, recursing** (scenarios C, E) — the
  `records` package (`records.go`) walks the dir and unions all decodable files.
- **Decoders:** JSONL/ndjson, multi-doc JSON (array + `.jsons`), **and CSV/TSV**
  (scenario J). CSV was shipped by decoding each data row into **one JSON object
  keyed by the header** (light int/float/bool inference) — so it rides the
  **opaque-document path** and did *not* need the typed-label-vector work the doc
  feared (see the reframed note in §2 "Integration gap").
- **Office/PDF text extraction** (scenario L) — `records/extract.go`, pure-Go
  (`.pdf`/`.docx`/`.xlsx` → one `{filename, kind, text}` JSON record each), again
  on the opaque-document path. The optional Tika/extractous+OCR backend is still
  future.
- **Transparent gzip** (`.gz`, scenario H). `.zst` is *recognized* by the walker
  but decoding is still a stub (`records: .zst not yet supported`).
- **`-formats` lockdown flag** (`records.ParseModes`) — comma-separated allow-list of
  formats/`recurse`/`gzip`.
- **`-meta` flag + `_meta` sub-object** (`records/meta.go`) — `on|off|auto`,
  injecting `path`/`name`/`ext`/`size`/`mtime`/`pos`. See §6.
- **`COUNT(*)` pushdown** — `glue` `VisitCountScan` via scan+count.
- **Compiler-differential + decoder-golden tests** landed (flat-root diff, decoder
  interp-vs-compiler proof, a 443-interp/439-compiler data-backed GSI suite).

Still proposal / not built: catalog/sidecar (`.n1k1/catalog.json`), manifests &
zone maps (§5), Parquet/ORC/Avro, `.zip` containers, zstd decode, synthetic
offset doc-IDs & seekable-fetch, query-defined VIEWs (needs `VisitUnionAll`),
object-store backend, encryption-at-rest, inline table functions (needs a grammar
fork). Status markers on the worked examples (§ "Worked examples") reflect this.

---

This document explores the kinds of source data n1k1 should support — file
formats, directory layouts, compression, "office"/unstructured documents — and
how derived artifacts (indexes, caches) should be stored and kept in sync with
changing source data. It takes inspiration from DuckDB, and from how Spark, AWS
Athena/Glue, ClickHouse, and log tools handle the same problems.

### Relationship to `DESIGN-indexing.md` (read them together)

These two docs are one design split in two, and they must stay coherent. The
division of ownership:

- **This doc (data):** what source formats/layouts n1k1 ingests, how a `FROM`
  term resolves to files, compression/containers, document extraction, synthetic
  `META().id`s, and the **change-detection manifest** (fingerprints + zone-map
  *data*).
- **`DESIGN-indexing.md` (indexing):** how the cbq planner comes to *use* an
  index (GSI via `RangeKey` sargability, FTS via `datastore.FTSIndex`), the
  `secondaryIndex`/bleve implementations, COUNT(*) pushdown, "index-everything"
  tiers, and the **canonical `.n1k1/` sidecar layout**.

Where they touch, and how they're kept consistent here:

1. **Fork = plan-time metadata only; execution lives in n1k1 — and the fork itself
   is untouched.** Both docs keep the fork thin and put real logic in n1k1 (the
   `engine.ExecOpEx` IoC pattern). Plan-time facts the planner needs are supplied
   *without editing the fork*: this doc, a keyspace's existence — achieved by
   **wrapping** the fork's datastore with its own `datastore/virtual` building
   blocks (`glue/flatroot.go`), **not** the once-anticipated `DiscoverKeyspaces`
   seam (which was never built); `DESIGN-indexing.md`, the available indexes
   (n1k1-built `datastore.Index`/`FTSIndex` objects via `fileIndexer.Indexes()` /
   `keyspace.Indexers()`). All *execution* — scan, fetch, decode, index `Scan()` —
   runs in n1k1 glue ops over `[]byte`, not in the fork. Fully consistent.
2. **The `.n1k1/` sidecar is shared.** The indexing doc specifies the *canonical*
   tree; this doc owns only `catalog.json`'s source/layout half and
   `manifest.json` (see §5 "Where"). `catalog.json` therefore holds **both**
   source mappings (here) **and** *declared* index definitions (there) — safe to
   comingle only because it stays **single-writer**; machine-managed/adaptive
   index state lives in per-instance dirs, never rewriting `catalog.json`. See
   §5 "Comingling in `catalog.json`".
3. **Zone maps are the load-bearing shared artifact — and the one prior tension.**
   The indexing doc's tier-1 "index-everything-lite" *consumes* the zone maps this
   doc's manifest *produces*. The one correction folded in: pruning is only
   "no-planner-change" once the **predicate reaches the scan** (filter pushdown);
   see the reconciliation caveat in §5. Both docs now agree.
4. **Doc-IDs match:** columnar `file#row_position` (indexing doc) = this doc's
   `<relpath>#<offset|line>` (§6). Consistent.
5. **COUNT(*) synergy:** the indexing doc answers `COUNT(*)` from this doc's
   per-file/partition `doc_count`. Consistent.

## Motivation & scope

Today n1k1 reads exactly one shape of data: a directory of single-document
`*.json` files arranged as `<datastoreDir>/<namespace>/<keyspace>/<key>.json`,
via the forked `couchbase/query` file datastore. To be a useful local SQL++ CLI
("DuckDB for SQL++/JSON"), it should ingest the formats people actually have on
disk — CSV/TSV, JSON Lines, multi-record JSON, log files, compressed archives,
and eventually PDFs/Office docs — across the directory layouts those formats
arrive in.

## Starting point (n1k1 today)

> **Update:** this section describes the *pre-MVP* landscape. As of the
> "Implementation status" section above, the `records` package now supersedes the
> naive path-B decoders: it has real JSONL / multi-doc-JSON / CSV-TSV / office
> decoders with transparent gzip, wired to `FROM` via the flat-root + records-scan
> glue. Read the below as the historical motivation, not the current state.

There are **two separate, only-partially-connected** data paths today:

**A. The SQL++ / file-datastore path (what FROM uses):**
- **Layout:** `<dir>/<namespace>/<keyspace>/<key>.json`, one JSON object per
  file; the file's base name is the document key. Keyspaces are subdirectories
  (`loadKeyspaces`); documents are files; `Scan` does `ioutil.ReadDir` and
  iterates non-directory entries; `fetch` does `ioutil.ReadFile` of `<key>.json`.
  (`n1k1-query/datastore/file/file.go`.)
- **FROM mapping:** `FROM default:orders` → `namespace=default`,
  `keyspace=orders` → directory `<dir>/default/orders/` (case-insensitive). The
  keyspace name *is* a directory name. **No** multi-file-per-keyspace, **no**
  compression, **no** non-JSON formats on this path.

**B. The engine's direct-file scan path (NOT reachable from FROM today):**
- `engine/op_scan.go` already has scan kinds `"filePath"`, `"csvData"`,
  `"jsonsData"`; `ScanFile()` routes by extension (`.csv` → `ScanReaderAsCsv`,
  `.jsons` → `ScanReaderAsJsons`). But the CSV reader is **naive** (splits on raw
  commas via `bytes.IndexByte`, no quoting/escaping, no header/type inference) and
  the JSONL reader just passes each raw line through. These exist as low-level
  ops; they are **not** wired to keyspaces/FROM. This is a useful primitive to
  build on, not a finished format layer.

**Existing infrastructure to reuse (important):**
- The fork already contains a **working Iceberg + Arrow reader**:
  `n1k1-query/primitives/external/iceberg_reader.go` (imports `arrow-go/v18`,
  `arrow/csv`, `arrow/ipc`, `iceberg-go/io`, `iceberg-go/table`; iterates Arrow
  RecordBatches over an Iceberg table scan). It is **implemented but not wired
  into the n1k1 engine** — a natural integration target for Parquet/Iceberg/CSV
  (Arrow has its own robust CSV reader) and for the change-tracking ideas in §5.
- A schema-inferencer hook exists (`GetDefaultInferencer` →
  `infer.NewDefaultSchemaInferencer`), but it's an enterprise feature that n1k1's
  pure-Go build drops.
- **Relevant deps already present** in `go.mod` (signalling intent):
  `apache/iceberg-go`, `apache/arrow-go/v18`, `substrait-io/substrait`,
  `scritchley/orc`, `hamba/avro`, `klauspost/compress`, `blevesearch/bleve/v2`,
  `go.etcd.io/bbolt`; `buger/jsonparser` is a direct dep used for JSON decoding.

## Where this code lives (the load-bearing decision)

**What we take from cbq, and what we don't.** n1k1 reuses cbq for its
parser + **planner output, the `plan.Operator` tree** (index selection, spans,
join order, pushdowns — see `DESIGN-indexing.md` "Background"). It does **not**
use cbq's **execution runtime** — the tuple-by-tuple operator iteration over boxed
`value.AnnotatedValue`s — because that allocates a boxed value per tuple/field,
the opposite of n1k1's `base.Val = []byte` buffer-reuse engine. `glue.Conv` lowers
the plan tree into n1k1 `base.Op`s that execute over `[]byte`. So the fork is a
source of *plans*, not a runtime — which is exactly why new datastore behavior
belongs in **thin seams**, not fork code. Exception: n1k1 does reuse cbq's
expression evaluation machinery as a fallback for complex expressions.

`FROM default:orders` is resolved by cbq's planner, which asks the
`datastore.Datastore` interface (the fork's `datastore/file`) for keyspace
metadata, producing a `plan.Op` that `conv` then lowers. **Two different needs
hide behind "the datastore" — separate them, because only one touches the fork:**

- **(A1) Plan-time keyspace metadata — n1k1 fakes it, NO fork change.** The planner
  must believe `FROM keyspace` exists and has a primary index (it calls
  `namespace.KeyspaceByName` / `keyspace.Indexers` during `Build`). Rather than a
  fork seam, n1k1 **wraps the datastore** to advertise a *synthetic* namespace +
  keyspace — pure planner-facing metadata, no physical dir — reusing the fork's
  importable `datastore/virtual` building blocks (`virtual.NewVirtualKeyspace` +
  `NewVirtualIndex(isPrimary)`) so the planner emits a `PrimaryScan`.
  **Implemented for flat roots** in `glue/flatroot.go`: `maybeFlatRoot` (called
  from `FileStore`) detects a root holding data files directly and exposes it as
  `default:<basename>`; the synthetic keyspace's `RecordsDir()` points records-scan
  at the root. Same pattern extends to catalog-defined names (§2 G) later. (Credit:
  the user's "fake it" insight — it converts B from a multi-repo fork+pin change
  into n1k1-only work.)
- **(A2) Execution-time scan/fetch — already n1k1's, no fork seam.** `conv` lowers
  `PrimaryScan`/`Fetch` to n1k1 `datastore-scan`/`datastore-fetch` `base.Op`s run
  by `glue.DatastoreOp`. Those glue ops *today* delegate down to cbq
  (`scan.Index().ScanEntries()` / `keyspace.Fetch()`, which do the
  `ReadDir`/`ReadFile` + `value.AnnotatedValue` boxing) — **but they don't have
  to.** For new formats/layouts, n1k1's glue scan/fetch ops **read the directory
  and decode records directly** (glue's `Store` knows the root; the keyspace gives
  ns+name ⇒ the dir path), yielding `base.Val = []byte` and bypassing cbq's
  boxing. So the earlier `OpenRecordStream`/`FetchRecord` fork seams are **not
  needed** — that leaf logic lives in n1k1, exactly because n1k1 owns execution.

  All decoders/layout/doc-ID/compression code is ordinary n1k1 — now the
  `records` package, registered via the existing IoC pattern
  (`engine.ExecOpEx = glue.DatastoreOp`, save/restored in `Session.Run`).
  **CONFIRMED — the convention-relaxation MVP (and flat roots, CSV, office, gzip)
  shipped with zero fork changes**, exactly as predicted: the keyspace resolves
  (or is faked via `glue/flatroot.go`), and n1k1's glue scan op enumerates the
  files + decodes them.
- **(B) Wire path-B `op_scan.go` ops to `FROM` — rejected.** FROM resolution is in
  the planner, upstream of `conv`; the engine ops never see a keyspace. Path B
  stays a low-level primitive — and a host for the shared byte-oriented decoders.
- **(C) A new n1k1-side `datastore.Datastore` — deferred.** Cleanest isolation but
  re-implements the whole datastore/index interface; only if even the discovery
  seam proves insufficient.

Things to get right: **(1) the discovery seam returns a *minimal* keyspace** —
primary index only, `Count()` may be lazy/0 (safe while `useCBO=false`); it exists
to make the planner emit a `PrimaryScan`, not to serve data. **(2) boundary types**
— it traffics in `datastore.Keyspace`/`errors` (glue already imports them); a few
fork lines, impl in n1k1. **(3) global vs per-store** — prefer hanging the hook off
the store/namespace instance over a package global (`ExecOpEx` is a process global
— fine for a one-process CLI, not
two stores at once); same save/restore either way.

## Compiler compatibility (don't break the Futamura path)

n1k1 is an interpreter **and** a compiler; a data-source design that only works
in the interpreter would silently break compiled queries for the *most common*
shape (`FROM <file>`). The good news: **if FROM-file scans keep flowing through
the existing `datastore-scan`/`datastore-fetch` op path, they compile for free.**
That path is already compiler-safe — the ops carry only int `Temps`-indices as
Params; the live datastore object arrives at runtime via `SetupCompiled*`
re-planning (see `test/suite_compiler.go`). This is the same bridge the recent
subquery/CTE ops use.

Two consequences for this design:
- **Do NOT introduce new engine scan *kinds* for new formats.** A new
  `parquetData` engine op would need its own bake/emit support and would fork the
  interpreter/compiler paths again. Instead, decode the format **inside the
  existing glue `datastore-scan`/`datastore-fetch` ops** (which read+decode
  n1k1-side; see "Where this code lives" A2), so the op *kind* is unchanged and the
  compiler differential keeps passing untouched.
- **Anything that can't be a Go literal must arrive via `Temps`.** A live
  `RecordSource`/file handle/decoder is not bakeable; it must be supplied at
  runtime like the store is today. Keep format/layout *choices* (which are static
  strings/ints) in Params, and live handles in `Temps`.
- **Test hook:** the queryCases compiler-differential harness
  (`test/query_compiler_test.go`) is exactly where a `FROM read_csv-style` or
  `FROM multi-file-keyspace` case should be added, so every new format is proven
  to compile, not just interpret.

## MVP line (what actually moves the needle next)

The full doc describes ~9 subsystems (formats × layouts × compression ×
containers × office/OCR × encryption × doc-IDs × manifests × incremental
indexing). That is "rebuild DuckDB + a lakehouse table format + Tika + a KMS."
**Draw the line here — the ~2-week win that makes n1k1 meaningfully more useful:**

> **MVP = relax the file datastore (home A) so a keyspace directory is the union
> of *all* its files (recursing subdirs), and add two decoders — JSONL and
> multi-doc JSON — plus transparent `.gz`. Prove one multi-file-keyspace case in
> the compiler differential.**

That is phasing steps 1, 2a, and the gzip half of 3. It delivers ~80% of the
"DuckDB for SQL++/JSON" value against the data people already have, is additive
to the fork, and is compiler-transparent.

> **✅ SHIPPED — and then some.** The MVP above landed, and in practice the line
> was crossed cheaply: **CSV/TSV (J), office/PDF extraction (L), flat-root
> keyspaces (B), and `COUNT(*)` pushdown also shipped**, because CSV and office
> both turned out to fit the *opaque-document path* (emit a JSON object per row /
> per doc) rather than needing the typed-label work that was assumed to gate them.
> The compiler differential has flat-root + decoder cases. **Still behind the
> line:** Parquet, catalog/sidecar, manifests/zone-maps, `.zip`, zstd decode,
> encryption — see the scope note before §6.

## Design principle: separate the concerns into layers

The single biggest lesson from DuckDB is to **decouple four things** that n1k1
currently fuses into "a keyspace is a directory of json files":

1. **Record format / decoder** — turns bytes into rows/JSON values (CSV, TSV,
   JSONL, JSON-array, Parquet, log-line, extracted-document).
2. **Layout / discovery** — turns a FROM term into the *set of files* to read,
   and optionally derives extra columns from the path (partitions).
3. **Compression / container** — transparently un-gzip/un-zst, or enumerate a
   `.zip`/archive, beneath the decoder.
4. **Derived artifacts** — indexes/caches/extracted text, plus the change-
   detection metadata that keeps them valid.

Each layer should be independently pluggable. The rest of this doc designs each.

## 1. File formats

### What DuckDB provides (the reference design)
- `read_csv` / `read_csv_auto`: a **sniffer** auto-detects delimiter, quoting,
  header, and column types (and gzip compression). CSV and TSV are the same
  reader with a different delimiter.
- `read_json` / `read_ndjson`: handles both newline-delimited JSON (JSONL) and
  JSON arrays/records; `format = 'auto' | 'newline_delimited' | 'array'`.
- `read_parquet`, plus ORC/Avro/Arrow via extensions.
- **Replacement scans:** `FROM 'data/foo.csv'` works directly — a bare path in
  FROM is treated as a file to read, with the reader chosen by extension.

### Recommendation for n1k1
- Define a small `RecordSource` interface shaped **for buffer reuse, not
  convenience** (see "Allocation model" below): prefer a
  `ReadInto(rec *Record) error` (or a `ForEach(func(rec *Record) error)`
  callback) where `rec` holds **`[][]byte` field slices borrowed from a reused
  read buffer**, valid only until the next call — *not* a `Read() (value.Value,
  error)` that allocates a fresh boxed value per row. The raw per-record bytes
  hand straight to `base.Val` (`[]byte`) or to a lazy `value.NewValue(bytes)`.
- Implement decoders in priority order: **JSONL** and **multi-doc JSON** (closest
  to today's model and to SQL++'s JSON nature; the engine's `ScanReaderAsJsons`
  is a starting point) → **CSV/TSV** (one shared reader, delimiter param, with a
  DuckDB-style sniffer for header/types — **replace the naive `op_scan.go` CSV
  splitter's *parsing*, but keep its zero-copy *allocation* model**; see the
  allocation caveat on `encoding/csv` below) → then **Parquet**
  (via `apache/arrow-go`, already a dep; columnar + pushdown is a big win) →
  ORC/Avro later (deps already present).
- **Reuse the existing `primitives/external/iceberg_reader.go`** (Arrow-batch
  iteration over Iceberg/Parquet) as the backbone for columnar formats rather
  than writing a new Parquet path — it just needs wiring into a `RecordSource`.
  - **Caveat — columnar source, row engine.** n1k1's engine is row-at-a-time
    (`base.Vals` per row) and built around garbage-avoidance. Feeding Arrow
    *columnar* RecordBatches into it means transposing to rows and allocating
    per value — which throws away Parquet's columnar/pushdown advantage and cuts
    against the project's whole ethos. So "Parquet is a big win" is only true
    with a vectorized/column-batch op path the engine **doesn't have today**.
    Treat Parquet as a *correctness* feature first (you can query it at all),
    and defer the *performance* win until/unless the engine grows column-batch
    ops. This is a real reason Parquet sits well below the MVP line.
- **Type handling:** JSON formats are naturally typed/loose (SQL++'s home turf).
  For CSV/TSV, sniff types but always allow "everything is a string" fallback;
  expose per-column type overrides like DuckDB's `columns=`/`types=`.
- **Format selection:** primarily by file extension (`.csv`,`.tsv`,`.jsonl`,
  `.ndjson`,`.json`,`.parquet`), overridable by an explicit FROM-term option (see
  §2). Content sniffing only as a tiebreaker.

### Allocation model: favor read-into / borrowed-slice APIs (a first-class axis)
n1k1 is built to avoid garbage: **`base.Val` is `[]byte`** ("JSON encoded, usually
treated as immutable"), and the engine already parses values *allocation-free* via
`buger/jsonparser` (see `base/arith.go`, `base/canonical.go` — `ParseInt`/
`ParseFloat`/`Get` return `[]byte` sub-slices, never boxing). A decoder that
allocates an `interface{}`/`string`/`map[string]interface{}` **per value or per
tuple** would blow that up over a large file. So allocation behavior is a
**selection criterion on par with correctness**, not an afterthought:

- **The rule.** Prefer readers that (a) **read into a caller-owned/reused buffer**
  (`ReadInto(buf)`) or (b) **return sub-slices that borrow a reused read buffer**
  (valid until the next `Read`, à la `csv.Reader.ReuseRecord` / jsonparser),
  over any `Read() (interface{}, error)` / `Read() ([]string, error)` that
  allocates per row/value. State the **borrow/lifetime contract** on the
  `RecordSource` explicitly — "slices are valid only until the next call; copy to
  persist" — because a downstream op that retains a borrowed slice past the next
  read corrupts data. (This is exactly what `base.Val`'s "usually immutable"
  already assumes.)
- **Today already gets this right.** `engine/op_scan.go:ScanReaderAsCsv` reuses its
  row slice (`lzValsScan[:0]`) and yields each field as a **sub-slice of the
  `bufio.Scanner` buffer** — zero-copy. The replacement CSV reader must **keep
  this borrow model**; only the comma-splitting *correctness* is naive.
- **JSONL / JSON → `buger/jsonparser` (already a direct dep).** It hands back
  `[]byte` sub-slices into the input (via `Get`/`ObjectEach`/`ArrayEach`), no map
  materialization — and it's the decoder the engine already trusts. Strongly
  prefer it over `encoding/json` (a `map[string]interface{}` per doc). On the FROM
  path the raw per-record bytes go to the fork's existing lazy
  `value.NewValue(bytes)` (couchbase/query parses on demand — *not* an eager
  unmarshal), so JSONL stays near-zero-alloc end to end.
- **CSV/TSV — the real tradeoff.** Go's `encoding/csv` is correct but its
  `Read()` returns freshly-allocated `[]string`; even with `ReuseRecord=true`
  (which reuses only the slice header) the **field strings are still allocated** —
  a *regression* from today's `[]byte`-borrow model, and `string` is the wrong
  target type for a `[]byte` engine. Options, best first: (1) a `[]byte`-oriented
  CSV reader that yields field sub-slices into a reused buffer, or fix the
  hand-rolled scanner's quoting/escaping while keeping its borrow model (a bounded
  task); (2) `encoding/csv` + `ReuseRecord` as a correctness-first fallback,
  eating the string allocs. Prefer (1) so correctness and allocation both win.
- **Arrow / Parquet.** Values live in **pooled contiguous buffers** (a reusable
  `memory.Allocator` + offset arrays), so per-value allocation *inside* Arrow is
  near zero, and `array.String/Binary.Value(i)` returns a **borrowed** sub-slice —
  use those, `Release()` each batch, and reuse the allocator. But crossing into
  n1k1's row world still costs the transpose/copy of the columnar caveat above;
  the byte-borrow discipline minimizes it, it doesn't erase it.
- **Make it measurable.** Treat **allocations/op** (`go test -benchmem`, using the
  existing `benchmark/` harness) as an acceptance metric for every decoder —
  "allocs per row" is the number to hold near constant regardless of file size.

#### Measured (2026-07): the *fetch* path is exactly where this discipline breaks

A heap profile of a 3-way nested-loop self-join
(`SELECT COUNT(*) FROM orders o1, orders o2, orders o3`, 262,144 rows;
`n1k1 -stats=final -profile-mem`) allocated **~931 MB** — yet only **~3 MB live at
exit**, so it's pure GC churn, not a leak. The breakdown (via `go tool pprof
-alloc_space`):

- **~71% is `glue.DatastoreFetch` → the fork's `query/datastore/file.(*keyspace).Fetch`
  (662 MB).** Unlike the native scan, the *fetch* path materializes cbq
  `value.AnnotatedValue`s (`value.init`, `annotatedValue.SetMetaField`) and
  re-parses with **`encoding/json.Unmarshal`, not jsonparser** — precisely the
  eager, per-doc boxing this section warns against. (`DatastoreScanRecords`, by
  contrast, yields `base.Val = rec.Doc` raw bytes and honors the borrow contract.)
- **~120 MB** copying file bytes onto the heap (`os.ReadFile` / `readFileContents`).
- **~135 MB** `readdir`/`lstat`/`keyPath` *locating* each doc by key — there is no
  key→path index, so fetch-by-key re-walks the directory.
- All of it amplified **O(|L|×|R|)** because the nested-loop join re-fetches the
  same docs hundreds of thousands of times.

Leverage order (biggest first): **(1)** don't re-read the same doc 262K times — a
build-side/hash join or a small decoded-doc cache; **(2)** route fetch through the
**native byte path** (`base.Val`+jsonparser, as `DatastoreScanRecords` already
does) instead of cbq `Fetch`, erasing the boxing + `encoding/json` garbage;
**(3)** a key→path index to kill the `readdir`/`stat` churn; only **(4)** then the
read-copy itself (below). The read technique is the *last* lever, not the first.

#### mmap vs read-into — choose per file shape, not universally

`blevesearch/mmap-go` is already an (indirect) dep, and mmap-ing a file as a
`[]byte` is genuinely zero-copy — jsonparser subslices straight into the mapping.
But it only removes the ~120 MB read-copy above (not the boxing/parse/locate cost),
and it has sharp edges:

- **Lifetime / SIGBUS — bounded by an existing contract.** An mmap `[]byte` is
  valid only while mapped, so a retained `base.Val` subslice would dangle into
  unmapped memory (a **segfault, not a nil-panic**). But n1k1's `YieldVals` contract
  *already* says a consumer "should copy any inputs it wants to keep, because the
  provided slices might be reused by future invocations" (`base/base.go`), and it is
  **load-bearing today** — the scan yields sub-slices of one reused per-row buffer
  (`lzValsScan[:0]`) and `Stage` deep-copies (`ValsDeepCopy`) at actor boundaries.
  So anything that *retains* data (a join build side, a GROUP BY map, an ORDER BY
  heap, a cross-actor batch) already holds a **copy**, not the borrowed bytes.
  Consequence: a mapping only has to outlive the **scan of its own file** (until the
  last yield for that file returns), not the whole query — unmap at end-of-scan is
  safe *as long as the contract is honored*. The residual risk is narrower than it
  first looks: a contract **violation** (an op that keeps a borrowed slice without
  copying) is a silent data-corruption bug *today* (the reused buffer clobbers it on
  the next row); under mmap the same latent bug becomes a **delayed SIGBUS at
  unmap**. So mmap mostly changes that bug class's failure mode (wrong-results →
  crash) rather than adding an architectural burden — worth a hardening/`-race`-style
  pass over retention sites, not a redesign.
- **Bad for large / compressed / container files.** mmap suits *uncompressed,
  byte-addressable, partially-read* files. `*.jsonl.gz` / `.zst` (§3) hand you the
  **compressed** bytes — you must stream-decompress into a buffer anyway, so mmap
  buys nothing. PDF/PPTX/XLSX (`extract` provider, §4) are *extracted*, not
  sub-sliced; the allocation is the extracted text, not the container read.
- **Bad for many tiny files.** Today's layout is one JSON doc per file; mmap has
  per-call syscall + page-fault overhead and 4 KB page granularity, so mapping a
  200-byte doc costs more than reading it. mmap's payoff is coupled to a
  **packed/segment layout** (few large files, §5-adjacent), not one-file-per-doc.
- **The portable alternative: read-into a reused buffer.** Go's
  **`io.ReaderAt.ReadAt(p []byte, off int64)` *is* the `ReadInto(prealloc, pos,
  numBytes)` API** — it reads a chosen range into a caller-owned slice (`*os.File`
  implements it). Paired with a pooled / per-actor reused buffer, reads become
  amortized zero-alloc **without** mmap's lifetime hazard, work for large files
  (read only the needed range), and skip page-granularity waste. It copies once,
  into memory you own and recycle, rather than zero-copy. Rule of thumb: **mmap for
  a packed segment of many docs; `ReadAt` + reused buffer for everything else**
  (large, compressed-after-decompress, or per-doc).

#### Push down what the query needs to the fetch/reader

The cheapest read is the one you skip. Thread the set of referenced fields (and
predicates) down to the scan/fetch op so it can do less:

- **`_meta`-only queries need no file read at all.** `_meta` (path/name/ext/size/
  mtime — §6) comes from the directory entry / `stat`, which the scan already holds;
  `SELECT _meta.name, _meta.size FROM ks WHERE _meta.size > …` (or a bare
  `COUNT(*)`) should answer straight from `readdir` results and never open a doc.
  This is the extreme of projection pushdown and ties to the pruning-visibility
  story in `DESIGN-stats.md`.
- **Partial decode for the fields you do use.** jsonparser's `EachKey` / `Get(path…)`
  pulls only the requested paths out of the raw bytes — no whole-object
  materialization — so a query touching 2 of 50 fields parses ~2 fields. This is
  pushdown *into* the decode step; it needs the referenced-path set threaded down
  from `conv`.
- **Range pushdown for big / columnar files.** With `ReadAt`, a reader fetches only
  the byte ranges it needs (a Parquet column chunk, a record at a manifest-known
  offset — §5), reading kilobytes of a gigabyte file. mmap gives this "for free"
  via the page cache, but with the caveats above; `ReadAt` gives it explicitly and
  safely.

Net: the allocation win is less about *how* we get bytes (mmap vs read) than about
**not materializing, not re-reading, and not reading at all what the query doesn't
touch** — with `ReadAt`+reused-buffer and field/`_meta` pushdown as the portable
levers, and mmap reserved for a packed-segment layout.

#### Implemented (2026-07): native byte-path fetch (`glue.DatastoreFetch`)

The lever above — "route fetch through the native byte path instead of cbq
`Fetch`" — has landed, **entirely inside n1k1** (no fork change; we only read the
fork to mirror its behavior). For the classic directory-backed file keyspace,
`DatastoreFetch` now reads each `<dir>/<key>.json` directly into a **reused,
growable buffer** via `io.ReaderAt.ReadAt(buf, 0)` and yields those raw JSON bytes
as `base.Val` — no `value.AnnotatedValue` boxing, and **no standard-JSON parsing**
in the path at all (even the `^id` key is decoded with jsonparser, not
`encoding/json`). Faithful drop-in: cbq's file keyspace is `.json`-only
(`keyPath = <base>/<key>.json`), so the native read replicates its key→path exactly,
including the **path-traversal guard** (`filepath.Rel` check) and the
missing-file-⇒-skip semantics. Measured on the 3-way `orders` self-join (identical
results): **total allocation ~2.0 GB → ~917 MB (~54%), the fetch subtree
~1468 MB → ~377 MB (~74%), GCs 420 → 200.** `DatastoreFetchNative` (env
`N1K1_FETCH_CBQ=1` forces the old path) toggles it for A/B.

- **Fallbacks (still cbq `Fetch`).** A subpath projection was pushed down
  (`SubPaths`), or the keyspace is a synthetic **flat-root** (`RecordsDir`) or
  **single-file** (`RecordsFile`) keyspace — i.e. not a directory of standalone
  `<key>.json` files. So a **container-backed** keyspace (docs inside a `.jsonl`
  or `.gz`) never gets a bogus `<key>.json` native read; it stays on the cbq/scan
  path (and cbq's `.json`-only Fetch can't fetch-by-key into a container either, so
  those are scan-only today regardless).
- **Done: per-request doc cache (the "don't re-read" lever).** The residual after
  the byte path was ~377 MB of per-key file-open churn (`os.Open`/`Stat` →
  `syscall.ByteSliceFromString`, `os.newFile`) — the nested-loop join re-opening the
  same files O(|L|×|R|) times. `GlueContext.fetchCache` now memoizes doc bytes per
  request, two-level (keyspace dir → doc key → owned, immutable copy), so after the
  first pass every fetch is a map hit with **no re-open, no re-read, no allocation**
  (and no per-fetch path building — the `<dir>/<key>.json` string and its
  path-traversal guard run only on a miss). It's guarded by a mutex (UNION-ALL
  actors share the `GlueContext`), bounded by `DatastoreFetchCacheMaxBytes` (64 MiB;
  a miss past the cap reads into the reused buffer instead of caching), and lives
  for exactly one request (fresh `GlueContext` per `Run`). Cached bytes are stable
  for the whole request — *safer* than the borrow contract needs. Measured on the
  3-way `orders` self-join (identical results): the **fetch subtree 377 MB → 78 MB**
  (~79%), and **total ~917 MB → ~541 MB** (native+cache vs native-no-cache; ~73%
  below the original cbq `Fetch` at ~2.0 GB). `DatastoreFetchCache` toggles it; env
  `N1K1_FETCH_NOCACHE=1` disables. The now-dominant ~470 MB is the *scan* re-listing
  the directory (`primaryIndex.Scan` → `readdir`/`lstat`) O(|L|²) times — a separate
  lever (a per-request directory-listing cache on the scan side), not the fetch.
- **Container fetch is future work.** Fetch-by-key *into* a `.jsonl` needs a
  manifest offset index (§5) + `ReadAt(buf, offset)` to read just that record;
  a `.gz` can't be range-read (must decompress a stream), so it needs either full
  decompression + an in-memory/segment index or a packed uncompressed segment. Both
  are exactly the mmap-vs-`ReadAt` tradeoffs above, keyed off a manifest.

## 2. Directory layouts & FROM-term resolution

This is the crux of the user's question: flat vs two-level vs deep, auto-detect
vs explicit.

### What the ecosystem does
- **DuckDB does not force a layout.** You point a glob/list at files:
  `'dir/*.csv'`, recursive `'dir/**/*.csv'`, brace `'{a,b}/*.json'`, or a list
  `['a.csv','b.csv']`. `union_by_name=true` merges files with differing schemas
  by column name; `filename=true` adds a source-file column. There is no
  mandatory "one directory per table."
- **Hive partitioning:** for paths like `.../year=2026/month=01/file.parquet`,
  DuckDB auto-detects the `key=value` segments and exposes them as virtual
  columns, enabling **partition pruning** (`WHERE year=2026` skips whole files
  before reading). It only works for the `key=value` naming convention. Spark,
  Hive, and Trino do the same.
- **Bare date-partition dirs** (`.../ecommerce/20260101/*.log.gz`, *no*
  `key=value`) are the user's "almost invisible container" case. The ecosystem
  answer is **AWS Athena partition projection**: instead of *listing* the
  directory tree, you declare a template — a `date` column with a `range` and a
  `format`/`storage.location.template` — and the engine **computes** the candidate
  paths from the query's predicate. This both avoids expensive listing and makes
  the container dirs invisible as far as the schema is concerned.
- **Log-specific tools** (lnav, etc.) auto-detect log formats and timestamps;
  Loki/Vector attach labels rather than relying on directory structure.

### Recommendation for n1k1: convention by default, explicit when needed
Support **three resolution modes**, in increasing power:

1. **Convention (zero-config), backward-compatible.** Keep today's
   `<dir>/<namespace>/<keyspace>/...` as the default. Relax it so a keyspace
   directory may contain *any* supported format (not just `*.json`), and may
   contain **many records across many files** (a directory = a keyspace = the
   union of its files), not just one-doc-per-file. Recurse into subdirectories by
   default so deep/partitioned layouts "just work" as an unkeyed union.
   - **Do NOT force two levels.** Allow a flat root too: if `<dir>` directly
     contains data files (no namespace/keyspace subdirs), treat the directory
     name (or a default) as the keyspace. Auto-detect: if subdirs contain data
     files, they're keyspaces; if `<dir>` itself contains data files, it's a
     single flat keyspace. This mirrors DuckDB's "no mandatory layout."
   - **Go one step flatter: the CLI arg may be a *single file*, not a dir**
     (**shipped**). The smallest onboarding case — "I have one `events.jsonl` (or
     `dump.ndjson`, or `orders.jsonl.gz`) and nothing else" — just works:
     `n1k1 -c "SELECT * FROM events" events.jsonl`. If the CLI arg is a regular
     file (not a directory), it's a one-file keyspace named after its base name
     *with the record/compression extensions stripped* (`events.jsonl` →
     `events`, `orders.jsonl.gz` → `orders`). The natural sibling of flat-root —
     same "fake the metadata" trick, no catalog, no deeper directories — closing
     the gap where DuckDB's `FROM 'foo.jsonl'` replacement scan had no n1k1
     equivalent. See worked example **B2**.

2. **Explicit table functions / globs in FROM** (DuckDB-style power mode) —
   **blocked on a grammar fork; not the near-term power path.** The aspiration is
   `FROM read_json('logs/**/*.jsonl.gz') AS t` / `FROM read_csv('sales/*.csv',
   header=true) AS t`. **Empirically, the fork's parser does not support this
   today:** `FROM read_csv('foo.csv')` fails with *"Invalid function read_csv
   (resolving to default:read_csv)"* (it's parsed as a scalar function call, not
   a table source), and bare `FROM 'foo.csv'` fails with *"FROM expression term
   must have a name or alias"*. There is **no table-valued-function machinery in
   the fork's `algebra/`** to hook into. So mode 2 is not "syntax TBD" — it
   requires **patching the goyacc grammar + adding a `FromTerm`/algebra node +
   planner support** in the fork, which is exactly the kind of deep, merge-hostile
   fork change we're trying to avoid (the grammar is generated and painful to
   re-base). **Verdict: defer mode 2; make the catalog (mode 3) the real power
   path.** If we ever do want inline globs, the cheapest surface is probably a
   thin `read_csv(...)`-shaped *keyspace-name convention* the datastore
   recognizes, not a true grammar extension — but even that is post-MVP.

3. **Catalog / sidecar mapping** for named, reusable, partitioned sources — **the
   realistic power path, since mode 2 needs a grammar fork.** A
   per-root config (e.g. `.n1k1/catalog.json`) maps a keyspace name to: a root
   glob, a format, partition columns (hive or **projected** date templates à la
   Athena), and compression. This is where the "invisible date container dirs"
   case is handled cleanly: declare `ecommerce` → `ecommerce/{date:YYYYMMDD}/*.log.gz`
   with `date` as a projected partition column, so `WHERE date >= ...` prunes by
   *computing* directory names instead of listing them.

**Auto-detect vs override:** use convention for the common cases and the
**catalog (mode 3)** for control; mode 2 (inline globs) is deferred per above.
Hive `key=value` partitions auto-detect within any mode; bare date partitions
require declaring a projection template (mode 3) because they're ambiguous by
construction.

**Lockdown flag (`-formats`):** flexibility is the default, but a user whose tree
contains subdirs/formats they *don't* want scanned can restrict n1k1 to an
explicit comma-separated set, e.g. `-formats=json,jsonl` (no `recurse` ⇒ don't
descend; no `gzip` ⇒ ignore `.gz`). Empty/absent or `all` ⇒ everything flexible.
This is "flexible by default, explicit when needed" as a one-flag safety valve;
`records.ParseModes` turns the list into the discovery/decoder filter. The REPL's
`.formats` command shows/sets it live and **persists** the set into the
datastore's `<sidecar>/catalog.json` (`"formats"` field), so a directory
remembers its formats across sessions. Precedence on open: an explicit `-formats`
flag wins, else the persisted catalog value, else the flexible default. (Named
`-formats`, not `-mode`, to avoid confusion with the `-mode` output flag.)

### Integration gap: schemaless docs vs n1k1's positional labels
n1k1's engine identifies fields by **positional `base.Labels`**, not by name —
an op tree is built against a known label vector. A multi-file keyspace whose
files have *different* shapes (the `union_by_name` case) has no single fixed
label vector, and JSON docs are schemaless to begin with. So a `RecordSource`
can't just "yield rows"; the FROM path has to settle on a label vector the plan
was built against. Two workable stances, both post-MVP-friendly:
- **Opaque-document scan (recommended default, matches today).** Yield each
  record as a single self-value (as the file datastore does now — one document
  in, projections pull fields by name at expr-eval time via the value layer),
  so the scan needs only a trivial label vector and heterogeneous shapes "just
  work." This is why the MVP (JSONL/multi-doc JSON) is easy: those are just more
  documents on the existing opaque-doc path.
- **Typed/columnar labels (CSV/Parquet).** Formats with a real header/schema
  *do* have a stable column set; there the `RecordSource`'s inferred schema
  becomes the label vector, and `union_by_name` across files means computing the
  union column set up front (a listing pass) or falling back to opaque per-row
  objects. Partition virtual-columns (§ hive/projected) would be appended to
  that vector. This was billed as *the* actually-hard part of "beyond JSON."
  - **Reframed by what shipped.** CSV/TSV landed **without** doing any of the
    typed-label-vector work — the decoder converts each data row into **one JSON
    object keyed by the header** (with light int/float/bool inference), so CSV
    rides the same **opaque-document path** as JSON and `union_by_name` becomes
    trivial (heterogeneous objects just coexist). Office extraction did the same
    (one `{filename,kind,text}` object per doc). So the "labels are the real cost
    boundary" claim was **too pessimistic for row-shaped formats**: emit-JSON
    sidesteps it. The typed/columnar-label reconciliation is only genuinely
    forced by **columnar Parquet** (where you want the columns *without*
    materializing a JSON object per row, or you lose the whole point) — which is
    the real reason **Parquet** (not CSV) still sits above the line. Hive/projected
    partition virtual-columns are the other case that still wants real labels.

## Query-defined virtual datasources (VIEWs & generated catalogs)

**The idea (user's S3 scenario):** a bucket's ingest layout/schema *morphed over
time* — an early era wrote flat `{ts, user}` JSON, a later era renamed fields and
nested them, a third switched to Parquet under `year=/month=` dirs. You want all
of it to look like **one coherent keyspace** — a **VIEW** — so `FROM events` just
works and the historical mess is hidden. This is a natural, high-value extension
of the catalog (§2 mode 3), and it splits into two distinct capabilities:

- **(a) VIEW = a catalog entry whose definition is a SQL++ query.** `FROM events`
  expands to a stored `SELECT` that *unions and normalizes* the heterogeneous
  physical sub-sources into one shape. The reshaping (rename, nest/unnest,
  cast, add-missing-as-NULL, tag with an era column) is expressed in SQL++.
- **(b) Generated catalog = a query that *produces* the catalog itself.** Instead
  of hand-listing partitions, a bootstrap query over a listing/metadata source
  (an S3 inventory, a Glue table, a manifest file) *emits* the set of
  sub-sources + derived partition columns. The physical layout is described by
  data, not static config — the crawler pattern.

### Why this fits n1k1 unusually well: a view is an implicit WITH binding

The expansion machinery **already exists** — it's the WITH/CTE stack recently
built in the glue layer. `Conv` threads `withBindings` (a `map[string]With`)
through conversions; `FROM <cte>` expands via the CTE / FROM-subquery path
(`VisitExpressionScan` / `VisitAlias`); CTE-referencing-CTE is threaded; and
`WITH RECURSIVE` runs a fixpoint. **A catalog VIEW is just an implicit,
always-available WITH binding**: before planning, seed `Conv.withBindings` (or
rewrite the FROM term) from the catalog, so

```sql
FROM events            -- events is a catalog view
```
is planned exactly as if the user had written
```sql
WITH events AS ( <the view's stored SELECT> ) SELECT … FROM events
```
Consequences, all leveraging work already done:
- **Pure glue-layer** — no fork/datastore change for the *expansion* itself
  (the sub-sources it reads are ordinary catalog keyspaces).
- **Views over views** compose for free via the existing CTE-ref-CTE threading.
- **Recursive views** ride the existing `WITH RECURSIVE` fixpoint.
- **Compiler-safe** — expansion happens before `conv`, so compiled and
  interpreted paths are identical (see "Compiler compatibility").

### The one real blocker for the morphing-schema case: UNION ALL (TODO)

The normalizing view is a union of per-era projections:
```sql
-- catalog view "events" (schema reconciliation across eras)
  SELECT ts,             user_id,      action, "era1" AS _era FROM events_era1
  UNION ALL
  SELECT event_time AS ts, uid AS user_id, act AS action, "era2" FROM events_era2
  UNION ALL
  SELECT meta.ts,        meta.user AS user_id, kind AS action, "era3" FROM events_era3
```
**`plan.UnionAll` is `NA()` in `glue/conv.go` today** (verified: the parser and
planner accept `UNION ALL`, but conv rejects it). So this view shape is blocked
until `VisitUnionAll` (and likely `VisitUnion`/distinct) is implemented. Good
news: it's a **bounded** task — the recursive-CTE work already built the
union *execution* substrate (data-staging batches + `trackSet` dedup in
`glue/recursive.go`), so what's missing is mainly the top-level `plan.UnionAll` →
`base.Op` conversion, not a new engine capability. **This is the single
prerequisite** and belongs on the roadmap before query-defined views are useful
for the morphing case. (Views that *don't* union — a single reshaping `SELECT`
over one evolving source — work as soon as the catalog-view expansion lands,
without UNION.)

### S3 / object store: orthogonal, but the deps are already here

The VIEW idea is independent of *where* bytes live — but the scenario is remote,
and n1k1 reads local files today. That's a separable backend concern, and it's
**dep-ready, not from-scratch**: `go.mod` already carries (indirect)
`aws-sdk-go-v2/service/s3` + `feature/s3/manager`, **`aws-sdk-go-v2/service/glue`**
(AWS's own data-catalog service), and `gocloud.dev` (whose `blob` package
abstracts S3/GCS/Azure). Two implications:
- An **object-store `RecordSource` backend** (via `gocloud.dev/blob` for
  portability, or `aws-sdk` directly) slots under the same decoder/layout layers
  as local files — the catalog's `root`/`layout` glob just points at `s3://…`.
- The presence of the **Glue** client hints at capability (b): read an existing
  **Glue Data Catalog** as the generated catalog, rather than crawling raw S3.

### Virtual vs materialized (ties to §5)

- **Virtual view** — re-expanded and re-scanned on every query. Simple, always
  fresh, but pays the full union/normalize/scan cost each time, and depends on
  **predicate pushdown through the view** to be fast (see below).
- **Materialized view** — run the view once, cache the flattened, normalized rows
  as a derived artifact in `.n1k1/` (a snapshot keyspace), rebuilt via the §5
  change-detection manifest when any underlying sub-source changes. This is the
  performance answer for expensive normalization over huge, mostly-static
  historical trees — and it's exactly what the manifest + sidecar are for.

### The hard part: predicate pushdown through a view

A `WHERE ts >= '2023-01-01'` on `events` must reach the *sub-source* scans — and
ideally prune whole eras/partitions (§2 F/G) — or the view reads all of history
every time. After expansion the planner sees a union of subqueries; whether it
pushes the outer predicate into each branch (and thence to partition pruning)
depends on cbq-query's rewrite rules and on the §5 predicate-to-scan work. **Flag
as the key open question** for views: a naive virtual view is correct but can be
catastrophically slow on morphing S3 histories; materialization or pushdown is
what makes it practical.

### Prior art
- **DuckDB `CREATE VIEW` + macros** — query-defined logical tables/functions.
- **Trino / Athena VIEWs + the Glue crawler** — the crawler *generates* the
  catalog from S3 (capability b); views normalize on top.
- **Iceberg schema evolution** — the lakehouse's native answer to "schema morphed
  over time": column-ID mapping makes renamed/added/dropped columns coherent
  across snapshots *without* a union view. A query-defined view is the
  poor-man's version when the data was never written as a managed table — worth
  saying explicitly, because "just put it in Iceberg" is the alternative.
- **dbt models** — query-defined, optionally materialized, dependency-tracked
  transformations; the materialized-view lifecycle mirrors §5.

### Recommendation
Model views as **catalog entries with a `query` field** (a stored SQL++ `SELECT`),
expanded as implicit WITH bindings before planning — reusing the CTE machinery,
no datastore change. **Sequencing:** (1) single-source reshaping views land with
the catalog-view expansion; (2) union/normalize views unblock once `VisitUnionAll`
is implemented; (3) object-store backend and generated/Glue catalogs are a
separable track; (4) materialization + pushdown are the performance follow-ups
tied to §5. See worked example **O**.

**Not the route: the fork's `datastore/virtual` package.** It's tempting (name
aside) but it's a *metadata-only, planner-facing shim*, not a datasource:
`virtualKeyspace` refuses `Fetch`/mutations (`Count()`→0) and `VirtualIndex.Scan`
is an **empty body** that yields nothing. It exists so the planner can hold a
keyspace/index *object* where the interface requires one without backing data
(`Namespace.VirtualKeyspaceByName`, system keyspaces) and — its one active use —
to run `SargableFor` against a throwaway `partitionVirtualIndex` for **partition
elimination** (see §2 F). Macro-expansion is better for views anyway: the view
name is rewritten away before planning, so no keyspace object is ever needed.
(The keyspace interface's `ExternalScan`/`IsExternalCollection` looked like a
cleaner external-data seam, but it's only really implemented on the EE couchbase
*collection* path — file/mock/virtual all `conn.Fatal` — so it's not a usable
hook on the file datastore.)

## 3. Compression & containers

### What DuckDB does
- gzip (`.gz`) and zstd (`.zst`) are decompressed **transparently**, detected by
  file extension; JSON supports gzip+zstd, CSV historically gzip-focused. No temp
  files. `.zip` archives are **not** transparently read (a zip is a container of
  many entries, not a single compressed stream) — repeatedly requested but
  treated as out of scope for direct scan.

### Recommendation for n1k1
- Treat **single-stream compression** (`.gz`, `.zst`, optionally `.bz2`/`.xz`)
  as a transparent decode layer *under* the decoder, keyed off the inner
  extension (`foo.jsonl.gz` → gzip → JSONL). Use Go's `compress/gzip` and
  `klauspost/compress` (`zstd`, already a dep) — both well-tested.
- Treat **`.zip`** as a *container/layout* concern, not a codec: enumerate its
  entries (`archive/zip`) and feed each entry through format detection, exactly
  as if the zip were a directory. This also covers the common "office" formats
  in §4 (docx/xlsx/pptx are zip files).
- Caveat: gzip/zstd streams aren't seekable, so columnar formats (Parquet) lose
  random-access/pushdown when gzipped — fine for row formats (CSV/JSONL/logs),
  document this limitation.

## 4. The `extract` provider — unstructured documents (PDF, DOCX, XLSX, PPTX, …)

Aspiration: crack open unstructured files, extract their content as queryable
rows, and optionally full-text index them (ties directly to the FTS/bleve work in
`DESIGN-indexing.md`).

### Model
Add an **extractor** kind of `RecordSource`: input is one document file, output
is one row (with `filename`, extracted `text`, and metadata like author/title/
page-count/sheet/row), or *many* rows for inherently tabular docs (one row per
spreadsheet row, one row per slide/page). The extracted rows then flow through
the normal pipeline and can be fed to a bleve FTS index for content search.

### Libraries (well-tested **and** permissively licensed — see the licensing note)
Document extraction is where the licensing landmines are, so the choices below are
constrained to MIT/Apache-2.0/BSD only.
- **Breadth, "bulletproof":** Apache **Tika** (Apache-2.0, 60+ formats) — Java,
  run as a sidecar server — or **`extractous`** (core Apache-2.0; Rust engine
  wrapping Tika + Tesseract OCR, both Apache-2.0; Go bindings exist). Recommended
  for *breadth* and scanned-PDF OCR, at the cost of a cgo/native dependency.
- **Pure-Go, narrower:** `xuri/excelize` (XLSX; **BSD-3**), `ledongthuc/pdf`
  (**BSD-3**) and/or `pdfcpu` (**Apache-2.0**) for PDF text.
- **Avoid (viral / non-permissive):** `go-fitz` and anything else wrapping
  **MuPDF** — **AGPLv3**. **UniDoc/unipdf** — **AGPL/commercial** dual license.
  `sajari/docconv`, though MIT itself, shells out to GPL binaries (`wv`,
  `poppler-utils`, `unrtf`, `antiword`) — avoid unless restricted to
  permissive-only backends. These are the common PDF/office traps; don't reach
  for them.
- **Recommendation:** a pluggable extraction backend with two implementations — a
  pure-Go default (excelize + ledongthuc/pdf or pdfcpu) and an optional
  Tika/extractous backend (build tag) for breadth + OCR. Both stay within the
  permissive-license policy. Office docs being zip-based dovetails with §3.

## Worked examples: sample input trees and their FROM clauses

Concrete layouts a user might drop on disk, and the exact `FROM` that reads them.
This is the section to argue with when making decisions — if a layout below has an
awkward or ambiguous `FROM`, that's a design smell to fix here first.

CLI invocation is `n1k1 [-c "<stmt>"] [-ns <namespace>] <dataRoot>` (default
`-ns default`). The datastore maps `<dataRoot>/<namespace>/<keyspace>/…`, so
`FROM default:orders` reads `<dataRoot>/default/orders/`. `<dataRoot>` is the last
CLI arg. Status legend, tied to Phasing:

- ✅ **implemented & shipped** (works today — includes the MVP *and* the
  post-MVP decoder items that already landed: flat-root, JSONL/multi-doc JSON,
  CSV/TSV, office/PDF, gzip)
- 🟡 **not yet built, decoder/convention** (Parquet, `.zip`, zstd — no catalog)
- 🟣 **not yet built, needs the `.n1k1/catalog.json` sidecar**
- 🔴 **deferred, needs a grammar fork** (inline table functions)

### A. Today's convention — one JSON document per file  ✅
```
shop/
  default/
    orders/     order-001.json  order-002.json  order-003.json
    customers/  alice.json       bob.json
```
`n1k1 -c "SELECT * FROM default:orders WHERE total > 100" shop`
→ reads `shop/default/orders/*.json`; `META().id` = filename stem (`order-002`).

### B. Flat root — a bare directory of files = one keyspace  ✅
```
sales/          2026-01.json  2026-02.json  2026-03.json
```
`n1k1 -c "SELECT * FROM sales" sales`
→ `sales/` holds data files directly (no ns/keyspace subdirs), so auto-detect
treats the whole dir as a single flat keyspace.
**Decision (RESOLVED — basename).** `glue/flatroot.go` names the keyspace after
the root's basename (`filepath.Base`), under a synthetic `default` namespace
(`default:sales`), matching the recommendation here.

### B2. Single file as a keyspace — no directory at all  ✅
```
events.jsonl              # just one file on disk; no shop/ dir, no default/ dir
```
`n1k1 -c "SELECT type, COUNT(*) FROM events GROUP BY type" events.jsonl`
→ the CLI arg is a **regular file**, not a directory. Auto-detect treats it as a
one-file keyspace named after the base name with record/compression extensions
stripped (`events.jsonl` → `events`; `orders.jsonl.gz` → `orders`;
`dump.ndjson` → `dump`). This is the "I just have a single JSONL/NDJSON/`*.gz`
file" case — the flattest possible onboarding, and DuckDB's `FROM 'foo.jsonl'`
replacement-scan analogue.
**Shipped** — the same "fake the metadata" move as flat-root (B), one level
flatter:
- `FileStore` (`glue/stmt.go`) `os.Stat`s the CLI arg; when it's a **regular file**
  and `records.IsRecordFile` accepts it, the fork's file datastore (which `ReadDir`s
  its root and so *cannot* be handed a file) is built against the file's **parent
  dir**, then wrapped by `maybeFlatFile` — advertising a synthetic
  `default:<stem>` keyspace (`records.Stem` strips the format + compression
  extensions). A non-record regular file falls through unchanged, so it errors as
  before instead of silently mis-resolving.
- The synthetic keyspace carries a `RecordsFile()` (sibling of flat-root's
  `RecordsDir()`); `DatastoreScanRecords` prefers it and calls `records.File(path,
  opts)` — a one-file `Source` (base name as the synthetic-ID prefix) — instead of
  `records.Walk(dir, opts)`, keeping the dir-walk and single-file paths from
  entangling. `-formats` lockdown still applies (a `.gz` file under `-formats=jsonl` is
  rejected).
- Compiler-transparent for free: still a `PrimaryScan` → `datastore-scan` op, no
  new op kind (see "Compiler compatibility"). Covered by `test/flatfile_test.go`.
`META().id` = `events.jsonl#57` for JSONL (base name + line index), or the file
stem for a single-document `.json` (matching scenario A).

### B3. Grab-bag directory — loose files + unrelated subdirs  ✅
```
~/Desktop/
  people-100.csv          organizations-100.csv     Sales Transaction.csv
  2025-W2.pdf   budget.xlsx   notes/   screenshots/   projects/   ...
```
`n1k1 -c "SELECT * FROM \`people-100\` LIMIT 5" ~/Desktop`
→ a *casual* directory that has loose data files **and** unrelated
subdirectories (the classic `~/Desktop`). Previously this reported **"no
keyspaces"**: the file datastore read every subdir as a `namespace`, which
suppressed flat-root detection (scenario B), so the loose files were never
exposed and `default` was empty.
**Resolved.** `glue/flat.go` `maybeFlat` now, when a directory has subdirs *and*
loose top-level record files, exposes **one keyspace per top-level structured
file, keyed by stem** (`people-100.csv` → `` `people-100` ``) — the
"directory = database, file = table" model. It differs from scenario B (no
subdirs → one union keyspace by basename) because `records.Walk` would recurse
into the unrelated subdirs. Decisions/limits:
- **Structured files only.** Auto-discovery is limited to JSON-family + CSV/TSV
  (`records.IsStructuredFile`); extracted documents (PDF/DOCX/XLSX) are *not*
  auto-exposed, so a folder of documents doesn't flood the keyspace list — query
  one explicitly via the single-file arg (B2), e.g. `n1k1 ~/Desktop/2025-W2.pdf`.
- **Additive / non-hiding.** A real `default` namespace's keyspaces (a classic
  `<ns>/<keyspace>` layout that *also* has loose root files) are merged in, and
  other real namespaces still resolve — flat discovery only ever *adds*.
- **First-seen wins** on a stem collision (`a.json` + `a.csv` → one `a`).
Each per-file keyspace rides the same `RecordsFile` single-file scan path as B2.
Covered by `glue/flat_test.go` (`TestMaybeFlatGrabBag`, `…MergesRealDefault`).

### C. Multi-file keyspace, many records per file  ✅
```
logs/
  default/
    events/   2026-01-01.jsonl  2026-01-02.jsonl  2026-01-03.jsonl
```
`n1k1 -c "SELECT type, COUNT(*) FROM default:events GROUP BY type" logs`
→ keyspace `events` = the **union of every record across all three `.jsonl`
files**; `META().id` = `events/2026-01-02.jsonl#L57`. This is the core MVP
relaxation (dir = union-of-files) and rides the opaque-document path — no label
reconciliation needed because each JSONL line is one document.

### D. Mixed formats in one keyspace  ✅
```
inventory/
  default/
    items/    legacy.csv   new.jsonl   adjustments.json
```
`n1k1 -c "SELECT sku, qty FROM default:items" inventory`
→ union across CSV + JSONL + JSON. Expected to be the hard `union_by_name` case,
but it **works today** precisely because CSV also decodes to a JSON object per row
(header-keyed): all three formats land on the opaque-document path, so
heterogeneous shapes coexist with no typed-label reconciliation needed. (The only
caveat is CSV type inference — a CSV `qty` is int-inferred, a JSON `qty` keeps its
JSON type.)

### E. Deep / recursive tree as an unkeyed union  ✅
```
metrics/
  default/
    cpu/
      hostA/2026/01/data-0001.jsonl
      hostA/2026/02/data-0002.jsonl
      hostB/2026/01/data-0003.jsonl
```
`n1k1 -c "SELECT * FROM default:cpu" metrics`
→ recurse all subdirs, union every `.jsonl`. The `hostA`/`hostB`/`2026`/`01`
path segments are **invisible** (not columns) — to expose them, use Hive naming
(F) or a catalog projection (G).

### F. Hive partitioning — `key=value` dirs become virtual columns  🟡
```
events/
  default/
    clicks/
      year=2026/month=01/part-0.parquet
      year=2026/month=02/part-1.parquet
      year=2025/month=12/part-2.parquet
```
`n1k1 -c "SELECT * FROM default:clicks WHERE year = 2026" events`
→ `year` and `month` auto-detected from the `key=value` segments as virtual
columns; `WHERE year = 2026` **prunes the 2025 file before opening it**. Depends
on partition/zone-map pruning at the scan layer — see the §5 zone-map caveat (the
predicate must reach the scan, and the fork's existing `partitionVirtualIndex` +
`SargableFor` already provides the sargability test).

### G. Bare date-partition dirs + compression — catalog projection  🟣
```
ecommerce/
  20260101/  access-0.log.gz  access-1.log.gz
  20260102/  access-0.log.gz
  .n1k1/catalog.json
```
No `key=value`, so the date dirs are **not** auto-detectable — declare them:
```json
{ "keyspaces": { "access": {
  "root": "ecommerce",
  "layout": "ecommerce/{date:YYYYMMDD}/*.log.gz",
  "format": "jsonl", "compression": "gzip",
  "partitions": [ { "name": "date", "type": "date", "projection": "YYYYMMDD" } ]
} } }
```
`n1k1 -c "SELECT * FROM access WHERE date >= '2026-01-02'" ecommerce`
→ the engine **computes** the candidate directory names from the predicate
(Athena-style projection) instead of listing the whole tree; `date` is a virtual
column. This is the marquee case for why the catalog (mode 3) exists.

### H. Transparent compression, single file  ✅ (gzip) / 🟡 (zstd)
```
archive/
  default/
    orders/   2025.jsonl.gz   2026.jsonl.zst
```
`n1k1 -c "SELECT * FROM default:orders" archive`
→ decompressed by *inner* extension (`.jsonl.gz` → gzip → JSONL). **gzip
shipped** (`compress/gzip`). zstd is still a stub: the walker recognizes `.zst`
(and `-formats=zstd`) but `openDecompressed` returns "not yet supported" — the
fast-follow is wiring `klauspost/compress` (already a dep) into that switch.

### I. `.zip` container = a directory of entries  🟡
```
exports/
  default/
    reports/   2026-q1.zip        # contains jan.csv, feb.csv, mar.csv
```
`n1k1 -c "SELECT * FROM default:reports" exports`
→ enumerate zip entries, decode each by its inner extension, union;
`META().id` = `reports/2026-q1.zip!feb.csv#L12`.

### J. CSV/TSV with header + sniffer  ✅
```
finance/
  default/
    txns/   2026.csv           # header: id,amount,currency,ts
```
`n1k1 -c "SELECT id, amount FROM default:txns WHERE currency = 'USD'" finance`
→ **shipped** (`records/records.go` `csvSource`): the header row names the columns
and each data row becomes **one JSON object** keyed by header names, with light
int/float/bool inference; TSV is the same reader with a tab delimiter. Built on
Go's `encoding/csv` (quoting/escaping/embedded-newlines correct), not the naive
`op_scan.go` splitter. **Allocation caveat, as the design warned:** this first cut
allocates the field strings per row (`encoding/csv` returns `[]string`); the
zero-copy `[]byte`-borrow reader is a later optimization. Emitting a JSON object
means CSV rides the opaque-document path — no typed-label work (see §2 reframe).

### K. Parquet  🟡 (correctness-first)
```
warehouse/
  default/
    sales/   part-0.parquet   part-1.parquet
```
`n1k1 -c "SELECT SUM(amount) FROM default:sales" warehouse`
→ read via `arrow-go` / the existing `iceberg_reader.go`. Correctness-first: the
columnar→row transpose (see §1 caveat) means no vectorized speedup until the
engine grows column-batch ops. Footer min/max later feed §5 zone-map pruning.

### L. Unstructured docs & media → `extract`-provider rows  ✅ (pure-Go text + media metadata; OCR later)
```
kb/
  default/
    docs/   handbook.pdf  q1-report.docx  budget.xlsx  deck.pptx  notes.txt  memo.rtf  readme.md
    media/  logo.png  clip.mp4          # .jpg/.jpeg/.mov also supported
```
`n1k1 -c "SELECT filename, text FROM default:docs WHERE text LIKE '%vacation%'" kb`
→ **shipped** (`records/extract.go`), pure-Go (no cgo). Each file yields **one**
`{filename, kind, text, …}` JSON record via a per-extension **`Extractor`** registry
producing a reusable `ExtractedDoc{Kind, Text, Meta}` (the seam the bleve FTS
indexer will consume — `DESIGN-indexing.md` Phase 2 — so extraction lives here
once). Two families:
- **Text** (`text` field): `.pdf` (content-stream show-text ops + `compress/zlib`
  inflate), `.docx`/`.pptx`/`.xlsx` (`archive/zip`+`encoding/xml` OOXML runs),
  `.txt`/`.log`/`.md`/`.markdown` (verbatim), `.rtf` (de-controlled to plain text).
- **Media metadata** (no `text` — that needs OCR/ASR): `.png`/`.jpg`/`.jpeg` →
  `width`/`height` (`image.DecodeConfig`); `.mp4`/`.mov` → `duration_secs`/`width`/
  `height`/`created` (a minimal ISO-BMFF box reader that streams headers, never
  the whole file).

**Deliberately narrow:** no scanned-PDF OCR, no exotic font encodings, no image
text or speech transcription, and one record per file (not the "one row per
spreadsheet row" the design floated) — those want the optional Tika/extractous+
Tesseract backend (a later cgo build tag). See §4. `-formats` groups: `doc`, `text`,
`image`, `video`, or `extract` for all.

### M. The co-located sidecar (applies to all of the above)
```
shop/
  default/orders/…                     # source data
  .n1k1/
    catalog.json                       # source mappings (G) + index defs (indexing doc)
    default/orders/
      manifest.json                    # source fingerprints + zone maps (§5)
      idx/byTotal__gsi__ab12/data.bolt # a secondary index (indexing doc)
```
Nothing in `FROM` changes; the sidecar just makes queries faster / incremental.
The full `.n1k1/` layout is specified canonically in **`DESIGN-indexing.md`**
("Sidecar layout"); this doc owns only `catalog.json`'s *source/layout* half and
`manifest.json`.

### N. Deferred — inline table functions  🔴
```
n1k1 -c "SELECT * FROM read_csv('finance/*.csv', header=true) AS t" .
```
**Rejected by the parser today** (see Open questions) — needs a grammar fork.
Until then, express the same intent with a catalog keyspace (G/J).

### O. Query-defined VIEW over a morphed-over-time source  🟣 (needs UNION ALL)
```
s3-events/                          # (or an s3:// root; object-store backend is separable)
  events_era1/  2019/*.json         # flat  {ts, user}
  events_era2/  2021/*.jsonl.gz     # renamed {event_time, uid}
  events_era3/  year=2023/*.parquet # nested  {meta:{ts,user}, kind}
  .n1k1/catalog.json
```
`.n1k1/catalog.json` defines each era as a keyspace (their own format/layout,
per §2 mode 3) **plus a view** that reconciles them:
```json
{ "views": { "events": { "query":
  "SELECT ts, user_id, action, 'era1' AS _era FROM events_era1
   UNION ALL SELECT event_time AS ts, uid AS user_id, act AS action, 'era2' FROM events_era2
   UNION ALL SELECT meta.ts, meta.user AS user_id, kind AS action, 'era3' FROM events_era3" } } }
```
`n1k1 -c "SELECT _era, COUNT(*) FROM events WHERE ts >= '2023-01-01' GROUP BY _era" s3-events`
→ `events` expands as an implicit WITH binding (reusing the CTE machinery); the
three eras present as one coherent keyspace. **Blocked on `VisitUnionAll`** (see
"Query-defined virtual datasources"); the `WHERE ts >=` also wants pushdown into
the era sub-scans to avoid reading all history (the open question there). A
single-source reshaping view (no UNION) works without that blocker.

**What the examples reveal for decisions:** (1) the flat-root keyspace-naming
question (B) was resolved to *basename* and shipped; (2) the cheap cases
(A/B/C/D/E/H-gzip/J/L) all stay on the opaque-document path, which is *why* they
shipped fast — **and (revised) that path stretched further than expected: CSV (J)
and office (L) also fit it** by emitting a JSON object per row/doc, so they did
*not* pay the typed-label cost; (3) the typed-label reconciliation turned out to
be forced only by **columnar Parquet (K)** and by **partition virtual-columns
(F/G)** — *not* by CSV — so the "columns ⇒ hard" heuristic was too broad; (4)
partition pruning (F/G) is still the first feature that needs the predicate pushed
to the scan layer, linking directly to `DESIGN-indexing.md`'s zone-map tier; (5)
the VIEW case (O) reuses the WITH/CTE machinery for free but is gated on
`VisitUnionAll` and on predicate-pushdown to stay fast — the highest-leverage
single feature for the "morphed-over-time" scenario.

## 5. Indexes & derived artifacts: storage + change detection

This is the part most coupled to `DESIGN-indexing.md`. Two questions: **where** do
derived artifacts live, and **how** do we know source data changed?

### Where: a sidecar directory, content-addressed
- Recommend a **single sidecar root per dataset**, e.g. `<dir>/.n1k1/` (hidden,
  co-located, easy to `.gitignore`/delete), holding: the catalog (§2.3), index
  files (bbolt for GSI, bleve dirs for FTS — see `DESIGN-indexing.md`), extracted
  document caches, and a **manifest** describing the source state each artifact
  was built from. A co-located hidden dir is cleaner than a parallel
  `<dir>-INDEXES` sibling (keeps everything movable as one tree) — but make the
  location configurable.
- **Canonical layout lives in `DESIGN-indexing.md` ("Sidecar layout").** To avoid
  the two docs drifting, that doc owns the full `.n1k1/` tree
  (`LAYOUT`, `catalog.json`, `<ns>/<ks>/manifest.json`,
  `<ns>/<ks>/idx/<name>__<kind>__<defhash>/…`, `tmp/`, `trash/`). This doc owns
  only two things *inside* that tree: **`catalog.json`'s source/layout half**
  (the keyspace→glob/format/partition/compression mappings of §2.3 — the
  *other* half, index definitions, is the indexing doc's) and the
  **`manifest.json`** contents (below).
- **Manifest placement — reconcile with the indexing doc: per-keyspace, not one
  root file.** This doc's "Per manifest (root)" fields below are a slight
  misnomer; align with `DESIGN-indexing.md`, which places a **`manifest.json` per
  keyspace** (`<ns>/<ks>/manifest.json`). Read "per-root" here as **per-keyspace-
  root**; the truly dataset-global bits (`manifest_schema_version`,
  `config_fingerprint`, a top `root_merkle_hash` over all keyspaces) live in
  `catalog.json`/`LAYOUT`, not in a competing top-level manifest.

### Comingling in `catalog.json`: separate by writer & lifecycle, not subkey
`catalog.json` carries *both* source/layout mappings (this doc) and index
definitions (the indexing doc) — is that a dangerous comingle when there can be
**many indexers, dynamically loaded, each maintaining/removing its own index
metadata**? The resolving principle is a **one-way data-flow** and a
**single-writer input file**:

```
source/layout config → source manifest → index defs → index instances (build-state)
  (human/gen INPUT)      (derived)          (intent)     (per-indexer OUTPUT)
```
Each layer reads only upstream. So split by **who writes it and how fast it
changes**, not by JSON subkey:

- **Declared input — safe to comingle in `catalog.json`.** Source mappings **and
  *declared* index intents** are human/generator-authored, slow-changing, and
  have **one writer / one lifecycle**. Clean subkeys (`sources`, `indexes`,
  `views`) in a single file are fine *because* there's a single writer — no
  contention, no cross-blast-radius. (Split into `sources.json` /`indexes.json`
  only if you ever want them versioned/owned separately; not required.)
- **Machine-managed output — must NOT live in `catalog.json`.** Everything that
  changes fast, is owned per-indexer, and gets dynamically created/rebuilt/blown
  away belongs in **self-describing per-instance dirs**
  (`idx/<name>__<kind>__<defhash>/meta.json`) + per-keyspace `manifest.json` —
  which the sidecar layout already does. This directly answers every worry:
  - *>0 indexers, each with its own metadata* → each instance owns its own dir;
    different indexers write **different** dirs ⇒ **no shared-file contention**.
  - *New indexer types loaded over time* → a new indexer = a new **`kind`**
    (`gsi`/`fts`/`zonemap`/`bloom`/`count`/…); its instances drop into their own
    dirs. Loading adds dirs; it never edits `catalog.json`.
  - *Info blown away / index removed* → trash that one instance dir; **blast
    radius = one dir**; `catalog.json` and other indexers untouched.
  - *Discovery* → `DESIGN-indexing.md` already notes `catalog.json` is
    reconstructable by scanning `idx/`, i.e. **the filesystem is the source of
    truth for what's built** — so built/adaptive indexes never need to be written
    back into the human catalog.
- **The one rule this forces:** `catalog.json`'s `indexes` section is **declared
  intent only**. Adaptive / auto-created indexes (the indexing doc's tier-2
  auto-index) live **purely as instance dirs** and must **not** rewrite the
  human `catalog.json` — otherwise the single-writer property (and with it the
  safety of comingling) is lost. Declared-vs-adaptive is the line, and it maps to
  file-vs-dir.

### When: the trigger and concurrency model (the actually-hard part)
§5 below details *what* to fingerprint, but *when* we re-validate — and who's
allowed to — is the harder design question and needs an explicit answer:
- **Trigger.** Default to **lazy check-on-query**: on each query touching a
  keyspace, `stat` the tree (cheap, Merkle-pruned) and rebuild only stale
  artifacts before scanning. Optionally a **TTL** ("trust the manifest for N
  seconds") to skip even the `stat` storm on hot repeated queries, and a
  `--no-revalidate` fast path for known-static trees (ties to `sealed?` below).
  A background `fsnotify` watcher is a *later* nicety, not the baseline — a CLI
  process is short-lived and shouldn't depend on a daemon.
- **Files changing mid-scan.** A long scan can race a writer. Baseline stance:
  snapshot the manifest fingerprints at query start; if a file's `(size,mtime)`
  changed *since* the snapshot when we open it, either error clearly or re-read —
  don't silently mix old and new. Document that the file datastore offers no MVCC.
- **Concurrency on `.n1k1/`.** Multiple n1k1 processes (or a stale sidecar from a
  crash) can corrupt a shared manifest/index. Need a **lockfile / atomic
  rename-into-place** for manifest writes, and readers must tolerate a
  concurrently-updating sidecar (or fall back to reading source directly if the
  sidecar is locked/absent). bbolt already gives single-writer file locking for
  the index store; the manifest needs the same discipline.

### How: a manifest with per-file fingerprints, Merkle-rolled
- **Per-file fingerprint:** record `(relative_path, size, mtime, content_hash?)`.
  - `(size, mtime)` alone is the cheap check the **Spark/Hive/Delta-class**
    manifest-driven tools effectively rely on — fast, but can miss same-size
    same-mtime edits. Good default. (DuckDB by contrast mostly *re-reads* files
    per query and has no persistent manifest by default, so the mtime-cache
    framing is really the Spark/lakehouse lineage, not DuckDB's.)
  - Add an optional **content hash** (xxhash/blake3) for correctness-critical
    use; only compute it when `(size, mtime)` says a file might have changed.
- **Merkle rollup for cheap subtree skipping:** hash each directory node from its
  children's fingerprints (git's tree-object model). A single root-hash compare
  answers "did anything change?"; on mismatch, descend only into subtrees whose
  rolled hash changed. This is what makes re-validation cheap over huge,
  mostly-static log trees.
- **Append-only / additive optimization (the log case):** for files that only
  grow, store `(size, hash_of_first_N_bytes)` or a per-file
  `(known_offset, hash_up_to_offset)`. If the prefix hash matches and size grew,
  the file is *append-only*: index only the **tail** beyond `known_offset` and
  advance the offset — never re-read old data. New dated container dirs appear as
  brand-new manifest entries (their parent subtree hash changes), so only the new
  partitions get indexed. This makes the common "logs keep coming, old data
  never changes" case incremental by construction. (Assumes `known_offset` sits
  on a *record boundary* in the decompressed stream — store it as the end of the
  last complete record, and re-scan from the prior boundary if a partial trailing
  record was appended since.)

### Manifest contents — what to track (per file, per partition, per root)
The manifest is the memory of "what we saw last time." The richer it is, the more
work we can *skip* — not just change detection, but **predicate/partition pruning**
(don't even open files that can't match a `WHERE`), **cardinality** for planning,
and **incremental** index builds. Three levels:

**Per source file:**
- `relpath`, and stable identity: `size`, `mtime`; optionally `inode`/`dev` (to
  distinguish rename from rewrite) and `ctime`.
- `content_hash` (xxhash/blake3), plus `prefix_hash` + `known_offset` for the
  append-only tail optimization.
- `format`, `compression`, `encryption` (as detected), and `codec_seekable?`
  (whether we can random-seek by logical offset — drives the doc-ID scheme in §6).
- `doc_count` (records contributed by this file) — feeds cardinality & `LIMIT`
  short-circuits.
- **Zone map / min–max stats:** `min_id`/`max_id` (the synthetic or natural doc
  IDs) **and** min/max (and null-count, distinct-estimate) for indexed/key columns
  — this is what lets a query prune the file without reading it (Parquet/Iceberg
  do exactly this).
- `schema_fingerprint` (columns + inferred types seen) — detects schema drift and
  drives `union_by_name`.
- `partition_values` (derived from the path: hive `k=v` or projected date).
- Per-index build state: for each index name, `built_through_offset` / `built?` —
  so incremental/partial index builds know what's already covered.
- `status` + `error` (files that failed to parse are recorded, not silently
  dropped — surfaces "we skipped N files").
- `last_scanned_at` (wall clock of the visit that produced this row).

**Per partition / subdirectory (rollup, Merkle-style):**
- `merkle_hash` rolled from child fingerprints (subtree-skip: unchanged hash ⇒
  skip the whole subtree without `stat`-ing every file).
- Aggregates: `doc_count`, `byte_count`, `file_count`, and rolled
  `min_id`/`max_id` + column min/max (partition-level zone map → partition
  pruning).
- `partition_key`/`value` (e.g. `date=20260101`).
- `sealed?` — an immutability hint: a *past* date partition that by policy will
  never change can be trusted from cache and skipped even for the cheap
  `(size,mtime)` check. Big win for huge historical log trees.
- `last_visited_at`.

**Per manifest (root):**
- `manifest_schema_version` and `producer_version` (n1k1 build) — bump ⇒ rebuild
  derived artifacts whose format changed.
- `root_merkle_hash` (one compare answers "did anything change at all?").
- `config_fingerprint` — hash of the catalog + index/extraction definitions the
  artifacts were built from; if the *definitions* change, invalidate derived data
  even when source bytes didn't.
- `encryption` info (wrapped-DEK / key id) if the sidecar is itself encrypted
  (§6's hard requirement).
- Global aggregates (total docs/bytes) and `last_full_scan_at`.

Rule of thumb: **stat-level fields** (`size`, `mtime`, hashes, offsets, merkle)
serve *change detection*; **stats fields** (min/max zone maps, counts, null/
distinct, schema) serve *pruning + planning*; **build-state fields** serve
*incremental indexing*. Start minimal (identity + hash + merkle + offset) and add
zone maps/counts when the planner can exploit them.

> **Caveat — the stats fields need a consumer, and it isn't free (reconciling
> with `DESIGN-indexing.md`).** That doc's "index-everything-lite" tier-1 pitches
> always-on zone maps + bloom as needing **no cbq-planner changes**, because
> file-skipping is a *scan-layer* concern rather than planner index-selection.
> That's true as far as it goes — but there's a real prerequisite it glosses and
> this doc must state: **the predicate has to reach the scan.** Today a primary
> scan doesn't get the `WHERE`; the planner emits a separate residual `Filter` op
> *above* the scan, so the datastore never sees what to prune by. So zone-map
> pruning needs one of: (a) filter/predicate **pushdown into the primary scan**
> (a conv + fork datastore-interface change — modest, and the recommended path),
> or (b) a datastore-side predicate hook. Neither is "nothing," but both are far
> short of turning **CBO** on. Cardinality / null-distinct estimates, by
> contrast, only pay off with CBO (off today) — so those stay speculative longer.
> **Sequencing:** the first manifest carries only **change-detection +
> build-state** fields; add per-file/partition **min/max zone maps** together with
> the predicate-pushdown work (that's when F/G partition pruning in the worked
> examples lights up); defer cardinality/distinct until CBO. This is the single
> most important point to keep the two design docs coherent.
>
> **Head start on the sargability half:** the fork already runs `SargableFor`
> against a throwaway `partitionVirtualIndex` (`datastore/virtual`, from
> `planner/build_scan_api.go`) to decide **partition elimination** — so the
> "does the predicate constrain the partition/zone key?" test exists; what's
> missing is delivering that verdict to the scan layer and acting on it (option
> (a)/(b) above).

### Libraries (well-tested building blocks)
- **Don't hand-roll a table format if you can avoid it.** `apache/iceberg-go`
  (already a dep; v0.5+ supports read and increasingly write, V3 spec, manifests
  with per-file stats + snapshots + time-travel) is the mature, battle-tested
  expression of exactly this manifest/snapshot idea. Leaning on Iceberg metadata
  (or DuckDB's newer **DuckLake** approach, which keeps table metadata in a SQL
  database rather than file manifests) gives partition pruning + change tracking
  for free, at the cost of adopting its on-disk conventions. Strong candidate if
  we want robustness over a bespoke `.n1k1` manifest.
- **Hashing/dedup primitives:** `cespare/xxhash` (fast non-crypto hash, already a
  dep) or blake3 for fingerprints; `restic/chunker` (FastCDC content-defined
  chunking) if we ever want sub-file dedup of large blobs. For directory Merkle
  state specifically there's no single famous Go drop-in — git's model
  (via `go-git`) and Iceberg manifests are the closest production-proven
  references; a thin custom manifest over xxhash is reasonable and small.
- **Recommendation:** start with a **thin custom manifest** in `.n1k1/`
  (per-file `size+mtime+xxhash`, Merkle-rolled, append-only offsets) — it's small,
  testable, and matches our needs exactly. Keep **Iceberg-go** in mind as the
  upgrade path when/if users want a real interoperable table format and don't
  want bespoke metadata.

---

> **Scope note — everything from here down is post-MVP / aspirational.** §6
> (doc-ID synthesis beyond the filename stem) is only *needed* once step 2 lands
> multi-record files, and even then the minimal answer — a composite
> `<relpath>#<line>` with a rescan-based `Fetch` — is enough; the seekable-zstd /
> BGFF / byte-offset machinery is a later optimization. The **encryption-at-rest**
> subsection (Tink vs age STREAM, envelope KEK/DEK, encrypted sidecars) is
> genuinely valuable to have written down, but it is a *much-later* enterprise
> feature and is presented here at the same fidelity as "add a JSONL reader" only
> because this is a design doc, not a plan. **Do not read the length of §6 as a
> measure of near-term effort.** For the MVP and the phase after it, the doc-ID is
> still just the filename stem (one-doc-per-file) or `<relpath>#L<lineno>` (multi-
> record), and there is no encryption layer at all.

## 6. Primary keys / document IDs (`META().id`)

Once we move past one-doc-per-file, "what is a record's key?" stops being
obvious — and it couples to almost everything else: fetch, indexing, compression,
and encryption.

### Implemented: file metadata via a `_meta` doc field (not `META()`)
The fork's `META()` exposes only a **fixed bitmask** of fields (id/cas/keyspace/
type/flags/expiration/xattrs), so per-file metadata can't ride `META()` without a
fork change. Instead the records layer injects a reserved **`_meta`** sub-object
into the document — `` `path` `` (dir-relative, incl. subdirs), `name`, `ext`,
`size`, `mtime`, and `pos` (the record's 0-based ordinal within a container file
— JSONL/CSV/gzip/JSON-array; absent for one-doc-per-file) — controlled by the CLI
`-meta` mode: `off` / `on` / `auto`
(default). Under `auto` each *provider* decides: office/PDF documents include it,
structured JSON/CSV do not — which also keeps the exact-match conformance suite
unchanged (it never sees `_meta`). `META().id` itself is left as the stable key
(stem / `relpath#i`), since `USE KEYS` / `JOIN ON KEYS` depend on its format;
full path + extension are available via `_meta` instead. (`path` is a SQL++
reserved word → query it as `` _meta.`path` ``.)

### Why it matters
SQL++ exposes `META().id`; `USE KEYS`, `JOIN … ON KEYS`, and the fetch-after-scan
path all need a stable per-record key. In `DESIGN-indexing.md` an index `Scan()`
emits a `PrimaryKey` **string** that `Fetch` later resolves back to a document.
Today, one-doc-per-file makes the **filename stem** the key. Multi-record formats
(CSV, JSONL, Parquet, logs) have **no natural key**, so we must synthesize one.

### Requirements for a synthesized ID
- **Deterministic** — same input ⇒ same ID, so an index built in one run matches
  fetches in another.
- **Unique within a keyspace** — which may span many files ⇒ composite with the
  source file identity.
- **Self-describing / addressable (ideally)** — encodes enough for `Fetch` to
  re-read *just that record* (O(1)) without rescanning the keyspace.
- **Stable under the expected mutation pattern** — append-only vs editable.

### Strategies (configurable per source)
1. **Filename stem** (today) — for one-doc-per-file. Human-meaningful, stable.
2. **User-designated natural key** — let the catalog (§2.3) name a key
   column/expression (a real PK), reusing the same expression parsing as the
   secondary-index design. Best when the data has a true key; keeps `META().id`
   stable across re-ingest.
3. **Ordinal / line number** within a file — zero-padded for lexicographic order
   (the "`0`-prefixed" idea). Simple, cheap, stable for append-only; but `Fetch`
   needs a rescan-to-line unless paired with a sync index.
4. **Byte offset** of the record's start in the **logical (decompressed/
   decrypted) stream** — enables O(1) `Fetch` (seek + decode one record) given a
   seekable substrate. Preferred for large files.
5. **Content hash** (xxhash/blake3) of the record — stable across reorder/move,
   dedup-friendly; but not addressable (no seek) and needs disambiguation for
   identical rows. Good for dedup/idempotency.

**Recommended default** for multi-record sources: a composite, self-describing ID
`<source-relpath>#<logical-offset>` (offset form) — globally unique, and `Fetch`
parses it to open the file (through the decrypt→decompress layers), seek to the
offset, and decode one record. Fall back to `#L<lineno>` when offsets aren't
seekable. Offer the **natural-key** option (strategy 2) for keyed data.

### Tweak: compressed containers
Plain gzip/zstd streams are **not** randomly seekable, so a byte offset alone
can't give O(1) fetch. Two fixes, both reusing §5's manifest checkpoints:
- **Seekable container formats for data we write/own:** BGZF (block-gzip) or the
  seekable-zstd format (`SaveTheRbtz/zstd-seekable-format-go` exposes
  `ReadAt`/`Seek` by *decompressed* offset, layered on `klauspost/compress` which
  we already depend on). The doc-ID stores the logical offset; the format's seek
  table maps it to the compressed block.
- **Opaque/plain-gzip inputs we don't control:** keep ordinal/line IDs and store
  periodic **sync points** (offset every N records) in the manifest, bounding
  `Fetch` re-scan to one inter-checkpoint span — the approach `zindex`/`gztool`
  use for gzip.
- **`.zip` containers:** the ID must include the entry name, e.g.
  `<zip-path>!<entry>#<offset>`. The central directory gives per-entry start
  offsets (random access *between* entries); *within* an entry the stream caveats
  above apply.

### Tweak: encrypted containers (encryption-at-rest)
A recurring enterprise ask; design it as another transparent layer.
- **Layering:** raw → **decrypt** → decompress → decode (mirrors §3). Don't invent
  crypto.
- **Random access needs segmented/chunked encryption, not whole-file AEAD:**
  - **Google Tink** `streamingaead` (AES-GCM-HKDF, ~1 MB segments; segments are
    position-bound and individually decryptable ⇒ random access by plaintext
    offset).
  - **age**'s STREAM (chunked; its `DecryptReaderAt` implements `io.ReaderAt` for
    random-access decryption).
  Both give plaintext-offset random access — exactly what offset doc IDs need. So
  **seekable-compression and seekable-encryption share one mechanism:** the
  doc-ID's logical (plaintext) offset is mapped through the format's segment/block
  table.
- **Key management:** envelope encryption — a data key (DEK) wrapped by a KEK from
  a KMS / keyring / passphrase. Use **`gocloud.dev/secrets`** (already a dep) for
  KMS-backed wrapping, or age recipients/passphrase for the simple local case.
  (Couchbase's `cbauth`/`gocbcrypto` are in the tree but heavier than a standalone
  CLI needs.)
- **Critical coupling — derived artifacts leak plaintext.** Indexes (bbolt/bleve),
  extracted Office text, and the manifest are all built from *decrypted* content;
  storing them in the clear would defeat encryption-at-rest. The `.n1k1` sidecar
  must itself be encrypted at rest (same DEK/KEK) or kept only in memory. Treat
  this as a hard requirement, not an afterthought.

### Stability coupling with §5
Positional IDs (offset/line) are durable only if the content *above* them is
immutable — exactly the append-only log case §5 optimizes, where per-file offset
checkpoints double as both the change-detection state **and** the `Fetch` seek
index. For mutable files, prefer a natural key (strategy 2) or content-hash IDs
(strategy 5), and document that synthetic positional IDs may shift on edit.

Idea: keep checksums and various sentinel positional ID's and local
chunk signatures and checksums at various offsets throughout the files
-- canary signals if anything might have changed. Not comprehensive.

## Dependency licensing (policy: permissive only — no GPL / AGPL)

Every library proposed here is intended to be **MIT / Apache-2.0 / BSD** — no
copyleft/viral licenses. Verified below (module-cache `LICENSE` files for deps
already in the graph; upstream repos otherwise):

| Library | Role | License |
|---|---|---|
| `go.etcd.io/bbolt` | GSI ordered store | MIT |
| `blevesearch/bleve/v2` | FTS index | Apache-2.0 |
| `couchbase/rhmap`, `couchbase/moss` | spill / alt store | Apache-2.0 |
| `apache/iceberg-go` | table format / manifests | Apache-2.0 |
| `apache/arrow-go/v18` | columnar / Parquet / CSV | Apache-2.0 |
| `substrait-io/substrait` | plan IR | Apache-2.0 |
| `scritchley/orc` | ORC reader | MIT |
| `hamba/avro` | Avro | MIT |
| `klauspost/compress` | gzip/zstd | BSD-3-Clause |
| `SaveTheRbtz/zstd-seekable-format-go` | seekable zstd | MIT |
| `buger/jsonparser` | JSON decode | MIT |
| Go stdlib (`encoding/csv`, `compress/gzip`, `archive/zip`) | formats | BSD-3 (Go) |
| `cespare/xxhash`, `lukechampine/blake3` | fingerprints | MIT |
| `restic/chunker` | FastCDC dedup | BSD-2-Clause |
| `google/tink-go` | streaming AEAD (encryption) | Apache-2.0 |
| `FiloSottile/age` | file encryption (STREAM) | BSD-3-Clause |
| `gocloud.dev/secrets` | KMS envelope keys | Apache-2.0 |
| Apache **Tika**, **extractous**, **Tesseract** | doc extraction / OCR | Apache-2.0 |
| `xuri/excelize` | XLSX | BSD-3-Clause |
| `ledongthuc/pdf` | PDF text | BSD-3-Clause |
| `pdfcpu/pdfcpu` | PDF text/tooling | Apache-2.0 |
| `go-git/go-git` | Merkle/tree reference | Apache-2.0 |

**Excluded (viral / non-permissive) — do NOT use:**
- `go-fitz` and any **MuPDF** wrapper — **AGPLv3**.
- **UniDoc / unipdf** — **AGPL / commercial** dual license.
- `sajari/docconv` — MIT itself, but shells out to **GPL** binaries (`wv`,
  `poppler-utils`, `unrtf`, `antiword`); avoid unless limited to permissive tools.

Note: DuckDB (MIT) is referenced only as design inspiration, not a dependency.

## Testing strategy (this project lives by its test harness)

n1k1's credibility rests on two oracles that any data-source work must plug into,
not bypass:
- **Interpreter/compiler differential.** Every new format/layout must be exercised
  by a case in the queryCases harness (`test/cases.go` + `test/query_compiler_test.go`)
  so the *compiled* path is proven to match the *interpreted* path — this is the
  guard that keeps new datastore behavior compiler-safe (see "Compiler
  compatibility" above). **Done for what shipped:** flat-root and the record
  decoders each have interp-vs-compiler cases, plus a data-backed GSI suite
  (443 interp / 439 compiler). Parquet will want one when it lands.
- **Golden fixtures for decoders.** Each decoder has small input fixtures with an
  expected row set (`records/records_test.go`) — table-driven, like `cases.go`.
  The CSV reader is built on `encoding/csv`, so quoting/escaping/embedded-newlines
  are handled by the stdlib rather than the old naive `op_scan.go` splitter.
- **Conformance suite.** The existing suite corpus is JSON one-doc-per-file, so it
  keeps validating the convention path unchanged; new formats need their own
  fixtures rather than riding the suite.
- **Differential vs DuckDB (optional, high-value).** For CSV type-inference and
  JSON array-vs-ndjson edge cases, comparing n1k1's output to DuckDB on the same
  file is a cheap, strong oracle — worth a small opt-in test target, not a
  dependency.
- **Change-detection tests.** The manifest logic (mtime skip, merkle subtree
  skip, append-only tail, concurrent-writer race) is pure logic over a temp dir
  and should be unit-tested directly — it's the part most likely to be subtly
  wrong.
- **Allocation benchmarks (a gate, not just a metric).** Per the "Allocation
  model" axis in §1, benchmark each decoder with `go test -benchmem` in
  `benchmark/` and assert **allocs/op stays ~flat as row count grows** — a rising
  curve means a per-value/per-tuple allocation leaked in (e.g. an `encoding/csv`
  regression, or a downstream op copying a borrowed slice it needn't). This is the
  guardrail that keeps a decoder faithful to n1k1's `base.Val = []byte` ethos.

## Phasing (suggested)

All new logic lands in **n1k1** — scan/fetch/decode in the glue `datastore-scan`/
`datastore-fetch` ops (which compile for free; see "Compiler compatibility"),
registered via the `engine.ExecOpEx = glue.DatastoreOp` IoC pattern. **Confirmed:
the fork needed no changes at all** — plan-time keyspace discovery was done by
wrapping the fork's datastore with `datastore/virtual` building blocks
(`glue/flatroot.go`), so the anticipated `DiscoverKeyspaces` seam was never built.

1. ✅ Relax the file datastore: directory = keyspace = union of *all* supported
   files; recurse; keep `<ns>/<keyspace>` convention + flat-root auto-detect.
   Yield records on the **opaque-document path** (no fixed label vector needed).
2. Add decoders: ✅ **JSONL + multi-doc JSON**, ✅ **CSV/TSV** (shipped via
   emit-JSON-object-per-row, so it stayed on the opaque path — the typed-label
   story was *not* needed after all); ⬜ *then* Parquet (arrow-go; correctness-only
   until column-batch ops exist — the one decoder that still needs real labels).
3. ✅ Transparent **gzip**; ⬜ zstd decode (walker recognizes `.zst`, decode is a
   stub); ⬜ `.zip` as a container.

   **← MVP LINE (crossed).** Steps 1, 2a, and gzip were the planned ~2-week win and
   shipped, *plus* CSV/TSV (2b), flat-root, office extraction (step 8), and
   `COUNT(*)` pushdown — with multi-file/flat-root/decoder differential cases.
   Everything below still waits behind demonstrated demand.

4. ⬜ Explicit `read_*('glob', opts)` table functions in FROM (power mode) —
   **blocked on a grammar fork (see Open questions); deferred in favor of step 5.**
5. ⬜ Catalog/sidecar (`.n1k1/catalog.json`) with hive + projected-date partitions.
6. ◐ Synthetic document IDs for multi-record sources: **partially done** — the
   `_meta.pos` in-file ordinal shipped (§6), but `META().id` itself is still the
   stem / simple `relpath#i`; the composite `<relpath>#<offset|line>` +
   natural-key option remain.
7. ⬜ Index/cache sidecar + manifest with Merkle + append-only offsets, where the
   offset checkpoints double as the `Fetch` seek index (joins
   `DESIGN-indexing.md`).
8. ✅ (basic) Office/unstructured extraction — **pure-Go default shipped**
   (`records/extract.go`, one record/file); ⬜ optional Tika/extractous+OCR backend
   and per-spreadsheet-row extraction remain; FTS wiring joins `DESIGN-indexing.md`.
9. ⬜ Encryption-at-rest: transparent decrypt layer (Tink/age segmented),
   envelope keys via `gocloud.dev/secrets`, and **encrypted sidecar artifacts**.

Separable tracks (not on the linear path above):
- **Query-defined VIEWs** (see "Query-defined virtual datasources"): (i)
  single-source reshaping views land with catalog-view expansion, riding the
  existing WITH/CTE machinery; (ii) union/normalize views (the morphed-schema
  case) unblock once **`VisitUnionAll`** is implemented in `glue/conv.go`; (iii)
  materialized views + predicate pushdown are the perf follow-ups (join §5).
- **Object-store backend** (S3/GCS/Azure via `gocloud.dev/blob` or `aws-sdk`,
  both already indirect deps) — lets any catalog `root`/glob point at `s3://…`;
  independent of format/layout. Reading an existing **Glue Data Catalog**
  (`aws-sdk-go-v2/service/glue`, present) is the "generated catalog" variant.

## Open questions

- **`RecordSource` signature & the CSV reader choice (allocation). (Partly
  settled.)** The shipped decoders use a `Next(rec *Record) (bool, error)` shape,
  and CSV was implemented on **`encoding/csv`** (correctness-first) — which does
  allocate the field `string`s per row, the very regression the design flagged.
  **Still open:** whether to replace it with a `[]byte`-oriented zero-copy reader
  (borrowed sub-slices, à la the old `op_scan.go` scanner's model), and to add the
  allocation benchmark gate from §1. Correctness shipped; the alloc discipline for
  large CSVs is the remaining work.
- **SQL++ surface for table functions / globs. (RESOLVED — no.)** Checked
  empirically: the fork's parser rejects both `FROM read_csv('foo.csv')`
  ("Invalid function … default:read_csv") and bare `FROM 'foo.csv'` ("must have a
  name or alias"), and there is no table-valued-function machinery in the fork's
  `algebra/`. So mode 2 requires a **goyacc grammar + algebra + planner fork**,
  which we're deferring; the **catalog (mode 3) is the power path**. Remaining
  question is only *whether we ever pay for the grammar fork* or settle for a
  datastore-recognized keyspace-name convention.
- **Fork divergence budget. (RESOLVED — zero.)** Everything shipped —
  flat-root discovery, multi-file union, JSONL/JSON/CSV/office decoders, gzip,
  `COUNT(*)`, `_meta` — landed **without a single datasource change to the fork.**
  The fork's only n1k1 commits are build-plumbing (EE-semantics toggle, pure-Go
  sigar stub, committed generated parser). Plan-time discovery was done by
  *wrapping* the datastore with `datastore/virtual` building blocks
  (`glue/flatroot.go`), so no `DiscoverKeyspaces` seam was needed. Execution
  (scan/fetch/decode) is all n1k1 glue ops. The only remaining sub-question is
  whether a future **catalog** (non-directory keyspace names) can still be faked by
  the same wrapping trick — the expectation, given flat-root worked, is **yes,
  still no fork change**; home C (a full n1k1-side datastore) stays the fallback of
  last resort.
- **Columnar-source performance.** Do we ever add column-batch ops to the engine
  so Parquet/Arrow is a real perf win, or accept the transpose-to-rows cost and
  treat columnar formats as correctness-only? (See §1 caveat.)
- **Partition columns vs document shape.** Hive/projected partition values become
  virtual columns — how do they coexist with SQL++'s schemaless document model?
- **Bespoke manifest vs Iceberg-go.** Adopt Iceberg's proven metadata, or keep a
  minimal custom `.n1k1` manifest? Trade interop/robustness vs simplicity.
- **CSV typing in a JSON/SQL++ world.** How aggressively to infer types vs treat
  CSV cells as strings; how to expose overrides.
- **Predicate pushdown through a VIEW.** For a query-defined virtual datasource
  (esp. the morphed-S3 union view), does a `WHERE`/partition predicate on the view
  reach the sub-source scans so whole eras/partitions prune — or does the view
  read all history each query? Depends on cbq's rewrite rules over the expanded
  union + the §5 predicate-to-scan work. The gating perf question for views;
  materialization is the fallback. (Correctness is fine either way.)
- **View definition home & DDL.** Views live in `.n1k1/catalog.json` as a stored
  `SELECT` string (no `CREATE VIEW` DDL, since n1k1 doesn't execute DDL — same
  situation as index defs in `DESIGN-indexing.md`). Is a catalog `views` map the
  right surface, and how do view names coexist with keyspace names in resolution
  (view shadows keyspace? separate namespace)?
- **Native vs cgo extractors/OCR.** Whether to accept the `extractous`/Tika
  native dependency for document breadth + OCR, or stay pure-Go and narrower.
- **Default doc-ID scheme.** Positional `<relpath>#<offset>` (addressable, but
  shifts on edit) vs content-hash (stable, not seekable) vs requiring a natural
  key — and how aggressively to default per source/mutation pattern.
- **Encryption scope & seekability.** Which segmented-encryption format (Tink
  vs age) and whether to require seekable compression/encryption for large
  encrypted sources, vs accepting rescan-from-checkpoint when inputs are opaque.

## Sources

- DuckDB — Reading Multiple Files (glob `**`, lists, `union_by_name`, `filename`):
  https://duckdb.org/docs/current/data/multiple_files/overview
- DuckDB — Hive Partitioning (auto-detect `key=value`, pruning):
  https://duckdb.org/docs/current/data/partitioning/hive_partitioning
- DuckDB — Loading JSON (gzip/zstd auto-detect, newline-delimited vs array):
  https://duckdb.org/docs/current/data/json/loading_json
- DuckDB — Directly Reading Files / replacement scans:
  https://duckdb.org/docs/current/guides/file_formats/read_file
- AWS Athena — Partition Projection (date templates, avoid listing):
  https://docs.aws.amazon.com/athena/latest/ug/partition-projection.html
- Open table formats overview (Iceberg/Delta/Hudi/Paimon/DuckLake):
  https://datalakehousehub.com/blog/2025-09-ultimate-guide-to-open-table-formats/
- Apache iceberg-go (read/write, V3, manifests):
  https://github.com/apache/iceberg-go ; release notes:
  https://iceberg.apache.org/blog/apache-iceberg-go-0.5.0-release/
- extractous-go (Go document extraction, Tika+Tesseract):
  https://github.com/rahulpoonia29/extractous-go
- restic/chunker (FastCDC content-defined chunking in Go):
  https://github.com/restic/chunker
- Seekable zstd (random access by decompressed offset, over klauspost/compress):
  https://github.com/SaveTheRbtz/zstd-seekable-format-go
- Tink Streaming AEAD (segmented encryption, random access):
  https://developers.google.com/tink/streaming-aead
- age STREAM / `DecryptReaderAt` (chunked, random-access decryption):
  https://github.com/FiloSottile/age
