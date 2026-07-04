# n1k1 in the browser — design & roadmap

n1k1's SQL++/N1QL engine compiled to WebAssembly, running fully client-side. See
`README.md` for usage; this doc is the design rationale + the roadmap, enough to
resume the thread. Mechanics that live in code comments aren't repeated here.

## What exists

A static page (`index.html`) loads `n1k1.wasm` and calls `globalThis.n1k1RunQuery`
/ `n1k1OpenDir`. Data comes from an in-memory filesystem (`wasm/fs_mem.js`) seeded
with the built-in sample (`samples.js`) or a folder the user picks. Every query is
currently a **primary scan** (no secondary indexes in the wasm build — see below).

File map: `wasm/main_wasm.go` (JS entry points), `wasm/fs_mem.js` (in-memory fs),
`wasm/build.sh` (+`wasm/overlay/*.go`, dep patches), `samples.js`, `index.html`.

## The two hard problems (solved)

1. **No syscalls under GOOS=js.** Two kinds of fix:
   - *Our code*: the on-disk index backends (`glue/idx_si.go` bbolt, `glue/idx_fts.go`
     bleve, `glue/idx_si_suggest.go`) use mmap/flock → excluded via `//go:build !wasm`;
     `glue/idx_wasm.go` stubs the few symbols the core still references (notably a
     `secondaryIndex` type that embeds `datastore.Index` so the type assertions stay
     legal). `idx_si_catalog.go`/`idx_si_encode.go` are pure and stay in.
   - *Deps*: `couchbase/query/{logging,util}` (rlimit/rusage/signals) and
     `edsrzf/mmap-go` (mmap) get js stubs applied to **local copies** via `go.mod`
     replaces — host-specific, uncommitted (like the EE stubs in
     `../DESIGN-testing.md`). `build.sh` regenerates them.
2. **No filesystem.** `wasm/fs_mem.js` installs a read-only in-memory `globalThis.fs`
   (open/read/stat/readdir + stdout/stderr → console; writes ENOSYS). The engine
   reads its `<ns>/<keyspace>/<key>.json` tree from there as from disk. `MakeVars`'
   TempDir MkdirTemp fails silently and demo-sized data never spills, so read-only
   is enough.

## Constraints worth remembering

- **Size**: ~67 MB raw / ~12 MB gzip / ~8 MB brotli. `-ldflags="-s -w"` barely helps
  (Go wasm). Build outputs (`n1k1.wasm`, `wasm_exec.js`) are git-ignored.
- **mmap does not exist in wasm** — not fixable by any storage API. bbolt/bleve read
  *through* an mmap, so they can't run in the browser regardless of file access. OPFS
  gives synchronous pread/pwrite, **not** mmap.
- **Single-threaded / cooperative**: a synchronous query on the main thread blocks the
  UI until it returns — no mid-query repaint. A **Web Worker** is the fix (see roadmap).
- **Browser support asymmetry**: `showDirectoryPicker` is **Chromium-only**; OPFS +
  sync access handles are cross-browser (Chromium, FF 111+, Safari 16.4+). Drag & drop
  and `<input type=file>` are universal.

## Local folder access (done, Chromium-only)

`index.html`'s **Open folder** uses `showDirectoryPicker({mode:"read"})`, walks the
tree, eager-loads `.json`/`.jsonl` into the fs (caps: 5000 files / 64 MB), mounts it,
and calls `n1k1OpenDir`, which reopens the Session and returns keyspaces (reusing the
CLI's `OpenSession` + `NamespaceByName().KeyspaceNames()` — see `cmd/n1k1/keyspaces.go`).
Mapping: subdirs → keyspaces; loose top-level files → a folder-named keyspace. Schema
sidebar + starter queries are rebuilt by asking the engine (`COUNT(*)` + `LIMIT 1` per
keyspace).

## Roadmap

**Recommended order** (all converge on the storage-as-interface refactor):
1. **Drag-drop ingestion** (#2) — cheapest, cross-browser, reuses existing gzip/JSONL
   handling. **DONE** — see `wasm/ingest.js` + "Ingestion" below.
2. **In-memory secondary index** (Phase 1 under #1) — **DONE** — see `glue/idx_mem.go`
   + "Secondary indexes" below. Real `IndexScan` in `EXPLAIN`, both builds.
3. **OPFS index cache** (Phase 2 under #1) — **DONE** — `wasm/opfs.js` + `glue/idx_mem.go`
   two-tier cache. See "Secondary indexes" below.
4. **Web Worker** (#3) — **DONE** — `wasm/worker.js` hosts the engine off the main
   thread; live stats stream mid-query. See "Web Worker" below. Next under it:
   cancellation + streaming results.

### 1. Secondary indexes in the wasm build (the through-line)
The reason we can't just recompile bbolt/bleve is mmap. Realistic path:
- **Phase 1 — DONE** (`glue/idx_mem.go`). mmap-free **in-memory SI**, built from
  `catalog.json` at open, backing the scan with a sorted `[][]byte` (encode(keys)+docID)
  binary-searched per span — sharing the order-preserving encoding and the exact
  bound/inclusion logic with bbolt (`idx_si_encode.go`). The engine dispatches on a
  `index` interface (`idx.go`) that both bbolt `secondaryIndex` and `memIndex`
  satisfy, so the ~5 core scan/convert sites (conv.go, datastore_scan.go) are backend-
  agnostic. Wiring: wasm always uses mem (`idx_wasm.go`); native keeps bbolt default with
  mem opt-in via `SecondaryIndexMode="mem"` (`idx_si.go`). Built process-wide cached, rebuilt
  on `sourceSignature` change. Gives real `IndexScan` in `EXPLAIN` in both builds; no
  persistence, no worker, cross-browser. Tests: `glue/idx_mem_test.go` (native, incl.
  primary-scan parity) + `web/wasm/e2e.test.mjs` (browser build). The `.n1k1/catalog.json`
  sidecar sits at the datastore root, beside the `default` namespace (a separate, empty
  ".n1k1" namespace to the file datastore — harmless). Follow-up: FTS still bbolt/bleve-only.
- **Phase 2 — DONE** (`wasm/opfs.js` + `glue/idx_mem.go`). `openMemIndex` is a three-tier
  cache: in-process slot → persisted blob → build+persist. The blob is a self-delimiting
  `encodeMemBlob(sig, entries)`. Native writes it to `<root>/.n1k1/cache/<ns>__<ks>__<hash>.idx`
  on disk (free persistent index cache for the CLI too); wasm's read-only fs write fails, so
  the blob is queued and `n1k1TakeIndexBlobs` hands it to JS. `n1k1OpenDir` returns a
  `cachePlan` ([{path, sig}]); `opfs.js` preloads matching OPFS blobs into the fs BEFORE the
  first query (`n1k1MountFile`) and persists freshly-built ones after — async, main-thread,
  no worker (index is RAM-resident during the query). OPFS is origin-private + evictable, so
  it's a pure cache; the embedded `sig` invalidates stale blobs. All degrades to no-ops where
  OPFS is absent (Firefox private mode, etc.). Tests: cache round-trip + tamper-proof
  "cache is actually used" (`idx_mem_test.go`), and the openDir-cachePlan / blob-drain
  boundary (`e2e.test.mjs`); the browser `opfsGet`/`opfsPut` themselves are browser-verified
  (node has no OPFS). Not yet: OPFS *source* persistence (re-open a dropped dataset without
  re-dropping) — the natural companion.
- **Phase 3** — sync-handle + worker only if an index must exceed RAM (bbolt-style
  paging), or for FTS. bleve *can* run in-memory via its `gtreap` store (no mmap) — worth
  a spike, but scorch/zap/boltdb must stay out of the graph; a hand-rolled inverted index
  may be simpler.

Refactor that ties it together: **make index storage an interface** — bbolt impl (native),
in-memory±OPFS impl (wasm) — sharing key-encoding + catalog logic. Also lifts the reusable
schema/suggest logic out of `cmd/n1k1`.

### 2. Ingestion: drag & drop  (DONE — `wasm/ingest.js`)
Drop (or **Load files…**, cross-browser) `.json`/`.jsonl`/`.ndjson`/`.gz`/`.tar`/`.tar.gz`
(`.tgz`). `ingest.js` inflates gzip via `DecompressionStream('gzip')`, reads USTAR tars in
JS, and **normalizes to the classic `<keyspace>/<key>.json` layout** (not n1k1's flat-file
discovery, whose pure-flat-root case collapses to one basename-named keyspace — see
`glue/flat.go`). Mapping: a loose `foo.jsonl`→keyspace `foo` (doc per line); a tar dir
`sub/x.json`→keyspace `sub`, doc key = file stem; doc key otherwise = id/_id/uuid/key field
or index. Then `n1k1MountTree` + `n1k1OpenDir` + `activateSource` (the picker's tail).
Validated headlessly with real gzip+tar bytes. Not supported: `.zst` (no browser codec),
`.zip` (central-directory parsing — a follow-up). Caps: 200k docs / 64 MB.

### 3. Web Worker  (DONE — `wasm/worker.js`)
The engine, fs, ingestion and OPFS all run in a classic Web Worker
(`importScripts` of wasm_exec.js + fs_mem/ingest/opfs + samples). `index.html` is a thin
async RPC client (`{id, op, args}` ⇄ `{id, ok, result}`): `openBuiltin`/`loadFiles`/
`openTree`/`query`, bundled to cut round-trips (each open also does the OPFS preload/persist
inside the worker). The main thread only ships SQL + file bytes and renders rows/stats, so
it stays responsive; a query blocks only the worker thread. All UI query calls (`run`,
`buildSchema`) are now async. Web Workers are ~universal so there's no inline fallback;
paths are relative to `web/wasm/` (../wasm_exec.js, ../n1k1.wasm, ../samples.js). The
transport is browser-verified — node has no Web Worker API — but the engine behind it stays
node-tested via the direct globals (e2e.test.mjs). Next under this: **cancellation**
(needs an engine stop signal) and **streaming results** (the engine is pull-based; yield
rows incrementally instead of collecting).

### 4. Live stats  (DONE)
`Session.CollectStats` + `OnStats(*base.Stats)` fire per `engine.ScanYieldStatsEvery` rows
(`../DESIGN-stats.md`). `main_wasm.go`'s runQuery enables them and, via an optional
`globalThis.n1k1EmitStats` hook (throttled 50 ms), streams `glue.StatsSnapshotJSON` snapshots;
the worker forwards each as a `{type:"stats"}` message that the (free) main thread paints —
the concrete payoff of #3. The final snapshot also rides in the query result. Serializer +
"OnStats fires + snapshot has positive counters" are node/native-tested
(`glue/stats_snapshot_test.go`). Only visibly *streams* on >`ScanYieldStatsEvery` (1024) rows,
so load a large folder to watch it; tiny queries just show the final line.

### 5. Streaming results
Engine is pull-based (`yieldVals`); today `main_wasm.go` collects all rows then returns.
For large result sets, stream rows incrementally to a virtualized table (needs #3 to avoid
blocking, or chunked yields).

### Considered, deferred (off n1k1's lane)
- **Remote-URL / HTTP-range querying** (DuckDB `httpfs`): great for columnar formats where
  you read only needed byte ranges; n1k1's per-doc JSON / JSONL isn't range-friendly, so the
  win is just "fetch a whole small file by URL." Low priority.
- **Parquet / CSV / columnar**: DuckDB's lane, not n1k1's document model. Skip.
- **Persisted mutations** (INSERT/UPDATE written back via FSA `readwrite` or OPFS): possible
  but out of the read-analytics mission for now.

## Peer notes (what to borrow)
- **DuckDB-WASM**: register a dropped File/Blob as a virtual file and query it directly;
  Arrow-chunked streaming results; OPFS persistence; worker-based. Borrow: drag-drop virtual
  files (#2), streaming (#5), worker (#3). Skip: Parquet/httpfs.
- **SQLite-WASM**: OPFS VFS via **sync access handles in a worker** — the reference for #3+
  persistent storage. Borrow the worker+OPFS pattern if/when paging is needed.
- **chdb / clickhouse-local**: query local files in many formats; same "engine in browser
  over local data" thesis. Confirms the direction; format breadth isn't our differentiator —
  *SQL++ over JSON documents* is.
