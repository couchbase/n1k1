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
2. **In-memory secondary index** (Phase 1 under #1) — engine value; `IndexScan` in
   `EXPLAIN`; no worker/persistence.
3. **Web Worker** (#3) — foundational; unlocks visible live stats + cancellation +
   streaming at once.
4. **OPFS index cache** (Phase 2 under #1) — layers on top of #2/#3.

### 1. Secondary indexes in the wasm build (the through-line)
The reason we can't just recompile bbolt/bleve is mmap. Realistic path:
- **Phase 1** — mmap-free **in-memory SI**, built from `catalog.json` at open, backing
  the scan with a sorted structure over the existing order-preserving key encoding
  (`idx_si_encode.go`). Gives real `IndexScan` plans in the browser (visible in
  `EXPLAIN`); no persistence, no worker, cross-browser. *Prerequisite for everything below.*
- **Phase 2** — **OPFS as a persistence cache** for that in-memory index: serialize on
  build, deserialize on open, invalidate on `sourceSignature` mismatch (`idx_si.go` has
  the freshness logic). Because the index is RAM-resident during queries, OPFS is touched
  only at open/build → **async, main-thread, no worker**. Keep source docs in the picked
  folder; keep indexes out of it (no `.n1k1/` clutter, no `readwrite` prompt). OPFS is a
  *cache* (evictable; call `navigator.storage.persist()`), never source of truth.
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

### 3. Web Worker  (foundational; unlocks several)
Run the engine off the main thread; postMessage queries/results. Enables: **visible live
stats** (see below), query **cancellation**/timeouts, responsive UI on big scans, and
synchronous OPFS handles if ever needed.

### 4. Live stats  (plumbing already there)
`glue.Session.CollectStats` + `OnStats(*base.Stats)` already emit per-operator counters at
engine checkpoints (`../DESIGN-stats.md`). Mechanically it works in wasm; but on the main
thread the UI can't repaint mid-query, so progress only shows *after* completion. Pair with
#3 (worker) to stream live snapshots into a progress view. Moot for <20 ms demo queries;
valuable once data/folders are large.

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
